"""Core payment orchestration service handling transactions, idempotency, and state transitions."""

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from payflow.cache.manager import cache_manager
from payflow.common.db import default_engine, get_db_session
from payflow.common.exceptions import PaymentNotFoundError
from payflow.common.resilience import CircuitBreakerOpenError, provider_circuit_breaker
from payflow.payments.idempotency import complete_idempotency, reserve_or_fetch_idempotency
from payflow.payments.models import IdempotencyKeyRecord, OutboxEventRecord, PaymentRecord
from payflow.payments.provider import PaymentProvider, SandboxPaymentProvider
from payflow.payments.schemas import PaymentCreateRequest, PaymentResponse, RefundResponse
from payflow.payments.state_machine import PaymentStatus, transition


class PaymentService:
    """Production payment service coordinating state transitions, outbox events, and idempotency."""

    def __init__(
        self,
        engine: Any = None,
        provider: PaymentProvider | None = None,
    ) -> None:
        self.engine = engine or default_engine
        self.provider = provider or SandboxPaymentProvider()

    def create_payment(
        self,
        merchant_id: str,
        idempotency_key: str,
        request: PaymentCreateRequest,
    ) -> tuple[PaymentResponse, bool]:
        """Processes a payment request with database-enforced idempotency.

        Returns:
            (PaymentResponse, is_replay):
            - is_replay=False indicates a newly authorized transaction (HTTP 201).
            - is_replay=True indicates a safely replayed previous transaction (HTTP 200).
        """
        payload_dict = request.model_dump()
        cache_key = f"ik_resp:{merchant_id}:{idempotency_key}"

        # Phase 1: Atomically claim idempotency key and record initial payment state (short OLTP transaction)
        with get_db_session(self.engine) as session:
            ik_record, is_new = reserve_or_fetch_idempotency(
                session=session,
                merchant_id=merchant_id,
                operation="create_payment",
                idempotency_key=idempotency_key,
                payload=payload_dict,
            )

            # Return cached response if this is a completed replay
            if not is_new:
                cached_data = json.loads(ik_record.response_body or "{}")
                # Populate cache for future hits
                cache_manager.set(cache_key, ik_record.response_body or "{}", ttl_seconds=600)
                return PaymentResponse(**cached_data), True

            # Initialize payment in PROCESSING state
            payment = PaymentRecord(
                merchant_id=merchant_id,
                amount_minor=request.amount_minor,
                currency=request.currency,
                status=PaymentStatus.PROCESSING,
                payment_method_token=request.payment_method_token,
                capture_method=request.capture_method,
                metadata_json=json.dumps(request.metadata),
                idempotency_key=idempotency_key,
            )
            session.add(payment)
            session.flush()
            payment_id = payment.id
            ik_id = ik_record.id
            # Session commits on exit; DB connection is released before downstream provider I/O

        # Phase 2: Call downstream payment provider protected by Circuit Breaker
        try:
            provider_res = provider_circuit_breaker.execute(
                self.provider.process_charge,
                payment_id=payment_id,
                amount_minor=request.amount_minor,
                currency=request.currency,
                token=request.payment_method_token,
                idempotency_key=idempotency_key,
            )
        except CircuitBreakerOpenError as cb_err:
            from payflow.payments.provider import ProviderChargeResult
            provider_res = ProviderChargeResult(
                status="FAILED",
                error_code="acquirer_circuit_breaker_open",
                error_message=str(cb_err),
            )
        except Exception as exc:
            from payflow.payments.provider import ProviderChargeResult
            provider_res = ProviderChargeResult(
                status="UNKNOWN",
                error_code="provider_connection_error",
                error_message=str(exc),
            )

        # Phase 3: Atomically update terminal state, outbox event, and complete idempotency (short OLTP transaction)
        with get_db_session(self.engine) as session:
            payment = session.execute(
                select(PaymentRecord).where(PaymentRecord.id == payment_id)
            ).scalar_one()
            ik_record = session.execute(
                select(IdempotencyKeyRecord).where(IdempotencyKeyRecord.id == ik_id)
            ).scalar_one()

            if provider_res.status == "SUCCEEDED":
                payment.status = transition(PaymentStatus.PROCESSING, PaymentStatus.SUCCEEDED)
                payment.provider_reference = provider_res.provider_reference
                event_name = "payment.succeeded"
            elif provider_res.status == "FAILED":
                payment.status = transition(PaymentStatus.PROCESSING, PaymentStatus.FAILED)
                payment.error_code = provider_res.error_code
                payment.error_message = provider_res.error_message
                event_name = "payment.failed"
            else:  # UNKNOWN (e.g. gateway timeout or connection error)
                payment.status = transition(PaymentStatus.PROCESSING, PaymentStatus.UNKNOWN)
                payment.error_code = provider_res.error_code or "provider_timeout"
                payment.error_message = provider_res.error_message or "Unknown provider outcome"
                event_name = "payment.unknown"

            # Atomically insert outbox event in the SAME transaction
            outbox_payload = {
                "payment_id": payment.id,
                "merchant_id": merchant_id,
                "amount_minor": payment.amount_minor,
                "currency": payment.currency,
                "status": payment.status,
                "provider_reference": payment.provider_reference,
            }
            outbox_evt = OutboxEventRecord(
                aggregate_type="payment",
                aggregate_id=payment.id,
                event_type=event_name,
                payload_json=json.dumps(outbox_payload),
                status="PENDING",
            )
            session.add(outbox_evt)

            # Build response
            response = PaymentResponse(
                id=payment.id,
                merchant_id=payment.merchant_id,
                amount_minor=payment.amount_minor,
                currency=payment.currency,
                status=PaymentStatus(payment.status),
                payment_method_token=payment.payment_method_token,
                capture_method=payment.capture_method,
                metadata=request.metadata,
                error_code=payment.error_code,
                error_message=payment.error_message,
                provider_reference=payment.provider_reference,
                created_at=payment.created_at,
                updated_at=payment.updated_at,
            )

            # Mark idempotency record COMPLETED with serialized response
            complete_idempotency(
                session=session,
                record=ik_record,
                response_code=201,
                response_body=response.model_dump_json(),
                payment_id=payment.id,
            )

            # Populate L1/L2 Cache for fast replays and read lookups
            cache_manager.set(cache_key, response.model_dump_json(), ttl_seconds=600)
            cache_manager.set(f"pay_id:{payment.id}", response.model_dump_json(), ttl_seconds=600)

            return response, False

    def get_payment(self, merchant_id: str, payment_id: str) -> PaymentResponse:
        """Retrieves a payment record by merchant_id and payment_id."""
        cache_key = f"pay_id:{payment_id}"
        cached = cache_manager.get(cache_key)
        if cached:
            try:
                data = json.loads(cached)
                if data.get("merchant_id") == merchant_id or merchant_id in ("ALL", "*"):
                    return PaymentResponse(**data)
            except Exception:
                pass

        with get_db_session(self.engine) as session:
            stmt = select(PaymentRecord).where(
                PaymentRecord.merchant_id == merchant_id,
                PaymentRecord.id == payment_id,
            )
            payment = session.execute(stmt).scalar_one_or_none()
            if not payment:
                raise PaymentNotFoundError(f"Payment with ID '{payment_id}' was not found.")

            metadata = json.loads(payment.metadata_json or "{}")
            return PaymentResponse(
                id=payment.id,
                merchant_id=payment.merchant_id,
                amount_minor=payment.amount_minor,
                currency=payment.currency,
                status=PaymentStatus(payment.status),
                payment_method_token=payment.payment_method_token,
                capture_method=payment.capture_method,
                metadata=metadata,
                error_code=payment.error_code,
                error_message=payment.error_message,
                provider_reference=payment.provider_reference,
                created_at=payment.created_at,
                updated_at=payment.updated_at,
            )

    def capture_payment(self, merchant_id: str, payment_id: str, amount_minor: int | None = None) -> PaymentResponse:
        """Captures an authorized payment."""
        with get_db_session(self.engine) as session:
            stmt = select(PaymentRecord).where(
                PaymentRecord.merchant_id == merchant_id,
                PaymentRecord.id == payment_id,
            )
            payment = session.execute(stmt).scalar_one_or_none()
            if not payment:
                raise PaymentNotFoundError(f"Payment with ID '{payment_id}' was not found.")

            payment.status = transition(PaymentStatus(payment.status), PaymentStatus.SUCCEEDED)
            if amount_minor is not None:
                payment.amount_minor = amount_minor

            outbox_payload = {
                "payment_id": payment.id,
                "merchant_id": merchant_id,
                "amount_minor": payment.amount_minor,
                "currency": payment.currency,
                "status": payment.status,
                "provider_reference": payment.provider_reference,
            }
            session.add(OutboxEventRecord(
                aggregate_type="payment",
                aggregate_id=payment.id,
                event_type="payment.succeeded",
                payload_json=json.dumps(outbox_payload),
                status="PENDING",
            ))
            session.commit()
            session.refresh(payment)

            metadata = json.loads(payment.metadata_json or "{}")
            return PaymentResponse(
                id=payment.id,
                merchant_id=payment.merchant_id,
                amount_minor=payment.amount_minor,
                currency=payment.currency,
                status=PaymentStatus(payment.status),
                payment_method_token=payment.payment_method_token,
                capture_method=payment.capture_method,
                metadata=metadata,
                error_code=payment.error_code,
                error_message=payment.error_message,
                provider_reference=payment.provider_reference,
                created_at=payment.created_at,
                updated_at=payment.updated_at,
            )

    def cancel_payment(self, merchant_id: str, payment_id: str) -> PaymentResponse:
        """Cancels/voids an uncaptured or unknown payment."""
        with get_db_session(self.engine) as session:
            stmt = select(PaymentRecord).where(
                PaymentRecord.merchant_id == merchant_id,
                PaymentRecord.id == payment_id,
            )
            payment = session.execute(stmt).scalar_one_or_none()
            if not payment:
                raise PaymentNotFoundError(f"Payment with ID '{payment_id}' was not found.")

            payment.status = transition(PaymentStatus(payment.status), PaymentStatus.CANCELLED)

            outbox_payload = {
                "payment_id": payment.id,
                "merchant_id": merchant_id,
                "amount_minor": payment.amount_minor,
                "currency": payment.currency,
                "status": payment.status,
            }
            session.add(OutboxEventRecord(
                aggregate_type="payment",
                aggregate_id=payment.id,
                event_type="payment.cancelled",
                payload_json=json.dumps(outbox_payload),
                status="PENDING",
            ))
            session.commit()
            session.refresh(payment)

            metadata = json.loads(payment.metadata_json or "{}")
            return PaymentResponse(
                id=payment.id,
                merchant_id=payment.merchant_id,
                amount_minor=payment.amount_minor,
                currency=payment.currency,
                status=PaymentStatus(payment.status),
                payment_method_token=payment.payment_method_token,
                capture_method=payment.capture_method,
                metadata=metadata,
                error_code=payment.error_code,
                error_message=payment.error_message,
                provider_reference=payment.provider_reference,
                created_at=payment.created_at,
                updated_at=payment.updated_at,
            )

    def refund_payment(
        self,
        merchant_id: str,
        payment_id: str,
        amount_minor: int | None = None,
        reason: str | None = None,
    ) -> RefundResponse:
        """Issues a full or partial refund and enqueues a compensating ledger event."""
        import uuid
        from datetime import datetime, timezone
        with get_db_session(self.engine) as session:
            stmt = select(PaymentRecord).where(
                PaymentRecord.merchant_id == merchant_id,
                PaymentRecord.id == payment_id,
            )
            payment = session.execute(stmt).scalar_one_or_none()
            if not payment:
                raise PaymentNotFoundError(f"Payment with ID '{payment_id}' was not found.")

            if payment.status not in (PaymentStatus.SUCCEEDED, PaymentStatus.PARTIALLY_REFUNDED):
                raise ValueError(f"Cannot refund payment with status '{payment.status}'. Must be SUCCEEDED.")

            refund_amt = amount_minor if amount_minor is not None else payment.amount_minor
            if refund_amt > payment.amount_minor:
                raise ValueError(f"Refund amount {refund_amt} exceeds payment amount {payment.amount_minor}.")

            is_partial = refund_amt < payment.amount_minor
            target_status = PaymentStatus.PARTIALLY_REFUNDED if is_partial else PaymentStatus.REFUNDED
            payment.status = transition(PaymentStatus(payment.status), target_status)

            refund_id = f"rf_{uuid.uuid4().hex[:16]}"
            now = datetime.now(timezone.utc)

            outbox_payload = {
                "refund_id": refund_id,
                "payment_id": payment.id,
                "merchant_id": merchant_id,
                "amount_minor": refund_amt,
                "currency": payment.currency,
                "reason": reason or "requested_by_customer",
                "status": "REFUNDED",
            }
            session.add(OutboxEventRecord(
                aggregate_type="payment",
                aggregate_id=payment.id,
                event_type="payment.refunded",
                payload_json=json.dumps(outbox_payload),
                status="PENDING",
            ))
            session.commit()

            return RefundResponse(
                id=refund_id,
                payment_id=payment.id,
                merchant_id=merchant_id,
                amount_minor=refund_amt,
                currency=payment.currency,
                reason=reason,
                status="REFUNDED",
                created_at=now,
            )
