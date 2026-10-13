"""Integration tests for PaymentService, state transitions, and outbox persistence."""

import pytest
from sqlalchemy import create_engine, select

from payflow.common.db import Base, get_db_session
from payflow.common.exceptions import IdempotencyConflictError
from payflow.payments.models import OutboxEventRecord, PaymentRecord
from payflow.payments.provider import SandboxPaymentProvider
from payflow.payments.schemas import PaymentCreateRequest
from payflow.payments.service import PaymentService
from payflow.payments.state_machine import PaymentStatus


@pytest.fixture
def service() -> PaymentService:
    # Use isolated in-memory SQLite database per test
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return PaymentService(engine=engine, provider=SandboxPaymentProvider())


def test_create_payment_success(service: PaymentService) -> None:
    req = PaymentCreateRequest(
        amount_minor=2500,
        currency="INR",
        payment_method_token="pm_sandbox_success",
        metadata={"order_ref": "ord_999"},
    )
    res, is_replay = service.create_payment("merch_1", "ik_success_001", req)

    assert is_replay is False
    assert res.amount_minor == 2500
    assert res.currency == "INR"
    assert res.status == PaymentStatus.SUCCEEDED
    assert res.provider_reference is not None

    # Verify Outbox event was created atomically
    with get_db_session(service.engine) as session:
        outbox = session.execute(
            select(OutboxEventRecord).where(OutboxEventRecord.aggregate_id == res.id)
        ).scalar_one()
        assert outbox.event_type == "payment.succeeded"
        assert outbox.status == "PENDING"


def test_idempotent_replay_returns_cached_result(service: PaymentService) -> None:
    req = PaymentCreateRequest(
        amount_minor=1500,
        currency="USD",
        payment_method_token="pm_sandbox_success",
    )
    # First call
    res1, is_replay1 = service.create_payment("merch_1", "ik_replay_001", req)
    assert is_replay1 is False

    # Second call with same key and payload
    res2, is_replay2 = service.create_payment("merch_1", "ik_replay_001", req)
    assert is_replay2 is True
    assert res2.id == res1.id
    assert res2.amount_minor == res1.amount_minor
    assert res2.status == res1.status

    # Assert exactly 1 record was written to payments table
    with get_db_session(service.engine) as session:
        count = len(session.execute(select(PaymentRecord)).scalars().all())
        assert count == 1


def test_payload_mismatch_raises_idempotency_conflict(service: PaymentService) -> None:
    req1 = PaymentCreateRequest(
        amount_minor=1500,
        currency="USD",
        payment_method_token="pm_sandbox_success",
    )
    req2 = PaymentCreateRequest(
        amount_minor=2000,  # Changed amount!
        currency="USD",
        payment_method_token="pm_sandbox_success",
    )

    service.create_payment("merch_1", "ik_conflict_001", req1)

    with pytest.raises(IdempotencyConflictError, match="reused with a different request payload"):
        service.create_payment("merch_1", "ik_conflict_001", req2)


def test_card_decline_transitions_to_failed(service: PaymentService) -> None:
    req = PaymentCreateRequest(
        amount_minor=5000,
        currency="EUR",
        payment_method_token="pm_sandbox_decline",
    )
    res, is_replay = service.create_payment("merch_1", "ik_decline_001", req)

    assert is_replay is False
    assert res.status == PaymentStatus.FAILED
    assert res.error_code == "card_declined"


def test_provider_timeout_transitions_to_unknown(service: PaymentService) -> None:
    req = PaymentCreateRequest(
        amount_minor=7500,
        currency="INR",
        payment_method_token="pm_sandbox_timeout",
    )
    res, is_replay = service.create_payment("merch_1", "ik_timeout_001", req)

    assert is_replay is False
    assert res.status == PaymentStatus.UNKNOWN
    assert res.error_code == "provider_timeout"
