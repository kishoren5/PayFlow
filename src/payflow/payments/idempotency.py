"""Idempotency policy, canonical hashing, and database-enforced reservation."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from payflow.common.exceptions import ConcurrentRequestError, IdempotencyConflictError
from payflow.common.hashing import compute_request_hash
from payflow.payments.models import IdempotencyKeyRecord


@dataclass(frozen=True)
class IdempotencyDecision:
    action: str
    detail: str


def compare_request_hashes(stored_hash: str, incoming_hash: str) -> IdempotencyDecision:
    """Pure policy helper; database uniqueness and transaction boundaries remain authoritative."""
    if stored_hash == incoming_hash:
        return IdempotencyDecision("replay", "same key and same canonical request")
    return IdempotencyDecision("conflict", "same key reused with a different request")


def reserve_or_fetch_idempotency(
    session: Session,
    merchant_id: str,
    operation: str,
    idempotency_key: str,
    payload: Any,
) -> tuple[IdempotencyKeyRecord, bool]:
    """Atomically claims an idempotency key in the database or retrieves the existing record.

    Returns:
        (record, is_new):
        - is_new=True: Key successfully claimed. The caller must proceed with payment execution.
        - is_new=False: Key already completed. The caller should replay the cached response.

    Raises:
        IdempotencyConflictError: If the key was already used with a different request payload.
        ConcurrentRequestError: If an identical request is actively processing right now.
    """
    incoming_hash = compute_request_hash(payload)

    # Attempt atomic insert using nested transaction (SAVEPOINT) to handle integrity constraint cleanly
    try:
        with session.begin_nested():
            new_record = IdempotencyKeyRecord(
                merchant_id=merchant_id,
                operation=operation,
                idempotency_key=idempotency_key,
                request_hash=incoming_hash,
                status="IN_PROGRESS",
            )
            session.add(new_record)
            session.flush()
            return new_record, True
    except IntegrityError:
        # Key collision on (merchant_id, operation, idempotency_key)
        pass

    # Fetch existing record outside savepoint
    stmt = select(IdempotencyKeyRecord).where(
        IdempotencyKeyRecord.merchant_id == merchant_id,
        IdempotencyKeyRecord.operation == operation,
        IdempotencyKeyRecord.idempotency_key == idempotency_key,
    )
    existing = session.execute(stmt).scalar_one_or_none()

    if not existing:
        # Extremely rare race where row was deleted right after collision
        raise ConcurrentRequestError("Concurrent reservation race encountered. Please retry.")

    # Validate payload consistency
    decision = compare_request_hashes(existing.request_hash, incoming_hash)
    if decision.action == "conflict":
        raise IdempotencyConflictError(
            f"Idempotency key '{idempotency_key}' was reused with a different request payload."
        )

    if existing.status == "IN_PROGRESS":
        raise ConcurrentRequestError(
            f"A request with idempotency key '{idempotency_key}' is actively being processed."
        )

    return existing, False


def complete_idempotency(
    session: Session,
    record: IdempotencyKeyRecord,
    response_code: int,
    response_body: str,
    payment_id: str,
) -> None:
    """Marks an idempotency record as COMPLETED with its cached response."""
    record.status = "COMPLETED"
    record.response_code = response_code
    record.response_body = response_body
    record.payment_id = payment_id
    session.add(record)
    session.flush()
