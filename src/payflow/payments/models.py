"""SQLAlchemy ORM models for Payments, Idempotency, and Outbox."""

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from payflow.common.db import Base


def utc_now() -> datetime:
    """Returns current UTC timestamp with timezone information."""
    return datetime.now(timezone.utc)


def generate_prefix_id(prefix: str) -> str:
    """Generates a URL-safe prefixed identifier (e.g., pay_1a2b3c...)."""
    return f"{prefix}_{uuid.uuid4().hex}"


class IdempotencyKeyRecord(Base):
    """Stores client idempotency keys, canonical request hashes, and cached outcomes."""

    __tablename__ = "idempotency_keys"

    id = Column(String(64), primary_key=True, default=lambda: generate_prefix_id("ik"))
    merchant_id = Column(String(64), nullable=False, index=True)
    operation = Column(String(64), nullable=False, index=True)
    idempotency_key = Column(String(128), nullable=False)
    request_hash = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="IN_PROGRESS")  # IN_PROGRESS, COMPLETED, FAILED
    response_code = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    payment_id = Column(String(64), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)
    expires_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("merchant_id", "operation", "idempotency_key", name="uq_idempotency_merchant_op_key"),
        Index("ix_idempotency_lookup", "merchant_id", "operation", "idempotency_key"),
    )


class PaymentRecord(Base):
    """Authoritative ledger-linked payment transaction records."""

    __tablename__ = "payments"

    id = Column(String(64), primary_key=True, default=lambda: generate_prefix_id("pay"))
    merchant_id = Column(String(64), nullable=False, index=True)
    amount_minor = Column(Integer, nullable=False)
    currency = Column(String(3), nullable=False)
    status = Column(String(32), nullable=False, index=True)  # CREATED, PROCESSING, SUCCEEDED, FAILED, UNKNOWN, CANCELLED
    payment_method_token = Column(String(128), nullable=False)
    capture_method = Column(String(32), nullable=False, default="automatic")
    metadata_json = Column(Text, nullable=False, default="{}")
    error_code = Column(String(64), nullable=True)
    error_message = Column(Text, nullable=True)
    idempotency_key = Column(String(128), nullable=True, index=True)
    provider_reference = Column(String(128), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    __table_args__ = (
        CheckConstraint("amount_minor > 0", name="chk_payment_amount_positive"),
        Index("ix_payments_merchant_created", "merchant_id", "created_at"),
    )


class OutboxEventRecord(Base):
    """Transactional outbox table for reliable at-least-once distributed event publishing."""

    __tablename__ = "outbox_events"

    id = Column(String(64), primary_key=True, default=lambda: generate_prefix_id("evt"))
    aggregate_type = Column(String(32), nullable=False, index=True)  # payment, ledger, refund
    aggregate_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(64), nullable=False, index=True)
    payload_json = Column(Text, nullable=False)
    status = Column(String(32), nullable=False, default="PENDING", index=True)  # PENDING, PUBLISHED, FAILED
    retry_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    published_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_outbox_pending", "status", "created_at"),
    )
