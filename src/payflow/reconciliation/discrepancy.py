"""Discrepancy taxonomy, persistence, and audit workflow for reconciliation."""

import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional

from sqlalchemy import BigInteger, Column, DateTime, String, Text

from payflow.common.db import Base


class DiscrepancyResolution(StrEnum):
    UNRESOLVED = "UNRESOLVED"
    ADJUST_LEDGER = "ADJUST_LEDGER"
    CONTACT_PROVIDER = "CONTACT_PROVIDER"
    MANUAL_OVERRIDE = "MANUAL_OVERRIDE"
    WRITTEN_OFF = "WRITTEN_OFF"


class ReconciliationDiscrepancyRecord(Base):
    """Authoritative ledger audit record of a financial discrepancy between records."""

    __tablename__ = "reconciliation_discrepancies"

    id = Column(String(64), primary_key=True)
    batch_id = Column(String(64), nullable=True, index=True)
    merchant_id = Column(String(64), nullable=False, index=True)
    discrepancy_type = Column(String(64), nullable=False, index=True)
    payment_id = Column(String(64), nullable=True, index=True)
    provider_reference = Column(String(128), nullable=True, index=True)
    expected_amount_minor = Column(BigInteger, nullable=True)
    actual_amount_minor = Column(BigInteger, nullable=True)
    currency = Column(String(3), nullable=True)
    detail = Column(Text, nullable=False)
    status = Column(String(32), default="DETECTED", nullable=False)  # DETECTED, INVESTIGATING, RESOLVED, WRITTEN_OFF
    resolution_action = Column(String(64), default=DiscrepancyResolution.UNRESOLVED, nullable=False)
    resolution_notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
