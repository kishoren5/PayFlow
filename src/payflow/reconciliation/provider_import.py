"""Immutable staging tables and ingestion pipeline for provider settlement feeds."""

import csv
import hashlib
import io
import json
import uuid
from datetime import datetime, timezone
from typing import Any, List, Tuple

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, select

from payflow.common.db import Base, default_engine, get_db_session
from payflow.reconciliation.matching import ProviderSettlementItem


class DuplicateBatchError(ValueError):
    """Raised when an identical settlement file/batch is imported more than once."""
    pass


class SettlementBatchRecord(Base):
    """Immutable header of an imported provider settlement batch."""

    __tablename__ = "settlement_batches"

    id = Column(String(64), primary_key=True)
    provider_name = Column(String(64), nullable=False)
    merchant_id = Column(String(64), nullable=False, index=True)
    checksum = Column(String(64), nullable=False, unique=True, index=True)
    total_records = Column(Integer, nullable=False)
    total_settled_minor = Column(BigInteger, nullable=False)
    currency = Column(String(3), nullable=False)
    imported_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class SettlementItemRecord(Base):
    """Immutable staging record for an item in a settlement batch."""

    __tablename__ = "settlement_items"

    id = Column(String(64), primary_key=True)
    batch_id = Column(String(64), ForeignKey("settlement_batches.id"), nullable=False, index=True)
    provider_reference = Column(String(128), nullable=False, index=True)
    payment_id = Column(String(64), nullable=False, index=True)
    amount_minor = Column(BigInteger, nullable=False)
    currency = Column(String(3), nullable=False)
    status = Column(String(32), nullable=False)


class ProviderImportService:
    """Parses, verifies checksums, and immutably stages provider settlement files."""

    def __init__(self, engine: Any = None) -> None:
        self.engine = engine or default_engine

    def import_settlement(
        self,
        merchant_id: str,
        provider_name: str,
        raw_content: str | bytes,
        file_format: str = "json",
    ) -> Tuple[SettlementBatchRecord, List[ProviderSettlementItem]]:
        """Parses settlement payload, checks checksum idempotency, and stages records."""
        content_bytes = raw_content.encode("utf-8") if isinstance(raw_content, str) else raw_content
        checksum = hashlib.sha256(content_bytes).hexdigest()

        # Parse items based on format
        items: List[ProviderSettlementItem] = []
        if file_format.lower() == "json":
            data = json.loads(content_bytes.decode("utf-8"))
            if not isinstance(data, list):
                raise ValueError("JSON settlement feed must be an array of settlement items.")
            for row in data:
                items.append(
                    ProviderSettlementItem(
                        provider_reference=row["provider_reference"],
                        payment_id=row["payment_id"],
                        amount_minor=int(row["amount_minor"]),
                        currency=row["currency"],
                        status=row.get("status", "CAPTURED"),
                    )
                )
        elif file_format.lower() == "csv":
            reader = csv.DictReader(io.StringIO(content_bytes.decode("utf-8")))
            for row in reader:
                items.append(
                    ProviderSettlementItem(
                        provider_reference=row["provider_reference"],
                        payment_id=row["payment_id"],
                        amount_minor=int(row["amount_minor"]),
                        currency=row["currency"],
                        status=row.get("status", "CAPTURED"),
                    )
                )
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        if not items:
            raise ValueError("Settlement batch contained zero items.")

        currency = items[0].currency
        total_minor = sum(it.amount_minor for it in items)
        batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

        with get_db_session(self.engine) as session:
            # Check duplicate batch checksum
            existing = session.execute(
                select(SettlementBatchRecord).where(SettlementBatchRecord.checksum == checksum)
            ).scalar_one_or_none()
            if existing:
                raise DuplicateBatchError(
                    f"Settlement batch with checksum {checksum[:12]}... was already imported (batch {existing.id})."
                )

            batch_record = SettlementBatchRecord(
                id=batch_id,
                provider_name=provider_name,
                merchant_id=merchant_id,
                checksum=checksum,
                total_records=len(items),
                total_settled_minor=total_minor,
                currency=currency,
            )
            session.add(batch_record)

            for item in items:
                session.add(
                    SettlementItemRecord(
                        id=f"sitem_{uuid.uuid4().hex[:16]}",
                        batch_id=batch_id,
                        provider_reference=item.provider_reference,
                        payment_id=item.payment_id,
                        amount_minor=item.amount_minor,
                        currency=item.currency,
                        status=item.status,
                    )
                )

            session.commit()
            session.refresh(batch_record)
            return batch_record, items
