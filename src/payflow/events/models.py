"""SQLAlchemy ORM models for event consumer deduplication."""

from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, String, UniqueConstraint

from payflow.common.db import Base


class ProcessedEventRecord(Base):
    """Consumer idempotency table ensuring duplicate broker deliveries produce zero duplicate effects."""

    __tablename__ = "processed_events"

    event_id = Column(String(64), primary_key=True)
    consumer_name = Column(String(64), primary_key=True)
    status = Column(String(32), nullable=False, default="COMPLETED")
    processed_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("event_id", "consumer_name", name="uq_processed_event_consumer"),
    )
