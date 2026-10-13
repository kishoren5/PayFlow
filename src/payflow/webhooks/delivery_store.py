"""Database records and repository for webhook endpoints, jobs, and delivery attempts."""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, select

from payflow.common.db import Base, default_engine, get_db_session


class WebhookEndpointRecord(Base):
    """Registered merchant destination for automated event notification delivery."""

    __tablename__ = "webhook_endpoints"

    id = Column(String(64), primary_key=True)
    merchant_id = Column(String(64), nullable=False, index=True)
    target_url = Column(String(512), nullable=False)
    secret = Column(String(128), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    subscribed_events_json = Column(Text, default='["*"]', nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class WebhookDeliveryJobRecord(Base):
    """Durable delivery queue job tracked across backoff cycles."""

    __tablename__ = "webhook_delivery_jobs"

    id = Column(String(64), primary_key=True)
    endpoint_id = Column(String(64), ForeignKey("webhook_endpoints.id"), nullable=False, index=True)
    merchant_id = Column(String(64), nullable=False, index=True)
    event_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(64), nullable=False)
    payload_json = Column(Text, nullable=False)
    status = Column(String(32), default="PENDING", nullable=False, index=True)  # PENDING, DELIVERED, FAILED, DEAD_LETTER
    attempt_count = Column(Integer, default=0, nullable=False)
    next_retry_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class WebhookDeliveryAttemptRecord(Base):
    """Immutable audit trail of an individual HTTP delivery attempt."""

    __tablename__ = "webhook_delivery_attempts"

    id = Column(String(64), primary_key=True)
    job_id = Column(String(64), ForeignKey("webhook_delivery_jobs.id"), nullable=False, index=True)
    attempt_number = Column(Integer, nullable=False)
    status_code = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    duration_ms = Column(Float, default=0.0, nullable=False)
    error_message = Column(Text, nullable=True)
    attempted_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class WebhookDeliveryStore:
    """Repository managing webhook configurations, queue jobs, and attempt logs."""

    def __init__(self, engine: Any = None) -> None:
        self.engine = engine or default_engine

    def register_endpoint(
        self,
        merchant_id: str,
        target_url: str,
        secret: Optional[str] = None,
        subscribed_events: Optional[List[str]] = None,
    ) -> WebhookEndpointRecord:
        """Registers a destination URL for a merchant."""
        endpoint_id = f"ep_{uuid.uuid4().hex[:16]}"
        ep_secret = secret or f"whsec_{uuid.uuid4().hex}"
        events = subscribed_events or ["*"]

        with get_db_session(self.engine) as session:
            record = WebhookEndpointRecord(
                id=endpoint_id,
                merchant_id=merchant_id,
                target_url=target_url,
                secret=ep_secret,
                is_active=True,
                subscribed_events_json=json.dumps(events),
            )
            session.add(record)
            session.commit()
            session.refresh(record)
            return record

    def get_endpoints(self, merchant_id: str, event_type: str) -> List[WebhookEndpointRecord]:
        """Finds all active endpoints subscribed to an event type for a merchant."""
        with get_db_session(self.engine) as session:
            records = session.execute(
                select(WebhookEndpointRecord).where(
                    WebhookEndpointRecord.merchant_id == merchant_id,
                    WebhookEndpointRecord.is_active == True,
                )
            ).scalars().all()

            matched = []
            for ep in records:
                events = json.loads(ep.subscribed_events_json)
                if "*" in events or event_type in events:
                    matched.append(ep)
            return matched

    def create_delivery_job(
        self,
        endpoint_id: str,
        merchant_id: str,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> WebhookDeliveryJobRecord:
        """Enqueues a persistent delivery job."""
        job_id = f"whj_{uuid.uuid4().hex[:16]}"
        with get_db_session(self.engine) as session:
            job = WebhookDeliveryJobRecord(
                id=job_id,
                endpoint_id=endpoint_id,
                merchant_id=merchant_id,
                event_id=event_id,
                event_type=event_type,
                payload_json=json.dumps(payload, sort_keys=True),
                status="PENDING",
                attempt_count=0,
                next_retry_at=datetime.now(timezone.utc),
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    def record_attempt(
        self,
        job_id: str,
        attempt_number: int,
        status_code: Optional[int],
        response_body: Optional[str],
        duration_ms: float,
        error_message: Optional[str],
        job_status: str,
        next_retry_at: Optional[datetime] = None,
    ) -> WebhookDeliveryAttemptRecord:
        """Appends an attempt log and transitions job status."""
        attempt_id = f"att_{uuid.uuid4().hex[:16]}"
        with get_db_session(self.engine) as session:
            attempt = WebhookDeliveryAttemptRecord(
                id=attempt_id,
                job_id=job_id,
                attempt_number=attempt_number,
                status_code=status_code,
                response_body=response_body[:2048] if response_body else None,
                duration_ms=duration_ms,
                error_message=error_message[:1024] if error_message else None,
            )
            session.add(attempt)

            job = session.get(WebhookDeliveryJobRecord, job_id)
            if job:
                job.attempt_count = attempt_number
                job.status = job_status
                job.next_retry_at = next_retry_at
                job.updated_at = datetime.now(timezone.utc)
                session.add(job)

            session.commit()
            session.refresh(attempt)
            return attempt

    def get_pending_jobs(self, limit: int = 50) -> List[WebhookDeliveryJobRecord]:
        """Fetches jobs eligible for execution."""
        now = datetime.now(timezone.utc)
        with get_db_session(self.engine) as session:
            stmt = (
                select(WebhookDeliveryJobRecord)
                .where(
                    WebhookDeliveryJobRecord.status == "PENDING",
                    WebhookDeliveryJobRecord.next_retry_at <= now,
                )
                .order_by(WebhookDeliveryJobRecord.created_at)
                .limit(limit)
            )
            return list(session.execute(stmt).scalars().all())
