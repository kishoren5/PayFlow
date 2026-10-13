import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from payflow.common.db import default_engine, get_db_session
from payflow.events.broker import EventBroker
from payflow.events.schemas import DomainEvent
from payflow.payments.models import OutboxEventRecord

logger = logging.getLogger(__name__)


class OutboxRelay:
    """Polls pending outbox records and durably delivers them to the event broker."""

    def __init__(self, broker: EventBroker, engine: Any = None) -> None:
        self.broker = broker
        self.engine = engine or default_engine
        self._stop_event = threading.Event()
        self._worker_thread: threading.Thread | None = None
        self._total_published: int = 0
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        return self._worker_thread is not None and self._worker_thread.is_alive()

    @property
    def total_published(self) -> int:
        with self._lock:
            return self._total_published

    def process_pending_events(
        self,
        batch_size: int = 50,
        crash_after_publish: bool = False,
    ) -> int:
        """Polls and publishes pending events.

        Args:
            batch_size: Max number of events to process in one pass.
            crash_after_publish: Simulates a process crash/power failure immediately
                after publishing to the broker, before committing the DB status update.

        Returns:
            Number of events successfully published.
        """
        published_count = 0

        with get_db_session(self.engine) as session:
            stmt = (
                select(OutboxEventRecord)
                .where(OutboxEventRecord.status == "PENDING")
                .order_by(OutboxEventRecord.created_at)
                .limit(batch_size)
            )
            records = session.execute(stmt).scalars().all()

            for record in records:
                # 1. Reconstruct domain event
                payload = json.loads(record.payload_json)
                event = DomainEvent(
                    event_id=record.id,
                    event_type=record.event_type,
                    aggregate_type=record.aggregate_type,
                    aggregate_id=record.aggregate_id,
                    payload=payload,
                )

                # 2. Publish to event broker
                topic = f"{record.aggregate_type}s"
                self.broker.publish(topic=topic, event=event)

                # 3. Simulate failure if requested
                if crash_after_publish:
                    raise RuntimeError(
                        "Simulated relay crash: Event was published to broker, but relay died before DB commit!"
                    )

                # 4. Mark published in database
                record.status = "PUBLISHED"
                record.published_at = datetime.now(timezone.utc)
                session.add(record)
                published_count += 1

            if published_count > 0:
                with self._lock:
                    self._total_published += published_count

            return published_count

    def run_worker_loop(
        self,
        poll_interval: float = 0.25,
        batch_size: int = 100,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Continuous polling loop for dedicated worker threads or background processes."""
        stopper = stop_event or self._stop_event
        while not stopper.is_set():
            try:
                count = self.process_pending_events(batch_size=batch_size)
                # If events were processed, check again immediately; otherwise wait for poll_interval
                if count == 0:
                    stopper.wait(timeout=poll_interval)
            except Exception as ex:
                logger.warning("Error during outbox relay pass (will retry): %s", ex)
                stopper.wait(timeout=min(poll_interval * 2, 2.0))

    def start_background_worker(
        self,
        poll_interval: float = 0.25,
        batch_size: int = 100,
    ) -> threading.Thread:
        """Starts asynchronous background polling thread for seamless non-blocking publishing."""
        if self.is_running:
            return self._worker_thread  # type: ignore[return-value]

        self._stop_event.clear()
        self._worker_thread = threading.Thread(
            target=self.run_worker_loop,
            kwargs={
                "poll_interval": poll_interval,
                "batch_size": batch_size,
                "stop_event": self._stop_event,
            },
            name="PayFlow-OutboxRelayWorker",
            daemon=True,
        )
        self._worker_thread.start()
        return self._worker_thread

    def stop_background_worker(self, timeout: float = 3.0) -> None:
        """Signals the background worker to finish and cleanly joins the thread."""
        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
        self._worker_thread = None
