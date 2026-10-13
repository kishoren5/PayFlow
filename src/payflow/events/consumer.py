"""Idempotent event consumer with database-enforced deduplication."""

from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from payflow.common.db import default_engine, get_db_session
from payflow.events.models import ProcessedEventRecord
from payflow.events.schemas import DomainEvent


class DeduplicatingConsumer:
    """Wraps any event handler with atomic deduplication tracking."""

    def __init__(
        self,
        consumer_name: str,
        handler: Callable[[DomainEvent], None],
        engine: Any = None,
    ) -> None:
        self.consumer_name = consumer_name
        self.handler = handler
        self.engine = engine or default_engine

    def handle(self, event: DomainEvent) -> bool:
        """Processes an incoming event.

        Returns:
            True if event was processed for the first time.
            False if event was a duplicate delivery and safely skipped.
        """
        with get_db_session(self.engine) as session:
            # 1. Attempt to claim event processing
            try:
                with session.begin_nested():
                    processed = ProcessedEventRecord(
                        event_id=event.event_id,
                        consumer_name=self.consumer_name,
                        status="PROCESSING",
                    )
                    session.add(processed)
                    session.flush()
            except IntegrityError:
                # Event already claimed or processed by this consumer group
                return False

            # 2. Invoke business handler
            self.handler(event)

            # 3. Mark completed
            processed.status = "COMPLETED"
            session.add(processed)
            return True
