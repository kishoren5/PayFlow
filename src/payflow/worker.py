"""Dedicated background worker process for PayFlow.

Runs outbox event relaying, asynchronous ledger consumer processing, and webhook dispatching
independently from the synchronous API gateway.
"""

import logging
import os
import signal
import sys
import time
from typing import Any

from payflow.common.db import default_engine, init_db
from payflow.events.broker import create_event_broker
from payflow.events.outbox_relay import OutboxRelay
from payflow.ledger.consumer import LedgerEventConsumer
from payflow.ledger.posting import LedgerService

logging.basicConfig(
    level=os.getenv("PAYFLOW_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("payflow.worker")


class PayFlowWorker:
    """Orchestrates background workers for durable event relay and async consumers."""

    def __init__(self) -> None:
        self.running = False
        init_db(default_engine)

        self.broker = create_event_broker()
        self.ledger_service = LedgerService(engine=default_engine)
        self.ledger_consumer = LedgerEventConsumer(ledger_service=self.ledger_service, engine=default_engine)

        # Wire topic subscriptions
        self.broker.subscribe("payments", "worker_ledger_group", self.ledger_consumer.on_event)

        self.outbox_relay = OutboxRelay(broker=self.broker, engine=default_engine)
        self.poll_interval = float(os.getenv("PAYFLOW_WORKER_POLL_INTERVAL", "0.25"))
        self.batch_size = int(os.getenv("PAYFLOW_WORKER_BATCH_SIZE", "100"))

    def start(self) -> None:
        """Starts the worker processing loop until interrupted."""
        self.running = True
        logger.info(
            "PayFlow background worker started (poll_interval=%.2fs, batch_size=%d)",
            self.poll_interval,
            self.batch_size,
        )

        def _handle_exit(signum: Any, frame: Any) -> None:
            logger.info("Received termination signal (%s). Shutting down worker...", signum)
            self.running = False

        signal.signal(signal.SIGINT, _handle_exit)
        signal.signal(signal.SIGTERM, _handle_exit)

        while self.running:
            try:
                published = self.outbox_relay.process_pending_events(batch_size=self.batch_size)
                if published > 0:
                    logger.debug("Relayed %d pending outbox events", published)
                else:
                    time.sleep(self.poll_interval)
            except Exception as ex:
                logger.error("Error in worker polling loop: %s", ex, exc_info=True)
                time.sleep(1.0)

        logger.info("PayFlow background worker stopped cleanly.")


def main() -> None:
    worker = PayFlowWorker()
    worker.start()


if __name__ == "__main__":
    main()
