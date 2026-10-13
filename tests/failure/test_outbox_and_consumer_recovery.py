"""Failure injection test: Simulates outbox relay crash and verifies consumer deduplication."""

import pytest
from sqlalchemy import create_engine, select

from payflow.common.db import Base, get_db_session
from payflow.events.broker import InMemoryEventBroker
from payflow.events.outbox_relay import OutboxRelay
from payflow.ledger.consumer import LedgerEventConsumer
from payflow.ledger.domain import AccountType
from payflow.ledger.models import JournalTransactionRecord
from payflow.ledger.posting import LedgerService
from payflow.payments.models import OutboxEventRecord
from payflow.payments.provider import SandboxPaymentProvider
from payflow.payments.schemas import PaymentCreateRequest
from payflow.payments.service import PaymentService


def test_outbox_crash_and_duplicate_delivery_recovery() -> None:
    """Proves that a relay crash after Kafka publish does not produce duplicate financial postings.

    Failure sequence:
    1. Payment is processed; payment state and outbox record commit atomically.
    2. Outbox relay publishes event to broker, but CRASHES before marking row PUBLISHED in database.
    3. Relay recovers and replays pending outbox events (producing at-least-once duplicate delivery).
    4. Deduplicating consumer and ledger invariant guard prevent duplicate financial effects.
    5. Balances and journal entries remain 100% correct.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    broker = InMemoryEventBroker()
    payment_service = PaymentService(engine=engine, provider=SandboxPaymentProvider())
    ledger_service = LedgerService(engine=engine)
    ledger_consumer = LedgerEventConsumer(ledger_service=ledger_service, engine=engine)

    # Wire consumer to the broker
    broker.subscribe("payments", "ledger_group", ledger_consumer.on_event)

    # Step 1: Process payment
    req = PaymentCreateRequest(
        amount_minor=10000,
        currency="INR",
        payment_method_token="pm_sandbox_success",
    )
    res, _ = payment_service.create_payment("merch_crash_test", "ik_crash_01", req)

    # Verify outbox event is PENDING
    with get_db_session(engine) as session:
        outbox = session.execute(
            select(OutboxEventRecord).where(OutboxEventRecord.aggregate_id == res.id)
        ).scalar_one()
        assert outbox.status == "PENDING"

    # Step 2: Run Outbox relay with INJECTED CRASH
    relay = OutboxRelay(broker=broker, engine=engine)

    with pytest.raises(RuntimeError, match="Simulated relay crash"):
        relay.process_pending_events(crash_after_publish=True)

    # In database, the outbox record is STILL 'PENDING' because transaction rolled back on crash!
    with get_db_session(engine) as session:
        outbox_after_crash = session.execute(
            select(OutboxEventRecord).where(OutboxEventRecord.aggregate_id == res.id)
        ).scalar_one()
        assert outbox_after_crash.status == "PENDING"

    # But the broker already received 1 delivery!
    assert len(broker.published_history) == 1

    # Step 3: Relay recovers and restarts (normal execution)
    replayed_count = relay.process_pending_events(crash_after_publish=False)
    assert replayed_count == 1

    # Now the broker has received 2 deliveries for the same event (duplicate delivery!)
    assert len(broker.published_history) == 2

    # Step 4: Validate ledger invariants
    with get_db_session(engine) as session:
        # Check outbox is finally marked PUBLISHED
        outbox_final = session.execute(
            select(OutboxEventRecord).where(OutboxEventRecord.aggregate_id == res.id)
        ).scalar_one()
        assert outbox_final.status == "PUBLISHED"

        # Check journal transactions: EXACTLY ONE journal transaction was posted!
        txns = session.execute(select(JournalTransactionRecord)).scalars().all()
        assert len(txns) == 1, f"Expected 1 journal transaction, got {len(txns)} (double-entry was duplicated!)"

    # Check merchant balance: exactly 1 payment (10000 - fee)
    clearing_acct = ledger_service.get_or_create_account("sys", "processor_clearing", AccountType.ASSET, "INR")
    assert ledger_service.get_account_balance(clearing_acct.id) == 10000
