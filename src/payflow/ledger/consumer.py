"""Ledger event consumer that posts balanced double-entry entries from payment events."""

from typing import Any

from payflow.events.consumer import DeduplicatingConsumer
from payflow.events.schemas import DomainEvent
from payflow.ledger.domain import AccountType
from payflow.ledger.invariants import Posting
from payflow.ledger.posting import LedgerService


class LedgerEventConsumer:
    """Consumes payment lifecycle events and translates them to immutable ledger journal transactions."""

    def __init__(self, ledger_service: LedgerService, engine: Any = None) -> None:
        self.ledger_service = ledger_service
        self.dedup_consumer = DeduplicatingConsumer(
            consumer_name="ledger_posting_worker",
            handler=self._process_event,
            engine=engine or ledger_service.engine,
        )

    def on_event(self, event: DomainEvent) -> bool:
        return self.dedup_consumer.handle(event)

    def _process_event(self, event: DomainEvent) -> None:
        if event.event_type == "payment.refunded":
            payload = event.payload
            merchant_id = payload["merchant_id"]
            currency = payload["currency"]
            amount_minor = payload["amount_minor"]

            clearing = self.ledger_service.get_or_create_account(
                merchant_id="sys",
                account_name="processor_clearing",
                account_type=AccountType.ASSET,
                currency=currency,
            )
            merchant_settlement = self.ledger_service.get_or_create_account(
                merchant_id=merchant_id,
                account_name="settlement",
                account_type=AccountType.LIABILITY,
                currency=currency,
            )

            postings = [
                Posting(account_id=merchant_settlement.id, currency=currency, debit_minor=amount_minor),
                Posting(account_id=clearing.id, currency=currency, credit_minor=amount_minor),
            ]

            self.ledger_service.post_transaction(
                source_event_id=event.event_id,
                description=f"Automated refund reversal for payment {event.aggregate_id}",
                postings=postings,
            )
            return

        if event.event_type != "payment.succeeded":
            return

        payload = event.payload
        merchant_id = payload["merchant_id"]
        currency = payload["currency"]
        amount_minor = payload["amount_minor"]

        # Default fee structure: 2.9% + 30 minor units
        fee_minor = int(amount_minor * 0.029) + 30
        merchant_net_minor = amount_minor - fee_minor

        # Ensure Chart of Accounts exists
        clearing = self.ledger_service.get_or_create_account(
            merchant_id="sys",
            account_name="processor_clearing",
            account_type=AccountType.ASSET,
            currency=currency,
        )
        merchant_settlement = self.ledger_service.get_or_create_account(
            merchant_id=merchant_id,
            account_name="settlement",
            account_type=AccountType.LIABILITY,
            currency=currency,
        )
        platform_fee = self.ledger_service.get_or_create_account(
            merchant_id="sys",
            account_name="platform_fee_revenue",
            account_type=AccountType.REVENUE,
            currency=currency,
        )

        # Balanced double-entry postings
        postings = [
            Posting(account_id=clearing.id, currency=currency, debit_minor=amount_minor),
            Posting(account_id=merchant_settlement.id, currency=currency, credit_minor=merchant_net_minor),
            Posting(account_id=platform_fee.id, currency=currency, credit_minor=fee_minor),
        ]

        self.ledger_service.post_transaction(
            source_event_id=event.event_id,
            description=f"Automated settlement for payment {event.aggregate_id}",
            postings=postings,
        )
