"""Ledger posting service, invariant enforcement, compensating entries, and rebuildable balances."""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from payflow.common.db import default_engine, get_db_session
from payflow.common.exceptions import LedgerInvariantError
from payflow.ledger.domain import AccountType, calculate_normal_balance
from payflow.ledger.invariants import Posting, validate_balanced
from payflow.ledger.models import (
    AccountBalanceSnapshotRecord,
    JournalTransactionRecord,
    LedgerAccountRecord,
    LedgerEntryRecord,
)


class LedgerService:
    """Production service for immutable financial transactions and balanced journal entries."""

    def __init__(self, engine: Any = None) -> None:
        self.engine = engine or default_engine

    def get_or_create_account(
        self,
        merchant_id: str,
        account_name: str,
        account_type: AccountType | str,
        currency: str,
    ) -> LedgerAccountRecord:
        """Retrieves an existing account or creates it idempotently."""
        account_type_str = str(account_type)
        with get_db_session(self.engine) as session:
            stmt = select(LedgerAccountRecord).where(
                LedgerAccountRecord.merchant_id == merchant_id,
                LedgerAccountRecord.account_name == account_name,
                LedgerAccountRecord.currency == currency,
            )
            acct = session.execute(stmt).scalar_one_or_none()
            if not acct:
                acct = LedgerAccountRecord(
                    merchant_id=merchant_id,
                    account_name=account_name,
                    account_type=account_type_str,
                    currency=currency,
                )
                session.add(acct)
                session.flush()
            return acct

    def post_transaction(
        self,
        source_event_id: str,
        description: str,
        postings: list[Posting],
    ) -> tuple[JournalTransactionRecord, bool]:
        """Validates and atomically posts a balanced double-entry journal transaction.

        Guarantees:
        1. Debits equal credits per currency (invariant check).
        2. Deduplication by unique source_event_id (duplicate events will not double-post).
        3. Updates account balance projections atomically.

        Returns:
            (JournalTransactionRecord, is_replay)
        """
        # Step 1: Invariant validation
        try:
            validate_balanced(postings)
        except ValueError as ex:
            raise LedgerInvariantError(str(ex)) from ex

        with get_db_session(self.engine) as session:
            # Step 2: Idempotency check via unique source_event_id
            stmt = select(JournalTransactionRecord).where(
                JournalTransactionRecord.source_event_id == source_event_id
            )
            existing = session.execute(stmt).scalar_one_or_none()
            if existing:
                return existing, True

            # Step 3: Insert journal transaction header
            txn = JournalTransactionRecord(
                source_event_id=source_event_id,
                description=description,
            )
            session.add(txn)
            session.flush()

            # Step 4: Insert immutable entries and update balance snapshots
            for seq, p in enumerate(postings):
                entry = LedgerEntryRecord(
                    transaction_id=txn.id,
                    account_id=p.account_id,
                    currency=p.currency,
                    debit_minor=p.debit_minor,
                    credit_minor=p.credit_minor,
                    sequence=seq,
                )
                session.add(entry)
                session.flush()

                # Update balance snapshot
                self._update_balance_snapshot(session, p.account_id, p.currency, p.debit_minor, p.credit_minor)

            return txn, False

    def post_compensating_reversal(
        self,
        original_source_event_id: str,
        reversal_source_event_id: str,
        reason: str,
    ) -> tuple[JournalTransactionRecord, bool]:
        """Executes a non-destructive compensating reversal of an earlier journal transaction.

        In double-entry accounting, history is immutable. Mistakes or refunds are corrected
        by creating an equal and opposite transaction rather than editing past entries.
        """
        with get_db_session(self.engine) as session:
            stmt = select(JournalTransactionRecord).where(
                JournalTransactionRecord.source_event_id == original_source_event_id
            )
            original_txn = session.execute(stmt).scalar_one_or_none()
            if not original_txn:
                raise ValueError(f"Original transaction with source_event_id '{original_source_event_id}' not found.")

            # Load entries
            entries_stmt = select(LedgerEntryRecord).where(
                LedgerEntryRecord.transaction_id == original_txn.id
            ).order_by(LedgerEntryRecord.sequence)
            original_entries = session.execute(entries_stmt).scalars().all()

            # Generate exactly inverted postings: debits become credits, credits become debits
            reversed_postings = [
                Posting(
                    account_id=e.account_id,
                    currency=e.currency,
                    debit_minor=e.credit_minor,
                    credit_minor=e.debit_minor,
                )
                for e in original_entries
            ]

        # Post the compensating transaction
        return self.post_transaction(
            source_event_id=reversal_source_event_id,
            description=f"Compensating reversal of {original_source_event_id}: {reason}",
            postings=reversed_postings,
        )

    def rebuild_balances(self) -> dict[str, int]:
        """Reconstructs all account balance projections from the ground up by replaying entries."""
        with get_db_session(self.engine) as session:
            accounts = session.execute(select(LedgerAccountRecord)).scalars().all()
            rebuilt_balances: dict[str, int] = {}

            for acct in accounts:
                entries_stmt = select(LedgerEntryRecord).where(
                    LedgerEntryRecord.account_id == acct.id
                )
                entries = session.execute(entries_stmt).scalars().all()

                tot_debit = sum(e.debit_minor for e in entries)
                tot_credit = sum(e.credit_minor for e in entries)
                net_balance = calculate_normal_balance(acct.account_type, tot_debit, tot_credit)

                snap_stmt = select(AccountBalanceSnapshotRecord).where(
                    AccountBalanceSnapshotRecord.account_id == acct.id
                )
                snap = session.execute(snap_stmt).scalar_one_or_none()
                if not snap:
                    snap = AccountBalanceSnapshotRecord(
                        account_id=acct.id,
                        currency=acct.currency,
                    )
                    session.add(snap)

                snap.total_debit_minor = tot_debit
                snap.total_credit_minor = tot_credit
                snap.balance_minor = net_balance
                rebuilt_balances[acct.id] = net_balance

            return rebuilt_balances

    def get_account_balance(self, account_id: str) -> int:
        """Returns the current projected balance minor for an account."""
        with get_db_session(self.engine) as session:
            stmt = select(AccountBalanceSnapshotRecord).where(
                AccountBalanceSnapshotRecord.account_id == account_id
            )
            snap = session.execute(stmt).scalar_one_or_none()
            return snap.balance_minor if snap else 0

    def _update_balance_snapshot(
        self,
        session: Session,
        account_id: str,
        currency: str,
        debit: int,
        credit: int,
    ) -> None:
        """Internal helper to atomically maintain the running balance projection."""
        acct_stmt = select(LedgerAccountRecord).where(LedgerAccountRecord.id == account_id)
        acct = session.execute(acct_stmt).scalar_one()

        snap_stmt = select(AccountBalanceSnapshotRecord).where(
            AccountBalanceSnapshotRecord.account_id == account_id
        )
        snap = session.execute(snap_stmt).scalar_one_or_none()
        if not snap:
            snap = AccountBalanceSnapshotRecord(
                account_id=account_id,
                currency=currency,
                balance_minor=0,
                total_debit_minor=0,
                total_credit_minor=0,
            )
            session.add(snap)

        snap.total_debit_minor += debit
        snap.total_credit_minor += credit
        snap.balance_minor = calculate_normal_balance(
            acct.account_type, snap.total_debit_minor, snap.total_credit_minor
        )
