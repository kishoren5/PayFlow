"""Tests for double-entry ledger service, balance projections, and compensating entries."""

import pytest
from sqlalchemy import create_engine

from payflow.common.db import Base
from payflow.common.exceptions import LedgerInvariantError
from payflow.ledger.domain import AccountType
from payflow.ledger.invariants import Posting
from payflow.ledger.posting import LedgerService


@pytest.fixture
def ledger_service() -> LedgerService:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return LedgerService(engine=engine)


def test_unbalanced_transaction_rejected(ledger_service: LedgerService) -> None:
    unbalanced_postings = [
        Posting("acct_a", "INR", debit_minor=5000),
        Posting("acct_b", "INR", credit_minor=4000),
    ]
    with pytest.raises(LedgerInvariantError, match="unbalanced"):
        ledger_service.post_transaction("evt_unbal_1", "Invalid unbalanced", unbalanced_postings)


def test_balanced_transaction_and_account_balances(ledger_service: LedgerService) -> None:
    # 1. Setup accounts
    clearing = ledger_service.get_or_create_account("sys", "processor_clearing", AccountType.ASSET, "INR")
    merchant = ledger_service.get_or_create_account("merch_1", "settlement", AccountType.LIABILITY, "INR")
    revenue = ledger_service.get_or_create_account("sys", "platform_fee", AccountType.REVENUE, "INR")

    # Payment: Customer pays 10,000 INR. Merchant gets 9,700 INR. Platform takes 300 INR fee.
    # Debit: Clearing (Asset +10,000)
    # Credit: Merchant (Liability +9,700)
    # Credit: Platform (Revenue +300)
    postings = [
        Posting(clearing.id, "INR", debit_minor=10000),
        Posting(merchant.id, "INR", credit_minor=9700),
        Posting(revenue.id, "INR", credit_minor=300),
    ]

    txn, is_replay = ledger_service.post_transaction("evt_pay_100", "Customer payment settlement", postings)
    assert is_replay is False

    # Check normal balances
    assert ledger_service.get_account_balance(clearing.id) == 10000
    assert ledger_service.get_account_balance(merchant.id) == 9700
    assert ledger_service.get_account_balance(revenue.id) == 300


def test_duplicate_event_does_not_double_post(ledger_service: LedgerService) -> None:
    acct_a = ledger_service.get_or_create_account("sys", "a", AccountType.ASSET, "USD")
    acct_b = ledger_service.get_or_create_account("sys", "b", AccountType.LIABILITY, "USD")

    postings = [
        Posting(acct_a.id, "USD", debit_minor=500),
        Posting(acct_b.id, "USD", credit_minor=500),
    ]

    # First post
    txn1, is_replay1 = ledger_service.post_transaction("evt_dedup_1", "First post", postings)
    assert is_replay1 is False
    assert ledger_service.get_account_balance(acct_a.id) == 500

    # Duplicate post with same source_event_id
    txn2, is_replay2 = ledger_service.post_transaction("evt_dedup_1", "First post", postings)
    assert is_replay2 is True
    assert txn2.id == txn1.id
    # Critical invariant: Balance must NOT be doubled!
    assert ledger_service.get_account_balance(acct_a.id) == 500


def test_compensating_reversal_restores_balance_immutably(ledger_service: LedgerService) -> None:
    clearing = ledger_service.get_or_create_account("sys", "clearing", AccountType.ASSET, "INR")
    merchant = ledger_service.get_or_create_account("merch_rev", "settlement", AccountType.LIABILITY, "INR")

    # Initial payment: 2,000 INR
    postings = [
        Posting(clearing.id, "INR", debit_minor=2000),
        Posting(merchant.id, "INR", credit_minor=2000),
    ]
    ledger_service.post_transaction("evt_orig_200", "Payment", postings)

    assert ledger_service.get_account_balance(clearing.id) == 2000
    assert ledger_service.get_account_balance(merchant.id) == 2000

    # Refund / reversal using compensating entries
    rev_txn, is_replay = ledger_service.post_compensating_reversal(
        original_source_event_id="evt_orig_200",
        reversal_source_event_id="evt_rev_200",
        reason="Customer requested full refund",
    )
    assert is_replay is False

    # Net balances are now safely zeroed out
    assert ledger_service.get_account_balance(clearing.id) == 0
    assert ledger_service.get_account_balance(merchant.id) == 0


def test_rebuild_balances_from_journal_history(ledger_service: LedgerService) -> None:
    clearing = ledger_service.get_or_create_account("sys", "rebuild_clearing", AccountType.ASSET, "USD")
    merchant = ledger_service.get_or_create_account("merch_1", "rebuild_merch", AccountType.LIABILITY, "USD")

    # Post 3 transactions
    for i in range(1, 4):
        ledger_service.post_transaction(
            f"evt_rebuild_{i}",
            f"Txn {i}",
            [
                Posting(clearing.id, "USD", debit_minor=1000 * i),
                Posting(merchant.id, "USD", credit_minor=1000 * i),
            ],
        )

    # Corrupt or wipe balances
    from payflow.common.db import get_db_session
    from payflow.ledger.models import AccountBalanceSnapshotRecord
    with get_db_session(ledger_service.engine) as session:
        for snap in session.query(AccountBalanceSnapshotRecord).all():
            snap.balance_minor = -999999

    # Rebuild from immutable journal entries
    rebuilt = ledger_service.rebuild_balances()

    # Total posted: 1000 + 2000 + 3000 = 6000 USD minor
    assert rebuilt[clearing.id] == 6000
    assert rebuilt[merchant.id] == 6000
    assert ledger_service.get_account_balance(clearing.id) == 6000
