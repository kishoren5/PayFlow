"""Property-based invariants testing for double-entry accounting."""

import random
import pytest
from sqlalchemy import create_engine

from payflow.common.db import Base
from payflow.common.exceptions import LedgerInvariantError
from payflow.ledger.domain import AccountType
from payflow.ledger.invariants import Posting, validate_balanced
from payflow.ledger.posting import LedgerService


def test_property_balanced_postings_always_validate() -> None:
    """Property: Any multi-legged transaction where sum(debits) == sum(credits) must validate."""
    random.seed(42)
    for _ in range(50):
        amount1 = random.randint(1, 10_000_000)
        amount2 = random.randint(1, 10_000_000)
        total = amount1 + amount2

        postings = [
            Posting(account_id="acct_1", currency="INR", debit_minor=amount1),
            Posting(account_id="acct_2", currency="INR", debit_minor=amount2),
            Posting(account_id="acct_3", currency="INR", credit_minor=total),
        ]

        # Must validate without error
        validate_balanced(postings)


def test_property_unbalanced_postings_always_rejected() -> None:
    """Property: Any transaction where sum(debits) != sum(credits) MUST raise an error."""
    random.seed(42)
    for _ in range(50):
        debit = random.randint(1, 1_000_000)
        imbalance = random.randint(1, 100_000)
        credit = debit + imbalance  # Always unbalanced!

        postings = [
            Posting(account_id="acct_debit", currency="INR", debit_minor=debit),
            Posting(account_id="acct_credit", currency="INR", credit_minor=credit),
        ]

        with pytest.raises(ValueError, match="unbalanced journal transaction"):
            validate_balanced(postings)


def test_property_ledger_rebuild_invariance() -> None:
    """Property: Rebuilding balances from scratch yields identical totals to incremental snapshots."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    service = LedgerService(engine=engine)

    acct_a = service.get_or_create_account("merch_prop", "asset_vault", AccountType.ASSET, "INR")
    acct_b = service.get_or_create_account("merch_prop", "payable", AccountType.LIABILITY, "INR")

    for i in range(1, 11):
        amt = i * 1000
        service.post_transaction(
            source_event_id=f"evt_prop_{i}",
            description=f"Transaction {i}",
            postings=[
                Posting(acct_a.id, "INR", debit_minor=amt),
                Posting(acct_b.id, "INR", credit_minor=amt),
            ],
        )

    bal_a_before = service.get_account_balance(acct_a.id)
    bal_b_before = service.get_account_balance(acct_b.id)

    # Rebuild from journal entries
    service.rebuild_balances()

    bal_a_after = service.get_account_balance(acct_a.id)
    bal_b_after = service.get_account_balance(acct_b.id)

    assert bal_a_before == bal_a_after == 55000
    assert bal_b_before == bal_b_after == 55000
