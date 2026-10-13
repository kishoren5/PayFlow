import pytest
from payflow.ledger.invariants import Posting, validate_balanced


def test_balanced_postings_pass() -> None:
    validate_balanced([
        Posting("merchant-clearing", "INR", debit_minor=1000),
        Posting("customer-funds", "INR", credit_minor=1000),
    ])


def test_unbalanced_postings_fail() -> None:
    with pytest.raises(ValueError, match="unbalanced"):
        validate_balanced([
            Posting("a", "INR", debit_minor=1000),
            Posting("b", "INR", credit_minor=900),
        ])


def test_cross_currency_cannot_net() -> None:
    with pytest.raises(ValueError, match="unbalanced"):
        validate_balanced([
            Posting("a", "INR", debit_minor=1000),
            Posting("b", "USD", credit_minor=1000),
        ])
