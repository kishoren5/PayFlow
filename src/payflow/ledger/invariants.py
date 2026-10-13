from collections import defaultdict
from dataclasses import dataclass
from collections.abc import Iterable


@dataclass(frozen=True)
class Posting:
    account_id: str
    currency: str
    debit_minor: int = 0
    credit_minor: int = 0


def validate_balanced(postings: Iterable[Posting]) -> None:
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    count = 0
    for p in postings:
        count += 1
        if not p.account_id or not p.currency:
            raise ValueError("account_id and currency are required")
        if p.debit_minor < 0 or p.credit_minor < 0:
            raise ValueError("posting amounts must be non-negative")
        if (p.debit_minor == 0) == (p.credit_minor == 0):
            raise ValueError("exactly one debit or credit must be positive")
        totals[p.currency][0] += p.debit_minor
        totals[p.currency][1] += p.credit_minor
    if count < 2:
        raise ValueError("a journal transaction requires at least two postings")
    unbalanced = {currency: amounts for currency, amounts in totals.items()
                  if amounts[0] != amounts[1]}
    if unbalanced:
        raise ValueError(f"unbalanced journal transaction: {unbalanced}")
