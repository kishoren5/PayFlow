"""Ledger domain types, Chart of Accounts, and accounting classifications."""

from enum import StrEnum


class AccountType(StrEnum):
    """Core standard accounting account classifications."""
    ASSET = "ASSET"          # Resources owned (e.g. processor receivables, bank accounts)
    LIABILITY = "LIABILITY"  # Obligations owed (e.g. merchant payout balance, customer refunds)
    EQUITY = "EQUITY"        # Capital / net worth
    REVENUE = "REVENUE"      # Earned income (e.g. processing fees, markup)
    EXPENSE = "EXPENSE"      # Operational costs (e.g. interchange, network fees)


def calculate_normal_balance(account_type: AccountType | str, total_debit: int, total_credit: int) -> int:
    """Calculates the net balance based on standard double-entry normal balance rules.

    - ASSET / EXPENSE: Normal balance is DEBIT (Balance = Debits - Credits)
    - LIABILITY / EQUITY / REVENUE: Normal balance is CREDIT (Balance = Credits - Debits)
    """
    atype = AccountType(account_type)
    if atype in (AccountType.ASSET, AccountType.EXPENSE):
        return total_debit - total_credit
    return total_credit - total_debit
