# Ledger Invariants

1. Posted journal transactions are immutable.
2. Debits equal credits for each journal transaction and currency.
3. Each posting references a valid account and currency.
4. Each source event can create at most one intended journal transaction.
5. Failed validation leaves no partial postings.
6. Corrections use compensating entries linked to the original.
7. Balance projections can be rebuilt and compared with stored balances.
8. Reconciliation differences are recorded, never silently discarded.

Use integer minor units and validate currency exponent rules. Never net different currencies without a separately modeled FX transaction.
