# Reconciliation Design

Import provider records into immutable staging with a source checksum and batch ID. Validate schema and counts, normalize statuses, match exact provider references, and use fallback matching only under documented rules. Classify missing records, duplicates, amount/currency mismatches, stale states, refund mismatches, and missing ledger postings. Persist evidence, first-seen time, severity, and resolution status.

Rerunning the same batch must not duplicate records or discrepancies. Test with known-good rows and injected missing, duplicate, wrong-amount, wrong-currency, and missing-ledger cases. Never silently rewrite historical ledger entries to make reports balance.
