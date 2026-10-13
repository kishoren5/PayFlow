# ADR 0005: 3-Way Financial Reconciliation Architecture

## Status
Accepted

## Context
Payment processing platforms must reconcile internal transaction states, processor/acquirer settlement statements, and double-entry general ledger postings to detect financial leakage, fee deviations, phantom charges, or missing deposits.

## Decision
1. **Immutable Staging Tables**: Acquirer settlement feeds (CSV/JSON) are ingested into immutable staging tables (`settlement_batches` and `settlement_items`).
2. **SHA-256 Batch Checksums**: Unique database constraints on batch payload checksums prevent accidental duplicate re-imports.
3. **3-Way Matching Engine**:
   - Compares internal payment records, settlement items, and ledger postings.
   - Exact matching on provider reference ID (`provider_reference`) and payment ID (`payment_id`).
4. **Structured Discrepancy Taxonomy**:
   - `AMOUNT_MISMATCH`: Net or gross minor units mismatch between internal records and settlement.
   - `CURRENCY_MISMATCH`: Currency code discrepancy.
   - `MISSING_IN_INTERNAL`: Acquirer settled funds for a payment missing from internal database (phantom charge).
   - `MISSING_IN_PROVIDER`: Payment succeeded internally but is absent from settlement feed.
   - `DUPLICATE_PROVIDER_CHARGE`: Same provider charge reference settled multiple times.
5. **Auditable Resolution Workflow**: Discrepancies track state (`DETECTED`, `INVESTIGATING`, `RESOLVED`, `WRITTEN_OFF`) and resolution actions (`ADJUST_LEDGER`, `CONTACT_PROVIDER`).
