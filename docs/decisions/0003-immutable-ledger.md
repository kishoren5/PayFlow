# ADR 0003: Immutable ledger

- Status: Proposed
- Context: Financial history must remain auditable.
- Decision: Posted journal entries are append-only; corrections use compensating entries.
- Consequences: Balance views are projections; reconciliation and rebuild tooling are required.
