# ADR 0002: Database-enforced idempotency

- Status: Proposed
- Context: Concurrent retries must not create duplicate logical operations.
- Decision: Unique key on merchant, operation, and idempotency key; compare canonical request hash.
- Consequences: Requires explicit retention and conflict semantics.
