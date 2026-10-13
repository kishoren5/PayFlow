# ADR 0001: Transactional outbox

- Status: Proposed
- Context: PostgreSQL state and Kafka publication cannot be committed atomically across systems.
- Decision: Write payment transition and outbox row in one DB transaction, then publish asynchronously.
- Consequences: Delivery is at-least-once; consumers must deduplicate and be idempotent.
