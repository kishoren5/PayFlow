# Idempotency and Consistency

A key identifies one intended operation within a defined merchant/account and operation scope. Retention and expiry behavior are part of the API contract.

1. Authenticate and validate request.
2. Canonicalize request and compute a stable hash.
3. Insert the idempotency record using a unique constraint.
4. Existing key + same hash: return stored logical result.
5. Existing key + different hash: return conflict.
6. For a new key, create payment and corresponding outbox event in one DB transaction when appropriate.

A pre-check followed by insert is race-prone. Database uniqueness is authoritative. A database cannot atomically commit with an external provider; use provider idempotency, durable references, unknown-outcome handling, and reconciliation. Do not claim universal exactly-once execution.

Outbox delivery is at-least-once. Deduplicate by stable event ID and make consumer transactions idempotent. Define retention, audit, and privacy policies; never store raw card credentials.
