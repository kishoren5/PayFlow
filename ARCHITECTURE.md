# PayFlow Architecture

## Payment lifecycle
Suggested states: `CREATED -> PROCESSING -> SUCCEEDED | FAILED | UNKNOWN | CANCELLED`. Optional authorization, capture, refund, and reversal states should be explicit. A provider timeout after transmission is an unknown outcome, not proof of failure.

## Idempotency under concurrency
Use a unique database constraint on `(merchant_id, operation, idempotency_key)`. Store a canonical request hash. Same hash returns the existing logical result; a different hash is a conflict. Create/claim the idempotency record and initial payment state transactionally. Redis may assist rate limiting or caching but is not the durable correctness boundary.

## Outbox and Kafka
Within one PostgreSQL transaction, update payment state and insert an outbox event. A relay publishes pending events. Publication may be duplicated, so consumers deduplicate by event ID and commit business effects idempotently. Use bounded retries, dead-letter handling, and audited replay.

## Ledger
Represent a journal transaction with postings. Debits must equal credits per currency. Posted entries are append-only; corrections are compensating entries. Balance tables are projections and must be rebuildable from the journal. Unique source-event references prevent double posting.

## Fraud
Version features and models. Persist score, model version, threshold policy, decision, and reason codes. Use a strict inference deadline and an explicit risk policy for unavailable/slow inference. The policy may differ by payment class; avoid an undocumented universal fail-open/fail-closed behavior.

## Webhooks
Persist delivery jobs and attempts. Sign canonical bodies with HMAC, include event ID and timestamp, retry transient errors with exponential backoff and jitter, and dead-letter after a configured budget. Delivery is at-least-once; receivers should deduplicate event IDs. Protect outbound requests against SSRF.

## Reconciliation
Import provider data to immutable staging tables with source checksum and batch ID. Match exact provider reference first; use fallback matching only with documented confidence. Persist discrepancy evidence and resolution state. Reruns must be idempotent.

## Failure matrix

| Failure | Expected behavior |
|---|---|
| Client retries after response loss | Return same logical payment for same key |
| Same key, different payload | Conflict; no second payment |
| Provider times out after request | Mark unknown and query/reconcile |
| Outbox relay crashes after publish | Event may duplicate; consumer deduplicates |
| Consumer processes event twice | Unique source reference prevents duplicate effect |
| Fraud exceeds deadline | Apply documented risk policy |
| Webhook unavailable | Retry with backoff, then dead-letter |
| Ledger invariant fails | Reject posting and alert |
| Reconciliation mismatch | Persist discrepancy for investigation |
| Deployment fails readiness | Stop rollout and roll back safely |

## Latency and SLOs
Distinguish synchronous API latency from eventual ledger posting, webhook delivery, and reconciliation latency. Define a workload and latency budget before interpreting p99. Measure provider, database, fraud, queue, and consumer stages separately.
