# Operations Runbook

## Payment errors rise
Check status distribution, DB connections/lock waits, provider latency, and fraud latency. Treat provider timeouts as potentially unknown outcomes; do not blindly replay charge operations.

## Outbox backlog grows
Check relay health, DB query latency, broker health, publish errors, and oldest event age. Scale only within DB/broker capacity. Verify consumer dedupe before replay.

## Ledger discrepancy
Preserve journal, source event, payment, and provider evidence. Check duplicate references and amount/currency mapping. Record an audited discrepancy; use compensating entries only after review.

## Fraud service unavailable
Check model loading, feature validation, and inference latency. Apply the documented risk policy, monitor fallback decisions, and restore a known model version with smoke tests.

## Release
CI, schema checks, contract tests, and load smoke tests must pass. Verify readiness and dashboards. Keep migrations backward-compatible where possible; do not assume application rollback makes a schema rollback safe.
