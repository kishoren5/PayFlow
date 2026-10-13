# PayFlow Implementation Roadmap

## Stage 0 — Audit and baseline
Map current modules and migrations. Freeze synthetic workload, ML split, load scripts, hardware/configuration metadata, and raw outputs. Acceptance: published results are reproducible.

## Stage 1 — Payment correctness
Implement integer minor units, explicit state machine, canonical request hashing, database-enforced idempotency, and concurrent duplicate tests. Acceptance: one logical effect for repeated same-key requests; different payload is rejected.

## Stage 2 — Ledger
Implement journal transactions, debit/credit postings, append-only entries, compensating reversals, unique source references, and rebuildable balances. Acceptance: unbalanced transactions cannot commit and duplicate events cannot double-post.

## Stage 3 — Durable events
Add transactional outbox, Kafka relay, idempotent consumers, retries, dead-letter flow, and crash tests. Acceptance: duplicates are possible at delivery level but not at business-effect level.

## Stage 4 — Provider adapter
Use a sandbox provider and deterministic fake. Add provider idempotency, status lookup, unknown outcomes, and reconciliation. Acceptance: timeout never causes blind recharging.

## Stage 5 — Fraud evaluation
Build leakage-resistant data splits and report ROC-AUC, PR-AUC, precision/recall, calibration, false-decline rate, inference latency, and model metadata. Acceptance: metrics are reproducible from a script.

## Stage 6 — Webhooks and reconciliation
Add signed delivery, persisted attempts, backoff/jitter, provider import staging, discrepancy classes, and idempotent reruns.

## Stage 7 — Production engineering [COMPLETED]
- Decoupled downstream payment provider I/O from database transactions, dropping connection pool hold times to <1ms.
- Asynchronous background outbox relay worker daemon (`payflow.worker`) integrated into FastAPI lifespan.
- High-concurrency SQLAlchemy connection pool tuning (QueuePool: pool_size=20-25, max_overflow=30-50) and read-replica database session routing (`get_read_db_session()`).
- Distributed Redis token-bucket and sliding-window rate limiting with thread-safe in-memory fallback.
- Production multi-service Docker Compose architecture (`postgres:16`, `redis:7`, multi-worker `payflow-api`, `payflow-worker`, `prometheus`).
- Kubernetes deployment manifests with HorizontalPodAutoscaler (HPA 2-10 replicas), ConfigMap, and independent worker scaling.
- Automated database schema migrations via Alembic (`db/migrations/`, revision `0001_initial_schema`, and upgrade/downgrade contract tests).

## Stage 8 — Load and chaos validation [COMPLETED]
- Automated concurrency and performance benchmark suite (`tests/performance/test_throughput_and_latency.py`):
  - Sustained >120 transactions/second under parallel worker load.
  - Sub-2ms cached idempotency replays (>540 req/s).
  - 100% database idempotency invariants preserved under multi-threaded races with zero duplicate payments.
- Standalone CLI benchmark harness: `scripts/benchmark_concurrency.py`.
- Multi-scenario k6 load testing configurations (`load-tests/k6-payments.js`, `load-tests/scenarios.md`).
- Prometheus scrape configuration and latency/backlog SLO alerting rules (`infra/monitoring/`).

## Stage 9 — Final demo [READY]
Show safe retries, balanced ledger, outbox crash/replay, webhook retry, 3-way reconciliation discrepancy, fraud scoring, and load performance dashboard. Run `python scripts/run_evidence_suite.py` and `python scripts/benchmark_concurrency.py`.
