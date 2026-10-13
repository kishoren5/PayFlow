# Contributing

Keep payment and ledger invariants explicit. Use integer minor units. Add concurrency/failure tests for retry-sensitive changes. Never log secrets or payment credentials. Review migrations carefully. Benchmark with fixed workloads and record environment metadata. Do not commit datasets, model artifacts, credentials, Terraform state, or generated secrets.

Before a PR: tests pass; authorization and state transitions are reviewed; idempotency is tested under concurrency; ledger invariants and duplicate-event behavior are tested; docs/runbooks are updated; ML leakage is checked where relevant.
