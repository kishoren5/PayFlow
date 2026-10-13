# PayFlow Load Testing Scenarios & Benchmark Methodology

This directory contains automated k6 and Python concurrency benchmark suites to validate throughput, latency budgets, idempotency enforcement, and system resilience under load.

## Scenarios

### 1. Steady Arrival Rate
- **Target:** 100 - 1,000 req/s sustained over 5 minutes.
- **Goal:** Verify database connection pool stability, CPU utilization, and p95 < 200 ms.
- **Command:**
  ```bash
  k6 run --env SCENARIO=steady load-tests/k6-payments.js
  ```

### 2. Traffic Burst / Surge
- **Target:** Step increase from 50 to 500 req/s in 10 seconds.
- **Goal:** Verify rate limiting throttling (HTTP 429) and ensure database connections do not exhaust under sudden surges.
- **Command:**
  ```bash
  k6 run --env SCENARIO=burst load-tests/k6-payments.js
  ```

### 3. Concurrent Duplicate Race (Idempotency Stress)
- **Target:** 50 virtual users simultaneously sending requests with the exact same `Idempotency-Key`.
- **Invariant:** Exactly 1 HTTP 201 response and 49 HTTP 200/409 responses. Zero duplicate payment records or duplicate charges in the ledger.

### 4. Idempotency Replay Soak
- **Target:** 10,000 repeated queries against established keys.
- **Goal:** Sub-millisecond cached responses, zero database writes, and verify zero ledger pollution.

## Local Python Concurrency Benchmark

When k6 is not installed locally, run the integrated multi-threaded test suite:

```bash
# Basic run: 200 requests, 20 concurrent threads
python scripts/benchmark_concurrency.py --requests 200 --concurrency 20

# Stress run: 500 requests, 50 concurrent threads
python scripts/benchmark_concurrency.py --requests 500 --concurrency 50
```
