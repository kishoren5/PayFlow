# Benchmark Methodology

Record commit SHA, CPU/RAM/storage, DB/Kafka settings, replicas, synthetic workload seed, warm-up, duration, concurrency, and failure conditions.

Report throughput, p50/p95/p99 client latency, errors/timeouts by payment outcome, duplicate requests vs duplicate effects, DB pool saturation, outbox lag, Kafka consumer lag, retry/DLQ counts, fraud inference latency/model version, reconciliation runtime, CPU/RSS, and recovery time.

Separate synchronous API latency from eventual ledger posting, webhook delivery, and reconciliation. Save raw results as JSON/CSV and publish the exact command. Do not report only the best run.
