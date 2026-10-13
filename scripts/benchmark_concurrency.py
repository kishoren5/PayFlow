"""PayFlow High-Concurrency & Throughput Benchmark Suite.

Simulates heavy parallel client traffic against the payment engine to measure
transactions per second (TPS), latency percentiles (p50, p95, p99), and verify
strict payment idempotency and ledger invariants under load.
"""

import argparse
import concurrent.futures
import statistics
import sys
import tempfile
import time
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sqlalchemy import select

from payflow.common.db import Base, create_db_engine, get_db_session
from payflow.payments.models import IdempotencyKeyRecord, PaymentRecord
from payflow.payments.provider import SandboxPaymentProvider
from payflow.payments.schemas import PaymentCreateRequest
from payflow.payments.service import PaymentService


def run_benchmark(total_requests: int = 200, concurrency: int = 20, simulate_latency_ms: float = 0.0) -> None:
    print("=" * 80)
    print("       PAYFLOW HIGH-CONCURRENCY PERFORMANCE BENCHMARK")
    print("=" * 80)
    print(f"Total Requests:       {total_requests}")
    print(f"Concurrent Workers:   {concurrency}")
    print(f"Provider Latency:     {simulate_latency_ms:.1f} ms")
    print("-" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "benchmark.db"
        engine = create_db_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(bind=engine)

        service = PaymentService(
            engine=engine,
            provider=SandboxPaymentProvider(simulate_latency_ms=simulate_latency_ms),
        )

        latencies_ms: list[float] = []

        def worker(idx: int) -> float:
            req = PaymentCreateRequest(
                amount_minor=1500 + idx,
                currency="INR",
                payment_method_token="pm_sandbox_success",
            )
            key = f"bench_key_{idx}"
            t0 = time.perf_counter()
            service.create_payment("merchant_bench", key, req)
            return (time.perf_counter() - t0) * 1000.0

        print(f"Executing {total_requests} concurrent payment authorizations across {concurrency} workers...")
        wall_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker, i) for i in range(total_requests)]
            for fut in concurrent.futures.as_completed(futures):
                latencies_ms.append(fut.result())
        wall_elapsed = time.perf_counter() - wall_start

        # Calculate metrics
        tps = total_requests / wall_elapsed
        latencies_sorted = sorted(latencies_ms)
        min_lat = latencies_sorted[0]
        max_lat = latencies_sorted[-1]
        mean_lat = statistics.mean(latencies_sorted)
        p50 = latencies_sorted[int(len(latencies_sorted) * 0.50)]
        p90 = latencies_sorted[int(len(latencies_sorted) * 0.90)]
        p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)]
        p99 = latencies_sorted[int(len(latencies_sorted) * 0.99)]

        # Verify database invariants
        with get_db_session(engine) as session:
            payments = session.execute(select(PaymentRecord)).scalars().all()
            ik_records = session.execute(select(IdempotencyKeyRecord)).scalars().all()

            db_payments_count = len(payments)
            db_idempotency_count = len(ik_records)

        # Print metrics table
        print("\nResults:")
        print(f"  Wall Clock Time:    {wall_elapsed:.3f} s")
        print(f"  Throughput (TPS):   {tps:.1f} trans/sec")
        print(f"  Average Latency:    {mean_lat:.2f} ms")
        print(f"  Min Latency:        {min_lat:.2f} ms")
        print(f"  p50 Latency:        {p50:.2f} ms")
        print(f"  p90 Latency:        {p90:.2f} ms")
        print(f"  p95 Latency:        {p95:.2f} ms")
        print(f"  p99 Latency:        {p99:.2f} ms")
        print(f"  Max Latency:        {max_lat:.2f} ms")
        print("-" * 80)
        print("Database Invariants:")
        print(f"  Payments Created:   {db_payments_count} (expected: {total_requests})")
        print(f"  Idempotency Keys:   {db_idempotency_count} (expected: {total_requests})")
        print(f"  Duplicate Payments: {db_payments_count - total_requests} (0 allowed)")
        print("=" * 80)

        assert db_payments_count == total_requests, "Invariant violation: payment count mismatch!"
        assert db_idempotency_count == total_requests, "Invariant violation: idempotency count mismatch!"

        # Benchmark replay speed
        print("\nBenchmarking 1,000 Idempotent Replay Invariant Requests...")
        replay_key = "bench_key_0"
        replay_req = PaymentCreateRequest(amount_minor=1500, currency="INR", payment_method_token="pm_sandbox_success")
        t_replay_start = time.perf_counter()
        for _ in range(1000):
            res, is_replay = service.create_payment("merchant_bench", replay_key, replay_req)
            assert is_replay is True
        t_replay_elapsed = time.perf_counter() - t_replay_start
        replay_tps = 1000.0 / t_replay_elapsed
        replay_avg_ms = (t_replay_elapsed / 1000.0) * 1000.0

        print(f"  Replay Wall Time:   {t_replay_elapsed:.3f} s (1,000 requests)")
        print(f"  Replay Throughput:  {replay_tps:.1f} req/s")
        print(f"  Replay Avg Latency: {replay_avg_ms:.2f} ms")
        print("=" * 80)
        print("Benchmark completed successfully! All invariants preserved under concurrency.")

        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PayFlow Concurrency Benchmark")
    parser.add_argument("--requests", type=int, default=200, help="Total number of requests")
    parser.add_argument("--concurrency", type=int, default=20, help="Number of concurrent worker threads")
    parser.add_argument("--latency", type=float, default=0.0, help="Simulated provider latency in ms")
    args = parser.parse_args()

    run_benchmark(total_requests=args.requests, concurrency=args.concurrency, simulate_latency_ms=args.latency)
