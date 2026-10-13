"""Concurrency stress test: Validates database-level idempotency under multi-threaded races."""

import concurrent.futures
import tempfile
from pathlib import Path

from sqlalchemy import select

from payflow.common.db import Base, create_db_engine, get_db_session
from payflow.common.exceptions import ConcurrentRequestError
from payflow.payments.models import IdempotencyKeyRecord, PaymentRecord
from payflow.payments.provider import SandboxPaymentProvider
from payflow.payments.schemas import PaymentCreateRequest, PaymentResponse
from payflow.payments.service import PaymentService


def test_concurrent_requests_single_logical_payment() -> None:
    """Fires 20 simultaneous threads racing with the exact same Idempotency-Key.

    Guarantees:
    1. Exactly ONE underlying payment record is created in the database.
    2. Zero duplicate charges occur.
    3. All winning/subsequent threads return the identical payment ID.
    """
    # Use a file-based SQLite database with WAL mode so multiple threads share the same file
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "concurrent_test.db"
        engine = create_db_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(bind=engine)

        service = PaymentService(engine=engine, provider=SandboxPaymentProvider(simulate_latency_ms=10.0))

        req = PaymentCreateRequest(
            amount_minor=9900,
            currency="INR",
            payment_method_token="pm_sandbox_success",
            metadata={"source": "concurrency_race"},
        )
        shared_key = "ik_race_condition_test_100"
        num_threads = 20

        results: list[tuple[PaymentResponse, bool]] = []
        in_flight_conflicts = 0

        def worker() -> tuple[PaymentResponse, bool] | str:
            try:
                return service.create_payment("merch_race", shared_key, req)
            except ConcurrentRequestError:
                return "in_flight_conflict"

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker) for _ in range(num_threads)]
            for fut in concurrent.futures.as_completed(futures):
                res = fut.result()
                if res == "in_flight_conflict":
                    in_flight_conflicts += 1
                else:
                    results.append(res)

        # At least one thread must have succeeded in completing
        assert len(results) >= 1

        # Check all successful results share the exact same payment ID
        first_payment_id = results[0][0].id
        for res, _ in results:
            assert res.id == first_payment_id
            assert res.amount_minor == 9900

        # Now query the database directly to inspect invariants
        with get_db_session(engine) as session:
            payments = session.execute(select(PaymentRecord)).scalars().all()
            idempotency_keys = session.execute(select(IdempotencyKeyRecord)).scalars().all()

            # The critical payment invariant: EXACTLY 1 payment was created!
            assert len(payments) == 1, f"Expected 1 payment, found {len(payments)} (duplicate charge detected!)"
            assert payments[0].id == first_payment_id
            assert len(idempotency_keys) == 1
            assert idempotency_keys[0].status == "COMPLETED"

        # Explicitly close all pooled SQLite connections on Windows before deleting tempdir
        engine.dispose()
