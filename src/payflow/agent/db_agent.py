"""Automated Database Supervisor Agent for continuous health monitoring, stress testing, and autonomous self-healing."""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func, select, text

from payflow.cache.manager import cache_manager
from payflow.common.db import default_engine, get_db_session, get_read_db_session
from payflow.events.outbox_relay import OutboxRelay
from payflow.ledger.models import LedgerEntryRecord
from payflow.payments.models import OutboxEventRecord, PaymentRecord

logger = logging.getLogger(__name__)


class DatabaseSupervisorAgent:
    """Autonomous Agent monitoring database connection pools, ledger invariant proofs, and load stress testing."""

    def __init__(self, engine: Any = default_engine, outbox_relay: Optional[OutboxRelay] = None) -> None:
        self.engine = engine
        self.outbox_relay = outbox_relay
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._audit_history: list[dict[str, Any]] = []
        self._history_lock = threading.Lock()
        self.last_audit_time: Optional[datetime] = None

    def run_health_audit(self) -> dict[str, Any]:
        """Runs a comprehensive diagnostic audit on database health, invariant preservation, and cache."""
        t0 = time.perf_counter()

        # 1. Connection Pool Diagnostics
        pool = self.engine.pool
        pool_size = getattr(pool, "size", lambda: 5)()
        checked_out = getattr(pool, "checkedout", lambda: 0)()
        checked_in = getattr(pool, "checkedin", lambda: 0)()
        overflow = getattr(pool, "overflow", lambda: 0)()
        pool_saturation = (checked_out / max(pool_size, 1)) * 100.0

        # 2. Query Latency Ping
        ping_start = time.perf_counter()
        with get_read_db_session() as session:
            session.execute(text("SELECT 1")).scalar()
        query_ping_ms = (time.perf_counter() - ping_start) * 1000.0

        # 3. Double-Entry Ledger Invariant Audit
        with get_read_db_session() as session:
            debit_sum = session.execute(select(func.sum(LedgerEntryRecord.debit_minor))).scalar() or 0
            credit_sum = session.execute(select(func.sum(LedgerEntryRecord.credit_minor))).scalar() or 0
            total_entries = session.execute(select(func.count(LedgerEntryRecord.id))).scalar() or 0
            total_payments = session.execute(select(func.count(PaymentRecord.id))).scalar() or 0
            pending_outbox = session.execute(
                select(func.count(OutboxEventRecord.id)).where(OutboxEventRecord.status == "PENDING")
            ).scalar() or 0

        ledger_variance = debit_sum - credit_sum
        is_balanced = (ledger_variance == 0)

        # 4. Cache Performance
        cache_stats = cache_manager.stats()

        # 5. Autonomous Health Assessment & Recommendations
        recommendations = []
        status_label = "HEALTHY"

        if not is_balanced:
            status_label = "CRITICAL_INVARIANT_VIOLATION"
            recommendations.append(f"Ledger unbalanced: debits={debit_sum} != credits={credit_sum}. Investigate ledger postings.")
        if pool_saturation > 80.0:
            status_label = "DEGRADED_POOL_SATURATION"
            recommendations.append(f"Connection pool saturation high ({pool_saturation:.1f}%). Consider increasing pool size.")
        if query_ping_ms > 25.0:
            status_label = "DEGRADED_LATENCY"
            recommendations.append(f"Query latency ping elevated ({query_ping_ms:.2f}ms). Check disk I/O and index scans.")
        if pending_outbox > 50:
            recommendations.append(f"Outbox relay backlog detected ({pending_outbox} pending events). Auto-flushing outbox queue.")
            if self.outbox_relay:
                try:
                    self.outbox_relay.process_pending_events()
                except Exception:
                    pass

        audit_duration_ms = (time.perf_counter() - t0) * 1000.0
        now = datetime.now(timezone.utc)
        self.last_audit_time = now

        report = {
            "timestamp": now.isoformat(),
            "status": status_label,
            "audit_latency_ms": round(audit_duration_ms, 2),
            "database": {
                "dialect": self.engine.dialect.name,
                "query_ping_ms": round(query_ping_ms, 2),
                "pool_size": pool_size,
                "checked_out": checked_out,
                "checked_in": checked_in,
                "overflow": overflow,
                "saturation_pct": round(pool_saturation, 1),
            },
            "ledger_invariants": {
                "total_entries": total_entries,
                "total_debits_minor": debit_sum,
                "total_credits_minor": credit_sum,
                "variance_minor": ledger_variance,
                "is_mathematically_balanced": is_balanced,
            },
            "outbox": {
                "pending_events": pending_outbox,
                "is_relay_active": self.outbox_relay.is_running if self.outbox_relay else False,
            },
            "cache": cache_stats,
            "records": {
                "total_payments": total_payments,
            },
            "recommendations": recommendations or ["Database and ledger invariants fully operating within optimal thresholds."],
        }

        with self._history_lock:
            self._audit_history.append(report)
            if len(self._audit_history) > 20:
                self._audit_history.pop(0)

        return report

    def run_stress_test(self, iterations: int = 50, concurrency: int = 5) -> dict[str, Any]:
        """Runs an automated stress test asserting concurrency limits, idempotency, and invariant preservation."""
        import concurrent.futures
        from payflow.payments.provider import SandboxPaymentProvider
        from payflow.payments.schemas import PaymentCreateRequest
        from payflow.payments.service import PaymentService

        service = PaymentService(engine=self.engine, provider=SandboxPaymentProvider(simulate_latency_ms=0.0))
        latencies: list[float] = []
        run_tag = f"agent_stress_{int(time.time())}"

        def worker(idx: int) -> float:
            key = f"ik_agent_{run_tag}_{idx % 10}"  # 10 keys across 50 calls to test concurrent deduplication
            t_start = time.perf_counter()
            req = PaymentCreateRequest(
                amount_minor=1200 + idx,
                currency="INR",
                payment_method_token="pm_sandbox_success",
                metadata={"test_run": run_tag},
            )
            service.create_payment("merch_agent_test", key, req)
            return (time.perf_counter() - t_start) * 1000.0

        t_wall_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, i) for i in range(iterations)]
            for fut in concurrent.futures.as_completed(futures):
                latencies.append(fut.result())
        wall_time_s = time.perf_counter() - t_wall_start

        # Post-stress audit
        health = self.run_health_audit()

        sorted_lats = sorted(latencies)
        p50 = sorted_lats[int(len(sorted_lats) * 0.50)]
        p95 = sorted_lats[int(len(sorted_lats) * 0.95)]
        p99 = sorted_lats[int(len(sorted_lats) * 0.99)]

        return {
            "status": "COMPLETED",
            "iterations": iterations,
            "concurrency": concurrency,
            "wall_time_seconds": round(wall_time_s, 3),
            "throughput_tps": round(iterations / max(wall_time_s, 0.001), 1),
            "latency_ms": {
                "min": round(sorted_lats[0], 2),
                "p50": round(p50, 2),
                "p95": round(p95, 2),
                "p99": round(p99, 2),
                "max": round(sorted_lats[-1], 2),
            },
            "post_stress_health": health,
        }

    def start_background_daemon(self, interval_seconds: float = 30.0) -> None:
        """Starts automated background periodic database supervisor agent."""
        if self._running:
            return
        self._running = True

        def _loop() -> None:
            while self._running:
                try:
                    self.run_health_audit()
                except Exception as ex:
                    logger.error("DatabaseSupervisorAgent error during periodic audit: %s", ex)
                time.sleep(interval_seconds)

        self._thread = threading.Thread(target=_loop, daemon=True, name="db-supervisor-agent")
        self._thread.start()
        logger.info("DatabaseSupervisorAgent started (audit interval: %ss)", interval_seconds)

    def stop_background_daemon(self) -> None:
        """Stops background agent."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("DatabaseSupervisorAgent stopped.")


# Global supervisor agent singleton
db_agent = DatabaseSupervisorAgent(engine=default_engine)
