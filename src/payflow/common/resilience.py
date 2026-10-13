"""Distributed system resilience patterns: Distributed Mutex, Circuit Breaker, and Bulkhead isolation."""

import logging
import os
import threading
import time
from collections.abc import Callable
from enum import Enum
from typing import Any, Optional, TypeVar

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"        # Normal operation: requests pass through
    OPEN = "OPEN"            # Degradation tripped: fail fast immediately
    HALF_OPEN = "HALF_OPEN"  # Testing canary probe traffic


class CircuitBreakerOpenError(Exception):
    """Raised when request is rejected immediately by the open circuit breaker."""
    pass


class CircuitBreaker:
    """Production Circuit Breaker preventing cascading upstream failures and thread starvation.

    When downstream payment providers experience latency or network failure,
    the breaker trips to fail fast, preserving local threads, connection pools, and memory.
    """

    def __init__(
        self,
        name: str = "payment_acquirer_breaker",
        failure_threshold: int = 5,
        recovery_time_s: float = 5.0,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time_s = recovery_time_s
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = time.time()
        self._lock = threading.Lock()

    def execute(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        with self._lock:
            now = time.time()
            if self.state == CircuitState.OPEN:
                if now - self.last_state_change >= self.recovery_time_s:
                    self.state = CircuitState.HALF_OPEN
                    self.last_state_change = now
                    logger.info("CircuitBreaker [%s]: transitioning to HALF_OPEN probe state", self.name)
                else:
                    raise CircuitBreakerOpenError(
                        f"CircuitBreaker [{self.name}] is OPEN. Downstream acquirer degraded, failing fast."
                    )

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except CircuitBreakerOpenError:
            raise
        except Exception as ex:
            self._on_failure()
            raise ex

    def _on_success(self) -> None:
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= 2:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.last_state_change = time.time()
                    logger.info("CircuitBreaker [%s]: recovered, returning to CLOSED state", self.name)
            else:
                self.failure_count = 0

    def _on_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold or self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.last_state_change = time.time()
                logger.warning("CircuitBreaker [%s]: threshold exceeded, tripped to OPEN state", self.name)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self.failure_count,
                "failure_threshold": self.failure_threshold,
                "uptime_seconds": round(time.time() - self.last_state_change, 1),
            }


class DistributedLock:
    """Distributed Mutex using Redis (or fallback thread lock) to coordinate across multi-replica nodes.

    Prevents parallel workers on different pods/nodes from acquiring the same idempotency key concurrently.
    """

    def __init__(self, redis_url: Optional[str] = None) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._local_locks: dict[str, threading.Lock] = {}
        self._local_mutex = threading.Lock()
        self._redis: Any = None
        self._init_redis()

    def _init_redis(self) -> None:
        try:
            import redis
            client = redis.from_url(self.redis_url, socket_timeout=0.2, socket_connect_timeout=0.2)
            client.ping()
            self._redis = client
        except Exception:
            self._redis = None

    def acquire(self, key: str, ttl_seconds: int = 10) -> bool:
        """Acquires lock with lease expiration."""
        lock_key = f"lock:payflow:{key}"
        if self._redis is not None:
            try:
                # SET key val NX EX ttl
                ok = self._redis.set(lock_key, "locked", nx=True, ex=ttl_seconds)
                return bool(ok)
            except Exception:
                pass

        # Fallback to local in-memory lock
        with self._local_mutex:
            if key not in self._local_locks:
                self._local_locks[key] = threading.Lock()
            target_lock = self._local_locks[key]

        return target_lock.acquire(blocking=False)

    def release(self, key: str) -> None:
        """Releases the lock."""
        lock_key = f"lock:payflow:{key}"
        if self._redis is not None:
            try:
                self._redis.delete(lock_key)
            except Exception:
                pass

        with self._local_mutex:
            target_lock = self._local_locks.get(key)
            if target_lock and target_lock.locked():
                try:
                    target_lock.release()
                except RuntimeError:
                    pass


class Bulkhead:
    """Isolates resources between transaction classes to prevent one service from monopolizing resources."""

    def __init__(self, max_concurrent: int = 25) -> None:
        self.semaphore = threading.Semaphore(max_concurrent)
        self.max_concurrent = max_concurrent
        self._active_count = 0
        self._lock = threading.Lock()

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active_count

    def execute(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        acquired = self.semaphore.acquire(blocking=True, timeout=5.0)
        if not acquired:
            raise RuntimeError("Bulkhead capacity saturated; request rejected to protect core OLTP.")
        with self._lock:
            self._active_count += 1
        try:
            return fn(*args, **kwargs)
        finally:
            with self._lock:
                self._active_count -= 1
            self.semaphore.release()


# Global resilience singletons
provider_circuit_breaker = CircuitBreaker("acquirer_sandbox", failure_threshold=4, recovery_time_s=3.0)
distributed_lock = DistributedLock()
report_bulkhead = Bulkhead(max_concurrent=10)
default_bulkhead = Bulkhead(max_concurrent=25)
