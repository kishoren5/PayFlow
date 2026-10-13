"""Token-bucket and distributed Redis sliding-window rate limiting for merchant API requests."""

import logging
import os
import threading
import time
from typing import Protocol, Tuple

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


class RateLimiter(Protocol):
    """Protocol defining rate limiting behavior."""

    def check_rate_limit(self, merchant_id: str, cost: float = 1.0) -> Tuple[bool, int, float]:
        """Evaluates whether merchant request is allowed.

        Returns:
            (allowed: bool, remaining_tokens: int, retry_after_sec: float)
        """
        ...


class InMemoryRateLimiter:
    """Thread-safe token bucket rate limiter tracking requests per merchant in memory."""

    def __init__(self, default_rate_per_sec: float = 200.0, burst_capacity: float = 300.0) -> None:
        self.default_rate = default_rate_per_sec
        self.burst_capacity = burst_capacity
        self._buckets: dict[str, tuple[float, float]] = {}  # merchant_id -> (tokens, last_refill_time)
        self._lock = threading.Lock()

    def check_rate_limit(self, merchant_id: str, cost: float = 1.0) -> Tuple[bool, int, float]:
        now = time.monotonic()
        with self._lock:
            tokens, last_time = self._buckets.get(merchant_id, (self.burst_capacity, now))

            # Refill tokens based on elapsed time
            elapsed = now - last_time
            tokens = min(self.burst_capacity, tokens + elapsed * self.default_rate)

            if tokens >= cost:
                tokens -= cost
                self._buckets[merchant_id] = (tokens, now)
                return True, int(tokens), 0.0
            else:
                deficit = cost - tokens
                retry_after = deficit / self.default_rate
                self._buckets[merchant_id] = (tokens, now)
                return False, int(tokens), round(retry_after, 2)


class RedisRateLimiter:
    """Distributed sliding-window rate limiter powered by Redis with in-memory fallback."""

    def __init__(
        self,
        redis_url: str,
        limit_per_minute: int = 6000,
        window_seconds: int = 60,
    ) -> None:
        self.redis_url = redis_url
        self.limit_per_minute = limit_per_minute
        self.window_seconds = window_seconds
        self.fallback = InMemoryRateLimiter()
        self._client: Any = None
        self._init_client()

    def _init_client(self) -> None:
        try:
            import redis
            self._client = redis.Redis.from_url(self.redis_url, socket_timeout=1.0, decode_responses=True)
            self._client.ping()
        except Exception as ex:
            logger.warning("Redis rate limiter unavailable (%s). Falling back to in-memory limiter.", ex)
            self._client = None

    def check_rate_limit(self, merchant_id: str, cost: float = 1.0) -> Tuple[bool, int, float]:
        if self._client is None:
            return self.fallback.check_rate_limit(merchant_id, cost)

        try:
            now = time.time()
            key = f"ratelimit:{merchant_id}:{int(now // self.window_seconds)}"
            pipe = self._client.pipeline()
            pipe.incrby(key, int(cost))
            pipe.expire(key, self.window_seconds + 5)
            results = pipe.execute()

            current_count = results[0]
            if current_count <= self.limit_per_minute:
                remaining = max(0, self.limit_per_minute - current_count)
                return True, remaining, 0.0
            else:
                ttl = self._client.ttl(key)
                retry_after = max(1.0, float(ttl if ttl > 0 else self.window_seconds))
                return False, 0, round(retry_after, 2)
        except Exception as ex:
            logger.warning("Redis operation failed in rate limiter (%s). Using fallback.", ex)
            return self.fallback.check_rate_limit(merchant_id, cost)


def create_rate_limiter() -> RateLimiter:
    """Creates appropriate rate limiter based on environment configuration."""
    redis_url = os.getenv("PAYFLOW_REDIS_URL") or os.getenv("REDIS_URL")
    if redis_url:
        return RedisRateLimiter(redis_url=redis_url)
    return InMemoryRateLimiter(default_rate_per_sec=200.0, burst_capacity=300.0)


rate_limiter: RateLimiter = create_rate_limiter()


def enforce_rate_limit(merchant_id: str) -> None:
    """FastAPI dependency to enforce distributed rate limiting on endpoints."""
    allowed, remaining, retry_after = rate_limiter.check_rate_limit(merchant_id)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded for merchant. Retry after {retry_after}s.",
            headers={"Retry-After": str(int(retry_after) + 1)},
        )
