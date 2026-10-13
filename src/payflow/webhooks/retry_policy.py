"""Retry policies and backoff computation with jitter for webhook dispatching."""

import random
from dataclasses import dataclass


@dataclass
class WebhookRetryPolicy:
    """Configurable exponential backoff policy with randomized jitter."""

    max_attempts: int = 5
    initial_interval_seconds: float = 1.0
    multiplier: float = 2.0
    max_interval_seconds: float = 300.0
    enable_jitter: bool = True

    def calculate_backoff(self, attempt: int) -> float:
        """Calculates backoff delay in seconds for a 1-indexed attempt number.
        
        attempt: 1 for first retry, 2 for second retry, etc.
        """
        if attempt <= 0:
            return 0.0

        backoff = self.initial_interval_seconds * (self.multiplier ** (attempt - 1))
        capped_backoff = min(backoff, self.max_interval_seconds)

        if self.enable_jitter:
            # Full jitter: random uniform between 0 and capped_backoff
            return random.uniform(0.5 * capped_backoff, capped_backoff)

        return capped_backoff

    def is_retryable_status(self, status_code: int) -> bool:
        """Determines whether an HTTP status code indicates a transient failure."""
        if status_code in (408, 429):
            return True
        if 500 <= status_code <= 599:
            return True
        return False
