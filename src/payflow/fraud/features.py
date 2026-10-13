"""Fraud feature extraction and normalization pipeline."""

from dataclasses import dataclass
from typing import Any


@dataclass
class FraudFeatures:
    """Normalized feature vector for risk evaluation."""
    amount_minor: int
    amount_log: float
    is_foreign_currency: float
    token_entropy: float
    velocity_count: int

    def to_vector(self) -> list[float]:
        return [
            float(self.amount_minor),
            self.amount_log,
            self.is_foreign_currency,
            self.token_entropy,
            float(self.velocity_count),
        ]


def extract_features(
    amount_minor: int,
    currency: str,
    payment_method_token: str,
    velocity_count: int = 1,
) -> FraudFeatures:
    """Extracts leakage-free, reproducible features from transaction context."""
    import math

    amount_log = math.log1p(max(amount_minor, 0))
    is_foreign = 1.0 if currency not in ("INR", "USD") else 0.0

    # Calculate token entropy / risk signature
    unique_chars = len(set(payment_method_token))
    token_entropy = unique_chars / max(len(payment_method_token), 1)

    return FraudFeatures(
        amount_minor=amount_minor,
        amount_log=amount_log,
        is_foreign_currency=is_foreign,
        token_entropy=token_entropy,
        velocity_count=velocity_count,
    )
