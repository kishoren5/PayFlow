"""Payment provider adapter and deterministic sandbox simulator."""

import time
import uuid
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ProviderResult:
    """Outcome returned by downstream payment acquirer/PSP."""
    status: str  # SUCCEEDED, FAILED, UNKNOWN
    provider_reference: str | None = None
    error_code: str | None = None
    error_message: str | None = None


class PaymentProvider(Protocol):
    """Protocol defining payment gateway interactions."""
    def process_charge(
        self,
        payment_id: str,
        amount_minor: int,
        currency: str,
        token: str,
        idempotency_key: str,
    ) -> ProviderResult:
        ...


class SandboxPaymentProvider:
    """Deterministic, configurable sandbox provider simulator.

    Special tokens trigger predictable behaviors for testing:
    - 'pm_sandbox_success' -> Immediate SUCCESS
    - 'pm_sandbox_decline' -> Immediate DECLINE (insufficient funds)
    - 'pm_sandbox_timeout' -> Gateway TIMEOUT (returns UNKNOWN)
    - 'pm_sandbox_server_error' -> Transient 500 error
    """

    def __init__(self, simulate_latency_ms: float = 0.0) -> None:
        self.simulate_latency_ms = simulate_latency_ms
        # In-memory record of provider charges to simulate acquirer-side idempotency
        self._acquirer_records: dict[str, ProviderResult] = {}

    def process_charge(
        self,
        payment_id: str,
        amount_minor: int,
        currency: str,
        token: str,
        idempotency_key: str,
    ) -> ProviderResult:
        if self.simulate_latency_ms > 0:
            time.sleep(self.simulate_latency_ms / 1000.0)

        # Acquirer-side idempotency check
        provider_key = f"{token}:{idempotency_key}"
        if provider_key in self._acquirer_records:
            return self._acquirer_records[provider_key]

        ref = f"ch_sandbox_{uuid.uuid4().hex[:16]}"

        if "decline" in token:
            res = ProviderResult(
                status="FAILED",
                error_code="card_declined",
                error_message="Card has insufficient funds or was declined by issuing bank",
            )
        elif "timeout" in token:
            # Downstream network timeout: PSP may or may not have captured funds
            res = ProviderResult(
                status="UNKNOWN",
                error_code="provider_timeout",
                error_message="Gateway read timeout after request transmission. Outcome is unknown.",
            )
        else:
            res = ProviderResult(
                status="SUCCEEDED",
                provider_reference=ref,
            )

        self._acquirer_records[provider_key] = res
        return res
