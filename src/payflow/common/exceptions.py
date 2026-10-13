"""Domain exceptions for PayFlow platform."""


class PayFlowError(Exception):
    """Base exception for all PayFlow errors."""

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class IdempotencyConflictError(PayFlowError):
    """Raised when an idempotency key is reused with a different canonical payload."""


class ConcurrentRequestError(PayFlowError):
    """Raised when a request with the same idempotency key is actively processing."""


class PaymentNotFoundError(PayFlowError):
    """Raised when the specified payment does not exist."""


class InvalidTransitionError(PayFlowError):
    """Raised when an illegal payment state transition is attempted."""


class LedgerInvariantError(PayFlowError):
    """Raised when ledger postings are unbalanced or violate accounting rules."""


class ProviderTimeoutError(PayFlowError):
    """Raised when a downstream payment provider times out and outcome is unknown."""


class ProviderDeclinedError(PayFlowError):
    """Raised when a downstream payment provider explicitly declines the transaction."""
