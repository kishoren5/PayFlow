from enum import StrEnum


class PaymentStatus(StrEnum):
    CREATED = "CREATED"
    PROCESSING = "PROCESSING"
    AUTHORIZED = "AUTHORIZED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"
    PARTIALLY_REFUNDED = "PARTIALLY_REFUNDED"


ALLOWED_TRANSITIONS = {
    PaymentStatus.CREATED: {PaymentStatus.PROCESSING, PaymentStatus.CANCELLED, PaymentStatus.FAILED},
    PaymentStatus.PROCESSING: {
        PaymentStatus.SUCCEEDED,
        PaymentStatus.AUTHORIZED,
        PaymentStatus.FAILED,
        PaymentStatus.UNKNOWN,
    },
    PaymentStatus.AUTHORIZED: {PaymentStatus.SUCCEEDED, PaymentStatus.CANCELLED, PaymentStatus.FAILED},
    PaymentStatus.UNKNOWN: {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED, PaymentStatus.CANCELLED},
    PaymentStatus.SUCCEEDED: {PaymentStatus.REFUNDED, PaymentStatus.PARTIALLY_REFUNDED},
    PaymentStatus.PARTIALLY_REFUNDED: {PaymentStatus.REFUNDED},
    PaymentStatus.FAILED: set(),
    PaymentStatus.CANCELLED: set(),
    PaymentStatus.REFUNDED: set(),
}


def transition(current: PaymentStatus, target: PaymentStatus) -> PaymentStatus:
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise ValueError(f"invalid payment transition: {current} -> {target}")
    return target
