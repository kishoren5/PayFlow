import pytest
from payflow.payments.state_machine import PaymentStatus, transition


def test_processing_can_become_unknown() -> None:
    assert transition(PaymentStatus.PROCESSING, PaymentStatus.UNKNOWN) == PaymentStatus.UNKNOWN


def test_terminal_success_cannot_fail_later() -> None:
    with pytest.raises(ValueError, match="invalid payment transition"):
        transition(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
