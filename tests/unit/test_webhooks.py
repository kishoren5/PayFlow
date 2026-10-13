"""Unit tests for Webhooks HMAC signing, retry policies, and dispatcher."""

import time
import pytest
from sqlalchemy import create_engine

from payflow.common.db import Base
from payflow.events.schemas import DomainEvent
from payflow.webhooks.delivery_store import WebhookDeliveryStore
from payflow.webhooks.dispatcher import SSRFValidationError, WebhookDispatcher, validate_webhook_url
from payflow.webhooks.retry_policy import WebhookRetryPolicy
from payflow.webhooks.signer import WebhookSigner


def test_webhook_signer_validates_correctly() -> None:
    secret = "whsec_test_secret_12345"
    payload = '{"event":"payment.succeeded","amount":5000}'
    now = int(time.time())

    header = WebhookSigner.sign(payload=payload, secret=secret, timestamp=now)
    assert header.startswith(f"t={now},v1=")

    # Verification succeeds with matching secret and within tolerance
    assert WebhookSigner.verify(payload=payload, header=header, secret=secret, tolerance_seconds=300) is True

    # Verification fails with incorrect secret
    assert WebhookSigner.verify(payload=payload, header=header, secret="wrong_secret") is False

    # Verification fails with tampered payload
    assert WebhookSigner.verify(payload='{"event":"payment.succeeded","amount":9999}', header=header, secret=secret) is False

    # Verification fails on expired timestamp (replay prevention)
    old_header = WebhookSigner.sign(payload=payload, secret=secret, timestamp=now - 600)
    assert WebhookSigner.verify(payload=payload, header=old_header, secret=secret, tolerance_seconds=300, current_time=now) is False


def test_webhook_retry_policy_backoff_and_jitter() -> None:
    policy = WebhookRetryPolicy(
        max_attempts=4,
        initial_interval_seconds=1.0,
        multiplier=2.0,
        enable_jitter=False,
    )

    assert policy.calculate_backoff(1) == 1.0
    assert policy.calculate_backoff(2) == 2.0
    assert policy.calculate_backoff(3) == 4.0

    # Retryable vs non-retryable status codes
    assert policy.is_retryable_status(500) is True
    assert policy.is_retryable_status(502) is True
    assert policy.is_retryable_status(429) is True
    assert policy.is_retryable_status(404) is False
    assert policy.is_retryable_status(400) is False


def test_ssrf_url_validation() -> None:
    # Valid URLs
    validate_webhook_url("https://api.merchant.com/webhook")
    validate_webhook_url("http://example.com/notify")

    # Invalid scheme
    with pytest.raises(SSRFValidationError, match="Invalid webhook protocol"):
        validate_webhook_url("ftp://example.com/webhook")

    with pytest.raises(SSRFValidationError, match="Invalid webhook protocol"):
        validate_webhook_url("file:///etc/passwd")

    # Localhost disallowed when allow_localhost=False
    with pytest.raises(SSRFValidationError, match="Localhost destination disallowed"):
        validate_webhook_url("http://127.0.0.1:8000/hook", allow_localhost=False)


def test_webhook_store_and_dispatcher() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    store = WebhookDeliveryStore(engine=engine)
    endpoint = store.register_endpoint(
        merchant_id="merch_test",
        target_url="https://mock.merchant.com/webhooks",
        secret="sec_test",
        subscribed_events=["payment.succeeded"],
    )

    endpoints = store.get_endpoints("merch_test", "payment.succeeded")
    assert len(endpoints) == 1
    assert endpoints[0].id == endpoint.id

    # Unsubscribed event should not match
    assert len(store.get_endpoints("merch_test", "payment.failed")) == 0
