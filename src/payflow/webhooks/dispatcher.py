"""Webhook delivery dispatcher with HMAC signing, retries, and SSRF prevention."""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import httpx

from payflow.events.schemas import DomainEvent
from payflow.webhooks.delivery_store import (
    WebhookDeliveryJobRecord,
    WebhookDeliveryStore,
    WebhookEndpointRecord,
)
from payflow.webhooks.retry_policy import WebhookRetryPolicy
from payflow.webhooks.signer import WebhookSigner


class SSRFValidationError(ValueError):
    """Raised when a webhook URL fails safety validation."""
    pass


def validate_webhook_url(url: str, allow_localhost: bool = True) -> None:
    """Validates URL for outbound webhook delivery, checking scheme and format."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise SSRFValidationError(f"Invalid webhook protocol: {parsed.scheme}. Only HTTP/HTTPS permitted.")

    hostname = parsed.hostname
    if not hostname:
        raise SSRFValidationError("Webhook URL missing hostname.")

    if not allow_localhost and hostname in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
        raise SSRFValidationError(f"Localhost destination disallowed in production mode: {hostname}")


class WebhookDispatcher:
    """Consumes domain events, signs canonical payloads, and reliably delivers webhooks."""

    def __init__(
        self,
        store: WebhookDeliveryStore,
        retry_policy: Optional[WebhookRetryPolicy] = None,
        http_client: Optional[httpx.Client] = None,
        allow_localhost: bool = True,
    ) -> None:
        self.store = store
        self.retry_policy = retry_policy or WebhookRetryPolicy()
        self.client = http_client or httpx.Client(timeout=10.0)
        self.allow_localhost = allow_localhost

    def dispatch_event(self, event: DomainEvent) -> list[str]:
        """Dispatches an incoming domain event to all subscribed merchant webhook endpoints."""
        merchant_id = event.payload.get("merchant_id")
        if not merchant_id:
            return []

        endpoints = self.store.get_endpoints(merchant_id=merchant_id, event_type=event.event_type)
        enqueued_job_ids = []

        for ep in endpoints:
            payload_data = {
                "id": event.event_id,
                "type": event.event_type,
                "created_at": event.occurred_at,
                "data": event.payload,
            }
            job = self.store.create_delivery_job(
                endpoint_id=ep.id,
                merchant_id=merchant_id,
                event_id=event.event_id,
                event_type=event.event_type,
                payload=payload_data,
            )
            enqueued_job_ids.append(job.id)
            # Attempt immediate synchronous delivery
            self.execute_job(job, ep)

        return enqueued_job_ids

    def execute_job(
        self,
        job: WebhookDeliveryJobRecord,
        endpoint: WebhookEndpointRecord,
    ) -> bool:
        """Executes one delivery attempt for a job with HMAC signing and status transition."""
        try:
            validate_webhook_url(endpoint.target_url, allow_localhost=self.allow_localhost)
        except SSRFValidationError as ex:
            self.store.record_attempt(
                job_id=job.id,
                attempt_number=job.attempt_count + 1,
                status_code=None,
                response_body=None,
                duration_ms=0.0,
                error_message=str(ex),
                job_status="DEAD_LETTER",
            )
            return False

        current_attempt = job.attempt_count + 1
        payload_bytes = job.payload_json.encode("utf-8")
        signature_header = WebhookSigner.sign(payload=payload_bytes, secret=endpoint.secret)

        headers = {
            "Content-Type": "application/json",
            WebhookSigner.HEADER_NAME: signature_header,
            "PayFlow-Event-ID": job.event_id,
            "PayFlow-Delivery-ID": job.id,
            "PayFlow-Attempt": str(current_attempt),
            "User-Agent": "PayFlow-Webhook-Dispatcher/1.0",
        }

        t0 = time.perf_counter()
        status_code = None
        response_body = None
        error_msg = None
        is_success = False

        try:
            resp = self.client.post(endpoint.target_url, content=payload_bytes, headers=headers)
            duration_ms = (time.perf_counter() - t0) * 1000.0
            status_code = resp.status_code
            response_body = resp.text

            if 200 <= status_code < 300:
                is_success = True
            else:
                error_msg = f"HTTP status {status_code}"
        except Exception as ex:
            duration_ms = (time.perf_counter() - t0) * 1000.0
            error_msg = str(ex)

        if is_success:
            self.store.record_attempt(
                job_id=job.id,
                attempt_number=current_attempt,
                status_code=status_code,
                response_body=response_body,
                duration_ms=duration_ms,
                error_message=None,
                job_status="DELIVERED",
            )
            return True
        else:
            # Check retry budget
            if current_attempt >= self.retry_policy.max_attempts:
                new_status = "DEAD_LETTER"
                next_retry = None
            else:
                is_retryable = status_code is None or self.retry_policy.is_retryable_status(status_code)
                if is_retryable:
                    new_status = "PENDING"
                    backoff_secs = self.retry_policy.calculate_backoff(current_attempt)
                    next_retry = datetime.now(timezone.utc) + timedelta(seconds=backoff_secs)
                else:
                    new_status = "FAILED"
                    next_retry = None

            self.store.record_attempt(
                job_id=job.id,
                attempt_number=current_attempt,
                status_code=status_code,
                response_body=response_body,
                duration_ms=duration_ms,
                error_message=error_msg,
                job_status=new_status,
                next_retry_at=next_retry,
            )
            return False
