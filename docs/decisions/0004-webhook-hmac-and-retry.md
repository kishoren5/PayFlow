# ADR 0004: Webhook HMAC Signing and Resilient Retries

## Status
Accepted

## Context
Payment events must be communicated to merchants reliably while ensuring integrity, authenticity, and replay resistance. Network failures, receiver downtime, and slow response times require bounded, non-thundering retry handling.

## Decision
1. **HMAC-SHA256 Signatures**: Compute `HMAC-SHA256(secret, f"{timestamp}.{payload}")` formatted as `PayFlow-Signature: t={timestamp},v1={hex_signature}`.
2. **Replay Attack Defense**: Receivers enforce a configurable tolerance window (default 300 seconds) based on header timestamp.
3. **Exponential Backoff with Full Jitter**: Deliveries retry transient errors (HTTP 408, 429, 5xx) with randomized exponential delay `U(0.5 * backoff, backoff)` to prevent thundering herds.
4. **Dead-Letter Queue**: After exceeding max configured attempts, the delivery job transitions to `DEAD_LETTER` for manual inspection and replay.
5. **SSRF Validation**: Validate outbound target URLs to prevent requests to loopback addresses, internal metadata endpoints, or invalid protocols.
