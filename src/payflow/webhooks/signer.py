"""Cryptographic signing and verification for outbound webhook events."""

import hashlib
import hmac
import time
from typing import Tuple


class WebhookSigner:
    """Signs and verifies webhook payloads using HMAC-SHA256 with timestamp replay prevention."""

    HEADER_NAME = "PayFlow-Signature"

    @classmethod
    def sign(
        cls,
        payload: str | bytes,
        secret: str,
        timestamp: int | None = None,
    ) -> str:
        """Generates canonical signature header formatted as 't={timestamp},v1={signature}'.
        
        Signs canonical payload: f"{timestamp}.{payload_str}".
        """
        if timestamp is None:
            timestamp = int(time.time())

        payload_bytes = payload.encode("utf-8") if isinstance(payload, str) else payload
        secret_bytes = secret.encode("utf-8")

        signed_payload = f"{timestamp}.".encode("utf-8") + payload_bytes
        signature = hmac.new(secret_bytes, signed_payload, hashlib.sha256).hexdigest()

        return f"t={timestamp},v1={signature}"

    @classmethod
    def parse_header(cls, header: str) -> Tuple[int, str]:
        """Parses 't={timestamp},v1={sig}' header into components."""
        parts = header.strip().split(",")
        timestamp_val: int | None = None
        signature_val: str | None = None

        for part in parts:
            if "=" not in part:
                continue
            key, val = part.split("=", 1)
            key = key.strip()
            val = val.strip()
            if key == "t":
                try:
                    timestamp_val = int(val)
                except ValueError:
                    raise ValueError(f"Invalid timestamp in webhook header: {val}")
            elif key == "v1":
                signature_val = val

        if timestamp_val is None or signature_val is None:
            raise ValueError(f"Malformed signature header format: {header}")

        return timestamp_val, signature_val

    @classmethod
    def verify(
        cls,
        payload: str | bytes,
        header: str,
        secret: str,
        tolerance_seconds: int = 300,
        current_time: int | None = None,
    ) -> bool:
        """Verifies signature header against payload and secret with replay attack protection.
        
        Returns True if signature is valid and within tolerance window, False otherwise.
        """
        if current_time is None:
            current_time = int(time.time())

        try:
            timestamp, signature = cls.parse_header(header)
        except ValueError:
            return False

        # Tolerance check to mitigate replay attacks
        if abs(current_time - timestamp) > tolerance_seconds:
            return False

        expected_header = cls.sign(payload=payload, secret=secret, timestamp=timestamp)
        _, expected_signature = cls.parse_header(expected_header)

        return hmac.compare_digest(signature, expected_signature)
