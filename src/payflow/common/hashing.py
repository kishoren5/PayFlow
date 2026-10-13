"""Canonical request serialization and SHA-256 hashing."""

import hashlib
import json
from typing import Any


def serialize_canonical_json(data: Any) -> str:
    """Serializes arbitrary nested data into deterministic, whitespace-stripped canonical JSON.

    - Dictionary keys are sorted recursively at every nesting level.
    - Separators are strictly without extra spaces (',', ':').
    - Native datatypes (sets, floats, ints, strings) are normalized.
    """

    def _normalize(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _normalize(v) for k, v in sorted(obj.items())}
        if isinstance(obj, (list, tuple, set)):
            return [_normalize(item) for item in obj]
        return obj

    normalized = _normalize(data)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_request_hash(payload: Any) -> str:
    """Computes a deterministic SHA-256 hexadecimal hash of the canonical request payload."""
    if isinstance(payload, (str, bytes)):
        try:
            parsed = json.loads(payload)
            canonical_str = serialize_canonical_json(parsed)
        except Exception:
            # Fallback to direct string/bytes normalization if not valid JSON
            canonical_str = payload if isinstance(payload, str) else payload.decode("utf-8", errors="replace")
    else:
        canonical_str = serialize_canonical_json(payload)

    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()
