"""Unit tests for canonical request serialization and SHA-256 hashing."""

from payflow.common.hashing import compute_request_hash, serialize_canonical_json


def test_dict_key_ordering_invariance() -> None:
    payload_a = {"amount_minor": 1000, "currency": "INR", "metadata": {"order_id": "123", "tag": "test"}}
    payload_b = {"metadata": {"tag": "test", "order_id": "123"}, "currency": "INR", "amount_minor": 1000}

    assert serialize_canonical_json(payload_a) == serialize_canonical_json(payload_b)
    assert compute_request_hash(payload_a) == compute_request_hash(payload_b)


def test_whitespace_and_formatting_invariance() -> None:
    json_compact = '{"amount_minor":1000,"currency":"INR"}'
    json_spaced = '{\n  "amount_minor": 1000,\n  "currency": "INR"\n}'

    assert compute_request_hash(json_compact) == compute_request_hash(json_spaced)


def test_different_payloads_produce_different_hashes() -> None:
    hash1 = compute_request_hash({"amount_minor": 1000, "currency": "INR"})
    hash2 = compute_request_hash({"amount_minor": 1001, "currency": "INR"})
    assert hash1 != hash2
