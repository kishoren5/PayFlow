from payflow.payments.idempotency import compare_request_hashes


def test_same_payload_is_replay() -> None:
    assert compare_request_hashes("abc", "abc").action == "replay"


def test_different_payload_is_conflict() -> None:
    assert compare_request_hashes("abc", "xyz").action == "conflict"
