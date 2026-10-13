"""Contract tests for PayFlow HTTP API Gateway idempotency semantics."""

from fastapi.testclient import TestClient

from payflow.gateway.app import app

client = TestClient(app)


def test_health_endpoints() -> None:
    live = client.get("/health/live")
    assert live.status_code == 200
    assert live.json() == {"status": "alive"}

    ready = client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


import uuid

def test_payment_idempotent_api_lifecycle() -> None:
    key = f"ik_contract_{uuid.uuid4().hex}"
    payload = {
        "amount_minor": 4500,
        "currency": "INR",
        "payment_method_token": "pm_sandbox_success",
        "capture_method": "automatic",
        "metadata": {"cart_id": "c_99"},
    }

    # 1. First execution -> HTTP 201 Created
    res1 = client.post("/v1/payments", headers={"Idempotency-Key": key}, json=payload)
    assert res1.status_code == 201, res1.text
    data1 = res1.json()
    assert data1["status"] == "SUCCEEDED"
    payment_id = data1["id"]

    # 2. Replay with identical payload -> HTTP 200 OK with identical payment ID
    res2 = client.post("/v1/payments", headers={"Idempotency-Key": key}, json=payload)
    assert res2.status_code == 200, res2.text
    data2 = res2.json()
    assert data2["id"] == payment_id
    assert data2["amount_minor"] == 4500

    # 3. Reuse key with different payload -> HTTP 409 Conflict
    conflict_payload = {**payload, "amount_minor": 9999}
    res3 = client.post("/v1/payments", headers={"Idempotency-Key": key}, json=conflict_payload)
    assert res3.status_code == 409, res3.text
    assert res3.json()["error"] == "idempotency_conflict"

    # 4. Fetch existing payment by ID -> HTTP 200 OK
    res_get = client.get(f"/v1/payments/{payment_id}")
    assert res_get.status_code == 200
    assert res_get.json()["id"] == payment_id

    # 5. Missing idempotency key -> HTTP 422 Unprocessable Entity
    res_missing_key = client.post("/v1/payments", json=payload)
    assert res_missing_key.status_code == 422
