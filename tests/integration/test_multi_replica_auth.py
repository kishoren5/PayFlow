"""Integration tests for multi-replica stateless authentication, merchant account isolation,
distributed Redis token revocation, and cross-replica verification.
"""

import pytest
from fastapi.testclient import TestClient

from payflow.gateway.app import app
from payflow.auth.security import (
    UserRole,
    create_access_token,
    decode_access_token,
    revoke_token,
    is_token_revoked,
    simulate_multi_replica_verification,
)


def test_login_success_and_jwt_claims():
    client = TestClient(app)
    resp = client.post("/v1/auth/login", json={
        "email": "admin@amazon.in",
        "password": "amazon123",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["user_id"] == "usr_amz_lead"
    assert data["merchant_id"] == "merch_amazon_in"
    assert data["merchant_name"] == "Amazon India Marketplace"
    assert data["role"] == "admin"
    assert data["email"] == "admin@amazon.in"

    # Verify token payload
    user = decode_access_token(data["access_token"])
    assert user is not None
    assert user.merchant_id == "merch_amazon_in"
    assert user.role == UserRole.ADMIN
    assert user.jti is not None


def test_login_invalid_password_returns_401():
    client = TestClient(app)
    resp = client.post("/v1/auth/login", json={
        "email": "admin@amazon.in",
        "password": "wrongpassword999",
    })
    assert resp.status_code == 401
    assert "Invalid email or password" in resp.json()["detail"]


def test_multi_tenant_merchant_isolation():
    client = TestClient(app)
    # Login as Spotify dev
    login_resp = client.post("/v1/auth/login", json={
        "email": "billing@spotify.com",
        "password": "spotify123",
    })
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}", "X-Merchant-ID": "merch_spotify"}

    # 1. Allowed: Accessing own Spotify account
    own_resp = client.get("/v1/payments?limit=5", headers=headers)
    assert own_resp.status_code == 200

    # 2. Denied (403): Attempting to access Amazon India account with Spotify token
    stolen_headers = {"Authorization": f"Bearer {token}", "X-Merchant-ID": "merch_amazon_in"}
    denied_resp = client.get("/v1/payments?limit=5", headers=stolen_headers)
    assert denied_resp.status_code == 403
    assert "Access denied" in denied_resp.json()["detail"]


def test_global_admin_can_access_all_merchants():
    client = TestClient(app)
    # Login as Global SRE Platform Admin
    login_resp = client.post("/v1/auth/login", json={
        "email": "admin@payflow.internal",
        "password": "payflow2026",
    })
    token = login_resp.json()["access_token"]

    # Global Admin can query ALL or specific merchants
    for target in ["ALL", "merch_amazon_in", "merch_spotify", "merch_uber_rides"]:
        resp = client.get("/v1/payments?limit=5", headers={
            "Authorization": f"Bearer {token}",
            "X-Merchant-ID": target,
        })
        assert resp.status_code == 200


def test_distributed_token_revocation_via_redis_blacklist():
    client = TestClient(app)
    login_resp = client.post("/v1/auth/login", json={
        "email": "ops@uber.com",
        "password": "uber123",
    })
    token = login_resp.json()["access_token"]

    # Request works before logout
    me_resp = client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_resp.status_code == 200
    jti = me_resp.json()["jti"]
    assert jti is not None

    # Logout (adds JTI to Redis blacklist)
    logout_resp = client.post("/v1/auth/logout", headers={"Authorization": f"Bearer {token}"})
    assert logout_resp.status_code == 200
    assert logout_resp.json()["status"] == "LOGGED_OUT"

    # Immediately, subsequent request with the same token is rejected across all replicas
    denied_resp = client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert denied_resp.status_code == 401
    assert "revoked" in denied_resp.json()["detail"].lower()


def test_multi_replica_verification_simulation():
    client = TestClient(app)
    login_resp = client.post("/v1/auth/login", json={
        "email": "finance@swiggy.in",
        "password": "swiggy123",
    })
    token = login_resp.json()["access_token"]

    verify_resp = client.post("/v1/auth/verify-replicas", json={"token": token})
    assert verify_resp.status_code == 200
    data = verify_resp.json()
    assert data["status"] == "ALL_REPLICAS_VERIFIED"
    assert data["merchant_id"] == "merch_swiggy"
    assert len(data["replicas"]) == 3
    for r in data["replicas"]:
        assert r["status"] == "VERIFIED_200_OK"
        assert r["stateless_signature_valid"] is True
        assert r["sticky_session_required"] is False
        assert r["database_queries_made"] == 0
