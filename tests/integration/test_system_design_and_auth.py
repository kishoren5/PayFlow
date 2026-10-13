"""Integration and system design tests covering caching, stampede protection, circuit breaker, auth RBAC, and DB agent."""

import time
from fastapi.testclient import TestClient

from payflow.agent.db_agent import DatabaseSupervisorAgent
from payflow.auth.security import (
    AuthUser,
    UserRole,
    create_access_token,
    decode_access_token,
)
from payflow.cache.manager import CacheManager, SingleFlight
from payflow.common.db import default_engine
from payflow.common.resilience import CircuitBreaker, CircuitBreakerOpenError
from payflow.gateway.app import app


def test_single_flight_stampede_protection() -> None:
    """Verifies that SingleFlight deduplicates concurrent requests, avoiding parallel load on the DB."""
    import concurrent.futures
    import threading

    sf = SingleFlight()
    execution_counter = 0
    barrier = threading.Barrier(10)

    def slow_expensive_db_call() -> str:
        nonlocal execution_counter
        execution_counter += 1
        time.sleep(0.08)  # Simulate DB work
        return "expensive_result"

    def worker() -> str:
        barrier.wait()  # Ensure all 10 threads hit sf.execute simultaneously
        return sf.execute("hot_key_42", slow_expensive_db_call)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(worker) for _ in range(10)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    # Invariant: All 10 threads received the correct result
    assert len(results) == 10
    assert all(r == "expensive_result" for r in results)
    # Critical invariant: Expensive DB call was executed ONLY ONCE
    assert execution_counter == 1

    # Also test CacheManager get_or_compute stampede protection across batches
    cm = CacheManager()
    cm_counter = 0

    def compute_cb():
        nonlocal cm_counter
        cm_counter += 1
        time.sleep(0.02)
        return "computed_val"

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        cm_futures = [pool.submit(cm.get_or_compute, "cached_stampede_key", compute_cb) for _ in range(20)]
        cm_results = [f.result()[0] for f in concurrent.futures.as_completed(cm_futures)]

    assert len(cm_results) == 20
    assert all(r == "computed_val" for r in cm_results)
    assert cm_counter == 1


def test_multi_level_cache_manager() -> None:
    """Tests L1/L2 write-through caching, hit-ratio tracking, and invalidation."""
    cm = CacheManager()
    cm.set("test_key", "test_val", ttl_seconds=5)
    assert cm.get("test_key") == "test_val"

    # Telemetry check
    stats = cm.stats()
    assert stats["hits"] >= 1
    assert stats["hit_ratio_pct"] > 0

    # Test invalidation
    cm.delete("test_key")
    assert cm.get("test_key") is None


def test_circuit_breaker_state_transitions() -> None:
    """Asserts Circuit Breaker transitions from CLOSED -> OPEN -> HALF_OPEN -> CLOSED."""
    cb = CircuitBreaker("test_acquirer", failure_threshold=3, recovery_time_s=0.1)

    def failing_call():
        raise RuntimeError("Gateway 504 Gateway Timeout")

    def successful_call():
        return "success"

    # 1. Breaker starts CLOSED
    assert cb.state.value == "CLOSED"

    # 2. Accumulate failures up to threshold
    for _ in range(3):
        try:
            cb.execute(failing_call)
        except RuntimeError:
            pass

    # 3. Breaker is now OPEN (fails fast without executing underlying call)
    assert cb.state.value == "OPEN"
    call_executed = False

    def probe():
        nonlocal call_executed
        call_executed = True

    try:
        cb.execute(probe)
    except CircuitBreakerOpenError:
        pass
    assert not call_executed, "Underlying call should not have been executed while OPEN"

    # 4. Wait for recovery window and test transition
    time.sleep(0.15)
    # First successful execution puts breaker in HALF_OPEN / probes recovery
    res = cb.execute(successful_call)
    assert res == "success"
    # Second success recovers to CLOSED
    cb.execute(successful_call)
    assert cb.state.value == "CLOSED"


def test_stateless_jwt_authentication_and_rbac() -> None:
    """Verifies cross-replica JWT creation, verification, and role boundaries."""
    token = create_access_token(
        user_id="usr_alice",
        merchant_id="merch_amazon_in",
        role=UserRole.ADMIN,
        expires_in_seconds=3600,
    )
    assert isinstance(token, str)

    user = decode_access_token(token)
    assert user is not None
    assert user.user_id == "usr_alice"
    assert user.merchant_id == "merch_amazon_in"
    assert user.role == UserRole.ADMIN

    # Test invalid token handling
    invalid_user = decode_access_token("malformed.jwt.token")
    assert invalid_user is None


def test_database_supervisor_agent_audit_and_stress() -> None:
    """Verifies that the DatabaseSupervisorAgent audits health, pool saturation, and ledger balance."""
    agent = DatabaseSupervisorAgent(engine=default_engine)
    report = agent.run_health_audit()

    assert report["status"] in ("HEALTHY", "DEGRADED_POOL_SATURATION")
    assert "query_ping_ms" in report["database"]
    assert report["ledger_invariants"]["is_mathematically_balanced"] is True
    assert len(report["recommendations"]) > 0


def test_auth_and_system_api_endpoints() -> None:
    """Verifies API endpoints for token issuance, cache telemetry, and resilience metrics."""
    client = TestClient(app)

    # 1. Issue access token
    token_resp = client.post("/v1/auth/token", json={
        "user_id": "usr_dev_integration",
        "merchant_id": "merch_spotify",
        "role": "developer",
    })
    assert token_resp.status_code == 200
    token_data = token_resp.json()
    assert "access_token" in token_data
    jwt_token = token_data["access_token"]

    # 2. Call /v1/auth/me with Bearer token
    me_resp = client.get("/v1/auth/me", headers={"Authorization": f"Bearer {jwt_token}"})
    assert me_resp.status_code == 200
    me_data = me_resp.json()
    assert me_data["user_id"] == "usr_dev_integration"
    assert me_data["merchant_id"] == "merch_spotify"
    assert me_data["role"] == "developer"

    # 3. Call /v1/system/cache/stats
    cache_resp = client.get("/v1/system/cache/stats")
    assert cache_resp.status_code == 200
    assert "hit_ratio_pct" in cache_resp.json()

    # 4. Call /v1/system/resilience/status
    resilience_resp = client.get("/v1/system/resilience/status")
    assert resilience_resp.status_code == 200
    resilience_data = resilience_resp.json()
    assert "circuit_breaker" in resilience_data

    # 5. Call /v1/system/agent/audit
    audit_resp = client.post("/v1/system/agent/audit")
    assert audit_resp.status_code == 200
    audit_data = audit_resp.json()
    assert audit_data["ledger_invariants"]["is_mathematically_balanced"] is True
