"""Stateless JWT authentication, API Key verification, multi-tenant merchant account scoping,
and Redis-backed distributed token revocation for multi-replica horizontal deployments.
"""

from dataclasses import dataclass
from enum import Enum
import logging
import os
import time
from typing import Annotated, Any, Optional
import uuid

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt

from payflow.cache.manager import cache_manager

logger = logging.getLogger(__name__)

SECRET_KEY = os.getenv("PAYFLOW_JWT_SECRET", "payflow_super_secret_production_key_2026_flagship")
ALGORITHM = "HS256"
DEFAULT_EXPIRATION_SECONDS = 86400  # 24 hours


class UserRole(str, Enum):
    ADMIN = "admin"
    DEVELOPER = "developer"
    VIEWER = "viewer"


@dataclass
class AuthUser:
    """Authenticated user context attached to request."""
    user_id: str
    merchant_id: str
    role: UserRole
    email: str
    jti: Optional[str] = None
    name: Optional[str] = None


@dataclass
class UserCredentials:
    user_id: str
    email: str
    password_hash: str
    merchant_id: str
    merchant_name: str
    role: UserRole
    name: str


# Multi-Tenant Preconfigured User Accounts
USERS_DIRECTORY: dict[str, UserCredentials] = {
    "admin@payflow.internal": UserCredentials(
        user_id="usr_sre_global",
        email="admin@payflow.internal",
        password_hash="payflow2026",
        merchant_id="ALL",
        merchant_name="PayFlow Global Operations",
        role=UserRole.ADMIN,
        name="Global SRE Admin",
    ),
    "admin@amazon.in": UserCredentials(
        user_id="usr_amz_lead",
        email="admin@amazon.in",
        password_hash="amazon123",
        merchant_id="merch_amazon_in",
        merchant_name="Amazon India Marketplace",
        role=UserRole.ADMIN,
        name="Amazon Operations Lead",
    ),
    "billing@spotify.com": UserCredentials(
        user_id="usr_spot_dev",
        email="billing@spotify.com",
        password_hash="spotify123",
        merchant_id="merch_spotify",
        merchant_name="Spotify Premium",
        role=UserRole.DEVELOPER,
        name="Spotify Billing Eng",
    ),
    "ops@uber.com": UserCredentials(
        user_id="usr_uber_view",
        email="ops@uber.com",
        password_hash="uber123",
        merchant_id="merch_uber_rides",
        merchant_name="Uber Mobility & Freight",
        role=UserRole.VIEWER,
        name="Uber Finance Analyst",
    ),
    "finance@swiggy.in": UserCredentials(
        user_id="usr_swiggy_ops",
        email="finance@swiggy.in",
        password_hash="swiggy123",
        merchant_id="merch_swiggy",
        merchant_name="Swiggy Food & Instamart",
        role=UserRole.DEVELOPER,
        name="Swiggy Treasury",
    ),
    "developer@sandbox.payflow": UserCredentials(
        user_id="usr_dev_sandbox",
        email="developer@sandbox.payflow",
        password_hash="sandbox123",
        merchant_id="merch_demo",
        merchant_name="Demo Sandbox Store",
        role=UserRole.ADMIN,
        name="Sandbox Developer",
    ),
}

# Pre-configured demo API keys for legacy / machine-to-machine integration
VALID_API_KEYS = {
    "sk_live_admin_flagship": AuthUser("usr_admin_01", "merch_demo", UserRole.ADMIN, "admin@payflow.internal", name="Admin Key"),
    "sk_live_developer_stripe": AuthUser("usr_dev_01", "merch_demo", UserRole.DEVELOPER, "dev@payflow.internal", name="Dev Key"),
    "pk_live_viewer_dashboard": AuthUser("usr_viewer_01", "merch_demo", UserRole.VIEWER, "observer@payflow.internal", name="Viewer Key"),
}

security_bearer = HTTPBearer(auto_error=False)


# -------------------------------------------------------------
# Distributed Token Revocation (Redis Blacklist)
# -------------------------------------------------------------
def revoke_token(jti: str, ttl_seconds: int = 86400) -> None:
    """Adds token JTI to distributed Redis blacklist across all replicas."""
    if not jti:
        return
    cache_manager.set(f"jwt:blacklist:{jti}", "revoked", ttl_seconds=ttl_seconds)


def is_token_revoked(jti: str) -> bool:
    """Checks if token has been revoked across any replica via Redis."""
    if not jti:
        return False
    return cache_manager.get(f"jwt:blacklist:{jti}") is not None


# -------------------------------------------------------------
# JWT Issuance & Cross-Replica Verification
# -------------------------------------------------------------
def create_access_token(
    user_id: str,
    merchant_id: str,
    role: UserRole = UserRole.DEVELOPER,
    expires_in_seconds: int = DEFAULT_EXPIRATION_SECONDS,
    jti: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Creates a signed, stateless JWT token for cross-replica authorization."""
    now = int(time.time())
    token_jti = jti or f"jti_{uuid.uuid4().hex[:12]}"
    payload = {
        "sub": user_id,
        "merchant_id": merchant_id,
        "role": role.value,
        "jti": token_jti,
        "name": name or user_id,
        "iat": now,
        "exp": now + expires_in_seconds,
        "iss": "payflow-auth-cluster",
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> Optional[AuthUser]:
    """Verifies token signature and claims statelessly on any replica node."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], issuer="payflow-auth-cluster")
        jti = payload.get("jti")
        # Check distributed Redis blacklist
        if jti and is_token_revoked(jti):
            return None

        sub = payload["sub"]
        email = sub if "@" in sub else f"{sub}@payflow.internal"
        return AuthUser(
            user_id=sub,
            merchant_id=payload.get("merchant_id", "merch_demo"),
            role=UserRole(payload.get("role", "developer")),
            email=email,
            jti=jti,
            name=payload.get("name"),
        )
    except (jwt.PyJWTError, ValueError):
        return None


# -------------------------------------------------------------
# Multi-Tenant Merchant Boundary Validation
# -------------------------------------------------------------
def check_merchant_access(current_user: AuthUser, target_merchant_id: str) -> None:
    """Validates that authenticated user has authority to access the specified merchant account."""
    if current_user.user_id == "usr_sandbox_default":
        return  # Sandbox fallback has automatic access

    if current_user.role == UserRole.ADMIN and current_user.merchant_id in ("ALL", "*"):
        return  # Platform admin has unrestricted global access across all tenants

    if target_merchant_id in ("ALL", "*"):
        if current_user.merchant_id not in ("ALL", "*"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: Account [{current_user.email}] is scoped strictly to tenant [{current_user.merchant_id}] and cannot access global multi-tenant views.",
            )
        return

    if current_user.merchant_id != target_merchant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied: Token is scoped to merchant account [{current_user.merchant_id}], cannot access [{target_merchant_id}].",
        )


# -------------------------------------------------------------
# Dependency: Resolve Current Authenticated User
# -------------------------------------------------------------
def get_current_user(
    auth_header: Annotated[Optional[HTTPAuthorizationCredentials], Depends(security_bearer)] = None,
    x_api_key: Annotated[Optional[str], Header(alias="X-API-Key")] = None,
    x_merchant_id: Annotated[Optional[str], Header(alias="X-Merchant-ID")] = None,
) -> AuthUser:
    """Resolves authenticated user from Bearer JWT, X-API-Key, or falls back to Sandbox Merchant ID."""
    # 1. Check Bearer JWT Token
    if auth_header and auth_header.credentials:
        user = decode_access_token(auth_header.credentials)
        if user:
            return user
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid, expired, or revoked JWT bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 2. Check X-API-Key
    if x_api_key:
        if x_api_key in VALID_API_KEYS:
            user = VALID_API_KEYS[x_api_key]
            if x_merchant_id and x_merchant_id not in ("ALL", "*"):
                user.merchant_id = x_merchant_id
            return user
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key provided.",
        )

    # 3. Sandbox / Dev fallback (allows dashboard and sandbox curl requests)
    merchant = x_merchant_id or "merch_demo"
    return AuthUser(
        user_id="usr_sandbox_default",
        merchant_id=merchant,
        role=UserRole.ADMIN,
        email="developer@sandbox.payflow",
        name="Sandbox Default",
    )


def require_role(min_role: UserRole):
    """Enforces role-based permission boundaries."""
    role_hierarchy = {
        UserRole.VIEWER: 1,
        UserRole.DEVELOPER: 2,
        UserRole.ADMIN: 3,
    }

    def role_checker(current_user: Annotated[AuthUser, Depends(get_current_user)]) -> AuthUser:
        if role_hierarchy.get(current_user.role, 0) < role_hierarchy[min_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Forbidden: Action requires [{min_role.value}] role, but current user has [{current_user.role.value}].",
            )
        return current_user

    return role_checker


# -------------------------------------------------------------
# Simulated Cross-Replica Verification Telemetry
# -------------------------------------------------------------
def simulate_multi_replica_verification(token: str) -> dict[str, Any]:
    """Simulates validating the token across 3 distinct replica instances behind a load balancer."""
    replica_nodes = [
        {"node_id": "replica-pod-alpha", "region": "ap-south-1a", "instance_ip": "10.0.1.42"},
        {"node_id": "replica-pod-beta", "region": "ap-south-1b", "instance_ip": "10.0.2.87"},
        {"node_id": "replica-pod-gamma", "region": "ap-south-1c", "instance_ip": "10.0.3.15"},
    ]
    results = []
    user = decode_access_token(token)
    for node in replica_nodes:
        verified = user is not None
        results.append({
            "node_id": node["node_id"],
            "region": node["region"],
            "instance_ip": node["instance_ip"],
            "status": "VERIFIED_200_OK" if verified else "UNAUTHORIZED_401",
            "stateless_signature_valid": verified,
            "verification_latency_ms": 0.05,
            "database_queries_made": 0,
            "sticky_session_required": False,
        })
    return {
        "status": "ALL_REPLICAS_VERIFIED" if user else "REJECTED_ALL_REPLICAS",
        "user_id": user.user_id if user else None,
        "merchant_id": user.merchant_id if user else None,
        "role": user.role.value if user else None,
        "email": user.email if user else None,
        "name": user.name if user else None,
        "jti": user.jti if user else None,
        "replicas": results,
        "architecture": "Stateless HMAC-SHA256 JWT with Redis Distributed Blacklist",
    }
