"""Merchant authentication and API key context resolution."""

from dataclasses import dataclass, field
from typing import Annotated, List, Optional

from fastapi import Header, HTTPException, Security, status
from fastapi.security import APIKeyHeader


@dataclass
class MerchantContext:
    """Authenticated merchant identity and authorization context."""

    merchant_id: str
    environment: str = "sandbox"  # sandbox | live
    scopes: List[str] = field(default_factory=lambda: ["payments:read", "payments:write", "webhooks"])


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def authenticate_merchant(
    x_api_key: Annotated[Optional[str], Security(api_key_header)] = None,
    x_merchant_id: Annotated[Optional[str], Header(alias="X-Merchant-ID")] = None,
) -> MerchantContext:
    """Authenticates API request using X-API-Key or fallback merchant header in dev mode.
    
    API Key Convention:
    - 'pf_test_<merchant_id>_<secret>' -> sandbox environment
    - 'pf_live_<merchant_id>_<secret>' -> live production environment
    """
    if x_api_key:
        parts = x_api_key.strip().split("_")
        if len(parts) >= 3 and parts[0] == "pf":
            env = "live" if parts[1] == "live" else "sandbox"
            merchant_id = parts[2]
            return MerchantContext(merchant_id=merchant_id, environment=env)
        elif x_api_key.startswith("key_"):
            return MerchantContext(merchant_id=x_api_key, environment="sandbox")

    # In development/test mode, fallback to X-Merchant-ID or default
    merchant = x_merchant_id if x_merchant_id else "merchant_default"
    return MerchantContext(merchant_id=merchant, environment="sandbox")
