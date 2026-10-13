"""Pydantic validation schemas for payment requests and responses."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from payflow.payments.state_machine import PaymentStatus


class PaymentCreateRequest(BaseModel):
    """Client request schema for creating a payment."""

    amount_minor: int = Field(..., gt=0, description="Amount in integer minor currency units (e.g., 1000 for 10.00)")
    currency: str = Field(..., min_length=3, max_length=3, description="ISO-4217 3-letter currency code")
    payment_method_token: str = Field(..., min_length=1, description="Opaque payment method token (no PAN/CVV)")
    capture_method: str = Field(default="automatic", description="Capture mode: 'automatic' or 'manual'")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary merchant key-value metadata")

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, v: str) -> str:
        return v.upper()

    @field_validator("capture_method")
    @classmethod
    def validate_capture_method(cls, v: str) -> str:
        if v not in ("automatic", "manual"):
            raise ValueError("capture_method must be either 'automatic' or 'manual'")
        return v


class PaymentResponse(BaseModel):
    """Response schema returned to clients representing the authoritative payment state."""

    id: str
    merchant_id: str
    amount_minor: int
    currency: str
    status: PaymentStatus
    payment_method_token: str
    capture_method: str
    metadata: dict[str, Any]
    error_code: str | None = None
    error_message: str | None = None
    provider_reference: str | None = None
    created_at: datetime
    updated_at: datetime


class ErrorResponse(BaseModel):
    """Standardized RFC-compliant error format."""

    error: str
    detail: str
    code: str | None = None


class PaymentCaptureRequest(BaseModel):
    """Request to capture an authorized payment."""

    amount_minor: int | None = Field(default=None, gt=0, description="Optional partial capture amount")


class PaymentRefundRequest(BaseModel):
    """Request to refund a settled payment."""

    amount_minor: int | None = Field(default=None, gt=0, description="Amount to refund (defaults to full remaining)")
    reason: str | None = Field(default="requested_by_customer", description="Reason for refund")


class RefundResponse(BaseModel):
    """Authoritative representation of a completed refund."""

    id: str
    payment_id: str
    merchant_id: str
    amount_minor: int
    currency: str
    reason: str | None = None
    status: str
    created_at: datetime
