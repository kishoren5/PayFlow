"""Webhooks management FastAPI router."""

from typing import Annotated, List, Optional
from pydantic import BaseModel, Field

from fastapi import APIRouter, Header, HTTPException, status

from payflow.common.db import default_engine, get_db_session
from payflow.webhooks.delivery_store import (
    WebhookDeliveryAttemptRecord,
    WebhookDeliveryJobRecord,
    WebhookDeliveryStore,
    WebhookEndpointRecord,
)
from payflow.webhooks.dispatcher import WebhookDispatcher

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])
webhook_store = WebhookDeliveryStore(engine=default_engine)
webhook_dispatcher = WebhookDispatcher(store=webhook_store)


class EndpointCreateRequest(BaseModel):
    target_url: str = Field(..., json_schema_extra={"example": "https://merchant.example.com/webhooks"})
    subscribed_events: Optional[List[str]] = Field(default=["*"], json_schema_extra={"example": ["payment.succeeded", "payment.failed"]})
    secret: Optional[str] = Field(default=None, json_schema_extra={"example": "whsec_custom123"})


class EndpointResponse(BaseModel):
    id: str
    merchant_id: str
    target_url: str
    secret: str
    is_active: bool
    subscribed_events: List[str]


class DeliveryAttemptResponse(BaseModel):
    id: str
    attempt_number: int
    status_code: Optional[int]
    duration_ms: float
    error_message: Optional[str]
    attempted_at: str


class DeliveryJobResponse(BaseModel):
    id: str
    endpoint_id: str
    merchant_id: str
    event_id: str
    event_type: str
    status: str
    attempt_count: int
    next_retry_at: Optional[str]
    created_at: str
    attempts: List[DeliveryAttemptResponse] = []


@router.post("/endpoints", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
def register_webhook_endpoint(
    req: EndpointCreateRequest,
    x_merchant_id: Annotated[str, Header(alias="X-Merchant-ID")] = "merchant_default",
) -> EndpointResponse:
    """Registers a new webhook destination endpoint for the merchant."""
    ep = webhook_store.register_endpoint(
        merchant_id=x_merchant_id,
        target_url=req.target_url,
        secret=req.secret,
        subscribed_events=req.subscribed_events,
    )
    import json
    return EndpointResponse(
        id=ep.id,
        merchant_id=ep.merchant_id,
        target_url=ep.target_url,
        secret=ep.secret,
        is_active=ep.is_active,
        subscribed_events=json.loads(ep.subscribed_events_json),
    )


@router.get("/endpoints", response_model=List[EndpointResponse])
def list_webhook_endpoints(
    x_merchant_id: Annotated[str, Header(alias="X-Merchant-ID")] = "merchant_default",
) -> List[EndpointResponse]:
    """Lists all configured webhook destinations for the merchant."""
    import json
    from sqlalchemy import select
    with get_db_session(default_engine) as session:
        records = session.execute(
            select(WebhookEndpointRecord).where(WebhookEndpointRecord.merchant_id == x_merchant_id)
        ).scalars().all()
        return [
            EndpointResponse(
                id=r.id,
                merchant_id=r.merchant_id,
                target_url=r.target_url,
                secret=r.secret,
                is_active=r.is_active,
                subscribed_events=json.loads(r.subscribed_events_json),
            )
            for r in records
        ]


@router.get("/jobs", response_model=List[DeliveryJobResponse])
def list_webhook_jobs(
    x_merchant_id: Annotated[str, Header(alias="X-Merchant-ID")] = "merchant_default",
    limit: int = 50,
) -> List[DeliveryJobResponse]:
    """Lists recent webhook delivery jobs and their attempt logs."""
    from sqlalchemy import select
    with get_db_session(default_engine) as session:
        jobs = session.execute(
            select(WebhookDeliveryJobRecord)
            .where(WebhookDeliveryJobRecord.merchant_id == x_merchant_id)
            .order_by(WebhookDeliveryJobRecord.created_at.desc())
            .limit(limit)
        ).scalars().all()

        results = []
        for job in jobs:
            attempts = session.execute(
                select(WebhookDeliveryAttemptRecord)
                .where(WebhookDeliveryAttemptRecord.job_id == job.id)
                .order_by(WebhookDeliveryAttemptRecord.attempt_number)
            ).scalars().all()

            results.append(DeliveryJobResponse(
                id=job.id,
                endpoint_id=job.endpoint_id,
                merchant_id=job.merchant_id,
                event_id=job.event_id,
                event_type=job.event_type,
                status=job.status,
                attempt_count=job.attempt_count,
                next_retry_at=job.next_retry_at.isoformat() if job.next_retry_at else None,
                created_at=job.created_at.isoformat(),
                attempts=[
                    DeliveryAttemptResponse(
                        id=a.id,
                        attempt_number=a.attempt_number,
                        status_code=a.status_code,
                        duration_ms=a.duration_ms,
                        error_message=a.error_message,
                        attempted_at=a.attempted_at.isoformat(),
                    )
                    for a in attempts
                ],
            ))
        return results


@router.post("/jobs/{job_id}/retry")
def retry_webhook_job(
    job_id: str,
    x_merchant_id: Annotated[str, Header(alias="X-Merchant-ID")] = "merchant_default",
) -> dict[str, str]:
    """Manually re-triggers delivery of a failed or dead-lettered webhook job."""
    with get_db_session(default_engine) as session:
        job = session.get(WebhookDeliveryJobRecord, job_id)
        if not job or job.merchant_id != x_merchant_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        endpoint = session.get(WebhookEndpointRecord, job.endpoint_id)
        if not endpoint:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Endpoint not found")

    success = webhook_dispatcher.execute_job(job, endpoint)
    return {"job_id": job_id, "result": "DELIVERED" if success else "FAILED"}
