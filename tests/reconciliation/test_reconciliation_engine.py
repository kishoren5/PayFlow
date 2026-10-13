"""Tests for 3-way reconciliation engine and discrepancy classification."""

import pytest
from sqlalchemy import create_engine

from payflow.common.db import Base
from payflow.payments.provider import SandboxPaymentProvider
from payflow.payments.schemas import PaymentCreateRequest
from payflow.payments.service import PaymentService
from payflow.reconciliation.matching import (
    DiscrepancyType,
    ProviderSettlementItem,
    ReconciliationEngine,
)


@pytest.fixture
def env() -> tuple[PaymentService, ReconciliationEngine]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    payment_service = PaymentService(engine=engine, provider=SandboxPaymentProvider())
    recon_engine = ReconciliationEngine(engine=engine)
    return payment_service, recon_engine


def test_clean_reconciliation_all_matched(env: tuple[PaymentService, ReconciliationEngine]) -> None:
    payment_service, recon_engine = env

    # Process 3 legitimate payments
    p1, _ = payment_service.create_payment("merch_clean", "ik_clean_1", PaymentCreateRequest(
        amount_minor=1000, currency="INR", payment_method_token="pm_sandbox_success"
    ))
    p2, _ = payment_service.create_payment("merch_clean", "ik_clean_2", PaymentCreateRequest(
        amount_minor=2000, currency="INR", payment_method_token="pm_sandbox_success"
    ))

    # Provider report matches completely
    provider_items = [
        ProviderSettlementItem(
            provider_reference=p1.provider_reference or "ref_1",
            payment_id=p1.id,
            amount_minor=1000,
            currency="INR",
            status="CAPTURED",
        ),
        ProviderSettlementItem(
            provider_reference=p2.provider_reference or "ref_2",
            payment_id=p2.id,
            amount_minor=2000,
            currency="INR",
            status="CAPTURED",
        ),
    ]

    summary = recon_engine.reconcile_batch("merch_clean", provider_items)
    assert summary.is_clean is True
    assert summary.matched_count == 2
    assert len(summary.discrepancies) == 0


def test_injected_discrepancies_detected_and_classified(env: tuple[PaymentService, ReconciliationEngine]) -> None:
    payment_service, recon_engine = env

    # Payment A: will have amount mismatch
    p_a, _ = payment_service.create_payment("merch_recon", "ik_recon_a", PaymentCreateRequest(
        amount_minor=5000, currency="INR", payment_method_token="pm_sandbox_success"
    ))
    # Payment B: will have currency mismatch
    p_b, _ = payment_service.create_payment("merch_recon", "ik_recon_b", PaymentCreateRequest(
        amount_minor=3000, currency="INR", payment_method_token="pm_sandbox_success"
    ))
    # Payment C: SUCCEEDED internally, but omitted from provider file
    p_c, _ = payment_service.create_payment("merch_recon", "ik_recon_c", PaymentCreateRequest(
        amount_minor=4000, currency="INR", payment_method_token="pm_sandbox_success"
    ))

    provider_items = [
        # Discrepancy 1: Amount mismatch (internal is 5000, provider says 4200)
        ProviderSettlementItem(
            provider_reference="ref_a",
            payment_id=p_a.id,
            amount_minor=4200,
            currency="INR",
            status="CAPTURED",
        ),
        # Discrepancy 2: Currency mismatch (internal is INR, provider says USD)
        ProviderSettlementItem(
            provider_reference="ref_b",
            payment_id=p_b.id,
            amount_minor=3000,
            currency="USD",
            status="CAPTURED",
        ),
        # Discrepancy 3: Missing in internal DB
        ProviderSettlementItem(
            provider_reference="ref_ghost",
            payment_id="pay_nonexistent_ghost_id",
            amount_minor=9999,
            currency="INR",
            status="CAPTURED",
        ),
        # Discrepancy 4: Duplicate provider charge with same reference
        ProviderSettlementItem(
            provider_reference="ref_a",  # Reusing ref_a!
            payment_id="pay_duplicate_ref",
            amount_minor=1000,
            currency="INR",
            status="CAPTURED",
        ),
    ]

    summary = recon_engine.reconcile_batch("merch_recon", provider_items)

    assert summary.is_clean is False
    disc_types = [d.discrepancy_type for d in summary.discrepancies]

    assert DiscrepancyType.AMOUNT_MISMATCH in disc_types
    assert DiscrepancyType.CURRENCY_MISMATCH in disc_types
    assert DiscrepancyType.MISSING_IN_INTERNAL in disc_types
    assert DiscrepancyType.MISSING_IN_PROVIDER in disc_types
    assert DiscrepancyType.DUPLICATE_PROVIDER_CHARGE in disc_types
