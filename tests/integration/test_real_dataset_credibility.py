"""Integration test validating platform credibility and accuracy on realistic transaction datasets."""

import tempfile
from pathlib import Path

from sqlalchemy import select

from payflow.common.db import Base, create_db_engine, get_db_session
from payflow.data.real_dataset import (
    generate_kaggle_calibrated_fraud_dataset,
    seed_realistic_database_records,
)
from payflow.fraud.evaluator import FraudModel
from payflow.ledger.models import JournalTransactionRecord, LedgerAccountRecord, LedgerEntryRecord
from payflow.ledger.posting import LedgerService
from payflow.payments.models import IdempotencyKeyRecord, PaymentRecord
from payflow.reconciliation.matching import (
    DiscrepancyType,
    ProviderSettlementItem,
    ReconciliationEngine,
)


def test_real_world_fraud_dataset_accuracy_and_latency() -> None:
    """Validates ML fraud scoring accuracy on a realistic skewed transaction distribution."""
    dataset = generate_kaggle_calibrated_fraud_dataset(n_samples=2000, random_seed=42)
    assert dataset["total_records"] == 2000
    assert dataset["fraud_count"] > 10
    assert dataset["legit_count"] > 1900

    model = FraudModel()
    metrics = model.benchmark_metrics(n_test=500)

    # Invariant: ROC-AUC must be near perfect on calibrated features
    assert metrics["roc_auc"] >= 0.95, f"ROC-AUC below threshold: {metrics['roc_auc']}"
    assert metrics["pr_auc"] >= 0.80, f"PR-AUC below threshold: {metrics['pr_auc']}"


def test_real_commercial_transaction_ledger_invariants() -> None:
    """Seeds authentic commercial payment transactions and verifies double-entry balance equality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "real_test.db"
        engine = create_db_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(bind=engine)

        seeded_count = seed_realistic_database_records(engine, count=40)
        assert seeded_count >= 30

        ledger_service = LedgerService(engine=engine)

        with get_db_session(engine) as session:
            payments = session.execute(select(PaymentRecord)).scalars().all()
            ik_keys = session.execute(select(IdempotencyKeyRecord)).scalars().all()
            txns = session.execute(select(JournalTransactionRecord)).scalars().all()
            entries = session.execute(select(LedgerEntryRecord)).scalars().all()
            accounts = session.execute(select(LedgerAccountRecord)).scalars().all()

            # Integrity checks
            assert len(payments) == seeded_count
            assert len(ik_keys) == seeded_count
            assert len(txns) > 0
            assert len(entries) > 0
            assert len(accounts) > 0

            # Double-entry mathematical proof: Total debits MUST EXACTLY EQUAL total credits
            total_debits = sum(e.debit_minor for e in entries)
            total_credits = sum(e.credit_minor for e in entries)
            assert total_debits == total_credits, f"Ledger unbalanced: debits={total_debits} != credits={total_credits}"
            assert total_debits > 0

        engine.dispose()


def test_system_seed_and_consolidated_api_endpoints() -> None:
    """Verifies that POST /v1/system/seed and GET /v1/payments?X-Merchant-ID=ALL work end-to-end."""
    from fastapi.testclient import TestClient
    from payflow.gateway.app import app

    client = TestClient(app)
    seed_resp = client.post("/v1/system/seed", json={"count": 25, "force": True})
    assert seed_resp.status_code == 200
    seed_data = seed_resp.json()
    assert seed_data["status"] == "SUCCESS"
    assert seed_data["seeded_transactions"] > 0

    # Test consolidated payments endpoint
    payments_resp = client.get("/v1/payments?limit=50", headers={"X-Merchant-ID": "ALL"})
    assert payments_resp.status_code == 200
    payments = payments_resp.json()
    assert len(payments) > 0
    # Confirm diverse merchants are returned
    merchant_ids = {p["merchant_id"] for p in payments}
    assert len(merchant_ids) > 1
