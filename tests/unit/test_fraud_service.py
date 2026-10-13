"""Tests for ML Fraud risk scoring, decision policies, and benchmark metrics."""

from payflow.fraud.evaluator import FraudModel


def test_fraud_model_allows_legitimate_payment() -> None:
    model = FraudModel()
    decision = model.evaluate(
        amount_minor=1200,
        currency="INR",
        token="pm_card_regular_user",
        velocity=1,
    )
    assert decision.decision == "ALLOW"
    assert decision.score < 0.50
    assert decision.model_version == FraudModel.MODEL_VERSION


def test_fraud_model_declines_anomalous_payment() -> None:
    model = FraudModel()
    decision = model.evaluate(
        amount_minor=950000,  # 9,500.00 (High amount)
        currency="EUR",       # Foreign currency
        token="x",            # Suspicious low entropy token
        velocity=10,          # High velocity
    )
    assert decision.decision in ("REVIEW", "DECLINE")
    assert "HIGH_TRANSACTION_VALUE" in decision.reason_codes
    assert "CROSS_BORDER_CURRENCY" in decision.reason_codes
    assert "HIGH_VELOCITY_SUSPECT" in decision.reason_codes


def test_reproducible_offline_benchmark_metrics() -> None:
    model = FraudModel()
    metrics = model.benchmark_metrics(n_test=500)

    assert "roc_auc" in metrics
    assert "pr_auc" in metrics
    assert metrics["roc_auc"] > 0.80
    assert metrics["samples_evaluated"] == 500
