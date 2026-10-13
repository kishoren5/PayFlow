"""Auditable financial reconciliation reporting and summary rollups."""

from dataclasses import dataclass
from typing import Any, Dict, List

from sqlalchemy import func, select

from payflow.common.db import default_engine, get_db_session
from payflow.reconciliation.discrepancy import ReconciliationDiscrepancyRecord
from payflow.reconciliation.matching import ReconciliationSummary
from payflow.reconciliation.provider_import import SettlementBatchRecord


@dataclass
class ExecutiveReconciliationReport:
    total_batches: int
    total_volume_settled_minor: int
    total_internal_payments: int
    total_matched: int
    match_rate_pct: float
    total_discrepancies: int
    unresolved_discrepancies: int
    discrepancies_by_type: Dict[str, int]


class ReconciliationReportService:
    """Generates auditable financial reconciliation and discrepancy reports."""

    def __init__(self, engine: Any = None) -> None:
        self.engine = engine or default_engine

    def generate_merchant_report(self, merchant_id: str) -> ExecutiveReconciliationReport:
        """Computes rollup metrics across all settlement batches for a merchant."""
        with get_db_session(self.engine) as session:
            # Batches aggregate
            batch_stmt = select(
                func.count(SettlementBatchRecord.id),
                func.coalesce(func.sum(SettlementBatchRecord.total_settled_minor), 0),
                func.coalesce(func.sum(SettlementBatchRecord.total_records), 0),
            ).where(SettlementBatchRecord.merchant_id == merchant_id)
            total_batches, total_settled_minor, total_provider_records = session.execute(batch_stmt).one()

            # Discrepancies aggregate
            disc_stmt = select(ReconciliationDiscrepancyRecord).where(
                ReconciliationDiscrepancyRecord.merchant_id == merchant_id
            )
            discrepancies = list(session.execute(disc_stmt).scalars().all())

            by_type: Dict[str, int] = {}
            unresolved = 0
            for d in discrepancies:
                by_type[d.discrepancy_type] = by_type.get(d.discrepancy_type, 0) + 1
                if d.status != "RESOLVED":
                    unresolved += 1

            matched = max(0, total_provider_records - len(discrepancies))
            match_rate = (matched / total_provider_records * 100.0) if total_provider_records > 0 else 100.0

            return ExecutiveReconciliationReport(
                total_batches=total_batches,
                total_volume_settled_minor=total_settled_minor,
                total_internal_payments=total_provider_records,
                total_matched=matched,
                match_rate_pct=round(match_rate, 2),
                total_discrepancies=len(discrepancies),
                unresolved_discrepancies=unresolved,
                discrepancies_by_type=by_type,
            )
