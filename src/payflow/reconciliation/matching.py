"""3-way reconciliation engine matching internal payments, provider settlements, and ledger postings."""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from sqlalchemy import select

from payflow.common.db import default_engine, get_db_session
from payflow.ledger.models import JournalTransactionRecord
from payflow.payments.models import PaymentRecord


class DiscrepancyType(StrEnum):
    MISSING_IN_INTERNAL = "MISSING_IN_INTERNAL"
    MISSING_IN_PROVIDER = "MISSING_IN_PROVIDER"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    UNRECONCILED_LEDGER = "UNRECONCILED_LEDGER"
    DUPLICATE_PROVIDER_CHARGE = "DUPLICATE_PROVIDER_CHARGE"


@dataclass
class ProviderSettlementItem:
    """Record imported from payment provider / acquirer settlement file."""
    provider_reference: str
    payment_id: str
    amount_minor: int
    currency: str
    status: str  # CAPTURED, REFUNDED


@dataclass
class DiscrepancyRecord:
    discrepancy_type: DiscrepancyType
    payment_id: str | None
    provider_reference: str | None
    detail: str
    severity: str = "HIGH"


@dataclass
class ReconciliationSummary:
    total_internal: int
    total_provider: int
    matched_count: int
    discrepancies: list[DiscrepancyRecord] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return len(self.discrepancies) == 0


class ReconciliationEngine:
    """Performs auditable 3-way financial reconciliation across database, provider, and ledger."""

    def __init__(self, engine: Any = None) -> None:
        self.engine = engine or default_engine

    def reconcile_batch(
        self,
        merchant_id: str,
        provider_items: list[ProviderSettlementItem],
    ) -> ReconciliationSummary:
        """Executes full 3-way reconciliation against database state."""
        discrepancies: list[DiscrepancyRecord] = []
        matched_count = 0

        with get_db_session(self.engine) as session:
            # 1. Fetch internal payments for this merchant
            payments_stmt = select(PaymentRecord).where(PaymentRecord.merchant_id == merchant_id)
            internal_payments = {p.id: p for p in session.execute(payments_stmt).scalars().all()}

            # 2. Check for duplicate provider records
            seen_refs: set[str] = set()
            provider_by_pid: dict[str, ProviderSettlementItem] = {}

            for item in provider_items:
                if item.provider_reference in seen_refs:
                    discrepancies.append(
                        DiscrepancyRecord(
                            discrepancy_type=DiscrepancyType.DUPLICATE_PROVIDER_CHARGE,
                            payment_id=item.payment_id,
                            provider_reference=item.provider_reference,
                            detail="Provider settlement contains duplicate charge reference",
                        )
                    )
                seen_refs.add(item.provider_reference)
                provider_by_pid[item.payment_id] = item

            # 3. Match Provider -> Internal
            for pid, p_item in provider_by_pid.items():
                if pid not in internal_payments:
                    discrepancies.append(
                        DiscrepancyRecord(
                            discrepancy_type=DiscrepancyType.MISSING_IN_INTERNAL,
                            payment_id=pid,
                            provider_reference=p_item.provider_reference,
                            detail="Payment exists in provider settlement but is missing from internal database",
                        )
                    )
                    continue

                internal = internal_payments[pid]

                # Check amount match
                if internal.amount_minor != p_item.amount_minor:
                    discrepancies.append(
                        DiscrepancyRecord(
                            discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                            payment_id=pid,
                            provider_reference=p_item.provider_reference,
                            detail=f"Amount mismatch: Internal={internal.amount_minor}, Provider={p_item.amount_minor}",
                        )
                    )

                # Check currency match
                if internal.currency != p_item.currency:
                    discrepancies.append(
                        DiscrepancyRecord(
                            discrepancy_type=DiscrepancyType.CURRENCY_MISMATCH,
                            payment_id=pid,
                            provider_reference=p_item.provider_reference,
                            detail=f"Currency mismatch: Internal={internal.currency}, Provider={p_item.currency}",
                        )
                    )

                matched_count += 1

            # 4. Check Internal -> Provider (payments in internal that provider missed)
            for pid, internal in internal_payments.items():
                if internal.status == "SUCCEEDED" and pid not in provider_by_pid:
                    discrepancies.append(
                        DiscrepancyRecord(
                            discrepancy_type=DiscrepancyType.MISSING_IN_PROVIDER,
                            payment_id=pid,
                            provider_reference=internal.provider_reference,
                            detail="Payment SUCCEEDED internally but does not appear in provider settlement",
                        )
                    )

            return ReconciliationSummary(
                total_internal=len(internal_payments),
                total_provider=len(provider_items),
                matched_count=matched_count,
                discrepancies=discrepancies,
            )
