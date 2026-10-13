"""SQLAlchemy ORM models for immutable double-entry ledger."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from payflow.common.db import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def generate_ledger_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class LedgerAccountRecord(Base):
    """Authoritative financial account in the Chart of Accounts."""

    __tablename__ = "ledger_accounts"

    id = Column(String(64), primary_key=True, default=lambda: generate_ledger_id("acc"))
    merchant_id = Column(String(64), nullable=False, index=True)
    account_name = Column(String(128), nullable=False)
    account_type = Column(String(32), nullable=False)  # ASSET, LIABILITY, EQUITY, REVENUE, EXPENSE
    currency = Column(String(3), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    __table_args__ = (
        UniqueConstraint("merchant_id", "account_name", "currency", name="uq_merchant_account_currency"),
    )


class JournalTransactionRecord(Base):
    """Header for a balanced group of postings (debits must equal credits per currency)."""

    __tablename__ = "journal_transactions"

    id = Column(String(64), primary_key=True, default=lambda: generate_ledger_id("txn"))
    source_event_id = Column(String(128), nullable=False, unique=True, index=True)
    description = Column(Text, nullable=False)
    posted_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    entries = relationship("LedgerEntryRecord", back_populates="transaction", cascade="all, delete-orphan")


class LedgerEntryRecord(Base):
    """Immutable single-sided posting line item in the journal."""

    __tablename__ = "ledger_entries"

    id = Column(String(64), primary_key=True, default=lambda: generate_ledger_id("ent"))
    transaction_id = Column(String(64), ForeignKey("journal_transactions.id"), nullable=False, index=True)
    account_id = Column(String(64), ForeignKey("ledger_accounts.id"), nullable=False, index=True)
    currency = Column(String(3), nullable=False)
    debit_minor = Column(Integer, nullable=False, default=0)
    credit_minor = Column(Integer, nullable=False, default=0)
    sequence = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    transaction = relationship("JournalTransactionRecord", back_populates="entries")
    account = relationship("LedgerAccountRecord")

    __table_args__ = (
        CheckConstraint("debit_minor >= 0", name="chk_debit_non_negative"),
        CheckConstraint("credit_minor >= 0", name="chk_credit_non_negative"),
        Index("ix_ledger_entries_account_created", "account_id", "created_at"),
    )


class AccountBalanceSnapshotRecord(Base):
    """Rebuildable materialized balance snapshot for fast balance queries."""

    __tablename__ = "account_balances"

    account_id = Column(String(64), primary_key=True)
    currency = Column(String(3), nullable=False)
    balance_minor = Column(Integer, nullable=False, default=0)
    total_debit_minor = Column(Integer, nullable=False, default=0)
    total_credit_minor = Column(Integer, nullable=False, default=0)
    last_entry_id = Column(String(64), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)
