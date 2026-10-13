"""Initial schema for PayFlow platform

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-10-13 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '0001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Idempotency Keys
    op.create_table(
        'idempotency_keys',
        sa.Column('id', sa.String(length=64), primary_key=True),
        sa.Column('merchant_id', sa.String(length=64), nullable=False, index=True),
        sa.Column('operation', sa.String(length=64), nullable=False, index=True),
        sa.Column('idempotency_key', sa.String(length=128), nullable=False),
        sa.Column('request_hash', sa.String(length=64), nullable=False, index=True),
        sa.Column('status', sa.String(length=32), nullable=False, default='IN_PROGRESS'),
        sa.Column('response_code', sa.Integer(), nullable=True),
        sa.Column('response_body', sa.Text(), nullable=True),
        sa.Column('payment_id', sa.String(length=64), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint('merchant_id', 'operation', 'idempotency_key', name='uq_idempotency_merchant_op_key'),
    )
    op.create_index('ix_idempotency_lookup', 'idempotency_keys', ['merchant_id', 'operation', 'idempotency_key'])

    # 2. Payments
    op.create_table(
        'payments',
        sa.Column('id', sa.String(length=64), primary_key=True),
        sa.Column('merchant_id', sa.String(length=64), nullable=False, index=True),
        sa.Column('amount_minor', sa.Integer(), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False, index=True),
        sa.Column('payment_method_token', sa.String(length=128), nullable=False),
        sa.Column('capture_method', sa.String(length=32), nullable=False, default='automatic'),
        sa.Column('metadata_json', sa.Text(), nullable=False, default='{}'),
        sa.Column('error_code', sa.String(length=64), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('idempotency_key', sa.String(length=128), nullable=True, index=True),
        sa.Column('provider_reference', sa.String(length=128), nullable=True, index=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('amount_minor > 0', name='chk_payment_amount_positive'),
    )
    op.create_index('ix_payments_merchant_created', 'payments', ['merchant_id', 'created_at'])

    # 3. Outbox Events
    op.create_table(
        'outbox_events',
        sa.Column('id', sa.String(length=64), primary_key=True),
        sa.Column('aggregate_type', sa.String(length=32), nullable=False, index=True),
        sa.Column('aggregate_id', sa.String(length=64), nullable=False, index=True),
        sa.Column('event_type', sa.String(length=64), nullable=False, index=True),
        sa.Column('payload_json', sa.Text(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False, default='PENDING', index=True),
        sa.Column('retry_count', sa.Integer(), nullable=False, default=0),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index('ix_outbox_pending', 'outbox_events', ['status', 'created_at'])

    # 4. Ledger Accounts
    op.create_table(
        'ledger_accounts',
        sa.Column('id', sa.String(length=64), primary_key=True),
        sa.Column('merchant_id', sa.String(length=64), nullable=False, index=True),
        sa.Column('account_name', sa.String(length=128), nullable=False),
        sa.Column('account_type', sa.String(length=32), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('merchant_id', 'account_name', 'currency', name='uq_merchant_account_currency'),
    )

    # 5. Journal Transactions
    op.create_table(
        'journal_transactions',
        sa.Column('id', sa.String(length=64), primary_key=True),
        sa.Column('source_event_id', sa.String(length=128), nullable=False, unique=True, index=True),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('posted_at', sa.DateTime(timezone=True), nullable=False),
    )

    # 6. Ledger Entries
    op.create_table(
        'ledger_entries',
        sa.Column('id', sa.String(length=64), primary_key=True),
        sa.Column('transaction_id', sa.String(length=64), sa.ForeignKey('journal_transactions.id'), nullable=False, index=True),
        sa.Column('account_id', sa.String(length=64), sa.ForeignKey('ledger_accounts.id'), nullable=False, index=True),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('debit_minor', sa.Integer(), nullable=False, default=0),
        sa.Column('credit_minor', sa.Integer(), nullable=False, default=0),
        sa.Column('sequence', sa.Integer(), nullable=False, default=0),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint('debit_minor >= 0', name='chk_debit_non_negative'),
        sa.CheckConstraint('credit_minor >= 0', name='chk_credit_non_negative'),
    )
    op.create_index('ix_ledger_entries_account_created', 'ledger_entries', ['account_id', 'created_at'])

    # 7. Account Balances Snapshot
    op.create_table(
        'account_balances',
        sa.Column('account_id', sa.String(length=64), primary_key=True),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('balance_minor', sa.Integer(), nullable=False, default=0),
        sa.Column('total_debit_minor', sa.Integer(), nullable=False, default=0),
        sa.Column('total_credit_minor', sa.Integer(), nullable=False, default=0),
        sa.Column('last_entry_id', sa.String(length=64), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    )

    # 8. Processed Events (Consumer deduplication)
    op.create_table(
        'processed_events',
        sa.Column('event_id', sa.String(length=64), primary_key=True),
        sa.Column('consumer_name', sa.String(length=64), primary_key=True),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False, default='COMPLETED'),
    )


def downgrade() -> None:
    op.drop_table('processed_events')
    op.drop_table('account_balances')
    op.drop_table('ledger_entries')
    op.drop_table('journal_transactions')
    op.drop_table('ledger_accounts')
    op.drop_table('outbox_events')
    op.drop_table('payments')
    op.drop_table('idempotency_keys')
