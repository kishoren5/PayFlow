"""Contract test verifying Alembic schema migrations."""

import tempfile
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from payflow.common.db import create_db_engine


def test_alembic_migrations_upgrade_and_downgrade() -> None:
    """Verifies that Alembic migration scripts execute cleanly and create expected tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_migration.db"
        db_url = f"sqlite:///{db_path}"

        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", db_url)

        # 1. Run migration upgrade to head
        command.upgrade(alembic_cfg, "head")

        engine = create_db_engine(db_url)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()

        # Invariant: Core tables must exist after migration
        assert "payments" in table_names
        assert "idempotency_keys" in table_names
        assert "outbox_events" in table_names
        assert "ledger_accounts" in table_names
        assert "journal_transactions" in table_names
        assert "ledger_entries" in table_names

        # 2. Run migration downgrade to base
        command.downgrade(alembic_cfg, "base")
        inspector_after = inspect(engine)
        table_names_after = inspector_after.get_table_names()

        assert "payments" not in table_names_after
        assert "journal_transactions" not in table_names_after

        engine.dispose()
