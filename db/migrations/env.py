import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from payflow.common.db import Base, get_database_url
# Import all model modules so Base.metadata is fully populated
import payflow.payments.models  # noqa: F401
import payflow.ledger.models  # noqa: F401
import payflow.events.models  # noqa: F401
import payflow.webhooks.delivery_store  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = os.getenv("PAYFLOW_DATABASE_URL", config.get_main_option("sqlalchemy.url"))
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    db_url = os.getenv("PAYFLOW_DATABASE_URL") or config.get_main_option("sqlalchemy.url") or get_database_url()
    configuration["sqlalchemy.url"] = db_url
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
