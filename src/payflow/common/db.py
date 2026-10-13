"""Database connection, session management, and base declarative class."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Base declarative class for all PayFlow SQLAlchemy models."""
    pass


def get_database_url() -> str:
    """Returns the configured primary database URL, defaulting to local SQLite."""
    return os.getenv("PAYFLOW_DATABASE_URL", "sqlite:///./payflow.db")


def get_read_database_url() -> str:
    """Returns configured read-replica database URL, falling back to primary database URL."""
    return os.getenv("PAYFLOW_READ_DATABASE_URL", get_database_url())


def create_db_engine(url: str | None = None, echo: bool = False) -> Engine:
    """Creates a SQLAlchemy engine configured for high concurrency, pooling, and resilience."""
    db_url = url or get_database_url()

    connect_args: dict[str, Any] = {}
    engine_kwargs: dict[str, Any] = {
        "echo": echo,
        "pool_pre_ping": True,
    }

    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        connect_args["timeout"] = 15.0  # 15s lock wait timeout for concurrent writes
        engine_kwargs["connect_args"] = connect_args
    else:
        # Production connection pool parameters for PostgreSQL/MySQL
        pool_size = int(os.getenv("PAYFLOW_DB_POOL_SIZE", "20"))
        max_overflow = int(os.getenv("PAYFLOW_DB_MAX_OVERFLOW", "30"))
        pool_timeout = float(os.getenv("PAYFLOW_DB_POOL_TIMEOUT", "30.0"))
        pool_recycle = int(os.getenv("PAYFLOW_DB_POOL_RECYCLE", "1800"))

        engine_kwargs.update({
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            "pool_timeout": pool_timeout,
            "pool_recycle": pool_recycle,
        })

    engine = create_engine(db_url, **engine_kwargs)

    # Enable SQLite WAL mode and foreign key constraints for robust concurrency
    if db_url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection: Any, connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=15000")
            cursor.close()

    return engine


# Default primary engine and read-replica engine
default_engine = create_db_engine()
read_engine = (
    create_db_engine(get_read_database_url())
    if get_read_database_url() != get_database_url()
    else default_engine
)

SessionFactory = sessionmaker(bind=default_engine, autoflush=False, autocommit=False, expire_on_commit=False)
ReadSessionFactory = sessionmaker(bind=read_engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db(engine: Engine | None = None) -> None:
    """Initializes all database tables registered on Base metadata."""
    target_engine = engine or default_engine
    Base.metadata.create_all(bind=target_engine)


def reset_db(engine: Engine | None = None) -> None:
    """Drops and recreates all database tables. Used for isolated testing."""
    target_engine = engine or default_engine
    Base.metadata.drop_all(bind=target_engine)
    Base.metadata.create_all(bind=target_engine)


@contextmanager
def get_db_session(engine: Engine | None = None) -> Generator[Session, None, None]:
    """Context manager for managing transactions with automatic rollback on error."""
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False) if engine else SessionFactory
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_read_db_session() -> Generator[Session, None, None]:
    """Context manager providing a read-only database session (routed to read replica when configured)."""
    session: Session = ReadSessionFactory()
    try:
        yield session
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency for write database sessions."""
    with get_db_session() as session:
        yield session


def get_read_db() -> Generator[Session, None, None]:
    """FastAPI dependency for read database sessions."""
    with get_read_db_session() as session:
        yield session
