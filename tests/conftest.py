"""Shared pytest fixtures."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import Base


@pytest.fixture(scope="session")
def test_engine():
    """
    In-memory SQLite (single connection) so ORM tests run without Postgres.

    Integer PKs match production models and autoincrement correctly under SQLite.
    """
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session(test_engine) -> Session:
    """Fresh database state for each test."""
    with test_engine.connect() as conn:
        conn.execute(text("DELETE FROM trade_flow_revisions"))
        conn.execute(text("DELETE FROM trade_flows"))
        conn.execute(text("DELETE FROM ingestion_runs"))
        conn.commit()

    SessionLocal = sessionmaker(bind=test_engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
