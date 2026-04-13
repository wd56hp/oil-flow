"""Last-known raw column fingerprint per (source, dataset) for drift detection."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class SchemaFingerprint(Base):
    __tablename__ = "schema_fingerprints"
    __table_args__ = (UniqueConstraint("source", "dataset", name="uq_schema_fingerprint_source_dataset"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    dataset: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    fingerprint_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    """SHA-256 hex of sorted column names (union across sample rows)."""

    columns_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    """Sorted column names used to compute the fingerprint."""

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
