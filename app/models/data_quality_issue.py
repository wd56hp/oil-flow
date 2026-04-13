"""Non-fatal data quality observations (warnings, schema drift, etc.)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class DataQualityIssue(Base):
    __tablename__ = "data_quality_issues"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    dataset: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    issue_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    """
    Examples: raw_missing_field, raw_unexpected_column, schema_fingerprint_changed,
    normalized_null_quantity
    """

    severity: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    """warning | info | error (error reserved; ingestion does not abort on DQ alone)."""

    message: Mapped[str] = mapped_column(Text, nullable=False)

    detail: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    ingestion_run_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("ingestion_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    ingestion_run: Mapped["IngestionRun | None"] = relationship(
        "IngestionRun",
        back_populates="data_quality_issues",
    )
