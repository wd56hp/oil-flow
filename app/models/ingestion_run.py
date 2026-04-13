"""One row per ingestion execution (batch from any connector)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    """running | completed | failed"""

    source_hint: Mapped[str | None] = mapped_column(String(128), nullable=True)
    """Optional label (e.g. connector name) for operators."""

    inserted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    revised_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unchanged_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Rows skipped due to per-record errors (when that mode is enabled); otherwise 0."""

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    revisions: Mapped[list["TradeFlowRevision"]] = relationship(
        "TradeFlowRevision", back_populates="ingestion_run"
    )
