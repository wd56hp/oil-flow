"""Historical measure change for an existing business key (revision audit trail)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class TradeFlowRevision(Base):
    """
    Recorded when an incoming row matches an existing business key but measures differ.

    Stores the **previous** measure snapshot before the canonical `trade_flows` row
    was updated to the incoming values.
    """

    __tablename__ = "trade_flow_revisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    trade_flow_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("trade_flows.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ingestion_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("ingestion_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    previous_quantity: Mapped[Decimal | None] = mapped_column(
        Numeric(38, 18), nullable=True
    )
    previous_quantity_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    previous_observed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    new_quantity: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    new_quantity_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    new_observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    trade_flow: Mapped["TradeFlow"] = relationship(
        "TradeFlow", back_populates="revisions"
    )
    ingestion_run: Mapped["IngestionRun"] = relationship(
        "IngestionRun", back_populates="revisions"
    )
