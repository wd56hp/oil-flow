"""Current trade flow fact: one row per business key."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class TradeFlow(Base):
    __tablename__ = "trade_flows"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "dataset",
            "period_date",
            "reporter_country",
            "partner_country",
            "commodity",
            "flow_direction",
            name="uq_trade_flow_business_key",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source: Mapped[str] = mapped_column(String(64), nullable=False)
    dataset: Mapped[str] = mapped_column(String(128), nullable=False)
    period_date: Mapped[date] = mapped_column(Date, nullable=False)
    reporter_country: Mapped[str] = mapped_column(String(8), nullable=False)
    partner_country: Mapped[str] = mapped_column(String(32), nullable=False)
    commodity: Mapped[str] = mapped_column(String(128), nullable=False)
    flow_direction: Mapped[str] = mapped_column(String(32), nullable=False)

    quantity: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    quantity_unit: Mapped[str | None] = mapped_column(String(64), nullable=True)

    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    eia_origin_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    eia_destination_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    eia_grade_id: Mapped[str | None] = mapped_column(String(32), nullable=True)

    revisions: Mapped[list["TradeFlowRevision"]] = relationship(
        "TradeFlowRevision",
        back_populates="trade_flow",
        cascade="all, delete-orphan",
    )
