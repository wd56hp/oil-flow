"""Canonical trade-flow shapes for normalized connector output (not persisted yet)."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TradeFlowRecord(BaseModel):
    """Aligned with the platform business key + measures; append-only uses observed_at later."""

    model_config = ConfigDict(frozen=True)

    source: str = Field(description="Upstream system identifier, e.g. eia")
    dataset: str = Field(description="Dataset or series family within the source")
    period_date: date
    reporter_country: str = Field(description="ISO 3166-1 alpha-2 where applicable")
    partner_country: str = Field(
        description="Counterparty region code (ISO2 when origin is a country)"
    )
    commodity: str = Field(description="Canonical commodity code")
    flow_direction: Literal["import", "export", "reexport", "unknown"] = "unknown"

    observed_at: datetime = Field(
        description="When this row was produced by our normalization step"
    )

    quantity: Decimal | None = None
    quantity_unit: str | None = None

    # Optional lineage for EIA crude imports (destination is disaggregated below country)
    eia_origin_id: str | None = None
    eia_destination_id: str | None = None
    eia_grade_id: str | None = None
