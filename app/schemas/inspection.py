"""JSON shapes for read-only inspection API (table-first verification)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class TradeFlowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    dataset: str
    period_date: date
    reporter_country: str
    partner_country: str
    commodity: str
    flow_direction: str
    quantity: Decimal | None
    quantity_unit: str | None
    observed_at: datetime
    eia_origin_id: str | None = None
    eia_destination_id: str | None = None
    eia_grade_id: str | None = None


class IngestionRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: str
    source_hint: str | None
    inserted_count: int
    revised_count: int
    unchanged_count: int
    failed_count: int
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None = None


class TradeFlowRevisionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    trade_flow_id: int
    ingestion_run_id: int
    previous_quantity: Decimal | None
    previous_quantity_unit: str | None
    previous_observed_at: datetime | None
    new_quantity: Decimal | None
    new_quantity_unit: str | None
    new_observed_at: datetime
    created_at: datetime


class Paginated(BaseModel):
    """Generic list wrapper."""

    limit: int = Field(ge=1, le=500)
    offset: int = Field(ge=0)
    total: int


class TradeFlowListResponse(Paginated):
    items: list[TradeFlowRead]


class IngestionRunListResponse(Paginated):
    items: list[IngestionRunRead]


class TradeFlowRevisionListResponse(Paginated):
    items: list[TradeFlowRevisionRead]
