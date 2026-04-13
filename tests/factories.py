"""
Mock EIA-derived :class:`~app.schemas.trade_flow.TradeFlowRecord` rows.

These mirror what :func:`app.connectors.eia.normalize_crude_import_rows` would emit,
without calling the live API.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.schemas.trade_flow import TradeFlowRecord

# Fixed timestamps keep assertions stable across runs.
MOCK_NORMALIZED_AT = datetime(2025, 7, 15, 18, 30, 0, tzinfo=timezone.utc)


def mock_eia_crude_import(
    *,
    partner_country: str = "AG",
    period_date: date | None = None,
    quantity: str = "658",
    quantity_unit: str = "thousand barrels",
    commodity: str = "crude_oil:LSW",
    observed_at: datetime | None = None,
    eia_origin_id: str = "CTY_AG",
    eia_destination_id: str = "PP_1",
    eia_grade_id: str = "LSW",
) -> TradeFlowRecord:
    """Single normalized row as if produced from EIA `crude-oil-imports` data."""
    return TradeFlowRecord(
        source="eia",
        dataset="crude-oil-imports",
        period_date=period_date or date(2025, 6, 1),
        reporter_country="US",
        partner_country=partner_country,
        commodity=commodity,
        flow_direction="import",
        observed_at=observed_at or MOCK_NORMALIZED_AT,
        quantity=Decimal(quantity),
        quantity_unit=quantity_unit,
        eia_origin_id=eia_origin_id,
        eia_destination_id=eia_destination_id,
        eia_grade_id=eia_grade_id,
    )
