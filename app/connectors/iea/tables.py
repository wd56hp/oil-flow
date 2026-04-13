"""
Table-based IEA ingestion: each raw row is a mapping (CSV row, API object, etc.).

Normalize into :class:`~app.schemas.trade_flow.TradeFlowRecord` for
:func:`app.services.ingestion_engine.ingest_trade_flow_records`.

Expected keys (case-sensitive, flexible aliases listed in ``_FIELD_ALIASES``):

- ``period_date`` or ``period`` (``YYYY-MM-DD`` or ``YYYY-MM``)
- ``reporter_country``, ``partner_country`` (ISO-style codes)
- ``commodity``, ``flow_direction``
- ``quantity``, ``quantity_unit`` (optional)
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast

from app.connectors.iea.constants import IEA_SOURCE
from app.schemas.trade_flow import TradeFlowRecord

logger = logging.getLogger(__name__)

_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "period_date": ("period_date", "period", "time"),
    "reporter_country": ("reporter_country", "reporter", "country"),
    "partner_country": ("partner_country", "partner", "counterparty"),
    "commodity": ("commodity", "product"),
    "flow_direction": ("flow_direction", "flow"),
    "quantity": ("quantity", "value", "obs_value"),
    "quantity_unit": ("quantity_unit", "unit", "units"),
}


def _pick(row: Mapping[str, Any], logical: str) -> Any:
    for key in _FIELD_ALIASES.get(logical, (logical,)):
        if key in row and row[key] is not None and str(row[key]).strip() != "":
            return row[key]
    return None


def _parse_period(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return date(y, m, d)
    if len(s) == 7 and s[4] == "-":
        y, m = int(s[0:4]), int(s[5:7])
        return date(y, m, 1)
    return None


def _parse_quantity(raw: Any) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None


def normalize_iea_table_row(
    row: Mapping[str, Any],
    *,
    dataset: str,
    observed_at: datetime | None = None,
) -> TradeFlowRecord | None:
    """
    Map one tabular row to a trade-flow record.

    Returns ``None`` if required fields are missing or invalid.
    """
    ts = observed_at or datetime.now(timezone.utc)
    period = _parse_period(_pick(row, "period_date"))
    reporter = _pick(row, "reporter_country")
    partner = _pick(row, "partner_country")
    commodity = _pick(row, "commodity")
    flow = _pick(row, "flow_direction")
    if period is None or not reporter or not partner or not commodity or not flow:
        logger.debug("Skipping IEA row with missing key fields: %s", row)
        return None

    flow_s = str(flow).lower()
    if flow_s not in ("import", "export", "reexport", "unknown"):
        flow_s = "unknown"
    flow_lit = cast(Literal["import", "export", "reexport", "unknown"], flow_s)

    qty = _parse_quantity(_pick(row, "quantity"))
    unit_raw = _pick(row, "quantity_unit")
    unit = str(unit_raw).strip() if unit_raw is not None else None

    return TradeFlowRecord(
        source=IEA_SOURCE,
        dataset=dataset,
        period_date=period,
        reporter_country=str(reporter).upper()[:8],
        partner_country=str(partner)[:32],
        commodity=str(commodity),
        flow_direction=flow_lit,
        observed_at=ts,
        quantity=qty,
        quantity_unit=unit,
    )


def normalize_iea_table_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    dataset: str,
    observed_at: datetime | None = None,
) -> list[TradeFlowRecord]:
    """Normalize many table rows; drops rows that fail validation."""
    out: list[TradeFlowRecord] = []
    for row in rows:
        rec = normalize_iea_table_row(row, dataset=dataset, observed_at=observed_at)
        if rec is not None:
            out.append(rec)
    return out
