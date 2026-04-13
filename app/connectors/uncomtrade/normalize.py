"""Map UN Comtrade Plus ``data`` rows to :class:`~app.schemas.trade_flow.TradeFlowRecord`."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Mapping, Sequence

from app.core.config import get_settings
from app.schemas.trade_flow import TradeFlowRecord

logger = logging.getLogger(__name__)

UN_SOURCE = "uncomtrade"


def _period_to_date(period: Any) -> date | None:
    if period is None:
        return None
    s = str(period).strip()
    if len(s) == 6 and s.isdigit():
        y, m = int(s[:4]), int(s[4:6])
        if 1 <= m <= 12:
            return date(y, m, 1)
    if len(s) >= 7 and s[4] == "-":
        try:
            parts = s.split("-")
            return date(int(parts[0]), int(parts[1]), 1)
        except (ValueError, IndexError):
            return None
    return None


def _flow_direction(flow_code: Any) -> Literal["import", "export", "unknown"]:
    fc = str(flow_code or "").strip().upper()
    if fc in ("M", "IMPORT"):
        return "import"
    if fc in ("X", "EXPORT"):
        return "export"
    return "unknown"


def _parse_quantity(row: Mapping[str, Any]) -> tuple[Decimal | None, str | None]:
    for key in ("qty", "Qty", "primaryValue", "netWeight"):
        raw = row.get(key)
        if raw is None or raw == "":
            continue
        try:
            return Decimal(str(raw)), row.get("qtyUnitAbbr") or row.get("qtyUnitCode")
        except (InvalidOperation, ValueError):
            continue
    return None, None


def normalize_uncomtrade_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    observed_at: datetime | None = None,
    dataset: str | None = None,
) -> list[TradeFlowRecord]:
    """
    Best-effort mapping from Comtrade ``data`` rows.

    Reporter/partner are stored as string codes returned by the API (often numeric ISO).
    Rows missing period or reporter/partner are skipped.
    """
    settings = get_settings()
    ds = dataset if dataset is not None else settings.uncomtrade_dataset
    ts = observed_at or datetime.now(timezone.utc)
    out: list[TradeFlowRecord] = []

    for row in rows:
        period_date = _period_to_date(row.get("period"))
        if period_date is None:
            logger.debug("Skipping Comtrade row without usable period: %s", row)
            continue
        reporter = row.get("reporterCode")
        partner = row.get("partnerCode")
        if reporter is None or partner is None:
            logger.debug("Skipping Comtrade row without reporter/partner: %s", row)
            continue

        qty, unit = _parse_quantity(row)
        cmd = row.get("cmdCode")
        commodity = f"hs:{cmd}" if cmd is not None else "unknown"

        out.append(
            TradeFlowRecord(
                source=UN_SOURCE,
                dataset=ds,
                period_date=period_date,
                reporter_country=str(reporter),
                partner_country=str(partner),
                commodity=str(commodity),
                flow_direction=_flow_direction(row.get("flowCode")),
                observed_at=ts,
                quantity=qty,
                quantity_unit=unit,
            )
        )
    return out
