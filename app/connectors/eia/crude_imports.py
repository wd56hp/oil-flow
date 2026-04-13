"""Crude oil imports from EIA v2 `crude-oil-imports` route — fetch + normalize to TradeFlowRecord."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.connectors.eia.client import EIAClient, build_query_params
from app.connectors.eia.exceptions import EIAAPIError
from app.core.config import get_settings
from app.schemas.trade_flow import TradeFlowRecord

logger = logging.getLogger(__name__)

EIA_SOURCE = "eia"
CRUDE_IMPORTS_DATASET = "crude-oil-imports"
CRUDE_IMPORTS_DATA_ROUTE = "crude-oil-imports/data/"

DEFAULT_DATA_FIELDS = ("quantity",)


@dataclass
class CrudeImportNormalizeSummary:
    """Filled when ``summary=`` is passed to :func:`normalize_crude_import_rows`."""

    raw_input_rows: int = 0
    skipped_no_period: int = 0
    skipped_bad_period: int = 0
    dropped_non_country_origin: int = 0
    normalized_out: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "raw_input_rows": self.raw_input_rows,
            "skipped_no_period": self.skipped_no_period,
            "skipped_bad_period": self.skipped_bad_period,
            "dropped_non_country_origin": self.dropped_non_country_origin,
            "normalized_out": self.normalized_out,
        }


# Raw EIA rows: core keys + common v2 display/facet fields (avoids false raw_unexpected_column DQ).
CRUDE_IMPORTS_EXPECTED_RAW_COLUMNS: frozenset[str] = frozenset(
    {
        "period",
        "quantity",
        # EIA v2 uses hyphenated key; underscore variant is not sent on crude-imports.
        "quantity-units",
        "originId",
        "originType",
        "originName",
        "originTypeName",
        "destinationId",
        "destinationType",
        "destinationName",
        "destinationTypeName",
        "gradeId",
        "gradeName",
    }
)


def get_eia_client(api_key: str | None = None) -> EIAClient:
    key = api_key
    if key is None:
        key = get_settings().eia_api_key
    if not key:
        raise ValueError("EIA_API_KEY is not set in the environment")
    return EIAClient(api_key=key)


def fetch_crude_oil_imports(
    *,
    api_key: str | None = None,
    frequency: str = "monthly",
    data_fields: Sequence[str] = DEFAULT_DATA_FIELDS,
    facets: Mapping[str, Sequence[str]] | None = None,
    sort: Sequence[tuple[str, str]] | None = None,
    start: str | None = None,
    end: str | None = None,
    offset: int | None = None,
    length: int | None = None,
    paginate: bool = True,
    client: EIAClient | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch raw rows from `https://api.eia.gov/v2/crude-oil-imports/data/`.

    When ``paginate`` is True and ``offset``/``length`` are not set, follows
    offset/length pages until the dataset is exhausted (5000 rows per page max).

    Parameters map directly to EIA query parameters (facets, frequency, start/end, etc.).
    """
    c = client or get_eia_client(api_key=api_key)

    base = build_query_params(
        api_key=c.api_key,
        frequency=frequency,
        data=list(data_fields),
        facets=dict(facets) if facets else None,
        sort=list(sort) if sort else None,
        start=start,
        end=end,
        offset=None,
        length=None,
    )

    if paginate and offset is None and length is None:
        logger.info("Fetching all crude-oil-imports pages (paginated)")
        return c.fetch_all_data_rows(CRUDE_IMPORTS_DATA_ROUTE, base_params=base)

    if offset is not None:
        base = list(base)
        base.append(("offset", str(offset)))
    if length is not None:
        base = list(base)
        base.append(("length", str(length)))

    payload = c.get_json(CRUDE_IMPORTS_DATA_ROUTE, base)
    rows = payload["response"].get("data") or []
    if not isinstance(rows, list):
        raise EIAAPIError("EIA crude-oil-imports response.response.data is not a list")
    return rows


def _period_to_date(period: str) -> date:
    """EIA uses YYYY-MM for monthly crude imports."""
    parts = period.strip().split("-")
    if len(parts) >= 2:
        y, m = int(parts[0]), int(parts[1])
        return date(y, m, 1)
    if len(parts) == 1:
        return date(int(parts[0]), 1, 1)
    raise ValueError(f"Unrecognized period format: {period!r}")


def _partner_country_from_row(row: Mapping[str, Any]) -> str:
    """Map EIA origin facet to a stable partner code (ISO2 when origin is a country)."""
    origin_type = str(row.get("originType") or "")
    origin_id = str(row.get("originId") or "")
    if origin_type == "CTY" and origin_id.startswith("CTY_") and len(origin_id) > 4:
        return origin_id[4:]
    return origin_id


def _is_eia_country_origin_row(row: Mapping[str, Any]) -> bool:
    """
    True when EIA marks the row as a country-level origin (``CTY`` + ``CTY_XX``).

    Excludes WORLD, regional rollups (``REG_*``), open-species / other aggregates (``OPN_*``),
    and any row without a proper country facet — reducing collisions on the canonical business key.
    """
    origin_type = str(row.get("originType") or "").strip().upper()
    origin_id = str(row.get("originId") or "").strip()
    return origin_type == "CTY" and origin_id.startswith("CTY_") and len(origin_id) > 4


def _commodity_code(row: Mapping[str, Any]) -> str:
    grade = row.get("gradeId")
    if grade:
        return f"crude_oil:{grade}"
    return "crude_oil"


def _parse_quantity(row: Mapping[str, Any]) -> tuple[Decimal | None, str | None]:
    raw = row.get("quantity")
    if raw is None or raw == "":
        return None, None
    try:
        return Decimal(str(raw)), row.get("quantity-units") or row.get("quantity_units")
    except (InvalidOperation, ValueError):
        logger.warning("Could not parse quantity from row keys=%s", row.keys())
        return None, row.get("quantity-units")


def normalize_crude_import_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    observed_at: datetime | None = None,
    country_origin_only: bool = True,
    summary: CrudeImportNormalizeSummary | None = None,
) -> list[TradeFlowRecord]:
    """
    Map EIA crude-oil-imports data rows to :class:`TradeFlowRecord`.

    Reporter is the United States (imports into U.S. destinations); partner is the
    country/region of origin derived from ``originId`` / ``originType``.

    When ``country_origin_only`` is True (default), only rows with EIA facet ``CTY`` and
    ``originId`` like ``CTY_XX`` are kept. That drops WORLD, ``REG_*``, ``OPN_*``, and other
    aggregates that share the same canonical business key as country rows and inflate revisions.

    **Note:** Destination disaggregation (``destinationId``) is not part of the business key; if
    multiple ports remain for the same country/grade/month, duplicates can still occur within a batch.
    """
    if summary is not None:
        summary.raw_input_rows = len(rows)

    ts = observed_at or datetime.now(timezone.utc)
    out: list[TradeFlowRecord] = []
    for row in rows:
        period = row.get("period")
        if not period:
            if summary is not None:
                summary.skipped_no_period += 1
            logger.warning("Skipping row without period: %s", row)
            continue
        try:
            period_date = _period_to_date(str(period))
        except ValueError as exc:
            if summary is not None:
                summary.skipped_bad_period += 1
            logger.warning("Skipping row with bad period %r: %s", period, exc)
            continue

        if country_origin_only and not _is_eia_country_origin_row(row):
            if summary is not None:
                summary.dropped_non_country_origin += 1
            continue

        partner = _partner_country_from_row(row)
        qty, unit = _parse_quantity(row)

        out.append(
            TradeFlowRecord(
                source=EIA_SOURCE,
                dataset=CRUDE_IMPORTS_DATASET,
                period_date=period_date,
                reporter_country="US",
                partner_country=partner,
                commodity=_commodity_code(row),
                flow_direction="import",
                observed_at=ts,
                quantity=qty,
                quantity_unit=unit,
                eia_origin_id=str(row.get("originId")) if row.get("originId") is not None else None,
                eia_destination_id=str(row.get("destinationId"))
                if row.get("destinationId") is not None
                else None,
                eia_grade_id=str(row.get("gradeId")) if row.get("gradeId") is not None else None,
            )
        )
    if summary is not None:
        summary.normalized_out = len(out)
    return out
