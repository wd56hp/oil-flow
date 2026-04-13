"""
Fetch commodity trade rows from UN Comtrade Plus ``/data``.

Without ``UNCOMTRADE_API_KEY``, returns ``[]`` so cron jobs stay idempotent and safe in CI.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.connectors.uncomtrade.client import UnComtradeClient
from app.core.config import get_settings

logger = logging.getLogger(__name__)

DATA_ROUTE = "data"

# Typical Comtrade ``data`` fields used by :func:`normalize_uncomtrade_rows` / DQ warnings.
COMTRADE_EXPECTED_RAW_COLUMNS: frozenset[str] = frozenset(
    {
        "period",
        "reporterCode",
        "partnerCode",
        "cmdCode",
        "flowCode",
        "qty",
    }
)


def get_uncomtrade_client(
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    client: UnComtradeClient | None = None,
) -> UnComtradeClient | None:
    settings = get_settings()
    key = api_key if api_key is not None else settings.uncomtrade_api_key
    if not key:
        return None
    base = base_url if base_url is not None else settings.uncomtrade_base_url
    return UnComtradeClient(api_key=key, base_url=base, client=None)


def _default_query_params(period: str | None) -> dict[str, str]:
    """Conservative defaults; override via ``UNCOMTRADE_QUERY_JSON`` or CLI."""
    # Monthly goods (C), US reporter, crude petroleum HS 2709 — adjust per deployment.
    p: dict[str, str] = {
        "typeCode": "C",
        "freqCode": "M",
        "reporterCode": "842",
        "cmdCode": "2709",
        "flowCode": "M",
    }
    if period:
        p["period"] = period
    return p


def fetch_comtrade_data_rows(
    *,
    period: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    extra_params: dict[str, str] | None = None,
    client: UnComtradeClient | None = None,
) -> list[dict[str, Any]]:
    """
    Return raw dict rows from Comtrade Plus ``GET .../data``.

    If no API key is configured, logs once and returns ``[]`` (no network).
    """
    c = client or get_uncomtrade_client(api_key=api_key, base_url=base_url)
    if c is None:
        logger.info(
            "UNCOMTRADE_API_KEY unset; Comtrade fetch skipped (idempotent no-op)"
        )
        return []

    settings = get_settings()
    params = _default_query_params(period)
    if settings.uncomtrade_query_json:
        try:
            merged = json.loads(settings.uncomtrade_query_json)
            if isinstance(merged, dict):
                for k, v in merged.items():
                    params[str(k)] = str(v)
        except json.JSONDecodeError:
            logger.warning("UNCOMTRADE_QUERY_JSON is not valid JSON; ignoring")

    if extra_params:
        params.update({str(k): str(v) for k, v in extra_params.items()})

    payload = c.get_json(DATA_ROUTE, params=params)
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        logger.warning("UN Comtrade response missing 'data' list: keys=%s", payload.keys())
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    logger.warning("Unexpected UN Comtrade JSON shape: %s", type(payload))
    return []
