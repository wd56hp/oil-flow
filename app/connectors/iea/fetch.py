"""
Optional HTTP fetch for IEA / licensed tabular endpoints.

With no base URL configured, :func:`fetch_table_rows` returns an empty list so
jobs can run in CI without credentials. Wire :func:`fetch_table_rows` to a real
path when contract and credentials are available.
"""

from __future__ import annotations

import logging
from typing import Any

from app.connectors.iea.client import IEAClient
from app.core.config import get_settings

logger = logging.getLogger(__name__)


def get_iea_client(
    *,
    base_url: str | None = None,
    api_key: str | None = None,
) -> IEAClient:
    """Build client from explicit args or ``Settings``."""
    settings = get_settings()
    return IEAClient(
        base_url=base_url if base_url is not None else settings.iea_api_base_url,
        api_key=api_key if api_key is not None else settings.iea_api_key,
    )


def fetch_table_rows(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    client: IEAClient | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch a JSON array of row objects from ``path`` (relative to base URL).

    Returns the ``data`` key if the payload is ``{"data": [...]}``, otherwise
    the top-level list if the body is a JSON array.

    If ``IEA_API_BASE_URL`` is unset, logs and returns ``[]`` (no network).
    """
    c = client or get_iea_client()
    if not c.base_url:
        logger.info("IEA_API_BASE_URL unset; fetch_table_rows(%r) returns []", path)
        return []

    payload = c.get_json(path, params=params)
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
    logger.warning("Unexpected IEA JSON shape for %s: %s", path, type(payload))
    return []
