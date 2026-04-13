"""
Minimal HTTP client for future IEA / licensed data services.

Auth schemes differ by product; this client only attaches optional credentials
so callers can switch to bearer tokens, API keys, or custom headers later without
changing the ingestion pipeline.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import urljoin

import httpx

from app.connectors.iea.exceptions import IEAConfigurationError

logger = logging.getLogger(__name__)


class IEAClient:
    """
    Thin wrapper around GET JSON endpoints.

    Set ``IEA_API_BASE_URL`` when a licensed or public base URL is available.
    ``IEA_API_KEY`` is sent as ``Authorization: Bearer <key>`` if present; adjust
    here when the vendor documents a different scheme.
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 120.0,
        client: httpx.Client | None = None,
    ):
        self._base_url = (base_url or "").strip().rstrip("/")
        self._api_key = (api_key or "").strip() or None
        self._timeout = timeout
        self._client = client

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def api_key_set(self) -> bool:
        return self._api_key is not None

    def _url(self, path: str) -> str:
        if not self._base_url:
            raise IEAConfigurationError(
                "IEA_API_BASE_URL is not set; cannot perform HTTP requests."
            )
        return urljoin(self._base_url + "/", path.lstrip("/"))

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """GET JSON resource; raises on HTTP errors or non-JSON body."""
        url = self._url(path)
        logger.debug("IEA GET %s", path)
        if self._client is None:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=dict(params or {}), headers=self._headers())
        else:
            response = self._client.get(url, params=dict(params or {}), headers=self._headers())

        response.raise_for_status()
        return response.json()
