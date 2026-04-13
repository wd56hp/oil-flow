"""HTTP client for UN Comtrade Plus public API (subscription key)."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

import httpx

logger = logging.getLogger(__name__)

SUBSCRIPTION_HEADER = "Ocp-Apim-Subscription-Key"


class UnComtradeClient:
    """GET JSON under ``{base_url}/`` with Comtrade Plus subscription header."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str,
        timeout: float = 120.0,
        client: httpx.Client | None = None,
    ):
        if not api_key or not str(api_key).strip():
            raise ValueError("UN Comtrade api_key is required when the client is used")
        self._api_key = str(api_key).strip()
        self._base_url = base_url.rstrip("/") + "/"
        self._timeout = timeout
        self._client = client

    def _url(self, route: str) -> str:
        return urljoin(self._base_url, route.lstrip("/"))

    def get_json(self, route: str, *, params: dict[str, str] | None = None) -> dict[str, Any]:
        url = self._url(route)
        headers = {SUBSCRIPTION_HEADER: self._api_key}
        logger.debug("UN Comtrade GET %s params=%s", route, params)
        if self._client is None:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=params or {}, headers=headers)
        else:
            response = self._client.get(url, params=params or {}, headers=headers)

        if response.status_code >= 400:
            body_preview = response.text[:2000] if response.text else None
            logger.warning(
                "UN Comtrade HTTP %s for %s body=%r",
                response.status_code,
                route,
                body_preview,
            )
            response.raise_for_status()

        return response.json()
