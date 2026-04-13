"""Low-level EIA Open Data API v2 client: auth, parameterized GET, pagination."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import urljoin

import httpx

from app.connectors.eia.exceptions import EIAAPIError, EIAHTTPError

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.eia.gov/v2"
MAX_PAGE_LENGTH = 5000


def build_query_params(
    *,
    api_key: str,
    frequency: str | None = None,
    data: Sequence[str] | None = None,
    facets: Mapping[str, Sequence[str]] | None = None,
    sort: Sequence[tuple[str, str]] | None = None,
    start: str | None = None,
    end: str | None = None,
    offset: int | None = None,
    length: int | None = None,
    extra: Mapping[str, Any] | None = None,
) -> list[tuple[str, str]]:
    """Flatten EIA v2 query parameters for httpx (supports repeated keys like data[])."""
    params: list[tuple[str, str]] = [("api_key", api_key)]

    if frequency is not None:
        params.append(("frequency", frequency))
    if data:
        for field in data:
            params.append(("data[]", field))
    if facets:
        for facet_name, values in facets.items():
            key = f"facets[{facet_name}][]"
            for value in values:
                params.append((key, str(value)))
    if sort:
        for i, (column, direction) in enumerate(sort):
            params.append((f"sort[{i}][column]", column))
            params.append((f"sort[{i}][direction]", direction))
    if start is not None:
        params.append(("start", start))
    if end is not None:
        params.append(("end", end))
    if offset is not None:
        params.append(("offset", str(offset)))
    if length is not None:
        params.append(("length", str(length)))
    if extra:
        for k, v in extra.items():
            if v is None:
                continue
            params.append((k, str(v)))
    return params


class EIAClient:
    """Minimal v2 client: GET JSON resources under /v2/<route>."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 60.0,
        client: httpx.Client | None = None,
    ):
        if not api_key or not api_key.strip():
            raise ValueError("EIA api_key is required")
        self._api_key = api_key.strip()
        self._base_url = base_url.rstrip("/") + "/"
        self._timeout = timeout
        self._client = client

    @property
    def api_key(self) -> str:
        return self._api_key

    def _url(self, route: str) -> str:
        route = route.lstrip("/")
        return urljoin(self._base_url, route)

    def get_json(self, route: str, params: list[tuple[str, str]]) -> dict[str, Any]:
        """GET and parse JSON; validate top-level shape and surface EIA errors."""
        url = self._url(route)
        logger.debug("EIA GET %s", route)
        if self._client is None:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=params)
        else:
            response = self._client.get(url, params=params)

        if response.status_code >= 400:
            body_preview = response.text[:2000] if response.text else None
            logger.warning(
                "EIA HTTP %s for %s body=%r",
                response.status_code,
                route,
                body_preview,
            )
            raise EIAHTTPError(
                f"EIA request failed with status {response.status_code}",
                status_code=response.status_code,
                body=body_preview,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise EIAAPIError("EIA response was not valid JSON") from exc

        if isinstance(payload, dict) and "error" in payload:
            err = payload["error"]
            code = None
            message = str(err)
            if isinstance(err, dict):
                code = err.get("code")
                message = err.get("message", message)
            logger.warning("EIA API error code=%s message=%s", code, message)
            raise EIAAPIError(message, code=str(code) if code is not None else None)

        if not isinstance(payload, dict) or "response" not in payload:
            raise EIAAPIError("EIA response missing expected 'response' key")

        if "warnings" in payload and payload["warnings"]:
            for w in payload["warnings"]:
                if isinstance(w, dict):
                    logger.warning(
                        "EIA warning: %s — %s",
                        w.get("warning"),
                        w.get("description"),
                    )
                else:
                    logger.warning("EIA warning: %s", w)

        return payload

    def iter_data_pages(
        self,
        route: str,
        *,
        base_params: list[tuple[str, str]],
        page_length: int = MAX_PAGE_LENGTH,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Repeat GET with increasing offset until all rows are fetched or a page is short.

        `route` should include `/data/` for tabular routes, e.g. `crude-oil-imports/data/`.
        """
        if page_length > MAX_PAGE_LENGTH:
            raise ValueError(f"page_length cannot exceed {MAX_PAGE_LENGTH}")

        offset = 0
        while True:
            params = list(base_params)
            params.append(("offset", str(offset)))
            params.append(("length", str(page_length)))
            payload = self.get_json(route, params)
            response = payload["response"]
            rows = response.get("data") or []
            if not isinstance(rows, list):
                raise EIAAPIError("EIA response.response.data is not a list")

            total_raw = response.get("total", "0")
            try:
                total = int(str(total_raw))
            except ValueError:
                total = -1

            logger.info(
                "EIA page offset=%s rows=%s total=%s route=%s",
                offset,
                len(rows),
                total,
                route,
            )
            yield rows

            if not rows:
                break
            if len(rows) < page_length:
                break
            offset += page_length
            if total >= 0 and offset >= total:
                break

    def fetch_all_data_rows(
        self,
        route: str,
        *,
        base_params: list[tuple[str, str]],
        page_length: int = MAX_PAGE_LENGTH,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for page in self.iter_data_pages(route, base_params=base_params, page_length=page_length):
            out.extend(page)
        return out
