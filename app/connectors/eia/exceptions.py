class EIAConnectorError(Exception):
    """Base class for EIA connector failures."""


class EIAAPIError(EIAConnectorError):
    """EIA returned a JSON error payload or unexpected body."""

    def __init__(self, message: str, *, code: str | None = None, http_status: int | None = None):
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class EIAHTTPError(EIAConnectorError):
    """Non-success HTTP status from the EIA API."""

    def __init__(self, message: str, *, status_code: int, body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body
