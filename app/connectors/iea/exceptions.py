class IEAConnectorError(Exception):
    """Base class for IEA connector failures."""


class IEAConfigurationError(IEAConnectorError):
    """Missing URL, credentials, or other configuration required for a request."""
