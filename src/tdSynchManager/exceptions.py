"""Custom exceptions for tdSynchManager data consistency and error handling."""

__all__ = [
    "TdSynchError",
    "SessionClosedError",
    "TruncatedResponseError",
    "InfluxDBAuthError",
    "ValidationError",
]


class TdSynchError(Exception):
    """Base exception for all tdSynchManager errors."""
    pass


class SessionClosedError(TdSynchError):
    """Raised when ThetaData session is closed and needs reconnection."""
    pass


class TruncatedResponseError(TdSynchError):
    """Raised when API response appears truncated or incomplete."""
    pass


class InfluxDBAuthError(TdSynchError):
    """Raised when InfluxDB authentication fails (401/403)."""
    pass


class ValidationError(TdSynchError):
    """Raised when data validation fails."""
    pass
