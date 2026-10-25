"""Shared pack failures with stable historical machine error codes."""

class DataProviderError(Exception):
    """Base class for shared data-provider failures reported to a caller.

    ``error_code`` is a stable machine-readable label carried into the JSON that
    the collector commands print for a failed operation.  Subclasses declare a
    default; an individual raise may override it for a specific condition.
    """

    error_code = "pattern_lab_error"

    def __init__(self, *args, error_code: str | None = None):
        super().__init__(*args)
        if error_code is not None:
            self.error_code = error_code


class DataError(DataProviderError):
    """Invalid market data, metadata, request interval or pack layout."""

    error_code = "invalid_data"


class DependencyError(DataProviderError):
    """A declared third-party dependency is missing from this environment."""

    error_code = "missing_dependency"


class BusyError(DataProviderError):
    """Another process holds the data root's cooperative exclusion lock."""

    error_code = "pack_busy"


class PendingError(DataProviderError):
    """A valid pending operation must be recovered or aborted first."""

    error_code = "pending_operation"
