"""Pattern Lab: local research tooling built on stable Parquet market-data packs.

The package stays independent of the Flask application, Merlin storage, strategy
packages and Numba.  ``tools.pattern_lab.data`` holds the public data API and
imports PyArrow lazily, so ``--help`` and dependency reporting still work when
the pinned wheel is absent from the environment.
"""


class PatternLabError(Exception):
    """Base class for every Pattern Lab failure reported to a caller.

    ``error_code`` is a stable machine-readable label carried into the JSON that
    the collector commands print for a failed operation.  Subclasses declare a
    default; an individual raise may override it for a specific condition.
    """

    error_code = "pattern_lab_error"

    def __init__(self, *args, error_code: str | None = None):
        super().__init__(*args)
        if error_code is not None:
            self.error_code = error_code


class PatternLabDataError(PatternLabError):
    """Invalid market data, metadata, request interval or pack layout."""

    error_code = "invalid_data"


class PatternLabDependencyError(PatternLabError):
    """A declared third-party dependency is missing from this environment."""

    error_code = "missing_dependency"


class PatternLabStudyError(PatternLabError):
    """A study or report operation failed; ``context`` names the phase and job.

    Unexpected execution failures are translated into this structured error with
    the original exception preserved as the cause, so a diagnostic never loses
    the underlying traceback.
    """

    error_code = "study_failed"

    def __init__(self, *args, error_code: str | None = None, context=None):
        super().__init__(*args, error_code=error_code)
        self.context = dict(context or {})


class PatternLabBusyError(PatternLabError):
    """Another process holds the data root's cooperative exclusion lock."""

    error_code = "pack_busy"


class PatternLabPendingError(PatternLabError):
    """A valid pending operation must be recovered or aborted first."""

    error_code = "pending_operation"


__all__ = [
    "PatternLabError",
    "PatternLabDataError",
    "PatternLabDependencyError",
    "PatternLabStudyError",
    "PatternLabBusyError",
    "PatternLabPendingError",
]
