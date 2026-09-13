"""Pattern Lab: local research tooling built on stable Parquet market-data packs.

The package stays independent of the Flask application, Merlin storage, strategy
packages and Numba.  ``tools.pattern_lab.data`` holds the public data API and
imports PyArrow lazily, so ``--help`` and dependency reporting still work when
the pinned wheel is absent from the environment.
"""


class PatternLabError(Exception):
    """Base class for every Pattern Lab failure reported to a caller."""


class PatternLabDataError(PatternLabError):
    """Invalid market data, metadata, request interval or pack layout."""


class PatternLabDependencyError(PatternLabError):
    """A declared third-party dependency is missing from this environment."""


__all__ = ["PatternLabError", "PatternLabDataError", "PatternLabDependencyError"]
