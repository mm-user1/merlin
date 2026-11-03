"""Continuous, independent Fixed research accounts and offline views."""

from contextlib import redirect_stdout
import sys

with redirect_stdout(sys.stderr):
    from .. import config as _bootstrap


def compare_fixed(*args, **kwargs):
    """Compare validated saved independent accounts without live source access."""
    from .compare import compare_fixed as compare
    return compare(*args, **kwargs)
