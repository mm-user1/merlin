"""Local research tooling; shared pack modules are canonical provider objects."""
from importlib import import_module
import os
from pathlib import Path
import sys


def _bootstrap_provider():
    source = Path(__file__).resolve().parents[2] / "src"
    provider = source / "data_provider"
    for name, module in tuple(sys.modules.items()):
        if name == "data_provider" or name.startswith("data_provider."):
            suffix = name.split(".")[1:]
            expected = provider.joinpath(*suffix).with_suffix(".py") if suffix else provider / "__init__.py"
            actual = getattr(module, "__file__", None)
            if actual is None or os.path.normcase(str(Path(actual).resolve())) != os.path.normcase(str(expected.resolve())):
                raise ImportError(f"Foreign data_provider module {name}; use this checkout in a fresh interpreter.")
    sys.path.insert(0, str(source))
    import_module("data_provider")


_bootstrap_provider()

from data_provider.errors import (
    DataProviderError as PatternLabError,
    DataError as PatternLabDataError,
    DependencyError as PatternLabDependencyError,
    BusyError as PatternLabBusyError,
    PendingError as PatternLabPendingError,
)

DEFAULT_ROSTER_PATH = Path(__file__).resolve().parent / "configs" / "universe.json"
_SHARED_MODULES = ("data", "manifest", "pack_lock", "update_transaction", "exchange_data", "collect")


def __getattr__(name):
    if name in _SHARED_MODULES:
        module = import_module("data_provider." + name)
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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


__all__ = ['PatternLabError', 'PatternLabDataError', 'PatternLabDependencyError', 'PatternLabBusyError', 'PatternLabPendingError', 'PatternLabStudyError', 'DEFAULT_ROSTER_PATH', 'data', 'manifest', 'pack_lock', 'update_transaction', 'exchange_data', 'collect']
