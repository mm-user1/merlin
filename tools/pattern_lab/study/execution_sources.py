"""Bounded file-level attribution for generic execution, without importing core.

This conservative contract includes admission for both execution routes and
quantity for native accounts even where no function from those files is called.
It is an explicit inventory, not a transitive import graph or execution guard.
"""
from pathlib import Path

from .. import PatternLabDataError


GENERIC_EXECUTION_PATHS = (
    "src/core/backtest_engine.py",
    "src/core/engine_v2/admission.py",
    "src/core/engine_v2/contracts.py",
    "src/core/engine_v2/diagnostics.py",
    "src/core/engine_v2/execution_modes.py",
    "src/core/engine_v2/kernel.py",
    "src/core/engine_v2/price_rounding.py",
    "src/core/engine_v2/quantity.py",
    "src/core/engine_v2/sizing.py",
)
INVENTORY_PATH = "tools/pattern_lab/study/execution_sources.py"


def execution_source_paths(repo: Path) -> tuple[str, ...]:
    """Require every generic source and this inventory's own implementation."""
    paths = (*GENERIC_EXECUTION_PATHS, INVENTORY_PATH)
    for name in paths:
        if not (repo / name).is_file():
            raise PatternLabDataError(f"Missing required execution implementation source: {name}.")
    return paths
