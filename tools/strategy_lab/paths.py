"""Shared immutable-evidence locations; no execution imports."""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTECTED_CANONICAL_OUTPUTS = tuple(
    (REPO_ROOT / "tools" / "strategy_lab" / "output" / name).resolve()
    for name in ("s06_bracket_mvp_pre_mtm_v1", "s06_bracket_mvp_mtm_v2")
)
