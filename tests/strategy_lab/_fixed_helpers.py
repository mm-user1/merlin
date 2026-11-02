"""Small public-provider fixtures for continuous Fixed tests; no ignored inputs."""
from copy import deepcopy
import json

import pytest

from tools.strategy_lab.config import REPO_ROOT
from tools.strategy_lab.synthetic_parquet import publish


def request(*, series=False):
    raw = json.loads((REPO_ROOT / "tools/strategy_lab/runspecs/s07_fixed_example.json").read_text())
    if not series:
        g = raw["generation"]
        g["parameter_sets"] = g["parameter_sets"][:1]
        g["market_data"]["instruments"] = g["market_data"]["instruments"][:1]
        g["timeframes_minutes"] = [60]
        g["periods"] = g["periods"][:1]
        g["expected_run_count"] = 1
    return deepcopy(raw)


def save_request(tmp_path, raw, name="fixed.json"):
    path = tmp_path / name
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def snapshot(root):
    return {p.relative_to(root).as_posix():(p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()}


@pytest.fixture(scope="module")
def fixed_pack(tmp_path_factory):
    root = tmp_path_factory.mktemp("fixed-pack") / "pack"
    publish(root, symbols=("AAA", "BBB"), start="2024-01-10", end="2024-04-03")
    return root
