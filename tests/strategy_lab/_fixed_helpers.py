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


def comparison_account(params, ticker="A", *, period=("2024-01-01", "2024-02-01"),
                       stratum="s", filter_key="off", timeframe=60, status="completed", value=1):
    """Detached reducer fixture; identities use the same declared match dimensions."""
    from tools.strategy_lab.analysis.research_inputs import digest
    variant = dict(params=params, filters=filter_key, timeframe_minutes=timeframe)
    return dict(account_id=digest([variant, ticker, period, stratum]), variant=variant,
        variant_key=digest(variant), stratum_key=stratum, filter_key=filter_key,
        strategy_params=params, timeframe_minutes=timeframe, ticker=ticker, period_key=period,
        status=status, input_pin=dict(input_fingerprint=ticker,
            bounds=dict(evaluation_start=period[0], evaluation_end=period[1])), admission={"mode": "test"},
        metrics={m: value for m in ("net_profit_pct", "max_drawdown_pct", "max_drawdown_mtm_pct", "total_trades")})


@pytest.fixture(scope="module")
def fixed_pack(tmp_path_factory):
    root = tmp_path_factory.mktemp("fixed-pack") / "pack"
    publish(root, symbols=("AAA", "BBB"), start="2024-01-10", end="2024-04-03")
    return root
