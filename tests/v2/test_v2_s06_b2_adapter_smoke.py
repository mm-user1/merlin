import json
from pathlib import Path

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.engine_v2.profile import is_v2_config, parse_execution_profile
from strategies import get_strategy, get_strategy_config
from strategies.s06_r_trend_v02.strategy import S06RTrendV02
from strategies.s06_r_trend_v02_b2.strategy import (
    S06RTrendV02B2,
    default_params_from_config,
    load_profile,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_b2_is_discoverable_without_replacing_v1_strategy():
    assert get_strategy("s06_r_trend_v02") is S06RTrendV02
    assert get_strategy("s06_r_trend_v02_b2") is S06RTrendV02B2
    assert get_strategy_config("s06_r_trend_v02")["id"] == "s06_r_trend_v02"
    assert get_strategy_config("s06_r_trend_v02_b2")["id"] == "s06_r_trend_v02_b2"


def test_b2_config_validates_as_v2_with_roles_for_optimized_parameters():
    config = get_strategy_config("s06_r_trend_v02_b2")

    assert is_v2_config(config)
    profile = parse_execution_profile(config)
    assert profile.engine == "v2"
    assert "start" not in config["parameters"]
    assert "end" not in config["parameters"]
    assert all(spec.get("type") != "datetime" for spec in config["parameters"].values())
    for name, spec in config["parameters"].items():
        if spec.get("optimize", {}).get("enabled", False):
            assert spec["role"] in {"signal", "execution", "runtime"}, name


def test_b2_cached_defaults_return_fresh_mutable_copies():
    first = default_params_from_config()
    second = default_params_from_config()

    assert first == second
    assert first is not second
    first["entryMode"] = "Trend @ Square"
    assert default_params_from_config()["entryMode"] == second["entryMode"]
    assert load_profile() is load_profile()


def test_b2_adapter_returns_enriched_strategy_result_on_small_window():
    reference_dir = REPO_ROOT / "data" / "baseline_v2" / "s06_r_trend_v02" / "reference_b_trend_bracket"
    with (reference_dir / "params.json").open(encoding="utf-8") as handle:
        params = json.load(handle)["strategy_inputs"]
    df = load_data(REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv")
    prepared, trade_start_idx = prepare_dataset_with_warmup(
        df,
        params.get("start"),
        params.get("end"),
        1000,
    )

    assert prepared.index[0] == df.index[0]
    assert trade_start_idx == 0

    result = get_strategy("s06_r_trend_v02_b2").run(
        prepared.iloc[:1100],
        params,
        trade_start_idx=trade_start_idx,
    )

    assert isinstance(result.total_trades, int)
    assert isinstance(result.equity_curve, list)
    assert result.profit_factor is None or result.profit_factor >= 0.0
