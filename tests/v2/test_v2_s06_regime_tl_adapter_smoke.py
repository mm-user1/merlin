import pandas as pd

from core.engine_v2.profile import is_v2_config, parse_execution_profile
from strategies import get_strategy, get_strategy_config
from strategies.s06_r_trend_v02_b2.strategy import S06RTrendV02B2
from strategies.s06_r_trend_v02_regime_trendlines_b2.strategy import (
    S06RTrendV02RegimeTLB2,
    default_params_from_config,
    load_profile,
)

from s06_regime_tl_test_helpers import (
    BASELINE_START,
    REFERENCE_B,
    merged_reference_params,
    prepared_reference_dataset,
)


STRATEGY_ID = "s06_r_trend_v02_regime_trendlines_b2"


def test_regime_tl_is_discoverable_without_replacing_existing_strategies():
    assert get_strategy(STRATEGY_ID) is S06RTrendV02RegimeTLB2
    assert get_strategy("s06_r_trend_v02_b2") is S06RTrendV02B2
    assert get_strategy_config(STRATEGY_ID)["id"] == STRATEGY_ID
    assert get_strategy_config("s06_r_trend_v02_b2")["id"] == "s06_r_trend_v02_b2"


def test_regime_tl_config_validates_as_v2_with_roles_for_optimized_parameters():
    config = get_strategy_config(STRATEGY_ID)

    assert is_v2_config(config)
    profile = parse_execution_profile(config)
    assert profile.engine == "v2"
    assert "start" not in config["parameters"]
    assert "end" not in config["parameters"]
    assert all(spec.get("type") != "datetime" for spec in config["parameters"].values())
    for name, spec in config["parameters"].items():
        if spec.get("optimize", {}).get("enabled", False):
            assert spec["role"] in {"signal", "execution", "runtime"}, name


def test_regime_tl_regime_axes_are_opt_in_only():
    config = get_strategy_config(STRATEGY_ID)
    parameters = config["parameters"]

    # useRegime is a fixed per-study selector: never axis-available.
    assert parameters["useRegime"]["optimize"] == {"enabled": False}
    # Regime numerics carry optimize metadata for future regime studies but do
    # not silently join the default grid axis set.
    for name in ("regimePivotLen", "regimeSlopeFactor", "regimeBreakBufferX"):
        optimize = parameters[name]["optimize"]
        assert optimize["enabled"] is True
        assert optimize["default_enabled"] is False
    # The baseline slope factor 0.25 must stay inside the config domain.
    assert parameters["regimeSlopeFactor"]["min"] <= 0.25


def test_regime_tl_cached_defaults_return_fresh_mutable_copies():
    first = default_params_from_config()
    second = default_params_from_config()

    assert first == second
    assert first is not second
    first["useRegime"] = False
    assert default_params_from_config()["useRegime"] == second["useRegime"]
    assert load_profile() is load_profile()


def test_regime_tl_adapter_returns_enriched_strategy_result_on_small_window():
    end = BASELINE_START + pd.Timedelta(days=30)
    params = merged_reference_params(REFERENCE_B, {"end": end.isoformat().replace("+00:00", "Z")})
    prepared, trade_start_idx = prepared_reference_dataset(end=end)

    result = S06RTrendV02RegimeTLB2.run(prepared, params, trade_start_idx=trade_start_idx)

    assert result.total_trades == len(result.trades)
    assert len(result.balance_curve) == len(prepared)
    for trade in result.trades:
        assert trade.direction in {"long", "short"}
        assert pd.Timestamp(trade.entry_time) >= BASELINE_START
