"""Fixed planning and successful/error result boundaries on synthetic data."""
from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from core import grid_engine
from core.grid_v2 import build_grid_v2_plan
from core.parameter_mode import normalize_parameter_mode
from strategies import get_strategy_config
from ui.server_services import _build_optimization_config


PROFILES = [
    "s03_reversal_v11_regime_er_b2", "s03_reversal_v16_4_a_adaptive_ma_b2",
    "s06_r_trend_v02_b2", "s06_r_trend_v02_regime_trendlines_b2",
    "s06_r_trend_v06_4_a2_b2",
]


def config_for(strategy=PROFILES[0], **params):
    return _build_optimization_config("unused.csv", dict(optimization_mode="grid", parameterMode="fixed",
        enabled_params={}, param_ranges={}, fixed_params=dict(dateFilter=False, **params)), 1, strategy, 100)


@pytest.mark.parametrize("strategy", PROFILES)
def test_fixed_profile_is_actual_singleton(strategy):
    config = config_for(strategy)
    plan = build_grid_v2_plan(get_strategy_config(strategy),
        settings=grid_engine._grid_v2_settings_from_config(config), base_params=config.fixed_params)
    assert plan.deduped_candidate_count == 1
    assert plan.settings.enabled_axes == ()
    assert grid_engine.preview_grid_parameter_space(config)["planned_candidate_count"] == 1


@pytest.mark.parametrize("params", [dict(maLength3=10_000_000), dict(maType3="INVALID"),
    dict(maLength3=1.5), dict(maLength3=True), dict(maOffset3=float("nan")), dict(useRegime="perhaps")])
def test_effective_scalar_domains(params):
    with pytest.raises(ValueError):
        config_for(**params)


def test_boolean_dependency_defaults_and_invalid_logical_group():
    config = config_for(useRegime="false", regimeERLength="ignored", useEmergencySL=False,
                        emergencySlPct="ignored")
    assert config.fixed_params["useRegime"] is False
    assert config.fixed_params["emergencySlPct"] == get_strategy_config(PROFILES[0])["parameters"]["emergencySlPct"]["default"]
    with pytest.raises(ValueError, match="no valid logical modes"):
        grid_engine.preview_grid_parameter_space(config_for(useCloseCount=False, useTBands=False))


def test_variant_scalar_wins_and_conflict_refused():
    config = config_for(PROFILES[-1], trailMode="Chandelier Exit")
    assert config.grid_enabled_modes == ["chandelier"]
    payload = dict(vars(config), grid_enabled_modes=["bracket"])
    with pytest.raises(ValueError, match="conflicts"):
        normalize_parameter_mode(payload, get_strategy_config(PROFILES[-1]))


@pytest.mark.parametrize("alias", [{"gridEnabledModes": ["bracket"]}, {"grid_config": {"enabled_modes": ["bracket"]}}])
def test_conflicting_mode_alias_cannot_hide_behind_empty_primary(alias):
    payload = dict(parameterMode="fixed", optimization_mode="grid", grid_enabled_modes=[],
                   fixed_params={"trailMode": "Chandelier Exit"}, **alias)
    with pytest.raises(ValueError, match="conflicts"):
        normalize_parameter_mode(payload, get_strategy_config(PROFILES[-1]))


def test_reference_exception_remains_actionable(monkeypatch):
    import core.grid_v2 as grid
    def failed_reference(**kwargs):
        raise RuntimeError("synthetic reference failure")
    monkeypatch.setattr(grid, "run_v2_strategy", failed_reference)
    config = config_for(useRegime=False)
    config.grid_v2_prefer_compiled = False
    frame = pd.DataFrame(dict(Open=[100.]*150, High=[101.]*150, Low=[99.]*150, Close=[100.]*150, Volume=[1.]*150),
        index=pd.date_range("2024-01-01", periods=150, freq="h", tz="UTC"))
    with pytest.raises(ValueError, match="synthetic reference failure"):
        grid_engine.run_grid_optimization(config, save_study=False, prepared_frame=frame)


def test_no_optimization_ranges_needed():
    original = get_strategy_config(PROFILES[0])
    config = deepcopy(original)
    for spec in config["parameters"].values():
        spec.pop("optimize", None)
    normalized = normalize_parameter_mode(dict(parameterMode="fixed", optimization_mode="grid", fixed_params={}), config)
    from core.grid_v2 import GridV2Settings
    plan = build_grid_v2_plan(config, settings=GridV2Settings(enabled_axes=()), base_params=normalized["fixed_params"])
    assert plan.deduped_candidate_count == 1


@pytest.mark.parametrize("no_trades", [False, True])
def test_success_retained_without_ranking_and_fast_reference_agree(monkeypatch, no_trades):
    x = np.arange(1800)
    close = 100 + .3 * np.sin(x / 17)
    frame = pd.DataFrame(dict(Open=close, High=close+1, Low=close-1, Close=close+.001, Volume=np.ones(len(x))),
        index=pd.date_range("2024-01-01", periods=len(x), freq="h", tz="UTC"))
    config = config_for(useRegime=False, useEmergencySL=False, maLength3=5, maOffset3=0, useTBands=False, closeCountLong=1, closeCountShort=1,
        enableLong=not no_trades, enableShort=not no_trades, commissionPct=1)
    def forbidden(*args, **kwargs):
        pytest.fail("Fixed must not rank or apply diversity")
    monkeypatch.setattr(grid_engine, "rank_grid_results", forbidden)
    monkeypatch.setattr(grid_engine, "apply_diversity_cap", forbidden)
    results, _ = grid_engine.run_grid_optimization(config, save_study=False, prepared_frame=frame)
    assert len(results) == 1 and results[0].grid_rank == 1
    result = results[0]
    if no_trades:
        assert result.total_trades == 0 and result.sharpe_ratio is None
    else:
        assert result.total_trades > 0 and result.net_profit_pct < 0
    for metric in ("net_profit_pct", "max_drawdown_pct", "total_trades", "win_rate"):
        tolerance = grid_engine.GridSettings().validation_tolerances[metric + "_abs"]
        assert getattr(result, metric) == pytest.approx(result.fast_metrics[metric], rel=0, abs=tolerance)


@pytest.mark.parametrize("compiled_flag", [False, True])
def test_error_row_is_never_a_zero_trade_success(monkeypatch, compiled_flag):
    import core.grid_v2 as grid
    original = grid.execute_grid_v2_candidates
    def damaged(*args, **kwargs):
        result = original(*args, **kwargs)
        if compiled_flag:
            values = np.zeros(26)
            values[grid.OUTPUT_FLAGS] = 256
            row = grid._row_from_compiled_output(args[0], 0, values)
        else:
            row = grid._error_row(args[0], 0, ValueError("reference exploded"))
        return replace(result, rows=(row,))
    monkeypatch.setattr(grid, "execute_grid_v2_candidates", damaged)
    frame = pd.DataFrame(dict(Open=[100.]*150, High=[101.]*150, Low=[99.]*150, Close=[100.]*150, Volume=[1.]*150),
        index=pd.date_range("2024-01-01", periods=150, freq="h", tz="UTC"))
    with pytest.raises(ValueError, match="Admission execution failed" if compiled_flag else "reference exploded"):
        grid_engine.run_grid_optimization(config_for(useRegime=False), save_study=False, prepared_frame=frame)


def test_ties_resolve_before_domain_validation():
    strategy = PROFILES[1]
    config = _build_optimization_config("unused.csv", dict(parameterMode="fixed", optimization_mode="grid",
        enabled_params={}, fixed_params={"closeCountLong": 4, "closeCountShort": "ignored", "useCloseCount": True},
        grid_v2_enabled_tie_groups=["symmetricLongShort"]), 1, strategy, 100)
    assert config.fixed_params["closeCountShort"] == config.fixed_params["closeCountLong"] == 4
    assert grid_engine.preview_grid_parameter_space(config)["planned_candidate_count"] == 1


def test_fixed_rejects_a_multi_candidate_plan_before_execution(monkeypatch):
    from types import SimpleNamespace
    import core.grid_v2 as grid
    original = grid.build_grid_v2_plan
    def bad_plan(*args, **kwargs):
        original(*args, **kwargs)
        return SimpleNamespace(deduped_candidate_count=2)
    monkeypatch.setattr(grid, "build_grid_v2_plan", bad_plan)
    monkeypatch.setattr(grid_engine, "_prepare_grid_dataframe", lambda *a: pytest.fail("must check before execution"))
    with pytest.raises(ValueError, match="conflicting modes/logical blocks"):
        grid_engine.run_grid_optimization(config_for(), save_study=False)
