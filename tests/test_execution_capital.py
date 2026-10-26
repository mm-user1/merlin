"""Execution capital is an economic denominator, independent of metric anchors."""
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.backtest_engine import StrategyResult, prepare_dataset_with_warmup
from core.execution_capital import resolve_execution_capital
from core import metrics
from core.testing import build_test_metrics, run_period_test_for_trials
from strategies import get_strategy, get_strategy_config
from tests._data_input_helpers import STRATEGY


@pytest.mark.parametrize("capital", [100, 250, 1000])
@pytest.mark.parametrize("factors,profit,dd", [([1, 1, 1], 0, 0),
    ([1.1, .99, 1.2], 20, 10), ([1.1, .99, .9], -10, 100 * .2 / 1.1),
    ([1, 1.1, 1.2], 20, 0)])
def test_metric_denominator_is_not_first_observation_or_advanced_anchor(capital, factors, profit, dd):
    balance = [capital * x for x in factors]
    result = StrategyResult(trades=[], balance_curve=balance,
        equity_curve=[x + capital * .2 for x in balance],
        timestamps=list(pd.date_range("2024-01-01", periods=3, tz="UTC")),
        metric_initial_equity=capital * .8)
    actual = build_test_metrics(result, capital)
    assert actual["net_profit_pct"] == pytest.approx(profit)
    assert actual["max_drawdown_pct"] == pytest.approx(dd)
    expected_romad = profit / dd if dd else max(0, profit * 100)
    assert actual["romad"] == pytest.approx(expected_romad)
    view = metrics._advanced_metric_view(result, initial_balance=capital)
    assert view.initial_equity == capital * .8
    assert actual["ulcer_index"] == metrics.calculate_advanced(result, capital).ulcer_index


@pytest.mark.parametrize("bad", [None, True, False, 0, -1, float("nan"), float("inf"), "", "bad", [], {}])
def test_invalid_declared_capital_is_not_defaulted(bad):
    with pytest.raises(ValueError, match="finite and positive"):
        resolve_execution_capital(STRATEGY, {"initialCapital": bad})
    assert resolve_execution_capital("s01_trailing_ma", {"initialCapital": bad}) == 100


def test_declared_default_and_missing_contract(monkeypatch):
    assert resolve_execution_capital(STRATEGY, {}) == get_strategy_config(STRATEGY)["parameters"]["initialCapital"]["default"]
    assert resolve_execution_capital(STRATEGY, {"initialCapital": "250"}) == 250
    monkeypatch.setattr("strategies.get_strategy_config", lambda _: {"parameters": {"initialCapital": {}}})
    with pytest.raises(ValueError, match="finite and positive"):
        resolve_execution_capital(STRATEGY, {})
    monkeypatch.setattr("strategies.get_strategy_config", lambda _: {"parameters": {}})
    with pytest.raises(ValueError, match="contract"):
        resolve_execution_capital("future_strategy", {})


@pytest.fixture
def capital_frame():
    rng = np.random.default_rng(7)
    index = pd.date_range("2024-01-01", periods=3000, freq="5min", tz="UTC")
    close = pd.Series(100 + 8 * np.sin(np.arange(3000) / 90) + np.cumsum(rng.normal(0, .15, 3000)), index=index)
    opened = close.shift(1).fillna(close.iloc[0])
    return pd.DataFrame(dict(Open=opened, High=np.maximum(opened, close) + .2,
        Low=np.minimum(opened, close) - .2, Close=close, Volume=1000 + rng.random(3000)), index=index)


def execution_params(capital):
    params = {k: v.get("default") for k, v in get_strategy_config(STRATEGY)["parameters"].items() if k != "warmupBars"}
    return {**params, "initialCapital": capital, "useRegime": False, "maLength3": 10}


@pytest.mark.parametrize("use_resolver,expected", [(False, 250), (True, 1000)])
def test_period_uses_final_candidate_or_runtime_resolver(capital_frame, use_resolver, expected):
    frame = capital_frame
    start, end = frame.index[1500], frame.index[-1]
    params = execution_params(expected)
    params.update(dateFilter=True, start=start, end=end)
    prepared, idx = prepare_dataset_with_warmup(frame, start, end, 200)
    raw = get_strategy(STRATEGY).run(prepared, params, idx)
    assert raw.trades
    actual = run_period_test_for_trials(df=frame, strategy_id=STRATEGY, warmup_bars=200,
        fixed_params=execution_params(100), start_ts=start, end_ts=end,
        trials=[dict(trial_number=1, params={"initialCapital": 250})], baseline_period_days=5,
        test_period_days=5, original_metrics_resolver=lambda _: dict(net_profit_pct=5, max_drawdown_pct=2),
        execution_params_resolver=(lambda *args: deepcopy(params)) if use_resolver else None)[0]
    assert actual["test_metrics"]["net_profit_pct"] == pytest.approx((raw.balance_curve[-1] / expected - 1) * 100)
    assert actual["test_metrics"]["romad"] == metrics.calculate_advanced(raw, expected).romad


def test_s01_actual_execution_ignores_extraneous_capital(capital_frame):
    strategy = get_strategy("s01_trailing_ma")
    params = {k: v.get("default") for k, v in get_strategy_config("s01_trailing_ma")["parameters"].items()}
    params["dateFilter"] = False
    original = strategy.run(capital_frame, params, 200)
    for value in (250, None, "invalid"):
        changed = strategy.run(capital_frame, {**params, "initialCapital": value}, 200)
        assert asdict(changed) == asdict(original)
        assert resolve_execution_capital("s01_trailing_ma", {"initialCapital": value}) == 100


def test_serial_and_spawn_ft_and_stress_use_candidate_capital(capital_frame):
    from core.post_process import (_ft_worker_entry, _run_is_backtest,
        run_forward_test, run_perturbations_parallel, PostProcessConfig, ft_result_meets_threshold,
        calculate_retention_metrics)
    frame = capital_frame
    start, end = frame.index[1500], frame.index[-1]
    candidates, expected, perturbations = [], {}, []
    for number, capital in enumerate((250, 1000), 1):
        params = execution_params(capital)
        params.update(dateFilter=True, start=start, end=end)
        prepared, idx = prepare_dataset_with_warmup(frame, start, end, 200)
        raw = get_strategy(STRATEGY).run(prepared, params, idx)
        assert raw.trades
        expected[number] = build_test_metrics(raw, capital)
        baseline = dict(net_profit_pct=5., max_drawdown_pct=2., total_trades=2)
        task = dict(trial_number=number, source_rank=number, params=params, is_metrics=baseline)
        ft = _ft_worker_entry("", STRATEGY, task, str(start), str(end), 200, 5, 5, prepared_frame=frame)
        assert ft is not None
        assert ft["ft_metrics"]["net_profit_pct"] == expected[number]["net_profit_pct"]
        assert ft["ft_metrics"]["romad"] == expected[number]["romad"]
        assert metrics.calculate_basic(raw, 100).net_profit_pct > 0
        assert expected[number]["net_profit_pct"] < 0
        assert not ft_result_meets_threshold({"ft_net_profit_pct": expected[number]["net_profit_pct"]}, 0)
        candidates.append(SimpleNamespace(params=params, optuna_trial_number=number, **baseline))
        perturbations.append(dict(params=params, perturbed_param="initialCapital", direction=1,
            base_value=100, perturbed_value=capital))
        stress = _run_is_backtest("", STRATEGY, params, str(start), str(end), 200, prepared_frame=frame)
        assert stress is not None and stress["net_profit_pct"] == expected[number]["net_profit_pct"]
        assert stress["romad"] == expected[number]["romad"]
    ft_results = run_forward_test(csv_path="", strategy_id=STRATEGY, optuna_results=candidates,
        config=PostProcessConfig(enabled=True, top_k=2, warmup_bars=200), is_period_days=5,
        ft_period_days=5, ft_start_date=str(start), ft_end_date=str(end), n_workers=2, prepared_frame=frame)
    assert len(ft_results) == 2
    for item in ft_results:
        assert item.ft_net_profit_pct == expected[item.trial_number]["net_profit_pct"]
        assert item.ft_romad == expected[item.trial_number]["romad"]
    stress_results = run_perturbations_parallel("", STRATEGY, perturbations, str(start), str(end),
        {"initialCapital": 100}, 200, 2, prepared_frame=frame)
    assert len(stress_results) == 2
    for item in stress_results:
        number = 1 if item["perturbed_value"] == 250 else 2
        assert item["net_profit_pct"] == expected[number]["net_profit_pct"]
        assert item["romad"] == expected[number]["romad"]
    retention = calculate_retention_metrics({"net_profit_pct": 10, "romad": 2},
        [{"net_profit_pct": 5, "romad": 1}, {"net_profit_pct": 15, "romad": 3}])
    assert retention.profit_retention == pytest.approx(.775)
    assert retention.romad_retention == pytest.approx(.775)
    rejected = calculate_retention_metrics(expected[2], stress_results)
    assert rejected.status.value == "skipped_bad_base"


@pytest.mark.parametrize("capital", [250, 1000])
def test_wfa_flat_prefix_and_proportional_stitch(capital):
    from core.walkforward_engine import WalkForwardEngine, WFConfig, StitchWindow
    start = pd.Timestamp("2024-01-01", tz="UTC")
    end = start + pd.Timedelta(days=4)
    engine = WalkForwardEngine(WFConfig(strategy_id=STRATEGY), {}, {})
    flat = engine._build_flat_result(start, end, capital)
    assert flat.balance_curve == flat.equity_curve == [capital, capital]
    assert build_test_metrics(flat, capital)["net_profit_pct"] == 0
    live = start + pd.Timedelta(days=2)
    for anchor in (None, capital * .9):
        raw = StrategyResult(trades=[], balance_curve=[capital, capital * 1.1],
            equity_curve=[capital, capital * 1.1], timestamps=[live, end], metric_initial_equity=anchor)
        prefixed = engine._prepend_flat_prefix(raw, scheduled_start=start, live_start=live, initial_balance=capital)
        assert prefixed.equity_curve[0] == (capital if anchor is None else anchor)
        assert prefixed.metric_initial_equity == (capital if anchor is None else anchor)
    def stitch(scale):
        return engine._stitch_windows([StitchWindow(window_id=n, oos_start=start,
            oos_equity_curve=[scale, scale * 1.1, scale * 1.05],
            oos_timestamps=[start, live, end], oos_total_trades=1) for n in (1, 2)], False)[0]
    assert stitch(capital) == pytest.approx([100, 110, 105, 115.5, 110.25])
    assert stitch(capital) == pytest.approx(stitch(100))


@pytest.mark.parametrize("adaptive", [False, True])
@pytest.mark.parametrize("mode", ["dense", "delayed", "no_trade"])
def test_wfa_window_processing_capital_and_truncation(monkeypatch, adaptive, mode):
    from core.walkforward_engine import WalkForwardEngine, WFConfig, ISPipelineResult, WindowExecutionPlan
    capital = 1000
    index = pd.date_range("2024-01-01", periods=26, freq="D", tz="UTC")
    frame = pd.DataFrame(dict(Open=1., High=1., Low=1., Close=1., Volume=1.), index=index)
    config = WFConfig(strategy_id=STRATEGY, is_period_days=10, oos_period_days=6,
        adaptive_mode=adaptive, max_oos_period_days=6, min_oos_trades=1, check_interval_trades=1)
    engine = WalkForwardEngine(config, {"fixed_params": {"dateFilter": False}}, {})
    measured = []
    original_advanced = metrics.calculate_advanced
    def advanced(result, initial_balance=None, **kwargs):
        value = original_advanced(result, initial_balance, **kwargs)
        measured.append((deepcopy(result), initial_balance, kwargs, value))
        return value
    monkeypatch.setattr(metrics, "calculate_advanced", advanced)
    def plan(self, **kwargs):
        delay = 2 if mode == "delayed" else 0
        live = kwargs["oos_start"] + pd.Timedelta(days=delay)
        pipeline = ISPipelineResult(best_result=SimpleNamespace(score=0.), best_params={"initialCapital": capital},
            param_id="fixed", best_trial_number=1, best_params_source="optuna_is", is_pareto_optimal=None,
            constraints_satisfied=None, available_modules=["optuna_is"], module_status={}, selection_chain={},
            optimization_start=kwargs["is_start"], optimization_end=kwargs["is_end"], ft_start=None, ft_end=None,
            optuna_is_trials=[], dsr_trials=None, forward_test_trials=None, stress_test_trials=None)
        return WindowExecutionPlan(pipeline=pipeline, final_is_start=kwargs["is_start"], final_is_end=kwargs["is_end"],
            trade_start=None if mode == "no_trade" else live, entry_delay_days=delay, ft_retry_attempts_used=delay > 0,
            remaining_oos_days_at_entry=(kwargs["oos_budget_end"] - live).days,
            window_status="no_trade" if mode == "no_trade" else "traded", no_trade_reason="test" if mode == "no_trade" else None)
    def backtest(self, df, start, end, params):
        assert params["initialCapital"] == capital
        stamps = list(pd.date_range(start, end, freq="D"))
        # First observation already contains PnL; ending MTM differs from realized balance.
        balances = [capital * (1.02 + i * .01) for i in range(len(stamps))]
        return StrategyResult(trades=[], balance_curve=balances, equity_curve=[x + 3 for x in balances],
            timestamps=stamps, metric_initial_equity=capital)
    def scan(self, **kwargs):
        cutoff = min(kwargs["oos_start"] + pd.Timedelta(days=2), kwargs["oos_max_end"])
        return SimpleNamespace(triggered=True, trigger_type="cusum", trigger_trade_idx=None, trigger_time=cutoff,
            cusum_final=6., cusum_threshold=5., dd_peak=0., dd_threshold=0., oos_actual_trades=0,
            oos_actual_days=(cutoff - kwargs["oos_start"]).total_seconds() / 86400)
    monkeypatch.setattr(WalkForwardEngine, "_resolve_window_execution_plan", plan)
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", backtest)
    monkeypatch.setattr(WalkForwardEngine, "_scan_triggers", scan)
    result, _ = engine.run_wf_optimization(frame)
    assert result.windows and len(measured) == len(result.windows) * 2
    scalar = 100.
    for window, (is_fact, oos_fact) in zip(result.windows, zip(measured[::2], measured[1::2])):
        for prefix, fact in (("is", is_fact), ("oos", oos_fact)):
            raw, supplied, options, advanced_value = fact
            assert supplied == capital
            assert getattr(window, prefix + "_net_profit_pct") == pytest.approx((raw.balance_curve[-1] / capital - 1) * 100)
            assert getattr(window, prefix + "_romad") == advanced_value.romad
            assert options.get("compute_sharpe_daily", False) is (prefix == "is" or mode == "dense")
        raw = oos_fact[0]
        assert window.oos_equity_curve == []  # Dense results are transient, not persisted per window.
        if mode == "no_trade":
            assert raw.balance_curve == [capital, capital]
            assert window.oos_net_profit_pct == 0
        else:
            assert raw.balance_curve[-1] > capital
            if adaptive:
                assert window.trigger_type == "cusum"
                assert raw.timestamps[-1] <= window.trade_start + pd.Timedelta(days=2)
            if mode == "delayed":
                assert raw.balance_curve[0] == capital
                assert raw.metric_initial_equity == capital
        scalar *= raw.balance_curve[-1] / raw.balance_curve[0]
    assert result.stitched_oos.final_net_profit_pct == pytest.approx(scalar - 100)
    assert result.stitched_oos.equity_curve[0] == 100
    assert result.stitched_oos.oos_win_rate == (0 if mode == "no_trade" else 100)
    avg_is = sum(w.is_net_profit_pct for w in result.windows) / len(result.windows)
    if adaptive:
        oos_per_day = sum(w.oos_net_profit_pct for w in result.windows) / sum(w.oos_elapsed_days for w in result.windows)
    else:
        oos_per_day = sum(w.oos_net_profit_pct for w in result.windows) / len(result.windows) / 6
    assert result.stitched_oos.wfe == pytest.approx(oos_per_day / (avg_is / 10) * 100)
