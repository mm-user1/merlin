import sys
from types import SimpleNamespace
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.backtest_engine import StrategyResult, TradeRecord
from core.post_process import PostProcessConfig
from core.walkforward_engine import ISPipelineResult, WFConfig, WalkForwardEngine, WindowResult


def _build_engine(**kwargs):
    config = WFConfig(strategy_id="s01_trailing_ma", **kwargs)
    return WalkForwardEngine(config, {"fixed_params": {"dateFilter": False}}, {})


def _trade(exit_time: str, profit_pct: float) -> TradeRecord:
    return TradeRecord(exit_time=pd.Timestamp(exit_time, tz="UTC"), profit_pct=profit_pct)


def _result(trades, balances, timestamps):
    return StrategyResult(
        trades=list(trades),
        equity_curve=list(balances),
        balance_curve=list(balances),
        timestamps=list(timestamps),
    )


def test_compute_is_baseline_zero_trades_disables_triggers():
    engine = _build_engine()
    is_result = _result([], [100.0, 102.0], [pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2025-01-02", tz="UTC")])

    baseline = engine._compute_is_baseline(is_result, is_period_days=90)

    assert baseline["cusum_enabled"] is False
    assert baseline["drawdown_enabled"] is False
    assert baseline["inactivity_enabled"] is False


def test_compute_is_baseline_one_trade_uses_inactivity_fallback():
    engine = _build_engine()
    is_result = _result(
        [_trade("2025-01-05 00:00:00", 1.2)],
        [100.0, 101.0],
        [pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2025-01-05", tz="UTC")],
    )

    baseline = engine._compute_is_baseline(is_result, is_period_days=90)

    assert baseline["drawdown_enabled"] is True
    assert baseline["cusum_enabled"] is False
    assert baseline["inactivity_enabled"] is True
    assert baseline["max_trade_interval"] == pytest.approx(45.0)


def test_scan_triggers_drawdown_precedes_cusum():
    engine = _build_engine(min_oos_trades=1, check_interval_trades=1)
    baseline = {
        "mu": 1.0,
        "sigma": 1.0,
        "h": 0.1,
        "dd_limit": 1.0,
        "cusum_enabled": True,
        "drawdown_enabled": True,
        "inactivity_enabled": False,
    }
    trades = [_trade("2025-02-01 00:00:00", -5.0)]
    balances = [100.0, 98.0]
    timestamps = [
        pd.Timestamp("2025-01-31 00:00:00", tz="UTC"),
        pd.Timestamp("2025-02-01 00:00:00", tz="UTC"),
    ]

    trigger = engine._scan_triggers(
        trades=trades,
        balance_curve=balances,
        timestamps=timestamps,
        baseline=baseline,
        oos_start=pd.Timestamp("2025-01-31 00:00:00", tz="UTC"),
        oos_max_end=pd.Timestamp("2025-02-05 00:00:00", tz="UTC"),
    )

    assert trigger.trigger_type == "drawdown"
    assert trigger.trigger_trade_idx == 0


def test_scan_triggers_uses_fractional_days_for_inactivity():
    engine = _build_engine()
    baseline = {
        "h": 5.0,
        "dd_limit": 0.0,
        "cusum_enabled": False,
        "drawdown_enabled": False,
        "inactivity_enabled": True,
        "max_trade_interval": 0.25,
    }

    oos_start = pd.Timestamp("2025-03-01 00:00:00", tz="UTC")
    oos_end = pd.Timestamp("2025-03-02 00:00:00", tz="UTC")
    trigger = engine._scan_triggers(
        trades=[],
        balance_curve=[],
        timestamps=[],
        baseline=baseline,
        oos_start=oos_start,
        oos_max_end=oos_end,
    )

    assert trigger.trigger_type == "inactivity"
    assert trigger.oos_actual_days == pytest.approx(0.25)


def test_scan_triggers_between_trade_inactivity_returns_previous_trade_index():
    engine = _build_engine()
    baseline = {
        "h": 5.0,
        "dd_limit": 0.0,
        "cusum_enabled": False,
        "drawdown_enabled": False,
        "inactivity_enabled": True,
        "max_trade_interval": 1.0,
    }

    trades = [
        _trade("2025-04-02 00:00:00", 1.0),
        _trade("2025-04-05 00:00:00", 1.0),
    ]
    trigger = engine._scan_triggers(
        trades=trades,
        balance_curve=[],
        timestamps=[],
        baseline=baseline,
        oos_start=pd.Timestamp("2025-04-01 00:00:00", tz="UTC"),
        oos_max_end=pd.Timestamp("2025-04-10 00:00:00", tz="UTC"),
    )

    assert trigger.trigger_type == "inactivity"
    assert trigger.trigger_trade_idx == 0
    assert trigger.oos_actual_trades == 1


def test_adaptive_wfe_is_duration_weighted():
    engine = _build_engine(adaptive_mode=True, is_period_days=90, oos_period_days=30)
    windows = [
        WindowResult(
            window_id=1,
            is_start=pd.Timestamp("2025-01-01", tz="UTC"),
            is_end=pd.Timestamp("2025-03-31", tz="UTC"),
            oos_start=pd.Timestamp("2025-03-31", tz="UTC"),
            oos_end=pd.Timestamp("2025-04-10", tz="UTC"),
            best_params={},
            param_id="w1",
            is_net_profit_pct=10.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=10,
            oos_net_profit_pct=2.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 102.0],
            oos_timestamps=[
                pd.Timestamp("2025-03-31", tz="UTC"),
                pd.Timestamp("2025-04-10", tz="UTC"),
            ],
            oos_actual_days=10.0,
        ),
        WindowResult(
            window_id=2,
            is_start=pd.Timestamp("2025-02-01", tz="UTC"),
            is_end=pd.Timestamp("2025-05-02", tz="UTC"),
            oos_start=pd.Timestamp("2025-05-02", tz="UTC"),
            oos_end=pd.Timestamp("2025-07-31", tz="UTC"),
            best_params={},
            param_id="w2",
            is_net_profit_pct=10.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=10,
            oos_net_profit_pct=6.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 106.0],
            oos_timestamps=[
                pd.Timestamp("2025-05-02", tz="UTC"),
                pd.Timestamp("2025-07-31", tz="UTC"),
            ],
            oos_actual_days=90.0,
        ),
    ]

    stitched = engine._build_stitched_oos_equity(windows)
    assert stitched.wfe == pytest.approx(72.0, rel=1e-3)


def test_adaptive_wfe_uses_elapsed_days_for_annualization():
    engine = _build_engine(adaptive_mode=True, is_period_days=90, oos_period_days=30)
    windows = [
        WindowResult(
            window_id=1,
            is_start=pd.Timestamp("2025-01-01", tz="UTC"),
            is_end=pd.Timestamp("2025-03-31", tz="UTC"),
            oos_start=pd.Timestamp("2025-03-31", tz="UTC"),
            oos_end=pd.Timestamp("2025-04-10", tz="UTC"),
            best_params={},
            param_id="w1",
            is_net_profit_pct=10.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=10,
            oos_net_profit_pct=2.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 102.0],
            oos_timestamps=[
                pd.Timestamp("2025-03-31", tz="UTC"),
                pd.Timestamp("2025-04-10", tz="UTC"),
            ],
            oos_actual_days=10.0,
            oos_elapsed_days=20.0,
        ),
        WindowResult(
            window_id=2,
            is_start=pd.Timestamp("2025-02-01", tz="UTC"),
            is_end=pd.Timestamp("2025-05-02", tz="UTC"),
            oos_start=pd.Timestamp("2025-05-02", tz="UTC"),
            oos_end=pd.Timestamp("2025-07-31", tz="UTC"),
            best_params={},
            param_id="w2",
            is_net_profit_pct=10.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=10,
            oos_net_profit_pct=6.0,
            oos_max_drawdown_pct=1.0,
            oos_total_trades=3,
            oos_equity_curve=[100.0, 106.0],
            oos_timestamps=[
                pd.Timestamp("2025-05-02", tz="UTC"),
                pd.Timestamp("2025-07-31", tz="UTC"),
            ],
            oos_actual_days=90.0,
            oos_elapsed_days=90.0,
        ),
    ]

    stitched = engine._build_stitched_oos_equity(windows)
    assert stitched.wfe == pytest.approx(65.4545, rel=1e-3)


def test_resolve_adaptive_roll_end_skips_cooldown_for_inactivity():
    engine = _build_engine(adaptive_mode=True, cooldown_enabled=True, cooldown_days=15)
    actual_end = pd.Timestamp("2025-04-10", tz="UTC")
    trading_end = pd.Timestamp("2025-05-01", tz="UTC")

    adaptive_roll_end, cooldown_days = engine._resolve_adaptive_roll_end(
        SimpleNamespace(triggered=True, trigger_type="inactivity"),
        oos_actual_end=actual_end,
        trading_end=trading_end,
    )

    assert adaptive_roll_end == actual_end
    assert cooldown_days is None


def test_adaptive_cooldown_extends_next_window_after_cusum_trigger(monkeypatch):
    index = pd.date_range("2025-01-01 00:00:00", "2025-04-30 00:00:00", freq="1D", tz="UTC")
    df = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
        index=index,
    )

    config = WFConfig(
        strategy_id="s01_trailing_ma",
        adaptive_mode=True,
        is_period_days=20,
        max_oos_period_days=10,
        min_oos_trades=1,
        check_interval_trades=1,
        cooldown_enabled=True,
        cooldown_days=5,
    )
    base_template = {
        "fixed_params": {
            "dateFilter": True,
            "start": "2025-01-01T00:00",
            "end": "2025-04-30T00:00",
        },
    }
    engine = WalkForwardEngine(config, base_template, {})
    scan_calls = {"count": 0}

    def fake_pipeline(self, df, is_start, is_end, window_id):  # noqa: ARG001
        return ISPipelineResult(
            best_result=SimpleNamespace(score=0.0),
            best_params={},
            param_id=f"w{window_id}",
            best_trial_number=window_id,
            best_params_source="optuna_is",
            is_pareto_optimal=None,
            constraints_satisfied=None,
            available_modules=["optuna_is"],
            module_status={"optuna_is": {"enabled": True, "ran": True, "reason": None}},
            selection_chain={},
            optimization_start=is_start,
            optimization_end=is_end,
            ft_start=None,
            ft_end=None,
            optuna_is_trials=[],
            dsr_trials=None,
            forward_test_trials=None,
            stress_test_trials=None,
        )

    def fake_backtest(self, df, start, end, params):  # noqa: ARG001
        timestamps = list(pd.date_range(start, end, freq="1D", tz="UTC"))
        if len(timestamps) == 1:
            timestamps.append(end)
        balances = [100.0 + float(i) for i in range(len(timestamps))]
        return StrategyResult(
            trades=[],
            equity_curve=balances,
            balance_curve=balances,
            timestamps=timestamps,
        )

    def fake_baseline(self, is_result, is_period_days):  # noqa: ARG001
        return {
            "h": 5.0,
            "dd_limit": 0.0,
            "mu": 0.0,
            "sigma": 1.0,
            "is_avg_trade_interval": None,
            "max_trade_interval": None,
            "cusum_enabled": True,
            "drawdown_enabled": False,
            "inactivity_enabled": False,
        }

    def fake_scan(self, trades, balance_curve, timestamps, baseline, oos_start, oos_max_end):  # noqa: ARG001
        scan_calls["count"] += 1
        if scan_calls["count"] == 1:
            trigger_time = oos_start + pd.Timedelta(days=2)
            return SimpleNamespace(
                triggered=True,
                trigger_type="cusum",
                trigger_trade_idx=None,
                trigger_time=trigger_time,
                cusum_final=6.0,
                cusum_threshold=5.0,
                dd_peak=0.0,
                dd_threshold=0.0,
                oos_actual_trades=0,
                oos_actual_days=2.0,
            )
        return SimpleNamespace(
            triggered=False,
            trigger_type="max_period",
            trigger_trade_idx=None,
            trigger_time=oos_max_end,
            cusum_final=0.0,
            cusum_threshold=5.0,
            dd_peak=0.0,
            dd_threshold=0.0,
            oos_actual_trades=0,
            oos_actual_days=(oos_max_end - oos_start).total_seconds() / 86400.0,
        )

    monkeypatch.setattr(WalkForwardEngine, "_run_window_is_pipeline", fake_pipeline)
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", fake_backtest)
    monkeypatch.setattr(WalkForwardEngine, "_compute_is_baseline", fake_baseline)
    monkeypatch.setattr(WalkForwardEngine, "_scan_triggers", fake_scan)

    result, _study_id = engine.run_wf_optimization(df)

    assert len(result.windows) >= 2
    first_window = result.windows[0]
    second_window = result.windows[1]
    assert first_window.trigger_type == "cusum"
    assert first_window.cooldown_days_applied == pytest.approx(5.0)
    assert first_window.oos_actual_days == pytest.approx(2.0)
    assert first_window.oos_elapsed_days == pytest.approx(7.0)
    assert (second_window.oos_start - first_window.oos_end).total_seconds() / 86400.0 == pytest.approx(6.0)


def test_adaptive_ft_retry_delays_entry_before_live_oos(monkeypatch):
    index = pd.date_range("2025-01-01 00:00:00", "2025-04-30 00:00:00", freq="1D", tz="UTC")
    df = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
        index=index,
    )

    config = WFConfig(
        strategy_id="s01_trailing_ma",
        adaptive_mode=True,
        is_period_days=20,
        max_oos_period_days=10,
        min_oos_trades=1,
        check_interval_trades=1,
        post_process=PostProcessConfig(
            enabled=True,
            ft_period_days=5,
            ft_threshold_pct=-5.0,
            ft_reject_action="cooldown_reoptimize",
            ft_reject_cooldown_days=3,
            ft_reject_max_attempts=2,
            ft_reject_min_remaining_oos_days=3,
        ),
    )
    engine = WalkForwardEngine(config, {"fixed_params": {"dateFilter": False}}, {})

    call_counter = {"count": 0}

    def fake_pipeline(self, df, is_start, is_end, window_id):  # noqa: ARG001
        call_counter["count"] += 1
        return ISPipelineResult(
            best_result=SimpleNamespace(score=0.0),
            best_params={},
            param_id=f"w{window_id}_{call_counter['count']}",
            best_trial_number=window_id,
            best_params_source="forward_test",
            is_pareto_optimal=None,
            constraints_satisfied=None,
            available_modules=["optuna_is", "forward_test"],
            module_status={
                "optuna_is": {"enabled": True, "ran": True, "reason": None},
                "forward_test": {"enabled": True, "ran": True, "reason": None},
            },
            selection_chain={},
            optimization_start=is_start,
            optimization_end=is_end - pd.Timedelta(days=5),
            ft_start=is_end - pd.Timedelta(days=5),
            ft_end=is_end,
            optuna_is_trials=[],
            dsr_trials=None,
            forward_test_trials=[],
            stress_test_trials=None,
            ft_gate_failed=call_counter["count"] == 1,
            ft_pass_count=0 if call_counter["count"] == 1 else 1,
        )

    def fake_backtest(self, df, start, end, params):  # noqa: ARG001
        return StrategyResult(
            trades=[],
            equity_curve=[100.0, 100.0],
            balance_curve=[100.0, 100.0],
            timestamps=[start, end],
        )

    monkeypatch.setattr(WalkForwardEngine, "_run_window_is_pipeline", fake_pipeline)
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", fake_backtest)

    result, _study_id = engine.run_wf_optimization(df)

    first_window = result.windows[0]
    assert first_window.window_status == "traded"
    assert first_window.entry_delay_days == pytest.approx(3.0)
    assert first_window.ft_retry_attempts_used == 1
    assert first_window.trade_start == pd.Timestamp("2025-01-24", tz="UTC")
    assert first_window.oos_actual_days == pytest.approx(7.0)
    assert first_window.oos_elapsed_days == pytest.approx(10.0)


def test_adaptive_does_not_append_zero_day_last_window(monkeypatch):
    index = pd.date_range("2025-01-01 00:00:00", "2025-01-20 23:30:00", freq="30min", tz="UTC")
    df_full = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
        index=index,
    )

    # Mimics API pre-filtering with explicit end timestamp at 00:00 (single boundary bar).
    end_ts = pd.Timestamp("2025-01-10 00:00:00", tz="UTC")
    df = df_full[df_full.index <= end_ts].copy()

    config = WFConfig(
        strategy_id="s01_trailing_ma",
        adaptive_mode=True,
        is_period_days=3,
        max_oos_period_days=2,
        min_oos_trades=5,
        check_interval_trades=3,
    )
    base_template = {
        "fixed_params": {
            "dateFilter": True,
            "start": "2025-01-01T00:00",
            "end": "2025-01-10T00:00",
        },
        "risk_per_trade_pct": 2.0,
        "contract_size": 0.01,
        "commission_rate": 0.0005,
    }
    engine = WalkForwardEngine(config, base_template, {})

    def fake_pipeline(self, df, is_start, is_end, window_id):  # noqa: ARG001
        return ISPipelineResult(
            best_result=SimpleNamespace(score=0.0),
            best_params={},
            param_id=f"w{window_id}",
            best_trial_number=window_id,
            best_params_source="optuna_is",
            is_pareto_optimal=None,
            constraints_satisfied=None,
            available_modules=["optuna_is"],
            module_status={"optuna_is": {"enabled": True, "ran": True, "reason": None}},
            selection_chain={},
            optimization_start=is_start,
            optimization_end=is_end,
            ft_start=None,
            ft_end=None,
            optuna_is_trials=[],
            dsr_trials=None,
            forward_test_trials=None,
            stress_test_trials=None,
        )

    def fake_backtest(self, df, start, end, params):  # noqa: ARG001
        return StrategyResult(
            trades=[],
            equity_curve=[100.0, 100.0],
            balance_curve=[100.0, 100.0],
            timestamps=[start, end],
        )

    monkeypatch.setattr(WalkForwardEngine, "_run_window_is_pipeline", fake_pipeline)
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", fake_backtest)

    result, _study_id = engine.run_wf_optimization(df)

    assert result.windows
    assert result.windows[-1].oos_end <= end_ts
    assert all((window.oos_end - window.oos_start).total_seconds() > 0 for window in result.windows)
    assert all((window.oos_actual_days or 0.0) > 0.0 for window in result.windows)


def test_adaptive_oos_windows_do_not_overlap(monkeypatch):
    index = pd.date_range("2025-01-01 00:00:00", "2025-05-01 00:00:00", freq="1h", tz="UTC")
    df = pd.DataFrame(
        {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
        index=index,
    )

    config = WFConfig(
        strategy_id="s01_trailing_ma",
        adaptive_mode=True,
        is_period_days=20,
        max_oos_period_days=10,
        min_oos_trades=5,
        check_interval_trades=3,
    )
    base_template = {
        "fixed_params": {
            "dateFilter": True,
            "start": "2025-01-01T00:00",
            "end": "2025-05-01T00:00",
        },
    }
    engine = WalkForwardEngine(config, base_template, {})

    def fake_pipeline(self, df, is_start, is_end, window_id):  # noqa: ARG001
        return ISPipelineResult(
            best_result=SimpleNamespace(score=0.0),
            best_params={},
            param_id=f"w{window_id}",
            best_trial_number=window_id,
            best_params_source="optuna_is",
            is_pareto_optimal=None,
            constraints_satisfied=None,
            available_modules=["optuna_is"],
            module_status={"optuna_is": {"enabled": True, "ran": True, "reason": None}},
            selection_chain={},
            optimization_start=is_start,
            optimization_end=is_end,
            ft_start=None,
            ft_end=None,
            optuna_is_trials=[],
            dsr_trials=None,
            forward_test_trials=None,
            stress_test_trials=None,
        )

    def fake_backtest(self, df, start, end, params):  # noqa: ARG001
        return StrategyResult(
            trades=[],
            equity_curve=[100.0, 100.0],
            balance_curve=[100.0, 100.0],
            timestamps=[start, end],
        )

    def fake_baseline(self, is_result, is_period_days):  # noqa: ARG001
        return {
            "h": 5.0,
            "dd_limit": 0.0,
            "mu": 0.0,
            "sigma": 0.0,
            "is_avg_trade_interval": None,
            "max_trade_interval": None,
            "cusum_enabled": False,
            "drawdown_enabled": False,
            "inactivity_enabled": False,
        }

    def fake_scan(self, trades, balance_curve, timestamps, baseline, oos_start, oos_max_end):  # noqa: ARG001
        return SimpleNamespace(
            triggered=False,
            trigger_type="max_period",
            trigger_trade_idx=None,
            trigger_time=oos_max_end,
            cusum_final=0.0,
            cusum_threshold=5.0,
            dd_peak=0.0,
            dd_threshold=0.0,
            oos_actual_trades=0,
            oos_actual_days=(oos_max_end - oos_start).total_seconds() / 86400.0,
        )

    monkeypatch.setattr(WalkForwardEngine, "_run_window_is_pipeline", fake_pipeline)
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", fake_backtest)
    monkeypatch.setattr(WalkForwardEngine, "_compute_is_baseline", fake_baseline)
    monkeypatch.setattr(WalkForwardEngine, "_scan_triggers", fake_scan)

    result, _study_id = engine.run_wf_optimization(df)
    assert len(result.windows) >= 2
    for prev_window, next_window in zip(result.windows, result.windows[1:]):
        assert next_window.oos_start > prev_window.oos_end
