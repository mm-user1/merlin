"""Entry masks precede Fast ranking and survive every reference/batch builder."""
from dataclasses import replace

import numba
import numpy as np
import pytest

from core.data_input import prepare_input, DataInputError
from core.entry_filters import OFF, PreparedPermissions, canonical, effective_config, apply_permissions
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy
from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan, execute_grid_v2_candidates
from tests.fixtures.admission_strategy import filtered as strategy
from tests.fixtures.admission_strategy.bootstrap import publish


def settings(source):
    return dict(version=1, enabled=True, items=[dict(id="f1", type="candleDirection", version=1,
        enabled=True, parameters=dict(confirmationBars=2), source=source)])


def prepare(tmp_path):
    source = publish(tmp_path / "pack", rows=4000, turn_day=6)
    effective = effective_config(settings({**source, "timeframe_minutes": 60}))
    return prepare_input(source, date_filter=True, start="2024-01-03", end="2024-01-13",
        warmup_bars=100, admission_params={}, entry_filters=effective), effective


@pytest.mark.parametrize("workers", [1, 2])
def test_filtered_fast_reference_packers_threads(tmp_path, workers):
    assert numba.config.NUMBA_NUM_THREADS >= workers
    prepared, effective = prepare(tmp_path)
    config = strategy.load_config()
    params = strategy.normalized_params(dict(dateFilter=True, start="2024-01-02", end="2024-01-13", riskPerTrade=3., maxEntryLeverage=3.35))
    runtime = GridV2Settings(enabled_axes=("signalEvery",), top_n=2, compiled_workers=workers,
                            admission_rules=prepared.admission_rules, entry_filter_config=tuple(effective))
    hooks = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=prepared.entry_filter_context)
    plan = build_grid_v2_plan(config, runtime, params)
    table = execute_grid_v2_candidates(plan, prepared.frame, prepared.trade_start_idx, hooks)
    mapping = execute_grid_v2_candidates(replace(plan, settings=replace(runtime, compiled_config_packing="mapping")), prepared.frame, prepared.trade_start_idx, hooks)
    reference = execute_grid_v2_candidates(replace(plan, settings=replace(runtime, prefer_compiled=False)), prepared.frame, prepared.trade_start_idx, hooks)
    assert table.metadata["compiled_workers"] == workers
    assert table.metadata["compiled_config_packing"] == "table"
    assert mapping.metadata["compiled_config_packing"] == "mapping"
    for a, b, c in zip(table.rows, mapping.rows, reference.rows):
        assert a.status == b.status == c.status == "ok"
        assert a.total_trades == b.total_trades == c.total_trades > 0
        assert a.net_profit_pct == b.net_profit_pct == pytest.approx(c.net_profit_pct, rel=1e-12)
        assert a.guardrail_summary["margin_reject_count"] == c.guardrail_summary["margin_reject_count"] > 0
    off_plan = build_grid_v2_plan(config, replace(runtime, entry_filter_config=()), params)
    off = execute_grid_v2_candidates(off_plan, prepared.frame, prepared.trade_start_idx,
                                     GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=OFF))
    assert off_plan.plan_fingerprint != plan.plan_fingerprint
    assert [r.total_trades for r in table.rows] != [r.total_trades for r in off.rows]
    with pytest.raises(DataInputError, match="conflicts"):
        execute_grid_v2_candidates(plan, prepared.frame, prepared.trade_start_idx,
                                  GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=OFF))


@pytest.mark.parametrize("long_on,short_on", [(True, True), (False, True), (True, False), (False, False)])
def test_all_permissive_dual_and_unchanged_exits(tmp_path, long_on, short_on):
    prepared, effective = prepare(tmp_path)
    context = prepared.entry_filter_context
    context = replace(context, permit_long=np.ones(len(context.opens_ns), dtype=bool),
                      permit_short=np.ones(len(context.opens_ns), dtype=bool), known=np.ones(len(context.opens_ns), dtype=bool))
    params = strategy.normalized_params(dict(enableLong=long_on, enableShort=short_on))
    data = strategy.build_v2_execution_data(prepared.frame, params)
    assert np.any(data.signals.long_entries & data.signals.short_entries)
    profile = parse_execution_profile(strategy.load_config())
    off = run_v2_strategy(data=data, profile=profile, params=params, admission_rules=prepared.admission_rules,
                         trade_start_idx=prepared.trade_start_idx, entry_filter_context=OFF)
    allowed = run_v2_strategy(data=data, profile=profile, params=params, admission_rules=prepared.admission_rules,
                             trade_start_idx=prepared.trade_start_idx, entry_filter_context=context)
    assert off.strategy_result.trades == allowed.strategy_result.trades
    assert off.basic_metrics == allowed.basic_metrics
    short_context = replace(context, permit_long=np.zeros(len(context.opens_ns), dtype=bool))
    masked = apply_permissions(data, short_context)
    assert masked.signals.long_exits is data.signals.long_exits
    assert masked.signals.short_exits is data.signals.short_exits
    assert masked.atr is data.atr
    assert not masked.signals.long_entries.any()
    np.testing.assert_array_equal(masked.signals.short_entries, data.signals.short_entries)
    assert data.signals.long_entries.any()


def test_omission_at_hooks_and_direct_even_off(tmp_path):
    prepared, _ = prepare(tmp_path)
    with pytest.raises(DataInputError, match="explicit Off"):
        GridV2StrategyHooks.from_strategy(strategy)
    params = strategy.normalized_params()
    with pytest.raises(DataInputError, match="explicit Off"):
        run_v2_strategy(data=strategy.build_v2_execution_data(prepared.frame, params),
            profile=parse_execution_profile(strategy.load_config()), params=params,
            admission_rules=prepared.admission_rules)
    context = prepared.entry_filter_context
    shifted = prepared.frame.copy()
    shifted.index += __import__('pandas').Timedelta(seconds=1)
    hooks = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=context)
    with pytest.raises(DataInputError, match="timestamps"):
        hooks.build_execution_data(shifted, params)


def test_filter_changes_fast_ranking_and_selected_candidates(tmp_path):
    prepared, effective = prepare(tmp_path)
    config = strategy.load_config()
    config["parameters"]["signalEvery"]["optimize"].update(min=2, max=24, step=2)
    params = strategy.normalized_params(dict(dateFilter=True, start="2024-01-03", end="2024-01-13"))
    runtime = GridV2Settings(enabled_axes=("signalEvery",), top_n=2, compiled_workers=1,
                            admission_rules=prepared.admission_rules)
    selections = []
    for context, values in ((OFF, ()), (prepared.entry_filter_context, tuple(effective))):
        plan = build_grid_v2_plan(config, replace(runtime, entry_filter_config=values), params)
        result = execute_grid_v2_candidates(plan, prepared.frame, prepared.trade_start_idx,
            GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=context))
        assert all(row.status == "ok" for row in result.rows)
        selections.append([s.row.params["signalEvery"] for s in result.selected])
    assert selections == [[4, 12], [2, 4]]


def test_overlapping_delayed_and_truncated_execution_uses_timestamp_masks(tmp_path):
    import pandas as pd
    from core.engine_v2.admission import run_strategy
    from core.walkforward_engine import WalkForwardEngine, WFConfig, TriggerResult
    from tests.fixtures.admission_strategy.bootstrap import installed
    prepared, _ = prepare(tmp_path)
    context = prepared.entry_filter_context
    hooks = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=context)
    a, b = prepared.frame.iloc[200:1000], prepared.frame.iloc[600:1400]
    params = strategy.normalized_params()
    first, second = hooks.build_execution_data(a, params), hooks.build_execution_data(b, params)
    np.testing.assert_array_equal(first.signals.long_entries[400:], second.signals.long_entries[:400])
    np.testing.assert_array_equal(first.signals.short_entries[400:], second.signals.short_entries[:400])
    start, end = pd.Timestamp("2024-01-07", tz="UTC"), pd.Timestamp("2024-01-10", tz="UTC")
    params.update(dateFilter=True, start=start, end=end)
    with installed(filtered=True):
        engine = WalkForwardEngine(WFConfig(strategy_id=strategy.STRATEGY_ID, warmup_bars=100),
            {"fixed_params": params, "entryFilters": settings({**prepared.facts['source'], 'timeframe_minutes': 60})},
            {}, prepared_input=prepared)
        actual = engine._run_period_backtest(prepared.frame, start, end, params)
        direct_frame = prepared.frame.loc[:end]
        direct = run_strategy(strategy.FilterTestStrategy, direct_frame, engine._window_execution_params(params, start, end),
            direct_frame.index.searchsorted(start), admission_rules=prepared.admission_rules, entry_filter_context=context)
    assert actual.trades == direct.trades and actual.entry_diagnostics == direct.entry_diagnostics
    assert actual.trades
    cutoff = pd.Timestamp(actual.trades[0].exit_time)
    trigger = TriggerResult(True, "inactivity", 0, cutoff, 0, 0, 0, 0, 1, 0)
    truncated, until = engine._truncate_oos_result(actual, trigger, end)
    prefixed = engine._prepend_flat_prefix(truncated, scheduled_start=start-pd.Timedelta(days=1), live_start=start)
    assert until == cutoff and all(pd.Timestamp(t.exit_time) <= cutoff for t in prefixed.trades)
    assert all(e.fill_time_ns <= cutoff.value for e in prefixed.entry_events)
    assert prefixed.entry_diagnostics == truncated.entry_diagnostics
