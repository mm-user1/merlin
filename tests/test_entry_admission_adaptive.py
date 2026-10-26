"""Retained OOS diagnostics follow trades and the inclusive live cutoff."""
import pandas as pd
from core.backtest_engine import TradeRecord
from core.engine_v2.admission import AdmissionStrategyResult, AdmissionEvent, summarize_entries, aggregate_entries
from core.walkforward_engine import WalkForwardEngine, WFConfig, TriggerResult


def test_inactivity_drops_early_entry_and_keeps_refusal_at_cutoff():
    times = pd.date_range("2024-01-01", periods=6, freq="h", tz="UTC")
    kept = TradeRecord(entry_time=times[0], exit_time=times[1])
    dropped = TradeRecord(entry_time=times[2], exit_time=times[5])
    events = (AdmissionEvent(times[0].value, 0, 2.), AdmissionEvent(times[2].value, 1, 7.),
              AdmissionEvent(times[3].value, None, None), AdmissionEvent(times[4].value, None, None))
    result = AdmissionStrategyResult(trades=[kept, dropped], timestamps=list(times), equity_curve=[100.]*6,
        balance_curve=[100.]*6, entry_events=events, entry_diagnostics=summarize_entries(events))
    engine = WalkForwardEngine(WFConfig(strategy_id="s01_trailing_ma"), {"fixed_params": {}}, {})
    trigger = TriggerResult(True, "inactivity", 0, times[3], 0, 0, 0, 0, 1, 0)
    truncated, end = engine._truncate_oos_result(result, trigger, times[-1])
    assert truncated.trades == [kept] and end == times[3]
    assert truncated.entry_diagnostics == dict(version=1, scope="oos", maxEntryLeverageUsed=2., leverageCapSkippedEntries=1)
    prefixed = engine._prepend_flat_prefix(truncated, scheduled_start=times[0]-pd.Timedelta(days=1), live_start=times[0])
    assert prefixed.entry_events == truncated.entry_events
    assert prefixed.entry_diagnostics == truncated.entry_diagnostics
    flat = engine._build_flat_result(times[0], times[-1], admission=True)
    assert flat.entry_diagnostics == dict(version=1, scope="oos", maxEntryLeverageUsed=None, leverageCapSkippedEntries=0)
    aggregate = aggregate_entries([("w1", truncated.entry_diagnostics), ("w1", truncated.entry_diagnostics), ("w2", flat.entry_diagnostics)])
    assert aggregate == truncated.entry_diagnostics
    assert aggregate_entries([("w1", truncated.entry_diagnostics), ("old", None)]) is None
    assert aggregate_entries([("bad", {"version": 1, "scope": "oos", "leverageCapSkippedEntries": 0})]) is None
