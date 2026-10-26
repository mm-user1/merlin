"""Independent arithmetic and reference/compiled admission contract."""
from copy import deepcopy
from dataclasses import replace
import math
import numpy as np
import pandas as pd
import pytest

from core.engine_v2.admission import AdmissionRules, positive_cap
from core.engine_v2.contracts import Signals, GUARDRAIL_FLAG_EXECUTION_ERROR
from core.engine_v2.kernel import ExecutionData, KernelConfig, run_reference_kernel
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy, build_kernel_config
from core.engine_v2.compiled_kernel import evaluate_compiled_batch, OUTPUT_FLAGS, OUTPUT_TOTAL_TRADES, OUTPUT_MARGIN_REJECT_COUNT, OUTPUT_MAX_REQUIRED_LEVERAGE
from tests.fixtures.admission_strategy.strategy import load_config, normalized_params, rule_entry


def data(*, short=False, opened=100., signals=(0,), close=100., length=5, swing=None):
    bars = np.full(length, close, dtype=float)
    opens = np.full(length, opened, dtype=float)
    entries = np.zeros(length, dtype=bool)
    entries[list(signals)] = True
    return ExecutionData(timestamps=pd.date_range("2024-01-01", periods=length, freq="30min", tz="UTC"),
        open=opens, close=bars, high=np.maximum(opens, bars) + .1, low=np.minimum(opens, bars) - .1,
        signals=Signals(np.zeros(length, bool) if short else entries, entries if short else np.zeros(length, bool)),
        atr=np.ones(length), rolling_low=np.full(length, 91 if swing is None else swing),
        rolling_high=np.full(length, 109 if swing is None else swing), trail_long=np.full(length, np.nan), trail_short=np.full(length, np.nan))


def run_pair(frame, *, rules=None, config=None, trade_start_idx=0, **values):
    params = normalized_params({"initialCapital": 100., "commissionPct": 0., "riskPerTrade": 3., "enableShort": True, **values})
    profile = parse_execution_profile(config or load_config())
    rules = rules or AdmissionRules.from_entry(rule_entry())
    slow = run_v2_strategy(data=frame, profile=profile, params=params, admission_rules=rules, trade_start_idx=trade_start_idx)
    fast = evaluate_compiled_batch(data=frame, profile=profile, params_batch=[params], trade_start_idx=trade_start_idx,
        admission_rules=rules, n_workers=1).outputs[0]
    assert fast[OUTPUT_TOTAL_TRADES] == len(slow.strategy_result.trades)
    assert fast[OUTPUT_MARGIN_REJECT_COUNT] == slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"]
    assert fast[OUTPUT_MAX_REQUIRED_LEVERAGE] == (slow.strategy_result.entry_diagnostics["maxEntryLeverageUsed"] or 0)
    assert fast[0] == pytest.approx(slow.basic_metrics.net_profit_pct, rel=1e-12, abs=1e-12)
    return slow, fast


@pytest.mark.parametrize("short", [False, True])
def test_ieee_lot_floor_and_cap_adjacent_values(short):
    # IEEE (100*3/100/10)/.1 is just below 3: two lots, not decimal three.
    for cap, count in [(math.nextafter(.2, 0), 0), (.2, 1), (math.nextafter(.2, 1), 1)]:
        slow, _ = run_pair(data(short=short), maxEntryLeverage=cap)
        assert len(slow.strategy_result.trades) == count
        assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 1 - count
        if count:
            assert slow.strategy_result.trades[0].size == .2


@pytest.mark.parametrize("bad", [None, True, False, 0, -1, float("nan"), float("inf"), "bad"])
def test_bad_cap_rejected(bad):
    with pytest.raises(ValueError, match="maxEntryLeverage"):
        positive_cap(bad)


def test_profile_and_missing_rules_fail_closed():
    config = load_config()
    profile = parse_execution_profile(config)
    for func in (lambda: build_kernel_config(profile=profile, params=normalized_params()),
                 lambda: run_reference_kernel(data(), KernelConfig(entry_admission="risk_bracket_v1")),
                 lambda: evaluate_compiled_batch(data=data(), profile=profile, params_batch=[normalized_params()], trade_start_idx=0)):
        with pytest.raises(ValueError, match="rules"):
            func()
    for update in ({"target": "none", "trail": "ma", "trailActivation": "rr"}, {"boundary": "none"}, {"priceRounding": "tick_outward"}):
        bad = deepcopy(config)
        bad["execution"].update(update)
        with pytest.raises(ValueError):
            parse_execution_profile(bad)
    config["parameters"]["contractSize"] = dict(type="float", default=.01, role="execution")
    with pytest.raises(ValueError, match="contractSize"):
        parse_execution_profile(config)


def test_rules_exact_units_and_provenance():
    bybit = AdmissionRules.from_entry(rule_entry())
    okx = AdmissionRules.from_entry(rule_entry("OKX", step="1", minimum="3"))
    assert bybit.base_step == "0.1" and bybit.minimum_lots == 1
    assert okx.base_step == "0.1" and okx.base_minimum == "0.3" and okx.minimum_lots == 3
    assert okx.minimum_notional is None
    changed = rule_entry()
    changed["instrument_rules"]["source_reference"] = "new acquisition"
    assert AdmissionRules.from_entry(changed).semantic() == bybit.semantic()


@pytest.mark.parametrize("notional", ["0", "0.000", "5", None])
def test_published_minimum_roundtrip_and_execution(notional):
    from core.engine_v2.admission import admission_envelope, read_admission_envelope
    from data_provider.manifest import validate_instrument_rules
    entry = rule_entry(notional=notional)
    validate_instrument_rules(entry["instrument_rules"], "test", venue="BYBIT")
    rules = AdmissionRules.from_entry(entry)
    assert rules.minimum_notional == ("0" if notional in {"0", "0.000"} else notional)
    assert read_admission_envelope(admission_envelope(rules, 8), 8) == rules
    slow, _ = run_pair(data(), rules=rules)
    assert len(slow.strategy_result.trades) == 1
    assert slow.strategy_result.trades[0].size == .2
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 0
    assert (rules.semantic() == AdmissionRules.from_entry(rule_entry(notional=None)).semantic()) == (notional is None)


@pytest.mark.parametrize("notional", ["-1", "NaN", "Infinity", "1e999", "1e-999", "bad"])
def test_invalid_or_unrepresentable_notional_still_rejected(notional):
    with pytest.raises(ValueError):
        AdmissionRules.from_entry(rule_entry(notional=notional))


@pytest.mark.parametrize("change", ["optimized", "role", "default", "variant", "selector", "dependency"])
def test_cap_profile_is_fixed_and_profile_wide(change):
    config = load_config()
    cap = config["parameters"]["maxEntryLeverage"]
    if change == "optimized": cap["optimize"] = dict(enabled=True, min=1, max=8, step=1)
    if change == "role": cap["role"] = "signal"
    if change == "default": cap["default"] = 7
    if change == "variant": config["execution"]["variants"] = {"a": {"entryAdmission": "risk_bracket_v1"}}
    if change == "selector": config["execution"]["variantSelector"] = {"param": "maxEntryLeverage"}
    if change == "dependency": cap["depends_on"] = "enableLong"
    with pytest.raises(ValueError): parse_execution_profile(config)


def test_minimum_lots_without_second_floor():
    rules = AdmissionRules.from_entry(rule_entry(minimum="4.3"))
    assert 4.3 / .1 < 43
    slow, _ = run_pair(data(), rules=rules, riskPerTrade=43.1)
    assert slow.strategy_result.trades[0].size == 4.3


@pytest.mark.parametrize("venue,step,minimum", [("BYBIT", "0.10000000000000000000000000001", "0.30000000000000000000000000004"),
                                                ("OKX", "0.10000000000000000000000000001", "0.30000000000000000000000000004")])
def test_exact_rule_minimum_ceiling_beyond_decimal_default_precision(venue, step, minimum):
    rules = AdmissionRules.from_entry(rule_entry(venue,step,minimum))
    assert rules.minimum_lots == 4


@pytest.mark.parametrize("change", ["missing", "venue", "step", "multiplier", "identity"])
def test_invalid_or_unrepresentable_rule_snapshots(change):
    entry = rule_entry("OKX" if change == "multiplier" else "BYBIT")
    if change == "missing": entry.pop("instrument_rules")
    if change == "venue": entry["venue"] = "UNKNOWN"
    if change == "step":
        entry["instrument_rules"]["quantity_step"] = entry["instrument_rules"]["raw_contract_fields"]["qtyStep"] = "1e-999"
    if change == "multiplier": entry["instrument_rules"]["raw_contract_fields"]["ctMult"] = "2"
    if change == "identity": entry["instrument_id"] = "BYBIT_BBBUSDT"
    with pytest.raises(ValueError): AdmissionRules.from_entry(entry)


@pytest.mark.parametrize("short,swing", [(False, -1), (True, 200), (False, float("nan"))])
def test_invalid_stop_target_and_unavailable_inputs_refuse(short, swing):
    slow, _ = run_pair(data(short=short, swing=swing))
    assert not slow.strategy_result.trades
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 0


def test_compiled_corrupt_state_is_error_not_success():
    profile = parse_execution_profile(load_config())
    params = normalized_params({"initialCapital": float("nan")})
    rules = AdmissionRules.from_entry(rule_entry())
    with pytest.raises(ValueError, match="Nonfinite"):
        run_v2_strategy(data=data(), profile=profile, params=params, admission_rules=rules)
    result = evaluate_compiled_batch(data=data(), profile=profile, params_batch=[params], trade_start_idx=0, admission_rules=rules)
    assert int(result.outputs[0, OUTPUT_FLAGS]) & GUARDRAIL_FLAG_EXECUTION_ERROR
    assert result.outputs[0, OUTPUT_TOTAL_TRADES] == 0


@pytest.mark.parametrize("short", [False, True])
def test_gap_fee_cap_and_later_retry(short):
    frame = data(short=short, signals=(0, 2), length=6)
    frame.open[1] = frame.high[1] = 150
    # First gap needs 30/(100-.3), refused. Later normal-price fill passes unchanged.
    slow, _ = run_pair(frame, maxEntryLeverage=.25, commissionPct=1.)
    assert len(slow.strategy_result.trades) == 1
    assert slow.strategy_result.trades[0].entry_time == frame.timestamps[3]
    assert slow.strategy_result.trades[0].size == .2
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 1
    assert slow.strategy_result.entry_diagnostics["maxEntryLeverageUsed"] == 20 / 99.8


@pytest.mark.parametrize("short", [False, True])
def test_gap_immediate_exit_counts_fill(short):
    frame = data(short=short, opened=130 if not short else 70)
    slow, _ = run_pair(frame)
    trade = slow.strategy_result.trades[0]
    assert trade.entry_time == trade.exit_time == frame.timestamps[1]
    assert slow.strategy_result.entry_diagnostics["maxEntryLeverageUsed"] == abs(frame.open[1] * .2) / 100


@pytest.mark.parametrize("step,minimum,notional,commission", [
    ("1", "1", "5", 0), ("0.1", "0.3", "5", 0), ("0.1", "0.1", "21", 0),
    ("0.1", "0.1", "21", 1000), ("0.1", "0.1", "5", 1000)])
def test_other_refusals_are_not_cap_skips(step, minimum, notional, commission):
    rules = AdmissionRules.from_entry(rule_entry(step=step, minimum=minimum, notional=notional))
    slow, _ = run_pair(data(), rules=rules, commissionPct=commission, maxEntryLeverage=.001)
    assert slow.strategy_result.entry_diagnostics == dict(version=1, scope="execution", maxEntryLeverageUsed=None, leverageCapSkippedEntries=0)
    assert not slow.strategy_result.trades
    assert slow.strategy_result.balance_curve[-1] == 100


@pytest.mark.parametrize("capital", [0., -100.])
def test_nonpositive_finite_capital_refuses(capital):
    slow, _ = run_pair(data(), initialCapital=capital)
    assert not slow.strategy_result.trades
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 0


def test_warmup_bounds_final_bar_and_direction_priority():
    frame = data(signals=(0, 1, 2, 4))
    slow, _ = run_pair(frame, trade_start_idx=2, maxEntryLeverage=.1,
                       dateFilter=True, start=frame.timestamps[3], end=frame.timestamps[4])
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 0
    both = replace(data(), signals=Signals(data().signals.long_entries, data().signals.long_entries))
    slow, _ = run_pair(both)
    assert slow.strategy_result.trades[0].direction == "long"
    slow, _ = run_pair(data(signals=(0,1,2,3,4)), enableLong=False, maxEntryLeverage=.1)
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 0


def test_optional_stop_width_and_max_days():
    config = load_config()
    config["parameters"]["stopMaxPct"] = dict(type="float", default=5., role="execution")
    slow, _ = run_pair(data(), config=config, stopMaxPct=5., maxEntryLeverage=.001)
    assert not slow.strategy_result.trades
    assert slow.strategy_result.entry_diagnostics["leverageCapSkippedEntries"] == 0
    slow, _ = run_pair(data(length=210), stopMaxDays=4)
    trade = slow.strategy_result.trades[0]
    assert (trade.exit_time - trade.entry_time).total_seconds() / 3600 == 96.5


@pytest.mark.parametrize("bad", [float("inf"), float("nan"), 0., -1.])
def test_direct_market_array_rejection(bad):
    frame = data()
    frame.open[2] = bad
    rules = AdmissionRules.from_entry(rule_entry())
    profile = parse_execution_profile(load_config())
    params = normalized_params()
    for run in (lambda: run_v2_strategy(data=frame, profile=profile, params=params, admission_rules=rules),
                lambda: evaluate_compiled_batch(data=frame, profile=profile, params_batch=[params], trade_start_idx=0, admission_rules=rules)):
        with pytest.raises(ValueError, match="positive|finite"):
            run()


def test_unrepresentable_sizing_is_run_error():
    profile = parse_execution_profile(load_config())
    params = normalized_params({"riskPerTrade": 1e308})
    rules = AdmissionRules.from_entry(rule_entry())
    with pytest.raises(ValueError):
        run_v2_strategy(data=data(), profile=profile, params=params, admission_rules=rules)
    fast = evaluate_compiled_batch(data=data(), profile=profile, params_batch=[params], trade_start_idx=0, admission_rules=rules).outputs[0]
    assert int(fast[OUTPUT_FLAGS]) & GUARDRAIL_FLAG_EXECUTION_ERROR
    assert math.isfinite(fast[OUTPUT_TOTAL_TRADES])
