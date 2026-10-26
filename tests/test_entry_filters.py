"""Synthetic native-clock and configuration oracles for entry permissions."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from core.entry_filters import (MISSING, OFF, PreparedPermissions, apply_permissions,
    canonical, capability, candle_direction, effective_config, source_locator, validate_context)
from core.entry_filter_input import source_bounds, validate_envelope
from core.data_input import DataInputError, prepare_input, replay_input
from tests._data_input_helpers import publish


def editor(source, count=2):
    return dict(version=1, enabled=True, items=[dict(id="f1", type="candleDirection", version=1,
        enabled=True, parameters=dict(confirmationBars=count), source=source)])


def bars(colours, minutes=240):
    return pd.DataFrame(dict(open=np.full(len(colours), 100.), close=100 + np.asarray(colours)),
        index=pd.date_range("2024-01-01", periods=len(colours), freq=f"{minutes}min", tz="UTC"))


@pytest.mark.parametrize("count", [2, 3, 35])
def test_native_warmup_neutral_reversal_and_expiry(count):
    prior = max(28, count-1)
    frame = bars([1]*(prior+2) + [0, -1, -1, -1])
    step = 240*60*10**9
    closes = frame.index.asi8 + step
    long, short, known, why = candle_direction(frame, 240, count, closes)
    assert not known[:prior].any() and known[prior:].all()
    assert long[prior:prior+2].all() and not long[prior+2:].any()
    assert not short[:prior+2+count].any()
    assert short[-1] == (count <= 3)
    decisions = np.array([closes[prior]-1, closes[prior], closes[-1]+step-1, closes[-1]+step])
    lo, sh, valid, why = candle_direction(frame, 240, count, decisions)
    assert not valid[0] and valid[1] and valid[2] and not valid[3]
    assert why.tolist() == [1, 0, 0, 2]


def test_gap_new_unknown_and_prefix_origin():
    frame = bars([1]*100)
    step = 240*60*10**9
    decision = frame.index.asi8 + step
    gap = frame.drop(frame.index[35])
    lo, _, known, why = candle_direction(gap, 240, 2, decision)
    assert lo[34] and not lo[35:64].any() and lo[64]
    assert why[35] == 2 and why[36] == 1
    assert not known[36:64].any()
    for end in (35, 36, 50, 64, 90):
        bounded = gap.loc[(gap.index >= frame.index[max(0, end-28)]) & (gap.index <= frame.index[end])]
        actual = candle_direction(bounded, 240, 2, decision[end:end+1])
        assert actual[0][0] == lo[end]
        assert actual[2][0] == known[end]


def test_effective_set_identity_and_offline_windows_roots():
    source = dict(type="parquet", version=1, pack_root="C:\\Data\\Pack", instrument_id="OKX_BTC-USDT-SWAP", timeframe_minutes=240)
    value = editor(source)
    duplicate = deepcopy(value["items"][0]); duplicate["id"] = "different"
    duplicate["source"]["pack_root"] = "c:/data/pack/"
    value["items"].append(duplicate)
    assert effective_config(value) == effective_config(editor(source))
    assert source_locator("C:/DATA/pack") == "c:/data/pack"
    value["items"][1]["id"] = "f1"
    with pytest.raises(DataInputError, match="unique"):
        effective_config(value)
    inactive = dict(version=1, enabled=False, items=[dict(id="future", type="unknown", payload=3)])
    assert effective_config(inactive) == []
    inactive["enabled"] = True
    with pytest.raises(DataInputError):
        effective_config(inactive)
    for bad in (None, {}, dict(version=1, enabled=True, items=[], logic="or")):
        with pytest.raises(DataInputError):
            effective_config(bad)


def test_explicit_context_and_capability():
    from tests.fixtures.admission_strategy.strategy import load_config
    from core.engine_v2.profile import parse_execution_profile
    config = load_config()
    assert capability(config) == ()
    config["entryFilterSupport"] = dict(version=1, methods=["candleDirection"])
    assert parse_execution_profile(config).entry_filter_methods == ("candleDirection",)
    for missing in (MISSING, None, {}, []):
        with pytest.raises(DataInputError, match="explicit Off"):
            validate_context(("candleDirection",), missing)
    validate_context(("candleDirection",), OFF, [])
    config["execution"]["boundary"] = "none"
    with pytest.raises(ValueError):
        parse_execution_profile(config)


@pytest.mark.parametrize("target_minutes,source_minutes", [(5, 60), (60, 5), (1440, 4320)])
def test_bounds_integer_grid_and_overflow(target_minutes, source_minutes):
    source = dict(type="parquet", version=1, pack_root="/synthetic", instrument_id="OKX_BTC-USDT-SWAP", timeframe_minutes=source_minutes)
    item = effective_config(editor(source))[0]
    start = pd.Timestamp("2024-06-01", tz="UTC").value
    step = target_minutes*60*10**9
    start = start//step*step
    pin = dict(source=dict(timeframe_minutes=target_minutes), bounds=dict(start=pd.Timestamp(start, tz="UTC").isoformat(), end=pd.Timestamp(start+10*step, tz="UTC").isoformat()))
    bounds = source_bounds(pin, item)
    duration = source_minutes*60*10**9
    assert pd.Timestamp(bounds["start"]).value == (start+step)//duration*duration-29*duration
    assert pd.Timestamp(bounds["end"]).value == (start+10*step)//duration*duration
    item["parameters"]["confirmationBars"] = 2**63-1
    with pytest.raises(DataInputError, match="supported date range"):
        source_bounds(pin, item)


def test_same_root_replay_and_missing_context(tmp_path, monkeypatch):
    from data_provider import data
    source = publish(tmp_path / "pack")
    effective = effective_config(editor(source))
    original = data.read_session
    calls = []
    def counted(root):
        calls.append(root)
        return original(root)
    monkeypatch.setattr(data, "read_session", counted)
    prepared = prepare_input(source, date_filter=True, start="2024-01-02", end="2024-01-02", warmup_bars=2, entry_filters=effective)
    assert len(calls) == 1
    assert isinstance(prepared.entry_filter_context, PreparedPermissions)
    replay = replay_input(prepared.facts, entry_filters=effective, entry_filter_inputs=prepared.entry_filter_inputs)
    np.testing.assert_array_equal(replay.entry_filter_context.permit_long, prepared.entry_filter_context.permit_long)
    changed = publish(tmp_path / "changed", change=280)
    with pytest.raises(DataInputError, match="filter input changed"):
        replay_input(prepared.facts, root_override=changed["pack_root"], entry_filters=effective,
            entry_filter_inputs=prepared.entry_filter_inputs,
            filter_root_overrides={effective[0]["source"]["pack_root"]: changed["pack_root"]})
    with pytest.raises(DataInputError):
        validate_envelope({"version": 1, "state": "off"}, effective)


@pytest.mark.parametrize("minutes", [5, 240, 4320])
def test_causal_history_and_future_mutations(minutes):
    frame = bars([1]*65 + [-1]*35, minutes)
    step = minutes*60*10**9
    decisions = np.array([frame.index[80].value + step])
    expected = candle_direction(frame, minutes, 3, decisions)
    bounded = candle_direction(frame.iloc[52:81], minutes, 3, decisions)
    changed = frame.copy()
    changed.iloc[:52, changed.columns.get_loc("close")] = 100
    changed.iloc[81:, changed.columns.get_loc("close")] = 100
    for actual in (bounded, candle_direction(changed, minutes, 3, decisions)):
        for a, b in zip(actual, expected):
            np.testing.assert_array_equal(a, b)
    assert expected[1].tolist() == [True]


@pytest.mark.parametrize("drop,first_reason", [([548], 1), ([576], 2)])
def test_provider_prefix_gap_and_missing_first_decision(tmp_path, drop, first_reason):
    target = publish(tmp_path / "target")
    source = publish(tmp_path / "filter", drop=drop)
    prepared = prepare_input(target, date_filter=True, start="2024-01-03", end="2024-01-03",
        warmup_bars=2, entry_filters=effective_config(editor(source)))
    context = prepared.entry_filter_context
    i = prepared.trade_start_idx
    assert not context.known[i] and context.reasons[i] == first_reason
    assert context.known[i+29] and context.permit_long[i+29]


def test_provider_empty_and_short_prefix_are_distinct(tmp_path):
    target = publish(tmp_path / "target")
    options = dict(date_filter=True, start="2024-01-03", end="2024-01-03", warmup_bars=2)
    for name, dropped, message in (("empty", range(548, 864), "empty source"),
                                    ("short", range(549), "required history")):
        source = publish(tmp_path / name, drop=dropped)
        with pytest.raises(DataInputError, match=message):
            prepare_input(target, **options, entry_filters=effective_config(editor(source)))


def test_multiple_bindings_intersect_and_release_on_error(tmp_path, monkeypatch):
    from data_provider import data
    from core.data_input import pack_session
    target = publish(tmp_path / "target")
    source = publish(tmp_path / "filter")
    value = editor(target)
    other = editor({**source, "timeframe_minutes": 60})["items"][0]
    other["id"] = "other"; value["items"].append(other)
    options = dict(date_filter=True, start="2024-01-03", end="2024-01-03", warmup_bars=2)
    effective = effective_config(value)
    single = [prepare_input(target, **options, entry_filters=[item]).entry_filter_context for item in effective]
    calls = []
    original = data.read_session
    def counted(root):
        calls.append(str(root)); return original(root)
    monkeypatch.setattr(data, "read_session", counted)
    combined = prepare_input(target, **options, entry_filters=effective).entry_filter_context
    assert len(calls) == 2
    np.testing.assert_array_equal(combined.permit_long, single[0].permit_long & single[1].permit_long)
    np.testing.assert_array_equal(combined.permit_short, single[0].permit_short & single[1].permit_short)
    other["source"]["instrument_id"] = "TEST_ABSENT-USDT-SWAP"
    with pytest.raises(DataInputError, match="absent"):
        prepare_input(target, **options, entry_filters=effective_config(value))
    for root in (target["pack_root"], source["pack_root"]):
        with pack_session(root, timeout=.1) as session:
            assert session.inspect()["instruments"]
    with pytest.raises(DataInputError, match="cancelled"):
        prepare_input(target, **options, entry_filters=effective, cancelled=lambda: True)
    value["enabled"] = False
    other["source"]["pack_root"] = str(tmp_path / "does-not-exist")
    calls.clear()
    off = prepare_input(target, **options, entry_filters=effective_config(value))
    assert off.entry_filter_context is OFF and len(calls) == 1


def test_replay_gap_fill_unused_extension_and_old_mutation(tmp_path):
    target = publish(tmp_path / "target")
    source = publish(tmp_path / "filter", drop=[576])
    effective = effective_config(editor(source))
    prepared = prepare_input(target, date_filter=True, start="2024-01-03", end="2024-01-03",
        warmup_bars=2, entry_filters=effective)
    for name, args, accept in (("filled", {}, False), ("extended", {"rows": 1500, "drop": [576]}, True),
                               ("old-change", {"drop": [576], "change": 10}, True)):
        replacement = publish(tmp_path / name, **args)
        def replay():
            return replay_input(prepared.facts, entry_filters=effective, entry_filter_inputs=prepared.entry_filter_inputs,
                filter_root_overrides={effective[0]["source"]["pack_root"]: replacement["pack_root"]})
        if accept:
            actual = replay()
            assert actual.entry_filter_inputs == prepared.entry_filter_inputs
            np.testing.assert_array_equal(actual.entry_filter_context.permit_long, prepared.entry_filter_context.permit_long)
        else:
            with pytest.raises(DataInputError, match="filter input changed"):
                replay()


def test_same_root_multiple_filters_share_load_and_warmup_independent_bounds(tmp_path, monkeypatch):
    from data_provider import data
    source = publish(tmp_path / "pack")
    value = editor(source)
    second = deepcopy(value["items"][0]); second["id"] = "second"; second["parameters"]["confirmationBars"] = 3
    value["items"].append(second)
    original = data._ReadSession.load_slice
    calls = []
    def counted(self, *args, **kwargs):
        calls.append(kwargs); return original(self, *args, **kwargs)
    monkeypatch.setattr(data._ReadSession, "load_slice", counted)
    options = dict(date_filter=True, start="2024-01-03", end="2024-01-03", entry_filters=effective_config(value))
    a = prepare_input(source, **options, warmup_bars=2)
    assert len(calls) == 2  # Target and one shared native slice, despite two configurations.
    b = prepare_input(source, **options, warmup_bars=100)
    assert a.entry_filter_inputs == b.entry_filter_inputs
    np.testing.assert_array_equal(a.entry_filter_context.permit_long[a.trade_start_idx:], b.entry_filter_context.permit_long[b.trade_start_idx:])
