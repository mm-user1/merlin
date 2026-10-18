from dataclasses import replace
import pickle

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError as Error, data
from tools.pattern_lab.accounts import (prepare_inputs, build_commands, candle_filter, HARecipe,
    StateRows, align_states, CommandBundle, AccountFeatureRequest, command_array_digest)
from ._account_helpers import binding, raw_binding, prepared, feature, bundle, side, filter_for, STEP


def test_hand_ha_atr_neutral_runs_and_gap_reset():
    bars = [(10, 14, 8, 12), (12, 16, 10, 14), (12, 14, 9, 11), (20, 22, 18, 20)]
    p = prepare_inputs(binding(bars, slots=[0, 1, 2, 4]), (), (feature(), feature("atr", "atr", length=2)))
    ha, atr = p.feature("ha"), p.feature("atr")
    np.testing.assert_array_equal(ha.values[:, 0], [11, 11, 12, 20])
    np.testing.assert_array_equal(ha.values[:, 3], [11, 13, 11.5, 20])
    np.testing.assert_array_equal(ha.colour, [0, 1, -1, 0])
    np.testing.assert_array_equal(ha.run, [0, 1, 1, 0])
    np.testing.assert_array_equal(ha.position, [0, 1, 2, 0])
    np.testing.assert_array_equal(atr.values, [np.nan, 6, 5.5, np.nan])
    assert p.target.inputFingerprint == binding(bars, slots=[0, 1, 2, 4]).series.input_fingerprint


def test_exact_k_not_reemitted_after_warmup_and_neutral_break():
    b = binding([(100+i, 105+i, 99+i, 103+i) for i in range(10)])
    p = prepare_inputs(b, (), (feature(priorBars=2),))
    c = build_commands(p, HARecipe("ha", entryRunLength=2), mode="signalManaged", stopUpdates=False)
    assert not (c.long.entry & c.long.entryKnown).any()  # exact k passed during warmup
    p0 = prepare_inputs(b, (), (feature(),))
    c0 = build_commands(p0, HARecipe("ha", entryRunLength=2), mode="signalManaged", stopUpdates=False)
    assert np.flatnonzero(c0.long.entry).tolist() == [1]
    assert np.flatnonzero(c0.short.exit).tolist() == list(range(10))


def test_complete_htf_independent_ha_and_expiry_at_missing_update():
    n = 48*6
    t = np.arange(n, dtype=np.int64)*300000
    x = 100+np.arange(n)/10
    raw = np.column_stack((x, x+3, x-1, x+2, np.ones(n)))
    # Missing 5m member removes the complete 04:00-08:00 four-hour group.
    keep = t != 5*3600000
    target = raw_binding(t[keep], raw[keep], end=24*3600000)
    src = raw_binding(t[keep], raw[keep], tf=240, alias="btc4h", end=24*3600000)
    p = prepare_inputs(target, (src,), (feature(), feature("sourceHA", inputId="btc4h"),
        feature("F06", "candleDirection", "btc4h", candleType="real", confirmationBars=1)))
    a = p.feature("F06").aligned
    at = {int(v): i for i, v in enumerate(a.decisionMs)}
    assert a.known[at[4*3600000]]
    assert not a.known[at[8*3600000]] and a.unknownReason[at[8*3600000]] == 3
    assert a.known[at[12*3600000]]
    assert a.availableAtMs[at[4*3600000]] == 4*3600000
    src_values = next(v for v in p.inputs if v.inputId == "btc4h").values
    assert p.feature("sourceHA").values[0, 0] == (src_values[0, 0]+src_values[0, 3])/2
    assert p.feature("sourceHA").values[1, 0] == (src_values[1, 0]+src_values[1, 3])/2  # gap reset
    filt = candle_filter(p, "F06")
    assert filt.known[at[4*3600000]] and filt.allowLong[at[4*3600000]]


def test_generic_alignment_latest_unknown_no_fallback_and_neutral():
    ints = lambda x: np.array(x, dtype=np.int64)
    rows = StateRows(ints([1, 2, 5]), ints([2, 4, 6]), ints([10, 6, 9]),
                     np.array([1., np.nan, 0.]), np.array([True, False, True]), 0, 10)
    a = align_states(rows, ints([1, 2, 4, 5, 6, 9]))
    assert a.known.tolist() == [False, True, False, False, True, False]
    assert a.unknownReason.tolist() == [1, 0, 2, 2, 0, 3]
    assert a.sourceIndex.tolist() == [-1, 0, 1, 1, 2, 2]
    assert a.value[4] == 0
    with pytest.raises(Error):
        replace(rows, availableAtMs=ints([2, 2, 6]))
    with pytest.raises(Error):
        replace(rows, coverageEndMs=8)


@pytest.mark.parametrize("change", ["timestamps", "values", "research_mask", "segment_start", "research_start_index", "timeframe_minutes", "base_gap_count", "input_fingerprint"])
def test_bad_native_bindings(change):
    b = binding([(100, 102, 98, 101)]*4)
    values = dict(timestamps=np.array([0, 0, STEP*2, STEP*3], np.int64), values=np.ones((4, 4)),
        research_mask=np.zeros(4, bool), segment_start=np.zeros(4, bool), research_start_index=True,
        timeframe_minutes=True, base_gap_count=1, input_fingerprint="fake")
    with pytest.raises(Error):
        prepare_inputs(replace(b, series=replace(b.series, **{change: values[change]})), (), ())


@pytest.mark.parametrize("changes", [dict(featureId="ema"), dict(version="2"), dict(inputId="missing"),
    dict(priorBars=True), dict(priorBars=-1), dict(parameters={"extra": 1}), dict(requestId=""),
    dict(featureId="atr", parameters={"length": True}),
    dict(featureId="candleDirection", parameters={"candleType": "ha", "confirmationBars": 0})])
def test_closed_feature_requests(changes):
    with pytest.raises(Error):
        prepare_inputs(binding([(100, 102, 98, 101)]*4), (), (replace(feature(), **changes),))


def test_duplicates_bounds_and_headers():
    b = binding([(100, 102, 98, 101)]*4)
    for target, sources, requests in [(b, (b,), ()), (b, (), (feature(), feature())),
        (replace(b, header={**b.header, "extra": 1}), (), ()),
        (b, (replace(b, inputId="source", header={**b.header, "start_ms": STEP}),), ())]:
        with pytest.raises(Error):
            prepare_inputs(target, sources, requests)


def test_owned_arrays_pickle_and_request_order_identity():
    b = binding([(100+i, 102+i, 98+i, 101+i) for i in range(8)])
    requests = (feature(), feature("atr", "atr", length=2))
    p = prepare_inputs(b, (), requests)
    assert p.identity == prepare_inputs(b, (), requests[::-1]).identity
    b.series.values[0, 0] = 101
    assert p.target.values[0, 0] == 100
    for value in (p, pickle.loads(pickle.dumps(p))):
        for a in (value.target.values, value.target.timestamps, value.feature("ha").values):
            with pytest.raises(ValueError):
                a.flat[0] = 0
            with pytest.raises(ValueError):
                a.setflags(write=True)


@pytest.mark.parametrize("provider", ["haLast", "haSwing2", "realSwingAtr"])
def test_stop_recipes_and_disabled_streams(provider):
    p = prepare_inputs(binding([(100+i, 104+i, 98+i, 103+i) for i in range(20)]), (),
                       (feature(), feature("atr", "atr", length=14)))
    recipe = HARecipe("ha", stopProvider=provider, atrRequestId="atr" if provider == "realSwingAtr" else None)
    c = build_commands(p, recipe, mode="signalManaged", stopUpdates=True)
    assert np.array_equal(c.long.initialStopKnown, c.long.proposedStopKnown)
    if provider == "realSwingAtr":
        assert not c.long.initialStopKnown[:13].any()
        assert c.long.initialStop[13] == min(p.target.values[12:14, 2])-2*p.feature("atr").values[13]
    elif provider == "haSwing2":
        assert not c.long.initialStopKnown[0]
        assert c.long.initialStop[2] == min(p.feature("ha").values[1:3, 2])
    else:
        assert c.long.initialStop[0] == p.feature("ha").values[0, 2]
    b = build_commands(p, recipe, mode="fixedBracket", stopUpdates=False)
    assert not b.long.exitKnown.any() and not b.long.exit.any() and not b.long.proposedStopKnown.any()
    assert np.isnan(b.long.proposedStop).all()
    with pytest.raises(Error):
        build_commands(p, replace(recipe, haRequestId="atr"), mode="signalManaged", stopUpdates=True)


def test_command_identity_content_provenance_and_single_filter():
    p = prepared()
    a = side(6, entries=[0])
    c = bundle(p, long=a)
    assert c.identity != bundle(p, long=replace(a, initialStop=a.initialStop+1)).identity
    assert c.identity != replace(c, provenance={"new": 1}).identity
    f = filter_for(p)
    assert bundle(p, long=a, filt=f).identity != bundle(p, long=a, filt=replace(f, provenance={"new": 1})).identity
    assert bundle(p, long=a, filt=f).identity != bundle(p, long=a, filt=replace(f, allowLong=~f.allowLong)).identity
    with pytest.raises(Error):
        bundle(p, filt=[f])
    with pytest.raises(TypeError):
        replace(c, identity="stale")
    with pytest.raises(Error):
        replace(c, decisionMs=c.decisionMs.astype(np.float64))
    digest = command_array_digest({"x": np.array([np.nan, -0.])}, {})
    alternate = np.array([np.nan, -0.]); alternate.view(np.uint64)[0] = 0x7ff8000000000001
    assert digest == command_array_digest({"x": alternate}, {})
    assert digest != command_array_digest({"x": np.array([np.nan, 0.])}, {})


def test_future_perturbation_and_origin_identity():
    bars = np.array([(100+i, 104+i, 98+i, 103+i) for i in range(24)], float)
    requests = (feature(), feature("atr", "atr", length=3),
                feature("state", "candleDirection", candleType="ha", confirmationBars=2))
    p = prepare_inputs(binding(bars), (), requests)
    bars[12:] *= 2
    q = prepare_inputs(binding(bars), (), requests)
    for request in requests:
        np.testing.assert_array_equal(p.feature(request.requestId).values[:12], q.feature(request.requestId).values[:12])
    c, d = [build_commands(v, HARecipe("ha"), mode="signalManaged", stopUpdates=True,
                           entryFilter=candle_filter(v, "state")) for v in (p, q)]
    assert p.identity != q.identity and c.identity != d.identity
    for key in vars(c.long):
        np.testing.assert_array_equal(getattr(c.long, key)[:12], getattr(d.long, key)[:12])
    shifted = prepare_inputs(binding(bars[2:], slots=np.arange(2, 24), start=2*STEP, warmup=2*STEP), (), requests)
    assert shifted.identity != p.identity


@pytest.mark.parametrize("value", [0., -1., np.nan, np.inf])
def test_invalid_real_prices_rejected(value):
    b = binding([(100, 102, 98, 101)]*4)
    v = b.series.values.copy(); v[0, 0] = value
    with pytest.raises(Error):
        prepare_inputs(replace(b, series=replace(b.series, values=v)), (), ())


def test_native_rejects_no_research_and_no_complete_source():
    stamps = np.array([0], dtype=np.int64)
    values = np.array([[100, 102, 98, 101, 1]], dtype=float)
    with pytest.raises(Error):
        raw_binding(stamps, values, tf=240, end=8*3600000)
    with pytest.raises(Error):
        raw_binding(stamps, values, tf=5, start=300000, end=600000)


def test_seed_origin_changes_with_same_research_interval():
    bars = [(100+i, 105+i, 99+i, 103+i) for i in range(12)]
    p = prepare_inputs(binding(bars, start=4*STEP), (), (feature(),))
    q = prepare_inputs(binding(bars[2:], slots=np.arange(2, 12), start=4*STEP, warmup=2*STEP), (), (feature(),))
    assert p.target.header["start_ms"] == q.target.header["start_ms"]
    assert p.preparationStartMs != q.preparationStartMs and p.identity != q.identity
    c, d = [build_commands(v, HARecipe("ha"), mode="signalManaged", stopUpdates=False) for v in (p, q)]
    assert c.identity != d.identity


def test_neutral_ha_breaks_run_and_can_propose_stop():
    # First HA green, second HA exactly neutral (open=101.25, close=101.25).
    p = prepare_inputs(binding([(100, 105, 99, 102), (101, 103, 99, 102), (102, 106, 100, 104)]), (), (feature(),))
    assert p.feature("ha").colour.tolist() == [1, 0, 1]
    assert p.feature("ha").run.tolist() == [1, 0, 1]
    c = build_commands(p, HARecipe("ha", entryRunLength=1), mode="signalManaged", stopUpdates=True)
    assert not c.long.exit[1] and not c.short.exit[1]
    assert c.long.proposedStopKnown[1]
