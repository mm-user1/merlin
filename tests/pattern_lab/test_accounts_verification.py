from dataclasses import replace
import math
import multiprocessing
import pickle
import os
from pathlib import Path
import subprocess
import sys
import json

import numpy as np
import pandas as pd
import pytest

from tools.pattern_lab import PatternLabDataError as Error
from tools.pattern_lab.accounts import (execute_account, validate_result, prepare_inputs, build_commands,
                                       HARecipe, AccountWindow)
from tools.pattern_lab.study import bracket, contracts
from ._account_helpers import (prepared, binding, feature, bundle, side, policy, window,
                               rules, filter_for, spawn_run, STEP)
from ._account_oracle import signal_oracle


def close(a, b):
    assert math.isfinite(a) and math.isfinite(b)
    assert math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-8), (a, b)


@pytest.mark.parametrize("seed", [1, 17, 44])
@pytest.mark.parametrize("direction", ["long", "short", "both"])
@pytest.mark.parametrize("updates", [False, True])
def test_independent_forward_oracle(seed, direction, updates):
    rng = np.random.default_rng(seed)
    n = 70
    op = 100+rng.normal(0, 5, n)
    cl = 100+rng.normal(0, 5, n)
    bars = np.column_stack((op, np.maximum(op, cl)+3, np.minimum(op, cl)-3, cl))
    slots = np.arange(n); slots[35:] += 3
    p = prepared(bars, slots=slots)
    args = {}
    for name, sign in (("long", 1), ("short", -1)):
        proposals = cl-sign*rng.uniform(1, 12, n) if updates else None
        if updates:
            proposals[::7] = np.nan
            proposals[::11] = -1
        args[name] = side(n, entries=np.flatnonzero(rng.random(n) < .3),
            stop=cl-sign*10, exits=np.flatnonzero(rng.random(n) < .12), proposals=proposals,
            exitKnown=rng.random(n) > .15, entryKnown=rng.random(n) > .1)
    c = bundle(p, updates=updates, filt=filter_for(p, rng.random(n) > .2, rng.random(n) > .2, rng.random(n) > .1), **args)
    pol, w, r = policy(updates=updates, direction=direction), window(p), rules()
    actual = execute_account(p, c, pol, w, r)
    expected, attempts, path = signal_oracle(p, c, pol, w, r)
    assert len(actual.trades) == len(expected)
    for a, b in zip(actual.trades.to_pylist(), expected):
        for k, v in b.items():
            if isinstance(v, float):
                close(a[k], v)
            else:
                assert a[k] == v
    assert [(a["signalIndex"], a["side"], a["reason"]) for a in actual.attempts.to_pylist()] == attempts
    for a, b in zip(zip(actual.path["balance"].to_pylist(), actual.path["equity"].to_pylist()), path):
        close(a[0], b[0]); close(a[1], b[1])


def legacy_inputs(direction, scenario):
    n = 210 if scenario == "expiry" else 60
    x = 100+np.sin(np.arange(n)*.73)*10
    bars = np.column_stack((x, x+2, x-2, x+.5))
    slots = np.arange(n)
    if scenario == "gap":
        slots[30:] += 3
    if scenario == "expiry":
        bars[:] = [100, 100.1, 99.9, 100]
    p = prepare_inputs(binding(bars, slots=slots, start=4*STEP), (),
                       (feature(), feature("atr", "atr", length=3)))
    c = build_commands(p, HARecipe("ha", "realSwingAtr", atrRequestId="atr", atrMultiplier=2),
                       mode="fixedBracket", stopUpdates=False)
    long, short = np.zeros(n, bool), np.zeros(n, bool)
    events = [4] if scenario == "expiry" else list(range(4, n-1, 3))
    if direction == "both":
        long[events[::2]], short[events[1::2]] = True, True
    else:
        (long if direction == "long" else short)[events] = True
    c = replace(c, long=replace(c.long, entry=long), short=replace(c.short, entry=short))
    pol = policy("fixedBracket", direction=direction, maxHoldingDays=4,
                 rewardRisk=100 if scenario == "expiry" else 2)
    return p, c, pol


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("scenario", ["continuous", "gap", "expiry"])
def test_unchanged_bracket_adapter_full_paths_and_trades(direction, scenario):
    p, c, pol = legacy_inputs(direction, scenario)
    w, r = window(p), rules()
    actual = execute_account(p, c, pol, w, r)
    t, v = p.target.timestamps, p.target.values
    bs = contracts.BarSeries("OKX_AAA-USDT-SWAP", 30, STEP, t, t//STEP, v, 4)
    settings = bracket.validate_settings(dict(directions=[direction], reward_risks=[pol.rewardRisk],
        atr_length=3, swing_lookback=2, atr_multiplier=2), [30])
    signals = c.long.entry if direction == "long" else c.short.entry
    events = pd.DataFrame([dict(variant_id="v", event_id=str(i), anchor_open_ms=int(t[i])) for i in np.flatnonzero(signals)])
    ref = bracket.evaluate(bs, events, [{"variant_id": "v"}],
        dict(model_instance_id="b", settings=settings), (w.startMs, w.endMs), r).tables
    for name in ("balance", "equity"):
        for a, b in zip(actual.path[name].to_pylist(), ref["path"][name].tolist()):
            close(a, b)
    assert len(actual.path) == len(ref["path"])
    assert len(actual.trades) == len(ref["trades"])
    for a, b in zip(actual.trades.to_pylist(), ref["trades"].to_dict("records")):
        for k, old in (("entryIndex", "entry_index"), ("exitIndex", "exit_index"), ("exitPhase", "exit_phase")):
            assert a[k] == b[old]
        assert a["exitReason"] == {"gap_boundary": "gapBoundary"}.get(b["exit_reason"], b["exit_reason"])
        for k, old in (("entryPrice", "entry_price"), ("exitPrice", "exit_price"), ("quantity", "quantity"),
                       ("entryFee", "entry_fee"), ("exitFee", "exit_fee"), ("netPnl", "net_pnl"), ("plannedCashRisk", "planned_cash_risk")):
            close(a[k], b[old])
    mapping = dict(indicator_unavailable="stopUnavailable", invalid_risk_distance="invalidRiskDistance",
                   zero_quantity="zeroQuantity", below_min_quantity="belowMinQuantity", nonpositive_capital="nonpositiveCapital",
                   leverage_undefined="leverageUndefined", below_min_notional="belowMinNotional",
                   leverage_cap_exceeded="leverageCapExceeded", reentry_suppressed="reentrySuppressed",
                   occupied="occupied", filled="filled")
    assert [(a["signalIndex"], a["reason"]) for a in actual.attempts.to_pylist()] == [
        (a["signal_index"], mapping[a["reason"]]) for a in ref["attempts"].to_dict("records")]


@pytest.mark.parametrize("scenario", ["continuous", "gap"])
def test_nonconflicting_shared_ls_segmented_v2(scenario):
    p, c, pol = legacy_inputs("both", scenario)
    r, w = rules(), window(p)
    actual = execute_account(p, c, pol, w, r)
    core = bracket.reference_core()
    stamps, bars = p.target.timestamps, p.target.values
    starts = np.flatnonzero(p.target.segmentStart)
    balance, expected, expected_trades = pol.initialCapital, [], []
    for first, end in zip(starts, np.r_[starts[1:], len(stamps)]):
        v, t = bars[first:end], stamps[first:end]
        atr = bracket.pine_atr(v[:, 1], v[:, 2], v[:, 3], 3)
        low = pd.Series(v[:, 2]).rolling(2).min().to_numpy()
        high = pd.Series(v[:, 1]).rolling(2).max().to_numpy()
        trace = core.KernelTrace()
        d = core.ExecutionData(pd.to_datetime(t, unit="ms", utc=True), *[v[:, i] for i in range(4)],
            core.Signals(c.long.entry[first:end], c.short.entry[first:end]), atr, low, high,
            np.full(len(t), np.nan), np.full(len(t), np.nan))
        cfg = core.KernelConfig(initial_capital=balance, commission_pct=pol.commissionPctPerSide, stop_x=2,
            reward_risk=2, max_days=4, risk_per_trade_pct=2, contract_size=float(r.base_step),
            trade_start_idx=max(0, 4-int(first)), start=pd.Timestamp(w.startMs, unit="ms", tz="UTC"),
            end=pd.Timestamp(w.endMs-STEP, unit="ms", tz="UTC"))
        ref = core.run_reference_kernel(d, cfg, policy=core.EntryPolicy(8, r.minimum_lots, None), trace=trace)
        for j in range(len(t)):
            if t[j] >= w.startMs:
                expected.append((ref.balance_curve[j], ref.equity_curve[j]))
        for trade, exit_ in zip(ref.trades, trace.exits):
            expected_trades.append((trade, exit_, int(first)))
        balance = ref.balance_curve[-1]
    assert len(expected) == len(actual.path) and len(expected_trades) == len(actual.trades)
    for a, b in zip(zip(actual.path["balance"].to_pylist(), actual.path["equity"].to_pylist()), expected):
        close(a[0], b[0]); close(a[1], b[1])
    for a, (t, ex, first) in zip(actual.trades.to_pylist(), expected_trades):
        assert a["side"] == (1 if t.direction == "long" else -1)
        assert a["entryIndex"] == first+ex.entry_index and a["exitIndex"] == first+ex.exit_index
        for k, v in (("quantity", t.size), ("entryPrice", t.entry_price), ("exitPrice", t.exit_price), ("netPnl", t.net_pnl)):
            close(a[k], v)


@pytest.mark.parametrize("table,column", [("trades", "entryPrice"), ("trades", "entryFee"),
    ("trades", "exitMs"), ("trades", "activeStop"), ("trades", "plannedCashRisk"),
    ("attempts", "availableAtMs"), ("attempts", "filterAllowed"), ("attempts", "reason"),
    ("proposals", "effectiveMs"), ("proposals", "newStop"), ("path", "equity"), ("path", "closePrice"),
    ("exitCommands", "decisionMs")])
def test_semantic_tampering_fails(table, column):
    p = prepared()
    c = bundle(p, long=side(6, entries=[0], exits=[3], proposals=[90, 91, 92, 93, 94, 95]),
               updates=True, filt=filter_for(p))
    pol, w, r = policy(updates=True), window(p), rules()
    result = execute_account(p, c, pol, w, r)
    original = getattr(result, table)
    rows = original.to_pylist()
    value = rows[0][column]
    rows[0][column] = not value if isinstance(value, bool) else "occupied" if isinstance(value, str) else value+1
    changed = type(original).from_pylist(rows, schema=original.schema)
    with pytest.raises(Error):
        validate_result(replace(result, **{table: changed}), p, c, pol, w, r)


def test_rules_filter_and_seed_identities_reuse_without_recalculation():
    p = prepare_inputs(binding([(100+i, 105+i, 99+i, 103+i) for i in range(12)]), (), (feature(),))
    c = build_commands(p, HARecipe("ha", entryRunLength=2), mode="signalManaged", stopUpdates=False)
    r, pol, w = rules(), policy(), window(p)
    first = execute_account(p, c, pol, w, r)
    changed_rules = replace(r, minimum_notional="1", enforce_minimum_notional=True)
    second = execute_account(p, c, pol, w, changed_rules)
    d = replace(c, entryFilter=filter_for(p, long=False))
    third = execute_account(p, d, pol, w, r)
    assert first.preparationId == second.preparationId == third.preparationId
    assert first.commandId == second.commandId != third.commandId
    assert len({first.identity, second.identity, third.identity}) == 3
    assert first.commands is c and second.commands is c
    assert d.long is c.long  # feature and command arrays are reusable
    for start in (0, 3*STEP, 7*STEP):
        execute_account(p, c, pol, AccountWindow(start, w.endMs), r)
    assert not p.feature("ha").values.flags.writeable


def test_pickle_and_explicit_spawn_readonly_and_equal():
    p = prepare_inputs(binding([(100+i, 105+i, 99+i, 103+i) for i in range(12)]), (), (feature(),))
    c = build_commands(p, HARecipe("ha", entryRunLength=2), mode="signalManaged", stopUpdates=True)
    pol, w, r = policy(updates=True), window(p), rules()
    direct = execute_account(p, c, pol, w, r)
    restored = pickle.loads(pickle.dumps(direct))
    validate_result(restored, p, c, pol, w, r)
    for a in (restored.commands.long.entry, restored.commands.decisionMs):
        with pytest.raises(ValueError):
            a.flat[0] = 0
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=spawn_run, args=(child, p, c, pol, w, r))
    process.start(); child.close()
    try:
        assert parent.poll(45), "spawn child did not return"
        protected, spawned = parent.recv()
        process.join(10)
        assert process.exitcode == 0 and all(protected)
        assert direct.identity == spawned.identity and direct.finalBalance == spawned.finalBalance
        for name in ("attempts", "trades", "proposals", "exitCommands", "path"):
            assert getattr(direct, name).equals(getattr(spawned, name))
        again = execute_account(p, c, pol, w, r)
        assert again.path.equals(direct.path)
    finally:
        if process.is_alive():
            process.terminate(); process.join(10)
        parent.close(); process.close()


def test_public_example_fresh_process_without_ignored_work_or_pythonpath():
    code = '''import sys, runpy
def guard(event, args):
    if event == "open" and isinstance(args[0], str) and "docs/_work" in args[0].replace(chr(92), "/"):
        raise RuntimeError("ignored research read")
sys.addaudithook(guard)
runpy.run_module("tools.pattern_lab.examples.account_synthetic", run_name="__main__")
'''
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    result = subprocess.run([sys.executable, "-B", "-X", "utf8", "-c", code],
        cwd=Path(__file__).resolve().parents[2], env=env, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["schema"] == "pattern-lab.account.v1" and value["trades"] > 0


def test_future_perturbation_preserves_prior_execution_decisions():
    bars = np.array([(100, 102, 98, 100)]*14, dtype=float)
    p = prepared(bars)
    bars[9:] *= 2
    q = prepared(bars)
    results = []
    for prep in (p, q):
        c = bundle(prep, long=side(14, entries=[0, 5, 9], exits=[2, 7, 11]))
        results.append(execute_account(prep, c, policy(), window(prep), rules()))
    a, b = results
    assert a.identity != b.identity
    for name in ("path", "attempts", "trades"):
        column = "barIndex" if name == "path" else "signalIndex"
        rows = [[{k: v for k, v in row.items() if k != "eventId"} for row in getattr(r, name).to_pylist()
                 if row[column] < 9] for r in results]
        assert rows[0] == rows[1]


@pytest.mark.parametrize("case", ["floor", "zero", "minimum", "capEqual", "capBelow", "nextOpenGap"])
def test_bracket_adapter_admission_edges(case):
    from ._bracket_helpers import evaluate
    bars = [(100, 102.5, 97.5, 100), (100, 101, 99, 100), (100, 101, 99, 100)]
    if case == "nextOpenGap":
        bars[1] = (85, 86, 84, 85)
    p = prepared(bars)
    c = bundle(p, mode="fixedBracket", long=side(3, entries=[0], stop=90))
    capital = 1150 if case == "floor" else .1 if case == "zero" else 10000
    cap = 2000/(10000-1) if case in ("capEqual", "capBelow") else 8
    if case == "capBelow":
        cap = math.nextafter(cap, 0)
    r = rules()
    if case == "minimum":
        r = replace(r, minimum_quantity="1000", base_minimum="100", minimum_lots=1000)
    pol = policy("fixedBracket", initialCapital=capital, maxLeverage=cap, maxHoldingDays=4)
    actual = execute_account(p, c, pol, window(p), r)
    old = evaluate([(*b, 1.) for b in bars], rules=r, initial_capital=capital, atr_multiplier=1.5,
                   commission_pct_per_side=.05, max_leverage=cap)
    reasons = dict(filled="filled", zero_quantity="zeroQuantity", below_min_quantity="belowMinQuantity",
                   leverage_cap_exceeded="leverageCapExceeded")
    assert actual.attempts["reason"].to_pylist() == [reasons[v] for v in old["attempts"].reason.tolist()]
    assert len(actual.trades) == len(old["trades"])
    for name in ("balance", "equity"):
        for a, b in zip(actual.path[name].to_pylist(), old["path"][name].tolist()):
            close(a, b)
    for a, b in zip(actual.trades.to_pylist(), old["trades"].to_dict("records")):
        assert a["quantity"] == b["quantity"] and a["entryPrice"] == b["entry_price"] and a["exitPrice"] == b["exit_price"]
