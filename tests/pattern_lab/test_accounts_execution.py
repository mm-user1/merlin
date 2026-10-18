from dataclasses import replace
import math

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError as Error
from tools.pattern_lab.accounts import (execute_account, AccountWindow, ExecutionPolicy, SideCommands,
    prepare_inputs, candle_filter)
from ._account_helpers import prepared, binding, feature, bundle, side, policy, window, rules, filter_for, STEP


def run(p, c, pol=None, w=None, r=None):
    return execute_account(p, c, pol or policy(c.mode, c.stopUpdates), w or window(p), r or rules())


@pytest.mark.parametrize("sign", [1, -1])
def test_next_open_explicit_exit_and_hand_cashflows(sign):
    p = prepared([(100, 102, 98, 101), (102, 104, 100, 103), (104, 106, 102, 105), (106, 108, 104, 107)])
    c = bundle(p, **{("long" if sign == 1 else "short"): side(4, entries=[0], stop=91 if sign == 1 else 111, exits=[2])})
    r = run(p, c)
    t = r.trades.to_pylist()[0]
    assert t["entryIndex"] == 1 and t["entryPrice"] == 102
    assert t["exitIndex"] == 3 and t["exitPrice"] == 106 and t["exitReason"] == "explicitExit"
    assert t["quantity"] == 20 and t["entryFee"] == 1.02 and t["exitFee"] == 1.06
    assert t["grossPnl"] == sign*80
    assert math.isclose(r.finalBalance, 10000+sign*80-2.08, rel_tol=1e-9, abs_tol=1e-8)
    path = r.path.to_pydict()
    assert path["balance"][1] == 10000-1.02
    assert path["equity"][1] == 10000-1.02+sign*20


@pytest.mark.parametrize("sign", [1, -1])
def test_open_stop_precedes_explicit_and_entry_bar_stop(sign):
    bars = [(100, 102, 98, 100), (100, 102, 98, 100),
            (89, 91, 87, 89) if sign == 1 else (111, 113, 109, 111)]
    p = prepared(bars)
    name = "long" if sign == 1 else "short"
    c = bundle(p, **{name: side(3, entries=[0], stop=90 if sign == 1 else 110, exits=[1])})
    t = run(p, c).trades.to_pylist()[0]
    assert t["exitReason"] == "stop" and t["openingStop"] and t["explicitExitPending"]
    assert t["exitPrice"] == bars[-1][0]
    p = prepared([bars[0], bars[-1]])
    c = bundle(p, **{name: side(2, entries=[0], stop=90 if sign == 1 else 110)})
    t = run(p, c).trades.to_pylist()[0]
    assert t["entryIndex"] == t["exitIndex"] == 1 and t["grossPnl"] == 0
    assert t["netPnl"] == -t["entryFee"]-t["exitFee"]


@pytest.mark.parametrize("sign", [1, -1])
def test_stop_updates_all_statuses_and_next_bar_crossing_close(sign):
    p = prepared([(100, 102, 98, 100)]*8)
    values = [np.nan, 92, 92, 91, np.nan, 0, 101, np.nan]
    if sign == -1:
        values = [np.nan, 108, 108, 109, np.nan, 0, 99, np.nan]
    c = bundle(p, updates=True, **{("long" if sign == 1 else "short"): side(8, entries=[0], stop=90 if sign == 1 else 110, proposals=values)})
    r = run(p, c)
    assert r.proposals["status"].to_pylist() == ["tightened", "equal", "looseningIgnored", "unavailable", "nonpositive", "tightened"]
    t = r.trades.to_pylist()[0]
    assert t["exitIndex"] == 7 and t["exitPrice"] == 100 and t["exitPhase"] == "open"
    assert t["plannedCashRisk"] == 200 and t["riskDistance"] == 10
    assert r.path["activeStopAtOpen"].to_pylist()[6] == (92 if sign == 1 else 108)
    assert r.path["stopNextBar"].to_pylist()[6] == (101 if sign == 1 else 99)


def test_unknown_exit_preserved_and_protection_continues():
    p = prepared([(100, 102, 98, 100)]*4+[(89, 100, 88, 99)])
    c = bundle(p, long=side(5, entries=[0], exits=[1, 2], exitKnown=[True, False, False, True, True]))
    r = run(p, c)
    assert r.exitCommands["status"].to_pylist() == ["unknown", "unknown", "knownFalse"]
    assert r.unknownExitDecisions == 2
    assert r.trades["exitReason"].to_pylist() == ["stop"]


@pytest.mark.parametrize("allowLong,allowShort,known,expected", [
    (True, True, True, ["entryConflict", "entryConflict"]),
    (True, False, True, ["filled", "filterBlocked"]),
    (False, True, True, ["filterBlocked", "filled"]),
    (False, False, True, ["filterBlocked", "filterBlocked"]),
    (True, True, False, ["filterUnknown", "filterUnknown"])])
def test_conflict_after_filter_and_permissions(allowLong, allowShort, known, expected):
    p = prepared()
    c = bundle(p, long=side(6, entries=[0]), short=side(6, entries=[0], stop=110),
               filt=filter_for(p, allowLong, allowShort, known))
    r = run(p, c)
    assert r.attempts["reason"].to_pylist() == expected
    assert r.attempts["rawConflict"].to_pylist() == [True, True]


def test_side_precedes_unknown_conflict_precedes_bad_stop_and_boundary():
    p = prepared()
    c = bundle(p, long=side(6, entries=[0, 5], stop=-1), short=side(6, entries=[0, 5], stop=-1))
    assert run(p, c).attempts["reason"].to_pylist() == ["entryConflict"]*4
    c = replace(c, entryFilter=filter_for(p, known=False))
    assert run(p, c, policy(direction="long")).attempts["reason"].to_pylist() == ["filterUnknown", "directionDisabled"]*2


@pytest.mark.parametrize("sign", [1, -1])
def test_candle_filter_canonical_permissions_preserve_execution(sign):
    p = prepare_inputs(binding([(100, 102, 98, 100+sign)]*6), (),
        (feature("state", "candleDirection", priorBars=1, candleType="real", confirmationBars=1),))
    a = p.feature("state").aligned
    filt = candle_filter(p, "state")
    former = replace(filt, allowLong=a.value == 1, allowShort=a.value == -1)
    assert a.value[0] == sign and not a.known[0]
    assert filt.identity != former.identity
    name = "long" if sign == 1 else "short"
    commands = bundle(p, filt=filt, **{name: side(6, entries=[0, 1],
        stop=90 if sign == 1 else 110, exits=[3])})
    former_commands = replace(commands, entryFilter=former)
    assert commands.identity != former_commands.identity
    corrected, previous = run(p, commands), run(p, former_commands)
    for result in (corrected, previous):
        assert result.attempts["reason"].to_pylist() == ["filterUnknown", "filled"]
        assert result.attempts["filterKnown"].to_pylist() == [False, True]
        assert len(result.trades) == 1
    assert corrected.attempts["filterAllowed"].to_pylist() == [False, True]
    assert previous.attempts["filterAllowed"].to_pylist() == [True, True]
    # Event IDs include command identity; compare execution facts, not those IDs.
    trade_columns = [n for n in corrected.trades.column_names if n != "eventId"]
    assert corrected.trades.select(trade_columns).equals(previous.trades.select(trade_columns))
    assert corrected.path.equals(previous.path)  # includes quantity, balance and equity
    assert corrected.finalBalance == previous.finalBalance


def test_occupied_exit_bar_suppression_new_pulse_and_shared_balance():
    p = prepared([(100, 102, 98, 100)]*7)
    c = bundle(p, long=side(7, entries=[0, 1, 2, 3, 4, 6], exits=[2]))
    r = run(p, c)
    assert r.attempts["reason"].to_pylist() == ["filled", "occupied", "occupied", "reentrySuppressed", "filled", "noNextBar"]
    assert len(r.trades) == 2
    assert r.trades["quantity"].to_pylist()[1] < r.trades["quantity"].to_pylist()[0]


def test_blocked_pulse_consumed_and_exit_continues_while_blocked():
    p = prepared()
    c = bundle(p, long=side(6, entries=[0, 2], exits=[3]),
               filt=filter_for(p, long=[False, True, True, False, False, False]))
    r = run(p, c)
    assert r.attempts["reason"].to_pylist() == ["filterBlocked", "filled"]
    assert r.trades["entryIndex"].to_pylist() == [3]
    assert r.trades["exitReason"].to_pylist() == ["explicitExit"]


@pytest.mark.parametrize("op,sign,expected", [(100, 1, "stop"), (101, 1, "target"), (100, -1, "target"), (101, -1, "stop")])
def test_bracket_both_barriers_path_tie(op, sign, expected):
    p = prepared([(100, 102, 98, 100), (op, 112, 88, 100)])
    c = bundle(p, mode="fixedBracket", **{("long" if sign == 1 else "short"): side(2, entries=[0], stop=90 if sign == 1 else 110)})
    t = run(p, c).trades.to_pylist()[0]
    assert t["exitReason"] == expected and t["exitPhase"] == "intrabar" and t["ambiguous"]


def test_four_day_expiry_and_coincident_open_stop_null_disables():
    bars = [(100, 102, 98, 100)]*197
    bars[194] = (80, 82, 78, 80)
    p = prepared(bars)
    c = bundle(p, mode="fixedBracket", long=side(197, entries=[0], stop=90))
    t = run(p, c, policy("fixedBracket", maxHoldingDays=4)).trades.to_pylist()[0]
    assert t["exitIndex"] == 194 and t["holdingMs"] == int(96.5*3600000)
    assert t["exitReason"] == "expiry" and t["openingStop"] and t["expiryPending"]
    assert run(p, c).trades["exitReason"].to_pylist() == ["stop"]


def test_windows_gaps_missing_tail_empty_and_flat_restart():
    p = prepared([(100, 102, 98, 100)]*7, slots=[0, 1, 2, 5, 6, 8, 9], start=STEP, end=12*STEP)
    c = bundle(p, long=side(7, entries=[0, 1, 3, 5]))
    r = run(p, c)
    assert r.trades["entryIndex"].to_pylist() == [2, 4, 6]
    assert r.trades["exitReason"].to_pylist() == ["gapBoundary", "gapBoundary", "terminal"]
    assert r.coverage.lastObservedCloseMs == 10*STEP and r.coverage.missingTailSlots == 2
    short = run(p, c, w=AccountWindow(5*STEP, 8*STEP))
    assert short.trades["exitReason"].to_pylist() == ["terminal"]  # external later bars do not relabel
    assert short.trades["quantity"].to_pylist()[0] == 20
    empty = run(p, c, w=AccountWindow(3*STEP, 5*STEP))
    assert empty.terminalStatus == "noObservedBars" and len(empty.path) == len(empty.trades) == 0
    assert empty.finalBalance == 10000
    assert p.identity == r.preparationId == short.preparationId == empty.preparationId
    assert len({r.identity, short.identity, empty.identity}) == 3
    with pytest.raises(Error):
        run(p, c, w=AccountWindow(0, STEP*2))


@pytest.mark.parametrize("stop,known,capital,risk,expected", [
    (90, False, 10000, 2, "stopUnavailable"), (0, True, 10000, 2, "invalidRiskDistance"),
    (101, True, 10000, 2, "invalidRiskDistance"), (90, True, .1, 2, "zeroQuantity")])
def test_planning_rejections(stop, known, capital, risk, expected):
    p = prepared()
    c = bundle(p, long=side(6, entries=[0], stop=stop, stopKnown=[known]*6))
    assert run(p, c, policy(initialCapital=capital, riskPct=risk)).attempts["reason"].to_pylist() == [expected]


def test_native_float_floor_minimum_and_unrepresentable_quantity():
    p = prepared()
    c = bundle(p, long=side(6, entries=[0]))
    r = run(p, c, policy(initialCapital=1150))
    assert r.trades["quantity"].to_pylist() == [2.2]
    higher = replace(rules(), minimum_quantity="1000", base_minimum="100", minimum_lots=1000)
    assert run(p, c, r=higher).attempts["reason"].to_pylist() == ["belowMinQuantity"]
    with pytest.raises(Error):
        run(p, c, policy(initialCapital=1e308))


def test_actual_open_leverage_boundary_and_min_notional_order():
    p = prepared()
    c = bundle(p, long=side(6, entries=[0]))
    cap = 2000/(10000-1)
    assert run(p, c, policy(maxLeverage=cap)).attempts["reason"].to_pylist() == ["filled"]
    assert run(p, c, policy(maxLeverage=math.nextafter(cap, 0))).attempts["reason"].to_pylist() == ["leverageCapExceeded"]
    minimum = replace(rules(), minimum_notional="2001", enforce_minimum_notional=True)
    assert run(p, c, r=minimum).attempts["reason"].to_pylist() == ["belowMinNotional"]
    # Finite nonpositive denominator checks minimum before undefined leverage.
    c = bundle(p, long=side(6, entries=[0], stop=99.99))
    minimum = replace(rules(), minimum_notional="1000000000", enforce_minimum_notional=True)
    assert run(p, c, policy(commissionPctPerSide=99), r=minimum).attempts["reason"].to_pylist() == ["belowMinNotional"]
    assert run(p, c, policy(commissionPctPerSide=99)).attempts["reason"].to_pylist() == ["leverageUndefined"]


def test_invalid_target_and_negative_equity_nonpositive_future_capital():
    p = prepared()
    c = bundle(p, mode="fixedBracket", short=side(6, entries=[0], stop=300))
    assert run(p, c).attempts["reason"].to_pylist() == ["invalidTarget"]
    p = prepared([(100, 102, 98, 100), (100, 102, 98, 100), (1, 2, .5, 1),
                  (1, 2, .5, 1), (1, 2, .5, 1)])
    c = bundle(p, long=side(5, entries=[0, 3], stop=[90, 90, .1, .1, .1]))
    r = run(p, c, policy(riskPct=100, maxLeverage=100))
    assert r.finalBalance < 0 and min(r.path["equity"].to_pylist()) < 0
    assert r.attempts["reason"].to_pylist() == ["filled", "nonpositiveCapital"]


@pytest.mark.parametrize("kwargs", [dict(initialCapital=True), dict(riskPct=True), dict(maxLeverage=float("inf")),
    dict(commissionPctPerSide=100), dict(riskPct=101), dict(maxHoldingDays=1), dict(rewardRisk=1),
    dict(direction="LS"), dict(stopUpdates=1)])
def test_bad_policy(kwargs):
    with pytest.raises(Error):
        policy(**kwargs)


def test_closed_policy_and_active_disabled_commands():
    with pytest.raises(TypeError):
        ExecutionPolicy("signalManaged", "both", None, False)
    with pytest.raises(TypeError):
        policy(unknown=True)
    with pytest.raises(Error):
        policy("fixedBracket", updates=True)
    p = prepared()
    with pytest.raises(Error):
        bundle(p, mode="fixedBracket", long=side(6, exits=[0]))
    with pytest.raises(Error):
        bundle(p, long=side(6, proposals=[90]*6))
    with pytest.raises(Error):
        side(6, stop=float("nan"))
    with pytest.raises(Error):
        side(6, stop=float("inf"), stopKnown=[False]*6)
    with pytest.raises(Error):
        run(p, bundle(p), policy("fixedBracket"))
    with pytest.raises(Error):
        run(p, bundle(p), r=replace(rules(), minimum_lots=True))
    with pytest.raises(Error):
        run(p, bundle(p), w=AccountWindow(True, 2*STEP))


def test_missing_head_no_warmup_order_and_start_is_first_decision():
    p = prepared([(100, 102, 98, 100)]*4, slots=[0, 3, 4, 5], start=STEP)
    c = bundle(p, long=side(4, entries=[0, 1]))
    result = run(p, c)
    assert result.coverage.missingHeadSlots == 2
    assert result.attempts["signalIndex"].to_pylist() == [1]
    assert result.trades["entryIndex"].to_pylist() == [2]


@pytest.mark.parametrize("changes", [dict(base_step="0"), dict(base_step="nan"), dict(base_step=True),
    dict(minimum_lots=0), dict(minimum_lots=2**54), dict(minimum_notional="inf", enforce_minimum_notional=True),
    dict(ct_mult="2"), dict(base_minimum="100"), dict(enforce_minimum_quantity=False)])
def test_malformed_native_rules_fail(changes):
    p = prepared()
    with pytest.raises(Error):
        run(p, bundle(p), r=replace(rules(), **changes))
