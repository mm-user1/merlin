"""One numerical account loop; generic commands only, no indicator formulas."""
from dataclasses import asdict
from decimal import Decimal, ROUND_CEILING
import math

import numpy as np

from .. import PatternLabDataError
from ..study.bracket import reference_core
from ..study.bracket_rules import ExecutionRules
from ..study.contracts import semantic_digest
from . import evidence
from .contracts import (SCHEMA, AccountResult, AccountWindow, ExecutionPolicy, Coverage,
                        require, integer, validate_bundle)
from .preparation import PreparedAccount


def validate_inputs(prepared, commands, policy, window, rules):
    require(isinstance(prepared, PreparedAccount) and prepared.schema == SCHEMA, "prepared schema")
    require(isinstance(policy, ExecutionPolicy) and isinstance(window, AccountWindow), "policy/window types")
    ExecutionPolicy(**asdict(policy))  # revalidate without mutating a shared policy
    validate_bundle(prepared, commands)
    require((commands.mode, commands.stopUpdates) == (policy.mode, policy.stopUpdates), "command mode mismatch")
    integer(window.startMs, "startMs", -(2**63))
    integer(window.endMs, "endMs", -(2**63))
    h = prepared.target.header
    require(h["start_ms"] <= window.startMs < window.endMs <= h["end_ms"] and
            window.startMs % prepared.stepMs == window.endMs % prepared.stepMs == 0, "window bounds/grid")
    require(isinstance(rules, ExecutionRules), "native ExecutionRules required")
    require(rules.venue == h["venue"] and rules.contract == h["contract"] and
            rules.quote_currency == rules.settlement_currency == h["quote_currency"] == "USDT", "rules instrument")
    integer(rules.minimum_lots, "minimum_lots", 1)
    require(rules.minimum_lots <= 2**53 and rules.enforce_minimum_quantity is True and
            type(rules.enforce_minimum_notional) is bool and
            rules.enforce_minimum_notional == (rules.minimum_notional is not None), "rules limits/flags")
    try:
        for name in ("base_step", "base_minimum", "quantity_step", "minimum_quantity"):
            value = getattr(rules, name)
            require(isinstance(value, str) and math.isfinite(float(value)) and float(value) > 0, "rules " + name)
        step, minimum = Decimal(rules.base_step), Decimal(rules.base_minimum)
        factor = Decimal(rules.ct_val) if rules.venue == "OKX" else Decimal(1)
        require((rules.venue == "OKX" and rules.ct_mult == "1" and factor > 0 and rules.quantity_unit == "contracts") or
                (rules.venue == "BYBIT" and rules.ct_val is None and rules.ct_mult is None and
                 rules.quantity_unit == rules.base_currency), "rules conversion")
        require(Decimal(rules.quantity_step) * factor == step and
                Decimal(rules.minimum_quantity) * factor == minimum and
                int((minimum / step).to_integral_value(rounding=ROUND_CEILING)) == rules.minimum_lots, "rules normalization")
        if rules.minimum_notional is not None:
            require(isinstance(rules.minimum_notional, str) and math.isfinite(float(rules.minimum_notional)) and
                    float(rules.minimum_notional) >= 0, "minimum notional")
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise PatternLabDataError("account: invalid rules arithmetic") from exc


def account_identity(prepared, commands, policy, window, rules):
    return semantic_digest(dict(schema=SCHEMA, preparationId=prepared.identity, commandId=commands.identity,
                                policy=asdict(policy), window=asdict(window), rules=asdict(rules)))


def window_indices(prepared, window):
    t = prepared.target.timestamps
    return np.flatnonzero((t >= window.startMs) & (t <= window.endMs - prepared.stepMs))


def coverage(prepared, window, indices):
    t, step = prepared.target.timestamps, prepared.stepMs
    count = (window.endMs-window.startMs)//step
    first, last = (int(t[indices[0]]), int(t[indices[-1]])+step) if len(indices) else (None, None)
    return Coverage(window.startMs, window.endMs, first, last, count, len(indices), count-len(indices),
                    count if first is None else (first-window.startMs)//step,
                    count if last is None else (window.endMs-last)//step)


def planning(core, close, stop, known, sign, balance, policy, rules):
    """Single-decision arithmetic, also checked by the evidence sweep."""
    result = dict(stop=float(stop) if math.isfinite(stop) else None,
                  target=None, riskDistance=None, quantity=None, lots=None)
    reason = "stopUnavailable"
    if known:
        risk = sign * (close-stop)
        result["riskDistance"] = risk if math.isfinite(risk) else None
        reason = "invalidRiskDistance"
        if stop > 0 and math.isfinite(risk) and risk > 0:
            reason = "nonpositiveCapital"
            if balance > 0:
                target = close + sign * policy.rewardRisk * risk if policy.mode == "fixedBracket" else None
                result["target"] = target if target is None or math.isfinite(target) else None
                reason = "invalidTarget"
                if target is None and policy.mode == "signalManaged" or target is not None and math.isfinite(target) and target > 0:
                    try:
                        q = core.risk_position_size(balance=balance, risk_distance=risk,
                            risk_per_trade_pct=policy.riskPct, contract_size=float(rules.base_step))
                        require(math.isfinite(q), "nonfinite sized quantity")
                        lots = core.quantity_lots(q, float(rules.base_step))
                    except (ValueError, ArithmeticError) as exc:
                        raise PatternLabDataError("account: unrepresentable sizing/lots") from exc
                    result.update(quantity=q, lots=lots)
                    reason = "zeroQuantity" if q == 0 else "belowMinQuantity" if lots < rules.minimum_lots else "planned"
    return reason, result


def admission(op, quantity, balance, policy, rules):
    notional = abs(op * quantity)
    fee = notional * (policy.commissionPctPerSide / 100.)
    denominator = balance - fee
    leverage = notional / denominator if denominator > 0 else math.nan
    finite = all(math.isfinite(x) for x in (notional, fee, denominator)) and (denominator <= 0 or math.isfinite(leverage))
    reason = ("leverageUndefined" if not finite else
              "belowMinNotional" if rules.minimum_notional is not None and notional < float(rules.minimum_notional) else
              "leverageUndefined" if not math.isfinite(leverage) else
              "leverageCapExceeded" if leverage > policy.maxLeverage else "filled")
    return reason, dict(notional=notional if math.isfinite(notional) else None,
                       proposedFee=fee if math.isfinite(fee) else None,
                       requiredLeverage=leverage if math.isfinite(leverage) else None)


def permissions(commands, i, sign):
    f = commands.entryFilter
    if f is None:
        return dict(filterKnown=True, filterAllowed=True, sourceIndex=-1, sourceReason=0,
                    observedAtMs=None, availableAtMs=None, validUntilMs=None)
    present = f.sourceIndex[i] >= 0
    return dict(filterKnown=bool(f.known[i]), filterAllowed=bool((f.allowLong if sign == 1 else f.allowShort)[i]),
        sourceIndex=int(f.sourceIndex[i]), sourceReason=int(f.unknownReason[i]),
        **{k: int(getattr(f, k)[i]) if present else None for k in ("observedAtMs", "availableAtMs", "validUntilMs")})


def gate_reason(policy, sign, facts):
    if policy.direction != "both" and policy.direction != ("long" if sign == 1 else "short"):
        return "directionDisabled"
    if not facts["filterKnown"]:
        return "filterUnknown"
    if not facts["filterAllowed"]:
        return "filterBlocked"
    return None


def proposal(old, price, known, sign):
    new = (max(old, price) if sign == 1 else min(old, price)) if known and price > 0 else old
    status = ("unavailable" if not known else "nonpositive" if price <= 0 else
              "tightened" if new != old else "equal" if price == old else "looseningIgnored")
    return new, status


def execute_account(prepared, commands, policy, window, rules):
    validate_inputs(prepared, commands, policy, window, rules)
    core = reference_core()
    stamps, bars, step = prepared.target.timestamps, prepared.target.values, prepared.stepMs
    indices = window_indices(prepared, window)
    attempts, trades, proposals, exits = [], [], [], []
    # Path is built directly in columns, avoiding a graph of per-bar dictionaries.
    path = {f.name: [] for f in evidence.PATH}
    balance, rate = policy.initialCapital, policy.commissionPctPerSide / 100.
    position = pending = None
    pending_exit = pending_expiry = False
    segment = -1

    def close(i, price, reason, phase, openingStop=False, openingTarget=False):
        nonlocal balance, position, pending_exit, pending_expiry
        p = position
        gross = p["side"] * (price-p["entryPrice"]) * p["quantity"]
        fee = price * p["quantity"] * rate
        balance += gross-fee
        clock = int(stamps[i]) + (step if phase == "close" else 0)
        trades.append({**p, "exitIndex": i, "exitMs": clock, "exitBarOpenMs": int(stamps[i]),
            "exitBarEndMs": int(stamps[i])+step, "exitPrice": price, "exitReason": reason, "exitPhase": phase,
            "holdingMs": clock-p["entryMs"], "exitFee": fee, "grossPnl": gross,
            "netPnl": gross-p["entryFee"]-fee, "balanceAfter": balance,
            "openingStop": openingStop, "openingTarget": openingTarget,
            "explicitExitPending": pending_exit, "expiryPending": pending_expiry})
        position = None
        pending_exit = pending_expiry = False

    for offset, raw_i in enumerate(indices):
        i = int(raw_i)
        t = int(stamps[i])
        op, hi, lo, cl = map(float, bars[i, :4])
        last = offset == len(indices)-1
        boundary = last or stamps[indices[offset+1]] != t+step
        if offset == 0 or t != int(stamps[indices[offset-1]])+step:
            segment += 1
        held = position is not None
        active_open = position["activeStop"] if held else None
        if position is not None:
            sign = position["side"]
            opening_stop = sign*(op-position["activeStop"]) <= 0
            opening_target = position["target"] is not None and sign*(op-position["target"]) >= 0
            if pending_expiry:
                close(i, op, "expiry", "open", opening_stop, opening_target)
            elif policy.mode == "signalManaged" and (opening_stop or pending_exit):
                close(i, op, "stop" if opening_stop else "explicitExit", "open", opening_stop)
        if pending is not None:
            require(not held and position is None, "entry queued across occupancy")
            a, pending = pending, None
            reason, facts = admission(op, a["quantity"], balance, policy, rules)
            a.update(reason=reason, fillIndex=i, fillMs=t, fillPrice=op, **facts)
            if reason == "filled":
                held = True
                balance -= a["proposedFee"]
                a["tradeId"] = len(trades)
                position = dict(tradeId=len(trades), attemptId=a["attemptId"], eventId=a["eventId"], side=a["side"],
                    signalIndex=a["signalIndex"], decisionMs=a["decisionMs"], entryIndex=i, entryMs=t,
                    entryPrice=op, quantity=a["quantity"], lots=a["lots"], initialStop=a["stop"],
                    activeStop=a["stop"], target=a["target"], riskDistance=a["riskDistance"],
                    plannedCashRisk=a["quantity"]*a["riskDistance"], entryFee=a["proposedFee"],
                    entryLeverage=a["requiredLeverage"], ambiguous=False)
                active_open = a["stop"]
        if position is not None:
            sign, stop, target = position["side"], position["activeStop"], position["target"]
            os = sign*(op-stop) <= 0
            ot = target is not None and sign*(op-target) >= 0
            if os or ot:
                close(i, op, "stop" if os else "target", "open", os, ot)
        if position is not None:
            stop, target = position["activeStop"], position["target"]
            if target is not None and lo <= stop <= hi and lo <= target <= hi:
                position["ambiguous"] = True
            prices = core.intrabar_path(op, hi, lo, cl)
            for start, end in zip(prices, prices[1:]):
                hits = [(abs(level-start), level, why) for level, why in ((stop, "stop"), (target, "target"))
                        if level is not None and min(start, end) <= level <= max(start, end)]
                if hits:
                    _, price, why = min(hits)
                    close(i, price, why, "intrabar")
                    break
        if boundary:
            if position is not None:
                close(i, cl, "terminal" if last else "gapBoundary", "close")
            pending_exit = pending_expiry = False
        elif position is not None:
            if policy.mode == "fixedBracket":
                pending_expiry = policy.maxHoldingDays is not None and t-position["entryMs"] >= policy.maxHoldingDays*86400000
            else:
                c = commands.long if position["side"] == 1 else commands.short
                known, event = bool(c.exitKnown[i]), bool(c.exit[i])
                pending_exit = known and event
                exits.append(dict(tradeId=position["tradeId"], decisionIndex=i, decisionMs=t+step,
                    effectiveMs=t+step, known=known, event=event,
                    status="scheduled" if pending_exit else "knownFalse" if known else "unknown"))
                if policy.stopUpdates:
                    old, price, known = position["activeStop"], float(c.proposedStop[i]), bool(c.proposedStopKnown[i])
                    new, status = proposal(old, price, known, position["side"])
                    proposals.append(dict(tradeId=position["tradeId"], decisionIndex=i, decisionMs=t+step,
                        effectiveMs=t+step, oldStop=old, newStop=new, known=known,
                        proposal=price if math.isfinite(price) else None, status=status))
                    position["activeStop"] = new
        raw = {s: bool(c.entryKnown[i] and c.entry[i]) for s, c in ((1, commands.long), (-1, commands.short))}
        facts = {s: permissions(commands, i, s) for s in (1, -1)}
        gates = {s: gate_reason(policy, s, facts[s]) for s in (1, -1)}
        conflict = all(raw[s] and gates[s] is None for s in (1, -1))
        for sign, c in ((1, commands.long), (-1, commands.short)):
            if not raw[sign]:
                continue
            reason = gates[sign] or ("entryConflict" if conflict else "noNextBar" if boundary else
                "occupied" if position is not None else "reentrySuppressed" if held else "planned")
            a = dict(attemptId=len(attempts), eventId=semantic_digest(dict(commandId=commands.identity, side=sign, decisionMs=t+step)),
                side=sign, signalIndex=i, decisionMs=t+step, intendedFillMs=t+step, anchorPrice=cl,
                balance=balance, reason=reason, rawConflict=all(raw.values()), eligibleConflict=conflict,
                occupied=position is not None, heldThisBar=held, **facts[sign])
            if reason == "planned":
                reason, plan = planning(core, cl, float(c.initialStop[i]), bool(c.initialStopKnown[i]), sign, balance, policy, rules)
                a.update(reason=reason, **plan)
                if reason == "planned":
                    pending = a
            attempts.append(a)
        q, sign, ep = (0., 0, None) if position is None else (position["quantity"], position["side"], position["entryPrice"])
        equity = balance + (sign*(cl-ep)*q if position is not None else 0.)
        require(math.isfinite(balance) and math.isfinite(equity), "nonfinite balance/equity")
        row = (i, t, cl, balance, equity, sign, q, segment, boundary, last, ep,
               active_open, None if position is None else position["activeStop"])
        for key, value in zip(path, row):
            path[key].append(value)
    require(position is None and pending is None and not pending_exit and not pending_expiry, "unclosed terminal state")
    result = AccountResult(SCHEMA, account_identity(prepared, commands, policy, window, rules), prepared.identity,
        commands.identity, policy, window, rules, coverage(prepared, window, indices),
        "closed" if len(indices) else "noObservedBars", balance,
        sum(not row["known"] for row in exits), commands,
        evidence.table(attempts, evidence.ATTEMPTS), evidence.table(trades, evidence.TRADES),
        evidence.table(proposals, evidence.PROPOSALS), evidence.table(exits, evidence.EXITS),
        evidence.pa.Table.from_pydict(path, schema=evidence.PATH))
    from .validation import validate_result
    validate_result(result, prepared, commands, policy, window, rules)
    return result
