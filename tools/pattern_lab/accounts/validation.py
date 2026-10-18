"""Bounded evidence consistency sweep; deliberately no first-exit search."""
import math

import numpy as np

from ..study.bracket import reference_core
from ..study.contracts import semantic_digest
from . import evidence
from .contracts import SCHEMA, AccountResult, CommandBundle, require
from .execution import (validate_inputs, account_identity, window_indices, coverage,
                        planning, admission, permissions, gate_reason, proposal)


def money(a, b):
    require(type(a) in (int, float) and math.isfinite(a) and math.isfinite(b) and
            math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-8), "accumulated money mismatch")


def exact_row(actual, expected, schema):
    require(set(actual) == set(schema.names), "evidence columns")
    for name in schema.names:
        require(actual[name] == expected.get(name), "evidence mismatch: " + name)


def validate_result(result, prepared, commands, policy, window, rules):
    """Check supplied evidence against immutable inputs; raises PatternLabDataError."""
    validate_inputs(prepared, commands, policy, window, rules)
    require(isinstance(result, AccountResult) and result.schema == SCHEMA, "result schema")
    require(result.preparationId == prepared.identity and result.commandId == commands.identity and
            result.identity == account_identity(prepared, commands, policy, window, rules), "result identity")
    require(result.policy == policy and result.window == window and result.rules == rules, "result parameters")
    require(isinstance(result.commands, CommandBundle) and result.commands.identity == commands.identity,
            "retained command evidence")
    indices = window_indices(prepared, window)
    require(result.coverage == coverage(prepared, window, indices) and
            result.terminalStatus == ("closed" if len(indices) else "noObservedBars"), "result coverage")
    for name, schema in (("attempts", evidence.ATTEMPTS), ("trades", evidence.TRADES),
                         ("proposals", evidence.PROPOSALS), ("exitCommands", evidence.EXITS), ("path", evidence.PATH)):
        t = getattr(result, name)
        require(isinstance(t, evidence.pa.Table) and t.schema == schema, "table schema " + name)
        for f in schema:
            if not f.nullable:
                require(t[f.name].null_count == 0, "required evidence null")
            if evidence.pa.types.is_floating(f.type):
                require(all(v is None or math.isfinite(v) for v in t[f.name].to_pylist()), "nonfinite evidence")
    require(len(result.path) == len(indices), "path coverage")
    stamps, bars, step = prepared.target.timestamps, prepared.target.values, prepared.stepMs
    core, rate = reference_core(), policy.commissionPctPerSide/100.
    trades, attempts = result.trades.to_pylist(), result.attempts.to_pylist()
    updates, exits = result.proposals.to_pylist(), result.exitCommands.to_pylist()
    n = len(stamps)
    held = np.zeros(n, bool)
    owner = np.full(n, -1, dtype=np.int64)
    active_open = np.full(n, np.nan)
    stop_next = np.full(n, np.nan)
    entries, closures = {}, {}
    expected_updates, expected_exits = [], []
    index_set = set(map(int, indices))
    trade_by_attempt = {}
    previous_exit = -1
    for j, t in enumerate(trades):
        a, e, x, sign = t["signalIndex"], t["entryIndex"], t["exitIndex"], t["side"]
        require(t["tradeId"] == j and sign in (1, -1) and a in index_set and e in index_set and x in index_set and
                e == a+1 and a > previous_exit and e <= x and np.all(np.diff(stamps[a:x+1]) == step), "trade links/chronology")
        previous_exit = x
        require(t["attemptId"] not in trade_by_attempt and 0 <= t["attemptId"] < len(attempts), "attempt trade link")
        trade_by_attempt[t["attemptId"]] = t
        attempt = attempts[t["attemptId"]]
        require(attempt["tradeId"] == j and attempt["reason"] == "filled" and attempt["signalIndex"] == a and
                attempt["side"] == sign and attempt["eventId"] == t["eventId"], "filled trade link")
        for tk, ak in (("quantity", "quantity"), ("lots", "lots"), ("initialStop", "stop"),
                       ("target", "target"), ("riskDistance", "riskDistance"), ("entryLeverage", "requiredLeverage"),
                       ("entryFee", "proposedFee")):
            require(t[tk] == attempt[ak], "original trade arithmetic " + tk)
        require(t["decisionMs"] == int(stamps[a])+step and t["entryMs"] == int(stamps[e]) and
                t["entryPrice"] == float(bars[e, 0]), "real entry timing/price")
        require(t["plannedCashRisk"] == t["quantity"]*t["riskDistance"], "original cash risk")
        require(t["exitPhase"] in ("open", "intrabar", "close"), "exit phase")
        require(t["exitBarOpenMs"] == int(stamps[x]) and t["exitBarEndMs"] == int(stamps[x])+step and
                t["exitMs"] == int(stamps[x])+(step if t["exitPhase"] == "close" else 0) and
                t["holdingMs"] == t["exitMs"]-t["entryMs"], "exit/holding clock")
        c = commands.long if sign == 1 else commands.short
        active = t["initialStop"]
        for i in range(e, x+1):
            require(not held[i], "overlapping positions")
            held[i], active_open[i] = True, active
            if i == x:
                break
            owner[i] = j
            if policy.mode == "signalManaged":
                known, event = bool(c.exitKnown[i]), bool(c.exit[i])
                expected_exits.append(dict(tradeId=j, decisionIndex=i, decisionMs=int(stamps[i])+step,
                    effectiveMs=int(stamps[i])+step, known=known, event=event,
                    status="scheduled" if known and event else "knownFalse" if known else "unknown"))
                require(not (known and event) or i+1 == x, "unexecuted explicit exit")
                if policy.stopUpdates:
                    price, known = float(c.proposedStop[i]), bool(c.proposedStopKnown[i])
                    new, status = proposal(active, price, known, sign)
                    expected_updates.append(dict(tradeId=j, decisionIndex=i, decisionMs=int(stamps[i])+step,
                        effectiveMs=int(stamps[i])+step, oldStop=active, newStop=new, known=known,
                        proposal=price if math.isfinite(price) else None, status=status))
                    active = new
            elif policy.maxHoldingDays is not None:
                require(int(stamps[i])-t["entryMs"] < policy.maxHoldingDays*86400000 or i+1 == x, "unexecuted expiry")
            stop_next[i] = active
        require(t["activeStop"] == active, "stop activation/monotonicity")
        op, hi, lo, cl = map(float, bars[x, :4])
        os = sign*(op-active) <= 0
        ot = t["target"] is not None and sign*(op-t["target"]) >= 0
        pending_exit = policy.mode == "signalManaged" and x > e and bool(c.exitKnown[x-1] and c.exit[x-1])
        pending_expiry = policy.mode == "fixedBracket" and policy.maxHoldingDays is not None and x > e and int(stamps[x-1])-t["entryMs"] >= policy.maxHoldingDays*86400000
        require(t["explicitExitPending"] == pending_exit and t["expiryPending"] == pending_expiry, "exit trigger facts")
        require(t["openingStop"] == (os if t["exitPhase"] == "open" else False) and
                t["openingTarget"] == (ot if t["exitPhase"] == "open" else False), "opening trigger facts")
        why, phase, price = t["exitReason"], t["exitPhase"], t["exitPrice"]
        if pending_expiry or os or pending_exit or ot:
            expected = "expiry" if pending_expiry else "stop" if os else "explicitExit" if pending_exit else "target"
            require(why == expected and phase == "open" and price == op, "opening precedence")
        elif why in ("stop", "target"):
            level = active if why == "stop" else t["target"]
            require(level is not None and phase == "intrabar" and price == level and lo <= price <= hi, "real protective fill")
        else:
            last = x == int(indices[-1])
            boundary = last or stamps[x+1] != stamps[x]+step
            require(boundary and phase == "close" and price == cl and
                    why == ("terminal" if last else "gapBoundary"), "boundary fill")
        # Ambiguity is a range fact on processed intrabar bars, not a forward exit search.
        ambiguous = False
        if t["target"] is not None:
            stop_at = x if phase != "open" else x-1
            for i in range(e, stop_at+1):
                ambiguous |= bool(bars[i, 2] <= t["initialStop"] <= bars[i, 1] and bars[i, 2] <= t["target"] <= bars[i, 1])
        require(t["ambiguous"] == ambiguous, "ambiguity fact")
        require(t["entryFee"] == abs(t["entryPrice"]*t["quantity"])*rate and
                t["exitFee"] == price*t["quantity"]*rate and
                t["grossPnl"] == sign*(price-t["entryPrice"])*t["quantity"], "single-event fees/PnL")
        money(t["netPnl"], t["grossPnl"]-t["entryFee"]-t["exitFee"])
        entries[e], closures[x] = t, t
    require(len(updates) == len(expected_updates) and len(exits) == len(expected_exits), "command trace coverage")
    require(type(result.unknownExitDecisions) is int and
            result.unknownExitDecisions == sum(not row["known"] for row in expected_exits), "unknown exit count")
    for actual, expected in zip(updates, expected_updates):
        exact_row(actual, expected, evidence.PROPOSALS)
    for actual, expected in zip(exits, expected_exits):
        exact_row(actual, expected, evidence.EXITS)
    balance, segment = policy.initialCapital, -1
    path_balance = {}
    # Read columns once. Arrow arrays transport exact float64; no missing row is zero.
    path_columns = {k: result.path[k].to_pylist() for k in evidence.PATH.names}
    for offset, raw_i in enumerate(indices):
        i = int(raw_i)
        if i in entries:
            balance -= entries[i]["entryFee"]
        if i in closures:
            t = closures[i]
            balance += t["grossPnl"]-t["exitFee"]
            money(t["balanceAfter"], balance)
        t = trades[owner[i]] if owner[i] >= 0 else None
        q, sign, ep = (t["quantity"], t["side"], t["entryPrice"]) if t else (0., 0, None)
        equity = balance + (sign*(float(bars[i, 3])-ep)*q if t else 0.)
        last = offset == len(indices)-1
        boundary = last or stamps[indices[offset+1]] != stamps[i]+step
        if offset == 0 or stamps[i] != stamps[indices[offset-1]]+step:
            segment += 1
        expected = dict(barIndex=i, barOpenMs=int(stamps[i]), closePrice=float(bars[i, 3]), side=sign,
            quantity=q, segment=segment, segmentEnd=boundary, windowEnd=last, entryPrice=ep,
            activeStopAtOpen=None if np.isnan(active_open[i]) else float(active_open[i]),
            stopNextBar=None if np.isnan(stop_next[i]) else float(stop_next[i]))
        for key, value in expected.items():
            require(path_columns[key][offset] == value, "path " + key)
        money(path_columns["balance"][offset], balance)
        money(path_columns["equity"][offset], equity)
        path_balance[i] = balance
    money(result.finalBalance, balance)
    money(result.finalBalance, policy.initialCapital+sum(t["netPnl"] for t in trades))
    pointer = 0
    for offset, raw_i in enumerate(indices):
        i, t = int(raw_i), int(stamps[raw_i])
        boundary = offset == len(indices)-1 or stamps[indices[offset+1]] != t+step
        raw = {s: bool(c.entryKnown[i] and c.entry[i]) for s, c in ((1, commands.long), (-1, commands.short))}
        facts = {s: permissions(commands, i, s) for s in (1, -1)}
        gates = {s: gate_reason(policy, s, facts[s]) for s in (1, -1)}
        conflict = all(raw[s] and gates[s] is None for s in (1, -1))
        for sign, c in ((1, commands.long), (-1, commands.short)):
            if not raw[sign]:
                continue
            require(pointer < len(attempts), "missing raw entry attempt")
            reason = gates[sign] or ("entryConflict" if conflict else "noNextBar" if boundary else
                "occupied" if owner[i] >= 0 else "reentrySuppressed" if held[i] else "planned")
            expected = dict(attemptId=pointer, eventId=semantic_digest(dict(commandId=commands.identity, side=sign, decisionMs=t+step)),
                side=sign, signalIndex=i, decisionMs=t+step, intendedFillMs=t+step, anchorPrice=float(bars[i, 3]),
                balance=path_balance[i], reason=reason, rawConflict=all(raw.values()), eligibleConflict=conflict,
                occupied=bool(owner[i] >= 0), heldThisBar=bool(held[i]), **facts[sign])
            if reason == "planned":
                reason, plan = planning(core, float(bars[i, 3]), float(c.initialStop[i]), bool(c.initialStopKnown[i]),
                                        sign, path_balance[i], policy, rules)
                expected.update(reason=reason, **plan)
                if reason == "planned":
                    reason, fill = admission(float(bars[i+1, 0]), plan["quantity"], path_balance[i], policy, rules)
                    expected.update(reason=reason, fillIndex=i+1, fillMs=int(stamps[i+1]), fillPrice=float(bars[i+1, 0]), **fill)
                    require((pointer in trade_by_attempt) == (reason == "filled"), "admitted trade missing/rejected trade present")
                    if reason == "filled":
                        expected["tradeId"] = trade_by_attempt[pointer]["tradeId"]
            actual = attempts[pointer]
            # Balance is accumulated money; planning expressions themselves are exact.
            money(actual["balance"], expected["balance"])
            expected["balance"] = actual["balance"]
            exact_row(actual, expected, evidence.ATTEMPTS)
            pointer += 1
    require(pointer == len(attempts), "unexpected attempts")
