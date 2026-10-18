"""Test-only forward trade search and separate cashflow reconstruction.

Independent scalar translation of the GEX004 oracle pattern, with native inputs;
does not import account execution arithmetic or the V2 core.
"""
import math


def signal_oracle(p, commands, policy, window, rules):
    stamps, bars, step = p.target.timestamps, p.target.values, p.stepMs
    indices = [i for i, t in enumerate(stamps) if window.startMs <= t and t+step <= window.endMs]
    cash, last_occupied = policy.initialCapital, -1
    trades, attempts = [], []
    rate = policy.commissionPctPerSide/100
    for off, i in enumerate(indices):
        events = [(s, c) for s, c in ((1, commands.long), (-1, commands.short)) if c.entryKnown[i] and c.entry[i]]
        eligible = []
        reasons = {}
        for sign, c in events:
            f = commands.entryFilter
            why = ("directionDisabled" if policy.direction not in ("both", "long" if sign == 1 else "short") else
                   "filterUnknown" if f is not None and not f.known[i] else
                   "filterBlocked" if f is not None and not (f.allowLong[i] if sign == 1 else f.allowShort[i]) else None)
            reasons[sign] = why
            if why is None:
                eligible.append(sign)
        for sign, c in events:
            why = reasons[sign]
            if why is None:
                why = ("entryConflict" if len(eligible) == 2 else
                       "noNextBar" if off+1 == len(indices) or stamps[indices[off+1]] != stamps[i]+step else
                       "occupied" if i < last_occupied else "reentrySuppressed" if i == last_occupied else None)
            stop = float(c.initialStop[i])
            distance = sign*(float(bars[i, 3])-stop)
            if why is None:
                why = ("stopUnavailable" if not c.initialStopKnown[i] else "invalidRiskDistance" if stop <= 0 or distance <= 0 else
                       "nonpositiveCapital" if cash <= 0 else None)
            if why:
                attempts.append((i, sign, why))
                continue
            quantity = math.floor((cash*policy.riskPct/100/distance)/float(rules.base_step))*float(rules.base_step)
            if quantity == 0 or round(quantity/float(rules.base_step)) < rules.minimum_lots:
                attempts.append((i, sign, "zeroQuantity" if quantity == 0 else "belowMinQuantity"))
                continue
            entry = i+1
            ep = float(bars[entry, 0])
            notional = abs(ep*quantity)
            fee = notional*rate
            lev = notional/(cash-fee) if cash-fee > 0 else math.nan
            why = ("belowMinNotional" if rules.minimum_notional is not None and notional < float(rules.minimum_notional) else
                   "leverageUndefined" if not math.isfinite(lev) else "leverageCapExceeded" if lev > policy.maxLeverage else "filled")
            attempts.append((i, sign, why))
            if why != "filled":
                continue
            active, pending = stop, False
            for exoff in range(off+1, len(indices)):
                x = indices[exoff]
                op, hi, lo, cl = map(float, bars[x, :4])
                if sign*(op-active) <= 0:
                    price, reason, phase = op, "stop", "open"
                elif pending:
                    price, reason, phase = op, "explicitExit", "open"
                elif (lo <= active if sign == 1 else hi >= active):
                    price, reason, phase = active, "stop", "intrabar"
                elif exoff+1 == len(indices) or stamps[indices[exoff+1]] != stamps[x]+step:
                    price, reason, phase = cl, "terminal" if exoff+1 == len(indices) else "gapBoundary", "close"
                else:
                    pending = bool(c.exitKnown[x] and c.exit[x])
                    candidate = float(c.proposedStop[x])
                    if policy.stopUpdates and c.proposedStopKnown[x] and candidate > 0:
                        active = max(active, candidate) if sign == 1 else min(active, candidate)
                    continue
                gross = sign*(price-ep)*quantity
                ef = price*quantity*rate
                trades.append(dict(side=sign, signalIndex=i, entryIndex=entry, exitIndex=x, entryPrice=ep,
                    exitPrice=price, exitReason=reason, exitPhase=phase, quantity=quantity,
                    entryFee=fee, exitFee=ef, grossPnl=gross, netPnl=gross-fee-ef))
                cash = cash-fee+gross-ef
                last_occupied = x
                break
    # Independent cashflow scan, deliberately separate from forward trade search.
    cash, path = policy.initialCapital, []
    for i in indices:
        q, ep, sign = 0, 0, 0
        for t in trades:
            if t["entryIndex"] == i:
                cash -= t["entryFee"]
            if t["exitIndex"] == i:
                cash += t["grossPnl"]-t["exitFee"]
            if t["entryIndex"] <= i < t["exitIndex"]:
                q, ep, sign = t["quantity"], t["entryPrice"], t["side"]
        path.append((cash, cash+sign*(float(bars[i, 3])-ep)*q))
    return trades, attempts, path
