"""Exact slot reductions, anchored metrics and compact display curves."""
import hashlib
import math
import struct

import numpy as np
import pandas as pd

from ..selection.ranking import require

BUDGET = 256 * 1024**2
DENSE_VERSION = "strategy_lab_portfolio_dense_curve_v1"
CURVE_POLICY = "strategy_lab_portfolio_buckets_v1"


def finite_sum(values):
    values = list(values)
    require(all(math.isfinite(v) for v in values), "nonfinite accounting atom.")
    try:
        value = math.fsum(values)
    except OverflowError as exc:
        raise ValueError('nonfinite accounting sum.') from exc
    require(math.isfinite(value), "nonfinite accounting sum.")
    return value


def preflight(windows, groups, slots, minutes):
    step = minutes * 60 * 10**9
    counts = {w['id']: (pd.Timestamp(w['end']).value-pd.Timestamp(w['start']).value)//step for w in windows}
    # Aggregate, metric/digest/sampling copies, period slot paths and reduction workspace.
    estimate = (sum(counts.values())+1)*80 + max(sum(counts[i] for i in g['windows']) for g in groups)*(16*slots+32)
    require(estimate <= BUDGET, f"dense accounting estimate {estimate} exceeds 256 MiB budget.")
    return estimate


def dense_digest(ts, balance, equity):
    validate_path(ts, balance, equity)
    h = hashlib.sha256((DENSE_VERSION+'\n').encode('ascii')+struct.pack('<Q', len(ts)))
    for column, dtype in ((ts, '<i8'), (balance, '<f8'), (equity, '<f8')):
        h.update(np.asarray(column, dtype=dtype).tobytes(order='C'))
    return h.hexdigest()


def validate_path(ts, balance, equity):
    require(len(ts) > 1 and len(ts) == len(balance) == len(equity), "curve shape mismatch.")
    require(np.all(np.diff(ts) > 0) and np.all(np.isfinite(balance)) and np.all(np.isfinite(equity)),
        "curve must have ordered unique timestamps and finite amounts.")


def witness(ts, values):
    peak = 0
    best, pair = 0., (0, 0)
    for i, v in enumerate(values):
        if v > values[peak]:
            peak = i
        elif values[peak] > 0:
            dd = (float(values[peak])-float(v))/float(values[peak])*100.
            if dd > best:
                best, pair = dd, (peak, i)
    return dict(percent=best, peak=int(ts[pair[0]]), trough=int(ts[pair[1]]))


def metrics(ts, balance, equity, initial):
    from core.engine_v2.metrics_kernel import _max_drawdowns
    from core.engine_v2.runner import _max_drawdown_mtm_from_equity
    validate_path(ts, balance, equity)
    require(balance[0] == equity[0] == initial and balance[-1] == equity[-1], "curve anchor/strict close mismatch.")
    bw, ew = witness(ts, balance), witness(ts, equity)
    pct, amount = _max_drawdowns(balance)
    mtm = _max_drawdown_mtm_from_equity(equity, 0, initial)
    require(bw['percent'] == pct and ew['percent'] == mtm, "drawdown witness disagrees with core.")
    final = float(balance[-1])
    return dict(initial=initial, final=final, profit=final-initial, return_pct=100*(final/initial-1),
        balance_dd_pct=pct, balance_dd_amount=amount, mtm_dd_pct=mtm, balance_witness=bw, mtm_witness=ew)


def compact(ts, balance, equity, buckets, mandatory):
    validate_path(ts, balance, equity)
    first, end = int(ts[0]), int(ts[-1])
    require(end > first and type(buckets) is int and 1 <= buckets <= 4096, "invalid curve bucket policy.")
    ordinary = set()
    groups = {}
    for i, t in enumerate(ts):
        b = min(buckets-1, (int(t)-first)*buckets//(end-first))
        points = groups.setdefault(b, [i]*6)
        points[1] = i
        for values, low, high in ((balance, 2, 3), (equity, 4, 5)):
            if values[i] < values[points[low]]:
                points[low] = i
            if values[i] > values[points[high]]:
                points[high] = i
    for indices in groups.values():
        ordinary.update(indices)
    required = {first, end, *mandatory}
    required_indices = {int(np.searchsorted(ts, t)) for t in required}
    require(all(i < len(ts) for i in required_indices) and {int(ts[i]) for i in required_indices} == required,
        "mandatory curve timestamp absent.")
    chosen = sorted(ordinary | required_indices)
    return chosen, dict(version=CURVE_POLICY, buckets=buckets, native_count=len(ts), saved_count=len(chosen),
        ordinary_count=len(ordinary), mandatory_count=len(required), additional_mandatory_count=len(chosen)-len(ordinary))


def reconcile_trades(opening, closing, trades):
    pnl = [t['net_pnl'] for t in trades]
    total = finite_sum(pnl)
    tolerance = 1e-10 + 1e-12*max(abs(opening), abs(closing), finite_sum(abs(v) for v in pnl))
    require(abs((closing-opening)-total) <= tolerance, "trade P&L contradicts account balance.")
    return total
