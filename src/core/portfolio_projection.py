"""Offline projections of immutable account-period realized balances."""
from bisect import bisect_right
import math
from statistics import median

import pandas as pd

from .analytics import _annualize_profit, _compute_max_drawdown


def balance_path(result, start, end, opening, minutes):
    """Keep every per-bar change plus logical anchors; observations are bar closes."""
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    points = {start: float(opening)}
    last = float(opening)
    step = pd.Timedelta(minutes=minutes)
    for stamp, value in zip(result.timestamps, result.balance_curve):
        stamp = pd.Timestamp(stamp)
        if stamp < start or stamp >= end:
            continue
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("Nonfinite account balance.")
        if value != last:
            points[min(stamp + step, end)] = value
        last = value
    points[end] = last
    return [[stamp.isoformat(), value] for stamp, value in sorted(points.items())]


def _value(path, stamp):
    timestamps = [pd.Timestamp(p[0]).value for p in path]
    return path[max(0, bisect_right(timestamps, pd.Timestamp(stamp).value) - 1)][1]


def resolve_atoms(portfolio, selection=None):
    """Selection is an explicit union of ranks, periods, and account IDs."""
    accounts = {a["id"]: a for p in portfolio["periods"] for a in p.get("accounts", [])}
    if selection is None:
        return [a for a in accounts.values() if a["role"] in ("funded", "idle") and a.get("path")]
    if not isinstance(selection, dict) or set(selection) - {"ranks", "periods", "accounts"}:
        raise ValueError("Invalid portfolio selection.")
    chosen = set()
    for rank in selection.get("ranks", []):
        if type(rank) is not int or not 1 <= rank <= portfolio["selector"]["topN"]:
            raise ValueError("Invalid portfolio rank.")
        chosen.update(a["id"] for a in accounts.values() if a["role"] in ("funded", "idle") and a["rank"] == rank and a.get("path"))
    for period in selection.get("periods", []):
        if type(period) is not int or period not in {p["id"] for p in portfolio["periods"]}:
            raise ValueError("Invalid portfolio period.")
        chosen.update(a["id"] for a in accounts.values() if a["period"] == period and a["role"] in ("funded", "idle") and a.get("path"))
    for identity in selection.get("accounts", []):
        if identity not in accounts or not accounts[identity].get("path"):
            raise ValueError("Account does not belong to this portfolio or has no saved execution.")
        chosen.add(identity)
    return [accounts[key] for key in sorted(chosen)]


def project(portfolio, selection=None):
    from .storage import _compute_cached_curve_consistency_scores
    atoms = resolve_atoms(portfolio, selection)
    empty = {"curve": [], "timestamps": [], "profit_pct": None, "ann_profit_pct": None,
             "max_drawdown_pct": None, "consistency_score": None, "consistency_score_full": None,
             "total_trades": 0, "winning_trades": 0, "wfe": None, "oos_win_rate": None,
             "oos_profit_median": None, "oos_wr_median": None, "entry_diagnostics": None,
             "profitable_pct": None, "depleted": False}
    if not atoms:
        return empty
    indexed = {a['id']: ([pd.Timestamp(p[0]).value for p in a['path']], [p[1] for p in a['path']]) for a in atoms}
    period_ids = {a["period"] for a in atoms}
    periods = [p for p in portfolio["periods"] if min(period_ids) <= p["id"] <= max(period_ids)]
    points, index, depleted = {}, 100.0, False
    window_returns, window_rates, account_windows = [], [], []
    for period in periods:
        selected = [a for a in atoms if a["period"] == period["id"]]
        if not selected or depleted:
            points[period["start"]] = index
            points[period["end"]] = index
            continue
        opening = math.fsum(a["opening"] for a in selected)
        if not math.isfinite(opening) or opening <= 0:
            raise ValueError("Invalid saved portfolio allocation.")
        times = sorted({period["start"], period["end"]} | {p[0] for a in selected for p in a["path"]})
        local = {}
        for stamp in times:
            instant = pd.Timestamp(stamp).value
            balances = (values[max(0, bisect_right(stamps, instant)-1)] for stamps,values in (indexed[a['id']] for a in selected))
            value = index if depleted else index * math.fsum(balances) / opening
            if not math.isfinite(value):
                raise ValueError("Nonfinite portfolio projection.")
            local[stamp] = value
            if value <= 0:
                depleted = True
                index = value
        points.update(local)
        index = local[times[-1]]
        if portfolio.get("wfa"):
            for window in period["windows"]:
                facts = [w for a in selected for w in a.get("windows", []) if w["id"] == window["id"]]
                if not facts:
                    continue
                account_windows.extend(facts)
                left = _value(list(local.items()), window["start"])
                right = _value(list(local.items()), window["end"])
                if left > 0:
                    window_returns.append(100 * (right / left - 1))
                trades = sum(w["trades"] for w in facts)
                if trades:
                    window_rates.append(100 * sum(w["wins"] for w in facts) / trades)
    timestamps = sorted(points)
    curve = [points[t] for t in timestamps]
    profit = 100 * (curve[-1] / 100 - 1)
    duration = (pd.Timestamp(timestamps[-1]) - pd.Timestamp(timestamps[0])).total_seconds() / 86400
    full, recent = _compute_cached_curve_consistency_scores(curve, timestamps)
    executed = [a for a in atoms if a["role"] != "idle"]
    diagnostics = [a.get("diagnostics") for a in executed]
    diagnostic = None
    if diagnostics and all(d is not None for d in diagnostics):
        maxima = [d["maxEntryLeverageUsed"] for d in diagnostics if d["maxEntryLeverageUsed"] is not None]
        diagnostic = {"version": 1, "scope": "oos", "maxEntryLeverageUsed": max(maxima) if maxima else None,
                      "leverageCapSkippedEntries": sum(d["leverageCapSkippedEntries"] for d in diagnostics)}
    wfe = None
    if account_windows:
        is_mean = math.fsum(w["isProfit"] for w in account_windows) / len(account_windows) * 12 / portfolio["wfa"]["is_period_months"]
        oos_mean = math.fsum(w["profit"] for w in account_windows) / len(account_windows) * 12 / portfolio["wfa"]["oos_period_months"]
        wfe = 100 * oos_mean / is_mean if is_mean != 0 else (0.0 if oos_mean <= 0 else 100.0)
    return {**empty, "curve": curve, "timestamps": timestamps, "profit_pct": None if depleted else profit,
            "ann_profit_pct": None if depleted else _annualize_profit(profit, duration),
            "max_drawdown_pct": None if depleted else _compute_max_drawdown(curve),
            "consistency_score": None if depleted else recent, "consistency_score_full": None if depleted else full,
            "overlap_days_exact": duration, "total_trades": sum(a.get("trades", 0) for a in executed),
            "winning_trades": sum(a.get("wins", 0) for a in executed), "wfe": None if depleted else wfe,
            "oos_win_rate": 100 * sum(r > 0 for r in window_returns) / len(window_returns) if window_returns and not depleted else None,
            "oos_profit_median": median(window_returns) if window_returns and not depleted else None,
            "oos_wr_median": median(window_rates) if window_rates else None,
            "oos_window_count": len(window_returns), "oos_positive_windows": sum(r > 0 for r in window_returns),
            "profitable_pct": 100 * sum(a["closing"] > a["opening"] for a in executed) / len(executed) if executed else None,
            "entry_diagnostics": diagnostic, "depleted": depleted,
            "warning": "Depleted return index" if depleted else None}
