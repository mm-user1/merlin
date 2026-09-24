"""Saved bracket-v1 consistency checks, with one indexed sweep per account.

These checks use retained facts, not an execution replay. Entry arithmetic is
the exact v1 contract; accumulated money retains abs=1e-8 / rel=1e-9 tolerance.
"""
from decimal import Decimal, InvalidOperation
import math

import numpy as np
import pyarrow as pa

from .. import PatternLabDataError
from . import contracts

PLAN = ("anchor_price", "stop", "target", "risk_distance", "quantity", "lots")
FILL = ("fill_index", "notional", "proposed_fee", "required_leverage")
EARLY = ("no_next_bar", "occupied", "reentry_suppressed", "indicator_unavailable",
         "invalid_risk_distance", "nonpositive_capital")
SIZING = ("zero_quantity", "below_min_quantity")
FILL_REASONS = ("below_min_notional", "leverage_undefined", "leverage_cap_exceeded", "filled")


def _same(left, right):
    return left is not None and right is not None and math.isclose(left, right, abs_tol=1e-8, rel_tol=1e-9)


def _same_columns(left, right):
    # Preserve math.isclose's symmetric maximum, not numpy.allclose's sum.
    tolerance = np.maximum(1e-8, 1e-9*np.maximum(np.abs(left), np.abs(right)))
    return np.all(np.isfinite(right) & (np.abs(left-right) <= tolerance))


def _require(ok, where, message):
    if not ok:
        raise PatternLabDataError(f"{where}: sequential evidence: {message}")


def _group_tables(tables, accounts):
    from .sequential import KEY, SCHEMAS, checked_table

    grouped = {name: {key: [] for key in accounts} for name in SCHEMAS}
    indices = {}
    for name in SCHEMAS:
        _require(name in tables, name, "missing table")
        table = checked_table(tables[name], name)
        locations = tables[name].groupby(list(KEY), sort=False, observed=True, dropna=False).indices
        _require(set(locations).issubset(accounts), name, "unknown account")
        indices[name] = locations
        if name == "path":
            # Numeric columns only: never materialize the high-volume path as rows.
            columns = {field.name: table[field.name].to_numpy(zero_copy_only=False)
                       for field in SCHEMAS[name] if field.name not in KEY}
            for key in accounts:
                rows = locations.get(key, np.empty(0, dtype=np.int64))
                # Slice contiguous account blocks without allocating another copy.
                selection = slice(int(rows[0]), int(rows[-1])+1) if len(rows) and rows[-1]-rows[0]+1 == len(rows) else rows
                grouped[name][key] = {column: values[selection] for column, values in columns.items()}
        else:
            for row in table.to_pylist():
                grouped[name][tuple(row[k] for k in KEY)].append(row)
    grouped["indices"] = indices
    return grouped


def _path_index(path, expected, step, where):
    bars, stamps = path["bar_index"], path["bar_open_ms"]
    _require(np.array_equal(stamps, expected), where, "missing/unordered account path bars")
    _require(not len(bars) or (bars[0] >= 0 and np.all(np.diff(bars) == 1)), where, "path index chronology")
    boundaries = np.r_[np.diff(stamps) != step, True] if len(bars) else np.array([], dtype=bool)
    _require(np.array_equal(path["segment_end"], boundaries), where, "path boundary flags")
    study_end = np.zeros(len(bars), dtype=bool)
    if len(bars): study_end[-1] = True
    _require(np.array_equal(path["study_end"], study_end), where, "path final-observed flags")
    _require(not len(bars) or (path["segment"][0] >= 0 and
        np.array_equal(np.diff(path["segment"]), boundaries[:-1].astype(np.int64))), where, "path segment chronology")
    ends = np.flatnonzero(boundaries)
    segment_last = np.repeat(ends, np.diff(np.r_[-1, ends]))
    return segment_last


def _position(path, index, where):
    bars = path["bar_index"]
    _require(len(bars) and bars[0] <= index <= bars[-1], where, "bar index outside account path")
    return int(index - bars[0])


def _trade_intervals(trades, path, where):
    """Build inclusive occupancy and close-position maps once, after chronology checks."""
    length = len(path["bar_index"])
    had_position = np.zeros(length, dtype=bool)
    active = np.full(length, -1, dtype=np.int64)
    entries, exits = {}, {}
    previous_exit = -1
    for number, trade in enumerate(trades):
        label = f"{where} trade {trade['trade_id']}"
        entry = _position(path, trade["entry_index"], label)
        exit_ = _position(path, trade["exit_index"], label)
        _require(entry <= exit_ and entry > previous_exit, label, "unordered/overlapping trades")
        _require(path["segment"][entry] == path["segment"][exit_], label, "trade crosses a gap")
        entries[entry], exits[exit_] = trade, trade
        had_position[entry:exit_+1] = True
        active[entry:exit_] = number
        previous_exit = exit_
    return had_position, active, entries, exits


def _fill_decision(attempt, settings, rules, where):
    notional, fee = attempt["notional"], attempt["proposed_fee"]
    if notional is None:
        _require(fee is None, where, "null notional requires null proposed fee")
        return "leverage_undefined", None
    expected_fee = notional * (settings["commission_pct_per_side"] / 100)
    _require(fee == expected_fee, where, "proposed fee differs from v1 notional formula")
    _require(notional >= 0, where, "negative notional")
    denominator = attempt["pre_entry_balance"] - expected_fee
    leverage = notional / denominator if denominator > 0 else math.nan
    finite = all(math.isfinite(x) for x in (notional, expected_fee, denominator))
    finite = finite and (denominator <= 0 or math.isfinite(leverage))
    saved_ratio = leverage if math.isfinite(leverage) else None
    if not finite:
        return "leverage_undefined", saved_ratio
    if rules["minimum_notional"] is not None and notional < float(rules["minimum_notional"]):
        return "below_min_notional", saved_ratio
    if not math.isfinite(leverage):
        return "leverage_undefined", None
    return ("leverage_cap_exceeded" if leverage > settings["max_leverage"] else "filled"), leverage


def _check_attempt(attempt, path, had_position, settings, rules, expected, by_trade, step, where):
    from .sequential import REASONS

    a = attempt
    where = f"{where} event {a['event_id']}"
    reason = a["reason"]
    _require(reason in REASONS, where, "unknown terminal reason")
    signal = _position(path, a["signal_index"], where)
    _require(a["anchor_open_ms"] == expected[a["event_id"]] == path["bar_open_ms"][signal]
        and a["intended_fill_ms"] == a["anchor_open_ms"]+step, where, "attempt event timing")
    previous_open = signal > 0 and path["segment"][signal-1] == path["segment"][signal] and path["position_direction"][signal-1] != 0
    priority = ("no_next_bar" if path["segment_end"][signal] else
                "occupied" if path["position_direction"][signal] else
                "reentry_suppressed" if had_position[signal] or previous_open else None)
    _require(reason == priority if priority else reason not in EARLY[:3], where, "signal disposition priority")
    _require(_same(a["pre_entry_balance"], path["balance"][signal]), where, "attempt capital differs from signal close")
    if reason in EARLY:
        _require(all(a[field] is None for field in (*PLAN, *FILL, "trade_id")), where, "early refusal carries plan/fill facts")
        if reason == "nonpositive_capital":
            _require(a["pre_entry_balance"] <= 0, where, "nonpositive capital disposition")
        return
    _require(all(a[field] is not None for field in PLAN), where, "missing complete plan")
    _require(a["pre_entry_balance"] > 0, where, "sizing/fill requires positive capital")
    lots, quantity = a["lots"], a["quantity"]
    _require(0 <= lots <= 2**53 and _same(quantity, lots*float(rules["base_step"])), where, "integer lot reconstruction")
    _require(quantity >= 0 and a["risk_distance"] > 0, where, "invalid planned quantity/distance")
    _require(a["anchor_price"] == path["close_price"][signal], where, "anchor differs from signal close")
    sign = 1 if settings["direction"] == "long" else -1
    _require(_same(a["risk_distance"], sign*(a["anchor_price"]-a["stop"])) and
        _same(a["target"], a["anchor_price"]+sign*settings["reward_risk"]*a["risk_distance"]), where, "planned levels")
    if reason in SIZING:
        _require(all(a[field] is None for field in (*FILL, "trade_id")), where, "sizing refusal carries fill facts")
        valid = lots == 0 and quantity == 0 if reason == "zero_quantity" else 0 < lots < rules["minimum_lots"] and quantity > 0
        _require(valid, where, "sizing disposition disagrees with lots")
        return
    _require(lots >= rules["minimum_lots"] and quantity > 0, where, "fill below minimum lots")
    _require(a["fill_index"] is not None, where, "missing fill index")
    fill = _position(path, a["fill_index"], where)
    _require(fill == signal+1 and path["segment"][fill] == path["segment"][signal]
        and path["bar_open_ms"][fill] == a["intended_fill_ms"], where, "noncontiguous fill")
    derived_reason, leverage = _fill_decision(a, settings, rules, where)
    _require(reason == derived_reason, where, "fill disposition contradicts recomputed admission")
    _require(a["required_leverage"] == leverage, where, "required leverage differs from exact v1 arithmetic")
    if reason != "filled":
        _require(a["trade_id"] is None, where, "rejected attempt links a trade")
        return
    trade = by_trade[a["trade_id"]]
    _require(trade["event_id"] == a["event_id"] and trade["signal_index"] == a["signal_index"]
        and trade["entry_index"] == a["fill_index"], where, "trade origin link")
    _require(all(trade[k] == a[k] for k in ("quantity", "lots", "stop", "target")), where, "trade plan changed")
    _require(a["notional"] == abs(trade["entry_price"]*trade["quantity"]), where, "attempt notional contradicts executed entry")
    _require(a["proposed_fee"] == trade["entry_fee"], where, "attempt fee contradicts executed entry")
    _require(trade["entry_leverage"] == leverage, where, "trade leverage differs from recomputed entry")
    _require(_same(trade["planned_cash_risk"], quantity*a["risk_distance"]), where, "planned risk")


def _expiry_open(stamps, entry, last, days):
    """First trigger by the reference floating predicate, in logarithmic work."""
    low, high = entry, last+1
    while low < high:
        middle = (low+high)//2
        elapsed_seconds = int(stamps[middle]-stamps[entry])/1000.0
        if elapsed_seconds/86400.0 >= days:
            high = middle
        else:
            low = middle+1
    return low+1 if low < last else None


def _check_trade(trade, path, segment_last, settings, rules, step, where):
    t = trade
    where = f"{where} trade {t['trade_id']}"
    entry = _position(path, t["entry_index"], where)
    exit_ = _position(path, t["exit_index"], where)
    stamps = path["bar_open_ms"]
    _require(t["direction"] == settings["direction"], where, "trade direction")
    _require(t["entry_time_ms"] == stamps[entry] and t["exit_bar_open_ms"] == stamps[exit_]
        and t["exit_bar_end_ms"] == stamps[exit_]+step, where, "trade times")
    reason, phase = t["exit_reason"], t["exit_phase"]
    phases = {"stop": ("open", "intrabar"), "target": ("open", "intrabar"), "expiry": ("open",),
              "terminal": ("close",), "gap_boundary": ("close",)}
    _require(reason in phases and phase in phases[reason], where, "exit reason/phase")
    _require(t["holding_ms"] == t["exit_bar_open_ms"]+(step if phase == "close" else 0)-t["entry_time_ms"], where, "duration clock")
    if reason in ("stop", "target"):
        level = t[reason]
        through = (t["exit_price"] <= level) if (reason == "stop") == (t["direction"] == "long") else (t["exit_price"] >= level)
        _require(t["exit_price"] == level if phase == "intrabar" else through, where, "exit price contradicts level/phase")
    if reason in ("terminal", "gap_boundary"):
        _require(path["segment_end"][exit_] and (reason == "terminal") == path["study_end"][exit_], where, "boundary attribution")
        _require(t["exit_price"] == path["close_price"][exit_], where, "boundary price differs from saved close")
    expiry = _expiry_open(stamps, entry, int(segment_last[entry]), settings["max_holding_days"])
    _require(reason != "expiry" if expiry is None else exit_ <= expiry and ((exit_ == expiry) == (reason == "expiry")), where, "expiry clock/precedence")
    _require(t["quantity"] > 0 and t["planned_cash_risk"] > 0, where, "nonpositive trade size/risk")
    try:
        unit_quantity = Decimal(t["instrument_quantity"])
        units_valid = unit_quantity.is_finite() and unit_quantity == Decimal(t["lots"])*Decimal(rules["quantity_step"])
    except (InvalidOperation, TypeError, ValueError):
        units_valid = False
    _require(units_valid, where, "instrument quantity units")
    rate = settings["commission_pct_per_side"]/100
    gross = (t["exit_price"]-t["entry_price"])*t["quantity"]*(1 if t["direction"] == "long" else -1)
    _require(_same(t["gross_pnl"], gross) and _same(t["entry_fee"], t["entry_price"]*t["quantity"]*rate)
        and _same(t["exit_fee"], t["exit_price"]*t["quantity"]*rate), where, "PnL/fee contradiction")
    _require(_same(t["net_pnl"], gross-t["entry_fee"]-t["exit_fee"])
        and _same(t["net_r"], t["net_pnl"]/t["planned_cash_risk"]), where, "net PnL/R contradiction")


def _reconcile(path, trades, active, entries, exits, settings, where):
    """One chronological cash sweep; inclusive occupancy is a separate index."""
    length = len(active)
    balances = np.empty(length)
    balance = settings["initial_capital"]
    for index in range(length):
        if index in entries: balance -= entries[index]["entry_fee"]
        if index in exits: balance += exits[index]["gross_pnl"]-exits[index]["exit_fee"]
        balances[index] = balance
    occupied = active >= 0
    quantity = np.zeros(length)
    entry_price = np.full(length, np.nan)
    if trades:
        quantity[occupied] = np.array([t["quantity"] for t in trades])[active[occupied]]
        entry_price[occupied] = np.array([t["entry_price"] for t in trades])[active[occupied]]
    direction = occupied.astype(np.int64)*(1 if settings["direction"] == "long" else -1)
    unrealized = np.zeros(length)
    unrealized[occupied] = (path["close_price"][occupied]-entry_price[occupied])*quantity[occupied]*direction[occupied]
    _require(_same_columns(path["balance"], balances)
        and _same_columns(path["equity"], balances+unrealized), where, "account balance/equity contradiction")
    _require(np.array_equal(path["position_direction"], direction) and
        _same_columns(path["position_quantity"], quantity), where, "account position contradiction")
    _require(np.array_equal(path["entry_price"], entry_price, equal_nan=True), where, "account entry price")
    _require(not np.any(path["segment_end"] & occupied), where, "position carried across boundary")


def validate(tables, *, instrument_id, instances, variants, emissions, expected_bars, rules):
    accounts = {}
    for instance in instances:
        _require(contracts.is_sequential(instance), instrument_id, "unsupported model contract")
        for timeframe, cases in instance["cases"].items():
            for variant in variants:
                for case in cases:
                    key = (instrument_id, int(timeframe), variant["variant_id"], instance["model_instance_id"], case["case_id"])
                    _require(key not in accounts, str(key), "duplicate account declaration")
                    accounts[key] = case["parameters"]
    grouped = _group_tables(tables, accounts)
    events = {}
    for row in emissions.itertuples():
        expected = events.setdefault((int(row.timeframe_minutes), row.variant_id), {})
        _require(row.event_id not in expected, instrument_id, "duplicate emitted event")
        expected[row.event_id] = int(row.anchor_open_ms)
    for key, settings in accounts.items():
        where = " / ".join(map(str, key))
        attempts, trades, path = (grouped[name][key] for name in ("attempts", "trades", "path"))
        expected = events.get((key[1], key[2]), {})
        _require(len(attempts) == len(expected) and {a["event_id"] for a in attempts} == set(expected), where, "duplicate/missing attempts")
        trade_ids = [t["trade_id"] for t in trades]
        _require(len(set(trade_ids)) == len(trades), where, "duplicate trades")
        by_trade = dict(zip(trade_ids, trades))
        filled_ids = [a["trade_id"] for a in attempts if a["reason"] == "filled"]
        _require(len(filled_ids) == len(trades) and set(filled_ids) == set(by_trade), where, "filled/trade links disagree")
        step = key[1]*60000
        segment_last = _path_index(path, expected_bars[key[1]], step, where)
        had, active, entries, exits = _trade_intervals(trades, path, where)
        for attempt in attempts:
            _check_attempt(attempt, path, had, settings, rules, expected, by_trade, step, where)
        for trade in trades:
            _check_trade(trade, path, segment_last, settings, rules, step, where)
        _reconcile(path, trades, active, entries, exits, settings, where)
    return grouped
