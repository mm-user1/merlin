"""ATR bracket declaration and thin, lazy generic-reference adapter."""
from decimal import Decimal
import importlib
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from . import contracts

DEFAULTS = dict(directions=["long", "short"], reward_risks=[1., 2., 3.], atr_length=14,
                swing_lookback=2, atr_multiplier=2., initial_capital=10000., risk_pct=2.,
                commission_pct_per_side=.05, max_leverage=8., max_holding_days=4.)
FIXED = dict(trailing="none", maximum_stop_width_pct=None, price_rounding="none",
             entry_cap_action="reject", boundary="strict_close", slippage=False, funding=False)


def validate_settings(raw, timeframes):
    contracts.closed_keys(raw, DEFAULTS, "atr_bracket.settings")
    values = {**DEFAULTS, **raw}
    directions = values["directions"]
    if not isinstance(directions, (list, tuple)) or not directions or any(x not in ("long", "short") for x in directions) or len(set(directions)) != len(directions):
        raise PatternLabDataError("atr_bracket.directions: expected a nonempty unique long/short subset")
    values["directions"] = [x for x in ("long", "short") if x in directions]
    rr = values["reward_risks"]
    if not isinstance(rr, (list, tuple)) or not rr:
        raise PatternLabDataError("atr_bracket.reward_risks: expected a nonempty list")
    def number(value, key, zero=False):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or (value < 0 if zero else value <= 0):
            raise PatternLabDataError(f"atr_bracket.{key}: expected a finite {'nonnegative' if zero else 'positive'} number")
        return float(value)
    rr = [number(x, "reward_risks") for x in rr]
    if len(set(rr)) != len(rr):
        raise PatternLabDataError("atr_bracket.reward_risks: duplicate semantic case")
    values["reward_risks"] = sorted(rr)
    for key in ("atr_length", "swing_lookback"):
        values[key] = contracts.require_int(values[key], key, minimum=1)
    for key in DEFAULTS.keys() - {"directions", "reward_risks", "atr_length", "swing_lookback"}:
        values[key] = number(values[key], key, key == "commission_pct_per_side")
    if values["risk_pct"] > 100 or values["commission_pct_per_side"] >= 100:
        raise PatternLabDataError("atr_bracket: risk_pct must be <=100 and commission_pct_per_side <100")
    return values


def prior_bars(settings):
    return max(settings["atr_length"] - 1, settings["swing_lookback"] - 1)


def resolve_cases(settings, timeframe):
    return tuple(contracts.ModelCase(
        f"{direction}_rr{format(Decimal(str(rr)), 'f').rstrip('0').rstrip('.') if rr % 1 else int(rr)}",
        timeframe, {**settings, **FIXED, "direction": direction, "reward_risk": rr}, ())
        for direction in settings["directions"] for rr in settings["reward_risks"])


def pine_atr(high, low, close, length):
    """Pine arithmetic seed and recurrence; call separately on contiguous segments."""
    previous_close = np.roll(close, 1).astype(np.float64)
    if len(previous_close):
        previous_close[0] = np.nan
    tr = np.maximum.reduce([np.abs(high-low), np.abs(high-previous_close), np.abs(low-previous_close)])
    if len(tr):
        tr[0] = abs(high[0]-low[0])
    result = np.full(len(tr), np.nan)
    seed, previous = [], math.nan
    for i, raw in enumerate(tr):
        value = float(raw)
        if not math.isfinite(value):
            continue
        if not math.isfinite(previous):
            seed.append(value)
            if len(seed) == length:
                previous = float(sum(seed)/length)
                result[i] = previous
            continue
        previous = (previous * (length-1) + value)/length
        result[i] = previous
    return result


def reference_core():
    """Import the repository core only at execution, restoring the caller's path."""
    src = Path(__file__).resolve().parents[3] / "src"
    expected = src / "core"
    def check():
        for name, module in tuple(sys.modules.items()):
            if name == "core" or name.startswith("core."):
                origin = getattr(module, "__file__", None)
                if origin is None or expected not in Path(origin).resolve().parents:
                    raise PatternLabDataError(f"Foreign preloaded core module {name}: {origin}")
    check()
    original = list(sys.path)
    try:
        sys.path.insert(0, str(src))
        kernel = importlib.import_module("core.engine_v2.kernel")
        check()
        return kernel
    finally:
        sys.path[:] = original


def evaluate(series, events, variants, instance, bounds, rules):
    """Prepare indicators once, then run isolated variant/case accounts in RAM."""
    from . import sequential
    kernel = reference_core()
    settings = instance["settings"]
    stamps, step = series.timestamps_ms, series.step_ms
    starts = np.r_[0, np.flatnonzero(np.diff(stamps) != step)+1]
    segments = []
    for segment, (first, end) in enumerate(zip(starts, np.r_[starts[1:], len(stamps)])):
        first, end = int(first), int(end)
        values = series.values[first:end]
        atr = pine_atr(values[:,1], values[:,2], values[:,3], settings["atr_length"])
        swing = settings["swing_lookback"]
        low = pd.Series(values[:,2]).rolling(swing, min_periods=swing).min().to_numpy()
        high = pd.Series(values[:,1]).rolling(swing, min_periods=swing).max().to_numpy()
        segments.append((segment, first, end, values, atr, low, high))
    rows = {name: [] for name in sequential.SCHEMAS}
    for variant in variants:
        selected = events.loc[events.variant_id == variant["variant_id"]]
        by_stamp = {int(row.anchor_open_ms): row.event_id for row in selected.itertuples()}
        if len(by_stamp) != len(selected) or not set(by_stamp).issubset(set(map(int, stamps))):
            raise PatternLabDataError("Sequential events have duplicate or unknown anchors")
        for case in resolve_cases(settings, series.timeframe_minutes):
            common = dict(instrument_id=series.instrument_id, timeframe_minutes=series.timeframe_minutes,
                          variant_id=variant["variant_id"], model_instance_id=instance["model_instance_id"], case_id=case.case_id)
            balance, trade_count = settings["initial_capital"], 0
            direction = case.parameters["direction"]
            for segment, first, end, values, atr, low, high in segments:
                local_stamps = stamps[first:end]
                signals = np.array([int(t) in by_stamp for t in local_stamps], dtype=bool)
                empty = np.zeros(len(values), dtype=bool)
                trace = kernel.KernelTrace()
                data = kernel.ExecutionData(pd.to_datetime(local_stamps, unit="ms", utc=True),
                    values[:,0], values[:,1], values[:,2], values[:,3],
                    kernel.Signals(signals if direction == "long" else empty, signals if direction == "short" else empty),
                    atr, low, high, np.full(len(values), np.nan), np.full(len(values), np.nan))
                config = kernel.KernelConfig(initial_capital=balance, commission_pct=settings["commission_pct_per_side"],
                    stop_x=settings["atr_multiplier"], reward_risk=case.parameters["reward_risk"],
                    max_days=settings["max_holding_days"], risk_per_trade_pct=settings["risk_pct"],
                    contract_size=float(rules.base_step), trade_start_idx=max(0, series.research_start_index-first),
                    start=pd.Timestamp(bounds[0], unit="ms", tz="UTC"), end=pd.Timestamp(bounds[1]-step, unit="ms", tz="UTC"))
                result = kernel.run_reference_kernel(data, config, policy=kernel.EntryPolicy(
                    settings["max_leverage"], rules.minimum_lots,
                    None if rules.minimum_notional is None else float(rules.minimum_notional)), trace=trace)
                attempts = {a.signal_index:a for a in trace.attempts}
                for a in trace.attempts:
                    anchor = int(local_stamps[a.signal_index])
                    rows["attempts"].append({**common, "event_id":by_stamp[anchor], "anchor_open_ms":anchor,
                        "signal_index":first+a.signal_index, "intended_fill_ms":anchor+step,
                        "fill_index":None if a.fill_index is None else first+a.fill_index,
                        "reason":a.reason, "anchor_price":a.anchor, "stop":a.stop, "target":a.target,
                        "risk_distance":a.risk_distance, "quantity":a.quantity, "lots":a.lots,
                        "pre_entry_balance":a.balance, "proposed_fee":a.proposed_fee,
                        "notional":a.notional, "required_leverage":a.required_leverage,
                        "trade_id":None if a.trade_index is None else str(trade_count+a.trade_index)})
                for index, (trade, exit_) in enumerate(zip(result.trades, trace.exits)):
                    a = attempts[exit_.signal_index]
                    risk_cash = a.quantity * a.risk_distance
                    reason = "gap_boundary" if exit_.reason == "terminal" and end < len(stamps) else exit_.reason
                    exit_bar = int(local_stamps[exit_.exit_index])
                    exit_clock = exit_bar + (step if exit_.phase == "close" else 0)
                    entry_time = int(local_stamps[exit_.entry_index])
                    rows["trades"].append({**common, "trade_id":str(trade_count+index), "event_id":by_stamp[int(local_stamps[a.signal_index])],
                        "direction":direction, "signal_index":first+a.signal_index,
                        "entry_index":first+exit_.entry_index, "exit_index":first+exit_.exit_index,
                        "entry_time_ms":entry_time, "exit_bar_open_ms":exit_bar, "exit_bar_end_ms":exit_bar+step,
                        "exit_phase":exit_.phase, "holding_ms":exit_clock-entry_time,
                        "entry_price":trade.entry_price, "exit_price":trade.exit_price,
                        "quantity":trade.size, "lots":a.lots,
                        "instrument_quantity":format(Decimal(a.lots)*Decimal(rules.quantity_step), "f"),
                        "stop":a.stop, "target":a.target, "planned_cash_risk":risk_cash,
                        "entry_fee":exit_.entry_fee, "exit_fee":exit_.exit_fee,
                        "gross_pnl":(trade.exit_price-trade.entry_price)*trade.size*(1 if direction == "long" else -1),
                        "net_pnl":trade.net_pnl, "net_r":trade.net_pnl/risk_cash,
                        "entry_leverage":a.required_leverage, "exit_reason":reason, "ambiguous":exit_.ambiguous})
                for i, (realized, equity, position) in enumerate(zip(result.balance_curve, result.equity_curve, trace.positions)):
                    if int(local_stamps[i]) < bounds[0]:
                        continue
                    rows["path"].append({**common, "bar_index":first+i, "bar_open_ms":int(local_stamps[i]),
                        "segment":segment, "segment_end":i == len(values)-1,
                        "study_end":first+i == len(stamps)-1, "close_price":float(values[i,3]),
                        "balance":realized, "equity":equity, "position_direction":position[0],
                        "position_quantity":position[1], "entry_price":position[2]})
                balance = result.balance_curve[-1]
                trade_count += len(result.trades)
    return contracts.SequentialEvidence({name:sequential.frame(name, data) for name,data in rows.items()})


DESCRIPTOR = contracts.SequentialModelDescriptor("atr_bracket", "1", validate_settings, resolve_cases,
    evaluate, prior_bars, description="Independent sequential ATR bracket accounts; descriptive, entry-capped reference simulation.")
