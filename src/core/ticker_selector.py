"""Causal price-only selection and data-independent portfolio calendars."""
from copy import deepcopy
import math

import pandas as pd

from data_provider import manifest
from .data_input import _timestamp, normalize_source, pack_session, timeframe_minutes


METHOD = {"id": "meanBarRangePct", "version": 1, "label": "X — Mean Candle Range",
          "parameters": {"lookbackMonths": {"default": 4, "min": 1},
                         "timeframeMinutes": {"default": 30, "min": 5, "step": 5}}}
DEFAULTS = {"version": 1, "enabled": False, "method": METHOD["id"],
            "methodVersion": 1, "topN": 8, "rebalanceMonths": 1,
            "evaluateUnselected": False,
            "params": {"lookbackMonths": 4, "timeframeMinutes": 30}}


def positive_integer(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 1 or int(value) != value:
        raise ValueError(f"{name} must be a positive whole number.")
    return int(value)


def normalize_selector(value=None):
    if value is None:
        return deepcopy(DEFAULTS)
    if not isinstance(value, dict) or type(value.get("enabled", False)) is not bool:
        raise ValueError("tickerSelector must be an object with a boolean enabled field.")
    if not value.get("enabled", False):
        return deepcopy(value)
    result = deepcopy(DEFAULTS)
    if set(value) - set(result):
        raise ValueError("Unknown active tickerSelector fields.")
    result.update(value)
    for field in ("version", "methodVersion"):
        if type(result[field]) is not int or result[field] != 1:
            raise ValueError(f"Unsupported tickerSelector {field}.")
    if result["method"] != METHOD["id"]:
        raise ValueError("Unsupported ticker selector method.")
    if type(result["evaluateUnselected"]) is not bool:
        raise ValueError("Evaluate Unselected must be boolean.")
    for field in ("topN", "rebalanceMonths"):
        result[field] = positive_integer(result[field], field)
    params = result["params"]
    if not isinstance(params, dict) or set(params) - set(DEFAULTS["params"]):
        raise ValueError("Unknown X parameters.")
    result["params"] = {**DEFAULTS["params"], **params}
    result["params"]["lookbackMonths"] = positive_integer(result["params"]["lookbackMonths"], "Lookback months")
    result["params"]["timeframeMinutes"] = timeframe_minutes(result["params"]["timeframeMinutes"])
    return result


def require_capability(strategy_config):
    from .engine_v2.profile import parse_execution_profile
    from .engine_v2.quantity import selector_capability
    if selector_capability(strategy_config) is None:
        raise ValueError("Ticker Selector requires a qualified V2 strict-close profile with declared initialCapital and instrument quantity support.")
    return parse_execution_profile(strategy_config)


def normalize_pool(sources):
    if not isinstance(sources, list) or not sources:
        raise ValueError("Ticker Selector requires a nonempty Parquet pool.")
    by_id = {}
    for raw in sources:
        source = normalize_source(raw)
        key = source["instrument_id"]
        if key in by_id and source != by_id[key]:
            raise ValueError(f"Conflicting sources for {key}.")
        by_id[key] = source
    pool = list(by_id.values())
    if len({s["timeframe_minutes"] for s in pool}) != 1:
        raise ValueError("The portfolio pool must use one trading timeframe.")
    return pool


def build_calendar(start, end, trading_minutes, rebalance_months, *, is_months=None, oos_months=None):
    """Half-open logical periods; inclusive user End admits only whole grid bars."""
    start, end = _timestamp(start), _timestamp(end, end=True)
    if start.day != 1 or start != start.normalize():
        raise ValueError("Portfolio Start must be day 1 at 00:00 UTC.")
    step = pd.Timedelta(minutes=timeframe_minutes(trading_minutes))
    exclusive = pd.Timestamp((end.value // step.value + 1) * step.value, tz="UTC")
    cadence = positive_integer(rebalance_months, "Rebalance months")
    wfa = is_months is not None or oos_months is not None
    width = positive_integer(oos_months, "OOS months") if wfa else 1
    history = positive_integer(is_months, "IS months") if wfa else 0
    if cadence % width:
        raise ValueError("Rebalance months must be a multiple of OOS months.")
    cursor = start + pd.DateOffset(months=history)
    windows = []
    while cursor + pd.DateOffset(months=width) <= exclusive:
        finish = cursor + pd.DateOffset(months=width)
        beginning = cursor - pd.DateOffset(months=history) if wfa else cursor
        if any(t.value % step.value for t in (beginning, cursor, finish)):
            raise ValueError("Trading timeframe straddles a required portfolio boundary.")
        windows.append({"id": len(windows) + 1, "start": cursor.isoformat(), "end": finish.isoformat(),
                        "isStart": beginning.isoformat() if wfa else None})
        cursor = finish
    if len(windows) < (2 if wfa else 1):
        raise ValueError("Portfolio requires two complete WFA windows." if wfa else "Portfolio requires one complete calendar month.")
    size = cadence // width
    return [{"id": i // size + 1, "start": windows[i]["start"],
             "end": windows[min(i + size, len(windows)) - 1]["end"],
             "windows": windows[i:i + size]} for i in range(0, len(windows), size)]


def rank_pool(pool, cutoff, selector, *, cancelled=None, cache=None):
    """Read each required month separately, excluding cross-month target groups."""
    cutoff = _timestamp(cutoff)
    params = selector["params"]
    first = cutoff - pd.DateOffset(months=params["lookbackMonths"])
    step = pd.Timedelta(minutes=params["timeframeMinutes"]).value
    cache = {} if cache is None else cache
    ranked = []
    for source in pool:
        with pack_session(source["pack_root"], cancelled=cancelled) as session:
            entry = manifest.find_instrument(manifest.read_manifest(session.data_root), source["instrument_id"])
            listing = (entry.get("instrument_rules") or {}).get("listed_at_utc")
            if listing:
                manifest.validate_instrument_rules(entry['instrument_rules'], source['instrument_id'], venue=entry['venue'])
            row = {"instrument": source["instrument_id"], "source": source, "listing": listing,
                   "eligible": True, "reason": None, "score": None, "rank": None,
                   "selected": False, "months": []}
            if listing and _timestamp(listing) > first:
                row.update(eligible=False, reason="Listed after required ranking history starts")
                ranked.append(row)
                continue
            terms = []
            month = first
            while month < cutoff:
                following = month + pd.DateOffset(months=1)
                left, right = -(-month.value // step) * step, following.value // step * step
                expected = (right - left) // step
                if expected <= 0:
                    raise ValueError(f"{source['instrument_id']} {month:%Y-%m}: ranking timeframe has no complete bars.")
                key = (source["pack_root"], source["instrument_id"], left, right, params["timeframeMinutes"])
                if key not in cache:
                    loaded = session.load_slice(source["instrument_id"], start=pd.Timestamp(left, tz="UTC"),
                                                end=pd.Timestamp(right, tz="UTC"), timeframe_minutes=params["timeframeMinutes"])
                    frame = loaded.bars
                    observed = len(frame)
                    if 100 * observed < 99 * expected:
                        raise ValueError(f"{source['instrument_id']} {month:%Y-%m}: ranking coverage {observed}/{expected} is below 99%.")
                    values = (100 * (frame.high - frame.low) / frame.close).tolist()
                    if any(not math.isfinite(x) or x < 0 for x in values):
                        raise ValueError(f"{source['instrument_id']}: invalid ranking prices.")
                    cache[key] = (values, {"month": month.strftime("%Y-%m"), "start": pd.Timestamp(left, tz="UTC").isoformat(),
                        "end": pd.Timestamp(right, tz="UTC").isoformat(), "expected": expected, "observed": observed,
                        "fingerprintVersion": loaded.fingerprint_version, "fingerprint": loaded.input_fingerprint})
                values, pin = cache[key]
                terms.extend(values)
                row["months"].append(deepcopy(pin))
                month = following
            row["score"] = math.fsum(terms) / len(terms)
            ranked.append(row)
    eligible = sorted((r for r in ranked if r["eligible"]), key=lambda r: (-r["score"], r["instrument"]))
    for number, row in enumerate(eligible, 1):
        row.update(rank=number, selected=number <= selector["topN"])
    return sorted(ranked, key=lambda r: (r["rank"] is None, r["rank"] or 0, r["instrument"]))
