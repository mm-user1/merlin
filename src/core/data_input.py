"""Merlin's strict, replayable market input boundary.

Pack I/O ends before execution. Prepared frames travel as explicit transient
arguments; only candle ``facts`` and the optional admission envelope are persisted.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
import os
from pathlib import Path
import re
import threading
import time
from weakref import WeakValueDictionary

import numpy as np
import pandas as pd

from data_provider import data, manifest
from data_provider.errors import BusyError, DataProviderError, PendingError
from .backtest_engine import prepare_dataset_with_warmup

VERSION = 1
POLICY = "merlin_contiguous_quote_v1"
_locks = WeakValueDictionary()
_locks_guard = threading.Lock()


class DataInputError(ValueError):
    """Actionable admission or replay failure, never a CSV fallback."""


def timeframe_minutes(value):
    if type(value) is not int or value <= 0 or value % 5:
        raise DataInputError("Timeframe must be a positive integer multiple of 5 minutes.")
    if value > pd.Timedelta.max.value // (60 * 10**9):
        raise DataInputError("Timeframe exceeds the supported date range.")
    return value


def timeframe_label(minutes):
    minutes = timeframe_minutes(minutes)
    for divisor, unit in ((1440, "D"), (60, "h"), (1, "m")):
        if minutes % divisor == 0:
            return f"{minutes // divisor}{unit}"


def normalize_source(source):
    if not isinstance(source, dict) or source.get("type") != "parquet":
        raise DataInputError("Expected a typed Parquet source.")
    if type(source.get("version")) is not int or source["version"] != VERSION:
        raise DataInputError("Unsupported Parquet source version.")
    raw_root = source.get("pack_root")
    if not isinstance(raw_root, str) or not Path(raw_root).is_absolute():
        raise DataInputError("Parquet pack_root must be an absolute directory path.")
    try:
        instrument = manifest.normalize_instrument_id(source.get("instrument_id"))
    except DataProviderError as exc:
        raise DataInputError(str(exc)) from exc
    return {"type": "parquet", "version": VERSION,
            "pack_root": str(Path(raw_root).resolve()), "instrument_id": instrument,
            "timeframe_minutes": timeframe_minutes(source.get("timeframe_minutes"))}


def source_label(source):
    source = normalize_source(source)
    return f"{source['instrument_id']}, {timeframe_label(source['timeframe_minutes'])}"


def stored_identity(config):
    """Display/export identity from saved facts, without touching the pack."""
    pin = config.get("data_input")
    try:
        source = normalize_source(pin["source"])
        venue, contract = source["instrument_id"].split("_", 1)
        return f"{venue}:{contract}", timeframe_label(source["timeframe_minutes"])
    except (KeyError, TypeError, ValueError):
        return None, None


def is_pack_directory(root):
    root = Path(root)
    return any((root / name).exists() or (root / name).is_symlink() for name in (
        manifest.MANIFEST_NAME, manifest.UPDATE_MARKER_NAME, manifest.UPDATE_MARKER_TEMP_NAME))


@contextmanager
def pack_session(root, *, cancelled=None, timeout=60):
    """Serialize local reads; independent processes retain fail-fast OS exclusion."""
    root = Path(root).resolve()
    if not is_pack_directory(root):
        raise DataInputError(f"{root}: no pack manifest or pending operation found.")
    key = os.path.normcase(str(root))
    with _locks_guard:
        lock = _locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _locks[key] = lock
    deadline = time.monotonic() + timeout
    held = False
    try:
        while not held:
            if cancelled and cancelled():
                raise DataInputError("Parquet preparation cancelled while waiting for a local read.")
            held = lock.acquire(timeout=min(.1, max(0, deadline - time.monotonic())))
            if not held and time.monotonic() >= deadline:
                raise DataInputError("Parquet pack is busy with another local preparation; retry later.")
        if cancelled and cancelled():
            raise DataInputError("Parquet preparation cancelled before opening the pack.")
        with data.read_session(root) as session:
            yield session
    except BusyError as exc:
        raise DataInputError(f"{root}: pack busy in another process; retry after it finishes.") from exc
    except PendingError as exc:
        raise DataInputError(f"{root}: pending collector operation; recover or abort it before running Merlin.") from exc
    except (DataProviderError, OSError) as exc:
        raise DataInputError(f"Parquet input: {exc}") from exc
    finally:
        if held:
            lock.release()


def inspect_pack(root, **kwargs):
    with pack_session(root, **kwargs) as session:
        report = session.inspect(verify=False)
        if report.get("update_in_progress"):
            raise DataInputError("Pack has a pending collector operation; recover or abort it first.")
        return report


def _timestamp(value, *, end=False):
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            raise ValueError("missing timestamp")
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        if end and isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value.strip()):
            ts += pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        # value is deliberately nanoseconds: a sub-ms start must not round down.
        _ = ts.value
        return ts
    except (ValueError, TypeError, OverflowError) as exc:
        raise DataInputError(f"Invalid or unrepresentable date: {value!r}") from exc


def _iso(ns):
    try:
        return pd.Timestamp(ns, unit="ns", tz="UTC").isoformat()
    except (ValueError, OverflowError) as exc:
        raise DataInputError("Requested interval exceeds the supported date range.") from exc


def resolve_range(source, entry, *, date_filter, start=None, end=None, warmup_bars=1000):
    """Resolve advisory metadata bounds using the same integer grid as execution."""
    minutes = timeframe_minutes(source["timeframe_minutes"])
    if type(warmup_bars) is not int or warmup_bars < 0:
        raise DataInputError("Warmup must be a nonnegative integer.")
    step = minutes * 60 * 10**9
    first = _timestamp(entry["first_open_utc"]).value
    cutoff = entry["verification"].get("closed_before_utc")
    if not cutoff or entry["verification"].get("volume_quote_verified") is not True:
        raise DataInputError("Pack requires certified closed candles and verified quote turnover.")
    stop = min(_timestamp(entry["coverage_end_utc"]).value, _timestamp(cutoff).value)
    first = -(-first // step) * step
    last = (stop // step - 1) * step
    user_start = bool(date_filter and start not in (None, ""))
    user_end = bool(date_filter and end not in (None, ""))
    start_ns = _timestamp(start).value if user_start else first
    end_ns = _timestamp(end, end=True).value if user_end else last
    opened = -(-start_ns // step) * step
    closed = (end_ns // step + 1) * step
    if start_ns > end_ns or opened >= closed:
        raise DataInputError("Selected interval contains no target bar opens.")
    actual_warmup = warmup_bars if user_start else 0
    warm = opened - actual_warmup * step
    return {"requested": {"dateFilter": bool(date_filter), "start": start, "end": end,
                           "warmupBars": warmup_bars},
            "start_origin": "user" if user_start else "metadata",
            "end_origin": "user" if user_end else "metadata",
            "evaluation_start": _iso(start_ns), "evaluation_end": _iso(end_ns),
            "start": _iso(opened), "end": _iso(closed), "warmup_start": _iso(warm),
            "trade_start_idx": actual_warmup}


@dataclass(frozen=True)
class PreparedInput:
    frame: pd.DataFrame
    trade_start_idx: int
    facts: dict
    admission_rules: object = None
    entry_admission: dict | None = None

    def window(self, start, end, warmup_bars):
        return prepare_dataset_with_warmup(self.frame, start, end, warmup_bars)


def _load(session, source, bounds):
    loaded = session.load_slice(source["instrument_id"], start=bounds["start"],
                                end=bounds["end"], warmup_start=bounds["warmup_start"],
                                timeframe_minutes=source["timeframe_minutes"])
    start = _timestamp(bounds["warmup_start"]).value
    end = _timestamp(bounds["end"]).value
    expected_base = (end - start) // (5 * 60 * 10**9)
    expected_target = (end - start) // (source["timeframe_minutes"] * 60 * 10**9)
    if (loaded.base_gap_count or loaded.omitted_group_count or
            loaded.base_row_count != expected_base or len(loaded.bars) != expected_target or
            np.any(loaded.segment_start[1:])):
        raise DataInputError(
            f"{source_label(source)}: consumed interval must be continuous, including warmup "
            f"[{bounds['warmup_start']}, {bounds['end']}) "
            f"({loaded.base_row_count}/{expected_base} base rows, "
            f"{loaded.base_gap_count} gaps, {loaded.omitted_group_count} incomplete groups).")
    facts = {"version": VERSION, "source": deepcopy(source), "preparation_policy": POLICY,
             "bounds": deepcopy(bounds), "fingerprint_version": loaded.fingerprint_version,
             "input_fingerprint": loaded.input_fingerprint,
             "instrument": {"instrument_id": loaded.instrument_id, "venue": loaded.venue,
                            "contract": loaded.contract, "quote_currency": loaded.quote_currency,
                            "volume_unit": "quote_turnover"}}
    frame = loaded.bars.rename(columns={"open": "Open", "high": "High", "low": "Low",
                                        "close": "Close", "volume_quote": "Volume"})
    frame = frame[["Open", "High", "Low", "Close", "Volume"]].astype("float64")
    frame.index = pd.DatetimeIndex(frame.index).as_unit("ns")
    return PreparedInput(frame, loaded.research_start_index, facts)


def _prepare(session, source, **range_options):
    report = session.inspect(verify=False)
    if report.get("update_in_progress"):
        raise DataInputError("Pack has a pending collector operation; recover or abort it first.")
    entry = next((e for e in report["instruments"] if e["instrument_id"] == source["instrument_id"]), None)
    if entry is None:
        raise DataInputError(f"Instrument {source['instrument_id']} is absent from this pack.")
    return _load(session, source, resolve_range(source, entry, **range_options))


def _admission_rules(session, instrument):
    from .engine_v2.admission import AdmissionRules
    entry = next((e for e in manifest.read_manifest(session.data_root)["instruments"]
                  if e["instrument_id"] == instrument), None)
    if entry is None:
        raise DataInputError(f"Instrument {instrument} is absent from this pack.")
    return AdmissionRules.from_entry(entry)


def preview_admission_rules(source, *, cancelled=None):
    """Resolve current rules from metadata only; actual runs still admit candles."""
    source = normalize_source(source)
    with pack_session(source["pack_root"], cancelled=cancelled) as session:
        report = session.inspect(verify=False)
        if report.get("update_in_progress"):
            raise DataInputError("Pack has a pending collector operation; recover or abort it first.")
        entry = next((e for e in report["instruments"] if e["instrument_id"] == source["instrument_id"]), None)
        if entry is None:
            raise DataInputError(f"Instrument {source['instrument_id']} is absent from this pack.")
        if not entry["research_readable"]:
            raise DataInputError(f"Instrument {source['instrument_id']} is unreadable: "
                                 + "; ".join(entry["research_blockers"]))
        return _admission_rules(session, source["instrument_id"])


def _with_admission(session, prepared, admission_params):
    if admission_params is None:
        return prepared
    from .engine_v2.admission import admission_envelope
    instrument = prepared.facts["source"]["instrument_id"]
    rules = _admission_rules(session, instrument)
    return replace(prepared, admission_rules=rules,
                   entry_admission=admission_envelope(rules, admission_params.get("maxEntryLeverage", 8)))


def prepare_input(source, *, cancelled=None, admission_params=None, **range_options):
    source = normalize_source(source)
    with pack_session(source["pack_root"], cancelled=cancelled) as session:
        return _with_admission(session, _prepare(session, source, **range_options), admission_params)


def validate_pin(pin):
    """Presence of a saved envelope is authoritative, including corrupt envelopes."""
    try:
        if not isinstance(pin, dict) or type(pin["version"]) is not int or pin["version"] != VERSION:
            raise ValueError("unsupported envelope version")
        source = normalize_source(pin["source"])
        if pin["preparation_policy"] != POLICY or type(pin["fingerprint_version"]) is not int or pin["fingerprint_version"] != 1:
            raise ValueError("unsupported preparation or fingerprint version")
        if not re.fullmatch("[0-9a-f]{64}", pin["input_fingerprint"]):
            raise ValueError("invalid input fingerprint")
        bounds = pin["bounds"]
        times = [_timestamp(bounds[k]).value for k in ("warmup_start", "start", "end")]
        step = source["timeframe_minutes"] * 60 * 10**9
        if not times[0] <= times[1] < times[2] or any(t % step for t in times):
            raise ValueError("invalid pinned interval")
        if type(bounds["trade_start_idx"]) is not int or bounds["trade_start_idx"] != (times[1] - times[0]) // step:
            raise ValueError("invalid trading origin")
        for key in ("start_origin", "end_origin"):
            if bounds[key] not in ("user", "metadata"):
                raise ValueError("invalid bound origin")
        ev_start, ev_end = (_timestamp(bounds[k]).value for k in ("evaluation_start", "evaluation_end"))
        if -(-ev_start // step) * step != times[1] or (ev_end // step + 1) * step != times[2]:
            raise ValueError("inconsistent evaluation bounds")
        if not isinstance(bounds["requested"], dict) or not isinstance(pin["instrument"], dict):
            raise ValueError("missing preparation facts")
        return source
    except (KeyError, TypeError, ValueError) as exc:
        raise DataInputError(f"Invalid saved data_input: {exc}. Execution is blocked; saved results remain viewable.") from exc


def _replay(session, pin, source):
    prepared = _load(session, source, pin["bounds"])
    if (prepared.facts["input_fingerprint"] != pin["input_fingerprint"] or
            prepared.facts["instrument"] != pin["instrument"]):
        raise DataInputError("Pinned input changed in consumed candles or instrument facts; replay refused.")
    return PreparedInput(prepared.frame, prepared.trade_start_idx, deepcopy(pin))


def replay_input(pin, *, root_override=None, cancelled=None, entry_admission=None, admission_cap=None):
    source = validate_pin(pin)
    root = root_override if root_override is not None else source["pack_root"]
    with pack_session(root, cancelled=cancelled) as session:
        prepared = _replay(session, pin, source)
        if entry_admission is None:
            return prepared
        from .engine_v2.admission import read_admission_envelope
        pinned = read_admission_envelope(entry_admission, admission_cap)
        current = _with_admission(session, prepared, {"maxEntryLeverage": admission_cap})
        if pinned.semantic() != current.admission_rules.semantic():
            raise DataInputError("Effective instrument rules changed; saved execution replay refused.")
        return replace(prepared, admission_rules=pinned, entry_admission=deepcopy(entry_admission))


def prepare_manual_input(pin, *, root_override=None, cancelled=None, admission_params=None, **range_options):
    source = validate_pin(pin)
    source["pack_root"] = str(Path(root_override or source["pack_root"]).resolve())
    with pack_session(source["pack_root"], cancelled=cancelled) as session:
        _replay(session, pin, source)
        return _with_admission(session, _prepare(session, source, **range_options), admission_params)
