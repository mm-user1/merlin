"""Fixed entry permissions: configuration, causal state and signal application.

No pack IO or execution policy lives here. Contexts are explicit, pickleable RAM
values; original raw signal arrays and all exit/dataprep arrays are preserved.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import json
import ntpath
import posixpath

import numpy as np
import pandas as pd


class FilterContext(Enum):
    MISSING = "missing"
    OFF = "off"


MISSING = FilterContext.MISSING
OFF = FilterContext.OFF
METHODS = {"candleDirection": {"version": 1, "name": "F06 · Candle Direction"}}
POLICY = "closed_candle_direction_v1"


def _error(message):
    from .data_input import DataInputError
    return DataInputError("Entry Filters: " + message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def source_locator(value):
    """Normalize original absolute locators, including Windows on Linux, without IO."""
    if not isinstance(value, str) or not value.strip():
        raise _error("source pack_root must be an absolute directory.")
    if ntpath.isabs(value) and (ntpath.splitdrive(value)[0] or value.startswith("\\\\")):
        return ntpath.normcase(ntpath.normpath(value)).replace("\\", "/")
    if posixpath.isabs(value):
        return posixpath.normpath(value)
    raise _error("source pack_root must be an absolute directory.")


def descriptor(item):
    from .data_input import timeframe_minutes
    from data_provider.errors import DataProviderError
    from data_provider.manifest import normalize_instrument_id
    if not isinstance(item, dict) or set(item) - {"id", "type", "version", "enabled", "parameters", "source"}:
        raise _error("unsupported filter fields or expression/optimization settings.")
    if item.get("type") not in METHODS or type(item.get("version")) is not int or item["version"] != 1:
        raise _error("unsupported filter method/version.")
    params = item.get("parameters")
    if not isinstance(params, dict) or set(params) != {"confirmationBars"}:
        raise _error("candleDirection requires only confirmationBars.")
    count = params["confirmationBars"]
    if type(count) is not int or not 2 <= count <= np.iinfo(np.int64).max:
        raise _error("confirmationBars must be a representable integer >= 2.")
    source = item.get("source")
    if (not isinstance(source, dict) or set(source) != {"type", "version", "pack_root", "instrument_id", "timeframe_minutes"}
            or source.get("type") != "parquet" or type(source.get("version")) is not int or source["version"] != 1):
        raise _error("expected a version-1 Parquet source.")
    try:
        instrument_id = normalize_instrument_id(source["instrument_id"])
    except DataProviderError as exc:
        raise _error(f"invalid source instrument_id: {exc}") from exc
    return {"type": item["type"], "version": 1, "parameters": {"confirmationBars": count},
            "source": {**source, "pack_root": source_locator(source["pack_root"]),
                       "instrument_id": instrument_id,
                       "timeframe_minutes": timeframe_minutes(source["timeframe_minutes"])}}


def effective_config(editor=MISSING):
    """Normalize launch/editor meaning; disabled settings are retained by callers."""
    if editor is MISSING:
        return []
    if (not isinstance(editor, dict) or type(editor.get("version")) is not int or editor["version"] != 1
            or type(editor.get("enabled")) is not bool or not isinstance(editor.get("items"), list)
            or set(editor) != {"version", "enabled", "items"}):
        raise _error("expected version-1 entryFilters with enabled and items.")
    ids = set()
    active = {}
    for item in editor["items"]:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"].strip() or item["id"] in ids:
            raise _error("each item needs a unique nonblank ID.")
        ids.add(item["id"])
        if not editor["enabled"]:
            continue
        if type(item.get("enabled")) is not bool:
            raise _error("item enabled must be a boolean.")
        if item["enabled"]:
            value = descriptor(item)
            active[canonical(value)] = value
    return [active[key] for key in sorted(active)]


def capability(config, variants=None):
    if "entryFilterSupport" not in config:
        return ()
    declaration = config["entryFilterSupport"]
    if (not isinstance(declaration, dict) or set(declaration) != {"version", "methods"}
            or type(declaration.get("version")) is not int or declaration["version"] != 1
            or declaration.get("methods") != ["candleDirection"]):
        raise _error("invalid entryFilterSupport; expected version 1 and methods [candleDirection].")
    if variants is None:
        from .engine_v2.profile import parse_execution_profile
        return parse_execution_profile(config).entry_filter_methods
    if config.get("engine") != "v2" or any(
            v.modes.get("entryAdmission") != "risk_bracket_v1" or v.modes.get("topology") is not None
            or v.modes.get("boundary") != "strict_close" for v in variants.values()):
        raise _error("entryFilterSupport requires risk_bracket_v1 strict-close position variants.")
    return ("candleDirection",)


@dataclass(frozen=True)
class PreparedPermissions:
    effective_json: str
    opens_ns: np.ndarray
    decisions_ns: np.ndarray
    permit_long: np.ndarray
    permit_short: np.ndarray
    known: np.ndarray
    reasons: np.ndarray  # 0 valid, 1 native warmup, 2 absent/expired publication

    def __post_init__(self):
        size = len(self.opens_ns)
        for name in ("opens_ns", "decisions_ns", "permit_long", "permit_short", "known", "reasons"):
            array = np.array(getattr(self, name), copy=True)
            if array.ndim != 1 or len(array) != size:
                raise _error("permission arrays must have equal one-dimensional alignment.")
            array.flags.writeable = False
            object.__setattr__(self, name, array)
        if (self.opens_ns.dtype != np.int64 or self.decisions_ns.dtype != np.int64
                or any(getattr(self, n).dtype != np.bool_ for n in ("permit_long", "permit_short", "known"))
                or np.any(np.diff(self.opens_ns) <= 0) or np.any(self.decisions_ns <= self.opens_ns)
                or np.any((self.permit_long | self.permit_short) & ~self.known)):
            raise _error("invalid permission timestamps, types or unknown permissions.")
        values = json.loads(self.effective_json)
        normalized = sorted({canonical(descriptor(value)) for value in values})
        if not normalized or canonical([json.loads(v) for v in normalized]) != self.effective_json:
            raise _error("invalid prepared effective configuration.")

    def indices(self, timestamps):
        stamps = pd.DatetimeIndex(timestamps).as_unit("ns").asi8
        indices = np.searchsorted(self.opens_ns, stamps)
        if (np.any(indices >= len(self.opens_ns)) or np.any(np.diff(stamps) <= 0)
                or not np.array_equal(self.opens_ns[indices], stamps)):
            raise _error("execution timestamps are outside the prepared permission context.")
        return indices


def validate_context(methods, context, effective=MISSING):
    if methods:
        if context is not OFF and not isinstance(context, PreparedPermissions):
            raise _error("capable execution requires explicit Off or prepared permissions; context is missing/invalid.")
    elif context is not MISSING and context is not OFF:
        raise _error("this strategy does not support entry filters.")
    if effective is not MISSING:
        actual = json.loads(context.effective_json) if isinstance(context, PreparedPermissions) else []
        if actual != effective:
            raise _error("permission context conflicts with the effective filter configuration.")
    return context


def apply_permissions(data, context):
    if not isinstance(context, PreparedPermissions):
        return data
    indices = context.indices(data.timestamps)
    return replace(data, signals=replace(data.signals,
        long_entries=data.signals.long_entries & context.permit_long[indices],
        short_entries=data.signals.short_entries & context.permit_short[indices]))


def wrap_builders(builder, batch, context):
    if not isinstance(context, PreparedPermissions):
        return builder, batch
    def single(frame, params):
        context.indices(frame.index)
        return apply_permissions(builder(frame, params), context)
    def multiple(frame, params):
        context.indices(frame.index)
        return [apply_permissions(data, context) for data in batch(frame, params)]
    return single, multiple if batch else None


def candle_direction(bars, source_minutes, confirmation_bars, decisions_ns):
    """Publish closed native states; newest unknown/neutral replaces older state."""
    step = int(source_minutes) * 60 * 10**9
    opens = pd.DatetimeIndex(bars.index).as_unit("ns").asi8
    colours = np.sign(bars["close"].to_numpy() - bars["open"].to_numpy()).astype(np.int8)
    prior = max(28, confirmation_bars - 1)
    known = np.zeros(len(opens), dtype=bool)
    long = known.copy()
    short = known.copy()
    run = position = 0
    for i, colour in enumerate(colours):
        contiguous = i > 0 and int(opens[i]) - int(opens[i-1]) == step
        position = position + 1 if contiguous else 0
        run = (run + 1 if contiguous and colour == colours[i-1] else 1) if colour else 0
        known[i] = position >= prior
        long[i] = known[i] and colour > 0 and run >= confirmation_bars
        short[i] = known[i] and colour < 0 and run >= confirmation_bars
    # Python integers prevent overflow at the timestamp boundary.
    closes = np.array([int(t) + step for t in opens], dtype=np.int64)
    decisions = np.asarray(decisions_ns, dtype=np.int64)
    ix = np.searchsorted(closes, decisions, side="right") - 1
    valid = ix >= 0
    safe = np.maximum(ix, 0)
    if len(opens) == 0:
        return (np.zeros(len(decisions), dtype=bool),) * 3 + (np.full(len(decisions), 2, dtype=np.int8),)
    # Unsigned subtraction is exact for the nonnegative timestamp distance, even
    # across the signed nanosecond midpoint. Negative distances are already invalid.
    valid &= decisions.astype(np.uint64) - closes[safe].astype(np.uint64) < step
    aligned_known = valid & known[safe]
    reason = np.where(~valid, 2, np.where(aligned_known, 0, 1)).astype(np.int8)
    return valid & long[safe], valid & short[safe], aligned_known, reason
