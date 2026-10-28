"""Bounded provider reads and replay pins for fixed entry filters."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from data_provider.errors import DataProviderError
from .data_input import DataInputError, _timestamp, _iso, pack_session, timeframe_minutes
from .entry_filters import (MISSING, OFF, POLICIES, PreparedPermissions, canonical,
                            descriptor, effective_config, source_locator, candle_direction, origin_rule)


def _bounded_bounds(target_pin, item):
    target_step = timeframe_minutes(target_pin["source"]["timeframe_minutes"]) * 60 * 10**9
    step = item["source"]["timeframe_minutes"] * 60 * 10**9
    prior = max(28, item["parameters"]["confirmationBars"] - 1)
    first = _timestamp(target_pin["bounds"]["start"]).value + target_step
    last = _timestamp(target_pin["bounds"]["end"]).value
    start, end = first // step * step - (prior + 1) * step, last // step * step
    if not pd.Timestamp.min.value <= start < end <= pd.Timestamp.max.value:
        raise DataInputError("Entry Filters: required history exceeds the supported date range; reduce Confirm Bars/timeframe or choose a later Start.")
    return {"start": _iso(start), "end": _iso(end), "origin": _iso(start), "priorBars": prior}


def source_bounds(target_pin, item, *, declared_first_open=None):
    bounds = _bounded_bounds(target_pin, item)
    if origin_rule(item) == "declared_source_origin":
        if declared_first_open is None:
            raise DataInputError("F06 v2 Break > 1 requires declared source history metadata.")
        step = item["source"]["timeframe_minutes"] * 60 * 10**9
        first = _timestamp(declared_first_open).value
        start = -(-first // step) * step
        if not pd.Timestamp.min.value <= start <= _timestamp(bounds["start"]).value:
            raise DataInputError("F06 declared source origin does not cover the required prefix; select a later Start.")
        bounds.update(start=_iso(start), origin=_iso(start))
    return bounds


def validate_target_bounds(pin, target_pin, item):
    bounded = _bounded_bounds(target_pin, item)
    actual = pin["bounds"]
    if origin_rule(item) == "declared_source_origin":
        if (actual["end"] != bounded["end"] or actual["priorBars"] != 28
                or _timestamp(actual["start"]) > _timestamp(bounded["start"])):
            raise DataInputError("Filter declared-origin bounds conflict with the saved target input.")
    elif actual != bounded:
        raise DataInputError("Filter bounds conflict with the saved target input.")


def validate_envelope(envelope, effective, target_pin=None):
    """Offline validation: absent/malformed new facts never silently become Off."""
    try:
        if not isinstance(envelope, dict) or type(envelope.get("version")) is not int or envelope["version"] != 1:
            raise ValueError("missing or unsupported entry_filter_inputs")
        if not effective:
            if envelope != {"version": 1, "state": "off"}:
                raise ValueError("Off settings require explicit Off inputs")
            return envelope
        if (set(envelope) != {"version", "state", "effective", "sources"} or envelope["state"] != "filtered"
                or envelope["effective"] != effective or len(envelope["sources"]) != len(effective)):
            raise ValueError("inputs conflict with saved entryFilters")
        for item, pin in zip(effective, envelope["sources"]):
            if descriptor(item) != item:
                raise ValueError("invalid effective descriptor")
            if (pin["descriptor"] != item or pin["policy"] != POLICIES[item["version"]] or pin["fingerprint_version"] != 1
                    or type(pin["fingerprint_version"]) is not int or not re.fullmatch("[0-9a-f]{64}", pin["input_fingerprint"])):
                raise ValueError("invalid source policy/binding/fingerprint")
            if (item["version"] == 2 and pin.get("originRule") != origin_rule(item)) or (item["version"] == 1 and "originRule" in pin):
                raise ValueError("invalid method originRule")
            bounds = pin["bounds"]
            step = item["source"]["timeframe_minutes"] * 60 * 10**9
            start, end = (_timestamp(bounds[k]).value for k in ("start", "end"))
            if (not start < end or start % step or end % step or bounds["origin"] != bounds["start"]
                    or type(bounds["priorBars"]) is not int or bounds["priorBars"] != max(28, item["parameters"]["confirmationBars"] - 1)
                    or not isinstance(pin["instrument"], dict)):
                raise ValueError("invalid native preparation origin")
            if target_pin is not None:
                validate_target_bounds(pin, target_pin, item)
        return envelope
    except (KeyError, TypeError, ValueError) as exc:
        raise DataInputError(f"Invalid saved entry_filter_inputs: {exc}; recomputation refused.") from exc


def saved_effective(config, *, required=False):
    if "entryFilters" not in config and "entry_filter_inputs" not in config and not required:
        return []  # Genuine pre-feature record; no inferred capability at execution.
    if "entryFilters" not in config or "entry_filter_inputs" not in config:
        raise DataInputError("Saved entry-filter settings/inputs are missing; a new study is required.")
    effective = effective_config(config["entryFilters"])
    if any(item["version"] == 2 for item in effective) and not isinstance(config.get("data_input"), dict):
        raise DataInputError("Saved v2 entry-filter context requires its target input bounds.")
    validate_envelope(config["entry_filter_inputs"], effective, config.get("data_input"))
    return effective


def describe_saved(config):
    """Offline selection identity; consumed hashes/dates and overrides are not identity."""
    from .data_input import timeframe_label
    try:
        items = saved_effective(config)
        text = "\n".join(
            f"F06{' v2' if i['version'] == 2 else ''} · {i['source']['instrument_id']} · {timeframe_label(i['source']['timeframe_minutes'])} · "
            + (f"N {i['parameters']['confirmationBars']} / break {i['parameters']['breakBars']}" if i['version'] == 2 else f"{i['parameters']['confirmationBars']} bars")
            for i in items)
        return {"key": canonical(items) if items else "off", "text": text or "Filter Off"}
    except (ValueError, TypeError, KeyError):
        return {"key": None, "text": "Unavailable"}


def metadata_entry(session, source):
    report = session.inspect(verify=False)
    if report.get("update_in_progress"):
        raise DataInputError("Entry filter pack has a pending operation; recover or abort it first.")
    entry = next((e for e in report["instruments"] if e["instrument_id"] == source["instrument_id"]), None)
    if entry is None or not entry["research_readable"]:
        raise DataInputError(f"Entry filter {source['instrument_id']} is absent or unreadable in {source['pack_root']}.")
    return entry


def verify_relocation(config, original, root):
    """Verify every filter dependency sharing an original locator before saving it."""
    effective = saved_effective(config, required=True)
    affected = [i for i, item in enumerate(effective) if item["source"]["pack_root"] == original]
    if not affected:
        raise DataInputError("Unknown filter dependency selected for relocation.")
    with pack_session(root) as session:
        cache = {}
        for i in affected:
            pin = config["entry_filter_inputs"]["sources"][i]
            validate_target_bounds(pin, config["data_input"], effective[i])
            _, actual = _load(session, effective[i], pin["bounds"], cache)
            if actual != pin:
                raise DataInputError("Relocated filter consumed inputs differ; override was not saved.")


def _load(session, item, bounds, cache):
    source = item["source"]
    key = canonical([source["instrument_id"], source["timeframe_minutes"], bounds["start"], bounds["end"]])
    if key not in cache:
        entry = metadata_entry(session, source)
        if _timestamp(entry["first_open_utc"]) > _timestamp(bounds["start"]):
            raise DataInputError(f"F06 {source['instrument_id']}: required history starts {bounds['start']}; select a later Start date.")
        try:
            cache[key] = session.load_slice(source["instrument_id"], start=bounds["start"],
                warmup_start=bounds["start"], end=bounds["end"], timeframe_minutes=source["timeframe_minutes"])
        except DataProviderError as exc:
            if "no complete" in str(exc) and "research bar" in str(exc):
                raise DataInputError(f"F06 {source['instrument_id']}: empty source; no complete candle in the required interval.") from exc
            raise
    loaded = cache[key]
    facts = {"descriptor": deepcopy(item), "policy": POLICIES[item["version"]], "bounds": deepcopy(bounds),
        "fingerprint_version": loaded.fingerprint_version, "input_fingerprint": loaded.input_fingerprint,
        "instrument": {"instrument_id": loaded.instrument_id, "venue": loaded.venue,
                       "contract": loaded.contract, "quote_currency": loaded.quote_currency, "volume_unit": "quote_turnover"}}
    if item["version"] == 2:
        facts["originRule"] = origin_rule(item)
    return loaded, facts


def prepare_bundle(target_root, target_loader, effective, *, saved=MISSING, overrides=None,
                   parent_target=None, parent_filters=None, cancelled=None):
    """Read each resolved root once, target and same-root filters under one session.

    target_loader owns target preparation/replay. Optional parent pins are verified
    alongside a manual interval. Independent roots are serial, not an atomic pack set.
    """
    if saved is not MISSING:
        validate_envelope(saved, effective)
    if parent_filters is not None:
        validate_envelope(parent_filters, effective)
    overrides = overrides or {}
    groups = {}
    for i, item in enumerate(effective):
        original = item["source"]["pack_root"]
        root = str(Path(overrides.get(original, original)).resolve())
        groups.setdefault(source_locator(root), []).append(i)
    target_key = source_locator(str(Path(target_root).resolve()))
    prepared = None
    loaded_by_index = {}
    pins = {}

    def read_group(session, indices):
        cache = {}
        for i in indices:
            item = effective[i]
            if parent_filters is not None and effective:
                parent = parent_filters["sources"][i]
                validate_target_bounds(parent, parent_target, item)
                _, actual_parent = _load(session, item, parent["bounds"], cache)
                if actual_parent != parent:
                    raise DataInputError("Parent filter inputs changed; manual execution refused.")
            if saved is not MISSING:
                validate_target_bounds(saved["sources"][i], prepared.facts, item)
                bounds = saved["sources"][i]["bounds"]
            elif parent_filters is not None and origin_rule(item) == "declared_source_origin":
                # A manual child verifies and retains the parent's consumed origin.
                # Current metadata may have gained earlier history since that run.
                bounds = source_bounds(prepared.facts, item,
                    declared_first_open=parent_filters["sources"][i]["bounds"]["start"])
            else:
                first_open = metadata_entry(session, item["source"])["first_open_utc"] if origin_rule(item) == "declared_source_origin" else None
                bounds = source_bounds(prepared.facts, item, declared_first_open=first_open)
            loaded, facts = _load(session, item, bounds, cache)
            if saved is not MISSING and facts != saved["sources"][i]:
                raise DataInputError("Pinned filter input changed in consumed candles/gaps/facts; replay refused.")
            loaded_by_index[i], pins[i] = loaded, facts

    with pack_session(target_root, cancelled=cancelled) as session:
        prepared = target_loader(session)
        read_group(session, groups.pop(target_key, []))
    for root, indices in groups.items():
        with pack_session(root, cancelled=cancelled) as session:
            read_group(session, indices)
    if not effective:
        return replace(prepared, entry_filter_context=OFF, entry_filter_inputs={"version": 1, "state": "off"})
    opens = prepared.frame.index.as_unit("ns").asi8
    step = prepared.facts["source"]["timeframe_minutes"] * 60 * 10**9
    decisions = np.array([int(t) + step for t in opens], dtype=np.int64)
    long = np.ones(len(opens), dtype=bool)
    short, known = long.copy(), long.copy()
    reasons = np.zeros(len(opens), dtype=np.int8)
    for i, item in enumerate(effective):
        lo, sh, valid, why = candle_direction(loaded_by_index[i].bars,
            item["source"]["timeframe_minutes"], item["parameters"]["confirmationBars"], decisions,
            break_bars=item["parameters"].get("breakBars"))
        long &= lo
        short &= sh
        known &= valid
        reasons = np.maximum(reasons, why)
    context = PreparedPermissions(canonical(effective), opens, decisions, long, short, known, reasons)
    envelope = {"version": 1, "state": "filtered", "effective": deepcopy(effective),
                "sources": [pins[i] for i in range(len(effective))]}
    return replace(prepared, entry_filter_context=context, entry_filter_inputs=envelope)
