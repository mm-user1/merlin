"""Lab transport for core filter descriptors, saved facts and runtime locators."""
from pathlib import Path
import ntpath
import os

from .config import canonical_sha256, _keys
from .dataset import load_json
from core.entry_filters import capability, effective_config, source_locator
from core.entry_filter_input import describe_saved, validate_envelope

REQUESTS = {"strategy_lab_runspec_v3", "strategy_lab_fixed_runspec_v2"}


def effective(raw, config=None):
    items = effective_config(raw["generation"]["execution"]["entryFilters"]) if raw["schema_version"] in REQUESTS else []
    if config is not None and items:
        methods = capability(config)
        if any(i["type"] not in methods for i in items):
            raise ValueError("strategy does not support the requested entry filters.")
    return items


def validate_saved(editor, items, envelope, target):
    if effective_config(editor) != items:
        raise ValueError("saved effective entry filters differ from declaration.")
    validate_envelope(envelope, items, target_pin=target)
    for item, pin in zip(items, envelope.get("sources", [])):
        instrument = pin["instrument"]
        if (instrument.get("instrument_id") != item["source"]["instrument_id"] or
                instrument.get("volume_unit") != "quote_turnover" or
                any(not isinstance(instrument.get(k), str) or not instrument[k] for k in ("venue", "contract", "quote_currency"))):
            raise ValueError("saved filter instrument ownership mismatch.")


def summary(editor, items, envelope, target):
    validate_saved(editor, items, envelope, target)
    description = describe_saved({"entryFilters": editor, "entry_filter_inputs": envelope, "data_input": target})
    if description["key"] is None:
        raise ValueError("saved filter description unavailable after validation.")
    return {"entry_filter_state": "filtered" if items else "off",
            "entry_filter_key": canonical_sha256(items), "entry_filter_description": description["text"]}


def read_overrides(path):
    if path is None:
        return None
    value = load_json(Path(path), "filter roots")
    _keys(value, "filter roots", {"version", "roots"})
    if type(value["version"]) is not int or value["version"] != 1 or not isinstance(value["roots"], dict):
        raise ValueError("filter roots require version 1 and a roots object.")
    return value["roots"]


def native_locator(value):
    """Match core's lexical locator classification before resolving a host path."""
    value = os.fspath(value)
    if ntpath.isabs(value) and (ntpath.splitdrive(value)[0] or value.startswith("\\\\")):
        return os.name == "nt"
    return Path(value).is_absolute()


def protect_output(output, roots):
    """Foreign-host recorded locators are metadata, never cwd-relative paths."""
    output = Path(output).resolve()
    for value in roots:
        root = Path(value)
        if native_locator(value):
            root = root.resolve()
            if output.is_relative_to(root) or root.is_relative_to(output):
                raise ValueError("output overlaps a dataset or target/filter source root.")


def runtime_roots(items, overrides, *, existing, output, target):
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict):
        raise ValueError("filter_root_overrides must map original locators to local roots.")
    original = {i["source"]["pack_root"] for i in items}
    normalized = {}
    for key, value in overrides.items():
        key = source_locator(key)
        if key not in original:
            raise ValueError("unused or unknown filter root override.")
        if not isinstance(value, str) or not value.strip() or not native_locator(value):
            raise ValueError("filter root override destination must be a native absolute directory.")
        destination = str(Path(value).resolve())
        if key in normalized and normalized[key] != destination:
            raise ValueError("conflicting normalized filter root overrides.")
        normalized[key] = destination
    if normalized and not existing:
        raise ValueError("new datasets require original filter locations; overrides require an existing dataset.")
    roots = [str(target)]
    for key in original:
        location = normalized.get(key, key)
        if not native_locator(location):
            raise ValueError("foreign filter locator requires a native absolute override before source I/O.")
        roots.append(location)
    protect_output(output, roots)
    return normalized


def recorded_roots(items):
    return [i["source"]["pack_root"] for i in items]
