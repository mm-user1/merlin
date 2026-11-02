"""Closed Fixed request boundary; live resolution is separate from saved structure."""
from copy import deepcopy
from itertools import product
import math
from pathlib import Path

from .. import config as c
from ..dataset import load_json

REQUEST_SCHEMA = "strategy_lab_fixed_runspec_v1"
DATASET_SCHEMA = "strategy_lab_fixed_dataset_v1"
REQUEST_V2 = "strategy_lab_fixed_runspec_v2"
DATASET_V2 = "strategy_lab_fixed_dataset_v2"
SCHEMA_PAIRS = {REQUEST_SCHEMA: DATASET_SCHEMA, REQUEST_V2: DATASET_V2}
ECONOMICS = ("initialCapital", "commissionPct", "riskPerTrade", "enableLong", "enableShort", "maxEntryLeverage")
RUNTIME = {"dateFilter", "start", "end", "warmupBars"}
VERSION_KEYS = {"strategy_id", "strategy_version", "engine", "grid_v2_engine_version",
    "plan_identity_schema_version", "semantic_identity_schema_version", "runtime_contract_version",
    "planning_policy_version", "allocator_version"}


def scalar(value, name, spec=None):
    if type(value) not in (bool, int, float, str) or isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name}: expected a finite scalar.")
    if spec is None:
        return
    kind = spec["type"]
    valid = (type(value) is bool if kind == "bool" else type(value) is int if kind == "int"
             else type(value) in (int, float) if kind == "float" else isinstance(value, str))
    if not valid:
        raise ValueError(f"{name}: invalid {kind} scalar type.")
    if "options" in spec and value not in spec["options"]:
        raise ValueError(f"{name}: invalid option.")
    if "min" in spec and value < spec["min"] or "max" in spec and value > spec["max"]:
        raise ValueError(f"{name}: outside Fixed min/max domain.")


def _list(value, name):
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name}: expected a nonempty ordered list.")
    return value


def validate_structure(raw):
    """Frozen v1 structure, without consulting registry/defaults or market inputs."""
    if raw.get("schema_version") not in SCHEMA_PAIRS:
        raise ValueError("unsupported Fixed request schema.")
    c._keys(raw, "Fixed request", {"schema_version", "run_name", "generation"})
    c._text(raw["run_name"], "run_name")
    g = c._object(raw["generation"], "generation")
    c._keys(g, "generation", {"strategy", "market_data", "economics", "execution", "parameter_sets",
        "timeframes_minutes", "periods", "resources", "expected_run_count"})
    c._keys(c._object(g["strategy"], "strategy"), "strategy", VERSION_KEYS)
    for k, v in g["strategy"].items():
        c._text(v, k)
    if g["strategy"]["engine"] != "v2":
        raise ValueError("Fixed requires engine v2.")
    key = "entryFilters" if raw["schema_version"] == REQUEST_V2 else "entry_filters"
    c._keys(c._object(g["execution"], "execution"), "execution", {"parameter_mode", "policy", key})
    if g["execution"]["parameter_mode"] != "fixed" or g["execution"]["policy"] != "risk_bracket_v1":
        raise ValueError("Fixed requires risk_bracket_v1; no optimization.")
    if key == "entryFilters":
        from ..filters import effective
        effective(raw)
    elif g["execution"][key] != []:
        raise ValueError("Fixed v1 requires explicit Filter Off.")
    c._keys(c._object(g["resources"], "resources"), "resources", {"outer_workers"})
    if type(g["resources"]["outer_workers"]) is not int or g["resources"]["outer_workers"] != 1:
        raise ValueError("Fixed requires outer_workers=1.")
    market = c._object(g["market_data"], "market_data")
    c._keys(market, "market_data", {"source_type", "instruments"})
    if market["source_type"] != "parquet":
        raise ValueError("Fixed requires Parquet.")
    from data_provider.manifest import normalize_instrument_id
    seen = set()
    for item in _list(market["instruments"], "instruments"):
        c._keys(c._object(item, "instrument"), "instrument", {"instrument_id", "cell"})
        name = c._text(item["instrument_id"], "instrument_id")
        if normalize_instrument_id(name) != name or name.endswith(".") or name in seen:
            raise ValueError("instrument IDs must be exact, normalized and unique.")
        seen.add(name)
        c._text(item["cell"], "cell")
    econ = c._object(g["economics"], "economics")
    c._keys(econ, "economics", {"base_params", "slippage_modelled", "funding_modelled"})
    if econ["slippage_modelled"] is not False or econ["funding_modelled"] is not False:
        raise ValueError("slippage and funding must be explicitly false.")
    base = c._object(econ["base_params"], "base_params")
    if not set(ECONOMICS) <= set(base):
        raise ValueError("base_params must explicitly declare capital, commission, risk, directions and cap.")
    for label, keys in (("parameter_sets", {"name", "params"}), ("periods", {"name", "start", "end", "warmup_bars"})):
        names = set()
        for item in _list(g[label], label):
            c._keys(c._object(item, label), label, keys)
            name = c._text(item["name"], label + ".name")
            if name in names:
                raise ValueError(f"{label}: duplicate name.")
            names.add(name)
    for params in [base, *(c._object(p["params"], "params") for p in g["parameter_sets"])]:
        for name, value in params.items():
            c._text(name, "parameter name")
            if name in RUNTIME or name == "contractSize":
                raise ValueError(f"runtime/input parameter is forbidden: {name}.")
            scalar(value, name)
    for p in g["parameter_sets"]:
        effective = base | p["params"]
        for key in ECONOMICS[:3] + ("maxEntryLeverage",):
            value = c._number(effective[key], key, positive=key != "commissionPct")
            if value < 0:
                raise ValueError(f"{key}: negative economics.")
        for key in ("enableLong", "enableShort"):
            if type(effective[key]) is not bool:
                raise ValueError(f"{key}: expected boolean.")
    frames = _list(g["timeframes_minutes"], "timeframes_minutes")
    if any(type(v) is not int or v < 5 or v % 5 or 1440 % v for v in frames) or len(set(frames)) != len(frames):
        raise ValueError("timeframes must be unique integer multiples of 5 dividing 1440.")
    for p in g["periods"]:
        c._text(p["start"], "start")
        c._text(p["end"], "end")
        if type(p["warmup_bars"]) is not int or not 100 <= p["warmup_bars"] <= 5000:
            raise ValueError("Fixed warmup_bars must be an integer in 100..5000.")
    count = c._integer(g["expected_run_count"], "expected_run_count", minimum=1)
    if count != len(g["parameter_sets"]) * len(frames) * len(g["periods"]) * len(market["instruments"]):
        raise ValueError("expected_run_count disagrees with declared matrix.")
    c.canonical_json_bytes(raw)
    return g


def resolve(raw):
    """Live generation boundary; only core owns defaults/dependencies/domains."""
    from core.parameter_mode import resolve_fixed_scalars
    from core.engine_v2.runtime_contract import normalize_v2_runtime_field_value
    from core.data_input import _timestamp
    g = validate_structure(raw)
    config, profile = c._validate_strategy(g["strategy"], parquet=True)
    if profile.modes.get("entryAdmission") != "risk_bracket_v1":
        raise ValueError("strategy lacks risk_bracket_v1 capability.")
    specs = {n: {k: deepcopy(v[k]) for k in ("type", "min", "max", "options") if k in v}
             for n, v in config["parameters"].items() if profile.parameter_roles.get(n) != "runtime"}
    sets = []
    for p in g["parameter_sets"]:
        supplied = g["economics"]["base_params"] | p["params"]
        for name, value in supplied.items():
            if name not in specs:
                raise ValueError(f"unknown Fixed parameter: {name}.")
            scalar(value, name, specs[name])
        params, modes = resolve_fixed_scalars(config, supplied, [])
        sets.append({"name": p["name"], "params": params, "variants": modes})
    for p in g["periods"]:
        normalize_v2_runtime_field_value("warmupBars", p["warmup_bars"], user_boundary=True)
        if _timestamp(p["start"]) > _timestamp(p["end"], end=True):
            raise ValueError("period start exceeds inclusive End.")
    resolved = {"parameter_specs": specs, "parameter_sets": sets}
    if raw["schema_version"] == REQUEST_V2:
        from ..filters import effective
        resolved["entry_filters"] = effective(raw, config)
    return config, resolved


def cells(raw, resolved):
    g = raw["generation"]
    return product(resolved["parameter_sets"], g["timeframes_minutes"], g["periods"], g["market_data"]["instruments"])


def load(path):
    raw = deepcopy(load_json(Path(path), "Fixed request"))
    config, resolved = resolve(raw)
    return raw, config, resolved
