"""Offline Parquet request validation and source-aware admission planning."""
from __future__ import annotations

import io
from contextlib import redirect_stdout

import pandas as pd

from . import config as c
from .rule_contract import rules_v1
from core.engine_v2.admission import admission_cap
from core.engine_v2.parameter_ties import enabled_parameter_ties
from core.engine_v2.runtime_contract import normalize_v2_runtime_field_value
from core.grid_v2 import GridV2Settings, build_grid_v2_plan
from core.walkforward_engine import build_calendar_month_windows
from data_provider.manifest import normalize_instrument_id
from data_provider.errors import DataProviderError

SCHEMA = "strategy_lab_runspec_v2"
DESCRIPTIVE = "strategy_lab_descriptive_v1"


def calendar(generation):
    """Plan the entire declared calendar on the UTC epoch grid, without data I/O."""
    w = generation["windows"]
    step = pd.Timedelta(minutes=w["timeframe_minutes"])
    start = pd.Timestamp(w["requested_start"], tz="UTC").ceil(step)
    end = pd.Timestamp(normalize_v2_runtime_field_value("end", w["requested_end"])).floor(step)
    index = pd.date_range(start, end, freq=step)
    with redirect_stdout(io.StringIO()):
        windows = tuple(build_calendar_month_windows(index, start, end,
            w["is_period_months"], w["oos_period_months"]))
    if len(windows) != w["expected_window_count"]:
        raise c.StrategyLabConfigError("calendar count disagrees with expected_window_count.")
    from .data_quality import expected_complete_oos_end
    for window in windows:
        expected_complete_oos_end(window, oos_period_months=w["oos_period_months"],
                                  timeframe_minutes=w["timeframe_minutes"])
    return windows


def validate_scopes(prereg, market, window_count):
    c._keys(prereg, "preregistration", {"analysis_contract", "analysis_scopes", "rule_registry", "seed", "random_control_draws"})
    if prereg["analysis_contract"] != DESCRIPTIVE or c.canonical_json_bytes(prereg["rule_registry"]) != c.canonical_json_bytes(rules_v1()):
        raise c.StrategyLabConfigError("unsupported descriptive analysis contract or rule registry.")
    if type(prereg["seed"]) is not int:
        raise c.StrategyLabConfigError("seed must be an integer, not boolean.")
    c._integer(prereg["random_control_draws"], "random_control_draws", minimum=1)
    scopes = prereg["analysis_scopes"]
    if not isinstance(scopes, list) or not scopes:
        raise c.StrategyLabConfigError("analysis_scopes must be a nonempty list.")
    cells = {entry["cell"] for entry in market["instruments"]}
    names = set()
    for scope in scopes:
        c._keys(c._object(scope, "scope"), "scope", {"name", "ticker_cell", "window_numbers", "requires_unlock"})
        name = c._text(scope["name"], "scope.name")
        if name in names or scope["ticker_cell"] not in cells or type(scope["requires_unlock"]) is not bool:
            raise c.StrategyLabConfigError("duplicate scope name, unknown cell or invalid unlock flag.")
        names.add(name)
        ids = scope["window_numbers"]
        if not isinstance(ids, list) or not ids:
            raise c.StrategyLabConfigError("scope.window_numbers must be nonempty.")
        for value in ids:
            c._integer(value, "scope.window_numbers", minimum=1)
        if ids != sorted(set(ids)) or max(ids) > window_count:
            raise c.StrategyLabConfigError("scope.window_numbers must be unique, ordered and in range.")


def validate_saved_request(raw):
    """Validate the supported persisted contract once, without current execution eligibility."""
    c._keys(raw, "run spec", {"schema_version", "run_name", "generation", "preregistration"})
    if raw["schema_version"] != SCHEMA:
        raise c.StrategyLabConfigError("unsupported Parquet request schema.")
    c._text(raw["run_name"], "run_name")
    g = c._object(raw["generation"], "generation")
    c._keys(g, "generation", {"strategy", "market_data", "windows", "economics", "execution", "planning", "resources"})
    strategy = c._object(g["strategy"], "strategy")
    c._keys(strategy, "generation.strategy", {"strategy_id", "strategy_version", "engine",
        "grid_v2_engine_version", "plan_identity_schema_version", "semantic_identity_schema_version",
        "runtime_contract_version", "planning_policy_version", "allocator_version"})
    for name, value in strategy.items():
        c._text(value, f"generation.strategy.{name}")
    if strategy["engine"] != "v2":
        raise c.StrategyLabConfigError("generation.strategy.engine: expected 'v2'.")
    execution = c._object(g["execution"], "execution")
    c._keys(execution, "execution", {"policy", "entry_filters"})
    if execution != {"policy": "risk_bracket_v1", "entry_filters": []}:
        raise c.StrategyLabConfigError("only risk_bracket_v1 with explicit entry_filters=[] (Off) is supported.")
    market = c._object(g["market_data"], "market_data")
    c._keys(market, "market_data", {"source_type", "timeframe_minutes", "instruments"})
    timeframe = c._integer(market["timeframe_minutes"], "timeframe_minutes", minimum=1)
    if market["source_type"] != "parquet" or timeframe % 5 or 1440 % timeframe:
        raise c.StrategyLabConfigError("Parquet timeframe must be a multiple of 5 dividing 1440.")
    roster = market["instruments"]
    if not isinstance(roster, list) or not roster:
        raise c.StrategyLabConfigError("instruments must be a nonempty ordered list.")
    seen = set()
    for entry in roster:
        c._keys(c._object(entry, "instrument"), "instrument", {"instrument_id", "cell"})
        instrument = c._text(entry["instrument_id"], "instrument_id")
        try:
            normalized = normalize_instrument_id(instrument)
        except DataProviderError as exc:
            raise c.StrategyLabConfigError(str(exc)) from exc
        if instrument != normalized or instrument.endswith(".") or instrument in seen:
            raise c.StrategyLabConfigError("instrument IDs must be exact normalized unique IDs without trailing dots.")
        seen.add(instrument)
        c._text(entry["cell"], "cell")
    w = c._object(g["windows"], "windows")
    c._keys(w, "windows", {"timeframe_minutes", "period_unit", "is_period_months", "oos_period_months", "calendar_anchor_day", "requested_start", "requested_end", "expected_window_count", "warmup_bars"})
    for key in ("timeframe_minutes", "is_period_months", "oos_period_months", "calendar_anchor_day", "warmup_bars"):
        c._integer(w[key], f"windows.{key}", minimum=1)
    c._integer(w["expected_window_count"], "expected_window_count", minimum=2)
    start, end = (c._date(w[key], key) for key in ("requested_start", "requested_end"))
    if (w["period_unit"] != "months" or w["timeframe_minutes"] != timeframe or
        w["calendar_anchor_day"] > 28 or start >= end or
        start.day != w["calendar_anchor_day"] or end.day != start.day):
        raise c.StrategyLabConfigError("invalid calendar contract.")
    windows = calendar(g)
    economics = c._object(g["economics"], "economics")
    c._keys(economics, "economics", {"base_params", "tick_size_provenance", "slippage_modelled", "funding_modelled"})
    if economics["slippage_modelled"] is not False or economics["funding_modelled"] is not False:
        raise c.StrategyLabConfigError("slippage and funding must be false.")
    tick = c._object(economics["tick_size_provenance"], "tick_size_provenance")
    c._keys(tick, "tick_size_provenance", {"tickSize", "operative", "reason"})
    c._number(tick["tickSize"], "tickSize", positive=True)
    if tick["operative"] is not False or tick["reason"] != "priceRounding=none":
        raise c.StrategyLabConfigError("unsupported tick provenance.")
    base = c._object(economics["base_params"], "base_params")
    required = {"initialCapital", "commissionPct", "riskPerTrade", "enableLong", "enableShort", "maxEntryLeverage"}
    if not required <= set(base):
        raise c.StrategyLabConfigError("base_params must explicitly freeze capital, fees, risk, directions and maxEntryLeverage.")
    for name, value in base.items():
        c._text(name, "base_params key")
        saved_scalar(value, name)
    for name in ("initialCapital", "commissionPct", "riskPerTrade", "maxEntryLeverage"):
        c._number(base[name], name, positive=name == "maxEntryLeverage")
    for name in ("enableLong", "enableShort"):
        if type(base[name]) is not bool:
            raise c.StrategyLabConfigError(f"{name}: expected boolean.")
    resources = c._object(g["resources"], "resources")
    c._keys(resources, "resources", {"outer_workers", "numba_threads", "grid_v2_max_cache_mb"})
    for name in ("outer_workers", "numba_threads"):
        if c._integer(resources[name], name, minimum=1) != 1:
            raise c.StrategyLabConfigError(f"{name} must be 1.")
    c._number(resources["grid_v2_max_cache_mb"], "grid_v2_max_cache_mb", positive=True)
    p = c._object(g["planning"], "planning")
    c._keys(p, "planning", {"enabled_variants", "enabled_axes", "axis_values", "planning_policy", "grid_v2_prefer_compiled", "expected_candidate_count", "expected_effective_planning_policy"}, optional=frozenset({"enabled_tie_groups"}))
    c._string_list(p["enabled_variants"], "enabled_variants")
    axes = c._string_list(p["enabled_axes"], "enabled_axes", allow_empty=True)
    c._string_list(p.get("enabled_tie_groups", []), "enabled_tie_groups", allow_empty=True)
    values = c._object(p["axis_values"], "axis_values")
    if set(values) != set(axes):
        raise c.StrategyLabConfigError("axis_values must match independent enabled_axes exactly.")
    for name in axes:
        if not isinstance(values[name], list) or not values[name]:
            raise c.StrategyLabConfigError(f"axis {name} requires a nonempty domain assertion.")
        for value in values[name]:
            saved_scalar(value, name)
    if p["planning_policy"] != "full" or p["expected_effective_planning_policy"] != "full" or p["grid_v2_prefer_compiled"] is not True:
        raise c.StrategyLabConfigError("full compiled planning is required.")
    c._integer(p["expected_candidate_count"], "expected_candidate_count", minimum=1)
    validate_scopes(c._object(raw["preregistration"], "preregistration"), market, w["expected_window_count"])
    c.canonical_json_bytes(raw)
    return windows


def saved_scalar(value, name):
    """Persisted parameters have JSON scalar values, independent of today's config."""
    if type(value) is bool:
        return
    if isinstance(value, (int, float)):
        c._number(value, name)
    elif isinstance(value, str):
        c._text(value, name)
    else:
        raise c.StrategyLabConfigError(f"{name}: expected a finite JSON scalar.")


def validate_request(raw):
    """Generation eligibility adds current pins, profile, bounds and registry checks."""
    validate_saved_request(raw)
    g = raw["generation"]
    config, profile = c._validate_strategy(g["strategy"], parquet=True)
    if profile.modes.get("entryAdmission") != "risk_bracket_v1":
        raise c.StrategyLabConfigError("Parquet Lab requires the risk_bracket_v1 capability.")
    if raw["preregistration"]["rule_registry"] != c.RULE_REGISTRY:
        raise c.StrategyLabConfigError("rule registry disagrees with current generation registry.")
    base, specs = g["economics"]["base_params"], config["parameters"]
    for name, value in base.items():
        if name not in specs or profile.parameter_roles.get(name) == "runtime":
            raise c.StrategyLabConfigError(f"invalid fixed parameter: {name}.")
        scalar(value, specs[name], name)
    admission_cap(profile, base)
    p = g["planning"]
    if any(name not in profile.variants for name in p["enabled_variants"]):
        raise c.StrategyLabConfigError("unknown enabled variant.")
    pairs = enabled_parameter_ties(config, p.get("enabled_tie_groups", []))
    if set(p["enabled_axes"]) & {target for _, target in pairs}:
        raise c.StrategyLabConfigError("axis_values must match independent enabled_axes exactly.")
    for name, values in p["axis_values"].items():
        if name not in specs or not specs[name].get("optimize", {}).get("enabled") or name == "maxEntryLeverage":
            raise c.StrategyLabConfigError(f"unsupported axis: {name}.")
        for value in values:
            scalar(value, specs[name], name)
    return config, profile


def scalar(value, spec, name):
    kind = spec.get("type")
    if kind == "bool":
        if type(value) is not bool:
            raise c.StrategyLabConfigError(f"{name}: expected boolean.")
    elif kind == "int":
        if type(value) is not int:
            raise c.StrategyLabConfigError(f"{name}: expected integer.")
    elif kind == "float":
        c._number(value, name)
    elif not isinstance(value, str):
        raise c.StrategyLabConfigError(f"{name}: expected string.")
    if spec.get("options") is not None and value not in spec["options"]:
        raise c.StrategyLabConfigError(f"{name}: unknown option.")
    # The admission owner accepts every positive finite cap, independently of UI bounds.
    if name != "maxEntryLeverage" and (("min" in spec and value < spec["min"]) or ("max" in spec and value > spec["max"])):
        raise c.StrategyLabConfigError(f"{name}: outside registered bounds.")


def build_plan(spec, rules):
    g = spec.generation
    config, profile = c._validate_strategy(g["strategy"], parquet=True)
    p = g["planning"]
    internal = profile.variant_selector is not None and not profile.variant_selector.user_facing
    settings = GridV2Settings(enabled_variants=None if internal else tuple(p["enabled_variants"]),
        enabled_axes=tuple(p["enabled_axes"]), enabled_tie_groups=tuple(p.get("enabled_tie_groups", [])),
        prefer_compiled=True, slow_enrich_selected=False, compiled_workers=1,
        max_signal_cache_mb=float(g["resources"]["grid_v2_max_cache_mb"]),
        planning_policy="full", admission_rules=rules)
    plan = build_grid_v2_plan(config, settings, g["economics"]["base_params"])
    if (plan.deduped_candidate_count != p["expected_candidate_count"] or
        tuple(plan.candidate_table.variant_names) != tuple(p["enabled_variants"]) or
        plan.metadata.get("planning", {}).get("effective_policy") != "full"):
        raise c.StrategyLabConfigError("source-aware plan count, variants or policy mismatch.")
    for name, values in p["axis_values"].items():
        if list(plan.parameter_domains[name].values) != values:
            raise c.StrategyLabConfigError(f"axis_values.{name} disagrees with registered Grid domain.")
    if any(d.get("severity") in {"warning", "error"} for d in plan.metadata.get("diagnostics", ())):
        raise c.StrategyLabConfigError("source-aware plan contains warning/error diagnostics.")
    return plan
