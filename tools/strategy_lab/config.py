"""Strict, portable Strategy Lab Phase 0 run-spec validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from core.engine_v2.profile import ProfileValidationError, parse_execution_profile  # noqa: E402
from core.engine_v2.parameter_ties import enabled_parameter_ties  # noqa: E402
from core.engine_v2.runtime_contract import V2_RUNTIME_CONTRACT_VERSION  # noqa: E402
from core.grid_v2 import (  # noqa: E402
    GRID_V2_ENGINE_VERSION,
    GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION,
    GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
    GridV2Plan,
    GridV2Settings,
    build_grid_v2_plan,
)
from core.grid_v2_sampling import (  # noqa: E402
    GRID_V2_ALLOCATOR_VERSION,
    GRID_V2_PLANNING_POLICY_VERSION,
)
from strategies import get_strategy_config  # noqa: E402


RUNSPEC_SCHEMA_VERSION = "strategy_lab_runspec_v1"
INVENTORY_SCHEMA_VERSION = "strategy_lab_inventory_v1"
RULES_REGISTRY_VERSION = "strategy_lab_rules_v1"
EVIDENCE_CRITERIA_VERSION = "strategy_lab_evidence_v1"
DATA_ROOT_ENV_VAR = "MERLIN_STRATEGY_LAB_DATA_ROOT"
FILENAME_INTERVAL_BOUNDARY = "inclusive_utc_days"


ANALYSIS_SCOPES = (
    {
        "name": "development",
        "ticker_cell": "dev",
        "window_numbers": [1, 2, 3, 4, 5, 6],
        "requires_unlock": False,
    },
    {
        "name": "holdout_cross_sectional",
        "ticker_cell": "holdout",
        "window_numbers": [1, 2, 3, 4, 5, 6],
        "requires_unlock": True,
    },
    {
        "name": "temporal_dev",
        "ticker_cell": "dev",
        "window_numbers": [7, 8],
        "requires_unlock": True,
    },
    {
        "name": "temporal_combined",
        "ticker_cell": "holdout",
        "window_numbers": [7, 8],
        "requires_unlock": True,
    },
)

OBSERVATION_CONTRACT = {
    "candidate_observation": "candidate_ticker_window",
    "monthly_ticker_aggregation": "equal_weight_valid_tickers",
    "six_month_headline_aggregation": "equal_weight_six_monthly_means",
    "headline_median": "all_valid_ticker_window_observations",
    "missing_required_value": "fail_or_mark_rule_pair_unavailable_with_count",
    "natural_zero_trade_net_profit_pct": 0,
    "natural_zero_trade_is_profitable": False,
    "natural_zero_trade_population_membership": "included",
    "unavailable_non_finite_metric": "excluded_from_selection_not_zero_filled",
}

EVIDENCE_CRITERIA = {
    "primary_confirmation": {
        "cell": "holdout_cross_sectional",
        "description": (
            "Cross-sectional confirmation on new tickers over the same six calendar "
            "months as development; it is not independent temporal confirmation."
        ),
    },
    "broad_population_edge": {
        "median_candidate_oos_net_profit_pct": {"operator": ">", "value": 0},
        "profitable_observation_share": {"operator": ">", "value": 0.5},
        "positive_monthly_population_medians": {"operator": ">=", "value": 4, "of": 6},
    },
    "selected_strategy_viability": {
        "six_month_headline_mean_oos_net_profit_pct": {"operator": ">", "value": 0},
        "median_selected_oos_net_profit_pct": {"operator": ">", "value": 0},
        "profitable_selected_observation_share": {"operator": ">", "value": 0.5},
        "positive_monthly_selected_means": {"operator": ">=", "value": 4, "of": 6},
    },
    "selection_lift": {
        "comparison": "rule_selected_minus_primary_profit_oos_net_profit_pct",
        "six_month_equal_weight_monthly_mean": {"operator": ">", "value": 0},
        "positive_monthly_top1_paired_means": {"operator": ">=", "value": 4, "of": 6},
        "top5_equal_weight_monthly_mean": {"operator": ">=", "value": 0},
        "robust_top1_headline_mean": {"operator": ">", "value": 0},
    },
    "outlier_procedure": {
        "contribution": "ticker_mean_top1_paired_lift_over_six_windows",
        "removal_candidates": "strictly_positive_contributors_only",
        "quota": "ceil(0.10*total_ticker_count_in_cell)",
        "remove_count": "min(quota,positive_contributor_count)",
        "order": ["mean_contribution_desc", "canonical_symbol_asc"],
        "recompute": "monthly_means_then_equal_weight_six_month_headline",
        "criterion_scope": "recomputed_headline_only_monthly_signs_descriptive",
    },
    "nomination": {
        "requires": ["selected_strategy_viability", "selection_lift"],
        "maximum_rules": 3,
        "multiple_nomination_warning_required": True,
        "fixed_minimum_lift_percentage_points": None,
    },
    "uncertainty": {
        "type": "descriptive_month_block_bootstrap",
        "blocks": 6,
        "resamples": 10000,
        "sample_size_per_resample": 6,
        "sampling": "with_replacement",
        "seed": 20260811,
        "percentiles": [2.5, 97.5],
        "pass_fail_criterion": False,
    },
    "temporal_windows_7_8": {
        "mandatory_final_mvp_check": True,
        "descriptive_only": True,
        "bootstrap_interval": False,
        "pass_fail_threshold": False,
    },
}

RULE_REGISTRY = {
    "version": RULES_REGISTRY_VERSION,
    "minimum_completed_trades": 15,
    "baseline_rule": "primary_profit",
    "selectable_rules": [
        "primary_profit",
        "trade_gate15_profit",
        "trade_gate15_profit_factor",
        "trade_gate15_daily_sharpe",
        "trade_gate15_romad_mtm",
        "star_mean_profit",
        "star_worst_profit",
        "balanced_percentile_raw",
        "balanced_percentile_star",
    ],
    "nomination_eligible_rules": [
        "trade_gate15_profit",
        "trade_gate15_profit_factor",
        "trade_gate15_daily_sharpe",
        "trade_gate15_romad_mtm",
        "star_mean_profit",
        "star_worst_profit",
        "balanced_percentile_raw",
        "balanced_percentile_star",
    ],
    "non_nominatable_diagnostics": [
        "trade_gate15_romad_realized",
        "balanced_percentile_raw_realized",
        "balanced_percentile_star_realized",
        "pareto_plus_primary",
        "population_no_skill",
        "oos_oracle",
        "oos_anti_oracle",
    ],
    "tie_break": [
        {"field": "rule_score", "direction": "descending"},
        {"field": "IS net_profit_pct", "direction": "descending"},
        {"field": "semantic_key", "direction": "ascending"},
        {"field": "candidate_id", "direction": "ascending"},
    ],
}


class StrategyLabConfigError(ValueError):
    """Concise ordinary-input failure raised by the Phase 0 loader."""


@dataclass(frozen=True)
class RunSpec:
    """Normalized, identity-checked Strategy Lab run specification."""

    source_path: Path
    raw: Mapping[str, Any]
    generation: Mapping[str, Any]
    preregistration: Mapping[str, Any]
    generation_sha256: str
    pre_registration_sha256: str
    inventory_path: Path | None
    inventory: Any | None
    plan: GridV2Plan | None
    plan_build_seconds: float

    @property
    def strategy_id(self) -> str:
        return str(self.generation["strategy"]["strategy_id"])


def canonical_json_bytes(obj: Any) -> bytes:
    """Return the single canonical JSON representation used by Strategy Lab."""

    try:
        payload = json.dumps(
            obj,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise StrategyLabConfigError(f"canonical JSON: {exc}") from None
    return payload.encode("utf-8")


def canonical_sha256(obj: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(obj)).hexdigest()


def semantic_key_digest(plan: GridV2Plan) -> str:
    digest = hashlib.sha256()
    for index in range(plan.deduped_candidate_count):
        digest.update(plan.candidate_table.semantic_key_for_index(index).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StrategyLabConfigError(f"{field}: expected an object.")
    return value


def _keys(
    value: Mapping[str, Any], field: str, expected: set[str],
    *, optional: frozenset[str] = frozenset(),
) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - (expected | optional))
    if missing:
        raise StrategyLabConfigError(f"{field}: missing required field(s): {', '.join(missing)}.")
    if unknown:
        raise StrategyLabConfigError(f"{field}: unknown field(s): {', '.join(unknown)}.")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StrategyLabConfigError(f"{field}: expected a non-empty string.")
    return value


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise StrategyLabConfigError(f"{field}: expected an integer >= {minimum} (booleans are invalid).")
    return value


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StrategyLabConfigError(f"{field}: expected a finite number (booleans are invalid).")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0):
        qualifier = "positive " if positive else ""
        raise StrategyLabConfigError(f"{field}: expected a finite {qualifier}number.")
    return result


def _date(value: Any, field: str) -> date:
    text = _text(value, field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        raise StrategyLabConfigError(f"{field}: expected YYYY-MM-DD.") from None
    if parsed.isoformat() != text:
        raise StrategyLabConfigError(f"{field}: expected canonical YYYY-MM-DD.")
    return parsed


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise StrategyLabConfigError(f"{field}: expected a lowercase SHA-256 hex digest.")
    return text


def _string_list(value: Any, field: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise StrategyLabConfigError(f"{field}: expected {'a' if not allow_empty else 'an'} ordered string list.")
    result = tuple(_text(item, f"{field}[{index}]") for index, item in enumerate(value))
    if len(set(result)) != len(result):
        raise StrategyLabConfigError(f"{field}: duplicate values are invalid.")
    return result


def _months_between(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + end.month - start.month


def complete_calendar_window_count(
    start: date,
    end: date,
    is_period_months: int,
    oos_period_months: int,
) -> int:
    """Count complete rolling calendar windows under Merlin's OOS step."""

    months = _months_between(start, end)
    required = is_period_months + oos_period_months
    if months < required:
        return 0
    return (months - required) // oos_period_months + 1


def _load_json_object(path: Path, field: str) -> Mapping[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except json.JSONDecodeError as exc:
        raise StrategyLabConfigError(f"{field}: malformed JSON at line {exc.lineno}, column {exc.colno}.") from None
    except OSError as exc:
        raise StrategyLabConfigError(f"{field}: cannot read {path}: {exc}") from None
    return _object(value, field)


def _validate_strategy(strategy: Mapping[str, Any]) -> tuple[Mapping[str, Any], Any]:
    expected = {
        "strategy_id",
        "strategy_version",
        "engine",
        "grid_v2_engine_version",
        "plan_identity_schema_version",
        "semantic_identity_schema_version",
        "runtime_contract_version",
        "planning_policy_version",
        "allocator_version",
    }
    _keys(strategy, "generation.strategy", expected)
    strategy_id = _text(strategy["strategy_id"], "generation.strategy.strategy_id")
    try:
        config = get_strategy_config(strategy_id)
    except ValueError:
        raise StrategyLabConfigError(
            f"generation.strategy.strategy_id: '{strategy_id}' is not registered in Merlin."
        ) from None
    if str(config.get("engine", "v1")).lower() != "v2":
        raise StrategyLabConfigError(
            f"generation.strategy.strategy_id: '{strategy_id}' is Backtester V1; Strategy Lab requires a certified Backtester V2 strategy."
        )
    if strategy["engine"] != "v2":
        raise StrategyLabConfigError("generation.strategy.engine: expected 'v2'.")
    if strategy["strategy_version"] != config.get("version"):
        raise StrategyLabConfigError(
            "generation.strategy.strategy_version: does not match the registered strategy config."
        )
    versions = {
        "grid_v2_engine_version": GRID_V2_ENGINE_VERSION,
        "plan_identity_schema_version": GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION,
        "semantic_identity_schema_version": GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
        "runtime_contract_version": V2_RUNTIME_CONTRACT_VERSION,
        "planning_policy_version": GRID_V2_PLANNING_POLICY_VERSION,
        "allocator_version": GRID_V2_ALLOCATOR_VERSION,
    }
    for name, expected_value in versions.items():
        if strategy[name] != expected_value:
            raise StrategyLabConfigError(
                f"generation.strategy.{name}: expected '{expected_value}', got {strategy[name]!r}."
            )
    try:
        profile = parse_execution_profile(config)
    except (ProfileValidationError, ValueError, TypeError) as exc:
        raise StrategyLabConfigError(
            f"generation.strategy.strategy_id: V2 profile validation failed: {exc}"
        ) from None
    if profile.engine != "v2":
        raise StrategyLabConfigError("generation.strategy.strategy_id: resolved profile is not V2.")
    if profile.modes.get("entryAdmission"):
        raise StrategyLabConfigError("Strategy Lab CSV execution does not support risk_bracket_v1; use Merlin Parquet execution.")
    return config, profile


def _validate_plan(
    config: Mapping[str, Any],
    generation: Mapping[str, Any],
    profile: Any,
) -> GridV2Plan:
    planning = _object(generation["planning"], "generation.planning")
    _keys(
        planning,
        "generation.planning",
        {
            "enabled_variants",
            "enabled_axes",
            "axis_values",
            "planning_policy",
            "grid_v2_prefer_compiled",
            "expected_candidate_count",
            "expected_effective_planning_policy",
            "expected_plan_fingerprint",
            "expected_semantic_key_digest",
        },
        optional=frozenset({"enabled_tie_groups"}),
    )
    ties = _string_list(planning.get("enabled_tie_groups", []), "generation.planning.enabled_tie_groups", allow_empty=True)
    try:
        enabled_parameter_ties(config, ties)
    except ValueError as exc:
        raise StrategyLabConfigError(f"generation.planning.enabled_tie_groups: {exc}") from None
    variants = _string_list(planning["enabled_variants"], "generation.planning.enabled_variants")
    axes = _string_list(planning["enabled_axes"], "generation.planning.enabled_axes", allow_empty=True)
    axis_values = _object(planning["axis_values"], "generation.planning.axis_values")
    if set(axis_values) != set(axes):
        raise StrategyLabConfigError(
            "generation.planning.axis_values: keys must exactly match enabled_axes."
        )
    for name in axes:
        values = axis_values[name]
        if not isinstance(values, list) or not values:
            raise StrategyLabConfigError(f"generation.planning.axis_values.{name}: expected a non-empty list.")
        for index, value in enumerate(values):
            if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                raise StrategyLabConfigError(
                    f"generation.planning.axis_values.{name}[{index}]: invalid axis value."
                )
    if planning["planning_policy"] != "full":
        raise StrategyLabConfigError("generation.planning.planning_policy: Phase 0 requires 'full'.")
    if planning["expected_effective_planning_policy"] != "full":
        raise StrategyLabConfigError(
            "generation.planning.expected_effective_planning_policy: expected 'full'."
        )
    if not isinstance(planning["grid_v2_prefer_compiled"], bool):
        raise StrategyLabConfigError(
            "generation.planning.grid_v2_prefer_compiled: expected a boolean."
        )
    expected_count = _integer(
        planning["expected_candidate_count"],
        "generation.planning.expected_candidate_count",
        minimum=1,
    )
    expected_fingerprint = _sha256(
        planning["expected_plan_fingerprint"],
        "generation.planning.expected_plan_fingerprint",
    )
    expected_semantic = _sha256(
        planning["expected_semantic_key_digest"],
        "generation.planning.expected_semantic_key_digest",
    )
    economics = _object(generation["economics"], "generation.economics")
    base_params = _object(economics["base_params"], "generation.economics.base_params")
    resources = _object(generation["resources"], "generation.resources")
    runtime_names = {"dateFilter", "start", "end", "warmupBars"}
    if runtime_names.intersection(base_params):
        raise StrategyLabConfigError(
            "generation.economics.base_params: runtime/window fields are not base parameters."
        )
    internal_selector = profile.variant_selector is not None and not profile.variant_selector.user_facing
    settings = GridV2Settings(
        enabled_variants=None if internal_selector else variants,
        enabled_tie_groups=ties,
        enabled_axes=axes,
        prefer_compiled=planning["grid_v2_prefer_compiled"],
        slow_enrich_selected=False,
        compiled_workers=1,
        max_signal_cache_mb=_number(
            resources["grid_v2_max_cache_mb"],
            "generation.resources.grid_v2_max_cache_mb",
            positive=True,
        ),
        planning_policy="full",
    )
    try:
        plan = build_grid_v2_plan(config, settings, base_params)
    except (ValueError, TypeError, KeyError) as exc:
        raise StrategyLabConfigError(f"generation.planning: V2 plan construction failed: {exc}") from None
    if internal_selector and variants != plan.candidate_table.variant_names:
        raise StrategyLabConfigError(
            "generation.planning.enabled_variants: declared variants "
            f"{variants!r} disagree with resolved internal variants {plan.candidate_table.variant_names!r}."
        )
    effective = plan.metadata.get("planning", {}).get("effective_policy")
    if plan.deduped_candidate_count != expected_count:
        raise StrategyLabConfigError(
            f"generation.planning.expected_candidate_count: expected {expected_count}, rebuilt {plan.deduped_candidate_count}."
        )
    if effective != planning["expected_effective_planning_policy"]:
        raise StrategyLabConfigError(
            f"generation.planning.expected_effective_planning_policy: expected {planning['expected_effective_planning_policy']!r}, rebuilt {effective!r}."
        )
    if plan.plan_fingerprint != expected_fingerprint:
        raise StrategyLabConfigError(
            "generation.planning.expected_plan_fingerprint: rebuilt plan fingerprint mismatch."
        )
    digest = semantic_key_digest(plan)
    if digest != expected_semantic:
        raise StrategyLabConfigError(
            "generation.planning.expected_semantic_key_digest: rebuilt semantic digest mismatch."
        )
    invalid_diagnostics = [
        item
        for item in plan.metadata.get("diagnostics", ())
        if item.get("severity") in {"warning", "error"}
    ]
    if invalid_diagnostics:
        levels = ", ".join(
            f"{item.get('severity')}:{item.get('code')}" for item in invalid_diagnostics
        )
        raise StrategyLabConfigError(
            f"generation.planning: rebuilt plan contains warning/error diagnostic(s): {levels}."
        )
    for name in axes:
        rebuilt = list(plan.parameter_domains[name].values)
        if rebuilt != list(axis_values[name]):
            raise StrategyLabConfigError(
                f"generation.planning.axis_values.{name}: declared {axis_values[name]!r}, rebuilt {rebuilt!r}."
            )
    execution = _object(generation["execution"], "generation.execution")
    for variant_name in variants:
        variant = plan.profile.variants.get(variant_name)
        if variant is None:
            raise StrategyLabConfigError(
                f"generation.planning.enabled_variants: unsupported variant '{variant_name}'."
            )
        if dict(variant.modes) != dict(execution):
            raise StrategyLabConfigError(
                f"generation.execution: does not match resolved V2 variant '{variant_name}'."
            )
    return plan


def _validate_common(generation: Mapping[str, Any], prereg: Mapping[str, Any]) -> None:
    _keys(
        generation,
        "generation",
        {"strategy", "inventory", "data_root", "market_data", "windows", "economics", "execution", "planning", "resources"},
    )
    data_root = _object(generation["data_root"], "generation.data_root")
    _keys(data_root, "generation.data_root", {"environment_variable", "precedence", "search_fallback", "absolute_path_tracked"})
    if data_root != {
        "environment_variable": DATA_ROOT_ENV_VAR,
        "precedence": ["explicit", "environment"],
        "search_fallback": False,
        "absolute_path_tracked": False,
    }:
        raise StrategyLabConfigError("generation.data_root: unsupported portable root contract.")

    market = _object(generation["market_data"], "generation.market_data")
    _keys(market, "generation.market_data", {"timeframe_minutes", "filename_start", "filename_end", "filename_interval", "expected_header", "expected_exchange_counts"})
    _integer(market["timeframe_minutes"], "generation.market_data.timeframe_minutes", minimum=1)
    filename_start = _date(market["filename_start"], "generation.market_data.filename_start")
    filename_end = _date(market["filename_end"], "generation.market_data.filename_end")
    if filename_start > filename_end or market["filename_interval"] != FILENAME_INTERVAL_BOUNDARY:
        raise StrategyLabConfigError(
            "generation.market_data: filename dates must be inclusive UTC days with start <= end."
        )
    if market["expected_header"] != ["time", "open", "high", "low", "close", "Volume"]:
        raise StrategyLabConfigError("generation.market_data.expected_header: unsupported CSV schema.")
    exchange_counts = _object(market["expected_exchange_counts"], "generation.market_data.expected_exchange_counts")
    for name, count in exchange_counts.items():
        _text(name, "generation.market_data.expected_exchange_counts key")
        _integer(count, f"generation.market_data.expected_exchange_counts.{name}")

    windows = _object(generation["windows"], "generation.windows")
    _keys(windows, "generation.windows", {"timeframe_minutes", "period_unit", "is_period_months", "oos_period_months", "calendar_anchor_day", "requested_start", "requested_end", "expected_window_count", "warmup_bars"})
    if windows["period_unit"] != "months":
        raise StrategyLabConfigError("generation.windows.period_unit: expected 'months'.")
    if windows["timeframe_minutes"] != market["timeframe_minutes"]:
        raise StrategyLabConfigError("generation.windows.timeframe_minutes: must match market_data.")
    is_months = _integer(windows["is_period_months"], "generation.windows.is_period_months", minimum=1)
    oos_months = _integer(windows["oos_period_months"], "generation.windows.oos_period_months", minimum=1)
    anchor = _integer(windows["calendar_anchor_day"], "generation.windows.calendar_anchor_day", minimum=1)
    if anchor > 28:
        raise StrategyLabConfigError("generation.windows.calendar_anchor_day: expected 1..28.")
    start = _date(windows["requested_start"], "generation.windows.requested_start")
    end = _date(windows["requested_end"], "generation.windows.requested_end")
    if start >= end or start.day != anchor or end.day != anchor:
        raise StrategyLabConfigError("generation.windows: dates must increase and use the calendar anchor day.")
    expected_windows = _integer(windows["expected_window_count"], "generation.windows.expected_window_count", minimum=0)
    calculated = complete_calendar_window_count(start, end, is_months, oos_months)
    if calculated != expected_windows:
        raise StrategyLabConfigError(
            f"generation.windows.expected_window_count: expected {calculated} from the declared calendar contract."
        )
    _integer(windows["warmup_bars"], "generation.windows.warmup_bars", minimum=1)

    economics = _object(generation["economics"], "generation.economics")
    _keys(economics, "generation.economics", {"base_params", "tick_size_provenance", "slippage_modelled", "funding_modelled"})
    if economics["slippage_modelled"] is not False or economics["funding_modelled"] is not False:
        raise StrategyLabConfigError("generation.economics: slippage and funding must be explicitly false.")
    tick = _object(economics["tick_size_provenance"], "generation.economics.tick_size_provenance")
    _keys(tick, "generation.economics.tick_size_provenance", {"tickSize", "operative", "reason"})
    _number(tick["tickSize"], "generation.economics.tick_size_provenance.tickSize", positive=True)
    if tick["operative"] is not False or tick["reason"] != "priceRounding=none":
        raise StrategyLabConfigError("generation.economics.tick_size_provenance: unsupported provenance contract.")

    resources = _object(generation["resources"], "generation.resources")
    _keys(resources, "generation.resources", {"outer_workers", "numba_threads", "grid_v2_max_cache_mb"})
    outer_workers = _integer(
        resources["outer_workers"], "generation.resources.outer_workers", minimum=1
    )
    if outer_workers != 1:
        raise StrategyLabConfigError(
            "generation.resources.outer_workers: Phase 1-A requires exactly 1."
        )
    numba_threads = _integer(
        resources["numba_threads"], "generation.resources.numba_threads", minimum=1
    )
    if numba_threads != 1:
        raise StrategyLabConfigError(
            "generation.resources.numba_threads: Phase 1-A requires exactly 1."
        )
    _number(resources["grid_v2_max_cache_mb"], "generation.resources.grid_v2_max_cache_mb", positive=True)

    _keys(prereg, "preregistration", {"split", "analysis_scopes", "rule_registry", "primary_comparison", "evidence_criteria_version", "maximum_nominated_rules", "observation_contract", "evidence_criteria"})
    if prereg["rule_registry"] != RULE_REGISTRY:
        raise StrategyLabConfigError(
            "preregistration.rule_registry: does not match the frozen strategy_lab_rules_v1 contract."
        )
    if prereg["primary_comparison"] != "top1_oos_net_profit_vs_primary_profit":
        raise StrategyLabConfigError("preregistration.primary_comparison: unsupported comparison.")
    if prereg["evidence_criteria_version"] != EVIDENCE_CRITERIA_VERSION:
        raise StrategyLabConfigError(f"preregistration.evidence_criteria_version: expected '{EVIDENCE_CRITERIA_VERSION}'.")
    if _integer(prereg["maximum_nominated_rules"], "preregistration.maximum_nominated_rules", minimum=1) != 3:
        raise StrategyLabConfigError("preregistration.maximum_nominated_rules: expected 3.")
    scopes = prereg["analysis_scopes"]
    if not isinstance(scopes, list):
        raise StrategyLabConfigError("preregistration.analysis_scopes: expected a list.")
    for scope_index, scope in enumerate(scopes):
        if not isinstance(scope, Mapping):
            raise StrategyLabConfigError(
                f"preregistration.analysis_scopes[{scope_index}]: expected an object."
            )
        numbers = scope.get("window_numbers")
        if not isinstance(numbers, list):
            raise StrategyLabConfigError(
                f"preregistration.analysis_scopes[{scope_index}].window_numbers: expected a list."
            )
        validated = [
            _integer(
                number,
                f"preregistration.analysis_scopes[{scope_index}].window_numbers[{index}]",
                minimum=1,
            )
            for index, number in enumerate(numbers)
        ]
        if len(set(validated)) != len(validated):
            raise StrategyLabConfigError(
                f"preregistration.analysis_scopes[{scope_index}].window_numbers: duplicates are invalid."
            )
        if any(number > expected_windows for number in validated):
            raise StrategyLabConfigError(
                f"preregistration.analysis_scopes[{scope_index}].window_numbers: values must be within 1..expected_window_count."
            )
    if scopes != list(ANALYSIS_SCOPES):
        raise StrategyLabConfigError("preregistration.analysis_scopes: does not match the frozen four-scope contract.")
    if prereg["observation_contract"] != OBSERVATION_CONTRACT:
        raise StrategyLabConfigError("preregistration.observation_contract: does not match the frozen contract.")
    if prereg["evidence_criteria"] != EVIDENCE_CRITERIA:
        raise StrategyLabConfigError("preregistration.evidence_criteria: does not match strategy_lab_evidence_v1.")


def load_run_spec(
    path: str | Path,
    *,
    repo_root: str | Path = REPO_ROOT,
    validate_inventory: bool = True,
    validate_plan: bool = True,
) -> RunSpec:
    source_path = Path(path).resolve()
    raw = _load_json_object(source_path, "run spec")
    _keys(raw, "run spec", {"schema_version", "run_name", "generation", "preregistration"})
    if raw["schema_version"] != RUNSPEC_SCHEMA_VERSION:
        raise StrategyLabConfigError(f"schema_version: expected '{RUNSPEC_SCHEMA_VERSION}'.")
    _text(raw["run_name"], "run_name")
    generation = _object(raw["generation"], "generation")
    prereg = _object(raw["preregistration"], "preregistration")
    _validate_common(generation, prereg)
    config, profile = _validate_strategy(_object(generation["strategy"], "generation.strategy"))

    inventory_contract = _object(generation["inventory"], "generation.inventory")
    _keys(inventory_contract, "generation.inventory", {"inventory_path", "inventory_sha256", "ticker_list_digest", "expected_ticker_count"})
    relative_inventory = Path(_text(inventory_contract["inventory_path"], "generation.inventory.inventory_path"))
    if relative_inventory.is_absolute() or ".." in relative_inventory.parts:
        raise StrategyLabConfigError("generation.inventory.inventory_path: expected a portable repository-relative path.")
    expected_inventory_sha = _sha256(inventory_contract["inventory_sha256"], "generation.inventory.inventory_sha256")
    expected_ticker_digest = _sha256(inventory_contract["ticker_list_digest"], "generation.inventory.ticker_list_digest")
    expected_tickers = _integer(inventory_contract["expected_ticker_count"], "generation.inventory.expected_ticker_count", minimum=1)
    market = _object(generation["market_data"], "generation.market_data")
    expected_exchange_counts = _object(
        market["expected_exchange_counts"],
        "generation.market_data.expected_exchange_counts",
    )
    if sum(expected_exchange_counts.values()) != expected_tickers:
        raise StrategyLabConfigError(
            "generation.market_data.expected_exchange_counts: sum must equal generation.inventory.expected_ticker_count."
        )

    split = _object(prereg["split"], "preregistration.split")
    _keys(split, "preregistration.split", {"algorithm", "development_ticker_count", "holdout_ticker_count"})
    if split["algorithm"] != "sha256_canonical_symbol_ascending_v1":
        raise StrategyLabConfigError("preregistration.split.algorithm: unsupported split algorithm.")
    dev_count = _integer(split["development_ticker_count"], "preregistration.split.development_ticker_count", minimum=1)
    holdout_count = _integer(split["holdout_ticker_count"], "preregistration.split.holdout_ticker_count", minimum=1)
    if dev_count + holdout_count != expected_tickers:
        raise StrategyLabConfigError("preregistration.split: development plus holdout must equal expected_ticker_count.")

    inventory_path: Path | None = None
    inventory = None
    if validate_inventory:
        inventory_path = (Path(repo_root).resolve() / relative_inventory).resolve()
        try:
            inventory_path.relative_to(Path(repo_root).resolve())
        except ValueError:
            raise StrategyLabConfigError("generation.inventory.inventory_path: escapes the repository root.") from None
        from .inventory import load_inventory

        inventory = load_inventory(inventory_path)
        if inventory.sha256 != expected_inventory_sha:
            raise StrategyLabConfigError("generation.inventory.inventory_sha256: inventory canonical digest mismatch.")
        if inventory.ticker_list_digest != expected_ticker_digest:
            raise StrategyLabConfigError("generation.inventory.ticker_list_digest: ordered ticker digest mismatch.")
        if len(inventory.entries) != expected_tickers:
            raise StrategyLabConfigError("generation.inventory.expected_ticker_count: inventory count mismatch.")
        if inventory.development_count != dev_count or inventory.holdout_count != holdout_count:
            raise StrategyLabConfigError("preregistration.split: inventory assignment counts mismatch.")
        inventory_interval = inventory.raw["filename_interval"]
        if market["timeframe_minutes"] != inventory.raw["timeframe_minutes"]:
            raise StrategyLabConfigError(
                "generation.market_data.timeframe_minutes: does not match inventory.timeframe_minutes."
            )
        expected_start = f"{market['filename_start']}T00:00:00Z"
        expected_end = f"{market['filename_end']}T00:00:00Z"
        if inventory_interval["start"] != expected_start:
            raise StrategyLabConfigError(
                "generation.market_data.filename_start: does not match inventory.filename_interval.start."
            )
        if inventory_interval["end"] != expected_end:
            raise StrategyLabConfigError(
                "generation.market_data.filename_end: does not match inventory.filename_interval.end."
            )
        if inventory_interval["boundary"] != market["filename_interval"]:
            raise StrategyLabConfigError(
                "generation.market_data.filename_interval: does not match inventory.filename_interval.boundary."
            )
        if inventory.raw["header"] != market["expected_header"]:
            raise StrategyLabConfigError(
                "generation.market_data.expected_header: does not match inventory.header."
            )
        observed_exchange_counts: dict[str, int] = {}
        for entry in inventory.entries:
            exchange = str(entry["exchange"])
            observed_exchange_counts[exchange] = observed_exchange_counts.get(exchange, 0) + 1
        if observed_exchange_counts != dict(expected_exchange_counts):
            raise StrategyLabConfigError(
                "generation.market_data.expected_exchange_counts: does not match inventory entries."
            )

    plan_started = time.perf_counter()
    plan = _validate_plan(config, generation, profile) if validate_plan else None
    plan_build_seconds = time.perf_counter() - plan_started if validate_plan else 0.0
    return RunSpec(
        source_path=source_path,
        raw=raw,
        generation=generation,
        preregistration=prereg,
        generation_sha256=canonical_sha256(generation),
        pre_registration_sha256=canonical_sha256(raw),
        inventory_path=inventory_path,
        inventory=inventory,
        plan=plan,
        plan_build_seconds=plan_build_seconds,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a Strategy Lab Phase 0 run specification.")
    parser.add_argument("runspec", type=Path)
    args = parser.parse_args(argv)
    try:
        spec = load_run_spec(args.runspec)
    except StrategyLabConfigError as exc:
        parser.exit(2, f"Strategy Lab run-spec error: {exc}\n")
    print(
        json.dumps(
            {
                "run_name": spec.raw["run_name"],
                "strategy_id": spec.strategy_id,
                "candidate_count": spec.plan.deduped_candidate_count if spec.plan else None,
                "inventory_count": len(spec.inventory.entries) if spec.inventory else None,
                "generation_sha256": spec.generation_sha256,
                "pre_registration_sha256": spec.pre_registration_sha256,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
