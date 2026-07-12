"""Generic Backtester V2 Grid backend.

This module plans deterministic V2 candidate spaces and executes candidates
through generic V2 execution contracts. Strategy-specific code supplies only
signal/dataprep hooks.
"""

from __future__ import annotations

import hashlib
import inspect
import itertools
import json
import math
import time
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import numpy as np
import pandas as pd

from core.engine_v2.contracts import ExecutionProfile, GuardrailSummary, VariantSpec
from core.engine_v2.kernel import ExecutionData
from core.engine_v2.compiled_kernel import (
    COMPILED_BATCH_KIND,
    OUTPUT_FINAL_BALANCE,
    OUTPUT_FLAGS,
    OUTPUT_GROSS_LOSS,
    OUTPUT_GROSS_PROFIT,
    OUTPUT_INVALID_STOP_DISTANCE_COUNT,
    OUTPUT_LOSING_TRADES,
    OUTPUT_LIQUIDATION_COUNT,
    OUTPUT_MARGIN_REJECT_COUNT,
    OUTPUT_MAX_CONSECUTIVE_LOSSES,
    OUTPUT_MAX_DRAWDOWN_PCT,
    OUTPUT_MAX_NOTIONAL,
    OUTPUT_MAX_REQUIRED_LEVERAGE,
    OUTPUT_NET_PROFIT_PCT,
    OUTPUT_NO_CAPITAL_HALT,
    OUTPUT_PROFIT_FACTOR,
    OUTPUT_REJECTED_FILL_COUNT,
    OUTPUT_ROMAD,
    OUTPUT_TOTAL_TRADES,
    OUTPUT_WINNING_TRADES,
    OUTPUT_WIN_RATE_PCT,
    OUTPUT_ZERO_SIZE_ENTRY_COUNT,
    OUTPUT_COLUMN_COUNT,
    build_stacked_execution_data,
    compiled_batch_available,
    compiled_unavailable_reason,
    evaluate_compiled_stacked_batch,
    pack_compiled_config_arrays_from_rows,
)
from core.engine_v2.metrics_kernel import compute_core_metrics_from_balance_and_trades
from core.engine_v2.profile import (
    active_parameter_names,
    canonical_selector_key,
    inactive_parameter_names,
    mode_binding_for,
    parse_execution_profile,
)
from core.engine_v2.runner import V2RunResult, run_v2_strategy


GRID_V2_ENGINE_VERSION = "grid_v2_phase2_5"
REFERENCE_BATCH_KIND = "reference"
_BOOL_SIGNAL_ARRAYS = 2
_FLOAT_DATAPREP_ARRAYS = 5
_BYTES_PER_MB = 1024.0 * 1024.0
_KERNEL_CONFIG_PARAM_NAMES = (
    "initialCapital",
    "commissionPct",
    "stopX",
    "stopRR",
    "stopMaxPct",
    "stopMaxDays",
    "riskPerTrade",
    "contractSize",
    "trailRR",
    "tickSize",
    "start",
    "end",
    "enableLong",
    "enableShort",
    "dateFilter",
)


@dataclass(frozen=True)
class GridV2Settings:
    """Runtime settings for a backend-only Grid V2 run."""

    top_n: int = 10
    max_signal_cache_mb: float = 512.0
    worker_multiplier: int = 1
    compiled_workers: int = 1
    slow_enrich_selected: bool = True
    enabled_variants: tuple[str, ...] | None = None
    enabled_axes: tuple[str, ...] | None = None
    price_rounding: str | None = None
    prefer_compiled: bool = True
    compiled_config_packing: str = "mapping"
    primary_metric: str = "net_profit_pct"
    include_inactive_axes_for_dedup: bool = False

    @property
    def top_candidates(self) -> int:
        """Compatibility alias for storage helpers shared with Grid V1."""

        return self.top_n


@dataclass(frozen=True)
class GridV2StrategyHooks:
    """Strategy-specific data hooks consumed by the generic backend."""

    build_execution_data: Callable[[pd.DataFrame, Mapping[str, Any]], ExecutionData]
    normalize_params: Callable[[Mapping[str, Any] | None], Mapping[str, Any]] | None = None
    label: str = ""
    signal_param_names: tuple[str, ...] | None = None
    dataprep_param_names: tuple[str, ...] | None = None
    function_fingerprint: str | None = None

    @classmethod
    def from_strategy(cls, strategy: Any) -> "GridV2StrategyHooks":
        builder = getattr(strategy, "build_v2_execution_data", None)
        if builder is None:
            builder = getattr(strategy, "build_execution_data", None)
        if builder is None or not callable(builder):
            raise TypeError("Grid V2 strategy hooks require a callable build_v2_execution_data.")
        normalizer = getattr(strategy, "normalized_params", None)
        if normalizer is not None and not callable(normalizer):
            normalizer = None
        signal_names = _optional_name_tuple(getattr(strategy, "SIGNAL_CACHE_PARAM_NAMES", None))
        dataprep_names = _optional_name_tuple(getattr(strategy, "DATAPREP_CACHE_PARAM_NAMES", None))
        return cls(
            build_execution_data=builder,
            normalize_params=normalizer,
            label=str(getattr(strategy, "__name__", getattr(strategy, "__class__", type(strategy)).__name__)),
            signal_param_names=signal_names,
            dataprep_param_names=dataprep_names,
        )


@dataclass(frozen=True)
class GridV2ParameterDomain:
    name: str
    role: str | None
    values: tuple[Any, ...]
    default: Any
    is_axis: bool
    is_runtime: bool
    source: str


@dataclass(frozen=True)
class CandidateMappingRecord:
    candidate_id: int
    variant_name: str
    semantic_key: str
    canonical_identity: str
    active_param_values: Mapping[str, Any]
    axis_param_values: Mapping[str, Any]


@dataclass(frozen=True)
class GridV2Candidate:
    candidate_id: int
    variant_name: str
    modes: Mapping[str, str]
    params: Mapping[str, Any]
    active_param_names: tuple[str, ...]
    inactive_param_names: tuple[str, ...]
    axis_param_names: tuple[str, ...]
    semantic_key: str
    semantic_payload: Mapping[str, Any]
    canonical_identity: str


@dataclass
class GridV2CandidateTable:
    """Typed, lazy candidate table for deterministic Grid V2 plans."""

    strategy_id: str
    strategy_version: str
    profile: ExecutionProfile
    parameter_domains: Mapping[str, GridV2ParameterDomain]
    axis_names: tuple[str, ...]
    axis_column_by_name: Mapping[str, int]
    variant_names: tuple[str, ...]
    mode_tuples_by_variant: tuple[tuple[tuple[str, str], ...], ...]
    variant_codes: np.ndarray = field(repr=False)
    axis_value_codes: np.ndarray = field(repr=False)
    semantic_keys_by_row: tuple[str, ...] | None = field(repr=False)
    params_by_row: tuple[Mapping[str, Any], ...] | None = field(repr=False)
    seed_params_by_variant: Mapping[str, Mapping[str, Any]]
    active_names_by_variant: Mapping[str, tuple[str, ...]]
    inactive_names_by_variant: Mapping[str, tuple[str, ...]]
    axis_names_by_variant: Mapping[str, tuple[str, ...]]
    raw_candidate_count: int
    enumerated_candidate_count: int
    semantic_dedup_count: int
    per_variant_counts: Mapping[str, int]
    _semantic_key_cache: dict[int, str] = field(default_factory=dict, init=False, repr=False)
    _canonical_identity_cache: dict[int, str] = field(default_factory=dict, init=False, repr=False)
    _candidate_cache: dict[int, GridV2Candidate] = field(default_factory=dict, init=False, repr=False)

    def __len__(self) -> int:
        return int(self.variant_codes.shape[0])

    @property
    def deduped_candidate_count(self) -> int:
        return len(self)

    @property
    def semantic_keys_materialized_count(self) -> int:
        if self.semantic_keys_by_row is not None:
            return len(self.semantic_keys_by_row)
        return len(self._semantic_key_cache)

    @property
    def canonical_identities_materialized_count(self) -> int:
        return len(self._canonical_identity_cache)

    @property
    def legacy_candidates_materialized_count(self) -> int:
        return len(self._candidate_cache)

    def validate_index(self, index: int) -> int:
        idx = int(index)
        if idx < 0 or idx >= len(self):
            raise IndexError(f"Grid V2 candidate index out of range: {idx}.")
        return idx

    def validate_candidate_id(self, candidate_id: int) -> int:
        idx = int(candidate_id) - 1
        if idx < 0 or idx >= len(self):
            raise IndexError(f"Grid V2 candidate id out of range: {candidate_id}.")
        return idx

    def candidate_id_for_index(self, index: int) -> int:
        return self.validate_index(index) + 1

    def variant_name_for_index(self, index: int) -> str:
        idx = self.validate_index(index)
        return self.variant_names[int(self.variant_codes[idx])]

    def modes_for_index(self, index: int) -> dict[str, str]:
        variant_name = self.variant_name_for_index(index)
        return dict(self.profile.variants[variant_name].modes)

    def active_names_for_index(self, index: int) -> tuple[str, ...]:
        return self.active_names_by_variant[self.variant_name_for_index(index)]

    def inactive_names_for_index(self, index: int) -> tuple[str, ...]:
        return self.inactive_names_by_variant[self.variant_name_for_index(index)]

    def axis_names_for_index(self, index: int) -> tuple[str, ...]:
        return self.axis_names_by_variant[self.variant_name_for_index(index)]

    def params_for_index(self, index: int) -> dict[str, Any]:
        idx = self.validate_index(index)
        if self.params_by_row is not None:
            return self.params_by_row[idx]  # type: ignore[return-value]
        variant_name = self.variant_name_for_index(idx)
        params = dict(self.seed_params_by_variant[variant_name])
        for column, name in enumerate(self.axis_names):
            code = int(self.axis_value_codes[idx, column]) if self.axis_value_codes.shape[1] else -1
            if code >= 0:
                params[name] = self.parameter_domains[name].values[code]
        return _jsonable_mapping(params)

    def param_value_for_index(self, index: int, name: str) -> Any:
        idx = self.validate_index(index)
        if self.params_by_row is not None:
            return _jsonable_value(self.params_by_row[idx].get(name))
        variant_name = self.variant_name_for_index(idx)
        column = self.axis_column_by_name.get(name)
        if column is not None:
            code = int(self.axis_value_codes[idx, column]) if self.axis_value_codes.shape[1] else -1
            if code >= 0:
                return _jsonable_value(self.parameter_domains[name].values[code])
        return _jsonable_value(self.seed_params_by_variant[variant_name].get(name))

    def has_param_for_index(self, index: int, name: str) -> bool:
        idx = self.validate_index(index)
        if self.params_by_row is not None:
            return name in self.params_by_row[idx]
        variant_name = self.variant_name_for_index(idx)
        if name in self.seed_params_by_variant[variant_name]:
            return True
        column = self.axis_column_by_name.get(name)
        if column is None:
            return False
        return bool(self.axis_value_codes.shape[1] and int(self.axis_value_codes[idx, column]) >= 0)

    def active_param_values_for_index(self, index: int) -> dict[str, Any]:
        params = self.params_for_index(index)
        return {
            name: _jsonable_value(params[name])
            for name in self.active_names_for_index(index)
            if name in params
        }

    def axis_param_values_for_index(self, index: int) -> dict[str, Any]:
        params = self.params_for_index(index)
        return {
            name: _jsonable_value(params[name])
            for name in self.axis_names_for_index(index)
            if name in params
        }

    def semantic_payload_for_index(self, index: int) -> dict[str, Any]:
        variant_name = self.variant_name_for_index(index)
        return _semantic_payload(
            config={"id": self.strategy_id, "version": self.strategy_version},
            profile=self.profile,
            variant=self.profile.variants[variant_name],
            params=self.params_for_index(index),
            active_names=self.active_names_for_index(index),
        )

    def semantic_key_for_index(self, index: int) -> str:
        idx = self.validate_index(index)
        if self.semantic_keys_by_row is not None:
            return self.semantic_keys_by_row[idx]
        key = self._semantic_key_cache.get(idx)
        if key is None:
            key = _stable_json(self.semantic_payload_for_index(idx))
            self._semantic_key_cache[idx] = key
        return key

    def canonical_identity_for_index(self, index: int) -> str:
        idx = self.validate_index(index)
        identity = self._canonical_identity_cache.get(idx)
        if identity is None:
            identity = _canonical_identity(
                variant_name=self.variant_name_for_index(idx),
                params=self.params_for_index(idx),
                names=self.active_names_for_index(idx),
            )
            self._canonical_identity_cache[idx] = identity
        return identity

    def candidate_for_index(self, index: int) -> GridV2Candidate:
        idx = self.validate_index(index)
        candidate = self._candidate_cache.get(idx)
        if candidate is None:
            candidate = GridV2Candidate(
                candidate_id=idx + 1,
                variant_name=self.variant_name_for_index(idx),
                modes=self.modes_for_index(idx),
                params=self.params_for_index(idx),
                active_param_names=self.active_names_for_index(idx),
                inactive_param_names=self.inactive_names_for_index(idx),
                axis_param_names=self.axis_names_for_index(idx),
                semantic_key=self.semantic_key_for_index(idx),
                semantic_payload=self.semantic_payload_for_index(idx),
                canonical_identity=self.canonical_identity_for_index(idx),
            )
            self._candidate_cache[idx] = candidate
        return candidate

    def candidate_for_id(self, candidate_id: int) -> GridV2Candidate:
        return self.candidate_for_index(self.validate_candidate_id(candidate_id))

    def mapping_record_for_index(self, index: int) -> CandidateMappingRecord:
        idx = self.validate_index(index)
        return CandidateMappingRecord(
            candidate_id=idx + 1,
            variant_name=self.variant_name_for_index(idx),
            semantic_key=self.semantic_key_for_index(idx),
            canonical_identity=self.canonical_identity_for_index(idx),
            active_param_values=self.active_param_values_for_index(idx),
            axis_param_values=self.axis_param_values_for_index(idx),
        )


@dataclass(frozen=True)
class GridV2Plan:
    settings: GridV2Settings
    strategy_id: str
    strategy_version: str
    profile: ExecutionProfile
    parameter_domains: Mapping[str, GridV2ParameterDomain]
    candidate_table: GridV2CandidateTable
    raw_candidate_count: int
    enumerated_candidate_count: int
    deduped_candidate_count: int
    per_variant_counts: Mapping[str, int]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    _candidates_cache: tuple[GridV2Candidate, ...] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _mapping_records_cache: tuple[CandidateMappingRecord, ...] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    @property
    def candidates(self) -> tuple[GridV2Candidate, ...]:
        cache = self._candidates_cache
        if cache is None:
            cache = tuple(self.candidate_table.candidate_for_index(index) for index in range(self.deduped_candidate_count))
            object.__setattr__(self, "_candidates_cache", cache)
        return cache

    @property
    def mapping_records(self) -> tuple[CandidateMappingRecord, ...]:
        cache = self._mapping_records_cache
        if cache is None:
            cache = tuple(
                self.candidate_table.mapping_record_for_index(index)
                for index in range(self.deduped_candidate_count)
            )
            object.__setattr__(self, "_mapping_records_cache", cache)
        return cache

    def candidate_for_index(self, index: int) -> GridV2Candidate:
        return self.candidate_table.candidate_for_index(index)

    def candidate_for_id(self, candidate_id: int) -> GridV2Candidate:
        return self.candidate_table.candidate_for_id(candidate_id)


@dataclass(frozen=True)
class GridV2CountPreview:
    raw_candidate_count: int
    enumerated_candidate_count: int
    deduped_candidate_count: int | None
    per_variant_counts: Mapping[str, int]
    axis_names_by_variant: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class GridV2CacheEstimate:
    n_bars: int
    signal_combo_count: int
    dataprep_combo_count: int
    worker_multiplier: int
    bytes_per_signal_combo: int
    bytes_per_dataprep_combo: int
    estimated_signal_mb: float
    estimated_dataprep_mb: float
    estimated_output_mb: float
    estimated_shared_market_mb: float
    estimated_stack_signal_mb: float
    estimated_stack_dataprep_mb: float
    estimated_total_mb: float
    max_signal_cache_mb: float
    physical_signal_stack_rows: int = 0
    physical_dataprep_stack_rows: int = 0
    output_candidate_count: int = 0
    output_column_count: int = OUTPUT_COLUMN_COUNT
    bytes_per_output_candidate: int = OUTPUT_COLUMN_COUNT * np.dtype(np.float64).itemsize
    bytes_per_shared_market_bar: int = 5 * np.dtype(np.float64).itemsize


@dataclass
class GridV2CacheStats:
    signal_hits: int = 0
    signal_misses: int = 0
    dataprep_hits: int = 0
    dataprep_misses: int = 0


@dataclass(frozen=True, slots=True)
class GridV2ResultRow:
    candidate_id: int
    semantic_key: str
    canonical_identity: Any
    variant_name: str
    modes: Mapping[str, str]
    params: Mapping[str, Any]
    net_profit_pct: float
    max_drawdown_pct: float
    romad: float
    profit_factor: float
    win_rate_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    gross_profit: float
    gross_loss: float
    max_consecutive_losses: int
    final_balance: float
    guardrail_summary: Mapping[str, Any]
    backend_kind: str = REFERENCE_BATCH_KIND
    status: str = "ok"
    error: str | None = None

    @property
    def win_rate(self) -> float:
        return self.win_rate_pct

    def metric_value(self, name: str) -> float:
        if name == "win_rate":
            name = "win_rate_pct"
        return float(getattr(self, name))


@dataclass(frozen=True)
class GridV2SelectedResult:
    row: GridV2ResultRow
    metrics: Mapping[str, Any]
    guardrail_summary: Mapping[str, Any]


@dataclass(frozen=True)
class GridV2RunResult:
    plan: GridV2Plan
    rows: tuple[GridV2ResultRow, ...]
    selected: tuple[GridV2SelectedResult, ...]
    cache_estimate: GridV2CacheEstimate
    cache_stats: GridV2CacheStats
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _CacheKeyContext:
    data_fingerprint: str
    trade_start_idx: int
    function_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class _CandidateCacheKeys:
    candidate_index: int
    candidate_id: int
    signal_key: Any
    dataprep_key: Any


def build_grid_v2_plan(
    config: Mapping[str, Any],
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
) -> GridV2Plan:
    """Build a deterministic candidate plan from V2 config/profile metadata."""

    settings = settings or GridV2Settings()
    config_copy = _config_with_settings(config, settings)
    profile = parse_execution_profile(config_copy)
    params_spec = _parameters(config_copy)
    defaults = _parameter_defaults(params_spec)
    fixed_params = dict(defaults)
    fixed_params.update(dict(base_params or {}))
    domains = _build_parameter_domains(config_copy, settings, fixed_params, profile)
    selector_values = _selector_values_by_variant(config_copy, profile)
    selected_variants = _selected_variants(profile, settings)
    candidate_table = _build_candidate_table(
        config=config_copy,
        settings=settings,
        profile=profile,
        fixed_params=fixed_params,
        domains=domains,
        selector_values=selector_values,
        selected_variants=selected_variants,
    )

    metadata = {
        "backend_kind": COMPILED_BATCH_KIND if compiled_batch_available() else REFERENCE_BATCH_KIND,
        "engine_version": GRID_V2_ENGINE_VERSION,
        "compiled_batch_available": compiled_batch_available(),
        "compiled_unavailable_reason": compiled_unavailable_reason(),
        "default_enabled_axes": [
            name for name, domain in domains.items() if domain.is_axis
        ],
        "select_option_subsets": {
            name: list(domain.values)
            for name, domain in domains.items()
            if domain.is_axis and str(domain.source).endswith(".runtime_options")
        },
        "variant_order": list(selected_variants),
        "semantic_dedup_count": candidate_table.semantic_dedup_count,
        "candidate_table": {
            "enabled": True,
            "layout": "typed_lazy",
            "axis_count": len(candidate_table.axis_names),
            "variant_count": len(candidate_table.variant_names),
        },
    }
    return GridV2Plan(
        settings=settings,
        strategy_id=profile.strategy_id,
        strategy_version=str(config_copy.get("version", "")),
        profile=profile,
        parameter_domains=domains,
        candidate_table=candidate_table,
        raw_candidate_count=candidate_table.raw_candidate_count,
        enumerated_candidate_count=candidate_table.enumerated_candidate_count,
        deduped_candidate_count=candidate_table.deduped_candidate_count,
        per_variant_counts=candidate_table.per_variant_counts,
        metadata=metadata,
    )


def _build_candidate_table(
    *,
    config: Mapping[str, Any],
    settings: GridV2Settings,
    profile: ExecutionProfile,
    fixed_params: Mapping[str, Any],
    domains: Mapping[str, GridV2ParameterDomain],
    selector_values: Mapping[str, Any],
    selected_variants: Sequence[str],
) -> GridV2CandidateTable:
    axis_names = tuple(name for name in profile.parameter_names if domains[name].is_axis)
    axis_columns = {name: index for index, name in enumerate(axis_names)}
    seed_params_by_variant: dict[str, dict[str, Any]] = {}
    active_names_by_variant: dict[str, tuple[str, ...]] = {}
    inactive_names_by_variant: dict[str, tuple[str, ...]] = {}
    axis_names_by_variant: dict[str, tuple[str, ...]] = {}
    per_variant_counts: dict[str, int] = {name: 0 for name in selected_variants}
    semantic_seen: set[tuple[Any, ...]] = set()
    semantic_keys: list[str] = []
    params_by_row: list[dict[str, Any]] = []
    variant_codes: list[int] = []
    axis_value_codes: list[list[int]] = []
    raw_count = 0
    enumerated_count = 0

    for variant_code, variant_name in enumerate(selected_variants):
        variant = profile.variants[variant_name]
        seed_params = _candidate_seed_params(fixed_params)
        if profile.variant_selector is not None:
            seed_params[profile.variant_selector.param] = selector_values[variant_name]
        active_names = _ordered_active_names(profile, seed_params)
        inactive_names = _ordered_inactive_names(profile, seed_params)
        variant_axis_names = _variant_axis_names(
            profile=profile,
            domains=domains,
            active_names=active_names,
            settings=settings,
        )
        seed_params_by_variant[variant_name] = seed_params
        active_names_by_variant[variant_name] = active_names
        inactive_names_by_variant[variant_name] = inactive_names
        axis_names_by_variant[variant_name] = variant_axis_names

        value_groups = tuple(domains[name].values for name in variant_axis_names)
        code_groups = tuple(range(len(domains[name].values)) for name in variant_axis_names)
        raw_count += _product_size(value_groups)
        for values, codes in zip(itertools.product(*value_groups), itertools.product(*code_groups)):
            enumerated_count += 1
            params = dict(seed_params)
            params.update(zip(variant_axis_names, values))
            semantic_identity = _semantic_identity_tuple(
                config=config,
                profile=profile,
                variant=variant,
                params=params,
                active_names=active_names,
            )
            if semantic_identity in semantic_seen:
                continue
            semantic_seen.add(semantic_identity)
            jsonable_params = _jsonable_mapping(params)
            params_by_row.append(jsonable_params)
            semantic_keys.append(
                _stable_json(
                    _semantic_payload(
                        config=config,
                        profile=profile,
                        variant=variant,
                        params=jsonable_params,
                        active_names=active_names,
                    )
                )
            )
            row_codes = [-1] * len(axis_names)
            for name, code in zip(variant_axis_names, codes):
                row_codes[axis_columns[name]] = int(code)
            variant_codes.append(int(variant_code))
            axis_value_codes.append(row_codes)
            per_variant_counts[variant_name] += 1

    axis_code_array = np.asarray(axis_value_codes, dtype=np.int32)
    if not axis_value_codes:
        axis_code_array = np.empty((0, len(axis_names)), dtype=np.int32)
    elif axis_code_array.ndim == 1:
        axis_code_array = axis_code_array.reshape((len(axis_value_codes), len(axis_names)))

    return GridV2CandidateTable(
        strategy_id=str(config.get("id", profile.strategy_id)),
        strategy_version=str(config.get("version", "")),
        profile=profile,
        parameter_domains=domains,
        axis_names=axis_names,
        axis_column_by_name=axis_columns,
        variant_names=tuple(selected_variants),
        mode_tuples_by_variant=tuple(
            tuple(sorted((str(name), str(value)) for name, value in profile.variants[variant].modes.items()))
            for variant in selected_variants
        ),
        variant_codes=np.asarray(variant_codes, dtype=np.int32),
        axis_value_codes=axis_code_array,
        semantic_keys_by_row=tuple(semantic_keys),
        params_by_row=tuple(params_by_row),
        seed_params_by_variant=seed_params_by_variant,
        active_names_by_variant=active_names_by_variant,
        inactive_names_by_variant=inactive_names_by_variant,
        axis_names_by_variant=axis_names_by_variant,
        raw_candidate_count=raw_count,
        enumerated_candidate_count=enumerated_count,
        semantic_dedup_count=enumerated_count - len(variant_codes),
        per_variant_counts=per_variant_counts,
    )


def preview_grid_v2_counts(
    config: Mapping[str, Any],
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
) -> GridV2CountPreview:
    """Compute deterministic Grid V2 breadth without materializing candidates."""

    settings = settings or GridV2Settings()
    config_copy = _config_with_settings(config, settings)
    profile = parse_execution_profile(config_copy)
    params_spec = _parameters(config_copy)
    defaults = _parameter_defaults(params_spec)
    fixed_params = dict(defaults)
    fixed_params.update(dict(base_params or {}))
    domains = _build_parameter_domains(config_copy, settings, fixed_params, profile)
    selector_values = _selector_values_by_variant(config_copy, profile)
    selected_variants = _selected_variants(profile, settings)
    per_variant: dict[str, int] = {}
    axis_names_by_variant: dict[str, tuple[str, ...]] = {}
    total = 0
    for variant_name in selected_variants:
        seed_params = _candidate_seed_params(fixed_params)
        if profile.variant_selector is not None:
            seed_params[profile.variant_selector.param] = selector_values[variant_name]
        active_names = _ordered_active_names(profile, seed_params)
        axis_names = _variant_axis_names(
            profile=profile,
            domains=domains,
            active_names=active_names,
            settings=settings,
        )
        count = _product_size(domains[name].values for name in axis_names)
        per_variant[variant_name] = count
        axis_names_by_variant[variant_name] = axis_names
        total += count
    deduped = None if settings.include_inactive_axes_for_dedup else total
    return GridV2CountPreview(
        raw_candidate_count=total,
        enumerated_candidate_count=total,
        deduped_candidate_count=deduped,
        per_variant_counts=per_variant,
        axis_names_by_variant=axis_names_by_variant,
    )


def estimate_grid_v2_cache(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    candidate_indices: Sequence[int] | None = None,
) -> GridV2CacheEstimate:
    """Estimate local cache memory for a planned run."""

    hooks = _coerce_hooks(hooks)
    selected_indices = _selected_candidate_indices(plan, candidate_indices)
    n_bars = int(len(df))
    context = _cache_key_context(df, trade_start_idx, hooks)
    cache_keys = _candidate_cache_keys(plan, context, hooks, selected_indices)
    return _estimate_grid_v2_cache_from_keys(plan, n_bars, cache_keys)


def _candidate_cache_keys(
    plan: GridV2Plan,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
    selected_indices: Sequence[int],
) -> tuple[_CandidateCacheKeys, ...]:
    signal_cache: dict[tuple[Any, ...], Any] = {}
    dataprep_cache: dict[tuple[Any, ...], Any] = {}
    records: list[_CandidateCacheKeys] = []
    for index in selected_indices:
        signal_signature = _cache_signature_for_index(plan, index, hooks, signal_only=True)
        signal_key = signal_cache.get(signal_signature)
        if signal_key is None:
            signal_key = _signal_cache_key_for_index(plan, index, context, hooks)
            signal_cache[signal_signature] = signal_key

        dataprep_signature = _cache_signature_for_index(plan, index, hooks, signal_only=False)
        dataprep_key = dataprep_cache.get(dataprep_signature)
        if dataprep_key is None:
            dataprep_key = _dataprep_cache_key_for_index(plan, index, context, hooks)
            dataprep_cache[dataprep_signature] = dataprep_key

        records.append(
            _CandidateCacheKeys(
                candidate_index=index,
                candidate_id=index + 1,
                signal_key=signal_key,
                dataprep_key=dataprep_key,
            )
        )
    return tuple(records)


def _estimate_grid_v2_cache_from_keys(
    plan: GridV2Plan,
    n_bars: int,
    cache_keys: Sequence[_CandidateCacheKeys],
) -> GridV2CacheEstimate:
    signal_combo_count = len({item.signal_key for item in cache_keys})
    dataprep_combo_count = len({item.dataprep_key for item in cache_keys})
    worker_multiplier = max(1, int(plan.settings.worker_multiplier))
    bytes_per_signal_combo = int(n_bars * _BOOL_SIGNAL_ARRAYS * np.dtype(np.bool_).itemsize)
    bytes_per_dataprep_combo = int(n_bars * _FLOAT_DATAPREP_ARRAYS * np.dtype(np.float64).itemsize)
    physical_signal_stack_rows = dataprep_combo_count
    physical_dataprep_stack_rows = dataprep_combo_count
    output_candidate_count = len(cache_keys)
    bytes_per_output_candidate = int(OUTPUT_COLUMN_COUNT * np.dtype(np.float64).itemsize)
    bytes_per_shared_market_bar = int(
        4 * np.dtype(np.float64).itemsize + np.dtype(np.int64).itemsize
    )
    estimated_signal_bytes = physical_signal_stack_rows * bytes_per_signal_combo
    estimated_dataprep_bytes = physical_dataprep_stack_rows * bytes_per_dataprep_combo
    estimated_output_bytes = output_candidate_count * bytes_per_output_candidate
    estimated_shared_market_bytes = n_bars * bytes_per_shared_market_bar
    estimated_total_bytes = (
        estimated_signal_bytes
        + estimated_dataprep_bytes
        + estimated_output_bytes
        + estimated_shared_market_bytes
    ) * worker_multiplier
    return GridV2CacheEstimate(
        n_bars=n_bars,
        signal_combo_count=signal_combo_count,
        dataprep_combo_count=dataprep_combo_count,
        worker_multiplier=worker_multiplier,
        bytes_per_signal_combo=bytes_per_signal_combo,
        bytes_per_dataprep_combo=bytes_per_dataprep_combo,
        estimated_signal_mb=estimated_signal_bytes / _BYTES_PER_MB,
        estimated_dataprep_mb=estimated_dataprep_bytes / _BYTES_PER_MB,
        estimated_output_mb=estimated_output_bytes / _BYTES_PER_MB,
        estimated_shared_market_mb=estimated_shared_market_bytes / _BYTES_PER_MB,
        estimated_stack_signal_mb=estimated_signal_bytes / _BYTES_PER_MB,
        estimated_stack_dataprep_mb=estimated_dataprep_bytes / _BYTES_PER_MB,
        estimated_total_mb=estimated_total_bytes / _BYTES_PER_MB,
        max_signal_cache_mb=float(plan.settings.max_signal_cache_mb),
        physical_signal_stack_rows=physical_signal_stack_rows,
        physical_dataprep_stack_rows=physical_dataprep_stack_rows,
        output_candidate_count=output_candidate_count,
        output_column_count=OUTPUT_COLUMN_COUNT,
        bytes_per_output_candidate=bytes_per_output_candidate,
        bytes_per_shared_market_bar=bytes_per_shared_market_bar,
    )


def execute_grid_v2_candidates(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    candidate_indices: Sequence[int] | None = None,
) -> GridV2RunResult:
    """Execute planned candidates through the selected V2 batch backend."""

    hooks = _coerce_hooks(hooks)
    selected_indices = _selected_candidate_indices(plan, candidate_indices)
    context = _cache_key_context(df, trade_start_idx, hooks)
    cache_keys = _candidate_cache_keys(plan, context, hooks, selected_indices)
    estimate = _estimate_grid_v2_cache_from_keys(plan, int(len(df)), cache_keys)
    if estimate.estimated_total_mb > estimate.max_signal_cache_mb:
        raise MemoryError(
            "Grid V2 cache estimate exceeds max_signal_cache_mb "
            f"({estimate.estimated_total_mb:.3f} MB > {estimate.max_signal_cache_mb:.3f} MB)."
        )

    eval_started = time.time()
    stats = GridV2CacheStats()
    signal_seen: set[Any] = set()
    dataprep_cache: dict[Any, ExecutionData] = {}
    data_groups: dict[Any, list[int]] = {}
    rows: list[GridV2ResultRow] = []

    for key_record in cache_keys:
        candidate_index = key_record.candidate_index
        signal_key = key_record.signal_key
        if signal_key in signal_seen:
            stats.signal_hits += 1
        else:
            stats.signal_misses += 1
            signal_seen.add(signal_key)

        data_key = key_record.dataprep_key
        if data_key in dataprep_cache:
            stats.dataprep_hits += 1
        else:
            stats.dataprep_misses += 1
            try:
                params = plan.candidate_table.params_for_index(candidate_index)
                data = hooks.build_execution_data(df, _normalized_candidate_params(hooks, params))
            except Exception as exc:
                rows.append(_error_row(plan, candidate_index, exc))
                continue
            dataprep_cache[data_key] = data
        data_groups.setdefault(data_key, []).append(candidate_index)

    compiled_available = compiled_batch_available()
    use_compiled = bool(plan.settings.prefer_compiled and compiled_available)
    backend_kind = COMPILED_BATCH_KIND if use_compiled else REFERENCE_BATCH_KIND
    compiled_execution_mode: str | None = None
    compiled_config_packing: str | None = None
    stack_metadata: dict[str, Any] = {}

    if use_compiled:
        data_keys = tuple(data_groups.keys())
        data_key_to_stack_row = {key: index for index, key in enumerate(data_keys)}
        compiled_indices: list[int] = []
        data_index: list[int] = []
        for key_record in cache_keys:
            data_key = key_record.dataprep_key
            if data_key not in data_key_to_stack_row:
                continue
            candidate_index = key_record.candidate_index
            compiled_indices.append(candidate_index)
            data_index.append(data_key_to_stack_row[data_key])
        if compiled_indices:
            stacked_data = build_stacked_execution_data(
                [dataprep_cache[key] for key in data_keys],
                data_index,
            )
            params_batch: list[Mapping[str, Any]] | None = None
            row_params_batch: list[Mapping[str, Any]] | None = None
            packed_config_arrays: Mapping[str, np.ndarray] | None = None
            requested_packing = str(plan.settings.compiled_config_packing or "mapping").strip().lower()
            if requested_packing not in {"mapping", "table"}:
                raise ValueError("Grid V2 compiled_config_packing must be 'mapping' or 'table'.")
            if requested_packing == "table" and _can_use_table_config_packer(plan, hooks, compiled_indices):
                packed_config_arrays = _pack_table_config_arrays(plan, compiled_indices, trade_start_idx)
                compiled_config_packing = "table"
            else:
                row_params_batch = [
                    plan.candidate_table.params_for_index(candidate_index)
                    for candidate_index in compiled_indices
                ]
                params_batch = [
                    _normalized_candidate_params(hooks, params)
                    for params in row_params_batch
                ]
                compiled_config_packing = "mapping"
            batch = evaluate_compiled_stacked_batch(
                stacked_data=stacked_data,
                profile=plan.profile,
                params_batch=params_batch,
                trade_start_idx=trade_start_idx,
                n_workers=plan.settings.compiled_workers,
                packed_config_arrays=packed_config_arrays,
            )
            compiled_execution_mode = batch.execution_mode
            stack_metadata = {
                "stack_row_count": stacked_data.row_count,
                "stack_candidate_count": stacked_data.candidate_count,
                "stack_signal_nbytes": stacked_data.signal_nbytes,
                "stack_dataprep_nbytes": stacked_data.dataprep_nbytes,
                "stack_shared_market_nbytes": stacked_data.shared_market_nbytes,
                "stack_output_nbytes": int(batch.outputs.nbytes),
                "stack_total_nbytes": int(stacked_data.nbytes + batch.outputs.nbytes),
                "stack_total_mb": (stacked_data.nbytes + batch.outputs.nbytes) / _BYTES_PER_MB,
            }
            for output_index, (candidate_index, values) in enumerate(zip(compiled_indices, batch.outputs)):
                params = row_params_batch[output_index] if row_params_batch is not None else None
                rows.append(_row_from_compiled_output(plan, candidate_index, values, params=params))
        else:
            compiled_execution_mode = "stacked"
    else:
        for data_key, candidate_indices_for_data in data_groups.items():
            data = dataprep_cache[data_key]
            for candidate_index in candidate_indices_for_data:
                params = plan.candidate_table.params_for_index(candidate_index)
                try:
                    run = run_v2_strategy(
                        data=data,
                        profile=plan.profile,
                        params=_normalized_candidate_params(hooks, params),
                        trade_start_idx=trade_start_idx,
                    )
                except Exception as exc:
                    rows.append(_error_row(plan, candidate_index, exc))
                    continue
                rows.append(_row_from_run(plan, candidate_index, run, params=params))

    rows.sort(key=lambda row: row.candidate_id)
    selected = ()
    if plan.settings.slow_enrich_selected:
        selected = tuple(
            _slow_enrich_selected(plan, context, trade_start_idx, hooks, row, dataprep_cache)
            for row in _rank_rows(rows, plan.settings.primary_metric)[: max(0, int(plan.settings.top_n))]
        )
    evaluation_seconds = time.time() - eval_started
    return GridV2RunResult(
        plan=plan,
        rows=tuple(rows),
        selected=selected,
        cache_estimate=estimate,
        cache_stats=stats,
        metadata={
            "backend_kind": backend_kind,
            "compiled_batch_available": compiled_available,
        "compiled_batch_used": use_compiled,
        "compiled_execution_mode": compiled_execution_mode,
        "compiled_config_packing": compiled_config_packing,
            "compiled_unavailable_reason": compiled_unavailable_reason(),
            "metric_tier": "core_fast_rows_plus_selected_public_v2_enrichment",
            "executed_candidate_count": len(rows),
            "slow_enrich_selected": bool(plan.settings.slow_enrich_selected),
            "compiled_workers": int(plan.settings.compiled_workers),
            "evaluation_seconds": evaluation_seconds,
            "candidates_per_second": (len(rows) / evaluation_seconds) if evaluation_seconds > 0.0 else None,
            "candidate_table_used": True,
            "legacy_candidates_materialized": plan.candidate_table.legacy_candidates_materialized_count,
            "semantic_keys_materialized": plan.candidate_table.semantic_keys_materialized_count,
            "canonical_identities_materialized": plan.candidate_table.canonical_identities_materialized_count,
            **stack_metadata,
        },
    )


def run_grid_v2(
    config: Mapping[str, Any],
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
    candidate_indices: Sequence[int] | None = None,
) -> GridV2RunResult:
    """Build and execute a Grid V2 plan."""

    plan = build_grid_v2_plan(config, settings=settings, base_params=base_params)
    return execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, candidate_indices)


def deterministic_candidate_subset_indices(
    total_count: int,
    limit: int,
    required_indices: Sequence[int] = (),
) -> tuple[int, ...]:
    """Return a stable zero-based subset covering edges and evenly spaced rows."""

    total = max(0, int(total_count))
    budget = max(0, int(limit))
    required = {int(index) for index in required_indices if 0 <= int(index) < total}
    if total == 0 or budget == 0:
        return ()
    if budget >= total:
        return tuple(range(total))
    required.update({0, total - 1})
    remaining = max(0, budget - len(required))
    if remaining > 0:
        for value in np.linspace(0, total - 1, remaining + 2, dtype=int)[1:-1]:
            required.add(int(value))
    if len(required) > budget:
        ordered_required = sorted(required)
        keep = {ordered_required[0], ordered_required[-1]}
        for value in ordered_required[1:-1]:
            if len(keep) >= budget:
                break
            keep.add(value)
        required = keep
    return tuple(sorted(required))


def _config_with_settings(config: Mapping[str, Any], settings: GridV2Settings) -> dict[str, Any]:
    config_copy = deepcopy(dict(config))
    if settings.price_rounding is not None:
        execution = dict(config_copy.get("execution") or {})
        execution["priceRounding"] = settings.price_rounding
        config_copy["execution"] = execution
    return config_copy


def _parameters(config: Mapping[str, Any]) -> Mapping[str, Any]:
    params = config.get("parameters", {})
    if not isinstance(params, Mapping):
        raise ValueError("Grid V2 config parameters must be a mapping.")
    return params


def _parameter_defaults(params: Mapping[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {}
    for name, spec in params.items():
        if isinstance(spec, Mapping) and "default" in spec:
            defaults[str(name)] = spec["default"]
    return defaults


def _candidate_seed_params(params: Mapping[str, Any]) -> dict[str, Any]:
    return {str(name): value for name, value in params.items() if not str(name).endswith("_options")}


def _build_parameter_domains(
    config: Mapping[str, Any],
    settings: GridV2Settings,
    fixed_params: Mapping[str, Any],
    profile: ExecutionProfile,
) -> dict[str, GridV2ParameterDomain]:
    params_spec = _parameters(config)
    selector_name = profile.variant_selector.param if profile.variant_selector is not None else None
    enabled_axes = set(settings.enabled_axes) if settings.enabled_axes is not None else None
    if enabled_axes is not None:
        unknown = sorted(enabled_axes - {str(name) for name in params_spec})
        if unknown:
            raise ValueError(f"Grid V2 enabled_axes contains unknown parameter(s): {unknown}.")

    domains: dict[str, GridV2ParameterDomain] = {}
    for raw_name, raw_spec in params_spec.items():
        name = str(raw_name)
        spec = raw_spec if isinstance(raw_spec, Mapping) else {}
        role = profile.parameter_roles.get(name)
        param_type = str(spec.get("type", "float")).strip().lower()
        optimize = spec.get("optimize", {}) if isinstance(spec.get("optimize", {}), Mapping) else {}
        is_runtime = role == "runtime"
        is_selector = name == selector_name
        default = fixed_params.get(name, spec.get("default"))
        axis_available = bool(optimize.get("enabled", False)) and not is_runtime and not is_selector
        if name in (enabled_axes or set()) and not axis_available:
            raise ValueError(f"Grid V2 axis '{name}' is not an optimized non-runtime parameter.")
        is_axis = axis_available and _axis_enabled(name, optimize, settings)
        if is_axis:
            values, source = _axis_values(name, spec, param_type, fixed_params)
        else:
            values, source = (_coerce_value(default, param_type),), "fixed_default"
        if not values:
            raise ValueError(f"Grid V2 parameter '{name}' has an empty domain.")
        domains[name] = GridV2ParameterDomain(
            name=name,
            role=role,
            values=tuple(values),
            default=_jsonable_value(default),
            is_axis=is_axis,
            is_runtime=is_runtime,
            source=source,
        )
    return domains


def _axis_enabled(name: str, optimize: Mapping[str, Any], settings: GridV2Settings) -> bool:
    if settings.enabled_axes is not None:
        return name in set(settings.enabled_axes)
    return optimize.get("default_enabled", True) is not False


def _axis_values(
    name: str,
    spec: Mapping[str, Any],
    param_type: str,
    fixed_params: Mapping[str, Any],
) -> tuple[tuple[Any, ...], str]:
    optimize = spec.get("optimize", {}) if isinstance(spec.get("optimize", {}), Mapping) else {}
    for source, values in (
        ("gridValues", spec.get("gridValues")),
        ("optimize.gridValues", optimize.get("gridValues")),
        ("optimize.values", optimize.get("values")),
    ):
        if values is not None:
            explicit = _explicit_values(name, values, param_type)
            return _select_subset_values(name, explicit, param_type, fixed_params, source)
    if param_type in {"select", "options"}:
        explicit = _explicit_values(name, spec.get("options"), param_type)
        return _select_subset_values(name, explicit, param_type, fixed_params, "options")
    if param_type == "bool":
        return (False, True), "bool"
    if param_type in {"int", "integer", "float", "number"}:
        return _numeric_range_values(name, spec, optimize, param_type), "optimize.range"
    raise ValueError(f"Grid V2 parameter '{name}' has unsupported type '{param_type}'.")


def _select_subset_values(
    name: str,
    values: tuple[Any, ...],
    param_type: str,
    fixed_params: Mapping[str, Any],
    source: str,
) -> tuple[tuple[Any, ...], str]:
    if param_type not in {"select", "options"}:
        return values, source
    option_key = f"{name}_options"
    if option_key not in fixed_params:
        return values, source
    raw_subset = fixed_params.get(option_key)
    if not isinstance(raw_subset, (list, tuple)) or not raw_subset:
        raise ValueError(f"Grid V2 select option subset '{option_key}' must be a non-empty list.")
    requested = tuple(dict.fromkeys(_coerce_value(value, param_type) for value in raw_subset))
    available = set(values)
    unknown = [value for value in requested if value not in available]
    if unknown:
        raise ValueError(
            f"Grid V2 select option subset '{option_key}' contains unknown option(s): {unknown}."
        )
    filtered = tuple(value for value in values if value in set(requested))
    if not filtered:
        raise ValueError(f"Grid V2 select option subset '{option_key}' leaves an empty domain.")
    return filtered, f"{source}.runtime_options"


def _explicit_values(name: str, values: Any, param_type: str) -> tuple[Any, ...]:
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError(f"Grid V2 parameter '{name}' requires a non-empty value list.")
    normalized = tuple(dict.fromkeys(_coerce_value(value, param_type) for value in values))
    if not normalized:
        raise ValueError(f"Grid V2 parameter '{name}' has an empty value list.")
    return normalized


def _numeric_range_values(
    name: str,
    spec: Mapping[str, Any],
    optimize: Mapping[str, Any],
    param_type: str,
) -> tuple[Any, ...]:
    missing = [
        key for key in ("min", "max", "step")
        if key not in optimize and key not in spec
    ]
    if missing:
        raise ValueError(f"Grid V2 numeric parameter '{name}' is missing {missing}.")
    start = _finite_decimal(optimize.get("min", spec.get("min")), name)
    stop = _finite_decimal(optimize.get("max", spec.get("max")), name)
    step = _finite_decimal(optimize.get("step", spec.get("step")), name)
    if step <= 0:
        raise ValueError(f"Grid V2 numeric parameter '{name}' requires a positive step.")
    if stop < start:
        raise ValueError(f"Grid V2 numeric parameter '{name}' has max below min.")
    values: list[Any] = []
    current = start
    while current <= stop:
        values.append(_coerce_decimal(current, param_type))
        current += step
    if not values:
        raise ValueError(f"Grid V2 numeric parameter '{name}' has an empty range.")
    return tuple(values)


def _finite_decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Grid V2 numeric parameter '{name}' has a non-numeric bound.") from exc
    if not parsed.is_finite():
        raise ValueError(f"Grid V2 numeric parameter '{name}' must be finite.")
    return parsed


def _coerce_decimal(value: Decimal, param_type: str) -> Any:
    if param_type in {"int", "integer"}:
        integral = value.to_integral_value()
        if value != integral:
            raise ValueError(f"Grid V2 int range produced non-integral value {value}.")
        return int(integral)
    as_float = float(value)
    if not math.isfinite(as_float):
        raise ValueError("Grid V2 float range produced a non-finite value.")
    return 0.0 if as_float == 0.0 else as_float


def _coerce_value(value: Any, param_type: str) -> Any:
    normalized_type = str(param_type).strip().lower()
    if normalized_type in {"int", "integer"}:
        return int(Decimal(str(value)).to_integral_value())
    if normalized_type in {"float", "number"}:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Grid V2 float values must be finite.")
        return 0.0 if parsed == 0.0 else parsed
    if normalized_type == "bool":
        return _coerce_bool(value)
    return value


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    raise ValueError(f"Grid V2 cannot coerce {value!r} to bool.")


def _selector_values_by_variant(
    config: Mapping[str, Any],
    profile: ExecutionProfile,
) -> dict[str, Any]:
    selector = profile.variant_selector
    if selector is None:
        return {}
    params = _parameters(config)
    selector_spec = params.get(selector.param, {})
    param_type = str(selector_spec.get("type", "select")).strip().lower() if isinstance(selector_spec, Mapping) else "select"
    values: dict[str, Any] = {}
    for raw_key, variant_name in selector.mapping.items():
        if variant_name in values:
            continue
        values[variant_name] = _coerce_value(raw_key, param_type)
    missing = [name for name in profile.variants if name not in values]
    if missing:
        raise ValueError(f"Grid V2 selector mapping is missing variant(s): {missing}.")
    return values


def _selected_variants(profile: ExecutionProfile, settings: GridV2Settings) -> tuple[str, ...]:
    variant_order = tuple(profile.variants.keys())
    if settings.enabled_variants is None:
        return variant_order
    requested = tuple(settings.enabled_variants)
    unknown = sorted(set(requested) - set(variant_order))
    if unknown:
        raise ValueError(f"Grid V2 enabled_variants contains unknown variant(s): {unknown}.")
    return tuple(name for name in variant_order if name in set(requested))


def _ordered_active_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> tuple[str, ...]:
    active = active_parameter_names(profile, params)
    return tuple(name for name in profile.parameter_names if name in active)


def _ordered_inactive_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> tuple[str, ...]:
    inactive = inactive_parameter_names(profile, params)
    return tuple(name for name in profile.parameter_names if name in inactive)


def _variant_axis_names(
    *,
    profile: ExecutionProfile,
    domains: Mapping[str, GridV2ParameterDomain],
    active_names: Sequence[str],
    settings: GridV2Settings,
) -> tuple[str, ...]:
    active = set(active_names)
    names: list[str] = []
    for name in profile.parameter_names:
        domain = domains[name]
        if not domain.is_axis:
            continue
        if settings.include_inactive_axes_for_dedup or name in active:
            names.append(name)
    return tuple(names)


def _product_size(value_groups: Any) -> int:
    total = 1
    for values in value_groups:
        total *= len(values)
    return total


def _semantic_payload(
    *,
    config: Mapping[str, Any],
    profile: ExecutionProfile,
    variant: VariantSpec,
    params: Mapping[str, Any],
    active_names: Sequence[str],
) -> dict[str, Any]:
    active_params = {
        name: _jsonable_value(params[name])
        for name in active_names
        if name in params and profile.parameter_roles.get(name) != "runtime"
    }
    return {
        "engine": GRID_V2_ENGINE_VERSION,
        "strategy": {
            "id": str(config.get("id", profile.strategy_id)),
            "version": str(config.get("version", "")),
        },
        "variant": variant.name,
        "modes": {name: _jsonable_value(value) for name, value in sorted(variant.modes.items())},
        "params": active_params,
    }


def _semantic_identity_tuple(
    *,
    config: Mapping[str, Any],
    profile: ExecutionProfile,
    variant: VariantSpec,
    params: Mapping[str, Any],
    active_names: Sequence[str],
) -> tuple[Any, ...]:
    active_params = tuple(
        sorted(
            (
                name,
                _hashable_jsonable_value(params[name]),
            )
            for name in active_names
            if name in params and profile.parameter_roles.get(name) != "runtime"
        )
    )
    modes = tuple(
        sorted((str(name), _hashable_jsonable_value(value)) for name, value in variant.modes.items())
    )
    return (
        GRID_V2_ENGINE_VERSION,
        str(config.get("id", profile.strategy_id)),
        str(config.get("version", "")),
        str(variant.name),
        modes,
        active_params,
    )


def _canonical_identity(
    *,
    variant_name: str,
    params: Mapping[str, Any],
    names: Sequence[str],
) -> str:
    return _stable_json(
        {
            "variant": variant_name,
            "params": {name: _jsonable_value(params[name]) for name in names if name in params},
        }
    )


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def _jsonable_mapping(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _jsonable_value(value) for key, value in payload.items()}


def _jsonable_value(value: Any) -> Any:
    if isinstance(value, (bool, int, str)) or value is None:
        return value
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        parsed = float(value)
        if not math.isfinite(parsed):
            return str(parsed)
        return 0.0 if parsed == 0.0 else parsed
    if isinstance(value, pd.Timestamp):
        ts = value if value.tzinfo is not None else value.tz_localize("UTC")
        return ts.tz_convert("UTC").isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return _jsonable_value(float(value))
    if isinstance(value, (list, tuple)):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _jsonable_value(item) for key, item in value.items()}
    return value


def _hashable_jsonable_value(value: Any) -> Any:
    normalized = _jsonable_value(value)
    if isinstance(normalized, Mapping):
        return tuple(sorted((str(key), _hashable_jsonable_value(item)) for key, item in normalized.items()))
    if isinstance(normalized, list):
        return tuple(_hashable_jsonable_value(item) for item in normalized)
    return normalized


def _optional_name_tuple(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    return tuple(str(item) for item in value)


def _coerce_hooks(hooks: GridV2StrategyHooks | Any) -> GridV2StrategyHooks:
    if isinstance(hooks, GridV2StrategyHooks):
        if hooks.function_fingerprint is not None:
            return hooks
        return GridV2StrategyHooks(
            build_execution_data=hooks.build_execution_data,
            normalize_params=hooks.normalize_params,
            label=hooks.label,
            signal_param_names=hooks.signal_param_names,
            dataprep_param_names=hooks.dataprep_param_names,
            function_fingerprint=_callable_fingerprint(hooks.build_execution_data),
        )
    return _coerce_hooks(GridV2StrategyHooks.from_strategy(hooks))


def _callable_fingerprint(func: Callable[..., Any]) -> str:
    module = getattr(func, "__module__", "")
    qualname = getattr(func, "__qualname__", "")
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        source = ""
    return hashlib.blake2b(
        _stable_json({"module": module, "qualname": qualname, "source": source}).encode("utf-8"),
        digest_size=12,
    ).hexdigest()


def _data_fingerprint(df: pd.DataFrame) -> str:
    payload = {
        "shape": tuple(int(item) for item in df.shape),
        "columns": tuple(str(column) for column in df.columns),
        "dtypes": tuple(str(dtype) for dtype in df.dtypes),
        "first_index": _jsonable_value(df.index[0]) if len(df.index) else None,
        "last_index": _jsonable_value(df.index[-1]) if len(df.index) else None,
    }
    digest = hashlib.blake2b(_stable_json(payload).encode("utf-8"), digest_size=12)
    if not df.empty:
        hashed = pd.util.hash_pandas_object(df, index=True).to_numpy(dtype=np.uint64, copy=False)
        digest.update(np.ascontiguousarray(hashed).tobytes())
    return digest.hexdigest()


def _cache_key_context(
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
) -> _CacheKeyContext:
    return _CacheKeyContext(
        data_fingerprint=_data_fingerprint(df),
        trade_start_idx=int(trade_start_idx),
        function_fingerprint=hooks.function_fingerprint,
    )


def _cache_param_names(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> tuple[str, ...]:
    if signal_only and hooks.signal_param_names is not None:
        return tuple(name for name in hooks.signal_param_names if name in candidate.params)
    if not signal_only and hooks.dataprep_param_names is not None:
        return tuple(name for name in hooks.dataprep_param_names if name in candidate.params)
    active = set(candidate.active_param_names)
    if signal_only:
        return tuple(
            name
            for name in plan.profile.parameter_names
            if name in active and plan.profile.parameter_roles.get(name) == "signal"
        )
    names: set[str] = set(_cache_param_names(plan, candidate, hooks, signal_only=True))
    for mode_field, mode_value in candidate.modes.items():
        binding = mode_binding_for(mode_field, mode_value)
        if binding is None or not binding.dataprep:
            continue
        names.update(name for name in binding.consumes_params if name in candidate.params)
    if not names:
        names.update(candidate.active_param_names)
    return tuple(name for name in plan.profile.parameter_names if name in names)


def _cache_param_names_for_index(
    plan: GridV2Plan,
    candidate_index: int,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> tuple[str, ...]:
    table = plan.candidate_table
    if signal_only and hooks.signal_param_names is not None:
        return tuple(name for name in hooks.signal_param_names if table.has_param_for_index(candidate_index, name))
    if not signal_only and hooks.dataprep_param_names is not None:
        return tuple(name for name in hooks.dataprep_param_names if table.has_param_for_index(candidate_index, name))
    active = set(table.active_names_for_index(candidate_index))
    if signal_only:
        return tuple(
            name
            for name in plan.profile.parameter_names
            if name in active and plan.profile.parameter_roles.get(name) == "signal"
        )
    names: set[str] = set(_cache_param_names_for_index(plan, candidate_index, hooks, signal_only=True))
    for mode_field, mode_value in table.modes_for_index(candidate_index).items():
        binding = mode_binding_for(mode_field, mode_value)
        if binding is None or not binding.dataprep:
            continue
        names.update(name for name in binding.consumes_params if table.has_param_for_index(candidate_index, name))
    if not names:
        names.update(active)
    return tuple(name for name in plan.profile.parameter_names if name in names)


def _cache_signature_for_index(
    plan: GridV2Plan,
    candidate_index: int,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> tuple[Any, ...]:
    table = plan.candidate_table
    variant_code = int(table.variant_codes[candidate_index])
    param_names = _cache_param_names_for_index(plan, candidate_index, hooks, signal_only=signal_only)
    params = tuple(
        (name, _cache_signature_param_value(table, candidate_index, name))
        for name in param_names
    )
    if signal_only:
        return ("signal", params)
    return (
        "dataprep",
        table.variant_names[variant_code],
        table.mode_tuples_by_variant[variant_code],
        params,
    )


def _cache_signature_param_value(
    table: GridV2CandidateTable,
    candidate_index: int,
    name: str,
) -> tuple[Any, ...]:
    column = table.axis_column_by_name.get(name)
    if column is not None and table.axis_value_codes.shape[1]:
        code = int(table.axis_value_codes[candidate_index, column])
        if code >= 0:
            return ("axis", code)
    if table.has_param_for_index(candidate_index, name):
        return ("value", _hashable_jsonable_value(table.param_value_for_index(candidate_index, name)))
    return ("missing",)


def _cache_key_payload(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> dict[str, Any]:
    param_names = _cache_param_names(plan, candidate, hooks, signal_only=signal_only)
    return {
        "strategy_id": plan.strategy_id,
        "strategy_version": plan.strategy_version,
        "engine": GRID_V2_ENGINE_VERSION,
        "data": context.data_fingerprint,
        "trade_start_idx": int(context.trade_start_idx),
        "function": context.function_fingerprint,
        "params": {
            name: _jsonable_value(candidate.params[name])
            for name in param_names
            if name in candidate.params
        },
    }


def _cache_key_payload_for_index(
    plan: GridV2Plan,
    candidate_index: int,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> tuple[Any, ...]:
    table = plan.candidate_table
    param_names = _cache_param_names_for_index(plan, candidate_index, hooks, signal_only=signal_only)
    params = tuple(
        (name, _hashable_jsonable_value(table.param_value_for_index(candidate_index, name)))
        for name in param_names
    )
    return (
        plan.strategy_id,
        plan.strategy_version,
        GRID_V2_ENGINE_VERSION,
        context.data_fingerprint,
        int(context.trade_start_idx),
        context.function_fingerprint,
        params,
    )


def _signal_cache_key(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
) -> str:
    return _stable_json(_cache_key_payload(plan, candidate, context, hooks, signal_only=True))


def _signal_cache_key_for_index(
    plan: GridV2Plan,
    candidate_index: int,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
) -> tuple[Any, ...]:
    return _cache_key_payload_for_index(plan, candidate_index, context, hooks, signal_only=True)


def _dataprep_cache_key(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
) -> str:
    payload = _cache_key_payload(plan, candidate, context, hooks, signal_only=False)
    payload["variant"] = candidate.variant_name
    payload["modes"] = dict(candidate.modes)
    return _stable_json(payload)


def _dataprep_cache_key_for_index(
    plan: GridV2Plan,
    candidate_index: int,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
) -> tuple[Any, ...]:
    table = plan.candidate_table
    variant_code = int(table.variant_codes[candidate_index])
    payload = _cache_key_payload_for_index(plan, candidate_index, context, hooks, signal_only=False)
    return (
        payload,
        ("variant", table.variant_names[variant_code]),
        ("modes", table.mode_tuples_by_variant[variant_code]),
    )


def _selected_candidate_indices(
    plan: GridV2Plan,
    candidate_indices: Sequence[int] | None,
) -> tuple[int, ...]:
    if candidate_indices is None:
        return tuple(range(plan.deduped_candidate_count))
    selected: list[int] = []
    for index in candidate_indices:
        selected.append(plan.candidate_table.validate_index(int(index)))
    return tuple(selected)


def _selected_candidates(
    plan: GridV2Plan,
    candidate_indices: Sequence[int] | None,
) -> tuple[GridV2Candidate, ...]:
    return tuple(plan.candidate_for_index(index) for index in _selected_candidate_indices(plan, candidate_indices))


def _can_use_table_config_packer(
    plan: GridV2Plan,
    hooks: GridV2StrategyHooks,
    candidate_indices: Sequence[int],
) -> bool:
    if hooks.normalize_params is None:
        return True
    if not candidate_indices:
        return True
    sample_positions = deterministic_candidate_subset_indices(
        len(candidate_indices),
        min(12, len(candidate_indices)),
    )
    for position in sample_positions:
        candidate_index = int(candidate_indices[position])
        raw = plan.candidate_table.params_for_index(candidate_index)
        normalized = hooks.normalize_params(dict(raw))
        for name in _KERNEL_CONFIG_PARAM_NAMES:
            raw_has_value = name in raw and raw.get(name) is not None
            normalized_has_value = name in normalized and normalized.get(name) is not None
            if not raw_has_value and not normalized_has_value:
                continue
            if _jsonable_value(raw.get(name)) != _jsonable_value(normalized.get(name)):
                return False
    return True


def _pack_table_config_arrays(
    plan: GridV2Plan,
    candidate_indices: Sequence[int],
    trade_start_idx: int,
) -> dict[str, np.ndarray]:
    table = plan.candidate_table
    packed_indices = tuple(int(index) for index in candidate_indices)

    def get_value(row_index: int, name: str, default: Any) -> Any:
        candidate_index = packed_indices[int(row_index)]
        if table.has_param_for_index(candidate_index, name):
            return table.param_value_for_index(candidate_index, name)
        return default

    def get_modes(row_index: int) -> Mapping[str, str]:
        return table.modes_for_index(packed_indices[int(row_index)])

    return pack_compiled_config_arrays_from_rows(
        row_count=len(packed_indices),
        get_value=get_value,
        get_modes=get_modes,
        trade_start_idx=trade_start_idx,
    )


def _normalized_candidate_params(
    hooks: GridV2StrategyHooks,
    params: Mapping[str, Any],
) -> Mapping[str, Any]:
    if hooks.normalize_params is None:
        return params
    return hooks.normalize_params(dict(params))


def _row_from_run(
    plan: GridV2Plan,
    candidate_index: int,
    run: V2RunResult,
    *,
    params: Mapping[str, Any] | None = None,
) -> GridV2ResultRow:
    table = plan.candidate_table
    params = params or table.params_for_index(candidate_index)
    result = run.strategy_result
    initial_balance = float(params.get("initialCapital", 100.0))
    core = compute_core_metrics_from_balance_and_trades(
        result.balance_curve,
        result.trades,
        initial_balance=initial_balance,
    )
    return GridV2ResultRow(
        candidate_id=table.candidate_id_for_index(candidate_index),
        semantic_key=table.semantic_key_for_index(candidate_index),
        canonical_identity=None,
        variant_name=table.variant_name_for_index(candidate_index),
        modes=table.modes_for_index(candidate_index),
        params=params,
        net_profit_pct=core.net_profit_pct,
        max_drawdown_pct=core.max_drawdown_pct,
        romad=core.romad,
        profit_factor=core.profit_factor,
        win_rate_pct=core.win_rate_pct,
        total_trades=core.total_trades,
        winning_trades=core.winning_trades,
        losing_trades=core.losing_trades,
        gross_profit=core.gross_profit,
        gross_loss=core.gross_loss,
        max_consecutive_losses=_max_consecutive_losses(result.trades),
        final_balance=core.final_balance,
        guardrail_summary=_guardrail_mapping(run.guardrail_summary),
        backend_kind=REFERENCE_BATCH_KIND,
    )


def _row_from_compiled_output(
    plan: GridV2Plan,
    candidate_index: int,
    values: Sequence[Any],
    *,
    params: Mapping[str, Any] | None = None,
) -> GridV2ResultRow:
    table = plan.candidate_table
    idx = int(candidate_index)
    variant_name = table.variant_names[int(table.variant_codes[idx])]
    semantic_key = (
        table.semantic_keys_by_row[idx]
        if table.semantic_keys_by_row is not None
        else table.semantic_key_for_index(idx)
    )
    params = dict(params or table.params_for_index(candidate_index))
    guardrail_summary = {
        "invalid_stop_distance_count": int(values[OUTPUT_INVALID_STOP_DISTANCE_COUNT]),
        "zero_size_entry_count": int(values[OUTPUT_ZERO_SIZE_ENTRY_COUNT]),
        "rejected_fill_count": int(values[OUTPUT_REJECTED_FILL_COUNT]),
        "margin_reject_count": int(values[OUTPUT_MARGIN_REJECT_COUNT]),
        "liquidation_count": int(values[OUTPUT_LIQUIDATION_COUNT]),
        "no_capital_halt": bool(int(values[OUTPUT_NO_CAPITAL_HALT])),
        "max_required_leverage": float(values[OUTPUT_MAX_REQUIRED_LEVERAGE]),
        "max_notional": float(values[OUTPUT_MAX_NOTIONAL]),
        "flags": int(values[OUTPUT_FLAGS]),
    }
    return GridV2ResultRow(
        candidate_id=idx + 1,
        semantic_key=semantic_key,
        canonical_identity=None,
        variant_name=variant_name,
        modes=table.profile.variants[variant_name].modes,
        params=params,
        net_profit_pct=float(values[OUTPUT_NET_PROFIT_PCT]),
        max_drawdown_pct=float(values[OUTPUT_MAX_DRAWDOWN_PCT]),
        romad=float(values[OUTPUT_ROMAD]),
        profit_factor=float(values[OUTPUT_PROFIT_FACTOR]),
        win_rate_pct=float(values[OUTPUT_WIN_RATE_PCT]),
        total_trades=int(values[OUTPUT_TOTAL_TRADES]),
        winning_trades=int(values[OUTPUT_WINNING_TRADES]),
        losing_trades=int(values[OUTPUT_LOSING_TRADES]),
        gross_profit=float(values[OUTPUT_GROSS_PROFIT]),
        gross_loss=float(values[OUTPUT_GROSS_LOSS]),
        max_consecutive_losses=int(values[OUTPUT_MAX_CONSECUTIVE_LOSSES]),
        final_balance=float(values[OUTPUT_FINAL_BALANCE]),
        guardrail_summary=guardrail_summary,
        backend_kind=COMPILED_BATCH_KIND,
    )


def _error_row(plan: GridV2Plan, candidate_index: int, exc: Exception) -> GridV2ResultRow:
    table = plan.candidate_table
    return GridV2ResultRow(
        candidate_id=table.candidate_id_for_index(candidate_index),
        semantic_key=table.semantic_key_for_index(candidate_index),
        canonical_identity=None,
        variant_name=table.variant_name_for_index(candidate_index),
        modes=table.modes_for_index(candidate_index),
        params=table.params_for_index(candidate_index),
        net_profit_pct=float("nan"),
        max_drawdown_pct=float("nan"),
        romad=float("nan"),
        profit_factor=float("nan"),
        win_rate_pct=float("nan"),
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        gross_profit=float("nan"),
        gross_loss=float("nan"),
        max_consecutive_losses=0,
        final_balance=float("nan"),
        guardrail_summary={},
        status="error",
        error=str(exc),
    )


def _max_consecutive_losses(trades: Sequence[Any]) -> int:
    max_consecutive = 0
    consecutive = 0
    for trade in trades:
        if isinstance(trade, Mapping):
            pnl = float(trade["net_pnl"])
        elif hasattr(trade, "net_pnl"):
            pnl = float(trade.net_pnl)
        else:
            pnl = float(trade)
        if pnl <= 0.0:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
    return max_consecutive


def _guardrail_mapping(summary: GuardrailSummary) -> Mapping[str, Any]:
    if is_dataclass(summary):
        return _jsonable_mapping(asdict(summary))
    if isinstance(summary, Mapping):
        return _jsonable_mapping(summary)
    return {}


def _rank_rows(rows: Sequence[GridV2ResultRow], metric: str) -> list[GridV2ResultRow]:
    metric_name = "win_rate_pct" if metric == "win_rate" else metric
    minimize = metric_name in {"max_drawdown_pct"}

    def key(row: GridV2ResultRow) -> tuple[int, float, int]:
        if row.status != "ok":
            return (1, 0.0, row.candidate_id)
        value = float(getattr(row, metric_name, float("nan")))
        if not math.isfinite(value):
            return (1, 0.0, row.candidate_id)
        return (0, value if minimize else -value, row.candidate_id)

    return sorted(rows, key=key)


def _slow_enrich_selected(
    plan: GridV2Plan,
    context: _CacheKeyContext,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
    row: GridV2ResultRow,
    dataprep_cache: Mapping[str, ExecutionData],
) -> GridV2SelectedResult:
    if row.status != "ok":
        return GridV2SelectedResult(row=row, metrics={}, guardrail_summary={})
    candidate_index = plan.candidate_table.validate_candidate_id(row.candidate_id)
    data_key = _dataprep_cache_key_for_index(plan, candidate_index, context, hooks)
    data = dataprep_cache[data_key]
    params = plan.candidate_table.params_for_index(candidate_index)
    run = run_v2_strategy(
        data=data,
        profile=plan.profile,
        params=_normalized_candidate_params(hooks, params),
        trade_start_idx=trade_start_idx,
    )
    strategy_result = run.strategy_result
    metrics = {
        "net_profit_pct": getattr(strategy_result, "net_profit_pct", None),
        "max_drawdown_pct": getattr(strategy_result, "max_drawdown_pct", None),
        "romad": getattr(strategy_result, "romad", None),
        "profit_factor": getattr(strategy_result, "profit_factor", None),
        "win_rate_pct": getattr(strategy_result, "win_rate", None),
        "total_trades": getattr(strategy_result, "total_trades", None),
        "winning_trades": getattr(strategy_result, "winning_trades", None),
        "losing_trades": getattr(strategy_result, "losing_trades", None),
        "gross_profit": getattr(strategy_result, "gross_profit", None),
        "gross_loss": getattr(strategy_result, "gross_loss", None),
        "max_consecutive_losses": _max_consecutive_losses(strategy_result.trades),
        "sharpe_ratio": getattr(strategy_result, "sharpe_ratio", None),
        "sortino_ratio": getattr(strategy_result, "sortino_ratio", None),
        "sqn": getattr(strategy_result, "sqn", None),
        "ulcer_index": getattr(strategy_result, "ulcer_index", None),
        "consistency_score": getattr(strategy_result, "consistency_score", None),
    }
    return GridV2SelectedResult(
        row=row,
        metrics=_jsonable_mapping(metrics),
        guardrail_summary=_guardrail_mapping(run.guardrail_summary),
    )


__all__ = [
    "GRID_V2_ENGINE_VERSION",
    "COMPILED_BATCH_KIND",
    "REFERENCE_BATCH_KIND",
    "CandidateMappingRecord",
    "GridV2CacheEstimate",
    "GridV2CacheStats",
    "GridV2Candidate",
    "GridV2CandidateTable",
    "GridV2CountPreview",
    "GridV2ParameterDomain",
    "GridV2Plan",
    "GridV2ResultRow",
    "GridV2RunResult",
    "GridV2SelectedResult",
    "GridV2Settings",
    "GridV2StrategyHooks",
    "build_grid_v2_plan",
    "deterministic_candidate_subset_indices",
    "estimate_grid_v2_cache",
    "execute_grid_v2_candidates",
    "preview_grid_v2_counts",
    "run_grid_v2",
]
