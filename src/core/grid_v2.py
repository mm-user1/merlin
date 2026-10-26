"""Generic Backtester V2 Grid backend.

This module plans deterministic V2 candidate spaces and executes candidates
through generic V2 execution contracts. Strategy-specific code supplies only
signal/dataprep hooks.
"""

from __future__ import annotations

from core.entry_filters import MISSING, validate_context

from core.engine_v2.parameter_ties import enabled_parameter_ties, expand_parameter_ties

import hashlib
import inspect
import itertools
import json
import math
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import numpy as np
import pandas as pd

from core.engine_v2.contracts import ExecutionProfile, GuardrailSummary, VariantSpec
from core.engine_v2.diagnostics import V2Diagnostic, warning_messages
from core.engine_v2.execution_modes import (
    TRAIL_MODE_CHANDELIER,
    TRAIL_MODE_FIXED_AF_SAR,
    TRAIL_MODE_R_DISTANCE,
)
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
    OUTPUT_SHARPE_DAILY,
    OUTPUT_SHARPE_DAILY_ACTIVE_DAYS,
    OUTPUT_SHARPE_DAILY_OBSERVATIONS,
    OUTPUT_SHARPE_RATIO,
    OUTPUT_SQN,
    OUTPUT_TOTAL_TRADES,
    OUTPUT_WINNING_TRADES,
    OUTPUT_WIN_RATE_PCT,
    OUTPUT_ZERO_SIZE_ENTRY_COUNT,
    OUTPUT_COLUMN_COUNT,
    build_calendar_month_ids,
    ROUNDING_TICK_OUTWARD_CODE,
    build_stacked_execution_data,
    compiled_batch_available,
    compiled_unavailable_reason,
    evaluate_compiled_stacked_batch,
    _cached_timestamp_ns,
    _coerce_bool as _compiled_coerce_bool,
    _compiled_mode_state,
    _empty_config_arrays,
    validate_tick_size,
)
from core.metrics import build_utc_day_ids
from core.engine_v2.compiled_kernel_signal import (
    build_signal_stacked_execution_data,
    evaluate_compiled_signal_stacked_batch,
)
from core.engine_v2.metrics_kernel import compute_core_metrics_from_balance_and_trades
from core.engine_v2.profile import (
    ProfileValidationError,
    active_mode_values,
    active_parameter_names,
    canonical_selector_key,
    inactive_parameter_names,
    mode_binding_for,
    parse_execution_profile,
)
from core.engine_v2.runner import V2RunResult, run_v2_strategy
from core.engine_v2.runtime_contract import (
    V2_REBASABLE_DATE_PARAM_NAMES,
    V2_RESERVED_RUNTIME_PARAM_NAMES,
    V2_RUNTIME_CONTRACT_VERSION,
)
from core.grid_v2_sampling import (
    GRID_V2_ALLOCATOR_VERSION,
    GRID_V2_PLANNING_POLICY_VERSION,
    GRID_V2_SAMPLER_VERSION,
    sample_block_codes,
    validate_grid_v2_seed,
)


GRID_V2_ENGINE_VERSION = "grid_v2_phase2_5"
GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION = "grid_v2_plan_identity_v3"
GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION = "grid_v2_semantic_identity_v2"
REFERENCE_BATCH_KIND = "reference"
_BOOL_SIGNAL_ARRAYS = 2
_SIGNAL_TOPOLOGY_BOOL_SIGNAL_ARRAYS = 4
_FLOAT_DATAPREP_ARRAYS = 5
_BYTES_PER_MB = 1024.0 * 1024.0
_PLAN_REUSE_RUNTIME_PARAM_NAMES = V2_REBASABLE_DATE_PARAM_NAMES
_PLAN_IDENTITY_RUNTIME_PARAM_NAMES = frozenset(V2_RESERVED_RUNTIME_PARAM_NAMES)
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
    "trailDistanceR",
    "chandelierATRLength",
    "chandelierATRMult",
    "sarSpeed",
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
    compiled_config_packing: str = "table"
    primary_metric: str = "net_profit_pct"
    include_inactive_axes_for_dedup: bool = False
    planning_policy: str = "full"
    requested_budget: int = 200_000
    seed: int = 42
    allocation_method: str = "auto_sqrt_space"
    min_quota: float = 0.10
    manual_percents: tuple[tuple[str, float], ...] = ()
    enabled_tie_groups: tuple[str, ...] = ()
    admission_rules: Any = None
    entry_filter_config: tuple = ()

    @property
    def top_candidates(self) -> int:
        """Compatibility alias for storage helpers shared with Grid V1."""

        return self.top_n


@dataclass(frozen=True)
class GridV2StrategyHooks:
    """Strategy-specific data hooks consumed by the generic backend."""

    build_execution_data: Callable[[pd.DataFrame, Mapping[str, Any]], ExecutionData]
    build_execution_data_batch: Callable[[pd.DataFrame, Sequence[Mapping[str, Any]]], Sequence[ExecutionData]] | None = None
    normalize_params: Callable[[Mapping[str, Any] | None], Mapping[str, Any]] | None = None
    label: str = ""
    signal_param_names: tuple[str, ...] | None = None
    dataprep_param_names: tuple[str, ...] | None = None
    function_fingerprint: str | None = None
    entry_filter_context: Any = MISSING

    def __post_init__(self):
        from core.entry_filters import wrap_builders
        single, batch = wrap_builders(self.build_execution_data, self.build_execution_data_batch,
                                      self.entry_filter_context)
        object.__setattr__(self, "build_execution_data", single)
        object.__setattr__(self, "build_execution_data_batch", batch)

    @classmethod
    def from_strategy(cls, strategy: Any, *, entry_filter_context=MISSING, config=None) -> "GridV2StrategyHooks":
        from core.entry_filters import capability
        context = entry_filter_context
        if config is None:
            loader = getattr(strategy, "load_config", None)
            config = loader() if callable(loader) else {}
        validate_context(capability(config), context)
        builder = getattr(strategy, "build_v2_execution_data", None)
        if builder is None:
            builder = getattr(strategy, "build_execution_data", None)
        if builder is None or not callable(builder):
            raise TypeError("Grid V2 strategy hooks require a callable build_v2_execution_data.")
        batch_builder = getattr(strategy, "build_v2_execution_data_batch", None)
        if batch_builder is None:
            batch_builder = getattr(strategy, "build_execution_data_batch", None)
        if batch_builder is not None and not callable(batch_builder):
            batch_builder = None
        normalizer = getattr(strategy, "normalized_params", None)
        if normalizer is not None and not callable(normalizer):
            normalizer = None
        signal_names = _optional_name_tuple(getattr(strategy, "SIGNAL_CACHE_PARAM_NAMES", None))
        dataprep_names = _optional_name_tuple(getattr(strategy, "DATAPREP_CACHE_PARAM_NAMES", None))
        return cls(
            build_execution_data=builder,
            build_execution_data_batch=batch_builder,
            normalize_params=normalizer,
            label=str(getattr(strategy, "__name__", getattr(strategy, "__class__", type(strategy)).__name__)),
            signal_param_names=signal_names,
            dataprep_param_names=dataprep_names,
            entry_filter_context=context,
        )


@dataclass(frozen=True)
class GridV2ParameterDomain:
    name: str
    role: str | None
    values: tuple[Any, ...]
    default: Any
    is_axis: bool
    # Runtime fields no longer enter parameter_domains. This compatibility slot
    # stays in the v3 identity tuple and is therefore false for current plans.
    is_runtime: bool
    source: str


@dataclass(frozen=True)
class CandidateMappingRecord:
    candidate_id: int
    variant_name: str
    grid_mode_name: str
    semantic_key: str
    canonical_identity: str
    active_param_values: Mapping[str, Any]
    axis_param_values: Mapping[str, Any]


class _LazyCandidateParams(Mapping[str, Any]):
    """Mapping view that decodes one candidate's params on first access."""

    __slots__ = ("_table", "_index")

    def __init__(self, table: "GridV2CandidateTable", index: int) -> None:
        self._table = table
        self._index = int(index)

    def _params(self) -> Mapping[str, Any]:
        return self._table.params_for_index(self._index)

    def __getitem__(self, key: str) -> Any:
        return self._params()[key]

    def __iter__(self):
        return iter(self._params())

    def __len__(self) -> int:
        return len(self._params())

    def get(self, key: str, default: Any = None) -> Any:
        return self._params().get(key, default)


class _LazyCanonicalIdentity:
    """String-like lazy canonical identity for full-population fast rows."""

    __slots__ = ("_table", "_index")

    def __init__(self, table: "GridV2CandidateTable", index: int) -> None:
        self._table = table
        self._index = int(index)

    def _value(self) -> str:
        return self._table.canonical_identity_for_index(self._index)

    def __str__(self) -> str:
        return self._value()

    def __repr__(self) -> str:
        return repr(self._value())

    def __eq__(self, other: object) -> bool:
        return self._value() == str(other)


@dataclass(frozen=True)
class GridV2Candidate:
    candidate_id: int
    variant_name: str
    grid_mode_name: str
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
    grid_mode_names: tuple[str, ...]
    grid_mode_labels: Mapping[str, str]
    variant_name_by_grid_mode: Mapping[str, str]
    mode_tuples_by_variant: tuple[tuple[tuple[str, str], ...], ...]
    variant_codes: np.ndarray = field(repr=False)
    grid_mode_codes: np.ndarray = field(repr=False)
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
    parameter_ties: tuple[tuple[str, str], ...] = ()
    _params_cache: dict[int, Mapping[str, Any]] = field(default_factory=dict, init=False, repr=False)
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

    @property
    def params_materialized_count(self) -> int:
        if self.params_by_row is not None:
            return len(self.params_by_row)
        return len(self._params_cache)

    def release_params_cache(self) -> None:
        """Release currently retained lazy params without changing plan identity."""

        self._params_cache.clear()

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

    def grid_mode_name_for_index(self, index: int) -> str:
        idx = self.validate_index(index)
        return self.grid_mode_names[int(self.grid_mode_codes[idx])]

    def modes_for_index(self, index: int) -> dict[str, str]:
        variant_name = self.variant_name_for_index(index)
        return dict(self.profile.variants[variant_name].modes)

    def active_names_for_index(self, index: int) -> tuple[str, ...]:
        return self.active_names_by_variant[self.grid_mode_name_for_index(index)]

    def inactive_names_for_index(self, index: int) -> tuple[str, ...]:
        return self.inactive_names_by_variant[self.grid_mode_name_for_index(index)]

    def axis_names_for_index(self, index: int) -> tuple[str, ...]:
        return self.axis_names_by_variant[self.grid_mode_name_for_index(index)]

    def params_for_index(self, index: int) -> dict[str, Any]:
        idx = self.validate_index(index)
        if self.params_by_row is not None:
            return self.params_by_row[idx]  # type: ignore[return-value]
        cached = self._params_cache.get(idx)
        if cached is not None:
            return cached  # type: ignore[return-value]
        grid_mode_name = self.grid_mode_name_for_index(idx)
        params = dict(self.seed_params_by_variant[grid_mode_name])
        for column, name in enumerate(self.axis_names):
            code = int(self.axis_value_codes[idx, column]) if self.axis_value_codes.shape[1] else -1
            if code >= 0:
                params[name] = self.parameter_domains[name].values[code]
        expand_parameter_ties(params, self.parameter_ties)
        materialized = _jsonable_mapping(params)
        self._params_cache[idx] = materialized
        return materialized

    def param_value_for_index(self, index: int, name: str) -> Any:
        if self.parameter_ties:
            name = next((source for source, target in self.parameter_ties if target == name), name)
        idx = self.validate_index(index)
        if self.params_by_row is not None:
            return _jsonable_value(self.params_by_row[idx].get(name))
        grid_mode_name = self.grid_mode_name_for_index(idx)
        column = self.axis_column_by_name.get(name)
        if column is not None:
            code = int(self.axis_value_codes[idx, column]) if self.axis_value_codes.shape[1] else -1
            if code >= 0:
                return _jsonable_value(self.parameter_domains[name].values[code])
        return _jsonable_value(self.seed_params_by_variant[grid_mode_name].get(name))

    def has_param_for_index(self, index: int, name: str) -> bool:
        idx = self.validate_index(index)
        if self.params_by_row is not None:
            return name in self.params_by_row[idx]
        grid_mode_name = self.grid_mode_name_for_index(idx)
        if name in self.seed_params_by_variant[grid_mode_name]:
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
        grid_mode_name = self.grid_mode_name_for_index(index)
        return _semantic_payload(
            config={"id": self.strategy_id, "version": self.strategy_version},
            profile=self.profile,
            variant=self.profile.variants[variant_name],
            grid_mode_name=grid_mode_name,
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
                grid_mode_name=self.grid_mode_name_for_index(idx),
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
                grid_mode_name=self.grid_mode_name_for_index(idx),
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
            grid_mode_name=self.grid_mode_name_for_index(idx),
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
    full_raw_candidate_count: int = 0
    full_valid_candidate_count: int | None = None
    planned_candidate_count: int = 0
    per_block_counts: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    plan_fingerprint: str = ""
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
    mode_labels: Mapping[str, str] = field(default_factory=dict)
    full_raw_candidate_count: int = 0
    full_valid_candidate_count: int | None = None
    planned_candidate_count: int = 0
    requested_planning_policy: str = "full"
    effective_planning_policy: str = "full"
    effective_policy_reason: str = "requested_full"
    requested_budget: int = 0
    budget_is_operative: bool = False
    coverage_pct: float = 100.0
    per_block_counts: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    effective_allocation_method: str = "full_enumeration_v2"
    plan_identity_schema_version: str = GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION
    semantic_identity_schema_version: str = GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION
    runtime_contract_version: str = V2_RUNTIME_CONTRACT_VERSION
    planning_policy_version: str = GRID_V2_PLANNING_POLICY_VERSION
    sampler_version: str | None = None
    allocator_version: str = GRID_V2_ALLOCATOR_VERSION
    diagnostics: tuple[Mapping[str, Any], ...] = ()
    validation_warnings: tuple[str, ...] = ()


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
    month_id_nbytes: int = 0
    day_id_nbytes: int = 0
    mtm_sidecar_nbytes: int = 0
    admission_config_nbytes: int = 0
    chandelier_combo_count: int = 0
    chandelier_atr_nbytes: int = 0
    chandelier_mapping_nbytes: int = 0


@dataclass
class GridV2CacheStats:
    signal_hits: int = 0
    signal_misses: int = 0
    dataprep_hits: int = 0
    dataprep_misses: int = 0


_UNDEFINED_METRIC = float("nan")


@dataclass(frozen=True, slots=True)
class GridV2ResultRow:
    candidate_id: int
    semantic_key: str
    canonical_identity: Any
    variant_name: str
    grid_mode_name: str
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
    sharpe_ratio: float = _UNDEFINED_METRIC
    sqn: float = _UNDEFINED_METRIC
    sharpe_daily: float = _UNDEFINED_METRIC
    sharpe_daily_observations: Optional[int] = None
    sharpe_daily_active_days: Optional[int] = None
    max_drawdown_mtm_pct: float = _UNDEFINED_METRIC
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
    entry_diagnostics: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class GridV2RunResult:
    plan: GridV2Plan
    rows: tuple[GridV2ResultRow, ...]
    selected: tuple[GridV2SelectedResult, ...]
    cache_estimate: GridV2CacheEstimate
    cache_stats: GridV2CacheStats
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GridV2PlanReuseStats:
    enabled: bool
    build_count: int
    hit_count: int
    miss_count: int


@dataclass(frozen=True)
class GridV2PlanReuseResult:
    plan: GridV2Plan
    enabled: bool
    hit: bool
    cache_key: str | None
    plan_build_seconds: float
    plan_reuse_lookup_seconds: float
    runtime_rebase_seconds: float
    stats: GridV2PlanReuseStats


@dataclass(frozen=True)
class _GridV2PlanBlock:
    name: str
    label: str
    variant_name: str
    seed_params: Mapping[str, Any]
    active_names: tuple[str, ...]
    inactive_names: tuple[str, ...]
    axis_names: tuple[str, ...]
    # Enabled axes fixed by a logical block remain diagnostic-only state.
    fixed_axis_names: tuple[str, ...] = ()
    parameter_ties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class _GridV2PlanningResolution:
    requested_policy: str
    effective_policy: str
    effective_policy_reason: str
    requested_budget: int
    planned_candidate_count: int
    budget_is_operative: bool
    effective_seed: int | None
    effective_allocation_method: str | None
    effective_min_quota: float | None
    effective_manual_percents: tuple[tuple[str, float], ...]
    target_counts: Mapping[str, int]
    allocation_metadata: Mapping[str, Any]


@dataclass(frozen=True)
class _GridV2PlanPrelude:
    config: Mapping[str, Any]
    settings: GridV2Settings
    profile: ExecutionProfile
    fixed_params: Mapping[str, Any]
    domains: Mapping[str, GridV2ParameterDomain]
    selector_values: Mapping[str, Any]
    selected_variants: tuple[str, ...]
    blocks: tuple[_GridV2PlanBlock, ...]
    axis_names: tuple[str, ...]
    axis_column_by_name: Mapping[str, int]
    seed_params_by_variant: Mapping[str, Mapping[str, Any]]
    active_names_by_variant: Mapping[str, tuple[str, ...]]
    inactive_names_by_variant: Mapping[str, tuple[str, ...]]
    axis_names_by_variant: Mapping[str, tuple[str, ...]]
    per_variant_counts: Mapping[str, int]
    mode_labels: Mapping[str, str]
    raw_candidate_count: int
    planning: _GridV2PlanningResolution
    parameter_ties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class _GridV2CachedPlan:
    plan: GridV2Plan
    identity_signature: Any


class GridV2PlanReuseCache:
    """WFA-local immutable Grid V2 plan cache with per-window runtime rebasing."""

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = bool(enabled)
        self._entries: dict[str, _GridV2CachedPlan] = {}
        self._build_count = 0
        self._hit_count = 0
        self._miss_count = 0

    @property
    def stats(self) -> GridV2PlanReuseStats:
        return GridV2PlanReuseStats(
            enabled=self.enabled,
            build_count=self._build_count,
            hit_count=self._hit_count,
            miss_count=self._miss_count,
        )

    def get_or_build(
        self,
        config: Mapping[str, Any],
        *,
        settings: GridV2Settings | None = None,
        base_params: Mapping[str, Any] | None = None,
    ) -> GridV2PlanReuseResult:
        settings = settings or GridV2Settings()
        if not self.enabled:
            build_started = time.time()
            plan = build_grid_v2_plan(config, settings=settings, base_params=base_params)
            return GridV2PlanReuseResult(
                plan=plan,
                enabled=False,
                hit=False,
                cache_key=None,
                plan_build_seconds=time.time() - build_started,
                plan_reuse_lookup_seconds=0.0,
                runtime_rebase_seconds=0.0,
                stats=self.stats,
            )

        lookup_started = time.time()
        prelude = _grid_v2_plan_prelude(config, settings, base_params)
        cache_key = _grid_v2_plan_reuse_key(prelude)
        fresh_signature = _grid_v2_prelude_identity_signature(prelude)
        entry = self._entries.get(cache_key)
        lookup_seconds = time.time() - lookup_started
        if entry is not None and entry.identity_signature == fresh_signature:
            rebase_started = time.time()
            rebased = _rebase_grid_v2_plan(entry.plan, prelude)
            self._hit_count += 1
            return GridV2PlanReuseResult(
                plan=rebased,
                enabled=True,
                hit=True,
                cache_key=cache_key,
                plan_build_seconds=0.0,
                plan_reuse_lookup_seconds=lookup_seconds,
                runtime_rebase_seconds=time.time() - rebase_started,
                stats=self.stats,
            )

        self._miss_count += 1
        build_started = time.time()
        plan = build_grid_v2_plan(config, settings=settings, base_params=base_params)
        build_seconds = time.time() - build_started
        self._build_count += 1
        self._entries[cache_key] = _GridV2CachedPlan(
            plan=plan,
            identity_signature=_grid_v2_plan_identity_signature(plan),
        )
        return GridV2PlanReuseResult(
            plan=plan,
            enabled=True,
            hit=False,
            cache_key=cache_key,
            plan_build_seconds=build_seconds,
            plan_reuse_lookup_seconds=lookup_seconds,
            runtime_rebase_seconds=0.0,
            stats=self.stats,
        )


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
    trail_mode: str


@dataclass(frozen=True, slots=True)
class _CacheGroupPlan:
    codes: tuple[int, ...]
    representatives: Mapping[int, int]


@dataclass(frozen=True, slots=True)
class _CacheKeyChunk:
    records: tuple[_CandidateCacheKeys, ...]
    estimate: GridV2CacheEstimate


def build_grid_v2_plan(
    config: Mapping[str, Any],
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
) -> GridV2Plan:
    """Build a deterministic candidate plan from V2 config/profile metadata."""

    prelude = _grid_v2_plan_prelude(config, settings or GridV2Settings(), base_params)
    settings = prelude.settings
    config_copy = prelude.config
    profile = prelude.profile
    domains = prelude.domains
    selected_variants = prelude.selected_variants
    if prelude.planning.effective_policy == "full":
        candidate_table = _build_candidate_table(
            config=config_copy,
            settings=settings,
            profile=profile,
            fixed_params=prelude.fixed_params,
            domains=domains,
            selector_values=prelude.selector_values,
            selected_variants=selected_variants,
        )
        sampling_diagnostics: Mapping[str, Any] = {}
    else:
        candidate_table, sampling_diagnostics = _build_sampled_candidate_table(prelude)
    full_valid_candidate_count = (
        candidate_table.deduped_candidate_count
        if settings.include_inactive_axes_for_dedup
        else prelude.raw_candidate_count
    )
    diagnostics = _planning_diagnostics(prelude)
    diagnostic_payloads = tuple(item.to_dict() for item in diagnostics)
    validation_warnings = warning_messages(diagnostics)

    per_block_counts = {
        block.name: {
            "label": block.label,
            "variant_name": block.variant_name,
            "raw_count": int(prelude.per_variant_counts[block.name]),
            "full_count": int(prelude.per_variant_counts[block.name]),
            "full_valid_count": int(prelude.per_variant_counts[block.name]),
            "target_count": int(prelude.planning.target_counts[block.name]),
            "delivered_count": int(candidate_table.per_variant_counts[block.name]),
            "generation_mode": (
                "full_enumeration_v2"
                if prelude.planning.target_counts[block.name] >= prelude.per_variant_counts[block.name]
                else GRID_V2_SAMPLER_VERSION
            ),
        }
        for block in prelude.blocks
    }
    planning_identity = _grid_v2_planning_identity_payload(prelude)
    effective_fingerprint_identity = dict(planning_identity)
    effective_fingerprint_identity.pop("requested_policy", None)
    effective_fingerprint_identity.pop("effective_policy_reason", None)
    plan_fingerprint = _grid_v2_plan_fingerprint(
        planning_identity=effective_fingerprint_identity,
        effective_policy=prelude.planning.effective_policy,
        semantic_keys=candidate_table.semantic_keys_by_row or (),
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
        "grid_mode_order": list(candidate_table.grid_mode_names),
        "grid_mode_labels": dict(candidate_table.grid_mode_labels),
        "resolved_internal_variants": dict(candidate_table.variant_name_by_grid_mode),
        "semantic_dedup_count": candidate_table.semantic_dedup_count,
        "diagnostics": diagnostic_payloads,
        "validation_warnings": validation_warnings,
        "planning": {
            "plan_identity_schema_version": GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION,
            "semantic_identity_schema_version": GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
            "runtime_contract_version": V2_RUNTIME_CONTRACT_VERSION,
            "planning_policy_version": GRID_V2_PLANNING_POLICY_VERSION,
            "sampler_version": (
                GRID_V2_SAMPLER_VERSION if prelude.planning.effective_policy == "sampled" else None
            ),
            "allocator_version": GRID_V2_ALLOCATOR_VERSION,
            "requested_policy": prelude.planning.requested_policy,
            "effective_policy": prelude.planning.effective_policy,
            "effective_policy_reason": prelude.planning.effective_policy_reason,
            "requested_budget": prelude.planning.requested_budget,
            "budget_is_operative": prelude.planning.budget_is_operative,
            "configured_seed": settings.seed,
            "effective_seed": prelude.planning.effective_seed,
            "configured_allocation_method": settings.allocation_method,
            "effective_allocation_method": prelude.planning.effective_allocation_method,
            "configured_min_quota": settings.min_quota,
            "effective_min_quota": prelude.planning.effective_min_quota,
            "configured_manual_percents": dict(settings.manual_percents),
            "effective_manual_percents": dict(prelude.planning.effective_manual_percents),
            "full_raw_candidate_count": prelude.raw_candidate_count,
            "full_valid_candidate_count": full_valid_candidate_count,
            "planned_candidate_count": len(candidate_table),
            "per_block_counts": per_block_counts,
            "allocation": prelude.planning.allocation_metadata,
            "sampling_diagnostics": sampling_diagnostics,
            "plan_fingerprint": plan_fingerprint,
            "identity": planning_identity,
        },
        "candidate_table": {
            "enabled": True,
            "layout": "typed_lazy",
            "axis_count": len(candidate_table.axis_names),
            "variant_count": len(candidate_table.variant_names),
            "grid_mode_count": len(candidate_table.grid_mode_names),
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
        full_raw_candidate_count=prelude.raw_candidate_count,
        full_valid_candidate_count=full_valid_candidate_count,
        planned_candidate_count=len(candidate_table),
        per_block_counts=per_block_counts,
        plan_fingerprint=plan_fingerprint,
        metadata=metadata,
    )


def _grid_v2_plan_reuse_key(
    prelude: _GridV2PlanPrelude,
) -> str:
    payload = _grid_v2_planning_identity_payload(prelude)
    return hashlib.blake2b(_stable_json(payload).encode("utf-8"), digest_size=16).hexdigest()


def _grid_v2_plan_prelude(
    config: Mapping[str, Any],
    settings: GridV2Settings,
    base_params: Mapping[str, Any] | None,
) -> _GridV2PlanPrelude:
    from core.grid_engine import normalize_grid_v2_planning_policy, parse_grid_budget

    enabled_parameter_ties(config, settings.enabled_tie_groups)
    if settings.enabled_tie_groups:
        settings = replace(settings, enabled_tie_groups=tuple(
            group["id"] for group in config["optimization_rules"]["parameter_tie_groups"]
            if group["id"] in settings.enabled_tie_groups
        ))
    supplied = base_params or {}
    reserved_option_names = {
        f"{name}_options" for name in V2_RESERVED_RUNTIME_PARAM_NAMES
    }
    invalid_runtime_options = sorted(reserved_option_names & set(supplied))
    if invalid_runtime_options:
        raise ValueError(
            "Grid V2 reserved runtime fields cannot be supplied as option subsets: "
            f"{invalid_runtime_options}."
        )
    manual_values: dict[str, float] = {}
    for raw_name, raw_percent in settings.manual_percents:
        name = str(raw_name).strip()
        percent = float(raw_percent)
        if not name or not math.isfinite(percent):
            raise ValueError("Grid V2 manual allocation requires named finite percentages.")
        manual_values[name] = percent
    manual_percents = tuple(sorted(manual_values.items()))
    min_quota = float(settings.min_quota)
    if not math.isfinite(min_quota) or min_quota < 0.0:
        raise ValueError("Grid V2 min quota must be a finite non-negative number.")
    settings = replace(
        settings,
        planning_policy=normalize_grid_v2_planning_policy(settings.planning_policy),
        requested_budget=parse_grid_budget(settings.requested_budget),
        seed=validate_grid_v2_seed(settings.seed),
        min_quota=min_quota,
        manual_percents=manual_percents,
    )
    config_copy = _config_with_settings(config, settings)
    profile = parse_execution_profile(config_copy)
    params_spec = _parameters(config_copy)
    defaults = _parameter_defaults(params_spec)
    fixed_params = dict(defaults)
    fixed_params.update(dict(base_params or {}))
    if profile.modes.get("entryAdmission") == "risk_bracket_v1":
        from .engine_v2.admission import execution_admission
        if "maxEntryLeverage" in (settings.enabled_axes or ()) or "maxEntryLeverage_options" in supplied:
            raise ValueError("maxEntryLeverage is fixed and cannot be a Grid axis or option subset.")
        cap, _ = execution_admission(profile, fixed_params, settings.admission_rules)
        fixed_params["maxEntryLeverage"] = cap
    selector_values = _selector_values_by_variant(config_copy, profile)
    selected_variants = _selected_variants(profile, settings, fixed_params)
    fixed_params = _canonicalize_globally_inactive_params(
        params_spec,
        profile,
        fixed_params,
        selector_values,
        selected_variants,
    )
    pairs = enabled_parameter_ties(config_copy, settings.enabled_tie_groups)
    if pairs:
        enabled = set(settings.enabled_axes) if settings.enabled_axes is not None else {
            name for name, spec in params_spec.items()
            if _axis_available(name, spec.get("optimize", {}), profile)
            and _axis_enabled(name, spec.get("optimize", {}), settings)
        }
        unknown = enabled - set(params_spec)
        if unknown:
            raise ValueError(f"Grid V2 enabled_axes contains unknown parameter(s): {sorted(unknown)}.")
        for source, target in pairs:
            if target in enabled and source not in enabled:
                raise ValueError(f"Tied target '{target}' cannot be optimized alone; use shared source '{source}'.")
            fixed_params[target] = fixed_params[source]
        settings = replace(settings, enabled_axes=tuple(name for name in params_spec if name in enabled and name not in {t for _, t in pairs}))
    domains = _build_parameter_domains(config_copy, settings, fixed_params, profile)
    for source, target in pairs:
        source_domain = domains[source]
        spec = params_spec[target]
        for value in source_domain.values:
            if not spec["min"] <= value <= spec["max"]:
                raise ValueError(f"Shared source '{source}' is outside target '{target}' domain.")
        domains[target] = replace(domains[target], values=source_domain.values, default=source_domain.default, is_axis=False, source="parameter_tie")
        fixed_params[source] = source_domain.default if source_domain.is_axis else source_domain.values[0]
        fixed_params[target] = fixed_params[source]
    return _grid_v2_plan_prelude_from_parts(
        config=config_copy,
        settings=settings,
        profile=profile,
        fixed_params=fixed_params,
        domains=domains,
        selector_values=selector_values,
        selected_variants=selected_variants,
    )


def _grid_v2_plan_prelude_from_parts(
    *,
    config: Mapping[str, Any],
    settings: GridV2Settings,
    profile: ExecutionProfile,
    fixed_params: Mapping[str, Any],
    domains: Mapping[str, GridV2ParameterDomain],
    selector_values: Mapping[str, Any],
    selected_variants: Sequence[str],
) -> _GridV2PlanPrelude:
    axis_names = tuple(
        name for name in profile.parameter_names
        if name in domains and domains[name].is_axis
    )
    axis_columns = {name: index for index, name in enumerate(axis_names)}
    blocks = _build_planning_blocks(
        config=config,
        settings=settings,
        profile=profile,
        fixed_params=fixed_params,
        domains=domains,
        selector_values=selector_values,
        selected_variants=selected_variants,
    )
    seed_params_by_variant: dict[str, dict[str, Any]] = {}
    active_names_by_variant: dict[str, tuple[str, ...]] = {}
    inactive_names_by_variant: dict[str, tuple[str, ...]] = {}
    axis_names_by_variant: dict[str, tuple[str, ...]] = {}
    per_variant_counts: dict[str, int] = {}
    mode_labels: dict[str, str] = {}
    raw_count = 0

    for block in blocks:
        count = _product_size(domains[name].values for name in block.axis_names)
        seed_params_by_variant[block.name] = dict(block.seed_params)
        active_names_by_variant[block.name] = block.active_names
        inactive_names_by_variant[block.name] = block.inactive_names
        axis_names_by_variant[block.name] = block.axis_names
        per_variant_counts[block.name] = count
        mode_labels[block.name] = block.label
        raw_count += count

    planning = _resolve_grid_v2_planning(
        config=config,
        settings=settings,
        profile=profile,
        domains=domains,
        blocks=blocks,
        full_counts=per_variant_counts,
        full_count=raw_count,
    )

    return _GridV2PlanPrelude(
        config=config,
        settings=settings,
        profile=profile,
        fixed_params=fixed_params,
        domains=domains,
        selector_values=selector_values,
        selected_variants=tuple(selected_variants),
        blocks=blocks,
        axis_names=axis_names,
        axis_column_by_name=axis_columns,
        seed_params_by_variant=seed_params_by_variant,
        active_names_by_variant=active_names_by_variant,
        inactive_names_by_variant=inactive_names_by_variant,
        axis_names_by_variant=axis_names_by_variant,
        per_variant_counts=per_variant_counts,
        mode_labels=mode_labels,
        raw_candidate_count=raw_count,
        planning=planning,
        parameter_ties=enabled_parameter_ties(config, settings.enabled_tie_groups),
    )


def _resolve_grid_v2_planning(
    *,
    config: Mapping[str, Any],
    settings: GridV2Settings,
    profile: ExecutionProfile,
    domains: Mapping[str, GridV2ParameterDomain],
    blocks: Sequence[_GridV2PlanBlock],
    full_counts: Mapping[str, int],
    full_count: int,
) -> _GridV2PlanningResolution:
    from core.grid_engine import allocate_ordered_block_budgets

    requested_policy = settings.planning_policy
    full_targets = {block.name: int(full_counts[block.name]) for block in blocks}
    if requested_policy == "full":
        return _GridV2PlanningResolution(
            requested_policy="full",
            effective_policy="full",
            effective_policy_reason="requested_full",
            requested_budget=settings.requested_budget,
            planned_candidate_count=full_count,
            budget_is_operative=False,
            effective_seed=None,
            effective_allocation_method="full_enumeration_v2",
            effective_min_quota=None,
            effective_manual_percents=(),
            target_counts=full_targets,
            allocation_metadata={},
        )

    if settings.include_inactive_axes_for_dedup:
        raise ValueError(
            "Sampled Grid V2 planning does not support include_inactive_axes_for_dedup=True."
        )
    _validate_sampled_dependency_axes(config, blocks)
    if settings.requested_budget >= full_count:
        return _GridV2PlanningResolution(
            requested_policy="sampled",
            effective_policy="full",
            effective_policy_reason="budget_covers_full_space",
            requested_budget=settings.requested_budget,
            planned_candidate_count=full_count,
            budget_is_operative=False,
            effective_seed=None,
            effective_allocation_method="full_enumeration_v2",
            effective_min_quota=None,
            effective_manual_percents=(),
            target_counts=full_targets,
            allocation_metadata={},
        )

    _prove_sampled_blocks_semantically_disjoint(profile, blocks)
    allocation = allocate_ordered_block_budgets(
        [block.name for block in blocks],
        full_targets,
        settings.requested_budget,
        method=settings.allocation_method,
        min_quota=settings.min_quota,
        manual_percents=dict(settings.manual_percents),
        reject_unknown_manual=True,
    )
    nonempty_blocks = sum(1 for count in full_targets.values() if count > 0)
    if allocation.allocation_method != "manual" and allocation.actual_budget < nonempty_blocks:
        raise ValueError(
            "Sampled Grid V2 budget must be at least the number of non-empty planning blocks "
            f"({nonempty_blocks}); increase the budget or disable blocks."
        )
    if allocation.allocation_method != "manual":
        allocation = _reconcile_grid_v2_automatic_allocation(
            allocation,
            block_order=tuple(block.name for block in blocks),
        )
    return _GridV2PlanningResolution(
        requested_policy="sampled",
        effective_policy="sampled",
        effective_policy_reason="budget_below_full_space",
        requested_budget=settings.requested_budget,
        planned_candidate_count=allocation.actual_budget,
        budget_is_operative=True,
        effective_seed=settings.seed,
        effective_allocation_method=allocation.allocation_method,
        effective_min_quota=float(settings.min_quota),
        effective_manual_percents=settings.manual_percents if allocation.allocation_method == "manual" else (),
        target_counts=dict(allocation.mode_budgets),
        allocation_metadata=asdict(allocation),
    )


def _reconcile_grid_v2_automatic_allocation(
    allocation: Any,
    *,
    block_order: Sequence[str],
) -> Any:
    """Give every non-empty V2 automatic block one row without changing successes."""

    if allocation.allocation_method not in {"auto_sqrt_space", "proportional_space"}:
        return allocation
    ordered = tuple(str(name) for name in block_order)
    budgets = dict(allocation.mode_budgets)
    sizes = dict(allocation.mode_space_sizes)
    zero_targets = [name for name in ordered if sizes.get(name, 0) > 0 and budgets.get(name, 0) <= 0]
    if not zero_targets:
        return allocation

    order_index = {name: index for index, name in enumerate(ordered)}
    for zero_name in zero_targets:
        donors = [name for name in ordered if budgets.get(name, 0) > 1]
        if not donors:
            raise RuntimeError("Grid V2 automatic allocation could not cover every non-empty block.")
        donor = min(donors, key=lambda name: (-int(budgets[name]), order_index[name]))
        budgets[zero_name] = 1
        budgets[donor] -= 1

    if sum(budgets.values()) != allocation.actual_budget:
        raise RuntimeError("Grid V2 automatic allocation reconciliation changed the total budget.")
    if any(budgets[name] < 1 for name in ordered if sizes.get(name, 0) > 0):
        raise RuntimeError("Grid V2 automatic allocation left a non-empty block uncovered.")
    if any(budgets[name] > sizes.get(name, 0) for name in ordered):
        raise RuntimeError("Grid V2 automatic allocation exceeded a block capacity.")
    coverage = {
        name: (budgets[name] / sizes[name] * 100.0) if sizes.get(name, 0) > 0 else 0.0
        for name in ordered
    }
    return replace(allocation, mode_budgets=budgets, mode_coverage_pct=coverage)


def _validate_sampled_dependency_axes(
    config: Mapping[str, Any],
    blocks: Sequence[_GridV2PlanBlock],
) -> None:
    specs = _parameters(config)
    for block in blocks:
        axes = set(block.axis_names)
        for child_name, raw_spec in specs.items():
            if not isinstance(raw_spec, Mapping):
                continue
            for parent_name in _dependency_names(raw_spec.get("depends_on")):
                if parent_name in axes:
                    raise ValueError(
                        "Sampled Grid V2 planning cannot vary depends_on parent "
                        f"'{parent_name}' in block '{block.name}' because child "
                        f"'{child_name}' changes the active schema."
                    )


def _prove_sampled_blocks_semantically_disjoint(
    profile: ExecutionProfile,
    blocks: Sequence[_GridV2PlanBlock],
) -> None:
    for left_index, left in enumerate(blocks):
        for right in blocks[left_index + 1 :]:
            if left.variant_name != right.variant_name:
                continue
            left_modes = tuple(sorted(profile.variants[left.variant_name].modes.items()))
            right_modes = tuple(sorted(profile.variants[right.variant_name].modes.items()))
            if left_modes != right_modes:
                continue
            shared_active = set(left.active_names) & set(right.active_names)
            fixed_discriminator = any(
                name not in {target for _, target in left.parameter_ties + right.parameter_ties}
                and name not in left.axis_names
                and name not in right.axis_names
                and _hashable_jsonable_value(left.seed_params.get(name))
                != _hashable_jsonable_value(right.seed_params.get(name))
                for name in shared_active
            )
            if fixed_discriminator:
                continue
            raise ValueError(
                "Sampled Grid V2 cannot prove semantic disjointness between planning blocks "
                f"'{left.name}' and '{right.name}'. Block names/grid_mode alone are not semantic identity."
            )


def _grid_v2_prelude_identity_signature(prelude: _GridV2PlanPrelude) -> Any:
    return _stable_json(_grid_v2_planning_identity_payload(prelude))


def _planning_diagnostics(prelude: _GridV2PlanPrelude) -> tuple[V2Diagnostic, ...]:
    """Return deterministic profile and selected-run planning warnings."""

    diagnostics = list(prelude.profile.diagnostics)
    covered_axis_names = {
        name
        for block in prelude.blocks
        for name in (*block.axis_names, *block.fixed_axis_names)
    }
    selected_variants = ", ".join(prelude.selected_variants)
    for name in prelude.profile.parameter_names:
        domain = prelude.domains.get(name)
        if domain is None or not domain.is_axis or name in covered_axis_names:
            continue
        diagnostics.append(
            V2Diagnostic(
                severity="warning",
                code="V2_INACTIVE_ENABLED_AXIS",
                strategy_id=prelude.profile.strategy_id,
                path=f"parameters.{name}",
                variant=None,
                message=(
                    f"{prelude.profile.strategy_id}: enabled axis '{name}' is bound but inactive "
                    f"for selected variant(s) [{selected_variants}] and does not multiply candidates."
                ),
            )
        )
    return tuple(diagnostics)


def _grid_v2_plan_identity_signature(plan: GridV2Plan) -> Any:
    return _stable_json(dict(plan.metadata.get("planning", {})).get("identity", {}))


def _grid_v2_planning_identity_payload(prelude: _GridV2PlanPrelude) -> dict[str, Any]:
    planning = prelude.planning
    return {
        **({"entry_filters": list(prelude.settings.entry_filter_config)} if prelude.settings.entry_filter_config else {}),
        **({"entry_admission_rules": prelude.settings.admission_rules.semantic()}
           if prelude.profile.modes.get("entryAdmission") == "risk_bracket_v1" else {}),
        **({"grid_v2_parameter_ties_v1": {"groups": list(prelude.settings.enabled_tie_groups), "pairs": prelude.parameter_ties}} if prelude.parameter_ties else {}),
        "engine": GRID_V2_ENGINE_VERSION,
        "plan_identity_schema_version": GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION,
        "semantic_identity_schema_version": GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
        "runtime_contract_version": V2_RUNTIME_CONTRACT_VERSION,
        "planning_policy_version": GRID_V2_PLANNING_POLICY_VERSION,
        "sampler_version": GRID_V2_SAMPLER_VERSION,
        "allocator_version": GRID_V2_ALLOCATOR_VERSION,
        "strategy_id": str(prelude.config.get("id", prelude.profile.strategy_id)),
        "strategy_version": str(prelude.config.get("version", "")),
        "requested_policy": planning.requested_policy,
        "effective_policy": planning.effective_policy,
        "effective_policy_reason": planning.effective_policy_reason,
        "planned_candidate_count": planning.planned_candidate_count,
        "effective_seed": planning.effective_seed,
        "effective_allocation_method": planning.effective_allocation_method,
        "effective_min_quota": planning.effective_min_quota,
        "effective_manual_percents": list(planning.effective_manual_percents),
        "selected_variants": list(prelude.selected_variants),
        "axis_names": list(prelude.axis_names),
        "domains": _domain_identity_signature(prelude.domains),
        "blocks": [
            {
                "name": block.name,
                "variant_name": block.variant_name,
                "modes": [
                    [str(name), _jsonable_value(value)]
                    for name, value in sorted(
                        prelude.profile.variants[block.variant_name].modes.items()
                    )
                ],
                "label": block.label,
                "active_names": list(block.active_names),
                "inactive_names": list(block.inactive_names),
                "axis_names": list(block.axis_names),
                "full_count": int(prelude.per_variant_counts[block.name]),
                "target_count": int(planning.target_counts[block.name]),
                "seed_params": _jsonable_mapping(
                    _without_plan_reuse_runtime_params(prelude.seed_params_by_variant[block.name])
                ),
            }
            for block in prelude.blocks
        ],
    }


def _grid_v2_plan_fingerprint(
    *,
    planning_identity: Mapping[str, Any],
    effective_policy: str,
    semantic_keys: Iterable[str],
) -> str:
    """Hash canonical plan identity and ordered keys with constant extra memory."""

    header = {
        "effective_policy": str(effective_policy),
        "plan_identity_schema_version": GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION,
        "planning_identity": dict(planning_identity),
    }
    digest = hashlib.blake2b(digest_size=32)
    digest.update(_stable_json(header).encode("utf-8"))
    for semantic_key in semantic_keys:
        digest.update(b"\n")
        digest.update(str(semantic_key).encode("utf-8"))
    return digest.hexdigest()


def _domain_identity_signature(domains: Mapping[str, GridV2ParameterDomain]) -> tuple[Any, ...]:
    # Runtime fields are absent from current domains. Retain the v3 tuple shape
    # and compatibility branches so identity bytes remain unchanged.
    return tuple(
        (
            name,
            str(domain.role) if domain.role is not None else None,
            ()
            if name in _PLAN_IDENTITY_RUNTIME_PARAM_NAMES
            else tuple(_jsonable_value(value) for value in domain.values),
            None if name in _PLAN_IDENTITY_RUNTIME_PARAM_NAMES else _jsonable_value(domain.default),
            bool(domain.is_axis),
            bool(domain.is_runtime),
            str(domain.source),
        )
        for name, domain in domains.items()
    )


def _without_plan_reuse_runtime_params(params: Mapping[str, Any]) -> dict[str, Any]:
    # Seed params intentionally retain all runtime values for execution-config
    # packing and row compatibility. Identity excludes all four; the public WFA
    # rebasable-date contract remains the separate three-name constant.
    return {
        str(name): value
        for name, value in params.items()
        if str(name) not in _PLAN_IDENTITY_RUNTIME_PARAM_NAMES
    }


def _rebase_grid_v2_plan(cached_plan: GridV2Plan, prelude: _GridV2PlanPrelude) -> GridV2Plan:
    cached_table = cached_plan.candidate_table
    rebased_table = replace(
        cached_table,
        parameter_ties=prelude.parameter_ties,
        profile=prelude.profile,
        parameter_domains=prelude.domains,
        axis_names=prelude.axis_names,
        axis_column_by_name=prelude.axis_column_by_name,
        grid_mode_names=tuple(block.name for block in prelude.blocks),
        grid_mode_labels=prelude.mode_labels,
        variant_name_by_grid_mode={block.name: block.variant_name for block in prelude.blocks},
        mode_tuples_by_variant=tuple(
            tuple(
                sorted(
                    (str(name), str(value))
                    for name, value in prelude.profile.variants[variant].modes.items()
                )
            )
            for variant in prelude.selected_variants
        ),
        seed_params_by_variant=prelude.seed_params_by_variant,
        active_names_by_variant=prelude.active_names_by_variant,
        inactive_names_by_variant=prelude.inactive_names_by_variant,
        axis_names_by_variant=prelude.axis_names_by_variant,
    )
    metadata = dict(cached_plan.metadata)
    metadata["select_option_subsets"] = {
        name: list(domain.values)
        for name, domain in prelude.domains.items()
        if domain.is_axis and str(domain.source).endswith(".runtime_options")
    }
    metadata["grid_mode_order"] = [block.name for block in prelude.blocks]
    metadata["grid_mode_labels"] = dict(prelude.mode_labels)
    metadata["resolved_internal_variants"] = {
        block.name: block.variant_name for block in prelude.blocks
    }
    diagnostics = _planning_diagnostics(prelude)
    diagnostic_payloads = tuple(item.to_dict() for item in diagnostics)
    validation_warnings = warning_messages(diagnostics)
    metadata["diagnostics"] = diagnostic_payloads
    metadata["validation_warnings"] = validation_warnings
    planning_metadata = dict(metadata.get("planning", {}))
    planning_metadata.pop("diagnostics", None)
    planning_metadata.pop("validation_warnings", None)
    planning_metadata.update(
        {
            "plan_identity_schema_version": GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION,
            "semantic_identity_schema_version": GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
            "runtime_contract_version": V2_RUNTIME_CONTRACT_VERSION,
            "requested_policy": prelude.planning.requested_policy,
            "effective_policy": prelude.planning.effective_policy,
            "effective_policy_reason": prelude.planning.effective_policy_reason,
            "requested_budget": prelude.planning.requested_budget,
            "budget_is_operative": prelude.planning.budget_is_operative,
            "configured_seed": prelude.settings.seed,
            "effective_seed": prelude.planning.effective_seed,
            "configured_allocation_method": prelude.settings.allocation_method,
            "effective_allocation_method": prelude.planning.effective_allocation_method,
            "configured_min_quota": prelude.settings.min_quota,
            "effective_min_quota": prelude.planning.effective_min_quota,
            "configured_manual_percents": dict(prelude.settings.manual_percents),
            "effective_manual_percents": dict(prelude.planning.effective_manual_percents),
            "identity": _grid_v2_planning_identity_payload(prelude),
        }
    )
    metadata["planning"] = planning_metadata
    return replace(
        cached_plan,
        settings=prelude.settings,
        strategy_id=prelude.profile.strategy_id,
        strategy_version=str(prelude.config.get("version", "")),
        profile=prelude.profile,
        parameter_domains=prelude.domains,
        candidate_table=rebased_table,
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
    axis_names = tuple(
        name for name in profile.parameter_names
        if name in domains and domains[name].is_axis
    )
    axis_columns = {name: index for index, name in enumerate(axis_names)}
    blocks = _build_planning_blocks(
        config=config,
        settings=settings,
        profile=profile,
        fixed_params=fixed_params,
        domains=domains,
        selector_values=selector_values,
        selected_variants=selected_variants,
    )
    variant_code_by_name = {name: index for index, name in enumerate(selected_variants)}
    seed_params_by_variant: dict[str, dict[str, Any]] = {}
    active_names_by_variant: dict[str, tuple[str, ...]] = {}
    inactive_names_by_variant: dict[str, tuple[str, ...]] = {}
    axis_names_by_variant: dict[str, tuple[str, ...]] = {}
    per_variant_counts: dict[str, int] = {block.name: 0 for block in blocks}
    mode_labels: dict[str, str] = {block.name: block.label for block in blocks}
    variant_name_by_mode: dict[str, str] = {block.name: block.variant_name for block in blocks}
    semantic_seen: set[tuple[Any, ...]] = set()
    semantic_keys: list[str] = []
    variant_codes: list[int] = []
    grid_mode_codes: list[int] = []
    axis_value_codes: list[list[int]] = []
    raw_count = 0
    enumerated_count = 0

    for block_code, block in enumerate(blocks):
        variant = profile.variants[block.variant_name]
        seed_params_by_variant[block.name] = dict(block.seed_params)
        active_names_by_variant[block.name] = block.active_names
        inactive_names_by_variant[block.name] = block.inactive_names
        axis_names_by_variant[block.name] = block.axis_names

        value_groups = tuple(domains[name].values for name in block.axis_names)
        code_groups = tuple(range(len(domains[name].values)) for name in block.axis_names)
        raw_count += _product_size(value_groups)
        for values, codes in zip(itertools.product(*value_groups), itertools.product(*code_groups)):
            enumerated_count += 1
            params = dict(block.seed_params)
            params.update(zip(block.axis_names, values))
            expand_parameter_ties(params, block.parameter_ties)
            semantic_identity = _semantic_identity_tuple(
                config=config,
                profile=profile,
                variant=variant,
                params=params,
                active_names=block.active_names,
            )
            if semantic_identity in semantic_seen:
                continue
            semantic_seen.add(semantic_identity)
            jsonable_params = _jsonable_mapping(params)
            semantic_keys.append(
                _stable_json(
                    _semantic_payload(
                        config=config,
                        profile=profile,
                        variant=variant,
                        grid_mode_name=block.name,
                        params=jsonable_params,
                        active_names=block.active_names,
                    )
                )
            )
            row_codes = [-1] * len(axis_names)
            for name, code in zip(block.axis_names, codes):
                row_codes[axis_columns[name]] = int(code)
            variant_codes.append(int(variant_code_by_name[block.variant_name]))
            grid_mode_codes.append(int(block_code))
            axis_value_codes.append(row_codes)
            per_variant_counts[block.name] += 1

    axis_code_array = np.asarray(axis_value_codes, dtype=np.int32)
    if not axis_value_codes:
        axis_code_array = np.empty((0, len(axis_names)), dtype=np.int32)
    elif axis_code_array.ndim == 1:
        axis_code_array = axis_code_array.reshape((len(axis_value_codes), len(axis_names)))

    return GridV2CandidateTable(
        parameter_ties=enabled_parameter_ties(config, settings.enabled_tie_groups),
        strategy_id=str(config.get("id", profile.strategy_id)),
        strategy_version=str(config.get("version", "")),
        profile=profile,
        parameter_domains=domains,
        axis_names=axis_names,
        axis_column_by_name=axis_columns,
        variant_names=tuple(selected_variants),
        grid_mode_names=tuple(block.name for block in blocks),
        grid_mode_labels=mode_labels,
        variant_name_by_grid_mode=variant_name_by_mode,
        mode_tuples_by_variant=tuple(
            tuple(sorted((str(name), str(value)) for name, value in profile.variants[variant].modes.items()))
            for variant in selected_variants
        ),
        variant_codes=np.asarray(variant_codes, dtype=np.int32),
        grid_mode_codes=np.asarray(grid_mode_codes, dtype=np.int32),
        axis_value_codes=axis_code_array,
        semantic_keys_by_row=tuple(semantic_keys),
        params_by_row=None,
        seed_params_by_variant=seed_params_by_variant,
        active_names_by_variant=active_names_by_variant,
        inactive_names_by_variant=inactive_names_by_variant,
        axis_names_by_variant=axis_names_by_variant,
        raw_candidate_count=raw_count,
        enumerated_candidate_count=enumerated_count,
        semantic_dedup_count=enumerated_count - len(variant_codes),
        per_variant_counts=per_variant_counts,
    )


def _build_sampled_candidate_table(
    prelude: _GridV2PlanPrelude,
) -> tuple[GridV2CandidateTable, Mapping[str, Any]]:
    """Build only the allocated O(K) sampled rows in canonical block/index order."""

    config = prelude.config
    profile = prelude.profile
    domains = prelude.domains
    axis_names = prelude.axis_names
    axis_columns = prelude.axis_column_by_name
    variant_code_by_name = {name: index for index, name in enumerate(prelude.selected_variants)}
    semantic_seen: set[tuple[Any, ...]] = set()
    semantic_keys: list[str] = []
    variant_codes: list[int] = []
    grid_mode_codes: list[int] = []
    axis_value_codes: list[list[int]] = []
    delivered_counts: dict[str, int] = {block.name: 0 for block in prelude.blocks}
    diagnostics_by_block: dict[str, Any] = {}

    for block_code, block in enumerate(prelude.blocks):
        variant = profile.variants[block.variant_name]
        full_count = int(prelude.per_variant_counts[block.name])
        target_count = int(prelude.planning.target_counts[block.name])
        radices = tuple(len(domains[name].values) for name in block.axis_names)
        if target_count >= full_count:
            code_rows: Sequence[tuple[int, ...]] = tuple(itertools.product(*(range(radix) for radix in radices)))
            diagnostics_by_block[block.name] = {
                "generation_mode": "full_enumeration_v2",
                "full_count": full_count,
                "requested_count": target_count,
                "delivered_count": full_count,
                "shortfall_count": 0,
            }
        else:
            code_rows, block_diagnostics = sample_block_codes(
                radices,
                target_count,
                global_seed=int(prelude.planning.effective_seed),
                strategy_id=str(config.get("id", profile.strategy_id)),
                strategy_version=str(config.get("version", "")),
                block_id=block.name,
                axis_names=block.axis_names,
            )
            diagnostics_by_block[block.name] = {
                "generation_mode": GRID_V2_SAMPLER_VERSION,
                **block_diagnostics.to_dict(),
            }
        for codes in code_rows:
            params = dict(block.seed_params)
            params.update(
                (name, domains[name].values[int(code)])
                for name, code in zip(block.axis_names, codes)
            )
            expand_parameter_ties(params, block.parameter_ties)
            semantic_identity = _semantic_identity_tuple(
                config=config,
                profile=profile,
                variant=variant,
                params=params,
                active_names=block.active_names,
            )
            if semantic_identity in semantic_seen:
                raise RuntimeError(
                    "Sampled Grid V2 semantic-disjointness proof was violated while building "
                    f"block '{block.name}'."
                )
            semantic_seen.add(semantic_identity)
            jsonable_params = _jsonable_mapping(params)
            semantic_keys.append(
                _stable_json(
                    _semantic_payload(
                        config=config,
                        profile=profile,
                        variant=variant,
                        grid_mode_name=block.name,
                        params=jsonable_params,
                        active_names=block.active_names,
                    )
                )
            )
            row_codes = [-1] * len(axis_names)
            for name, code in zip(block.axis_names, codes):
                row_codes[axis_columns[name]] = int(code)
            variant_codes.append(int(variant_code_by_name[block.variant_name]))
            grid_mode_codes.append(int(block_code))
            axis_value_codes.append(row_codes)
            delivered_counts[block.name] += 1

    axis_code_array = np.asarray(axis_value_codes, dtype=np.int32)
    if not axis_value_codes:
        axis_code_array = np.empty((0, len(axis_names)), dtype=np.int32)
    elif axis_code_array.ndim == 1:
        axis_code_array = axis_code_array.reshape((len(axis_value_codes), len(axis_names)))
    table = GridV2CandidateTable(
        parameter_ties=prelude.parameter_ties,
        strategy_id=str(config.get("id", profile.strategy_id)),
        strategy_version=str(config.get("version", "")),
        profile=profile,
        parameter_domains=domains,
        axis_names=axis_names,
        axis_column_by_name=axis_columns,
        variant_names=tuple(prelude.selected_variants),
        grid_mode_names=tuple(block.name for block in prelude.blocks),
        grid_mode_labels=prelude.mode_labels,
        variant_name_by_grid_mode={block.name: block.variant_name for block in prelude.blocks},
        mode_tuples_by_variant=tuple(
            tuple(sorted((str(name), str(value)) for name, value in profile.variants[variant].modes.items()))
            for variant in prelude.selected_variants
        ),
        variant_codes=np.asarray(variant_codes, dtype=np.int32),
        grid_mode_codes=np.asarray(grid_mode_codes, dtype=np.int32),
        axis_value_codes=axis_code_array,
        semantic_keys_by_row=tuple(semantic_keys),
        params_by_row=None,
        seed_params_by_variant=prelude.seed_params_by_variant,
        active_names_by_variant=prelude.active_names_by_variant,
        inactive_names_by_variant=prelude.inactive_names_by_variant,
        axis_names_by_variant=prelude.axis_names_by_variant,
        raw_candidate_count=prelude.raw_candidate_count,
        enumerated_candidate_count=len(variant_codes),
        semantic_dedup_count=0,
        per_variant_counts=delivered_counts,
    )
    return table, diagnostics_by_block


def preview_grid_v2_counts(
    config: Mapping[str, Any],
    settings: GridV2Settings | None = None,
    base_params: Mapping[str, Any] | None = None,
) -> GridV2CountPreview:
    """Compute deterministic Grid V2 breadth without materializing candidates."""

    prelude = _grid_v2_plan_prelude(config, settings or GridV2Settings(), base_params)
    full_valid = None if prelude.settings.include_inactive_axes_for_dedup else prelude.raw_candidate_count
    planned = prelude.planning.planned_candidate_count
    per_block = {
        block.name: {
            "label": block.label,
            "variant_name": block.variant_name,
            "raw_count": int(prelude.per_variant_counts[block.name]),
            "full_count": int(prelude.per_variant_counts[block.name]),
            "full_valid_count": int(prelude.per_variant_counts[block.name]),
            "target_count": int(prelude.planning.target_counts[block.name]),
            "planned_count": int(prelude.planning.target_counts[block.name]),
            "generation_mode": (
                "full_enumeration_v2"
                if prelude.planning.target_counts[block.name] >= prelude.per_variant_counts[block.name]
                else GRID_V2_SAMPLER_VERSION
            ),
        }
        for block in prelude.blocks
    }
    diagnostics = _planning_diagnostics(prelude)
    return GridV2CountPreview(
        raw_candidate_count=prelude.raw_candidate_count,
        enumerated_candidate_count=planned,
        deduped_candidate_count=None if full_valid is None else planned,
        per_variant_counts=dict(prelude.planning.target_counts),
        axis_names_by_variant=prelude.axis_names_by_variant,
        mode_labels=prelude.mode_labels,
        full_raw_candidate_count=prelude.raw_candidate_count,
        full_valid_candidate_count=full_valid,
        planned_candidate_count=planned,
        requested_planning_policy=prelude.planning.requested_policy,
        effective_planning_policy=prelude.planning.effective_policy,
        effective_policy_reason=prelude.planning.effective_policy_reason,
        requested_budget=prelude.planning.requested_budget,
        budget_is_operative=prelude.planning.budget_is_operative,
        coverage_pct=(planned / prelude.raw_candidate_count * 100.0) if prelude.raw_candidate_count else 0.0,
        per_block_counts=per_block,
        effective_allocation_method=prelude.planning.effective_allocation_method
        or "full_enumeration_v2",
        sampler_version=(
            GRID_V2_SAMPLER_VERSION if prelude.planning.effective_policy == "sampled" else None
        ),
        diagnostics=tuple(item.to_dict() for item in diagnostics),
        validation_warnings=warning_messages(diagnostics),
    )


def estimate_grid_v2_cache(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    candidate_indices: Sequence[int] | None = None,
    *,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> GridV2CacheEstimate:
    """Estimate local cache memory for a planned run."""

    hooks = _coerce_hooks(hooks)
    selected_indices = _selected_candidate_indices(plan, candidate_indices)
    validate_context(getattr(plan.profile, "entry_filter_methods", ()), hooks.entry_filter_context,
                     list(plan.settings.entry_filter_config))
    n_bars = int(len(df))
    context = _cache_key_context(df, trade_start_idx, hooks)
    cache_keys = _candidate_cache_keys(plan, context, hooks, selected_indices)
    return _estimate_grid_v2_cache_from_keys(
        plan,
        n_bars,
        cache_keys,
        compute_sharpe=compute_sharpe,
        compute_sharpe_daily=compute_sharpe_daily,
        compute_max_drawdown_mtm=compute_max_drawdown_mtm,
    )


def _candidate_cache_keys(
    plan: GridV2Plan,
    context: _CacheKeyContext,
    hooks: GridV2StrategyHooks,
    selected_indices: Sequence[int],
) -> tuple[_CandidateCacheKeys, ...]:
    signal_groups = _cache_group_plan(plan, hooks, selected_indices, signal_only=True)
    dataprep_groups = _cache_group_plan(plan, hooks, selected_indices, signal_only=False)
    signal_cache = {
        group_code: _signal_cache_key_for_index(plan, representative, context, hooks)
        for group_code, representative in signal_groups.representatives.items()
    }
    dataprep_cache = {
        group_code: _dataprep_cache_key_for_index(plan, representative, context, hooks)
        for group_code, representative in dataprep_groups.representatives.items()
    }
    records: list[_CandidateCacheKeys] = []
    for position, index in enumerate(selected_indices):
        records.append(
            _CandidateCacheKeys(
                candidate_index=index,
                candidate_id=index + 1,
                signal_key=signal_cache[signal_groups.codes[position]],
                dataprep_key=dataprep_cache[dataprep_groups.codes[position]],
                trail_mode=str(plan.candidate_table.modes_for_index(index).get("trail", "none")),
            )
        )
    return tuple(records)


def _estimate_grid_v2_cache_from_keys(
    plan: GridV2Plan,
    n_bars: int,
    cache_keys: Sequence[_CandidateCacheKeys],
    *,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> GridV2CacheEstimate:
    topology = _grid_v2_execution_topology(plan)
    signal_combo_count = len({item.signal_key for item in cache_keys})
    dataprep_combo_count = len({item.dataprep_key for item in cache_keys})
    worker_multiplier = max(1, int(plan.settings.worker_multiplier))
    bool_signal_arrays = _SIGNAL_TOPOLOGY_BOOL_SIGNAL_ARRAYS if topology == "signal_reversal" else _BOOL_SIGNAL_ARRAYS
    float_dataprep_arrays = 0 if topology == "signal_reversal" else _FLOAT_DATAPREP_ARRAYS
    bytes_per_signal_combo = int(n_bars * bool_signal_arrays * np.dtype(np.bool_).itemsize)
    bytes_per_dataprep_combo = int(n_bars * float_dataprep_arrays * np.dtype(np.float64).itemsize)
    physical_signal_stack_rows = dataprep_combo_count
    physical_dataprep_stack_rows = 0 if topology == "signal_reversal" else dataprep_combo_count
    output_candidate_count = len(cache_keys)
    bytes_per_output_candidate = int(OUTPUT_COLUMN_COUNT * np.dtype(np.float64).itemsize)
    bytes_per_shared_market_bar = int(
        4 * np.dtype(np.float64).itemsize
        + np.dtype(np.int64).itemsize
        + (np.dtype(np.int32).itemsize if compute_sharpe else 0)
        + (np.dtype(np.int32).itemsize if compute_sharpe_daily else 0)
    )
    estimated_signal_bytes = physical_signal_stack_rows * bytes_per_signal_combo
    chandelier_keys = {
        item.dataprep_key for item in cache_keys if item.trail_mode == "chandelier"
    }
    chandelier_combo_count = len(chandelier_keys)
    chandelier_atr_nbytes = chandelier_combo_count * n_bars * np.dtype(np.float64).itemsize
    chandelier_mapping_nbytes = (
        physical_dataprep_stack_rows * np.dtype(np.int32).itemsize
        if chandelier_combo_count
        else 0
    )
    estimated_dataprep_bytes = (
        physical_dataprep_stack_rows * bytes_per_dataprep_combo
        + chandelier_atr_nbytes
        + chandelier_mapping_nbytes
    )
    estimated_output_bytes = output_candidate_count * bytes_per_output_candidate
    admission_config_nbytes = output_candidate_count * 24 if topology is None else 0
    month_id_nbytes = n_bars * np.dtype(np.int32).itemsize if compute_sharpe else 0
    day_id_nbytes = n_bars * np.dtype(np.int32).itemsize if compute_sharpe_daily else 0
    mtm_sidecar_nbytes = (
        output_candidate_count * np.dtype(np.float64).itemsize
        if compute_max_drawdown_mtm
        else 0
    )
    estimated_shared_market_bytes = n_bars * bytes_per_shared_market_bar
    estimated_total_bytes = (
        estimated_signal_bytes
        + estimated_dataprep_bytes
        + estimated_output_bytes
        + mtm_sidecar_nbytes
        + admission_config_nbytes
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
        month_id_nbytes=month_id_nbytes,
        day_id_nbytes=day_id_nbytes,
        mtm_sidecar_nbytes=mtm_sidecar_nbytes,
        admission_config_nbytes=admission_config_nbytes,
        chandelier_combo_count=chandelier_combo_count,
        chandelier_atr_nbytes=chandelier_atr_nbytes,
        chandelier_mapping_nbytes=chandelier_mapping_nbytes,
    )


def _grid_v2_execution_topology(plan: GridV2Plan) -> str | None:
    topologies = {
        plan.profile.variants[variant_name].modes.get("topology")
        for variant_name in plan.candidate_table.variant_names
    }
    if len(topologies) > 1:
        raise ValueError("Grid V2 requires all enabled variants to share one execution topology.")
    topology = next(iter(topologies), None)
    if topology in (None, ""):
        return None
    if topology == "signal_reversal":
        return topology
    raise ValueError(f"Unsupported Grid V2 execution topology: {topology!r}.")


def _raise_cache_estimate_error(estimate: GridV2CacheEstimate) -> None:
    raise MemoryError(
        "Grid V2 cache estimate exceeds max_signal_cache_mb "
        f"({estimate.estimated_total_mb:.3f} MB > {estimate.max_signal_cache_mb:.3f} MB)."
    )


def _signal_chunk_estimated_mb(
    plan: GridV2Plan,
    n_bars: int,
    *,
    signal_row_count: int,
    candidate_count: int,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> float:
    worker_multiplier = max(1, int(plan.settings.worker_multiplier))
    signal_bytes = (
        int(signal_row_count)
        * int(n_bars)
        * _SIGNAL_TOPOLOGY_BOOL_SIGNAL_ARRAYS
        * np.dtype(np.bool_).itemsize
    )
    output_bytes = int(candidate_count) * OUTPUT_COLUMN_COUNT * np.dtype(np.float64).itemsize
    mtm_sidecar_bytes = (
        int(candidate_count) * np.dtype(np.float64).itemsize
        if compute_max_drawdown_mtm
        else 0
    )
    shared_market_bytes = int(n_bars) * (
        4 * np.dtype(np.float64).itemsize
        + np.dtype(np.int64).itemsize
        + (np.dtype(np.int32).itemsize if compute_sharpe else 0)
        + (np.dtype(np.int32).itemsize if compute_sharpe_daily else 0)
    )
    return (
        (signal_bytes + output_bytes + mtm_sidecar_bytes + shared_market_bytes)
        * worker_multiplier
    ) / _BYTES_PER_MB


def _signal_cache_key_chunks(
    plan: GridV2Plan,
    n_bars: int,
    cache_keys: Sequence[_CandidateCacheKeys],
    *,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> tuple[_CacheKeyChunk, ...]:
    if not cache_keys:
        return (
            _CacheKeyChunk(
                (),
                _estimate_grid_v2_cache_from_keys(
                    plan,
                    n_bars,
                    (),
                    compute_sharpe=compute_sharpe,
                    compute_sharpe_daily=compute_sharpe_daily,
                    compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                ),
            ),
        )

    chunks: list[_CacheKeyChunk] = []
    current: list[_CandidateCacheKeys] = []
    current_data_keys: set[Any] = set()
    limit_mb = float(plan.settings.max_signal_cache_mb)

    for record in cache_keys:
        next_data_key_count = len(current_data_keys) + (0 if record.dataprep_key in current_data_keys else 1)
        next_candidate_count = len(current) + 1
        next_estimated_mb = _signal_chunk_estimated_mb(
            plan,
            n_bars,
            signal_row_count=next_data_key_count,
            candidate_count=next_candidate_count,
            compute_sharpe=compute_sharpe,
            compute_sharpe_daily=compute_sharpe_daily,
            compute_max_drawdown_mtm=compute_max_drawdown_mtm,
        )
        if current and next_estimated_mb > limit_mb:
            chunk_records = tuple(current)
            chunks.append(
                _CacheKeyChunk(
                    chunk_records,
                    _estimate_grid_v2_cache_from_keys(
                        plan,
                        n_bars,
                        chunk_records,
                        compute_sharpe=compute_sharpe,
                        compute_sharpe_daily=compute_sharpe_daily,
                        compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                    ),
                )
            )
            current = []
            current_data_keys = set()

        current.append(record)
        current_data_keys.add(record.dataprep_key)
        single_chunk_mb = _signal_chunk_estimated_mb(
            plan,
            n_bars,
            signal_row_count=len(current_data_keys),
            candidate_count=len(current),
            compute_sharpe=compute_sharpe,
            compute_sharpe_daily=compute_sharpe_daily,
            compute_max_drawdown_mtm=compute_max_drawdown_mtm,
        )
        if single_chunk_mb > limit_mb:
            chunk_estimate = _estimate_grid_v2_cache_from_keys(
                plan,
                n_bars,
                tuple(current),
                compute_sharpe=compute_sharpe,
                compute_sharpe_daily=compute_sharpe_daily,
                compute_max_drawdown_mtm=compute_max_drawdown_mtm,
            )
            _raise_cache_estimate_error(chunk_estimate)

    if current:
        chunk_records = tuple(current)
        chunks.append(
            _CacheKeyChunk(
                chunk_records,
                _estimate_grid_v2_cache_from_keys(
                    plan,
                    n_bars,
                    chunk_records,
                    compute_sharpe=compute_sharpe,
                    compute_sharpe_daily=compute_sharpe_daily,
                    compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                ),
            )
        )
    return tuple(chunks)


def _build_execution_data_rows(
    plan: GridV2Plan,
    df: pd.DataFrame,
    hooks: GridV2StrategyHooks,
    candidate_indices: Sequence[int],
) -> tuple[ExecutionData, ...]:
    params_batch = tuple(
        _normalized_candidate_params(hooks, plan.candidate_table.params_for_index(candidate_index))
        for candidate_index in candidate_indices
    )
    if hooks.build_execution_data_batch is not None:
        rows = tuple(hooks.build_execution_data_batch(df, params_batch))
        if len(rows) != len(params_batch):
            raise ValueError(
                "Grid V2 build_execution_data_batch returned "
                f"{len(rows)} rows for {len(params_batch)} parameter mappings."
            )
        return rows
    return tuple(hooks.build_execution_data(df, params) for params in params_batch)


def _stack_metadata_from_batch(
    stacked_data: Any,
    outputs: np.ndarray,
    max_drawdown_mtm_pct: np.ndarray | None = None,
    admission_config_nbytes: int = 0,
) -> dict[str, Any]:
    mtm_sidecar_nbytes = (
        int(max_drawdown_mtm_pct.nbytes) if max_drawdown_mtm_pct is not None else 0
    )
    total_nbytes = int(stacked_data.nbytes + outputs.nbytes + mtm_sidecar_nbytes + admission_config_nbytes)
    return {
        "stack_row_count": stacked_data.row_count,
        "stack_candidate_count": stacked_data.candidate_count,
        "stack_signal_nbytes": stacked_data.signal_nbytes,
        "stack_dataprep_nbytes": stacked_data.dataprep_nbytes,
        "stack_shared_market_nbytes": stacked_data.shared_market_nbytes,
        "stack_output_nbytes": int(outputs.nbytes),
        "stack_mtm_sidecar_nbytes": mtm_sidecar_nbytes,
        "stack_admission_config_nbytes": admission_config_nbytes,
        "stack_total_nbytes": total_nbytes,
        "stack_total_mb": total_nbytes / _BYTES_PER_MB,
    }


def execute_grid_v2_candidates(
    plan: GridV2Plan,
    df: pd.DataFrame,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks | Any,
    candidate_indices: Sequence[int] | None = None,
    *,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_sqn: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> GridV2RunResult:
    """Execute planned candidates through the selected V2 batch backend."""

    hooks = _coerce_hooks(hooks)
    validate_context(getattr(plan.profile, "entry_filter_methods", ()), hooks.entry_filter_context,
                     list(plan.settings.entry_filter_config))
    selected_indices = _selected_candidate_indices(plan, candidate_indices)
    topology = _grid_v2_execution_topology(plan)
    context = _cache_key_context(df, trade_start_idx, hooks)
    cache_key_started = time.time()
    cache_keys = _candidate_cache_keys(plan, context, hooks, selected_indices)
    cache_key_build_seconds = time.time() - cache_key_started
    month_ids = (
        build_calendar_month_ids(df.index)
        if compute_sharpe
        else np.empty(0, dtype=np.int32)
    )
    if month_ids.size not in {0, int(len(df))}:
        raise ValueError("Grid V2 month_ids length must match the execution dataframe.")
    day_ids = (
        np.ascontiguousarray(build_utc_day_ids(df.index), dtype=np.int32)
        if compute_sharpe_daily
        else np.empty(0, dtype=np.int32)
    )
    if day_ids.size not in {0, int(len(df))}:
        raise ValueError("Grid V2 day_ids length must match the execution dataframe.")
    estimate = _estimate_grid_v2_cache_from_keys(
        plan,
        int(len(df)),
        cache_keys,
        compute_sharpe=compute_sharpe,
        compute_sharpe_daily=compute_sharpe_daily,
        compute_max_drawdown_mtm=compute_max_drawdown_mtm,
    )
    if estimate.estimated_total_mb > estimate.max_signal_cache_mb and topology != "signal_reversal":
        _raise_cache_estimate_error(estimate)
    chunks = (
        _signal_cache_key_chunks(
            plan,
            int(len(df)),
            cache_keys,
            compute_sharpe=compute_sharpe,
            compute_sharpe_daily=compute_sharpe_daily,
            compute_max_drawdown_mtm=compute_max_drawdown_mtm,
        )
        if topology == "signal_reversal" and estimate.estimated_total_mb > estimate.max_signal_cache_mb
        else (_CacheKeyChunk(cache_keys, estimate),)
    )

    eval_started = time.time()
    stats = GridV2CacheStats()
    signal_seen: set[Any] = set()
    dataprep_seen: set[Any] = set()
    selected_dataprep_cache: dict[Any, ExecutionData] = {}
    rows: list[GridV2ResultRow] = []
    compiled_available = compiled_batch_available()
    use_compiled = bool(plan.settings.prefer_compiled and compiled_available)
    backend_kind = COMPILED_BATCH_KIND if use_compiled else REFERENCE_BATCH_KIND
    compiled_execution_mode: str | None = None
    compiled_config_packing: str | None = None
    signal_build_seconds = 0.0
    stack_build_seconds = 0.0
    compiled_batch_seconds = 0.0
    peak_stack_metadata: dict[str, Any] = {}
    signal_stack_rows_built = 0
    signal_stack_rows_peak = 0

    for chunk in chunks:
        data_groups: dict[Any, list[int]] = {}
        representatives: dict[Any, int] = {}
        for key_record in chunk.records:
            signal_key = key_record.signal_key
            if signal_key in signal_seen:
                stats.signal_hits += 1
            else:
                stats.signal_misses += 1
                signal_seen.add(signal_key)

            data_key = key_record.dataprep_key
            if data_key in dataprep_seen:
                stats.dataprep_hits += 1
            else:
                stats.dataprep_misses += 1
                dataprep_seen.add(data_key)
            representatives.setdefault(data_key, key_record.candidate_index)
            data_groups.setdefault(data_key, []).append(key_record.candidate_index)

        data_keys = tuple(data_groups.keys())
        dataprep_cache: dict[Any, ExecutionData] = {}
        data_errors: dict[Any, Exception] = {}
        signal_build_started = time.time()
        if data_keys:
            representative_indices = tuple(representatives[key] for key in data_keys)
            try:
                data_rows = _build_execution_data_rows(plan, df, hooks, representative_indices)
                dataprep_cache = dict(zip(data_keys, data_rows))
            except Exception:
                for data_key, candidate_index in zip(data_keys, representative_indices):
                    try:
                        params = _normalized_candidate_params(
                            hooks,
                            plan.candidate_table.params_for_index(candidate_index),
                        )
                        dataprep_cache[data_key] = hooks.build_execution_data(df, params)
                    except Exception as exc:  # pragma: no cover - exercised by existing error row tests indirectly
                        data_errors[data_key] = exc
        signal_build_seconds += time.time() - signal_build_started

        if data_errors:
            for key_record in chunk.records:
                exc = data_errors.get(key_record.dataprep_key)
                if exc is not None:
                    rows.append(_error_row(plan, key_record.candidate_index, exc))
            data_groups = {
                data_key: candidate_group
                for data_key, candidate_group in data_groups.items()
                if data_key not in data_errors
            }

        if not data_groups:
            continue

        if topology != "signal_reversal" or len(chunks) == 1:
            selected_dataprep_cache.update(dataprep_cache)

        if use_compiled:
            data_keys = tuple(data_groups.keys())
            data_key_to_stack_row = {key: index for index, key in enumerate(data_keys)}
            compiled_indices: list[int] = []
            data_index: list[int] = []
            for key_record in chunk.records:
                data_key = key_record.dataprep_key
                if data_key not in data_key_to_stack_row:
                    continue
                candidate_index = key_record.candidate_index
                compiled_indices.append(candidate_index)
                data_index.append(data_key_to_stack_row[data_key])
            if compiled_indices:
                if topology == "signal_reversal":
                    stack_started = time.time()
                    stacked_data = build_signal_stacked_execution_data(
                        [dataprep_cache[key] for key in data_keys],
                        data_index,
                        month_ids=month_ids,
                        day_ids=day_ids,
                    )
                    stack_build_seconds += time.time() - stack_started
                    compiled_started = time.time()
                    params_batch = [
                        _normalized_candidate_params(
                            hooks,
                            plan.candidate_table.params_for_index(candidate_index),
                        )
                        for candidate_index in compiled_indices
                    ]
                    row_params_batch: list[Mapping[str, Any]] | None = None
                    compiled_config_packing = "mapping"
                    batch = evaluate_compiled_signal_stacked_batch(
                        stacked_data=stacked_data,
                        profile=plan.profile,
                        params_batch=params_batch,
                        trade_start_idx=trade_start_idx,
                        n_workers=plan.settings.compiled_workers,
                        compute_sharpe=compute_sharpe,
                        compute_sharpe_daily=compute_sharpe_daily,
                        compute_sqn=compute_sqn,
                        compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                    )
                    compiled_batch_seconds += time.time() - compiled_started
                else:
                    stack_started = time.time()
                    stacked_data = build_stacked_execution_data(
                        [dataprep_cache[key] for key in data_keys],
                        data_index,
                        month_ids=month_ids,
                        day_ids=day_ids,
                    )
                    stack_build_seconds += time.time() - stack_started
                    compiled_started = time.time()
                    params_batch = None
                    row_params_batch = None
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
                        admission_rules=plan.settings.admission_rules,
                        stacked_data=stacked_data,
                        profile=plan.profile,
                        params_batch=params_batch,
                        trade_start_idx=trade_start_idx,
                        n_workers=plan.settings.compiled_workers,
                        packed_config_arrays=packed_config_arrays,
                        compute_sharpe=compute_sharpe,
                        compute_sharpe_daily=compute_sharpe_daily,
                        compute_sqn=compute_sqn,
                        compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                    )
                    compiled_batch_seconds += time.time() - compiled_started
                compiled_execution_mode = batch.execution_mode
                chunk_stack_metadata = _stack_metadata_from_batch(
                    stacked_data,
                    batch.outputs,
                    batch.max_drawdown_mtm_pct,
                    len(compiled_indices) * 24 if topology is None else 0,
                )
                if (
                    not peak_stack_metadata
                    or int(chunk_stack_metadata["stack_total_nbytes"]) > int(peak_stack_metadata["stack_total_nbytes"])
                ):
                    peak_stack_metadata = chunk_stack_metadata
                if topology == "signal_reversal":
                    signal_stack_rows_built += int(stacked_data.row_count)
                    signal_stack_rows_peak = max(signal_stack_rows_peak, int(stacked_data.row_count))
                for output_index, (candidate_index, values) in enumerate(zip(compiled_indices, batch.outputs)):
                    params = row_params_batch[output_index] if row_params_batch is not None else None
                    rows.append(
                        _row_from_compiled_output(
                            plan,
                            candidate_index,
                            values,
                            params=params,
                            max_drawdown_mtm_pct=(
                                float(batch.max_drawdown_mtm_pct[output_index])
                                if batch.max_drawdown_mtm_pct is not None
                                else None
                            ),
                        )
                    )
            else:
                compiled_execution_mode = "stacked"
        else:
            for data_key, candidate_indices_for_data in data_groups.items():
                data = dataprep_cache[data_key]
                for candidate_index in candidate_indices_for_data:
                    params = plan.candidate_table.params_for_index(candidate_index)
                    try:
                        run = run_v2_strategy(
                            entry_filter_context=hooks.entry_filter_context,
                            admission_rules=plan.settings.admission_rules,
                            data=data,
                            profile=plan.profile,
                            params=_normalized_candidate_params(hooks, params),
                            trade_start_idx=trade_start_idx,
                            compute_sharpe_daily=compute_sharpe_daily,
                            compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                        )
                    except Exception as exc:
                        rows.append(_error_row(plan, candidate_index, exc))
                        continue
                    rows.append(
                        _row_from_run(
                            plan,
                            candidate_index,
                            run,
                            params=params,
                            compute_sharpe=compute_sharpe,
                            compute_sharpe_daily=compute_sharpe_daily,
                            compute_sqn=compute_sqn,
                            compute_max_drawdown_mtm=compute_max_drawdown_mtm,
                        )
                    )

        if topology == "signal_reversal":
            dataprep_cache.clear()
            plan.candidate_table.release_params_cache()

    rows.sort(key=lambda row: row.candidate_id)
    selected = ()
    if plan.settings.slow_enrich_selected:
        selectable = rows
        if plan.profile.modes.get("entryAdmission") == "risk_bracket_v1":
            selectable = [row for row in rows if row.status == "ok"]
        selected = tuple(
            _slow_enrich_selected(
                plan,
                df,
                context,
                trade_start_idx,
                hooks,
                row,
                selected_dataprep_cache,
                compute_sharpe_daily=compute_sharpe_daily,
            )
            for row in _rank_rows(selectable, plan.settings.primary_metric)[: max(0, int(plan.settings.top_n))]
        )
    evaluation_seconds = time.time() - eval_started
    chunk_estimates = tuple(chunk.estimate for chunk in chunks)
    max_chunk_estimated_mb = max(
        (float(chunk_estimate.estimated_total_mb) for chunk_estimate in chunk_estimates),
        default=0.0,
    )
    max_chunk_candidates = max((len(chunk.records) for chunk in chunks), default=0)
    chunk_metadata = {
        "chunk_count": len(chunks),
        "max_chunk_candidates": max_chunk_candidates,
        "max_chunk_estimated_mb": max_chunk_estimated_mb,
        "chunk_estimated_mb": max_chunk_estimated_mb,
        "full_run_estimated_signal_mb": float(estimate.estimated_signal_mb),
        "configured_limit_mb": float(estimate.max_signal_cache_mb),
        "signal_stack_rows_built": signal_stack_rows_built,
        "signal_stack_rows_peak": signal_stack_rows_peak,
        "full_population_result_object_note": (
            "Full-population OptimizationResult materialization remains outside the Grid V2 "
            "compiled execution kernel."
        ),
    }
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
            "compute_sharpe": bool(compute_sharpe),
            "compute_sharpe_daily": bool(compute_sharpe_daily),
            "compute_sqn": bool(compute_sqn),
            "compute_max_drawdown_mtm": bool(compute_max_drawdown_mtm),
            "month_id_nbytes": int(month_ids.nbytes),
            "day_id_nbytes": int(day_ids.nbytes),
            "mtm_sidecar_nbytes": int(estimate.mtm_sidecar_nbytes),
            "evaluation_seconds": evaluation_seconds,
            "candidates_per_second": (len(rows) / evaluation_seconds) if evaluation_seconds > 0.0 else None,
            "cache_key_build_seconds": cache_key_build_seconds,
            "signal_build_seconds": signal_build_seconds,
            "stack_build_seconds": stack_build_seconds,
            "compiled_batch_seconds": compiled_batch_seconds,
            "candidate_table_used": True,
            "legacy_candidates_materialized": plan.candidate_table.legacy_candidates_materialized_count,
            "params_materialized": plan.candidate_table.params_materialized_count,
            "semantic_keys_materialized": plan.candidate_table.semantic_keys_materialized_count,
            "canonical_identities_materialized": plan.candidate_table.canonical_identities_materialized_count,
            **peak_stack_metadata,
            **chunk_metadata,
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
    *,
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_sqn: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> GridV2RunResult:
    """Build and execute a Grid V2 plan."""

    plan = build_grid_v2_plan(config, settings=settings, base_params=base_params)
    return execute_grid_v2_candidates(
        plan,
        df,
        trade_start_idx,
        hooks,
        candidate_indices,
        compute_sharpe=compute_sharpe,
        compute_sharpe_daily=compute_sharpe_daily,
        compute_sqn=compute_sqn,
        compute_max_drawdown_mtm=compute_max_drawdown_mtm,
    )


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
    # Runtime values stay in row seeds for execution packing compatibility even
    # though _build_parameter_domains excludes them from candidate mathematics.
    return {str(name): value for name, value in params.items() if not str(name).endswith("_options")}


def _canonicalize_globally_inactive_params(
    params_spec: Mapping[str, Any],
    profile: ExecutionProfile,
    fixed_params: Mapping[str, Any],
    selector_values: Mapping[str, Any],
    selected_variants: Sequence[str],
) -> dict[str, Any]:
    """Replace fields unused by every selected variant before fixed coercion."""

    active_names: set[str] = set()
    for variant_name in selected_variants:
        variant_params = dict(fixed_params)
        if profile.variant_selector is not None:
            variant_params[profile.variant_selector.param] = selector_values[variant_name]
        active_names.update(active_parameter_names(profile, variant_params))

    canonical = dict(fixed_params)
    for name in profile.parameter_names:
        spec = params_spec.get(name)
        if (
            name not in active_names
            and profile.parameter_roles.get(name) != "runtime"
            and isinstance(spec, Mapping)
            and "default" in spec
        ):
            canonical[name] = spec["default"]
    return canonical


def _build_parameter_domains(
    config: Mapping[str, Any],
    settings: GridV2Settings,
    fixed_params: Mapping[str, Any],
    profile: ExecutionProfile,
) -> dict[str, GridV2ParameterDomain]:
    params_spec = _parameters(config)
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
        default = fixed_params.get(name, spec.get("default"))
        if is_runtime:
            if name in (enabled_axes or set()):
                raise ValueError(f"Grid V2 axis '{name}' is a runtime parameter.")
            continue
        axis_available = _axis_available(name, optimize, profile)
        if name in (enabled_axes or set()) and not axis_available:
            raise ValueError(f"Grid V2 axis '{name}' is not an optimized non-runtime parameter.")
        is_axis = axis_available and _axis_enabled(name, optimize, settings)
        if is_axis:
            values, source = _axis_values(name, spec, param_type, fixed_params)
        else:
            try:
                fixed_value = _coerce_value(default, param_type)
            except (ArithmeticError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"Grid V2 fixed parameter '{name}' has an invalid {param_type} value: {exc}"
                ) from exc
            values, source = (fixed_value,), "fixed_default"
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


def _axis_available(name: str, optimize: Mapping[str, Any], profile: ExecutionProfile) -> bool:
    return (
        bool(optimize.get("enabled", False))
        and profile.parameter_roles.get(name) != "runtime"
        and (profile.variant_selector is None or name != profile.variant_selector.param)
    )


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
        if isinstance(value, bool):
            raise ValueError("integer values cannot be booleans.")
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("integer values must be numeric.") from exc
        if not parsed.is_finite() or parsed != parsed.to_integral_value():
            raise ValueError("integer values must be finite and integral.")
        return int(parsed)
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


def _selected_variants(
    profile: ExecutionProfile,
    settings: GridV2Settings,
    fixed_params: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
    variant_order = tuple(profile.variants.keys())
    selector = profile.variant_selector
    if selector is not None and selector.user_facing is False:
        requested = tuple(settings.enabled_variants or ())
        if requested:
            raise ValueError(
                "Grid V2 enabled_variants/grid_enabled_modes cannot be used for "
                f"internal variant selector '{selector.param}'. Configure the selector "
                "parameter itself instead."
            )
        params = dict(profile.parameter_defaults)
        params.update(dict(fixed_params or {}))
        raw_value = params.get(selector.param, profile.parameter_defaults.get(selector.param))
        key = canonical_selector_key(raw_value)
        variant_name = selector.mapping.get(key)
        if variant_name is None:
            raise ValueError(
                f"Grid V2 selector parameter '{selector.param}' value {raw_value!r} "
                f"maps to '{key}', which is not in variantSelector.mapping."
            )
        return (variant_name,)
    if settings.enabled_variants is None:
        return variant_order
    requested = tuple(str(name).strip() for name in settings.enabled_variants)
    if any(not name for name in requested):
        raise ValueError("Grid V2 enabled_variants contains an empty variant name.")
    if len(set(requested)) != len(requested):
        raise ValueError("Grid V2 enabled_variants contains duplicate normalized variant names.")
    if not requested:
        raise ValueError("Grid V2 enabled_variants must contain at least one variant.")
    unknown = sorted(set(requested) - set(variant_order))
    if unknown:
        raise ValueError(f"Grid V2 enabled_variants contains unknown variant(s): {unknown}.")
    return tuple(name for name in variant_order if name in set(requested))


def _dependency_names(depends_on: Any) -> tuple[str, ...]:
    if depends_on in (None, ""):
        return ()
    if isinstance(depends_on, str):
        return (depends_on,)
    if isinstance(depends_on, Sequence) and not isinstance(depends_on, (str, bytes, bytearray)):
        return tuple(str(item) for item in depends_on)
    raise ValueError("Grid V2 depends_on must be a string or list of strings.")


def _inactive_dependency_children(
    config: Mapping[str, Any],
    params: Mapping[str, Any],
) -> set[str]:
    specs = _parameters(config)
    inactive: set[str] = set()
    for raw_name, raw_spec in specs.items():
        if not isinstance(raw_spec, Mapping):
            continue
        parents = _dependency_names(raw_spec.get("depends_on"))
        if not parents:
            continue
        for parent in parents:
            parent_spec = specs.get(parent, {})
            param_type = (
                str(parent_spec.get("type", "bool")).strip().lower()
                if isinstance(parent_spec, Mapping)
                else "bool"
            )
            if param_type != "bool":
                raise ValueError(
                    f"Grid V2 depends_on parent '{parent}' for '{raw_name}' must be a bool parameter."
                )
            parent_default = parent_spec.get("default") if isinstance(parent_spec, Mapping) else False
            if not _coerce_bool(params.get(parent, parent_default)):
                inactive.add(str(raw_name))
                break
    return inactive


def _ordered_active_names_for_block(
    config: Mapping[str, Any],
    profile: ExecutionProfile,
    params: Mapping[str, Any],
) -> tuple[str, ...]:
    active = active_parameter_names(profile, params) - _inactive_dependency_children(config, params)
    return tuple(name for name in profile.parameter_names if name in active)


def _ordered_inactive_names_for_block(
    config: Mapping[str, Any],
    profile: ExecutionProfile,
    params: Mapping[str, Any],
) -> tuple[str, ...]:
    inactive = inactive_parameter_names(profile, params) | _inactive_dependency_children(config, params)
    return tuple(
        name
        for name in profile.parameter_names
        if name in inactive and profile.parameter_roles.get(name) != "runtime"
    )


def _optimization_bool_groups(config: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rules = config.get("optimization_rules", {})
    if not isinstance(rules, Mapping):
        raise _bool_group_error(
            config,
            "optimization_rules must be a mapping.",
            path="optimization_rules",
        )
    groups = rules.get("bool_groups", ())
    if not isinstance(groups, (list, tuple)):
        raise _bool_group_error(
            config,
            "optimization_rules.bool_groups must be a list.",
            path="optimization_rules.bool_groups",
        )
    for index, group in enumerate(groups):
        if not isinstance(group, Mapping):
            raise _bool_group_error(
                config,
                "bool-group entry must be a mapping.",
                path=f"optimization_rules.bool_groups[{index}]",
            )
    return tuple(groups)


def _bool_group_error(
    config: Mapping[str, Any],
    message: str,
    *,
    path: str,
) -> ProfileValidationError:
    strategy_id = str(config.get("id") or "<unknown strategy>")
    return ProfileValidationError(
        V2Diagnostic(
            severity="error",
            code="V2_INVALID_BOOL_GROUP",
            strategy_id=strategy_id,
            path=path,
            variant=None,
            message=f"{strategy_id}: {message}",
        )
    )


def _logical_mode_metadata(
    values: Mapping[str, bool],
) -> tuple[str, str]:
    """Return the identity-stable fallback when logical metadata is absent."""

    if set(values) == {"useCloseCount", "useTBands"}:
        use_cc = bool(values["useCloseCount"])
        use_tbands = bool(values["useTBands"])
        if use_cc and not use_tbands:
            return "cc_only", "Close Count only"
        if use_tbands and not use_cc:
            return "tbands_only", "T Bands only"
        if use_cc and use_tbands:
            return "both", "Both"

    key = "_".join(f"{name}_{str(value).lower()}" for name, value in sorted(values.items()))
    return key, key


def _coerce_logical_mode_bool(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(float(value)) or float(value) not in {0.0, 1.0}:
            raise ValueError(f"Grid V2 cannot coerce {value!r} to bool.")
    return _coerce_bool(value)


def _validated_logical_modes(
    config: Mapping[str, Any],
    group: Mapping[str, Any],
    group_params: Sequence[str],
    supported_combinations: Sequence[tuple[bool, ...]],
) -> Mapping[tuple[bool, ...], tuple[str, str, int]] | None:
    if "logical_modes" not in group:
        return None
    raw_modes = group.get("logical_modes")
    root_path = "optimization_rules.bool_groups[0].logical_modes"
    if not isinstance(raw_modes, Mapping):
        raise _bool_group_error(
            config,
            "bool-group logical_modes must be a mapping when present.",
            path=root_path,
        )

    normalized: dict[tuple[bool, ...], tuple[str, str, int]] = {}
    for index, (raw_key, raw_meta) in enumerate(raw_modes.items()):
        if (
            not isinstance(raw_key, str)
            or not raw_key
            or raw_key.strip() != raw_key
        ):
            raise _bool_group_error(
                config,
                "logical-mode keys must be non-empty strings without surrounding whitespace.",
                path=root_path,
            )
        entry_path = f"{root_path}.{raw_key}"
        if not isinstance(raw_meta, Mapping):
            raise _bool_group_error(
                config,
                f"logical mode '{raw_key}' must be a mapping.",
                path=entry_path,
            )
        raw_values = raw_meta.get("values")
        if not isinstance(raw_values, Mapping):
            raise _bool_group_error(
                config,
                f"logical mode '{raw_key}' values must be a mapping.",
                path=f"{entry_path}.values",
            )
        unknown = [str(name) for name in raw_values if name not in group_params]
        missing = [name for name in group_params if name not in raw_values]
        if unknown or missing:
            raise _bool_group_error(
                config,
                f"logical mode '{raw_key}' values must reference exactly the bool-group "
                f"parameters; missing={missing}, unknown={unknown}.",
                path=f"{entry_path}.values",
            )
        combination: list[bool] = []
        for name in group_params:
            try:
                combination.append(_coerce_logical_mode_bool(raw_values[name]))
            except (TypeError, ValueError) as exc:
                raise _bool_group_error(
                    config,
                    f"logical mode '{raw_key}' value for '{name}' is not a valid boolean: {exc}",
                    path=f"{entry_path}.values.{name}",
                ) from exc
        combination_key = tuple(combination)
        if not any(combination_key):
            raise _bool_group_error(
                config,
                f"logical mode '{raw_key}' cannot use the all-false combination.",
                path=f"{entry_path}.values",
            )
        if combination_key in normalized:
            prior = normalized[combination_key][0]
            raise _bool_group_error(
                config,
                f"logical modes '{prior}' and '{raw_key}' have duplicate value combinations.",
                path=f"{entry_path}.values",
            )
        label = raw_meta.get("label", raw_key)
        if not isinstance(label, str) or not label or label.strip() != label:
            raise _bool_group_error(
                config,
                f"logical mode '{raw_key}' label must be a non-empty string without surrounding whitespace.",
                path=f"{entry_path}.label",
            )
        normalized[combination_key] = (raw_key, label, index)

    missing_combinations = [
        combination
        for combination in supported_combinations
        if combination not in normalized
    ]
    if missing_combinations:
        rendered = [
            {name: value for name, value in zip(group_params, combination)}
            for combination in missing_combinations
        ]
        raise _bool_group_error(
            config,
            f"logical_modes must define every supported non-all-false combination; missing={rendered}.",
            path=root_path,
        )
    return normalized


def _bool_group_value_blocks(
    config: Mapping[str, Any],
    domains: Mapping[str, GridV2ParameterDomain],
    seed_params: Mapping[str, Any],
) -> tuple[tuple[str, str, Mapping[str, bool]], ...]:
    groups = _optimization_bool_groups(config)
    if not groups:
        return (("", "", {}),)
    if len(groups) > 1:
        raise _bool_group_error(
            config,
            "Grid V2 supports exactly one optimization_rules.bool_groups entry.",
            path="optimization_rules.bool_groups",
        )
    group = groups[0]
    if str(group.get("mode", "")).strip().lower() != "at_least_one_true":
        raise _bool_group_error(
            config,
            "bool-group mode must be 'at_least_one_true'.",
            path="optimization_rules.bool_groups[0].mode",
        )
    raw_params = group.get("params")
    if not isinstance(raw_params, (list, tuple)) or len(raw_params) != 2:
        raise _bool_group_error(
            config,
            "bool-group params must be a list of exactly two parameter names.",
            path="optimization_rules.bool_groups[0].params",
        )
    group_params: list[str] = []
    for raw_name in raw_params:
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise _bool_group_error(
                config,
                "bool-group parameter names must be non-empty strings.",
                path="optimization_rules.bool_groups[0].params",
            )
        group_params.append(raw_name.strip())
    if len(set(group_params)) != len(group_params):
        raise _bool_group_error(
            config,
            "bool-group parameter names must be unique.",
            path="optimization_rules.bool_groups[0].params",
        )
    specs = _parameters(config)
    for name in group_params:
        if name not in specs:
            raise _bool_group_error(
                config,
                f"bool-group parameter '{name}' is not declared.",
                path="optimization_rules.bool_groups[0].params",
            )
        spec = specs[name]
        role = (
            str(spec.get("role") or "").strip().lower()
            if isinstance(spec, Mapping)
            else ""
        )
        if name in V2_RESERVED_RUNTIME_PARAM_NAMES or role == "runtime":
            raise _bool_group_error(
                config,
                f"bool-group parameter '{name}' cannot be a reserved/runtime field.",
                path="optimization_rules.bool_groups[0].params",
            )
        param_type = (
            str(spec.get("type") or "").strip().lower()
            if isinstance(spec, Mapping)
            else ""
        )
        if param_type != "bool":
            raise _bool_group_error(
                config,
                f"bool-group parameter '{name}' must have type 'bool'.",
                path="optimization_rules.bool_groups[0].params",
            )
        if name not in domains:
            raise _bool_group_error(
                config,
                f"bool-group parameter '{name}' has no Grid V2 domain.",
                path="optimization_rules.bool_groups[0].params",
            )

    value_groups: list[tuple[bool, ...]] = []
    for name in group_params:
        domain = domains[name]
        try:
            if domain.is_axis:
                values = tuple(_coerce_bool(value) for value in domain.values)
            else:
                values = (_coerce_bool(seed_params.get(name, domain.default)),)
        except (TypeError, ValueError) as exc:
            raise _bool_group_error(
                config,
                f"bool-group parameter '{name}' has an invalid boolean domain: {exc}",
                path="optimization_rules.bool_groups[0].params",
            ) from exc
        if not values:
            raise _bool_group_error(
                config,
                f"bool-group parameter '{name}' has an empty boolean domain.",
                path="optimization_rules.bool_groups[0].params",
            )
        value_groups.append(values)

    supported_combinations = tuple(
        tuple(bool(value) for value in raw_values)
        for raw_values in itertools.product(*value_groups)
        if any(raw_values)
    )
    logical_modes = _validated_logical_modes(
        config,
        group,
        group_params,
        supported_combinations,
    )

    blocks: list[tuple[str, str, Mapping[str, bool]]] = []
    for raw_values in supported_combinations:
        values = dict(zip(group_params, (bool(value) for value in raw_values)))
        if logical_modes is None:
            key, label = _logical_mode_metadata(values)
        else:
            key, label, _ = logical_modes[raw_values]
        blocks.append((key, label, values))
    if not blocks:
        names = ", ".join(group_params)
        raise _bool_group_error(
            config,
            f"bool group has no valid logical modes for params: {names}.",
            path="optimization_rules.bool_groups[0].params",
        )
    if logical_modes is not None:
        blocks.sort(key=lambda item: logical_modes[tuple(item[2].values())][2])
    return tuple(blocks)


def _build_planning_blocks(
    *,
    config: Mapping[str, Any],
    settings: GridV2Settings,
    profile: ExecutionProfile,
    fixed_params: Mapping[str, Any],
    domains: Mapping[str, GridV2ParameterDomain],
    selector_values: Mapping[str, Any],
    selected_variants: Sequence[str],
) -> tuple[_GridV2PlanBlock, ...]:
    used_names: set[str] = set()
    blocks: list[_GridV2PlanBlock] = []
    has_bool_groups = bool(_optimization_bool_groups(config))
    for variant_name in selected_variants:
        seed_params = _candidate_seed_params(fixed_params)
        if profile.variant_selector is not None:
            seed_params[profile.variant_selector.param] = selector_values[variant_name]
        bool_blocks = _bool_group_value_blocks(config, domains, seed_params)
        for raw_mode_name, raw_label, bool_values in bool_blocks:
            block_seed = dict(seed_params)
            block_seed.update(bool_values)
            if has_bool_groups:
                block_name = raw_mode_name
                block_label = raw_label
            else:
                block_name = variant_name
                block_label = variant_name
            if block_name in used_names:
                block_name = f"{variant_name}_{block_name}"
                block_label = f"{variant_name} {block_label}"
            used_names.add(block_name)
            active_names = _ordered_active_names_for_block(config, profile, block_seed)
            inactive_names = _ordered_inactive_names_for_block(config, profile, block_seed)
            variant_axis_names = _variant_axis_names(
                profile=profile,
                domains=domains,
                active_names=active_names,
                settings=settings,
                block_fixed_names=tuple(bool_values),
            )
            pairs = enabled_parameter_ties(config, settings.enabled_tie_groups)
            if pairs:
                # Equality filtering visits a pair at its earliest original axis.
                sources = {target: source for source, target in pairs}
                variant_axis_names = tuple(dict.fromkeys(
                    sources.get(name, name) for name in profile.parameter_names
                    if sources.get(name, name) in variant_axis_names
                ))
            blocks.append(
                _GridV2PlanBlock(
                    parameter_ties=pairs,
                    name=block_name,
                    label=block_label,
                    variant_name=variant_name,
                    seed_params=block_seed,
                    active_names=active_names,
                    inactive_names=inactive_names,
                    axis_names=variant_axis_names,
                    fixed_axis_names=tuple(
                        name
                        for name in bool_values
                        if domains[name].is_axis and name in active_names
                    ),
                )
            )
    return tuple(blocks)


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
    block_fixed_names: Sequence[str] = (),
) -> tuple[str, ...]:
    active = set(active_names)
    fixed = set(block_fixed_names)
    names: list[str] = []
    for name in profile.parameter_names:
        domain = domains.get(name)
        if domain is None:
            continue
        if not domain.is_axis:
            continue
        if name in fixed:
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
    grid_mode_name: str,
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
        "semantic_identity_schema_version": GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
        "strategy": {
            "id": str(config.get("id", profile.strategy_id)),
            "version": str(config.get("version", "")),
        },
        "variant": variant.name,
        **({"grid_mode": grid_mode_name} if grid_mode_name != variant.name else {}),
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
        GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
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
    grid_mode_name: str,
    params: Mapping[str, Any],
    names: Sequence[str],
) -> str:
    payload = {
        "semantic_identity_schema_version": GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION,
        "variant": variant_name,
        "params": {name: _jsonable_value(params[name]) for name in names if name in params},
    }
    if grid_mode_name != variant_name:
        payload["grid_mode"] = grid_mode_name
    return _stable_json(payload)


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
            build_execution_data_batch=hooks.build_execution_data_batch,
            normalize_params=hooks.normalize_params,
            label=hooks.label,
            signal_param_names=hooks.signal_param_names,
            dataprep_param_names=hooks.dataprep_param_names,
            function_fingerprint=_callable_fingerprint(hooks.build_execution_data),
            entry_filter_context=hooks.entry_filter_context,
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


def _cache_group_plan(
    plan: GridV2Plan,
    hooks: GridV2StrategyHooks,
    selected_indices: Sequence[int],
    *,
    signal_only: bool,
) -> _CacheGroupPlan:
    table = plan.candidate_table
    if not selected_indices:
        return _CacheGroupPlan(codes=(), representatives={})

    selected = np.asarray(tuple(int(index) for index in selected_indices), dtype=np.int64)
    selected_grid_mode_codes = table.grid_mode_codes[selected]
    codes = np.empty(int(selected.shape[0]), dtype=np.int32)
    group_codes: dict[tuple[Any, ...], int] = {}
    representatives: dict[int, int] = {}
    next_code = 0

    for grid_mode_code, grid_mode_name in enumerate(table.grid_mode_names):
        positions = np.flatnonzero(selected_grid_mode_codes == int(grid_mode_code))
        if positions.size == 0:
            continue
        row_indices = selected[positions]
        descriptor, axis_columns = _cache_group_descriptor_for_grid_mode(
            plan,
            hooks,
            int(grid_mode_code),
            signal_only=signal_only,
        )
        if axis_columns:
            matrix = table.axis_value_codes[row_indices][:, list(axis_columns)]
            unique_rows, first_positions, inverse = np.unique(
                matrix,
                axis=0,
                return_index=True,
                return_inverse=True,
            )
        else:
            unique_rows = np.empty((1, 0), dtype=np.int32)
            first_positions = np.asarray([0], dtype=np.int64)
            inverse = np.zeros(int(row_indices.shape[0]), dtype=np.int32)

        local_to_global = np.empty(int(unique_rows.shape[0]), dtype=np.int32)
        for local_code, unique_row in enumerate(unique_rows):
            group_key = (
                descriptor,
                tuple(int(value) for value in unique_row),
            )
            group_code = group_codes.get(group_key)
            if group_code is None:
                group_code = next_code
                group_codes[group_key] = group_code
                representatives[group_code] = int(row_indices[int(first_positions[local_code])])
                next_code += 1
            local_to_global[local_code] = int(group_code)
        codes[positions] = local_to_global[inverse]

    return _CacheGroupPlan(
        codes=tuple(int(code) for code in codes),
        representatives=representatives,
    )


def _cache_group_descriptor_for_grid_mode(
    plan: GridV2Plan,
    hooks: GridV2StrategyHooks,
    grid_mode_code: int,
    *,
    signal_only: bool,
) -> tuple[tuple[Any, ...], tuple[int, ...]]:
    table = plan.candidate_table
    grid_mode_name = table.grid_mode_names[int(grid_mode_code)]
    variant_name = table.variant_name_by_grid_mode[grid_mode_name]
    variant_code = table.variant_names.index(variant_name)
    param_names = _cache_param_names_for_grid_mode(plan, hooks, grid_mode_name, signal_only=signal_only)
    variant_axis_names = set(table.axis_names_by_variant[grid_mode_name])
    constants: list[tuple[str, tuple[Any, ...]]] = []
    axis_columns: list[int] = []
    axis_names: list[str] = []
    for name in param_names:
        column = table.axis_column_by_name.get(name)
        if column is not None and name in variant_axis_names:
            axis_columns.append(int(column))
            axis_names.append(name)
            continue
        if name in table.seed_params_by_variant[grid_mode_name]:
            constants.append(
                (
                    name,
                    (
                        "value",
                        _hashable_jsonable_value(
                            _jsonable_value(table.seed_params_by_variant[grid_mode_name].get(name))
                        ),
                    ),
                )
            )
        else:
            constants.append((name, ("missing",)))
    if signal_only:
        descriptor = ("signal", tuple(constants), tuple(axis_names))
    else:
        descriptor = (
            "dataprep",
            variant_name,
            *(() if grid_mode_name == variant_name else (grid_mode_name,)),
            table.mode_tuples_by_variant[int(variant_code)],
            tuple(constants),
            tuple(axis_names),
        )
    return descriptor, tuple(axis_columns)


def _grid_mode_has_param(table: GridV2CandidateTable, grid_mode_name: str, name: str) -> bool:
    return (
        name in table.seed_params_by_variant[grid_mode_name]
        or name in table.axis_names_by_variant[grid_mode_name]
    )


def _cache_param_names_for_grid_mode(
    plan: GridV2Plan,
    hooks: GridV2StrategyHooks,
    grid_mode_name: str,
    *,
    signal_only: bool,
) -> tuple[str, ...]:
    table = plan.candidate_table
    active = set(table.active_names_by_variant[grid_mode_name])
    if signal_only and hooks.signal_param_names is not None:
        return tuple(
            name
            for name in hooks.signal_param_names
            if name in active and _grid_mode_has_param(table, grid_mode_name, name)
        )
    if not signal_only and hooks.dataprep_param_names is not None:
        return tuple(
            name
            for name in hooks.dataprep_param_names
            if name in active and _grid_mode_has_param(table, grid_mode_name, name)
        )
    if signal_only:
        return tuple(
            name
            for name in plan.profile.parameter_names
            if name in active and plan.profile.parameter_roles.get(name) == "signal"
        )
    names: set[str] = set(_cache_param_names_for_grid_mode(plan, hooks, grid_mode_name, signal_only=True))
    variant_name = table.variant_name_by_grid_mode[grid_mode_name]
    for mode_field, mode_value in table.profile.variants[variant_name].modes.items():
        binding = mode_binding_for(mode_field, mode_value)
        if binding is None or not binding.dataprep:
            continue
        names.update(
            name
            for name in (binding.dataprep_params or binding.consumes_params)
            if name in active and _grid_mode_has_param(table, grid_mode_name, name)
        )
    if not names:
        names.update(active)
    return tuple(name for name in plan.profile.parameter_names if name in names)


def _cache_param_names(
    plan: GridV2Plan,
    candidate: GridV2Candidate,
    hooks: GridV2StrategyHooks,
    *,
    signal_only: bool,
) -> tuple[str, ...]:
    active = set(candidate.active_param_names)
    if signal_only and hooks.signal_param_names is not None:
        return tuple(name for name in hooks.signal_param_names if name in active and name in candidate.params)
    if not signal_only and hooks.dataprep_param_names is not None:
        return tuple(name for name in hooks.dataprep_param_names if name in active and name in candidate.params)
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
        names.update(
            name for name in (binding.dataprep_params or binding.consumes_params) if name in candidate.params
        )
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
    active = set(table.active_names_for_index(candidate_index))
    if signal_only and hooks.signal_param_names is not None:
        return tuple(
            name
            for name in hooks.signal_param_names
            if name in active and table.has_param_for_index(candidate_index, name)
        )
    if not signal_only and hooks.dataprep_param_names is not None:
        return tuple(
            name
            for name in hooks.dataprep_param_names
            if name in active and table.has_param_for_index(candidate_index, name)
        )
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
        names.update(
            name
            for name in (binding.dataprep_params or binding.consumes_params)
            if table.has_param_for_index(candidate_index, name)
        )
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
    grid_mode_name = table.grid_mode_name_for_index(candidate_index)
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
        *(() if grid_mode_name == table.variant_names[variant_code] else (grid_mode_name,)),
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
    if candidate.grid_mode_name != candidate.variant_name:
        payload["grid_mode"] = candidate.grid_mode_name
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
    variant_name = table.variant_names[variant_code]
    grid_mode_name = table.grid_mode_name_for_index(candidate_index)
    payload = _cache_key_payload_for_index(plan, candidate_index, context, hooks, signal_only=False)
    return (
        payload,
        ("variant", variant_name),
        *(() if grid_mode_name == variant_name else (("grid_mode", grid_mode_name),)),
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
    if str(plan.metadata.get("planning", {}).get("effective_policy", "full")) == "sampled":
        # The legacy table packer is structurally proven only for full-order tables.
        return False
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
        normalized = hooks.normalize_params(dict(raw)) if hooks.normalize_params else raw
        if active_mode_values(plan.profile, normalized) != plan.candidate_table.modes_for_index(candidate_index):
            return False
        if hooks.normalize_params is None:
            continue
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
    indices = np.asarray(tuple(int(index) for index in candidate_indices), dtype=np.int64)
    arrays = _empty_config_arrays(int(indices.shape[0]))
    if plan.profile.modes.get("entryAdmission") == "risk_bracket_v1":
        from .engine_v2.compiled_kernel import pack_compiled_config_arrays_from_rows
        # Typed accessors retain table semantics without materializing full candidates.
        return pack_compiled_config_arrays_from_rows(row_count=len(indices),
            get_value=lambda row, name, default: table.param_value_for_index(int(indices[row]), name)
                if table.has_param_for_index(int(indices[row]), name) else default,
            get_modes=lambda row: table.modes_for_index(int(indices[row])), trade_start_idx=trade_start_idx,
            profile=plan.profile, admission_rules=plan.settings.admission_rules)
    if indices.size == 0:
        return arrays

    _fill_float_config_array(arrays["initial_capital"], table, indices, "initialCapital", 100.0)
    _fill_float_config_array(arrays["commission_pct"], table, indices, "commissionPct", 0.0)
    _fill_float_config_array(arrays["stop_x"], table, indices, "stopX", 2.0)
    _fill_float_config_array(arrays["reward_risk"], table, indices, "stopRR", 2.0)
    _fill_float_config_array(arrays["max_stop_pct"], table, indices, "stopMaxPct", math.inf)
    _fill_float_config_array(arrays["max_days"], table, indices, "stopMaxDays", math.inf)
    _fill_float_config_array(arrays["risk_per_trade_pct"], table, indices, "riskPerTrade", 2.0)
    _fill_float_config_array(arrays["contract_size"], table, indices, "contractSize", 0.01)
    _fill_float_config_array(arrays["trail_activation_rr"], table, indices, "trailRR", 1.0)
    _fill_float_config_array(arrays["trail_distance_r"], table, indices, "trailDistanceR", 0.0)
    _fill_float_config_array(
        arrays["chandelier_atr_mult"], table, indices, "chandelierATRMult", 0.0
    )
    _fill_float_config_array(arrays["sar_speed"], table, indices, "sarSpeed", 0.0)
    _fill_timestamp_config_array(
        arrays["start_ns"],
        table,
        indices,
        "start",
        np.iinfo(np.int64).min,
    )
    _fill_timestamp_config_array(
        arrays["end_ns"],
        table,
        indices,
        "end",
        np.iinfo(np.int64).max,
    )
    _fill_bool_config_array(arrays["enable_long"], table, indices, "enableLong", True)
    _fill_bool_config_array(arrays["enable_short"], table, indices, "enableShort", True)
    _fill_bool_config_array(arrays["use_date_filter"], table, indices, "dateFilter", True)
    _fill_mode_config_arrays(arrays, table, indices)
    _fill_tick_size_config_array(arrays["tick_size"], arrays["rounding_code"], table, indices)
    return arrays


def _iter_variant_index_groups(
    table: GridV2CandidateTable,
    indices: np.ndarray,
):
    variant_codes = table.variant_codes[indices]
    for variant_code, variant_name in enumerate(table.variant_names):
        positions = np.flatnonzero(variant_codes == int(variant_code))
        if positions.size:
            yield variant_name, positions, indices[positions]


def _iter_grid_mode_index_groups(
    table: GridV2CandidateTable,
    indices: np.ndarray,
):
    grid_mode_codes = table.grid_mode_codes[indices]
    for grid_mode_code, grid_mode_name in enumerate(table.grid_mode_names):
        positions = np.flatnonzero(grid_mode_codes == int(grid_mode_code))
        if positions.size:
            yield grid_mode_name, positions, indices[positions]


def _fill_float_config_array(
    output: np.ndarray,
    table: GridV2CandidateTable,
    indices: np.ndarray,
    name: str,
    default: float,
) -> None:
    output.fill(float(default))
    column = table.axis_column_by_name.get(name)
    domain_values = None
    if column is not None:
        domain_values = np.asarray(
            [float(value) for value in table.parameter_domains[name].values],
            dtype=np.float64,
        )
    for grid_mode_name, positions, row_indices in _iter_grid_mode_index_groups(table, indices):
        if column is not None and name in table.axis_names_by_variant[grid_mode_name]:
            codes = table.axis_value_codes[row_indices, int(column)]
            output[positions] = domain_values[codes]  # type: ignore[index]
        elif name in table.seed_params_by_variant[grid_mode_name]:
            output[positions] = float(table.seed_params_by_variant[grid_mode_name][name])


def _fill_bool_config_array(
    output: np.ndarray,
    table: GridV2CandidateTable,
    indices: np.ndarray,
    name: str,
    default: bool,
) -> None:
    output.fill(bool(default))
    column = table.axis_column_by_name.get(name)
    domain_values = None
    if column is not None:
        domain_values = np.asarray(
            [_compiled_coerce_bool(value, default) for value in table.parameter_domains[name].values],
            dtype=np.bool_,
        )
    for grid_mode_name, positions, row_indices in _iter_grid_mode_index_groups(table, indices):
        if column is not None and name in table.axis_names_by_variant[grid_mode_name]:
            codes = table.axis_value_codes[row_indices, int(column)]
            output[positions] = domain_values[codes]  # type: ignore[index]
        elif name in table.seed_params_by_variant[grid_mode_name]:
            output[positions] = _compiled_coerce_bool(table.seed_params_by_variant[grid_mode_name][name], default)


def _fill_timestamp_config_array(
    output: np.ndarray,
    table: GridV2CandidateTable,
    indices: np.ndarray,
    name: str,
    default: int,
) -> None:
    output.fill(int(default))
    cache: dict[Any, int] = {}
    column = table.axis_column_by_name.get(name)
    domain_values = None
    if column is not None:
        domain_values = np.asarray(
            [
                _cached_timestamp_ns(value, int(default), cache)
                for value in table.parameter_domains[name].values
            ],
            dtype=np.int64,
        )
    for grid_mode_name, positions, row_indices in _iter_grid_mode_index_groups(table, indices):
        if column is not None and name in table.axis_names_by_variant[grid_mode_name]:
            codes = table.axis_value_codes[row_indices, int(column)]
            output[positions] = domain_values[codes]  # type: ignore[index]
        elif name in table.seed_params_by_variant[grid_mode_name]:
            output[positions] = _cached_timestamp_ns(
                table.seed_params_by_variant[grid_mode_name][name],
                int(default),
                cache,
            )


def _fill_mode_config_arrays(
    arrays: Mapping[str, np.ndarray],
    table: GridV2CandidateTable,
    indices: np.ndarray,
) -> None:
    for variant_name, positions, _row_indices in _iter_variant_index_groups(table, indices):
        (
            target_enabled,
            packed_trail_mode_code,
            max_days_enabled,
            strict_boundary,
            boundary_none,
            report_margin,
            rounding_code,
        ) = _compiled_mode_state(table.profile.variants[variant_name].modes)
        arrays["target_enabled"][positions] = target_enabled
        arrays["trail_mode_code"][positions] = packed_trail_mode_code
        arrays["max_days_enabled"][positions] = max_days_enabled
        arrays["strict_boundary"][positions] = strict_boundary
        arrays["boundary_none"][positions] = boundary_none
        arrays["report_margin"][positions] = report_margin
        arrays["rounding_code"][positions] = rounding_code

    stateful = arrays["trail_mode_code"] >= TRAIL_MODE_R_DISTANCE
    if np.any(stateful & (~np.isfinite(arrays["trail_activation_rr"]) | (arrays["trail_activation_rr"] <= 0.0))):
        raise ValueError("trailRR must be finite and positive for the active stateful trail mode.")
    r_distance = arrays["trail_mode_code"] == TRAIL_MODE_R_DISTANCE
    if np.any(r_distance & (~np.isfinite(arrays["trail_distance_r"]) | (arrays["trail_distance_r"] <= 0.0))):
        raise ValueError("trailDistanceR must be finite and positive for r_distance.")
    chandelier = arrays["trail_mode_code"] == TRAIL_MODE_CHANDELIER
    if np.any(chandelier & (~np.isfinite(arrays["chandelier_atr_mult"]) | (arrays["chandelier_atr_mult"] <= 0.0))):
        raise ValueError("chandelierATRMult must be finite and positive for chandelier.")
    sar = arrays["trail_mode_code"] == TRAIL_MODE_FIXED_AF_SAR
    if np.any(sar & (~np.isfinite(arrays["sar_speed"]) | (arrays["sar_speed"] <= 0.0) | (arrays["sar_speed"] > 1.0))):
        raise ValueError("sarSpeed must be greater than 0 and no greater than 1.")


def _fill_tick_size_config_array(
    output: np.ndarray,
    rounding_codes: np.ndarray,
    table: GridV2CandidateTable,
    indices: np.ndarray,
) -> None:
    output.fill(math.nan)
    column = table.axis_column_by_name.get("tickSize")
    domain_values = None
    if column is not None:
        domain_values = np.asarray(
            [validate_tick_size(float(value)) for value in table.parameter_domains["tickSize"].values],
            dtype=np.float64,
        )
    for grid_mode_name, positions, row_indices in _iter_grid_mode_index_groups(table, indices):
        tick_positions = positions[rounding_codes[positions] == ROUNDING_TICK_OUTWARD_CODE]
        if tick_positions.size == 0:
            continue
        if column is not None and "tickSize" in table.axis_names_by_variant[grid_mode_name]:
            tick_rows = row_indices[rounding_codes[positions] == ROUNDING_TICK_OUTWARD_CODE]
            codes = table.axis_value_codes[tick_rows, int(column)]
            output[tick_positions] = domain_values[codes]  # type: ignore[index]
        elif "tickSize" in table.seed_params_by_variant[grid_mode_name]:
            output[tick_positions] = validate_tick_size(float(table.seed_params_by_variant[grid_mode_name]["tickSize"]))
        else:
            raise ValueError("tickSize is required when priceRounding='tick_outward'.")


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
    compute_sharpe: bool = False,
    compute_sharpe_daily: bool = False,
    compute_sqn: bool = False,
    compute_max_drawdown_mtm: bool = False,
) -> GridV2ResultRow:
    table = plan.candidate_table
    params = params or table.params_for_index(candidate_index)
    result = run.strategy_result
    advanced_metrics = run.advanced_metrics
    initial_balance = float(params.get("initialCapital", 100.0))
    core = compute_core_metrics_from_balance_and_trades(
        result.balance_curve,
        result.trades,
        initial_balance=initial_balance,
    )
    return GridV2ResultRow(
        candidate_id=table.candidate_id_for_index(candidate_index),
        semantic_key=table.semantic_key_for_index(candidate_index),
        canonical_identity=_LazyCanonicalIdentity(table, candidate_index),
        variant_name=table.variant_name_for_index(candidate_index),
        grid_mode_name=table.grid_mode_name_for_index(candidate_index),
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
        sharpe_ratio=(
            _internal_metric_value(advanced_metrics.sharpe_ratio)
            if compute_sharpe
            else _UNDEFINED_METRIC
        ),
        sqn=_internal_metric_value(advanced_metrics.sqn) if compute_sqn else _UNDEFINED_METRIC,
        sharpe_daily=(
            _internal_metric_value(advanced_metrics.sharpe_daily)
            if compute_sharpe_daily
            else _UNDEFINED_METRIC
        ),
        sharpe_daily_observations=(
            advanced_metrics.sharpe_daily_observations if compute_sharpe_daily else None
        ),
        sharpe_daily_active_days=(
            advanced_metrics.sharpe_daily_active_days if compute_sharpe_daily else None
        ),
        max_drawdown_mtm_pct=(
            _internal_metric_value(run.max_drawdown_mtm_pct)
            if compute_max_drawdown_mtm
            else _UNDEFINED_METRIC
        ),
        backend_kind=REFERENCE_BATCH_KIND,
    )


def _row_from_compiled_output(
    plan: GridV2Plan,
    candidate_index: int,
    values: Sequence[Any],
    *,
    params: Mapping[str, Any] | None = None,
    max_drawdown_mtm_pct: float | None = None,
) -> GridV2ResultRow:
    from .engine_v2.contracts import GUARDRAIL_FLAG_EXECUTION_ERROR
    if int(values[OUTPUT_FLAGS]) & GUARDRAIL_FLAG_EXECUTION_ERROR:
        return _error_row(plan, candidate_index, ValueError("Admission execution failed: corrupt capital or unrepresentable sizing."))
    table = plan.candidate_table
    idx = int(candidate_index)
    variant_name = table.variant_names[int(table.variant_codes[idx])]
    semantic_key = (
        table.semantic_keys_by_row[idx]
        if table.semantic_keys_by_row is not None
        else table.semantic_key_for_index(idx)
    )
    if params is None:
        params = _LazyCandidateParams(table, idx)
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
        canonical_identity=_LazyCanonicalIdentity(table, idx),
        variant_name=variant_name,
        grid_mode_name=table.grid_mode_name_for_index(idx),
        modes=table.modes_for_index(idx),
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
        sharpe_ratio=_internal_metric_value(values[OUTPUT_SHARPE_RATIO]),
        sqn=_internal_metric_value(values[OUTPUT_SQN]),
        sharpe_daily=_internal_metric_value(values[OUTPUT_SHARPE_DAILY]),
        sharpe_daily_observations=_optional_compiled_count(
            values[OUTPUT_SHARPE_DAILY_OBSERVATIONS],
            "sharpe_daily_observations",
        ),
        sharpe_daily_active_days=_optional_compiled_count(
            values[OUTPUT_SHARPE_DAILY_ACTIVE_DAYS],
            "sharpe_daily_active_days",
        ),
        max_drawdown_mtm_pct=(
            _internal_metric_value(max_drawdown_mtm_pct)
            if max_drawdown_mtm_pct is not None
            else _UNDEFINED_METRIC
        ),
        backend_kind=COMPILED_BATCH_KIND,
    )


def _error_row(plan: GridV2Plan, candidate_index: int, exc: Exception) -> GridV2ResultRow:
    table = plan.candidate_table
    return GridV2ResultRow(
        candidate_id=table.candidate_id_for_index(candidate_index),
        semantic_key=table.semantic_key_for_index(candidate_index),
        canonical_identity=None,
        variant_name=table.variant_name_for_index(candidate_index),
        grid_mode_name=table.grid_mode_name_for_index(candidate_index),
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
        sharpe_ratio=_UNDEFINED_METRIC,
        sqn=_UNDEFINED_METRIC,
        sharpe_daily=_UNDEFINED_METRIC,
        sharpe_daily_observations=None,
        sharpe_daily_active_days=None,
        status="error",
        error=str(exc),
    )


def _internal_metric_value(value: Any) -> float:
    if value is None:
        return _UNDEFINED_METRIC
    numeric = float(value)
    return numeric if math.isfinite(numeric) else _UNDEFINED_METRIC


def _optional_compiled_count(value: Any, name: str) -> Optional[int]:
    numeric = float(value)
    if math.isnan(numeric):
        return None
    if not math.isfinite(numeric) or numeric < 0.0 or not numeric.is_integer():
        raise ValueError(f"Compiled Grid V2 {name} must be NaN or a non-negative integer.")
    if numeric > float(np.iinfo(np.int64).max):
        raise ValueError(f"Compiled Grid V2 {name} is out of range.")
    return int(numeric)


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
    df: pd.DataFrame,
    context: _CacheKeyContext,
    trade_start_idx: int,
    hooks: GridV2StrategyHooks,
    row: GridV2ResultRow,
    dataprep_cache: Mapping[Any, ExecutionData],
    *,
    compute_sharpe_daily: bool = False,
) -> GridV2SelectedResult:
    if row.status != "ok":
        return GridV2SelectedResult(row=row, metrics={}, guardrail_summary={})
    candidate_index = plan.candidate_table.validate_candidate_id(row.candidate_id)
    data_key = _dataprep_cache_key_for_index(plan, candidate_index, context, hooks)
    params = plan.candidate_table.params_for_index(candidate_index)
    normalized_params = _normalized_candidate_params(hooks, params)
    data = dataprep_cache.get(data_key)
    if data is None:
        data = hooks.build_execution_data(df, normalized_params)
    run = run_v2_strategy(
        entry_filter_context=hooks.entry_filter_context,
        admission_rules=plan.settings.admission_rules,
        data=data,
        profile=plan.profile,
        params=normalized_params,
        trade_start_idx=trade_start_idx,
        compute_sharpe_daily=compute_sharpe_daily,
    )
    basic_metrics = run.basic_metrics
    advanced_metrics = run.advanced_metrics
    metrics = {
        "net_profit_pct": basic_metrics.net_profit_pct,
        "max_drawdown_pct": basic_metrics.max_drawdown_pct,
        "romad": advanced_metrics.romad,
        "profit_factor": advanced_metrics.profit_factor,
        "win_rate_pct": basic_metrics.win_rate,
        "total_trades": basic_metrics.total_trades,
        "winning_trades": basic_metrics.winning_trades,
        "losing_trades": basic_metrics.losing_trades,
        "gross_profit": basic_metrics.gross_profit,
        "gross_loss": basic_metrics.gross_loss,
        "max_consecutive_losses": basic_metrics.max_consecutive_losses,
        "sharpe_ratio": advanced_metrics.sharpe_ratio,
        "sharpe_daily": advanced_metrics.sharpe_daily,
        "sharpe_daily_observations": advanced_metrics.sharpe_daily_observations,
        "sharpe_daily_active_days": advanced_metrics.sharpe_daily_active_days,
        "sortino_ratio": advanced_metrics.sortino_ratio,
        "sqn": advanced_metrics.sqn,
        "ulcer_index": advanced_metrics.ulcer_index,
        "consistency_score": advanced_metrics.consistency_score,
    }
    return GridV2SelectedResult(
        row=row,
        metrics=_jsonable_mapping(metrics),
        guardrail_summary=_guardrail_mapping(run.guardrail_summary),
        entry_diagnostics=getattr(run.strategy_result, "entry_diagnostics", None),
    )


__all__ = [
    "GRID_V2_ENGINE_VERSION",
    "GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION",
    "GRID_V2_SEMANTIC_IDENTITY_SCHEMA_VERSION",
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
    "GridV2PlanReuseCache",
    "GridV2PlanReuseResult",
    "GridV2PlanReuseStats",
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
