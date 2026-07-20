"""Backtester V2 Phase 0 contracts.

These dataclasses define stable, importable shapes for later V2 phases. They
intentionally avoid runtime behavior and pandas objects so future Numba-facing
code can pack them into primitive arrays without inheriting V1 engine state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

import numpy as np


GUARDRAIL_FLAG_CORRECTED_FILL = 1
GUARDRAIL_FLAG_REJECTED_FILL = 2
GUARDRAIL_FLAG_INVALID_STOP_DISTANCE = 4
GUARDRAIL_FLAG_ZERO_SIZE_ENTRY = 8
GUARDRAIL_FLAG_MARGIN_REJECT = 16
GUARDRAIL_FLAG_LIQUIDATION = 32
GUARDRAIL_FLAG_NO_CAPITAL_HALT = 64
GUARDRAIL_FLAG_CLAMP_MODE_USED = 128

EXECUTION_REASON_SIGNAL_ENTRY_NEXT_OPEN = "signal_entry_next_open"
EXECUTION_REASON_MAX_DAYS_CLOSE_NEXT_OPEN = "max_days_close_next_open"
EXECUTION_REASON_TRAIL_RATCHET = "trail_ratchet"
EXECUTION_REASON_STOP_HIT = "stop_hit"
EXECUTION_REASON_TARGET_HIT = "target_hit"
EXECUTION_REASON_BOUNDARY_STATE = "boundary_state"
EXECUTION_REASON_MARGIN_REJECT = "margin_reject"
EXECUTION_REASON_NO_CAPITAL_HALT = "no_capital_halt"


def _array_1d(name: str, values: Any) -> np.ndarray:
    array = np.asarray(values)
    if array.ndim != 1:
        raise ValueError(f"{name} must be a 1D array.")
    return array


def _bool_array(name: str, values: Any, expected_length: Optional[int] = None) -> np.ndarray:
    array = _array_1d(name, values)
    if array.dtype != np.bool_:
        raise ValueError(f"{name} must be a boolean array.")
    if expected_length is not None and len(array) != expected_length:
        raise ValueError(f"{name} length must match entry signal length.")
    return array


def _float_array(name: str, values: Any, expected_length: int) -> np.ndarray:
    array = _array_1d(name, values)
    if not np.issubdtype(array.dtype, np.floating):
        raise ValueError(f"{name} must be a float array.")
    if len(array) != expected_length:
        raise ValueError(f"{name} length must match entry signal length.")
    return array


@dataclass(frozen=True)
class Signals:
    """Causal strategy signal arrays aligned to the full prepared dataset."""

    long_entries: np.ndarray
    short_entries: np.ndarray
    long_exits: Optional[np.ndarray] = None
    short_exits: Optional[np.ndarray] = None
    long_entry_levels: Optional[np.ndarray] = None
    short_entry_levels: Optional[np.ndarray] = None

    def __post_init__(self) -> None:
        long_entries = _bool_array("long_entries", self.long_entries)
        short_entries = _bool_array("short_entries", self.short_entries, len(long_entries))
        object.__setattr__(self, "long_entries", long_entries)
        object.__setattr__(self, "short_entries", short_entries)

        for name in ("long_exits", "short_exits"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _bool_array(name, value, len(long_entries)))

        for name in ("long_entry_levels", "short_entry_levels"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _float_array(name, value, len(long_entries)))


@dataclass(frozen=True)
class VariantSelector:
    """Parameter-driven mapping from a config value to a variant name."""

    param: str
    mapping: Mapping[str, str]
    user_facing: bool = True


@dataclass(frozen=True)
class VariantSpec:
    """One resolved execution topology variant."""

    name: str
    modes: Mapping[str, str]


@dataclass(frozen=True)
class ModeBinding:
    """Normative mapping from a profile mode to consumed execution params."""

    mode_field: str
    mode_value: str
    status: str
    consumes_params: tuple[str, ...] = ()
    dataprep: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExecutionProfile:
    """Parsed V2 execution profile metadata.

    The profile is data-only: it does not execute trades and does not route a
    strategy to any runtime adapter.
    """

    strategy_id: str
    engine: str
    modes: Mapping[str, str]
    variants: Mapping[str, VariantSpec]
    variant_selector: Optional[VariantSelector] = None
    parameter_defaults: Mapping[str, Any] = field(default_factory=dict)
    parameter_names: tuple[str, ...] = ()
    parameter_roles: Mapping[str, str] = field(default_factory=dict)
    variant_independent_params: tuple[str, ...] = ()
    validation_warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class GuardrailSummary:
    """Compact guardrail telemetry for V2 execution runs."""

    corrected_fill_count: int = 0
    rejected_fill_count: int = 0
    invalid_stop_distance_count: int = 0
    zero_size_entry_count: int = 0
    margin_reject_count: int = 0
    liquidation_count: int = 0
    no_capital_halt: bool = False
    max_required_leverage: float = 0.0
    max_notional: float = 0.0
    max_initial_margin_used_pct: float = 0.0
    min_margin_buffer_pct: float = math.inf
    first_guardrail_code: int = 0
    flags: int = 0


@dataclass(frozen=True)
class StandingState:
    """End-of-window state for future boundary-free replay integrations."""

    position_direction: int = 0
    position_size: float = 0.0
    entry_price: float = math.nan
    entry_time_ns: int = 0
    anchor_price: float = math.nan
    initial_stop: float = math.nan
    active_stop: float = math.nan
    target_price: float = math.nan
    trail_active: bool = False
    trail_stop: float = math.nan
    pending_market_close: bool = False
    pending_entry_direction: int = 0
    pending_entry_order_type: str = ""
    pending_entry_anchor_price: float = math.nan
    pending_entry_trigger: float = math.nan
    pending_entry_stop: float = math.nan
    pending_entry_target: float = math.nan
    pending_entry_size: float = 0.0
    pending_entry_ttl_bars: int = 0


@dataclass(frozen=True)
class ExecutionIntent:
    """Serializable execution intent shape for later live/replay phases."""

    action: str
    reason_code: str
    direction: Optional[str] = None
    trigger_price: Optional[float] = None
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    ttl_bars: Optional[int] = None


@dataclass(frozen=True)
class OverlaySpec:
    """JSON-safe chart overlay metadata produced by a V2 strategy."""

    id: str
    label: str
    kind: str
    color: str
    data: tuple[Mapping[str, Any], ...] = ()
    line_width: int = 1
    line_style: str = "solid"


__all__ = [
    "EXECUTION_REASON_BOUNDARY_STATE",
    "EXECUTION_REASON_MARGIN_REJECT",
    "EXECUTION_REASON_MAX_DAYS_CLOSE_NEXT_OPEN",
    "EXECUTION_REASON_NO_CAPITAL_HALT",
    "EXECUTION_REASON_SIGNAL_ENTRY_NEXT_OPEN",
    "EXECUTION_REASON_STOP_HIT",
    "EXECUTION_REASON_TARGET_HIT",
    "EXECUTION_REASON_TRAIL_RATCHET",
    "GUARDRAIL_FLAG_CLAMP_MODE_USED",
    "GUARDRAIL_FLAG_CORRECTED_FILL",
    "GUARDRAIL_FLAG_INVALID_STOP_DISTANCE",
    "GUARDRAIL_FLAG_LIQUIDATION",
    "GUARDRAIL_FLAG_MARGIN_REJECT",
    "GUARDRAIL_FLAG_NO_CAPITAL_HALT",
    "GUARDRAIL_FLAG_REJECTED_FILL",
    "GUARDRAIL_FLAG_ZERO_SIZE_ENTRY",
    "ExecutionIntent",
    "ExecutionProfile",
    "GuardrailSummary",
    "ModeBinding",
    "OverlaySpec",
    "Signals",
    "StandingState",
    "VariantSelector",
    "VariantSpec",
]
