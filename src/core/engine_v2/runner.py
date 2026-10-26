"""Backtester V2 runner that adapts kernel output to Merlin results."""

from __future__ import annotations

from dataclasses import dataclass, fields
import math
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult
from core.metrics import AdvancedMetrics, BasicMetrics

from .contracts import GuardrailSummary, StandingState
from .kernel import ExecutionData, KernelConfig, KernelResult, run_reference_kernel
from .kernel_signal import SignalKernelConfig, run_signal_reversal_kernel
from .execution_modes import (
    resolve_position_mode_state,
    resolve_signal_reversal_mode_state,
    stateful_trail_params,
)
from .price_rounding import PRICE_ROUNDING_NONE, PRICE_ROUNDING_TICK_OUTWARD, validate_tick_size
from .profile import active_mode_values
from .admission import AdmissionRules, AdmissionStrategyResult, execution_admission, summarize_entries


@dataclass(frozen=True)
class V2RunResult:
    """High-level V2 run output with compact execution telemetry."""

    strategy_result: StrategyResult
    basic_metrics: BasicMetrics
    advanced_metrics: AdvancedMetrics
    guardrail_summary: GuardrailSummary
    standing_state: StandingState
    kernel_result: KernelResult
    max_drawdown_mtm_pct: float | None = None


def _max_drawdown_mtm_from_equity(
    equity_curve: Sequence[float],
    trade_start_idx: int,
    initial_equity: float,
) -> float:
    """Return sticky-NaN bar-close MTM drawdown for the evaluation interval."""

    start = max(0, int(trade_start_idx))
    if start >= len(equity_curve):
        return float("nan")
    peak = float(initial_equity)
    maximum = 0.0
    for value in equity_curve[start:]:
        equity = float(value)
        if not math.isfinite(equity):
            return float("nan")
        if equity > peak:
            peak = equity
        elif peak > 0.0 and equity < peak:
            maximum = max(maximum, (peak - equity) / peak * 100.0)
    return maximum


def _coerce_bool(value: Any, default: bool) -> bool:
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
    return default


def _timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _validate_price_rounding_mode(mode: str, params: Mapping[str, Any]) -> tuple[str, float]:
    if mode == PRICE_ROUNDING_NONE:
        return mode, float("nan")
    if mode == PRICE_ROUNDING_TICK_OUTWARD:
        if "tickSize" not in params:
            raise ValueError("tickSize is required when priceRounding='tick_outward'.")
        return mode, validate_tick_size(float(params["tickSize"]))
    raise ValueError(f"Unsupported Phase-1 priceRounding mode: {mode!r}.")


def build_kernel_config(
    *,
    profile: Any,
    params: Mapping[str, Any],
    trade_start_idx: int = 0,
    admission_rules: AdmissionRules | None = None,
) -> KernelConfig:
    """Convert a parsed execution profile and params into kernel settings."""

    modes = active_mode_values(profile, params)
    mode_state = resolve_position_mode_state(modes)
    target_mode = mode_state.target_mode
    trail_mode = mode_state.trail_mode
    trail_activation_mode = mode_state.trail_activation_mode
    margin_mode = mode_state.margin_mode
    boundary_mode = mode_state.boundary_mode
    price_rounding_mode, tick_size = _validate_price_rounding_mode(
        mode_state.price_rounding_mode,
        params,
    )
    trail_params = stateful_trail_params(trail_mode, params)
    admission = execution_admission(profile, params, admission_rules)
    return KernelConfig(
        initial_capital=float(params.get("initialCapital", 100.0)),
        commission_pct=float(params.get("commissionPct", 0.0)),
        stop_x=float(params.get("stopX", 2.0)),
        reward_risk=float(params.get("stopRR", 2.0)),
        max_stop_pct=float(params.get("stopMaxPct", float("inf"))),
        max_days=float(params.get("stopMaxDays", float("inf"))),
        risk_per_trade_pct=float(params.get("riskPerTrade", 2.0)),
        contract_size=float(admission[1].base_step) if admission else float(params.get("contractSize", 0.01)),
        entry_admission=modes.get("entryAdmission"),
        admission_rules=admission_rules if admission else None,
        max_entry_leverage=admission[0] if admission else 8.0,
        enable_long=_coerce_bool(params.get("enableLong"), True),
        enable_short=_coerce_bool(params.get("enableShort"), True),
        target_mode=target_mode,
        trail_mode=trail_mode,
        trail_activation_mode=trail_activation_mode,
        trail_activation_rr=trail_params.trail_activation_rr,
        trail_distance_r=trail_params.trail_distance_r,
        chandelier_atr_mult=trail_params.chandelier_atr_mult,
        sar_speed=trail_params.sar_speed,
        max_days_enabled=mode_state.max_days_enabled,
        boundary_mode=boundary_mode,
        margin_mode=margin_mode,
        trade_start_idx=trade_start_idx,
        use_date_filter=_coerce_bool(params.get("dateFilter"), True),
        start=_timestamp(params.get("start")),
        end=_timestamp(params.get("end")),
        price_rounding_mode=price_rounding_mode,
        tick_size=tick_size,
    )


def build_signal_kernel_config(
    *,
    profile: Any,
    params: Mapping[str, Any],
    trade_start_idx: int = 0,
) -> SignalKernelConfig:
    """Convert a signal_reversal profile and params into kernel settings."""

    modes = active_mode_values(profile, params)
    mode_state = resolve_signal_reversal_mode_state(modes)
    stop_mode = mode_state.stop_mode
    boundary_mode = mode_state.boundary_mode

    return SignalKernelConfig(
        initial_capital=float(params.get("initialCapital", 100.0)),
        commission_pct=float(params.get("commissionPct", 0.0)),
        position_pct=float(params.get("positionPct", 100.0)),
        contract_size=float(params.get("contractSize", 0.01)),
        enable_long=_coerce_bool(params.get("enableLong"), True),
        enable_short=_coerce_bool(params.get("enableShort"), True),
        emergency_stop_enabled=stop_mode == "emergency_pct",
        emergency_sl_pct=float(params.get("emergencySlPct", 20.0)),
        emergency_sl_update_bars=int(params.get("emergencySlUpdateBars", 16)),
        boundary_mode=boundary_mode,
        trade_start_idx=trade_start_idx,
        use_date_filter=_coerce_bool(params.get("dateFilter"), True),
        start=_timestamp(params.get("start")),
        end=_timestamp(params.get("end")),
    )


def run_v2_strategy(
    *,
    data: ExecutionData,
    profile: Any,
    params: Mapping[str, Any],
    trade_start_idx: int = 0,
    compute_sharpe_daily: bool = False,
    compute_max_drawdown_mtm: bool = False,
    admission_rules: AdmissionRules | None = None,
) -> V2RunResult:
    """Run V2 execution and return an enriched Merlin strategy result."""

    modes = active_mode_values(profile, params)
    topology = modes.get("topology")
    if topology == "signal_reversal":
        config = build_signal_kernel_config(profile=profile, params=params, trade_start_idx=trade_start_idx)
        kernel_result = run_signal_reversal_kernel(data, config)
        initial_balance = config.initial_capital
    elif topology is None:
        config = build_kernel_config(profile=profile, params=params, trade_start_idx=trade_start_idx,
                                     admission_rules=admission_rules)
        kernel_result = run_reference_kernel(data, config)
        initial_balance = config.initial_capital
    else:
        raise ValueError(f"Unsupported V2 execution topology: {topology!r}.")
    result_type = AdmissionStrategyResult if kernel_result.entry_events is not None else StrategyResult
    strategy_result = result_type(
        trades=kernel_result.trades,
        equity_curve=kernel_result.equity_curve,
        balance_curve=kernel_result.balance_curve,
        timestamps=kernel_result.timestamps,
        metric_start_idx=trade_start_idx,
        metric_initial_equity=initial_balance,
    )
    if kernel_result.entry_events is not None:
        strategy_result.entry_events = kernel_result.entry_events
        strategy_result.entry_diagnostics = summarize_entries(kernel_result.entry_events)
    if kernel_result.entry_events is not None and initial_balance < 0:
        # Finite exhausted capital is an admission refusal. Return the actual
        # unchanged account curve; return-based metrics have no valid anchor.
        basic_metrics = metrics.calculate_basic(strategy_result, initial_balance=initial_balance)
        advanced_metrics = AdvancedMetrics()
        for name, value in basic_metrics.to_dict().items():
            if name in {field.name for field in fields(strategy_result)}:
                setattr(strategy_result, name, value)
    else:
        basic_metrics, advanced_metrics = metrics.enrich_strategy_result(
            strategy_result,
            initial_balance=initial_balance,
            risk_free_rate=0.02,
            compute_sharpe_daily=compute_sharpe_daily,
        )
    return V2RunResult(
        strategy_result=strategy_result,
        basic_metrics=basic_metrics,
        advanced_metrics=advanced_metrics,
        guardrail_summary=kernel_result.guardrail_summary,
        standing_state=kernel_result.standing_state,
        kernel_result=kernel_result,
        max_drawdown_mtm_pct=(
            _max_drawdown_mtm_from_equity(
                kernel_result.equity_curve,
                trade_start_idx,
                initial_balance,
            )
            if compute_max_drawdown_mtm
            else None
        ),
    )


__all__ = ["V2RunResult", "build_kernel_config", "build_signal_kernel_config", "run_v2_strategy"]
