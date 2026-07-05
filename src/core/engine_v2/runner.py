"""Backtester V2 runner that adapts kernel output to Merlin results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult

from .contracts import GuardrailSummary, StandingState
from .kernel import ExecutionData, KernelConfig, KernelResult, run_reference_kernel
from .price_rounding import PRICE_ROUNDING_NONE, PRICE_ROUNDING_TICK_OUTWARD, validate_tick_size
from .profile import active_mode_values


@dataclass(frozen=True)
class V2RunResult:
    """High-level V2 run output with compact execution telemetry."""

    strategy_result: StrategyResult
    guardrail_summary: GuardrailSummary
    standing_state: StandingState
    kernel_result: KernelResult


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


def _require_mode(modes: Mapping[str, str], name: str, expected: str) -> None:
    actual = modes.get(name)
    if actual != expected:
        raise ValueError(f"Unsupported Phase-1 execution mode {name}={actual!r}; expected {expected!r}.")


def _validate_bool_mode(name: str, value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"Unsupported Phase-1 execution mode {name}={value!r}; expected 'true' or 'false'.")


def _validate_phase1_exit_topology(
    *,
    target_mode: str,
    trail_mode: str,
    trail_activation_mode: str,
) -> None:
    if trail_activation_mode not in {"none", "rr"}:
        raise ValueError(f"Unsupported Phase-1 trailActivation mode: {trail_activation_mode!r}.")

    valid_bracket = (
        target_mode == "rr"
        and trail_mode == "none"
        and trail_activation_mode == "none"
    )
    valid_trail = (
        target_mode == "none"
        and trail_mode == "ma"
        and trail_activation_mode == "rr"
    )
    if not (valid_bracket or valid_trail):
        raise ValueError(
            "Phase 1 supports exactly one exit topology: target=rr/trail=none "
            "or target=none/trail=ma with trailActivation=rr."
        )


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
) -> KernelConfig:
    """Convert a parsed execution profile and params into kernel settings."""

    modes = active_mode_values(profile, params)
    _require_mode(modes, "entryOrder", "market_next_open")
    _require_mode(modes, "stop", "atr_swing")
    _require_mode(modes, "sizing", "risk_per_trade")

    margin_mode = modes.get("margin", "off")
    if margin_mode not in {"off", "report_only"}:
        raise ValueError(f"Unsupported Phase-1 margin mode: {margin_mode!r}.")

    boundary_mode = modes.get("boundary", "strict_close")
    if boundary_mode not in {"strict_close", "none"}:
        raise ValueError(f"Unsupported Phase-1 boundary mode: {boundary_mode!r}.")

    target_mode = modes.get("target", "none")
    trail_mode = modes.get("trail", "none")
    if target_mode not in {"rr", "none"}:
        raise ValueError(f"Unsupported Phase-1 target mode: {target_mode!r}.")
    if trail_mode not in {"ma", "none"}:
        raise ValueError(f"Unsupported Phase-1 trail mode: {trail_mode!r}.")

    trail_activation_mode = modes.get("trailActivation", "none")
    _validate_phase1_exit_topology(
        target_mode=target_mode,
        trail_mode=trail_mode,
        trail_activation_mode=trail_activation_mode,
    )

    max_days_mode = modes.get("maxDays", "false")
    max_days_enabled = _validate_bool_mode("maxDays", max_days_mode)
    price_rounding_mode, tick_size = _validate_price_rounding_mode(
        modes.get("priceRounding", PRICE_ROUNDING_NONE),
        params,
    )
    return KernelConfig(
        initial_capital=float(params.get("initialCapital", 100.0)),
        commission_pct=float(params.get("commissionPct", 0.0)),
        stop_x=float(params.get("stopX", 2.0)),
        reward_risk=float(params.get("stopRR", 2.0)),
        max_stop_pct=float(params.get("stopMaxPct", float("inf"))),
        max_days=float(params.get("stopMaxDays", float("inf"))),
        risk_per_trade_pct=float(params.get("riskPerTrade", 2.0)),
        contract_size=float(params.get("contractSize", 0.01)),
        enable_long=_coerce_bool(params.get("enableLong"), True),
        enable_short=_coerce_bool(params.get("enableShort"), True),
        target_mode=target_mode,
        trail_mode=trail_mode,
        trail_activation_mode=trail_activation_mode,
        trail_activation_rr=float(params.get("trailRR", 1.0)),
        max_days_enabled=max_days_enabled,
        boundary_mode=boundary_mode,
        margin_mode=margin_mode,
        trade_start_idx=trade_start_idx,
        use_date_filter=_coerce_bool(params.get("dateFilter"), True),
        start=_timestamp(params.get("start")),
        end=_timestamp(params.get("end")),
        price_rounding_mode=price_rounding_mode,
        tick_size=tick_size,
    )


def run_v2_strategy(
    *,
    data: ExecutionData,
    profile: Any,
    params: Mapping[str, Any],
    trade_start_idx: int = 0,
) -> V2RunResult:
    """Run V2 execution and return an enriched Merlin strategy result."""

    config = build_kernel_config(profile=profile, params=params, trade_start_idx=trade_start_idx)
    kernel_result = run_reference_kernel(data, config)
    strategy_result = StrategyResult(
        trades=kernel_result.trades,
        equity_curve=kernel_result.equity_curve,
        balance_curve=kernel_result.balance_curve,
        timestamps=kernel_result.timestamps,
    )
    metrics.enrich_strategy_result(
        strategy_result,
        initial_balance=config.initial_capital,
        risk_free_rate=0.02,
    )
    return V2RunResult(
        strategy_result=strategy_result,
        guardrail_summary=kernel_result.guardrail_summary,
        standing_state=kernel_result.standing_state,
        kernel_result=kernel_result,
    )


__all__ = ["V2RunResult", "build_kernel_config", "run_v2_strategy"]
