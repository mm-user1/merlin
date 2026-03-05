from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .backtest_engine import prepare_dataset_with_warmup
from .post_process import calculate_comparison_metrics
from . import metrics


def _extract_trial_number(trial: Any, fallback: int) -> int:
    if isinstance(trial, dict):
        for key in ("trial_number", "optuna_trial_number"):
            value = trial.get(key)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
    for attr in ("trial_number", "optuna_trial_number"):
        if hasattr(trial, attr):
            value = getattr(trial, attr)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
    return fallback


def _extract_attr(trial: Any, name: str, fallback: Any = None) -> Any:
    if isinstance(trial, dict):
        return trial.get(name, fallback)
    return getattr(trial, name, fallback)


def _is_stress_ok(trial: Any) -> bool:
    status = _extract_attr(trial, "status")
    if status is None:
        return _extract_attr(trial, "profit_retention") is not None
    return str(status).lower() == "ok"


def select_oos_source_candidates(
    *,
    optuna_results: Sequence[Any],
    dsr_results: Sequence[Any],
    ft_results: Sequence[Any],
    st_results: Sequence[Any],
    st_ran: bool = False,
) -> Tuple[str, List[Dict[str, int]]]:
    """
    Select OOS source candidates using last-finished-module precedence.

    Returns:
        (source_module, candidates) where candidates is a list of
        {"trial_number": int, "source_rank": int} in source order.
    """
    if st_ran:
        filtered_st = [item for item in (st_results or []) if _is_stress_ok(item)]
        return "stress_test", _build_source_candidates(filtered_st, "st_rank")
    if ft_results:
        return "forward_test", _build_source_candidates(ft_results, "ft_rank")
    if dsr_results:
        return "dsr", _build_source_candidates(dsr_results, "dsr_rank")
    return "optuna", _build_source_candidates(optuna_results or [], None)


def _build_source_candidates(items: Iterable[Any], rank_key: Optional[str]) -> List[Dict[str, int]]:
    candidates: List[Dict[str, int]] = []
    for idx, item in enumerate(items, 1):
        trial_number = _extract_trial_number(item, idx)
        if trial_number <= 0:
            continue
        source_rank = None
        if rank_key:
            source_rank = _extract_attr(item, rank_key)
        try:
            source_rank_val = int(source_rank) if source_rank is not None else idx
        except (TypeError, ValueError):
            source_rank_val = idx
        candidates.append({"trial_number": trial_number, "source_rank": source_rank_val})
    return candidates


def build_test_metrics(
    result: Any,
    *,
    consistency_segments: Optional[int] = None,
) -> Dict[str, Any]:
    basic = metrics.calculate_basic(result, 100.0)
    advanced = metrics.calculate_advanced(
        result,
        100.0,
        consistency_segments=consistency_segments,
    )
    return {
        "net_profit_pct": basic.net_profit_pct,
        "max_drawdown_pct": basic.max_drawdown_pct,
        "total_trades": basic.total_trades,
        "win_rate": basic.win_rate,
        "max_consecutive_losses": basic.max_consecutive_losses,
        "sharpe_ratio": advanced.sharpe_ratio,
        "sortino_ratio": advanced.sortino_ratio,
        "romad": advanced.romad,
        "profit_factor": advanced.profit_factor,
        "ulcer_index": advanced.ulcer_index,
        "sqn": advanced.sqn,
        "consistency_score": advanced.consistency_score,
        "consistency_segments_used": consistency_segments,
    }


def run_period_test_for_trials(
    *,
    df: pd.DataFrame,
    strategy_id: str,
    warmup_bars: int,
    fixed_params: Dict[str, Any],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    trials: Sequence[Dict[str, Any]],
    baseline_period_days: int,
    test_period_days: int,
    is_period_days_for_segments: Optional[int] = None,
    consistency_segments_is: Optional[int] = None,
    original_metrics_resolver: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        raise ValueError("Dataset is empty for period test.")
    if start_ts is None or end_ts is None:
        raise ValueError("Start/end timestamps are required for period test.")

    from strategies import get_strategy

    strategy_class = get_strategy(strategy_id)

    df_prepared, trade_start_idx = prepare_dataset_with_warmup(
        df, start_ts, end_ts, int(warmup_bars)
    )
    if df_prepared.empty:
        raise ValueError("No data available in the selected test period.")

    normalized_is_segments = (
        metrics.normalize_consistency_segments(consistency_segments_is)
        if consistency_segments_is is not None
        else None
    )
    auto_test_segments = (
        metrics.derive_auto_consistency_segments(
            is_period_days_for_segments,
            normalized_is_segments,
            test_period_days,
        )
        if normalized_is_segments is not None
        else None
    )

    results_payload: List[Dict[str, Any]] = []
    for idx, trial in enumerate(trials, 1):
        if not trial:
            continue
        trial_number = _extract_trial_number(trial, idx)
        params = {**fixed_params, **(trial.get("params") or {})}
        params["dateFilter"] = True
        params["start"] = start_ts
        params["end"] = end_ts

        result = strategy_class.run(df_prepared, params, trade_start_idx)
        test_metrics = build_test_metrics(
            result,
            consistency_segments=auto_test_segments,
        )
        original_metrics = original_metrics_resolver(trial)
        comparison = calculate_comparison_metrics(
            original_metrics,
            test_metrics,
            int(baseline_period_days or 0),
            int(test_period_days),
        )

        results_payload.append(
            {
                "trial_number": trial_number,
                "original_metrics": original_metrics,
                "test_metrics": test_metrics,
                "comparison": comparison,
            }
        )

    return results_payload
