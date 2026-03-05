"""
Post Process module for optimization validation.

Phase 1: Forward Test (FT) implementation
- Auto FT after Optuna optimization (TRUE HOLDOUT)
- Manual testing from Results page
- WFA integration
"""
from __future__ import annotations

import logging
import multiprocessing as mp
from dataclasses import dataclass
from enum import Enum
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# Configuration Dataclasses
# ============================================================


@dataclass
class PostProcessConfig:
    """Configuration for Post Process forward test."""

    enabled: bool = False
    ft_period_days: int = 30
    top_k: int = 20
    sort_metric: str = "profit_degradation"  # or "ft_romad"
    warmup_bars: int = 1000


@dataclass
class DSRConfig:
    """Configuration for Deflated Sharpe Ratio (DSR) analysis."""

    enabled: bool = False
    top_k: int = 20
    warmup_bars: int = 1000
    risk_free_rate: float = 0.02


@dataclass
class StressTestConfig:
    """Stress Test configuration."""

    enabled: bool = False
    top_k: int = 5
    failure_threshold: float = 0.7
    sort_metric: str = "profit_retention"
    warmup_bars: int = 1000


@dataclass
class FTResult:
    """Forward test result for a single trial."""

    trial_number: int
    source_rank: int
    params: dict

    is_net_profit_pct: float
    is_max_drawdown_pct: float
    is_total_trades: int
    is_win_rate: float
    is_max_consecutive_losses: int
    is_sharpe_ratio: Optional[float]
    is_romad: Optional[float]
    is_profit_factor: Optional[float]

    ft_net_profit_pct: float
    ft_max_drawdown_pct: float
    ft_total_trades: int
    ft_win_rate: float
    ft_max_consecutive_losses: int
    ft_sharpe_ratio: Optional[float]
    ft_sortino_ratio: Optional[float]
    ft_romad: Optional[float]
    ft_profit_factor: Optional[float]
    ft_ulcer_index: Optional[float]
    ft_sqn: Optional[float]
    ft_consistency_score: Optional[float]
    ft_consistency_segments_used: Optional[int]

    profit_degradation: float
    max_dd_change: float
    romad_change: float
    sharpe_change: float
    pf_change: float

    ft_rank: Optional[int] = None
    rank_change: Optional[int] = None


@dataclass
class DSRResult:
    """DSR analysis result for a single trial."""

    trial_number: int
    optuna_rank: int
    params: dict
    original_result: Any

    dsr_probability: Optional[float]
    dsr_rank: Optional[int] = None
    dsr_skewness: Optional[float] = None
    dsr_kurtosis: Optional[float] = None
    dsr_track_length: Optional[int] = None
    dsr_luck_share_pct: Optional[float] = None


class StressTestStatus(Enum):
    """Status of Stress Test for a candidate."""

    OK = "ok"
    SKIPPED_BAD_BASE = "skipped_bad_base"
    SKIPPED_NO_PARAMS = "skipped_no_params"
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass
class RetentionMetrics:
    """
    Retention metrics from perturbation results.

    "Retention ratio" terminology (not "stability score" or "confidence"):
    - ratio = 1.0 means neighbor equals base
    - ratio < 1.0 means neighbor degraded
    - ratio > 1.0 means neighbor improved (possible, not clipped)
    """

    status: StressTestStatus

    profit_retention: Optional[float]
    profit_lower_tail: Optional[float]
    profit_median: Optional[float]
    profit_worst: Optional[float]

    romad_retention: Optional[float]
    romad_lower_tail: Optional[float]
    romad_median: Optional[float]
    romad_worst: Optional[float]

    profit_failure_rate: Optional[float]
    romad_failure_rate: Optional[float]
    combined_failure_rate: float

    profit_failure_count: int
    romad_failure_count: int
    combined_failure_count: int
    total_perturbations: int
    failure_threshold: float

    param_worst_ratios: dict


@dataclass
class StressTestResult:
    """
    Result of Stress Test for a single candidate.

    Uses "retention" terminology instead of "stability".
    Retention values can exceed 1.0 (no clipping).
    Includes status field for bad base handling.
    """

    trial_number: int
    source_rank: int
    status: str

    base_net_profit_pct: float
    base_max_drawdown_pct: float
    base_romad: Optional[float]
    base_sharpe_ratio: Optional[float]

    profit_retention: Optional[float]
    romad_retention: Optional[float]

    profit_worst: Optional[float]
    profit_lower_tail: Optional[float]
    profit_median: Optional[float]

    romad_worst: Optional[float]
    romad_lower_tail: Optional[float]
    romad_median: Optional[float]

    profit_failure_rate: Optional[float]
    romad_failure_rate: Optional[float]
    combined_failure_rate: float
    profit_failure_count: int
    romad_failure_count: int
    combined_failure_count: int
    total_perturbations: int
    failure_threshold: float

    param_worst_ratios: dict
    most_sensitive_param: Optional[str]

    st_rank: Optional[int] = None
    rank_change: Optional[int] = None


EULER_GAMMA = 0.5772156649015329
MIN_NEIGHBORS = 4



# ============================================================
# Timestamp Handling (Timezone-Aware)
# ============================================================


# ============================================================
# Worker Function (module-level for multiprocessing)
# ============================================================


def _ft_worker_entry(
    csv_path: str,
    strategy_id: str,
    task_dict: Dict[str, Any],
    ft_start_date: str,
    ft_end_date: str,
    warmup_bars: int,
    is_period_days: int,
    ft_period_days: int,
    consistency_segments_is: int,
) -> Optional[Dict[str, Any]]:
    """
    Entry point for FT worker process.

    Follows optuna_engine pattern: load data and strategy inside worker.
    """
    from .backtest_engine import align_date_bounds, load_data
    from . import metrics
    from strategies import get_strategy

    worker_logger = logging.getLogger(__name__)
    trial_number = task_dict["trial_number"]

    try:
        df = load_data(csv_path)
        strategy_class = get_strategy(strategy_id)

        ft_start, ft_end = align_date_bounds(df.index, ft_start_date, ft_end_date)

        if ft_start is None or ft_end is None:
            raise ValueError(f"Invalid FT dates: start={ft_start_date}, end={ft_end_date}")

        ft_start_idx = df.index.get_indexer([ft_start], method="bfill")[0]
        if ft_start_idx < 0 or ft_start_idx >= len(df):
            raise ValueError(
                f"FT start date {ft_start_date} not found in data range "
                f"{df.index.min()} to {df.index.max()}"
            )

        warmup_start_idx = max(0, ft_start_idx - warmup_bars)
        df_ft_with_warmup = df.iloc[warmup_start_idx:]
        df_ft_with_warmup = df_ft_with_warmup[df_ft_with_warmup.index <= ft_end]

        if len(df_ft_with_warmup) == 0:
            raise ValueError(f"No data in FT period {ft_start_date} to {ft_end_date}")

        trade_start_idx = ft_start_idx - warmup_start_idx

        params = task_dict["params"]
        result = strategy_class.run(df_ft_with_warmup, params, trade_start_idx)

        basic = metrics.calculate_basic(result, 100.0)
        ft_consistency_segments = metrics.derive_auto_consistency_segments(
            is_period_days,
            consistency_segments_is,
            ft_period_days,
        )
        advanced = metrics.calculate_advanced(
            result,
            100.0,
            consistency_segments=ft_consistency_segments,
        )

        ft_metrics = {
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
            "consistency_segments_used": ft_consistency_segments,
        }

        is_metrics = task_dict["is_metrics"]
        comparison = calculate_comparison_metrics(
            is_metrics, ft_metrics, is_period_days, ft_period_days
        )

        return {
            "trial_number": trial_number,
            "source_rank": task_dict["source_rank"],
            "params": params,
            "is_metrics": is_metrics,
            "ft_metrics": ft_metrics,
            "comparison": comparison,
        }

    except Exception as exc:
        worker_logger.warning("FT failed for trial %s: %s", trial_number, exc)
        return None


# ============================================================
# Core Functions
# ============================================================


def calculate_ft_dates(
    user_start: pd.Timestamp,
    user_end: pd.Timestamp,
    ft_period_days: int,
) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, int, int]:
    """
    Calculate IS/FT date boundaries within USER-SELECTED range.

    Returns:
        (is_end, ft_start, ft_end, is_days, ft_days)
    """
    total_days = (user_end - user_start).days

    if ft_period_days >= total_days:
        raise ValueError(
            f"FT period ({ft_period_days} days) must be less than "
            f"user-selected range ({total_days} days). "
            f"User range: {user_start.date()} to {user_end.date()}"
        )

    ft_start = user_end - pd.Timedelta(days=ft_period_days)
    is_end = ft_start
    is_days = (is_end - user_start).days
    ft_days = ft_period_days

    return is_end, ft_start, user_end, is_days, ft_days


def calculate_period_dates(
    user_start: pd.Timestamp,
    user_end: pd.Timestamp,
    *,
    ft_enabled: bool = False,
    ft_period_days: Optional[int] = None,
    oos_enabled: bool = False,
    oos_period_days: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Calculate IS/FT/OOS boundaries with inclusive ends and shared boundary bars.

    Returns dict with:
        is_end, ft_start, ft_end, oos_start, oos_end, is_days, ft_days, oos_days
    """
    if user_start is None or user_end is None:
        raise ValueError("User start/end dates are required for period splitting.")

    total_days = (user_end - user_start).days
    if total_days <= 0:
        raise ValueError("User-selected range must be at least 1 day.")

    ft_start = None
    ft_end = None
    oos_start = None
    oos_end = None
    ft_days = None
    oos_days = None

    if oos_enabled:
        if oos_period_days is None:
            raise ValueError("OOS period days is required when OOS test is enabled.")
        oos_days = int(oos_period_days)
        if oos_days >= total_days:
            raise ValueError(
                f"OOS period ({oos_days} days) must be less than "
                f"user-selected range ({total_days} days). "
                f"User range: {user_start.date()} to {user_end.date()}"
            )
        oos_start = user_end - pd.Timedelta(days=oos_days)
        oos_end = user_end

    if ft_enabled:
        if ft_period_days is None:
            raise ValueError("FT period days is required when Forward Test is enabled.")
        ft_days = int(ft_period_days)
        remaining_end = oos_start if oos_enabled else user_end
        remaining_days = (remaining_end - user_start).days
        if ft_days >= remaining_days:
            raise ValueError(
                f"FT period ({ft_days} days) must be less than "
                f"remaining range ({remaining_days} days). "
                f"User range: {user_start.date()} to {user_end.date()}"
            )
        ft_end = remaining_end
        ft_start = ft_end - pd.Timedelta(days=ft_days)

    if ft_enabled:
        is_end = ft_start
    elif oos_enabled:
        is_end = oos_start
    else:
        is_end = user_end

    is_days = (is_end - user_start).days
    if is_days <= 0:
        raise ValueError("In-sample period must be at least 1 day.")

    return {
        "is_end": is_end,
        "ft_start": ft_start,
        "ft_end": ft_end,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "is_days": is_days,
        "ft_days": ft_days,
        "oos_days": oos_days,
    }


def calculate_profit_degradation(
    is_profit: float,
    ft_profit: float,
    is_period_days: int,
    ft_period_days: int,
) -> float:
    """
    Calculate annualized profit degradation ratio.

    Returns ratio where 1.0 = no degradation, <1.0 = worse in FT.
    """
    if is_period_days <= 0 or ft_period_days <= 0:
        return 0.0

    is_annual = is_profit * (365 / is_period_days)
    ft_annual = ft_profit * (365 / ft_period_days)

    if is_annual <= 0:
        return 0.0

    return ft_annual / is_annual


def calculate_comparison_metrics(
    is_metrics: Dict[str, Any],
    ft_metrics: Dict[str, Any],
    is_period_days: int,
    ft_period_days: int,
) -> Dict[str, Any]:
    """
    Calculate comparison between IS and FT metrics.
    """
    profit_deg = calculate_profit_degradation(
        is_metrics.get("net_profit_pct", 0),
        ft_metrics.get("net_profit_pct", 0),
        is_period_days,
        ft_period_days,
    )

    return {
        "profit_degradation": profit_deg,
        "max_dd_change": (ft_metrics.get("max_drawdown_pct") or 0)
        - (is_metrics.get("max_drawdown_pct") or 0),
        "romad_change": (ft_metrics.get("romad") or 0) - (is_metrics.get("romad") or 0),
        "sharpe_change": (ft_metrics.get("sharpe_ratio") or 0)
        - (is_metrics.get("sharpe_ratio") or 0),
        "pf_change": (ft_metrics.get("profit_factor") or 0)
        - (is_metrics.get("profit_factor") or 0),
    }


def calculate_expected_max_sharpe(
    mu: float,
    var_sr: float,
    n_trials: int,
) -> Optional[float]:
    """
    Expected maximum Sharpe ratio under null hypothesis.

    SR0 = mu + sqrt(var_sr) * ((1 - gamma) * norm.ppf(1 - 1/N)
                               + gamma * norm.ppf(1 - 1/(N * e)))
    """
    if n_trials is None or n_trials <= 1:
        return None
    if var_sr is None or var_sr < 0 or not math.isfinite(var_sr):
        return None

    try:
        from scipy.stats import norm
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("SciPy not available for DSR calculation: %s", exc)
        return None

    try:
        p1 = 1.0 - (1.0 / float(n_trials))
        p2 = 1.0 - (1.0 / (float(n_trials) * math.e))
        if p1 <= 0.0 or p2 <= 0.0:
            return None
        term = ((1.0 - EULER_GAMMA) * norm.ppf(p1)) + (EULER_GAMMA * norm.ppf(p2))
        return float(mu + math.sqrt(var_sr) * term)
    except Exception:
        return None


def calculate_dsr(
    sr: float,
    sr0: float,
    skew: float,
    kurtosis: float,
    track_length: int,
) -> Optional[float]:
    """
    Calculate Deflated Sharpe Ratio (probability SR exceeds SR0).
    """
    if track_length is None or track_length < 3:
        return None
    if sr is None or sr0 is None:
        return None
    if skew is None or kurtosis is None:
        return None

    try:
        from scipy.stats import norm
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("SciPy not available for DSR calculation: %s", exc)
        return None

    try:
        denom = 1.0 - (skew * sr) + (((kurtosis - 1.0) / 4.0) * (sr**2))
        if denom <= 0.0:
            return None
        z = ((sr - sr0) * math.sqrt(track_length - 1.0)) / math.sqrt(denom)
        value = float(norm.cdf(z))
    except Exception:
        return None

    if not math.isfinite(value):
        return None
    return min(1.0, max(0.0, value))


def calculate_luck_share(sr: float, sr0: float) -> Optional[float]:
    if sr is None or sr0 is None:
        return None
    if sr <= 0:
        return None
    try:
        value = (float(sr0) / float(sr)) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if not math.isfinite(value):
        return None
    return value


def calculate_is_period_days(config_json: Dict[str, Any]) -> Optional[int]:
    """Calculate IS period days from stored config_json.fixed_params."""
    if not isinstance(config_json, dict):
        return None
    fixed = config_json.get("fixed_params") or {}
    from .backtest_engine import parse_timestamp_utc

    start = parse_timestamp_utc(fixed.get("start"))
    end = parse_timestamp_utc(fixed.get("end"))
    if start is None or end is None:
        return None
    return max(0, (end - start).days)


def _build_is_metrics(result: Any) -> Dict[str, Any]:
    return {
        "net_profit_pct": getattr(result, "net_profit_pct", 0.0),
        "max_drawdown_pct": getattr(result, "max_drawdown_pct", 0.0),
        "total_trades": getattr(result, "total_trades", 0),
        "win_rate": getattr(result, "win_rate", 0.0),
        "max_consecutive_losses": getattr(result, "max_consecutive_losses", 0),
        "sharpe_ratio": getattr(result, "sharpe_ratio", None),
        "romad": getattr(result, "romad", None),
        "profit_factor": getattr(result, "profit_factor", None),
    }


def _filter_dsr_candidates(
    results: Sequence[Any],
    *,
    filter_min_profit: bool,
    min_profit_threshold: float,
    score_config: Optional[Dict[str, Any]],
) -> List[Any]:
    candidates = list(results or [])
    if not candidates:
        return []

    if score_config and score_config.get("filter_enabled"):
        try:
            threshold = float(score_config.get("min_score_threshold", 0.0))
        except (TypeError, ValueError):
            threshold = 0.0
        candidates = [r for r in candidates if float(getattr(r, "score", 0.0)) >= threshold]

    if filter_min_profit:
        try:
            threshold = float(min_profit_threshold)
        except (TypeError, ValueError):
            threshold = 0.0
        candidates = [r for r in candidates if float(getattr(r, "net_profit_pct", 0.0)) >= threshold]

    return candidates


def run_dsr_analysis(
    *,
    optuna_results: Sequence[Any],
    all_results: Optional[Sequence[Any]] = None,
    config: DSRConfig,
    n_trials_total: Optional[int],
    csv_path: Optional[str],
    strategy_id: str,
    fixed_params: Optional[Dict[str, Any]],
    warmup_bars: Optional[int],
    score_config: Optional[Dict[str, Any]] = None,
    filter_min_profit: bool = False,
    min_profit_threshold: float = 0.0,
    df: Optional[pd.DataFrame] = None,
) -> Tuple[List[DSRResult], Dict[str, Any]]:
    """
    Run DSR analysis for top-K candidates.
    """
    results = list(optuna_results or [])
    if not results or not config.enabled:
        return [], {}

    sharpe_source = list(all_results) if all_results is not None else results
    sharpe_values = []
    for item in sharpe_source:
        value = getattr(item, "sharpe_ratio", None)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric):
            sharpe_values.append(numeric)

    mean_sharpe = float(np.mean(sharpe_values)) if sharpe_values else None
    var_sharpe = float(np.var(sharpe_values, ddof=0)) if sharpe_values else None

    trials_total = int(n_trials_total) if n_trials_total else len(results)
    if trials_total <= 0:
        trials_total = len(results)

    sr0 = None
    if var_sharpe is not None:
        sr0 = calculate_expected_max_sharpe(0.0, var_sharpe, trials_total)

    candidates = _filter_dsr_candidates(
        results,
        filter_min_profit=filter_min_profit,
        min_profit_threshold=min_profit_threshold,
        score_config=score_config,
    )
    if not candidates:
        return [], {
            "dsr_n_trials": trials_total,
            "dsr_mean_sharpe": mean_sharpe,
            "dsr_var_sharpe": var_sharpe,
        }

    top_k = max(1, int(config.top_k or 1))
    top_k = min(top_k, len(candidates))

    if df is None:
        if not csv_path:
            raise ValueError("CSV path is required for DSR analysis.")
        from .backtest_engine import load_data

        df = load_data(csv_path)

    from strategies import get_strategy
    from .backtest_engine import align_date_bounds, prepare_dataset_with_warmup
    from . import metrics

    strategy_class = get_strategy(strategy_id)

    fixed = dict(fixed_params or {})
    date_filter = bool(fixed.get("dateFilter"))
    if date_filter:
        start, end = align_date_bounds(df.index, fixed.get("start"), fixed.get("end"))
    else:
        start, end = None, None

    if date_filter:
        if start is None:
            start = df.index.min()
        if end is None:
            end = df.index.max()

    analysis_results: List[DSRResult] = []
    for idx, optuna_result in enumerate(candidates[:top_k], 1):
        params = {**fixed, **(getattr(optuna_result, "params", {}) or {})}
        if date_filter:
            params["dateFilter"] = True
            params["start"] = start
            params["end"] = end

        dsr_prob = None
        skewness = None
        kurtosis = None
        track_length = None
        luck_share = None

        try:
            df_prepared, trade_start_idx = prepare_dataset_with_warmup(
                df, start, end, int(warmup_bars or config.warmup_bars)
            )
            if not df_prepared.empty:
                result = strategy_class.run(df_prepared, params, trade_start_idx)
                timestamps = getattr(result, "timestamps", None) or []
                equity_curve = getattr(result, "equity_curve", None) or []
                time_index = pd.DatetimeIndex(timestamps) if timestamps else None
                if time_index is not None and equity_curve:
                    monthly_returns = metrics._calculate_monthly_returns(
                        equity_curve, time_index
                    )
                else:
                    monthly_returns = []
                track_length = len(monthly_returns)
                if track_length >= 3:
                    rfr_monthly = (float(config.risk_free_rate) * 100.0) / 12.0
                    excess_returns = [ret - rfr_monthly for ret in monthly_returns]
                    skewness, kurtosis = metrics.calculate_higher_moments_from_monthly_returns(
                        excess_returns
                    )
                    sr_value = getattr(optuna_result, "sharpe_ratio", None)
                    if sr_value is not None and skewness is not None and kurtosis is not None and sr0 is not None:
                        try:
                            sr_value = float(sr_value)
                        except (TypeError, ValueError):
                            sr_value = None
                    if sr_value is not None:
                        dsr_prob = calculate_dsr(
                            sr_value,
                            sr0,
                            skewness,
                            kurtosis,
                            track_length,
                        )
                        luck_share = calculate_luck_share(sr_value, sr0)
        except Exception as exc:
            logger.warning("DSR re-run failed for trial %s: %s", idx, exc)

        trial_number = getattr(optuna_result, "optuna_trial_number", None)
        if trial_number is None:
            trial_number = idx
        analysis_results.append(
            DSRResult(
                trial_number=int(trial_number),
                optuna_rank=idx,
                params=dict(getattr(optuna_result, "params", {}) or {}),
                original_result=optuna_result,
                dsr_probability=dsr_prob,
                dsr_skewness=skewness,
                dsr_kurtosis=kurtosis,
                dsr_track_length=track_length,
                dsr_luck_share_pct=luck_share,
            )
        )

    def _dsr_sort_key(item: DSRResult) -> tuple:
        prob = item.dsr_probability
        prob_key = prob if prob is not None else float("-inf")
        return (prob is None, -prob_key, item.optuna_rank)

    analysis_results.sort(key=_dsr_sort_key)
    for rank, item in enumerate(analysis_results, 1):
        item.dsr_rank = rank

    summary = {
        "dsr_n_trials": trials_total,
        "dsr_mean_sharpe": mean_sharpe,
        "dsr_var_sharpe": var_sharpe,
    }

    return analysis_results, summary


def run_forward_test(
    *,
    csv_path: str,
    strategy_id: str,
    optuna_results: Sequence[Any],
    config: PostProcessConfig,
    is_period_days: int,
    ft_period_days: int,
    ft_start_date: str,
    ft_end_date: str,
    n_workers: int,
    consistency_segments: int = 4,
) -> List[FTResult]:
    """
    Run forward test for top-K optuna results.
    """
    from . import metrics

    if not config.enabled:
        return []

    candidates = list(optuna_results or [])
    if not candidates:
        return []

    top_k = max(1, int(config.top_k or 1))
    top_k = min(top_k, len(candidates))
    normalized_consistency_segments = metrics.normalize_consistency_segments(
        consistency_segments
    )

    tasks: List[Dict[str, Any]] = []
    for idx, result in enumerate(candidates[:top_k], 1):
        trial_number = getattr(result, "optuna_trial_number", None) or idx
        tasks.append(
            {
                "trial_number": int(trial_number),
                "source_rank": idx,
                "params": dict(getattr(result, "params", {}) or {}),
                "is_metrics": _build_is_metrics(result),
            }
        )

    max_workers = max(1, min(int(n_workers or 1), len(tasks)))
    ctx = mp.get_context("spawn")
    results: List[FTResult] = []

    with ctx.Pool(processes=max_workers) as pool:
        worker_args = [
            (
                csv_path,
                strategy_id,
                task,
                ft_start_date,
                ft_end_date,
                int(config.warmup_bars),
                int(is_period_days),
                int(ft_period_days),
                int(normalized_consistency_segments),
            )
            for task in tasks
        ]
        for payload in pool.starmap(_ft_worker_entry, worker_args):
            if not payload:
                continue
            is_metrics = payload["is_metrics"]
            ft_metrics = payload["ft_metrics"]
            comparison = payload["comparison"]
            results.append(
                FTResult(
                    trial_number=int(payload["trial_number"]),
                    source_rank=int(payload["source_rank"]),
                    params=payload["params"],
                    is_net_profit_pct=is_metrics.get("net_profit_pct", 0.0),
                    is_max_drawdown_pct=is_metrics.get("max_drawdown_pct", 0.0),
                    is_total_trades=is_metrics.get("total_trades", 0),
                    is_win_rate=is_metrics.get("win_rate", 0.0),
                    is_max_consecutive_losses=is_metrics.get("max_consecutive_losses", 0),
                    is_sharpe_ratio=is_metrics.get("sharpe_ratio"),
                    is_romad=is_metrics.get("romad"),
                    is_profit_factor=is_metrics.get("profit_factor"),
                    ft_net_profit_pct=ft_metrics.get("net_profit_pct", 0.0),
                    ft_max_drawdown_pct=ft_metrics.get("max_drawdown_pct", 0.0),
                    ft_total_trades=ft_metrics.get("total_trades", 0),
                    ft_win_rate=ft_metrics.get("win_rate", 0.0),
                    ft_max_consecutive_losses=ft_metrics.get("max_consecutive_losses", 0),
                    ft_sharpe_ratio=ft_metrics.get("sharpe_ratio"),
                    ft_sortino_ratio=ft_metrics.get("sortino_ratio"),
                    ft_romad=ft_metrics.get("romad"),
                    ft_profit_factor=ft_metrics.get("profit_factor"),
                    ft_ulcer_index=ft_metrics.get("ulcer_index"),
                    ft_sqn=ft_metrics.get("sqn"),
                    ft_consistency_score=ft_metrics.get("consistency_score"),
                    ft_consistency_segments_used=ft_metrics.get("consistency_segments_used"),
                    profit_degradation=comparison.get("profit_degradation", 0.0),
                    max_dd_change=comparison.get("max_dd_change", 0.0),
                    romad_change=comparison.get("romad_change", 0.0),
                    sharpe_change=comparison.get("sharpe_change", 0.0),
                    pf_change=comparison.get("pf_change", 0.0),
                )
            )

    if not results:
        return []

    sort_metric = (config.sort_metric or "profit_degradation").strip().lower()
    if sort_metric == "ft_romad":
        results.sort(
            key=lambda r: float(r.ft_romad) if r.ft_romad is not None else float("-inf"),
            reverse=True,
        )
    else:
        results.sort(key=lambda r: float(r.profit_degradation), reverse=True)

    for idx, result in enumerate(results, 1):
        result.ft_rank = idx
        result.rank_change = result.source_rank - idx

    return results


# ============================================================
# Stress Test
# ============================================================


def generate_perturbations(base_params: dict, config_json: dict) -> List[dict]:
    """
    Generate OAT perturbations for a parameter set.

    Uses ±1 step from config.json optimize.step for each numeric parameter.
    """
    if not isinstance(config_json, dict):
        return []
    parameters = config_json.get("parameters") or {}
    if not isinstance(parameters, dict):
        return []

    perturbations: List[dict] = []

    for param_name, param_config in parameters.items():
        if not isinstance(param_config, dict):
            continue
        param_type = str(param_config.get("type", "")).lower()

        if param_type in {"select", "options", "bool", "boolean"}:
            continue

        optimize_cfg = param_config.get("optimize", {}) if isinstance(param_config.get("optimize"), dict) else {}
        if not optimize_cfg.get("enabled", False):
            continue

        if param_name not in base_params:
            continue

        base_value = base_params.get(param_name)
        try:
            numeric_base = float(base_value)
        except (TypeError, ValueError):
            continue

        step = optimize_cfg.get("step", param_config.get("step", 1))
        try:
            step_value = float(step)
        except (TypeError, ValueError):
            continue
        if step_value == 0:
            continue

        min_val = optimize_cfg.get("min", param_config.get("min"))
        max_val = optimize_cfg.get("max", param_config.get("max"))
        min_num = None
        max_num = None
        try:
            if min_val is not None:
                min_num = float(min_val)
        except (TypeError, ValueError):
            min_num = None
        try:
            if max_val is not None:
                max_num = float(max_val)
        except (TypeError, ValueError):
            max_num = None

        for direction in (-1, 1):
            perturbed_value = numeric_base + (direction * step_value)
            if param_type == "int":
                perturbed_value = int(round(perturbed_value))

            if min_num is not None and perturbed_value < min_num:
                continue
            if max_num is not None and perturbed_value > max_num:
                continue

            perturbed_params = dict(base_params)
            perturbed_params[param_name] = perturbed_value

            perturbations.append(
                {
                    "params": perturbed_params,
                    "perturbed_param": param_name,
                    "direction": int(direction),
                    "base_value": base_value,
                    "perturbed_value": perturbed_value,
                }
            )

    return perturbations


def _run_is_backtest(
    csv_path: str,
    strategy_id: str,
    params: Dict[str, Any],
    is_start_date: Optional[str],
    is_end_date: Optional[str],
    warmup_bars: int,
) -> Optional[Dict[str, Any]]:
    from .backtest_engine import align_date_bounds, load_data, prepare_dataset_with_warmup
    from . import metrics
    from strategies import get_strategy

    try:
        df = load_data(csv_path)
        strategy_class = get_strategy(strategy_id)

        start_ts, end_ts = align_date_bounds(df.index, is_start_date, is_end_date)

        if start_ts is not None or end_ts is not None:
            df_prepared, trade_start_idx = prepare_dataset_with_warmup(
                df, start_ts, end_ts, int(warmup_bars)
            )
        else:
            df_prepared = df
            trade_start_idx = 0

        if df_prepared.empty:
            raise ValueError("No data available for stress test period.")

        result = strategy_class.run(df_prepared, params, trade_start_idx)

        basic = metrics.calculate_basic(result, 100.0)
        advanced = metrics.calculate_advanced(result, 100.0)

        return {
            "net_profit_pct": basic.net_profit_pct,
            "max_drawdown_pct": basic.max_drawdown_pct,
            "total_trades": basic.total_trades,
            "win_rate": basic.win_rate,
            "sharpe_ratio": advanced.sharpe_ratio,
            "romad": advanced.romad,
            "profit_factor": advanced.profit_factor,
        }
    except Exception as exc:
        logger.warning("Stress test backtest failed: %s", exc)
        return None


def _perturbation_worker(
    csv_path: str,
    strategy_id: str,
    params: Dict[str, Any],
    start_date: Optional[str],
    end_date: Optional[str],
    fixed_params: Dict[str, Any],
    warmup_bars: int,
    perturbed_param: str,
    direction: int,
    base_value: Any,
    perturbed_value: Any,
) -> Optional[Dict[str, Any]]:
    try:
        full_params = dict(fixed_params or {})
        full_params.update(params or {})
        metrics_payload = _run_is_backtest(
            csv_path=csv_path,
            strategy_id=strategy_id,
            params=full_params,
            is_start_date=start_date,
            is_end_date=end_date,
            warmup_bars=warmup_bars,
        )
        if metrics_payload is None:
            return None

        metrics_payload.update(
            {
                "perturbed_param": perturbed_param,
                "direction": direction,
                "base_value": base_value,
                "perturbed_value": perturbed_value,
            }
        )
        return metrics_payload
    except Exception as exc:
        logger.warning("Perturbation worker failed: %s", exc)
        return None


def run_perturbations_parallel(
    csv_path: str,
    strategy_id: str,
    perturbations: List[dict],
    is_start_date: Optional[str],
    is_end_date: Optional[str],
    fixed_params: dict,
    warmup_bars: int,
    n_workers: int,
) -> List[dict]:
    if not perturbations:
        return []

    max_workers = max(1, min(int(n_workers or 1), len(perturbations)))
    ctx = mp.get_context("spawn")
    worker_args = [
        (
            csv_path,
            strategy_id,
            p.get("params", {}),
            is_start_date,
            is_end_date,
            fixed_params,
            warmup_bars,
            p.get("perturbed_param"),
            p.get("direction"),
            p.get("base_value"),
            p.get("perturbed_value"),
        )
        for p in perturbations
    ]

    results: List[dict] = []
    with ctx.Pool(processes=max_workers) as pool:
        for payload in pool.starmap(_perturbation_worker, worker_args):
            if payload:
                results.append(payload)

    return results


def calculate_retention_metrics(
    base_metrics: dict,
    perturbation_results: List[dict],
    failure_threshold: float = 0.7,
    total_perturbations_generated: int = 0,
) -> RetentionMetrics:
    n = len(perturbation_results)
    n_generated = total_perturbations_generated if total_perturbations_generated > 0 else n

    if n == 0:
        return RetentionMetrics(
            status=StressTestStatus.INSUFFICIENT_DATA,
            profit_retention=None,
            profit_lower_tail=None,
            profit_median=None,
            profit_worst=None,
            romad_retention=None,
            romad_lower_tail=None,
            romad_median=None,
            romad_worst=None,
            profit_failure_rate=1.0,
            romad_failure_rate=None,
            combined_failure_rate=1.0,
            profit_failure_count=n_generated,
            romad_failure_count=0,
            combined_failure_count=n_generated,
            total_perturbations=n_generated,
            failure_threshold=failure_threshold,
            param_worst_ratios={},
        )

    base_profit = base_metrics.get("net_profit_pct", 0)
    base_romad = base_metrics.get("romad")

    if base_profit <= 0:
        return RetentionMetrics(
            status=StressTestStatus.SKIPPED_BAD_BASE,
            profit_retention=None,
            profit_lower_tail=None,
            profit_median=None,
            profit_worst=None,
            romad_retention=None,
            romad_lower_tail=None,
            romad_median=None,
            romad_worst=None,
            profit_failure_rate=None,
            romad_failure_rate=None,
            combined_failure_rate=1.0,
            profit_failure_count=n_generated,
            romad_failure_count=0,
            combined_failure_count=n_generated,
            total_perturbations=n_generated,
            failure_threshold=failure_threshold,
            param_worst_ratios={},
        )

    romad_valid = base_romad is not None and math.isfinite(base_romad) and base_romad > 0

    profit_ratios: List[float] = []
    romad_ratios: List[float] = []
    param_profit_ratios: Dict[str, List[float]] = {}

    for result in perturbation_results:
        neighbor_profit = result.get("net_profit_pct")
        if neighbor_profit is None:
            continue
        profit_ratio = float(neighbor_profit) / float(base_profit)
        profit_ratios.append(profit_ratio)

        param_name = result.get("perturbed_param", "unknown")
        param_profit_ratios.setdefault(param_name, []).append(profit_ratio)

        if romad_valid:
            neighbor_romad = result.get("romad")
            if neighbor_romad is not None and math.isfinite(neighbor_romad):
                romad_ratio = float(neighbor_romad) / float(base_romad)
            else:
                romad_ratio = 0.0
            romad_ratios.append(romad_ratio)

    n_valid = len(profit_ratios)
    status = StressTestStatus.OK if n_valid >= MIN_NEIGHBORS else StressTestStatus.INSUFFICIENT_DATA

    if n_valid == 0:
        return RetentionMetrics(
            status=StressTestStatus.INSUFFICIENT_DATA,
            profit_retention=None,
            profit_lower_tail=None,
            profit_median=None,
            profit_worst=None,
            romad_retention=None,
            romad_lower_tail=None,
            romad_median=None,
            romad_worst=None,
            profit_failure_rate=1.0,
            romad_failure_rate=None,
            combined_failure_rate=1.0,
            profit_failure_count=n_generated,
            romad_failure_count=0,
            combined_failure_count=n_generated,
            total_perturbations=n_generated,
            failure_threshold=failure_threshold,
            param_worst_ratios={},
        )

    profit_ratios_arr = np.array(profit_ratios)
    profit_lower_tail = float(np.quantile(profit_ratios_arr, 0.05, method="linear"))
    profit_median = float(np.quantile(profit_ratios_arr, 0.50, method="linear"))
    profit_worst = float(np.min(profit_ratios_arr))
    profit_retention = 0.5 * profit_lower_tail + 0.5 * profit_median

    if romad_valid and romad_ratios:
        romad_ratios_arr = np.array(romad_ratios)
        romad_lower_tail = float(np.quantile(romad_ratios_arr, 0.05, method="linear"))
        romad_median = float(np.quantile(romad_ratios_arr, 0.50, method="linear"))
        romad_worst = float(np.min(romad_ratios_arr))
        romad_retention = 0.5 * romad_lower_tail + 0.5 * romad_median
    else:
        romad_lower_tail = None
        romad_median = None
        romad_worst = None
        romad_retention = None

    profit_failures = int(np.sum(profit_ratios_arr < failure_threshold))
    profit_failure_rate = profit_failures / n_valid

    if romad_valid and romad_ratios:
        romad_ratios_arr = np.array(romad_ratios)
        romad_failures = int(np.sum(romad_ratios_arr < failure_threshold))
        romad_failure_rate = romad_failures / len(romad_ratios_arr)

        combined_failures = 0
        for profit_ratio, romad_ratio in zip(profit_ratios_arr, romad_ratios_arr):
            if profit_ratio < failure_threshold or romad_ratio < failure_threshold:
                combined_failures += 1
        combined_failure_rate = combined_failures / n_valid
    else:
        romad_failures = 0
        romad_failure_rate = None
        combined_failures = profit_failures
        combined_failure_rate = profit_failure_rate

    param_worst_ratios = {
        param: float(min(ratios))
        for param, ratios in param_profit_ratios.items()
        if ratios
    }

    return RetentionMetrics(
        status=status,
        profit_retention=round(profit_retention, 4),
        profit_lower_tail=round(profit_lower_tail, 4),
        profit_median=round(profit_median, 4),
        profit_worst=round(profit_worst, 4),
        romad_retention=round(romad_retention, 4) if romad_retention is not None else None,
        romad_lower_tail=round(romad_lower_tail, 4) if romad_lower_tail is not None else None,
        romad_median=round(romad_median, 4) if romad_median is not None else None,
        romad_worst=round(romad_worst, 4) if romad_worst is not None else None,
        profit_failure_rate=round(profit_failure_rate, 4),
        romad_failure_rate=round(romad_failure_rate, 4) if romad_failure_rate is not None else None,
        combined_failure_rate=round(combined_failure_rate, 4),
        profit_failure_count=profit_failures,
        romad_failure_count=romad_failures,
        combined_failure_count=combined_failures,
        total_perturbations=n_generated,
        failure_threshold=failure_threshold,
        param_worst_ratios=param_worst_ratios,
    )


def _extract_candidate_params(candidate: Any) -> Dict[str, Any]:
    if candidate is None:
        return {}
    if isinstance(candidate, dict):
        params = candidate.get("params") or {}
        if isinstance(params, dict):
            return dict(params)
        return {}
    if hasattr(candidate, "params"):
        params = getattr(candidate, "params") or {}
        if isinstance(params, dict):
            return dict(params)
    if hasattr(candidate, "original_result"):
        original = getattr(candidate, "original_result")
        if hasattr(original, "params"):
            params = getattr(original, "params") or {}
            if isinstance(params, dict):
                return dict(params)
    return {}


def _get_trial_number(candidate: Any, fallback: int) -> int:
    for attr in ("trial_number", "optuna_trial_number"):
        if hasattr(candidate, attr):
            value = getattr(candidate, attr)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass
    if isinstance(candidate, dict):
        for key in ("trial_number", "optuna_trial_number"):
            value = candidate.get(key)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass
    return fallback


def _merge_stress_params(
    candidate_params: Dict[str, Any],
    fixed_params: Optional[Dict[str, Any]],
    is_start_date: Optional[str],
    is_end_date: Optional[str],
) -> Dict[str, Any]:
    merged = dict(fixed_params or {})
    merged.update(candidate_params or {})
    if is_start_date:
        merged["start"] = is_start_date
        merged["dateFilter"] = True
    if is_end_date:
        merged["end"] = is_end_date
        merged["dateFilter"] = True
    return merged


def run_stress_test(
    csv_path: str,
    strategy_id: str,
    source_results: List[Any],
    config: StressTestConfig,
    is_start_date: Optional[str],
    is_end_date: Optional[str],
    fixed_params: dict,
    config_json: dict,
    n_workers: int = 6,
) -> Tuple[List[StressTestResult], dict]:
    if not config.enabled:
        return [], {}

    candidates = list(source_results or [])
    if not candidates:
        return [], {}

    top_k = min(int(config.top_k or 1), len(candidates))
    candidates = candidates[:top_k]

    results: List[StressTestResult] = []

    for source_rank, candidate in enumerate(candidates, 1):
        candidate_params = _extract_candidate_params(candidate)
        params = _merge_stress_params(candidate_params, fixed_params, is_start_date, is_end_date)

        base_metrics = _run_is_backtest(
            csv_path=csv_path,
            strategy_id=strategy_id,
            params=params,
            is_start_date=is_start_date,
            is_end_date=is_end_date,
            warmup_bars=int(config.warmup_bars),
        )

        trial_number = _get_trial_number(candidate, source_rank)

        if base_metrics is None or base_metrics.get("net_profit_pct", 0) <= 0:
            results.append(
                StressTestResult(
                    trial_number=trial_number,
                    source_rank=source_rank,
                    status=StressTestStatus.SKIPPED_BAD_BASE.value,
                    base_net_profit_pct=(base_metrics or {}).get("net_profit_pct", 0.0),
                    base_max_drawdown_pct=(base_metrics or {}).get("max_drawdown_pct", 0.0),
                    base_romad=(base_metrics or {}).get("romad"),
                    base_sharpe_ratio=(base_metrics or {}).get("sharpe_ratio"),
                    profit_retention=None,
                    romad_retention=None,
                    profit_worst=None,
                    profit_lower_tail=None,
                    profit_median=None,
                    romad_worst=None,
                    romad_lower_tail=None,
                    romad_median=None,
                    profit_failure_rate=None,
                    romad_failure_rate=None,
                    combined_failure_rate=1.0,
                    profit_failure_count=0,
                    romad_failure_count=0,
                    combined_failure_count=0,
                    total_perturbations=0,
                    failure_threshold=config.failure_threshold,
                    param_worst_ratios={},
                    most_sensitive_param=None,
                )
            )
            continue

        perturbations = generate_perturbations(params, config_json)
        if not perturbations:
            results.append(
                StressTestResult(
                    trial_number=trial_number,
                    source_rank=source_rank,
                    status=StressTestStatus.SKIPPED_NO_PARAMS.value,
                    base_net_profit_pct=base_metrics["net_profit_pct"],
                    base_max_drawdown_pct=base_metrics["max_drawdown_pct"],
                    base_romad=base_metrics.get("romad"),
                    base_sharpe_ratio=base_metrics.get("sharpe_ratio"),
                    profit_retention=None,
                    romad_retention=None,
                    profit_worst=None,
                    profit_lower_tail=None,
                    profit_median=None,
                    romad_worst=None,
                    romad_lower_tail=None,
                    romad_median=None,
                    profit_failure_rate=None,
                    romad_failure_rate=None,
                    combined_failure_rate=1.0,
                    profit_failure_count=0,
                    romad_failure_count=0,
                    combined_failure_count=0,
                    total_perturbations=0,
                    failure_threshold=config.failure_threshold,
                    param_worst_ratios={},
                    most_sensitive_param=None,
                )
            )
            continue

        perturbation_results = run_perturbations_parallel(
            csv_path=csv_path,
            strategy_id=strategy_id,
            perturbations=perturbations,
            is_start_date=is_start_date,
            is_end_date=is_end_date,
            fixed_params=fixed_params,
            warmup_bars=int(config.warmup_bars),
            n_workers=int(n_workers or 1),
        )

        metrics = calculate_retention_metrics(
            base_metrics=base_metrics,
            perturbation_results=perturbation_results,
            failure_threshold=config.failure_threshold,
            total_perturbations_generated=len(perturbations),
        )

        most_sensitive = None
        if metrics.param_worst_ratios:
            most_sensitive = min(metrics.param_worst_ratios, key=metrics.param_worst_ratios.get)

        results.append(
            StressTestResult(
                trial_number=trial_number,
                source_rank=source_rank,
                status=metrics.status.value,
                base_net_profit_pct=base_metrics["net_profit_pct"],
                base_max_drawdown_pct=base_metrics["max_drawdown_pct"],
                base_romad=base_metrics.get("romad"),
                base_sharpe_ratio=base_metrics.get("sharpe_ratio"),
                profit_retention=metrics.profit_retention,
                romad_retention=metrics.romad_retention,
                profit_worst=metrics.profit_worst,
                profit_lower_tail=metrics.profit_lower_tail,
                profit_median=metrics.profit_median,
                romad_worst=metrics.romad_worst,
                romad_lower_tail=metrics.romad_lower_tail,
                romad_median=metrics.romad_median,
                profit_failure_rate=metrics.profit_failure_rate,
                romad_failure_rate=metrics.romad_failure_rate,
                combined_failure_rate=metrics.combined_failure_rate,
                profit_failure_count=metrics.profit_failure_count,
                romad_failure_count=metrics.romad_failure_count,
                combined_failure_count=metrics.combined_failure_count,
                total_perturbations=metrics.total_perturbations,
                failure_threshold=metrics.failure_threshold,
                param_worst_ratios=metrics.param_worst_ratios,
                most_sensitive_param=most_sensitive,
            )
        )

    sort_metric = (config.sort_metric or "profit_retention").strip().lower()
    if sort_metric == "romad_retention":
        results.sort(
            key=lambda r: (r.romad_retention is not None, r.romad_retention or 0),
            reverse=True,
        )
    else:
        results.sort(
            key=lambda r: (r.profit_retention is not None, r.profit_retention or 0),
            reverse=True,
        )

    for idx, result in enumerate(results, 1):
        result.st_rank = idx
        result.rank_change = result.source_rank - idx

    valid_results = [r for r in results if r.profit_retention is not None]
    profit_retention_vals = [r.profit_retention for r in valid_results if r.profit_retention is not None]
    avg_profit_retention = (
        sum(profit_retention_vals) / len(profit_retention_vals)
        if profit_retention_vals
        else None
    )

    romad_retention_vals = [r.romad_retention for r in valid_results if r.romad_retention is not None]
    avg_romad_retention = (
        sum(romad_retention_vals) / len(romad_retention_vals)
        if romad_retention_vals
        else None
    )

    combined_rates = [r.combined_failure_rate for r in valid_results if r.combined_failure_rate is not None]
    avg_combined_failure_rate = (
        sum(combined_rates) / len(combined_rates)
        if combined_rates
        else None
    )

    summary = {
        "avg_profit_retention": round(avg_profit_retention, 4) if avg_profit_retention is not None else None,
        "avg_romad_retention": round(avg_romad_retention, 4) if avg_romad_retention is not None else None,
        "avg_combined_failure_rate": round(avg_combined_failure_rate, 4)
        if avg_combined_failure_rate is not None
        else None,
        "total_perturbations_run": sum(r.total_perturbations for r in results),
        "candidates_skipped_bad_base": sum(
            1 for r in results if r.status == StressTestStatus.SKIPPED_BAD_BASE.value
        ),
        "candidates_skipped_no_params": sum(
            1 for r in results if r.status == StressTestStatus.SKIPPED_NO_PARAMS.value
        ),
        "candidates_insufficient_data": sum(
            1 for r in results if r.status == StressTestStatus.INSUFFICIENT_DATA.value
        ),
        "failure_threshold": config.failure_threshold,
    }

    return results, summary
