"""
Walk-Forward Analysis Engine - Rolling WFA (Phase 2)
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from copy import deepcopy
import hashlib
import io
import json
import logging
import time

import pandas as pd

from . import metrics
from .backtest_engine import prepare_dataset_with_warmup
from .optuna_engine import OptunaConfig, OptimizationConfig, SamplerConfig, run_optuna_optimization
from .storage import save_wfa_study_to_db
from .post_process import (
    DSRConfig,
    PostProcessConfig,
    StressTestConfig,
    run_dsr_analysis,
    run_forward_test,
    run_stress_test,
)

logger = logging.getLogger(__name__)


@dataclass
class WFConfig:
    """Walk-Forward Analysis Configuration"""

    # Window sizing (calendar-based)
    is_period_days: int = 180
    oos_period_days: int = 60

    # Strategy and warmup
    strategy_id: str = ""
    warmup_bars: int = 1000
    post_process: Optional[PostProcessConfig] = None
    dsr_config: Optional[DSRConfig] = None
    stress_test_config: Optional[StressTestConfig] = None
    store_top_n_trials: int = 50

    # Adaptive mode
    adaptive_mode: bool = False
    max_oos_period_days: int = 90
    min_oos_trades: int = 5
    check_interval_trades: int = 3
    cusum_threshold: float = 5.0
    dd_threshold_multiplier: float = 1.5
    inactivity_multiplier: float = 5.0
    cooldown_enabled: bool = False
    cooldown_days: int = 15


@dataclass
class WindowSplit:
    """One IS/OOS window with timestamps"""

    window_id: int
    is_start: pd.Timestamp
    is_end: pd.Timestamp
    oos_start: pd.Timestamp
    oos_end: pd.Timestamp


@dataclass
class WindowResult:
    """Results from one WFA window"""

    window_id: int

    # Window boundaries
    is_start: pd.Timestamp
    is_end: pd.Timestamp
    oos_start: pd.Timestamp
    oos_end: pd.Timestamp

    # Best parameter set
    best_params: Dict[str, Any]
    param_id: str

    # Performance metrics
    is_net_profit_pct: float
    is_max_drawdown_pct: float
    is_total_trades: int

    oos_net_profit_pct: float
    oos_max_drawdown_pct: float
    oos_total_trades: int

    # OOS equity curve for stitching
    oos_equity_curve: List[float]
    oos_timestamps: List[pd.Timestamp]

    # Optional OOS trade decomposition
    oos_winning_trades: Optional[int] = None

    # Optional IS details
    is_best_trial_number: Optional[int] = None
    is_equity_curve: Optional[List[float]] = None

    # P/C badges for selected trial
    is_pareto_optimal: Optional[bool] = None
    constraints_satisfied: Optional[bool] = None

    # Source of selected params
    best_params_source: str = "optuna_is"

    # Available modules for this window
    available_modules: List[str] = field(default_factory=list)

    # Intermediate results for storage
    optuna_is_trials: Optional[List[Dict[str, Any]]] = None
    dsr_trials: Optional[List[Dict[str, Any]]] = None
    forward_test_trials: Optional[List[Dict[str, Any]]] = None
    stress_test_trials: Optional[List[Dict[str, Any]]] = None

    # Module status and selection chain
    module_status: Optional[Dict[str, Any]] = None
    selection_chain: Optional[Dict[str, Any]] = None

    # Optimization/FT slice dates for equity/trade generation
    optimization_start: Optional[pd.Timestamp] = None
    optimization_end: Optional[pd.Timestamp] = None
    ft_start: Optional[pd.Timestamp] = None
    ft_end: Optional[pd.Timestamp] = None

    # Optional timestamps for IS equity curves
    is_timestamps: Optional[List[pd.Timestamp]] = None

    # Additional IS metrics
    is_win_rate: Optional[float] = None
    is_max_consecutive_losses: Optional[int] = None
    is_romad: Optional[float] = None
    is_sharpe_ratio: Optional[float] = None
    is_profit_factor: Optional[float] = None
    is_sqn: Optional[float] = None
    is_ulcer_index: Optional[float] = None
    is_consistency_score: Optional[float] = None
    is_composite_score: Optional[float] = None

    # Additional OOS metrics
    oos_win_rate: Optional[float] = None
    oos_max_consecutive_losses: Optional[int] = None
    oos_romad: Optional[float] = None
    oos_sharpe_ratio: Optional[float] = None
    oos_profit_factor: Optional[float] = None
    oos_sqn: Optional[float] = None
    oos_ulcer_index: Optional[float] = None
    oos_consistency_score: Optional[float] = None

    # Adaptive WFA metadata (None for fixed mode)
    trigger_type: Optional[str] = None
    cusum_final: Optional[float] = None
    cusum_threshold: Optional[float] = None
    dd_threshold: Optional[float] = None
    oos_actual_days: Optional[float] = None
    cooldown_days_applied: Optional[float] = None
    oos_elapsed_days: Optional[float] = None


@dataclass
class StitchWindow:
    window_id: int
    oos_equity_curve: List[float]
    oos_timestamps: List[pd.Timestamp]
    oos_total_trades: int
    oos_start: pd.Timestamp


@dataclass
class TriggerResult:
    """Result of adaptive trigger scan for one OOS segment."""

    triggered: bool
    trigger_type: str
    trigger_trade_idx: Optional[int]
    trigger_time: Optional[pd.Timestamp]
    cusum_final: float
    cusum_threshold: float
    dd_peak: float
    dd_threshold: float
    oos_actual_trades: int
    oos_actual_days: float


@dataclass
class ISPipelineResult:
    """Selected IS candidate and module-chain artifacts for one window."""

    best_result: Any
    best_params: Dict[str, Any]
    param_id: str
    best_trial_number: Optional[int]
    best_params_source: str
    is_pareto_optimal: Optional[bool]
    constraints_satisfied: Optional[bool]
    available_modules: List[str]
    module_status: Dict[str, Any]
    selection_chain: Dict[str, Any]
    optimization_start: pd.Timestamp
    optimization_end: pd.Timestamp
    ft_start: Optional[pd.Timestamp]
    ft_end: Optional[pd.Timestamp]
    optuna_is_trials: Optional[List[Dict[str, Any]]]
    dsr_trials: Optional[List[Dict[str, Any]]]
    forward_test_trials: Optional[List[Dict[str, Any]]]
    stress_test_trials: Optional[List[Dict[str, Any]]]


@dataclass
class OOSStitchedResult:
    """Stitched out-of-sample equity curve and summary"""

    final_net_profit_pct: float
    max_drawdown_pct: float
    total_trades: int
    wfe: float
    oos_win_rate: float

    equity_curve: List[float]
    timestamps: List[pd.Timestamp]
    window_ids: List[int]


@dataclass
class WFResult:
    """Complete Walk-Forward Analysis results"""

    config: WFConfig
    windows: List[WindowResult]
    stitched_oos: OOSStitchedResult

    strategy_id: str
    total_windows: int
    trading_start_date: pd.Timestamp
    trading_end_date: pd.Timestamp
    warmup_bars: int


class WalkForwardEngine:
    """Main engine for Walk-Forward Analysis"""

    def __init__(
        self,
        config: WFConfig,
        base_config_template: Dict[str, Any],
        optuna_settings: Dict[str, Any],
        csv_file_path: Optional[str] = None,
    ):
        self.config = config
        self.base_config_template = deepcopy(base_config_template)
        self.optuna_settings = deepcopy(optuna_settings)
        self.csv_file_path = csv_file_path

        from strategies import get_strategy

        try:
            self.strategy_class = get_strategy(config.strategy_id)
        except ValueError as e:  # noqa: BLE001
            raise ValueError(f"Failed to load strategy '{config.strategy_id}': {e}")

    def split_data(
        self,
        df: pd.DataFrame,
        trading_start: pd.Timestamp,
        trading_end: pd.Timestamp,
    ) -> List[WindowSplit]:
        """
        Create rolling walk-forward windows using calendar-based periods.

        All window boundaries are aligned to 00:00 day start to match TradingView behavior.
        """
        if self.config.is_period_days <= 0 or self.config.oos_period_days <= 0:
            raise ValueError("IS and OOS periods must be positive")

        if df.empty:
            raise ValueError("Input dataframe is empty.")

        trading_start_normalized = trading_start.normalize()
        trading_end_normalized = trading_end.normalize() + pd.Timedelta(days=1)

        start_idx = df.index.searchsorted(trading_start_normalized, side="left")
        if start_idx >= len(df):
            raise ValueError(
                f"Normalized trading start {trading_start_normalized.date()} "
                f"is beyond available data range"
            )

        trading_start_aligned = df.index[start_idx]
        if trading_start_aligned.time() != pd.Timestamp("00:00:00").time():
            print(
                "Warning: First trading bar is at "
                f"{trading_start_aligned.time()}, not 00:00. "
                "Window alignment may not match TradingView exactly."
            )

        trading_days = (trading_end_normalized - trading_start_aligned).days
        min_required_days = self.config.is_period_days + self.config.oos_period_days
        if trading_days < min_required_days:
            raise ValueError(
                "Insufficient data for WFA. Need at least "
                f"{min_required_days} days (IS={self.config.is_period_days}d + "
                f"OOS={self.config.oos_period_days}d), but trading period is only "
                f"{trading_days} days."
            )

        max_possible_windows = (trading_days - self.config.is_period_days) // self.config.oos_period_days
        if max_possible_windows < 2:
            raise ValueError(
                f"Configuration produces only {max_possible_windows} window(s). "
                "WFA requires at least 2 windows for meaningful results. "
                "Reduce IS/OOS period lengths or provide more data."
            )

        print("Creating walk-forward windows:")
        print(f"  IS Period: {self.config.is_period_days} days")
        print(f"  OOS Period: {self.config.oos_period_days} days")
        print(f"  Trading Start (aligned to 00:00): {trading_start_aligned}")
        print(f"  Trading End (normalized): {trading_end_normalized.date()}")
        print(f"  Trading Days: {trading_days}")
        print(f"  Maximum Windows: {max_possible_windows}")

        windows: List[WindowSplit] = []
        window_id = 1
        current_start = trading_start_aligned

        while True:
            is_start_target = current_start
            is_end_target = is_start_target + pd.Timedelta(days=self.config.is_period_days)
            oos_start_target = is_end_target
            oos_end_target = oos_start_target + pd.Timedelta(days=self.config.oos_period_days)

            if oos_end_target > trading_end_normalized:
                print(
                    "  Stopping: Window "
                    f"{window_id} OOS end ({oos_end_target.date()}) "
                    f"exceeds trading end ({trading_end.date()})"
                )
                break

            is_start_idx = df.index.searchsorted(is_start_target, side="left")
            is_end_idx = df.index.searchsorted(is_end_target, side="left")
            oos_start_idx = df.index.searchsorted(oos_start_target, side="left")
            oos_end_idx = df.index.searchsorted(oos_end_target, side="left")

            if is_end_idx > 0 and is_end_idx <= len(df):
                is_end_idx -= 1
            if oos_end_idx > 0 and oos_end_idx <= len(df):
                oos_end_idx -= 1

            if (
                is_start_idx >= len(df)
                or is_end_idx >= len(df)
                or oos_start_idx >= len(df)
                or oos_end_idx >= len(df)
            ):
                print(f"  Stopping: Window {window_id} indices exceed dataframe bounds")
                break

            is_bar_count = is_end_idx - is_start_idx + 1
            oos_bar_count = oos_end_idx - oos_start_idx + 1
            min_bars = 100

            if is_bar_count < min_bars:
                print(
                    f"  Warning: Window {window_id} IS has only {is_bar_count} bars "
                    f"(recommended minimum: {min_bars})"
                )
            if oos_bar_count < min_bars:
                print(
                    f"  Warning: Window {window_id} OOS has only {oos_bar_count} bars "
                    f"(recommended minimum: {min_bars})"
                )

            is_start_aligned = df.index[is_start_idx]
            is_end_aligned = df.index[is_end_idx]
            oos_start_aligned = df.index[oos_start_idx]
            oos_end_aligned = df.index[oos_end_idx]

            windows.append(
                WindowSplit(
                    window_id=window_id,
                    is_start=is_start_aligned,
                    is_end=is_end_aligned,
                    oos_start=oos_start_aligned,
                    oos_end=oos_end_aligned,
                )
            )

            print(f"  Window {window_id}:")
            print(f"    IS:  {is_start_aligned} to {is_end_aligned} ({is_bar_count} bars)")
            print(f"    OOS: {oos_start_aligned} to {oos_end_aligned} ({oos_bar_count} bars)")

            # Shift forward by OOS period length (rolling window)
            next_start_target = current_start + pd.Timedelta(days=self.config.oos_period_days)
            next_start_normalized = next_start_target.normalize()
            next_start_idx = df.index.searchsorted(next_start_normalized, side="left")

            if next_start_idx >= len(df):
                print("  Stopping: Next window start would exceed dataframe bounds")
                break

            current_start = df.index[next_start_idx]
            window_id += 1

        if not windows:
            raise ValueError("Failed to create any walk-forward windows with current configuration")

        print(f"Created {len(windows)} windows successfully")
        return windows

    def run_wf_optimization(self, df: pd.DataFrame) -> tuple[WFResult, Optional[str]]:
        """
        Run complete Walk-Forward Analysis.

        Steps:
        1. Split data into rolling windows
        2. Optimize IS per window, test OOS per window
        3. Stitch OOS equity and compute summary metrics
        4. Return results
        """
        if self.config.adaptive_mode:
            return self._run_adaptive_wfa(df)

        print("Starting Walk-Forward Analysis...")
        start_time = time.time()

        fixed_params = self.base_config_template.get("fixed_params", {})
        use_date_filter = fixed_params.get("dateFilter", False)
        start_date = fixed_params.get("start")
        end_date = fixed_params.get("end")

        if use_date_filter and start_date is not None and end_date is not None:
            from .backtest_engine import align_date_bounds

            trading_start, trading_end = align_date_bounds(df.index, start_date, end_date)
            if trading_start is None or trading_end is None:
                raise ValueError("Invalid date filter range for walk-forward.")
        else:
            trading_start = df.index[0]
            trading_end = df.index[-1]

        if trading_start.tzinfo is None:
            trading_start = trading_start.tz_localize("UTC")
        if trading_end.tzinfo is None:
            trading_end = trading_end.tz_localize("UTC")

        windows = self.split_data(df, trading_start, trading_end)

        window_results: List[WindowResult] = []
        dense_stitch_windows: List[StitchWindow] = []
        compact_stitch_windows: List[StitchWindow] = []

        for window in windows:
            print(f"\n--- Window {window.window_id}/{len(windows)} ---")

            print(
                "IS optimization: dates "
                f"{window.is_start.date()} to {window.is_end.date()}"
            )

            available_modules = ["optuna_is"]
            module_status: Dict[str, Any] = {
                "optuna_is": {"enabled": True, "ran": True, "reason": None}
            }
            selection_chain: Dict[str, Any] = {}

            ft_config = self.config.post_process
            optimization_start = window.is_start
            optimization_end = window.is_end
            training_end = None
            ft_start = None
            ft_end = None

            if ft_config and ft_config.enabled:
                available_modules.append("forward_test")
                module_status["forward_test"] = {
                    "enabled": True,
                    "ran": False,
                    "reason": None,
                }
                is_days = (window.is_end - window.is_start).days
                ft_days = int(ft_config.ft_period_days)
                if ft_days > 0 and ft_days < is_days and self.csv_file_path:
                    training_end = window.is_end - pd.Timedelta(days=ft_days)
                    if training_end > window.is_start:
                        optimization_end = training_end
                        ft_start = training_end
                        ft_end = window.is_end
                    else:
                        module_status["forward_test"]["reason"] = "insufficient_is_period"
                else:
                    module_status["forward_test"]["reason"] = "insufficient_is_period_or_missing_csv"
                    logger.warning(
                        "Skipping FT for window %s: insufficient IS period or missing CSV path.",
                        window.window_id,
                    )

            optimization_results, optimization_all_results = self._run_optuna_on_window(
                df, optimization_start, optimization_end
            )
            if not optimization_results:
                raise ValueError(f"No optimization results for window {window.window_id}.")

            best_result = optimization_results[0]
            best_params_source = "optuna_is"

            optuna_map: Dict[int, Any] = {}
            for result in (optimization_all_results or optimization_results):
                trial_num = getattr(result, "optuna_trial_number", None)
                if trial_num is not None:
                    optuna_map[int(trial_num)] = result

            optuna_is_trials = self._convert_optuna_results_for_storage(
                optimization_results, int(self.config.store_top_n_trials)
            )
            if optuna_is_trials:
                optuna_is_trials[0]["is_selected"] = True
                selection_chain["optuna_is"] = optuna_is_trials[0].get("trial_number")

            is_pareto_optimal = getattr(best_result, "is_pareto_optimal", None)
            constraints_satisfied = getattr(best_result, "constraints_satisfied", None)

            dsr_results = []
            dsr_trials = None
            dsr_config = self.config.dsr_config
            if dsr_config and dsr_config.enabled and optimization_results:
                available_modules.append("dsr")
                module_status["dsr"] = {"enabled": True, "ran": False, "reason": None}
                fixed_params = {
                    "dateFilter": True,
                    "start": optimization_start.isoformat(),
                    "end": optimization_end.isoformat(),
                }
                try:
                    dsr_results, _summary = run_dsr_analysis(
                        optuna_results=optimization_results,
                        all_results=optimization_all_results or optimization_results,
                        config=dsr_config,
                        n_trials_total=len(optimization_all_results or optimization_results),
                        csv_path=self.csv_file_path,
                        strategy_id=self.config.strategy_id,
                        fixed_params=fixed_params,
                        warmup_bars=self.config.warmup_bars,
                        score_config=deepcopy(self.base_config_template.get("score_config", {})),
                        filter_min_profit=bool(self.base_config_template.get("filter_min_profit")),
                        min_profit_threshold=float(
                            self.base_config_template.get("min_profit_threshold") or 0.0
                        ),
                        df=df,
                    )
                    module_status["dsr"]["ran"] = True
                except Exception as exc:
                    module_status["dsr"]["reason"] = str(exc)
                    logger.warning("DSR analysis failed for window %s: %s", window.window_id, exc)

                dsr_trials = self._convert_dsr_results_for_storage(
                    dsr_results, int(self.config.store_top_n_trials)
                )

                if dsr_trials:
                    dsr_trials[0]["is_selected"] = True
                    selection_chain["dsr"] = dsr_trials[0].get("trial_number")
                if dsr_results:
                    best_result = dsr_results[0].original_result
                    best_params_source = "dsr"
                    is_pareto_optimal = getattr(best_result, "is_pareto_optimal", None)
                    constraints_satisfied = getattr(best_result, "constraints_satisfied", None)

            ft_results = []
            ft_trials = None
            if ft_config and ft_config.enabled and training_end and best_result:
                worker_count = int(self.base_config_template.get("worker_processes", 1))
                ft_candidates = optimization_results
                if dsr_results:
                    ft_candidates = [item.original_result for item in dsr_results]
                ft_results = run_forward_test(
                    csv_path=self.csv_file_path,
                    strategy_id=self.config.strategy_id,
                    optuna_results=ft_candidates,
                    config=ft_config,
                    is_period_days=max(0, (training_end - window.is_start).days),
                    ft_period_days=int(ft_config.ft_period_days),
                    ft_start_date=training_end.strftime("%Y-%m-%d"),
                    ft_end_date=window.is_end.strftime("%Y-%m-%d"),
                    n_workers=worker_count,
                )
                module_status["forward_test"]["ran"] = True
                if ft_results:
                    best_result = ft_results[0]
                    best_params_source = "forward_test"
                    selection_chain["forward_test"] = ft_results[0].trial_number
                ft_trials = self._convert_ft_results_for_storage(
                    ft_results, int(self.config.store_top_n_trials), optuna_map
                )
                if ft_trials:
                    ft_trials[0]["is_selected"] = True

            st_results = []
            st_trials = None
            st_config = self.config.stress_test_config
            if st_config and st_config.enabled and best_result:
                available_modules.append("stress_test")
                module_status["stress_test"] = {"enabled": True, "ran": False, "reason": None}
                worker_count = int(self.base_config_template.get("worker_processes", 1))
                stress_start = optimization_start or window.is_start
                stress_end = optimization_end or window.is_end
                fixed_params = {
                    "dateFilter": True,
                    "start": stress_start.isoformat(),
                    "end": stress_end.isoformat(),
                }
                st_candidates: List[Any] = optimization_results
                if ft_results:
                    st_candidates = ft_results
                elif dsr_results:
                    st_candidates = dsr_results

                try:
                    from strategies import get_strategy_config

                    strategy_config_json = get_strategy_config(self.config.strategy_id)
                except Exception as exc:
                    strategy_config_json = {}
                    logger.warning("Failed to load strategy config for stress test: %s", exc)

                try:
                    st_results, _summary = run_stress_test(
                        csv_path=self.csv_file_path,
                        strategy_id=self.config.strategy_id,
                        source_results=st_candidates,
                        config=st_config,
                        is_start_date=stress_start.isoformat() if stress_start else None,
                        is_end_date=stress_end.isoformat() if stress_end else None,
                        fixed_params=fixed_params,
                        config_json=strategy_config_json,
                        n_workers=worker_count,
                    )
                    module_status["stress_test"]["ran"] = True
                except Exception as exc:
                    module_status["stress_test"]["reason"] = str(exc)
                    logger.warning("Stress test failed for window %s: %s", window.window_id, exc)

                candidate_map: Dict[int, Any] = {}
                trial_to_params: Dict[int, Dict[str, Any]] = {}
                for candidate in st_candidates:
                    trial_num = getattr(candidate, "trial_number", None)
                    if trial_num is None:
                        trial_num = getattr(candidate, "optuna_trial_number", None)
                    if trial_num is None:
                        continue
                    trial_num = int(trial_num)
                    candidate_map[trial_num] = candidate
                    trial_to_params[trial_num] = getattr(candidate, "params", {}) or {}

                if st_results:
                    selected = candidate_map.get(st_results[0].trial_number)
                    if selected is not None:
                        best_result = selected
                        best_params_source = "stress_test"
                        selection_chain["stress_test"] = st_results[0].trial_number

                st_trials = self._convert_st_results_for_storage(
                    st_results, int(self.config.store_top_n_trials), trial_to_params, optuna_map
                )
                if st_trials:
                    st_trials[0]["is_selected"] = True

            if best_params_source in {"forward_test", "stress_test"}:
                best_trial_number = getattr(best_result, "trial_number", None)
                optuna_result = optuna_map.get(best_trial_number)
                if optuna_result:
                    is_pareto_optimal = getattr(optuna_result, "is_pareto_optimal", None)
                    constraints_satisfied = getattr(optuna_result, "constraints_satisfied", None)

            best_params = self._result_to_params(best_result)
            param_id = self._create_param_id(best_params)
            best_trial_number = getattr(best_result, "optuna_trial_number", None)
            if best_trial_number is None and hasattr(best_result, "trial_number"):
                best_trial_number = getattr(best_result, "trial_number")

            print(f"Best param ID: {param_id}")

            is_df_prepared, is_trade_start_idx = prepare_dataset_with_warmup(
                df, window.is_start, window.is_end, self.config.warmup_bars
            )

            is_params = best_params.copy()
            is_params["dateFilter"] = True
            is_params["start"] = window.is_start
            is_params["end"] = window.is_end

            is_result = self.strategy_class.run(
                is_df_prepared, is_params, is_trade_start_idx
            )

            is_basic = metrics.calculate_basic(is_result, initial_balance=100.0)
            is_adv = metrics.calculate_advanced(is_result, initial_balance=100.0)

            print(
                "OOS validation: dates "
                f"{window.oos_start.date()} to {window.oos_end.date()}"
            )

            oos_df_prepared, oos_trade_start_idx = prepare_dataset_with_warmup(
                df, window.oos_start, window.oos_end, self.config.warmup_bars
            )

            oos_params = best_params.copy()
            oos_params["dateFilter"] = True
            oos_params["start"] = window.oos_start
            oos_params["end"] = window.oos_end

            oos_result = self.strategy_class.run(
                oos_df_prepared, oos_params, oos_trade_start_idx
            )

            oos_basic = metrics.calculate_basic(oos_result, initial_balance=100.0)
            oos_adv = metrics.calculate_advanced(oos_result, initial_balance=100.0)

            if oos_basic.total_trades == 0:
                print(
                    "Warning: Window "
                    f"{window.window_id} produced no OOS trades. "
                    "This may indicate overfitting or unsuitable parameters."
                )

            dense_curve = list(oos_result.balance_curve or [])
            dense_timestamps = list(oos_result.timestamps or [])
            dense_stitch_windows.append(
                StitchWindow(
                    window_id=window.window_id,
                    oos_equity_curve=dense_curve,
                    oos_timestamps=dense_timestamps,
                    oos_total_trades=oos_basic.total_trades,
                    oos_start=window.oos_start,
                )
            )

            compact_equity, compact_timestamps = self._build_compact_oos_curve(
                oos_result, window
            )
            compact_stitch_windows.append(
                StitchWindow(
                    window_id=window.window_id,
                    oos_equity_curve=compact_equity,
                    oos_timestamps=compact_timestamps,
                    oos_total_trades=oos_basic.total_trades,
                    oos_start=window.oos_start,
                )
            )

            window_results.append(
                WindowResult(
                    window_id=window.window_id,
                    is_start=window.is_start,
                    is_end=window.is_end,
                    oos_start=window.oos_start,
                    oos_end=window.oos_end,
                    best_params=best_params,
                    param_id=param_id,
                    is_net_profit_pct=is_basic.net_profit_pct,
                    is_max_drawdown_pct=is_basic.max_drawdown_pct,
                    is_total_trades=is_basic.total_trades,
                    is_best_trial_number=best_trial_number,
                    is_equity_curve=None,
                    is_timestamps=None,
                    oos_net_profit_pct=oos_basic.net_profit_pct,
                    oos_max_drawdown_pct=oos_basic.max_drawdown_pct,
                    oos_total_trades=oos_basic.total_trades,
                    oos_equity_curve=[],
                    oos_timestamps=[],
                    oos_winning_trades=oos_basic.winning_trades,
                    is_pareto_optimal=is_pareto_optimal,
                    constraints_satisfied=constraints_satisfied,
                    best_params_source=best_params_source,
                    available_modules=available_modules,
                    optuna_is_trials=optuna_is_trials,
                    dsr_trials=dsr_trials,
                    forward_test_trials=ft_trials,
                    stress_test_trials=st_trials,
                    module_status=module_status,
                    selection_chain=selection_chain,
                    optimization_start=optimization_start,
                    optimization_end=optimization_end,
                    ft_start=ft_start,
                    ft_end=ft_end,
                    is_win_rate=is_basic.win_rate,
                    is_max_consecutive_losses=is_basic.max_consecutive_losses,
                    is_romad=is_adv.romad,
                    is_sharpe_ratio=is_adv.sharpe_ratio,
                    is_profit_factor=is_adv.profit_factor,
                    is_sqn=is_adv.sqn,
                    is_ulcer_index=is_adv.ulcer_index,
                    is_consistency_score=is_adv.consistency_score,
                    is_composite_score=getattr(best_result, "score", None),
                    oos_win_rate=oos_basic.win_rate,
                    oos_max_consecutive_losses=oos_basic.max_consecutive_losses,
                    oos_romad=oos_adv.romad,
                    oos_sharpe_ratio=oos_adv.sharpe_ratio,
                    oos_profit_factor=oos_adv.profit_factor,
                    oos_sqn=oos_adv.sqn,
                    oos_ulcer_index=oos_adv.ulcer_index,
                    oos_consistency_score=oos_adv.consistency_score,
                )
            )

        stitched_oos = self._build_stitched_oos_equity(
            window_results,
            dense_windows=dense_stitch_windows,
            compact_windows=compact_stitch_windows,
        )

        wf_result = WFResult(
            config=self.config,
            windows=window_results,
            stitched_oos=stitched_oos,
            strategy_id=self.config.strategy_id,
            total_windows=len(window_results),
            trading_start_date=trading_start,
            trading_end_date=trading_end,
            warmup_bars=self.config.warmup_bars,
        )

        study_id = None
        if self.csv_file_path:
            study_id = save_wfa_study_to_db(
                wf_result=wf_result,
                config=self.base_config_template,
                csv_file_path=self.csv_file_path,
                start_time=start_time,
                score_config=self.base_config_template.get("score_config")
                if isinstance(self.base_config_template, dict)
                else None,
            )

        return wf_result, study_id

    def _resolve_trading_bounds(self, df: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp]:
        fixed_params = self.base_config_template.get("fixed_params", {})
        use_date_filter = fixed_params.get("dateFilter", False)
        start_date = fixed_params.get("start")
        end_date = fixed_params.get("end")

        if use_date_filter and start_date is not None and end_date is not None:
            from .backtest_engine import align_date_bounds

            trading_start, trading_end = align_date_bounds(df.index, start_date, end_date)
            if trading_start is None or trading_end is None:
                raise ValueError("Invalid date filter range for walk-forward.")
        else:
            trading_start = df.index[0]
            trading_end = df.index[-1]

        if trading_start.tzinfo is None:
            trading_start = trading_start.tz_localize("UTC")
        if trading_end.tzinfo is None:
            trading_end = trading_end.tz_localize("UTC")

        return trading_start, trading_end

    def _run_period_backtest(
        self,
        df: pd.DataFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
        params: Dict[str, Any],
    ) -> Any:
        df_prepared, trade_start_idx = prepare_dataset_with_warmup(
            df, start, end, self.config.warmup_bars
        )

        run_params = params.copy()
        run_params["dateFilter"] = True
        run_params["start"] = start
        run_params["end"] = end
        return self.strategy_class.run(df_prepared, run_params, trade_start_idx)

    def _run_window_is_pipeline(
        self,
        df: pd.DataFrame,
        is_start: pd.Timestamp,
        is_end: pd.Timestamp,
        window_id: int,
    ) -> ISPipelineResult:
        available_modules = ["optuna_is"]
        module_status: Dict[str, Any] = {
            "optuna_is": {"enabled": True, "ran": True, "reason": None}
        }
        selection_chain: Dict[str, Any] = {}

        ft_config = self.config.post_process
        optimization_start = is_start
        optimization_end = is_end
        training_end = None
        ft_start = None
        ft_end = None

        if ft_config and ft_config.enabled:
            available_modules.append("forward_test")
            module_status["forward_test"] = {
                "enabled": True,
                "ran": False,
                "reason": None,
            }
            is_days = (is_end - is_start).days
            ft_days = int(ft_config.ft_period_days)
            if ft_days > 0 and ft_days < is_days and self.csv_file_path:
                training_end = is_end - pd.Timedelta(days=ft_days)
                if training_end > is_start:
                    optimization_end = training_end
                    ft_start = training_end
                    ft_end = is_end
                else:
                    module_status["forward_test"]["reason"] = "insufficient_is_period"
            else:
                module_status["forward_test"]["reason"] = "insufficient_is_period_or_missing_csv"
                logger.warning(
                    "Skipping FT for window %s: insufficient IS period or missing CSV path.",
                    window_id,
                )

        optimization_results, optimization_all_results = self._run_optuna_on_window(
            df, optimization_start, optimization_end
        )
        if not optimization_results:
            raise ValueError(f"No optimization results for window {window_id}.")

        best_result = optimization_results[0]
        best_params_source = "optuna_is"

        optuna_map: Dict[int, Any] = {}
        for result in (optimization_all_results or optimization_results):
            trial_num = getattr(result, "optuna_trial_number", None)
            if trial_num is not None:
                optuna_map[int(trial_num)] = result

        optuna_is_trials = self._convert_optuna_results_for_storage(
            optimization_results, int(self.config.store_top_n_trials)
        )
        if optuna_is_trials:
            optuna_is_trials[0]["is_selected"] = True
            selection_chain["optuna_is"] = optuna_is_trials[0].get("trial_number")

        is_pareto_optimal = getattr(best_result, "is_pareto_optimal", None)
        constraints_satisfied = getattr(best_result, "constraints_satisfied", None)

        dsr_results = []
        dsr_trials = None
        dsr_config = self.config.dsr_config
        if dsr_config and dsr_config.enabled and optimization_results:
            available_modules.append("dsr")
            module_status["dsr"] = {"enabled": True, "ran": False, "reason": None}
            fixed_params = {
                "dateFilter": True,
                "start": optimization_start.isoformat(),
                "end": optimization_end.isoformat(),
            }
            try:
                dsr_results, _summary = run_dsr_analysis(
                    optuna_results=optimization_results,
                    all_results=optimization_all_results or optimization_results,
                    config=dsr_config,
                    n_trials_total=len(optimization_all_results or optimization_results),
                    csv_path=self.csv_file_path,
                    strategy_id=self.config.strategy_id,
                    fixed_params=fixed_params,
                    warmup_bars=self.config.warmup_bars,
                    score_config=deepcopy(self.base_config_template.get("score_config", {})),
                    filter_min_profit=bool(self.base_config_template.get("filter_min_profit")),
                    min_profit_threshold=float(
                        self.base_config_template.get("min_profit_threshold") or 0.0
                    ),
                    df=df,
                )
                module_status["dsr"]["ran"] = True
            except Exception as exc:
                module_status["dsr"]["reason"] = str(exc)
                logger.warning("DSR analysis failed for window %s: %s", window_id, exc)

            dsr_trials = self._convert_dsr_results_for_storage(
                dsr_results, int(self.config.store_top_n_trials)
            )

            if dsr_trials:
                dsr_trials[0]["is_selected"] = True
                selection_chain["dsr"] = dsr_trials[0].get("trial_number")
            if dsr_results:
                best_result = dsr_results[0].original_result
                best_params_source = "dsr"
                is_pareto_optimal = getattr(best_result, "is_pareto_optimal", None)
                constraints_satisfied = getattr(best_result, "constraints_satisfied", None)

        ft_results = []
        ft_trials = None
        if ft_config and ft_config.enabled and training_end and best_result:
            worker_count = int(self.base_config_template.get("worker_processes", 1))
            ft_candidates = optimization_results
            if dsr_results:
                ft_candidates = [item.original_result for item in dsr_results]
            ft_results = run_forward_test(
                csv_path=self.csv_file_path,
                strategy_id=self.config.strategy_id,
                optuna_results=ft_candidates,
                config=ft_config,
                is_period_days=max(0, (training_end - is_start).days),
                ft_period_days=int(ft_config.ft_period_days),
                ft_start_date=training_end.strftime("%Y-%m-%d"),
                ft_end_date=is_end.strftime("%Y-%m-%d"),
                n_workers=worker_count,
            )
            module_status["forward_test"]["ran"] = True
            if ft_results:
                best_result = ft_results[0]
                best_params_source = "forward_test"
                selection_chain["forward_test"] = ft_results[0].trial_number
            ft_trials = self._convert_ft_results_for_storage(
                ft_results, int(self.config.store_top_n_trials), optuna_map
            )
            if ft_trials:
                ft_trials[0]["is_selected"] = True

        st_results = []
        st_trials = None
        st_config = self.config.stress_test_config
        if st_config and st_config.enabled and best_result:
            available_modules.append("stress_test")
            module_status["stress_test"] = {"enabled": True, "ran": False, "reason": None}
            worker_count = int(self.base_config_template.get("worker_processes", 1))
            stress_start = optimization_start or is_start
            stress_end = optimization_end or is_end
            fixed_params = {
                "dateFilter": True,
                "start": stress_start.isoformat(),
                "end": stress_end.isoformat(),
            }
            st_candidates: List[Any] = optimization_results
            if ft_results:
                st_candidates = ft_results
            elif dsr_results:
                st_candidates = dsr_results

            try:
                from strategies import get_strategy_config

                strategy_config_json = get_strategy_config(self.config.strategy_id)
            except Exception as exc:
                strategy_config_json = {}
                logger.warning("Failed to load strategy config for stress test: %s", exc)

            try:
                st_results, _summary = run_stress_test(
                    csv_path=self.csv_file_path,
                    strategy_id=self.config.strategy_id,
                    source_results=st_candidates,
                    config=st_config,
                    is_start_date=stress_start.isoformat() if stress_start else None,
                    is_end_date=stress_end.isoformat() if stress_end else None,
                    fixed_params=fixed_params,
                    config_json=strategy_config_json,
                    n_workers=worker_count,
                )
                module_status["stress_test"]["ran"] = True
            except Exception as exc:
                module_status["stress_test"]["reason"] = str(exc)
                logger.warning("Stress test failed for window %s: %s", window_id, exc)

            candidate_map: Dict[int, Any] = {}
            trial_to_params: Dict[int, Dict[str, Any]] = {}
            for candidate in st_candidates:
                trial_num = getattr(candidate, "trial_number", None)
                if trial_num is None:
                    trial_num = getattr(candidate, "optuna_trial_number", None)
                if trial_num is None:
                    continue
                trial_num = int(trial_num)
                candidate_map[trial_num] = candidate
                trial_to_params[trial_num] = getattr(candidate, "params", {}) or {}

            if st_results:
                selected = candidate_map.get(st_results[0].trial_number)
                if selected is not None:
                    best_result = selected
                    best_params_source = "stress_test"
                    selection_chain["stress_test"] = st_results[0].trial_number

            st_trials = self._convert_st_results_for_storage(
                st_results, int(self.config.store_top_n_trials), trial_to_params, optuna_map
            )
            if st_trials:
                st_trials[0]["is_selected"] = True

        if best_params_source in {"forward_test", "stress_test"}:
            best_trial_number = getattr(best_result, "trial_number", None)
            optuna_result = optuna_map.get(best_trial_number)
            if optuna_result:
                is_pareto_optimal = getattr(optuna_result, "is_pareto_optimal", None)
                constraints_satisfied = getattr(optuna_result, "constraints_satisfied", None)

        best_params = self._result_to_params(best_result)
        param_id = self._create_param_id(best_params)
        best_trial_number = getattr(best_result, "optuna_trial_number", None)
        if best_trial_number is None and hasattr(best_result, "trial_number"):
            best_trial_number = getattr(best_result, "trial_number")

        return ISPipelineResult(
            best_result=best_result,
            best_params=best_params,
            param_id=param_id,
            best_trial_number=best_trial_number,
            best_params_source=best_params_source,
            is_pareto_optimal=is_pareto_optimal,
            constraints_satisfied=constraints_satisfied,
            available_modules=available_modules,
            module_status=module_status,
            selection_chain=selection_chain,
            optimization_start=optimization_start,
            optimization_end=optimization_end,
            ft_start=ft_start,
            ft_end=ft_end,
            optuna_is_trials=optuna_is_trials,
            dsr_trials=dsr_trials,
            forward_test_trials=ft_trials,
            stress_test_trials=st_trials,
        )

    @staticmethod
    def _duration_days(start: pd.Timestamp, end: pd.Timestamp) -> float:
        return max(0.0, (end - start).total_seconds() / 86400.0)

    def _compute_is_baseline(self, is_result: Any, is_period_days: int) -> Dict[str, Any]:
        closed_trades = [
            trade
            for trade in list(getattr(is_result, "trades", None) or [])
            if getattr(trade, "exit_time", None) is not None
        ]
        closed_trades.sort(key=lambda trade: getattr(trade, "exit_time"))

        balance_curve = list(getattr(is_result, "balance_curve", None) or [])
        peak = balance_curve[0] if balance_curve else 100.0
        is_max_dd = 0.0
        for balance in balance_curve:
            if balance > peak:
                peak = balance
            if peak > 0:
                drawdown = (peak - balance) / peak * 100.0
                if drawdown > is_max_dd:
                    is_max_dd = drawdown

        baseline: Dict[str, Any] = {
            "mu": 0.0,
            "sigma": 0.0,
            "h": float(self.config.cusum_threshold),
            "is_max_dd": is_max_dd,
            "dd_limit": is_max_dd * float(self.config.dd_threshold_multiplier),
            "is_avg_trade_interval": None,
            "max_trade_interval": None,
            "cusum_enabled": False,
            "drawdown_enabled": False,
            "inactivity_enabled": False,
        }

        if not closed_trades:
            return baseline

        baseline["drawdown_enabled"] = True

        trade_profits = [
            float(getattr(trade, "profit_pct"))
            for trade in closed_trades
            if getattr(trade, "profit_pct", None) is not None
        ]
        if len(trade_profits) >= 2:
            baseline["mu"] = float(pd.Series(trade_profits).mean())
            baseline["sigma"] = float(pd.Series(trade_profits).std(ddof=0))
            baseline["cusum_enabled"] = baseline["sigma"] > 0.0
        elif len(trade_profits) == 1:
            baseline["mu"] = trade_profits[0]

        if len(closed_trades) == 1:
            avg_interval_days = float(is_period_days) / 2.0
            baseline["is_avg_trade_interval"] = avg_interval_days
            baseline["max_trade_interval"] = avg_interval_days
            baseline["inactivity_enabled"] = True
            return baseline

        intervals = []
        for idx in range(1, len(closed_trades)):
            prev_exit = getattr(closed_trades[idx - 1], "exit_time")
            curr_exit = getattr(closed_trades[idx], "exit_time")
            if prev_exit is None or curr_exit is None:
                continue
            intervals.append(self._duration_days(prev_exit, curr_exit))

        if intervals:
            avg_interval_days = float(pd.Series(intervals).mean())
            baseline["is_avg_trade_interval"] = avg_interval_days
            baseline["max_trade_interval"] = avg_interval_days * float(
                self.config.inactivity_multiplier
            )
            baseline["inactivity_enabled"] = True

        return baseline

    def _scan_triggers(
        self,
        trades: List[Any],
        balance_curve: List[float],
        timestamps: List[pd.Timestamp],
        baseline: Dict[str, Any],
        oos_start: pd.Timestamp,
        oos_max_end: pd.Timestamp,
    ) -> TriggerResult:
        closed_trades = [
            trade
            for trade in list(trades or [])
            if getattr(trade, "exit_time", None) is not None
        ]
        closed_trades.sort(key=lambda trade: getattr(trade, "exit_time"))

        h = float(baseline.get("h") or 0.0)
        dd_limit = float(baseline.get("dd_limit") or 0.0)
        mu = float(baseline.get("mu") or 0.0)
        sigma = float(baseline.get("sigma") or 0.0)
        max_trade_interval = baseline.get("max_trade_interval")
        cusum_enabled = bool(baseline.get("cusum_enabled"))
        drawdown_enabled = bool(baseline.get("drawdown_enabled"))
        inactivity_enabled = bool(baseline.get("inactivity_enabled"))

        if max_trade_interval is None:
            inactivity_enabled = False
        else:
            max_trade_interval = float(max_trade_interval)

        balance_points = list(balance_curve or [])
        time_points = list(timestamps or [])
        dd_by_index: List[float] = []
        if balance_points:
            peak = balance_points[0]
            for balance in balance_points:
                if balance > peak:
                    peak = balance
                if peak > 0:
                    dd_by_index.append((peak - balance) / peak * 100.0)
                else:
                    dd_by_index.append(0.0)

        def _drawdown_at(ts: pd.Timestamp) -> float:
            if not dd_by_index or not time_points:
                return 0.0
            time_index = pd.DatetimeIndex(time_points)
            idx = int(time_index.searchsorted(ts, side="right") - 1)
            if idx < 0:
                idx = 0
            if idx >= len(dd_by_index):
                idx = len(dd_by_index) - 1
            return float(dd_by_index[idx])

        def _inactivity_trigger_time(anchor: pd.Timestamp) -> pd.Timestamp:
            trigger_time = anchor + pd.Timedelta(days=max_trade_interval or 0.0)
            return min(trigger_time, oos_max_end)

        s = 0.0

        if not closed_trades:
            if inactivity_enabled and self._duration_days(oos_start, oos_max_end) > float(max_trade_interval):
                trigger_time = _inactivity_trigger_time(oos_start)
                return TriggerResult(
                    triggered=True,
                    trigger_type="inactivity",
                    trigger_trade_idx=None,
                    trigger_time=trigger_time,
                    cusum_final=s,
                    cusum_threshold=h,
                    dd_peak=0.0,
                    dd_threshold=dd_limit,
                    oos_actual_trades=0,
                    oos_actual_days=self._duration_days(oos_start, trigger_time),
                )

            return TriggerResult(
                triggered=False,
                trigger_type="max_period",
                trigger_trade_idx=None,
                trigger_time=oos_max_end,
                cusum_final=s,
                cusum_threshold=h,
                dd_peak=0.0,
                dd_threshold=dd_limit,
                oos_actual_trades=0,
                oos_actual_days=self._duration_days(oos_start, oos_max_end),
            )

        first_exit = getattr(closed_trades[0], "exit_time")
        if (
            inactivity_enabled
            and first_exit is not None
            and self._duration_days(oos_start, first_exit) > float(max_trade_interval)
        ):
            trigger_time = _inactivity_trigger_time(oos_start)
            return TriggerResult(
                triggered=True,
                trigger_type="inactivity",
                trigger_trade_idx=None,
                trigger_time=trigger_time,
                cusum_final=s,
                cusum_threshold=h,
                dd_peak=0.0,
                dd_threshold=dd_limit,
                oos_actual_trades=0,
                oos_actual_days=self._duration_days(oos_start, trigger_time),
            )

        min_oos_trades = max(1, int(self.config.min_oos_trades))
        check_interval = max(1, int(self.config.check_interval_trades))

        for idx, trade in enumerate(closed_trades):
            exit_time = getattr(trade, "exit_time")
            if exit_time is None:
                continue

            if idx > 0 and inactivity_enabled:
                prev_exit = getattr(closed_trades[idx - 1], "exit_time")
                if (
                    prev_exit is not None
                    and self._duration_days(prev_exit, exit_time) > float(max_trade_interval)
                ):
                    trigger_time = _inactivity_trigger_time(prev_exit)
                    return TriggerResult(
                        triggered=True,
                        trigger_type="inactivity",
                        trigger_trade_idx=idx - 1,
                        trigger_time=trigger_time,
                        cusum_final=s,
                        cusum_threshold=h,
                        dd_peak=_drawdown_at(prev_exit),
                        dd_threshold=dd_limit,
                        oos_actual_trades=idx,
                        oos_actual_days=self._duration_days(oos_start, trigger_time),
                    )

            current_dd = _drawdown_at(exit_time)
            if drawdown_enabled and current_dd > dd_limit:
                return TriggerResult(
                    triggered=True,
                    trigger_type="drawdown",
                    trigger_trade_idx=idx,
                    trigger_time=exit_time,
                    cusum_final=s,
                    cusum_threshold=h,
                    dd_peak=current_dd,
                    dd_threshold=dd_limit,
                    oos_actual_trades=idx + 1,
                    oos_actual_days=self._duration_days(oos_start, exit_time),
                )

            if cusum_enabled and sigma > 0.0:
                trade_profit = float(getattr(trade, "profit_pct") or 0.0)
                z = (trade_profit - mu) / sigma
                s = max(0.0, s - z)

                closed_count = idx + 1
                is_checkpoint = (
                    closed_count >= min_oos_trades
                    and (closed_count - min_oos_trades) % check_interval == 0
                )
                if is_checkpoint and s > h:
                    return TriggerResult(
                        triggered=True,
                        trigger_type="cusum",
                        trigger_trade_idx=idx,
                        trigger_time=exit_time,
                        cusum_final=s,
                        cusum_threshold=h,
                        dd_peak=current_dd,
                        dd_threshold=dd_limit,
                        oos_actual_trades=idx + 1,
                        oos_actual_days=self._duration_days(oos_start, exit_time),
                    )

        last_exit = getattr(closed_trades[-1], "exit_time")
        if (
            inactivity_enabled
            and last_exit is not None
            and self._duration_days(last_exit, oos_max_end) > float(max_trade_interval)
        ):
            trigger_time = _inactivity_trigger_time(last_exit)
            return TriggerResult(
                triggered=True,
                trigger_type="inactivity",
                trigger_trade_idx=len(closed_trades) - 1,
                trigger_time=trigger_time,
                cusum_final=s,
                cusum_threshold=h,
                dd_peak=_drawdown_at(last_exit),
                dd_threshold=dd_limit,
                oos_actual_trades=len(closed_trades),
                oos_actual_days=self._duration_days(oos_start, trigger_time),
            )

        return TriggerResult(
            triggered=False,
            trigger_type="max_period",
            trigger_trade_idx=len(closed_trades) - 1,
            trigger_time=oos_max_end,
            cusum_final=s,
            cusum_threshold=h,
            dd_peak=_drawdown_at(oos_max_end),
            dd_threshold=dd_limit,
            oos_actual_trades=len(closed_trades),
            oos_actual_days=self._duration_days(oos_start, oos_max_end),
        )

    def _truncate_oos_result(
        self,
        oos_result: Any,
        trigger_result: TriggerResult,
        oos_max_end: pd.Timestamp,
    ) -> Tuple[Any, pd.Timestamp]:
        trigger_time = trigger_result.trigger_time or oos_max_end

        closed_trades = [
            trade
            for trade in list(getattr(oos_result, "trades", None) or [])
            if getattr(trade, "exit_time", None) is not None
        ]
        closed_trades.sort(key=lambda trade: getattr(trade, "exit_time"))

        trade_idx = trigger_result.trigger_trade_idx
        if trade_idx is None or trade_idx < 0:
            truncated_trades = []
        else:
            max_idx = min(int(trade_idx), len(closed_trades) - 1)
            truncated_trades = closed_trades[: max_idx + 1]

        oos_timestamps = list(getattr(oos_result, "timestamps", None) or [])
        oos_balance = list(getattr(oos_result, "balance_curve", None) or [])
        oos_equity = list(getattr(oos_result, "equity_curve", None) or [])

        if not oos_timestamps:
            return (
                type(oos_result)(
                    trades=truncated_trades,
                    equity_curve=[],
                    balance_curve=[],
                    timestamps=[],
                ),
                trigger_time,
            )

        time_index = pd.DatetimeIndex(oos_timestamps)
        cutoff_idx = int(time_index.searchsorted(trigger_time, side="right") - 1)
        if cutoff_idx < 0:
            cutoff_idx = 0
        if cutoff_idx >= len(oos_timestamps):
            cutoff_idx = len(oos_timestamps) - 1

        truncated_timestamps = oos_timestamps[: cutoff_idx + 1]
        truncated_balance = oos_balance[: cutoff_idx + 1] if oos_balance else []
        truncated_equity = oos_equity[: cutoff_idx + 1] if oos_equity else []

        return (
            type(oos_result)(
                trades=truncated_trades,
                equity_curve=truncated_equity,
                balance_curve=truncated_balance,
                timestamps=truncated_timestamps,
            ),
            trigger_time,
        )

    def _resolve_adaptive_roll_end(
        self,
        trigger_result: TriggerResult,
        oos_actual_end: pd.Timestamp,
        trading_end: pd.Timestamp,
    ) -> Tuple[pd.Timestamp, Optional[float]]:
        cooldown_enabled = bool(getattr(self.config, "cooldown_enabled", False))
        cooldown_days = int(getattr(self.config, "cooldown_days", 0) or 0)
        cooldown_trigger = (
            trigger_result.triggered
            and trigger_result.trigger_type in {"cusum", "drawdown"}
            and cooldown_enabled
            and cooldown_days > 0
        )
        if not cooldown_trigger:
            return oos_actual_end, None

        cooldown_end = min(
            oos_actual_end + pd.Timedelta(days=cooldown_days),
            trading_end,
        )
        cooldown_days_applied = self._duration_days(oos_actual_end, cooldown_end)
        if cooldown_days_applied <= 0.0:
            return oos_actual_end, None

        return cooldown_end, cooldown_days_applied

    def _run_adaptive_wfa(self, df: pd.DataFrame) -> tuple[WFResult, Optional[str]]:
        print("Starting Walk-Forward Analysis (adaptive mode)...")
        start_time = time.time()

        if self.config.max_oos_period_days <= 0:
            raise ValueError("max_oos_period_days must be positive for adaptive WFA.")
        if self.config.min_oos_trades <= 0:
            raise ValueError("min_oos_trades must be positive for adaptive WFA.")
        if self.config.check_interval_trades <= 0:
            raise ValueError("check_interval_trades must be positive for adaptive WFA.")
        if self.config.cooldown_enabled and self.config.cooldown_days <= 0:
            raise ValueError("cooldown_days must be positive when cooldown is enabled.")

        trading_start, trading_end = self._resolve_trading_bounds(df)
        trading_start_normalized = trading_start.normalize()

        start_idx = df.index.searchsorted(trading_start_normalized, side="left")
        if start_idx >= len(df):
            raise ValueError(
                f"Normalized trading start {trading_start_normalized.date()} "
                "is beyond available data range"
            )

        current_start_idx = start_idx
        current_start = df.index[current_start_idx]

        window_results: List[WindowResult] = []
        dense_stitch_windows: List[StitchWindow] = []
        compact_stitch_windows: List[StitchWindow] = []
        window_id = 1

        while True:
            if current_start >= trading_end:
                break

            is_start_target = current_start
            is_end_target = is_start_target + pd.Timedelta(days=self.config.is_period_days)
            oos_start_target = is_end_target

            if oos_start_target >= trading_end:
                break

            is_start_idx = df.index.searchsorted(is_start_target, side="left")
            is_end_idx = df.index.searchsorted(is_end_target, side="left") - 1
            oos_start_idx = df.index.searchsorted(oos_start_target, side="left")

            if (
                is_start_idx >= len(df)
                or is_end_idx >= len(df)
                or oos_start_idx >= len(df)
                or is_end_idx < is_start_idx
            ):
                break

            is_start = df.index[is_start_idx]
            is_end = df.index[is_end_idx]
            oos_start = df.index[oos_start_idx]

            oos_max_end_target = min(
                oos_start_target + pd.Timedelta(days=self.config.max_oos_period_days),
                trading_end,
            )
            oos_max_end_idx = df.index.searchsorted(oos_max_end_target, side="right") - 1
            if oos_max_end_idx < oos_start_idx or oos_max_end_idx >= len(df):
                break
            oos_max_end = df.index[oos_max_end_idx]
            if oos_max_end <= oos_start:
                break

            print(f"\n--- Adaptive Window {window_id} ---")
            print(f"IS optimization: dates {is_start.date()} to {is_end.date()}")

            is_pipeline = self._run_window_is_pipeline(
                df=df,
                is_start=is_start,
                is_end=is_end,
                window_id=window_id,
            )
            print(f"Best param ID: {is_pipeline.param_id}")

            is_result = self._run_period_backtest(
                df=df,
                start=is_start,
                end=is_end,
                params=is_pipeline.best_params,
            )
            is_basic = metrics.calculate_basic(is_result, initial_balance=100.0)
            is_adv = metrics.calculate_advanced(is_result, initial_balance=100.0)
            baseline = self._compute_is_baseline(is_result, self.config.is_period_days)

            if not (
                baseline.get("cusum_enabled")
                or baseline.get("drawdown_enabled")
                or baseline.get("inactivity_enabled")
            ):
                logger.warning(
                    "Adaptive triggers disabled in window %s due to no IS trades; "
                    "window will end by max period.",
                    window_id,
                )

            print(f"OOS validation (adaptive max): dates {oos_start.date()} to {oos_max_end.date()}")
            oos_result = self._run_period_backtest(
                df=df,
                start=oos_start,
                end=oos_max_end,
                params=is_pipeline.best_params,
            )

            trigger_result = self._scan_triggers(
                trades=list(getattr(oos_result, "trades", None) or []),
                balance_curve=list(getattr(oos_result, "balance_curve", None) or []),
                timestamps=list(getattr(oos_result, "timestamps", None) or []),
                baseline=baseline,
                oos_start=oos_start,
                oos_max_end=oos_max_end,
            )
            truncated_oos_result, oos_actual_end = self._truncate_oos_result(
                oos_result=oos_result,
                trigger_result=trigger_result,
                oos_max_end=oos_max_end,
            )
            adaptive_roll_end, cooldown_days_applied = self._resolve_adaptive_roll_end(
                trigger_result=trigger_result,
                oos_actual_end=oos_actual_end,
                trading_end=trading_end,
            )
            oos_elapsed_days = self._duration_days(oos_start, adaptive_roll_end)
            oos_basic = metrics.calculate_basic(truncated_oos_result, initial_balance=100.0)
            oos_adv = metrics.calculate_advanced(truncated_oos_result, initial_balance=100.0)

            if oos_basic.total_trades == 0:
                print(
                    "Warning: Window "
                    f"{window_id} produced no OOS trades before trigger."
                )
            if cooldown_days_applied is not None:
                print(
                    "  Cooldown activated: "
                    f"{cooldown_days_applied:.1f}d after {trigger_result.trigger_type} trigger"
                )

            dense_stitch_windows.append(
                StitchWindow(
                    window_id=window_id,
                    oos_equity_curve=list(truncated_oos_result.balance_curve or []),
                    oos_timestamps=list(truncated_oos_result.timestamps or []),
                    oos_total_trades=oos_basic.total_trades,
                    oos_start=oos_start,
                )
            )

            window_split = WindowSplit(
                window_id=window_id,
                is_start=is_start,
                is_end=is_end,
                oos_start=oos_start,
                oos_end=oos_actual_end,
            )
            compact_equity, compact_timestamps = self._build_compact_oos_curve(
                truncated_oos_result, window_split
            )
            compact_stitch_windows.append(
                StitchWindow(
                    window_id=window_id,
                    oos_equity_curve=compact_equity,
                    oos_timestamps=compact_timestamps,
                    oos_total_trades=oos_basic.total_trades,
                    oos_start=oos_start,
                )
            )

            window_results.append(
                WindowResult(
                    window_id=window_id,
                    is_start=is_start,
                    is_end=is_end,
                    oos_start=oos_start,
                    oos_end=oos_actual_end,
                    best_params=is_pipeline.best_params,
                    param_id=is_pipeline.param_id,
                    is_net_profit_pct=is_basic.net_profit_pct,
                    is_max_drawdown_pct=is_basic.max_drawdown_pct,
                    is_total_trades=is_basic.total_trades,
                    is_best_trial_number=is_pipeline.best_trial_number,
                    is_equity_curve=None,
                    is_timestamps=None,
                    oos_net_profit_pct=oos_basic.net_profit_pct,
                    oos_max_drawdown_pct=oos_basic.max_drawdown_pct,
                    oos_total_trades=oos_basic.total_trades,
                    oos_equity_curve=[],
                    oos_timestamps=[],
                    oos_winning_trades=oos_basic.winning_trades,
                    is_pareto_optimal=is_pipeline.is_pareto_optimal,
                    constraints_satisfied=is_pipeline.constraints_satisfied,
                    best_params_source=is_pipeline.best_params_source,
                    available_modules=is_pipeline.available_modules,
                    optuna_is_trials=is_pipeline.optuna_is_trials,
                    dsr_trials=is_pipeline.dsr_trials,
                    forward_test_trials=is_pipeline.forward_test_trials,
                    stress_test_trials=is_pipeline.stress_test_trials,
                    module_status=is_pipeline.module_status,
                    selection_chain=is_pipeline.selection_chain,
                    optimization_start=is_pipeline.optimization_start,
                    optimization_end=is_pipeline.optimization_end,
                    ft_start=is_pipeline.ft_start,
                    ft_end=is_pipeline.ft_end,
                    is_win_rate=is_basic.win_rate,
                    is_max_consecutive_losses=is_basic.max_consecutive_losses,
                    is_romad=is_adv.romad,
                    is_sharpe_ratio=is_adv.sharpe_ratio,
                    is_profit_factor=is_adv.profit_factor,
                    is_sqn=is_adv.sqn,
                    is_ulcer_index=is_adv.ulcer_index,
                    is_consistency_score=is_adv.consistency_score,
                    is_composite_score=getattr(is_pipeline.best_result, "score", None),
                    oos_win_rate=oos_basic.win_rate,
                    oos_max_consecutive_losses=oos_basic.max_consecutive_losses,
                    oos_romad=oos_adv.romad,
                    oos_sharpe_ratio=oos_adv.sharpe_ratio,
                    oos_profit_factor=oos_adv.profit_factor,
                    oos_sqn=oos_adv.sqn,
                    oos_ulcer_index=oos_adv.ulcer_index,
                    oos_consistency_score=oos_adv.consistency_score,
                    trigger_type=trigger_result.trigger_type,
                    cusum_final=trigger_result.cusum_final,
                    cusum_threshold=trigger_result.cusum_threshold,
                    dd_threshold=trigger_result.dd_threshold,
                    oos_actual_days=trigger_result.oos_actual_days,
                    cooldown_days_applied=cooldown_days_applied,
                    oos_elapsed_days=oos_elapsed_days,
                )
            )

            shift = adaptive_roll_end - oos_start
            if shift <= pd.Timedelta(0):
                shift = pd.Timedelta(days=1)

            next_start_target = current_start + shift
            # Start the next adaptive window on the first bar strictly after
            # the previous OOS actual end to avoid same-day overlaps.
            next_start_idx = df.index.searchsorted(next_start_target, side="right")
            if next_start_idx <= current_start_idx:
                next_start_idx = current_start_idx + 1
            if next_start_idx >= len(df):
                break

            current_start_idx = next_start_idx
            current_start = df.index[current_start_idx]
            window_id += 1

        if not window_results:
            raise ValueError("Failed to create any adaptive walk-forward windows.")

        stitched_oos = self._build_stitched_oos_equity(
            window_results,
            dense_windows=dense_stitch_windows,
            compact_windows=compact_stitch_windows,
        )

        wf_result = WFResult(
            config=self.config,
            windows=window_results,
            stitched_oos=stitched_oos,
            strategy_id=self.config.strategy_id,
            total_windows=len(window_results),
            trading_start_date=trading_start,
            trading_end_date=trading_end,
            warmup_bars=self.config.warmup_bars,
        )

        study_id = None
        if self.csv_file_path:
            study_id = save_wfa_study_to_db(
                wf_result=wf_result,
                config=self.base_config_template,
                csv_file_path=self.csv_file_path,
                start_time=start_time,
                score_config=self.base_config_template.get("score_config")
                if isinstance(self.base_config_template, dict)
                else None,
            )

        return wf_result, study_id

    def _build_compact_oos_curve(
        self,
        result: Any,
        window: WindowSplit,
    ) -> Tuple[List[float], List[pd.Timestamp]]:
        timestamps = list(getattr(result, "timestamps", None) or [])
        balance_curve = list(getattr(result, "balance_curve", None) or [])
        if not timestamps or not balance_curve:
            return [], []

        time_index = pd.DatetimeIndex(timestamps)

        def _resolve_point(target: Optional[pd.Timestamp]) -> Optional[Tuple[pd.Timestamp, float]]:
            if target is None:
                return None
            idx = int(time_index.searchsorted(target, side="right") - 1)
            if idx < 0:
                idx = 0
            if idx >= len(balance_curve):
                idx = len(balance_curve) - 1
            return timestamps[idx], balance_curve[idx]

        points: List[Tuple[pd.Timestamp, float]] = []
        for anchor in (window.oos_start, window.oos_end):
            resolved = _resolve_point(anchor)
            if resolved:
                points.append(resolved)

        for trade in getattr(result, "trades", None) or []:
            exit_time = getattr(trade, "exit_time", None)
            resolved = _resolve_point(exit_time)
            if resolved:
                points.append(resolved)

        if not points:
            return [balance_curve[0]], [timestamps[0]]

        points.sort(key=lambda item: item[0])
        deduped: List[Tuple[pd.Timestamp, float]] = []
        for ts, equity in points:
            if deduped and ts == deduped[-1][0]:
                deduped[-1] = (ts, equity)
            else:
                deduped.append((ts, equity))

        return [equity for _, equity in deduped], [ts for ts, _ in deduped]

    def _stitch_windows(
        self,
        windows: List[StitchWindow],
        include_start_for_all: bool,
    ) -> Tuple[List[float], List[pd.Timestamp], List[int]]:
        stitched_equity: List[float] = []
        stitched_timestamps: List[pd.Timestamp] = []
        stitched_window_ids: List[int] = []

        current_balance = 100.0
        for idx, window_result in enumerate(windows):
            window_equity = list(window_result.oos_equity_curve or [])
            window_timestamps = list(window_result.oos_timestamps or [])
            if not window_equity:
                continue

            start_equity = window_equity[0] if window_equity else 0.0
            if start_equity == 0:
                start_equity = 100.0

            start_idx = 0 if include_start_for_all or idx == 0 else 1
            for j in range(start_idx, len(window_equity)):
                pct_change = (window_equity[j] / start_equity) - 1.0
                new_balance = current_balance * (1.0 + pct_change)
                stitched_equity.append(new_balance)
                if j < len(window_timestamps):
                    stitched_timestamps.append(window_timestamps[j])
                else:
                    stitched_timestamps.append(window_result.oos_start)
                stitched_window_ids.append(window_result.window_id)

            if stitched_equity:
                current_balance = stitched_equity[-1]

        return stitched_equity, stitched_timestamps, stitched_window_ids

    def _build_stitched_oos_equity(
        self,
        windows: List[WindowResult],
        dense_windows: Optional[List[StitchWindow]] = None,
        compact_windows: Optional[List[StitchWindow]] = None,
    ) -> OOSStitchedResult:
        """Build stitched OOS equity curve and metrics (metrics from dense data)."""

        if dense_windows is None:
            dense_windows = [
                StitchWindow(
                    window_id=window.window_id,
                    oos_equity_curve=list(window.oos_equity_curve or []),
                    oos_timestamps=list(window.oos_timestamps or []),
                    oos_total_trades=window.oos_total_trades,
                    oos_start=window.oos_start,
                )
                for window in windows
            ]
        use_compact = compact_windows is not None
        if compact_windows is None:
            compact_windows = dense_windows

        dense_equity, _dense_timestamps, _dense_window_ids = self._stitch_windows(
            dense_windows, include_start_for_all=False
        )
        compact_equity, compact_timestamps, compact_window_ids = self._stitch_windows(
            compact_windows, include_start_for_all=use_compact
        )

        if len(dense_equity) == 0:
            final_net_profit_pct = 0.0
            max_drawdown_pct = 0.0
        else:
            final_net_profit_pct = (dense_equity[-1] / 100.0 - 1.0) * 100.0

            peak = dense_equity[0]
            max_dd = 0.0
            for equity_value in dense_equity:
                if equity_value > peak:
                    peak = equity_value
                if peak > 0:
                    dd = (peak - equity_value) / peak * 100.0
                    if dd > max_dd:
                        max_dd = dd
            max_drawdown_pct = max_dd

        total_trades = sum(w.oos_total_trades for w in windows) if windows else 0

        if windows:
            avg_is_profit = sum(w.is_net_profit_pct for w in windows) / len(windows)
            days_per_year = 365.0
            is_annual_factor = days_per_year / self.config.is_period_days

            annualized_is = avg_is_profit * is_annual_factor
            if self.config.adaptive_mode:
                total_oos_profit = sum(w.oos_net_profit_pct for w in windows)
                total_oos_days = 0.0
                for window in windows:
                    oos_days = getattr(window, "oos_elapsed_days", None)
                    if oos_days is None:
                        oos_days = getattr(window, "oos_actual_days", None)
                    if oos_days is None:
                        oos_days = self._duration_days(window.oos_start, window.oos_end)
                    if oos_days > 0:
                        total_oos_days += float(oos_days)
                if total_oos_days > 0:
                    annualized_oos = (total_oos_profit / total_oos_days) * days_per_year
                else:
                    annualized_oos = 0.0
            else:
                avg_oos_profit = sum(w.oos_net_profit_pct for w in windows) / len(windows)
                oos_annual_factor = days_per_year / self.config.oos_period_days
                annualized_oos = avg_oos_profit * oos_annual_factor

            if annualized_is != 0:
                wfe = (annualized_oos / annualized_is) * 100.0
            else:
                wfe = 0.0 if annualized_oos <= 0 else 100.0
        else:
            wfe = 0.0

        if windows:
            profitable_oos = sum(1 for w in windows if w.oos_net_profit_pct > 0)
            oos_win_rate = (profitable_oos / len(windows)) * 100.0
        else:
            oos_win_rate = 0.0

        return OOSStitchedResult(
            final_net_profit_pct=final_net_profit_pct,
            max_drawdown_pct=max_drawdown_pct,
            total_trades=total_trades,
            wfe=wfe,
            oos_win_rate=oos_win_rate,
            equity_curve=compact_equity,
            timestamps=compact_timestamps,
            window_ids=compact_window_ids,
        )

    def _convert_optuna_results_for_storage(
        self, results: List[Any], limit: int
    ) -> List[Dict[str, Any]]:
        trials: List[Dict[str, Any]] = []
        for index, result in enumerate(results[:limit]):
            trial_number = getattr(result, "optuna_trial_number", None)
            if trial_number is None:
                trial_number = index

            params = getattr(result, "params", {}) or {}
            trials.append(
                {
                    "trial_number": trial_number,
                    "params": params,
                    "param_id": self._create_param_id(params),
                    "source_rank": None,
                    "module_rank": index + 1,
                    "net_profit_pct": getattr(result, "net_profit_pct", None),
                    "max_drawdown_pct": getattr(result, "max_drawdown_pct", None),
                    "total_trades": getattr(result, "total_trades", None),
                    "win_rate": getattr(result, "win_rate", None),
                    "profit_factor": getattr(result, "profit_factor", None),
                    "romad": getattr(result, "romad", None),
                    "sharpe_ratio": getattr(result, "sharpe_ratio", None),
                    "sortino_ratio": getattr(result, "sortino_ratio", None),
                    "sqn": getattr(result, "sqn", None),
                    "ulcer_index": getattr(result, "ulcer_index", None),
                    "consistency_score": getattr(result, "consistency_score", None),
                    "max_consecutive_losses": getattr(result, "max_consecutive_losses", None),
                    "composite_score": getattr(result, "score", None),
                    "objective_values": getattr(result, "objective_values", []) or [],
                    "constraint_values": getattr(result, "constraint_values", []) or [],
                    "constraints_satisfied": getattr(result, "constraints_satisfied", None),
                    "is_pareto_optimal": getattr(result, "is_pareto_optimal", None),
                    "dominance_rank": getattr(result, "dominance_rank", None),
                }
            )
        return trials

    def _convert_dsr_results_for_storage(
        self, results: List[Any], limit: int
    ) -> List[Dict[str, Any]]:
        trials: List[Dict[str, Any]] = []
        for result in results[:limit]:
            original = getattr(result, "original_result", None)
            params = getattr(result, "params", {}) or {}
            trials.append(
                {
                    "trial_number": getattr(result, "trial_number", None),
                    "params": params,
                    "param_id": self._create_param_id(params),
                    "source_rank": getattr(result, "optuna_rank", None),
                    "module_rank": getattr(result, "dsr_rank", None),
                    "net_profit_pct": getattr(original, "net_profit_pct", None),
                    "max_drawdown_pct": getattr(original, "max_drawdown_pct", None),
                    "total_trades": getattr(original, "total_trades", None),
                    "win_rate": getattr(original, "win_rate", None),
                    "profit_factor": getattr(original, "profit_factor", None),
                    "romad": getattr(original, "romad", None),
                    "sharpe_ratio": getattr(original, "sharpe_ratio", None),
                    "sortino_ratio": getattr(original, "sortino_ratio", None),
                    "sqn": getattr(original, "sqn", None),
                    "ulcer_index": getattr(original, "ulcer_index", None),
                    "consistency_score": getattr(original, "consistency_score", None),
                    "max_consecutive_losses": getattr(original, "max_consecutive_losses", None),
                    "composite_score": getattr(original, "score", None),
                    "objective_values": getattr(original, "objective_values", []) or [],
                    "constraint_values": getattr(original, "constraint_values", []) or [],
                    "constraints_satisfied": getattr(original, "constraints_satisfied", None),
                    "is_pareto_optimal": getattr(original, "is_pareto_optimal", None),
                    "dominance_rank": getattr(original, "dominance_rank", None),
                    "module_metrics": {
                        "dsr_probability": getattr(result, "dsr_probability", None),
                        "dsr_rank": getattr(result, "dsr_rank", None),
                        "dsr_skewness": getattr(result, "dsr_skewness", None),
                        "dsr_kurtosis": getattr(result, "dsr_kurtosis", None),
                        "dsr_track_length": getattr(result, "dsr_track_length", None),
                        "dsr_luck_share_pct": getattr(result, "dsr_luck_share_pct", None),
                    },
                }
            )
        return trials

    def _convert_ft_results_for_storage(
        self,
        results: List[Any],
        limit: int,
        optuna_map: Dict[int, Any],
    ) -> List[Dict[str, Any]]:
        trials: List[Dict[str, Any]] = []
        for result in results[:limit]:
            params = getattr(result, "params", {}) or {}
            trial_number = getattr(result, "trial_number", None)
            optuna_result = optuna_map.get(trial_number)
            trials.append(
                {
                    "trial_number": trial_number,
                    "params": params,
                    "param_id": self._create_param_id(params),
                    "source_rank": getattr(result, "source_rank", None),
                    "module_rank": getattr(result, "ft_rank", None),
                    "net_profit_pct": getattr(result, "ft_net_profit_pct", None),
                    "max_drawdown_pct": getattr(result, "ft_max_drawdown_pct", None),
                    "total_trades": getattr(result, "ft_total_trades", None),
                    "win_rate": getattr(result, "ft_win_rate", None),
                    "profit_factor": getattr(result, "ft_profit_factor", None),
                    "romad": getattr(result, "ft_romad", None),
                    "sharpe_ratio": getattr(result, "ft_sharpe_ratio", None),
                    "sortino_ratio": getattr(result, "ft_sortino_ratio", None),
                    "sqn": getattr(result, "ft_sqn", None),
                    "ulcer_index": getattr(result, "ft_ulcer_index", None),
                    "consistency_score": getattr(result, "ft_consistency_score", None),
                    "max_consecutive_losses": getattr(result, "ft_max_consecutive_losses", None),
                    "composite_score": getattr(optuna_result, "score", None),
                    "objective_values": getattr(optuna_result, "objective_values", []) or [],
                    "constraint_values": getattr(optuna_result, "constraint_values", []) or [],
                    "constraints_satisfied": getattr(optuna_result, "constraints_satisfied", None),
                    "is_pareto_optimal": getattr(optuna_result, "is_pareto_optimal", None),
                    "dominance_rank": getattr(optuna_result, "dominance_rank", None),
                    "module_metrics": {
                        "is_net_profit_pct": getattr(result, "is_net_profit_pct", None),
                        "is_max_drawdown_pct": getattr(result, "is_max_drawdown_pct", None),
                        "is_total_trades": getattr(result, "is_total_trades", None),
                        "is_win_rate": getattr(result, "is_win_rate", None),
                        "profit_degradation": getattr(result, "profit_degradation", None),
                        "max_dd_change": getattr(result, "max_dd_change", None),
                        "romad_change": getattr(result, "romad_change", None),
                        "sharpe_change": getattr(result, "sharpe_change", None),
                        "pf_change": getattr(result, "pf_change", None),
                    },
                }
            )
        return trials

    def _convert_st_results_for_storage(
        self,
        results: List[Any],
        limit: int,
        trial_to_params: Dict[int, Dict[str, Any]],
        optuna_map: Dict[int, Any],
    ) -> List[Dict[str, Any]]:
        trials: List[Dict[str, Any]] = []
        for result in results[:limit]:
            trial_number = getattr(result, "trial_number", None)
            params = trial_to_params.get(trial_number) or {}
            optuna_result = optuna_map.get(trial_number)
            trials.append(
                {
                    "trial_number": trial_number,
                    "params": params,
                    "param_id": self._create_param_id(params),
                    "source_rank": getattr(result, "source_rank", None),
                    "module_rank": getattr(result, "st_rank", None),
                    "net_profit_pct": getattr(optuna_result, "net_profit_pct", None)
                    if optuna_result is not None
                    else getattr(result, "base_net_profit_pct", None),
                    "max_drawdown_pct": getattr(optuna_result, "max_drawdown_pct", None)
                    if optuna_result is not None
                    else getattr(result, "base_max_drawdown_pct", None),
                    "total_trades": getattr(optuna_result, "total_trades", None)
                    if optuna_result is not None
                    else None,
                    "win_rate": getattr(optuna_result, "win_rate", None)
                    if optuna_result is not None
                    else None,
                    "profit_factor": getattr(optuna_result, "profit_factor", None)
                    if optuna_result is not None
                    else None,
                    "romad": getattr(optuna_result, "romad", None)
                    if optuna_result is not None
                    else getattr(result, "base_romad", None),
                    "sharpe_ratio": getattr(optuna_result, "sharpe_ratio", None)
                    if optuna_result is not None
                    else getattr(result, "base_sharpe_ratio", None),
                    "sortino_ratio": getattr(optuna_result, "sortino_ratio", None)
                    if optuna_result is not None
                    else None,
                    "sqn": getattr(optuna_result, "sqn", None)
                    if optuna_result is not None
                    else None,
                    "ulcer_index": getattr(optuna_result, "ulcer_index", None)
                    if optuna_result is not None
                    else None,
                    "consistency_score": getattr(optuna_result, "consistency_score", None)
                    if optuna_result is not None
                    else None,
                    "max_consecutive_losses": getattr(optuna_result, "max_consecutive_losses", None)
                    if optuna_result is not None
                    else None,
                    "composite_score": getattr(optuna_result, "score", None),
                    "objective_values": getattr(optuna_result, "objective_values", []) or [],
                    "constraint_values": getattr(optuna_result, "constraint_values", []) or [],
                    "constraints_satisfied": getattr(optuna_result, "constraints_satisfied", None),
                    "is_pareto_optimal": getattr(optuna_result, "is_pareto_optimal", None),
                    "dominance_rank": getattr(optuna_result, "dominance_rank", None),
                    "status": getattr(result, "status", None),
                    "module_metrics": {
                        "profit_retention": getattr(result, "profit_retention", None),
                        "romad_retention": getattr(result, "romad_retention", None),
                        "profit_worst": getattr(result, "profit_worst", None),
                        "profit_lower_tail": getattr(result, "profit_lower_tail", None),
                        "profit_median": getattr(result, "profit_median", None),
                        "romad_worst": getattr(result, "romad_worst", None),
                        "romad_lower_tail": getattr(result, "romad_lower_tail", None),
                        "romad_median": getattr(result, "romad_median", None),
                    "profit_failure_rate": getattr(result, "profit_failure_rate", None),
                    "romad_failure_rate": getattr(result, "romad_failure_rate", None),
                    "combined_failure_rate": getattr(result, "combined_failure_rate", None),
                    "combined_failure_count": getattr(result, "combined_failure_count", None),
                    "total_perturbations": getattr(result, "total_perturbations", None),
                    "most_sensitive_param": getattr(result, "most_sensitive_param", None),
                },
            }
        )
        return trials

    def _run_optuna_on_window(
        self, df: pd.DataFrame, start_time: pd.Timestamp, end_time: pd.Timestamp
    ) -> Tuple[List[Any], List[Any]]:
        """Run Optuna optimization for a single WFA window."""
        csv_buffer = self._dataframe_to_csv_buffer(df)

        fixed_params = deepcopy(self.base_config_template.get("fixed_params", {}))
        fixed_params["dateFilter"] = True
        fixed_params["start"] = start_time.isoformat()
        fixed_params["end"] = end_time.isoformat()

        base_config = OptimizationConfig(
            csv_file=csv_buffer,
            strategy_id=self.config.strategy_id,
            enabled_params=deepcopy(self.base_config_template["enabled_params"]),
            param_ranges=deepcopy(self.base_config_template["param_ranges"]),
            param_types=deepcopy(self.base_config_template.get("param_types", {})),
            fixed_params=fixed_params,
            worker_processes=int(self.base_config_template["worker_processes"]),
            warmup_bars=self.config.warmup_bars,
            csv_original_name=self.base_config_template.get("csv_original_name"),
            risk_per_trade_pct=float(self.base_config_template["risk_per_trade_pct"]),
            contract_size=float(self.base_config_template["contract_size"]),
            commission_rate=float(self.base_config_template["commission_rate"]),
            filter_min_profit=bool(self.base_config_template["filter_min_profit"]),
            min_profit_threshold=float(self.base_config_template["min_profit_threshold"]),
            score_config=deepcopy(self.base_config_template.get("score_config", {})),
            detailed_log=bool(self.base_config_template.get("detailed_log", False)),
            trials_log=bool(self.base_config_template.get("trials_log", False)),
            dispatcher_batch_result_processing=bool(
                self.base_config_template.get("dispatcher_batch_result_processing", True)
            ),
            dispatcher_soft_duplicate_cycle_limit_enabled=bool(
                self.base_config_template.get("dispatcher_soft_duplicate_cycle_limit_enabled", True)
            ),
            dispatcher_duplicate_cycle_limit=int(
                self.base_config_template.get("dispatcher_duplicate_cycle_limit", 18)
            ),
            optimization_mode="wfa",
            objectives=list(self.base_config_template.get("objectives") or []),
            primary_objective=self.base_config_template.get("primary_objective"),
            constraints=deepcopy(self.base_config_template.get("constraints", [])),
            sampler_type=self.base_config_template.get("sampler_type", "tpe"),
            population_size=self.base_config_template.get("population_size", 50),
            crossover_prob=self.base_config_template.get("crossover_prob", 0.9),
            mutation_prob=self.base_config_template.get("mutation_prob"),
            swapping_prob=self.base_config_template.get("swapping_prob", 0.5),
            n_startup_trials=self.base_config_template.get("n_startup_trials", 20),
            coverage_mode=bool(self.base_config_template.get("coverage_mode", False)),
        )

        objectives = list(self.optuna_settings.get("objectives") or [])
        primary_objective = self.optuna_settings.get("primary_objective")
        constraints_payload = list(self.optuna_settings.get("constraints") or [])

        sampler_config = SamplerConfig(
            sampler_type=str(self.optuna_settings.get("sampler", "tpe")).lower(),
            population_size=int(self.optuna_settings.get("population_size") or 50),
            crossover_prob=float(self.optuna_settings.get("crossover_prob") or 0.9),
            mutation_prob=self.optuna_settings.get("mutation_prob"),
            swapping_prob=float(self.optuna_settings.get("swapping_prob") or 0.5),
            n_startup_trials=int(self.optuna_settings.get("warmup_trials") or 20),
        )

        enable_pruning = bool(self.optuna_settings.get("enable_pruning", True))
        if len(objectives) > 1:
            enable_pruning = False

        optuna_cfg = OptunaConfig(
            objectives=objectives,
            primary_objective=primary_objective,
            constraints=constraints_payload,
            sampler_config=sampler_config,
            budget_mode=self.optuna_settings["budget_mode"],
            n_trials=self.optuna_settings["n_trials"],
            time_limit=self.optuna_settings["time_limit"],
            convergence_patience=self.optuna_settings["convergence_patience"],
            enable_pruning=enable_pruning,
            pruner=self.optuna_settings["pruner"],
            warmup_trials=int(self.optuna_settings.get("warmup_trials") or 20),
            coverage_mode=bool(self.optuna_settings.get("coverage_mode", False)),
            save_study=False,
            study_name=None,
        )

        results, _study_id = run_optuna_optimization(base_config, optuna_cfg)
        all_results = list(getattr(base_config, "optuna_all_results", []))
        return results, all_results

    def _dataframe_to_csv_buffer(self, df_window: pd.DataFrame) -> io.StringIO:
        buffer = io.StringIO()
        working_df = df_window.copy()
        working_df["time"] = working_df.index.view("int64") // 10**9
        ordered_cols = ["time", "Open", "High", "Low", "Close", "Volume"]
        working_df = working_df[ordered_cols]
        working_df.to_csv(buffer, index=False)
        buffer.seek(0)
        return buffer

    def _result_to_params(self, result) -> Dict[str, Any]:
        params = dict(getattr(result, "params", {}) or {})

        params.setdefault("dateFilter", False)
        params.setdefault("start", None)
        params.setdefault("end", None)
        params.setdefault("riskPerTrade", float(self.base_config_template["risk_per_trade_pct"]))
        params.setdefault("contractSize", float(self.base_config_template["contract_size"]))
        params.setdefault("commissionRate", float(self.base_config_template["commission_rate"]))

        return params

    def _create_param_id(self, params: Dict[str, Any]) -> str:
        """Create unique ID for param set using first 2 optimizable parameters."""
        param_str = json.dumps(params, sort_keys=True)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]

        try:
            from strategies import get_strategy_config

            config = get_strategy_config(self.config.strategy_id)
            parameters = config.get("parameters", {}) if isinstance(config, dict) else {}

            preferred_pairs = [
                ("maType", "maLength"),
                ("maType3", "maLength3"),
                ("maType2", "maLength2"),
            ]
            for left, right in preferred_pairs:
                if left in params and right in params:
                    label = f"{params.get(left)} {params.get(right)}"
                    return f"{label}_{param_hash}"

            optimizable: List[str] = []
            for param_name, param_spec in parameters.items():
                if not isinstance(param_spec, dict):
                    continue
                optimize_cfg = param_spec.get("optimize", {})
                if isinstance(optimize_cfg, dict) and optimize_cfg.get("enabled", False):
                    optimizable.append(param_name)
                if len(optimizable) == 2:
                    break

            label_parts = [str(params.get(param_name, "?")) for param_name in optimizable]
            if label_parts:
                label = " ".join(label_parts)
                return f"{label}_{param_hash}"
        except (ImportError, ValueError, KeyError, TypeError, AttributeError) as exc:
            logger.warning(
                "Falling back to hash-only param_id for strategy '%s': %s",
                self.config.strategy_id,
                exc,
            )

        return param_hash
