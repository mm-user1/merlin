"""SQLite storage utilities for persisted optimization studies."""
from __future__ import annotations

import json
import hashlib
import math
import os
import re
import sqlite3
import statistics
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

OBJECTIVE_DIRECTIONS: Dict[str, str] = {
    "net_profit_pct": "maximize",
    "max_drawdown_pct": "minimize",
    "sharpe_ratio": "maximize",
    "sortino_ratio": "maximize",
    "romad": "maximize",
    "profit_factor": "maximize",
    "win_rate": "maximize",
    "sqn": "maximize",
    "ulcer_index": "minimize",
    "consistency_score": "maximize",
    "composite_score": "maximize",
}

ANALYTICS_GROUP_KEY_ALL = "all"
ANALYTICS_GROUP_TYPE_ALL = "all"
ANALYTICS_GROUP_TYPE_SET = "set"
ANALYTICS_CONSISTENCY_RECENT_FRACTION = 0.25
ANALYTICS_CONSISTENCY_MIN_POINTS = 3

DB_INIT_LOCK = threading.Lock()
DB_ACCESS_LOCK = threading.RLock()
DB_INITIALIZED = False

BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / "storage"
JOURNAL_DIR = STORAGE_DIR / "journals"
_active_db_path: Path = STORAGE_DIR / "studies.db"

_INVALID_DB_LABEL_CHARS = re.compile(r'[<>:"/\\|?*]')


def _sanitize_db_label(label: str) -> str:
    return _INVALID_DB_LABEL_CHARS.sub("", str(label or "")).strip().replace(" ", "-")[:50]


def _generate_db_filename(label: str) -> str:
    """Generate a timestamped DB filename with optional sanitized label."""
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    safe_label = _sanitize_db_label(label)
    if safe_label:
        return f"{ts}_{safe_label}.db"
    return f"{ts}.db"


def _pick_newest_db() -> Path:
    """Return newest .db file in STORAGE_DIR by ctime, or generated default path."""
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    db_files = sorted(
        STORAGE_DIR.glob("*.db"),
        key=lambda path: os.path.getctime(path),
        reverse=True,
    )
    if db_files:
        return db_files[0]
    return STORAGE_DIR / _generate_db_filename("")


_active_db_path = _pick_newest_db()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def init_database(db_path: Optional[Path] = None) -> None:
    """Initialize database schema and ensure storage directories exist."""
    global DB_INITIALIZED
    path = db_path or _active_db_path

    if DB_INITIALIZED and not path.exists():
        DB_INITIALIZED = False
    if DB_INITIALIZED and path.exists():
        with sqlite3.connect(
            str(path),
            check_same_thread=False,
            timeout=30.0,
            isolation_level="DEFERRED",
        ) as conn:
            _configure_connection(conn)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='studies'"
            )
            if cursor.fetchone():
                return
        DB_INITIALIZED = False
    with DB_INIT_LOCK:
        if DB_INITIALIZED:
            return
        STORAGE_DIR.mkdir(parents=True, exist_ok=True)
        JOURNAL_DIR.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(
            str(path),
            check_same_thread=False,
            timeout=30.0,
            isolation_level="DEFERRED",
        ) as conn:
            _configure_connection(conn)
            _create_schema(conn)
        DB_INITIALIZED = True


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS studies (
            study_id TEXT PRIMARY KEY,
            study_name TEXT UNIQUE NOT NULL,
            strategy_id TEXT NOT NULL,
            strategy_version TEXT,

            optimization_mode TEXT NOT NULL,

            objectives_json TEXT,
            n_objectives INTEGER DEFAULT 1,
            directions_json TEXT,
            primary_objective TEXT,

            constraints_json TEXT,

            sampler_type TEXT DEFAULT 'tpe',
            population_size INTEGER,
            crossover_prob REAL,
            mutation_prob REAL,
            swapping_prob REAL,

            budget_mode TEXT,
            n_trials INTEGER,
            time_limit INTEGER,
            convergence_patience INTEGER,

            total_trials INTEGER DEFAULT 0,
            completed_trials INTEGER DEFAULT 0,
            pruned_trials INTEGER DEFAULT 0,
            pareto_front_size INTEGER,

            best_value REAL,
            best_values_json TEXT,

            score_config_json TEXT,
            config_json TEXT,

            csv_file_path TEXT,
            csv_file_name TEXT,

            dataset_start_date TEXT,
            dataset_end_date TEXT,
            warmup_bars INTEGER,

            ft_enabled INTEGER DEFAULT 0,
            ft_period_days INTEGER,
            ft_top_k INTEGER,
            ft_sort_metric TEXT,
            ft_threshold_pct REAL,
            ft_reject_action TEXT,
            ft_reject_cooldown_days INTEGER,
            ft_reject_max_attempts INTEGER,
            ft_reject_min_remaining_oos_days INTEGER,
            ft_start_date TEXT,
            ft_end_date TEXT,
            is_period_days INTEGER,
            adaptive_mode INTEGER DEFAULT 0,
            max_oos_period_days INTEGER,
            min_oos_trades INTEGER,
            check_interval_trades INTEGER,
            cusum_threshold REAL,
            dd_threshold_multiplier REAL,
            inactivity_multiplier REAL,
            cooldown_enabled INTEGER DEFAULT 0,
            cooldown_days INTEGER,

            dsr_enabled INTEGER DEFAULT 0,
            dsr_top_k INTEGER,
            dsr_n_trials INTEGER,
            dsr_mean_sharpe REAL,
            dsr_var_sharpe REAL,

            st_enabled INTEGER DEFAULT 0,
            st_top_k INTEGER,
            st_failure_threshold REAL,
            st_sort_metric TEXT,
            st_avg_profit_retention REAL,
            st_avg_romad_retention REAL,
            st_avg_combined_failure_rate REAL,
            st_total_perturbations INTEGER,
            st_candidates_skipped_bad_base INTEGER,
            st_candidates_skipped_no_params INTEGER,
            st_candidates_insufficient_data INTEGER,
            optimization_time_seconds INTEGER,

            oos_test_enabled INTEGER DEFAULT 0,
            oos_test_period_days INTEGER,
            oos_test_top_k INTEGER,
            oos_test_start_date TEXT,
            oos_test_end_date TEXT,
            oos_test_source_module TEXT,

            created_at TEXT DEFAULT (datetime('now')),
            completed_at TEXT,

            filter_min_profit INTEGER DEFAULT 0,
            min_profit_threshold REAL DEFAULT 0.0,
            sanitize_enabled INTEGER DEFAULT 1,
            sanitize_trades_threshold INTEGER DEFAULT 0,

            stitched_oos_equity_curve TEXT,
            stitched_oos_timestamps_json TEXT,
            stitched_oos_window_ids_json TEXT,
            stitched_oos_start_ts TEXT,
            stitched_oos_end_ts TEXT,
            stitched_oos_point_count INTEGER,
            stitched_oos_net_profit_pct REAL,
            stitched_oos_max_drawdown_pct REAL,
            stitched_oos_total_trades INTEGER,
            stitched_oos_winning_trades INTEGER,
            stitched_oos_win_rate REAL,
            stitched_oos_consistency_full REAL,
            stitched_oos_consistency_recent REAL,
            profitable_windows INTEGER,
            total_windows INTEGER,
            median_window_profit REAL,
            median_window_wr REAL,
            worst_window_profit REAL,
            worst_window_dd REAL
        );

        CREATE INDEX IF NOT EXISTS idx_studies_strategy ON studies(strategy_id);
        CREATE INDEX IF NOT EXISTS idx_studies_created ON studies(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_studies_name ON studies(study_name);

        CREATE TABLE IF NOT EXISTS trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id TEXT NOT NULL,
            trial_number INTEGER NOT NULL,

            params_json TEXT NOT NULL,

            objective_values_json TEXT,

            is_pareto_optimal INTEGER DEFAULT 0,
            dominance_rank INTEGER,

            constraints_satisfied INTEGER DEFAULT 1,
            constraint_values_json TEXT,

            net_profit_pct REAL,
            max_drawdown_pct REAL,
            total_trades INTEGER,
            win_rate REAL,
            max_consecutive_losses INTEGER,
            avg_win REAL,
            avg_loss REAL,
            gross_profit REAL,
            gross_loss REAL,
            sharpe_ratio REAL,
            sortino_ratio REAL,
            romad REAL,
            profit_factor REAL,
            sqn REAL,
            ulcer_index REAL,
            consistency_score REAL,

            composite_score REAL,

            ft_net_profit_pct REAL,
            ft_max_drawdown_pct REAL,
            ft_total_trades INTEGER,
            ft_win_rate REAL,
            ft_max_consecutive_losses INTEGER,
            ft_sharpe_ratio REAL,
            ft_sortino_ratio REAL,
            ft_romad REAL,
            ft_profit_factor REAL,
            ft_ulcer_index REAL,
            ft_sqn REAL,
            ft_consistency_score REAL,
            profit_degradation REAL,
            ft_rank INTEGER,
            ft_passes_threshold INTEGER,
            ft_source TEXT,

            dsr_probability REAL,
            dsr_rank INTEGER,
            dsr_skewness REAL,
            dsr_kurtosis REAL,
            dsr_track_length INTEGER,
            dsr_luck_share_pct REAL,

            st_rank INTEGER,
            st_status TEXT,
            profit_retention REAL,
            romad_retention REAL,
            profit_worst REAL,
            profit_lower_tail REAL,
            profit_median REAL,
            romad_worst REAL,
            romad_lower_tail REAL,
            romad_median REAL,
            profit_failure_rate REAL,
            romad_failure_rate REAL,
            combined_failure_rate REAL,
            profit_failure_count INTEGER,
            romad_failure_count INTEGER,
            combined_failure_count INTEGER,
            total_perturbations INTEGER,
            st_failure_threshold REAL,
            param_worst_ratios TEXT,
            most_sensitive_param TEXT,
            st_source TEXT,

            oos_test_net_profit_pct REAL,
            oos_test_max_drawdown_pct REAL,
            oos_test_total_trades INTEGER,
            oos_test_win_rate REAL,
            oos_test_max_consecutive_losses INTEGER,
            oos_test_sharpe_ratio REAL,
            oos_test_sortino_ratio REAL,
            oos_test_romad REAL,
            oos_test_profit_factor REAL,
            oos_test_ulcer_index REAL,
            oos_test_sqn REAL,
            oos_test_consistency_score REAL,
            oos_test_profit_degradation REAL,
            oos_test_source TEXT,
            oos_test_source_rank INTEGER,

            created_at TEXT DEFAULT (datetime('now')),

            UNIQUE(study_id, trial_number),
            FOREIGN KEY (study_id) REFERENCES studies(study_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_trials_pareto ON trials(study_id, is_pareto_optimal);
        CREATE INDEX IF NOT EXISTS idx_trials_constraints ON trials(study_id, constraints_satisfied);

        CREATE TABLE IF NOT EXISTS manual_tests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),

            test_name TEXT,
            data_source TEXT NOT NULL,
            csv_path TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,

            source_tab TEXT NOT NULL,

            trials_count INTEGER NOT NULL,
            trials_tested_csv TEXT NOT NULL,
            best_profit_degradation REAL,
            worst_profit_degradation REAL,

            results_json TEXT NOT NULL,

            FOREIGN KEY (study_id) REFERENCES studies(study_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_manual_tests_study ON manual_tests(study_id);
        CREATE INDEX IF NOT EXISTS idx_manual_tests_created ON manual_tests(created_at DESC);

        CREATE TABLE IF NOT EXISTS wfa_windows (
            window_id TEXT PRIMARY KEY,
            study_id TEXT NOT NULL,
            window_number INTEGER NOT NULL,

            best_params_json TEXT NOT NULL,
            param_id TEXT,

            is_start_date TEXT,
            is_end_date TEXT,
            is_start_ts TEXT,
            is_end_ts TEXT,
            is_net_profit_pct REAL,
            is_max_drawdown_pct REAL,
            is_total_trades INTEGER,
            is_best_trial_number INTEGER,
            is_equity_curve TEXT,

            oos_start_date TEXT,
            oos_end_date TEXT,
            oos_start_ts TEXT,
            oos_end_ts TEXT,
            oos_net_profit_pct REAL,
            oos_max_drawdown_pct REAL,
            oos_total_trades INTEGER,
            oos_winning_trades INTEGER,
            oos_equity_curve TEXT,
            trigger_type TEXT,
            cusum_final REAL,
            cusum_threshold REAL,
            dd_threshold REAL,
            oos_actual_days REAL,
            cooldown_days_applied REAL,
            oos_elapsed_days REAL,
            trade_start_date TEXT,
            trade_end_date TEXT,
            trade_start_ts TEXT,
            trade_end_ts TEXT,
            entry_delay_days REAL,
            ft_retry_attempts_used INTEGER,
            remaining_oos_days_at_entry REAL,
            window_status TEXT,
            no_trade_reason TEXT,

            wfe REAL,

            FOREIGN KEY (study_id) REFERENCES studies(study_id) ON DELETE CASCADE,
            UNIQUE(study_id, window_number)
        );

        CREATE INDEX IF NOT EXISTS idx_wfa_windows_study ON wfa_windows(study_id);
        CREATE INDEX IF NOT EXISTS idx_wfa_windows_number ON wfa_windows(study_id, window_number);
        """
    )
    _ensure_columns(conn)
    _ensure_wfa_schema_updated(conn)
    ensure_study_sets_tables(conn=conn)
    ensure_analytics_group_cache_tables(conn=conn)


def _ensure_columns(conn: sqlite3.Connection) -> None:
    def ensure(table: str, column: str, definition: str) -> None:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        existing = {row["name"] for row in cursor.fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    ensure("studies", "dsr_enabled", "INTEGER DEFAULT 0")
    ensure("studies", "dsr_top_k", "INTEGER")
    ensure("studies", "dsr_n_trials", "INTEGER")
    ensure("studies", "dsr_mean_sharpe", "REAL")
    ensure("studies", "dsr_var_sharpe", "REAL")
    ensure("studies", "st_enabled", "INTEGER DEFAULT 0")
    ensure("studies", "st_top_k", "INTEGER")
    ensure("studies", "st_failure_threshold", "REAL")
    ensure("studies", "st_sort_metric", "TEXT")
    ensure("studies", "st_avg_profit_retention", "REAL")
    ensure("studies", "st_avg_romad_retention", "REAL")
    ensure("studies", "st_avg_combined_failure_rate", "REAL")
    ensure("studies", "st_total_perturbations", "INTEGER")
    ensure("studies", "st_candidates_skipped_bad_base", "INTEGER")
    ensure("studies", "st_candidates_skipped_no_params", "INTEGER")
    ensure("studies", "st_candidates_insufficient_data", "INTEGER")
    ensure("studies", "optimization_time_seconds", "INTEGER")
    ensure("studies", "oos_test_enabled", "INTEGER DEFAULT 0")
    ensure("studies", "oos_test_period_days", "INTEGER")
    ensure("studies", "oos_test_top_k", "INTEGER")
    ensure("studies", "oos_test_start_date", "TEXT")
    ensure("studies", "oos_test_end_date", "TEXT")
    ensure("studies", "oos_test_source_module", "TEXT")
    ensure("studies", "stitched_oos_equity_curve", "TEXT")
    ensure("studies", "stitched_oos_timestamps_json", "TEXT")
    ensure("studies", "stitched_oos_window_ids_json", "TEXT")
    ensure("studies", "stitched_oos_start_ts", "TEXT")
    ensure("studies", "stitched_oos_end_ts", "TEXT")
    ensure("studies", "stitched_oos_point_count", "INTEGER")
    ensure("studies", "stitched_oos_net_profit_pct", "REAL")
    ensure("studies", "stitched_oos_max_drawdown_pct", "REAL")
    ensure("studies", "stitched_oos_total_trades", "INTEGER")
    ensure("studies", "stitched_oos_winning_trades", "INTEGER")
    ensure("studies", "stitched_oos_win_rate", "REAL")
    ensure("studies", "stitched_oos_consistency_full", "REAL")
    ensure("studies", "stitched_oos_consistency_recent", "REAL")
    ensure("studies", "profitable_windows", "INTEGER")
    ensure("studies", "total_windows", "INTEGER")
    ensure("studies", "median_window_profit", "REAL")
    ensure("studies", "median_window_wr", "REAL")
    ensure("studies", "worst_window_profit", "REAL")
    ensure("studies", "worst_window_dd", "REAL")
    ensure("studies", "adaptive_mode", "INTEGER DEFAULT 0")
    ensure("studies", "max_oos_period_days", "INTEGER")
    ensure("studies", "min_oos_trades", "INTEGER")
    ensure("studies", "check_interval_trades", "INTEGER")
    ensure("studies", "cusum_threshold", "REAL")
    ensure("studies", "dd_threshold_multiplier", "REAL")
    ensure("studies", "inactivity_multiplier", "REAL")
    ensure("studies", "cooldown_enabled", "INTEGER DEFAULT 0")
    ensure("studies", "cooldown_days", "INTEGER")
    ensure("studies", "ft_threshold_pct", "REAL")
    ensure("studies", "ft_reject_action", "TEXT")
    ensure("studies", "ft_reject_cooldown_days", "INTEGER")
    ensure("studies", "ft_reject_max_attempts", "INTEGER")
    ensure("studies", "ft_reject_min_remaining_oos_days", "INTEGER")

    ensure("trials", "max_consecutive_losses", "INTEGER")
    ensure("trials", "ft_max_consecutive_losses", "INTEGER")
    ensure("trials", "ft_passes_threshold", "INTEGER")
    ensure("trials", "dsr_probability", "REAL")
    ensure("trials", "dsr_rank", "INTEGER")
    ensure("trials", "dsr_skewness", "REAL")
    ensure("trials", "dsr_kurtosis", "REAL")
    ensure("trials", "dsr_track_length", "INTEGER")
    ensure("trials", "dsr_luck_share_pct", "REAL")
    ensure("trials", "st_rank", "INTEGER")
    ensure("trials", "st_status", "TEXT")
    ensure("trials", "profit_retention", "REAL")
    ensure("trials", "romad_retention", "REAL")
    ensure("trials", "profit_worst", "REAL")
    ensure("trials", "profit_lower_tail", "REAL")
    ensure("trials", "profit_median", "REAL")
    ensure("trials", "romad_worst", "REAL")
    ensure("trials", "romad_lower_tail", "REAL")
    ensure("trials", "romad_median", "REAL")
    ensure("trials", "profit_failure_rate", "REAL")
    ensure("trials", "romad_failure_rate", "REAL")
    ensure("trials", "combined_failure_rate", "REAL")
    ensure("trials", "profit_failure_count", "INTEGER")
    ensure("trials", "romad_failure_count", "INTEGER")
    ensure("trials", "combined_failure_count", "INTEGER")
    ensure("trials", "total_perturbations", "INTEGER")
    ensure("trials", "st_failure_threshold", "REAL")
    ensure("trials", "param_worst_ratios", "TEXT")
    ensure("trials", "most_sensitive_param", "TEXT")
    ensure("trials", "oos_test_net_profit_pct", "REAL")
    ensure("trials", "oos_test_max_drawdown_pct", "REAL")
    ensure("trials", "oos_test_total_trades", "INTEGER")
    ensure("trials", "oos_test_win_rate", "REAL")
    ensure("trials", "oos_test_max_consecutive_losses", "INTEGER")
    ensure("trials", "oos_test_sharpe_ratio", "REAL")
    ensure("trials", "oos_test_sortino_ratio", "REAL")
    ensure("trials", "oos_test_romad", "REAL")
    ensure("trials", "oos_test_profit_factor", "REAL")
    ensure("trials", "oos_test_ulcer_index", "REAL")
    ensure("trials", "oos_test_sqn", "REAL")
    ensure("trials", "oos_test_consistency_score", "REAL")
    ensure("trials", "oos_test_profit_degradation", "REAL")
    ensure("trials", "oos_test_source", "TEXT")
    ensure("trials", "oos_test_source_rank", "INTEGER")


def _ensure_wfa_schema_updated(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wfa_window_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id TEXT NOT NULL,
            module_type TEXT NOT NULL,
            trial_number INTEGER NOT NULL,
            params_json TEXT NOT NULL,
            param_id TEXT,
            source_rank INTEGER,
            module_rank INTEGER,
            net_profit_pct REAL,
            max_drawdown_pct REAL,
            total_trades INTEGER,
            win_rate REAL,
            profit_factor REAL,
            romad REAL,
            sharpe_ratio REAL,
            sortino_ratio REAL,
            sqn REAL,
            ulcer_index REAL,
            consistency_score REAL,
            max_consecutive_losses INTEGER,
            composite_score REAL,
            objective_values_json TEXT,
            constraint_values_json TEXT,
            constraints_satisfied INTEGER,
            is_pareto_optimal INTEGER,
            dominance_rank INTEGER,
            status TEXT,
            is_selected INTEGER DEFAULT 0,
            module_metrics_json TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (window_id) REFERENCES wfa_windows(window_id) ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_wfa_window_trials_window ON wfa_window_trials(window_id);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_wfa_window_trials_module ON wfa_window_trials(window_id, module_type);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_wfa_window_trials_trial ON wfa_window_trials(window_id, trial_number);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_wfa_window_trials_selected ON wfa_window_trials(window_id, module_type, is_selected);"
    )

    cur.execute("PRAGMA table_info(wfa_windows);")
    existing = {row[1] for row in cur.fetchall()}

    def add_col(col_sql: str, col_name: str) -> None:
        if col_name not in existing:
            cur.execute(col_sql)

    add_col("ALTER TABLE wfa_windows ADD COLUMN best_params_source TEXT;", "best_params_source")
    add_col("ALTER TABLE wfa_windows ADD COLUMN available_modules TEXT;", "available_modules")
    add_col("ALTER TABLE wfa_windows ADD COLUMN module_status_json TEXT;", "module_status_json")
    add_col("ALTER TABLE wfa_windows ADD COLUMN selection_chain_json TEXT;", "selection_chain_json")
    add_col("ALTER TABLE wfa_windows ADD COLUMN store_top_n_trials INTEGER;", "store_top_n_trials")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_pareto_optimal INTEGER;", "is_pareto_optimal")
    add_col("ALTER TABLE wfa_windows ADD COLUMN constraints_satisfied INTEGER;", "constraints_satisfied")

    add_col("ALTER TABLE wfa_windows ADD COLUMN optimization_start_date TEXT;", "optimization_start_date")
    add_col("ALTER TABLE wfa_windows ADD COLUMN optimization_end_date TEXT;", "optimization_end_date")
    add_col("ALTER TABLE wfa_windows ADD COLUMN optimization_start_ts TEXT;", "optimization_start_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN optimization_end_ts TEXT;", "optimization_end_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN ft_start_date TEXT;", "ft_start_date")
    add_col("ALTER TABLE wfa_windows ADD COLUMN ft_end_date TEXT;", "ft_end_date")
    add_col("ALTER TABLE wfa_windows ADD COLUMN ft_start_ts TEXT;", "ft_start_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN ft_end_ts TEXT;", "ft_end_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_timestamps_json TEXT;", "is_timestamps_json")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_timestamps_json TEXT;", "oos_timestamps_json")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_start_ts TEXT;", "is_start_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_end_ts TEXT;", "is_end_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_start_ts TEXT;", "oos_start_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_end_ts TEXT;", "oos_end_ts")

    add_col("ALTER TABLE wfa_windows ADD COLUMN is_win_rate REAL;", "is_win_rate")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_max_consecutive_losses INTEGER;", "is_max_consecutive_losses")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_romad REAL;", "is_romad")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_sharpe_ratio REAL;", "is_sharpe_ratio")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_profit_factor REAL;", "is_profit_factor")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_sqn REAL;", "is_sqn")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_ulcer_index REAL;", "is_ulcer_index")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_consistency_score REAL;", "is_consistency_score")
    add_col("ALTER TABLE wfa_windows ADD COLUMN is_composite_score REAL;", "is_composite_score")

    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_win_rate REAL;", "oos_win_rate")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_winning_trades INTEGER;", "oos_winning_trades")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_max_consecutive_losses INTEGER;", "oos_max_consecutive_losses")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_romad REAL;", "oos_romad")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_sharpe_ratio REAL;", "oos_sharpe_ratio")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_profit_factor REAL;", "oos_profit_factor")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_sqn REAL;", "oos_sqn")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_ulcer_index REAL;", "oos_ulcer_index")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_consistency_score REAL;", "oos_consistency_score")
    add_col("ALTER TABLE wfa_windows ADD COLUMN trigger_type TEXT;", "trigger_type")
    add_col("ALTER TABLE wfa_windows ADD COLUMN cusum_final REAL;", "cusum_final")
    add_col("ALTER TABLE wfa_windows ADD COLUMN cusum_threshold REAL;", "cusum_threshold")
    add_col("ALTER TABLE wfa_windows ADD COLUMN dd_threshold REAL;", "dd_threshold")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_actual_days REAL;", "oos_actual_days")
    add_col("ALTER TABLE wfa_windows ADD COLUMN cooldown_days_applied REAL;", "cooldown_days_applied")
    add_col("ALTER TABLE wfa_windows ADD COLUMN oos_elapsed_days REAL;", "oos_elapsed_days")
    add_col("ALTER TABLE wfa_windows ADD COLUMN trade_start_date TEXT;", "trade_start_date")
    add_col("ALTER TABLE wfa_windows ADD COLUMN trade_end_date TEXT;", "trade_end_date")
    add_col("ALTER TABLE wfa_windows ADD COLUMN trade_start_ts TEXT;", "trade_start_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN trade_end_ts TEXT;", "trade_end_ts")
    add_col("ALTER TABLE wfa_windows ADD COLUMN entry_delay_days REAL;", "entry_delay_days")
    add_col("ALTER TABLE wfa_windows ADD COLUMN ft_retry_attempts_used INTEGER;", "ft_retry_attempts_used")
    add_col(
        "ALTER TABLE wfa_windows ADD COLUMN remaining_oos_days_at_entry REAL;",
        "remaining_oos_days_at_entry",
    )
    add_col("ALTER TABLE wfa_windows ADD COLUMN window_status TEXT;", "window_status")
    add_col("ALTER TABLE wfa_windows ADD COLUMN no_trade_reason TEXT;", "no_trade_reason")

    conn.commit()


@contextmanager
def get_db_connection() -> Iterator[sqlite3.Connection]:
    with DB_ACCESS_LOCK:
        path = _active_db_path

    init_database(db_path=path)
    conn = sqlite3.connect(
        str(path),
        check_same_thread=False,
        timeout=30.0,
        isolation_level="DEFERRED",
    )
    _configure_connection(conn)
    try:
        yield conn
    finally:
        conn.close()


def _validate_db_filename(filename: str) -> Path:
    if not filename:
        raise ValueError("Invalid database filename")
    target = STORAGE_DIR / filename
    if target.parent.resolve() != STORAGE_DIR.resolve():
        raise ValueError("Invalid database filename")
    if target.suffix.lower() != ".db":
        raise ValueError("Invalid database filename")
    return target


def get_active_db_name() -> str:
    """Return the active database filename."""
    return _active_db_path.name


def _set_active_db_path(filename: str) -> None:
    """Internal setter that updates active DB path without existence checks."""
    global _active_db_path, DB_INITIALIZED
    target = _validate_db_filename(filename)
    _active_db_path = target
    DB_INITIALIZED = False


def set_active_db(filename: str) -> None:
    """Set active DB to an existing file in STORAGE_DIR."""
    with DB_ACCESS_LOCK:
        target = _validate_db_filename(filename)
        if not target.exists():
            raise ValueError(f"Database '{filename}' not found")
        _set_active_db_path(filename)


def create_new_db(label: str = "") -> str:
    """Create a new timestamped DB file, set it active, and initialize schema."""
    with DB_ACCESS_LOCK:
        filename = _generate_db_filename(label)
        target = STORAGE_DIR / filename
        _set_active_db_path(filename)
        init_database(db_path=target)
        return filename


def list_db_files() -> List[Dict[str, Any]]:
    """List available DB files sorted by creation time with active marker."""
    with DB_ACCESS_LOCK:
        STORAGE_DIR.mkdir(parents=True, exist_ok=True)
        active_name = _active_db_path.name
        db_files = sorted(
            STORAGE_DIR.glob("*.db"),
            key=lambda path: os.path.getctime(path),
            reverse=True,
        )
        return [{"name": path.name, "active": path.name == active_name} for path in db_files]


def ensure_study_sets_tables(
    db_path: Optional[Path] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    Ensure Study Sets tables exist and remove legacy orphan members.

    When `conn` is provided, this function uses it directly and does not commit.
    """
    if conn is not None:
        _ensure_study_sets_tables_with_conn(conn)
        return

    path = db_path or _active_db_path
    init_database(db_path=path)
    with sqlite3.connect(
        str(path),
        check_same_thread=False,
        timeout=30.0,
        isolation_level="DEFERRED",
    ) as local_conn:
        _configure_connection(local_conn)
        _ensure_study_sets_tables_with_conn(local_conn)
        local_conn.commit()


def _ensure_study_sets_tables_with_conn(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS study_sets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_study_sets_name_nocase
            ON study_sets(LOWER(name));
        CREATE INDEX IF NOT EXISTS idx_study_sets_sort
            ON study_sets(sort_order ASC, id ASC);

        CREATE TABLE IF NOT EXISTS study_set_members (
            set_id INTEGER NOT NULL,
            study_id TEXT NOT NULL,
            UNIQUE(set_id, study_id),
            FOREIGN KEY (set_id) REFERENCES study_sets(id) ON DELETE CASCADE,
            FOREIGN KEY (study_id) REFERENCES studies(study_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_study_set_members_set_id
            ON study_set_members(set_id);
        CREATE INDEX IF NOT EXISTS idx_study_set_members_study_id
            ON study_set_members(study_id);
        """
    )

    set_columns = {row["name"] for row in conn.execute("PRAGMA table_info(study_sets)").fetchall()}
    if "color_token" not in set_columns:
        conn.execute("ALTER TABLE study_sets ADD COLUMN color_token TEXT")

    # Clean legacy orphans from pre-FK setups or manually edited DBs.
    conn.execute(
        """
        DELETE FROM study_set_members
        WHERE set_id NOT IN (SELECT id FROM study_sets)
           OR study_id NOT IN (SELECT study_id FROM studies)
        """
    )


def ensure_analytics_group_cache_tables(
    db_path: Optional[Path] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    if conn is not None:
        _ensure_analytics_group_cache_tables_with_conn(conn)
        return

    path = db_path or _active_db_path
    init_database(db_path=path)
    with sqlite3.connect(
        str(path),
        check_same_thread=False,
        timeout=30.0,
        isolation_level="DEFERRED",
    ) as local_conn:
        _configure_connection(local_conn)
        _ensure_analytics_group_cache_tables_with_conn(local_conn)
        local_conn.commit()


def _ensure_analytics_group_cache_tables_with_conn(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS analytics_group_cache (
            group_key TEXT PRIMARY KEY,
            group_type TEXT NOT NULL CHECK(group_type IN ('all', 'set')),
            set_id INTEGER,
            members_hash TEXT NOT NULL,
            selected_count INTEGER NOT NULL DEFAULT 0,
            curve_json TEXT,
            timestamps_json TEXT,
            return_profile_json TEXT,
            profit_pct REAL,
            max_drawdown_pct REAL,
            ann_profit_pct REAL,
            consistency_full REAL,
            consistency_recent REAL,
            overlap_days INTEGER NOT NULL DEFAULT 0,
            overlap_days_exact REAL NOT NULL DEFAULT 0,
            studies_used INTEGER NOT NULL DEFAULT 0,
            studies_excluded INTEGER NOT NULL DEFAULT 0,
            warning TEXT,
            computed_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (set_id) REFERENCES study_sets(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_analytics_group_cache_set_id
            ON analytics_group_cache(set_id);
        CREATE INDEX IF NOT EXISTS idx_analytics_group_cache_type
            ON analytics_group_cache(group_type);
        """
    )
    existing_columns = {
        str(row["name"] or "")
        for row in conn.execute("PRAGMA table_info(analytics_group_cache)").fetchall()
    }
    if "consistency_full" not in existing_columns:
        conn.execute("ALTER TABLE analytics_group_cache ADD COLUMN consistency_full REAL")
    if "consistency_recent" not in existing_columns:
        conn.execute("ALTER TABLE analytics_group_cache ADD COLUMN consistency_recent REAL")


def _parse_json_array_text(raw_value: Any) -> List[Any]:
    if isinstance(raw_value, list):
        return raw_value
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _parse_json_object_text(raw_value: Any) -> Dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_stitched_oos_metadata(
    raw_curve: Any,
    raw_timestamps: Any,
) -> tuple[Optional[str], Optional[str], int]:
    curve = _parse_json_array_text(raw_curve)
    timestamps = _parse_json_array_text(raw_timestamps)
    if not curve or len(curve) != len(timestamps):
        return None, None, 0

    start_ts = str(timestamps[0] or "").strip()
    end_ts = str(timestamps[-1] or "").strip()
    if not start_ts or not end_ts:
        return None, None, 0
    return start_ts, end_ts, len(timestamps)


def _extract_stitched_oos_metadata_and_consistency(
    raw_curve: Any,
    raw_timestamps: Any,
) -> tuple[Optional[str], Optional[str], int, Optional[float], Optional[float]]:
    curve = _parse_json_array_text(raw_curve)
    timestamps = _parse_json_array_text(raw_timestamps)
    start_ts, end_ts, point_count = _extract_stitched_oos_metadata(curve, timestamps)
    consistency_full, consistency_recent = _compute_cached_curve_consistency_scores(curve, timestamps)
    return start_ts, end_ts, point_count, consistency_full, consistency_recent


def _coerce_finite_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _parse_cache_timestamp(raw_value: Any) -> Optional[datetime]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_curve_for_consistency(
    curve: Any,
    timestamps: Any,
) -> tuple[List[float], List[datetime]]:
    if not isinstance(curve, list) or not isinstance(timestamps, list):
        return [], []
    if len(curve) != len(timestamps):
        return [], []

    normalized_curve: List[float] = []
    normalized_timestamps: List[datetime] = []
    for raw_value, raw_timestamp in zip(curve, timestamps):
        parsed_value = _coerce_finite_float(raw_value)
        parsed_timestamp = _parse_cache_timestamp(raw_timestamp)
        if parsed_value is None or parsed_timestamp is None:
            return [], []
        normalized_curve.append(parsed_value)
        normalized_timestamps.append(parsed_timestamp)

    return normalized_curve, normalized_timestamps


def _round_consistency_value(value: Optional[float]) -> Optional[float]:
    parsed = _coerce_finite_float(value)
    if parsed is None:
        return None
    return round(parsed, 6)


def _compute_cached_curve_consistency_scores(
    curve: Any,
    timestamps: Any,
) -> tuple[Optional[float], Optional[float]]:
    from .metrics import _calculate_r2_consistency

    normalized_curve, normalized_timestamps = _normalize_curve_for_consistency(curve, timestamps)
    if len(normalized_curve) < ANALYTICS_CONSISTENCY_MIN_POINTS:
        return None, None

    full_score = _round_consistency_value(_calculate_r2_consistency(normalized_curve))

    start_ts = normalized_timestamps[0]
    end_ts = normalized_timestamps[-1]
    span = end_ts - start_ts
    if span <= timedelta(0):
        return full_score, None

    recent_start = start_ts + (span * (1.0 - ANALYTICS_CONSISTENCY_RECENT_FRACTION))
    recent_curve = [
        value
        for value, timestamp in zip(normalized_curve, normalized_timestamps)
        if timestamp >= recent_start
    ]
    if len(recent_curve) < ANALYTICS_CONSISTENCY_MIN_POINTS:
        return full_score, None

    recent_score = _round_consistency_value(_calculate_r2_consistency(recent_curve))
    return full_score, recent_score


def backfill_stitched_oos_metadata(
    db_path: Optional[Path] = None,
    *,
    conn: Optional[sqlite3.Connection] = None,
    study_ids: Optional[Sequence[str]] = None,
) -> int:
    if conn is not None:
        return _backfill_stitched_oos_metadata_with_conn(conn, study_ids=study_ids)

    updated_count = 0
    with get_db_connection() as local_conn:
        updated_count = _backfill_stitched_oos_metadata_with_conn(local_conn, study_ids=study_ids)
        if updated_count > 0:
            local_conn.commit()
    return updated_count


def _backfill_stitched_oos_metadata_with_conn(
    conn: sqlite3.Connection,
    *,
    study_ids: Optional[Sequence[str]] = None,
) -> int:
    normalized_ids = [
        str(study_id or "").strip()
        for study_id in (study_ids or [])
        if str(study_id or "").strip()
    ]
    where_clauses = [
        "LOWER(COALESCE(optimization_mode, '')) = 'wfa'",
        "TRIM(COALESCE(stitched_oos_equity_curve, '')) <> ''",
        "TRIM(COALESCE(stitched_oos_timestamps_json, '')) <> ''",
    ]
    params: List[Any] = []
    if normalized_ids:
        placeholders = ", ".join(["?"] * len(normalized_ids))
        where_clauses.append(f"study_id IN ({placeholders})")
        params.extend(normalized_ids)
    where_clauses.append(
        """
        (
            stitched_oos_point_count IS NULL
            OR stitched_oos_start_ts IS NULL
            OR stitched_oos_end_ts IS NULL
            OR stitched_oos_consistency_full IS NULL
        )
        """.strip()
    )

    rows = conn.execute(
        f"""
        SELECT
            study_id,
            stitched_oos_equity_curve,
            stitched_oos_timestamps_json,
            stitched_oos_start_ts,
            stitched_oos_end_ts,
            stitched_oos_point_count,
            stitched_oos_consistency_full,
            stitched_oos_consistency_recent
        FROM studies
        WHERE {" AND ".join(where_clauses)}
        """,
        tuple(params),
    ).fetchall()

    updates: List[tuple[Optional[str], Optional[str], int, Optional[float], Optional[float], str]] = []
    for row in rows:
        start_ts, end_ts, point_count, consistency_full, consistency_recent = (
            _extract_stitched_oos_metadata_and_consistency(
                row["stitched_oos_equity_curve"],
                row["stitched_oos_timestamps_json"],
            )
        )
        existing_start_ts = row["stitched_oos_start_ts"]
        existing_end_ts = row["stitched_oos_end_ts"]
        existing_point_count = int(row["stitched_oos_point_count"] or 0)
        existing_consistency_full = _round_consistency_value(row["stitched_oos_consistency_full"])
        existing_consistency_recent = _round_consistency_value(row["stitched_oos_consistency_recent"])
        if (
            existing_start_ts == start_ts
            and existing_end_ts == end_ts
            and existing_point_count == point_count
            and existing_consistency_full == consistency_full
            and existing_consistency_recent == consistency_recent
        ):
            continue
        updates.append(
            (
                start_ts,
                end_ts,
                point_count,
                consistency_full,
                consistency_recent,
                str(row["study_id"]),
            )
        )

    if not updates:
        return 0

    conn.executemany(
        """
        UPDATE studies
        SET
            stitched_oos_start_ts = ?,
            stitched_oos_end_ts = ?,
            stitched_oos_point_count = ?,
            stitched_oos_consistency_full = ?,
            stitched_oos_consistency_recent = ?
        WHERE study_id = ?
        """,
        updates,
    )
    return len(updates)


def _hash_analytics_group_members(study_ids: Sequence[str]) -> str:
    normalized = sorted(
        {str(study_id or "").strip() for study_id in study_ids if str(study_id or "").strip()}
    )
    return hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()


def _load_all_wfa_study_ids_with_conn(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        """
        SELECT study_id
        FROM studies
        WHERE LOWER(COALESCE(optimization_mode, '')) = 'wfa'
        ORDER BY study_id ASC
        """
    ).fetchall()
    return [str(row["study_id"] or "") for row in rows if row["study_id"]]


def _load_study_curve_rows_by_id_with_conn(
    conn: sqlite3.Connection,
    study_ids: Sequence[str],
    *,
    chunk_size: int = 200,
) -> Dict[str, Dict[str, Any]]:
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    normalized_ids = [str(study_id or "").strip() for study_id in study_ids if str(study_id or "").strip()]
    if not normalized_ids:
        return rows_by_id

    start = 0
    total = len(normalized_ids)
    while start < total:
        chunk = normalized_ids[start : start + max(1, int(chunk_size))]
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"""
            SELECT
                study_id,
                stitched_oos_equity_curve,
                stitched_oos_timestamps_json
            FROM studies
            WHERE study_id IN ({placeholders})
              AND LOWER(COALESCE(optimization_mode, '')) = 'wfa'
            """,
            tuple(chunk),
        )
        for row in cursor.fetchall():
            row_dict = dict(row)
            rows_by_id[str(row_dict.get("study_id") or "")] = row_dict
        start += max(1, int(chunk_size))

    return rows_by_id


def _compute_analytics_group_result_with_conn(
    conn: sqlite3.Connection,
    study_ids: Sequence[str],
) -> Dict[str, Any]:
    from .analytics import aggregate_equity_curves

    normalized_ids = [str(study_id or "").strip() for study_id in study_ids if str(study_id or "").strip()]
    if not normalized_ids:
        result = dict(aggregate_equity_curves([]))
        result["selected_count"] = 0
        result["missing_study_ids"] = []
        return result

    rows_by_id = _load_study_curve_rows_by_id_with_conn(conn, normalized_ids)
    studies_data: List[Dict[str, Any]] = []
    missing_study_ids: List[str] = []

    for study_id in normalized_ids:
        row = rows_by_id.get(study_id)
        if row is None:
            missing_study_ids.append(study_id)
            continue
        studies_data.append(
            {
                "equity_curve": _parse_json_array_text(row.get("stitched_oos_equity_curve")),
                "timestamps": _parse_json_array_text(row.get("stitched_oos_timestamps_json")),
            }
        )

    result = dict(aggregate_equity_curves(studies_data))
    if missing_study_ids:
        result["studies_excluded"] = int(result.get("studies_excluded") or 0) + len(missing_study_ids)
        warning = str(result.get("warning") or "").strip()
        missing_note = f"{len(missing_study_ids)} selected studies were not found."
        result["warning"] = f"{warning} {missing_note}".strip() if warning else missing_note

    result["selected_count"] = len(normalized_ids)
    result["missing_study_ids"] = missing_study_ids
    consistency_full, consistency_recent = _compute_cached_curve_consistency_scores(
        result.get("curve"),
        result.get("timestamps"),
    )
    result["consistency_full"] = consistency_full
    result["consistency_recent"] = consistency_recent
    return result


def _normalize_analytics_group_cache_payload(row: sqlite3.Row | Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row)
    payload["curve"] = _parse_json_array_text(payload.get("curve_json"))
    payload["timestamps"] = _parse_json_array_text(payload.get("timestamps_json"))
    payload["return_profile"] = _parse_json_object_text(payload.get("return_profile_json"))
    payload["selected_count"] = int(payload.get("selected_count") or 0)
    payload["overlap_days"] = int(payload.get("overlap_days") or 0)
    payload["overlap_days_exact"] = float(payload.get("overlap_days_exact") or 0.0)
    payload["studies_used"] = int(payload.get("studies_used") or 0)
    payload["studies_excluded"] = int(payload.get("studies_excluded") or 0)
    payload["missing_study_ids"] = []
    payload["has_curve"] = bool(payload["curve"] and len(payload["curve"]) == len(payload["timestamps"]))
    payload["consistency_full"] = _round_consistency_value(payload.get("consistency_full"))
    payload["consistency_recent"] = _round_consistency_value(payload.get("consistency_recent"))
    if payload["consistency_full"] is None or payload["consistency_recent"] is None:
        computed_full, computed_recent = _compute_cached_curve_consistency_scores(
            payload["curve"],
            payload["timestamps"],
        )
        if payload["consistency_full"] is None:
            payload["consistency_full"] = computed_full
        if payload["consistency_recent"] is None:
            payload["consistency_recent"] = computed_recent
    payload.pop("curve_json", None)
    payload.pop("timestamps_json", None)
    payload.pop("return_profile_json", None)
    return payload


def _analytics_group_cache_summary_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    curve = payload.get("curve")
    timestamps = payload.get("timestamps")
    has_curve = bool(
        isinstance(curve, list)
        and isinstance(timestamps, list)
        and len(curve) > 0
        and len(curve) == len(timestamps)
    )
    return {
        "ann_profit_pct": payload.get("ann_profit_pct"),
        "profit_pct": payload.get("profit_pct"),
        "max_drawdown_pct": payload.get("max_drawdown_pct"),
        "consistency_full": payload.get("consistency_full"),
        "consistency_recent": payload.get("consistency_recent"),
        "overlap_days": payload.get("overlap_days"),
        "overlap_days_exact": payload.get("overlap_days_exact"),
        "studies_used": payload.get("studies_used"),
        "studies_excluded": payload.get("studies_excluded"),
        "selected_count": payload.get("selected_count"),
        "warning": payload.get("warning"),
        "computed_at": payload.get("computed_at"),
        "has_curve": has_curve,
        "curve_point_count": len(curve) if isinstance(curve, list) else 0,
    }


def _upsert_analytics_group_cache_with_conn(
    conn: sqlite3.Connection,
    *,
    group_key: str,
    group_type: str,
    set_id: Optional[int],
    study_ids: Sequence[str],
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_ids = [str(study_id or "").strip() for study_id in study_ids if str(study_id or "").strip()]
    consistency_full = _round_consistency_value(payload.get("consistency_full"))
    consistency_recent = _round_consistency_value(payload.get("consistency_recent"))
    if consistency_full is None or consistency_recent is None:
        computed_full, computed_recent = _compute_cached_curve_consistency_scores(
            payload.get("curve"),
            payload.get("timestamps"),
        )
        if consistency_full is None:
            consistency_full = computed_full
        if consistency_recent is None:
            consistency_recent = computed_recent
    stored_payload = {
        "group_key": group_key,
        "group_type": group_type,
        "set_id": set_id,
        "members_hash": _hash_analytics_group_members(normalized_ids),
        "selected_count": len(normalized_ids),
        "curve_json": (
            json.dumps(payload.get("curve"))
            if isinstance(payload.get("curve"), list)
            else None
        ),
        "timestamps_json": (
            json.dumps(payload.get("timestamps"))
            if isinstance(payload.get("timestamps"), list)
            else None
        ),
        "return_profile_json": json.dumps(payload.get("return_profile") or {}),
        "profit_pct": payload.get("profit_pct"),
        "max_drawdown_pct": payload.get("max_drawdown_pct"),
        "ann_profit_pct": payload.get("ann_profit_pct"),
        "consistency_full": consistency_full,
        "consistency_recent": consistency_recent,
        "overlap_days": int(payload.get("overlap_days") or 0),
        "overlap_days_exact": float(payload.get("overlap_days_exact") or 0.0),
        "studies_used": int(payload.get("studies_used") or 0),
        "studies_excluded": int(payload.get("studies_excluded") or 0),
        "warning": payload.get("warning"),
        "computed_at": _utc_now_iso(),
    }
    conn.execute(
        """
        INSERT INTO analytics_group_cache (
            group_key,
            group_type,
            set_id,
            members_hash,
            selected_count,
            curve_json,
            timestamps_json,
            return_profile_json,
            profit_pct,
            max_drawdown_pct,
            ann_profit_pct,
            consistency_full,
            consistency_recent,
            overlap_days,
            overlap_days_exact,
            studies_used,
            studies_excluded,
            warning,
            computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(group_key) DO UPDATE SET
            group_type = excluded.group_type,
            set_id = excluded.set_id,
            members_hash = excluded.members_hash,
            selected_count = excluded.selected_count,
            curve_json = excluded.curve_json,
            timestamps_json = excluded.timestamps_json,
            return_profile_json = excluded.return_profile_json,
            profit_pct = excluded.profit_pct,
            max_drawdown_pct = excluded.max_drawdown_pct,
            ann_profit_pct = excluded.ann_profit_pct,
            consistency_full = excluded.consistency_full,
            consistency_recent = excluded.consistency_recent,
            overlap_days = excluded.overlap_days,
            overlap_days_exact = excluded.overlap_days_exact,
            studies_used = excluded.studies_used,
            studies_excluded = excluded.studies_excluded,
            warning = excluded.warning,
            computed_at = excluded.computed_at
        """,
        (
            stored_payload["group_key"],
            stored_payload["group_type"],
            stored_payload["set_id"],
            stored_payload["members_hash"],
            stored_payload["selected_count"],
            stored_payload["curve_json"],
            stored_payload["timestamps_json"],
            stored_payload["return_profile_json"],
            stored_payload["profit_pct"],
            stored_payload["max_drawdown_pct"],
            stored_payload["ann_profit_pct"],
            stored_payload["consistency_full"],
            stored_payload["consistency_recent"],
            stored_payload["overlap_days"],
            stored_payload["overlap_days_exact"],
            stored_payload["studies_used"],
            stored_payload["studies_excluded"],
            stored_payload["warning"],
            stored_payload["computed_at"],
        ),
    )
    return _normalize_analytics_group_cache_payload(stored_payload)


def _get_or_build_analytics_group_cache_with_conn(
    conn: sqlite3.Connection,
    *,
    group_key: str,
    group_type: str,
    study_ids: Sequence[str],
    set_id: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_analytics_group_cache_tables(conn=conn)
    normalized_ids = [str(study_id or "").strip() for study_id in study_ids if str(study_id or "").strip()]
    members_hash = _hash_analytics_group_members(normalized_ids)
    row = conn.execute(
        "SELECT * FROM analytics_group_cache WHERE group_key = ?",
        (group_key,),
    ).fetchone()
    if row is not None:
        cached = _normalize_analytics_group_cache_payload(row)
        if (
            str(cached.get("members_hash") or "") == members_hash
            and int(cached.get("selected_count") or 0) == len(normalized_ids)
        ):
            return cached

    computed = _compute_analytics_group_result_with_conn(conn, normalized_ids)
    return _upsert_analytics_group_cache_with_conn(
        conn,
        group_key=group_key,
        group_type=group_type,
        set_id=set_id,
        study_ids=normalized_ids,
        payload=computed,
    )


def invalidate_analytics_group_cache_for_set_ids(
    set_ids: Sequence[int],
    *,
    conn: sqlite3.Connection,
) -> None:
    normalized_ids = [int(set_id) for set_id in set_ids if int(set_id) > 0]
    if not normalized_ids:
        return
    ensure_analytics_group_cache_tables(conn=conn)
    placeholders = ",".join("?" for _ in normalized_ids)
    conn.execute(
        f"DELETE FROM analytics_group_cache WHERE set_id IN ({placeholders})",
        tuple(normalized_ids),
    )


def invalidate_all_studies_analytics_cache(*, conn: sqlite3.Connection) -> None:
    ensure_analytics_group_cache_tables(conn=conn)
    conn.execute(
        "DELETE FROM analytics_group_cache WHERE group_key = ?",
        (ANALYTICS_GROUP_KEY_ALL,),
    )


def invalidate_analytics_group_cache_for_study(
    study_id: str,
    *,
    conn: sqlite3.Connection,
) -> None:
    ensure_analytics_group_cache_tables(conn=conn)
    affected_rows = conn.execute(
        """
        SELECT DISTINCT set_id
        FROM study_set_members
        WHERE study_id = ?
        """,
        (study_id,),
    ).fetchall()
    affected_set_ids = [int(row["set_id"]) for row in affected_rows if row["set_id"] is not None]
    invalidate_all_studies_analytics_cache(conn=conn)
    invalidate_analytics_group_cache_for_set_ids(affected_set_ids, conn=conn)


def get_or_build_all_studies_analytics_cache(
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    _ = db_path
    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        backfill_stitched_oos_metadata(conn=conn)
        payload = _get_or_build_analytics_group_cache_with_conn(
            conn,
            group_key=ANALYTICS_GROUP_KEY_ALL,
            group_type=ANALYTICS_GROUP_TYPE_ALL,
            study_ids=_load_all_wfa_study_ids_with_conn(conn),
            set_id=None,
        )
        conn.commit()
        return payload


def get_or_build_study_set_analytics_cache(
    set_id: int,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    _ = db_path
    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        backfill_stitched_oos_metadata(conn=conn)
        study_set = _load_study_set_by_id(conn, int(set_id))
        if study_set is None:
            raise ValueError("Study set not found.")
        payload = _get_or_build_analytics_group_cache_with_conn(
            conn,
            group_key=f"set:{int(set_id)}",
            group_type=ANALYTICS_GROUP_TYPE_SET,
            study_ids=study_set.get("study_ids") or [],
            set_id=int(set_id),
        )
        conn.commit()
        return payload


def _normalize_study_set_name(name: Any) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        raise ValueError("Set name cannot be empty.")
    if len(normalized) > 120:
        raise ValueError("Set name is too long (max 120 characters).")
    return normalized


def _study_set_name_with_suffix(base_name: str, suffix: int) -> str:
    candidate = str(base_name or "").strip()
    if suffix <= 0:
        return candidate

    suffix_text = f" ({int(suffix)})"
    if len(candidate) + len(suffix_text) <= 120:
        return f"{candidate}{suffix_text}"

    trimmed = candidate[: max(1, 120 - len(suffix_text))].rstrip()
    return f"{trimmed}{suffix_text}"


def _run_study_set_name_operation_with_suffix(
    operation: Callable[[str], Any],
    base_name: str,
    *,
    max_attempts: int = 1000,
) -> tuple[Any, str]:
    normalized_name = _normalize_study_set_name(base_name)
    for suffix in range(max_attempts + 1):
        candidate_name = _study_set_name_with_suffix(normalized_name, suffix)
        try:
            return operation(candidate_name), candidate_name
        except sqlite3.IntegrityError:
            if suffix >= max_attempts:
                raise ValueError("Failed to resolve unique set name.") from None

    raise RuntimeError("Failed to resolve unique set name.")


_STUDY_SET_COLOR_TOKENS = (
    "blue",
    "teal",
    "mint",
    "olive",
    "sand",
    "amber",
    "rose",
    "lavender",
)
_STUDY_SET_COLOR_TOKEN_SET = frozenset(_STUDY_SET_COLOR_TOKENS)
_STUDY_SET_FIELD_UNSET = object()


def _coerce_study_set_color_token(color_token: Any) -> Optional[str]:
    token = str(color_token or "").strip().lower()
    if not token:
        return None
    return token if token in _STUDY_SET_COLOR_TOKEN_SET else None


def _normalize_study_set_color_token(color_token: Any) -> Optional[str]:
    token = str(color_token or "").strip().lower()
    if not token:
        return None
    if token not in _STUDY_SET_COLOR_TOKEN_SET:
        allowed = ", ".join(_STUDY_SET_COLOR_TOKENS)
        raise ValueError(f"Unsupported set color. Allowed values: {allowed}.")
    return token


def _normalize_set_study_ids(study_ids: Any) -> List[str]:
    if study_ids is None:
        return []
    if not isinstance(study_ids, (list, tuple, set)):
        raise ValueError("study_ids must be an array.")

    seen = set()
    normalized: List[str] = []
    for raw in study_ids:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _normalize_study_set_ids(set_ids: Any) -> List[int]:
    if not isinstance(set_ids, (list, tuple, set)):
        raise ValueError("set_ids must be an array of set IDs.")

    normalized: List[int] = []
    seen = set()
    for raw in set_ids:
        try:
            set_id = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("set_ids must contain integer set IDs.") from exc
        if set_id <= 0:
            raise ValueError("set_ids must contain positive set IDs.")
        if set_id in seen:
            raise ValueError("set_ids contains duplicate set IDs.")
        seen.add(set_id)
        normalized.append(set_id)

    if not normalized:
        raise ValueError("set_ids must contain at least one set ID.")
    return normalized


def _validate_wfa_study_ids(conn: sqlite3.Connection, study_ids: Sequence[str]) -> None:
    ids = [str(value or "").strip() for value in study_ids if str(value or "").strip()]
    if not ids:
        return

    placeholders = ",".join(["?"] * len(ids))
    rows = conn.execute(
        f"""
        SELECT study_id
        FROM studies
        WHERE study_id IN ({placeholders})
          AND LOWER(COALESCE(optimization_mode, '')) = 'wfa'
        """,
        tuple(ids),
    ).fetchall()
    existing = {str(row["study_id"]) for row in rows}
    missing = [value for value in ids if value not in existing]
    if missing:
        preview = ", ".join(missing[:5])
        suffix = "..." if len(missing) > 5 else ""
        raise ValueError(f"Unknown or non-WFA study IDs: {preview}{suffix}")


def _load_study_set_by_id(conn: sqlite3.Connection, set_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, name, sort_order, created_at, color_token
        FROM study_sets
        WHERE id = ?
        """,
        (int(set_id),),
    ).fetchone()
    if row is None:
        return None

    member_rows = conn.execute(
        """
        SELECT study_id
        FROM study_set_members
        WHERE set_id = ?
        ORDER BY rowid ASC
        """,
        (int(set_id),),
    ).fetchall()
    return {
        "id": int(row["id"]),
        "name": str(row["name"] or ""),
        "sort_order": int(row["sort_order"] or 0),
        "created_at": row["created_at"],
        "color_token": _coerce_study_set_color_token(row["color_token"]),
        "study_ids": [str(member["study_id"] or "") for member in member_rows if member["study_id"]],
    }


def _ensure_study_set_ids_exist(conn: sqlite3.Connection, set_ids: Sequence[int]) -> None:
    normalized_ids = [int(set_id) for set_id in set_ids]
    if not normalized_ids:
        raise ValueError("set_ids must contain at least one set ID.")

    placeholders = ",".join(["?"] * len(normalized_ids))
    rows = conn.execute(
        f"""
        SELECT id
        FROM study_sets
        WHERE id IN ({placeholders})
        """,
        tuple(normalized_ids),
    ).fetchall()
    existing_ids = {int(row["id"]) for row in rows}
    missing_ids = [set_id for set_id in normalized_ids if set_id not in existing_ids]
    if missing_ids:
        missing_preview = ", ".join(str(set_id) for set_id in missing_ids[:5])
        suffix = "..." if len(missing_ids) > 5 else ""
        raise ValueError(f"Unknown study set IDs: {missing_preview}{suffix}")


def _list_study_sets_with_conn(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    ensure_study_sets_tables(conn=conn)
    set_rows = conn.execute(
        """
        SELECT id, name, sort_order, created_at, color_token
        FROM study_sets
        ORDER BY sort_order ASC, id ASC
        """
    ).fetchall()
    member_rows = conn.execute(
        """
        SELECT set_id, study_id
        FROM study_set_members
        ORDER BY set_id ASC, rowid ASC
        """
    ).fetchall()

    members_by_set: Dict[int, List[str]] = {}
    for row in member_rows:
        set_id = int(row["set_id"])
        study_id = str(row["study_id"] or "").strip()
        if not study_id:
            continue
        members_by_set.setdefault(set_id, []).append(study_id)

    return [
        {
            "id": int(row["id"]),
            "name": str(row["name"] or ""),
            "sort_order": int(row["sort_order"] or 0),
            "created_at": row["created_at"],
            "color_token": _coerce_study_set_color_token(row["color_token"]),
            "study_ids": members_by_set.get(int(row["id"]), []),
        }
        for row in set_rows
    ]


def list_study_sets(db_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    _ = db_path
    with get_db_connection() as conn:
        return _list_study_sets_with_conn(conn)


def list_study_sets_with_analytics_cache(
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    _ = db_path
    with get_db_connection() as conn:
        backfill_stitched_oos_metadata(conn=conn)
        study_sets = _list_study_sets_with_conn(conn)
        all_metrics = _get_or_build_analytics_group_cache_with_conn(
            conn,
            group_key=ANALYTICS_GROUP_KEY_ALL,
            group_type=ANALYTICS_GROUP_TYPE_ALL,
            study_ids=_load_all_wfa_study_ids_with_conn(conn),
            set_id=None,
        )
        enriched_sets = []
        for study_set in study_sets:
            metrics = _get_or_build_analytics_group_cache_with_conn(
                conn,
                group_key=f"set:{int(study_set['id'])}",
                group_type=ANALYTICS_GROUP_TYPE_SET,
                study_ids=study_set.get("study_ids") or [],
                set_id=int(study_set["id"]),
            )
            enriched_sets.append(
                {
                    **study_set,
                    "metrics": _analytics_group_cache_summary_payload(metrics),
                }
            )

        conn.commit()
        return {
            "sets": enriched_sets,
            "all_metrics": _analytics_group_cache_summary_payload(all_metrics),
        }


def load_study_analytics_equity(
    study_id: str,
    db_path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    _ = db_path
    normalized_study_id = str(study_id or "").strip()
    if not normalized_study_id:
        return None

    with get_db_connection() as conn:
        backfill_stitched_oos_metadata(conn=conn)
        row = conn.execute(
            """
            SELECT
                study_id,
                optimization_mode,
                stitched_oos_equity_curve,
                stitched_oos_timestamps_json,
                stitched_oos_start_ts,
                stitched_oos_end_ts,
                stitched_oos_point_count
            FROM studies
            WHERE study_id = ?
            """,
            (normalized_study_id,),
        ).fetchone()
        conn.commit()

    if row is None:
        return None

    payload = dict(row)
    payload["curve"] = _parse_json_array_text(payload.get("stitched_oos_equity_curve"))
    payload["timestamps"] = _parse_json_array_text(payload.get("stitched_oos_timestamps_json"))
    point_count = int(payload.get("stitched_oos_point_count") or 0)
    payload["point_count"] = point_count
    payload["has_equity_curve"] = bool(
        point_count > 0
        and len(payload["curve"]) == len(payload["timestamps"])
    )
    return payload


def create_study_set(
    name: Any,
    study_ids: Any,
    db_path: Optional[Path] = None,
    *,
    color_token: Any = None,
) -> Dict[str, Any]:
    normalized_ids = _normalize_set_study_ids(study_ids)
    normalized_color = _normalize_study_set_color_token(color_token)
    if not normalized_ids:
        raise ValueError("Set must contain at least one study_id.")

    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        ensure_analytics_group_cache_tables(conn=conn)
        _validate_wfa_study_ids(conn, normalized_ids)

        next_order_row = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 AS next_order FROM study_sets"
        ).fetchone()
        next_order = int(next_order_row["next_order"] if next_order_row else 0)

        cursor, _created_name = _run_study_set_name_operation_with_suffix(
            lambda candidate_name: conn.execute(
                """
                INSERT INTO study_sets (name, sort_order, color_token)
                VALUES (?, ?, ?)
                """,
                (candidate_name, next_order, normalized_color),
            ),
            name,
        )

        if cursor is None:
            raise RuntimeError("Failed to create study set.")
        set_id = int(cursor.lastrowid)
        conn.executemany(
            """
            INSERT INTO study_set_members (set_id, study_id)
            VALUES (?, ?)
            """,
            [(set_id, study_id) for study_id in normalized_ids],
        )
        conn.commit()
        created = _load_study_set_by_id(conn, set_id)

    if created is None:
        raise RuntimeError("Failed to load created study set.")
    return created


def update_study_set(
    set_id: int,
    name: Any = None,
    study_ids: Any = None,
    sort_order: Any = None,
    db_path: Optional[Path] = None,
    *,
    color_token: Any = _STUDY_SET_FIELD_UNSET,
) -> Dict[str, Any]:
    set_id_int = int(set_id)

    if (
        name is None
        and study_ids is None
        and sort_order is None
        and color_token is _STUDY_SET_FIELD_UNSET
    ):
        raise ValueError("No fields provided to update.")

    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        ensure_analytics_group_cache_tables(conn=conn)
        current = _load_study_set_by_id(conn, set_id_int)
        if current is None:
            raise ValueError("Study set not found.")

        if name is not None:
            _updated_name_cursor, _updated_name = _run_study_set_name_operation_with_suffix(
                lambda candidate_name: conn.execute(
                    "UPDATE study_sets SET name = ? WHERE id = ?",
                    (candidate_name, set_id_int),
                ),
                name,
            )

        if sort_order is not None:
            try:
                sort_value = int(sort_order)
            except (TypeError, ValueError) as exc:
                raise ValueError("sort_order must be an integer.") from exc
            if sort_value < 0:
                raise ValueError("sort_order cannot be negative.")
            conn.execute(
                "UPDATE study_sets SET sort_order = ? WHERE id = ?",
                (sort_value, set_id_int),
            )

        if color_token is not _STUDY_SET_FIELD_UNSET:
            normalized_color = _normalize_study_set_color_token(color_token)
            conn.execute(
                "UPDATE study_sets SET color_token = ? WHERE id = ?",
                (normalized_color, set_id_int),
            )

        if study_ids is not None:
            normalized_ids = _normalize_set_study_ids(study_ids)
            _validate_wfa_study_ids(conn, normalized_ids)
            conn.execute("DELETE FROM study_set_members WHERE set_id = ?", (set_id_int,))
            if normalized_ids:
                conn.executemany(
                    """
                    INSERT INTO study_set_members (set_id, study_id)
                    VALUES (?, ?)
                    """,
                    [(set_id_int, study_id) for study_id in normalized_ids],
                )
            invalidate_analytics_group_cache_for_set_ids([set_id_int], conn=conn)

        conn.commit()
        updated = _load_study_set_by_id(conn, set_id_int)

    if updated is None:
        raise RuntimeError("Failed to load updated study set.")
    return updated


def delete_study_set(set_id: int, db_path: Optional[Path] = None) -> bool:
    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        ensure_analytics_group_cache_tables(conn=conn)
        cursor = conn.execute("DELETE FROM study_sets WHERE id = ?", (int(set_id),))
        conn.commit()
        return cursor.rowcount > 0


def delete_study_sets(set_ids: Any, db_path: Optional[Path] = None) -> int:
    normalized_ids = _normalize_study_set_ids(set_ids)

    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        ensure_analytics_group_cache_tables(conn=conn)
        _ensure_study_set_ids_exist(conn, normalized_ids)
        conn.executemany(
            "DELETE FROM study_sets WHERE id = ?",
            [(set_id,) for set_id in normalized_ids],
        )
        conn.commit()
    return len(normalized_ids)


def update_study_sets_color(
    set_ids: Any,
    color_token: Any,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    normalized_ids = _normalize_study_set_ids(set_ids)
    normalized_color = _normalize_study_set_color_token(color_token)

    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        _ensure_study_set_ids_exist(conn, normalized_ids)
        conn.executemany(
            "UPDATE study_sets SET color_token = ? WHERE id = ?",
            [(normalized_color, set_id) for set_id in normalized_ids],
        )
        conn.commit()
        updated = [
            _load_study_set_by_id(conn, set_id)
            for set_id in normalized_ids
        ]

    return [item for item in updated if item is not None]


def reorder_study_sets(id_order: Any, db_path: Optional[Path] = None) -> None:
    if not isinstance(id_order, (list, tuple)):
        raise ValueError("order must be an array of set IDs.")

    normalized_order: List[int] = []
    seen = set()
    for raw in id_order:
        try:
            set_id = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("order must contain integer set IDs.") from exc
        if set_id in seen:
            raise ValueError("order contains duplicate set IDs.")
        seen.add(set_id)
        normalized_order.append(set_id)

    with get_db_connection() as conn:
        ensure_study_sets_tables(conn=conn)
        rows = conn.execute("SELECT id FROM study_sets ORDER BY id ASC").fetchall()
        existing_ids = [int(row["id"]) for row in rows]

        if len(existing_ids) != len(normalized_order) or set(existing_ids) != set(normalized_order):
            raise ValueError("order must contain all existing set IDs exactly once.")

        conn.executemany(
            """
            UPDATE study_sets
            SET sort_order = ?
            WHERE id = ?
            """,
            [(index, set_id) for index, set_id in enumerate(normalized_order)],
        )
        conn.commit()


def generate_study_id() -> str:
    return str(uuid.uuid4())


def generate_study_name(
    strategy_id: str,
    csv_filename: str,
    start_date,
    end_date,
    mode: str,
) -> str:
    match = re.match(r"s(\d+)_", strategy_id)
    prefix = f"S{match.group(1).zfill(2)}" if match else strategy_id.upper()[:3]

    ticker_tf = _extract_file_prefix(csv_filename or "")

    if hasattr(start_date, "strftime"):
        start_str = start_date.strftime("%Y.%m.%d")
    else:
        start_str = str(start_date)[:10].replace("-", ".") if start_date else "0000.00.00"

    if hasattr(end_date, "strftime"):
        end_str = end_date.strftime("%Y.%m.%d")
    else:
        end_str = str(end_date)[:10].replace("-", ".") if end_date else "0000.00.00"

    mode_suffix = "WFA" if str(mode).lower() == "wfa" else "OPT"
    base_name = f"{prefix}_{ticker_tf} {start_str}-{end_str}_{mode_suffix}"

    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT study_name FROM studies WHERE study_name LIKE ? ORDER BY study_name",
            (f"{base_name}%",),
        )
        existing = {row[0] for row in cursor.fetchall()}

    if base_name not in existing:
        return base_name

    counter = 1
    while f"{base_name} ({counter})" in existing:
        counter += 1
    return f"{base_name} ({counter})"


def _extract_file_prefix(csv_filename: str) -> str:
    name = Path(csv_filename).stem
    date_pattern = re.compile(r"\b\d{4}[.\-/]\d{2}[.\-/]\d{2}\b")
    match = date_pattern.search(name)
    if match:
        prefix = name[:match.start()].rstrip()
        return prefix if prefix else name
    return name or "dataset"


def _get_csv_display_name(config: Any, csv_file_path: str) -> str:
    if isinstance(config, dict):
        name = config.get("csv_original_name") or config.get("csv_file_name") or ""
        if name:
            return str(Path(name).name)
    name = getattr(config, "csv_original_name", None)
    if name:
        return str(Path(name).name)
    if csv_file_path:
        return str(Path(csv_file_path).name)
    return "upload"


def save_optuna_study_to_db(
    study,
    config,
    optuna_config,
    trial_results: List,
    csv_file_path: str,
    start_time: float,
    score_config: Optional[Dict] = None,
) -> str:
    import pandas as pd
    from optuna.trial import TrialState

    init_database()

    study_id = generate_study_id()

    start_date = config.fixed_params.get("start") or pd.Timestamp.now(tz="UTC")
    end_date = config.fixed_params.get("end") or pd.Timestamp.now(tz="UTC")

    csv_display_name = _get_csv_display_name(config, csv_file_path)

    study_name = generate_study_name(
        strategy_id=config.strategy_id,
        csv_filename=csv_display_name,
        start_date=start_date,
        end_date=end_date,
        mode="optuna",
    )

    strategy_version = None
    try:
        from strategies import get_strategy

        strategy_class = get_strategy(config.strategy_id)
        strategy_version = getattr(strategy_class, "STRATEGY_VERSION", None)
    except Exception:
        pass

    resolved_score_config = score_config or getattr(config, "score_config", None) or {}
    filter_score_enabled = bool(resolved_score_config.get("filter_enabled", False))
    try:
        filter_score_threshold = float(resolved_score_config.get("min_score_threshold", 0.0))
    except (TypeError, ValueError):
        filter_score_threshold = 0.0

    filtered_results = list(trial_results or [])

    if filter_score_enabled:
        filtered_results = [r for r in filtered_results if float(r.score) >= filter_score_threshold]

    if getattr(config, "filter_min_profit", False):
        threshold = float(getattr(config, "min_profit_threshold", 0.0) or 0.0)
        filtered_results = [r for r in filtered_results if float(r.net_profit_pct) >= threshold]

    best_result = filtered_results[0] if filtered_results else None

    completed_trials = len(trial_results or [])
    pruned_trials = 0
    total_trials = completed_trials

    if study is not None:
        try:
            effective_trials = [
                t for t in study.trials if not bool((getattr(t, "user_attrs", {}) or {}).get("merlin.duplicate_skipped", False))
            ]
            completed_trials = sum(1 for t in effective_trials if t.state == TrialState.COMPLETE)
            pruned_trials = sum(1 for t in effective_trials if t.state == TrialState.PRUNED)
            total_trials = len(effective_trials)
        except Exception:
            completed_trials = len(trial_results or [])
            pruned_trials = 0
            total_trials = completed_trials

    objectives = list(getattr(optuna_config, "objectives", None) or [])
    if not objectives:
        objectives = ["net_profit_pct"]

    directions = None
    if study is not None:
        try:
            directions = [str(d).lower() for d in study.directions]
        except Exception:
            directions = None

    primary_objective = getattr(optuna_config, "primary_objective", None)
    constraints_payload = []
    for spec in getattr(optuna_config, "constraints", []) or []:
        if isinstance(spec, dict):
            constraints_payload.append(spec)
        else:
            constraints_payload.append(
                {
                    "metric": getattr(spec, "metric", None),
                    "threshold": getattr(spec, "threshold", None),
                    "enabled": bool(getattr(spec, "enabled", False)),
                }
            )

    sampler_cfg = getattr(optuna_config, "sampler_config", None)
    if isinstance(sampler_cfg, dict):
        sampler_payload = sampler_cfg
    else:
        sampler_payload = {
            "sampler_type": getattr(sampler_cfg, "sampler_type", None),
            "population_size": getattr(sampler_cfg, "population_size", None),
            "crossover_prob": getattr(sampler_cfg, "crossover_prob", None),
            "mutation_prob": getattr(sampler_cfg, "mutation_prob", None),
            "swapping_prob": getattr(sampler_cfg, "swapping_prob", None),
            "n_startup_trials": getattr(sampler_cfg, "n_startup_trials", None),
        }

    best_value = None
    best_values_json = None
    if best_result is not None and getattr(best_result, "objective_values", None):
        if len(objectives) > 1:
            best_values_json = json.dumps(
                dict(zip(objectives, list(best_result.objective_values)))
            )
        else:
            best_value = float(best_result.objective_values[0])

    pareto_front_size = sum(
        1 for r in filtered_results if getattr(r, "is_pareto_optimal", False)
    ) if len(objectives) > 1 else None

    summary = getattr(config, "optuna_summary", None) or {}
    optimization_time_seconds = summary.get("optimization_time_seconds")
    if optimization_time_seconds is None and start_time:
        optimization_time_seconds = max(0, time.time() - float(start_time))
    try:
        optimization_time_seconds = int(round(float(optimization_time_seconds))) if optimization_time_seconds is not None else None
    except (TypeError, ValueError):
        optimization_time_seconds = None

    config_payload = _safe_dict(config)
    if optuna_config is not None:
        config_payload["optuna_config"] = _safe_dict(optuna_config)

    ft_enabled = int(getattr(config, "ft_enabled", 0) or 0)
    ft_period_days = getattr(config, "ft_period_days", None)
    ft_top_k = getattr(config, "ft_top_k", None)
    ft_sort_metric = getattr(config, "ft_sort_metric", None)
    ft_threshold_pct = getattr(config, "ft_threshold_pct", None)
    ft_reject_action = getattr(config, "ft_reject_action", None)
    ft_reject_cooldown_days = getattr(config, "ft_reject_cooldown_days", None)
    ft_reject_max_attempts = getattr(config, "ft_reject_max_attempts", None)
    ft_reject_min_remaining_oos_days = getattr(
        config, "ft_reject_min_remaining_oos_days", None
    )
    ft_start_date = getattr(config, "ft_start_date", None)
    ft_end_date = getattr(config, "ft_end_date", None)
    is_period_days = getattr(config, "is_period_days", None)

    with get_db_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            _ensure_wfa_schema_updated(conn)
            conn.execute(
                """
                INSERT INTO studies (
                    study_id, study_name, strategy_id, strategy_version,
                    optimization_mode,
                    objectives_json, n_objectives, directions_json, primary_objective,
                    constraints_json,
                    sampler_type, population_size, crossover_prob, mutation_prob, swapping_prob,
                    budget_mode, n_trials, time_limit, convergence_patience,
                    total_trials, completed_trials, pruned_trials, pareto_front_size,
                    best_value, best_values_json,
                    score_config_json, config_json,
                    csv_file_path, csv_file_name,
                    dataset_start_date, dataset_end_date, warmup_bars,
                    ft_enabled, ft_period_days, ft_top_k, ft_sort_metric,
                    ft_threshold_pct, ft_reject_action, ft_reject_cooldown_days,
                    ft_reject_max_attempts, ft_reject_min_remaining_oos_days,
                    ft_start_date, ft_end_date, is_period_days,
                    optimization_time_seconds,
                    completed_at,
                    filter_min_profit, min_profit_threshold,
                    sanitize_enabled, sanitize_trades_threshold
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    study_id,
                    study_name,
                    config.strategy_id,
                    strategy_version,
                    "optuna",
                    json.dumps(objectives) if objectives else None,
                    len(objectives),
                    json.dumps(directions) if directions else None,
                    primary_objective,
                    json.dumps(constraints_payload) if constraints_payload else None,
                    sampler_payload.get("sampler_type") or "tpe",
                    sampler_payload.get("population_size"),
                    sampler_payload.get("crossover_prob"),
                    sampler_payload.get("mutation_prob"),
                    sampler_payload.get("swapping_prob"),
                    getattr(optuna_config, "budget_mode", None),
                    getattr(optuna_config, "n_trials", None),
                    getattr(optuna_config, "time_limit", None),
                    getattr(optuna_config, "convergence_patience", None),
                    total_trials,
                    completed_trials,
                    pruned_trials,
                    pareto_front_size,
                    best_value,
                    best_values_json,
                    json.dumps(resolved_score_config) if resolved_score_config else None,
                    json.dumps(config_payload),
                    str(Path(csv_file_path).resolve()) if csv_file_path else "",
                    csv_display_name,
                    _format_date(start_date),
                    _format_date(end_date),
                    getattr(config, "warmup_bars", None),
                    ft_enabled,
                    ft_period_days,
                    ft_top_k,
                    ft_sort_metric,
                    ft_threshold_pct,
                    ft_reject_action,
                    ft_reject_cooldown_days,
                    ft_reject_max_attempts,
                    ft_reject_min_remaining_oos_days,
                    _format_date(ft_start_date),
                    _format_date(ft_end_date),
                    is_period_days,
                    optimization_time_seconds,
                    _utc_now_iso(),
                    1 if getattr(config, "filter_min_profit", False) else 0,
                    getattr(config, "min_profit_threshold", None)
                    if getattr(config, "filter_min_profit", False)
                    else None,
                    1 if getattr(optuna_config, "sanitize_enabled", True) else 0,
                    int(getattr(optuna_config, "sanitize_trades_threshold", 0) or 0),
                ),
            )

            trial_rows = []
            used_trial_numbers = set()
            next_fallback = 1
            for idx, result in enumerate(filtered_results, 1):
                trial_number = getattr(result, "optuna_trial_number", None)
                if trial_number is None:
                    trial_number = idx
                trial_number = int(trial_number)
                if trial_number in used_trial_numbers:
                    while next_fallback in used_trial_numbers:
                        next_fallback += 1
                    trial_number = next_fallback
                used_trial_numbers.add(trial_number)
                constraint_values = list(getattr(result, "constraint_values", []) or [])
                constraints_satisfied = getattr(result, "constraints_satisfied", None)
                if constraints_satisfied is None:
                    constraints_satisfied = all(v <= 0.0 for v in constraint_values) if constraint_values else True
                trial_rows.append(
                    (
                        study_id,
                        int(trial_number),
                        json.dumps(result.params),
                        json.dumps(list(getattr(result, "objective_values", []) or [])),
                        1 if getattr(result, "is_pareto_optimal", False) else 0,
                        getattr(result, "dominance_rank", None),
                        1 if constraints_satisfied else 0,
                        json.dumps(constraint_values) if constraint_values else None,
                        result.net_profit_pct,
                        result.max_drawdown_pct,
                        result.total_trades,
                        result.win_rate,
                        result.max_consecutive_losses,
                        result.avg_win,
                        result.avg_loss,
                        result.gross_profit,
                        result.gross_loss,
                        result.romad,
                        result.sharpe_ratio,
                        result.sortino_ratio,
                        result.profit_factor,
                        result.ulcer_index,
                        result.sqn,
                        result.consistency_score,
                        result.score,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                )

            if trial_rows:
                conn.executemany(
                    """
                    INSERT INTO trials (
                        study_id, trial_number,
                        params_json, objective_values_json, is_pareto_optimal, dominance_rank,
                        constraints_satisfied, constraint_values_json,
                        net_profit_pct, max_drawdown_pct, total_trades, win_rate, max_consecutive_losses, avg_win, avg_loss,
                        gross_profit, gross_loss,
                        romad, sharpe_ratio, sortino_ratio, profit_factor, ulcer_index, sqn,
                        consistency_score, composite_score,
                        ft_net_profit_pct, ft_max_drawdown_pct, ft_total_trades, ft_win_rate,
                        ft_sharpe_ratio, ft_sortino_ratio, ft_romad, ft_profit_factor,
                        ft_ulcer_index, ft_sqn, ft_consistency_score, profit_degradation, ft_rank,
                        ft_passes_threshold
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    trial_rows,
                )

            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to save study to database: {exc}")

    return study_id


def save_wfa_study_to_db(
    wf_result,
    config,
    csv_file_path: str,
    start_time: float,
    score_config: Optional[Dict] = None,
) -> str:
    init_database()

    study_id = generate_study_id()
    csv_display_name = _get_csv_display_name(config, csv_file_path)

    study_name = generate_study_name(
        strategy_id=wf_result.strategy_id,
        csv_filename=csv_display_name,
        start_date=wf_result.trading_start_date,
        end_date=wf_result.trading_end_date,
        mode="wfa",
    )

    strategy_version = None
    try:
        from strategies import get_strategy

        strategy_class = get_strategy(wf_result.strategy_id)
        strategy_version = getattr(strategy_class, "STRATEGY_VERSION", None)
    except Exception:
        pass

    objectives = []
    primary_objective = None
    constraints_payload: List[Dict[str, Any]] = []
    optuna_config: Dict[str, Any] = {}
    wfa_config: Dict[str, Any] = {}
    if isinstance(config, dict):
        objectives = list(config.get("objectives") or [])
        primary_objective = config.get("primary_objective")
        constraints_payload = list(config.get("constraints") or [])
        optuna_candidate = config.get("optuna_config")
        if isinstance(optuna_candidate, dict):
            optuna_config = dict(optuna_candidate)
        wfa_candidate = config.get("wfa")
        if isinstance(wfa_candidate, dict):
            wfa_config = dict(wfa_candidate)

    optimization_time_seconds = None
    if start_time:
        try:
            optimization_time_seconds = int(round(max(0, time.time() - float(start_time))))
        except (TypeError, ValueError):
            optimization_time_seconds = None

    with get_db_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")

            def _tri_state(value: Optional[bool]) -> Optional[int]:
                if value is None:
                    return None
                return 1 if value else 0

            def _serialize_timestamps(values: Optional[List[Any]]) -> Optional[str]:
                if not values:
                    return None
                return json.dumps(
                    [
                        value.isoformat() if hasattr(value, "isoformat") else value
                        for value in values
                    ]
                )

            stitched_equity = None
            stitched_timestamps = None
            stitched_window_ids = None
            stitched_start_ts = None
            stitched_end_ts = None
            stitched_point_count = 0
            stitched_net_profit_pct = None
            stitched_max_drawdown_pct = None
            stitched_total_trades = None
            stitched_winning_trades = None
            stitched_win_rate = None
            stitched_consistency_full = None
            stitched_consistency_recent = None
            profitable_windows = 0
            total_windows = 0
            median_window_profit = None
            median_window_wr = None
            worst_window_profit = None
            worst_window_dd = None

            def _finite_float(value: Any) -> Optional[float]:
                if value is None:
                    return None
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    return None
                if not math.isfinite(parsed):
                    return None
                return parsed

            def _derive_winning_trades(window_obj: Any) -> Optional[int]:
                total_raw = _finite_float(getattr(window_obj, "oos_total_trades", None))
                if total_raw is None:
                    return None
                total_int = max(0, int(round(total_raw)))

                wins_raw = _finite_float(getattr(window_obj, "oos_winning_trades", None))
                if wins_raw is not None:
                    wins_int = max(0, int(round(wins_raw)))
                    return min(wins_int, total_int)

                win_rate_raw = _finite_float(getattr(window_obj, "oos_win_rate", None))
                if win_rate_raw is None:
                    return None

                derived = int(round(total_int * win_rate_raw / 100.0))
                return min(max(derived, 0), total_int)

            stitched = getattr(wf_result, "stitched_oos", None)
            if stitched:
                stitched_equity = (
                    json.dumps(list(stitched.equity_curve))
                    if getattr(stitched, "equity_curve", None)
                    else None
                )
                stitched_timestamps = _serialize_timestamps(getattr(stitched, "timestamps", None))
                stitched_window_ids = (
                    json.dumps(list(stitched.window_ids))
                    if getattr(stitched, "window_ids", None)
                    else None
                )
                stitched_timestamps_values = list(getattr(stitched, "timestamps", None) or [])
                stitched_curve_values = list(getattr(stitched, "equity_curve", None) or [])
                if stitched_curve_values and len(stitched_curve_values) == len(stitched_timestamps_values):
                    stitched_point_count = len(stitched_timestamps_values)
                    stitched_start_ts = _format_timestamp(stitched_timestamps_values[0])
                    stitched_end_ts = _format_timestamp(stitched_timestamps_values[-1])
                    stitched_consistency_full, stitched_consistency_recent = (
                        _compute_cached_curve_consistency_scores(
                            stitched_curve_values,
                            stitched_timestamps_values,
                        )
                    )
                stitched_net_profit_pct = getattr(stitched, "final_net_profit_pct", None)
                stitched_max_drawdown_pct = getattr(stitched, "max_drawdown_pct", None)
                stitched_total_trades = getattr(stitched, "total_trades", None)
                stitched_win_rate = getattr(stitched, "oos_win_rate", None)

            windows = list(getattr(wf_result, "windows", []) or [])
            total_windows = len(windows)

            window_profits: List[float] = []
            window_trade_win_rates: List[float] = []
            window_drawdowns: List[float] = []
            trade_totals: List[int] = []
            all_window_totals_known = True
            all_window_wins_known = True
            wins_sum = 0

            for window in windows:
                profit = _finite_float(getattr(window, "oos_net_profit_pct", None))
                if profit is not None:
                    window_profits.append(profit)
                    if profit > 0:
                        profitable_windows += 1

                trade_win_rate = _finite_float(getattr(window, "oos_win_rate", None))
                if trade_win_rate is not None:
                    window_trade_win_rates.append(trade_win_rate)

                drawdown = _finite_float(getattr(window, "oos_max_drawdown_pct", None))
                if drawdown is not None:
                    window_drawdowns.append(drawdown)

                total_raw = _finite_float(getattr(window, "oos_total_trades", None))
                if total_raw is None:
                    all_window_totals_known = False
                else:
                    trade_totals.append(max(0, int(round(total_raw))))

                wins = _derive_winning_trades(window)
                if wins is None:
                    all_window_wins_known = False
                else:
                    wins_sum += wins

            if stitched_total_trades is None and all_window_totals_known:
                stitched_total_trades = sum(trade_totals)

            if all_window_wins_known:
                stitched_winning_trades = wins_sum
            elif stitched_total_trades == 0:
                stitched_winning_trades = 0

            if stitched_total_trades is not None and stitched_winning_trades is not None:
                stitched_total_trades_int = max(0, int(round(float(stitched_total_trades))))
                stitched_total_trades = stitched_total_trades_int
                stitched_winning_trades = min(
                    max(0, int(round(float(stitched_winning_trades)))),
                    stitched_total_trades_int,
                )

            if stitched_win_rate is None and total_windows > 0:
                stitched_win_rate = (profitable_windows / total_windows) * 100.0

            if window_profits:
                median_window_profit = float(statistics.median(window_profits))
                worst_window_profit = min(window_profits)
            if window_trade_win_rates:
                median_window_wr = float(statistics.median(window_trade_win_rates))
            if window_drawdowns:
                worst_window_dd = max(window_drawdowns)

            wf_cfg = getattr(wf_result, "config", None)
            is_period_days = getattr(wf_cfg, "is_period_days", None)
            if is_period_days is None:
                is_period_days = wfa_config.get("is_period_days")
            adaptive_mode = 1 if bool(getattr(wf_cfg, "adaptive_mode", False)) else 0
            max_oos_period_days = getattr(wf_cfg, "max_oos_period_days", None)
            min_oos_trades = getattr(wf_cfg, "min_oos_trades", None)
            check_interval_trades = getattr(wf_cfg, "check_interval_trades", None)
            cusum_threshold = getattr(wf_cfg, "cusum_threshold", None)
            dd_threshold_multiplier = getattr(wf_cfg, "dd_threshold_multiplier", None)
            inactivity_multiplier = getattr(wf_cfg, "inactivity_multiplier", None)
            cooldown_enabled = 1 if bool(getattr(wf_cfg, "cooldown_enabled", False)) else 0
            cooldown_days = getattr(wf_cfg, "cooldown_days", None)
            post_process_cfg = getattr(wf_cfg, "post_process", None)
            ft_enabled = 1 if bool(getattr(post_process_cfg, "enabled", False)) else 0
            ft_period_days = getattr(post_process_cfg, "ft_period_days", None)
            ft_top_k = getattr(post_process_cfg, "top_k", None)
            ft_sort_metric = getattr(post_process_cfg, "sort_metric", None)
            ft_threshold_pct = getattr(post_process_cfg, "ft_threshold_pct", None)
            ft_reject_action = getattr(post_process_cfg, "ft_reject_action", None)
            ft_reject_cooldown_days = getattr(post_process_cfg, "ft_reject_cooldown_days", None)
            ft_reject_max_attempts = getattr(post_process_cfg, "ft_reject_max_attempts", None)
            ft_reject_min_remaining_oos_days = getattr(
                post_process_cfg, "ft_reject_min_remaining_oos_days", None
            )
            dsr_cfg = getattr(wf_cfg, "dsr_config", None)
            dsr_enabled = 1 if bool(getattr(dsr_cfg, "enabled", False)) else 0
            dsr_top_k = getattr(dsr_cfg, "top_k", None)
            st_cfg = getattr(wf_cfg, "stress_test_config", None)
            st_enabled = 1 if bool(getattr(st_cfg, "enabled", False)) else 0
            st_top_k = getattr(st_cfg, "top_k", None)
            st_failure_threshold = getattr(st_cfg, "failure_threshold", None)
            st_sort_metric = getattr(st_cfg, "sort_metric", None)
            sampler_type = (
                optuna_config.get("sampler_type")
                or optuna_config.get("sampler")
                or (config.get("sampler_type") if isinstance(config, dict) else None)
            )
            population_size = optuna_config.get("population_size")
            if population_size is None and isinstance(config, dict):
                population_size = config.get("population_size")
            crossover_prob = optuna_config.get("crossover_prob")
            if crossover_prob is None and isinstance(config, dict):
                crossover_prob = config.get("crossover_prob")
            mutation_prob = optuna_config.get("mutation_prob")
            if mutation_prob is None and isinstance(config, dict):
                mutation_prob = config.get("mutation_prob")
            swapping_prob = optuna_config.get("swapping_prob")
            if swapping_prob is None and isinstance(config, dict):
                swapping_prob = config.get("swapping_prob")
            budget_mode = optuna_config.get("budget_mode")
            n_trials = optuna_config.get("n_trials")
            time_limit = optuna_config.get("time_limit")
            convergence_patience = optuna_config.get("convergence_patience")

            study_columns = (
                "study_id", "study_name", "strategy_id", "strategy_version",
                "optimization_mode",
                "objectives_json", "n_objectives", "directions_json", "primary_objective",
                "constraints_json",
                "sampler_type", "population_size", "crossover_prob", "mutation_prob", "swapping_prob",
                "budget_mode", "n_trials", "time_limit", "convergence_patience",
                "total_trials", "completed_trials", "pruned_trials", "pareto_front_size",
                "best_value", "best_values_json",
                "score_config_json", "config_json",
                "csv_file_path", "csv_file_name",
                "dataset_start_date", "dataset_end_date", "warmup_bars",
                "is_period_days",
                "ft_enabled", "ft_period_days", "ft_top_k", "ft_sort_metric",
                "ft_threshold_pct", "ft_reject_action", "ft_reject_cooldown_days",
                "ft_reject_max_attempts", "ft_reject_min_remaining_oos_days",
                "dsr_enabled", "dsr_top_k",
                "st_enabled", "st_top_k", "st_failure_threshold", "st_sort_metric",
                "adaptive_mode", "max_oos_period_days", "min_oos_trades",
                "check_interval_trades", "cusum_threshold",
                "dd_threshold_multiplier", "inactivity_multiplier",
                "cooldown_enabled", "cooldown_days",
                "optimization_time_seconds",
                "completed_at",
                "filter_min_profit", "min_profit_threshold",
                "stitched_oos_equity_curve", "stitched_oos_timestamps_json",
                "stitched_oos_window_ids_json", "stitched_oos_start_ts",
                "stitched_oos_end_ts", "stitched_oos_point_count",
                "stitched_oos_net_profit_pct",
                "stitched_oos_max_drawdown_pct", "stitched_oos_total_trades",
                "stitched_oos_winning_trades", "stitched_oos_win_rate",
                "stitched_oos_consistency_full", "stitched_oos_consistency_recent",
                "profitable_windows", "total_windows",
                "median_window_profit", "median_window_wr",
                "worst_window_profit", "worst_window_dd",
            )
            study_values = (
                study_id,
                study_name,
                wf_result.strategy_id,
                strategy_version,
                "wfa",
                json.dumps(objectives) if objectives else None,
                len(objectives) if objectives else 1,
                None,
                primary_objective,
                json.dumps(constraints_payload) if constraints_payload else None,
                sampler_type,
                population_size,
                crossover_prob,
                mutation_prob,
                swapping_prob,
                budget_mode,
                n_trials,
                time_limit,
                convergence_patience,
                wf_result.total_windows,
                wf_result.total_windows,
                0,
                None,
                getattr(wf_result.stitched_oos, "wfe", None),
                None,
                json.dumps(score_config) if score_config else None,
                json.dumps(_safe_dict(config)),
                str(Path(csv_file_path).resolve()) if csv_file_path else "",
                csv_display_name,
                _format_date(wf_result.trading_start_date),
                _format_date(wf_result.trading_end_date),
                wf_result.warmup_bars,
                is_period_days,
                ft_enabled,
                ft_period_days,
                ft_top_k,
                ft_sort_metric,
                ft_threshold_pct,
                ft_reject_action,
                ft_reject_cooldown_days,
                ft_reject_max_attempts,
                ft_reject_min_remaining_oos_days,
                dsr_enabled,
                dsr_top_k,
                st_enabled,
                st_top_k,
                st_failure_threshold,
                st_sort_metric,
                adaptive_mode,
                max_oos_period_days,
                min_oos_trades,
                check_interval_trades,
                cusum_threshold,
                dd_threshold_multiplier,
                inactivity_multiplier,
                cooldown_enabled,
                cooldown_days,
                optimization_time_seconds,
                _utc_now_iso(),
                1 if isinstance(config, dict) and config.get("filter_min_profit") else 0,
                config.get("min_profit_threshold") if isinstance(config, dict) else None,
                stitched_equity,
                stitched_timestamps,
                stitched_window_ids,
                stitched_start_ts,
                stitched_end_ts,
                stitched_point_count,
                stitched_net_profit_pct,
                stitched_max_drawdown_pct,
                stitched_total_trades,
                stitched_winning_trades,
                stitched_win_rate,
                stitched_consistency_full,
                stitched_consistency_recent,
                profitable_windows,
                total_windows,
                median_window_profit,
                median_window_wr,
                worst_window_profit,
                worst_window_dd,
            )
            study_placeholders = ", ".join(["?"] * len(study_columns))
            conn.execute(
                f"INSERT INTO studies ({', '.join(study_columns)}) VALUES ({study_placeholders})",
                study_values,
            )
            invalidate_all_studies_analytics_cache(conn=conn)

            window_rows = []
            for window in wf_result.windows:
                is_equity = None
                oos_equity = None
                oos_timestamps = None
                is_timestamps = None
                available_modules = getattr(window, "available_modules", None)
                window_rows.append(
                    (
                        f"{study_id}_w{window.window_id}",
                        study_id,
                        window.window_id,
                        json.dumps(window.best_params),
                        window.param_id,
                        getattr(window, "best_params_source", None),
                        _tri_state(getattr(window, "is_pareto_optimal", None)),
                        _tri_state(getattr(window, "constraints_satisfied", None)),
                        json.dumps(available_modules) if available_modules is not None else None,
                        getattr(wf_result.config, "store_top_n_trials", None),
                        json.dumps(getattr(window, "module_status", None))
                        if getattr(window, "module_status", None) is not None
                        else None,
                        json.dumps(getattr(window, "selection_chain", None))
                        if getattr(window, "selection_chain", None) is not None
                        else None,
                        _format_date(getattr(window, "optimization_start", None)),
                        _format_date(getattr(window, "optimization_end", None)),
                        _format_timestamp(getattr(window, "optimization_start", None)),
                        _format_timestamp(getattr(window, "optimization_end", None)),
                        _format_date(getattr(window, "ft_start", None)),
                        _format_date(getattr(window, "ft_end", None)),
                        _format_timestamp(getattr(window, "ft_start", None)),
                        _format_timestamp(getattr(window, "ft_end", None)),
                        is_timestamps,
                        _format_date(window.is_start),
                        _format_date(window.is_end),
                        _format_timestamp(window.is_start),
                        _format_timestamp(window.is_end),
                        window.is_net_profit_pct,
                        window.is_max_drawdown_pct,
                        window.is_total_trades,
                        getattr(window, "is_best_trial_number", None),
                        is_equity,
                        getattr(window, "is_win_rate", None),
                        getattr(window, "is_max_consecutive_losses", None),
                        getattr(window, "is_romad", None),
                        getattr(window, "is_sharpe_ratio", None),
                        getattr(window, "is_profit_factor", None),
                        getattr(window, "is_sqn", None),
                        getattr(window, "is_ulcer_index", None),
                        getattr(window, "is_consistency_score", None),
                        getattr(window, "is_composite_score", None),
                        _format_date(window.oos_start),
                        _format_date(window.oos_end),
                        _format_timestamp(window.oos_start),
                        _format_timestamp(window.oos_end),
                        window.oos_net_profit_pct,
                        window.oos_max_drawdown_pct,
                        window.oos_total_trades,
                        getattr(window, "oos_winning_trades", None),
                        oos_equity,
                        oos_timestamps,
                        getattr(window, "oos_win_rate", None),
                        getattr(window, "oos_max_consecutive_losses", None),
                        getattr(window, "oos_romad", None),
                        getattr(window, "oos_sharpe_ratio", None),
                        getattr(window, "oos_profit_factor", None),
                        getattr(window, "oos_sqn", None),
                        getattr(window, "oos_ulcer_index", None),
                        getattr(window, "oos_consistency_score", None),
                        getattr(window, "trigger_type", None),
                        getattr(window, "cusum_final", None),
                        getattr(window, "cusum_threshold", None),
                        getattr(window, "dd_threshold", None),
                        getattr(window, "oos_actual_days", None),
                        getattr(window, "cooldown_days_applied", None),
                        getattr(window, "oos_elapsed_days", None),
                        _format_date(getattr(window, "trade_start", None)),
                        _format_date(getattr(window, "trade_end", None)),
                        _format_timestamp(getattr(window, "trade_start", None)),
                        _format_timestamp(getattr(window, "trade_end", None)),
                        getattr(window, "entry_delay_days", None),
                        getattr(window, "ft_retry_attempts_used", None),
                        getattr(window, "remaining_oos_days_at_entry", None),
                        getattr(window, "window_status", None),
                        getattr(window, "no_trade_reason", None),
                        getattr(window, "wfe", None),
                    )
                )

            if window_rows:
                window_columns = (
                    "window_id", "study_id", "window_number",
                    "best_params_json", "param_id", "best_params_source",
                    "is_pareto_optimal", "constraints_satisfied",
                    "available_modules", "store_top_n_trials",
                    "module_status_json", "selection_chain_json",
                    "optimization_start_date", "optimization_end_date",
                    "optimization_start_ts", "optimization_end_ts",
                    "ft_start_date", "ft_end_date",
                    "ft_start_ts", "ft_end_ts",
                    "is_timestamps_json",
                    "is_start_date", "is_end_date",
                    "is_start_ts", "is_end_ts",
                    "is_net_profit_pct", "is_max_drawdown_pct", "is_total_trades", "is_best_trial_number",
                    "is_equity_curve",
                    "is_win_rate", "is_max_consecutive_losses", "is_romad", "is_sharpe_ratio",
                    "is_profit_factor", "is_sqn", "is_ulcer_index", "is_consistency_score", "is_composite_score",
                    "oos_start_date", "oos_end_date",
                    "oos_start_ts", "oos_end_ts",
                    "oos_net_profit_pct", "oos_max_drawdown_pct", "oos_total_trades",
                    "oos_winning_trades",
                    "oos_equity_curve", "oos_timestamps_json",
                    "oos_win_rate", "oos_max_consecutive_losses", "oos_romad", "oos_sharpe_ratio",
                    "oos_profit_factor", "oos_sqn", "oos_ulcer_index", "oos_consistency_score",
                    "trigger_type", "cusum_final", "cusum_threshold", "dd_threshold", "oos_actual_days",
                    "cooldown_days_applied", "oos_elapsed_days",
                    "trade_start_date", "trade_end_date", "trade_start_ts", "trade_end_ts",
                    "entry_delay_days", "ft_retry_attempts_used", "remaining_oos_days_at_entry",
                    "window_status", "no_trade_reason",
                    "wfe",
                )
                placeholders = ", ".join(["?"] * len(window_columns))
                conn.executemany(
                    f"""
                    INSERT INTO wfa_windows ({", ".join(window_columns)})
                    VALUES ({placeholders})
                    """,
                    window_rows,
                )

            for window in wf_result.windows:
                window_key = f"{study_id}_w{window.window_id}"
                _save_window_trials(conn, window_key, "optuna_is", window.optuna_is_trials)
                _save_window_trials(conn, window_key, "dsr", window.dsr_trials)
                _save_window_trials(conn, window_key, "forward_test", window.forward_test_trials)
                _save_window_trials(conn, window_key, "stress_test", window.stress_test_trials)

            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to save WFA study to database: {exc}")

    return study_id


def _save_window_trials(
    conn: sqlite3.Connection,
    window_id: str,
    module_type: str,
    trials: Optional[List[Dict[str, Any]]],
) -> None:
    if not trials:
        return

    def _tri_state(value: Optional[bool]) -> Optional[int]:
        if value is None:
            return None
        return 1 if value else 0

    rows = []
    for trial in trials:
        params = trial.get("params") or {}
        rows.append(
            (
                window_id,
                module_type,
                trial.get("trial_number"),
                json.dumps(params),
                trial.get("param_id"),
                trial.get("source_rank"),
                trial.get("module_rank"),
                trial.get("net_profit_pct"),
                trial.get("max_drawdown_pct"),
                trial.get("total_trades"),
                trial.get("win_rate"),
                trial.get("profit_factor"),
                trial.get("romad"),
                trial.get("sharpe_ratio"),
                trial.get("sortino_ratio"),
                trial.get("sqn"),
                trial.get("ulcer_index"),
                trial.get("consistency_score"),
                trial.get("max_consecutive_losses"),
                trial.get("composite_score"),
                json.dumps(trial.get("objective_values") or []),
                json.dumps(trial.get("constraint_values") or []),
                _tri_state(trial.get("constraints_satisfied")),
                _tri_state(trial.get("is_pareto_optimal")),
                trial.get("dominance_rank"),
                trial.get("status"),
                1 if trial.get("is_selected") else 0,
                json.dumps(trial.get("module_metrics") or {}),
            )
        )

    conn.executemany(
        """
        INSERT INTO wfa_window_trials (
            window_id, module_type, trial_number,
            params_json, param_id,
            source_rank, module_rank,
            net_profit_pct, max_drawdown_pct, total_trades, win_rate, profit_factor,
            romad, sharpe_ratio, sortino_ratio, sqn, ulcer_index, consistency_score,
            max_consecutive_losses,
            composite_score, objective_values_json, constraint_values_json,
            constraints_satisfied, is_pareto_optimal, dominance_rank,
            status, is_selected, module_metrics_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def load_wfa_window_trials(window_id: str) -> Dict[str, List[Dict[str, Any]]]:
    def _parse_json(value: Optional[str], default):
        if not value:
            return default
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            SELECT
                module_type,
                trial_number,
                params_json,
                param_id,
                source_rank,
                module_rank,
                net_profit_pct,
                max_drawdown_pct,
                total_trades,
                win_rate,
                profit_factor,
                romad,
                sharpe_ratio,
                sortino_ratio,
                sqn,
                ulcer_index,
                consistency_score,
                max_consecutive_losses,
                composite_score,
                objective_values_json,
                constraint_values_json,
                constraints_satisfied,
                is_pareto_optimal,
                dominance_rank,
                status,
                is_selected,
                module_metrics_json
            FROM wfa_window_trials
            WHERE window_id = ?
            ORDER BY
                CASE WHEN module_type IS NULL THEN 1 ELSE 0 END,
                module_type ASC,
                (module_rank IS NULL) ASC,
                module_rank ASC,
                source_rank ASC,
                trial_number ASC
            """,
            (window_id,),
        )
        for row in cursor.fetchall():
            trial = dict(row)
            trial["params"] = _parse_json(trial.pop("params_json", None), {})
            trial["objective_values"] = _parse_json(trial.pop("objective_values_json", None), [])
            trial["constraint_values"] = _parse_json(trial.pop("constraint_values_json", None), [])
            trial["module_metrics"] = _parse_json(trial.pop("module_metrics_json", None), {})
            trial["constraints_satisfied"] = (
                None if trial.get("constraints_satisfied") is None else bool(trial.get("constraints_satisfied"))
            )
            trial["is_pareto_optimal"] = (
                None if trial.get("is_pareto_optimal") is None else bool(trial.get("is_pareto_optimal"))
            )
            trial["is_selected"] = bool(trial.get("is_selected"))
            if trial.get("composite_score") is not None and trial.get("score") is None:
                trial["score"] = trial.get("composite_score")
            grouped.setdefault(trial.get("module_type") or "optuna_is", []).append(trial)
    return grouped


def list_studies() -> List[Dict]:
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            SELECT
                study_id, study_name, strategy_id, optimization_mode,
                created_at, completed_at, completed_trials, best_value,
                csv_file_name
            FROM studies
            ORDER BY created_at DESC
            """
        )
        rows = []
        for row in cursor.fetchall():
            study = dict(row)
            if "status" not in study:
                study["status"] = "completed" if study.get("completed_at") else "unknown"
            rows.append(study)
        return rows


def load_study_from_db(study_id: str) -> Optional[Dict]:
    with get_db_connection() as conn:
        if backfill_stitched_oos_metadata(conn=conn, study_ids=[study_id]) > 0:
            conn.commit()
        cursor = conn.execute("SELECT * FROM studies WHERE study_id = ?", (study_id,))
        study_row = cursor.fetchone()
        if not study_row:
            return None

        study = dict(study_row)
        for key in (
            "config_json",
            "score_config_json",
            "objectives_json",
            "directions_json",
            "constraints_json",
            "best_values_json",
        ):
            if study.get(key):
                try:
                    study[key] = json.loads(study[key])
                except json.JSONDecodeError:
                    pass

        if isinstance(study.get("objectives_json"), list):
            study["objectives"] = study["objectives_json"]
        if isinstance(study.get("directions_json"), list):
            study["directions"] = study["directions_json"]
        if isinstance(study.get("constraints_json"), list):
            study["constraints"] = study["constraints_json"]
        if isinstance(study.get("best_values_json"), dict):
            study["best_values"] = study["best_values_json"]

        csv_path = study.get("csv_file_path")
        csv_exists = bool(csv_path and Path(csv_path).exists())

        stitched_oos = None
        if study.get("stitched_oos_equity_curve"):
            try:
                equity_curve = json.loads(study.get("stitched_oos_equity_curve") or "[]")
            except json.JSONDecodeError:
                equity_curve = []
            try:
                timestamps = json.loads(study.get("stitched_oos_timestamps_json") or "[]")
            except json.JSONDecodeError:
                timestamps = []
            try:
                window_ids = json.loads(study.get("stitched_oos_window_ids_json") or "[]")
            except json.JSONDecodeError:
                window_ids = []
            stitched_oos = {
                "equity_curve": equity_curve,
                "timestamps": timestamps,
                "window_ids": window_ids,
                "final_net_profit_pct": study.get("stitched_oos_net_profit_pct"),
                "max_drawdown_pct": study.get("stitched_oos_max_drawdown_pct"),
                "total_trades": study.get("stitched_oos_total_trades"),
                "winning_trades": study.get("stitched_oos_winning_trades"),
                "wfe": study.get("best_value"),
                "oos_win_rate": study.get("stitched_oos_win_rate"),
                "consistency_full": study.get("stitched_oos_consistency_full"),
                "consistency_recent": study.get("stitched_oos_consistency_recent"),
                "profitable_windows": study.get("profitable_windows"),
                "total_windows": study.get("total_windows"),
                "median_window_profit": study.get("median_window_profit"),
                "median_window_wr": study.get("median_window_wr"),
                "worst_window_profit": study.get("worst_window_profit"),
                "worst_window_dd": study.get("worst_window_dd"),
            }

        trials: List[Dict] = []
        windows: List[Dict] = []
        manual_tests: List[Dict] = []

        if study.get("optimization_mode") == "optuna":
            cursor = conn.execute(
                "SELECT * FROM trials WHERE study_id = ?",
                (study_id,),
            )
            for row in cursor.fetchall():
                trial = dict(row)
                trial["params"] = json.loads(trial["params_json"])
                trial["objective_values"] = json.loads(trial["objective_values_json"] or "[]")
                trial["constraint_values"] = json.loads(trial["constraint_values_json"] or "[]")
                trial["is_pareto_optimal"] = bool(trial.get("is_pareto_optimal"))
                trial["constraints_satisfied"] = bool(trial.get("constraints_satisfied"))
                if trial.get("ft_passes_threshold") is not None:
                    trial["ft_passes_threshold"] = bool(trial.get("ft_passes_threshold"))
                if trial.get("param_worst_ratios"):
                    try:
                        trial["param_worst_ratios"] = json.loads(trial["param_worst_ratios"])
                    except json.JSONDecodeError:
                        pass
                if trial.get("composite_score") is not None:
                    trial["score"] = trial.get("composite_score")
                trials.append(trial)
            objectives = study.get("objectives_json") or []
            if isinstance(objectives, list) and objectives:
                directions = study.get("directions_json") or []
                primary_objective = study.get("primary_objective") or objectives[0]
                try:
                    primary_idx = objectives.index(primary_objective)
                except ValueError:
                    primary_idx = 0
                primary_direction = None
                if isinstance(directions, list) and len(directions) > primary_idx:
                    primary_direction = directions[primary_idx]
                if primary_direction not in {"maximize", "minimize"}:
                    primary_direction = OBJECTIVE_DIRECTIONS.get(primary_objective, "maximize")

                constraints_payload = study.get("constraints_json") or []
                constraints_enabled = any(
                    bool(item.get("enabled")) for item in constraints_payload if isinstance(item, dict)
                )

                if len(objectives) == 1:
                    reverse = primary_direction == "maximize"
                    trials.sort(
                        key=lambda t: float(t.get("objective_values", [0.0])[0]),
                        reverse=reverse,
                    )
                else:
                    def _calculate_total_violation(item: Dict[str, Any]) -> float:
                        values = item.get("constraint_values") or []
                        if not values:
                            if item.get("constraints_satisfied") is False:
                                return float("inf")
                            return 0.0
                        try:
                            return sum(max(0.0, float(v)) for v in values)
                        except (TypeError, ValueError):
                            return float("inf")

                    def group_rank(item: Dict[str, Any]) -> int:
                        if constraints_enabled:
                            if not item.get("constraints_satisfied", True):
                                return 2
                            return 0 if item.get("is_pareto_optimal") else 1
                        return 0 if item.get("is_pareto_optimal") else 1

                    def primary_value(item: Dict[str, Any]) -> float:
                        values = item.get("objective_values") or []
                        value = float(values[primary_idx]) if len(values) > primary_idx else 0.0
                        return -value if primary_direction == "maximize" else value

                    trials.sort(
                        key=lambda t: (
                            group_rank(t),
                            _calculate_total_violation(t),
                            primary_value(t),
                            int(t.get("trial_number") or 0),
                        ),
                    )
        elif study.get("optimization_mode") == "wfa":
            cursor = conn.execute(
                "SELECT * FROM wfa_windows WHERE study_id = ? ORDER BY window_number",
                (study_id,),
            )
            for row in cursor.fetchall():
                window = dict(row)
                window["best_params"] = json.loads(window["best_params_json"])
                if window.get("oos_equity_curve"):
                    window["oos_equity_curve"] = json.loads(window["oos_equity_curve"])
                if window.get("is_equity_curve"):
                    window["is_equity_curve"] = json.loads(window["is_equity_curve"])
                if window.get("available_modules"):
                    try:
                        window["available_modules"] = json.loads(window["available_modules"])
                    except json.JSONDecodeError:
                        pass
                if window.get("module_status_json"):
                    try:
                        window["module_status"] = json.loads(window["module_status_json"])
                    except json.JSONDecodeError:
                        pass
                if window.get("selection_chain_json"):
                    try:
                        window["selection_chain"] = json.loads(window["selection_chain_json"])
                    except json.JSONDecodeError:
                        pass
                if window.get("is_timestamps_json"):
                    try:
                        window["is_timestamps"] = json.loads(window["is_timestamps_json"])
                    except json.JSONDecodeError:
                        pass
                if window.get("oos_timestamps_json"):
                    try:
                        window["oos_timestamps"] = json.loads(window["oos_timestamps_json"])
                    except json.JSONDecodeError:
                        pass
                if window.get("is_pareto_optimal") is not None:
                    window["is_pareto_optimal"] = bool(window.get("is_pareto_optimal"))
                if window.get("constraints_satisfied") is not None:
                    window["constraints_satisfied"] = bool(window.get("constraints_satisfied"))
                windows.append(window)

        cursor = conn.execute(
            """
            SELECT
                id, study_id, created_at, test_name, data_source, csv_path,
                start_date, end_date, source_tab, trials_count, trials_tested_csv,
                best_profit_degradation, worst_profit_degradation
            FROM manual_tests
            WHERE study_id = ?
            ORDER BY created_at DESC
            """,
            (study_id,),
        )
        manual_tests = [dict(row) for row in cursor.fetchall()]

    return {
        "study": study,
        "trials": trials,
        "windows": windows,
        "manual_tests": manual_tests,
        "stitched_oos": stitched_oos,
        "csv_exists": csv_exists,
    }


def get_study_trial(study_id: str, trial_number: int) -> Optional[Dict]:
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT * FROM trials WHERE study_id = ? AND trial_number = ?",
            (study_id, trial_number),
        )
        row = cursor.fetchone()
        if not row:
            return None
        trial = dict(row)
        trial["params"] = json.loads(trial["params_json"])
        if trial.get("ft_passes_threshold") is not None:
            trial["ft_passes_threshold"] = bool(trial.get("ft_passes_threshold"))
        return trial


def update_csv_path(study_id: str, new_path: str) -> bool:
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE studies
            SET csv_file_path = ?
            WHERE study_id = ?
            """,
            (new_path, study_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def update_study_config_json(study_id: str, config_json: Dict[str, Any]) -> bool:
    if not isinstance(config_json, dict):
        return False
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE studies
            SET config_json = ?
            WHERE study_id = ?
            """,
            (json.dumps(config_json), study_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def save_forward_test_results(
    study_id: str,
    ft_results: List[Any],
    *,
    ft_enabled: bool,
    ft_period_days: Optional[int],
    ft_top_k: Optional[int],
    ft_sort_metric: Optional[str],
    ft_threshold_pct: Optional[float],
    ft_reject_action: Optional[str],
    ft_reject_cooldown_days: Optional[int],
    ft_reject_max_attempts: Optional[int],
    ft_reject_min_remaining_oos_days: Optional[int],
    ft_start_date: Optional[str],
    ft_end_date: Optional[str],
    is_period_days: Optional[int],
    ft_source: Optional[str] = None,
) -> bool:
    if not study_id:
        return False

    with get_db_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                """
                UPDATE studies
                SET
                    ft_enabled = ?,
                    ft_period_days = ?,
                    ft_top_k = ?,
                    ft_sort_metric = ?,
                    ft_threshold_pct = ?,
                    ft_reject_action = ?,
                    ft_reject_cooldown_days = ?,
                    ft_reject_max_attempts = ?,
                    ft_reject_min_remaining_oos_days = ?,
                    ft_start_date = ?,
                    ft_end_date = ?,
                    is_period_days = ?
                WHERE study_id = ?
                """,
                (
                    1 if ft_enabled else 0,
                    ft_period_days,
                    ft_top_k,
                    ft_sort_metric,
                    ft_threshold_pct,
                    ft_reject_action,
                    ft_reject_cooldown_days,
                    ft_reject_max_attempts,
                    ft_reject_min_remaining_oos_days,
                    _format_date(ft_start_date),
                    _format_date(ft_end_date),
                    is_period_days,
                    study_id,
                ),
            )

            if ft_results:
                rows = []
                for result in ft_results:
                    payload = result
                    if hasattr(result, "__dict__"):
                        payload = result.__dict__
                    rows.append(
                        (
                            payload.get("ft_net_profit_pct"),
                            payload.get("ft_max_drawdown_pct"),
                            payload.get("ft_total_trades"),
                            payload.get("ft_win_rate"),
                            payload.get("ft_max_consecutive_losses"),
                            payload.get("ft_sharpe_ratio"),
                            payload.get("ft_sortino_ratio"),
                            payload.get("ft_romad"),
                            payload.get("ft_profit_factor"),
                            payload.get("ft_ulcer_index"),
                            payload.get("ft_sqn"),
                            payload.get("ft_consistency_score"),
                            payload.get("profit_degradation"),
                            payload.get("ft_rank"),
                            1 if payload.get("ft_passes_threshold") is True else 0
                            if payload.get("ft_passes_threshold") is False
                            else None,
                            ft_source,
                            study_id,
                            payload.get("trial_number"),
                        )
                    )

                conn.executemany(
                    """
                    UPDATE trials
                    SET
                        ft_net_profit_pct = ?,
                        ft_max_drawdown_pct = ?,
                        ft_total_trades = ?,
                        ft_win_rate = ?,
                        ft_max_consecutive_losses = ?,
                        ft_sharpe_ratio = ?,
                        ft_sortino_ratio = ?,
                        ft_romad = ?,
                        ft_profit_factor = ?,
                        ft_ulcer_index = ?,
                        ft_sqn = ?,
                        ft_consistency_score = ?,
                        profit_degradation = ?,
                        ft_rank = ?,
                        ft_passes_threshold = ?,
                        ft_source = ?
                    WHERE study_id = ? AND trial_number = ?
                    """,
                    rows,
                )

            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to save FT results: {exc}")

    return True


def save_dsr_results(
    study_id: str,
    dsr_results: List[Any],
    *,
    dsr_enabled: bool,
    dsr_top_k: Optional[int],
    dsr_n_trials: Optional[int],
    dsr_mean_sharpe: Optional[float],
    dsr_var_sharpe: Optional[float],
) -> bool:
    if not study_id:
        return False

    with get_db_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                """
                UPDATE studies
                SET
                    dsr_enabled = ?,
                    dsr_top_k = ?,
                    dsr_n_trials = ?,
                    dsr_mean_sharpe = ?,
                    dsr_var_sharpe = ?
                WHERE study_id = ?
                """,
                (
                    1 if dsr_enabled else 0,
                    dsr_top_k,
                    dsr_n_trials,
                    dsr_mean_sharpe,
                    dsr_var_sharpe,
                    study_id,
                ),
            )

            conn.execute(
                """
                UPDATE trials
                SET
                    dsr_probability = NULL,
                    dsr_rank = NULL,
                    dsr_skewness = NULL,
                    dsr_kurtosis = NULL,
                    dsr_track_length = NULL,
                    dsr_luck_share_pct = NULL
                WHERE study_id = ?
                """,
                (study_id,),
            )

            if dsr_results:
                rows = []
                for result in dsr_results:
                    payload = result
                    if hasattr(result, "__dict__"):
                        payload = result.__dict__
                    rows.append(
                        (
                            payload.get("dsr_probability"),
                            payload.get("dsr_rank"),
                            payload.get("dsr_skewness"),
                            payload.get("dsr_kurtosis"),
                            payload.get("dsr_track_length"),
                            payload.get("dsr_luck_share_pct"),
                            study_id,
                            payload.get("trial_number"),
                        )
                    )

                conn.executemany(
                    """
                    UPDATE trials
                    SET
                        dsr_probability = ?,
                        dsr_rank = ?,
                        dsr_skewness = ?,
                        dsr_kurtosis = ?,
                        dsr_track_length = ?,
                        dsr_luck_share_pct = ?
                    WHERE study_id = ? AND trial_number = ?
                    """,
                    rows,
                )

            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to save DSR results: {exc}")

    return True


def save_stress_test_results(
    study_id: str,
    st_results: List[Any],
    st_summary: Dict[str, Any],
    config: Any,
    *,
    st_source: Optional[str] = None,
) -> bool:
    if not study_id:
        return False

    with get_db_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                """
                UPDATE studies
                SET
                    st_enabled = ?,
                    st_top_k = ?,
                    st_failure_threshold = ?,
                    st_sort_metric = ?,
                    st_avg_profit_retention = ?,
                    st_avg_romad_retention = ?,
                    st_avg_combined_failure_rate = ?,
                    st_total_perturbations = ?,
                    st_candidates_skipped_bad_base = ?,
                    st_candidates_skipped_no_params = ?,
                    st_candidates_insufficient_data = ?
                WHERE study_id = ?
                """,
                (
                    1 if getattr(config, "enabled", False) else 0,
                    getattr(config, "top_k", None),
                    getattr(config, "failure_threshold", None),
                    getattr(config, "sort_metric", None),
                    st_summary.get("avg_profit_retention"),
                    st_summary.get("avg_romad_retention"),
                    st_summary.get("avg_combined_failure_rate"),
                    st_summary.get("total_perturbations_run"),
                    st_summary.get("candidates_skipped_bad_base", 0),
                    st_summary.get("candidates_skipped_no_params", 0),
                    st_summary.get("candidates_insufficient_data", 0),
                    study_id,
                ),
            )

            conn.execute(
                """
                UPDATE trials
                SET
                    st_rank = NULL,
                    st_status = NULL,
                    profit_retention = NULL,
                    romad_retention = NULL,
                    profit_worst = NULL,
                    profit_lower_tail = NULL,
                    profit_median = NULL,
                    romad_worst = NULL,
                    romad_lower_tail = NULL,
                    romad_median = NULL,
                    profit_failure_rate = NULL,
                    romad_failure_rate = NULL,
                    combined_failure_rate = NULL,
                    profit_failure_count = NULL,
                    romad_failure_count = NULL,
                    combined_failure_count = NULL,
                    total_perturbations = NULL,
                    st_failure_threshold = NULL,
                    param_worst_ratios = NULL,
                    most_sensitive_param = NULL,
                    st_source = NULL
                WHERE study_id = ?
                """,
                (study_id,),
            )

            if st_results:
                rows = []
                for result in st_results:
                    payload = result
                    if hasattr(result, "__dict__"):
                        payload = result.__dict__
                    param_worst = payload.get("param_worst_ratios") or {}
                    param_worst_json = json.dumps(param_worst) if param_worst else None
                    rows.append(
                        (
                            payload.get("st_rank"),
                            payload.get("status"),
                            payload.get("profit_retention"),
                            payload.get("romad_retention"),
                            payload.get("profit_worst"),
                            payload.get("profit_lower_tail"),
                            payload.get("profit_median"),
                            payload.get("romad_worst"),
                            payload.get("romad_lower_tail"),
                            payload.get("romad_median"),
                            payload.get("profit_failure_rate"),
                            payload.get("romad_failure_rate"),
                            payload.get("combined_failure_rate"),
                            payload.get("profit_failure_count"),
                            payload.get("romad_failure_count"),
                            payload.get("combined_failure_count"),
                            payload.get("total_perturbations"),
                            payload.get("failure_threshold"),
                            param_worst_json,
                            payload.get("most_sensitive_param"),
                            st_source,
                            study_id,
                            payload.get("trial_number"),
                        )
                    )

                conn.executemany(
                    """
                    UPDATE trials
                    SET
                        st_rank = ?,
                        st_status = ?,
                        profit_retention = ?,
                        romad_retention = ?,
                        profit_worst = ?,
                        profit_lower_tail = ?,
                        profit_median = ?,
                        romad_worst = ?,
                        romad_lower_tail = ?,
                        romad_median = ?,
                        profit_failure_rate = ?,
                        romad_failure_rate = ?,
                        combined_failure_rate = ?,
                        profit_failure_count = ?,
                        romad_failure_count = ?,
                        combined_failure_count = ?,
                        total_perturbations = ?,
                        st_failure_threshold = ?,
                        param_worst_ratios = ?,
                        most_sensitive_param = ?,
                        st_source = ?
                    WHERE study_id = ? AND trial_number = ?
                    """,
                    rows,
                )

            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to save stress test results: {exc}")

    return True


def save_oos_test_results(
    study_id: str,
    oos_results: List[Dict[str, Any]],
    *,
    oos_enabled: bool,
    oos_period_days: Optional[int],
    oos_top_k: Optional[int],
    oos_start_date: Optional[str],
    oos_end_date: Optional[str],
    oos_source_module: Optional[str],
) -> bool:
    if not study_id:
        return False

    with get_db_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                """
                UPDATE studies
                SET
                    oos_test_enabled = ?,
                    oos_test_period_days = ?,
                    oos_test_top_k = ?,
                    oos_test_start_date = ?,
                    oos_test_end_date = ?,
                    oos_test_source_module = ?
                WHERE study_id = ?
                """,
                (
                    1 if oos_enabled else 0,
                    oos_period_days,
                    oos_top_k,
                    _format_date(oos_start_date),
                    _format_date(oos_end_date),
                    oos_source_module,
                    study_id,
                ),
            )

            conn.execute(
                """
                UPDATE trials
                SET
                    oos_test_net_profit_pct = NULL,
                    oos_test_max_drawdown_pct = NULL,
                    oos_test_total_trades = NULL,
                    oos_test_win_rate = NULL,
                    oos_test_max_consecutive_losses = NULL,
                    oos_test_sharpe_ratio = NULL,
                    oos_test_sortino_ratio = NULL,
                    oos_test_romad = NULL,
                    oos_test_profit_factor = NULL,
                    oos_test_ulcer_index = NULL,
                    oos_test_sqn = NULL,
                    oos_test_consistency_score = NULL,
                    oos_test_profit_degradation = NULL,
                    oos_test_source = NULL,
                    oos_test_source_rank = NULL
                WHERE study_id = ?
                """,
                (study_id,),
            )

            if oos_results:
                rows = []
                for result in oos_results:
                    test_metrics = result.get("test_metrics") or {}
                    comparison = result.get("comparison") or {}
                    rows.append(
                        (
                            test_metrics.get("net_profit_pct"),
                            test_metrics.get("max_drawdown_pct"),
                            test_metrics.get("total_trades"),
                            test_metrics.get("win_rate"),
                            test_metrics.get("max_consecutive_losses"),
                            test_metrics.get("sharpe_ratio"),
                            test_metrics.get("sortino_ratio"),
                            test_metrics.get("romad"),
                            test_metrics.get("profit_factor"),
                            test_metrics.get("ulcer_index"),
                            test_metrics.get("sqn"),
                            test_metrics.get("consistency_score"),
                            comparison.get("profit_degradation"),
                            result.get("oos_test_source"),
                            result.get("oos_test_source_rank"),
                            study_id,
                            result.get("trial_number"),
                        )
                    )

                conn.executemany(
                    """
                    UPDATE trials
                    SET
                        oos_test_net_profit_pct = ?,
                        oos_test_max_drawdown_pct = ?,
                        oos_test_total_trades = ?,
                        oos_test_win_rate = ?,
                        oos_test_max_consecutive_losses = ?,
                        oos_test_sharpe_ratio = ?,
                        oos_test_sortino_ratio = ?,
                        oos_test_romad = ?,
                        oos_test_profit_factor = ?,
                        oos_test_ulcer_index = ?,
                        oos_test_sqn = ?,
                        oos_test_consistency_score = ?,
                        oos_test_profit_degradation = ?,
                        oos_test_source = ?,
                        oos_test_source_rank = ?
                    WHERE study_id = ? AND trial_number = ?
                    """,
                    rows,
                )

            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to save OOS test results: {exc}")

    return True


def save_manual_test_to_db(
    *,
    study_id: str,
    test_name: Optional[str],
    data_source: str,
    csv_path: Optional[str],
    start_date: str,
    end_date: str,
    source_tab: str,
    trials_count: int,
    trials_tested_csv: str,
    best_profit_degradation: Optional[float],
    worst_profit_degradation: Optional[float],
    results_json: Dict[str, Any],
) -> int:
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO manual_tests (
                study_id, test_name, data_source, csv_path,
                start_date, end_date, source_tab,
                trials_count, trials_tested_csv,
                best_profit_degradation, worst_profit_degradation,
                results_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                study_id,
                test_name,
                data_source,
                csv_path,
                start_date,
                end_date,
                source_tab,
                trials_count,
                trials_tested_csv,
                best_profit_degradation,
                worst_profit_degradation,
                json.dumps(results_json),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def list_manual_tests(study_id: str) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            SELECT
                id, study_id, created_at, test_name, data_source, csv_path,
                start_date, end_date, source_tab, trials_count, trials_tested_csv,
                best_profit_degradation, worst_profit_degradation
            FROM manual_tests
            WHERE study_id = ?
            ORDER BY created_at DESC
            """,
            (study_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


def load_manual_test_results(study_id: str, test_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT * FROM manual_tests WHERE study_id = ? AND id = ?",
            (study_id, int(test_id)),
        )
        row = cursor.fetchone()
        if not row:
            return None
        payload = dict(row)
        if payload.get("results_json"):
            try:
                payload["results_json"] = json.loads(payload["results_json"])
            except json.JSONDecodeError:
                pass
        return payload


def delete_manual_test(study_id: str, test_id: int) -> bool:
    with get_db_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM manual_tests WHERE study_id = ? AND id = ?",
            (study_id, int(test_id)),
        )
        conn.commit()
        return cursor.rowcount > 0


def delete_study(study_id: str) -> bool:
    with get_db_connection() as conn:
        invalidate_analytics_group_cache_for_study(study_id, conn=conn)
        cursor = conn.execute("DELETE FROM studies WHERE study_id = ?", (study_id,))
        conn.commit()
        return cursor.rowcount > 0


def _safe_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if hasattr(obj, "__dataclass_fields__"):
        try:
            data = asdict(obj)
        except Exception:
            data = {}
        return _serialize_dict(data)
    if isinstance(obj, dict):
        return _serialize_dict(obj)
    return {"value": str(obj)}


def _serialize_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in data.items():
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
        elif isinstance(value, (list, dict, str, int, float, bool, type(None))):
            result[key] = value
        else:
            result[key] = str(value)
    return result


def _format_date(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    if value:
        return str(value)[:10]
    return ""


def _format_timestamp(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value:
        return str(value)
    return ""
