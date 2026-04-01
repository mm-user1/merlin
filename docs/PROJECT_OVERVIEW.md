# Merlin - Project Overview

Config-driven backtesting and Optuna optimization platform for cryptocurrency trading strategies with SQLite database persistence and web-based studies management.

## Project Structure

```
project-root/
|-- CLAUDE.md                 # AI assistant guidance
|-- README.md                 # Quick start guide
|-- requirements.txt          # Python dependencies
|-- agents.md                 # GPT Codex agent instructions
|-- docs/                     # Documentation
|   |-- PROJECT_OVERVIEW.md   # This file
|   `-- ADDING_NEW_STRATEGY.md # Strategy development guide
|-- data/                     # Data files (not code)
|   |-- raw/                  # Source OHLCV CSV files
|   `-- baseline/             # Regression test baselines
|-- tools/                    # Development utilities
|   |-- generate_baseline_s01.py # Generate regression baselines
|   |-- benchmark_indicators.py  # Indicator performance tests
|   |-- benchmark_metrics.py     # Metrics performance tests
|   `-- test_all_ma_types.py     # Test all 11 MA types
|-- tests/                    # Pytest test suite
|   |-- conftest.py            # Shared fixtures (isolated storage, Flask client)
|   |-- test_sanity.py         # Infrastructure sanity checks
|   |-- test_regression_s01.py # S01 baseline regression
|   |-- test_s01_migration.py  # S01 migration validation
|   |-- test_s03_reversal_v10.py # S03 strategy tests
|   |-- test_s04_stochrsi.py   # S04 strategy tests
|   |-- test_metrics.py        # Metrics calculation tests
|   |-- test_export.py         # Export functionality tests
|   |-- test_indicators.py     # Indicator tests
|   |-- test_naming_consistency.py # camelCase naming guardrails
|   |-- test_walkforward.py    # Walk-forward analysis tests
|   |-- test_adaptive_wfa.py   # Adaptive WFA trigger detection tests
|   |-- test_server.py         # HTTP API tests
|   |-- test_storage.py        # Database storage tests
|   |-- test_db_management.py  # Multi-database management tests
|   |-- test_post_process.py   # Post-process module tests
|   |-- test_dsr.py            # Deflated Sharpe Ratio tests
|   |-- test_oos_selection.py  # OOS selection tests
|   |-- test_stress_test.py    # Stress test tests
|   |-- test_analytics.py          # Analytics equity aggregation tests
|   |-- test_multiprocess_score.py    # Multi-process scoring tests
|   |-- test_optuna_sanitization.py   # Optuna sanitization tests
|   |-- test_score_normalization.py   # Score normalization tests
|   |-- test_coverage_startup.py      # Initial Search Coverage mode tests
|   `-- test_strategy_loop_regression.py # Strategy loop performance regression
`-- src/                      # Application source code
    |-- run_backtest.py       # CLI backtest runner
    |-- core/                 # Core engines and utilities
    |   |-- backtest_engine.py   # Trade simulation engine
    |   |-- optuna_engine.py     # Optuna optimization engine
    |   |-- walkforward_engine.py # Walk-forward analysis engine
    |   |-- metrics.py           # Metrics calculation
    |   |-- analytics.py         # Portfolio equity aggregation for Analytics page
    |   |-- storage.py           # SQLite database functions
    |   |-- export.py            # Trade CSV export functions
    |   |-- post_process.py      # Forward Test and DSR validation
    |   `-- testing.py           # OOS selection and test utilities
    |-- indicators/           # Technical indicator library
    |   |-- ma.py              # Moving averages (11 types)
    |   |-- volatility.py      # ATR, NATR
    |   |-- oscillators.py     # RSI, StochRSI
    |   `-- trend.py           # ADX, trend indicators
    |-- strategies/           # Trading strategies
    |   |-- base.py            # BaseStrategy class
    |   |-- s01_trailing_ma/   # Trailing MA strategy
    |   |   |-- config.json    # Parameter schema
    |   |   `-- strategy.py    # Strategy implementation
    |   |-- s03_reversal_v10/  # Reversal strategy (T-Bands + close counts)
    |   |   |-- config.json
    |   |   `-- strategy.py
    |   `-- s04_stochrsi/      # StochRSI strategy
    |       |-- config.json
    |       `-- strategy.py
    |-- storage/              # Database storage (gitignored)
    |   |-- .gitkeep           # Directory marker
    |   |-- *.db               # SQLite database files (WAL mode, multiple supported)
    |   |-- journals/          # SQLite journal files
    |   `-- queue.json         # Scheduled run queue state
    |-- ui/                   # Web interface
    |   |-- server.py                 # Flask entrypoint + app creation + route registration
    |   |-- server_services.py        # Helpers/shared logic (no route decorators)
    |   |-- server_routes_data.py     # Pages + studies/tests/trades + presets + strategies + DB/CSV/queue endpoints
    |   |-- server_routes_run.py      # Optimization status/cancel + optimize/walkforward/backtest
    |   |-- server_routes_analytics.py # Analytics page + WFA summary API
    |   |-- templates/
    |   |   |-- index.html    # Start page (configuration)
    |   |   |-- results.html  # Results page (studies browser)
    |   |   `-- analytics.html # Analytics page (WFA research)
    |   `-- static/
    |       |-- js/           # Frontend JavaScript
    |       |   |-- main.js               # Start page logic
    |       |   |-- results-state.js      # Results state + localStorage/sessionStorage
    |       |   |-- results-format.js     # Results formatters + labels + MD5
    |       |   |-- results-tables.js     # Results table/chart renderers
    |       |   |-- results-controller.js # Results orchestration + API + events
    |       |   |-- api.js                # API client functions
    |       |   |-- strategy-config.js    # Dynamic form generation
    |       |   |-- ui-handlers.js        # Shared UI event handlers
    |       |   |-- optuna-ui.js          # Optuna Start-page UI helpers + coverage analysis
    |       |   |-- optuna-results-ui.js  # Optuna Results-page render helpers
    |       |   |-- post-process-ui.js    # Post process UI helpers
    |       |   |-- oos-test-ui.js        # OOS test UI helpers
    |       |   |-- wfa-results-ui.js     # WFA Results-page UI helpers
    |       |   |-- presets.js            # Preset management
    |       |   |-- results.js            # Results page initialization
    |       |   |-- queue.js              # Scheduled run queue management
    |       |   |-- dataset-preview.js    # WFA window layout preview
    |       |   |-- analytics.js          # Analytics page logic + state
    |       |   |-- analytics-equity.js   # Analytics equity curve rendering
    |       |   |-- analytics-filters.js  # Analytics filter panel management
    |       |   |-- analytics-table.js    # Analytics study table rendering
    |       |   |-- analytics-sets.js     # Analytics study sets management
    |       |   `-- utils.js              # Shared utility functions
    |       `-- css/
    |           `-- style.css # Light theme styles
    `-- presets/              # Saved parameter presets
        `-- *.json
```

## Architecture

### Core Principles

1. **Config-Driven Design**
   - Backend loads parameter schemas from each strategy's `config.json`
   - Frontend renders UI controls dynamically from `config.json`
   - Core modules remain strategy-agnostic

2. **camelCase Naming Convention**
   - Parameter names use camelCase end-to-end: Pine Script -> `config.json` -> Python -> Database
   - Examples: `maType`, `closeCountLong`, `rsiLen`
   - Internal control fields (`use_backtester`, `start`, `end`) may use snake_case but are excluded from UI/config

3. **Data Structure Ownership**
   - Structures live where they're populated:
     - `TradeRecord`, `StrategyResult` -> `backtest_engine.py`
     - `BasicMetrics`, `AdvancedMetrics` -> `metrics.py`
     - `OptimizationResult`, `OptunaConfig`, `OptimizationConfig`, `InMemoryJournalBackend` -> `optuna_engine.py`
     - `WFConfig`, `WFResult`, `WindowResult` -> `walkforward_engine.py`
     - Strategy params dataclass -> each strategy's `strategy.py`

4. **Optuna-Only Optimization**
   - Grid search removed; Optuna handles all optimization
   - Supports **single- and multi-objective** optimization (select **1-6 objectives**)
   - Multi-objective studies return a **Pareto front**; UI provides **primary objective** sorting
   - Supports **soft constraints** (e.g., Total Trades >= 30): feasible trials are prioritized; infeasible trials are retained but deprioritized
   - Samplers: Random, TPE (incl. multi-objective TPE), NSGA-II, NSGA-III
   - Budget modes: n_trials, timeout, patience
   - Pruning is supported for **single-objective** only (Optuna `should_prune()` does not support multi-objective)
   - **Initial Search Coverage**: optional systematic parameter space exploration (coverage mode) with block size hints and auto-fill warmup
   - **Bool group rules**: strategy `config.json` can declare invalid boolean combinations (e.g., `at_least_one_true`) to reduce wasted trials
   - **Trial deduplication**: duplicate parameter sets are detected and skipped; search space exhaustion triggers early stopping
   - **In-memory backend**: `InMemoryJournalBackend` replaces file-based journal storage for faster multiprocess optimization
   - **Trial log switch**: `trials_log` flag controls Optuna trial-level INFO logging (togglable from UI)

5. **Database Persistence**
   - All optimization results automatically saved to SQLite database
   - Multiple `.db` files supported with active DB switching
   - Studies browsable through web UI Results page
   - Trade exports generated on-demand from stored parameters
   - Original CSV files referenced, not duplicated

### Module Responsibilities

#### Core Engines (`src/core/`)

| Module | Purpose |
|--------|---------|
| `backtest_engine.py` | Bar-by-bar trade simulation, position management, data preparation |
| `optuna_engine.py` | Optuna optimization engine: single/multi-objective, constraints, samplers (TPE/Random/NSGA), pruning (single-objective only), Initial Search Coverage, trial deduplication, InMemoryJournalBackend, and database persistence |
| `walkforward_engine.py` | Rolling walk-forward analysis with calendar-based IS/OOS windows, stitched OOS equity, annualized WFE, adaptive re-optimization triggers (CUSUM, drawdown, inactivity), database persistence |
| `metrics.py` | Calculate BasicMetrics and AdvancedMetrics (Sharpe, RoMaD, Profit Factor, SQN, Ulcer Index, Consistency R²) |
| `analytics.py` | Portfolio equity aggregation: equal-weight curve merging, forward-fill alignment, annualized profit, max drawdown for aggregated curves |
| `storage.py` | SQLite database operations: save/load studies, manage trials/windows, handle CSV file references, multi-database management, study sets CRUD, queue state persistence |
| `export.py` | Export trade history to CSV (TradingView format) |
| `post_process.py` | Forward Test validation, DSR (Deflated Sharpe Ratio) analysis, profit degradation metrics |
| `testing.py` | OOS selection utilities, stress test candidate filtering, comparison metrics |

#### Indicators (`src/indicators/`)

| Module | Indicators |
|--------|------------|
| `ma.py` | SMA, EMA, WMA, DEMA, KAMA, HMA, ALMA, TMA, T3, VWMA, VWAP |
| `volatility.py` | ATR, NATR |
| `oscillators.py` | RSI, StochRSI |
| `trend.py` | ADX |

All indicators accessed via `get_ma()` facade for moving averages.

#### Strategies (`src/strategies/`)

Each strategy contains:
- `config.json` - Parameter schema with types, defaults, min/max, optimization ranges
- `strategy.py` - Params dataclass and strategy class with `run()` method

Strategies auto-discovered by `strategies/__init__.py` if both files exist.

#### UI (`src/ui/`)

**Backend (Flask):**
- `server.py` - Thin entrypoint: Flask app creation, route registration, test re-exports
- `server_services.py` - Helpers/shared logic (no route decorators), safe logging via `_get_logger()`
- `server_routes_data.py` - Pages + studies/tests/trades + presets + strategies + DB management + CSV browse + queue endpoints
- `server_routes_run.py` - Optimization status/cancel + optimize/walkforward/backtest (run endpoints)
- `server_routes_analytics.py` - Analytics page + WFA summary API endpoint

**Frontend (JavaScript):**
- `templates/index.html` - Start page: strategy configuration, coverage mode, trials log toggle, optimization launch, run queue
- `templates/results.html` - Results page: studies browser, trials/windows display, trade downloads
- `templates/analytics.html` - Analytics page: WFA research, multi-study comparison, filtering
- `static/js/main.js` - Start page logic and form handling
- `static/js/results-state.js` - Results page state management, localStorage/sessionStorage, URL helpers
- `static/js/results-format.js` - Results page formatters, labels, stableStringify, MD5 hashing
- `static/js/results-tables.js` - Results page table/chart renderers, row selection, parameter details
- `static/js/results-controller.js` - Results page orchestration, API calls, event binding, modals
- `static/js/queue.js` - Scheduled run queue management (add/remove/execute items)
- `static/js/dataset-preview.js` - WFA window layout preview and validation
- `static/js/analytics.js` - Analytics page logic, state management, study selection
- `static/js/analytics-equity.js` - Analytics equity curve SVG rendering
- `static/js/analytics-filters.js` - Analytics filter panel (strategy/symbol/TF/WFA/IS-OOS)
- `static/js/analytics-table.js` - Analytics sortable study table with checkbox selection
- `static/js/analytics-sets.js` - Analytics study sets management (save/load/reorder named collections)
- `static/js/api.js` - API client functions for all pages
- `static/css/style.css` - Light theme styling for all pages

### Data Flow

#### Optimization Flow (Optuna/WFA)
```
Start Page (index.html)
  -> User submits optimization (direct or via queue)
  -> server.py builds OptimizationConfig
  -> optuna_engine / walkforward_engine (fixed or adaptive)
  -> strategy (s01/s03/s04/...) + indicators
  -> backtest_engine (per trial/window)
  -> metrics.py
  -> storage.py
  -> active .db file (SQLite)
```

#### Results Viewing Flow
```
Results Page (results.html)
  -> GET /api/studies (from active database)
  -> storage.py loads study + trials/windows
  -> Display in UI
     - Click trial -> Generate trades on-demand
     - Delete study -> Remove from database
     - Update CSV path -> Update file reference
     - Switch database -> GET /api/databases + POST /api/databases/active
```

#### Analytics Flow
```
Analytics Page (analytics.html)
  -> GET /api/analytics/summary (filtered by strategy/symbol/TF/etc.)
  -> storage.py loads WFA studies with aggregated metrics
  -> Display summary table + equity curves
  -> Filter/sort/compare studies
```

#### Trade Export (On-Demand)
```
User clicks "Download Trades"
  -> Endpoint depends on context:
     - Single Backtest: POST /api/backtest/trades
     - IS: POST /api/studies/{id}/trials/{n}/trades
     - Forward Test: POST /api/studies/{id}/trials/{n}/ft-trades
     - OOS Test: POST /api/studies/{id}/trials/{n}/oos-trades
     - Manual Test: POST /api/studies/{id}/tests/{test_id}/trials/{n}/mt-trades
     - WFA Window: POST /api/studies/{id}/wfa/windows/{n}/trades
     - WFA Stitched: POST /api/studies/{id}/wfa/trades
  -> storage.py loads params/metadata (or backtest re-runs directly)
  -> backtest_engine re-runs strategy
  -> export.py outputs CSV
```

### Database Schema

SQLite database stored in `src/storage/` directory. Multiple `.db` files supported with active DB switching. WAL (Write-Ahead Logging) mode enabled.

#### Tables

**studies** - Optimization study metadata
- Primary key: `study_id` (UUID)
- Unique constraint: `study_name`
- Fields: strategy_id, strategy_version, optimization_mode ('optuna'/'wfa'), status, trial counts, best value, filters applied, configuration JSON, CSV file path, timestamps
- **Adaptive WFA fields:** adaptive_mode, max_oos_period_days, min_oos_trades, check_interval_trades, cusum_threshold, dd_threshold_multiplier, inactivity_multiplier
- **Stitched OOS fields:** stitched_oos_equity_curve, stitched_oos_timestamps_json, stitched_oos_window_ids_json, stitched_oos_net_profit_pct, stitched_oos_max_drawdown_pct, stitched_oos_total_trades, stitched_oos_winning_trades, stitched_oos_win_rate
- **Window aggregate fields:** profitable_windows, total_windows, median_window_profit, median_window_wr, worst_window_profit, worst_window_dd

**trials** - Individual Optuna trial results (for Optuna mode studies)
- Foreign key: `study_id` -> studies
- Unique constraint: (study_id, trial_number)
- Stores params (JSON) and computed metrics
- **Multi-objective:** stores `objective_values_json` aligned with selected objectives
- **Constraints:** stores `constraint_values_json`, `constraints_satisfied`, and `is_pareto_optimal` flags

**wfa_windows** - Walk-Forward Analysis window results (for WFA mode studies)
- Foreign key: `study_id` -> studies
- Unique constraint: (study_id, window_number)
- Fields: best parameters (JSON), IS/OOS metrics, IS/OOS equity curves (JSON arrays), WFE, oos_winning_trades

**study_sets** - Named collections of WFA studies for Analytics page
- Primary key: `id` (autoincrement)
- Unique constraint: case-insensitive `name`
- Fields: name, sort_order, created_at

**study_set_members** - Many-to-many link between study sets and studies
- Unique constraint: (set_id, study_id)
- Foreign keys: set_id -> study_sets (CASCADE), study_id -> studies (CASCADE)

### API Endpoints

#### Page Routes
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Serve Start page (optimization configuration) |
| `/results` | GET | Serve Results page (studies browser) |
| `/analytics` | GET | Serve Analytics page (WFA research) |

#### Optimization
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/backtest` | POST | Run single backtest (no storage) |
| `/api/backtest/trades` | POST | Download trades CSV for single backtest |
| `/api/optimize` | POST | Run Optuna optimization, save to database |
| `/api/walkforward` | POST | Run WFA (fixed or adaptive mode), save to database |
| `/api/optimization/status` | GET | Get current optimization state |
| `/api/optimization/cancel` | POST | Cancel running optimization |

#### Studies Management
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/studies` | GET | List all saved studies with summary info |
| `/api/studies/<study_id>` | GET | Load complete study (metadata + trials/windows) |
| `/api/studies/<study_id>` | DELETE | Delete study from database |
| `/api/studies/<study_id>/update-csv-path` | POST | Update CSV file path reference |
| `/api/studies/<study_id>/test` | POST | Run manual test on selected trials |
| `/api/studies/<study_id>/tests` | GET | List manual tests |
| `/api/studies/<study_id>/tests/<test_id>` | GET | Load manual test results |
| `/api/studies/<study_id>/tests/<test_id>` | DELETE | Delete manual test |
| `/api/studies/<study_id>/trials/<trial_number>/trades` | POST | Generate and download IS trades CSV |
| `/api/studies/<study_id>/trials/<trial_number>/ft-trades` | POST | Generate and download Forward Test trades CSV |
| `/api/studies/<study_id>/trials/<trial_number>/oos-trades` | POST | Generate and download OOS Test trades CSV |
| `/api/studies/<study_id>/tests/<test_id>/trials/<trial_number>/mt-trades` | POST | Generate and download Manual Test trades CSV |
| `/api/studies/<study_id>/wfa/windows/<window_number>` | GET | Get WFA window details with module trials |
| `/api/studies/<study_id>/wfa/windows/<window_number>/equity` | POST | Generate WFA window equity curve on-demand |
| `/api/studies/<study_id>/wfa/windows/<window_number>/trades` | POST | Download WFA window trades CSV |
| `/api/studies/<study_id>/wfa/trades` | POST | Generate and download stitched WFA OOS trades CSV |

#### Database Management
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/databases` | GET | List all `.db` files with active marker |
| `/api/databases/active` | POST | Switch active database |
| `/api/databases` | POST | Create new timestamped database |

#### CSV Browse
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/csv/browse` | GET | Browse CSV directory (files + subdirectories) |

#### Run Queue
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/queue` | GET | Load scheduled run queue state |
| `/api/queue` | PUT | Save/update queue state |
| `/api/queue` | DELETE | Clear queue state |

#### Analytics
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/analytics/summary` | GET | WFA studies summary with filters and aggregated metrics |
| `/api/analytics/equity` | POST | Aggregate equity curves for selected study IDs |
| `/api/analytics/equity/batch` | POST | Batch aggregate equity curves for multiple groups |
| `/api/analytics/studies/<study_id>/window-boundaries` | GET | Get WFA window boundary timestamps |

#### Study Sets
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/analytics/sets` | GET | List all study sets |
| `/api/analytics/sets` | POST | Create a new study set |
| `/api/analytics/sets/<set_id>` | PUT | Update study set (name, study_ids, sort_order) |
| `/api/analytics/sets/<set_id>` | DELETE | Delete a study set |
| `/api/analytics/sets/reorder` | PUT | Reorder study sets |

#### Strategy Configuration
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/strategies` | GET | List all available strategies |
| `/api/strategies/<strategy_id>` | GET | Get strategy metadata |
| `/api/strategy/<strategy_id>/config` | GET | Get strategy parameter schema |

#### Presets
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/presets` | GET | List all saved presets |
| `/api/presets/<name>` | GET | Load preset values |
| `/api/presets` | POST | Create new preset |
| `/api/presets/<name>` | PUT | Update existing preset |
| `/api/presets/defaults` | PUT | Update default preset values |
| `/api/presets/<name>` | DELETE | Delete preset |
| `/api/presets/import-csv` | POST | Import preset from CSV parameter block |

## Running the Application

### Web Server
```bash
cd src/ui
python server.py
```
Opens at http://127.0.0.1:5000

### CLI Backtest
```bash
cd src
python run_backtest.py --csv ../data/raw/OKX_LINKUSDT.P,\ 15\ 2025.05.01-2025.11.20.csv
```

### Tests
```bash
pytest tests/ -v
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `CLAUDE.md` | AI assistant instructions (for Claude models) |
| `agents.md` | GPT Codex agent instructions |
| `docs/ADDING_NEW_STRATEGY.md` | How to add new strategies |
| `data/baseline/` | Regression test reference data |
| `tools/generate_baseline_s01.py` | Regenerate S01 baseline |

## Current Strategies

| ID | Name | Description |
|----|------|-------------|
| `s01_trailing_ma` | S01 Trailing MA | Complex trailing MA strategy with 11 MA types, close counts, ATR stops |
| `s03_reversal_v10` | S03 Reversal | Reversal strategy using close-count confirmation and T-Bands hysteresis |
| `s04_stochrsi` | S04 StochRSI | StochRSI swing strategy with swing-based stops |

## Adding New Strategies

See `docs/ADDING_NEW_STRATEGY.md` for complete instructions on converting PineScript strategies to Python and integrating them into the platform.
