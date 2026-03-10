# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working with this repository.

## Project: Merlin

Cryptocurrency trading strategy backtesting and Optuna optimization platform with a Flask SPA frontend.

## Running the Application

### Web Server
```bash
cd src/ui
python server.py
```
Server runs at http://127.0.0.1:5000

### CLI Backtest
```bash
cd src
python run_backtest.py --csv ../data/raw/OKX_LINKUSDT.P,\ 15\ 2025.05.01-2025.11.20.csv
```

### Tests
```bash
pytest tests/ -v
```

### Dependencies
```bash
pip install -r requirements.txt
```
Key: Flask, pandas, numpy, matplotlib, optuna==4.6.0

## Architecture

### Core Principles

1. **Config-driven design** - Parameter schemas in `config.json`, UI renders dynamically
2. **camelCase naming** - End-to-end: Pine Script -> config.json -> Python -> CSV
3. **Optuna-only optimization** - Grid search removed; optional Initial Search Coverage mode for systematic parameter exploration
4. **Strategy isolation** - Each strategy owns its params dataclass
5. **Rolling WFA** - Calendar-based IS/OOS windows, stitched OOS equity, annualized WFE, adaptive re-optimization triggers
6. **In-memory backend** - RAM-based Optuna journal storage for faster multiprocess optimization
7. **Trial deduplication** - Automatic detection/skipping of duplicate parameter sets with search space exhaustion early stopping
8. **Database persistence** - All optimization results automatically saved to SQLite, browsable through web UI
9. **Multi-database support** - Multiple `.db` files with active DB switching
10. **Three-page UI** - Start (configuration), Results (studies browser), Analytics (WFA research)

### Directory Structure
```
src/
|-- core/                     # Engines + utilities
|   |-- backtest_engine.py    # Trade simulation, TradeRecord, StrategyResult
|   |-- optuna_engine.py      # Optimization, OptimizationResult, OptunaConfig, InMemoryJournalBackend, coverage, dedup
|   |-- walkforward_engine.py # WFA orchestration
|   |-- metrics.py            # BasicMetrics, AdvancedMetrics (incl. Consistency R²)
|   |-- analytics.py          # Portfolio equity aggregation for Analytics page
|   |-- storage.py            # SQLite database operations
|   |-- export.py             # Trade CSV export functions
|   |-- post_process.py       # Forward Test and DSR validation
|   `-- testing.py            # OOS selection and test utilities
|-- indicators/               # Technical indicators
|   |-- ma.py                 # 11 MA types via get_ma()
|   |-- volatility.py         # ATR, NATR
|   `-- oscillators.py        # RSI, StochRSI
|-- strategies/               # Trading strategies
|   |-- base.py               # BaseStrategy class
|   |-- s01_trailing_ma/
|   |-- s03_reversal_v10/
|   `-- s04_stochrsi/
|-- storage/                  # Database storage (gitignored)
|   |-- *.db                  # SQLite database files (WAL mode, multiple supported)
|   |-- journals/             # SQLite journal files
|   `-- queue.json            # Scheduled run queue state
`-- ui/                       # Web interface
    |-- server.py                 # Thin entrypoint + app creation + route registration
    |-- server_services.py        # Helpers/shared logic (no route decorators)
    |-- server_routes_data.py     # Pages + studies/tests/trades + presets + strategies + DB/CSV/queue endpoints
    |-- server_routes_run.py      # Optimization status/cancel + optimize/walkforward/backtest
    |-- server_routes_analytics.py # Analytics page + WFA summary API
    |-- templates/
    |   |-- index.html        # Start page (configuration)
    |   |-- results.html      # Results page (studies browser)
    |   `-- analytics.html    # Analytics page (WFA research)
    `-- static/
        |-- js/
        |   |-- main.js               # Start page logic
        |   |-- results-state.js      # Results state + localStorage/sessionStorage + URL helpers
        |   |-- results-format.js     # Results formatters + labels + MD5
        |   |-- results-tables.js     # Results table/chart renderers + row selection
        |   |-- results-controller.js # Results orchestration + API calls + event binding
        |   |-- api.js                # API client
        |   |-- strategy-config.js    # Dynamic form generation from config.json
        |   |-- ui-handlers.js        # Shared UI event handlers
        |   |-- optuna-ui.js          # Optuna Start-page UI helpers + coverage analysis
        |   |-- optuna-results-ui.js  # Optuna Results-page UI helpers
        |   |-- post-process-ui.js    # Post process UI helpers
        |   |-- oos-test-ui.js        # OOS test UI helpers
        |   |-- wfa-results-ui.js     # WFA Results-page UI helpers
        |   |-- presets.js            # Preset management
        |   |-- results.js            # Results page initialization
        |   |-- queue.js              # Scheduled run queue management
        |   |-- dataset-preview.js    # WFA window layout preview
        |   |-- analytics.js          # Analytics page logic + state
        |   |-- analytics-equity.js   # Analytics equity curve rendering
        |   |-- analytics-filters.js  # Analytics filter panel management
        |   |-- analytics-table.js    # Analytics study table rendering
        |   |-- analytics-sets.js     # Analytics study sets management
        |   `-- utils.js              # Shared utility functions
        `-- css/
```

### Data Structure Ownership

| Structure | Module |
|-----------|--------|
| `TradeRecord`, `StrategyResult` | `backtest_engine.py` |
| `BasicMetrics`, `AdvancedMetrics` | `metrics.py` |
| `OptimizationResult`, `OptunaConfig`, `OptimizationConfig`, `InMemoryJournalBackend` | `optuna_engine.py` |
| `WFConfig`, `WFResult`, `WindowResult` | `walkforward_engine.py` |
| `aggregate_equity_curves` | `analytics.py` |
| Strategy params dataclass | Each strategy's `strategy.py` |

## Parameter Naming Rules

**CRITICAL: Use camelCase everywhere**

- Correct: `maType`, `closeCountLong`, `rsiLen`, `stopLongMaxPct`
- Avoid: `ma_type`, `close_count_long`, `rsi_len`, `stop_long_max_pct`

Internal control fields (`use_backtester`, `start`, `end`) may use snake_case but are excluded from UI/config.

**Do NOT add:**
- `to_dict()` methods - use `dataclasses.asdict(params)` instead
- Snake<->camel conversion helpers
- Feature flags

## Adding New Strategies

See `docs/ADDING_NEW_STRATEGY.md` for complete guide.

Quick checklist:
1. Create `src/strategies/<strategy_id>/` directory
2. Create `config.json` with parameter schema (camelCase)
3. Create `strategy.py` with params dataclass and strategy class
4. Ensure `STRATEGY_ID`, `STRATEGY_NAME`, `STRATEGY_VERSION` class attributes
5. Implement `run(df, params, trade_start_idx) -> StrategyResult` static method
6. Strategy auto-discovered - no manual registration needed

## Database Operations

### Accessing Studies
```python
from core.storage import list_studies, load_study_from_db

# List all saved studies
studies = list_studies()
for study in studies:
    print(f"{study['study_name']}: {study['saved_trials']} trials")

# Load complete study with trials/windows
study_data = load_study_from_db(study_id)
print(study_data['study'])      # Study metadata
print(study_data['trials'])     # Optuna trials (if mode='optuna')
print(study_data['windows'])    # WFA windows (if mode='wfa')
print(study_data['csv_exists']) # Whether CSV file still exists

### Understanding Study Storage

**Optuna studies:**
- Saved to `studies` table (metadata) + `trials` table (parameter sets)
- Trials include: params (JSON), metrics, composite score
- Multi-objective studies store objective vectors and Pareto/feasibility flags (constraints)
- Study summaries may include completed/failed/pruned counts; results lists include COMPLETE trials (failed trials are retained only if explicitly stored for debugging)
- Optional filters (by score/profit threshold) may reduce stored trials for UI browsing

**WFA studies:**
- Saved to `studies` table (metadata) + `wfa_windows` table (per-window results)
- Each window includes: best params, IS/OOS metrics, equity curves (JSON arrays)
- WFE (Walk-Forward Efficiency) stored as `best_value`

### Database Location

**Note:** Database files are gitignored. Only `.gitkeep` files are tracked.

## Common Tasks

### Running Single Backtest
```python
from core.backtest_engine import load_data, prepare_dataset_with_warmup
from strategies.s01_trailing_ma.strategy import S01TrailingMA

df = load_data("data/raw/OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv")
df_prepared, trade_start_idx = prepare_dataset_with_warmup(df, start, end, warmup_bars=1000)
result = S01TrailingMA.run(df_prepared, params, trade_start_idx)

### Calculating Metrics
```python
from core import metrics
basic = metrics.calculate_basic(result, initial_capital=100.0)
advanced = metrics.calculate_advanced(result)  # includes consistency_score (R²)

### Walk-Forward Analysis (Rolling)
```python
from core.walkforward_engine import WFConfig, WalkForwardEngine

wf_config = WFConfig(
    strategy_id="s01_trailing_ma",
    is_period_days=180,
    oos_period_days=60,
    warmup_bars=1000,
)
engine = WalkForwardEngine(wf_config, base_config_template, optuna_settings)
wf_result = engine.run_wf_optimization(df)

### Walk-Forward Analysis (Adaptive)
```python
wf_config = WFConfig(
    strategy_id="s01_trailing_ma",
    is_period_days=180,
    oos_period_days=60,
    warmup_bars=1000,
    adaptive_mode=True,
    max_oos_period_days=90,
    min_oos_trades=5,
    cusum_threshold=5.0,
    dd_threshold_multiplier=1.5,
    inactivity_multiplier=5.0,
)

### Using Indicators
```python
from indicators.ma import get_ma
from indicators.volatility import atr
from indicators.oscillators import rsi, stoch_rsi

ma_values = get_ma(df["Close"], "HMA", 50)
atr_values = atr(df["High"], df["Low"], df["Close"], 14)
rsi_values = rsi(df["Close"], 14)

## Testing

### Run All Tests
```bash
pytest tests/ -v

### Key Test Files
- `conftest.py` - Shared fixtures (isolated storage, Flask test client)
- `test_sanity.py` - Infrastructure checks
- `test_regression_s01.py` - S01 baseline regression
- `test_s03_reversal_v10.py` - S03 strategy tests
- `test_s04_stochrsi.py` - S04 strategy tests
- `test_naming_consistency.py` - camelCase guardrails
- `test_storage.py` - Database storage tests
- `test_server.py` - HTTP API endpoint tests
- `test_post_process.py` - Post-process module tests
- `test_dsr.py` - Deflated Sharpe Ratio tests
- `test_oos_selection.py` - OOS selection tests
- `test_stress_test.py` - Stress test tests
- `test_analytics.py` - Analytics equity aggregation tests
- `test_adaptive_wfa.py` - Adaptive WFA trigger detection tests
- `test_db_management.py` - Multi-database management tests
- `test_coverage_startup.py` - Initial Search Coverage mode tests
- `test_strategy_loop_regression.py` - Strategy loop performance regression tests

### Regenerate S01 Baseline
```bash
python tools/generate_baseline_s01.py

## Optuna: Multi-objective & constraints

**Key behavioral rules (keep these consistent across backend + UI):**

- **Single objective vs multi-objective**
  - 1 objective: create study with `direction=...`
  - 2+ objectives: create study with `directions=[...]` and return a tuple of objective values
  - Multi-objective results are a **Pareto front**; UI sorts Pareto-first then by **primary objective**

- **Pruning**
  - Pruning is supported for **single-objective** only.
  - Optuna `Trial.should_prune()` does **not** support multi-objective optimization.

- **Invalid objectives / missing metrics**
  - If an objective value is missing/NaN, return `float("nan")` (or a NaN tuple for multi-objective).
  - Optuna treats NaN returns as **FAILED trials** (study continues).
  - Failed trials are ignored by Optuna samplers (they do not affect future suggestions).

- **Constraints**
- Constraints are **soft**: infeasible trials are retained but deprioritized in UI and "best" selection.
  - `constraints_func` is evaluated only after **successful** trials; it is not called for failed/pruned trials.
- Sorting/labeling should follow: feasible Pareto -> feasible non-Pareto -> infeasible (then by total violation, then primary objective).

- **Initial Search Coverage**
  - Optional coverage mode (`coverage_mode: true`) for systematic parameter space exploration during startup.
  - Generates structured coverage trials from categorical combinations and numeric quantiles.
  - UI provides coverage analysis with block size hints (multipliers: 1, 3, 5, 9, 17) and auto-fill for warmup trials.
  - Bool group rules (e.g., `at_least_one_true`) reduce coverage block size by excluding invalid combinations.

- **Trial deduplication**
  - Duplicate parameter sets are detected via deterministic JSON key comparison.
  - Duplicates are marked FAIL with `merlin.duplicate_skipped` attribute and skipped.
  - Soft duplicate cycle limit (`dispatcher_duplicate_cycle_limit`, default 18) prevents infinite loops.
  - Search space exhaustion triggers early stopping.

- **Trial logging**
  - `trials_log` flag (default false) controls Optuna trial-level INFO logging.
  - Togglable from UI via "Trials Log" checkbox.

- **In-memory backend**
  - `InMemoryJournalBackend` replaces file-based journal storage for multiprocess optimization.
  - Uses `mp.Manager().list()` for process-shared storage.

- **Concurrency**
- Keep Merlin's existing multi-process optimization architecture. Do not replace it with `study.optimize(..., n_jobs=...)` threading.


## UI Notes

### Three-Page Architecture

**Start Page (`/` - index.html):**
- Strategy selection and parameter configuration
- Optuna settings (objectives + primary objective, budget, sampler, pruner, constraints)
- Initial Search Coverage mode toggle with coverage analysis and warmup auto-fill
- Trials Log toggle for Optuna trial-level logging control
- Walk-Forward Analysis settings (IS/OOS periods, adaptive mode)
- Scheduled run queue management
- CSV file browser
- Dataset preview (WFA window layout)
- Run Optuna or Run WFA buttons
- Results automatically saved to database
- Light theme UI with dynamic forms from `config.json`

**Results Page (`/results` - results.html):**
- Studies Manager: List all saved optimization studies
- Database switching (multi-database support)
- Study details: View trials (Optuna) or windows (WFA)
- Pareto badge + constraint feasibility indicators for Optuna trials
- Equity curve visualization
- Parameter comparison tables
- Download trades CSV for IS/FT/OOS/Manual/WFA results (on-demand generation)
- Delete studies or update CSV file paths

**Analytics Page (`/analytics` - analytics.html):**
- WFA-focused research and analysis
- Multi-study equity curve comparison
- Aggregated (portfolio) equity curve with annualized profit and max drawdown
- Focused study mode with WFA window boundary overlays on equity chart
- Study sets: save/load/reorder named collections of studies (persisted in DB)
- Study summary table with sorting and filtering
- Filter by strategy, symbol, timeframe, WFA mode, IS/OOS periods
- Aggregated metrics: profit %, max DD %, win rate, WFE %, profitable windows %

### Frontend Architecture

- **main.js**: Start page logic, form handling, optimization launch
- **results-state.js**: Results page state management, localStorage/sessionStorage, URL query helpers
- **results-format.js**: Results page formatters, labels, stableStringify, MD5 hashing
- **results-tables.js**: Results page table/chart renderers, row selection, parameter details
- **results-controller.js**: Results page orchestration, API calls, event binding, modals
- **api.js**: Centralized API calls for all pages
- **strategy-config.js**: Dynamic form generation from `config.json`
- **ui-handlers.js**: Shared UI event handlers
- **optuna-ui.js**: Optuna Start-page UI helpers (objectives/constraints/sampler panels, coverage analysis)
- **optuna-results-ui.js**: Optuna Results-page UI helpers (dynamic columns/badges)
- **post-process-ui.js**: Post process UI helpers (Forward Test, DSR panels)
- **oos-test-ui.js**: OOS test UI helpers
- **wfa-results-ui.js**: WFA Results-page UI helpers
- **presets.js**: Preset management (load/save/import)
- **results.js**: Results page initialization
- **queue.js**: Scheduled run queue management (add/remove/execute items)
- **dataset-preview.js**: WFA window layout preview and validation
- **analytics.js**: Analytics page logic, state management, study selection
- **analytics-equity.js**: Analytics equity curve SVG rendering
- **analytics-filters.js**: Analytics filter panel (strategy/symbol/TF/WFA/IS-OOS)
- **analytics-table.js**: Analytics sortable study table with checkbox selection
- **analytics-sets.js**: Analytics study sets management (save/load/reorder named collections)
- **utils.js**: Shared utility functions
- Forms generated dynamically from `config.json`
- Strategy dropdown auto-populated from discovered strategies
- No hardcoded parameters in frontend

### Backend Architecture (server split)

- **server.py**: Thin entrypoint, Flask app creation, route registration, test re-exports
- **server_services.py**: All helper/utility functions (no route decorators), safe logging via `_get_logger()`
- **server_routes_data.py**: Pages + studies/tests/trades + presets + strategies + DB management + CSV browse + queue endpoints
- **server_routes_run.py**: Optimization status/cancel + optimize/walkforward/backtest (run endpoints)
- **server_routes_analytics.py**: Analytics page + WFA summary API endpoint

## API Endpoints Reference

### Page Routes
- `GET /` - Serve Start page
- `GET /results` - Serve Results page
- `GET /analytics` - Serve Analytics page

### Optimization
- `POST /api/optimize` - Run Optuna optimization, returns study_id
- `POST /api/walkforward` - Run WFA (fixed or adaptive mode), returns study_id
- `POST /api/backtest` - Run single backtest (no database storage)
- `POST /api/backtest/trades` - Download trades CSV for single backtest
- `GET /api/optimization/status` - Get current optimization state
- `POST /api/optimization/cancel` - Cancel running optimization

### Studies Management

- `GET /api/studies` - List all saved studies
- `GET /api/studies/<study_id>` - Load study with trials/windows
- `DELETE /api/studies/<study_id>` - Delete study
- `POST /api/studies/<study_id>/update-csv-path` - Update CSV path
- `POST /api/studies/<study_id>/test` - Run manual test on selected trials
- `GET /api/studies/<study_id>/tests` - List manual tests
- `GET /api/studies/<study_id>/tests/<test_id>` - Load manual test results
- `DELETE /api/studies/<study_id>/tests/<test_id>` - Delete manual test
- `POST /api/studies/<study_id>/trials/<trial_number>/trades` - Download IS trades CSV
- `POST /api/studies/<study_id>/trials/<trial_number>/ft-trades` - Download Forward Test trades CSV
- `POST /api/studies/<study_id>/trials/<trial_number>/oos-trades` - Download OOS Test trades CSV
- `POST /api/studies/<study_id>/tests/<test_id>/trials/<trial_number>/mt-trades` - Download Manual Test trades CSV
- `GET /api/studies/<study_id>/wfa/windows/<window_number>` - Get WFA window details with module trials
- `POST /api/studies/<study_id>/wfa/windows/<window_number>/equity` - Generate WFA window equity curve on-demand
- `POST /api/studies/<study_id>/wfa/windows/<window_number>/trades` - Download WFA window trades CSV
- `POST /api/studies/<study_id>/wfa/trades` - Download stitched WFA OOS trades CSV

### Database Management
- `GET /api/databases` - List all `.db` files with active marker
- `POST /api/databases/active` - Switch active database
- `POST /api/databases` - Create new timestamped database

### CSV Browse
- `GET /api/csv/browse` - Browse CSV directory (files + subdirectories)

### Run Queue
- `GET /api/queue` - Load scheduled run queue state
- `PUT /api/queue` - Save/update queue state
- `DELETE /api/queue` - Clear queue state

### Analytics
- `GET /api/analytics/summary` - WFA studies summary with filters and aggregated metrics
- `POST /api/analytics/equity` - Aggregate equity curves for selected study IDs
- `POST /api/analytics/equity/batch` - Batch aggregate equity curves for multiple groups
- `GET /api/analytics/studies/<study_id>/window-boundaries` - Get WFA window boundary timestamps

### Study Sets
- `GET /api/analytics/sets` - List all study sets
- `POST /api/analytics/sets` - Create a new study set
- `PUT /api/analytics/sets/<set_id>` - Update study set (name, study_ids, sort_order)
- `DELETE /api/analytics/sets/<set_id>` - Delete a study set
- `PUT /api/analytics/sets/reorder` - Reorder study sets

### Strategy & Presets
- `GET /api/strategies` - List available strategies
- `GET /api/strategies/<strategy_id>` - Get strategy metadata
- `GET /api/strategy/<strategy_id>/config` - Get strategy parameter schema
- `GET /api/presets` - List presets
- `POST /api/presets` - Create preset
- `GET/PUT/DELETE /api/presets/<name>` - Load/update/delete preset
- `PUT /api/presets/defaults` - Update default preset values
- `POST /api/presets/import-csv` - Import preset from CSV parameter block

## Performance Considerations

- Use vectorized pandas/numpy operations
- Pre-extract NumPy arrays from DataFrame columns before strategy loops (`.to_numpy()`)
- Reuse indicator calculations where possible
- Avoid expensive logging in hot paths (optimization loops)
- `trade_start_idx` skips warmup bars in simulation
- Database uses WAL mode for concurrent read access
- Bulk inserts used for saving trials (executemany, not loop)
- In-memory Optuna backend eliminates file I/O for trial communication between processes
- Trial deduplication prevents wasted evaluations of already-seen parameter sets

## Current Strategies

| ID | Name | Description |
|----|------|-------------|
| `s01_trailing_ma` | S01 Trailing MA | Complex trailing MA with 11 MA types, close counts, ATR stops |
| `s03_reversal_v10` | S03 Reversal | Reversal strategy using close-count confirmation and T-Bands hysteresis |
| `s04_stochrsi` | S04 StochRSI | StochRSI swing strategy with swing-based stops |

## Key Files for Reference

| Purpose | File |
|---------|------|
| Full architecture | `docs/PROJECT_OVERVIEW.md` |
| Adding strategies | `docs/ADDING_NEW_STRATEGY.md` |
| Database operations | `src/core/storage.py` |
| WFA engine (fixed + adaptive) | `src/core/walkforward_engine.py` |
| Start page logic | `src/ui/static/js/main.js` |
| Results page logic | `src/ui/static/js/results-controller.js` (orchestration) |
| Analytics page logic | `src/ui/static/js/analytics.js` |
| Analytics study sets | `src/ui/static/js/analytics-sets.js` |
| Equity aggregation | `src/core/analytics.py` |
| Queue management | `src/ui/static/js/queue.js` |
| Flask API entrypoint | `src/ui/server.py` |
| Flask services/helpers | `src/ui/server_services.py` |
| Flask data routes | `src/ui/server_routes_data.py` |
| Flask run routes | `src/ui/server_routes_run.py` |
| Flask analytics routes | `src/ui/server_routes_analytics.py` |
| S03 example | `src/strategies/s03_reversal_v10/strategy.py` |
| S04 example | `src/strategies/s04_stochrsi/strategy.py` |
| config.json example | `src/strategies/s04_stochrsi/config.json` |
| Test baseline | `data/baseline/` |
