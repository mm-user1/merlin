# Merlin

Config-driven backtesting and Optuna optimization platform for cryptocurrency trading strategies with SQLite database persistence and web-based studies management.

## Features

- **Database persistence** - All optimization results automatically saved to SQLite database
- **Multi-database support** - Multiple `.db` files with active DB switching
- **Studies browser** - Web UI for browsing, opening, and managing historical optimization studies
- **Multi-strategy support** - S01 Trailing MA, S03 Reversal, and S04 StochRSI included, easily extensible
- **Optuna optimization** - Single- and multi-objective optimization (1-6 objectives) with Pareto front results, primary-objective sorting, and multiple samplers (Random, TPE/MOTPE, NSGA-II/NSGA-III)
- **Initial Search Coverage** - Optional systematic parameter space exploration during optimization startup with configurable coverage block sizes
- **Soft constraints (Optuna)** - Configure feasibility rules (e.g., Total Trades >= 30). Results show feasible/infeasible indicators; infeasible trials are deprioritized, not discarded.
- **Bool group rules** - Declare invalid boolean parameter combinations (e.g., `at_least_one_true`) in strategy `config.json` to reduce wasted trials
- **Trial deduplication** - Automatic detection and skipping of duplicate parameter sets with search space exhaustion early stopping
- **In-memory backend** - RAM-based Optuna storage (InMemoryJournalBackend) for faster multiprocess optimization
- **Robust trial handling** - If an objective returns NaN, the trial is marked FAIL (study continues) and failed trials are ignored by samplers.
- **Trial log switch** - Toggle Optuna trial-level logging on/off from the UI
- **Walk-forward analysis** - IS/OOS validation with stitched equity curves and WFE metrics, fixed and adaptive modes
- **Adaptive WFA** - Re-optimization triggers based on CUSUM, drawdown degradation, and inactivity detection
- **WFA Analytics** - Research-focused analytics page with multi-study comparison, filtering, equity visualization, and aggregated portfolio curves
- **Study sets** - Save named collections of WFA studies for quick recall and comparison on Analytics page
- **Scheduled run queue** - Queue multiple optimization runs for sequential execution
- **Three-page UI** - Start page for configuration, Results page for studies management, Analytics page for WFA research
- **On-demand trade export** - Generate TradingView-compatible CSV for IS, Forward Test, OOS Test, Manual Test, and WFA exports
- **Config-driven architecture** - Add new strategies via `config.json` + `strategy.py` only

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start web server
cd src/ui
python server.py
```

Open http://127.0.0.1:5000 in your browser.

## Project Structure

```
project-root/
|-- src/
|   |-- core/           # Backtest, Optuna, WFA engines + metrics + analytics + database + export + post-process + testing
|   |-- indicators/     # MA (11 types), ATR, RSI, StochRSI
|   |-- strategies/     # s01_trailing_ma, s03_reversal_v10, s04_stochrsi
|   |-- storage/        # SQLite databases (multiple .db files) + queue state
|   `-- ui/             # Flask server + three-page frontend (Start/Results/Analytics)
|-- data/               # OHLCV CSVs and regression baselines
|-- tests/              # Pytest test suite
|-- tools/              # Development utilities
`-- docs/               # Documentation
```

## Documentation

- [Project Overview](docs/PROJECT_OVERVIEW.md) - Architecture and module details
- [Adding New Strategy](docs/ADDING_NEW_STRATEGY.md) - PineScript to Python conversion guide

## Usage

### Start Page (Configuration)
1. Browse and select OHLCV CSV data (or use included `data/raw/OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv`)
2. Select strategy from dropdown
3. Configure parameters via dynamic form
4. Enable Initial Search Coverage mode for systematic parameter space exploration
5. Toggle trial-level logging on/off
6. Preview WFA window layout before running
7. Run Optuna optimization or Walk-Forward Analysis (fixed or adaptive)
8. Queue multiple runs for sequential execution
9. Results automatically saved to database

### Results Page (Studies Browser)
1. View all historical optimization studies
2. Switch between databases
3. Select and open any study to view trials/windows
4. Analyze equity curves and performance metrics
5. Download trades CSV for IS/FT/OOS/Manual/WFA results (TradingView format)
6. Delete old studies or update CSV file paths

### Analytics Page (WFA Research)
1. Filter WFA studies by strategy, symbol, timeframe, WFA mode, IS/OOS periods
2. Compare multi-study equity curves
3. View aggregated (portfolio) equity curve with annualized profit and max drawdown
4. Focus on a single study to see WFA window boundary overlays
5. Save and load study sets for quick recall
6. Sort and analyze aggregated metrics (profit, drawdown, WFE, win rate)

## CLI Backtest

```bash
cd src
python run_backtest.py --csv ../data/raw/OKX_LINKUSDT.P,\ 15\ 2025.05.01-2025.11.20.csv
```

## Tests

```bash
pytest tests/ -v
```
