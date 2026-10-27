# Merlin

Merlin is a config-driven cryptocurrency strategy backtesting and
optimization platform with SQLite persistence and a Flask single-page web UI.

## Capabilities

- Single-strategy backtests with dynamic config-driven parameter forms.
- Backtester V1 optimization through Optuna or deterministic Fast Grid.
- Backtester V2 optimization through deterministic core-owned Grid V2,
  including full and budgeted sampled planning.
- V2 Fixed research for one scalar configuration, with optional OOS or regular
  day/calendar WFA through the Grid engine.
- Fixed and Adaptive walk-forward analysis with stitched OOS results.
- Opt-in V2 risk-bracket admission for Parquet instruments, with a fixed entry
  leverage cap and selected-run diagnostics across testing and WFA.
- SQLite study history, multiple databases, manual/forward/OOS testing, and
  trade export.
- Start, Results, and Analytics pages, saved study sets, and a scheduled run
  Queue.
- Local V2 Strategy Lab workflows for certified dataset generation,
  certification, analysis, and fixed-capacity allocation.

## Optimizer compatibility

Backtester V1 supports Optuna and Grid. New Backtester V2 Optimize and WFA
requests must explicitly use the Grid engine. The visible Fixed mode uses that
engine without parameter search. Optuna has not been removed: historical V2
Optuna studies remain readable, supported replay/manual-test paths remain
compatible, and V1 Optuna remains fully supported. Merlin never silently
converts old Optuna requests, Queue items, Presets, or studies to Grid.

## Quick start

Install the pinned dependencies:

```bash
pip install -r requirements.txt
```

Start the web server:

```bash
cd src/ui
python server.py
```

Open <http://127.0.0.1:5001>.

On this Windows checkout, repository development and tests use:

```text
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe
```

Linux/VPS hosts use their configured project environment.

The existing command-line backtest entry point has S01-specific defaults. Run
its help from `src/` rather than treating it as a generic V2 workflow:

```powershell
cd src
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe run_backtest.py --help
```

Its current default `--csv` path is repository-root-relative and does not
resolve from `src/`; for an actual run, pass an explicit path such as
`--csv ../data/raw/<file>`.

## UI workflow

V2 can use **Use Parquet** to select prepared packs, instruments and editable
timeframes. See [Parquet inputs](docs/PARQUET_INPUTS.md) for coverage, warmup,
Queue, pinned replay, relocation and explicit collection. CSV remains supported.

- **Start** — select market data and a strategy, configure parameters,
  preview/run optimization or WFA, and manage queued runs. V1 exposes
  Optuna/Grid; V2 is Grid-only.
- **Results** — browse stored studies and trials/windows, run supported tests,
  view equity/parameter details, export trades, and manage database paths.
- **Analytics** — compare WFA studies and sets, filter research views, and
  inspect aggregated and focused equity results.

## Project layout

```text
src/       application, engines, strategies, storage, and Flask UI
data/      market inputs and tracked regression/certification baselines
tests/     core/server, V2, JavaScript, and Strategy Lab tests
tools/     maintenance, benchmark, and Strategy Lab commands
docs/      architecture, procedures, evidence, and documentation index
```

New strategy development should normally target Backtester V2. A V2 import
requires a strategy package, execution profile, signals/dataprep, Grid
planning compatibility, parity/certification evidence, and focused tests; it
is not only a `config.json` plus `strategy.py` change.

## Documentation

- [Complete documentation index and ownership map](docs/README.md)
- [Current project architecture and strategy matrix](docs/PROJECT_OVERVIEW.md)
- [Cross-engine metrics](docs/METRICS.md)
- [V1 optimizer contracts](docs/OPTIMIZERS.md)
- [New V2 strategy import guide](docs/ADDING_NEW_STRATEGY_V2.md)
- [Legacy V1 strategy guide](docs/ADDING_NEW_STRATEGY.md)
- [V2 architecture](docs/engine_v2/ARCHITECTURE.md)
- [V2 certification evidence](docs/engine_v2/CERTIFICATION.md)
- [V2 performance history](docs/engine_v2/PERFORMANCE.md)
- [Testing guide](tests/README.md)
- [Tool catalog](tools/README.md)
- [Strategy Lab guide](tools/strategy_lab/README.md)

## Tests

Run from the repository root with the configured interpreter:

```bash
python tools/run_tests.py fast
python tools/run_tests.py full
```

If this checkout is nested inside another Git worktree, set `MERLIN_TEST_ROOT`
to an external location first. Use focused suites and prepared raw pytest as described in
[tests/README.md](tests/README.md).
