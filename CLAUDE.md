# Merlin Repository Guidance

Read this file completely before repository work. It contains universal rules
and routes task-specific work to its tracked authority.

## Repository-wide rules

- Work to the approved specification and preserve unrelated user changes.
- Prefer clear, concise, efficient implementations. Avoid new abstractions,
  compatibility layers, or framework changes unless the task requires them.
- Keep the established light-theme UI unless a task explicitly changes it.
- Public strategy parameters use camelCase from Pine/config through Python,
  requests, storage, and exports. Do not add snake/camel conversion helpers.
- Serialize parameter dataclasses with `dataclasses.asdict()`; do not give
  those parameter dataclasses custom `to_dict()` methods.
- Strategy data-preparation boundaries must normalize arrays that require a
  floating dtype even when called with caller-created DataFrames.
- Update the authoritative tracked document whenever a current contract
  changes. Do not make ignored work notes the only authority.

## Environment and safety

On Windows, always use this interpreter for Python commands and tests:

```text
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe
```

On Linux/VPS, use the Python environment configured for that host and
repository. The Windows absolute path does not apply there.

Before editing, verify the requested baseline and inspect the worktree. Never
overwrite unrelated changes. Production databases, SQLite WAL/SHM files,
Queue state, Presets, market data, baselines, strategy configs, and Strategy
Lab inputs/outputs are protected unless the task explicitly places them in
scope. Tests and investigations must use task-owned temporary storage outside
protected paths. Remove only temporary paths created by the task.

Do not install dependencies, commit, amend, push, migrate data, regenerate
baselines, or mutate external systems unless explicitly authorized.

## High-impact product policies

- Merlin is a config-driven cryptocurrency strategy backtesting and
  optimization platform with a Flask SPA and SQLite persistence.
- Backtester V1 supports Optuna and Grid. New Backtester V2 Optimize and WFA
  requests are explicit Grid-only. Historical V2 Optuna studies remain
  readable and supported replay/manual-test paths remain compatible. Never
  silently convert an old request, Queue item, Preset, or study to Grid.
  Optuna remains a supported V1 optimizer.
- V2 strategies own deterministic signals, dataprep, config/profile metadata,
  and thin adapters. Generic V2 core owns fills, sizing, stops, targets,
  trails, guardrails, metrics transport, and compiled Grid execution. Do not
  add strategy-specific V2 execution or Grid kernels when generic certified
  modes already cover the strategy.
- Strategy Lab is local, research-only, Backtester-V2-only tooling. It reads
  market data and input datasets without modifying them and writes only to an
  explicit output directory. Canonical datasets are ignored local artifacts
  and may be absent from another clone. CSV-level multiprocessing is not
  implemented.

## Directory roles

- `src/` — application, engines, strategies, persistence, and Flask UI.
- `data/` — market inputs and tracked regression/certification baselines.
- `tests/` — core/server, V2, JavaScript, Strategy Lab, and Pattern Lab
  verification.
- `tools/` — maintenance, benchmark, Strategy Lab, and Pattern Lab commands.
- `docs/` — current architecture, procedures, evidence, and historical
  references. `docs/_work/` is ignored local planning history.

## Mandatory task routing

Read the destination for the surface being changed before editing it.

| Task or question | Authoritative tracked document |
| --- | --- |
| Full documentation index and ownership | [docs/README.md](docs/README.md) |
| Current components, UI, API ownership, storage, data flow, strategy matrix | [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md) |
| WFA windowing and WFE semantics | [docs/PROJECT_OVERVIEW.md#wfa-and-analytics](docs/PROJECT_OVERVIEW.md#wfa-and-analytics) |
| Cross-engine metrics and availability | [docs/METRICS.md](docs/METRICS.md) |
| Legacy Backtester V1 strategy work | [docs/ADDING_NEW_STRATEGY.md](docs/ADDING_NEW_STRATEGY.md) |
| V1 Optuna and V1 Fast Grid contracts | [docs/OPTIMIZERS.md](docs/OPTIMIZERS.md) |
| New Backtester V2 strategy import | [docs/ADDING_NEW_STRATEGY_V2.md](docs/ADDING_NEW_STRATEGY_V2.md) |
| V2 engine, Grid, runtime, WFA, and persistence contracts | [docs/engine_v2/ARCHITECTURE.md](docs/engine_v2/ARCHITECTURE.md) |
| Certified profiles, parity evidence, tolerances, and preservation | [docs/engine_v2/CERTIFICATION.md](docs/engine_v2/CERTIFICATION.md) |
| Strategy-specific preservation guards | Relevant [V1](docs/ADDING_NEW_STRATEGY.md) or [V2](docs/ADDING_NEW_STRATEGY_V2.md) guide, then certification/baseline evidence |
| Benchmark protocol and historical timing results | [docs/engine_v2/PERFORMANCE.md](docs/engine_v2/PERFORMANCE.md) |
| Tool catalog | [tools/README.md](tools/README.md) |
| Strategy Lab usage, schemas, safety, analysis, and allocation | [tools/strategy_lab/README.md](tools/strategy_lab/README.md) |
| Pattern Lab data pack, schema, import, reads, and identity | [tools/pattern_lab/README.md](tools/pattern_lab/README.md) |
| Test tiers, isolation, and command selection | [tests/README.md](tests/README.md) |
| Baseline provenance and interpretation | [data/baseline/README.md](data/baseline/README.md) and the [V2 baseline index](docs/README.md#baseline-evidence) |

An explicit approved task specification governs its change. For factual
questions, executable source/config/schema/tests outrank prose; certification
and baseline documents own exact evidence; current architecture and metrics
documents own stable contracts; procedures own workflows; performance and
the changelog are historical records.

## Development workflow

Inspect the smallest relevant source and test surface, implement the narrowest
change, and verify in proportion to risk. Run commands from the repository
root unless a documented tool says otherwise. Start with focused tests, then
run the relevant suite; use the full suite when required by the task.

Pytest storage, journal, CSV, Queue, export, bytecode, and Numba artifacts must
be isolated under task-owned temporary roots. Do not write generated files
under `tests/` or protected application directories. See
[tests/README.md](tests/README.md) for suite selection and commands.

For application startup and user-facing capabilities, see
[README.md](README.md). For exact routes or implementation details, inspect the
current source modules identified by the project overview rather than
maintaining duplicate endpoint or file inventories here.
