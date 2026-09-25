# Merlin Project Overview

Merlin is a config-driven cryptocurrency strategy backtesting and
optimization platform. This document owns current component responsibilities,
principal data flows, persistence/UI structure, and the complete current
strategy matrix. It does not duplicate route or source-file inventories.

## Component map

```text
src/core/             V1/V2 execution, optimization, Grid, WFA, metrics,
                      persistence, analytics, post-processing, and exports
src/core/engine_v2/   generic validated V2 reference and compiled execution
src/strategies/       discoverable strategy packages and config metadata
src/indicators/       shared technical indicators
src/ui/               Flask routes/services and the three-page frontend
data/                 market inputs and tracked baseline evidence
tools/strategy_lab/   local V2-only research pipeline
tools/pattern_lab/    local research-only market-data pack, collector,
                      descriptive event studies, matched comparisons, and
                      sequential bracket probes via the generic V2 reference
tests/                core/server, V2, JavaScript, Strategy Lab, and
                      Pattern Lab suites
```

For detailed V2 guarantees see [V2 architecture](engine_v2/ARCHITECTURE.md).
For formulas and surface availability see [Metrics](METRICS.md).
For V1 Optuna and Fast Grid contracts see [V1 optimizers](OPTIMIZERS.md).

## Core ownership

`backtest_engine.py` owns V1 data preparation, trade/result structures, and
single-strategy execution support. `optuna_engine.py` owns the legacy-supported
V1 Optuna implementation and common optimization result/config structures.
`grid_engine.py` owns shared Grid selection, validation, ranking, constraints,
Fast/Slow refinement, and dispatch. V1 Fast Grid generation remains in its
three strategy-owned backends; V2 planning and execution are generic in
`grid_v2.py`, `grid_v2_sampling.py`, `grid_pareto.py`, and `engine_v2/`.

`walkforward_engine.py` owns fixed day/calendar-month and Adaptive day-based
WFA orchestration, window execution, plan reuse, and stitched OOS output.
`metrics.py` owns canonical cross-engine metrics. `storage.py` owns SQLite
schemas and reads/writes; `analytics.py` aggregates WFA equity; post-processing
and export modules own their named operations.

V2 strategies own config/profile metadata, deterministic signals/dataprep,
cache identities, and thin adapters. Generic V2 core owns fills, sizing,
stops, targets, trails, guardrails, metrics transport, and compiled Grid
evaluation. New V2 strategies do not add strategy-specific execution/Grid
kernels for already-supported modes.

### Shared data-structure ownership

| Structure or operation | Owning module |
| --- | --- |
| `TradeRecord`, `StrategyResult` | `src/core/backtest_engine.py` |
| `WFConfig`, `WindowSplit`, `OOSStitchedResult`, `WFResult` | `src/core/walkforward_engine.py` |
| `GridSelectionConfig`, `GridAllocation` | `src/core/grid_engine.py` |
| V1 `GridParameterSpace`, `GridCandidate`, `FastGridData` | Each strategy's `fast_grid.py` backend |
| WFA equity aggregation | `src/core/analytics.py` |
| WFA display parameter identity | `src/core/param_identity.py` |

Optimizer duplicate identity and Grid plan/semantic identities are separate;
they live in `src/core/optuna_engine.py`, `src/core/grid_v2.py`, and the V1
strategy-owned `fast_grid.py` backends.

## Optimizer and execution flow

```text
config + market data
        |
        v
strategy discovery and request validation
        |
        +--> V1 Optuna --------> V1 strategy execution
        |
        +--> V1 Grid ----------> strategy Fast backend -> selected Slow rerun
        |
        `--> V2 Grid plan -----> generic compiled screen -> reference rerun
                                      |
                                      `--> direct study or WFA windows
                                                    |
                                                    v
                                             SQLite persistence
```

Backtester V1 supports Optuna and Grid. New V2 Optimize/WFA requests require
explicit Grid. Historical V2 Optuna studies remain readable and supported
replay/manual-test paths remain compatible; no stored or queued object is
silently converted.

CSV display metadata uses one lexical `core.csv_metadata.csv_basename` helper:
both slashes are separators, including a literal backslash in a POSIX filename.
Save/log labels normalize each supported source before trying the next fallback.
Historical full-path CSV names are normalized only in derived read/display and
export values. Stored CSV metadata, configuration payloads, actual filesystem
paths, and opaque Queue/Preset values are not migrated or rewritten by this rule.

Grid selection uses shared objective, constraint, Pareto, diversity, DSR, and
storage structures. V2 supports exact full plans and deterministic sampled
plans. Selected Fast candidates are always rerun through authoritative Slow
strategy/reference execution before final result use.

## Flask and UI ownership

The Flask application is split by responsibility:

- `src/ui/server.py` creates the app and registers routes.
- `server_services.py` contains shared helpers and validation without route
  decorators.
- `server_routes_run.py` owns optimize, WFA, backtest, status, and cancellation
  operations.
- `server_routes_data.py` owns pages, studies/tests/trades, strategies,
  Presets, databases, CSV browsing, Queue, and export-facing data routes.
- `server_routes_analytics.py` owns Analytics summary/equity/set routes.

These modules are the exact route authority; documentation intentionally does
not maintain a duplicate endpoint list.

The browser UI has three pages:

- **Start** dynamically renders strategy config, market/date/Warmup settings,
  V1 Optuna/Grid or V2 Grid controls, Grid Preview, WFA, and Queue management.
- **Results** browses stored studies and trials/windows, renders metrics/equity,
  and exposes supported tests and exports.
- **Analytics** filters and compares WFA studies, aggregates equity, and
  manages persisted study sets.

Frontend code is organized by page and concern under `src/ui/static/js/`.
Strategy forms come from `config.json`; strategy parameters are not duplicated
in JavaScript. Starting a new config load, or failing the current load, clears
strategy-generated form, strategy-info, and Grid Preview state while preserving
CSV selection, date/Warmup, database, budget, WFA, Queue, and Preset controls.
Obsolete asynchronous successes and failures are ignored. Config readiness
blocks direct launches while the selected config is loading or invalid;
persisted Queue execution remains independent of editable-form readiness.

## Persistence and compatibility

SQLite stores study metadata, optimization trials, WFA windows/module trials,
manual/forward/OOS results, study sets, and analytics caches. Multiple database
files are supported. Production databases and Queue state under `src/storage/`
are ignored operational data, not fixtures.

Grid and Optuna share interoperable trial/result storage while retaining their
mode metadata. New V2 studies add versioned runtime/identity diagnostics;
historical rows may omit them and remain readable. No read path migrates or
rewrites historical studies.

Queue is a generic persisted transport. Optimize/WFA launch boundaries own
runtime validation; Queue reads preserve invalid or legacy state for explicit
user handling. Presets configure editable UI state but do not override V2's
effective Grid-only policy.

V2 configs may expose numeric equality groups such as **Symmetric L/S**.
These reduce independent Grid axes while candidates retain explicit values.
Queue stores executable group selection separately from the optional UI
snapshot needed to restore asymmetric controls. Stored-study Preview restores
the selection; replay uses explicit candidate parameters. Presets do not
persist parameter ties.

Trade exports cover the supported IS, Forward Test, OOS, Manual, and WFA
surfaces. Lancelot partial-bundle export is a narrow legacy integration for
`s03_reversal_v10`, not a general V2 import requirement.

## WFA and analytics

Fixed WFA supports legacy/default day units and `period_unit="months"`. Month
mode uses authoritative month counts with `is_period_days=None` and
`oos_period_days=None`. Calendar Months requires Date Filter and a requested
UTC Start whose day is 1 through 28; that requested anchor day is preserved
across month boundaries. Bars are selected inside half-open logical calendar
boundaries, while persisted and displayed end timestamps retain Merlin's
inclusive bar representation. Requested End and available data first clamp the
effective range, and only complete OOS calendar periods are emitted, so an
incomplete tail is ignored.

Adaptive WFA remains day-only. WFE annualization uses `12 / months` in month
mode and `365 / days` in day mode. Queue uses compact month labels such as
`2m/1m` and may include that form in its generated WFA label. Results and
Analytics show separate unit-labelled `IS (months)` and `OOS (months)` values.
Windows execute their selected optimizer under the engine policy, preserve
per-window candidate identity where available, and produce stitched OOS
results. Analytics reads persisted WFA results, creates focused or portfolio
equity views, and caches group summaries in SQLite.

This section owns shared day/calendar WFA and WFE semantics. The
[V2 architecture WFA section](engine_v2/ARCHITECTURE.md#wfa) owns only V2
runtime rebasing, worker transport, delayed-OOS handling, and plan reuse.
Metric meanings are in [Metrics](METRICS.md).

## Strategy Lab integration

Strategy Lab is local, research-only tooling for certified V2 strategies. It
uses tracked run specs and read-only market inputs to generate deterministic,
resumable datasets in explicit ignored output directories. Implemented
capabilities include structural/real-pack certification, schema-v2 bar-close
MTM data, development analysis, and fixed-capacity allocation. It does not
change Merlin runtime behavior and has no CSV-level multiprocessing extension.

The complete usage, identity, schema, resume, analysis, allocation, and safety
contracts are in the [Strategy Lab guide](../tools/strategy_lab/README.md).

## Pattern Lab integration

M5 integrated research workflow is accepted at `d423016` within its documented
scope and the identity-portability limits of that generation; resume remains deferred.
Post-M5 prospective policy-2 identities and verified frozen extension relocation
are accepted at `9db9402` for their documented scope; historical artifacts remain
unchanged. The Windows-to-Linux replay verifies the shipped synthetic fixture,
not universal cross-platform numerical equality.
M4 remains accepted at `c26334e` for its documented scope.

Pattern Lab is separate local, research-only tooling for testing hypotheses
before a strategy exists. Merlin and Strategy Lab do not import it, and it does
not change Merlin runtime behavior or CSV handling. Implemented capabilities are
the Parquet market-data pack with its manifest, NPZ import and fixed-interval
reader; the closed-bar exchange collector with recoverable updates, cooperative
process exclusion and collector/instrument-rule provenance; the descriptive
event study, which turns a versioned study request and protocol into immutable
per-instrument evidence, a machine-readable summary and one offline HTML report,
with a bounded spawn pool for its per-instrument jobs; and **M3a matched
comparisons with calibrated inference**, which analyses a completed study
offline into its own sealed artifact. It uses the existing Merlin project
dependencies, including `pyarrow==22.0.0` and, for ordinary v2 monthly inference,
`scipy==1.16.3`, as pinned in root `requirements.txt`. These are not a standalone
two-package installation. It reaches the network only inside an explicit
collect, update or recover operation.

`workers` accepts any positive integer: `workers=1` runs each instrument job
directly and a larger count runs the same job under an explicit spawn pool
bounded by the selected instrument count, with coordinator-only pack reads and
publication and canonical evidence identical to the direct run. Event studies
themselves remain descriptive, with no p-value, confidence interval, matched
control or edge verdict.

M3a adds matched control populations, event-weighted point estimates and one
declared Holm family over a completed study's saved evidence. Request v2 uses
the shared monthly jackknife; explicit v1 retains the seven-day bootstrap. It
reads the study strictly, never modifies it, and takes no worker option. The v1
calibration met 11/15 checks and measured about 7.5-8.3% error on persistent
daily-signal fixtures. Monthly inference passed the declared synthetic screen
at G=12: eight nulls with 2,000 attempts each, raw error 4.15-5.70% and family
Holm error 0.9-2.4%, assessed with exact one-sided 95% upper bounds against 8%
and a 95% availability floor. These correlated checks do not certify nominal
5% control or arbitrary dependence across months. Other mathematically supported
month counts remain available with an explicit calibration-scope disclosure.
**M3a is accepted at `9fe7816` for the documented approximate development-screen scope.**
M3b adds explicit external-series/panel context and frozen-candidate validation,
accepted at `908efb4` for its documented research scope and supported extension
routes. It reuses monthly v2 without lowering support gates;
short validation can remain descriptive. M4 sequential ATR bracket accounts are
accepted at `c26334e` for their documented descriptive scope: the generic reference enforces frozen minimum-order
rules and a configurable entry-leverage cap, with checked attempts/trades/path
tables and offline descriptive reports. Bracket inference and frozen-candidate
validation are unsupported; fixed-only workflows remain available.
A nominal rejection is an approximate development-screen
result, not a validated edge or production acceptance.

The complete schema, roster, adapter, exclusion, recovery, study, evidence,
worker-pool, analysis, estimator, support-gate, artifact and command contracts
are in the [Pattern Lab guide](../tools/pattern_lab/README.md).

## Current strategies

This matrix is derived from `src/strategies/*/config.json` and is the only
complete current strategy matrix in general documentation.

| Strategy ID | Config name | Version | Engine | Concise purpose |
| --- | --- | --- | --- | --- |
| `s01_trailing_ma` | S01 Trailing MA | `v26` | V1 | MA crossover with trailing stops and ATR-based sizing |
| `s03_reversal_v10` | S03 Reversal | `v10` | V1 | Close-count/T-Bands reversal strategy with V1 Fast Grid |
| `s03_reversal_v11` | S03 Reversal | `v11` | V1 | v10 behavior plus optional Emergency SL and V1 Fast Grid |
| `s03_reversal_v11_regime_er_b2` | S03 Reversal v11 Regime-ER B2 | `v11-regime-er-b2` | V2 | Regime-ER S03 signals on generic signal-reversal execution |
| `s03_reversal_v16_4_a_adaptive_ma_b2` | S03 Reversal v16-4-A Adaptive MA B2 | `v16-4-a-b2` | V2 | Four adaptive MAs with combined Close Count/T Bands and optional symmetric Grid planning |
| `s04_stochrsi` | S04 StochRSI | `v02` | V1 | StochRSI swing entries with swing-based stops |
| `s06_r_trend_v02` | S06 R-Trend | `v02` | V1 | Williams %R trend/reversal entries with bracket or MA-trail risk management and V1 Fast Grid |
| `s06_r_trend_v02_b2` | S06 R-Trend B2 | `v02-b2` | V2 | S06 v02 signals on generic position execution |
| `s06_r_trend_v02_regime_trendlines_b2` | S06 R-Trend Regime-TL B2 | `v02-regime-tl-b2` | V2 | S06 plus optional trendline regime filtering |
| `s06_r_trend_v06_4_a2_b2` | S06 R-Trend v06-4-A2 B2 | `v06-4-a2-b2` | V2 | Multi-mode bracket, R-distance, Chandelier, and Fixed-AF SAR strategy |

## Procedures and evidence

- New strategy work normally follows the
  [V2 import guide](ADDING_NEW_STRATEGY_V2.md).
- Existing V1 strategy maintenance follows the
  [legacy V1 guide](ADDING_NEW_STRATEGY.md).
- V1 Optuna and Fast Grid contracts live in
  [V1 optimizers](OPTIMIZERS.md).
- Exact V2 parity/certification evidence is preserved in the
  [certification registry](engine_v2/CERTIFICATION.md) and tracked
  [baseline documents](README.md#baseline-evidence).
- Benchmark methodology and historical measurements live in
  [Performance](engine_v2/PERFORMANCE.md).
