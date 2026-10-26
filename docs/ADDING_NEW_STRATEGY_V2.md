# Importing a Backtester V2 Strategy

This is the primary procedure for new Merlin strategy work. Read the stable
[V2 architecture contract](engine_v2/ARCHITECTURE.md) and
[cross-engine metrics contract](METRICS.md) before implementation. Exact
existing evidence is catalogued in
[V2 certification](engine_v2/CERTIFICATION.md) and baseline READMEs.

Every new V2 strategy must ship a certified Grid planning/execution path. New
Optimize and WFA requests use explicit `optimization_mode="grid"`; do not add
an Optuna smoke path. Historical Optuna compatibility is a platform concern,
not part of a new strategy package.

### Optional risk-bracket entry admission

For a new strict ATR/swing RR bracket, top-level
`execution.entryAdmission="risk_bracket_v1"` opts into generic admission in both
reference and compiled Grid. Declare `maxEntryLeverage` as a fixed execution
float, default 8, label `Max Entry Leverage`, group `Risk`; do not declare
`contractSize`. Omit `stopMaxPct` to disable that filter, or declare a finite
execution value. No trailing, price rounding, variant-level admission or cap
optimization/ties/selectors are supported. Existing strategies are not migrated.

Continue providing normal signals/dataprep and parameter normalization. Pass the
explicit validated `admission_rules` argument when calling `run_v2_strategy`
directly; the application adapter supplies this context for registered profiles.
Never read a pack inside strategy signals, candidate execution or workers, and
never import Pattern Lab. Application runs require Parquet; CSV CLI/Strategy Lab
reject this mode. Verify both packing routes, actual spawned workers, saved replay
and selected diagnostics as described in [Architecture](engine_v2/ARCHITECTURE.md)
and [Parquet inputs](PARQUET_INPUTS.md). Synthetic generic-engine coverage alone
does not establish Pine parity or market performance for a new strategy.

## 1. Freeze the external execution contract

Before coding, record the exact Pine/external source and execution properties:

- input names, types, defaults, option order, and tested parameter set;
- chart timezone, timeframe, source rows/date range, and session behavior;
- signal timing, order timing, sizing, commission, slippage/funding, rounding,
  stops, targets, trails, forced exits, and same-bar rules;
- TradingView/export conventions that differ from Merlin metrics;
- expected trades, metrics, and any tolerated residuals.

Preserve raw sources, exports, screenshots, and hashes. Put normalized
machine-readable expectations in tracked baseline assets without changing the
raw evidence. If a required external input is local-only, disclose that
limitation; do not describe it as portable evidence.

## 2. Confirm generic execution coverage

Map the strategy to certified V2 modes before designing its package. Current
families are:

| Family | Supported shape |
| --- | --- |
| Position | next-open entry, ATR-swing stop, risk-per-trade sizing; RR target with no trail, or no target with MA/R-distance/Chandelier/Fixed-AF-SAR RR-activated trail; optional max-days, strict/none boundary, report-only/off margin, none/tick-outward rounding |
| Signal reversal | next-open entry, fixed-percent-equity sizing, signal exits, optional percentage Emergency SL, strict/none boundary, no price rounding |

The currently certified trail vocabulary is `none`, `ma`, `r_distance`,
`chandelier`, and `fixed_af_sar` under the valid compositions in
[V2 architecture](engine_v2/ARCHITECTURE.md).

If the strategy fits, reuse generic core unchanged. If it needs a genuinely
unsupported execution primitive, stop the import and separately certify that
primitive in this order: contract/profile validation, reference kernel,
direct tests, compiled parity, then architecture/certification updates. Never
hide a new primitive in a strategy-specific V2 Grid loop.

## 3. Create the package and thin adapter

```text
src/strategies/<strategy_id>_b2/
  __init__.py
  config.json
  signals.py
  strategy.py
```

`signals.py` owns causal indicators, entry/exit arrays, and aligned dataprep.
`strategy.py` should only load cached config/profile/defaults, normalize
aliases, build `ExecutionData`, call `run_v2_strategy`, and return the standard
`StrategyResult`.

Repository convention is a `BaseStrategy` subclass defined in `strategy.py`,
declaring `STRATEGY_ID`, `STRATEGY_NAME`, and `STRATEGY_VERSION`, with the
standard static `run(df, params, trade_start_idx) -> StrategyResult` adapter.
The registry mechanically discovers a class defined in that module when it has
`STRATEGY_ID` and `run`; it does not itself validate the full convention. A
package without such a discoverable class is skipped with a warning.

Expose:

```python
def load_config() -> dict: ...
def load_profile() -> ExecutionProfile: ...
def normalized_params(params: Mapping[str, Any] | None = None) -> dict: ...
def build_v2_execution_data(df, params: Mapping[str, Any]) -> ExecutionData: ...
```

An optional ordered batch hook may reuse indicator work within one call/chunk:

```python
def build_v2_execution_data_batch(df, params_list) -> list[ExecutionData]: ...
```

Never retain a module-global DataFrame cache. Normalize floating working
arrays at the preparation boundary; caller-created DataFrames are not
guaranteed to have floating OHLC dtypes.

## 4. Declare config roles and execution variants

Set top-level `"engine": "v2"`. Declare the execution profile in the
top-level `"execution"` object of `config.json`; the current design has no
separate profile file. Public parameter names remain camelCase.
Every optimized parameter declares exactly one role:

- `signal` — changes signals or signal-dependent dataprep;
- `execution` — consumed by a declared generic execution mode;
- `runtime` — only a reserved core runtime field.

Declare base execution modes, variants, their consumers, and an optional
`variantSelector`. A user-facing selector becomes Grid mode selection. Set
`userFacing=false` when a normal parameter chooses an internal variant, such
as optional Emergency SL; internal names must not appear as Grid modes.

Same-role boolean `depends_on` is allowed. Cross-role dependencies are invalid.
Small two-boolean `at_least_one_true` groups may define user-facing
`logical_modes`; define every reachable non-all-false combination once.

Reserved runtime fields are ordered `dateFilter`, `start`, `end`, and
`warmupBars`. If declared, they use `role=runtime`, are never optimized, and
must match the core types/ranges. They are not option subsets, selectors,
dependencies, candidate axes, or identity inputs. V2 requests transport user
Warmup separately from `fixed_params`.

For select/options axes, `{param}_options` may restrict a run to a non-empty
subset while preserving config option order. Do not edit config domains merely
to produce a comparison count.

## 5. Make signals and dataprep deterministic

Signal construction must be causal and prefix-invariant:

- no future reads, centered windows, negative shifts, or repainting;
- aligned one-dimensional boolean signals with missing values normalized to
  `False`;
- aligned float dataprep arrays, with NaN only where the core contract permits
  unavailable/inactive levels;
- execution parameters excluded from signal/dataprep unless explicitly part
  of that preparation identity;
- `trade_start_idx` honored after technical warmup.

Direction/regime gates belong in entry arrays. Signal-based close-all behavior
belongs in long/short exit arrays, not a new execution mode.

## 6. Declare and test cache identity

When present:

```python
SIGNAL_CACHE_PARAM_NAMES = (...)
DATAPREP_CACHE_PARAM_NAMES = (...)
```

The signal tuple must equal config parameters with `role=signal`. The dataprep
tuple adds every parameter that changes dataprep arrays. Neither contains
runtime fields. Add mechanical equality tests and behavioral backstops:
varying each declared signal parameter must change relevant arrays, and a Grid
axis must yield the expected distinct cache groups.

Request-gate optional indicator work. Chandelier ATR, for example, is prepared
only for Chandelier rows and its length participates in dataprep identity;
unrelated ATR rows are not interchangeable.

## 7. Prove Grid planning

Use the shared planner; do not create candidates in strategy code. Verify:

- enabled axes, inactive dependency children, variants/logical blocks, and
  option subsets produce the intended full count;
- semantic keys and candidate order are deterministic;
- full planning preserves the complete canonical order;
- sampled planning is deterministic for budget, seed, and allocation and is
  eligible for the declared block topology;
- effective planning policy—not backend profile name—is used in UI, Queue,
  WFA, storage, and reports;
- signal/dataprep cache and worker limits are realistic.

Record enabled axes/modes, option subsets, requested/effective policy, full
space, delivered budget, seed, allocation, and profile in any count claim.
Use sampled planning for intentionally bounded work; never add sampling inside
the strategy package or weaken cache estimates.

### Optional equality groups

For independent, always-active numeric signal pairs with matching types and
finite supported bounds, declare `optimization_rules.parameter_tie_groups`.
Each group supplies a unique `id`, user-facing `label`/`description`, and a list
of `[source, target]` pairs. Sources own the shared values and optimization
domains. Do not put a planning checkbox in strategy parameters or add tying to
normalizers/signals. The generic planner expands every candidate explicitly.

Verify disabled preservation, effective counts, fixed sources, target-only
rejection, full order, sampled determinism, lazy accessors and resolved cache
keys. Test request/Queue restoration and nested stored Preview. Keep restoration
snapshots out of execution identity. Unsupported dependencies or overlapping
pairs must fail explicitly. See the [architecture contract](engine_v2/ARCHITECTURE.md#optional-numeric-parameter-ties)
for the request and persistence fields. Preset tie persistence and Strategy Lab
tie certification are separate work.

## 8. Build parity and integration evidence

Use layered evidence:

1. signal values/state transitions against the source contract;
2. direct reference V2 execution for representative modes and both sides;
3. compiled-vs-reference one-candidate and multi-candidate parity;
4. selected Fast candidate Slow/reference enrichment;
5. external/Pine metric and trade parity where claimed;
6. UI Grid Preview and direct Grid request construction;
7. fixed/Adaptive WFA behavior applicable to the strategy;
8. storage/reload/manual replay compatibility;
9. Strategy Lab compatibility when the strategy is certified for that tool.

Ordinary oracle imports leave JIT enabled. Test helpers must not mutate
process-global JIT settings: restoring flags cannot repair modules already
imported as ordinary Python functions. If an oracle requires interpreted
execution, configure `NUMBA_DISABLE_JIT` in a separate process before imports
and keep compiled gates independent. Assert actual dispatcher/backend use in
compiled evidence, and include fresh-import and targeted cold-cache checks
when changing compiled paths (see the [test guide](../tests/README.md)). Do not modify core when
an external export merely rounds displayed prices, sizes, or PnL; document a
bounded residual in certification evidence.

Required focused tests include config/profile validation, discovery/run smoke,
causality, no-repainting/window-start invariance, cache declarations, Grid-only
HTTP/core rejection before side effects, full/sample count and identity,
reference/compiled parity, thread determinism, selected Slow enrichment,
runtime option subsets, WFA/storage transport, and external baseline assertions
appropriate to the strategy.

## 9. Update the correct authority

- Stable new core guarantee: [V2 architecture](engine_v2/ARCHITECTURE.md).
- Shared formula/availability change: [Metrics](METRICS.md).
- Exact values, hashes, tolerances, or parity conclusion:
  [Certification](engine_v2/CERTIFICATION.md) and baseline README.
- Timing measurement: [Performance](engine_v2/PERFORMANCE.md).
- New user-facing strategy: config-derived matrix in
  [Project overview](PROJECT_OVERVIEW.md).

Do not place phase chronology, benchmark numbers, local output status, or
exact certification results in this procedure.

## Common import failures

- a signal/dataprep parameter is absent from cache identity;
- an execution parameter is marked as signal/runtime or has no active consumer;
- inactive parameters alter semantic identity or candidate counts;
- an internal variant is accidentally exposed as a Grid mode;
- signals are misaligned, object-typed, non-causal, or contain missing truth
  values;
- a state-machine indicator lacks warmup/window-start convergence tests;
- execution logic leaks into `signals.py` or the adapter;
- candidate generation, sampling, packing, or stop/trail logic is duplicated
  in the strategy package;
- selected results trust Fast summaries instead of the one Slow/reference run;
- JIT-off oracle state contaminates compiled tests;
- a TradingView UI drawdown is compared to realized or bar-close MTM drawdown
  without naming the different convention;
- local-only evidence is presented as available in a fresh clone.
