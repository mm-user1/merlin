# Adding A Backtester V2 Strategy

Backtester V2 separates strategy logic from execution logic:

```text
strategy signals/dataprep -> generic V2 execution profile -> core V2 runner/Grid
```

The strategy owns indicators, causal signal arrays, parameter normalization,
and any aligned dataprep arrays required by the declared execution profile.
Core V2 owns fills, sizing, stops, targets, trails, guardrails, metrics, and
Grid execution. Grid V2 owns candidate enumeration and batch screening.

## Package Layout

Use a normal strategy package:

```text
src/strategies/<strategy_id>_b2/
  __init__.py
  config.json
  signals.py
  strategy.py
```

`strategy.py` should stay thin:

1. load cached config/profile/defaults;
2. normalize user, UI, and baseline aliases;
3. build `ExecutionData`;
4. call `core.engine_v2.runner.run_v2_strategy`;
5. return the standard Merlin `StrategyResult`.

## What Belongs In Strategy Code

Put this in the strategy package:

- indicator calculations and deterministic signal generation;
- mapping from config params to signal/dataprep params;
- aligned arrays needed by the profile, such as ATR, rolling swing highs/lows,
  and moving-average trail levels;
- config metadata, parameter roles, variant selector, and execution profile;
- optional chart overlays later, if they remain read-only presentation data.

## What Must Not Belong In Strategy Code

Do not add:

- a custom V2 execution engine;
- a custom V2 Grid backend;
- a strategy-specific Numba Grid loop;
- strategy-owned stop, target, trail, sizing, or fill logic outside supported
  V2 execution modes;
- lookahead/repainting allowances documented as acceptable behavior.

Generic mode names such as `target`, `trail`, `rr`, and `ma` are profile
vocabulary. Core V2/Grid V2 must not branch on strategy IDs or strategy-specific
variant names.

## Config Requirements

Set the engine:

```json
"engine": "v2"
```

Each parameter must keep the existing Merlin shape where applicable:

```json
{
  "type": "float",
  "label": "Stop X",
  "default": 2.0,
  "group": "Risk",
  "role": "execution",
  "optimize": { "enabled": true, "min": 1.0, "max": 3.0, "step": 0.5 }
}
```

Every optimized parameter must declare `role` as one of:

```text
signal
execution
runtime
```

Use `signal` for params that change signal/dataprep cache contents, `execution`
for params consumed by the profile, and `runtime` for run-window controls. Avoid
cross-role `depends_on`; V2 rejects it because it creates ambiguous cache keys.

Declare `execution` with:

- base modes such as `entryOrder`, `stop`, `sizing`, `maxDays`, `boundary`,
  `margin`, and `priceRounding`;
- `variants` for alternative generic topologies;
- `variantSelector` mapping a parameter value to a variant;
- parameters consumed by each mode through the existing V2 mode bindings;
- optional axes through normal `optimize.enabled` and `optimize.default_enabled`
  metadata.

For `select`/`options` Grid axes, a runtime config may restrict the enumerated
values with `{param}_options`. The value must be a non-empty subset of the
declared config options. Grid V2 preserves the strategy config order and rejects
unknown runtime options. Use this for apples-to-apples candidate count checks
instead of editing the strategy config.

## Signals.py Requirements

Signal code must be deterministic and causal:

- no reads from future bars;
- no centered rolling windows or negative shifts;
- no repainting after a bar is closed;
- no execution-param leakage into signal/dataprep cache unless that parameter is
  explicitly declared as part of the cache;
- boolean signal arrays must be 1D, aligned to the prepared dataset, and use
  explicit `False` for missing signal values;
- float dataprep arrays must be 1D, aligned, and use `NaN` only where the
  execution kernel expects inactive or unavailable levels.

Keep signal construction in Python/NumPy/Pandas. The compiled Grid evaluator
does not compile arbitrary strategy Python.

## Required Strategy Hooks

Expose these from `strategy.py`:

```python
def load_config() -> dict: ...
def load_profile() -> ExecutionProfile: ...
def normalized_params(params: Mapping[str, Any] | None = None) -> dict: ...
def build_v2_execution_data(df: pd.DataFrame, params: Mapping[str, Any]) -> ExecutionData: ...
```

Optional cache declarations:

```python
SIGNAL_CACHE_PARAM_NAMES = (...)
DATAPREP_CACHE_PARAM_NAMES = (...)
```

`build_v2_execution_data` must return fully aligned `ExecutionData` containing
timestamps, OHLC arrays, entry signals, and any profile-required arrays. It must
not place orders or simulate exits.

## Supported Phase 2.5 Modes

Phase 2.5 supports:

```text
entryOrder=market_next_open
stop=atr_swing
sizing=risk_per_trade
target=rr or none
trail=none or ma
trailActivation=none or rr
maxDays=true or false
boundary=strict_close
margin=off or report_only
priceRounding=none or tick_outward
```

Certified exit topologies:

```text
target=rr, trail=none, trailActivation=none
target=none, trail=ma, trailActivation=rr
```

If the next strategy fits these modes, add only the strategy package, config,
hooks, and tests. Do not add a new Grid backend.

## Grid V2 Runtime Settings

The normal Grid dispatcher passes V2 runs into the generic Grid V2 planner and
compiled evaluator when available. `grid_v2_prefer_compiled` defaults to `true`.
Set it to `false` only when a reference-tier run is intentionally required.

`grid_v2_max_cache_mb` overrides the signal/dataprep cache estimate limit. The
default is `512`; custom values must be finite positive numbers. In the normal
dispatcher, `worker_processes` caps Numba batch threads for compiled Grid V2
evaluation. Signal/dataprep cache memory is estimated once per in-process run,
so the dispatcher uses a cache worker multiplier of `1` even when multiple Numba
threads are requested.

When comparing candidate counts across tools or baselines, document the enabled
axes, enabled variants, `{param}_options` subsets, budget, and whether the UI
preview profile is `full_enumeration` or `full_enumeration_v2`. Both full
enumeration preview profiles should be treated as complete enumeration rows.

## Adding Unsupported Modes Later

For a new execution mode, update the system in this order:

1. extend V2 contracts/profile bindings and validation;
2. implement reference-kernel behavior first;
3. add direct execution and metrics tests;
4. extend the compiled evaluator with the same primitive packed inputs;
5. add compiled-vs-reference parity tests;
6. update this document and certification docs.

Do not add ad hoc exceptions in Grid V2 or strategy packages.

## Required Tests

Add focused tests for every new V2 strategy:

- config/profile parse and role validation;
- signal causality and prefix invariance;
- no repainting/window-start invariance;
- direct strategy discovery and `run()` smoke;
- direct V2 run smoke for representative params;
- Grid V2 count and identity smoke;
- one-candidate and multi-candidate Grid V2 parity against direct V2 runs;
- compiled-vs-reference Grid V2 subset parity when Numba is available;
- selected slow-enrichment smoke through the normal Grid workflow.
- select/options runtime subset count checks when `{param}_options` is part of
  the workflow;
- compiled Grid V2 thread-count determinism checks when a strategy relies on the
  compiled evaluator.

When using a V1 or external oracle, document any process-global test settings
such as `NUMBA_DISABLE_JIT`.

## Baseline And Certification

Use a TradingView or other external baseline when the strategy is meant to
match an external reference, when execution semantics are newly introduced, or
when the strategy will be used as a certification target. Keep raw exports and
screenshots unchanged, and use normalized UTC machine-readable files in
automated tests.

A Merlin-only baseline is acceptable for an internal strategy that uses already
certified execution modes and has no external parity claim. Store enough params,
data-window metadata, metrics, and trade signatures to make regressions
repeatable.

## Common Failure Modes

Check these first when V2 strategy work drifts:

- a signal parameter is missing from `SIGNAL_CACHE_PARAM_NAMES`;
- an execution parameter is incorrectly marked as `signal` or `runtime`;
- inactive variant parameters are changing semantic identity;
- signal arrays have object dtype, `NaN`, or length mismatch;
- rolling indicators use future bars or centered windows;
- `trade_start_idx` is ignored in warmup data;
- tick rounding is applied to risk sizing instead of only placed levels;
- selected Grid candidates are not slow-enriched through the reference runner;
- selected Grid candidates reuse fast guardrail summaries instead of the slow
  reference summary;
- cache estimates are multiplied by Numba thread count even though the
  signal/dataprep cache is shared in process;
- a test imported the V1 Numba oracle before setting process-global JIT state.
