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

`variantSelector.userFacing` defaults to `true` for backward compatibility.
Set it to `false` only when variants are internal execution variants controlled
by a normal strategy parameter, not user-selectable Grid modes. For example,
S03-like Emergency SL strategies map `useEmergencySL=false/true` to internal
`plain`/`emergency` variants, while the Start page exposes only the normal
`Use Emergency SL` and Emergency SL parameter controls. With
`userFacing=false`, Grid V2 resolves exactly one internal variant from fixed
params, publishes no selectable `grid_enabled_modes`, rejects stale non-empty
mode selections, and stores user-facing logical mode identity separately in
`grid_mode_name`.

For `select`/`options` Grid axes, a runtime config may restrict the enumerated
values with `{param}_options`. The value must be a non-empty subset of the
declared config options. Grid V2 preserves the strategy config order and rejects
unknown runtime options. Use this for apples-to-apples candidate count checks
instead of editing the strategy config.

Same-role boolean `depends_on` is part of the Grid V2 planning contract. If a
boolean parent is false, dependent child axes are inactive, do not multiply
candidate counts, and are omitted from semantic identity/cache keys. Inactive
children are passed to execution at their fixed/default value. Cross-role
dependencies remain invalid.

Use `optimization_rules.bool_groups` for small declarative logical mode groups.
The supported production shape is a two-parameter `at_least_one_true` group with
optional `logical_modes` metadata:

```json
{
  "params": ["useCloseCount", "useTBands"],
  "mode": "at_least_one_true",
  "logical_modes": {
    "cc_only": {"values": {"useCloseCount": true, "useTBands": false}, "label": "Close Count only"},
    "tbands_only": {"values": {"useCloseCount": false, "useTBands": true}, "label": "T Bands only"},
    "both": {"values": {"useCloseCount": true, "useTBands": true}, "label": "Both"}
  }
}
```

These logical mode keys are user-facing Grid modes for S03-like planning. They
are not execution variants and must not be encoded as core strategy branches.

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

The `signal_reversal` topology supports S03-like signal systems on both the
direct reference runner and the compiled Grid V2 stacked path:

```text
topology=signal_reversal
entryOrder=market_next_open
sizing=fixed_pct_equity
exitOnSignal=true
stop=none or emergency_pct
boundary=strict_close or none
priceRounding=none
```

`target`, `trail`, `trailActivation`, `maxDays`, and `margin` must be absent or
their inert values (`none`, `false`, `off`). Sizing is
`floor((realized_balance * positionPct / 100 / signal_bar_close) /
contractSize) * contractSize`, planned at bar close and filled at the next
open. Optional Emergency SL is a generic protective stop selected with
`stop=emergency_pct`: it seeds from the actual next-open fill, cannot trigger on
the fill bar, becomes eligible from `fill_index + 1`, fills long stops at
`min(open, stop)` and short stops at `max(open, stop)`, and ratchets only on
favorable close-based updates after `emergencySlUpdateBars`.

Emergency SL should remain a normal strategy parameter. If it also selects an
internal execution variant, mark the `variantSelector` as `userFacing=false` so
Grid/UI mode controls do not expose internal names such as `plain` or
`emergency`.

Flat or close-all behavior is data-driven. Strategies should populate
`Signals.long_exits` and `Signals.short_exits`; there is no separate `flatExit`
execution mode. Direction or regime gates belong inside `long_entries` and
`short_entries`, so opposite-signal reversal exits naturally follow the gated
entry conditions. The compiled/Grid tier for this topology uses mapping config
packing, not the vectorized table packer.

If the next strategy fits these modes, add only the strategy package, config,
hooks, and tests. Do not add a new Grid backend.

## Grid V2 Runtime Settings

The normal Grid dispatcher passes V2 runs into the generic Grid V2 planner and
compiled evaluator when available. `grid_v2_prefer_compiled` defaults to `true`.
Set it to `false` only when a reference-tier run is intentionally required.

When compiled execution is available, Grid V2 uses a core-owned stacked batch
path by default. Strategy code still only builds normal `ExecutionData` rows;
it does not provide packed candidate arrays or a strategy-specific Grid loop.
Core validates that the OHLC and timestamp arrays are identical across stacked
execution-data rows before sharing them as 1D market arrays. Signal and
dataprep arrays are stacked internally and addressed by per-candidate row
indices. For `topology=signal_reversal`, the compiled stack contains
`long_entries`, `short_entries`, optional `long_exits`, and optional
`short_exits` normalized to boolean rows, with no float dataprep rows.

Grid V2 candidate planning is also core-owned. Strategies do not build candidate
objects or own Grid execution loops. The planner uses a typed candidate table
internally and keeps the legacy `plan.candidates` tuple as a lazy debugging and
test compatibility surface. Strategy authors only need accurate config/profile
metadata and optional `SIGNAL_CACHE_PARAM_NAMES` / `DATAPREP_CACHE_PARAM_NAMES`
declarations. Compiled Grid V2 config packing is also core-owned and table
driven when compatible with the strategy normalizer; no new Phase 2.6.3 or
Phase 2.6.3.1 strategy hook is required.

Candidate rows have both `variant_name` and `grid_mode_name`. For user-facing
variant strategies such as S06 B2, both are normally the same values
(`bracket`/`trail`). For internal-variant strategies such as S03 Regime-ER B2,
`variant_name` stores the resolved execution variant (`plain`/`emergency`) and
`grid_mode_name` stores the user-facing logical mode
(`cc_only`/`tbands_only`/`both`). Diversity grouping for internal variants uses
`grid_mode_name`.

WFA Grid V2 plan reuse is also core-owned. Strategy authors do not add a
Phase 2.6.4 hook or cache object. Keep `start`, `end`, and `dateFilter`
declared and treated as runtime-only date-filter params so the WFA engine can
reuse candidate identity while rebasing those values per window.

`grid_v2_max_cache_mb` overrides the signal/dataprep cache estimate limit. The
default is `512`; custom values must be finite positive numbers. In the normal
dispatcher, `worker_processes` caps Numba batch threads for compiled Grid V2
evaluation. Signal/dataprep cache memory is estimated once per in-process run,
so the dispatcher uses a cache worker multiplier of `1` even when multiple Numba
threads are requested. The estimate includes the actual planned stacked
signal/dataprep rows, compiled output arrays, and shared OHLC/timestamp arrays;
the run fails before strategy data builds when the estimate exceeds the limit.

Correct planning can still produce a grid that is too large to run under the
default cache budget. The S03 Regime-ER B2 S03-like count with Regime off,
Emergency SL off, 10 MA types excluding `VWAP`, 20 MA lengths, Close Count
2..7, and T Bands 0.2..2.0 is:

```text
cc_only      = 7,200
tbands_only = 20,000
both        = 720,000
total       = 747,200
```

Every candidate is signal-role heavy, so even a corrected full-enumeration run
can exceed `grid_v2_max_cache_mb=512` on the SUI pilot dataset. Do not hide that
by raising defaults, weakening estimates, or adding sampling in a strategy
import patch.

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

Cache-declaration invariant tests are mandatory, not optional. Stale
`SIGNAL_CACHE_PARAM_NAMES`/`DATAPREP_CACHE_PARAM_NAMES` declarations are the
highest-risk silent failure mode for imports: Grid V2 would reuse cached arrays
across parameter values that should differ. The proven mechanical shape (see
`tests/v2/test_v2_s06_regime_tl_causality.py`):

- `set(SIGNAL_CACHE_PARAM_NAMES)` equals the config params with
  `role="signal"`;
- `DATAPREP_CACHE_PARAM_NAMES` equals the signal names plus every param that
  changes dataprep arrays;
- neither tuple contains runtime/window fields (`dateFilter`, `start`, `end`,
  `warmupBars`);
- a behavioral backstop: varying each declared signal param changes the signal
  arrays, and an axis over a signal param yields that many distinct
  `signal_combo_count` groups in `estimate_grid_v2_cache`.

## Pilot Import Lessons (S06 Regime-TL, B2-TZ 26)

Learned from the first real Pine v5 pilot import
(`s06_r_trend_v02_regime_trendlines_b2`); apply these to every future import:

- **Pin TradingView properties in the baseline package**:
  `process_orders_on_close=false`, `fill_orders_on_standard_ohlc=true`, zero
  slippage, percent commission, and the chart timezone. Record them in
  `dataset.json` so parity failures can be triaged against the pinned setup.
- **Pin the exact compiled Pine source.** The committed pilot Pine file
  referenced a variable whose declaration was commented out (stats-table code),
  so it cannot have been the byte-exact compiled source. Harmless here, but
  export the source *after* the reference run, from the same editor state.
- **Expect three residual classes vs TradingView exports** (all bounded,
  none code defects): exit prices rounded to the display grid (one tick),
  sizes rounded to one contract step, and per-trade PnL rounded to 2 decimals
  (accumulates into a ~0.03pp net-profit residual over ~45 trades). TradingView
  UI drawdown uses an equity/open-excursion convention that Merlin does not
  reproduce — pin Merlin-convention values and document the TV numbers.
  `round(profit_factor, 3)` does not reliably reproduce the TV display.
- **Fixed-per-study selector params**: a bool like `useRegime` can stay
  `"optimize": {"enabled": false}` and be varied per study through fixed params
  when the certification target is one explicit regime state. Grid V2 now
  models same-role boolean `depends_on` activation for planning, but selector
  axes should still be added only with explicit count, identity, and cache tests.
  Numeric companions may carry
  `"optimize": {"enabled": true, "default_enabled": false, ...}` so they are
  opt-in axes only.
- **State-machine indicators need explicit warmup convergence checks.** A
  regime/trendline state machine has unbounded memory in principle, unlike
  bounded-lookback indicators. Lock the warmup recipe with a window-start
  invariance test at a larger warmup; raise the warmup in the baseline recipe
  if it diverges instead of touching core.
- **JIT test process isolation**: never mix `NUMBA_DISABLE_JIT=1` oracle tests
  with compiled Grid V2 assertions in one pytest process; run compiled parity
  in a fresh JIT-on process.

## Pilot Import Lessons (S03 Regime-ER, B2-TZ 36)

Learned from importing `s03_reversal_v11_regime_er_b2`, the first production
strategy on the `signal_reversal` topology:

- **Map Pine `close_all` to exit arrays, not a new mode.** Regime-flat exits are
  expressed as both `Signals.long_exits` and `Signals.short_exits`. The generic
  topology then closes the active position at the next open and leaves
  non-Emergency-SL exit reasons as `None`.
- **Keep `useRegime` fixed per study.** The Regime-ER numeric params are signal
  params, but `useRegime` itself is not a default Grid axis because disabled
  regime would make those numeric params inert. Use fixed study params and opt
  into numeric regime axes explicitly.
- **Do not infer config defaults from one baseline.** The Regime-ER reference
  uses `maOffset3=0.0`, `regimeErLength=30`, `regimeErThresh=0.40`, and
  `emergencySlPct=10.0`; the Pine defaults remain `0.2`, `20`, `0.30`, and
  `20.0` respectively. Baseline tests inject the reference params.
- **Date-expiry `close_all` can require post-end bars.** The S03 Regime-ER
  TradingView reference emits the final close after leaving the date range and
  fills it at `2026-02-01T01:00:00Z`. The production B2 adapter follows the
  established truncation-at-`end` pattern and closes at the strict boundary
  `2026-02-01T00:00:00Z`; keep this as a documented residual unless core date
  boundary semantics are deliberately reopened.
- **Emergency SL export prices can be display-rounded.** The reference B
  Emergency SL event matches in time and behavior; the TradingView CSV rounds
  the computed fill price to the 4-decimal display grid. Use a one-tick
  exit-price tolerance for that exported field, while keeping entry prices and
  timestamps exact.
- **S03-like Grid modes are logical modes, not Emergency variants.**
  `plain`/`emergency` are internal execution variants selected by
  `useEmergencySL`. User-facing Grid planning uses `cc_only`, `tbands_only`,
  and `both`, with same-role boolean `depends_on` collapse so inactive Close
  Count, T Bands, Regime-ER, and Emergency SL child axes do not multiply the
  parameter space.

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
