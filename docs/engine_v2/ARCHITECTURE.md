# Backtester V2 Architecture

This document is the stable current contract for Backtester V2 execution,
profiles, Grid planning/evaluation, runtime metadata, WFA, and persistence.
Exact evidence belongs in [Certification](CERTIFICATION.md), measurements in
[Performance](PERFORMANCE.md), shared metric behavior in
[Metrics](../METRICS.md), and import steps in the
[V2 strategy guide](../ADDING_NEW_STRATEGY_V2.md).

## Ownership boundary

V2 separates strategy meaning from execution mechanics:

```text
strategy signals/dataprep -> validated execution profile -> generic runner/Grid
```

The strategy package owns config/profile metadata, parameter normalization,
causal signals, deterministic dataprep arrays, cache declarations, and a thin
adapter to `run_v2_strategy`. Generic core owns entries/fills, position sizing,
stops, targets, trails, boundaries, guardrails, metric transport, reference
execution, and compiled population evaluation. Grid V2 owns candidate
planning, identity, packing, batching, ranking transport, and resource checks.

Core code never branches on a strategy ID or strategy-specific variant name.
A strategy must not add its own V2 execution engine, candidate generator, or
Numba Grid loop when the required generic modes are already certified.

## Execution families

### Position family

Position-family profiles represent topology by leaving `topology` absent;
`topology="position"` is not supported. The position/bracket family requires
`entryOrder=market_next_open`, `stop=atr_swing`, and
`sizing=risk_per_trade`. It supports:

- `target=rr`, `trail=none`, `trailActivation=none`; or
- `target=none`, `trail=ma|r_distance|chandelier|fixed_af_sar`,
  `trailActivation=rr`.

Optional validated modes are `maxDays=true|false`,
`boundary=strict_close|none`, `margin=off|report_only`, and
`priceRounding=none|tick_outward`. The public trail vocabulary is therefore
`none`, `ma`, `r_distance`, `chandelier`, and `fixed_af_sar`, but only the
compositions above are valid.

### Application risk-bracket admission

Profiles may opt in at top-level `execution.entryAdmission="risk_bracket_v1"`.
This position-family mode requires next-open market entry, ATR/swing stop,
risk-per-trade sizing, RR target, no trail/activation, strict-close boundary and
no price rounding. Existing max-days and `margin=off|report_only` remain distinct.
Every variant must satisfy this composition; variant-level admission is invalid.
Absent admission preserves existing profiles and identity pins.

The profile declares fixed execution parameter `maxEntryLeverage`, default 8,
label `Max Entry Leverage`, group `Risk`. Positive finite fractional values are
valid; booleans/null/nonpositive/nonfinite values and Grid axes/ranges/subsets,
ties or selectors involving this cap are rejected. New runs resolve an absent cap
once and save it. `contractSize` declarations/overrides are forbidden: validated
instrument rules own the base-unit step. `stopMaxPct` is optional; an undeclared
override is invalid, and a declared finite filter acts before sizing.

Application entry requires Parquet. `PreparedInput.admission_rules` is an immutable
`AdmissionRules` carrier resolved under the preparation session's pack lock, then
passed explicitly through `OptimizationConfig`, Grid settings, selected reference
execution, WFA and spawned FT/Stress workers. Workers consume the prepared frame
and rules without reopening packs. `entry_admission` is the version-1 saved
envelope; it pins the cap, effective decimal rules and validated source snapshot.
See [Parquet inputs](../PARQUET_INPUTS.md) for conversion and replay semantics.
Direct core and both compiled packers require rules too. CSV CLI and Strategy Lab
generation reject this mode; V2 DSR remains unsupported.

Grid Preview resolves these rules from validated metadata under one local/provider
read session, without preparing or hashing candles. Pending and metadata-known
unreadable packs are refused; coverage, continuity and warmup checks remain at
actual launch. Published minimum notional accepts finite zero while preserving
null as an absent rule and rejecting unrepresentable nonzero values.

Eligible signal closes plan long first, inside entry bounds and outside warmup.
Unavailable inputs, invalid stop/risk, exhausted finite capital, invalid target
and stop width refuse before sizing. Nonfinite account state is an error. Sizing
retains IEEE operation order: `floor((balance*riskPct/100/riskDistance)/step)*step`.
Recover integer lots with rounding, never a second floor; unrepresentable sizing
is an error. At next open, use unchanged quantity and
`abs(open*quantity)/(balance-entryFee)`. Nonfinite arithmetic, minimum notional and
undefined denominator refuse before the cap; only a finite value strictly above
the cap increments skips. Equality fills. Refusals charge no fee and do not resize.
Immediate fill-bar exits still contribute executed leverage. Strict boundary and
max-days ordering remain unchanged (four days on the established 30m example
closes after 96.5 hours).

Compiled output remains 26 columns. New-mode cap refusals use column 15 and
executed leverage column 18, including margin-off. Execution-error FLAGS bit 256
is decoded before count conversion; failed rows cannot enter new-mode selection
and an all-error application population fails. Three constant-size configuration
arrays add 24 bytes per position candidate to memory estimates. Grid has no
per-bar policy callback or attempt trace. Selected reference runs use the V2-local
`AdmissionStrategyResult` subclass with sparse accepted/refused entry events;
shared `StrategyResult` and provider code remain unchanged.

Diagnostics, retained Adaptive aggregation and storage/UI scope are defined in
[Metrics](../METRICS.md#entry-admission-diagnostics). This application capability
does not replace or change the separate historical policy API below.
Trial diagnostic saves replace only the requested scope, clearing omitted/None
values while retaining other scopes. Equal parsed content causes no diagnostic
UPDATE, including legacy NULL-to-NULL; JSON formatting alone is not rewritten.

### Application quantity-only execution

`quantity_rules` is an opt-in immutable instrument-rule context, separate from
`admission_rules`. Compatible legacy position and signal-reversal profiles use it
only in portfolio execution; ordinary strategy adapters remain unchanged without
context. The application adapter, reference runner, both compiled families, Grid
packers/Slow fallback, WFA actual OOS, spawned FT/Stress, manual tests and exports
must apply it or reject an incompatible context. No worker reopens a pack.

Sizing retains each family's original IEEE order and one floor. The already-rounded
quantity recovers integer lots without another floor; minimum lots and actual-fill
minimum notional refuse entries only. Existing stop/risk/width/zero-size guardrail
precedence remains; finite exhausted balances are ordinary refusals. Nonfinite
account state and positive unrepresentable lot ratios fail explicitly (compiled
execution-error flag 256). Existing closes, S03 reversal sequencing, S06 trail
timing and strict boundaries remain independent of entry minimums.

Position packing reuses the existing cap/minimum arrays: cap zero plus positive
minimum lots activates quantity checks only when the explicit validated context
agrees. With neither context these arrays remain zero. Signal packing passes
per-plan minimum scalars; no candidate/per-bar arrays or memory-estimate change.
No output columns or cap diagnostics are added. Executed Contract Size is canonical
at its declared default, and rules supply the effective step. Contract Size axes,
ranges/options are rejected. Conditional rule semantics participate in plan/cache
identity only for the new policy, preserving old no-context and S07 identities.

Persistence uses the separate `quantity_execution` envelope and verified owned
child identity, never inference from a strategy name or current pack. See
[Ticker portfolios](../TICKER_PORTFOLIOS.md) for eligibility and replay boundaries.

### Opt-in reference admission and trace

`run_reference_kernel(..., policy=EntryPolicy(...), trace=KernelTrace())` adds an
opt-in reference-only admission/diagnostic boundary for strict nontrailing
brackets without a stop-width filter or price rounding (`price_rounding_mode`
must be `none`; ordinary no-policy tick rounding is unchanged). Integer-lot minimums, optional minimum
notional and an exact configurable entry-leverage cap run at their planning/fill
boundaries. Cap rejections increment margin rejection count/flag; undefined
leverage and minimum-rule refusals remain separate reasons. No resizing or fee
is applied on rejection. Nonpositive capital is an attempt outcome, not a halt.
The legacy executed-fill leverage diagnostic is unchanged; trace consumers own
separate pre-cap/executed populations. Trace records decisions, exit phases,
fees and ambiguous intrabar processing directly at execution branches and leaves
default TradeRecord exit reasons unchanged. Trace alone preserves numbers.
Exhaustive terminal attempt reasons are guaranteed only with the policy;
trace-only historical modes can retain `planned` for unclassified refusals.
These arguments are absent from profile/compiled/Grid APIs; no new compiled
mode or strategy certification is claimed. The no-policy path and certified
sizing, fill, trailing, boundary and guardrail contracts remain preservation
authorities. Core imports no Pattern Lab code.


### Signal-reversal family

The S03-like signal-reversal family requires:

```text
topology=signal_reversal
entryOrder=market_next_open
sizing=fixed_pct_equity
exitOnSignal=true
stop=none or emergency_pct
boundary=strict_close or none
priceRounding=none
```

Target, trail, trail activation, max-days, and margin are absent or inert.
Strategies provide long/short entry and optional exit arrays; flat/close-all
behavior is data-driven rather than a separate execution mode. Position size
is planned from realized balance and signal-bar close, rounded down to
`contractSize`, and filled at the next open. An Emergency SL is seeded from
the actual fill, is inactive on the fill bar, uses gap-aware stop fills from
the following bar, and ratchets only on favorable scheduled close updates.

## Profiles, parameters, and variants

Every optimized parameter has role `signal`, `execution`, or `runtime`.
Signal parameters affect signals/cache identity; execution parameters are
consumed by certified modes; runtime is reserved for the four core fields.
Cross-role dependencies are invalid. Same-role boolean `depends_on` may make a
child inactive; inactive children use fixed/default values and do not multiply
candidate count or enter semantic identity.

Profiles validate before execution or packing:

- every execution mode and composition is certified;
- variant-selector values resolve to declared variants;
- mode-consumed parameters are declared as execution parameters;
- dependencies are same-role boolean relationships;
- optimized execution parameters have a selected certified consumer.

A truly unbound optimized execution parameter is fatal; a fixed unbound one
is a warning and excluded from semantic identity. A fixed parameter consumed
only by an unselected but compatible mode is informational; optimizing it is
fatal. Family-incompatible consumers remain unbound rather than producing an
impossible recommendation. Structured diagnostics are authoritative; warning
strings are only a compatibility projection.

`variantSelector.userFacing=true` exposes variants as Grid modes. With
`userFacing=false`, fixed params select one internal variant and the UI does
not expose its names. Candidate rows keep both `variant_name` and
`grid_mode_name`: the former owns execution identity, while the latter owns
user-facing logical modes and diversity grouping for internal variants.

Two-boolean `at_least_one_true` groups may declare complete logical-mode
metadata. Their non-all-false combinations are fixed logical blocks, not core
strategy branches. Bool discriminators remain active semantic axes even when
fixed per block.

## Signal and dataprep identity

Opt-in [fixed entry filters](../ENTRY_FILTERS.md) sit between preparation and
execution. Top-level `entryFilterSupport` is outside modes. Capable hooks/runners
require explicit Off or prepared context; single/batch wrappers independently mask
long/short entries without mutating cached raw signals or exits. Context travels
beside frames/rules through Grid, WFA, workers and replay. Active effective settings
enter plan identity; Off and legacy candidate identities remain unchanged. Source
pins are separate. The linked guide owns native timing, source bounds and replay.
F06 v1 and v2 dispatch by descriptor version. V2 with Break > 1 retains state
from a pinned declared source origin; Break=1 remains bounded. This preparation
changes neither execution kernels nor the signal-mask ABI.

Signals are causal, aligned one-dimensional boolean arrays; absent values are
explicit `False`. Dataprep arrays are aligned floating arrays with NaN only
where a kernel contract permits unavailable/inactive values. The strategy may
provide a batch build hook, but its cache is scoped to that call or chunk and
never module-global.

`SIGNAL_CACHE_PARAM_NAMES` must exactly cover config parameters with
`role=signal`. `DATAPREP_CACHE_PARAM_NAMES` adds every parameter that changes
dataprep arrays. Runtime fields never enter either declaration. A parameter
inactive in the resolved variant does not enter the semantic/cache key.

The S07 HA adapter (`s07_ha_v01_b2`) owns causal HA/run arrays and deterministic
ATR14: first TR is high-low; the initial 14 terms are folded left to right in
float64, then `(previous * 13 + TR) / 14`. It emits only the kth directional
HA event from index 64 with finite ATR/extrema. HA starts at the first supplied
preparation bar; warmup events do not carry forward. Its call-scoped batch shares
HA/ATR and caches inclusive extrema by stopLP. Signal identity includes
entryRunLength/enableLong/enableShort; dataprep additionally includes stopLP.
Generic risk-bracket execution still owns ordinary-price fills, sizing, cap and
strict-close exits. Its five default axes form 840 candidates; Fixed retains one
scalar configuration. Entry filters remain independent, fixed and initially Off.

Compiled evaluation shares identical OHLC/timestamp arrays and internally
stacks signal/dataprep rows. The signal-reversal stack contains boolean entry
and optional exit rows and no floating dataprep stack. Position-family stacks
carry the profile-required arrays. Reference and compiled paths must preserve
candidate results within their certified tolerances; selected candidates are
always rerun through the public reference runner.

## Stateful trail contract

`r_distance`, `chandelier`, and `fixed_af_sar` share these guarantees:

1. The initial stop protects the entry-fill bar.
2. Activation is detected from the completed bar High/Low after the configured
   RR threshold is reached.
3. Activation moves protection to break-even at the actual fill price.
4. Method updates are derived from completed closes and become effective only
   on a later bar; a newly accepted stop cannot execute retroactively.
5. The first finite raw candidate must be strictly protective relative to the
   completed close (below it for long, above it for short) before rounding.
6. Later accepted candidates ratchet protection only; stops never loosen.
7. Per-trade state resets fully on exit and new entry, for both sides.

`r_distance` consumes `trailRR` and positive `trailDistanceR`.
Chandelier consumes `trailRR`, positive integer `chandelierATRLength`, and
positive `chandelierATRMult`. Its Pine-compatible ATR/RMA row is optional,
request-gated dataprep keyed by length and included in cache/memory mapping; an
armed trail holds fill-price break-even while ATR is unavailable.

Fixed-AF SAR consumes `trailRR` and `sarSpeed` in `(0, 1]`. Its recurrence is
trade-local: SAR starts at actual fill, EP at the activation-bar extreme, no
advance occurs on the activation bar, and subsequent updates apply the current
and previous bar range cap. These stateful rules do not alter the established
MA trail timing.

## Grid V2 planning and identity

The static backend profile is `full_enumeration_v2`; it identifies capability,
not whether an individual plan is full. `grid_v2_planning_policy` selects:

- `full` (also the default): exact complete population and historical order;
- `sampled` with `K < N`: deterministic allocation across ordered logical
  blocks and balanced discrete LHS using named PCG64 substreams, mixed-radix
  dedup/top-up, and canonical per-block ordering;
- sampled requests with `K >= N`: the unchanged full builder.

Automatic allocation covers every enabled non-empty block with at least one
candidate. Manual allocation may assign zero-percent weights or use a budget
below block count; capacity reflow can later use a zero-weight block after
positive blocks fill. Sampled blocks require a fixed active schema, no varied
dependency parent, no inactive-axis dedup, and provably disjoint semantic
identity. Unsupported layouts fail explicitly.

Sampled planning builds O(K) rows rather than the full N population. Candidate
IDs are local to the resulting plan. Semantic keys include strategy,
execution, and active parameter semantics but exclude policy, budget, seed,
allocation, workers, and runtime fields. Ordered semantic keys are streamed
into the plan fingerprint without materializing a duplicate key collection.
Current plans publish
`grid_v2_plan_identity_v3`, `grid_v2_semantic_identity_v2`, and
`v2_runtime_contract_v1`; mode changes invalidate reuse.

`{param}_options` can restrict select/options axes to a non-empty subset in
declared config order. It does not rewrite config metadata. Numeric request
ranges do not independently redefine V2 granularity.

### Optional numeric parameter ties

`optimization_rules.parameter_tie_groups` declares groups with `id`, `label`,
`description`, and independent `[source, target]` numeric pairs. The initial
capability accepts matching numeric signal types with the same finite declared
bounds. Runtime fields, selectors, dependencies, overlapping pairs, chains,
cycles, bool/select parameters, malformed declarations and unknown selections
fail validation. This is a bounded equality facility, not a constraint solver.

The request list `grid_v2_enabled_tie_groups` defaults to `[]` and becomes the
immutable `GridV2Settings.enabled_tie_groups` tuple. Enabling a group removes
the target's independent axis: the source owns its domain or validated fixed
value. Target editing values/ranges do not intersect that domain or alter the
effective plan. Redundant target enablement is normalized away; target-only
enablement is rejected. Generated source values must fit the target domain.
An explicitly empty `enabled_axes=()` in the direct planner yields one fixed
expanded candidate per selected variant/logical block. Unmarked HTTP/config
empty-axis search behavior still selects default axes.

Preview multiplies reduced dimensions without building a table. Full plans
expand those dimensions directly; sampled K < N requests use the existing
sampler on reduced dimensions and expand only the delivered rows. Saturation
uses the full builder. Both builders and scalar/materialized table accessors
resolve target values. Independent axis codes omit derived targets. The
sampled block-disjointness proof never treats a derived target seed as a fixed
discriminator. Rebasing carries the same resolution metadata and clears lazy
parameter caches through a new table instance.

Tied axis ordering follows the authoritative parameter order and retains axes
deliberately included by `include_inactive_axes_for_dedup`. Inactive axes still
contribute raw enumeration before semantic deduplication. Automatic eligibility
uses the same optimized, non-runtime, non-selector rule with or without ties;
explicitly requesting an ineligible axis remains an error.

Expanded candidate semantic and value-based signal/dataprep keys contain both
members and exclude tie selection. Internal within-plan grouping signatures
may encode independent axes and resolved values differently; cross-plan equality
is required of resolved value keys, not those internal codes. Enabled ties add
`grid_v2_parameter_ties_v1` to plan identity, including selected groups and pairs;
effective domains remain in the existing identity payload. Absent/empty ties
retain existing versions, candidate order, semantic keys and pinned fingerprints.

The config-driven checkbox defaults off, mirrors disabled target controls and
marks source labels as common L/S. Disabling restores both members' captured
asymmetric values and optimization settings. Queue's existing `uiSnapshot`
adds optional `parameterTies: {strategyId, groups}` restoration data. Request
`item.config.grid_v2_enabled_tie_groups` alone owns execution. Queue applies
base controls and ordinary snapshots first, validates restoration membership,
then resolves active groups from the request. Missing restoration uses supplied
independent values/defaults. Missing legacy selection means off. Reads do not
rewrite saved items; stale asynchronous configuration responses are ignored.

Studies save the canonical selection and `grid_config.enabled_tie_groups`.
Stored Preview can recover from the nested field alone; canonical `[]` wins
over stale nested facts. WFA reuses the reduced plan and transports expanded
IS/OOS parameters. Manual/Forward/OOS replay never reapplies study ties.
Strategy Lab accepts optional `generation.planning.enabled_tie_groups`, validates
it through the shared declaration contract, and transports an immutable tuple
to Grid. Missing and explicit empty lists resolve to the same no-tie plan;
Lab preserves their original presence in normalized input and content hashes.
Its required variant list declares the expected resolved variants for internal
selectors; Grid receives `enabled_variants=None` and resolves the fixed selector.
Lab checks exact agreement before fingerprints and retains execution-mode
validation. Public selector behavior is unchanged. Candidate projection retains
explicit target values and independent-axis geometry. Resume protects both
plan and run identity, even when fixed candidate values coincide. The bounded
S03 certification covers CRV windows 1..3; Presets integration remains outside
this work. See the [Lab guide](../../tools/strategy_lab/README.md).

## Fixed research mode

Top-level `parameterMode` accepts exactly `search` or `fixed`; absence keeps
existing search semantics. Explicit null, unknown values and wrong types fail.
The optional `OptimizationConfig` None default represents absence and is omitted
from saved dataclass configurations; an explicit saved dictionary null stays invalid.
Fixed requires V2 and `optimization_mode="grid"`, without a new optimizer enum.

Fixed collects every non-runtime scalar, resolves defaults/dependencies/ties and
validates strict Grid types plus declared options/min/max. The scalar variant
selector resolves exactly one variant; conflicting nonempty Grid Modes fail.
Internal selectors keep empty Grid Modes. The adapter passes `enabled_axes=()`
and checks the actual deduplicated plan has exactly one candidate, including Preview.
The marker travels through WFA base templates and per-window configs and stored
Preview. It does not enter semantic/plan identity; existing unmarked identities remain.

Active axes/ranges/subsets, constraints, profit/score filters, Slow refinement,
DSR/Forward/Stress and Adaptive WFA are refused before dataset preparation.
Inert objectives, budget/allocation and diversity values normalize before parsing:
one internal Net Profit objective, top count 1, diversity off, full planning and
ordinary inert budget defaults. Original search controls belong only to UI restoration.

The runner requires one successful row and surfaces a failed row's original error,
including admission execution flags. It bypasses ranking/diversity and retains the
authoritative reference result even for losses, zero trades or unavailable metrics.
Only finite objective metadata is populated. Existing runtime validation is unchanged:
Daily Sharpe comparison is request-gated; an empty diff map is not general parity proof.

Standalone OOS and regular day/month WFA execute the same effective non-runtime
parameters, filter and cap in every window. IS is descriptive. Existing bounds,
capital, position closing, OOS-only stitching, WFE and diagnostic deduplication remain.
Zero-trade windows remain visible. This adds no portfolio carry or Analytics study types.
Malformed saved mode metadata is unavailable per study; reads never repair it.

## Compiled execution and resources

Compiled Grid uses core-owned config packing and population batching. Full
table-compatible position plans use vectorized table packing, including explicit
quantity-only context: validate once and broadcast the rule-owned step, zero cap,
minimum lots and optional notional. Context validation precedes the empty return;
filled quantity rows must remain strict-close and unrounded. Existing public array
and active-trail validation still runs. Inactive stateful-trail seeds retain their
ordinary table representation; the kernel reads them only in their active modes.
Risk-bracket admission retains typed-row packing. Sampled, explicit mapping and
normalization-incompatible cases retain mapping packing; signal-reversal preparation
is unchanged. Each call allocates fresh arrays, including after window rebasing.
Strategies do not add packing hooks; execution and identity versions are unchanged.

The compiled result ABI remains 26 columns. Optional sidecars, including the
request-gated bar-close MTM drawdown sidecar, do not change that ABI.
Both position and generic signal-reversal execution support it. The reference
path scans existing equity; compiled signal execution keeps a peak and maximum
drawdown per candidate, observing close equity after fills and final-boundary
handling from `trade_start_idx`. MTM alone enables equity calculation without
enabling other metrics. The optional float64 sidecar costs eight bytes per
candidate, counted once in existing cache/chunk accounting. Requested empty
batches return shape `(0,)`; zero bars or empty evaluation return NaN. Any
non-finite observation is sticky NaN, and negative equity is not clamped.
Default-off results and per-call thread restoration retain their prior contract.

`grid_v2_max_cache_mb` is a finite positive signal/dataprep estimate limit,
default 512 MB. The estimate includes planned stacks, outputs, and shared
market arrays and is checked before data builds. Numba `worker_processes` caps
batch threads but does not multiply the one in-process cache estimate.

Signal-reversal execution can chunk its boolean signal population under the
configured limit. Position-family stacks fail fast when the estimate exceeds
the guardrail. The guardrail bounds the reported stack, not O(candidates)
planning/result objects or strategy-side transient allocations. Diagnostics
distinguish logical cache hits from physical rows retained after chunk release.

## Ranking and selected Slow authority

Fast objectives and their limits are defined in [Metrics](../METRICS.md).
Every selected Fast objective must be finite. Constraints are soft feasibility
and ranking rules, not candidate pruning. Grid Pareto membership is exact;
two-objective ranking uses the optimized algorithm and higher dimensions use
the established exact fallback.

Selected candidates are reference-rerun and slow-authoritative. Slow
enrichment reads the single reference run's typed `BasicMetrics` and
`AdvancedMetrics`; it does not reconstruct or default them. The authoritative
Fast `grid_rank`, `candidate_id`, and `semantic_key` survive enrichment and
storage. Optional Slow ranking uses separate `slow_refinement_rank`.

## Reserved runtime and request boundary

The ordered `v2_runtime_contract_v1` fields are `dateFilter`, `start`, `end`,
and `warmupBars`. They cannot be candidate axes, option subsets, selectors, or
dependency members and are excluded from candidate, semantic, plan, and cache
identity. `dateFilter` defaults to false when genuinely absent. User Warmup is
transported separately and production declarations constrain it to 100..5000;
core callers may use non-negative values.

Strict dates normalize to UTC. A date-only start is midnight; a date-only end
is the inclusive final microsecond of that day. Naive datetimes are UTC and
offset-bearing values convert to the same instant. Duplicate transports are
compared by canonical meaning.

New V2 Optimize and WFA requests must explicitly provide canonical top-level
`optimization_mode` that normalizes to `grid`. Missing, null, blank, Optuna, or
other values fail with `V2_GRID_ONLY_OPTIMIZER` before runtime-axis, dataset,
worker, optimizer, state, or storage work. Grid Preview supplies Grid itself.
HTTP validation and the public core Optuna guards independently enforce the
policy. V1 behavior is unchanged.

Strategy identity is resolved before runtime normalization. Missing, unknown,
or conflicting aliases fail without falling back to another strategy. A
known invalid V2 profile blocks config/run surfaces before execution.

## WFA

WFA builds one V2 plan for reuse across windows. Window rebasing changes only
`dateFilter`, `start`, and `end`; Warmup remains the request value. Runtime and
mode changes invalidate plan reuse. The HTTP boundary passes the raw worker
request to the shared configuration builder, whose normalized value is used by
Grid, replay, and other execution paths.

Window diagnostics retain the actual compiled worker count and top-level plan
fingerprint. Selected module trials retain Fast rank, candidate ID, and
semantic key. Delayed OOS transformation removes live technical-warmup
observations, retains only its scheduled flat prefix, produces increasing
unique timestamps, and rebases the metric boundary with an explicit anchor.
Non-delayed stitched output is unchanged.

IS/OOS metric callers resolve capital from the same final parameters used for
execution. Flat no-trade windows use that balance; delayed engine prefixes retain
the explicit advanced anchor, with execution capital as the missing-anchor fallback.
Reconstructed route prefixes use saved execution capital while preserving their
existing warmup/order behavior. Stitched curves remain normalized at 100.
Corrected non-100 FT/stress metrics can alter later selection; fixed-candidate raw
executions are unchanged. Historical rows are not repaired; see
[metric compatibility](../METRICS.md#execution-capital-and-historical-results).

## Persistence and historical compatibility

V2 Parquet integration uses the [application input contract](../PARQUET_INPUTS.md).
The route prepares one coherent consumed union and passes its frame separately
from JSON configuration through Grid, Slow, WFA and post-processing workers.
WFA keeps its existing window preparation origins; CSV still uses its historical
buffer round trip. Source pins are additive JSON facts, not strategy parameters or
Grid axes. Reconstruction validates the saved interval and fingerprint before
execution. This does not change V2 execution semantics or enable V2 DSR.
Successful Parquet Optimize OOS stages additionally persist exact timestamps,
prepared bar bounds, trading index and warmup count in the versioned
`config_json.parquet_oos_replay` record, atomically with OOS trial results.
OOS reconstruction verifies those facts against the parent pin and reproduces
the original preparation, without expanding date-only display labels. Older
Parquet records without this metadata require a new study for OOS reconstruction;
stored metrics and other supported views remain available. CSV replay is unchanged.

New V2 Grid/WFA studies persist one request-level `config_json.v2_runtime`
envelope with schema `v2_runtime_metadata_v1`. The envelope separately records
contract version `v2_runtime_contract_v1` and contains the complete ordered
runtime values and diagnostics. Per-window configs do not duplicate it. V1
writers omit the envelope.

Stored execution resolves current registry/profile authority first. It then
uses valid current runtime metadata, compatible legacy facts, and defaults in
the established presence-sensitive order. Candidate params never own runtime;
operation-specific dates are applied last and Warmup stays separate. Reads,
post-processing, and compatibility views never rewrite stored metadata.

Historical rows may omit additive runtime, worker, rank, and identity fields
and remain readable. Historical V2 Optuna studies and currently supported
manual/replay/export paths remain compatible; no record is silently converted
to Grid. Unknown/removed strategies remain viewable from stored facts but
cannot execute without current registry authority. Corrupt or unsupported
versioned runtime metadata blocks strict execution rather than downgrading.

## Carried portfolio coordination

The [ticker portfolio coordinator](../TICKER_PORTFOLIOS.md) reuses qualified
risk_bracket_v1/strict_close V2 profiles without strategy-name branches. It builds one
data-independent calendar, admits consumed slices, then executes sequential account
jobs with the existing Grid plan cache and one worker layer. WFA `run_window` is the
shared per-window extraction; ordinary WFA still constructs and runs its own calendar.
IS reference capital is constant; actual OOS overrides are finite positive internal
values independent of scalar editor bounds. Chosen IS params are not rewritten.
The shared stored-execution resolver applies the same internal-capital normalization
to Fixed children only after matching their saved portfolio/account identity to
database ownership. It requires an explicit finite positive saved capital and
retains all other scalar/profile/runtime validation. This shared path covers equity,
trade exports and manual-source execution; original user scalars and ordinary Fixed
studies still use declared editor bounds. No historical records are rewritten.

Connection-aware Grid/WFA saves participate in one final portfolio transaction;
ordinary callers still own their transactions. Additive parent/ownership tables and
an ordinary_studies view keep portfolio children out of ordinary enumeration while
retaining explicit child Results/replay. Nullable WFA OOS execution capital supports
exact replay/export. Pure saved-fact projection owns PF metrics, not another kernel.
No reference/Numba ABI, historical metrics, Pattern Lab or Strategy Lab contract changes.
