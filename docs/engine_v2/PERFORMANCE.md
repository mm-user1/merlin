# Backtester V2 Performance Baselines

This document records the benchmark protocol for Grid V2 performance work. It
is a measurement log and template, not a claim that Phase 2.6.0 improves
runtime.

Benchmark numbers are machine-dependent. Always record the machine, CPU, Python
version, Numba version, thread environment, command, worker count, candidate
count, and the full Grid V2 timings dict before comparing runs.

## Required Commands

Direct Grid V2 benchmark from the in-repository SUI baseline payload:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools\benchmark_grid_v2.py direct-grid --config tools\benchmark_configs\s06_b2_sui_baseline_grid.json --workers 1,6 --warmup-runs 1 --runs 2 --output-json docs\engine_v2\performance_direct_grid_v2_local.json
```

Corrected WFA comparison DB inspection:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools\benchmark_grid_v2.py inspect-wfa-db --db src\storage\2026-07-06_233217_backtester-v2-test.db --output-json docs\engine_v2\performance_wfa_db_inspection_local.json
```

Optional manual Windows WFA rerun protocol:

1. Use the same Windows interpreter shown above.
2. Use the same CSV, date window, `grid_enabled_modes`, `trailMAType_options`,
   warmup bars, candidate count, selected count, and `worker_processes=6` as the
   corrected DB studies.
3. Run the B2 CORE and B2 DOGE WFA studies twice with JIT enabled and a warm
   Numba cache.
4. Record the second run's wall time, stitched OOS metrics, per-window candidate
   counts, selected counts, and any `module_status.grid_v2` diagnostics.

Do not read external market-data paths stored in old DB rows unless explicitly
approved. The inspection helper reads stored metadata only.

On Windows, prefer the repo-local pytest wrapper for validation:

```powershell
.\tools\run_pytest.ps1 -q tests\v2
```

The wrapper uses the required Merlin Python by default, passes
`.pytest_tmp/run_<pid>` as pytest `--basetemp`, and removes the per-run
directory unless `-KeepTemp` is supplied.

`inspect-wfa-db` opens benchmark databases with SQLite
`mode=ro&immutable=1`. This is intended for frozen/checkpointed benchmark DB
snapshots and avoids `-shm`/`-wal` sidecars during inspection. Do not use
immutable reads for live DBs that may have uncheckpointed WAL frames.

## Grid V2 Domain Semantics

Grid V2 candidate domains are governed by the strategy `config.json` optimize
metadata, the runtime `enabled_params` selection, and select
`{param}_options` subsets. The optimize-style benchmark payload keeps numeric
`param_ranges` for compatibility with the canonical UI config builder, but
those numeric ranges do not independently redefine V2 grid granularity.

The current S06 B2 benchmark config mirrors the strategy config. Editing only a
numeric `param_ranges` entry in that payload should not be expected to change
the V2 candidate count; change the strategy optimize domain, `enabled_params`,
or a supported select `{param}_options` subset instead.

## Stable Fields To Record

For direct Grid V2 runs, record:

- `strategy_id`
- dataset or CSV path label
- `date_range.dateFilter`, `date_range.start`, `date_range.end`
- `warmup_bars`
- `worker_processes`
- `engine`
- `backend_kind`
- `compiled_batch_used`
- `compiled_workers`
- `candidate_count`
- `valid_candidate_count`
- `selected_candidate_count`
- `cache_estimate`
- `cache_stats`
- full `timings` dict
- `timings.candidate_generation_seconds`
- `timings.cache_key_build_seconds`, when present
- `timings.signal_build_seconds`, when present
- `timings.stack_build_seconds`, when present
- `timings.compiled_batch_seconds`, when present
- `timings.data_prepare_seconds`
- `timings.fast_evaluation_seconds`
- `timings.slow_validation_seconds`
- `timings.slow_refinement_seconds`, when present
- `timings.total_seconds`
- chunk diagnostics when present: `chunk_count`, `chunk_estimated_mb`,
  `max_chunk_candidates`, `max_chunk_estimated_mb`,
  `configured_limit_mb`, `estimated_signal_mb`,
  `full_run_estimated_signal_mb`, `signal_stack_rows_built`,
  `signal_stack_rows_peak`, `compiled_config_packing`, and
  `full_population_result_object_note`
- `candidates_per_second`
- `measured_wall_seconds`
- selected result count
- top selected candidate id, params, and core metrics

Keep `measured_wall_seconds`, `timings.total_seconds`, and
`candidates_per_second` separate. `candidates_per_second` is reported by Grid V2
and must not be recomputed with a different denominator.

For saved WFA studies, record:

- study id and study name
- strategy id
- optimizer mode from `config_json`
- `worker_processes`
- grid budget
- enabled modes
- select option subsets, such as `trailMAType_options`
- total windows
- per-window `grid_valid_candidate_count` summary
- per-window `grid_selected_candidate_count` summary
- `optimization_time_seconds`
- stitched OOS metrics
- `module_status.grid_v2` diagnostics status and stable timing keys, if present
- optional `module_status.grid_v2` signal bucket and chunk fields:
  `signal_build_seconds`, `stack_build_seconds`,
  `compiled_batch_seconds`, `cache_key_build_seconds`, `chunk_count`,
  `chunk_estimated_mb`, `max_chunk_candidates`,
  `max_chunk_estimated_mb`, `configured_limit_mb`,
  `estimated_signal_mb`, `full_run_estimated_signal_mb`,
  `signal_stack_rows_built`, `signal_stack_rows_peak`,
  `compiled_config_packing`, and `full_population_result_object_note`

## Current Corrected DB Baseline

Source DB:

```text
src/storage/2026-07-06_233217_backtester-v2-test.db
```

Known WFA studies:

| Group | Study id | Strategy | Time |
| --- | --- | --- | --- |
| V1 CORE | `d0ee7c87-dfb5-46f9-bc9a-d1bcab195635` | `s06_r_trend_v02` | 27s |
| V1 DOGE | `a43338af-e113-4fc3-a105-5dd01266fbc8` | `s06_r_trend_v02` | 27s |
| B2 CORE, earlier run | `67ddc409-4de5-4d5f-8071-967e1588e692` | `s06_r_trend_v02_b2` | 173s |
| B2 DOGE, earlier run | `6c594dc4-3431-4dc5-bb67-5bb8e473f72c` | `s06_r_trend_v02_b2` | 168s |
| B2 CORE, after Phase 2.5.1 | `c4662e90-4afc-451e-9964-0e1456efb20f` | `s06_r_trend_v02_b2` | 160s |
| B2 DOGE, after Phase 2.5.1 | `ffb37cc4-1bac-4075-b985-8a0b64f561b5` | `s06_r_trend_v02_b2` | 163s |

Current known settings and observations:

- candidate count: 48,480 per WFA IS window
- selected count: 10 per WFA IS window
- `worker_processes`: 6
- enabled modes: Bracket and Trail
- `trailMAType_options`: `["SMA", "HMA", "KAMA", "T3"]`
- B2 final/stiched metrics match the V1 reference studies for the accepted
  Phase 2.5.1 comparison

Per-window Grid V2 timing diagnostics are not captured in this corrected
comparison DB. These studies predate Phase 2.5.2 diagnostics persistence, so
`wfa_windows.module_status_json` has no `grid_v2` block and study-level
`grid_summary_json` is empty for these WFA-Grid studies. Mark timings as "not
captured" for these rows.

## Phase 2.6.0 Windows Baseline

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_2_6_0_direct_grid_baseline.json
docs/_work/backtester_V2/benchmarks/phase_2_6_0_fresh_wfa_db_inspection.json
src/storage/2026-07-11_184902_backtester-v2-phase-2-6-baseline.db
```

Environment:

- platform: Windows-10-10.0.19042-SP0
- logical CPUs: 16
- Python: 3.13.7 MSC 64-bit
- NumPy: 2.3.3
- Pandas: 2.3.2
- Numba: 0.65.1

Direct Grid V2 baseline command used the S06 B2 SUI payload, 48,480 candidates,
`workers=1,6`, one warmup run, and three measured runs.

| Workers | Mean Wall | Mean Total | Candidate Gen | Fast Eval | Slow Validation | Mean CPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 23.211s | 23.062s | 3.244s | 18.156s | 0.856s | 2,670.5 |
| 6 | 21.693s | 21.550s | 3.202s | 16.700s | 0.864s | 2,903.3 |

The top selected candidate was `18436` with
`net_profit_pct=45.74422762364992`, `max_drawdown_pct=14.133826459897126`,
and `total_trades=55`.

Fresh WFA baseline inspection used the same Windows environment and the DB
above. V1 `s06_r_trend_v02` studies completed in 26s / 26s with diagnostics
absent. B2 `s06_r_trend_v02_b2` studies completed in 157s / 158s with
diagnostics present, 12 windows each, 48,480 valid candidates per window, and
10 selected candidates per window.

## Phase 2.6.1 Windows After-Run

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_2_6_1_direct_grid_after.json
docs/_work/backtester_V2/benchmarks/phase_2_6_1_fresh_wfa_db_inspection.json
```

Implemented quick wins:

- Grid V2 computes signal/dataprep cache keys once per candidate and reuses
  those keys for the pre-allocation cache estimate and the execution grouping
  pass. The memory-limit check still runs before any `build_execution_data`
  call.
- The compiled evaluator uses vectorized timestamp-to-UTC-nanosecond
  conversion for clean `DatetimeIndex` / `datetime64` inputs and falls back to
  the previous scalar conversion for mixed or null-like inputs.

Same command, payload, candidate count, worker list, warmup count, measured
run count, and Windows workstation as the Phase 2.6.0 direct baseline.

| Workers | Mean Wall | Mean Total | Candidate Gen | Fast Eval | Slow Validation | Mean CPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19.774s | 19.632s | 3.234s | 14.985s | 0.619s | 3,235.3 |
| 6 | 17.861s | 17.719s | 3.314s | 13.193s | 0.624s | 3,674.8 |

Workers=6 mean wall improved from 21.693s to 17.861s, a 17.7% reduction. Mean
`fast_evaluation_seconds` improved from 16.700s to 13.193s, a 21.0% reduction,
and mean candidates/sec increased from 2,903.3 to 3,674.8. Candidate generation
time was effectively unchanged. The top selected candidate remained `18436`
with the same core metrics and params as the Phase 2.6.0 baseline.

The regenerated fresh WFA DB inspection reports diagnostics still absent for
the two V1 studies and present for the two B2 studies. B2 timing aggregates are
now visible in JSON; the two B2 studies report mean `total_seconds` of 12.700s
and 12.764s per window, mean `fast_evaluation_seconds` of 8.397s and 8.407s,
and mean candidates/sec of 5,774.4 and 5,767.5.

## Phase 2.6.2 Windows After-Run

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_2_6_2_direct_grid_after.json
docs/_work/backtester_V2/benchmarks/phase_2_6_2_fresh_wfa_db_inspection_before_new_wfa.json
```

Implemented structural changes:

- Grid V2 compiled runs now build one generic stacked execution payload per
  run and evaluate all successful candidates through a single compiled batch
  call. The grouped compiled evaluator remains available as a parity oracle.
- The stacked payload keeps OHLC and timestamps shared as 1D arrays, stacks
  signal/dataprep arrays as 2D rows, validates that every `ExecutionData` row
  has identical OHLC/timestamps, and uses per-candidate data-row indices.
- The compiled config packer now packs primitive arrays directly from generic
  V2 profile modes and candidate params, with cached mode validation and
  timestamp conversion. It does not add strategy-owned packing hooks.
- The cache estimate now accounts for physical stack signal rows, dataprep
  rows, output arrays, and shared OHLC/timestamps. For the SUI benchmark the
  estimate and actual stack+output allocation both report `52.52260971069336`
  MB with `162` stack rows.

Deferred work:

- The full typed/lazy candidate table was not enabled. `build_grid_v2_plan`
  still materializes legacy `GridV2Candidate` rows and full semantic identity.
- Full-population result materialization was kept unchanged so
  `config.optuna_all_results` remains a normal full-population list for WFA and
  route consumers.
- Strategy-side dataprep memoization and indicator optimization remain deferred
  to a later strategy-scoped phase.

Same command, payload, candidate count, worker list, warmup count, measured run
count, and Windows workstation as the Phase 2.6.1 direct after-run.

| Workers | Mean Wall | Mean Total | Candidate Gen | Fast Eval | Slow Validation | Mean CPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 17.122s | 16.989s | 3.243s | 12.344s | 0.624s | 3,927.8 |
| 6 | 14.322s | 14.192s | 3.309s | 9.449s | 0.623s | 5,131.5 |

Workers=6 mean wall improved from 17.861s to 14.322s, a 19.8% reduction.
This narrowly missed the nominal 20% wall-time target by about 0.03s on this
run, but did not regress the hard gate. Mean `fast_evaluation_seconds`
improved from 13.193s to 9.449s, a 28.4% reduction, and candidates/sec
increased from 3,674.8 to 5,131.5. Candidate generation time was effectively
unchanged because the typed/lazy candidate table was deferred. The top selected
candidate remained `18436` with `net_profit_pct=45.74422762364992`,
`max_drawdown_pct=14.133826459897126`, and `total_trades=55`.

The Phase 2.6.2 WFA DB inspection was run before any new WFA studies were
created. It preserves the Phase 2.6.1 comparison table: V1 studies remain
26s/26s with diagnostics absent, and the latest B2 studies remain 134s/134s
with diagnostics present. A fresh WFA rerun after stacked execution was not
performed during this coding pass.

## Phase 2.6.3 Windows After-Run

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_2_6_3_direct_grid_after.json
docs/_work/backtester_V2/benchmarks/phase_2_6_3_fresh_wfa_db_inspection_before_new_wfa.json
```

Implemented typed-table changes:

- `GridV2Plan` now owns a typed candidate table. `plan.candidates` and
  `plan.mapping_records` are lazy compatibility properties; normal execution,
  cache estimation, cache grouping, and slow enrichment bypass the legacy
  candidate tuple.
- Full-population semantic keys are still materialized because shared Grid
  ranking uses `semantic_key` as a deterministic tie-break. This is reported
  explicitly as `semantic_keys_materialized=48480` for the SUI benchmark.
- Full params are cached in the table for the full population. A fully lazy
  params adapter was measured slower because normal dispatch still needs
  full-population `OptimizationResult.params`.
- Canonical identities are not built for fast-screening rows. They are
  materialized only for selected slow-reference rows or explicit compatibility
  access.
- Cache grouping uses typed signatures and caches full cache keys by unique
  group. The SUI benchmark still reports `signal_combo_count=1` and
  `dataprep_combo_count=162`.
- A table-aware compiled config packer exists and is parity-tested against the
  mapping packer, but the default remains the mapping packer. The callback
  table packer was correct but slower on this benchmark; a vectorized table
  packer is deferred.

Same command, payload, candidate count, worker list, warmup count, measured run
count, and Windows workstation as the Phase 2.6.2 direct after-run.

| Workers | Mean Wall | Mean Total | Candidate Gen | Fast Eval | Slow Validation | Mean CPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 17.689s | 17.600s | 3.346s | 12.980s | 0.640s | 3,735.3 |
| 6 | 14.871s | 14.788s | 3.254s | 10.279s | 0.656s | 4,716.2 |

Workers=6 mean wall changed from 14.322s to 14.871s, a 3.8% regression, which
passes the 5% hard gate but does not meet the target improvement gate. Mean
`candidate_generation_seconds` improved slightly from 3.309s to 3.254s. Mean
`fast_evaluation_seconds` regressed from 9.449s to 10.279s because full params
and result rows are still full-population compatibility surfaces. The top
selected candidate remained `18436` with
`net_profit_pct=45.74422762364992`, `max_drawdown_pct=14.133826459897126`,
and `total_trades=55`.

The WFA inspection was run before any new WFA rerun. The existing Phase 2.6
baseline DB still shows V1 studies at 26s/26s with diagnostics absent and the
latest B2 studies at 117s/117s with diagnostics present. The stitched OOS
metrics remain unchanged for the stored comparable studies. A fresh WFA rerun
after Phase 2.6.3 was not performed during this coding pass.

## Phase 2.6.3.1 Windows After-Run

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_2_6_3_1_direct_grid_after.json
docs/_work/backtester_V2/benchmarks/phase_2_6_3_1_fresh_wfa_db_inspection_before_new_wfa.json
```

Implemented rescue changes:

- Stage A completed: cache grouping now derives compact group codes from the
  typed candidate table and builds full cache keys only for unique group
  representatives. The SUI benchmark remains at one signal group and `162`
  dataprep groups.
- Stage A also removed avoidable full-population params dict copies from
  compiled result rows and Grid V2 fast `OptimizationResult` conversion.
- Stage B completed: normal compiled dispatch uses a vectorized table packer
  by default when the strategy normalizer preserves kernel-visible fields and
  mode state. Mapping packing remains the compatibility fallback/oracle.
- `params_by_row` is no longer eagerly populated in the normal table build
  path. The SUI benchmark reported `params_materialized=173`, not `48,480`.
- Full-population semantic keys remain materialized because shared Grid ranking
  still uses `semantic_key` as a deterministic tie-break.
- Canonical identities remain lazy for fast-screening rows and are materialized
  for selected slow-reference rows or explicit access.
- `config.optuna_all_results` remains full-population. Fast rows now carry lazy
  params/canonical mappings with eager metrics and ranking annotations.

Same command, payload, candidate count, worker list, warmup count, measured run
count, and Windows workstation as the Phase 2.6.2 and Phase 2.6.3 direct
after-runs. Wall and total are primary; bucket deltas are diagnostic.

| Workers | Mean Wall | Mean Total | Candidate Gen | Fast Eval | Slow Validation | Mean CPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 15.500s | 15.440s | 3.385s | 10.609s | 0.645s | 4,569.6 |
| 6 | 12.822s | 12.759s | 3.367s | 7.952s | 0.667s | 6,101.1 |

Workers=6 mean wall improved from the Phase 2.6.2 baseline `14.322s` to
`12.822s`, and from the Phase 2.6.3 regressed baseline `14.871s` to `12.822s`.
This passes both the hard gate (`<=14.608s`) and the target gate (faster than
Phase 2.6.2). Mean total improved from `14.192s` in Phase 2.6.2 and `14.788s`
in Phase 2.6.3 to `12.759s`. Mean fast evaluation improved from `9.449s` in
Phase 2.6.2 and `10.279s` in Phase 2.6.3 to `7.952s`.

Candidate generation did not improve versus Phase 2.6.2 because semantic keys
remain full-population for ranking. The removed costs are in cache grouping,
compiled config packing, and params/result materialization, so wall/total are
the honest success metrics.

The top selected candidate remained `18436` with
`net_profit_pct=45.74422762364992`, `max_drawdown_pct=14.133826459897126`,
and `total_trades=55`.

The WFA DB inspection before any new WFA rerun preserved the stored comparison:
V1 studies remain 26s/26s with diagnostics absent; stored B2 rows show the
Phase 2.6.2 117s/117s baseline and the Phase 2.6.3 124s/124s regression with
unchanged stitched OOS metrics. A fresh WFA rerun after Phase 2.6.3.1 was not
performed during this coding pass. Based on the direct Grid V2 gate, proceed to
Phase 2.6.4; use a fresh Windows UI WFA rerun to close the WFA timing row if
needed.

## Phase 2.6.4 Windows After-Run

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_2_6_4_direct_grid_after.json
docs/_work/backtester_V2/benchmarks/phase_2_6_4_fresh_wfa_db_inspection_before_new_wfa.json
```

Implemented WFA-local plan reuse:

- `GridV2PlanReuseCache` reuses only the immutable candidate identity/table
  core across WFA windows: variant order/codes, axis value codes, semantic keys,
  candidate IDs/order, and domain/layout metadata.
- Each WFA hit rebases runtime seeds into a fresh table/plan view using the
  current window's `start`, `end`, and `dateFilter`. Cached plan/table objects
  are not mutated in place, and lazy params/candidate/canonical caches are
  fresh per rebased view.
- The reuse key includes `GRID_V2_ENGINE_VERSION`, the effective strategy
  config, all `GridV2Settings`, and fixed params with only `start`, `end`, and
  `dateFilter` removed. Axis/domain/variant layout is defensively validated on
  hit; mismatches rebuild.
- Signal/dataprep arrays and market data are not reused across windows. Normal
  cache keys and compiled/reference execution stay per-run.
- Direct Grid V2 is not expected to improve materially from WFA-local plan
  reuse because direct runs do not pass a reuse cache.

Same direct benchmark command, payload, candidate count, worker list, warmup
count, measured run count, and Windows workstation as Phase 2.6.3.1.

| Workers | Mean Wall | Mean Total | Candidate Gen | Plan Build | Runtime Rebase | Fast Eval | Fast Materialize | Ranking | Slow Validation | Mean CPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 14.922s | 14.864s | 3.259s | 3.259s | 0.000s | 10.172s | 0.442s | 0.329s | 0.640s | 4,766.0 |
| 6 | 12.364s | 12.301s | 3.251s | 3.251s | 0.000s | 7.718s | 0.277s | 0.396s | 0.639s | 6,281.6 |

Workers=6 mean wall changed from the Phase 2.6.3.1 baseline `12.822s` to
`12.364s`, a 3.6% improvement. This is within normal direct-run noise and is
not attributed to WFA plan reuse. Mean total changed from `12.759s` to
`12.301s`; mean fast evaluation changed from `7.952s` to `7.718s`.
Candidate generation/setup remained a plan-build bucket for direct runs:
`candidate_generation_seconds=3.251s` and `plan_build_seconds=3.251s` at
workers=6, with no runtime rebase.

The top selected candidate remained `18436` with
`net_profit_pct=45.74422762364992`, `max_drawdown_pct=14.133826459897126`,
`total_trades=55`, and the same selected core metrics as the Phase 2.6.3.1
run. The S06 B2 benchmark stayed at `48,480` candidates.

The WFA DB inspection was run before any new WFA studies were created. Existing
old rows inspect cleanly with the new optional timing/reuse fields absent. The
latest stored B2 studies in
`src/storage/2026-07-11_184902_backtester-v2-phase-2-6-baseline.db` have
diagnostics present, `48,480` valid candidates and `10` selected candidates per
window, stitched OOS metrics unchanged from their stored rows, and no
`plan_reuse` fields because they predate Phase 2.6.4. The last two stored B2
rows report optimization times `94s` and `93s`, mean total `7.593s` and
`7.466s`, and mean fast evaluation `3.338s` and `3.233s`.

A fresh full 48,480-candidate x 12-window WFA rerun was not performed during
this coding pass. Focused tests prove WFA plan reuse vs cache-disabled
equivalence for a constrained real S06 B2 Grid V2 window pair; use a fresh
Windows UI WFA rerun to close full wall-time confirmation if needed.

Remaining performance targets:

- semantic-key/ranking tie-break redesign to reduce candidate generation/setup;
- multi-objective Pareto optimization and ranking allocations;
- signal/dataprep stack splitting when data prep is the bottleneck;
- strategy-side dataprep optimization;
- WFA window parallelism.

## Signal-Reversal Rescue After-Run

Source artifacts:

```text
docs/_work/backtester_V2/benchmarks/phase_signal_reversal_rescue_wfa_db_inspection_after.json
docs/_work/backtester_V2/benchmarks/phase_signal_reversal_rescue_direct_grid_after.json
src/storage/2026-07-19_135447_s03-v11-regime-er-test.db
```

Implemented rescue behavior:

- `signal_reversal` Grid V2 runs use chunked execution when the monolithic
  signal-stack estimate exceeds `grid_v2_max_cache_mb`.
- The historical fail-fast memory guardrail still applies to non-signal/S06
  stacked paths; this rescue did not add chunking to those topologies.
- The optional strategy hook
  `build_v2_execution_data_batch(df, params_list) -> list[ExecutionData]`
  lets a strategy build one run or chunk with per-call/per-chunk caches.
  Strategies must not keep module-global DataFrame caches.
- Normal production performance assumes Numba is available. The optimized
  Regime-ER loop has a pure-Python fallback for tests/JIT-off runs, but that
  fallback is intentionally slower.

WFA DB evidence for S03 Regime-ER B2, same `45,405` candidates per IS window,
same 12 WFA windows, same selected count, same fixed params/ranges, and
unchanged stitched OOS metrics:

| Symbol | Runtime Before | Runtime After | Mean Fast Before | Mean Fast After | Mean Signal | Mean Stack | Mean Compiled | Mean Cache Key | Mean CPS After |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| COREUSDT 1h | 11,750s | 147s | 977.721s | 10.709s | 5.051s | 0.578s | 0.747s | 3.427s | 4,254.7 |
| DOGEUSDT 1h | 11,756s | 141s | 978.225s | 10.248s | 4.679s | 0.563s | 0.695s | 3.397s | 4,431.1 |

The pre-TZ43 rows have older stable timing fields but do not have the new
signal bucket or chunk fields; the artifact records those fields as absent.
The post-TZ43 DB rows fit under the default chunk limit in one chunk:
`chunk_count=1`, `max_chunk_candidates=45,405`,
`max_chunk_estimated_mb=429.991`, and
`full_run_estimated_signal_mb=422.623`.

Direct multi-chunk evidence used a deterministic 7,300-candidate prefix of the
747,200-candidate S03-like full plan. The monolithic estimate was
`517.636 MB` with `max_signal_cache_mb=512`, so the run split into
`chunk_count=2`. The largest chunk had `7,220` candidates and
`511.971 MB` estimated stack memory. Wall time was `13.208s`, Grid V2
evaluation time was `12.741s`, and selected slow enrichment completed for all
three selected rows. The top candidate was `6516` with
`net_profit_pct=629.0332414140`, `max_drawdown_pct=51.4077109943`, and
`total_trades=446`.

Memory caveats:

- The 512 MB guardrail bounds signal-stack chunk memory. Full-population result
  objects, candidate planning/materialization, ranking, and storage surfaces
  remain O(candidates) and are outside that guardrail.
- Strategy-side batch feature caches are outside the stack estimate and must be
  scoped to the current batch/chunk.
- Stacking can temporarily hold source `ExecutionData` rows and stacked arrays
  at the same time, so process peak memory can exceed the reported chunk
  estimate.
- `dataprep_hits` and `signal_hits` are logical cache-key reuse counts across
  the run. In chunked `signal_reversal` execution, physical arrays from an
  earlier chunk may already have been released.

## JSON Report Shape

The benchmark helper writes schema version 1 JSON:

```json
{
  "schema_version": 1,
  "command": "direct-grid",
  "environment": {},
  "runs": [
    {
      "worker_processes": 6,
      "run_index": 1,
      "warmup": false,
      "measured_wall_seconds": 0.0,
      "grid_summary": {},
      "timings": {},
      "top_result": {}
    }
  ]
}
```

The WFA inspection command uses the same top-level schema version and reports
`studies` plus optional `comparisons`.

## Future Run Template

| Field | Value |
| --- | --- |
| Date | |
| Machine | |
| CPU | |
| OS | |
| Python version | |
| NumPy version | |
| Pandas version | |
| Numba version | |
| `NUMBA_NUM_THREADS` | |
| `NUMBA_DISABLE_JIT` | |
| `NUMBA_THREADING_LAYER` | |
| `numba.get_num_threads()` | |
| Command | |
| Dataset / CSV label | |
| Strategy | |
| Date range | |
| Warmup bars | |
| Worker count | |
| Run number | |
| Warmup run? | |
| Candidate count | |
| Valid candidate count | |
| Selected candidate count | |
| Wall time | |
| Full timings dict | |
| Candidates per second | |
| Cache estimate | |
| Cache stats | |
| Top selected candidate id | |
| Top selected core metrics | |
| Notes | |

Do not edit historical baseline rows to make later performance changes look
better. Add a new measured row for each phase and keep the command and
environment metadata attached.
