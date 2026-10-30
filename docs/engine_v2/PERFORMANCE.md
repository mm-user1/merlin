# Backtester V2 Performance Baselines

This document records the benchmark protocol for Grid V2 performance work. It
is a measurement log and template, not a claim that Phase 2.6.0 improves
runtime.

For current engine ownership and runtime contracts, read the
[V2 architecture](ARCHITECTURE.md). Paths under `docs/_work/` in this log name
local historical benchmark artifacts. They may be absent from a fresh clone,
are not operational dependencies, and do not supersede the measurements and
protocol recorded here.

Benchmark numbers are machine-dependent. Always record the machine, CPU, Python
version, Numba version, thread environment, command, worker count, candidate
count, and the full Grid V2 timings dict before comparing runs.

## Quantity-only vectorized position preparation (T10, 2026-10-30)

Bounded local Windows measurement against accepted T09
`7f84ea9a9f1707aa01bbb005551ad4da1bd7ca88`, with quantity rules **On on both sides**.
The product change is confined to `core/grid_v2.py`; the compiled kernel and its
execution ABI are unchanged. Full compatible position tables now broadcast four
validated constants instead of calling typed accessors for every candidate.

Reproducible recipe (run sequentially using each side's source root):

```python
from core.grid_v2 import (GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan,
                         execute_grid_v2_candidates, _pack_table_config_arrays)
from tests._quantity_helpers import setup, frame, rules
_, module, config, profile, params = setup('s06_r_trend_v02_b2', 'false')
settings = GridV2Settings(
    enabled_variants=('bracket',),
    enabled_axes=('thresholdOS', 'stopX', 'stopRR', 'stopLP', 'stopMaxPct', 'stopMaxDays'),
    requested_budget=1440, compiled_workers=1, top_n=1,
    quantity_rules=rules(step='.02', minimum='.060001', notional='1'))
bars = frame(1500)
hooks = GridV2StrategyHooks.from_strategy(module)
plan = build_grid_v2_plan(config, settings, params)
indices = tuple(range(plan.deduped_candidate_count))
packed = _pack_table_config_arrays(plan, indices, 100)
result = execute_grid_v2_candidates(plan, bars, 100, hooks,
    compute_sharpe=False, compute_sharpe_daily=False, compute_sqn=False,
    compute_max_drawdown_mtm=False)
```

The tracked `frame(1500)` uses NumPy default_rng seed 7, hourly UTC bars starting
2023-01-01, synthetic lognormal closes and deterministic opening/high/low noise.
`setup` uses the declared defaults, normalized bracket parameters and dateFilter
False; Warmup/trade_start_idx is 100. Axes use unchanged declared domains:
thresholdOS 20/30/40, stopX 1/1.5/2/2.5/3, stopRR 1.5/2/2.5/3, stopLP 2/4,
stopMaxPct 2/4/6/8, stopMaxDays 2/4/6: **1,440 full candidates**, not sampled.
Rules are synthetic BYBIT base step .02, minimum .060001 (ceiling to four lots),
minimum notional 1, no admission cap. Remaining Grid settings are defaults:
full planning, table request, compiled preferred, 512 MiB cache estimate limit,
net_profit_pct primary, selected Slow enrichment On, no filters/ties. One actual
Numba thread was observed inside every compiled loop; top_n=1 executed real trades.

Each side used a separate initially empty external NUMBA_CACHE_DIR and bytecode/
output root. The accepted-base collection preceded product edits. Actual imported
grid_v2, compiled_kernel and fixture-helper paths were asserted under the explicit
source root, with raw SHA-256, labelled LF-normalized SHA-256 and Git-blob hashes.
The frozen helper uses the same inputs on both sides. There were three packing-only
calls on the prepared plan, then one JIT-inclusive public call and three warm public
calls. Planning is measured separately. Public calls include Fast and selected Slow
work; packing timings are diagnostic subsets and must not be added to public times.

| Measurement (seconds) | Accepted T09 | T10 final source |
| --- | ---: | ---: |
| Planning | 0.0445734 | 0.0439154 |
| Packing repetitions | 0.0276396, 0.0265587, 0.0268818 | 0.0005267, 0.0003668, 0.0003651 |
| Packing median | 0.0268818 | 0.0003668 |
| First public call, including JIT | 40.590706 | 42.205280 |
| Warm public repetitions | 0.0638962, 0.0627859, 0.0622214 | 0.0374299, 0.0365786, 0.0354185 |
| Warm public median | 0.0627859 | 0.0365786 |

Packing decreased in this case; the warm public median decreased about 42%.
The first call was slower and is disclosed, not used as a speedup claim. These are
small synthetic Grid evaluation measurements, not UI/WFA research duration or a
prediction for 200,000 candidates, other markets, S03 or S07. No CI speed threshold
or universal performance percentage follows from them.

Host: Windows 11 10.0.26200, AMD64 Family 26 Model 68 Stepping 0, Python 3.13.7,
NumPy 2.3.3, Numba 0.65.1. Backend was compiled_numba and metadata said table on
both sides; independent permanent dispatch spies establish the actual acceleration.
Lossless float.hex/NaN-aware serialization matched every Fast output, row/flag/count,
ordering, selected result, reference trade/balance/equity/metric and plan/semantic
identity exactly. Timing/source-attribution fields were not equality targets.

Evidence: `docs/_work/dev_04_merlin/T10/code-review-inputs/benchmark-before-02.json`,
`benchmark-after-03.json`, `benchmark-comparison.json`, and `benchmark-ledger.json`.
Frozen `benchmark_v2.py` SHA-256:
`0cf1cd327b0d08743dee328e6a5b0c4aede4d8e17e48c37879c44bcbd251df6e`.
These ignored artifacts supplement the complete tracked recipe above.
All attempts totalled **186.769 seconds** against the cumulative 300-second host
budget, including startup/setup/warmups/JIT. This includes a retained 44.795-second
first attempt that failed serialization, 42.604 seconds for the corrected baseline,
55.325 seconds for an initial after-source, and 44.044 seconds for final after-source
after a guard-order correction. The corrected helper is identical for the accepted
pair. Each process had a remaining-budget deadline; no allowance was reset and no
6,000-bar extension was run. Correctness and isolated mutation checks are separate.

## Risk-bracket admission preservation measurement (2026-10-26)

Exact parent `68a101c9ddf9d07d37bda35e8d880a2234975cb5` was captured before editing.
Windows, AMD64 Family 26 Model 68 Stepping 0, 24 available CPUs/Numba threads,
Python 3.13.7, NumPy 2.3.3, Numba 0.65.1. The deterministic old S06 B2 bracket
fixture has 12,000 synthetic 30m bars, 200 warmup bars, five actual candidates,
two compiled threads and three selected reference reruns. Each process makes
six complete Grid calls: first separately, then median of five warm calls.

| Capture | First call (s) | Warm median (s) | Process peak RSS (MiB) |
| --- | ---: | ---: | ---: |
| Parent, populated cache | 0.3538 | 0.1011 | 185.28 |
| Current, populated cache | 0.3783 | 0.1039 | 185.75 |
| Parent, fresh cache/JIT | 30.5835 | 0.1187 | 354.79 |
| Current, fresh cache/JIT | 42.7266 | 0.1486 | 373.45 |

Populated-cache warm median increased about 2.8% (2.85 ms); this small fixture
does not show material steady-state old-mode regression. Fresh-process JIT costs
increased and post-JIT warm timings were noisier/slower; these costs are not
hidden in the steady-state claim. No absolute speed guarantee is inferred.
Output stays 26 columns; three position-candidate admission configuration arrays
add 120 bytes for this five-candidate batch. Estimated stack bytes changed from
985,040 to 985,160. Pack reads finish before execution; candidate evaluation has
no per-bar Python admission callback or Grid attempt log.

All numerical/status/guardrail fields, canonical identities, semantic keys and
plan fingerprint match exactly. The original helper serialized its lazy `params`
mapping as a memory-address repr; comparison excludes only that unusable field.
The preserved canonical identity/semantic key still compares semantic parameters
exactly. Original parent files/helper were not rewritten. Retained artifacts live
under `docs/_work/dev_04_merlin/T03/code-review-inputs/`; they are review evidence,
not operational dependencies. No market data or historical goldens were regenerated.

## TZ17-1 Stateful-Trail Engine Measurement

Measured 2026-08-27 on Windows 10 build 19042 with Python 3.13.7, NumPy
2.3.3, Numba 0.65.1, 16 available Numba threads, and `n_workers=1`. Each case
used 4,096 deterministic synthetic bars and 512 candidates. A first call was
excluded for JIT warmup, followed by nine complete
`evaluate_compiled_batch(...)` measurements. The baseline was executed from an
archive of commit `452a2f58`; the after-run used the working tree and the exact
same S06 B2 profile, arrays, parameters, timing boundary, interpreter, and
worker policy.

| Existing mode | Before median | After median | Before/after output SHA-256 |
| --- | ---: | ---: | --- |
| `none` | 0.023373s | 0.026391s | `9443c8ce5d6847b7232fc9750b01e11a1899ab8483a1705af32bbe569942558a` |
| `ma` | 0.020990s | 0.023812s | `9fd8d9230f53ccec19b3fdd9a8dc65054b7e6400d2d062a15e0d112642c7105e` |

The exact hashes and `(512, 26)` outputs match before and after. The warm
medians are 12.9% (`none`) and 13.4% (`ma`) slower in this small synthetic
microbenchmark, so the greater-than-5% regression was investigated rather than
hidden. Configuration packing accounts for about 0.65 ms of the delta
(approximately 3.49 ms before and 4.14 ms after for 512 rows); the remainder is
the cost of carrying the additive mode code and trade-local primitive state in
the shared compiled loop. A helper-call isolation experiment was rejected
because it made both legacy paths slower. No opaque duplicated legacy kernel
was introduced solely to optimize this microbenchmark. Full S06 parity,
identity pins, and the complete test suite remain the acceptance authorities.

The expanded deterministic harness used the same bar/candidate/worker counts
and reported these warm medians: `r_distance` 0.018784s, `chandelier`
0.019078s, `fixed_af_sar` 0.018865s, and a mixed five-mode stacked batch
0.016658s. Raw nine-run ranges were respectively 0.018585-0.019059s,
0.018944-0.019332s, 0.018696-0.018952s, and 0.016546-0.016868s. These absolute
figures use the dedicated test profile and should not be compared directly to
the S06 before/after table.

Optional Chandelier transport is request-gated. With 4,096 bars, a mixed
two-dataprep-row stack retaining one Chandelier row added exactly 32,776 bytes:
32,768 bytes for the float64 ATR row and 8 bytes for the two-entry int32 row
mapping. A no-Chandelier row retained no optional full-size array. Grid cache
estimation counts unique active Chandelier dataprep keys, the float rows, and
mapping metadata; focused tests assert those estimates are not below the
stacked retained arrays and that non-Chandelier estimates are unchanged.

This is an engine-foundation measurement only. No new strategy, Strategy Lab
dataset, or production performance claim is introduced by TZ17-1.

## Deterministic Budgeted Planning

For `grid_v2_planning_policy=sampled` with requested `K < N`, planning builds
only K candidate-table rows. The balanced discrete LHS columns, semantic dedup,
mixed-radix top-up, ordering, semantic keys, and diagnostics are all O(K) in
memory; preview computes N and allocation by closed-form domain products and
does not build the population. A saturated request (`K >= N`) intentionally
uses the exact full builder, so its cost is the historical O(N) cost.

The `grid_v2_plan_identity_v3` fingerprint implementation consumes ordered
semantic keys as a one-pass stream and uses constant additional memory. Isolated
Windows `tracemalloc` measurements from the TZ60 implementation report were:

| Key stream | Time | Peak traced memory |
|---|---:|---:|
| Real S06 default, 48,480 keys | 0.100283s | 0.014645 MB |
| One-pass generator, 200,000 S06-length keys | 0.683079s | 0.015377 MB |

The earlier aggregate-JSON implementation measured 81.090 MB peak for the same
48,480-key S06 sequence. These are isolated fingerprint measurements after the
candidate table already exists, not end-to-end planning or execution results.
CI therefore verifies the one-pass structural contract rather than an absolute
machine-dependent memory threshold. A separate O(K) planning test covers
`N=300,000,000` with `K=50,000` without enumerating N.

Benchmark reports must record requested/effective policy, full N, requested K,
delivered K, seed, allocation method, per-block targets/delivery, planner and
sampler versions, plan fingerprint, workers, and cold/warm JIT state. Compare
planning, signal/data builds, stack, compiled kernel, ranking, and total wall
time separately. Sampled execution uses mapping config packing; full execution
retains the certified table-packing path when compatible.

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

The wrapper uses the required Merlin Python by default and forwards to
`tools/run_tests.py` in focused mode. Artifacts use external per-run directories
and reusable versioned caches; set `MERLIN_TEST_ROOT` outside all Git worktrees
when the default sibling root is inside an enclosing checkout. Successful runs
remove only their own run directory unless `-KeepTemp` is supplied. Failures and
interruptions retain it. See [test isolation](../../tests/README.md).

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
- evidence-driven exact optimization for Grid studies using three or more objectives;
- signal/dataprep stack splitting when data prep is the bottleneck;
- strategy-side dataprep optimization;
- WFA window parallelism.

## Exact Two-Objective Grid Pareto Ranking

The shared Grid V1/V2 ranking path now uses an exact `O(n log n)` numeric
algorithm when exactly two objectives are configured. It normalizes objective
directions out of place, sorts by the first then second objective, groups equal
first coordinates, and compares each group minimum with the best strictly
earlier group. This preserves strict dominance for duplicate rows, equal
coordinates, and signed zero. Pareto flags are assigned by row position rather
than trial number. Three or more Grid objectives remain exact through the
historical direct-Python quadratic fallback. Optuna is unchanged.

Direct V1 and V2 Grid summaries include `ranking_seconds` around full fast
ranking and diversity selection. V1 WFA does not currently project a
per-window ranking timing. Hard requirements are usually better expressed as
constraints: adding correlated objectives can enlarge the Pareto front and
increase selection cost even though the exact 2D membership stage is fast.

Windows after-run environment (2026-08-02): Windows 10 build 19042,
Intel Core i9-9900K (8 physical / 16 logical cores), Python 3.13.7, NumPy
2.3.3. Timings below are workstation evidence, not universal guarantees.

Bounded pre/post measurements used the same deterministic populations and
timed Pareto marking only:

| Shape | Rows | Front | Before | After median |
| --- | ---: | ---: | ---: | ---: |
| 2D adverse, all nondominated | 500 | 500 | 0.084997s | 0.000229s |
| 2D adverse, all nondominated | 1,000 | 1,000 | 0.341413s | 0.000389s |
| 2D adverse, all nondominated | 2,000 | 2,000 | 1.385368s | 0.000747s |
| 2D adverse, all nondominated | 5,000 | 5,000 | 8.799478s | 0.001934s |
| 2D independent random | 4,000 | 11 | 0.060093s | 0.001946s |
| 2D independent random | 8,000 | 9 | 0.131720s | 0.003901s |
| 2D independent random | 16,000 | 9 | 0.198358s | 0.008652s |
| 2D independent random | 32,000 | 10 | 0.636846s | 0.019742s |

Large after-runs used adverse all-nondominated values. Helper peak memory was
measured with `tracemalloc` after the input array existed. Normal end-to-end
time covers `rank_grid_results` after result construction; the separate traced
run supplies transient-memory evidence and includes tracing overhead.

| Rows | Helper | Helper peak | Front | End-to-end normal | End-to-end traced peak |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 48,480 | 0.002185s | 3.839 MB | 48,480 | 0.194801s | 14.060 MB |
| 436,320 | 0.021383s | 34.538 MB | 436,320 | 2.020593s | 126.699 MB |

On independent random large inputs the helper took 0.008800s for 48,480
rows (front 13) and 0.107399s for 436,320 rows (front 16). The old adverse
algorithm was deliberately not run at those sizes. A clearly labeled quadratic
extrapolation from the measured old 5,000-row case is about 827s at 48,480 and
67,008s (18.6h) at 436,320.

The unchanged 3D fallback medians before/after were 0.005248s/0.005672s at 400
rows, 0.024956s/0.026317s at 800, and 0.027975s/0.029233s at 1,200, with
identical front sizes. A future 3+ optimization requires observed production
usage and a separate exact-algorithm scope.

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

Runtime metadata build/parse and stored-runtime resolution are O(1) in the
four-field contract and run once per request/study operation, never per Grid
candidate or WFA row. Historical Grid Settings uses saved summaries first. If
derivation is required during Analytics aggregation, one request-owned memo is
keyed by strategy and deterministic derivation inputs; there is no global
cache, full V2 enumeration, or per-row diagnostic logging.

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

## TZ-07-2 conditional Sharpe/SQN measurement (2026-08-03)

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1; one warmup plus three
measured runs, four compiled workers, isolated task-local Numba cache. The
fixed output grew from 21 to 23 float64 columns (16 bytes per candidate) even
when optional metrics are disabled. Month IDs add one shared contiguous int32
value per bar only when Sharpe is requested.

| Family / plan | Candidates | Bars | Disabled median evaluation | Sharpe+SQN median | Delta | Disabled / enabled stack bytes |
|---|---:|---:|---:|---:|---:|---:|
| position/bracket S06 full | 48,480 | 6,857 | 5.7765s | 5.7630s | -0.23% | 55,849,628 / 55,877,056 |
| signal-reversal sampled | 4,096 | 18,521 | 11.2532s | 11.1815s | -0.64% | 304,942,568 / 305,016,652 |

S06's 23-column output was 8,920,320 bytes and its optional month IDs were
27,428 bytes. The sampled signal output was 753,664 bytes and month IDs were
74,084 bytes. Both stayed in one chunk under the 512 MB limit. These small
negative deltas are measurement noise; the evidence establishes no measurable
disabled-path regression and no total-evaluation penalty from enabling both
metrics on these warmed runs, not a universal speedup claim.

The empirical gate found usable SQN for 38,032/48,480 S06 four-month rows
(78.45%), 11,035/48,480 S06 two-month rows (22.76%), 3,943/4,096 signal
twelve-month rows (96.26%), and 2,683/4,096 signal three-month rows (65.50%).
Top-50 compiled/reference Sharpe and SQN differences were exactly zero in all
four cases. Canonical Top-50 Sharpe month counts were respectively 6, 4, 14,
and 5 (configured inclusive trading-calendar counts 5, 3, 13, and 4 because
warmup months participate); minimum monthly standard deviations were 0.496,
1.384, 11.012, and 1.232 percentage points, safely above the `1e-12` review
threshold. Non-finite selected objectives are removed from ranking, so these
percentages are operationally relevant for short WFA windows.

## TZ-10 evaluation-boundary correction (2026-08-09)

The TZ-07-2 rows above are retained as historical measurements of the former
warmup-inclusive implementation. Current Sharpe month counts exclude technical
warmup: the corresponding S06 four-month evaluation has five real calendar
observations, and the twelve-month S03 signal evaluation has thirteen. The
transition bar belongs to the new month and the final partial month remains.

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1, isolated task-local
Numba caches. The S06 comparison used six workers and one warmup plus one
measured run. The signal comparison used the same deterministic 4,096-row,
18,521-bar real-data shape and four workers; its before values are the retained
TZ-07-2 medians and its after values are one warmed measured run.

| Family / request | Candidates / bars | Before | After | Delta |
|---|---:|---:|---:|---:|
| S06 full, metrics disabled, wall | 48,480 / 6,857 | 10.044s | 10.024s | -0.20% |
| S06 full, Sharpe+SQN, wall | 48,480 / 6,857 | 10.243s | 10.173s | -0.68% |
| Signal sampled, metrics disabled, evaluation | 4,096 / 18,521 | 11.253s | 11.231s | -0.19% |
| Signal sampled, Sharpe+SQN, evaluation | 4,096 / 18,521 | 11.182s | 11.340s | +1.42% |

The S06 full population remains 48,480. Disabled and enabled runs retained
candidate IDs 18,436 and 18,981 respectively. The signal run produced 4,096
rows; all 4,096 enabled rows had usable Sharpe and SQN. No disabled-path
regression or material enabled-path regression was observed.

TZ-11 only retains existing reference-enrichment snapshots for selected Slow consumers; Fast code is unchanged, so the existing Fast baselines remain applicable.

## TZ-14-1 request-gated Daily Sharpe measurement (2026-08-10)

Windows 10, Python 3.13.7, committed 30-minute SUI fixture, real 1,000-bar
warmup, nine warmed alternating reference runs per state. S06 used the
certified bracket reference over 5,857 evaluation bars; S03 used the certified
signal-reversal reference over 17,521 bars.

| Family | Disabled median | Daily enabled median | Delta |
|---|---:|---:|---:|
| S06 position/bracket | 40.81 ms | 42.98 ms | +2.17 ms (+5.33%) |
| S03 signal-reversal | 73.78 ms | 79.63 ms | +5.84 ms (+7.92%) |

The enabled helper is vectorized over bars and stores at most one return per
observed UTC day. It does not resample, copy a DataFrame, or synthesize missing
dates. Disabled runner tests replace daily aggregation with a failing sentinel
and still pass, proving there is no day-ID or daily-return traversal when the
flag is false. Fast/Grid candidate paths remain unchanged and were not timed
because Phase 1 does not expose Daily Sharpe there.

## TZ-14-2 Stage 1 Fast Daily Sharpe measurement (2026-08-10)

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1, task-local Numba
cache. Runs used one warmup followed by three measured iterations on the same
host. Daily, Monthly, and combined requests were measured independently; all
candidate populations and selected trading results were preserved.

| Family / plan | Candidates / bars | Disabled median | Daily median | Combined Daily+Monthly median | Daily delta | Combined delta |
|---|---:|---:|---:|---:|---:|---:|
| V2 position/bracket S06 full, evaluation | 48,480 / 6,857 | 5.804s | 5.827s | 5.890s | +0.40% | +1.48% |
| V2 signal-reversal sampled, wall | 4,096 / 18,521 | 11.977s | 12.012s | 12.108s | +0.29% | +1.09% |

The full S06 disabled wall median was 10.503s versus the same-host pre-change
10.209s (+2.88%); its Fast median was 5.804s versus 5.694s (+1.93%). Candidate
18,436 remained selected with `net_profit_pct=45.74422762364992`,
`max_drawdown_pct=14.133826459897126`, and 55 trades. The sampled signal
disabled wall median was 11.977s versus 11.827s (+1.27%).

V1 warmed 4,096-candidate representative medians were: S03 v10 disabled
0.191s and Daily 0.263s; S03 v11 disabled 0.249s and Daily 0.286s; S06 disabled
0.146s and Daily 0.168s. A 50,000-candidate S03 v10 representative run measured
2.590s disabled and 3.257s Daily before the final constant-request dispatcher
specialization; the final 4,096-candidate disabled rerun was within +0.41% of
the pre-change median. Daily is therefore inexpensive in both V2 population
families but remains a measurable opt-in cost in V1, especially S03 v10; the
disabled path remains gated.

The fixed V2 result matrix grew from 23 to 26 float64 columns, adding 24 bytes
per candidate: 10,083,840 bytes for 48,480 rows (+1,163,520) and 851,968 bytes
for 4,096 rows (+98,304). Requested day IDs add exactly one shared int32 per
bar: 27,428 bytes for S06 and 74,084 bytes for the signal sample. Both measured
plans remained single-chunk. No per-candidate daily arrays or dictionaries are
allocated.

## TZ-14-2 Stage 2 public-objective measurement (2026-08-10)

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1, task-local Numba
caches. The production-sized S06 Grid V2 plan used 48,480 candidates, 6,857
bars, six workers, one warmup, and two measured runs. The comparison preserves
the accepted Stage 1 kernels and measures the Stage 2 request path only.

| Request | Median wall | Median total | Median Fast evaluation | Valid objective rows |
|---|---:|---:|---:|---:|
| Daily disabled, before Stage 2 | 11.012s | 10.947s | 6.048s | 48,480 |
| Daily disabled, after Stage 2 | 10.762s | 10.704s | 5.991s | 48,480 |
| Daily Sharpe objective | 11.094s | 11.030s | 6.314s | 46,056 |

The disabled after/before medians changed by -2.27% wall, -2.22% total, and
-0.93% Fast evaluation (measurement noise, with no regression). Enabling the
Daily objective added 0.332s wall (+3.09%) and 0.323s total (+3.02%) relative
to the same-host disabled-after median. Candidate 18,436 remained selected in
all runs, with `net_profit_pct=45.74422762364992`,
`max_drawdown_pct=14.133826459897126`, and 55 trades; its Daily objective was
`2.6399771876928075` when enabled.

All runs retained the 48,480-row population and one chunk. The fixed 26-column
output remained 10,083,840 bytes. Daily enabled only the shared contiguous
27,428-byte `int32` day-ID array; disabled runs retained zero day-ID bytes.

## TZ-16 realized-drawdown tail correction (2026-08-11)

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1, task-local fresh Numba
caches. Each case used two warmups; Slow used nine measured runs, the S03 and
V2 cases used seven, and the production-sized S06 case used five. All Fast
measurements used one worker. The before and after runs used the same scripts,
data, candidate populations, and host.

| Family / plan | Candidates / bars | Before median | After median | Delta |
|---|---:|---:|---:|---:|
| Slow `calculate_basic` | 350,000 balances | 2.2323s | 0.0179s | -99.20% |
| S03 v10 V1 | 256 / real SUI fixture | 0.0382s | 0.0398s | +4.20% |
| S06 V1 full | 48,480 / real SUI fixture | 4.2660s | 4.3872s | +2.84% |
| V2 position/bracket | 512 / 2,048 synthetic | 0.0110s | 0.0106s | -3.95% |
| V2 signal-reversal | 512 / 2,048 synthetic | 0.0077s | 0.0068s | -10.78% |

The Slow speedup comes from replacing the external episodic drawdown helper
with a vectorized full-path calculation. Compiled kernels keep scalar streaming
state and skip repeated flat balances. Every Fast after/before delta is within
the 5% regression gate or faster. The full S06 population remains 48,480, and
the pinned Fast drawdown/RoMaD outputs remained stable to floating-point ULPs.

## TZ-04 Phase 2 Stage 1 MTM sidecar (2026-08-13)

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1, task-local Numba
caches. The benchmark uses the real CRVUSDT Window 1 IS segment, 480 frozen
S06 bracket candidates, 3,928 bars, one compiled worker/thread, exact-signature
warmup, and medians of repeated calls after setup/JIT.

The pre-patch disabled median captured before editing was 0.016662s over nine
runs. Repeated patched measurements showed material host-frequency drift, so a
cross-build percentage is not accepted as stable evidence. Within the patched
build, 31-run medians with the normal Strategy Lab Daily Sharpe/SQN request
were 0.023712s disabled and 0.025295s MTM-enabled (+6.68%). Repeats ranged from
roughly +6.7% to +11.5%, above the 5% investigation threshold. The
implementation remains one O(n) streaming accumulator; meeting 5% would
require broader kernel specialization or fusion outside this narrow Stage 1
scope. The residual performance gate is reported for review rather than hidden
by changing the workload.

The requested sidecar is exactly 3,840 bytes (`480 * sizeof(float64)`);
disabled calls expose `None` and use only the internal zero-length placeholder.

The measured `+6.68%` best stable MTM-enabled overhead (with repeats around
`+6.7%` to `+11.5%`) is accepted as a request-only exception to the 5%
investigation threshold. It reflects the additional streaming drawdown work on
each evaluation bar. Normal Merlin callers leave the request disabled and are
unaffected. The follow-up certification gates do not change the formula,
operation order, sidecar ABI, Numba architecture, or compiled loop structure;
no speculative kernel optimization is warranted for this bounded research-only
cost.

## TZ-17.2 S06 v06-4-A2 import (2026-08-28)

Windows 10, Python 3.13.7, NumPy 2.3.3, Numba 0.65.1, one compiled worker,
task-local caches, real SUI 30-minute data, and excluded exact-signature JIT.
The unchanged old 48,480-candidate path measured 13.038s median wall and 8.548s
Fast versus the same-host pre-change 13.112s and 8.720s (-0.56% and -1.97%).
Candidate 18,436 retained net 45.74422762364992, drawdown 14.13382645989712,
and 55 trades.

| New/paired plan | Candidates | Median wall |
|---|---:|---:|
| Existing S06 Bracket | 480 | 0.087317s |
| New S06 Bracket | 480 | 0.087648s |
| R Trail | 3,600 | 0.357126s |
| Chandelier | 6,000 | 0.577487s |
| Fixed-AF SAR | 2,400 | 0.261838s |
| Combined | 12,480 | 1.175622s |

The paired Bracket delta is +0.38%. Signal preparation had one row throughout;
dataprep rows were 2 for each non-Chandelier single-mode plan, 4 for Chandelier
(two `stopLP` by two active ATR lengths), and 10 combined. Chandelier ATR length
therefore does not create rows for modes that do not consume it.


## TZ20-1 S03 v16-4-A Adaptive MA import (2026-09-09)

Measured on Windows 10 build 19042, Intel64 Family 6 Model 158 Stepping 13,
16 logical CPUs, Python 3.13.7, NumPy 2.3.3 and Numba 0.65.1. The process
reported 16 available Numba threads; execution explicitly requested one
compiled worker. No JIT flags were changed. SUI 30-minute preparation uses
August 1 through inclusive December 1 2025 UTC, Warmup 1000, 6,857 bars and
trade start index 1000. All four MAs participate; Emergency SL is fixed off.
Sharpe, Daily Sharpe and SQN are enabled; two selected rows receive Slow
reference enrichment. The signal/stack budget is 32 MiB.

Reproduce from the repository root with the project interpreter and a fresh
external output/cache directory:

```powershell
$py = 'C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe'
$out = Join-Path $env:LOCALAPPDATA 'Temp\merlin-tz20-1-performance-certified'
$env:NUMBA_CACHE_DIR = Join-Path $out 'numba'
$env:PYTHONPYCACHEPREFIX = Join-Path $out 'pycache'
& $py tools/benchmark_s03_adaptive_ma.py --output $out
```

`references.json` retains all reference trades/deltas and `performance.json`
retains settings, metadata, selected semantic identities and timings. Preview
counted 196,000 asymmetric rows in 0.000808 s and 2,800 symmetric rows in
0.000684 s without constructing either population.

| Production workload | Rows | Plan construction | Execution wall | Signal preparation | Compiled evaluation | Selected Slow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Full symmetric, first compiled call | 2,800 | 0.153140 s | 12.801122 s | 0.795680 s | 11.583210 s | 0.124359 s |
| Same full symmetric, warm | 2,800 | same plan | 1.392891 s | 0.776973 s | 0.242862 s | 0.122565 s |
| Sampled asymmetric, warm | 128 | 0.008772 s | 0.504410 s | 0.425803 s | 0.011230 s | 0.050480 s |

These are individual observed calls, not median claims. The other sampled
call took 0.505918 s. The initial directory was empty; reference
characterization had already compiled indicator/reference signatures before
the first population call compiled the Grid kernel. Warm timings exclude
that compilation. Timed selected Slow calls are included in execution wall
and the engine's evaluation measurement, so the columns are not additive.
Warm full/sample stack construction took 0.052408/0.002316 s and value-cache
key construction 0.127318/0.005351 s.

The sampled policy uses budget 128, seed 42 and default `auto_sqrt_space` allocation;
all four MAs are present (32 samples each). The single plain block targets
and delivers 128, with zero collisions, top-ups or shortfall. Planning versions
are `grid_v2_planning_v1`, `balanced_discrete_lhs_pcg64_v1`, and
`ordered_block_allocator_v1`; complete per-block metadata is retained. This
metadata was reconstructed read-only after timing and its fingerprint checked
against the measured plan. Full symmetric planning enumerates four independent
signal axes and expands both right-side values. The full plan fingerprint is
`a975735c9983d10528a23d075b9129e84445396ae966444461fec618c587f1a2`;
the sampled fingerprint is
`1ccf81e016887923e485923ee622765918b8e88fae50f6bf158602af7d8f7e11`.
Both execute actual `compiled_numba`, stacked signals, and mapping packing,
with zero legacy candidate population materialization. Selected full IDs are
499 and 359; sampled IDs are 98 and 21. IDs are scoped to their own plans.

The full run builds exactly 2,800 signal rows in three chunks, with peak 1,202
rows and 31.993492 MiB of estimated stack resources. Its unchunked signal
estimate is 73.240662 MiB. The sample uses one 128-row, 3.687424 MiB stack.
Native Windows peak process working set was 379,908,096 bytes (362.31 MiB),
covering reference characterization, JIT, both plans and executions together.
It is not a separate candidate-table peak or a promise that total process
memory stays within the 32 MiB signal budget.

Existing planning was compared against base HEAD
`a12bdf2bad99d1cc95f78136c9ba971fc3d57811`, loading its unchanged source into
isolated module namespaces in the same process. Configs, defaults, axes and
settings were identical; each batch used one warmup and 31 timed builds, with
two alternating before/after batches. The retained external
`compare_planning.py` and `planning_comparison.json` reproduce that comparison.

| Existing workload | Rows / enabled axes | Before medians | Final medians | Paired change |
| --- | --- | --- | --- | --- |
| S03 v11 Regime-ER | 60 / closeCountLong, tBandLongPct | 0.004327 / 0.004352 s | 0.004379 / 0.004395 s | +1.20% / +0.98% |
| S06 v02 B2 | 20 / stopLP, stopX | 0.002203 / 0.002207 s | 0.002260 / 0.002242 s | +2.58% / +1.56% |

An earlier S06 repeat showed +7.09%; inspection found unnecessary declaration
traversal for old configs with no ties. A direct empty-declaration return and
avoiding empty tie accessor work reduced the final repeated overhead above.
Both old fingerprints remain exact: S03
`599a0c0773acb592cb9aa959aba850e81bc85a584cea2da18e836c1d09a77009`, S06
`783a1ff2da8b6a110764ec73a6403fee025b00fb2347d1477075cf9480dd650b`.
No broad planner rewrite, disabled diagnostics or machine-dependent test
threshold was introduced.

The optional complete 196,000-row asymmetric construction was omitted to
keep this evidence bounded; it is absent from normal test discovery. No
196,000-candidate real-data backtest was performed. Audit Ubuntu planning
measurements/extrapolations are separate context and are not measurements of
this Windows implementation. Ubuntu verification remains the independent
reviewer's responsibility.
