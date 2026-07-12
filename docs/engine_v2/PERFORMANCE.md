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
- `timings.data_prepare_seconds`
- `timings.fast_evaluation_seconds`
- `timings.slow_validation_seconds`
- `timings.slow_refinement_seconds`, when present
- `timings.total_seconds`
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
