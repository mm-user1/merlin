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
