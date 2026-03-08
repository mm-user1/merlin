# Phase 2.2.2 Report: Fixed Duplicated NSGA Trials

Date: 2026-03-08  
Project: Merlin (`./+Merlin/+Merlin-GH/`)

## 1. Summary

This update fixed the duplicated-trial problem in Merlin's multiprocess `NSGA2` / `NSGA3` optimization path without changing:

- `TPE` multiprocess behavior
- single-process optimization behavior
- coverage generation logic
- WFA transport logic
- current RAM-only runtime design for active optimization work

The fix replaces the old worker-owned NSGA `study.optimize()` pattern with a centralized main-process `ask/tell` dispatcher for multiprocess NSGA samplers only.

Workers are now evaluator-only processes. The main process owns the single Optuna study and sampler, filters duplicate proposals before expensive evaluation, and keeps user-visible trial counts aligned with real evaluated unique trials.

## 2. Problem solved

Before this update, Merlin could waste a large part of the NSGA budget on repeated exact parameter combinations.

Observed real studies showed:

- `NSGA2 + coverage ON`: severe duplicate rates
- `NSGA2 + coverage OFF`: duplicates still present, but lower
- `TPE`: clean in the latest checked S01/S03 studies

The issue was not:

- DB corruption
- UI hash collision
- strategy-loop performance refactor
- RAM journal migration

The effective problem was the multiprocess NSGA architecture:

1. Multiple workers owned separate sampler instances.
2. Workers sampled concurrently from the same evolving study state.
3. Merlin had no duplicate-trial suppression before evaluation.
4. Coverage mode amplified the issue by feeding NSGA a highly homogeneous startup population.

## 3. What was changed

## 3.1 Multiprocess NSGA now uses centralized `ask/tell`

File:

- `src/core/optuna_engine.py`

New behavior for `worker_processes > 1` and sampler in `{nsga2, nsga3}`:

1. Main process creates the only Optuna study and the only NSGA sampler.
2. Main process enqueues coverage trials exactly as before.
3. Main process calls `study.ask()`.
4. Main process resolves full parameter payloads via `_prepare_trial_parameters(...)`.
5. Main process suppresses exact duplicates before evaluation.
6. Only unique parameter sets are dispatched to evaluator workers.
7. Workers evaluate strategy performance only.
8. Main process applies results back via `study.tell(...)`.

This removes the former race between multiple worker-owned NSGA samplers.

## 3.2 New evaluator-only worker path

File:

- `src/core/optuna_engine.py`

Added evaluator worker entrypoint:

- `_evaluator_worker_entry(...)`

This worker:

1. loads the CSV source in RAM exactly as before
2. prepares strategy/data once
3. receives already-sampled params from the main process
4. computes the full optimization payload
5. sends metrics/objective data back to the main process

Workers no longer own Optuna studies or samplers in the NSGA multiprocess path.

## 3.3 Shared evaluation logic extracted

File:

- `src/core/optuna_engine.py`

Added:

- `OptunaOptimizer._evaluate_trial_payload(...)`

This consolidates the core per-trial evaluation logic:

- backtest execution
- score calculation
- objective extraction
- sanitization handling
- constraint evaluation

This keeps the new NSGA dispatcher consistent with the existing single-process and legacy multiprocess paths.

## 3.4 Duplicate suppression added before evaluation

File:

- `src/core/optuna_engine.py`

Added:

- `_build_params_key(...)`
- duplicate suppression using exact canonicalized parameter keys
- tracking of both:
  - `seen_keys`
  - `in_flight_keys`

Behavior:

- duplicate proposals are not sent to workers
- duplicate proposals are logged
- duplicate proposals do not appear in Merlin results
- duplicate proposals do not count toward user-visible evaluated trial totals

This suppression is exact-match deduplication, not heuristic deduplication.

## 3.5 Coverage mode preserved unchanged

File:

- `src/core/optuna_engine.py`

The following behavior was intentionally preserved:

- `_generate_coverage_trials(...)` unchanged
- `_enqueue_coverage_trials(...)` unchanged
- midpoint-based startup coverage unchanged
- all categorical combinations still covered exactly as before

This was required so that repeated studies keep the same deterministic startup coverage conditions.

## 3.6 Coverage generation metadata preserved for NSGA

File:

- `src/core/optuna_engine.py`

The current helper:

- `_mark_coverage_generation_for_nsga(...)`

was kept and reused in the new NSGA dispatcher path.

Reason:

Optuna enqueued fixed-parameter trials do not automatically receive NSGA generation metadata when those params bypass sampler suggestion. Coverage trials still need explicit generation tagging so they remain visible as generation `0`.

## 3.7 Finite-space early stop added

File:

- `src/core/optuna_engine.py`

Added:

- `_estimate_search_space_size(...)`

For fully finite discrete spaces, the new NSGA dispatcher can stop early once all unique combinations have been evaluated or are already in flight.

This prevents wasting time when budget exceeds the exact finite search-space size.

## 3.8 DB summary counts now ignore skipped duplicate proposals

Files:

- `src/core/optuna_engine.py`
- `src/core/storage.py`

Skipped duplicate asked trials are marked internally and excluded from:

- final summary counts
- persisted study `total_trials`
- persisted study `completed_trials`
- persisted study `pruned_trials`

This keeps visible trial counts aligned with real evaluated unique trials.

## 3.9 TPE and single-process paths were left unchanged

Files:

- `src/core/optuna_engine.py`

Dispatch behavior now is:

- single-process: unchanged
- multiprocess `tpe` / `random`: unchanged legacy path
- multiprocess `nsga2` / `nsga3`: new centralized `ask/tell` path

This was verified with explicit routing tests.

## 4. Key logic and targets of the update

The target was not merely to hide duplicate rows in UI.  
The target was to stop duplicate NSGA proposals from consuming real optimization work.

The new logic achieves that by separating two responsibilities:

### Main process

- owns sampler and study
- samples candidates
- handles coverage queue
- detects duplicates
- manages budget and worker orchestration
- writes Optuna trial results back

### Worker processes

- evaluate already-sampled parameter sets only
- do no sampler work
- do no study ownership

This design removes the previous root cause without altering TPE or single-process behavior.

## 5. Files changed

- `src/core/optuna_engine.py`
- `src/core/storage.py`
- `tests/test_coverage_startup.py`
- `tests/test_multiprocess_score.py`
- `docs/phase_2-2-2_fixed-dublicated-nsga-trials_report.md` (this report)

## 6. Reference tests added

### `tests/test_coverage_startup.py`

Added:

1. `test_optuna_optimizer_routes_nsga_multiprocess_to_centralized_path`
2. `test_optuna_optimizer_keeps_tpe_on_legacy_multiprocess_path`

These verify:

- multiprocess NSGA now routes to the new dispatcher
- TPE remains on the existing multiprocess path

### `tests/test_multiprocess_score.py`

Added:

1. `test_nsga_multiprocess_small_space_stops_without_duplicate_results`
2. `test_nsga_multiprocess_preserves_coverage_trials_as_generation_zero`

These verify:

- multiprocess NSGA no longer wastes budget on duplicates in a finite small space
- coverage mode remains deterministic
- coverage trials still receive NSGA generation `0` metadata

## 7. Reference test results

Interpreter used:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

### Focused coverage tests

Command:

- `python.exe -m pytest -q tests/test_coverage_startup.py`

Result:

- `16 passed`

### Focused multiprocess Optuna tests

Command:

- `python.exe -m pytest -q tests/test_multiprocess_score.py`

Result:

- `7 passed`

### Full Merlin suite

Command:

- `python.exe -m pytest -q`

Result:

- `257 passed`

## 8. Validation notes

### 8.1 Windows sandbox restriction

Inside the sandbox, Windows multiprocessing pipes/queues/manager creation can fail with:

- `PermissionError: [WinError 5] Access is denied`

This is an environment restriction, not a code regression.

The focused multiprocess tests and the full suite were rerun outside the sandbox and passed successfully.

### 8.2 Warnings observed

Only existing/expected warnings were present:

- Optuna experimental warning for `NSGAIIISampler`
- Optuna experimental warning for multivariate TPE
- Optuna experimental warning for `Study.set_metric_names`

No new runtime warnings specific to this patch were introduced.

## 9. Safety and compatibility assessment

### Safe aspects

- `TPE` path unchanged
- single-process path unchanged
- coverage generation unchanged
- WFA CSV-in-RAM behavior unchanged
- current RAM runtime behavior preserved
- DB schema unchanged
- results persistence format unchanged

### Behavioral changes

Only multiprocess `NSGA2` / `NSGA3` behavior changed.

Specifically:

- duplicate exact parameter proposals are now intercepted before evaluation
- user-visible counts now reflect unique evaluated trials only
- finite search spaces may stop early once exhausted instead of repeating combinations

### Performance expectation

Expected performance is neutral to slightly better overall:

- evaluation remains parallel
- dispatcher overhead is small relative to a full backtest
- duplicate evaluations are removed
- NSGA contention on shared study ownership is reduced

## 10. Problems solved by this update

1. **Duplicate exact NSGA trials no longer waste evaluation budget**
   - duplicate proposals are filtered before backtesting

2. **Coverage mode no longer amplifies duplicate waste into visible repeated results**
   - deterministic startup remains intact
   - duplicate proposals after coverage are suppressed

3. **Visible study counts now match real evaluated unique trials**
   - skipped duplicate asks are internal only

4. **TPE and single-process stability is preserved**
   - no changes to those execution paths

## 11. Errors encountered during implementation

### Environment issue

- Initial multiprocess pytest runs inside the sandbox failed due Windows multiprocessing IPC restrictions.
- Resolution: reran the multiprocess tests and the full suite outside the sandbox.

### Code issues

- No final code failures remained after the completed patch set.
- Full suite passed after implementation.

## 12. Final status

Update completed successfully.

The duplicated NSGA trial issue is fixed in the supported target scope:

- multiprocess `NSGA2`
- multiprocess `NSGA3`

Coverage mode remains deterministic and unchanged.  
TPE and single-process behavior remain intact.  
All reference validation passed.
