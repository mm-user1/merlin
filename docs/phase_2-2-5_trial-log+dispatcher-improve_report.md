# Phase 2.2.5 Report: Trials Log + Dispatcher Improvement

## Summary

This update introduced two targeted improvements to Merlin's Optuna workflow:

1. A new separate `Trials Log` switch for per-trial terminal logging.
2. A shared batch-oriented result handling improvement for the centralized multiprocess dispatcher used by `TPE`, `NSGA2`, and `NSGA3`.

The goal was to reduce avoidable terminal/logging overhead, preserve minimal useful study lifecycle visibility when trial logging is disabled, and improve centralized multiprocess queue efficiency without changing the already-correct duplicate-trial fixes.

After the main implementation, a small follow-up logging fix was also applied:

3. Console logger routing for the `core` logger family was configured in server startup so the new lifecycle lines are actually visible in PowerShell.
4. The visible start-phase budget lines now include the CSV/ticker file name, so the currently processed source is visible immediately even before study completion.
5. The shared centralized dispatcher now yields out of duplicate-heavy refill cycles so fresh worker results can be processed sooner.
6. Experimental dispatcher controls were added to the Optuna Advanced Settings UI so batch result processing and the soft duplicate-cycle limit can be toggled per study.

No filesystem-storage behavior was changed in this update. The current RAM-backed Optuna runtime workflow remains intact.

## Problems Addressed

### 1. Detailed per-trial logs and sanitize-detail logs were coupled

Before this update, Merlin had only `Detailed log`, which is used for extended sanitize/failure logging. After the centralized `ask/tell` refactor, per-trial centralized completion logs were also emitted from Merlin code, but there was no dedicated user-facing control for them.

This was undesirable because:

- terminal output could become noisy on large studies
- logging overhead could become visible on short/fast trials
- `Detailed log` has a different purpose and should remain focused on sanitize/failure detail

### 2. Centralized dispatcher processed results one-by-one

The shared centralized dispatcher for multiprocess `TPE` and `NSGA` previously:

- dispatched work
- waited for one worker result
- processed one result
- looped again

This left avoidable queue overhead in place when multiple worker results were already ready at the same time.

That behavior was correct, but not optimal.

## Implemented Changes

## A. New `Trials Log` UI/Config Path

A new `Trials Log` checkbox was added next to `Detailed log` in the main UI.

Changed files:

- `src/ui/templates/index.html`
- `src/ui/static/js/ui-handlers.js`
- `src/ui/static/js/queue.js`
- `src/ui/server_services.py`
- `src/ui/server_routes_run.py`
- `src/core/walkforward_engine.py`

### Behavior

`Detailed log`:

- unchanged
- still controls extended sanitize/failure detail

`Trials Log`:

- new
- controls per-trial completion logging
- controls duplicate-skip per-trial logging in centralized dispatcher
- is forwarded through normal Optuna mode and WFA mode
- is preserved in queue config load/save paths

### WFA forwarding

The WFA window optimizer now forwards:

- `csv_original_name`
- `detailed_log`
- `trials_log`

into the per-window `OptimizationConfig`, so lifecycle study logs inside WFA windows can show the real CSV/ticker name instead of a generic in-memory source label.

## B. Minimal Lifecycle Logging When `Trials Log` Is Off

The optimizer now emits explicit study lifecycle lines with the CSV/ticker file name:

- at study start
- at study finish

These logs include:

- execution mode / sampler label
- source CSV display name
- objectives
- budget mode
- worker count
- coverage count or `off`
- finish elapsed time
- completed/pruned/duplicate-skipped counts

This gives useful visibility while keeping terminal noise low.

### Important design choice

No periodic progress lines were added. This follows the requested behavior: minimal lifecycle visibility without repeated progress spam.

## C. Single-Process Optuna Trial Logging Is Now Controlled Properly

Single-process Optuna previously relied on Optuna's native INFO logs.

This update added a scoped Optuna logging-verbosity context so that:

- `Trials Log = on` -> Optuna INFO trial-finished logs remain visible
- `Trials Log = off` -> Optuna INFO trial-finished logs are suppressed
- Optuna warnings still remain visible

This keeps single-process behavior aligned with centralized multiprocess behavior.

## D. Legacy Multiprocess Worker `study.optimize()` Logging Is Also Controlled

The legacy multiprocess worker path now applies the same scoped Optuna verbosity logic around worker-owned `study.optimize()`.

This keeps terminal behavior consistent for any remaining non-centralized multiprocess sampler paths.

## E. Shared Centralized Dispatcher Now Processes Result Batches

Changed file:

- `src/core/optuna_engine.py`

New internal helpers were added:

- `_drain_result_queue_nowait(...)`
- `_wait_for_result_batch(...)`
- `_process_centralized_result_batch(...)`

### New queue behavior

The centralized dispatcher now:

1. Drains any already-ready worker results first.
2. Processes that ready set as one batch.
3. Only then decides whether to dispatch more trials.
4. When it must block, it waits for one result and then drains any additional ready results immediately into the same batch.

### Why this is better

This reduces queue round-trip overhead and lets the sampler see more newly completed trials before the next wave of `ask()` calls.

This batching improvement is shared by:

- multiprocess `TPE`
- multiprocess `NSGA2`
- multiprocess `NSGA3`

So the dispatcher improvement is consistent across centralized sampler modes.

## F. `constant_liar` Was Not Changed

Per the agreed scope, `TPE` still uses:

- `multivariate=True`
- `group=True`
- `constant_liar=True`

This update does not tune TPE's sampling behavior. It only improves logging control and dispatcher orchestration.

## G. Follow-Up Console Logger Fix

After validation in the live UI, it became clear that the lifecycle log lines were correct in code but were not visible in the server terminal.

Root cause:

- Optuna and Werkzeug were emitting visible console logs
- Merlin's `core.optuna_engine` lifecycle logs used the `core.*` logger family
- that logger family was not explicitly configured to emit `INFO` to the console

Smallest acceptable fix applied:

- configure the `core` logger family in `src/ui/server.py`
- attach one idempotent console `StreamHandler`
- set level to `INFO`
- keep propagation enabled so pytest `caplog` and other upstream handlers can still capture Merlin logs

This made the lifecycle logs from `src/core/optuna_engine.py` visible in PowerShell without changing the lifecycle-log code itself.

## H. Start-Phase CSV Visibility Improvement

To make the currently processed CSV/ticker visible immediately, the start-phase budget logs were updated to include the CSV display name.

Examples after the follow-up:

- `Global trial budget for OKX_SUIUSDT.P, 30 ...csv: 1500`
- `Time budget per study for OKX_SUIUSDT.P, 30 ...csv: 3600s`

This complements the explicit end-of-study lifecycle line and makes the active data source obvious at the beginning of the run.

## I. Follow-Up Shared Soft Duplicate-Cycle Yield Limit

After real `TPE` WFA runs were reviewed, it became clear that the shared centralized dispatcher could still spend too long inside the inner refill loop when a sampler repeatedly proposed exact duplicates.

Root problem:

- the inner loop kept calling `study.ask()`
- each duplicate proposal was immediately marked skipped
- but the dispatcher could continue spinning inside the same refill cycle before returning to process already-completed worker results

That behavior was especially harmful for `TPE`, but the control-flow issue itself belongs to the shared centralized dispatcher.

Follow-up fix applied:

- add `_get_dispatch_cycle_duplicate_limit(n_workers)`
- use that soft limit inside the shared inner refill loop in `src/core/optuna_engine.py`
- after too many duplicate proposals in one refill cycle, break only the inner loop
- return control to the outer loop so Merlin can drain/process fresh worker results and refresh study state
- keep the existing hard `duplicate_retry_limit` unchanged as the emergency stop

This is a scheduler/orchestration fix, not a sampler-logic rewrite:

- duplicate suppression semantics are unchanged
- saved trial semantics are unchanged
- coverage behavior is unchanged
- sampler selection is unchanged

The follow-up applies generically to:

- multiprocess `TPE`
- multiprocess `NSGA2`
- multiprocess `NSGA3`

## J. Experimental Dispatcher Controls In Advanced Settings

The centralized dispatcher improvements are now user-configurable from:

- `Optimizer Parameters -> Optuna Settings -> Advanced Settings`

UI changes:

- `Coverage mode` now sits on the same line as `Initial trials`
- new checkbox: `Dispatcher batch result processing`
- new checkbox + numeric field: `Soft duplicate-cycle limit [18]`

Behavior:

- checkbox-first layout follows Merlin's existing UI convention
- the numeric input is disabled when `Soft duplicate-cycle limit` is off
- the limit is validated/clamped to `1..1000`
- default value is the explicit fixed value `18`
- these settings are saved in the run payload, restored from queue items, forwarded through WFA child runs, and exposed in analytics summary payloads

Scope:

- applies only to the shared centralized multiprocess dispatcher
- therefore affects centralized multiprocess `TPE`, `NSGA2`, and `NSGA3`
- does not affect single-process optimization

## Key Logic / Targets Achieved

This update was designed to achieve the following targets:

1. Keep `Detailed log` semantics unchanged.
2. Add an explicit, separate user control for trial-by-trial terminal logs.
3. Keep terminal output minimal by default.
4. Show the actual CSV/ticker name at study start and end.
5. Improve centralized dispatcher efficiency without changing duplicate suppression semantics.
6. Apply dispatcher batching consistently to both `TPE` and `NSGA`.
7. Keep RAM-backed Optuna runtime storage unchanged.
8. Ensure lifecycle logger messages are actually visible in the terminal.
9. Make the active CSV visible in the start-phase budget line.
10. Prevent duplicate-heavy centralized refill cycles from monopolizing the dispatcher before fresh results are processed.

All of these targets were implemented.

## Code-Level Notes

### `src/core/optuna_engine.py`

Main additions:

- `OptimizationConfig.trials_log`
- `OptimizationConfig.dispatcher_batch_result_processing`
- `OptimizationConfig.dispatcher_soft_duplicate_cycle_limit_enabled`
- `OptimizationConfig.dispatcher_duplicate_cycle_limit`
- `_scoped_optuna_trial_logging(...)`
- `_drain_result_queue_nowait(...)`
- `_wait_for_result_batch(...)`
- `_drain_dispatch_results_nowait(...)`
- `_wait_for_dispatch_results(...)`
- `_normalize_dispatch_cycle_duplicate_limit(...)`
- `_trials_log_enabled(...)`
- `_describe_execution_label(...)`
- `_get_dataset_label(...)`
- `_log_study_start(...)`
- `_log_study_end(...)`
- `_process_centralized_result_batch(...)`

Main behavior changes:

- centralized per-trial completion logs are gated by `trials_log`
- centralized duplicate-skip per-trial logs are gated by `trials_log`
- single-process Optuna INFO trial logs are gated by `trials_log`
- legacy multiprocess worker Optuna INFO trial logs are gated by `trials_log`
- centralized dispatcher now drains/processes ready results in batches
- centralized dispatcher now yields out of duplicate-heavy refill cycles after a bounded soft limit

### UI / server wiring

The new checkbox is now:

- collected from the UI payload
- parsed safely on the server
- stored in `OptimizationConfig`
- restored from queued config payloads
- forwarded into WFA child optimization runs
- persisted in saved Optuna/WFA config payloads
- exposed through analytics summary payloads

### Follow-up server logger routing

`src/ui/server.py` now configures the `core` logger family with a dedicated console handler so `INFO` lifecycle logs from `src/core/optuna_engine.py` are visible in the PowerShell terminal, while still allowing upstream log capture.

### Follow-up start-phase CSV visibility

`src/core/optuna_engine.py` budget-start messages now include the CSV/ticker display name in both:

- centralized multiprocess path
- legacy multiprocess path

### Follow-up shared duplicate-cycle yield control

`src/core/optuna_engine.py` now uses a soft per-cycle duplicate limit in the shared centralized dispatcher:

- threshold: configurable per study, default `18`
- hard emergency limit remains unchanged at `1000` consecutive duplicates
- the soft limit breaks only the inner refill loop
- the outer loop then processes worker results and refreshes sampler state before asking again

### Follow-up dispatcher experiment controls

The shared centralized dispatcher now honors three runtime settings from `OptimizationConfig`:

- `dispatcher_batch_result_processing`
- `dispatcher_soft_duplicate_cycle_limit_enabled`
- `dispatcher_duplicate_cycle_limit`

These settings are wired through:

- `src/ui/templates/index.html`
- `src/ui/static/js/ui-handlers.js`
- `src/ui/static/js/optuna-ui.js`
- `src/ui/static/js/queue.js`
- `src/ui/server_services.py`
- `src/ui/server_routes_run.py`
- `src/ui/server_routes_analytics.py`
- `src/core/walkforward_engine.py`

## Reference Tests

Interpreter used:

`C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

### Focused validation

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_server.py tests/test_coverage_startup.py tests/test_walkforward.py -q
```

Result:

- `85 passed`

### Follow-up dispatcher validation

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_coverage_startup.py -q
```

Result:

- `24 passed`

### Focused multiprocess / WFA validation

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_multiprocess_score.py tests/test_walkforward.py -q
```

Result:

- `19 passed`

### Final full suite

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q
```

Result:

- `272 passed`

### Compile validation

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m py_compile src\core\optuna_engine.py src\core\walkforward_engine.py src\ui\server_services.py src\ui\server_routes_run.py src\ui\server_routes_analytics.py src\ui\server.py
```

Result:

- passed

### Added / updated regression coverage

- `tests/test_server.py`
  - verifies `trials_log` is parsed into `OptimizationConfig`
  - verifies dispatcher controls are parsed into `OptimizationConfig`
  - verifies the `core` console logger handler is configured only once
  - verifies analytics summary includes the saved dispatcher settings

- `tests/test_coverage_startup.py`
  - verifies centralized per-trial completion logging is suppressed unless `trials_log` is enabled
  - verifies study lifecycle logs include the CSV/ticker file name
  - verifies result-batch helper behavior drains all ready messages
  - verifies dispatcher helpers support both batch and single-result modes
  - verifies the duplicate-cycle limit normalization/clamping logic

- `tests/test_walkforward.py`
  - verifies WFA forwards `csv_original_name`, `detailed_log`, `trials_log`, and dispatcher controls into per-window optimization config

## Errors / Issues Encountered

### 1. Windows sandbox multiprocessing restriction

The focused test run initially hit the known Windows sandbox restriction:

- `PermissionError: [WinError 5] Access is denied`

This occurred when pytest exercised multiprocessing IPC objects inside the sandbox.

Resolution:

- reran the affected tests outside the sandbox with the required interpreter

This is an environment limitation, not a Merlin code defect.

### 2. Logger capture compatibility needed one extra adjustment

After the `core` console logger follow-up was in place, the full suite exposed a compatibility issue:

- the dedicated console handler was correct
- but `propagate = False` prevented pytest `caplog` from seeing some Merlin log records once `ui.server` had been imported

Resolution:

- keep the dedicated `core` console handler
- allow propagation so test log capture and upstream handlers still receive the records

This preserved terminal visibility and restored full-suite compatibility.

### 3. No functional code regressions found

After rerunning outside the sandbox:

- focused suite passed
- full suite passed

No Merlin logic regressions were found.

## Final Assessment

This update is correct, safe, and internally consistent with the current Optuna architecture.

It does not alter:

- duplicate-trial correctness fixes
- RAM-backed runtime storage
- coverage-mode logic
- sampler selection logic
- TPE `constant_liar` behavior

It does improve:

- logging control
- terminal noise by default
- study lifecycle visibility with ticker/file context
- actual visibility of lifecycle logs in the PowerShell terminal
- start-phase visibility of the active CSV/ticker before study completion
- centralized dispatcher queue efficiency
- centralized dispatcher stability when duplicate proposals spike in one refill cycle
- reproducible experimentation with centralized dispatcher behavior from the main UI

The implementation is future-proof in the sense that:

- `Trials Log` is now a first-class config property
- the batching logic lives in the shared centralized dispatcher
- dispatcher orchestration controls are now explicit config properties instead of hidden constants
- both `TPE` and `NSGA` benefit from the same orchestration improvement
- duplicate-heavy refill cycles now yield back to result processing instead of spinning unboundedly inside one dispatch pass

No further corrective action is required for this update.
