# Phase 2-2-4: Fixed Duplicated TPE Trials

## Goal

Fix the remaining multiprocess Optuna duplicate-trial waste for `TPESampler`.

After the NSGA centralized ask/tell fix and the S03 bool-dependency fix, Merlin still showed exact duplicate TPE trials in real multiprocess studies. The issue was visible in the latest S03 SUI Optuna run:

- study `bed1fe3c-0656-43b1-874e-d0bd522ed7e5`
- `1500` stored trial rows
- `1473` unique exact `params_json`
- `27` exact duplicate rows

Coverage mode itself was not the cause:

- the first `99` coverage trials were unique
- duplicates started only after coverage
- the first repeats were clustered tightly (`132 -> 133 -> 134`, etc.), which pointed to concurrent worker sampling on the same study state

The fix had to:

- remove exact duplicate TPE evaluations in multiprocess mode
- preserve deterministic coverage mode
- keep single-process behavior intact
- stay RAM-only
- work correctly with S03 conditional search space
- remain consistent with the already-fixed NSGA multiprocess architecture

## Root Cause

Two issues were interacting:

### 1. Legacy multiprocess TPE architecture

Multiprocess TPE was still using the old worker-owned `study.optimize()` model:

- each worker had its own sampler instance
- all workers sampled against the shared study/storage concurrently
- multiple workers could propose the same exact params before the study state advanced

This was the same class of architectural problem that previously affected NSGA.

### 2. Dynamic S03 search space with old TPE sampler configuration

After the S03 bool fix, S03 correctly became a dynamic conditional search space:

- `useCloseCount=False` removes `closeCountLong` / `closeCountShort`
- `useTBands=False` removes `tBandLongPct` / `tBandShortPct`

Merlin was still creating TPE as:

- `TPESampler(multivariate=True)`

But Optuna documents that dynamic search spaces require grouped multivariate TPE:

- `group=True`

Without that, TPE falls back to independent sampling for some conditional params, which increases waste and causes warning spam.

## Implemented Changes

### 1. Switched multiprocess TPE to centralized ask/tell

File:

- `src/core/optuna_engine.py`

Multiprocess TPE now uses the same evaluator-worker orchestration model as multiprocess NSGA:

- main process owns the only Optuna study
- main process owns the only TPE sampler
- workers are evaluator-only processes
- main process dispatches params via `study.ask()`
- workers only compute backtest payloads and return results
- main process finishes trials with `study.tell()`

This removes concurrent worker-owned TPE sampling against the same study state.

### 2. Added exact duplicate suppression for multiprocess TPE

File:

- `src/core/optuna_engine.py`

The centralized dispatcher now applies:

- `_build_params_key(...)`
- `seen_keys`
- `in_flight_keys`

for TPE the same way as for NSGA.

Result:

- exact duplicate proposals are skipped before evaluation
- skipped duplicates do not consume the user trial budget
- skipped duplicates are not stored in Merlin DB results
- skipped duplicates are visible only in logs

### 3. Enabled grouped multivariate TPE with constant liar

File:

- `src/core/optuna_engine.py`

TPE sampler construction now uses:

- `multivariate=True`
- `group=True`
- `constant_liar=True`

Why:

- `group=True` matches Merlin’s conditional S03 search space
- `constant_liar=True` is the correct batch/distributed TPE setting to discourage workers from clustering around the same in-flight region
- this reduces independent-sampling fallback and makes multiprocess TPE behavior better aligned with Optuna’s intended parallel use

### 4. Kept coverage mode unchanged

File:

- `src/core/optuna_engine.py`

Coverage mode behavior was preserved:

- same deterministic startup count
- same deterministic startup lattice
- same coverage trial generation

Only the execution architecture changed after those coverage trials are enqueued.

### 5. Preserved single-process TPE behavior

File:

- `src/core/optuna_engine.py`

Single-process optimization still uses the existing `study.optimize()` path.

The TPE fix is isolated to multiprocess mode.

### 6. Added explicit centralized trial-completion logs

File:

- `src/core/optuna_engine.py`

Because centralized ask/tell does not emit Optuna’s old worker-local completion log lines automatically, Merlin now logs centralized trial completion explicitly:

- objective values
- params payload used for the trial

This keeps TPE centralized runs observable in logs.

## Behavioral Result

After this update:

- multiprocess TPE no longer evaluates exact duplicate parameter sets
- duplicate TPE proposals are skipped before backtest execution
- skipped duplicates are filtered out from final trial/result accounting
- coverage mode still starts from the same deterministic trial set
- single-process runs remain on their existing simpler path
- S03 conditional branches are sampled with grouped multivariate TPE instead of the previous mismatched configuration

## Tests Added / Updated

### Coverage / routing tests

File:

- `tests/test_coverage_startup.py`

Updated / added:

- TPE multiprocess now routes to the centralized dispatcher
- TPE sampler settings are verified:
  - multivariate
  - group
  - constant liar

### Multiprocess integration tests

File:

- `tests/test_multiprocess_score.py`

Updated / added:

- TPE worker-failure regression adjusted for the new centralized error message
- new small-space TPE multiprocess regression:
  - centralized TPE stops at unique combinations
  - duplicate results are not emitted
  - summary counts reflect only effective trials

### Existing validation retained

Files:

- `tests/test_walkforward.py`
- `tests/test_s03_reversal_v10.py`

These continued to validate:

- multiprocess WFA path
- RAM-only CSV transport
- S03 conditional parameter behavior

## Verification Results

### Focused regression suite

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_coverage_startup.py tests/test_multiprocess_score.py tests/test_s03_reversal_v10.py tests/test_walkforward.py -q
```

Result:

- `43 passed`

### Full suite

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q
```

Result:

- `264 passed`

## Issues Encountered During Validation

### 1. Windows sandbox multiprocessing restriction

Inside the sandbox, Windows multiprocessing IPC objects (`Manager`, `Queue`, `Pipe`) are restricted and multiprocess tests fail with `PermissionError: [WinError 5]`.

This is an environment limitation, not a Merlin code problem.

Resolution:

- reran focused multiprocess tests outside the sandbox
- reran the full suite outside the sandbox
- both passed

### 2. Optuna experimental warnings

Optuna emits expected experimental warnings for:

- `multivariate=True`
- `group=True`
- `constant_liar=True`
- `NSGAIIISampler`
- `Study.set_metric_names(...)`

These are library warnings only. They did not cause failures.

## Final Assessment

This update fixes the remaining real TPE duplicate-trial problem in multiprocess Merlin.

It is:

- effective: exact duplicate TPE proposals no longer spend budget
- accurate: duplicate suppression is based on full exact parameter payloads
- robust: the same centralized evaluator architecture now covers both NSGA and TPE multiprocess paths
- safe: single-process behavior is preserved and coverage mode is unchanged
- future-proof: grouped multivariate TPE now matches Merlin’s conditional strategy spaces better

The result is a cleaner multiprocess Optuna architecture:

- centralized sampling in the main process
- evaluator-only workers
- deterministic coverage preserved
- RAM-only runtime state preserved
- exact duplicate trial waste removed before evaluation
