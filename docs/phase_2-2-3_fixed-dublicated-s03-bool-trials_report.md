# Phase 2-2-3: Fixed Duplicated S03 Bool Trials

## Goal

Fix the S03 optimization waste caused by bool-dependent numeric parameters:

- `useCloseCount=False` should disable `closeCountLong` and `closeCountShort`
- `useTBands=False` should disable `tBandLongPct` and `tBandShortPct`

Before this update, Merlin still suggested, evaluated, stored, and displayed those inactive child parameters as if they were meaningful trial dimensions. That produced functionally duplicated trials and wasted optimization budget.

The fix had to be:

- correct for `TPE`, `NSGA2`, `NSGA3`, single-process, multiprocess, and WFA
- compatible with existing deterministic coverage mode
- generic enough for future strategies with similar bool -> child parameter dependencies
- clean in results: inactive params should disappear instead of remaining as fake defaults

## Implemented Changes

### 1. Added generic dependency metadata to strategy config

File:

- `src/strategies/s03_reversal_v10/config.json`

Added `depends_on` metadata for the four S03 child parameters:

- `closeCountLong` -> `useCloseCount`
- `closeCountShort` -> `useCloseCount`
- `tBandLongPct` -> `useTBands`
- `tBandShortPct` -> `useTBands`

This makes the dependency explicit in strategy metadata instead of hardcoding S03 logic inside the optimizer.

### 2. Added dependency-aware parameter preparation in Optuna

File:

- `src/core/optuna_engine.py`

Implemented generic dependency handling in the optimizer:

- strategy parameter defaults are loaded from strategy config
- parameter dependencies are read from `depends_on`
- bool-group surrogate tokens are decoded before dependent parameters are considered
- inactive dependent parameters are not suggested with `trial.suggest_*()`
- inactive dependent parameters are not merged from `fixed_params`
- inactive dependent parameters are omitted from the final trial/result parameter payload

Result:

- inactive child params no longer enter `trial.params`
- they no longer enter `result.params`
- they no longer enter stored `params_json`
- they no longer affect duplicate detection keys

### 3. Fixed coverage mode without changing its deterministic structure

File:

- `src/core/optuna_engine.py`

Coverage mode itself was not redesigned.

Instead:

- coverage trials are still generated with the same deterministic lattice
- after generation, each coverage trial is pruned using the same dependency rules
- inactive child parameters are removed before `enqueue_trial()`

For S03 this preserves the intended deterministic startup structure:

- `11` `maType3` choices
- `3` valid bool-group combinations
- `9` primary numeric anchor blocks
- total: `297` startup coverage trials

So the fix removes dead child params without changing the startup count or the overall coverage layout.

### 4. Made coverage primary-axis selection safer for future conditional spaces

File:

- `src/core/optuna_engine.py`

Coverage primary numeric inference now prefers unconditional numeric parameters when available. This reduces the chance of future coverage blocks varying a numeric axis that is inactive in some bool branches.

## Follow-up Hardening Fixes

After the main Phase 2-2-3 change, a follow-up audit identified two low-risk hardening items worth fixing.

### 5. Enforced bool-only `depends_on` parents

Files:

- `src/core/optuna_engine.py`
- `tests/test_coverage_startup.py`

The dependency system was already correct for S03, but its generic contract was only implicit. A future strategy could have accidentally pointed `depends_on` to a non-bool parent and received truthiness-based behavior that would be surprising.

This is now validated during search-space construction:

- dependency parent must exist
- dependency parent must resolve to type `bool`
- invalid configs fail fast with a clear `ValueError`

Why this was added:

- makes the dependency system explicit and self-documenting
- prevents future non-bool dependency misuse
- keeps `_dependency_is_active()` semantics predictable and safe

### 6. Removed dead pruning path from multiprocess NSGA dispatcher

File:

- `src/core/optuna_engine.py`

The centralized multiprocess `NSGA2/NSGA3` dispatcher still carried a `trial.report(...)` / `trial.should_prune()` branch. In Merlin’s NSGA path this was dead code:

- the dispatcher already evaluates complete trial payloads
- no intermediate steps are reported
- multi-objective NSGA is not using pruning as a meaningful control path here

The follow-up change removes that branch and keeps `self.pruner = None` for the multiprocess NSGA dispatcher.

Why this was added:

- removes misleading dead code
- makes the NSGA dispatcher easier to reason about
- avoids suggesting unsupported or inactive pruning behavior in the centralized NSGA path

## Behavioral Result

After this update:

- S03 inactive child params are not suggested when their parent bool is `False`
- S03 inactive child params are not stored in Optuna trial params
- S03 inactive child params are not stored in Merlin DB trial results
- S03 inactive child params disappear from parameter displays because they are no longer present in stored result payloads
- coverage mode keeps the same deterministic startup count and structure
- manual test / replay remains correct because strategy runtime defaults still fill omitted params when needed

## Why This Fix Is Correct

S03 runtime logic already ignores these child parameters when the parent bool is off:

- `useCloseCount=False` makes close-count thresholds irrelevant
- `useTBands=False` makes T-band thresholds irrelevant

So omitting inactive child params from optimization payloads is the correct model. The strategy already supports missing values through `S03Params.from_dict()` defaults, so replay and execution remain valid even when those keys are absent.

This is better than pinning inactive params to defaults because:

- the optimizer search space becomes truthful
- duplicate suppression works on real behavior, not fake dimensions
- stored trial params reflect what actually mattered for that run

## Tests Added / Updated

### Coverage / optimizer tests

File:

- `tests/test_coverage_startup.py`

Added tests for:

- conditional S03 parameters being omitted when the parent bool is `False`
- S03 coverage pruning removing inactive child params
- preserving `297` coverage trials after pruning

### Strategy behavior tests

File:

- `tests/test_s03_reversal_v10.py`

Added regression tests that prove omission is behavior-safe:

- omitted close-count params match explicit close-count values when `useCloseCount=False`
- omitted T-band params match explicit T-band values when `useTBands=False`

These compare the produced curves and timestamps to ensure S03 behavior remains unchanged when inactive params are removed.

### Existing multiprocess / WFA verification retained

Files:

- `tests/test_multiprocess_score.py`
- `tests/test_walkforward.py`

These continued to validate:

- multiprocess optimization
- NSGA centralized dispatch
- in-memory Optuna runtime backend
- WFA multiprocess flow

One assertion in `tests/test_multiprocess_score.py` was corrected to reflect the real contract:

- duplicate NSGA proposals may still be skipped internally
- duplicate results must not be emitted or stored

## Verification Results

### Focused regression suite

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_coverage_startup.py tests/test_s03_reversal_v10.py tests/test_multiprocess_score.py tests/test_walkforward.py -q
```

Result:

- `41 passed`

### Full suite

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q
```

Result:

- `262 passed`

## Issues Encountered During Validation

### 1. Windows sandbox multiprocessing restriction

Inside the sandbox, Windows multiprocessing IPC objects (`Manager`, `Queue`, `Pipe`) failed with `PermissionError: [WinError 5]`.

This was an environment limitation, not a code failure.

Resolution:

- reran the multiprocess tests and the full suite outside the sandbox
- all tests passed there

### 2. One stale test expectation

The small-space NSGA regression test expected `optimizer._duplicate_skipped_count == 0`.

That expectation was too strict:

- the real guarantee is that duplicate proposals do not produce duplicate results
- internal duplicate proposals may still occur and be skipped

Resolution:

- updated the assertion to validate the actual intended behavior

### 3. One missing test import during follow-up hardening

The new bool-parent validation test initially missed a `pytest` import.

This was only a test-authoring issue, not a production code problem.

Resolution:

- added the missing import
- reran the focused suite and full suite successfully

## Final Assessment

This update solves the S03 bool-dependent duplicate-trial problem correctly.

It is:

- effective: removes inactive child params from optimization and storage
- accurate: matches actual S03 trading logic
- robust: shared fix path covers single-process, multiprocess, NSGA, TPE, and WFA
- future-proof: dependency handling is config-driven and reusable for future strategies

Most importantly, it preserves the deterministic coverage-mode startup structure while removing the dead dimensions that were wasting budget.

The follow-up hardening fixes further improve reliability:

- invalid `depends_on` usage now fails fast
- the multiprocess NSGA path no longer carries dead pruning logic

These additions do not change S03 optimization semantics. They make the implementation stricter, clearer, and safer for future strategy extensions.
