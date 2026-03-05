# Phase 2-1 Consistency Score Overhaul - Implementation Report

Date: 2026-03-05
Project: Merlin (`+Merlin/+Merlin-GH`)

## 1. Update Goal

Implemented the full Phase 2-1 transition from legacy monthly-profit percentage consistency to a stability-based consistency metric, with configurable IS segmentation and automatic FT/OOS/manual segment derivation, while preserving backward compatibility for legacy studies.

## 2. What Was Implemented

### 2.1 Core metric logic (`src/core/metrics.py`)

- Replaced old consistency logic with:
  - `consistency_score = median(sub_returns) / (1 + std(sub_returns)) * (1 - loss_ratio^1.5)`
- Added sub-period infrastructure:
  - `normalize_consistency_segments(...)`
  - `derive_auto_consistency_segments(...)`
  - `_calculate_subperiod_returns(...)`
- Updated `calculate_advanced(...)` to accept `consistency_segments` and compute consistency from equity sub-period returns (Sharpe/Sortino remain monthly-based).

### 2.2 Optimization and WFA pipeline

- Added `consistency_segments` to:
  - `OptimizationConfig` (`src/core/optuna_engine.py`)
  - `WFConfig` (`src/core/walkforward_engine.py`)
- Passed segment configuration through:
  - Optuna single-combination worker path
  - Fixed and adaptive WFA IS/OOS metric calculations
  - FT and OOS/manual test execution paths
- Updated default score bounds for consistency from `0..100` to `0..5` in backend/UI score configs.

### 2.3 Persistence and DB robustness (`src/core/storage.py`)

- Added new trial columns and schema migration guards:
  - `consistency_segments_used`
  - `ft_consistency_segments_used`
  - `oos_test_consistency_segments_used`
- Completed all write paths:
  - Optuna trial insert now stores IS segment metadata
  - Forward Test update stores FT segment metadata
  - OOS Test reset/update stores OOS segment metadata

### 2.4 Result data models and payload propagation

- Added segment metadata fields where needed:
  - `OptimizationResult.consistency_segments_used`
  - `FTResult.ft_consistency_segments_used`
- Included segment usage in returned test metrics payloads (manual/OOS) and preserved during rendering state mapping.

### 2.5 UI and display behavior

- Start page:
  - Added `Consistency Segments IS` input (`2..24`, default `4`)
  - Updated consistency labels (`Consistency %` -> `Consistency`)
  - Updated composite hints (`Profitable months %` -> `Stability score`)
  - Updated consistency bound inputs for new scale
  - Moved sanitize hint inline
- Results formatting:
  - Added formatter logic to show:
    - New metric as `value/segments`
    - Legacy metric as `value%`
- WFA/FT/OOS/manual result rendering:
  - Added segment propagation/fallback logic so consistency is formatted correctly in result tables and module views.

### 2.6 Tests

- Updated and expanded tests:
  - `tests/test_metrics.py`
    - Added helper-level tests for sub-period returns, normalization, auto-segment derivation, and consistency formula edge cases.
    - Updated regression assertion to validate consistency against the new formula output (instead of legacy baseline value).
  - `tests/test_storage.py`
    - Added schema assertions for the three new segment metadata columns.

## 3. Problems Solved

- Eliminated legacy `% profitable months` consistency weakness.
- Enabled consistency evaluation on configurable IS segmentation.
- Enforced consistent FT/OOS/manual segment derivation from IS segment size.
- Preserved legacy study readability via `%` fallback display.
- Closed persistence gaps so segment counts are actually saved and reloaded, not only computed transiently.

## 4. Key Technical Notes

- The implemented formula exactly follows the planned expression.
- Important consequence:
  - If all sub-periods are losing (`loss_ratio = 1`), the penalty term becomes `0`, so score becomes `0` (not negative).
  - Tests were aligned to this formula behavior.

## 5. Validation and Test Results

Interpreter used exactly as requested:

`C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Executed:

1. `-m pytest tests/test_metrics.py -q`
2. `-m pytest tests/test_metrics.py tests/test_score_normalization.py tests/test_storage.py tests/test_post_process.py tests/test_server.py -q`
3. `-m pytest tests/test_walkforward.py tests/test_adaptive_wfa.py tests/test_oos_selection.py tests/test_db_management.py tests/test_optuna_sanitization.py tests/test_multiprocess_score.py -q`
4. `-m pytest -q`

Final status:

- Full suite passed: **241 passed**
- Warnings: Optuna experimental warnings only (existing/non-blocking)
- Runtime errors/failures after final fixes: **none**

## 6. Errors Encountered During Implementation

- Found and fixed a runtime issue:
  - `run_forward_test(...)` used `metrics.normalize_consistency_segments(...)` without importing `metrics` in scope.
  - Added local import in function.
- Found and fixed persistence incompleteness:
  - Added missing insert/update SQL plumbing for new segment columns in `storage.py`.
- Found and fixed UI mapping gap:
  - OOS trials in results state were sourced from unmapped raw trial list; switched to mapped state list to preserve segment metadata fallback behavior.

## 7. Target Completion Assessment

Assessment: **Implemented, robust, and fully test-validated**.

- Core algorithm replacement: complete
- End-to-end segment propagation (IS/FT/OOS/manual): complete
- UI config and display updates: complete
- Backward compatibility handling for legacy studies: complete
- DB persistence and retrieval consistency: complete
- Regression safety: validated by full test suite pass
