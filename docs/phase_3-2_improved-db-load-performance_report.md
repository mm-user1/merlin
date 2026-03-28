# Phase 3-2 Report: Improved DB Load Performance for Analytics

## Executive Summary

This update improves Analytics page responsiveness by removing repeated heavy portfolio aggregation from the hot path and replacing it with a safe, additive, persistent cache model.

The final implementation does **not** rewrite or invalidate existing user data. Instead, it:

- keeps existing study-level stitched OOS data as the source of truth,
- adds lightweight metadata for studies,
- adds a new persistent cache table for `All Studies` and named sets,
- makes study and set curves load lazily on demand,
- stops the frontend from re-triggering expensive recomputation after set reorder,
- preserves the old ad-hoc aggregation path for arbitrary multi-selection.

This is intentionally future-proof and conservative:

- no destructive schema migration,
- no breaking change for legacy databases,
- cache is fully derivable and can be safely rebuilt,
- cache invalidation is based on real membership changes only.

## Initial Problem

The observed Analytics slowdown had three main sources:

1. `GET /api/analytics/summary` returned full `equity_curve` and `equity_timestamps` for every WFA study, which made the payload much heavier than necessary for the table view.
2. `Study Sets` metrics were computed by running expensive portfolio aggregation in batch, even though the table only needed scalar values such as `Ann.P%`, `Profit%`, and `MaxDD%`.
3. Reordering sets via `Move` invalidated frontend metrics state and caused unnecessary recomputation, even though membership did not change.

An earlier investigation also confirmed that loading the whole SQLite database into RAM would not be the right primary fix. The bottleneck was not SSD access, but repeated JSON parsing and Python-side curve aggregation.

## Final Solution

### 1. Lightweight Study Metadata in `studies`

Three additive columns were introduced in `studies`:

- `stitched_oos_start_ts`
- `stitched_oos_end_ts`
- `stitched_oos_point_count`

These fields are persisted for new WFA studies at save time and lazily backfilled for old databases.

This allows Analytics summary to compute annualization and OOS span without sending the full stitched curve for every study.

### 2. Persistent Cache Table for Group Analytics

A new additive SQLite table was introduced:

- `analytics_group_cache`

It stores exact cached analytics results for:

- synthetic `All Studies`
- every named study set

The cache persists:

- scalar portfolio metrics,
- overlap statistics,
- warning text,
- study usage/exclusion counts,
- aggregated equity curve and timestamps,
- optional return profile data,
- membership hash and computation time.

The cache is not authoritative data. It is a materialized derivative of existing study-level stitched OOS curves and can always be rebuilt safely.

### 3. Exact, Narrow Invalidation

Cache invalidation follows set membership, not UI order.

Implemented rules:

- `Move` / reorder: no invalidation
- rename: no invalidation
- color change: no invalidation
- `Update Set` with changed `study_ids`: invalidate only that set
- delete set: cached row is removed automatically via cascade
- save new WFA study: invalidate synthetic `All Studies`
- delete WFA study: invalidate `All Studies` and every set containing that study

This keeps cached results stable and avoids wasteful recalculation.

### 4. Lightweight Summary + Lazy Curves

`/api/analytics/summary` was redesigned to return compact study metadata instead of full study curves.

Summary now returns:

- scalar study metrics,
- compact stitched OOS metadata,
- annualization inputs,
- focus sidebar payloads,
- no full `equity_curve` / `equity_timestamps`.

New lazy endpoints were added:

- `GET /api/analytics/studies/<study_id>/equity`
- `GET /api/analytics/sets/<set_id>/equity`
- `GET /api/analytics/all-studies/equity`

This means the heavy curve data is loaded only when the user actually needs to see the chart.

### 5. Frontend Refactor

The Analytics frontend was updated to match the new backend behavior.

Main changes:

- `analytics-sets.js` no longer requests batch portfolio aggregation for the sets table.
- Set row metrics now come directly from persistent cached scalar data.
- Focusing a set loads its aggregated curve lazily from the cache-backed endpoint.
- Focusing a study loads its stitched OOS curve lazily from the study endpoint.
- Selecting all studies uses the synthetic `All Studies` cache instead of recomputing the same portfolio repeatedly.
- Arbitrary custom multi-selection still uses the old live aggregation endpoint, preserving flexibility.

The old frontend recomputation path that was sensitive to set order was removed from the hot path.

## How Behavior Changed

### Before

- Opening Analytics could trigger large summary payloads and expensive set aggregation.
- Study summary included full curve arrays for all studies.
- Set metrics were recalculated in batch even when only scalar values were displayed.
- Reordering sets caused pointless metrics refresh and waiting.
- Set focus chart was recomputed instead of read from persisted cache.

### After

- Analytics summary is compact.
- Set metrics are read from persistent cached results.
- Set focus chart is loaded from persisted cache.
- Single-study chart is lazy-loaded on demand.
- `All Studies` portfolio can use the same persistent cache model.
- Reordering sets no longer causes expensive metrics rebuild.

## Safety and Compatibility

This update was designed to be safe for existing databases.

Safety properties:

- schema changes are additive only,
- no existing tables or columns are removed,
- no existing payload needed by other features was destructively changed,
- old databases are upgraded lazily at runtime,
- missing cache rows are rebuilt automatically,
- if cache is absent or stale, exact values are recomputed from source study curves.

Legacy DB strategy:

- `CREATE TABLE IF NOT EXISTS` for the cache table,
- `ALTER TABLE ADD COLUMN` style additive study metadata handling,
- lazy backfill of `stitched_oos_*` compact metadata,
- no mandatory offline migration script.

## Files Changed

### Backend

- `src/core/storage.py`
- `src/ui/server_routes_analytics.py`

### Frontend

- `src/ui/static/js/api.js`
- `src/ui/static/js/analytics-table.js`
- `src/ui/static/js/analytics-sets.js`
- `src/ui/static/js/analytics.js`

### Tests

- `tests/test_storage.py`
- `tests/test_server.py`

## Test and Validation Results

### Python Syntax Validation

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m py_compile src/core/storage.py src/ui/server_routes_analytics.py tests/test_storage.py tests/test_server.py
```

Result:

- Passed

### Main Regression Suite

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_storage.py tests/test_server.py -q
```

Result:

- `78 passed`

Covered areas:

- additive schema expectations,
- persisted study metadata,
- set/all cache creation,
- cache invalidation on membership changes,
- lightweight summary contract,
- lazy study equity endpoint,
- lazy set equity endpoint,
- lazy all-studies equity endpoint,
- set CRUD / reorder / delete behavior.

### Related Analytics / WFA Tests

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_analytics.py tests/test_walkforward.py tests/test_adaptive_wfa.py -q
```

Result:

- `30 passed`
- `1 failed`

Failure:

- `tests/test_walkforward.py::test_run_optuna_on_window_multiprocess_uses_in_memory_worker_csv`

Failure reason:

- environment-level Windows multiprocessing permission error:
  - `PermissionError: [WinError 5] Access is denied`

This failure is not caused by the Analytics DB performance update.

To verify the rest of that module:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_walkforward.py -q -k "not multiprocess_uses_in_memory_worker_csv"
```

Result:

- `10 passed`
- `1 deselected`

### Frontend Syntax Validation

Commands:

```powershell
node --check src/ui/static/js/api.js
node --check src/ui/static/js/analytics-table.js
node --check src/ui/static/js/analytics-sets.js
node --check src/ui/static/js/analytics.js
```

Result:

- All passed

### Full Repository Test Run

Command:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q
```

Result:

- `289 passed`
- `8 failed`

All 8 failures were environment-level Windows multiprocessing permission failures and were not caused by the Analytics DB performance update.

Failing tests:

- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_multiprocess_uses_minmax`
- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_single_and_multi_produce_same_scores`
- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_multiprocess_accepts_in_memory_csv_without_journal_files`
- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_multiprocess_worker_failure_raises_runtime_error`
- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_nsga_multiprocess_small_space_stops_without_duplicate_results`
- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_tpe_multiprocess_small_space_stops_without_duplicate_results`
- `tests/test_multiprocess_score.py::TestMultiProcessScore::test_nsga_multiprocess_preserves_coverage_trials_as_generation_zero`
- `tests/test_walkforward.py::test_run_optuna_on_window_multiprocess_uses_in_memory_worker_csv`

Shared failure reason:

- `PermissionError: [WinError 5] Access is denied`
- raised while creating multiprocessing queues / pipes on this Windows environment

## Notable Implementation Details

### Why Not RAM-Load the Entire DB?

That idea was evaluated earlier and rejected as the main fix because the actual hot cost was repeated JSON parsing and Python aggregation, not raw disk reads. The current solution attacks the real bottleneck while keeping memory usage predictable.

### Why Keep the Old Live Aggregation Endpoint?

Because users can still build arbitrary custom portfolios by checking any combination of studies. Those combinations are not stable named entities and should remain dynamic.

The new cache model accelerates:

- study sets,
- all studies,
- repeated focus actions,
- initial Analytics load,

while preserving flexibility for custom ad-hoc analysis.

### Why Store Curves for Sets Too?

Because your requirement explicitly included fast chart opening for focused sets, and the measured storage cost is acceptable relative to the database sizes involved.

This makes set focus nearly instant after the cache exists.

## Known Limitations

1. The persistent cache currently targets named sets and synthetic `All Studies`, not every arbitrary checkbox combination.
2. The frontend has no dedicated browser-level automated tests in this repository, so frontend validation here was done through backend regression tests plus JavaScript syntax checks.
3. One unrelated Windows multiprocessing test still depends on environment permissions and remains outside the scope of this Analytics update.

## Final Assessment

The implemented solution is safe, additive, and aligned with the root cause of the slowdown.

It should materially improve Analytics responsiveness by:

- shrinking summary payload size,
- avoiding repeated set aggregation,
- removing reorder-triggered recomputation,
- serving focused set curves from persistent cache,
- preserving correctness through exact recomputation from source curves when cache is missing or stale.

This is a solid long-term architecture for Analytics performance and should remain maintainable as the number of studies and sets grows.
