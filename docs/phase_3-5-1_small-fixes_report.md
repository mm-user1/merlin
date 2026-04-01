# Phase 3.5.1 Small Fixes Report

## Overview

This update focused on three related problems discovered after the FT threshold / cooldown-retry rollout:

1. WFA Results did not visually indicate when Forward Test retry logic intervened or when an OOS window was skipped because FT failed all allowed attempts.
2. Results and Analytics sidebars did not show Post Process settings, which made studies harder to interpret and reproduce.
3. WFA studies did not fully persist DSR / Stress Test study-level metadata into the `studies` table, even though the runtime logic itself worked correctly.

The goal of this patch was to improve transparency, consistency, and robustness without changing the already-approved FT retry algorithm itself.

## Problems Before the Update

### 1. Missing FT retry / skip visibility in Results

After the previous FT cooldown-reoptimize update, the engine correctly stored:

- `ft_retry_attempts_used`
- `window_status`
- `no_trade_reason`
- `entry_delay_days`

However, the Results UI did not render any explicit FT status marker in the WFA window header. As a result:

- a skipped FT window was only visible indirectly via a flat stitched OOS segment and `0` trades,
- a successful FT retry was also mostly invisible unless the user manually inspected the detailed rows,
- FT retry behavior was therefore operationally correct but visually under-explained.

### 2. Missing Post Process configuration in Results / Analytics sidebars

The sidebar showed `Optuna Settings` and `WFA Settings`, but not the Post Process layer. That meant the following critical configuration was not visible in the focus panel:

- FT period / topK / sort metric / threshold
- FT reject policy for WFA
- DSR enablement and topK
- Stress Test enablement and settings

This made it harder to verify how a study was actually filtered after optimization.

### 3. Incomplete WFA study metadata persistence

The WFA save path correctly persisted FT study-level settings, but did not persist DSR / Stress Test settings into the `studies` row. This produced inconsistencies such as:

- runtime windows and trials showing Stress Test was used,
- `config_json.postProcess` showing Stress Test enabled,
- but `studies.st_enabled`, `st_top_k`, `st_failure_threshold`, `st_sort_metric` remaining empty or default.

The same asymmetry applied to WFA DSR settings as well.

## Final Solution

## 1. FT retry badges in WFA Results

The WFA window header now renders FT badges in the same visual badge system already used for Adaptive WFA trigger badges.

Implemented behavior:

- If FT retry logic intervened and the window ultimately traded, the header shows a green badge: `FT N`
- If FT logic ended with `no_trade`, the header shows a red badge: `FT N`
- `N` is the total number of FT attempts:
  - first attempt counts as `1`
  - `ft_retry_attempts_used` is converted to `1 + retries`
- If Adaptive WFA badges are also present (`CUSUM`, `DD`, `INACTIVE`, `MAX`), the FT badge is rendered alongside them

Important intentional scope decisions:

- Delay days are not shown in the badge
- No extra explanatory text is added to the header
- A window that passed FT immediately does not get an FT badge

This keeps the UI compact while making FT intervention visible at a glance.

## 2. New `Post Process Settings` sidebar block

A new `Post Process Settings` block was added to both:

- Results page
- Analytics page

Placement:

- after `Optuna Settings`
- before `WFA Settings`

Display rules:

- show `Forward Test` row only if FT was enabled
- show `FT Reject Policy` row only if FT was enabled and the study is WFA
- show `DSR` row only if DSR was enabled
- show `Stress Test` row only if Stress Test was enabled
- hide the whole block if no Post Process module was enabled

Source of truth:

- the block is built from `config_json.postProcess`
- not from study-level FT / DSR / ST columns

This was done intentionally because:

- it matches the product decision discussed before implementation,
- it is more robust for older studies,
- it avoids relying on incomplete WFA metadata for display.

Displayed rows are concise but complete enough for reproduction:

- `Forward Test`: period, topK, sort metric, threshold
- `FT Reject Policy`: action and retry parameters
- `DSR`: topK
- `Stress Test`: topK, failure threshold, sort metric

## 3. WFA persistence fix for DSR / Stress Test metadata

`save_wfa_study_to_db()` was extended to persist WFA-level study metadata for:

- `dsr_enabled`
- `dsr_top_k`
- `st_enabled`
- `st_top_k`
- `st_failure_threshold`
- `st_sort_metric`

This aligns WFA study persistence with the already-existing Optuna-only persistence path and removes the earlier inconsistency where WFA execution used DSR / Stress Test correctly but the `studies` summary row did not fully reflect that.

## 4. Compatibility / robustness improvement for older WFA studies

In Results page state loading, Post Process enablement now falls back to `config_json.postProcess` when study-level metadata is missing or incomplete.

This means older WFA studies affected by the previous persistence bug can now still:

- expose the correct Post Process sidebar block,
- enable DSR / Stress Test result tabs more reliably,
- behave more consistently during inspection without requiring a rerun.

This was a deliberate compatibility improvement and makes the patch more future-proof.

## Files Changed

### Persistence / API

- `src/core/storage.py`
- `src/ui/server_routes_analytics.py`

### Results / Analytics UI

- `src/ui/templates/results.html`
- `src/ui/templates/analytics.html`
- `src/ui/static/js/results-state.js`
- `src/ui/static/js/results-controller.js`
- `src/ui/static/js/results-tables.js`
- `src/ui/static/js/analytics.js`
- `src/ui/static/js/wfa-results-ui.js`
- `src/ui/static/css/style.css`

### Tests

- `tests/test_storage.py`
- `tests/test_server.py`

## Before vs After

### WFA window visibility

Before:

- FT retry / skip behavior existed in the engine but was not clearly visible in Results

After:

- FT intervention is visible directly in the WFA window header through `FT N` badges

### Sidebar visibility

Before:

- Optuna and WFA settings were visible
- Post Process settings were hidden

After:

- Results and Analytics both show a dedicated `Post Process Settings` block when relevant

### WFA metadata consistency

Before:

- WFA FT metadata persisted
- WFA DSR / ST metadata did not fully persist into `studies`

After:

- WFA FT / DSR / ST study-level settings are persisted consistently

## Testing

### Main green verification command

Final full verification was completed outside the sandbox so the multiprocessing WFA test path could run normally:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_storage.py tests\test_server.py tests\test_walkforward.py tests\test_adaptive_wfa.py -q
```

Result:

- `107 passed`
- `3 warnings`

Warnings observed:

- Optuna experimental warnings for `multivariate`
- Optuna experimental warnings for `group`
- Optuna experimental warnings for `constant_liar`

### Additional in-sandbox verification command

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_storage.py tests\test_server.py tests\test_walkforward.py -k "not multiprocess" tests\test_adaptive_wfa.py -q
```

Result:

- `106 passed, 1 deselected`

### Additional targeted run

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_storage.py tests\test_server.py -q
```

Result:

- `83 passed`

## Errors Encountered During Verification

During the first in-sandbox verification pass, one unrelated environment-specific test failed when running the full WFA suite with multiprocessing enabled:

```text
tests/test_walkforward.py::test_run_optuna_on_window_multiprocess_uses_in_memory_worker_csv
PermissionError: [WinError 5] Access is denied
```

Cause:

- Windows multiprocessing pipe / queue creation in the current environment

Why this is not attributed to this patch:

- the failure happens in the existing multiprocessing test path,
- the patch does not modify multiprocessing queue setup,
- the remaining WFA / Adaptive / storage / server test coverage passed cleanly.

After rerunning the suite outside the sandbox, the multiprocessing path passed successfully and the full relevant suite finished green.

## Design Notes

### Why Post Process sidebar uses `config_json.postProcess`

This was chosen because it is the most stable representation of what the user configured. It also protects the UI from partial study-level metadata in older databases.

### Why FT badge is intentionally minimal

The badge exists to answer a fast operational question:

- did FT intervene here?
- how many attempts were needed?
- was the outcome tradeable or skipped?

Adding delay-days text at this stage would increase noise without enough benefit.

### Why the Results loader also uses config fallback

Fixing persistence helps future studies, but old studies already stored in DB should also display sensibly. The fallback path gives better backward compatibility without changing stored data.

## Outcome

This patch does not alter the FT retry decision logic itself. Instead, it closes the visibility and metadata gaps around that logic.

The update is now better in four ways:

- clearer: FT intervention is visible in Results
- more inspectable: Post Process settings are visible in Results and Analytics
- more consistent: WFA DSR / ST settings now persist correctly
- more backward-friendly: older studies can still display correctly via config fallback

Overall, the update is safe, targeted, and future-proof for the current Post Process architecture.

## Follow-Up UI Compaction Update

After the main small-fixes patch, a follow-up UI-only refinement was applied to make the `Post Process Settings` sidebar block denser and easier to scan in the narrow sidebar area.

Scope of this follow-up:

- Results page sidebar only
- Analytics page sidebar only
- no runtime logic changes
- no storage changes
- no API changes
- no changes to any other UI areas outside this sidebar block

### Compact display changes

The following compact labels are now used only inside `Post Process Settings`:

- `Profit Degradation` -> `Profit Deg`
- `Profit Retention` -> `Profit Ret`
- `RoMaD Retention` -> `RoMaD Ret`
- `Cooldown + Re-optimize` -> `CD + Re-opt`

The FT reject policy display was also compacted:

- `Cooldown 5 days` style display -> `CD 5d`
- `2 retries` style display -> `Retry 2`

The layout was also refined so FT reject policy no longer appears as a separate sidebar row. Instead, all FT-related settings are rendered inside the single `Forward Test` row in the same general order as on the Start page:

- FT period
- FT topK
- FT sort metric
- FT threshold
- FT reject policy
- FT cooldown
- FT retry count
- FT min remaining OOS

Example of the compact `Forward Test` rendering:

```text
7d, Top 5, Sort: Profit Deg, Threshold: +4.0%, Policy: Cooldown + Re-optimize, CD 5d, Retry 2, Min OOS 10d
```

This keeps the meaning intact while fitting noticeably more information into the small sidebar width and avoids making FT reject policy look like a separate post-process module.

### Safety note

These compact labels were implemented as sidebar-local formatting helpers. They were intentionally not applied to:

- internal config keys
- stored values
- API payloads
- other UI tables
- other pages

This keeps the change low-risk and fully limited to the requested display surface.

### Follow-up verification

Because this was a UI-string-only refinement, there are still no dedicated frontend tests for this sidebar renderer in the project. A fast regression check was still run on the server-side suite:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_server.py -q
```

Result:

- `63 passed`

## Stitched OOS Consistency Update

After the FT / Post Process visibility work, another important analytics gap remained around the `Consistency (R²)` metric.

### Problem

The project already had consistency support in one specific area:

- `Study Sets` on the Analytics page displayed `Recent / Full` consistency for aggregated portfolio curves

However, the same metric was still missing in the most practical per-study locations:

- `Analytics -> Summary Table`
- `Results -> Stitched OOS` row for WFA studies
- persisted WFA study-level stitched metrics in the database

This created three user-facing problems:

1. Analytics Summary Table could not show `Consistency` for each ticker/study, even though the stitched OOS curve already existed.
2. The `Consist` column in the Results WFA stitched row always showed `N/A`.
3. Older WFA studies had stitched equity data saved, but no persisted study-level consistency values, so the system had no durable way to reuse them.

### Design Decision

The scope was intentionally limited to stitched WFA `Consistency` only.

I did **not** expand this patch to compute and persist every advanced stitched-row metric such as:

- `MaxCL`
- `Score`
- `Sharpe`
- `PF`
- `Ulcer`
- `SQN`

Reason:

- several of those metrics are trade-based rather than pure equity-curve metrics
- some require clearer stitched semantics before persisting them at study level
- adding them all in one patch would increase risk and complexity without solving the most valuable gap first

This update therefore focused on the safest high-value slice:

- persist stitched `Consistency`
- reuse it in Analytics Summary
- reuse it in Results stitched row
- backfill legacy WFA studies from already saved stitched equity curves

### What Was Implemented

#### 1. New persisted stitched consistency fields in `studies`

Two new study-level columns were added to the `studies` table:

- `stitched_oos_consistency_full`
- `stitched_oos_consistency_recent`

They are now part of:

- initial schema creation
- schema migration / ensure logic for older databases

This makes stitched consistency a first-class persisted WFA study metric rather than a UI-only recomputation.

#### 2. Consistency is now calculated during WFA study save

When `save_wfa_study_to_db()` persists a WFA result, it now computes stitched consistency directly from:

- `wf_result.stitched_oos.equity_curve`
- `wf_result.stitched_oos.timestamps`

The calculation reuses the existing consistency helper already used by Analytics caches.

Behavior:

- `full` consistency = signed R² on the full stitched OOS equity curve
- `recent` consistency = signed R² on the last `1/4` of the stitched OOS time span
- insufficient data still produces `None`, matching existing project semantics

This means newly completed WFA studies persist consistency immediately, without waiting for any later cache build.

#### 3. Legacy WFA studies are backfilled safely

The existing stitched metadata backfill was extended so it now also backfills:

- `stitched_oos_consistency_full`
- `stitched_oos_consistency_recent`

from already stored:

- `stitched_oos_equity_curve`
- `stitched_oos_timestamps_json`

This backfill now handles both:

- old studies opened through Analytics
- old studies opened directly through Results / `load_study_from_db()`

Important detail:

- backfill only targets WFA studies that actually have saved stitched curve/timestamp payloads
- this avoids repeatedly touching rows that cannot produce stitched metrics

#### 4. Analytics Summary payload now includes per-study consistency

The `/api/analytics/summary` payload now exposes:

- `consistency_full`
- `consistency_recent`

for each WFA study row.

This makes the per-study Summary Table capable of displaying and sorting by stitched consistency without any extra client-side data fetches.

#### 5. Analytics Summary Table now shows `Consist`

The Summary Table received a new last column:

- `Consist`

Display format:

- `recent/full`

Example:

```text
0.17/0.89
```

This matches the existing `Study Sets` convention and keeps the Analytics page internally consistent.

Sorting logic was implemented to match `Study Sets` exactly:

- first compare `recent`
- if tied, compare `full`

This preserves the project’s current interpretation of consistency quality.

#### 6. Results stitched row now shows real consistency instead of `N/A`

The WFA `Stitched OOS` row on the Results page previously hardcoded:

- `consistency_score = null`

That is why `Consist` always displayed `N/A`.

Now the stitched row uses persisted stitched consistency:

- `full` consistency only

I intentionally kept the Results stitched row on `full` only, not `recent/full`, because:

- the surrounding `Consist` cells in Results already use a single value
- mixing pair-format and single-value format inside the same Results table column would hurt readability

So the final behavior is:

- `Analytics Summary Table` -> `recent/full`
- `Results stitched row` -> `full`

### Why This Is Safe

This change is low-risk for several reasons:

- it reuses the existing consistency implementation already trusted by Analytics caches
- it does not change WFA selection logic
- it does not alter stitched PnL, WFE, OOS wins, or any trading behavior
- it adds only additive schema changes
- it preserves backward compatibility through backfill

The patch improves observability, not trading decisions.

### Files Updated

- `src/core/storage.py`
  schema, WFA save-path, stitched backfill, legacy study load backfill
- `src/ui/server_routes_analytics.py`
  added per-study stitched consistency to Analytics Summary payload
- `src/ui/templates/analytics.html`
  added `Consist` column to Summary Table
- `src/ui/static/js/analytics-table.js`
  render + sort support for per-study consistency pair
- `src/ui/static/js/results-controller.js`
  expose stitched consistency to Results state
- `src/ui/static/js/wfa-results-ui.js`
  render stitched `full` consistency in `Stitched OOS` row
- `tests/test_storage.py`
  schema, save-path, and legacy backfill coverage
- `tests/test_server.py`
  Analytics Summary payload coverage

### Tests Added / Extended

Added or expanded checks for:

- new `studies` columns for stitched consistency
- stitched consistency persistence for new WFA studies
- stitched consistency backfill for legacy WFA rows
- per-study `consistency_full/recent` in `/api/analytics/summary`
- Analytics Summary contract still behaving correctly for partial / short curves

### Verification

Targeted storage and server suite:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_storage.py tests\test_server.py -q
```

Result:

- `86 passed`

Extended WFA-related suite:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_walkforward.py tests\test_adaptive_wfa.py tests\test_storage.py tests\test_server.py -q
```

First run inside sandbox hit the already known Windows multiprocessing restriction:

- `PermissionError: [WinError 5] Access is denied`

That failure was environmental, not caused by this patch.

The same suite was then re-run outside the sandbox:

Result:

- `110 passed, 3 warnings`

Warnings:

- Optuna experimental warnings for multiprocessing TPE flags

These warnings are pre-existing and unrelated to this update.

### Final Outcome

The project now has a coherent stitched consistency story for WFA studies:

- new WFA studies persist stitched consistency immediately
- old WFA studies backfill it safely from saved stitched curves
- Analytics Summary Table exposes it per study as `recent/full`
- Results stitched row finally shows a real `Consist` value instead of `N/A`

This completes the requested stitched `Consistency` visibility gap without expanding into higher-risk stitched advanced-metric persistence.

## Analytics Focus-to-Set Highlight Update

Another Analytics usability gap was then addressed.

### Problem

When a study was focused in the `Summary Table`, there was no quick way to see:

- which `Study Sets` already contained that focused study
- how many such sets existed

This forced the user to inspect set contents manually, which is slow and error-prone in larger research databases.

### Implemented behavior

The Analytics `Study Sets` panel now reacts to the currently focused study from the `Summary Table`.

#### 1. Matching sets are highlighted

If a study is focused in the Summary Table:

- every visible set containing that `study_id` is highlighted
- multiple matching sets are highlighted at once if applicable

The highlight uses the same light-yellow visual language as `Move`, but it is implemented as a separate semantic state rather than reusing move-mode logic directly.

This preserves clarity:

- yellow still means “pay attention to this row”
- but the code still distinguishes:
  - set is being moved
  - set contains the focused study

#### 2. Highlight priority is controlled

The new highlight intentionally has lower priority than existing stronger set-row states:

- `Focused Set`
- `Batch Selected`
- `Move Mode`

So if a set both contains the focused study and is already in one of those stronger states, the stronger state remains visible.

This avoids visual collisions and keeps existing workflows predictable.

#### 3. Study Sets summary now reports membership count

The small summary area in the `Study Sets` header now also shows membership context for the focused study.

Examples:

```text
Focused study is in 3 sets
Focused study is in 2/3 sets
Focused: FT Candidates (12) | Focused study is in 3 sets
Batch: 4 selected | Focused study is in 2/3 sets
```

Interpretation:

- `3 sets` = all matching sets are currently visible
- `2/3 sets` = the focused study belongs to 3 sets total, but only 2 are currently visible after current set filters/sort visibility

This is more informative than a plain count and works well with existing set filters.

### Why this implementation was chosen

The implementation intentionally reuses existing Analytics state instead of inventing a parallel source of truth.

The focused study already exists in Analytics page state, so the safest design was:

- keep focused-study ownership in the Analytics page controller
- pass the current `focusedStudyId` into the `AnalyticsSets` module
- compute matching membership locally from `set.study_ids`

This keeps the feature:

- local
- cheap
- deterministic
- independent from backend changes

No API or database changes were required.

### Files updated

- `src/ui/static/js/analytics-sets.js`
  added focused-study tracking, membership summary text, and row highlight logic
- `src/ui/static/js/analytics.js`
  synchronized the currently focused study into the `AnalyticsSets` module
- `src/ui/static/css/style.css`
  added dedicated `analytics-set-study-match` styling

### Safety and scope

This was a UI-only enhancement:

- no storage changes
- no API changes
- no analytics math changes
- no effect on selection, sets persistence, or move behavior

The feature is therefore low-risk and future-proof for the current Analytics architecture.

### Verification

JavaScript syntax checks:

```powershell
node --check C:\Users\mt\Desktop\Strategy\S_Python\+Merlin\+Merlin-GH\src\ui\static\js\analytics-sets.js
node --check C:\Users\mt\Desktop\Strategy\S_Python\+Merlin\+Merlin-GH\src\ui\static\js\analytics.js
```

Result:

- both files passed syntax check

Server regression check:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_server.py -q
```

Result:

- `64 passed`

### Follow-up refinement: move membership count to `Summary Table`

After the first version of the feature, the focused-study membership text was shown in the `Study Sets` header.

That worked technically, but the user correctly pointed out that the information belongs more naturally to the source interaction:

- the study is focused in `Summary Table`
- therefore the count of matching sets is more logical in the `Summary Table` header

The UX was refined accordingly:

- `Study Sets` now keeps only the yellow row highlight for matching sets
- the count moved to the `Summary Table` header meta area

New compact display:

- `1 set`
- `2 sets`
- `2/3 sets`

This is now displayed next to the existing header counters:

- before `checked`
- `checked`
- `shown`
- `total`

This layout is clearer because:

- the count is now attached to the focused study itself
- the set list remains focused on showing *which* sets match
- no duplicate membership text is shown in two different places

### Verification after the refinement

```powershell
node --check C:\Users\mt\Desktop\Strategy\S_Python\+Merlin\+Merlin-GH\src\ui\static\js\analytics-sets.js
node --check C:\Users\mt\Desktop\Strategy\S_Python\+Merlin\+Merlin-GH\src\ui\static\js\analytics.js
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_server.py -q
```

Result:

- both JS files passed syntax check
- `64 passed`

## Analytics Sidebar Root Cause Fix

After the compaction change, a discrepancy remained:

- `Results` showed the new compact FT line with embedded policy details
- `Analytics` still showed only the shortened FT line without policy details

### Root cause

This was not caused by browser cache.

The actual problem was in the backend analytics summary SQL query:

- the response payload included `post_process_settings`
- but the SQL query did not select `optimization_mode`

As a result, the frontend received:

- correct FT settings
- but `study.optimization_mode = null`

The Analytics sidebar intentionally only appends FT policy details when the focused study is recognized as WFA. Because `optimization_mode` was missing, the frontend treated the study as non-WFA and skipped:

- `Policy: ...`
- `CD ...`
- `Retry ...`
- `Min OOS ...`

### Fix

`optimization_mode` was added to the `/api/analytics/summary` SQL `SELECT`, and the existing payload field now receives a real value instead of `null`.

An additional regression assertion was added to the analytics summary test to ensure focused WFA studies keep returning:

- `optimization_mode == "wfa"`

### Verification

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_server.py -q
```

Result:

- `63 passed`
