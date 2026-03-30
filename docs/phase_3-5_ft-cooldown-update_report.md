# Phase 3.5 FT Cooldown Update Report

## Summary

This update fixes the main Forward Test (FT) logic flaw in Merlin:

- Before the update, when all FT candidates were unprofitable, Merlin could still continue with the "least bad" candidate.
- After the update, FT now applies an explicit threshold gate and can reject all candidates.
- Rejected FT candidates are no longer allowed to continue into Stress Test.
- In Walk-Forward Analysis (WFA), FT rejection can now trigger an intra-window cooldown and re-optimization cycle instead of silently falling back to an upstream candidate.
- If retries still do not produce an acceptable FT candidate, the remaining OOS budget is marked as `no_trade`.

The implementation was done without changing the deferred `profit_degradation` issue for `IS <= 0`, as requested.

## Problem Statement

### Previous behavior

The previous FT flow ranked all FT candidates and selected the top FT result even if:

- every FT candidate was negative;
- no candidate should realistically be deployed;
- Stress Test was enabled and could still evaluate candidates that should already have been rejected by FT.

This created a logical contradiction:

- FT was supposed to validate robustness on a holdout segment;
- but when FT failed for every candidate, the pipeline still selected one of them.

For WFA this was especially problematic, because the engine could continue into OOS trading with a parameter set that had already failed the FT validation stage.

### Why this is dangerous

- It weakens FT as a holdout filter.
- It hides clear "do not trade" signals.
- It allows Stress Test to operate on candidates that should have been vetoed earlier.
- It biases OOS results by continuing when the pre-live validation signal is already negative.

## Final Product Decisions Implemented

The following agreed product decisions were implemented:

1. FT threshold is applied to raw `ft_net_profit_pct`.
2. Threshold is signed and supports both negative and positive values.
3. Candidate passes FT only if:

   `ft_net_profit_pct >= ftThresholdPct`

4. FT-rejected candidates are hard-vetoed for Stress Test.
5. If FT rejects all candidates in WFA:

   - Merlin does not silently fall back to Optuna/DSR;
   - Merlin follows the configured reject policy.

6. Reject policy supports:

   - `No Trade`
   - `Cooldown + Re-optimize`

7. `Cooldown + Re-optimize` works inside the same OOS budget/window:

   - wait flat for `cooldownDays`;
   - shift the full `IS + FT` block forward;
   - retry selection;
   - if a candidate passes, trade only the remaining part of the original OOS budget.

8. FT retry overlap is allowed when cooldown is shorter than the FT period.
9. `maxRetryAttempts` and `minRemainingOosDays` are shared WFA FT-reject controls and work in both Fixed and Adaptive modes.
10. Global IS guard was not added.

## What Changed

## 1. FT Threshold Gate

### Before

- FT returned ranked results.
- The pipeline used the top FT result if FT ran at all.
- No explicit pass/fail gate existed.

### After

- FT results are now annotated with `ft_passes_threshold`.
- Helper functions were added to:
  - normalize FT reject action values;
  - evaluate FT pass/fail against the configured threshold;
  - annotate FT results with pass/fail;
  - filter FT-passing candidates only.

### Result

- FT now produces both ranking and deployment eligibility.
- Legacy persisted FT results without the new flag are still treated as passing for backward-compatible inspection.

## 2. Hard Veto for Stress Test

### Before

- Stress Test could run on raw FT results even if every FT result was below the intended acceptance bar.

### After

- Stress Test only receives FT-passing candidates.
- If FT rejects all candidates, Stress Test is skipped with explicit reason:

  `skipped_upstream_ft_reject`

### Result

- Stress Test cannot "rescue" FT-rejected candidates.

## 3. Plain Optuna/OOS Selection Behavior

### Before

- OOS source selection could still continue past FT by using presence/order of FT results alone.

### After

- OOS source selection now respects FT pass/fail.
- If FT ran and all FT candidates were rejected, source selection returns `forward_test` with an empty candidate set rather than falling back to DSR/Optuna.

### Result

- FT remains a real veto layer in non-WFA flows too.

## 4. WFA Fixed FT Reject Handling

### Before

- Fixed WFA duplicated IS-pipeline logic inline.
- It could implicitly continue with an upstream candidate after FT failure.
- There was no intra-window FT retry policy.

### After

- Fixed WFA now uses a centralized pre-OOS execution-plan resolver.
- When FT rejects all candidates and reject policy is `Cooldown + Re-optimize`:
  - Merlin waits flat for the configured cooldown;
  - shifts the full IS/FT block forward;
  - retries selection;
  - trades only the remaining OOS portion if a valid FT candidate is found.
- If no valid candidate is found in time or retries are exhausted:
  - the remainder of the window is `no_trade`;
  - OOS equity is stored as flat.

### Result

- Fixed WFA window cadence is preserved.
- The engine no longer needs to skip the entire OOS window by default.
- There is no silent fallback after FT rejection.

## 5. WFA Adaptive FT Reject Handling

### Before

- Adaptive WFA entered OOS immediately after IS selection.
- It had no pre-entry FT retry/cooldown behavior.

### After

- Adaptive WFA uses the same FT reject policy resolver as Fixed WFA.
- If FT rejects all candidates:
  - Merlin can delay live entry with cooldown;
  - re-run IS/FT selection inside the same adaptive OOS budget;
  - then trade the remaining adaptive OOS budget only.
- `oos_elapsed_days` now correctly includes pre-entry FT retry delay.
- If no acceptable FT candidate is found, the full adaptive OOS budget is consumed as `no_trade`.

### Result

- Fixed and Adaptive now follow the same FT gating model.
- Adaptive trigger logic still runs only after a valid entry is found.

## 6. No-Trade and Flat OOS Handling

### Before

- There was no explicit "this window intentionally did not trade" state for FT rejection.

### After

- WFA windows now store explicit execution metadata:
  - `trade_start`
  - `trade_end`
  - `entry_delay_days`
  - `ft_retry_attempts_used`
  - `remaining_oos_days_at_entry`
  - `window_status`
  - `no_trade_reason`

- If a window becomes `no_trade`:
  - OOS metrics are computed from a flat synthetic result;
  - stitched OOS remains calendar-consistent;
  - the window no longer impersonates a real traded OOS segment.

### Result

- Flat/no-trade windows are now first-class states instead of implicit fallbacks.

## 7. Storage and Persistence

### Added study-level FT fields

- `ft_threshold_pct`
- `ft_reject_action`
- `ft_reject_cooldown_days`
- `ft_reject_max_attempts`
- `ft_reject_min_remaining_oos_days`

### Added trial-level FT field

- `ft_passes_threshold`

### Added WFA window fields

- `trade_start_date`
- `trade_end_date`
- `trade_start_ts`
- `trade_end_ts`
- `entry_delay_days`
- `ft_retry_attempts_used`
- `remaining_oos_days_at_entry`
- `window_status`
- `no_trade_reason`

### Migration strategy

- Schema creation was updated for new databases.
- Incremental `ALTER TABLE` migration logic was added for existing databases.

### Result

- Existing DBs can evolve without manual intervention.
- New FT gate and WFA retry state are fully persisted.

## 8. UI Changes

New FT settings were added to the FT module settings panel on the main page:

- `FT Threshold (%)`
- `On FT Reject`
- `Cooldown Step (days)`
- `Max Retry Attempts`
- `Min Remaining OOS (days)`

No extra help text, tooltips, section dividers, or layout redesign were added, per request.

### UI behavior

- `FT Threshold (%)` is available whenever FT is enabled.
- FT reject policy fields appear only when:
  - FT is enabled;
  - WFA is enabled.
- Cooldown-specific fields appear only when:
  - `On FT Reject = Cooldown + Re-optimize`

Queue load/apply support was also updated for the new FT fields.

## 9. WFA Export / Results Route Adjustments

Because windows can now have delayed live entry or `no_trade`, the WFA data routes were updated:

- OOS equity export now prepends a flat prefix when live entry was delayed.
- OOS trade export for `no_trade` windows returns an empty trade set.
- WFA window detail payload now exposes the new execution metadata.

This prevents post-update exports from misrepresenting delayed-entry windows as if they had traded from the scheduled OOS start.

## Implementation Notes

## Core design choices

### FT gate vs ranking

FT ranking and FT eligibility are now separate concepts:

- ranking still decides ordering among valid FT candidates;
- threshold decides whether a candidate is even deployable.

### Retry scope

Retry logic was implemented at the WFA window execution level, not inside raw FT itself.

This keeps the behavior explicit:

- FT remains a validator;
- WFA decides what to do when FT rejects all candidates.

### Fixed cadence preservation

For Fixed WFA, the update preserves the outer fixed window cadence by:

- keeping the original OOS budget/calendar window;
- storing a delayed trade start inside it.

### Adaptive consistency

For Adaptive WFA, the same FT retry logic consumes part of the OOS budget before live trading begins, and `oos_elapsed_days` reflects that consumed time.

## Files Changed

- `src/core/post_process.py`
- `src/core/testing.py`
- `src/core/walkforward_engine.py`
- `src/core/storage.py`
- `src/ui/server_routes_run.py`
- `src/ui/server_routes_data.py`
- `src/ui/static/js/post-process-ui.js`
- `src/ui/static/js/queue.js`
- `src/ui/templates/index.html`
- `tests/test_post_process.py`
- `tests/test_oos_selection.py`
- `tests/test_walkforward.py`
- `tests/test_adaptive_wfa.py`
- `tests/test_storage.py`
- `tests/test_server.py`

## Test Results

### Main verification command

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_post_process.py tests\test_oos_selection.py tests\test_walkforward.py -k "not multiprocess" tests\test_adaptive_wfa.py tests\test_storage.py tests\test_server.py -q
```

Result:

- `118 passed`
- `1 deselected`

### Additional targeted runs

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_post_process.py tests\test_oos_selection.py -q
```

Result:

- `9 passed`

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests\test_storage.py tests\test_server.py -q
```

Result:

- `81 passed`

## Errors Encountered During Verification

One pre-existing environment-specific test issue appeared during a broader test run:

```powershell
tests/test_walkforward.py::test_run_optuna_on_window_multiprocess_uses_in_memory_worker_csv
```

Observed error:

- `PermissionError: [WinError 5] Access is denied`

Source:

- Windows multiprocessing queue creation in the current environment during the Optuna multiprocess path.

Interpretation:

- This was not caused by the FT cooldown update.
- The FT/WFA update was verified successfully with the full relevant suite excluding that environment-limited multiprocess test.

## Before vs After

## Before

- FT could reject everything economically but still select a candidate technically.
- Stress Test could receive FT-failed candidates.
- WFA had no clean intra-window FT reject recovery path.
- No-trade windows were not explicit.
- Exported OOS behavior could not represent delayed entry windows.

## After

- FT now has an explicit signed threshold gate.
- FT rejection is a real veto.
- Stress Test only sees FT-passing candidates.
- Fixed and Adaptive WFA both support cooldown + re-optimization inside the same OOS budget.
- Remaining-window `no_trade` is explicit and persisted.
- Delayed-entry windows are represented correctly in storage and exports.

## Future-Proofing Assessment

This update is future-proof enough for the current architecture because:

- FT gating is configuration-driven.
- The threshold supports both negative and positive policies.
- Retry handling is centralized instead of duplicated across flows.
- Storage keeps explicit window execution metadata.
- Fixed and Adaptive share the same FT reject model.
- Export paths understand delayed-entry/no-trade windows.

### Known deferred item

The previously identified `profit_degradation = 0.0 when IS annualized profit <= 0` behavior was intentionally left untouched in this phase.

## Final Outcome

The update fully addresses the main problem:

- Merlin no longer deploys the "least bad" FT candidate when FT rejects the entire candidate set.

It also upgrades the FT module from a ranking-only stage to a true deployment gate, while adding a controlled and configurable recovery path for WFA windows through cooldown and re-optimization.
