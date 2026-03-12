# Phase 2.4 Queue Update Report

## Scope

This update improves the Merlin main page `Run Queue` UI and queue execution flow with three targets:

1. Automatic `Study Set` creation for eligible WFA queue items after item completion.
2. Single-item removal for completed and failed queue history entries.
3. Simple and consistent queue reordering and batch deletion without drag-and-drop.

Reliability, backward compatibility, and safety for the existing `queue.json` history were the main priorities.

## What Was Implemented

### 1. Auto-create Set for completed WFA queue items

Implemented a new queue-item-level metadata block: `studySet`.

Stored metadata includes:

- `autoCreate`
- `completedStudyIds`
- `createdSetId`
- `createdSetName`
- `status`
- `error`
- `lastUpdatedAt`

Key behavior:

- The `Auto-create Set` option is shown only when:
  - WFA is enabled
  - more than 1 CSV is selected
- The option is hidden for Optuna mode and single-CSV runs.
- Default state is `ON` when the option first becomes visible.
- User choice is preserved while editing the current queue item.
- The option is saved inside the queue item itself, not as a global page setting.

Execution logic:

- For each successful WFA source run, the created `study_id` is appended to `studySet.completedStudyIds`.
- Progress is persisted incrementally, so partial completion and page reloads do not lose successful studies.
- After the queue item finishes:
  - if the item is eligible for auto-set creation
  - and at least 2 successful studies were produced
  - Merlin creates a new Analytics `Study Set`
- Set color is created as `lavender`
- Set creation is non-fatal:
  - queue item completion remains valid
  - set creation failure is stored in queue metadata as warning state
  - warning count is shown in queue summary text
- If fewer than 2 successful studies exist, set creation is skipped and this is recorded in queue metadata.

Auto-generated set name format:

`#73 · S03 · 30m · NSGA-2 (357) · 1.5k · WFA-F 60/30`

Included fields:

- queue item index
- short strategy label
- timeframe of the first CSV
- sampler name
- initial trials count
- compact budget label
- WFA mode and IS/OOS periods

### 2. Removal of completed queue items one by one

The existing per-item `x` remove control is now available for:

- completed items
- failed items

It remains unavailable for:

- running items

This allows history cleanup without clearing the full queue or touching pending items.

### 3. Batch actions and keyboard-based queue move

Added queue toolbar controls:

- `Batch`
- `Move`

Final queue toolbar order:

- `+ Add to Queue`
- `Load Queue`
- `Clear`
- `Batch`
- `Move`

Implemented behavior:

- `Batch` toggles batch selection mode.
- In batch mode:
  - click selects one item
  - `Ctrl/Cmd+Click` toggles selection
  - `Shift+Click` selects range
- While batch mode is active, `Clear` changes to `Delete`
- `Delete` removes all selected queue items
- `Move` works for pending items only
- Reordering is keyboard-driven:
  - `Move`
  - `ArrowUp` / `ArrowDown`
  - `Enter` to save order
  - `Esc` to cancel

The implementation reorders only pending queue items while preserving completed/failed history placement and item identity data.

## UI Changes

Updated main page queue UI:

- added `Batch` and `Move` buttons
- added a second queue row under the toolbar for `Auto-create Set`
- added a move-mode hint row text: `Move mode: Enter save, Esc cancel`
- added focused, batch-selected, and moving queue row visual states

The `Auto-create Set` control is left-aligned and visually associated with queue actions, but kept on a separate row to avoid overcrowding the toolbar.

## Safety and Backward Compatibility

`queue.json` safety was treated as a hard requirement.

Implemented safeguards:

- Queue item normalization keeps arbitrary extra fields.
- New metadata is additive and backward-compatible.
- Existing completed queue history remains valid.
- Queue order changes use array order, not destructive renumbering.
- Existing queue item ids and indexes are preserved.
- Queue metadata for auto-created sets survives persistence roundtrips.

To verify this explicitly, a new server test was added for extended queue item metadata roundtrip and on-disk persistence.

## Files Updated

- `src/ui/templates/index.html`
- `src/ui/static/css/style.css`
- `src/ui/static/js/queue.js`
- `src/ui/static/js/api.js`
- `src/ui/static/js/main.js`
- `src/ui/static/js/ui-handlers.js`
- `tests/test_server.py`

## Key Logic Notes

### Queue study set metadata

Queue items now persist study-set execution state directly in queue storage. This makes the feature resilient to:

- partial queue completion
- browser reloads
- mixed success/failure items

### Set creation timing

Study sets are created only after an item finishes processing, never before. This avoids incomplete sets and prevents saving transient intermediate state as final research groups.

### Non-fatal warning model

If optimization/WFA succeeds but set creation fails, the queue item is still completed successfully. The set failure is stored separately as metadata and surfaced as warning.

### Consistency with Analytics page

The queue move/delete interaction model follows the existing Analytics multi-item pattern instead of introducing a queue-specific custom mode.

## Verification

### Static checks

JavaScript syntax checks passed:

- `node --check src/ui/static/js/queue.js`
- `node --check src/ui/static/js/main.js`
- `node --check src/ui/static/js/ui-handlers.js`
- `node --check src/ui/static/js/api.js`

### Python tests

All tests were run with the required interpreter:

`C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Targeted verification:

- `-m pytest tests/test_server.py -k "queue or analytics_sets" -q` -> `12 passed`
- `-m pytest tests/test_storage.py -k "study_sets" -q` -> `5 passed`

Full relevant suites:

- `-m pytest tests/test_server.py -q` -> `53 passed`
- `-m pytest tests/test_storage.py -q` -> `14 passed`

## Problems Solved

This update solves the following workflow issues:

- removes the need to manually create Analytics study sets after multi-CSV WFA queue runs
- makes newly generated research groups easy to find via `lavender` set color
- allows cleanup of completed/failed queue history without clearing everything
- adds simple reorder support without drag-and-drop complexity
- keeps queue interactions consistent with the rest of Merlin UI
- preserves existing `queue.json` history and metadata safely

## Follow-up UI Fixes

After the first implementation pass, three additional frontend fixes were applied:

- `Auto-create Set` now defaults correctly for eligible multi-CSV WFA form states after loading legacy/history queue items, by distinguishing explicitly configured queue items from old items without saved auto-set metadata.
- Queue toolbar buttons were reduced in size so the full `Add / Load / Clear / Batch / Move` row fits more reliably.
- Batch `Shift+Click` no longer causes transient text selection on queue item labels.

Final follow-up fixes:

- `Batch` mode no longer exits immediately when selection becomes empty; this allows `Esc -> Batch -> select items` to work correctly and keeps `Batch` logically independent from focus.
- `Auto-create Set` now uses sticky user preference for the current form state:
  - eligible state (`WFA + 2+ CSV`) restores the last explicit user choice
  - `WFA + 0/1 CSV` shows the option as disabled/greyed and unchecked
  - switching back to eligible state restores the saved preference instead of forcing `ON`
- Analytics study set rename now uses the same duplicate-name auto-suffix logic as set creation, so renaming to an existing name resolves to `Name (1)`, `Name (2)`, and so on instead of returning an error.

## Errors / Issues During Verification

Final implementation status:

- no JavaScript syntax errors detected
- no backend test failures detected
- no storage test failures detected

No runtime or test errors remained after final verification.
