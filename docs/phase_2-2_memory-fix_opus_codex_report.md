# Phase 2.2 Memory Fix: Implementation Report

## Summary

This update replaces Merlin's multiprocess Optuna filesystem journal with a RAM-only backend and removes the temporary WFA CSV disk materialization path.

Primary result:

- multiprocess Optuna no longer creates `.journal.log` or `.lock` files
- WFA window CSV copies are no longer written to temp storage for worker processes
- the deprecated `optuna_study.db` feature is fully disabled

This directly removes the high-frequency small-write pattern that was causing excessive SSD churn and filesystem-event pressure in the project/Desktop tree.

## Target And Outcome

### Target

Eliminate the major runtime write storm caused by Optuna multiprocess storage and WFA worker CSV materialization, while preserving Merlin's normal finished-study persistence and keeping WFA regeneration from original CSV paths correct.

### Outcome

Implemented successfully.

The update now uses:

- multiprocess Optuna runtime storage in RAM via `JournalStorage(InMemoryJournalBackend(...))`
- WFA worker CSV transport in RAM via serialized text/bytes payloads reconstructed inside worker processes
- fail-fast worker shutdown and explicit study failure when a worker exits non-zero

The update does **not** attempt to preserve unfinished raw Optuna runtime state after a crash, which matches the agreed product behavior.

## What Was Changed

### 1. Multiprocess Optuna journal moved from filesystem to RAM

File:

- `src/core/optuna_engine.py`

Implemented:

- new `InMemoryJournalBackend(BaseJournalBackend)`
- shared process-safe log transport using `multiprocessing.Manager().list()`
- worker-side `JournalStorage` reconstruction from the shared log list

Important design choice:

- no `Manager.Lock()` was added

Reason:

- `Manager.Lock()` can remain held if a worker dies after acquire and before release
- plain proxy calls on `Manager().list()` avoid introducing a separate lock ownership failure mode

### 2. WFA temp CSV writes removed

Files:

- `src/core/optuna_engine.py`
- `src/core/walkforward_engine.py`

Implemented:

- removed the old temp-file materialization path for file-like CSV sources
- added `_serialize_csv_source_for_worker(...)`
- added `_restore_csv_source_from_worker(...)`
- WFA window data is now passed to workers as RAM payload instead of being copied to temp CSV on disk

Important behavior:

- this affects only runtime worker input transport
- Merlin still preserves the real original absolute `csv_file_path` for stored studies
- later on-demand WFA equity/trade regeneration continues to use the persisted original CSV path on disk

### 3. `optuna_study.db` feature removed

Files:

- `src/core/optuna_engine.py`
- `src/core/walkforward_engine.py`
- `src/ui/server_services.py`
- `src/ui/server_routes_run.py`
- `src/ui/templates/index.html`
- `src/ui/static/js/ui-handlers.js`
- `src/ui/static/js/queue.js`

Implemented:

- removed UI checkbox for `Save study database (optuna_study.db)`
- stopped emitting `optuna_save_study` from frontend payloads
- stopped forwarding `save_study` through WFA route setup
- single-process optimizer no longer creates `RDBStorage(sqlite:///optuna_study.db)`
- optimizer now force-disables legacy `save_study=True` requests
- legacy payloads containing `optuna_save_study` are tolerated but ignored with a warning

### 4. Worker failure handling hardened

File:

- `src/core/optuna_engine.py`

Implemented:

- best-effort worker termination helper
- fail-fast multiprocess monitoring loop instead of simple sequential blocking joins
- worker error queue for diagnostic details
- explicit `RuntimeError` when any worker exits non-zero
- guaranteed `manager.shutdown()` in `finally`, including worker-failure paths

This prevents silent partial-study acceptance and fixes a cleanup hole in the initial draft implementation.

## Problems Solved

### Solved

- Optuna multiprocess journal file churn removed
- journal lock file churn removed
- WFA worker temp CSV writes removed
- `optuna_study.db` creation removed
- worker crashes now fail the study instead of allowing partial-success ambiguity

### Preserved Correctly

- finished Merlin study persistence into Merlin DB
- single-process optimization behavior, except raw Optuna persistence is intentionally removed
- WFA later regeneration from original absolute CSV path
- legacy queue/config payload compatibility for stale `optuna_save_study` values

## Key Logic

### Runtime storage model

For `worker_processes > 1`:

- main process creates one `Manager()` and one shared log list
- main process creates Optuna study on top of RAM journal storage
- each worker rebuilds `JournalStorage` from the same shared RAM log list
- all Optuna storage mutations stay in RAM for the life of that optimization run

For `worker_processes == 1`:

- no filesystem journal is used
- no `optuna_study.db` is created

### WFA CSV transport model

- WFA window dataframe is converted to CSV text in memory
- parent serializes that text/bytes payload
- worker reconstructs `StringIO` or `BytesIO`
- worker loads data directly from the in-memory stream

This removes the temporary CSV write path without changing the persisted source CSV path used later for regeneration.

## Safety And Robustness Notes

- The backend is intentionally disposable runtime IPC, not crash-recovery storage.
- No separate manager lock is used, avoiding stale-lock deadlock risk on worker termination.
- Worker failure now terminates remaining workers and raises clearly.
- Manager cleanup now runs even when study execution fails before result finalization.

## What This Update Does Not Change

This update removes the dominant Optuna journal/temp-CSV write storm, but it does not eliminate all disk writes in Merlin.

Remaining normal writes still include:

- Merlin DB writes for saved studies/results
- queue persistence writes
- other normal application writes unrelated to the Optuna journal backend

Those writes are much smaller in volume and were not the target of this phase.

## Tests Added Or Updated

### Added / extended automated coverage

- `tests/test_multiprocess_score.py`
  - multiprocess optimization with in-memory CSV input
  - no journal files created
  - single-process `save_study=True` does not create `optuna_study.db`
  - worker setup failure propagates as `RuntimeError`

- `tests/test_walkforward.py`
  - multiprocess WFA window optimization using in-memory worker CSV transport
  - no journal files created

- `tests/test_server.py`
  - legacy `optuna_save_study=True` payload is ignored and warns

## Test Results

### Targeted verification

Command:

- `pytest tests/test_multiprocess_score.py tests/test_walkforward.py tests/test_server.py -q`

Result:

- `62 passed`

### Full regression suite

Command:

- `pytest tests -q`

Result:

- `247 passed`

Warnings observed:

- Optuna experimental warnings for sampler features already used by the project
- no functional test failures

## Errors Encountered During Validation

One environment-specific issue occurred during validation:

- running multiprocess tests inside the restricted sandbox failed with Windows `PermissionError` on `multiprocessing.Manager()` / temp access

Resolution:

- reran the multiprocess test suite outside the sandbox
- the implementation itself passed all targeted tests and the full suite in the unrestricted environment

No code-level defects remained after the final verification pass.

## Final Assessment

This update achieves the agreed target.

It is:

- effective: removes the Optuna journal and WFA temp CSV disk write storm
- accurate: preserves Merlin's intended persisted-study behavior and WFA regeneration logic
- safe: rejects partial worker failure as a successful run and cleans up manager resources correctly
- future-proof enough for the agreed scope: raw Optuna runtime persistence is explicitly deprecated and no longer part of the supported product behavior

For the agreed problem statement, this implementation fully completes the required fix.
