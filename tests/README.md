# Tests

Run from the repository root using the configured project interpreter. On Windows:

```powershell
$py = 'C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe'
# Required when the default sibling directory is inside an enclosing Git checkout:
$env:MERLIN_TEST_ROOT = Join-Path $env:LOCALAPPDATA 'Temp\merlin-tests'
& $py tools/run_tests.py fast
& $py tools/run_tests.py full -- --durations=30
& $py tools/run_tests.py -- tests/test_metrics.py
& $py tools/run_tests.py -- tests/test_metrics.py::TestMetricsEdgeCases
& $py tools/run_tests.py fast -- -k calendar
& $py tools/run_tests.py full --keep-temp -- --durations=20
& $py tools/run_tests.py -- --collect-only
```

On Linux, activate the configured environment and use the same arguments:

```bash
export MERLIN_TEST_ROOT="${TMPDIR:-/tmp}/merlin-tests"
python tools/run_tests.py fast
python tools/run_tests.py full -- --durations=30
python tools/run_tests.py -- tests/v2
```

## Selection and dependencies

Fast adds `-m "not slow"`. Full includes every normally discovered test with no
cost filter. Focused mode (no mode before `--`) adds no selector. Pytest owns
discovery through `testpaths = tests`; new ordinary tests enter full automatically.
The launcher prints its command. A full command with file targets, `-k`,
`--deselect`, or other filters is useful but is not an unfiltered acceptance gate.
Reserve `-m` for named modes; use focused mode for custom markers:

```powershell
& $py tools/run_tests.py -- -m regression
& $py tools/run_tests.py -- tests/strategy_lab
$jsTests = @(Get-ChildItem tests -Filter 'test_js_*.py' -File |
    Sort-Object Name | Select-Object -ExpandProperty FullName)
if ($jsTests.Count -eq 0) { throw 'No JavaScript pytest wrappers found.' }
& $py tools/run_tests.py -- @jsTests
if ($LASTEXITCODE -ne 0) { throw 'JavaScript wrapper tests failed.' }
```

All JS wrappers belong in fast. They invoke Node with script-specific inputs.
Existing wrappers skip when Node is absent, leaving JS acceptance pending;
the parameter-tie certification wrapper requires Node and fails if absent. Compiled gates
require Numba and actual compiled backend execution, not reference fallback.
Explicit module-level JIT-off and missing-dependency guards remain supported for
focused/raw use; their skips cannot certify compiled execution.

Use static `@pytest.mark.slow` on expensive parity, full-population identity,
numerical oracle, or worker checks based on warm cost and purpose. Keep cheap
baseline/contract checks in fast. A subprocess alone does not justify `slow`.
Full always includes slow tests. Keep the existing `regression` marker for evidence
selection; collection output and in-code markers own the current inventory.

## Test design and server ownership

Test observable behavior, including success, validation failures, and boundaries.
Use isolated mutable state and restore it after each test. Preserve numerical
oracles, tolerances, candidate order, parity checks, and negative cases. V1 supports
Optuna and Grid; new V2 Optimize/WFA requests require explicit Grid, while historical
V2 Optuna reads and supported replay remain compatible. Ordinary helpers must not
mutate global JIT settings; interpreted oracles require separately configured
processes. Tests never write protected application data.

The former server module is now `tests/server`, with these responsibilities:

| Module | Ownership |
| --- | --- |
| `test_ui_contracts.py` | Source-text UI/readiness, bootstrap and logging |
| `test_run.py` | Strategy/optimizer policy, configuration and cancellation |
| `test_runtime.py` | Runtime adapter, validation precedence, dates and backtest projection |
| `test_grid.py` | Grid metadata and preview HTTP contracts |
| `test_grid_settings.py` | Stored Grid display, constraints and memoization |
| `test_wfa.py` | WFA construction, months, adaptive fields and execution routes |
| `test_data.py` | CSV import and stored WFA details/equity |
| `test_queue.py` | Queue persistence, transport and non-mutating reads |
| `test_analytics.py` | Analytics summary, equity and sets |
| `test_export.py` | Stored execution, trade downloads and Lancelot compatibility |

Keep tests independent of other test modules. The package-local `conftest.py`
provides the function-scoped client for the existing Flask app and restores
`TESTING`; root fixtures continue to isolate storage, journals, Queue and CSV roots.
Keep single-owner builders local. Import shared builders explicitly with
`from ._helpers import ...`; they return fresh mutable values and own the package's
repository-root constant for tracked source/sample reads. Register assertion-bearing
shared modules with `pytest.register_assert_rewrite` in the local conftest **before**
importing them, using their actual qualified name (`server._helpers`). Keep the
package marker empty. Do not import conftest or add test-to-test imports.

`tests/pattern_lab` covers the Pattern Lab data foundation, collector, event
studies, M3a matched comparisons and M3b context/frozen validation with runtime-generated synthetic packs only;
it reads no market data and commits no binary fixtures. The `test_pattern_lab_study_*.py` modules build
hand-calculated bars, write their trusted extension modules into the launcher's
external temporary root, and drive the real coordinator, so admission ordering,
recorded status, retained evidence and exit codes are observed rather than
asserted structurally. `test_pattern_lab_study_contracts.py` owns the enforced
boundary contracts: custom-model evidence admission and read-side revalidation
of saved tables whose file hashes still match, completion and partial-inspection
integrity, every accepted public request form, used-source attribution, resolved
dependency warmup, failure attribution and bounded evidence reuse. It counts
decoded evidence tables to keep the summary read count independent of the group
count, rather than asserting a wall-clock threshold.
`test_pattern_lab_context.py` covers dense-grid arithmetic, missingness, causal
prefixes, explicit aliases, scope/cycle rejection, the custom context extension,
read-only transport, shared reads, identity and worker parity, and context-phase
failure/interrupt status. `test_pattern_lab_candidate.py` covers frozen source
generation, v1 lifting, splits, code policy, CLI status, receipts, short-period
descriptions and relocated offline reporting. Its point-estimate oracle computes
the target-count weighted means independently from checked saved accumulations.
`test_pattern_lab_extension_generation.py` covers fresh/cached namespace and scalar
helper imports, transitive declarations, unknown/stale runtime generations,
verified reuse, stale bytecode avoidance and owned cleanup on failure/interrupt.
It also covers deterministic stale directory-listing caches (by restoring a
task-owned directory's recorded mtime), public in-window `importlib.import_module`
calls at module scope and registration, aliases, relative package imports, all
three cached-generation defects and transitive cached-helper reuse. No sleeps
or caller-side cache invalidation stabilize those tests. Hook/path snapshots
include both public import routes; whole-path restoration is the documented
policy. These process-wide observer tests run serially within each interpreter;
do not overlap extension loading with imports from other threads. Spawn checks
exercise separate interpreters and do not certify thread safety. Fixtures use
unique module names or restore only their task-owned imported state, leaving
unrelated cached modules intact. The dense-grid test also drives the shipped
BTC evaluator and checks independently expected gap/recovery values.
`test_pattern_lab_frozen_integrity.py` checks actual outer-root generation admission
for mapping/file/normalized requests, module/helper drift, source relocation with
direct/spawn parity, strict saved attribution, authoritative parent receipts,
publication faults on both sides of the atomic write, and relocated offline
verification without numerical decoding. The context tests also demonstrate
multi-bar dense validity and the exact recovery boundary after a missing slot.
The compatibility suite also reseals malformed saved method metadata and checks
the separate historical-v1 numeric domains against current launch policy.
Run these focused modules while changing the contracts, then the whole
`tools/run_tests.py -- tests/pattern_lab` suite once after final changes. Windows
process inspection requires usable `tasklist`; a sandbox access denial is a
verification failure requiring an authorized rerun, not evidence of cleanup.
WinError 1314 remains a named unverified symlink boundary where privileges are
unavailable; do not silently skip it or change Windows policy.
`test_pattern_lab_study_workers.py` starts real `spawn` children through the
production coordinator and owns worker parity, effective capacity, out-of-order
completion, the retention bound, child isolation, the thread policy, declared
helper source generations in a fresh child, result-transport liveness, failure
and cancellation mapping and coordinator death. Its small test-local process
helpers distinguish a successful query saying a PID is absent from a failed
`tasklist`/`taskkill` invocation, report the return code and stderr, and clean
up every owned PID even when one operation fails; an unavailable or refused
process tool is never read as proof of death or of successful cleanup. Their
command decisions are exercised with injected responses on any host, which does
not certify actual Windows execution. Because `monkeypatch` state is not
inherited by a spawned interpreter, its child-local network, pack and
publication guards live in a declared temporary test extension that installs
them only inside a worker; there is no production test mode. Its module is
marked `slow`, and its ordering proof uses explicit test-owned handshake markers
rather than sleeps. Each case uses a unique extension module name because a
Python interpreter imports a module once, which is the condition the study's
fresh-interpreter source-integrity error describes. Its import-isolation and missing-dependency checks run in fresh
child processes, because the root fixtures already import storage into the pytest
process. They simulate an absent PyArrow with an import blocker instead of
uninstalling the pinned dependency, so a missing wheel can never turn the suite
into an all-skipped success.

Collector cases never call an exchange API. Every response is generated at runtime
by the synthetic OKX/Bybit protocol fixture in `_helpers.py` and injected through
the adapters' transport boundary, with an injected clock and sleeper so retries are
deterministic and fast. The package-local `conftest.py` adds a default network
denial that replaces the collector's default transport and `urllib.request.urlopen`
for every test in the package, so a forgotten injection fails loudly instead of
reaching a venue. It is installed with plain assignment rather than `monkeypatch`,
so a test's own `monkeypatch.undo()` cannot restore the real socket path; the guard
itself is covered by tests. Shared collector, journal and recovery builders live in
`_helpers.py`, and no Pattern Lab test module imports another test module.
Process-exclusion cases use task-owned child processes with bounded timeouts and
external storage; no production process is signalled. A Linux run does not certify
the Windows `msvcrt` lock branch.

The M3a analysis modules are `test_pattern_lab_analysis_estimator.py` (the
numerical core, checked against an independent row-level weighted oracle, a
central finite-difference derivative of the exact weighted estimator and a slow
index-based resampling oracle), `_analysis_contracts.py` (the versioned request,
the resolved family and source admission), `_analysis_artifact.py` (publication,
failure and interrupt states, the versioned seal, relocation, regeneration and
the CLI) and `_analysis_calibration.py` (the calibration driver's machinery,
including the frozen generator contract and the exact-binomial acceptance
arithmetic). They build studies and analyses under the launcher's external
temporary root and never read market data.

`test_pattern_lab_analysis_v2.py` covers ordinary v2 requests, real sealed
artifacts and relocation, version/method agreement after rehashing, G=13 scope,
method evaluation order and isolation from research imports. Its available and
refused cases use runtime-generated synthetic studies.
`test_pattern_lab_analysis_monthly.py` covers the shared
`monthly_cluster_jackknife_v1` numerical kernel and bounded research pipeline
replays. Companion `_monthly_platform.py`, `_monthly_replay.py`,
`_monthly_admission.py` and `_monthly_provenance.py` cover native memory readings
and injected failures, requested-work projection and pilot reuse, complete replay
families, plan-1 evidence admission and source attribution. Constructed compact
records test PASS/FAIL/INCOMPLETE without running a Monte Carlo matrix. The tests
include historical format 1 and new format 2, optional decoded-byte checksums,
ambiguous plain/gzip rejection and mandatory disclosure/refusal behavior.
Consumed verifier settings are checked before scoring or stop proofs; old
five-source research attribution remains readable while revision 2 requires
the shared monthly module.
Native Windows readings are exercised on Windows; injected Linux results on that
host do not certify native Linux `getrusage` or `/proc` calls. Run the platform
module on each host, and keep the full Pattern Lab suite's worker/platform cases.

The monthly CLI is independent of production analysis requests:

```bash
python -m tools.pattern_lab.analysis.calibration_monthly --output-root NEW_EXTERNAL_DIR \
    --fixtures 002_null_dependent_t5 102_null_causal_ohlc_v1 --attempts 2
python -m tools.pattern_lab.analysis.calibration_monthly --output-root COPIED_ARCHIVE \
    --summarize-only
```

The first command must complete its bounded replays but remains diagnostic
INCOMPLETE (exit 2). The second generates no data and requires no memory API;
preserve the archive's original summaries before it overwrites the copy's
top-level summaries. Exit 0 requires the complete valid 17-entry plan, all main
gates, mandatory disclosures/refusals and both named replays. See the
[Pattern Lab guide](../tools/pattern_lab/README.md#the-experimental-monthly-jackknife-candidate)
for resource, acceptance and historical provenance limits. Candidate evidence
does not accept M3a or start M3b.

The large declared calibration experiments deliberately stay **outside** normal
discovery: they run once for a delivery and save one compact JSON artifact under
an external task-owned root. Their purpose is to measure and bound the
finite-sample error of the approximate inference screen on declared synthetic
fixtures — it is not a certification of exact 5% control, and the required
long-dependence experiment sits outside the admitted-null envelope by design.
**The legacy bootstrap delivery met 11 of its 15 acceptance checks**. Monthly
jackknife passed its declared G=12 synthetic screen, and ordinary v2 integration
awaits M3a tech-lead acceptance. Changes to the formula, support gates or frozen
experiment require an explicit new calibration decision, not an automatic full
experiment during ordinary verification. That saved document is also *ineligible*
under the versioned `legacy_bootstrap_v1` gate, because schema-v1 records carry
no attempt ledger and it embedded no replay; ineligibility is separate from, and
does not excuse, its 11-of-15 rate result. Exit 0 means complete acceptance of
that named protocol; exit 2 covers both a completed diagnostic or subset run and
a completed failing one, so callers read the explicit JSON state.

```bash
python -m tools.pattern_lab.analysis.calibration \
    --output-root "${TMPDIR:-/tmp}/pattern-lab-calibration"
```

```powershell
& $py tools/run_tests.py -- tests/pattern_lab
& $py tools/run_tests.py -- tests/server
& $py tools/run_tests.py -- tests/server/test_runtime.py
& $py tools/run_tests.py -- tests/server --co
& $py tools/run_tests.py -- tests/v2/test_v2_grid_identity.py::test_tz64a_request_runtime_row_digests_and_identity_pins
```

The last case remains slow and preserves request normalization, row digests and
identity pins without requiring the server client.

## Isolation, preflight, and retention

Pattern Lab M4 coverage is in `test_pattern_lab_bracket.py` and
`test_pattern_lab_sequential_evidence.py`: hand-computed bidirectional RR trades,
Pine ATR bitwise parity, quantity-unit conversion, closed settings, warmup,
gap/expiry/occupancy, cap admission, explicit Arrow schemas, resealed semantic
contradictions and offline reports. Fresh subprocess tests unset PYTHONPATH,
parse complete CLI stdout as JSON and compare direct/spawn mixed-study tables.
Fixed-only API/report tests keep core/strategies unloaded. The storage-import
probe copies source to task-owned temporary storage; it never removes or renames
the actual storage directory. Existing worker cancellation/failure and frozen
candidate/receipt suites remain preservation gates. Importer tests include
cross-window retained aliases and resolved-path, unchanged-mtime cache priming.

After changing the generic reference policy/trace, run
`tests/v2/test_v2_kernel_execution.py tests/v2/test_v2_stateful_trails.py
tests/v2/test_v2_tick_rounding.py tests/v2/test_v2_s06_b2_parity.py`.
Default-mode preservation and trace-on/off numerical parity do not certify a
new compiled mode. Keep known Windows symlink privilege failures visible; do
not regenerate baselines or rerun calibration for bracket implementation work.


`tools/run_tests.py` uses only the standard library and launches pytest with
`sys.executable`, the repository cwd, and its tracked pytest configuration.
Supported options and targets after `--` are pytest input. Every mode rejects nonblank `PYTEST_ADDOPTS`:
unset it and pass intentional options explicitly after `--`. Named fast/full modes
also reject nonempty `NUMBA_DISABLE_JIT` other than `0`; unset it or use `0`.
The launcher never silently changes JIT or thread counts.

The launcher owns `--basetemp`, `cache_dir`, and configuration selection. Alternate
configs, `addopts` overrides, and argument files are rejected because they can
hide selectors or path overrides. Advanced configuration can use prepared raw
pytest below. Disabling the cache provider (`-p no:cacheprovider`) is allowed.

Use individual short switches or ordinary `-qq`/`-vv` repetitions. The value-taking
options `-k`, `-m`, `-o`, `-p`, `-r`, and `-W` accept separate or attached values:
`-kclock`, focused `-mslow`, `-ra`/`-rs`, `-o console_output_style=count`, and
`-pno:cacheprovider` work. Consumed values are opaque to grouping checks;
`-po:cacheprovider` means the plugin name `o:cacheprovider`, whose existence pytest
decides. Safe ini overrides remain supported. Both exact aliases `--co` and
`--collect-only` work; abbreviations of forbidden long options remain forbidden.
Mixed clusters such as `-qm`, `-qocache_dir=...`, `-qc`, `-vs` and `-sx` are
rejected before run preparation: separate the switches or use prepared raw pytest.
The launcher does not support a second `--` positional terminator. Unfamiliar
advanced forms also belong in prepared raw pytest. This bounded grammar does not
sandbox plugins: explicit `-p` and `PYTEST_PLUGINS` retain pytest semantics.

Default root: `repository_root.parent / "merlin-tests"`. Override with
`MERLIN_TEST_ROOT` when needed. The resolved root must be outside every Git
worktree, including `.git` files for linked worktrees/submodules and enclosing
checkouts. No implicit alternate root is chosen on rejection.

```text
merlin-tests/
  cache/numba/<python-and-numba-version>/
  cache/pycache/<python-version>/
  runs/<unique-run>/
    pytest/
    pytest-cache/
    tmp/
```

Before the child starts, the launcher sets `NUMBA_CACHE_DIR`,
`PYTHONPYCACHEPREFIX`, `TMPDIR`, `TMP`, and `TEMP`, plus pytest paths. Subprocesses
inherit this isolation. Existing storage/journal/Queue/CSV-root fixtures remain
responsible for application state. The Lab two-process determinism test reuses
the configured Numba cache; with an unset/empty value it uses one temporary
`tmp_path / "numba_cache"` shared by its two children, never an in-tree default.

Successful runs delete only their unique run directory unless `--keep-temp` is
set. Failures and handled interruptions retain it and print its location. Cleanup
failure preserves a passing exit code and reports the retained directory. Shared
caches and other runs are never cleaned. The PowerShell `tools/run_pytest.ps1`
shim forwards ordinary pytest arguments in focused mode and supports `-KeepTemp`;
it retains its configured interpreter selection (`MERLIN_PYTHON` override).
Pass pytest switches explicitly as an array: PowerShell otherwise binds bare `-k`
to `KeepTemp` and bare `-v` to its common `Verbose` parameter.

```powershell
.\tools\run_pytest.ps1 -PytestArgs @('-v', '-k', 'core_logger_console_handler_is_configured_once', 'tests/server')
```

Use `-KeepTemp` separately; prefer the Python launcher for ordinary commands.

Numba cache reuse does not guarantee invalidation of dependencies in other files
or compile-time globals; see [Numba caching limitations](https://numba.readthedocs.io/en/stable/developer/caching.html#caching-limitations).
When kernels, relevant dependencies, or compiler versions change, use a fresh
external root for a targeted cold check. Do not delete existing shared caches:

```powershell
$env:MERLIN_TEST_ROOT = Join-Path $env:LOCALAPPDATA ("Temp\merlin-cold-" + [guid]::NewGuid())
& $py tools/run_tests.py -- tests/test_s06_fast_grid.py tests/v2/test_v2_grid_s06_gate.py::test_s06_t1_reference_subset_metrics_match_v1_fast_grid --deselect=tests/test_s06_fast_grid.py
# Use another fresh root for cold fast evidence, then repeat for warm evidence.
$env:MERLIN_TEST_ROOT = Join-Path $env:LOCALAPPDATA ("Temp\merlin-fast-" + [guid]::NewGuid())
& $py tools/run_tests.py fast -- --durations=20
& $py tools/run_tests.py fast -- --durations=20
```

For organization-only acceptance, reuse applicable cold evidence and let focused
checks warm the external shared cache before unfiltered fast/full gates. The Lab
two-process smoke has a 300-second child timeout that has failed on a cold,
constrained one-vCPU host. If that known timeout recurs, retain its failed log,
confirm the unmodified case passes with the warmed cache, then rerun the affected
unfiltered gate successfully. A focused warm pass alone does not repair a failed
suite result. Investigate other failures or a repeated warm failure; do not shorten
the workload, skip it, disable JIT, or change its timeout. Record warmup, cache/host
conditions and failed attempts. Warm acceptance does not certify cold-start timing.

## Prepared raw pytest and coverage

Raw collection, the environment's pytest entry point, and pytest-cov remain
supported. An unprepared bare pytest invocation is not fully isolated. Configure
all paths before Python starts, including the parent for subprocess tests.
Choose a new task-owned directory outside every checkout for each raw run:

```powershell
$raw = Join-Path $env:LOCALAPPDATA ("Temp\merlin-raw-" + [guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $raw, "$raw\tmp" | Out-Null
$env:PYTHONPYCACHEPREFIX = "$raw\pycache"
$env:NUMBA_CACHE_DIR = "$raw\numba"
$env:TMPDIR = "$raw\tmp"; $env:TMP = $env:TMPDIR; $env:TEMP = $env:TMPDIR
$env:COVERAGE_FILE = "$raw\.coverage"
& $py -m pytest --basetemp "$raw\pytest" -o "cache_dir=$raw\pytest-cache" --collect-only
# Or use the configured environment's pytest.exe with the same arguments.
# Coverage: replace --collect-only with --cov=src --cov-report=term-missing.
```

```bash
raw=$(mktemp -d "${TMPDIR:-/tmp}/merlin-raw-XXXXXXXX")
mkdir "$raw/tmp"
export PYTHONPYCACHEPREFIX="$raw/pycache" NUMBA_CACHE_DIR="$raw/numba"
export TMPDIR="$raw/tmp" TMP="$raw/tmp" TEMP="$raw/tmp"
export COVERAGE_FILE="$raw/.coverage"
python -m pytest --basetemp "$raw/pytest" -o "cache_dir=$raw/pytest-cache" --collect-only
# pytest --basetemp "$raw/pytest" -o "cache_dir=$raw/pytest-cache" --collect-only
```

Raw runs own their retention; remove only exact directories you created. Keep
coverage reports external too if requesting HTML/XML output.

## Evidence and external certification

Tracked baselines under `data/baseline/` and `data/baseline_v2/` are immutable
oracles. A mismatch requires review, not baseline regeneration. Preserve numerical
tolerances, candidate order, fingerprints, trades, and meaningful negative cases.
Performance comparisons need the same dataset, plan, workers, warmup, and cache
conditions; see [performance evidence](../docs/engine_v2/PERFORMANCE.md).

Real WFA certification remains explicitly outside normal discovery. It requires
the exact external read-only pack and existing `smoke_one` output; missing
prerequisites fail instead of producing an all-skipped success:

```powershell
$env:MERLIN_STRATEGY_LAB_DATA_ROOT = '<read-only-data-root>'
$env:MERLIN_STRATEGY_LAB_CERT_WORK_DIR = '<absolute-certification-dir>'
& $py tools/run_tests.py -- tests/strategy_lab/phase1b_real_wfa_certification.py
```

Follow the [Strategy Lab manual](../tools/strategy_lab/README.md) for preparation
and authorization. Normalization cases are ordinarily collected without importing
this opt-in module. Ordinary full runs use synthetic SQLite data and require no
operational database. See the [V1](../docs/ADDING_NEW_STRATEGY.md) and
[V2](../docs/ADDING_NEW_STRATEGY_V2.md) guides for strategy-specific test obligations.
