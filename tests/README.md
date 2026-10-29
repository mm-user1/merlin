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

Start/Queue source UI preservation uses `test_js_source_workflow.py` alongside
the existing Queue, Parquet, Entry Filters, admission, Fixed, Grid-objective and
strategy-readiness JavaScript checks. It drives actual selection/editor/Queue
handlers with deterministic pending responses and simulated source completion:
typed eligibility, explicit Off, matching Preview counts, late replies, role-based
Ticker Sets, independent filter defaults and actionable partial failures. Server
data-input tests check metadata roles and the default-root template without reading
or creating a production pack. Browser layout remains a separate owner check.

Fixed research coverage is in `v2/test_v2_fixed_singleton.py`,
`server/test_fixed_mode.py` and `test_js_fixed_mode.py` (real JavaScript handlers).
It covers scalar/variant singleton planning, early refusal, successful loss/no-trade
retention, actual Fast/reference metrics, CSV/Parquet OOS/WFA, stored Preview,
manual replay/pins and Queue restoration/fan-out. Use the existing Grid domains,
identity, dispatch/storage, plan-reuse, server Grid/settings/WFA/Queue, OOS selection,
admission integration and UI regression modules alongside these tests. No host
exclusions are implicit. Use external scratch and retain logs/XML; owner browser
appearance is a separate gate. The marker changes no certified execution arithmetic.

Fixed entry-filter coverage is in `test_entry_filters.py`,
`test_entry_filter_workers.py`, `v2/test_v2_entry_filters.py`,
`server/test_entry_filters.py` and `test_js_entry_filters.py`. The synthetic opt-in
strategy stays in `fixtures/admission_strategy/filtered.py`; no production strategy
is registered by these tests. Tests cover causal native-clock permissions, prefix
gaps, bounded replay, hook/worker transport, Fast ranking, HTTP/Queue/WFA/manual
execution and real editor/Analytics handlers. Use the isolated launcher below.
Run `test_filtered_fast_reference_packers_threads[1]` and `[2]` on a host with
those actual Numba thread capacities. The broad T04 one-CPU review excludes exactly:

- `v2/test_v2_entry_filters.py::test_filtered_fast_reference_packers_threads[2]`
- `v2/test_v2_admission_grid.py::test_fast_admission_packers_and_threads[2]`
- `v2/test_v2_grid_compiled.py::test_compiled_grid_v2_worker_count_is_deterministic`
- `v2/test_v2_grid_signal_topology.py::test_compiled_signal_worker_count_is_deterministic`

The narrower invalid-instrument follow-up uses `test_entry_filters.py`,
`server/test_entry_filters.py`, `server/test_analytics.py` and
`v2/test_v2_entry_filters.py`; only the first two-thread selector above is in that
set. Its regressions cover application-error translation, inactive Off controls,
launch 400 without execution and offline Analytics 200/Unavailable for a damaged
row alongside valid records, with unchanged saved configuration and strict writes.
Real two-process spawn checks are distinct from Numba thread capacity and still
run in the broad Linux gate. The filter-root symlink case must run
on a symlink-capable host; Windows privilege failures are not passes. All JS
wrappers in this slice accept the installed `MERLIN_NODE_EXECUTABLE`, including
Electron with process-local `ELECTRON_RUN_AS_NODE=1`; they run from external scratch.
Browser layout/focus remains a separate owner gate. See
[entry filters](../docs/ENTRY_FILTERS.md) for the contracts.

Risk-bracket admission uses `tests/v2/test_v2_entry_admission.py`,
`tests/v2/test_v2_admission_grid.py`, `tests/test_entry_admission_integration.py`,
`tests/test_entry_admission_adaptive.py`, `tests/server/test_entry_admission.py`
and `tests/test_js_entry_admission.py`, plus the affected V2/server/data-input,
Grid/WFA/FT/Stress/storage/Analytics/export/capital suites. The fixture under
`tests/fixtures/admission_strategy/` is explicitly registered only for tests or
an isolated preview. Its spawn wrapper registers the fixture then calls the
production initializer with unchanged arguments; no frame/rule injection or
mock worker execution. Collision checks protect the registry.

The admission follow-up adds zero/null rule and strict replay controls, partial-cap
Fast selection and persisted IS/OOS refusals, and actual SQLite diagnostic UPDATE
counts. `tests/server/test_admission_preview.py` enforces metadata-only Preview,
source/rule/lock errors and full candle admission on launch. These scoped changes
use the admission/data-input/storage/Grid suites; unchanged shared execution and
provider sources do not require another full Pattern Lab preservation run.

The Grid test's `[1]` and `[2]` selectors require those actual Numba thread
capacities; a one-CPU host cannot substitute for Windows two-thread evidence.
Two-process spawn remains a distinct gate. The new JS wrapper also accepts
`MERLIN_NODE_EXECUTABLE` for an existing compatible runtime (for Electron's
bundled Node, use process-local `ELECTRON_RUN_AS_NODE=1`); no installation is
implied. Browser layout/selection remains an owner check.

Preserve the existing Pattern Lab bracket, bracket-consistency, account execution,
account verification, account-study integration, carried selection and identity
tests when shared V2 helpers change. Prospective source hashes change honestly;
unchanged numerical fixtures do not imply unchanged implementation identity or
permission to rewrite sealed artifacts. Strategy Lab stays CSV-only and rejects
the new mode; its planning/packing preservation tests remain applicable.

For Merlin Parquet input changes, add `tests/test_data_input.py`,
`tests/test_data_input_workers.py`, `tests/server/test_data_input_routes.py`,
`tests/server/test_data_input_timings.py` and `tests/test_js_parquet_input.py` to
the applicable server/Grid/WFA/storage/export and shared provider slice/lock gates.
Fixtures use public provider APIs and external task-owned storage. Exact CSV/Parquet
frame parity precedes adapter trade/metric comparisons. Worker checks use actual
spawn processes while a separate process holds the pack lock. Diagnostic Grid/WFA
timings have no speed threshold; CSV parsing/window buffers remain unchanged.
Node harnesses require an actual Node pass; a missing executable is a pending gate.
`tests/server/test_data_input_followup.py` compares exact Parquet OOS replay with
captured original strategy executions (5m/15m, midnight/intraday/subsecond and
metadata-origin cases), validates atomic metadata persistence and old-record
refusal, and preserves CSV error/no-trade and WFA symbol contracts. Its Queue
regression runs the real restore entrypoint with the Parquet controller in
`tests/js/parquet_input.test.cjs`, including contradictory generic snapshots.
That harness also checks that a pinned replay refusal clears the previous chart
and date labels while retaining its error, and that a valid chart can render again.
Existing CSV OOS-chart and IS-export period inconsistencies are separate legacy
defects; blanket CSV/Parquet export equality is not their acceptance oracle.
Owner browser layout/focus/selection checks remain separate from JS model tests.
Shared-reader preservation guidance below still applies; application integration
alone does not authorize regenerating historical research artifacts.

A1 shared-provider changes use `tests/pattern_lab/test_data_provider.py` for
standalone imports, canonical module/error identity, foreign-provider refusal,
explicit roster admission, exact attribution and candidate policy 2. Existing
candidate, storage/collector/recovery/lock, auxiliary and account/selection tests
remain preservation coverage. Final A1 preservation runs all `tests/pattern_lab`
and `tests/v2/test_v2_kernel_execution.py` through this launcher with retained
logs and external storage. Genuine pre-edit Windows artifacts are separate
review inputs for historical policy-1 freezing/loading/refusal and exact-parent
fixed/carried replay; ordinary tests do not depend on ignored evidence.

The scoped LF and autocrlf tests include shared provider root/nested Python paths,
including new untracked files before commit. They do not stage product files or
change repository/global Git configuration. Guards patch canonical `data_provider`
modules; supported Pattern Lab attributes are those same objects. Old direct
`tools.pattern_lab.data` submodule imports are no longer supported.

The provider ownership check in `test_pattern_lab_cli.py` uses the same AST
predicate for actual files and positive/negative fixtures. It covers direct and
nested imports, `from tools import pattern_lab`, and literal `import_module`
targets with ordinary importlib aliases. Documentation strings remain allowed;
the predicate is not a general analyzer for computed dynamic imports.

T05 `test_account_containers.py` covers direct streamed STORED/ZIP64 records,
physical versus logical admission, local headers/CRC/extents, immutable metadata
reuse across interleaved tickers, failed publication and handle release, transport
byte preservation and explicit schema/version dispatch. Native/auxiliary tests
retain full directory routes through the private writer hook alongside the new
format-3 default, including debug, offline verify/rebuild and fixed/carried
selection with exact spawned replay and original-parent pin refusal. Genuine
pre-edit Windows archives and historical bundles are explicit handoff gates;
ordinary pytest never depends on ignored development evidence.

T05-1 adds explicit ordinary, sentinel and redundant local ZIP64-size fixtures
on every host, plus a coherently resealed format-3 economic forgery that loads
structurally but fails full validation. `account_report_generic.cjs` checks actual
embedded models and real UI view/selection helpers for arbitrary native reports;
`test_account_report_generic.py` covers a plain generated report and broken models.
The strict `account_report_interactions.cjs` fixture assertions remain unchanged.
Node execution/assertion failures fail their gates; missing Node is explicitly
pending until Linux review. The scoped T05-1 gate runs container, study evidence,
study integration, auxiliary, robustness, generic report and selection
integration/carried/auxiliary/storage-v2 modules, plus five corrected explicit
archive cases. This narrow follow-up does not require a new full preservation or
UAC run; the retained T05 preservation receipt certifies its original sources.

T05 preservation requires all `tests/pattern_lab` and
`tests/v2/test_v2_kernel_execution.py` through this launcher, external temporary
roots and real Windows symlink capability. Linux reviews must retain subprocess
exit statuses as well as numerical output. The reviewed Python 3.11.0rc1 /
PyArrow 22 host has an independently observed non-path Parquet shutdown abort;
a recurring failure is an unresolved host/CLI gate, not a pass. Preserve and
isolate it with the supplied probe. Do not use tracemalloc around non-path
Parquet reads or add sleeps, GC workarounds, dependency installs or security
changes. Node and owner browser/focus checks remain separate gates.


T04B adds `test_selection_carried.py`, `test_selection_auxiliary.py` and
`test_selection_storage_v2.py`: native lot-floor execution, exact capital carry,
missing-month stops/depletion, scalar-only random retention, one/six spawn byte
parity, exact-parent replay and coherent forgery detection, causal auxiliary
coverage/lag/ownership, partition guards, bounded failure cleanup and the public
example outside the repository. `selection_carried_report.cjs` accepts generated
v2 HTML and checks both capital modes and selector/K interactions; its pytest
wrapper skips only when Node is absent. Owner visual/focus review is separate.
Run all `test_selection*.py` plus the nine accepted account/account-study/
robustness/auxiliary modules, then all `tests/pattern_lab` and
`tests/v2/test_v2_kernel_execution.py`. Windows symlink tests require a capable
token; use only the task-specific owner-approved UAC procedure, with no security
settings changes. Exact Windows archive compatibility/replay helpers live with
the explicit task handoff and are not discovery dependencies. Linux replay uses
saved exact capitals and requires exact facts/IDs/paths, never tolerance or ID
remapping. Same-host offline rebuilds must match byte-for-byte.

Pattern Lab's `test_accounts_preparation.py`, `test_accounts_execution.py` and
`test_accounts_verification.py` cover the T01A library account API: native 5m
preparation bindings, independent timeframe features, causal state expiry,
immutable transport, generic command modes, shared long/short accounting,
windows and boundary chronology, unchanged bracket/V2 differentials, an
independent scalar forward-search oracle and semantic evidence tampering.
Provider regressions cover canonical false permissions on unknown candle states
and unchanged long/short execution versus the former raw-value permissions.
`_account_helpers.py` and `_account_oracle.py` contain synthetic fixtures only.
The fresh-process public example denies ignored-work reads and removes
PYTHONPATH. One explicit spawn child checks failed mutations and exact same-host
result equality; integrated six-worker account routing belongs to T01B.
The `test_account_study_{integration,evidence,metrics}.py` modules cover the
separate T01B route: native admission/warmup, overlapping windows, fixed/per-target
sources, mode/direction adapters, direct/six-worker equality, failure cleanup,
generic codecs, standard offline validation/rebuild, safe ZIP import and metric
boundaries. Primary parity/offline fixtures assert filtered trades in every
allocation/window and nonempty trailing proposals using a fixed synthetic BTC
source; dedicated own-ticker cases retain blocked/unknown permission and source
subset checks. Unused ATR dependencies are refused before pack/output access.
`node tests/pattern_lab/account_report_interactions.cjs` checks report
interaction state without claiming browser/focus coverage. Synthetic fixtures
are under `_account_study_helpers.py`; the runnable public synthetic example
uses no test/ignored-work imports. Retain a Windows-produced compact bundle for
Linux full verification and rebuild comparison after the owner's Git update.
`test_account_robustness.py` adds independent overlap, passive endpoint, removal,
side reconciliation, calendar/coverage and pair-eligibility checks. The portable
v1 expectation in `fixtures/account_metrics_v1.json` was captured at accepted
`29574bb` before T02 metrics edits with explicit integer prices; it pins metric
values/labels/counts, not freshly generated identities or raw float bytes. The
exact old Windows ZIP is a separate explicit delivery/review gate, never an
ordinary test dependency. The multi-month dyadic fixture asserts both trade sides,
missing passive endpoints, non-exact/calendar edges, tied/differing pairs and a
nonempty profit-erasing ticker subset. Generated HTML remains in external temp.
The Node wrapper locates Node with `shutil.which` and passes that generated HTML
to the existing executable harness. Only this wrapper skips if Node is absent;
record the Node gate pending until it executes on the review host. The harness
also accepts no argument, preserves ticker-navigation checks and reads shared
numeric vectors in `fixtures/robustness_numbers.json`. Neither form certifies
browser layout or focus. Account integration tests retain standard/debug and
one/six-worker exact derived parity. Run the new module plus all six existing
account/account-study modules, then the named preservation gate after final edits.
Preservation gates are all `tests/pattern_lab` and
`tests/v2/test_v2_kernel_execution.py`.

T03 adds `test_auxiliary_data.py` (offline recording transports, immutable typed
snapshots, overlap/late arrivals, strict source units, causal lag/coverage/gap
boundaries and synthetic provider/type extensions) and `test_account_auxiliary.py`
(closed request v2, genuine filtered/scalar trades, one/six-worker and
standard/debug parity, consumed versus parent identities, offline regeneration,
honestly resealed command tampering, CLI and an external-process public example).
No test reads a live API, market pack or ignored work document. Run both new
modules plus the seven existing account/account-study/robustness modules after
final changes, then the same full Windows preservation gate above. Tests retain
real symlink/process permissions; use owner-approved elevation if needed, without
changing OS policy. Keep logs/JUnit, actual case IDs, source/pin hashes and skips.

The report Node harness accepts either the T02 multi-month v2 metrics HTML or
the T03 auxiliary v3 metrics HTML. `test_node_auxiliary_report` skips only when
`shutil.which('node')` is absent; record that Linux Node execution is pending.
The Python example is also executable by absolute script path from outside the
repository with PYTHONPATH unset. Generated outputs must remain external.
Exact old T01B-1/T02 ZIP compatibility and the new Windows bundle's full Linux
semantic check are separate delivery/review gates, not ignored-fixture dependencies
of ordinary tests. For the new dyadic fixture, full checking must regenerate
command/component identities exactly on Linux; metric sums still use rel_tol
1e-9 / abs_tol 1e-8. Do not rewrite sealed inputs or weaken these gates.

T04A adds `test_selection_math.py` and `test_selection_integration.py`, with
independent ledger/path arithmetic, exact ties/hash vectors, past-only immutable
views, explicit missingness/coverage masks, experimental scorer/source checks,
real monthly v1/v2 accounts, offline seals/ZIPs/rebuild/deep verification and
resealed semantic forgery checks. The public seven-month/six-target example is
tested in an external subprocess with PYTHONPATH unset. No live or ignored-work
fixtures are used. `selection_report_interactions.cjs` checks genuinely generated
HTML through a pytest Node wrapper; only missing Node permits that wrapper's skip.
Visual appearance/focus remains an owner check. All 20 candidate pins and 44
protected account/study/auxiliary modules must stay byte-identical.

After final T04A edits run both selection modules and the nine T03/account-study/
account/robustness modules, then all `tests/pattern_lab` plus
`tests/v2/test_v2_kernel_execution.py` through tools/run_tests.py and an external
MERLIN_TEST_ROOT. Keep source/pin inventories, collected IDs, logs/JUnit and actual
skips. Use the established owner-approved symlink-capable Windows run without
changing OS settings. Separate delivery gates check the exact monthly Windows
parent and selection ZIPs on Windows and Linux: deep verification, original member
preservation and two byte-identical same-host rebuilds; cross-host numeric metrics
use rtol=1e-9/atol=1e-8, while IDs/labels/counts/memberships stay exact. Do not replace
the transferred parent with a freshly regenerated fixture. Linux review needs the
selection modules, these archive gates and installed Node; protected module suites
need no additional Linux sweep without a concrete regression reason.

For the narrow T04A-1 follow-up, run the two selection modules plus
`test_selection_capacity.py`. They cover early dimension/identifier-aware
capacity refusal, practical 44-ticker sizing, planning/write byte boundaries,
failed unsealed writes and one offline contract per selector. Genuine x. fixtures
and coherently resealed contract mutations are built by `_selection_helpers.py`;
they require no ignored files. The Node harness checks rendered monthly capital,
utilization and PnL for both series, underfill, empty pools and readable unavailable
states. Exact original T04A ZIP compatibility remains an explicit delivery gate:
unchanged economic identities/results and original hashes, two identical new
rebuilds, expected updated HTML. No full preservation/UAC rerun is required for
this scoped follow-up absent regression evidence or material scope expansion.

Pattern Lab's `test_pattern_lab_pilot.py` covers the M5 breakout loop oracle,
warmup/gaps and future invariance, downside RMS validity/null semantics, public
reader ownership and requested-table copy count, and synthetic study-to-analysis
and saved-report integration. Existing bracket, context, candidate and worker
tests remain preservation gates.

`test_pattern_lab_portability.py` pins pre-T09 policy-1 digest vectors, checks
policy-2 saved-input recomputation and source-only markers, exercises frozen
extension relocation and import/final-receipt refusals, and guards scoped LF
source bytes. Its temporary Git checkout tests `core.autocrlf=true` without
changing repository or global Git settings. Frozen-integrity tests retain the
offline receipt and contradictory saved-attribution checks.

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

`test_execution_capital.py` checks capital/default precedence, the S01 fixed-base
exception, hand-calculated realized profit/RoMaD, real FT/stress spawn values,
and Fixed/Adaptive WFA flat/delayed/truncated metrics and normalized stitching.
`server/test_data_input_followup.py` covers non-100 CSV/Parquet OOS/manual storage,
historical source comparisons, real Grid/WFA and offline flat reconstruction,
shared delayed-chart observations and malformed relocation requests. These tracked
regressions use synthetic inputs and do not import ignored task evidence.

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
`test_pattern_lab_bracket_consistency.py` adds exact executed-entry/admission links,
the complete stage/null matrix, saved anchor/exit/expiry invariants, missing-tail
pipeline coverage, historical custom-name/candidate compatibility, table-family
agreement, reader-copy isolation and one grouping/interval/sweep per account.
Source/rule identity composition, actual-open Bybit minimums, overflow at its
reachable boundary, state-entry transitions and context-missing price exits are
durable regressions. Synthetic builders live in `_bracket_helpers.py`; tests
must not import other test modules or ignored benchmark/audit scripts.
Performance comparisons use deterministic month/year inputs, native lifetime
peak RSS with initialized-process baselines and separate fresh processes.
They are review evidence, never CI timing thresholds or weaker semantic checks.

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

S07 HA synthetic coverage is in `tests/v2/test_v2_s07_ha.py`,
`tests/test_entry_filters_v2.py` and the versioned editor/Queue assertions in
`tests/js/entry_filters.test.cjs`. The full five-axis plan has 840 candidates;
one/two-thread and reference/compiled checks execute a small subset, not a market
parameter sweep. Historical F06 v1 and generic admission gates remain required.

`tests/test_s07_baselines.py` is an explicit external certification gate. Set
`MERLIN_S07_PACK` to the existing market pack and `MERLIN_S07_REFERENCE` to the
sealed `PLH-013-A/reference-evidence` directory, then use the isolated launcher.
Without the pack opt-in it skips; missing or mismatched opted-in inputs fail.
It verifies the hashes in `data/baseline_v2/s07_ha_v01/expected.json`, reads packs
under supported locks and writes comparisons only into retained test scratch.
It never imports the Pattern Lab account runner or rebuilds sealed references.
Keep `--keep-temp` for the two long-TV reports and September decision/trade/attempt
results. The January-origin September core check deliberately exceeds the UI
warmup limit; a separate production preparation with W=500 checks the same gate.
Do not raise the application maximum or call these synthetic CI fixtures.

```powershell
$env:MERLIN_S07_PACK = (Resolve-Path data/parquet-data).Path
$env:MERLIN_S07_REFERENCE = (Resolve-Path docs/_work/pattern-lab-for-strategies/PLH-013-A/reference-evidence).Path
& $py tools/run_tests.py --keep-temp -- tests/test_s07_baselines.py
```

The T04B-1 selection follow-up uses all `tests/pattern_lab/test_selection*.py`
through `tools/run_tests.py` with an external `MERLIN_TEST_ROOT`. Auxiliary tests
cover per-cell parsing, immutable ownership/native-price checks, batch release
in generation and deep recomputation, early envelope/resource refusal, late
required-coverage refusal before any scorer, corrected known-but-incomplete
features, explicit `.2`/`.3` dispatch, and honest auxiliary recomputation for
empty/no-input/stored-only/mixed selectors. New runs emit `monthly-selection.3`;
old `.2` artifacts retain original availability rules without migration. The
Node harness checks rendered dependency pins/roles and the unselected-member
chain-stop explanation separately from owner visual acceptance.

Explicit review helpers and genuine baseline/new F2 archives live in the
task's ignored review inputs; they are not ordinary tracked-test dependencies.
The corrected original-bundle helper uses `math.fsum` without changing expected
fees or exact account/path comparisons. Bounded synthetic 2/4/8-ticker raw 5m OI
probes (four-month history and sequential months) retain parse counts, elapsed
time and Python allocation peaks against isolated baseline code. They have no
CI timing thresholds and do not claim a 44-target runtime or hard RAM bound.
This scoped follow-up requires no full preservation/UAC rerun absent a concrete
regression; protected engine inputs stay immutable. Node and exact archived
Linux replay are separate required review gates when pending locally.

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

### Ticker portfolios

Quantity-only preservation is covered by `v2/test_v2_quantity_execution.py`
(all five configurations/12 variants, exact neutral-rule comparisons, Grid packers
and conditional identity), `v2/test_v2_quantity_edges.py` (independent arithmetic,
refusals, close independence, exhausted/nonfinite capital), `test_quantity_workers.py`
(real spawned workers with locked sources), and `server/test_quantity_portfolio.py`
(Fixed/WFA actual carry, FT/Stress, ownership, saved/manual replay and relocation).
`test_js_portfolio.py` includes the actual Start/form/Queue/Results quantity handlers;
browser appearance and restoration still require the separate owner check.
Keep all V2 historical identity and baseline tests; do not regenerate frozen oracles.
Because `engine_v2/kernel.py` is shared with Pattern Lab, include its scoped bracket,
consistency, sequential evidence, LF portability and account execution/verification/
integration suites. New source identities are truthful; sealed old evidence is immutable.

`tests/server/test_portfolio_replay.py` covers stored Fixed portfolio allocations
below/above editor bounds, actual carry, manual-source execution and fresh-process
reconstruction against an explicitly selected external DB. It also checks ordinary
and new-user bounds, ownership, invalid capital and replay-pin refusal. Portfolio
modules opt into a per-test DB fixture that restores the prior active DB in `finally`;
run them before `tests/test_storage.py` to retain order-isolation coverage. The ordinary
WFA export cases in `tests/server/test_export.py` retain explicit no-trade skipping
and the header-only empty-export response without rewriting saved windows.

Focused deterministic coverage lives in `test_ticker_selector.py`, `test_portfolio.py`,
`server/test_portfolio.py` and `test_js_portfolio.py` (actual Analytics set/table handlers
under a small Node DOM shim).
`test_js_portfolio.py` also runs `portfolio_loading.test.cjs` against the real
portfolio controller and Analytics coordinator/header renderer. Controlled delayed
detail/projection promises cover stale responses, PF exit/reset, retryable errors,
empty/unavailable results, keyboard navigation and asynchronous counters. The shared
DOM shim models events/selectors, not browser layout: actual main-area scrolling,
short destinations and user scrolling during requests need the separate browser gate.
The adjacent `portfolio_navigation.test.cjs` cases assert cached/uncached single-study
exit completion, current errors/empty curves, superseding selections, and rejected
database switches with successful retries and old-reply invalidation. They run via
the same `test_js_portfolio.py` wrapper and real coordinator completion handlers.
Use `tools/run_tests.py --keep-temp --` with an external
MERLIN_TEST_ROOT. Packs are import-built synthetic fixtures; no production data is needed.
The Node executable may be supplied with MERLIN_NODE_EXECUTABLE. Keep JUnit, failed
attempts and actual commands. Windows symlink boundary cases require host privilege;
report WinError 1314 as a host limitation, never a passing security check.

Relevant preservation includes WFA/Adaptive, execution capital, storage/Analytics,
Parquet workers/routes, admission/filters, FT/Stress, Fixed and Grid plan reuse/storage,
Results/trades/export and Start/Queue JavaScript suites. The portfolio guide documents
the contracts. Owner browser verification of actual layout, focus, inline periods,
Queue execution and Results remains separate from the handler harness.
