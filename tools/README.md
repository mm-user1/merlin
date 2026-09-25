# Tools

Run repository tools from the project root. On Windows, use the configured
project interpreter:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe <command>
```

Linux/VPS environments use their configured project Python and native paths.

## Baselines and checks

`generate_baseline_s01.py` regenerates the S01 regression evidence after an
explicitly reviewed behavioral change:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools\generate_baseline_s01.py
```

It writes `data/baseline/s01_metrics.json` and
`data/baseline/s01_trades.csv`. Do not refresh these files merely to make a
regression failure pass.

`test_all_ma_types.py` exercises all supported S01 moving-average types.
`benchmark_indicators.py` and `benchmark_metrics.py` provide focused timing
checks for their respective subsystems.

## Grid V2 diagnostics

`benchmark_grid_v2.py` measures direct Grid V2 runs and inspects saved WFA
diagnostics:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools\benchmark_grid_v2.py --help
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools\benchmark_grid_v2.py inspect-wfa-db --db <snapshot.db>
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools\benchmark_grid_v2.py direct-grid --config tools\benchmark_configs\s06_b2_sui_baseline_grid.json --workers 1,6 --warmup-runs 1 --runs 2
```

Candidate domains come from strategy `config.json` optimization metadata,
`enabled_params`, and selected `{param}_options`. Numeric `param_ranges` in a
benchmark payload do not independently redefine V2 grid granularity.
`inspect-wfa-db` uses SQLite read-only immutable mode and is for frozen
snapshots, not a live database with possible WAL frames.

`benchmark_s03_adaptive_ma.py --output <external-directory>` verifies the
frozen S03 adaptive-MA hashes and references, emits full per-trade comparisons
and measured warmup signal differences, and measures both Previews, the full
2,800-row symmetric run and a 128-row asymmetric sample across every MA. It
uses one worker, a 32 MB cache budget, and two executions per plan. Output and
Numba/Python caches remain external. It never constructs the 196,000-row full
asymmetric population or changes frozen inputs. See the performance document
for the measured environment, cold/warm distinction and process-memory scope.

## Isolated tests

`python -B -m tools.strategy_lab.certify_s03_smoke --data-root <market-root>
--work-dir <fresh-external-directory>` runs the explicit S03 CRV/windows-1..3
gate. It writes the pre-execution subset recipe, three dataset directories,
generation/analysis logs, partial development analysis and `evidence.json`.
It reuses the portable run spec and existing generation, parity, resume and
reader contracts; it never enters normal test discovery. See the
[Strategy Lab guide](strategy_lab/README.md) for cache isolation and commands.

`run_tests.py` is the canonical standard-library launcher. It uses the current
interpreter in a child process, external per-run pytest/temp directories, and
persistent Python/Numba caches. The default root is `../merlin-tests`;
`MERLIN_TEST_ROOT` can override it, but the root must be outside every Git
worktree (including an enclosing checkout). See the [test guide](../tests/README.md)
for selection, preflight rules, prepared raw commands, and cold checks.

Use separate short switches, `-qq`/`-vv`, or attached/separate values for
`-k`, `-m`, `-o`, `-p`, `-r`, and `-W`. Focused markers, safe ini overrides,
`-ra`, `-pno:cacheprovider`, `--co` and `--collect-only` are supported.
Ambiguous clusters (`-qm`, `-qo`, `-qc`, `-vs`, `-sx`) and a second `--` are
rejected before preparation. Use prepared raw pytest for advanced syntax.
The launcher controls its documented CLI, not arbitrary pytest plugin behavior.

```powershell
& C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools/run_tests.py fast
& C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools/run_tests.py full
& C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe tools/run_tests.py -- tests/test_metrics.py
```

`run_pytest.ps1` selects the required Merlin interpreter and forwards ordinary
pytest arguments to the Python launcher's focused mode:

```powershell
.\tools\run_pytest.ps1 -q tests\test_benchmark_grid_v2.py
.\tools\run_pytest.ps1 -q tests\v2
.\tools\run_pytest.ps1 -PytestArgs @('-v', '-k', 'core_logger_console_handler_is_configured_once', 'tests/server')
```

Set `MERLIN_PYTHON` only when an alternate project interpreter is intentional.
Use `-KeepTemp` before pytest arguments to retain a successful run directory for
debugging. Failures and interruptions retain their run automatically. Successful
runs otherwise remove only their own run directory; shared caches persist.
Use the explicit `-PytestArgs` array for pytest switches: bare `-k` binds to
`KeepTemp` and bare `-v` to PowerShell's `Verbose`. The forwarding implementation
is unchanged; prefer the Python launcher for ordinary commands.

## Strategy Lab

Strategy Lab is a local, read-only-input research pipeline for certified V2
strategies. Its current commands cover run-spec validation, inventory,
resumable dataset generation, real-pack certification, deterministic analysis,
and fixed-capacity allocation:

```powershell
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m tools.strategy_lab.config tools\strategy_lab\runspecs\s06_bracket_mvp.json
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m tools.strategy_lab.generate --help
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m tools.strategy_lab.certify --help
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m tools.strategy_lab.analysis.cli --help
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m tools.strategy_lab.analysis.allocation_certify --help
```

See the [Strategy Lab manual](strategy_lab/README.md) for identity, isolation,
schema, resume, scope-unlock, analysis, allocation, and certification contracts.

## Pattern Lab

Pattern Lab is local, research-only tooling for testing hypotheses before a
strategy exists. Its implemented milestones are the data foundation (a stable
Parquet market-data pack, an explicit manifest, a one-way importer for the
historical prototype NPZ pack, and a fixed-interval reader/resampler with a
reproducible research input fingerprint), the collector (closed-bar 5m
collection from public exchange APIs, recoverable updates, cooperative process
exclusion and collector/instrument-rule provenance) and the descriptive event
study (a versioned study request and protocol, extensible
feature/hypothesis/model/metric contracts, fixed-horizon and path outcomes,
immutable evidence, a regenerable offline HTML report and a bounded spawn pool
for its per-instrument jobs) and matched comparisons with calibrated inference
(M3a: offline analysis of a completed study into its own sealed artifact, with
matched control populations, event-weighted estimates, monthly jackknife in
request v2 or the legacy joint calendar block bootstrap in explicit v1, one
declared Holm family and a standalone offline report). It
requires the existing Merlin project dependencies, including `pyarrow==22.0.0`
and, for ordinary v2 monthly analysis, `scipy==1.16.3`, all pinned in root
`requirements.txt`. Those two packages alone are not a standalone environment;
Pattern Lab never installs dependencies itself.

```bash
python -m tools.pattern_lab --help
python -m tools.pattern_lab collect --universe tools/pattern_lab/configs/universe.json \
    --data-root <new-pack> --start 2025-06-01T00:00:00Z --end latest-closed
python -m tools.pattern_lab update --data-root <pack> --end 2026-10-01T00:00:00Z
python -m tools.pattern_lab recover --data-root <pack>
python -m tools.pattern_lab abort-update --data-root <pack>
python -m tools.pattern_lab inspect --data-root <pack> --verify
python -m tools.pattern_lab slice --data-root <pack> --instrument OKX_LINK-USDT-SWAP \
    --start 2025-07-01T00:00:00Z --end 2026-07-01T00:00:00Z --timeframe-minutes 30
python -m tools.pattern_lab study --spec tools/pattern_lab/configs/example_study_two_green_30m.json \
    --data-root <pack> --output-root <new-run> --workers 1
python -m tools.pattern_lab study --spec tools/pattern_lab/configs/example_study_two_green_30m.json \
    --data-root <pack> --output-root <new-run-2> --workers 2
python -m tools.pattern_lab report --run-root <new-run>
python -m tools.pattern_lab study \
    --spec tools/pattern_lab/configs/example_study_two_green_pair_30m.json \
    --data-root <pack> --output-root <pair-run>
python -m tools.pattern_lab analyze --run-root <pair-run> \
    --spec tools/pattern_lab/configs/example_analysis_two_green_pair.json \
    --output-root <new-analysis>
python -m tools.pattern_lab analysis-report --analysis-root <new-analysis>
python -m tools.pattern_lab.analysis.calibration --output-root <external-temp-dir>
python -m tools.pattern_lab.analysis.calibration_monthly --output-root <new-external-smoke> \
    --fixtures 002_null_dependent_t5 102_null_causal_ohlc_v1 --attempts 2
python -m tools.pattern_lab.analysis.calibration_monthly --output-root <copied-archive> --summarize-only
```

Network access happens only inside `collect`, `update` and `recover`, through
unauthenticated public REST endpoints. Every pack-level operation, including a
read, takes the in-root `.pack-lock` guard, so a busy root is reported (exit `3`)
rather than queued and two independent readers conflict deliberately. Exit `4`
means a pending operation must be recovered or aborted first. Collector code
availability is not an operationally prepared market pack.

`calibration_monthly` is a separate research-only candidate driver on Windows
and Linux. The displayed subset command completes bounded replays but remains
INCOMPLETE (exit 2). Its full frozen plan is 17 entries / 19,400 attempts under
sampled resource guards; missing required peak resident/available physical memory
measurements stop generation. PASS (exit 0) requires all main gates, complete
mandatory disclosures, actual refusals and both named replay checks. Offline
scoring needs no generator or memory API, preserves producer versus verifier
attribution, and labels historical source/replay limitations. Copy archives and
save their original summaries before re-scoring; the command replaces top-level
summaries. The [candidate guide](pattern_lab/README.md#the-experimental-monthly-jackknife-candidate)
owns the format, optional checksum and platform contracts. The numerical method
is integrated into ordinary v2 analysis; this research driver remains separate.
M3a is accepted at `9fe7816` for its documented approximate-screen scope.

`study` accepts any positive `--workers`, default `1`; above `1` the same
instrument job runs in an explicit spawn pool bounded by the selected instrument
count, and the canonical evidence and identities match the direct run.
`--output-root` is exactly the new run directory. Exit `130` reports a user
interrupt of `study`, `report`, `analyze` or `analysis-report`.

`analyze` reads one completed study strictly, never modifies it, and writes one
sealed analysis artifact into a new output root; it takes no worker count, and a
completed artifact exits `0` even when every comparison lacks inferential
support. `analysis-report` re-renders a sealed analysis from that artifact alone,
without the study or its pack. The tracked example uses v2 monthly jackknife;
explicit v1 keeps its failed bootstrap calibration (11/15 checks, about 7.5-8.3%
error on persistent-signal fixtures). Monthly inference passed the synthetic
G=12 screen against an 8% upper-bound envelope; it does not certify nominal 5%
control or arbitrary dependence across months. **M3a is accepted at `9fe7816`
for this approximate development-screen scope.** M3b context/frozen validation is
accepted at `908efb4` for its documented research scope and supported extension
routes: study v2 declares explicit context aliases,
`freeze-candidate` binds a completed development generation and monthly family,
and `validate-candidate` executes its exact later interval into a new parent root.
Repeat `--extension-root MODULE=LOCAL_DIRECTORY` to relocate declared main/helper
files while preserving the candidate and exact required bytes. Use a fresh process;
the API accepts an optional `extension_roots` map of absolute directories.
See the [M3b contract](pattern_lab/README.md#explicit-context-and-frozen-validation-m3b).
The M4 sequential ATR bracket probe is accepted at `c26334e` through the same
study/report CLI and direct/spawn jobs. It saves checked independent-account
attempts, trades and paths with frozen quantity rules and configurable entry cap;
see the [bracket guide](pattern_lab/README.md#sequential-atr-bracket-probes-m4).
M5 integrated workflow is accepted at `d423016` within the guide's documented
scope and the identity-portability limits of that generation. T09's prospective
policy-2 identities and verified extension relocation remain pending owner review.
Its tracked
`configs/pilot_m5_study.json` and `configs/pilot_m5_analysis.json` freeze the
44-target development workload. `examples/pilot_extension.py` supplies the
causal prior-high breakout and descriptive downside RMS;
`examples/rank_bracket_accounts.py` demonstrates checked offline account ranking.
See the [agent quick start](pattern_lab/README.md#agent-quick-start-m5).
Resume remains deferred.
Bracket inference and frozen-candidate validation are unsupported. Neither an M2 descriptive
result nor an M3a nominal Holm rejection is a validated edge. Merlin and Strategy Lab do not
import Pattern Lab, and ordinary Merlin CSV behavior is unchanged. See the
[Pattern Lab guide](pattern_lab/README.md) for the schema, manifest, roster,
adapters, identity encoding, exclusion/recovery contract, the event-study
request/evidence/report and worker-pool contracts, the M3a request, estimator,
support gates, declared family, sealed artifact and calibration, and the
operational recipes.

## Related documentation

- [Test workflow](../tests/README.md)
- [V2 architecture](../docs/engine_v2/ARCHITECTURE.md)
- [Performance evidence](../docs/engine_v2/PERFORMANCE.md)
