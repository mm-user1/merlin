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
reproducible research input fingerprint) and the collector (closed-bar 5m
collection from public exchange APIs, recoverable updates, cooperative process
exclusion and collector/instrument-rule provenance). It requires
`pyarrow==22.0.0` from the root `requirements.txt` and never installs
dependencies itself.

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
```

Network access happens only inside `collect`, `update` and `recover`, through
unauthenticated public REST endpoints. Every pack-level operation, including a
read, takes the in-root `.pack-lock` guard, so a busy root is reported (exit `3`)
rather than queued and two independent readers conflict deliberately. Exit `4`
means a pending operation must be recovered or aborted first. Collector code
availability is not an operationally prepared market pack.

Feature, hypothesis, evaluation-model, bracket-probe and report commands are not
implemented. Merlin and Strategy Lab do not import Pattern Lab, and ordinary
Merlin CSV behavior is unchanged. See the
[Pattern Lab data guide](pattern_lab/README.md) for the schema, manifest,
roster, adapters, identity encoding, exclusion/recovery contract and the
operational recipes.

## Related documentation

- [Test workflow](../tests/README.md)
- [V2 architecture](../docs/engine_v2/ARCHITECTURE.md)
- [Performance evidence](../docs/engine_v2/PERFORMANCE.md)
