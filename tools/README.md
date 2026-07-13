# Tools

Development and debugging utilities for the Merlin platform.

## Available Tools

### generate_baseline_s01.py

Generates regression test baseline for S01 strategy.

**When to use:** After intentional changes to S01 that affect results.

```bash
python tools/generate_baseline_s01.py
```

**Output:**
- `data/baseline/s01_baseline_metrics.json` - Metrics snapshot
- `data/baseline/s01_baseline_trades.csv` - Trade list

### benchmark_indicators.py

Performance benchmark for indicator calculations.

**When to use:** After modifying indicator implementations to verify performance.

```bash
python tools/benchmark_indicators.py
```

**Output:** Timing results for each MA type and ATR (100 iterations on 10,000 bars).

### benchmark_metrics.py

Performance benchmark for metrics calculations.

**When to use:** After modifying metrics module to verify performance.

```bash
python tools/benchmark_metrics.py
```

### benchmark_grid_v2.py

Grid V2 diagnostics and benchmark helper.

**When to use:** Before and after Grid V2 performance work to collect comparable
direct-grid timings, or to inspect saved WFA Grid metadata without rerunning WFA.

```bash
python tools/benchmark_grid_v2.py --help
python tools/benchmark_grid_v2.py inspect-wfa-db --db src/storage/2026-07-06_233217_backtester-v2-test.db
python tools/benchmark_grid_v2.py direct-grid --config tools/benchmark_configs/s06_b2_sui_baseline_grid.json --workers 1,6 --warmup-runs 1 --runs 2
```

Grid V2 candidate domains come from the strategy `config.json` optimize
metadata, `enabled_params`, and select `{param}_options` subsets. Numeric
`param_ranges` in the benchmark payload are kept for compatibility with the
canonical UI config builder; editing only those ranges does not independently
redefine V2 grid granularity.

`inspect-wfa-db` opens frozen benchmark DB snapshots with SQLite
`mode=ro&immutable=1`, which avoids read sidecars. Do not use that mode for a
live DB that may still have uncheckpointed WAL frames. Newer WFA Grid V2 rows
may include optional Phase 2.6.4 timing buckets and plan-reuse counters; older
rows without those fields still inspect as valid, and the stable diagnostics
status is based only on the original required timing keys.

### run_pytest.ps1

Windows pytest wrapper that uses the required Merlin Python by default and
redirects pytest temp directories into a repo-local `.pytest_tmp/run_<pid>`
directory.

```powershell
.\tools\run_pytest.ps1 -q tests\test_benchmark_grid_v2.py
.\tools\run_pytest.ps1 -q tests\v2
```

Set `MERLIN_PYTHON` to override the interpreter. Pass `-KeepTemp` before pytest
arguments to preserve the per-run temp directory for debugging.

### test_all_ma_types.py

Tests S01 strategy with all 11 MA types to ensure indicators work correctly.

**When to use:** After modifying MA indicators or S01 strategy logic.

```bash
python tools/test_all_ma_types.py
```

**Output:** Summary table showing profit/drawdown/trades for each MA type.

## Usage Notes

- All tools should be run from project root: `python tools/<script>.py`
- Tools add `src/` to Python path automatically
- Tools use the standard test data: `data/raw/OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv`
