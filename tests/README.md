# Tests

Pytest test suite for the Merlin backtesting platform.

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_regression_s01.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Test Files

| File | Purpose |
|------|---------|
| `conftest.py` | Shared fixtures: isolated storage, Flask test client, CSV roots config |
| `test_sanity.py` | Infrastructure sanity checks (imports, directories, Python version) |
| `test_regression_s01.py` | S01 baseline regression - validates results match saved baseline |
| `test_s01_migration.py` | S01 migration validation - ensures migrated strategy works correctly |
| `test_s03_reversal_v10.py` | S03 Reversal v10 strategy tests |
| `test_s04_stochrsi.py` | S04 StochRSI strategy tests |
| `test_metrics.py` | Metrics calculation tests (BasicMetrics, AdvancedMetrics) |
| `test_export.py` | CSV export functionality tests |
| `test_indicators.py` | Technical indicator tests (MA types, ATR, RSI) |
| `test_naming_consistency.py` | camelCase naming guardrails - prevents snake_case parameters |
| `test_walkforward.py` | Walk-forward analysis tests |
| `test_adaptive_wfa.py` | Adaptive WFA trigger detection tests (CUSUM, drawdown, inactivity) |
| `test_server.py` | HTTP API endpoint tests |
| `test_storage.py` | Database storage tests |
| `test_db_management.py` | Multi-database management tests (list, create, switch active DB) |
| `test_post_process.py` | Post-process module tests (FT, DSR, stress test) |
| `test_dsr.py` | Deflated Sharpe Ratio calculation tests |
| `test_oos_selection.py` | OOS candidate selection tests |
| `test_stress_test.py` | Stress test module tests |
| `test_analytics.py` | Analytics equity aggregation tests (portfolio curves, annualized profit) |
| `test_multiprocess_score.py` | Multi-process scoring tests |
| `test_optuna_sanitization.py` | Optuna sanitization tests |
| `test_score_normalization.py` | Score normalization tests |
| `test_coverage_startup.py` | Initial Search Coverage mode tests |
| `test_strategy_loop_regression.py` | Strategy loop performance regression tests |

## Test Categories

### Sanity Tests
Quick infrastructure checks. Run first to verify environment:
```bash
pytest tests/test_sanity.py -v
```

### Regression Tests
Validate strategy results against saved baselines:
```bash
pytest tests/test_regression_s01.py -v
```

If regression tests fail after intentional changes, regenerate baseline:
```bash
python tools/generate_baseline_s01.py
```

### Strategy Tests
Test individual strategy implementations:
```bash
pytest tests/test_s01_migration.py tests/test_s03_reversal_v10.py tests/test_s04_stochrsi.py -v
```

### Naming Guardrails
Ensure camelCase convention is maintained:
```bash
pytest tests/test_naming_consistency.py -v
```

## Baseline Data

Regression baselines stored in `data/baseline/`:
- `s01_metrics.json` - Expected S01 metrics
- `s01_trades.csv` - Expected S01 trade list

## Adding Tests for New Strategies

1. Create `tests/test_<strategy_id>.py`
2. Include tests for:
   - Basic execution (strategy runs without error)
   - Expected results validation (against PineScript reference)
   - Edge cases (empty data, extreme parameters)
3. Run with: `pytest tests/test_<strategy_id>.py -v`
