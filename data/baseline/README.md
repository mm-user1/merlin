# S01 Trailing MA v26 - Baseline Data

## Overview

This directory contains baseline results for the S01 Trailing MA strategy.
These results serve as the "golden standard" for regression testing during migration.

**Generated:** 2025-12-25 10:01:00

## Dataset

- **File:** `data/raw/OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv`
- **Symbol:** OKX_LINKUSDT.P
- **Timeframe:** 15 minutes
- **Full Range:** 2025-05-01 to 2025-11-20

## Backtest Configuration

### Date Range
- **Start:** 2025-06-15 00:00:00
- **End:** 2025-11-15 00:00:00
- **Warmup Bars:** 1000

### Strategy Parameters

**Main MA:**
- Type: SMA
- Length: 300

**Entry Logic:**
- Close Count Long: 9
- Close Count Short: 5

**Stop Loss (Long):**
- ATR Multiplier: 2.0
- Risk/Reward: 3
- Lookback Period: 2
- Max %: 7.0%
- Max Days: 5

**Stop Loss (Short):**
- ATR Multiplier: 2.0
- Risk/Reward: 3
- Lookback Period: 2
- Max %: 10.0%
- Max Days: 2

**Trailing Stops:**
- Long Trail RR: 1
- Trail MA Type: EMA
- Long Trail Length: 90
- Long Trail Offset: -0.5%
- Short Trail RR: 1
- Short Trail Length: 190
- Short Trail Offset: 2.0%

## Expected Results

Based on user requirements, the baseline should produce:

- **Net Profit:** 230.75% (Expected: ~230.75% �0.5%)
- **Max Drawdown:** 20.03% (Expected: ~20.03% �0.5%)
- **Total Trades:** 93 (Expected: ~93 �2)

## Tolerance Levels for Regression Tests

The following tolerances are used for regression validation:

- **net_profit_pct:** �0.01% (floating point tolerance)
- **max_drawdown_pct:** �0.01%
- **total_trades:** exact match (�0)
- **trade entry/exit times:** exact match
- **trade PnL:** �0.0001 (floating point epsilon)

## Files

- `s01_metrics.json` - Basic and advanced metrics (incl. `consistency_score`)
- `s01_trades.csv` - All trade records
- `README.md` - This file

## Usage

The regression test (`tests/test_regression_s01.py`) loads these baseline results
and compares them against the current implementation to ensure no behavioral changes
during migration.
