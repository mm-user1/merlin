# S06 R-Trend v02 Regime-TL Baseline V2

This directory contains TradingView reference assets for the first Merlin
Backtester V2 pilot strategy import:
`S_06 R-Trend v02 Regime-TL`.

The pilot isolates the new trendline regime entry filter. The two references
use identical Trend + Bracket settings and differ only by `useRegime`.

## Timezone Policy

TradingView screenshots and `tradingview_trades.csv` exports use the chart
timezone shown in the UI: `UTC+8`. In this dataset, `2025-12-01 08:00` in
TradingView equals `2025-12-01T00:00:00Z`.

All machine-readable files created for automated tests use UTC:

- `dataset.json`
- `params.json`
- `tradingview_summary.json`
- `trades_normalized_utc.csv`

Use the raw TradingView CSV only as the immutable export artifact. Use
`trades_normalized_utc.csv` for automated parity checks.

## Canonical Period

- TradingView local start: `2025-08-01 08:00 UTC+8`
- TradingView local end: `2025-12-01 08:00 UTC+8`
- UTC start: `2025-08-01T00:00:00Z`
- UTC end: `2025-12-01T00:00:00Z`

The Pine source contains `close_all` after the date-filter range is left, but
this reference pair does not exercise a boundary close because all positions are
closed before the end date.

## TradingView Execution Profile

- Initial capital: `100 USDT`
- Default order size: `100 USDT`
- Pyramiding: `0`
- Bar detailization: `Default (4 ticks per bar)`
- Script execution: `On bar close`, `On order fill`, `On realtime bar tick`
- Order execution delay: `One tick`
- Commission: `0.05%`
- Slippage: `0 ticks`
- Limit order execution: `Requested price`
- Long leverage: `Infinity`
- Short leverage: `Infinity`
- Pine property: `process_orders_on_close=false`
- Pine property: `fill_orders_on_standard_ohlc=true`

## Shared Strategy Inputs

- Entry mode: `Trend @ Square`
- `useTrailMA`: `false`
- `stopX`: `2.0`
- `stopRR`: `3.0`
- `stopLP`: `2`
- `stopMaxPct`: `6.0`
- `stopMaxDays`: `4`
- `riskPerTrade`: `2.0`
- `contractSize`: `0.01`
- `trailRR`: `1.0` (inactive in bracket mode)
- `trailMAType`: `T3` (inactive in bracket mode)
- `trailMALength`: `150` (inactive in bracket mode)
- `trailMAOffsetPct`: `0.0` (inactive in bracket mode)
- `regimePivotLen`: `15`
- `regimeSlopeFactor`: `0.25`
- `regimeBreakBufferX`: `0.5`

## References

### reference_a_trend_square_bracket_no_regime

Control reference. The regime filter is disabled; this validates that the new
Pine strategy keeps the Trend + Bracket execution path stable when `useRegime`
is off.

- `useRegime`: `false`
- Total trades: `45`
- TradingView net profit: `11.35%`
- TradingView max drawdown: `15.02%`
- TradingView profit factor: `1.196`

### reference_b_trend_square_bracket_regime_tl

Primary pilot reference. The trendline regime filter is enabled and gates only
entries; exits are unchanged.

- `useRegime`: `true`
- Total trades: `43`
- TradingView net profit: `22.33%`
- TradingView max drawdown: `13.19%`
- TradingView profit factor: `1.405`

## File Roles

- `pine/`: Pine source used to generate TradingView references.
- `tradingview_trades.csv`: raw TradingView export, kept in TradingView order
  and timezone.
- `trades_normalized_utc.csv`: one row per closed trade, UTC timestamps,
  generated from `tradingview_trades.csv`.
- `tradingview_inputs_01.PNG`, `tradingview_inputs_02.PNG`: UI screenshots for
  strategy inputs.
- `tradingview_properties.PNG`: UI screenshot for strategy properties.
- `tradingview_metrics.PNG`: UI screenshot for Strategy Tester metrics.
- `params.json`: machine-readable strategy inputs transcribed from the inputs
  screenshots.
- `tradingview_summary.json`: machine-readable metrics transcribed from the
  metrics screenshot and cross-checked against the raw trades export.
