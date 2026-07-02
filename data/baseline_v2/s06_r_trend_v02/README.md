# S06 R-Trend v02 Baseline V2

This directory contains TradingView reference assets for the Merlin Backtester V2
S06 R-Trend v02 parity target.

The assets are intended to be a stable source of truth for engine changes. Raw
TradingView exports and screenshots are kept unchanged. Machine-readable files
normalize timestamps and settings so tests can consume them directly.

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

The Pine source used for these references is
`pine/S_06-R-Trend_v02_end-close.pine`. It force-closes any open position on the
end-date bar, matching Merlin's strict final-bar close behavior for a bounded
backtest/WFA window.

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

## References

### reference_a_reversal_trail

Trail mode reference. This case exercises the MA trail execution path and the
strict end-date close.

- Entry mode: `Reversal @ Triangle`
- `useTrailMA`: `true`
- Total trades: `61`
- TradingView net profit: `30.87%`
- TradingView max drawdown: `14.15%`
- TradingView profit factor: `1.507`

### reference_b_trend_bracket

Bracket mode reference. This is the cleaner primary parity target because the
last trade closes before the final boundary.

- Entry mode: `Trend @ Square`
- `useTrailMA`: `false`
- Total trades: `48`
- TradingView net profit: `25.87%`
- TradingView max drawdown: `10.56%`
- TradingView profit factor: `1.438`

## File Roles

- `pine/`: Pine sources used to generate TradingView references.
- `tradingview_trades.csv`: raw TradingView export, kept in TradingView order
  and timezone.
- `trades_normalized_utc.csv`: one row per closed trade, UTC timestamps,
  generated from `tradingview_trades.csv`.
- `tradingview_inputs.PNG`: UI screenshot for strategy inputs.
- `tradingview_properties.PNG`: UI screenshot for strategy properties.
- `tradingview_metrics.PNG`: UI screenshot for strategy tester metrics.
- `params.json`: machine-readable strategy inputs transcribed from the inputs
  screenshot.
- `tradingview_summary.json`: machine-readable metrics transcribed from the
  metrics screenshot and cross-checked against the raw trades export.
