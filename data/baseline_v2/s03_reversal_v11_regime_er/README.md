# S03 Reversal v11 Regime-ER Baseline V2

This directory contains TradingView reference assets for the Merlin
Backtester V2 pilot import of `S_03 Reversal v11 Regime-ER`.

The two references use identical S03 and Regime-ER inputs and differ only by
the optional Emergency SL switch.

## Timezone Policy

TradingView screenshots and `tradingview_trades.csv` exports use the chart
timezone shown in the UI: `UTC+8`. In this dataset, `2025-02-01 08:00` in
TradingView equals `2025-02-01T00:00:00Z`.

All machine-readable files created for automated tests use UTC:

- `dataset.json`
- `params.json`
- `tradingview_summary.json`
- `trades_normalized_utc.csv`

Use the raw TradingView CSV only as the immutable export artifact. Use
`trades_normalized_utc.csv` for automated parity checks.

## Canonical Period

- TradingView local start: `2025-02-01 08:00 UTC+8`
- TradingView local end: `2026-02-01 08:00 UTC+8`
- UTC start: `2025-02-01T00:00:00Z`
- UTC end: `2026-02-01T00:00:00Z`

The Pine source contains `strategy.close_all("Close")` after the date-filter
range is left. With TradingView's next-tick order processing, that close order
is created after the first out-of-range bar and fills on the next bar open.
Both references therefore have the final trade exit at
`2026-02-01 09:00 UTC+8` (`2026-02-01T01:00:00Z`), one hour after the canonical
end timestamp. This is expected and must be preserved for parity. Do not
truncate the market data at `end_utc`; the replay data must include the
post-end bars needed to fill the date-expiry close.

## TradingView Execution Profile

- Initial capital: `100 USDT`
- Default order size: `100% of equity`
- Pyramiding: `0`
- Bar detailization: `Default (4 ticks per bar)`
- Script execution: `On bar close`
- Order execution delay: `One tick`
- Commission: `0.05%`
- Slippage: `0 ticks`
- Limit order execution: `Requested price`
- Long leverage: `Infinity`
- Short leverage: `Infinity`

## Shared Strategy Inputs

- `dateFilter`: `true`
- `drawTester`: `true`
- `mptable_on`: `true`
- `maType3`: `SMA`
- `maLength3`: `75`
- `maOffset3`: `0.0`
- `useCloseCount`: `true`
- `closeCountLong`: `7`
- `closeCountShort`: `5`
- `useTBands`: `true`
- `tBandLongPct`: `1.0`
- `tBandShortPct`: `1.3`
- `contractSize`: `0.01`
- `useRegime`: `true`
- `regimeErLength`: `30`
- `regimeErThresh`: `0.40`
- `emergencySlPct`: `10.0`
- `emergencySlUpdateBars`: `16`

## References

### reference_a_no_emergency_sl

Control reference. Regime-ER is enabled and Emergency SL is disabled.

- `useEmergencySL`: `false`
- Total trades: `151`
- Winning trades: `63`
- TradingView net profit: `150.89%`
- TradingView max drawdown: `19.80%`
- TradingView profit factor: `1.801`
- Emergency SL exits: `0`

### reference_b_emergency_sl_10

Primary Emergency SL reference. Regime-ER is enabled and Emergency SL is
enabled at `10%`.

- `useEmergencySL`: `true`
- `emergencySlPct`: `10.0`
- Total trades: `152`
- Winning trades: `63`
- TradingView net profit: `154.02%`
- TradingView max drawdown: `20.84%`
- TradingView profit factor: `1.802`
- Emergency SL exits: `1`

## File Roles

- `pine/`: Pine source used to generate TradingView references.
- `tradingview_trades.csv`: raw TradingView export, kept in TradingView order
  and timezone.
- `trades_normalized_utc.csv`: one row per closed trade, UTC timestamps,
  generated from `tradingview_trades.csv`.
- `tradingview_inputs.PNG`: UI screenshot for strategy inputs.
- `tradingview_properties.PNG`: UI screenshot for strategy properties.
- `tradingview_metrics.PNG`: UI screenshot for Strategy Tester metrics.
- `params.json`: machine-readable strategy inputs transcribed from the inputs
  screenshots.
- `tradingview_summary.json`: machine-readable metrics transcribed from the
  metrics screenshot and cross-checked against the raw trades export.
