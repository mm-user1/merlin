# S07 HA v01 TradingView baselines

Owner-supplied evidence checked on 2026-10-27. Both references are internally
consistent and suitable as external inputs for the planned Backtester V2 import.
This is not a claim of completed Merlin parity or strategy profitability.
Original CSV files and screenshots remain unchanged.

## Shared setup

- Market: OKX:ETHUSDT.P, 30m, ordinary OHLC execution.
- Trading interval: `[2025-03-01T00:00:00Z, 2026-10-01T00:00:00Z)`.
  Inputs show 08:00 on both dates in the owner's UTC+8 timezone.
- Report calculation range shown: January 1, 2025 through October 27, 2026.
  The enabled strategy Date Filter restricts trading to the interval above.
- Long and Short enabled; HA Run Length 6; Show HA Candles off.
- Stop X 2, Stop RR 3, Stop LP 2, Stop Max Days 4.
- Risk Per Trade 2%, Max Entry Leverage 8, Contract Size 0.001.
- Initial capital 10,000 USDT; default cash order size 10,000 USDT;
  pyramiding 0; commission 0.05% per side; slippage 0.
- Default four-tick bar detailization, on-bar-close script execution,
  requested-price limit execution and one-tick order delay.
- Broker long/short leverage Infinity is distinct from the script's entry cap 8.
- Both Properties images are byte-identical. Each reference contains its own
  Inputs, Properties and metrics screenshot; the empty common directory is unused.

Reference A enables the ordinary-candle entry filter using OKX:BTCUSDT.P,
4h, Trend Confirm Bars 2 and Trend Break Bars 1. Reference B disables that filter;
the other visible inputs are equal.

## Results

| Reference | Closed trades | Long / Short | Wins | Net PnL USDT | Net % | Profit Factor | TV maximum drawdown |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A: filter on | 253 | 129 / 124 | 68 (26.88%) | -5,282.47 | -52.82% | 0.831 | 7,007.68 USDT / 60.08% |
| B: filter off | 363 | 196 / 167 | 110 (30.30%) | -1,766.93 | -17.67% | 0.969 | 6,484.12 USDT / 44.33% |

Drawdown is transcribed from TradingView screenshots, not independently reproduced
from closed trades. Preserve the platform's definition when comparing Merlin.

## Verification

Read-only CSV and local Parquet inspection used the configured Windows Python.
No strategy simulation, market download or reference regeneration was performed.

- A contains 506 CSV rows; B contains 726. Every consecutive trade ID has exactly
  one entry and one exit, with matching side and quantity.
- All entries/exits are ordered, within the trading interval, on the 30m grid,
  with no overlapping positions. Quantities are positive multiples of 0.001 ETH.
- All 616 entry prices exactly match the corresponding UTC bar opens in
  `data/parquet-data/ohlcv/OKX_ETH-USDT-SWAP_5m.parquet` after converting exports
  from UTC+8. This supports the expected instrument/time interpretation; it is
  not a comparison of all OHLC candles or unexecuted signals.
- Counts, profitable counts, final cumulative PnL and rounded Profit Factor agree
  with the screenshots. Trade duration fields equal elapsed 30m bars.
- Reconstructing net PnL from exported prices and quantities, less 0.05% per-side
  fees, gives -5282.471147605 USDT for A and -1766.925219165 USDT for B.
  Both round to the screenshot totals. Maximum absolute per-trade / cumulative
  residuals against exported values are 0.004991360 / 0.004998245 USDT for A and
  0.005006920 / 0.005257475 USDT for B. These small export-precision residuals
  are recorded, not treated as proof of identical hidden precision.
- A has 24 Max Days exits and B has 44; every such holding duration is 96.5 hours,
  consistent with the agreed next-open expiry semantics.
- B includes one End Close, labeled September 30, 2026 at 23:30 UTC (October 1
  07:30 UTC+8), the opening timestamp of the last 30m bar before the end boundary.
  Preserve this raw label separately from the semantic closing boundary.

## Provenance and comparison boundary

The frozen Pine authority is [S_07 HA v01.pine](pine/S_07%20HA%20v01.pine), copied
byte-for-byte on 2026-10-28 from `S_07 HA/S_07 HA v01.pine` in the sibling
Strategies repository (40,771 bytes). Source and copy were hash-verified.
T06-1 v3 SHA-256:
`dda5010d2b597d75bf87d7c1370f22c530670ec75727309283fcc6cfae616413`.
Screenshots and exports do not independently authenticate TradingView editor bytes.
The owner's preceding verification establishes that the display toggle leaves
calculations unchanged; these exports have that toggle off.

Retain the previously documented Pine/Merlin differences: planned signal-close
entry leverage versus actual fill-price admission after entry fees, platform price
grid behavior, and terminal-exit timestamp labeling. The earlier September 2025
trade-level comparison remains in `docs/_work/dev_04_merlin/T06_tv-comparison.md`.
Do not assume its fixture-specific explanation proves parity for this longer run.

### Raw input SHA-256

| Reference / file | SHA-256 |
| --- | --- |
| A / tradingview_trades.csv | `0980b4a153f3e531b93437632af2883ab0e962af80eb42d9f32deaf985f8c2a0` |
| A / tradingview_inputs.PNG | `fba00fb0ca39ec26fbf343599fdc90432a0b8de806aea5b9265f10daee1e8d12` |
| A / tradingview_metrics.PNG | `b001dbb363d801863de7822265e8b73d114416f0e3a3d1ed487868262620d20d` |
| A and B / tradingview_properties.PNG | `44c69b8be17415de1ffc19c47c71aefa25c7d594b8b432b0fc5d1f3fc5251e63` |
| B / tradingview_trades.csv | `a458f5744cadd929a036a0e66d69e1249d15b7dc75260419c4bd4c56bfcdc134` |
| B / tradingview_inputs.PNG | `dd949056fc7c12ed27b656a4b1b3dab1b5100f21dc12068e1282fae07877b9ab` |
| B / tradingview_metrics.PNG | `9a4edb1e2787e0998787a52b23d080b0bf1f21da05905efe47fb1228105c5c0f` |
