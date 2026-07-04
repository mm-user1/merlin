# Backtester V2 Profile Certification Registry

This registry tracks which Backtester V2 execution profile features have been
certified against external references. A mode absent from this file is not yet
certified for Python-native strategy trust.

## Fields

- Profile feature set
- Covered modes
- Certifying strategy
- Golden dataset hash/path
- TradingView settings/export reference
- Certification date
- Documented approximations
- Current status

## Registry

| Profile feature set | Covered modes | Certifying strategy | Golden dataset | TradingView reference | Date | Approximations | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Canonical Phase 1 bracket profile: next-open market entries, strict final-bar close, zero slippage, default TradingView OHLC path, risk-per-trade sizing, ATR/swing stops, RR targets | `entryOrder=market_next_open`, `stop=atr_swing`, `target=rr`, `trail=none`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close` | `s06_r_trend_v02_b2` | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`) | `data/baseline_v2/s06_r_trend_v02/reference_b_trend_bracket/` | 2026-07-04 | No Bar Magnifier, no slippage, no lower-timeframe reconstruction. TradingView export prices are 4-decimal display values; Merlin keeps full float stop/target levels. Three Reference B exported sizes differ by one `0.01` contract step from full-balance float sizing, while timestamps, directions, supported rounded metrics, and Merlin DD convention match. | Phase 1 bracket path certified for the Python reference kernel |
| Canonical Phase 1 MA-trail profile: next-open market entries, strict final-bar close, zero slippage, default TradingView OHLC path, risk-per-trade sizing, ATR/swing stops, MA trail | `entryOrder=market_next_open`, `stop=atr_swing`, `target=none`, `trail=ma`, `trailActivation=rr`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close` | `s06_r_trend_v02_b2` | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`) | `data/baseline_v2/s06_r_trend_v02/reference_a_reversal_trail/` | 2026-07-04 | Matches trade count, wins, and strict final close at `2025-12-01T00:00:00Z`. Uses Merlin/V1-style confirmed-bar trail approximation and full-balance float sizing; 35 of 61 exported trade sizes differ from Merlin, first at trade 11 (`24.36` vs `24.37`), maximum drift `0.03` contract units, last measured residual at trade 61 (`27.91` vs `27.93`). Merlin convention metrics are `net_profit_pct=30.9420054193`, `profit_factor=1.5088788696`, `max_drawdown_pct=13.4683032109`; TradingView UI shows `30.87`, `1.507`, and `14.15`. | Characterized with narrow residual; not fully TV-certified |

## Notes

Reference B uses Merlin's realized-balance drawdown convention:
`max_drawdown_pct=9.9211555042`. TradingView's UI drawdown for the same export
is `10.56%` because it uses an equity/open-excursion convention. Merlin metric
semantics are intentionally unchanged in Phase 1.

Reference A remains a documented trail-profile residual. The residual should be
revisited before declaring the MA-trail profile fully TradingView-certified, but
it must not be fixed by changing global Merlin metric semantics or degrading the
Reference B bracket behavior.
