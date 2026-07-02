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
| Canonical Phase 1 profile: next-open market entries, strict final-bar close, zero slippage, default TradingView OHLC path, risk-per-trade sizing, ATR/swing stops, RR targets, MA trail profile variants | `entryOrder=market_next_open`, `stop=atr_swing`, `target=rr/none`, `trail=ma/none`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close` | `s06_r_trend_v02` baseline package | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256`) | `data/baseline_v2/s06_r_trend_v02/reference_b_trend_bracket/` primary, `data/baseline_v2/s06_r_trend_v02/reference_a_reversal_trail/` secondary | Pending Phase 1 kernel certification | No Bar Magnifier, no slippage, no lower-timeframe reconstruction; entry-fill-bar trail behavior follows the current Merlin/S06 confirmed-bar approximation | Baseline assets prepared; V2 execution not certified yet |

## Notes

Phase 0 only validates the prepared baseline assets and pins profile contracts.
Trade-for-trade certification starts when the V2 execution kernel and adapter
exist in Phase 1.
