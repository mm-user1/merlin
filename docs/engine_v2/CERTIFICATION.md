# Backtester V2 Profile Certification Registry

This registry tracks which Backtester V2 execution profile features have been
certified against external references. A mode absent from this file is not yet
certified for Python-native strategy trust.

Phase 1.5 adds a shared balance-based V2 metric parity layer, deterministic run
checks, prefix/window-start anti-repainting checks, and an opt-in
TradingView-compatible outward tick-rounding mode for computed order levels.

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
| Canonical Phase 1 bracket profile, default no-rounding: next-open market entries, strict final-bar close, zero slippage, default TradingView OHLC path, risk-per-trade sizing, ATR/swing stops, RR targets | `entryOrder=market_next_open`, `stop=atr_swing`, `target=rr`, `trail=none`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close`, `priceRounding=none` | `s06_r_trend_v02_b2` | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02/reference_b_trend_bracket/` | 2026-07-05 | Default mode intentionally keeps full-float computed stop/target levels. TradingView export prices are 4-decimal display/order-grid values; three exported sizes differ by one `0.01` contract step from full-balance float sizing. Frozen Merlin-convention metrics remain `48` trades, `21` wins, `net_profit_pct=25.87` rounded, `profit_factor=1.438` rounded, `max_drawdown_pct=9.9211555042`. | Default no-rounding bracket path certified and preserved |
| Canonical Phase 1 MA-trail profile, default no-rounding: next-open market entries, strict final-bar close, zero slippage, default TradingView OHLC path, risk-per-trade sizing, ATR/swing stops, MA trail | `entryOrder=market_next_open`, `stop=atr_swing`, `target=none`, `trail=ma`, `trailActivation=rr`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close`, `priceRounding=none` | `s06_r_trend_v02_b2` | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02/reference_a_reversal_trail/` | 2026-07-05 | Default mode intentionally keeps full-float computed stop/trail levels. It matches trade count, wins, and strict final close at `2025-12-01T00:00:00Z`; exported size residuals remain the preserved default-mode characterization. Frozen Merlin-convention metrics remain `net_profit_pct=30.9420054193`, `profit_factor=1.5088788696`, `max_drawdown_pct=13.4683032109`. | Default no-rounding trail path characterized and preserved |
| Canonical Phase 1 bracket profile with outward tick-rounded order levels | `entryOrder=market_next_open`, `stop=atr_swing`, `target=rr`, `trail=none`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close`, `priceRounding=tick_outward`, `tickSize=0.0001` | `s06_r_trend_v02_b2` | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02/reference_b_trend_bracket/` | 2026-07-05 | Rounds only placed stop/target levels outward after raw risk, size, and `stopMaxPct` checks. No OHLC, market entry, max-days close, boundary close, anchor, ATR, MA, or signal value is rounded. Frozen Merlin-convention values: `48` trades, `21` wins, `43.75%` win rate, `net_profit_pct=25.8746180135`, `profit_factor=1.4379099877`, `max_drawdown_pct=9.9271828348`. | Tick-outward bracket path certified trade-for-trade against the committed TradingView export |
| Canonical Phase 1 MA-trail profile with outward tick-rounded order levels | `entryOrder=market_next_open`, `stop=atr_swing`, `target=none`, `trail=ma`, `trailActivation=rr`, `sizing=risk_per_trade`, `margin=report_only/off`, `boundary=strict_close`, `priceRounding=tick_outward`, `tickSize=0.0001` | `s06_r_trend_v02_b2` | `data/baseline_v2/s06_r_trend_v02/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02/reference_a_reversal_trail/` | 2026-07-05 | Rounds only placed stops and trailing stop/band levels outward after raw risk, size, and `stopMaxPct` checks. The preserved default no-rounding residual is resolved in this certification profile. Frozen Merlin-convention values: `61` trades, `31` wins, `50.82%` win rate, `net_profit_pct=30.8652320330`, `profit_factor=1.5073481143`, `max_drawdown_pct=13.4921966575`. | Tick-outward trail path certified trade-for-trade against the committed TradingView export |

## Phase 1.5 Coverage

The V2 metric helper `compute_core_metrics_from_balance_and_trades(...)` is
explicitly balance-based and covered against `core.metrics` for:
`start_balance`, `final_balance`, `net_profit`, `net_profit_pct`,
`total_trades`, `winning_trades`, `losing_trades`, `win_rate_pct`,
`gross_profit`, `gross_loss`, `profit_factor`, `max_drawdown_pct`,
`max_drawdown`, and `romad`. Undefined optional numeric results use `nan`;
canonical infinite profit factor uses `inf`.

Determinism tests cover repeated in-process V2 baseline runs, compact guardrail
summaries, final standing state, and a small threaded harness through the public
`S06RTrendV02B2.run()` path. The threaded test verifies repeatability of
isolated runs and cached config/profile access; it is not a Grid V2 worker
scheduling test.

Anti-repainting tests cover signal prefix invariance with appended future bars,
closed-decision prefix invariance before the cutoff, and warmup-window
invariance using the pinned 1000-bar recipe against a larger prefix. The
window-start test requires exact trade skeleton and exact contract-rounded
sizes, but uses `1e-9` relative tolerance for prices and PnL because EMA/RMA and
rolling calculations can have ULP-level start-dependent differences.

## Notes

The tick-rounding API is:

- default: `priceRounding="none"`;
- certified TradingView-compatible mode: `priceRounding="tick_outward"`;
- active tick mode requires positive `tickSize`;
- `tickSize=0.0001` is pinned in the SUIUSDT.P baseline metadata.

Outward rounding means levels below market are floored to the tick grid and
levels above market are ceiled. For the certified profiles, long stops/trails
and short targets are below-market levels; short stops/trails and long targets
are above-market levels.

Reference B and Reference A use Merlin's realized-balance drawdown convention.
TradingView UI drawdown can differ when it uses an equity/open-excursion
convention. Merlin does not duplicate that TradingView drawdown convention in
Phase 1.5.

No slippage, Bar Magnifier, lower-timeframe reconstruction, Grid V2, Scout WFA,
or V1 runtime migration is certified by this registry.
