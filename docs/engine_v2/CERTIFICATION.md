# Backtester V2 Profile Certification Registry

This registry tracks which Backtester V2 execution profile features have been
certified against external references. A mode absent from this file is not yet
certified for Python-native strategy trust.

This is an evidence registry, not the current engine specification. Read the
[V2 architecture](ARCHITECTURE.md) for stable execution contracts and
[metrics authority](../METRICS.md) for metric definitions. Exact values,
tolerances, hashes, residuals, and dates below are preserved certification
evidence.

Phase 1.5 adds a shared balance-based V2 metric parity layer, deterministic run
checks, prefix/window-start anti-repainting checks, and an opt-in
TradingView-compatible outward tick-rounding mode for computed order levels.
Phase 2 adds generic Grid V2 planning. Phase 2.5 integrates Grid V2 into the
normal Grid dispatcher/storage workflow and adds a generic compiled batch
evaluator for supported V2 execution profiles. The later TZ61-TZ64 sequence
completes ordinary V2 WFA runtime integration; Scout WFA remains deferred.
Phase 2.5.1 tightens dispatcher/storage behavior, runtime Grid
settings, and compiled batch determinism without changing V1 runtime paths.
Phase 2.6.2 changes the compiled Grid V2 plumbing to a generic stacked batch
path while preserving the reference runner and grouped compiled path as
certification oracles. Phase 2.6.3 moves Grid V2 planning/execution onto a
typed candidate table while keeping legacy candidates as a lazy compatibility
surface.
The TZ37 follow-up adds metadata-driven internal execution variants and same-role
boolean dependency collapse for S03-like Grid V2 planning without changing V1
runtime paths.

## Synthetic risk-bracket admission coverage (2026-10-26)

`risk_bracket_v1` has generic-engine synthetic coverage in
`tests/v2/test_v2_entry_admission.py`, `test_v2_admission_grid.py`,
`tests/test_entry_admission_integration.py`, `test_entry_admission_adaptive.py`,
`tests/server/test_entry_admission.py` and `tests/js/entry_admission.test.cjs`.
The importable test-only strategy is registered explicitly in parent/spawned test
processes and is absent from normal discovery. This is not Pine/PLH parity,
production-strategy certification or evidence of market edge.

Coverage includes independent IEEE/decimal boundaries, long/short/gap fills,
fee/cap precedence, exact adjacent cap values, defensive failures, both packers,
full/sampled Grid, selected Slow, separate legacy/new plans, one/two compiled
threads, actual spawned FT/Stress with a locked source, strict replay and current
manual rules, non-100 capital, Fixed/Adaptive delayed/rejected OOS, retained-trade
and inclusive-cutoff diagnostics, persistence and UI scope. Existing historical
certification rows and identity pins below remain preservation authorities.
Owner browser appearance and platform-specific evidence are separate review gates;
Node assertions are not visual acceptance.

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
| Generic stateful-trail engine foundation | `trail=r_distance/chandelier/fixed_af_sar`, `target=none`, `trailActivation=rr`, both `priceRounding` modes | Test-only deterministic long/short harness; no Phase-2 strategy import | Synthetic OHLC sequences in `tests/v2/test_v2_kernel_execution.py` and `tests/v2/test_v2_stateful_trails.py` | Stable contract in `docs/engine_v2/ARCHITECTURE.md`; Pine source used only as formula provenance | 2026-08-27 | Engine certification covers exact High/Low activation, fill-price break-even, close-only future effectiveness, strict first-candidate side validity before rounding, ratcheting, Chandelier ATR warmup/current-row transport, trade-local fixed-AF SAR recurrence/caps, resets, reference/compiled parity, mixed batches, and one/two-worker determinism. This foundation does not certify or import S06 v06-4-A2 and adds no external strategy golden dataset. Existing `none` and `ma` outputs remain preservation oracles. | Generic reference and compiled/Grid engine foundation certified; strategy certification deferred to Phase 2 |
| Pilot import: S06 Regime-TL no-regime control (Trend @ Square + bracket, default no-rounding) | `entryOrder=market_next_open`, `stop=atr_swing`, `target=rr`, `trail=none`, `sizing=risk_per_trade`, `margin=report_only`, `boundary=strict_close` (not exercised), `priceRounding=none`, `useRegime=false` | `s06_r_trend_v02_regime_trendlines_b2` | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/reference_a_trend_square_bracket_no_regime/` | 2026-07-12 | Exact trade skeleton (45 trades, directions, UTC entry/exit timestamps, exact entry prices); exit prices within one exported tick; 16 of 45 sizes differ by one `0.01` contract step (TV export rounding). Frozen Merlin-convention metrics: `45` trades, `16` wins, `35.56%` win rate, `net_profit_pct=11.3836924235` (TV displays `11.35`; 2-decimal per-trade export rounding accumulation), `profit_factor=1.1966147730` (TV displays `1.196`; `round(pf,3)` does not reproduce the TV display for this baseline), `max_drawdown_pct=13.8496161888` (TV `15.02` uses the equity/open-excursion convention). This baseline does not exercise the final-boundary close; strict boundary logic stays covered by the S06 v02 baseline. | No-regime control certified trade-for-trade against the committed TradingView export |
| Pilot import: S06 Regime-TL trendline regime filter (Trend @ Square + bracket, entry gating only) | Same execution modes as the control row plus signal-layer regime gating: `useRegime=true`, `regimePivotLen=15`, `regimeSlopeFactor=0.25`, `regimeBreakBufferX=0.5` | `s06_r_trend_v02_regime_trendlines_b2` | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/reference_b_trend_square_bracket_regime_tl/` | 2026-07-12 | Exact trade skeleton (43 trades); exit prices within one exported tick; 11 of 43 sizes differ by one `0.01` contract step. Frozen Merlin-convention metrics: `43` trades, `17` wins, `39.53%` win rate, `net_profit_pct=22.3484141712` (TV `22.33`), `profit_factor=1.4057224218` (TV displays `1.405`), `max_drawdown_pct=11.9928996219` (TV `13.19`, drawdown-convention residual). Regime gating affects entries only; the 1000-bar warmup recipe is test-locked as converged for the regime state machine on this baseline. Final-boundary close not exercised. | Trendline regime entry filter certified trade-for-trade against the committed TradingView export |
| Signal-reversal topology for S03-like strategies | `topology=signal_reversal`, `entryOrder=market_next_open`, `sizing=fixed_pct_equity`, `exitOnSignal=true`, `stop=none/emergency_pct`, `boundary=strict_close/none`, `priceRounding=none` | Test-only S03-like signal harness in `tests/v2/s03_like_test_helpers.py`; production pilot covered by the S03 Regime-ER rows below | Real gate: `data/raw/OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv`; structural gate: synthetic gapless OHLC in `tests/v2/test_v2_signal_kernel_s03_v11_gate.py`; compiled/Grid fixture gate in `tests/v2/test_v2_grid_signal_topology.py` | Local-only TradingView reference `docs/_work/S_03-v11_Update/reference_tv_s03_v11_emergency_sl_10pct.json`; it may be absent from a fresh clone and promotion to tracked baseline evidence is deferred | 2026-07-16 | V2 uses true next-open market fills while V1 S03 v11 uses close-fill approximation; exact V1/V2 trade equality is certified only on gapless synthetic data with the documented +1-bar timestamp map. Real SUI certification is metric-level against the TradingView JSON with V1 tolerances and exactly 12 `Emergency SL` exits. V2 deducts entry commission at fill time, so intra-trade balance/equity can differ from V1 while per-trade net PnL and final net profit match on gapless data. Compiled Grid uses a topology-specific stacked boolean payload and mapping config packing, not the vectorized table packer. | Reference and compiled/Grid fixture tiers certified; production pilot added separately |
| Pilot import: S03 Regime-ER without Emergency SL | `topology=signal_reversal`, `entryOrder=market_next_open`, `sizing=fixed_pct_equity`, `exitOnSignal=true`, `stop=none`, `boundary=strict_close`, `priceRounding=none`; signal-layer Regime-ER gates entries and emits flat exit arrays | `s03_reversal_v11_regime_er_b2` | `data/baseline_v2/s03_reversal_v11_regime_er/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s03_reversal_v11_regime_er/reference_a_no_emergency_sl/` | 2026-07-18 | Strict parity for trades 1..150 (direction, UTC fill timestamps, entry prices, exit prices, and current V2 exit-reason contract). The production B2 adapter truncates at `2026-02-01T00:00:00Z`, so trade 151 closes at `2026-02-01T00:00:00Z @ 1.1549`; TradingView's date-expiry `close_all` fills one hour later at `2026-02-01T01:00:00Z @ 1.1608`. An untruncated kernel proof matches that TradingView final fill and net profit (`150.8911260100%`, TV `150.89%`). Production-adapter Merlin metrics are pinned at `151` trades, `63` wins, `net_profit_pct=152.0521772455`, `profit_factor=1.8074539749`, `max_drawdown_pct=18.1492940138`; TV drawdown (`19.80%`) uses a different convention. | Production pilot certified with documented final-boundary residual |
| Pilot import: S03 Regime-ER with Emergency SL 10% | Same as the no-Emergency-SL row, with `stop=emergency_pct`, `useEmergencySL=true`, `emergencySlPct=10.0`, `emergencySlUpdateBars=16` | `s03_reversal_v11_regime_er_b2` | `data/baseline_v2/s03_reversal_v11_regime_er/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s03_reversal_v11_regime_er/reference_b_emergency_sl_10/` | 2026-07-18 | Strict parity for trades 1..151 except the single Emergency SL exit price is within one tick because TradingView exports the computed stop fill at 4 decimals (`3.0036` vs Merlin full-float `3.00355`). Trade 152 has the same production final-boundary residual as Reference A (`2026-02-01T00:00:00Z @ 1.1549` vs TradingView `2026-02-01T01:00:00Z @ 1.1608`). An untruncated kernel proof matches TradingView's final fill and net profit (`154.0231148748%`, TV `154.02%`). Production-adapter Merlin metrics are pinned at `152` trades, `63` wins, one `Emergency SL` exit, `net_profit_pct=155.1986873673`, `profit_factor=1.8077375729`, `max_drawdown_pct=19.2133837712`; TV drawdown (`20.84%`) uses a different convention. | Production pilot certified with documented final-boundary and export-rounding residuals |

## Phase 1.5 Coverage

The V2 metric helper `compute_core_metrics_from_balance_and_trades(...)` is
explicitly balance-based and covered against `core.metrics` for:
`start_balance`, `final_balance`, `net_profit`, `net_profit_pct`,
`total_trades`, `winning_trades`, `losing_trades`, `win_rate_pct`,
`gross_profit`, `gross_loss`, `profit_factor`, `max_drawdown_pct`,
`max_drawdown`, and `romad`. Undefined optional numeric results use `nan`;
canonical infinite profit factor uses `inf`.

Realized Max DD uses the complete finite realized `balance_curve`, including
flat warmup and the final observation. Percentage and absolute drawdown are
independent running-peak maxima, and RoMaD uses percentage DD. Advanced metric
boundaries do not truncate or seed this balance scan. This balance convention
is distinct from TradingView/open-excursion or future Strategy Lab MTM DD. All
three V1 Fast backends, the V2 Python reference, and both V2 compiled families
are parity-gated on the corrected full-path behavior.

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

## Phase 2.5 Grid V2 Status

Grid V2 entry points live in `src/core/grid_v2.py`:

- `build_grid_v2_plan(...)`
- `preview_grid_v2_counts(...)`
- `estimate_grid_v2_cache(...)`
- `execute_grid_v2_candidates(...)`
- `run_grid_v2(...)`
- `deterministic_candidate_subset_indices(...)`

Deterministic budgeted planning certification (TZ48):

- `grid_v2_planning_policy` has canonical values `full|sampled` and defaults to
  `full`; V1 Grid is outside this contract.
- `grid_v2_planning_v1` uses `ordered_block_allocator_v1` and
  `balanced_discrete_lhs_pcg64_v1`. Named BLAKE2b substreams drive only raw
  PCG64 draws, rejection-sampled integer choices, and explicit Fisher-Yates.
- Sampled `K < N` planning is O(K), assigns IDs after canonical mixed-radix
  ordering, reports per-block allocation/collision/top-up diagnostics, and is
  deterministic across worker counts. Sampled table config packing is
  conservatively disabled; mapping packing remains its parity-safe path.
- Automatic sampled allocation requires `K` to cover every enabled non-empty
  block and reconciles rounded zero targets to deterministic one-row coverage.
  Manual allocation accepts explicit zero-percent blocks and `K` below the block
  count. Existing capacity/reflow rules remain authoritative, so manual `0%` is
  a weight rather than an unconditional block exclusion.
- Full policy and sampled `K >= N` use the exact pre-TZ48 full builder/order.
  At the TZ48 identity baseline, S06 default remained 48,480 candidates with
  semantic-key digest
  `f1aa8bba4099d07f4d0d2865a72bf6537767329f3fdd9b324f07986b8b8369e4`.
- Semantic keys exclude planning inputs. Plan fingerprints use identity schema
  `grid_v2_plan_identity_v2` at this historical baseline, cover effective
  planning identity plus ordered
  semantic keys, and include normalized execution modes. The fingerprint hashes
  the ordered key stream incrementally with constant additional memory.
- WFA reuse excludes runtime dates only, includes execution-mode identity, and
  preserves the sampled table/fingerprint across window rebases.
- `full_enumeration_v2` is the static backend profile, not the effective plan
  policy. Preview, stored summaries, and sidebar rendering use authoritative
  effective-policy/allocation facts; effective-full plans hide non-operative
  seed/allocation inputs, while effective-sampled plans expose them.
- Existing JSON columns store additive planning facts; no schema migration was
  added. For Grid studies, `n_trials` and `total_trials` are delivered counts,
  while `grid_requested_budget` remains configured intent.

Runtime/profile hardening certification (TZ61 Stage 1):

- `v2_runtime_contract_v1` owns the exact ordered runtime fields
  `dateFilter`, `start`, `end`, and `warmupBars`. They are normalized centrally,
  excluded from Grid domains and identity, and rejected as optimization axes,
  option overrides, selectors, or dependencies. WFA rebase remains limited to
  the three date fields.
- Profile parsing now validates certified execution-mode combinations,
  variant-selector mappings, declared mode consumers, role topology, and
  dependency topology. Reference and compiled paths use the same central mode
  validators. Structured diagnostics distinguish fatal optimized-unbound
  execution parameters from fixed-unbound and selected-run inactive-axis
  warnings.
- Current plans publish `grid_v2_plan_identity_v3`,
  `grid_v2_semantic_identity_v2`, and `v2_runtime_contract_v1`. The version bump
  deliberately changes current identity digests without changing candidate
  rows, ordering, allocation, sampling, ranking, or execution semantics.
- The certified S06 full plan remains 48,480 rows (480 bracket and 48,000
  trail). Under the current identity schemas its ordered semantic-key digest is
  `fc55d174e835e7196ae5fcf21427d318dc364241f6b10560aa32545e6910a08f`
  and its plan fingerprint is
  `0f8d001c380df5ee95d34ca4e25910c674e20e9e8f34886a1bd2f1c261f019b2`.
- User-facing variants group diversity by `variant_name`; internal execution
  variants group by the logical `grid_mode_name`. This aligns actual grouping
  with advertised backend metadata and does not change V1 grouping.
- Stage 1 adds no database migration or dependency. Route/API/JavaScript
  exposure of the runtime contract is reserved for the separately gated Stage
  2 and is not certified here.

TZ62 diagnostic corrections preserve all Stage 1 identity and candidate pins:

- Bool-group discriminators fixed by logical blocks count as covered axes;
  genuinely inactive selected-run axes still emit
  `V2_INACTIVE_ENABLED_AXIS`.
- Truly unbound execution parameters remain warning/fatal by optimization
  state. Certified-but-unselected mode parameters instead emit
  `V2_UNSELECTED_MODE_EXECUTION_PARAM` at info severity when fixed, or fail
  with `V2_UNSELECTED_MODE_OPTIMIZED_EXECUTION_PARAM` when optimized.
  Uncertified-only consumers remain unbound and say so explicitly.
- Reserved runtime declarations reject all present non-mapping `optimize`
  values, including `null`, and report an ordered actionable reason list.
- Invalid bool groups fail with structured `V2_INVALID_BOOL_GROUP`
  diagnostics. Defensive selector resolution fails with
  `V2_INVALID_SELECTOR` and the real strategy identity.
- Built and rebased plans keep their authoritative structured collection only
  at `plan.metadata["diagnostics"]`; warning text is only at
  `plan.metadata["validation_warnings"]`. Sampler diagnostics remain separately
  under `planning.sampling_diagnostics`.
- The full emitted/default compatibility code set also includes
  `V2_VALIDATION_ERROR`, `V2_PROFILE_VALIDATION_ERROR`,
  `V2_INVALID_RUNTIME_VALUE`,
  `V2_INCOMPATIBLE_RUNTIME_DECLARATION`, `V2_INVALID_PARAMETER_ROLE`,
  `V2_INVALID_PROFILE`, `V2_UNSUPPORTED_EXECUTION_MODE`,
  `V2_UNDECLARED_CONSUMED_PARAMETER`, `V2_INVALID_DEPENDENCY`,
  `V2_UNBOUND_OPTIMIZED_EXECUTION_PARAM`, and
  `V2_UNBOUND_FIXED_EXECUTION_PARAM`.

TZ64A certifies the first cross-surface runtime/API boundary without changing
the core runtime or identity versions:

- strategy identity is explicit and conflict-checked before work for Grid
  Preview, Backtest/trade download, direct Optuna/Grid, and Walk-Forward
  identity selection; request fallbacks to S03 or the first discovered strategy
  are removed;
- config readiness, Preview, Backtest/trades, and direct Optuna/Grid use one
  engine-aware server adapter around `normalize_v2_runtime_values`; V1 bypasses
  it and retains its existing Warmup clamp/truthiness behavior;
- the adapter preserves date-key presence in execution parameters while
  canonicalizing present values (`false` to boolean, blank dates to `null`, and
  valid dates to UTC `Z`). Warmup remains orchestration state and reserved
  optimizer/Backtest injection fails with `V2_RESERVED_RUNTIME_AXIS`;
- valid V2 config responses add the exact runtime contract/defaults and
  structured diagnostics. Known invalid profiles return 422 on the config
  resource and 400 on Phase-A run surfaces; unknown configs return structured
  JSON 404;
- S06 full remains 48,480 rows with semantic digest
  `fc55d174e835e7196ae5fcf21427d318dc364241f6b10560aa32545e6910a08f`
  and fingerprint
  `0f8d001c380df5ee95d34ca4e25910c674e20e9e8f34886a1bd2f1c261f019b2`.
  Request representation changes only the ordered row payload where approved:
  blank dates move from
  `8c2ca227ef140b31700f358d16a84246f34d7edb0f46b63f139bef003a8a25a5`
  to canonical-null
  `c5645dfc07084855c4618053b12c58b55859637425fedafe849dd9f751f01ff6`,
  and naive dates move from
  `73d4665bffa3a4df39d84e71cce5778d50cae0cb43ecb1f6f340010d20c68784`
  to canonical-UTC
  `54a7709b31efc592bf6d329d45b7a0dd2bab38a78d6a7bdf1673d89f1829e19d`.
  Omitted dates retain the omitted-key digest
  `e0ba87a4dbf66a843d462d0f73d8b1c991841b247acbb6051d999bd5767b5f7c`.

Queue is certified as a generic V1/V2 transport through the normal Optimize/WFA
runtime boundary. Queue GET is non-mutating: unreadable encoding/JSON and
invalid top-level shapes return a visible conflict while preserving the source
bytes, and valid empty or leniently normalized legacy state is not rewritten or
deleted merely by reading. Missing legacy `warmupBars` is omitted at launch and
uses the core default `1000`; present malformed values remain strict runtime
errors. Queue does not add another runtime normalization authority. Preset
runtime integration remains deferred and uncertified.

TZ64A-1 corrects the runtime boundary without changing any contract or identity
version:

- strict date-only V2 starts canonicalize to UTC `00:00:00Z`; strict date-only
  ends canonicalize to inclusive UTC `23:59:59.999999Z`. A June 1 through June
  30 synthetic 15-minute window retains all 2,880 bars, and a same-day window
  retains all 96 bars, matching legacy date-only alignment;
- duplicate runtime transports compare core-canonical values, so `1000` and
  `"1000"`, false aliases, equivalent UTC timestamps, and a date-only end plus
  its inclusive canonical instant agree. The complete mapping normalizer still
  runs exactly once per request;
- Forward Test/OOS period splitting intentionally sets `dateFilter=true` for
  the derived IS window. Derived V2 bounds use canonical UTC `Z` strings without
  a second complete user-runtime normalization; V1 derivation is unchanged;
- structured server diagnostics remain authoritative JSON. The three existing
  Start-page clients that previously displayed `response.text()` now extract a
  concise non-empty `error` string while preserving legacy text failures;
- ordinary WFA uses the canonical V2 date/Warmup runtime contract, validates
  known fatal profile declarations before dataset, window, run-state, worker,
  optimizer, or storage work, and applies canonical UTC boundaries to each V2
  execution window while preserving V1 representations;
- Python `DISPLAY_PARAM_ID_IGNORED_KEYS` and JavaScript
  `PARAM_ID_IGNORED_KEYS` exclude all four runtime fields. Contrary to the
  historical TZ64A implementation-report hedge, runtime representation does
  not change current display identity;
- the added S06 date-only raw/canonical ordered-row digests are
  `b1900647b0afefb3f8f11b5150426ef04c4dc8164221f51eb260e19f1c46339c`
  and `9faf81224522a95727fb39961e3a7ea9da010c3dc4655e99ce84be11edd24e68`.
  The 48,480 count, semantic digest, full fingerprint, existing six row
  digests, sampled pins, allocation, ranking, parity, and trading semantics
  remain unchanged.

Grid V2 has two execution tiers:

- reference tier: loops candidates through the shared public V2 runner;
- compiled tier: `src/core/engine_v2/compiled_kernel.py` packs primitive
  candidate config arrays and evaluates candidates through generic
  Numba-compiled batch loops when Numba is available and JIT is not disabled.
  The default compiled Grid V2 path is now stacked: it validates shared
  OHLC/timestamps once, stacks signal/dataprep arrays as 2D rows, uses
  per-candidate data-row indices, and calls the existing per-candidate bar-loop
  body. The grouped compiled evaluator remains available for direct tests and
  parity checks. Both compiled loops are built with `cache=True`,
  `parallel=True`, and `prange`; evaluation saves and restores the process
  Numba thread count around each compiled batch.

`GridV2Settings.prefer_compiled` is live. `grid_v2_prefer_compiled` defaults to
true in normal Grid dispatch. `grid_v2_max_cache_mb` overrides the default
512 MB signal/dataprep cache estimate limit and must be finite and positive.
The normal dispatcher in `src/core/grid_engine.py` routes `engine="v2"`
strategies into Grid V2 before V1 `validate_grid_config`, V1
`FAST_GRID_BACKENDS`, V1 Numba checks, or V1 backend loading. V2 strategies are
not registered in `FAST_GRID_BACKENDS`.

Selected V2 Grid candidates are persisted with `save_grid_study_to_db(...)`
using the existing studies/trials schema. V2 compact metadata is stored in
`grid_summary_json`, including engine, Grid V2 engine version, backend kind,
compiled availability/use, compiled execution mode, candidate counts,
per-variant counts, cache estimate/stats, timings, candidates/sec,
optional-axis/variant settings, DSR deferred status, and aggregate guardrail
counters.

The normal dispatcher does not duplicate slow enrichment from the generic Grid
V2 runner. It executes the compiled/reference screening tier, then slow-reruns
only the selected persisted rows through the public V2 reference runner. The
per-result `guardrail_summary` stored on selected rows comes from that slow
reference run, not from fast screening metadata.

Phase 2.6.2 certification notes:

- `compiled_execution_mode="stacked"` is additive metadata under the existing
  `backend_kind="compiled_numba"` and `compiled_batch_used=true` contract.
- The stacked path asserts that all execution-data rows share identical OHLC
  and timestamps. A mismatch fails clearly instead of silently sharing the
  wrong market arrays.
- The cache estimate is physically tied to the stacked allocation: signal
  stack bytes, dataprep stack bytes, output bytes, and shared OHLC/timestamp
  bytes are included before any strategy `build_execution_data` calls.
- Selected rows remain slow-reference enriched through the public
  `run_v2_strategy` path.
- The typed/lazy candidate table and row-level lazy full-population result
  materialization remain deferred; `config.optuna_all_results` stays
  full-population.

Phase 2.6.3 certification notes:

- Normal Grid V2 execution uses the typed candidate table for selected-index
  handling, cache estimation, signal/dataprep grouping, params lookup, and slow
  selected-row enrichment. `plan.candidates` is lazy and is not materialized by
  normal compiled dispatch.
- Full-population semantic keys remain materialized because the shared Grid
  ranker uses `semantic_key` as a deterministic tie-break. This keeps ranking
  semantics unchanged.
- Full-population params remain cached in the table because storage/WFA/UI
  compatibility still requires full-population `OptimizationResult.params`.
- Canonical identity is selected-row lazy: fast-screening rows do not build
  canonical JSON; selected slow-reference results do.
- Typed cache grouping is certified for the S06 B2 SUI baseline at one signal
  group and 162 dataprep rows.
- The table-aware compiled config packer is covered by parity tests against the
  mapping packer for both certified topologies and tick modes, but the default
  compiled dispatch path uses mapping packing because the callback table packer
  was slower on the Phase 2.6.3 benchmark. A vectorized table packer remains
  future work.

Phase 2.6.3.1 certification notes:

- Normal compiled Grid V2 dispatch uses vectorized table config packing by
  default when the strategy normalizer preserves kernel-visible fields and mode
  state. Mapping packing remains the fallback and parity oracle.
- The vectorized table packer is parity-tested against mapping packing for both
  certified topologies, date filter settings, tick rounding modes, max-day
  fields, missing `stopMaxPct`/`stopMaxDays` defaults, timestamp conversion,
  and boolean defaults.
- Cache grouping is code-based from `variant_codes` and `axis_value_codes`.
  Cache build, memory estimation, and selected-row slow enrichment share the
  same cache-key semantics; the S06 B2 SUI baseline remains certified at one
  signal group and `162` dataprep groups.
- `params_by_row` is no longer eagerly populated. Full-population fast results
  expose lazy params mappings, while selected slow-reference results remain
  normal materialized params dicts for storage/UI compatibility.
- `config.optuna_all_results` remains full-population and retains ranking
  annotations. Fast-result metrics are eager; params and canonical identity are
  lazy accessors.
- Full-population semantic keys remain eager because the shared Grid ranker uses
  `semantic_key` for deterministic tie-breaking. V1 ranking behavior is
  unchanged.
- The Phase 2.6.3.1 direct SUI benchmark preserved the selected top candidate
  `18436` and core metrics while improving workers=6 mean wall from `14.871s`
  to `12.822s`.

Phase 2.6.4 certification notes:

- WFA Grid V2 uses a WFA-local `GridV2PlanReuseCache`. There is no global cache
  and no persisted `OptimizationConfig` cache field.
- The cache key includes the Grid V2 engine version, effective strategy config,
  all `GridV2Settings`, normalized parsed execution modes, and fixed params with
  only `start`, `end`, and `dateFilter` removed. Defensive hit validation checks
  the candidate-shaping domain, variant/mode, active/inactive, and axis layout
  before reuse.
- Cache hits reuse the immutable candidate identity/table core, then create a
  fresh rebased plan/table view with current-window `seed_params_by_variant`.
  Cached plan/table objects are not mutated in place, and rebased lazy caches
  start empty.
- Signal/dataprep arrays, market data, execution outputs, selected slow
  enrichment, and `config.optuna_all_results` remain per-run surfaces. No
  data-dependent arrays are reused across WFA windows.
- Grid V2 summaries and WFA `module_status.grid_v2` diagnostics now expose
  optional plan-reuse counters plus additional timing buckets:
  `plan_build_seconds`, `plan_reuse_lookup_seconds`,
  `runtime_rebase_seconds`, `fast_result_materialization_seconds`, and
  `ranking_seconds`. Existing timing keys and `candidates_per_second` keep
  their prior meanings.
- Storage schema and `GRID_V2_ENGINE_VERSION` are unchanged.
- The Phase 2.6.4 direct SUI benchmark preserved `48,480` candidates, selected
  top candidate `18436`, and core selected metrics. A focused real S06 B2 WFA
  Grid V2 test verifies reuse-enabled and cache-disabled selected/full
  population results match for two windows and that the first window's
  materialized runtime dates are not mutated by the second window.

Signal-reversal rescue certification notes:

- TZ43/TZ44 optimized `signal_reversal` Grid V2 execution and diagnostics
  without changing V1 runtime paths, S06 behavior, storage schema, or
  `GRID_V2_ENGINE_VERSION`.
- `signal_reversal` compiled Grid V2 uses chunked execution when the
  monolithic signal-stack estimate exceeds `grid_v2_max_cache_mb`. Non-signal
  stacked paths keep the existing fail-fast memory behavior.
- Chunked-vs-monolithic `signal_reversal` tests require exact candidate IDs,
  statuses, metrics, ranking order, selected IDs, selected metrics, and
  selected guardrail summaries. Tolerances remain limited to compiled-vs-
  reference parity tests where prior numerical tolerance already existed.
- WFA `module_status.grid_v2` diagnostics persist the additional signal timing
  buckets and chunk fields: `signal_build_seconds`, `stack_build_seconds`,
  `compiled_batch_seconds`, `cache_key_build_seconds`, `chunk_count`,
  `chunk_estimated_mb`, `max_chunk_candidates`, `max_chunk_estimated_mb`,
  `configured_limit_mb`, `full_run_estimated_signal_mb`,
  `signal_stack_rows_built`, `signal_stack_rows_peak`,
  `compiled_config_packing`, and `full_population_result_object_note`.
- `params_materialized` in signal topology reports the params currently
  retained/materialized after chunk cache release, not total params ever built.
- The Regime-ER optimized loop has a test-safe pure-Python fallback when Numba
  JIT is disabled; production performance assumes normal Numba availability.

Candidate planning is data-driven from V2 config/profile metadata. Semantic
keys include the strategy id/version, Grid V2 engine version, resolved variant,
resolved mode values, and active non-runtime parameter values. Runtime params
such as date filters and start/end values are excluded. Parameters inactive for
the resolved variant or for a false same-role boolean `depends_on` parent are
excluded from semantic identity and deduplication.

Optional axes use `optimize.default_enabled`: optimized params are axes by
default unless `default_enabled=false`; `GridV2Settings.enabled_axes` can
override the default set explicitly. Variant selector params are not normal
axes. Grid V2 enumerates variants from `execution.variants` and derives the
selector param value from the inverse `execution.variantSelector.mapping`.
`variantSelector.userFacing=false` resolves one internal variant from fixed
params and publishes no selectable `grid_enabled_modes`; user-facing logical
mode identity is stored in `grid_mode_name`.
`select`/`options` axes can be narrowed at runtime by passing
`{param}_options`; values must be a non-empty subset of the declared config
options. The subset preserves config order and is recorded in Grid V2 metadata.

Signal/dataprep cache scope is local to one run. Cache keys include
strategy/version, data fingerprint, trade-start metadata, active cache params,
and the hook fingerprint. The memory estimate includes two bool signal arrays,
five float dataprep arrays, number of bars, combo counts, and worker multiplier.
The default hard limit is `max_signal_cache_mb=512`; normal dispatch exposes it
as `grid_v2_max_cache_mb`. Runs fail clearly if the estimate exceeds the limit.
In normal dispatch, Numba workers are threads sharing one in-process
signal/dataprep cache, so the cache estimate uses worker multiplier `1` while
`worker_processes` is passed separately as the compiled batch thread cap.
Diagnostics expose signal/dataprep hits and misses, combo counts, estimated MB,
compiled worker count, and the configured cache limit.

S03 Regime-ER B2 TZ37 Grid gates:

- `plain` and `emergency` are internal execution variants selected by
  `useEmergencySL`; new Grid metadata publishes no selectable internal modes and
  rejects stale non-empty `grid_enabled_modes`.
- User-facing Grid rows are logical S03 modes stored as `grid_mode_name`:
  `cc_only`, `tbands_only`, and `both`. `variant_name` remains the resolved
  internal execution variant for debugging/certification.
- With Regime off, Emergency SL off, 10 MA types excluding `VWAP`, 20 MA
  lengths, Close Count 2..7, and T Bands 0.2..2.0, corrected full-enumeration
  counts are `cc_only=7,200`, `tbands_only=20,000`, `both=720,000`,
  `total=747,200`.
- Boolean `depends_on` collapse keeps disabled Close Count, T Bands, Regime-ER,
  and Emergency SL child axes out of counts and semantic/cache identity.
  Enabling `emergencySlPct` while `useEmergencySL=false` is inert; enabling it
  while `useEmergencySL=true` doubles the counts to `1,494,400`. Enabling
  `regimeErLength` over three values while `useRegime=true` yields `2,241,600`.
- This is a planning/usability certification, not a performance certification.
  Full S03-scale Grid V2 execution remains signal-cache heavy and can exceed
  the default `grid_v2_max_cache_mb=512` guardrail on the SUI pilot dataset.

S06 B2 Phase 2.5 gates:

- Full T1 identity gate vs S06 V1 fast-grid candidate order: `48,480` V2
  candidates, per-variant counts `480` and `48,000`, one-to-one canonical
  mapping, no execution.
- Runtime `trailMAType_options` subsetting preserves config-order counts:
  all four options keep `48,480` candidates, one option yields `12,480`, and
  two options yield `24,480`.
- Expanded threshold-enabled breadth is count-previewed at `436,320`.
- T1 metric gate is a deterministic 240-candidate subset against V1 fast-grid
  metrics with first/last, variant-boundary, stop/target/trail, and
  default-like coverage.
- T2 authority gate reruns top-ranked candidates from the executed T1 subset,
  plus bracket/trail/default-like coverage cases, against the S06 V1 slow
  strategy with B2-to-V1 param translation (`fastSmooth`/`slowSmooth`) and exact
  trade sequence comparison.
- Tick-outward Grid V2 support is certified only against direct V2 single-run
  output, not against V1 no-rounding Grid output.
- Compiled-vs-reference V2 Grid parity is covered by
  `tests/v2/test_v2_grid_compiled.py`: deterministic 240-candidate subsets for
  `priceRounding=none` and `priceRounding=tick_outward`, plus direct synthetic
  no-trade, zero-loss, max-days/strict-boundary, and full-path drawdown edge
  cases. It must be run without `NUMBA_DISABLE_JIT=1`. Full-population compiled
  parity remains deferred to an explicit slow gate. A focused determinism test
  compares identical compiled candidate subsets under different Numba worker
  caps.

The S06 B2 config order was adjusted so trailing parameters are declared as
`trailMAType`, `trailMALength`, `trailMAOffsetEx`, `trailRR`, matching the V1
candidate axis order for literal T1 identity. V1 strategy and V1 fast-grid
runtime files are unchanged by Phase 2.5.

## Notes

### Runtime persistence and replay gate

Certified new V2 studies persist the exact `v2_runtime_metadata_v1` envelope
under `config_json.v2_runtime`; V1 omits it and no migration/backfill is
performed. Writer validation happens before the study transaction. Stored
execution uses current registry/profile validation plus the shared
current/legacy/defaulted reader, ignores candidate runtime authority, applies
operation dates last, and keeps Warmup separate. A NULL study Warmup with no
config Warmup or metadata is a usable defaulted value of 1000. Current metadata
is preserved byte-for-byte as a JSON subobject when post-process/OOS config is
updated and is never reconstructed on read.

WFA persists request runtime only in its base study; window records remain the
date authority for selected-window replay and per-window optimizer configs do
not carry persistence metadata. Canonical `Z` formatting is required at V2
execution boundaries, not for the existing FT/DSR/Stress scheduling strings.
The certified preparation invariant is `dateFilter`-gated: inactive filtering
uses full data and index zero. This intentionally corrects V2 stored exports
whose candidate artifacts previously overrode an inactive study filter.

Forward Test removes all candidate runtime keys before dispatch and projects
the aligned start and inclusive end timestamps into an execution-only mapping
after dataset alignment. This keeps the strategy date gate identical to the FT
slice, including the final day, while the returned module candidate remains
JSON-safe for identity, Stress handoff, and WFA storage. The correction applies
to V2 and date-aware V1 strategies. Existing FT rows are not backfilled and
must be rerun when they influence a decision.

Lancelot partial-bundle export is certified only for the legacy
`s03_reversal_v10` integration across Optuna, Grid, and WFA selection. It is not
part of the generic Backtester V2 import or certification contract, and V2
strategy certification requires neither Lancelot aliases nor export support.
After a read-only `study_id`/`strategy_id` lookup, the API rejects every other
strategy before full study/candidate loading, stitched-OOS backfill,
stored-runtime resolution, CSV access, hashing, or bundle construction. Adding
another strategy requires a separate reviewed Merlin/Lancelot contract and
implementation task after its live-trading contract is known. The accepted S03
v10 bundle remains candidate-only, keeps Warmup at the top level, and projects
the inactive live date triple (`false`, `null`, `null`) last. Historical Merlin
trade/equity exports continue to use their study or window dates.

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
Phase 1.5. The same convention residual applies to the S06 Regime-TL pilot
references (Merlin `13.85`/`11.99` vs TV `15.02`/`13.19`).

The S06 Regime-TL pilot import (`s06_r_trend_v02_regime_trendlines_b2`) is a
strategy-package-only import: no core V2 or Grid V2 file changed. Its regime
filter is a causal signal-layer entry mask; execution stays entirely inside the
already-certified bracket profile. Pilot coverage lives in
`tests/v2/test_v2_s06_regime_tl_parity.py` (TradingView parity),
`tests/v2/test_v2_s06_regime_tl_signals.py` (pivot/state-machine units),
`tests/v2/test_v2_s06_regime_tl_causality.py` (prefix/warmup invariance and the
mandatory signal/dataprep cache-declaration invariants), and
`tests/v2/test_v2_grid_s06_regime_tl_gate.py` (generic Grid V2 plan identity,
fixed-per-study `useRegime`, opt-in regime axes with distinct signal cache
identities, compiled-vs-reference subset parity for both `useRegime` studies —
run the compiled tests without `NUMBA_DISABLE_JIT=1`). `useRegime` remains a
fixed per-study parameter for this certification target. Later strategies may
use same-role boolean `depends_on` axes, but they need explicit count,
identity, and cache tests.

Historical Windows verification used `NUMBA_DISABLE_JIT=1` before oracle imports
because normal-JIT V1 import had previously timed out. That workaround is not
the current test procedure. Ordinary V1 oracle imports now leave JIT enabled
and reuse an external Numba cache. The deterministic 240-candidate V1/V2 metric
comparison and selected-candidate V1 Slow trade comparison retain their cases,
order, and tolerances. Existing V1 numerical protection compares Fast execution
against Slow metrics/trades; it does not independently assert a compiled-versus-
interpreted Fast pair. Current compiled evidence additionally checks actual
dispatchers in fresh processes and runs with JIT enabled; historical measurements
above are unchanged.

No slippage, Bar Magnifier, lower-timeframe reconstruction, Scout WFA,
Grid-WFA integration, V2 population DSR, or V1 runtime migration is certified
by this registry.

## Conditional Fast-metric certification (TZ-07-2)

Both certified compiled execution families conditionally produce exact
reference-authoritative Sharpe and SQN. All four position/bracket and all three
signal-reversal completed-trade sites update the SQN Welford state once. The
fixed output matrix preserves offsets 0..20 and appends `sharpe_ratio=21`,
`sqn=22`, for 23 float64 columns. Disabled and undefined matrix values are
NaN; row objects share one NaN sentinel and public results project them to
`None`.

The request flags are excluded from candidate order, semantic identity, plan
identity, fingerprints, sampling, and trading behavior. The S06 certified full
population remains 48,480 and retains semantic digest
`fc55d174e835e7196ae5fcf21427d318dc364241f6b10560aa32545e6910a08f`
and plan fingerprint
`0f8d001c380df5ee95d34ca4e25910c674e20e9e8f34886a1bd2f1c261f019b2`.
Two-objective Pareto acceleration and the unchanged quadratic 3+ objective
fallback both apply to these metrics.

## Advanced-metric warmup boundary certification (TZ-10)

The reference runner, both compiled V2 execution families, and all three V1
Fast backends share one evaluation-only calendar contract. Technical warmup is
excluded using `trade_start_idx`; initial capital anchors the first evaluation
month; the previous bar closes the old month; the transition bar belongs to
the new month; and the final partial month is retained. No-completed-trade
results expose no usable Sharpe track. One real month cannot define Sharpe, two
real months can, and DSR higher moments remain unavailable below three.

The S06 full population remains 48,480 with unchanged semantic digest
`fc55d174e835e7196ae5fcf21427d318dc364241f6b10560aa32545e6910a08f`
and plan fingerprint
`0f8d001c380df5ee95d34ca4e25910c674e20e9e8f34886a1bd2f1c261f019b2`.
No schema, identity, output-offset, or realized-drawdown contract changed.

## Selected Slow-metric transport certification (TZ-11)

Each successful reference `V2RunResult` owns the exact required `BasicMetrics`
and `AdvancedMetrics` objects returned by its single enrichment call. Generic
selected enrichment and the normal Grid V2 selected validator read those typed
snapshots directly, without silent defaults or recalculation. Fast rows remain
Fast-authoritative and unchanged. Canonical Win Rate and finite Sortino now
reach selected results, storage, and WFA module trials; genuinely undefined
Sortino remains `None` and is still unusable as a sole Slow objective.

## Request-gated Daily Sharpe reference certification (TZ-14-1)

All three certified adapters across the position/bracket and signal-reversal
families preserve disabled behavior and transport the exact typed snapshots
from one runner enrichment. When explicitly requested, Daily Sharpe uses the
evaluation-only mark-to-market curve, resolution-independent int32 UTC days
since the Unix epoch, fractional returns, the canonical anchor, arithmetic
`rf/365`, population variance, and `sqrt(365)`. Monthly Sharpe remains
unannualized and unchanged, so thresholds cannot be shared between the two
metrics. Any non-finite evaluation equity observation invalidates the complete
Daily Sharpe series, including a non-closing intraday observation.

Synthetic and committed-fixture certification covers first/new-day ownership,
partial edges, inclusive-midnight one-bar final days, absent dates, warmup
exclusion, invalid-series suppression, zero-trade diagnostics, and independent
literal-loop parity. Both diagnostics are `None` when disabled or structurally
invalid and are integers, including genuine zero, for a valid series; the ratio
can remain `None` for no completed trades, insufficient observations, or zero
variance. No active-day gate exists. Sparse delayed-OOS endpoints produce only
represented daily observations, never implicit calendar rows. Phase 1 does not
enable this metric in Fast Grid, selected-Slow payloads, WFA production, UI,
storage, or DSR; those surfaces require an explicit Phase 2 decision.

## Internal Fast Daily Sharpe certification (TZ-14-2 Stage 1)

All three V1 Fast backends and both V2 compiled execution families now accept
an internal `compute_sharpe_daily` request and reproduce the accepted reference
contract. One canonical helper prepares a contiguous signed `int32` UTC day-ID
array once per dataset/window and rejects `NaT`, length mismatches, and invalid
ranges before compiled dispatch. Disabled requests use a zero-length typed
sentinel and do no Daily per-bar candidate work.

Each candidate streams opening/closing equity, validity, Welford state, emitted
observations, and active-day count in constant memory. The final observed day is
emitted independently of completed trades. Structurally valid zero-trade,
insufficient-sample, and zero-variance series retain exact integer diagnostics
while the ratio remains unavailable. Non-finite evaluation equity or an invalid
opening denominator suppresses all three fields. The canonical compounding
self-check remains reference-only by design; selected validation compares the
Fast ratio within `abs=1e-12`, `rel=1e-9` and both counts exactly, so the kernels
do not add a product accumulator.

The V2 output contract is 26 float64 columns: existing offsets 0..22 remain
fixed and offsets 23..25 are Daily Sharpe, observations, and active days.
Undefined values remain NaN internally and project to `None`; genuine diagnostic
zero survives as integer zero. The S06 population remains 48,480 with semantic
digest `fc55d174e835e7196ae5fcf21427d318dc364241f6b10560aa32545e6910a08f`
and plan fingerprint
`0f8d001c380df5ee95d34ca4e25910c674e20e9e8f34886a1bd2f1c261f019b2`.
Public objective registration, WFA behavior, UI, Queue, persistence, Results,
Analytics, DSR, and all existing metric formulas remain outside this stage.

## Public Daily Sharpe integration certification (TZ-14-2 Stage 2)

Daily Sharpe is registered as a maximize Optuna objective and an explicit
Fast-only Grid objective for V1 and V2. Optuna and Grid derive the execution
request from their authoritative objective arrays; unrelated candidates retain
the disabled path, and the existing independent six-objective limits are
unchanged. A selected Daily objective may reduce the rankable population;
unavailable values are excluded or fail the Optuna trial contract and are never
fabricated as zero.

Fixed and Adaptive WFA final selections compute the canonical metric for each
real IS series and real undelayed dense OOS series regardless of the ranking
objective. A delayed `trade_start` or authoritative `no_trade` plan suppresses
all three OOS fields without applying the helper to a synthetic flat prefix;
this preserves missing-date semantics. Nullable SQLite columns store the ratio
and exact integer diagnostics for trials, selected windows, and module trials.
Additive idempotent migrations leave historical rows `NULL` and perform no
backfill. Generic Queue, Results, API, and Analytics transports expose stored
facts without new charts or identity changes. DSR, Monthly Sharpe, WFE,
stitched OOS, trading, drawdown, and exports remain unchanged.

## S06 v06-4-A2 multi-trail certification (TZ-17.2)

The separate `s06_r_trend_v06_4_a2_b2` adapter preserves both existing S06
entry modes and initial-stop preparation. Full variant populations are Bracket
480, R Trail 3,600, Chandelier 6,000, Fixed-AF SAR 2,400, and combined 12,480.
The combined semantic digest is
`4d7173e4c4b2bb26a68195e32fb6b8a27b47c7098af7bcaecd158d1431337036`;
its plan fingerprint is
`73244c7dc697e11b33e721956b2b3a9c45098f055a64e95f26b921934763578d`.
Complete 480-row compiled outputs, including MTM, match the old disabled-MA
Bracket path for both entry modes. Six frozen TradingView configurations cover
all three trail families and both entries. Their non-exact observations arise
only from the accepted fill-bar initial-stop, one-tick candidate, CSV rounding,
final-boundary, and TradingView intrabar-drawdown representations.

Merlin intentionally arms a valid initial protective stop on the entry fill
bar. The imported TradingView Pine behavior may not provide the same fill-bar
stop. If that stop is reached, the divergence can change the exit, trade count,
and every later equity-based position size, so the remaining trade sequence can
also diverge. Certification therefore uses the pinned alignment opcodes and
per-trade delta evidence; it does not assume a bounded one-trade price residual.

## S03 v16-4-A Adaptive MA import

The `s03_reversal_v16_4_a_adaptive_ma_b2` package uses generic signal-reversal
reference/compiled execution with optional Emergency SL. Only SuperSmoother,
FRAMA and DSMA were added locally; `indicators.ma.kama`, its seed, dispatcher
and existing option domains remain unchanged. Calls to the shared KAMA are
verified on short histories, different lengths and shifted windows.

All 22 frozen manifest hashes were rechecked, including the Pine
`aa45babe2ee8b6a8a61250a217c116346ad6d9e7b55dc9a3b81f23edc5bab47c`
and market
`d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`.
The source copy matches the original local Pine. SUI contains 19,056
consecutive 30-minute bars. Production preparation uses Warmup 1000 and
inclusive End `2025-12-01T00:00:00Z`: 6,857 rows, trade start index 1000.

| Reference | Trades / wins | Net Profit % | Profit Factor | Realized Max DD % | Emergency SL (Long / Short) |
| --- | --- | --- | --- | --- | --- |
| A KAMA | 63 / 23 | 36.65663378600007 | 1.285729984357437 | 21.320000857549502 | 0 / 0 |
| B SuperSmoother | 66 / 24 | 37.35478997499999 | 1.2385119655553722 | 19.481348177057505 | 0 / 0 |
| C FRAMA | 62 / 21 | -2.594669402000008 | 0.9823431476058537 | 27.289743307351365 | 1 / 2 |
| D DSMA | 53 / 22 | 8.600414829999934 | 1.077460764726533 | 23.064183519914025 | 1 / 3 |

[Separate Merlin expectations](../../data/baseline_v2/s03_reversal_v16_4_a_adaptive_ma/merlin_expectations.json)
pin all Basic/Advanced metrics, 244 quantities and net PnLs, residual trade
numbers and measured warmup signal differences. Price/skeleton checks read the
unchanged normalized external CSVs. Tests compare floating metrics with
`rel=1e-9, abs=1e-10`, quantities with absolute `1e-10`, and identified prices
with absolute `1e-12`; unknown residual trades do not inherit a blanket tick
tolerance. The full task-owned `references.json` records every trade and delta.

All directions, entry timestamps and entry prices match the exports exactly.
Non-final exit timestamps also match. The four final timestamps move from
November 30 23:30 to December 1 00:00 UTC. A/B/C final trades close at 1.4230
instead of 1.4958. D final trade 53 executes an Emergency SL at 1.42443 on the
additional bar, rather than the Pine End Close at 1.4958. Its fourth stop is
Long. Single/batch adapters use the same inclusive truncation; date-only End,
equivalent UTC offsets, between-bar End, empty eligibility and Date Filter off
retain the common runtime behavior.

### Exact non-final exported stop-price residuals

| Reference / trade | Merlin price | Export price | Scheduled ratchet evidence for newly identified cases |
| --- | --- | --- | --- |
| C / 8 | 3.72573 | 3.7257 | Aug 14 03:00 UTC Close 4.1397 multiplied by 0.9 |
| C / 11 | 3.68973 | 3.6898 | Short Emergency SL, below one tick |
| C / 47 | 2.12113 | 2.1212 | Short Emergency SL, below one tick |
| D / 9 | 3.69743 | 3.6975 | Short Emergency SL, below one tick |
| D / 31 | 2.72217 | 2.7222 | Oct 12 01:00 UTC Close 2.4747 multiplied by 1.1 |
| D / 45 | 2.09286 | 2.0929 | Nov 4 20:00 UTC Close 1.9026 multiplied by 1.1 |

Preparation listed C11/C47/D9; production validation additionally identified
C8/D31/D45 as full-float differences, correctly exposing the parent specification's
incomplete three-case list. Investigation used immutable market data and the
generic 16-bar ratchet schedule; tests independently reconstruct each stop.
The follow-up accepts all six individually pinned cases above. Each difference
is below one 0.0001 tick; on these fixtures all six agree with outward tick
rounding (floor Long / ceil Short). The additional C8/D31/D45 also agree with
nearest four-decimal rounding. This fixture agreement does not establish a
universal TradingView rounding rule. Execution rounding, trade timing, frozen
baseline values and tolerances remain unchanged; no indicator change or general
tick tolerance was introduced. All four expected counts and net profits remain
reproduced.

A/B quantities match throughout. C trades 15,40,49,50,52,54,56,58,59 and D
trades 17,30 are one 0.01 contract step larger in Merlin after the bounded
stop-price effects propagate through realized-equity sizing. Other quantities
match (ignoring floating representations such as 76.32000000000001). Per-trade
net PnL is independently reconstructed using both entry/exit commissions.
Maximum non-final absolute PnL differences versus two-decimal CSV PnL are
A 0.0047338495, B 0.0048556315, C 0.0056629600 and D 0.0049925480 USDT.
Do not equate rounded per-trade sums with TradingView cumulative PnL, or use
TradingView intrabar DD/custom tables as canonical Merlin metric oracles.

### Signal, planning and workflow coverage

`tests/v2/test_v2_s03_adaptive_ma.py` covers independent small filter formulas,
odd FRAMA windows, seeds/constant data, prefix causality, integer/empty inputs,
batch reuse, all six signal fields, band precedence/memory and counter resets.
Against Warmup 1000, measured signal differences at Warmup 100 are A/B/C/D
25/8/16/173; at 500 and 2000 they are zero for these four references. This
finite observation does not establish universal recursive-history convergence
or universal Pine-KAMA equality.

Default axes are four MAs, ten lengths, seven counts on each side and ten
bands on each side, with execution fixed and Emergency SL off. Preview gives
196,000 asymmetric and 2,800 symmetric rows (49,000/700 for one MA). Optimizing
active Emergency SL 10/20 doubles either count. The complete 2,800-row table
is constructed directly, without an asymmetric population. Small full plans
prove canonical filtered order, including reversed pair declarations; sampled tests prove deterministic membership,
no full builder for K<N, saturation, explicit expansion, fixed sources,
value-key equality, rebasing and discarded-target identity invariance.

Compiled/reference comparisons cover all four MAs and both stop settings,
one/two workers, all applicable emitted metrics/diagnostics, selected Slow
calls and chunking. The separate performance run executes all 2,800 symmetric
rows on SUI, plus 128 asymmetric sampled rows covering every MA. Actual
`compiled_numba`/stacked/mapping dispatch is asserted. Selected rows retain
candidate/semantic identity and use one authoritative reference run each.
Fixed-day, calendar-month and Adaptive WFA exercise reduced-plan reuse,
SQLite save/reload and explicit asymmetric replay. Standard Optimize also
executes, saves/reloads its study, and reconstructs the reduced Preview.
Delayed OOS replays explicit asymmetric parameters and preserves trades while
removing technical history from the flat-prefix metric boundary.

`tests/server/test_parameter_ties.py` covers HTTP/core Preview, invalid request
selection, inactive target ranges, Queue transport and nested-only stored
Preview (2,800), canonical explicit off (196,000), and legacy omission.
`tests/js/test_parameter_ties.js` exercises toggle/edit/restore, double toggle,
repeated Queue snapshot reload, config selection authority, API fallback,
Preview invalidation/rescheduling, source range editability, stale same-ID
async responses, empty loads and strategy switches.
Existing JS readiness/Queue/Presets wrappers remain preservation gates;
Presets integration was not extended. The parent did not extend Strategy Lab;
the bounded follow-up certification below supersedes that exclusion.

Before shared edits, 86 existing identity/S03 Regime-ER/S06 v06-4-A2 tests
passed on Windows. Existing kernel behavior, KAMA, old configs and pinned
candidate fingerprints remain preservation authorities; no old oracle values
were refreshed. Final suite results are recorded in the implementation report.


## S03 Adaptive MA follow-up: Grid ties and Strategy Lab

TZ 20-1-1 v2 was validated on Windows 10 build 19042, Python 3.13.7, NumPy
2.3.3 and Numba 0.65.1 against parent `c5d6fcea37fd3ede3310d8ae818d83b6afa1f570`.
611 distinct focused cases passed across the final accepted outcomes, including
496 Grid/S03/S06/Lab preservation cases. A separate fresh external compiler-cache
run passed all 45 MTM cases in 132.20 seconds. Actual one/two-thread signal
execution and success/failure restoration passed; the host exposed 16 threads.
No old metric, identity or baseline pin was refreshed. All seven existing S06
specs preserve file hashes, normalized content, generation/preregistration hashes,
fingerprints, semantic digests and counts. The six additional content-hash pairs
are permanent literal regression assertions.

Grid now preserves the tied inactive-axis fixture's 14 raw / 7 unique rows and
interleaved ordering. Automatic selector/runtime eligibility is consistent with
untied planning. Lab accepts strict optional ties and checks internal selector
variant declarations before fingerprint checks. The necessary adjacent generator
guard permits null start/end defaults but rejects non-null window dates, leaving
explicit candidate metadata and segment-boundary authority unchanged.

The portable `s03_adaptive_ma_symmetric.json` plan has 2,800 candidates, 700 per
MA, and fingerprint
`abd173f523cf956862041c00ebd60ba1d36616cbe7b7b7c83dfeecb5ad6eec7f`;
its semantic digest is
`1a8aab968ff6165ae23c55a735642a587700a959b073374046a4591e0ceab17b`.
The read-only 0801 30-minute pack passed all 118 inventory/source checks and
all source SHA-256/size/mtime preservation checks. CRVUSDT (development) windows
1..3 produced three float64 `(2800, 2, 21)` schema-v2 arrays, all rows effectively
`compiled_numba`, with all 16,800 MTM observations finite. Each saved candidate
retains equal explicit Long/Short counts and bands and four independent axes.

A geometry-only recipe was saved before execution: every MA at minimum,
maximum and interior length/count/band points, plus shared edges and evenly
spaced fill. All 39 selected candidates matched reference across all six
segments (234 comparisons): zero identity/count/availability/float mismatches,
including exactly zero MTM differences. Independent small fixtures cover
commissions, next-open exits, strict final close, Emergency SL, warmup/empty
boundaries, sticky non-finite equity, >100% drawdown, packed/mapping input,
repeated data indices, reordered selection and forced chunking. The older S03
Regime-ER and existing position-family MTM remain preservation gates.

Two fresh generator processes matched deterministic semantic artifacts; a third
output interrupted after its first group resumed to the same result and its
completed rerun was an immutable verified no-op. Small fixed-value fixtures
reject changed tie/run identity without output mutation for partial and complete
outputs. Schema-v2 analysis read the explicit projection using the existing
`development --allow-partial-scope` override and emitted all five normal files.
This certifies only partial development (one ticker, three windows), not full
pack execution, holdout, allocation, policy nomination or strategy quality.

The generation resource contract was one outer worker, one Numba thread and a
32 MB signal cache limit. Clean generation took 20.419 seconds including first
signal compilation and 5.825 seconds warm; resumed remaining work took 3.810
seconds. A bounded 128-candidate / 3,928-bar warm comparison retained exactly the
baseline 26-column output bytes: baseline/final MTM-off mapping medians were
3.132/3.324 ms, and final MTM-on was 4.886 ms. A prepacked experiment localized
the small off cost to the compiled path (2.294/2.443 ms); these timings are
machine-specific, not a performance improvement claim. MTM adds exactly eight
bytes per requested candidate and does not alter existing cache/thread policy.

Commands, all old S06 hashes, first-run corrections, source/group hashes,
precise boundaries, cold/warm details and limitations are in the
[follow-up implementation report](../_work/TZ_20-1-1_Follow-up-Patch_v2-agent-answer.md).
Evidence was retained under the external Windows Temp directory
`merlin-tz20-1-1-v2/real-smoke-02`, with `evidence.json`, `selection.json`,
three datasets, logs and partial analysis. The maintained
[Lab command](../../tools/strategy_lab/README.md#certify-the-real-pack) recreates
this bounded gate in a fresh directory. The parent full-suite reports were
trusted; no new unfiltered/JS suite or Ubuntu/VPS verification was claimed.
Changes remain uncommitted for the user's independent review and commit.
