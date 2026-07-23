# Backtester V2 Profile Certification Registry

This registry tracks which Backtester V2 execution profile features have been
certified against external references. A mode absent from this file is not yet
certified for Python-native strategy trust.

Phase 1.5 adds a shared balance-based V2 metric parity layer, deterministic run
checks, prefix/window-start anti-repainting checks, and an opt-in
TradingView-compatible outward tick-rounding mode for computed order levels.
Phase 2 adds generic Grid V2 planning. Phase 2.5 integrates Grid V2 into the
normal Grid dispatcher/storage workflow and adds a generic compiled batch
evaluator for supported V2 execution profiles. WFA/Scout integration remains
deferred. Phase 2.5.1 tightens dispatcher/storage behavior, runtime Grid
settings, and compiled batch determinism without changing V1 runtime paths.
Phase 2.6.2 changes the compiled Grid V2 plumbing to a generic stacked batch
path while preserving the reference runner and grouped compiled path as
certification oracles. Phase 2.6.3 moves Grid V2 planning/execution onto a
typed candidate table while keeping legacy candidates as a lazy compatibility
surface.
The TZ37 follow-up adds metadata-driven internal execution variants and same-role
boolean dependency collapse for S03-like Grid V2 planning without changing V1
runtime paths.

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
| Pilot import: S06 Regime-TL no-regime control (Trend @ Square + bracket, default no-rounding) | `entryOrder=market_next_open`, `stop=atr_swing`, `target=rr`, `trail=none`, `sizing=risk_per_trade`, `margin=report_only`, `boundary=strict_close` (not exercised), `priceRounding=none`, `useRegime=false` | `s06_r_trend_v02_regime_trendlines_b2` | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/reference_a_trend_square_bracket_no_regime/` | 2026-07-12 | Exact trade skeleton (45 trades, directions, UTC entry/exit timestamps, exact entry prices); exit prices within one exported tick; 16 of 45 sizes differ by one `0.01` contract step (TV export rounding). Frozen Merlin-convention metrics: `45` trades, `16` wins, `35.56%` win rate, `net_profit_pct=11.3836924235` (TV displays `11.35`; 2-decimal per-trade export rounding accumulation), `profit_factor=1.1966147730` (TV displays `1.196`; `round(pf,3)` does not reproduce the TV display for this baseline), `max_drawdown_pct=13.8496161888` (TV `15.02` uses the equity/open-excursion convention). This baseline does not exercise the final-boundary close; strict boundary logic stays covered by the S06 v02 baseline. | No-regime control certified trade-for-trade against the committed TradingView export |
| Pilot import: S06 Regime-TL trendline regime filter (Trend @ Square + bracket, entry gating only) | Same execution modes as the control row plus signal-layer regime gating: `useRegime=true`, `regimePivotLen=15`, `regimeSlopeFactor=0.25`, `regimeBreakBufferX=0.5` | `s06_r_trend_v02_regime_trendlines_b2` | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/dataset.json` (`market_data.sha256=d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae`, `instrument.tick_size=0.0001`) | `data/baseline_v2/s06_r_trend_v02_regime_trendlines/reference_b_trend_square_bracket_regime_tl/` | 2026-07-12 | Exact trade skeleton (43 trades); exit prices within one exported tick; 11 of 43 sizes differ by one `0.01` contract step. Frozen Merlin-convention metrics: `43` trades, `17` wins, `39.53%` win rate, `net_profit_pct=22.3484141712` (TV `22.33`), `profit_factor=1.4057224218` (TV displays `1.405`), `max_drawdown_pct=11.9928996219` (TV `13.19`, drawdown-convention residual). Regime gating affects entries only; the 1000-bar warmup recipe is test-locked as converged for the regime state machine on this baseline. Final-boundary close not exercised. | Trendline regime entry filter certified trade-for-trade against the committed TradingView export |
| Signal-reversal topology for S03-like strategies | `topology=signal_reversal`, `entryOrder=market_next_open`, `sizing=fixed_pct_equity`, `exitOnSignal=true`, `stop=none/emergency_pct`, `boundary=strict_close/none`, `priceRounding=none` | Test-only S03-like signal harness in `tests/v2/s03_like_test_helpers.py`; production pilot covered by the S03 Regime-ER rows below | Real gate: `data/raw/OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv`; structural gate: synthetic gapless OHLC in `tests/v2/test_v2_signal_kernel_s03_v11_gate.py`; compiled/Grid fixture gate in `tests/v2/test_v2_grid_signal_topology.py` | `docs/_work/S_03-v11_Update/reference_tv_s03_v11_emergency_sl_10pct.json` | 2026-07-16 | V2 uses true next-open market fills while V1 S03 v11 uses close-fill approximation; exact V1/V2 trade equality is certified only on gapless synthetic data with the documented +1-bar timestamp map. Real SUI certification is metric-level against the TradingView JSON with V1 tolerances and exactly 12 `Emergency SL` exits. V2 deducts entry commission at fill time, so intra-trade balance/equity can differ from V1 while per-trade net PnL and final net profit match on gapless data. Compiled Grid uses a topology-specific stacked boolean payload and mapping config packing, not the vectorized table packer. | Reference and compiled/Grid fixture tiers certified; production pilot added separately |
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
  all `GridV2Settings`, and fixed params with only `start`, `end`, and
  `dateFilter` removed. Defensive hit validation checks the candidate-shaping
  domain, variant, active/inactive, and axis layout before reuse.
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
  no-trade, zero-loss, max-days/strict-boundary, and episodic drawdown edge
  cases. It must be run without `NUMBA_DISABLE_JIT=1`. Full-population compiled
  parity remains deferred to an explicit slow gate. A focused determinism test
  compares identical compiled candidate subsets under different Numba worker
  caps.

The S06 B2 config order was adjusted so trailing parameters are declared as
`trailMAType`, `trailMALength`, `trailMAOffsetEx`, `trailRR`, matching the V1
candidate axis order for literal T1 identity. V1 strategy and V1 fast-grid
runtime files are unchanged by Phase 2.5.

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

Tests that import the V1 fast-grid oracle set `NUMBA_DISABLE_JIT=1` before any
Numba-importing modules are loaded. This keeps the V1 oracle practical in this
Windows environment, where normal-JIT V1 fast-grid import has previously timed
out. V1 compiled-vs-interpreted behavior remains covered by existing V1 tests,
and the V2 compiled Grid test is intentionally separate.

No slippage, Bar Magnifier, lower-timeframe reconstruction, Scout WFA,
Grid-WFA integration, V2 population DSR, or V1 runtime migration is certified
by this registry.
