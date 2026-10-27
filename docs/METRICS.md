# Merlin Metrics

This is the cross-engine authority for current metric behavior shared by
Backtester V1, Backtester V2, Grid, WFA, storage/UI, and Strategy Lab. Exact
certification values and external residuals belong in
[V2 certification](engine_v2/CERTIFICATION.md), not here.

## Ownership and categories

`src/core/metrics.py` owns canonical `BasicMetrics`, `AdvancedMetrics`, and
`WFAMetrics` calculations. Strategy execution produces trades plus balance and
equity curves; metric code interprets those outputs. Fast Grid implementations
must match the canonical reference for every metric they advertise.

- Realized/basic metrics use completed trades and the realized balance curve.
- Advanced path metrics use the explicitly bounded mark-to-market equity view.
- WFA reports per-window IS/OOS metrics and aggregate/stitched facts.
- Strategy Lab projects a fixed research schema from certified V2 execution;
  its MTM drawdown is not a public Merlin optimization metric.

## Entry admission diagnostics

Only `risk_bracket_v1` produces authoritative selected-run entry diagnostics:

```json
{"version":1,"scope":"is","maxEntryLeverageUsed":7.0,"leverageCapSkippedEntries":26}
```

Maximum is over accepted next-open fills using notional divided by realized
balance after entry fee, including same-bar exits. It is null with no fills.
Skips count only finite leverage strictly above the configured cap, once per
pending order; minimum quantity/notional, unavailable inputs, invalid brackets,
undefined leverage and occupied/suppressed signals do not count. This is an entry
statistic, not simultaneous portfolio margin or maximum holding-time exposure.

Selected Slow is authoritative. Nullable `trials.entry_diagnostics_json` maps
`is`, `ft`, `oos_test`; `wfa_windows.entry_diagnostics_json` maps `is`, `oos`.
`wfa_window_trials.module_metrics_json.entry_diagnostics` stores the selected
module execution, and manual `results_json` entries carry scope `manual`.
Transient/reference scope is `execution`. No historical backfill is performed.
Stress selections use saved IS diagnostics; perturbations enforce the cap but
their diagnostics are neither persisted individually nor summed into this card.

Whole WFA/stitched summaries aggregate distinct actual OOS windows only. Selected
IS is separate; a collapsed window header uses that window's OOS. Adaptive
truncation retains fill leverage only for surviving trades, including the rule
that an early entry with a later discarded exit contributes nothing. Refusals
are retained through the inclusive trigger-time cutoff within the live interval.
Delayed prefixes add no events. Proven no-trade/FT-rejected windows have null
maximum and zero skips; missing/failed execution is unavailable. Aggregation is
read-only and never sums optimizer populations or alternative module executions.
Analytics deduplicates selected study identities; any missing included diagnostic
makes a mixed aggregate unavailable.

The last WFA/Analytics card is `Max Entry Lev / Skipped`, after OOS Win Rate (Med),
with no tooltip. Non-WFA Optimize shows only this card under the chart for selected
IS/FT/OOS/manual rows (Stress uses IS). Formats are `7.00× / 26`, `— / 0`, `— / 26`
or `N/A` for unavailable/old data. The configured cap remains in Parameters.
Selection/loading/error transitions clear stale values; saved views need no pack.

## Evaluation boundary

`StrategyResult.metric_start_idx` is the first evaluation observation in the
curves. `metric_initial_equity` is the equity immediately before it. Producers
must set both when prepared data includes technical warmup.

Monthly Sharpe, Monthly Sortino, Daily Sharpe, Ulcer Index, and Consistency use
the evaluation-only equity observations and their pre-evaluation anchor. The
boundary excludes technical warmup without inventing observations. Realized
basic metrics—including realized Max DD and RoMaD—do not use this advanced
boundary.

## Realized metrics

Net Profit, gross profit/loss, Profit Factor, trade counts, Win Rate, average
trade statistics, and maximum consecutive losses are derived from completed
trades. Net Profit is final realized balance minus execution initial capital;
Net Profit % divides that difference by execution initial capital, not by the
first curve observation or ending mark-to-market equity.

Realized Max DD is balance-based; non-finite balance observations do not create
drawdown points. The Slow/Python calculation converts the realized
`balance_curve` to floating values, forward-fills missing interior observations,
then filters values that remain non-finite. The V2 metrics kernel skips
non-finite observations directly. These produce the same effective realized
drawdown path because repeating the preceding balance cannot create a deeper
drawdown. Both scan the full path, including technical warmup and the final
unrecovered tail. At each observation the running realized-balance peak is
updated. Percentage drawdown is the maximum
`(peak - balance) / peak * 100` for positive peaks; absolute drawdown is the
independent maximum `peak - balance`. Neither `metric_start_idx`,
`metric_initial_equity`, nor a Net Profit starting-balance argument seeds or
truncates this scan. An unrecovered final loss therefore remains visible.

RoMaD uses Net Profit percentage divided by this realized percentage Max DD.
It retains the established zero-drawdown convention in `calculate_advanced`;
it must not be recomputed from MTM drawdown.

### Execution capital and historical results

OOS/manual, Forward Test, Stress Test and Fixed/Adaptive WFA pass the actual
execution capital to both canonical metric functions. `core.execution_capital`
resolves the final execution parameters after candidate/fixed merging or stored
runtime resolution. A declared `initialCapital` value wins when present; only
an absent key uses the registered strategy's default. It must parse as a finite,
positive number; null, booleans and malformed/nonpositive/nonfinite values fail.
S01 v26 has no declared capital and keeps its established fixed 100, ignoring
extraneous capital keys. No strategy defaults or launch validators are changed.
The execution denominator remains distinct from `metric_initial_equity` and the
advanced evaluation boundary. Canonical formulas are unchanged.

Invalid declared capital follows existing caller failures: manual requests return
400, OOS/WFA calculations fail, FT workers return no result, and a failed Stress
base is `skipped_bad_base`. These are not new launch-time validation guarantees.

Earlier non-100 FT/OOS/manual and WFA-window Net Profit % and RoMaD may be wrong.
Derived comparisons/degradation, FT admission/ranks, stress retention/status, WFE,
OOS win rate and save-time profitable-window aggregates may also be affected.
Correcting FT/stress metrics can change candidate selection and subsequent WFA
plans; identical candidate execution inputs still produce identical trades,
fees, quantities, balances, equity and timestamps.

Stored rows are never repaired by reading or exporting them. A new manual test
uses corrected metrics but compares with the saved source: `forward_test` uses
potentially affected historical `ft_*` metrics; `optuna` (including Grid IS),
`dsr` and `stress_test` use original IS trial metrics. Original optimizer IS/DSR,
realized DD, Win Rate, Profit Factor, SQN and advanced path metrics with explicit
anchors were not directly corrupted by this denominator bug. Normalized stitched
profit/DD and Analytics calculations also have no direct capital-base mismatch,
although their historical window selection may reflect faulty FT/stress decisions.
Run a fresh full study for a consistently corrected selection chain. Reconstructed
charts do not repair stored metrics. The frontend's fixed-100 reference line and
y-range remain a separate deferred visual issue.

WFA no-trade curves use execution capital; delayed engine prefixes preserve the
existing advanced anchor and use execution capital only for a missing anchor.
Route synthetic prefixes use saved execution capital. The engine's evaluation-only
view and the route's existing warmup/timestamp ordering are distinct; this correction
does not redesign them. Stitching remains a normalized index starting at 100.
Daily Sharpe availability is unchanged: IS and dense OOS are supported; delayed
and no-trade OOS remain unavailable.

## Monthly risk-adjusted metrics

Monthly returns are built from evaluation-only bar-close equity. The
pre-evaluation anchor opens the first month, a transition bar belongs to the
new UTC calendar month, the preceding bar closes the old month, and the final
partial month is retained. Technical warmup months are not observations.

Monthly Sharpe (`sharpe_ratio`) uses a fixed 2% annual risk-free rate divided
by 12 and population variance. It is not annualized with a square root. It is
undefined without completed trades, with fewer than two real monthly returns,
or with non-positive/non-finite variance.

Monthly Sortino (`sortino_ratio`) uses the same monthly series and requires
real downside observations. Two- to four-month windows commonly yield `None`;
selecting it as a Slow objective can remove many or all candidates.

## Daily Sharpe

`sharpe_daily` is distinct from Monthly Sharpe and is calculated only when
requested. Evaluation timestamps are grouped into observed UTC days since the
Unix epoch. The algorithm uses the pre-evaluation equity anchor, fractional
simple returns for observed days (including partial edge days), `0.02 / 365`
as the daily risk-free rate, population variance, and `sqrt(365)`
annualization. Missing calendar dates are not synthesized.

Any non-finite evaluation equity observation or non-positive opening
denominator invalidates the complete daily series. When disabled or
structurally invalid, `sharpe_daily_observations` and
`sharpe_daily_active_days` are `None`. For a valid constructed series both are
integers, including zero; the ratio can still be `None` with no trades, too few
observations, or zero variance. An active day has absolute raw return greater
than `1e-12`; this is diagnostic, not an eligibility gate.

The reference implementation and V1/V2 Fast implementations share these
boundaries and validity rules. Fast execution uses one dataset/window UTC-day
array and per-candidate streaming statistics. Fixed and Adaptive WFA final
reporting requests Daily Sharpe for real IS and real undelayed dense OOS
series. Delayed/no-trade sparse OOS does not publish it because missing dates
must not be invented. Storage fields are nullable and historical rows are not
backfilled.

## Other advanced metrics

- Profit Factor is gross winning PnL divided by absolute gross losing PnL when
  defined.
- SQN uses exact net trade PnL and sample variance. It is undefined below 30
  completed trades, with non-positive variance, or standard deviation below
  `1e-10`.
- Ulcer Index measures drawdown along the advanced evaluation equity path.
- Consistency is signed R² of that evaluation equity path, in `[-1, 1]`.
- Composite Score is an Optuna/UI scoring construct over configured normalized
  metrics, not an independently generated market-performance series.

Non-finite selected objective values are not silently replaced. Optuna marks
invalid objective returns as failed trials; Grid removes candidates with a
non-finite selected objective from ranking. Short WFA windows can therefore
have no rankable SQN, Daily Sharpe, or Sortino candidate.

Fixed research bypasses ranking and retains every successful singleton reference
result, including losses, zero trades and undefined metrics. It does not fabricate
missing metrics or treat execution-error rows as valid no-trade results. Its regular
WFA IS windows are descriptive; existing WFE and OOS stitching formulas are unchanged.

## Surface availability

`Yes` means the metric is available on that surface when its mathematical
preconditions hold. “Gated” means it is computed only when requested or during
the surface's explicit reporting path.

| Metric or group | V1 Optuna objective | Grid Fast | Grid Slow | WFA reporting | Strategy Lab schema v2 |
| --- | --- | --- | --- | --- | --- |
| Net Profit %, realized Max DD %, RoMaD, Profit Factor, Win Rate | Yes | V1 + V2 | Yes | Per-window/aggregate as applicable | Yes |
| Monthly Sharpe (`sharpe_ratio`) | Yes | Gated, V1 + V2 | Yes | Reported when defined | No |
| Daily Sharpe | Gated | Gated, V1 + V2; Fast-only | No | Gated final IS/dense OOS | Yes, with observation/active-day diagnostics |
| SQN | Yes | Gated, V1 + V2 | Yes | Reported when defined | Yes |
| Monthly Sortino | Yes | No | Yes | Reported when defined | No |
| Ulcer Index, Consistency | Yes | No | Yes | Reported when defined | No |
| Composite Score | Yes | No | No | Selection metadata where applicable | No |
| Total Trades, Max Consecutive Losses | Constraint/reporting; core direction exists | V2 Fast objective; V1 reporting/constraint | V2 Slow objective; V1 reporting/constraint | Yes | Yes |
| Winning/losing trades, gross profit/loss | Reporting | Fast result facts | Slow result facts | Per-window where carried | Yes |
| Guardrail diagnostics | No | V2 execution facts | V2 selected reference facts | Stored diagnostics where applicable | Yes |
| Bar-close MTM Max DD | No | Internal request-gated V2 sidecar only | No | No | Position and signal-reversal families |

Grid permits at most six Fast objectives. The common V1/V2 Fast set is Net
Profit %, realized Max DD %, RoMaD, Profit Factor, Win Rate, Monthly Sharpe,
Daily Sharpe, and SQN. V2 additionally allows Total Trades and Max Consecutive
Losses. Slow refinement supports the common Fast set except Daily Sharpe and
adds Sortino, Ulcer Index, and Consistency; V2 also allows the two count/loss
metrics. Availability is validated by the current constants in
`src/core/grid_engine.py`.

V1 Optuna exposes one to six selections from Net Profit %, realized Max DD %,
Monthly Sharpe, Daily Sharpe, Sortino, RoMaD, Profit Factor, Win Rate, SQN,
Ulcer Index, Consistency, and Composite Score. Optuna remains V1-only for new
optimizer execution.

## Strategy Lab MTM drawdown

`max_drawdown_mtm_pct` is a request-gated research column for the generic V2
position and signal-reversal families. It scans finite bar-close MTM equity from
`trade_start_idx` through the final bar and seeds the peak with positive
initial capital. An empty interval or any non-finite observation returns NaN;
a present flat interval returns `0.0`; negative equity can produce drawdown
above 100%. Both families support reference and compiled execution, including
MTM-only requests with Sharpe, Daily Sharpe, and SQN disabled.

This metric is not realized Max DD. As defined by the
[V2 architecture ABI](engine_v2/ARCHITECTURE.md#compiled-execution-and-resources),
compiled execution transports it in an optional sidecar without changing the
compiled result ABI. It is also not TradingView intrabar/open-excursion
drawdown, which can use within-bar High/Low behavior unavailable to this
bar-close series. A future `romad_mtm` would use NaN when MTM drawdown is zero,
unlike established realized RoMaD.

The Strategy Lab schema-v2 metric axis is authoritative in
`tools/strategy_lab/dataset.py`; its analysis and allocation interpretation is
documented in the [Strategy Lab guide](../tools/strategy_lab/README.md).
