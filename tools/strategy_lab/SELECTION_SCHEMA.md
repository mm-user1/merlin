# Merlin X selection: commands, frozen contracts and interpretation

This research-only interface reuses the production
[ticker selector](../../docs/TICKER_PORTFOLIOS.md) for live preparation and the
existing [Fixed and candidate readers](README.md) for saved outcomes. Selection
code is in `selection/`; it does not change the allocator or execute a strategy.

## Commands and Python API

Run from the repository root with the configured interpreter and an explicit,
new empty output directory. Each successful selection CLI command emits one JSON
object to stdout; diagnostics/progress go to stderr. Expected invalid inputs exit 2.

```text
python -B -m tools.strategy_lab.selection prepare REQUEST --ticker-list UNIVERSE --data-root PACK --output-dir RANKING
python -B -m tools.strategy_lab.selection fixed-request --ranking RANKING --template FIXED_REQUEST --warmup-bars 100 --output-dir TRANSFORM
python -B -m tools.strategy_lab.selection evaluate --ranking RANKING --fixed-dataset off=OFF --fixed-dataset on=ON --parameter-set off=base --parameter-set on=base --baseline off --output-dir REPORT
python -B -m tools.strategy_lab.selection evaluate --ranking RANKING --dataset off=OFF --dataset on=ON --scope research --rule primary_profit --baseline off --output-dir REPORT
```

Fixed accepts repeatable `--parameter-set LABEL=NAME` and explicit
`--allow-partial`. Candidate evaluation instead requires exactly one `--rule`
or `--candidate-policy PATH`; `--scope-for LABEL=NAME` overrides `--scope`.
Locked scopes require `--unlock-scope --unlock-policy LABEL=PATH` and the existing
scope owner's evidence validation. Labels must be unique nonblank display names;
unknown label overrides and irrelevant family options fail. Fixed and candidate
families cannot be mixed in one evaluation. There is no intersection mode and
no automatic holdout unlock, ticker replacement, strategy rerun or resume here.

`tools.strategy_lab.selection` exports:

```python
prepare_ranking(request_path, output_dir, ticker_list=universe_path, data_root=pack)
ranking = open_ranking(ranking_dir)
write_fixed_request(ranking, template_path, output_dir, warmup_bars=100)
result = evaluate_selection(ranking, fixed_inputs={"off": fixed_dir},
                            parameter_sets={"off": "base"}, allow_partial=False)
# Candidate alternative: inputs=[DatasetInput(label, dataset, resolved_scope)],
# candidate_rule="primary_profit" OR candidate_policy=load_candidate_policy(path).
write_selection(result, output_dir)
```

Paths or loaded rankings are accepted by transform/evaluate. Loaded ranking
snapshots detect mutation. Reports detach metadata from mutable input snapshots.
All outputs must be disjoint from input artifacts/documents, market/filter roots
and protected application/research roots. Existing nonempty output directories
and symlink escapes fail. Each publication writes its descriptor last; interrupted
directories cannot be treated as completed or resumed. Fixed execution retains
its own existing resume lifecycle.

## Explicit request v1

`strategy_lab_x_request_v1` is closed: every field below is required and unknown
fields fail. There are no implicit scientific defaults. See
[the complete example](runspecs/x_selection_example.json).

| Object | Fields and domain |
| --- | --- |
| Root | `schema_version`, nonblank `name`, `calendar`, `tickerSelector`, `controls` |
| `calendar` | `mode` (`fixed` or `is_oos`), `start`, inclusive `end`, `tradingTimeframeMinutes`; only IS/OOS additionally requires `isMonths`, `oosMonths` |
| `tickerSelector` | `version:1`, `enabled:true`, `method:"meanBarRangePct"`, `methodVersion:1`, `topN`, `rebalanceMonths`, boolean `evaluateUnselected`, `params` |
| `params` | `lookbackMonths`, `timeframeMinutes` |
| `controls` | integer `seed` in 0..2^64-1; integer `draws` in 1..100000; booleans fail |

Count settings accept positive whole numbers as core does; effective counts are
integers. Trading TF is an integer multiple of five dividing 1440. Ranking TF
uses the actual core/provider domain: positive integer multiples of five within
the supported date range, including 70-minute and multi-day bars. It need not
divide a day. K must not exceed the declared roster; eligibility can underfill K.

The universe is the existing `strategy_lab_universe_v1` exploratory document with
unique canonical full `VENUE_CONTRACT` IDs; case aliases are not accepted here.
Preparation calls core `normalize_selector`, `normalize_pool`, `build_calendar`
and `rank_pool` directly, with a fresh cache at each cutoff. Core owns the X
formula, complete prior-month bounds, listing exclusions, 99% coverage, price
validation, ordering and tie breaks. Missing/invalid history propagates failure.

Start must be month-day 1 at 00:00 UTC. Date-only End includes that day; core
converts it to the next trading-grid boundary. Only full months/OOS windows enter
the calendar; the leftover interval is recorded. Fixed groups monthly windows
into rebalance periods; IS/OOS groups complete OOS windows and requires cadence
to be a multiple of OOS width and at least two windows. Final groups may be short.
All saved logical bounds are UTC half-open `[start,end)`.

## Frozen ranking and identities

`ranking.json` (`strategy_lab_x_ranking_v1`) contains `request` (raw/effective,
raw byte SHA-256, effective canonical SHA-256, original path), `universe` (raw
byte and document hashes/snapshot/path), canonical `pool` sources, `calendar`,
`git`, `ranking_identity`, `controls`, `controls_identity`, and `artifacts`.
`calendar` has `requested_start`, `requested_end`, `exclusive_end`, `periods`,
and optional `trailing` interval. Each period has `id,start,end,windows`; each
window has `id,start,end,isStart` (null for Fixed).

`ranking_rows.json` is authoritative (`strategy_lab_x_rows_v1`, `periods`). Each
period record has `id,period_key,rows,ranking_decision_identity`. Every roster
instrument occurs exactly once per period. Row fields are
`instrument,source,listing,eligible,reason,score,rank,selected,months,reason_code`.
Eligible rows sort by descending score then ascending full ID; ineligible rows
follow by full ID. Listing-ineligible rows have no score/rank/months/selection
and `reason_code="listing_after_lookback_start"`. Eligible reason fields are null.
Each month pin has `month,start,end,expected,observed,fingerprintVersion,fingerprint`.
Scores are mean bar-range percentages; ranks are one-based.

`ranking.csv` is an exact checked readable projection with these headers:

```text
period_id,cutoff,instrument,eligible,reason_code,reason,listing,score,rank,selected,months
```

Offline v1 reading validates schema, identities, row order/membership/ranks,
listing facts, month bounds/counts/fingerprints, calendar invariants, CSV agreement
and all artifact hashes. It uses the frozen v1 contract, not live X/calendar
normalization or market reads. Unknown versions fail. Hashes establish internal
integrity, not a signature proving that an untrusted author actually ran core.

The semantic hashes use canonical JSON (sorted keys, compact separators, no NaN)
and SHA-256. Separate identities serve separate purposes:

* `ranking_identity` binds active selector settings excluding inert
  `evaluateUnselected`, sorted roster, trading TF, normalized calendar and semantic
  rows (including consumed monthly fingerprints/listings). Controls, templates,
  outcome labels/values, paths, universe order/notes and display names are excluded.
* `ranking_decision_identity` binds only that cutoff's common selector/roster/TF,
  period key, cutoff and row evidence. Later price changes cannot reseed earlier
  periods. The period's declared end remains part of its planned key.
* `controls_identity` additionally binds ranking identity, seed/draws/algorithm,
  each period key, sorted eligible IDs, index dtype/shape and content hash.
  NumPy version is generation provenance, not semantic identity.

`controls` records `seed,draws,algorithm,numpy_version,total_array_bytes,periods`.
The algorithm is `numpy_generator_pcg64_choice_without_replacement_v1`.
Each plan records `period_id,period_key,eligible,shape,dtype,derived_seed,file,
content_sha256`. Shape is `[draws,min(K,E)]`, including `[draws,0]`. Unsigned
index dtype is `|u1` for E<=256, `<u2` for E<=65536, otherwise `<u4`.
The content hash covers C-order index bytes; the artifact hash covers the NPY file.
The derived seed is the first eight SHA-256 bytes, big endian, of canonical JSON:

```text
{schema_version:"strategy_lab_x_random_seed_v1", seed, draws,
 ranking_decision_identity, period_key}
```

Preparation saves actual sorted, unique, in-range indices in `plans/ID.npy`, one
plan per rebalance period. Draw d identifies the same path across periods and
labels. Members are held for every window in a period. Total array bytes are
reported before allocation; allocation/validation are per period. Offline replay
reads those arrays (no RNG regeneration, pickle or compressed archives), so a
different installed NumPy version does not silently change historical draws.

## Offline Fixed transform and saved outcome joins

`transform.json` (`strategy_lab_x_fixed_transform_v1`) binds template byte hash,
ranking byte/semantic identities, controls identity, warmup, original periods,
explicit source-to-target period map, changed fields and canonical request hash.
It retains byte-identical `template.json` and writes `fixed-request.json`.
Only `run_name`, `generation.periods`, `generation.expected_run_count` change.
Each Fixed account covers one full rebalance group; inclusive last-bar End is
logical end minus one trading bar. Warmup is explicitly 100..5000 bars.
Template roster and single trading TF must match exactly. Known listings later
than any required warmup start refuse before creating output. An instrument can
be X-ineligible yet executable; it remains a required control outcome. Structural
validation is offline; the existing Fixed runner owns live preflight/admission.

Fixed v1/v2 datasets may mix across labels. Select one named parameter set per
label, or omit only when it is unambiguous. The full requested roster/period
matrix must exist with exact instrument, trading TF and evaluation bounds;
continuous accounts cannot substitute monthly slices. Duplicate semantic cells,
ambiguous parameter sets, varying non-runtime parameters/economics and invalid
saved contracts fail. The existing reader validates each completed account once;
the adapter keeps scalar metrics only. Additional declared accounts are recorded
as unused. Partial datasets require explicit opt-in; pending/failed cells remain
unknown. Any reader integrity failure still aborts.

Candidate datasets must be complete Parquet v3/v4 (mixed versions allowed).
Legacy CSV v2 is unsupported. Scope authority must contain every requested ticker
and exact IS/OOS window; extra authorized tickers/windows are recorded unused.
The existing reader and policy/rule owners validate saved inputs and metric
availability. For each target window all labels freeze their IS selections before
any label's OOS reveal. Existing `evaluate_rule`/candidate policy and
`select_candidates` choose at most one candidate per ticker. Neither candidate
availability, IS profit nor trade counts may change the frozen X roster/rank.
No eligible candidate means known zero-return cash without replacement; selected
nonfinite OOS means unknown. Source descriptors retain detached scopes/unlock
evidence, bindings/pins, contracts, hashes and decision access facts.

## Returns, coverage and controls

Each account starts with its own declared reference capital. Its saved
`net_profit_pct` feeds a normalized weighted return, not a combined equity curve.

| Arm | Members | Weight per member |
| --- | --- | --- |
| `x_top_k` | Highest X scores, ascending full-ID ties | 1/K |
| `x_bottom_k` | Lowest X scores, ascending full-ID ties | 1/K |
| `all_declared` | Entire declared roster U | 1/U |
| `all_x_eligible` | Entire X-eligible roster E | 1/E; E=0 is cash |
| `random_k` | Each saved draw's min(K,E) members | 1/K |

Underfill and IS abstention stay cash. Unknown outcomes keep their assigned slots
and invalidate every return requiring them; they never become zero or trigger
reweighting. `occupied_weight` counts membership, `funded_weight` counts assigned
non-abstained slots (including unknown), and `cash_weight` counts underfill plus
known abstention. A completed no-trade account with finite zero remains known.

Headline returns are equal-block means only when every expected block is finite.
Fixed blocks are whole rebalance groups; candidate blocks are individual OOS
windows. `duration_month_counts` and the headline explicitly label mixed-duration
means. Finite-only descriptive distributions remain separate. Random per-block
distributions differ from the distribution across draw-path equal-block means;
quantiles use NumPy's linear method. No averaging block quantiles. Random summaries
require every draw/path finite and expose expected/finite counts when unavailable.
X's `x_midrank_fraction` is `(count(random<X)+.5*count(random==X))/draws`, null if
X or any path is incomplete. It is descriptive, not a p-value.

Baseline comparisons pair identical block keys and, for random controls, identical
draw indices. Delta is label minus baseline in percentage points, unavailable
unless both observations are finite; path/headline deltas require the full
calendar. Strategy/filter/economics/source/IS-geometry differences are disclosed
as potential confounders. `inference="not_requested"`. No compounding, annualizing,
portfolio Sharpe/drawdown, capital carry or strict Off/on causal attribution is
claimed. Constituent account drawdowns/trades are only constituent statistics.

## Evaluation descriptor and exact CSV columns

`selection.json` (`strategy_lab_x_evaluation_v1`) publishes last. It contains
`git,ranking_path,ranking_sha256,ranking,sources,candidate_rule,candidate_policy,
family,baseline,freeze_trace,interpretation,confounders,inference,artifacts`.
The ranking descriptor and each source snapshot appear once, not as repeated
large cells. Plans are referenced by their ranking records, never copied.
`summary.json` (`strategy_lab_x_summary_v1`) contains `expected_blocks,
duration_month_counts,headline,datasets,random_interpretation`; each dataset has
`arms` and `random_path_mean_distribution`. Arm distributions also expose a
separate `finite_only_descriptive` distribution. `index.html` is a light offline
summary with complete/expected counts and links to all authoritative tables.

CSV list/dict cells are compact JSON, booleans are `True`/`False`, null is an empty
cell (JSON uses null). Columns are the following unions in first-occurrence order;
fields inapplicable to a row level are empty. Numbers use the unit column, except
explicit counts, weights, ranks, shares and constituent metrics. Distribution
fields are `expected,finite,mean,median,p05,p95,reason`.

```text
decisions.csv:
dataset_label,block_id,block_key,period_id,ticker,eligible,score,rank,selected,ranking_reason,status,reason,value,account_id,semantic_key,candidate_id,is_net_profit_pct,is_total_trades,total_trades,max_drawdown_pct,max_drawdown_mtm_pct

blocks.csv:
level,dataset_label,block_id,block_key,period_id,duration_months,arm,unit,value,reason,unknown_tickers,expected_denominator,selected_count,active_count,abstained_count,missing_count,occupied_weight,funded_weight,cash_weight,expected,finite,mean,median,p05,p95

comparisons.csv (when a non-baseline label exists):
level,dataset_label,baseline,arm,block_key,value,unit,reason,expected,finite,mean,median,p05,p95

turnover.csv (when multiple periods exist):
period_id,arm,added,removed,retained,added_count,removed_count,retained_count,expected_draws,added_distribution,removed_distribution,retained_distribution

concentration.csv:
dataset_label,arm,unit,participation_blocks,unknown_participation_blocks,expected_blocks,group,contribution,expected,finite,observed_subtotal,complete_total,partial_support,positive_gross,absolute_negative_gross,positive_gross_share,largest_positive_group,largest_positive_share,total_excluding_largest,reason
```

Decisions join on `(dataset_label,block_id,ticker)`; `block_key=[start,end]` is the
portable semantic key and `period_id` refers to shared ranking membership. Status
is `completed`, `abstained` or `unknown`. Returns/IS profit and constituent DD
are percentages, trades are counts, and `semantic_key`/`candidate_id` are null
for Fixed. Unknown reasons distinguish failed, pending and nonfinite observations.

Blocks use `arm_block` or `random_block_distribution`. For random rows active,
abstained and missing counts/weights are draw averages; selected count and
denominator are fixed. `value` applies to deterministic arms, distribution fields
to random. `unknown_tickers` identifies required missing support.

Comparison levels are `paired_block`, `paired_random_block_distribution`,
`paired_headline`, `paired_random_path_distribution`; headline/path rows have
null block keys. Without a non-baseline comparison the headers are `level,reason`
and the sole level is `not_requested`. Turnover is computed once per shared
rebalance boundary: deterministic full-ID sets and random same-draw count
distributions/means. A one-period report instead has `arm,reason` with
`arm=not_requested`.

Concentration uses each ticker's additive weighted equal-block contribution and
the original arm/calendar denominators. Random participation/contributions average
over actual draws; participation may be fractional. Unknown participation is
explicit. An all-unknown contributing ticker has null contribution; mixed known
and unknown support has only an observed subtotal. `complete_total` and
`total_excluding_largest` stay null for an incomplete arm. Concentration's
`expected,finite` count the one aggregated ticker contribution, while
`expected_blocks,participation_blocks,unknown_participation_blocks` describe
calendar support. Largest removal is subtraction, never a rerun or reweighting.

## Copyable synthetic Fixed and IS/OOS walkthrough (PowerShell)

These commands create only a fresh temporary directory. The basic publisher
repeats prices across symbols: this walkthrough checks wiring, not discriminatory
ranking or trading quality. Durable selection tests use varied instrument prices
and assert real top/bottom and Off/F06 differences. Run from the repo root.

```powershell
$py = 'C:\Users\MT\Desktop\Strategy\S_Python\.venv\Scripts\python.exe'
$work = Join-Path $env:LOCALAPPDATA ('Temp/merlin-x-example-' + [guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null
$env:MERLIN_X_EXAMPLE = $work
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:NUMBA_CACHE_DIR = Join-Path $work 'numba-cache'
& $py -B -m tools.strategy_lab.synthetic_parquet --output-dir "$work/pack" --symbols AAA BBB CCC --start 2023-12-01 --end 2024-09-01
@'
import copy, json, os
from pathlib import Path
w = Path(os.environ['MERLIN_X_EXAMPLE'])
examples = Path('tools/strategy_lab/runspecs')
def save(name, value):
    (w/name).write_text(json.dumps(value, indent=2), encoding='utf-8')
tickers = ['BYBIT_AAAUSDT','BYBIT_BBBUSDT','BYBIT_CCCUSDT']
save('universe.json', dict(schema_version='strategy_lab_universe_v1', name='synthetic',
    evidence_classification='exploratory', selection_note='Wiring example only', tickers=tickers))
x = json.loads((examples/'x_selection_example.json').read_text())
save('x-fixed.json', x)
x = copy.deepcopy(x)
x['calendar'].update(mode='is_oos', start='2024-02-01', isMonths=1, oosMonths=1)
save('x-is.json', x)
fixed = json.loads((examples/'s07_filters_fixed_off.json').read_text())
fixed['generation']['market_data']['instruments'] = [dict(instrument_id=t,cell='research') for t in tickers]
fixed['generation']['expected_run_count'] = 3
save('fixed-template.json', fixed)
candidate = json.loads((examples/'s07_filters_is_oos_off.json').read_text())
g = candidate['generation']
g['market_data'].update(timeframe_minutes=60, instruments=[dict(instrument_id=t,cell='research') for t in tickers])
g['windows'].update(timeframe_minutes=60, requested_start='2024-02-01', requested_end='2024-06-01', expected_window_count=3)
g['planning'].update(enabled_axes=['stopLP'], axis_values={'stopLP':[2,4]}, expected_candidate_count=2)
candidate['preregistration']['analysis_scopes'][0]['window_numbers'] = [1,2,3]
save('candidate.json', candidate)
'@ | & $py -B -

& $py -B -m tools.strategy_lab.selection prepare "$work/x-fixed.json" --ticker-list "$work/universe.json" --data-root "$work/pack" --output-dir "$work/fixed-ranking"
& $py -B -m tools.strategy_lab.selection fixed-request --ranking "$work/fixed-ranking" --template "$work/fixed-template.json" --warmup-bars 100 --output-dir "$work/transform"
& $py -B -m tools.strategy_lab.fixed run "$work/transform/fixed-request.json" --data-root "$work/pack" --output-dir "$work/fixed"
& $py -B -m tools.strategy_lab.selection evaluate --ranking "$work/fixed-ranking" --fixed-dataset "off=$work/fixed" --output-dir "$work/fixed-report"

& $py -B -m tools.strategy_lab.selection prepare "$work/x-is.json" --ticker-list "$work/universe.json" --data-root "$work/pack" --output-dir "$work/is-ranking"
& $py -B -m tools.strategy_lab.generate "$work/candidate.json" --data-root "$work/pack" --output-dir "$work/candidates"
& $py -B -m tools.strategy_lab.selection evaluate --ranking "$work/is-ranking" --dataset "off=$work/candidates" --scope research --rule primary_profit --output-dir "$work/is-report"
```

For an explicit Off/F06 study, prepare an independent filter-on template using the
[entry-filter workflow](README.md#generic-entry-filters), transform it against the
same frozen ranking, execute with the existing runner, and supply both labels
with `--baseline off`. Candidate inputs likewise use separately saved Off/on
datasets with the same authorized calendar. Do not change dates, roster, K,
candidate rules or filter parameters retrospectively and call it preregistered.
