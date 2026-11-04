# Historical ticker-selection research

This local research workflow ranks tickers from saved independent-account or
selected-candidate outcomes. It executes no strategy and needs no market pack
for planning or evaluation. Methods are exploratory; results do not establish
an edge or production shared/carried-capital portfolio behavior. The existing
[X workflow](SELECTION_SCHEMA.md), allocator, engine and historical formats are
unchanged.

## Commands and direct APIs

Run from the repository root with the configured Python and a new empty output
directory. Success prints one JSON object; diagnostics go to stderr and expected
input/publication errors exit 2. No resume or overwrite is supported.

```text
python -B -m tools.strategy_lab.selection history-plan REQUEST --ticker-list UNIVERSE --output-dir PLAN
python -B -m tools.strategy_lab.selection history-fixed-request --plan PLAN --template TEMPLATE --warmup-bars 100 --output-dir TRANSFORM
python -B -m tools.strategy_lab.selection history-evaluate --plan PLAN --fixed-dataset off=OFF --fixed-dataset on=ON --parameter-set off=base --parameter-set on=base --baseline off --output-dir REPORT
python -B -m tools.strategy_lab.selection history-evaluate --plan PLAN --dataset off=OFF --dataset on=ON --scope temporal_dev --history-scope development --candidate-policy POLICY --unlock-scope --unlock-policy off=EVIDENCE --unlock-policy on=EVIDENCE --baseline off --output-dir REPORT
```

Fixed also accepts `--allow-partial`; structurally absent cells always refuse.
Candidate evaluation requires exactly one `--rule` or `--candidate-policy`.
`--scope-for LABEL=NAME` and `--history-scope-for LABEL=NAME` override their
respective defaults. Locked initial history requires its own
`--history-unlock-scope --history-unlock-policy LABEL=PATH`. Evaluation unlock
flags never unlock history. Unknown/duplicate labels and irrelevant family
options refuse. An unlock document is distinct from the candidate policy.

```python
from tools.strategy_lab.selection import (
    prepare_history_plan, open_history_plan, write_history_fixed_request,
    evaluate_history,
)
prepare_history_plan(request_path, plan_dir, ticker_list=universe_path)
plan = open_history_plan(plan_dir)
write_history_fixed_request(plan, template_path, transform_dir, warmup_bars=100)
evaluate_history(plan, report_dir, fixed_inputs={"off": off_dir, "on": on_dir},
                 parameter_sets={"off": "base", "on": "base"}, baseline="off")
# Candidate alternative: existing DatasetInput(label, dataset, resolved_scope),
# with a separate history_scopes={label: dataset.resolve_scope(...)} mapping.
evaluate_history(plan, report_dir, inputs=inputs, history_scopes=history_scopes,
                 candidate_policy=loaded_policy, baseline="off")
```

Plan paths or loaded plans are accepted. Loaded plan, universe and policy
snapshots detect mutation; provenance is detached. Inputs, source/filter roots,
unlock evidence and application/research roots are protected from output writes.
Symlink escapes refuse. Each completion descriptor publishes last.

## Closed request and calendar

All fields are required; unknown fields and implicit research defaults refuse.
Here is a complete monthly Fixed example with four initial history accounts per
ticker, followed by four target accounts:

```json
{
  "schema_version": "strategy_lab_history_request_v1",
  "name": "fixed-history-example",
  "calendar": {"mode": "fixed", "start": "2024-01-01", "end": "2024-08-31",
               "tradingTimeframeMinutes": 60, "periodMonths": 1},
  "evaluationStart": "2024-05-01",
  "historyWindows": 4,
  "topN": 2,
  "methods": ["prior_oos_positive_count", "prior_oos_mean"],
  "referenceMethod": "prior_oos_mean",
  "controls": {"seed": 17, "draws": 32}
}
```

`name` is nonblank. `topN` is a positive integer no larger than the explicit
universe. `historyWindows` is exactly integer 4. Methods form a unique nonempty
list; `referenceMethod` belongs to it. Seed is an integer in 0..2^64-1, draws in
1..100000; booleans refuse. Trading TF is a positive integer multiple of five
dividing 1440. The existing exploratory universe-v1 document supplies exact
canonical full provider IDs, with no silent membership changes.

For IS/OOS, calendar mode is `is_oos`; replace `periodMonths` with positive
integer `isMonths` and `oosMonths`. Start means first IS start. Example: IS4/OOS1
Start January 1, End December 31, evaluationStart September 1 emits May–August
as initial history and September–December as targets. It needs data through
January 1, plus the configured warmup before the initial IS. To use standard
development windows 1..6 and locked temporal_dev 7..8, evaluationStart is
November 1 instead; November uses July–October and December uses August–November.

Core `build_calendar` owns live creation. Fixed accounts cover emitted rebalance
groups at `periodMonths` cadence; candidate accounts cover individual complete
OOS windows at `oosMonths` cadence. Save requested inclusive endpoints, normalized
half-open intervals, IS bounds and any trailing interval. `evaluationStart` must
equal an emitted start with at least four preceding complete blocks and one
target. All earlier blocks are excluded from **every** headline/control/delta/
concentration result, including selected-IS. Each target uses exactly its four
immediate predecessors; no older replacement for a missing middle observation.

Four two-month blocks mean eight months of history in either family. Fixed may
have a final short group of complete months; reports expose mixed target
durations and do not normalize or annualize them. Candidate windows must be
complete. Gaps, overlaps and clipped joins refuse.

`plan.json` (`strategy_lab_history_plan_v1`) contains exactly `schema_version,
request,universe,calendar,mapping,git,plan_identity,artifacts`. Request saves
`path,file_sha256,raw,effective,effective_sha256`; universe uses the existing
detached provenance contract. Calendar has `requested_start,requested_end,
exclusive_end,periods,trailing`. Mapping has `blocks,history_ids,target_ids`;
each block is `id,start,end,isStart`. The semantic plan digest binds version
`strategy_lab_history_plan_identity_v1`, effective request, canonical roster,
calendar and mapping. Offline reopening verifies identities, artifacts and
frozen calendar invariants without invoking live core/calendar or market access.

## Fixed coverage, transformation and saved authority

The transform preserves template bytes and all strategy/economic/filter/scalar
settings. Only `run_name`, `generation.periods` and
`generation.expected_run_count` change. It requires the full roster and exactly
one matching trading TF. Period names are `history-period-ID`; each inclusive
saved End is `block.end - trading_step`. Explicit warmup is 100..5000 bars.
The existing offline Fixed structural validator checks the transformed request.

`transform.json` (`strategy_lab_history_fixed_transform_v1`) saves
`template_path,template_sha256,plan_identity,plan_sha256,warmup_bars,
original_periods,period_map,changed_fields,request_sha256,artifacts` plus its
schema. It links preserved `template.json`, generated `fixed-request.json` and
HTML. Period-map entries contain source period ID/key and target period.
There is no X listing evidence in a history plan. Full market and filter coverage
including warmup is still required for **every** independent account.

Use the existing preflight route before execution:

```powershell
$py = 'C:/Users/MT/Desktop/Strategy/S_Python/.venv/Scripts/python.exe'
$work = Join-Path $env:LOCALAPPDATA 'Temp/strategy-lab-history-example'
# Supply explicit plan/request/universe/template files under $work first.
& $py -B -m tools.strategy_lab.selection history-plan "$work/request.json" --ticker-list "$work/universe.json" --output-dir "$work/plan"
& $py -B -m tools.strategy_lab.selection history-fixed-request --plan "$work/plan" --template "$work/off-template.json" --warmup-bars 100 --output-dir "$work/off-transform"
& $py -B -m tools.strategy_lab.fixed validate "$work/off-transform/fixed-request.json"
& $py -B -m tools.strategy_lab.fixed run "$work/off-transform/fixed-request.json" --data-root "$work/pack" --output-dir "$work/off-dataset" --preflight-only
& $py -B -m tools.strategy_lab.fixed run "$work/off-transform/fixed-request.json" --data-root "$work/pack" --output-dir "$work/off-dataset" --resume
& $py -B -m tools.strategy_lab.selection history-evaluate --plan "$work/plan" --fixed-dataset "off=$work/off-dataset" --output-dir "$work/report"
```

For a second filter configuration, repeat transform/preflight/run from its own
template, then supply both saved datasets with explicit labels/baseline. Linux
uses its configured Python and native paths. No template, pack or trading
settings are silently created by these commands. The January monthly example
at TF60/warmup100 first consumes December 27 at 20:00 UTC. Late listings or gaps
can abort preflight before a manifest exists: choose a covered roster or later
Start explicitly. Partial datasets are not a sparse late-listing representation.

Fixed v1/v2 and complete candidate Parquet v3/v4 are supported; versions can mix
within a family. CSV/v2 and cross-family inputs refuse. Fixed uses exact
ticker/TF/interval/set joins, constant non-runtime parameters/economics, and the
accepted last-bar-open End restriction. Saved date-only End accounts may be
refused even if their bar coverage is equivalent. Every structural cell must
exist. Declared pending/failed cells need partial opt-in; corruption always
aborts. Curves validate once per account, retaining only scalars.

Changing K, methods, random settings or evaluationStart requires a new plan but
does not require an engine rerun when exact saved cells match. Transform identity,
display names and experiment settings are not account joins.

Candidate initial-history and target roles are resolved independently by the
existing scope owner, with no partial-scope override or invented union. Both
must contain the full roster and have the same ticker cell within a label.
Scope definitions may overlap and contain unused inputs; consumed roles are
disjoint. Record unused tickers/windows separately by role. Completed earlier
targets may supply subsequent history under their original evaluation authority.
Every label freezes its IS choices before any label reads OOS for that window.
The existing saved trade gate, policy/rule geometry and candidate ties remain.

## Causal decisions and missingness

Evaluation first validates a scalar ledger once. Physical OOS reads can precede
historical ranking. The chronological decision trace is separate from the actual
candidate IS-freeze/OOS-read trace; it makes no fictitious I/O-order claim.

The pure scorer receives only `schema_version,settings,block,prior,current`.
Version is `strategy_lab_history_decision_v1`. Settings contain exactly
`method,methodVersion,topN,roster,tradingTimeframeMinutes,family,geometry,
historyWindows,strategy_settings`. Local block fields are `start,end,isStart`.
Each prior entry contains `block,rows`, with full-roster rows containing exactly
`status,reason,value,account_id,candidate_id,semantic_key,is_net_profit_pct,
is_total_trades`. `current` is null for Fixed; candidate current rows contain
only `candidate_id,semantic_key,is_net_profit_pct,is_total_trades`.

The four prior rows completed before the cutoff. Selected-IS consumes no prior
feature rows. No current payoff/status/reason/metrics/account state or future row
enters the scorer. Candidate presence is determined from the frozen candidate ID,
never its OOS status. Fixed has no current-account gate. Stable settings bind
strategy/version, execution policy, effective filters without source paths,
normalized Fixed parameters or saved candidate params/geometry/rule registry,
and declared candidate rule/policy (excluding its display name). Full sources,
scopes and unlock evidence remain separate provenance.

Decision identity is SHA-256 of canonical JSON of that closed projection:
UTF-8, sorted keys, compact separators, no NaN, Unicode unescaped. Exclude display
labels/names, direct source-path fields, scope names/evidence bytes, global hashes, whole ledger/plan
identity, calendar End, evaluationStart, method list, referenceMethod, seed/draws
and later rankings. Local prior keys/IDs and consumed scalars remain included.
Unavailable numeric facts normalize to null, with explicit reasons. An unchanged
cutoff therefore keeps its identity when only future/global evidence changes.

Direct path exclusion does not make prior Fixed account identities path-independent.
A filtered Fixed `account_id` hashes the effective filter descriptor, including
its source locator (`fixed/storage.py`). Regenerating equivalent accounts with a
different bound filter root can therefore change a history decision identity.
Relocating an unchanged saved artifact is distinct from regenerating accounts
with a changed locator. Random pool keys remain unchanged when their own eligible
roster and settings are unchanged; no account-identity normalization is applied.

| Saved observation | Meaning |
| --- | --- |
| `completed`, finite value (including zero/no trades) | Known return |
| `abstained`, reason `no_eligible_is_candidate`, value 0 | Known policy cash, null candidate/IS fields |
| `unknown`, reason `nonfinite_oos`, `nonfinite_net_profit_pct`, `pending` or `failed` | Unknown return, never zero |

Every roster ticker retains shadow history even when unfunded. Simple methods
may consume past abstention zeroes; tc4w needs finite historical selected IS too.
All candidate ticker scorers require a current candidate with finite IS profit.
An existing rule can select a candidate with nonfinite IS profit: preserve that
candidate and its OOS shadow payoff, but exclude it from current ticker scoring.
A later simple method can use finite OOS from that row; tc4w cannot use its
missing IS. This finite-IS ticker gate and retained abstention-zero history
deliberately differ from meta104's candidate gate and dropped abstention rows.

One unavailable required prior row makes a ticker ineligible until four adjacent
usable rows exist again. It cannot be replaced after a selected target becomes
unknown. Previously unfunded re-entry means an already covered, unselected ticker.
Required metric columns/bindings are structural requirements; missing columns
are not converted to unknown observations.

## Methods and controls

All components below rank descending; final ties use ascending full ticker ID.
Feature reductions use float64 `np.mean`/`np.min` over chronological four-row
history, matching the frozen IS4/monthly-OOS reference.

| Method | Score tuple |
| --- | --- |
| `prior_oos_positive_count` | count(prior return > 0) |
| `prior_oos_mean` | mean(prior return) |
| `selected_is_net_profit` | current selected IS net profit |
| `transfer_consistency_4w_v1` | positive count, minimum return, mean return, mean(return − historical IS/4), current selected IS profit |

Fixed supports only the first two; the names' `oos` means prior independent Fixed
account returns, not invented IS/OOS. Selected-IS is candidate-only. Tc4w requires
IS4/OOS1; division by four is a frozen descriptive proxy, not compounding or a
universal transfer estimate. Simple controls intentionally omit the September
scripts' secondary cascades. No weights, percentile composites or best-method
selection are added.

Top/bottom/random select min(K,E), weight 1/K and retain unused slots as cash.
Bottom takes the last min(K,E) entries of the same published ranking, including
the highest full IDs within a boundary tie. Top and bottom are disjoint when
E>=2K; when K<E<2K they overlap in 2K-E entries, and when E<=K both contain every
eligible ticker (empty at E=0). Equal scores convey no method preference, and
disjoint controls do not establish predictive ability. All-method-eligible
weights 1/E (E=0 cash); all-declared weights 1/R. Current abstention is known
cash; unknown selected outcomes invalidate required returns without replacement.
`planned_cash_weight` is unused slots only; `cash_weight` also includes abstention.
`funded_weight` includes unknown assigned slots. Complete-only equal-block means,
canonical float64 member sums, sequential block accumulation, linear quantiles,
exact `<`/`==` midrank and paired deltas reuse accepted X arithmetic. No compounding,
portfolio equity/DD/Sharpe, annualization, bootstrap or p-value is reported.

Random pool projection is exactly `schema_version:strategy_lab_history_random_pool_v1,
block_key:[start,end],tradingTimeframeMinutes,topN,seed,draws,eligible` (canonical
ascending IDs). Pool key is its canonical SHA-256; seed is the first eight digest
bytes interpreted unsigned big endian. Exclude labels, method, outcomes, future
history, paths and input hashes. Identical cutoff pools share one plan across
labels/methods. PCG64 choice without replacement produces sorted indices.

Actual `plans/POOL_KEY.npy` arrays have shape `(draws,min(K,E))`, including zero
width. Dtypes are uint8 for E<=256, uint16 for E<=65536, otherwise uint32. Save
projection/key, eligible roster, shape/dtype, derived seed, content SHA-256,
artifact size/hash and NumPy/algorithm provenance. Validation checks actual
bytes, sorted uniqueness/range and shape without regenerating RNG. Unique count
and payload bytes are printed before allocation; actual saved bytes are recorded.
No label×method×block×draw×ticker cube exists. Memory retains scalar history,
per-context draw sums/flags and one plan at a time. Random turnover uses draw
flags with sequential plan mapping.

Random outcomes share a context per `(label,target pool-key sequence)`;
all-declared is computed once per label. Methods reference these contexts for
midrank. Within-label random block deltas are omitted whenever that block's pool
keys agree, even if whole-path contexts differ or payoffs are incomplete.
Identical whole contexts also omit within-label path deltas. Cross-label deltas
remain present, including legitimate zero distributions.
Cross-context random block pairing requires equal pool keys; whole-path pairing
requires equality in every target. Otherwise reason is `different_random_pools`.
Deterministic deltas remain descriptive when memberships differ, with overlap
and exclusions visible. Baseline comparisons mean **whole-system comparison:
configuration plus its own history-based selection**, not isolated F06 causality.

Block concentration groups use numeric chronological block-ID order, retaining
string IDs in reports. Equal positive contributions designate the earliest block
as the largest group. Ticker ordering and contribution arithmetic are unchanged.

These corrections retain the v1 schema and method versions. Existing saved
reports remain immutable; use a new output directory for corrected bottom controls.
Accepted X bottom semantics are unchanged.

## Published authority and exact columns

`history-evaluation.json` (`strategy_lab_history_evaluation_v1`) publishes last.
Fields: `schema_version,git,plan_path,plan_sha256,plan,sources,strategy_settings,
method_versions,candidate_rule,candidate_policy,family,baseline,
candidate_access_trace,history_decision_trace,controls,interpretation,inference,
artifacts`. `inference` is `not_requested`. Sources retain full validated
contract/manifest/scope/filter/policy identities and per-cell IDs, including
role-specific unused inputs. Controls contain `algorithm,numpy_version,
unique_plan_count,total_array_bytes,saved_array_bytes,plans`.

Canonical JSON ledger, decisions and actual random arrays are structured
authority. CSVs/HTML are derived readable projections. No public result-reopen
API or second formula implementation is provided; the offline plan reader is
retained because transforms/evaluation consume it. Artifact hashes establish
internal consistency, not authenticity. Nulls are JSON null and empty CSV cells;
nested fields are compact JSON in CSV. Arrays and numeric values never serialize
NaN.

* `history.json`: schema `strategy_lab_history_ledger_v1`, `rows`. Row/CSV fields:
  `label,block_id,block_key,history_only,ticker,status,reason,value,account_id,
  candidate_id,semantic_key,is_net_profit_pct,is_total_trades,metrics`. Field
  ordering follows the adapter; names are stable. Metrics are a scalar object.
  Current payoff/account facts live here, separately from decision inputs.
* `decisions.json`: schema `strategy_lab_history_decisions_v1`, `decisions`.
  Each decision contains `decision_identity,rows,ordered,eligible,tie_groups,
  label,method,block_id,block_key,pool_key,history_block_ids`. Each row contains
  `ticker,candidate_facts,history_refs,eligible,reason,features,score,rank,selected,
  bottom`. History references are `[start,end,ticker]` within the decision's
  label. Tie groups contain `score,tickers`. Features have the named score
  components `positive_count,minimum_return,mean_return,mean_transfer_gap,
  selected_is_net_profit` appropriate to the method. Ineligible features/score
  are empty, rank null; reasons are `no_current_candidate`,
  `nonfinite_current_is`, `unavailable_adjacent_history`, or
  `unavailable_historical_is`. `decisions.csv` prefixes row fields with
  `label,method,block_id,block_key,decision_identity,pool_key`.
* `blocks.csv`: `label,context,arm,block_id,block_key,level,unit,
  planned_cash_weight,value,reason,unknown_tickers,expected_denominator,
  selected_count,active_count,abstained_count,missing_count,occupied_weight,
  funded_weight,cash_weight,expected,finite,mean,median,p05,p95`.
  Arm levels are `arm_block` or `random_block_distribution`; nonapplicable
  fields are empty. Deterministic context is method ID or `all_declared`;
  random context is the summary context key. Unit is percent reference return.
* `comparisons.csv`: union of `label,method,reference_label,reference_method,
  interpretation,level,arm,block_key,value,reason,top_overlap,left_only,
  right_only,same_pool,expected,finite,mean,median,p05,p95`. Levels are
  `paired_block`, `paired_headline`, `paired_random_block_distribution`,
  `paired_random_path_distribution`. Deltas are percentage points.
* `turnover.csv`: `label,context,arm,block_id,added,removed,retained,
  added_distribution,removed_distribution,retained_distribution`.
  Deterministic lists describe actual membership changes; random fields are
  draw distributions. First targets have no prior target turnover.
* `concentration.csv`: union of `label,context,arm,group_kind,expected_blocks,
  participation_blocks,unknown_participation_blocks,group,contribution,expected,
  finite,observed_subtotal,complete_total,partial_support,positive_gross,
  absolute_negative_gross,positive_gross_share,largest_positive_group,
  largest_positive_share,total_excluding_largest,reason`. Groups are tickers or
  target blocks; contribution units are percentage points of equal-block return.
* `summary.json`: `strategy_lab_history_summary_v1`, `expected_blocks,
  excluded_history_blocks,duration_month_counts,headline,datasets,
  random_contexts,coverage`. Each dataset has shared `all_declared` and method
  `arms,random_context`; arms retain `finite_only_descriptive`. Random contexts
  contain `label,pool_keys,methods,path_mean_distribution,method_midrank,coverage`.
  Each deterministic arm and random context also has coverage fields
  `expected_blocks,unknown_selected_blocks,selected_observations,
  unknown_observations,abstained_observations,mean_cash_weight,
  mean_planned_cash_weight`. Random observation counts are draw-averaged counts
  summed over targets; they are not counts of independent samples.
  Distribution objects use `expected,finite,mean,median,p05,p95,reason`; incomplete
  required observations make statistics null. Coverage counts target statuses.

All CSVs use the union of fields in first-seen order. An empty table has the
single `status` header. `index.html` links all artifacts and shows target count,
excluded history, durations and target Top-K means. Historical artifacts remain
immutable; recomputation writes a new directory.

## Separate X comparison on the same saved cells

There is no history `--x-ranking` option. Prepare X over targets only and evaluate
the same full-history saved datasets using accepted `evaluate`. For monthly
Fixed targets starting May 1, set X Start May 1. For IS4/OOS1 targets starting
September 1, set X Start May 1 (September minus four IS months). Set X cadence
one, matching End/TF/full roster; verify actual emitted target bounds.

```powershell
& $py -B -m tools.strategy_lab.selection prepare "$work/x-target-request.json" --ticker-list "$work/universe.json" --data-root "$work/pack" --output-dir "$work/x-target-ranking"
& $py -B -m tools.strategy_lab.selection evaluate --ranking "$work/x-target-ranking" --fixed-dataset "off=$work/off-dataset" --output-dir "$work/x-report"
# Candidate alternative, with the same target scope/unlock evidence:
& $py -B -m tools.strategy_lab.selection evaluate --ranking "$work/x-target-ranking" --dataset "off=$work/is-off" --scope temporal_dev --unlock-scope --unlock-policy "off=$work/unlock.json" --candidate-policy "$work/policy.json" --output-dir "$work/x-candidate-report"
```

Earlier history cells are reported unused by X. Compare equal target bounds and
underlying cell payoffs, not merely report labels. X and history random controls
use distinct seed/identity contracts and are never paired by draw index. A unified
X/history report, carried accounts, data acquisition and market experiments remain
separate work.
