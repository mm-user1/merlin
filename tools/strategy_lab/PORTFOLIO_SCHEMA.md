# Frozen carried-capital portfolio research

This Lab-only workflow executes frozen policies through existing S07
`risk_bracket_v1` singletons. It supports Off and existing F06 configurations,
Fixed v1/v2 and Parquet IS/OOS v3/v4 sources. It does not optimize, update
production storage, carry positions between windows, scale reference returns,
or establish a trading edge. S03/S06 quantity-only profiles are unsupported.

## Commands and Python API

Run from the repository root with configured Python. Every output must be a new
empty directory outside input/application/protected research roots. Ancestor
symlinks and traversal are forbidden. No overwrite or resume is supported.
Success prints one JSON object; diagnostics go to stderr; expected refusals exit 2.

```text
python -B -m tools.strategy_lab.portfolio prepare REQUEST --output-dir PLAN
python -B -m tools.strategy_lab.portfolio run --plan PLAN --data-root PACK --output-dir RESULT
python -B -m tools.strategy_lab.portfolio report --result RESULT --output-dir REPORT
python -B -m tools.strategy_lab.portfolio detail --result RESULT --variant VARIANT --data-root PACK --output-dir DETAIL
```

`run` and `detail` accept `--filter-roots FILE`, using the existing closed
`{"version":1,"roots":{"/original/filter-pack":"/relocated/filter-pack"}}`
contract. Original pins and filter origins stay unchanged. Unknown overrides or
changed contents refuse. Python exports from `tools.strategy_lab.portfolio`:

```python
prepare(request_path, output_dir)
open_plan(plan_dir)  # validated saved dict, no live sources
run(plan_dir, data_root, output_dir, filter_root_overrides=None, cancelled=lambda: False)
open_result(result_dir)  # (descriptor, plan, allocation_rows, trade_rows)
report(result_dir, output_dir)
detail(result_dir, variant_id, data_root, output_dir, filter_root_overrides=None,
       cancelled=lambda: False)
```

`prepare` is offline and uses existing scope/candidate collectors and pure
scorers. All labels' IS choices freeze before the window's reference OOS is
revealed. Historical decisions receive only the permitted closed projection.
`run` verifies required full-roster bindings/capabilities before execution and
replays consumed pins again at use. Errors/cancellation prevent completion
publication. Detail reruns one entire variant and publishes only after exact
reproduction; it neither interpolates nor changes the parent.

## Request v1

The closed `strategy_lab_portfolio_request_v1` object contains exactly:

| Field | Contract |
| --- | --- |
| `schema_version`, `name` | Version above; nonblank display name |
| `sourcePlan` | Exactly `kind` (`x` or `history`) and `path` to an accepted source directory |
| `inputs` | Nonempty, unique labels, one family; forms below |
| `candidateSelection` | Fixed: null. IS/OOS: exactly `{"rule":"primary_profit"}` (or another declared existing rule) or `{"policy":"/frozen/policy.json"}` |
| `initialCapital` | Finite positive total per independent variant, never boolean |
| `rebalanceMonths` | Positive integer, bound to source calendar |
| `variants` | Unique closed `{id,input,method}` objects; `input` references a label; no duplicate input/method pair |
| `curveBuckets` | Explicit integer 1..4096; recommended example value 512; display policy only |

Fixed input: `{label,dataset,parameterSet}` with an explicit parameter-set name.
IS/OOS input: `{label,dataset,evaluationScope,historyScope,evaluationUnlock,
historyUnlock}`. Both history fields are null for X. Each unlock is null or
`{"unlock":true,"policy":"/authorization/evidence.json"}`. History and target
scopes require separate authorization; unlock evidence is not candidate policy.

Each input needs at least one selected-method variant and an `all_declared`
control. X permits `merlin_x`; history permits its declared accepted methods:
`prior_oos_positive_count`, `prior_oos_mean`, `selected_is_net_profit`,
`transfer_consistency_4w_v1`. Existing family/geometry restrictions apply.
Tc4w remains IS4/OOS1. The source owns K, full roster, timeframe and dates.
Different timeframe/date campaigns require separate requests.

X cadence must match its source. Fixed history cadence equals `periodMonths`;
multi-month Fixed execution has no artificial monthly close. Candidate history
chunks validated targets from `evaluationStart`, using `rebalanceMonths/oosMonths`
windows; divisibility is required. A single complete target and a final shorter
group are legal. Four preceding OOS windows remain the history, even when
membership is held longer. Initial history and authorized unused cells are
recorded and never funded. Source incomplete trailing-window conventions persist.

## Execution and arithmetic

Every variant starts with the same total C. At a positive rebalance opening B,
each of K slots gets the exact binary64 B/K, retaining each unused atom as cash.
The control uses all declared tickers and denominator R. No missing ticker can
silently shrink R. Each ticker owns its balance between rebalances, closes at
every OOS end and may use new frozen parameters next OOS. Abstention retains
that balance idle; no ticker is promoted early. Nonpositive accounts retain
their signed balance without executing until a later positive pooled allocation.
Nonpositive total ends funding; remaining scheduled observations hold flat.

Use `math.fsum` over individual slot atoms at every native row and pool boundary.
Save `allocation_rounding_delta=fsum([B/K]*K)-B` without altering any allocation
or introducing a residual account. Period opening/anchor remains prior pooled B.
Strict-close equity equals balance exactly. Details of exact funding, declared
trade/growth tolerances, bar-close labels, anchored drawdowns and depleted-result
availability are in [Metrics](../../docs/METRICS.md).

Reference singleton identity must match the saved candidate. Actual execution
rebuilds a singleton with only `initialCapital` changed; its semantic key and
fingerprint may differ. Every other normalized scalar/variant and the original
segment, warmup, filter context and native origin remain bound. Internal carried
capital may be outside editor bounds. No funded result changes a later choice.

Reference comparisons are per-window T04 percentage reductions for the **same
held membership and frozen candidates**, denominator K or R. They are explicitly
`independent_reference_returns`, separate from `actual_carried_execution`.
An older rerank-every-OOS headline is non-equivalent. Off/F06 comparisons with
different memberships are whole-system comparisons.

## Saved canonical authority

JSON schemas use suffix `_v1`. Parquet uses Zstandard. Completion JSON publishes
last after validation; unclaimed files do not constitute a completed result.

`plan.json` (`strategy_lab_portfolio_plan_v1`) has exactly `schema_version,
request,source,calendar,sources,settings,ledger,decisions,access_trace,plan_identity`.
`source` stores source metadata, X records (empty for history) and source SHA-256.
`calendar` stores `family,timeframe,roster,topN,windows,groups,excluded_history_ids,
trailing`. Windows retain original `id,start,end,isStart`; groups contain
`id,windows,start,end`. `sources` stores each validated source manifest/contract,
bindings/scopes and shared candidate table once. Ledger rows reference these
bindings by account/candidate IDs; they contain window/ticker and collected
status, reasons, reference scalar metrics and candidate IS facts. Decisions map
variant IDs to `{group,cutoff,members,decision}`, retaining full source rankings.
The versioned identity hashes canonical plan fields, excluding `schema_version`,
its own identity, and request `curveBuckets`; the identity projection declares
`strategy_lab_portfolio_plan_identity_v1`. Locators remain frozen provenance in
this plan identity; live override paths never rewrite it.

`result.json` (`strategy_lab_portfolio_result_v1`) has exactly `schema_version,status,
plan_identity,execution_identity,provenance,variants,artifacts,limitations,units`.
Its execution identity hashes the plan identity and
`strategy_lab_portfolio_execution_v1`. Git/platform/Python/NumPy/peak memory are
provenance, not an execution-source dependency manifest. Variant facts contain
the request variant, status, exact metrics, period/window capital/return facts,
held reference windows, diagnostics, trade/fee counts, dense digest/count/bounds,
accounting estimate/replay count, curve file/policy and separate storage identity.
There is one copied frozen `plan.json`, one allocation table and one trade table.

`allocations.parquet` columns, in order:

```text
variant period window instrument rank allocated opening closing status reason
allocation_rounding_delta candidate_id source_account_id trade_count fees net_pnl
actual_fingerprint actual_semantic_key reference_fingerprint reference_semantic_key
reference_capital diagnostics
```

One row per slot/window of a funded period, including idle slots (null instrument).
Unfunded periods/windows are explicit in the descriptor, with null returns and
`nonpositive_opening_capital`. Diagnostics are JSON strings in Parquet, decoded
to dicts by the Python reader. Counter fields sum; `no_capital_halt` uses any;
`maxEntryLeverageUsed` uses the maximum available value. Unsupported diagnostics
remain null. Zero counters do **not** imply no lot-floor/minimum-order skips.

`trades.parquet` contains only executed trades, with columns:

```text
variant period window rank instrument trade direction entry_time exit_time
entry_price exit_price size net_pnl profit_pct side exit_reason entry_fee exit_fee
```

Core trade fields are preserved; entry/exit fees use the same S07 core
`price * size * commissionPct/100` expression. No hypothetical candidate trade
histories, raw candles or duplicate HTML/CSV trade tables are saved.

`curves/VARIANT.parquet` columns are `timestamp_ns,balance,equity`. Values retain
binary64 amounts and UTC nanoseconds. The first point is the true initial anchor;
native values follow at bar close. Headline metrics precede sampling. The reader
checks hashes, joins, source contracts, frozen decisions, exact capital carry,
allocations, boundary/metric/table reconciliation and declared tolerance checks.
Compact points certify retained facts, not omitted timestamps or exact full DD.
No live data or current strategy discovery is needed for reading/reporting.

## Curves, resources and detail

Dense numerical digest projection `strategy_lab_portfolio_dense_curve_v1`:
ASCII version plus newline, unsigned little-endian uint64 count, then contiguous
little-endian int64 UTC-nanosecond timestamps, float64 balance and float64 equity
columns including the anchor. Timestamps are ordered/unique; amounts finite.
Digest excludes locators, Git/environment and buckets.

Bucket policy `strategy_lab_portfolio_buckets_v1` computes
`min(B-1,(int(t)-int(t0))*B//(int(T)-int(t0)))` before any fixed-width multiplication.
Each bucket retains first/last and earliest min/max for both balance and equity,
at most six distinct ordinary points. Union initial/final anchors, all window
boundaries and earliest percentage-DD peak/trough witnesses. Policy records
native/saved/ordinary/mandatory/additional-mandatory counts. Storage identity
hashes the dense digest and policy separately; buckets never affect execution.

Variants execute sequentially with one dense aggregate and only the current
group's slot curves. One original prepared union serves a ticker's consecutive
OOS segments without restarting its filter. Required-source preflight is an
additional bounded replay pass. The 256 MiB accounting-array preflight estimates
`80*(native_rows+1) + max_group_rows*(16*slots+32)` bytes; it excludes engine/provider
frames. Oversized jobs refuse before allocation, never coarsen silently. Saved
outputs remain compact by default. Costs grow with variants, selected accounts,
windows and actual trades; all-declared controls execute the full roster.

`detail.json` (`strategy_lab_portfolio_detail_v1`) records parent execution ID,
variant, verified status, native count/digest, checked facts, current provenance
and the `dense.parquet` artifact. Reproduction is exact in the same environment:
all financial facts/statuses, bindings, trades, available diagnostics and dense
digest must match. Even an omitted changed native sample refuses. V1 supplies
no cross-platform replay tolerance. A different Git commit may reproduce; Git
alone neither proves nor forbids it. Offline reports remain available if live
sources are missing. `report.json` identifies its saved execution; summary CSV
and static HTML link to authoritative artifacts without embedding large tables.

## Executable synthetic walkthrough

These are fixture settings, not a recommended trading policy. Choose a fresh
external directory; the helper creates only synthetic packs and reference inputs.
Run the commands from the repository root in PowerShell:

```powershell
$py = 'C:/Users/MT/Desktop/Strategy/S_Python/.venv/Scripts/python.exe'
$work = Join-Path $env:LOCALAPPDATA 'Temp/strategy-lab-portfolio-example'
& $py -B -m tools.strategy_lab.portfolio.example --output-dir $work
& $py -B -m tools.strategy_lab.portfolio prepare "$work/request.json" --output-dir "$work/plan"
& $py -B -m tools.strategy_lab.portfolio run --plan "$work/plan" --data-root "$work/pack" --output-dir "$work/result"
& $py -B -m tools.strategy_lab.portfolio report --result "$work/result" --output-dir "$work/report"
& $py -B -m tools.strategy_lab.portfolio detail --result "$work/result" --variant selected --data-root "$work/pack" --output-dir "$work/detail"
```

On Linux use `/home/dev/projects/.venv/bin/python` and a fresh `/tmp` directory
with the same arguments. The generated `request.json` is a complete editable
example before preparation. For IS/OOS use saved candidate inputs and explicit
scope/unlock fields above; for history substitute an accepted history plan,
declare a source-supported method and a valid holding cadence. Freeze choices
before inspecting funded returns; source files and historical outputs stay immutable.
