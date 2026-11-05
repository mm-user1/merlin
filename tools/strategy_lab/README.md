# Strategy Lab

Strategy Lab is a local, research-only pipeline for certified Backtester V2
strategies. It validates preregistered runs, inventories an external read-only
market-data pack, generates deterministic candidate datasets, certifies engine
parity, analyzes frozen development rules, and evaluates fixed-capacity ticker
allocation. It does not support Backtester V1, nominate a policy, or establish
strategy quality by itself.

Continuous Fixed research also runs explicit scalar configurations as independent
accounts, with persisted evaluation curves and offline calendar views. It is a
separate mode from the candidate-enumeration IS/OOS datasets described below.

## Frozen carried-capital portfolios

`python -B -m tools.strategy_lab.portfolio` prepares a frozen plan from accepted
X/history sources and saved Fixed or IS/OOS inputs, executes each selected policy
at actual capital, renders offline reports, and verifies detailed regeneration.
Accounts close at every OOS boundary, carry their own balances between rebalances,
and pool only at the declared cadence. This new S07 risk-bracket output family
keeps existing independent reference-return reports unchanged.
Offline HTML and summary/window/period CSVs expose held-membership actual/reference
returns, funding/status and saved diagnostics. Detail validates the whole parent
but needs live sources only for its selected input's full roster; unused known
filter overrides remain reusable and all recorded/override roots stay protected.
See [portfolio schemas and executable synthetic workflow](PORTFOLIO_SCHEMA.md)
for exact requests, four commands, resource limits and interpretation.

## Merlin X ticker-selection research

Historical ticker selection is also available through `history-plan`,
`history-fixed-request` and `history-evaluate`. It uses saved full-roster outcomes,
four adjacent completed blocks and separate histories per configuration/filter.
See [Historical selection contracts and workflows](HISTORY_SELECTION_SCHEMA.md)
for the four explicit methods, scope authorization, persisted random controls,
Fixed preflight requirements and separate same-cell X comparison.

`python -B -m tools.strategy_lab.selection` prepares a price-only ranking through
the production Merlin X selector, transforms its frozen calendar into reusable
Fixed requests, and evaluates saved Fixed or IS/OOS outcomes. The full declared
universe remains in every outcome block. Controls share persisted random index
arrays; offline evaluation does not read market packs or rerun X/strategies.

See [Selection schemas and synthetic walkthrough](SELECTION_SCHEMA.md) for the
three commands, complete request/artifact contracts, exact CSV headers, identity
boundaries and copyable Fixed/IS-OOS examples. The shipped
[X request](runspecs/x_selection_example.json) explicitly chooses K=2, a two-month
cadence, one-month lookback, 30-minute ranking bars, 60-minute trading bars and
32 random draws. Analysts must choose their own dates, roster and settings before
examining outcomes; the example is not a recommended trading policy.

Returns are independent reference-capital percentages with fixed arm denominators.
Known IS abstention leaves cash; missing/failed/nonfinite observations stay unknown.
The report labels mixed-duration equal-block means and makes no carried-capital,
portfolio equity/drawdown, significance or validated-edge claim. X membership
parity with core does not imply production portfolio execution parity.

## Generic entry filters

One explicit fixed `entryFilters` editor object applies to each new request.
Multiple active items intersect long/short permissions independently; filter settings
are not an optimization axis or another series dimension. Core owns normalization,
capability, preparation, native clocks, state and signal masking. Initially certified:
S07 with F06 `candleDirection` v1 and v2, including v2 Break > 1. Filters gate raw
entries, never exits, sizing or next-open fills. Active filters require Parquet and
an opted-in strategy. Unknown active methods/versions fail; inactive content follows
core semantics and causes no dependency reads.

| Family | Request | Dataset / supporting format |
| --- | --- | --- |
| Historical CSV | `strategy_lab_runspec_v1` | `strategy_lab_dataset_v2`, unchanged |
| Historical Parquet Off | `strategy_lab_runspec_v2` | `strategy_lab_dataset_v3`, bindings 1 |
| New Parquet filters or Off | `strategy_lab_runspec_v3` | `strategy_lab_dataset_v4`, bindings 2, candidates v2 |
| Historical continuous Fixed Off | `strategy_lab_fixed_runspec_v1` | Fixed dataset/result v1 |
| New continuous Fixed filters or Off | `strategy_lab_fixed_runspec_v2` | `strategy_lab_fixed_dataset_v2` / `strategy_lab_fixed_result_v2` |

New execution blocks replace `entry_filters: []` with required camelCase
`entryFilters`; old formats remain Off-only. Explicit Off is
`{"version":1,"enabled":false,"items":[]}`. Active items use the unchanged core
editor described in [Entry Filters](../../docs/ENTRY_FILTERS.md). V1 and v2 are
distinct methods; no implicit upgrade, alias or absent-field default is introduced.
The target retains Lab timeframe restrictions; filter-source instruments and
timeframes are independent and follow the core/provider domain.

Effective descriptors are saved once in Fixed `contract.resolved.entry_filters`
or IS/OOS bindings-2 `effective_entry_filters`. Each account/instrument saves its
own `entry_filter_inputs` envelope. Raw declaration binds strict request resume,
including changed inactive settings/labels. Normalized effective settings bind
plan and Fixed account identity, excluding item IDs/order/disabled/duplicate AND
items. Original core-normalized filter locators remain in that identity. Consumed
hashes and native origins bind source contracts, not candidate semantic keys.

Core v1/v2 M=1 consume a bounded prefix; M>1 retains its declared native origin.
Fixed v2 resume/no-op replays the saved envelope and verifies live scalar/plan
agreement, so earlier metadata extension never chooses a new origin. IS/OOS
prepares the selected window union once per instrument/pass and aligns permissions
by timestamp across segments, including target warmup; filter state does not restart
at IS/OOS boundaries. Target gaps fail; valid filter gaps create unknown/blocked
intervals. Insufficient prefix, empty source or closure failures refuse execution.
Consumed candle/gap edits fail replay even if later permissions coincide.

### Filter relocation and offline views

Both generation commands accept `--filter-roots PATH`. The file is a closed JSON
object: `{"version":1,"roots":{"/original/filter-pack":"/current/filter-pack"}}`.
Python APIs `fixed.run.run` and `generate.generate_dataset` accept the equivalent
roots mapping as `filter_root_overrides`. Keys canonicalize with core `source_locator`
(including raw Windows keys on Linux); conflicts, unused/unknown keys, relative or
blank destinations fail. Every dependency sharing a locator uses its override.
Original descriptors/facts remain unchanged; target relocation still uses `--data-root`.
Overrides require an existing preflighted dataset. New outputs require original
native absolute filter locations. A nonnative original locator without an explicit
usable override fails before core I/O. Output may not overlap either direction
with the target or any resolved filter source, including aliases/overrides.

Historical reads, reports and comparison need no packs or live strategy registry.
They verify exact schema pairs, effective declarations, envelopes/origins, target
ownership and artifacts without translating old formats. Foreign-host recorded
roots remain metadata, never paths interpreted under the current working directory.
New Fixed CSV/account/series views expose `entry_filter_state`, `entry_filter_key`
and core saved description text; the key is SHA-256 of the canonical effective list,
including `[]` for Off. Full settings and source facts remain in saved evidence.
Old output columns and historical-route reports are unchanged.

New IS/OOS v4 keeps the candidate × IS/OOS × 22-metric shape, descriptive-v1 rules
and normalized allocation. Reports identify filters and saved source facts.
Locked scopes still need explicit unlock and frozen policy evidence through
`resolve_scope`; available matrices do not authorize holdout analysis. Allocation
keeps each input labeled and rejects mixed dataset-format combinations.

### Bounded synthetic filter workflow

Examples are fixtures, not recommendations or the 44-ticker research protocol:
[Fixed Off](runspecs/s07_filters_fixed_off.json), [Fixed on](runspecs/s07_filters_fixed_on.json),
[IS/OOS Off](runspecs/s07_filters_is_oos_off.json), [IS/OOS on](runspecs/s07_filters_is_oos_on.json).
The on examples contain an illustrative absolute locator. Copy requests and set
their actual synthetic root explicitly; never edit a frozen request to relocate it.
Use a fresh external directory, from the repository root:

```powershell
$py = 'C:/Users/MT/Desktop/Strategy/S_Python/.venv/Scripts/python.exe'
$work = Join-Path $env:LOCALAPPDATA 'Temp/strategy-lab-filter-example'
& $py -B -m tools.strategy_lab.synthetic_parquet --output-dir "$work/pack" --symbols AAA BBB --start 2024-01-01 --end 2024-05-02
@'
import json, pathlib, sys
work = pathlib.Path(sys.argv[1]).resolve()
for family in ('fixed', 'is_oos'):
    for state in ('off', 'on'):
        name = f's07_filters_{family}_{state}.json'
        raw = json.loads((pathlib.Path('tools/strategy_lab/runspecs') / name).read_text())
        for item in raw['generation']['execution']['entryFilters']['items']:
            item['source']['pack_root'] = str(work / 'pack')
        (work / name).write_text(json.dumps(raw, indent=2), encoding='utf-8')
'@ | & $py -B - $work
foreach ($state in 'off','on') {
  $request = "$work/s07_filters_fixed_$state.json"
  & $py -B -m tools.strategy_lab.fixed validate $request
  & $py -B -m tools.strategy_lab.fixed run $request --data-root "$work/pack" --output-dir "$work/fixed-$state" --preflight-only
  & $py -B -m tools.strategy_lab.fixed run $request --data-root "$work/pack" --output-dir "$work/fixed-$state" --resume
  & $py -B -m tools.strategy_lab.fixed report --dataset "$work/fixed-$state" --output-dir "$work/report-$state"
  $request = "$work/s07_filters_is_oos_$state.json"
  & $py -B -m tools.strategy_lab.config $request
  & $py -B -m tools.strategy_lab.generate $request --data-root "$work/pack" --output-dir "$work/is-$state" --preflight-only
  & $py -B -m tools.strategy_lab.generate $request --data-root "$work/pack" --output-dir "$work/is-$state" --resume
  & $py -B -m tools.strategy_lab.analysis.cli analyze --dataset "$work/is-$state" --scope research --output "$work/analysis-$state"
}
& $py -B -m tools.strategy_lab.analysis.cli allocate --dataset "off=$work/is-off" --dataset "on=$work/is-on" --scope research --rule primary_profit --output "$work/allocation"
& $py -B -m tools.strategy_lab.compare_filters --baseline "$work/fixed-off" --variant "$work/fixed-on" --output-dir "$work/comparison"
```

All declared cells and these public commands are exercised in the CLI tests, with
packs hidden for offline readback. Linux uses its configured Python/native paths.
One target frame plus required filter history is retained at a time. M>1 may consume
long native history; this is not a hard memory budget. Same-root reads share the
core session; distinct roots are serial, without an atomic cross-pack snapshot.
No cross-account cache or scheduler is added. Fixed preserves source verification
of every completed account and final-index recovery; IS/OOS preserves pending-work
source checks and all-valid partial reuse. Complete IS/OOS no-op verifies all sources.

### Paired Fixed Off/on CSV

`python -m tools.strategy_lab.compare_filters --baseline OFF_DATASET --variant ON_DATASET --output-dir REPORT`
accepts exactly two complete Fixed-v2 datasets, Off baseline and active variant.
Every non-filter declaration except `run_name` must agree, including roster/order,
cells, economics, parameters, dates/warmup and resources. Portable target pins,
operative admission rules, resolved scalars and singleton geometry must also agree.
Equivalent target relocation/provenance is allowed; mismatches never silently
intersect coverage. Partial, IS/OOS, historical/new, On/on and Off/off inputs refuse.

`pairs.csv` has one row per declared account with labels, both account IDs, candidate
semantic key, Off/On values and On-minus-Off deltas for Net Profit %, MTM Max DD %
and trade count. Percentage deltas are percentage points; trade deltas are counts.
Saved unavailable values/reasons remain visible; deltas need two finite values.
Negative drawdown delta means less measured drawdown, not higher profit. There is
no ranking, aggregation, inference or claim of established improvement.
`comparison.json` (`strategy_lab_filter_comparison_v1`) records source identities,
digests, effective keys, coverage, matching policy and units and is published last.
Output must be a separate empty directory. Sources remain unchanged; comparison
resume/caching and paired IS/OOS deltas are not implemented (the latter belongs to T03).

## Continuous Fixed research

`python -m tools.strategy_lab.fixed` supports `validate`, `run` and `report`.
The request `strategy_lab_fixed_runspec_v1` pairs exclusively with
`strategy_lab_fixed_dataset_v1`. Both Fixed and IS/OOS readers reject the other
family; old schemas and numerical contracts are preserved.

One account starts once with its explicit initial capital and runs continuously
through its Start–End interval. Reporting months never restart capital, close a
position or reset indicators. A separately rerun subperiod has fresh capital,
warmup and a new final boundary, so it is a different experiment. Every instrument
has its own account; there is no shared capital, portfolio curve, ranking or winner
selection. Fixed means fixed scalar parameters; Merlin's sizing still uses the
changing balance. Generic `risk_bracket_v1` hooks/reference execution are reused,
initially verified with S07. No S07-specific kernel is implemented in the Lab.

### Request, input and runtime boundaries

The complete examples are [a 16-account series](runspecs/s07_fixed_example.json)
and [one account](runspecs/s07_fixed_single.json). Their settings are synthetic
fixtures, not trading recommendations. The closed request declares:

- Strategy/engine/runtime/planning/identity pins, checked against the live config
  before generation; complete resolved scalars are frozen for historical reads.
- Ordered exact full instrument IDs and descriptive cells; explicit initial
  capital, commission, risk, long/short flags and leverage cap. Funding and
  slippage must explicitly be false. Manual Contract Size is not allowed.
- Named scalar parameter overlays, target timeframes, named periods with explicit
  Start/End and integer warmup, `expected_run_count`, and `outer_workers: 1`.
- Historical v1 `execution` exactly `parameter_mode: "fixed"`, `policy: "risk_bracket_v1"`,
  `entry_filters: []`; new v2 uses the explicit editor above. Ties, ranges, axes, optimization, WFA,
  automatic discovery, shared capital and concurrent execution are unsupported.

Core Fixed resolution owns defaults, selectors, dependencies and scalar domains.
Valid off-grid literals are supported; optimization-grid membership is irrelevant.
Unknown names and wrong types are rejected before resolution. S07 Fixed's current
cap domain is **1..10**, and user warmup is **100..5000 target bars**, without
clamping. T01 IS/OOS retains its broader positive-cap and warmup domains; use values
supported by both when comparing modes.

Timeframes must be unique integers at least 5, divisible by 5, and dividing 1440
exactly (5, 15, 60 and 240 are examples; 25 is invalid). Core prepares complete,
closed, continuous 5-minute source groups, verified quote turnover and admission
rules, plus all requested preceding warmup bars. No gaps are filled, no incomplete
groups accepted, and no CSV conversion occurs. Gaps outside consumption remain
irrelevant. Warmup creates state without funded trading history.

Start/End select UTC target candle **open times**. End is inclusive. A date-only
End such as `2024-03-31` includes that entire UTC day; a precise
`2024-04-01T00:00:00Z` includes one April candle and may materially change monthly
Sharpe. Core owns parsing/alignment. Effective execution dates and cap come from
prepared facts/admission, and `warmupBars` matches the actual trade-start index.
Requested bounds, effective evaluation bounds and half-open consumed bounds are
all saved. There is only one `strict_close` final boundary, retaining core fees,
pending-order rules, admission rounding/cap refusals and guardrails.

### Bounded synthetic commands

Use a fresh external directory; the builder publishes generated candles and never
collects data. These two-day intervals avoid the sine fixture's known long-period
5m/15m compounding overflow. All 16 declared cells are exercised by the CLI tests.

```powershell
$py = 'C:/Users/MT/Desktop/Strategy/S_Python/.venv/Scripts/python.exe'
$scratch = Join-Path $env:LOCALAPPDATA 'Temp/strategy-lab-fixed-example'
$spec = 'tools/strategy_lab/runspecs/s07_fixed_example.json'
& $py -B -m tools.strategy_lab.synthetic_parquet --output-dir "$scratch/pack" `
  --symbols AAA BBB --start 2024-01-28 --end 2024-02-07
& $py -B -m tools.strategy_lab.fixed validate $spec
& $py -B -m tools.strategy_lab.fixed run $spec --data-root "$scratch/pack" `
  --output-dir "$scratch/dataset" --preflight-only
& $py -B -m tools.strategy_lab.fixed run $spec --data-root "$scratch/pack" `
  --output-dir "$scratch/dataset" --resume
& $py -B -m tools.strategy_lab.fixed report --dataset "$scratch/dataset" `
  --output-dir "$scratch/report"
# Open $scratch/report/index.html locally. Market inputs are unnecessary for reporting.
```

For a single account, use `s07_fixed_single.json` and a fresh dataset/report
directory with the same pack. Linux uses its configured interpreter and native
paths. For verification, use the isolated launcher and external caches documented
in [tests/README.md](../../tests/README.md).

`validate` prints resolved settings, the matrix count and the sequential/date/
warmup policy. It performs live-config validation, but cannot claim source admission.
Preflight resolves every declared cell before any execution and freezes all bindings.
Traversal is parameter set → timeframe → period → instrument in declared order.
Duplicate account IDs fail preflight, including differently written periods that
collapse to the same coarse timeframe grid. IDs bind pinned semantics, canonical
non-runtime scalars, instrument/timeframe and resolved warmup/start/end/trade index;
labels, raw date spellings, paths and provenance are not account-ID discriminators.
The dataset contract separately binds the full request and source/effective facts.

### Artifacts, accounting and offline reports

`manifest.partial.json` embeds the frozen request, scalar contract and ordered
account bindings. Each `accounts/<account_id>/` contains:

| File | Meaning |
| --- | --- |
| `result.json` | Exact execution parameters/pins/bounds, source/admission/Off facts, capital/metric anchor, core metrics and units, availability, guards, final state, backend, timings/RSS and curve/table hashes |
| `curve.parquet` | Every evaluation bar: `timestamp_ns` int64 UTC bar-open, `equity` and `balance` float64 account currency; no warmup or synthetic anchor row |
| `monthly.csv` | Calendar accounting derived from those saved observations and anchor |
| `report.html` | Static embedded-SVG equity/balance view, metrics, economics and monthly table |

`series-index.csv` retains every declared row, including pending/failed rows and
reasons, cells/labels, economics, metrics, counts and relative artifact paths.
`manifest.json` alone marks completion. The backend is `generic_v2_reference`;
Fixed does not run a redundant compiled screen. Existing IS/OOS generation still
requires its compiled backend.

Whole-run basic/advanced metrics are core results, not reconstructed trade
statistics. Net Profit is realized balance change; realized Max DD and bar-close
MTM Max DD are separate. Daily Sharpe and MTM calculations are explicitly requested.
Null/unavailable metrics stay JSON `null` and empty numeric CSV cells, with reasons;
valid zero stays zero. A loss, no trades or cap-blocked account can complete normally.
Invalid accounting or execution errors cannot become zero-profit results. Trade
ledgers and raw event exports are not provided by this feature.

Ulcer Index values are already percentage-scale; newly generated metadata and
HTML label them `percent`. Older saved `ratio` labels remain readable and are
preserved, including when resuming already claimed accounts. Values are not rescaled.

Monthly observations use end-of-bar values indexed by UTC bar-open timestamps.
The first month opens at the saved pre-evaluation capital anchor; subsequent months
open at the prior month's last equity/balance. This preserves first-bar P&L.
Return is `100*(closing_equity/opening_equity-1)` only for positive opening equity;
otherwise the visible month has null return and `nonpositive_opening_equity`.
Realized P&L is closing minus opening balance, distinct from equity change.
Rows retain first/last timestamps, counts and `partial_month`; a full month must
cover every target bar-open. Final partial months remain visible without annualizing
returns, inventing months or reallocating crossing-position profits.

`report` verifies saved contracts, artifact hashes/shapes and calendar arithmetic
without current registry/default/engine lookup or market access. It writes a separate
offline index and one account view at a time. Links point to original JSON/Parquet/CSV;
keep their relative locations if moving a report. Rebuilding derived views never
changes execution artifacts. Default reporting requires completion; `--allow-partial`
shows verified completed accounts and all declared pending/failed rows. The static
plot samples at most 1,000 points; stored curves, metrics and tables retain all rows.
There is no JavaScript, CDN or server dependency. Saved validation checks consistency,
not execution truth against an adversary who rewrites and reseals every artifact.

### Failure, resume and resource policy

Per-file atomic writes precede a validated completed-account claim in the partial
manifest. That claim publishes the account. An interrupted unclaimed account restarts
from its original capital; its exact owned files may be replaced. Claimed completed
files are immutable: missing/corrupt artifacts fail rather than regenerate. A partial
dataset requires explicit `--resume`; changed experiments require a new output.

Finalization refreshes the series index from durable claims before publishing
`manifest.json`, even when resume has no accounts left to execute. An index-write
failure leaves the claims resumable and withholds completion. Completed datasets
remain unchanged on a valid no-op. If a historical completed dataset has a stale
index from an earlier interruption, use `report --dataset DATASET --output-dir REPORT`
with a separate output directory to reconstruct truthful views without modifying
the original dataset.

Every invocation source-verifies all accounts before proceeding, including completed
accounts on partial resume and complete no-op. Pending accounts replay their frozen
inputs/rules again at consumption. Equivalent pack relocation and provenance-only
rule refresh are allowed; changed consumed candles, operative rules, scalars, geometry
or live pins are refused. This is stricter than T01 partial resume, which can reuse
valid completed groups without market reads. Offline Fixed reporting needs no sources.

Failure/interruption records the failing account and stops with nonzero status;
later cells remain pending. There is no continue-after-failure option. Resume cannot
repair deterministic core refusals (including quantity overflow): correcting settings
requires a new request/output, without clamping quantities or dropping failed cells.

Preparation/execution retain one account's frame/working arrays at a time. One account
still needs memory proportional to its interval. Recorded RSS is the native **process
lifetime peak**, or null if unavailable, not a per-account allocation guarantee.
Preparation/execution timers and consumed row counts accompany it. There is no input
cache budget, persistent frame cache, hard row limit or speculative parallelism.

## IS/OOS candidate research

The canonical S06 bracket run is `runspecs/s06_bracket_mvp.json`: 480 Grid V2
candidates, 30-minute data, two-month IS and one-month OOS, eight calendar
windows, and a frozen 24-development/94-holdout split. Six additional tracked
S06 v06-4-A2 run specs fix the entry family (reversal or trend) and trail family
(`r_trail`, `chandelier`, or `fixed_af_sar`). Dataset generation for those six
profiles is deliberately separate from preregistration.

`runspecs/s03_adaptive_ma_symmetric.json` declares 2,800 candidates (700 per MA)
for S03 Adaptive MA: KAMA, SuperSmoother, FRAMA, DSMA; lengths 25..250 by 25;
common Close Count 1..7 and T Band 0.2..2.0% by 0.2%. Only the four source axes
are independent. The plan stores explicit equal Long/Short values. This research
run fixes initial capital 1,000, commission 0.05%, position size 100%, contract
size 0.0001, both directions, and Emergency SL off. It uses the same inventory,
24/94 split, eight windows, frozen policy and 1,000-bar warmup as S06, with one
outer worker, one Numba thread and a 32 MB cache limit. These assumptions do not
change the separate external S03 baseline.

### Planning declarations

`generation.planning.enabled_tie_groups` is optional and must be an ordered
list of unique, declared group IDs. Missing and `[]` both disable ties; null,
scalars, malformed members, duplicates and unknown groups fail before execution
or output writes. `axis_values` keys must exactly equal `enabled_axes`, without
derived targets. All other required and unknown-field checks remain strict.

The required non-empty `enabled_variants` list remains an actual Grid selection
for public selectors. For internal selectors it declares the expected resolved
variant tuple: S03 fixes `base_params.useEmergencySL=false` and declares
`["plain"]`. Grid resolves the selector without a public variant request; Lab
requires exact declaration agreement before checking fingerprints and still
checks execution modes. Changing the selector changes plan identity.

Defaults are resolved without editing input dictionaries. Old specs retain
their normalized content and hashes; explicit empty ties remain present and may
hash differently from absence while sharing the no-tie Grid identity. Enabled
ties use Grid's existing identity; semantic/value keys describe expanded values.
Resume rejects changed plan/run identity even when fixed values coincide.
Candidate `start`/`end` defaults may be null; non-null window dates are rejected.
Window preparation remains the authority for segment boundaries.

Schema-v2 MTM is supported for generic signal reversal as well as position
execution, using the research-only optional sidecar and unchanged 26-column
compiled ABI. See [metric semantics](../../docs/METRICS.md#strategy-lab-mtm-drawdown).

## Parquet and S07 Off

| Request | Input/execution | Dataset | Reader/report |
| --- | --- | --- | --- |
| `strategy_lab_runspec_v1` | Existing CSV and certified S03/S06 | `strategy_lab_dataset_v2`, 21 metrics | Existing analysis/allocation v1 |
| `strategy_lab_runspec_v2` | Parquet, admission-capable V2, explicit Off | `strategy_lab_dataset_v3`, 22 metrics | Descriptive analysis/allocation v2 |

Dataset v1 is not supported by the analysis reader. Existing request hashes,
CSV validation, v2 metrics, rules and evidence contracts are preserved. The new
route requires `entryAdmission="risk_bracket_v1"`; S07 is the certified production
integration. This historical request family remains Off-only; active filters use
the [new explicit schemas](#generic-entry-filters). S03/S06 Parquet, selectors,
carried capital, FT/stress and delayed OOS remain unsupported and are rejected.

The complete runnable request is [s07_off_example.json](runspecs/s07_off_example.json).
Its dates, roster and economics are synthetic illustrations, not an authorized
market experiment. It declares one research cell, three windows, explicit
capital/fees/risk/directions/cap, and all 840 registered S07 combinations.
`axis_values` assert registered domains, never override them. Disable axes and
supply fixed scalars for a smaller grid. The cap cannot be an axis. Manual
Contract Size and runtime/date parameters in base parameters are refused.

From the repository root, using a fresh external scratch directory:

```powershell
$py = 'C:/Users/MT/Desktop/Strategy/S_Python/.venv/Scripts/python.exe'
$scratch = Join-Path $env:LOCALAPPDATA 'Temp/strategy-lab-synthetic-example'
$spec = 'tools/strategy_lab/runspecs/s07_off_example.json'
& $py -B -m tools.strategy_lab.synthetic_parquet --output-dir "$scratch/pack"
& $py -B -m tools.strategy_lab.config $spec
& $py -B -m tools.strategy_lab.generate $spec --data-root "$scratch/pack" `
  --output-dir "$scratch/smoke" --ticker BYBIT_AAAUSDT --window 2 --preflight-only
& $py -B -m tools.strategy_lab.generate $spec --data-root "$scratch/pack" `
  --output-dir "$scratch/smoke" --ticker BYBIT_AAAUSDT --window 2 --resume
& $py -B -m tools.strategy_lab.analysis.cli analyze --dataset "$scratch/smoke" `
  --scope research --allow-partial-scope --output "$scratch/smoke-analysis"
& $py -B -m tools.strategy_lab.generate $spec --data-root "$scratch/pack" `
  --output-dir "$scratch/full"
& $py -B -m tools.strategy_lab.analysis.cli analyze --dataset "$scratch/full" `
  --scope research --output "$scratch/analysis"
& $py -B -m tools.strategy_lab.analysis.cli allocate --dataset "s07=$scratch/full" `
  --scope research --rule primary_profit --primary-k 6 --sensitivity-k 8 `
  --output "$scratch/allocation"
```

For isolated verification use the test launcher and external compiler caches as
specified in [tests/README.md](../../tests/README.md). The synthetic command only
publishes generated candles into a new directory; it collects no market data.
Repeat generation with `--resume` after interruption, retaining exact selections.
A completed repeat verifies sources/artifacts and returns a no-op. Changed requests
require a fresh output. Offline `config` reports plan facts as
`requires_source_rules`; only `generate --preflight-only` prepares rules-aware
plans, checks count/domains/geometry and publishes an incomplete resumable plan.
No manually calculated fingerprints are needed.

Requests declare ordered normalized full instrument IDs, cells, one pack/timeframe,
calendar, analysis scopes, and random-control seed/draw count. Durations must be
positive multiples of 5 minutes dividing 1440. Full schedules retain Merlin's
minimum of two complete windows; a smoke can select one original window ID.
Multi-month OOS blocks are supported. A sole `research` cell needs no holdout.
The CLI default remains `development`; explicitly pass `--scope research`.
Locked scopes require both `--unlock-scope` and a frozen `--policy`; missing windows
require `--allow-partial-scope`.

### Frozen inputs and publication

Preflight processes instruments sequentially, freezing each selected union with
outer warmup and intervening bars for nonadjacent windows. Merlin `prepare_input`
owns closed continuous candles, quote-turnover Volume and float64 OHLCV. No clipping,
filling, CSV conversion or collection occurs. Locks end before numerical work;
one instrument frame is retained at a time. Each segment uses Merlin warmup slicing
and fresh strategy preparation, as in WFA, at the declared fixed initial capital.
All candidates execute compiled Grid without ranking, sampling or selected Slow.

Shared `candidates.json` schema v2 stores geometry once. Exact order/IDs/keys,
parameters, modes, axes/codes/masks must agree across instruments; unequal geometry
fails without remapping. Each instrument retains its own plan fingerprint and
semantic digest. `input_bindings.json` version 1 owns portable consumed pins,
admission/Off envelopes, plans and exact segment bounds/warmup facts.
`data_quality.csv` keeps the historical columns/order, with full IDs in
`canonical_symbol` and blank CSV-only filename/hash fields. It reports
status/ownership; bindings own preparation identity.

Identity binds the request, roster, selections, geometry, effective admission
rules/cap, consumed fingerprints, versions and metric axes. Roots, acquisition
metadata and original decimal spelling are provenance, protected by artifact
checksums but excluded from semantic identity. Compatible provenance refreshes
and equivalent rule spellings never rewrite the saved envelopes.

Partial resume verifies bindings/artifacts and replays each source immediately
before pending or regenerated groups. Valid groups remain reusable without market
reads, with no final source sweep. Completion proves consumed historical inputs,
not an atomic multi-instrument snapshot or equality with today's pack. A completed
rerun explicitly replays every source to verify current reproducibility. Changed
consumed candles/effective rules fail; compatible outside appends pass. Completed
outputs remain immutable even when corrupt; only partial groups can regenerate.

For relocation, pass an equivalent pack through `--data-root` (or the existing
environment fallback). Lab reconstructs the current-host core descriptor from
portable facts, including Windows-origin provenance on Linux, and verifies saved
content/rules through core replay without editing artifacts. Wrong venue,
instrument, timeframe, rules or consumed candles fail. This rule is Off-only.
Output beneath the resolved source root, including aliases, is refused.

### Descriptive v3 outputs

The float64 layout remains `[candidate, is/oos, metric]`. V3 appends raw
`margin_reject_count` to the unchanged v2 axis. In risk-bracket mode it counts
above-cap refusals; `rejected_fill_count` is not a substitute. Raw
`max_required_leverage` remains unchanged: zero on a no-fill row does not prove
an observed accepted-fill maximum. Minimum-notional refusals have no invented
counter. Finite zero returns remain data; nonfinite/unavailable values remain
unavailable.

The offline reader checks schemas, bindings, geometry, ownership, hashes, shapes
and backend records without opening market packs or constructing Grid plans.
Before reader dispatch, dataset v2 requires RunSpec v1 and dataset v3 requires
RunSpec v2. Missing, unknown or mismatched request versions fail explicitly;
candidate schemas remain checked separately (v1 for dataset v2, v2 for dataset v3).
It validates the saved supported contract once, independently of the installed
strategy registry, parameter bounds/options and current engine version pins.
Producing pins remain required, structurally valid and consistent across artifacts;
they need not match today's stack, and the producing strategy need not remain
registered. Historical `strategy_lab_rules_v1` membership/order, baseline,
15-trade gate and ties remain exact supported semantics, independently of changes
to the current generation registry. Unknown or altered semantics still fail.
Config loading, new generation, preflight, partial resume and completed generation
reruns retain strict current strategy/profile/version/domain/registry checks.
Offline readability does not authorize generating or replaying a stale request.
Analysis joins use full IDs in historical `canonical_symbol`/ticker fields.
Mixed v2/v3 allocation comparisons fail; v3 comparisons align exact IDs and UTC
blocks. Candidate and ticker decisions freeze from IS before OOS access. The
existing 15-trade rules and tie order are unchanged.

The population headline mean first averages finite candidate returns within each
instrument/block, then weights available instruments equally within each block
and available blocks equally. Population median and quantiles instead pool finite
observations. Profitable share pools positive finite observations over all
observations, with unavailable entries retained in the denominator. Generic
diagnostic summaries of other metrics pool finite observations; `net_profit_pct`
has no generic metric-summary output. Counts/missingness remain explicit.
For example, block 1 with
A=[0,0], B=[100,unavailable] and block 2 with four zeros has block means 50 and 0,
headline 25, pooled median 0 and seven finite observations; profitable share is 1/8.
The illustrative seven finite values have pooled mean 100/7; this is not an
additional net-profit field emitted by the tool. Placing those same synthetic
values in an existing diagnostic channel such as `profit_factor` also gives
100/7 through its pooling rule. This arithmetic witness asserts no financial
relationship between profit factor and net return and adds no statistic.
Allocation retains fixed `1/K` weights,
cash underfill and all/bottom/random/oracle controls. Required missing blocks make
headline/compounding unavailable; allocation never compounds around them. Random
controls use explicit seed/draw count even for empty pools. Oracle controls are
hindsight diagnostics, and all allocation remains normalized research.

Both metadata and summary use `strategy_lab_analysis_v2` or
`strategy_lab_allocation_v2`, `period_unit="oos_block"`, and explicit
`oos_period_months` (by input label for allocation). Five analysis and six allocation
filenames remain unchanged. `monthly_results.csv` and `monthly_*` fields are
historical aliases for per-OOS-block quantities, including multi-month blocks;
returns are not annualized or rescaled. Evidence verdicts, nominations, bootstrap
intervals and outlier removal are `not_requested` under
`strategy_lab_descriptive_v1`. Allocation omits `matched_fraction`, recording one
capability status instead. No executable portfolio or carried-capital claim is made.

## Safety and identity

- Source CSVs are read only. The tool never rewrites, sorts, or writes beside
  market data.
- Cross-host identities and run specifications are tracked under `runspecs/`.
- Resolved roots, host facts, generated datasets, caches, and temporary output
  live under ignored `output/`, `cache/`, or `tmp/`, or another explicit local
  path.
- An explicit `--data-root` wins over
  `MERLIN_STRATEGY_LAB_DATA_ROOT`; absence of both is an error.
- Absolute source roots and host metadata are provenance, not tracked identity.
- Output equal to or beneath the market-data root is rejected before writes or
  candidate execution.

The inventory builder rejects subdirectories, source symlinks, malformed file
names or schemas, duplicate canonical symbols, unexpected counts, and
insufficient initial-capital sizing headroom. Raw validation checks the accepted
file identity, exact header and column count, integer Unix-second timestamps,
strict ordering, duplicates, exact 30-minute cadence and boundaries, row count,
finite positive OHLC relationships, and finite non-negative volume.

## Commands

Run commands from the repository root. On Windows:

```powershell
$py = 'C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe'
& $py -m tools.strategy_lab.config tools\strategy_lab\runspecs\s06_bracket_mvp.json
& $py -m tools.strategy_lab.generate --help
& $py -m tools.strategy_lab.certify --help
& $py -m tools.strategy_lab.analysis.cli --help
& $py -m tools.strategy_lab.analysis.allocation_certify --help
```

Linux/VPS environments use their configured project Python and native paths.
These modules bootstrap the repository `src/` path and require no manual
`PYTHONPATH` setup.

### Inventory

Rebuild the tracked inventory only as an explicitly reviewed preregistration
change:

```powershell
& $py -m tools.strategy_lab.inventory `
  --data-root '<read-only-data-root>' `
  --output tools\strategy_lab\runspecs\tickers_current.json `
  --provenance-output tools\strategy_lab\output\current_pack_provenance.json `
  --expected-ticker-count 118 --development-ticker-count 24 `
  --timeframe-minutes 30 --initial-capital 1000 --risk-per-trade-pct 2 `
  --contract-size 0.0001 --max-stop-pct 8 --minimum-size-steps 100
```

### Generate a bounded dataset

```powershell
& $py -m tools.strategy_lab.generate `
  tools\strategy_lab\runspecs\s06_bracket_mvp.json `
  --data-root '<read-only-data-root>' `
  --output-dir tools\strategy_lab\tmp\mtm-smoke `
  --ticker <canonical-symbol> --window 1
```

Repeat `--ticker` or `--window` for a larger subset. Use `--resume` only for an
identity-compatible partial output. Progress reports validation, group reuse or
regeneration, completion, output path, and duration—never performance results.

### Certify the real pack

For the bounded S03 gate, use a fresh external work directory and isolated
bytecode/compiler caches. The command validates all 118 sources, then executes
CRVUSDT windows 1, 2 and 3 with all 2,800 rows in both segments. It checks 39
deterministic geometry-selected candidates against reference execution in all
six segments, two fresh generation processes, partial/resume equivalence,
verified no-op, finite MTM, saved candidate geometry and partial analysis.

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:NUMBA_CACHE_DIR = '<external-task-cache>'
& $py -B -m tools.strategy_lab.certify_s03_smoke `
  --data-root '<read-only-data-root>' --work-dir '<fresh-external-work-dir>'
```

The directory contains `selection.json` written before execution, `clean-a`,
`clean-b`, `resumed`, per-process logs, `analysis`, and `evidence.json`. To run
the underlying generation and partial read-back explicitly:

```powershell
& $py -B -m tools.strategy_lab.generate `
  tools\strategy_lab\runspecs\s03_adaptive_ma_symmetric.json `
  --data-root '<read-only-data-root>' --output-dir '<fresh-external-dataset>' `
  --ticker CRVUSDT --window 1 --window 2 --window 3
& $py -B -m tools.strategy_lab.analysis.cli analyze `
  --dataset '<fresh-external-dataset>' --scope development `
  --allow-partial-scope --output '<fresh-external-analysis>'
```

This is partial development evidence only. The run spec still declares all
eight windows; normal development requires windows 1..6. The explicit override
does not certify full development, holdout, allocation, or strategy quality.
The original S06 full-pack certifier and its frozen gates remain separate:

```powershell
& $py -m tools.strategy_lab.certify `
  tools\strategy_lab\runspecs\s06_bracket_mvp.json `
  --data-root '<read-only-data-root>' `
  --work-dir tools\strategy_lab\tmp\real-pack-certification
```

This opt-in gate checks compiled/reference parity, selected typed-Slow parity,
one-thread/two-thread determinism, and deterministic smoke generation. The
separate isolated production HTTP WFA tie-back is intentionally outside normal
pytest discovery:

```powershell
$env:MERLIN_STRATEGY_LAB_DATA_ROOT = '<read-only-data-root>'
$env:MERLIN_STRATEGY_LAB_CERT_WORK_DIR = '<absolute-certification-dir>'
& $py tools/run_tests.py -- tests\strategy_lab\phase1b_real_wfa_certification.py
```

The explicit WFA command requires an existing real pack and the completed
`smoke_one` certification output; missing prerequisites fail. It remains outside
normal discovery. Ordinary normalization cases live in `test_strategy_lab_certify.py`
and share `certification_helpers.py` with the explicit WFA module. The ordinary
two-process smoke test reuses the configured external Numba cache, or a contained
temporary cache when unset. See [test isolation](../../tests/README.md), including
`MERLIN_TEST_ROOT` for nested checkouts.

### Analyze development data

```powershell
& $py -m tools.strategy_lab.analysis.cli analyze `
  --dataset tools\strategy_lab\output\s06_bracket_mvp_mtm_v2 `
  --scope development `
  --output tools\strategy_lab\tmp\development-analysis
```

The deterministic analysis output contains exactly five files. The manifest
owns axes, actual windows, groups, and dataset identities; the normalized run
spec owns scopes, aggregation, trade gates, rule membership, tie-breaking,
evidence thresholds, outlier treatment, and bootstrap parameters. Rules see IS
data only until candidate IDs are frozen; OOS labels load afterward.

Supported readers dispatch by schema/version. A missing metric makes only its
dependent rules `unsupported_for_dataset`; a non-finite candidate observation
makes that candidate unavailable; finite zero remains data. Headline means
equal-weight valid tickers inside each UTC OOS block and then equal-weight
blocks. Descriptive month-block intervals are evidence, never pass/fail gates.

Run the explicit non-default real analysis certifier to compare bounded and
full-development point estimates with independent frozen oracles:

```powershell
& $py -m tools.strategy_lab.analysis.certify `
  --dataset tools\strategy_lab\output\s06_bracket_mvp_mtm_v2
```

### Allocate fixed capacity

Dataset labels are mandatory and stable in comparison mode:

```powershell
& $py -m tools.strategy_lab.analysis.cli allocate `
  --dataset canonical=tools\strategy_lab\output\s06_bracket_mvp_mtm_v2 `
  --scope development --rule primary_profit `
  --primary-k 6 --sensitivity-k 8 `
  --output tools\strategy_lab\tmp\development-allocation
```

Candidate choice freezes from current IS. Ticker scoring then ranks finite
current-IS net profit descending, finite selected IS trade count descending,
and canonical symbol ascending. In multi-dataset comparison, exact canonical
ticker and OOS UTC boundaries define aligned cells; every dataset ranks the
same common eligible pool before OOS is revealed.

Slot weight is always `1/K`. Underfill remains cash and is never renormalized.
Each K reports all-available, Bottom-K, Random-K, oracle, and anti-oracle
controls. Oracle controls are hindsight diagnostics. Compounding and tail
drawdown operate on calendar-block capacity returns, not bar-level portfolio
equity. Allocation publishes exactly six deterministic files, including
`ticker_allocations.csv`.

The opt-in allocation certifier checks bounded, underfilled, and full
development cases against an independent oracle. Repeat `--dataset label=path`
for an aligned comparison:

```powershell
& $py -m tools.strategy_lab.analysis.allocation_certify `
  --dataset canonical=tools\strategy_lab\output\s06_bracket_mvp_mtm_v2 `
  --rule primary_profit
```

### External universes and declarative candidate policies

Both analysis commands and Python evaluation APIs accept explicit exploratory
inputs for saved candidate datasets v2/v3/v4. This does not regenerate a dataset,
change its registered rules, or adapt continuous Fixed accounts to IS/OOS.
Mixed-family allocation remains unsupported. With neither new input, historical
output filenames, CSV columns, identities and calculations remain unchanged.

The [universe example](runspecs/analysis_universe_example.json) is a closed JSON
object with all five required fields:

```json
{
  "schema_version": "strategy_lab_universe_v1",
  "name": "synthetic-research-subset",
  "evidence_classification": "exploratory",
  "selection_note": "Explicit synthetic membership; no confirmation claim.",
  "tickers": ["BYBIT_AAAUSDT"]
}
```

Names/notes must be nonblank strings; tickers must be a nonempty list of unique
exact saved identifiers without surrounding whitespace. Parquet uses full IDs;
CSV retains its own symbols. There is no case folding, aliasing or live lookup.
Unknown fields/versions, duplicate JSON keys and malformed types fail. Record how
membership was chosen in `selection_note`, including reuse of strategy outcomes.
V1 classification is always `exploratory`; a frozen list proves no independence.

`--ticker-list PATH` restricts an already resolved scope in its saved order.
Strict mode (the only analyze mode) requires every requested ID in every scope;
an ID in another cell is still outside the authorized scope. Allocation optionally
accepts `--ticker-list-mode intersection`: IDs absent from every scope fail, each
scope is restricted, then the existing exact-calendar/common-pool alignment runs.
Empty per-input or common subsets fail. A mode without a list fails.

The original cell denominator, missing/declared windows, prior partial status and
unlock evidence are retained. A proper membership subset is partial; the list
itself declares that restriction and needs no `--allow-partial-scope`. That flag
still concerns missing windows. Full membership in any file order preserves saved
scope order and full/partial status. Allocation keeps its sorted common pool.
Existing allocation requested counts/omissions refer to the post-list scopes;
the new metadata records original authority and missing requested IDs separately.
Calendar exclusions and candidate/score ineligibility remain separate facts.

The [candidate policy example](runspecs/candidate_policy_example.json) has a closed
schema, including nested objects:

```json
{
  "schema_version": "strategy_lab_candidate_policy_v1",
  "name": "profit-with-minimum-trades",
  "version": "1",
  "objective": {"metric": "net_profit_pct", "direction": "maximize"},
  "constraints": [{"metric": "total_trades", "operator": ">=", "value": 15}],
  "selection_count": 1,
  "no_eligible": "unavailable",
  "evidence_classification": "exploratory"
}
```

All fields are required. Name/version are nonblank strings, selection count is
integer 1, and no-eligible behavior is unavailable with no fallback. Constraints
may be empty. Directions are `maximize`/`minimize`, operators `>`, `>=`, `<`, `<=`;
thresholds are finite JSON numbers, excluding booleans and strings. Thresholds
normalize to binary64, including positive zero, before hashing. Constraint order
remains part of identity. Expressions, OOS references, unknown fields/versions,
nonfinite thresholds and duplicate keys fail.

The closed policy-v1 metric vocabulary is:

```text
net_profit_pct, max_drawdown_pct, total_trades, winning_trades, losing_trades,
gross_profit, gross_loss, profit_factor, win_rate_pct, romad,
max_consecutive_losses, sharpe_daily, sharpe_daily_observations,
sharpe_daily_active_days, sqn, rejected_fill_count, zero_size_entry_count,
invalid_stop_distance_count, max_required_leverage, max_drawdown_mtm_pct,
margin_reject_count
```

Every requested metric must exist in every input before group metrics load.
Both tools also require `net_profit_pct` for ties/outcomes, including allocation
with a built-in rule. Per-row nonfinite values in the objective or constraints
make that row ineligible; finite zero is data. There is no implicit trade gate,
positive-profit gate or extra finite-net-profit eligibility test. Scores use the
objective for maximization and its negative for minimization, with positive zero
scores. Exact ties use score descending, finite IS net profit first/descending,
semantic key ascending, then candidate ID ascending. No candidate trade-count
tie-break is added. This does not reproduce a historical script's extra ties.

One policy selects zero or one candidate per ticker/window using only IS views.
All candidate decisions freeze before OOS access; allocation also freezes every
input's common pool and ticker ranking before revealing any OOS. Allocation still
ranks tickers by selected IS net profit, irrespective of the candidate objective.
An unavailable ticker score does not trigger candidate reselection; unavailable
candidate, ticker score and selected OOS remain distinct. `1/K` cash underfill,
missing-block arithmetic and existing controls are preserved. Random-K seeds
include candidate-rule identity, so an equivalent policy and built-in rule need
not share random draws. Repeated identical policy runs remain deterministic.

Analysis adds `declarative:<canonical-policy-sha256>` beside built-in/control rows,
with kind `declarative_policy` and label `exploratory`. Top-5/10 fields and their
lift/percentile summaries are null; policy rule state and Top-5 interval say
`not_requested`, reason `selection_count=1`. The policy never enters official
evidence or nominations. Legacy Top-1 descriptive intervals may remain exploratory;
v3/v4 inference stays not requested. Legacy custom-callable behavior is unchanged,
including its descriptive-family refusal; it cannot be mixed with a policy.

After generating the synthetic datasets in the filter workflow above:

```powershell
$universe = 'tools/strategy_lab/runspecs/analysis_universe_example.json'
$selection = 'tools/strategy_lab/runspecs/candidate_policy_example.json'
& $py -B -m tools.strategy_lab.analysis.cli analyze --dataset "$work/is-on" --scope research --ticker-list $universe --candidate-policy $selection --output "$work/exploratory-analysis"
& $py -B -m tools.strategy_lab.analysis.cli allocate --dataset "on=$work/is-on" --dataset "off=$work/is-off" --scope research --ticker-list $universe --candidate-policy $selection --primary-k 6 --sensitivity-k 8 --output "$work/exploratory-allocation"
```

Allocate requires exactly one of `--rule` and `--candidate-policy`. Existing
`--policy` is a **different file: scope-unlock evidence**. A candidate policy or
universe never unlocks a scope; locked scopes still need `--unlock-scope --policy`.
The Python equivalents are public loaders and optional keyword arguments:

```python
from tools.strategy_lab.analysis import (
    DatasetInput, open_dataset, load_universe, load_candidate_policy,
    evaluate_scope, evaluate_allocation,
)
dataset = open_dataset(dataset_path)
scope = dataset.resolve_scope("research")
universe = load_universe("tools/strategy_lab/runspecs/analysis_universe_example.json")
selection = load_candidate_policy("tools/strategy_lab/runspecs/candidate_policy_example.json")
analysis = evaluate_scope(dataset, scope, universe=universe, candidate_policy=selection)
allocation = evaluate_allocation(
    [DatasetInput("on", dataset, scope)], universe=universe,
    ticker_list_mode="strict", candidate_policy=selection, primary_k=6,
)
```

Pass the validated `Universe`/`CandidatePolicy` values to the evaluators. Each
loader reads and hashes one raw byte snapshot and normalizes that same document;
evaluation does not reload it. An integrity check rejects nested document mutation
before evaluation. Returned provenance, including nested scope-unlock evidence,
is detached from the loaded documents and caller scopes. Python scope APIs retain their trusted-caller
boundary. Unknown membership, missing metrics and malformed definitions fail with
`AnalysisError` (CLI exit 2) before decisions or publication.

Either input adds `strategy_lab_research_inputs_v1` under metadata
`research_inputs`: input paths/raw hashes, canonical documents/hashes, requested
ordered membership, original authority/unlock evidence per label, effective scopes,
canonical sorted final membership and its digest, policy definition, required
metrics, tie/no-eligible semantics and exploratory status. Runtime paths are
provenance, not canonical identity. Summary contains only schema version, universe
document hash and policy key; existing summaries/rows retain counts. Reports render
the inputs and capability limits. Without new inputs the envelope is absent.
Five/six output files and CSV columns stay unchanged. Same-input publication is a
verified no-op; changed raw bytes or definitions are different output evidence.
Existing dataset/filter protection and nonempty-output refusal remain in force.

## Saved-result audits and comparative screens

Three offline reports use validated saved results and explicit research inputs.
They do not execute strategies, select parameters on OOS, nominate policies, or
turn independent accounts into a shared portfolio. Candidate reports support
saved v2/v3/v4; a screen requires one family across its inputs. Fixed comparison
supports descriptive mixtures of saved Fixed v1/v2. The older strict
`compare_filters` remains Fixed-v2-only.

After the bounded synthetic filter workflow above has created `$work/is-off`,
`$work/is-on`, `$work/fixed-off` and `$work/fixed-on`, run:

```powershell
& $py -B -m tools.strategy_lab.analysis.cli field-audit --dataset "$work/is-on" --scope research --candidate-policy tools/strategy_lab/runspecs/candidate_policy_example.json --output "$work/field-audit"
& $py -B -m tools.strategy_lab.analysis.cli screen --dataset "off=$work/is-off" --dataset "on=$work/is-on" --scope research --rule primary_profit --filter-baseline off --primary-k 6 --sensitivity-k 8 --output "$work/filter-screen"
& $py -B -m tools.strategy_lab.fixed compare --dataset "off=$work/fixed-off" --dataset "on=$work/fixed-on" --parameter-axis stopLP --output-dir "$work/fixed-comparison"
```

Output directories must be new or empty; these reports have no resume/no-op mode.
Every file is atomic and the JSON descriptor is published last. An interrupted
directory without that descriptor is incomplete and must not be reused. Output
cannot overlap any input dataset or recorded native target/filter root in either
direction, the canonical Lab evidence directories, or `src/storage`; symlink
ancestors/leaves are refused. Display labels are unique nonblank strings, never
filesystem components; duplicate source identities fail. HTML is escaped, light,
self-contained and links relative artifacts. CSV and shared JSON facts are authoritative. Structured
CSV cells use canonical JSON; empty numeric cells mean unavailable, not zero.

All three new CLIs emit one success JSON document to stdout; failures and
strategy-discovery messages go to stderr. Fixed's package bootstrap also moves
discovery chatter to stderr for existing `validate`, `run`, and `report` commands.
Their JSON payloads and saved files are unchanged; `run` retains its two JSON
payload lines. Existing analysis/allocate may similarly move reader chatter to
stderr. This is scoped to these entry points, not a global registry change.

Public Python entry points:

```python
from tools.strategy_lab.analysis import (
    open_dataset, DatasetInput, evaluate_field_audit, write_field_audit,
    evaluate_screen, write_screen, load_universe, load_candidate_policy,
)
from tools.strategy_lab.fixed import compare_fixed

dataset = open_dataset(is_on_path)
scope = dataset.resolve_scope("research")
audit = evaluate_field_audit(dataset, scope)  # requires an empty access log
write_field_audit(audit, audit_output)
off = open_dataset(is_off_path)
on = open_dataset(is_on_path)
screen = evaluate_screen([
    DatasetInput("off", off, off.resolve_scope("research")),
    DatasetInput("on", on, on.resolve_scope("research")),
], candidate_rule="primary_profit", filter_baseline="off")
write_screen(screen, screen_output)
compare_fixed([("off", fixed_off_path), ("on", fixed_on_path)], fixed_output,
              parameter_axes=["stopLP"])
```

Evaluators accept loaded `universe` and `candidate_policy` values. Screen requires
exactly one of `candidate_rule` and `candidate_policy`; it also accepts
`ticker_list_mode`, `primary_k`, `sensitivity_k`, `baseline` or `filter_baseline`.
Fixed accepts `universe` (or a `ticker_list` file), `ticker_list_mode`,
`allow_partial`, and `parameter_axes`. Each input resolves membership independently
in saved order; strict membership is the default. Explicit intersection rejects
wholly unknown IDs, empty per-input membership and an empty common input roster.
The common-roster guard retains each input's effective subset and saved order.
Without a Fixed universe,
declared inventories and per-input membership are retained.

| Descriptor schema | Authoritative tables and nested publication |
| --- | --- |
| `strategy_lab_field_audit_v2` (`audit.json`) | `cell_summary.csv`, `candidate_summary.csv`, `axis_marginals.csv`, `neighbour_stability.csv`, `concentration.csv`; unchanged five-file `analysis/` |
| `strategy_lab_screen_v1` (`screen.json`) | `comparisons.csv`, `candidate_turnover.csv`, `concentration.csv`; unchanged six-file `allocation/`; strict pairs add `filter_candidate_pairs.csv` and `filter_selection_pairs.csv` |
| `strategy_lab_fixed_comparison_v2` (`comparison.json`) | `accounts.csv`, `calendar.csv`, `summaries.csv`, `neighbours.csv`, `concentration.csv`, shared `cohort_facts.json` |

The [comparative report schema](COMPARATIVE_REPORT_SCHEMA.md) defines every column,
row level, structured cell and shared-fact join. Fixed v2 replaces repeated cohort
payloads with references; field v2 uses stable window groups with exact IS/OOS
interval metadata. Screen v1 adds nullable `same_candidate`. Historical reports
remain unchanged.

Descriptors retain input manifests/hashes/contracts/families, effective scopes,
unlock evidence, research-document identities, membership, metric units, Git
facts, counts/omissions, interpretation and artifact sizes/hashes. Field audit
runs the existing analysis first, then a window-major diagnostic pass with its
own access offset/log. Screen calls allocation once; strict candidate-pair CSV
streams in an authorized post-freeze pass. Fixed captures validated scalar and
monthly snapshots during the reader's mandatory account validation, retaining
no curves; the old filter comparator uses the same collector.

Field tables identify every candidate by semantic key, local ID and scalar
parameters. Cell rows include expected/finite/missing counts, positive shares
over expected and finite denominators, zero trades, means/medians/linear quartiles
and extrema. Spearman uses paired finite values, average ranks and a minimum of
eight; constant vectors are unavailable. Candidate means use equal finite tickers
within each block, then equal available blocks. Marginals first average finite
candidates within a block/mask/active-axis value, then use that hierarchy. No
inactive value becomes an axis setting. Candidate ticker/block rows carry net
return separately; drawdown/trades have candidate summaries with the same
weighting. Finite zero is data; NaN/infinity are missing.

Policy feasibility counts finite objectives and each constraint independently,
then all-constraint eligibility, without an implicit trade gate. Selected OOS
comparisons and detailed neighborhoods reuse frozen IS decisions from the policy
or saved baseline rule. OOS never reselects. Core `star_neighbours` includes the
center and preserves geometry block/mask and globally generated code adjacency:
codes absent everywhere are bridged, codes present only in another block are not.
Categorical adjacency does not mean numeric proximity. Aggregate neighborhood rows
cover every candidate/segment; detailed rows cover only IS-selected candidates.
No geometric neighbor gives `insufficient_neighbors`; no finite distinct neighbor
gives `no_finite_distinct_neighbor` and null mean/worst/positive share. A nonfinite
center makes center-relative deltas unavailable. Missing members remain in counts;
no automatic robustness threshold is applied.

Screen `--scope-for LABEL=NAME` overrides a default scope. Each locked input needs
its own `--unlock-policy LABEL=PATH` plus `--unlock-scope`, even when files are
shared. Partial scopes/incomplete inputs need the existing explicit flags.
All inputs freeze IS candidate decisions and common-pool ticker rankings before
OOS. Primary/sensitivity, cash underfill, random seeds/draws, bottom/oracle/anti-oracle
controls, omissions and ticker turnover reuse allocation. Matched fraction is
legacy-only. Candidate turnover compares nonnull semantic keys in adjacent common
blocks (including gaps), independent of OOS or common-pool exclusion.

Only an explicit `--baseline` creates generic input-minus-baseline deltas. Capacity
headlines require every paired block to be finite. All-available deltas also require
identical nonempty finite-OOS ticker sets; otherwise `different_finite_ticker_sets`
or `no_finite_tickers` explains the null. Both sets and values remain visible; no
new paired-subset mean is invented. Generic comparison is `descriptive_aligned`,
with strategy/economics/TF/source/IS-geometry confounders disclosed, and no new
inference. Legacy nested inference is preserved.

`--filter-baseline` requires exactly two complete v4 inputs, effective Off then
active, equal effective ticker and full IS/OOS window sets, and the released exact
non-filter saved projection: generation minus only `execution.entryFilters`,
analysis/rules/seeds, complete candidates, non-filter identity fields, portable
target pins, segment preparation facts and operative admission. Filter-dependent
plan/binding hashes and source locators are not equality requirements; readers
still validate every saved hash. The three estimands stay separate: **same
configuration**, **policy reselection with filters**, and **policy and ticker
allocation on the common IS-eligible pool**. Reselection joins frozen semantic keys
to each side's saved OOS, including local selections excluded from the common pool;
selection, finite selected-IS score and pool membership have separate columns.
`same_candidate` compares nonnull frozen semantic keys independently of these
availability flags; a missing key yields null.

Fixed primary summaries aggregate tickers only at **variant × economics stratum ×
exact evaluation-period key**. Variant contains producing strategy identity,
non-runtime/non-economic scalars, effective filters and TF. Stratum contains
strategy/version, execution policy, all economics and funding/slippage flags.
Period keys normalize saved `evaluation_start`/`evaluation_end` to UTC; names,
requested strings, aligned bar ends and warmup are retained facts, not join keys.
Duplicate account IDs across inputs and ambiguous duplicate tickers within a
primary grain fail. Pending/failed declared accounts require `--allow-partial`;
corrupt completed claims always fail.

For each metric, common membership intersects completed finite ticker sets across
all variants in that stratum/period cohort. `cohort_id` joins shared roster and
metric facts in `cohort_facts.json`; account references retain exact pins, bounds,
admission and missingness. Available coverage divides by the variant's declared
roster; common coverage divides by the cohort union. Both expose a separate
positive/union share and `cohort_expected_count`. Missingness includes failed and
pending accounts; available trade totals require the variant roster to be finite,
while common totals require the whole union to be finite in every variant.
Empty common sets are unavailable. Equal membership with unequal pins/bounds/TF/
admission is `descriptive_common_membership`. Trades expose observed sums and
medians with complete totals unavailable when expected members are missing.
Cross-period/TF rows list primary estimates and finite count/min/median/max, with
no pooled mean headline or chaining. Monthly rows remain continuous calendar
slices with saved opening denominators and partial-month facts; no monthly trade
count or drawdown is invented.

Fixed neighborhoods require explicit numeric strategy axes; unknown, boolean,
economic and nonfinite axes fail. Sorted declared values include failed/pending
configurations, so a failed middle value is never bridged. Edges change exactly
one axis and retain its actual distance. Comparable edges require the same ticker,
exact period, target pin, admission and evaluation geometry. Per-edge returns,
drawdowns and trades retain missingness; neighborhood net-return summaries follow
the finite-distinct-neighbor convention above. Without axes the table explicitly
says `not_requested` and gives the command hint.

Concentration groups explicit additive contributions by ticker and time separately.
It reports signed observed/complete totals, positive gross, absolute negative gross,
positive shares, the largest positive contributor (canonical ID/UTC tie break) and
arithmetic total excluding it. No positive contribution means null shares/removal;
zero net totals are not percentages of net. Field contributions reconcile to the
finite-support hierarchical candidate mean, with partial-support counts. Field
time groups are `window:<window_id>`; `audit.json.window_groups` maps each to both
IS and OOS endpoint pairs. Segment chooses the interval; overlapping IS windows
remain separate descriptive blocks. Equal-largest ties retain saved OOS-start
string order, with window ID breaking identical-start ties. Screen
member contributions divide by the total expected common-block count, so they
reconcile to mean capacity return; an unavailable block contributes no partial
member subtotal and makes the complete total unavailable. Cash contributes zero.
Fixed sums saved realized currency P&L only within compatible currency/capital/
economics/variant/period groups, and shows monthly realized P&L and MTM changes as
separate channels. It never sums return percentages or overlapping independent runs.
Exclusion never reranks, reruns or reweights a policy.

## Dataset and resume contract

The legacy CSV generator builds one validated V2 plan and evaluates every candidate over
independent IS/OOS prepared segments. Effective `compiled_numba` execution is
required; unavailable compilation or silent reference fallback fails before a
group is published. Calendar windows come from Merlin's authoritative builder,
and every segment contains exactly 1,000 warmup bars before its evaluation
range.

Current `strategy_lab_dataset_v2` group arrays are `float64` with axes
`[candidate, segment, metric]`, segment order `[is, oos]`, and 21 declared
metrics. The final metric is research-only bar-close
`max_drawdown_mtm_pct`; it is not a public optimizer objective or persisted
Merlin run result. It differs from realized balance Max DD and TradingView's
intrabar High/Low drawdown. Both generic position and signal-reversal execution
support this metric. See [metrics authority](../../docs/METRICS.md).

`candidates.json` preserves candidate order, identity, and geometry. Group
files publish atomically with checksums. `manifest.partial.json` claims only
published groups; resume revalidates schema, run, inventory, plan, axes, scope,
version, resources, digest, size, shape, and dtype. Missing or corrupt claimed
groups regenerate. `manifest.json` is the sole completion marker, and a fully
verified identical rerun returns `verified_noop`. A completed output is
immutable if later source, identity, or artifact verification fails.

The scope is `full` only for the exact accepted inventory and every declared
window; strict subsets are `smoke`. Resource settings are identity-bearing for
the frozen run. Changing worker, Numba-thread, or cache limits requires an
explicitly reviewed refreeze.

Analysis and allocation likewise reject output beneath an input dataset,
preserve incompatible existing output, and return `verified_noop` for a
byte-identical rerun. Development is the default scope. Holdout and temporal
scopes require both an explicit unlock and a frozen policy; allocation and
analysis must not infer permission from path names or available arrays.

## Interpretation boundary

The primary holdout is cross-sectional confirmation on new tickers over the
same months as development, not independent temporal confirmation. Later
windows are mandatory descriptive temporal checks. Random controls retain only
their deterministic summary, and oracle controls are non-deployable.

Canonical schema-v1 and schema-v2 datasets were generated and certified
locally. They are ignored, immutable evidence and may be absent in a fresh
clone. They are never silently regenerated or
treated as tracked fixtures. Historical prototypes or task records under
`docs/_work/`, when present locally, are non-authoritative development history
and are not imported by Strategy Lab.

## Related authorities

- [Documentation map](../../docs/README.md)
- [V2 architecture](../../docs/engine_v2/ARCHITECTURE.md)
- [Certification evidence](../../docs/engine_v2/CERTIFICATION.md)
- [Test workflow](../../tests/README.md)
