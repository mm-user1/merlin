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
- `execution` exactly `parameter_mode: "fixed"`, `policy: "risk_bracket_v1"`,
  `entry_filters: []`. Active filters, ties, ranges, axes, optimization, WFA,
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
integration. Active F06, S03/S06 Parquet, selectors, carried capital, FT/stress and
delayed OOS remain unsupported and are rejected.

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
