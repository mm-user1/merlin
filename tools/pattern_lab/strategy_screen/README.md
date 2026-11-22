# Native Strategy Screen

Strategy Screen is a descriptive, two-period backend. It executes every explicitly
declared setting in each period with the accepted account executor, retains exact
compact monthly evidence, and calculates canonical numerical summaries. It does
not choose a trading universe, optimize settings, or carry money
or positions between periods. No outcome establishes an executable edge.
The separate [offline report](../strategy_screen_report/README.md) consumes this
public reader after full verification: `strategy-screen-report --run-root SCREEN
--output-root NEW_REPORT [--presentation FILE --export-view-data]`. Report output
does not require market packs, auxiliary snapshots or retained execution paths.

## Python and CLI

Run from the repository with the configured Windows Python and `-B`:

```python
from tools.pattern_lab.strategy_screen import (
    normalize_request, run_strategy_screen, load_strategy_screen,
    verify_strategy_screen, analyze_strategy_screen,
)
reader = run_strategy_screen(request, data_root=pack, output_root=new_screen,
                             workers=2, auxiliary_roots={"aux": snapshot})
check = verify_strategy_screen(reader.root)
analysis_root = analyze_strategy_screen(reader.root, output_root=new_analysis)
```

Omit `auxiliary_roots` for ordinary v1 native children. An empty mapping is accepted
for a parent with an empty `auxiliaryInputs` list. Otherwise the root aliases must
match the complete parent snapshot declaration exactly. Offline operations need
only the completed screen, not market inputs, snapshots or the generating source
version. They do not execute accounts or contact the network.

```text
python -B -m tools.pattern_lab strategy-screen --request request.json --data-root PACK --output-root NEW_SCREEN --workers 2 --auxiliary-root aux=SNAPSHOT
python -B -m tools.pattern_lab strategy-screen-verify --run-root SCREEN
python -B -m tools.pattern_lab strategy-screen-analyze --run-root SCREEN --output-root NEW_ANALYSIS
python -B -m tools.pattern_lab.examples.strategy_screen_synthetic --output-root NEW_EXAMPLE --auxiliary --workers 2
```

`--auxiliary-root` is repeatable. Analysis exports `analysis.json` and `receipt.json`
with deterministic bytes, source attribution and the exact input completion hash.
It contains numerical summaries and metadata, not HTML or a full account archive.

## Request

The closed `pattern-lab.strategy-screen-request.v1` object has required `schema`,
`studyId`, `allocations`, `sources`, `cells`, `periods`, `criteria`. Optional fields
are `segments` (default `[]`), `previousReviewEndUtc` (default null), `notes`, and
`auxiliaryInputs`. Presence of `auxiliaryInputs`, even `[]`, selects native v2
children and requires `auxiliaryFeatures` on allocations. Absence selects v1.

Allocations, candle sources, auxiliary sources, features, providers and policies
use the existing native account request contracts and default expansion. The
[synthetic example](../examples/strategy_screen_synthetic.py) exposes `request()`
and `build_inputs()` for a complete six-setting, trade-bearing request. The latter
includes HA, a candle filter and a scalar auxiliary provider with ATR stops.

```json
{
  "cells": [{"instrumentId": "BYBIT_AAAUSDT", "timeframeMinutes": 1440,
             "allocationIds": ["rr2", "rr3"]}],
  "criteria": {"annualizedProfitPct": 10, "mtmMaxDdPct": 25,
               "minClosedTrades": 30, "minClosedTradesByTimeframe": {"1440": 10}},
  "segments": [{"segmentId": "recent-tail", "periodId": "recent",
                "startUtc": "2020-05-01T00:00:00Z", "endUtc": "2020-07-01T00:00:00Z"}],
  "previousReviewEndUtc": null
}
```

This excerpt supplies only the cells/criteria/segments portion. Each of exactly
two `periods`, in logical `earlier`, `recent` order, has `periodId`, `startUtc`,
`endUtc`, `preparationStartUtc`, and a native `protocol`. New request protocol
paths resolve relative to the request file; stored protocols must be objects.
Periods and segments use whole UTC calendar months, `[start,end)`, with
`earlier.end <= recent.start`. Gaps and a shared protocol are permitted if every
projected native child passes its own admission. Cutoff must be strictly inside
recent. A null cutoff means `firstScreenSnapshot`, never prospective evidence.

Cells are explicit and may be unequal or sparse. No missing cross-product cell
is invented. All allocations and bindings must be used somewhere in the parent;
each child projects only its selected allocations and consumed bindings. Full
instrument IDs remain identities across venues. Equivalent normalized settings
are rejected per cell. Allocation renaming does not change configuration identity;
snapshot/input identity is separate from logical configuration matching.

Python `minClosedTradesByTimeframe` keys must already be canonical positive-integer
strings naming declared TFs. Integer/bool/float keys are rejected before JSON
ownership copying; normalization never changes the caller's mapping.

Both periods are admitted before creating output or executing recent. Execution
order is recent first, then earlier, with ascending instrument, numeric TF and
configuration ID. Each account starts flat with the common initial capital,
continues through its period, and closes normally at its terminal observation.

## Reader and downstream data dictionary

The reader exposes fresh copies; no consumer needs to parse ZIP member names.

| Access | Return value |
| --- | --- |
| `metadata` | Stored request, normalized configurations, implementation, identities, synthetic flag, counts, verification limitations and artifact identity |
| `configurations` | Canonical configurationId/allocationId/instrumentId/timeframeMinutes/cellId rows; recipes are in metadata request |
| `result_rows` | Canonical configurationId/periodId/resultId/status enumeration |
| `result(configurationId, periodId)` | Facts version, metrics, every month, execution attribution, terminal and audit facts, classification and per-setting analytics |
| `months(configurationId, periodId)` | Chronological declared monthly records, including unavailable months |
| `scopes` | Cell, instrument, timeframe, then panel scopes, each with explicit constituent cell IDs |
| `summary(scopeId='all')` | Scope membership, earlier/recent summaries, changes and matched-pair persistence |
| `chunk_index`, `raw_member(name)` | Fresh index and integrity-checked bytes for specialized offline transport |

Whole-period `metrics` are copied from canonical `extract_metrics` unchanged.
`execution` retains native account/preparation/command IDs, prepared input headers
and fingerprints, rules, auxiliary snapshot pins, resolved source descriptors and
consumed-slice fingerprints. `audit` records extraction counts and reconciled
gross/net/fees, terminal fees and monthly carried-peak DD maximum. Synthetic
compact-only fixtures explicitly have `syntheticFixture` execution with no native
validation claim.

Monthly amounts are quote currency; percentages are percentage points, unrounded.
`diagnosticPnl`/`diagnosticContributionPp` preserve synchronized cash observations.
`pnl`, `contributionPp`, `windowReturnPct` and `cumulativeReturnPct` have explicit
availability. An analytical month needs positive expected slots, full coverage,
and exact opening/closing boundaries. Coarse TFs may have full whole-period
coverage while internal months are inexact. Zero expected slots are unavailable.
Only native proven-flat cash gaps can be synchronized; this never creates an
observed bar. A later exact month can contribute after a missing prefix, while
cumulative-from-start stays unavailable.

For initial capital C, opening O and close E, contribution is `100*(E-O)/C`;
opening-relative return is `100*(E/O-1)` only if O>0; cumulative is
`100*(E/C-1)` only for an exact complete prefix. Closed/terminal counts and
terminal exit fees share half-open exit-month ownership, except period-end exits
belong to the last month. Equity closes use `(monthStart,monthEnd]`, so the two
month assignments can intentionally differ. Terminal fees are already in equity.

Each observed month's `carriedPeakMaxDdPct` uses the period running positive peak,
not the month opening. Signed equity may produce DD above 100%. A no-observation
month has null carried DD. `localDrawdownPct` is diagnostic and null without a
positive local peak. Monthly extrema/local DD cannot reconstruct signed-path
chronology. The 100 -> 200 -> 50 -> -10 path has 105% carried DD.

Classification requires completed execution, positive full coverage and finite,
available return, annualization, MTM DD and trades. Profitable means strict >0;
Interesting uses inclusive annualized-return/DD/trade thresholds. Low activity
uses the effective per-TF threshold whenever trade count is known, independently
of classification. Short native annualization can make a flat February account
unclassifiable while its zero return/trades remain real available values.

The protected pack-preparation API refuses a required research span with no
complete bar before producing an account. This is a supported failure limitation:
the entire screen fails with `nativePreparationRejected`, its owning period/cell
and honest completed count, even when recent accounts already completed. No
complete summary or seal is published. This reason also covers other native
preparation data rejections; it does not assert that every rejection is empty.
Clock, storage and worker failures have separate reasons. Genuine native
unobserved-subwindow metrics/classification remain unavailable where supported.

Scope breadth exposes pooled counts and `equalCellPct` separately. Multi-cell
cards average cell fractions including all declared settings. Metric medians pool
independently eligible settings; `n`, status and reason accompany statistics.
Trades/month uses the integer declared month count. Conditional persistence pools
matched classifiable pairs with its own success/nonsuccess denominators. Criterion
failures overlap; unavailable observations are separate.

Monthly contribution/cumulative quantiles use linear `(n-1)*p` interpolation.
Monthly/window positive shares use pooled positive/declared counts; no available
values means null percentage. Equal-cell mean contribution requires all settings.
Own/common best-one/two-month residuals are arithmetic attribution, not reruns.
Ties are chronological, removing all months yields exactly zero, too few months
is unavailable, and a genuine small nonempty residual keeps strict >0. Common
selection requires complete scope evidence. Both pooled and equal-cell residual
shares are named. Per-setting own residuals, latest-three-month, segments and
newOnly facts are stored under `result()['analytics']` with effective criteria.

Latest means the last three declared months without skipping missing months.
Windows expose separate return/contribution/trade availability; nonpositive
opening disables only return. Contributions/trades remain when exact. NewOnly
describes the current run after cutoff; it does not splice older terminal facts.

T03B owns geometry, colors, labels, histogram bins and UI state. It must not
reclassify facts or redefine summaries. Cumulative points lie at month-end (the
next first-of-month); initial display point is zero. A linked display may use
`100*((1+earlierFinal/100)*(1+recentCumulative/100)-1)` only with an exact earlier
cumulative endpoint. +50% then -20% gives +20%. Preserve calendar gaps without
inventing connecting observations. This display never represents carried capital.

## Storage, resources and failure states

The six-file layout is `study.json`, `index.json`, `results.zip`,
`provenance.json`, `status.json`, and last-written atomic `completion.json`.
Economic schemas are `pattern-lab.strategy-screen.v1`, its index/chunk/completion
v1 schemas, facts version `strategy-screen-facts.1`, and analysis version
`strategy-screen-analysis.1`. Results are grouped at most 16 per chunk with byte
splitting; each summary has a chunk. Canonical numeric JSON is gzip-compressed
with mtime zero inside an ordinary ZIP_STORED archive. ZIP64 is refused.

Bounds are 16 MiB per root JSON, 8 MiB decoded member, 512 MiB aggregate decoded,
128 MiB archive including directory, and 65,534 members. Calendar/roster/metadata
estimates precede execution; actual limits are enforced before publication.
Physical root JSON is canonical ASCII-escaped UTF-8 plus CRLF on Windows or LF
on other platforms. Admission and atomic publication use the same checked bytes.
An individual calendar is bounded to 8,192 months and aggregate account-month
planning is additionally bounded. There is no precision reduction or truncation.

Workers are 1..6; one uses direct AccountJobs, larger counts reuse the existing
spawn pool and single-threaded child controls. Preparation is scoped to a cell
and period. Submitted-but-not-canonically-consumed credit stays <= effective
workers, even for early out-of-order completions. Full returned arrays/trades
are extracted and released immediately; bounded compact chunks and narrow
numeric projections support summaries. No durable full native study is created.

All input/output roots must be disjoint and outputs new. Symlink/reparse path
components are rejected before resolution. Preflight rejection leaves output
absent and exposes a fresh safe Python domain error without private exception
context. Before any mkdir, ancestors are checked by entry name for `completion.json`,
`study.json`, `selection.json`, `status.json`, or the pair `analysis.json` and
`receipt.json`. Malformed files, directories and dangling marker links reserve
ownership without being opened. Inspection errors reject admission. This also
conservatively reserves unrelated `status.json` files. An unmarked empty directory
or half of an analysis pair is not a recognizable artifact; ordinary siblings
remain valid. This is a bounded existing-output guard under single-writer use,
not a general filesystem race sandbox.

Before reservation, execution budgets both the actual initial plan and a fixed
full-roster initialization-failure envelope against the physical JSON limit.
Until the first plan publication returns successfully, failures write that exact
precomputed envelope. After acknowledgement, failure status references local
`study.json` and the normalized request identity; it never duplicates the roster.
The reference works for the initial plan and for final study metadata published
before sealing. This admission applies only to new execution, not stored readers.
Later failure status retains bounded period/cell/configuration IDs (no display
labels), counters, worker count, at most six affected jobs, allowlisted phase/reason
and optional fixed cleanup flags. No exception names, messages or graphs are saved.
Owned resources close and unsuccessful runs do not seal. Atomic
completion is the commit point; a late load/return failure cannot invalidate it.
An existing or uncertain completion marker prevents failed-state rewriting even
when its contents cannot temporarily be read. A failed reservation never grants
ownership. Worker-loss status maps only fully known pending IDs: one owner, an
ordered bounded `affectedJobs` list for several, or no precise owner. Receive-wide
failures keep period/cell context; each buffered append sets its own coordinate.
Completed counts advance only after a successful append. Unwritable storage and
hard process stops cannot guarantee a saved failed status.
Source generations are checked around dispatch and immediately before sealing.
Group checks clear precise job ownership and retain only the owning group. Normal
pool shutdown and publication clear group/job ownership. Cleanup failures do not
replace primary failure attribution.

Preparation classification follows at most 16 explicit-cause/unsuppressed-context
exceptions, with cycle detection, and at most 64 traceback frames per exception.
Typed OSError takes precedence. Only a domain error whose actual raising frame
is the pinned native in-memory `data.prepare_series` function is recognized as
`nativePreparationRejected`; other origins and traversal limits produce
`strategyScreenOperationFailed`. The private classifier inspects types and code
identities, never messages, filenames, line numbers or locals. It cannot classify
all legitimate native rejections without a richer provider API. Interrupts still
propagate after cleanup; public errors contain no private cause/context.

`load` checks sealed structural/content integrity and fresh-copy access without
per-result central-directory reparsing or a permanent raw-result cache. `verify`
also re-derives numerical summaries. These checks are **compact consistency**,
with `fullExecutionRetained=false`. Full execution, per-fill fee arithmetic,
daily Sharpe chronology and DD-path chronology cannot be replayed from monthly
facts. Recorded attribution can differ from current sources; coherent fabricated
history cannot be authenticated by these checks.

Live extraction checks normalized native policy/window/candle declarations,
HA/filter or auxiliary provider/feature/component metadata, actual dependency
bindings and native identity links before pruning. Actual HA/scalar command
provenance joins the declared provider, decision identity and feature identity map.
Every feature joins its consumed slice (one fingerprint per binding, reused for
compact attribution). Component IDs/declarations, scalar source feature identity,
native candle provenance and ordered filter references must agree, including empty
v2 decisions. Decision, feature and component evidence IDs must be 64 lowercase
hexadecimal characters; returned decision schema/projection must be the supported
auxiliary-decisions.v1 / derived-target-decision.1 pair, even for empty v2.
These live checks do not rehash command arrays or reread snapshots. Extraction, writer admission
and stored verification all recompute the native preparation/account equations
and validate recorded execution rules against the target header. Observed coverage
joins first-open/head and last-close/tail on the period grid. Each month's count
fits the grid interval from max(month-start, first-open) through its closing sample.
The first-close month must contain that observation, with at least two observations
when its closing sample is later. Head plus tail cannot exceed total missing bars
for observed accounts. With zero observations, each missing edge count equals the
whole expected count; their sum is deliberately not bounded by that count. Terminal counts
and fees belong to the month derived from the last observed close; equity's
separate boundary convention is retained. Exact observed closing endpoints impose
a DD floor using the full prefix of monthly maxima, with native tolerance. Signed
equity, inexact boundaries and unobserved months retain their existing semantics.

Compact verification cannot derive commands from commandId, detect a coherently
borrowed provider-only command/account pair, hash original candles from headers,
or authenticate a consumed slice against an absent snapshot. Snapshot pins are
admission provenance. Stronger consistency checks do not establish authenticity
or replay the pruned path.

The current task's verification scope is Windows. Linux case sensitivity,
numerical portability and cross-host replay are not claimed. See the maintained
five `test_strategy_screen_*` modules and [test guide](../../../tests/README.md).
