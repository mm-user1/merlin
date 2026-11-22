# Offline Strategy Screen report

Render a completed native Strategy Screen into a standalone HTML. This separate
report does not execute accounts, read market packs or auxiliary snapshots, or
select a trading strategy. Native compact verification is required before mapping;
a structural load alone is insufficient. Both independent periods are immediately visible.

## API and commands

```python
from tools.pattern_lab.strategy_screen_report import render_strategy_screen_report
html = render_strategy_screen_report(
    screen_root, output_root=new_report,
    presentation=None, export_view_data=True,
)
```

```text
python -B -m tools.pattern_lab strategy-screen --request request.json --data-root PACK --output-root SCREEN --workers 2
python -B -m tools.pattern_lab strategy-screen-verify --run-root SCREEN
python -B -m tools.pattern_lab strategy-screen-report --run-root SCREEN --output-root REPORT --presentation presentation.json --export-view-data
```

On Windows use the configured project Python, with `-B -X utf8`. The report
command has no workers, market roots, thresholds or finalist options. Success
returns JSON with `status=complete`, `command=strategy-screen-report` and `report`.
Outputs are `report.html`, `report.json`, and optionally `view-data.json`.

A maintained small artificial-price example, in a new external temporary root:

```text
python -B -m tools.pattern_lab.examples.strategy_screen_synthetic --output-root NEW_EXAMPLE
python -B -m tools.pattern_lab strategy-screen-report --run-root NEW_EXAMPLE/screen --output-root NEW_REPORT
```

Add `--auxiliary` to the example for the genuine auxiliary execution route.
These examples retain the native `synthetic=false` marker, since accounts really
execute on artificial prices; when distributing example previews provide the
subtitle `Artificial-price native execution fixture; not market research`.
Compact-only test fixtures retain `synthetic=true` and display their synthetic label.
Historical experimental facts are not accepted by the public API.

## Presentation

The optional mapping or UTF-8 JSON file is bounded to 1 MiB and uses a closed
schema. Required `artifactIdentity` matches the native reader metadata:

```json
{
  "schema": "pattern-lab.strategy-screen-report-presentation.v1",
  "artifactIdentity": "<reader.metadata artifactIdentity>",
  "title": "Two-period screen",
  "subtitle": "Descriptive research",
  "periodLabels": {"earlier": "Earlier", "recent": "Recent"},
  "parameterLabels": [
    {"path": "/policy/rewardRisk", "label": "Reward / risk", "shortLabel": "RR"}
  ]
}
```

Title/subtitle limits are 512/2048 characters. Optional earlier/recent period labels
and parameter label/shortLabel/unit strings are nonblank and at most 128 characters.
Parameter paths are unique ordered RFC6901 pointers resolving to scalar leaves in
at least one used normalized allocation. Other allocations display “Not applicable”;
explicit null displays “null”. Booleans, strings and numbers remain distinct.
Unresolved paths, containers and nonfinite values are refused. No inferred strategy
parameters: default setting labels are allocation IDs. Full recipes occur once in
collapsed Experiment details. Default period labels show exact UTC dates.
Presentation affects report identity only.

## Native-to-view mapping

Let R be `reader.result(configurationId, periodId)`, P be
`reader.summary(scopeId).periods[periodId]`, and S the full scope summary.
Each result and each summary is read once during mapping. Only narrow numeric
monthly arrays and shared metadata/recipes are retained in the view. The public
reader owns container decoding; this package does not parse ZIPs or cache chunks.

| View | Source |
| --- | --- |
| Canonical setting/result/scope/cell identities | Public configurations, result_rows, scopes and per-result IDs |
| Whole metrics and field eligibility | R.metrics, status, performanceStatus, annualizedProfitStatus |
| Classification, low activity, effective criteria | R.classification and R.analytics.effectiveCriteria |
| Individual contribution/cumulative arrays and reasons | R.months exact, reason, contributionPp, cumulativeReturnPct/cumulativeStatus |
| Seven cards / all matrices | P.breadth equalCellPct with pooled success/declared/available/missing; P.metrics statistics; P.concentration.own['2'] |
| Success retained and comparison | S.persistence earlierSuccess, earlierNonsuccess, retentionDifferencePp |
| Metric/breadth deltas | S.changes; only own-best-two delta is recent minus earlier saved equalCellPositivePct |
| Whole histogram counts | Eligible full-coverage R.metrics returnPct/annualizedProfitPct on shared 10pp edges; quantiles copied from P.metrics |
| Monthly median/bands/distributions | P.monthly contribution/cumulative statistics; available R.months contributions for bin counts |
| Concentration / latest / segments / new-only | P.concentration, latestThreeMonths, segments and newOnly, preserving reasons and denominators |
| Criterion failures | P.failures, including unavailable; display percentage = 100 * count / declared |
| Periods, recipes, costs, cutoff and thresholds | Metadata request; per-timeframe minimum overrides retained |
| Synthetic/provenance/limitations | Native metadata, kept distinct from current renderer/verifier sources |

No annualization, classification, financial quantile, residual, window or retention
recalculation occurs in the report mapper. Saved medians pool eligible settings.
Multi-cell breadth and residual cards use equal-cell percentages; pooled counts
are supporting counts, not a replacement fraction. Failure criteria overlap.

Eligibility is per field: completed result, finite actual number (not bool),
available performance except closed trades, plus available annualization for the
annualized field. Whole histograms also require positive expected bars and equal
observed/expected counts. Their bin totals must equal saved metric n. A partial
return may rank individually while being excluded from a histogram. A short flat
February can have zero return/trades and known low activity while classification
is unavailable. Breadth may honestly show 0/N with zero classifiable observations.

Monthly contribution bins share one global step max(1, ceil((max-min)/16)),
independent of the whole-return 10pp bins. Domains include zero; bins are left
inclusive/right exclusive, except the final right edge. Zero occupies a nonnegative
bin. Empty data shows N/A/n=0; all-zero has at least [0,step]. Extreme representable
ranges use occupied sparse bins at true positions with bounded ticks; unrepresentable
geometry is refused, never clipped or coarsened.
Membership compares the actual indexed binary64 boundaries, including adjacent
representable observations. Both languages use `lo + i * step` for each edge;
neighboring intervals share exactly the same boundary. Subnormal negative values
extend the domain below zero rather than disappearing through division underflow.

Monthly contributions and cumulative paths have independent availability. Missing
prefixes break cumulative paths without suppressing later exact contributions.
UTC month closing points are at the following month boundary. Linked display uses
the exact earlier final cumulative endpoint and recent cumulative observations;
missing endpoints stay unavailable. Calendar gaps break both paths and hit testing.
Linked Min/Max requires both exact final endpoints. This illustration does not
change execution or imply a continuous backtest.

## Interaction and publication

Seven cards, compact/normal matrices and selected-scope charts use the approved
shared scales. Scatter categories select earlier identities in both periods;
low activity is independent of classification, and hollow markers use each period.
The individual chart has all configurations, its own ticker/TF/category/Min/Max
state, adaptive earlier-return hues and independent viewport/reset controls.
A matrix selection presets chart filters in one direction; chart changes never
alter aggregate scopes. Hover/pin, keyboard, wheel zoom and drag pan use a cached
base canvas and separate overlay, without idle polling or a line-count cap.
Scatter axes include the criteria even without classifiable points. Inspected
earlier/recent metrics use their own availability, with reasons, account coverage
and effective trade minimum. Evidence labels distinguish verified native execution,
compact-only fixtures and explicitly identified legacy analytical previews.
Matrix arrows follow rendered row/column coordinates, skipping holes and stopping
at boundaries within the same matrix. Distribution metric changes retain focus and
the individual chart viewport without scrolling.

Output must be new or empty, disjoint in both directions from input and any
presentation path, and outside protected src/data/tests trees. Components are
inspected before resolution; linked/reparse paths and inspection errors are refused.
Ancestor marker names completion.json, study.json, selection.json, status.json,
report.json, or analysis.json plus receipt.json reserve ownership even if malformed
or dangling. Siblings are permitted.

Inputs/options are validated before creating output parents. Unique temporary
files are checked and destinations claimed exclusively. report.json is published
last and is the completion point. A successful final replacement confirms publication.
If that call raises, one bounded reconciliation distinguishes the exact receipt
from the intact owned empty claim. The former confirms success; the latter permits
cleanup of owned incomplete files and new empty directories. Competing files and
pre-existing empty roots survive. An inconclusive or interrupted read preserves
output for inspection and reports unknown publication; retrying into that nonempty
directory is refused. No read failure can prove that a report was not published.
This is not a crash-atomic filesystem snapshot or an adversarial concurrency claim.
Public errors expose a fixed message without private cause/context; interrupts
remain fresh KeyboardInterrupt instances. Unknown publication uses the fixed message
`Strategy Screen report publication could not be confirmed; output was preserved for inspection.`
CLI failure/interruption use exits 2/130 and fixed JSON; uncertainty also includes
`publicationState=unknown`. Either replacement or reconciliation interruption selects
the interrupted outcome. Normal publication still returns the HTML path and exit 0.

CLI success notification includes stdout write and flush. A notification failure
or interrupt preserves the report, returns 2/130 and attempts one fixed stderr
diagnostic stating that publication succeeded but notification failed/interrupted.
It never attempts another stdout payload. Secondary diagnostic failures preserve
the determined disposition; a broken buffered pipe cannot change it to shutdown
exit 120. These semantics apply only to the Strategy Screen report command.

Receipt schema is `pattern-lab.strategy-screen-report.v1`; view schema is
`pattern-lab.strategy-screen-report-view.v1`. Receipts bind the input completion
hash captured before verification and checked after mapping, verified identity/
counts, historical native implementation, synthetic marker, normalized presentation,
current generator/dependencies, view and HTML. Compact consistency is not replay
verification or authenticity against coherent rewriting.

The explicit dependency list is `render.DEPENDENCIES`: native reader/facts/analytics/
request, configuration validation/request/codec/curves/metrics, account contracts/
request/common/containers/codec/metrics, study contracts/rules and provider data/
manifest helpers, account preparation, auxiliary request/features/model and study
spec validators, plus all report Python/JS/CSS/template files. Source digests
normalize CRLF to LF. Deterministic files contain no clock, host or absolute paths.

Canonical script-safe JSON uses sorted compact ASCII keys/values, finite numbers
and escaped < > &. Embedded bytes exclude LF; exported bytes add exactly one LF
and have a separate file hash. Export representation is limited to 32 MiB; HTML
to 64 MiB. CSP hashes the inline executable script and denies connections, objects,
base changes and forms. Labels are text-escaped, including literal template tokens.

## Verification and limits

Run the three `test_strategy_screen_report*.py` modules through
`tools/run_tests.py --keep-temp --` with external MERLIN_TEST_ROOT; Node is required,
not silently skipped. Relevant preservation includes the five Strategy Screen,
four Configuration Study, three Configuration Report and CLI modules. The sole
assigned historical deselection is the CLI pyarrow import-isolation case.
No full suite or Windows elevation is implied by this renderer.

Real file:// Chrome layout/focus and complete 3,200-setting build/open samples
are separate evidence; Node shims do not certify browser layout. The bounded
synthetic recipe is `tests.pattern_lab._strategy_screen_report_helpers.compact_report_input`
with capacity=True, per_cell=200 and missing=True: 4 instruments × 4 TFs × 200
settings, 6,400 results and 12+10 months. It is not a market experiment.
Owner Firefox visual/focus review remains separate. Engineering tests do not
establish a trading edge.

The bounded correctness follow-up uses stored ordinary/auxiliary compact inputs,
one regenerated legacy preview and one complete capacity build/open per role.
The versioned browser helper measures 20 prescribed gesture epochs after draining
pending work: sum actual chart callback CPU durations once per callback, including
combined base/overlay work and hit testing. Overlay-only gestures never reuse a
previous draw duration. No-redraw epochs report zero with zero callback count.
Raw counts/durations, linear p95, settling wall time and browser/heap memory are
separate evidence; callback CPU is not GPU/compositor completion.

