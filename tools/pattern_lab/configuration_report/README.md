# Comparative configuration reports

Rebuild a standalone comparison from a sealed compact configuration
study, without market packs, auxiliary snapshots, accounts, network or execution.
Existing account, selection and event-study reports retain their own routes.

```python
from tools.pattern_lab.configuration_report import render_configuration_report
html = render_configuration_report('owner', output_root='owner-report',
    annotations='annotations.json', presentation='presentation.json')
```

```text
python -B -m tools.pattern_lab configuration-report --run-root owner --output-root owner-report
python -B -m tools.pattern_lab configuration-public --run-root owner --output-root public
python -B -m tools.pattern_lab configuration-report --run-root public --output-root public-report
```

Open `report.html` with `file://` in installed desktop Chrome/Edge supporting Web
Crypto and `DecompressionStream`. No bundler, framework, CDN, server or new runtime
dependency is used. Missing browser capabilities produce an actionable error.

The source visibility is authoritative. Owner reports start with HO2 hidden on
every load; no hidden financial chunks are decoded. Reveal loads all HO2 table
metrics, independently of annotations or selection, and only displayed curves.
Hide invalidates pending display work, evicts hidden decoded data and resets HO2
stage/sort/time view. It cannot undo human exposure or provide cryptographic
secrecy. Public reports physically omit all HO2 financial blocks. No presentation
option can relabel owner results as public.

## Sidecars and generic metadata

Annotations use accepted `pattern-lab.configuration-annotations.v1` and
metadata-only `normalize_annotations`: external IS annotations, not part of the
study seal. Return/stability/reference roles may coexist; absent annotations
never invent finalists or a reference configuration.

Presentation is a mapping or JSON file (at most 1 MiB), closed schema
`pattern-lab.configuration-report-presentation.v1`, with required
`specificationIdentity` matching the source. Optional `title`/`subtitle` are plain
text up to 512/2048 characters. Optional `parameterLabels` is an ordered list of
unique `{path,label,shortLabel?,unit?}` objects. RFC 6901 pointers, including array
indices and escaped keys, must resolve to a scalar in at least one allocation.
Labels/units are nonempty strings up to 128 characters. `initialSelection` allows
up to twelve unique existing configuration IDs; the default is the first canonical
configuration, never a performance ranking. All validation precedes output writes.

Optional `filterParameters` is an ordered list of unique scalar allocation
pointers. Omitted and empty lists normalize identically to `[]`: only Universe,
Ticker, Timeframe, search and reset appear. Each explicit pointer adds one control,
including deliberately chosen constants, using its label or the generic fallback.
Unknown/non-scalar pointers, duplicates and `/allocationId` or `/timeframeMinutes`
are refused before output. Other `*Id` paths are allowed. `parameterLabels` controls
heading names/order independently and never enables filters. Without labels,
varying scalar paths define heading dimensions. All settings remain searchable.

Choose filters before an experiment and record them in its presentation/protocol.
If the owner supplies no list, the preparing analyst may leave it empty or choose
two or three main declared strategy/grid parameters before examining outcomes;
this requires no extra approval or trials. Two or three is guidance, not a schema
limit. For HA, Reward/risk, HA run and Hold days may be useful when actually present.
The renderer never chooses filters from field names, values or outcomes.

Missing/null/string/numeric/boolean values remain distinct. Details retain full
allocation/provider/policy/features and source/auxiliary bindings. Active visible
stage execution facts load from the native metric member; absent synthetic facts
are not invented. Full instrument IDs remain available and equal contracts from
different venues get distinct labels. Nonstandard minute timeframes are supported.

## Values, interactions and sampling

Financial values and statuses are copied from saved native metrics. Ann.P% is
`annualizedProfitPct`, without financial arithmetic in Python or JavaScript.
Zoom does not change full-stage values. Sorting uses unrounded values, nulls last
in both directions, with canonical tie order. No-observation/no-trade/unavailable/
withheld states differ. Sharpe counts and annualized short-period markers remain.

UTC epoch milliseconds and `valueScale=100` are preserved. Q values divided by
that scale are percent change, not currency. Independent stage accounts start at
zero; curves are never compounded or aggregated into a portfolio. Three-calendar-
day account sampling retains mandatory endpoint/gap points. Segments between
supported points are display guides, not observations or intrabar extrema. Cash
gaps are known flat cash, with explicit markers; head support begins at stage
start and tail extension has `endpoint=flatCashExtension`. Daily price context
breaks at nulls/coverage gaps, never forward-fills. No-observation accounts have
no line or numerical hover value; empty prices are unavailable.

Checkboxes collect configurations; plain rows/chips activate them. Normal Up/Down
browse displayed selected chips. Alt+row browses filtered/sorted rows in single view;
Alt+chip browses matching selected configurations. Escape/exit/repeated Alt/plain activation exits
single view. Shift applies the anchor's check state across a range; Ctrl toggles
all filtered rows. Tri-state header selection preserves outside-filter marks.
Clear/Ctrl+Alt+D clear all marks and chart filters without hiding HO2. More than twelve matching selected
preserves every mark and displays a chart-limit explanation, with single browsing
still available. A small DOM row window bounds renderer memory; all operations
use the complete logical roster, including off-screen rows. Focus-only navigation
patches affected controls; keyboard input fields retain their native behavior.

Chart ticker/timeframe cards filter checkbox membership independently of table
filters: OR within each group, AND between groups. Only cards scroll horizontally;
Finalists +, Profitable, Save filtered and Reset filters remain fixed alongside
them. Finalists + adds every annotated return/stability finalist, excluding
reference-only rows, and exits Single View/manual price focus. Add listed finalists
remains scoped to the table. Neither action invents annotations.

Profitable requires an available, finite, strictly positive saved return in IS
and HO1, plus HO2 when revealed, regardless of the chart stage tab. Unavailable,
no-observation and zero returns fail. Newly revealed metrics resolve before matches
are reconciled; pending data shows loading feedback. Active filters remain
removable when no matches remain. Reset filters preserves membership. Save filtered
replaces membership with every match, including above the twelve-line limit or
in Single View, then clears chart filters/Single View. Order, colors, stage/zoom
and table filters are preserved. Subsequent HO2 reveal does not uncheck saved rows.

Standard USDT-SWAP labels use short symbols, with contract/venue disambiguation;
full instrument IDs remain searchable. Matching selected rows are light green,
with stronger emphasis for committed focus. Hover previews cards/chips and curves
without changing membership, committed focus, table or details. Pointer leave,
geometry changes and actual resize clear preview. A stationary click commits;
movement beyond four pixels pans. Saved-point markers respect gaps and stages.

Account lines are solid; active lines are stronger. Real Price draws one series
per instrument/stage, with a solid dark active price and dashed context prices.
Manual price focus preserves the active account; account activation restores
automatic pairing. Left/Right chooses price only when enabled with choices and
outside both horizontal table regions. Shift+Left/Right pans the focused chart;
wheel/plus/minus zoom, Home/double-click reset. The Y axis is on the right.

Physical Ctrl+Alt shortcuts work across keyboard layouts: E reveals/hides owner
HO2, G toggles details/top, F toggles selected-route Single View, T toggles price,
D clears selection, and Digit1 toggles only the hover table. Editable controls,
composition, AltGraph and the modal retain their guards. Escape closes help first,
otherwise exits Single View or clears preview. Shortcuts help contains focus and
returns it to the invoking control.

## Schemas, determinism and publication

Exactly `report.html` and last-published `report.json` are produced. Output must
be new or empty and disjoint from the source. Temporary HTML is validated before
publication. Handled failures/interrupts remove only this call's temporary/final
files, preserving pre-existing empty directories and unrelated concurrent files.
This is not power-loss/crash atomicity across two files. CLI errors/interrupts have
fixed report-specific JSON/stderr messages without arbitrary owner payloads.

Receipt schema `pattern-lab.configuration-report.v1` records source completion
SHA-256, original source identities/visibility/synthetic marker/counts, generator
version/source hashes, normalized presentation/annotation hashes, HTML size/hash.
Generator hashes normalize UTF-8 CRLF to LF; embedded assets use the same rule.
CSP hashes the exact embedded JS and allows reviewed inline styles, with network,
objects and forms denied. Metadata is escaped as text/JSON, never executable code.
Outputs omit generated timestamps, host names and paths. Relocation/CRLF copies
produce the same bytes. Original historical study attribution is retained.

Bootstrap schema `pattern-lab.configuration-report-bootstrap.v1` contains native
metadata, declared stages/date/month labels, result identity rows, native chunk
index, normalized sidecars and attribution. It defines no competing economic
records. Every native `pattern-lab.configuration-chunk.v1` gzip member is embedded
verbatim once in canonical index order, including metrics. See the
[native schema](../configuration_study/README.md#physical-schema). Assembly uses
one public reader and metadata/index/rows/`raw_member`; no second member decode,
per-result reader calls, compression or projection DTO occurs.

Browser decode checks compressed size/SHA, exact bounded streamed decoded size/
SHA, schemas, scoped identities and array shapes. Corruption cannot leave a stale
successful chart. Duplicate pending requests share a decode; at most two chunks
decode at once, with a 32 MiB declared decoded-byte cache/reservation bound and a
latest-generation loop rather than an unbounded queue. Table fields are retained
separately; native facts remain in bounded active chunks. Resource errors refuse
a selection instead of drawing an arbitrary subset. JS heap/process measurements
are separate from decoded-byte accounting.

Hard bounds: HTML 256 MiB, decoded chunk 8 MiB. The 3,200-configuration/9,600-result
synthetic presentation target is 64 MiB/HTML, 60 s/512 MiB per build, 256 MiB paired
output/scratch, cold/warm open 8/5 s, reveal 3 s, sort/filter and uncached navigation
1 s, cached navigation 250 ms, JS heap 256 MiB and renderer working set 768 MiB.
These are engineering budgets, not CI timing assertions or backtest claims.
At the native 65,534-member ceiling, public `raw_member` performs a linear index
lookup per member; this renderer does not bypass protected reader internals.

Compact verification certifies integrity/consistency, not full execution replay.
Synthetic compact fixtures show an automatic banner. Native runs over artificial
prices have `synthetic=false`; label those explicitly in the presentation subtitle.
Owner visual acceptance is separate from automated browser/cross-host checks.
Loading and actionable failure feedback overlays the existing chart without
reserving a row above the title or moving the workspace. Success hides the status;
readiness remains available through the diagnostic state. The synthetic banner
is independent and remains visible for synthetic sources.
