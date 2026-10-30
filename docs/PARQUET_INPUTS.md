# Parquet inputs in Merlin

Opted-in V2 risk-bracket strategies can bind separate OHLCV sources through
[fixed entry filters](ENTRY_FILTERS.md). Their gap-aware native preparation remains
distinct from strict target continuity. Same-root dependencies share a read session;
saved filter pins and separate location overrides accompany the target pin.

F06 method v2 M>1 consumes native history from the aligned declared source origin
once per preparation; M=1 and historical v1 retain bounded prefixes. Replay and
parent-based manual execution retain the saved origin. All consumed early history
participates in the fingerprint, even where a repair leaves later permissions
unchanged. Version/policy/origin and target-relative bounds are validated offline;
see [Entry Filters](ENTRY_FILTERS.md) for the exact rules.

Backtester V2 accepts prepared market-data packs through Start and Queue. Choose
**Use Parquet**, open a directory containing `manifest.json`, select instruments
and check their timeframes. The directory browser lists manifest instruments,
not individual storage files. CSV and Parquet selections remain separate during
the page session; only the active selection is submitted. V1 requires CSV.

## Selection and coverage

The seven timeframe columns begin at 5m, 15m, 30m, 1h, 2h, 4h and 1D. Edit a label
in place using an integer plus `m`, `h`, `D` or `d`. Enter or blur commits; Escape
restores its committed value. Durations must be distinct positive multiples of
five minutes within the supported timestamp range. Equivalent values canonicalize
to the largest exact day/hour/minute unit. Invalid edits block Add Selected.
Bulk checks affect every row, independently of row highlighting. Add requires a
highlighted instrument and at least one checked timeframe per highlighted row.
Reopening clears row selections/checks, retaining committed column durations.
Added source pairs are independent of later header edits and deduplicate by
resolved pack root, exact venue/contract instrument ID and duration.

**Ticker Sets**, beside Refresh in an opened target pack, replaces highlighted
rows using manifest roles: Main (`trading`), Additional (`research_only`), or All
(including factor and unclassified instruments). Counts come from that pack;
empty actions are disabled. Existing timeframe checks and committed durations
remain unchanged. Choose timeframes and use Add Selected to merge source pairs;
the action never removes previously added sources. CSV browsing, directory
navigation, filter pack selection and relocation do not show Ticker Sets.

Source changes immediately invalidate Grid Preview and schedule a fresh metadata
request. Usable counts belong to the current strategy, admission source, warmup
and optimizer/filter settings; obsolete replies are ignored even during debounce.
Full-enumeration V2 displays an unavailable count until a matching Preview exists,
without treating a retained legacy budget as the candidate population. This does
not prevent valid launches. Fixed retains one configuration and sampled/V1 budget
semantics are unchanged.

Auto-create Set is available for Grid or Fixed WFA with more than one active
source pair. Explicit Off survives source/mode changes and temporary ineligibility.
Filter sources are independent and do not count as target sources. Queue labels
use PQ/PQs and CSV/CSVs, with separate counts for mixed sources and an explicit
unsupported-source label. Failed source messages appear in the existing run-message
area as well as the retained item error; Queue state and set completion rules
remain unchanged.

Start keeps per-source details in the collapsed **Coverage and warmup (UTC)**
section. Metadata coverage is advisory and reads no candle rows. Execution rechecks the
consumed interval. Bars use the fixed UTC epoch grid, including multi-day bars:
3D means 72-hour groups from 1970-01-01 UTC, not calendar or session boundaries.
Start/End select **bar opens inclusively**. Date-only End includes the last
microsecond of that date; explicit midnight End includes the midnight-open bar.
For example, 30m from 10:10 through 11:00 selects 10:30 and 11:00 and requires
base candles through 11:30. A selected multi-day bar can close after End.

### Collection planning for inclusive End

When preparing or extending Merlin packs, collect enough closed base candles to
complete the last included target bar, rather than stopping at the UI End instant.
For duration T and inclusive End E, the required exclusive coverage boundary is
`(floor(E / T) + 1) * T`, using the UTC epoch grid. Take the maximum required
boundary across the intended target timeframes and separately check the data
requirements of enabled filters. Preserve the requested research dates.

For month-boundary End at 00:00 UTC, the owner's standard collection policy is
one additional UTC day, supporting the standard 5m, 15m, 30m, 1h, 2h, 4h and 1D
timeframes. Custom durations, especially multi-day bars, require the formula
above; one day is not a guarantee for arbitrary durations. This is an explicit
collection convention, not an automatic downloader or relaxed coverage check.
Only collect candles that have actually closed; do not fill or truncate missing
data to satisfy the buffer. Previously consumed history must remain unchanged.

Funding/OI snapshots have their own observation, publication, coverage and
staleness contracts. Extend them when a consuming feature requires later data;
do not impose OHLCV continuity or manufacture samples at candle boundaries.
Current S07/F06 use OHLCV and do not require auxiliary snapshot extension.

With Date Filter on, missing bounds use the first/last complete certified bar.
With it off, stale form dates are ignored and all complete certified coverage is
requested. A user-supplied Start requires all N preceding target-timeframe warmup
bars; a metadata-derived Start has zero outer warmup. Nothing silently shortens
a requested prefix. At 1D, 1000 warmup bars require roughly 2.7 preceding years.
Period splitting uses the resolved outer range, including enabled FT/OOS. Internal
stages and WFA windows use their established available-prefix rules.
Calendar-month WFA with Date Filter off anchors Start at the first complete
certified bar from metadata. Existing engine calendar-anchor constraints still
apply; turning the filter off does not change month scheduling.

Every required 5m row and target group, including warmup, must be valid, closed
and continuous. Required gaps, duplicates, off-grid rows, incomplete groups,
unverified quote volume or insufficient coverage reject execution. Unused gaps
outside the consumed interval do not reject it. No interpolation or fill occurs.

## Execution and saved results

`core.data_input` owns strict Merlin admission and the prepared frame contract:
UTC nanosecond index, float64 Open/High/Low/Close/Volume, with quote turnover as
Volume. One coherent provider session prepares the consumed union per source job;
the lock is released before calculation. Grid, selected Slow reruns, fixed/adaptive
WFA, FT/stress workers and OOS reuse RAM inputs. Frames are passed explicitly and
never serialized into Queue/configuration. Parquet WFA does not round-trip through
CSV. Existing CSV parsing and CSV WFA buffer transport remain unchanged.

New V2 Optimize/WFA remain Grid-only. Existing V2 DSR rejection remains in force;
this integration does not add DSR engine support. Manual tests, trial/test equity,
Start Backtest and its download, and applicable IS/FT/OOS/manual/WFA trade exports
use source-aware preparation. Legacy V1 Lancelot remains CSV-only. No new engine,
strategy, funding cashflow, selector or Preset support is added by Parquet transport.

Each study stores a versioned `config_json.data_input` envelope containing the
source descriptor, requested/resolved/consumed bounds, bound origins, warmup origin,
preparation policy, consumed input fingerprint/version and relevant instrument facts.
The descriptor is `{type: "parquet", version: 1, pack_root: <absolute root>,
instrument_id: <exact ID>, timeframe_minutes: <integer>}`. It is outside strategy
parameters and candidate axes. Parquet CSV path/name columns are empty; existing
JSON fields suffice without a database migration. Manual tests store their own pin
under `results_json.config.data_input`, retaining the existing results list shape.

New successful Optimize OOS results also store `config_json.parquet_oos_replay`:
`version: 1`, exact full UTC `start`/`end` timestamps (including subsecond precision),
inclusive first/last prepared bar opens `preparation_start`/`preparation_end`, and
nonnegative integer `trade_start_idx`/`warmup_bars`. These facts come from the actual
OOS stage preparation and are saved atomically with its trial results. The separate
input pin, root override and other configuration keys are preserved.

OOS chart/trade reconstruction requires a saved OOS result, validates this record,
verifies the parent pin and reproduces the recorded frame origin and trading index.
Date-only display labels are not a replay oracle. Older Parquet studies without
the record refuse only OOS reconstruction with an instruction to create a new study;
null, malformed, unsupported or conflicting records also refuse it. Saved tables
and otherwise supported IS/FT/manual/WFA views remain available. No metadata is
backfilled on read.
When pinned reconstruction is refused, Results clears the previous curve and
date labels/diagnostics before showing the error; the saved result table stays
visible. Obsolete replies cannot replace the current selection, including a
selection change while parameter details or response JSON are still loading.

CSV Optimize OOS uses sibling `config_json.csv_oos_replay` version 1 with the same
exact timestamp/preparation/index/warmup fields, saved atomically with successful
trial results. Present malformed/unknown/conflicting facts refuse. When that key
is absent, supported history must retain enabled `fixed_params.dateFilter`, a
full-precision executed IS `end` (naive historical V1 timestamps mean UTC), positive
integer OOS days and, if FT is enabled, positive integer FT days. OOS starts at
IS end plus FT days and ends OOS days later; saved OOS/FT date labels must agree.
Date-only IS end, missing or contradictory history requires a new study. FT
requires enabled FT and a saved trial FT rank; OOS requires enabled OOS and non-null
saved trade/profit results (zero is valid). These checks do not hash CSV content.

Valid exact CSV OOS facts may establish a preparation that originally clipped a
requested start/end to available CSV history. Replay uses the original requested
dates and warmup, then requires the prepared first/last bar and trading index to
match those facts before execution. It does not reject solely because the request
extends outside current coverage. Missing required preparation/warmup or appended
bars inside an originally clipped interval that change that preparation refuse.
This validates preparation boundaries/index, not every interior CSV value.
Without exact facts, legacy OOS and other guarded CSV stages retain conservative
interval-coverage refusal: dates alone cannot distinguish original clipping from
missing history. Invalid present facts never fall back to the legacy path.

Both formats execute with original warmup before projecting the chart's metric
view into the selected stage. Optimize IS dates come from executed post-split
`fixed_params`, while runtime validation/warmup remains authoritative. FT preserves
its original date-only invocation. Charts receive explicit execution capital;
individual charts keep index spacing, normalized stitched curves keep time spacing.

Reconstruction verifies the saved consumed interval, including warmup. Unused
extension/backfill or a physical rewrite is compatible when consumed inputs and
applied facts are identical. Changed used inputs, unsupported policy/version or
corrupt/present-null metadata refuse execution without CSV fallback. A manual
original-source test verifies the parent pin and prepares its new interval under
one session; this intentional two-slice operation may read the full parent history.
Start trade downloads verify the pin returned by the displayed backtest.

**Locate Parquet Pack** reuses the directory picker and verifies the saved input
before persisting `data_input_root_override` beside the unchanged original pin.
CSV relocation cannot overwrite Parquet records. Stored metrics/tables, WFA stitched
equity and Analytics remain readable without the pack. Reconstructed charts and
trade exports need matching inputs. Launch a new study to use changed data.
Stored WFA OOS no-trade windows can return their flat curve or header-only trade
CSV without reading the pack; windows that reconstruct trades still verify it.
Traded WFA OOS requires complete precise actual timestamps. Only legacy undelayed
windows without populated actual/status/delay facts may use precise scheduled
timestamps; date-only labels are insufficient. Combined downloads use these same
windows and fail completely on an invalid required window. No-trade windows still
need valid precise scheduled bounds; an all-no-trade download is header-only offline.

Display/study labels use the exact instrument ID and canonical TF, for example
`OKX_BTC-USDT-SWAP, 1h`. Analytics and trade exports use `OKX:BTC-USDT-SWAP`.
These identities come from saved facts, without pack I/O. Legacy CSV identities
and historical V2 Optuna replay compatibility are retained.

Queue stores requested source facts and resolves/pins current data at execution,
one sequential source job per pair. It does not promise one snapshot for the entire
Queue. Unknown typed and malformed Parquet sources retain their positions/cursor
through persistence and fail visibly at launch; they are never converted to CSV.
A bounded `lastError` survives reload and clears on the next attempt. Restore uses
source types rather than a contradictory form snapshot; mixed/invalid items remain
preserved but cannot be edited as one Start selection.

## Risk-bracket instrument rules

Profiles opting into `execution.entryAdmission="risk_bracket_v1"` require Parquet
at application execution boundaries. During the same locked preparation session,
the existing manifest API validates the selected instrument's rule snapshot.
`PreparedInput.admission_rules` carries immutable effective rules; the version-1
`entry_admission` envelope persists the resolved cap, rules and source snapshot.
The cap defaults to 8 only for a new request, never to repair saved context.

BYBIT linear USDT quantity step/minimum are base units. OKX linear USDT contracts
use `baseStep=lotSz*ctVal`, `baseMinimum=minSz*ctVal`, with finite `ctMult=1` only.
Decimal normalization and exact rational ceiling derive minimum lots without
binary rounding; runtime sizing retains the established IEEE expression. Rules
must remain representable as positive finite floats and exact supported integer
lot counts (up to 2^53). Published minimum notional is nonnegative and finite;
zero never refuses a positive-notional entry. Null means no published rule and
remains semantically distinct from zero. Nonzero values that underflow to float
zero are rejected. Price tick is retained but does not round prices.

New-mode Grid Preview resolves current rules under one serialized provider session
using validated metadata only. It checks the source, allowed root, instrument,
timeframe, cap, pending update and metadata-known unreadable state, without candle
reads, hashes or resampling. Preview describes parameter space; it does not certify
file availability, continuity, date-range coverage or warmup sufficiency. Actual
launches still prepare and fingerprint all consumed candles. No rules cache is used.

Saved replay validates current schema and exact effective decimal semantics:
instrument/venue/contract/currencies, quantity unit, base step/minimum, minimum
lots/notional and OKX multiplier/value. Equivalent decimal spelling is accepted.
Acquisition timestamp/reference, listing time and unused price tick may change
without changing execution semantics. Root relocation and history extension
outside consumed candles retain the existing candle-pin rules. Saved provenance
is preserved. The envelope cap must equal the authoritative saved execution cap.

New manual tests first verify the parent's consumed candles/instrument facts,
then resolve current rules in the same session and save their own envelope.
Changed parent rules therefore permit a new manual test but reject reconstruction
of the old saved execution. Missing/malformed/unknown context blocks reconstruction
for a current admission profile; stored tables remain readable offline. FT/Stress
spawned workers receive frame/rules explicitly and never reopen the pack.

## Quantity-only portfolio rules

Quantity-only portfolios reuse the same instrument validation, exact base-unit
conversion and source snapshot through `PreparedInput.quantity_rules` and a separate
version-1 `quantity_execution` envelope (`mode="instrument_rules_v1"`). This does
not opt a legacy strategy into risk admission or Entry Filters. Preparation uses
the same target read session with filters absent; spawned FT/Stress workers receive
RAM frames/rules only. Quantity-only Preview reads neither metadata nor candles.

Saved quantity replay additionally validates actual database portfolio ownership,
profile, instrument and effective rule semantics. Present-null/unknown context is
invalid; old ordinary records remain ordinary. Manual tests from these owned children
verify the parent's quantity semantics before preparing a new interval. Changed
relevant rules refuse reconstruction; append-only history outside consumed bounds
and equivalent decimal spellings remain compatible. Relocation changes only the
locator; offline tables do not require a pack. See [Ticker portfolios](TICKER_PORTFOLIOS.md).

## Locations, locks and explicit collection

Generic test/WFA metric callers now use execution initial capital for CSV and
Parquet alike. Exact OOS preparation metadata, pin/transaction rules and replay
refusals are unchanged. Saved historical values remain untouched; fresh manual
tests can still compare against affected old Forward Test metrics. A fresh full
study is needed for corrected selection throughout; see
[metric history](METRICS.md#execution-capital-and-historical-results).

Locate Pack requires a nonblank string `packRoot`. Malformed or missing roots
return 400 before pack IO or writes, after the existing study/test ownership and
missing-pin checks. Valid strings retain their original path semantics; relocation
still verifies the pin and changes only the root override.

The default root is repository-relative `data/parquet-data/`, ignored by Git.
Merlin does not create a fake pack or download when it is absent. Other roots must
be inside the existing configured CSV allowed roots or this resolved default root.
Resolved containment checks apply to browse, execution and relocation, including
symlink/traversal aliases. Ordinary folders are navigable without provider calls
or `.pack-lock` creation. Packs require writable lock-file access even for reads.
Permission errors do not authorize changing permissions.

Merlin serializes its own readers per resolved root with bounded, cancellable
local waiting. Other processes retain the provider's fail-fast exclusive OS lock.
Pending collector transactions require explicit recovery or abort before use.
Use separate physical Merlin and Pattern Lab roots; no automatic synchronization
or shared-reader locking is introduced.

The [shared provider](../src/data_provider/README.md) and
[pack contract](../tools/pattern_lab/README.md#pack-layout) own the existing format:
`manifest.json`, generated `README.md`, `updates.jsonl`, `ohlcv/` and lock/recovery
files. The pack README is generated metadata, not a separately maintained schema.
Shared reader algorithms, fingerprints and source-attribution generations are
unchanged by Merlin integration. Merlin never imports Pattern Lab.

Collection is a separate explicit operational action. From a configured Python
environment with the checkout's `src` on the import path, choose an owner-managed
absolute root, explicit roster file and UTC interval before invoking:

```python
from pathlib import Path
from data_provider import collect

# Example only: these calls download data and are not application startup/tests.
root = Path("/absolute/merlin-market-pack")
roster = Path("/absolute/explicit-roster.json")
collect.collect_pack(root, start="2024-01-01T00:00:00Z",
                     end="2024-07-01T00:00:00Z", roster_path=roster)
collect.update_pack(root, end="2024-08-01T00:00:00Z")
```

Updates retain the pack's roster. See the provider/pack guide for roster schema,
options, evidence, `recover_pack` and `abort_update`. Do not edit generated metadata
or copy arbitrary files into a live pack to resolve an error. No downloader UI or
automatic acquisition is part of Merlin execution.

## Ticker portfolio slices

[Ticker portfolios](TICKER_PORTFOLIOS.md) rank each pool member over prior complete
months with per-month 99% complete ranking-bar coverage. A verified later listing
means ineligible; unexplained missing history fails preparation. Execution admission
remains stricter and covers only each funded account-period's actual IS/FT/OOS/Fixed
slice plus warmup and independent filter dependencies. Unconsumed execution gaps do
not require preparing the whole ticker history. All funded inputs are admitted before
numerical work; optional comparison defects are recorded separately.

Ranking pins/listing evidence and execution/filter/rule pins are separate. Account
replay validates its consumed histories, accepts unrelated appended data, and never
re-ranks or opens unrelated ranking inputs. Saved Analytics/Results displays offline.
The current live-instrument pool may introduce survivorship bias; no collector or
storage/locking policy changes accompany this application feature.
