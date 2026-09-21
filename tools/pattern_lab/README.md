# Pattern Lab data foundation, event studies and matched comparisons

Pattern Lab is local, research-only tooling. These milestones are implemented:

- **M1a, the data boundary**: a stable Parquet pack, an explicit manifest, a
  one-way importer for the historical prototype NPZ pack, and an interval reader
  with reproducible research input identity.
- **M1b, the collector**: closed-bar 5m collection from public exchange APIs,
  recoverable updates with overlap conflict reporting, cooperative process
  exclusion, and collector/instrument-rule provenance.
- **M2a, the sequential event study**: a versioned study request and protocol,
  extensible feature/hypothesis/model/metric contracts, fixed-horizon and path
  outcomes, immutable per-instrument evidence and a regenerable offline HTML
  report. See [Event studies](#event-studies).
- **M2b block A, enforced boundary contracts**: custom-model evidence admitted
  before publication and revalidated on every read, verified completion in both
  reader modes, one semantic validation path for every accepted request form,
  used-source attribution, resolved dependency warmup, truthful failure and
  partial-progress accounting, and bounded per-instrument evidence reuse.
- **M2b block B, the bounded spawn pool**: the same top-level instrument job run
  under an explicit `spawn` pool with a bounded backlog, coordinator-only pack
  reads and publication, pinned child thread counts, and canonical evidence
  identical to a direct `workers=1` run. See
  [Workers and the bounded spawn pool](#workers-and-the-bounded-spawn-pool).
- **M3a, matched comparisons and calibrated inference**: offline analysis of a
  **completed** study — matched control populations, event-weighted point
  estimates, one joint calendar block bootstrap and one declared Holm family,
  sealed into its own artifact with a standalone offline report. See
  [Matched comparisons and calibrated inference](#matched-comparisons-and-calibrated-inference).

**M3a is implemented; M3b and M4 are not.** M3b owns external-series and panel
feature context and a frozen-candidate validation path; M4 owns sequential
bracket execution with sizing, leverage and expiry. The M2 event study itself
stays descriptive: it reports no p-value, confidence interval, significance
badge, matched control or edge verdict, and those live only in an M3a analysis
artifact.

**M3a inference is an approximate development screen, and its delivered
calibration DID NOT MEET the declared empirical error envelope.** On the tracked
fixtures with persistent daily signal states, rejection and nominal-95%
noncoverage reached approximately **7.5-8.3%** against nominal 5%. The cause is
still under investigation; these inferential outputs remain unvalidated and were
anti-conservative on those fixtures. The implementation is delivered and
verified; its statistical acceptance gate is **open pending tech-lead review**.
Until that is resolved, treat every M3a p-value, interval and Holm rejection as
an unvalidated, anti-conservative screening hint — never as evidence of an edge. See
[Calibration](#calibration).

Merlin may not import Pattern Lab. Pattern Lab reads market data and writes only
to an explicit output directory; it never touches Merlin databases, Queue state,
Presets, baselines or Strategy Lab artifacts. Network access happens **only**
inside an explicit `collect`, `update` or `recover` operation, never on import,
`--help`, `inspect`, `slice`, `study` or `report`. Only unauthenticated public REST endpoints are
used: there are no credentials, websockets, orders, proxy rotation and no silent
fallback to another venue.

**Code availability is not an operationally prepared pack.** These commands and
their synthetic verification are complete; populating a real market-data root is
a separate, explicitly authorized operational run, and running a study on real
data is another.

## Dependency setup

The package requires the pinned root dependency `pyarrow==22.0.0`. Install it into
the configured project interpreter; Pattern Lab never installs anything itself:

```bash
python -m pip install --only-binary=:all: --no-deps --no-cache-dir pyarrow==22.0.0
python -m pip check
```

```powershell
& C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pip install --only-binary=:all: --no-deps --no-cache-dir pyarrow==22.0.0
```

Importing `tools.pattern_lab` and its modules works without PyArrow; every data
command then fails with an actionable dependency message and a nonzero status.

## Pack layout

```text
<data-root>/
  .pack-lock                       persistent cooperative exclusion file
  manifest.json                    authoritative metadata
  README.md                        rendered from the manifest; never edited independently
  updates.jsonl                    publication/update history
  ohlcv/
    OKX_LINK-USDT-SWAP_5m.parquet
  .update-in-progress.json         operation journal, only while one is pending
  .pack-staging-<operation-id>/    that operation's staged replacements
```

`.pack-lock` is created once and then stays in place forever, including after an
aborted initial collect. It is never unlinked or recreated to release the guard.
The journal and staging directory exist only while an operation is pending.

One file per exact venue/contract, with no dates in file names. The instrument ID
is `<VENUE>_<CONTRACT>`, not the ambiguous symbol. Each raw component must be
ASCII and match `^[A-Z0-9][A-Z0-9.-]*$` after uppercasing; `_` is reserved for the
delimiter and whitespace is invalid. Manifest paths are relative POSIX paths under
`ohlcv/`; absolute paths, `..`, backslashes, duplicates and resolved symlink
escapes are rejected on every platform.

## Parquet schema v1

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `timestamp` | `timestamp[ms, UTC]`, non-null | Candle open time |
| `open`, `high`, `low`, `close` | `float64`, non-null | Real OHLC prices |
| `volume_quote` | `float64`, non-null | True quote turnover in the declared quote currency |

Files are Zstandard-compressed with bounded row groups (8192 rows) and explicit
types; no implicit pandas index column is written. Only 5m storage exists in v1.

Rows have strictly increasing unique timestamps on the UTC 5m grid. Prices are
finite and positive, `low <= min(open, close) <= max(open, close) <= high`, and
volume is finite and nonnegative. Signed-zero volume is normalized to `+0.0` on
write and when encoding fingerprints. Zero volume is allowed; empty instruments
are not. Invalid input is rejected: values are never interpolated, deduplicated,
clamped, or derived from base volume times close.

The writer emits exactly these six columns. Extra columns are tolerated on the
**reader** side only: they are ignored, and they never become a dependency.
These snake_case data fields are not public strategy parameters, so Merlin's
camelCase strategy-parameter rule does not rename them.

## Manifest

UTF-8 strict JSON (`allow_nan=False`, duplicate keys rejected), `schema_version=1`.
An unknown schema version fails clearly. Unknown extra metadata is retained and
ignored; required fields are never inferred from their absence.

Validation returns a normalized copy and never mutates the supplied document: an
omitted optional `universe.notes` becomes an explicit null, and accepted
`first_open_utc`, `last_open_utc`, `coverage_end_utc` and a non-null
`verification.closed_before_utc` are canonicalized to the `Z` representation, so
an equivalent explicit offset in a supplied manifest still renders and verifies.
`base_timeframe_minutes` must be the integer `5`; `5.0`, `True` and `"5"` fail.

Top level: `revision` (positive integer), `state` (`ready` or `incomplete`),
`generated_utc`, `base_timeframe_minutes=5`, `volume_unit="quote_turnover"`,
`universe` and `instruments`.

`universe` records `selection_source`, `selection_date`, `historical_membership`
(`verified` or `unknown`) and `notes`. A selection date without a source is
rejected: a pack generation date is not a universe selection date.

Each `instruments` entry carries `instrument_id`, `symbol`, `venue`, `contract`,
`quote_currency`, `roles`, `file`, `row_count`, `first_open_utc`, `last_open_utc`,
`coverage_end_utc` (exclusive), `missing_bar_count`, the file `sha256`, plus
`source` and `verification` objects and an optional `instrument_rules` metadata
object. A collector-managed pack also carries a top-level `collector` object and
requires the versioned `instrument_rules` object on **every** entry; both are
described under [Collector provenance](#collector-provenance-and-instrument-rules).
Instrument rules are metadata, not an execution implementation.

Legal nonempty role sets are exactly `{trading}`, `{research_only}`, `{factor}`
and `{trading, factor}`; roles are serialized sorted. BTC as a factor-only series
has no trading role. File presence never changes trading eligibility.

`source` records `input_format`, `input_dtype`, `source_hash` (nullable),
`source_reference` and `volume_unit_evidence`. `verification` records
`closed_before_utc` (nullable) with `closure_evidence`/`closure_source`, plus
`volume_quote_verified` and `volume_quote_evidence`, and the optional
collector-owned `retained_closure` record described under
[Coverage, closure and gaps](#coverage-closure-and-gaps). A closure cutoff
certifies that retained bars ending at or before it were closed when observed; it
does not certify a gap-free history. Unknown evidence stays unknown.

Publishers never emit `state=incomplete` or `volume_quote_verified=false`:
a failed publication leaves no manifest, and unproven quote units are rejected.
The validator and reader recognize both states for future writers.

The pack's own generated `README.md` renders a coverage table plus one evidence
block per instrument: the quote-volume assertion and its evidence, the declared
volume unit and currency, the evidence source when recorded, the input format and
dtype, the source reference and source digest when present, the closure cutoff
with its evidence and source (or an explicit "unknown" stating that research reads
are refused), any retained previous certification, and the remaining research
limitations. Absent provenance is left
out rather than invented. That evidence is the assertion of the operator who
published or imported the pack: the legacy NPZ importer performs no exchange
verification of its own, while the collector records what it actually observed.
A collector-managed pack's README adds a section naming the managed start, the
frozen last request, the roster digest, the publishing operation, and each
instrument's actual coverage end with its tail shortfall in 5m bars.

## Collector provenance and instrument rules

A collector-managed pack carries this closed `collector` object. Its presence is
what makes a pack managed; archival manifests without it stay valid and unmanaged,
and an `update` refuses them rather than silently adopting them.

| Key | Type and contract |
| --- | --- |
| `schema_version` | integer `1`; an unknown collector version fails |
| `roster` | nonempty list of the six-key roster entries, sorted by `instrument_id`, roles sorted; must match the published instrument identities and roles exactly |
| `roster_sha256` | SHA-256 of the canonical roster bytes: UTF-8 `json.dumps(roster, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)` with no trailing newline. It excludes the universe object, dates, paths and physical file formatting, so it is **not** the roster file's own checksum |
| `managed_start_utc` | canonical 5m-aligned UTC start equal to every instrument's first row; a prefix update may only decrease it |
| `last_request` | closed object `{start_utc, end_utc}`, canonical, aligned and nonempty, with `start_utc == managed_start_utc`; the `latest-closed` token never appears here |
| `operation_id` | the operation that last published semantic changes |

A no-op leaves `operation_id`, `last_request` and the revision untouched. A new
publication keeps the frozen requested end even when actual tails fall short:
actual coverage lives in each instrument entry and is never inferred from
`last_request`.

Every collector-managed instrument entry carries this closed `instrument_rules`
object. Unmanaged archival entries keep their existing opaque-mapping behavior.

| Key | Type and contract |
| --- | --- |
| `schema_version` | integer `1` |
| `source_reference` | the public instrument endpoint this was read from |
| `as_of_utc` | the actual metadata observation time |
| `contract_type` | literal `linear_perpetual`, only after source identity verification |
| `base_currency`, `quote_currency`, `settlement_currency` | source-verified currency codes; quote and settlement are `USDT`. The base is read from the source (OKX `ctValCcy`, Bybit `baseCoin`), never inferred from the display symbol |
| `quantity_unit` | `contracts` (OKX) or the base currency (Bybit) |
| `quantity_step`, `minimum_quantity`, `price_tick` | positive finite decimal strings preserving the source spelling, validated with `Decimal` |
| `minimum_notional` | nonnegative finite decimal string, or null when the source does not publish one |
| `listed_at_utc` | canonical UTC instant, or null when unavailable; a listing need not sit on the 5m grid |
| `trading_status` | the original validated source status: `live` (OKX) or `Trading` (Bybit) |
| `raw_contract_fields` | the source-specific closed object below; values stay strings, and an explicitly absent optional value is null |

OKX raw keys are `instType`, `ctType`, `settleCcy`, `ctVal`, `ctValCcy`, `ctMult`,
`lotSz`, `minSz`, `tickSz`, `listTime`, `state`. SWAP/linear/USDT identity is
required; `ctVal`/`ctValCcy` establish the base-denominated contract value and an
optional `ctMult` is retained without inventing a multiplier. Spot-only
`baseCcy`/`quoteCcy` fields are not required for a swap. Bybit raw keys are
`contractType`, `baseCoin`, `quoteCoin`, `settleCoin`, `launchTime`, `status`,
`qtyStep`, `minOrderQty`, `minNotionalValue`, `tickSize`, taken from the
instrument and its nested filters.

`raw_contract_fields` is **closed per venue**: exactly those keys, with the
required ones carrying a nonblank source string and only the explicitly nullable
ones (`ctMult`/`listTime`, `launchTime`/`minNotionalValue`) allowed to be null. An
empty mapping fails. Every normalized value must agree with the source field it was
derived from — product and contract type, currencies, trading status, quantity
unit, and the listing instant — and quantities are compared **numerically while
both spellings are preserved**, so a source `"0.100"` matches a normalized `"0.1"`
while a `qtyStep` of `"999"` beside a `quantity_step` of `"0.1"` is rejected. The
base currency is read from `ctValCcy`/`baseCoin` and is never inferred from the
display symbol. A managed pack contains only OKX and Bybit entries; `live` is
required for OKX and `Trading` for Bybit. Required identity or quantity fields are
never guessed.

The `collector.roster` must match the published entries on **all six** roster
fields, not only identities and roles, and `managed_start_utc` must equal every
instrument's first stored row as well as the start of the last effective request.

Only a change to a **relevant** value counts as new rules: a refreshed
`as_of_utc` alone preserves the stored object. M4 owns interpretation and
enforcement, including contract conversion; current metadata are not historical
rules, and stored tick sizes or order minimums enable no extra simulation. This
collector adds no funding, slippage, liquidation or order simulation, and keeps
numerical sizing outside these schemas.

## Coverage, closure and gaps

- Coverage is per instrument. A lagging instrument is never hidden behind a
  nominal pack end, and one instrument never truncates another.
- `missing_bar_policy` is `no_fill_v1`: absent 5m slots stay absent. They are
  reported as gaps and segment breaks, never as fabricated flat candles.
- A research read requires `volume_quote_verified` true and a `closed_before_utc`
  at or after the requested exclusive end. That prefix certification covers warmup
  rows as well as research rows. There is no ignore-verification switch, and a
  later wall clock never promotes unverified rows.
- **A publication never withdraws certification from rows it keeps.** When a
  prefix extension is requested with an end older than the cutoff that already
  certifies the stored tail, the published cutoff is the **maximum justified** of
  the previous and current ends: retained rows keep the certification they had,
  newly fetched rows carry this operation's evidence, and the closure evidence and
  source record both when they differ. The older request is never presented as
  having certified the old tail, and an unverified archival row is never upgraded
  merely because time passed.
- **The retained certification lives in `verification.retained_closure`.**
  `updates.jsonl` records revisions, membership and a per-operation source
  summary; it has never stored a previous generation's per-instrument closure
  evidence, so that is where the retained justification is kept instead. The
  field is optional and collector-owned: `null` when this operation's own fetch
  certifies every stored row, or a flat object with exactly
  `closed_before_utc`, `closure_evidence` and `closure_source` copied from the
  generation that actually published them. Its cutoff must equal the published
  winning cutoff, and its evidence and source must be nonblank; a malformed
  supplied record is rejected rather than repaired. A second older-end extension
  **carries the existing record forward unchanged** rather than nesting a record
  inside a record or concatenating another generation's generated explanation,
  and a rebuild whose own cutoff wins — including equality — sets the field to
  `null` so no obsolete record survives. Entries published before this field
  existed remain valid with it absent; evidence an earlier implementation
  discarded is never invented retroactively, so such a legacy entry keeps only
  the cutoff it published. The record is frozen with the rest of the target
  metadata in the normal staging/applying path, so recovery republishes exactly
  those bytes.
- A collector-managed entry must therefore satisfy
  `closed_before_utc >= coverage_end_utc`; a manifest that would leave stored rows
  uncertified is refused before publication. **Stored coverage is not the
  requested end**: a short requested tail is legitimate and is reported as a tail
  shortfall, not as a certification failure.
- An archival M1a pack may still carry a cutoff earlier than its stored coverage.
  That is exposed honestly as a research limitation naming the uncertified rows,
  and it blocks neither the pack as a whole nor the earlier intervals the cutoff
  does certify. An unknown cutoff remains a blocker, as before.

## Python API

```python
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import manifest as pack_manifest

sources = [
    pack_data.InstrumentSource(
        symbol="LINK", venue="OKX", contract="LINK-USDT-SWAP", quote_currency="USDT",
        roles=["trading"], timestamps=timestamps_ms, ohlcv=ohlcv_array,
        source=pack_manifest.build_source_metadata(
            input_format="npz", input_dtype="float32",
            source_reference="prototype pack 5m/LINK.npz",
            volume_unit_evidence="OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
        ),
        verification=pack_manifest.build_verification(
            volume_quote_verified=True,
            volume_quote_evidence="OKX swap volCcyQuote",
            closed_before_utc="2026-07-01T00:00:00Z",
            closure_evidence="Every retained bar precedes the recorded fetch cutoff.",
            closure_source="Prototype fetch log reviewed at import time",
        ),
    ),
]
pack_data.publish_pack(output_root, sources, universe=pack_manifest.build_universe())

loaded = pack_data.load_slice(
    data_root, "OKX_LINK-USDT-SWAP",
    start="2025-07-01T00:00:00Z", end="2026-07-01T00:00:00Z",
    warmup_start="2025-06-24T00:00:00Z", timeframe_minutes=30,
)
frame = loaded.bars                       # UTC index, float64 OHLCV columns
research = pack_data.research_bars(loaded)  # research interval only
metadata = pack_data.slice_metadata(loaded)
```

The collector operations mirror the commands exactly, and take an injectable
request transport and clock/sleeper so tests drive the real adapters without a
network:

```python
from tools.pattern_lab import collect as pack_collect

pack_collect.collect_pack(
    data_root, start="2025-06-01T00:00:00Z", end="latest-closed",
    roster_path="tools/pattern_lab/configs/universe.json",
    options={"okx_rps": 2, "bybit_rps": 2, "timeout_seconds": 20},
    progress=pack_collect.stderr_progress,
)
pack_collect.update_pack(data_root, end="2026-10-01T00:00:00Z")
pack_collect.update_pack(data_root, end="latest-closed", start="2025-01-01T00:00:00Z")
pack_collect.recover_pack(data_root)
pack_collect.abort_update(data_root)

with pack_data.read_session(data_root) as session:      # one pinned generation
    first = session.load_slice("OKX_LINK-USDT-SWAP", start=..., end=...)
    second = session.load_slice("BYBIT_ENAUSDT", start=..., end=...)
```

The event-study API is documented under [Event studies](#event-studies):
`study.run_study`, `study.load_results`, `study.summarize_results`,
`study.render_report` and `study.regenerate_report`, plus the feature,
hypothesis, model and metric descriptors.

Reusable building blocks, all independently callable:
`validate_series`, `series_from_frame`, `write_ohlcv_file`, `read_ohlcv_rows`,
`parquet_column_names`, `resample_complete_groups`, `fingerprint_header`,
`input_fingerprint`, `prepare_series` (the in-memory resample-and-fingerprint
helper the study coordinator and `load_slice` share), `inspect_pack`,
`verify_instrument_files`, plus the manifest
serializer (`build_manifest`, `validate_manifest`, `read_manifest`,
`write_manifest`), README renderer (`render_readme`), update history
(`build_update_record`, `render_update_line`, `append_update_record`), roster
helpers (`validate_roster_entries`, `canonical_roster_bytes`, `roster_sha256`),
schema validators (`validate_collector`, `validate_instrument_entry`,
`validate_instrument_rules`) and the durable write primitives (`write_text_atomic`, `fsync_path`, `fsync_directory`).

### Process exclusion and read sessions

One coarse, exclusive OS file lock at `<data-root>/.pack-lock` guards **every**
public pack-level operation: `collect`, `update`, `recover`, `abort-update`,
`publish_pack`, `import-npz`, `inspect_pack`, `load_slice` and `read_session`.
Linux uses `flock`; Windows locks one fixed byte range with `msvcrt`. Opening the
file never truncates or replaces it.

- Acquisition is always **nonblocking**: a busy root is reported with exit `3` and
  the rejected caller mutates nothing. There is no wait option and no shared-lock
  mode in this milestone, so **two independent readers conflict deliberately**.
- The guard is process-lifetime only. OS process exit releases it; there is no PID
  lease, no expiry and no lock stealing. A pending journal still blocks reads after
  such an exit, so a crashed writer cannot be mistaken for a finished one.
- Every pack-level operation, **including a read**, needs permission to create or
  open `.pack-lock` inside an existing root. A fully read-only mounted pack is
  therefore unsupported by this cooperative API; the failure names the path and the
  permission requirement rather than bypassing the lock. No write ever goes to the
  root's parent, and a read never creates a missing root.
- `read_session(root)` holds the guard across several related reads so they all see
  one pinned generation. Hold it while loading inputs and release it before a
  lengthy RAM-only computation. **A session's lifetime is exactly its context.**
  It is bound to the live guard it was created with and is deactivated on normal
  and exceptional exit, after which every `load_slice` and `inspect` call on it is
  refused with `expired_read_session`. There is no public constructor that turns a
  path alone into a working unlocked session, so a saved reference can never read a
  root that another guard now owns.
- Public entry points are thin lock-owning wrappers around module-private unlocked
  cores, and only cores call cores: `import-npz` never takes the lock twice through
  `publish_pack`, and `update` never calls a second locked `inspect_pack`. There is
  no public `skip_lock` flag or bypass token.
- The root is resolved to `normcase(str(Path(root).resolve()))` and that identity is
  journalled. Lock, staging and journal symlinks, unsafe paths and resolved escapes
  are rejected. A root cannot be renamed, moved, deleted or replaced while an
  operation or read session is active; a moved **pending** root is refused before any
  write, and the fix is to restore its original location or to collect into a new
  root, never to edit the journal's recorded path. A ready idle pack without a
  journal can move and be used at its new location.
- Local NTFS and Linux filesystems are supported. There is no filesystem detection.

The implemented M2a study coordinator owns this session for the whole run: it
reads each selected instrument's consumed 5m slice once, prepares every requested
timeframe in memory, and then runs its sequential jobs on RAM-only inputs. A
competing reader or writer is therefore reported as busy while a study is running.
The planned M2b pool keeps that boundary — workers receive prepared in-memory
payloads and never open the pack — so **no worker-private read, inherited OS
handle or bypass token exists or is needed**. The earlier prospective statement
that M2's workers would call the private read core themselves is superseded.
**M2a exposes no worker pool**; M2b must implement and test spawn execution,
including parent failure, before any parallel execution is exposed.

### Read and write boundaries

- `publish_pack` creates a **new** directory exclusively. A pre-existing
  destination (even an empty one) and overlapping source/output roots are refused.
  Each instrument's verification metadata is checked before its file is written:
  publication requires `volume_quote_verified` to be the boolean `True`, so a
  missing, false or mistyped flag fails with no ready manifest. Unknown closure
  stays publishable as archival provenance.
  Each file is written to a sibling temporary name, closed, re-read, validated and
  then replaced; the README and update record follow, and the `ready` manifest is
  published atomically **last**. A failure leaves no ready manifest and no
  recursive cleanup of caller directories, so partial output survives as evidence.
- `write_ohlcv_file` defaults to refusing replacement. `replace_existing=True`
  exists for a caller-owned file, does not update a ready root's manifest and
  offers no concurrency safety. The new-pack command never enables it.
  The directory is created exclusively, locked immediately and then **rechecked**
  under the lock, so a competing initial collect that created it first can never be
  overwritten by a process that merely got there earlier.
- `write_ohlcv_file` and `write_text_atomic` write to a same-directory temporary,
  flush and `fsync` it **before** the atomic replace, and flush the containing
  directory **after** it. Windows offers no supported directory flush, so that step
  is a documented no-op there and interruption recovery relies on the retained
  journal instead. Tested process-interruption recovery is guaranteed; protection
  from arbitrary disk corruption or from power-loss behavior the host does not
  support is **not** claimed.
- Readers fail on a missing or `incomplete` manifest and on any pending operation,
  including the initial journal temporary. `load_slice` compares the pending state
  and the manifest revision/state before and after the read and fails rather than
  returning a mixed result. That check remains, and is now backed by the exclusion
  lock rather than standing alone.
- A multi-file update is still not one atomic filesystem transaction. What is
  guaranteed is the staging/applying journal contract below: no live file changes
  before every target is ready, and an interrupted operation is recoverable.

### Fixed-range reads

Times are timezone-aware ISO-8601 strings or datetimes with an explicit offset;
naive values and not-a-time are rejected. `warmup_start <= start < end` is
required, an omitted warmup means `warmup_start = start`, and **all three**
boundaries must be aligned to the requested timeframe on the UTC epoch grid. The
timeframe is a positive integer multiple of 5 minutes; booleans, floats,
fractional splits and implicit latest/all-history requests are rejected.

The consumed interval is `[warmup_start, end)`. Range and column filters are
pushed into Parquet, and the logical interval is asserted again so physically
decoded neighbours never reach the frame, the computation or the fingerprint.
Requested outer coverage must exist: insufficient history or end coverage fails
rather than shifting dates, dropping the instrument or shortening warmup.

`resample_complete_groups` validates its input at the public boundary through the
same 5m series rules used for storage, because its count-based algorithm is only
correct once uniqueness, ordering and grid alignment hold. Duplicate, unsorted,
off-grid, fractional or shape-invalid input fails instead of being sorted,
deduplicated, truncated or repaired; a well-formed empty input returns empty
outputs and zero omissions.

A group is aggregated only when it contains exactly the expected unique source
timestamps from its open to its last 5m slot: first open, max high, min low, last
close, summed quote volume. Incomplete groups are omitted and counted; no partial
leading or trailing group is promoted to a full candle. A 5m request returns the
canonical values unchanged. If no complete research bar remains, the read fails
with a coverage error. The reader computes no indicators and no outcomes.

`DataSlice` exposes the sorted bars, the normalized request and resolved warmup
start, `research_mask`/`research_start_index`, `segment_start` (first row true,
and true whenever spacing exceeds the timeframe), `base_row_count`,
`base_gap_count`, `omitted_group_count`, the input fingerprint and a `physical`
provenance record. There is no global validity flag and no forward-looking label.

Every `physical` field is what the manifest **declared**, copied verbatim:
`declared_file_sha256`, `declared_row_count`, `declared_first_open_utc`,
`declared_coverage_end_utc` and `declared_missing_bar_count`. A slice read
deliberately does not hash the file it read, so `declared_file_sha256` is
provenance, not proof that the bytes were checked. Only `inspect --verify`
compares the digest and coverage against the actual files.

## Research input identity versus physical provenance

Keep the two separate:

1. **Physical provenance**: manifest revision, file SHA-256, source evidence and
   the verification cutoff. Appending data or changing writer bytes changes these.
2. **Research input identity**: a versioned SHA-256 over canonical metadata and
   the actual consumed 5m input, including warmup and gaps, before resampling.

Encoding v1 is frozen. The closed header has exactly these keys and JSON types;
integer fields never accept booleans or floats, and there are no nulls, lists or
extra keys:

| Key | Type and value |
| --- | --- |
| `fingerprint_version` | integer, `1` |
| `instrument_id` | string, normalized instrument ID |
| `venue` | string, normalized venue |
| `contract` | string, normalized exact contract |
| `quote_currency` | string, declared quote currency |
| `volume_unit` | string, `quote_turnover` |
| `base_timeframe_minutes` | integer, `5` |
| `timeframe_minutes` | integer, requested timeframe |
| `start_ms` | integer, requested start in UTC epoch milliseconds |
| `end_ms` | integer, exclusive end in UTC epoch milliseconds |
| `warmup_start_ms` | integer, resolved warmup start in UTC epoch milliseconds |
| `resampling_policy` | string, `utc_epoch_complete_v1` |
| `missing_bar_policy` | string, `no_fill_v1` |

Reference encoder. It assumes an already validated v1 header and 5m series —
`input_fingerprint` performs those checks itself — and it copies the OHLCV array
so a caller's data is never rewritten:

```python
import hashlib, json, struct
import numpy as np

payload = json.dumps(
    header, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
).encode("utf-8")
rows = np.array(ohlcv, dtype="<f8", order="C")            # (N, 5), interleaved by row; always a copy
rows[:, 4][rows[:, 4] == 0.0] = 0.0                       # normalize signed zero on the copy
digest = hashlib.sha256()
digest.update(struct.pack("<Q", len(payload)))            # header byte length
digest.update(payload)                                    # header bytes
digest.update(struct.pack("<Q", len(timestamps)))         # raw consumed row count
digest.update(np.ascontiguousarray(timestamps, dtype="<i8").tobytes())
digest.update(rows.tobytes())
fingerprint = digest.hexdigest()                          # 64 lowercase hex, no prefix
```

`input_fingerprint` validates before hashing: the header must be exactly the
closed 13-key v1 mapping above with the documented JSON types, the frozen
constants, canonical venue/contract/instrument identity (the ID must equal
`<VENUE>_<CONTRACT>`), a supported timeframe and
`warmup_start_ms <= start_ms < end_ms` with all three boundaries aligned to that
timeframe. The raw arrays pass the same 5m series rules as storage, and nonempty
rows must lie inside `[warmup_start_ms, end_ms)` — outside rows are invalid input,
not silently filtered. Gaps and incomplete groups remain valid, and a well-formed
empty input (a 1-D empty timestamp array with a `(0, 5)` OHLCV array) is accepted.
Validation never mutates the caller's header or arrays.

Timestamps encode missing slots, so raw rows are hashed even when an incomplete
aggregate was omitted: repairing that slot later must change identity.
Verification is an admission check; neither its true flag nor the growing closure
cutoff is hashed. Absolute paths, mtimes, full-file digests, pack generation
date/revision, unrelated instruments or columns, environment versions, roles and
universe membership are all excluded — adding a factor role does not alter the
candles. M2 composes the per-series data fingerprint with the selected universe,
roles and settings into run identity.

The golden vector pinned in `tests/pattern_lab/test_pattern_lab_identity.py` uses
`TEST_AAA-USDT` / `TEST` / `AAA-USDT` / `USDT`, `start_ms=warmup_start_ms=0`,
`end_ms=600000`, `timeframe_minutes=5`, raw timestamps `[0, 300000]` and OHLCV
rows `[[10, 12, 9, 11, 100], [11, 13, 10, 12, 0]]`. Its canonical header is 325
bytes, the total payload is 437 bytes, and the digest is
`260319787a122516ce732bf93bf8b5c68aec4881e2d47e8e23911d3c161548b7`.

## Historical NPZ import

The importer is a bounded, one-way archival adapter for the prototype layout:

```text
<source-root>/MANIFEST.json
<source-root>/5m/<SYMBOL>.npz
```

`ts` is a 1-D int64 array of UTC milliseconds; `ohlcv` is `(N, 5)` float32 or
float64 in open/high/low/close/quote-turnover order. Optional zero-dimensional
`ex`/`sym` Unicode scalars are read without pickle and checked against the
manifest. Archives are opened with `allow_pickle=False`; no prototype module is
ever imported or executed. Shapes, timestamps, values, manifest counts/ranges and
source SHA-256 digests are verified. Stable timestamp sorting is permitted and
recorded; duplicate timestamps fail. float32 is converted to float64 exactly — no
recovered precision is claimed. Legacy roles map `trading -> trading`,
`research_only -> research_only`, `factor -> factor`. Old fractional splits,
gap-fill policy, free-text USD labels and untouched-holdout claims are not copied
into the new contract.

The prototype manifest carries no accepted structured quote-unit evidence, and its
free-text USD label is not USDT evidence. Every import therefore requires
`--source-metadata`, whose closed schema is:

```json
{
  "schema_version": 1,
  "instruments": {
    "BYBIT_ENAUSDT": {
      "quote_currency": "USDT",
      "volume_unit_evidence": "Linear ENAUSDT turnover; fetch_base.py reads kline field 6.",
      "evidence_source": "Source review plus the Bybit V5 kline field definition",
      "closed_before_utc": null,
      "closure_evidence": null,
      "closure_source": null
    },
    "OKX_LINK-USDT-SWAP": {
      "quote_currency": "USDT",
      "volume_unit_evidence": "OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
      "evidence_source": "Source review plus the OKX candle field definition"
    }
  }
}
```

Top-level keys are exactly `schema_version` and `instruments`. **Every** source
series needs an entry: the instrument keys must match the normalized
`<VENUE>_<CONTRACT>` identifiers derived from the source manifest exactly, so a
full import of the 54-series prototype pack needs all 54 keys. Required entry
fields are `quote_currency` (uppercase ASCII code), `volume_unit_evidence` and
`evidence_source`. Closure fields may be omitted or all null; a non-null
`closed_before_utc` requires both nonblank closure fields and an aware,
UTC-normalizable, 5m-aligned timestamp. Partial closure evidence, unknown keys,
wrong types, duplicate JSON keys, missing or extra instrument IDs and unknown
versions are rejected. The example describes evidence; it is not a closure proof.

Missing closure evidence does not prevent archival conversion: all validated rows
are preserved with `closed_before_utc = null`. Such a pack is structurally ready
but not verified for research reads, and every read of those instruments is
refused. No last-bar trimming, shared shortest-series cutoff, or inference that an
old candle must have closed because today is later. Re-collect an archival pack
into a separate managed root to obtain verified closure evidence; `update` never
adopts an imported pack in place.

## Roster configuration

`tools/pattern_lab/configs/universe.json` is the tracked roster: **configuration,
not market data**. It preserves 54 exact venue/contract entries (44 `trading`,
9 `research_only`, one factor-only BTC). Its closed shape is `schema_version=1`,
`universe` and `instruments`; each entry requires `instrument_id`, `symbol`,
`venue`, `contract`, `quote_currency` and `roles` with consistent normalized
identity. v1 allows only OKX USDT linear SWAP (`<BASE>-USDT-SWAP`) and Bybit USDT
linear perpetual (`<BASE>USDT`) contracts. Duplicates, unsafe identifiers,
unsupported products or currencies, unsorted entries and unknown keys are rejected.

Custom explicit rosters are supported; pass any validated file to `--universe`.
No count is hardcoded and membership is never inferred from directory contents.
Dates, paths and HTTP options are **not** part of the roster.

Its canonical semantic digest, `collector.roster_sha256`, is
`bdf28da2fdecc83df9c70791741de75e33f1eb12d95ebb1454018497591b8beb`. That is the
hash of the roster's semantic bytes, not of the configuration file itself
(`63515750970411ec22b1404220707631cc205fa95f387b8611b3d3d3ef1d1e22`).

## Source adapters and closure evidence

| Venue | Candle request | Accepted fields and pagination |
| --- | --- | --- |
| OKX | `GET /api/v5/market/history-candles`, exact `instId`, `bar=5m`, `limit=300` | Array indices 0..4 are timestamp/OHLC, index 7 is `volCcyQuote`, index 8 is the completion flag. Only rows with flag `"1"` are retained. Paged backwards with `after`, advancing to the oldest timestamp |
| Bybit | `GET /v5/market/kline`, `category=linear`, exact `symbol`, `interval=5`, `limit=1000` | Array indices 0..4 are timestamp/OHLC, index 6 is `turnover`. Paged backwards with `end = oldest_timestamp - 1`, filtered to the fixed requested interval |

Instrument metadata comes from `GET /api/v5/public/instruments` (`instType=SWAP`)
and `GET /v5/market/instruments-info` (`category=linear`); server clocks come from
`GET /api/v5/public/time` and `GET /v5/market/time`.

**One frozen end per operation.** At operation start both required venues' server
clocks are sampled and the end is frozen at
`floor((min(server_times_ms) - 60000) / 300000) * 300000`. The 60-second lag is
our conservative publication-latency allowance, **not** an exchange guarantee.
The samples, the observed time and the resolved end are recorded. A failed server
time request blocks the operation, and recovering a staged operation preserves its
original end. `latest-closed` is a collector-only convenience resolved once to a
concrete end; it never enters `load_slice` or a frozen research configuration, and
an explicit end beyond the safe cutoff **fails rather than being shortened**.

Both adapters retain only rows whose open plus 5m is at or before the resolved end.
For Bybit the fresh fetch after the recorded server cutoff is the time-based
closure basis; it has no per-row completion flag. OKX additionally requires its
flag. An old imported row is never upgraded merely because the clock advanced.

Numeric strings are parsed directly to float64 and validated by the same rules as
storage: no float32 intermediate, no quote approximation, no mark or index-price
candles. Page responses may descend in time and overlap; downloaded rows are
sorted stably, identical duplicates collapse and conflicting duplicates fail.
Cursor progress must be strictly backward: a repeated or non-progressing page is a
protocol error, never a silently completed interval. A short nonempty page is
**never** an exhaustion condition; only a successful empty response or reaching the
requested lower boundary ends the walk.

### Preflight

Before any bulk paging, identity, rules, status and listing metadata are fetched
for **all** roster entries and every explicit failure is collected. An available
supported live contract is required (OKX `live`, Bybit `Trading`), and a known
listing after the requested managed history start is rejected. An unknown listing
time is recorded as null, never treated as proof of availability.

Metadata alone cannot prove candle retention depth, so an initial collection and
an earlier-start extension also make **one bounded start-boundary probe** per
otherwise eligible instrument through the same adapter: one page ending just after
the requested first slot, which must contain that slot. Years of history are never
walked for this check, and an ordinary append needs no historical probe because its
start is already verified in the pack. All detected preflight failures are reported
before any bulk download; final merged coverage is still checked afterwards. Probe
success is not a promise that a later request cannot fail. There is no separate
preflight command and no cache.

### HTTP policy

TLS verification stays on. Each request has a finite timeout (20s by default) and
conservative per-venue pacing (`--okx-rps` / `--bybit-rps`, default 2
requests/second each, counting retries and metadata requests, with a documented
ceiling of 10). The effective pacing values are frozen in the journal and reused by
`recover`. There is no adaptive scheduler.

**One bounded attempt budget per request.** Transport failures, HTTP status codes
and HTTP-200 venue business codes are classified by the same code and share the
same budget: a maximum of **five attempts** in total, with simple bounded
exponential backoff and `Retry-After` honored within a bounded wait budget of 60
seconds. There is no nested retry loop, so a transient venue code costs one extra
request, not another five. The attempt count is an integer in `[1, 5]`, enforced
(never clamped) at the exposed options, at the client boundary and again when a
journal's frozen options are restored; booleans, non-integers and out-of-range
values are rejected.

Only documented transient public-REST conditions are retried:

| Class | Retried | Not retried |
| --- | --- | --- |
| Transport | a timeout, or a temporary connection/DNS failure | a TLS/certificate failure or a transport configuration error, which fail on the first response |
| HTTP status | 429 and 5xx | every other non-200 status, including 4xx access restrictions |
| OKX business code | `50004` endpoint request timeout, `50011` rate limit, `50013` systems busy, `50026` system error | everything else, including `50113`, which is an invalid-signature configuration error rather than a throttle |
| Bybit business code | `10000` server timeout, `10006` too many visits, `10016` server error | everything else, including `10002` (request time window) and the WebSocket-only `10429`; HTTP 429 is already classified from the status line |

**One documented status-line exception.** OKX publishes `50004` ("API endpoint
request timeout") with **HTTP 400**, a status this policy otherwise treats as
permanent. The OKX adapter alone parses a 400 body for exactly that code in a
valid OKX error envelope and retries it inside the same attempt budget; the same
code inside an HTTP-200 envelope is classified by the ordinary business-code
allowlist. A malformed 400 body, any other code, any other status, and another
venue's body quoting the same digits all keep failing on the first response: the
classification follows the adapter that made the request, never a string match.
The retried codes are the ones each venue documents for these public read-only
GET operations; no order-placement retry semantics, endpoint registry or SDK is
involved. OKX `50001` is documented at HTTP 503 and is therefore already retried
from the status line, and `50013`/`50026` are documented at HTTP 429/500, so
their presence in the HTTP-200 allowlist is defensive rather than load-bearing.
Bybit `10018` (IP rate limit) is **struck through in the current official
table**: its bounded handling is retained as legacy compatibility and is labelled
as such in `BYBIT_LEGACY_RETRYABLE_CODES`, not claimed as a current required API
condition.

Invalid symbols, invalid parameters and access restrictions therefore fail on
their first response and are never treated as empty history. API success codes are
checked even on HTTP 200. Malformed JSON — including a bare JSON `null` — is
reported as a clear source/protocol error naming what arrived. An exhausted budget
reports the final underlying error together with the exact number of attempts
made, so a diagnostic never claims retries that did not happen.

**First-load timing is an estimate, not a benchmark.** 470 days at 5m is about
135,360 rows per instrument: roughly 22,100 OKX pages at 300 rows and 680 Bybit
pages at 1000. At 2 requests/second that is about 3.2 hours of pacing alone, before
network latency, retries and metadata. A 30-day update is about 13 minutes of pacing
alone. Effective smaller pages increase these estimates. Bounded instrument and page
progress is logged to stderr so a multi-hour job is visibly progressing.

## Update, merge and no-op semantics

Before updating, the existing managed manifest is validated and every declared
file's integrity is verified under the operation lock. An update refuses a file
carrying unknown extra OHLCV columns, because the six-column writer would silently
discard them; **reading** such a file remains supported.

The new tail is fetched with one hour (12 base bars) of overlap. An earlier
requested start also fetches the missing prefix plus one hour inside existing
coverage, and overlapping requests are coalesced. The effective start is
`min(previous managed start, any requested earlier start)`; a later start never
trims stored data and an older end never truncates the tail. Both are simply
already covered, and no inverted fetch range is manufactured.

Duplicate timestamps are compared by all five canonical float64 values with signed
zero normalized: **no tolerance, no dedup-last, no averaging**. Identical overlaps
preserve the existing row. Newly observed timestamps may be added, including holes
inside the explicitly fetched ranges; inserted historical slots are reported
separately because they legitimately change affected slice fingerprints.

Any differing existing row is a **conflict**. The instrument, timestamp and old/new
values are reported in a bounded sample with a total count, every live file is left
intact, and the whole publication is blocked. There is no force or
overwrite-history switch: an intentional historical-value repair needs its own
reviewed operation. The error says to abort the staging operation to unblock the
unchanged pack, then investigate the source discrepancy, and — if it persists — to
collect a separate new root or wait for a reviewed repair operation. Deleting the
marker or changing the overlap to conceal a conflict is never advised.

Data outside the fetched ranges is unchanged, and per-instrument coverage and
closure certification are never shrunk. A **semantic no-op** preserves the OHLCV
files, the manifest, the revision, the README and `updates.jsonl` byte for byte; a
new wall-clock fetch time alone is not a reason to rewrite the pack, and the latest
checking time is returned only in the command's own result. Existing rule and
evidence timestamps are preserved when their relevant values did not change.

The requested first slot must exist for every instrument after merging: an empty
series, an unavailable prefix, listing or retention limit, or a source/protocol
error blocks the whole publication. A missing exact first candle has **several
possible causes** that this operation cannot tell apart — a gap at exactly that
slot, candle retention that does not reach back that far, or a later listing than
the metadata reports — so the error names all three rather than guessing. Dates are
never silently shifted, no member is omitted and no venue is substituted. A successfully queried but **short tail** does
not block publication: each actual `last_open_utc`/`coverage_end_utc` is recorded
and `tail_shortfall_bars = max(0, (requested_end_ms - coverage_end_ms) // 300000)`
is reported, with exit status `1` whenever any shortfall exists — including a no-op
with no newly available rows. A structurally ready pack can therefore have a short
requested tail; that is not full research or reserve coverage. Research reads still
reject requests outside an instrument's actual coverage. Internal missing slots stay
gaps, reported separately with counts and bounded range samples; candles are never
fabricated.

## Staging, commit and recovery

`.update-in-progress.json` is the durable operation journal and one
operation-owned staging directory under the same data root holds every completed
replacement. There are no whole-pack snapshots, no general transaction engine and
no backup service. The old ready manifest may remain while staging, but the marker
blocks research reads.

The journal freezes the operation ID and kind, the canonical root identity, the
resolved roster, request and options, `operation_started_utc`, the base
revision/manifest digest (null for a new pack), the old README and history digests,
the target revision, the phase, completed preflight evidence, and one record per
completed staged file with its relative final and staged paths, old digest if any,
new digest, row/coverage facts and source/rules/verification evidence. On entering
`applying` it also fixes the complete target manifest, README and history artifact
digests. The journal is validated before any journal-driven data or metadata write; opening
the lock file is the one necessary coordination exception. Validation checks the
promised relationships, not only container types:

- the closed request, options, closure, preflight and staged-record shapes with
  their scalar types; canonical aligned ranges; agreement between the effective
  request and the frozen closure evidence; roster membership for every staged and
  preflight record; and options that are valid frozen HTTP options;
- **closure evidence** whose clock samples cover exactly the distinct venues of
  the frozen roster — a missing venue is never accepted because the remaining
  samples happen to give the same cutoff — the fixed publication allowance, the
  derived safe cutoff, and `latest-closed` resolving to that cutoff;
- **restored preflight evidence** that is usable as it stands: each record's
  rules pass the same venue-aware managed-rule validator the manifest uses; the
  closed probe object has a boolean `checked`, the effective requested start as
  its aligned `slot_utc` and a nonblank `reason`, with `available` true when
  checked and null when not; `listing_known`, the normalized `listed_at_utc` and
  the rules' own listing agree; and a known listing after the effective requested
  start is rejected. An initial collect requires successful checked probes.
  Preflight is checkpointed for the whole roster at once, so a recorded preflight
  must cover every member, and any completed instrument — or the applying phase —
  requires it; the legitimate state before preflight is an empty mapping with
  nothing completed and no frozen targets. Invalid saved evidence fails with
  `invalid_journal` **before** any network call, staged write or live
  replacement, and is never fabricated or quietly re-run;
- an initial collect at target revision 1 with the required null base facts, and an
  update at exactly base revision + 1;
- deterministic final and staged paths, instrument identity, row/coverage facts and
  old/new digest agreement with the recorded base and the staged manifest entry;
- **derived facts that follow from the saved request and entry**:
  `tail_shortfall_bars == max(0, (request_end_ms - coverage_end_ms) // 300000)`,
  so a stored tail reaching past an older requested end is zero rather than
  negative; `changed` agreeing with the record; `added_rows` equal to
  `prefix_rows + inserted_rows + appended_rows`, with no additions when the data
  are unchanged (a rules-only update is still legitimate, and initial collection
  counts its new rows as prefix rows); `gap_bar_count` equal to the entry's
  `missing_bar_count`, with consistent nonnegative range counts; fetch ranges
  carrying exactly `start_utc`/`end_utc` as aligned nonempty intervals inside the
  effective request, sorted and coalesced, with an empty list valid for an
  already-covered older request; and gap samples carrying exactly
  `from_utc`/`to_utc`/`missing_bars` as aligned nonempty half-open intervals
  inside the stored coverage, sorted, nonoverlapping, counted from the interval
  length, and numbering `min(gap_range_count, 10)`. A truncated sample stays
  valid without summing to the full total, and the positions or counts of
  unsampled gaps are never inferred;
- for the applying phase, the complete expected instrument set and the complete
  frozen metadata targets, with no duplicate or foreign record;
- agreement between the journal and the **frozen target manifest** — revision,
  roster, collector operation and request, entries and artifact digests — read from
  staging or, when the staged file was already consumed by a completed rename,
  from the published destination that carries the recorded new digest.

The whole applicable plan is validated **before the first live replacement**, so a
valid-looking subset is never published ahead of discovering that another planned
target is inconsistent. Commands and arbitrary journal paths are never executed,
and there is no journal signing, corruption repair or pack migration.

1. **staging** — the operation is recorded before any work. Each instrument is
   fetched and merged in RAM, written through the verified writer to its staged
   replacement, validated, flushed, and then checkpointed. Completed artifacts are
   reused by digest on recovery, so only the currently unfinished instrument is
   ever downloaded again. Live OHLCV and authoritative metadata stay unchanged
   throughout, so failed or conflicting work remains recoverable or abortable.
2. **applying** — entered durably only once every required instrument and target
   metadata artifact is ready and verified. Changed final files are replaced
   sequentially, then the target history and README, then the manifest **last**.
   All targets are verified, only recorded staging artifacts are deleted, and the
   marker is removed **last**. Cleanup is idempotent: an already-consumed artifact
   may be absent when its destination matches the recorded target digest.

One further check needs the base manifest and therefore lives at the collector
boundary, where that manifest is already required and verified: when a restored
staging **update** is resumed, an unchecked probe — which records an
already-covered start — is refused for an instrument whose stored history begins
after this operation's effective start. Nothing re-reads a data file to make that
check, and it is not required of the applying phase, where the old generation may
already have been consumed.

Before each replacement the destination must match either the recorded old digest
(the step has not run) or the recorded new digest (it completed before the crash).
A third value is unexpected modification and blocks recovery. Unchanged files are
verified against their target facts before final publication. Recovery never
re-downloads during `applying`; if the target manifest is already published but the
marker remains, the targets are verified and cleanup finishes without creating a
second revision. Exactly one revision and one operation-ID-tagged history event are
published however often recovery runs.

`abort-update` is allowed only while staging. It verifies that live files and
metadata still match the recorded base, removes only that operation's artifacts,
and removes the marker last. For an aborted initial collect it removes
task-created empty subdirectories only while they are still empty and retains the
root and its persistent `.pack-lock`, so the result is a reusable lock-only root. A
caller's root is never recursively deleted.

**Applying cannot be aborted**: finish forward with `recover`. No rollback copy is
needed, because there are no live changes before applying and afterwards the
verified staged targets provide the forward path. **Unrecoverable-loss limit**: if a
required applying-stage artifact is lost and its destination is not already the new
digest, recovery stops. Restore the exact missing staged artifact from an available
copy, or collect a separate new root. Preserve the failed root and its journal;
never clear the marker to make a partially replaced pack readable.

If a crash leaves a completed-but-uncheckpointed staged file, or an uncheckpointed
temporary, it is incomplete work for that operation and is never adopted as a live
instrument. The set of names an operation owns inside its staging directory is
derived from the validated frozen roster and the fixed metadata target plan: each
instrument's `<INSTRUMENT_ID>_5m.parquet`, the three target artifact names, and
their exact temporary conventions. **Abort and cleanup therefore remove a durable
file written just before its journal checkpoint**, while every entry that is not
one of those exact names — an unrelated hidden file, a directory, or a symlink
that merely borrows an owned name — is preserved and reported, never deleted or
followed, and the journal is retained so the operation fails closed. A name only
identifies incomplete owned work for removal and re-download; **only a validated
checkpoint and digest ever permit a staged file to be reused** instead of fetched
again. A caller's directory is never recursed into or recursively deleted. The journal owns its exact
`..update-in-progress.json.tmp` text temporary and its named staging directory, and
Parquet temporaries use the existing `.<file>.tmp-<uuid>` convention inside that
directory only. A crash during the **very first** journal write leaves that
temporary: status reports it, ordinary mutating commands refuse it, and `recover`
promotes it only after full journal and root validation — an initial collection
requires the otherwise lock-only destination, and an update requires the recorded
base manifest, data and metadata to match. A ready old pack is never mistaken for a
new ready generation. A malformed temporary blocks `collect` and is reported for
manual inspection, never silently adopted or deleted.

## Operational recipes

Replace `<data-root>` with the destination you selected; there is no default.

```bash
PL=tools/pattern_lab/configs/universe.json

# First load: a June 2025 buffer before the [2025-07-01, 2026-07-01) research year.
python -m tools.pattern_lab collect --universe "$PL" --data-root <data-root> \
    --start 2025-06-01T00:00:00Z --end latest-closed

# Monthly update to a concrete closed end.
python -m tools.pattern_lab update --data-root <data-root> --end 2026-10-01T00:00:00Z

# Earlier-start backfill: adds missing prefix history only.
python -m tools.pattern_lab update --data-root <data-root> --end latest-closed \
    --start 2025-01-01T00:00:00Z

# Status, including a pending operation without a manifest.
python -m tools.pattern_lab inspect --data-root <data-root>
python -m tools.pattern_lab inspect --data-root <data-root> --verify

# After an interruption: finish forward, or discard a staging operation.
python -m tools.pattern_lab recover --data-root <data-root>
python -m tools.pattern_lab abort-update --data-root <data-root>
```

On a **conflict** (exit `2`, `error_code` `historical_conflict`): run
`abort-update`, then investigate the reported timestamps at the source. If the
discrepancy persists, collect a separate new root or wait for a reviewed repair
operation. Do not delete the marker and do not change the overlap.

**Any failure after the journal was written keeps that journal.** The error
explains the operation ID, its phase, that research reads of the root are refused
until it is resolved, and which of `recover` or `abort-update` applies. Nothing is
deleted automatically, and an apparently empty operation is never auto-aborted,
because a durable staged file can already exist without its checkpoint. A failure
*before* the journal exists — a server-clock failure, for example — leaves no
pending operation and says so.

**An unavailable or delisted roster member blocks the whole all-roster update.**
Preflight requires an available supported live contract for every entry, and the
roster of a managed pack is fixed: `update` cannot add, drop or relabel an
instrument, and no roster-editing command exists. Re-running the same command with
the same roster fails the same way. The bounded workaround is to collect into a
**separate new root** with an explicitly corrected roster, keeping the old root
with its historical revisions and `updates.jsonl` evidence intact.

The June 2025 buffer above is a collection default example, not a universal
indicator warmup guarantee: the APIs take explicit dates and M2 records its actual
warmup independently. The reserved interval `[2026-07-01, 2026-10-01)` is not
authorized for analysis, September 2026 cannot be called complete until its candles
have closed, and re-fetching prototype data does not erase its prior analytical use.

### Bounded public smoke, before the first full download

Run this once, manually, **after code acceptance and before the operational
download**. It is not part of pytest, it authorizes neither a market study nor a
bulk download, and it writes no pack. It requests 1100 closed 5m bars per venue
through the real adapters at production page limits, so OKX (limit 300) needs four
pages and Bybit (limit 1000) needs two: a single page would prove nothing about
pagination.

A thin transport wrapper keeps **one** raw successful candle page per venue from
that same fetch, so the decoded quote turnover can be compared with the documented
raw field directly: OKX array index 7 (`volCcyQuote`) and Bybit index 6
(`turnover`). Only that small sample is retained, and no candle is re-fetched for
the comparison — a second response could legitimately differ.

```bash
python - <<'SMOKE'
import json
import numpy as np
from tools.pattern_lab import exchange_data as ex
from tools.pattern_lab.manifest import BASE_STEP_MS, format_epoch_ms

BARS = 1100


class Capture:
    """Pass every request through, retaining one raw candle page per venue."""

    def __init__(self, venue):
        self.venue = venue
        self.sample, self.pages, self.attempts = None, 0, 0

    def __call__(self, url, timeout):
        response = ex.urllib_transport(url, timeout)
        if "candles" in url or "kline" in url:
            self.attempts += 1
            if response.status != 200:
                return response
            try:
                body = json.loads(response.body)
            except ValueError:
                return response  # the adapter owns malformed-response diagnostics
            if not isinstance(body, dict):
                return response
            rows = None
            if self.venue == "OKX" and body.get("code") == "0":
                rows = body.get("data")
            elif self.venue == "BYBIT" and type(body.get("retCode")) is int and body["retCode"] == 0:
                result = body.get("result")
                rows = result.get("list") if isinstance(result, dict) else None
            if isinstance(rows, list):
                self.pages += 1
                if rows and self.sample is None:
                    self.sample = body
        return response


for adapter, contract, quote_index, rows_of in (
    (ex.OkxAdapter(), "BTC-USDT-SWAP", 7, lambda body: body["data"]),
    (ex.BybitAdapter(), "ENAUSDT", 6, lambda body: body["result"]["list"]),
):
    capture = Capture(adapter.venue)
    client = ex.HttpClient(transport=capture, rates={"OKX": 2.0, "BYBIT": 2.0})
    server_ms = adapter.server_time_ms(client)
    end_ms = ex.safe_closed_cutoff_ms([server_ms])
    start_ms = end_ms - BARS * BASE_STEP_MS
    rules = adapter.instrument_metadata(client, contract)
    stamps, values = adapter.fetch_candles(client, contract, start_ms=start_ms, end_ms=end_ms)
    assert stamps.size == BARS, (contract, stamps.size)                  # exact count
    assert list(stamps) == sorted(stamps), contract                      # ascending order
    assert all(int(s) % BASE_STEP_MS == 0 for s in stamps), contract     # 5m UTC grid
    assert int(stamps[0]) == start_ms and int(stamps[-1]) + BASE_STEP_MS == end_ms
    assert values.dtype.name == "float64" and values.shape == (BARS, 5)
    assert np.isfinite(values).all(), contract                           # no NaN or inf
    assert (values[:, :4] > 0).all()                                     # positive OHLC
    assert (values[:, 2] <= values[:, [0, 3]].min(axis=1)).all()         # low <= min(open, close)
    assert (values[:, [0, 3]].max(axis=1) <= values[:, 1]).all()         # ... <= high
    assert (values[:, 4] >= 0).all()                                     # quote turnover

    # The quote-field check: one retained closed bar from the captured page,
    # compared with the documented raw index of that very response.
    raw = {int(row[0]): row for row in rows_of(capture.sample)}
    matched = [int(s) for s in stamps if int(s) in raw]
    assert matched, contract
    stamp = matched[0]
    decoded = float(values[list(stamps).index(stamp), 4])
    assert float(raw[stamp][quote_index]) == decoded, (contract, raw[stamp])
    print(adapter.venue, contract, "candle pages", capture.pages, "attempts", capture.attempts,
          format_epoch_ms(int(stamps[0])), "->", format_epoch_ms(end_ms),
          "| quote field", format_epoch_ms(stamp), decoded,
          "| server", format_epoch_ms(server_ms), "| unit", rules["quantity_unit"],
          "| step", rules["quantity_step"], "| status", rules["trading_status"],
          "| http requests", client.request_count)
SMOKE
```

The raw-versus-decoded equality is the quote-field evidence: it shows the adapter
read the documented **quote-currency** turnover index of the response it actually
decoded. A median-turnover plausibility print is a useful sanity check but is
**not** independent proof of field selection, so it is no longer used as one.
`np.isfinite(values).all()` is the finiteness check; the `> 0` assertions alone are
not one. With full pages, four (OKX) and two (Bybit) are expected; smaller pages
can increase those counts. Retries increase the separate attempt counter and
API error envelopes never become candle samples. Confirm that the server time
is close to the host clock and that the rules
match the venue's published contract page. Then run the full-roster preflight by
starting the real `collect` and letting its preflight pass before the bulk download
begins.

The snippet's comparison logic is covered synthetically by
`tests/pattern_lab/test_pattern_lab_collector.py::TestQuoteFieldEvidence`, using a
fixture whose base and quote volumes differ so a wrong-index comparison fails.
That is evidence about the snippet only: it certifies nothing about the live
endpoints, which this recipe alone can exercise.

## Event studies

M2a turns a frozen study request into reproducible evidence and one offline HTML
report. It is an event study, not an executable strategy or an equity backtest:
events may overlap, and the outputs describe what happened after an event.

```bash
python -m tools.pattern_lab study --spec STUDY.json --data-root PACK \
    --output-root NEW_RUN --workers 1
python -m tools.pattern_lab study --spec STUDY.json --data-root PACK \
    --output-root NEW_RUN_2 --workers 2
python -m tools.pattern_lab report --run-root NEW_RUN
```

`--output-root` is **exactly** the run directory; no run-ID subdirectory is
created inside it, the target must not exist, and it may not overlap the pack.
`report --run-root` takes that same directory. The CLI is a thin wrapper around
the Python API, which agent scripts call directly:

```python
from tools.pattern_lab import study

result = study.run_study(
    request="tools/pattern_lab/configs/example_study_two_green_30m.json",
    data_root="docs/_work/pattern-lab-data",
    output_root="runs/two-green-30m",
    workers=1,
)
results = study.load_results(result["run_root"])          # a completed run
partial = study.load_results(result["run_root"], allow_partial=True)
summary = study.summarize_results(results)
html = study.render_report(summary)
```

### Study request and protocol

Both documents are strict JSON at `schema_version=1`; unknown semantic keys,
duplicate JSON keys, duplicate IDs or cases, non-finite values, booleans used as
integers and unsupported versions are rejected, and every error names its field.
Relative `protocol` and `extensions[].source_root` paths resolve against the
**declaring request file**; CLI `--data-root` and `--output-root` resolve against
the current directory and are recorded as provenance only.

| Request key | Contract |
| --- | --- |
| `study_name` | Nonblank label shown in the report |
| `notes` | Free-form text, kept outside semantic identity |
| `protocol` | A path to a protocol document, or an inline protocol object |
| `study` | `start_utc`, `end_utc` (exclusive) and `warmup_start_utc`, all aligned to **every** selected timeframe |
| `instruments` | Exactly one of `{"roles": [...]}` or `{"ids": [...]}`; selectable roles are `trading` and `research_only` |
| `timeframes_minutes` | Positive integer multiples of 5m, deduplicated and sorted |
| `hypotheses` | Explicit variants: `id`, `hypothesis`, `parameters`, `occurrence` |
| `models` | Explicit instances: `id`, `model`, `settings`; the model owns its case axes |
| `metrics` | Optional declared summary metrics: `id`, `metric` |
| `extensions` | Optional trusted modules: `module`, `source_root`, `helpers` |

The tracked protocol `configs/protocol_development_v1.json` records the
development interval `[2025-07-01, 2026-07-01)`, the reserved interval
`[2026-07-01, 2026-10-01)` and the earliest permitted warmup `2025-06-01`. The
study interval and the consumed warmup are validated against it in both the CLI
and the Python API **before** any computation; there is no holdout mode and no
bypass flag. Changing the protocol changes the experiment identity — it never
establishes untouched historical data, and the earlier prototype already consumed
part of the reserve. `configs/example_study_two_green_30m.json` is the tracked
example: the full development year, June warmup, 30m, both directions, horizons
60/120/240/480 with primary 240, the built-in condition, `every_qualifying_bar`
and every `trading` instrument of the supplied pack.

A factor-only series is context, not a standalone target. A research-only target
is allowed when it is explicitly selected. Missing IDs, unsupported roles, an
empty selection and unsupported panel or external-series scope all fail before
any work; panel execution is M3.

### Features, hypotheses and occurrence

A feature declares its ID, version, parameters, dependencies, required prior
bars, initialization and instrument scope, and returns aligned float64 values
with a boolean validity mask. A hypothesis returns an aligned boolean condition
and its own validity mask from declared features. **Unknown is invalid, not
false.** Shapes, dtypes and finite valid values are checked, duplicate IDs and
unsupported dependencies are rejected, and a shared feature is computed once per
instrument, timeframe and parameter set.

Each candle is known at its close, and only current or past observations may
affect a feature or a condition there. A lookback never crosses a gap: state
resets and re-warms at every contiguous segment. Each parameterized
feature/hypothesis declares the prior observation bars it needs at the first
research bar as a **total, including its dependencies**. The transitive feature
closure is resolved before a run is created — checking registration, instrument
scope, normalized dependency parameters and dependency cycles — and the
effective requirement is the **maximum** of the hypothesis's own declared total
and every transitive dependency's declared total. Totals are never added, which
would count one shared lookback twice, and a hypothesis that understates its own
lookback still gets its dependencies' requirement. The declared elapsed warmup is
validated against that resolved requirement for every timeframe and recorded in
`spec/family.json`; an insufficient warmup is refused rather than shortening a
lookback. A downstream rolling transformation must still declare the total
history its own calculation needs: the framework cannot infer an arbitrary
Python lookback. Calendar
sufficiency does not prove contiguous observations: actual validity masks still
re-warm after a gap, and early gap-affected anchors can be invalid without
invalidating an otherwise usable study. The entire declared consumed warmup
enters input identity, so a repaired consumed June bar changes that identity.
Warmup bars are consumed but emit no study event.

The built-in condition `two_green_rising_quote_volume` is, at bar `i`:

```text
close[i-1] > open[i-1] and close[i] > open[i] and volume_quote[i] > volume_quote[i-1]
```

Both candles must be contiguous and valid. Equal open/close is not green and
equal volume does not rise. This is **at least two**, not exactly two: a longer
green run qualifies on several closes.

An **eligible study anchor** is a complete bar whose open is `>=` the study start
and whose close is strictly `<` the study end; its signal timestamp is that
close. The final bar closing exactly at the end therefore emits no event, while
an earlier event's outcome may end exactly at the end.

| Occurrence policy | Emission rule |
| --- | --- |
| `every_qualifying_bar` | Every eligible anchor whose condition is valid and true |
| `state_entry` | Only where the immediately preceding contiguous observation is valid and **false** and the current one is valid and true |

`state_entry` uses warmup history across the study start. A true condition after
unknown history, a gap or an invalid boundary is conservatively **not** a
false-to-true transition and does not emit.

An **episode** is a maximal contiguous run of a valid, true condition; gaps and
invalid regions break it. Episodes intersecting eligible anchors are recorded
separately from events, with their observed bounds, left/right censoring and
eligible-anchor count. Episode identity never depends on future duration, and
later duration or censoring fields are descriptive labels, never features. None
of these counts estimates an independent sample size.

Condition identity covers the hypothesis semantics, parameters and dependencies
but **not** the occurrence policy. Emitted-event identity adds the occurrence
policy to the condition identity, instrument, timeframe and signal time, so two
otherwise identical every-bar and state-entry variants can never collide.
Direction, horizon and model availability do not enter event identity: adding a
horizon or a metric cannot change an existing event, and an event is emitted even
when every requested outcome is invalid.

### The built-in fixed-horizon and path model

Models own their case axes. A model descriptor validates its own settings and
resolves a finite ordered list of cases per selected timeframe, each with a
stable semantic ID, JSON parameters and declared outcome names and units. A model
with no direction or horizon resolves one axis-free case and never invents dummy
axes. Fees belong to the models that use them.

```json
{
  "directions": ["long", "short"],
  "commission_pct_per_side": 0.05,
  "by_timeframe": {
    "30": {"horizons_minutes": [60, 120, 240, 480], "primary_horizon_minutes": 240},
    "120": {"horizons_minutes": [120, 240, 480], "primary_horizon_minutes": 240}
  }
}
```

One entry is required for every selected observation timeframe and no extra
entry is allowed. Every horizon must be a positive integer multiple of **its own**
timeframe — there is no global common-multiple restriction and no silent
rounding — and the primary horizon must belong to that timeframe's list.

Timing uses the real resampled OHLC, never Heikin-Ashi or another transform. For
a bar `i` opening at `t` with observation duration `D` and horizon `H = k*D`: the
event is known at `t+D`, notional entry is `open[i+1]` at `t+D`, the exit is
`close[i+k]` at `t+D+H`, and the path is bars `i+1..i+k` inclusive, excluding the
signal bar. A 30m bar opening at 10:00 closes at 10:30; entry is at 10:30 and the
1h outcome exits at 11:30 using the close of the 11:00 bar, with path extremes
from the 10:30 and 11:00 bars. This is a measurement convention, not a promise
that a market order would fill at that close.

Exact expected timestamps and contiguous complete bars are required from the
signal to the exit. No horizon may cross a gap, an invalid price region or the
study end; an exit at the study end is allowed when its last bar opens before it.
**Validity is per model, outcome and horizon**: a short horizon is never trimmed
to the longest horizon's support. Unavailability is recorded with a bounded reason
enum whose deterministic precedence is `terminal_study_end`, then
`missing_entry_bar`, then `incomplete_path`. Known input prices are retained and
unavailable prices and extrema are null — never zero.

For entry `P`, exit `X` and direction `d = +1` (long) or `-1` (short), with a
per-side commission rate `c` (`commission_pct_per_side / 100`):

```text
gross_return      = d * (X / P - 1)
commission_return = c * (1 + X / P)
net_return        = gross_return - commission_return

long  MFE = max(0, U/P - 1)   long  MAE = max(0, 1 - L/P)
short MFE = max(0, 1 - L/P)   short MAE = max(0, U/P - 1)
```

`U` and `L` are the highest high and lowest low of the same path. Commission is
charged on the entry notional and separately on the exit notional for a constant
quantity; the fixed 0.1% approximation is not used, and an unchanged exit price
still pays both sides. Values are stored in fractional-return units, and any
percent or basis-point rendering is labelled. Excursions are nonnegative
fractions of the entry price, gross of fees: a non-flat path that never rises
above entry has a long MFE of exactly zero. They describe price movement — they
are not realized profit, an R multiple or evidence of stop/target hit order.
Gross returns are sign mirrors on common support; **net returns are not**, and
the short side is computed from its own formulas rather than as `-net_long`.

Slippage and funding are excluded by explicit decision. The 10,000 USDT deposit,
the 2% risk setting and the 8x cap belong to M4 sizing and are not implied here.

### Evidence layout and identity

```text
<run-root>/
  spec/request.json spec/protocol.json spec/family.json spec/source.json
  spec/snapshots/<module>__<file>.py     inert copies of declared custom source
  admitted/<INSTRUMENT_ID>.json          per-job input identity and state
  jobs/<INSTRUMENT_ID>/                  conditions/episodes/emissions/primitives
  jobs/<INSTRUMENT_ID>/bundle.json       the job's own file digests and counts
  provenance.json status.json metrics.json
  completion.json                        written last
  derived/summary.json derived/report.html
```

Tables are versioned Parquet with UTC epoch-millisecond times, float64 numerical
evidence and explicit null/validity; JSON is strict, with no NaN or Infinity. An
empty table keeps its declared schema. There is no SQLite registry.

| Evidence | Contents |
| --- | --- |
| Frozen spec/protocol/family | All normalized settings, the exact selection, the model-owned resolved cases, warmup requirements and source references |
| Per-job identity/status | Per-timeframe admission fingerprints, declared coverage, gaps and groups, and the completed/failed/not-started state |
| Conditions | Anchor open and signal time, condition identity, value, validity and episode link, for **every** eligible anchor |
| Emissions | One row per emitted event, with its variant, occurrence policy, event ID and episode link |
| Episodes | Stable identity, observed bounds, bar and eligible-anchor counts, and left/right censoring |
| Fixed-model primitives | Instrument, timeframe, model instance, anchor, horizon, intended entry/exit times, entry `P`, exit `X`, path high `U`, path low `L`, and separate return/path validity and reasons |
| Custom model evidence | The common instrument/timeframe/model-instance/case/anchor envelope plus that model's own declared typed outcomes |

**A custom model's returned evidence is admitted before it is published, and the
same structural and value validator runs again whenever a saved custom table is
decoded.** The returned evidence kind must match the registered descriptor and
the frozen family. Exactly one row must exist for every `(resolved case,
eligible anchor)` pair of that instrument and timeframe, including explicitly
unavailable outcomes; duplicate, missing and unknown keys all fail rather than
silently changing the sample or being inner-joined away. Anchor timestamps must
be exact integer UTC milliseconds — a boolean, a fractional or a string
timestamp is not an integer timestamp. Every declared outcome keeps the float64
contract: `available` requires a finite value, an unavailable value is null with
a nonblank non-`available` reason, and an infinity, a null marked available or a
finite value marked unavailable is rejected. Unavailable reasons stay
model-owned strings; no build hardcodes a future model's reason enum. When a
model's cases declare different outcomes, each row is validated against its own
case's declarations and the union columns that case does not declare stay null
with an explicit nonavailable reason. Empty eligible anchors produce the declared
empty schema, not an exception and not a schema-less table.

On read, the expected anchors come from the run's own saved all-anchor
conditions table and the resolved cases from the frozen family — never from the
emissions, the market pack or rerunning the model. **A matching file hash proves
the bytes, not the rows**, so a missing or duplicated row, a non-finite available
outcome or an inconsistent reason fails observation, summary, metric and report
reading with an instrument- and model-specific error even when every recorded
digest matches. Valid older evidence stays readable: nothing is filtered,
repaired or migrated.

`signal_time_ms = anchor_open_ms + timeframe_minutes * 60000` is part of the
common custom observation view and is **derived on read** by the core, so valid
evidence saved before this rule gains the column with no pack read, no migration
and no fabricated fixed-horizon field. `EVIDENCE_VIEW_VERSION` stays `1`: the
column is additive and derivable, and no existing value or formula changed.

**The built-in's physical rows are direction-independent.** One primitive row is
saved per model instance, instrument, timeframe, anchor and horizon — not
duplicated long and short rows — and the case registry maps both directional
cases to that row. Every eligible anchor is kept, including non-events and
invalid outcomes, so M3 can obtain aligned event and non-event evidence with
distinct masks. Condition validity is separate from model availability, and
conditions do not determine it.

A versioned public evidence-expansion function derives the directional outcomes
from `P`/`X`/`U`/`L` and the frozen case settings. It reads no market data and
executes no hypothesis. `load_results` exposes both the raw primitives and that
documented observation view per job and group, so a report or a custom metric
never materializes every directional row of the universe at once.

Four identities are kept apart:

1. **Specification/family identity** — the normalized semantic request, the
   protocol and the complete planned family.
2. **Data-input identity** — composed at successful completion from the ordered
   per-timeframe fingerprints, the semantic settings, the selected universe with
   its roles, and the protocol.
3. **Implementation identity** — consumed core source digests, declared
   extension digests and the numerical library versions actually used.
4. **Physical/environment provenance** — manifest revision and digest, the
   `integrity_scope=full_pack` record, the source snapshot, commit and dirty
   state, Python and platform, worker count, roots and timings.

Adding data outside the consumed interval, moving the root, the worker count and
unrelated Git edits leave identical fixed inputs identical. Relevant code changes
stay visible in implementation identity. An incomplete run keeps its planned
identity and its known job identities, never a complete all-input identity. There
is no resume or reuse in this milestone: retained bundles are evidence, not a
cache.

For scale, a full-year 44-instrument 30m run with four horizons produces about
**3.08 million** primitive rows, representing about 6.17 million logical
directional observations. Actual bytes depend on the schema and compression;
measure a bounded run rather than assuming a size.

### Admission, execution and failure states

`workers` accepts any **positive integer**, default `1`. A boolean, `0`, a
negative number, a float and a string are actionable validation errors **before**
any output is created; nothing falls back silently and nothing is capped to a
detected CPU count. See
[Workers and the bounded spawn pool](#workers-and-the-bounded-spawn-pool).

**Every accepted public request form passes the same execution-boundary
validation.** A request file, a request mapping and an already normalized
`StudyRequest` all reach one semantic path. A frozen dataclass is not trusted on
its own: `dataclasses.replace` never revalidates, and a frozen request's nested
parameter and settings mappings stay mutable. At the boundary the semantic
settings are resolved again through the same validators, the protocol bounds are
rebuilt from the protocol document rather than read from its private `_bounds`
cache, and the condition identities, resolved cases, warmup and subsequent
identities are recomputed. A supplied derived fact that contradicts that fresh
resolution is rejected. The result is a **fresh validated value**: a caller's
mapping is never mutated, and a valid normalized request keeps working
unchanged.

Protocol **provenance** survives that revalidation. A normalized request that
came from a protocol file keeps recording that file in `spec/request.json`,
because `protocol_source` is retained as non-semantic caller-supplied provenance,
while the consistency check validates the protocol document itself. Carrying it
never reopens the path or lets the file's current contents replace the normalized protocol,
which is still rebuilt inline; an inline protocol stays `inline`. A request
supplied as a file, as a mapping and as an already normalized object with
equivalent settings therefore keeps one semantic identity. For a caller-created
normalized object, the recorded source path is not independently authenticated.

Before the run directory exists, and before any study feature or outcome is
computed, the coordinator validates the request, protocol, extension
declarations, resolved cases, feature warmup requirements, selection and
input/output path separation; then, under one pinned read session, it collects
**all** metadata admission failures across the selected instruments and
timeframes — state, declared coverage, closure, quote verification and boundary
alignment — and finally calls `inspect(verify=True)` **once for the entire pack**.
A corrupt *unselected* file therefore blocks the run too. Any preflight failure
exits `2` and leaves no run directory; a busy or pending pack keeps exits `3`
and `4`.

**Metadata cannot prove that a timeframe contains complete research groups or
that warmup is contiguous.** After preflight the run is created and instruments
are admitted one at a time, reproducing `load_slice`'s per-timeframe check on the
actual resampled inputs: no complete research bar is a data-admission failure,
not a successful empty series. A valid timeframe with no eligible anchor or no
event is different and finishes with explicit empty evidence.

Each timeframe's fingerprint is computed over the raw consumed 5m rows, including
rows in subsequently omitted aggregate groups, and matches
`load_slice(..., timeframe_minutes=tf)` exactly. An admitted job's input identity
is persisted **before** calculation, so even a failed numerical job retains what
was observed.

**The coordinator tracks the instrument it is working on from the first base
read.** **Every** exceptional exit from that read, preparation or the admission
record write marks that exact instrument failed with its phase and the actual
diagnostic — an ordinary
exception and a control-flow exception such as `KeyboardInterrupt` alike, not
only an already actionable data error. The metadata context that was known is
recorded: a successful read's consumed context is kept when preparation then
fails, and prepared fingerprints survive a subsequent admission-write failure
(phase `admit`). No input fingerprint is invented for rows that were never read.
On the first job or admission failure the coordinator stops dispatching, marks that
job failed and the remaining jobs not started, retains completed bundles and
exits `2`. A user interrupt keeps its own type, records the interrupted state
and exits `130`. **No completion marker is published for these states**; the
coordinator's own view is updated before anything is written, so a secondary
admission, per-job or status write failure can neither replace the original
diagnostic nor prevent the outer failure handling and pool cleanup. Best-effort
writes are not a recovery guarantee for an unwritable filesystem or an arbitrary
process kill.

**Immutable evidence and regenerable reports are separate.** The completion
record hashes the frozen spec, protocol and source evidence, the admitted job
identities, the complete raw job bundles and the final terminal status; it never
hashes itself and never hashes `derived/summary.json` or `derived/report.html`.
It is written last, only after every planned job, all immutable metadata, every
declared metric and the initial derived report have succeeded, and the terminal
status is not rewritten afterwards. The initial summary is built through a small
internal prepublication path, so a run's own first report and every later
regeneration describe it identically without the public partial reader ever
claiming premature success. There is no public `skip_verify` or `assume_complete`
flag and no second hash registry.

`report` verifies the raw evidence and the completion record **once**, through
the same loading path the public reader uses, regenerates the derived outputs
from the saved observations and frozen settings, and atomically replaces only
those two files. It needs no pack and imports no saved custom module. A
regeneration failure leaves the evidence and the original completion record
intact and never reclassifies a successful study; raw corruption blocks it
entirely. Regenerated derived files are not required to be byte-identical across
code versions — the frozen semantics are.

**An existing completion record is verified in every reader mode.**
`load_results(run_root)` and `report` require a verified completed run: the
record, the immutable evidence it names and a consistent terminal state and
planned-job count must all agree. `load_results(run_root, allow_partial=True)` is
the explicit partial-inspection API: it reconciles the planned, admitted,
completed, failed and not-started counts from the committed per-job records,
published bundles and run-level status conservatively, verifies and loads only
intact completed bundles, and always reports `complete=False` without a verified
record — including when the terminal status says `completed`. A job claimed
completed whose bundle is missing or corrupt **fails** rather than disappearing,
an unfinished staging directory is never completed evidence, and partial loading
is read-only: it repairs nothing, resumes nothing and promises no power-loss
recovery. Partial inspection is **not** a corruption bypass, and malformed or
unsupported completion metadata gives an actionable validation error rather than
an incidental `KeyError`.

**What a completion record must actually say.** The whole published schema-v1
record is validated, not only the fields the file-digest pass happens to touch.
A supported integer `schema_version` — a boolean is not an integer — a completed
terminal state, the `evidence_sha256` map, `evidence_set_sha256`,
`derived_files`, `counts`, `identities` and `run_root` must all be present and
well formed; digests use this evidence's SHA-256 representation and counts are
nonnegative integers, never booleans or floats. `evidence_set_sha256` is
**recomputed** from the verified file map and compared by value, so a missing
digest, a valid-length wrong digest and 64 zeroes all fail. The required count
keys are `planned`, `admitted`, `completed`, `failed` and `not_started`, and the
required identity keys are `specification_sha256`, `implementation_sha256` and
`data_input_sha256`. A completed run cannot record a failed, unstarted or merely
admitted job, its counts must agree with the reconciled per-job records, terminal
status counts and the family's planned job count, and its identities must agree
with the same run's verified provenance: duplicated
metadata is not accepted merely because its types are valid. `derived_files`
must name exactly the supported regenerable outputs, without duplicates or
arbitrary paths; those files are **not** hashed and may legitimately be absent
or edited. `run_root` is a recorded provenance string that is never resolved,
required to exist on this host or required to match this host's path syntax, so
a valid run stays readable after it is moved between directories or hosts.
Anything else is a `corrupt_evidence` error in strict loading, in explicit
partial loading and in report regeneration — raised **before** any derived
output is replaced, leaving the raw evidence, the status and the existing
derived outputs untouched. A missing record keeps its existing meaning: strict
loading reports an incomplete run and partial loading can inspect intact
progress without certifying completion. Validation needs no market data, no
custom import and no agreement between the historical run's implementation
identity and today's checkout.

A completed job's published bundle and its committed record are discoverable by
explicit partial inspection **before** the final run status is written, so
durable progress is visible at the moment it becomes durable.

**Handled publication failure and the point of no return.** If the initial
publication fails before a valid completion record exists, the coordinator
removes only this run's own generated `derived/summary.json` and
`derived/report.html` on a best-effort basis — never a recursive directory
removal — retains all raw evidence, reports any cleanup failure separately in the
recorded status, and keeps the original diagnostic. Once the completion record
has been atomically replaced the run is sealed: a later exception never
overwrites that sealed status, never deletes its published derived report and
never reclassifies the run. These are handled-error contracts, not promises about
`SIGKILL` or an unwritable filesystem.

**Failure, cancellation and process lifetime.** On the first observed
admission, job, source or publication error, or on a user interrupt, admission
and dispatch stop immediately and already published bundles are retained; no
further result is published once the run enters abort handling. Unstarted work
cancels and this run's workers are shut down within the bounded cleanup deadline
described under [Internal deadlines](#internal-deadlines): graceful cleanup is
attempted, then owned processes are terminated and, if necessary, killed, with
bounded joins and closed IPC handles. Unsubmitted
instruments remain `not_started`; a dispatched job without an accepted result
becomes `failed` with an explicit aborted reason **distinct** from the original
computation error. A worker initialization error before any dispatch is a
run-level failure with every instrument not started — no instrument is invented.
An abrupt child exit, a serialization failure, a lost job or a stopped result
transport is an actionable failed run, never an infinite wait or a silently
replaced job, and there is no automatic resubmission and no silent serial
fallback.

During teardown, pending transfers are consumed before waiting for a producer
that may be blocked flushing a large message; during an abort, received results
are discarded rather than published. A broken channel is abandoned and its
owners are bounded-joined instead of being drained indefinitely — "drain before
join" is not an unconditional obligation once a producer has died mid-message.
See the [Python multiprocessing programming
guidelines](https://docs.python.org/3.11/library/multiprocessing.html#programming-guidelines).

For an arbitrary coordinator termination only the smaller existing contract
holds: the OS releases its pack guard, no child reads or writes the pack, no
child publishes, and no completion marker exists unless publication had already
finished. Immediate orphan CPU cleanup and a freshly written terminal status
cannot be guaranteed after `SIGKILL`/`TerminateProcess`, and there is no parent
heartbeat, lease or watchdog service.

**Bounded evidence reuse.** During ordinary summary aggregation each needed raw
table is decoded at most once per instrument, reused across every declared group,
and released before the next instrument; the summary read count therefore does
not grow with the group count. The public observation API and the summary share
one expansion implementation, and no all-instrument or all-directional cache is
kept. A group's assembled observation columns are shared between that group's
declared metrics instead of being decoded again for every metric; metrics may
need whole-group rows, so those frames stay bounded to the group being computed
and a later group reads the tables again rather than retaining every group at
once. Exact summary quantiles still retain compact per-group sample arrays across
instruments: that is a separate memory cost from the one live instrument's
frames, and it is not covered by any per-job bound.

### Workers and the bounded spawn pool

`--workers N` / `workers=N` accepts any positive integer and defaults to `1`.
Effective parallel capacity is `min(requested_workers, selected_instruments)`;
both counts are recorded in `provenance.json` under `execution`, together with
the start method, the coordinator PID, the worker PIDs and the thread settings.
The capacity is never silently reduced to a detected CPU count, and no arbitrary
backend cap is invented: on the supported platforms the chosen public
`multiprocessing` facilities impose no further documented worker-count limit,
and the only clamp is the selected instrument count.

`workers=1` calls the same top-level numerical job **directly**, with no child
process. `workers>1` uses an explicit **spawn** context on both hosts, including
when the selection reduces effective capacity to one. The global start method is
never changed. CLI help and ordinary imports start no job, spawn nothing, read
no pack and import no user module. Spawned children re-import the calling
module, so an agent script must stay importable with its work behind an
`if __name__ == "__main__"` guard;
`tools/pattern_lab/examples/run_example_study.py` shows that shape and takes its
own `--workers`.

One job owns one instrument and all of its requested timeframes, variants and
models. There are no nested pools, no per-case processes and no configurable
task types: transport bookkeeping uses an opaque job ID and a prepared payload,
while instrument selection, OHLC/timeframe semantics and evidence publication
stay in the coordinator. Explicit `Process` handles and two bounded queues are
used rather than a higher-level executor, because on the supported Python
version cancelling a future does not cancel a running call and returning from
`shutdown(wait=False)` is not process termination.

**Coordinator ownership and backpressure.** Request and metadata admission,
every collected metadata error and exactly one full-pack `inspect(verify=True)`
still happen before any output. One coordinator-owned pinned read session covers
every input read of one pack generation; each admitted instrument's consumed 5m
slice is read once and its timeframes are prepared in RAM with the shared M1
helper and unchanged fingerprints. With `W` the effective parallel capacity, at
most **W submitted, running or returned-but-not-published instrument jobs
combined** are retained, plus at most **one** instrument being prepared. A free
slot is filled only after ready results have been drained and published and
failures have been checked; nothing eagerly prepares the whole universe, maps
over it or accumulates finished results. A slot stays occupied from dispatch
through accepted publication or abort: its input transport buffers and its
returned result are parts of the same job, not separately free slots. The
coordinator drops its extra reference to a prepared payload once transport owns
it, but retained IPC buffers still belong to that slot, and serialization is not
claimed to keep only one physical copy. This supersedes the earlier prospective
`2*workers` payload wording; it is a job-count bound with a bounded constant IPC
factor, not a universal byte or RAM guarantee.

Admitted input identity is persisted before dispatch. Publication and every run
output write happen in the coordinator, once per completed instrument plus small
status updates and the final reports. Children never receive an open session, a
lock handle or a market-file path as a reading instruction, never call the pack
API and never publish; no temporary file is used for payload transport. The read
session is held while further pack reads are possible and is released before
final aggregation: in practice the coordinator keeps it through pool teardown
for simpler ownership, which is its actual documented lifetime. A spawned child
inherits no lock file descriptor, so it cannot extend the guard's lifetime after
the coordinator dies; no reader bypass or lock redesign is needed.

**Worker initialization and numerical threads.** Each worker explicitly calls
the idempotent `register_builtins()` and then initializes the declared extension
set, rather than relying on an unused import or a package import order to
populate the registry. **Every** declared module *and helper* file is checked
against the coordinator's frozen records — which travel with the declarations as
private worker settings, not as a second registry or manifest format — **before**
anything is imported or registered, and again after import. The declaration and
the frozen record must cover exactly the same files, so a helper the coordinator
never froze is as unusable as one that changed; a helper-only edit fails even
when the main module is untouched, an edit made during import is caught by the
recheck, and a child never substitutes its own freshly observed hashes for the
coordinator's generation. The required descriptors must then be present, and a
missing built-in or custom registration is a named initialization error. An
initialization failure is run-level: every instrument stays not started, no
instrument is invented, no job output is accepted and the pool is cleaned up
within its bounded deadline. Only serializable settings and numeric arrays cross
the boundary, reconstructed cases and descriptors are checked against the frozen
job semantics, and input arrays are marked read-only again after IPC
reconstruction — serialization does not preserve the flag. The same protection
is applied to direct execution: a feature or model may allocate its own working
arrays but never mutates the shared bars.

Numerical-library inner thread limits are pinned to one in workers. The startup
variables are set in the caller's environment **before** a child can import
NumPy or BLAS — an initializer running after those imports cannot reliably
retune an already loaded library — and the override is scoped to the whole pool
lifetime, including any lazy initial spawn, and restored in a `finally` clause
after teardown, including the absence of a variable. Arrow's own pools are
additionally set through `set_cpu_count`/`set_io_thread_count` in the child.
Requested settings and the values each child could actually read back are both
recorded, and they are distinguished: environment variables are **startup policy
evidence**, while BLAS runtime thread limits are not observable from Python here
and are not claimed to be measured. A default `pyarrow.cpu_count() == 1` on a
one-core host proves nothing by itself, which is why the check seeds the
caller's variables with non-1 values and verifies that every initialized child
still sees `1` while the caller regains its original mapping after success and
after failure. The embedding application's environment, global start method and
parent thread pools are not permanently changed; the temporary process-global
change is serialized inside this tool's startup helper, and concurrent
independent startups from unrelated threads are **not supported**. None of this
permits replacing a failed worker or retrying a job.

A worker ignores `SIGINT` before importing or executing any trusted extension,
so the coordinator owns handled Ctrl+C and no worker races it to publish an
interrupted status, and a worker's Python stdout is redirected to stderr before
extension import, so a printing extension cannot corrupt the machine-readable
CLI stdout. Neither is a sandbox against arbitrary native file-descriptor
writes.

**A stopped result transport is a run-level failure.** A daemon pump thread
moves results off the IPC channel, because a worker killed mid-message can leave
a truncated frame that no timeout can recover from once its length prefix has
been consumed. If that pump ever exits while the pool is still open — only
teardown may end it — no further result can arrive, so both the coordinator's
blocking wait and its nonblocking progress check report an actionable
`transport_failed` error with the underlying diagnostic instead of waiting out
the result deadline. If an owned worker is also known to have exited, both
receive paths prefer `worker_lost` and preserve any known orphaned-job attribution.
A transport-only failure names no instrument: already published bundles are kept,
no further result is accepted, dispatched work without an accepted result is
marked aborted, no completion is published and the existing bounded cleanup
runs. Normal teardown and the pump's own sentinel are not failures, and a
*blocked* pump is not an *exited* one: worker-loss detection and the outer
deadlines still cover that case. Nothing here drains a broken channel, restarts
the pump or a worker, or redesigns the IPC. Abandoning a pump that is stuck on a
truncated frame remains exceptional and is honestly incomplete: it does not
guarantee that every daemon thread and buffer is reclaimed inside a long-lived
embedding process.

#### Internal deadlines

These are failure detectors, not performance budgets or configuration, and none
of them is a per-instrument or total study duration limit.

| Bound | Value | What it measures |
| --- | --- | --- |
| Worker startup | 300 s | Waiting for every child to report itself initialized |
| Result wait | 3600 s | One `take()` call's own wait, measured from that call |
| Graceful cleanup | 10 s | The whole graceful stop before forceful steps begin |
| Terminate join | 2 s per worker | Waiting after `terminate()` before `kill()` |
| Kill join | 2 s per worker | Waiting after `kill()` |
| Pump join | 0.5 s | Waiting for the daemon pump before abandoning it |

An abort skips graceful waiting entirely and goes straight to the forceful
steps. The 10-second constant is therefore **not** an absolute bound on the
entire teardown: the bounded per-worker joins and the pump join follow it.

**Determinism.** For identical inputs, source and library versions on one host,
`workers=1` and `workers>1` produce identical canonical numerical evidence,
event and episode identities, resolved family, data-input identity,
implementation identity, counts and descriptive metrics, whatever the completion
order. Fingerprints are composed in frozen planned order, saved instruments are
consumed in stable order, and no completion-order-dependent floating-point
reduction exists. Execution provenance may differ: worker counts, PIDs, roots,
timestamps, timings and thread observations. The completion record's
`evidence_set_sha256` therefore **can** differ between a direct and a pooled run
of the same study, because it covers `provenance.json` too; Parquet byte
identity is not a substitute for decoded row and value equivalence.

### Agent extensions and source integrity

Extending Pattern Lab needs no core dispatch edit and no inheritance: ordinary
importable functions plus small frozen descriptors and explicit registration are
enough. `tools/pattern_lab/examples/custom_extension.py` is a complete working
example — a causal SMA feature, a `close_above_sma` hypothesis, an axis-free
custom model and a descriptive metric — and
`tools/pattern_lab/examples/run_example_study.py` runs it through the public API
with explicit paths. Tracked examples work in a fresh clone once the caller
supplies their own valid pack; they never download data and never assume an
ignored local artifact exists.

The caller names each trusted module, its explicit source root and its local
helper dependencies. There is no directory-wide discovery, no `eval`, no
pickle-based configuration, no silent built-in replacement and no runtime
monkeypatching. A declared module must expose `register(context)` and register
its own descriptors.

**Every descriptor a study actually uses must come from a declared, verified
source generation.** The used set is resolved explicitly — the requested
hypotheses, their transitive feature dependencies, the model instances and the
declared metrics — and each non-built-in member of it must be attributable to an
extension this request declares and verifies. A registration with no source, an
unknown source or a mismatched generation fails **before** the run directory is
created. A source generation is a digest, not a path, so two declarations of
byte-identical source are the same generation. Registry entries this study does
not use are not consulted, so an unrelated leftover registration cannot block a
built-in run.

**One source-integrity mechanism is used: digest verification, with inert
snapshots.** Declared module and helper files are hashed before import and
registration, verified again before each job, before that job's result is
accepted and around parent-side custom metric computation, and — in a spawned
child — against the coordinator's frozen generation both before and after the
child's own import, and any detected change fails the run. Copied source in
`spec/snapshots/` is provenance only and is never executed. Only registrations
whose recorded digests match a currently declared file are reused; a module this
interpreter imported by another path cannot have its source generation
established, so the error asks for a fresh interpreter. This checks cooperating
stable source files — it is **not** a sandbox against an adversarial filesystem
or hidden dependencies of trusted Python.

A metric declares the saved columns it requires, its value and unit. When a
group's observation view does not expose them the metric is recorded as
explicitly unavailable; nothing is guessed. Declared metric values are stored in
the immutable `metrics.json`, so normal report regeneration reuses them without
importing any saved module. Recomputing a custom metric requires supplying its
registration again. Missing future per-bar information may require a new explicit
study rather than fabricated data.

### The descriptive report

`derived/report.html` is a standalone light-theme page with no CDN and no network
dependency: escaped HTML tables plus a few lines of local JavaScript. It shows
the study question, exact dates and warmup, universe and roles, timeframes and
horizons, the signal/entry/exit convention, the occurrence policy and the costs,
and displays conspicuously:

> Descriptive event study — statistical validation is not implemented in M2.

It lists working offline **relative** links to the run's machine-readable
evidence — the frozen spec, the provenance, the status, the completion record,
its own `derived/summary.json` and each completed job's bundle — resolved from
`derived/report.html` with escaped paths. It states explicitly that authoritative
completion is a matching verified `completion.json` and that opening the page is
not a completion check.

It also discloses overlapping observations, unknown historical universe
membership, prior use of the reserve and the excluded slippage and funding.
Results are grouped by **resolved model case**, not by an assumed global
direction or horizon axis, and a custom model's own declared outcomes and units
are reported rather than fabricated fixed-model values. Each declared outcome
group shows events, episodes, valid outcomes, invalid counts by reason and ticker
coverage; returns show the arithmetic mean, median, 10th/25th/75th/90th empirical
percentiles (NumPy's linear convention) and the fraction strictly above zero,
with gross and net separate; MFE and MAE show the same location statistics. No
standard error, confidence interval, Sharpe, CAGR, WFE, compounded balance or
leveraged profit appears in this event model.

Primary pooling is event-weighted over the valid emitted-event observations.
Beside it the report shows the equal-ticker **mean of defined per-ticker means**
for the same outcome; averaged per-ticker quantiles are never called pooled
quantiles. Minimum support for descriptive inclusion is one valid event per group
and ticker, zero-support tickers and actual denominators are reported, and no
inferential adequacy is claimed. Zero-event and all-invalid groups stay visible
with null metrics rather than zero profit, and raw all-anchor observations are
never counted as signal events. The default order follows the declared family,
not performance, and the declared primary horizon's emphasis never hides the
other horizons or directions.

## Matched comparisons and calibrated inference

M3a analyses a **completed** M2 study offline. It measures the difference
between signal outcomes and comparable control outcomes, quantifies that
difference's uncertainty, and reports the declared multiple-testing family. It
runs no new backtest, generates no equity and certifies no profitable strategy.
Every T05 analysis, including a rerun, is an **exploratory development
analysis**: the family is frozen for one execution, which is not historical
preregistration.

```bash
python -m tools.pattern_lab analyze --run-root COMPLETED_STUDY \
    --spec ANALYSIS.json --output-root NEW_ANALYSIS
python -m tools.pattern_lab analysis-report --analysis-root ANALYSIS
```

```python
from tools.pattern_lab import analysis

result = analysis.run_analysis(
    request="analysis.json", run_root="completed-study", output_root="new-analysis"
)
saved = analysis.load_analysis("new-analysis")
saved.comparisons()   # estimates, intervals, p-values and availability reasons
saved.strata()        # every stratum, including zero-event and excluded ones
saved.daily()         # the daily counts and sums that reproduce the estimator
analysis.regenerate_report("new-analysis")
```

These helpers execute no hypothesis module, no saved Python snapshot and no
metric plugin. Report regeneration needs **only** the sealed analysis artifact:
not the original study, not its market pack and not any saved source. That
deliberately differs from M2's `report`, which recomputes its summary from raw
evidence; `analysis-report` only renders the sealed analysis summary.

### The analysis request

Strict JSON at `schema_version=1`, accepted as a file path or as a mapping
through one normalization path. There is no trusted normalized-object bypass: an
already normalized request is rendered back into this schema and revalidated.

```json
{
  "schema_version": 1,
  "analysis_name": "Two green candles: matched comparisons",
  "model_instances": ["fixed_horizon"],
  "pairwise": [
    {
      "id": "volume_filter",
      "target_variant": "two_green_volume",
      "control_variant": "two_green_plain"
    }
  ],
  "resamples": 9999,
  "seed": 20260920,
  "notes": null
}
```

| Key | Contract |
| --- | --- |
| `schema_version` | Exactly `1`; a boolean is not an integer |
| `analysis_name` | Nonblank label, outside semantic identity |
| `model_instances` | Nonempty, unique model-instance IDs of the source study |
| `pairwise` | Optional explicit comparisons; defaults to an empty list |
| `resamples` | Integer `B` in `[1999, 99999]` |
| `seed` | Integer in `[0, 2**32-1]` |
| `notes` | Free-form text or null, outside semantic identity |

Unknown keys, duplicate JSON keys, duplicate IDs, duplicate semantic pairs,
self-comparisons, non-finite values and unknown references are all rejected, and
every error names its field. Pairwise IDs may not use the reserved
`baseline__` prefix. Cost scales linearly in `resamples` and family size;
batching bounds intermediate RAM, not total computational work.

Version 1 **fixes** the method: `calendar_score_cbb_v1`, matching
`instrument_utc_month_v1`, block length **7 days**, two-sided alpha **0.05**,
pointwise confidence level **0.95** and the support rules below. These resolved
values are recorded in the frozen family and in the analysis identity even
though they are not request switches. A future method change needs an explicit
version, not a silent default change; there is no menu of inference or matching
methods.

### Admission

Before any output directory exists:

- The source run is loaded **strictly** through the existing reader. A partial
  run, a missing or invalid completion record, inconsistent counts or identities
  and corrupt evidence are errors. A caller-supplied results object does not
  bypass this check: `run_root` must be a path.
- Every selected model must be the built-in fixed-horizon contract: the exact
  triple `model_id="fixed_horizon_path"`, `model_version="1"` and
  `evidence_kind="fixed_horizon_path_v1"`, plus evidence view version `1`, and
  its saved settings and resolved cases must agree with the built-in validator
  and resolver. **Evidence kind alone is insufficient** — an extension may
  legally declare the same kind — and no source extension is imported to decide
  it. A selected custom model is rejected clearly; return and holding semantics
  are never guessed from column names. Unselected custom models may remain in
  the source run.
- The saved study bounds are rechecked against the study's own saved development
  protocol. M3a introduces no reserved-period bypass.
- The output root must be new, must not contain or sit inside the source run,
  and must not overlap the recorded pack root. Paths are compared resolved,
  including symlinks.

All source instruments, observation timeframes and cases of the selected models
participate. There is **no** result-driven ticker, horizon or direction filter in
this interface.

Once admitted, the normalized request, the source binding, the resolved family
and an initial status are written **before** any outcome aggregation. Those four
writes are the `freeze` phase and sit **inside** the protected region, so a
failure or interrupt in any of them records the same honest terminal status,
phase and analysis root as a later failure. Output-root admission and creation
stay **outside** it: an admission failure keeps its own cause and never reaches
status writing with a root this operation does not own.

### The declared family

A nonsignal baseline comparison is generated for **every** saved hypothesis
variant, with the ID `baseline__<source_variant_id>`, followed by the explicitly
declared pairwise comparisons. All of them are then expanded across the selected
model instances, timeframes and cases. The canonical order and the stable
semantic IDs depend only on the semantics, never on the request's list order, on
filesystem traversal or on worker order; labels are preserved separately and
identifiers are never truncated.

| Comparison | Target E | Control C |
| --- | --- | --- |
| Nonsignal baseline | That variant's saved emitted events with a valid return outcome | Anchors with a known-valid **false** condition for that variant and a valid return outcome |
| Explicit pairwise | The target variant's saved emissions | The control variant's saved emissions, on the **common** availability of both conditions |

For `state_entry`, a known-true nonemitting anchor belongs to **neither** group,
and unknown history or an unknown condition is never a false control. Overlap
between E and C is allowed, counted and reported: an inclusive two-green parent
genuinely contains some target events, the two means are not independent, and
the overlap is neither removed nor pretended away. Valid overlap and empty
support are both accepted rather than rejected.

This delivery implements **inclusive parent** comparisons; there is no
additional disjoint-complement mode. A report calls a comparison an inclusive
parent only where that relationship actually holds in the retained strata, and
logical implication is never inferred from a variant name.

### Masks, time ownership and one checked alignment

The analysis uses the reader's **all-anchor** case view and owns one checked
alignment stage that joins the saved conditions and emissions to those anchors.
The existing M2 `join_events` inner merge is not used for admission, and M2's
event-filtering semantics are unchanged. Join keys are normalized losslessly
(saved timeframe values may be `int32`); a coercion from a boolean, a float or a
string is rejected. Duplicate or missing evidence fails; it never disappears
into a merge. Every selected emission must join a known-valid, true condition at
the expected saved anchor. Each saved condition and emission row's
`signal_time_ms` is checked **row-wise** against that same row's
`anchor_open_ms + timeframe`: the two columns are never sorted independently, so
a permutation of signal times across different anchors is rejected, while a
legitimately reordered but intact table is still admitted.

Saved conditions and emissions are used as they stand: the hypothesis is never
reevaluated. UTC day and calendar-month membership use the **signal close time**
`signal_time_ms`. The existing next-open entry, horizon, fees and per-outcome
validity are unchanged; an outcome may cross a month boundary when M2 considered
it valid, but never the source study end, and a short horizon is never trimmed
to the longest one's support.

Only anchors that are a target or a control of the comparison are materialized
as records. An anchor that is neither contributes to no statistic; the
eligible-anchor counts and the availability and validity losses stay recorded in
the artifact rather than carried as inert rows. Each raw table is decoded once
per instrument and reused across every case and comparison, then released.

### Strata, exclusions and weights

A stratum `s` is `(instrument_id, UTC signal month)` inside one resolved
comparison, model instance, timeframe and case. A stratum is retained only when
it has at least **20** valid target and **20** valid control observations, and
target observations on at least **10** distinct UTC days and control
observations on at least **10** distinct UTC days.

These are explicit support safeguards, not assertions of independent samples.
They depend on availability and counts, **never** on return values. Every
stratum is recorded — including zero-event and excluded ones — with its counts,
days and reason codes (`target_count_below_minimum`,
`control_count_below_minimum`, `target_days_below_minimum`,
`control_days_below_minimum`). There is no fallback to another month, no zero
fill of outcomes, no inferred missing control and no performance-based
exclusion. The retained strata define the supported population.

For retained strata, with counts `nE_s`, `nC_s` and net-return sums `a_s`, `b_s`:

```text
N       = sum_s nE_s
muE_s   = a_s / nE_s
muC_s   = b_s / nC_s
w_s     = nE_s / N
signal  = sum_s w_s * muE_s       # equivalently sum_s a_s / N
control = sum_s w_s * muC_s
lift    = signal - control
```

Gross means use the **same** masks and support and fees are preserved
separately. Returns are fractions internally; the HTML labels percentages and
percentage points explicitly. Signal, control, net and gross are distinct
fields; overlapping observation returns are never summed into profit. An empty
supported population produces null estimates with reasons, never zeros and never
an exception.

The artifact reports retained and available valid target counts and their ratio,
lost target counts by availability and support reason, retained control counts,
unique overlapping anchors, included and excluded instruments and months, and
the original source family counts. A support-filtered mean is never labelled the
mean of all original signals, and the raw valid target summaries before matching
are shown separately.

For a supported stratum where E is a subset of C, the artifact publishes
`nE_s/nC_s` and the attenuation identity

```text
lift_s = (1 - nE_s/nC_s) * disjoint_lift_s
```

when that complement is nonempty. This is a **stratum-level** identity: different
stratum shares are exactly why one pooled attenuation factor may not be applied
to the weighted aggregate, and no additional unplanned contrast is estimated.

Ticker and month diagnostics use the same retained stratum definition. The
equal-ticker descriptive companion averages defined per-ticker estimates, where
each ticker's own months use its target-count weights; a missing ticker is
omitted with its denominator disclosed, never assigned zero. These diagnostics
carry **no** p-value and no significance badge.

A per-case fingerprint of the retained target anchors and their support is
stored. Fingerprints are compared within one comparison, model instance,
timeframe and direction across horizons: when they differ, the report states
that those horizon rows use different populations even where the counts happen
to agree. There is no automatic common-support retrimming. Occurrence variants
that share one condition also share its known-false control population; those
comparisons are named and are not independent tests.

### One statistical method, with an explicit approximation

The estimand is the supported, event-weighted **mean net-return difference**
above. It is not causation, an equity curve, a random-direction strategy or
every possible use of the pattern. Matching is conditional on instrument and UTC
month only; it does not remove every market regime or historical
universe-selection bias.

Inference is a calendar block bootstrap of the **joint linearized estimator**,
computed from daily counts and sums. It includes control uncertainty, changing
event weights and their covariance, without materializing bar tables repeatedly
and without bootstrap ratios that can have empty control denominators. It is an
asymptotic approximation that requires weak dependence, adequate moments and
support and a reasonably stable centered influence process. Seven days is a
declared research setting, not a guarantee against arbitrary long memory or
regime change.

Saved results carry `inference_scope="approximate_development_screen"`, and the
qualification is visible next to every inferential output. More calendar days do
not prove those assumptions, and this build does **not** assert that a 7-day
block controls error under long dependence; see
[known limits](#milestone-handoff-and-known-limits). Holm cannot repair an
invalid or anti-conservative individual p-value. Nothing here advertises
guaranteed 5% family-wise error or an independently validated edge.

**The calendar grid** is every UTC day intersecting the source study interval,
sorted, including days with no eligible observation. Precisely, day `d` is
included iff `[d, d+1 day)` intersects the half-open study interval, so
`[2025-07-01, 2026-07-01)` contains **365** days, not 366. Partial boundary days
stay explicit. A missing contribution means count and sum zero for aggregation
only: it fabricates no valid market bar and no zero-return observation, and
zero-contribution padding is never counted as supported inference history. The
same day grid and the same resampled day indices serve all instruments and all
comparisons.

For each retained stratum and day the artifact saves `e_sd`, `c_sd` (counts),
`a_sd`, `b_sd` (net-return sums), the gross sums and the overlap counts.
Outside a stratum's month its contribution is zero, and the table is stored in
compact sparse form with that rule and the complete grid recorded beside it.
Excluded stratum diagnostics are preserved as well.

**Joint daily influence contributions.** For each calendar day `d`:

```text
uE_d = [sum_s a_sd - signal * sum_s e_sd] / N

uC_d = sum_s [
           (muC_s - control) * e_sd
         + (nE_s / nC_s) * (b_sd - muC_s * c_sd)
       ] / N

uD_d = uE_d - uC_d
```

Each vector sums to zero up to floating-point error; its tiny empirical mean is
subtracted before bootstrap evaluation, and a residual that is material against
the absolute contributions that produced it is an implementation failure rather
than something centering conceals. Counts stay exact integers and sums are
float64. The `e_sd` terms matter: omitting them silently freezes the estimated
event weights. Overlapping E and C observations contribute to both vectors on
the same day, preserving their covariance.

**Resampling.** Method `calendar_score_cbb_v1` uses a fixed-length circular
block bootstrap **of the day vectors**, not circular shifting of a signal mask.
With `T` days, `L=7` and `K=ceil(T/L)`: each replicate independently draws `K`
starts uniformly from `[0,T)`, expands each to `L` consecutive indices modulo
`T`, concatenates them and keeps the first `T`. That *same* index sequence is
applied to every instrument's joint contributions and to every comparison —
there is no per-ticker, per-side, per-event or per-case independent resampling.
For each estimator, `z_b = sum_d u[index_b[d]]` is the centered bootstrap error;
it is not divided by `T` again, because the contributions already include `N`.

Draws come from `numpy.random.Generator(numpy.random.PCG64(seed))` in a stable
**replicate-major** order, so processing replicates in bounded batches never
changes the drawn sequence. The implementation uses block prefix sums over two
concatenated copies of the day vector; the tests keep a slow index-based oracle.
No `resamples × anchors × cases` tensor is materialized and no replicate
intermediate is written to disk.

**Intervals and the p-value.** For each of signal, control and lift the 95%
pointwise **basic** interval is

```text
[theta - quantile(z, 0.975), theta - quantile(z, 0.025)]
```

with NumPy's linear quantile convention. Intervals may be asymmetric and are
never recentred by a normal approximation. They are **not** simultaneous
Holm-adjusted intervals.

Only lift is tested, against `H0: lift = 0`, two-sided:

```text
p_upper = (1 + count(z_D >= observed_lift)) / (B + 1)
p_lower = (1 + count(z_D <= observed_lift)) / (B + 1)
p_raw   = min(1, 2 * min(p_upper, p_lower))
```

Ties count inclusively. The finite-draw correction avoids a zero p-value; it
does **not** make bootstrap inference exact. `B`, the seed, the method version
and the minimum two-sided resolution `2/(B+1)` are all recorded. Net
profitability is not inferred from the lift p-value, and no gross-return,
MFE/MAE or subgroup p-value is reported.

### Inference availability and the declared family correction

Beyond the retained-stratum requirements, an inferential result additionally
requires all of:

| Gate | Requirement |
| --- | --- |
| Day grid and supported span | `T >= 336` **and** `supported_span_days >= 336` |
| Joint active days | `joint_active_days >= 252` |
| Supported blocks | `supported_blocks >= 48` |
| Retained coverage | `>= 80%` of valid target observations on common condition and outcome availability retained after stratum support exclusions |
| Horizon ceiling | case horizon `<= 8 hours` |
| Bootstrap variation | finite and nondegenerate for the tested contrast |

A **joint active day** has at least one retained E and one retained C
observation in the pooled population. `supported_span_days` is the inclusive
calendar distance between the first and last joint active days, and zero when
there are none. The full calendar grid is partitioned into non-overlapping
7-day bins anchored at its first UTC day; `supported_blocks` counts **full**
seven-day bins with at least 4 joint active days, and an incomplete last bin is
not one. These are coverage diagnostics, not independent-sample-size estimates.
Neither extra tickers nor empty padded days may turn an 84- or 180-day
population into a near-year inferential claim, and the gates are not
stationarity tests.

There is **no** minimum ticker count: an explicitly selected single ticker is a
valid population with that scope named, and a day count is never multiplied by a
ticker count and called independent support.

`T`, `L`, `K_draw = ceil(T/L)`, `supported_span_days`, `joint_active_days`,
`supported_blocks` and the retained months and counts are published for every
member. **`K_draw` is the number of bootstrap block draws, not the amount of
supported data.** A sparse near-year population that fails any gate keeps its
point estimates and descriptive results with null inferential fields. There is
no warnings-only bypass.

Reason codes are explicit and every applicable reason is reported in a fixed
order: `no_matched_support`, `insufficient_span`, `insufficient_active_days`,
`insufficient_supported_blocks`, `insufficient_retained_coverage`,
`unsupported_inference_horizon`, `degenerate_contrast`.

A constant contrast — including identical E and C — keeps its point estimate and
gets no inferential claim. The numerical rule is fixed: with
`sigma = std(z_D, ddof=1)` and
`scale = sqrt(T) * max(max|uE|, max|uC|, max|uD|)` over the centered vectors, the
contrast is degenerate when every `z_D` is identical or
`sigma <= 128 * finfo(float64).eps * scale`. This detects cancellation relative
to the **component** uncertainty, not merely relative to the already cancelled
`uD`. There is no fixed economic epsilon, no jitter and no variance floor. Each
computed bootstrap records its standard deviation and both interval quantiles
for all three estimators; an unsupported member keeps null bootstrap diagnostics
while its coverage geometry stays present.

**One family** comprises all resolved comparison, model, timeframe and case lift
tests frozen above. Both directions remain explicit members; there is no
mirrored-direction deduplication. Unavailable members stay in the family size
`m` with `p = 1` used internally for the adjustment and null inferential fields
in the output. No member is removed after seeing support, effects or p-values.

Holm sorts by `(p, stable_id)`; for rank `i` from 1 the adjusted value is the
running maximum of `(m - i + 1) * p_i` clipped to 1, then mapped back to the
stable IDs. An available member has `nominal_reject_holm=true` only when its
adjusted `p <= 0.05`. The sign of a detected difference is labelled: a negative
difference is not a positive edge. The artifact states whether
`2/(B+1) > 0.05/m` prevents any first rejection; `B` is never increased
silently.

There is no automatic practical-effect threshold, practical rejection verdict,
candidate promotion or winner sorting. A non-rejection means insufficient
evidence for that test, not proof that the effect is absent. Holm covers only
this declared family, never an agent's unrecorded adaptive search across earlier
runs.

### The sealed analysis artifact

The analysis writes into its own output root and never mutates an input study
file, including that study's derived report.

```text
request.json          normalized analysis request
family.json           resolved comparisons/cases/method/support constants
source.json           source study identities, completion and evidence-set hashes
strata.parquet        included/excluded strata and support/point-estimate facts
daily.parquet         counts/sums sufficient to reproduce the estimator
summary.json          immutable result rows, intervals, p-values and disclosures
provenance.json       paths, environment, timings and implementation attribution
status.json           terminal state and planned/processed comparison counts
completion.json       validated hash manifest, published last
derived/report.html   regenerable; outside the immutable hash set
```

Each artifact and table carries a documented schema version, stable keys, units
and availability rules. Strict JSON uses `null` rather than NaN or Infinity. The
existing serialization and path helpers are reused, but the completion record
carries its own `analysis_schema_version` and `artifact="pattern_lab_analysis"`
so it can never masquerade as, or be validated as, an M2 study completion
record.

The artifact is bound to the source's exact completion and evidence-set hashes
and to its specification, data and implementation identities. It also records a
**semantic analysis identity** over the canonical request, the family, the
method and the source's semantic inputs, and an **analysis implementation
digest** of the modules actually used — including the shared expansion and
statistic code, explicitly `study/builtins.py`, `study/observations.py` and the
resolved `EVIDENCE_VIEW_VERSION`. Paths, labels, wall-clock timings and worker
provenance are physical metadata, not numerical identity. Dependency versions are
recorded; bitwise cross-platform equality is not claimed universally.

Source runs from `workers=1` and `workers=2` may have different physical
evidence hashes while their semantic identities and numerical results are
identical; both facts are preserved. A relocated source or analysis stays
readable, because paths are recorded provenance and not integrity proofs. Saved
source code is inert provenance and is never imported by this workflow.

The admitted source binding is verified **again** before final publication: if
its immutable evidence changed while it was being read, the analysis fails
without a completion seal. Cooperative immutable input is assumed; this is not
an adversarial filesystem security model, and no lock or recovery journal is
added to an otherwise immutable study run.

Publication uses atomic writes with the completion record last. A normal failure
or interrupt leaves an honest `failed` or `interrupted` status and no valid
completion, and the original cause survives even when the status write also
fails. An output directory this operation already created may remain
inspectable, but there is **no** automatic retry, overwrite or resume: a failed
output root cannot be reused, so choose a new root or remove the failed artifact
explicitly. The completion record is the point of no return: once it has been
atomically published, a later exception never rewrites that sealed status, and a
sealed successful analysis is never retroactively failed by a report-regeneration
exception.

`load_analysis` and `analysis-report` verify the complete versioned seal and its
file hashes before trusting a result or rewriting the HTML, and they validate
counts, identities and result/family membership agreement rather than matching
bytes alone. An unsealed, truncated or inconsistent artifact is rejected before
any derived file is altered. There is no partial-inference bypass.

### The offline analysis report

`derived/report.html` is a standalone light-theme page with no CDN, no network
dependency and no required JavaScript package: escaped HTML tables only. It
shows the source identities and interval, the analysis family and method
settings; signal and control net means, net lift, gross means and the fee
assumption; support, exclusion and overlap counts, inclusive-parent shares, the
retained target share, per-member calendar and block geometry and
differing-horizon support; pointwise intervals, raw and adjusted lift p-values
and availability reasons; every declared case with primary-horizon emphasis that
never hides the alternatives; ticker and month descriptive diagnostics with the
equal-ticker companion; and explicit exploratory, overlapping-observation and
method-assumption disclosures. Nominal-level labels, the approximation
qualification and the documented long-dependence limitation sit adjacent to the
inference table. Commission is included; funding and slippage are excluded, and
no equity or profit claim appears.

Artifact completion and statistical availability are distinguished. All labels,
IDs and notes are escaped. The default order is the canonical family, not best
return or smallest p.

The banner, the approximation qualification and the long-dependence limitation
are rendered by the **current** renderer, while the disclosure list is the
artifact's own saved text. Regenerating an older sealed artifact therefore shows
corrected current wording beside its historical saved disclosures. That is
provenance, not a contradiction: the sealed bytes are never rewritten, and
regeneration still needs only the artifact — no research script, no saved module
and no market data.

### Memory, performance and the extension boundary

One pure numerical entry point, `analysis.evaluate_observations`, accepts
per-observation aligned records — one table or an iterable of per-instrument
tables — with the closed column set

```text
member_id, instrument_id, signal_time_ms, is_target, is_control,
available, net_return, gross_return, return_valid
```

plus the frozen family, the declared instruments and the study interval. It runs
the **same production** calendar stratification, support exclusions, weighting,
daily aggregation, joint influence, inference gates, bootstrap and Holm, and
returns numeric results and compact tables. It publishes no artifact and
certifies no source provenance. Dimensions, exact key types, masks and finite
valid returns are validated at this boundary; an invalid row must be null and is
never silently removed.

`run_analysis` obtains those records through its checked evidence alignment and
calls this function, so a large calibration repetition enters here rather than
at precomputed means, selected strata, daily sums or influence vectors. Lower
level helpers remain suitable as unit-test oracles but never substitute for
end-to-end numerical calibration.

Compact day and stratum aggregates are retained and bootstrap replicates are
processed in bounded batches. Memory may scale with the declared family and the
daily evidence, and that cost is honest: there is no `O(B × bars × cases)`
tensor, no per-replicate Parquet scan, no repeated hypothesis computation, no
worker retry and no hidden parallel layer. M2's workers remain available for
generating studies; offline M3a aggregation is a vectorized coordinator-side
calculation and takes **no** worker-count option.

Agent scripts can read the saved tables and add their own descriptive metrics
and sorts without editing inference internals. An unsupported inferential model
fails explicitly. The comparison aggregation and numerical functions stay
reusable for a later model adapter, but no speculative interface and no
arbitrary inferential plugin system is provided. Source-study report behavior is
descriptive and backward compatible.

### Calibration

The declared calibration experiments live in
`tools.pattern_lab.analysis.calibration` and run through
`evaluate_observations`, so every repetition exercises the production path:

```bash
python -m tools.pattern_lab.analysis.calibration --output-root EXTERNAL_TEMP_DIR
```

The module freezes every generator setting — a 30m grid, four instruments,
`SeedSequence([20260920, scenario_id, repetition_id, stream_id])` with stream 0
for data and stream 1 for the bootstrap seed, actual UTC calendar months, the
common and individual factor loadings, the daily AR(1) coefficient and level
scale, the bar innovation scale and law, the signal chain transition
probabilities and their stationary initialization, and the bar-level target
probabilities — and records them, with their digest, beside the results.

The generators produce **synthetic outcome and mask records**, not coherent OHLC
backtests: a horizon outcome is an additive path sum and the per-observation
cost is a constant, which leaves the tested lift unchanged because a constant
cancels in a difference of means. No distributional claim about real markets
follows from any setting.

Acceptance is an **empirical screen**, not a test that the true error equals 5%.
For each admitted scenario the raw primary rejection rate, the primary
nominal-95% interval noncoverage and the nominal Holm family-wise rejection rate
are published with Wilson 95% intervals and exact denominators, and each needs a
one-sided 95% exact binomial upper bound at or below **0.08**. That ceiling is a
declared maximum empirical error envelope on these fixtures — **not** a new test
alpha, a 92% interval or a certification of exact 5% control. Alpha stays 0.05
and the reported intervals stay nominally 95%. The checks are an intersection of
individual bounds and are not jointly 95%. At least 95% primary inferential
availability is required, and every refusal reason is reported, because refusal
cannot manufacture a passing rate.

#### Rate checks, run completeness and the release gate

Passing rate checks are not acceptance. `score_acceptance` scores exactly the
admitted records it is handed and publishes
`all_requested_checks_passed` with `scope="requested_subset"`: one passing
fixture, or the same fixture supplied twice, legitimately satisfies it. The
release decision belongs to `legacy_protocol_state`, the versioned gate of the
named **`legacy_bootstrap_v1`** protocol, which answers three separate questions
and never collapses them:

| Field | Meaning |
| --- | --- |
| `diagnostic_checks_passed` | Every rate and availability check of the records actually present passed |
| `complete_run` | This document is a complete, eligible run of the named protocol |
| `accepted` | A complete run whose checks passed, with `accepted_reasons` otherwise |

Eligibility is bound to the **driver**, never to result-supplied names, counts or
`admitted` flags. `LEGACY_PROTOCOL_SCENARIOS` pins the protocol's fourteen
fixtures in their original order and is also the CLI default, so a fixture added
to the registry later changes neither. `build_run_plan` resolves those names into
the exact entries the run executes, listing the padded 365-day refusal variants
the driver generates itself. Each executed entry carries an **attempt ledger**
with its fixture ID, padded or unpadded variant, configuration digest, declared
and requested repetition counts, resamples, and the attempted and completed
repetition IDs as compressed ranges. The gate requires every planned entry
exactly once at its declared configuration and contiguous ID block, self-
consistent counters and denominators, actual refusal from the refusal fixtures,
and the required evidence and state-entry replays with their declared count and
agreement. Stress and planted fixtures are required disclosures whose rates sit
outside the admitted-null ceiling by design; they are not extra error checks and
the planted results are not a power gate.

Any subset, any `--repetitions` smoke override, an omitted or disagreeing replay
and any unfinished run are **diagnostic-only** and can never return `accepted`.
Exit status 0 means complete acceptance of this named protocol and nothing more;
exit 2 is every other completed run and **deliberately does not distinguish** a
diagnostic run from a statistical failure — read `complete_run`,
`diagnostic_checks_passed` and the two reason lists in the JSON.

`CALIBRATION_SCHEMA_VERSION` is **2**, because the release-facing Boolean changed
meaning and a complete run now carries a run plan and attempt ledgers. Saved
schema-v1 documents stay readable historical records: they carry no ledger, so
they are ineligible under this contract, they are never rewritten, and they gain
no retrospective verified status. The delivered T05 document is ineligible on
that ground **and**, independently, fails its own rate checks 11 of 15. Its
`evidence_replay` and `state_entry_replay` fields are null, so that release run
never embedded the replay; separately retained replay evidence is not proof that
no replay was ever performed and is not injected into the saved document.

**The delivered run passed 11 of those 15 checks.** Availability was 100% on
every admitted scenario and all five Holm family-wise rates passed (2.55-2.85%).
The independent scenario (5.30%) and the conditional-confounding scenario
(5.20%) are at the nominal level. The two scenarios whose generator combines
AR(1) daily factors with a **persistent daily signal-state chain** failed: the
dependent Student-t scenario measured 8.05% rejection and 8.30% noncoverage
(bounds 9.12% and 9.39%), and the 336-day admission-boundary scenario measured
7.45% and 7.65% (bounds 8.49% and 8.70%). The cause is still under
investigation. One measured scale diagnostic of that same 2000-repetition run:
the bootstrap-implied **standard error** (mean interval width / (2 x 1.96)) is
about 0.99 of the empirical **standard deviation** of the primary lift across
that scenario's own repetitions on the two iid-signal scenarios — including the
one with AR(1) daily returns — and about 0.90 on the two failing
persistent-signal scenarios. Those are standard-deviation ratios on the named
scenarios' own repetitions; they are neither a variance factor nor an
established cause. **M3a's statistical acceptance is therefore open**; the
method needs a reviewed revision before its inferential output can be treated
as an accepted screen.

A bounded follow-up compared estimated variance with the true variance under a
**known synthetic generator** on scenarios 1, 2, 3, 4, 5 and 12 at 400 repetitions
each. Conditional on the realized masks, availability and retained strata, the matched estimate is a
fixed linear combination of the generator's bar returns, so its exact sampling
variance is computable on those fixtures. That is a **synthetic oracle**: it needs
the data-generating process and is never available on market data. Against it, the
exact calendar-block variance of the production influence vector averaged
**0.97, 0.87, 0.97, 0.94, 0.86 and 0.87** of the true variance on scenarios 1, 2,
3, 4, 5 and 12. These are **means of variance ratios**, not the standard-deviation
ratios above; taking their square roots does not reproduce that earlier statistic.
Using the generator's known means in the daily reference removes only part of
each average deficit. On scenarios 2, 5 and 12, the fitted-score and
block-calculation discrepancies are each about half of the deficit and are tied
at 400 repetitions. These six fixtures differ in signal persistence, matching
shape and innovation law at the same time, so they isolate no single cause.
The count of named fitted mean terms (2 + retained strata) alone does not explain
the differences and is **not** a degrees-of-freedom correction. It does not rule
out an effect of estimating means from dependent observations or of their
alignment with daily count patterns. These are descriptive diagnostics at
that repetition count: they are not a universal undercoverage factor, not a
calibrated inflation, not a correction, and they neither revise the delivered
2000-repetition result nor accept M3a.

#### Versioned fixtures: the confounded null

Scenario 3, `null_conditional_confounded`, assigns its event probability from the
**anchor's own bar month**, while matching owns the **signal-close** month. The
23:30 anchor of a month's last day signals at 00:00 in the next month, so 44
anchors per repetition carry the previous month's probability and the fixture's
population null is not exactly zero. At the 240m primary horizon the exact
row-mixture contrast is at most `4.1254148207e-08` per stratum and
`1.4622196520e-08` event-weighted, against a per-repetition lift standard
deviation of order `1e-4`. The fixture, its draws and its original evidence are
retained unchanged; its `caveat` field carries this limitation with every record
that reports it. **The caveat qualifies the interpretation of a rate; it is never
a waiver for a failed rate.**

Fixture **101**, `null_confounded_signal_month_v2`, is the corrected version. It
keeps scenario 3's return law, comparison, calendar, support, family and
generator parameters and assigns the target-event probability by the anchor's
signal-close UTC month, so the probability is constant inside every actual
matching stratum and the deterministic row-mixture target and control population
means agree to roundoff (at most `3.5e-18` at the 480m horizon). The
deterministic return-mean schedule keeps its original **bar-time** ownership, and
production UTC ownership is unchanged. Its seed is keyed on fixture ID 101, so it
is an independent realization of that law rather than a paired correction of
scenario 3's draws, and rate comparisons between the two are unpaired.

Fixture 101 is **not** part of `LEGACY_PROTOCOL_SCENARIOS`: adding it left the
legacy command's run, its gate and its numerical outputs untouched. It does
change the scenario list embedded in `generator_contract`, and therefore that
contract's `generator_digest`, for every run including legacy ones; legacy
equivalence is demonstrated by comparing the legacy generators' actual masks,
returns, availability and results, never by digest equality.

#### The experimental monthly-jackknife candidate

`tools.pattern_lab.analysis.calibration_monthly` holds one **research-only**
experimental candidate, `monthly_cluster_jackknife_v1`, and the driver that
decides it against the unchanged acceptance contract above. It is reachable
only from its own command. No analysis request, method default, sealed artifact
schema or HTML rendering exposes it, the existing seven-day block bootstrap
remains the implemented — and explicitly unvalidated — production method, and no
outcome of this experiment adopts a method, accepts M3a or starts M3b.

```bash
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
python -m tools.pattern_lab.analysis.calibration_monthly --output-root EXTERNAL_TEMP_DIR
python -m tools.pattern_lab.analysis.calibration_monthly --output-root EXTERNAL_TEMP_DIR \
    --summarize-only
```

The first command runs the frozen matrix; the second rebuilds `summary.json` and
`summary.md` from the saved manifest and records alone, generating no data and
running no inference. Exit 0 is a PASS of the synthetic contract; exit 2 covers
both FAIL and INCOMPLETE, which the JSON names explicitly.

**The method.** For each family member the candidate keeps the production
retained instrument x signal-month strata, their target and control counts
`e_s`, `c_s` and their net sums `a_s`, `b_s`, with `r_s = e_s/c_s`. It aggregates
them to whole signal months,

```text
N_m = sum_{s in m} e_s
Q_m = [sum a_s, sum r_s b_s, sum (a_s - r_s b_s)]
N   = sum_m N_m            theta = sum_m Q_m / N
```

where `theta` is (signal, matched control, lift) and equals the production point
estimate. With `G` informative months it deletes each month in turn,

```text
theta_(-m) = (sum_l Q_l - Q_m) / (N - N_m)
V_J        = (G-1)/G * sum_m outer(theta_(-m) - mean, theta_(-m) - mean)
CI_j       = theta_j +/- t(.975, G-1) * sqrt(V_J[j,j])
p_lift     = 2 * t_survival(|theta_lift| / SE_lift, G-1)
```

and reports the **full-sample** `theta`, never the deletion average. Holm uses
the candidate's own p-values over the entire unchanged declared family with the
existing deterministic tie handling; unavailable members keep internal `p = 1`.
No seed and no resample count governs this formula.

`V_J` is positive semidefinite and **singular by construction**, because the lift
column is the signal column minus the matched-control column. Only its diagonal
is used: it is never inverted, Cholesky-factored or ridged, and its off-diagonal
entries do not add an independent dimension. The `t` reference is an explicit
approximation — months need not be independent in real data. The monthly
cancellation removes the need to estimate a fitted within-stratum imbalance term
at a block scale smaller than its own stratum; it does **not** remove
control-mean uncertainty, which stays in the variation of `A_m`.

**Support and validity.** The existing full-sample strata, span, active-day,
block, retained-coverage and horizon gates are applied once, by the shared
accumulation, and are never reapplied to a deleted-month sample. Partial
endpoint months, unequal counts and outcomes crossing month boundaries are
preserved, and each outcome belongs to its signal month; there is no equal-month
reweighting, trimming, post-hoc concentration gate or selection of successful
months. On top of them the candidate adds only mathematical validity rules: at
least two informative months, a positive remaining target count after every
deletion, finite inputs and results, and a nondegenerate contrast under the fixed
float64 rule

```text
mass          = sum_s (|a_s| + |r_s b_s|) / N
amplification = max_m N / (N - N_m)
scale_lift    = max(|theta_lift|, mass * amplification)
degenerate   <=>  SE_lift <= 128 * finfo(float64).eps * scale_lift
```

which uses pre-cancellation component mass and the deletion-denominator
amplification rather than already-cancelled sums. An exact zero stays unavailable
even when the threshold is zero. Refusals publish explicit reasons and nulls,
never an epsilon-filled SE, a NaN or `p = 0`; a non-finite input is a correctness
error and raises, rather than hiding in availability.

**The shared pre-inference stage.** `estimator.accumulate_observations` is the
one owner of accumulation, matching, support and the per-member point estimates.
`evaluate_observations` keeps its exact signature, behaviour, table schemas and
numerical results and now continues from that object into the bootstrap, Holm and
the published tables; the candidate consumes the same object. Candidate
availability is therefore decided **before** any resampling and can never depend
on a sampled bootstrap degeneracy. Editing `estimator.py` changes
`artifacts.ATTRIBUTED_MODULES` digests and the implementation provenance of
future sealed artifacts. That is expected: `artifacts.semantic_identity`
deliberately excludes the implementation digest, so analysis semantic identity is
preserved, and numerical equivalence is demonstrated by comparing results on
fixed synthetic sources rather than by preserving an obsolete hash.

**Fixture 102.** `null_causal_ohlc_v1` is a coherent synthetic candle null that
the candidate module owns, because it produces OHLCV and runs the real study job
rather than outcome records. Four instruments on a 30-minute grid cover the same
365-day research year as fixture 1 with seven preceding warmup days. Each bar
draws a shared Rademacher sign `C` and per-instrument signs `E_i`, giving the
simple return `r_i = 0.001 * (0.8*C + 0.6*E_i)`; the initial open is 100, the
close is `open*(1+r)`, the next open is the previous close, and
`high = max(open,close)*1.0002`, `low = min(open,close)*0.9998`. Quote volume is
`exp(0.25*Z_i)` with `Z_i` standard normal and independent of the return
innovations. The RNG order is pinned: PCG64 from `SeedSequence([20260920, 102,
repetition_id, 0])` draws `C` for every chronological warmup and study bar first,
then, in the frozen instrument order, each instrument's `E_i` signs followed by
its `Z_i` normals.

Because `|0.8| > |0.6|` and both draws are +/-1, a bar is green exactly when
`C = +1`: the two-green component of the condition is **identical on all four
instruments by construction**. The fixture therefore deliberately tests a fully
common price signal with strong cross-sectional event clustering; the independent
volume filter still gives distinct event masks and the return magnitudes still
differ. Common signs do not invalidate the null. Rademacher innovations are
bounded, which is what guarantees strictly positive price paths; Gaussian return
innovations would lose that guarantee.

The fixture runs the actual built-in two-green/rising-volume condition in
occurrence mode, the existing nonsignal comparison, the next-open fixed-horizon
model and the real price-ratio fee expansion, with
`commission_pct_per_side = 0.05` pinned in both the model settings and the
family metadata — `f = 0.0005` as a fraction. The legacy record families keep
their zero commission. Entry is `open[i+1] = close[i]` and exit is `close[i+k]`,
so the gross ratio has conditional mean one given the past and the expected fee
is `2f`; the condition is measurable with respect to the past, so the target and
control populations have the same zero expected net difference. This is a
coherent population-null construction, not a claim that a finite
random-denominator estimate, or an expectation conditional on the whole
endogenous mask, is exactly zero: the exogenous-mask oracle of the record
fixtures does not apply here.

A small research-local in-memory table provider replaces only the verified table
read, so `InstrumentReader`, the shared case expansion, the checked condition and
emission alignment, the all-anchor case view and the membership mapping are the
production ones. A bounded two-case disk replay writes the same evidence as
Parquet, reads it back through the normal reader and requires identical results.

**The frozen matrix.** Master seed 20260920, repetition IDs 20000-21999 only,
eight required fixtures of 2,000 attempts each in the fixed order 2, 5, 1, 4, 3,
12, 101, 102 — at most 16,000 main attempts, one candidate and no second formula.
All family members are computed on every attempt. After the main matrix passes,
the refusal fixtures 10 and 11 with their padded variants, the long-dependence
fixtures 20 and 21 and the planted fixtures 30, 31 and 32 run at their original
declared counts with fresh IDs from 20000, followed by the bounded replay and
adapter checks. Every setting — the formula, both generator contracts and their
digests, the numerical rules, the support contract, the family and primary
definitions, the ordering, the seeds and the decision criteria — is written to
`manifest.json` with its own digest **before** any outcome is inspected.

The experiment has a three-hour wall-clock and 1 GiB process-RSS budget,
measured rather than assumed, with a fixed five-attempt pilot per required
fixture whose completed records are reused in the final matrix. Host available
RAM and swap are read first; on a small host the ceiling is tightened to leave a
system reserve, which never loosens the declared limit. A sampled RSS reading is
a guard, not an OS-enforced allocation limit. Reaching the ceiling, inadequate
headroom or a projected cost that cannot fit stops the run as **INCOMPLETE**
rather than reducing attempts; a budget stop is not a statistical rejection.
Statistical early stopping is allowed only where success is impossible: 140
errors in any required rate cannot pass at the largest denominator 2,000, and 101
unavailable primary attempts cannot meet the availability floor. There is no
early acceptance.

**Reading the result.** The candidate's interval and test are an exact inversion
of one another, so on a zero-truth fixture raw rejection and nominal-95%
noncoverage are the same event apart from boundary conventions; both columns are
kept for compatibility, and the 24 displayed rate checks are not 24 independent
confirmations. Constant-cost long and short lifts are exact sign mirrors with
equal candidate p-values, and fixture 102's fixed proportional fee makes them
near-exact mirrors with `lift_short = -(1+f)/(1-f) * lift_long` and an SE scaled
by the same positive factor. Holm over tied pairs is conservative, so the
family-wise rate is limited global-null evidence rather than proof of strong
family-wise control. Legacy fixture 3 is a hard compatibility gate whose
documented tiny population-null mismatch qualifies its number without waiving it.
Refusal, long-dependence and planted fixtures are required disclosures whose
rates are never added to the admitted-null envelope.

The 252 active-day support gate implies at least nine occupied calendar months,
so a `G = 2` case is mathematically valid but is not admitted inference, and that
lower bound does not prove that every `G >= 9` population passes the other
support gates. **The frozen matrix covers `G = 12` only.** A PASS therefore
supplies no calibration evidence for other month counts or for all admitted
support geometries, and broader production adoption remains a later decision.

**The delivered decision: PASS on this synthetic contract.** The frozen matrix
ran once, on repetition IDs 20000-21999, in 2.04 hours against the three-hour
budget with a 352 MB peak RSS. All eight required fixtures completed their 2,000
attempts and passed availability and all three rate checks, so the 24 required
rate checks passed — the original 15 and the nine supplementary ones, kept
separately visible and not pooled.

| Fixture | Availability | Raw rejection (95% UB) | Noncoverage (95% UB) | Holm FWER (95% UB) | Mean SE / empirical SD |
| --- | ---: | --- | --- | --- | ---: |
| 002 `null_dependent_t5` | 100.00% | 5.70% (6.63%) | 5.70% (6.63%) | 2.05% (2.65%) | 0.954 |
| 005 `null_admission_boundary_336` | 100.00% | 4.65% (5.50%) | 4.65% (5.50%) | 1.55% (2.09%) | 0.989 |
| 001 `null_independent` | 100.00% | 5.25% (6.15%) | 5.25% (6.15%) | 2.40% (3.04%) | 0.992 |
| 004 `null_inclusive_parent` | 99.95% | 5.25% (6.15%) | 5.25% (6.15%) | 1.90% (2.48%) | 0.962 |
| 003 `null_conditional_confounded` | 100.00% | 5.00% (5.88%) | 5.00% (5.88%) | 1.75% (2.31%) | 0.992 |
| 012 `null_dependent_gaussian_companion` | 100.00% | 4.15% (4.96%) | 4.15% (4.96%) | 0.90% (1.33%) | 0.990 |
| 101 `null_confounded_signal_month_v2` | 100.00% | 4.60% (5.45%) | 4.60% (5.45%) | 1.95% (2.54%) | 0.974 |
| 102 `null_causal_ohlc_v1` | 100.00% | 5.60% (6.52%) | 5.60% (6.52%) | 2.35% (2.99%) | 0.982 |

Every fixture measured `G = 12` informative months, so **only twelve clusters are
calibrated by this run**. The two rate columns coincide exactly on every fixture,
as the interval/test duality requires, and each long/short pair produced
identical marginal rates, as the mirror property requires; neither doubling is
independent evidence. The longest 480-minute horizon of fixture 102 measured a
5.95% marginal raw rejection rate, the highest of its eight members.

The required disclosures behaved as declared and are **outside** the gate. All
four refusal runs, including both zero-contribution-padded variants, published no
p-value and no interval on any of their 200 attempts. The long-dependence
fixtures at daily AR 0.9 measured 3.60% and 4.50% raw rejection at signal
persistence 0.70 and 0.97, against the 8.3% and 11.1% the tracked long-dependence
experiment measured for the seven-day bootstrap on the same two fixture
definitions and the same rate. Those runs used different repetition IDs, so the
pair is unpaired rather than a matched comparison. Either way this is a
disclosure about these two fixtures, **not** a claim that the candidate controls
error under long memory. The strong planted effects
were detected in 100% of attempts and the modest one in 5.00%; those are wiring
checks, not a power result. Both bounded checks agreed: the fixture-102 disk
replay to `0.0` and the checked-joins replay to `3.0e-15`.

**What this PASS does and does not mean.** It means the candidate met the
unchanged declared synthetic screen that the seven-day block bootstrap failed
11-of-15, on eight fixtures at twelve monthly clusters, and that on each
fixture's own repetitions its mean standard error is 0.954-0.992 of the
empirical spread of the primary lift. That last figure is **not** a paired
comparison with the delivered bootstrap run: that run predates the
`bootstrap_scale` field and published no direct standard-error ratio, only an
interval-implied one — mean width / (2 x 1.96) — of about 0.99 on the iid-signal
scenarios and about 0.90 on the two persistent-signal ones. Those are different
statistics on different methods, so they indicate a direction and nothing
sharper. The PASS is **not** adoption, not a validated market edge, not error
control under dependence longer than these fixtures', and not calibration for
any other month count or support geometry. Integrating the method is a separate,
later decision; at this commit the production screen is still the bootstrap and
**M3a's statistical acceptance gate remains open**.

Alongside the acceptance scenarios the driver runs short-population refusal
checks at 84 and 180 days — including the same records embedded in a 365-day grid with
zero-contribution padding, which must stay unavailable — a **required
long-dependence limitation experiment** at daily AR 0.9 with two signal
persistences, planted strong and modest effects, a default-`B` smoke, and a
bounded replay that routes fixed repetitions through the production checked
joins as evidence-shaped frames and compares them with the numerical boundary.
The long-dependence results sit **outside** the admitted-null envelope: they are
a disclosed limitation, not a claim that a seven-day method handles long memory,
and the software does not detect such dependence automatically in a real run.

Each repetition also retains the primary lift's **already computed** bootstrap
standard deviation and its two error-distribution quantiles; all three are null
when primary inference is unavailable, including a degenerate contrast. No second
bootstrap runs and no extra random draw is consumed. Each scenario record publishes a
`bootstrap_scale` block: the diagnostic count, the empirical standard deviation
of the available-primary lifts, the mean and RMS bootstrap standard deviation,
and the mean-bootstrap-SD / empirical-SD ratio. Numerator and denominator use
exactly the same available samples, and the block reports its own count and
scope because the existing `effect.*` fields keep their own wider population of
every repetition with a non-null lift. An undefined ratio is published as null
with its count. These are standard-deviation diagnostics, never variance
factors. They were introduced additively at `CALIBRATION_SCHEMA_VERSION` `1`,
which changed no existing field's meaning; the version moved to `2` later, for
the acceptance semantics above. Read these diagnostic fields,
including `bootstrap_scale`, as optional: records saved before their introduction
do not contain them, and the same applies to `ledger` and `caveat` in schema-v1
documents.

The direct bootstrap-SD ratio differs from the interval-implied SE ratio above.
Both depend on the sampled repetitions and their empirical SD; shorter runs have
greater sampling uncertainty, especially with heavy tails. Compare the statistic,
scenario and available population, and record the repetition count and seeds.
A different ratio in a smaller experiment does not revise the delivered
2000-repetition result or imply that empirical SD must increase with sample size.

### Reading the saved tables

| Helper | Contents |
| --- | --- |
| `comparisons()` | One row per family member: estimates, interval endpoints, raw and Holm-adjusted p-values, the nominal rejection flag, availability reasons and the calendar geometry |
| `strata()` | Every stratum of every member, retained or not, with counts, distinct days, overlap, net and gross sums and means, the inclusive-parent share, the disjoint-complement mean and the exclusion reason codes |
| `daily()` | Retained strata's daily target and control counts, net and gross sums and overlap counts, with the day index and its UTC date |

`AnalysisResults` also exposes the verified `request`, `family`, `source`,
`summary`, `provenance`, `status` and `completion` documents.

## Commands

```bash
python -m tools.pattern_lab --help
python -m tools.pattern_lab collect --universe JSON --data-root NEW_PATH \
    --start UTC --end UTC_OR_latest-closed [--okx-rps RATE] [--bybit-rps RATE] \
    [--timeout SECONDS] [--note TEXT]
python -m tools.pattern_lab update --data-root PATH --end UTC_OR_latest-closed \
    [--start EARLIER_UTC] [--okx-rps RATE] [--bybit-rps RATE] [--timeout SECONDS] [--note TEXT]
python -m tools.pattern_lab recover --data-root PATH
python -m tools.pattern_lab abort-update --data-root PATH
python -m tools.pattern_lab import-npz --source-root PATH --output-root NEW_PATH --source-metadata JSON
python -m tools.pattern_lab inspect --data-root PATH [--verify]
python -m tools.pattern_lab slice --data-root PATH --instrument ID --start UTC --end UTC \
    [--warmup-start UTC] [--timeframe-minutes INT]
python -m tools.pattern_lab study --spec STUDY.json --data-root PACK --output-root NEW_RUN \
    [--workers N]
python -m tools.pattern_lab report --run-root NEW_RUN
python -m tools.pattern_lab analyze --run-root COMPLETED_STUDY --spec ANALYSIS.json \
    --output-root NEW_ANALYSIS
python -m tools.pattern_lab analysis-report --analysis-root ANALYSIS
```

`collect` accepts an absent destination, an empty one, or one containing nothing
except the persistent `.pack-lock` file; its parent must already exist and is named
if missing. A ready manifest, a pending journal or any other artifact prevents a
new collect. `update` uses the collector-managed manifest's own roster and cannot
add, drop or relabel instruments, or adopt an archival NPZ-imported pack.

All four collector commands return structured JSON with the operation ID, the
resolved request, the revision before and after, per-instrument added rows, gaps
and coverage, the changed files, and the completion or no-op status. `import-npz`
prints a concise JSON summary. `inspect` is read-only and prints pack metadata,
coverage and verification limitations, checking the pending marker **before** a
manifest is required so an interrupted initial collect still reports its operation,
phase and completed staged instruments; `--verify` additionally checks file hashes,
schema and actual coverage for every declared file. It is an integrity check, not a
promotion of unknown evidence; a normal slice read hashes nothing and loads no
unrelated file. `slice` prints metadata, coverage and the input fingerprint, never
rows or files. `study` runs one event study into a new run directory and `report`
regenerates that run's derived summary and HTML; neither reaches the network, and
help and imports start no job, spawn no process, scan no pack and execute no
extension.

`analyze` reads one completed study strictly, writes one sealed analysis into a
new output root and never modifies the study it read; `analysis-report`
re-renders a sealed analysis from its own artifact alone. Neither reaches the
network, reads a market pack, starts a worker or imports a saved module, and
neither takes a worker count.

JSON goes to stdout and is never polluted by progress logs; diagnostics and
progress go to stderr. A failed collector command prints a JSON object carrying a
stable `error_code` on stdout alongside its stderr explanation; the `study` and
`report` commands do the same; `import-npz`, `inspect` and `slice` keep their
stderr-only error behavior. Exit status:

| Code | Meaning |
| --- | --- |
| `0` | success, including a semantic no-op without a coverage shortfall |
| `1` | inspection verification problems, or a completed publication or no-op with a reported tail shortfall — published does not mean the requested range is complete |
| `2` | invalid request, data or dependency; source failure; missing start coverage; historical conflict; invalid or unrecoverable journal; a `study`/`report` preflight, job, data-admission or report failure, or a recorded failed or partial study; an `analyze`/`analysis-report` invalid request, refused source admission, computation failure or unsealed artifact |
| `3` | pack busy; the rejected caller mutated nothing |
| `4` | a valid pending operation must be recovered or aborted before the requested action, including a research `slice`, a `study`, and a pending **initial** collect that has no manifest yet |
| `130` | a user `KeyboardInterrupt` during `study`, `report`, `analyze` or `analysis-report`, after a best-effort status write and cleanup |

Exit `1` keeps its existing data-command meaning and is never the study-failure
default. A `study`, `report`, `analyze` or `analysis-report` translates an
unexpected execution failure — extension import, an ordinary job exception, a
storage or report error — into a structured JSON status naming the operation,
the phase and the job or analysis identity, while preserving the original
exception as its cause. They share one dispatch set rather than a parallel
exception handler, and collector command behavior is unchanged. A **completed**
analysis artifact exits `0` even when every comparison lacks inferential
support: that is a published result, not a failure.

A detected conflict returns `2` even though it leaves a valid staging journal
behind; a subsequent ordinary update then sees that journal and returns `4`. A plain
`inspect` of a valid pending journal returns its status with `0`, while
`inspect --verify` returns `1` with `checked=false`, `ok=false` and a
pending-operation problem — never a passing certification. Every other read,
including `slice`, checks the pending state **before** requiring a manifest, so a
pending first collect and the known initial journal temporary both return `4`
rather than looking like a missing pack. A malformed journal is invalid input and
returns `2`; a busy root returns `3` before the pending state is consulted. There is no hidden default data root, no default output directory and
no environment-wide output location.

## Verification commands

```bash
export MERLIN_TEST_ROOT="${TMPDIR:-/tmp}/merlin-tests"
python tools/run_tests.py -- tests/pattern_lab
python -m tools.pattern_lab inspect --data-root <pack> --verify
```

```bash
# The declared M3a calibration experiments, once, into an external root:
python -m tools.pattern_lab.analysis.calibration --output-root "${TMPDIR:-/tmp}/pattern-lab-calibration"

# The research-only monthly-jackknife candidate experiment, once, into its own
# external root, and its offline summary rebuilt from that root alone:
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
python -m tools.pattern_lab.analysis.calibration_monthly \
    --output-root "${TMPDIR:-/tmp}/pattern-lab-monthly"
python -m tools.pattern_lab.analysis.calibration_monthly \
    --output-root "${TMPDIR:-/tmp}/pattern-lab-monthly" --summarize-only
```

The event-study cases live in `tests/pattern_lab/test_pattern_lab_study_events.py`,
`_study_model.py`, `_study_admission.py`, `_study_extensions.py`,
`_study_report.py`, `_study_contracts.py` and `_study_workers.py`. The M3a cases
live in `test_pattern_lab_analysis_estimator.py` (the numerical core, with
independent row-level, finite-difference and index-based oracles),
`_analysis_contracts.py` (the request, the resolved family and source
admission), `_analysis_artifact.py` (publication, failure, the seal, relocation,
regeneration and the CLI) and `_analysis_calibration.py` (the calibration
machinery, its versioned protocol gate, its attempt ledger and the corrected
fixture 101; the large declared experiments run once through the command above).
`test_pattern_lab_analysis_monthly.py` owns the experimental candidate's focused
checks: the point and monthly-score identities, direct deletion recomputation,
the equal-count reduction and the interval/test inversion, every refusal, the
fixture-102 candle law, its actual condition, emission, outcome and fee
pipeline, the bounded disk-replay proof of its in-memory adapter, the causal
prefix and suffix invariance of the built-in condition and the custom example,
and the frozen matrix's own stopping arithmetic.
`_study_contracts.py` owns the enforced boundary contracts: custom-model evidence
admission and read-side revalidation, completion and partial-inspection
integrity, every accepted request form, used-source attribution, resolved
dependency warmup, failure attribution and bounded evidence reuse.
`_study_workers.py` starts real spawn children through the production
coordinator and owns worker parity, effective capacity, out-of-order completion,
the retention bound, child isolation, the thread policy, failure and
cancellation mapping and coordinator death. Because pytest `monkeypatch` state
is not inherited by a spawned interpreter, its child-local guards live in a
declared temporary test extension that installs them only inside a worker; there
is no production test mode or hook. They generate synthetic packs and trusted extension modules at
runtime under the launcher's external temporary root and never read market data.

```powershell
$py = 'C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe'
$env:MERLIN_TEST_ROOT = Join-Path $env:LOCALAPPDATA 'Temp\merlin-tests'
& $py tools/run_tests.py -- tests/pattern_lab
& $py tools/run_tests.py -- tests/pattern_lab/test_pattern_lab_lock.py tests/pattern_lab/test_pattern_lab_recovery.py
```

The Windows commands matter because a Linux run does not certify the Windows
`msvcrt` byte-range lock branch or the documented absence of a directory flush.
Ordinary pytest never calls an exchange API. Every response is generated at runtime
through the injectable transport, and the suite additionally installs a default
network denial: the package's default transport and `urllib.request.urlopen` both
fail loudly, so a forgotten injection cannot reach a real venue. The guard is
installed outside `monkeypatch`, so a test's own `monkeypatch.undo()` restores the
guard rather than the real socket path.

## Milestone handoff and known limits

M1a defines the schemas and the reader; M1b adds the collector, the exclusion lock
and the recovery contract; M2a adds the sequential event study, its evidence and
its report, and composes the per-series data fingerprint with the selected
universe, roles and settings into run identity. M2b adds the bounded spawn pool
and `workers=1/2` equivalence. **M3a adds matched controls and calibrated
inference over a completed study**; M3b and M4 are not implemented — M3b owns
external-series/panel feature context and a frozen-candidate validation path, and
M4 owns bracket execution, sizing, leverage, expiry and the interpretation of the
stored instrument rules. M2b block A enforces the boundary contracts M2a
advertised but did not check; block B adds the bounded spawn pool and the
`workers=1`/`workers>1` equivalence.

Known limits of these milestones:

- **M3a inference is unvalidated: its acceptance gate failed.** The delivered
  calibration met 11 of 15 checks; the two admitted scenarios with a persistent
  daily signal-state chain measured about 7.5-8.3% rejection and noncoverage
  against a nominal 5%. A nominal Holm rejection is not a validated edge, and
  these p-values and intervals are anti-conservative under clustered signals.
  The experimental `monthly_cluster_jackknife_v1` candidate passed the same
  synthetic screen at twelve monthly clusters, but it is research-only and
  unintegrated: it changes nothing about the production screen, and adopting it
  is a separate decision that has not been taken.
- **A seven-day block does not control error under dependence substantially
  longer than a week**, and the measurements above show it is already
  anti-conservative at the declared signal persistence. The tracked
  long-dependence experiment measures the worse case at daily AR 0.9 (8.3% and
  11.1% rejection at signal persistence 0.70 and 0.97); it sits outside the
  admitted-null envelope, and the
  software does not detect such dependence automatically in a real run. No
  automatic AR fitting, ACF-based refusal rule, alternate block length or
  runtime sensitivity menu exists, and a longer block is not automatically a
  cure because it also reduces the number of draws.
- M3a admits only the built-in fixed-horizon model. A custom model is rejected
  clearly rather than interpreted, and there is no inferential plugin system.
- Matching is conditional on instrument and UTC calendar month only. It does not
  remove every market regime or historical universe-selection bias, and the
  support gates are not stationarity tests.
- The near-year admission window (336 grid and supported-span days, 252 joint
  active days, 48 populated full seven-day bins) is a bounded-usage decision.
  Shorter or sparser studies stay descriptively usable and publish no p-value or
  confidence interval; zero-contribution padding never restores availability.
- Reusing the scalar aggregation later does not validate M4 sequential trading
  comparisons: occupancy, controls and sizing need their own model-specific
  estimand.
- An analysis assumes cooperative immutable input. The source binding is
  verified twice, but this is not an adversarial filesystem security model, and
  there is no lock, recovery journal, retry or resume for an analysis.

- Parallelism is a bounded instrument-job pool, not a scheduler: there is no
  adaptive scheduling, no automatic worker-count tuning, no worker replacement,
  no task retry and no resume or reuse of an earlier run.
- The retention bound counts jobs, not bytes. IPC and pickle copies add a
  bounded constant factor, and exact summary quantiles keep separate per-group
  samples, so it is not a whole-run RAM guarantee.
- Child thread evidence is startup policy plus what Arrow reports. BLAS runtime
  thread limits are not observable from Python here and are not claimed to be
  measured on either host.
- Cleanup guarantees cover handled failures and interrupts. After a `SIGKILL` of
  the coordinator only the smaller contract holds: the OS releases the pack
  guard and no child reads, writes, publishes or completes anything.
- The internal deadlines above are failure detectors and are not configurable.
  A single instrument job that needs more than one `take()` wait, or a pool that
  cannot start within the startup bound, fails the run rather than continuing.
- Abandoning a result pump blocked on a truncated IPC frame is a deliberate
  exceptional exit. It bounds the coordinator, but it does not guarantee that
  every daemon thread and buffer is reclaimed inside a long-lived embedding
  process; passing process-exit tests are not proof of that.
- Read-side custom-evidence validation is structural and value-level. It proves
  that a saved sample is complete and coherent against the run's own frozen
  family and saved anchors; it cannot prove that a model's numbers are right.
- Summary and metric memory is bounded per instrument and per group, not for the
  whole run: exact quantiles keep compact per-group samples across instruments.
- M2a is descriptive: no p-value, confidence interval, significance badge, edge
  verdict, matched control or automatic winner selection exists there. Those
  live only in an M3a analysis artifact, which adds no automatic practical-effect
  threshold, practical rejection verdict, candidate promotion or winner sorting
  either.
- Source integrity is a digest check over cooperating stable files. It is not a
  sandbox: trusted Python can still open paths of its own, and arbitrary
  causality cannot be proved by shape checks.
- A study's real-pack evidence covers only the intervals its protocol admits;
  gap, feature-reset and omitted-group behavior is covered by synthetic fixtures.

- No historical-value repair operation: a conflict is reported and blocks
  publication, and there is no force or overwrite-history switch.
- No roster editing: an unavailable or delisted member blocks the whole all-roster
  update, and the bounded workaround is a corrected roster in a separate new root.
- Recovery covers tested process interruption, not arbitrary disk corruption or
  unsupported power-loss behavior. A lost applying-stage artifact whose destination
  is not already the new digest is unrecoverable in place.
- Windows has no supported directory flush, and the Windows OS lock branch is not
  certified by a Linux test run.
- Fully read-only mounted packs are unsupported, because every pack-level operation
  opens the in-root `.pack-lock`.
- No downloader worker pool, no adaptive request scheduler, no OI or funding
  collector, no CSV adapter and no second NPZ research reader.
- Unknown source closure for the prototype NPZ pack is unchanged; only re-collected
  data carries verified closure evidence.
- Existing Merlin CSV behavior is unchanged, and Merlin still does not import
  Pattern Lab.
