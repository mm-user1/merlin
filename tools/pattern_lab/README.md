# Pattern Lab data foundation

Pattern Lab is local, research-only tooling. Two milestones are implemented:

- **M1a, the data boundary**: a stable Parquet pack, an explicit manifest, a
  one-way importer for the historical prototype NPZ pack, and an interval reader
  with reproducible research input identity.
- **M1b, the collector**: closed-bar 5m collection from public exchange APIs,
  recoverable updates with overlap conflict reporting, cooperative process
  exclusion, and collector/instrument-rule provenance.

Feature, hypothesis, evaluation-model, bracket-probe and HTML-report commands are
future work and are **not** implemented here.

Merlin may not import Pattern Lab. Pattern Lab reads market data and writes only
to an explicit output directory; it never touches Merlin databases, Queue state,
Presets, baselines or Strategy Lab artifacts. Network access happens **only**
inside an explicit `collect`, `update` or `recover` operation, never on import,
`--help`, `inspect` or `slice`. Only unauthenticated public REST endpoints are
used: there are no credentials, websockets, orders, proxy rotation and no silent
fallback to another venue.

**Code availability is not an operationally prepared pack.** These commands and
their synthetic verification are complete; populating a real market-data root is
a separate, explicitly authorized operational run.

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

Reusable building blocks, all independently callable:
`validate_series`, `series_from_frame`, `write_ohlcv_file`, `read_ohlcv_rows`,
`parquet_column_names`, `resample_complete_groups`, `fingerprint_header`,
`input_fingerprint`, `inspect_pack`, `verify_instrument_files`, plus the manifest
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

For M2, the coordinator owns this guard and its workers must call the same private
read core, with the parent holding the guard until every child has completed its
reads or has been stopped and joined on error. No inherited OS handle and no
user-supplied bypass token exist. **T02 exposes no worker pool and makes no claim
that such a cross-process lifetime is implemented or certified**; M2 must implement
and test it under `spawn`, including parent failure, before exposing parallel reads.

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

    def __init__(self):
        self.sample, self.pages = None, 0

    def __call__(self, url, timeout):
        response = ex.urllib_transport(url, timeout)
        if "candles" in url or "kline" in url:
            self.pages += 1
            if self.sample is None and response.status == 200:
                self.sample = json.loads(response.body)
        return response


for adapter, contract, quote_index, rows_of in (
    (ex.OkxAdapter(), "BTC-USDT-SWAP", 7, lambda body: body["data"]),
    (ex.BybitAdapter(), "ENAUSDT", 6, lambda body: body["result"]["list"]),
):
    capture = Capture()
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
    print(adapter.venue, contract, "candle pages", capture.pages,
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
not one. Confirm the reported candle-page counts match the expected four (OKX) and
two (Bybit), that the server time is close to the host clock, and that the rules
match the venue's published contract page. Then run the full-roster preflight by
starting the real `collect` and letting its preflight pass before the bulk download
begins.

The snippet's comparison logic is covered synthetically by
`tests/pattern_lab/test_pattern_lab_collector.py::TestQuoteFieldEvidence`, using a
fixture whose base and quote volumes differ so a wrong-index comparison fails.
That is evidence about the snippet only: it certifies nothing about the live
endpoints, which this recipe alone can exercise.

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
rows or files.

JSON goes to stdout and is never polluted by progress logs; diagnostics and
progress go to stderr. A failed collector command prints a JSON object carrying a
stable `error_code` on stdout alongside its stderr explanation; `import-npz`,
`inspect` and `slice` keep their stderr-only error behavior. Exit status:

| Code | Meaning |
| --- | --- |
| `0` | success, including a semantic no-op without a coverage shortfall |
| `1` | inspection verification problems, or a completed publication or no-op with a reported tail shortfall — published does not mean the requested range is complete |
| `2` | invalid request, data or dependency; source failure; missing start coverage; historical conflict; invalid or unrecoverable journal |
| `3` | pack busy; the rejected caller mutated nothing |
| `4` | a valid pending operation must be recovered or aborted before the requested action, including a research `slice` and a pending **initial** collect that has no manifest yet |

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

## M2 handoff and known limits

M1a defines the schemas and the reader; M1b adds the collector, the exclusion lock
and the recovery contract. M2 owns hypotheses, features, compute multiprocessing
and HTML reporting; M3 owns statistics; M4 owns bracket execution and the
interpretation of the stored instrument rules. M2 composes the per-series data
fingerprint with the selected universe, roles and settings into run identity, and
must implement and test the coordinator-owned read-session lifetime under `spawn`
before exposing parallel reads.

Known limits of these milestones:

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
