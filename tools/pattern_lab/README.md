# Pattern Lab data foundation

Pattern Lab is local, research-only tooling. This milestone (M1a) implements only
its data boundary: a stable Parquet pack, an explicit manifest, a one-way importer
for the historical prototype NPZ pack, and an interval reader with reproducible
research input identity. Feature, hypothesis, evaluation-model, bracket-probe and
HTML-report commands are future work and are **not** implemented here.

Merlin may not import Pattern Lab. Pattern Lab reads market data and writes only
to an explicit output directory; it never touches Merlin databases, Queue state,
Presets, baselines or Strategy Lab artifacts, and it never downloads anything.

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
  manifest.json        authoritative metadata
  README.md            rendered from the manifest; never edited independently
  updates.jsonl        publication/update history
  ohlcv/
    OKX_LINK-USDT-SWAP_5m.parquet
```

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
object (populated by M1b; it is metadata, not an execution implementation).

Legal nonempty role sets are exactly `{trading}`, `{research_only}`, `{factor}`
and `{trading, factor}`; roles are serialized sorted. BTC as a factor-only series
has no trading role. File presence never changes trading eligibility.

`source` records `input_format`, `input_dtype`, `source_hash` (nullable),
`source_reference` and `volume_unit_evidence`. `verification` records
`closed_before_utc` (nullable) with `closure_evidence`/`closure_source`, plus
`volume_quote_verified` and `volume_quote_evidence`. A closure cutoff certifies
that retained bars ending at or before it were closed when observed; it does not
certify a gap-free history. Unknown evidence stays unknown.

T01 publishers never emit `state=incomplete` or `volume_quote_verified=false`:
a failed publication leaves no manifest, and unproven quote units are rejected.
The validator and reader recognize both states for future writers.

## Coverage, closure and gaps

- Coverage is per instrument. A lagging instrument is never hidden behind a
  nominal pack end, and one instrument never truncates another.
- `missing_bar_policy` is `no_fill_v1`: absent 5m slots stay absent. They are
  reported as gaps and segment breaks, never as fabricated flat candles.
- A research read requires `volume_quote_verified` true and a `closed_before_utc`
  at or after the requested exclusive end. That prefix certification covers warmup
  rows as well as research rows. There is no ignore-verification switch, and a
  later wall clock never promotes unverified rows.

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

Reusable building blocks, all independently callable and shared with M1b:
`validate_series`, `series_from_frame`, `write_ohlcv_file`, `read_ohlcv_rows`,
`resample_complete_groups`, `fingerprint_header`, `input_fingerprint`,
`inspect_pack`, plus the manifest serializer (`build_manifest`,
`validate_manifest`, `read_manifest`, `write_manifest`), README renderer
(`render_readme`) and update history (`build_update_record`,
`append_update_record`).

### Read and write boundaries

- `publish_pack` creates a **new** directory exclusively. A pre-existing
  destination (even an empty one) and overlapping source/output roots are refused.
  Each file is written to a sibling temporary name, closed, re-read, validated and
  then replaced; the README and update record follow, and the `ready` manifest is
  published atomically **last**. A failure leaves no ready manifest and no
  recursive cleanup of caller directories, so partial output survives as evidence.
- `write_ohlcv_file` defaults to refusing replacement. `replace_existing=True`
  exists for a caller-owned file, does not update a ready root's manifest and
  offers no concurrency safety. The new-pack command never enables it.
- Readers fail on a missing or `incomplete` manifest and on a
  `.update-in-progress.json` marker. `load_slice` compares the marker and the
  manifest revision/state before and after the read and fails rather than
  returning a mixed result. This is cooperative change detection, not the M1b
  update/run exclusion lock.
- Concurrent mutation of a ready root is unsupported in T01; there is no update
  command and no claim of multi-file atomicity.

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

Reference encoder:

```python
import hashlib, json, struct
import numpy as np

payload = json.dumps(
    header, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
).encode("utf-8")
rows = np.ascontiguousarray(ohlcv, dtype="<f8")          # (N, 5), interleaved by row
rows[:, 4][rows[:, 4] == 0.0] = 0.0                       # normalize signed zero
digest = hashlib.sha256()
digest.update(struct.pack("<Q", len(payload)))            # header byte length
digest.update(payload)                                    # header bytes
digest.update(struct.pack("<Q", len(timestamps)))         # raw consumed row count
digest.update(np.ascontiguousarray(timestamps, dtype="<i8").tobytes())
digest.update(rows.tobytes())
fingerprint = digest.hexdigest()                          # 64 lowercase hex, no prefix
```

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
old candle must have closed because today is later. M1b re-fetches and verifies
uncertain history before operational use.

## Commands

```bash
python -m tools.pattern_lab --help
python -m tools.pattern_lab import-npz --source-root PATH --output-root NEW_PATH --source-metadata JSON
python -m tools.pattern_lab inspect --data-root PATH [--verify]
python -m tools.pattern_lab slice --data-root PATH --instrument ID --start UTC --end UTC \
    [--warmup-start UTC] [--timeframe-minutes INT]
```

`import-npz` prints a concise JSON summary. `inspect` is read-only and prints pack
metadata, coverage and verification limitations; `--verify` additionally checks
file hashes, schema and actual coverage for every declared file and exits nonzero
on corruption. It is an integrity check, not a promotion of unknown evidence; a
normal slice read hashes nothing and loads no unrelated file. `slice` prints
metadata, coverage and the input fingerprint, never rows or files.

JSON goes to stdout and is never polluted by progress logs; errors name the
instrument, field or interval and go to stderr. Exit status: `0` success,
`1` verification problems found, `2` invalid request, invalid data or a missing
dependency. There is no hidden default data root, no default output directory and
no environment-wide output location.

## Verification commands

```bash
export MERLIN_TEST_ROOT="${TMPDIR:-/tmp}/merlin-tests"
python tools/run_tests.py -- tests/pattern_lab
python -m tools.pattern_lab inspect --data-root <pack> --verify
```

## M1b handoff

M1a defines the schemas and the reader. The separate M1b collector task owns the
exchange collector and backfill, monthly append/repair with overlap conflict
reporting, the update/run exclusion lock and journal recovery, the full
operational pack with verified history, and the real README/manifest/update-log
population. M1b reuses `write_ohlcv_file`, the manifest serializer, the README
renderer and the update-history helpers; it must add the transaction, locking and
recovery contract before any update command exists.

Known limits of this milestone: no updater or locking, no historical
reconstruction of changed values, unknown source closure for the prototype pack,
no CSV adapter, no exchange access and no second NPZ research reader. Existing
Merlin CSV behavior is unchanged.
