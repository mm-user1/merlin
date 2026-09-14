"""Parquet storage, publication, inspection, fixed-range reads and input identity.

PyArrow is imported lazily by :func:`require_pyarrow` so that importing this
module, rendering ``--help`` and reporting a missing dependency all work without
the optional wheel.  Pattern Lab never installs dependencies itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import struct
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

import numpy as np
import pandas as pd

from . import PatternLabDataError, PatternLabDependencyError
from . import manifest as pack_manifest
from .manifest import (
    BASE_STEP_MS,
    BASE_TIMEFRAME_MINUTES,
    OHLCV_DIR,
    VOLUME_UNIT,
    format_epoch_ms,
    from_epoch_ms,
    normalize_id_part,
    normalize_instrument_id,
    normalize_timeframe_minutes,
    require_currency,
    require_int,
    require_aligned,
    to_epoch_ms,
)

OHLCV_COLUMNS = ("open", "high", "low", "close", "volume_quote")
PARQUET_COLUMNS = ("timestamp",) + OHLCV_COLUMNS
ROW_GROUP_SIZE = 8192
PARQUET_COMPRESSION = "zstd"

FINGERPRINT_VERSION = 1
RESAMPLING_POLICY = "utc_epoch_complete_v1"
MISSING_BAR_POLICY = "no_fill_v1"

PYARROW_REQUIREMENT = "pyarrow==22.0.0"
_MISSING_PYARROW = (
    f"Pattern Lab data commands require PyArrow. Install the pinned project dependency "
    f"with 'python -m pip install {PYARROW_REQUIREMENT}' (it is listed in the root "
    f"requirements.txt). Pattern Lab never installs dependencies automatically."
)


def require_pyarrow():
    """Return ``(pyarrow, pyarrow.parquet)`` or fail with an actionable message."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - exercised in a child process
        raise PatternLabDependencyError(_MISSING_PYARROW) from exc
    return pa, pq


# --------------------------------------------------------------------------
# array canonicalization and validation
# --------------------------------------------------------------------------

def canonicalize_timestamps(values: Any, field: str = "timestamps") -> np.ndarray:
    """Normalize candle open times to a 1-D int64 array of UTC epoch milliseconds."""
    if isinstance(values, pd.Series):
        values = pd.DatetimeIndex(values) if pd.api.types.is_datetime64_any_dtype(values) else values.to_numpy()
    if isinstance(values, pd.DatetimeIndex):
        if values.tz is not None:
            values = values.tz_convert("UTC").tz_localize(None)
        values = values.to_numpy()
    array = np.asarray(values)
    if array.ndim != 1:
        raise PatternLabDataError(f"{field}: expected a one-dimensional array, got shape {array.shape}.")
    if array.dtype.kind == "M":
        milliseconds = array.astype("datetime64[ms]")
        if not np.array_equal(milliseconds.astype(array.dtype), array):
            raise PatternLabDataError(f"{field}: sub-millisecond timestamps are not supported.")
        return np.ascontiguousarray(milliseconds.astype(np.int64), dtype=np.int64)
    if array.dtype.kind in "iu":
        converted = array.astype(np.int64)
        if not np.array_equal(converted, array):
            raise PatternLabDataError(f"{field}: values do not fit in int64 epoch milliseconds.")
        return np.ascontiguousarray(converted, dtype=np.int64)
    raise PatternLabDataError(
        f"{field}: expected integer epoch milliseconds or datetime64 values, got dtype {array.dtype}."
    )


def canonicalize_ohlcv(values: Any, field: str = "ohlcv") -> np.ndarray:
    """Normalize OHLCV input to a C-contiguous ``(N, 5)`` float64 array.

    Caller-created DataFrames and float32 arrays are accepted and promoted here;
    the stored and hashed representation is always float64.
    """
    if isinstance(values, pd.DataFrame):
        missing = [name for name in OHLCV_COLUMNS if name not in values.columns]
        if missing:
            raise PatternLabDataError(f"{field}: DataFrame is missing columns {missing}.")
        values = values.loc[:, list(OHLCV_COLUMNS)].to_numpy()
    array = np.asarray(values)
    if array.dtype.kind not in "fiu":
        raise PatternLabDataError(f"{field}: expected real numeric values, got dtype {array.dtype}.")
    if array.ndim != 2 or array.shape[1] != len(OHLCV_COLUMNS):
        raise PatternLabDataError(
            f"{field}: expected a (N, {len(OHLCV_COLUMNS)}) array in open/high/low/close/volume_quote order, "
            f"got shape {array.shape}."
        )
    return np.ascontiguousarray(array, dtype=np.float64)


def _normalize_volume_signed_zero(values: np.ndarray) -> np.ndarray:
    """Return values whose zero volumes are positive zero, without mutating input."""
    volume = values[:, 4]
    negative_zero = (volume == 0.0) & np.signbit(volume)
    if negative_zero.any():
        values = values.copy()
        values[:, 4][negative_zero] = 0.0
    return values


def _canonical_arrays(timestamps: Any, ohlcv: Any, where: str) -> tuple[np.ndarray, np.ndarray]:
    stamps = canonicalize_timestamps(timestamps, f"{where}.timestamps")
    values = canonicalize_ohlcv(ohlcv, f"{where}.ohlcv")
    if stamps.size != values.shape[0]:
        raise PatternLabDataError(
            f"{where}: {stamps.size} timestamps do not match {values.shape[0]} OHLCV rows."
        )
    return stamps, values


def validate_series(timestamps: Any, ohlcv: Any, *, where: str = "series") -> tuple[np.ndarray, np.ndarray]:
    """Validate one stored instrument's 5m series and return canonical arrays.

    Invalid input is rejected: values are never interpolated, deduplicated,
    clamped, or converted from base volume. Empty instruments are rejected here;
    use :func:`validate_consumed_rows` where an empty read is well defined.
    """
    stamps, values = _canonical_arrays(timestamps, ohlcv, where)
    if stamps.size == 0:
        raise PatternLabDataError(f"{where}: empty instruments are rejected; coverage must be defined.")
    return _validate_rows(stamps, values, where)


def validate_consumed_rows(timestamps: Any, ohlcv: Any, *, where: str = "rows") -> tuple[np.ndarray, np.ndarray]:
    """Validate consumed 5m rows under the same rules, accepting a well-formed empty input.

    A one-dimensional empty timestamp array with a ``(0, 5)`` OHLCV array is the
    only accepted empty shape; malformed empty shapes still fail.
    """
    stamps, values = _canonical_arrays(timestamps, ohlcv, where)
    if stamps.size == 0:
        return stamps, values
    return _validate_rows(stamps, values, where)


def _validate_rows(stamps: np.ndarray, values: np.ndarray, where: str) -> tuple[np.ndarray, np.ndarray]:
    """Enforce the 5m series contract on a nonempty canonical pair of arrays."""
    steps = np.diff(stamps)
    if steps.size and (steps == 0).any():
        raise PatternLabDataError(f"{where}: duplicate timestamps are rejected.")
    if steps.size and (steps < 0).any():
        raise PatternLabDataError(f"{where}: timestamps must be strictly increasing.")
    if (stamps % BASE_STEP_MS != 0).any():
        raise PatternLabDataError(
            f"{where}: every timestamp must sit on the {BASE_TIMEFRAME_MINUTES}m UTC epoch grid."
        )
    prices = values[:, :4]
    if not np.isfinite(prices).all():
        raise PatternLabDataError(f"{where}: OHLC prices must be finite.")
    if (prices <= 0.0).any():
        raise PatternLabDataError(f"{where}: OHLC prices must be positive.")
    body_low = np.minimum(values[:, 0], values[:, 3])
    body_high = np.maximum(values[:, 0], values[:, 3])
    if (values[:, 2] > body_low).any() or (body_high > values[:, 1]).any():
        raise PatternLabDataError(f"{where}: requires low <= min(open, close) <= max(open, close) <= high.")
    volume = values[:, 4]
    if not np.isfinite(volume).all():
        raise PatternLabDataError(f"{where}: volume_quote must be finite.")
    if (volume < 0.0).any():
        raise PatternLabDataError(f"{where}: volume_quote must be nonnegative.")
    return stamps, _normalize_volume_signed_zero(values)


def series_from_frame(frame: pd.DataFrame, *, timestamp_column: str = "timestamp", where: str = "frame"):
    """Validate a caller-created DataFrame and return canonical 5m arrays."""
    if not isinstance(frame, pd.DataFrame):
        raise PatternLabDataError(f"{where}: expected a pandas DataFrame, got {type(frame).__name__}.")
    if timestamp_column in frame.columns:
        stamps = frame[timestamp_column]
    else:
        stamps = frame.index
    return validate_series(stamps, frame, where=where)


def missing_bar_count(timestamps: np.ndarray) -> int:
    """Count absent 5m slots between the first and last observed open time."""
    if timestamps.size == 0:
        return 0
    span = int((timestamps[-1] - timestamps[0]) // BASE_STEP_MS) + 1
    return span - int(timestamps.size)


# --------------------------------------------------------------------------
# Parquet file primitives
# --------------------------------------------------------------------------

def parquet_schema(pa):
    """Return the frozen schema v1 with explicit non-nullable column types."""
    return pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
            *(pa.field(name, pa.float64(), nullable=False) for name in OHLCV_COLUMNS),
        ]
    )


def _validate_parquet_schema(pa, schema, path: Path) -> None:
    names = set(schema.names)
    missing = [name for name in PARQUET_COLUMNS if name not in names]
    if missing:
        raise PatternLabDataError(f"{path}: Parquet file is missing required columns {missing}.")
    stamp = schema.field("timestamp").type
    if not pa.types.is_timestamp(stamp) or stamp.unit != "ms" or stamp.tz not in ("UTC", "+00:00"):
        raise PatternLabDataError(f"{path}: column 'timestamp' must be timestamp[ms, UTC], got {stamp}.")
    for name in OHLCV_COLUMNS:
        column = schema.field(name).type
        if not pa.types.is_float64(column):
            raise PatternLabDataError(f"{path}: column {name!r} must be double, got {column}.")


@dataclass(frozen=True)
class WrittenFile:
    """Verified facts about one published Parquet file."""

    path: Path
    sha256: str
    row_count: int
    first_open_ms: int
    last_open_ms: int
    missing_bar_count: int


def write_ohlcv_file(
    target: Path,
    timestamps: Any,
    ohlcv: Any,
    *,
    replace_existing: bool = False,
    where: str = "series",
) -> WrittenFile:
    """Write one validated 5m Parquet file through temp, readback and replace.

    The default refuses to replace an existing target.  ``replace_existing`` is a
    low-level option for a caller that owns the existing file; it neither updates
    a published manifest nor provides any concurrency safety.
    """
    pa, pq = require_pyarrow()
    stamps, values = validate_series(timestamps, ohlcv, where=where)
    target = Path(target)
    if target.exists() or target.is_symlink():
        if not replace_existing:
            raise PatternLabDataError(
                f"{target}: refusing to replace an existing file; pass replace_existing=True only for a "
                "caller-owned target."
            )
    if not target.parent.is_dir():
        raise PatternLabDataError(f"{target.parent}: destination directory does not exist.")

    schema = parquet_schema(pa)
    table = pa.Table.from_arrays(
        [
            pa.array(stamps, type=pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
            *(pa.array(values[:, index], type=pa.float64()) for index in range(len(OHLCV_COLUMNS))),
        ],
        schema=schema,
    )
    temporary = target.with_name(f".{target.name}.tmp-{uuid4().hex}")
    try:
        pq.write_table(
            table,
            temporary,
            compression=PARQUET_COMPRESSION,
            row_group_size=ROW_GROUP_SIZE,
            store_schema=True,
        )
        read_stamps, read_values = read_ohlcv_rows(temporary)
        if not np.array_equal(read_stamps, stamps) or not np.array_equal(read_values, values):
            raise PatternLabDataError(f"{target}: readback of the written file did not reproduce the input values.")
        digest = pack_manifest.file_sha256(temporary)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, target)
    return WrittenFile(
        path=target,
        sha256=digest,
        row_count=int(stamps.size),
        first_open_ms=int(stamps[0]),
        last_open_ms=int(stamps[-1]),
        missing_bar_count=missing_bar_count(stamps),
    )


def read_ohlcv_rows(
    path: Path,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
    allow_empty: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    """Read and validate 5m rows, optionally restricted to ``[start_ms, end_ms)``.

    Range arguments are pushed down to Parquet, and the logical interval is then
    asserted again so that physically decoded neighbours never reach the caller.
    Extra columns in the file are ignored.
    """
    pa, pq = require_pyarrow()
    path = Path(path)
    if not path.is_file():
        raise PatternLabDataError(f"{path}: Parquet file is missing.")
    _validate_parquet_schema(pa, pq.read_schema(path), path)

    filters = []
    if start_ms is not None:
        filters.append(("timestamp", ">=", from_epoch_ms(start_ms)))
    if end_ms is not None:
        filters.append(("timestamp", "<", from_epoch_ms(end_ms)))
    table = pq.read_table(path, columns=list(PARQUET_COLUMNS), filters=filters or None)
    for name in PARQUET_COLUMNS:
        if table.column(name).null_count:
            raise PatternLabDataError(f"{path}: column {name!r} contains null values.")

    stamps = table.column("timestamp").cast(pa.int64()).combine_chunks().to_numpy(zero_copy_only=False)
    stamps = np.ascontiguousarray(stamps, dtype=np.int64)
    columns = [
        np.ascontiguousarray(
            table.column(name).combine_chunks().to_numpy(zero_copy_only=False), dtype=np.float64
        )
        for name in OHLCV_COLUMNS
    ]
    values = np.ascontiguousarray(np.column_stack(columns), dtype=np.float64)

    if start_ms is not None or end_ms is not None:
        inside = np.ones(stamps.size, dtype=bool)
        if start_ms is not None:
            inside &= stamps >= start_ms
        if end_ms is not None:
            inside &= stamps < end_ms
        if not inside.all():
            stamps = np.ascontiguousarray(stamps[inside], dtype=np.int64)
            values = np.ascontiguousarray(values[inside], dtype=np.float64)

    if stamps.size == 0:
        if allow_empty:
            return stamps, values.reshape(0, len(OHLCV_COLUMNS))
        raise PatternLabDataError(f"{path}: contains no rows.")
    return validate_series(stamps, values, where=str(path))


# --------------------------------------------------------------------------
# publication
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class InstrumentSource:
    """One instrument's validated arrays plus the metadata published with them."""

    symbol: str
    venue: str
    contract: str
    quote_currency: str
    roles: Sequence[str]
    timestamps: Any
    ohlcv: Any
    source: Mapping[str, Any]
    verification: Mapping[str, Any]
    instrument_rules: Mapping[str, Any] | None = None


def _resolved_target(path: Path) -> Path:
    path = Path(path)
    return path.parent.resolve() / path.name


def _require_disjoint(output_root: Path, source_root: Path | None) -> None:
    if source_root is None:
        return
    destination = _resolved_target(output_root)
    origin = Path(source_root).resolve()
    if destination == origin or destination.is_relative_to(origin) or origin.is_relative_to(destination):
        raise PatternLabDataError(
            f"output root {destination} and source root {origin} must not overlap."
        )


def publish_pack(
    output_root: Path,
    instruments: Iterable[InstrumentSource],
    *,
    universe: Mapping[str, Any],
    generated_utc: Any = None,
    revision: int = 1,
    source: Mapping[str, Any] | None = None,
    note: str | None = None,
    source_root: Path | None = None,
) -> dict[str, Any]:
    """Publish a NEW pack: per-instrument files, README, history, manifest last.

    Instruments are consumed one at a time; no universe-wide cube is built.  The
    destination must not already exist, and the ``ready`` manifest is published
    only after every file has been written, re-read and verified.  Each
    instrument must declare verified quote-volume units before its file is
    written; unknown closure remains publishable as archival provenance.
    """
    require_pyarrow()
    output_root = Path(output_root)
    _require_disjoint(output_root, source_root)
    try:
        output_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise PatternLabDataError(
            f"{output_root}: destination already exists; publication requires a new directory."
        ) from exc
    (output_root / OHLCV_DIR).mkdir()

    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in instruments:
        if not isinstance(item, InstrumentSource):
            raise PatternLabDataError(
                f"instruments: expected InstrumentSource records, got {type(item).__name__}."
            )
        instrument_id = pack_manifest.build_instrument_id(item.venue, item.contract)
        if instrument_id in seen:
            raise PatternLabDataError(f"instruments: duplicate instrument {instrument_id!r}.")
        seen.add(instrument_id)
        # T01 publishers never emit unverified quote volume: reject before writing.
        pack_manifest.require_published_verification(item.verification, instrument_id)
        relative = pack_manifest.instrument_relative_file(instrument_id)
        written = write_ohlcv_file(
            output_root / relative,
            item.timestamps,
            item.ohlcv,
            where=instrument_id,
        )
        entries.append(
            pack_manifest.build_instrument_entry(
                symbol=item.symbol,
                venue=item.venue,
                contract=item.contract,
                quote_currency=item.quote_currency,
                roles=item.roles,
                row_count=written.row_count,
                first_open_ms=written.first_open_ms,
                last_open_ms=written.last_open_ms,
                missing_bar_count=written.missing_bar_count,
                sha256=written.sha256,
                source=item.source,
                verification=item.verification,
                instrument_rules=item.instrument_rules,
            )
        )
    if not entries:
        raise PatternLabDataError("instruments: at least one instrument is required to publish a pack.")

    moment = generated_utc or datetime.now(timezone.utc).replace(microsecond=0)
    manifest = pack_manifest.build_manifest(
        instruments=entries, universe=universe, generated_utc=moment, revision=revision, state="ready"
    )
    pack_manifest.write_text_atomic(output_root / pack_manifest.README_NAME, pack_manifest.render_readme(manifest))
    pack_manifest.append_update_record(
        output_root, pack_manifest.build_update_record(manifest, event="publish", source=source, note=note)
    )
    pack_manifest.write_manifest(output_root, manifest)
    return publication_summary(output_root, manifest)


def publication_summary(data_root: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Return the concise JSON summary printed after a publication."""
    return {
        "data_root": str(Path(data_root)),
        "schema_version": manifest["schema_version"],
        "revision": manifest["revision"],
        "state": manifest["state"],
        "generated_utc": manifest["generated_utc"],
        "instrument_count": len(manifest["instruments"]),
        "instruments": [
            {
                "instrument_id": entry["instrument_id"],
                "roles": entry["roles"],
                "row_count": entry["row_count"],
                "first_open_utc": entry["first_open_utc"],
                "coverage_end_utc": entry["coverage_end_utc"],
                "missing_bar_count": entry["missing_bar_count"],
                "sha256": entry["sha256"],
                "closed_before_utc": entry["verification"]["closed_before_utc"],
            }
            for entry in manifest["instruments"]
        ],
    }


# --------------------------------------------------------------------------
# inspection
# --------------------------------------------------------------------------

def _verify_instrument(data_root: Path, entry: Mapping[str, Any]) -> list[str]:
    problems: list[str] = []
    instrument_id = entry["instrument_id"]
    try:
        path = pack_manifest.resolve_pack_path(data_root, entry["file"], f"{instrument_id}.file")
    except PatternLabDataError as exc:
        return [str(exc)]
    if not path.is_file():
        return [f"{instrument_id}: declared file {entry['file']} is missing."]
    digest = pack_manifest.file_sha256(path)
    if digest != entry["sha256"]:
        problems.append(f"{instrument_id}: SHA-256 {digest} does not match the manifest digest {entry['sha256']}.")
    try:
        stamps, _ = read_ohlcv_rows(path)
    except PatternLabDependencyError:
        raise
    except Exception as exc:  # a corrupted file raises from the Parquet reader itself
        problems.append(f"{instrument_id}: {exc}")
        return problems
    if int(stamps.size) != entry["row_count"]:
        problems.append(f"{instrument_id}: {stamps.size} rows do not match the declared row_count {entry['row_count']}.")
    if format_epoch_ms(int(stamps[0])) != entry["first_open_utc"]:
        problems.append(
            f"{instrument_id}: first open {format_epoch_ms(int(stamps[0]))} does not match {entry['first_open_utc']}."
        )
    if format_epoch_ms(int(stamps[-1])) != entry["last_open_utc"]:
        problems.append(
            f"{instrument_id}: last open {format_epoch_ms(int(stamps[-1]))} does not match {entry['last_open_utc']}."
        )
    actual_missing = missing_bar_count(stamps)
    if actual_missing != entry["missing_bar_count"]:
        problems.append(
            f"{instrument_id}: {actual_missing} missing bars do not match the declared {entry['missing_bar_count']}."
        )
    return problems


def inspect_pack(data_root: Path, *, verify: bool = False) -> dict[str, Any]:
    """Return read-only structured pack metadata, optionally verifying the files.

    ``verify`` is an integrity check of hashes, schema and actual coverage.  It
    never promotes unknown closure or quote-unit evidence.
    """
    data_root = Path(data_root)
    manifest = pack_manifest.read_manifest(data_root)
    instruments = []
    for entry in manifest["instruments"]:
        limitations = pack_manifest.research_limitations(entry)
        blockers = pack_manifest.research_blockers(entry)
        instruments.append(
            {
                "instrument_id": entry["instrument_id"],
                "symbol": entry["symbol"],
                "venue": entry["venue"],
                "contract": entry["contract"],
                "quote_currency": entry["quote_currency"],
                "roles": entry["roles"],
                "file": entry["file"],
                "sha256": entry["sha256"],
                "row_count": entry["row_count"],
                "first_open_utc": entry["first_open_utc"],
                "last_open_utc": entry["last_open_utc"],
                "coverage_end_utc": entry["coverage_end_utc"],
                "missing_bar_count": entry["missing_bar_count"],
                "source": dict(entry["source"]),
                "verification": dict(entry["verification"]),
                "research_readable": not blockers,
                "research_blockers": blockers,
                "research_limitations": limitations,
            }
        )
    report = {
        "data_root": str(data_root),
        "schema_version": manifest["schema_version"],
        "revision": manifest["revision"],
        "state": manifest["state"],
        "generated_utc": manifest["generated_utc"],
        "base_timeframe_minutes": manifest["base_timeframe_minutes"],
        "volume_unit": manifest["volume_unit"],
        "update_in_progress": pack_manifest.update_marker_path(data_root).exists(),
        "universe": dict(manifest["universe"]),
        "instrument_count": len(manifest["instruments"]),
        "instruments": instruments,
    }
    if verify:
        problems: list[str] = []
        for entry in manifest["instruments"]:
            problems.extend(_verify_instrument(data_root, entry))
        report["verification_check"] = {"checked": True, "ok": not problems, "problems": problems}
    else:
        report["verification_check"] = {"checked": False, "ok": None, "problems": []}
    return report


# --------------------------------------------------------------------------
# resampling
# --------------------------------------------------------------------------

def resample_complete_groups(
    timestamps: np.ndarray, ohlcv: np.ndarray, timeframe_minutes: int
) -> tuple[np.ndarray, np.ndarray, int]:
    """Aggregate complete UTC epoch-anchored groups; incomplete groups are omitted.

    Input is validated at this public boundary through the same 5m series rules
    used for storage, because the count-based algorithm is only correct once
    uniqueness, ordering and grid alignment hold.  A group is aggregated only when
    it holds exactly the expected unique 5m timestamps from its open to its last
    slot.  No partial leading or trailing group is promoted to a full candle.
    """
    minutes = normalize_timeframe_minutes(timeframe_minutes)
    step_ms = minutes * 60_000
    per_group = step_ms // BASE_STEP_MS
    stamps, values = validate_consumed_rows(timestamps, ohlcv, where="resample")
    if stamps.size == 0:
        return stamps, values, 0

    groups = (stamps // step_ms) * step_ms
    starts = np.flatnonzero(np.concatenate(([True], groups[1:] != groups[:-1])))
    ends = np.concatenate((starts[1:], [stamps.size]))
    counts = ends - starts
    complete = counts == per_group

    aggregated = np.column_stack(
        [
            values[starts, 0],
            np.maximum.reduceat(values[:, 1], starts),
            np.minimum.reduceat(values[:, 2], starts),
            values[ends - 1, 3],
            np.add.reduceat(values[:, 4], starts),
        ]
    )
    omitted = int(np.count_nonzero(~complete))
    return (
        np.ascontiguousarray(groups[starts][complete], dtype=np.int64),
        _normalize_volume_signed_zero(np.ascontiguousarray(aggregated[complete], dtype=np.float64)),
        omitted,
    )


# --------------------------------------------------------------------------
# research input identity
# --------------------------------------------------------------------------

FINGERPRINT_HEADER_KEYS = (
    "fingerprint_version",
    "instrument_id",
    "venue",
    "contract",
    "quote_currency",
    "volume_unit",
    "base_timeframe_minutes",
    "timeframe_minutes",
    "start_ms",
    "end_ms",
    "warmup_start_ms",
    "resampling_policy",
    "missing_bar_policy",
)


def _require_canonical(value: Any, normalized: str, field: str) -> str:
    if value != normalized:
        raise PatternLabDataError(
            f"{field}: expected the canonical value {normalized!r}, got {value!r}."
        )
    return normalized


def validate_fingerprint_header(header: Any, *, where: str = "fingerprint.header") -> dict[str, Any]:
    """Validate the closed v1 fingerprint header and return a canonical copy.

    The encoder accepts a canonical header; it never silently rewrites one.  The
    key set, JSON types, frozen constants, instrument identity and the requested
    interval are all enforced before any bytes are hashed.
    """
    if not isinstance(header, Mapping):
        raise PatternLabDataError(f"{where}: expected a mapping, got {type(header).__name__}.")
    missing = sorted(set(FINGERPRINT_HEADER_KEYS) - set(header))
    extra = sorted(set(header) - set(FINGERPRINT_HEADER_KEYS))
    if missing or extra:
        raise PatternLabDataError(
            f"{where}: the v1 header is closed; missing keys {missing}, unexpected keys {extra}."
        )

    version = require_int(header["fingerprint_version"], f"{where}.fingerprint_version")
    if version != FINGERPRINT_VERSION:
        raise PatternLabDataError(
            f"{where}.fingerprint_version: unsupported version {version}; this build writes "
            f"{FINGERPRINT_VERSION}."
        )
    base = require_int(header["base_timeframe_minutes"], f"{where}.base_timeframe_minutes")
    if base != BASE_TIMEFRAME_MINUTES:
        raise PatternLabDataError(
            f"{where}.base_timeframe_minutes: must be {BASE_TIMEFRAME_MINUTES}, got {base}."
        )
    for field, expected in (
        ("volume_unit", VOLUME_UNIT),
        ("resampling_policy", RESAMPLING_POLICY),
        ("missing_bar_policy", MISSING_BAR_POLICY),
    ):
        if header[field] != expected:
            raise PatternLabDataError(f"{where}.{field}: must be {expected!r}, got {header[field]!r}.")

    venue = _require_canonical(
        header["venue"], normalize_id_part(header["venue"], f"{where}.venue"), f"{where}.venue"
    )
    contract = _require_canonical(
        header["contract"], normalize_id_part(header["contract"], f"{where}.contract"), f"{where}.contract"
    )
    instrument_id = _require_canonical(
        header["instrument_id"],
        normalize_instrument_id(header["instrument_id"], f"{where}.instrument_id"),
        f"{where}.instrument_id",
    )
    if instrument_id != f"{venue}_{contract}":
        raise PatternLabDataError(
            f"{where}.instrument_id: {instrument_id!r} does not match venue/contract {venue}_{contract}."
        )
    currency = require_currency(header["quote_currency"], f"{where}.quote_currency")

    minutes = normalize_timeframe_minutes(header["timeframe_minutes"], f"{where}.timeframe_minutes")
    step_ms = minutes * 60_000
    warmup_ms = require_aligned(
        require_int(header["warmup_start_ms"], f"{where}.warmup_start_ms"), step_ms, f"{where}.warmup_start_ms"
    )
    start_ms = require_aligned(
        require_int(header["start_ms"], f"{where}.start_ms"), step_ms, f"{where}.start_ms"
    )
    end_ms = require_aligned(
        require_int(header["end_ms"], f"{where}.end_ms"), step_ms, f"{where}.end_ms"
    )
    if not warmup_ms <= start_ms < end_ms:
        raise PatternLabDataError(
            f"{where}: requires warmup_start_ms <= start_ms < end_ms, got "
            f"{warmup_ms} / {start_ms} / {end_ms}."
        )
    return {
        "fingerprint_version": version,
        "instrument_id": instrument_id,
        "venue": venue,
        "contract": contract,
        "quote_currency": currency,
        "volume_unit": VOLUME_UNIT,
        "base_timeframe_minutes": base,
        "timeframe_minutes": minutes,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "warmup_start_ms": warmup_ms,
        "resampling_policy": RESAMPLING_POLICY,
        "missing_bar_policy": MISSING_BAR_POLICY,
    }


def fingerprint_header(
    *,
    instrument_id: str,
    venue: str,
    contract: str,
    quote_currency: str,
    timeframe_minutes: int,
    start_ms: int,
    end_ms: int,
    warmup_start_ms: int,
) -> dict[str, Any]:
    """Build the frozen v1 fingerprint header (closed key set, exact JSON types).

    Arguments are normalized here; the result is then checked by
    :func:`validate_fingerprint_header`, so inconsistent identity or an invalid
    requested interval is rejected by the same rules a direct caller meets.
    """
    return validate_fingerprint_header(
        {
            "fingerprint_version": FINGERPRINT_VERSION,
            "instrument_id": normalize_instrument_id(instrument_id),
            "venue": normalize_id_part(venue, "venue"),
            "contract": normalize_id_part(contract, "contract"),
            "quote_currency": require_currency(quote_currency, "quote_currency"),
            "volume_unit": VOLUME_UNIT,
            "base_timeframe_minutes": BASE_TIMEFRAME_MINUTES,
            "timeframe_minutes": normalize_timeframe_minutes(timeframe_minutes),
            "start_ms": require_int(start_ms, "start_ms"),
            "end_ms": require_int(end_ms, "end_ms"),
            "warmup_start_ms": require_int(warmup_start_ms, "warmup_start_ms"),
            "resampling_policy": RESAMPLING_POLICY,
            "missing_bar_policy": MISSING_BAR_POLICY,
        }
    )


def input_fingerprint(
    *,
    header: Mapping[str, Any],
    timestamps: Any,
    ohlcv: Any,
) -> str:
    """Return the versioned SHA-256 identity of one consumed raw 5m input.

    The header and the raw arrays are validated first: a malformed header, an
    invalid series or a row outside the header's ``[warmup_start_ms, end_ms)``
    interval is rejected rather than hashed.  Gaps and incomplete groups remain
    valid, and every supplied raw row is hashed before resampling.  The digest
    consumes the canonical header bytes, then the raw consumed row count,
    timestamps and one interleaved ``(N, 5)`` float64 array.  Physical provenance,
    roles, universe membership, paths and verification flags are excluded.
    """
    canonical = validate_fingerprint_header(header)
    stamps, values = validate_consumed_rows(timestamps, ohlcv, where="fingerprint")
    if stamps.size and (stamps[0] < canonical["warmup_start_ms"] or stamps[-1] >= canonical["end_ms"]):
        raise PatternLabDataError(
            "fingerprint: consumed rows must lie in "
            f"[{format_epoch_ms(canonical['warmup_start_ms'])}, {format_epoch_ms(canonical['end_ms'])}); "
            f"got [{format_epoch_ms(int(stamps[0]))}, {format_epoch_ms(int(stamps[-1]))}]."
        )
    payload = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    stamps = np.ascontiguousarray(stamps, dtype="<i8")
    rows = np.ascontiguousarray(values, dtype="<f8")
    digest = hashlib.sha256()
    digest.update(struct.pack("<Q", len(payload)))
    digest.update(payload)
    digest.update(struct.pack("<Q", int(stamps.size)))
    digest.update(stamps.tobytes())
    digest.update(rows.tobytes())
    return digest.hexdigest()


# --------------------------------------------------------------------------
# fixed-range reads
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class DataSlice:
    """One instrument's fixed-interval bars plus identity and coverage evidence."""

    instrument_id: str
    symbol: str
    venue: str
    contract: str
    quote_currency: str
    timeframe_minutes: int
    warmup_start: datetime
    start: datetime
    end: datetime
    bars: pd.DataFrame
    research_mask: np.ndarray
    research_start_index: int
    segment_start: np.ndarray
    base_row_count: int
    base_gap_count: int
    omitted_group_count: int
    fingerprint_version: int
    input_fingerprint: str
    physical: Mapping[str, Any]


def research_bars(data_slice: DataSlice) -> pd.DataFrame:
    """Return only the research interval, excluding resolved warmup bars."""
    return data_slice.bars.iloc[data_slice.research_start_index:]


def _read_pack_state(data_root: Path) -> tuple[dict[str, Any], bool]:
    return pack_manifest.read_manifest(data_root), pack_manifest.update_marker_path(data_root).exists()


def load_slice(
    data_root: Path,
    instrument_id: str,
    *,
    start: Any,
    end: Any,
    warmup_start: Any = None,
    timeframe_minutes: int = BASE_TIMEFRAME_MINUTES,
) -> DataSlice:
    """Load one instrument over the fixed half-open interval ``[start, end)``.

    ``warmup_start`` extends the consumed interval backwards; the consumed
    interval is ``[warmup_start, end)``.  All three boundaries must be aligned to
    the requested timeframe on the UTC epoch grid.  The declared coverage,
    quote-unit verification and closure cutoff must admit the whole consumed
    interval; nothing is shifted, shortened or filled to make a request succeed.
    """
    data_root = Path(data_root)
    if not data_root.is_dir():
        raise PatternLabDataError(f"{data_root}: data root is not an existing directory.")
    manifest_before, marker_before = _read_pack_state(data_root)
    if marker_before:
        raise PatternLabDataError(
            f"{data_root}: {pack_manifest.UPDATE_MARKER_NAME} is present; the pack is being updated."
        )
    if manifest_before["state"] != "ready":
        raise PatternLabDataError(
            f"{data_root}: manifest state is {manifest_before['state']!r}; only a ready pack can be read."
        )
    entry = pack_manifest.find_instrument(manifest_before, instrument_id)
    resolved_id = entry["instrument_id"]

    minutes = normalize_timeframe_minutes(timeframe_minutes)
    step_ms = minutes * 60_000
    start_ms = require_aligned(to_epoch_ms(start, "start"), step_ms, "start")
    end_ms = require_aligned(to_epoch_ms(end, "end"), step_ms, "end")
    warmup_ms = start_ms if warmup_start is None else require_aligned(
        to_epoch_ms(warmup_start, "warmup_start"), step_ms, "warmup_start"
    )
    if start_ms >= end_ms:
        raise PatternLabDataError(f"{resolved_id}: requires start < end, got {format_epoch_ms(start_ms)} >= {format_epoch_ms(end_ms)}.")
    if warmup_ms > start_ms:
        raise PatternLabDataError(
            f"{resolved_id}: requires warmup_start <= start, got {format_epoch_ms(warmup_ms)} > {format_epoch_ms(start_ms)}."
        )

    first_ms = to_epoch_ms(entry["first_open_utc"], f"{resolved_id}.first_open_utc")
    coverage_end_ms = to_epoch_ms(entry["coverage_end_utc"], f"{resolved_id}.coverage_end_utc")
    if warmup_ms < first_ms:
        raise PatternLabDataError(
            f"{resolved_id}: coverage starts at {format_epoch_ms(first_ms)}; the requested consumed interval "
            f"starts at {format_epoch_ms(warmup_ms)}."
        )
    if end_ms > coverage_end_ms:
        raise PatternLabDataError(
            f"{resolved_id}: coverage ends at {format_epoch_ms(coverage_end_ms)}; the requested end is "
            f"{format_epoch_ms(end_ms)}."
        )

    verification = entry["verification"]
    if verification["volume_quote_verified"] is not True:
        raise PatternLabDataError(
            f"{resolved_id}: quote-volume units are not verified; research reads of unverified rows are refused."
        )
    if verification["closed_before_utc"] is None:
        raise PatternLabDataError(
            f"{resolved_id}: final-candle closure is unknown; the consumed interval cannot be certified."
        )
    closure_ms = to_epoch_ms(verification["closed_before_utc"], f"{resolved_id}.closed_before_utc")
    if closure_ms < end_ms:
        raise PatternLabDataError(
            f"{resolved_id}: closure cutoff {format_epoch_ms(closure_ms)} is earlier than the requested end "
            f"{format_epoch_ms(end_ms)}; the consumed interval is not certified closed."
        )

    path = pack_manifest.resolve_pack_path(data_root, entry["file"], f"{resolved_id}.file")
    stamps, values = read_ohlcv_rows(path, start_ms=warmup_ms, end_ms=end_ms, allow_empty=True)

    manifest_after, marker_after = _read_pack_state(data_root)
    if marker_after:
        raise PatternLabDataError(
            f"{data_root}: {pack_manifest.UPDATE_MARKER_NAME} appeared during the read; the result would mix versions."
        )
    if (manifest_after["revision"], manifest_after["state"]) != (manifest_before["revision"], manifest_before["state"]):
        raise PatternLabDataError(
            f"{data_root}: the pack changed during the read "
            f"(revision {manifest_before['revision']}/{manifest_before['state']} -> "
            f"{manifest_after['revision']}/{manifest_after['state']}); no mixed result is returned."
        )

    fingerprint = input_fingerprint(
        header=fingerprint_header(
            instrument_id=resolved_id,
            venue=entry["venue"],
            contract=entry["contract"],
            quote_currency=entry["quote_currency"],
            timeframe_minutes=minutes,
            start_ms=start_ms,
            end_ms=end_ms,
            warmup_start_ms=warmup_ms,
        ),
        timestamps=stamps,
        ohlcv=values,
    )

    bar_stamps, bar_values, omitted = resample_complete_groups(stamps, values, minutes)
    research = bar_stamps >= start_ms
    if not research.any():
        raise PatternLabDataError(
            f"{resolved_id}: no complete {minutes}m research bar remains in "
            f"[{format_epoch_ms(start_ms)}, {format_epoch_ms(end_ms)}); "
            f"{int(stamps.size)} base rows were consumed with {omitted} incomplete groups."
        )

    index = pd.DatetimeIndex(pd.to_datetime(bar_stamps, unit="ms", utc=True), name="timestamp")
    frame = pd.DataFrame(bar_values, index=index, columns=list(OHLCV_COLUMNS), dtype=np.float64)
    segment_start = np.ones(bar_stamps.size, dtype=bool)
    if bar_stamps.size > 1:
        segment_start[1:] = np.diff(bar_stamps) > step_ms

    expected_base = (end_ms - warmup_ms) // BASE_STEP_MS
    return DataSlice(
        instrument_id=resolved_id,
        symbol=entry["symbol"],
        venue=entry["venue"],
        contract=entry["contract"],
        quote_currency=entry["quote_currency"],
        timeframe_minutes=minutes,
        warmup_start=from_epoch_ms(warmup_ms),
        start=from_epoch_ms(start_ms),
        end=from_epoch_ms(end_ms),
        bars=frame,
        research_mask=research,
        research_start_index=int(np.argmax(research)),
        segment_start=segment_start,
        base_row_count=int(stamps.size),
        base_gap_count=int(expected_base - stamps.size),
        omitted_group_count=omitted,
        fingerprint_version=FINGERPRINT_VERSION,
        input_fingerprint=fingerprint,
        physical={
            "manifest_revision": manifest_before["revision"],
            "manifest_state": manifest_before["state"],
            "manifest_generated_utc": manifest_before["generated_utc"],
            "file": entry["file"],
            "declared_file_sha256": entry["sha256"],
            "declared_row_count": entry["row_count"],
            "declared_first_open_utc": entry["first_open_utc"],
            "declared_coverage_end_utc": entry["coverage_end_utc"],
            "declared_missing_bar_count": entry["missing_bar_count"],
            "roles": list(entry["roles"]),
            "source": dict(entry["source"]),
            "verification": dict(entry["verification"]),
        },
    )


def slice_metadata(data_slice: DataSlice) -> dict[str, Any]:
    """Return the JSON-ready metadata, coverage and identity of a loaded slice."""
    stamps = data_slice.bars.index
    return {
        "instrument_id": data_slice.instrument_id,
        "symbol": data_slice.symbol,
        "venue": data_slice.venue,
        "contract": data_slice.contract,
        "quote_currency": data_slice.quote_currency,
        "volume_unit": VOLUME_UNIT,
        "base_timeframe_minutes": BASE_TIMEFRAME_MINUTES,
        "timeframe_minutes": data_slice.timeframe_minutes,
        "warmup_start_utc": pack_manifest.format_utc(data_slice.warmup_start),
        "start_utc": pack_manifest.format_utc(data_slice.start),
        "end_utc": pack_manifest.format_utc(data_slice.end),
        "bar_count": int(len(data_slice.bars)),
        "warmup_bar_count": int(data_slice.research_start_index),
        "research_bar_count": int(np.count_nonzero(data_slice.research_mask)),
        "research_start_index": int(data_slice.research_start_index),
        "first_bar_utc": pack_manifest.format_utc(stamps[0].to_pydatetime()),
        "last_bar_utc": pack_manifest.format_utc(stamps[-1].to_pydatetime()),
        "segment_count": int(np.count_nonzero(data_slice.segment_start)),
        "base_row_count": data_slice.base_row_count,
        "base_gap_count": data_slice.base_gap_count,
        "omitted_group_count": data_slice.omitted_group_count,
        "resampling_policy": RESAMPLING_POLICY,
        "missing_bar_policy": MISSING_BAR_POLICY,
        "fingerprint_version": data_slice.fingerprint_version,
        "input_fingerprint": data_slice.input_fingerprint,
        "physical_provenance": dict(data_slice.physical),
    }
