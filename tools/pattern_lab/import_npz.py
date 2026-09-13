"""One-way importer for the historical Pattern Lab prototype NPZ pack.

The importer preserves archival values and provenance.  It does not promise a
research-readable pack: without explicit closure evidence the published
instruments keep ``closed_before_utc = null`` and every research read of them is
refused.  Prototype modules are never imported or executed; only their data
files and manifest are read.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
import re
from typing import Any, Iterator, Mapping

import numpy as np

from . import PatternLabDataError
from . import manifest as pack_manifest
from .data import InstrumentSource, publish_pack
from .manifest import (
    BASE_STEP_MS,
    build_instrument_id,
    build_source_metadata,
    build_universe,
    build_verification,
    file_sha256,
    format_epoch_ms,
    read_json_file,
    require_aligned,
    require_currency,
    require_int,
    require_text,
    to_epoch_ms,
)

LEGACY_MANIFEST_NAME = "MANIFEST.json"
LEGACY_SERIES_DIR = "5m"
LEGACY_ROLE_TRANSLATION = {
    "trading": ("trading",),
    "research_only": ("research_only",),
    "factor": ("factor",),
}
SOURCE_METADATA_SCHEMA_VERSION = 1
SOURCE_METADATA_REQUIRED = ("quote_currency", "volume_unit_evidence", "evidence_source")
SOURCE_METADATA_OPTIONAL = ("closed_before_utc", "closure_evidence", "closure_source")

_INSTRUMENT_ID_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]*_[A-Z0-9][A-Z0-9.-]*$")
_NPZ_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.npz$")


# --------------------------------------------------------------------------
# quote-unit and closure evidence sidecar
# --------------------------------------------------------------------------

def validate_source_metadata(raw: Any, *, source: str = "source metadata") -> dict[str, Any]:
    """Validate the closed source-metadata schema required for every import."""
    if not isinstance(raw, Mapping):
        raise PatternLabDataError(f"{source}: expected a JSON object.")
    keys = set(raw)
    expected = {"schema_version", "instruments"}
    if keys != expected:
        raise PatternLabDataError(
            f"{source}: top-level keys must be exactly {sorted(expected)}, got {sorted(keys)}."
        )
    version = raw["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise PatternLabDataError(f"{source}.schema_version: expected an integer.")
    if version != SOURCE_METADATA_SCHEMA_VERSION:
        raise PatternLabDataError(
            f"{source}.schema_version: unsupported version {version}; this build reads "
            f"{SOURCE_METADATA_SCHEMA_VERSION}."
        )
    instruments = raw["instruments"]
    if not isinstance(instruments, Mapping) or not instruments:
        raise PatternLabDataError(f"{source}.instruments: expected a nonempty object keyed by instrument ID.")

    validated: dict[str, dict[str, Any]] = {}
    for instrument_id, entry in instruments.items():
        where = f"{source}.instruments[{instrument_id!r}]"
        if not _INSTRUMENT_ID_RE.fullmatch(instrument_id):
            raise PatternLabDataError(
                f"{where}: keys must be normalized <VENUE>_<CONTRACT> identifiers, got {instrument_id!r}."
            )
        if not isinstance(entry, Mapping):
            raise PatternLabDataError(f"{where}: expected an object.")
        unknown = set(entry) - set(SOURCE_METADATA_REQUIRED) - set(SOURCE_METADATA_OPTIONAL)
        if unknown:
            raise PatternLabDataError(f"{where}: unknown keys {sorted(unknown)}.")
        missing = [name for name in SOURCE_METADATA_REQUIRED if name not in entry]
        if missing:
            raise PatternLabDataError(f"{where}: missing required keys {missing}.")
        record = {
            "quote_currency": require_currency(entry["quote_currency"], f"{where}.quote_currency"),
            "volume_unit_evidence": require_text(entry["volume_unit_evidence"], f"{where}.volume_unit_evidence"),
            "evidence_source": require_text(entry["evidence_source"], f"{where}.evidence_source"),
            "closed_before_utc": None,
            "closure_evidence": None,
            "closure_source": None,
        }
        cutoff = entry.get("closed_before_utc")
        evidence = entry.get("closure_evidence")
        origin = entry.get("closure_source")
        if cutoff is None:
            if evidence is not None or origin is not None:
                raise PatternLabDataError(
                    f"{where}: partial closure evidence; closure_evidence/closure_source require a "
                    "non-null closed_before_utc."
                )
        else:
            cutoff_ms = require_aligned(
                to_epoch_ms(cutoff, f"{where}.closed_before_utc"), BASE_STEP_MS, f"{where}.closed_before_utc"
            )
            record["closed_before_utc"] = format_epoch_ms(cutoff_ms)
            record["closure_evidence"] = require_text(evidence, f"{where}.closure_evidence")
            record["closure_source"] = require_text(origin, f"{where}.closure_source")
        validated[instrument_id] = record
    return {"schema_version": version, "instruments": validated}


def load_source_metadata(path: Path) -> dict[str, Any]:
    """Read and validate the source-metadata sidecar required by every import."""
    path = Path(path)
    return validate_source_metadata(read_json_file(path, source=str(path)), source=str(path))


# --------------------------------------------------------------------------
# legacy manifest and NPZ reading
# --------------------------------------------------------------------------

def _validate_legacy_relative_file(value: Any, where: str) -> str:
    text = require_text(value, where)
    if "\\" in text or text.startswith("/") or ":" in text:
        raise PatternLabDataError(f"{where}: expected a relative POSIX path, got {text!r}.")
    parts = PurePosixPath(text).parts
    if len(parts) != 2 or parts[0] != LEGACY_SERIES_DIR or not _NPZ_NAME_RE.fullmatch(parts[1]):
        raise PatternLabDataError(f"{where}: expected '{LEGACY_SERIES_DIR}/<name>.npz', got {text!r}.")
    return text


def _resolve_source_file(source_root: Path, relative: str, where: str) -> Path:
    root = Path(source_root).resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root):
        raise PatternLabDataError(f"{where}: {relative!r} resolves outside the source root {root}.")
    return target


def read_legacy_manifest(source_root: Path) -> list[dict[str, Any]]:
    """Read the prototype MANIFEST.json and return its validated series entries."""
    source_root = Path(source_root)
    path = source_root / LEGACY_MANIFEST_NAME
    raw = read_json_file(path, source=str(path))
    if not isinstance(raw, Mapping):
        raise PatternLabDataError(f"{path}: expected a JSON object.")
    version = raw.get("manifest_version")
    if isinstance(version, bool) or not isinstance(version, int) or version != 1:
        raise PatternLabDataError(
            f"{path}.manifest_version: only the version 1 prototype layout is supported, got {version!r}."
        )
    series = raw.get("series")
    if not isinstance(series, list) or not series:
        raise PatternLabDataError(f"{path}.series: expected a nonempty list.")

    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(series):
        where = f"{path}.series[{index}]"
        if not isinstance(item, Mapping):
            raise PatternLabDataError(f"{where}: expected an object.")
        role = require_text(item.get("role"), f"{where}.role")
        if role not in LEGACY_ROLE_TRANSLATION:
            raise PatternLabDataError(
                f"{where}.role: unknown legacy role {role!r}; known roles are {sorted(LEGACY_ROLE_TRANSLATION)}."
            )
        instrument_id = build_instrument_id(item.get("venue"), item.get("contract"), where=where)
        if instrument_id in seen:
            raise PatternLabDataError(f"{where}: duplicate instrument {instrument_id!r}.")
        seen.add(instrument_id)
        entries.append(
            {
                "instrument_id": instrument_id,
                "symbol": require_text(item.get("symbol"), f"{where}.symbol"),
                "venue": item["venue"],
                "contract": item["contract"],
                "role": role,
                "file": _validate_legacy_relative_file(item.get("file"), f"{where}.file"),
                "bars": require_int(item.get("bars"), f"{where}.bars", minimum=1),
                "first_ts_ms": require_int(item.get("first_ts_ms"), f"{where}.first_ts_ms"),
                "last_ts_ms": require_int(item.get("last_ts_ms"), f"{where}.last_ts_ms"),
                "sha256": pack_manifest.require_sha256(item.get("sha256"), f"{where}.sha256"),
                "source_endpoint": item.get("source_endpoint"),
                "where": where,
            }
        )
    return entries


def _read_scalar_text(archive, name: str, where: str) -> str | None:
    if name not in archive.files:
        return None
    value = archive[name]
    if value.dtype.kind != "U" or value.ndim != 0:
        raise PatternLabDataError(f"{where}.{name}: expected a zero-dimensional Unicode scalar, got {value.dtype!r}.")
    return str(value.item())


def read_legacy_series(path: Path, entry: Mapping[str, Any]) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Read one prototype NPZ file without pickle and check it against the manifest."""
    where = f"{entry['instrument_id']} ({path.name})"
    if not path.is_file():
        raise PatternLabDataError(f"{where}: declared source file {path} is missing.")
    digest = file_sha256(path)
    if digest != entry["sha256"]:
        raise PatternLabDataError(
            f"{where}: SHA-256 {digest} does not match the source manifest digest {entry['sha256']}."
        )
    with np.load(path, allow_pickle=False) as archive:
        for name in ("ts", "ohlcv"):
            if name not in archive.files:
                raise PatternLabDataError(f"{where}: NPZ archive is missing the {name!r} array.")
        try:
            stamps = archive["ts"]
            values = archive["ohlcv"]
            venue_scalar = _read_scalar_text(archive, "ex", where)
            symbol_scalar = _read_scalar_text(archive, "sym", where)
        except ValueError as exc:
            raise PatternLabDataError(f"{where}: unreadable without pickle ({exc}).") from exc

    if stamps.dtype != np.int64 or stamps.ndim != 1:
        raise PatternLabDataError(f"{where}: 'ts' must be a one-dimensional int64 array, got {stamps.dtype}{stamps.shape}.")
    dtype_name = str(values.dtype)
    if values.dtype not in (np.float32, np.float64):
        raise PatternLabDataError(f"{where}: 'ohlcv' must be float32 or float64, got {values.dtype}.")
    if values.ndim != 2 or values.shape != (stamps.size, 5):
        raise PatternLabDataError(
            f"{where}: 'ohlcv' must have shape ({stamps.size}, 5), got {values.shape}."
        )
    if venue_scalar is not None and venue_scalar.upper() != entry["instrument_id"].split("_", 1)[0]:
        raise PatternLabDataError(
            f"{where}: NPZ 'ex' scalar {venue_scalar!r} does not match the manifest venue {entry['venue']!r}."
        )
    if symbol_scalar is not None and symbol_scalar != entry["symbol"]:
        raise PatternLabDataError(
            f"{where}: NPZ 'sym' scalar {symbol_scalar!r} does not match the manifest symbol {entry['symbol']!r}."
        )

    order = np.argsort(stamps, kind="stable")
    resorted = not np.array_equal(order, np.arange(stamps.size))
    stamps = np.ascontiguousarray(stamps[order], dtype=np.int64)
    values = np.ascontiguousarray(values[order].astype(np.float64), dtype=np.float64)

    if int(stamps.size) != entry["bars"]:
        raise PatternLabDataError(f"{where}: {stamps.size} rows do not match the manifest count {entry['bars']}.")
    if int(stamps[0]) != entry["first_ts_ms"] or int(stamps[-1]) != entry["last_ts_ms"]:
        raise PatternLabDataError(
            f"{where}: observed range [{int(stamps[0])}, {int(stamps[-1])}] does not match the manifest range "
            f"[{entry['first_ts_ms']}, {entry['last_ts_ms']}]."
        )
    facts = {"source_hash": digest, "timestamps_resorted": resorted, "input_dtype": dtype_name}
    return stamps, values, facts


def iter_instrument_sources(
    source_root: Path, entries: list[Mapping[str, Any]], source_metadata: Mapping[str, Any]
) -> Iterator[InstrumentSource]:
    """Yield one validated InstrumentSource at a time; no universe cube is built."""
    declared = source_metadata["instruments"]
    for entry in entries:
        instrument_id = entry["instrument_id"]
        evidence = declared[instrument_id]
        path = _resolve_source_file(source_root, entry["file"], f"{instrument_id}.file")
        stamps, values, facts = read_legacy_series(path, entry)
        dtype_name = facts["input_dtype"]
        reference = f"{entry['file']} in {LEGACY_MANIFEST_NAME} of the Pattern Lab prototype NPZ pack"
        endpoint = entry.get("source_endpoint")
        if isinstance(endpoint, str) and endpoint.strip():
            reference = f"{reference}; source endpoint: {endpoint.strip()}"
        yield InstrumentSource(
            symbol=entry["symbol"],
            venue=entry["venue"],
            contract=entry["contract"],
            quote_currency=evidence["quote_currency"],
            roles=LEGACY_ROLE_TRANSLATION[entry["role"]],
            timestamps=stamps,
            ohlcv=values,
            source=build_source_metadata(
                input_format="npz",
                input_dtype=dtype_name,
                source_reference=reference,
                volume_unit_evidence=evidence["volume_unit_evidence"],
                source_hash=facts["source_hash"],
                extra={
                    "evidence_source": evidence["evidence_source"],
                    "legacy_role": entry["role"],
                    "timestamps_resorted": facts["timestamps_resorted"],
                    "float32_promoted": dtype_name == "float32",
                },
            ),
            verification=build_verification(
                volume_quote_verified=True,
                volume_quote_evidence=f"{evidence['volume_unit_evidence']} (evidence source: {evidence['evidence_source']})",
                closed_before_utc=evidence["closed_before_utc"],
                closure_evidence=evidence["closure_evidence"],
                closure_source=evidence["closure_source"],
            ),
        )


def import_npz_pack(
    source_root: Path,
    output_root: Path,
    *,
    source_metadata: Mapping[str, Any],
    universe: Mapping[str, Any] | None = None,
    generated_utc: Any = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Convert the prototype NPZ pack into a new Parquet pack, preserving values.

    Source files are only read.  Old fractional splits, gap-fill policy, free-text
    USD labels and untouched-holdout claims are deliberately not carried into the
    new operational contract.
    """
    source_root = Path(source_root)
    output_root = Path(output_root)
    metadata = validate_source_metadata(source_metadata)
    entries = read_legacy_manifest(source_root)

    declared = set(metadata["instruments"])
    observed = {entry["instrument_id"] for entry in entries}
    if declared != observed:
        missing = sorted(observed - declared)
        extra = sorted(declared - observed)
        raise PatternLabDataError(
            "source metadata instrument IDs must match the source series exactly; "
            f"missing {missing}, unexpected {extra}."
        )

    legacy_manifest_hash = file_sha256(source_root / LEGACY_MANIFEST_NAME)
    resolved_universe = universe or build_universe(
        selection_source=f"Pattern Lab prototype NPZ pack {LEGACY_MANIFEST_NAME} (sha256 {legacy_manifest_hash})",
        selection_date=None,
        historical_membership="unknown",
        notes=(
            "Imported from the prototype NPZ pack. The pack generation date is not a universe "
            "selection date, and point-in-time historical membership is not verified."
        ),
    )
    summary = publish_pack(
        output_root,
        iter_instrument_sources(source_root, entries, metadata),
        universe=resolved_universe,
        generated_utc=generated_utc or datetime.now(timezone.utc).replace(microsecond=0),
        source={
            "kind": "legacy_npz_import",
            "source_root": str(source_root.resolve()),
            "source_manifest": LEGACY_MANIFEST_NAME,
            "source_manifest_sha256": legacy_manifest_hash,
            "series_count": len(entries),
        },
        note=note,
        source_root=source_root,
    )
    summary["source_root"] = str(source_root.resolve())
    summary["source_manifest_sha256"] = legacy_manifest_hash
    return summary
