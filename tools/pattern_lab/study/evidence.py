"""Run layout, versioned evidence tables, identity and publication.

Immutable raw evidence and regenerable derived outputs are separate:
``derived/summary.json`` and ``derived/report.html`` are excluded from the
completion record's hash set, and everything else is written once.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shutil
from typing import Any, Mapping, Sequence
from uuid import uuid4

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from .. import data as pack_data
from .. import manifest as pack_manifest
from . import contracts

RUN_SCHEMA_VERSION = 1
BUNDLE_SCHEMA_VERSION = 1

SPEC_DIR = "spec"
JOBS_DIR = "jobs"
ADMITTED_DIR = "admitted"
SNAPSHOT_DIR = "spec/snapshots"
DERIVED_DIR = "derived"

REQUEST_FILE = "spec/request.json"
PROTOCOL_FILE = "spec/protocol.json"
FAMILY_FILE = "spec/family.json"
SOURCE_FILE = "spec/source.json"
PROVENANCE_FILE = "provenance.json"
STATUS_FILE = "status.json"
METRICS_FILE = "metrics.json"
COMPLETION_FILE = "completion.json"
SUMMARY_FILE = "derived/summary.json"
REPORT_FILE = "derived/report.html"

# Regenerable outputs are never part of the immutable evidence hash set.
DERIVED_FILES = (SUMMARY_FILE, REPORT_FILE)

TABLE_NAMES = ("conditions", "episodes", "emissions", "primitives")

STATE_ADMITTED = "admitted"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"
STATE_NOT_STARTED = "not_started"

TERMINAL_COMPLETED = "completed"
TERMINAL_FAILED = "failed"
TERMINAL_INTERRUPTED = "interrupted"


# --------------------------------------------------------------------------
# file helpers
# --------------------------------------------------------------------------

def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    """Write strict JSON (no NaN or Infinity) through a durable atomic replace."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pack_manifest.write_text_atomic(path, pack_manifest.dumps_json(payload) + "\n")


def read_json(path: Path) -> Any:
    return pack_manifest.read_json_file(Path(path))


def _arrow_schema(pa, name: str, frame: pd.DataFrame):
    fixed = {
        "instrument_id": pa.string(),
        "timeframe_minutes": pa.int32(),
        "condition_id": pa.string(),
        "variant_id": pa.string(),
        "model_instance_id": pa.string(),
        "case_id": pa.string(),
        "occurrence": pa.string(),
        "episode_id": pa.string(),
        "event_id": pa.string(),
        "anchor_open_ms": pa.int64(),
        "signal_time_ms": pa.int64(),
        "first_bar_open_ms": pa.int64(),
        "last_bar_open_ms": pa.int64(),
        "bar_count": pa.int64(),
        "eligible_anchor_count": pa.int64(),
        "horizon_minutes": pa.int64(),
        "entry_time_ms": pa.int64(),
        "exit_time_ms": pa.int64(),
        "entry_price": pa.float64(),
        "exit_price": pa.float64(),
        "path_high": pa.float64(),
        "path_low": pa.float64(),
        "value": pa.bool_(),
        "valid": pa.bool_(),
        "left_censored": pa.bool_(),
        "right_censored": pa.bool_(),
        "return_valid": pa.bool_(),
        "path_valid": pa.bool_(),
        "return_reason": pa.string(),
        "path_reason": pa.string(),
    }
    fields = []
    for column in frame.columns:
        if column in fixed:
            fields.append(pa.field(column, fixed[column]))
        elif column.endswith("__reason"):
            fields.append(pa.field(column, pa.string()))
        else:
            fields.append(pa.field(column, pa.float64()))
    return pa.schema(fields)


def write_table(path: Path, frame: pd.DataFrame, *, name: str) -> None:
    """Write one evidence table; an empty table keeps its declared schema."""
    pa, pq = pack_data.require_pyarrow()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = _arrow_schema(pa, name, frame)
    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    pq.write_table(table, path, compression=pack_data.PARQUET_COMPRESSION,
                   row_group_size=pack_data.ROW_GROUP_SIZE)
    pack_manifest.fsync_path(path)


def read_table(path: Path) -> pd.DataFrame:
    _pa, pq = pack_data.require_pyarrow()
    return pq.read_table(Path(path)).to_pandas()


# --------------------------------------------------------------------------
# run directory
# --------------------------------------------------------------------------

def create_run_directory(output_root: Path, *, data_root: Path) -> Path:
    """Create the run directory, refusing an existing target or an overlap.

    ``--output-root`` is exactly the run directory: no run-ID subdirectory is
    created inside it, and nothing is ever written inside the market pack.
    """
    run_root = Path(output_root).expanduser()
    pack_root = Path(data_root).expanduser().resolve()
    if run_root.exists():
        raise PatternLabDataError(
            f"{run_root}: the study output root must be a new directory; an existing target is "
            "never reused, overwritten or extended."
        )
    resolved = run_root.resolve()
    if resolved == pack_root or pack_root in resolved.parents or resolved in pack_root.parents:
        raise PatternLabDataError(
            f"{resolved}: the output root overlaps the market-data root {pack_root}; study output "
            "never goes inside the pack."
        )
    run_root.mkdir(parents=True, exist_ok=False)
    return resolved


def require_run_directory(run_root: Path) -> Path:
    root = Path(run_root).expanduser()
    if not root.is_dir():
        raise PatternLabDataError(
            f"{root}: run root is not an existing directory. Pass the exact run directory a study "
            "created."
        )
    return root.resolve()


# --------------------------------------------------------------------------
# identity
# --------------------------------------------------------------------------

def specification_identity(request_document: Mapping[str, Any], family: Mapping[str, Any]) -> str:
    """Semantic identity of the normalized request, protocol and planned family."""
    return contracts.semantic_digest(
        {"specification": request_document, "family": family, "version": RUN_SCHEMA_VERSION}
    )


def implementation_identity(source: Mapping[str, Any]) -> str:
    """Identity of the consumed source digests and numerical library versions."""
    return contracts.semantic_digest(
        {
            "core_source": source["core_source"],
            "extensions": source["extensions"],
            "library_versions": source["library_versions"],
            "version": RUN_SCHEMA_VERSION,
        }
    )


def data_input_identity(
    *,
    fingerprints: Sequence[Mapping[str, Any]],
    semantic_specification: Mapping[str, Any],
    universe: Sequence[Mapping[str, Any]],
    protocol: Mapping[str, Any],
) -> str:
    """Compose the final data-input identity of a completed run.

    Ordered per-instrument/timeframe fingerprints, the semantic settings, the
    selected universe with its roles and the protocol enter it.  Roots, worker
    count, data added outside the consumed interval and unrelated Git edits do
    not.
    """
    return contracts.semantic_digest(
        {
            "fingerprints": [
                {
                    "instrument_id": item["instrument_id"],
                    "timeframe_minutes": item["timeframe_minutes"],
                    "input_fingerprint": item["input_fingerprint"],
                }
                for item in fingerprints
            ],
            "specification": semantic_specification,
            "universe": [
                {"instrument_id": item["instrument_id"], "roles": list(item["roles"])}
                for item in universe
            ],
            "protocol": protocol,
            "version": RUN_SCHEMA_VERSION,
        }
    )


# --------------------------------------------------------------------------
# job admission and publication
# --------------------------------------------------------------------------

def admitted_path(run_root: Path, instrument_id: str) -> Path:
    return Path(run_root) / ADMITTED_DIR / f"{instrument_id}.json"


def job_path(run_root: Path, instrument_id: str) -> Path:
    return Path(run_root) / JOBS_DIR / instrument_id


def record_admission(run_root: Path, instrument_id: str, identity: Mapping[str, Any]) -> None:
    """Persist an admitted job's input identity before any calculation."""
    payload = dict(identity)
    payload["state"] = STATE_ADMITTED
    write_json(admitted_path(run_root, instrument_id), payload)


def record_job_state(
    run_root: Path, instrument_id: str, state: str, *, extra: Mapping[str, Any] | None = None
) -> None:
    path = admitted_path(run_root, instrument_id)
    payload = dict(read_json(path))
    payload["state"] = state
    if extra:
        payload.update(extra)
    write_json(path, payload)


def publish_job(
    run_root: Path,
    instrument_id: str,
    *,
    tables: Mapping[str, pd.DataFrame],
    stats: Mapping[str, Any],
) -> dict[str, Any]:
    """Write one completed instrument's tables, then publish them atomically.

    Everything is written into a directory this job owns and renamed into place
    only after every file and its digest exist, so a partially written job is
    never mistaken for a completed one.
    """
    run_root = Path(run_root)
    final = job_path(run_root, instrument_id)
    if final.exists():
        raise PatternLabDataError(f"{final}: this job has already been published.")
    staging = run_root / JOBS_DIR / f".{instrument_id}.tmp-{uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        files: list[dict[str, Any]] = []
        for name in sorted(tables):
            frame = tables[name]
            file_name = f"{name}.parquet"
            write_table(staging / file_name, frame, name=name)
            files.append(
                {
                    "name": file_name,
                    "table": name,
                    "row_count": int(len(frame)),
                    "sha256": file_digest(staging / file_name),
                }
            )
        bundle = {
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "instrument_id": instrument_id,
            "files": files,
            "stats": dict(stats),
        }
        write_json(staging / "bundle.json", bundle)
        pack_manifest.fsync_directory(staging)
        os.rename(staging, final)
        pack_manifest.fsync_directory(final.parent)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    bundle["bundle_sha256"] = file_digest(final / "bundle.json")
    return bundle


def verify_job_bundle(run_root: Path, instrument_id: str) -> dict[str, Any]:
    """Verify one published job's bundle and every file digest it records."""
    directory = job_path(run_root, instrument_id)
    bundle_file = directory / "bundle.json"
    if not bundle_file.is_file():
        raise PatternLabDataError(
            f"{directory}: the completed job bundle is missing its bundle.json record.",
            error_code="corrupt_evidence",
        )
    bundle = read_json(bundle_file)
    for record in bundle["files"]:
        path = directory / record["name"]
        if not path.is_file():
            raise PatternLabDataError(
                f"{path}: a recorded raw evidence file is missing.", error_code="corrupt_evidence"
            )
        actual = file_digest(path)
        if actual != record["sha256"]:
            raise PatternLabDataError(
                f"{path}: SHA-256 {actual} does not match the recorded {record['sha256']}; raw "
                "evidence cannot be regenerated.",
                error_code="corrupt_evidence",
            )
    bundle["bundle_sha256"] = file_digest(bundle_file)
    return bundle


# --------------------------------------------------------------------------
# status and completion
# --------------------------------------------------------------------------

def write_status(run_root: Path, status: Mapping[str, Any]) -> None:
    write_json(Path(run_root) / STATUS_FILE, status)


def read_status(run_root: Path) -> dict[str, Any]:
    return dict(read_json(Path(run_root) / STATUS_FILE))


def completion_path(run_root: Path) -> Path:
    return Path(run_root) / COMPLETION_FILE


def immutable_files(run_root: Path) -> list[str]:
    """Every immutable evidence file, relative to the run root and sorted."""
    root = Path(run_root)
    names: list[str] = []
    for relative in (REQUEST_FILE, PROTOCOL_FILE, FAMILY_FILE, SOURCE_FILE, PROVENANCE_FILE,
                     STATUS_FILE, METRICS_FILE):
        if (root / relative).is_file():
            names.append(relative)
    for directory in (SNAPSHOT_DIR, ADMITTED_DIR):
        base = root / directory
        if base.is_dir():
            names.extend(
                sorted(str(path.relative_to(root).as_posix()) for path in base.iterdir() if path.is_file())
            )
    jobs = root / JOBS_DIR
    if jobs.is_dir():
        for job in sorted(jobs.iterdir()):
            if not job.is_dir() or job.name.startswith("."):
                continue
            names.extend(
                sorted(str(path.relative_to(root).as_posix()) for path in job.iterdir() if path.is_file())
            )
    return sorted(names)


def write_completion(run_root: Path, *, summary: Mapping[str, Any]) -> dict[str, Any]:
    """Hash the immutable evidence and terminal status, then write the record last.

    The record never hashes itself and never hashes the regenerable derived
    outputs.
    """
    root = Path(run_root)
    digests = {name: file_digest(root / name) for name in immutable_files(root)}
    record = {
        "schema_version": RUN_SCHEMA_VERSION,
        "terminal_status": TERMINAL_COMPLETED,
        "evidence_sha256": digests,
        "evidence_set_sha256": contracts.semantic_digest(digests),
        "derived_files": list(DERIVED_FILES),
        **dict(summary),
    }
    write_json(completion_path(root), record)
    return record


def verify_completion(run_root: Path) -> dict[str, Any]:
    """Verify a completed run's immutable evidence against its completion record."""
    root = Path(run_root)
    path = completion_path(root)
    if not path.is_file():
        raise PatternLabDataError(
            f"{root}: this run has no completion record, so it is not a completed study. Use "
            "load_results(run_root, allow_partial=True) to inspect an incomplete run.",
            error_code="incomplete_run",
        )
    record = dict(read_json(path))
    recorded = dict(record["evidence_sha256"])
    present = set(immutable_files(root))
    missing = sorted(set(recorded) - present)
    unexpected = sorted(present - set(recorded))
    if missing:
        raise PatternLabDataError(
            f"{root}: recorded raw evidence files are missing: {missing}.",
            error_code="corrupt_evidence",
        )
    if unexpected:
        raise PatternLabDataError(
            f"{root}: unrecorded files appeared inside the immutable evidence: {unexpected}.",
            error_code="corrupt_evidence",
        )
    for name, expected in sorted(recorded.items()):
        actual = file_digest(root / name)
        if actual != expected:
            raise PatternLabDataError(
                f"{root / name}: SHA-256 {actual} does not match the completion record's {expected}; "
                "changed raw evidence cannot be regenerated.",
                error_code="corrupt_evidence",
            )
    return record


def replace_derived(run_root: Path, *, summary: Mapping[str, Any], html: str) -> None:
    """Atomically replace only the regenerable derived outputs."""
    root = Path(run_root)
    (root / DERIVED_DIR).mkdir(parents=True, exist_ok=True)
    write_json(root / SUMMARY_FILE, summary)
    pack_manifest.write_text_atomic(root / REPORT_FILE, html)
