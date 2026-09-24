"""Run layout, versioned evidence tables, identity and publication.

Immutable raw evidence and regenerable derived outputs are separate:
``derived/summary.json`` and ``derived/report.html`` are excluded from the
completion record's hash set, and everything else is written once.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
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
CURRENT_RUN_SCHEMA_VERSION = 2
SUPPORTED_RUN_SCHEMA_VERSIONS = (1, 2)
CONTEXT_ADMISSION_FILE = "spec/context.json"
CONTEXT_FILE = "context.json"
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

# The published completion record of schema v1.  Every one of these fields is
# validated before the record is treated as a verified completion.
COMPLETION_COUNT_KEYS = ("planned", "admitted", "completed", "failed", "not_started")
COMPLETION_IDENTITY_KEYS = (
    "specification_sha256",
    "implementation_sha256",
    "data_input_sha256",
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


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
    if name.startswith("sequential_"):
        from .sequential import SCHEMAS
        return SCHEMAS[name.removeprefix("sequential_")]
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
    if Path(path).stem.startswith("sequential_"):
        from .sequential import check_physical
        table = pq.read_table(Path(path))
        check_physical(table, Path(path).stem.removeprefix("sequential_"))
        return table.to_pandas(types_mapper=pd.ArrowDtype)
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

def specification_identity(request_document: Mapping[str, Any], family: Mapping[str, Any], *, run_version: int = 1) -> str:
    """Semantic identity of the normalized request, protocol and planned family."""
    return contracts.semantic_digest(
        {"specification": request_document, "family": family, "version": run_version}
    )


def implementation_identity(source: Mapping[str, Any], *, run_version: int = 1) -> str:
    """Identity of the consumed source digests and numerical library versions."""
    return contracts.semantic_digest(
        {
            "core_source": source["core_source"],
            **({"bracket_source":source["bracket_source"]} if "bracket_source" in source else {}),
            "extensions": source["extensions"],
            "library_versions": source["library_versions"],
            "version": run_version,
        }
    )


def data_input_identity(
    *,
    fingerprints: Sequence[Mapping[str, Any]],
    semantic_specification: Mapping[str, Any],
    universe: Sequence[Mapping[str, Any]],
    protocol: Mapping[str, Any],
    run_version: int = 1,
    context: Mapping[str, Any] | None = None,
    bracket_rules: Mapping[str, Any] | None = None,
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
            "version": run_version,
            **({"context": dict(context or {})} if run_version == 2 else {}),
            **({"bracket_rules":dict(bracket_rules)} if bracket_rules is not None else {}),
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


def read_job_records(run_root: Path) -> dict[str, dict[str, Any]]:
    """Read every committed per-job admission record of a run.

    These small records are written as each job is admitted, published or
    failed, so explicit partial inspection sees committed progress without
    trusting a run-level status that may be older.
    """
    directory = Path(run_root) / ADMITTED_DIR
    records: dict[str, dict[str, Any]] = {}
    if not directory.is_dir():
        return records
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.suffix != ".json":
            continue
        payload = read_json(path)
        if not isinstance(payload, Mapping) or "instrument_id" not in payload:
            raise PatternLabDataError(
                f"{path}: this admission record has no instrument_id; the run's recorded progress "
                "cannot be reconciled.",
                error_code="corrupt_evidence",
            )
        records[str(payload["instrument_id"])] = dict(payload)
    return records


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
    family = read_json(run_root / FAMILY_FILE)
    sequential_instances = [m for m in family["models"] if contracts.is_sequential(m)]
    sequential_names = {name for name in tables if name.startswith("sequential_")}
    expected_sequential = {"sequential_attempts", "sequential_trades", "sequential_path"} if sequential_instances else set()
    if sequential_names != expected_sequential:
        raise PatternLabDataError(f"{instrument_id}: sequential table coverage disagrees with the saved family")
    if sequential_instances:
        from . import sequential
        from ..manifest import to_epoch_ms
        request = read_json(run_root / REQUEST_FILE)
        admitted = read_json(admitted_path(run_root, instrument_id))
        sequential.validate({name:tables["sequential_"+name] for name in sequential.SCHEMAS},
            instrument_id=instrument_id, instances=sequential_instances,
            variants=family["variants"], emissions=tables["emissions"], rules=admitted["bracket_rules"],
            expected_bars=sequential.expected_bars(tables["conditions"], stats, to_epoch_ms(request["study"]["end_utc"])))
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


def immutable_files(run_root: Path, *, run_version: int = 1) -> list[str]:
    """Every immutable evidence file, relative to the run root and sorted."""
    root = Path(run_root)
    names: list[str] = []
    for relative in (REQUEST_FILE, PROTOCOL_FILE, FAMILY_FILE, SOURCE_FILE, PROVENANCE_FILE,
                     STATUS_FILE, METRICS_FILE,
                     *((CONTEXT_ADMISSION_FILE, CONTEXT_FILE) if run_version == 2 else ())):
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


def write_completion(run_root: Path, *, summary: Mapping[str, Any], run_version: int = 1) -> dict[str, Any]:
    """Hash the immutable evidence and terminal status, then write the record last.

    The record never hashes itself and never hashes the regenerable derived
    outputs.
    """
    root = Path(run_root)
    digests = {name: file_digest(root / name) for name in immutable_files(root, run_version=run_version)}
    record = {
        "schema_version": run_version,
        "terminal_status": TERMINAL_COMPLETED,
        "evidence_sha256": digests,
        "evidence_set_sha256": contracts.semantic_digest(digests),
        "derived_files": list(DERIVED_FILES),
        **dict(summary),
    }
    write_json(completion_path(root), record)
    return record


def _corrupt(message: str) -> PatternLabDataError:
    return PatternLabDataError(message, error_code="corrupt_evidence")


def _is_digest(value: Any) -> bool:
    """A SHA-256 in this evidence's own lowercase hexadecimal representation."""
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _is_count(value: Any) -> bool:
    """A nonnegative integer count; a boolean and a float are not counts."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def verify_completion(run_root: Path) -> dict[str, Any]:
    """Verify a completed run's published record and its immutable evidence.

    The whole published schema-v1 record is checked, not only the fields the
    file-digest pass happens to touch: a missing, mistyped or self-contradictory
    field is an actionable ``corrupt_evidence`` error rather than an incidental
    ``KeyError`` later on.  The aggregate digest is recomputed from the verified
    file map and compared by value.

    ``run_root`` is recorded physical provenance: it is checked as a string and
    never compared with the current directory, so moving a valid run between
    directories or hosts keeps working.  The regenerable derived outputs are
    named but not hashed — they may legitimately be absent or edited.  Facts
    that need the run's reconciled jobs, such as the counts and the identities,
    are cross-checked once at the results-loading boundary that owns them.
    """
    root = Path(run_root)
    path = completion_path(root)
    if not path.is_file():
        raise PatternLabDataError(
            f"{root}: this run has no completion record, so it is not a completed study. Use "
            "load_results(run_root, allow_partial=True) to inspect an incomplete run.",
            error_code="incomplete_run",
        )
    document = read_json(path)
    if not isinstance(document, Mapping):
        raise _corrupt(f"{path}: the completion record is not a JSON object.")
    record = dict(document)
    version = record.get("schema_version")
    # A boolean compares equal to an integer, so it is rejected explicitly.
    if type(version) is not int or version not in SUPPORTED_RUN_SCHEMA_VERSIONS:
        raise _corrupt(
            f"{path}: completion schema_version {version!r} is not the supported integer "
            f"{SUPPORTED_RUN_SCHEMA_VERSIONS}; this record cannot be verified."
        )
    if record.get("terminal_status") != TERMINAL_COMPLETED:
        raise _corrupt(
            f"{path}: the completion record's terminal status is "
            f"{record.get('terminal_status')!r}, not {TERMINAL_COMPLETED!r}."
        )
    digests = record.get("evidence_sha256")
    if not isinstance(digests, Mapping) or not all(
        isinstance(name, str) and _is_digest(value) for name, value in digests.items()
    ):
        raise _corrupt(f"{path}: the completion record has no usable evidence_sha256 mapping.")
    recorded = dict(digests)
    present = set(immutable_files(root, run_version=version))
    if version == 2 and not {CONTEXT_ADMISSION_FILE, CONTEXT_FILE} <= present:
        raise _corrupt(f"{root}: v2 context metadata is missing.")
    missing = sorted(set(recorded) - present)
    unexpected = sorted(present - set(recorded))
    if missing:
        raise _corrupt(f"{root}: recorded raw evidence files are missing: {missing}.")
    if unexpected:
        raise _corrupt(
            f"{root}: unrecorded files appeared inside the immutable evidence: {unexpected}."
        )
    for name, expected in sorted(recorded.items()):
        actual = file_digest(root / name)
        if actual != expected:
            raise _corrupt(
                f"{root / name}: SHA-256 {actual} does not match the completion record's "
                f"{expected}; changed raw evidence cannot be regenerated."
            )

    aggregate = record.get("evidence_set_sha256")
    if not _is_digest(aggregate):
        raise _corrupt(
            f"{path}: evidence_set_sha256 {aggregate!r} is not a SHA-256 digest; this record has "
            "no usable identity of its own."
        )
    expected_aggregate = contracts.semantic_digest(recorded)
    if aggregate != expected_aggregate:
        raise _corrupt(
            f"{path}: evidence_set_sha256 {aggregate} does not match the canonical digest "
            f"{expected_aggregate} of the verified evidence map it claims to summarize."
        )

    derived = record.get("derived_files")
    if (
        not isinstance(derived, list)
        or not all(isinstance(name, str) for name in derived)
        or len(derived) != len(set(derived))
        or set(derived) != set(DERIVED_FILES)
    ):
        raise _corrupt(
            f"{path}: derived_files {derived!r} does not describe the supported schema-v1 "
            f"regenerable outputs {list(DERIVED_FILES)}."
        )

    counts = record.get("counts")
    if not isinstance(counts, Mapping):
        raise _corrupt(f"{path}: the completion record has no counts mapping.")
    bad_counts = sorted(
        key for key in COMPLETION_COUNT_KEYS if not _is_count(counts.get(key))
    )
    if bad_counts:
        raise _corrupt(
            f"{path}: completion counts {bad_counts} are missing or are not nonnegative "
            f"integers: {dict(counts)!r}."
        )
    unfinished = {
        key: int(counts[key])
        for key in (STATE_ADMITTED, STATE_FAILED, STATE_NOT_STARTED)
        if int(counts[key])
    }
    if unfinished or int(counts["completed"]) != int(counts["planned"]):
        raise _corrupt(
            f"{path}: a completed run cannot record {dict(counts)!r}; every planned job must be "
            "completed, with no failed, admitted or not-started job left."
        )

    identities = record.get("identities")
    if not isinstance(identities, Mapping):
        raise _corrupt(f"{path}: the completion record has no identities mapping.")
    bad_identities = sorted(
        key for key in COMPLETION_IDENTITY_KEYS if not _is_digest(identities.get(key))
    )
    if bad_identities:
        raise _corrupt(
            f"{path}: completion identities {bad_identities} are missing or are not SHA-256 "
            "digests."
        )

    # Recorded physical provenance only: it is never resolved, required to
    # exist here, or required to use this host's path syntax.
    if not isinstance(record.get("run_root"), str) or not record["run_root"].strip():
        raise _corrupt(
            f"{path}: run_root {record.get('run_root')!r} is not a recorded provenance string."
        )
    return record


def replace_derived(run_root: Path, *, summary: Mapping[str, Any], html: str) -> None:
    """Atomically replace only the regenerable derived outputs."""
    root = Path(run_root)
    (root / DERIVED_DIR).mkdir(parents=True, exist_ok=True)
    write_json(root / SUMMARY_FILE, summary)
    pack_manifest.write_text_atomic(root / REPORT_FILE, html)


def remove_new_derived(run_root: Path) -> dict[str, list[str]]:
    """Best-effort removal of this run's own generated derived files.

    Only the two named regenerable outputs are unlinked; no directory is removed
    recursively.  The caller keeps its original diagnostic and reports what
    could not be cleaned, so a surviving derived report is never presented as
    evidence of a completed run.
    """
    root = Path(run_root)
    removed: list[str] = []
    retained: list[str] = []
    for relative in DERIVED_FILES:
        path = root / relative
        if not path.exists():
            continue
        try:
            # Only the two named files are unlinked; no directory is removed
            # recursively, so anything else is reported rather than destroyed.
            path.unlink()
        except OSError as exc:  # pragma: no cover - reported, never masking the cause
            retained.append(f"{relative}: {exc}")
        else:
            removed.append(relative)
    return {"removed": removed, "retained": retained}
