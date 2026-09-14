"""Durable staging, commit and recovery for one collector operation.

The existing ``.update-in-progress.json`` marker is the operation journal.  One
operation-owned staging directory under the same data root holds every completed
replacement until the whole target set is ready.  There is no whole-pack
snapshot, no general transaction engine and no backup service.

Two phases are sufficient:

``staging``
    The operation is recorded before any work.  Completed per-instrument
    artifacts are checkpointed as they are written.  Live OHLCV and the
    authoritative metadata stay untouched, so staging can always be aborted.

``applying``
    Entered durably only once every required artifact exists and verifies.  From
    there recovery only ever moves forward: changed files, then history and
    README, then the manifest last, then recorded cleanup, then the marker.

This module owns journal validation and the file mechanics.  It performs no
network access and never imports PyArrow, so a journal can be inspected in an
environment without the optional wheel.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from . import PatternLabDataError
from . import manifest as pack_manifest
from .manifest import (
    MANIFEST_NAME,
    README_NAME,
    UPDATE_MARKER_NAME,
    UPDATE_MARKER_TEMP_NAME,
    UPDATES_NAME,
    file_sha256,
    format_utc,
    require_int,
    require_sha256,
    require_text,
)

JOURNAL_VERSION = 1
STAGING_PREFIX = ".pack-staging-"
OPERATION_KINDS = ("collect", "update")
PHASES = ("staging", "applying")
TEXT_TARGETS = ("updates", "readme", "manifest")
TARGET_FINAL_NAMES = {"updates": UPDATES_NAME, "readme": README_NAME, "manifest": MANIFEST_NAME}

_OPERATION_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$")
_STAGED_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def new_operation_id(moment: datetime) -> str:
    """Return a sortable, filesystem-safe operation identifier."""
    stamp = moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid4().hex[:12]}"


def marker_path(data_root: Path) -> Path:
    return Path(data_root) / UPDATE_MARKER_NAME


def marker_temp_path(data_root: Path) -> Path:
    return Path(data_root) / UPDATE_MARKER_TEMP_NAME


def staging_dir_name(operation_id: str) -> str:
    return f"{STAGING_PREFIX}{operation_id}"


def staging_dir(data_root: Path, operation_id: str) -> Path:
    return Path(data_root) / staging_dir_name(operation_id)


# --------------------------------------------------------------------------
# journal validation
# --------------------------------------------------------------------------

def _mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PatternLabDataError(
            f"{where}: expected an object, got {type(value).__name__}.", error_code="invalid_journal"
        )
    return dict(value)


def _journal_error(message: str) -> PatternLabDataError:
    return PatternLabDataError(message, error_code="invalid_journal")


def _relative_staged_path(value: Any, operation_id: str, where: str) -> str:
    """Validate a staged path: exactly one file inside this operation's directory."""
    text = require_text(value, where)
    if "\\" in text or text.startswith("/") or ":" in text:
        raise _journal_error(f"{where}: {text!r} is not a relative POSIX pack path.")
    parts = PurePosixPath(text).parts
    if len(parts) != 2 or parts[0] != staging_dir_name(operation_id):
        raise _journal_error(
            f"{where}: {text!r} must be '{staging_dir_name(operation_id)}/<name>'."
        )
    if not _STAGED_NAME_RE.fullmatch(parts[1]):
        raise _journal_error(f"{where}: unsafe staged file name {parts[1]!r}.")
    return text


def _validate_staged_record(raw: Any, operation_id: str, where: str) -> dict[str, Any]:
    record = _mapping(raw, where)
    instrument_id = pack_manifest.normalize_instrument_id(
        record.get("instrument_id"), f"{where}.instrument_id"
    )
    changed = record.get("changed")
    if not isinstance(changed, bool):
        raise _journal_error(f"{where}.changed: expected a boolean.")
    old = record.get("old_sha256")
    staged = record.get("staged_path")
    validated = {
        "instrument_id": instrument_id,
        "final_path": pack_manifest.validate_relative_file(record.get("final_path"), f"{where}.final_path"),
        "staged_path": None
        if staged is None
        else _relative_staged_path(staged, operation_id, f"{where}.staged_path"),
        "old_sha256": None if old is None else require_sha256(old, f"{where}.old_sha256"),
        "new_sha256": require_sha256(record.get("new_sha256"), f"{where}.new_sha256"),
        "changed": changed,
        "entry": _mapping(record.get("entry"), f"{where}.entry"),
        "facts": _mapping(record.get("facts"), f"{where}.facts"),
    }
    if changed and validated["staged_path"] is None:
        raise _journal_error(f"{where}: a changed instrument must record its staged replacement path.")
    if not changed and validated["old_sha256"] != validated["new_sha256"]:
        raise _journal_error(f"{where}: an unchanged instrument cannot change its digest.")
    extra = sorted(set(record) - set(validated))
    if extra:
        raise _journal_error(f"{where}: unexpected journal keys {extra}.")
    return validated


def _validate_text_target(raw: Any, key: str, operation_id: str, where: str) -> dict[str, Any]:
    target = _mapping(raw, where)
    old = target.get("old_sha256")
    validated = {
        "final_name": TARGET_FINAL_NAMES[key],
        "staged_path": _relative_staged_path(target.get("staged_path"), operation_id, f"{where}.staged_path"),
        "old_sha256": None if old is None else require_sha256(old, f"{where}.old_sha256"),
        "new_sha256": require_sha256(target.get("new_sha256"), f"{where}.new_sha256"),
    }
    if target.get("final_name") not in (None, TARGET_FINAL_NAMES[key]):
        raise _journal_error(f"{where}.final_name: must be {TARGET_FINAL_NAMES[key]!r}.")
    extra = sorted(set(target) - set(validated))
    if extra:
        raise _journal_error(f"{where}: unexpected journal keys {extra}.")
    return validated


def validate_journal(raw: Any, *, where: str = "journal") -> dict[str, Any]:
    """Validate the versioned operation journal before any journal-driven write."""
    journal = _mapping(raw, where)
    version = require_int(journal.get("journal_version"), f"{where}.journal_version")
    if version != JOURNAL_VERSION:
        raise _journal_error(
            f"{where}.journal_version: unsupported version {version}; this build writes {JOURNAL_VERSION}."
        )
    operation_id = require_text(journal.get("operation_id"), f"{where}.operation_id")
    if not _OPERATION_ID_RE.fullmatch(operation_id):
        raise _journal_error(f"{where}.operation_id: unsafe operation identifier {operation_id!r}.")
    kind = journal.get("kind")
    if kind not in OPERATION_KINDS:
        raise _journal_error(f"{where}.kind: expected one of {list(OPERATION_KINDS)}, got {kind!r}.")
    phase = journal.get("phase")
    if phase not in PHASES:
        raise _journal_error(f"{where}.phase: expected one of {list(PHASES)}, got {phase!r}.")
    if journal.get("staging_dir") != staging_dir_name(operation_id):
        raise _journal_error(
            f"{where}.staging_dir: must be {staging_dir_name(operation_id)!r}, "
            f"got {journal.get('staging_dir')!r}."
        )

    base = _mapping(journal.get("base"), f"{where}.base")
    base_revision = base.get("revision")
    base_files = _mapping(base.get("files"), f"{where}.base.files")
    validated_base = {
        "revision": None if base_revision is None else require_int(base_revision, f"{where}.base.revision", minimum=1),
        "manifest_sha256": _optional_sha(base.get("manifest_sha256"), f"{where}.base.manifest_sha256"),
        "readme_sha256": _optional_sha(base.get("readme_sha256"), f"{where}.base.readme_sha256"),
        "updates_sha256": _optional_sha(base.get("updates_sha256"), f"{where}.base.updates_sha256"),
        "files": {
            pack_manifest.normalize_instrument_id(key, f"{where}.base.files key"): require_sha256(
                value, f"{where}.base.files[{key}]"
            )
            for key, value in base_files.items()
        },
    }
    if kind == "collect" and validated_base["revision"] is not None:
        raise _journal_error(f"{where}.base.revision: an initial collect has no base revision.")
    if kind == "update" and validated_base["revision"] is None:
        raise _journal_error(f"{where}.base.revision: an update must record the base revision.")

    request = _mapping(journal.get("request"), f"{where}.request")
    staged_raw = _mapping(journal.get("staged"), f"{where}.staged")
    staged: dict[str, Any] = {}
    for key, value in staged_raw.items():
        record = _validate_staged_record(value, operation_id, f"{where}.staged[{key}]")
        if record["instrument_id"] != key:
            raise _journal_error(f"{where}.staged[{key}]: record declares {record['instrument_id']!r}.")
        staged[key] = record

    targets_raw = journal.get("targets")
    targets: dict[str, Any] = {}
    if targets_raw is not None:
        mapping = _mapping(targets_raw, f"{where}.targets")
        missing = sorted(set(TEXT_TARGETS) - set(mapping))
        extra = sorted(set(mapping) - set(TEXT_TARGETS))
        if missing or extra:
            raise _journal_error(
                f"{where}.targets: expected exactly {list(TEXT_TARGETS)}; missing {missing}, unexpected {extra}."
            )
        targets = {
            key: _validate_text_target(mapping[key], key, operation_id, f"{where}.targets.{key}")
            for key in TEXT_TARGETS
        }
    if phase == "applying" and not targets:
        raise _journal_error(f"{where}.targets: the applying phase requires frozen target metadata.")

    validated = {
        "journal_version": version,
        "operation_id": operation_id,
        "kind": kind,
        "root": require_text(journal.get("root"), f"{where}.root"),
        "staging_dir": staging_dir_name(operation_id),
        "operation_started_utc": format_utc(
            journal.get("operation_started_utc"), f"{where}.operation_started_utc"
        ),
        "request": request,
        "options": _mapping(journal.get("options"), f"{where}.options"),
        "universe": _mapping(journal.get("universe"), f"{where}.universe"),
        "roster": pack_manifest.validate_roster_entries(journal.get("roster"), f"{where}.roster"),
        "roster_sha256": require_sha256(journal.get("roster_sha256"), f"{where}.roster_sha256"),
        "base": validated_base,
        "target_revision": require_int(journal.get("target_revision"), f"{where}.target_revision", minimum=1),
        "phase": phase,
        "closure": _mapping(journal.get("closure"), f"{where}.closure"),
        "preflight": _mapping(journal.get("preflight"), f"{where}.preflight"),
        "staged": staged,
        "targets": targets or None,
    }
    if pack_manifest.roster_sha256(validated["roster"]) != validated["roster_sha256"]:
        raise _journal_error(f"{where}.roster_sha256: does not match the recorded roster.")
    extra = sorted(set(journal) - set(validated))
    if extra:
        raise _journal_error(f"{where}: unexpected journal keys {extra}.")
    return validated


def _optional_sha(value: Any, where: str) -> str | None:
    return None if value is None else require_sha256(value, where)


# --------------------------------------------------------------------------
# journal IO and pending detection
# --------------------------------------------------------------------------

def write_journal(data_root: Path, journal: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and durably publish the operation journal through its own temp."""
    validated = validate_journal(journal)
    pack_manifest.write_text_atomic(
        marker_path(data_root), pack_manifest.dumps_json(validated) + "\n"
    )
    return validated


def _read_journal_file(path: Path) -> dict[str, Any]:
    return validate_journal(pack_manifest.read_json_file(path, source=str(path)))


def pending_state(data_root: Path) -> dict[str, Any] | None:
    """Describe any pending operation without adopting or deleting it.

    Returns ``None`` when neither the canonical marker nor its initial temporary
    is present.  A malformed artifact is reported with ``valid=False`` and a
    problem description; the caller decides whether that is an error, because a
    malformed marker must be inspected manually rather than silently removed.
    """
    for source, path in (("marker", marker_path(data_root)), ("temporary", marker_temp_path(data_root))):
        if not path.is_file():
            continue
        try:
            journal = _read_journal_file(path)
        except PatternLabDataError as exc:
            return {"source": source, "path": str(path), "valid": False, "problem": str(exc), "journal": None}
        return {"source": source, "path": str(path), "valid": True, "problem": None, "journal": journal}
    return None


def pending_summary(state: Mapping[str, Any]) -> dict[str, Any]:
    """Return the JSON-ready status of a pending operation for inspect output."""
    if not state["valid"]:
        return {
            "source": state["source"],
            "path": state["path"],
            "valid": False,
            "problem": state["problem"],
        }
    journal = state["journal"]
    return {
        "source": state["source"],
        "path": state["path"],
        "valid": True,
        "problem": None,
        "operation_id": journal["operation_id"],
        "kind": journal["kind"],
        "phase": journal["phase"],
        "operation_started_utc": journal["operation_started_utc"],
        "request": dict(journal["request"]),
        "base_revision": journal["base"]["revision"],
        "target_revision": journal["target_revision"],
        "roster_size": len(journal["roster"]),
        "completed_instruments": sorted(journal["staged"]),
        "completed_instrument_count": len(journal["staged"]),
        "targets_frozen": journal["targets"] is not None,
    }


def pending_problem(state: Mapping[str, Any]) -> str:
    """Return the actionable one-line description used by readers and inspect."""
    if not state["valid"]:
        return (
            f"{state['path']}: a malformed pending-operation journal is present ({state['problem']}). "
            "Inspect it manually; Pattern Lab never removes or adopts it automatically."
        )
    journal = state["journal"]
    return (
        f"{state['path']}: operation {journal['operation_id']} ({journal['kind']}) is pending in the "
        f"{journal['phase']} phase; run 'recover' to finish it, or 'abort-update' while it is still "
        "staging. Research reads refuse a pending generation."
    )


# --------------------------------------------------------------------------
# staging directory and artifact helpers
# --------------------------------------------------------------------------

def ensure_staging_dir(data_root: Path, operation_id: str) -> Path:
    path = staging_dir(data_root, operation_id)
    if path.is_symlink():
        raise PatternLabDataError(
            f"{path}: the staging directory must not be a symlink.", error_code="unsafe_path"
        )
    path.mkdir(exist_ok=True)
    pack_manifest.fsync_directory(Path(data_root))
    return path


def resolve_staged(data_root: Path, relative: str, operation_id: str, where: str) -> Path:
    """Resolve one journalled staged path, rejecting escapes and symlinks."""
    _relative_staged_path(relative, operation_id, where)
    root = Path(data_root).resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root / staging_dir_name(operation_id)):
        raise _journal_error(f"{where}: {relative!r} resolves outside this operation's staging directory.")
    if Path(root / relative).is_symlink():
        raise PatternLabDataError(f"{where}: {relative!r} is a symlink.", error_code="unsafe_path")
    return Path(root / relative)


def write_staged_text(data_root: Path, operation_id: str, name: str, text: str) -> dict[str, Any]:
    """Write one frozen metadata artifact into the staging directory."""
    directory = ensure_staging_dir(data_root, operation_id)
    path = directory / name
    pack_manifest.write_text_atomic(path, text)
    return {
        "staged_path": f"{staging_dir_name(operation_id)}/{name}",
        "new_sha256": file_sha256(path),
    }


def digest_or_none(path: Path) -> str | None:
    return file_sha256(path) if Path(path).is_file() else None


# --------------------------------------------------------------------------
# applying: forward-only publication and recovery
# --------------------------------------------------------------------------

def _unrecoverable(message: str) -> PatternLabDataError:
    return PatternLabDataError(
        message
        + " Restore the exact missing staged artifact from an available copy, or collect a separate "
        "new root. Preserve this root and its journal; never clear the marker to make a partially "
        "replaced pack readable.",
        error_code="unrecoverable_staged_artifact",
    )


def _replace_file(
    data_root: Path,
    *,
    destination: Path,
    staged_relative: str | None,
    old_sha256: str | None,
    new_sha256: str,
    operation_id: str,
    where: str,
) -> bool:
    """Replace one destination from its staged artifact; return whether it moved.

    The destination must match either the recorded old digest (the step has not
    run) or the recorded new digest (it completed before the crash).  A third
    value is unexpected modification and blocks recovery.
    """
    current = digest_or_none(destination)
    if current == new_sha256:
        return False
    if current != old_sha256:
        raise PatternLabDataError(
            f"{where}: {destination} has SHA-256 {current} but the journal recorded "
            f"{old_sha256} before the replacement and {new_sha256} after it. The file was modified "
            "outside this operation; recovery stops rather than guessing.",
            error_code="unexpected_target_state",
        )
    if staged_relative is None:
        raise _unrecoverable(f"{where}: no staged replacement was recorded for {destination}.")
    staged = resolve_staged(data_root, staged_relative, operation_id, f"{where}.staged_path")
    if not staged.is_file():
        raise _unrecoverable(f"{where}: the staged artifact {staged} is missing.")
    actual = file_sha256(staged)
    if actual != new_sha256:
        raise PatternLabDataError(
            f"{where}: the staged artifact {staged} has SHA-256 {actual}, not the recorded "
            f"{new_sha256}.",
            error_code="unexpected_target_state",
        )
    os.replace(staged, destination)
    pack_manifest.fsync_directory(destination.parent)
    return True


def apply_operation(
    data_root: Path, journal: Mapping[str, Any], *, progress=None
) -> dict[str, Any]:
    """Publish the frozen targets, then clean up, then remove the marker last.

    This is the only forward path once ``applying`` has been recorded, and it is
    idempotent: every step recognizes its own completed result by digest, so a
    resumed operation publishes exactly one revision and one history event.
    """
    root = Path(data_root)
    if journal["phase"] != "applying":
        raise _journal_error("apply: the journal is not in the applying phase.")
    targets = journal["targets"]
    operation_id = journal["operation_id"]
    replaced: list[str] = []

    for instrument_id in sorted(journal["staged"]):
        record = journal["staged"][instrument_id]
        destination = pack_manifest.resolve_pack_path(
            root, record["final_path"], f"staged[{instrument_id}].final_path"
        )
        if not record["changed"]:
            actual = digest_or_none(destination)
            if actual != record["new_sha256"]:
                raise PatternLabDataError(
                    f"staged[{instrument_id}]: the unchanged file {destination} has SHA-256 {actual}, "
                    f"not the recorded {record['new_sha256']}.",
                    error_code="unexpected_target_state",
                )
            continue
        moved = _replace_file(
            root,
            destination=destination,
            staged_relative=record["staged_path"],
            old_sha256=record["old_sha256"],
            new_sha256=record["new_sha256"],
            operation_id=operation_id,
            where=f"staged[{instrument_id}]",
        )
        if moved:
            replaced.append(record["final_path"])
        if progress is not None:
            progress(f"applied {instrument_id}")

    # History and README first, the manifest last: a ready manifest is the only
    # signal that the whole generation is published.
    for key in TEXT_TARGETS:
        target = targets[key]
        _replace_file(
            root,
            destination=root / target["final_name"],
            staged_relative=target["staged_path"],
            old_sha256=target["old_sha256"],
            new_sha256=target["new_sha256"],
            operation_id=operation_id,
            where=f"targets.{key}",
        )

    _verify_targets(root, journal)
    cleanup_operation(root, journal)
    remove_marker(root)
    return {"replaced_files": replaced}


def _verify_targets(data_root: Path, journal: Mapping[str, Any]) -> None:
    """Verify every published artifact against the journal before cleanup."""
    root = Path(data_root)
    for instrument_id in sorted(journal["staged"]):
        record = journal["staged"][instrument_id]
        destination = pack_manifest.resolve_pack_path(
            root, record["final_path"], f"staged[{instrument_id}].final_path"
        )
        actual = digest_or_none(destination)
        if actual != record["new_sha256"]:
            raise PatternLabDataError(
                f"staged[{instrument_id}]: published {destination} has SHA-256 {actual}, not the "
                f"target {record['new_sha256']}.",
                error_code="unexpected_target_state",
            )
    for key in TEXT_TARGETS:
        target = journal["targets"][key]
        path = root / target["final_name"]
        actual = digest_or_none(path)
        if actual != target["new_sha256"]:
            raise PatternLabDataError(
                f"targets.{key}: published {path} has SHA-256 {actual}, not the target "
                f"{target['new_sha256']}.",
                error_code="unexpected_target_state",
            )


def staged_artifact_paths(data_root: Path, journal: Mapping[str, Any]) -> list[Path]:
    """Return every staged artifact this operation recorded, in a stable order."""
    operation_id = journal["operation_id"]
    relatives: list[str] = []
    for instrument_id in sorted(journal["staged"]):
        staged = journal["staged"][instrument_id]["staged_path"]
        if staged is not None:
            relatives.append(staged)
    if journal["targets"] is not None:
        relatives.extend(journal["targets"][key]["staged_path"] for key in TEXT_TARGETS)
    return [
        resolve_staged(data_root, relative, operation_id, f"staged artifact {relative}")
        for relative in relatives
    ]


def cleanup_operation(data_root: Path, journal: Mapping[str, Any]) -> list[str]:
    """Remove only this operation's recorded artifacts and its staging directory.

    Cleanup is idempotent: an already-consumed artifact is simply absent.  An
    unexpected live artifact inside the staging directory is reported, never
    removed, and the directory is then retained for inspection.
    """
    root = Path(data_root)
    removed: list[str] = []
    for path in staged_artifact_paths(root, journal):
        if path.is_file():
            path.unlink()
            removed.append(path.name)
    directory = staging_dir(root, journal["operation_id"])
    if directory.is_dir():
        # Parquet temporaries of an interrupted write live here and belong to this
        # operation; anything else is reported rather than deleted.
        for child in sorted(directory.iterdir()):
            if child.is_file() and child.name.startswith("."):
                child.unlink()
                removed.append(child.name)
        remaining = sorted(item.name for item in directory.iterdir())
        if remaining:
            raise PatternLabDataError(
                f"{directory}: unexpected artifacts {remaining} remain in this operation's staging "
                "directory. They were not created by the recorded plan and are reported rather than "
                "deleted; inspect and remove them manually.",
                error_code="unexpected_staging_artifact",
            )
        directory.rmdir()
        pack_manifest.fsync_directory(root)
    return removed


def remove_marker(data_root: Path) -> None:
    """Remove the operation marker last, after every target is published."""
    root = Path(data_root)
    marker_path(root).unlink(missing_ok=True)
    marker_temp_path(root).unlink(missing_ok=True)
    pack_manifest.fsync_directory(root)


def abort_operation(data_root: Path, journal: Mapping[str, Any]) -> dict[str, Any]:
    """Abort a staging operation, leaving the base pack byte-for-byte unchanged."""
    root = Path(data_root)
    if journal["phase"] != "staging":
        raise PatternLabDataError(
            f"operation {journal['operation_id']} has already entered the applying phase and cannot be "
            "aborted; finish it forward with 'recover'.",
            error_code="abort_after_applying",
        )
    verify_base_intact(root, journal)
    removed = cleanup_operation(root, journal)
    remove_marker(root)
    return {"removed_artifacts": removed}


def verify_base_intact(data_root: Path, journal: Mapping[str, Any]) -> None:
    """Confirm the live files and metadata still match the recorded base."""
    root = Path(data_root)
    base = journal["base"]
    for name, digest in (
        (MANIFEST_NAME, base["manifest_sha256"]),
        (README_NAME, base["readme_sha256"]),
        (UPDATES_NAME, base["updates_sha256"]),
    ):
        actual = digest_or_none(root / name)
        if actual != digest:
            raise PatternLabDataError(
                f"{root / name}: has SHA-256 {actual} but the operation recorded {digest} as its base; "
                "the live pack changed outside this operation.",
                error_code="unexpected_target_state",
            )
    for instrument_id, digest in sorted(base["files"].items()):
        path = root / pack_manifest.instrument_relative_file(instrument_id)
        actual = digest_or_none(path)
        if actual != digest:
            raise PatternLabDataError(
                f"{path}: has SHA-256 {actual} but the operation recorded {digest} as its base; the "
                "live pack changed outside this operation.",
                error_code="unexpected_target_state",
            )


def remove_empty_created_directories(data_root: Path, names: Iterable[str]) -> list[str]:
    """Remove task-created subdirectories of an aborted initial collect if still empty.

    The data root itself and its persistent ``.pack-lock`` are always retained, so
    a completed initial abort leaves a reusable lock-only root.
    """
    root = Path(data_root)
    removed: list[str] = []
    for name in names:
        path = root / name
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
            removed.append(name)
    if removed:
        pack_manifest.fsync_directory(root)
    return removed


def discard_unrecorded_staging(data_root: Path, operation_id: str) -> None:
    """Delete an operation's own staging directory when no journal was published.

    Used only on the failure path of the very first journal write, where the
    directory was created by this process and can contain nothing else.
    """
    directory = staging_dir(data_root, operation_id)
    if directory.is_dir() and not any(directory.iterdir()):
        directory.rmdir()


def unexpected_root_artifacts(data_root: Path, allowed: Sequence[str]) -> list[str]:
    """Return the entries of a destination that block a new initial collect."""
    root = Path(data_root)
    permitted = set(allowed)
    return sorted(item.name for item in root.iterdir() if item.name not in permitted)
