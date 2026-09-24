"""The study coordinator.

The coordinator owns one pinned read session, admits instruments, prepares every
requested timeframe in memory from one consumed 5m payload, and dispatches the
top-level RAM-only job.  ``workers=1`` calls that job directly in this process;
``workers>1`` runs the same job under an explicit spawn pool.  Children never
open the pack, never publish and never receive a lock handle or a market-file
path: instrument selection, reads, evidence semantics and every output write
stay here.

Every accepted public request form — a request file, a request mapping and an
already normalized :class:`~tools.pattern_lab.study.spec.StudyRequest` — passes
the same execution-boundary validation, and every used non-built-in descriptor
must come from a declared, verified source generation, before any output
directory exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import time
from typing import Any, Mapping, Sequence

import numpy as np

from .. import PatternLabDataError, PatternLabPendingError, PatternLabStudyError
from .. import data as pack_data
from .. import manifest as pack_manifest
from .. import update_transaction
from ..manifest import format_epoch_ms, to_epoch_ms
from . import contracts, evidence, extensions as study_extensions, report as study_report
from . import results as study_results
from . import spec as study_spec
from . import validation as study_validation
from . import workers as study_workers
from .job import InstrumentJobInput, TimeframeInput, run_instrument_job
from .spec import StudyRequest

# The default worker count. Any positive integer is accepted; effective
# parallel capacity is additionally bounded by the selected instrument count.
DEFAULT_WORKERS = 1
INTEGRITY_SCOPE = "full_pack"

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_workers(value: Any) -> int:
    """Accept any positive integer; never silently fall back or cap.

    A boolean, a float, a string and a nonpositive number are rejected before
    any output exists.  The requested count is recorded as provenance; the
    effective parallel capacity is ``min(requested, selected instruments)`` and
    is never silently reduced to a detected CPU count.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise PatternLabDataError(
            f"workers: expected a positive integer, got {type(value).__name__}. "
            f"The default is {DEFAULT_WORKERS}; nothing falls back silently."
        )
    if value < 1:
        raise PatternLabDataError(
            f"workers: expected a positive integer, got {value}."
        )
    return value


# --------------------------------------------------------------------------
# selection and metadata admission
# --------------------------------------------------------------------------

def _selectable(entry: Mapping[str, Any]) -> bool:
    return bool(set(entry["roles"]) & set(study_spec.SELECTABLE_ROLES))


def resolve_selection(report: Mapping[str, Any], request: StudyRequest) -> list[dict[str, Any]]:
    """Resolve the exact ordered instruments, with no substitution or inference."""
    by_id = {entry["instrument_id"]: entry for entry in report["instruments"]}
    failures: list[str] = []
    if request.instrument_selection is not None:
        selected: list[dict[str, Any]] = []
        for identifier in request.instrument_selection:
            entry = by_id.get(identifier)
            if entry is None:
                failures.append(
                    f"{identifier}: not declared in the pack manifest; no instrument is substituted "
                    "or omitted."
                )
                continue
            if not _selectable(entry):
                failures.append(
                    f"{identifier}: roles {entry['roles']} cannot be a standalone study target; a "
                    "factor-only series is context, not a target."
                )
                continue
            selected.append(entry)
    else:
        wanted = set(request.role_selection or ())
        selected = [
            entry
            for identifier, entry in sorted(by_id.items())
            if set(entry["roles"]) & wanted
        ]
    if failures:
        raise PatternLabDataError(
            "study selection failed:\n  - " + "\n  - ".join(failures), error_code="admission_failed"
        )
    if not selected:
        raise PatternLabDataError(
            "study selection resolved no instrument; an empty study selection fails rather than "
            "producing an empty run.",
            error_code="admission_failed",
        )
    return sorted(selected, key=lambda entry: entry["instrument_id"])


def metadata_admission_failures(
    entries: Sequence[Mapping[str, Any]], request: StudyRequest
) -> list[str]:
    """Collect every metadata admission failure across the whole selection."""
    failures: list[str] = []
    for entry in entries:
        identifier = entry["instrument_id"]
        if entry["research_blockers"]:
            for blocker in entry["research_blockers"]:
                failures.append(f"{identifier}: {blocker}")
        first_ms = to_epoch_ms(entry["first_open_utc"], f"{identifier}.first_open_utc")
        coverage_end_ms = to_epoch_ms(entry["coverage_end_utc"], f"{identifier}.coverage_end_utc")
        if first_ms > request.warmup_start_ms:
            failures.append(
                f"{identifier}: declared coverage starts at {entry['first_open_utc']}, after the "
                f"requested consumed start {format_epoch_ms(request.warmup_start_ms)}."
            )
        if coverage_end_ms < request.study_end_ms:
            failures.append(
                f"{identifier}: declared coverage ends at {entry['coverage_end_utc']}, before the "
                f"requested study end {format_epoch_ms(request.study_end_ms)}."
            )
        verification = entry["verification"]
        if verification["volume_quote_verified"] is not True:
            failures.append(f"{identifier}: quote-volume units are not verified.")
        closure = verification["closed_before_utc"]
        if closure is None:
            failures.append(f"{identifier}: final-candle closure is unknown.")
        elif to_epoch_ms(closure, f"{identifier}.closed_before_utc") < request.study_end_ms:
            failures.append(
                f"{identifier}: closure cutoff {closure} is earlier than the requested study end "
                f"{format_epoch_ms(request.study_end_ms)}."
            )
        for timeframe in request.timeframes:
            step_ms = timeframe * 60_000
            for label, value in (
                ("warmup_start", request.warmup_start_ms),
                ("start", request.study_start_ms),
                ("end", request.study_end_ms),
            ):
                if value % step_ms:
                    failures.append(
                        f"{identifier}: the requested {label} {format_epoch_ms(value)} is not aligned "
                        f"to the selected {timeframe}m timeframe."
                    )
    return failures


# --------------------------------------------------------------------------
# the planned family
# --------------------------------------------------------------------------

def planned_family(request: StudyRequest, entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Freeze the exact selection, variants and model-owned resolved cases."""
    groups: list[dict[str, Any]] = []
    for variant in request.variants:
        for instance in request.models:
            for timeframe in request.timeframes:
                for case in instance.cases[timeframe]:
                    groups.append(
                        {
                            "variant_id": variant.variant_id,
                            "model_instance_id": instance.model_instance_id,
                            "timeframe_minutes": timeframe,
                            "case_id": case.case_id,
                            "primary": case.primary,
                            "outcomes": [item.name for item in case.outcomes],
                        }
                    )
    instruments = [
        {
            "instrument_id": entry["instrument_id"],
            "symbol": entry["symbol"],
            "venue": entry["venue"],
            "contract": entry["contract"],
            "quote_currency": entry["quote_currency"],
            "roles": list(entry["roles"]),
        }
        for entry in entries
    ]
    return {
        "schema_version": request.schema_version,
        **({"context": dict(request.context), "execution": dict(request.execution)}
           if request.schema_version == 2 else {}),
        "instruments": instruments,
        "timeframes_minutes": list(request.timeframes),
        "variants": [variant.as_json() for variant in request.variants],
        "models": [instance.as_json() for instance in request.models],
        "groups": groups,
        "planned_job_count": len(instruments),
        "planned_group_count": len(groups),
        "planned_family_size": len(groups) * len(instruments),
        "warmup_requirements": study_spec.warmup_requirements(request),
        "anchor_convention": (
            "An eligible study anchor is a complete bar whose open is >= the study start and whose "
            "close is strictly < the study end; its signal timestamp is that close."
        ),
    }


# --------------------------------------------------------------------------
# run state
# --------------------------------------------------------------------------

@dataclass
class _RunState:
    """The coordinator's own view of planned, attempted and finished jobs."""

    order: list[str]
    states: dict[str, str]
    errors: dict[str, str] = field(default_factory=dict)
    fingerprints: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # The instrument currently being read, prepared, computed or published.
    current: str | None = None
    phase: str = "execute"
    run_version: int = 1
    context_outputs: Mapping[str, Any] = field(default_factory=dict)

    def counts(self) -> dict[str, int]:
        tally = {
            evidence.STATE_ADMITTED: 0,
            evidence.STATE_COMPLETED: 0,
            evidence.STATE_FAILED: 0,
            evidence.STATE_NOT_STARTED: 0,
        }
        for state in self.states.values():
            tally[state] = tally.get(state, 0) + 1
        tally["planned"] = len(self.order)
        return tally

    def ordered_fingerprints(self) -> list[dict[str, Any]]:
        """Fingerprints in frozen planned order, never in completion order."""
        composed: list[dict[str, Any]] = []
        for identifier in self.order:
            composed.extend(self.fingerprints.get(identifier, []))
        return composed

    def document(self, *, terminal: str, started: str, finished: str,
                 failure: Mapping[str, Any] | None) -> dict[str, Any]:
        return {
            "schema_version": self.run_version,
            "terminal_status": terminal,
            "counts": self.counts(),
            "instruments": [
                {
                    "instrument_id": identifier,
                    "state": self.states[identifier],
                    "error": self.errors.get(identifier),
                }
                for identifier in self.order
            ],
            "failure": dict(failure) if failure else None,
            "started_utc": started,
            "finished_utc": finished,
        }


def _record_state(run_root: Path, state: _RunState, identifier: str, name: str,
                  extra: Mapping[str, Any] | None = None) -> None:
    """Update one job's committed record; a record failure never hides a cause."""
    state.states[identifier] = name
    try:
        evidence.record_job_state(run_root, identifier, name, extra=extra)
    except Exception:  # pragma: no cover - the original diagnostic always wins
        pass


# --------------------------------------------------------------------------
# admission and preparation
# --------------------------------------------------------------------------

def _declared_identity(entry: Mapping[str, Any], request: StudyRequest,
                       family: Mapping[str, Any]) -> dict[str, Any]:
    """The input context known from metadata alone, before any row is read."""
    return {
        "instrument_id": entry["instrument_id"],
        "roles": list(entry["roles"]),
        "symbol": entry["symbol"],
        "declared": {
            "file_sha256": entry["sha256"],
            "row_count": entry["row_count"],
            "first_open_utc": entry["first_open_utc"],
            "coverage_end_utc": entry["coverage_end_utc"],
            "missing_bar_count": entry["missing_bar_count"],
        },
        "requested": {
            "warmup_start_utc": format_epoch_ms(request.warmup_start_ms),
            "start_utc": format_epoch_ms(request.study_start_ms),
            "end_utc": format_epoch_ms(request.study_end_ms),
        },
        "warmup_requirements": family["warmup_requirements"],
        "timeframes": [],
        **({"bracket_rules":entry["bracket_rules"].semantic(),
            "rule_snapshot":entry["instrument_rules"]} if "bracket_rules" in entry else {}),
    }


def _prepare_timeframes(
    entry: Mapping[str, Any], base: pack_data.DataSlice, request: StudyRequest
) -> tuple[list[TimeframeInput], list[dict[str, Any]]]:
    """Resample and fingerprint every requested timeframe from one raw payload."""
    stamps = base.bars.index.view("int64") // 1_000_000
    values = base.bars.to_numpy(dtype=np.float64)
    prepared: list[TimeframeInput] = []
    fingerprints: list[dict[str, Any]] = []
    for timeframe in request.timeframes:
        series = pack_data.prepare_series(
            instrument_id=entry["instrument_id"],
            venue=entry["venue"],
            contract=entry["contract"],
            quote_currency=entry["quote_currency"],
            timestamps=stamps,
            ohlcv=values,
            start_ms=request.study_start_ms,
            end_ms=request.study_end_ms,
            warmup_start_ms=request.warmup_start_ms,
            timeframe_minutes=timeframe,
        )
        prepared.append(
            TimeframeInput(
                timeframe_minutes=timeframe,
                timestamps_ms=series.timestamps,
                values=series.values,
                research_start_index=series.research_start_index,
                input_fingerprint=series.input_fingerprint,
                base_row_count=series.base_row_count,
                base_gap_count=series.base_gap_count,
                omitted_group_count=series.omitted_group_count,
                segment_count=int(np.count_nonzero(series.segment_start)),
            )
        )
        fingerprints.append(
            {
                "instrument_id": entry["instrument_id"],
                "timeframe_minutes": timeframe,
                "input_fingerprint": series.input_fingerprint,
                "bar_count": int(series.timestamps.size),
                "warmup_bar_count": series.research_start_index,
                "base_row_count": series.base_row_count,
                "base_gap_count": series.base_gap_count,
                "omitted_group_count": series.omitted_group_count,
                "segment_count": int(np.count_nonzero(series.segment_start)),
            }
        )
    return prepared, fingerprints


def _diagnostic(exc: BaseException) -> str:
    """One readable diagnostic that never loses the exception type."""
    message = str(exc)
    if isinstance(exc, PatternLabDataError):
        return message
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def _fail_admission(
    run_root: Path,
    state: _RunState,
    identifier: str,
    identity: Mapping[str, Any],
    exc: BaseException,
    *,
    phase: str,
) -> None:
    """Record the attempted instrument as failed with its phase and diagnostic.

    The coordinator's own view is updated first, so a secondary admission or
    status write failure can never leave a run naming an instrument it also
    reports as untouched.  Neither write can replace the original cause or stop
    the outer failure handling and pool cleanup.
    """
    message = _diagnostic(exc)
    state.phase = phase
    state.states[identifier] = evidence.STATE_FAILED
    state.errors[identifier] = message
    try:
        evidence.record_admission(run_root, identifier, identity)
    except Exception:  # pragma: no cover - the original diagnostic always wins
        pass
    _record_state(
        run_root, state, identifier, evidence.STATE_FAILED, {"error": message, "phase": phase}
    )


def _admit(
    run_root: Path,
    session,
    entry: Mapping[str, Any],
    request: StudyRequest,
    family: Mapping[str, Any],
    state: _RunState,
) -> list[TimeframeInput]:
    """Read one instrument's consumed slice, prepare its timeframes and admit it.

    The instrument is the coordinator's current job from the first base read, so
    *every* exceptional read, preparation or admission-write exit after preflight
    — ordinary exceptions and control flow such as ``KeyboardInterrupt`` alike —
    names that instrument with its phase and the actual diagnostic instead of
    leaving it reported as not started.  Only an already actionable
    :class:`PatternLabDataError` is rewrapped; anything else keeps its own type
    so the interrupt semantics of the caller are preserved.  When the read
    failed, only the metadata context is recorded: no input fingerprint is
    invented for rows that were never observed.
    """
    identifier = entry["instrument_id"]
    state.current = identifier
    identity = _declared_identity(entry, request, family)
    try:
        state.phase = "read"
        base = session.load_slice(
            identifier,
            start=format_epoch_ms(request.study_start_ms),
            end=format_epoch_ms(request.study_end_ms),
            warmup_start=format_epoch_ms(request.warmup_start_ms),
            timeframe_minutes=pack_manifest.BASE_TIMEFRAME_MINUTES,
        )
    except BaseException as exc:
        identity["read_error"] = _diagnostic(exc)
        _fail_admission(run_root, state, identifier, identity, exc, phase="read")
        if isinstance(exc, PatternLabDataError):
            raise PatternLabDataError(
                f"{identifier}: the consumed base slice could not be read after preflight: {exc}",
                error_code="job_failed",
            ) from exc
        raise

    try:
        state.phase = "prepare"
        identity["consumed"] = {
            "warmup_start_utc": format_epoch_ms(request.warmup_start_ms),
            "start_utc": format_epoch_ms(request.study_start_ms),
            "end_utc": format_epoch_ms(request.study_end_ms),
            "base_row_count": base.base_row_count,
            "base_gap_count": base.base_gap_count,
        }
        prepared, fingerprints = _prepare_timeframes(entry, base, request)
        state.phase = "admit"
        identity["timeframes"] = fingerprints
        evidence.record_admission(run_root, identifier, identity)
        state.states[identifier] = evidence.STATE_ADMITTED
        state.fingerprints[identifier] = fingerprints
    except BaseException as exc:
        # Keep all observed context, including prepared fingerprints when the
        # admission write failed. Unprepared timeframes stay empty.
        _fail_admission(run_root, state, identifier, identity, exc, phase=state.phase)
        if isinstance(exc, PatternLabDataError):
            raise PatternLabDataError(
                f"{identifier}: data admission failed after preflight: {exc}",
                error_code="job_failed",
            ) from exc
        raise
    finally:
        del base

    state.phase = "execute"
    from .context import align
    return align(prepared, request, state.context_outputs) if request.schema_version == 2 else prepared


def _job_payload(
    entry: Mapping[str, Any], request: StudyRequest, prepared: Sequence[TimeframeInput]
) -> InstrumentJobInput:
    return InstrumentJobInput(
        instrument_id=entry["instrument_id"],
        symbol=entry["symbol"],
        venue=entry["venue"],
        contract=entry["contract"],
        quote_currency=entry["quote_currency"],
        roles=tuple(entry["roles"]),
        study_start_ms=request.study_start_ms,
        study_end_ms=request.study_end_ms,
        warmup_start_ms=request.warmup_start_ms,
        timeframes=tuple(prepared),
        variants=tuple(variant.as_json() for variant in request.variants),
        models=tuple(instance.as_json() for instance in request.models),
        execution_rules=entry.get("bracket_rules"),
    )


@dataclass(frozen=True)
class _SourceChecks:
    """The declared-source checks applied around every dispatch and result."""

    loaded: Sequence[study_extensions.LoadedExtension]
    used: Sequence[Any]
    declared: Mapping[str, str]

    def verify(self, where: str) -> None:
        study_extensions.verify_extensions(self.loaded, where=where)
        study_validation.require_declared_sources(self.used, self.declared, where=where)


def _execute_direct(
    run_root: Path, session, entries: Sequence[Mapping[str, Any]], request: StudyRequest,
    family: Mapping[str, Any], state: _RunState, checks: _SourceChecks,
) -> None:
    """Run every instrument job in this process, one at a time."""
    for entry in entries:
        identifier = entry["instrument_id"]
        prepared = _admit(run_root, session, entry, request, family, state)
        checks.verify(f"job {identifier} start")
        payload = _job_payload(entry, request, prepared)
        del prepared
        try:
            result = run_instrument_job(payload)
        finally:
            del payload
        checks.verify(f"job {identifier} result")
        _publish_result(run_root, state, identifier, result)
        del result


def _execute_pooled(
    run_root: Path, session, entries: Sequence[Mapping[str, Any]], request: StudyRequest,
    family: Mapping[str, Any], state: _RunState, checks: _SourceChecks, *,
    effective_workers: int, provenance: dict[str, Any],
) -> None:
    """Run the same job under an explicit spawn pool, with a bounded backlog.

    At most ``effective_workers`` submitted, running or returned-but-unpublished
    jobs are retained at once, plus at most one instrument being prepared here.
    A free slot is filled only after every ready result has been drained and
    published and every failure has been checked.  IPC and pickle copies add a
    bounded constant factor to that job count; this is a job-count bound, not a
    universal byte or RAM guarantee.
    """
    settings = study_workers.WorkerSettings(
        extensions=tuple(request.extensions),
        required=tuple((item.kind, item.identifier) for item in checks.used),
        # The coordinator's frozen records for every declared module and helper
        # file travel with the declarations, so a child checks the generation
        # this run froze rather than whatever it observes for itself.
        frozen=tuple(checks.loaded),
    )
    # The startup override is scoped to the whole pool lifetime, including any
    # lazy initial spawn, and is restored in the finally clause after teardown.
    with study_workers.single_threaded_children() as caller_environment:
        pool = study_workers.SpawnJobPool(effective_workers, settings)
        aborting = False
        try:
            pool.start()
            provenance["execution"]["worker_pids"] = pool.worker_pids
            provenance["execution"]["thread_settings"] = {
                "caller_environment": caller_environment,
                "requested": {name: "1" for name in study_workers.THREAD_ENVIRONMENT},
                "observed_in_children": pool.thread_evidence,
            }
            inflight: set[str] = set()
            index = 0
            while index < len(entries) or inflight:
                drained = False
                while True:
                    message = pool.poll()
                    if message is None:
                        break
                    _accept(run_root, state, checks, message, inflight)
                    drained = True
                if drained:
                    continue
                if index < len(entries) and len(inflight) < effective_workers:
                    entry = entries[index]
                    index += 1
                    identifier = entry["instrument_id"]
                    # The single preparation slot: one payload is live here.
                    prepared = _admit(run_root, session, entry, request, family, state)
                    checks.verify(f"job {identifier} start")
                    payload = _job_payload(entry, request, prepared)
                    del prepared
                    pool.submit(identifier, payload)
                    # Transport owns the buffers now; the coordinator's extra
                    # reference is dropped. Retained IPC buffers still belong to
                    # this job's slot.
                    del payload
                    inflight.add(identifier)
                    continue
                if inflight:
                    _accept(run_root, state, checks, pool.take(), inflight)
        except study_workers.WorkerLostError as exc:
            aborting = True
            # Attribute the loss to the one job that worker had started, when
            # exactly one is known; never invent an instrument otherwise.
            state.current = exc.orphaned[0] if len(exc.orphaned) == 1 else None
            raise
        except study_workers.WorkerTransportError:
            aborting = True
            # A dead result transport belongs to the run, not to whichever
            # instrument the coordinator last touched.
            state.current = None
            raise
        except BaseException:
            aborting = True
            raise
        finally:
            pool.shutdown(abort=aborting)


def _accept(
    run_root: Path, state: _RunState, checks: _SourceChecks, message, inflight: set[str]
) -> None:
    """Publish one finished job, or raise its failure with the instrument named."""
    kind, identifier, payload = message
    inflight.discard(identifier)
    state.current = identifier
    if kind == "error":
        raise study_workers.WorkerJobError(
            f"{identifier}: the instrument job failed in a worker with "
            f"{payload['type']}: {payload['message']}",
            error_code=payload.get("error_code") or "job_failed",
            worker_traceback=payload.get("traceback", ""),
        )
    checks.verify(f"job {identifier} result")
    _publish_result(run_root, state, identifier, payload)


def _publish_result(run_root: Path, state: _RunState, identifier: str, result) -> None:
    """Publish one finished job's bundle and commit its completed record."""
    state.phase = "publish"
    bundle = evidence.publish_job(
        run_root, identifier, tables=result.tables, stats=result.stats
    )
    state.states[identifier] = evidence.STATE_COMPLETED
    evidence.record_job_state(
        run_root,
        identifier,
        evidence.STATE_COMPLETED,
        extra={"bundle_sha256": bundle["bundle_sha256"]},
    )
    state.phase = "execute"


def _write_snapshots(run_root: Path, loaded: Sequence[study_extensions.LoadedExtension]) -> list[str]:
    """Copy declared custom source into the run as inert provenance."""
    names: list[str] = []
    for record in loaded:
        root = Path(record.source_root)
        for name, _digest in record.files:
            target = Path(run_root) / evidence.SNAPSHOT_DIR / f"{record.module}__{name.replace('/', '__')}"
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / name, target)
            names.append(target.name)
    return sorted(names)


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

def run_study(
    *,
    request: Any,
    data_root: Any,
    output_root: Any,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Run one complete study and return its structured result.

    ``request`` is a study-request file path, an already normalized
    :class:`~tools.pattern_lab.study.spec.StudyRequest`, or a request mapping.
    Every form is revalidated here: a normalized object's semantic settings are
    resolved again and its derived facts must agree.  ``data_root`` and
    ``output_root`` are execution arguments: they are recorded as provenance and
    never enter semantic or data identity.
    """
    started = _now()
    clock = time.monotonic()
    requested_workers = normalize_workers(workers)

    normalized = study_validation.validated_request(request)
    candidate = None
    if normalized.execution["kind"] == "validation":
        from ..candidate import validate_execution, verify_current_generation
        candidate = validate_execution(normalized.semantic_document())

    loaded = study_extensions.load_extensions(normalized.extensions)
    study_extensions.verify_extensions(loaded, where="study preflight")
    if candidate is not None:
        verify_current_generation(candidate, actual_extensions=[record.as_json() for record in loaded])
    used = study_validation.require_used_sources(normalized, loaded, where="study preflight")
    declared_digests = study_validation.declared_digests(loaded)
    source_identity = {
        "core_source": study_extensions.core_source_digests(),
        "extensions": [record.as_json() for record in loaded],
        "library_versions": study_extensions.library_versions(),
        "evidence_view_version": study_results.EVIDENCE_VIEW_VERSION,
    }
    has_sequential = any(contracts.is_sequential(m.as_json()) for m in normalized.models)
    if has_sequential:
        source_identity["bracket_source"] = study_extensions.bracket_source_digests()

    pack_root = Path(data_root).expanduser()
    with pack_data.read_session(pack_root) as session:
        report = session.inspect(verify=False)
        # A valid pending collector operation is an actionable recover/abort
        # state, not a corrupt pack: it keeps its own exit status.
        pending = update_transaction.pending_state(Path(pack_root))
        if pending is not None and pending["valid"]:
            raise PatternLabPendingError(
                f"{pack_root}: a study cannot read this pack while an operation is pending. "
                + update_transaction.pending_problem(pending)
            )
        if report.get("state") != "ready":
            raise PatternLabDataError(
                f"{pack_root}: manifest state is {report.get('state')!r}; only a ready pack can be "
                "studied.",
                error_code="admission_failed",
            )
        from . import context as study_context
        entries = resolve_selection(report, normalized)
        if has_sequential:
            from .bracket_rules import normalize_rules
            rule_entries = {entry["instrument_id"]:entry for entry in pack_manifest.read_manifest(pack_root)["instruments"]}
            entries = [{**entry, "instrument_rules":rule_entries[entry["instrument_id"]].get("instrument_rules")}
                       for entry in entries]
            entries = [{**entry, "bracket_rules": normalize_rules(entry)} for entry in entries]
        context_entries = study_context.resolve_entries(report, normalized)
        if candidate is not None:
            from ..candidate import verify_admitted_contracts
            verify_admitted_contracts(candidate, normalized, entries, context_entries)
        union = {entry["instrument_id"]: entry for entry in entries + context_entries}
        failures = metadata_admission_failures(list(union.values()), normalized)
        if failures:
            raise PatternLabDataError(
                "metadata admission failed for the requested study:\n  - " + "\n  - ".join(failures),
                error_code="admission_failed",
            )
        integrity = session.inspect(verify=True)["verification_check"]
        if not integrity["ok"]:
            raise PatternLabDataError(
                f"{pack_root}: full-pack integrity verification failed; a corrupt file blocks this "
                "run even when its instrument is not selected:\n  - "
                + "\n  - ".join(integrity["problems"]),
                error_code="admission_failed",
            )

        family = planned_family(normalized, entries)
        request_document = normalized.request_document()
        semantic = normalized.semantic_document()
        identities = {
            "specification_sha256": evidence.specification_identity(semantic, family, run_version=normalized.schema_version),
            "implementation_sha256": evidence.implementation_identity(source_identity, run_version=normalized.schema_version),
            "data_input_sha256": None,
        }

        effective_workers = min(requested_workers, len(entries))
        run_root = evidence.create_run_directory(output_root, data_root=pack_root)
        state = _RunState(
            run_version=normalized.schema_version,
            order=[entry["instrument_id"] for entry in entries],
            states={entry["instrument_id"]: evidence.STATE_NOT_STARTED for entry in entries},
        )
        manifest_file = pack_manifest.manifest_path(Path(pack_root))
        provenance = {
            "schema_version": normalized.schema_version,
            "data_root": str(Path(pack_root).resolve()),
            "output_root": str(run_root),
            "workers": requested_workers,
            "execution": {
                "requested_workers": requested_workers,
                "effective_workers": effective_workers,
                "mode": "direct" if requested_workers == 1 else study_workers.START_METHOD,
                "start_method": None if requested_workers == 1 else study_workers.START_METHOD,
                "coordinator_pid": os.getpid(),
                "worker_pids": [],
                "thread_settings": None,
            },
            "integrity_scope": INTEGRITY_SCOPE,
            "manifest": {
                "revision": report["revision"],
                "state": report["state"],
                "generated_utc": report["generated_utc"],
                "manifest_sha256": pack_manifest.file_sha256(manifest_file),
                "collector_managed": report["collector_managed"],
                "universe": dict(report["universe"]),
            },
            "environment": study_extensions.environment_provenance(REPOSITORY_ROOT),
            "identities": identities,
            "timings": {"started_utc": started, "finished_utc": None, "elapsed_seconds": None},
        }

        checks = _SourceChecks(loaded=loaded, used=used, declared=declared_digests)
        context_identity = {}
        try:
            state.phase = "freeze"
            evidence.write_json(run_root / evidence.REQUEST_FILE, request_document)
            evidence.write_json(
                run_root / evidence.PROTOCOL_FILE, study_spec.protocol_document(normalized.protocol)
            )
            evidence.write_json(run_root / evidence.FAMILY_FILE, family)
            source_identity["snapshots"] = _write_snapshots(run_root, loaded)
            evidence.write_json(run_root / evidence.SOURCE_FILE, source_identity)
            evidence.write_json(run_root / evidence.PROVENANCE_FILE, provenance)
            evidence.write_status(
                run_root,
                state.document(terminal="running", started=started, finished="", failure=None),
            )

            if normalized.schema_version == 2:
                admitted_context = study_context.admission(
                    normalized, context_entries, state.order
                )
                evidence.write_json(run_root / evidence.CONTEXT_ADMISSION_FILE, admitted_context)
                state.phase = "context"
                checks.verify("context preparation start")
                state.context_outputs, context_facts = study_context.prepare(session, context_entries, normalized)
                checks.verify("context preparation result")
                evidence.write_json(run_root / evidence.CONTEXT_FILE, context_facts)
                context_identity = {"admission": admitted_context, "outputs": context_facts}
            if requested_workers == 1:
                _execute_direct(run_root, session, entries, normalized, family, state, checks)
            else:
                _execute_pooled(
                    run_root, session, entries, normalized, family, state, checks,
                    effective_workers=effective_workers, provenance=provenance,
                )
        except KeyboardInterrupt:
            failure = _execution_failure(
                run_root, state,
                KeyboardInterrupt("the operation was interrupted by the user"),
                phase=state.phase,
            )
            failure["reason"] = "keyboard_interrupt"
            _finalize_failure(
                run_root, state, provenance, started, clock, evidence.TERMINAL_INTERRUPTED, failure
            )
            raise
        except Exception as exc:
            failure = _execution_failure(run_root, state, exc, phase=state.phase)
            _finalize_failure(
                run_root, state, provenance, started, clock, evidence.TERMINAL_FAILED, failure
            )
            raise PatternLabStudyError(
                f"study failed during {failure['phase']}"
                + (f" on {failure['instrument_id']}" if failure["instrument_id"] else "")
                + f": {exc}",
                error_code=getattr(exc, "error_code", "study_failed"),
                context=failure,
            ) from exc
        except BaseException:
            _finalize_failure(
                run_root,
                state,
                provenance,
                started,
                clock,
                evidence.TERMINAL_FAILED,
                {"reason": "interrupted", "operation": "study", "phase": state.phase,
                 "instrument_id": state.current},
            )
            raise

    state.current = None
    finished = _now()
    provenance["timings"] = {
        "started_utc": started,
        "finished_utc": finished,
        "elapsed_seconds": round(time.monotonic() - clock, 3),
    }
    identities["data_input_sha256"] = evidence.data_input_identity(
        fingerprints=state.ordered_fingerprints(),
        semantic_specification=semantic,
        universe=family["instruments"],
        protocol=study_spec.protocol_document(normalized.protocol),
        run_version=normalized.schema_version,
        context=context_identity,
        bracket_rules={entry["instrument_id"]:entry["bracket_rules"].semantic() for entry in entries} if has_sequential else None,
    )
    provenance["identities"] = identities
    evidence.write_json(run_root / evidence.PROVENANCE_FILE, provenance)
    evidence.write_status(
        run_root,
        state.document(terminal=evidence.TERMINAL_COMPLETED, started=started, finished=finished,
                       failure=None),
    )

    try:
        record = _publish_run(
            run_root, normalized, state, identities,
            loaded=loaded, used=used, declared=declared_digests,
        )
    except BaseException as exc:
        sealed = _verified_completion(run_root)
        if sealed is None:
            _handle_publication_failure(run_root, state, provenance, started, clock, exc)
            raise AssertionError("publication failure handling must raise")  # pragma: no cover
        # The completion record was already atomically published: this run is
        # sealed, and a later error never rewrites a sealed status or removes a
        # published derived report.
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        record = sealed
    return {
        "status": "completed",
        "run_root": str(run_root),
        "counts": state.counts(),
        "identities": identities,
        "evidence_set_sha256": record["evidence_set_sha256"],
        "report": str(run_root / evidence.REPORT_FILE),
        "summary": str(run_root / evidence.SUMMARY_FILE),
    }


def _publish_run(
    run_root: Path,
    request: StudyRequest,
    state: _RunState,
    identities: Mapping[str, Any],
    *,
    loaded: Sequence[study_extensions.LoadedExtension],
    used: Sequence[Any],
    declared: Mapping[str, str],
) -> dict[str, Any]:
    """Compute declared metrics, publish the first report, then seal the run.

    The completion record is written last, after every job, immutable metadata,
    declared metric and the initial derived report has succeeded.  The initial
    summary is built through the internal prepublication reader so it describes
    the run exactly as a later regeneration does, without letting the public
    partial reader claim premature success.
    """
    results = study_results.load_prepublication_results(run_root)
    if request.metrics:
        study_extensions.verify_extensions(loaded, where="metric computation start")
        study_validation.require_declared_sources(used, declared, where="metric computation start")
        evidence.write_json(
            run_root / evidence.METRICS_FILE,
            study_results.compute_metric_values(results, request.metrics),
        )
        study_extensions.verify_extensions(loaded, where="metric computation result")
        study_validation.require_declared_sources(used, declared, where="metric computation result")
    summary = study_results.summarize_results(results)
    html = study_report.render_report(summary)
    evidence.replace_derived(run_root, summary=summary, html=html)
    return evidence.write_completion(
        run_root, run_version=request.schema_version,
        summary={
            "run_root": str(run_root),
            "identities": dict(identities),
            "counts": state.counts(),
        },
    )


def _verified_completion(run_root: Path) -> dict[str, Any] | None:
    """Return the run's completion record when one is published and verifiable."""
    try:
        return evidence.verify_completion(Path(run_root))
    except Exception:
        return None


def _handle_publication_failure(run_root, state, provenance, started, clock, exc) -> None:
    """Record a handled publication failure and remove only this run's derived files."""
    cleanup = evidence.remove_new_derived(run_root)
    interrupted = isinstance(exc, KeyboardInterrupt)
    failure = {
        "reason": "keyboard_interrupt" if interrupted else type(exc).__name__,
        "operation": "study",
        "phase": "publish",
        "message": str(exc),
        "instrument_id": None,
        "derived_cleanup": cleanup,
    }
    _finalize_failure(
        run_root,
        state,
        provenance,
        started,
        clock,
        evidence.TERMINAL_INTERRUPTED if interrupted else evidence.TERMINAL_FAILED,
        failure,
    )
    if isinstance(exc, BaseException) and not isinstance(exc, Exception):
        raise exc
    raise PatternLabStudyError(
        f"study failed during publish: {exc}",
        error_code=getattr(exc, "error_code", "study_failed"),
        context=failure,
    ) from exc


# Every other dispatched job that produced no accepted result is failed with a
# reason distinct from the original computation error.
CANCELLED_REASON = (
    "aborted: the run stopped after an earlier failure or interrupt, so this dispatched job "
    "produced no accepted result."
)


def _execution_failure(run_root, state: _RunState, exc, *, phase: str) -> dict[str, Any]:
    """Mark the job that failed and describe the failure for the run status."""
    identifier = state.current
    if identifier is not None and state.states.get(identifier) == evidence.STATE_ADMITTED:
        _record_state(run_root, state, identifier, evidence.STATE_FAILED, {"error": str(exc)})
        state.errors.setdefault(identifier, str(exc))
    for other in state.order:
        if state.states[other] == evidence.STATE_ADMITTED:
            _record_state(
                run_root, state, other, evidence.STATE_FAILED, {"error": CANCELLED_REASON}
            )
            state.errors.setdefault(other, CANCELLED_REASON)
    failure = {
        "reason": type(exc).__name__,
        "operation": "study",
        "phase": phase,
        "message": str(exc),
        "instrument_id": identifier,
    }
    child_traceback = getattr(exc, "worker_traceback", None)
    if child_traceback:
        failure["worker_traceback"] = child_traceback
    return failure


def _finalize_failure(run_root, state, provenance, started, clock, terminal, failure) -> None:
    """Record an honest terminal state; a status failure never masks the cause."""
    try:
        for identifier in state.order:
            if state.states[identifier] == evidence.STATE_ADMITTED:
                state.states[identifier] = evidence.STATE_FAILED
        finished = _now()
        provenance["timings"] = {
            "started_utc": started,
            "finished_utc": finished,
            "elapsed_seconds": round(time.monotonic() - clock, 3),
        }
        evidence.write_json(Path(run_root) / evidence.PROVENANCE_FILE, provenance)
        evidence.write_status(
            run_root,
            state.document(terminal=terminal, started=started, finished=finished, failure=failure),
        )
    except Exception:  # pragma: no cover - the original diagnostic always wins
        pass


def regenerate_report(run_root: Any) -> dict[str, Any]:
    """Rebuild the derived summary and HTML of a completed run.

    Raw evidence and the completion record are verified once, through the same
    loading path the public reader uses, and are never rewritten.  The
    regeneration reads no market pack and imports no saved custom source:
    recorded custom metric values and the built-in evidence derivation are used
    as they stand.  A failure here leaves the original successful study exactly
    as it was; it never reclassifies the run.
    """
    root = evidence.require_run_directory(run_root)
    results = study_results.load_results(root)
    summary = study_results.summarize_results(results)
    html = study_report.render_report(summary)
    evidence.replace_derived(root, summary=summary, html=html)
    return {
        "status": "regenerated",
        "run_root": str(root),
        "counts": dict(results.counts),
        "evidence_set_sha256": results.completion["evidence_set_sha256"],
        "report": str(root / evidence.REPORT_FILE),
        "summary": str(root / evidence.SUMMARY_FILE),
    }
