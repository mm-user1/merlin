"""The sequential study coordinator.

The coordinator owns one pinned read session, admits instruments, prepares every
requested timeframe in memory from one consumed 5m payload, and dispatches the
top-level RAM-only job.  This build executes ``workers=1``: the bounded spawn
pool is the separately specified M2b block and is not implemented here.

Every accepted public request form — a request file, a request mapping and an
already normalized :class:`~tools.pattern_lab.study.spec.StudyRequest` — passes
the same execution-boundary validation, and every used non-built-in descriptor
must come from a declared, verified source generation, before any output
directory exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
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
from . import evidence, extensions as study_extensions, report as study_report
from . import results as study_results
from . import spec as study_spec
from . import validation as study_validation
from .job import InstrumentJobInput, TimeframeInput, run_instrument_job
from .spec import StudyRequest

SUPPORTED_WORKERS = 1
INTEGRITY_SCOPE = "full_pack"

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_workers(value: Any) -> int:
    """Accept only the integer 1; never silently fall back to a serial run."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise PatternLabDataError(
            f"workers: expected the integer {SUPPORTED_WORKERS}, got {type(value).__name__}."
        )
    if value != SUPPORTED_WORKERS:
        raise PatternLabDataError(
            f"workers: this build executes only workers={SUPPORTED_WORKERS}, got {value}. "
            "The bounded spawn pool is separately specified M2b work; nothing falls back silently."
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
        "schema_version": evidence.RUN_SCHEMA_VERSION,
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
            "schema_version": evidence.RUN_SCHEMA_VERSION,
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
    a read or preparation failure after preflight names that instrument with its
    phase and the actual diagnostic instead of an anonymous run failure.  When
    the read failed, only the metadata context is recorded: no input fingerprint
    is invented for rows that were never observed.
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
    except PatternLabDataError as exc:
        identity["read_error"] = str(exc)
        evidence.record_admission(run_root, identifier, identity)
        _record_state(run_root, state, identifier, evidence.STATE_FAILED,
                      {"error": str(exc), "phase": "read"})
        state.errors[identifier] = str(exc)
        raise PatternLabDataError(
            f"{identifier}: the consumed base slice could not be read after preflight: {exc}",
            error_code="job_failed",
        ) from exc

    identity["consumed"] = {
        "warmup_start_utc": format_epoch_ms(request.warmup_start_ms),
        "start_utc": format_epoch_ms(request.study_start_ms),
        "end_utc": format_epoch_ms(request.study_end_ms),
        "base_row_count": base.base_row_count,
        "base_gap_count": base.base_gap_count,
    }
    try:
        state.phase = "prepare"
        prepared, fingerprints = _prepare_timeframes(entry, base, request)
    except PatternLabDataError as exc:
        evidence.record_admission(run_root, identifier, identity)
        _record_state(run_root, state, identifier, evidence.STATE_FAILED,
                      {"error": str(exc), "phase": "prepare"})
        state.errors[identifier] = str(exc)
        raise PatternLabDataError(
            f"{identifier}: data admission failed after preflight: {exc}",
            error_code="job_failed",
        ) from exc
    finally:
        del base

    identity["timeframes"] = fingerprints
    evidence.record_admission(run_root, identifier, identity)
    state.states[identifier] = evidence.STATE_ADMITTED
    state.fingerprints[identifier] = fingerprints
    state.phase = "execute"
    return prepared


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
    )


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
    workers: int = SUPPORTED_WORKERS,
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

    loaded = study_extensions.load_extensions(normalized.extensions)
    study_extensions.verify_extensions(loaded, where="study preflight")
    used = study_validation.require_used_sources(normalized, loaded, where="study preflight")
    declared_digests = study_validation.declared_digests(loaded)
    source_identity = {
        "core_source": study_extensions.core_source_digests(),
        "extensions": [record.as_json() for record in loaded],
        "library_versions": study_extensions.library_versions(),
        "evidence_view_version": study_results.EVIDENCE_VIEW_VERSION,
    }

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
        entries = resolve_selection(report, normalized)
        failures = metadata_admission_failures(entries, normalized)
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
            "specification_sha256": evidence.specification_identity(semantic, family),
            "implementation_sha256": evidence.implementation_identity(source_identity),
            "data_input_sha256": None,
        }

        effective_workers = min(requested_workers, len(entries))
        run_root = evidence.create_run_directory(output_root, data_root=pack_root)
        state = _RunState(
            order=[entry["instrument_id"] for entry in entries],
            states={entry["instrument_id"]: evidence.STATE_NOT_STARTED for entry in entries},
        )
        manifest_file = pack_manifest.manifest_path(Path(pack_root))
        provenance = {
            "schema_version": evidence.RUN_SCHEMA_VERSION,
            "data_root": str(Path(pack_root).resolve()),
            "output_root": str(run_root),
            "workers": requested_workers,
            "execution": {
                "requested_workers": requested_workers,
                "effective_workers": effective_workers,
                "mode": "direct",
                "coordinator_pid": None,
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

        try:
            for entry in entries:
                identifier = entry["instrument_id"]
                prepared = _admit(run_root, session, entry, normalized, family, state)
                study_extensions.verify_extensions(loaded, where=f"job {identifier} start")
                study_validation.require_declared_sources(
                    used, declared_digests, where=f"job {identifier} start"
                )
                payload = _job_payload(entry, normalized, prepared)
                del prepared
                try:
                    result = run_instrument_job(payload)
                finally:
                    del payload
                study_extensions.verify_extensions(loaded, where=f"job {identifier} result")
                study_validation.require_declared_sources(
                    used, declared_digests, where=f"job {identifier} result"
                )
                _publish_result(run_root, state, identifier, result)
                del result
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
        run_root,
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


def _execution_failure(run_root, state: _RunState, exc, *, phase: str) -> dict[str, Any]:
    """Mark the job that failed and describe the failure for the run status."""
    identifier = state.current
    if identifier is not None and state.states.get(identifier) == evidence.STATE_ADMITTED:
        _record_state(run_root, state, identifier, evidence.STATE_FAILED, {"error": str(exc)})
        state.errors.setdefault(identifier, str(exc))
    for other in state.order:
        if state.states[other] == evidence.STATE_ADMITTED:
            _record_state(run_root, state, other, evidence.STATE_FAILED, {"error": str(exc)})
            state.errors.setdefault(other, str(exc))
    return {
        "reason": type(exc).__name__,
        "operation": "study",
        "phase": phase,
        "message": str(exc),
        "instrument_id": identifier,
    }


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
