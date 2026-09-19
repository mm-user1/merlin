"""The sequential study coordinator.

The coordinator owns one pinned read session, admits instruments, prepares every
requested timeframe in memory from one consumed 5m payload, and dispatches the
top-level RAM-only job.  T03 accepts only ``workers=1``; the bounded spawn pool
is separately specified T04 work and is not implemented here.
"""

from __future__ import annotations

from dataclasses import dataclass
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
            f"workers: this build (M2a) executes only workers={SUPPORTED_WORKERS}, got {value}. "
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
# run
# --------------------------------------------------------------------------

@dataclass
class _RunState:
    order: list[str]
    states: dict[str, str]
    errors: dict[str, str]
    fingerprints: list[dict[str, Any]]

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
    ``data_root`` and ``output_root`` are execution arguments: they are recorded
    as provenance and never enter semantic or data identity.
    """
    started = _now()
    clock = time.monotonic()
    normalize_workers(workers)

    if isinstance(request, StudyRequest):
        normalized = request
    elif isinstance(request, (str, Path)):
        normalized = study_spec.load_request(Path(request))
    else:
        normalized = study_spec.normalize_request(request, source="request", base=None)

    loaded = study_extensions.load_extensions(normalized.extensions)
    study_extensions.verify_extensions(loaded, where="study preflight")
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

        run_root = evidence.create_run_directory(output_root, data_root=pack_root)
        state = _RunState(
            order=[entry["instrument_id"] for entry in entries],
            states={entry["instrument_id"]: evidence.STATE_NOT_STARTED for entry in entries},
            errors={},
            fingerprints=[],
        )
        manifest_file = pack_manifest.manifest_path(Path(pack_root))
        provenance = {
            "schema_version": evidence.RUN_SCHEMA_VERSION,
            "data_root": str(Path(pack_root).resolve()),
            "output_root": str(run_root),
            "workers": workers,
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

        failure: dict[str, Any] | None = None
        terminal = evidence.TERMINAL_COMPLETED
        try:
            for entry in entries:
                identifier = entry["instrument_id"]
                base = session.load_slice(
                    identifier,
                    start=format_epoch_ms(normalized.study_start_ms),
                    end=format_epoch_ms(normalized.study_end_ms),
                    warmup_start=format_epoch_ms(normalized.warmup_start_ms),
                    timeframe_minutes=pack_manifest.BASE_TIMEFRAME_MINUTES,
                )
                identity = {
                    "instrument_id": identifier,
                    "roles": list(entry["roles"]),
                    "symbol": entry["symbol"],
                    "declared": {
                        "file_sha256": entry["sha256"],
                        "row_count": entry["row_count"],
                        "first_open_utc": entry["first_open_utc"],
                        "coverage_end_utc": entry["coverage_end_utc"],
                        "missing_bar_count": entry["missing_bar_count"],
                    },
                    "consumed": {
                        "warmup_start_utc": format_epoch_ms(normalized.warmup_start_ms),
                        "start_utc": format_epoch_ms(normalized.study_start_ms),
                        "end_utc": format_epoch_ms(normalized.study_end_ms),
                        "base_row_count": base.base_row_count,
                        "base_gap_count": base.base_gap_count,
                    },
                    "warmup_requirements": family["warmup_requirements"],
                }
                try:
                    prepared, fingerprints = _prepare_timeframes(entry, base, normalized)
                except PatternLabDataError as exc:
                    identity["timeframes"] = []
                    evidence.record_admission(run_root, identifier, identity)
                    state.states[identifier] = evidence.STATE_FAILED
                    state.errors[identifier] = str(exc)
                    evidence.record_job_state(
                        run_root, identifier, evidence.STATE_FAILED, extra={"error": str(exc)}
                    )
                    raise PatternLabDataError(
                        f"{identifier}: data admission failed after preflight: {exc}",
                        error_code="job_failed",
                    ) from exc
                finally:
                    del base
                identity["timeframes"] = fingerprints
                evidence.record_admission(run_root, identifier, identity)
                state.states[identifier] = evidence.STATE_ADMITTED

                study_extensions.verify_extensions(loaded, where=f"job {identifier} start")
                payload = _job_payload(entry, normalized, prepared)
                del prepared
                try:
                    result = run_instrument_job(payload)
                finally:
                    del payload
                study_extensions.verify_extensions(loaded, where=f"job {identifier} result")
                bundle = evidence.publish_job(
                    run_root, identifier, tables=result.tables, stats=result.stats
                )
                del result
                state.states[identifier] = evidence.STATE_COMPLETED
                state.fingerprints.extend(fingerprints)
                evidence.record_job_state(
                    run_root,
                    identifier,
                    evidence.STATE_COMPLETED,
                    extra={"bundle_sha256": bundle["bundle_sha256"]},
                )
        except KeyboardInterrupt:
            failure = {"reason": "keyboard_interrupt", "operation": "study", "phase": "execute"}
            _finalize_failure(
                run_root, state, provenance, started, clock, evidence.TERMINAL_INTERRUPTED, failure
            )
            raise
        except Exception as exc:
            failure = _execution_failure(run_root, state, exc, phase="execute")
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
                {"reason": "interrupted", "operation": "study", "phase": "execute"},
            )
            raise

    finished = _now()
    provenance["timings"] = {
        "started_utc": started,
        "finished_utc": finished,
        "elapsed_seconds": round(time.monotonic() - clock, 3),
    }
    identities["data_input_sha256"] = evidence.data_input_identity(
        fingerprints=state.fingerprints,
        semantic_specification=semantic,
        universe=family["instruments"],
        protocol=study_spec.protocol_document(normalized.protocol),
    )
    provenance["identities"] = identities
    evidence.write_json(run_root / evidence.PROVENANCE_FILE, provenance)
    evidence.write_status(
        run_root,
        state.document(terminal=evidence.TERMINAL_COMPLETED, started=started, finished=finished, failure=None),
    )

    try:
        loaded_results = study_results.load_results(run_root, allow_partial=True)
        if normalized.metrics:
            evidence.write_json(
                run_root / evidence.METRICS_FILE,
                study_results.compute_metric_values(loaded_results, normalized.metrics),
            )
        summary = study_results.summarize_results(loaded_results)
        html = study_report.render_report(summary)
        evidence.replace_derived(run_root, summary=summary, html=html)
        record = evidence.write_completion(
            run_root,
            summary={
                "run_root": str(run_root),
                "identities": identities,
                "counts": state.counts(),
            },
        )
    except KeyboardInterrupt:
        _finalize_failure(
            run_root, state, provenance, started, clock, evidence.TERMINAL_INTERRUPTED,
            {"reason": "keyboard_interrupt", "operation": "study", "phase": "publish"},
        )
        raise
    except Exception as exc:
        failure = {
            "reason": type(exc).__name__,
            "operation": "study",
            "phase": "publish",
            "message": str(exc),
            "instrument_id": None,
        }
        _finalize_failure(
            run_root, state, provenance, started, clock, evidence.TERMINAL_FAILED, failure
        )
        raise PatternLabStudyError(
            f"study failed during publish: {exc}",
            error_code=getattr(exc, "error_code", "study_failed"),
            context=failure,
        ) from exc
    return {
        "status": "completed",
        "run_root": str(run_root),
        "counts": state.counts(),
        "identities": identities,
        "evidence_set_sha256": record["evidence_set_sha256"],
        "report": str(run_root / evidence.REPORT_FILE),
        "summary": str(run_root / evidence.SUMMARY_FILE),
    }


def _execution_failure(run_root, state, exc, *, phase: str) -> dict[str, Any]:
    """Mark the job that failed and describe the failure for the run status."""
    instrument_id = next(
        (
            identifier
            for identifier in state.order
            if state.states[identifier] in (evidence.STATE_FAILED, evidence.STATE_ADMITTED)
        ),
        None,
    )
    for identifier in state.order:
        if state.states[identifier] == evidence.STATE_ADMITTED:
            state.states[identifier] = evidence.STATE_FAILED
            state.errors.setdefault(identifier, str(exc))
            try:
                evidence.record_job_state(
                    run_root, identifier, evidence.STATE_FAILED, extra={"error": str(exc)}
                )
            except Exception:  # pragma: no cover - the original diagnostic always wins
                pass
    return {
        "reason": type(exc).__name__,
        "operation": "study",
        "phase": phase,
        "message": str(exc),
        "instrument_id": instrument_id,
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

    Raw evidence and the completion record are verified first and never
    rewritten.  The regeneration reads no market pack and imports no saved
    custom source: recorded custom metric values and the built-in evidence
    derivation are used as they stand.  A failure here leaves the original
    successful study exactly as it was.
    """
    root = evidence.require_run_directory(run_root)
    record = evidence.verify_completion(root)
    results = study_results.load_results(root)
    if not results.complete:
        raise PatternLabDataError(
            f"{root}: this run's terminal status is {results.status['terminal_status']!r}; report "
            "regeneration requires a completed study. Use load_results(..., allow_partial=True) to "
            "inspect it instead.",
            error_code="incomplete_run",
        )
    summary = study_results.summarize_results(results)
    html = study_report.render_report(summary)
    evidence.replace_derived(root, summary=summary, html=html)
    return {
        "status": "regenerated",
        "run_root": str(root),
        "counts": results.counts,
        "evidence_set_sha256": record["evidence_set_sha256"],
        "report": str(root / evidence.REPORT_FILE),
        "summary": str(root / evidence.SUMMARY_FILE),
    }
