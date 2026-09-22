"""The analysis coordinator: admit, freeze, estimate, publish.

``run_analysis`` admits a completed source study, freezes the normalized
request, the source binding and the resolved family *before* any outcome
aggregation, runs the one numerical estimator over the checked evidence
alignment, then publishes the artifacts atomically with the completion record
last.  ``regenerate_report`` re-renders the sealed summary and needs neither the
original study nor the market pack.
"""

from __future__ import annotations

from pathlib import Path
import time
from typing import Any

from .. import PatternLabStudyError
from ..study import contracts, evidence
from ..study import extensions as study_extensions
from . import artifacts, family as analysis_family, report as analysis_report
from . import request as analysis_request
from . import source as analysis_source
from .estimator import evaluate_observations, evaluate_monthly_observations

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

EXPLORATORY_DISCLOSURES = (
    "Exploratory development analysis. The family is frozen for this execution; that is not "
    "historical preregistration, and a rerun is still exploratory.",
    "Inference is an approximate development screen, not certification of exact 5% family-wise "
    "error, 95% coverage or an independently validated edge.",
)

OBSERVATION_DISCLOSURES = (
    "Observations overlap in time and across comparisons: these are conditional measurements, not "
    "an executable equity curve or realized profit.",
    "Matching is conditional on instrument and UTC calendar month only. It does not remove every "
    "market regime or historical universe-selection bias.",
    "Occurrence variants that share one condition also share its known-false control population; "
    "those comparisons are not independent tests.",
    "Commission is included; funding and slippage are excluded by explicit decision.",
)

NONREJECTION_DISCLOSURE = (
    "A non-rejection means insufficient evidence for that test, not proof that the effect is "
    "absent. Holm covers only this declared family, not an unrecorded adaptive search."
)

# Preserve the public legacy tuple's text and order.
DISCLOSURES = (
    *EXPLORATORY_DISCLOSURES,
    "The delivered calibration did NOT meet its declared empirical error envelope. On the tracked "
    "fixtures with persistent daily signal states, rejection and nominal-95% noncoverage reached "
    "approximately 7.5-8.3% against nominal 5%. The cause is still under investigation; these "
    "inferential outputs remain unvalidated and were anti-conservative on those fixtures.",
    "The calendar block bootstrap assumes weak dependence, adequate moments and support and a "
    "reasonably stable centered influence process. A seven-day block does not control error under "
    "dependence substantially longer than a week; see the tracked long-dependence experiment.",
    *OBSERVATION_DISCLOSURES,
    "Neither event counts, ticker breadth nor bootstrap draws imply independent observations.",
    NONREJECTION_DISCLOSURE,
)

MONTHLY_DISCLOSURES = (
    *EXPLORATORY_DISCLOSURES,
    "Monthly jackknife passed eight required synthetic null fixtures at G=12, 2,000 attempts each. "
    "Observed primary raw rejection was 4.15-5.70%; Holm global-null rejection was 0.90-2.40%. "
    "Acceptance required an exact one-sided 95% error-rate upper bound at most 8%, separately "
    "from 95% primary availability. PASS does not certify nominal 5% market error.",
    "Calendar months need not be independent. Tested persistent signal occurrence does not establish "
    "error control under arbitrary multi-month dependence of returns or correlated monthly contrasts.",
    "Only G=12 was covered by retained calibration. Another known G remains mathematically admissible "
    "but was not covered; equality of G alone is not validation. Balance is diagnostic, not effective df.",
    "Raw rejection/noncoverage and mirrored long/short checks are correlated, not 24 independent confirmations.",
    *OBSERVATION_DISCLOSURES,
    "Neither event counts nor ticker breadth imply independent observations.",
    NONREJECTION_DISCLOSURE,
)


def _failure_document(exc: BaseException, *, phase: str) -> dict[str, Any]:
    return {
        "reason": "keyboard_interrupt" if isinstance(exc, KeyboardInterrupt) else type(exc).__name__,
        "operation": "analyze",
        "phase": phase,
        "message": str(exc),
    }


def run_analysis(
    *,
    request: Any,
    run_root: Any,
    output_root: Any,
) -> dict[str, Any]:
    """Run one offline analysis of a completed study into a new output root."""
    started = artifacts.now_utc()
    clock = time.monotonic()

    normalized = analysis_request.load_analysis_request(request)
    artifact_version = {1: 1, 2: 2}[normalized.schema_version]
    source = analysis_source.admit_source(
        run_root, model_instances=normalized.model_instances, where="analysis admission"
    )
    analysis_request.require_known_variants(
        normalized, [item["variant_id"] for item in source.variants], where="analysis admission"
    )
    analysis_request.require_known_models(
        normalized, source.instances, where="analysis admission"
    )
    comparisons, members = analysis_family.resolve_family(
        normalized,
        variants=source.variants,
        instances=source.instances,
        timeframes=source.timeframes,
    )
    family_document = analysis_family.family_document(
        request_version=normalized.schema_version,
        comparisons=comparisons,
        members=members,
        model_instances=sorted(normalized.model_instances),
        timeframes=source.timeframes,
        variants=source.variants,
    )
    request_document = normalized.request_document()
    source_binding = source.binding_document()
    pack_root = source.results.provenance.get("data_root")

    root = artifacts.create_output_root(
        output_root, source_root=source.run_root, pack_root=pack_root
    )
    counts = {
        "planned_comparisons": len(comparisons),
        "planned_members": len(members),
        "processed_members": 0,
        "members_with_inference": 0,
    }
    # The output root is admitted and created before the protected region, so
    # `root`, `counts`, `started`, `clock` and `phase` are always bound when the
    # failure handler runs; an admission failure keeps its own cause and never
    # reaches status writing with an unbound or unowned root.
    phase = "freeze"
    try:
        # The request, the source binding, the resolved family and an initial
        # status are durable before any outcome is aggregated, and a failure in
        # any of those writes is recorded like every later one.
        evidence.write_json(root / artifacts.REQUEST_FILE, request_document)
        evidence.write_json(root / artifacts.SOURCE_FILE, source_binding)
        evidence.write_json(root / artifacts.FAMILY_FILE, family_document)
        artifacts.write_status(
            root, terminal=artifacts.TERMINAL_RUNNING, counts=counts, started=started,
            finished=None, artifact_version=artifact_version,
        )

        phase = "aggregate"
        records = analysis_source.RecordSource(source, members)
        evaluate = evaluate_observations if normalized.schema_version == 1 else evaluate_monthly_observations
        settings = {"resamples": normalized.resamples, "seed": normalized.seed} if normalized.schema_version == 1 else {}
        estimates = evaluate(
            records.frames(),
            family=members,
            instruments=source.instruments,
            study_start_ms=source.study_start_ms,
            study_end_ms=source.study_end_ms,
            **settings,
        )
        phase = "verify_source"
        analysis_source.reverify_source(source, where="analysis publication")
        phase = "publish"
        counts["processed_members"] = len(estimates["members"])
        counts["members_with_inference"] = sum(
            1 for item in estimates["members"] if item["inference_available"]
        )
        diagnostics = {
            "eligible_anchors_by_timeframe": dict(records.eligible_anchors),
            "source_family_counts": analysis_source.source_family_counts(source),
            "source_study_interval": analysis_source.describe_interval(source),
            "decoded_evidence_tables": records.decoded_tables,
            "record_scope": (
                "Only anchors that are a target or a control of the comparison are materialized as "
                "records; an anchor that is neither contributes to no statistic. Availability and "
                "validity losses are counted per member."
            ),
        }
        summary = artifacts.summary_document(
            request=normalized,
            family=family_document,
            source_binding=source_binding,
            estimates=estimates,
            diagnostics=diagnostics,
            disclosures=DISCLOSURES if normalized.schema_version == 1 else MONTHLY_DISCLOSURES,
            artifact_version=artifact_version,
        )
        artifacts.write_table(root / artifacts.STRATA_FILE, estimates["strata"])
        artifacts.write_table(root / artifacts.DAILY_FILE, estimates["daily"])
        evidence.write_json(root / artifacts.SUMMARY_FILE, summary)

        implementation = artifacts.implementation_identity(artifact_version)
        semantic_request = {
            key: value
            for key, value in request_document.items()
            if key not in ("analysis_name", "notes")
        }
        identities = {
            "analysis_semantic_sha256": artifacts.semantic_identity(
                artifact_version=artifact_version,
                request_document=semantic_request,
                family=family_document,
                source=source_binding["semantic_inputs"],
            ),
            "analysis_implementation_sha256": implementation["digest"],
            "request_sha256": contracts.semantic_digest(semantic_request),
            "family_sha256": analysis_family.family_identity(family_document),
            "source_specification_sha256": source_binding["semantic"]["specification_sha256"],
            "source_data_input_sha256": source_binding["semantic"]["data_input_sha256"],
        }
        finished = artifacts.now_utc()
        evidence.write_json(
            root / artifacts.PROVENANCE_FILE,
            {
                "schema_version": artifact_version,
                "artifact": artifacts.ARTIFACT_KIND,
                "analysis_root": str(root),
                "source_run_root": str(source.run_root),
                "identities": identities,
                "implementation": implementation["attribution"],
                "environment": study_extensions.environment_provenance(REPOSITORY_ROOT),
                "timings": {
                    "started_utc": started,
                    "finished_utc": finished,
                    "elapsed_seconds": round(time.monotonic() - clock, 3),
                    "note": (
                        "Wall time includes both full source-integrity passes: strict admission "
                        "and the pre-publication reverification."
                    ),
                },
            },
        )
        artifacts.write_status(
            root,
            artifact_version=artifact_version,
            terminal=artifacts.TERMINAL_COMPLETED,
            counts=counts,
            started=started,
            finished=finished,
        )
        artifacts.verify_agreement(
            root,
            completion={
                "analysis_schema_version": artifact_version,
                "counts": counts,
                "identities": identities,
            },
            request_document=request_document, family=family_document,
            source=source_binding, summary=summary,
            provenance=evidence.read_json(root / artifacts.PROVENANCE_FILE),
            status=evidence.read_json(root / artifacts.STATUS_FILE),
        )
        artifacts.replace_report(root, analysis_report.render_report(summary))
        record = artifacts.write_completion(root, counts=counts, identities=identities, artifact_version=artifact_version)
    except KeyboardInterrupt as exc:
        _record_failure(root, counts, started, clock, artifacts.TERMINAL_INTERRUPTED, exc, phase, artifact_version)
        raise
    except Exception as exc:
        _record_failure(root, counts, started, clock, artifacts.TERMINAL_FAILED, exc, phase, artifact_version)
        raise PatternLabStudyError(
            f"analysis failed during {phase}: {exc}",
            error_code=getattr(exc, "error_code", "analysis_failed"),
            context={"operation": "analyze", "phase": phase, "analysis_root": str(root)},
        ) from exc
    except BaseException as exc:  # pragma: no cover - control flow, never masked
        _record_failure(root, counts, started, clock, artifacts.TERMINAL_FAILED, exc, phase, artifact_version)
        raise

    return {
        "status": "completed",
        "analysis_root": str(root),
        "source_run_root": str(source.run_root),
        "counts": dict(counts),
        "identities": dict(record["identities"]),
        "evidence_set_sha256": record["evidence_set_sha256"],
        "family_size": len(members),
        "members_with_inference": counts["members_with_inference"],
        "inference_scope": analysis_request.INFERENCE_SCOPE,
        "report": str(root / artifacts.REPORT_FILE),
        "summary": str(root / artifacts.SUMMARY_FILE),
    }


def _sealed(root) -> bool:
    """True when this analysis already has a verifiable completion record."""
    try:
        artifacts.verify_completion(root)
    except Exception:
        return False
    return True


def _record_failure(root, counts, started, clock, terminal, exc, phase, artifact_version) -> None:
    """Record an honest terminal state; a status failure never masks the cause.

    Once the completion record has been atomically published the analysis is
    sealed, and a later exception never rewrites that sealed status.
    """
    if _sealed(root):
        return
    try:
        artifacts.write_status(
            root,
            artifact_version=artifact_version,
            terminal=terminal,
            counts=counts,
            started=started,
            finished=artifacts.now_utc(),
            failure={
                **_failure_document(exc, phase=phase),
                "elapsed_seconds": round(time.monotonic() - clock, 3),
                "note": (
                    "No completion seal exists. This output root cannot be reused automatically: "
                    "choose a new root, or remove this failed artifact explicitly."
                ),
            },
        )
    except Exception:  # pragma: no cover - the original diagnostic always wins
        pass


def load_analysis(analysis_root: Any) -> artifacts.AnalysisResults:
    """Verify and load a sealed analysis artifact; see :mod:`.artifacts`."""
    return artifacts.load_analysis(analysis_root)


def regenerate_report(analysis_root: Any) -> dict[str, Any]:
    """Re-render the sealed analysis summary as HTML.

    The complete versioned seal and its file hashes are verified first, so an
    unsealed, truncated or inconsistent artifact is rejected before any derived
    file is altered.  This renders the saved results: it reruns no bootstrap and
    needs neither the original study, its market pack nor any saved module.  It
    deliberately differs from M2's ``report``, which recomputes its summary from
    raw evidence.  A failure here never reclassifies a sealed analysis.
    """
    results = artifacts.load_analysis(analysis_root)
    artifacts.replace_report(results.analysis_root, analysis_report.render_report(results.summary))
    return {
        "status": "regenerated",
        "analysis_root": str(results.analysis_root),
        "counts": dict(results.completion["counts"]),
        "evidence_set_sha256": results.completion["evidence_set_sha256"],
        "report": str(results.analysis_root / artifacts.REPORT_FILE),
    }


__all__ = [
    "DISCLOSURES",
    "load_analysis",
    "regenerate_report",
    "run_analysis",
]
