"""The sealed analysis artifact: layout, schemas, publication and verification.

The analysis writes into its own output root and never touches an input study
file, including that study's derived report.  Immutable artifacts are hashed
into a completion record written last; the regenerable HTML report stays outside
that hash set.  This record deliberately carries its own schema name so it can
never be mistaken for, or validated as, an M2 study completion record.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import re
import sys
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from .. import data as pack_data
from .. import manifest as pack_manifest
from ..study import contracts, evidence
from ..study import extensions as study_extensions
from ..study.builtins import EVIDENCE_VIEW_VERSION
from . import request as analysis_request

ANALYSIS_SCHEMA_VERSION = 1
SUPPORTED_ANALYSIS_SCHEMA_VERSIONS = (1, 2)
CURRENT_ANALYSIS_SCHEMA_VERSION = 2
ARTIFACT_KIND = "pattern_lab_analysis"

REQUEST_FILE = "request.json"
FAMILY_FILE = "family.json"
SOURCE_FILE = "source.json"
STRATA_FILE = "strata.parquet"
DAILY_FILE = "daily.parquet"
SUMMARY_FILE = "summary.json"
PROVENANCE_FILE = "provenance.json"
STATUS_FILE = "status.json"
COMPLETION_FILE = "completion.json"
REPORT_FILE = "derived/report.html"

# Everything the completion record hashes, in a fixed order.  The record never
# hashes itself and never hashes the regenerable report.
IMMUTABLE_FILES = (
    REQUEST_FILE,
    FAMILY_FILE,
    SOURCE_FILE,
    STRATA_FILE,
    DAILY_FILE,
    SUMMARY_FILE,
    PROVENANCE_FILE,
    STATUS_FILE,
)
DERIVED_FILES = (REPORT_FILE,)

TERMINAL_COMPLETED = "completed"
TERMINAL_FAILED = "failed"
TERMINAL_INTERRUPTED = "interrupted"
TERMINAL_RUNNING = "running"

COMPLETION_COUNT_KEYS = (
    "planned_comparisons",
    "planned_members",
    "processed_members",
    "members_with_inference",
)
COMPLETION_IDENTITY_KEYS = (
    "analysis_semantic_sha256",
    "analysis_implementation_sha256",
    "request_sha256",
    "family_sha256",
    "source_specification_sha256",
    "source_data_input_sha256",
)

# The modules whose source actually decides an analysis result, including the
# shared observation expansion and the statistic code it reuses.
ATTRIBUTED_MODULES = (
    "tools.pattern_lab.candidate",
    "tools.pattern_lab.study.validation",
    "tools.pattern_lab.study.contracts",
    "tools.pattern_lab.study.context",
    "tools.pattern_lab.study.builtins",
    "tools.pattern_lab.study.observations",
    "tools.pattern_lab.study.evidence",
    "tools.pattern_lab.study.results",
    "tools.pattern_lab.study.spec",
    "tools.pattern_lab.analysis.request",
    "tools.pattern_lab.analysis.family",
    "tools.pattern_lab.analysis.estimator",
    "tools.pattern_lab.analysis.monthly",
    "tools.pattern_lab.analysis.source",
    "tools.pattern_lab.analysis.artifacts",
    "tools.pattern_lab.analysis.report",
    "tools.pattern_lab.analysis.runner",
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def module_digests() -> dict[str, str]:
    """Digest the modules a published analysis result actually depends on."""
    digests: dict[str, str] = {}
    for name in ATTRIBUTED_MODULES:
        module = sys.modules.get(name)
        origin = getattr(module, "__file__", None) if module is not None else None
        if origin is None:
            specification = importlib.util.find_spec(name)
            origin = None if specification is None else specification.origin
        if origin and Path(origin).is_file():
            digests[name] = study_extensions.file_digest(Path(origin))
    return digests


def implementation_identity(artifact_version: int) -> dict[str, Any]:
    """The analysis implementation attribution and its own digest."""
    attribution = {
        "modules": module_digests(),
        "evidence_view_version": EVIDENCE_VIEW_VERSION,
        "library_versions": study_extensions.library_versions(),
        "note": (
            "The shared observation expansion (study/builtins.py, study/observations.py) and the "
            "resolved evidence view version are part of this attribution. Dependency versions are "
            "recorded; bitwise equality across platforms is not claimed universally."
        ),
    }
    return {
        "attribution": attribution,
        "digest": contracts.semantic_digest(
            {
                "modules": attribution["modules"],
                "evidence_view_version": EVIDENCE_VIEW_VERSION,
                "version": artifact_version,
            }
        ),
    }


def semantic_identity(
    *, request_document: Mapping[str, Any], family: Mapping[str, Any], source: Mapping[str, Any],
    artifact_version: int,
) -> str:
    """The analysis identity over the canonical request, family, method and source.

    Paths, labels, wall-clock timings and worker provenance are physical
    metadata and stay out of this digest.
    """
    return contracts.semantic_digest(
        {
            "request": request_document,
            "family": family,
            "source": source,
            "version": artifact_version,
        }
    )


# --------------------------------------------------------------------------
# output root
# --------------------------------------------------------------------------

def create_output_root(output_root: Any, *, source_root: Path, pack_root: Any = None) -> Path:
    """Create the analysis output root, refusing an existing or overlapping target."""
    root = Path(output_root).expanduser()
    if root.exists():
        raise PatternLabDataError(
            f"{root}: the analysis output root must be a new directory; an existing target is "
            "never reused, overwritten or extended. A failed output root cannot be reused "
            "automatically: choose a new root, or remove the failed artifact explicitly."
        )
    resolved = root.resolve()
    study_root = Path(source_root).expanduser().resolve()
    if resolved == study_root or study_root in resolved.parents or resolved in study_root.parents:
        raise PatternLabDataError(
            f"{resolved}: the analysis output root overlaps the source study run {study_root}; "
            "analysis output never goes inside the study it reads."
        )
    if pack_root:
        market_root = Path(pack_root).expanduser()
        if market_root.exists():
            market_root = market_root.resolve()
            if (
                resolved == market_root
                or market_root in resolved.parents
                or resolved in market_root.parents
            ):
                raise PatternLabDataError(
                    f"{resolved}: the analysis output root overlaps the recorded market-data root "
                    f"{market_root}."
                )
    root.mkdir(parents=True, exist_ok=False)
    return resolved


def require_analysis_root(analysis_root: Any) -> Path:
    root = Path(analysis_root).expanduser()
    if not root.is_dir():
        raise PatternLabDataError(
            f"{root}: the analysis root is not an existing directory. Pass the exact directory an "
            "analysis created."
        )
    return root.resolve()


# --------------------------------------------------------------------------
# writing
# --------------------------------------------------------------------------

_TABLE_DTYPES = {
    "timeframe_minutes": "int64",
    "target_observations": "int64",
    "control_observations": "int64",
    "target_days": "int64",
    "control_days": "int64",
    "overlapping_observations": "int64",
    "day_index": "int64",
    "target_count": "int64",
    "control_count": "int64",
    "overlap_count": "int64",
}


def _frame(columns: Mapping[str, Any]) -> pd.DataFrame:
    data = {}
    for name, values in columns.items():
        array = np.asarray(values)
        if name in _TABLE_DTYPES:
            array = array.astype(_TABLE_DTYPES[name])
        data[name] = array
    return pd.DataFrame(data)


def write_table(path: Path, columns: Mapping[str, Any]) -> None:
    """Write one analysis table; an empty table keeps its declared columns."""
    pa, pq = pack_data.require_pyarrow()
    frame = _frame(columns)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression=pack_data.PARQUET_COMPRESSION)
    pack_manifest.fsync_path(path)


def write_status(
    root: Path,
    *,
    artifact_version: int,
    terminal: str,
    counts: Mapping[str, int],
    started: str,
    finished: str | None,
    failure: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    document = {
        "schema_version": artifact_version,
        "artifact": ARTIFACT_KIND,
        "terminal_status": terminal,
        "counts": {key: int(counts.get(key, 0)) for key in COMPLETION_COUNT_KEYS},
        "failure": dict(failure) if failure else None,
        "started_utc": started,
        "finished_utc": finished,
    }
    evidence.write_json(Path(root) / STATUS_FILE, document)
    return document


def write_completion(
    root: Path, *, counts: Mapping[str, int], identities: Mapping[str, Any], artifact_version: int,
) -> dict[str, Any]:
    """Hash every immutable artifact and publish the completion record last."""
    base = Path(root)
    missing = [name for name in IMMUTABLE_FILES if not (base / name).is_file()]
    if missing:
        raise PatternLabDataError(
            f"{base}: cannot seal this analysis; immutable artifacts are missing: {missing}."
        )
    digests = {name: evidence.file_digest(base / name) for name in IMMUTABLE_FILES}
    record = {
        "analysis_schema_version": artifact_version,
        "artifact": ARTIFACT_KIND,
        "terminal_status": TERMINAL_COMPLETED,
        "evidence_sha256": digests,
        "evidence_set_sha256": contracts.semantic_digest(digests),
        "derived_files": list(DERIVED_FILES),
        "counts": {key: int(counts[key]) for key in COMPLETION_COUNT_KEYS},
        "identities": {key: identities[key] for key in COMPLETION_IDENTITY_KEYS},
        "analysis_root": str(base),
    }
    evidence.write_json(base / COMPLETION_FILE, record)
    return record


def replace_report(root: Path, html: str) -> None:
    base = Path(root)
    (base / "derived").mkdir(parents=True, exist_ok=True)
    pack_manifest.write_text_atomic(base / REPORT_FILE, html)


# --------------------------------------------------------------------------
# verification
# --------------------------------------------------------------------------

def _corrupt(message: str) -> PatternLabDataError:
    return PatternLabDataError(message, error_code="corrupt_evidence")


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def verify_completion(root: Path) -> dict[str, Any]:
    """Verify the complete versioned seal and every file hash it names.

    The whole schema-v1 record is validated, not only the fields the digest pass
    happens to touch, and the aggregate digest is recomputed by value.  A
    truncated, unsealed or self-contradictory artifact is rejected here, before
    any derived file is altered.
    """
    base = Path(root)
    path = base / COMPLETION_FILE
    if not path.is_file():
        raise PatternLabDataError(
            f"{base}: this directory holds no sealed analysis; {COMPLETION_FILE} is missing. An "
            "incomplete analysis output is never read as a result.",
            error_code="incomplete_analysis",
        )
    document = evidence.read_json(path)
    if not isinstance(document, Mapping):
        raise _corrupt(f"{path}: the analysis completion record is not a JSON object.")
    record = dict(document)
    version = record.get("analysis_schema_version")
    if type(version) is not int or version not in SUPPORTED_ANALYSIS_SCHEMA_VERSIONS:
        raise _corrupt(
            f"{path}: analysis_schema_version {version!r} is not the supported integer "
            f"versions {SUPPORTED_ANALYSIS_SCHEMA_VERSIONS}."
        )
    if record.get("artifact") != ARTIFACT_KIND:
        raise _corrupt(
            f"{path}: artifact {record.get('artifact')!r} is not {ARTIFACT_KIND!r}; a study "
            "completion record is not an analysis seal."
        )
    if record.get("terminal_status") != TERMINAL_COMPLETED:
        raise _corrupt(
            f"{path}: the terminal status is {record.get('terminal_status')!r}, not "
            f"{TERMINAL_COMPLETED!r}."
        )
    digests = record.get("evidence_sha256")
    if not isinstance(digests, Mapping) or not all(
        isinstance(name, str) and _is_digest(value) for name, value in digests.items()
    ):
        raise _corrupt(f"{path}: the record has no usable evidence_sha256 mapping.")
    recorded = dict(digests)
    if set(recorded) != set(IMMUTABLE_FILES):
        raise _corrupt(
            f"{path}: the hashed artifact set {sorted(recorded)} does not describe the supported "
            f"immutable artifacts {sorted(IMMUTABLE_FILES)}."
        )
    for name, expected in sorted(recorded.items()):
        target = base / name
        if not target.is_file():
            raise _corrupt(f"{target}: a recorded immutable analysis artifact is missing.")
        actual = evidence.file_digest(target)
        if actual != expected:
            raise _corrupt(
                f"{target}: SHA-256 {actual} does not match the recorded {expected}; changed "
                "analysis evidence is never trusted or re-rendered."
            )
    aggregate = record.get("evidence_set_sha256")
    if not _is_digest(aggregate):
        raise _corrupt(f"{path}: evidence_set_sha256 {aggregate!r} is not a SHA-256 digest.")
    expected_aggregate = contracts.semantic_digest(recorded)
    if aggregate != expected_aggregate:
        raise _corrupt(
            f"{path}: evidence_set_sha256 {aggregate} does not match the canonical digest "
            f"{expected_aggregate} of the verified artifact map it claims to summarize."
        )
    derived = record.get("derived_files")
    if (
        not isinstance(derived, list)
        or not all(isinstance(name, str) for name in derived)
        or len(derived) != len(set(derived))
        or set(derived) != set(DERIVED_FILES)
    ):
        raise _corrupt(
            f"{path}: derived_files {derived!r} does not describe the supported regenerable "
            f"outputs {list(DERIVED_FILES)}."
        )
    counts = record.get("counts")
    if not isinstance(counts, Mapping):
        raise _corrupt(f"{path}: the record has no counts mapping.")
    bad = sorted(key for key in COMPLETION_COUNT_KEYS if not _is_count(counts.get(key)))
    if bad:
        raise _corrupt(
            f"{path}: completion counts {bad} are missing or are not nonnegative integers."
        )
    if int(counts["processed_members"]) != int(counts["planned_members"]):
        raise _corrupt(
            f"{path}: a sealed analysis cannot record {dict(counts)!r}; every planned family "
            "member must be processed."
        )
    if int(counts["members_with_inference"]) > int(counts["planned_members"]):
        raise _corrupt(
            f"{path}: more members carry inference than the family declares: {dict(counts)!r}."
        )
    identities = record.get("identities")
    if not isinstance(identities, Mapping):
        raise _corrupt(f"{path}: the record has no identities mapping.")
    bad = sorted(key for key in COMPLETION_IDENTITY_KEYS if not _is_digest(identities.get(key)))
    if bad:
        raise _corrupt(f"{path}: completion identities {bad} are missing or are not digests.")
    # Recorded physical provenance only: never resolved, never required to exist
    # on this host, so a relocated analysis stays readable.
    if not isinstance(record.get("analysis_root"), str) or not record["analysis_root"].strip():
        raise _corrupt(
            f"{path}: analysis_root {record.get('analysis_root')!r} is not a provenance string."
        )
    return record


@dataclass(frozen=True)
class AnalysisResults:
    """One sealed analysis artifact, verified before any value is trusted."""

    analysis_root: Path
    request: Mapping[str, Any]
    family: Mapping[str, Any]
    source: Mapping[str, Any]
    summary: Mapping[str, Any]
    provenance: Mapping[str, Any]
    status: Mapping[str, Any]
    completion: Mapping[str, Any]

    @property
    def members(self) -> list[Mapping[str, Any]]:
        """The sealed result rows, in the canonical family order."""
        return list(self.summary["members"])

    def comparisons(self) -> pd.DataFrame:
        """A flat comparison table: estimates, intervals, p-values and reasons."""
        rows = []
        for member in self.members:
            intervals = member["intervals"]
            monthly = member.get("monthly_inference", {})
            rows.append(
                {
                    "member_id": member["member_id"],
                    "method": self.summary["method"]["method"],
                    "informative_months": monthly.get("informative_months"),
                    "degrees_of_freedom": monthly.get("degrees_of_freedom"),
                    **({"validation": self.summary["validation"],
                        "candidate_id": self.summary["validation"]["candidate_id"]}
                       if "validation" in self.summary else {}),
                    "standard_error_lift": monthly.get("standard_error", {}).get("lift"),
                    "month_count_in_calibration": monthly.get("month_count_in_calibration"),
                    "effect_sign": member["effect_sign"],
                    "nominal_reject_raw": (None if not member["inference_available"] else
                                           bool(member["p_raw"] <= self.summary["method"]["alpha"])),
                    "comparison_id": member["comparison_id"],
                    "kind": member["kind"],
                    "model_instance_id": member["model_instance_id"],
                    "timeframe_minutes": member["timeframe_minutes"],
                    "case_id": member["case_id"],
                    "direction": member["direction"],
                    "horizon_minutes": member["horizon_minutes"],
                    "primary": member["primary"],
                    "signal_net": member["signal"],
                    "control_net": member["control"],
                    "lift_net": member["lift"],
                    "signal_gross": member["signal_gross"],
                    "control_gross": member["control_gross"],
                    "lift_gross": member["lift_gross"],
                    "retained_target_observations": member["supported_population"][
                        "retained_target_observations"
                    ],
                    "retained_control_observations": member["supported_population"][
                        "retained_control_observations"
                    ],
                    "retained_target_share": member["supported_population"][
                        "retained_target_share"
                    ],
                    "lift_interval_lower": None if intervals["lift"] is None else intervals["lift"]["lower"],
                    "lift_interval_upper": None if intervals["lift"] is None else intervals["lift"]["upper"],
                    "p_raw": member["p_raw"],
                    "p_holm": member["p_holm"],
                    "nominal_reject_holm": member["nominal_reject_holm"],
                    "inference_available": member["inference_available"],
                    "unavailable_reasons": ";".join(member["unavailable_reasons"]),
                    "joint_active_days": member["geometry"]["joint_active_days"],
                    "supported_span_days": member["geometry"]["supported_span_days"],
                    "supported_blocks": member["geometry"]["supported_blocks"],
                }
            )
        return pd.DataFrame(rows)

    def strata(self) -> pd.DataFrame:
        """Every stratum, including zero-event and excluded ones, with reasons."""
        return evidence.read_table(self.analysis_root / STRATA_FILE)

    def daily(self) -> pd.DataFrame:
        """Daily counts and sums sufficient to reproduce the estimator."""
        return evidence.read_table(self.analysis_root / DAILY_FILE)



def load_analysis(analysis_root: Any) -> AnalysisResults:
    """Verify and load a sealed analysis artifact.

    The complete versioned seal, its file hashes and the agreement between the
    request, the family and the saved results are all checked before any value is
    trusted.  Nothing here needs the original study, the market pack or any saved
    module.
    """
    root = require_analysis_root(analysis_root)
    completion = verify_completion(root)
    request_document = dict(evidence.read_json(root / REQUEST_FILE))
    family = dict(evidence.read_json(root / FAMILY_FILE))
    source = dict(evidence.read_json(root / SOURCE_FILE))
    summary = dict(evidence.read_json(root / SUMMARY_FILE))
    provenance = dict(evidence.read_json(root / PROVENANCE_FILE))
    status = dict(evidence.read_json(root / STATUS_FILE))
    verify_agreement(
        root,
        completion=completion,
        request_document=request_document,
        family=family,
        source=source,
        summary=summary,
        status=status,
        provenance=provenance,
    )
    return AnalysisResults(
        analysis_root=root,
        request=request_document,
        family=family,
        source=source,
        summary=summary,
        provenance=provenance,
        status=status,
        completion=completion,
    )


def verify_agreement(
    root: Path,
    *,
    completion: Mapping[str, Any],
    request_document: Mapping[str, Any],
    family: Mapping[str, Any],
    source: Mapping[str, Any],
    summary: Mapping[str, Any],
    status: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> None:
    """Cross-check counts, identities and result/family membership by value."""
    path = root / COMPLETION_FILE
    version = completion["analysis_schema_version"]
    for name, document in (("summary", summary), ("status", status), ("provenance", provenance)):
        if type(document.get("schema_version")) is not int or document["schema_version"] != version:
            raise _corrupt(f"{path}: {name} artifact version contradicts completion")
    try:
        request_version = analysis_request.validate_saved_request(
            request_document, source="saved analysis request"
        )
    except PatternLabDataError as error:
        raise _corrupt(f"{path}: invalid saved request: {error}") from error
    if request_version != version:
        raise _corrupt(f"{path}: request/artifact version mapping is inconsistent")
    expected_method = request_document["method"]
    if type(source.get("schema_version")) is not int or source["schema_version"] not in (1, 2):
        raise _corrupt("source binding: unsupported version")
    if source.get("schema_version") == 2:
        execution = source["semantic_inputs"].get("execution")
        if not isinstance(execution, dict):
            raise _corrupt("source binding v2 requires execution metadata")
        if execution.get("kind") == "validation":
            from ..candidate import load_candidate, validation_metadata
            candidate = load_candidate(execution.get("candidate"))
            semantic = candidate["recipe"]["study_semantic"]
            if (source["study"] != candidate["split"]["evaluation"] or source["protocol"] != semantic["protocol"]
                    or source["semantic_inputs"]["context"] != semantic["context"]
                    or source["instruments"] != semantic["instruments"]["ids"]
                    or source["timeframes_minutes"] != semantic["timeframes_minutes"]
                    or family != candidate["recipe"]["analysis_family"]
                    or summary.get("validation") != validation_metadata(candidate)):
                raise _corrupt("source binding: inconsistent candidate, purpose, family or split")
        elif execution != {"kind":"development"}:
            raise _corrupt("source binding: invalid execution purpose")
        else:
            from ..study.validation import validate_saved_execution
            validate_saved_execution({"schema_version":2, "execution":execution,
                "context":source["semantic_inputs"]["context"], "study":source["study"], "protocol":source["protocol"]})
        if source["semantic_inputs"]["study"] != source["study"] or source["semantic_inputs"]["protocol"] != source["protocol"]:
            raise _corrupt("source binding: semantic projection contradicts protocol/period")
    for name, document in (("family", family), ("summary", summary)):
        if contracts.semantic_digest(document.get("method")) != contracts.semantic_digest(expected_method):
            raise _corrupt(f"{path}: {name} method contradicts the saved request method")
    if status.get("terminal_status") != TERMINAL_COMPLETED:
        raise _corrupt(
            f"{path}: the seal claims a completed analysis, but the recorded terminal status is "
            f"{status.get('terminal_status')!r}."
        )
    declared = [item["member_id"] for item in family["members"]]
    produced = [item["member_id"] for item in summary["members"]]
    if declared != produced:
        raise _corrupt(
            f"{path}: the saved results do not cover the frozen family in its canonical order; "
            f"{len(declared)} declared member(s) against {len(produced)} produced."
        )
    counts = completion["counts"]
    if int(counts["planned_members"]) != len(declared):
        raise _corrupt(
            f"{path}: the sealed planned member count {counts['planned_members']} contradicts the "
            f"frozen family's {len(declared)}."
        )
    if int(counts["planned_comparisons"]) != len(family["comparisons"]):
        raise _corrupt(
            f"{path}: the sealed comparison count {counts['planned_comparisons']} contradicts the "
            f"frozen family's {len(family['comparisons'])}."
        )
    available = sum(1 for item in summary["members"] if item["inference_available"])
    if int(counts["members_with_inference"]) != available:
        raise _corrupt(
            f"{path}: the sealed inferential member count {counts['members_with_inference']} "
            f"contradicts the {available} saved results that carry inference."
        )
    identities = completion["identities"]
    recomputed = {
        "request_sha256": contracts.semantic_digest(
            _semantic_request_document(request_document)
        ),
        "family_sha256": contracts.semantic_digest(family),
        "source_specification_sha256": source["semantic"]["specification_sha256"],
        "source_data_input_sha256": source["semantic"]["data_input_sha256"],
    }
    disagree = sorted(
        key for key, value in recomputed.items() if identities.get(key) != value
    )
    if disagree:
        raise _corrupt(
            f"{path}: the sealed identities {disagree} contradict the artifacts of the same "
            "analysis."
        )
    expected_semantic = semantic_identity(
        artifact_version=version,
        request_document=_semantic_request_document(request_document),
        family=family,
        source=source["semantic_inputs"],
    )
    if identities.get("analysis_semantic_sha256") != expected_semantic:
        raise _corrupt(
            f"{path}: the sealed analysis semantic identity contradicts the request, family and "
            "source binding it claims to summarize."
        )
    implementation = provenance.get("implementation", {})
    expected_implementation = contracts.semantic_digest({
        "modules": implementation.get("modules"),
        "evidence_view_version": implementation.get("evidence_view_version"),
        "version": version,
    })
    if identities.get("analysis_implementation_sha256") != expected_implementation:
        raise _corrupt(f"{path}: implementation identity contradicts recorded attribution/version")
    if provenance.get("identities") != identities:
        raise _corrupt(f"{path}: provenance identities contradict completion")


def _semantic_request_document(request_document: Mapping[str, Any]) -> dict[str, Any]:
    """The request's semantic payload; the free-form name and notes stay out."""
    return {
        key: value
        for key, value in request_document.items()
        if key not in ("analysis_name", "notes")
    }


def summary_document(
    *,
    request: analysis_request.AnalysisRequest,
    family: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    estimates: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    disclosures: Sequence[str],
    artifact_version: int,
) -> dict[str, Any]:
    """The immutable result document the report renders and agents read."""
    execution = source_binding["semantic_inputs"].get("execution", {"kind":"development"})
    validation_info = None
    if execution["kind"] == "validation":
        from ..candidate import validation_metadata
        validation_info = validation_metadata(execution["candidate"])
    return {
        **({"validation": validation_info} if validation_info is not None else {}),
        "schema_version": artifact_version,
        "artifact": ARTIFACT_KIND,
        "analysis_name": request.analysis_name,
        "notes": request.notes,
        "method": estimates["method"],
        "inference_scope": estimates["inference_scope"],
        "nominal_levels": {
            "alpha": analysis_request.ALPHA,
            "confidence_level": analysis_request.CONFIDENCE_LEVEL,
            "note": ("Monthly jackknife passed the declared synthetic screen at G=12; nominal levels "
                     "remain approximate and do not certify market error control." if artifact_version == 2 else (
                "Every test is nominally 5% and every interval nominally 95%. The delivered "
                "calibration did not meet the declared empirical error envelope: measured "
                "rejection and noncoverage reached about 7.5-8.3% on fixtures with persistent "
                "daily signal states, so these levels are nominal and anti-conservative rather "
                "than verified."
            )),
        },
        "calendar": estimates["calendar"],
        **({key: estimates[key] for key in ("resamples", "seed", "p_resolution",
            "p_resolution_blocks_first_rejection")} if artifact_version == 1 else {}),
        "family_size": estimates["family_size"],
        "instruments": estimates["instruments"],
        "comparisons": list(family["comparisons"]),
        "source": dict(source_binding),
        **({"source_variants": list(family["source_variants"])} if source_binding["schema_version"] == 2 else {}),
        "diagnostics": dict(diagnostics),
        "members": list(estimates["members"]),
        "units": (
            "Returns are fractions internally; the report labels percentages and percentage "
            "points explicitly."
        ),
        "disclosures": list(disclosures),
    }
