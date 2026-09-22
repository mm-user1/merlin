"""The versioned analysis request and the frozen method constants.

One strict normalization path serves the CLI and agent scripts: a JSON file path
and a mapping reach the same validators, and there is no trusted
normalized-object bypass. Version 1 preserves the calendar block bootstrap;
version 2 selects monthly jackknife with the same matching and support rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .. import PatternLabDataError
from ..manifest import read_json_file, require_int, require_text
from ..study import contracts

ANALYSIS_REQUEST_SCHEMA_VERSION = 1
SUPPORTED_REQUEST_VERSIONS = (1, 2)
CURRENT_REQUEST_SCHEMA_VERSION = 2
V2_METHOD_ID = "monthly_cluster_jackknife_v1"

# Version 1 fixes the method; these are resolved values recorded in the frozen
# family and identity, not optional request switches.
METHOD_ID = "calendar_score_cbb_v1"
MATCHING_ID = "instrument_utc_month_v1"
BLOCK_LENGTH_DAYS = 7
ALPHA = 0.05
CONFIDENCE_LEVEL = 0.95

# Support rules of version 1.  They depend on availability and counts, never on
# return values, and they are explicit safeguards rather than assertions that the
# retained observations are independent.
MIN_STRATUM_TARGET = 20
MIN_STRATUM_CONTROL = 20
MIN_STRATUM_TARGET_DAYS = 10
MIN_STRATUM_CONTROL_DAYS = 10
MIN_DAY_GRID = 336
MIN_SUPPORTED_SPAN_DAYS = 336
MIN_JOINT_ACTIVE_DAYS = 252
MIN_SUPPORTED_BLOCKS = 48
MIN_SUPPORTED_BLOCK_ACTIVE_DAYS = 4
MIN_RETAINED_TARGET_SHARE = 0.80
MAX_INFERENCE_HORIZON_MINUTES = 480

# The approximate development screen this delivery implements.  It is visible
# next to every inferential output and is not a certification of exact 5% error.
INFERENCE_SCOPE = "approximate_development_screen"

MIN_RESAMPLES = 1999
MAX_RESAMPLES = 99999
MAX_SEED = 2**32 - 1

# Deterministic reason order; every applicable reason is reported.
REASON_NO_SUPPORT = "no_matched_support"
REASON_SPAN = "insufficient_span"
REASON_ACTIVE_DAYS = "insufficient_active_days"
REASON_BLOCKS = "insufficient_supported_blocks"
REASON_COVERAGE = "insufficient_retained_coverage"
REASON_HORIZON = "unsupported_inference_horizon"
REASON_DEGENERATE = "degenerate_contrast"
REASON_ORDER = (
    REASON_NO_SUPPORT,
    REASON_SPAN,
    REASON_ACTIVE_DAYS,
    REASON_BLOCKS,
    REASON_COVERAGE,
    REASON_HORIZON,
    REASON_DEGENERATE,
)

# Generated nonsignal-baseline comparison IDs reserve this prefix.
BASELINE_PREFIX = "baseline__"

REQUEST_KEYS = (
    "schema_version",
    "analysis_name",
    "model_instances",
    "pairwise",
    "resamples",
    "seed",
    "notes",
)
PAIRWISE_KEYS = ("id", "target_variant", "control_variant")


def method_settings(schema_version: int = 1) -> dict[str, Any]:
    """Resolved constants; the no-argument form retains the legacy v1 contract."""
    _require_version(schema_version, "schema_version")
    settings = {
        "method": METHOD_ID,
        "matching": MATCHING_ID,
        "block_length_days": BLOCK_LENGTH_DAYS,
        "alpha": ALPHA,
        "confidence_level": CONFIDENCE_LEVEL,
        "inference_scope": INFERENCE_SCOPE,
        "support": {
            "min_stratum_target": MIN_STRATUM_TARGET,
            "min_stratum_control": MIN_STRATUM_CONTROL,
            "min_stratum_target_days": MIN_STRATUM_TARGET_DAYS,
            "min_stratum_control_days": MIN_STRATUM_CONTROL_DAYS,
            "min_day_grid": MIN_DAY_GRID,
            "min_supported_span_days": MIN_SUPPORTED_SPAN_DAYS,
            "min_joint_active_days": MIN_JOINT_ACTIVE_DAYS,
            "min_supported_blocks": MIN_SUPPORTED_BLOCKS,
            "min_supported_block_active_days": MIN_SUPPORTED_BLOCK_ACTIVE_DAYS,
            "min_retained_target_share": MIN_RETAINED_TARGET_SHARE,
            "max_inference_horizon_minutes": MAX_INFERENCE_HORIZON_MINUTES,
        },
    }
    if schema_version == 2:
        settings.pop("block_length_days")
        settings.update(method=V2_METHOD_ID, grouping="UTC signal month, all instruments jointly",
                        reference="Student t with G-1 degrees of freedom",
                        confidence_level=CONFIDENCE_LEVEL,
                        mathematical_validity={"min_informative_months": 2,
                            "positive_deletion_denominator": True, "finite_inputs_and_results": True,
                            "degeneracy_multiplier": 128},
                        calibration={"tested_informative_month_counts": [12], "main_null_fixtures": 8,
                            "attempts_per_fixture": 2000, "primary_raw_rejection_range": [0.0415, 0.057],
                            "holm_fwer_range": [0.009, 0.024], "one_sided_95_upper_bound_envelope": 0.08,
                            "minimum_primary_availability": 0.95,
                            "scope": "Synthetic screen only; matching G does not validate market inference."})
    return settings


def _require_version(value, where):
    version = require_int(value, where)
    if version not in SUPPORTED_REQUEST_VERSIONS:
        raise PatternLabDataError(f"{where}: unsupported analysis request version {version}; reads 1 and 2.")
    return version


@dataclass(frozen=True)
class PairwiseComparison:
    """One explicitly declared target-versus-control variant comparison."""

    comparison_id: str
    target_variant: str
    control_variant: str

    def as_json(self) -> dict[str, Any]:
        return {
            "id": self.comparison_id,
            "target_variant": self.target_variant,
            "control_variant": self.control_variant,
        }


@dataclass(frozen=True)
class AnalysisRequest:
    """The normalized semantic analysis request."""

    schema_version: int
    analysis_name: str
    model_instances: tuple[str, ...]
    pairwise: tuple[PairwiseComparison, ...]
    resamples: int | None
    seed: int | None
    notes: str | None

    def semantic_document(self) -> dict[str, Any]:
        """The canonical semantic payload; the free-form name and notes stay out."""
        document = self.external_document()
        document.pop("analysis_name")
        document.pop("notes")
        document["method"] = method_settings(self.schema_version)
        return document

    def request_document(self) -> dict[str, Any]:
        document = self.semantic_document()
        document["analysis_name"] = self.analysis_name
        document["notes"] = self.notes
        return document

    def external_document(self) -> dict[str, Any]:
        """Render this request back into the external request schema."""
        _require_version(self.schema_version, "normalized analysis request.schema_version")
        if self.schema_version == 2 and (self.seed is not None or self.resamples is not None):
            raise PatternLabDataError("normalized analysis request: v2 does not support seed or resamples")
        document = {
            "schema_version": self.schema_version,
            "analysis_name": self.analysis_name,
            "model_instances": list(self.model_instances),
            "pairwise": [item.as_json() for item in self.pairwise],
            "resamples": self.resamples,
            "seed": self.seed,
            "notes": self.notes,
        }
        if self.schema_version == 2:
            document.pop("resamples")
            document.pop("seed")
        return document


def _require_bounded_int(value: Any, field_name: str, *, minimum: int, maximum: int) -> int:
    """Return an integer inside an inclusive range; booleans are not integers.

    ``require_int`` has no ``maximum`` argument, so the upper bound is checked
    explicitly here rather than silently accepted.
    """
    number = require_int(value, field_name, minimum=minimum)
    if number > maximum:
        raise PatternLabDataError(f"{field_name}: must be <= {maximum}, got {number}.")
    return number


def _normalize_model_instances(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)) or not raw:
        raise PatternLabDataError(
            "model_instances: expected a nonempty list of model-instance IDs of the source study."
        )
    identifiers: list[str] = []
    for index, item in enumerate(raw):
        text = require_text(item, f"model_instances[{index}]")
        if text in identifiers:
            raise PatternLabDataError(
                f"model_instances[{index}]: duplicate model instance {text!r}."
            )
        identifiers.append(text)
    return tuple(identifiers)


def _normalize_pairwise(raw: Any) -> tuple[PairwiseComparison, ...]:
    if raw is None:
        return ()
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError("pairwise: expected a list of explicit comparison declarations.")
    comparisons: list[PairwiseComparison] = []
    seen_ids: set[str] = set()
    seen_pairs: dict[tuple[str, str], str] = {}
    for index, item in enumerate(raw):
        where = f"pairwise[{index}]"
        values = contracts.require_mapping(item, where)
        contracts.closed_keys(values, PAIRWISE_KEYS, where)
        for key in PAIRWISE_KEYS:
            if key not in values:
                raise PatternLabDataError(f"{where}.{key}: an explicit value is required.")
        comparison_id = contracts.require_identifier(values["id"], f"{where}.id")
        if comparison_id.startswith(BASELINE_PREFIX):
            raise PatternLabDataError(
                f"{where}.id: {comparison_id!r} uses the reserved {BASELINE_PREFIX!r} prefix of the "
                "automatically generated nonsignal-baseline comparisons."
            )
        if comparison_id in seen_ids:
            raise PatternLabDataError(f"{where}.id: duplicate comparison ID {comparison_id!r}.")
        target = require_text(values["target_variant"], f"{where}.target_variant")
        control = require_text(values["control_variant"], f"{where}.control_variant")
        if target == control:
            raise PatternLabDataError(
                f"{where}: a comparison of variant {target!r} with itself is not a comparison."
            )
        pair = (target, control)
        if pair in seen_pairs:
            raise PatternLabDataError(
                f"{where}: the same semantic pair is already declared as {seen_pairs[pair]!r}; "
                "duplicate semantic comparisons are rejected instead of counted twice."
            )
        seen_ids.add(comparison_id)
        seen_pairs[pair] = comparison_id
        comparisons.append(
            PairwiseComparison(
                comparison_id=comparison_id, target_variant=target, control_variant=control
            )
        )
    return tuple(comparisons)


def normalize_analysis_request(document: Any, *, source: str) -> AnalysisRequest:
    """Validate an analysis request document and return its normalized form."""
    values = contracts.require_mapping(document, source)
    version = _require_version(values.get("schema_version"), f"{source}.schema_version")
    keys = REQUEST_KEYS if version == 1 else tuple(k for k in REQUEST_KEYS if k not in ("seed", "resamples"))
    contracts.closed_keys(values, keys, source)
    analysis_name = require_text(values.get("analysis_name"), f"{source}.analysis_name")
    notes = values.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise PatternLabDataError(f"{source}.notes: expected a string or null.")
    for key in (("model_instances", "resamples", "seed") if version == 1 else ("model_instances",)):
        if key not in values:
            raise PatternLabDataError(f"{source}.{key}: an explicit value is required.")
    model_instances = _normalize_model_instances(values["model_instances"])
    pairwise = _normalize_pairwise(values.get("pairwise"))
    resamples = None if version == 2 else _require_bounded_int(
        values["resamples"], f"{source}.resamples", minimum=MIN_RESAMPLES, maximum=MAX_RESAMPLES
    )
    seed = None if version == 2 else _require_bounded_int(values["seed"], f"{source}.seed", minimum=0, maximum=MAX_SEED)
    return AnalysisRequest(
        schema_version=version,
        analysis_name=analysis_name,
        model_instances=model_instances,
        pairwise=pairwise,
        resamples=resamples,
        seed=seed,
        notes=notes,
    )


def load_analysis_request(request: Any) -> AnalysisRequest:
    """Normalize any accepted public request form through one strict path.

    A path is read as strict JSON and a mapping is validated directly.  There is
    no trusted normalized-object bypass: an already normalized
    :class:`AnalysisRequest` is rendered back into the external schema and
    revalidated, so derived facts can never contradict the settings they claim
    to summarize.
    """
    if isinstance(request, AnalysisRequest):
        return normalize_analysis_request(
            request.external_document(), source="normalized analysis request"
        )
    if isinstance(request, (str, Path)):
        path = Path(request).expanduser().resolve()
        return normalize_analysis_request(read_json_file(path), source=str(path))
    return normalize_analysis_request(request, source="analysis request")


def require_known_variants(
    request: AnalysisRequest, known: Sequence[str], *, where: str
) -> None:
    """Every declared pairwise variant must exist in the source study's family."""
    available = set(known)
    unknown: list[str] = []
    for comparison in request.pairwise:
        for role, variant in (
            ("target_variant", comparison.target_variant),
            ("control_variant", comparison.control_variant),
        ):
            if variant not in available:
                unknown.append(f"{comparison.comparison_id}.{role}={variant!r}")
    if unknown:
        raise PatternLabDataError(
            f"{where}: pairwise comparisons reference variants the source study never saved: "
            + ", ".join(sorted(unknown))
            + f". Saved variants are {sorted(available)}."
        )


def require_known_models(
    request: AnalysisRequest, known: Mapping[str, Any], *, where: str
) -> None:
    """Every selected model instance must exist in the source study's family."""
    unknown = sorted(set(request.model_instances) - set(known))
    if unknown:
        raise PatternLabDataError(
            f"{where}: model instances {unknown} are not saved by the source study; saved "
            f"instances are {sorted(known)}."
        )
