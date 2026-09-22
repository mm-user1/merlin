"""The resolved comparison family: the declared multiple-testing scope.

Every saved hypothesis variant receives an automatic nonsignal-baseline
comparison, then the explicitly declared pairwise comparisons are added.  Both
kinds are expanded across the selected model instances, observation timeframes
and resolved cases *before* any calculation, and the resulting order and IDs
depend only on the semantics — never on the request's list order, on filesystem
traversal or on worker order.

The family is fixed for one analysis execution.  That is a frozen scope for the
Holm correction inside this artifact; it is not proof of historical
preregistration, and every T05 analysis is an exploratory development analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .. import PatternLabDataError
from ..study import contracts
from .request import BASELINE_PREFIX, AnalysisRequest, method_settings

BASELINE_KIND = "nonsignal_baseline"
PAIRWISE_KIND = "pairwise"


@dataclass(frozen=True)
class Comparison:
    """One resolved comparison: which anchors are targets and which controls."""

    comparison_id: str
    kind: str
    target_variant: str
    # ``None`` for a nonsignal baseline, whose control population is that
    # variant's own known-valid false-condition anchors.
    control_variant: str | None
    label: str

    def as_json(self) -> dict[str, Any]:
        return {
            "comparison_id": self.comparison_id,
            "kind": self.kind,
            "target_variant": self.target_variant,
            "control_variant": self.control_variant,
            "label": self.label,
        }


@dataclass(frozen=True)
class FamilyMember:
    """One resolved comparison/model/timeframe/case lift test."""

    member_id: str
    comparison_id: str
    kind: str
    target_variant: str
    control_variant: str | None
    model_instance_id: str
    timeframe_minutes: int
    case_id: str
    direction: str
    horizon_minutes: int
    commission_pct_per_side: float
    primary: bool
    label: str

    def as_json(self) -> dict[str, Any]:
        return {
            "member_id": self.member_id,
            "comparison_id": self.comparison_id,
            "kind": self.kind,
            "target_variant": self.target_variant,
            "control_variant": self.control_variant,
            "model_instance_id": self.model_instance_id,
            "timeframe_minutes": self.timeframe_minutes,
            "case_id": self.case_id,
            "direction": self.direction,
            "horizon_minutes": self.horizon_minutes,
            "commission_pct_per_side": self.commission_pct_per_side,
            "primary": self.primary,
            "label": self.label,
        }

    @property
    def horizon_group(self) -> tuple[str, str, int, str]:
        """The key whose members differ only by horizon, used for fingerprints."""
        return (self.comparison_id, self.model_instance_id, self.timeframe_minutes, self.direction)


def baseline_comparison_id(variant_id: str) -> str:
    return f"{BASELINE_PREFIX}{variant_id}"


def _variant_label(variant: Mapping[str, Any]) -> str:
    return f"{variant['variant_id']} ({variant['hypothesis_id']}, {variant['occurrence']})"


def resolve_comparisons(
    request: AnalysisRequest, variants: Sequence[Mapping[str, Any]]
) -> tuple[Comparison, ...]:
    """Generate every baseline comparison, then the declared pairwise ones.

    Generated IDs are ``baseline__<source_variant_id>``; complete names are
    recorded and never truncated to create a collision.  The ``baseline__``
    prefix is reserved against user pairwise IDs by the request validator.
    """
    by_id = {item["variant_id"]: item for item in variants}
    comparisons: list[Comparison] = []
    for variant_id in sorted(by_id):
        variant = by_id[variant_id]
        comparisons.append(
            Comparison(
                comparison_id=baseline_comparison_id(variant_id),
                kind=BASELINE_KIND,
                target_variant=variant_id,
                control_variant=None,
                label=(
                    f"{_variant_label(variant)} versus its known-false-condition anchors "
                    "in the same instrument and UTC month"
                ),
            )
        )
    generated = {item.comparison_id for item in comparisons}
    for declaration in sorted(request.pairwise, key=lambda item: item.comparison_id):
        if declaration.comparison_id in generated:
            raise PatternLabDataError(
                f"pairwise: comparison ID {declaration.comparison_id!r} collides with a generated "
                "nonsignal-baseline comparison."
            )
        comparisons.append(
            Comparison(
                comparison_id=declaration.comparison_id,
                kind=PAIRWISE_KIND,
                target_variant=declaration.target_variant,
                control_variant=declaration.control_variant,
                label=(
                    f"{_variant_label(by_id[declaration.target_variant])} versus "
                    f"{_variant_label(by_id[declaration.control_variant])} on common availability"
                ),
            )
        )
    return tuple(comparisons)


def _case_sort_key(case: Mapping[str, Any]) -> tuple[int, str, str]:
    parameters = case["parameters"]
    return (int(parameters["horizon_minutes"]), str(parameters["direction"]), str(case["case_id"]))


def member_id(
    comparison_id: str, model_instance_id: str, timeframe_minutes: int, case_id: str
) -> str:
    """The stable semantic ID of one family member."""
    return f"{comparison_id}|{model_instance_id}|tf{int(timeframe_minutes)}m|{case_id}"


def resolve_family(
    request: AnalysisRequest,
    *,
    variants: Sequence[Mapping[str, Any]],
    instances: Mapping[str, Mapping[str, Any]],
    timeframes: Sequence[int],
) -> tuple[tuple[Comparison, ...], tuple[FamilyMember, ...]]:
    """Expand every comparison across selected models, timeframes and cases."""
    comparisons = resolve_comparisons(request, variants)
    members: list[FamilyMember] = []
    seen: set[str] = set()
    for comparison in comparisons:
        for instance_id in sorted(request.model_instances):
            instance = instances[instance_id]
            for timeframe in sorted(int(item) for item in timeframes):
                cases = sorted(instance["cases"][str(timeframe)], key=_case_sort_key)
                for case in cases:
                    parameters = case["parameters"]
                    identifier = member_id(
                        comparison.comparison_id, instance_id, timeframe, case["case_id"]
                    )
                    if identifier in seen:
                        raise PatternLabDataError(
                            f"analysis family: duplicate member identity {identifier!r}."
                        )
                    seen.add(identifier)
                    members.append(
                        FamilyMember(
                            member_id=identifier,
                            comparison_id=comparison.comparison_id,
                            kind=comparison.kind,
                            target_variant=comparison.target_variant,
                            control_variant=comparison.control_variant,
                            model_instance_id=instance_id,
                            timeframe_minutes=timeframe,
                            case_id=case["case_id"],
                            direction=str(parameters["direction"]),
                            horizon_minutes=int(parameters["horizon_minutes"]),
                            commission_pct_per_side=float(
                                parameters["commission_pct_per_side"]
                            ),
                            primary=bool(case["primary"]),
                            label=(
                                f"{comparison.label} · {instance_id} · {timeframe}m · "
                                f"{parameters['direction']} {parameters['horizon_minutes']}m"
                            ),
                        )
                    )
    if not members:
        raise PatternLabDataError(
            "analysis family: no comparison resolved. A source study with no saved variant or no "
            "resolved case cannot be analyzed."
        )
    return comparisons, tuple(members)


def family_document(
    *,
    comparisons: Sequence[Comparison],
    members: Sequence[FamilyMember],
    model_instances: Sequence[str],
    timeframes: Sequence[int],
    variants: Sequence[Mapping[str, Any]],
    request_version: int = 1,
) -> dict[str, Any]:
    """The frozen family document written before any outcome aggregation."""
    return {
        "schema_version": 1,
        "method": method_settings(request_version),
        "model_instances": list(model_instances),
        "timeframes_minutes": [int(item) for item in timeframes],
        "source_variants": [dict(item) for item in variants],
        "comparisons": [item.as_json() for item in comparisons],
        "members": [item.as_json() for item in members],
        "family_size": len(members),
        "declared_scope": (
            "One family: every resolved comparison/model/timeframe/case lift test. Both directions "
            "stay explicit members and unavailable members keep their place in the family size."
        ),
        "preregistration": (
            "The family is fixed for this analysis execution. That is not proof of historical "
            "preregistration: every T05 analysis, including a rerun, is an exploratory development "
            "analysis."
        ),
    }


def family_identity(document: Mapping[str, Any]) -> str:
    """The canonical digest of the resolved family and its method settings."""
    return contracts.semantic_digest(document)
