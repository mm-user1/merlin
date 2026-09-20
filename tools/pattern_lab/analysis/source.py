"""Admission of a completed M2 study and the one checked evidence alignment.

Nothing here reruns a hypothesis, reopens the market pack or imports saved
source.  The completed run is loaded strictly through the existing reader, the
selected models are checked against the built-in fixed-horizon contract, and one
alignment stage joins the saved conditions and emissions to the reader's
**all-anchor** case view.  The existing M2 ``join_events`` inner merge is not
used: a missing or duplicated row must fail, not disappear into a merge.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from ..manifest import format_epoch_ms, to_epoch_ms
from ..study import builtins as study_builtins
from ..study import contracts, evidence
from ..study import results as study_results
from ..study import spec as study_spec
from .estimator import RECORD_COLUMNS, exact_int64
from .family import BASELINE_KIND, FamilyMember

SUPPORTED_MODEL_ID = study_builtins.FIXED_HORIZON_MODEL_ID
SUPPORTED_MODEL_VERSION = "1"
SUPPORTED_EVIDENCE_KIND = contracts.FIXED_HORIZON_EVIDENCE_KIND
SUPPORTED_EVIDENCE_VIEW_VERSION = study_builtins.EVIDENCE_VIEW_VERSION


@dataclass(frozen=True)
class AdmittedSource:
    """One strictly loaded, contract-checked completed source study."""

    run_root: Path
    results: study_results.StudyResults
    variants: tuple[Mapping[str, Any], ...]
    instances: Mapping[str, Mapping[str, Any]]
    timeframes: tuple[int, ...]
    instruments: tuple[str, ...]
    study_start_ms: int
    study_end_ms: int

    def binding_document(self) -> dict[str, Any]:
        """The exact source identities this analysis is bound to.

        Physical evidence hashes and semantic identities are recorded side by
        side: two source runs from ``workers=1`` and ``workers=2`` may differ
        physically while their semantic identities and numerical results agree.
        """
        completion = dict(self.results.completion or {})
        identities = dict(self.results.provenance["identities"])
        return {
            "schema_version": 1,
            "run_root": str(self.run_root),
            "study_name": self.results.request["study_name"],
            "study": dict(self.results.request["study"]),
            "protocol": dict(self.results.protocol),
            "physical": {
                "evidence_set_sha256": completion.get("evidence_set_sha256"),
                "evidence_sha256": dict(completion.get("evidence_sha256", {})),
            },
            "semantic": {
                "specification_sha256": identities.get("specification_sha256"),
                "data_input_sha256": identities.get("data_input_sha256"),
                "implementation_sha256": identities.get("implementation_sha256"),
                "evidence_view_version": int(self.results.source["evidence_view_version"]),
            },
            "semantic_inputs": self.semantic_inputs(),
            "counts": dict(self.results.counts),
            "instruments": list(self.instruments),
            "timeframes_minutes": list(self.timeframes),
            "note": (
                "Recorded paths are provenance, not integrity proofs. A relocated source study "
                "stays readable and keeps the same semantic identity."
            ),
        }

    def semantic_inputs(self) -> dict[str, Any]:
        """The source facts that enter this analysis's own semantic identity."""
        identities = dict(self.results.provenance["identities"])
        return {
            "specification_sha256": identities.get("specification_sha256"),
            "data_input_sha256": identities.get("data_input_sha256"),
            "evidence_view_version": int(self.results.source["evidence_view_version"]),
            "study": dict(self.results.request["study"]),
            "protocol": study_spec.protocol_document(self.results.protocol),
            "instruments": list(self.instruments),
            "timeframes_minutes": list(self.timeframes),
        }


def _require_source_path(run_root: Any) -> Path:
    if not isinstance(run_root, (str, Path)):
        raise PatternLabDataError(
            f"run_root: expected a path to a completed study run directory, got "
            f"{type(run_root).__name__}. A caller-supplied results object does not bypass strict "
            "admission."
        )
    return evidence.require_run_directory(run_root)


def _check_model_instance(instance: Mapping[str, Any], timeframes: Sequence[int]) -> None:
    """Admit only the built-in fixed-horizon model, by its whole contract.

    The evidence kind alone is not enough: an extension may legally declare the
    same kind.  The model ID, version and kind must match the built-in triple,
    and the saved settings and resolved cases must agree with the built-in
    validator and resolver.  No source extension is loaded to decide this.
    """
    instance_id = instance["model_instance_id"]
    triple = (
        instance.get("model_id"),
        str(instance.get("model_version")),
        instance.get("evidence_kind"),
    )
    expected = (SUPPORTED_MODEL_ID, SUPPORTED_MODEL_VERSION, SUPPORTED_EVIDENCE_KIND)
    if triple != expected:
        raise PatternLabDataError(
            f"model instance {instance_id!r}: this analysis supports only the built-in "
            f"{expected} contract, and this instance declares {triple}. A custom model's return "
            "and holding semantics are never guessed from its column names."
        )
    settings = study_builtins.validate_fixed_horizon_settings(
        contracts.require_mapping(instance["settings"], f"model instance {instance_id!r}.settings"),
        list(timeframes),
    )
    if settings != dict(instance["settings"]):
        raise PatternLabDataError(
            f"model instance {instance_id!r}: the saved settings do not match the built-in "
            "validator's normalization of the same document."
        )
    for timeframe in timeframes:
        resolved = [
            case.as_json()
            for case in study_builtins.resolve_fixed_horizon_cases(settings, timeframe)
        ]
        saved = [dict(case) for case in instance["cases"][str(int(timeframe))]]
        if resolved != saved:
            raise PatternLabDataError(
                f"model instance {instance_id!r}: the saved {timeframe}m cases do not match the "
                "built-in resolver's cases for the same settings."
            )


def admit_source(
    run_root: Any, *, model_instances: Sequence[str], where: str = "analysis admission"
) -> AdmittedSource:
    """Strictly load and contract-check a completed source study.

    A partial run, a missing or invalid completion record, inconsistent counts or
    identities and corrupt evidence are all errors here, before any output
    directory exists.
    """
    root = _require_source_path(run_root)
    results = study_results.load_results(root)
    view_version = results.source.get("evidence_view_version")
    if view_version != SUPPORTED_EVIDENCE_VIEW_VERSION:
        raise PatternLabDataError(
            f"{where}: the source study records evidence view version {view_version!r}; this "
            f"analysis reads version {SUPPORTED_EVIDENCE_VIEW_VERSION}."
        )
    family = results.family
    instances = {item["model_instance_id"]: dict(item) for item in family["models"]}
    unknown = sorted(set(model_instances) - set(instances))
    if unknown:
        raise PatternLabDataError(
            f"{where}: model instances {unknown} are not saved by this study; saved instances are "
            f"{sorted(instances)}."
        )
    timeframes = tuple(int(item) for item in family["timeframes_minutes"])
    for instance_id in model_instances:
        _check_model_instance(instances[instance_id], timeframes)

    # The saved study bounds are rechecked against the study's own saved
    # development protocol; T05 introduces no reserved-period bypass.
    protocol = study_spec.normalize_protocol(
        study_spec.protocol_document(results.protocol), source=f"{where} source protocol"
    )
    study = results.request["study"]
    start_ms = to_epoch_ms(study["start_utc"], f"{where}.study.start_utc")
    end_ms = to_epoch_ms(study["end_utc"], f"{where}.study.end_utc")
    warmup_ms = to_epoch_ms(study["warmup_start_utc"], f"{where}.study.warmup_start_utc")
    study_spec.validate_against_protocol(
        protocol, study_start_ms=start_ms, study_end_ms=end_ms, warmup_start_ms=warmup_ms
    )

    instruments = tuple(results.completed_instruments)
    planned = [item["instrument_id"] for item in family["instruments"]]
    if sorted(instruments) != sorted(planned):
        raise PatternLabDataError(
            f"{where}: the completed instruments {sorted(instruments)} do not match the frozen "
            f"planned selection {sorted(planned)}."
        )
    return AdmittedSource(
        run_root=root,
        results=results,
        variants=tuple(dict(item) for item in family["variants"]),
        instances=instances,
        timeframes=timeframes,
        instruments=instruments,
        study_start_ms=start_ms,
        study_end_ms=end_ms,
    )


def reverify_source(source: AdmittedSource, *, where: str) -> None:
    """Verify the admitted source binding again before final publication.

    Cooperative immutable input is assumed; this is not an adversarial
    filesystem security model.  If the source's own immutable evidence changed
    while it was being read, the analysis fails without a completion seal.
    """
    fresh = study_results.load_results(source.run_root)
    expected = dict(source.results.completion or {}).get("evidence_set_sha256")
    actual = dict(fresh.completion or {}).get("evidence_set_sha256")
    if expected != actual:
        raise PatternLabDataError(
            f"{where}: the source study's evidence set changed during this analysis "
            f"({expected} -> {actual}). No completion seal is written.",
            error_code="corrupt_evidence",
        )


# --------------------------------------------------------------------------
# the checked alignment
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class _TimeframeMasks:
    """One instrument/timeframe's canonical anchors, conditions and emissions."""

    anchors: np.ndarray
    signal_time_ms: np.ndarray
    condition_value: Mapping[str, np.ndarray]
    condition_valid: Mapping[str, np.ndarray]
    emitted: Mapping[str, np.ndarray]


def _aligned_masks(
    reader: study_results.InstrumentReader,
    *,
    timeframe: int,
    condition_ids: Sequence[str],
    variants: Mapping[str, Mapping[str, Any]],
    where: str,
) -> _TimeframeMasks:
    """Align the saved conditions and emissions to the canonical anchor order."""
    conditions = reader.table("conditions")
    emissions = reader.table("emissions")
    tf_conditions = conditions.loc[
        exact_int64(conditions["timeframe_minutes"].to_numpy(), f"{where}.timeframe_minutes")
        == timeframe
    ]
    if tf_conditions.empty:
        raise PatternLabDataError(
            f"{where}: the source study saved no condition row at {timeframe}m."
        )
    value: dict[str, np.ndarray] = {}
    valid: dict[str, np.ndarray] = {}
    canonical: np.ndarray | None = None
    for condition_id in condition_ids:
        rows = tf_conditions.loc[tf_conditions["condition_id"] == condition_id]
        stamps = exact_int64(
            rows["anchor_open_ms"].to_numpy(), f"{where}.conditions[{condition_id}].anchor_open_ms"
        )
        order = np.argsort(stamps, kind="stable")
        stamps = stamps[order]
        if stamps.size == 0:
            raise PatternLabDataError(
                f"{where}: condition {condition_id!r} has no saved anchor at {timeframe}m."
            )
        if np.any(np.diff(stamps) == 0):
            raise PatternLabDataError(
                f"{where}: condition {condition_id!r} has duplicate anchors at {timeframe}m; a "
                "duplicate join key changes the sample and is never deduplicated silently."
            )
        signal = exact_int64(
            rows["signal_time_ms"].to_numpy(), f"{where}.conditions[{condition_id}].signal_time_ms"
        )[order]
        expected_signal = stamps + timeframe * 60_000
        if not np.array_equal(signal, expected_signal):
            raise PatternLabDataError(
                f"{where}: condition {condition_id!r} saves a signal time that is not its anchor's "
                f"close at {timeframe}m."
            )
        if canonical is None:
            canonical = stamps
        elif not np.array_equal(canonical, stamps):
            raise PatternLabDataError(
                f"{where}: condition {condition_id!r} covers a different anchor set at "
                f"{timeframe}m than the other saved conditions of the same instrument."
            )
        value[condition_id] = rows["value"].to_numpy(dtype=bool)[order]
        valid[condition_id] = rows["valid"].to_numpy(dtype=bool)[order]
    assert canonical is not None

    tf_emissions = emissions.loc[
        exact_int64(emissions["timeframe_minutes"].to_numpy(), f"{where}.emissions.timeframe_minutes")
        == timeframe
    ] if len(emissions) else emissions
    emitted: dict[str, np.ndarray] = {}
    for variant_id, variant in variants.items():
        rows = tf_emissions.loc[tf_emissions["variant_id"] == variant_id] if len(tf_emissions) else tf_emissions
        stamps = (
            exact_int64(
                rows["anchor_open_ms"].to_numpy(), f"{where}.emissions[{variant_id}].anchor_open_ms"
            )
            if len(rows)
            else np.zeros(0, dtype=np.int64)
        )
        unique = np.unique(stamps)
        if unique.size != stamps.size:
            raise PatternLabDataError(
                f"{where}: variant {variant_id!r} saves duplicate emissions at {timeframe}m."
            )
        position = np.searchsorted(canonical, unique)
        position = np.clip(position, 0, max(canonical.size - 1, 0))
        known = canonical[position] == unique if unique.size else np.zeros(0, dtype=bool)
        if unique.size and not known.all():
            raise PatternLabDataError(
                f"{where}: variant {variant_id!r} emits {int(np.count_nonzero(~known))} event(s) "
                f"at {timeframe}m whose anchor is not a saved eligible anchor."
            )
        mask = np.zeros(canonical.size, dtype=bool)
        mask[position] = True
        condition_id = variant["condition_id"]
        supported = value[condition_id] & valid[condition_id]
        if np.any(mask & ~supported):
            raise PatternLabDataError(
                f"{where}: variant {variant_id!r} emits {int(np.count_nonzero(mask & ~supported))} "
                f"event(s) at {timeframe}m whose saved condition is not a known-valid true "
                "condition."
            )
        if len(rows):
            signal = exact_int64(
                rows["signal_time_ms"].to_numpy(),
                f"{where}.emissions[{variant_id}].signal_time_ms",
            )
            # Row-wise: each emission's signal time must be its *own* anchor's
            # close. Sorting the two columns independently would accept a
            # permutation of signal times across different anchors, while the
            # element-wise comparison stays independent of the saved row order.
            if not np.array_equal(signal, stamps + timeframe * 60_000):
                raise PatternLabDataError(
                    f"{where}: variant {variant_id!r} saves an emission signal time that is not "
                    f"its anchor's close at {timeframe}m."
                )
        emitted[variant_id] = mask
    return _TimeframeMasks(
        anchors=canonical,
        signal_time_ms=canonical + timeframe * 60_000,
        condition_value=value,
        condition_valid=valid,
        emitted=emitted,
    )


def _case_outcomes(
    reader: study_results.InstrumentReader,
    *,
    instance: Mapping[str, Any],
    case,
    timeframe: int,
    anchors: np.ndarray,
    where: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """One case's all-anchor net/gross returns and validity, in anchor order."""
    frame = reader.observations(
        instance=instance, case=case, timeframe_minutes=timeframe, variant_id=None
    )
    stamps = exact_int64(frame["anchor_open_ms"].to_numpy(), f"{where}.anchor_open_ms")
    order = np.argsort(stamps, kind="stable")
    stamps = stamps[order]
    if stamps.size != anchors.size or not np.array_equal(stamps, anchors):
        raise PatternLabDataError(
            f"{where}: the case observation view covers {stamps.size} anchor(s) at {timeframe}m, "
            f"but the saved conditions cover {anchors.size}. The two must be a checked one-to-one "
            "population, never an inner join."
        )
    net = frame["net_return"].to_numpy(dtype=np.float64)[order]
    gross = frame["gross_return"].to_numpy(dtype=np.float64)[order]
    valid = frame["return_valid"].to_numpy(dtype=bool)[order]
    net = np.where(valid, net, np.nan)
    gross = np.where(valid, gross, np.nan)
    return net, gross, valid


class RecordSource:
    """The one checked alignment stage, and the diagnostics it observes.

    Each raw table is decoded once per instrument and reused across every case
    and comparison, then released.  Only anchors that are a target or a control
    of the comparison are materialized: an anchor that is neither contributes to
    no statistic, while the eligible-anchor counts stay recorded here rather than
    carried as inert rows.
    """

    def __init__(self, source: AdmittedSource, members: Sequence[FamilyMember]) -> None:
        self._source = source
        self._members = list(members)
        self.eligible_anchors: dict[str, int] = {
            str(item): 0 for item in source.timeframes
        }
        self.decoded_tables = 0

    def frames(self) -> Iterator[pd.DataFrame]:
        """Yield one aligned observation frame per instrument and family member."""
        source = self._source
        variants = {item["variant_id"]: item for item in source.variants}
        condition_ids = sorted({item["condition_id"] for item in source.variants})
        by_timeframe: dict[int, list[FamilyMember]] = {}
        for member in self._members:
            by_timeframe.setdefault(member.timeframe_minutes, []).append(member)

        for instrument_id in source.instruments:
            reader = source.results.instrument_reader(instrument_id)
            try:
                for timeframe in sorted(by_timeframe):
                    where = f"{instrument_id} at {timeframe}m"
                    masks = _aligned_masks(
                        reader,
                        timeframe=timeframe,
                        condition_ids=condition_ids,
                        variants=variants,
                        where=where,
                    )
                    key = str(int(timeframe))
                    self.eligible_anchors[key] = self.eligible_anchors.get(key, 0) + int(
                        masks.anchors.size
                    )
                    cache: dict[tuple[str, str], tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
                    for member in by_timeframe[timeframe]:
                        case_key = (member.model_instance_id, member.case_id)
                        outcomes = cache.get(case_key)
                        if outcomes is None:
                            instance = source.instances[member.model_instance_id]
                            case = source.results.case(
                                member.model_instance_id, timeframe, member.case_id
                            )
                            outcomes = _case_outcomes(
                                reader,
                                instance=instance,
                                case=case,
                                timeframe=timeframe,
                                anchors=masks.anchors,
                                where=f"{where} case {member.case_id}",
                            )
                            cache[case_key] = outcomes
                        frame = _member_frame(member, masks, outcomes, instrument_id, variants)
                        if len(frame):
                            yield frame
                    del cache
            finally:
                self.decoded_tables += len(getattr(reader, "_tables", {}))
                reader.release()


def _member_frame(
    member: FamilyMember,
    masks: _TimeframeMasks,
    outcomes: tuple[np.ndarray, np.ndarray, np.ndarray],
    instrument_id: str,
    variants: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    """Build one member's target/control membership and availability rows."""
    net, gross, return_valid = outcomes
    target_condition = variants[member.target_variant]["condition_id"]
    if member.kind == BASELINE_KIND:
        # Target: this variant's emitted events. Control: anchors whose condition
        # is known valid and false. A known-true nonemitting state_entry anchor is
        # in neither group, and unknown history is never a false control.
        available = masks.condition_valid[target_condition]
        is_target = masks.emitted[member.target_variant]
        is_control = available & ~masks.condition_value[target_condition]
    else:
        control_condition = variants[member.control_variant]["condition_id"]
        # Both conditions must be known valid at the anchor; on that common
        # availability the groups are the two variants' own saved emissions, with
        # their own occurrence rules. Overlap is allowed and counted.
        available = (
            masks.condition_valid[target_condition] & masks.condition_valid[control_condition]
        )
        is_target = masks.emitted[member.target_variant]
        is_control = masks.emitted[member.control_variant]
    selected = is_target | is_control
    if not selected.any():
        return pd.DataFrame({name: [] for name in RECORD_COLUMNS})
    return pd.DataFrame(
        {
            "member_id": member.member_id,
            "instrument_id": instrument_id,
            "signal_time_ms": masks.signal_time_ms[selected],
            "is_target": is_target[selected],
            "is_control": is_control[selected],
            "available": available[selected],
            "net_return": net[selected],
            "gross_return": gross[selected],
            "return_valid": return_valid[selected],
        },
        columns=list(RECORD_COLUMNS),
    )


def source_family_counts(source: AdmittedSource) -> dict[str, int]:
    """The original source study's own frozen family counts."""
    family = source.results.family
    return {
        "planned_job_count": int(family["planned_job_count"]),
        "planned_group_count": int(family["planned_group_count"]),
        "planned_family_size": int(family["planned_family_size"]),
        "variants": len(family["variants"]),
        "models": len(family["models"]),
    }


def describe_interval(source: AdmittedSource) -> str:
    return (
        f"[{format_epoch_ms(source.study_start_ms)}, {format_epoch_ms(source.study_end_ms)})"
    )
