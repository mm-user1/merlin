"""The numerical instrument job: RAM only, no pack I/O and no output writes.

The coordinator prepares immutable observation arrays and hands them to
:func:`run_instrument_job`.  The job opens no pack, acquires no lock and writes
no file, so the same top-level function is reusable under an explicit spawn
pool without a second numerical implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from . import contracts
from . import observations as study_observations
from .contracts import Anchors, BarSeries, FeatureValue, FeatureRequest

EVERY_QUALIFYING_BAR = "every_qualifying_bar"
STATE_ENTRY = "state_entry"


@dataclass(frozen=True)
class TimeframeInput:
    """One prepared observation timeframe of one instrument."""

    timeframe_minutes: int
    timestamps_ms: np.ndarray
    values: np.ndarray
    research_start_index: int
    input_fingerprint: str
    base_row_count: int
    base_gap_count: int
    omitted_group_count: int
    segment_count: int


@dataclass(frozen=True)
class InstrumentJobInput:
    """Everything one instrument job needs, as plain serializable values."""

    instrument_id: str
    symbol: str
    venue: str
    contract: str
    quote_currency: str
    roles: tuple[str, ...]
    study_start_ms: int
    study_end_ms: int
    warmup_start_ms: int
    timeframes: tuple[TimeframeInput, ...]
    variants: tuple[Mapping[str, Any], ...]
    models: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class InstrumentJobResult:
    """One instrument's evidence tables and observed counts."""

    instrument_id: str
    tables: Mapping[str, pd.DataFrame]
    stats: Mapping[str, Any]


def build_series(instrument_id: str, prepared: TimeframeInput) -> BarSeries:
    """Wrap one prepared timeframe as the aligned series the contracts use."""
    step_ms = prepared.timeframe_minutes * 60_000
    stamps = np.ascontiguousarray(prepared.timestamps_ms, dtype=np.int64)
    return BarSeries(
        instrument_id=instrument_id,
        timeframe_minutes=prepared.timeframe_minutes,
        step_ms=step_ms,
        timestamps_ms=stamps,
        slots=stamps // step_ms,
        values=np.ascontiguousarray(prepared.values, dtype=np.float64),
        research_start_index=int(prepared.research_start_index),
    )


def eligible_anchors(series: BarSeries, *, study_start_ms: int, study_end_ms: int) -> Anchors:
    """Complete bars whose open is >= the study start and whose close is < its end.

    The final bar closing exactly at the study end therefore emits no event,
    while an earlier anchor's outcome may end exactly at that boundary.
    """
    stamps = series.timestamps_ms
    mask = (stamps >= study_start_ms) & (stamps + series.step_ms < study_end_ms)
    rows = np.flatnonzero(mask).astype(np.int64)
    return Anchors(
        rows=rows,
        open_ms=stamps[rows],
        study_start_ms=int(study_start_ms),
        study_end_ms=int(study_end_ms),
    )


def _feature_value(
    series: BarSeries,
    request: FeatureRequest,
    cache: dict[str, FeatureValue],
    pending: tuple[str, ...] = (),
) -> FeatureValue:
    key = f"{series.instrument_id}|{series.timeframe_minutes}|{request.key}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    if request.feature_id in pending:
        raise PatternLabDataError(
            f"feature {request.feature_id!r}: circular dependency through {list(pending)}."
        )
    descriptor = contracts.feature(request.feature_id)
    contracts.require_scope(descriptor.scope, f"feature {request.feature_id}.scope")
    parameters = descriptor.validate_parameters(dict(request.parameters))
    dependencies: dict[str, FeatureValue] = {}
    for dependency in descriptor.dependencies(parameters):
        dependencies[dependency.key] = _feature_value(
            series, dependency, cache, pending + (request.feature_id,)
        )
    result = contracts.check_feature_result(
        descriptor.evaluate(series, parameters, dependencies),
        series,
        f"feature {request.feature_id}",
    )
    cache[key] = result
    return result


def _episodes(series: BarSeries, active: np.ndarray, valid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return the first and last row of every maximal contiguous true run."""
    rows = series.row_count
    if rows == 0 or not active.any():
        empty = np.zeros(0, dtype=np.int64)
        return empty, empty
    contiguous = series.contiguous_with_previous()
    starts = active.copy()
    if rows > 1:
        starts[1:] = active[1:] & ~(active[:-1] & contiguous[1:])
    ends = active.copy()
    if rows > 1:
        ends[:-1] = active[:-1] & ~(active[1:] & contiguous[1:])
    return np.flatnonzero(starts).astype(np.int64), np.flatnonzero(ends).astype(np.int64)


def _condition_tables(
    series: BarSeries,
    anchors: Anchors,
    condition_id: str,
    condition,
    variants: Sequence[Mapping[str, Any]],
    instrument_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build the conditions, episodes and emissions rows of one condition."""
    rows = series.row_count
    value = condition.value
    valid = condition.valid
    active = value & valid
    contiguous = series.contiguous_with_previous()

    episode_of_row = np.full(rows, -1, dtype=np.int64)
    starts, ends = _episodes(series, active, valid)
    for index, (first, last) in enumerate(zip(starts, ends)):
        episode_of_row[first : last + 1] = index

    anchor_rows = anchors.rows
    eligible = np.zeros(rows, dtype=bool)
    eligible[anchor_rows] = True

    episode_ids: list[str | None] = []
    for index in range(int(starts.size)):
        episode_ids.append(
            contracts.short_digest(
                {
                    "condition_id": condition_id,
                    "instrument_id": instrument_id,
                    "timeframe_minutes": series.timeframe_minutes,
                    "first_bar_open_ms": int(series.timestamps_ms[starts[index]]),
                }
            )
        )

    anchor_episode = episode_of_row[anchor_rows]
    conditions = pd.DataFrame(
        {
            "instrument_id": instrument_id,
            "timeframe_minutes": np.int64(series.timeframe_minutes),
            "condition_id": condition_id,
            "anchor_open_ms": series.timestamps_ms[anchor_rows],
            "signal_time_ms": series.timestamps_ms[anchor_rows] + series.step_ms,
            "value": value[anchor_rows],
            "valid": valid[anchor_rows],
            "episode_id": [
                episode_ids[item] if item >= 0 else None for item in anchor_episode
            ],
        }
    )

    emission_frames: list[pd.DataFrame] = []
    for variant in variants:
        policy = variant["occurrence"]
        if policy == EVERY_QUALIFYING_BAR:
            emitted = eligible & active
        elif policy == STATE_ENTRY:
            previous_known_false = np.zeros(rows, dtype=bool)
            if rows > 1:
                previous_known_false[1:] = contiguous[1:] & valid[:-1] & ~value[:-1]
            emitted = eligible & active & previous_known_false
        else:  # pragma: no cover - the request validator rejects other policies
            raise PatternLabDataError(f"unknown occurrence policy {policy!r}.")
        selected = np.flatnonzero(emitted).astype(np.int64)
        signal_times = series.timestamps_ms[selected] + series.step_ms
        event_ids = [
            contracts.short_digest(
                {
                    "condition_id": condition_id,
                    "occurrence": policy,
                    "instrument_id": instrument_id,
                    "timeframe_minutes": series.timeframe_minutes,
                    "signal_time_ms": int(stamp),
                }
            )
            for stamp in signal_times
        ]
        episode_links = episode_of_row[selected]
        emission_frames.append(
            pd.DataFrame(
                {
                    "instrument_id": instrument_id,
                    "timeframe_minutes": np.int64(series.timeframe_minutes),
                    "variant_id": variant["variant_id"],
                    "condition_id": condition_id,
                    "occurrence": policy,
                    "anchor_open_ms": series.timestamps_ms[selected],
                    "signal_time_ms": signal_times,
                    "event_id": event_ids,
                    "episode_id": [
                        episode_ids[item] if item >= 0 else None for item in episode_links
                    ],
                }
            )
        )

    keep = np.zeros(int(starts.size), dtype=bool)
    if starts.size:
        for index, (first, last) in enumerate(zip(starts, ends)):
            keep[index] = bool(eligible[first : last + 1].any())
    selected_episodes = np.flatnonzero(keep).astype(np.int64)
    left_censored = []
    right_censored = []
    anchor_counts = []
    for index in selected_episodes:
        first = int(starts[index])
        last = int(ends[index])
        unknown_before = first == 0 or not bool(contiguous[first]) or not bool(valid[first - 1])
        left_censored.append(
            bool(unknown_before or series.timestamps_ms[first] < anchors.study_start_ms)
        )
        after = last + 1
        right_censored.append(
            bool(after >= rows or not bool(contiguous[after]) or not bool(valid[after]))
        )
        anchor_counts.append(int(np.count_nonzero(eligible[first : last + 1])))
    episodes = pd.DataFrame(
        {
            "instrument_id": instrument_id,
            "timeframe_minutes": np.int64(series.timeframe_minutes),
            "condition_id": condition_id,
            "episode_id": [episode_ids[int(index)] for index in selected_episodes],
            "first_bar_open_ms": series.timestamps_ms[starts[selected_episodes]]
            if selected_episodes.size
            else np.zeros(0, dtype=np.int64),
            "last_bar_open_ms": series.timestamps_ms[ends[selected_episodes]]
            if selected_episodes.size
            else np.zeros(0, dtype=np.int64),
            "bar_count": (ends[selected_episodes] - starts[selected_episodes] + 1).astype(np.int64)
            if selected_episodes.size
            else np.zeros(0, dtype=np.int64),
            "eligible_anchor_count": np.asarray(anchor_counts, dtype=np.int64),
            "left_censored": np.asarray(left_censored, dtype=bool),
            "right_censored": np.asarray(right_censored, dtype=bool),
        }
    )
    emissions = (
        pd.concat(emission_frames, ignore_index=True) if emission_frames else _empty_emissions()
    )
    return conditions, episodes, emissions


def _empty_emissions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "instrument_id": pd.Series(dtype="object"),
            "timeframe_minutes": pd.Series(dtype="int64"),
            "variant_id": pd.Series(dtype="object"),
            "condition_id": pd.Series(dtype="object"),
            "occurrence": pd.Series(dtype="object"),
            "anchor_open_ms": pd.Series(dtype="int64"),
            "signal_time_ms": pd.Series(dtype="int64"),
            "event_id": pd.Series(dtype="object"),
            "episode_id": pd.Series(dtype="object"),
        }
    )


def _custom_frame(
    model_evidence, instance: Mapping[str, Any], series: BarSeries, anchors: Anchors
) -> pd.DataFrame:
    """Validate a custom model's returned rows, then wrap them in the envelope."""
    where = (
        f"model instance {instance['model_instance_id']!r} on {series.instrument_id} at "
        f"{series.timeframe_minutes}m"
    )
    rows = model_evidence.rows
    if not isinstance(rows, Mapping):
        raise PatternLabDataError(f"{where}: evidence rows must be a mapping of column arrays.")
    declared_cases = instance["cases"][str(series.timeframe_minutes)]
    outcomes = study_observations.outcome_union(declared_cases)
    study_observations.validate_custom_slice(
        dict(rows),
        declared_cases=declared_cases,
        required_outcomes=outcomes,
        expected_anchors=anchors.open_ms,
        where=where,
    )
    frame = pd.DataFrame(
        {
            "instrument_id": series.instrument_id,
            "timeframe_minutes": np.int64(series.timeframe_minutes),
            "model_instance_id": instance["model_instance_id"],
            "case_id": np.asarray(rows["case_id"], dtype=object),
            "anchor_open_ms": np.asarray(rows["anchor_open_ms"], dtype=np.int64),
        }
    )
    for name in outcomes:
        frame[name] = np.asarray(rows[name], dtype=np.float64)
        frame[f"{name}__reason"] = np.asarray(rows[f"{name}__reason"], dtype=object)
    return frame


def _custom_table(instance: Mapping[str, Any], frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate one instance's timeframes over its instance-wide outcome union.

    Timeframes whose cases declare fewer outcomes keep the remaining union
    columns explicitly null with a core-owned nonapplicable reason, so the saved
    table never contains a hole that is neither a measurement nor a stated
    absence.
    """
    outcomes = study_observations.instance_outcome_union(instance)
    columns = list(study_observations.CUSTOM_ENVELOPE) + study_observations.custom_columns(outcomes)[2:]
    filled: list[pd.DataFrame] = []
    for frame in frames:
        for name in outcomes:
            if name not in frame.columns:
                frame[name] = np.nan
                frame[f"{name}__reason"] = study_observations.REASON_NOT_APPLICABLE
        filled.append(frame.loc[:, columns])
    return pd.concat(filled, ignore_index=True)


def protect_inputs(payload: InstrumentJobInput) -> None:
    """Mark every prepared input array read-only, in the parent and in a child.

    Serialization does not preserve the flag, so the job restores it itself: a
    feature or model may allocate its own working arrays, but never mutates the
    shared bars it was given.
    """
    for prepared in payload.timeframes:
        for array in (prepared.timestamps_ms, prepared.values):
            if isinstance(array, np.ndarray) and array.flags.writeable:
                array.flags.writeable = False


def run_instrument_job(payload: InstrumentJobInput) -> InstrumentJobResult:
    """Evaluate one instrument's conditions, episodes, events and model evidence.

    The features of one condition identity are computed once per instrument and
    timeframe and reused across the variants and models that need them.
    """
    protect_inputs(payload)
    condition_frames: list[pd.DataFrame] = []
    episode_frames: list[pd.DataFrame] = []
    emission_frames: list[pd.DataFrame] = []
    primitive_frames: list[pd.DataFrame] = []
    custom_frames: dict[str, list[pd.DataFrame]] = {}
    stats: dict[str, Any] = {"timeframes": {}}

    for prepared in payload.timeframes:
        series = build_series(payload.instrument_id, prepared)
        anchors = eligible_anchors(
            series, study_start_ms=payload.study_start_ms, study_end_ms=payload.study_end_ms
        )
        feature_cache: dict[str, FeatureValue] = {}

        by_condition: dict[str, list[Mapping[str, Any]]] = {}
        for variant in payload.variants:
            by_condition.setdefault(variant["condition_id"], []).append(variant)

        condition_counts: dict[str, Any] = {}
        for condition_id, variants in by_condition.items():
            descriptor = contracts.hypothesis(variants[0]["hypothesis_id"])
            parameters = dict(variants[0]["parameters"])
            dependencies: dict[str, FeatureValue] = {}
            for dependency in descriptor.dependencies(parameters):
                dependencies[dependency.key] = _feature_value(series, dependency, feature_cache)
            condition = contracts.check_condition_result(
                descriptor.evaluate(series, parameters, dependencies),
                series,
                f"hypothesis {variants[0]['hypothesis_id']}",
            )
            conditions, episodes, emissions = _condition_tables(
                series, anchors, condition_id, condition, variants, payload.instrument_id
            )
            condition_frames.append(conditions)
            episode_frames.append(episodes)
            emission_frames.append(emissions)
            condition_counts[condition_id] = {
                "valid_anchors": int(np.count_nonzero(conditions["valid"].to_numpy())),
                "true_anchors": int(
                    np.count_nonzero(conditions["value"].to_numpy() & conditions["valid"].to_numpy())
                ),
                "episodes": int(len(episodes)),
                "events_by_variant": {
                    str(variant_id): int(count)
                    for variant_id, count in emissions.groupby("variant_id").size().items()
                },
            }

        for instance in payload.models:
            descriptor = contracts.model(instance["model_id"])
            settings = instance["settings"]
            resolved = descriptor.resolve_cases(settings, series.timeframe_minutes)
            declared = [case["case_id"] for case in instance["cases"][str(series.timeframe_minutes)]]
            if [case.case_id for case in resolved] != declared:
                raise PatternLabDataError(
                    f"model instance {instance['model_instance_id']!r}: the resolved case list "
                    "changed after the family was frozen; the run is stopped."
                )
            if descriptor.evidence_kind != instance["evidence_kind"]:
                raise PatternLabDataError(
                    f"model instance {instance['model_instance_id']!r}: the registered evidence "
                    f"kind is {descriptor.evidence_kind!r}, but the frozen family declares "
                    f"{instance['evidence_kind']!r}; the run is stopped."
                )
            model_evidence = descriptor.evaluate(series, settings, anchors)
            if not isinstance(model_evidence, contracts.ModelEvidence):
                raise PatternLabDataError(
                    f"model instance {instance['model_instance_id']!r}: evaluate must return a "
                    f"ModelEvidence, got {type(model_evidence).__name__}."
                )
            if model_evidence.kind != descriptor.evidence_kind:
                raise PatternLabDataError(
                    f"model instance {instance['model_instance_id']!r}: returned evidence kind "
                    f"{model_evidence.kind!r} does not match the registered "
                    f"{descriptor.evidence_kind!r}."
                )
            if model_evidence.kind == contracts.FIXED_HORIZON_EVIDENCE_KIND:
                frame = pd.DataFrame(dict(model_evidence.rows))
                frame.insert(0, "model_instance_id", instance["model_instance_id"])
                frame.insert(0, "timeframe_minutes", np.int64(series.timeframe_minutes))
                frame.insert(0, "instrument_id", payload.instrument_id)
                primitive_frames.append(frame)
            else:
                custom_frames.setdefault(instance["model_instance_id"], []).append(
                    _custom_frame(model_evidence, instance, series, anchors)
                )

        stats["timeframes"][str(prepared.timeframe_minutes)] = {
            "bar_count": series.row_count,
            "warmup_bar_count": int(series.research_start_index),
            "research_bar_count": int(series.row_count - series.research_start_index),
            "eligible_anchor_count": int(anchors.rows.size),
            "base_row_count": int(prepared.base_row_count),
            "base_gap_count": int(prepared.base_gap_count),
            "omitted_group_count": int(prepared.omitted_group_count),
            "segment_count": int(prepared.segment_count),
            "conditions": condition_counts,
        }

    tables: dict[str, pd.DataFrame] = {
        "conditions": pd.concat(condition_frames, ignore_index=True) if condition_frames else pd.DataFrame(),
        "episodes": pd.concat(episode_frames, ignore_index=True) if episode_frames else pd.DataFrame(),
        "emissions": pd.concat(emission_frames, ignore_index=True) if emission_frames else _empty_emissions(),
    }
    if primitive_frames:
        tables["primitives"] = pd.concat(primitive_frames, ignore_index=True)
    by_instance = {instance["model_instance_id"]: instance for instance in payload.models}
    for instance_id, frames in custom_frames.items():
        tables[f"custom__{instance_id}"] = _custom_table(by_instance[instance_id], frames)
    return InstrumentJobResult(instrument_id=payload.instrument_id, tables=tables, stats=stats)
