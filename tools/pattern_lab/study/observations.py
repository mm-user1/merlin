"""Custom evidence validation and the one shared observation expansion.

A custom model owns its outcomes, but not its envelope.  One structural and
value validator checks a model's returned evidence before publication and the
same saved table whenever it is decoded again for an observation view, a
summary, a declared metric or a report: file hashes prove bytes, not row
validity.  One expansion implementation turns verified raw tables into the
public per-case observation view, so the public API and the summary aggregation
cannot drift apart.

The common custom envelope is core-owned.  ``signal_time_ms`` is derived on read
from the anchor open and the observation timeframe, so valid evidence saved
before this rule existed gains the column without a migration, a pack read or a
fabricated fixed-horizon field.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from . import builtins as study_builtins
from . import contracts

# The envelope columns the core writes around every custom model's outcomes.
CUSTOM_ENVELOPE = ("instrument_id", "timeframe_minutes", "model_instance_id", "case_id",
                   "anchor_open_ms")
# The core's own reason for a union column that this row's case does not declare.
REASON_NOT_APPLICABLE = "outcome_not_declared_by_case"
REASON_AVAILABLE = study_builtins.REASON_AVAILABLE


def case_outcome_names(case: Mapping[str, Any]) -> list[str]:
    return [item["name"] for item in case["outcomes"]]


def outcome_union(cases: Iterable[Mapping[str, Any]]) -> list[str]:
    """Union of the declared outcome names, in first-declaration order."""
    names: list[str] = []
    for case in cases:
        for name in case_outcome_names(case):
            if name not in names:
                names.append(name)
    return names


def instance_outcome_union(instance: Mapping[str, Any]) -> list[str]:
    """Union of every outcome any case of this model instance declares."""
    cases = [case for timeframe in sorted(instance["cases"], key=int)
             for case in instance["cases"][timeframe]]
    return outcome_union(cases)


def custom_columns(outcomes: Sequence[str]) -> list[str]:
    columns = ["case_id", "anchor_open_ms"]
    for name in outcomes:
        columns.extend([name, f"{name}__reason"])
    return columns


def signal_time_ms(anchor_open_ms: np.ndarray, timeframe_minutes: int) -> np.ndarray:
    """The anchor's close: the instant the condition and the model are known."""
    return anchor_open_ms.astype(np.int64) + int(timeframe_minutes) * 60_000


# --------------------------------------------------------------------------
# value-level checks
# --------------------------------------------------------------------------

def _exact_int64(values: Any, where: str) -> np.ndarray:
    """Return exact int64 milliseconds; booleans and inexact types are rejected."""
    array = np.asarray(values)
    if array.dtype == np.bool_ or not np.issubdtype(array.dtype, np.integer):
        raise PatternLabDataError(
            f"{where}: expected exact integer UTC milliseconds, got dtype {array.dtype}. A boolean, "
            "a fractional timestamp or a string timestamp is not an integer timestamp."
        )
    converted = array.astype(np.int64)
    if array.size and not np.array_equal(converted.astype(array.dtype), array):
        raise PatternLabDataError(
            f"{where}: integer milliseconds do not survive exact int64 conversion; the timestamps "
            "would be silently coerced."
        )
    return converted


def _reason_text(values: Any, where: str) -> np.ndarray:
    array = np.asarray(values, dtype=object)
    for index, item in enumerate(array.tolist()):
        if not isinstance(item, str) or not item.strip():
            raise PatternLabDataError(
                f"{where}[{index}]: every outcome reason must be a nonblank string, got {item!r}."
            )
    return array


def _check_outcome(
    name: str, values: np.ndarray, reasons: np.ndarray, declared: np.ndarray, where: str
) -> None:
    """Enforce the float64 availability contract of one outcome column."""
    finite = np.isfinite(values)
    null = np.isnan(values)
    infinite = ~finite & ~null
    if infinite.any():
        raise PatternLabDataError(
            f"{where}: outcome {name!r} holds {int(np.count_nonzero(infinite))} non-finite "
            "value(s); an infinity is neither an available measurement nor an explicit null."
        )
    available = reasons == REASON_AVAILABLE
    bad = available & ~finite
    if bad.any():
        raise PatternLabDataError(
            f"{where}: outcome {name!r} reports {REASON_AVAILABLE!r} for "
            f"{int(np.count_nonzero(bad))} null value(s); an available outcome requires a finite "
            "value."
        )
    bad = ~available & finite
    if bad.any():
        raise PatternLabDataError(
            f"{where}: outcome {name!r} stores {int(np.count_nonzero(bad))} finite value(s) with a "
            "non-available reason; an unavailable outcome must be null."
        )
    bad = ~declared & available
    if bad.any():
        raise PatternLabDataError(
            f"{where}: outcome {name!r} is reported as available for "
            f"{int(np.count_nonzero(bad))} row(s) whose resolved case does not declare it; a "
            "nonapplicable union column stays null with an explicit nonavailable reason."
        )


def validate_custom_slice(
    columns: Mapping[str, Any],
    *,
    declared_cases: Sequence[Mapping[str, Any]],
    required_outcomes: Sequence[str],
    expected_anchors: np.ndarray,
    where: str,
) -> None:
    """Check one instrument/timeframe's custom rows against its resolved cases.

    Exactly one row must exist for every ``(resolved case, eligible anchor)``
    pair, including unavailable outcomes.  Duplicate, missing and unknown keys,
    lossy timestamps, infinities and incoherent value/reason pairs all fail; the
    sample is never silently deduplicated, filtered or inner-joined away.
    """
    required = custom_columns(required_outcomes)
    supplied = set(columns)
    missing = sorted(set(required) - supplied)
    extra = sorted(supplied - set(required))
    if missing or extra:
        raise PatternLabDataError(
            f"{where}: the custom evidence table is closed; missing columns {missing}, unexpected "
            f"columns {extra}."
        )

    anchors = _exact_int64(columns["anchor_open_ms"], f"{where}.anchor_open_ms")
    length = int(anchors.size)
    for name in required:
        array = np.asarray(columns[name])
        if array.shape != (length,):
            raise PatternLabDataError(
                f"{where}: column {name!r} has shape {array.shape}, expected ({length},)."
            )

    case_ids = np.asarray(columns["case_id"], dtype=object)
    for index, item in enumerate(case_ids.tolist()):
        if not isinstance(item, str) or not item.strip():
            raise PatternLabDataError(
                f"{where}.case_id[{index}]: expected a declared case ID, got {item!r}."
            )

    declared_ids = [case["case_id"] for case in declared_cases]
    expected = _exact_int64(expected_anchors, f"{where} eligible anchors")
    expected_pairs = {(case_id, int(anchor)) for case_id in declared_ids
                      for anchor in expected.tolist()}
    supplied_pairs: set[tuple[str, int]] = set()
    duplicates: list[tuple[str, int]] = []
    for case_id, anchor in zip(case_ids.tolist(), anchors.tolist()):
        key = (str(case_id), int(anchor))
        if key in supplied_pairs:
            duplicates.append(key)
        supplied_pairs.add(key)
    if duplicates:
        sample = sorted(set(duplicates))[:5]
        raise PatternLabDataError(
            f"{where}: {len(duplicates)} duplicate (case, anchor) row(s), for example {sample}; "
            "duplicates change the event count and the weighting."
        )
    unknown_cases = sorted({case_id for case_id, _anchor in supplied_pairs} - set(declared_ids))
    if unknown_cases:
        raise PatternLabDataError(
            f"{where}: rows reference undeclared case IDs {unknown_cases}; the resolved cases are "
            f"{declared_ids}."
        )
    eligible = set(expected.tolist())
    unknown_anchors = sorted({anchor for _case_id, anchor in supplied_pairs} - eligible)
    if unknown_anchors:
        raise PatternLabDataError(
            f"{where}: rows reference {len(unknown_anchors)} anchor(s) that are not eligible study "
            f"anchors, for example {unknown_anchors[:5]}."
        )
    absent = sorted(expected_pairs - supplied_pairs)
    if absent:
        raise PatternLabDataError(
            f"{where}: {len(absent)} (case, anchor) row(s) are missing, for example {absent[:5]}. "
            "Every eligible anchor needs one row per resolved case, including an explicitly "
            "unavailable outcome."
        )

    declares = {case["case_id"]: set(case_outcome_names(case)) for case in declared_cases}
    for name in required_outcomes:
        values = np.asarray(columns[name], dtype=np.float64)
        reasons = _reason_text(columns[f"{name}__reason"], f"{where}.{name}__reason")
        declared_mask = np.asarray(
            [name in declares[str(case_id)] for case_id in case_ids.tolist()], dtype=bool
        )
        _check_outcome(name, values, reasons, declared_mask, where)


def validate_custom_table(
    frame: pd.DataFrame,
    *,
    instance: Mapping[str, Any],
    instrument_id: str,
    expected_anchors: Mapping[int, np.ndarray],
    where: str,
) -> None:
    """Validate a complete saved custom table before any group filtering."""
    instance_id = instance["model_instance_id"]
    outcomes = instance_outcome_union(instance)
    expected_columns = list(CUSTOM_ENVELOPE) + custom_columns(outcomes)[2:]
    missing = sorted(set(expected_columns) - set(frame.columns))
    extra = sorted(set(frame.columns) - set(expected_columns))
    if missing or extra:
        raise PatternLabDataError(
            f"{where}: the saved custom evidence table is closed; missing columns {missing}, "
            f"unexpected columns {extra}."
        )
    if len(frame) and not (frame["model_instance_id"] == instance_id).all():
        raise PatternLabDataError(
            f"{where}: the table holds rows of another model instance than {instance_id!r}."
        )
    if len(frame) and not (frame["instrument_id"] == instrument_id).all():
        raise PatternLabDataError(
            f"{where}: the table holds rows of another instrument than {instrument_id!r}."
        )
    timeframes = sorted(int(key) for key in instance["cases"])
    stored = _exact_int64(frame["timeframe_minutes"].to_numpy(), f"{where}.timeframe_minutes")
    unexpected = sorted(set(int(item) for item in stored.tolist()) - set(timeframes))
    if unexpected:
        raise PatternLabDataError(
            f"{where}: rows reference unplanned observation timeframes {unexpected}."
        )
    for timeframe in timeframes:
        selection = frame.loc[stored == timeframe]
        columns = {name: selection[name].to_numpy() for name in custom_columns(outcomes)}
        validate_custom_slice(
            columns,
            declared_cases=instance["cases"][str(timeframe)],
            required_outcomes=outcomes,
            expected_anchors=expected_anchors.get(timeframe, np.zeros(0, dtype=np.int64)),
            where=f"{where} at {timeframe}m",
        )


# --------------------------------------------------------------------------
# the shared expansion
# --------------------------------------------------------------------------

def custom_case_view(
    frame: pd.DataFrame, *, case_id: str, timeframe_minutes: int
) -> pd.DataFrame:
    """One case's custom rows plus the core-derived signal timestamp."""
    timeframe = int(timeframe_minutes)
    selection = frame.loc[
        (frame["case_id"] == case_id) & (frame["timeframe_minutes"] == timeframe)
    ].reset_index(drop=True)
    anchors = selection["anchor_open_ms"].to_numpy(dtype=np.int64)
    selection.insert(
        selection.columns.get_loc("anchor_open_ms") + 1,
        "signal_time_ms",
        signal_time_ms(anchors, timeframe),
    )
    return selection


def expand_case(
    *,
    evidence_kind: str,
    table: pd.DataFrame,
    model_instance_id: str,
    case,
    timeframe_minutes: int,
) -> pd.DataFrame:
    """Return one resolved case's observation view for one instrument."""
    timeframe = int(timeframe_minutes)
    if evidence_kind == contracts.FIXED_HORIZON_EVIDENCE_KIND:
        selection = table.loc[
            (table["model_instance_id"] == model_instance_id)
            & (table["timeframe_minutes"] == timeframe)
        ].reset_index(drop=True)
        return study_builtins.expand_fixed_horizon_case(selection, case)
    return custom_case_view(table, case_id=case.case_id, timeframe_minutes=timeframe)


def join_events(
    frame: pd.DataFrame, emissions: pd.DataFrame, *, variant_id: str, timeframe_minutes: int
) -> pd.DataFrame:
    """Restrict an all-anchor case view to one variant's emitted events."""
    events = emissions.loc[
        (emissions["variant_id"] == variant_id)
        & (emissions["timeframe_minutes"] == int(timeframe_minutes)),
        ["anchor_open_ms", "event_id", "episode_id"],
    ]
    return frame.merge(events, on="anchor_open_ms", how="inner")
