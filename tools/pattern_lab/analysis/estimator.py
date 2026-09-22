"""The one numerical estimator: matched comparisons and calibrated inference.

:func:`evaluate_observations` is the pure numerical entry point.  It accepts
aligned per-observation records and the frozen family, and runs the production
calendar stratification, support exclusions, event weighting, daily aggregation,
joint influence contributions, inference gates, calendar block bootstrap and
Holm correction.  It publishes no artifact and certifies no provenance, so a
large calibration repetition enters here rather than at precomputed means,
selected strata, daily sums or influence vectors.

The inference is an **approximate development screen**.  The calendar block
bootstrap is an asymptotic approximation that needs weak dependence, adequate
moments and support, and a reasonably stable centered influence process; seven
days is a declared research setting, not a guarantee against arbitrary long
memory or regime change.  Holm cannot repair an invalid individual p-value.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from ..study import contracts
from . import request as analysis_request
from .monthly import monthly_jackknife, _unavailable_candidate
from .request import V2_METHOD_ID as CANDIDATE_METHOD_ID, CONFIDENCE_LEVEL
CANDIDATE_SCHEMA_VERSION = 1
from .request import (
    ALPHA,
    BLOCK_LENGTH_DAYS,
    INFERENCE_SCOPE,
    MAX_INFERENCE_HORIZON_MINUTES,
    MIN_DAY_GRID,
    MIN_JOINT_ACTIVE_DAYS,
    MIN_RETAINED_TARGET_SHARE,
    MIN_STRATUM_CONTROL,
    MIN_STRATUM_CONTROL_DAYS,
    MIN_STRATUM_TARGET,
    MIN_STRATUM_TARGET_DAYS,
    MIN_SUPPORTED_BLOCKS,
    MIN_SUPPORTED_BLOCK_ACTIVE_DAYS,
    MIN_SUPPORTED_SPAN_DAYS,
)

ESTIMATOR_SCHEMA_VERSION = 1

DAY_MS = 86_400_000
MAX_DAYS_IN_MONTH = 31

# The record columns the numerical boundary accepts.  The set is closed: an
# unexpected column is a contract error, not silently ignored context.
RECORD_COLUMNS = (
    "member_id",
    "instrument_id",
    "signal_time_ms",
    "is_target",
    "is_control",
    "available",
    "net_return",
    "gross_return",
    "return_valid",
)

from .request import (
    REASON_NO_SUPPORT, REASON_SPAN, REASON_ACTIVE_DAYS, REASON_BLOCKS, REASON_COVERAGE, REASON_HORIZON, REASON_DEGENERATE, REASON_ORDER
)

STRATUM_REASON_TARGET_COUNT = "target_count_below_minimum"
STRATUM_REASON_CONTROL_COUNT = "control_count_below_minimum"
STRATUM_REASON_TARGET_DAYS = "target_days_below_minimum"
STRATUM_REASON_CONTROL_DAYS = "control_days_below_minimum"

# Replicates are processed in bounded batches; the draw order stays
# replicate-major, so batching never changes the drawn index sequence.
BOOTSTRAP_BATCH = 512

# A joint influence vector sums to zero up to floating-point error.  The residual
# is judged against the vector's *pre-cancellation* mass, because a vector whose
# terms cancel exactly is legitimately pure roundoff while a structurally wrong
# vector leaves a residual comparable to the sums that produced it.
ZERO_SUM_TOLERANCE = 1e-9

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# the calendar grid
# --------------------------------------------------------------------------

class CalendarGrid:
    """All UTC days intersecting the half-open source study interval.

    Day ``d`` belongs to the grid exactly when ``[d, d+1 day)`` intersects
    ``[start, end)``, so ``[2025-07-01, 2026-07-01)`` contains 365 days, not 366.
    Partial boundary days stay explicit, and a day with no eligible observation
    contributes zero for aggregation only: zero padding never fabricates a valid
    market bar, a zero-return observation or supported inference history.
    """

    def __init__(self, study_start_ms: int, study_end_ms: int) -> None:
        if study_end_ms <= study_start_ms:
            raise PatternLabDataError(
                f"study interval: requires start < end, got {study_start_ms} >= {study_end_ms}."
            )
        self.study_start_ms = int(study_start_ms)
        self.study_end_ms = int(study_end_ms)
        self.first_day = self.study_start_ms // DAY_MS
        self.last_day = (self.study_end_ms - 1) // DAY_MS
        self.days = int(self.last_day - self.first_day + 1)

        keys: list[tuple[int, int]] = []
        month_of_day = np.zeros(self.days, dtype=np.int64)
        index_by_key: dict[tuple[int, int], int] = {}
        for offset in range(self.days):
            moment = _EPOCH + timedelta(days=int(self.first_day + offset))
            key = (moment.year, moment.month)
            position = index_by_key.get(key)
            if position is None:
                position = len(keys)
                index_by_key[key] = position
                keys.append(key)
            month_of_day[offset] = position
        self.month_keys = tuple(keys)
        self.month_labels = tuple(f"{year:04d}-{month:02d}" for year, month in keys)
        self.month_of_day = month_of_day
        self.month_count = len(keys)
        self.month_first_offset = np.zeros(self.month_count, dtype=np.int64)
        self.month_length = np.zeros(self.month_count, dtype=np.int64)
        for position in range(self.month_count):
            offsets = np.flatnonzero(month_of_day == position)
            self.month_first_offset[position] = int(offsets[0])
            self.month_length[position] = int(offsets.size)
        # (month, day-in-month) -> grid day offset, or -1 outside the month.
        table = np.full((self.month_count, MAX_DAYS_IN_MONTH), -1, dtype=np.int64)
        for position in range(self.month_count):
            length = int(self.month_length[position])
            table[position, :length] = self.month_first_offset[position] + np.arange(length)
        self.day_index_table = table

    def day_offsets(self, signal_time_ms: np.ndarray) -> np.ndarray:
        return signal_time_ms // DAY_MS - self.first_day

    def month_index_of(self, signal_time_ms: np.ndarray) -> np.ndarray:
        return self.month_of_day[self.day_offsets(signal_time_ms)]

    def day_utc(self, offset: int) -> str:
        moment = _EPOCH + timedelta(days=int(self.first_day + int(offset)))
        return moment.strftime("%Y-%m-%dT%H:%M:%SZ")

    def as_json(self) -> dict[str, Any]:
        return {
            "day_grid_days": self.days,
            "first_day_utc": self.day_utc(0),
            "last_day_utc": self.day_utc(self.days - 1),
            "months": list(self.month_labels),
            "rule": (
                "Day d is included iff [d, d+1 day) intersects the half-open study interval. "
                "A day with no eligible observation contributes count and sum zero for "
                "aggregation only."
            ),
        }


# --------------------------------------------------------------------------
# record validation
# --------------------------------------------------------------------------

def exact_int64(values: Any, where: str) -> np.ndarray:
    """Return exact int64 values; booleans and inexact types are rejected.

    Saved timeframe and timestamp columns may legitimately be int32, which
    converts losslessly; a boolean, a float or a string never does.
    """
    array = np.asarray(values)
    if array.dtype == np.bool_ or not np.issubdtype(array.dtype, np.integer):
        raise PatternLabDataError(
            f"{where}: expected exact integer values, got dtype {array.dtype}. A boolean, a "
            "fractional or a string value is not an integer key."
        )
    converted = array.astype(np.int64)
    if array.size and not np.array_equal(converted.astype(array.dtype), array):
        raise PatternLabDataError(
            f"{where}: integer keys do not survive exact int64 conversion; they would be silently "
            "coerced."
        )
    return converted


def _boolean(values: Any, where: str, length: int) -> np.ndarray:
    array = np.asarray(values)
    if array.dtype != np.bool_:
        raise PatternLabDataError(
            f"{where}: expected a boolean mask, got dtype {array.dtype}."
        )
    if array.shape != (length,):
        raise PatternLabDataError(
            f"{where}: expected an aligned ({length},) mask, got {array.shape}."
        )
    return array


def _float(values: Any, where: str, length: int) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    if array.shape != (length,):
        raise PatternLabDataError(
            f"{where}: expected an aligned ({length},) array, got {array.shape}."
        )
    return array


def _check_returns(
    name: str, values: np.ndarray, valid: np.ndarray, where: str
) -> None:
    finite = np.isfinite(values)
    bad = valid & ~finite
    if bad.any():
        raise PatternLabDataError(
            f"{where}: {name} holds {int(np.count_nonzero(bad))} non-finite value(s) marked valid; "
            "a valid return must be finite."
        )
    bad = ~valid & finite
    if bad.any():
        raise PatternLabDataError(
            f"{where}: {name} holds {int(np.count_nonzero(bad))} finite value(s) marked invalid. "
            "Invalid rows stay null and are counted as lost support; they are never removed "
            "silently."
        )
    bad = ~valid & ~finite & ~np.isnan(values)
    if bad.any():
        raise PatternLabDataError(
            f"{where}: {name} holds {int(np.count_nonzero(bad))} infinity value(s); an invalid "
            "return is null, never an infinity."
        )


# --------------------------------------------------------------------------
# accumulation
# --------------------------------------------------------------------------

def _splitmix64(values: np.ndarray) -> np.ndarray:
    """A small deterministic 64-bit mix used only for population fingerprints."""
    state = values.astype(np.uint64, copy=True)
    state += np.uint64(0x9E3779B97F4A7C15)
    mixed = state
    mixed = (mixed ^ (mixed >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    mixed = (mixed ^ (mixed >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return mixed ^ (mixed >> np.uint64(31))


class _Accumulator:
    """Compact per-(member, instrument, month, day-in-month) sufficient statistics.

    The whole estimator needs nothing else from the observation rows, so the
    records are consumed one instrument frame at a time and released.
    """

    def __init__(self, *, members: Sequence[str], instruments: Sequence[str], grid: CalendarGrid):
        self.member_index = {name: index for index, name in enumerate(members)}
        self.instrument_index = {name: index for index, name in enumerate(instruments)}
        self.grid = grid
        self.n_member = len(members)
        self.n_instrument = len(instruments)
        self.n_month = grid.month_count
        self.strata = self.n_member * self.n_instrument * self.n_month
        cells = self.strata * MAX_DAYS_IN_MONTH
        self._cells = cells
        self.target_count = np.zeros(cells, dtype=np.int64)
        self.control_count = np.zeros(cells, dtype=np.int64)
        self.target_net = np.zeros(cells, dtype=np.float64)
        self.control_net = np.zeros(cells, dtype=np.float64)
        self.target_gross = np.zeros(cells, dtype=np.float64)
        self.control_gross = np.zeros(cells, dtype=np.float64)
        self.overlap_count = np.zeros(cells, dtype=np.int64)
        # Exact order-independent identity sums, kept as two 32-bit halves so
        # float64 accumulation stays exact.
        self.fingerprint_low = np.zeros(self.strata, dtype=np.float64)
        self.fingerprint_high = np.zeros(self.strata, dtype=np.float64)
        # Per-member availability bookkeeping, for the honest loss report.
        self.rows_total = np.zeros(self.n_member, dtype=np.int64)
        self.target_rows = np.zeros(self.n_member, dtype=np.int64)
        self.target_unavailable = np.zeros(self.n_member, dtype=np.int64)
        self.target_invalid_return = np.zeros(self.n_member, dtype=np.int64)
        self.control_rows = np.zeros(self.n_member, dtype=np.int64)
        self.control_unavailable = np.zeros(self.n_member, dtype=np.int64)
        self.control_invalid_return = np.zeros(self.n_member, dtype=np.int64)
        self.instruments_seen = np.zeros((self.n_member, self.n_instrument), dtype=bool)

    def add(self, frame: Any, *, where: str) -> None:
        columns = _frame_columns(frame, where=where)
        length = _frame_length(columns, where=where)
        if length == 0:
            return
        member_names = np.asarray(columns["member_id"], dtype=object)
        instrument_names = np.asarray(columns["instrument_id"], dtype=object)
        try:
            member_idx = np.fromiter(
                (self.member_index[str(name)] for name in member_names.tolist()),
                dtype=np.int64,
                count=length,
            )
        except KeyError as exc:
            raise PatternLabDataError(
                f"{where}.member_id: {exc.args[0]!r} is not a member of the frozen family."
            ) from None
        try:
            instrument_idx = np.fromiter(
                (self.instrument_index[str(name)] for name in instrument_names.tolist()),
                dtype=np.int64,
                count=length,
            )
        except KeyError as exc:
            raise PatternLabDataError(
                f"{where}.instrument_id: {exc.args[0]!r} is not one of the declared instruments."
            ) from None

        signal = exact_int64(columns["signal_time_ms"], f"{where}.signal_time_ms")
        if signal.shape != (length,):
            raise PatternLabDataError(
                f"{where}.signal_time_ms: expected an aligned ({length},) array, got {signal.shape}."
            )
        outside = (signal < self.grid.study_start_ms) | (signal >= self.grid.study_end_ms)
        if outside.any():
            raise PatternLabDataError(
                f"{where}.signal_time_ms: {int(np.count_nonzero(outside))} observation(s) fall "
                "outside the half-open source study interval."
            )
        is_target = _boolean(columns["is_target"], f"{where}.is_target", length)
        is_control = _boolean(columns["is_control"], f"{where}.is_control", length)
        available = _boolean(columns["available"], f"{where}.available", length)
        return_valid = _boolean(columns["return_valid"], f"{where}.return_valid", length)
        net = _float(columns["net_return"], f"{where}.net_return", length)
        gross = _float(columns["gross_return"], f"{where}.gross_return", length)
        _check_returns("net_return", net, return_valid, where)
        _check_returns("gross_return", gross, return_valid, where)

        day_offset = self.grid.day_offsets(signal)
        month_idx = self.grid.month_of_day[day_offset]
        in_month = day_offset - self.grid.month_first_offset[month_idx]
        stratum = (
            member_idx * (self.n_instrument * self.n_month)
            + instrument_idx * self.n_month
            + month_idx
        )
        cell = stratum * MAX_DAYS_IN_MONTH + in_month

        valid_target = is_target & available & return_valid
        valid_control = is_control & available & return_valid

        self.rows_total += np.bincount(member_idx, minlength=self.n_member)
        self.instruments_seen[member_idx, instrument_idx] = True
        for mask, counter in (
            (is_target, self.target_rows),
            (is_target & ~available, self.target_unavailable),
            (is_target & available & ~return_valid, self.target_invalid_return),
            (is_control, self.control_rows),
            (is_control & ~available, self.control_unavailable),
            (is_control & available & ~return_valid, self.control_invalid_return),
        ):
            if mask.any():
                counter += np.bincount(member_idx[mask], minlength=self.n_member)

        self._scatter_counts(cell, valid_target, self.target_count)
        self._scatter_counts(cell, valid_control, self.control_count)
        self._scatter_counts(cell, valid_target & valid_control, self.overlap_count)
        self._scatter_sums(cell, valid_target, net, self.target_net)
        self._scatter_sums(cell, valid_control, net, self.control_net)
        self._scatter_sums(cell, valid_target, gross, self.target_gross)
        self._scatter_sums(cell, valid_control, gross, self.control_gross)

        if valid_target.any():
            keys = (instrument_idx[valid_target].astype(np.uint64) << np.uint64(42)) ^ (
                signal[valid_target].astype(np.uint64)
            )
            hashed = _splitmix64(keys)
            low = (hashed & np.uint64(0xFFFFFFFF)).astype(np.float64)
            high = (hashed >> np.uint64(32)).astype(np.float64)
            selected = stratum[valid_target]
            self.fingerprint_low += np.bincount(selected, weights=low, minlength=self.strata)
            self.fingerprint_high += np.bincount(selected, weights=high, minlength=self.strata)

    def _scatter_counts(self, cell: np.ndarray, mask: np.ndarray, target: np.ndarray) -> None:
        if mask.any():
            target += np.bincount(cell[mask], minlength=self._cells)

    def _scatter_sums(
        self, cell: np.ndarray, mask: np.ndarray, values: np.ndarray, target: np.ndarray
    ) -> None:
        if mask.any():
            target += np.bincount(cell[mask], weights=values[mask], minlength=self._cells)

    def daily(self, name: str) -> np.ndarray:
        """Return one statistic reshaped to (stratum, day-in-month)."""
        return getattr(self, name).reshape(self.strata, MAX_DAYS_IN_MONTH)


def _frame_columns(frame: Any, *, where: str) -> Mapping[str, Any]:
    if isinstance(frame, pd.DataFrame):
        supplied = list(frame.columns)
        missing = sorted(set(RECORD_COLUMNS) - set(supplied))
        extra = sorted(set(supplied) - set(RECORD_COLUMNS))
        if missing or extra:
            raise PatternLabDataError(
                f"{where}: the observation record schema is closed; missing columns {missing}, "
                f"unexpected columns {extra}."
            )
        return {name: frame[name].to_numpy() for name in RECORD_COLUMNS}
    if isinstance(frame, Mapping):
        missing = sorted(set(RECORD_COLUMNS) - set(frame))
        extra = sorted(set(frame) - set(RECORD_COLUMNS))
        if missing or extra:
            raise PatternLabDataError(
                f"{where}: the observation record schema is closed; missing columns {missing}, "
                f"unexpected columns {extra}."
            )
        return frame
    raise PatternLabDataError(
        f"{where}: expected a pandas DataFrame or a mapping of aligned column arrays, got "
        f"{type(frame).__name__}."
    )


def _frame_length(columns: Mapping[str, Any], *, where: str) -> int:
    lengths = {name: int(np.asarray(columns[name]).shape[0]) for name in RECORD_COLUMNS}
    unique = set(lengths.values())
    if len(unique) != 1:
        raise PatternLabDataError(f"{where}: the record columns are not aligned: {lengths}.")
    for name in RECORD_COLUMNS:
        shape = np.asarray(columns[name]).shape
        if len(shape) != 1:
            raise PatternLabDataError(
                f"{where}.{name}: expected a one-dimensional column, got shape {shape}."
            )
    return unique.pop()


# --------------------------------------------------------------------------
# the calendar block bootstrap
# --------------------------------------------------------------------------

def block_starts(rng: np.random.Generator, *, days: int, blocks: int, count: int) -> np.ndarray:
    """Draw ``count`` replicates of ``blocks`` uniform starts in ``[0, days)``.

    The draw order is replicate-major: one C-ordered ``(count, blocks)`` call
    consumes the same stream as the concatenation of smaller batches, so batching
    never changes the drawn index sequence.
    """
    return rng.integers(0, days, size=(count, blocks), dtype=np.int64)


def block_lengths(days: int) -> np.ndarray:
    """Block lengths of one replicate, with the last block truncated to T days."""
    blocks = -(-days // BLOCK_LENGTH_DAYS)
    lengths = np.full(blocks, BLOCK_LENGTH_DAYS, dtype=np.int64)
    lengths[-1] = days - BLOCK_LENGTH_DAYS * (blocks - 1)
    return lengths


def replicate_indices(starts: np.ndarray, days: int) -> np.ndarray:
    """Expand one replicate's block starts into its full day index sequence.

    This is the explicit definition the fast prefix-sum path must reproduce; the
    tests keep it as a slow index-based oracle.
    """
    lengths = block_lengths(days)
    pieces = [
        (np.arange(length, dtype=np.int64) + int(start)) % days
        for start, length in zip(np.asarray(starts).tolist(), lengths.tolist())
    ]
    return np.concatenate(pieces)[:days]


def _bootstrap_sums(vector: np.ndarray, starts: np.ndarray, lengths: np.ndarray) -> np.ndarray:
    """Sum ``vector`` over each replicate's circular block index sequence.

    ``prefix`` covers two concatenated copies of the day vector, so a block that
    wraps past the last day is one contiguous difference rather than a
    materialized index array.
    """
    days = int(vector.size)
    prefix = np.zeros(2 * days + 1, dtype=np.float64)
    np.cumsum(np.concatenate([vector, vector]), out=prefix[1:])
    ends = starts + lengths[np.newaxis, :]
    return (prefix[ends] - prefix[starts]).sum(axis=1)


# --------------------------------------------------------------------------
# the estimator
# --------------------------------------------------------------------------

def _member_key(member: Any, name: str) -> Any:
    if isinstance(member, Mapping):
        return member[name]
    return getattr(member, name)


def _family_records(family: Sequence[Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in family:
        record = {
            "member_id": str(_member_key(item, "member_id")),
            "comparison_id": str(_member_key(item, "comparison_id")),
            "kind": str(_member_key(item, "kind")),
            "model_instance_id": str(_member_key(item, "model_instance_id")),
            "timeframe_minutes": int(_member_key(item, "timeframe_minutes")),
            "case_id": str(_member_key(item, "case_id")),
            "direction": str(_member_key(item, "direction")),
            "horizon_minutes": int(_member_key(item, "horizon_minutes")),
            "primary": bool(_member_key(item, "primary")),
        }
        if record["member_id"] in seen:
            raise PatternLabDataError(
                f"family: duplicate member identity {record['member_id']!r}."
            )
        seen.add(record["member_id"])
        records.append(record)
    if not records:
        raise PatternLabDataError("family: at least one resolved member is required.")
    return records


def _holm(p_values: np.ndarray, member_ids: Sequence[str]) -> np.ndarray:
    """Holm's step-down adjustment over the declared family.

    Members are sorted by ``(p, stable_id)``; for rank ``i`` starting at 1 the
    adjusted value is the running maximum of ``(m - i + 1) * p_i`` clipped to 1,
    then mapped back to the stable IDs.
    """
    size = int(p_values.size)
    order = sorted(range(size), key=lambda index: (float(p_values[index]), member_ids[index]))
    adjusted = np.ones(size, dtype=np.float64)
    running = 0.0
    for rank, index in enumerate(order, start=1):
        candidate = (size - rank + 1) * float(p_values[index])
        running = max(running, min(1.0, candidate))
        adjusted[index] = running
    return adjusted


@dataclass(frozen=True)
class AccumulatedEstimates:
    """The shared pre-inference stage of one evaluation.

    Accumulation, stratum support, point estimates, coverage geometry and the
    joint daily influence of every family member are all decided here, **before**
    any resampling.  :func:`evaluate_observations` continues from this object into
    the bootstrap, the Holm correction and the published tables; research code
    that applies a different uncertainty calculation consumes the same object, so
    it shares one owner of matching, support and weighting and its availability
    can never depend on a sampled bootstrap draw.
    """

    members: tuple[Mapping[str, Any], ...]
    member_ids: tuple[str, ...]
    instrument_ids: tuple[str, ...]
    grid: CalendarGrid
    results: list[dict[str, Any]]
    influence: dict[int, dict[str, np.ndarray]]
    strata: dict[int, dict[str, np.ndarray]]
    retained: np.ndarray
    nE: np.ndarray
    nC: np.ndarray
    a_sum: np.ndarray
    b_sum: np.ndarray
    ag_sum: np.ndarray
    bg_sum: np.ndarray
    overlap: np.ndarray
    target_days: np.ndarray
    control_days: np.ndarray
    e_daily: np.ndarray
    c_daily: np.ndarray
    a_daily: np.ndarray
    b_daily: np.ndarray
    ag_daily: np.ndarray
    bg_daily: np.ndarray
    o_daily: np.ndarray
    stratum_member: np.ndarray
    stratum_instrument: np.ndarray
    stratum_month: np.ndarray
    lengths: np.ndarray


def _resolved_members(
    family: Sequence[Any], instruments: Sequence[str]
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    members = _family_records(family)
    member_ids = [item["member_id"] for item in members]
    instrument_ids = list(dict.fromkeys(str(item) for item in instruments))
    if len(instrument_ids) != len(instruments):
        raise PatternLabDataError("instruments: duplicate instrument IDs are rejected.")
    if not instrument_ids:
        raise PatternLabDataError("instruments: at least one instrument is required.")
    return members, member_ids, instrument_ids


def accumulate_observations(
    records: Any,
    *,
    family: Sequence[Any],
    instruments: Sequence[str],
    study_start_ms: int,
    study_end_ms: int,
) -> AccumulatedEstimates:
    """Accumulate aligned observation records into point estimates and support.

    ``records`` is one aligned observation table or an iterable of per-instrument
    tables, validated exactly as :func:`evaluate_observations` validates them.
    Nothing is resampled and no inference is published here.
    """
    members, member_ids, instrument_ids = _resolved_members(family, instruments)
    grid = CalendarGrid(study_start_ms, study_end_ms)
    accumulator = _Accumulator(members=member_ids, instruments=instrument_ids, grid=grid)
    frames: Iterable[Any]
    if isinstance(records, (pd.DataFrame, Mapping)):
        frames = [records]
    else:
        frames = records
    for position, frame in enumerate(frames):
        accumulator.add(frame, where=f"observation records[{position}]")

    n_member = len(member_ids)
    n_instrument = len(instrument_ids)
    n_month = grid.month_count
    per_member = n_instrument * n_month

    e_daily = accumulator.daily("target_count")
    c_daily = accumulator.daily("control_count")
    a_daily = accumulator.daily("target_net")
    b_daily = accumulator.daily("control_net")
    ag_daily = accumulator.daily("target_gross")
    bg_daily = accumulator.daily("control_gross")
    o_daily = accumulator.daily("overlap_count")

    nE = e_daily.sum(axis=1)
    nC = c_daily.sum(axis=1)
    a_sum = a_daily.sum(axis=1)
    b_sum = b_daily.sum(axis=1)
    ag_sum = ag_daily.sum(axis=1)
    bg_sum = bg_daily.sum(axis=1)
    overlap = o_daily.sum(axis=1)
    target_days = (e_daily > 0).sum(axis=1)
    control_days = (c_daily > 0).sum(axis=1)

    retained = (
        (nE >= MIN_STRATUM_TARGET)
        & (nC >= MIN_STRATUM_CONTROL)
        & (target_days >= MIN_STRATUM_TARGET_DAYS)
        & (control_days >= MIN_STRATUM_CONTROL_DAYS)
    )

    stratum_month = np.tile(np.arange(n_month, dtype=np.int64), n_member * n_instrument)
    stratum_instrument = np.tile(
        np.repeat(np.arange(n_instrument, dtype=np.int64), n_month), n_member
    )
    stratum_member = np.repeat(np.arange(n_member, dtype=np.int64), per_member)

    results: list[dict[str, Any]] = []
    influence: dict[int, dict[str, np.ndarray]] = {}
    strata: dict[int, dict[str, np.ndarray]] = {}
    for index in range(n_member):
        member = members[index]
        block = slice(index * per_member, (index + 1) * per_member)
        outcome = _member_estimates(
            member,
            grid=grid,
            retained=retained[block],
            nE=nE[block],
            nC=nC[block],
            a_sum=a_sum[block],
            b_sum=b_sum[block],
            ag_sum=ag_sum[block],
            bg_sum=bg_sum[block],
            overlap=overlap[block],
            e_daily=e_daily[block],
            c_daily=c_daily[block],
            a_daily=a_daily[block],
            b_daily=b_daily[block],
            month_index=stratum_month[block],
            instrument_index=stratum_instrument[block],
            instrument_ids=instrument_ids,
            fingerprint_low=accumulator.fingerprint_low[block],
            fingerprint_high=accumulator.fingerprint_high[block],
            valid_target_total=int(
                accumulator.target_rows[index]
                - accumulator.target_unavailable[index]
                - accumulator.target_invalid_return[index]
            ),
            counts={
                "records_supplied": int(accumulator.rows_total[index]),
                "target_rows": int(accumulator.target_rows[index]),
                "target_unavailable": int(accumulator.target_unavailable[index]),
                "target_invalid_return": int(accumulator.target_invalid_return[index]),
                "control_rows": int(accumulator.control_rows[index]),
                "control_unavailable": int(accumulator.control_unavailable[index]),
                "control_invalid_return": int(accumulator.control_invalid_return[index]),
                "instruments_present": int(accumulator.instruments_seen[index].sum()),
            },
        )
        vectors = outcome.pop("_influence", None)
        if vectors is not None:
            influence[index] = vectors
        retained_strata = outcome.pop("_strata", None)
        if retained_strata is not None:
            strata[index] = retained_strata
        results.append(outcome)

    _apply_horizon_fingerprints(members, results)

    return AccumulatedEstimates(
        members=tuple(members),
        member_ids=tuple(member_ids),
        instrument_ids=tuple(instrument_ids),
        grid=grid,
        results=results,
        influence=influence,
        strata=strata,
        retained=retained,
        nE=nE,
        nC=nC,
        a_sum=a_sum,
        b_sum=b_sum,
        ag_sum=ag_sum,
        bg_sum=bg_sum,
        overlap=overlap,
        target_days=target_days,
        control_days=control_days,
        e_daily=e_daily,
        c_daily=c_daily,
        a_daily=a_daily,
        b_daily=b_daily,
        ag_daily=ag_daily,
        bg_daily=bg_daily,
        o_daily=o_daily,
        stratum_member=stratum_member,
        stratum_instrument=stratum_instrument,
        stratum_month=stratum_month,
        lengths=block_lengths(grid.days),
    )


def evaluate_observations(
    records: Any,
    *,
    family: Sequence[Any],
    instruments: Sequence[str],
    study_start_ms: int,
    study_end_ms: int,
    resamples: int,
    seed: int,
    batch_size: int = BOOTSTRAP_BATCH,
) -> dict[str, Any]:
    """Estimate every family member's matched comparison and its uncertainty.

    ``records`` is one aligned observation table or an iterable of per-instrument
    tables.  Each row is one eligible anchor of one resolved family member, with
    its target/control membership, the comparison's common condition
    availability, the net and gross returns and the return validity.

    The returned mapping holds the numeric results and compact columnar tables;
    it publishes nothing and verifies no source provenance.
    """
    if isinstance(resamples, bool) or not isinstance(resamples, int) or resamples < 1:
        raise PatternLabDataError(f"resamples: expected a positive integer, got {resamples!r}.")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= analysis_request.MAX_SEED:
        raise PatternLabDataError(f"seed: expected an integer in [0, 2**32-1], got {seed!r}.")

    accumulated = accumulate_observations(
        records,
        family=family,
        instruments=instruments,
        study_start_ms=study_start_ms,
        study_end_ms=study_end_ms,
    )
    return _evaluate_accumulated(
        accumulated,
        resamples=int(resamples),
        seed=int(seed),
        batch_size=int(batch_size),
    )


def _evaluate_accumulated(
    accumulated: AccumulatedEstimates,
    *,
    resamples: int,
    seed: int,
    batch_size: int,
) -> dict[str, Any]:
    """Resample the accumulated members, adjust the family and build the tables."""
    members = accumulated.members
    member_ids = accumulated.member_ids
    instrument_ids = accumulated.instrument_ids
    grid = accumulated.grid
    results = deepcopy(accumulated.results)
    n_member = len(member_ids)
    lengths = accumulated.lengths
    k_draw = int(lengths.size)
    rng = np.random.Generator(np.random.PCG64(seed))

    _run_bootstrap(
        results, accumulated.influence, rng=rng, days=grid.days, lengths=lengths,
        resamples=resamples, batch_size=batch_size,
    )

    internal = np.array(
        [1.0 if item["p_raw"] is None else float(item["p_raw"]) for item in results],
        dtype=np.float64,
    )
    adjusted = _holm(internal, list(member_ids))
    for index, item in enumerate(results):
        item["p_holm_internal"] = float(adjusted[index])
        if item["inference_available"]:
            item["p_holm"] = float(adjusted[index])
            item["nominal_reject_holm"] = bool(adjusted[index] <= ALPHA)
            item["effect_sign"] = (
                "positive" if item["lift"] > 0 else "negative" if item["lift"] < 0 else "zero"
            )
        else:
            item["p_holm"] = None
            item["nominal_reject_holm"] = None
            item["effect_sign"] = None

    resolution = 2.0 / (resamples + 1)
    return {
        "schema_version": ESTIMATOR_SCHEMA_VERSION,
        "method": analysis_request.method_settings(1),
        "inference_scope": INFERENCE_SCOPE,
        "calendar": {
            **grid.as_json(),
            "block_length_days": BLOCK_LENGTH_DAYS,
            "k_draw": k_draw,
            "k_draw_note": (
                "K_draw is the number of bootstrap block draws per replicate, not the amount of "
                "supported data."
            ),
        },
        "resamples": resamples,
        "seed": seed,
        "family_size": n_member,
        "p_resolution": resolution,
        "p_resolution_blocks_first_rejection": bool(resolution > ALPHA / n_member),
        "instruments": list(instrument_ids),
        "members": results,
        **_accumulated_tables(accumulated),
    }


def evaluate_monthly_observations(
    records: Any, *, family: Sequence[Any], instruments: Sequence[str],
    study_start_ms: int, study_end_ms: int,
) -> dict[str, Any]:
    """Version-2 inference over the same checked matched accumulation."""
    return _evaluate_monthly_accumulated(accumulate_observations(
        records, family=family, instruments=instruments,
        study_start_ms=study_start_ms, study_end_ms=study_end_ms,
    ))


def _evaluate_monthly_accumulated(accumulated: AccumulatedEstimates) -> dict[str, Any]:
    candidates = _evaluate_monthly_candidate(accumulated)
    results = deepcopy(accumulated.results)
    for result, evaluated in zip(results, candidates["members"]):
        candidate = evaluated["candidate"]
        for key in ("bootstrap", "p_upper", "p_lower", "degeneracy"):
            result.pop(key, None)
        result["geometry"].pop("k_draw", None)
        for key in ("inference_available", "unavailable_reasons", "p_raw", "p_holm",
                    "p_holm_internal", "nominal_reject_holm", "nominal_reject_raw"):
            result[key] = deepcopy(evaluated[key])
        result["intervals"] = deepcopy(candidate["intervals"])
        result["effect_sign"] = ("positive" if result["lift"] > 0 else
                                 "negative" if result["lift"] < 0 else "zero") if result["inference_available"] else None
        diagnostic = {key: deepcopy(candidate[key]) for key in (
            "method", "standard_error", "informative_months", "degrees_of_freedom", "balance",
            "degeneracy", "t_statistic", "t_critical", "covariance_note")}
        # Full-sample support refusals still retain known month geometry. No SE/t
        # calculation is invented for a member the support rules refused.
        if diagnostic["informative_months"] is None:
            known = result["geometry"].get("retained_months")
            if known is not None:
                diagnostic["informative_months"] = known
                diagnostic["degrees_of_freedom"] = max(known - 1, 0)
        groups = diagnostic["informative_months"]
        diagnostic["month_count_in_calibration"] = None if groups is None else groups == 12
        result["monthly_inference"] = diagnostic
    return {
        "schema_version": 2, "method": analysis_request.method_settings(2),
        "inference_scope": INFERENCE_SCOPE, "calendar": accumulated.grid.as_json(),
        "family_size": len(results), "instruments": list(accumulated.instrument_ids),
        "members": results, **_accumulated_tables(accumulated),
    }


def _mean(total: float, count: int) -> float | None:
    return float(total / count) if count else None


def _member_estimates(
    member: Mapping[str, Any],
    *,
    grid: CalendarGrid,
    retained: np.ndarray,
    nE: np.ndarray,
    nC: np.ndarray,
    a_sum: np.ndarray,
    b_sum: np.ndarray,
    ag_sum: np.ndarray,
    bg_sum: np.ndarray,
    overlap: np.ndarray,
    e_daily: np.ndarray,
    c_daily: np.ndarray,
    a_daily: np.ndarray,
    b_daily: np.ndarray,
    month_index: np.ndarray,
    instrument_index: np.ndarray,
    instrument_ids: Sequence[str],
    fingerprint_low: np.ndarray,
    fingerprint_high: np.ndarray,
    valid_target_total: int,
    counts: Mapping[str, int],
) -> dict[str, Any]:
    """Point estimates, support geometry and the joint influence of one member."""
    rows = np.flatnonzero(retained)
    total_target = int(nE.sum())
    total_control = int(nC.sum())
    outcome: dict[str, Any] = {
        "member_id": member["member_id"],
        "comparison_id": member["comparison_id"],
        "kind": member["kind"],
        "model_instance_id": member["model_instance_id"],
        "timeframe_minutes": member["timeframe_minutes"],
        "case_id": member["case_id"],
        "direction": member["direction"],
        "horizon_minutes": member["horizon_minutes"],
        "primary": member["primary"],
        "counts": {
            **dict(counts),
            "valid_target_available": int(valid_target_total),
            "valid_target_all_strata": total_target,
            "valid_control_all_strata": total_control,
            "strata_total": int(retained.size),
            "strata_retained": int(rows.size),
            "strata_with_any_target": int(np.count_nonzero(nE > 0)),
        },
        "raw_valid_target": {
            "n": total_target,
            "mean_net_return": _mean(float(a_sum.sum()), total_target),
            "mean_gross_return": _mean(float(ag_sum.sum()), total_target),
        },
    }

    if total_target != int(valid_target_total):
        raise PatternLabDataError(
            f"{member['member_id']}: {total_target} stratified valid target observation(s) "
            f"contradict the {int(valid_target_total)} counted at the record boundary."
        )
    N = int(nE[rows].sum()) if rows.size else 0
    if N == 0:
        outcome.update(_empty_estimates(grid, valid_target_total, instrument_ids))
        return outcome

    nE_r = nE[rows].astype(np.float64)
    nC_r = nC[rows].astype(np.float64)
    a_r = a_sum[rows]
    b_r = b_sum[rows]
    muC = b_r / nC_r
    weights = nE_r / N
    # Equivalently sum_s w_s * muE_s; the pooled form avoids a second division.
    signal = float(a_r.sum() / N)
    control = float((weights * muC).sum())
    lift = signal - control
    signal_gross = float(ag_sum[rows].sum() / N)
    control_gross = float((weights * (bg_sum[rows] / nC_r)).sum())

    day_table = grid.day_index_table[month_index[rows]]
    valid_cells = day_table >= 0
    days = day_table[valid_cells]

    e_cells = e_daily[rows][valid_cells].astype(np.float64)
    c_cells = c_daily[rows][valid_cells].astype(np.float64)
    a_cells = a_daily[rows][valid_cells]
    b_cells = b_daily[rows][valid_cells]

    daily_e = np.bincount(days, weights=e_cells, minlength=grid.days)
    daily_c = np.bincount(days, weights=c_cells, minlength=grid.days)
    daily_a = np.bincount(days, weights=a_cells, minlength=grid.days)

    uE = (daily_a - signal * daily_e) / N
    # The e_sd terms matter: dropping them silently freezes the estimated event
    # weights and understates the joint uncertainty.
    stratum_of_cell, _column = np.nonzero(valid_cells)
    alpha = (muC - control) / N
    beta = (nE_r / nC_r) / N
    contribution = alpha[stratum_of_cell] * e_cells + beta[stratum_of_cell] * (
        b_cells - muC[stratum_of_cell] * c_cells
    )
    uC = np.bincount(days, weights=contribution, minlength=grid.days)
    uD = uE - uC

    mass_target = float(np.abs(a_cells).sum() + abs(signal) * e_cells.sum()) / N
    mass_control = float(
        (
            np.abs(alpha[stratum_of_cell] * e_cells)
            + np.abs(beta[stratum_of_cell] * b_cells)
            + np.abs(beta[stratum_of_cell] * muC[stratum_of_cell] * c_cells)
        ).sum()
    )
    masses = {"uE": mass_target, "uC": mass_control, "uD": mass_target + mass_control}
    for name, vector in (("uE", uE), ("uC", uC), ("uD", uD)):
        residual = abs(float(vector.sum()))
        if residual > ZERO_SUM_TOLERANCE * masses[name]:
            raise PatternLabDataError(
                f"{member['member_id']}: the joint influence vector {name} sums to {residual}, "
                f"which is material against the {masses[name]} of absolute contributions that "
                "produced it. A nonzero sum is an implementation failure, not something centering "
                "may conceal."
            )
    uE = uE - uE.mean()
    uC = uC - uC.mean()
    uD = uD - uD.mean()

    active = (daily_e > 0) & (daily_c > 0)
    active_days = int(np.count_nonzero(active))
    positions = np.flatnonzero(active)
    span = int(positions[-1] - positions[0] + 1) if positions.size else 0
    full_bins = grid.days // BLOCK_LENGTH_DAYS
    if full_bins:
        binned = active[: full_bins * BLOCK_LENGTH_DAYS].reshape(full_bins, BLOCK_LENGTH_DAYS)
        supported_blocks = int(
            np.count_nonzero(binned.sum(axis=1) >= MIN_SUPPORTED_BLOCK_ACTIVE_DAYS)
        )
    else:
        supported_blocks = 0
    share = (N / valid_target_total) if valid_target_total else None

    reasons: list[str] = []
    if grid.days < MIN_DAY_GRID or span < MIN_SUPPORTED_SPAN_DAYS:
        reasons.append(REASON_SPAN)
    if active_days < MIN_JOINT_ACTIVE_DAYS:
        reasons.append(REASON_ACTIVE_DAYS)
    if supported_blocks < MIN_SUPPORTED_BLOCKS:
        reasons.append(REASON_BLOCKS)
    if share is None or share < MIN_RETAINED_TARGET_SHARE:
        reasons.append(REASON_COVERAGE)
    if int(member["horizon_minutes"]) > MAX_INFERENCE_HORIZON_MINUTES:
        reasons.append(REASON_HORIZON)

    # The inclusive-parent identity is a stratum-level fact; different stratum
    # shares are exactly why one pooled share cannot correct the aggregate.
    inclusive = overlap[rows] == nE[rows]
    inclusive_shares = np.where(inclusive & (nC_r > 0), nE_r / nC_r, np.nan)

    per_ticker = _per_ticker_estimates(
        rows=rows,
        instrument_index=instrument_index,
        instrument_ids=instrument_ids,
        nE=nE_r,
        nC=nC_r,
        a_sum=a_r,
        b_sum=b_r,
    )

    outcome.update(
        {
            "supported_population": {
                "retained_target_observations": N,
                "retained_control_observations": int(nC[rows].sum()),
                "retained_overlapping_anchors": int(overlap[rows].sum()),
                "retained_target_share": share,
                "retained_instruments": sorted(
                    {instrument_ids[int(item)] for item in instrument_index[rows]}
                ),
                "retained_months": sorted(
                    {grid.month_labels[int(item)] for item in month_index[rows]}
                ),
                "excluded_instruments": sorted(
                    set(instrument_ids)
                    - {instrument_ids[int(item)] for item in instrument_index[rows]}
                ),
                "excluded_months": sorted(
                    set(grid.month_labels)
                    - {grid.month_labels[int(item)] for item in month_index[rows]}
                ),
                "excluded_target_observations": int(total_target - N),
                "mean_inclusive_parent_share": (
                    float(np.nanmean(inclusive_shares))
                    if np.isfinite(inclusive_shares).any()
                    else None
                ),
                "inclusive_parent_strata": int(np.count_nonzero(inclusive)),
            },
            "signal": signal,
            "control": control,
            "lift": lift,
            "signal_gross": signal_gross,
            "control_gross": control_gross,
            "lift_gross": signal_gross - control_gross,
            "signal_commission": signal_gross - signal,
            "control_commission": control_gross - control,
            "equal_ticker": per_ticker,
            "geometry": {
                "day_grid_days": grid.days,
                "block_length_days": BLOCK_LENGTH_DAYS,
                "k_draw": int(block_lengths(grid.days).size),
                "joint_active_days": active_days,
                "supported_span_days": span,
                "supported_blocks": supported_blocks,
                "retained_months": int(np.unique(month_index[rows]).size),
                "retained_strata": int(rows.size),
            },
            "population_fingerprint": _fingerprint(
                rows=rows,
                instrument_index=instrument_index,
                month_index=month_index,
                instrument_ids=instrument_ids,
                months=grid.month_labels,
                nE=nE,
                low=fingerprint_low,
                high=fingerprint_high,
            ),
            "unavailable_reasons": [item for item in REASON_ORDER if item in reasons],
            "inference_available": not reasons,
            "p_raw": None,
            "p_upper": None,
            "p_lower": None,
            "intervals": {"signal": None, "control": None, "lift": None},
            "bootstrap": {"signal": None, "control": None, "lift": None},
            "degeneracy": None,
        }
    )
    if not reasons:
        outcome["_influence"] = {"uE": uE, "uC": uC, "uD": uD}
        # The retained sufficient statistics of this member, in retained-stratum
        # order: the one place matching, support and weighting are decided, so a
        # different uncertainty calculation reuses them rather than reselecting.
        outcome["_strata"] = {
            "month_index": month_index[rows].copy(),
            "instrument_index": instrument_index[rows].copy(),
            "target_count": nE_r.copy(),
            "control_count": nC_r.copy(),
            "target_net_sum": a_r.copy(),
            "control_net_sum": b_r.copy(),
        }
    return outcome


def _empty_estimates(
    grid: CalendarGrid, valid_target_total: int, grid_instruments: Sequence[str]
) -> dict[str, Any]:
    """Null estimates with an explicit reason; never zeros and never an exception."""
    return {
        "supported_population": {
            "retained_target_observations": 0,
            "retained_control_observations": 0,
            "retained_overlapping_anchors": 0,
            "retained_target_share": 0.0 if valid_target_total else None,
            "retained_instruments": [],
            "retained_months": [],
            "excluded_instruments": list(grid_instruments),
            "excluded_months": list(grid.month_labels),
            "excluded_target_observations": 0,
            "mean_inclusive_parent_share": None,
            "inclusive_parent_strata": 0,
        },
        "signal": None,
        "control": None,
        "lift": None,
        "signal_gross": None,
        "control_gross": None,
        "lift_gross": None,
        "signal_commission": None,
        "control_commission": None,
        "equal_ticker": {
            "tickers": 0,
            "signal": None,
            "control": None,
            "lift": None,
            "per_ticker": [],
            "omitted_instruments": [],
        },
        "geometry": {
            "day_grid_days": grid.days,
            "block_length_days": BLOCK_LENGTH_DAYS,
            "k_draw": int(block_lengths(grid.days).size),
            "joint_active_days": 0,
            "supported_span_days": 0,
            "supported_blocks": 0,
            "retained_months": 0,
            "retained_strata": 0,
        },
        "population_fingerprint": contracts.semantic_digest([])[:32],
        "unavailable_reasons": [REASON_NO_SUPPORT],
        "inference_available": False,
        "p_raw": None,
        "p_upper": None,
        "p_lower": None,
        "intervals": {"signal": None, "control": None, "lift": None},
        "bootstrap": {"signal": None, "control": None, "lift": None},
        "degeneracy": None,
    }


def _per_ticker_estimates(
    *,
    rows: np.ndarray,
    instrument_index: np.ndarray,
    instrument_ids: Sequence[str],
    nE: np.ndarray,
    nC: np.ndarray,
    a_sum: np.ndarray,
    b_sum: np.ndarray,
) -> dict[str, Any]:
    """The equal-ticker descriptive companion, with disclosed denominators.

    Each ticker's own months use that ticker's target-count weights.  A ticker
    with no retained stratum is omitted with its denominator disclosed; it is
    never assigned zero, and these diagnostics carry no p-value.
    """
    selected = instrument_index[rows]
    entries: list[dict[str, Any]] = []
    for position in sorted(set(int(item) for item in selected.tolist())):
        mask = selected == position
        total = float(nE[mask].sum())
        if total <= 0:
            continue
        signal = float(a_sum[mask].sum() / total)
        control = float(((nE[mask] / total) * (b_sum[mask] / nC[mask])).sum())
        entries.append(
            {
                "instrument_id": instrument_ids[position],
                "signal": signal,
                "control": control,
                "lift": signal - control,
                "retained_target_observations": int(total),
                "retained_strata": int(np.count_nonzero(mask)),
            }
        )
    present = {item["instrument_id"] for item in entries}
    return {
        "tickers": len(entries),
        "signal": float(np.mean([item["signal"] for item in entries])) if entries else None,
        "control": float(np.mean([item["control"] for item in entries])) if entries else None,
        "lift": float(np.mean([item["lift"] for item in entries])) if entries else None,
        "per_ticker": entries,
        "omitted_instruments": [name for name in instrument_ids if name not in present],
    }


def _fingerprint(
    *,
    rows: np.ndarray,
    instrument_index: np.ndarray,
    month_index: np.ndarray,
    instrument_ids: Sequence[str],
    months: Sequence[str],
    nE: np.ndarray,
    low: np.ndarray,
    high: np.ndarray,
) -> str:
    """A compact identity of the retained target anchors and their support."""
    payload = [
        [
            instrument_ids[int(instrument_index[index])],
            months[int(month_index[index])],
            int(nE[index]),
            f"{int(high[index]):d}:{int(low[index]):d}",
        ]
        for index in sorted(
            rows.tolist(),
            key=lambda index: (
                instrument_ids[int(instrument_index[index])],
                months[int(month_index[index])],
            ),
        )
    ]
    return contracts.semantic_digest(payload)[:32]


def _run_bootstrap(
    results: Sequence[dict[str, Any]],
    influence: Mapping[int, Mapping[str, np.ndarray]],
    *,
    rng: np.random.Generator,
    days: int,
    lengths: np.ndarray,
    resamples: int,
    batch_size: int,
) -> None:
    """Resample every gated member with one shared calendar index sequence.

    The same drawn day indices apply to every instrument's joint contributions
    and to every comparison; there is no per-ticker, per-side, per-event or
    per-case independent resampling.
    """
    if batch_size < 1:
        raise PatternLabDataError(f"batch_size: expected a positive integer, got {batch_size!r}.")
    order = sorted(influence)
    collected = {
        index: {name: np.empty(resamples, dtype=np.float64) for name in ("uE", "uC", "uD")}
        for index in order
    }
    blocks = int(lengths.size)
    drawn = 0
    while drawn < resamples:
        count = min(batch_size, resamples - drawn)
        starts = block_starts(rng, days=days, blocks=blocks, count=count)
        for index in order:
            for name, vector in influence[index].items():
                collected[index][name][drawn : drawn + count] = _bootstrap_sums(
                    vector, starts, lengths
                )
        drawn += count

    for index in order:
        item = results[index]
        z_signal = collected[index]["uE"]
        z_control = collected[index]["uC"]
        z_lift = collected[index]["uD"]
        sigma = float(np.std(z_lift, ddof=1)) if z_lift.size > 1 else 0.0
        scale = float(
            np.sqrt(days)
            * max(
                float(np.max(np.abs(influence[index]["uE"]))) if days else 0.0,
                float(np.max(np.abs(influence[index]["uC"]))) if days else 0.0,
                float(np.max(np.abs(influence[index]["uD"]))) if days else 0.0,
            )
        )
        item["bootstrap"] = {
            name: _bootstrap_record(values)
            for name, values in (
                ("signal", z_signal), ("control", z_control), ("lift", z_lift)
            )
        }
        item["degeneracy"] = {
            "sigma": sigma,
            "component_scale": scale,
            "threshold": 128 * float(np.finfo(np.float64).eps) * scale,
        }
        constant = bool(np.all(z_lift == z_lift[0])) if z_lift.size else True
        if constant or sigma <= 128 * float(np.finfo(np.float64).eps) * scale:
            item["unavailable_reasons"] = sorted(
                set(item["unavailable_reasons"]) | {REASON_DEGENERATE},
                key=REASON_ORDER.index,
            )
            item["inference_available"] = False
            continue
        item["intervals"] = {
            "signal": _basic_interval(item["signal"], z_signal),
            "control": _basic_interval(item["control"], z_control),
            "lift": _basic_interval(item["lift"], z_lift),
        }
        upper = (1 + int(np.count_nonzero(z_lift >= item["lift"]))) / (resamples + 1)
        lower = (1 + int(np.count_nonzero(z_lift <= item["lift"]))) / (resamples + 1)
        item["p_upper"] = float(upper)
        item["p_lower"] = float(lower)
        item["p_raw"] = float(min(1.0, 2 * min(upper, lower)))


def _bootstrap_record(values: np.ndarray) -> dict[str, float]:
    quantiles = np.quantile(values, [0.025, 0.975], method="linear")
    return {
        "standard_deviation": float(np.std(values, ddof=1)) if values.size > 1 else 0.0,
        "quantile_0025": float(quantiles[0]),
        "quantile_0975": float(quantiles[1]),
    }


def _basic_interval(theta: float, values: np.ndarray) -> dict[str, float]:
    """The pointwise basic interval; it is never recentred by a normal fit."""
    quantiles = np.quantile(values, [0.025, 0.975], method="linear")
    return {"lower": float(theta - quantiles[1]), "upper": float(theta - quantiles[0])}


def _apply_horizon_fingerprints(
    members: Sequence[Mapping[str, Any]], results: Sequence[dict[str, Any]]
) -> None:
    """Name the horizon rows of one contrast that use different populations."""
    groups: dict[tuple[str, str, int, str], list[int]] = {}
    for index, member in enumerate(members):
        key = (
            member["comparison_id"],
            member["model_instance_id"],
            member["timeframe_minutes"],
            member["direction"],
        )
        groups.setdefault(key, []).append(index)
    for indices in groups.values():
        fingerprints = {results[index]["population_fingerprint"] for index in indices}
        agree = len(fingerprints) == 1
        for index in indices:
            results[index]["horizon_population"] = {
                "agrees_across_horizons": agree,
                "compared_members": [results[item]["member_id"] for item in indices],
                "note": (
                    "Every horizon of this contrast retained the same target anchors."
                    if agree
                    else "These horizon rows use different retained populations, even where the "
                    "counts happen to agree; no common-support retrimming is applied."
                ),
            }


# --------------------------------------------------------------------------
# compact tables
# --------------------------------------------------------------------------

def _stratum_reasons(
    nE: int, nC: int, target_days: int, control_days: int
) -> list[str]:
    reasons: list[str] = []
    if nE < MIN_STRATUM_TARGET:
        reasons.append(STRATUM_REASON_TARGET_COUNT)
    if nC < MIN_STRATUM_CONTROL:
        reasons.append(STRATUM_REASON_CONTROL_COUNT)
    if target_days < MIN_STRATUM_TARGET_DAYS:
        reasons.append(STRATUM_REASON_TARGET_DAYS)
    if control_days < MIN_STRATUM_CONTROL_DAYS:
        reasons.append(STRATUM_REASON_CONTROL_DAYS)
    return reasons


def _stratum_table(
    *,
    members: Sequence[Mapping[str, Any]],
    member_ids: Sequence[str],
    instrument_ids: Sequence[str],
    grid: CalendarGrid,
    retained: np.ndarray,
    nE: np.ndarray,
    nC: np.ndarray,
    a_sum: np.ndarray,
    b_sum: np.ndarray,
    ag_sum: np.ndarray,
    bg_sum: np.ndarray,
    overlap: np.ndarray,
    target_days: np.ndarray,
    control_days: np.ndarray,
    stratum_member: np.ndarray,
    stratum_instrument: np.ndarray,
    stratum_month: np.ndarray,
) -> dict[str, Any]:
    """Every stratum, including zero-event and excluded ones, with its reasons."""
    with np.errstate(invalid="ignore", divide="ignore"):
        mean_target = np.where(nE > 0, a_sum / np.maximum(nE, 1), np.nan)
        mean_control = np.where(nC > 0, b_sum / np.maximum(nC, 1), np.nan)
        mean_target_gross = np.where(nE > 0, ag_sum / np.maximum(nE, 1), np.nan)
        mean_control_gross = np.where(nC > 0, bg_sum / np.maximum(nC, 1), np.nan)
        share = np.where((nC > 0) & (overlap == nE) & (nE > 0), nE / np.maximum(nC, 1), np.nan)
        disjoint = np.where(
            (overlap == nE) & (nC > nE) & (nE > 0),
            (b_sum - a_sum) / np.maximum(nC - nE, 1),
            np.nan,
        )
    return {
        "member_id": [member_ids[int(index)] for index in stratum_member],
        "comparison_id": [members[int(index)]["comparison_id"] for index in stratum_member],
        "model_instance_id": [
            members[int(index)]["model_instance_id"] for index in stratum_member
        ],
        "timeframe_minutes": np.array(
            [members[int(index)]["timeframe_minutes"] for index in stratum_member], dtype=np.int64
        ),
        "case_id": [members[int(index)]["case_id"] for index in stratum_member],
        "instrument_id": [instrument_ids[int(index)] for index in stratum_instrument],
        "utc_month": [grid.month_labels[int(index)] for index in stratum_month],
        "target_observations": nE,
        "control_observations": nC,
        "target_days": target_days,
        "control_days": control_days,
        "overlapping_observations": overlap,
        "target_net_sum": a_sum,
        "control_net_sum": b_sum,
        "target_gross_sum": ag_sum,
        "control_gross_sum": bg_sum,
        "mean_target_net": mean_target,
        "mean_control_net": mean_control,
        "mean_target_gross": mean_target_gross,
        "mean_control_gross": mean_control_gross,
        "inclusive_parent_share": share,
        "disjoint_complement_mean_net": disjoint,
        "retained": retained,
        "exclusion_reasons": [
            ";".join(
                _stratum_reasons(
                    int(nE[index]), int(nC[index]), int(target_days[index]),
                    int(control_days[index]),
                )
            )
            for index in range(int(nE.size))
        ],
    }


def _daily_table(
    *,
    member_ids: Sequence[str],
    instrument_ids: Sequence[str],
    grid: CalendarGrid,
    retained: np.ndarray,
    e_daily: np.ndarray,
    c_daily: np.ndarray,
    a_daily: np.ndarray,
    b_daily: np.ndarray,
    ag_daily: np.ndarray,
    bg_daily: np.ndarray,
    o_daily: np.ndarray,
    stratum_member: np.ndarray,
    stratum_instrument: np.ndarray,
    stratum_month: np.ndarray,
) -> dict[str, Any]:
    """Daily counts and sums of the retained strata, in compact sparse form.

    Only days with a contribution are stored.  The complete calendar grid and
    the zero-contribution rule are recorded beside the table, so a missing day is
    an explicit zero for aggregation and never a fabricated observation.
    """
    rows = np.flatnonzero(retained)
    if rows.size == 0:
        empty_int = np.zeros(0, dtype=np.int64)
        empty_float = np.zeros(0, dtype=np.float64)
        return {
            "member_id": [], "instrument_id": [], "utc_month": [], "day_index": empty_int,
            "day_utc": [], "target_count": empty_int, "control_count": empty_int,
            "overlap_count": empty_int, "target_net_sum": empty_float,
            "control_net_sum": empty_float, "target_gross_sum": empty_float,
            "control_gross_sum": empty_float,
        }
    day_table = grid.day_index_table[stratum_month[rows]]
    keep = (day_table >= 0) & ((e_daily[rows] > 0) | (c_daily[rows] > 0))
    stratum_position, _column = np.nonzero(keep)
    selected = rows[stratum_position]
    days = day_table[keep]
    return {
        "member_id": [member_ids[int(stratum_member[index])] for index in selected],
        "instrument_id": [instrument_ids[int(stratum_instrument[index])] for index in selected],
        "utc_month": [grid.month_labels[int(stratum_month[index])] for index in selected],
        "day_index": days.astype(np.int64),
        "day_utc": [grid.day_utc(int(offset)) for offset in days],
        "target_count": e_daily[rows][keep].astype(np.int64),
        "control_count": c_daily[rows][keep].astype(np.int64),
        "overlap_count": o_daily[rows][keep].astype(np.int64),
        "target_net_sum": a_daily[rows][keep],
        "control_net_sum": b_daily[rows][keep],
        "target_gross_sum": ag_daily[rows][keep],
        "control_gross_sum": bg_daily[rows][keep],
    }


def _accumulated_tables(accumulated):
    """Shared compact evidence assembly; the numerical arrays stay read-only."""
    return {
        "strata": _stratum_table(
            members=accumulated.members,
            member_ids=accumulated.member_ids,
            instrument_ids=accumulated.instrument_ids,
            grid=accumulated.grid,
            retained=accumulated.retained,
            nE=accumulated.nE,
            nC=accumulated.nC,
            a_sum=accumulated.a_sum,
            b_sum=accumulated.b_sum,
            ag_sum=accumulated.ag_sum,
            bg_sum=accumulated.bg_sum,
            overlap=accumulated.overlap,
            target_days=accumulated.target_days,
            control_days=accumulated.control_days,
            stratum_member=accumulated.stratum_member,
            stratum_instrument=accumulated.stratum_instrument,
            stratum_month=accumulated.stratum_month,
        ),
        "daily": _daily_table(
            member_ids=accumulated.member_ids,
            instrument_ids=accumulated.instrument_ids,
            grid=accumulated.grid,
            retained=accumulated.retained,
            e_daily=accumulated.e_daily,
            c_daily=accumulated.c_daily,
            a_daily=accumulated.a_daily,
            b_daily=accumulated.b_daily,
            ag_daily=accumulated.ag_daily,
            bg_daily=accumulated.bg_daily,
            o_daily=accumulated.o_daily,
            stratum_member=accumulated.stratum_member,
            stratum_instrument=accumulated.stratum_instrument,
            stratum_month=accumulated.stratum_month,
        ),
    }

def _evaluate_monthly_candidate(accumulated: AccumulatedEstimates) -> dict[str, Any]:
    """Apply the candidate to every member of one accumulated family.

    The unchanged full-sample support, span, active-day, block, coverage and
    horizon gates have already been applied once, by the production accumulation.
    A member they refused stays refused; the candidate adds only its own
    mathematical validity rules and never reapplies a year-length admission gate
    to a deleted-month sample.
    """
    members: list[dict[str, Any]] = []
    for index, result in enumerate(accumulated.results):
        inherited = list(result["unavailable_reasons"])
        strata = accumulated.strata.get(index)
        if strata is None:
            candidate = _unavailable_candidate(inherited or [REASON_NO_SUPPORT])
        else:
            candidate = monthly_jackknife(
                month_index=strata["month_index"],
                target_count=strata["target_count"],
                control_count=strata["control_count"],
                target_net_sum=strata["target_net_sum"],
                control_net_sum=strata["control_net_sum"],
            )
        point = {name: result[name] for name in ("signal", "control", "lift")}
        identity = None
        if candidate["theta"]["lift"] is not None and point["lift"] is not None:
            identity = max(
                abs(float(candidate["theta"][name]) - float(point[name]))
                for name in ("signal", "control", "lift")
            )
        members.append(
            {
                "member_id": result["member_id"],
                "primary": bool(result["primary"]),
                "horizon_minutes": int(result["horizon_minutes"]),
                "direction": result["direction"],
                # The reported point estimate is the original full-sample one.
                "signal": point["signal"],
                "control": point["control"],
                "lift": point["lift"],
                "point_identity_max_abs_difference": identity,
                "inherited_reasons": inherited,
                "candidate": candidate,
                "inference_available": bool(candidate["available"]),
                "unavailable_reasons": candidate["reasons"],
                "p_raw": candidate["p_lift"],
            }
        )

    internal = np.array(
        [1.0 if item["p_raw"] is None else float(item["p_raw"]) for item in members],
        dtype=np.float64,
    )
    adjusted = _holm(internal, [item["member_id"] for item in members])
    for index, item in enumerate(members):
        item["p_holm_internal"] = float(adjusted[index])
        if item["inference_available"]:
            item["p_holm"] = float(adjusted[index])
            item["nominal_reject_holm"] = bool(adjusted[index] <= ALPHA)
            item["nominal_reject_raw"] = bool(item["p_raw"] <= ALPHA)
        else:
            item["p_holm"] = None
            item["nominal_reject_holm"] = None
            item["nominal_reject_raw"] = None
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "method": CANDIDATE_METHOD_ID,
        "scope": "experimental_research_candidate",
        "alpha": ALPHA,
        "confidence_level": CONFIDENCE_LEVEL,
        "family_size": len(members),
        "members": members,
        "note": (
            "Holm uses the candidate's own p-values over the entire unchanged declared family, "
            "with p=1 kept internally for unavailable members. No original bootstrap p-value "
            "enters this family-wise result, and no seed or resample count governs this method."
        ),
    }
