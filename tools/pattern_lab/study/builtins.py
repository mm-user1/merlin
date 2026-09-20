"""Built-in hypotheses and the first evaluation model.

The built-in conditions are the project's two-green-candles example and its
plain parent without the rising-quote-volume filter, so an analysis can compare
a filtered child with the inclusive parent it came from.  The built-in model
measures a fixed horizon and the price path that follows an anchor bar; it owns
its own direction and per-timeframe horizon axes, and stores one
direction-independent primitive row per anchor and horizon.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from ..manifest import require_int, require_text
from . import contracts
from .contracts import (
    Anchors,
    BarSeries,
    ConditionValue,
    FeatureValue,
    HypothesisDescriptor,
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
)

TWO_GREEN_HYPOTHESIS_ID = "two_green_rising_quote_volume"
TWO_GREEN_PLAIN_HYPOTHESIS_ID = "two_green"
FIXED_HORIZON_MODEL_ID = "fixed_horizon_path"

# Versioned public evidence view: it derives directional outcomes from the saved
# direction-independent primitives and the frozen case settings.
EVIDENCE_VIEW_VERSION = 1

DIRECTIONS = ("long", "short")

REASON_AVAILABLE = "available"
REASON_TERMINAL = "terminal_study_end"
REASON_MISSING_ENTRY = "missing_entry_bar"
REASON_INCOMPLETE_PATH = "incomplete_path"
# Deterministic precedence when several causes apply: the declared study
# boundary is known before any data, a missing entry bar makes the whole outcome
# meaningless, and only then is an interrupted path reported.
REASON_PRECEDENCE = (REASON_TERMINAL, REASON_MISSING_ENTRY, REASON_INCOMPLETE_PATH)

FIXED_HORIZON_OUTCOMES = (
    OutcomeSpec("gross_return", "fraction", "Direction-signed exit-over-entry return before costs."),
    OutcomeSpec("commission_return", "fraction", "Entry plus exit commission as a fraction of entry notional."),
    OutcomeSpec("net_return", "fraction", "gross_return minus commission_return."),
    OutcomeSpec("mfe", "fraction", "Maximum favorable excursion of the path, gross of fees."),
    OutcomeSpec("mae", "fraction", "Maximum adverse excursion of the path, gross of fees."),
)

PRIMITIVE_COLUMNS = (
    "anchor_open_ms",
    "signal_time_ms",
    "horizon_minutes",
    "entry_time_ms",
    "exit_time_ms",
    "entry_price",
    "exit_price",
    "path_high",
    "path_low",
    "return_valid",
    "return_reason",
    "path_valid",
    "path_reason",
)


# --------------------------------------------------------------------------
# built-in hypothesis
# --------------------------------------------------------------------------

def _two_green_evaluate(
    series: BarSeries, parameters: Mapping[str, Any], features: Mapping[str, FeatureValue]
) -> ConditionValue:
    """``close[i-1] > open[i-1] and close[i] > open[i] and vq[i] > vq[i-1]``.

    Both candles must be contiguous and valid, so the first observation and every
    bar after a gap are unknown rather than false.  Equal open/close is not
    green and equal quote volume does not rise.  This is *at least two*: a longer
    green run may qualify on several closes.
    """
    rows = series.row_count
    value = np.zeros(rows, dtype=bool)
    valid = np.zeros(rows, dtype=bool)
    if rows > 1:
        contiguous = series.contiguous_with_previous()[1:]
        previous_green = series.close[:-1] > series.open[:-1]
        current_green = series.close[1:] > series.open[1:]
        rising_volume = series.volume_quote[1:] > series.volume_quote[:-1]
        valid[1:] = contiguous
        value[1:] = contiguous & previous_green & current_green & rising_volume
    return ConditionValue(value=value, valid=valid)


TWO_GREEN_DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id=TWO_GREEN_HYPOTHESIS_ID,
    version="1",
    evaluate=_two_green_evaluate,
    prior_bars=lambda parameters: 1,
    description=(
        "At least two consecutive green candles whose quote volume rose on the second. "
        "Requires one contiguous prior observation bar."
    ),
)


def _two_green_plain_evaluate(
    series: BarSeries, parameters: Mapping[str, Any], features: Mapping[str, FeatureValue]
) -> ConditionValue:
    """``close[i-1] > open[i-1] and close[i] > open[i]``, without a volume filter.

    Same contiguity, validity and prior-bar requirement as
    :data:`TWO_GREEN_DESCRIPTOR`; only the rising-quote-volume term is dropped,
    so this condition is an inclusive parent of that one on common support.
    """
    rows = series.row_count
    value = np.zeros(rows, dtype=bool)
    valid = np.zeros(rows, dtype=bool)
    if rows > 1:
        contiguous = series.contiguous_with_previous()[1:]
        previous_green = series.close[:-1] > series.open[:-1]
        current_green = series.close[1:] > series.open[1:]
        valid[1:] = contiguous
        value[1:] = contiguous & previous_green & current_green
    return ConditionValue(value=value, valid=valid)


TWO_GREEN_PLAIN_DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id=TWO_GREEN_PLAIN_HYPOTHESIS_ID,
    version="1",
    evaluate=_two_green_plain_evaluate,
    prior_bars=lambda parameters: 1,
    description=(
        "At least two consecutive green candles, with no quote-volume filter. "
        "Requires one contiguous prior observation bar."
    ),
)


# --------------------------------------------------------------------------
# built-in fixed-horizon and path model
# --------------------------------------------------------------------------

def _validate_directions(raw: Any) -> list[str]:
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError(
            f"settings.directions: expected a list of directions, got {type(raw).__name__}."
        )
    if not raw:
        raise PatternLabDataError("settings.directions: at least one direction is required.")
    seen: list[str] = []
    for index, item in enumerate(raw):
        text = require_text(item, f"settings.directions[{index}]")
        if text not in DIRECTIONS:
            raise PatternLabDataError(
                f"settings.directions[{index}]: unknown direction {text!r}; supported: {list(DIRECTIONS)}."
            )
        if text in seen:
            raise PatternLabDataError(f"settings.directions[{index}]: duplicate direction {text!r}.")
        seen.append(text)
    # Canonical order, so two spellings of the same semantic family are one family.
    return [item for item in DIRECTIONS if item in seen]


def _validate_horizons(raw: Any, timeframe: int, where: str) -> list[int]:
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError(f"{where}: expected a list of horizons, got {type(raw).__name__}.")
    if not raw:
        raise PatternLabDataError(f"{where}: at least one horizon is required.")
    horizons: list[int] = []
    for index, item in enumerate(raw):
        minutes = require_int(item, f"{where}[{index}]", minimum=1)
        if minutes % timeframe:
            raise PatternLabDataError(
                f"{where}[{index}]: {minutes} minutes is not a positive integer multiple of this "
                f"entry's own {timeframe}m observation timeframe."
            )
        if minutes in horizons:
            raise PatternLabDataError(f"{where}[{index}]: duplicate horizon {minutes}.")
        horizons.append(minutes)
    return sorted(horizons)


def validate_fixed_horizon_settings(
    settings: Mapping[str, Any], timeframes: Sequence[int]
) -> dict[str, Any]:
    """Validate the built-in model's own settings against the selected timeframes."""
    values = contracts.require_mapping(settings, "settings")
    contracts.closed_keys(values, ("directions", "commission_pct_per_side", "by_timeframe"), "settings")
    for key in ("directions", "commission_pct_per_side", "by_timeframe"):
        if key not in values:
            raise PatternLabDataError(f"settings.{key}: this model requires an explicit value.")
    directions = _validate_directions(values["directions"])
    commission = contracts.require_number(
        values["commission_pct_per_side"], "settings.commission_pct_per_side", minimum=0.0
    )
    by_timeframe = contracts.require_mapping(values["by_timeframe"], "settings.by_timeframe")
    expected = {str(int(item)) for item in timeframes}
    missing = sorted(expected - set(by_timeframe))
    extra = sorted(set(by_timeframe) - expected)
    if missing or extra:
        raise PatternLabDataError(
            "settings.by_timeframe: one entry is required for every selected observation timeframe; "
            f"missing {missing}, unexpected {extra}."
        )
    resolved: dict[str, Any] = {}
    for key in sorted(expected, key=int):
        where = f"settings.by_timeframe[{key!r}]"
        entry = contracts.require_mapping(by_timeframe[key], where)
        contracts.closed_keys(entry, ("horizons_minutes", "primary_horizon_minutes"), where)
        for name in ("horizons_minutes", "primary_horizon_minutes"):
            if name not in entry:
                raise PatternLabDataError(f"{where}.{name}: an explicit value is required.")
        timeframe = int(key)
        horizons = _validate_horizons(entry["horizons_minutes"], timeframe, f"{where}.horizons_minutes")
        primary = require_int(entry["primary_horizon_minutes"], f"{where}.primary_horizon_minutes", minimum=1)
        if primary not in horizons:
            raise PatternLabDataError(
                f"{where}.primary_horizon_minutes: {primary} is not one of this timeframe's declared "
                f"horizons {horizons}."
            )
        resolved[key] = {"horizons_minutes": horizons, "primary_horizon_minutes": primary}
    return {
        "directions": directions,
        "commission_pct_per_side": commission,
        "by_timeframe": resolved,
    }


def resolve_fixed_horizon_cases(settings: Mapping[str, Any], timeframe_minutes: int) -> tuple[ModelCase, ...]:
    """Resolve one logical case per timeframe, horizon and direction."""
    entry = settings["by_timeframe"][str(int(timeframe_minutes))]
    commission = settings["commission_pct_per_side"]
    cases: list[ModelCase] = []
    for horizon in entry["horizons_minutes"]:
        for direction in settings["directions"]:
            cases.append(
                ModelCase(
                    case_id=f"tf{timeframe_minutes}m.h{horizon}m.{direction}",
                    timeframe_minutes=int(timeframe_minutes),
                    parameters={
                        "direction": direction,
                        "horizon_minutes": int(horizon),
                        "commission_pct_per_side": commission,
                    },
                    outcomes=FIXED_HORIZON_OUTCOMES,
                    primary=horizon == entry["primary_horizon_minutes"],
                )
            )
    return tuple(cases)


def _window_extremes(values: np.ndarray, length: int, reducer) -> np.ndarray:
    """Return ``reducer`` of every ``length``-long window, indexed by its start."""
    windows = np.lib.stride_tricks.sliding_window_view(values, length)
    return reducer(windows, axis=1)


def evaluate_fixed_horizon(series: BarSeries, settings: Mapping[str, Any], anchors: Anchors) -> ModelEvidence:
    """Measure entry, exit and path extremes for every anchor and horizon.

    Entry is ``open[i+1]`` at the anchor's close, the exit is ``close[i+k]`` and
    the path is bars ``i+1..i+k`` inclusive, so the signal bar is excluded.  A
    horizon may not cross a gap or the study end; validity is per horizon, and
    shorter horizons are never trimmed to the longest one's support.
    """
    entry = settings["by_timeframe"][str(int(series.timeframe_minutes))]
    step_ms = series.step_ms
    rows = anchors.rows
    count = int(rows.size)
    total_rows = series.row_count
    slots = series.slots
    columns: dict[str, list[np.ndarray]] = {name: [] for name in PRIMITIVE_COLUMNS}

    for horizon in entry["horizons_minutes"]:
        bars = horizon // series.timeframe_minutes
        anchor_open = anchors.open_ms
        exit_time = anchor_open + (bars + 1) * step_ms
        terminal = exit_time > anchors.study_end_ms

        entry_rows = rows + 1
        safe_entry = np.minimum(entry_rows, max(total_rows - 1, 0))
        entry_present = (entry_rows < total_rows) & (slots[safe_entry] - slots[rows] == 1)
        exit_rows = rows + bars
        safe_exit = np.minimum(exit_rows, max(total_rows - 1, 0))
        path_present = entry_present & (exit_rows < total_rows) & (slots[safe_exit] - slots[rows] == bars)

        available = path_present & ~terminal
        reason = np.full(count, REASON_AVAILABLE, dtype=object)
        reason[entry_present & ~path_present] = REASON_INCOMPLETE_PATH
        reason[~entry_present] = REASON_MISSING_ENTRY
        reason[terminal] = REASON_TERMINAL

        entry_price = np.full(count, np.nan)
        known_entry = entry_present
        if known_entry.any():
            entry_price[known_entry] = series.open[entry_rows[known_entry]]
        exit_price = np.full(count, np.nan)
        path_high = np.full(count, np.nan)
        path_low = np.full(count, np.nan)
        if available.any():
            selected = rows[available]
            exit_price[available] = series.close[selected + bars]
            highs = _window_extremes(series.high, bars, np.max)
            lows = _window_extremes(series.low, bars, np.min)
            path_high[available] = highs[selected + 1]
            path_low[available] = lows[selected + 1]

        columns["anchor_open_ms"].append(anchor_open.astype(np.int64))
        columns["signal_time_ms"].append((anchor_open + step_ms).astype(np.int64))
        columns["horizon_minutes"].append(np.full(count, int(horizon), dtype=np.int64))
        columns["entry_time_ms"].append((anchor_open + step_ms).astype(np.int64))
        columns["exit_time_ms"].append(exit_time.astype(np.int64))
        columns["entry_price"].append(entry_price)
        columns["exit_price"].append(exit_price)
        columns["path_high"].append(path_high)
        columns["path_low"].append(path_low)
        columns["return_valid"].append(available)
        columns["return_reason"].append(reason)
        # The built-in currently requires the same contiguous path for the return
        # and for the excursions; they are still stored and reported separately.
        columns["path_valid"].append(available.copy())
        columns["path_reason"].append(reason.copy())

    empty_int = np.zeros(0, dtype=np.int64)
    rows_out = {
        name: (np.concatenate(values) if values else (empty_int if name.endswith("_ms") or name == "horizon_minutes" else np.zeros(0)))
        for name, values in columns.items()
    }
    return ModelEvidence(kind=contracts.FIXED_HORIZON_EVIDENCE_KIND, rows=rows_out)


FIXED_HORIZON_DESCRIPTOR = ModelDescriptor(
    model_id=FIXED_HORIZON_MODEL_ID,
    version="1",
    validate_settings=validate_fixed_horizon_settings,
    resolve_cases=resolve_fixed_horizon_cases,
    evaluate=evaluate_fixed_horizon,
    evidence_kind=contracts.FIXED_HORIZON_EVIDENCE_KIND,
    description=(
        "Fixed-horizon return and path excursions: notional entry at the next open, exit at the "
        "close of the horizon's last bar, MFE/MAE over the same path."
    ),
)


# --------------------------------------------------------------------------
# the versioned public evidence view
# --------------------------------------------------------------------------

def expand_fixed_horizon_case(primitives: pd.DataFrame, case: ModelCase) -> pd.DataFrame:
    """Derive one case's directional observations from the saved primitives.

    This reads no market data and executes no hypothesis: it applies the frozen
    case settings to the stored ``P``/``X``/``U``/``L`` primitives exactly as the
    specification's formulas require.  Short net returns are computed from the
    short formulas, never as the negated long net return.
    """
    horizon = int(case.parameters["horizon_minutes"])
    direction = str(case.parameters["direction"])
    commission_pct = float(case.parameters["commission_pct_per_side"])
    frame = primitives.loc[primitives["horizon_minutes"] == horizon].reset_index(drop=True)

    entry = frame["entry_price"].to_numpy(dtype=np.float64)
    exit_price = frame["exit_price"].to_numpy(dtype=np.float64)
    high = frame["path_high"].to_numpy(dtype=np.float64)
    low = frame["path_low"].to_numpy(dtype=np.float64)
    return_valid = frame["return_valid"].to_numpy(dtype=bool)
    path_valid = frame["path_valid"].to_numpy(dtype=bool)

    sign = 1.0 if direction == "long" else -1.0
    rate = commission_pct / 100.0
    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = exit_price / entry
        gross = sign * (ratio - 1.0)
        commission = rate * (1.0 + ratio)
        upper = high / entry - 1.0
        lower = 1.0 - low / entry
    net = gross - commission

    if direction == "long":
        favorable, adverse = upper, lower
    else:
        favorable, adverse = lower, upper
    mfe = np.maximum(0.0, favorable)
    mae = np.maximum(0.0, adverse)

    invalid_return = ~return_valid
    for values in (gross, commission, net):
        values[invalid_return] = np.nan
    invalid_path = ~path_valid
    mfe[invalid_path] = np.nan
    mae[invalid_path] = np.nan

    result = pd.DataFrame(
        {
            "case_id": case.case_id,
            "direction": direction,
            "horizon_minutes": horizon,
            "anchor_open_ms": frame["anchor_open_ms"].to_numpy(dtype=np.int64),
            "signal_time_ms": frame["signal_time_ms"].to_numpy(dtype=np.int64),
            "entry_time_ms": frame["entry_time_ms"].to_numpy(dtype=np.int64),
            "exit_time_ms": frame["exit_time_ms"].to_numpy(dtype=np.int64),
            "entry_price": entry,
            "exit_price": exit_price,
            "path_high": high,
            "path_low": low,
            "gross_return": gross,
            "commission_return": commission,
            "net_return": net,
            "mfe": mfe,
            "mae": mae,
            "return_valid": return_valid,
            "return_reason": frame["return_reason"].to_numpy(),
            "path_valid": path_valid,
            "path_reason": frame["path_reason"].to_numpy(),
        }
    )
    for column in ("instrument_id", "timeframe_minutes", "model_instance_id"):
        if column in frame.columns:
            result.insert(0, column, frame[column].to_numpy())
    return result


def register_builtins() -> None:
    """Register the built-in hypothesis and model exactly once."""
    if TWO_GREEN_HYPOTHESIS_ID not in contracts.registered("hypothesis"):
        contracts.register_hypothesis(TWO_GREEN_DESCRIPTOR, builtin=True)
    if TWO_GREEN_PLAIN_HYPOTHESIS_ID not in contracts.registered("hypothesis"):
        contracts.register_hypothesis(TWO_GREEN_PLAIN_DESCRIPTOR, builtin=True)
    if FIXED_HORIZON_MODEL_ID not in contracts.registered("model"):
        contracts.register_model(FIXED_HORIZON_DESCRIPTOR, builtin=True)
