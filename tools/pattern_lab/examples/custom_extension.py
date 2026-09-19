"""A complete trusted Pattern Lab extension, used by the example script and tests.

It demonstrates the four contracts an agent can extend without editing any core
dispatch: a causal feature, a hypothesis built on it, an axis-free evaluation
model that declares its own outcome, and a descriptive summary metric.

The module is loaded by digest-verified path and must expose ``register``.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.study import contracts
from tools.pattern_lab.study.contracts import (
    Anchors,
    BarSeries,
    ConditionValue,
    FeatureDescriptor,
    FeatureRequest,
    FeatureValue,
    HypothesisDescriptor,
    MetricDescriptor,
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
)

SMA_FEATURE_ID = "example_sma"
CLOSE_ABOVE_SMA_ID = "example_close_above_sma"
OPEN_GAP_MODEL_ID = "example_next_open_gap"
POSITIVE_SHARE_METRIC_ID = "example_positive_net_share"


def _period(parameters: Mapping[str, Any]) -> dict[str, Any]:
    contracts.closed_keys(parameters, ("period",), "parameters")
    if "period" not in parameters:
        raise PatternLabDataError("parameters.period: an explicit integer period is required.")
    value = parameters["period"]
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PatternLabDataError(f"parameters.period: expected a positive integer, got {value!r}.")
    return {"period": value}


def _contiguity_prefix(series: BarSeries) -> np.ndarray:
    return np.concatenate(([0], np.cumsum(series.contiguous_with_previous().astype(np.int64))))


def _sma(series: BarSeries, parameters: Mapping[str, Any], features) -> FeatureValue:
    """A causal simple moving average that re-warms at every segment break.

    The anchor is explicit: a value exists only where the whole window is one
    contiguous run of observed bars, so a lookback never crosses a gap and the
    series does not restart at the first research row.
    """
    period = int(parameters["period"])
    rows = series.row_count
    values = np.full(rows, np.nan, dtype=np.float64)
    valid = np.zeros(rows, dtype=bool)
    if rows >= period:
        windows = np.lib.stride_tricks.sliding_window_view(series.close, period)
        starts = np.arange(0, rows - period + 1)
        prefix = _contiguity_prefix(series)
        whole_window = prefix[starts + period] - prefix[starts + 1] == (period - 1)
        values[starts + period - 1] = windows.mean(axis=1)
        valid[starts + period - 1] = whole_window
    return FeatureValue(values=values, valid=valid)


SMA_DESCRIPTOR = FeatureDescriptor(
    feature_id=SMA_FEATURE_ID,
    version="1",
    evaluate=_sma,
    validate_parameters=_period,
    prior_bars=lambda parameters: int(parameters["period"]) - 1,
    description="Causal simple moving average of the close over one contiguous window.",
    initialization="No seed value: the first value exists only once a whole contiguous window exists.",
)


def _close_above_sma(series: BarSeries, parameters: Mapping[str, Any], features) -> ConditionValue:
    average = features[FeatureRequest(SMA_FEATURE_ID, {"period": int(parameters["period"])}).key]
    value = np.zeros(series.row_count, dtype=bool)
    np.greater(series.close, average.values, out=value, where=average.valid)
    return ConditionValue(value=value & average.valid, valid=average.valid)


CLOSE_ABOVE_SMA_DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id=CLOSE_ABOVE_SMA_ID,
    version="1",
    evaluate=_close_above_sma,
    validate_parameters=_period,
    dependencies=lambda parameters: (
        FeatureRequest(SMA_FEATURE_ID, {"period": int(parameters["period"])}),
    ),
    prior_bars=lambda parameters: int(parameters["period"]) - 1,
    description="The close is strictly above its own causal SMA.",
)


OPEN_GAP_OUTCOME = OutcomeSpec(
    "open_gap", "fraction", "Signed (next open - anchor close) / anchor close at the signal time."
)


def _validate_gap_settings(settings: Mapping[str, Any], timeframes) -> dict[str, Any]:
    contracts.closed_keys(settings, (), "settings")
    return {}


def _resolve_gap_cases(settings: Mapping[str, Any], timeframe_minutes: int) -> tuple[ModelCase, ...]:
    """One axis-free case: this model has no direction and no horizon."""
    return (
        ModelCase(
            case_id=f"tf{timeframe_minutes}m.axis_free",
            timeframe_minutes=int(timeframe_minutes),
            parameters={},
            outcomes=(OPEN_GAP_OUTCOME,),
            primary=True,
        ),
    )


def _evaluate_gap(series: BarSeries, settings: Mapping[str, Any], anchors: Anchors) -> ModelEvidence:
    rows = anchors.rows
    count = int(rows.size)
    total = series.row_count
    following = rows + 1
    safe = np.minimum(following, max(total - 1, 0))
    present = (following < total) & (series.slots[safe] - series.slots[rows] == 1)
    gap = np.full(count, np.nan, dtype=np.float64)
    if present.any():
        anchor_close = series.close[rows[present]]
        gap[present] = series.open[following[present]] / anchor_close - 1.0
    reason = np.where(present, "available", "missing_next_bar").astype(object)
    case_id = f"tf{series.timeframe_minutes}m.axis_free"
    return ModelEvidence(
        kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
        rows={
            "case_id": np.full(count, case_id, dtype=object),
            "anchor_open_ms": anchors.open_ms.astype(np.int64),
            "open_gap": gap,
            "open_gap__reason": reason,
        },
    )


OPEN_GAP_DESCRIPTOR = ModelDescriptor(
    model_id=OPEN_GAP_MODEL_ID,
    version="1",
    validate_settings=_validate_gap_settings,
    resolve_cases=_resolve_gap_cases,
    evaluate=_evaluate_gap,
    evidence_kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
    description="An axis-free model: one case per timeframe, with no direction and no horizon.",
)


def _positive_share(frame) -> float | None:
    values = frame["net_return"].to_numpy(dtype=np.float64)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return None
    return float(np.count_nonzero(values > 0.0) / values.size)


POSITIVE_SHARE_DESCRIPTOR = MetricDescriptor(
    metric_id=POSITIVE_SHARE_METRIC_ID,
    version="1",
    required_columns=("net_return",),
    unit="fraction",
    compute=_positive_share,
    description="Share of valid net returns strictly above zero. Descriptive only.",
)


def register(context) -> None:
    """Explicit registration; nothing is discovered and nothing is replaced."""
    context.register_feature(SMA_DESCRIPTOR)
    context.register_hypothesis(CLOSE_ABOVE_SMA_DESCRIPTOR)
    context.register_model(OPEN_GAP_DESCRIPTOR)
    context.register_metric(POSITIVE_SHARE_DESCRIPTOR)
