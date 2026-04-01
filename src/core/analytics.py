"""Portfolio equity aggregation utilities for Analytics page."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence


SECONDS_PER_DAY = 86400.0
SHORT_SPAN_DAYS = 30.0
RETURN_PROFILE_STEM_LIMIT = 60

WARNING_NO_VALID_DATA = "No valid equity data found for selected studies."
WARNING_NO_OVERLAP = "Selected studies have no overlapping time period."


@dataclass(frozen=True)
class _StudyCurve:
    timestamps: List[datetime]
    values: List[float]


def _empty_return_profile() -> Dict[str, Any]:
    return {
        "stems": [],
        "source_count": 0,
        "display_count": 0,
        "is_binned": False,
    }


def _empty_result(
    *,
    warning: Optional[str],
    studies_used: int,
    studies_excluded: int,
) -> Dict[str, Any]:
    return {
        "curve": None,
        "timestamps": None,
        "profit_pct": None,
        "max_drawdown_pct": None,
        "ann_profit_pct": None,
        "overlap_days": 0,
        "overlap_days_exact": 0.0,
        "studies_used": studies_used,
        "studies_excluded": studies_excluded,
        "return_profile": _empty_return_profile(),
        "warning": warning,
    }


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _normalize_study_curve(study: Dict[str, Any]) -> Optional[_StudyCurve]:
    raw_curve = study.get("equity_curve", [])
    raw_timestamps = study.get("timestamps", [])
    if not isinstance(raw_curve, list) or not isinstance(raw_timestamps, list):
        return None
    if len(raw_curve) < 2 or len(raw_curve) != len(raw_timestamps):
        return None

    pairs: List[tuple[datetime, float]] = []
    for ts_raw, value_raw in zip(raw_timestamps, raw_curve):
        ts = _parse_timestamp(ts_raw)
        if ts is None:
            return None
        try:
            value = float(value_raw)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(value):
            return None
        pairs.append((ts, value))

    if len(pairs) < 2:
        return None

    pairs.sort(key=lambda item: item[0])
    # If two points share the same timestamp, keep the latest value.
    deduped: List[tuple[datetime, float]] = []
    for ts, value in pairs:
        if deduped and ts == deduped[-1][0]:
            deduped[-1] = (ts, value)
        else:
            deduped.append((ts, value))

    if len(deduped) < 2:
        return None

    return _StudyCurve(
        timestamps=[ts for ts, _ in deduped],
        values=[value for _, value in deduped],
    )


def _build_time_grid(
    studies: Sequence[_StudyCurve],
    t_start: datetime,
    t_end: datetime,
) -> List[datetime]:
    values = {t_start, t_end}
    for study in studies:
        for ts in study.timestamps:
            if t_start <= ts <= t_end:
                values.add(ts)
    return sorted(values)


def _forward_fill_values(
    source_timestamps: Sequence[datetime],
    source_values: Sequence[float],
    target_timestamps: Sequence[datetime],
) -> List[float]:
    result: List[float] = []
    src_idx = 0
    src_last = len(source_timestamps) - 1

    for target in target_timestamps:
        while src_idx < src_last and source_timestamps[src_idx + 1] <= target:
            src_idx += 1
        result.append(source_values[src_idx])

    return result


def _compute_max_drawdown(equity_curve: Sequence[float]) -> float:
    if not equity_curve:
        return 0.0

    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        if peak > 0:
            drawdown = (peak - value) / peak * 100.0
            if drawdown > max_dd:
                max_dd = drawdown
    return max_dd


def _annualize_profit(profit_pct: float, span_days: float) -> Optional[float]:
    if not math.isfinite(profit_pct) or not math.isfinite(span_days):
        return None
    if span_days <= SHORT_SPAN_DAYS:
        return None

    return_multiple = 1.0 + (profit_pct / 100.0)
    if return_multiple <= 0:
        return None

    ann = (return_multiple ** (365.0 / span_days) - 1.0) * 100.0
    if not math.isfinite(ann):
        return None
    return ann


def _build_return_profile(values: Sequence[float], stem_limit: int = RETURN_PROFILE_STEM_LIMIT) -> Dict[str, Any]:
    finite_values = []
    for raw_value in values:
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            finite_values.append(value)

    if not finite_values:
        return _empty_return_profile()

    sorted_values = sorted(finite_values, reverse=True)
    normalized_limit = max(1, int(stem_limit))
    source_count = len(sorted_values)

    if source_count <= normalized_limit:
        stems = [round(value, 4) for value in sorted_values]
        return {
            "stems": stems,
            "source_count": source_count,
            "display_count": len(stems),
            "is_binned": False,
        }

    stems: List[float] = []
    for bucket_index in range(normalized_limit):
        start = math.floor(bucket_index * source_count / normalized_limit)
        end = math.floor((bucket_index + 1) * source_count / normalized_limit)
        if end <= start:
            end = min(source_count, start + 1)
        bucket = sorted_values[start:end]
        if not bucket:
            continue
        stems.append(round(sum(bucket) / len(bucket), 4))

    return {
        "stems": stems,
        "source_count": source_count,
        "display_count": len(stems),
        "is_binned": True,
    }


def aggregate_equity_curves(studies_data: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate study stitched equity curves into an equal-weight portfolio curve."""
    valid: List[_StudyCurve] = []
    excluded = 0

    for study in studies_data:
        normalized = _normalize_study_curve(study)
        if normalized is None:
            excluded += 1
            continue
        valid.append(normalized)

    if not valid:
        return _empty_result(
            warning=WARNING_NO_VALID_DATA,
            studies_used=0,
            studies_excluded=excluded,
        )

    t_start = max(study.timestamps[0] for study in valid)
    t_end = min(study.timestamps[-1] for study in valid)
    if t_start >= t_end:
        return _empty_result(
            warning=WARNING_NO_OVERLAP,
            studies_used=len(valid),
            studies_excluded=excluded,
        )

    time_grid = _build_time_grid(valid, t_start, t_end)
    if len(time_grid) < 2:
        return _empty_result(
            warning=WARNING_NO_OVERLAP,
            studies_used=len(valid),
            studies_excluded=excluded,
        )

    aligned_curves: List[List[float]] = []
    overlap_returns: List[float] = []
    for study in valid:
        filled = _forward_fill_values(study.timestamps, study.values, time_grid)
        start_value = filled[0]
        if start_value <= 0 or not math.isfinite(start_value):
            excluded += 1
            continue
        aligned = [value / start_value * 100.0 for value in filled]
        aligned_curves.append(aligned)
        overlap_returns.append((aligned[-1] / 100.0 - 1.0) * 100.0)

    if not aligned_curves:
        return _empty_result(
            warning=WARNING_NO_VALID_DATA,
            studies_used=0,
            studies_excluded=excluded,
        )

    grid_size = len(time_grid)
    curve_count = len(aligned_curves)
    portfolio = [
        round(sum(curve[idx] for curve in aligned_curves) / curve_count, 6)
        for idx in range(grid_size)
    ]

    profit_pct = (portfolio[-1] / 100.0 - 1.0) * 100.0
    max_drawdown_pct = _compute_max_drawdown(portfolio)

    span_days_exact = (t_end - t_start).total_seconds() / SECONDS_PER_DAY
    overlap_days = max(0, int(math.floor(span_days_exact)))
    ann_profit_pct = _annualize_profit(profit_pct, span_days_exact)

    warning: Optional[str] = None
    if span_days_exact <= SHORT_SPAN_DAYS:
        warning = (
            f"Short overlapping period ({int(round(span_days_exact))} days) - "
            "annualized metric is suppressed."
        )

    return {
        "curve": portfolio,
        "timestamps": [ts.isoformat() for ts in time_grid],
        "profit_pct": round(profit_pct, 4),
        "max_drawdown_pct": round(max_drawdown_pct, 4),
        "ann_profit_pct": round(ann_profit_pct, 2) if ann_profit_pct is not None else None,
        "overlap_days": overlap_days,
        "overlap_days_exact": round(span_days_exact, 6),
        "studies_used": len(aligned_curves),
        "studies_excluded": excluded,
        "return_profile": _build_return_profile(overlap_returns),
        "warning": warning,
    }
