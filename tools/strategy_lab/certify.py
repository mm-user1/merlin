"""Opt-in Phase 1-B real-pack structural and execution certification."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numba
import numpy as np
import pandas as pd

from .config import REPO_ROOT, canonical_json_bytes, load_run_spec, semantic_key_digest
from core.engine_v2.runner import run_v2_strategy  # noqa: E402
from core.grid_v2 import GridV2StrategyHooks, execute_grid_v2_candidates  # noqa: E402
from strategies import get_strategy  # noqa: E402

from .data_quality import (
    build_authoritative_windows,
    prepare_segment,
    validate_selected_sources,
    verify_source_preservation,
)
from .dataset import (
    METRIC_AXIS,
    SEGMENT_AXIS,
    DatasetError,
    project_candidates,
    result_row_values,
    segment_matrix,
)
from .generate import (
    REQUIRED_BACKEND_KIND,
    _execution_backend_facts,
    _quality_preflight,
    _require_compiled_backend_available,
    generate_dataset,
)
from .inventory import resolve_data_root


EXPECTED_SOURCE_COUNT = 118
EXPECTED_SEGMENT_COUNT = 1_888
EXPECTED_QUALITY_ROW_COUNT = 2_006
EXPECTED_CANDIDATE_COUNT = 480
REPRESENTATIVE_TICKER = "CRVUSDT"
REL_TOL = 1e-9
ABS_TOL = 1e-12
MISMATCH_SAMPLE_LIMIT = 5
WINDOW_NET_PROFIT_BASIS = "legacy_wfa_100"
LEGACY_BRACKET_IDENTITY = {
    "strategy_id": "s06_r_trend_v02_b2",
    "strategy_version": "v02-b2",
    "candidate_count": 480,
    "plan_fingerprint": "c0e40ede6521a1cc02063ef2c9245f58c0093ca97aeb4bd858b75b5d09c7f434",
    "semantic_key_digest": "60e563c74876258e52de4c4ff3b598ed3a3a12d55d640f52ce262cd6b543fb55",
    "target": "rr",
    "trail": "none",
}
SELECTED_TRIAL_NET_PROFIT_BASIS = "initial_capital_1000"
from .paths import PROTECTED_CANONICAL_OUTPUTS
LEGACY_METRIC_COUNT = 20
WINDOW_PINS = {
    1: (
        "2025-10-01T00:00:00Z",
        "2025-11-30T23:30:00Z",
        "2025-12-01T00:00:00Z",
        "2025-12-31T23:30:00Z",
    ),
    8: (
        "2026-05-01T00:00:00Z",
        "2026-06-30T23:30:00Z",
        "2026-07-01T00:00:00Z",
        "2026-07-31T23:30:00Z",
    ),
}


def _iso(value: Any) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise DatasetError(message)


def assert_certification_work_dir_allowed(work_dir: str | Path) -> Path:
    """Reject work directories that could overwrite either canonical dataset."""

    work = Path(work_dir).resolve()
    protected = next(
        (
            path
            for path in PROTECTED_CANONICAL_OUTPUTS
            if work == path or path in work.parents
        ),
        None,
    )
    if protected is not None:
        raise DatasetError(
            "certification work directory must not equal or be nested under "
            f"protected canonical output paths: {protected}."
        )
    return work


def finite_mtm_group_facts(
    group: np.ndarray,
    *,
    group_label: str,
    candidate_count: int = EXPECTED_CANDIDATE_COUNT,
) -> dict[str, int]:
    """Require at least one populated MTM value in a generated schema-v2 group."""

    expected_shape = (
        candidate_count,
        len(SEGMENT_AXIS),
        len(METRIC_AXIS),
    )
    if group.shape != expected_shape or group.dtype != np.float64:
        raise DatasetError(
            f"{group_label}: schema-v2 group shape/dtype must be "
            f"{expected_shape}/float64; observed {group.shape}/{group.dtype}."
        )
    column = group[:, :, METRIC_AXIS.index("max_drawdown_mtm_pct")]
    finite_count = int(np.isfinite(column).sum())
    if finite_count == 0:
        raise DatasetError(f"{group_label}: generated MTM column has no finite values.")
    return {"finite_mtm_count": finite_count, "mtm_value_count": int(column.size)}


def legacy_column_preservation_facts(
    schema_v2_group: np.ndarray,
    schema_v1_group: np.ndarray,
    *,
    schema_v2_label: str,
    schema_v1_label: str,
) -> dict[str, Any]:
    """Prove that schema-v2 preserves all immutable schema-v1 metric values."""

    expected_v2_shape = (
        EXPECTED_CANDIDATE_COUNT,
        len(SEGMENT_AXIS),
        len(METRIC_AXIS),
    )
    expected_v1_shape = (
        EXPECTED_CANDIDATE_COUNT,
        len(SEGMENT_AXIS),
        LEGACY_METRIC_COUNT,
    )
    for group, expected_shape, label in (
        (schema_v2_group, expected_v2_shape, schema_v2_label),
        (schema_v1_group, expected_v1_shape, schema_v1_label),
    ):
        if group.shape != expected_shape or group.dtype != np.float64:
            raise DatasetError(
                f"{label}: group shape/dtype must be {expected_shape}/float64; "
                f"observed {group.shape}/{group.dtype}."
            )

    legacy_v2 = schema_v2_group[:, :, :LEGACY_METRIC_COUNT]
    if not np.array_equal(legacy_v2, schema_v1_group, equal_nan=True):
        equal = (legacy_v2 == schema_v1_group) | (
            np.isnan(legacy_v2) & np.isnan(schema_v1_group)
        )
        mismatch_indices = np.argwhere(~equal)
        samples = []
        for candidate, segment, metric in mismatch_indices[:MISMATCH_SAMPLE_LIMIT]:
            samples.append(
                {
                    "candidate_id": int(candidate) + 1,
                    "segment": SEGMENT_AXIS[int(segment)],
                    "field": METRIC_AXIS[int(metric)],
                    "schema_v2": repr(float(legacy_v2[candidate, segment, metric])),
                    "schema_v1": repr(float(schema_v1_group[candidate, segment, metric])),
                }
            )
        raise DatasetError(
            f"legacy columns differ between {schema_v2_label} and {schema_v1_label}: "
            f"mismatch_count={len(mismatch_indices)}; "
            f"{_format_mismatch_samples(samples, mismatch_count=len(mismatch_indices))}."
        )
    return {
        "legacy_column_count": LEGACY_METRIC_COUNT,
        "compared_value_count": int(schema_v1_group.size),
        "mismatch_count": 0,
        "bitwise_equal_with_equal_nan": True,
        "schema_v2_label": schema_v2_label,
        "schema_v1_label": schema_v1_label,
    }


def semantic_float_equal(left: Any, right: Any) -> bool:
    """Compare finite values while treating every non-finite transport as unavailable."""

    left_unavailable = left is None or not math.isfinite(float(left))
    right_unavailable = right is None or not math.isfinite(float(right))
    if left_unavailable or right_unavailable:
        return left_unavailable and right_unavailable
    return math.isclose(float(left), float(right), rel_tol=REL_TOL, abs_tol=ABS_TOL)


def utc_timestamp(value: Any, field: str) -> pd.Timestamp:
    """Return one timezone-aware timestamp normalized to UTC."""

    if value is None:
        raise DatasetError(f"{field}: timestamp is unavailable.")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        raise DatasetError(f"{field}: invalid timestamp {value!r}.") from None
    if pd.isna(timestamp):
        raise DatasetError(f"{field}: timestamp is unavailable.")
    if timestamp.tzinfo is None:
        raise DatasetError(f"{field}: timestamp must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def utc_timestamps_equal(left: Any, right: Any, *, field: str) -> bool:
    return utc_timestamp(left, f"{field} stored") == utc_timestamp(
        right,
        f"{field} expected",
    )


def assert_path_contained(path: str | Path, root: str | Path, *, field: str) -> Path:
    resolved = Path(path).resolve()
    resolved_root = Path(root).resolve()
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError:
        raise DatasetError(
            f"{field}: {resolved} is not contained under {resolved_root}."
        ) from None
    return relative


def allowed_roots_with_data_root(
    roots: Sequence[str | Path],
    data_root: str | Path,
) -> list[Path]:
    """Return resolved allowed roots with the data root present exactly once."""

    updated = [Path(root) for root in roots]
    resolved = Path(data_root).resolve()
    if all(root.resolve() != resolved for root in updated):
        updated.append(resolved)
    return updated


def changed_snapshot_paths(
    before: Mapping[str, tuple[str, int, int]],
    after: Mapping[str, tuple[str, int, int]],
    *,
    label: str,
) -> list[str]:
    """Name protected snapshot entries whose digest, size, or mtime changed."""

    return [
        f"{label}:{path}"
        for path in sorted(set(before) | set(after))
        if before.get(path) != after.get(path)
    ]


def wfa_window_net_profit_from_lab(value: Any) -> float:
    """Convert the Lab 1000-capital percentage to legacy WFA window basis 100."""

    return 10.0 * float(value) + 900.0


def selected_trial_net_profit_from_lab(value: Any) -> float:
    """Keep the selected Grid trial on the run's initialCapital=1000 basis."""

    return float(value)


def select_primary_candidate(
    profits: Sequence[Any],
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    """Select by finite profit desc, semantic key asc, then candidate ID asc."""

    if len(profits) != len(candidates):
        raise DatasetError("primary selection values and candidate rows differ in length.")
    candidate_ids = [int(row["candidate_id"]) for row in candidates]
    if len(set(candidate_ids)) != len(candidate_ids):
        raise DatasetError("primary selection candidate IDs must be unique.")
    finite_indices = [
        index for index, value in enumerate(profits) if math.isfinite(float(value))
    ]
    if not finite_indices:
        raise DatasetError("primary selection has no finite Net Profit values.")
    ordered = sorted(
        finite_indices,
        key=lambda index: (
            -float(profits[index]),
            str(candidates[index]["semantic_key"]),
            int(candidates[index]["candidate_id"]),
        ),
    )
    selected_index = ordered[0]
    top_value = float(profits[selected_index])
    tie_count = sum(float(profits[index]) == top_value for index in finite_indices)
    return selected_index, tie_count


def _new_mismatch_counts() -> dict[str, int]:
    return {
        "identity_mismatch_count": 0,
        "availability_pattern_mismatch_count": 0,
        "exact_field_mismatch_count": 0,
        "floating_mismatch_count": 0,
    }


def _record_mismatch(
    counts: dict[str, int],
    samples: list[dict[str, Any]],
    *,
    category: str,
    candidate_id: int | None,
    segment: str | None = None,
    field: str | None = None,
    identity_category: str | None = None,
    row_position: int | None = None,
    reference_candidate_id: int | None = None,
) -> None:
    count_name = f"{category}_mismatch_count"
    counts[count_name] += 1
    if len(samples) >= MISMATCH_SAMPLE_LIMIT:
        return
    sample: dict[str, Any] = {
        "category": category,
        "candidate_id": candidate_id,
    }
    for name, value in (
        ("segment", segment),
        ("field", field),
        ("identity_category", identity_category),
        ("row_position", row_position),
        ("reference_candidate_id", reference_candidate_id),
    ):
        if value is not None:
            sample[name] = value
    samples.append(sample)


def _format_mismatch_samples(
    samples: Sequence[Mapping[str, Any]],
    *,
    mismatch_count: int,
) -> str:
    rendered = []
    for sample in samples:
        rendered.append(
            " ".join(f"{name}={value}" for name, value in sample.items())
        )
    omitted = max(0, mismatch_count - len(samples))
    return (
        "samples=["
        + "; ".join(rendered)
        + f"]; additional_mismatches_omitted={omitted}"
    )


def _raise_parity_mismatches(
    surface: str,
    evidence: Mapping[str, Any],
    samples: Sequence[Mapping[str, Any]],
) -> None:
    mismatch_count = int(evidence["mismatch_count"])
    if mismatch_count == 0:
        return
    counters = "; ".join(
        f"{name}={int(evidence[name])}"
        for name in (
            "identity_mismatch_count",
            "availability_pattern_mismatch_count",
            "exact_field_mismatch_count",
            "floating_mismatch_count",
            "mismatch_count",
        )
    )
    raise DatasetError(
        f"{surface} parity mismatch: {counters}; "
        f"{_format_mismatch_samples(samples, mismatch_count=mismatch_count)}."
    )


def _row_identity_categories(left: Any, right: Any) -> list[str]:
    pairs = (
        ("candidate_id", left.candidate_id, right.candidate_id),
        ("semantic_key", left.semantic_key, right.semantic_key),
        ("canonical_identity", left.canonical_identity, right.canonical_identity),
        ("variant_name", left.variant_name, right.variant_name),
        ("grid_mode_name", left.grid_mode_name, right.grid_mode_name),
        ("modes", dict(left.modes), dict(right.modes)),
        ("params", dict(left.params), dict(right.params)),
        ("status", left.status, right.status),
        ("error", left.error, right.error),
    )
    return [name for name, left_value, right_value in pairs if left_value != right_value]


def _row_count_mismatch_message(
    compiled_rows: Sequence[Any],
    reference_rows: Sequence[Any],
    plan: Any,
    *,
    expected_candidate_count: int,
    segment_name: str | None,
) -> str:
    expected_id_list = [
        int(plan.candidate_table.candidate_id_for_index(index))
        for index in range(plan.deduped_candidate_count)
    ]
    expected_ids = set(expected_id_list)
    expected_position_by_id = {
        candidate_id: position
        for position, candidate_id in enumerate(expected_id_list)
    }
    samples: list[dict[str, Any]] = []
    missing_total = 0
    for label, rows in (
        ("compiled_missing_row", compiled_rows),
        ("reference_missing_row", reference_rows),
    ):
        observed_ids = {int(row.candidate_id) for row in rows}
        for candidate_id in sorted(expected_ids - observed_ids):
            missing_total += 1
            if len(samples) < MISMATCH_SAMPLE_LIMIT:
                sample = {
                    "category": label,
                    "candidate_id": candidate_id,
                    "row_position": expected_position_by_id[candidate_id],
                }
                if segment_name is not None:
                    sample["segment"] = segment_name
                samples.append(sample)
    if not samples:
        missing_total = max(
            1,
            abs(len(compiled_rows) - expected_candidate_count)
            + abs(len(reference_rows) - expected_candidate_count),
        )
        sample = {
            "category": "row_count",
            "candidate_id": None,
            "row_position": min(len(compiled_rows), len(reference_rows)),
        }
        if segment_name is not None:
            sample["segment"] = segment_name
        samples.append(sample)
    return (
        "compiled/reference row count mismatch: "
        f"expected={expected_candidate_count}; compiled={len(compiled_rows)}; "
        f"reference={len(reference_rows)}; "
        f"{_format_mismatch_samples(samples, mismatch_count=missing_total)}."
    )


def _grid_run_parity_facts(
    compiled: Any,
    reference: Any,
    plan: Any,
    *,
    expected_candidate_count: int,
    segment_name: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Derive compiled/reference parity counters without asserting zero."""

    compiled_rows = tuple(compiled.rows)
    reference_rows = tuple(reference.rows)
    if not (
        len(compiled_rows) == len(reference_rows) == expected_candidate_count
    ):
        raise DatasetError(
            _row_count_mismatch_message(
                compiled_rows,
                reference_rows,
                plan,
                expected_candidate_count=expected_candidate_count,
                segment_name=segment_name,
            )
        )

    counts = _new_mismatch_counts()
    samples: list[dict[str, Any]] = []
    for position, (compiled_row, reference_row) in enumerate(
        zip(compiled_rows, reference_rows)
    ):
        identity_categories = _row_identity_categories(compiled_row, reference_row)
        if identity_categories:
            _record_mismatch(
                counts,
                samples,
                category="identity",
                candidate_id=int(compiled_row.candidate_id),
                segment=segment_name,
                identity_category=",".join(identity_categories),
                row_position=position,
                reference_candidate_id=int(reference_row.candidate_id),
            )

    left = np.stack([result_row_values(row) for row in compiled_rows])
    right = np.stack([result_row_values(row) for row in reference_rows])
    finite = np.isfinite(left) & np.isfinite(right)
    availability_mismatches = (
        (np.isfinite(left) != np.isfinite(right))
        | (np.isnan(left) != np.isnan(right))
        | (np.isposinf(left) != np.isposinf(right))
        | (np.isneginf(left) != np.isneginf(right))
    )
    for row_index, column in np.argwhere(availability_mismatches):
        _record_mismatch(
            counts,
            samples,
            category="availability_pattern",
            candidate_id=int(compiled_rows[int(row_index)].candidate_id),
            segment=segment_name,
            field=METRIC_AXIS[int(column)],
        )

    exact_names = {
        "total_trades",
        "winning_trades",
        "losing_trades",
        "max_consecutive_losses",
        "sharpe_daily_observations",
        "sharpe_daily_active_days",
        "rejected_fill_count",
        "zero_size_entry_count",
        "invalid_stop_distance_count",
        "flags",
        "max_drawdown_mtm_pct",
    }
    discrepancies: dict[str, dict[str, float | int]] = {}
    for column, name in enumerate(METRIC_AXIS):
        column_finite = finite[:, column]
        if name in exact_names:
            finite_indices = np.flatnonzero(column_finite)
            equal = left[column_finite, column] == right[column_finite, column]
            for row_index in finite_indices[~equal]:
                _record_mismatch(
                    counts,
                    samples,
                    category="exact_field",
                    candidate_id=int(compiled_rows[int(row_index)].candidate_id),
                    segment=segment_name,
                    field=name,
                )
        else:
            close = np.isclose(
                left[column_finite, column],
                right[column_finite, column],
                rtol=REL_TOL,
                atol=ABS_TOL,
            )
            finite_indices = np.flatnonzero(column_finite)
            for row_index in finite_indices[~close]:
                _record_mismatch(
                    counts,
                    samples,
                    category="floating",
                    candidate_id=int(compiled_rows[int(row_index)].candidate_id),
                    segment=segment_name,
                    field=name,
                )
        absolute = np.abs(left[column_finite, column] - right[column_finite, column])
        denominator = np.maximum(np.abs(right[column_finite, column]), ABS_TOL)
        relative = absolute / denominator
        discrepancies[name] = {
            "finite_pair_count": int(column_finite.sum()),
            "max_absolute": float(absolute.max()) if absolute.size else 0.0,
            "max_relative": float(relative.max()) if relative.size else 0.0,
        }
    mismatch_count = sum(counts.values())
    evidence = {
        "row_count": len(compiled_rows),
        **counts,
        "mismatch_count": mismatch_count,
        "discrepancies": discrepancies,
    }
    return evidence, samples


def _compare_grid_runs(
    compiled: Any,
    reference: Any,
    plan: Any,
    *,
    expected_candidate_count: int,
    segment_name: str | None = None,
) -> dict[str, Any]:
    _execution_backend_facts(compiled)
    _assert(
        reference.metadata.get("backend_kind") == "reference",
        "reference view did not use the reference backend.",
    )
    _assert(
        not compiled.selected and not reference.selected,
        "parity runs must not select Slow rows.",
    )
    evidence, samples = _grid_run_parity_facts(
        compiled,
        reference,
        plan,
        expected_candidate_count=expected_candidate_count,
        segment_name=segment_name,
    )
    _raise_parity_mismatches("compiled/reference", evidence, samples)
    return evidence


def candidate_identity_mappings(
    plan: Any,
    projection: Mapping[str, Any],
) -> tuple[dict[int, Mapping[str, Any]], dict[int, int]]:
    rows = projection.get("candidates")
    if not isinstance(rows, list) or not rows:
        raise DatasetError("candidate projection must contain candidate rows.")
    projected_by_id: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        candidate_id = int(row["candidate_id"])
        if candidate_id in projected_by_id:
            raise DatasetError(f"candidate projection contains duplicate ID {candidate_id}.")
        projected_by_id[candidate_id] = row

    plan_index_by_id: dict[int, int] = {}
    table = plan.candidate_table
    for index in range(plan.deduped_candidate_count):
        candidate_id = int(table.candidate_id_for_index(index))
        if candidate_id in plan_index_by_id:
            raise DatasetError(f"loaded plan contains duplicate candidate ID {candidate_id}.")
        plan_index_by_id[candidate_id] = index
    if set(projected_by_id) != set(plan_index_by_id):
        raise DatasetError("projected and loaded-plan candidate ID sets differ.")

    for candidate_id, index in plan_index_by_id.items():
        projected = projected_by_id[candidate_id]
        identity_pairs = (
            (projected["semantic_key"], table.semantic_key_for_index(index)),
            (projected["canonical_identity"], table.canonical_identity_for_index(index)),
            (projected["variant_name"], table.variant_name_for_index(index)),
            (projected["grid_mode_name"], table.grid_mode_name_for_index(index)),
            (projected["modes"], table.modes_for_index(index)),
            (projected["params"], table.params_for_index(index)),
        )
        if any(left != right for left, right in identity_pairs):
            raise DatasetError(
                f"candidate {candidate_id}: projected and loaded-plan identities differ."
            )
    return projected_by_id, plan_index_by_id


def geometry_candidate_ids(projection: Mapping[str, Any]) -> tuple[int, ...]:
    rows = projection["candidates"]
    if not isinstance(rows, list) or not rows:
        raise DatasetError("geometry selection requires candidate rows.")
    rows_by_id: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        candidate_id = int(row["candidate_id"])
        if candidate_id in rows_by_id:
            raise DatasetError(
                f"geometry selection contains duplicate candidate ID {candidate_id}."
            )
        rows_by_id[candidate_id] = row
    middle_right = len(rows) // 2
    middle_left = middle_right - 1
    selected = {
        int(rows[0]["candidate_id"]),
        int(rows[-1]["candidate_id"]),
        int(rows[middle_left]["candidate_id"]),
        int(rows[middle_right]["candidate_id"]),
    }
    axis_names = projection["global_axis_names"]
    for column, name in enumerate(axis_names):
        active = [row for row in rows if row["active_axis_mask"][column]]
        if not active:
            raise DatasetError(
                f"geometry selection axis {name!r} has no active candidate row."
            )
        codes = [int(row["axis_value_codes"][column]) for row in active]
        for boundary in (min(codes), max(codes)):
            selected.add(
                next(
                    int(row["candidate_id"])
                    for row in active
                    if int(row["axis_value_codes"][column]) == boundary
                )
            )
    missing = sorted(selected - set(rows_by_id))
    if missing:
        raise DatasetError(f"geometry-selected candidate IDs are missing: {missing}.")
    return tuple(sorted(selected))


def _slow_values(run: Any) -> dict[str, Any]:
    basic = run.basic_metrics
    advanced = run.advanced_metrics
    guardrails = asdict(run.guardrail_summary)
    return {
        "net_profit_pct": basic.net_profit_pct,
        "max_drawdown_pct": basic.max_drawdown_pct,
        "total_trades": basic.total_trades,
        "winning_trades": basic.winning_trades,
        "losing_trades": basic.losing_trades,
        "gross_profit": basic.gross_profit,
        "gross_loss": basic.gross_loss,
        "profit_factor": advanced.profit_factor,
        "win_rate_pct": basic.win_rate,
        "romad": advanced.romad,
        "max_consecutive_losses": basic.max_consecutive_losses,
        "sharpe_daily": advanced.sharpe_daily,
        "sharpe_daily_observations": advanced.sharpe_daily_observations,
        "sharpe_daily_active_days": advanced.sharpe_daily_active_days,
        "sqn": advanced.sqn,
        **{
            name: guardrails[name]
            for name in (
                "rejected_fill_count",
                "zero_size_entry_count",
                "invalid_stop_distance_count",
                "max_required_leverage",
                "flags",
            )
        },
        "max_drawdown_mtm_pct": run.max_drawdown_mtm_pct,
    }


def _selected_slow_row_mismatches(
    fast: Any,
    slow_values: Mapping[str, Any],
    projected: Mapping[str, Any],
    *,
    candidate_id: int,
    segment_name: str,
) -> tuple[
    dict[str, int],
    list[dict[str, Any]],
    dict[str, dict[str, float]],
]:
    """Derive mismatch facts for one typed-Slow/reference candidate segment."""

    integer_names = {
        "total_trades",
        "winning_trades",
        "losing_trades",
        "max_consecutive_losses",
        "sharpe_daily_observations",
        "sharpe_daily_active_days",
        "rejected_fill_count",
        "zero_size_entry_count",
        "invalid_stop_distance_count",
        "flags",
    }
    maximums = {
        name: {"max_absolute": 0.0, "max_relative": 0.0}
        for name in METRIC_AXIS
    }
    counts = _new_mismatch_counts()
    samples: list[dict[str, Any]] = []
    identity_pairs = (
        ("semantic_key", fast.semantic_key, projected["semantic_key"]),
        (
            "canonical_identity",
            fast.canonical_identity,
            projected["canonical_identity"],
        ),
        ("params", dict(fast.params), projected["params"]),
        ("modes", dict(fast.modes), projected["modes"]),
    )
    identity_categories = [
        name for name, fast_value, projected_value in identity_pairs
        if fast_value != projected_value
    ]
    if identity_categories:
        _record_mismatch(
            counts,
            samples,
            category="identity",
            candidate_id=candidate_id,
            segment=segment_name,
            identity_category=",".join(identity_categories),
        )

    for name in METRIC_AXIS:
        fast_value = (
            fast.guardrail_summary[name]
            if name
            in (
                "rejected_fill_count",
                "zero_size_entry_count",
                "invalid_stop_distance_count",
                "max_required_leverage",
                "flags",
            )
            else getattr(fast, name)
        )
        slow_value = slow_values[name]
        fast_unavailable = fast_value is None or not math.isfinite(float(fast_value))
        slow_unavailable = slow_value is None or not math.isfinite(float(slow_value))
        if fast_unavailable or slow_unavailable:
            if fast_unavailable != slow_unavailable:
                _record_mismatch(
                    counts,
                    samples,
                    category="availability_pattern",
                    candidate_id=candidate_id,
                    segment=segment_name,
                    field=name,
                )
            continue
        if name in integer_names:
            if fast_value != slow_value:
                _record_mismatch(
                    counts,
                    samples,
                    category="exact_field",
                    candidate_id=candidate_id,
                    segment=segment_name,
                    field=name,
                )
        elif not semantic_float_equal(fast_value, slow_value):
            _record_mismatch(
                counts,
                samples,
                category="floating",
                candidate_id=candidate_id,
                segment=segment_name,
                field=name,
            )
        if name not in integer_names:
            absolute = abs(float(fast_value) - float(slow_value))
            relative = absolute / max(abs(float(slow_value)), ABS_TOL)
            maximums[name]["max_absolute"] = absolute
            maximums[name]["max_relative"] = relative
    return counts, samples, maximums


def _selected_slow_parity(
    plan: Any,
    hooks: GridV2StrategyHooks,
    prepared_by_segment: Mapping[str, Any],
    reference_by_segment: Mapping[str, Any],
    projection: Mapping[str, Any],
    candidate_ids: Sequence[int],
) -> dict[str, Any]:
    maximums = {name: {"max_absolute": 0.0, "max_relative": 0.0} for name in METRIC_AXIS}
    projected_by_id, plan_index_by_id = candidate_identity_mappings(plan, projection)
    missing = sorted(set(candidate_ids) - set(plan_index_by_id))
    _assert(not missing, f"typed Slow candidate IDs are missing from the plan: {missing}.")
    counts = _new_mismatch_counts()
    samples: list[dict[str, Any]] = []
    comparisons = 0
    for segment_name in SEGMENT_AXIS:
        prepared = prepared_by_segment[segment_name]
        reference_rows = reference_by_segment[segment_name].rows
        reference_by_id: dict[int, Any] = {}
        for row in reference_rows:
            candidate_id = int(row.candidate_id)
            if candidate_id in reference_by_id:
                raise DatasetError(
                    f"typed Slow {segment_name} reference rows duplicate candidate "
                    f"ID {candidate_id}."
                )
            reference_by_id[candidate_id] = row
        for candidate_id in candidate_ids:
            index = plan_index_by_id[candidate_id]
            params = plan.candidate_table.params_for_index(index)
            data = hooks.build_execution_data(prepared.dataframe, params)
            slow = run_v2_strategy(
                data=data,
                profile=plan.profile,
                params=params,
                trade_start_idx=prepared.trade_start_idx,
                compute_sharpe_daily=True,
                compute_max_drawdown_mtm=True,
            )
            values = _slow_values(slow)
            fast = reference_by_id.get(candidate_id)
            if fast is None:
                raise DatasetError(
                    f"typed Slow {segment_name} reference rows omit candidate "
                    f"ID {candidate_id}."
                )
            projected = projected_by_id[candidate_id]
            row_counts, row_samples, row_maximums = _selected_slow_row_mismatches(
                fast,
                values,
                projected,
                candidate_id=candidate_id,
                segment_name=segment_name,
            )
            for name, count in row_counts.items():
                counts[name] += count
            samples.extend(row_samples[: MISMATCH_SAMPLE_LIMIT - len(samples)])
            for name, row_maximum in row_maximums.items():
                maximums[name]["max_absolute"] = max(
                    maximums[name]["max_absolute"],
                    row_maximum["max_absolute"],
                )
                maximums[name]["max_relative"] = max(
                    maximums[name]["max_relative"],
                    row_maximum["max_relative"],
                )
            comparisons += 1
    mismatch_count = sum(counts.values())
    evidence = {
        "candidate_ids": list(candidate_ids),
        "candidate_segment_comparisons": comparisons,
        **counts,
        "mismatch_count": mismatch_count,
        "discrepancies": maximums,
    }
    _raise_parity_mismatches("typed Slow/reference", evidence, samples)
    return evidence


def _thread_determinism(plan: Any, hooks: GridV2StrategyHooks, prepared: Any) -> dict[str, Any]:
    capacity = int(numba.config.NUMBA_NUM_THREADS)
    _assert(capacity >= 2, "Numba thread capacity is below two.")
    previous = numba.get_num_threads()
    runs = []
    observations = []
    try:
        for threads in (1, 2):
            numba.set_num_threads(threads)
            _assert(
                numba.get_num_threads() == threads,
                f"Numba did not apply {threads} thread(s).",
            )
            view = replace(plan, settings=replace(plan.settings, compiled_workers=threads))
            run = execute_grid_v2_candidates(
                view,
                prepared.dataframe,
                prepared.trade_start_idx,
                hooks,
                compute_sharpe=False,
                compute_sharpe_daily=True,
                compute_sqn=True,
                compute_max_drawdown_mtm=True,
            )
            _execution_backend_facts(run)
            _assert(
                run.metadata.get("compiled_workers") == threads,
                "compiled worker metadata mismatch.",
            )
            runs.append(segment_matrix(run.rows, view))
            observations.append(
                {
                    "runtime_threads": numba.get_num_threads(),
                    "compiled_workers": threads,
                }
            )
        bitwise_equal = bool(np.array_equal(runs[0], runs[1], equal_nan=True))
        _assert(bitwise_equal, "one-thread/two-thread results are not bitwise equal.")
    finally:
        numba.set_num_threads(previous)
    _assert(numba.get_num_threads() == previous, "Numba thread count was not restored.")
    return {
        "capacity": capacity,
        "original_threads": previous,
        "runs": observations,
        "bitwise_equal": bitwise_equal,
        "restored_threads": numba.get_num_threads(),
    }


def _snapshot_tree(root: Path) -> dict[str, tuple[str, int, int]]:
    result = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        result[path.relative_to(root).as_posix()] = (
            digest,
            path.stat().st_size,
            path.stat().st_mtime_ns,
        )
    return result


def _deterministic_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(manifest))
    provenance = value["provenance"]
    for name in ("host", "platform", "resolved_data_root", "source_mtime_ns", "timings"):
        provenance.pop(name, None)
    return value


def _run_smoke_process(
    run_spec: Path,
    data_root: Path,
    output: Path,
    repo_root: Path,
    work_dir: Path,
) -> None:
    environment = os.environ.copy()
    environment["NUMBA_CACHE_DIR"] = str(work_dir / "numba_cache")
    command = [
        sys.executable, "-m", "tools.strategy_lab.generate", str(run_spec),
        "--data-root", str(data_root), "--output-dir", str(output),
        "--ticker", REPRESENTATIVE_TICKER,
    ]
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    if completed.returncode != 0:
        raise DatasetError(f"real smoke process failed: {completed.stdout}\n{completed.stderr}")


def _smoke_gate(
    run_spec: Path,
    data_root: Path,
    work_dir: Path,
    repo_root: Path,
    *,
    candidate_count: int,
    window_count: int,
    compare_legacy_bracket: bool,
) -> dict[str, Any]:
    outputs = (work_dir / "smoke_one", work_dir / "smoke_two")
    _assert(all(not output.exists() for output in outputs), "smoke output paths must be fresh.")
    for output in outputs:
        _run_smoke_process(run_spec, data_root, output, repo_root, work_dir)
    manifests = [
        json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        for output in outputs
    ]
    finite_mtm = []
    for output, manifest in zip(outputs, manifests):
        _assert(manifest["scope"] == "smoke", "real smoke scope must be smoke.")
        _assert(
            len(manifest["groups"]) == window_count,
            "real smoke group count does not match the run spec.",
        )
        _assert(
            manifest["identity"]["candidate_count"] == candidate_count,
            "real smoke candidate count does not match the loaded plan.",
        )
        _assert(
            manifest["provenance"]["selected_slow_row_count"] == 0,
            "real smoke selected Slow rows must be zero.",
        )
        _assert(
            manifest["provenance"]["execution_backend"]["segment_execution_count"]
            == window_count * len(SEGMENT_AXIS),
            "real smoke segment execution count mismatch.",
        )
        for record in manifest["groups"]:
            _assert(
                record["shape"]
                == [candidate_count, len(SEGMENT_AXIS), len(METRIC_AXIS)]
                and record["dtype"] == "float64",
                "real smoke group shape/dtype mismatch.",
            )
            relative = str(record["path"])
            group = np.load(output / relative, allow_pickle=False)
            facts = finite_mtm_group_facts(
                group,
                group_label=relative,
                candidate_count=candidate_count,
            )
            finite_mtm.append({"output": str(output), "path": relative, **facts})
    deterministic_equal = bool(
        _deterministic_manifest(manifests[0])
        == _deterministic_manifest(manifests[1])
    )
    _assert(deterministic_equal, "fresh real smoke deterministic fields differ.")
    before = _snapshot_tree(outputs[0])
    result = generate_dataset(
        run_spec,
        data_root=data_root,
        output_dir=outputs[0],
        ticker_selectors=[REPRESENTATIVE_TICKER],
        repo_root=repo_root,
    )
    after = _snapshot_tree(outputs[0])
    immutable_no_op = bool(result.no_op and before == after)
    _assert(immutable_no_op, "real smoke rerun was not an immutable no-op.")
    legacy_columns: dict[str, Any]
    if compare_legacy_bracket:
        window_one_relative = f"groups/{REPRESENTATIVE_TICKER}/window_01.npy"
        schema_v2_path = outputs[0] / window_one_relative
        schema_v1_path = PROTECTED_CANONICAL_OUTPUTS[0] / window_one_relative
        _assert(
            schema_v1_path.is_file(),
            f"immutable schema-v1 comparison group is missing: {schema_v1_path}.",
        )
        legacy_columns = legacy_column_preservation_facts(
            np.load(schema_v2_path, allow_pickle=False),
            np.load(schema_v1_path, allow_pickle=False),
            schema_v2_label=str(schema_v2_path),
            schema_v1_label=str(schema_v1_path),
        )
    else:
        legacy_columns = {
            "status": "not_applicable",
            "reason": "loaded plan does not match the exact frozen legacy Bracket identity",
        }
    return {
        "outputs": [str(path) for path in outputs],
        "group_sha256": [
            [record["sha256"] for record in manifest["groups"]]
            for manifest in manifests
        ],
        "deterministic_equal": deterministic_equal,
        "excluded_provenance": [
            "timings",
            "host",
            "platform",
            "resolved_data_root",
            "source_mtime_ns",
        ],
        "immutable_no_op": immutable_no_op,
        "finite_mtm": finite_mtm,
        "legacy_columns": legacy_columns,
    }


def _matches_frozen_legacy_bracket_plan(spec: Any, plan: Any) -> bool:
    execution = spec.generation.get("execution", {})
    return bool(
        spec.strategy_id == LEGACY_BRACKET_IDENTITY["strategy_id"]
        and plan.strategy_version == LEGACY_BRACKET_IDENTITY["strategy_version"]
        and plan.deduped_candidate_count == LEGACY_BRACKET_IDENTITY["candidate_count"]
        and plan.plan_fingerprint == LEGACY_BRACKET_IDENTITY["plan_fingerprint"]
        and semantic_key_digest(plan) == LEGACY_BRACKET_IDENTITY["semantic_key_digest"]
        and execution.get("target") == LEGACY_BRACKET_IDENTITY["target"]
        and execution.get("trail") == LEGACY_BRACKET_IDENTITY["trail"]
    )


def certify_real_pack(
    run_spec_path: str | Path,
    *,
    data_root: str | Path,
    work_dir: str | Path,
    repo_root: str | Path = REPO_ROOT,
) -> Mapping[str, Any]:
    repo = Path(repo_root).resolve()
    run_spec_path = Path(run_spec_path).resolve()
    root = resolve_data_root(data_root)
    work = assert_certification_work_dir_allowed(work_dir)
    work.mkdir(parents=True, exist_ok=False)
    _require_compiled_backend_available()
    disk_free_bytes_before = shutil.disk_usage(work).free
    _assert(
        disk_free_bytes_before >= 1024 ** 3,
        "less than 1 GiB is free on the certification volume.",
    )

    spec = load_run_spec(run_spec_path, repo_root=repo)
    plan = spec.plan
    assert plan is not None and spec.inventory is not None
    timeframe_minutes = int(spec.generation["market_data"]["timeframe_minutes"])
    warmup_bars = int(spec.generation["windows"]["warmup_bars"])
    oos_period_months = int(spec.generation["windows"]["oos_period_months"])
    expected_window_count = int(
        spec.generation["windows"]["expected_window_count"]
    )
    candidate_count = int(plan.deduped_candidate_count)
    sources, raw_rows = validate_selected_sources(
        root,
        spec.inventory.entries,
        timeframe_minutes=timeframe_minutes,
    )
    windows = build_authoritative_windows(spec, sources)
    quality_rows = _quality_preflight(
        sources,
        raw_rows,
        windows,
        warmup_bars=warmup_bars,
        timeframe_minutes=timeframe_minutes,
        oos_period_months=oos_period_months,
    )
    _assert(len(sources) == EXPECTED_SOURCE_COUNT, "real source count mismatch.")
    _assert(
        len(windows) == expected_window_count,
        "authoritative window count does not match the run spec.",
    )
    _assert(
        (len(sources) * len(windows) * len(SEGMENT_AXIS))
        == EXPECTED_SEGMENT_COUNT,
        "prepared segment count mismatch.",
    )
    _assert(len(quality_rows) == EXPECTED_QUALITY_ROW_COUNT, "quality row count mismatch.")
    exchange_counts = {
        name: sum(1 for source in sources if source.entry["exchange"] == name)
        for name in ("OKX", "BYBIT")
    }
    _assert(exchange_counts == {"OKX": 110, "BYBIT": 8}, "exchange source counts mismatch.")
    cell_counts = {
        name: sum(1 for source in sources if source.entry["cell"] == name)
        for name in ("dev", "holdout")
    }
    _assert(cell_counts == {"dev": 24, "holdout": 94}, "inventory cell counts mismatch.")
    for window_id, pin in WINDOW_PINS.items():
        window = windows[window_id - 1]
        observed = tuple(
            _iso(value)
            for value in (
                window.is_start,
                window.is_end,
                window.oos_start,
                window.oos_end,
            )
        )
        _assert(observed == pin, f"Window {window_id} boundary pin mismatch.")
    preservation = verify_source_preservation(sources)

    source = next(
        source
        for source in sources
        if source.entry["canonical_symbol"] == REPRESENTATIVE_TICKER
    )
    _assert(source.entry["cell"] == "dev", "representative ticker is not in the development cell.")
    window = windows[0]
    prepared = {
        name: prepare_segment(
            source,
            window,
            name,
            warmup_bars=warmup_bars,
            timeframe_minutes=timeframe_minutes,
            oos_period_months=oos_period_months,
        )
        for name in SEGMENT_AXIS
    }
    _assert(
        all(item.trade_start_idx == warmup_bars for item in prepared.values()),
        "representative warmup index mismatch.",
    )

    strategy_module = importlib.import_module(get_strategy(spec.strategy_id).__module__)
    hooks = GridV2StrategyHooks.from_strategy(strategy_module)
    reference_plan = replace(plan, settings=replace(plan.settings, prefer_compiled=False))
    compiled_runs = {}
    reference_runs = {}
    parity = {}
    for name in SEGMENT_AXIS:
        item = prepared[name]
        compiled_runs[name] = execute_grid_v2_candidates(
            plan,
            item.dataframe,
            item.trade_start_idx,
            hooks,
            compute_sharpe=False,
            compute_sharpe_daily=True,
            compute_sqn=True,
            compute_max_drawdown_mtm=True,
        )
        reference_runs[name] = execute_grid_v2_candidates(
            reference_plan,
            item.dataframe,
            item.trade_start_idx,
            hooks,
            compute_sharpe=False,
            compute_sharpe_daily=True,
            compute_sqn=True,
            compute_max_drawdown_mtm=True,
        )
        parity[name] = _compare_grid_runs(
            compiled_runs[name],
            reference_runs[name],
            plan,
            expected_candidate_count=candidate_count,
            segment_name=name,
        )

    projection = project_candidates(plan, spec.generation["strategy"])
    selected_ids = geometry_candidate_ids(projection)
    slow = _selected_slow_parity(
        plan,
        hooks,
        prepared,
        reference_runs,
        projection,
        selected_ids,
    )
    threads = _thread_determinism(plan, hooks, prepared["is"])
    smoke = _smoke_gate(
        run_spec_path,
        root,
        work,
        repo,
        candidate_count=candidate_count,
        window_count=expected_window_count,
        compare_legacy_bracket=_matches_frozen_legacy_bracket_plan(spec, plan),
    )

    evidence = {
        "status": "passed",
        "scope": "strategy_lab_phase1b_stage1_real_certification",
        "run_spec": str(run_spec_path),
        "data_root": str(root),
        "disk_free_bytes_before": disk_free_bytes_before,
        "run_spec_derived": {
            "timeframe_minutes": timeframe_minutes,
            "warmup_bars": warmup_bars,
            "oos_period_months": oos_period_months,
            "expected_window_count": expected_window_count,
            "candidate_count": candidate_count,
            "segment_axis_size": len(SEGMENT_AXIS),
            "metric_axis_size": len(METRIC_AXIS),
            "group_shape": [
                candidate_count,
                len(SEGMENT_AXIS),
                len(METRIC_AXIS),
            ],
        },
        "frozen_experiment_pins": {
            "source_count": EXPECTED_SOURCE_COUNT,
            "prepared_segment_count": EXPECTED_SEGMENT_COUNT,
            "quality_row_count_excluding_header": EXPECTED_QUALITY_ROW_COUNT,
            "representative_ticker": REPRESENTATIVE_TICKER,
            "window_pins": WINDOW_PINS,
        },
        "frozen_identity": {
            "candidate_count": plan.deduped_candidate_count,
            "plan_fingerprint": plan.plan_fingerprint,
            "semantic_key_digest": semantic_key_digest(plan),
        },
        "quality": {
            "source_count": len(sources),
            "exchange_counts": exchange_counts,
            "cell_counts": cell_counts,
            "window_count": len(windows), "prepared_segment_count": EXPECTED_SEGMENT_COUNT,
            "quality_row_count_excluding_header": len(quality_rows), "window_pins": WINDOW_PINS,
            "source_preservation": preservation,
        },
        "representative": {
            "ticker": REPRESENTATIVE_TICKER,
            "cell": source.entry["cell"],
            "window_id": 1,
        },
        "compiled_reference_parity": parity,
        "selected_slow_parity": slow,
        "thread_determinism": threads,
        "compiled_config_packing": sorted(
            {
                run.metadata.get("compiled_config_packing")
                for run in compiled_runs.values()
            }
        ),
        "smoke": smoke,
    }
    evidence_path = work / "certification.json"
    evidence_path.write_bytes(canonical_json_bytes(evidence) + b"\n")
    json.loads(evidence_path.read_text(encoding="utf-8"))
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Certify the Strategy Lab Phase 1-B real pack without canonical "
            "generation."
        )
    )
    parser.add_argument("run_spec", type=Path)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = certify_real_pack(
            args.run_spec,
            data_root=args.data_root,
            work_dir=args.work_dir,
        )
    except (DatasetError, ValueError, OSError) as exc:
        print(f"Strategy Lab certification failed: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "evidence": str(args.work_dir / "certification.json"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
