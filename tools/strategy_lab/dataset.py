"""Candidate projection, metric matrices, and atomic dataset artifacts."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import uuid
from numbers import Integral, Real
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

import numpy as np

if TYPE_CHECKING:
    from core.grid_v2 import GridV2Plan, GridV2ResultRow


DATASET_SCHEMA_VERSION = "strategy_lab_dataset_v2"
CANDIDATE_SCHEMA_VERSION = "strategy_lab_candidates_v1"
SEGMENT_AXIS = ("is", "oos")
METRIC_AXIS = (
    "net_profit_pct",
    "max_drawdown_pct",
    "total_trades",
    "winning_trades",
    "losing_trades",
    "gross_profit",
    "gross_loss",
    "profit_factor",
    "win_rate_pct",
    "romad",
    "max_consecutive_losses",
    "sharpe_daily",
    "sharpe_daily_observations",
    "sharpe_daily_active_days",
    "sqn",
    "rejected_fill_count",
    "zero_size_entry_count",
    "invalid_stop_distance_count",
    "max_required_leverage",
    "flags",
    "max_drawdown_mtm_pct",
)
METRIC_AXIS_V3 = (*METRIC_AXIS, "margin_reject_count")
GUARDRAIL_FIELDS = (
    "rejected_fill_count",
    "zero_size_entry_count",
    "invalid_stop_distance_count",
    "max_required_leverage",
    "flags",
)
INTEGER_METRICS = {
    "total_trades",
    "winning_trades",
    "losing_trades",
    "max_consecutive_losses",
    "rejected_fill_count",
    "zero_size_entry_count",
    "invalid_stop_distance_count",
    "flags",
}
OPTIONAL_INTEGER_METRICS = {
    "sharpe_daily_observations",
    "sharpe_daily_active_days",
}


class DatasetError(ValueError):
    """A hard dataset projection/publication contract failure."""


def _jsonable(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(child) for child in value]
    return value


def project_candidates(plan: GridV2Plan, versions: Mapping[str, Any]) -> dict[str, Any]:
    from .config import semantic_key_digest

    table = plan.candidate_table
    global_axes = tuple(table.axis_names)
    candidates: list[dict[str, Any]] = []
    ids: set[int] = set()
    semantic_keys: set[str] = set()
    for row_index in range(plan.deduped_candidate_count):
        candidate_id = table.candidate_id_for_index(row_index)
        if candidate_id != row_index + 1:
            raise DatasetError("candidate IDs must be 1-based and aligned to matrix rows.")
        semantic_key = table.semantic_key_for_index(row_index)
        if candidate_id in ids or semantic_key in semantic_keys:
            raise DatasetError("candidate IDs and semantic keys must be unique.")
        ids.add(candidate_id)
        semantic_keys.add(semantic_key)
        active_axis_names = tuple(table.axis_names_for_index(row_index))
        codes = [int(value) for value in table.axis_value_codes[row_index].tolist()]
        if len(codes) != len(global_axes):
            raise DatasetError("candidate axis-code width does not match global axes.")
        mask: list[bool] = []
        active_values: list[Any] = []
        for column, (axis_name, code) in enumerate(zip(global_axes, codes)):
            is_active = axis_name in active_axis_names
            mask.append(is_active)
            if is_active:
                domain = table.parameter_domains[axis_name].values
                if code < 0 or code >= len(domain):
                    raise DatasetError(
                        f"candidate {candidate_id}: active axis code for {axis_name} is out of bounds."
                    )
                expected = _jsonable(domain[code])
                actual = _jsonable(table.param_value_for_index(row_index, axis_name))
                if actual != expected:
                    raise DatasetError(
                        f"candidate {candidate_id}: active value/code mismatch for {axis_name}."
                    )
                active_values.append(actual)
            else:
                if code != -1:
                    raise DatasetError(
                        f"candidate {candidate_id}: inactive axis {axis_name} must use code -1."
                    )
                active_values.append(None)
        candidates.append(
            {
                "row_index": row_index,
                "candidate_id": candidate_id,
                "variant_name": table.variant_name_for_index(row_index),
                "grid_mode_name": table.grid_mode_name_for_index(row_index),
                "modes": _jsonable(table.modes_for_index(row_index)),
                "semantic_key": semantic_key,
                "canonical_identity": _jsonable(
                    table.canonical_identity_for_index(row_index)
                ),
                "params": _jsonable(table.params_for_index(row_index)),
                "axis_names": list(active_axis_names),
                "axis_value_codes": codes,
                "active_axis_mask": mask,
                "active_axis_values": active_values,
            }
        )
    projected = {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "global_axis_names": list(global_axes),
        "candidate_count": plan.deduped_candidate_count,
        "plan_fingerprint": plan.plan_fingerprint,
        "semantic_key_digest": semantic_key_digest(plan),
        "identity_versions": _jsonable(versions),
        "candidates": candidates,
    }
    validate_candidate_projection(projected, plan)
    return projected


def validate_candidate_projection(payload: Mapping[str, Any], plan: GridV2Plan) -> None:
    from .config import semantic_key_digest

    if payload.get("schema_version") != CANDIDATE_SCHEMA_VERSION:
        raise DatasetError("candidates.json schema version mismatch.")
    if payload.get("candidate_count") != plan.deduped_candidate_count:
        raise DatasetError("candidates.json candidate count mismatch.")
    if payload.get("plan_fingerprint") != plan.plan_fingerprint:
        raise DatasetError("candidates.json plan fingerprint mismatch.")
    if payload.get("semantic_key_digest") != semantic_key_digest(plan):
        raise DatasetError("candidates.json semantic digest mismatch.")
    rows = payload.get("candidates")
    if not isinstance(rows, list) or len(rows) != plan.deduped_candidate_count:
        raise DatasetError("candidates.json rows do not match the plan.")
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or row.get("row_index") != index:
            raise DatasetError("candidates.json row order mismatch.")
        if row.get("candidate_id") != index + 1:
            raise DatasetError("candidates.json candidate ID order mismatch.")
        if row.get("semantic_key") != plan.candidate_table.semantic_key_for_index(index):
            raise DatasetError("candidates.json semantic-key order mismatch.")


def _numeric(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise DatasetError(f"{field}: expected a numeric value.")
    return float(value)


def _nonnegative_integer(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (Integral, Real)):
        raise DatasetError(f"{field}: expected a non-negative integral value.")
    converted = float(value)
    if not math.isfinite(converted) or converted < 0.0 or not converted.is_integer():
        raise DatasetError(f"{field}: expected a finite non-negative integral value.")
    return converted


def result_row_values(row: GridV2ResultRow) -> np.ndarray:
    if row.status != "ok":
        raise DatasetError(
            f"candidate {row.candidate_id}: execution status {row.status!r}: {row.error or ''}".rstrip()
        )
    guardrails = row.guardrail_summary
    missing_guardrails = [name for name in GUARDRAIL_FIELDS if name not in guardrails]
    if missing_guardrails:
        raise DatasetError(
            f"candidate {row.candidate_id}: missing guardrail field(s): {', '.join(missing_guardrails)}."
        )
    raw: dict[str, Any] = {
        "net_profit_pct": row.net_profit_pct,
        "max_drawdown_pct": row.max_drawdown_pct,
        "total_trades": row.total_trades,
        "winning_trades": row.winning_trades,
        "losing_trades": row.losing_trades,
        "gross_profit": row.gross_profit,
        "gross_loss": row.gross_loss,
        "profit_factor": row.profit_factor,
        "win_rate_pct": row.win_rate_pct,
        "romad": row.romad,
        "max_consecutive_losses": row.max_consecutive_losses,
        "sharpe_daily": row.sharpe_daily,
        "sharpe_daily_observations": row.sharpe_daily_observations,
        "sharpe_daily_active_days": row.sharpe_daily_active_days,
        "sqn": row.sqn,
        **{name: guardrails[name] for name in GUARDRAIL_FIELDS},
        "max_drawdown_mtm_pct": row.max_drawdown_mtm_pct,
    }
    if tuple(raw) != METRIC_AXIS:
        raise DatasetError("result mapping order does not match the frozen metric axis.")
    values: list[float] = []
    for name in METRIC_AXIS:
        value = raw[name]
        if name in OPTIONAL_INTEGER_METRICS:
            values.append(
                float("nan") if value is None else _nonnegative_integer(value, name)
            )
        elif name in INTEGER_METRICS:
            values.append(_nonnegative_integer(value, name))
        else:
            values.append(_numeric(value, name))
    return np.asarray(values, dtype=np.float64)


def segment_matrix(
    rows: Sequence[GridV2ResultRow],
    plan: GridV2Plan,
) -> np.ndarray:
    if len(rows) != plan.deduped_candidate_count:
        raise DatasetError("execution row count does not match candidate count.")
    matrix = np.empty((plan.deduped_candidate_count, len(METRIC_AXIS)), dtype=np.float64)
    for index, row in enumerate(rows):
        if row.candidate_id != index + 1:
            raise DatasetError("execution rows are not in exact candidate-ID order.")
        expected_key = plan.candidate_table.semantic_key_for_index(index)
        if row.semantic_key != expected_key:
            raise DatasetError("execution row semantic key does not match candidates.json order.")
        matrix[index] = result_row_values(row)
    return matrix


def group_matrix(
    is_rows: Sequence[GridV2ResultRow],
    oos_rows: Sequence[GridV2ResultRow],
    plan: GridV2Plan,
) -> np.ndarray:
    result = np.stack(
        (segment_matrix(is_rows, plan), segment_matrix(oos_rows, plan)),
        axis=1,
    )
    expected = (plan.deduped_candidate_count, len(SEGMENT_AXIS), len(METRIC_AXIS))
    if result.shape != expected or result.dtype != np.float64:
        raise DatasetError("group matrix shape or dtype violates strategy_lab_dataset_v2.")
    return result


def group_matrix_v3(is_rows, oos_rows, plan):
    """Preserve v2 projection exactly, appending the required raw cap counter."""
    base = group_matrix(is_rows, oos_rows, plan)
    if any("margin_reject_count" not in row.guardrail_summary for rows in (is_rows, oos_rows) for row in rows):
        raise DatasetError("v3 requires the raw margin_reject_count on every row.")
    counts = np.asarray([
        [_nonnegative_integer(row.guardrail_summary["margin_reject_count"], "margin_reject_count")
         for row in rows] for rows in (is_rows, oos_rows)
    ], dtype=np.float64).T[:, :, None]
    return np.concatenate((base, counts), axis=2)


def _task_temp(path: Path) -> Path:
    return path.with_name(f".{path.name}.strategy-lab-{uuid.uuid4().hex}.tmp")


def atomic_write(path: Path, writer: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _task_temp(path)
    try:
        writer(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_json(path: Path, payload: Any) -> None:
    from .config import canonical_json_bytes

    data = canonical_json_bytes(payload) + b"\n"

    def write(temporary: Path) -> None:
        with temporary.open("wb") as handle:
            handle.write(data)

    atomic_write(path, write)


def atomic_write_quality_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    data = quality_csv_bytes(rows)

    def write(temporary: Path) -> None:
        with temporary.open("wb") as handle:
            handle.write(data)

    atomic_write(path, write)


def quality_csv_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    from .data_quality import QUALITY_COLUMNS

    handle = io.StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=QUALITY_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({name: row.get(name, "") for name in QUALITY_COLUMNS})
    return handle.getvalue().encode("utf-8")


def atomic_write_group(path: Path, matrix: np.ndarray) -> None:
    if matrix.dtype != np.float64 or matrix.ndim != 3:
        raise DatasetError("group publication requires a three-dimensional float64 matrix.")

    def write(temporary: Path) -> None:
        with temporary.open("wb") as handle:
            np.save(handle, matrix, allow_pickle=False)

    atomic_write(path, write)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_record(path: Path, output_dir: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(output_dir).as_posix(),
        "size": path.stat().st_size,
        "sha256": file_sha256(path),
    }


def group_record(path: Path, output_dir: Path, matrix: np.ndarray) -> dict[str, Any]:
    record = artifact_record(path, output_dir)
    record.update(
        {
            "shape": list(matrix.shape),
            "dtype": str(matrix.dtype),
        }
    )
    return record


def load_json(path: Path, field: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetError(f"{field}: cannot load valid JSON: {exc}") from None
    if not isinstance(value, Mapping):
        raise DatasetError(f"{field}: expected an object.")
    return value


def validate_artifact_record(
    output_dir: Path,
    record: Mapping[str, Any],
    *,
    expected_shape: Sequence[int] | None = None,
    expected_dtype: str | None = None,
) -> bool:
    relative = record.get("path")
    if not isinstance(relative, str):
        return False
    path = output_dir / relative
    try:
        path.resolve().relative_to(output_dir.resolve())
    except ValueError:
        return False
    if not path.is_file() or path.is_symlink():
        return False
    if path.stat().st_size != record.get("size") or file_sha256(path) != record.get("sha256"):
        return False
    if expected_shape is not None or expected_dtype is not None:
        try:
            array = np.load(path, mmap_mode="r", allow_pickle=False)
        except (OSError, ValueError):
            return False
        if expected_shape is not None and list(array.shape) != list(expected_shape):
            return False
        if expected_dtype is not None and str(array.dtype) != expected_dtype:
            return False
        if list(record.get("shape", ())) != list(array.shape):
            return False
        if record.get("dtype") != str(array.dtype):
            return False
    return True


def manifest_identity_matches(manifest: Mapping[str, Any], identity: Mapping[str, Any]) -> bool:
    return manifest.get("identity") == identity


__all__ = [
    "CANDIDATE_SCHEMA_VERSION",
    "DATASET_SCHEMA_VERSION",
    "DatasetError",
    "GUARDRAIL_FIELDS",
    "METRIC_AXIS",
    "SEGMENT_AXIS",
    "artifact_record",
    "atomic_write_group",
    "atomic_write_json",
    "atomic_write_quality_csv",
    "file_sha256",
    "group_matrix",
    "group_record",
    "load_json",
    "manifest_identity_matches",
    "project_candidates",
    "quality_csv_bytes",
    "result_row_values",
    "segment_matrix",
    "validate_artifact_record",
    "validate_candidate_projection",
]
