"""Phase 1-A Strategy Lab dataset-generation orchestration and CLI."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numba
import numpy as np

from .config import REPO_ROOT, RunSpec, canonical_json_bytes, load_run_spec, semantic_key_digest
from core.engine_v2.compiled_kernel import (  # noqa: E402
    compiled_batch_available,
    compiled_unavailable_reason,
)
from core.grid_v2 import GridV2StrategyHooks, execute_grid_v2_candidates  # noqa: E402
from strategies import get_strategy  # noqa: E402

from .data_quality import (
    DataQualityError,
    PreparedSegment,
    ValidatedSource,
    build_authoritative_windows,
    prepare_segment,
    quality_row,
    validate_selected_sources,
    verify_source_preservation,
)
from .dataset import (
    DATASET_SCHEMA_VERSION,
    METRIC_AXIS,
    SEGMENT_AXIS,
    DatasetError,
    artifact_record,
    atomic_write_group,
    atomic_write_json,
    atomic_write_quality_csv,
    file_sha256,
    group_matrix,
    group_record,
    load_json,
    manifest_identity_matches,
    project_candidates,
    quality_csv_bytes,
    validate_artifact_record,
)
from .inventory import resolve_data_root


REQUIRED_BACKEND_KIND = "compiled_numba"
COMPUTE_MAX_DRAWDOWN_MTM = True


@dataclass(frozen=True)
class GenerationResult:
    output_dir: Path
    manifest_path: Path
    scope: str
    completed_groups: int
    reused_groups: int
    regenerated_groups: int
    no_op: bool
    timings: Mapping[str, float]


def runtime_thread_report(
    requested_threads: int,
    applied_threads: int,
    available_threads: int,
) -> dict[str, Any]:
    """Classify thread evidence without calling a clamped run multi-threaded."""

    values = (requested_threads, applied_threads, available_threads)
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in values):
        raise DatasetError("thread capability values must be positive integers.")
    exact = requested_threads == applied_threads
    return {
        "requested_threads": requested_threads,
        "applied_threads": applied_threads,
        "available_threads": available_threads,
        "requested_applied_exact": exact,
        "multi_thread_verified": bool(
            exact and requested_threads > 1 and applied_threads > 1 and available_threads > 1
        ),
    }


def _selected_entries(spec: RunSpec, selectors: Sequence[str] | None) -> tuple[Mapping[str, Any], ...]:
    if spec.inventory is None:
        raise DatasetError("generation requires a validated inventory.")
    entries = tuple(spec.inventory.entries)
    if not selectors:
        return entries
    requested = [str(value).upper() for value in selectors]
    if len(set(requested)) != len(requested):
        raise DatasetError("ticker selectors must not contain duplicates.")
    available = {str(entry["canonical_symbol"]): entry for entry in entries}
    missing = [symbol for symbol in requested if symbol not in available]
    if missing:
        raise DatasetError(f"unknown ticker selector(s): {', '.join(missing)}.")
    requested_set = set(requested)
    return tuple(entry for entry in entries if entry["canonical_symbol"] in requested_set)


def _selected_windows(windows: Sequence[Any], selectors: Sequence[int] | None) -> tuple[Any, ...]:
    if not selectors:
        return tuple(windows)
    requested = [int(value) for value in selectors]
    if len(set(requested)) != len(requested):
        raise DatasetError("window selectors must not contain duplicates.")
    available = {int(window.window_id): window for window in windows}
    missing = [str(window_id) for window_id in requested if window_id not in available]
    if missing:
        raise DatasetError(f"unknown window selector(s): {', '.join(missing)}.")
    requested_set = set(requested)
    return tuple(window for window in windows if int(window.window_id) in requested_set)


def _window_payload(window: Any) -> dict[str, Any]:
    def iso(value: Any) -> str:
        return value.isoformat().replace("+00:00", "Z")

    return {
        "window_id": int(window.window_id),
        "is_start": iso(window.is_start),
        "is_end": iso(window.is_end),
        "oos_start": iso(window.oos_start),
        "oos_end": iso(window.oos_end),
    }


def _quality_preflight(
    sources: Sequence[ValidatedSource],
    raw_rows: Sequence[Mapping[str, Any]],
    windows: Sequence[Any],
    *,
    warmup_bars: int,
    timeframe_minutes: int,
    oos_period_months: int,
) -> tuple[dict[str, Any], ...]:
    source_rows = {
        str(row["canonical_symbol"]): dict(row)
        for row in raw_rows
    }
    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for source in sources:
        rows.append(source_rows[str(source.entry["canonical_symbol"])])
        for window in windows:
            for segment_name in SEGMENT_AXIS:
                try:
                    prepared = prepare_segment(
                        source,
                        window,
                        segment_name,
                        warmup_bars=warmup_bars,
                        timeframe_minutes=timeframe_minutes,
                        oos_period_months=oos_period_months,
                    )
                except DataQualityError as exc:
                    message = str(exc)
                    failures.append(message)
                    rows.append(
                        quality_row(
                            source.entry,
                            window_id=window.window_id,
                            segment=segment_name,
                            status="error",
                            expected_row_count="",
                            message=message,
                        )
                    )
                else:
                    rows.append(
                        quality_row(
                            source.entry,
                            window_id=window.window_id,
                            segment=segment_name,
                            status="ok",
                            first_timestamp=prepared.first_timestamp,
                            last_timestamp=prepared.last_timestamp,
                            row_count=prepared.evaluation_row_count,
                            expected_row_count=prepared.evaluation_row_count,
                            warmup_row_count=prepared.trade_start_idx,
                            trade_start_idx=prepared.trade_start_idx,
                            message="authoritative segment and warmup validated",
                        )
                    )
    if failures:
        error = DataQualityError("segment validation failed: " + " | ".join(failures))
        error.quality_rows = tuple(rows)  # type: ignore[attr-defined]
        raise error
    return tuple(rows)


def _identity(
    spec: RunSpec,
    sources: Sequence[ValidatedSource],
    windows: Sequence[Any],
) -> dict[str, Any]:
    if spec.plan is None or spec.inventory is None:
        raise DatasetError("generation identity requires the validated plan and inventory.")
    strategy = spec.generation["strategy"]
    resources = spec.generation["resources"]
    return {
        "dataset_schema": DATASET_SCHEMA_VERSION,
        "run_name": spec.raw["run_name"],
        "generation_sha256": spec.generation_sha256,
        "pre_registration_sha256": spec.pre_registration_sha256,
        "inventory_sha256": spec.inventory.sha256,
        "ticker_list_digest": spec.inventory.ticker_list_digest,
        "candidate_count": spec.plan.deduped_candidate_count,
        "semantic_key_digest": semantic_key_digest(spec.plan),
        "plan_fingerprint": spec.plan.plan_fingerprint,
        "candidate_axis": list(range(1, spec.plan.deduped_candidate_count + 1)),
        "segment_axis": list(SEGMENT_AXIS),
        "metric_axis": list(METRIC_AXIS),
        "metric_requests": {
            "compute_max_drawdown_mtm": COMPUTE_MAX_DRAWDOWN_MTM,
        },
        "included_tickers": [
            {"canonical_symbol": source.entry["canonical_symbol"], "cell": source.entry["cell"]}
            for source in sources
        ],
        "windows": [_window_payload(window) for window in windows],
        "dtype": "float64",
        "expected_group_shape": [spec.plan.deduped_candidate_count, 2, len(METRIC_AXIS)],
        "versions": {
            "strategy_id": strategy["strategy_id"],
            "strategy_version": strategy["strategy_version"],
            "grid_v2_engine_version": strategy["grid_v2_engine_version"],
            "plan_identity_schema_version": strategy["plan_identity_schema_version"],
            "semantic_identity_schema_version": strategy["semantic_identity_schema_version"],
            "runtime_contract_version": strategy["runtime_contract_version"],
            "planning_policy_version": strategy["planning_policy_version"],
            "allocator_version": strategy["allocator_version"],
        },
        "resources": {
            "numba_threads": int(resources["numba_threads"]),
            "compiled_workers": int(spec.plan.settings.compiled_workers),
            "outer_workers": int(resources["outer_workers"]),
            "max_signal_cache_mb": float(spec.plan.settings.max_signal_cache_mb),
            "prefer_compiled": bool(spec.plan.settings.prefer_compiled),
            "slow_enrich_selected": bool(spec.plan.settings.slow_enrich_selected),
        },
    }


def _expected_group_paths(
    sources: Sequence[ValidatedSource], windows: Sequence[Any]
) -> tuple[str, ...]:
    return tuple(
        f"groups/{source.entry['canonical_symbol']}/window_{int(window.window_id):02d}.npy"
        for source in sources
        for window in windows
    )


def _bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git_facts(repo_root: Path) -> tuple[str, bool]:
    command = [
        "git",
        "-c",
        f"safe.directory={repo_root.as_posix()}",
        "-C",
        str(repo_root),
    ]
    try:
        head = subprocess.run(
            [*command, "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            [*command, "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return "unavailable", True
    return head, bool(status.strip())


def _assert_output_outside_data_root(output_dir: Path, data_root: Path) -> None:
    try:
        output_dir.relative_to(data_root)
    except ValueError:
        return
    raise DatasetError("output directory must not equal or be beneath the market-data root.")


def _execution_backend_facts(run: Any) -> dict[str, Any]:
    metadata = getattr(run, "metadata", None)
    if not isinstance(metadata, Mapping):
        raise DatasetError("V2 execution result is missing backend metadata.")
    if metadata.get("backend_kind") != REQUIRED_BACKEND_KIND:
        raise DatasetError("Strategy Lab requires effective backend_kind='compiled_numba'.")
    if metadata.get("compiled_batch_used") is not True:
        raise DatasetError("Strategy Lab requires compiled_batch_used=True.")
    rows = tuple(getattr(run, "rows", ()))
    # Run-level compiled facts are immutable; preserve row errors before checking
    # successful-row backend homogeneity.
    for row in rows:
        if getattr(row, "status", None) != "ok":
            candidate_id = getattr(row, "candidate_id", "unknown")
            error = getattr(row, "error", None) or "execution failed"
            raise DatasetError(f"candidate {candidate_id}: {error}")
    row_backends = {getattr(row, "backend_kind", None) for row in rows}
    if row_backends != {REQUIRED_BACKEND_KIND}:
        raise DatasetError(
            "Strategy Lab requires homogeneous compiled_numba backend metadata on every row."
        )
    return {
        "backend_kind": REQUIRED_BACKEND_KIND,
        "compiled_batch_used": True,
        "compiled_execution_mode": metadata.get("compiled_execution_mode"),
        "compiled_config_packing": metadata.get("compiled_config_packing"),
        "compiled_unavailable_reason": metadata.get("compiled_unavailable_reason"),
    }


def _require_compiled_backend_available() -> None:
    if compiled_batch_available():
        return
    reason = compiled_unavailable_reason() or "unknown reason"
    raise DatasetError(f"Strategy Lab compiled backend is unavailable: {reason}")


def _valid_group_backend_record(record: Mapping[str, Any]) -> bool:
    backend = record.get("execution_backend")
    if not isinstance(backend, Mapping):
        return False
    if (
        backend.get("backend_kind") != REQUIRED_BACKEND_KIND
        or backend.get("compiled_batch_used") is not True
    ):
        return False
    segments = backend.get("segments")
    if not isinstance(segments, Mapping) or set(segments) != set(SEGMENT_AXIS):
        return False
    return all(
        isinstance(segments.get(segment), Mapping)
        and segments[segment].get("backend_kind") == REQUIRED_BACKEND_KIND
        and segments[segment].get("compiled_batch_used") is True
        for segment in SEGMENT_AXIS
    )


def _backend_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    segment_facts = [
        record["execution_backend"]["segments"][segment]
        for record in records
        for segment in SEGMENT_AXIS
    ]

    def observed(field: str) -> list[str]:
        return sorted(
            {
                str(facts[field])
                for facts in segment_facts
                if facts.get(field) is not None
            }
        )

    return {
        "backend_kind": REQUIRED_BACKEND_KIND,
        "compiled_batch_used": True,
        "execution_modes": observed("compiled_execution_mode"),
        "config_packings": observed("compiled_config_packing"),
        "unavailable_reasons": observed("compiled_unavailable_reason"),
        "segment_execution_count": len(segment_facts),
    }


def _validate_complete(
    output_dir: Path,
    manifest: Mapping[str, Any],
    identity: Mapping[str, Any],
    scope: str,
    expected_supporting_sha: Mapping[str, str],
    expected_group_paths: Sequence[str],
) -> bool:
    if (
        manifest.get("status") != "complete"
        or manifest.get("scope") != scope
        or not manifest_identity_matches(manifest, identity)
    ):
        return False
    artifacts = manifest.get("artifacts")
    groups = manifest.get("groups")
    if not isinstance(artifacts, Mapping) or not isinstance(groups, list):
        return False
    for name, expected_sha in expected_supporting_sha.items():
        record = artifacts.get(name)
        if not isinstance(record, Mapping) or record.get("sha256") != expected_sha:
            return False
        if not validate_artifact_record(output_dir, record):
            return False
    if [record.get("path") for record in groups if isinstance(record, Mapping)] != list(expected_group_paths):
        return False
    shape = identity["expected_group_shape"]
    return all(
        isinstance(record, Mapping)
        and _valid_group_backend_record(record)
        and validate_artifact_record(
            output_dir,
            record,
            expected_shape=shape,
            expected_dtype="float64",
        )
        for record in groups
    )


def _assert_plan_contract(spec: RunSpec) -> None:
    if spec.plan is None:
        raise DatasetError("run spec did not produce a V2 plan.")
    settings = spec.plan.settings
    resources = spec.generation["resources"]
    if (
        settings.prefer_compiled is not True
        or settings.slow_enrich_selected is not False
        or settings.compiled_workers != 1
        or settings.max_signal_cache_mb != float(resources["grid_v2_max_cache_mb"])
        or settings.planning_policy != "full"
    ):
        raise DatasetError("V2 plan execution settings do not match the Phase 1-A contract.")
    if int(resources["outer_workers"]) != 1:
        raise DatasetError("Phase 1-A requires outer_workers=1 for sequential orchestration.")
    for index in range(spec.plan.deduped_candidate_count):
        params = spec.plan.candidate_table.params_for_index(index)
        if params.get("start") is not None or params.get("end") is not None:
            raise DatasetError("candidate params must not contain per-window start/end values.")


def generate_dataset(
    run_spec_path: str | Path,
    *,
    data_root: str | Path | None,
    output_dir: str | Path,
    ticker_selectors: Sequence[str] | None = None,
    window_selectors: Sequence[int] | None = None,
    resume: bool = False,
    preflight_only: bool = False,
    repo_root: str | Path = REPO_ROOT,
    _after_group: Callable[[int, Path], None] | None = None,
    _progress: Callable[[str], None] | None = None,
) -> GenerationResult:
    """Generate or resume one identity-bound Strategy Lab dataset scope."""

    started = time.perf_counter()
    repo = Path(repo_root).resolve()
    output = Path(output_dir).resolve()
    root = resolve_data_root(data_root)
    _assert_output_outside_data_root(output, root)
    manifest_path = output / "manifest.json"
    partial_path = output / "manifest.partial.json"
    existing_manifest = (
        load_json(manifest_path, "manifest.json") if manifest_path.exists() else None
    )
    run_spec_started = time.perf_counter()
    spec = load_run_spec(run_spec_path, repo_root=repo)
    if spec.raw["schema_version"] == "strategy_lab_runspec_v2":
        from .parquet import generate_parquet

        return generate_parquet(spec, root, output, ticker_selectors=ticker_selectors,
            window_selectors=window_selectors, resume=resume, preflight_only=preflight_only,
            repo=repo, after_group=_after_group, progress=_progress)
    if preflight_only:
        raise DatasetError("--preflight-only requires a v2 Parquet run spec.")
    run_spec_load_seconds = time.perf_counter() - run_spec_started
    plan_build_seconds = spec.plan_build_seconds
    _assert_plan_contract(spec)
    plan = spec.plan
    assert plan is not None
    entries = _selected_entries(spec, ticker_selectors)
    timeframe = int(spec.generation["market_data"]["timeframe_minutes"])

    quality_path = output / "data_quality.csv"
    quality_started = time.perf_counter()
    sources: tuple[ValidatedSource, ...] = ()
    raw_rows: tuple[dict[str, Any], ...] = ()
    try:
        sources, raw_rows = validate_selected_sources(
            root,
            entries,
            timeframe_minutes=timeframe,
        )
        all_windows = build_authoritative_windows(spec, sources)
        windows = _selected_windows(all_windows, window_selectors)
        warmup_bars = int(spec.generation["windows"]["warmup_bars"])
        quality_rows = _quality_preflight(
            sources,
            raw_rows,
            windows,
            warmup_bars=warmup_bars,
            timeframe_minutes=timeframe,
            oos_period_months=int(spec.generation["windows"]["oos_period_months"]),
        )
    except DataQualityError as exc:
        failure_rows = getattr(exc, "quality_rows", None)
        if failure_rows is None:
            failure_rows = tuple(raw_rows) + tuple(
                quality_row(
                    source.entry,
                    segment="windows",
                    status="error",
                    message=str(exc),
                )
                for source in sources
            )
        if existing_manifest is None:
            atomic_write_quality_csv(
                quality_path,
                failure_rows,
            )
        raise
    source_quality_seconds = time.perf_counter() - quality_started
    if _progress is not None:
        _progress(
            f"validated {len(sources)} ticker(s); selected {len(windows)} window(s)"
        )

    scope = (
        "full"
        if len(entries) == len(spec.inventory.entries)  # type: ignore[union-attr]
        and len(windows) == int(spec.generation["windows"]["expected_window_count"])
        else "smoke"
    )
    candidate_projection = project_candidates(plan, spec.generation["strategy"])
    normalized_runspec = spec.raw
    identity = _identity(spec, sources, windows)
    expected_paths = _expected_group_paths(sources, windows)
    normalized_bytes = canonical_json_bytes(normalized_runspec) + b"\n"
    candidate_bytes = canonical_json_bytes(candidate_projection) + b"\n"
    quality_bytes = quality_csv_bytes(quality_rows)
    expected_supporting_sha = {
        "normalized_runspec.json": _bytes_sha256(normalized_bytes),
        "candidates.json": _bytes_sha256(candidate_bytes),
        "data_quality.csv": _bytes_sha256(quality_bytes),
    }
    if existing_manifest is not None:
        manifest = existing_manifest
        if not manifest_identity_matches(manifest, identity):
            raise DatasetError("completed output directory has an incompatible run identity.")
        if not _validate_complete(
            output,
            manifest,
            identity,
            scope,
            expected_supporting_sha,
            expected_paths,
        ):
            raise DatasetError("completed output directory failed checksum/shape/dtype verification.")
        verify_source_preservation(sources)
        elapsed = time.perf_counter() - started
        return GenerationResult(
            output_dir=output,
            manifest_path=manifest_path,
            scope=scope,
            completed_groups=len(expected_paths),
            reused_groups=len(expected_paths),
            regenerated_groups=0,
            no_op=True,
            timings={
                "total_seconds": elapsed,
                "run_spec_load_seconds": run_spec_load_seconds,
                "plan_build_seconds": plan_build_seconds,
                "source_quality_seconds": source_quality_seconds,
                "is_execution_seconds": 0.0,
                "oos_execution_seconds": 0.0,
                "matrix_publication_seconds": 0.0,
            },
        )

    completed_by_path: dict[str, Mapping[str, Any]] = {}
    if partial_path.exists():
        if not resume:
            raise DatasetError("partial output exists; pass --resume to continue it.")
        partial = load_json(partial_path, "manifest.partial.json")
        if (
            partial.get("status") != "partial"
            or partial.get("scope") != scope
            or not manifest_identity_matches(partial, identity)
        ):
            raise DatasetError("partial output has an incompatible schema, identity, resources, axes, or scope.")
        records = partial.get("groups")
        if not isinstance(records, list):
            raise DatasetError("manifest.partial.json groups must be a list.")
        for record in records:
            if not isinstance(record, Mapping) or record.get("path") not in expected_paths:
                raise DatasetError("manifest.partial.json contains an unrecognized group record.")
            if not _valid_group_backend_record(record):
                raise DatasetError(
                    "manifest.partial.json group does not prove compiled_numba execution."
                )
            completed_by_path[str(record["path"])] = record
    elif resume:
        completed_by_path = {}

    _require_compiled_backend_available()
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output / "normalized_runspec.json", normalized_runspec)
    atomic_write_json(output / "candidates.json", candidate_projection)
    atomic_write_quality_csv(quality_path, quality_rows)
    partial_payload: dict[str, Any] = {
        "status": "partial",
        "scope": scope,
        "identity": identity,
        "groups": [],
    }

    valid_records: dict[str, Mapping[str, Any]] = {}
    for relative, record in completed_by_path.items():
        if validate_artifact_record(
            output,
            record,
            expected_shape=identity["expected_group_shape"],
            expected_dtype="float64",
        ):
            valid_records[relative] = record
    partial_payload["groups"] = [
        valid_records[path] for path in expected_paths if path in valid_records
    ]
    atomic_write_json(partial_path, partial_payload)

    strategy_class = get_strategy(spec.strategy_id)
    strategy_module = importlib.import_module(strategy_class.__module__)
    hooks = GridV2StrategyHooks.from_strategy(strategy_module)
    previous_threads = numba.get_num_threads()
    requested_threads = int(spec.generation["resources"]["numba_threads"])
    if requested_threads != 1:
        raise DatasetError("Phase 1-A requires exactly one runtime Numba thread.")
    is_execution_seconds = 0.0
    oos_execution_seconds = 0.0
    publication_seconds = 0.0
    thread_report: Mapping[str, Any] | None = None
    reused = len(valid_records)
    regenerated = 0
    group_index = 0
    preservation: Mapping[str, Any] | None = None
    primary_error: BaseException | None = None
    try:
        numba.set_num_threads(requested_threads)
        thread_report = runtime_thread_report(
            requested_threads,
            numba.get_num_threads(),
            int(numba.config.NUMBA_NUM_THREADS),
        )
        for source in sources:
            for window in windows:
                relative = f"groups/{source.entry['canonical_symbol']}/window_{int(window.window_id):02d}.npy"
                group_index += 1
                if relative in valid_records:
                    if _progress is not None:
                        _progress(f"group {group_index}/{len(expected_paths)} reused: {relative}")
                    continue
                segment_rows: dict[str, Any] = {}
                segment_backends: dict[str, dict[str, Any]] = {}
                for segment_name in SEGMENT_AXIS:
                    prepared: PreparedSegment = prepare_segment(
                        source,
                        window,
                        segment_name,
                        warmup_bars=warmup_bars,
                        timeframe_minutes=timeframe,
                        oos_period_months=int(
                            spec.generation["windows"]["oos_period_months"]
                        ),
                    )
                    execution_started = time.perf_counter()
                    run = execute_grid_v2_candidates(
                        plan,
                        prepared.dataframe,
                        prepared.trade_start_idx,
                        hooks,
                        compute_sharpe=False,
                        compute_sharpe_daily=True,
                        compute_sqn=True,
                        compute_max_drawdown_mtm=COMPUTE_MAX_DRAWDOWN_MTM,
                    )
                    duration = time.perf_counter() - execution_started
                    if segment_name == "is":
                        is_execution_seconds += duration
                    else:
                        oos_execution_seconds += duration
                    if run.selected:
                        raise DatasetError("Slow enrichment selected rows must be empty.")
                    segment_backends[segment_name] = _execution_backend_facts(run)
                    segment_rows[segment_name] = run.rows
                matrix = group_matrix(segment_rows["is"], segment_rows["oos"], plan)
                path = output / relative
                publish_started = time.perf_counter()
                atomic_write_group(path, matrix)
                record = group_record(path, output, matrix)
                record["execution_backend"] = {
                    "backend_kind": REQUIRED_BACKEND_KIND,
                    "compiled_batch_used": True,
                    "segments": segment_backends,
                }
                publication_seconds += time.perf_counter() - publish_started
                valid_records[relative] = record
                regenerated += 1
                if _progress is not None:
                    _progress(
                        f"group {group_index}/{len(expected_paths)} regenerated: {relative}"
                    )
                partial_payload["groups"] = [
                    valid_records[path_name]
                    for path_name in expected_paths
                    if path_name in valid_records
                ]
                atomic_write_json(partial_path, partial_payload)
                if _after_group is not None:
                    _after_group(group_index, path)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_failures: list[tuple[str, BaseException]] = []
        try:
            numba.set_num_threads(previous_threads)
        except BaseException as exc:
            cleanup_failures.append(("Numba thread restoration", exc))
        try:
            preservation = verify_source_preservation(sources)
        except BaseException as exc:
            cleanup_failures.append(("source preservation verification", exc))
        if cleanup_failures:
            if primary_error is not None:
                for label, failure in cleanup_failures:
                    primary_error.add_note(
                        f"Secondary {label} failure: {type(failure).__name__}: {failure}"
                    )
            else:
                label, cleanup_error = cleanup_failures[0]
                for secondary_label, failure in cleanup_failures[1:]:
                    cleanup_error.add_note(
                        f"Secondary {secondary_label} failure: "
                        f"{type(failure).__name__}: {failure}"
                    )
                cleanup_error.add_note(f"Cleanup stage: {label}")
                raise cleanup_error

    if preservation is None:
        raise DatasetError("source preservation verification did not complete.")
    ordered_records = [valid_records[path] for path in expected_paths if path in valid_records]
    if len(ordered_records) != len(expected_paths):
        raise DatasetError("not every expected group was published.")
    for record in ordered_records:
        if not validate_artifact_record(
            output,
            record,
            expected_shape=identity["expected_group_shape"],
            expected_dtype="float64",
        ):
            raise DatasetError("a completed group failed final checksum/shape/dtype verification.")

    artifacts = {
        name: artifact_record(output / name, output)
        for name in ("normalized_runspec.json", "candidates.json", "data_quality.csv")
    }
    head, dirty = _git_facts(repo)
    total_seconds = time.perf_counter() - started
    final_manifest = {
        "status": "complete",
        "schema_version": DATASET_SCHEMA_VERSION,
        "scope": scope,
        "identity": identity,
        "economics": spec.generation["economics"],
        "execution_modes": spec.generation["execution"],
        "artifacts": artifacts,
        "groups": ordered_records,
        "provenance": {
            "source_head": head,
            "dirty_worktree": dirty,
            "resolved_data_root": str(root),
            "host": platform.node(),
            "python_version": platform.python_version(),
            "numpy_version": np.__version__,
            "numba_version": numba.__version__,
            "platform": platform.platform(),
            "source_preservation": preservation,
            "source_mtime_ns": {
                source.entry["relative_filename"]: source.snapshot.mtime_ns
                for source in sources
            },
            "plan_build_count": 1,
            "selected_slow_row_count": 0,
            "execution_backend": _backend_summary(ordered_records),
            "thread_capability": thread_report,
            "timings": {
                "run_spec_load_seconds": run_spec_load_seconds,
                "plan_build_seconds": plan_build_seconds,
                "source_quality_seconds": source_quality_seconds,
                "is_execution_seconds": is_execution_seconds,
                "oos_execution_seconds": oos_execution_seconds,
                "matrix_publication_seconds": publication_seconds,
                "total_seconds": total_seconds,
            },
        },
    }
    atomic_write_json(manifest_path, final_manifest)
    if partial_path.parent != output or partial_path.name != "manifest.partial.json":
        raise DatasetError("refusing to remove an unexpected partial-manifest path.")
    partial_path.unlink()
    return GenerationResult(
        output_dir=output,
        manifest_path=manifest_path,
        scope=scope,
        completed_groups=len(ordered_records),
        reused_groups=reused,
        regenerated_groups=regenerated,
        no_op=False,
        timings=final_manifest["provenance"]["timings"],
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a Strategy Lab Phase 1 dataset scope.")
    parser.add_argument("run_spec", type=Path)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--ticker", action="append", dest="tickers")
    parser.add_argument("--window", action="append", type=int, dest="windows")
    args = parser.parse_args(argv)
    try:
        result = generate_dataset(
            args.run_spec,
            data_root=args.data_root,
            output_dir=args.output_dir,
            ticker_selectors=args.tickers,
            window_selectors=args.windows,
            resume=args.resume,
            preflight_only=args.preflight_only,
            _progress=lambda message: print(f"Strategy Lab: {message}"),
        )
    except (DataQualityError, DatasetError, ValueError) as exc:
        parser.exit(2, f"Strategy Lab generation error: {exc}\n")
    state = "verified no-op" if result.no_op else ("preflight incomplete" if result.manifest_path.name == "manifest.partial.json" else "complete")
    print(
        f"Strategy Lab {result.scope} {state}: {result.completed_groups} group(s), "
        f"reused={result.reused_groups}, regenerated={result.regenerated_groups}, "
        f"output={result.output_dir}, duration={result.timings['total_seconds']:.3f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
