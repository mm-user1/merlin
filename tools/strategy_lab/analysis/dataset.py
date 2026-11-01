"""Strict read-only access to Strategy Lab dataset-v2 analysis inputs."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import numpy as np


SUPPORTED_DATASET_SCHEMAS = {"strategy_lab_dataset_v2", "strategy_lab_dataset_v3"}
SUPPORTED_RULE_VERSIONS = {"strategy_lab_rules_v1"}
SUPPORTED_EVIDENCE_VERSIONS = {"strategy_lab_evidence_v1"}
EVALUABLE_EVIDENCE_BLOCKS = (
    "broad_population_edge",
    "selected_strategy_viability",
    "selection_lift",
)
EVIDENCE_METADATA_BLOCKS = (
    "nomination",
    "outlier_procedure",
    "primary_confirmation",
    "temporal_windows_7_8",
    "uncertainty",
)
KNOWN_OPERATORS = {">", ">="}
EXPECTED_TIE_BREAK = (
    ("rule_score", "descending"),
    ("IS net_profit_pct", "descending"),
    ("semantic_key", "ascending"),
    ("candidate_id", "ascending"),
)
CODE_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


class AnalysisError(ValueError):
    """An invalid or unsupported analysis input contract."""


class ScopeLockedError(AnalysisError):
    """A pre-registered scope was requested without its unlock evidence."""


@dataclass(frozen=True)
class Window:
    window_id: int
    is_start: str
    is_end: str
    oos_start: str
    oos_end: str

    @property
    def block_key(self) -> tuple[str, str]:
        return self.oos_start, self.oos_end


@dataclass(frozen=True)
class CandidateGeometry:
    candidate_ids: np.ndarray
    semantic_keys: tuple[str, ...]
    params: tuple[Mapping[str, Any], ...]
    global_axis_names: tuple[str, ...]
    axis_codes: np.ndarray
    active_masks: np.ndarray
    block_keys: tuple[tuple[str, str], ...]

    @property
    def count(self) -> int:
        return int(self.candidate_ids.size)


@dataclass(frozen=True)
class AnalysisContract:
    observation: Mapping[str, Any]
    rule_registry: Mapping[str, Any]
    scopes: tuple[Mapping[str, Any], ...]
    split: Mapping[str, Any]
    evidence: Mapping[str, Any]
    evidence_version: str
    maximum_nominated_rules: int
    primary_comparison: str
    descriptive: bool = False
    seed: int | None = None
    random_control_draws: int | None = None


@dataclass(frozen=True)
class ResolvedScope:
    name: str
    ticker_cell: str
    tickers: tuple[str, ...]
    ticker_cell_count: int
    windows: tuple[Window, ...]
    declared_window_ids: tuple[int, ...]
    missing_window_ids: tuple[int, ...]
    is_partial: bool
    requires_unlock: bool
    unlock_evidence: Mapping[str, Any] | None

    @property
    def total_pairs(self) -> int:
        return len(self.tickers) * len(self.windows)

    @property
    def block_keys(self) -> tuple[tuple[str, str], ...]:
        return tuple(window.block_key for window in self.windows)


@dataclass(frozen=True)
class ISView:
    """The complete rule-facing surface. It intentionally has no OOS member."""

    metrics: Mapping[str, np.ndarray]
    ticker: str
    window_id: int
    is_start: str
    is_end: str


def _load_json(path: Path, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AnalysisError(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise AnalysisError(f"{label} must contain a JSON object.")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    """Match the frozen Strategy Lab canonical JSON without importing execution code."""
    try:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise AnalysisError(f"canonical JSON: {exc}") from None
    return payload.encode("utf-8")


def _safe_path(root: Path, relative: Any, label: str) -> Path:
    if not isinstance(relative, str) or not relative:
        raise AnalysisError(f"{label} path must be a non-empty relative string.")
    candidate = Path(relative)
    if candidate.is_absolute():
        raise AnalysisError(f"{label} path must be relative to the dataset root.")
    resolved = (root / candidate).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise AnalysisError(f"{label} path escapes the dataset root: {relative!r}.") from exc
    return resolved


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AnalysisError(f"{label} must be an object.")
    return value


def _validate_calendar_blocks(
    scope_name: str, windows: Sequence[Window]
) -> None:
    seen: dict[tuple[str, str], int] = {}
    for window in windows:
        prior = seen.get(window.block_key)
        if prior is not None:
            raise AnalysisError(
                f"scope {scope_name!r} has duplicate UTC OOS block "
                f"{window.block_key!r} in windows {prior} and {window.window_id}."
            )
        seen[window.block_key] = window.window_id


def _validate_contract(run_spec: Mapping[str, Any]) -> AnalysisContract:
    prereg = _require_mapping(run_spec.get("preregistration"), "preregistration")
    if run_spec.get("schema_version") == "strategy_lab_runspec_v2":
        # The v3 reader has already validated the complete saved contract.
        return AnalysisContract(
            observation={"monthly_ticker_aggregation": "equal_weight_valid_tickers",
                         "headline_aggregation": "equal_weight_valid_oos_blocks"},
            rule_registry=prereg["rule_registry"], scopes=tuple(prereg["analysis_scopes"]),
            split={}, evidence={}, evidence_version="not_requested", maximum_nominated_rules=0,
            primary_comparison="top1_oos_net_profit_vs_primary_profit", descriptive=True,
            seed=prereg["seed"], random_control_draws=prereg["random_control_draws"])
    required = (
        "observation_contract",
        "rule_registry",
        "analysis_scopes",
        "split",
        "evidence_criteria",
        "evidence_criteria_version",
        "maximum_nominated_rules",
        "primary_comparison",
    )
    missing = [name for name in required if name not in prereg]
    if missing:
        raise AnalysisError(f"pre-registration is missing: {', '.join(missing)}.")
    registry = _require_mapping(prereg["rule_registry"], "rule_registry")
    version = registry.get("version")
    if version not in SUPPORTED_RULE_VERSIONS:
        raise AnalysisError(f"unsupported rule registry version: {version!r}.")
    evidence_version = prereg["evidence_criteria_version"]
    if evidence_version not in SUPPORTED_EVIDENCE_VERSIONS:
        raise AnalysisError(f"unsupported evidence criteria version: {evidence_version!r}.")
    tie_break = registry.get("tie_break")
    if not isinstance(tie_break, list):
        raise AnalysisError("rule_registry.tie_break must be a list.")
    observed_ties = tuple(
        (item.get("field"), item.get("direction"))
        if isinstance(item, Mapping)
        else (None, None)
        for item in tie_break
    )
    if observed_ties != EXPECTED_TIE_BREAK:
        raise AnalysisError(f"unknown rule tie-break contract: {observed_ties!r}.")
    evidence = _require_mapping(prereg["evidence_criteria"], "evidence_criteria")
    for block_name in EVALUABLE_EVIDENCE_BLOCKS:
        block = _require_mapping(evidence.get(block_name), f"evidence.{block_name}")
        for leaf_name, leaf in block.items():
            if block_name == "selection_lift" and leaf_name == "comparison":
                continue
            item = _require_mapping(leaf, f"evidence.{block_name}.{leaf_name}")
            if item.get("operator") not in KNOWN_OPERATORS or "value" not in item:
                raise AnalysisError(
                    f"malformed evidence leaf: {block_name}.{leaf_name}."
                )
    scopes = prereg["analysis_scopes"]
    if not isinstance(scopes, list) or not scopes:
        raise AnalysisError("analysis_scopes must be a non-empty list.")
    names: set[str] = set()
    normalized_scopes: list[Mapping[str, Any]] = []
    for index, scope in enumerate(scopes):
        item = _require_mapping(scope, f"analysis_scopes[{index}]")
        name = item.get("name")
        windows = item.get("window_numbers")
        if (
            not isinstance(name, str)
            or not name
            or name in names
            or not isinstance(item.get("ticker_cell"), str)
            or not isinstance(item.get("requires_unlock"), bool)
            or not isinstance(windows, list)
            or not windows
            or any(not isinstance(window, int) or window < 1 for window in windows)
        ):
            raise AnalysisError(f"malformed analysis scope at index {index}.")
        names.add(name)
        normalized_scopes.append(item)
    gate = registry.get("minimum_completed_trades")
    if isinstance(gate, bool) or not isinstance(gate, (int, float)) or gate < 0:
        raise AnalysisError("minimum_completed_trades must be non-negative.")
    maximum = prereg["maximum_nominated_rules"]
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 1:
        raise AnalysisError("maximum_nominated_rules must be a positive integer.")
    return AnalysisContract(
        observation=_require_mapping(prereg["observation_contract"], "observation_contract"),
        rule_registry=registry,
        scopes=tuple(normalized_scopes),
        split=_require_mapping(prereg["split"], "split"),
        evidence=evidence,
        evidence_version=str(evidence_version),
        maximum_nominated_rules=maximum,
        primary_comparison=str(prereg["primary_comparison"]),
    )


class AnalysisDataset:
    """Validated metadata plus window-major, segment-specific group loading."""

    def __init__(
        self,
        root: Path,
        manifest: Mapping[str, Any],
        run_spec: Mapping[str, Any],
        contract: AnalysisContract,
        geometry: CandidateGeometry,
        windows: tuple[Window, ...],
        tickers: tuple[str, ...],
        ticker_cells: Mapping[str, str],
        group_records: Mapping[tuple[str, int], Mapping[str, Any]],
        *,
        allow_incomplete: bool,
    ) -> None:
        self.root = root
        self.manifest = manifest
        self.run_spec = run_spec
        self.contract = contract
        self.geometry = geometry
        self.windows = windows
        self.tickers = tickers
        self.ticker_cells = dict(ticker_cells)
        self.group_records = dict(group_records)
        self.allow_incomplete = allow_incomplete
        self.manifest_sha256 = _sha256(root / "manifest.json")
        self.schema_version = str(manifest["schema_version"])
        self.scope_label = str(manifest["scope"])
        self.status = str(manifest["status"])
        identity = manifest["identity"]
        self.metric_axis = tuple(identity["metric_axis"])
        self.segment_axis = tuple(identity["segment_axis"])
        self.metric_index = {name: index for index, name in enumerate(self.metric_axis)}
        self.segment_index = {name: index for index, name in enumerate(self.segment_axis)}
        self.access_log: list[tuple[str, int, str]] = []

    def resolve_scope(
        self,
        name: str = "development",
        *,
        allow_partial: bool = False,
        unlock: bool = False,
        policy_path: Path | None = None,
    ) -> ResolvedScope:
        definition = next(
            (scope for scope in self.contract.scopes if scope["name"] == name), None
        )
        if definition is None:
            raise AnalysisError(f"unknown analysis scope: {name!r}.")
        requires_unlock = bool(definition["requires_unlock"])
        evidence: Mapping[str, Any] | None = None
        if requires_unlock:
            if not unlock or policy_path is None:
                raise ScopeLockedError(
                    f"scope {name!r} requires --unlock-scope and --policy."
                )
            policy = Path(policy_path).resolve()
            if not policy.is_file():
                raise ScopeLockedError(f"frozen policy file does not exist: {policy}.")
            evidence = {
                "policy_ref": str(policy),
                "policy_sha256": _sha256(policy),
                **git_facts(),
            }
        actual_by_id = {window.window_id: window for window in self.windows}
        declared_ids = tuple(int(value) for value in definition["window_numbers"])
        missing = tuple(value for value in declared_ids if value not in actual_by_id)
        if missing and not allow_partial:
            joined = ", ".join(str(value) for value in missing)
            raise AnalysisError(
                f"scope {name!r} is missing actual window(s): {joined}; "
                "use the explicit partial-scope override to analyze the intersection."
            )
        windows = tuple(actual_by_id[value] for value in declared_ids if value in actual_by_id)
        if not windows:
            raise AnalysisError(
                f"scope {name!r} has no actual windows in its declared intersection "
                f"{list(declared_ids)}."
            )
        _validate_calendar_blocks(name, windows)
        cell = str(definition["ticker_cell"])
        tickers = tuple(symbol for symbol in self.tickers if self.ticker_cells[symbol] == cell)
        if not tickers:
            raise AnalysisError(f"scope {name!r} has no tickers in cell {cell!r}.")
        return ResolvedScope(
            name=name,
            ticker_cell=cell,
            tickers=tickers,
            ticker_cell_count=len(tickers),
            windows=windows,
            declared_window_ids=declared_ids,
            missing_window_ids=missing,
            is_partial=bool(missing),
            requires_unlock=requires_unlock,
            unlock_evidence=evidence,
        )

    def subset_scope(
        self,
        scope: ResolvedScope,
        *,
        tickers: Sequence[str] | None = None,
        window_ids: Sequence[int] | None = None,
    ) -> ResolvedScope:
        chosen_tickers = scope.tickers if tickers is None else tuple(tickers)
        if any(ticker not in scope.tickers for ticker in chosen_tickers):
            raise AnalysisError("scope subset includes a ticker outside the resolved scope.")
        if not chosen_tickers:
            raise AnalysisError(
                f"scope {scope.name!r} subset has no tickers in cell "
                f"{scope.ticker_cell!r}."
            )
        chosen_ids = (
            tuple(window.window_id for window in scope.windows)
            if window_ids is None
            else tuple(window_ids)
        )
        by_id = {window.window_id: window for window in scope.windows}
        if any(window_id not in by_id for window_id in chosen_ids):
            raise AnalysisError("scope subset includes a window outside the resolved scope.")
        if not chosen_ids:
            raise AnalysisError(
                f"scope {scope.name!r} subset has no actual windows in the requested intersection."
            )
        chosen_windows = tuple(by_id[window_id] for window_id in chosen_ids)
        _validate_calendar_blocks(scope.name, chosen_windows)
        return ResolvedScope(
            name=scope.name,
            ticker_cell=scope.ticker_cell,
            tickers=chosen_tickers,
            ticker_cell_count=scope.ticker_cell_count,
            windows=chosen_windows,
            declared_window_ids=scope.declared_window_ids,
            missing_window_ids=tuple(
                sorted(set(scope.declared_window_ids) - set(chosen_ids))
            ),
            is_partial=(
                chosen_tickers != scope.tickers
                or chosen_ids != tuple(window.window_id for window in scope.windows)
                or scope.is_partial
            ),
            requires_unlock=scope.requires_unlock,
            unlock_evidence=scope.unlock_evidence,
        )

    def _load_segment(
        self, scope: ResolvedScope, window: Window, segment: str
    ) -> dict[str, np.ndarray]:
        if segment not in self.segment_index:
            raise AnalysisError(f"dataset has no declared segment {segment!r}.")
        output: dict[str, np.ndarray] = {}
        expected_shape = (
            self.geometry.count,
            len(self.segment_axis),
            len(self.metric_axis),
        )
        for ticker in scope.tickers:
            record = self.group_records.get((ticker, window.window_id))
            if record is None:
                raise AnalysisError(
                    f"manifest has no group for {ticker} window {window.window_id}."
                )
            path = _safe_path(self.root, record.get("path"), "group")
            try:
                matrix = np.load(path, allow_pickle=False)
            except (OSError, ValueError) as exc:
                raise AnalysisError(f"cannot load group {record.get('path')}: {exc}") from exc
            declared_shape = tuple(record.get("shape", ()))
            if matrix.shape != declared_shape or matrix.shape != expected_shape:
                raise AnalysisError(
                    f"group {record.get('path')} shape {matrix.shape} does not match "
                    f"declared/axis shape {declared_shape}/{expected_shape}."
                )
            declared_dtype = str(record.get("dtype"))
            if str(matrix.dtype) != declared_dtype or declared_dtype != str(
                self.manifest["identity"]["dtype"]
            ):
                raise AnalysisError(f"group {record.get('path')} dtype mismatch.")
            output[ticker] = np.array(
                matrix[:, self.segment_index[segment], :], copy=True
            )
            self.access_log.append((ticker, window.window_id, segment))
            del matrix
        return output

    def load_is_window(
        self, scope: ResolvedScope, window: Window
    ) -> dict[str, ISView]:
        matrices = self._load_segment(scope, window, "is")
        views: dict[str, ISView] = {}
        for ticker, matrix in matrices.items():
            metrics: dict[str, np.ndarray] = {}
            for index, name in enumerate(self.metric_axis):
                values = matrix[:, index]
                values.setflags(write=False)
                metrics[name] = values
            views[ticker] = ISView(
                metrics=MappingProxyType(metrics),
                ticker=ticker,
                window_id=window.window_id,
                is_start=window.is_start,
                is_end=window.is_end,
            )
        return views

    def load_oos_window(
        self, scope: ResolvedScope, window: Window
    ) -> Mapping[str, np.ndarray]:
        return self._load_segment(scope, window, "oos")


def _candidate_geometry(
    payload: Mapping[str, Any], manifest_identity: Mapping[str, Any]
) -> CandidateGeometry:
    candidate_schema = ("strategy_lab_candidates_v2" if manifest_identity.get("dataset_schema") == "strategy_lab_dataset_v3"
                        else "strategy_lab_candidates_v1")
    if payload.get("schema_version") != candidate_schema:
        raise AnalysisError("unsupported candidates.json schema version.")
    rows = payload.get("candidates")
    count = payload.get("candidate_count")
    if not isinstance(rows, list) or isinstance(count, bool) or not isinstance(count, int):
        raise AnalysisError("candidates.json has an invalid candidate table.")
    if len(rows) != count or count != manifest_identity.get("candidate_count"):
        raise AnalysisError("candidate count disagrees with the manifest.")
    if payload.get("plan_fingerprint") != manifest_identity.get("plan_fingerprint"):
        raise AnalysisError("candidate plan fingerprint disagrees with the manifest.")
    if payload.get("semantic_key_digest") != manifest_identity.get("semantic_key_digest"):
        raise AnalysisError("candidate semantic digest disagrees with the manifest.")
    indexed: dict[int, Mapping[str, Any]] = {}
    for raw in rows:
        row = _require_mapping(raw, "candidate row")
        index = row.get("row_index")
        if isinstance(index, bool) or not isinstance(index, int) or index in indexed:
            raise AnalysisError("candidate row_index values must be unique integers.")
        indexed[index] = row
    if set(indexed) != set(range(count)):
        raise AnalysisError("candidate row_index values must cover the matrix rows exactly.")
    ordered = tuple(indexed[index] for index in range(count))
    ids = np.asarray([row.get("candidate_id") for row in ordered], dtype=object)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in ids):
        raise AnalysisError("candidate IDs must be integers.")
    ids = ids.astype(np.int64)
    keys = tuple(row.get("semantic_key") for row in ordered)
    if len(set(ids.tolist())) != count or any(not isinstance(key, str) for key in keys):
        raise AnalysisError("candidate IDs and semantic keys must be valid and unique.")
    if len(set(keys)) != count:
        raise AnalysisError("candidate semantic keys must be unique.")
    if list(ids.tolist()) != manifest_identity.get("candidate_axis"):
        raise AnalysisError("candidate ID order disagrees with the manifest axis.")
    axes = payload.get("global_axis_names")
    if not isinstance(axes, list) or any(not isinstance(axis, str) for axis in axes):
        raise AnalysisError("global_axis_names must be a string list.")
    width = len(axes)
    raw_codes: list[Sequence[int]] = []
    raw_masks: list[Sequence[bool]] = []
    for row_index, row in enumerate(ordered):
        code_row = row.get("axis_value_codes")
        mask_row = row.get("active_axis_mask")
        for field, values in (
            ("axis_value_codes", code_row),
            ("active_axis_mask", mask_row),
        ):
            if (
                not isinstance(values, Sequence)
                or isinstance(values, (str, bytes))
                or len(values) != width
                or any(isinstance(value, Sequence) and not isinstance(value, (str, bytes)) for value in values)
            ):
                raise AnalysisError(
                    f"candidate row {row_index} {field} must be a flat sequence "
                    f"of width {width}."
                )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in code_row):
            raise AnalysisError(
                f"candidate row {row_index} axis_value_codes must contain integers, not booleans."
            )
        if any(not isinstance(value, bool) for value in mask_row):
            raise AnalysisError(
                f"candidate row {row_index} active_axis_mask must contain booleans."
            )
        raw_codes.append(code_row)
        raw_masks.append(mask_row)
    codes = np.asarray(raw_codes, dtype=np.int64)
    masks = np.asarray(raw_masks, dtype=bool)
    if codes.shape != (count, len(axes)) or masks.shape != codes.shape:
        raise AnalysisError("candidate geometry width does not match global axes.")
    for row_index, (code_row, mask_row) in enumerate(zip(codes, masks)):
        if np.any(code_row[~mask_row] != -1):
            raise AnalysisError(
                f"candidate row {row_index} has a non-sentinel inactive axis code."
            )
    return CandidateGeometry(
        candidate_ids=ids,
        semantic_keys=keys,
        params=tuple(_require_mapping(row.get("params"), "candidate params") for row in ordered),
        global_axis_names=tuple(axes),
        axis_codes=codes,
        active_masks=masks,
        block_keys=tuple(
            (str(row.get("variant_name")), str(row.get("grid_mode_name")))
            for row in ordered
        ),
    )


def _quality_cells(root: Path) -> Mapping[str, str]:
    path = root / "data_quality.csv"
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise AnalysisError(f"cannot read data_quality.csv: {exc}") from exc
    cells: dict[str, str] = {}
    for row in rows:
        if row.get("segment") != "source":
            continue
        symbol = row.get("canonical_symbol")
        cell = row.get("cell")
        if not symbol or not cell or symbol in cells:
            raise AnalysisError("data_quality.csv has missing or duplicate source ownership.")
        cells[symbol] = cell
    return cells


def _git_output(root: Path, *args: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={root.as_posix()}",
            "-C",
            str(root),
            *args,
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def git_facts() -> Mapping[str, Any]:
    """Annotate the code checkout; Git availability must never block analysis."""
    try:
        return {
            "code_commit": _git_output(CODE_REPOSITORY_ROOT, "rev-parse", "HEAD"),
            "dirty_worktree": bool(
                _git_output(
                    CODE_REPOSITORY_ROOT,
                    "status",
                    "--porcelain",
                    "--untracked-files=all",
                )
            ),
        }
    except (OSError, subprocess.CalledProcessError):
        return {"code_commit": "unavailable", "dirty_worktree": True}


def open_dataset(
    dataset_root: str | Path, *, allow_incomplete: bool = False
) -> AnalysisDataset:
    root = Path(dataset_root).resolve()
    if not root.is_dir():
        raise AnalysisError(f"dataset root does not exist: {root}.")
    manifest = _load_json(root / "manifest.json", "manifest.json")
    schema = manifest.get("schema_version")
    if schema not in SUPPORTED_DATASET_SCHEMAS:
        raise AnalysisError(f"unsupported dataset schema: {schema!r}.")
    if manifest.get("status") != "complete" and not allow_incomplete:
        raise AnalysisError(
            f"dataset status is {manifest.get('status')!r}; use the explicit incomplete override."
        )
    identity = _require_mapping(manifest.get("identity"), "manifest identity")
    if identity.get("dataset_schema") != schema:
        raise AnalysisError("manifest dataset schema identities disagree.")
    metric_axis = identity.get("metric_axis")
    segment_axis = identity.get("segment_axis")
    if (
        not isinstance(metric_axis, list)
        or len(metric_axis) != len(set(metric_axis))
        or not isinstance(segment_axis, list)
        or len(segment_axis) != len(set(segment_axis))
        or "is" not in segment_axis
        or "oos" not in segment_axis
    ):
        raise AnalysisError("manifest metric/segment axes are invalid.")
    for artifact_name in ("normalized_runspec.json", "candidates.json", "data_quality.csv"):
        artifact = _require_mapping(
            _require_mapping(manifest.get("artifacts"), "manifest artifacts").get(artifact_name),
            f"artifact {artifact_name}",
        )
        artifact_path = _safe_path(root, artifact.get("path"), artifact_name)
        if not artifact_path.is_file():
            raise AnalysisError(f"manifest artifact is missing: {artifact_name}.")
        if artifact_path.stat().st_size != artifact.get("size") or _sha256(
            artifact_path
        ) != artifact.get("sha256"):
            raise AnalysisError(f"manifest artifact verification failed: {artifact_name}.")
    run_spec = _load_json(root / "normalized_runspec.json", "normalized_runspec.json")
    if schema == "strategy_lab_dataset_v3":
        try:
            from .dataset_v3 import validate_dataset_v3

            validate_dataset_v3(root, manifest, run_spec)
        except (ValueError, KeyError, TypeError, OSError) as exc:
            raise AnalysisError(f"invalid dataset v3: {exc}") from exc
    contract = _validate_contract(run_spec)
    # The Phase-0 field name is historical: its frozen identity hashes the
    # complete normalized run spec, as config.load_run_spec() does.
    prereg_digest = hashlib.sha256(_canonical_json_bytes(run_spec)).hexdigest()
    if prereg_digest != identity.get("pre_registration_sha256"):
        raise AnalysisError("normalized pre-registration disagrees with manifest identity.")
    candidates = _load_json(root / "candidates.json", "candidates.json")
    geometry = _candidate_geometry(candidates, identity)
    ticker_rows = identity.get("included_tickers")
    if not isinstance(ticker_rows, list) or not ticker_rows:
        raise AnalysisError("manifest included_tickers is invalid.")
    tickers: list[str] = []
    manifest_cells: dict[str, str] = {}
    for row in ticker_rows:
        item = _require_mapping(row, "included ticker")
        symbol, cell = item.get("canonical_symbol"), item.get("cell")
        if not isinstance(symbol, str) or not isinstance(cell, str) or symbol in manifest_cells:
            raise AnalysisError("manifest ticker/cell ownership is invalid or duplicate.")
        tickers.append(symbol)
        manifest_cells[symbol] = cell
    quality_cells = _quality_cells(root)
    if quality_cells != manifest_cells:
        missing = sorted(set(manifest_cells) - set(quality_cells))
        unknown = sorted(set(quality_cells) - set(manifest_cells))
        conflict = sorted(
            symbol
            for symbol in set(manifest_cells) & set(quality_cells)
            if manifest_cells[symbol] != quality_cells[symbol]
        )
        raise AnalysisError(
            "manifest/data-quality ticker ownership disagrees "
            f"(missing={missing}, unknown={unknown}, conflicting={conflict})."
        )
    raw_windows = identity.get("windows")
    if not isinstance(raw_windows, list) or not raw_windows:
        raise AnalysisError("manifest actual windows are missing.")
    windows: list[Window] = []
    window_ids: set[int] = set()
    for raw in raw_windows:
        item = _require_mapping(raw, "manifest window")
        window_id = item.get("window_id")
        if isinstance(window_id, bool) or not isinstance(window_id, int) or window_id in window_ids:
            raise AnalysisError("manifest window IDs must be unique integers.")
        window_ids.add(window_id)
        try:
            windows.append(
                Window(
                    window_id=window_id,
                    is_start=str(item["is_start"]),
                    is_end=str(item["is_end"]),
                    oos_start=str(item["oos_start"]),
                    oos_end=str(item["oos_end"]),
                )
            )
        except KeyError as exc:
            raise AnalysisError("manifest window boundary is missing.") from exc
    raw_groups = manifest.get("groups")
    if not isinstance(raw_groups, list):
        raise AnalysisError("manifest groups must be a list.")
    records_by_path: dict[str, Mapping[str, Any]] = {}
    for raw in raw_groups:
        record = _require_mapping(raw, "manifest group")
        relative = record.get("path")
        path = _safe_path(root, relative, "group")
        if not path.is_file() or relative in records_by_path:
            raise AnalysisError(f"manifest group is missing or duplicate: {relative!r}.")
        records_by_path[str(relative).replace("\\", "/")] = record
    group_records: dict[tuple[str, int], Mapping[str, Any]] = {}
    for symbol in tickers:
        for window in windows:
            key = f"groups/{symbol}/window_{window.window_id:02d}.npy"
            record = records_by_path.get(key)
            if record is None:
                raise AnalysisError(
                    f"manifest has no listed group for {symbol} window {window.window_id}."
                )
            group_records[(symbol, window.window_id)] = record
    if len(group_records) != len(records_by_path):
        extras = sorted(set(records_by_path) - {
            str(record["path"]).replace("\\", "/") for record in group_records.values()
        })
        raise AnalysisError(f"manifest has unknown group records: {extras}.")
    return AnalysisDataset(
        root,
        manifest,
        run_spec,
        contract,
        geometry,
        tuple(windows),
        tuple(tickers),
        manifest_cells,
        group_records,
        allow_incomplete=allow_incomplete,
    )
