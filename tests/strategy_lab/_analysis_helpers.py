from pathlib import Path
import csv
import json
import hashlib
import numpy as np
from tools.strategy_lab.analysis.dataset import CandidateGeometry, ISView, open_dataset
from tools.strategy_lab.analysis.json_utils import canonical_json_bytes

METRICS = (
    "profit_factor",
    "net_profit_pct",
    "total_trades",
    "sharpe_daily",
    "max_drawdown_mtm_pct",
    "max_drawdown_pct",
    "gross_profit",
    "gross_loss",
    "win_rate_pct",
    "sqn",
    "rejected_fill_count",
    "zero_size_entry_count",
    "invalid_stop_distance_count",
    "flags",
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _contract(window_ids=(1, 2)):
    selectable = [
        "primary_profit",
        "trade_gate15_profit",
        "trade_gate15_profit_factor",
        "trade_gate15_daily_sharpe",
        "trade_gate15_romad_mtm",
        "star_mean_profit",
        "star_worst_profit",
        "balanced_percentile_raw",
        "balanced_percentile_star",
    ]
    diagnostics = [
        "trade_gate15_romad_realized",
        "balanced_percentile_raw_realized",
        "balanced_percentile_star_realized",
        "pareto_plus_primary",
        "population_no_skill",
        "oos_oracle",
        "oos_anti_oracle",
    ]
    evidence = {
        "broad_population_edge": {
            "median_candidate_oos_net_profit_pct": {"operator": ">", "value": 0},
            "profitable_observation_share": {"operator": ">", "value": 0.5},
            "positive_monthly_population_medians": {"operator": ">=", "value": 1, "of": 2},
        },
        "selected_strategy_viability": {
            "six_month_headline_mean_oos_net_profit_pct": {"operator": ">", "value": 0},
            "median_selected_oos_net_profit_pct": {"operator": ">", "value": 0},
            "profitable_selected_observation_share": {"operator": ">", "value": 0.5},
            "positive_monthly_selected_means": {"operator": ">=", "value": 1, "of": 2},
        },
        "selection_lift": {
            "comparison": "rule_selected_minus_primary_profit_oos_net_profit_pct",
            "six_month_equal_weight_monthly_mean": {"operator": ">", "value": 0},
            "positive_monthly_top1_paired_means": {"operator": ">=", "value": 1, "of": 2},
            "top5_equal_weight_monthly_mean": {"operator": ">=", "value": 0},
            "robust_top1_headline_mean": {"operator": ">", "value": 0},
        },
        "nomination": {"maximum_rules": 3},
        "outlier_procedure": {
            "contribution": "ticker_mean_top1_paired_lift_over_six_windows",
            "removal_candidates": "strictly_positive_contributors_only",
            "quota": "ceil(0.10*total_ticker_count_in_cell)",
            "remove_count": "min(quota,positive_contributor_count)",
            "order": ["mean_contribution_desc", "canonical_symbol_asc"],
            "recompute": "monthly_means_then_equal_weight_six_month_headline",
            "criterion_scope": "recomputed_headline_only_monthly_signs_descriptive",
        },
        "primary_confirmation": {"cell": "holdout"},
        "temporal_windows_7_8": {"descriptive_only": True},
        "uncertainty": {
            "type": "descriptive_month_block_bootstrap",
            "blocks": 2,
            "resamples": 50,
            "sample_size_per_resample": 2,
            "sampling": "with_replacement",
            "seed": 17,
            "percentiles": [2.5, 97.5],
            "pass_fail_criterion": False,
        },
    }
    return {
        "observation_contract": {
            "candidate_observation": "candidate_ticker_window",
            "monthly_ticker_aggregation": "equal_weight_valid_tickers",
            "six_month_headline_aggregation": "equal_weight_six_monthly_means",
            "headline_median": "all_valid_ticker_window_observations",
            "missing_required_value": "fail_or_mark_rule_pair_unavailable_with_count",
            "natural_zero_trade_net_profit_pct": 0,
            "natural_zero_trade_is_profitable": False,
            "natural_zero_trade_population_membership": "included",
            "unavailable_non_finite_metric": "excluded_from_selection_not_zero_filled",
        },
        "rule_registry": {
            "version": "strategy_lab_rules_v1",
            "minimum_completed_trades": 15,
            "baseline_rule": "primary_profit",
            "selectable_rules": selectable,
            "nomination_eligible_rules": selectable[1:],
            "non_nominatable_diagnostics": diagnostics,
            "tie_break": [
                {"field": "rule_score", "direction": "descending"},
                {"field": "IS net_profit_pct", "direction": "descending"},
                {"field": "semantic_key", "direction": "ascending"},
                {"field": "candidate_id", "direction": "ascending"},
            ],
        },
        "analysis_scopes": [
            {"name": "development", "ticker_cell": "dev", "window_numbers": list(window_ids), "requires_unlock": False},
            {"name": "holdout", "ticker_cell": "holdout", "window_numbers": list(window_ids), "requires_unlock": True},
        ],
        "split": {"development_ticker_count": 2, "holdout_ticker_count": 1},
        "evidence_criteria": evidence,
        "evidence_criteria_version": "strategy_lab_evidence_v1",
        "maximum_nominated_rules": 3,
        "primary_comparison": "top1_oos_net_profit_vs_primary_profit",
    }


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _rewrite_runspec(root: Path, mutate) -> None:
    path = root / "normalized_runspec.json"
    run_spec = json.loads(path.read_text())
    mutate(run_spec)
    _write_json(path, run_spec)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["identity"]["pre_registration_sha256"] = hashlib.sha256(
        canonical_json_bytes(run_spec)
    ).hexdigest()
    manifest["artifacts"]["normalized_runspec.json"].update(
        sha256=_sha(path), size=path.stat().st_size
    )
    _write_json(manifest_path, manifest)


def _rewrite_candidates(root: Path, mutate) -> None:
    path = root / "candidates.json"
    candidates = json.loads(path.read_text())
    mutate(candidates)
    _write_json(path, candidates)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["artifacts"]["candidates.json"].update(
        sha256=_sha(path), size=path.stat().st_size
    )
    _write_json(manifest_path, manifest)


def _remove_metric(root: Path, metric: str) -> None:
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    axis = manifest["identity"]["metric_axis"]
    index = axis.index(metric)
    axis.pop(index)
    for record in manifest["groups"]:
        path = root / record["path"]
        matrix = np.load(path)
        matrix = np.delete(matrix, index, axis=2)
        np.save(path, matrix)
        record.update(
            sha256=_sha(path),
            size=path.stat().st_size,
            shape=list(matrix.shape),
        )
    _write_json(manifest_path, manifest)


def _synthetic_dataset(
    tmp_path: Path,
    *,
    actual_windows=(1, 2),
    metric_axis=METRICS,
    tickers=None,
    declared_window_ids=(1, 2),
    declared_dev_pool=None,
) -> Path:
    root = tmp_path / "dataset"
    root.mkdir(parents=True)
    prereg = _contract(declared_window_ids)
    if declared_dev_pool is not None:
        prereg["split"]["development_ticker_count"] = declared_dev_pool
    run_spec = {"schema_version": "strategy_lab_runspec_v1", "run_name": "synthetic", "generation": {}, "preregistration": prereg}
    _write_json(root / "normalized_runspec.json", run_spec)
    candidates = {
        "schema_version": "strategy_lab_candidates_v1",
        "candidate_count": 4,
        "global_axis_names": ["x"],
        "plan_fingerprint": "plan",
        "semantic_key_digest": "semantic",
        "identity_versions": {},
        "candidates": [
            {
                "row_index": row,
                "candidate_id": 10 + row * 10,
                "semantic_key": key,
                "params": {"x": row},
                "axis_value_codes": [row],
                "active_axis_mask": [True],
                "variant_name": "v",
                "grid_mode_name": "b",
            }
            for row, key in enumerate(("z", "a", "m", "x"))
        ],
    }
    _write_json(root / "candidates.json", candidates)
    if tickers is None:
        tickers = (("AAA", "dev"), ("BBB", "dev"), ("HHH", "holdout"))
    with (root / "data_quality.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("segment", "canonical_symbol", "cell"))
        writer.writeheader()
        for ticker, cell in tickers:
            writer.writerow({"segment": "source", "canonical_symbol": ticker, "cell": cell})
    windows = [
        {
            "window_id": number,
            "is_start": f"2026-0{number}-01T00:00:00Z",
            "is_end": f"2026-0{number}-28T23:30:00Z",
            "oos_start": f"2026-0{number + 2}-01T00:00:00Z",
            "oos_end": f"2026-0{number + 2}-28T23:30:00Z",
        }
        for number in actual_windows
    ]
    groups = []
    index = {name: pos for pos, name in enumerate(metric_axis)}
    for ticker_index, (ticker, _) in enumerate(tickers):
        folder = root / "groups" / ticker
        folder.mkdir(parents=True)
        for window in windows:
            matrix = np.zeros((4, 2, len(metric_axis)), dtype=np.float64)
            for segment in range(2):
                matrix[:, segment, index["net_profit_pct"]] = np.array([-1.0, 2.0, 0.0, 1.0]) + ticker_index + window["window_id"] + segment
                matrix[:, segment, index["total_trades"]] = [20.0, 20.0, 0.0, 20.0]
                matrix[:, segment, index["profit_factor"]] = [1.0, 2.0, np.nan, 1.5]
                matrix[:, segment, index["sharpe_daily"]] = [0.1, 0.2, np.nan, 0.3]
                matrix[:, segment, index["max_drawdown_mtm_pct"]] = [2.0, 2.0, 0.0, 1.0]
                matrix[:, segment, index["max_drawdown_pct"]] = [3.0, 3.0, 0.0, 1.5]
                matrix[:, segment, index["gross_profit"]] = [2.0, 3.0, 0.0, 2.0]
                matrix[:, segment, index["gross_loss"]] = [1.0, 1.0, 0.0, 1.0]
                matrix[:, segment, index["win_rate_pct"]] = [40.0, 50.0, 0.0, 45.0]
                matrix[:, segment, index["sqn"]] = np.nan
                matrix[:, segment, index["flags"]] = [2.0, 0.0, 1.0, 0.0]
            path = folder / f"window_{window['window_id']:02d}.npy"
            np.save(path, matrix)
            groups.append({
                "path": path.relative_to(root).as_posix(),
                "sha256": _sha(path),
                "size": path.stat().st_size,
                "shape": list(matrix.shape),
                "dtype": "float64",
            })
    artifacts = {}
    for name in ("normalized_runspec.json", "candidates.json", "data_quality.csv"):
        path = root / name
        artifacts[name] = {"path": name, "sha256": _sha(path), "size": path.stat().st_size}
    manifest = {
        "schema_version": "strategy_lab_dataset_v2",
        "scope": "full" if len(actual_windows) == 2 else "smoke",
        "status": "complete",
        "artifacts": artifacts,
        "identity": {
            "dataset_schema": "strategy_lab_dataset_v2",
            "metric_axis": list(metric_axis),
            "segment_axis": ["is", "oos"],
            "dtype": "float64",
            "candidate_count": 4,
            "candidate_axis": [10, 20, 30, 40],
            "plan_fingerprint": "plan",
            "semantic_key_digest": "semantic",
            "pre_registration_sha256": hashlib.sha256(canonical_json_bytes(run_spec)).hexdigest(),
            "included_tickers": [
                {"canonical_symbol": ticker, "cell": cell} for ticker, cell in tickers
            ],
            "windows": windows,
        },
        "groups": groups,
    }
    _write_json(root / "manifest.json", manifest)
    return root


def _geometry(*, keys=("z", "a", "m", "x")) -> CandidateGeometry:
    return CandidateGeometry(
        candidate_ids=np.array([10, 20, 30, 40]),
        semantic_keys=tuple(keys),
        params=tuple({"x": index} for index in range(4)),
        global_axis_names=("x",),
        axis_codes=np.array([[0], [1], [2], [3]]),
        active_masks=np.ones((4, 1), dtype=bool),
        block_keys=(("v", "b"),) * 4,
    )


def _view(**overrides) -> ISView:
    metrics = {
        "net_profit_pct": np.array([1.0, 4.0, 3.0, 2.0]),
        "total_trades": np.array([20.0, 20.0, 10.0, 20.0]),
        "profit_factor": np.array([1.0, 2.0, 9.0, 3.0]),
        "sharpe_daily": np.array([0.1, 0.3, 8.0, 0.2]),
        "max_drawdown_mtm_pct": np.array([2.0, 2.0, 1.0, 4.0]),
        "max_drawdown_pct": np.array([4.0, 2.0, 1.0, 2.0]),
    }
    metrics.update(overrides)
    return ISView(metrics=metrics, ticker="AAA", window_id=1, is_start="is0", is_end="is1")



def mutate_group(root, ticker, window, change):
    dataset = open_dataset(root)
    path = root/f"groups/{ticker}/window_{window:02d}.npy"
    matrix = np.load(path)
    change(matrix, dataset.metric_index)
    np.save(path, matrix)
    manifest = json.loads((root/"manifest.json").read_text())
    for row in manifest["groups"]:
        if row["path"] == path.relative_to(root).as_posix():
            row.update(sha256=_sha(path), size=path.stat().st_size)
    (root/"manifest.json").write_text(json.dumps(manifest))


