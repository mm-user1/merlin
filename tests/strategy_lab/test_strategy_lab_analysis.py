from __future__ import annotations

import csv
import gc
import hashlib
import json
import subprocess
import weakref
from pathlib import Path

import numpy as np
import pytest

from tools.strategy_lab.analysis.dataset import (
    AnalysisError,
    CandidateGeometry,
    ISView,
    ScopeLockedError,
    git_facts,
    open_dataset,
)
from tools.strategy_lab.analysis.cli import main as analysis_main
from tools.strategy_lab.analysis.evaluate import (
    CustomRule,
    descriptive_bootstrap,
    evaluate_evidence,
    evaluate_scope,
    monthly_headline,
    outlier_robustness,
    profitable_share,
)
from tools.strategy_lab.analysis.output import OUTPUT_FILES, render_files, write_analysis
from tools.strategy_lab.analysis.rules import (
    RuleResult,
    evaluate_custom_rule,
    evaluate_rule,
    percentile_rank,
    percentile_scores,
    select_candidates,
    star_neighbours,
)
from tools.strategy_lab.analysis.json_utils import canonical_json_bytes


from _analysis_helpers import (
    METRICS, _sha, _contract, _write_json, _rewrite_runspec, _rewrite_candidates, _remove_metric, _synthetic_dataset, _geometry, _view
)


def test_reader_uses_declared_axes_nonzero_ids_and_structural_is_view(tmp_path):
    root = _synthetic_dataset(tmp_path, metric_axis=tuple(reversed(METRICS)))
    before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    dataset = open_dataset(root)
    assert dataset.schema_version == "strategy_lab_dataset_v2" and dataset.run_spec["generation"] == {}
    scope = dataset.resolve_scope()
    assert dataset.geometry.candidate_ids.tolist() == [10, 20, 30, 40]
    view = dataset.load_is_window(scope, scope.windows[0])["AAA"]
    assert view.metrics["net_profit_pct"].tolist() == [0.0, 3.0, 1.0, 2.0]
    assert not hasattr(view, "oos")
    assert not hasattr(view, "load_oos")
    assert not view.metrics["net_profit_pct"].flags.writeable
    assert before == {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_outside_worktree_dataset_uses_code_repository_git_facts(tmp_path):
    inside_worktree = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        text=True,
    ).returncode == 0
    if inside_worktree:
        pytest.skip("basetemp is inside a Git worktree; provenance test needs an external path")
    root = _synthetic_dataset(tmp_path)
    result = evaluate_scope(open_dataset(root), open_dataset(root).resolve_scope())
    repository = Path(__file__).resolve().parents[2]
    expected = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()
    assert result.run_metadata["git"]["code_commit"] == expected


def test_git_unavailability_is_conservative_and_holdout_uses_same_provenance(
    tmp_path, monkeypatch
):
    import tools.strategy_lab.analysis.dataset as dataset_module

    root = _synthetic_dataset(tmp_path)
    dataset = open_dataset(root)
    policy = tmp_path / "policy.json"
    policy.write_text('{"frozen":true}', encoding="utf-8")

    def unavailable(*args, **kwargs):
        raise OSError("git unavailable")

    monkeypatch.setattr(dataset_module.subprocess, "run", unavailable)
    assert git_facts() == {"code_commit": "unavailable", "dirty_worktree": True}
    unlocked = dataset.resolve_scope("holdout", unlock=True, policy_path=policy)
    assert unlocked.unlock_evidence["code_commit"] == "unavailable"
    assert unlocked.unlock_evidence["dirty_worktree"] is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", "strategy_lab_dataset_v99", "unsupported dataset schema"),
        ("status", "partial", "dataset status"),
    ],
)
def test_reader_rejects_schema_and_status(tmp_path, field, value, message):
    root = _synthetic_dataset(tmp_path)
    manifest = json.loads((root / "manifest.json").read_text())
    manifest[field] = value
    if field == "schema_version":
        manifest["identity"]["dataset_schema"] = value
    _write_json(root / "manifest.json", manifest)
    with pytest.raises(AnalysisError, match=message):
        open_dataset(root)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda run_spec: run_spec["preregistration"]["rule_registry"].update(version="rules_v99"),
            "unsupported rule registry version",
        ),
        (
            lambda run_spec: run_spec["preregistration"].update(evidence_criteria_version="evidence_v99"),
            "unsupported evidence criteria version",
        ),
        (
            lambda run_spec: run_spec["preregistration"]["rule_registry"]["tie_break"][0].update(field="mystery"),
            "unknown rule tie-break contract",
        ),
        (
            lambda run_spec: run_spec["preregistration"]["evidence_criteria"]["broad_population_edge"]["median_candidate_oos_net_profit_pct"].update(operator="=="),
            "malformed evidence leaf",
        ),
    ],
)
def test_reader_rejects_unknown_contract_versions_operators_and_ties(tmp_path, mutate, message):
    root = _synthetic_dataset(tmp_path)
    _rewrite_runspec(root, mutate)
    with pytest.raises(AnalysisError, match=message):
        open_dataset(root)


def test_reader_rejects_quality_disagreement_and_escaping_group(tmp_path):
    root = _synthetic_dataset(tmp_path)
    manifest = json.loads((root / "manifest.json").read_text())
    manifest["groups"][0]["path"] = "../escape.npy"
    _write_json(root / "manifest.json", manifest)
    with pytest.raises(AnalysisError, match="escapes the dataset root"):
        open_dataset(root)
    root = _synthetic_dataset(tmp_path / "other")
    text = (root / "data_quality.csv").read_text().replace("AAA,dev", "AAA,holdout")
    (root / "data_quality.csv").write_text(text)
    manifest = json.loads((root / "manifest.json").read_text())
    artifact = root / "data_quality.csv"
    manifest["artifacts"]["data_quality.csv"].update(sha256=_sha(artifact), size=artifact.stat().st_size)
    _write_json(root / "manifest.json", manifest)
    with pytest.raises(AnalysisError, match="ownership disagrees"):
        open_dataset(root)


def test_partial_scope_and_synthetic_holdout_lock(tmp_path):
    root = _synthetic_dataset(tmp_path, actual_windows=(1,))
    dataset = open_dataset(root)
    with pytest.raises(AnalysisError, match="missing actual window.*2"):
        dataset.resolve_scope()
    partial = dataset.resolve_scope(allow_partial=True)
    assert partial.missing_window_ids == (2,)
    with pytest.raises(ScopeLockedError, match="requires --unlock-scope"):
        dataset.resolve_scope("holdout")
    policy = tmp_path / "policy.json"
    policy.write_text('{"freeze":true}')
    unlocked = dataset.resolve_scope("holdout", allow_partial=True, unlock=True, policy_path=policy)
    assert unlocked.unlock_evidence["policy_sha256"] == _sha(policy)


def test_empty_scope_intersections_and_subsets_fail_cleanly(tmp_path):
    dataset = open_dataset(_synthetic_dataset(tmp_path, actual_windows=(3,)))
    with pytest.raises(AnalysisError, match="no actual windows.*intersection"):
        dataset.resolve_scope(allow_partial=True)
    normal = open_dataset(_synthetic_dataset(tmp_path / "normal"))
    scope = normal.resolve_scope()
    with pytest.raises(AnalysisError, match="subset has no tickers"):
        normal.subset_scope(scope, tickers=[])
    with pytest.raises(AnalysisError, match="subset has no actual windows"):
        normal.subset_scope(scope, window_ids=[])


def test_duplicate_calendar_block_keys_are_rejected(tmp_path):
    root = _synthetic_dataset(tmp_path)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["identity"]["windows"][1]["oos_start"] = manifest["identity"]["windows"][0]["oos_start"]
    manifest["identity"]["windows"][1]["oos_end"] = manifest["identity"]["windows"][0]["oos_end"]
    _write_json(manifest_path, manifest)
    dataset = open_dataset(root)
    with pytest.raises(AnalysisError, match="duplicate UTC OOS block"):
        dataset.resolve_scope()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.pop("axis_value_codes"),
        lambda row: row.pop("active_axis_mask"),
        lambda row: row.update(axis_value_codes=[[0]]),
        lambda row: row.update(axis_value_codes=[True]),
        lambda row: row.update(active_axis_mask=[1]),
    ],
)
def test_malformed_candidate_geometry_is_analysis_error_and_cli_exit_two(
    tmp_path, capsys, mutate
):
    root = _synthetic_dataset(tmp_path)
    _rewrite_candidates(root, lambda payload: mutate(payload["candidates"][0]))
    with pytest.raises(AnalysisError, match="candidate row 0"):
        open_dataset(root)
    assert analysis_main(
        ["analyze", "--dataset", str(root), "--output", str(tmp_path / "out")]
    ) == 2
    captured = capsys.readouterr()
    assert "Strategy Lab analysis error: candidate row 0" in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize(
    ("rule", "expected_scores", "expected_eligible"),
    [
        ("primary_profit", [1.0, 4.0, 3.0, 2.0], [1, 1, 1, 1]),
        ("trade_gate15_profit", [1.0, 4.0, 3.0, 2.0], [1, 1, 0, 1]),
        ("trade_gate15_profit_factor", [1.0, 2.0, 9.0, 3.0], [1, 1, 0, 1]),
        ("trade_gate15_daily_sharpe", [0.1, 0.3, 8.0, 0.2], [1, 1, 0, 1]),
        ("trade_gate15_romad_mtm", [0.5, 2.0, None, 0.5], [1, 1, 0, 1]),
    ],
)
def test_direct_rule_formulas(rule, expected_scores, expected_eligible):
    result = evaluate_rule(rule, _view(), _geometry(), 15)
    actual = [None if not np.isfinite(value) else value for value in result.score]
    assert actual == expected_scores
    assert result.eligible.astype(int).tolist() == expected_eligible


def test_missing_metric_nan_and_natural_zero_remain_distinct():
    geometry = _geometry()
    missing = ISView({"net_profit_pct": np.array([0.0, np.nan, 1.0, 2.0])}, "A", 1, "a", "b")
    primary = evaluate_rule("primary_profit", missing, geometry, 15)
    assert primary.eligible.tolist() == [True, False, True, True]
    with pytest.raises(AnalysisError, match="unsupported for dataset; missing: total_trades"):
        evaluate_rule("trade_gate15_profit", missing, geometry, 15)


def test_missing_mtm_marks_only_dependent_rules_unsupported(tmp_path):
    root = _synthetic_dataset(tmp_path)
    _remove_metric(root, "max_drawdown_mtm_pct")
    dataset = open_dataset(root)
    result = evaluate_scope(dataset, dataset.resolve_scope())
    assert result.run_metadata["rule_states"]["trade_gate15_romad_mtm"] == {
        "status": "unsupported_for_dataset",
        "missing_metrics": ["max_drawdown_mtm_pct"],
        "kind": "selectable",
    }
    assert "trade_gate15_romad_mtm" not in result.summary["rules"]
    assert result.summary["rules"]["primary_profit"]["selected_pairs"] == 4
    unavailable = result.summary["evidence_by_selectable_rule"]["trade_gate15_romad_mtm"]
    assert len(unavailable) == 11
    assert all(row["status"] == "unavailable" for row in unavailable)
    assert all("missing metrics" in row["reason"] for row in unavailable)


def test_star_geometry_respects_active_mask_variant_and_ordered_domains():
    geometry = CandidateGeometry(
        candidate_ids=np.arange(100, 106),
        semantic_keys=tuple(f"k{x}" for x in range(6)),
        params=tuple({} for _ in range(6)),
        global_axis_names=("x", "y"),
        axis_codes=np.array([[0, -1], [2, -1], [4, -1], [0, -1], [0, 0], [0, 2]]),
        active_masks=np.array([[1, 0], [1, 0], [1, 0], [1, 0], [1, 1], [1, 1]], dtype=bool),
        block_keys=(("A", "b"), ("A", "b"), ("A", "b"), ("B", "b"), ("A", "b"), ("A", "b")),
    )
    neighbours = star_neighbours(geometry)
    assert neighbours[0].tolist() == [0, 1]
    assert neighbours[1].tolist() == [0, 1, 2]
    assert 3 not in neighbours[0] and 4 not in neighbours[0]
    assert neighbours[4].tolist() == [4, 5]


def test_star_profit_and_balanced_star_use_exact_valid_set():
    geometry = _geometry()
    neighbours = star_neighbours(geometry)
    mean = evaluate_rule("star_mean_profit", _view(), geometry, 15, neighbours=neighbours)
    worst = evaluate_rule("star_worst_profit", _view(), geometry, 15, neighbours=neighbours)
    assert mean.score[[0, 1, 3]].tolist() == [2.5, 2.5, 2.0]
    assert worst.score[[0, 1, 3]].tolist() == [1.0, 1.0, 2.0]
    assert np.isnan(mean.score[2]) and np.isnan(worst.score[2])
    view = _view(profit_factor=np.array([1.0, np.nan, 4.0, 3.0]))
    balanced = evaluate_rule("balanced_percentile_star", view, geometry, 15, neighbours=neighbours)
    assert not balanced.eligible[1]
    assert balanced.support["star_support_profit_factor"].tolist() == [1, 0, 0, 1]


def test_balanced_percentile_raw_matches_hand_computed_components():
    result = evaluate_rule("balanced_percentile_raw", _view(), _geometry(), 15)
    assert result.eligible.tolist() == [True, True, False, True]
    assert result.score[[0, 1, 3]].tolist() == pytest.approx([0.25, 0.75, 0.5])


def test_percentile_formula_ties_and_small_n():
    values = np.array([1.0, 1.0, 3.0, np.nan])
    assert percentile_scores(values, np.array([1, 1, 1, 1], dtype=bool)).tolist()[:3] == [0.25, 0.25, 1.0]
    assert np.isnan(percentile_scores(values, np.array([1, 0, 0, 0], dtype=bool))).all()


def test_three_way_rank1_tie_uses_verbatim_semantic_key_and_records_tie():
    geometry = _geometry(keys=("β", "A", "a", "Z"))
    result = RuleResult("tie", np.array([5.0, 5.0, 5.0, 1.0]), np.ones(4, bool), (), {})
    selection = select_candidates(result, np.array([2.0, 2.0, 2.0, 9.0]), geometry)
    assert selection.candidate_ids[:3] == (20, 30, 10)
    assert selection.tie_depth_at_rank1 == 3
    assert selection.tie_break_level_used == "semantic_key"


def test_pareto_strict_epsilon_and_topk_percentile_semantics():
    view = _view(
        net_profit_pct=np.array([4.0, 3.0, 2.0, 1.0]),
        max_drawdown_mtm_pct=np.array([2.0, 2.0 - 5e-13, 1.0, 3.0]),
        total_trades=np.full(4, 20.0),
    )
    result = evaluate_rule("pareto_plus_primary", view, _geometry(), 15)
    assert result.eligible.tolist() == [True, False, True, False]
    values = np.array([0.0, 10.0, 20.0, 30.0])
    assert percentile_rank(values, 1) == pytest.approx(1 / 3)
    assert np.mean([percentile_rank(values, row) for row in (1, 3)]) == pytest.approx(2 / 3)


def test_statistic_specific_aggregation_and_all_cell_denominator():
    values = np.array([[100.0, 1.0], [np.nan, 1.0], [np.nan, 1.0]])
    monthly, headline = monthly_headline(values)
    assert monthly.tolist() == [100.0, 1.0]
    assert headline == 50.5
    assert np.nanmean(values) == pytest.approx(25.75)
    assert profitable_share(np.array([[1.0, np.nan], [-1.0, np.nan]]), denominator=4) == 0.25


def test_all_eleven_evidence_leaves_and_metadata_blocks_not_evaluated():
    evidence = _contract()["evidence_criteria"]
    population = {"median": 1.0, "profitable_share": 0.75, "positive_monthly_medians": 2}
    rule = {
        "top1_headline_mean": 1.0,
        "top1_pooled_median": 1.0,
        "profitable_selected_observation_share": 0.75,
        "positive_monthly_selected_means": 2,
        "top1_lift_headline": 0.5,
        "positive_monthly_top1_lift": 2,
        "top5_lift_headline": 0.0,
        "robustness": {"robust_lift_headline": 0.1},
    }
    rows = evaluate_evidence(evidence, population, rule, actual_block_count=2)
    assert len(rows) == 11 and all(row["status"] == "pass" for row in rows)
    assert not any(row["criterion"].startswith("nomination.") for row in rows)
    partial = evaluate_evidence(evidence, population, rule, actual_block_count=1)
    assert next(row for row in partial if row["criterion"].endswith("positive_monthly_population_medians"))["status"] == "unavailable"


def test_outlier_symbol_tie_and_robust_lift_not_absolute_profit():
    lift = np.array([[2.0, 2.0], [2.0, 2.0], [-1.0, -1.0]])
    selected = np.array([[100.0, 100.0], [-50.0, -50.0], [5.0, 5.0]])
    result = outlier_robustness(lift, selected, ("BBB", "AAA", "CCC"), _contract()["evidence_criteria"]["outlier_procedure"])
    assert result["removed_tickers"] == ["AAA"]
    assert result["robust_lift_headline"] == 0.5
    assert result["robust_absolute_selected_headline"] == 52.5
    malformed = dict(_contract()["evidence_criteria"]["outlier_procedure"])
    malformed["criterion_scope"] = "unknown"
    with pytest.raises(AnalysisError, match="unsupported outlier procedure"):
        outlier_robustness(lift, selected, ("BBB", "AAA", "CCC"), malformed)


def test_bounded_outlier_quota_keeps_full_ticker_cell_authority(tmp_path):
    dataset = open_dataset(_synthetic_dataset(tmp_path))
    full = dataset.resolve_scope()
    subset = dataset.subset_scope(full, tickers=(full.tickers[0],))
    assert subset.ticker_cell_count == 2
    result = outlier_robustness(
        np.ones((1, 2)),
        np.ones((1, 2)),
        subset.tickers,
        _contract()["evidence_criteria"]["outlier_procedure"],
        ticker_cell_count=11,
    )
    assert result["ticker_cell_count"] == 11 and result["quota"] == 2


def test_bootstrap_is_fresh_order_independent_and_requires_all_blocks():
    uncertainty = _contract()["evidence_criteria"]["uncertainty"]
    first = descriptive_bootstrap([1.0, 3.0], uncertainty)
    descriptive_bootstrap([-100.0, 100.0], uncertainty)
    second = descriptive_bootstrap([1.0, 3.0], uncertainty)
    assert first == second
    assert descriptive_bootstrap([2.0, 2.0], uncertainty)["width"] == 0.0
    unavailable = descriptive_bootstrap([2.0, np.nan], uncertainty)
    assert unavailable["status"] == "unavailable" and "observed 1" in unavailable["reason"]


def test_all_four_bootstrap_headlines_and_robust_series_are_bound(tmp_path):
    dataset = open_dataset(_synthetic_dataset(tmp_path))
    result = evaluate_scope(dataset, dataset.resolve_scope())
    uncertainty = dataset.contract.evidence["uncertainty"]
    for name in dataset.contract.rule_registry["selectable_rules"]:
        mappings = result.summary["rules"][name]["bootstraps"]
        assert set(mappings) == {
            "top1_absolute",
            "top1_lift",
            "top5_lift",
            "robust_top1_lift",
        }
        for interval in mappings.values():
            assert set(interval) == {
                "effect", "status", "reason", "block_count", "lower", "upper", "width"
            }
        robust = result.summary["rules"][name]["robustness"]
        expected = descriptive_bootstrap(robust["robust_monthly_lift_descriptive"], uncertainty)
        assert mappings["robust_top1_lift"] == {
            "effect": robust["robust_lift_headline"], **expected
        }


def test_each_bootstrap_headline_is_unavailable_with_insufficient_blocks(tmp_path):
    root = _synthetic_dataset(tmp_path)
    _rewrite_runspec(
        root,
        lambda run_spec: run_spec["preregistration"]["evidence_criteria"]["uncertainty"].update(blocks=3),
    )
    dataset = open_dataset(root)
    result = evaluate_scope(dataset, dataset.resolve_scope())
    for interval in result.summary["rules"]["primary_profit"]["bootstraps"].values():
        assert interval["status"] == "unavailable"
        assert interval["block_count"] == 2


def test_custom_callable_validation_and_exploratory_label():
    result = evaluate_custom_rule("custom", "v1", lambda view, geometry, context: np.arange(4.0), _view(), _geometry(), {})
    assert result.exploratory and result.eligible.all()
    with pytest.raises(AnalysisError, match="align"):
        evaluate_custom_rule("bad", "v1", lambda *args: np.arange(3.0), _view(), _geometry(), {})
    with pytest.raises(AnalysisError, match="non-finite"):
        evaluate_custom_rule("bad", "v1", lambda *args: RuleResult("bad", np.array([1.0, np.nan, 2.0, 3.0]), np.ones(4, bool), (), {}), _view(), _geometry(), {})


def test_custom_rule_cannot_mutate_is_or_change_official_tie_break(tmp_path):
    root = _synthetic_dataset(tmp_path)
    baseline_dataset = open_dataset(root)
    baseline = evaluate_scope(baseline_dataset, baseline_dataset.resolve_scope())
    baseline_ids = [
        row["selected_candidate_ids"]
        for row in baseline.pair_decisions
        if row["rule"] == "primary_profit"
    ]
    mutation_failures = []

    def attempted_mutation(view, geometry, context):
        try:
            view.metrics["net_profit_pct"][0] = 999.0
        except ValueError:
            mutation_failures.append(True)
        return view.metrics["net_profit_pct"]

    dataset = open_dataset(root)
    result = evaluate_scope(
        dataset,
        dataset.resolve_scope(),
        custom_rules=(CustomRule("mutation_attempt", "v1", attempted_mutation),),
    )
    official_ids = [
        row["selected_candidate_ids"]
        for row in result.pair_decisions
        if row["rule"] == "primary_profit"
    ]
    assert len(mutation_failures) == 4
    assert official_ids == baseline_ids

    dataset = open_dataset(root)
    with pytest.raises(AnalysisError, match="custom rule 'mutation_fails' failed"):
        evaluate_scope(
            dataset,
            dataset.resolve_scope(),
            custom_rules=(
                CustomRule(
                    "mutation_fails",
                    "v1",
                    lambda view, geometry, context: view.metrics["net_profit_pct"].__setitem__(0, 5.0),
                ),
            ),
        )


def test_diagnostics_do_not_require_first_active_formula(tmp_path):
    root = _synthetic_dataset(tmp_path)
    _rewrite_runspec(
        root,
        lambda run_spec: run_spec["preregistration"]["rule_registry"].update(
            selectable_rules=["trade_gate15_romad_mtm"],
            nomination_eligible_rules=["trade_gate15_romad_mtm"],
            baseline_rule="trade_gate15_romad_mtm",
        ),
    )
    _remove_metric(root, "max_drawdown_mtm_pct")
    dataset = open_dataset(root)
    result = evaluate_scope(
        dataset,
        dataset.resolve_scope(),
        custom_rules=(CustomRule("custom", "v1", lambda view, geometry, context: view.metrics["net_profit_pct"]),),
    )
    assert any(row["rule"] == "oos_oracle" for row in result.pair_decisions)


def test_no_supported_official_or_custom_rule_fails_early(tmp_path):
    root = _synthetic_dataset(tmp_path)
    _rewrite_runspec(
        root,
        lambda run_spec: run_spec["preregistration"]["rule_registry"].update(
            selectable_rules=["trade_gate15_romad_mtm"],
            nomination_eligible_rules=["trade_gate15_romad_mtm"],
            baseline_rule="trade_gate15_romad_mtm",
        ),
    )
    _remove_metric(root, "max_drawdown_mtm_pct")
    dataset = open_dataset(root)
    with pytest.raises(AnalysisError, match="no supported official rule"):
        evaluate_scope(dataset, dataset.resolve_scope())


def test_window_transients_are_released_before_next_window(tmp_path, monkeypatch):
    import tools.strategy_lab.analysis.evaluate as evaluate_module

    dataset = open_dataset(_synthetic_dataset(tmp_path))
    original_load = dataset.load_is_window
    original_evaluate = evaluate_module.evaluate_rule
    prior_views = []
    prior_results = []
    calls = 0

    def tracked_load(scope, window):
        nonlocal calls
        calls += 1
        if calls == 2:
            gc.collect()
            assert prior_views and all(reference() is None for reference in prior_views)
            assert prior_results and all(reference() is None for reference in prior_results)
        views = original_load(scope, window)
        if calls == 1:
            prior_views.extend(weakref.ref(view) for view in views.values())
        return views

    def tracked_evaluate(*args, **kwargs):
        result = original_evaluate(*args, **kwargs)
        if args[1].window_id == 1:
            prior_results.append(weakref.ref(result))
        return result

    monkeypatch.setattr(dataset, "load_is_window", tracked_load)
    monkeypatch.setattr(evaluate_module, "evaluate_rule", tracked_evaluate)
    evaluate_scope(dataset, dataset.resolve_scope())
    assert calls == 2


def test_end_to_end_guardrails_outputs_determinism_and_protection(tmp_path):
    root = _synthetic_dataset(tmp_path)
    dataset = open_dataset(root)
    result = evaluate_scope(dataset, dataset.resolve_scope(), custom_rules=(CustomRule("custom", "v1", lambda view, geometry, context: view.metrics["net_profit_pct"]),))
    assert result.run_metadata["analysis_scope"]["actual_calendar_block_count"] == 2
    assert result.run_metadata["analysis_scope"]["utc_blocks"][0]["block_key"] == [
        "2026-03-01T00:00:00Z",
        "2026-03-28T23:30:00Z",
    ]
    assert result.summary["diagnostics"]["unknown_flag_bits"] == 1
    assert result.summary["diagnostics"]["known_flag_bits"] == {
        "rejected_fill": 2,
        "invalid_stop_distance": 4,
        "zero_size_entry": 8,
    }
    assert result.summary["diagnostics"]["known_flag_bit_counts"]["rejected_fill"] == 4
    assert result.summary["diagnostics"]["guardrail_fault_observation_counts"] == {"zero_size_entry_count": 0, "invalid_stop_distance_count": 0}
    custom_rows = [row for row in result.pair_decisions if row["rule"] == "custom"]
    assert custom_rows and all(row["result_label"] == "exploratory" for row in custom_rows)
    baseline = next(
        row
        for row in result.pair_decisions
        if row["rule"] == "primary_profit" and row["ticker"] == "AAA" and row["window_id"] == 1
    )
    assert baseline["selected_candidate_ids"] == [20, 40, 30, 10]
    assert baseline["top1_oos_net_profit_pct"] == 4.0
    assert baseline["top5_oos_net_profit_pct"] == 2.5
    output = tmp_path / "analysis"
    first = write_analysis(result, output, dataset_root=root)
    before = {name: (output / name).read_bytes() for name in OUTPUT_FILES}
    second = write_analysis(result, output, dataset_root=root)
    assert first["status"] == "published" and second["status"] == "verified_noop"
    assert before == {name: (output / name).read_bytes() for name in OUTPUT_FILES}
    assert set(path.name for path in output.iterdir()) == set(OUTPUT_FILES)
    report = (output / "report.md").read_text(encoding="utf-8")
    assert "strategy_lab_dataset_v2` / `full` / `complete" in report
    assert "Declared / actual / missing window IDs" in report
    assert "Descriptive month-block bootstrap intervals" in report
    assert "Guardrails and availability diagnostics" in report
    assert report.count("`broad_population_edge.") == 27
    assert report.count("`selected_strategy_viability.") == 36
    assert report.count("`selection_lift.") == 36
    assert "nomination.maximum_rules" not in report
    assert "Code commit / dirty worktree" not in report
    assert set(json.loads((output / "run_metadata.json").read_text())["git"]) == {
        "code_commit",
        "dirty_worktree",
    }
    with pytest.raises(AnalysisError, match="outside every input dataset root"):
        write_analysis(result, root / "analysis", dataset_root=root)
    (output / "summary.json").write_text("{}")
    with pytest.raises(AnalysisError, match="nonmatching"):
        write_analysis(result, output, dataset_root=root)


def test_report_distinguishes_absent_of_from_unavailable_status(tmp_path):
    root = _synthetic_dataset(tmp_path)
    dataset = open_dataset(root)
    full_scope = dataset.resolve_scope()
    partial_scope = dataset.subset_scope(full_scope, window_ids=(2, 1))
    result = evaluate_scope(dataset, partial_scope)
    report = render_files(result)["report.md"].decode("utf-8")
    no_denominator = next(
        line
        for line in report.splitlines()
        if "broad_population_edge.median_candidate_oos_net_profit_pct" in line
    )
    numeric_denominator = next(
        line
        for line in report.splitlines()
        if "broad_population_edge.positive_monthly_population_medians" in line
    )
    assert "| 0 |  |" in no_denominator
    assert "| unavailable | actual analysis is a partial intersection" in no_denominator
    assert "| 1 | 2 |" in numeric_denominator
