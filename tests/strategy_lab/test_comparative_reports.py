"""Saved report arithmetic, authorization, provenance and publication witnesses."""
from copy import deepcopy
from dataclasses import replace
import csv
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest

from _analysis_helpers import _synthetic_dataset, _rewrite_runspec, _sha, mutate_group
from tools.strategy_lab.analysis import (DatasetInput, AnalysisError, open_dataset, evaluate_scope,
    evaluate_allocation, evaluate_field_audit, write_field_audit, evaluate_screen, write_screen,
    load_universe, load_candidate_policy)
from tools.strategy_lab.analysis.output import render_files, render_allocation_files
from tools.strategy_lab.analysis.evaluate import _spearman
from tools.strategy_lab.report_support import concentration, neighbourhood, new_output


def document(tmp_path, value, name="document.json"):
    path = tmp_path/name
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def universe(tmp_path, tickers):
    return load_universe(document(tmp_path, dict(schema_version="strategy_lab_universe_v1", name="test",
        evidence_classification="exploratory", selection_note="explicit synthetic membership", tickers=tickers)))


def policy(tmp_path):
    return load_candidate_policy(document(tmp_path, dict(schema_version="strategy_lab_candidate_policy_v1",
        name="test", version="1", objective=dict(metric="net_profit_pct", direction="maximize"), constraints=[],
        selection_count=1, no_eligible="unavailable", evidence_classification="exploratory")))


def input_at(root, label="test", **kwargs):
    dataset = open_dataset(root)
    return DatasetInput(label, dataset, dataset.resolve_scope("development", **kwargs))


def test_field_preserves_analyze_and_hierarchical_support(tmp_path):
    root = _synthetic_dataset(tmp_path)
    mutate_group(root, "AAA", 1, lambda a, m: a.__setitem__((0, 1, m["net_profit_pct"]), np.nan))
    expected = input_at(root)
    standalone = render_files(evaluate_scope(expected.dataset, expected.scope))
    item = input_at(root)
    result = evaluate_field_audit(item.dataset, item.scope)
    assert render_files(result.analysis) == standalone
    assert result.metadata["diagnostic_access_start"] == 8
    assert len(result.metadata["diagnostic_access"]) == 8
    row = next(r for r in result.tables["candidate_summary"] if r["semantic_key"] == "z" and
        r["segment"] == "oos" and r["metric"] == "net_profit_pct" and r["level"] == "candidate")
    assert row["finite_cells"] == 3 and row["mean"] == pytest.approx((2+2.5)/2)
    parts = [r for r in result.tables["concentration"] if r["semantic_key"] == "z" and
        r["segment"] == "oos" and r["dimension"] == "ticker"]
    assert sum(r["contribution"] for r in parts) == row["mean"]
    assert all(r["partial_support"] for r in parts)
    out = tmp_path/"audit"
    write_field_audit(result, out)
    assert all((out/"analysis"/n).read_bytes() == b for n, b in standalone.items())
    assert json.loads((out/"audit.json").read_text())["schema_version"] == "strategy_lab_field_audit_v1"
    with pytest.raises(AnalysisError, match="empty access log"):
        evaluate_field_audit(item.dataset, item.scope)
    with pytest.raises(AnalysisError, match="empty output"):
        write_field_audit(result, out)


def test_full_membership_allocation_order_and_screen_preservation(tmp_path):
    root = _synthetic_dataset(tmp_path)
    base = input_at(root)
    original = evaluate_allocation([base], candidate_rule="primary_profit")
    for tickers in (("AAA", "BBB"), ("BBB", "AAA")):
        item = input_at(root)
        result = evaluate_allocation([item], candidate_rule="primary_profit", universe=universe(tmp_path, tickers))
        assert result.run_metadata["research_inputs"]["universe"]["datasets"]["test"]["effective_tickers"] == ["AAA", "BBB"]
        result.run_metadata.pop("research_inputs")
        result.summary.pop("research_inputs")
        assert render_allocation_files(result) == render_allocation_files(original)
    item = input_at(root)
    screen = evaluate_screen([item], candidate_rule="primary_profit")
    assert render_allocation_files(screen.allocation) == render_allocation_files(original)
    out = tmp_path/"screen"
    write_screen(screen, out)
    for row in screen.tables["concentration"]:
        headline = original.summary["datasets"]["test"]["variants"][row["variant"]]["headline_mean_capacity_return_pct"]
        assert row["complete_total"] == pytest.approx(headline)


def test_integrity_and_nested_unlock_snapshots(tmp_path):
    root = _synthetic_dataset(tmp_path)
    item = input_at(root)
    u, p = universe(tmp_path, ["AAA", "BBB"]), policy(tmp_path)
    p.document["objective"]["direction"] = "minimize"
    with pytest.raises(AnalysisError, match="mutated"):
        evaluate_scope(item.dataset, item.scope, candidate_policy=p)
    u.document["tickers"].pop()
    with pytest.raises(AnalysisError, match="mutated"):
        evaluate_allocation([item], candidate_rule="primary_profit", universe=u)
    assert not item.dataset.access_log
    u, p = universe(tmp_path, ["HHH"]), policy(tmp_path)
    evidence = document(tmp_path, {"frozen": True}, "unlock.json")
    scope = item.dataset.resolve_scope("holdout", unlock=True, policy_path=evidence)
    scope.unlock_evidence["nested"] = {"entries": ["original"]}
    result = evaluate_scope(item.dataset, scope, universe=u, candidate_policy=p)
    saved = render_files(result)
    scope.unlock_evidence["nested"]["entries"].append("changed")
    u.document["tickers"].append("evil")
    p.document["objective"]["direction"] = "minimize"
    assert render_files(result) == saved


def test_finite_diagnostics_and_concentration_definitions():
    assert _spearman(np.arange(7.), np.arange(7.)) is None
    assert _spearman(np.arange(8.), np.arange(8.)) == 1
    assert _spearman(np.ones(8), np.arange(8.)) is None
    assert neighbourhood([0, np.nan], [0, 1], 0)["status"] == "no_finite_distinct_neighbor"
    row = neighbourhood([np.nan, 0, -1], [0, 1, 2], 0)
    assert row["mean"] == -.5 and row["center_minus_mean"] is None and row["positive_share"] == 0
    assert neighbourhood([2], [0], 0)["mean"] is None
    rows = concentration([("B", 2), ("A", 2), ("C", -4)])
    assert rows[0]["complete_total"] == 0 and rows[0]["largest_positive_group"] == "A"
    assert rows[0]["total_excluding_largest"] == -2
    assert concentration([("A", -2)])[0]["largest_positive_share"] is None


def test_all_available_guard_turnover_and_missing_capacity(tmp_path):
    roots = [_synthetic_dataset(tmp_path/n) for n in ("a", "b")]
    _rewrite_runspec(roots[1], lambda d: d.update(run_name="other"))
    mutate_group(roots[1], "AAA", 1, lambda a, m: a.__setitem__((1, 1, m["net_profit_pct"]), np.inf))
    result = evaluate_screen([input_at(r, n) for r, n in zip(roots, ("a", "b"))], candidate_rule="primary_profit", baseline="a")
    rows = result.tables["comparisons"]
    block = next(r for r in rows if r["level"] == "block" and r["variant"] == "all_available")
    assert block["delta"] is None and block["reason"] == "different_finite_ticker_sets"
    assert block["baseline_selected_tickers"] == ["AAA", "BBB"] and block["input_selected_tickers"] == ["BBB"]
    assert all(r["unchanged"] == 1 for r in result.tables["candidate_turnover"])
    bad = [r for r in result.tables["concentration"] if r["dataset_label"] == "b" and r["variant"] == "primary"]
    assert all(r["complete_total"] is None for r in bad)
    headline = next(r for r in rows if r["level"] == "headline" and r["variant"] == "all_available")
    assert headline["finite_paired_blocks"] == 1 and headline["delta"] is None


def test_44_of_104_scopes_and_output_protection(tmp_path):
    tickers = tuple((f"T{i:03}", "dev") for i in range(104))
    root = _synthetic_dataset(tmp_path, tickers=tickers, declared_dev_pool=104)
    item = input_at(root)
    result = evaluate_screen([item], candidate_rule="primary_profit", universe=universe(tmp_path, [t for t, _ in tickers[:44]]))
    assert result.metadata["alignment"]["common_ticker_count"] == 44
    assert {e[0] for e in item.dataset.access_log} == {t for t, _ in tickers[:44]}
    from tools.strategy_lab.paths import PROTECTED_CANONICAL_OUTPUTS, REPO_ROOT
    for path in [root/"report", root.parent, *PROTECTED_CANONICAL_OUTPUTS, REPO_ROOT/"src/storage/report"]:
        with pytest.raises(ValueError):
            new_output(path, [root])


def test_new_cli_json_and_html_escaping(tmp_path):
    root = _synthetic_dataset(tmp_path)
    for command in ("field-audit", "screen"):
        out = tmp_path/command
        args = [sys.executable, "-B", "-m", "tools.strategy_lab.analysis.cli", command,
            "--dataset", str(root) if command == "field-audit" else "<unsafe>="+str(root),
            "--scope", "development", "--output", str(out)]
        if command == "screen":
            args += ["--rule", "primary_profit"]
        process = subprocess.run(args, capture_output=True, text=True)
        assert process.returncode == 0, process.stderr
        assert json.loads(process.stdout)["status"] == "published"
        assert "<unsafe>" not in (out/"index.html").read_text()


def test_named_partial_scopes_unlock_each_input_and_cross_input_freeze(tmp_path, monkeypatch):
    from tools.strategy_lab.analysis.cli import main
    roots = [_synthetic_dataset(tmp_path/n, actual_windows=(1,)) for n in ("a", "b")]
    for n, root in zip(("a", "b"), roots):
        def change(doc):
            doc["run_name"] = n
            doc["preregistration"]["analysis_scopes"][0].update(name=n, requires_unlock=True)
        _rewrite_runspec(root, change)
    evidence = document(tmp_path, {"frozen": True}, "unlock.json")
    command = ["screen", "--dataset", "a="+str(roots[0]), "--dataset", "b="+str(roots[1]), "--scope", "a",
        "--scope-for", "b=b", "--rule", "primary_profit", "--allow-partial-scope", "--unlock-scope",
        "--unlock-policy", "a="+str(evidence), "--output", str(tmp_path/"refused")]
    assert main(command) == 2 and not (tmp_path/"refused").exists()
    assert main(command+["--unlock-policy", "b="+str(evidence)]) == 0
    assert main(command+["--unlock-policy", "unknown="+str(evidence)]) == 2
    inputs = []
    for n, root in zip(("a", "b"), roots):
        d = open_dataset(root)
        s = d.resolve_scope(n, allow_partial=True, unlock=True, policy_path=evidence)
        inputs.append(DatasetInput(n, d, s))
    events = []
    for i in inputs:
        original = i.dataset._load_segment
        def load(s, w, segment, original=original, label=i.label):
            events.append((label, segment))
            if segment == "oos":
                assert ("a", "is") in events and ("b", "is") in events
            return original(s, w, segment)
        monkeypatch.setattr(i.dataset, "_load_segment", load)
    result = evaluate_screen(inputs, candidate_rule="primary_profit")
    assert all(v["scope"]["is_partial"] and v["scope"]["unlock_evidence"] for v in result.metadata["sources"].values())
    inputs[0].scope.unlock_evidence["policy_sha256"] = "mutated"
    assert result.metadata["sources"]["a"]["scope"]["unlock_evidence"]["policy_sha256"] != "mutated"


def test_policy_feasibility_all_missing_zero_and_sparse_neighbors(tmp_path):
    from tools.strategy_lab.analysis.rules import star_neighbours
    from _analysis_helpers import _geometry
    root = _synthetic_dataset(tmp_path)
    def change(a, m):
        a[:, 0, m["net_profit_pct"]] = [0, np.nan, np.inf, -2]
        a[:, 1, m["net_profit_pct"]] = np.nan
    for ticker in ("AAA", "BBB"):
        for window in (1, 2):
            mutate_group(root, ticker, window, change)
    p = policy(tmp_path)
    doc = deepcopy(p.document)
    doc["constraints"] = [dict(metric="total_trades", operator=">=", value=15),
        dict(metric="net_profit_pct", operator=">=", value=0)]
    p = load_candidate_policy(document(tmp_path, doc))
    item = input_at(root)
    result = evaluate_field_audit(item.dataset, item.scope, candidate_policy=p)
    cell = next(r for r in result.tables["cell_summary"] if r["segment"] == "is" and r["metric"] == "net_profit_pct")
    assert cell["finite"] == 2 and cell["positive"] == 0
    assert cell["feasibility"]["finite_objective_count"] == 2
    assert cell["feasibility"]["constraint_pass_counts"] == [3, 1]
    assert cell["feasibility"]["eligible_count"] == 1
    assert all(r["mean"] is None for r in result.tables["candidate_summary"] if r["segment"] == "oos" and r["metric"] == "net_profit_pct")
    geometry = _geometry()
    geometry = replace(geometry, axis_codes=np.array([[0], [2], [3], [4]]))
    assert 1 in star_neighbours(geometry)[0]
    geometry = replace(geometry, axis_codes=np.array([[0], [2], [1], [4]]), block_keys=(("v", "b"), ("v", "b"), ("other", "b"), ("v", "b")))
    assert list(star_neighbours(geometry)[0]) == [0]


def test_interruption_descriptor_last(tmp_path, monkeypatch):
    from tools.strategy_lab import dataset as publication
    root = _synthetic_dataset(tmp_path)
    item = input_at(root)
    result = evaluate_field_audit(item.dataset, item.scope)
    original = publication.atomic_write
    def interrupt(path, writer):
        if path.name == "candidate_summary.csv":
            raise OSError("synthetic interrupted publication")
        return original(path, writer)
    monkeypatch.setattr(publication, "atomic_write", interrupt)
    out = tmp_path/"interrupted"
    with pytest.raises(OSError, match="interrupted"):
        write_field_audit(result, out)
    assert not (out/"audit.json").exists()
    with pytest.raises(AnalysisError, match="empty output"):
        write_field_audit(result, out)


def test_symlink_ancestors(tmp_path):
    target = tmp_path/"target"
    target.mkdir()
    link = tmp_path/"link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"host cannot create symlinks: {exc}")
    with pytest.raises(AnalysisError, match="symlinks"):
        new_output(link/"nested", [])
