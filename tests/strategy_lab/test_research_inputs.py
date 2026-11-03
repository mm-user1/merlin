"""Offline exploratory inputs through existing selection, scope and publication owners."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest

from tools.strategy_lab.analysis import (AnalysisError, DatasetInput, CandidatePolicy,
    evaluate_scope, evaluate_allocation, load_candidate_policy, load_universe, open_dataset)
from tools.strategy_lab.analysis.candidate_policy import METRICS
from tools.strategy_lab.analysis.cli import main
from tools.strategy_lab.analysis.dataset import ISView, ScopeLockedError
from tools.strategy_lab.analysis.evaluate import CustomRule
from tools.strategy_lab.analysis.json_utils import canonical_json_bytes
from tools.strategy_lab.analysis.output import (render_files, render_allocation_files,
    write_analysis, write_allocation, OUTPUT_FILES, ALLOCATION_OUTPUT_FILES)
from tools.strategy_lab.analysis.research_inputs import restrict_universe
from tools.strategy_lab.analysis.rules import select_candidates
from test_strategy_lab_analysis import _synthetic_dataset, _remove_metric, _rewrite_runspec
from test_strategy_lab_allocation import _input, _mutate_groups, _distinguish, _change_calendar


def policy_doc(metric="net_profit_pct", direction="maximize", constraints=None):
    return dict(schema_version="strategy_lab_candidate_policy_v1", name="synthetic policy", version="1",
        objective=dict(metric=metric, direction=direction), constraints=constraints or [], selection_count=1,
        no_eligible="unavailable", evidence_classification="exploratory")


def universe_doc(tickers):
    return dict(schema_version="strategy_lab_universe_v1", name="synthetic membership",
        evidence_classification="exploratory", selection_note="Explicit synthetic fixture membership.", tickers=tickers)


def document(tmp_path, doc, name="policy.json"):
    path = tmp_path / name
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def snapshot(root):
    return {p.relative_to(root).as_posix(): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in root.rglob("*") if p.is_file()}


@pytest.fixture
def source(tmp_path):
    return _synthetic_dataset(tmp_path / "source", tickers=(("BBB", "dev"), ("AAA", "dev"), ("HHH", "holdout")))


@pytest.mark.parametrize("change", [
    {"schema_version": "future"}, {"extra": 1}, {"name": " "}, {"selection_note": ""},
    {"evidence_classification": "confirmed"}, {"tickers": []}, {"tickers": ["AAA", "AAA"]},
    {"tickers": [" AAA"]}, {"tickers": ["AAA "]}, {"tickers": [1]}, {"tickers": "AAA"},
])
def test_universe_closed_validation(tmp_path, change):
    with pytest.raises(AnalysisError): load_universe(document(tmp_path, universe_doc(["AAA"]) | change))


@pytest.mark.parametrize("change", [
    {"schema_version": "future"}, {"extra": 1}, {"version": 1}, {"name": " "},
    {"selection_count": True}, {"selection_count": 2}, {"no_eligible": "first"},
    {"evidence_classification": "pre_registered"}, {"objective": {"metric": "flags", "direction": "maximize"}},
    {"objective": {"metric": "oos.net_profit_pct", "direction": "maximize"}},
    {"objective": {"metric": "net_profit_pct", "direction": "up"}},
    {"objective": {"metric": "net_profit_pct", "direction": "maximize", "segment": "oos"}},
    {"constraints": {}},
    *[{"constraints": [dict(metric="total_trades", operator=">=", value=value)]} for value in (True, "15", float("nan"), float("inf"), 10**400)],
    {"constraints": [dict(metric="missing", operator=">=", value=1)]},
    {"constraints": [dict(metric="total_trades", operator="==", value=1)]},
])
def test_policy_closed_validation(tmp_path, change):
    with pytest.raises(AnalysisError): load_candidate_policy(document(tmp_path, policy_doc() | change))


@pytest.mark.parametrize("loader,raw", [
    (load_universe, '[]'), (load_universe, '{"name":"a","name":"b"}'),
    (load_candidate_policy, '{"objective":{"metric":"a","metric":"b"}}'),
    (load_candidate_policy, '{}'), (load_universe, '{}'), (load_candidate_policy, '{bad'),
])
def test_json_root_duplicates_and_missing_fields(tmp_path, loader, raw):
    path = tmp_path / "bad.json"; path.write_text(raw)
    with pytest.raises(AnalysisError): loader(path)


def test_single_snapshot_canonical_identity_and_imports(tmp_path, monkeypatch):
    doc = policy_doc(constraints=[dict(metric="total_trades", operator=">=", value=-0.0)])
    path = document(tmp_path, doc); original = Path.read_bytes; calls = []
    def read(self):
        calls.append(self)
        return original(self)
    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", read)
        first = load_candidate_policy(path)
    assert calls == [path.resolve()]
    moved = tmp_path / "moved.json"; moved.write_bytes(path.read_bytes())
    assert load_candidate_policy(moved).document_sha256 == first.document_sha256
    doc["constraints"][0]["value"] = 0
    moved.write_text(json.dumps(dict(reversed(list(doc.items()))), indent=2))
    second = load_candidate_policy(moved)
    assert first.key == second.key and first.file_sha256 != second.file_sha256
    doc["constraints"][0]["value"] = 1
    assert load_candidate_policy(document(tmp_path, doc)).key != first.key
    from tools.strategy_lab.dataset import METRIC_AXIS_V3
    assert METRICS == set(METRIC_AXIS_V3) - {"flags"}
    code = "import sys; from tools.strategy_lab.analysis import load_universe, load_candidate_policy; " \
        "assert 'tools.strategy_lab.config' not in sys.modules; " \
        "assert not any(n == 'strategies' or n.startswith('strategies.') or 'grid_v2' in n or 'grid_planning' in n for n in sys.modules)"
    completed = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True)
    assert completed.returncode == 0 and not completed.stdout and not completed.stderr


@pytest.mark.parametrize("direction", ["maximize", "minimize"])
def test_arithmetic_constraints_finiteness_zero_and_ties(source, tmp_path, direction):
    ds = open_dataset(source); g = ds.geometry
    policy = load_candidate_policy(document(tmp_path, policy_doc("max_drawdown_mtm_pct", direction,
        [dict(metric="total_trades", operator=">=", value=0), dict(metric="profit_factor", operator="<", value=2)])))
    view = ISView(dict(net_profit_pct=np.array([np.nan, 2., 2., 2.]), max_drawdown_mtm_pct=np.array([0., -0., 0., 0.]),
        total_trades=np.zeros(4), profit_factor=np.array([1., 1., 2., np.inf])), "AAA", 1, "start", "end")
    result = policy.evaluate(view, g)
    assert result.eligible.tolist() == [True, True, False, False]  # No implicit trade/net gate.
    assert not np.any(np.signbit(result.score))
    assert select_candidates(result, view.metrics["net_profit_pct"], g, maximum=1).candidate_ids == (20,)
    for op, expected in ((">", [False, False, True, True]), (">=", [False, True, True, True]),
                         ("<", [True, False, False, False]), ("<=", [True, True, False, False])):
        p = load_candidate_policy(document(tmp_path, policy_doc(constraints=[dict(metric="total_trades", operator=op, value=1)])))
        v = replace(view, metrics=dict(net_profit_pct=np.arange(4.), total_trades=np.arange(4.)))
        assert p.evaluate(v, g).eligible.tolist() == expected
    p = load_candidate_policy(document(tmp_path, policy_doc("max_drawdown_mtm_pct", direction)))
    v = replace(view, metrics=dict(net_profit_pct=np.array([1., 1., 1., 1.]), max_drawdown_mtm_pct=np.array([1., 4., np.nan, np.inf])))
    assert select_candidates(p.evaluate(v, g), v.metrics["net_profit_pct"], g, maximum=1).candidate_ids == ((20,) if direction == "maximize" else (10,))
    order = [3, 2, 1, 0]
    reordered = replace(g, candidate_ids=g.candidate_ids[order], semantic_keys=tuple(g.semantic_keys[i] for i in order),
        axis_codes=g.axis_codes[order], active_masks=g.active_masks[order], block_keys=tuple(g.block_keys[i] for i in order))
    tied = replace(view, metrics=dict(net_profit_pct=np.ones(4), max_drawdown_mtm_pct=np.zeros(4)))
    assert select_candidates(p.evaluate(tied, reordered), tied.metrics["net_profit_pct"], reordered, maximum=1).candidate_ids == (20,)
    bad = replace(tied, metrics=dict(net_profit_pct=np.ones(4), max_drawdown_mtm_pct=np.zeros(3)))
    with pytest.raises(AnalysisError, match="aligned"): p.evaluate(bad, g)


def test_full_membership_order_preserves_legacy_results(source, tmp_path):
    def analyze(universe=None):
        ds = open_dataset(source)
        return evaluate_scope(ds, ds.resolve_scope(), universe=universe)
    control = analyze(); before = snapshot(source); identities = []; memberships = []
    for tickers in (["AAA", "BBB"], ["BBB", "AAA"]):
        universe = load_universe(document(tmp_path, universe_doc(tickers), "universe.json"))
        result = analyze(universe)
        identities.append(universe.document_sha256)
        memberships.append(result.run_metadata["research_inputs"]["universe"]["resolved_tickers_sha256"])
        assert result.run_metadata["analysis_scope"] == control.run_metadata["analysis_scope"]
        assert not result.run_metadata["analysis_scope"]["is_partial"]
        assert canonical_json_bytes(result.pair_decisions) == canonical_json_bytes(control.pair_decisions)
        stripped_meta = dict(result.run_metadata); stripped_meta.pop("research_inputs")
        stripped_summary = dict(result.summary); stripped_summary.pop("research_inputs")
        assert render_files(replace(result, run_metadata=stripped_meta, summary=stripped_summary)) == render_files(control)
    assert snapshot(source) == before
    assert identities[0] != identities[1] and memberships[0] == memberships[1]


def test_44_of_104_authority_and_missing_windows(tmp_path):
    names = [f"T{i:03}" for i in range(104)]
    root = _synthetic_dataset(tmp_path / "wide", tickers=[(n, "dev") for n in reversed(names)],
        actual_windows=(1,), declared_dev_pool=104)
    ds = open_dataset(root); scope = ds.resolve_scope(allow_partial=True)
    universe = load_universe(document(tmp_path, universe_doc(names[:44])))
    (subset,), facts = restrict_universe(universe, [("wide", ds, scope)])
    assert subset.tickers == tuple(reversed(names[:44])) and subset.ticker_cell_count == 104
    assert subset.missing_window_ids == (2,) and subset.declared_window_ids == (1, 2) and subset.is_partial
    assert facts["datasets"]["wide"]["authorized_ticker_count"] == 104 and ds.access_log == []


def test_scope_refusals_and_unlock_are_independent(source, tmp_path):
    p = document(tmp_path, policy_doc()); u = document(tmp_path, universe_doc(["HHH"]), "universe.json")
    before = snapshot(source)
    for options in ([], ["--unlock-scope"]):
        assert main(["analyze", "--dataset", str(source), "--scope", "holdout", "--candidate-policy", str(p),
            "--ticker-list", str(u), "--output", str(tmp_path / "refused"), *options]) == 2
    ds = open_dataset(source)
    with pytest.raises(AnalysisError, match="analysis.*HHH"):
        evaluate_scope(ds, ds.resolve_scope(), universe=load_universe(u))
    assert ds.access_log == []
    evidence = tmp_path / "unlock.txt"; evidence.write_text("explicit synthetic holdout policy")
    scope = ds.resolve_scope("holdout", unlock=True, policy_path=evidence)
    result = evaluate_scope(ds, scope, universe=load_universe(u), candidate_policy=load_candidate_policy(p))
    assert result.run_metadata["research_inputs"]["universe"]["datasets"]["analysis"]["unlock_evidence"] == scope.unlock_evidence
    assert snapshot(source) == before and not (tmp_path / "refused").exists()


@pytest.mark.parametrize("metric", ["net_profit_pct", "margin_reject_count"])
def test_metric_refusal_before_decisions(source, tmp_path, metric):
    if metric == "net_profit_pct": _remove_metric(source, metric)
    ds = open_dataset(source); scope = ds.resolve_scope()
    policy = load_candidate_policy(document(tmp_path, policy_doc(metric)))
    for call in (lambda: evaluate_scope(ds, scope, candidate_policy=policy),
                 lambda: evaluate_allocation([DatasetInput("a", ds, scope)], candidate_policy=policy)):
        with pytest.raises(AnalysisError, match="missing|requires"): call()
        assert ds.access_log == []
    if metric == "net_profit_pct":
        with pytest.raises(AnalysisError, match="net_profit_pct"):
            evaluate_allocation([DatasetInput("a", ds, scope)], candidate_rule="trade_gate15_profit_factor")
        assert ds.access_log == []
        assert main(["allocate", "--dataset", "a="+str(source), "--rule", "trade_gate15_profit_factor",
            "--output", str(tmp_path / "refused")]) == 2


@pytest.mark.parametrize("gate", [None, 15])
def test_shared_selection_top1_fields_capacity_and_noop(source, tmp_path, gate):
    policy = load_candidate_policy(document(tmp_path, policy_doc(constraints=[] if gate is None else
        [dict(metric="total_trades", operator=">=", value=gate)])))
    ds = open_dataset(source); scope = ds.resolve_scope()
    analysis = evaluate_scope(ds, scope, candidate_policy=policy)
    rows = [r for r in analysis.pair_decisions if r["rule"] == policy.key]
    built_in = "primary_profit" if gate is None else "trade_gate15_profit"
    baseline = { (r["ticker"], r["window_id"]): r for r in analysis.pair_decisions if r["rule"] == built_in }
    allocation = evaluate_allocation([_input(source)], candidate_policy=policy, primary_k=6)
    old = evaluate_allocation([_input(source)], candidate_rule=built_in, primary_k=6)
    for row in rows:
        control = baseline[row["ticker"], row["window_id"]]
        assert row["selected_candidate_ids"] == control["selected_candidate_ids"][:1]
        assert row["rule_kind"] == "declarative_policy" and row["result_label"] == "exploratory"
        for field in ("top5_oos_net_profit_pct", "top10_oos_net_profit_pct", "top5_mean_individual_oos_percentile"):
            assert row[field] is None
        pair = next(r for r in allocation.pair_decisions if r["ticker"] == row["ticker"] and r["window_id"] == row["window_id"])
        assert pair["candidate_id"] == row["selected_candidate_ids"][0]
        assert pair["candidate_rule_score"] == row["ordered_scores"][0]
    summary = analysis.summary["rules"][policy.key]
    assert policy.key not in analysis.summary["evidence_by_selectable_rule"]
    assert summary["top5_lift_monthly_means"] == [None, None]
    assert summary["bootstraps"]["top5_lift"] == dict(status="not_requested", reason="selection_count=1", lower=None, upper=None, width=None)
    assert analysis.run_metadata["rule_states"][policy.key]["not_requested"] == ["top5", "top10"]
    for row in analysis.monthly_results:
        if row["rule"] == policy.key: assert row["top5_lift_mean"] is None
    for new_block, old_block in zip(allocation.summary["datasets"]["canonical"]["blocks"], old.summary["datasets"]["canonical"]["blocks"]):
        for key in ("selected_tickers", "capacity_return_pct", "cash_slots"):
            assert new_block["variants"]["primary"][key] == old_block["variants"]["primary"][key]
        assert new_block["variants"]["primary"]["cash_slots"] == 4
    assert render_allocation_files(allocation) == render_allocation_files(evaluate_allocation([_input(source)], candidate_policy=policy))
    out = tmp_path / "analysis"; write_analysis(analysis, out, dataset_root=source)
    before = snapshot(out); assert write_analysis(analysis, out, dataset_root=source)["status"] == "verified_noop"
    assert snapshot(out) == before and set(p.name for p in out.iterdir()) == set(OUTPUT_FILES)
    assert "selection_count=1" in (out / "report.md").read_text()
    assert set(analysis.summary["research_inputs"]) == {"schema_version", "universe_document_sha256", "candidate_policy_key"}


def test_intersection_reuses_alignment_and_retains_authority(tmp_path):
    a = _synthetic_dataset(tmp_path / "a", tickers=[(t, "dev") for t in ("CCC", "AAA", "BBB")])
    b = _synthetic_dataset(tmp_path / "b", tickers=[(t, "dev") for t in ("DDD", "CCC", "BBB")])
    _distinguish(b, "b"); _change_calendar(b, id_offset=10)
    policy = load_candidate_policy(document(tmp_path, policy_doc()))
    universe = load_universe(document(tmp_path, universe_doc(["AAA", "BBB", "CCC", "DDD"]), "universe.json"))
    inputs = [_input(a, "a"), _input(b, "b")]
    with pytest.raises(AnalysisError, match="dataset 'a'.*DDD"):
        evaluate_allocation(inputs, candidate_policy=policy, universe=universe)
    assert all(not i.dataset.access_log for i in inputs)
    result = evaluate_allocation(inputs, candidate_policy=policy, universe=universe, ticker_list_mode="intersection")
    assert result.run_metadata["alignment"]["common_tickers"] == ["BBB", "CCC"]
    assert result.summary["datasets"]["a"]["omitted_tickers"] == ["AAA"]
    assert result.summary["datasets"]["b"]["omitted_tickers"] == ["DDD"]
    facts = result.run_metadata["research_inputs"]["universe"]
    assert facts["datasets"]["a"]["missing_requested_tickers"] == ["DDD"]
    assert facts["datasets"]["b"]["missing_requested_tickers"] == ["AAA"]
    assert facts["resolved_tickers_sha256"] == hashlib.sha256(canonical_json_bytes(["BBB", "CCC"])).hexdigest()
    assert all(t in ("BBB", "CCC") for item in inputs for t, _, _ in item.dataset.access_log)
    for tickers, match in ((["AAA", "DDD", "typo"], "absent from every"), (["AAA", "DDD"], "no common"), (["AAA"], "no tickers")):
        u = load_universe(document(tmp_path, universe_doc(tickers), "universe.json"))
        with pytest.raises(AnalysisError, match=match):
            evaluate_allocation([_input(a, "a"), _input(b, "b")], candidate_policy=policy, universe=u, ticker_list_mode="intersection")
    with pytest.raises(AnalysisError, match="requires.*list"):
        evaluate_allocation([_input(a)], candidate_policy=policy, ticker_list_mode="strict")


def test_decisions_freeze_across_inputs_and_adversarial_oos(source, tmp_path, monkeypatch):
    other = _synthetic_dataset(tmp_path / "other"); _distinguish(other, "other")
    policy = load_candidate_policy(document(tmp_path, policy_doc("max_drawdown_mtm_pct", "minimize",
        [dict(metric="total_trades", operator=">=", value=15)])))
    original = CandidatePolicy.evaluate
    decisions = []
    def observe(self, view, geometry):
        assert not hasattr(view, "oos") and not hasattr(view, "dataset")
        result = original(self, view, geometry)
        decisions.append((view.window_id, view.ticker))
        return result
    monkeypatch.setattr(CandidatePolicy, "evaluate", observe)
    ds = open_dataset(source); load = ds.load_oos_window
    def reveal(scope, window):
        assert sorted(t for w, t in decisions if w == window.window_id) == ["AAA", "BBB"]
        return load(scope, window)
    monkeypatch.setattr(ds, "load_oos_window", reveal)
    analyzed = evaluate_scope(ds, ds.resolve_scope(), candidate_policy=policy)
    assert all(r["selected_candidate_ids"] == [40] for r in analyzed.pair_decisions if r["rule"] == policy.key)
    # Ignoring the trade constraint would select zero-trade row 30 instead.
    decisions.clear(); inputs = [_input(source, "a"), _input(other, "b")]
    from tools.strategy_lab.analysis import allocation as owner
    original_rank = owner._rank; rankings = []
    def rank(values, **kwargs):
        result = original_rank(values, **kwargs)
        rankings.append((values[0].view.dataset_label, values[0].view.window_id))
        return result
    monkeypatch.setattr(owner, "_rank", rank)
    for item in inputs:
        original_oos = item.dataset.load_oos_window
        def oos(scope, window, loader=original_oos):
            assert sorted(t for w, t in decisions if w == window.window_id) == ["AAA", "AAA", "BBB", "BBB"]
            assert sorted(label for label, w in rankings if w == window.window_id) == ["a", "a", "b", "b"]
            return loader(scope, window)
        monkeypatch.setattr(item.dataset, "load_oos_window", oos)
    before = evaluate_allocation(inputs, candidate_policy=policy, primary_k=1)
    def change(matrix, ticker, window, axis):
        matrix[:, 1, axis.index("net_profit_pct")] = [10000, -10000, 9999, -9999]
    for root in (source, other): _mutate_groups(root, change)
    after = evaluate_allocation([_input(source, "a"), _input(other, "b")], candidate_policy=policy, primary_k=1)
    keys = ("dataset_label", "ticker", "window_id", "candidate_id", "semantic_key", "candidate_rule_score", "ticker_score")
    assert [{k:r[k] for k in keys} for r in before.pair_decisions] == [{k:r[k] for k in keys} for r in after.pair_decisions]
    for label in ("a", "b"):
        assert [b["variants"]["primary"]["selected_tickers"] for b in before.summary["datasets"][label]["blocks"]] == [b["variants"]["primary"]["selected_tickers"] for b in after.summary["datasets"][label]["blocks"]]


def test_unavailable_candidate_score_and_oos_stay_distinct(source, tmp_path):
    policy = load_candidate_policy(document(tmp_path, policy_doc("profit_factor", "maximize")))
    def change(matrix, ticker, window, axis):
        if ticker == "BBB": matrix[:, 0, axis.index("profit_factor")] = np.nan
        if ticker == "AAA" and window == 1: matrix[1, 0, axis.index("net_profit_pct")] = np.nan
        if ticker == "AAA" and window == 2: matrix[1, 1, axis.index("net_profit_pct")] = np.nan
    _mutate_groups(source, change)
    result = evaluate_allocation([_input(source)], candidate_policy=policy, primary_k=1)
    pairs = {(r["ticker"], r["window_id"]): r for r in result.pair_decisions}
    assert pairs["BBB", 1]["reason"] == "no eligible finite candidate"
    assert pairs["AAA", 1]["reason"] == "ticker score is unavailable"
    assert pairs["AAA", 2]["candidate_id"] == 20
    assert pairs["AAA", 2]["oos_return_pct"] is None
    assert result.summary["datasets"]["canonical"]["variants"]["primary"]["headline_mean_capacity_return_pct"] is None


def test_no_eligible_policy_does_not_fallback_or_mix_custom(source, tmp_path):
    policy = load_candidate_policy(document(tmp_path, policy_doc(constraints=[dict(metric="total_trades", operator=">", value=1000)])))
    ds = open_dataset(source); scope = ds.resolve_scope()
    with pytest.raises(AnalysisError, match="custom"):
        evaluate_scope(ds, scope, candidate_policy=policy, custom_rules=[CustomRule("custom", "1", lambda *a: np.ones(4))])
    assert ds.access_log == []
    for args in ({}, {"candidate_rule": "primary_profit", "candidate_policy": policy}):
        with pytest.raises(AnalysisError, match="exactly one"): evaluate_allocation([_input(source)], **args)
    result = evaluate_scope(ds, scope, candidate_policy=policy)
    rows = [r for r in result.pair_decisions if r["rule"] == policy.key]
    assert all(r["status"] == "unavailable_pair" and r["selected_candidate_ids"] == [] for r in rows)
    assert result.summary["rules"][policy.key]["bootstraps"]["top5_lift"]["reason"] == "selection_count=1"


def test_policy_counts_as_usable_rule_and_inputs_are_snapshots(source, tmp_path, monkeypatch):
    _rewrite_runspec(source, lambda r: r["preregistration"]["rule_registry"].update(
        selectable_rules=["trade_gate15_romad_mtm"], nomination_eligible_rules=["trade_gate15_romad_mtm"],
        baseline_rule="trade_gate15_romad_mtm"))
    _remove_metric(source, "max_drawdown_mtm_pct")
    ds = open_dataset(source); scope = ds.resolve_scope()
    with pytest.raises(AnalysisError, match="no supported"): evaluate_scope(ds, scope)
    path = document(tmp_path, universe_doc(["AAA"]), "universe.json")
    original = Path.read_bytes; reads = []
    def read(self):
        reads.append(self)
        return original(self)
    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", read)
        universe = load_universe(path)
    assert reads == [path.resolve()]
    policy = load_candidate_policy(document(tmp_path, policy_doc()))
    path.write_text("invalid later bytes")
    result = evaluate_scope(ds, scope, universe=universe, candidate_policy=policy)
    assert result.summary["rules"][policy.key]["selected_pairs"] == 2
    assert result.run_metadata["analysis_scope"]["tickers"] == ["AAA"]
    with pytest.raises(AnalysisError): load_universe(path)


@pytest.mark.parametrize("command", ["analyze", "allocate"])
def test_cli_publication_provenance_noop_and_refusal(source, tmp_path, command):
    policy_path = document(tmp_path, policy_doc())
    universe_path = document(tmp_path, universe_doc(["AAA"]), "universe.json")
    out = tmp_path / "output"
    args = [command, "--dataset", str(source) if command == "analyze" else "one="+str(source),
        "--candidate-policy", str(policy_path), "--ticker-list", str(universe_path), "--output", str(out)]
    original = snapshot(source)
    calls = []
    def invoke():
        call = [sys.executable, "-B", "-m", "tools.strategy_lab.analysis.cli", *args]
        result = subprocess.run(call, capture_output=True, text=True)
        calls.append(dict(command=call, returncode=result.returncode, stdout=result.stdout, stderr=result.stderr))
        assert result.returncode == 0 and not result.stderr
        return json.loads(result.stdout)
    assert invoke()["status"] == "published"
    saved = snapshot(out); assert invoke()["status"] == "verified_noop" and snapshot(out) == saved
    (tmp_path / "cli-readback.json").write_text(json.dumps(calls, indent=2), encoding="utf-8")
    assert set(p.name for p in out.iterdir()) == set(OUTPUT_FILES if command == "analyze" else ALLOCATION_OUTPUT_FILES)
    meta = json.loads((out / "run_metadata.json").read_text())
    facts = meta["research_inputs"]["universe"]
    assert facts["resolved_tickers"] == ["AAA"] and facts["datasets"]["analysis" if command == "analyze" else "one"]["is_partial"]
    assert "Exploratory research inputs" in (out / "report.md").read_text()
    # Changed raw bytes are distinct evidence, even with the same canonical policy.
    policy_path.write_text(json.dumps(policy_doc(), indent=2))
    assert main(args) == 2 and snapshot(out) == saved
    assert snapshot(source) == original
    if command == "allocate":
        assert main(["allocate", "--dataset", "one="+str(source), "--rule", "primary_profit", "--ticker-list-mode", "strict", "--output", str(tmp_path / "refused")]) == 2
        with pytest.raises(SystemExit) as error: main(args + ["--rule", "primary_profit"])
        assert error.value.code == 2
    ds = open_dataset(source)
    result = evaluate_scope(ds, ds.resolve_scope(), universe=load_universe(universe_path))
    all_before = snapshot(tmp_path)
    for output in (universe_path, tmp_path):
        with pytest.raises(AnalysisError): write_analysis(result, output, dataset_root=source)
        assert snapshot(tmp_path) == all_before
