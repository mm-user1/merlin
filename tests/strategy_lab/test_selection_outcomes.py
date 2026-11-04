"""Real saved-family integration and independent normalized-return oracles."""
from copy import deepcopy
import json
import shutil
import subprocess
import sys
import numpy as np
import pytest

from _selection_helpers import selection_family, document
from _fixed_helpers import snapshot
from _analysis_helpers import mutate_group, _rewrite_runspec
from tools.strategy_lab.selection import open_ranking, write_fixed_request, evaluate_selection, write_selection
from tools.strategy_lab.analysis import open_dataset, DatasetInput, evaluate_allocation, load_candidate_policy
from tools.strategy_lab.fixed import storage
from tools.strategy_lab.dataset import atomic_write_json


def candidate_inputs(family, roots=None):
    inputs = []
    for label, root in (roots or family["candidates"]).items():
        dataset = open_dataset(root)
        inputs.append(DatasetInput(label, dataset, dataset.resolve_scope("research")))
    return inputs


def test_real_fixed_workflow_nonvacuous_and_scalar_collector(selection_family, tmp_path, monkeypatch):
    f = selection_family
    ranking = open_ranking(f["rankings"]["fixed"])
    before = snapshot(ranking.root)
    write_fixed_request(ranking, f["work"]/"off-template.json", tmp_path/"again", warmup_bars=100)
    assert (tmp_path/"again/fixed-request.json").read_bytes() == (f["work"]/"off-transform/fixed-request.json").read_bytes()
    assert before == snapshot(ranking.root)
    calls = []
    original = storage.read_account
    def read(*a, **k):
        calls.append(a[1]["account_id"])
        return original(*a, **k)
    monkeypatch.setattr(storage, "read_account", read)
    result = evaluate_selection(ranking, fixed_inputs=f["fixed"], baseline="off")
    assert len(calls) == 12 and len(set(calls)) == 12
    assert result.summary["duration_month_counts"] == {"1":1, "2":1}
    assert "mixed durations" in result.summary["headline"]
    rows = result.tables["blocks"]
    primary = [r for r in rows if r["level"] == "arm_block"]
    by = {(r["dataset_label"], r["block_id"], r["arm"]):r["value"] for r in primary}
    assert any(by[label, block, "x_top_k"] != by[label, block, "x_bottom_k"] for label in ("off", "on") for block in (1,2))
    assert any(by["off", block, "x_top_k"] != by["on", block, "x_top_k"] for block in (1,2))
    # Durable P6 completion/boundary assertions, not caught-and-recorded exceptions.
    for label in ("off", "on"):
        manifest = result.metadata["sources"][label]["manifest"]
        assert manifest["complete"] and len(manifest["completed"]) == 6
        assert not result.metadata["sources"][label]["unused_account_ids"]
        accounts = {a["account_id"]:a for a in manifest["contract"]["accounts"]}
        for d in result.tables["decisions"]:
            if d["dataset_label"] != label: continue
            a = accounts[d["account_id"]]
            assert a["input_pin"]["bounds"]["evaluation_start"] == d["block_key"][0]
            assert a["input_pin"]["bounds"]["end"] == d["block_key"][1]
    for label in ("off", "on"):
        for arm in ("x_top_k", "all_declared", "all_x_eligible", "x_bottom_k", "random_k"):
            parts = [r for r in result.tables["concentration"] if r["dataset_label"] == label and r["arm"] == arm]
            target = result.summary["datasets"][label]["random_path_mean_distribution"]["mean"] if arm == "random_k" else result.summary["datasets"][label]["arms"][arm]["mean"]
            assert sum(r["contribution"] for r in parts) == pytest.approx(target)
    write_selection(result, tmp_path/"report")
    assert json.loads((tmp_path/"report/selection.json").read_text())["schema_version"] == "strategy_lab_x_evaluation_v1"


@pytest.mark.parametrize("failed", [True, False])
def test_fixed_partial_unknown_and_full_matrix_refusal(selection_family, tmp_path, failed):
    f = selection_family
    root = tmp_path/"partial"
    shutil.copytree(f["fixed"]["off"], root)
    m = storage.open_dataset(root)
    # Last ticker of last period is unknown; full declared matrix stays present.
    missing = m["contract"]["accounts"][-1]["account_id"]
    m["completed"].pop(missing); m["complete"] = False
    m["failure"] = dict(account_id=missing, reason="synthetic failure") if failed else None
    (root/"manifest.json").unlink(); atomic_write_json(root/"manifest.partial.json", m)
    with pytest.raises(ValueError, match="partial"):
        evaluate_selection(f["rankings"]["fixed"], fixed_inputs={"off":root})
    result = evaluate_selection(f["rankings"]["fixed"], fixed_inputs={"off":root}, allow_partial=True)
    unknown = [r for r in result.tables["decisions"] if r["status"] == "unknown"]
    assert len(unknown) == 1 and unknown[0]["value"] is None and unknown[0]["reason"] == ("failed" if failed else "pending")
    assert result.summary["datasets"]["off"]["arms"]["all_declared"]["mean"] is None
    with pytest.raises(ValueError, match="parameter set"):
        evaluate_selection(f["rankings"]["fixed"], fixed_inputs={"off":root}, allow_partial=True, parameter_sets={"off":"absent"})


def test_mixed_candidate_families_freeze_scope_superset_and_allocation_parity(selection_family, tmp_path, monkeypatch):
    f = selection_family
    inputs = candidate_inputs(f)
    expected = {}
    for item in candidate_inputs(f):
        allocation = evaluate_allocation([item], candidate_rule="primary_profit")
        expected[item.label] = {(p["ticker"],p["window_id"]):p["semantic_key"] for p in allocation.pair_decisions}
    events = []
    for item in inputs:
        original = item.dataset._load_segment
        def load(scope, window, segment, original=original, label=item.label):
            if segment == "oos":
                assert all((name, window.window_id, "is") in events for name in ("off", "on"))
            events.append((label, window.window_id, segment))
            return original(scope, window, segment)
        monkeypatch.setattr(item.dataset, "_load_segment", load)
    result = evaluate_selection(f["rankings"]["is_oos"], inputs=inputs, candidate_rule="primary_profit", baseline="off")
    assert {s["schema_version"] for s in result.metadata["sources"].values()} == {"strategy_lab_dataset_v3", "strategy_lab_dataset_v4"}
    assert all(s["unused_window_ids"] == [4] for s in result.metadata["sources"].values())
    for row in result.tables["decisions"]:
        assert row["semantic_key"] == expected[row["dataset_label"]][row["ticker"],row["block_id"]]
    assert len(result.tables["turnover"]) == 5  # Shared, independent of label count.
    assert any(r["value"] != 0 for r in result.tables["comparisons"] if r["level"] == "paired_block" and r["value"] is not None)
    write_selection(result, tmp_path/"report")
    with pytest.raises(ValueError, match="empty output"):
        write_selection(result, tmp_path/"report")


def test_candidate_abstention_unknown_and_contradictory_is_keep_x(selection_family, tmp_path):
    f = selection_family
    roots = {}
    ranking = open_ranking(f["rankings"]["is_oos"])
    selected = [r["instrument"] for r in ranking.records[0]["rows"] if r["selected"]]
    unselected = next(t for t in f["tickers"] if t not in selected)
    for label in ("off", "on"):
        root = tmp_path/label; shutil.copytree(f["candidates"][label], root); roots[label] = root
        def negative(a, metrics):
            a[:,0,metrics["net_profit_pct"]] = -5
        def tempting(a, metrics):
            a[:,0,metrics["net_profit_pct"]] = [10000, 9999]
            a[:,0,metrics["total_trades"]] = [10000, 10001]
        mutate_group(root, selected[0], 1, negative)
        mutate_group(root, selected[1], 1, lambda a,m:a.__setitem__((slice(None),0,m["net_profit_pct"]), [1,2]))
        mutate_group(root, unselected, 1, tempting)
    mutate_group(roots["on"], selected[1], 1, lambda a,m:a.__setitem__((slice(None),1,m["net_profit_pct"]), np.nan))
    policy = load_candidate_policy(document(tmp_path, "policy.json", dict(schema_version="strategy_lab_candidate_policy_v1",
        name="positive", version="1", objective=dict(metric="net_profit_pct",direction="maximize"),
        constraints=[dict(metric="net_profit_pct",operator=">=",value=0)], selection_count=1,
        no_eligible="unavailable", evidence_classification="exploratory")))
    result = evaluate_selection(ranking, inputs=candidate_inputs(f, roots), candidate_policy=policy, baseline="off")
    rows = [r for r in result.tables["decisions"] if r["block_id"] == 1]
    assert {r["ticker"] for r in rows if r["selected"]} == set(selected)
    assert all(r["status"] == "abstained" and r["value"] == 0 for r in rows if r["ticker"] == selected[0])
    unknown = next(r for r in rows if r["ticker"] == selected[1] and r["dataset_label"] == "on")
    assert unknown["status"] == "unknown" and unknown["value"] is None
    block = next(r for r in result.tables["blocks"] if r["block_id"] == 1 and r["arm"] == "x_top_k" and r["dataset_label"] == "on")
    assert block["selected_count"] == 2 and block["cash_weight"] >= .5 and block["value"] is None
    assert result.summary["datasets"]["on"]["random_path_mean_distribution"]["mean"] is None


def test_public_cli_all_commands_and_refusals(selection_family, tmp_path):
    f = selection_family
    def call(*args):
        return subprocess.run([sys.executable, "-B", "-m", "tools.strategy_lab.selection", *map(str,args)], capture_output=True, text=True)
    prepared = call("prepare", f["work"]/"fixed.json", "--ticker-list", f["universe"], "--data-root", f["pack"], "--output-dir", tmp_path/"ranking")
    assert prepared.returncode == 0, prepared.stderr
    assert json.loads(prepared.stdout)["status"] == "published"
    transformed = call("fixed-request", "--ranking", tmp_path/"ranking", "--template", f["work"]/"off-template.json", "--warmup-bars", 100, "--output-dir", tmp_path/"transform")
    assert transformed.returncode == 0, transformed.stderr
    fixed = call("evaluate", "--ranking", tmp_path/"ranking", "--fixed-dataset", "off="+str(f["fixed"]["off"]), "--output-dir", tmp_path/"fixed-report")
    assert fixed.returncode == 0, fixed.stderr
    candidates = call("evaluate", "--ranking", f["rankings"]["is_oos"], "--dataset", "off="+str(f["candidates"]["off"]), "--scope", "research", "--rule", "primary_profit", "--output-dir", tmp_path/"candidate-report")
    assert candidates.returncode == 0, candidates.stderr
    assert json.loads(candidates.stdout)["status"] == "published"
    refused = call("evaluate", "--ranking", tmp_path/"ranking", "--fixed-dataset", "off="+str(f["fixed"]["off"]), "--scope", "research", "--output-dir", tmp_path/"refused")
    assert refused.returncode != 0 and not (tmp_path/"refused").exists()


def test_real_filter_candidate_choices_differ_without_changing_market_membership(selection_family, tmp_path):
    f = selection_family
    policy = load_candidate_policy(document(tmp_path, "gate.json", dict(schema_version="strategy_lab_candidate_policy_v1",
        name="nine trades", version="1", objective=dict(metric="net_profit_pct",direction="maximize"),
        constraints=[dict(metric="total_trades",operator=">=",value=9)], selection_count=1,
        no_eligible="unavailable", evidence_classification="exploratory")))
    result = evaluate_selection(f["rankings"]["is_oos"], inputs=candidate_inputs(f), candidate_policy=policy)
    off = {(r["ticker"],r["block_id"]):r for r in result.tables["decisions"] if r["dataset_label"] == "off"}
    on = [r for r in result.tables["decisions"] if r["dataset_label"] == "on"]
    assert any(r["semantic_key"] != off[r["ticker"],r["block_id"]]["semantic_key"] and r["semantic_key"] is not None and
        off[r["ticker"],r["block_id"]]["semantic_key"] is not None for r in on)
    assert all(r["rank"] == off[r["ticker"],r["block_id"]]["rank"] and r["selected"] == off[r["ticker"],r["block_id"]]["selected"] for r in on)


def test_scope_authority_and_detached_unlock_evidence(selection_family, tmp_path):
    from dataclasses import replace
    f = selection_family
    root = tmp_path/"locked"; shutil.copytree(f["candidates"]["off"], root)
    _rewrite_runspec(root, lambda raw:raw["preregistration"]["analysis_scopes"][0].update(requires_unlock=True))
    d = open_dataset(root)
    with pytest.raises(ValueError, match="requires.*unlock"):
        d.resolve_scope("research")
    path = document(tmp_path, "unlock.json", dict(frozen="synthetic"))
    scope = d.resolve_scope("research", unlock=True, policy_path=path)
    result = evaluate_selection(f["rankings"]["is_oos"], inputs=[DatasetInput("locked",d,scope)], candidate_rule="primary_profit")
    scope.unlock_evidence["policy_sha256"] = "changed after evaluation"
    assert result.metadata["sources"]["locked"]["scope"]["unlock_evidence"]["policy_sha256"] != "changed after evaluation"
    with pytest.raises(ValueError, match="overlaps"):
        write_selection(result, path.parent)
    item = candidate_inputs(f)[0]
    for restricted in (item.dataset.subset_scope(item.scope,tickers=item.scope.tickers[:1]),
                       item.dataset.subset_scope(item.scope,window_ids=[1,2])):
        with pytest.raises(ValueError, match="authorized"):
            evaluate_selection(f["rankings"]["is_oos"], inputs=[replace(item,scope=restricted)], candidate_rule="primary_profit")


def test_draw_paths_and_paired_deltas_against_scalar_oracle(selection_family, monkeypatch):
    import importlib
    owner = importlib.import_module("tools.strategy_lab.selection.evaluate")
    f = selection_family
    ranking = open_ranking(f["rankings"]["fixed"])
    roster = f["tickers"]
    values = [[10., -20., 30.], [-40., 50., -60.]]
    cells = {label:{(b+1,t):dict(value=v+offset, status="completed", reason=None, account_id=t+str(b),
        semantic_key=None,candidate_id=None,is_net_profit_pct=None,is_total_trades=None,metrics={})
        for b,row in enumerate(values) for t,v in zip(roster,row)} for label,offset in (("off",0),("on",3))}
    monkeypatch.setattr(owner,"fixed_outcomes",lambda *a,**k:(cells,{},[],[]))
    result = evaluate_selection(ranking,fixed_inputs={"off":"off","on":"on"},baseline="off")
    plans = [ranking.plan(i) for i in (0,1)]
    expected = [[sum(values[b][int(i)] for i in row)/2 for row in plans[b]] for b in (0,1)]
    paths = [(a+b)/2 for a,b in zip(*expected)]
    summary = result.summary["datasets"]["off"]["random_path_mean_distribution"]
    assert summary["mean"] == pytest.approx(sum(paths)/len(paths))
    assert [summary[k] for k in ("p05","median","p95")] == pytest.approx(np.quantile(paths,[.05,.5,.95]))
    assert not np.allclose(np.quantile(paths,[.05,.5,.95]), np.mean([np.quantile(v,[.05,.5,.95]) for v in expected], axis=0))
    deltas = [r for r in result.tables["comparisons"] if r["level"] == "paired_random_path_distribution"]
    assert len(deltas) == 1 and deltas[0]["mean"] == pytest.approx(3) and deltas[0]["finite"] == 32
    # Unknown unselected outcomes do not invalidate X; controls keep full denominators.
    unselected = next(r["instrument"] for r in ranking.records[0]["rows"] if not r["selected"])
    cells["on"][1,unselected].update(value=None,status="unknown",reason="pending")
    result = evaluate_selection(ranking,fixed_inputs={"off":"off","on":"on"},baseline="off")
    assert result.summary["datasets"]["on"]["arms"]["x_top_k"]["mean"] is not None
    assert result.summary["datasets"]["on"]["arms"]["all_declared"]["mean"] is None
    assert result.summary["datasets"]["on"]["random_path_mean_distribution"]["mean"] is None
    assert next(r for r in result.tables["comparisons"] if r["level"] == "paired_random_path_distribution")["mean"] is None


def test_underfill_empty_eligibility_known_cash_and_zero():
    from tools.strategy_lab.selection.evaluate import arm_return, memberships
    rows = [dict(instrument="A",eligible=True,score=1),dict(instrument="B",eligible=False,score=None)]
    arms = memberships(dict(rows=rows),["A","B"],2)
    cells = {"A":dict(status="completed",value=0),"B":dict(status="unknown",value=None)}
    assert arm_return(*arms["x_top_k"],cells)["value"] == 0
    assert arm_return(*arms["x_top_k"],cells)["cash_weight"] == .5
    assert arm_return(*arms["all_declared"],cells)["value"] is None
    cells["A"] = dict(status="abstained",value=0)
    assert arm_return(*arms["x_top_k"],cells)["cash_weight"] == 1
    rows[0]["eligible"] = False
    empty = memberships(dict(rows=rows),["A","B"],2)
    assert arm_return(*empty["all_x_eligible"],cells)["value"] == 0
    assert arm_return(*empty["all_x_eligible"],cells)["cash_weight"] == 1


def test_exact_matrix_family_mixing_and_interrupted_publication(selection_family, tmp_path, monkeypatch):
    from _selection_helpers import request
    from tools.strategy_lab.selection import prepare_ranking
    from tools.strategy_lab import dataset as publication
    f = selection_family
    prepare_ranking(document(tmp_path,"monthly.json",request(cadence=1)),tmp_path/"monthly",ticker_list=f["universe"],data_root=f["pack"])
    with pytest.raises(ValueError,match="full exact.*matrix"):
        evaluate_selection(tmp_path/"monthly",fixed_inputs=f["fixed"])
    with pytest.raises(ValueError,match="exactly one"):
        evaluate_selection(f["rankings"]["fixed"],fixed_inputs=f["fixed"],inputs=candidate_inputs(f))
    result = evaluate_selection(f["rankings"]["fixed"],fixed_inputs=f["fixed"])
    original = publication.atomic_write
    def interrupt(path,writer):
        if path.name == "blocks.csv": raise OSError("synthetic report interruption")
        return original(path,writer)
    monkeypatch.setattr(publication,"atomic_write",interrupt)
    with pytest.raises(OSError,match="interruption"):
        write_selection(result,tmp_path/"interrupted")
    assert not (tmp_path/"interrupted/selection.json").exists()


def test_real_multi_month_oos_adapter_holds_membership(selection_family, tmp_path):
    from _selection_helpers import request
    from _filter_helpers import is_request
    from tools.strategy_lab.selection import prepare_ranking
    from tools.strategy_lab.generate import generate_dataset
    from core.ticker_selector import build_calendar
    f = selection_family
    raw = request(mode="is_oos", start="2024-01-01", end="2024-08-15", cadence=4)
    raw["calendar"].update(oosMonths=2)
    prepare_ranking(document(tmp_path,"x.json",raw),tmp_path/"ranking",ticker_list=f["universe"],data_root=f["pack"])
    candidate = is_request()
    candidate["generation"]["market_data"]["instruments"] = [dict(instrument_id=t,cell="research") for t in f["tickers"]]
    candidate["generation"]["windows"].update(requested_start="2024-01-01",requested_end="2024-08-01",
        oos_period_months=2,expected_window_count=3)
    candidate["preregistration"]["analysis_scopes"][0]["window_numbers"] = [1,2,3]
    generate_dataset(document(tmp_path,"candidate.json",candidate),data_root=f["pack"],output_dir=tmp_path/"candidates")
    d = open_dataset(tmp_path/"candidates")
    ranking = open_ranking(tmp_path/"ranking")
    assert ranking.metadata["calendar"]["periods"] == build_calendar("2024-01-01","2024-08-15",60,4,is_months=1,oos_months=2)
    result = evaluate_selection(ranking,inputs=[DatasetInput("off",d,d.resolve_scope("research"))],candidate_rule="primary_profit")
    assert result.summary["expected_blocks"] == 3 and result.summary["duration_month_counts"] == {"2":3}
    decisions = result.tables["decisions"]
    assert {(r["ticker"],r["rank"],r["selected"]) for r in decisions if r["block_id"]==1} == {
        (r["ticker"],r["rank"],r["selected"]) for r in decisions if r["block_id"]==2}
    assert len(result.tables["turnover"]) == 5
    assert [len(p["windows"]) for p in ranking.metadata["calendar"]["periods"]] == [2,1]


def test_fixed_v1_v2_mix_explicit_parameter_set_and_duplicate_join(selection_family, tmp_path, monkeypatch):
    from tools.strategy_lab.fixed.run import run
    f = selection_family
    raw = json.loads((f["work"]/"off-template.json").read_text())
    raw["schema_version"] = "strategy_lab_fixed_runspec_v1"
    raw["generation"]["execution"].pop("entryFilters")
    raw["generation"]["execution"]["entry_filters"] = []
    raw["generation"]["parameter_sets"].append(dict(name="alternate",params=dict(stopLP=4)))
    raw["generation"]["expected_run_count"] = 6
    write_fixed_request(f["rankings"]["fixed"],document(tmp_path,"template.json",raw),tmp_path/"transform",warmup_bars=100)
    run(tmp_path/"transform/fixed-request.json",f["pack"],tmp_path/"legacy")
    with pytest.raises(ValueError,match="unambiguous"):
        evaluate_selection(f["rankings"]["fixed"],fixed_inputs={"legacy":tmp_path/"legacy"})
    result = evaluate_selection(f["rankings"]["fixed"],fixed_inputs={"legacy":tmp_path/"legacy","on":f["fixed"]["on"]},
        parameter_sets={"legacy":"base"})
    assert {s["schema_version"] for s in result.metadata["sources"].values()} == {"strategy_lab_fixed_dataset_v1","strategy_lab_fixed_dataset_v2"}
    assert len(result.metadata["sources"]["legacy"]["unused_account_ids"]) == 6
    original = storage.open_dataset
    def duplicate(*args,**kwargs):
        manifest = original(*args,**kwargs)
        manifest["contract"]["accounts"].append(deepcopy(manifest["contract"]["accounts"][0]))
        return manifest
    monkeypatch.setattr(storage,"open_dataset",duplicate)
    with pytest.raises(ValueError,match="duplicate Fixed semantic"):
        evaluate_selection(f["rankings"]["fixed"],fixed_inputs={"on":f["fixed"]["on"]})
