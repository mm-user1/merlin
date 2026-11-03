"""Real bounded provider fixtures, then offline reports across saved families."""
from copy import deepcopy
import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from _filter_helpers import packs, is_request, fixed_request, save_request
from _analysis_helpers import mutate_group
from tools.strategy_lab.generate import generate_dataset
from tools.strategy_lab.fixed.run import run
from tools.strategy_lab.fixed import compare_fixed
from tools.strategy_lab.fixed import storage
from tools.strategy_lab.analysis import (open_dataset, DatasetInput, evaluate_field_audit, write_field_audit,
    evaluate_screen, write_screen, AnalysisError)
from tools.strategy_lab.analysis.screen import _strict_pair
from tools.strategy_lab.dataset import atomic_write_json, artifact_record


def read_rows(path):
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="module")
def families(packs, tmp_path_factory):
    work = tmp_path_factory.mktemp("comparison-families")
    target, filters = packs
    roots = {}
    for name, raw in (("v4off", is_request()), ("v4on", is_request(filters)), ("v3", is_request())):
        if name == "v3":
            raw["schema_version"] = "strategy_lab_runspec_v2"
            raw["generation"]["execution"] = {"policy": "risk_bracket_v1", "entry_filters": []}
        raw["generation"]["market_data"]["instruments"].append({"instrument_id": "BYBIT_BBBUSDT", "cell": "research"})
        roots[name] = work/name
        generate_dataset(save_request(work, raw, name+".json"), data_root=target, output_dir=roots[name])
    for name, raw in (("fixedoff", fixed_request()), ("fixedon", fixed_request(filters)), ("fixedv1", fixed_request())):
        if name == "fixedv1":
            raw["schema_version"] = "strategy_lab_fixed_runspec_v1"
            raw["generation"]["execution"].pop("entryFilters")
            raw["generation"]["execution"]["entry_filters"] = []
        raw["generation"]["parameter_sets"] = [dict(name=f"p{x}", params={"stopLP": x}) for x in (2, 3, 4)]
        raw["generation"]["market_data"]["instruments"].append({"instrument_id": "BYBIT_BBBUSDT", "cell": "synthetic_a"})
        raw["generation"]["expected_run_count"] = 6
        roots[name] = work/name
        run(save_request(work, raw, name+".json"), target, roots[name])
    return roots


def item(path, label):
    dataset = open_dataset(path)
    return DatasetInput(label, dataset, dataset.resolve_scope("research"))


def test_offline_public_apis_relocation_and_strict_stream(families, tmp_path, monkeypatch):
    from core import data_input
    from tools.strategy_lab import config
    for module, name in ((data_input, "prepare_input"), (data_input, "replay_input"), (config, "get_strategy_config")):
        monkeypatch.setattr(module, name, lambda *a, **k: pytest.fail("report used live source/strategy"))
    for family in ("v3", "v4off"):
        relocated = tmp_path/family
        shutil.copytree(families[family], relocated)
        i = item(relocated, family)
        result = evaluate_field_audit(i.dataset, i.scope)
        write_field_audit(result, tmp_path/(family+"-audit"))
    inputs = [item(families[n], n) for n in ("v4off", "v4on")]
    result = evaluate_screen(inputs, candidate_rule="primary_profit", filter_baseline="v4off")
    out = tmp_path/"paired"
    write_screen(result, out)
    pairs = read_rows(out/"filter_candidate_pairs.csv")
    assert len(pairs) == 2*2*2*2
    assert any(float(r["delta_net_profit_pct"]) != 0 for r in pairs)
    assert len(read_rows(out/"filter_selection_pairs.csv")) == 4
    assert all(len(result.metadata["diagnostic_access"][i.label]["entries"]) == 8 for i in inputs)
    relocated = tmp_path/"fixed"
    shutil.copytree(families["fixedv1"], relocated)
    compare_fixed([("old", relocated), ("on", families["fixedon"])], tmp_path/"fixed-report", parameter_axes=["stopLP"])
    meta = json.loads((tmp_path/"fixed-report/comparison.json").read_text())
    assert meta["account_count"] == 12 and len(meta["variants"]) == 6
    assert {r["period_kind"] for r in read_rows(tmp_path/"fixed-report/calendar.csv")} == {"continuous_calendar_slice"}


@pytest.mark.parametrize("field", ["economics", "planning", "seed", "geometry", "resources", "pin", "admission", "segments", "scope"])
def test_strict_pair_projections_refuse_before_metrics(families, field):
    inputs = [item(families[n], n) for n in ("v4off", "v4on")]
    d = inputs[1].dataset
    if field in ("economics", "planning"):
        d.run_spec["generation"][field] = {"changed": True}
    elif field == "seed":
        d.run_spec["preregistration"]["seed"] += 1
    elif field == "geometry":
        d.manifest["identity"]["geometry_sha256"] = "0"*64
    elif field == "resources":
        d.manifest["identity"]["resources"] = {}
    elif field == "scope":
        inputs[1] = DatasetInput(inputs[1].label, d, d.subset_scope(inputs[1].scope, tickers=(inputs[1].scope.tickers[0],)))
    else:
        # Inject a parsed-file mismatch at this read-only compatibility boundary.
        original = json.loads
        with pytest.MonkeyPatch.context() as patch:
            def loads(text, *a, **k):
                doc = original(text, *a, **k)
                if isinstance(doc, dict) and doc.get("effective_entry_filters"):
                    b = doc["instruments"][0]
                    if field == "pin": b["input_pin"]["input_fingerprint"] = "different"
                    elif field == "admission": b["entry_admission"]["maxEntryLeverage"] += 1
                    else: b["segments"] = []
                return doc
            patch.setattr(json, "loads", loads)
            with pytest.raises(AnalysisError, match="differs at"):
                _strict_pair(inputs, "v4off")
        assert not any(i.dataset.access_log for i in inputs)
        return
    with pytest.raises(AnalysisError, match="differs at"):
        evaluate_screen(inputs, candidate_rule="primary_profit", filter_baseline="v4off")
    assert not any(i.dataset.access_log for i in inputs)


def test_fixed_collector_once_detached_and_duplicate_cells(families, tmp_path, monkeypatch):
    original = storage.read_account
    calls = []
    def read(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(args[1]["account_id"])
        return result
    monkeypatch.setattr(storage, "read_account", read)
    compare_fixed([("off", families["fixedoff"]), ("on", families["fixedon"])], tmp_path/"report")
    assert len(calls) == 12
    calls.clear()
    from tools.strategy_lab.compare_filters import compare
    compare(families["fixedoff"], families["fixedon"], tmp_path/"old")
    assert len(calls) == 12
    snapshots = []
    manifest = storage.open_dataset(families["fixedoff"], on_account=lambda a, r, rows: snapshots.append((a, r, rows)))
    snapshots[0][0]["params"]["stopLP"] = 999
    assert manifest["contract"]["accounts"][0]["params"]["stopLP"] == 2
    with pytest.raises(AnalysisError, match="duplicate account_id.*old.*new"):
        compare_fixed([("old", families["fixedv1"]), ("new", families["fixedoff"])], tmp_path/"duplicate")
    assert not (tmp_path/"duplicate").exists()


def test_fixed_metric_cohorts_failed_middle_and_tampering(families, tmp_path):
    root = tmp_path/"partial"
    shutil.copytree(families["fixedoff"], root)
    manifest = storage.open_dataset(root)
    accounts = manifest["contract"]["accounts"]
    # Three declared strategy values remain; failed middle is not removed from adjacency.
    manifest["complete"] = False
    manifest["completed"] = {a["account_id"]: manifest["completed"][a["account_id"]] for a in accounts[:2]}
    manifest["failure"] = dict(account_id=accounts[2]["account_id"], reason="synthetic failure")
    (root/"manifest.json").unlink()
    atomic_write_json(root/"manifest.partial.json", manifest)
    with pytest.raises(ValueError, match="partial"):
        compare_fixed([("partial", root)], tmp_path/"refused")
    out = tmp_path/"report"
    compare_fixed([("partial", root)], out, allow_partial=True, parameter_axes=["stopLP"])
    neighbors = read_rows(out/"neighbours.csv")
    assert all(abs(float(r["distance"])) == 1 for r in neighbors if r["level"] == "edge")
    assert all(r["status"] == "no_finite_distinct_neighbor" for r in neighbors
        if r["level"] == "neighborhood" and r["center_account_id"] in {a["account_id"] for a in accounts[:2]})
    summaries = read_rows(out/"summaries.csv")
    common = [r for r in summaries if r["level"] == "primary" and r["membership"] == "common"]
    assert common and all(r["reason"] == "empty_common_sample" and r["mean"] == "" for r in common)
    claim = manifest["completed"][accounts[0]["account_id"]]
    (root/claim["monthly.csv"]["path"]).write_text("corrupt")
    with pytest.raises(ValueError, match="verification"):
        compare_fixed([("partial", root)], tmp_path/"corrupt", allow_partial=True)
    assert not (tmp_path/"corrupt").exists()


@pytest.mark.parametrize("axis", ["unknown", "initialCapital", "enableLong"])
def test_fixed_axes_reject_without_publication(families, tmp_path, axis):
    with pytest.raises(AnalysisError, match="numeric strategy"):
        compare_fixed([("off", families["fixedoff"])], tmp_path/"out", parameter_axes=[axis])
    assert not (tmp_path/"out").exists()


def test_fresh_fixed_and_v4_cli_stdout(families, tmp_path):
    commands = [
        ["tools.strategy_lab.fixed", "report", "--dataset", str(families["fixedoff"]), "--output-dir", str(tmp_path/"old")],
        ["tools.strategy_lab.fixed", "compare", "--dataset", "off="+str(families["fixedoff"]), "--output-dir", str(tmp_path/"new")],
        ["tools.strategy_lab.analysis.cli", "analyze", "--dataset", str(families["v4off"]), "--scope", "research", "--output", str(tmp_path/"analysis")],
        ["tools.strategy_lab.analysis.cli", "screen", "--dataset", "off="+str(families["v4off"]), "--scope", "research",
         "--rule", "primary_profit", "--output", str(tmp_path/"screen")],
    ]
    for command in commands:
        result = subprocess.run([sys.executable, "-B", "-m", *command], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert isinstance(json.loads(result.stdout), dict)
        assert "Discovered" not in result.stdout


def test_filter_reselection_keeps_local_excluded_candidate_oos(families, tmp_path):
    from tools.strategy_lab.analysis import load_candidate_policy
    roots = []
    for name in ("v4off", "v4on"):
        root = tmp_path/name
        shutil.copytree(families[name], root)
        roots.append(root)
    def off(a, m):
        a[:, 0, m["max_drawdown_pct"]] = [1, 2]
        a[:, 0, m["net_profit_pct"]] = [float("nan"), 20]
        a[:, 1, m["net_profit_pct"]] = [17, 9]
    def on(a, m):
        a[:, 0, m["max_drawdown_pct"]] = [2, 1]
        a[:, 0, m["net_profit_pct"]] = [3, 4]
        a[:, 1, m["net_profit_pct"]] = [50, 23]
    mutate_group(roots[0], "BYBIT_AAAUSDT", 1, off)
    mutate_group(roots[1], "BYBIT_AAAUSDT", 1, on)
    path = tmp_path/"policy.json"
    path.write_text(json.dumps(dict(schema_version="strategy_lab_candidate_policy_v1", name="minimum DD", version="1",
        objective=dict(metric="max_drawdown_pct", direction="minimize"), constraints=[], selection_count=1,
        no_eligible="unavailable", evidence_classification="exploratory")))
    result = evaluate_screen([item(r, n) for r, n in zip(roots, ("off", "on"))],
        candidate_policy=load_candidate_policy(path), filter_baseline="off")
    write_screen(result, tmp_path/"report")
    rows = read_rows(tmp_path/"report/filter_selection_pairs.csv")
    row = next(r for r in rows if r["ticker"] == "BYBIT_AAAUSDT")
    assert row["off_semantic_key"] != row["on_semantic_key"]
    assert row["off_selection_available"] == row["on_selection_available"] == "True"
    assert row["off_ticker_score_available"] == "False" and row["on_ticker_score_available"] == "True"
    assert row["off_in_common_pool"] == row["on_in_common_pool"] == "False"
    assert float(row["off_net_profit_pct"]) == 17 and float(row["on_net_profit_pct"]) == 23
    assert float(row["delta_net_profit_pct"]) == 6


def test_fixed_exact_period_metric_specific_cohort_and_ambiguity(families, tmp_path):
    from tools.strategy_lab.fixed.compare import _summaries, _period
    from tools.strategy_lab.analysis.research_inputs import digest
    out = tmp_path/"report"
    compare_fixed([("off", families["fixedoff"])], out)
    raw = read_rows(out/"accounts.csv")
    rows = []
    for r in raw:
        for key in ("period_key", "variant", "stratum", "input_pin", "admission", "strategy_params", "metrics"):
            r[key] = json.loads(r[key])
        r["timeframe_minutes"] = int(r["timeframe_minutes"])
        rows.append(r)
    assert rows[0]["period_key"][1].endswith("23:59:59.999999+00:00")
    assert rows[0]["period_key"][1] != rows[0]["input_pin"]["bounds"]["end"]
    a = storage.open_dataset(families["fixedoff"])["contract"]["accounts"][0]
    a["input_pin"]["bounds"]["evaluation_start"] = "2024-03-01T08:00:00+08:00"
    assert _period(a)[0] == rows[0]["period_key"][0]
    rows[0]["metrics"]["max_drawdown_mtm_pct"] = None
    rows[2]["input_pin"]["input_fingerprint"] = "different target snapshot"
    summaries, _ = _summaries(rows)
    common = [r for r in summaries if r["level"] == "primary" and r["membership"] == "common"]
    assert all(r["finite"] == 1 for r in common if r["metric"] == "max_drawdown_mtm_pct")
    assert all(r["finite"] == 2 for r in common if r["metric"] == "net_profit_pct")
    assert all(r["sample_classification"] == "descriptive_common_membership" for r in common if r["metric"] == "net_profit_pct")
    other = deepcopy(rows[0]); other["stratum_key"] = digest({"capital": 2000})
    temporal = deepcopy(rows[0]); temporal["period_key"] = ["2024-03-02T00:00:00+00:00", rows[0]["period_key"][1]]
    tf = deepcopy(rows[0]); tf["variant"]["timeframe_minutes"] = tf["timeframe_minutes"] = 120
    tf["variant_key"] = digest(tf["variant"])
    summaries, groups = _summaries([*rows, other, temporal, tf])
    assert len(groups) == 6
    assert all(r["mean"] is None for r in summaries if r["level"].startswith("cross_"))
    duplicated = deepcopy(rows[0]); duplicated["input_pin"]["bounds"]["warmup_start"] = "different"
    with pytest.raises(AnalysisError, match="ambiguous duplicate ticker"):
        _summaries([*rows, duplicated])


@pytest.mark.parametrize("artifact", ["result.json", "curve.parquet", "monthly.csv"])
def test_fixed_any_completed_tamper_aborts_before_publication(families, tmp_path, artifact):
    root = tmp_path/"source"
    shutil.copytree(families["fixedoff"], root)
    manifest = storage.open_dataset(root)
    # Last claim: even valid early collector snapshots cannot permit a report.
    claim = list(manifest["completed"].values())[-1][artifact]
    with (root/claim["path"]).open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(ValueError, match="verification"):
        compare_fixed([("bad", root)], tmp_path/"out")
    assert not (tmp_path/"out").exists()


def test_fixed_universe_uses_declared_inventory_and_captured_snapshots(families, tmp_path, monkeypatch):
    from tools.strategy_lab.analysis import load_universe
    path = tmp_path/"universe.json"
    path.write_text(json.dumps(dict(schema_version="strategy_lab_universe_v1", name="one",
        evidence_classification="exploratory", selection_note="synthetic", tickers=["BYBIT_BBBUSDT"])))
    u = load_universe(path)
    out = tmp_path/"report"
    compare_fixed([("off", families["fixedoff"])], out, universe=u)
    assert {r["ticker"] for r in read_rows(out/"accounts.csv")} == {"BYBIT_BBBUSDT"}
    saved = (out/"comparison.json").read_bytes()
    u.document["tickers"][0] = "UNKNOWN"
    assert (out/"comparison.json").read_bytes() == saved
    with pytest.raises(AnalysisError, match="mutated"):
        compare_fixed([("off", families["fixedoff"])], tmp_path/"mutated", universe=u)
    # A validated result is consumed from its detached capture, even if the reader's
    # local dictionary is subsequently changed before it returns the manifest.
    original = storage.read_account
    captured_results = []
    def read(*args, **kwargs):
        result = original(*args, **kwargs)
        captured_results.append(result[0])
        return result
    monkeypatch.setattr(storage, "read_account", read)
    snapshots = []
    storage.open_dataset(families["fixedoff"], on_account=lambda a, r, m: snapshots.append(r))
    before = deepcopy(snapshots)
    captured_results[0]["metrics"]["net_profit_pct"] = 999999
    assert snapshots == before
