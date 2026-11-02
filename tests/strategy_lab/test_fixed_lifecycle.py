"""Whole-series freeze, strict failure, account publication and saved-only reading."""
from copy import deepcopy
import csv
import importlib
import json
import shutil
import weakref

import pytest

from tools.strategy_lab.dataset import atomic_write_json, artifact_record, load_json
from tools.strategy_lab.fixed.config import resolve
from tools.strategy_lab.fixed.storage import open_dataset, read_account, contract_identity
from tools.strategy_lab.fixed.report import report
from tools.strategy_lab.synthetic_parquet import publish, synthetic_rules
from _fixed_helpers import fixed_pack, request, save_request, snapshot

runner = importlib.import_module("tools.strategy_lab.fixed.run")


def assert_complete_index(out, manifest):
    with (out / "series-index.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    accounts = manifest["contract"]["accounts"]
    assert [r["account_id"] for r in rows] == [a["account_id"] for a in accounts]
    for row, account in zip(rows, accounts):
        claim = manifest["completed"][account["account_id"]]
        result = load_json(out / claim["result.json"]["path"], "result")
        assert row["status"] == "complete" and row["reason"] == ""
        for key in ("parameter_set", "period", "instrument_id", "cell", "timeframe_minutes"):
            assert row[key] == str(account[key])
        assert float(row["net_profit"]) == result["metrics"]["net_profit"]
        for field, name in (("result", "result.json"), ("curve", "curve.parquet"), ("monthly", "monthly.csv")):
            assert row[field] == claim[name]["path"] and (out / row[field]).is_file()


@pytest.mark.parametrize("error", [OSError, KeyboardInterrupt])
def test_final_claim_index_recovery(fixed_pack, tmp_path, monkeypatch, error):
    path = save_request(tmp_path, request())
    out = tmp_path / "out"
    write_index = runner.write_index

    def interrupted_index(root, manifest):
        if len(manifest["completed"]) == len(manifest["contract"]["accounts"]):
            raise error("final claim saved, index interrupted")
        write_index(root, manifest)

    monkeypatch.setattr(runner, "write_index", interrupted_index)
    with pytest.raises(error, match="index interrupted"):
        runner.run(path, fixed_pack, out)
    partial = open_dataset(out, allow_partial=True)
    assert not partial["complete"] and len(partial["completed"]) == 1
    assert partial["failure"] is None and not (out / "manifest.json").exists()
    with (out / "series-index.csv").open(newline="", encoding="utf-8") as stream:
        assert [r["status"] for r in csv.DictReader(stream)] == ["pending"]
    saved = snapshot(out / "accounts")
    before_retry = snapshot(out)

    def forbidden(*args):
        pytest.fail("completed account executed or republished")

    monkeypatch.setattr(runner, "execute_account", forbidden)
    monkeypatch.setattr(runner, "publish_account", forbidden)
    # With zero accounts left, a failed final index write must still withhold completion.
    with pytest.raises(error, match="index interrupted"):
        runner.run(path, fixed_pack, out, resume=True)
    assert snapshot(out) == before_retry
    assert open_dataset(out, allow_partial=True) == partial

    monkeypatch.setattr(runner, "write_index", write_index)
    write_json = runner.atomic_write_json

    def complete_after_index(destination, manifest):
        if destination.name == "manifest.json":
            assert_complete_index(out, manifest)
        write_json(destination, manifest)

    monkeypatch.setattr(runner, "atomic_write_json", complete_after_index)
    assert runner.run(path, fixed_pack, out, resume=True)["complete"]
    assert_complete_index(out, open_dataset(out))
    assert snapshot(out / "accounts") == saved
    complete = snapshot(out)
    assert runner.run(path, fixed_pack, out)["no_op"]
    assert snapshot(out) == complete


def test_final_marker_interruption(fixed_pack, tmp_path, monkeypatch):
    path = save_request(tmp_path, request())
    out = tmp_path / "out"
    write_json = runner.atomic_write_json

    def interrupt_marker(destination, manifest):
        if destination.name == "manifest.json":
            assert_complete_index(out, manifest)
            raise KeyboardInterrupt("index refreshed, completion interrupted")
        write_json(destination, manifest)

    monkeypatch.setattr(runner, "atomic_write_json", interrupt_marker)
    with pytest.raises(KeyboardInterrupt, match="completion interrupted"):
        runner.run(path, fixed_pack, out)
    partial = open_dataset(out, allow_partial=True)
    assert not partial["complete"] and partial["failure"] is None
    assert not (out / "manifest.json").exists()
    assert_complete_index(out, partial)
    saved = snapshot(out / "accounts")
    monkeypatch.setattr(runner, "atomic_write_json", write_json)
    monkeypatch.setattr(runner, "execute_account", lambda *a: pytest.fail("completed account executed"))
    monkeypatch.setattr(runner, "publish_account", lambda *a: pytest.fail("completed account republished"))
    assert runner.run(path, fixed_pack, out, resume=True)["complete"]
    assert_complete_index(out, open_dataset(out))
    assert snapshot(out / "accounts") == saved


@pytest.mark.parametrize("change", ["run_name", "scalar", "cell"])
def test_changed_request_resume_refused(fixed_pack, tmp_path, monkeypatch, change):
    raw = request()
    path = save_request(tmp_path, raw)
    out = tmp_path / "out"
    runner.run(path, fixed_pack, out, preflight_only=True)
    before = snapshot(out)
    if change == "run_name":
        raw["run_name"] += "-changed"
    elif change == "scalar":
        raw["generation"]["parameter_sets"][0]["params"]["stopX"] = 2.17
    else:
        raw["generation"]["market_data"]["instruments"][0]["cell"] = "changed"
    changed = save_request(tmp_path, raw, "changed.json")
    monkeypatch.setattr(runner, "execute_account", lambda *a: pytest.fail("refused request executed"))
    with pytest.raises(ValueError, match="contract changed; use a new output"):
        runner.run(changed, fixed_pack, out, resume=True)
    assert snapshot(out) == before


def test_foreign_output_refused(fixed_pack, tmp_path, monkeypatch):
    path = save_request(tmp_path, request())
    out = tmp_path / "foreign"
    out.mkdir()
    (out / "owner.txt").write_text("unrelated output", encoding="utf-8")
    before = snapshot(out)
    monkeypatch.setattr(runner, "execute_account", lambda *a: pytest.fail("foreign output executed"))
    with pytest.raises(ValueError, match="nonempty output without a Fixed manifest"):
        runner.run(path, fixed_pack, out, resume=True)
    assert snapshot(out) == before


def test_series_interruption_resume_and_memory_scope(fixed_pack, tmp_path, monkeypatch):
    from core import data_input
    raw = request(series=True)
    path = save_request(tmp_path, raw);out = tmp_path / "out"
    refs = []
    prepare = data_input.prepare_input
    def one_frame(*args, **kwargs):
        assert all(r() is None for r in refs), "preflight retained a preceding account frame"
        value = prepare(*args, **kwargs);refs.append(weakref.ref(value.frame));return value
    monkeypatch.setattr(data_input, "prepare_input", one_frame)
    assert runner.run(path, fixed_pack, out, preflight_only=True)["completed"] == 0
    before = snapshot(out)
    with pytest.raises(ValueError, match="--resume"):
        runner.run(path, fixed_pack, out)
    assert snapshot(out) == before
    original = runner.execute_account;calls=[]
    def interrupted(*args):
        calls.append(args[2]["account_id"])
        if len(calls) == 3:
            raise KeyboardInterrupt("bounded synthetic interruption")
        return original(*args)
    monkeypatch.setattr(runner, "execute_account", interrupted)
    with pytest.raises(KeyboardInterrupt):
        runner.run(path, fixed_pack, out, resume=True)
    m = open_dataset(out, allow_partial=True)
    assert len(m["completed"]) == 2 and m["failure"]["account_id"] == calls[-1]
    with pytest.raises(ValueError, match="partial"):
        report(out, tmp_path/"default")
    assert report(out, tmp_path/"partial", allow_partial=True)["accounts"] == 16
    rows = list(csv.DictReader((out/"series-index.csv").open()))
    assert [r["status"] for r in rows] == ["complete"]*2 + ["failed"] + ["pending"]*13
    assert set(r["cell"] for r in rows) == {"synthetic_a", "synthetic_b"}
    completed = {n:v for n,v in snapshot(out).items() if n.startswith(tuple("accounts/"+i for i in m["completed"]))}
    monkeypatch.setattr(runner, "execute_account", original)
    assert runner.run(path, fixed_pack, out, resume=True)["completed"] == 16
    after = snapshot(out)
    assert all(after[n] == v for n,v in completed.items())
    assert all(r() is None for r in refs)
    final = open_dataset(out)
    assert len(final["completed"]) == 16
    assert [a["parameter_set"] for a in final["contract"]["accounts"]] == ["base"]*8 + ["lower_risk"]*8
    assert {a["timeframe_minutes"] for a in final["contract"]["accounts"]} == {5,60}
    runner.run(path, fixed_pack, out)
    assert snapshot(out) == after


def test_claim_failure_leaves_recoverable_files(fixed_pack, tmp_path, monkeypatch):
    path=save_request(tmp_path, request());out=tmp_path/"out"
    original = runner.atomic_write_json;failed=[]
    def crash(path, payload):
        if path.name == "manifest.partial.json" and payload["completed"] and not failed:
            failed.append(True)
            raise OSError("interrupted before account claim")
        original(path, payload)
    monkeypatch.setattr(runner, "atomic_write_json", crash)
    with pytest.raises(OSError, match="before account claim"):
        runner.run(path, fixed_pack, out)
    m=open_dataset(out, allow_partial=True)
    assert not m["completed"] and list(out.glob("accounts/*/result.json"))
    monkeypatch.setattr(runner, "atomic_write_json", original)
    assert runner.run(path, fixed_pack, out, resume=True)["complete"]


def test_deterministic_failure_stops_every_resume(fixed_pack, tmp_path, monkeypatch):
    raw=request(series=True);path=save_request(tmp_path, raw);out=tmp_path/"out"
    original=runner.execute_account
    def refusal(config, ps, account, root):
        if account["instrument_id"] == "BYBIT_BBBUSDT":
            raise ValueError("deterministic synthetic quantity refusal")
        return original(config, ps, account, root)
    monkeypatch.setattr(runner, "execute_account", refusal)
    with pytest.raises(ValueError, match="deterministic"):
        runner.run(path, fixed_pack, out)
    m=open_dataset(out,allow_partial=True)
    assert len(m["completed"]) == 1
    saved=snapshot(out/"accounts")
    with pytest.raises(ValueError, match="deterministic"):
        runner.run(path, fixed_pack, out,resume=True)
    assert snapshot(out/"accounts") == saved and len(open_dataset(out,allow_partial=True)["completed"]) == 1


def test_grid_resolved_duplicate_rejected_before_execution(fixed_pack, tmp_path, monkeypatch):
    raw=request();g=raw["generation"]
    g["periods"]=[dict(name="a",start="2024-02-03T01:00Z",end="2024-02-04",warmup_bars=100),
                  dict(name="b",start="2024-02-03T02:00Z",end="2024-02-04",warmup_bars=100)]
    g.update(timeframes_minutes=[5,240],expected_run_count=4)
    config,resolved=resolve(raw)
    instrument=g["market_data"]["instruments"][0];ps=resolved["parameter_sets"][0]
    fine=[runner.prepare_account(raw,config,(ps,5,p,instrument),fixed_pack)["account_id"] for p in g["periods"]]
    assert fine[0] != fine[1]
    monkeypatch.setattr(runner,"execute_account",lambda *a:pytest.fail("execution before duplicate check"))
    out=tmp_path/"out"
    with pytest.raises(ValueError,match="duplicate account"):
        runner.run(save_request(tmp_path,raw),fixed_pack,out)
    assert not out.exists()


def test_source_replay_mutation_relocation_and_completed_check(fixed_pack,tmp_path,monkeypatch):
    raw=request();path=save_request(tmp_path,raw);out=tmp_path/"out"
    runner.run(path,fixed_pack,out,preflight_only=True)
    moved=tmp_path/"relocated";shutil.copytree(fixed_pack,moved)
    assert runner.run(path,moved,out,resume=True)["complete"]
    before=snapshot(out)
    changed=tmp_path/"changed"
    publish(changed,symbols=("AAA","BBB"),start="2024-01-10",end="2024-04-03",change=24*288+10)
    with pytest.raises(ValueError,match="input/rules/geometry changed"):
        runner.run(path,changed,out)
    assert snapshot(out)==before
    rules=tmp_path/"rules"
    publish(rules,symbols=("AAA","BBB"),start="2024-01-10",end="2024-04-03",rules={"AAA":synthetic_rules(step="0.002",minimum="0.002")})
    with pytest.raises(ValueError,match="input/rules/geometry changed"):
        runner.run(path,rules,out)
    assert snapshot(out)==before
    with pytest.raises((ValueError,OSError)):
        runner.run(path,tmp_path/"absent",out)
    assert snapshot(out)==before
    # Switch the task-owned source after whole-series preflight, before replay.
    fresh=tmp_path/"consumption";source=tmp_path/"source";shutil.copytree(fixed_pack,source)
    replay=runner.replay
    def mutate(binding,root):
        shutil.copytree(changed,root,dirs_exist_ok=True)
        return replay(binding,root)
    monkeypatch.setattr(runner,"replay",mutate)
    with pytest.raises(ValueError,match="changed"):
        runner.run(path,source,fresh)
    assert not open_dataset(fresh,allow_partial=True)["completed"]


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_claimed_results_never_regenerate(fixed_pack,tmp_path,damage):
    path=save_request(tmp_path,request());out=tmp_path/"out"
    runner.run(path,fixed_pack,out)
    curve=next(out.glob("accounts/*/curve.parquet"))
    if damage=="missing":curve.unlink()
    else:curve.write_bytes(b"corrupt")
    before=snapshot(out)
    with pytest.raises(ValueError,match="verification"):
        runner.run(path,fixed_pack,out,resume=True)
    assert snapshot(out)==before


def test_historical_offline_and_family_refusals(fixed_pack,tmp_path,monkeypatch):
    from tools.strategy_lab import config as old_config
    from tools.strategy_lab.analysis.dataset import open_dataset as old_reader
    from core import data_input,grid_v2
    import strategies
    path=save_request(tmp_path,request());out=tmp_path/"out"
    runner.run(path,fixed_pack,out)
    before=snapshot(out)
    def blocked(*args,**kwargs):pytest.fail("saved reader used live registry/plan/source")
    for module,name in [(old_config,"_validate_strategy"),(old_config,"get_strategy_config"),(strategies,"get_strategy_config"),
                        (grid_v2,"build_grid_v2_plan"),(data_input,"prepare_input"),(data_input,"replay_input")]:
        monkeypatch.setattr(module,name,blocked)
    monkeypatch.setattr(old_config,"GRID_V2_ENGINE_VERSION","future")
    assert report(out,tmp_path/"offline")["complete"] and snapshot(out)==before
    with pytest.raises(ValueError,match="unsupported dataset"):
        old_reader(out)
    with pytest.raises(ValueError):old_config.load_run_spec(path)
    for schema in ("strategy_lab_dataset_v2","strategy_lab_dataset_v3",None,"unknown"):
        foreign=tmp_path/str(schema);foreign.mkdir()
        atomic_write_json(foreign/"manifest.json",{"schema_version":schema})
        with pytest.raises(ValueError,match="schema pair"):open_dataset(foreign)


@pytest.mark.parametrize("case", ["request_schema", "missing_request_schema", "label", "count", "runtime", "path", "monthly", "curve_shape"])
def test_resealed_saved_contradictions(fixed_pack,tmp_path,case):
    out=tmp_path/"out";runner.run(save_request(tmp_path,request()),fixed_pack,out)
    m=deepcopy(load_json(out/"manifest.json","manifest"));a=m["contract"]["accounts"][0]
    claim=m["completed"][a["account_id"]]
    if case=="request_schema":m["contract"]["request"]["schema_version"]="strategy_lab_runspec_v2"
    elif case=="missing_request_schema":m["contract"]["request"].pop("schema_version")
    elif case=="label":a["cell"]="different"
    elif case=="count":m["contract"]["accounts"].append(deepcopy(a))
    elif case=="runtime":a["params"]["end"]="2024-02-04T00:00:00Z"
    elif case=="path":claim["result.json"]["path"]="../foreign.json"
    else:
        name="monthly.csv" if case=="monthly" else "curve.parquet"
        artifact=out/claim[name]["path"]
        if case=="monthly":artifact.write_bytes(artifact.read_bytes().replace(b"1000.0",b"2000.0",1))
        else:
            import pyarrow.parquet as pq
            table=pq.read_table(artifact);pq.write_table(table.slice(1),artifact)
        claim[name]=artifact_record(artifact,out)
        rp=out/claim["result.json"]["path"];result=deepcopy(load_json(rp,"result"))
        result["artifacts"][name]=claim[name];atomic_write_json(rp,result);claim["result.json"]=artifact_record(rp,out)
    from tools.strategy_lab.config import canonical_sha256
    m["request_sha256"]=canonical_sha256(m["contract"]["request"])
    m["contract_sha256"]=contract_identity(m["contract"])
    atomic_write_json(out/"manifest.json",m)
    before=snapshot(out)
    with pytest.raises(ValueError):open_dataset(out)
    assert snapshot(out)==before


@pytest.mark.parametrize("mode", ["short_warmup","gap","quote","rules"])
def test_bad_sources_before_output(tmp_path,mode):
    root=tmp_path/"pack"
    publish(root,start="2024-02-01" if mode=="short_warmup" else "2024-01-20",end="2024-02-07",drop=14*288 if mode=="gap" else None)
    if mode in ("quote","rules"):
        path=root/"manifest.json";m=json.loads(path.read_text())
        entry=m["instruments"][0]
        if mode=="quote":entry["verification"]["volume_quote_verified"]=False
        else:entry["instrument_rules"]["quantity_step"]="0"
        path.write_text(json.dumps(m))
    out=tmp_path/"out"
    with pytest.raises((ValueError,OSError)):
        runner.run(save_request(tmp_path,request()),root,out)
    assert not out.exists()
