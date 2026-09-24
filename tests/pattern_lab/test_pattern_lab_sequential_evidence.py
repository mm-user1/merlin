"""Mixed studies, checked offline accounts and resealed contradictions."""
import copy
from dataclasses import replace
import json
from pathlib import Path
import shutil
import os
import subprocess
import sys

import pandas as pd
import pyarrow.parquet as pq
import pytest

from tools.pattern_lab import PatternLabDataError, study, pack_lock
from tools.pattern_lab.study import evidence, spec, contracts
from ._helpers import (ANCHOR_MS, timeframe_bars, instrument_source, publish,
    study_request, study_protocol, fixed_horizon_model, TWO_GREEN_EVERY_BAR, TWO_GREEN_STATE_ENTRY)
from .test_pattern_lab_bracket import rule_entry


def build(root, *, mixed=True):
    values=[(100+i,102+i,99+i,101+i,10+i) for i in range(40)]
    stamps,bars=timeframe_bars(30,values)
    sources=[]
    for venue in ("OKX","BYBIT"):
        entry=rule_entry(venue)
        source=instrument_source(stamps,bars,venue=venue,contract=entry["contract"])
        sources.append(replace(source,instrument_rules=entry["instrument_rules"]))
    publish(root,sources)
    end=ANCHOR_MS+40*1800000
    models=[{"id":"br","model":"atr_bracket","settings":{}}]
    if mixed: models.append(fixed_horizon_model(30,[30,60]))
    request=study_request(protocol=study_protocol(first_ms=ANCHOR_MS,coverage_end_ms=end),
        start_ms=ANCHOR_MS+14*1800000,end_ms=end,warmup_ms=ANCHOR_MS,timeframes=[30],
        hypotheses=[TWO_GREEN_EVERY_BAR,TWO_GREEN_STATE_ENTRY],models=models)
    request.update(schema_version=2,context={},execution={"kind":"development"})
    return request


def test_mixed_public_accounts_typed_tables_and_locked_offline_relocation(tmp_path):
    pack=tmp_path/"pack"
    request=build(pack)
    run=study.run_study(request=request,data_root=pack,output_root=tmp_path/"run")
    results=study.load_results(run["run_root"])
    assert study.EVIDENCE_VIEW_VERSION==1
    summary=study.summarize_results(results)
    assert len(summary["sequential_accounts"])==24
    assert len(summary["groups"])==8
    assert "bracket_source" in results.source
    for identifier in results.completed_instruments:
        for name,schema in __import__('tools.pattern_lab.study.sequential',fromlist=['SCHEMAS']).SCHEMAS.items():
            actual=pq.read_table(Path(run["run_root"])/"jobs"/identifier/f"sequential_{name}.parquet")
            assert actual.schema.equals(schema,check_metadata=False)
        account=results.sequential_account(identifier,timeframe_minutes=30,variant_id="two_green_every",
                                           model_instance_id="br",case_id="long_rr2")
        assert len(account["attempts"])==25 and len(account["path"])==26
        with pytest.raises(PatternLabDataError,match="per-anchor"):
            results.observations(identifier,model_instance_id="br",timeframe_minutes=30,case_id="long_rr2")
    moved=tmp_path/"moved"
    shutil.copytree(run["run_root"],moved)
    with pack_lock.pack_guard(pack):
        study.regenerate_report(moved)
    assert (moved/"derived"/"report.html").is_file()


@pytest.mark.parametrize("mutation",["duplicate_attempt","missing_attempt","unknown_id","bad_link","fee","balance","nonfinite","triple"])
def test_resealed_semantic_damage_refused_before_report_replacement(tmp_path,mutation):
    from tools.pattern_lab.study import sequential
    pack=tmp_path/"pack"
    request=build(pack,mixed=False)
    root=Path(study.run_study(request=request,data_root=pack,output_root=tmp_path/"run")["run_root"])
    before=(root/"derived"/"report.html").read_bytes()
    identifier="OKX_AAA-USDT-SWAP"
    name="attempts" if mutation in ("duplicate_attempt","missing_attempt","unknown_id","bad_link") else "path" if mutation=="balance" else "trades"
    if mutation=="triple":
        family=evidence.read_json(root/evidence.FAMILY_FILE)
        family["models"][0]["model_id"]="impostor"
        evidence.write_json(root/evidence.FAMILY_FILE,family)
    else:
        path=root/"jobs"/identifier/f"sequential_{name}.parquet"
        rows=pq.read_table(path).to_pylist()
        if mutation=="duplicate_attempt": rows.append(rows[0])
        elif mutation=="missing_attempt": rows.pop()
        elif mutation=="unknown_id": rows[0]["variant_id"]="missing"
        elif mutation=="bad_link": rows[0]["trade_id"]="missing"
        elif mutation=="fee": rows[0]["entry_fee"]+=1
        elif mutation=="balance": rows[0]["balance"]+=1
        elif mutation=="nonfinite": rows[0]["net_pnl"]=float('inf')
        pq.write_table(__import__('pyarrow').Table.from_pylist(rows,schema=sequential.SCHEMAS[name]),path)
        bundle_path=root/"jobs"/identifier/"bundle.json"
        bundle=evidence.read_json(bundle_path)
        for item in bundle["files"]:
            if item["name"]==path.name:
                item.update(sha256=evidence.file_digest(path),row_count=len(rows))
        evidence.write_json(bundle_path,bundle)
    seal=evidence.read_json(root/evidence.COMPLETION_FILE)
    seal["evidence_sha256"]={name:evidence.file_digest(root/name) for name in seal["evidence_sha256"]}
    seal["evidence_set_sha256"]=contracts.semantic_digest(seal["evidence_sha256"])
    evidence.write_json(root/evidence.COMPLETION_FILE,seal)
    with pytest.raises(PatternLabDataError): study.load_results(root)
    with pytest.raises(PatternLabDataError): study.regenerate_report(root)
    assert (root/"derived"/"report.html").read_bytes()==before


@pytest.mark.parametrize("missing", ["all", "trades"])
def test_publication_requires_all_sequential_tables_before_writing(tmp_path, missing):
    pack = tmp_path / "pack"
    request = build(pack, mixed=False)
    root = Path(study.run_study(request=request, data_root=pack, output_root=tmp_path/"run")["run_root"])
    result = study.load_results(root)
    identifier = result.completed_instruments[0]
    bundle = result.jobs[identifier]
    tables = {item["table"]: result.table(identifier, item["table"]) for item in bundle["files"]}
    tables = {name: frame for name, frame in tables.items()
              if not (name.startswith("sequential_") if missing == "all" else name == "sequential_trades")}
    target = tmp_path / "publication"
    (target / "spec").mkdir(parents=True)
    shutil.copy2(root / evidence.FAMILY_FILE, target / evidence.FAMILY_FILE)
    with pytest.raises(PatternLabDataError, match="sequential table coverage"):
        evidence.publish_job(target, identifier, tables=tables, stats=bundle["stats"])
    assert not (target / "jobs").exists()


def test_request_forms_warmup_and_fixed_only_document_preservation(tmp_path):
    from tools.pattern_lab.study import validation
    request=build(tmp_path/"pack")
    normalized=validation.validated_request(request)
    assert spec.warmup_requirements(normalized)["30"]["by_model"]=={"br":13}
    assert validation.validated_request(normalized)==normalized
    path=tmp_path/"request.json"
    path.write_text(json.dumps(request))
    assert validation.validated_request(path).semantic_document()==normalized.semantic_document()
    changed=copy.deepcopy(normalized)
    changed.models[0].settings["max_leverage"]=float('inf')
    with pytest.raises(PatternLabDataError): validation.validated_request(changed)
    starved=copy.deepcopy(request)
    starved["study"]["warmup_start_utc"]=starved["study"]["start_utc"]
    with pytest.raises(PatternLabDataError,match="warmup"): validation.validated_request(starved)
    old=copy.deepcopy(request)
    old.update(schema_version=1)
    del old["context"],old["execution"]
    with pytest.raises(PatternLabDataError,match="version 2"): validation.validated_request(old)
    old["models"]=old["models"][1:]
    fixed=validation.validated_request(old)
    assert "by_model" not in spec.warmup_requirements(fixed)["30"]
    assert set(fixed.models[0].as_json())=={"model_instance_id","model_id","model_version","evidence_kind","settings","cases"}


def test_missing_rules_refused_before_output_and_fixed_only_still_allowed(tmp_path):
    from tools.pattern_lab import manifest
    request=build(tmp_path/"pack")
    document=manifest.read_manifest(tmp_path/"pack")
    document["instruments"][0]["instrument_rules"]={"opaque":"archival"}
    manifest.write_manifest(tmp_path/"pack",document)
    with pytest.raises(PatternLabDataError,match="instrument_rules"):
        study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"bad")
    assert not (tmp_path/"bad").exists()
    request["models"]=request["models"][1:]
    study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"fixed")


def test_observation_metric_scoping_and_bracket_inference_boundary(tmp_path, monkeypatch):
    from tools.pattern_lab.study import validation
    from tools.pattern_lab.analysis.source import admit_source
    monkeypatch.setattr(contracts,"_REGISTRY",{k:dict(v) for k,v in contracts._REGISTRY.items()})
    request=build(tmp_path/"pack")
    descriptor=contracts.MetricDescriptor("bracket_test_metric","1",("net_return",),"fraction",lambda frame:None)
    contracts.register_metric(descriptor,builtin=True)
    request["metrics"]=[{"id":"m","metric":"bracket_test_metric"}]
    only=copy.deepcopy(request)
    only["models"]=only["models"][:1]
    with pytest.raises(PatternLabDataError,match="per-anchor"): validation.validated_request(only)
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"mixed")["run_root"])
    metrics=evidence.read_json(root/evidence.METRICS_FILE)
    assert metrics["values"] and all(m["model_instance_id"]=="fh" for m in metrics["values"])
    admit_source(root,model_instances=["fh"],where="test")
    with pytest.raises(PatternLabDataError,match="supports only"):
        admit_source(root,model_instances=["br"],where="test")


@pytest.mark.slow
def test_fresh_cli_direct_spawn_and_fixed_only_api_without_core(tmp_path):
    from ._helpers import REPO_ROOT
    request=build(tmp_path/"pack")
    path=tmp_path/"request.json"
    path.write_text(json.dumps(request))
    env=dict(os.environ)
    env.pop("PYTHONPATH",None)
    outputs=[]
    for workers in (1,2):
        root=tmp_path/f"cli-{workers}"
        completed=subprocess.run([sys.executable,"-B","-m","tools.pattern_lab","study","--spec",str(path),
            "--data-root",str(tmp_path/"pack"),"--output-root",str(root),"--workers",str(workers)],
            cwd=REPO_ROOT,env=env,text=True,capture_output=True,timeout=180)
        assert completed.returncode==0,completed.stdout+completed.stderr
        assert json.loads(completed.stdout)["status"]=="completed"
        outputs.append(study.load_results(root))
    assert outputs[0].completion["identities"]==outputs[1].completion["identities"]
    for identifier in outputs[0].completed_instruments:
        for record in outputs[0].jobs[identifier]["files"]:
            pd.testing.assert_frame_equal(outputs[0].table(identifier,record["table"]),outputs[1].table(identifier,record["table"]))
    request["models"]=request["models"][1:]
    path.write_text(json.dumps(request))
    script='''
import json, sys
from tools.pattern_lab import study
assert 'core' not in sys.modules and 'strategies' not in sys.modules
study.run_study(request=sys.argv[1],data_root=sys.argv[2],output_root=sys.argv[3])
study.regenerate_report(sys.argv[3])
assert 'core' not in sys.modules and 'strategies' not in sys.modules
print(json.dumps({'fixed_only_without_core':True}))
'''
    completed=subprocess.run([sys.executable,"-B","-c",script,str(path),str(tmp_path/"pack"),str(tmp_path/"fixed-fresh")],
        cwd=REPO_ROOT,env=env,text=True,capture_output=True,timeout=90)
    assert completed.returncode==0,completed.stdout+completed.stderr
    assert json.loads(completed.stdout)=={"fixed_only_without_core":True}


def test_lazy_import_failure_restores_path_and_source_copy_side_effect(tmp_path,monkeypatch):
    from tools.pattern_lab.study import bracket
    from ._helpers import REPO_ROOT
    # Independent fresh child also proves the actual absent-directory side effect.
    copy_root=tmp_path/"source-copy"
    for relative in ("src/core","src/utils","src/indicators"):
        original=REPO_ROOT/relative
        if original.is_dir():
            shutil.copytree(original,copy_root/relative,ignore=shutil.ignore_patterns("__pycache__","*.pyc"))
    env=dict(os.environ)
    env.pop("PYTHONPATH",None)
    script='''
import json, sys, sqlite3
from pathlib import Path
root=Path(sys.argv[1])
assert not (root/'src/storage').exists()
def forbidden(*args,**kwargs): raise AssertionError('database call')
sqlite3.connect=forbidden
sys.path.insert(0,str(root/'src'))
import core.engine_v2.kernel
assert 'strategies' not in sys.modules
assert (root/'src/storage').is_dir()
assert not list((root/'src/storage').rglob('*'))
print(json.dumps({'empty_storage_only':True}))
'''
    completed=subprocess.run([sys.executable,"-B","-c",script,str(copy_root)],cwd=REPO_ROOT,env=env,text=True,capture_output=True,timeout=90)
    assert completed.returncode==0,completed.stdout+completed.stderr
    assert json.loads(completed.stdout)=={"empty_storage_only":True}
    prior=list(sys.path)
    def fail(*args,**kwargs): raise ImportError("probe")
    monkeypatch.setattr(bracket.importlib,"import_module",fail)
    with pytest.raises(ImportError,match="probe"): bracket.reference_core()
    assert sys.path==prior


def test_sequential_candidate_refusal_precedes_both_split_calls(tmp_path,monkeypatch):
    from tools.pattern_lab import candidate, analysis
    request=build(tmp_path/"pack")
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    analysis.run_analysis(request={"schema_version":2,"analysis_name":"fixed selected", "model_instances":["fh"],"pairwise":[]},
                          run_root=root,output_root=tmp_path/"analysis")
    def forbidden(*args,**kwargs): raise AssertionError("split reached")
    monkeypatch.setattr(candidate,"_split",forbidden)
    with pytest.raises(PatternLabDataError,match="any sequential model"):
        candidate.freeze_candidate(study_root=root,analysis_root=tmp_path/"analysis",start="2025-10-10T00:00:00Z",
                                   end="2025-10-11T00:00:00Z",warmup_start="2025-10-09T00:00:00Z",output=tmp_path/"candidate.json")
    # The saved refusal helper sees the entire recipe before load's split checks.
    document={key:None for key in candidate._KEYS}
    document.update(schema_version=1,required_code_policy_version=1,
        recipe={"study":{},"study_semantic":{},"study_family":study.load_results(root).family,
                "context_admission":{},"analysis_request":{},"analysis_family":{}})
    with pytest.raises(PatternLabDataError,match="any sequential model"): candidate.load_candidate(document)
