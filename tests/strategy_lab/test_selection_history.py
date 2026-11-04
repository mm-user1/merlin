"""Causal scalar decisions, frozen calendars, and actual saved-family workflows."""
from copy import deepcopy
import json
import shutil
import numpy as np
import pytest

from _history_helpers import history_family, history_request, plan
from _selection_helpers import document, universe, publish, request
from _fixed_helpers import snapshot
from _analysis_helpers import mutate_group
from tools.strategy_lab.selection import (prepare_history_plan, open_history_plan, write_history_fixed_request,
    evaluate_history, prepare_ranking, evaluate_selection)
from tools.strategy_lab.selection.history_score import project_decision, score_history
from tools.strategy_lab.selection.history_plan import METHODS
from tools.strategy_lab.selection.history_evaluate import pool_spec, read_random_plan
from tools.strategy_lab.analysis import open_dataset, DatasetInput, load_candidate_policy
from tools.strategy_lab.fixed import storage
from tools.strategy_lab.dataset import atomic_write_json


def read(root, name):
    return json.loads((root/name).read_text(encoding="utf-8"))


def scalar_projection(method=METHODS[3]):
    import pandas as pd
    roster = [f"BYBIT_{t}USDT" for t in "ABCDE"]
    blocks = [dict(id=i+1,start=pd.Timestamp(f"2024-{i+1:02}-01",tz="UTC").isoformat(),
        end=pd.Timestamp(f"2024-{i+2:02}-01",tz="UTC").isoformat(),isStart=None) for i in range(7)]
    ledger = {(b["id"],t):dict(status="completed",reason=None,value=float(j+1),account_id=None,
        candidate_id=j,semantic_key=t,is_net_profit_pct=4.,is_total_trades=10.) for b in blocks for j,t in enumerate(roster)}
    r = history_request(mode="is_oos",methods=list(METHODS))
    return project_decision(r,roster,blocks,4,ledger,method,{}), blocks, ledger, r


def test_hand_cascade_exact_ties_and_closed_projection():
    p,_,_,_ = scalar_projection()
    tickers = p["settings"]["roster"]
    # Count, minimum, mean, transfer, current IS, full-ID tie are separately material.
    returns = [[1,1,1,1],[1,1,1,2],[1,1,1,2],[1,1,1,2],[1,1,1,2]]
    for j,t in enumerate(tickers):
        for i,row in enumerate(p["prior"]):
            row["rows"][t].update(value=returns[j][i],is_net_profit_pct=8. if j == 1 else 4.)
        p["current"][t]["is_net_profit_pct"] = 5. if j >= 3 else 4.
    d = score_history(p)
    assert d["ordered"] == [tickers[i] for i in (3,4,2,1,0)]
    assert d["rows"][3]["score"] == [4,1.,1.25,.25,5.]
    assert d["tie_groups"] == [dict(score=[4,1.,1.25,.25,5.],tickers=tickers[3:])]
    q = deepcopy(p); q["current"][tickers[0]]["status"] = "completed"
    with pytest.raises(ValueError,match="exactly these keys"): score_history(q)
    q = deepcopy(p); q["prior"][1]["block"]["start"] = "2023-01-01T00:00:00+00:00"
    with pytest.raises(ValueError,match="adjacent"): score_history(q)
    # Simple ties ignore secondary profits and use only canonical full IDs.
    p["settings"]["method"] = METHODS[0]
    assert score_history(p)["ordered"] == tickers


def test_missing_middle_reentry_abstention_and_selected_nonfinite_is():
    p,blocks,ledger,r = scalar_projection(METHODS[1]); roster=p["settings"]["roster"]; t=roster[0]
    ledger[2,t].update(status="unknown",reason="nonfinite_oos",value=None)
    assert not score_history(project_decision(r,roster,blocks,4,ledger,METHODS[1],{}))["rows"][0]["eligible"]
    assert not score_history(project_decision(r,roster,blocks,5,ledger,METHODS[1],{}))["rows"][0]["eligible"]
    assert score_history(project_decision(r,roster,blocks,6,ledger,METHODS[1],{}))["rows"][0]["eligible"]
    ledger[2,t].update(status="abstained",reason="no_eligible_is_candidate",value=0.,candidate_id=None,is_net_profit_pct=None)
    assert score_history(project_decision(r,roster,blocks,4,ledger,METHODS[1],{}))["rows"][0]["eligible"]
    assert score_history(project_decision(r,roster,blocks,4,ledger,METHODS[3],{}))["rows"][0]["reason"] == "unavailable_historical_is"
    ledger[5,t]["is_net_profit_pct"] = None
    assert score_history(project_decision(r,roster,blocks,4,ledger,METHODS[0],{}))["rows"][0]["reason"] == "nonfinite_current_is"
    ledger[5,t].update(value=99.,status="completed")
    assert score_history(project_decision(r,roster,blocks,5,ledger,METHODS[1],{}))["rows"][0]["eligible"]


def test_strict_positive_zero_negative_and_minimum_cascade():
    p,_,_,_=scalar_projection()
    values=[[0,0,-1,2],[-1,-1,1,1],[1,1,2,2],[.5,1.5,2,2],[0,0,0,0]]
    for j,t in enumerate(p["settings"]["roster"]):
        for i,prior in enumerate(p["prior"]):prior["rows"][t]["value"]=values[j][i]
    d=score_history(p);ts=p["settings"]["roster"]
    assert d["ordered"]==[ts[i] for i in (2,3,1,0,4)]
    assert [r["features"]["positive_count"] for r in d["rows"]]==[1,2,4,4,0]


@pytest.mark.parametrize("mode",["fixed","is_oos"])
def test_plan_two_month_history_offline_mutation_and_resealed_calendar(tmp_path,monkeypatch,mode):
    r=history_request(mode=mode,end="2025-02-28",evaluation="2024-09-01" if mode=="fixed" else "2024-11-01")
    r["calendar"].update(dict(periodMonths=2) if mode=="fixed" else dict(isMonths=2,oosMonths=2))
    root=plan(tmp_path,["BYBIT_AAAUSDT"],dict(r,topN=1)); saved=open_history_plan(root)
    import core.ticker_selector as core
    monkeypatch.setattr(core,"build_calendar",lambda *a,**k:pytest.fail("live calendar offline"))
    assert len(open_history_plan(root).metadata["mapping"]["history_ids"])==4
    saved.metadata["request"]["effective"]["topN"]=2
    with pytest.raises(ValueError,match="mutated"):saved.verify()
    m=read(root,"plan.json"); m["calendar"]["periods"][1]["start"]=m["calendar"]["periods"][0]["start"]
    from tools.strategy_lab.selection.history_plan import plan_identity
    m["plan_identity"]=plan_identity(m); atomic_write_json(root/"plan.json",m)
    with pytest.raises(ValueError,match="gap/start"):open_history_plan(root)


def candidate_inputs(f,scope="research",history="development",unlock=False):
    inputs=[]; histories={}
    for label,root in f["candidates"].items():
        d=open_dataset(root)
        inputs.append(DatasetInput(label,d,d.resolve_scope(scope,unlock=unlock,policy_path=f["policy"] if unlock else None)))
        histories[label]=d.resolve_scope(history,unlock=unlock,policy_path=f["policy"] if unlock else None)
    return inputs,histories


def evaluate_candidates(f,out,**kwargs):
    inputs,histories=candidate_inputs(f)
    return evaluate_history(f["plans"]["is_oos"],out,inputs=inputs,history_scopes=histories,
        candidate_policy=load_candidate_policy(f["policy"]),baseline="off",**kwargs)


def test_real_fixed_all_cells_reuse_partial_and_x_alignment(history_family,tmp_path,monkeypatch):
    f=history_family; calls=[]; original=storage.read_account
    def collect(*a,**kw): calls.append(a[1]["account_id"]); return original(*a,**kw)
    monkeypatch.setattr(storage,"read_account",collect)
    out=tmp_path/"fixed"
    evaluate_history(f["plans"]["fixed"],out,fixed_inputs=f["fixed"],baseline="off")
    assert len(calls)==64 and len(set(calls))==64
    m=read(out,"history-evaluation.json"); h=read(out,"history.json")["rows"]; d=read(out,"decisions.json")["decisions"]
    s=read(out,"summary.json")
    assert len(h)==64 and s["expected_blocks"]==4 and s["excluded_history_blocks"]==4
    assert all(v["manifest"]["complete"] for v in m["sources"].values())
    assert m["controls"]["unique_plan_count"]==4 and len(s["random_contexts"])==2
    assert any(s["datasets"]["off"]["methods"][method]["arms"]["top_k"]["mean"] != s["datasets"]["on"]["methods"][method]["arms"]["top_k"]["mean"] for method in METHODS[:2])
    changed=plan(tmp_path,f["tickers"],history_request(k=1,methods=[METHODS[1]]),"changed")
    evaluate_history(changed,tmp_path/"changed-report",fixed_inputs=f["fixed"])
    # Current pending status cannot change decisions/pools; it only changes payoffs.
    partial=tmp_path/"partial"; shutil.copytree(f["fixed"]["off"],partial)
    manifest=storage.open_dataset(partial); missing=manifest["contract"]["accounts"][-1]["account_id"]
    manifest["completed"].pop(missing); manifest["complete"]=False
    (partial/"manifest.json").unlink(); atomic_write_json(partial/"manifest.partial.json",manifest)
    evaluate_history(f["plans"]["fixed"],tmp_path/"partial-report",fixed_inputs={"off":partial},allow_partial=True)
    pd=read(tmp_path/"partial-report","decisions.json")["decisions"]
    assert pd==[v for v in d if v["label"]=="off"]
    assert read(tmp_path/"partial-report","summary.json")["datasets"]["off"]["all_declared"]["mean"] is None
    xr=request(start="2024-05-01",end="2024-08-31",cadence=1)
    ranking=tmp_path/"x"; prepare_ranking(document(tmp_path,"x.json",xr),ranking,ticker_list=universe(tmp_path,f["tickers"]),data_root=f["pack"])
    x=evaluate_selection(ranking,fixed_inputs=f["fixed"])
    actual={(row["label"],tuple(row["block_key"]),row["ticker"]):row["value"] for row in h if not row["history_only"]}
    assert {(row["dataset_label"],tuple(row["block_key"]),row["ticker"]):row["value"] for row in x.tables["decisions"]}==actual
    assert all(len(src["unused_account_ids"])==16 for src in x.metadata["sources"].values())


def test_real_candidate_scopes_freeze_abstention_reentry_and_pools(history_family,tmp_path):
    f=history_family; out=tmp_path/"candidate"; evaluate_candidates(f,out)
    m=read(out,"history-evaluation.json"); h=read(out,"history.json")["rows"]; ds=read(out,"decisions.json")["decisions"]
    assert {s["schema_version"] for s in m["sources"].values()}=={"strategy_lab_dataset_v3","strategy_lab_dataset_v4"}
    trace=m["candidate_access_trace"]
    assert [v["event"] for v in trace]==["is_frozen","is_frozen","oos_reveal","oos_reveal"]*8
    first=f["tickers"][0]
    assert next(r for r in h if r["label"]=="off" and r["block_id"]==1 and r["ticker"]==first)["status"]=="abstained"
    tc={d["block_id"]:next(r for r in d["rows"] if r["ticker"]==first) for d in ds if d["label"]=="off" and d["method"]==METHODS[3]}
    assert not tc[5]["eligible"] and tc[6]["eligible"]
    assert any(d["ordered"] != next(o for o in ds if o["label"]=="on" and o["method"]==d["method"] and o["block_id"]==d["block_id"])["ordered"] for d in ds if d["label"]=="off")
    assert any(d["pool_key"] != next(o for o in ds if o["label"]==d["label"] and o["method"]==METHODS[2] and o["block_id"]==d["block_id"])["pool_key"] for d in ds if d["method"]==METHODS[3])
    import csv
    with (out/"comparisons.csv").open(newline="",encoding="utf-8") as stream:
        comparisons=list(csv.DictReader(stream))
    assert any(row["level"]=="paired_random_path_distribution" and row["reason"]=="different_random_pools" for row in comparisons)
    r=history_request(mode="is_oos",end="2024-12-31",evaluation="2024-11-01",methods=list(METHODS))
    target=plan(tmp_path,f["tickers"],r,"holdout")
    with pytest.raises(ValueError,match="requires"):candidate_inputs(f,scope="temporal_dev")
    inputs,histories=candidate_inputs(f,scope="temporal_dev",unlock=True)
    evaluate_history(target,tmp_path/"holdout-report",inputs=inputs,history_scopes=histories,candidate_policy=load_candidate_policy(f["policy"]))
    assert read(tmp_path/"holdout-report","summary.json")["expected_blocks"]==2
    with pytest.raises(ValueError,match="requires"):candidate_inputs(f,history="locked_history")
    inputs,histories=candidate_inputs(f,scope="temporal_dev",history="locked_history",unlock=True)
    evaluate_history(target,tmp_path/"both-unlocked",inputs=inputs,history_scopes=histories,candidate_policy=load_candidate_policy(f["policy"]))


def test_current_oos_and_future_independence(history_family,tmp_path):
    f=history_family; original=tmp_path/"original"; evaluate_candidates(f,original)
    variant=deepcopy(f); root=tmp_path/"mutated"; shutil.copytree(f["candidates"]["off"],root); variant["candidates"]["off"]=root
    mutate_group(root,f["tickers"][1],5,lambda a,metrics:a.__setitem__((slice(None),1,metrics["net_profit_pct"]),np.nan))
    evaluate_candidates(variant,tmp_path/"changed")
    before=read(original,"decisions.json")["decisions"]; after=read(tmp_path/"changed","decisions.json")["decisions"]
    assert [d for d in before if d["block_id"]==5]==[d for d in after if d["block_id"]==5]
    for d in before:
        if d["block_id"]==5:
            name="plans/"+d["pool_key"]+".npy"
            assert (original/name).read_bytes()==(tmp_path/"changed"/name).read_bytes()
    assert any(a["decision_identity"]!=b["decision_identity"] for a,b in zip(before,after) if a["label"]=="off" and a["block_id"]==6 and a["method"]!=METHODS[2])


def test_report_recomputation_arrays_views_and_calendar_extension(history_family,tmp_path,monkeypatch):
    import csv
    f=history_family; out=tmp_path/"report"; evaluate_candidates(f,out)
    m=read(out,"history-evaluation.json"); h=read(out,"history.json")["rows"]; ds=read(out,"decisions.json")["decisions"]
    summary=read(out,"summary.json"); cells={(r["label"],r["block_id"],r["ticker"]):r for r in h}
    method=METHODS[1]; decisions=[d for d in ds if d["label"]=="off" and d["method"]==method]
    specs={p["pool_key"]:p for p in m["controls"]["plans"]}
    monkeypatch.setattr(np.random,"PCG64",lambda *a:pytest.fail("RNG forbidden on replay"))
    sums=np.zeros(32); top=[]
    for d in decisions:
        spec=specs[d["pool_key"]]; a=read_random_plan(out,spec)
        v=np.array([cells["off",d["block_id"],t]["value"] for t in spec["eligible"]])
        sums+=np.sum(v[a],axis=1)/2
        members=sorted(r["ticker"] for r in d["rows"] if r["selected"])
        top.append(float(np.sum([cells["off",d["block_id"],t]["value"] for t in members])/2))
    target=summary["datasets"]["off"]["methods"][method]
    assert target["arms"]["top_k"]["mean"]==float(np.cumsum(top)[-1]/4)
    random=summary["random_contexts"][target["random_context"]]["path_mean_distribution"]
    assert random["p05"]==float(np.quantile(sums/4,.05)) and random["p95"]==float(np.quantile(sums/4,.95))
    with (out/"decisions.csv").open(newline="",encoding="utf-8") as stream:
        rows=list(csv.DictReader(stream))
    assert len(rows)==len(ds)*4 and rows[0]["decision_identity"]==ds[0]["decision_identity"]
    assert str(target["arms"]["top_k"]["mean"]) in (out/"index.html").read_text()
    # Local decisions ignore global End/evaluationStart/method-list/manifest changes.
    monkeypatch.undo()
    short=history_request(mode="is_oos",end="2024-10-31",evaluation="2024-10-01",methods=[method])
    p=plan(tmp_path,f["tickers"],short,"short")
    inputs,histories=candidate_inputs(f)
    evaluate_history(p,tmp_path/"short-report",inputs=inputs,history_scopes=histories,candidate_policy=load_candidate_policy(f["policy"]))
    short_ds=read(tmp_path/"short-report","decisions.json")["decisions"]
    assert {(d["label"],d["block_id"]):(d["decision_identity"],d["pool_key"]) for d in short_ds}=={
        (d["label"],d["block_id"]):(d["decision_identity"],d["pool_key"]) for d in ds if d["method"]==method and d["block_id"]==6}


def test_selected_nonfinite_is_retains_real_candidate_payoff(history_family,tmp_path):
    f=history_family; root=tmp_path/"nonfinite-is"; shutil.copytree(f["candidates"]["off"],root)
    ticker=f["tickers"][1]
    def edit(a,metrics):
        a[:,0,metrics["net_profit_pct"]]=np.nan
        a[:,0,metrics["total_trades"]]=[100,101]
    mutate_group(root,ticker,5,edit)
    policy=load_candidate_policy(document(tmp_path,"trades.json",dict(schema_version="strategy_lab_candidate_policy_v1",name="trades",version="1",
        objective=dict(metric="total_trades",direction="maximize"),constraints=[],selection_count=1,no_eligible="unavailable",evidence_classification="exploratory")))
    d=open_dataset(root); out=tmp_path/"report"
    evaluate_history(f["plans"]["is_oos"],out,inputs=[DatasetInput("off",d,d.resolve_scope("research"))],
        history_scopes={"off":d.resolve_scope("development")},candidate_policy=policy)
    row=next(r for r in read(out,"history.json")["rows"] if r["ticker"]==ticker and r["block_id"]==5)
    assert row["candidate_id"] is not None and row["is_net_profit_pct"] is None and row["status"]=="completed" and row["value"] is not None
    ds=read(out,"decisions.json")["decisions"]
    assert all(next(r for r in d["rows"] if r["ticker"]==ticker)["reason"]=="nonfinite_current_is" for d in ds if d["block_id"]==5)
    assert next(r for d in ds if d["block_id"]==6 and d["method"]==METHODS[1] for r in d["rows"] if r["ticker"]==ticker)["eligible"]
    assert not next(r for d in ds if d["block_id"]==6 and d["method"]==METHODS[3] for r in d["rows"] if r["ticker"]==ticker)["eligible"]


def test_scope_metric_missingness_and_loaded_policy_refusals(history_family,tmp_path):
    from dataclasses import replace
    f=history_family; inputs,histories=candidate_inputs(f)
    histories["off"]=replace(histories["off"],ticker_cell="other")
    with pytest.raises(ValueError,match="ticker-cell"):evaluate_history(f["plans"]["is_oos"],tmp_path/"wrong-cell",inputs=inputs,history_scopes=histories,candidate_rule="primary_profit")
    histories["off"]=inputs[0].dataset.resolve_scope("temporal_dev",unlock=True,policy_path=f["policy"])
    with pytest.raises(ValueError,match="authorized exact"):evaluate_history(f["plans"]["is_oos"],tmp_path/"wrong-history",inputs=inputs,history_scopes=histories,candidate_rule="primary_profit")
    inputs,histories=candidate_inputs(f)
    inputs[0].dataset.metric_index.pop("net_profit_pct")
    with pytest.raises(ValueError,match="required candidate"):evaluate_history(f["plans"]["is_oos"],tmp_path/"missing-metric",inputs=inputs,history_scopes=histories,candidate_rule="primary_profit")
    inputs,histories=candidate_inputs(f); policy=load_candidate_policy(f["policy"]); policy.document["constraints"][0]["value"]=99
    with pytest.raises(ValueError,match="mutated"):evaluate_history(f["plans"]["is_oos"],tmp_path/"changed-policy",inputs=inputs,history_scopes=histories,candidate_policy=policy)


def test_large_k_exact_ties_and_cash_unknown_arithmetic():
    from tools.strategy_lab.selection.evaluate import arm_return, random_returns, accumulate_paths, midrank
    values=[-35.574492,-38.220776,-19.151818,31.612636,-31.927362,8.160016,.1,12.345678,-7.777777,3.3333331]
    roster=[f"BYBIT_T{i:02}USDT" for i in range(10)]
    top=roster[2:]; sums=np.zeros(64); valid=np.ones(64,dtype=bool); block_values=[]
    plans=np.tile(np.arange(2,10,dtype=np.uint8),(64,1)); plans[32:]=[0,3,4,5,6,7,8,9]
    rows=[[values[(i*3+b)%10]*(1+b)+i*.0137 for i in range(10)] for b in range(3)]
    assert any(sum(v[2:])/8!=float(np.sum(v[2:])/8) for v in rows), "witness must detect Python-sum substitution"
    for row in rows:
        cells={t:dict(value=v,status="completed") for t,v in zip(roster,row)}
        value=arm_return(top[::-1],8,cells)["value"]; block_values.append(value)
        random=random_returns(cells,roster,plans,8)
        assert np.all(random[:32]==value)
        accumulate_paths(sums,valid,random)
    reference=float(np.cumsum(block_values)[-1]/3)
    assert midrank(sums/3,reference)==.75
    assert arm_return([],8,cells)["value"]==0 and arm_return([],8,cells)["cash_weight"]==1
    cells[top[0]].update(value=None,status="unknown")
    assert arm_return(top,8,cells)["value"] is None


@pytest.mark.parametrize("change",["too_early","unsupported_fixed","tc_geometry","boolean_draws","unknown_field"])
def test_bad_history_requests(tmp_path,change):
    r=history_request()
    if change=="too_early":r["evaluationStart"]="2024-04-01"
    elif change=="unsupported_fixed":r["methods"]=[METHODS[2]];r["referenceMethod"]=METHODS[2]
    elif change=="tc_geometry":r=history_request(mode="is_oos",methods=[METHODS[3]]);r["calendar"]["isMonths"]=3
    elif change=="boolean_draws":r["controls"]["draws"]=True
    else:r["hidden_setting"]=1
    with pytest.raises(ValueError):plan(tmp_path,["BYBIT_AAAUSDT","BYBIT_BBBUSDT"],r)
    assert not (tmp_path/"plan/plan.json").exists()


def test_publication_protection_interruption_and_late_preflight(tmp_path,monkeypatch):
    from tools.strategy_lab.selection import history_plan as owner
    from _filter_helpers import fixed_request
    from tools.strategy_lab.fixed.run import run
    ticker="BYBIT_AAAUSDT"; u=universe(tmp_path,[ticker]); request_path=document(tmp_path,"request.json",history_request(k=1))
    with pytest.raises(ValueError):prepare_history_plan(request_path,tmp_path,ticker_list=u)
    original=owner.write_bytes
    # Completion descriptor is the sole publication marker, even after HTML exists.
    import tools.strategy_lab.report_support as support
    write=support.write_bytes
    def interrupt(root,name,data):
        if name=="plan.json":raise OSError("interruption witness")
        return write(root,name,data)
    monkeypatch.setattr(support,"write_bytes",interrupt)
    with pytest.raises(OSError,match="interruption"):prepare_history_plan(request_path,tmp_path/"interrupted",ticker_list=u)
    assert not (tmp_path/"interrupted/plan.json").exists()
    monkeypatch.undo()
    p=plan(tmp_path,[ticker],history_request(k=1),"good")
    raw=fixed_request(); template=document(tmp_path,"template.json",raw)
    write_history_fixed_request(p,template,tmp_path/"transform",warmup_bars=100)
    pack=tmp_path/"late-pack"; publish(pack,count=1,late="2024-02-15")
    with pytest.raises(ValueError,match="coverage starts"):run(tmp_path/"transform/fixed-request.json",pack,tmp_path/"late-run",preflight_only=True)
    assert not (tmp_path/"late-run/manifest.partial.json").exists()


def test_history_symlink_ancestors(tmp_path):
    target=tmp_path/"real";target.mkdir();link=tmp_path/"link"
    try:link.symlink_to(target,target_is_directory=True)
    except OSError as exc:pytest.skip(f"host symlink privilege unavailable: {exc}")
    r=document(target,"request.json",history_request(k=1));u=universe(target,["BYBIT_AAAUSDT"])
    with pytest.raises(ValueError,match="symlink"):prepare_history_plan(link/r.name,tmp_path/"out",ticker_list=u)
    with pytest.raises(ValueError,match="symlink"):prepare_history_plan(r,link/"out",ticker_list=u)


def test_history_cli_json_and_independent_unlocks(history_family,tmp_path):
    import subprocess
    import sys
    f=history_family
    def cli(*args,code=0):
        p=subprocess.run([sys.executable,"-B","-m","tools.strategy_lab.selection",*map(str,args)],capture_output=True,text=True)
        assert p.returncode==code,p.stderr
        if code==0:assert json.loads(p.stdout)["status"]=="published"
        else:assert not p.stdout and p.stderr
    cli("history-plan",f["work"]/"plan-request.json","--ticker-list",f["work"]/"universe.json","--output-dir",tmp_path/"plan")
    cli("history-fixed-request","--plan",tmp_path/"plan","--template",f["work"]/"off-template.json","--warmup-bars",100,"--output-dir",tmp_path/"transform")
    cli("history-evaluate","--plan",tmp_path/"plan","--fixed-dataset",f"off={f['fixed']['off']}","--output-dir",tmp_path/"fixed")
    args=("history-evaluate","--plan",f["plans"]["is_oos"],"--dataset",f"off={f['candidates']['off']}",
        "--scope","research","--history-scope","locked_history","--candidate-policy",f["policy"])
    cli(*args,"--output-dir",tmp_path/"locked",code=2)
    cli(*args,"--unlock-scope","--unlock-policy",f"off={f['policy']}","--output-dir",tmp_path/"still-locked",code=2)
    cli(*args,"--history-unlock-scope","--history-unlock-policy",f"off={f['policy']}","--output-dir",tmp_path/"candidate")
    cli(*args,"--history-scope-for","missing=research","--output-dir",tmp_path/"bad-label",code=2)


def test_empty_underfilled_and_unknown_selected_no_replacement(history_family,tmp_path,monkeypatch):
    import tools.strategy_lab.selection.history_evaluate as owner
    f=history_family;p=open_history_plan(f["plans"]["fixed"]);roster=f["tickers"]
    collected=owner.collect_fixed(roster,60,p.metadata["mapping"]["blocks"],list(f["fixed"].items()),{},False)
    ledger,sources,roots,trace=deepcopy(collected)
    for label in ledger:
        for t in roster:
            ledger[label][4,t].update(status="unknown",reason="synthetic_missing_middle",value=None)
    monkeypatch.setattr(owner,"collect_fixed",lambda *a:(deepcopy(ledger),deepcopy(sources),roots,trace))
    evaluate_history(p,tmp_path/"empty",fixed_inputs=f["fixed"])
    s=read(tmp_path/"empty","summary.json")
    for label in s["datasets"]:
        assert s["datasets"][label]["methods"][METHODS[0]]["arms"]["top_k"]["mean"]==0
        assert next(v for v in s["random_contexts"].values() if v["label"]==label)["method_midrank"][METHODS[0]]==.5
    for label in ledger:
        ledger[label][4,roster[0]].update(status="completed",reason=None,value=0.)
        ledger[label][5,roster[0]].update(status="unknown",reason="synthetic_target",value=None)
    evaluate_history(p,tmp_path/"underfilled",fixed_inputs=f["fixed"])
    d=read(tmp_path/"underfilled","decisions.json")["decisions"][0]
    assert [r["ticker"] for r in d["rows"] if r["selected"]]==roster[:1]
    s=read(tmp_path/"underfilled","summary.json")
    assert s["datasets"]["off"]["methods"][METHODS[0]]["arms"]["top_k"]["mean"] is None
    import csv
    with (tmp_path/"underfilled/blocks.csv").open(newline="",encoding="utf-8") as stream:
        row=next(r for r in csv.DictReader(stream) if r["label"]=="off" and r["arm"]=="top_k")
    assert row["planned_cash_weight"]=="0.5" and row["missing_count"]=="1" and row["value"]==""


def test_real_two_month_saved_cells(history_family,tmp_path):
    from _filter_helpers import is_request
    from tools.strategy_lab.fixed.run import run
    from tools.strategy_lab.generate import generate_dataset
    f=history_family
    r=history_request(end="2024-10-31",evaluation="2024-09-01")
    r["calendar"]["periodMonths"]=2
    p=plan(tmp_path,f["tickers"],r,"two-fixed")
    write_history_fixed_request(p,f["work"]/"off-template.json",tmp_path/"transform",warmup_bars=100)
    run(tmp_path/"transform/fixed-request.json",f["pack"],tmp_path/"dataset")
    evaluate_history(p,tmp_path/"fixed-report",fixed_inputs={"off":tmp_path/"dataset"})
    s=read(tmp_path/"fixed-report","summary.json")
    assert s["expected_blocks"]==1 and s["excluded_history_blocks"]==4 and s["duration_month_counts"]=={"2":1}
    r=history_request(mode="is_oos",end="2024-12-31",evaluation="2024-11-01")
    r["calendar"].update(isMonths=2,oosMonths=2)
    p=plan(tmp_path,f["tickers"],r,"two-candidate")
    raw=is_request();g=raw["generation"]
    g["market_data"]["instruments"]=[dict(instrument_id=t,cell="research") for t in f["tickers"]]
    g["windows"].update(requested_start="2024-01-01",requested_end="2025-01-01",is_period_months=2,oos_period_months=2,expected_window_count=5)
    raw["preregistration"]["analysis_scopes"][0]["window_numbers"]=list(range(1,6))
    generate_dataset(document(tmp_path,"candidate-request.json",raw),data_root=f["pack"],output_dir=tmp_path/"candidate")
    d=open_dataset(tmp_path/"candidate")
    evaluate_history(p,tmp_path/"candidate-report",inputs=[DatasetInput("off",d,d.resolve_scope("research"))],
        history_scopes={"off":d.resolve_scope("research")},candidate_rule="primary_profit")
    s=read(tmp_path/"candidate-report","summary.json")
    assert s["expected_blocks"]==1 and s["excluded_history_blocks"]==4 and s["duration_month_counts"]=={"2":1}
