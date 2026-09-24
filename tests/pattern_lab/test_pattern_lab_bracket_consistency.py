"""Saved v1 invariants, bounded reader ownership and public contract composition."""
import copy
from dataclasses import replace
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from tools.pattern_lab import PatternLabDataError, study, manifest
from tools.pattern_lab.study import bracket, contracts, evidence, extensions, sequential, sequential_checks
from tools.pattern_lab.study.bracket_rules import normalize_rules
from . import _helpers as h
from ._bracket_helpers import RULES, build, evaluate, rule_entry

STEP = 1800000
FLAT = [100,101,99,100,10]


def test_reconciliation_refuses_overflow_in_recomputed_balance():
    # All saved scalar facts are finite; their chronological sum overflows.
    trade=dict(quantity=1.,entry_price=1.,entry_fee=0.,gross_pnl=1e308,exit_fee=0.)
    path=dict(balance=np.array([1e308]),equity=np.array([1e308]),close_price=np.array([100.]),
        position_direction=np.array([0]),position_quantity=np.array([0.]),
        entry_price=np.array([np.nan]),segment_end=np.array([True]))
    with pytest.raises(PatternLabDataError,match="balance/equity"):
        sequential_checks._reconcile(path,[trade],np.array([-1]),{0:trade},{0:trade},
            dict(initial_capital=1e308,direction="long"),"overflow account")


@pytest.mark.parametrize("capital,inside,outside", [(1.,9e-9,1.05e-8),(1000.,9e-7,1.005e-6)])
@pytest.mark.parametrize("column", ["balance","equity"])
def test_money_path_preserves_original_tolerance_boundary(capital, inside, outside, column):
    tables,kwargs=evaluate([FLAT]*3,signals=(),initial_capital=capital,return_context=True)
    for delta,accepted in ((inside,True),(outside,False)):
        rows=sequential.checked_table(tables["path"],"path").to_pylist()
        rows[0][column]+=delta
        changed={**tables,"path":sequential.frame("path",rows)}
        if accepted:
            sequential.validate(changed,**kwargs)
        else:
            with pytest.raises(PatternLabDataError,match="balance/equity"):
                sequential.validate(changed,**kwargs)


@pytest.mark.parametrize("reason,settings", [
    ("filled",{}), ("leverage_cap_exceeded",{"max_leverage":.1}),
    ("below_min_quantity",{"initial_capital":20}),
    ("zero_quantity",{"initial_capital":.01}),
    ("indicator_unavailable",{"atr_length":3}),
    ("nonpositive_capital",{}),
])
def test_stage_null_matrix_rejects_missing_or_fabricated_facts(reason, settings):
    if reason == "nonpositive_capital":
        tables, kwargs = evaluate([FLAT,FLAT,[1000,1001,999,1000,12],FLAT,FLAT],
            direction="short",signals=(0,3),return_context=True)
    else:
        rules=replace(RULES,minimum_lots=2) if reason=="below_min_quantity" else RULES
        tables,kwargs=evaluate([FLAT]*4,signals=(0,),rules=rules,return_context=True,**settings)
    rows=sequential.checked_table(tables["attempts"],"attempts").to_pylist()
    row=next(a for a in rows if a["reason"]==reason)
    if reason in ("indicator_unavailable","nonpositive_capital"):
        row["quantity"]=1.
    else:
        row["lots"]=None
    with pytest.raises(PatternLabDataError):
        sequential.validate({**tables,"attempts":sequential.frame("attempts",rows)},**kwargs)


@pytest.mark.parametrize("opening,minimum,cap,fee,expected", [
    (100,660,8,0,"filled"), (90,660,8,0,"below_min_notional"),
    (100,5,.66,0,"filled"), (100,5,.659,0,"leverage_cap_exceeded"),
    (100,5,3,0,"filled"), (100,5,8,99,"leverage_undefined"),
    (100,50000,8,99,"below_min_notional"),
])
def test_bybit_actual_open_minimum_and_admission_through_adapter(opening,minimum,cap,fee,expected):
    rules=replace(normalize_rules(rule_entry("BYBIT")),minimum_notional=str(minimum))
    tables=evaluate([FLAT,[opening,opening+1,opening-1,opening,11]],rules=rules,
        max_leverage=cap,commission_pct_per_side=fee, risk_pct=100 if fee else 2)
    a=tables["attempts"].iloc[0]
    assert a.reason==expected
    assert a.notional == opening*a.quantity
    assert len(tables["trades"])==int(expected=="filled")


def test_overflow_nullable_facts_and_constructed_ratio_overflow():
    rules=replace(RULES,base_step="1000000",base_minimum="1000000",quantity_step="1000000")
    values=[[1e300,1.01e300,.99e300,1e300,1],[1e302,1.01e302,.99e302,1e302,1]]
    tables=evaluate(values,rules=rules,initial_capital=1e308,risk_pct=1)
    a=sequential.checked_table(tables["attempts"],"attempts").to_pylist()[0]
    assert a["reason"]=="leverage_undefined" and a["notional"] is a["proposed_fee"] is None
    # Deliberately validator-only: keep arithmetic finite until division overflows.
    tables,kwargs=evaluate([FLAT,FLAT],max_leverage=.1,return_context=True)
    rows=sequential.checked_table(tables["attempts"],"attempts").to_pylist()
    row=rows[0]
    row.update(pre_entry_balance=1e-300,notional=1e100,proposed_fee=0.,required_leverage=None,reason="leverage_undefined")
    kwargs["instances"][0]["cases"]["30"][0]["parameters"]["initial_capital"]=1e-300
    path=sequential.checked_table(tables["path"],"path").to_pylist()
    for p in path: p.update(balance=1e-300,equity=1e-300)
    sequential.validate({**tables,"attempts":sequential.frame("attempts",rows),"path":sequential.frame("path",path)},**kwargs)


@pytest.mark.parametrize("mutation",["tl1","tl2","exact_cap","missing_plan","occupied_plan","missing_fill","false_minimum","anchor","exit_phase","exit_price","expiry"])
def test_resealed_entry_stage_and_exit_contradictions_preserve_derived(tmp_path,mutation):
    request=build(tmp_path/"pack",mixed=False)
    if mutation=="tl2": request["models"][0]["settings"]={"max_leverage":.01}
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    identifier="OKX_AAA-USDT-SWAP"
    result=study.load_results(root)
    reader=result.instrument_reader(identifier)
    tables=reader.sequential_tables(); reader.release()
    rows={name:sequential.checked_table(table,name).to_pylist() for name,table in tables.items()}
    a=next(a for a in rows["attempts"] if a["reason"]==("leverage_cap_exceeded" if mutation=="tl2" else "filled"))
    t=next((t for t in rows["trades"] if all(t[k]==a[k] for k in sequential.KEY) and t["trade_id"]==a["trade_id"]),None)
    if mutation=="tl1":
        a["notional"] /= 2; a["proposed_fee"]=a["notional"]*.0005
        a["required_leverage"]=a["notional"]/(a["pre_entry_balance"]-a["proposed_fee"])
        t["entry_leverage"]=a["required_leverage"]
    elif mutation=="tl2": a.update(reason="leverage_undefined",required_leverage=None)
    elif mutation=="exact_cap":
        # A display-sized perturbation was formerly tolerated even for exact decisions.
        a["required_leverage"]*=1-5e-10; t["entry_leverage"]=a["required_leverage"]
    elif mutation=="missing_plan": a["quantity"]=None
    elif mutation=="occupied_plan":
        occupied=next(row for row in rows["attempts"] if row["reason"]=="occupied")
        for key in sequential_checks.PLAN: occupied[key]=a[key]
    elif mutation=="missing_fill": a["fill_index"]=None
    elif mutation=="false_minimum": a.update(reason="below_min_notional",trade_id=None)
    elif mutation=="anchor": a["anchor_price"]+=.000000001
    elif mutation=="exit_phase": t["exit_phase"]="close" if t["exit_phase"]!="close" else "intrabar"
    elif mutation=="exit_price": t["exit_price"]+=1e-10
    elif mutation=="expiry": t["exit_reason"]="expiry"
    before={name:(root/name).read_bytes() for name in evidence.DERIVED_FILES}
    bundle_path=root/"jobs"/identifier/"bundle.json"
    bundle=evidence.read_json(bundle_path)
    for name,values in rows.items():
        path=root/"jobs"/identifier/f"sequential_{name}.parquet"
        evidence.write_table(path,sequential.frame(name,values),name="sequential_"+name)
        for item in bundle["files"]:
            if item["name"]==path.name: item["sha256"]=evidence.file_digest(path)
    evidence.write_json(bundle_path,bundle)
    reseal(root)
    with pytest.raises(PatternLabDataError): study.load_results(root)
    with pytest.raises(PatternLabDataError): study.regenerate_report(root)
    assert before=={name:(root/name).read_bytes() for name in before}


def reseal(root):
    seal=evidence.read_json(root/evidence.COMPLETION_FILE)
    seal["evidence_sha256"]={name:evidence.file_digest(root/name) for name in seal["evidence_sha256"]}
    seal["evidence_set_sha256"]=contracts.semantic_digest(seal["evidence_sha256"])
    evidence.write_json(root/evidence.COMPLETION_FILE,seal)


def test_exact_recomputed_ratio_above_cap_rejects_tolerance_sized_lie():
    tables,kwargs=evaluate([FLAT,FLAT],commission_pct_per_side=.05,return_context=True)
    a=sequential.checked_table(tables["attempts"],"attempts").to_pylist()[0]
    t=sequential.checked_table(tables["trades"],"trades").to_pylist()[0]
    true=a["required_leverage"]
    parameters=kwargs["instances"][0]["cases"]["30"][0]["parameters"]
    parameters["max_leverage"]=true
    sequential.validate(tables,**kwargs)  # true equality remains accepted
    parameters["max_leverage"]=true*(1-4e-10)
    a["required_leverage"]=t["entry_leverage"]=true*(1-5e-10)
    with pytest.raises(PatternLabDataError,match="admission"):
        sequential.validate({**tables,"attempts":sequential.frame("attempts",[a]),"trades":sequential.frame("trades",[t])},**kwargs)


@pytest.mark.parametrize("direction",["long","short"])
@pytest.mark.parametrize("length,days,reason,phase",[(5,2/48,"expiry","open"),(4,1/48,"expiry","open"),
    (3,1/48,"terminal","close"),(3,2/48,"terminal","close"),(4,np.nextafter(1/48,np.inf),"terminal","close")])
def test_expiry_threshold_and_final_open_clock(direction,length,days,reason,phase):
    tables=evaluate([FLAT]*length,direction=direction,max_holding_days=days)
    trade=tables["trades"].iloc[0]
    assert (trade.exit_reason,trade.exit_phase)==(reason,phase)


@pytest.mark.parametrize("direction",["long","short"])
@pytest.mark.parametrize("bar,reason,phase",[
    ([100,101,96,100,11],"stop","intrabar"),([100,104,99,100,11],"target","intrabar"),
    ([96,97,95,96,11],"stop","open"),([104,105,103,104,11],"target","open")])
def test_exit_attribution_in_both_directions(bar,reason,phase,direction):
    if direction=="short": bar=[200-bar[0],200-bar[2],200-bar[1],200-bar[3],bar[4]]
    tables,kwargs=evaluate([FLAT,bar],direction=direction,return_context=True)
    trades=sequential.checked_table(tables["trades"],"trades").to_pylist()
    assert (trades[0]["exit_reason"],trades[0]["exit_phase"])==(reason,phase)
    trades[0]["exit_reason"]="target" if reason=="stop" else "stop"
    with pytest.raises(PatternLabDataError):
        sequential.validate({**tables,"trades":sequential.frame("trades",trades)},**kwargs)


def test_bounded_reader_reuse_copies_and_per_account_work(tmp_path,monkeypatch):
    request=build(tmp_path/"pack")
    run=study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")
    result=study.load_results(run["run_root"])
    counts={name:0 for name in ("validate","_group_tables","_trade_intervals","_reconcile")}
    for name in counts:
        module=sequential if name=="validate" else sequential_checks
        original=getattr(module,name)
        def counted(*args,_name=name,_original=original,**kwargs):
            counts[_name]+=1; return _original(*args,**kwargs)
        monkeypatch.setattr(module,name,counted)
    reader=result.instrument_reader(result.completed_instruments[0])
    for key in reader.sequential_keys():
        selection=dict(zip(sequential.KEY[1:],key[1:]))
        account=reader.sequential_account(**selection)
        account["path"].loc[0,"balance"]=-123.
        assert reader.sequential_account(**selection)["path"].iloc[0].balance!=-123.
        assert reader.sequential_coverage(**selection)["tail_complete"]
    assert counts==dict(validate=1,_group_tables=1,_trade_intervals=12,_reconcile=12)
    reader.release()
    assert not reader._tables and not hasattr(reader,"_sequential_grouped")
    study.load_results(run["run_root"])
    assert counts["validate"]==3


def small_pack(root,values,*,end=None,drop=(),settings=None,occurrence="every_qualifying_bar",context_drop=None):
    stamps,bars=h.timeframe_bars(30,values,drop_groups=drop)
    rules=rule_entry()
    sources=[replace(h.instrument_source(stamps,bars,venue="OKX",contract=rules["contract"]),instrument_rules=rules["instrument_rules"])]
    if context_drop is not None:
        context_values=[(100+i,102+i,99+i,101+i,100+i) for i in range(len(values))]
        cs,cv=h.timeframe_bars(30,context_values,drop_groups=context_drop)
        sources.append(h.instrument_source(cs,cv,venue="TEST",symbol="BTC",contract="BTC-USDT-SWAP",roles=["factor"]))
    h.publish(root,sources)
    end=end or len(values)
    parameters=dict(atr_length=1,swing_lookback=1,atr_multiplier=1,directions=["long"],reward_risks=[1])
    parameters.update(settings or {})
    request=h.study_request(protocol=h.study_protocol(first_ms=h.ANCHOR_MS,coverage_end_ms=h.ANCHOR_MS+len(values)*STEP),
        start_ms=h.ANCHOR_MS+2*STEP,end_ms=h.ANCHOR_MS+end*STEP,warmup_ms=h.ANCHOR_MS,timeframes=[30],
        hypotheses=[dict(id="v",hypothesis="two_green_volume_btc" if context_drop is not None else "two_green_rising_quote_volume",
            parameters={"alias":"btc"} if context_drop is not None else {},occurrence=occurrence)],
        models=[dict(id="br",model="atr_bracket",settings=parameters)])
    request.update(schema_version=2,context={"btc":{"ids":["TEST_BTC-USDT-SWAP"]}} if context_drop is not None else {},execution={"kind":"development"})
    return request


@pytest.mark.parametrize("missing",[0,1,2])
def test_missing_tail_pipeline_coverage_and_immutable_raw(tmp_path,missing):
    values=[(99,101,98,100,100+i) for i in range(10)]
    request=small_pack(tmp_path/"pack",values,end=8,drop=tuple(range(8-missing,9)),settings={"max_holding_days":2.5})
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    before={p:evidence.file_digest(p) for p in (root/"jobs").rglob('*') if p.is_file()}
    results=study.load_results(root); reader=results.instrument_reader(results.completed_instruments[0])
    selection=dict(timeframe_minutes=30,variant_id="v",model_instance_id="br",case_id="long_rr1")
    coverage=reader.sequential_coverage(**selection)
    assert coverage["missing_tail_slots"]==missing and coverage["tail_complete"]==(missing==0)
    assert coverage["terminal_exits_before_requested_end"]==int(missing>0)
    assert reader.sequential_account(**selection)["trades"].iloc[-1].exit_reason=="terminal"
    assert study.summarize_results(results)["sequential_accounts"][0]["coverage"]==coverage
    reader.release(); study.regenerate_report(root)
    assert before=={p:evidence.file_digest(p) for p in before}
    html=(root/evidence.REPORT_FILE).read_text(encoding="utf-8")
    assert ("Missing tail:" in html)==(missing>0) and "2.5" in html


def test_nonpositive_denominator_full_pipeline(tmp_path):
    request=small_pack(tmp_path/"pack",[(99,101,98,100,100+i) for i in range(8)],
        settings={"risk_pct":100,"commission_pct_per_side":99,"atr_multiplier":.001})
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    result=study.load_results(root)
    attempts=result.table(result.completed_instruments[0],"sequential_attempts")
    assert set(attempts.reason)=={"leverage_undefined"}
    assert (attempts.pre_entry_balance-attempts.proposed_fee<0).all()
    study.regenerate_report(root)


def test_state_entry_transitions_execute_without_exit_reemission(tmp_path):
    values=[(100,101,98,99,100),(99,101,98,100,101),(99,101,98,100,102),
            (100,106,99,105,103),(104,106,103,105,104),(106,107,104,105,105),
            (104,106,103,105,106),(104,106,103,105,107),(105,120,104,119,108),
            (118,120,117,119,109)]
    request=small_pack(tmp_path/"pack",values,occurrence="state_entry")
    root=study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"]
    result=study.load_results(root); identifier=result.completed_instruments[0]
    attempts=result.table(identifier,"sequential_attempts")
    assert attempts.signal_index.tolist()==[2,7] and attempts.reason.tolist()==["filled","filled"]
    trades=result.table(identifier,"sequential_trades")
    assert trades.entry_index.tolist()==[3,8] and trades.exit_index.tolist()==[3,8]


def test_context_missingness_does_not_stop_price_execution(tmp_path):
    values=[(99,101,98,100,100+i) for i in range(10)]
    values[5]=(90,92,89,91,105)
    request=small_pack(tmp_path/"pack",values,context_drop=(4,5),settings={"reward_risks":[3]})
    root=study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"]
    result=study.load_results(root); identifier=result.completed_instruments[0]
    attempts=result.table(identifier,"sequential_attempts")
    assert not set(attempts.signal_index)&{4,5}
    trade=result.table(identifier,"sequential_trades").iloc[0]
    assert (trade.entry_index,trade.exit_index,trade.exit_reason)==(3,5,"stop")
    assert set(result.table(identifier,"sequential_path").segment)=={0}


def test_production_source_and_rule_identity_composition(tmp_path,monkeypatch):
    request=build(tmp_path/"pack",mixed=False)
    def run(name): return study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/name)["identities"]
    original=run("original")
    hashes=extensions.bracket_source_digests()
    assert set(hashes)=={f"src/core/engine_v2/{name}.py" for name in ("kernel","sizing","contracts","price_rounding","execution_modes","diagnostics")} | {"src/core/backtest_engine.py"} | {f"tools/pattern_lab/study/{name}.py" for name in ("bracket","bracket_rules","sequential","sequential_checks")}
    for index,path in enumerate(hashes):
        with monkeypatch.context() as patch:
            patch.setattr(extensions,"bracket_source_digests",lambda path=path:{**hashes,path:"0"*64})
            changed=run(f"source-{index}")
        assert changed["implementation_sha256"]!=original["implementation_sha256"]
        assert changed["data_input_sha256"]==original["data_input_sha256"]
    document=manifest.read_manifest(tmp_path/"pack")
    rules=next(e for e in document["instruments"] if e["venue"]=="OKX")["instrument_rules"]
    rules.update(as_of_utc="2026-09-25T00:00:00Z",quantity_step="1.00")
    manifest.write_manifest(tmp_path/"pack",document)
    assert run("spelling")["data_input_sha256"]==original["data_input_sha256"]
    rules["raw_contract_fields"]["ctVal"]="0.2"
    manifest.write_manifest(tmp_path/"pack",document)
    assert run("rule")["data_input_sha256"]!=original["data_input_sha256"]


def test_old_custom_atr_bracket_study_and_candidate_remain_offline_readable(tmp_path,monkeypatch):
    from tools.pattern_lab import analysis, candidate
    request=build(tmp_path/"pack")
    request["models"]=request["models"][1:]+[dict(id="custom",model="example_next_open_gap",settings={})]
    request["extensions"]=[dict(module="custom_extension",source_root=str(h.REPO_ROOT/"tools/pattern_lab/examples"),helpers=[])]
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    # Consistent historical saved shape; do not register the newly reserved name.
    for name in (evidence.REQUEST_FILE,evidence.FAMILY_FILE):
        document=evidence.read_json(root/name)
        for model in document["models"]:
            if model["model_instance_id"]=="custom": model["model_id"]="atr_bracket"
        for group in document.get("groups",[]):
            if group.get("model_instance_id")=="custom": group["model_id"]="atr_bracket"
        evidence.write_json(root/name,document)
    reseal(root)
    result=study.load_results(root)
    assert not contracts.is_sequential(result.model_instance("custom"))
    study.regenerate_report(root)
    analysis.run_analysis(request={"schema_version":2,"analysis_name":"Historical custom companion", "model_instances":["fh"],"pairwise":[]},
        run_root=root,output_root=tmp_path/"analysis")
    frozen=candidate.freeze_candidate(study_root=root,analysis_root=tmp_path/"analysis",
        start=request["protocol"]["reserved"]["start_utc"],end=request["protocol"]["reserved"]["end_utc"],
        warmup_start=request["study"]["warmup_start_utc"],output=tmp_path/"candidate.json")
    def forbidden(*args,**kwargs): pytest.fail("offline reader attempted source execution")
    monkeypatch.setattr(extensions,"load_extensions",forbidden)
    monkeypatch.setattr(bracket,"reference_core",forbidden)
    assert candidate.load_candidate(tmp_path/"candidate.json")==frozen
    moved=tmp_path/"moved"; shutil.copytree(root,moved)
    study.regenerate_report(moved)
    # Reservation is explicit even in an otherwise empty model registry.
    monkeypatch.setitem(contracts._REGISTRY,"model",{})
    ordinary=contracts.ModelDescriptor("atr_bracket","1",lambda a,b:a,lambda a,b:(),lambda *a:None)
    with pytest.raises(PatternLabDataError,match="owned built-in"):
        contracts.register_model(ordinary,builtin=True)


@pytest.mark.parametrize("mutation",["missing_all","extra","relabel","missing_custom"])
def test_strict_table_family_coverage_rejects_resealed_artifacts(tmp_path,mutation):
    request=build(tmp_path/"pack",mixed=False)
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    identifier="OKX_AAA-USDT-SWAP"; bundle_path=root/"jobs"/identifier/"bundle.json"
    bundle=evidence.read_json(bundle_path)
    if mutation=="missing_all":
        bundle["files"]=[item for item in bundle["files"] if not item["table"].startswith("sequential_")]
    elif mutation=="extra":
        item=copy.deepcopy(bundle["files"][-1]);item["table"]="sequential_unknown";bundle["files"].append(item)
    else:
        for name in (evidence.REQUEST_FILE,evidence.FAMILY_FILE):
            document=evidence.read_json(root/name)
            document["models"][0]["evidence_kind"]=contracts.CUSTOM_CASE_EVIDENCE_KIND
            evidence.write_json(root/name,document)
        if mutation=="missing_custom":
            bundle["files"]=[item for item in bundle["files"] if not item["table"].startswith("sequential_")]
    evidence.write_json(bundle_path,bundle); reseal(root)
    before=(root/evidence.REPORT_FILE).read_bytes()
    with pytest.raises(PatternLabDataError): study.load_results(root)
    with pytest.raises(PatternLabDataError): study.regenerate_report(root)
    assert (root/evidence.REPORT_FILE).read_bytes()==before


def test_report_comparisons_escape_text_and_show_coverage_status(tmp_path):
    from tools.pattern_lab.study import report
    request=build(tmp_path/"pack",mixed=False)
    request["models"][0]["settings"]={"max_holding_days":2.5,"max_leverage":.01}
    root=study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"]
    summary=study.summarize_results(study.load_results(root))
    a=summary["sequential_accounts"][0]
    a["variant_id"]="<script>bad & text</script>"
    a["coverage"].update(tail_complete=False,missing_tail_slots=2,terminal_exits_before_requested_end=1)
    html=report.render_report(summary)
    for label in ("Win rate %","Mean net R","Finite-cap attempts","Max required x","Mean / median holding hours",
                  "no_wins_or_losses","Missing tail: 2","2.5","Rule snapshot provenance","UTC"):
        assert label in html
    assert "&lt;script&gt;bad &amp; text&lt;/script&gt;" in html
    assert "<script>bad" not in html and "Four days is" not in html and "{'" not in html.split("Sequential ATR bracket accounts")[1]
    del a["coverage"]
    assert "coverage unavailable" in report.render_report(summary)


def test_coordinator_rejects_bad_entry_before_any_job_publication(tmp_path):
    request=build(tmp_path/"pack",mixed=False)
    root=Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    result=study.load_results(root); identifier=result.completed_instruments[0]
    tables={item["table"]:result.table(identifier,item["table"]) for item in result.jobs[identifier]["files"]}
    rows=sequential.checked_table(tables["sequential_attempts"],"attempts").to_pylist()
    next(a for a in rows if a["reason"]=="filled")["notional"]*=.5
    tables["sequential_attempts"]=sequential.frame("attempts",rows)
    target=tmp_path/"publication"
    for directory in ("spec","admitted"): shutil.copytree(root/directory,target/directory)
    with pytest.raises(PatternLabDataError,match="proposed fee"):
        evidence.publish_job(target,identifier,tables=tables,stats=result.jobs[identifier]["stats"])
    assert not (target/"jobs").exists()


def test_direct_two_of_three_tail_and_too_short_pack_admission(tmp_path):
    tables,kwargs=evaluate([FLAT,FLAT],return_context=True,end_ms=h.ANCHOR_MS+3*STEP)
    grouped=sequential.validate(tables,**kwargs); key=next(iter(grouped["path"]))
    assert sequential.coverage(grouped["path"][key],grouped["trades"][key],30,h.ANCHOR_MS+3*STEP)==dict(
        requested_end_ms=h.ANCHOR_MS+3*STEP,last_observed_close_ms=h.ANCHOR_MS+2*STEP,
        tail_complete=False,missing_tail_slots=1,terminal_exits_before_requested_end=1)
    with pytest.raises(PatternLabDataError,match="impossible"):
        sequential.coverage(grouped["path"][key],grouped["trades"][key],30,h.ANCHOR_MS+STEP)
    request=small_pack(tmp_path/"pack",[(99,101,98,100,100+i) for i in range(10)],drop=(8,9))
    with pytest.raises(PatternLabDataError,match="coverage"):
        study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"bad")
    assert not (tmp_path/"bad").exists()
