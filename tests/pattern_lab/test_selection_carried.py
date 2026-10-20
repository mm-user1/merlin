"""Independent capital arithmetic and native compact execution integration."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import math
import os
import shutil
import subprocess
import sys
import numpy as np
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.examples.selection_synthetic import build_inputs
from tools.pattern_lab.examples.selection_carried_synthetic import build_snapshots,request
from tools.pattern_lab.examples.selection_auxiliary_scorer import SCORER
from tools.pattern_lab.account_study import run_account_study
from tools.pattern_lab.account_study.common import read_json,write_json,file_hash
from tools.pattern_lab.selection import run_selection,load_selection,verify_selection,rebuild_selection_report,export_selection_bundle,import_selection_bundle
from tools.pattern_lab.selection.carried import carried_calculation,settle
from tools.pattern_lab.selection.funded import execution_key,FundedExecutor
from tools.pattern_lab.selection.ledger import REJECTIONS
from tools.pattern_lab.selection.request import month_bounds
from tools.pattern_lab.selection.artifacts import seal_directory
from tools.pattern_lab.selection.storage_v2 import read_parts,write_parts,PORTFOLIOS,FUNDED


@pytest.fixture(scope='module')
def example(tmp_path_factory):
    root=tmp_path_factory.mktemp('carried');raw=build_inputs(root/'pack')
    parent=run_account_study(raw,data_root=root/'pack',output_root=root/'parent')
    snapshot,bindings=build_snapshots(parent,root/'snapshot')
    readers={mode:run_selection(request(parent,bindings,mode),parent_root=parent.root,output_root=root/mode,
        scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root}) for mode in ('fixedReference','carried')}
    return root,parent,snapshot,bindings,readers


def test_native_lot_floor_compact_retention_and_control_facts(example):
    root,parent,snapshot,bindings,readers=example;reader=readers['carried']
    funded=[r for rows in reader._funded.values() for r in rows]
    assert len(funded)>30 and sum(r['trades'] for r in funded)>100 and sum(r['fees'] for r in funded)>0
    shadow={r['accountKey']:r for r in reader.ledger}
    assert any(abs((r['finalBalance']-r['capital'])-shadow[r['accountKey']]['netPnl']*r['capital']/10000)>1e-5 for r in funded)
    assert any(r['pathRef'] is None for r in funded) and any(r['pathRef'] is not None for r in funded)
    for month,rows in reader._funded.items():
        assert set(reader._funded_paths[month])=={r['executionKey'] for r in rows if r['pathRef'] is not None}
    assert reader.scores==readers['fixedReference'].scores
    for c in reader.results['comparisons']:
        assert c['months'][0]['selected']['openingCapital']==c['months'][0]['allEligible']['openingCapital']==10000
        assert c['months'][1]['selected']['openingCapital']==c['months'][0]['selected']['closingCapital']
        if c['k']==8:assert c['months'][0]['selected']['idleSlots']==2 and c['months'][0]['selected']['cash']==2500
    assert any(len({tuple(m['selected']['members']) for m in c['months']})>1 for c in reader.results['comparisons'])
    assert all('random' not in m for c in reader.results['comparisons'] for m in c['months'])


def test_one_six_economic_bytes_and_changed_t0(example,tmp_path):
    _,parent,snapshot,bindings,readers=example
    six=run_selection(request(parent,bindings,'carried'),parent_root=parent.root,output_root=tmp_path/'six',workers=6,
        scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    one=readers['carried'];a=read_json(one.root/'completion.json')['members'];b=read_json(six.root/'completion.json')['members']
    assert {k:v for k,v in a.items() if k!='provenance.json'}=={k:v for k,v in b.items() if k!='provenance.json'}
    doc=request(parent,bindings,'carried');doc['initialCapital']=17.
    small=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'small',scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    assert one.scores==small.scores
    for a,b in zip(one.results['comparisons'],small.results['comparisons']):
        assert [m['selected']['members'] for m in a['months']]==[m['selected']['members'] for m in b['months']]
    assert sum(r['trades'] for rows in small._funded.values() for r in rows)<sum(r['trades'] for rows in one._funded.values() for r in rows)
    assert sum(sum(r['sizingRejections'].values()) for rows in small._funded.values() for r in rows)>0


def test_offline_zip_rebuild_and_optional_required_replay(example,tmp_path,monkeypatch):
    _,parent,snapshot,bindings,readers=example;reader=readers['carried']
    deep=verify_selection(reader.root,parent_root=parent.root)
    assert deep['scorers']=={'consistency':'recomputed','oi':'storedOutputsOnly'} and not deep['auxiliaryRecomputed']
    with pytest.raises(PatternLabDataError,match='dependencies missing'):verify_selection(reader.root,parent_root=parent.root,require_scorers=True)
    checked=verify_selection(reader.root,parent_root=parent.root,scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root},require_scorers=True)
    assert checked['fundedExecutions']==sum(map(len,reader._funded.values())) and checked['auxiliaryRecomputed']
    def denied(*a,**k):raise AssertionError('offline operation reached external execution/dependency')
    import tools.pattern_lab.selection.funded as funded
    import tools.pattern_lab.selection.artifacts_v2 as artifacts
    import tools.pattern_lab.auxiliary.storage as snapshots
    import socket
    for mod,name in ((funded,'execute_account'),(artifacts,'admit_parent'),(artifacts,'admit_snapshots'),(snapshots,'load_snapshot'),(socket,'socket')):monkeypatch.setattr(mod,name,denied)
    for mode,reader in readers.items():
        assert load_selection(reader.root).results==reader.results
        for i in (1,2):rebuild_selection_report(reader.root,output_root=tmp_path/(mode+str(i)))
        for name in ('summary.json','report.html'):assert (tmp_path/(mode+'1')/'derived'/name).read_bytes()==(tmp_path/(mode+'2')/'derived'/name).read_bytes()
        archive=tmp_path/(mode+'.zip');export_selection_bundle(reader.root,zip_path=archive)
        assert import_selection_bundle(archive,output_root=tmp_path/(mode+'-import')).results==reader.results


@pytest.mark.parametrize('kind',['chain','membership','link','path','contract','dependency'])
def test_resealed_tampering_refused(example,tmp_path,kind):
    reader=example[-1]['carried'];root=tmp_path/'forged';shutil.copytree(reader.root,root)
    month=reader.selection['request']['evaluationMonths'][0];parts=read_json(root/'parts.json')
    if kind in ('chain','membership'):
        names=parts[month]['portfolios'];rows=read_parts(root,names,PORTFOLIOS)
        if kind=='chain':rows[0]['closingCapital']+=.01
        else:rows[0]['members']=[]
        write_parts(root,'portfolios',month,rows,PORTFOLIOS)
    elif kind=='link':
        names=parts[month]['funded'];rows=read_parts(root,names,FUNDED);rows[0]['commandId']='0'*64;write_parts(root,'funded',month,rows,FUNDED)
    elif kind=='path':
        p=root/parts[month]['paths'][0]
        with np.load(p,allow_pickle=False) as z:arrays={k:z[k] for k in z.files}
        key=next(k for k in arrays if k!='times');arrays[key][-1]+=1
        with p.open('wb') as stream:np.savez_compressed(stream,**arrays)
    else:
        scores=read_json(root/'scores.json');batch=next(b for b in scores if b['contract']['auxiliaryUses'])
        if kind=='contract':batch['contract']['auxiliaryUses'][0]['antecedentMs']+=1
        else:batch['auxiliaryDependencies'][0]['source']['unit']='wrong'
        write_json(root/'scores.json',scores)
    seal_directory(root,2)
    with pytest.raises(PatternLabDataError):load_selection(root)


def test_exact_replay_saved_capitals_and_first_field(example,monkeypatch):
    reader=example[-1]['carried'];parent=example[1];calls=[]
    original=FundedExecutor.run
    def altered(self,root,needs):
        calls.append(deepcopy(needs));rows,paths=original(self,root,needs)
        if rows:rows[0]['finalBalance']=np.nextafter(rows[0]['finalBalance'],np.inf).item()
        return rows,paths
    monkeypatch.setattr(FundedExecutor,'run',altered)
    with pytest.raises(PatternLabDataError,match='funded replay mismatch .*first field .finalBalance'):verify_selection(reader.root,parent_root=parent.root)
    first=reader.selection['request']['evaluationMonths'][0]
    assert {(key,c) for key,v in calls[0].items() for c in v}=={(r['accountKey'],r['capital']) for r in reader._funded[first]}
    assert len(calls)==1


def test_coherent_random_scalar_forgery_requires_exact_parent_replay(example,tmp_path):
    from tools.pattern_lab.selection.artifacts_v2 import SelectionV2
    from tools.pattern_lab.selection.artifacts import write_derived
    original=example[-1]['carried'];root=tmp_path/'forged';shutil.copytree(original.root,root)
    facts=deepcopy(original._funded);month=original.selection['request']['evaluationMonths'][-1]
    row=next(r for r in facts[month] if r['pathRef'] is None);row['finalBalance']+=.01
    results,_,portfolios,_,curves=carried_calculation(original.selection['request'],original.ledger,original.selection['roster'],original.scores,
        lambda m,needs:(facts[m],original._funded_paths[m]))
    write_parts(root,'funded',month,facts[month],FUNDED);write_parts(root,'portfolios',month,portfolios[month],PORTFOLIOS)
    write_json(root/'results.json',results)
    forged=SelectionV2(root,original.selection,original.ledger,original.scores,results,original._paths,curves,
        funded=facts,portfolios=portfolios,funded_paths=original._funded_paths,links=original._links)
    write_derived(forged,root);seal_directory(root,2)
    assert load_selection(root).results==results
    with pytest.raises(PatternLabDataError,match='funded replay mismatch '+row['executionKey']):verify_selection(root,parent_root=example[1].root)


def hand_case():
    months=['2020-05','2020-06','2020-07'];doc=dict(selectors=[dict(selectorId='s')],kValues=[1,3],initialCapital=100.,
        randomControl=dict(seed=7,replicates=4),evaluationMonths=months,pool=dict(minObservedBarsPct=99.))
    ledger=[];scores=[]
    for month in months:
        start,end=month_bounds(month)
        for ticker in ('A','B'):ledger.append(dict(month=month,instrumentId=ticker,accountKey=month+ticker,available=True,
            observedBars=1,expectedBars=1,terminalStatus='closed',stepMs=86400000))
        scores.append(dict(selectorId='s',month=month,commonExclusions=[],rows=[dict(instrumentId=i,eligible=True,key=[2-j]) for j,i in enumerate(('A','B'))]))
    return doc,ledger,scores


def synthetic_provider(ledger,returns,low=None):
    def provide(month,needs):
        rows=[];paths={};start,end=month_bounds(month);times=np.arange(start+86400000,end+1,86400000,dtype=np.int64)
        for key,capitals in sorted(needs.items()):
            ticker=key[-1]
            for capital,retain in sorted(capitals.items()):
                final=capital*(1+returns[month][ticker]);identity=execution_key(key,capital)
                rows.append(dict(executionKey=identity,accountKey=key,instrumentId=ticker,capital=capital,finalBalance=final,
                    fees=0.,trades=1,sizingRejections=dict.fromkeys(REJECTIONS,0),forcedExits={r:dict(count=0,netPnl=0.) for r in ('terminal','gapBoundary')},pathRef=identity if retain else None))
                if retain:
                    equity=np.linspace(capital,final,len(times));equity[1]=capital*(low if low is not None else 1)
                    paths[identity]=dict(times=times,equity=equity)
        return rows,paths
    return provide


def test_independent_gain_loss_changing_pool_underfill_empty_and_drawdown():
    doc,ledger,scores=hand_case();returns={m:dict(A=a,B=b) for m,a,b in zip(doc['evaluationMonths'],(.1,-.2,.5),(.2,.1,-.1))}
    scores[1]['rows']=scores[1]['rows'][:1];scores[2]['rows']=[]
    results,funded,rows,paths,curves=carried_calculation(doc,ledger,['A','B'],scores,synthetic_provider(ledger,returns,low=.5))
    one,three=results['comparisons'];assert one['months'][0]['selected']['closingCapital']==pytest.approx(110)
    assert one['months'][1]['selected']['openingCapital']==one['months'][0]['selected']['closingCapital']
    assert one['months'][1]['selected']['closingCapital']==pytest.approx(88)
    assert one['months'][2]['selected']['closingCapital']==one['months'][1]['selected']['closingCapital']
    assert one['months'][0]['allEligible']['openingCapital']==100 and one['months'][0]['allEligible']['closingCapital']==pytest.approx(115)
    assert three['months'][0]['selected']['idleSlots']==1 and three['months'][0]['selected']['cash']==100-math.fsum([100/3]*2)
    assert one['fullPeriod']['selected']['maxDrawdownPct']>=50
    assert one['months'][2]['allEligible']['netPnl']==0 and one['months'][2]['allEligible']['plannedSlots']==0


def test_missing_unselected_pool_member_stops_all_k_controls_other_selector_continues():
    doc,ledger,scores=hand_case();doc['selectors'].append(dict(selectorId='other'))
    scores += [dict(deepcopy(b),selectorId='other',rows=b['rows'][:1]) for b in scores.copy()]
    next(r for r in ledger if r['month']=='2020-06' and r['instrumentId']=='B').update(available=False,observedBars=0)
    returns={m:dict(A=.1,B=.2) for m in doc['evaluationMonths']}
    results,_,rows,_,_=carried_calculation(doc,ledger,['A','B'],scores,synthetic_provider(ledger,returns))
    for c in results['comparisons'][:2]:
        assert not c['fullPeriodAvailable'] and c['commonPrefix']['months']==['2020-05']
        assert c['months'][1]['selected']['status']=='unavailable' and c['months'][2]['selected']['status']=='priorCapitalUnknown'
        assert c['months'][2]['selected']['members'] and c['fullPeriod']['selected']['totalReturnPct'] is None
    assert all(not r['slots'] and r['closingCapital'] is None for m in doc['evaluationMonths'][1:] for r in rows[m] if r['selectorId']=='s')
    assert all(c['fullPeriodAvailable'] for c in results['comparisons'][2:])


def test_depletion_signed_idle_then_missing_mask_wins():
    doc,ledger,scores=hand_case();returns={m:dict(A=-1.2,B=-1.2) for m in doc['evaluationMonths']}
    doc['kValues']=[1,2]
    next(r for r in ledger if r['month']=='2020-07' and r['instrumentId']=='B').update(available=False,observedBars=0)
    _,funded,rows,_,_=carried_calculation(doc,ledger,['A','B'],scores,synthetic_provider(ledger,returns))
    top=next(r for r in rows['2020-05'] if r['portfolio']=='selected' and r['k']==1)
    assert top['closingCapital']==pytest.approx(-20) and top['status']=='depleted'
    assert not funded['2020-06'] and not funded['2020-07']
    for r in rows['2020-06']:
        if r['openingCapital']<=0:assert r['closingCapital']==r['openingCapital'] and r['netPnl']==0 and r['returnPct'] is None
    assert all(r['status']=='unavailable' for r in rows['2020-07'])


def test_negative_residual_and_ulp_authoritative_boundary():
    # A total whose split into three binary floats sums one ULP above it.
    opening=.1;n=11;capital=opening/n
    assert math.fsum([capital]*n)>opening
    rows={str(i):dict(capital=capital,finalBalance=capital,fees=0.,trades=0,sizingRejections=dict.fromkeys(REJECTIONS,0),
        forcedExits={r:dict(count=0,netPnl=0.) for r in ('terminal','gapBoundary')}) for i in range(n)}
    row=dict(status='available',openingCapital=opening,slotCapital=capital,slots=[dict(instrumentId=str(i),executionKey=str(i)) for i in range(n)],portfolio='random')
    settle(row,rows,{},np.array([1],np.int64));assert row['cash']<0 and row['closingCapital']==opening
    rows['0']['finalBalance']=np.nextafter(capital,np.inf).item()
    authoritative=opening+math.fsum(r['finalBalance']-capital for r in rows.values())
    settle(row,rows,{},np.array([1],np.int64));assert row['closingCapital']==authoritative
    opening=.1;capital=opening/3;final=.033666666666666664
    rows={str(i):dict(rows['0'],capital=capital,finalBalance=final) for i in range(3)}
    row.update(openingCapital=opening,slotCapital=capital,slots=[dict(instrumentId=str(i),executionKey=str(i)) for i in range(3)])
    settle(row,rows,{},np.array([1],np.int64))
    assert row['closingCapital']==.101 and row['cash']+math.fsum([final]*3)!=row['closingCapital']


def test_capacity_refusal_before_executor(example,tmp_path,monkeypatch):
    _,parent,snapshot,bindings,_=example;doc=request(parent,bindings,'carried');doc['randomControl']['replicates']=10**8
    def denied(*a,**k):raise AssertionError('funded execution before capacity')
    monkeypatch.setattr(FundedExecutor,'__enter__',denied)
    with pytest.raises(PatternLabDataError,match='capacity budget'):run_selection(doc,parent_root=parent.root,output_root=tmp_path/'no',scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    assert not (tmp_path/'no').exists()


def test_signal_managed_uses_native_executor(tmp_path):
    raw=build_inputs(tmp_path/'pack');raw['instruments']=raw['instruments'][:1];raw['windows']=raw['windows'][-1:]
    raw['allocations'][0]['policy'].update(mode='signalManaged',rewardRisk=None,stopUpdates=True)
    parent=run_account_study(raw,data_root=tmp_path/'pack',output_root=tmp_path/'parent')
    key=parent.accountKeys[0]
    with FundedExecutor(1) as executor:rows,paths=executor.run(parent.root,{key:{1234.:True,17.:False}})
    assert rows[-1]['trades']>0 and rows[-1]['fees']>0
    assert rows[0]['trades']==0 and sum(rows[0]['sizingRejections'].values())>0
    from tools.pattern_lab.accounts import execute_account
    original=parent.account(key)
    for r in rows:
        result=execute_account(original.prepared,original.result.commands,replace(original.result.policy,initialCapital=r['capital']),original.result.window,original.result.rules)
        assert r['accountId']==result.identity and r['finalBalance']==result.finalBalance


def test_generated_v2_node_report(example):
    node=shutil.which('node')
    if node is None:pytest.skip('Node unavailable; generated v2 HTML gate pending on Linux')
    for reader in example[-1].values():subprocess.run([node,str(Path(__file__).with_name('selection_carried_report.cjs')),str(reader.root/'derived/report.html')],check=True)
