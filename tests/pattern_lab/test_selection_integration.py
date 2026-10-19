"""Real monthly native accounts and offline selection evidence."""
from copy import deepcopy
from pathlib import Path
import json
import os
import shutil
import subprocess
import sys
import zipfile
import pytest

from tools.pattern_lab.account_study import run_account_study
from tools.pattern_lab.examples.selection_synthetic import build_inputs, selection_request
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.selection import (run_selection, verify_selection, rebuild_selection_report,load_selection,
    export_selection_bundle,import_selection_bundle,Scorer,Score)
from tools.pattern_lab.account_study.common import read_json,write_json,file_hash
from tools.pattern_lab.selection.artifacts import seal_directory
from tools.pattern_lab.selection.ledger import extract_ledger,score_view,REJECTIONS
from tools.pattern_lab.selection.engine import build_scores
from tools.pattern_lab.selection.scorers import registry
from ._selection_helpers import assert_fixture


@pytest.fixture(scope='module')
def monthly_parent(tmp_path_factory):
    root=tmp_path_factory.mktemp('monthly-parent')
    raw=build_inputs(root/'pack')
    parent=run_account_study(raw,data_root=root/'pack',output_root=root/'parent')
    return parent


def test_real_monthly_selection(monthly_parent,tmp_path):
    request=selection_request(monthly_parent)
    reader=run_selection(request,parent_root=monthly_parent.root,output_root=tmp_path/'selection')
    assert assert_fixture(reader)['trades']>0
    assert verify_selection(reader.root,parent_root=monthly_parent.root)['status']=='verified'
    for name in ('one','two'):rebuild_selection_report(reader.root,output_root=tmp_path/name)
    for name in ('summary.json','report.html'):
        assert (tmp_path/'one/derived'/name).read_bytes()==(tmp_path/'two/derived'/name).read_bytes()


@pytest.mark.parametrize('case',['capital','parent','partial','window','overlap','missing','unknown'])
def test_admission_before_output(monthly_parent,tmp_path,case):
    request=selection_request(monthly_parent);parent=monthly_parent.root;output=tmp_path/'output'
    if case=='capital':request['referenceCapital']=1.
    if case=='parent':request['parent']['completionSha256']='1'*64
    if case=='partial':parent=tmp_path/'partial';parent.mkdir();write_json(parent/'status.json',dict(state='failed'))
    if case=='window':request['monthWindows'][0]['windowId']='absent'
    if case=='overlap':output=parent/'nested-selection'
    if case=='missing':parent=tmp_path/'missing'
    if case=='unknown':request['extra']='refuse'
    with pytest.raises(PatternLabDataError):run_selection(request,parent_root=parent,output_root=output)
    assert not output.exists()


def test_native_ledger_boundary_and_terminal_accounting(monthly_parent):
    doc=selection_request(monthly_parent);ledger,paths,roster=extract_ledger(monthly_parent,doc)
    forced=0
    for row in ledger:
        record=monthly_parent.account(row['accountKey']);r=record.result
        assert row['netPnl']==r.finalBalance-r.policy.initialCapital
        assert row['returnPct']==(100.0*(r.finalBalance-r.policy.initialCapital))/r.policy.initialCapital
        trades=r.trades.to_pylist()
        assert row['fees']==pytest.approx(sum(t['entryFee']+t['exitFee'] for t in trades))
        assert all(t['entryMs']>=row['startMs']+row['stepMs'] for t in trades)
        assert r.policy.initialCapital==10000.
        forced+=row['forcedExits']['terminal']['count']
        assert row['sizingRejectionCount']==sum(row['sizingRejections'][name] for name in REJECTIONS)
    assert forced>0


def test_prefix_and_future_evidence_do_not_change_scores(monthly_parent):
    doc=selection_request(monthly_parent);ledger,paths,roster=extract_ledger(monthly_parent,doc)
    scores=build_scores(monthly_parent,doc,ledger,roster,registry())
    changed=deepcopy(ledger)
    for r in changed:
        if r['month']>='2020-05':r.update(netPnl=99999.,returnPct=999.,available=False,observedBars=0)
    earlier=deepcopy(doc);earlier['evaluationMonths']=['2020-05']
    prefix=build_scores(monthly_parent,earlier,changed,roster,registry())
    assert prefix==[b for b in scores if b['month']=='2020-05']
    for b in scores:
        assert len(b['historyMonths'])==(2 if b['selectorId']=='mean' else 4)
        assert all(m<b['month'] for m in b['historyMonths'])


def test_experimental_bars_exclusions_sources_and_offline_rebuild(monthly_parent,tmp_path,monkeypatch):
    source=tmp_path/'research.py'
    source.write_text('''from tools.pattern_lab.selection import Scorer, Score
def normalize(p):
    assert set(p)=={'months','excluded','scale'}
    assert type(p['months']) is int and p['months']>0
    return dict(p)
def score(view,p):
    for ticker in view.instruments:
        bars=view.bars(ticker)
        assert (bars.timestamps+bars.stepMs<=view.cutoffMs).all()
        assert len(view.months(ticker))==p['months']
        assert not bars.values.flags.writeable and not bars.timestamps.flags.writeable
        try: bars.values.flags.writeable=True
        except ValueError: pass
        else: raise AssertionError('mutable backing')
        if ticker==p['excluded']:yield Score(ticker,False,None,'research exclusion',{})
        else:yield Score(ticker,True,(float(bars.values[-1,3])*p['scale'],),None,dict(rows=len(bars.timestamps)))
''',encoding='utf-8')
    import importlib.util
    spec=importlib.util.spec_from_file_location('selection_research',source);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    scorer=Scorer('x.native','1',module.normalize,lambda p:p['months'],('close',),module.score,{'research.py':source},True)
    request=selection_request(monthly_parent)
    request['selectors']=[dict(selectorId='research',scorerId='x.native',version='1',parameters=dict(months=2,excluded='BYBIT_FFFUSDT',scale=-1.))]
    reader=run_selection(request,parent_root=monthly_parent.root,output_root=tmp_path/'selected',scorers={'x.native':scorer})
    for c in reader.results['comparisons']:
        for m in c['months']:
            assert m['eligibleCount']==5 and 'BYBIT_FFFUSDT' not in m['eligiblePool']
            assert all(set(r['members'])<=set(m['eligiblePool']) for r in m['random'])
    assert verify_selection(reader.root,parent_root=monthly_parent.root,scorers={'x.native':scorer})['scorers']=={'research':'recomputed'}
    assert verify_selection(reader.root,parent_root=monthly_parent.root)['scorers']=={'research':'storedOutputsOnly'}
    source.write_text(source.read_text()+'\n# Comment-only change\n',encoding='utf-8')
    with pytest.raises(PatternLabDataError,match='source mismatch'):verify_selection(reader.root,parent_root=monthly_parent.root,scorers={'x.native':scorer})
    second=run_selection(request,parent_root=monthly_parent.root,output_root=tmp_path/'second',scorers={'x.native':scorer})
    assert second.selection['identities']['specification']==reader.selection['identities']['specification']
    assert second.selection['identities']['implementation']!=reader.selection['identities']['implementation']
    bundle=tmp_path/'research.zip';export_selection_bundle(reader.root,zip_path=bundle)
    source.unlink()  # task-owned module deliberately absent from offline reconstruction
    def denied(*a,**kw):raise AssertionError('offline rebuild touched parent/market/network/execution')
    for name in ('tools.pattern_lab.selection.ledger.load_account_study','tools.pattern_lab.selection.artifacts.verify_account_study',
        'tools.pattern_lab.data.read_session','tools.pattern_lab.exchange_data.HttpClient.get_json','tools.pattern_lab.account_study.job.execute_account'):
        monkeypatch.setattr(name,denied)
    imported=import_selection_bundle(bundle,output_root=tmp_path/'imported')
    rebuild_selection_report(imported.root,output_root=tmp_path/'rebuilt')
    assert (tmp_path/'rebuilt/derived/summary.json').read_bytes()==(reader.root/'derived/summary.json').read_bytes()


@pytest.mark.parametrize('kind',['scores','membership','arithmetic','extra','bytes'])
def test_resealed_semantic_and_byte_tampering(monthly_parent,tmp_path,kind):
    reader=run_selection(selection_request(monthly_parent),parent_root=monthly_parent.root,output_root=tmp_path/'selection')
    if kind=='scores':
        value=read_json(reader.root/'scores.json');value[0]['rows'][0]['key'][0]+=1
        write_json(reader.root/'scores.json',value)
    elif kind in ('membership','arithmetic'):
        value=read_json(reader.root/'results.json');row=value['comparisons'][0]['months'][0]['selected']
        if kind=='membership':row['members']=[]
        else:row['returnPct']+=1
        write_json(reader.root/'results.json',value)
    elif kind=='extra':(reader.root/'extra.txt').write_text('unexpected')
    else:(reader.root/'ledger.json').write_bytes(b'bad')
    if kind!='bytes':seal_directory(reader.root)
    with pytest.raises((PatternLabDataError,ValueError)):load_selection(reader.root)


@pytest.mark.parametrize('name',['../escape','/absolute','paths/../escape','DUPLICATE','case-alias','file-directory','symlink','nfc'])
def test_zip_path_and_type_refusals(tmp_path,name):
    archive=tmp_path/'bad.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('completion.json','{}')
        if name=='DUPLICATE':
            with pytest.warns(UserWarning):z.writestr('completion.json','{}')
        elif name=='case-alias':z.writestr('DIR/a','a');z.writestr('dir/b','b')
        elif name=='file-directory':z.writestr('a','a');z.writestr('a/b','b')
        elif name=='symlink':
            info=zipfile.ZipInfo('link');info.external_attr=0o120777<<16;z.writestr(info,'target')
        else:z.writestr('e\u0301' if name=='nfc' else name,'x')
    with pytest.raises(PatternLabDataError):import_selection_bundle(archive,output_root=tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_gap_coverage_common_mask_and_diagnostics(tmp_path):
    request=build_inputs(tmp_path/'pack',gaps=True)
    parent=run_account_study(request,data_root=tmp_path/'pack',output_root=tmp_path/'parent')
    doc=selection_request(parent)
    reader=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'selection')
    may=[r for r in reader.ledger if r['month']=='2020-05' and r['instrumentId'] in ('BYBIT_AAAUSDT','BYBIT_BBBUSDT','BYBIT_CCCUSDT')]
    assert len({r['observedBars'] for r in may})==1 and all(r['missingBars']==1 for r in may)
    assert [(r['missingHeadSlots'],r['missingTailSlots'],r['missingInteriorSlots']) for r in may]==[(1,0,0),(0,1,0),(0,0,1)]
    assert any(not r['available'] and r['returnPct'] is None for r in reader.ledger)
    for c in reader.results['comparisons']:
        assert c['months'][0]['available'] and not c['months'][1]['available']
        assert [m['eligibleCount'] for m in c['months']]==[6,6,5]
        assert c['months'][1]['selected']['returnPct'] is None and c['months'][1]['allEligible']['returnPct'] is None
        assert c['matchedAvailableMonths']['months']==['2020-05','2020-07']
    doc['pool']['minObservedBarsPct']=100.
    strict=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'strict')
    assert all(not c['months'][0]['available'] for c in strict.results['comparisons'])


def test_public_example_external_subprocess(tmp_path):
    script=Path(__file__).resolve().parents[2]/'tools/pattern_lab/examples/selection_synthetic.py'
    env=dict(os.environ);env.pop('PYTHONPATH',None)
    result=subprocess.run([sys.executable,'-B','-X','utf8',str(script),'--output-root',str(tmp_path/'example')],
        cwd=tmp_path,env=env,capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stdout+result.stderr
    receipt=json.loads(result.stdout);assert receipt['verification']['status']=='verified'
    assert_fixture(load_selection(tmp_path/'example/selection'))


def test_node_selection_report(monthly_parent,tmp_path):
    node=shutil.which('node')
    if node is None:pytest.skip('Node unavailable on PATH; selection report interaction gate pending Linux Node review')
    reader=run_selection(selection_request(monthly_parent),parent_root=monthly_parent.root,output_root=tmp_path/'selection')
    harness=Path(__file__).with_name('selection_report_interactions.cjs')
    result=subprocess.run([node,str(harness),str(reader.root/'derived/report.html')],capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr
    assert json.loads(result.stdout)['status']=='passed'


def test_v2_parent_all_rejected_is_observed_zero(monthly_parent,tmp_path):
    raw=monthly_parent.study['request'];raw['schema']='pattern-lab.account-study-request.v2';raw['auxiliaryInputs']=[]
    for a in raw['allocations']:
        a['auxiliaryFeatures']=[];a['policy']['riskPct']=1e-9
    parent=run_account_study(raw,data_root=monthly_parent.root.parent/'pack',output_root=tmp_path/'v2-parent')
    reader=run_selection(selection_request(parent),parent_root=parent.root,output_root=tmp_path/'selection')
    assert all(r['available'] and r['trades']==0 and r['returnPct']==0 for r in reader.ledger)
    assert sum(r['sizingRejectionCount'] for r in reader.ledger)>0
    assert verify_selection(reader.root,parent_root=parent.root)['status']=='verified'


def test_scorer_source_change_leaves_failed_unsealed(monthly_parent,tmp_path):
    source=tmp_path/'scorer.py';source.write_text('# initial source\n')
    def changing(view,p):
        source.write_text('# changed source\n')
        return [Score(t,True,(1.,),None,{}) for t in view.instruments]
    scorer=Scorer('x.changing','1',lambda p:p,lambda p:1,('value',),changing,{'scorer.py':source})
    request=selection_request(monthly_parent)
    request['selectors']=[dict(selectorId='changed',scorerId='x.changing',version='1',parameters={})]
    with pytest.raises(PatternLabDataError,match='source changed'):
        run_selection(request,parent_root=monthly_parent.root,output_root=tmp_path/'failed',scorers={'x.changing':scorer})
    assert not (tmp_path/'failed/completion.json').exists()
    assert read_json(tmp_path/'failed/status.json')['state']=='failed'


def test_coherent_ledger_forgery_needs_exact_parent_verification(monthly_parent,tmp_path):
    from tools.pattern_lab.selection.artifacts import Selection,save_arrays,write_derived
    from tools.pattern_lab.selection.engine import data_identity
    from tools.pattern_lab.selection.arithmetic import calculate
    from tools.pattern_lab.selection.ledger import money
    reader=run_selection(selection_request(monthly_parent),parent_root=monthly_parent.root,output_root=tmp_path/'selection')
    ledger=reader.ledger;paths={m:{k:a.copy() for k,a in p.items()} for m,p in reader._paths.items()}
    row=next(r for r in ledger if r['month']=='2020-07' and r['instrumentId']=='BYBIT_AAAUSDT')
    row['finalBalance']+=1.;row['netPnl'],row['returnPct']=money(row['finalBalance'],row['initialCapital'])
    paths['2020-07']['pnl'][0,-1]=row['netPnl']
    meta=reader.selection;scores=reader.scores;doc=meta['request']
    meta['identities']['dataInput']=data_identity(doc,ledger,paths,scores,meta['allocation'])
    results,curves=calculate(doc,ledger,paths,meta['roster'],scores)
    for name,value in (('selection',meta),('ledger',ledger),('results',results)):write_json(reader.root/(name+'.json'),value)
    path=reader.root/'paths/2020-07.npz';path.unlink();save_arrays(path,paths['2020-07'])
    path=reader.root/'curves.npz';path.unlink();save_arrays(path,{i+'.'+k:a for i,p in curves.items() for k,a in p.items()})
    forged=Selection(reader.root,meta,ledger,scores,results,paths,curves);write_derived(forged,reader.root);seal_directory(reader.root)
    load_selection(reader.root)
    with pytest.raises(PatternLabDataError,match='parent ledger'):verify_selection(reader.root,parent_root=monthly_parent.root)
    with pytest.raises(PatternLabDataError):verify_selection(reader.root,parent_root=tmp_path/'missing-parent')


def test_native_bar_prefix_ignores_future_append_and_mutation(monthly_parent):
    from types import SimpleNamespace
    import numpy as np
    from tools.pattern_lab.selection.request import month_bounds
    from tools.pattern_lab.selection import check_prefix_invariance
    doc=selection_request(monthly_parent);ledger,_,roster=extract_ledger(monthly_parent,doc)
    months=['2020-03','2020-04'];cutoff=month_bounds('2020-05')[0]
    original,dependencies=score_view(monthly_parent,doc,ledger,roster,months,cutoff,True)
    def altered_account(key):
        target=monthly_parent.account(key).prepared.target
        values=target.values.copy();values[target.timestamps>=cutoff]=99999.
        times=np.r_[target.timestamps,target.timestamps[-1]+target.stepMs]
        values=np.vstack((values,np.ones((1,5))*99999))
        return SimpleNamespace(prepared=SimpleNamespace(target=SimpleNamespace(timestamps=times,values=values,stepMs=target.stepMs)))
    changed=SimpleNamespace(key_for=monthly_parent.key_for,account=altered_account)
    extended,new_dependencies=score_view(changed,doc,ledger,roster,months,cutoff,True)
    assert dependencies==new_dependencies
    def score(v,p):return [Score(t,True,(float(v.bars(t).values[-1,3]),),None,{}) for t in v.instruments]
    scorer=Scorer('x.prefix','1',lambda p:p,lambda p:2,('close',),score,needsBars=True)
    check_prefix_invariance(scorer,original,extended,{})
    assert all(m['month'] in months for m in original.months(roster[0]))


def test_selection_cli_and_embedded_label_safety(monthly_parent,tmp_path,capsys):
    from tools.pattern_lab.__main__ import main
    request=selection_request(monthly_parent);request['notes']='</script><script>untrusted()</script>'
    write_json(tmp_path/'request.json',request)
    assert main(['selection','--request',str(tmp_path/'request.json'),'--parent-root',str(monthly_parent.root),'--output-root',str(tmp_path/'selection')])==0
    assert json.loads(capsys.readouterr().out)['status']=='complete'
    assert main(['selection-report','--run-root',str(tmp_path/'selection'),'--output-root',str(tmp_path/'report')])==0
    assert json.loads(capsys.readouterr().out)['status']=='complete'
    html=(tmp_path/'report/derived/report.html').read_text(encoding='utf-8')
    assert '<script>untrusted()' not in html and '\\u003c/script\\u003e' in html
