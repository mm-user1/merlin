from copy import deepcopy
from dataclasses import replace
import json
import os

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError as Error
from tools.pattern_lab.account_study import (run_account_study, normalize_request, load_account_study,
    verify_account_study, rebuild_account_report, export_account_bundle, import_account_bundle)
from tools.pattern_lab.account_study import runner
from tools.pattern_lab.account_study.codec import equal_records, load_debug_preparation
from tools.pattern_lab.accounts import build_commands, HARecipe, candle_filter, execute_account
from ._account_study_helpers import build_pack, request, assert_filtered_trades


@pytest.mark.parametrize('legacy',[False,True])
def test_native_roundtrip_windows_debug_and_offline(tmp_path, monkeypatch, legacy):
    pack = tmp_path/'pack'; ids=build_pack(pack,count=6,gaps=True)
    raw=request(ids)
    path=tmp_path/'request.json'; path.write_text(json.dumps(raw))
    normalized=normalize_request(path)
    assert normalized == normalize_request(normalized) == normalize_request(raw)
    standard=run_account_study(normalized,data_root=pack,output_root=tmp_path/'standard',_legacy_directory=legacy)
    debug=run_account_study(path,data_root=pack,output_root=tmp_path/'debug',retention_profile='debug',_legacy_directory=legacy)
    assert standard.study['identities'] == debug.study['identities']
    assert standard.summary == debug.summary
    assert assert_filtered_trades(standard) == assert_filtered_trades(debug)
    for key in standard.accountKeys:
        s,d=standard.account(key),debug.account(key)
        assert equal_records(s,d)
        p=load_debug_preparation(debug.root,d.result.preparationId)
        allocation=next(a for a in normalized.document['allocations'] if a['allocationId']==d.labels['allocationId'])
        commands=build_commands(p,HARecipe(**allocation['provider']['recipe']),mode=d.result.policy.mode,
            stopUpdates=d.result.policy.stopUpdates,entryFilter=candle_filter(p,'state'))
        assert commands.identity==d.result.commandId
        direct=execute_account(p,commands,d.result.policy,d.result.window,d.result.rules)
        assert equal_records(d,replace(d,prepared=p,result=direct))
        with pytest.raises(ValueError): s.prepared.target.values.flat[0]=1
    for a in normalized.document['allocations']:
        x=standard.account(standard.key_for(ids[0],a['allocationId'],'full'))
        y=standard.account(standard.key_for(ids[0],a['allocationId'],'overlap'))
        assert x.result.commandId==y.result.commandId and x.result.preparationId==y.result.preparationId
        assert x.result.policy.initialCapital==y.result.policy.initialCapital==10000
    assert any(s['commandReadiness']['long']['unknownEntryDecisionBars'] for s in standard.summary['accounts'])
    before=(standard.root/'completion.json').read_bytes()
    def denied(*a,**kw): raise AssertionError('offline pack/executor access')
    monkeypatch.setattr(runner.data,'read_session',denied)
    monkeypatch.setattr('tools.pattern_lab.accounts.execute_account',denied)
    assert verify_account_study(standard.root)['accountsChecked']==len(standard.accountKeys)
    assert verify_account_study(debug.root)['accountsChecked']==len(debug.accountKeys)
    rebuild_account_report(standard.root,output_root=tmp_path/'rebuilt')
    for name in ('summary.json','report.html'):
        assert (standard.root/'derived'/name).read_bytes()==(tmp_path/'rebuilt/derived'/name).read_bytes()
    assert (standard.root/'completion.json').read_bytes()==before
    export_account_bundle(standard.root,zip_path=tmp_path/'bundle.zip')
    imported=import_account_bundle(tmp_path/'bundle.zip',output_root=tmp_path/'imported')
    assert imported.summary==standard.summary
    assert assert_filtered_trades(imported) == assert_filtered_trades(standard)
    assert verify_account_study(imported.root)['status']=='verified'


def test_six_workers_same_evidence_and_bounded_payload(tmp_path):
    pack=tmp_path/'pack'; ids=build_pack(pack,count=6,gaps=True)
    raw=request(ids)
    from tools.pattern_lab.study.workers import THREAD_ENVIRONMENT
    before={k:os.environ.get(k) for k in THREAD_ENVIRONMENT}
    direct=run_account_study(raw,data_root=pack,output_root=tmp_path/'direct')
    pooled=run_account_study(raw,data_root=pack,output_root=tmp_path/'pooled',workers=6)
    assert {k:os.environ.get(k) for k in THREAD_ENVIRONMENT}==before
    for name in ['account-index.json', *direct._locator.groups]:
        assert (direct.root/name).read_bytes()==(pooled.root/name).read_bytes()
    assert pooled.status['effectiveWorkers']==6 and pooled.status['maximumInFlight']<=6
    assert len(pooled.status['workerPids'])==6
    assert direct.study['identities']==pooled.study['identities']
    assert assert_filtered_trades(direct) == assert_filtered_trades(pooled)
    for key in direct.accountKeys: assert equal_records(direct.account(key),pooled.account(key))
    for name in ('summary.json','report.html'):
        assert (direct.root/'derived'/name).read_bytes()==(pooled.root/'derived'/name).read_bytes()


@pytest.mark.parametrize('change', ['warmup','source','atr','stop','version','duplicate','stacked','policy','reserve',
    'unusedAtr_haLast','unusedAtr_haSwing2'])
def test_invalid_request_before_pack_or_output(tmp_path,monkeypatch,change):
    raw=request(['BYBIT_AAAUSDT'])
    a=raw['allocations'][0]
    if change=='warmup': raw['preparationStartUtc']=raw['windows'][0]['startUtc']
    elif change=='source': raw['sources'].append(dict(inputId='unused',instrumentId='BYBIT_UNUSEDUSDT',timeframeMinutes=240))
    elif change=='atr': raw['allocations'][2]['features'][-1]['parameters']['length']=100
    elif change=='stop': raw['allocations'][2]['provider']['recipe']['swingBars']=100
    elif change=='version': a['provider']['version']='2'
    elif change=='duplicate': raw['instruments']*=2
    elif change=='stacked': a['entryFilter']=[a['entryFilter']]
    elif change=='policy': del a['policy']['maxHoldingDays']
    elif change=='reserve': raw['protocol']['reserved']=deepcopy(raw['protocol']['development'])
    elif change.startswith('unusedAtr_'):
        a['features'].append(dict(requestId='atr',featureId='atr',version='1',inputId='target',
            parameters=dict(length=3),priorBars=0))
        a['provider']['recipe'].update(stopProvider=change.split('_')[1],atrRequestId='atr')
    monkeypatch.setattr(runner.data,'read_session',lambda *a: pytest.fail('premature pack read'))
    with pytest.raises(Error,match='unused ATR dependency' if change.startswith('unusedAtr_') else None): normalize_request(raw)
    with pytest.raises(Error): run_account_study(raw,data_root=tmp_path/'none',output_root=tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_publication_failure_retains_truthful_partial(tmp_path,monkeypatch):
    pack=tmp_path/'pack';ids=build_pack(pack)
    def fail(*a,**kw): raise OSError('publication refused')
    monkeypatch.setattr(runner,'write_derived',fail)
    with pytest.raises(OSError): run_account_study(request(ids),data_root=pack,output_root=tmp_path/'failed')
    with pytest.raises(Error): load_account_study(tmp_path/'failed')
    partial=load_account_study(tmp_path/'failed',allow_partial=True)
    assert partial.status['state']=='failed' and partial.status['phase']=='derive'
    assert not (partial.root/'completion.json').exists()
    with pytest.raises(Error): _=partial.summary


@pytest.mark.parametrize('direction',['long','short','both'])
@pytest.mark.parametrize('mode,updates,stop',[('fixedBracket',False,'haLast'),
    ('signalManaged',False,'haSwing2'),('signalManaged',True,'realSwingAtr')])
def test_mode_direction_adapter_matrix(tmp_path,direction,mode,updates,stop):
    pack=tmp_path/'pack';ids=build_pack(pack,count=1)
    raw=request(ids,filtered=False,multiple=False)
    a=raw['allocations'][0]
    a['policy'].update(mode=mode,stopUpdates=updates,direction=direction,
                       rewardRisk=2. if mode=='fixedBracket' else None)
    a['provider']['recipe']['stopProvider']=stop
    if stop=='realSwingAtr':
        a['features'].append(dict(requestId='atr',featureId='atr',version='1',inputId='target',
            parameters=dict(length=3),priorBars=0))
        a['provider']['recipe']['atrRequestId']='atr'
    reader=run_account_study(raw,data_root=pack,output_root=tmp_path/'run',retention_profile='debug')
    assert verify_account_study(reader.root)['accountsChecked']==2
    for key in reader.accountKeys:
        record=reader.account(key);r=record.result
        p=load_debug_preparation(reader.root,r.preparationId)
        direct=execute_account(p,r.commands,r.policy,r.window,r.rules)
        assert equal_records(record,replace(record,prepared=p,result=direct))


def test_fixed_source_once_per_span_and_allocation_subsets(tmp_path,monkeypatch):
    pack=tmp_path/'pack';ids=build_pack(pack,count=2,slots=112,gaps=True)
    raw=request(ids,slots=96,own_source=True)
    a=deepcopy(raw['allocations'][0]);a['allocationId']='fixed'
    a['features'][1]['inputId']='btc4h'
    raw['allocations'].append(a)
    raw['sources'].append(dict(inputId='btc4h',instrumentId='BYBIT_BTCUSDT',timeframeMinutes=240))
    calls=[];original=runner.data._ReadSession.load_slice
    def spy(self,identifier,**kw):
        calls.append((identifier,kw))
        assert kw['end']==raw['windows'][0]['endUtc']
        return original(self,identifier,**kw)
    monkeypatch.setattr(runner.data._ReadSession,'load_slice',spy)
    reader=run_account_study(raw,data_root=pack,output_root=tmp_path/'run')
    assert [i for i,kw in calls].count('BYBIT_BTCUSDT')==1
    assert len(calls)==3
    for key in reader.accountKeys:
        r=reader.account(key)
        aliases={v['inputId'] for v in r.preparationFacts['inputs']}
        assert aliases=={'target','btc4h' if r.labels['allocationId']=='fixed' else 'own4h'}
        if 'own4h' in aliases:
            assert all(v['header']['instrument_id']==r.labels['instrumentId'] for v in r.preparationFacts['inputs'])
            assert len(r.result.trades)==0
    own_reasons={reason for key in reader.accountKeys if reader.account(key).labels['allocationId']!='fixed'
        for reason in reader.account(key).result.attempts['reason'].to_pylist()}
    assert {'filterBlocked','filterUnknown'} <= own_reasons


def test_source_close_warmup_off_by_one():
    raw=request(['BYBIT_AAAUSDT'],multiple=False,own_source=True)
    # At 08:30 the latest completed 4h source bar has index 1, not 2.
    raw['allocations'][0]['features'][1]['parameters']['confirmationBars']=3
    with pytest.raises(Error,match='insufficient declared warmup.*own4h'): normalize_request(raw)


def test_metadata_and_later_payload_failure_do_not_publish_subset(tmp_path,monkeypatch):
    pack=tmp_path/'pack';ids=build_pack(pack,count=2)
    invalid=request(ids+['BYBIT_ABSENTUSDT'])
    with pytest.raises(Error):run_account_study(invalid,data_root=pack,output_root=tmp_path/'absent')
    assert not (tmp_path/'absent').exists()
    original=runner.data._ReadSession.load_slice
    def fail_later(self,identifier,**kw):
        if identifier==ids[-1]:raise Error('synthetic payload unavailable')
        return original(self,identifier,**kw)
    monkeypatch.setattr(runner.data._ReadSession,'load_slice',fail_later)
    with pytest.raises(Error):run_account_study(request(ids),data_root=pack,output_root=tmp_path/'partial')
    r=load_account_study(tmp_path/'partial',allow_partial=True)
    assert r.status['completedJobs']==1 and r.status['jobs'][ids[-1]]=='failed'
    assert not (r.root/'completion.json').exists() and not (r.root/'derived/report.html').exists()


def test_cli_native_and_offline_report(tmp_path,capsys):
    from tools.pattern_lab.__main__ import main
    pack=tmp_path/'pack';ids=build_pack(pack,count=1)
    path=tmp_path/'request.json';path.write_text(json.dumps(request(ids,filtered=False,multiple=False)))
    assert main(['account-study','--request',str(path),'--data-root',str(pack),
                 '--output-root',str(tmp_path/'run')])==0
    response=json.loads(capsys.readouterr().out)
    assert response['status']=='complete' and response['accounts']==2
    assert main(['account-report','--run-root',str(tmp_path/'run'),'--output-root',str(tmp_path/'rebuilt')])==0
    assert json.loads(capsys.readouterr().out)['status']=='complete'
    assert main(['account-study','--request',str(path),'--data-root',str(pack),
                 '--output-root',str(tmp_path/'run')])!=0
    assert json.loads(capsys.readouterr().out)['status']=='failed'
