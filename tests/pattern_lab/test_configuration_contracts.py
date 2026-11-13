"""Closed admission, stable identities, canonical metrics and actual-time displays."""
from copy import deepcopy
from types import SimpleNamespace as NS
import math
import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.configuration_study import normalize_request
from tools.pattern_lab.configuration_study.request import configurations, numeric_key, result_id
from tools.pattern_lab.configuration_study.metrics import daily_sharpe, annualized, month_equivalents, DAY
from tools.pattern_lab.configuration_study.curves import account_curve, price_curve, quantize, SAFE
from ._configuration_helpers import request


@pytest.mark.parametrize('fault',['extra','stages','protocol','overlap','reserve','capital','duplicate','bool','window'])
def test_admission(fault):
    doc=request(['BYBIT_AAAUSDT'])
    if fault=='extra':doc['finalists']=[]
    if fault=='stages':doc['stages'].reverse()
    if fault=='protocol':doc['stages'][1]['protocol']['protocol_id']=doc['stages'][0]['protocol']['protocol_id']
    if fault=='overlap':doc['stages'][1]['startUtc']=doc['stages'][0]['startUtc']
    if fault=='reserve':doc['stages'][0]['protocol']['reserved']['end_utc']=doc['stages'][1]['endUtc']
    if fault=='capital':doc['allocations'][0]['policy']['initialCapital']=1001
    if fault=='duplicate':
        a=deepcopy(doc['allocations'][0]);a['allocationId']='duplicate';a['policy']['rewardRisk']=2.0
        doc['allocations'].append(a)
    if fault=='bool':doc['allocations'][0]['timeframeMinutes']=True
    if fault=='window':doc['stages'][2]['endUtc']=doc['stages'][2]['startUtc']
    with pytest.raises(PatternLabDataError):normalize_request(doc)


def test_immutable_normalization_and_identity_boundaries():
    doc=request(['BYBIT_AAAUSDT']);normalized=normalize_request(doc)
    before=configurations(normalized.document);first=before[0]['configurationId']
    copy=normalized.document;copy['studyId']='mutated'
    assert normalized.document['studyId']!=copy['studyId']
    assert normalize_request(normalized)==normalized
    changed=deepcopy(doc);changed['studyId']='renamed';changed['notes']='presentation'
    changed['allocations'][0]['allocationId']='newAlias'
    assert {r['configurationId'] for r in configurations(normalize_request(changed).document)}=={r['configurationId'] for r in before}
    stage=normalized.document['stages'][0];later=dict(stage,endUtc=doc['stages'][1]['endUtc'])
    assert result_id(first,stage)!=result_id(first,later)
    assert numeric_key(2)==numeric_key(2.0) and numeric_key(True)!=numeric_key(1)
    assert numeric_key(2**53+1)!=numeric_key(float(2**53+1))


def test_native_atr_numeric_spellings_are_equivalent_duplicates():
    from tools.pattern_lab.configuration_study.request import child_request, native_request, configuration_payload
    from tools.pattern_lab.study.contracts import semantic_digest
    doc=request(['BYBIT_AAAUSDT'],filtered=False,multi_tf=False)
    first=deepcopy(doc['allocations'][-1]);first['provider']['recipe']['atrMultiplier']=1
    second=deepcopy(first);second['allocationId']='numericAlias';second['provider']['recipe']['atrMultiplier']=1.0
    doc['allocations']=[first,second]
    native=native_request(child_request(doc,doc['stages'][0])).document
    values=[a['provider']['recipe']['atrMultiplier'] for a in native['allocations']]
    assert {type(v) for v in values}=={int,float} and values[0]==values[1]
    payloads=[configuration_payload(native,'BYBIT_AAAUSDT',a) for a in native['allocations']]
    assert semantic_digest(payloads[0])!=semantic_digest(payloads[1])
    with pytest.raises(PatternLabDataError,match='numerically equivalent configuration'):
        normalize_request(doc)


def test_execution_request_file_and_relative_protocols(tmp_path,monkeypatch):
    doc=request(['BYBIT_AAAUSDT']);expected=normalize_request(doc)
    directory=tmp_path/'inputs';directory.mkdir()
    for stage in doc['stages']:
        name=stage['stageId']+'.json';(directory/name).write_text(json.dumps(stage['protocol']))
        stage['protocol']=name
    path=directory/'request.json';path.write_text(json.dumps(doc));monkeypatch.chdir(tmp_path)
    assert normalize_request(path)==expected


def member_request(sizes,instruments):
    doc=request([f'BYBIT_T{i}USDT' for i in range(instruments)],filtered=False,multi_tf=False)
    base=deepcopy(doc['allocations'][0]);doc['allocations']=[]
    for tf,size in zip((30,60,120,240),sizes):
        for i in range(size):
            a=deepcopy(base);a.update(allocationId=f'tf{tf}-rr{i}',timeframeMinutes=tf)
            a['policy']['rewardRisk']=1+i/1000;doc['allocations'].append(a)
    return doc


@pytest.mark.parametrize('sizes,instruments,members',[([1],1,9),([16,16],1,15),([16,17],2,42),([17,17],1,27),([200]*4,4,1260)])
def test_planned_member_group_rounding_and_prices(monkeypatch,sizes,instruments,members):
    from tools.pattern_lab.configuration_study import request as contract,codec
    assert contract.MAX_MEMBERS==codec.MAX_MEMBERS==65534
    doc=member_request(sizes,instruments)
    monkeypatch.setattr(contract,'MAX_MEMBERS',members)
    normalized=normalize_request(doc)
    assert len(configurations(normalized.document))==sum(sizes)*instruments
    monkeypatch.setattr(contract,'MAX_MEMBERS',members-1)
    with pytest.raises(PatternLabDataError,match='planned roster exceeds member capacity'):
        normalize_request(doc)


def test_many_small_groups_refused_before_pack_or_output(tmp_path,monkeypatch):
    from tools.pattern_lab.configuration_study import runner
    doc=member_request([1]*4,2500)  # 3 * 2500 * (2 * 4 + 1) = 67,500.
    def denied(*a,**kw):raise AssertionError('impossible roster reached pack/output/worker')
    monkeypatch.setattr(runner.data,'read_session',denied)
    monkeypatch.setattr(runner,'fresh_directory',denied)
    monkeypatch.setattr(runner.workers_module,'SpawnJobPool',denied)
    with pytest.raises(PatternLabDataError,match='planned roster exceeds member capacity'):
        runner.run_configuration_study(doc,data_root=tmp_path/'absent',output_root=tmp_path/'out',workers=2)
    assert not (tmp_path/'out').exists()


@pytest.mark.parametrize('field',['instrumentId','allocationId'])
@pytest.mark.parametrize('value',[[],{},None,1,True])
def test_annotation_reference_types_are_domain_errors(field,value):
    from tools.pattern_lab.configuration_study import normalize_annotations
    class MetadataOnly:
        metadata={'identities':{'specification':'s'}}
        configurations=[dict(instrumentId='BYBIT_AAAUSDT',allocationId='rr2')]
        def __getattr__(self,name):raise AssertionError('annotation accessed '+name)
    annotation=dict(instrumentId='BYBIT_AAAUSDT',allocationId='rr2',role='reference',methodId='manual',
        methodDescription='Synthetic IS reference',selectedFromStage='IS')
    doc=dict(schema='pattern-lab.configuration-annotations.v1',specificationIdentity='s',annotations=[annotation])
    assert normalize_annotations(doc,MetadataOnly())==doc
    annotation[field]='unknown'
    with pytest.raises(PatternLabDataError,match='unknown annotation configuration'):normalize_annotations(doc,MetadataOnly())
    annotation[field]=value
    with pytest.raises(PatternLabDataError,match='annotation reference IDs must be strings'):normalize_annotations(doc,MetadataOnly())


@pytest.mark.parametrize('days,available,short',[(30,False,False),(30.5,True,False),(31,True,True),(89.5,True,True),(90,True,False)])
def test_annualized_edges(days,available,short):
    value,status,marker=annualized(10,days)
    assert (value is not None)==available and marker==short
    if available:assert value==pytest.approx((1.1**(365/days)-1)*100,rel=1e-12)
    assert annualized(-100,days)[0] is None


def test_calendar_months_and_explicit_ms_sharpe():
    ms=lambda s:int(pd.Timestamp(s,tz='UTC').timestamp()*1000)
    assert month_equivalents(ms('2020-02-01'),ms('2020-03-01'))==1
    assert month_equivalents(ms('2020-02-15'),ms('2020-03-16'))==pytest.approx(15/29+15/31)
    times=[ms('2020-02-01')+1,ms('2020-02-01')+5000,ms('2020-02-03'),ms('2020-02-04')]
    equity=[101.,102.,101.,105.]
    value,status,count,active=daily_sharpe(equity,times,100.,3)
    returns=np.array([.02,101/102-1,105/101-1])
    expected=float(np.mean(returns-.02/365)/np.std(returns)*math.sqrt(365))
    assert count==active==3 and status=='available' and value==pytest.approx(expected,abs=1e-12)
    assert daily_sharpe(equity,times,100.,0)[1:]==('noTrades',3,3)
    assert daily_sharpe([],[],100.,0)==(None,'invalidDailySeries',None,None)
    assert daily_sharpe([100,100],times[::2],100,1)[1:]==('zeroOrInvalidVariance',2,0)


def test_curve_actual_closes_and_exact_cash_gaps():
    opens=[DAY,2*DAY,5*DAY];equity=[101,102,105]
    path=pa.table(dict(barOpenMs=opens,equity=equity,balance=equity,side=[0]*3,quantity=[0]*3))
    record=NS(result=NS(path=path,window=NS(startMs=0,endMs=8*DAY),policy=NS(initialCapital=100),finalBalance=105),prepared=NS(stepMs=DAY))
    c=account_curve(record)
    assert c['timeMs']==[0,DAY,3*DAY,5*DAY,6*DAY,8*DAY]
    assert c['cashGaps']==[dict(startMs=0,endMs=DAY,cashQ=0),dict(startMs=3*DAY,endMs=5*DAY,cashQ=200),dict(startMs=6*DAY,endMs=8*DAY,cashQ=500)]
    assert c['endpoint']=='flatCashExtension' and c['equityQ']==[0,0,200,200,500,500]
    record.result.path=path.slice(0,0);assert account_curve(record)['status']=='noObservedBars'


def test_price_missing_midnight_does_not_flat_extend():
    times=np.array([DAY-300000,DAY,2*DAY+300000],dtype=np.int64)
    values=np.array([[100,102,99,101,1],[101,103,100,102,1],[103,105,102,104,1]])
    p=price_curve(times,values,0,3*DAY,'0'*64)
    assert p['timeMs']==[DAY-300000,DAY,2*DAY,2*DAY+600000,3*DAY]
    assert p['priceQ']==[0,100,None,400,None]
    assert p['lastObservedTimeMs']==2*DAY+600000
    assert len(p['coverageGaps'])==3
    assert quantize(.125)==12 and quantize(.375)==38
    with pytest.raises(PatternLabDataError):quantize(SAFE)


def test_initial_peak_drawdown_and_no_observation_metrics():
    from tools.pattern_lab.account_study.metrics import drawdown
    from tools.pattern_lab.configuration_study.metrics import extract_metrics
    from tools.pattern_lab.accounts import execute_account
    from tools.pattern_lab.account_study.codec import AccountRecord
    from ._account_helpers import prepared,bundle,policy,window,rules,STEP
    assert drawdown([90,80],100)==20
    p=prepared([(100,102,98,100)]*2,slots=[0,2],start=0,end=3*STEP)
    result=execute_account(p,bundle(p),policy(),window(p,start=STEP,end=2*STEP),rules())
    m=extract_metrics(AccountRecord(p,result,dict(instrumentId='OKX_AAA-USDT-SWAP',allocationId='empty',windowId='IS')))
    assert m['performanceStatus']=='noObservedBars' and m['profitFactorStatus']=='noTrades'
    assert m['initialCapital']==m['finalCapital'] and m['mtmMaxDdPct'] is None and m['balanceMaxDdPct'] is None
    assert m['dailySharpeObservations'] is None and m['annualizedProfitPct'] is None
