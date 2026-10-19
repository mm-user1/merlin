"""Offline snapshot, collection and causal-clock contracts; no ignored inputs."""
from copy import deepcopy
from datetime import datetime, timezone
from urllib.parse import urlsplit, parse_qs
import json

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import exchange_data as ex
from tools.pattern_lab.auxiliary import publish_snapshot, load_snapshot, collect_auxiliary, inspect_auxiliary
from tools.pattern_lab.auxiliary.model import TYPES, TypeDescriptor, content, fingerprint, normalize_rows
from tools.pattern_lab.auxiliary.features import build_feature, event_buckets, transition_pulse, condition
from tools.pattern_lab.accounts.preparation import align_states
from tools.pattern_lab.manifest import format_epoch_ms

STEP=300000


def series(kind='settledFunding', sid='funding'):
    t=TYPES[kind]
    return dict(seriesId=sid,provider='bybit',providerVersion='v5',venue='BYBIT',contract='1000PEPEUSDT',
        market='linearUSDTPerpetual',baseUnit='1000PEPE',baseMultiplier=1000,interval='5min' if t.kind=='sampled' else None,
        type=t.document(),units={f:'ratePerSettlement' if kind=='settledFunding' else '1000PEPE' for f in t.fields})


def feature(**changes):
    f=dict(featureId='f',bindingId='source',transform='auxiliaryLevel',version='1',field='fundingRate',
           publicationLagMs=10,maxAgeMs=100,lookbackMinutes=None)
    f.update(changes);return f


def test_snapshot_semantic_identity_and_integrity(tmp_path):
    v=content(series(),[dict(observedAtMs=20,fundingRate=0.),dict(observedAtMs=10,fundingRate=-.125)],[[0,50]])
    a=publish_snapshot([v],output_root=tmp_path/'a',provenance={'fetched':'first'})
    b=publish_snapshot([v],output_root=tmp_path/'b',provenance={'fetched':'later'})
    assert a.contentId==b.contentId and a.seal['provenanceId']!=b.seal['provenanceId']
    assert a.series('funding')['rows'][0]['fundingRate']==-.125
    assert inspect_auxiliary(a.root,verify=True)['integrityVerified']
    with pytest.raises(PatternLabDataError):publish_snapshot([v],output_root=a.root)
    (b.root/'extra').write_text('x')
    with pytest.raises(PatternLabDataError,match='member'):load_snapshot(b.root)


def test_consumed_arrays_remain_immutable_after_spawn_transport():
    import pickle
    from tools.pattern_lab.auxiliary.model import ConsumedSlice
    v=content(series(),[dict(observedAtMs=0,fundingRate=None),dict(observedAtMs=20,fundingRate=0)],[[0,50]])
    original=ConsumedSlice.from_content(v);restored=pickle.loads(pickle.dumps(original))
    assert restored.document==v
    for source in (original,restored):
        with pytest.raises(ValueError):source.values[0,0]=1
        with pytest.raises(ValueError):source.observedAtMs[0]=1


def test_funding_gap_lag_and_exclusive_age():
    v=content(series(),[dict(observedAtMs=19,fundingRate=0.),dict(observedAtMs=60,fundingRate=.125)],[[0,20],[50,80]])
    decisions=np.array([18,28,29,30,59,60,69,70,89,90],dtype=np.int64)
    a,meta,_=build_feature(v,feature(),decisions,start=0,end=80)
    assert a.known.tolist()==[False,False,True,False,False,False,False,True,True,False]
    assert a.value[2]==0 and a.validUntilMs[2]==30
    assert meta['rawCoverage']==[[0,20],[50,80]]
    # A future row cannot change any earlier decision facts.
    future=content(series(),v['rows']+[dict(observedAtMs=120,fundingRate=-1)],[[0,20],[50,150]])
    b,_,_=build_feature(future,feature(),decisions[:8],start=0,end=150)
    for name in ('known','value','observedAtMs','availableAtMs'):
        np.testing.assert_array_equal(getattr(a,name)[:8],getattr(b,name))
    stale,_,_=build_feature(content(series(),[dict(observedAtMs=0,fundingRate=1)],[[0,1000]]),feature(),
                           np.array([99,100],dtype=np.int64),start=0,end=1000)
    assert stale.unknownReason.tolist()==[0,3]


def test_oi_missing_slot_recovered_exact_lookback():
    s=series('openInterest','oi')
    rows=[dict(observedAtMs=i*STEP,singleOpenInterest=float(2**i),openInterest=float(2**(i+1))) for i in (0,1,3,4,5)]
    v=content(s,rows,[[0,6*STEP]])
    f=feature(field='singleOpenInterest',transform='auxiliaryChange',lookbackMinutes=10,publicationLagMs=STEP,maxAgeMs=4*STEP)
    a,_,_=build_feature(v,f,np.arange(1,8,dtype=np.int64)*STEP,start=0,end=6*STEP)
    assert a.known.tolist()==[False,False,False,False,False,True,False]
    assert a.value[5]==3
    bad=deepcopy(rows);bad[0]['observedAtMs']=1
    with pytest.raises(PatternLabDataError,match='off-grid'):content(s,bad,[[0,6*STEP]])
    rows[3]['singleOpenInterest']=None
    assert normalize_rows(s,rows)[3]['singleOpenInterest'] is None


def test_unknown_precedence_and_transition_gap():
    v=content(series(),[dict(observedAtMs=20,fundingRate=1)],[[0,20],[20,40]])
    a,_,_=build_feature(v,feature(),np.array([29,30,49,50],dtype=np.int64),start=0,end=40)
    assert a.known.tolist()==[False,True,True,False]
    truth=np.array([True,False,True,True,False,True,False,True])
    known=np.array([True,True,True,False,True,True,True,True])
    segments=np.array([True,False,False,False,False,False,False,True])
    assert transition_pulse(truth,known,segments).tolist()==[False,False,True,False,False,True,False,False]
    assert not condition(np.array([0.,1.]),dict(operator='gt',threshold=1)).any()
    assert not condition(np.array([0.,1.]),None).any()


def test_registered_ratio_and_event_types(tmp_path,monkeypatch):
    ratio=TypeDescriptor('ratio','1','sampled',('ratio',),'Accounts long / accounts short',
        'Observation time','Explicit retrieval spans',('auxiliaryLevel',),True,'accountCount')
    events=TypeDescriptor('liquidations','1','events',('volume',),'Native volume per identified event',
        'Event occurrence','Covered-empty distinct from uncovered',('eventBuckets',),True,'positionSize')
    monkeypatch.setitem(TYPES,'ratio',ratio);monkeypatch.setitem(TYPES,'liquidations',events)
    s=series('ratio','ratio');s['provider']='synthetic'
    v=content(s,[dict(observedAtMs=0,ratio=2.)],[[0,STEP]])
    assert publish_snapshot([v],output_root=tmp_path/'ratio').series('ratio')==v
    a,_,_=build_feature(v,feature(field='ratio'),np.array([10],dtype=np.int64),start=0,end=STEP)
    assert condition(a.value,dict(operator='gt',threshold=1)).tolist()==[True]
    e=series('liquidations','events');e['provider']='synthetic'
    rows=[dict(observedAtMs=9,eventId='1',volume=2),dict(observedAtMs=9,eventId='2',volume=3),dict(observedAtMs=10,eventId='3',volume=4)]
    v=content(e,rows+[rows[0]],[[0,20],[30,40]])
    assert len(publish_snapshot([v],output_root=tmp_path/'events').series('events')['rows'])==3
    with pytest.raises(PatternLabDataError,match='conflicting'):content(e,rows+[dict(rows[0],volume=4)],[[0,40]])
    with pytest.raises(PatternLabDataError,match='conflicting'):content(e,rows+[dict(rows[0],observedAtMs=11)],[[0,40]])
    b=event_buckets(v,field='volume',widthMs=10,start=0,end=40,publicationLagMs=2,maxAgeMs=100)
    a=align_states(b,np.array([11,12,21,22,32,42],dtype=np.int64))
    assert a.known.tolist()==[False,True,True,True,False,True]
    assert a.value[1]==5 and a.value[3]==4 and a.value[-1]==0


class Clock:
    def __init__(self):self.t=0
    def monotonic(self):return self.t
    def sleep(self,n):self.t+=n
    def now_utc(self):return datetime.fromtimestamp(self.t,tz=timezone.utc)


def collection_request(kind='settledFunding',start=0,end=6*STEP,sid='funding'):
    return dict(schema='pattern-lab.auxiliary-collection-request.v1',provider='bybit',series=[dict(
        seriesId=sid,contract='1000PEPEUSDT',typeId=kind,interval='5min' if kind=='openInterest' else None,
        startUtc=format_epoch_ms(start),endUtc=format_epoch_ms(end),tailMarginMs=3600000,baseMultiplier=1000)])


def http_client(pages, *, bad_metadata=False, status=200):
    calls=[];pages=iter(pages)
    def transport(url,timeout):
        path=urlsplit(url).path;params=parse_qs(urlsplit(url).query);calls.append((path,params))
        if path.endswith('/time'):result=dict(timeSecond='100000')
        elif path.endswith('/instruments-info'):
            result=dict(category='linear',list=[dict(symbol='1000PEPEUSDT',contractType='LinearPerpetual',quoteCoin='USDT',
                settleCoin='USDT',status='Settled' if bad_metadata else 'Trading',baseCoin='1000PEPE',launchTime='0',
                lotSizeFilter=dict(qtyStep='1',minOrderQty='1'),priceFilter=dict(tickSize='.0001'))])
        else:
            result=next(pages)
            if isinstance(result,ex.HttpResponse):return result
        return ex.HttpResponse(status,json.dumps(dict(retCode=0,result=result)))
    return ex.HttpClient(transport=transport,clock=Clock(),rates={'BYBIT':2}),calls


def funding_page(*pairs):
    return dict(list=[dict(symbol='1000PEPEUSDT',fundingRateTimestamp=str(t),fundingRate=str(v)) for t,v in pairs])


def test_funding_short_pages_overlap_late_and_immutable_update(tmp_path):
    client,calls=http_client([funding_page((STEP,0)),funding_page()])
    a=collect_auxiliary(collection_request(),output_root=tmp_path/'a',client=client)
    assert len(calls)==4 and calls[-1][1]['endTime']==[str(STEP-1)]
    client,_=http_client([funding_page((5*STEP,.125),(2*STEP,.25),(STEP,0)),funding_page()])
    b=collect_auxiliary(collection_request(end=8*STEP),output_root=tmp_path/'b',previous_root=a.root,client=client)
    assert len(b.series('funding')['rows'])==3 and len(a.series('funding')['rows'])==1
    receipt=json.loads((b.root/'series/funding/receipt.json').read_text())
    assert receipt['lateArrivals']==2
    client,_=http_client([funding_page((STEP,1)),funding_page()])
    with pytest.raises(PatternLabDataError,match='changed or disappearing'):
        collect_auxiliary(collection_request(end=8*STEP),output_root=tmp_path/'bad',previous_root=a.root,client=client)
    assert (tmp_path/'bad/failure.json').exists() and not (tmp_path/'bad/completion.json').exists()


def test_oi_cursor_missing_field_duplicates_and_new_series(tmp_path):
    def row(i,**kw):return dict(timestamp=str(i*STEP),singleOpenInterest='10',openInterest='20',**kw)
    p1=dict(category='linear',symbol='1000PEPEUSDT',list=[row(3),row(2)],nextPageCursor='next')
    r=row(1);del r['singleOpenInterest']
    p2=dict(category='linear',symbol='1000PEPEUSDT',list=[row(2),r],nextPageCursor='')
    client,calls=http_client([p1,p2])
    a=collect_auxiliary(collection_request('openInterest',sid='oi'),output_root=tmp_path/'a',client=client)
    assert a.series('oi')['rows'][0]['singleOpenInterest'] is None
    assert calls[-1][1]['cursor']==['next']
    client,calls=http_client([funding_page((STEP,0)),funding_page()])
    b=collect_auxiliary(collection_request(),output_root=tmp_path/'b',previous_root=a.root,client=client)
    assert b.series('oi')==a.series('oi') and not any(p.endswith('open-interest') for p,_ in calls)


@pytest.mark.parametrize('problem',['metadata','unsafe','retry','conflict','cursor'])
def test_collection_failures_and_single_retry_budget(tmp_path,problem):
    if problem=='retry':pages=[ex.HttpResponse(503,'retry')]*5
    elif problem=='conflict':pages=[funding_page((STEP,0),(STEP,1))]
    elif problem=='cursor':pages=[dict(list=[dict(timestamp=str(STEP),singleOpenInterest='1',openInterest='2')],nextPageCursor='same')]*2
    else:pages=[]
    client,calls=http_client(pages,bad_metadata=problem=='metadata')
    r=collection_request('openInterest' if problem=='cursor' else 'settledFunding')
    if problem=='unsafe':r['series'][0]['endUtc']=format_epoch_ms(100000000)
    with pytest.raises(PatternLabDataError):collect_auxiliary(r,output_root=tmp_path/'out',client=client)
    assert not (tmp_path/'out/completion.json').exists()
    if problem=='unsafe':assert not (tmp_path/'out').exists() and len(calls)==1
    if problem=='retry':assert len(calls)==7


def test_noop_disappearing_nullable_change_and_required_overlap(tmp_path):
    client,_=http_client([funding_page((STEP,0)),funding_page()])
    a=collect_auxiliary(collection_request(),output_root=tmp_path/'a',client=client)
    client,_=http_client([funding_page((STEP,0)),funding_page()]);client.clock.t=100
    b=collect_auxiliary(collection_request(),output_root=tmp_path/'b',previous_root=a.root,client=client)
    assert a.contentId==b.contentId and a.seal['provenanceId']!=b.seal['provenanceId']
    client,calls=http_client([])
    with pytest.raises(PatternLabDataError,match='overlap'):
        collect_auxiliary(collection_request(start=STEP,end=8*STEP),output_root=tmp_path/'skip',previous_root=a.root,client=client)
    assert not calls and not (tmp_path/'skip').exists()
    client,_=http_client([funding_page()])
    with pytest.raises(PatternLabDataError,match='disappearing'):
        collect_auxiliary(collection_request(),output_root=tmp_path/'gone',previous_root=a.root,client=client)
    row=dict(timestamp=str(STEP),openInterest='20')
    client,_=http_client([dict(list=[row])])
    old=collect_auxiliary(collection_request('openInterest',sid='oi'),output_root=tmp_path/'oi',client=client)
    client,_=http_client([dict(list=[dict(row,singleOpenInterest='10')])])
    with pytest.raises(PatternLabDataError,match='changed'):
        collect_auxiliary(collection_request('openInterest',sid='oi'),output_root=tmp_path/'filled',previous_root=old.root,client=client)


def test_retry_success_permanent_failure_malformed_and_tail_floor(tmp_path):
    client,calls=http_client([ex.HttpResponse(429,'limited',{'Retry-After':'1'}),funding_page((STEP,0)),funding_page()])
    a=collect_auxiliary(collection_request(),output_root=tmp_path/'a',client=client)
    receipt=json.loads((a.root/'series/funding/receipt.json').read_text())
    assert len(receipt['attempts'])==4 and receipt['attempts'][1]['retryable']
    client,calls=http_client([ex.HttpResponse(401,'denied')])
    with pytest.raises(PatternLabDataError):collect_auxiliary(collection_request(),output_root=tmp_path/'denied',client=client)
    assert len(calls)==3
    for n,bad in enumerate(('NaN','Inf','broken')):
        client,_=http_client([funding_page((STEP,bad))])
        with pytest.raises(PatternLabDataError):collect_auxiliary(collection_request(),output_root=tmp_path/str(n),client=client)
    request=collection_request();request['series'][0]['tailMarginMs']=3599999
    client,calls=http_client([])
    with pytest.raises(PatternLabDataError):collect_auxiliary(request,output_root=tmp_path/'margin',client=client)
    assert not calls and not (tmp_path/'margin').exists()


def test_alternative_provider_registration_without_executor_branches(tmp_path,monkeypatch):
    from tools.pattern_lab.auxiliary.collect import PROVIDERS
    class Alternative:
        def server_time_ms(self,client):return 100000000
        def fetch(self,request,client):
            s=series();s.update(provider='alternative',venue='SYNTHETIC')
            return content(s,[dict(observedAtMs=STEP,fundingRate=.125)],[[0,6*STEP]]),dict(origin='synthetic adapter')
    monkeypatch.setitem(PROVIDERS,'alternative',Alternative)
    request=collection_request();request['provider']='alternative'
    client,calls=http_client([])
    result=collect_auxiliary(request,output_root=tmp_path/'other',client=client)
    assert not calls
    v=result.series('funding')
    a,_,_=build_feature(v,feature(publicationLagMs=10,maxAgeMs=STEP),np.array([STEP+10],dtype=np.int64),start=0,end=6*STEP)
    assert a.known.tolist()==[True] and a.value.tolist()==[.125]


@pytest.mark.parametrize('interval,step',[('5min',300000),('15min',900000),('30min',1800000),('1h',3600000),('4h',14400000),('1d',86400000)])
def test_declared_oi_interval_epoch_phase(interval,step):
    s=series('openInterest','oi');s['interval']=interval
    assert content(s,[dict(observedAtMs=step,singleOpenInterest=0,openInterest=0)],[[0,2*step]])['rows'][0]['singleOpenInterest']==0
    with pytest.raises(PatternLabDataError,match='off-grid'):
        content(s,[dict(observedAtMs=step+1,singleOpenInterest=0,openInterest=0)],[[0,2*step]])
