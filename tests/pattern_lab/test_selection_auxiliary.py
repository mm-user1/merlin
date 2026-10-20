"""Cutoff-only auxiliary ownership, coverage, frozen contracts and pool admission."""
from copy import deepcopy
from dataclasses import replace
import numpy as np
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.auxiliary.model import TYPES,content
from tools.pattern_lab.auxiliary.features import slice_content
from tools.pattern_lab.selection.auxiliary import cell,envelope,AuxiliaryView,prepare_cells
from tools.pattern_lab.selection.request_v2 import normalize_uses
from tools.pattern_lab.selection.ledger import ScoreView
from tools.pattern_lab.selection.scorers import evaluate
from tools.pattern_lab.selection.request import month_bounds
from tools.pattern_lab.selection import normalize_selection_request,run_selection
from tools.pattern_lab.examples.selection_auxiliary_scorer import SCORER
from tools.pattern_lab.examples.selection_carried_synthetic import request
from tools.pattern_lab.auxiliary import publish_snapshot
from .test_selection_carried import example


class Snapshot:
    def __init__(self,value):self.value=value
    def slice(self,sid,start,end):return slice_content(self.value,start,end)


def fixture(kind='openInterest',policy='requireComplete',access='feature'):
    cutoff=month_bounds('2020-05')[0];start=month_bounds('2020-01')[0];step=300000
    t=TYPES[kind];oi=kind=='openInterest';field='singleOpenInterest' if oi else 'fundingRate'
    series=dict(seriesId='s',provider='bybit',providerVersion='v5',venue='BYBIT',contract='1000PEPEUSDT',
        market='linearUSDTPerpetual',baseUnit='1000PEPE',baseMultiplier=1000,interval='5min' if oi else None,
        type=t.document(),units={f:'1000PEPE' if oi else 'ratePerSettlement' for f in t.fields})
    source={k:series[k] for k in ('seriesId','provider','venue','contract','interval','baseUnit','baseMultiplier')}
    source.update(typeId=kind,field=field,unit=series['units'][field])
    binding=dict(inputId='b',snapshotId='aux',expectedSnapshotContentId='0'*64,coveragePolicy=policy,sources={'*':source})
    use=dict(useId='u',bindingId='b',access=access,publicationLagMs=60000,antecedentMs=0)
    if access=='feature':use['feature']=dict(featureId='f',bindingId='b',transform='auxiliaryChange' if oi else 'auxiliaryLevel',
        version='1',field=field,publicationLagMs=60000,maxAgeMs=3600000,lookbackMinutes=10 if oi else None)
    rows=[dict(observedAtMs=stamp,singleOpenInterest=128.+n,openInterest=256.+2*n) for n,stamp in enumerate(range(cutoff-7200000,cutoff+step,step))] if oi else []
    value=content(series,rows,[[start,cutoff+86400000]])
    return Snapshot(value),binding,use,['2020-01','2020-02','2020-03','2020-04'],cutoff,[start-86400000,start,cutoff+86400000]


def call(args):return cell(args[0],args[1],'AAA',*args[2:])


def test_exact_feature_footprint_lag_future_prefix_and_owned_view():
    args=fixture();diagnostic,state=call(args)
    assert diagnostic['startMs']==args[4]-(3600000+600000+300000+60000)
    assert diagnostic['endMs']==args[4]-60000+1 and state['known'] and state['availableAtMs']<=args[4]
    assert diagnostic['source']['unit']=='1000PEPE' and diagnostic['source']['baseMultiplier']==1000
    changed=deepcopy(args);changed[0].value['rows'][-1]['singleOpenInterest']=999999.
    changed[0].value['coverage'][0][1]+=86400000
    assert call(changed)==(diagnostic,state)
    price=ScoreView(['AAA'],args[3],args[4],[]);view=AuxiliaryView(price,{'AAA/u':dict(access='feature',value=state)})
    with pytest.raises(TypeError):view.auxiliaryFeature('AAA','u')['value']=0
    with pytest.raises(PatternLabDataError):view.auxiliaryRaw('AAA','u')
    assert not any(hasattr(view,n) for n in ('root','snapshot','reader','contentId'))
    before=view.fingerprint;state['value']=999.;assert view.fingerprint==before


def test_raw_inclusive_publication_cutoff_empty_events_and_clipped_coverage():
    args=fixture('settledFunding',access='raw');s,b,u,h,d,p=args
    s.value['rows']=[dict(observedAtMs=d-60000,fundingRate=.1),dict(observedAtMs=d-59999,fundingRate=.2)]
    diag,raw=call(args);assert len(raw['rows'])==1 and raw['rows'][0]['fundingRate']==.1
    assert raw['coverage']==[[month_bounds(h[0])[0],d-60000+1]]
    s.value['rows']=[];assert call(args)[0]['available']
    s.value['coverage']=[]
    with pytest.raises(PatternLabDataError,match='unavailable'):call(args)
    b['coveragePolicy']='allowUnknown';assert call(args)[0]['reason']=='incompleteCoverage'


@pytest.mark.parametrize('change',['missing','null','stale','edge'])
def test_unknown_wins_and_require_complete(change):
    args=fixture(policy='allowUnknown');s,b,u,h,d,p=args
    if change=='missing':s.value['rows']=[r for r in s.value['rows'] if r['observedAtMs']!=d-600000]
    if change=='null':s.value['rows'][-2]['singleOpenInterest']=None
    if change=='stale':
        args=fixture('settledFunding',policy='allowUnknown');s,b,u,h,d,p=args
        s.value['rows']=[dict(observedAtMs=d-7200000,fundingRate=.1)]
    if change=='edge':s.value['coverage'][0][1]=d-60000
    diag,state=call(args);assert not diag['available']
    b['coveragePolicy']='requireComplete'
    with pytest.raises(PatternLabDataError,match='unavailable'):call(args)


@pytest.mark.parametrize('mutation',['negative','extra','binding','lag','field','rawFeature','unused','warmup'])
def test_auxiliary_use_and_bounds_refusals(mutation):
    args=fixture();_,binding,use,history,cutoff,pb=args;doc=dict(auxiliaryInputs=[binding])
    if mutation=='negative':use['antecedentMs']=-1
    elif mutation=='extra':use['future']=True
    elif mutation=='binding':use['feature']['bindingId']='wrong'
    elif mutation=='lag':use['feature']['publicationLagMs']+=1
    elif mutation=='field':use['feature']['field']='openInterest'
    elif mutation=='rawFeature':use['access']='raw'
    elif mutation=='unused':use['access']='unknown'
    else:
        pb[0]=cutoff-1000
        with pytest.raises(PatternLabDataError,match='outside parent'):call(args)
        return
    with pytest.raises(PatternLabDataError):normalize_uses([use],doc)


def test_required_cells_skip_price_excluded_ticker_and_reuse_shared_source():
    args=fixture('settledFunding',access='raw');snapshot,binding,use,history,cutoff,pb=args
    ledger=[dict(instrumentId=i,month=m,available=i=='AAA',terminalStatus='closed' if i=='AAA' else 'noObservedBars',observedBars=1 if i=='AAA' else 0,expectedBars=1) for m in history for i in ('AAA','BBB')]
    doc=dict(auxiliaryInputs=[binding],monthWindows=[dict(month=m) for m in history+['2020-05']],evaluationMonths=['2020-05'],selectors=[dict(selectorId='s')],pool=dict(historyMonths=4,minObservedBarsPct=99.))
    prepared=prepare_cells(doc,{'s':dict(lookbackMonths=4,auxiliaryUses=[use])},ledger,['AAA','BBB'],{'aux':snapshot},pb)
    assert [d['instrumentId'] for d in prepared['s','2020-05'][0]]==['AAA']
    snapshot.value['coverage']=[]
    with pytest.raises(PatternLabDataError,match='unavailable'):prepare_cells(doc,{'s':dict(lookbackMonths=4,auxiliaryUses=[use])},ledger,['AAA','BBB'],{'aux':snapshot},pb)


def test_allow_unknown_coordinator_excludes_and_mapping_pins_fail(example,tmp_path):
    _,parent,snapshot,bindings,_=example
    values=[];cutoff=month_bounds('2020-05')[0]
    for entry in snapshot.manifest['series']:
        sid=entry['seriesId'];v=snapshot.slice(sid,month_bounds('2020-01')[0]-2*86400000,month_bounds('2020-07')[1])
        if sid=='oi0':v['rows']=[r for r in v['rows'] if r['observedAtMs']!=cutoff-14400000]
        values.append(v)
    altered=publish_snapshot(values,output_root=tmp_path/'snapshot')
    doc=request(parent,deepcopy(bindings),'carried')
    for b in doc['auxiliaryInputs']:b.update(expectedSnapshotContentId=altered.contentId,coveragePolicy='allowUnknown')
    reader=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'selection',scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':altered.root})
    batch=next(b for b in reader.scores if b['selectorId']=='oi' and b['month']=='2020-05')
    row=next(r for r in batch['rows'] if r['instrumentId']=='BYBIT_AAAUSDT')
    assert not row['eligible'] and row['reason']=='auxiliaryUnavailable'
    with pytest.raises(PatternLabDataError,match='content pin'):run_selection(doc,parent_root=parent.root,output_root=tmp_path/'wrong',scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    with pytest.raises(PatternLabDataError,match='mapping'):run_selection(doc,parent_root=parent.root,output_root=tmp_path/'missing',scorers={SCORER.scorerId:SCORER},auxiliary_roots={})


def test_cross_month_auxiliary_contract_change_refused(example,tmp_path):
    _,parent,snapshot,bindings,_=example;calls=[]
    def uses(p):
        value=SCORER.auxiliaryUses(p);calls.append(1)
        if len(calls)>1:value[0]['antecedentMs']=1
        return value
    scorer=replace(SCORER,auxiliaryUses=uses)
    with pytest.raises(PatternLabDataError,match='contract changed across months'):run_selection(request(parent,bindings,'carried'),parent_root=parent.root,
        output_root=tmp_path/'changed',scorers={scorer.scorerId:scorer},auxiliary_roots={'aux':snapshot.root})
