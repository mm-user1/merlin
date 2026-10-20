"""Cutoff-only auxiliary ownership, coverage, frozen contracts and pool admission."""
from copy import deepcopy
from dataclasses import replace
import numpy as np
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.auxiliary.model import TYPES,content
from tools.pattern_lab.auxiliary.features import slice_content
from tools.pattern_lab.selection.auxiliary import cell,envelope,AuxiliaryView,prepare_batch
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
    diag,state=call(args);assert not diag['available'] and diag['reason']==state['reason']
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
    from tools.pattern_lab.selection.ledger import history_pool
    pool,_=history_pool(ledger,['AAA','BBB'],history,99.)
    prepared=prepare_batch(doc,dict(auxiliaryUses=[use]),pool,history,cutoff,{'aux':snapshot},pb,algorithm='monthly-selection.3')
    assert [d['instrumentId'] for d in prepared[0]]==['AAA']
    snapshot.value['coverage']=[]
    with pytest.raises(PatternLabDataError,match='unavailable'):prepare_batch(doc,dict(auxiliaryUses=[use]),pool,history,cutoff,{'aux':snapshot},pb,algorithm='monthly-selection.3')


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


@pytest.mark.parametrize('algorithm,available',[('monthly-selection.2',False),('monthly-selection.3',True)])
def test_known_feature_with_older_incomplete_envelope(algorithm,available):
    s,b,u,h,d,p=fixture(policy='allowUnknown')
    s.value['rows']=[r for r in s.value['rows'] if r['observedAtMs']!=d-2400000]
    diag,state=cell(s,b,'AAA',u,h,d,p,algorithm=algorithm)
    assert state['known'] and not diag['complete'] and diag['available'] is available
    assert diag['reason']==(None if available else 'incompleteCoverage')
    b['coveragePolicy']='requireComplete'
    with pytest.raises(PatternLabDataError,match='unavailable'):cell(s,b,'AAA',u,h,d,p,algorithm=algorithm)


def test_cell_lookup_and_fingerprint_do_not_parse_other_tickers(monkeypatch):
    from tools.pattern_lab.selection import auxiliary as aux
    s,b,u,h,d,p=fixture('settledFunding',access='raw');_,raw=call((s,b,u,h,d,p))
    price=ScoreView(['AAA','BBB'],h,d,[])
    view=AuxiliaryView(price,{i+'/u':dict(access='raw',value=dict(raw,marker=i)) for i in price.instruments})
    parsed=[];original=aux.parse_json
    def parse(text):parsed.append(text);return original(text)
    monkeypatch.setattr(aux,'parse_json',parse)
    before=view.fingerprint
    for _ in range(3):
        value=view.auxiliaryRaw('AAA','u')
        with pytest.raises(TypeError):value['marker']='mutated'
        assert view.fingerprint==before
    assert len(parsed)==3 and all('BBB' not in t for t in parsed)
    raw['rows'].append(dict(observedAtMs=d,fundingRate=99.))
    assert not view.auxiliaryRaw('BBB','u')['rows'] and view.fingerprint==before
    with pytest.raises(TypeError):view._cells['AAA/u']='{}'
    # Native bar protection is still called by the auxiliary fingerprint.
    from tools.pattern_lab.selection.ledger import NativeBars
    price._bars['AAA']=(np.array([1],np.int64).tobytes(),np.ones((1,5)).tobytes(),1,[])
    price.bars('AAA')
    price._cache['AAA']=NativeBars(np.array([2]),np.ones((1,5)),1,())
    with pytest.raises(PatternLabDataError,match='mutated native'):view.fingerprint


def test_batches_released_during_generation_and_deep_recomputation(example,tmp_path,monkeypatch):
    import weakref
    from tools.pattern_lab.selection import scores_v2,verify_selection
    _,parent,snapshot,bindings,_=example;original=scores_v2.prepare_batch;refs=[];calls=[]
    class Owned(dict):pass
    def bounded(*args,**kwargs):
        assert all(ref() is None for ref in refs),'previous raw selector-month retained'
        diagnostics,values=original(*args,**kwargs);owned=Owned(values)
        refs.append(weakref.ref(owned));calls.append((args[4],len(values)))
        return diagnostics,owned
    monkeypatch.setattr(scores_v2,'prepare_batch',bounded)
    r=run_selection(request(parent,bindings,'carried'),parent_root=parent.root,output_root=tmp_path/'bounded',
        scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    before=len(calls)
    result=verify_selection(r.root,parent_root=parent.root,scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root},require_scorers=True)
    assert result['auxiliaryRecomputed'] and len(calls)==2*before and all(ref() is None for ref in refs)
    assert len({cutoff for cutoff,n in calls if n})==3


def test_late_required_refusal_precedes_every_scorer_and_output(example,tmp_path,monkeypatch):
    from tools.pattern_lab.selection import scores_v2
    _,parent,snapshot,bindings,_=example;original=scores_v2.prepare_batch
    def refuse(*args,**kwargs):
        if args[4]==month_bounds('2020-07')[0] and args[1]['auxiliaryUses']:
            raise PatternLabDataError('required later cell unavailable')
        return original(*args,**kwargs)
    def deny(*a,**k):raise AssertionError('scorer called before required validation completed')
    monkeypatch.setattr(scores_v2,'prepare_batch',refuse);monkeypatch.setattr(scores_v2,'evaluate',deny)
    target=tmp_path/'late-refusal'
    with pytest.raises(PatternLabDataError,match='later cell'):run_selection(request(parent,bindings,'carried'),parent_root=parent.root,
        output_root=target,scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    assert not target.exists()


def test_early_envelope_and_dimension_refusals_without_materialization(example,tmp_path,monkeypatch):
    from tools.pattern_lab.selection import scores_v2
    from tools.pattern_lab.selection.capacity_v2 import auxiliary_plan
    _,parent,snapshot,bindings,readers=example;r=readers['carried'];doc=r.selection['request']
    contracts={b['selectorId']:b['contract'] for b in r.scores}
    plan=auxiliary_plan(doc,contracts,r.ledger,r.selection['roster'],{'aux':snapshot},r.selection['parentBounds'],set(contracts))
    assert plan['largestBatchCells']==12 and plan['liveBatchBytesEstimate']>0
    def deny(*a,**k):raise AssertionError('raw cells materialized before early planning')
    monkeypatch.setattr(scores_v2,'prepare_batch',deny)
    scorer=replace(SCORER,auxiliaryUses=lambda p:[dict(u,antecedentMs=10**15) for u in SCORER.auxiliaryUses(p)])
    with pytest.raises(PatternLabDataError,match='outside parent'):run_selection(request(parent,bindings,'carried'),parent_root=parent.root,
        output_root=tmp_path/'bad-envelope',scorers={scorer.scorerId:scorer},auxiliary_roots={'aux':snapshot.root})
    assert not (tmp_path/'bad-envelope').exists()
    tiny_step=[dict(row,stepMs=1) for row in r.ledger]
    with pytest.raises(PatternLabDataError,match='early selection dimensions'):
        auxiliary_plan(doc,contracts,tiny_step,r.selection['roster'],{'aux':snapshot},r.selection['parentBounds'],set())


def test_stored_only_auxiliary_never_builds_cells_or_claims_recomputation(example,monkeypatch):
    from tools.pattern_lab.selection import auxiliary,verify_selection
    _,parent,snapshot,_,readers=example
    def deny(*a,**k):raise AssertionError('unused raw cell constructed')
    monkeypatch.setattr(auxiliary,'cell',deny)
    result=verify_selection(readers['carried'].root,parent_root=parent.root,auxiliary_roots={'aux':snapshot.root})
    assert result['scorers']=={'consistency':'recomputed','oi':'storedOutputsOnly'}
    assert result['auxiliaryRecomputed'] is False
    with pytest.raises(PatternLabDataError,match='dependencies missing'):
        verify_selection(readers['carried'].root,parent_root=parent.root,auxiliary_roots={'aux':snapshot.root},require_scorers=True)


def test_no_auxiliary_flag_and_derived_dependencies(example,tmp_path):
    from tools.pattern_lab.selection import verify_selection,export_selection_bundle
    from tools.pattern_lab.selection.dependencies_v2 import description
    _,parent,snapshot,bindings,readers=example
    doc=request(parent,[],'carried');doc['selectors']=doc['selectors'][:1]
    r=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'no-aux')
    assert verify_selection(r.root,parent_root=parent.root,auxiliary_roots={})['auxiliaryRecomputed'] is False
    reader=readers['carried'];before=reader.summary
    exported=export_selection_bundle(reader.root,zip_path=tmp_path/'export.zip')
    deps=exported['dependencies'];assert deps==description(reader.summary)
    assert deps['exactParent']['pins']==reader.selection['request']['parent']
    assert deps['auxiliaryRecomputation']['snapshots']==[dict(snapshotId='aux',contentId=snapshot.contentId)]
    assert deps['auxiliaryRecomputation']['experimentalSources']==reader.selection['implementation']['experimental']
    html=(reader.root/'derived/report.html').read_text(encoding='utf-8').split('<script')[0]
    assert 'Even one unselected member' in html and 'across every K and all controls' in html
    assert snapshot.contentId in html and 'not included by selection export' in html
    assert reader.summary==before and 'dependencies' not in before


def test_corrected_feature_pool_controls_and_explicit_algorithm_validation(example,tmp_path):
    from tools.pattern_lab.selection import verify_selection,load_selection
    from tools.pattern_lab.selection.scores_v2 import validate
    from tools.pattern_lab.account_study.common import read_json,write_json
    from tools.pattern_lab.selection.artifacts import seal_directory
    _,parent,snapshot,bindings,_=example;values=[];cutoff=month_bounds('2020-05')[0]
    for entry in snapshot.manifest['series']:
        v=snapshot.series(entry['seriesId'])
        if entry['seriesId']=='oi0':v['rows']=[r for r in v['rows'] if r['observedAtMs']!=cutoff-2*86400000]
        values.append(v)
    altered=publish_snapshot(values,output_root=tmp_path/'older-gap');doc=request(parent,deepcopy(bindings),'carried')
    for b in doc['auxiliaryInputs']:b.update(expectedSnapshotContentId=altered.contentId,coveragePolicy='allowUnknown')
    r=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'new',scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':altered.root})
    assert r.selection['implementation']['algorithm']=='monthly-selection.3'
    batch=next(b for b in r.scores if b['selectorId']=='oi' and b['month']=='2020-05')
    diag=next(d for d in batch['auxiliaryDependencies'] if d['instrumentId']=='BYBIT_AAAUSDT' and d['useId']=='oiChange')
    assert diag['available'] and not diag['complete'] and diag['featureState']['known'] and diag['reason'] is None
    assert next(row for row in batch['rows'] if row['instrumentId']=='BYBIT_AAAUSDT')['eligible']
    for c in r.results['comparisons']:
        if c['selectorId']=='oi':assert 'BYBIT_AAAUSDT' in c['months'][0]['allEligible']['members']
    assert any(row['trades']>0 for rows in r._funded.values() for row in rows)
    assert load_selection(r.root).results==r.results
    assert verify_selection(r.root,parent_root=parent.root,scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':altered.root},require_scorers=True)['auxiliaryRecomputed']
    with pytest.raises(PatternLabDataError,match='availability'):
        validate(r.selection['request'],r.ledger,r.selection['roster'],r.scores,r.selection['parentBounds'],algorithm='monthly-selection.2')
    wrong=r.scores
    b=next(b for b in wrong if b['selectorId']=='oi' and b['month']=='2020-05')
    d=next(d for d in b['auxiliaryDependencies'] if d['instrumentId']=='BYBIT_AAAUSDT' and d['useId']=='oiChange')
    d.update(available=False,reason='incompleteCoverage')
    with pytest.raises(PatternLabDataError,match='availability'):
        validate(r.selection['request'],r.ledger,r.selection['roster'],wrong,r.selection['parentBounds'],algorithm='monthly-selection.3')
    meta=read_json(r.root/'selection.json');meta['implementation']['algorithm']='monthly-selection.999'
    write_json(r.root/'selection.json',meta);seal_directory(r.root,2)
    with pytest.raises(PatternLabDataError,match='unknown selection algorithm'):load_selection(r.root)


def test_empty_common_pool_has_no_auxiliary_recomputation(tmp_path,monkeypatch):
    from tools.pattern_lab.examples.selection_synthetic import build_inputs
    from tools.pattern_lab.examples.selection_carried_synthetic import build_snapshots
    from tools.pattern_lab.account_study import run_account_study
    from tools.pattern_lab.selection import verify_selection,auxiliary
    # Genuine parent with a May gap, only that target and July evaluation. May is
    # required price history at a 100% threshold, so the whole common pool is empty.
    raw=build_inputs(tmp_path/'pack',gaps=True);raw['instruments']=raw['instruments'][:1]
    parent=run_account_study(raw,data_root=tmp_path/'pack',output_root=tmp_path/'parent')
    snapshot,bindings=build_snapshots(parent,tmp_path/'snapshot');doc=request(parent,bindings,'carried')
    doc['evaluationMonths']=['2020-07'];doc['pool']['minObservedBarsPct']=100.
    def deny(*a,**k):raise AssertionError('empty common pool constructed auxiliary cells')
    monkeypatch.setattr(auxiliary,'cell',deny)
    r=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'empty',scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    assert all(not b['commonPool'] and not b['auxiliaryDependencies'] for b in r.scores)
    check=verify_selection(r.root,parent_root=parent.root,scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root},require_scorers=True)
    assert check['scorers']['oi']=='recomputed' and check['auxiliaryRecomputed'] is False


def test_mixed_auxiliary_selector_recomputation_levels(example,tmp_path):
    from tools.pattern_lab.selection import verify_selection
    _,parent,snapshot,bindings,_=example
    source=tmp_path/'second.py';source.write_text('# explicitly pinned second experimental scorer\n')
    second=replace(SCORER,scorerId='x.second',sourceFiles={'second.py':source})
    doc=request(parent,bindings,'carried')
    doc['selectors'].append(dict(selectorId='second',scorerId=second.scorerId,version=second.version,parameters={}))
    r=run_selection(doc,parent_root=parent.root,output_root=tmp_path/'mixed',scorers={SCORER.scorerId:SCORER,second.scorerId:second},auxiliary_roots={'aux':snapshot.root})
    check=verify_selection(r.root,parent_root=parent.root,scorers={SCORER.scorerId:SCORER},auxiliary_roots={'aux':snapshot.root})
    assert check['auxiliaryRecomputed'] is True
    assert check['scorers']=={'consistency':'recomputed','oi':'recomputed','second':'storedOutputsOnly'}
