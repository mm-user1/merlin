import pytest
from tools.pattern_lab.strategy_screen.facts import local_dd,analytical_month,trade_month
from tools.pattern_lab.strategy_screen import runner
from ._strategy_screen_helpers import run
from ._strategy_screen_helpers import request,build_inputs,results


@pytest.fixture(scope='module')
def native_records(tmp_path_factory):
    from ._strategy_screen_helpers import capture_native
    root=tmp_path_factory.mktemp('screen-native-declarations')
    return [capture_native(root/name,auxiliary=aux) for name,aux in [('v1',False),('v2',True)]]


def test_native_payload_equations_and_renamed_labels(native_records):
    from copy import deepcopy
    from dataclasses import replace
    from tools.pattern_lab.account_study.codec import preparation_facts
    from tools.pattern_lab.accounts.execution import account_identity
    from tools.pattern_lab.study.contracts import semantic_digest
    from tools.pattern_lab.strategy_screen.facts import extract
    for _,records in native_records:
        for record,c,p,child in records:
            r=record.result
            assert semantic_digest(preparation_facts(record.prepared))==r.preparationId
            assert account_identity(record.prepared,r.commands,r.policy,r.window,r.rules)==r.identity
            renamed=deepcopy(child)
            next(a for a in renamed['allocations'] if a['allocationId']==c['allocationId'])['allocationId']='renamed'
            unit=extract(replace(record,labels=dict(record.labels,allocationId='renamed')),dict(c,allocationId='renamed'),p,renamed)
            assert unit['execution']['accountId']==r.identity


@pytest.mark.parametrize('mutation',['policy','feature','fingerprint','header','preparation','rr3ToRr4'])
def test_native_compact_identity_refusal(native_records,mutation):
    from copy import deepcopy
    from tools.pattern_lab import PatternLabDataError
    from tools.pattern_lab.strategy_screen.facts import extract,validate
    records=native_records[0][1];record,c,p,child=next(v for v in records if v[1]['allocationId']=='rr4')
    unit=extract(record,c,p,child);child=deepcopy(child)
    if mutation=='policy':next(a for a in child['allocations'] if a['allocationId']=='rr4')['policy']['riskPct']+=1
    elif mutation=='feature':next(a for a in child['allocations'] if a['allocationId']=='rr4')['features'][0]['priorBars']+=1
    elif mutation=='fingerprint':unit['execution']['inputs'][0]['inputFingerprint']='0'*64
    elif mutation=='header':unit['execution']['inputs'][0]['header']['contract']='OTHER'
    elif mutation=='preparation':unit['execution']['preparationId']='0'*64
    else:
        other=next(v for v in records if v[1]['allocationId']=='rr3' and v[2]['periodId']==p['periodId'])
        unit['execution']=extract(*other)['execution']
    with pytest.raises(PatternLabDataError,match='identity|rules|fingerprint.header'):validate(unit,c,p,child,synthetic=False)


@pytest.mark.parametrize('mutation',['haRecipe','filter','policy','features','preparedLink','commandLink',
    'auxSource','auxCoverage','auxRoster','auxProvider','auxFeature','auxComponent','emptyV2Provider'])
def test_live_declaration_refusal(native_records,mutation):
    from copy import deepcopy
    from dataclasses import replace
    import json
    from tools.pattern_lab import PatternLabDataError
    from tools.pattern_lab.strategy_screen.facts import extract
    v2=mutation.startswith('aux') or mutation in ('emptyV2Provider','filter')
    aid='rr7' if mutation.startswith('aux') else 'rr6' if mutation=='filter' else 'rr2'
    record,c,p,child=next(v for v in native_records[int(v2)][1] if v[1]['allocationId']==aid)
    r=record.result
    if mutation=='haRecipe':
        from tools.pattern_lab.accounts import build_commands,HARecipe
        a=next(a for a in child['allocations'] if a['allocationId']==aid)
        recipe=dict(a['provider']['recipe']);recipe['entryRunLength']+=1
        commands=build_commands(record.prepared,HARecipe(**recipe),mode=r.policy.mode,stopUpdates=r.policy.stopUpdates)
        record=replace(record,result=replace(r,commands=commands,commandId=commands.identity))
    elif mutation=='filter':record=replace(record,result=replace(r,commands=replace(r.commands,entryFilter=None)))
    elif mutation=='policy':record=replace(record,result=replace(r,policy=replace(r.policy,riskPct=r.policy.riskPct+1)))
    elif mutation=='features':record=replace(record,prepared=replace(record.prepared,features=()))
    elif mutation=='preparedLink':record=replace(record,prepared=replace(record.prepared,identity='0'*64))
    elif mutation=='commandLink':record=replace(record,result=replace(r,commandId='0'*64))
    else:
        evidence=deepcopy(record.auxiliaryEvidence);meta=evidence['decisions']['metadata'];deps=evidence['dependencies']
        if mutation=='auxSource':next(iter(deps.values()))['content']['series']['seriesId']='WRONG'
        elif mutation=='auxCoverage':next(iter(deps.values()))['coveragePolicy']='requireComplete'
        elif mutation=='auxRoster':deps.clear()
        elif mutation in ('auxProvider','emptyV2Provider'):meta['provider']['providerId']='wrong'
        elif mutation=='auxFeature':next(iter(meta['features'].values()))['metadata']['feature']['maxAgeMs']+=1
        else:meta['components'].append(dict(metadata=dict(component={'componentId':'invented'})))
        record=replace(record,auxiliaryEvidence=evidence)
    with pytest.raises(PatternLabDataError):extract(record,c,p,child)


@pytest.mark.parametrize('mutation',['terminalMonth','terminalFee','ddFloor','lastSample','tailSlots','lastGrid'])
def test_retained_monthly_contradictions(native_records,mutation):
    from tools.pattern_lab import PatternLabDataError
    from tools.pattern_lab.strategy_screen.facts import extract,validate
    args=next(v for v in native_records[0][1] if v[1]['allocationId']=='rr4' and v[2]['periodId']=='recent')
    unit=extract(*args);months=unit['months']
    if mutation in ('terminalMonth','terminalFee'):
        keys=('terminalTradeCount','terminalExitFees') if mutation=='terminalMonth' else ('terminalExitFees',)
        assert months[-1]['terminalTradeCount'] and months[-1]['terminalExitFees']>0
        for key in keys:months[0][key],months[-1][key]=months[-1][key],0
    elif mutation=='ddFloor':
        assert months[1]['closingEquity']<months[1]['openingEquity'] and months[1]['carriedPeakMaxDdPct']<unit['metrics']['mtmMaxDdPct']
        months[1]['carriedPeakMaxDdPct']=0.
    elif mutation=='lastSample':months[-1]['closingSampleMs']-=86400000
    elif mutation=='tailSlots':unit['metrics']['coverage']['missingTailSlots']+=1
    else:unit['metrics']['coverage']['lastObservedCloseMs']-=1
    with pytest.raises(PatternLabDataError):validate(unit,*args[1:],synthetic=False)


def test_v1_filter_provenance_matches_declared_reference(native_records):
    from copy import deepcopy
    from dataclasses import replace
    from tools.pattern_lab import PatternLabDataError
    from tools.pattern_lab.accounts import build_commands,candle_filter,HARecipe,execute_account
    from tools.pattern_lab.strategy_screen.facts import extract
    record,c,p,child=next(v for v in native_records[1][1] if v[1]['allocationId']=='rr6')
    child=deepcopy(child);child.pop('auxiliaryInputs');child['schema']='pattern-lab.account-study-request.v1'
    a=next(a for a in child['allocations'] if a['allocationId']=='rr6');a.pop('auxiliaryFeatures');a['entryFilter'].pop('componentId')
    child['allocations']=[a];r=record.result
    commands=build_commands(record.prepared,HARecipe(**a['provider']['recipe']),mode=r.policy.mode,
        stopUpdates=r.policy.stopUpdates,entryFilter=candle_filter(record.prepared,'btc'))
    result=execute_account(record.prepared,commands,r.policy,r.window,r.rules)
    record=replace(record,result=result,auxiliaryEvidence=None)
    assert extract(record,c,p,child)['execution']['accountId']==result.identity
    child['allocations'][0]['entryFilter']['requestId']='ha'
    with pytest.raises(PatternLabDataError,match='filter declaration'):extract(record,c,p,child)


@pytest.mark.parametrize('field,value',[('contract','WRONG'),('quantity_step','3'),('minimum_lots',2),
    ('enforce_minimum_quantity',False),('quantity_unit','contracts'),('settlement_currency','USD')])
def test_recorded_rule_semantics_even_with_recomputed_account_id(native_records,field,value):
    from tools.pattern_lab import PatternLabDataError
    from tools.pattern_lab.strategy_screen.facts import extract,validate
    from tools.pattern_lab.study.contracts import semantic_digest
    from tools.pattern_lab.strategy_screen.request import month_boundary
    args=native_records[0][1][0];unit=extract(*args);e=unit['execution'];e['rules'][field]=value
    a=next(a for a in args[3]['allocations'] if a['allocationId']==args[1]['allocationId']);p=args[2]
    e['accountId']=semantic_digest(dict(schema='pattern-lab.account.v1',preparationId=e['preparationId'],commandId=e['commandId'],
        policy=a['policy'],window=dict(startMs=month_boundary(p['startUtc']),endMs=month_boundary(p['endUtc'])),rules=e['rules']))
    with pytest.raises(PatternLabDataError,match='rules'):validate(unit,*args[1:],synthetic=False)


@pytest.mark.parametrize('closes,values,expected',[
    ([1,31,60],[200,50,-10],[75,105]),
    ([1,31,60],[200,100,-10],[50,105]),
    ([1,31,60],[200,150,100],[25,50]),
    ([1,31,59,60],[200,-10,-20,-10],[105,110]),
    ([1,31,59,60],[100,100,50,200],[0,50]),
    ([1,31,33,60],[200,50,200,200],[75,0]),
    ([1,31],[200,50],[75,None]),
])
def test_two_month_extraction_signed_path_oracle(native_records,closes,values,expected):
    """Inject explicit numerical paths into a truthful native metadata envelope.

    This is an extraction/array oracle, not a claim that HA executed these paths.
    No validation/helper is patched out; metadata uses actual native primitives.
    """
    import numpy as np
    import pyarrow as pa
    from dataclasses import replace
    from tools.pattern_lab.accounts import prepare_inputs,execute_account,AccountWindow,AccountFeatureRequest,HARecipe,build_commands,ExecutionPolicy
    from tools.pattern_lab.accounts.contracts import Coverage
    from tools.pattern_lab.account_study.codec import AccountRecord
    from tools.pattern_lab.strategy_screen.request import normalize_request,configurations,child_request,month_boundary
    from tools.pattern_lab.strategy_screen.facts import extract
    from ._account_helpers import raw_binding,rules
    doc=request();doc['allocations']=doc['allocations'][:1];doc['allocations'][0]['policy']['initialCapital']=100.
    doc['cells'][0].update(instrumentId='OKX_AAA-USDT-SWAP',allocationIds=['rr2'])
    doc['periods'][0]['endUtc']='2020-03-01T00:00:00Z';doc['segments']=[];doc['previousReviewEndUtc']=None
    doc=normalize_request(doc).document;c=configurations(doc)[0];p=doc['periods'][0];a=doc['allocations'][0]
    start,end=map(month_boundary,(p['startUtc'],p['endUtc']));warm=month_boundary(p['preparationStartUtc']);day=86400000
    times=np.arange(warm,end,300000,dtype=np.int64);raw=np.tile([100.,102.,98.,100.,1000.],(len(times),1))
    prepared=prepare_inputs(raw_binding(times,raw,tf=1440,start=start,end=end,warmup=warm),(),tuple(AccountFeatureRequest(**f) for f in a['features']))
    commands=build_commands(prepared,HARecipe(**a['provider']['recipe']),mode=a['policy']['mode'],stopUpdates=False)
    r=execute_account(prepared,commands,ExecutionPolicy(**a['policy']),AccountWindow(start,end),rules())
    path=[]
    for close,value in zip(closes,values):
        row=dict(r.path.to_pylist()[close-1]);row.update(equity=float(value),balance=float(value),side=0,quantity=0.);path.append(row)
    # A single explicitly injected terminal cashflow reconciles the scalar path.
    trade=dict(native_records[0][1][0][0].result.trades.to_pylist()[0]);net=float(values[-1]-100)
    trade.update(entryMs=start,exitMs=start+closes[-1]*day,holdingMs=closes[-1]*day,entryIndex=path[0]['barIndex'],exitIndex=path[-1]['barIndex'],
        grossPnl=net,netPnl=net,entryFee=0.,exitFee=0.,exitReason='terminal',exitPhase='close',side=1)
    coverage=Coverage(start,end,start+(closes[0]-1)*day,start+closes[-1]*day,60,len(closes),60-len(closes),closes[0]-1,60-closes[-1])
    r=replace(r,path=pa.Table.from_pylist(path,schema=r.path.schema),trades=pa.Table.from_pylist([trade],schema=r.trades.schema),
        finalBalance=float(values[-1]),coverage=coverage)
    record=AccountRecord(prepared,r,dict(instrumentId=c['instrumentId'],allocationId=c['allocationId'],windowId=p['periodId']))
    unit=extract(record,c,p,child_request(doc,doc['cells'][0],p))
    peaks=np.maximum.accumulate(np.r_[100.,values])[1:];oracle=100*(peaks-values)/peaks
    assert [m['carriedPeakMaxDdPct'] for m in unit['months']]==expected
    assert unit['metrics']['mtmMaxDdPct']==max(oracle)==max(v for v in expected if v is not None)
    if values==[200,50,-10]:assert unit['months'][1]['localDrawdownPct']==120.


def test_signed_peak_drawdown():
    assert local_dd([200,50,-10],100)==105


def test_boundary_exactness_is_independent_of_coverage():
    row=dict(expectedBars=10,observedBars=10,openingExact=False,closingExact=True,openingEquity=100,closingEquity=110)
    value=analytical_month(row,100,True)
    assert value['coverageComplete'] and not value['exact'] and value['reason']=='inexactBoundary'
    row['openingExact']=True
    later=analytical_month(row,100,False)
    assert later['contributionPp']==10 and later['cumulativeReturnPct'] is None
    row.update(openingEquity=-10,closingEquity=10)
    later=analytical_month(row,100,True)
    assert later['contributionPp']==20 and later['windowReturnPct'] is None


def test_terminal_boundary_ownership():
    months=[dict(startMs=0,endMs=10),dict(startMs=10,endMs=20)]
    assert trade_month(10,months,20)==1
    assert trade_month(20,months,20)==1


def test_independent_native_array_oracle_and_cross_month_trade(monkeypatch,tmp_path):
    import math
    import numpy as np
    from datetime import datetime,timezone
    from tools.pattern_lab.configuration_study.metrics import extract_metrics
    original=runner.extract;witnesses=[]
    def observe(record,*args):
        unit=original(record,*args);r=record.result;path=r.path.to_pydict();trades=r.trades.to_pylist()
        assert unit['metrics']==extract_metrics(record)
        close=np.asarray(path['barOpenMs'])+record.prepared.stepMs
        equity=np.asarray(path['equity']);peak=np.maximum.accumulate(np.r_[r.policy.initialCapital,equity])[1:]
        dd=100*(peak-equity)/peak
        for month in unit['months']:
            mask=(close>month['startMs'])&(close<=month['endMs'])
            assert month['carriedPeakMaxDdPct']==float(dd[mask].max())
            assert month['observedBars']==sum(mask)
            assert month['closingEquity']==equity[np.searchsorted(close,month['endMs'],side='right')-1]
            owned=[t for t in trades if month['startMs']<=t['exitMs']<month['endMs'] or
                t['exitMs']==month['endMs']==r.window.endMs]
            assert month['closedTrades']==len(owned)
            assert month['terminalExitFees']==math.fsum(t['exitFee'] for t in owned if t['exitReason']=='terminal')
        month=lambda t:datetime.fromtimestamp(t/1000,timezone.utc).strftime('%Y-%m')
        witnesses.extend((t['entryMs'],t['exitMs']) for t in trades if month(t['entryMs'])!=month(t['exitMs']))
        assert math.isclose(math.fsum(t['netPnl'] for t in trades),r.finalBalance-r.policy.initialCapital,rel_tol=1e-9,abs_tol=1e-8)
        return unit
    monkeypatch.setattr(runner,'extract',observe)
    run(tmp_path/'oracle',auxiliary=True)
    assert witnesses, 'synthetic execution must actually carry a trade across a month boundary'


def test_coarse_fully_observed_jan_mar_2026_has_inexact_internal_months(tmp_path):
    doc=request();doc['allocations']=doc['allocations'][:1];doc['allocations'][0]['timeframeMinutes']=2880
    doc['cells'][0].update(timeframeMinutes=2880,allocationIds=['rr2']);doc['segments']=[];doc['previousReviewEndUtc']=None
    from data_provider.manifest import to_epoch_ms,format_epoch_ms
    step=2880*60000;warm=to_epoch_ms('2025-09-01T00:00:00Z','warm')//step*step
    for p,start,end in zip(doc['periods'],('2025-10-01T00:00:00Z','2026-01-01T00:00:00Z'),('2026-01-01T00:00:00Z','2026-04-01T00:00:00Z')):
        p.update(startUtc=start,endUtc=end,preparationStartUtc=format_epoch_ms(warm))
        p['protocol'].update(development=dict(start_utc=start,end_utc=end),reserved=dict(start_utc=end,end_utc='2026-08-01T00:00:00Z'),earliest_warmup_start_utc=format_epoch_ms(warm))
    build_inputs(tmp_path/'pack',document=doc)
    reader=runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=tmp_path/'screen')
    recent=next(r for r in results(reader) if r['periodId']=='recent')
    assert recent['classification']['classifiable']
    assert recent['metrics']['coverage']['expectedBars']==recent['metrics']['coverage']['observedBars']==45
    assert any(not m['exact'] for m in recent['months'])
    assert recent['months'][0]['closingExact'] is False


def test_zero_expected_and_flat_cash_gap_are_not_exact_observations():
    row=dict(expectedBars=0,observedBars=0,openingExact=True,closingExact=True,openingEquity=100,closingEquity=100)
    assert analytical_month(row,100,True)['reason']=='noExpectedBars'
    row.update(expectedBars=10,observedBars=0,openingExact=False,closingExact=False)
    value=analytical_month(row,100,True)
    assert value['contributionPp'] is None and not value['coverageComplete']


@pytest.mark.parametrize('values,opening,expected',[([200,50],100,75),([50,200],100,50),([-20,-10],-10,None),([0,20,-10],-10,150)])
def test_local_peak_chronology(values,opening,expected):
    assert local_dd(values,opening)==expected


@pytest.mark.parametrize('case',['noObservations','noTrades','february','terminalAtMissingTailBoundary'])
def test_native_availability_and_terminal_tail(case):
    import numpy as np
    from tools.pattern_lab.accounts import prepare_inputs,execute_account,AccountWindow,AccountFeatureRequest,HARecipe,build_commands,ExecutionPolicy
    from tools.pattern_lab.account_study.codec import AccountRecord
    from tools.pattern_lab.strategy_screen.request import normalize_request,configurations,child_request,month_boundary
    from tools.pattern_lab.strategy_screen.facts import extract
    from tools.pattern_lab.strategy_screen.analytics import classification
    from ._account_helpers import raw_binding,bundle,side,policy,rules
    doc=request();doc['cells'][0]['instrumentId']='OKX_AAA-USDT-SWAP';doc['allocations']=doc['allocations'][:1];doc['cells'][0]['allocationIds']=['rr2']
    doc['segments']=[];doc['previousReviewEndUtc']=None
    if case=='february':doc['periods'][0].update(startUtc='2020-02-01T00:00:00Z',endUtc='2020-03-01T00:00:00Z')
    if case=='terminalAtMissingTailBoundary':doc['allocations'][0]['policy']['rewardRisk']=1000.
    doc=normalize_request(doc).document;c=configurations(doc)[0];period=doc['periods'][1 if case=='terminalAtMissingTailBoundary' else 0]
    start,end=map(month_boundary,(period['startUtc'],period['endUtc']))
    warm=month_boundary('2019-12-01T00:00:00Z');last=end if case!='terminalAtMissingTailBoundary' else month_boundary('2020-05-01T00:00:00Z')
    times=np.arange(warm,last,300000,dtype=np.int64)
    if case=='noObservations':times=np.r_[times[times<start],np.arange(end,end+86400000,300000,dtype=np.int64)]
    raw=np.tile([100.,102.,98.,100.,1000.],(len(times),1))
    if case=='terminalAtMissingTailBoundary':
        # Native HA entry after a neutral preparation prefix, then a long-lived
        # position with an unreachable target and a genuine missing-tail close.
        days=(times[times>=start]-start)//86400000
        raw[times>=start]=np.column_stack((100.+days,102.+days,99.5+days,101.+days,np.full(len(days),1000.)))
    allocation=doc['allocations'][0]
    prepared=prepare_inputs(raw_binding(times,raw,tf=1440,start=start,end=end+(86400000 if case=='noObservations' else 0),warmup=warm),(),
        tuple(AccountFeatureRequest(**f) for f in allocation['features']))
    commands=build_commands(prepared,HARecipe(**allocation['provider']['recipe']),mode=allocation['policy']['mode'],stopUpdates=allocation['policy']['stopUpdates'])
    result=execute_account(prepared,commands,ExecutionPolicy(**allocation['policy']),AccountWindow(start,end),rules())
    record=AccountRecord(prepared,result,dict(instrumentId=c['instrumentId'],allocationId=c['allocationId'],windowId=period['periodId']))
    if case=='noObservations':
        # The accepted native executor supports an empty subwindow. Native pack
        # preparation refuses an entirely empty research span; do not forge a
        # screen input receipt or bypass that protected guard for this oracle.
        from tools.pattern_lab.configuration_study.metrics import extract_metrics
        metrics=extract_metrics(record);flags=classification(dict(status='complete',metrics=metrics),doc['criteria'])
        assert metrics['performanceStatus']=='noObservedBars' and flags['classifiable'] is False
        assert metrics['mtmMaxDdPct'] is metrics['annualizedProfitPct'] is None
        return
    unit=extract(record,c,period,child_request(doc,doc['cells'][0],period));flags=classification(unit,doc['criteria'])
    if case=='noTrades':assert flags['classifiable'] and flags['profitable'] is False and unit['metrics']['trades']==0
    elif case=='february':assert unit['metrics']['annualizedProfitStatus']=='shortSpan' and flags['profitable'] is None
    else:
        assert result.trades.to_pylist()[0]['exitMs']==month_boundary('2020-05-01T00:00:00Z')
        assert [m['terminalTradeCount'] for m in unit['months']]==[0,1,0]
        assert unit['months'][1]['observedBars']==0 and unit['months'][1]['terminalExitFees']>0
        assert unit['months'][0]['closingEquity']==unit['months'][-1]['closingEquity']==result.finalBalance
