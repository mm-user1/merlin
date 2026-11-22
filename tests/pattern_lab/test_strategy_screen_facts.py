import pytest
from tools.pattern_lab.strategy_screen.facts import local_dd,analytical_month,trade_month
from tools.pattern_lab.strategy_screen import runner
from ._strategy_screen_helpers import run
from ._strategy_screen_helpers import request,build_inputs,results


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
    from tools.pattern_lab.accounts import prepare_inputs,execute_account,AccountWindow
    from tools.pattern_lab.account_study.codec import AccountRecord
    from tools.pattern_lab.strategy_screen.request import normalize_request,configurations,child_request,month_boundary
    from tools.pattern_lab.strategy_screen.facts import extract
    from tools.pattern_lab.strategy_screen.analytics import classification
    from ._account_helpers import raw_binding,bundle,side,policy,rules
    doc=request();doc['cells'][0]['instrumentId']='OKX_AAA-USDT-SWAP';doc['allocations']=doc['allocations'][:1];doc['cells'][0]['allocationIds']=['rr2']
    doc['segments']=[];doc['previousReviewEndUtc']=None
    if case=='february':doc['periods'][0].update(startUtc='2020-02-01T00:00:00Z',endUtc='2020-03-01T00:00:00Z')
    doc=normalize_request(doc).document;c=configurations(doc)[0];period=doc['periods'][1 if case=='terminalAtMissingTailBoundary' else 0]
    start,end=map(month_boundary,(period['startUtc'],period['endUtc']))
    warm=month_boundary('2019-12-01T00:00:00Z');last=end if case!='terminalAtMissingTailBoundary' else month_boundary('2020-05-01T00:00:00Z')
    times=np.arange(warm,last,300000,dtype=np.int64)
    if case=='noObservations':times=np.r_[times[times<start],np.arange(end,end+86400000,300000,dtype=np.int64)]
    raw=np.tile([100.,102.,98.,100.,1000.],(len(times),1))
    prepared=prepare_inputs(raw_binding(times,raw,tf=1440,start=start,end=end+(86400000 if case=='noObservations' else 0),warmup=warm),(),())
    entry=next((i for i,t in enumerate(prepared.target.timestamps) if t>=start),0)
    commands=bundle(prepared,mode='fixedBracket',long=side(len(prepared.target.timestamps),entries=[entry] if case=='terminalAtMissingTailBoundary' else []))
    result=execute_account(prepared,commands,policy(mode='fixedBracket',rewardRisk=2.),AccountWindow(start,end),rules())
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
