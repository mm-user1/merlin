"""Synthetic screen fixtures shared by maintained tests, independent of _work."""
from tools.pattern_lab.examples.strategy_screen_synthetic import build_inputs, request
from tools.pattern_lab.strategy_screen import run_strategy_screen
from copy import deepcopy
import math


def run(root, *, workers=1, auxiliary=False):
    root.mkdir()
    aux=root/'auxiliary' if auxiliary else None
    doc=build_inputs(root/'pack',aux)
    return run_strategy_screen(doc,data_root=root/'pack',output_root=root/'screen',workers=workers,
        auxiliary_roots={'aux':aux} if auxiliary else None)


def results(reader):
    return [reader.result(r['configurationId'],r['periodId']) for r in reader.result_rows]


def capture_native(root, *, auxiliary=False):
    """Small test-only record roster; production still prunes each result."""
    from unittest.mock import patch
    from tools.pattern_lab.strategy_screen import runner
    extract=runner.extract;records=[]
    def capture(record,c,p,child):
        records.append((record,c,p,child))
        return extract(record,c,p,child)
    with patch.object(runner,'extract',capture):reader=run(root,auxiliary=auxiliary)
    return reader,records


def compact_unit(config,period,ordinal=0):
    """Invented compact fixture, explicitly without native execution evidence."""
    from ._configuration_capacity import unit as old_unit
    from tools.pattern_lab.strategy_screen.request import calendar,result_id
    from tools.pattern_lab.strategy_screen.facts import analytical_month,VERSION
    from tools.pattern_lab.configuration_study.metrics import annualized,DAY
    slots=calendar(period);start=slots[0]['startMs'];end=slots[-1]['endMs'];initial=10000.
    m=old_unit(config,dict(stageId=period['periodId'],startUtc=period['startUtc'],endUtc=period['endUtc']),ordinal)['metric']['metrics']
    months=[];value=peak=initial
    for i,slot in enumerate(slots):
        opening=value;value+=float(((ordinal*7+i*11)%31)-15)*10
        peak=max(peak,value);dd=100*(peak-value)/peak;bars=(slot['endMs']-start)//(config['timeframeMinutes']*60000)-(slot['startMs']-start)//(config['timeframeMinutes']*60000)
        row=dict(**slot,openingEquity=opening,closingEquity=value,openingBalance=opening,closingBalance=value,
            openingSampleMs=slot['startMs'],closingSampleMs=slot['endMs'],openingExact=True,closingExact=True,
            observedBars=bars,expectedBars=bars,diagnosticPnl=value-opening,diagnosticContributionPp=100*(value-opening)/initial,
            closedTrades=1+(ordinal+i)%7,terminalTradeCount=0,terminalExitFees=0.,minEquity=min(opening,value),maxEquity=max(opening,value),
            localDrawdownPct=max(0,100*(opening-value)/opening),localDrawdownStatus='available',carriedPeakMaxDdPct=dd,carriedPeakStatus='available')
        row.update(analytical_month(row,initial,True));months.append(row)
    net=value-initial;trades=sum(v['closedTrades'] for v in months);fees=trades*.1;wins=trades//2;ret=100*(value/initial-1)
    m.update(finalCapital=value,netPnl=net,grossPnl=net+fees,fees=fees,returnPct=ret,trades=trades,wins=wins,losses=trades-wins,
        winRatePct=100*wins/trades,tradesPerMonth=trades/len(months),exitReasons={'stop':trades},attemptDispositions={'filled':trades},
        mtmMaxDdPct=max(v['carriedPeakMaxDdPct'] for v in months),balanceMaxDdPct=max(v['carriedPeakMaxDdPct'] for v in months))
    m['annualizedProfitPct'],m['annualizedProfitStatus'],m['annualizedProfitShortSpan']=annualized(ret,(end-start)/DAY)
    return dict(factsVersion=VERSION,configurationId=config['configurationId'],resultId=result_id(config['configurationId'],period),
        periodId=period['periodId'],status='complete',metrics=m,months=months,
        execution=dict(kind='syntheticFixture',accountId=None,preparationId=None,commandId=None,validation=None,inputs=[],rules=None,auxiliary=[]),
        terminal=dict(status=m['terminalStatus'],finalFlat=True,finalBalance=value),
        audit=dict(version=VERSION,tradesChecked=trades,pathPoints=m['coverage']['observedBars'],grossPnl=net+fees,netPnl=net,fees=fees,
            monthlyReconstructedDd=m['mtmMaxDdPct'],terminalTrades=0,terminalExitFees=0.))


def compact_request(per_cell=2,*,capacity=False):
    doc=request();base=deepcopy(doc['allocations'][0]);doc['allocations']=[];doc['cells']=[]
    for tf in ((30,60,120,240) if capacity else (1440,)):
        ids=[]
        for i in range(per_cell):
            a=deepcopy(base);a.update(allocationId=f'tf{tf}-rr{i}',timeframeMinutes=tf);a['policy']['rewardRisk']=1+i/1000
            doc['allocations'].append(a);ids.append(a['allocationId'])
        for target in (('BYBIT_AAAUSDT','BYBIT_BBBUSDT','OKX_AAAUSDT','OKX_CCCUSDT') if capacity else ('BYBIT_AAAUSDT',)):
            doc['cells'].append(dict(instrumentId=target,timeframeMinutes=tf,allocationIds=list(ids)))
    if capacity:
        doc['segments']=[];doc['previousReviewEndUtc']=None
        for p,start,end in zip(doc['periods'],('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z'),('2021-01-01T00:00:00Z','2021-11-01T00:00:00Z')):
            p.update(startUtc=start,endUtc=end)
            p['protocol']['development']=dict(start_utc=start,end_utc=end)
            p['protocol']['reserved']=dict(start_utc=end,end_utc='2022-01-01T00:00:00Z')
    return doc


def compact(root,per_cell=2,*,capacity=False,implementation=None):
    from tools.pattern_lab.strategy_screen.request import normalize_request,configurations
    from tools.pattern_lab.strategy_screen.artifacts import Writer,load_strategy_screen
    normalized=normalize_request(compact_request(per_cell,capacity=capacity));doc=normalized.document
    root.mkdir(parents=True,exist_ok=False);writer=Writer(root,normalized,implementation or {'fixture':'synthetic-screen'},synthetic=True)
    for p in reversed(doc['periods']):
        for i,c in enumerate(configurations(doc)):writer.append(compact_unit(c,p,i+(13 if p['periodId']=='recent' else 0)))
    writer.finish({'fixture':'invented numeric compact-only; no native execution'},dict(plannedResults=2*len(configurations(doc))))
    return load_strategy_screen(root)
