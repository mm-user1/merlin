"""Shared synthetic selection evidence checks; no ignored files or live inputs."""
import math
from copy import deepcopy
import numpy as np

from tools.pattern_lab.selection.request import month_bounds


def request_template():
    return dict(schema='pattern-lab.selection-request.v1',selectionId='test',
        parent={k:'0'*64 for k in ('specification','dataInput','implementation','completionSha256')},allocationId='base',
        monthWindows=[dict(windowId='m'+str(m),month=f'2020-{m:02d}') for m in range(1,8)],
        evaluationMonths=['2020-05','2020-06','2020-07'],pool=dict(historyMonths=4,minObservedBarsPct=90.),
        selectors=[dict(selectorId='mean',scorerId='meanMonthlyReturn',version='1',parameters=dict(months=2))],
        kValues=[1,3],capitalMode='fixedReference',referenceCapital=100.,randomControl=dict(seed=7,replicates=4))


def hand_case():
    """Two independently specified paths, same endpoints in May, then a loss."""
    doc=request_template();doc['evaluationMonths']=['2020-05','2020-06'];doc['kValues']=[1,3]
    roster=['A','B'];ledger=[];paths={};scores=[]
    for month,values,keys in [('2020-05',[[10.,-10.,10.],[0.,0.,10.]],[[2.],[2.]]),
                              ('2020-06',[[-5.,-5.,-5.],[5.,5.,5.]],[[3.],[1.]] )]:
        start,end=month_bounds(month);times=np.array([start+1,start+2,end],np.int64)
        paths[month]=dict(times=times,pnl=np.array(values,np.float64))
        for ticker,v in zip(roster,values):
            ledger.append(dict(instrumentId=ticker,month=month,available=True,terminalStatus='closed',observedBars=3,expectedBars=3,
                netPnl=v[-1],returnPct=v[-1],fees=1.,trades=1,sizingRejectionCount=0,
                forcedExits={r:dict(count=0,netPnl=0.) for r in ('gapBoundary','terminal')}))
        scores.append(dict(selectorId='mean',month=month,commonExclusions=[],rows=[dict(instrumentId=t,eligible=True,key=k,reason=None,components={}) for t,k in zip(roster,keys)]))
    return doc,ledger,paths,roster,scores


def assert_fixture(reader):
    ledger=reader.ledger;results=reader.results['comparisons']
    assert len(ledger)==42
    evaluation=[r for r in ledger if r['month'] in reader.selection['request']['evaluationMonths']]
    assert all(any(r['trades']>0 for r in evaluation if r['month']==m) for m in ('2020-05','2020-06','2020-07'))
    assert len({r['instrumentId'] for r in evaluation if r['trades']})>=2
    assert math.fsum(r['fees'] for r in evaluation)>0
    assert any(r['returnPct']>0 for r in evaluation) and any(r['returnPct']<0 for r in evaluation)
    assert any(r['available'] and r['trades']==0 and r['returnPct']==0 for r in evaluation)
    assert any(m['boundaryTie'] for c in results for m in c['months'])
    assert any(len({tuple(m['selected']['members']) for m in c['months']})>1 for c in results)
    assert any(c['underfilledMonths']>0 for c in results)
    assert any(m['selected']['returnPct']!=m['allEligible']['returnPct'] for c in results for m in c['months'])
    return dict(accounts=len(ledger),trades=sum(r['trades'] for r in ledger),
        evaluationTrades=sum(r['trades'] for r in evaluation),evaluationFees=math.fsum(r['fees'] for r in evaluation),
        zeroTradeMonths=sum(r['available'] and r['trades']==0 for r in evaluation),
        positiveMonths=sum(r['returnPct']>0 for r in evaluation),negativeMonths=sum(r['returnPct']<0 for r in evaluation),
        boundaryTies=sum(m['boundaryTie'] is not None for c in results for m in c['months']),
        totals={c['selectorId']+'/'+str(c['k']):c['fullPeriod'] for c in results})
