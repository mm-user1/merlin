"""Compact descriptive reductions of retained accounts, without re-execution."""
from itertools import combinations
from pathlib import Path
import hashlib
import importlib.metadata
import math
import sys

import numpy as np

from ..accounts.contracts import require
from ..manifest import to_epoch_ms


def close(a, b):
    return a is not None and b is not None and math.isfinite(a) and math.isfinite(b) and abs(a-b) <= max(1e-9*max(abs(a),abs(b)),1e-8)


def direction(value):
    return None if value is None else 'flat' if close(value,0) else 'up' if value > 0 else 'down'


def attribution(reader, schema):
    directory = Path(__file__).parent
    return dict(metricSchema=schema,
        sources={'tools/pattern_lab/account_study/'+p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(directory.iterdir()) if p.suffix in ('.py','.js') and p.is_file()},
        inputStudy=dict(schema=reader.study['schema'],identities=reader.study['identities']),
        python=sys.version,libraries={n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow','numba')})


def window_facts(request):
    windows=[]
    for w in request['windows']:
        start,end=(to_epoch_ms(w[k],k) for k in ('startUtc','endUtc'))
        windows.append(dict(windowId=w['windowId'],startMs=start,endMs=end,durationMs=end-start))
    windows.sort(key=lambda w:(w['startMs'],w['endMs'],w['windowId']))
    overlaps=[]
    for a,b in combinations(windows,2):
        shared=max(0,min(a['endMs'],b['endMs'])-max(a['startMs'],b['startMs']))
        overlaps.append(dict(windowA=a['windowId'],windowB=b['windowId'],sharedMs=shared,
            sharedPctOfA=100*shared/a['durationMs'],sharedPctOfB=100*shared/b['durationMs']))
    return dict(windows=windows,overlaps=overlaps)


def passive_month(target, step, month):
    left,right=month['startMs'],month['endMs']
    stamps=target.timestamps
    mask=(stamps >= left)&(stamps+step <= right)
    count=int(mask.sum()); aligned=left % step == right % step == 0
    expected=(right-left)//step if aligned else None
    opening=np.flatnonzero(mask & (stamps==left)); closing=np.flatnonzero(mask & (stamps+step==right))
    interior=int(((stamps > left)&(stamps+step < right)).sum())
    gap=interior < max(0,expected-2) if aligned else None
    reason='unalignedInterval' if not aligned else 'missingOpeningBar' if not len(opening) else 'missingClosingBar' if not len(closing) else None
    value=None
    if reason is None:
        op,cl=float(target.values[opening[0],0]),float(target.values[closing[0],3])
        if not all(math.isfinite(x) and x > 0 for x in (op,cl)): reason='invalidEndpointPrice'
        else:value=100*(cl/op-1)
    return dict(month=month['month'],startMs=left,endMs=right,partialEdgeMonth=month['partialEdgeMonth'],
        status='unavailable' if reason else 'available',reason=reason,returnPct=value,direction=direction(value),
        expectedBars=expected,observedBars=count,interiorGap=gap,
        openingExact=bool(len(opening)),closingExact=bool(len(closing)))


def sides(trades, initial, duration, *, available=True):
    rows=[]
    for label,side in (('long',1),('short',-1)):
        selected=[t for t in trades if t['side']==side]
        gross=math.fsum(t['grossPnl'] for t in selected)
        fees=math.fsum(t['entryFee']+t['exitFee'] for t in selected)
        net=math.fsum(t['netPnl'] for t in selected)
        holding=sum(t['holdingMs'] for t in selected)
        rows.append(dict(side=label,status='available' if available else 'noObservedBars',trades=len(selected),
            grossPnl=gross,fees=fees,netPnl=net,holdingMs=holding,
            contributionPct=100*net/initial if available else None,
            timeInMarketPct=100*holding/duration if available else None))
    return rows


def account_facts(record, summary):
    r=record.result
    split=sides(r.trades.to_pylist(),r.policy.initialCapital,r.window.endMs-r.window.startMs,
                available=summary['coverage']['observedBars'] > 0)
    for key in ('trades','grossPnl','fees','netPnl','holdingMs'):
        require(close(math.fsum(s[key] for s in split),summary[key]),'side '+key+' reconciliation')
    return dict(sides=split,passiveMonths=[passive_month(record.prepared.target,record.prepared.stepMs,m) for m in summary['months']])


def ranked(values):
    return sorted(((label,value) for label,value in values if value > 0),key=lambda x:(-x[1],x[0]))


def erasure(total, positives, count, *, allow_empty):
    base=dict(originalPnl=total,originalCount=count,removedCount=None,removedFraction=None,removedIds=[])
    if total <= 0:return dict(**base,status='originalNetNonpositive',remainingPnl=None,allRemoved=False)
    for n in range(1,len(positives)+1):
        remaining=total-math.fsum(v for _,v in positives[:n])
        if (remaining <= 0 or close(remaining,0)) and (allow_empty or n < count):
            base.update(removedCount=n,removedFraction=n/count,removedIds=[k for k,_ in positives[:n]])
            return dict(**base,status='available',remainingPnl=remaining,allRemoved=n==count)
    return dict(**base,status='noNonemptySubsetErasesProfit' if not allow_empty else 'noPrefixErasesProfit',remainingPnl=None,allRemoved=False)


def removals(aggregate, summaries):
    total=aggregate['netPnl']; count=len(summaries); initial=aggregate['initialCapital']; capital=initial/count
    require(close(math.fsum(s['netPnl'] for s in summaries),total),'ticker removal reconciliation')
    months=aggregate['months']
    require(close(math.fsum(m['pnl'] for m in months),total),'month removal reconciliation')
    positive=ranked((s['instrumentId'],s['netPnl']) for s in summaries)
    ex=[]
    for requested in (1,5):
        removed=positive[:requested]; pnl=total-math.fsum(v for _,v in removed); remaining=count-len(removed)
        ex.append(dict(requestedCount=requested,removedCount=len(removed),removedIds=[k for k,_ in removed],
            removedPnl=math.fsum(v for _,v in removed),originalCount=count,remainingCount=remaining,
            originalCapital=initial,remainingCapital=remaining*capital,originalPnl=total,remainingPnl=pnl,
            remainingReturnPct=100*pnl/(remaining*capital) if remaining else None,
            status='available' if remaining else 'emptyRemainingRoster'))
    positive_months=ranked((m['month'],m['pnl']) for m in months)
    selected=next((m for m in months if positive_months and m['month']==positive_months[0][0]),None)
    removed=selected['pnl'] if selected else 0
    month=dict(status='available',removedCount=int(selected is not None),selectedMonth=selected['month'] if selected else None,
        removedPnl=removed,originalPnl=total,remainingPnl=total-removed,originalCapital=initial,
        remainingContributionPct=100*(total-removed)/initial,
        selectedFlags={k:selected[k] for k in ('partialEdgeMonth','allAccountsObserved','openingExact','closingExact','observedBars','expectedBars')} if selected else None)
    ticker_erasure=erasure(total,positive,count,allow_empty=False)
    remaining=count-ticker_erasure['removedCount'] if ticker_erasure['removedCount'] is not None else None
    ticker_erasure.update(originalCapital=initial,remainingCount=remaining,
        remainingCapital=remaining*capital if remaining is not None else None)
    month_erasure=erasure(total,positive_months,len(months),allow_empty=True)
    month_erasure.update(originalCapital=initial,
        remainingContributionPct=100*month_erasure['remainingPnl']/initial if month_erasure['remainingPnl'] is not None else None)
    return dict(status='available',tickerExTop=ex,tickerProfitErasure=ticker_erasure,
        monthExBest=month,monthProfitErasure=month_erasure,
        monthInterpretation='Accounting removal; no account re-execution')


def aggregate_facts(aggregate, summaries):
    available=aggregate['status']=='available'
    # Even unavailable performance has declared month intervals and calendar edges.
    template=summaries[0]['months']; months=aggregate['months']
    result=dict(partialEdgeMonths=sum(m['partialEdgeMonth'] for m in template),totalMonths=len(template),
        incompleteAccountMonths=aggregate['monthBreadth']['partialMonths'] if available else None,
        observedMonths=aggregate['monthBreadth']['observedMonths'] if available else None,
        nonExactBoundaryMonths=sum(not (m['openingExact'] and m['closingExact']) for m in months) if available else None,
        coverageStatus='available' if available else aggregate['reason'])
    for m in months:
        m['pairEligible']=m['returnPct'] is not None and math.isfinite(m['returnPct']) and m['allAccountsObserved'] and m['openingExact'] and m['closingExact']
    passive=[]
    for index,m in enumerate(template):
        cells=[s['robustness']['passiveMonths'][index] for s in summaries]
        usable=sum(c['status']=='available' for c in cells)
        value=math.fsum(c['returnPct'] for c in cells)/len(cells) if usable==len(cells) else None
        passive.append(dict(month=m['month'],startMs=m['startMs'],endMs=m['endMs'],partialEdgeMonth=m['partialEdgeMonth'],
            status='available' if value is not None else 'unavailable',reason=None if value is not None else 'incompleteRosterEndpoints',
            returnPct=value,direction=direction(value),usableAccounts=usable,plannedAccounts=len(cells),
            observedBars=sum(c['observedBars'] for c in cells),
            expectedBars=sum(c['expectedBars'] for c in cells) if all(c['expectedBars'] is not None for c in cells) else None,
            interiorGap=any(c['interiorGap'] for c in cells) if all(c['interiorGap'] is not None for c in cells) else None,
            unavailableInstruments=[dict(instrumentId=s['instrumentId'],reason=c['reason']) for s,c in zip(summaries,cells) if c['status']!='available']))
    result['passiveMonths']=passive
    if not available:
        result.update(sides=[dict(side=side,status='unavailable',reason=aggregate['reason'],trades=None,grossPnl=None,
            fees=None,netPnl=None,holdingMs=None,contributionPct=None,timeInMarketPct=None) for side in ('long','short')],
            removals=dict(status='unavailable',reason=aggregate['reason']))
        return result
    split=[]; duration=sum(m['endMs']-m['startMs'] for m in template)
    for index,label in enumerate(('long','short')):
        items=[s['robustness']['sides'][index] for s in summaries]
        row={key:math.fsum(x[key] for x in items) for key in ('grossPnl','fees','netPnl')}
        row.update(trades=sum(x['trades'] for x in items),holdingMs=sum(x['holdingMs'] for x in items))
        row.update(side=label,status='available',contributionPct=100*row['netPnl']/aggregate['initialCapital'],
            timeInMarketPct=100*row['holdingMs']/(len(summaries)*duration))
        split.append(row)
    for key in ('trades','fees','netPnl','timeInMarketPct'):
        require(close(math.fsum(s[key] for s in split),aggregate[key]),'aggregate side '+key+' reconciliation')
    result.update(sides=split,removals=removals(aggregate,summaries))
    return result
