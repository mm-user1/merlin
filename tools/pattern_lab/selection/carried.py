"""Authoritative monthly cash chains, reconstructed identically from saved facts."""
import math
import itertools
import numpy as np
from ..accounts.contracts import require
from ..study.contracts import semantic_digest
from .arithmetic import rank,boundary_tie,random_order,distribution
from .ledger import covered,REJECTIONS
from .request import month_bounds
from .funded import execution_key
from .storage_v2 import portfolio_order


def schedules(doc,ledger,scores):
    lookup={(r['month'],r['instrumentId']):r for r in ledger};out={}
    for batch in scores:
        ranked=rank(batch['rows']);pool=sorted(r['instrumentId'] for r in ranked);month=batch['month']
        failures=[dict(instrumentId=i,reason='noObservedBars' if not lookup[month,i]['available'] else 'insufficientCoverage')
            for i in pool if not covered(lookup[month,i],doc['pool']['minObservedBarsPct'])]
        out[batch['selectorId'],month]=dict(ranked=ranked,pool=pool,failures=failures)
    return out


def portfolio_id(selector,k,kind,replicate):return semantic_digest([selector,k,kind,replicate])


def demands(doc,plan,month,opening,stopped,lookup):
    rows=[];needs={}
    for selector in doc['selectors']:
        sid=selector['selectorId'];p=plan[sid,month]
        was_stopped=sid in stopped
        if p['failures']:stopped.add(sid)
        for k in doc['kValues']:
            variants=[('selected',-1,[r['instrumentId'] for r in p['ranked'][:k]]),('allEligible',-1,p['pool'])]
            variants.extend(('random',r,random_order(p['pool'],doc['randomControl']['seed'],r,month)[:k]) for r in range(doc['randomControl']['replicates']))
            for kind,replicate,members in variants:
                pid=portfolio_id(sid,k,kind,replicate);total=opening.get(pid,doc['initialCapital'])
                planned=len(p['pool']) if kind=='allEligible' else k
                status='priorCapitalUnknown' if was_stopped else 'unavailable' if sid in stopped else 'depleted' if total<=0 else 'available'
                slot=total/planned if status=='available' and planned else None
                links=[]
                if status=='available':
                    for ticker in members:
                        key=lookup[month,ticker]['accountKey'];retain=kind!='random'
                        needs.setdefault(key,{})[slot]=needs.get(key,{}).get(slot,False) or retain
                        links.append(dict(instrumentId=ticker,executionKey=execution_key(key,slot)))
                funded=len(links)
                row=dict(portfolioId=pid,selectorId=sid,k=k,portfolio=kind,replicateIndex=replicate,month=month,
                    status=status,reason=None if status=='available' else status,plannedSlots=planned,fundedSlots=funded,
                    idleSlots=planned-funded,slots=links,members=members,
                    openingCapital=None if was_stopped else total,closingCapital=None,slotCapital=slot,
                    fundedCapital=None,cash=None,utilizationPct=None,netPnl=None,returnPct=None,fees=None,
                    trades=None,sizingRejections=None,forcedExits=None)
                rows.append(row)
    return sorted(rows,key=portfolio_order),needs


def settle(row,records,paths,times):
    if row['status'] in ('unavailable','priorCapitalUnknown'):return None
    opening=row['openingCapital'];funded=[records[s['executionKey']] for s in row['slots']]
    capitals=[f['capital'] for f in funded]
    require(all(c==row['slotCapital'] for c in capitals),'funded slot capital differs')
    pnl=math.fsum(f['finalBalance']-f['capital'] for f in funded)
    closing=opening+pnl;cash=opening-math.fsum(capitals)
    require(all(math.isfinite(v) for v in (pnl,closing,cash)),'nonfinite carried arithmetic')
    require(math.isclose(cash+math.fsum(f['finalBalance'] for f in funded),closing,rel_tol=1e-9,abs_tol=1e-8),'funded cash reconciliation')
    row.update(closingCapital=closing,fundedCapital=math.fsum(capitals),cash=cash,
        utilizationPct=100*math.fsum(capitals)/opening if opening>0 else None,netPnl=pnl,
        returnPct=100*(closing-opening)/opening if opening>0 else None,
        fees=math.fsum(f['fees'] for f in funded),trades=sum(f['trades'] for f in funded),
        sizingRejections={r:sum(f['sizingRejections'][r] for f in funded) for r in REJECTIONS},
        forcedExits={r:dict(count=sum(f['forcedExits'][r]['count'] for f in funded),
            netPnl=math.fsum(f['forcedExits'][r]['netPnl'] for f in funded)) for r in ('terminal','gapBoundary')})
    if closing<=0:row['status']='depleted';row['reason']='depleted'
    if row['portfolio']=='random':return None
    ordered=sorted(row['slots'],key=lambda s:s['instrumentId'])
    for slot in ordered:require(slot['executionKey'] in paths,'required funded path absent')
    equity=np.array([cash+math.fsum(float(paths[s['executionKey']]['equity'][n]) for s in ordered)
        for n in range(len(times))],np.float64)
    require(np.isfinite(equity).all() and math.isclose(float(equity[-1]),closing,rel_tol=1e-9,abs_tol=1e-8),'funded curve endpoint reconciliation')
    return dict(times=times,equity=equity,opening=opening,closing=closing)


def carried_calculation(doc,ledger,roster,scores,provide):
    plan=schedules(doc,ledger,scores);lookup={(r['month'],r['instrumentId']):r for r in ledger}
    opening={};stopped=set();portfolios={};funded={};paths={};curves={}
    step=ledger[0]['stepMs']
    for month in doc['evaluationMonths']:
        rows,needs=demands(doc,plan,month,opening,stopped,lookup)
        records,arrays=provide(month,needs);by_id={r['executionKey']:r for r in records}
        expected={execution_key(key,capital) for key,variants in needs.items() for capital in variants}
        retained={execution_key(key,c) for key,variants in needs.items() for c,retain in variants.items() if retain}
        require(len(by_id)==len(records) and set(by_id)==expected,'funded execution set')
        require(set(arrays)==retained and {r['executionKey'] for r in records if r['pathRef'] is not None}==retained,'funded path retention set')
        start,end=month_bounds(month);times=np.arange(start+step,end+1,step,dtype=np.int64)
        for key,path in arrays.items():require(np.array_equal(path['times'],times),'funded native path grid')
        for row in rows:
            curve=settle(row,by_id,arrays,times)
            opening[row['portfolioId']]=row['closingCapital']
            if curve is not None:curves[row['portfolioId'],month]=curve
        portfolios[month]=rows;funded[month]=records;paths[month]=arrays
    comparisons=[]
    for selector in doc['selectors']:
        sid=selector['selectorId']
        for k in doc['kValues']:
            selected={kind:[r for month in doc['evaluationMonths'] for r in portfolios[month]
                if r['selectorId']==sid and r['k']==k and r['portfolio']==kind] for kind in ('selected','allEligible','random')}
            prefix=[r['month'] for r in selected['selected'] if r['closingCapital'] is not None]
            complete=len(prefix)==len(doc['evaluationMonths']);full={};prefix_stats={}
            for kind in ('selected','allEligible'):
                rows=[r for r in selected[kind] if r['closingCapital'] is not None];peak=doc['initialCapital'];dd=0.
                for r in rows:
                    path=curves[r['portfolioId'],r['month']]
                    for v in itertools.chain((path['opening'],),path['equity'],(path['closing'],)):
                        peak=max(peak,float(v));dd=max(dd,100*(peak-float(v))/peak)
                value=dict(totalReturnPct=100*(rows[-1]['closingCapital']-doc['initialCapital'])/doc['initialCapital'] if rows else None,
                    maxDrawdownPct=dd if rows else None,netPnl=rows[-1]['closingCapital']-doc['initialCapital'] if rows else None)
                prefix_stats[kind]=value;full[kind]=value if complete else dict.fromkeys(value)
            totals=[]
            for r in range(doc['randomControl']['replicates']):
                rows=[v for v in selected['random'] if v['replicateIndex']==r and v['closingCapital'] is not None]
                totals.append(100*(rows[-1]['closingCapital']-doc['initialCapital'])/doc['initialCapital'] if rows else None)
            months=[]
            for index,month in enumerate(doc['evaluationMonths']):
                p=plan[sid,month]
                batch=next(b for b in scores if b['selectorId']==sid and b['month']==month)
                months.append(dict(month=month,eligiblePool=p['pool'],eligibleCount=len(p['pool']),
                    rankedInstruments=[r['instrumentId'] for r in p['ranked']],commonExclusions=batch['commonExclusions'],
                    methodExclusions=[r for r in batch['rows'] if not r['eligible']],
                    selected={n:v for n,v in selected['selected'][index].items() if n!='slots'},
                    allEligible={n:v for n,v in selected['allEligible'][index].items() if n!='slots'},
                    unavailableReasons=p['failures'],boundaryTie=boundary_tie(p['ranked'],k),tieNeutralExpectedReturnPct=None))
            comparisons.append(dict(selectorId=sid,k=k,months=months,fullPeriodAvailable=complete,fullPeriod=full,
                underfilledMonths=sum(m['eligibleCount']<k for m in months),emptyPoolMonths=sum(not m['eligiblePool'] for m in months),
                commonPrefix=dict(months=prefix,usedMonths=len(prefix),requestedMonths=len(months),metrics=prefix_stats,
                    randomReturnPct=totals,distribution=distribution(totals,prefix_stats['selected']['totalReturnPct'])),
                randomFullReturnPct=totals if complete else [None]*len(totals),
                randomFullDistribution=distribution(totals,full['selected']['totalReturnPct']) if complete else
                    dict(p10=None,median=None,p90=None,below=None,equal=None,above=None,reason='incompleteEvaluationPeriod')))
    return dict(schema='pattern-lab.selection-results.v2',capitalMode='carried',comparisons=comparisons),funded,portfolios,paths,curves
