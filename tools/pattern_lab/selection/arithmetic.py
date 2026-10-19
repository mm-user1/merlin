"""Fixed-reference accounting, exact ranking, and versioned hash controls."""
import math
import numpy as np

from ..accounts.contracts import require
from ..study.contracts import semantic_digest
from .ledger import covered
from .request import month_bounds


def random_order(pool, seed, replicate, month):
    return sorted(pool,key=lambda i:(semantic_digest(['random-k.1',seed,replicate,month,i]),i))


def rank(rows):
    return sorted((r for r in rows if r['eligible']),key=lambda r:(tuple(-v for v in r['key']),r['instrumentId']))


def boundary_tie(ranked, k):
    if not ranked or k >= len(ranked): return None
    key = ranked[k-1]['key']
    tied = [r['instrumentId'] for r in ranked if r['key'] == key]
    before = sum(r['key'] != key for r in ranked[:k])
    slots = k-before
    if len(tied) <= slots: return None
    return dict(key=key,members=tied,totalMembers=len(tied),slots=slots,chosen=tied[:slots])


def portfolio(members, planned, capital, lookup, valid):
    rows = [lookup[i] for i in members]
    result = dict(members=list(members),fundedSlots=len(members),plannedSlots=planned,idleSlots=planned-len(members),
        fundedCapital=len(members)*capital,plannedCapital=planned*capital,
        utilizationPct=100*len(members)/planned if planned else 0.,available=valid)
    if valid:
        pnl = math.fsum(r['netPnl'] for r in rows)
        result.update(netPnl=pnl,returnPct=100*pnl/(planned*capital) if planned else 0.,
            investedReturnPct=100*pnl/(len(members)*capital) if members else None,
            fees=math.fsum(r['fees'] for r in rows),trades=sum(r['trades'] for r in rows),
            sizingRejections=sum(r['sizingRejectionCount'] for r in rows),
            forcedExits={reason:dict(count=sum(r['forcedExits'][reason]['count'] for r in rows),
                netPnl=math.fsum(r['forcedExits'][reason]['netPnl'] for r in rows)) for reason in ('terminal','gapBoundary')})
    else:
        result.update(netPnl=None,returnPct=None,investedReturnPct=None,fees=None,trades=None,sizingRejections=None,forcedExits=None)
    return result


def quantile(values, q):
    values = sorted(values); position=(len(values)-1)*q
    low=int(position); high=min(low+1,len(values)-1)
    return values[low]+(position-low)*(values[high]-values[low])


def distribution(values, selected):
    if selected is None: return dict(p10=None,median=None,p90=None,below=None,equal=None,above=None,reason='noAvailableMonths')
    equal = sum(abs(v-selected) <= max(1e-9*max(abs(v),abs(selected)),1e-8) for v in values)
    below = sum(v < selected and abs(v-selected) > max(1e-9*max(abs(v),abs(selected)),1e-8) for v in values)
    return dict(p10=quantile(values,.1),median=quantile(values,.5),p90=quantile(values,.9),below=below,equal=equal,
        above=len(values)-equal-below,reason=None)


def path_values(path, members, roster, capital, planned):
    columns = [roster.index(i) for i in sorted(members)]
    return np.array([100*math.fsum(float(path['pnl'][j,n]) for j in columns)/(planned*capital) if planned else 0.
        for n in range(len(path['times']))],dtype=np.float64)


def calculate(doc, ledger, paths, roster, scores):
    """No parent/scorer access; one common future mask after memberships freeze."""
    lookup = {(r['month'],r['instrumentId']):r for r in ledger}
    batches = {(s['selectorId'],s['month']):s for s in scores}
    output, curves = [], {}
    capital=doc['referenceCapital']; repetitions=doc['randomControl']['replicates']
    for selector in doc['selectors']:
        sid=selector['selectorId']
        for k in doc['kValues']:
            months=[]
            for month in doc['evaluationMonths']:
                batch=batches[sid,month]; ranked=rank(batch['rows'])
                pool=sorted(r['instrumentId'] for r in ranked)
                selected=[r['instrumentId'] for r in ranked[:k]]
                draws=[random_order(pool,doc['randomControl']['seed'],replicate,month)[:k] for replicate in range(repetitions)]
                facts={i:lookup[month,i] for i in pool}
                failures=[dict(instrumentId=i,reason='noObservedBars' if not facts[i]['available'] else 'insufficientCoverage')
                    for i in pool if not covered(facts[i],doc['pool']['minObservedBarsPct'])]
                valid=not failures
                top=portfolio(selected,k,capital,facts,valid)
                all_=portfolio(pool,len(pool),capital,facts,valid)
                random=[dict(replicateIndex=n,**portfolio(m,k,capital,facts,valid)) for n,m in enumerate(draws)]
                tie=boundary_tie(ranked,k)
                neutral=top['returnPct']
                if valid and tie:
                    untied=[i for i in selected if i not in tie['members']]
                    pnl=math.fsum(facts[i]['netPnl'] for i in untied)+tie['slots']/tie['totalMembers']*math.fsum(facts[i]['netPnl'] for i in tie['members'])
                    neutral=100*pnl/(k*capital)
                curve_ids={}
                for name,p in (('selected',top),('allEligible',all_)):
                    identity=semantic_digest([sid,k,month,name]); curve_ids[name]=identity
                    values=path_values(paths[month],p['members'],roster,capital,p['plannedSlots']) if valid else np.empty(0,np.float64)
                    curves[identity]=dict(times=paths[month]['times'] if valid else np.empty(0,np.int64),values=values)
                    if valid and len(values): require(math.isclose(float(values[-1]),p['returnPct'],rel_tol=1e-9,abs_tol=1e-8),'monthly path endpoint')
                months.append(dict(month=month,eligiblePool=pool,eligibleCount=len(pool),rankedInstruments=[r['instrumentId'] for r in ranked],
                    available=valid,unavailableReasons=failures,selected=top,allEligible=all_,random=random,
                    boundaryTie=tie,tieNeutralExpectedReturnPct=neutral,curves=curve_ids,
                    commonExclusions=batch['commonExclusions'],methodExclusions=[r for r in batch['rows'] if not r['eligible']]))
            valid_months=[m for m in months if m['available']]; all_valid=len(valid_months)==len(months)
            used=[m['month'] for m in valid_months]
            subtotal={name:math.fsum(m[name]['returnPct'] for m in valid_months) if used else None for name in ('selected','allEligible')}
            random_totals=[math.fsum(m['random'][n]['returnPct'] for m in valid_months) if used else None for n in range(repetitions)]
            full={}
            for name in ('selected','allEligible'):
                maximum=None
                if all_valid:
                    peak=0.;maximum=0.;previous=[]
                    for m in months:
                        offset=math.fsum(previous)
                        for v in curves[m['curves'][name]]['values']:
                            current=offset+float(v);peak=max(peak,current);maximum=max(maximum,peak-current)
                        previous.append(m[name]['returnPct'])
                full[name]=dict(cumulativeReturnPp=subtotal[name] if all_valid else None,maxDrawdownPp=maximum,
                    netPnl=math.fsum(m[name]['netPnl'] for m in valid_months) if all_valid else None)
            partial=dict(months=used,usedMonths=len(used),requestedMonths=len(months),
                omitted=[dict(month=m['month'],reasons=m['unavailableReasons']) for m in months if not m['available']],
                selectedReturnPp=subtotal['selected'],allEligibleReturnPp=subtotal['allEligible'],
                randomReturnPp=random_totals,distribution=distribution(random_totals,subtotal['selected']))
            output.append(dict(selectorId=sid,k=k,months=months,fullPeriodAvailable=all_valid,fullPeriod=full,
                randomFullReturnPp=random_totals if all_valid else [None]*repetitions,
                randomFullDistribution=distribution(random_totals,subtotal['selected']) if all_valid else
                    dict(p10=None,median=None,p90=None,below=None,equal=None,above=None,reason='incompleteEvaluationPeriod'),
                matchedAvailableMonths=partial,underfilledMonths=sum(m['eligibleCount']<k for m in months),
                emptyPoolMonths=sum(not m['eligiblePool'] for m in months)))
    return dict(schema='pattern-lab.selection-results.v1',comparisons=output),curves
