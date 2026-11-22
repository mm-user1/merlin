"""Pure descriptive reducers with explicit availability and denominator rules."""
import math
from .request import calendar, effective_criteria, cell_id, month_boundary

VERSION = 'strategy-screen-analysis.1'
METRICS = ('returnPct','annualizedProfitPct','mtmMaxDdPct','trades','tradesPerMonth')


def finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def covered(unit):
    c=unit['metrics']['coverage']
    return c['expectedBars']>0 and c['observedBars']==c['expectedBars']


def metric_available(unit, name):
    m=unit['metrics']
    return finite(m.get(name)) and (name=='trades' or m['performanceStatus']=='available') and (
        name!='annualizedProfitPct' or m['annualizedProfitStatus']=='available')


def classification(unit, criteria):
    reasons=[]; m=unit['metrics']
    if unit['status']!='complete':reasons.append('incompleteExecution')
    if not covered(unit):reasons.append('incompleteExpectedBars')
    for k in METRICS[:4]:
        if not metric_available(unit,k):reasons.append(k+'Unavailable')
    valid=not reasons
    known=unit['status']=='complete' and finite(m.get('trades')) and m['trades']>=0
    return dict(classifiable=valid,reasons=reasons,
        profitable=m['returnPct']>0 if valid else None,
        interesting=(m['annualizedProfitPct']>=criteria['annualizedProfitPct'] and
            m['mtmMaxDdPct']<=criteria['mtmMaxDdPct'] and m['trades']>=criteria['minClosedTrades']) if valid else None,
        lowActivity=m['trades']<criteria['minClosedTrades'] if known else None)


def stats(values):
    values=sorted(v for v in values if finite(v));n=len(values)
    def q(p):
        if not n:return None
        x=(n-1)*p;left=int(x);right=min(left+1,n-1)
        return values[left]+(values[right]-values[left])*(x-left)
    return dict(n=n,status='available' if n else 'unavailable',reason=None if n else 'noAvailableValues',
        min=q(0),q25=q(.25),median=q(.5),q75=q(.75),max=q(1))


def share(success, declared, available, *, require_available=True):
    return dict(success=success,declared=declared,available=available,missing=declared-available,
        pct=100*success/declared if declared and (available or not require_available) else None,
        status='available' if declared and (available or not require_available) else 'unavailable',
        reason=None if declared and (available or not require_available) else 'noAvailableValues')


def project(unit, config, doc):
    """Narrow ephemeral values for reductions, never a cache of execution/raw rows."""
    criteria=effective_criteria(doc,config['timeframeMinutes'])
    return dict(configurationId=unit['configurationId'],cellId=config['cellId'],periodId=unit['periodId'],
        status=unit['status'],criteria=criteria,classification=classification(unit,criteria),
        coverageComplete=covered(unit),initialCapital=unit['metrics']['initialCapital'],
        metrics={k:unit['metrics'][k] if metric_available(unit,k) else None for k in METRICS},
        months=[{k:m[k] for k in ('month','startMs','endMs','openingEquity','closingEquity','exact',
            'contributionPp','cumulativeReturnPct','closedTrades')} for m in unit['months']])


def equal_cell(rows, success):
    cells=sorted({r['cellId'] for r in rows})
    return math.fsum(100*sum(success(r) for r in rows if r['cellId']==c)/sum(r['cellId']==c for r in rows)
                     for c in cells)/len(cells) if cells else None


def residual(row,k,selected=None):
    months=row['months']
    if len(months)<k:return dict(value=None,selectedMonths=[],reason='tooFewMonths')
    if not all(m['exact'] for m in months) or not finite(row['metrics']['returnPct']):
        return dict(value=None,selectedMonths=[],reason='incompleteMonthlyEvidence')
    chosen=selected if selected is not None else [m['month'] for m in sorted(months,key=lambda m:(-m['contributionPp'],m['month']))[:k]]
    value=0.0 if len(months)==k else row['metrics']['returnPct']-math.fsum(m['contributionPp'] for m in months if m['month'] in chosen)
    return dict(value=value,selectedMonths=chosen,reason=None)


def residual_summary(rows, values, *, reason=None, selected=None):
    available=sum(finite(v) for v in values);positive=sum(v>0 for v in values if finite(v))
    cells=sorted({r['cellId'] for r in rows})
    equal=math.fsum(100*sum(finite(v) and v>0 for r,v in zip(rows,values) if r['cellId']==c)/
        sum(r['cellId']==c for r in rows) for c in cells)/len(cells) if cells and available else None
    return dict(declared=len(rows),available=available,missing=len(rows)-available,positive=positive,
        pooledPositivePct=100*positive/len(rows) if rows and available else None,equalCellPositivePct=equal,
        residualPp=stats(values),selectedMonths=selected,status='available' if available else 'unavailable',
        reason=reason if reason else None if available else 'noAvailableValues')


def concentration(rows):
    labels=[m['month'] for m in rows[0]['months']] if rows else []
    complete=bool(rows) and all([m['month'] for m in r['months']]==labels and
        all(m['exact'] for m in r['months']) for r in rows)
    cells=sorted({r['cellId'] for r in rows});means=[]
    for i,label in enumerate(labels):
        available=all(r['months'][i]['exact'] for r in rows)
        mean=math.fsum(math.fsum(r['months'][i]['contributionPp'] for r in rows if r['cellId']==c)/
            sum(r['cellId']==c for r in rows) for c in cells)/len(cells) if available else None
        means.append(dict(month=label,value=mean,reason=None if available else 'incompleteScopeEvidence'))
    own,common={},{}
    for k in (1,2):
        own[str(k)]=residual_summary(rows,[residual(r,k)['value'] for r in rows])
        chosen=[m['month'] for m in sorted(means,key=lambda m:(-m['value'],m['month']))[:k]] if complete and len(labels)>=k else []
        reason=None if len(chosen)==k else 'tooFewMonths' if len(labels)<k else 'incompleteScopeEvidence'
        common[str(k)]=residual_summary(rows,[residual(r,k,chosen)['value'] for r in rows] if not reason else [None]*len(rows),
            reason=reason,selected=chosen)
    return dict(own=own,common=common,equalCellMeanContributions=means)


def window(row,start,end):
    chosen=[m for m in row['months'] if start<=m['startMs'] and m['endMs']<=end]
    reason='invalidWindowGeometry' if not chosen or chosen[0]['startMs']!=start or chosen[-1]['endMs']!=end else (
        'incompleteMonthlyEvidence' if not all(m['exact'] for m in chosen) else None)
    if reason:return dict(exact=False,reason=reason,returnPct=None,contributionPp=None,trades=None,returnReason=reason)
    op,cl=chosen[0]['openingEquity'],chosen[-1]['closingEquity']
    return dict(exact=True,reason=None,returnPct=100*(cl/op-1) if op>0 else None,
        contributionPp=100*(cl-op)/row['initialCapital'],trades=sum(m['closedTrades'] for m in chosen),
        returnReason=None if op>0 else 'nonpositiveOpeningEquity')


def result_analytics(row,doc):
    """Canonical per-setting attribution/windows for consumers; no UI inference."""
    period=next(p for p in doc['periods'] if p['periodId']==row['periodId']);slots=calendar(period)
    def unavailable(reason):
        return dict(exact=False,reason=reason,returnPct=None,contributionPp=None,trades=None,returnReason=reason)
    latest=window(row,slots[-3]['startMs'],slots[-1]['endMs']) if len(slots)>=3 else unavailable('tooFewMonths')
    new=unavailable('firstScreenSnapshot' if doc['previousReviewEndUtc'] is None else 'notRecentPeriod')
    if row['periodId']=='recent' and doc['previousReviewEndUtc'] is not None:
        new=window(row,month_boundary(doc['previousReviewEndUtc']),slots[-1]['endMs'])
    return dict(version=VERSION,effectiveCriteria=row['criteria'],ownResiduals={str(k):residual(row,k) for k in (1,2)},
        latestThreeMonths=latest,segments={s['segmentId']:window(row,month_boundary(s['startUtc']),month_boundary(s['endUtc']))
            for s in doc['segments'] if s['periodId']==row['periodId']},newOnly=new)


def window_summary(rows,slots,start=None,end=None,reason=None):
    chosen=[m for m in slots if start is not None and start<=m['startMs'] and m['endMs']<=end]
    values=[window(r,start,end) for r in rows] if reason is None else []
    returns=[v['returnPct'] for v in values];available=sum(finite(v) for v in returns)
    exact=sum(v['exact'] for v in values)
    out=dict(startMs=start,endMs=end,months=[m['month'] for m in chosen],declared=len(rows),exactWindowAvailable=exact,
        status='available' if exact else 'unavailable',reason=reason or (None if exact else 'noExactEvidence'),
        returnPct=stats(returns),contributionPp=stats(v['contributionPp'] for v in values),trades=stats(v['trades'] for v in values),
        profitable=share(sum(v>0 for v in returns if finite(v)),len(rows),available))
    if not available:
        out['returnPct']['reason']=reason or ('nonpositiveOpeningEquity' if exact else 'noExactEvidence')
        out['profitable']['reason']=out['returnPct']['reason']
    return out


def period_summary(rows,period,doc):
    n=len(rows);slots=calendar(period);flags=[r['classification'] for r in rows]
    valid=sum(f['classifiable'] for f in flags);full=sum(r['coverageComplete'] for r in rows)
    breadth={}
    for name in ('profitable','interesting'):
        breadth[name]=dict(**share(sum(f[name] is True for f in flags),n,valid,require_available=False),
            equalCellPct=equal_cell(rows,lambda r:r['classification'][name] is True))
    medians={k:stats(r['metrics'][k] for r in rows if r['status']=='complete' and r['coverageComplete']) for k in METRICS}
    failures=dict(declared=n,classifiable=valid,unavailable=n-valid,
        annualizedReturn=sum(r['classification']['classifiable'] and r['metrics']['annualizedProfitPct']<r['criteria']['annualizedProfitPct'] for r in rows),
        drawdown=sum(r['classification']['classifiable'] and r['metrics']['mtmMaxDdPct']>r['criteria']['mtmMaxDdPct'] for r in rows),
        activity=sum(r['classification']['classifiable'] and r['metrics']['trades']<r['criteria']['minClosedTrades'] for r in rows))
    monthly=[];conc=concentration(rows)
    for i,slot in enumerate(slots):
        mm=[r['months'][i] for r in rows]
        contribution=stats(m['contributionPp'] for m in mm);cumulative=stats(m['cumulativeReturnPct'] for m in mm)
        monthly.append(dict(**slot,declared=n,contribution=contribution,cumulative=cumulative,
            positiveContribution=share(sum(m['contributionPp']>0 for m in mm if finite(m['contributionPp'])),n,contribution['n']),
            equalCellMeanContributionPp=conc['equalCellMeanContributions'][i]['value'] if rows else None,
            equalCellMeanReason=conc['equalCellMeanContributions'][i]['reason'] if rows else 'emptyScope'))
    latest=window_summary(rows,slots,slots[-3]['startMs'],slots[-1]['endMs']) if len(slots)>=3 else window_summary(rows,slots,reason='tooFewMonths')
    segments={s['segmentId']:window_summary(rows,slots,month_boundary(s['startUtc']),month_boundary(s['endUtc']))
        for s in doc['segments'] if s['periodId']==period['periodId']}
    new=window_summary(rows,slots,reason='firstScreenSnapshot' if doc['previousReviewEndUtc'] is None else 'notRecentPeriod')
    if period['periodId']=='recent' and doc['previousReviewEndUtc'] is not None:
        new=window_summary(rows,slots,month_boundary(doc['previousReviewEndUtc']),slots[-1]['endMs'])
    return dict(declared=n,completed=sum(r['status']=='complete' for r in rows),coverageComplete=full,incompleteCoverage=n-full,
        classifiable=valid,unclassifiable=n-valid,activity=dict(known=sum(f['lowActivity'] is not None for f in flags),
        unknown=sum(f['lowActivity'] is None for f in flags),low=sum(f['lowActivity'] is True for f in flags)),
        breadth=breadth,metrics=medians,failures=failures,monthly=monthly,concentration=conc,latestThreeMonths=latest,segments=segments,newOnly=new)


def persistence(earlier,recent):
    e={r['configurationId']:r['classification'] for r in earlier};r={r['configurationId']:r['classification'] for r in recent}
    pairs=[(e[k],r[k]) for k in sorted(e.keys()&r.keys())]
    valid=[(a,b) for a,b in pairs if a['classifiable'] and b['classifiable']]
    out=dict(matched=len(pairs),unmatched=len(e.keys()^r.keys()),unavailablePairs=len(pairs)-len(valid),classifiablePairs=len(valid))
    for name in ('profitable','interesting'):
        values={}
        for success in (True,False):
            group=[b for a,b in valid if a[name]==success]
            values['earlierSuccess' if success else 'earlierNonsuccess']=share(sum(b[name] is True for b in group),len(group),len(group))
        a,b=(values[k]['pct'] for k in ('earlierSuccess','earlierNonsuccess'))
        values['retentionDifferencePp']=a-b if a is not None and b is not None else None;out[name]=values
    return out


def scopes(doc):
    cells=[dict(cellId=cell_id(c['instrumentId'],c['timeframeMinutes']),**c) for c in doc['cells']]
    result=[dict(scopeId='cell:'+c['cellId'],kind='cell',key=c['cellId'],cells=[c['cellId']]) for c in cells]
    for kind,field in (('instrument','instrumentId'),('timeframe','timeframeMinutes')):
        for key in sorted({c[field] for c in cells}):
            result.append(dict(scopeId=kind+':'+str(key),kind=kind,key=key,cells=[c['cellId'] for c in cells if c[field]==key]))
    result.append(dict(scopeId='all',kind='panel',key='all',cells=[c['cellId'] for c in cells]))
    return result


def derive(doc, projections):
    """Canonical scope order: cells, instruments, timeframes, panel; periods logical."""
    for scope in scopes(doc):
        members=set(scope['cells']);groups={p['periodId']:[r for r in projections if r['cellId'] in members and r['periodId']==p['periodId']]
            for p in doc['periods']}
        periods={p['periodId']:period_summary(groups[p['periodId']],p,doc) for p in doc['periods']}
        e,r=periods['earlier'],periods['recent']
        changes={k+'BreadthPp':(r['breadth'][k]['equalCellPct']-e['breadth'][k]['equalCellPct']
            if r['breadth'][k]['equalCellPct'] is not None and e['breadth'][k]['equalCellPct'] is not None else None)
            for k in ('profitable','interesting')}
        for k in METRICS:
            a,b=e['metrics'][k]['median'],r['metrics'][k]['median']
            changes[k]=dict(value=b-a if a is not None and b is not None else None,
                unit='trades/month' if k=='tradesPerMonth' else 'trades' if k=='trades' else 'percentagePoints')
        yield dict(**scope,periods=periods,changes=changes,persistence=persistence(groups['earlier'],groups['recent']))
