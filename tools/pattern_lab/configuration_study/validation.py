"""Compact consistency checks; these do not replay or certify account execution."""
import math

from ..accounts.contracts import require
from ..account_study.common import closed, digest_id
from .request import result_id
from .curves import SAFE, VERSION
from .metrics import NATIVE_FIELDS, VERSION as METRICS_VERSION, month_equivalents, annualized, DAY
from data_provider.manifest import to_epoch_ms

METRIC_KEYS=(*NATIVE_FIELDS,'metricsVersion','performanceStatus','mtmMaxDdPct','balanceMaxDdPct','readiness',
    'dailySharpe','dailySharpeStatus','dailySharpeObservations','dailySharpeActiveDays','monthEquivalents',
    'tradesPerMonth','annualizedProfitPct','annualizedProfitStatus','annualizedProfitShortSpan')


def finite_json(value):
    if type(value) is float:require(math.isfinite(value),'nonfinite compact number')
    elif type(value) is dict:
        for v in value.values():finite_json(v)
    elif type(value) is list:
        for v in value:finite_json(v)


def ints(values, *, ordered=False):
    require(type(values) is list and all(type(v) is int and abs(v)<=SAFE for v in values),'JS-safe integer array')
    if ordered:require(all(a<b for a,b in zip(values,values[1:])),'strict display timestamps')


def metric_row(row,config,stage,synthetic):
    closed(row,('configurationId','resultId','stageId','status','metrics','execution'))
    require(row['configurationId']==config['configurationId'] and row['resultId']==result_id(config['configurationId'],stage)
            and row['stageId']==stage['stageId'] and row['status']=='complete','metric result identity/status')
    m=row['metrics'];closed(m,METRIC_KEYS);finite_json(m)
    require(m['metricsVersion']==METRICS_VERSION and m['performanceStatus'] in ('available','noObservedBars'),'metrics version/availability')
    require(type(m['initialCapital']) in (int,float) and m['initialCapital']>0,'metric initial capital')
    for k in ('trades','wins','losses','breakevens'):
        require(type(m[k]) is int and m[k]>=0,'metric count')
    require(m['trades']==m['wins']+m['losses']+m['breakevens'],'trade count reconciliation')
    for k in ('finalCapital','netPnl','grossPnl','fees','returnPct','timeInMarketPct','monthEquivalents','tradesPerMonth'):
        require(type(m[k]) in (int,float),'financial number type')
    for k in ('winRatePct','profitFactor','maxEntryLeverage','mtmMaxDdPct','balanceMaxDdPct','dailySharpe','annualizedProfitPct'):
        require(m[k] is None or type(m[k]) in (int,float),'optional financial number type')
    start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
    require(m['monthEquivalents']==month_equivalents(start,end) and
            m['tradesPerMonth']==m['trades']/m['monthEquivalents'],'calendar rate reconciliation')
    require(m['winRatePct']==(100*m['wins']/m['trades'] if m['trades'] else None),'win rate reconciliation')
    cov=m['coverage'];closed(cov,('requestedStartMs','requestedEndMs','expectedBars','observedBars','missingBars',
        'missingHeadSlots','missingTailSlots','firstObservedOpenMs','lastObservedCloseMs'))
    require(cov['requestedStartMs']==start and cov['requestedEndMs']==end,'coverage stage bounds')
    for k in ('expectedBars','observedBars','missingBars','missingHeadSlots','missingTailSlots'):
        require(type(cov[k]) is int and cov[k]>=0,'coverage count')
    require(cov['expectedBars']==cov['observedBars']+cov['missingBars'] and
        cov['expectedBars']==(end-start)//(config['timeframeMinutes']*60000),'coverage reconciliation')
    require((m['performanceStatus']=='available')==(cov['observedBars']>0),'performance/coverage status')
    if not cov['observedBars']:
        require(all(m[k] is None for k in ('mtmMaxDdPct','balanceMaxDdPct','dailySharpe')),'unavailable observed performance')
    else:
        require(all(type(cov[k]) is int for k in ('firstObservedOpenMs','lastObservedCloseMs')) and
            start<=cov['firstObservedOpenMs']<cov['lastObservedCloseMs']<=end,'observed coverage extent')
    require(m['returnPct']==100*(m['finalCapital']/m['initialCapital']-1) and
            m['netPnl']==m['finalCapital']-m['initialCapital'],'metric capital reconciliation')
    require(m['profitFactorStatus'] in ('available','noTrades','noLosses'),'profit factor status')
    require((m['profitFactor'] is not None)==(m['profitFactorStatus']=='available'),'profit factor availability')
    require(m['profitFactorStatus']!='noTrades' or m['trades']==0,'noTrades status')
    require(m['trades']!=0 or m['winRatePct'] is None and m['profitFactorStatus']=='noTrades','no-trade metrics')
    require(m['dailySharpeStatus'] in ('available','invalidDailySeries','noTrades','tooFewObservations','zeroOrInvalidVariance'),
            'Sharpe status')
    require((m['dailySharpe'] is not None)==(m['dailySharpeStatus']=='available'),'Sharpe availability')
    for k in ('dailySharpeObservations','dailySharpeActiveDays'):
        require(m[k] is None or type(m[k]) is int and m[k]>=0,'Sharpe count')
    if m['dailySharpeObservations'] is not None:
        require(m['dailySharpeActiveDays'] is not None and m['dailySharpeActiveDays']<=m['dailySharpeObservations'],'active days')
    require(m['annualizedProfitStatus'] in ('available','shortSpan','nonpositiveMultiple','invalidOrOverflow','noObservedBars') and
            type(m['annualizedProfitShortSpan']) is bool,'annualized status')
    require((m['annualizedProfitPct'] is not None)==(m['annualizedProfitStatus']=='available'),'annualized availability')
    ann,status,short=annualized(m['returnPct'],(end-start)/DAY)
    require(m['annualizedProfitShortSpan']==short and m['annualizedProfitStatus']==
        (status if cov['observedBars'] else 'noObservedBars'),'annualization duration/status')
    if ann is not None and cov['observedBars']:
        require(math.isclose(m['annualizedProfitPct'],ann,rel_tol=1e-12,abs_tol=1e-12),'annualization consistency')
    e=row['execution'];closed(e,('kind','accountId','preparationId','commandId','validation','inputs','rules','auxiliary'))
    require(type(e['inputs']) is list and type(e['auxiliary']) is list,'execution input facts')
    if synthetic:
        require(e['kind']=='syntheticFixture' and all(e[k] is None for k in ('accountId','preparationId','commandId','validation')),
                'synthetic fixture cannot claim native execution')
        require(e['inputs']==e['auxiliary']==[] and e['rules'] is None,'synthetic fixture has no native input/rule receipt')
    else:
        require(e['kind']=='native' and e['validation']=='nativeValidated','native validation receipt required')
        for k in ('accountId','preparationId','commandId'):digest_id(e[k])
        require(e['inputs'] and type(e['rules']) is dict,'native input/rule facts required')
        ids=[]
        for item in e['inputs']:
            closed(item,('inputId','header','inputFingerprint'));digest_id(item['inputFingerprint']);ids.append(item['inputId'])
            h=item['header']
            require(type(h) is dict and h['start_ms']==start and h['end_ms']==end,'native input stage bounds')
            if item['inputId']=='target':
                require(h['instrument_id']==config['instrumentId'] and h['timeframe_minutes']==config['timeframeMinutes'],
                        'native target binding coordinates')
        require(ids==sorted(set(ids)) and 'target' in ids,'native binding roster')
        for item in e['auxiliary']:
            closed(item,('inputId','coveragePolicy','consumedSliceFingerprint'));digest_id(item['consumedSliceFingerprint'])
            require(item['coveragePolicy'] in ('allowUnknown','requireComplete'),'auxiliary coverage policy')


def curve_row(row,metric,stage):
    closed(row,('configurationId','resultId','stageId','curve'))
    for key in ('configurationId','resultId','stageId'):require(row[key]==metric[key],'curve/metric identity')
    c=row['curve'];closed(c,('curveVersion','valueScale','timeMs','equityQ','balanceQ','cashGaps','status','endpoint'))
    require(c['curveVersion']==VERSION and type(c['valueScale']) is int and c['valueScale']==100,'curve version/scale')
    t,e,b=c['timeMs'],c['equityQ'],c['balanceQ']
    ints(t,ordered=True);ints(e);ints(b)
    start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
    require(len(t)==len(e)==len(b)>0 and t[0]==start and e[0]==b[0]==0 and t[-1]<=end,'curve arrays/anchor')
    require(type(c['cashGaps']) is list,'cash gap list')
    if c['status']=='noObservedBars':
        require(len(t)==1 and c['endpoint']=='unavailable' and not c['cashGaps'] and
                metric['metrics']['performanceStatus']=='noObservedBars','unavailable path')
    else:
        require(c['status']=='available' and c['endpoint'] in ('observedClose','flatCashExtension') and t[-1]==end,
                'available endpoint')
        require(metric['metrics']['performanceStatus']=='available','curve/metric observed status')
        last=metric['metrics']['coverage']['lastObservedCloseMs']
        require(last in t and (c['endpoint']=='observedClose')==(last==end),'actual final observation')
        value=metric['metrics']['returnPct'];tolerance=.005+max(1e-10,1e-12*abs(value))
        require(e[-1]==b[-1] and abs(b[-1]/100-value)<=tolerance,'curve/metric terminal return')
    previous=start
    lookup={v:i for i,v in enumerate(t)}
    for gap in c['cashGaps']:
        closed(gap,('startMs','endMs','cashQ'));ints(list(gap.values()))
        left,right,q=gap['startMs'],gap['endMs'],gap['cashQ']
        require(previous<=left<right<=end and left in lookup and right in lookup,'cash gap extent/support')
        require(all(e[lookup[x]]==b[lookup[x]]==q for x in (left,right)),'cash gap values')
        previous=right


def price_row(row,instrument,stage):
    closed(row,('instrumentId','stageId','price'))
    require(row['instrumentId']==instrument and row['stageId']==stage['stageId'],'price scope')
    p=row['price'];closed(p,('curveVersion','valueScale','status','timeMs','priceQ','anchorPrice','anchorTimeMs',
        'lastObservedTimeMs','coverageGaps','sourceFingerprint'));finite_json(p)
    require(p['curveVersion']==VERSION and type(p['valueScale']) is int and p['valueScale']==100,'price version/scale')
    ints(p['timeMs'],ordered=True);ints([v for v in p['priceQ'] if v is not None])
    require(len(p['timeMs'])==len(p['priceQ']),'price arrays')
    start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
    if p['status']=='noObservedBars':
        require(not p['timeMs'] and all(p[k] is None for k in ('anchorPrice','anchorTimeMs','lastObservedTimeMs')),
                'unavailable price anchor')
    else:
        require(p['status']=='available' and p['anchorPrice']>0 and p['timeMs'] and
                p['timeMs'][0]==p['anchorTimeMs']>=start and p['priceQ'][0]==0 and p['timeMs'][-1]<=end and
                p['lastObservedTimeMs'] in p['timeMs'],'price anchors/bounds')
    previous=start
    require(type(p['coverageGaps']) is list,'price gap list')
    for g in p['coverageGaps']:
        closed(g,('startMs','endMs'));ints(list(g.values()))
        require(previous<=g['startMs']<g['endMs']<=end,'price coverage gap')
        previous=g['endMs']
    if p['sourceFingerprint'] is not None:digest_id(p['sourceFingerprint'])
