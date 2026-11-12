"""Deterministic compact-only capacity recipe, seed 20261112; never native evidence."""
from copy import deepcopy
import math
from data_provider.manifest import to_epoch_ms
from tools.pattern_lab.configuration_study.request import normalize_request,configurations,result_id
from tools.pattern_lab.configuration_study.artifacts import Writer,groups
from tools.pattern_lab.configuration_study.metrics import month_equivalents,annualized,DAY,VERSION
from tools.pattern_lab.configuration_study.curves import quantize,VERSION as CURVE_VERSION
from ._configuration_helpers import request as small_request

SEED=20261112


def request(per_tf=200):
    doc=small_request(['BYBIT_'+s+'USDT' for s in ('AAA','BBB','CCC','DDD')],filtered=False,multi_tf=False)
    base=deepcopy(doc['allocations'][0]);doc['allocations']=[]
    for tf in (30,60,120,240):
        for i in range(per_tf):
            a=deepcopy(base);a.update(allocationId=f'tf{tf}-rr{i}',timeframeMinutes=tf)
            a['policy']['rewardRisk']=1+i/1000
            doc['allocations'].append(a)
    doc['stages']=[]
    for name,start,end in (('IS','2020-01-01','2024-01-01'),('HO1','2024-01-01','2025-01-01'),('HO2','2025-01-01','2025-11-01')):
        start+='T00:00:00Z';end+='T00:00:00Z';warm='2019-12-01T00:00:00Z'
        doc['stages'].append(dict(stageId=name,startUtc=start,endUtc=end,preparationStartUtc=warm,
            protocol=dict(schema_version=1,protocol_id='capacity-'+name,development=dict(start_utc=start,end_utc=end),
                reserved=dict(start_utc=end,end_utc='2026-01-01T00:00:00Z'),earliest_warmup_start_utc=warm)))
    return normalize_request(doc)


def unit(config,stage,ordinal):
    start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
    times=list(range(start,end,3*DAY))+[end]
    values=[quantize((i/(len(times)-1))*(5+(ordinal%151)/5)+math.sin(i*.37+ordinal)*2) for i in range(len(times))]
    values[0]=0
    balance=[v-(i%5)*2 for i,v in enumerate(values)];balance[-1]=values[-1]
    final=10000*(1+values[-1]/10000);ret=100*(final/10000-1);net=final-10000
    months=month_equivalents(start,end);ann,status,short=annualized(ret,(end-start)/DAY)
    trades=100+ordinal%73;wins=trades//2;bars=(end-start)//(config['timeframeMinutes']*60000)
    metrics=dict(initialCapital=10000.,finalCapital=final,netPnl=net,grossPnl=net+trades*.1,fees=trades*.1,returnPct=ret,
        trades=trades,wins=wins,losses=trades-wins,breakevens=0,winRatePct=100*wins/trades,profitFactor=1.1+ordinal%9/10,
        profitFactorStatus='available',timeInMarketPct=40.,maxEntryLeverage=2.,exitReasons={'stop':trades},
        attemptDispositions={'filled':trades},terminalStatus='closed',coverage=dict(requestedStartMs=start,requestedEndMs=end,
            expectedBars=bars,observedBars=bars,missingBars=0,missingHeadSlots=0,missingTailSlots=0,
            firstObservedOpenMs=start,lastObservedCloseMs=end),metricsVersion=VERSION,performanceStatus='available',
        mtmMaxDdPct=2.,balanceMaxDdPct=2.1,readiness=dict(commands={},unknownExitDecisions=0,filterUnknownDecisionBars=0,
            filterUnknownAttempts=0,filterBlockedAttempts=0),dailySharpe=.5+ordinal%20/10,dailySharpeStatus='available',
        dailySharpeObservations=(end-start)//DAY,dailySharpeActiveDays=(end-start)//DAY,monthEquivalents=months,
        tradesPerMonth=trades/months,annualizedProfitPct=ann,annualizedProfitStatus=status,annualizedProfitShortSpan=short)
    common=dict(configurationId=config['configurationId'],resultId=result_id(config['configurationId'],stage),stageId=stage['stageId'])
    return dict(metric=dict(**common,status='complete',metrics=metrics,execution=dict(kind='syntheticFixture',accountId=None,
        preparationId=None,commandId=None,validation=None,inputs=[],rules=None,auxiliary=[])),curve=dict(**common,curve=dict(
        curveVersion=CURVE_VERSION,valueScale=100,timeMs=times,equityQ=values,balanceQ=balance,cashGaps=[],status='available',endpoint='observedClose')))


def build(root,implementation,*,per_tf=200):
    root.mkdir(parents=True,exist_ok=False);normalized=request(per_tf);doc=normalized.document
    writer=Writer(root,normalized,implementation,synthetic=True);rows=configurations(doc);ordinal=0
    for stage in doc['stages']:
        for (instrument,tf),group in groups(rows):
            writer.group(stage['stageId'],instrument,tf,(unit(c,stage,ordinal+i) for i,c in enumerate(group)))
            ordinal+=len(group)
    for j,instrument in enumerate(doc['instruments']):
        for stage in sorted(doc['stages'],key=lambda s:s['stageId']):
            start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'));times=list(range(start,end+1,DAY))
            values=[quantize(i*.01+math.sin(i*.05+j)) for i in range(len(times))];values[0]=0
            writer.price(dict(instrumentId=instrument,stageId=stage['stageId'],price=dict(curveVersion=CURVE_VERSION,valueScale=100,
                status='available',timeMs=times,priceQ=values,anchorPrice=100.,anchorTimeMs=start,lastObservedTimeMs=end,
                coverageGaps=[],sourceFingerprint=None)))
    writer.finish(dict(recipe='20261112; four instruments, four TFs, varying reward/risk and sinusoidal display paths'),
                  dict(state='complete',completedResults=3*len(rows)))
    return root
