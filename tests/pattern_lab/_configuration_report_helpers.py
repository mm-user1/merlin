"""Tracked small native/synthetic report inputs, independent of delivery archives."""
import base64
import json
from pathlib import Path
import re

from tools.pattern_lab.configuration_study import export_public_results, run_configuration_study
from ._configuration_helpers import inputs
from ._configuration_capacity import build


def native(root):
    request = inputs(root/'pack', count=1, multi_tf=True)
    owner = run_configuration_study(request, data_root=root/'pack', output_root=root/'owner')
    public = export_public_results(owner.root, output_root=root/'public')
    return owner, public


def synthetic(root, per_tf=2):
    return build(root, {'fixture':'report-tests'}, per_tf=per_tf)


def extract(path):
    html = Path(path).read_text(encoding='utf-8')
    manifest = json.loads(re.search(r'<script type="application/json" id="bootstrap">(.*?)</script>', html).group(1))
    blocks = [base64.b64decode(raw) for raw in re.findall(r'<script type="application/octet-stream" id="block-\d+">(.*?)</script>', html)]
    return manifest, blocks, html


def adversarial(root, *, hidden_variant=0):
    """Valid compact-only statuses, gaps, venue collisions and nonstandard TFs."""
    from copy import deepcopy
    from data_provider.manifest import to_epoch_ms
    from tools.pattern_lab.configuration_study.request import normalize_request, configurations
    from tools.pattern_lab.configuration_study.artifacts import Writer, groups
    from tools.pattern_lab.configuration_study.metrics import DAY, annualized
    from ._configuration_capacity import request, unit
    doc=request(2).document
    doc['instruments']=['BYBIT_AAAUSDT','BYBIT_BBBUSDT','OKX_AAAUSDT','OKX_CCCUSDT']
    # Native normalization supports positive multiples of the base five minutes.
    doc['allocations']=doc['allocations'][:4]
    for i,a in enumerate(doc['allocations']):
        a['timeframeMinutes']=45 if i<2 else 90
        a['allocationId']='generic-'+str(i)
    for stage,left,right in zip(doc['stages'],['2020-01-01','2020-03-01','2020-03-31'],['2020-03-01','2020-03-31','2020-05-31']):
        stage['startUtc']=left+'T00:00:00Z';stage['endUtc']=right+'T00:00:00Z'
        stage['protocol']['development']=dict(start_utc=stage['startUtc'],end_utc=stage['endUtc'])
        stage['protocol']['reserved']['start_utc']=stage['endUtc']
    normalized=normalize_request(doc);doc=normalized.document;rows=configurations(doc)
    root.mkdir(parents=True,exist_ok=False)
    writer=Writer(root,normalized,{'fixture':'report-adversarial-v1'},synthetic=True)
    ordinal={c['configurationId']:i for i,c in enumerate(rows)}
    for stage in doc['stages']:
        start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
        def units(group):
            for c in group:
                n=ordinal[c['configurationId']]
                u=unit(c,stage,n+(hidden_variant*51 if stage['stageId']=='HO2' else 0))
                m=u['metric']['metrics'];curve=u['curve']['curve']
                if n%4==0:
                    m.update(trades=0,wins=0,losses=0,breakevens=0,tradesPerMonth=0,winRatePct=None,
                        profitFactor=None,profitFactorStatus='noTrades',dailySharpe=None,dailySharpeStatus='noTrades',exitReasons={},attemptDispositions={})
                    m.update(finalCapital=10000.,netPnl=0.,grossPnl=0.,fees=0.,returnPct=0.,timeInMarketPct=0.,maxEntryLeverage=None)
                    m['annualizedProfitPct'],m['annualizedProfitStatus'],m['annualizedProfitShortSpan']=annualized(0.,(end-start)/DAY)
                    curve['equityQ']=[0]*len(curve['timeMs']);curve['balanceQ']=[0]*len(curve['timeMs'])
                elif n%5==3:
                    m.update(profitFactor=None,profitFactorStatus='noLosses',wins=m['trades'],losses=0,winRatePct=100.)
                if n%7==0:
                    m.update(finalCapital=10000.,netPnl=0.,grossPnl=0.,fees=0.,returnPct=0.,trades=0,wins=0,losses=0,breakevens=0,
                        tradesPerMonth=0,winRatePct=None,profitFactor=None,profitFactorStatus='noTrades',timeInMarketPct=0.,maxEntryLeverage=None,
                        mtmMaxDdPct=None,balanceMaxDdPct=None,performanceStatus='noObservedBars',dailySharpe=None,dailySharpeStatus='noTrades',
                        dailySharpeObservations=0,dailySharpeActiveDays=0,annualizedProfitPct=None,annualizedProfitStatus='noObservedBars',exitReasons={},attemptDispositions={})
                    cov=m['coverage'];cov.update(observedBars=0,missingBars=cov['expectedBars'],missingHeadSlots=cov['expectedBars'],missingTailSlots=cov['expectedBars'],firstObservedOpenMs=None,lastObservedCloseMs=None)
                    curve.update(timeMs=[start],equityQ=[0],balanceQ=[0],cashGaps=[],status='noObservedBars',endpoint='unavailable')
                elif n%4==2:
                    curve['equityQ'][1]=curve['balanceQ'][1]=0
                    tail=end-3*DAY;q=curve['balanceQ'][-1]
                    kept=[i for i,t in enumerate(curve['timeMs']) if t<tail]
                    curve['timeMs']=[curve['timeMs'][i] for i in kept]+[tail,end]
                    for field in ('equityQ','balanceQ'):curve[field]=[curve[field][i] for i in kept]+[q,q]
                    curve['endpoint']='flatCashExtension'
                    curve['cashGaps']=[dict(startMs=start,endMs=start+3*DAY,cashQ=0),dict(startMs=tail,endMs=end,cashQ=q)]
                    cov=m['coverage'];missing=3*DAY//(c['timeframeMinutes']*60000)
                    cov.update(firstObservedOpenMs=start+3*DAY,lastObservedCloseMs=tail,observedBars=cov['expectedBars']-2*missing,missingBars=2*missing,missingHeadSlots=missing,missingTailSlots=missing)
                    m['dailySharpeObservations']=m['dailySharpeActiveDays']=(end-start)//DAY-6
                if stage['stageId']=='HO2' and m['dailySharpeStatus']=='available':m['dailySharpe']=87654321.125+hidden_variant
                yield u
        for (instrument,tf),group in groups(rows):writer.group(stage['stageId'],instrument,tf,units(group))
    for i,instrument in enumerate(doc['instruments']):
        for stage in sorted(doc['stages'],key=lambda s:s['stageId']):
            start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
            price=dict(curveVersion='configuration-curves.1',valueScale=100,status='available',timeMs=[start,start+DAY,start+2*DAY,end],priceQ=[0,None,20,30],
                anchorPrice=100.,anchorTimeMs=start,lastObservedTimeMs=end,coverageGaps=[dict(startMs=start+DAY,endMs=start+2*DAY)],sourceFingerprint=None)
            if i==3:price.update(status='noObservedBars',timeMs=[],priceQ=[],anchorPrice=None,anchorTimeMs=None,lastObservedTimeMs=None,coverageGaps=[])
            writer.price(dict(instrumentId=instrument,stageId=stage['stageId'],price=price))
    writer.finish(dict(recipe='report-adversarial-v1'),dict(completedResults=3*len(rows)))
    return root

