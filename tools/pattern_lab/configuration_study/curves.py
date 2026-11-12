"""Display-only actual-time samples; account cash gaps differ from price gaps."""
import math
import numpy as np

from ..accounts.contracts import require

DAY=86400000
SAFE=2**53-1
VERSION='configuration-curves.1'


def safe_int(value):
    require(type(value) is int and abs(value)<=SAFE,'unsafe display integer')
    return value


def quantize(value):
    require(math.isfinite(value),'nonfinite display value')
    scaled=value*100
    require(math.isfinite(scaled),'display scale overflow')
    q=safe_int(round(scaled))
    require(abs(q/100-value)<=.005+max(1e-10,1e-12*abs(value)),'display quantization tolerance')
    return q


def account_curve(record):
    r,p=record.result,record.prepared
    start,end=r.window.startMs,r.window.endMs
    path=r.path.to_pydict();opens=np.asarray(path['barOpenMs'],dtype=np.int64)
    closes=opens+p.stepMs
    result=dict(curveVersion=VERSION,valueScale=100,timeMs=[start],equityQ=[0],balanceQ=[0],
                cashGaps=[],status='noObservedBars',endpoint='unavailable')
    if not len(opens):return result
    initial=r.policy.initialCapital
    eq=[quantize(100*(v/initial-1)) for v in path['equity']]
    balance=[quantize(100*(v/initial-1)) for v in path['balance']]
    points={start:(0,0)}
    for boundary in range(start+3*DAY,end+1,3*DAY):
        index=int(np.searchsorted(closes,boundary,side='right'))-1
        if index>=0:points[int(closes[index])]=(eq[index],balance[index])
    points[int(closes[-1])]=(eq[-1],balance[-1])
    gaps=[]
    def flat(index):
        require(path['side'][index]==0 and path['quantity'][index]==0 and
                path['balance'][index]==path['equity'][index],'gap boundary is not flat cash')
    if opens[0]>start:gaps.append(dict(startMs=start,endMs=int(opens[0]),cashQ=0))
    for i,close in enumerate(closes):
        following=int(opens[i+1]) if i+1<len(opens) else end
        if following>close:
            flat(i)
            gaps.append(dict(startMs=int(close),endMs=following,cashQ=balance[i]))
    flat(-1)
    require(path['balance'][-1]==r.finalBalance,'terminal balance differs')
    for gap in gaps:
        for time in (gap['startMs'],gap['endMs']):
            point=(gap['cashQ'],gap['cashQ'])
            require(time not in points or points[time]==point,'inconsistent gap support point')
            points[time]=point
    if closes[-1]<end:points[end]=(balance[-1],balance[-1])
    times=sorted(points)
    for t in times:safe_int(t)
    result.update(timeMs=times,equityQ=[points[t][0] for t in times],balanceQ=[points[t][1] for t in times],
        cashGaps=gaps,status='available',endpoint='flatCashExtension' if closes[-1]<end else 'observedClose')
    return result


def price_curve(times, values, start, end, fingerprint):
    """Input is the real base-5m slice, including causal warmup if present."""
    mask=(times>=start)&(times<end);times=times[mask];values=values[mask]
    result=dict(curveVersion=VERSION,valueScale=100,status='noObservedBars',timeMs=[],priceQ=[],
        anchorPrice=None,anchorTimeMs=None,lastObservedTimeMs=None,coverageGaps=[],sourceFingerprint=fingerprint)
    if not len(times):return result
    closes=times+300000;anchor=float(values[0,0]);first=int(times[0]);last=int(closes[-1])
    require(anchor>0 and math.isfinite(anchor),'invalid price anchor')
    points={first:0}
    for boundary in range((start//DAY+1)*DAY,end+1,DAY):
        if boundary<=first:continue
        i=int(np.searchsorted(closes,boundary))
        points[boundary]=quantize(100*(float(values[i,3])/anchor-1)) if i<len(closes) and closes[i]==boundary else None
    points[last]=quantize(100*(float(values[-1,3])/anchor-1))
    gaps=[]
    if first>start:gaps.append(dict(startMs=start,endMs=first))
    for i,t in enumerate(closes):
        following=int(times[i+1]) if i+1<len(times) else end
        if following>t:gaps.append(dict(startMs=int(t),endMs=following))
    ordered=sorted(points)
    for t in ordered:safe_int(t)
    result.update(status='available',timeMs=ordered,priceQ=[points[t] for t in ordered],
        anchorPrice=anchor,anchorTimeMs=first,lastObservedTimeMs=last,coverageGaps=gaps)
    return result
