"""Compact display sampling; financial calculations stay on full paths."""
from pathlib import Path
import html
import json

import numpy as np

from .request import month_bounds


def display_paths(comparison, curves, name):
    """Monthly endpoints/earliest extrema are mandatory; the rest are bucketed."""
    segments=[];previous=[]
    for month in comparison['months']:
        if not month['available']:
            previous=[];continue
        import math
        offset=math.fsum(previous) if comparison['fullPeriodAvailable'] else 0.
        source=curves[month['curves'][name]]
        times=np.r_[month_bounds(month['month'])[0],source['times']]
        values=np.r_[offset,offset+source['values']]
        mandatory={0,len(values)-1,int(np.argmin(values)),int(np.argmax(values))}
        segments.append(dict(month=month['month'],times=times,values=values,mandatory=mandatory))
        previous.append(month[name]['returnPct'])
    cap=max(2000,4*len(comparison['months'])+1)
    mandatory_count=sum(len(s['mandatory']) for s in segments)
    remaining=cap-mandatory_count
    candidates=[(j,n) for j,s in enumerate(segments) for n in range(len(s['values'])) if n not in s['mandatory']]
    if len(candidates)>remaining:
        # One deterministic midpoint per equal-index bucket; extrema already fixed.
        candidates=[candidates[min(len(candidates)-1,int((i+.5)*len(candidates)/remaining))] for i in range(remaining)] if remaining else []
    for j,n in candidates:segments[j]['mandatory'].add(n)
    return [dict(month=s['month'],points=[[int(s['times'][n]),float(s['values'][n])] for n in sorted(s['mandatory'])]) for s in segments]


def summary(reader):
    meta=reader.selection;comparisons=reader.results['comparisons']
    for c in comparisons:
        c['displayPaths']={name:display_paths(c,reader._curves,name) for name in ('selected','allEligible')}
        for m in c['months']:m.pop('random')
        c.pop('randomFullReturnPp')
        c['matchedAvailableMonths'].pop('randomReturnPp')
    return dict(schema='pattern-lab.selection-summary.v1',request=meta['request'],allocation=meta['allocation'],
        identities=meta['identities'],implementation=meta['implementation'],roster=meta['roster'],
        verification=dict(level='retainedEvidence',experimentalLogic='storedOutputsOnly'),
        comparisons=comparisons,scores=reader.scores,
        coverage=[{k:r[k] for k in ('instrumentId','month','available','observedBars','expectedBars','missingHeadSlots',
            'missingTailSlots','missingInteriorSlots','sizingRejectionCount','forcedExits','trades','fees')} for r in reader.ledger])


def render(value):
    payload=json.dumps(value,ensure_ascii=True,separators=(',',':'),allow_nan=False).replace('<','\\u003c').replace('>','\\u003e').replace('&','\\u0026')
    title=html.escape(value['request']['selectionId'])
    script=Path(__file__).with_name('report.js').read_text(encoding='utf-8')
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monthly universe selection — '''+title+'''</title>
<style>body{font:15px system-ui,sans-serif;background:#f5f7fa;color:#1d2939;margin:0}main{max-width:1150px;margin:auto;padding:24px}section,.card{background:white;border:1px solid #d5dce5;border-radius:8px;padding:18px;margin:16px 0}h1{font-size:26px}h2{font-size:19px}p{line-height:1.5}.muted{color:#566577}.controls{display:flex;gap:24px;flex-wrap:wrap}select{padding:8px;margin-left:8px}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:8px;border-bottom:1px solid #e1e6ed;vertical-align:top}.scroll{overflow:auto}svg{width:100%;height:250px}dl{display:grid;grid-template-columns:max-content 1fr;gap:8px}dd{margin:0;overflow-wrap:anywhere}code{overflow-wrap:anywhere}details{margin:14px 0}@media(max-width:600px){main{padding:10px}dl{display:block}dd{margin:4px 0 12px}section{padding:12px}}</style>
<main><h1>Monthly universe selection: '''+title+'''</h1>
<p>Descriptive fixed-reference experiment. Every funded slot starts each month at the same capital;
profits are withdrawn and losses replenished. This is not a self-financing compounded portfolio.</p>
<div id="overview"></div><section class="controls"><label>Selector<select id="selector"></select></label><label>Slots (K)<select id="k"></select></label></section>
<div id="results"></div><section><h2>Interpretation and boundaries</h2>
<p>One fixed parent roster can introduce survivor and coverage bias. Eligibility uses only completed history;
evaluation coverage never changes membership or denominators. Head, tail and interior gaps obey the same declared coverage rule.
Missing observations are unavailable, not idle cash. Empty eligible pools are explicit cash months.</p>
<p>All-eligible funds a different total capital and is a per-slot reference. Underfill changes utilization;
differences are not necessarily ranking lift. Different scorer pools are different universes.
Random-K schedules, K variants and months are not independent statistical tests; distributions are not p-values.</p>
<p>Every monthly account starts flat. Positions close through native terminal logic, including its fee;
pending orders are not carried between months. Full paths determine max drawdown in percentage points.
Display sampling preserves monthly endpoints and extrema and does not determine maxDrawdownPp.
Unavailable months are never bridged; incomplete-period plots show separate monthly fragments.</p>
<p>historicalConsistency4m counts positive months, then compares worst and mean return. It is not IS/OOS transfer consistency.
No automatic best-method verdict, profitability guarantee, Funding cashflow, slippage or carried-capital simulation is supplied.</p></section>
<details><summary>Input and source attribution</summary><div id="attribution"></div></details></main>
<script id="selection-data" type="application/json">'''+payload+'''</script><script>'''+script+'''</script></html>'''
