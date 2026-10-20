"""Standalone v2 presentation, with financial facts computed solely in Python."""
from pathlib import Path
import html
import json
import numpy as np
from .request import month_bounds
from .carried import portfolio_id
from .report import display_paths
from .dependencies_v2 import description


def carried_display(c,curves,kind,initial):
    segments=[]
    for m in c['months']:
        key=portfolio_id(c['selectorId'],c['k'],kind,-1),m['month']
        if key not in curves:continue
        p=curves[key];times=np.r_[month_bounds(m['month'])[0],p['times']]
        equity=np.r_[p['opening'],p['equity']].copy();equity[-1]=p['closing']
        values=100*(equity-initial)/initial
        keep={0,len(values)-1,int(np.argmin(values)),int(np.argmax(values))}
        segments.append(dict(month=m['month'],times=times,values=values,keep=keep))
    cap=max(2000,4*len(c['months'])+1);remaining=cap-sum(len(s['keep']) for s in segments)
    candidates=[(j,n) for j,s in enumerate(segments) for n in range(len(s['values'])) if n not in s['keep']]
    if len(candidates)>remaining:
        candidates=[candidates[min(len(candidates)-1,int((i+.5)*len(candidates)/remaining))] for i in range(remaining)] if remaining else []
    for j,n in candidates:segments[j]['keep'].add(n)
    return [dict(month=s['month'],points=[[int(s['times'][n]),float(s['values'][n])] for n in sorted(s['keep'])]) for s in segments]


def summary(reader):
    meta=reader.selection;doc=meta['request'];comparisons=reader.results['comparisons'];carried=doc['capitalMode']=='carried'
    for c in comparisons:
        for m in c['months']:
            for kind in ('selected','allEligible'):
                p=m[kind]
                p['displayCapital']=dict(opening=p['openingCapital'],closing=p['closingCapital'],slot=p['slotCapital'],idleCash=p['cash']) if carried else dict(
                    opening=p['plannedCapital'],closing=p['plannedCapital']+p['netPnl'] if p['available'] else None,
                    slot=doc['referenceCapital'],idleCash=p['idleSlots']*doc['referenceCapital'])
        c['displayPaths']={kind:carried_display(c,reader._curves,kind,doc['initialCapital']) if carried else display_paths(c,reader._curves,kind)
            for kind in ('selected','allEligible')}
        c.pop('randomFullReturnPct' if carried else 'randomFullReturnPp')
        c['commonPrefix' if carried else 'matchedAvailableMonths'].pop('randomReturnPct' if carried else 'randomReturnPp')
    return dict(schema='pattern-lab.selection-summary.v2',request=doc,allocation=meta['allocation'],identities=meta['identities'],
        implementation=meta['implementation'],roster=meta['roster'],capacity=meta['capacity'],
        verification=dict(level='retainedEvidence',experimentalLogic='storedOutputsOnly',
            limitation='Stored execution arithmetic and identities are checked; individual execution truth requires exact-parent replay.'),
        comparisons=comparisons,scores=reader.scores,coverage=[{k:r[k] for k in ('instrumentId','month','available','observedBars','expectedBars',
            'missingHeadSlots','missingTailSlots','missingInteriorSlots','sizingRejectionCount','forcedExits','trades','fees')} for r in reader.ledger])


def render(value):
    dependencies=description(value)
    explanation=''.join('<p>'+html.escape(dependencies[k])+'</p>' for k in ('supply','verification'))
    explanation+='<p>Ordinary load, import and report rebuild need only the sealed selection bundle.</p>'
    explanation+='<p>Exact funded execution verification needs this exact pinned monthly parent:</p><pre>'+html.escape(json.dumps(dependencies['exactParent']['pins'],indent=2))+'</pre>'
    explanation+='<p>Auxiliary scorer recomputation additionally needs these exact snapshot content IDs and explicitly supplied matching experimental scorer source hashes, where used:</p><pre>'+html.escape(json.dumps(dependencies['auxiliaryRecomputation'],indent=2))+'</pre>'
    payload=json.dumps(value,ensure_ascii=True,separators=(',',':'),allow_nan=False).replace('<','\\u003c').replace('>','\\u003e').replace('&','\\u0026')
    title=html.escape(value['request']['selectionId'])
    script=Path(__file__).with_name('report_v2.js').read_text(encoding='utf-8')
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monthly selection: '''+title+'''</title><style>
body{font:15px system-ui;background:#f4f6f8;color:#182334;margin:0}main{max-width:1250px;margin:auto;padding:24px}
section,details{background:white;padding:18px;margin:16px 0;border:1px solid #d0d8e0;border-radius:8px}table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:8px;text-align:left;vertical-align:top;border-bottom:1px solid #ddd}.scroll{overflow:auto}select{padding:8px;margin:8px}svg{width:100%;height:260px}pre{white-space:pre-wrap;overflow-wrap:anywhere}p{line-height:1.5}.muted{color:#536174}</style>
<main><h1>Monthly universe selection: '''+title+'''</h1><section id="overview"></section>
<section><label>Selector <select id="selector"></select></label><label>K <select id="k"></select></label></section><div id="results"></div>
<section><h2>Interpretation</h2><p>Ranking uses fixed-capital shadow history only. Every monthly account starts flat and closes through native terminal logic including fees; positions and pending orders do not carry between months.</p>
<p>Carried portfolios start with equal total capital T0, rebalance each month, and keep underfilled slots in cash. Sizing uses the native executor at actual slot capital. Even one unselected member of the eligible pool with insufficient evaluation-month data stops all later carried comparisons for that selector, across every K and all controls. Later schedules remain visible, with priorCapitalUnknown. A depleted balance stays signed and stops trading. Full-period metrics are unavailable for an incomplete chain; the common prefix is labelled separately.</p>
<p>Fixed-reference slots reset to C monthly; profits are withdrawn and losses replenished. Cumulative returns and drawdown are percentage points (pp), not compounded percentages. Its all-eligible control can fund a different total capital.</p>
<p>Carried drawdown uses native equity and authoritative capital boundaries, includes T0 in the running peak, and may exceed 100%. Display sampling preserves monthly boundaries and extrema; plotted endpoints use authoritative closing capital. The sampled plot does not calculate drawdown. Random controls supply return distributions only, without equity curves or drawdown.</p>
<p>Underfill and different scorer pools confound ranking lift. A fixed roster can introduce survivor and coverage bias. Random schedules, K variants and months are dependent; distributions are not p-values. Historical consistency is not IS/OOS transfer. No automatic best-method verdict or profitability claim is supplied.</p>
<p>Funding is a rate per settlement; no eight-hour normalization or Funding PnL is applied. OI units and base multipliers remain explicit; raw differently scaled contracts are not comparable. Auxiliary values obey declared publication lags, coverage and staleness; unknown required inputs exclude the ticker.</p></section>
<section id="dependencies"><h2>Verification dependencies</h2>'''+explanation+'''</section>
<details><summary>Scores, auxiliary diagnostics and coverage</summary><pre id="diagnostics"></pre></details>
<details><summary>Identity, source attribution and verification</summary><pre id="attribution"></pre></details></main>
<script id="selection-data" type="application/json">'''+payload+'''</script><script>'''+script+'''</script></html>'''
