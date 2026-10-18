"""Deterministic self-contained light-theme account report."""
from pathlib import Path
import html
import json

from ..study.contracts import semantic_digest
from .common import json_text


def allocation_labels(request):
    groups, labels = {}, {}
    for allocation in request['allocations']:
        doc = json.loads(json_text(allocation))
        identifier = doc.pop('allocationId')
        if doc['policy']['mode'] != 'fixedBracket':
            labels[identifier] = identifier
            continue
        rr = doc['policy'].pop('rewardRisk')
        groups.setdefault(semantic_digest(doc), []).append((identifier, rr))
    for members in groups.values():
        for identifier, rr in members:
            labels[identifier] = ('RR '+str(rr)+' · '+identifier) if len(members) > 1 else identifier
    return labels


def render(request, summary, identities):
    model = dict(request=request, summary=summary, identities=identities, allocationLabels=allocation_labels(request))
    payload = json_text(model).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    script = (Path(__file__).with_name('report.js')).read_text(encoding='utf-8')
    title = html.escape(request['studyId'])
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Account study · '''+title+'''</title><style>
body{font:15px system-ui,sans-serif;color:#172738;background:#f5f7fa;margin:0;padding:24px;line-height:1.5}
main{max-width:1450px;margin:auto}h1{margin-bottom:4px}section,details{background:white;border:1px solid #dbe2e9;border-radius:8px;padding:16px;margin:16px 0}
.scroll{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:right;white-space:nowrap;padding:8px;border-bottom:1px solid #e2e8ee}
th:first-child,td:first-child{text-align:left}th button{font:inherit;font-weight:600;border:0;background:none;color:#135b91;cursor:pointer}
tr.selected{background:#e2f1ff}tbody tr{cursor:pointer}button,select{padding:6px}label{display:inline-flex;gap:8px;align-items:center;margin:8px}
svg{width:100%;height:230px;background:#fafcfe}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px}.note{color:#526577}
@media(max-width:650px){body{padding:10px}section{padding:10px}h1{font-size:24px}}
</style><main><h1>'''+title+'''</h1><p>Completed descriptive account study. This is not a validated trading edge or a shared-margin portfolio.</p>
<label>Window <select id="window"></select></label><label>Compare allocations <select id="allocations" multiple></select></label>
<section><h2>Allocation comparison</h2><div class="scroll"><table id="comparison"></table></div></section>
<section><h2>Aggregate cumulative return (%)</h2><svg id="aggregateCurve" role="img" aria-label="Aggregate percentage returns"></svg><p id="aggregateLegend"></p></section>
<section><h2>Tickers</h2><p class="note">Click a numeric header: descending first, then toggle. Null values stay last. Arrow navigation follows the displayed order.</p>
<label>Ticker <select id="ticker"></select></label><button id="up">Previous</button> <button id="down">Next</button>
<div class="scroll" id="tickerRegion" tabindex="0"><table id="tickers"></table></div>
<h3 id="tickerTitle">Ticker cumulative return (%)</h3><svg id="tickerCurve" role="img" aria-label="Ticker percentage returns"></svg></section>
<section><h2>Monthly continuous-run contributions</h2><p class="note">A close at midnight belongs to the preceding month. Curves use monthly endpoints; maximum drawdown scans every retained bar. Monthly returns are not added.</p>
<div class="scroll"><table id="months"></table></div></section>
<details><summary>Policy, coverage, passive references, source provenance and limitations</summary><pre id="disclosures"></pre></details>
</main><script id="account-data" type="application/json">'''+payload+'''</script><script>'''+script+'''</script></html>\n'''
