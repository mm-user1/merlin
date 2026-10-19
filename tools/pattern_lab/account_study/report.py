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
#monthMatrix td{white-space:normal;min-width:300px;max-width:480px;vertical-align:top}#monthMatrix td:first-child{min-width:70px}
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
<section><h2>Descriptive robustness views</h2><p>No result is certified robust. These views describe the declared research choices, not a search for a winning configuration.</p>
<label>Global overview allocation <select id="overviewAllocation"></select></label>
<h3>1. All declared windows</h3><p>The local Window selector above does not restrict this overview. Overlap is time overlap, not independent replication or identical trades. Unequal durations are shown explicitly.</p>
<details><summary>Declared protocol and development/reserve intervals</summary><pre id="protocol"></pre></details>
<div class="scroll"><table id="windowOverview"></table></div><div class="scroll"><table id="overlaps"></table></div>
<h3>2–3. Monthly strategy performance and passive price context across windows</h3>
<p>Each cell uses its own clipped interval. A shared month is not new evidence; window restarts can differ. Monthly returns are not summed. Passive long prices reset each month, require exact endpoints for every ticker, and exclude fees, funding, liquidation and sizing. They are not an execution-matched benchmark or alpha.</p>
<div class="scroll"><table id="monthMatrix"></table></div>
<h3>4. Dependence on positive tickers and months</h3><p>Local window and allocation controls apply below. Ticker removal is an exact subset of already-run independent accounts chosen with hindsight, not forward universe selection. Empty rosters have no return.</p>
<div class="scroll"><table id="tickerRemoval"></table></div><div class="scroll"><table id="erasure"></table></div>
<p>Accounting removal; no account re-execution. Both monthly diagnostics retain original sizing, entries, fees and subsequent PnL. Removing all months is not an investable path. Dependence on a few winners is not an automatic failure.</p>
<div class="scroll"><table id="monthRemoval"></table></div>
<h3>5. Attribution within the executed account</h3><p>Both sides share balance and occupancy. A side is not a separately executed long-only or short-only strategy. Holding times use the existing intrabar-as-open approximation; no standalone side drawdown is implied.</p>
<div class="scroll"><table id="sideAttribution"></table></div>
<h3>6. Explicit allocation pair in the local window</h3>
<label>Baseline <select id="baseline"></select></label><label>Candidate <select id="candidate"></select></label>
<p>Candidate minus baseline, with equal capital and the entire roster. Different settings are not automatically a controlled filter-effect test. Breadth is descriptive, not a sample size for inference; monthly differences are not summed.</p>
<p id="pairStatus"></p><div class="scroll"><table id="pairTotals"></table></div><div class="scroll"><table id="pairTickers"></table></div><div class="scroll"><table id="pairMonths"></table></div>
<details><summary>Explicit pair settings</summary><pre id="pairSettings"></pre></details></section>
<details><summary>Policy, coverage, passive references, source provenance and limitations</summary><pre id="disclosures"></pre></details>
</main><script id="account-data" type="application/json">'''+payload+'''</script><script>'''+script+'''</script></html>\n'''
