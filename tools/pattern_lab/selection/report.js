/* Python owns every financial value. This module only chooses and displays. */
"use strict";
const SelectionReport = (() => {
  const escape = value => String(value).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const number = value => value === null || value === undefined ? "Unavailable" : Number(value).toLocaleString("en-US", {maximumFractionDigits:6});
  const percentage = value => value === null || value === undefined ? "Unavailable" : number(value) + "%";
  const equivalent = (a,b) => a !== null && b !== null && Math.abs(a-b) <= Math.max(1e-9*Math.max(Math.abs(a),Math.abs(b)),1e-8);
  const reasonText = value => ({noObservedBars:'No observed bars',insufficientCoverage:'Insufficient observed coverage',missingMonth:'Missing history month'}[value] || value);
  const choose = (data,selector,k) => data.comparisons.find(c => c.selectorId === selector && c.k === Number(k));
  const table = (headers,rows) => '<div class="scroll"><table><thead><tr>'+headers.map(h=>'<th>'+escape(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(row=>'<tr>'+row.map(cell=>'<td>'+escape(cell)+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';
  function chart(c) {
    const series = [c.displayPaths.selected,c.displayPaths.allEligible];
    const points = series.flatMap(s=>s.flatMap(segment=>segment.points));
    if (!points.length) return '<p>No available path observations.</p>';
    const xs=points.map(p=>p[0]),ys=points.map(p=>p[1]);
    const left=Math.min(...xs),right=Math.max(...xs),low=Math.min(0,...ys),high=Math.max(0,...ys);
    const x=t=>35+730*(t-left)/(right-left||1),y=v=>220-190*(v-low)/(high-low||1);
    let body='<line x1="35" x2="765" y1="'+y(0)+'" y2="'+y(0)+'" stroke="#b6c0cd"/>';
    series.forEach((segments,index)=>segments.forEach(s=>{
      body+='<polyline fill="none" stroke="'+(index?'#aa6100':'#2563b5')+'" stroke-width="2" points="'+s.points.map(p=>x(p[0])+','+y(p[1])).join(' ')+'"/>';
    }));
    return '<svg viewBox="0 0 800 250" role="img" aria-label="Selected and all-eligible return paths">'+body+'</svg><p class="muted">Blue: selected; amber: all-eligible. '+(c.fullPeriodAvailable?'Cumulative return, percentage points.':'Separate available-month fragments; no full-period cumulative curve.')+'</p>';
  }
  function render(data,c) {
    if (!c) return '<p>No matching selector/K.</p>';
    const partial=c.matchedAvailableMonths, distribution=c.fullPeriodAvailable?c.randomFullDistribution:partial.distribution;
    let out='<section><h2>Fixed-reference performance</h2>'+table(['Series','Cumulative return (pp)','Max drawdown (pp)','Net PnL (USDT)'],['selected','allEligible'].map(name=>[name,number(c.fullPeriod[name].cumulativeReturnPp),number(c.fullPeriod[name].maxDrawdownPp),number(c.fullPeriod[name].netPnl)]))+chart(c)+'</section>';
    out+='<section><h2>'+(c.fullPeriodAvailable?'Random-K full-period distribution':'Matched available-month subtotal — partial period')+'</h2>';
    out+='<p>Used '+partial.usedMonths+' of '+partial.requestedMonths+' requested months: '+escape(partial.months.join(', ')||'none')+'.</p>';
    if (!c.fullPeriodAvailable) out+=table(['Selected subtotal (pp)','All-eligible subtotal (pp)'],[[number(partial.selectedReturnPp),number(partial.allEligibleReturnPp)]])+'<p>Full-period total and drawdown remain unavailable. No concatenated-fragment drawdown is reported.</p>';
    out+=table(['Random p10 (pp)','Median (pp)','p90 (pp)','Below selected','Equal','Above selected'],[[number(distribution.p10),number(distribution.median),number(distribution.p90),number(distribution.below),number(distribution.equal),number(distribution.above)]])+'<p>Underfilled months: '+c.underfilledMonths+'; empty-pool cash months: '+c.emptyPoolMonths+'.</p></section>';
    out+='<section><h2>Monthly capital, coverage and membership</h2>'+table(['Month','Final pool','Selected / idle slots','Selected capital','All-eligible capital','Selected return','All-eligible return','Fees / trades','Availability'],c.months.map(m=>[m.month,m.eligiblePool.join(', ')||'Empty pool',m.selected.members.join(', ')+' / '+m.selected.idleSlots,number(m.selected.plannedCapital),number(m.allEligible.plannedCapital),percentage(m.selected.returnPct),percentage(m.allEligible.returnPct),number(m.selected.fees)+' / '+number(m.selected.trades),m.available?'Available':m.unavailableReasons.map(r=>r.instrumentId+': '+reasonText(r.reason)).join('; ')]))+'</section>';
    out+='<section><h2>Ranking, exclusions and boundary ties</h2>';
    for (const m of c.months) {
      const batch=data.scores.find(s=>s.selectorId===c.selectorId&&s.month===m.month);
      out+='<details><summary>'+escape(m.month)+' — '+m.eligibleCount+' eligible</summary>';
      out+=table(['Instrument','Eligible','Ranking key','Reason'],batch.rows.map(r=>[r.instrumentId,r.eligible?'Yes':'No',r.key===null?'Not ranked':r.key.map(number).join(' / '),reasonText(r.reason)||'Available']));
      out+='<p>History: '+escape(batch.historyMonths.join(', '))+'. Common exclusions: '+escape(batch.commonExclusions.map(r=>r.instrumentId+': '+r.reasons.map(v=>v.month+' '+reasonText(v.reason)).join(', ')).join('; ')||'none')+'.</p>';
      out+='<p>Boundary tie: '+escape(m.boundaryTie?m.boundaryTie.totalMembers+' members for '+m.boundaryTie.slots+' slots; chosen '+m.boundaryTie.chosen.join(', '):'No split boundary tie')+'. Tie-neutral expected return: '+percentage(m.tieNeutralExpectedReturnPct)+'. Descriptive expectation only.</p></details>';
    }
    out+='</section><section><h2>Native account diagnostics</h2>'+table(['Month / instrument','Observed / expected bars','Missing head / interior / tail','Sizing rejections','Forced terminal / gap exits','Fees / trades'],data.coverage.map(r=>[r.month+' / '+r.instrumentId,r.observedBars+' / '+r.expectedBars,r.missingHeadSlots+' / '+number(r.missingInteriorSlots)+' / '+r.missingTailSlots,r.sizingRejectionCount,r.forcedExits.terminal.count+' / '+r.forcedExits.gapBoundary.count,number(r.fees)+' / '+r.trades]))+'</section>';
    return out;
  }
  function mount(data,doc) {
    const selector=doc.getElementById('selector'),k=doc.getElementById('k');
    selector.innerHTML=data.request.selectors.map(s=>'<option value="'+escape(s.selectorId)+'">'+escape(s.selectorId)+' — '+escape(s.scorerId)+'</option>').join('');
    k.innerHTML=data.request.kValues.map(v=>'<option value="'+v+'">'+v+'</option>').join('');
    const parameters=data.request.selectors.map(s=>s.selectorId+': '+(Object.entries(s.parameters).map(([key,value])=>key+' = '+String(value)).join(', ')||'no parameters')).join('; ');
    const allocation=data.allocation,policy=allocation.policy;
    const trading=allocation.timeframeMinutes+' minute bars; '+policy.mode+'; '+policy.direction+'; risk '+policy.riskPct+'%; reward/risk '+number(policy.rewardRisk)+'; commission '+policy.commissionPctPerSide+'% per side';
    doc.getElementById('overview').innerHTML='<section><dl><dt>Allocation</dt><dd>'+escape(data.request.allocationId)+'</dd><dt>Trading parameters</dt><dd>'+escape(trading)+'</dd><dt>Capital per slot (USDT)</dt><dd>'+number(data.request.referenceCapital)+'</dd><dt>History / coverage rule</dt><dd>'+data.request.pool.historyMonths+' months / '+percentage(data.request.pool.minObservedBarsPct)+'</dd><dt>Evaluation</dt><dd>'+escape(data.request.evaluationMonths.join(', '))+'</dd><dt>Scorer parameters</dt><dd>'+escape(parameters)+'</dd><dt>Random schedules</dt><dd>'+data.request.randomControl.replicates+'; seed '+data.request.randomControl.seed+'</dd></dl></section>';
    const sources=Object.entries(data.implementation.sources).concat(Object.entries(data.implementation.experimental).flatMap(([id,files])=>Object.entries(files).map(([name,hash])=>[id+'/'+name,hash])));
    doc.getElementById('attribution').innerHTML='<p>Verification: retained evidence; experimental logic: stored outputs only. Deep verification requires the exact parent and matching optional scorer sources.</p>'+table(['Selection identity','SHA-256'],Object.entries(data.identities))+table(['Parent identity / completion','SHA-256'],Object.entries(data.request.parent))+table(['Source','SHA-256'],sources);
    function update(){doc.getElementById('results').innerHTML=render(data,choose(data,selector.value,k.value));}
    selector.addEventListener('change',update);k.addEventListener('change',update);update();
  }
  return {escape,number,percentage,equivalent,choose,render,mount};
})();
if (typeof module !== 'undefined') module.exports=SelectionReport;
if (typeof document !== 'undefined') SelectionReport.mount(JSON.parse(document.getElementById('selection-data').textContent),document);
