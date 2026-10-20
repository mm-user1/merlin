'use strict';
(() => {
  const d=JSON.parse(document.getElementById('selection-data').textContent), carried=d.request.capitalMode==='carried';
  const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
  const fmt=v=>v===null||v===undefined?'Unavailable':typeof v==='number'?v.toLocaleString('en-US',{maximumFractionDigits:6}):String(v);
  const table=(headers,rows)=>{const div=el('div');div.className='scroll';const t=el('table'),head=el('tr');headers.forEach(h=>head.append(el('th',h)));t.append(head);
    rows.forEach(row=>{const tr=el('tr');row.forEach(v=>tr.append(el('td',fmt(v))));t.append(tr);});div.append(t);return div;};
  const overview=document.getElementById('overview');overview.append(el('h2',carried?'Carried monthly capital':'Fixed reference capital'));
  overview.append(el('p',`Shadow capital C: ${fmt(d.request.referenceCapital)}; initial portfolio T0: ${carried?fmt(d.request.initialCapital):'not applicable'}. R: ${d.request.randomControl.replicates}; seed: ${d.request.randomControl.seed}.`));
  overview.append(el('p',`Verification: ${d.verification.level}; scorer logic: ${d.verification.experimentalLogic}. ${d.verification.limitation}`));
  const sel=document.getElementById('selector'),ks=document.getElementById('k');
  d.request.selectors.forEach(s=>{const o=el('option',s.selectorId);o.value=s.selectorId;sel.append(o);});
  d.request.kValues.forEach(k=>{const o=el('option',k);o.value=k;ks.append(o);});
  function chart(c){
    const box=el('section');box.append(el('h2',`Top-K (blue) and all-eligible (orange): ${carried?'return %':'cumulative return pp'}`));
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 1000 260');svg.setAttribute('role','img');svg.setAttribute('aria-label','Sampled portfolio returns; financial metrics use full paths');
    const points=Object.values(c.displayPaths).flatMap(a=>a.flatMap(s=>s.points));
    if(points.length){const xs=points.map(p=>p[0]),ys=points.map(p=>p[1]),lo=Math.min(...ys,0),hi=Math.max(...ys,0),xmin=Math.min(...xs),xmax=Math.max(...xs);
      const x=v=>60+910*(v-xmin)/(xmax-xmin||1),y=v=>220-190*(v-lo)/(hi-lo||1);
      [lo,hi].forEach(v=>{const text=document.createElementNS(ns,'text');text.setAttribute('x','0');text.setAttribute('y',y(v));text.textContent=fmt(v);svg.append(text);});
      Object.entries(c.displayPaths).forEach(([kind,segments])=>segments.forEach(s=>{const p=document.createElementNS(ns,'polyline');p.setAttribute('fill','none');p.setAttribute('stroke',kind==='selected'?'#2563eb':'#c66a08');p.setAttribute('stroke-width','1.7');p.setAttribute('points',s.points.map(v=>`${x(v[0])},${y(v[1])}`).join(' '));svg.append(p);}));
    } else box.append(el('p','No available capital path.'));box.append(svg);return box;
  }
  function draw(){
    const c=d.comparisons.find(v=>v.selectorId===sel.value&&v.k===Number(ks.value)),root=document.getElementById('results');root.replaceChildren();
    const s=el('section');s.append(el('h2',`Full period: ${c.fullPeriodAvailable?'available':'unavailable'}`));
    const unit=carried?'%':'pp';s.append(table(['Portfolio',`Return ${unit}`,`Max drawdown ${unit}`,'Net PnL'],['selected','allEligible'].map(n=>[n,c.fullPeriod[n][carried?'totalReturnPct':'cumulativeReturnPp'],c.fullPeriod[n][carried?'maxDrawdownPct':'maxDrawdownPp'],c.fullPeriod[n].netPnl])));
    const prefix=c[carried?'commonPrefix':'matchedAvailableMonths'];s.append(el('p',`${carried?'Common contiguous prefix':'Matched available months'}: ${prefix.usedMonths}/${prefix.requestedMonths} (${prefix.months.join(', ')}). Underfilled months: ${c.underfilledMonths}; empty pools: ${c.emptyPoolMonths}.`));
    if(carried)s.append(table(['Prefix portfolio','Return %','Max drawdown %'],['selected','allEligible'].map(n=>[n,prefix.metrics[n].totalReturnPct,prefix.metrics[n].maxDrawdownPct])));
    s.append(table(['Random distribution','P10','Median','P90','Below / equal / above Top-K'],[['Full',c.randomFullDistribution],['Prefix / matched',prefix.distribution]].map(([n,v])=>[n,v.p10,v.median,v.p90,`${fmt(v.below)} / ${fmt(v.equal)} / ${fmt(v.above)}`])));root.append(s,chart(c));
    const months=el('section');months.append(el('h2','Monthly portfolio facts'));
    months.append(table(['Month / portfolio','State','Pool / members','Slots funded / planned / idle','Opening / closing (USDT)','Funded capital / idle cash (USDT)','Slot capital (USDT) / utilization %','PnL (USDT) / return %','Fees (USDT) / trades','Rejections / forced exits'],c.months.flatMap(m=>['selected','allEligible'].map(n=>{
      const p=m[n],capital=p.displayCapital;
      return [m.month+' / '+n,p.status||(p.available?'available':'unavailable'),m.eligibleCount+' / '+p.members.join(', '),
        `${p.fundedSlots} / ${p.plannedSlots} / ${p.idleSlots}`,`${fmt(capital.opening)} / ${fmt(capital.closing)}`,
        `${fmt(p.fundedCapital)} / ${fmt(capital.idleCash)}`,`${fmt(capital.slot)} / ${fmt(p.utilizationPct)}`,
        `${fmt(p.netPnl)} / ${fmt(p.returnPct)}`,`${fmt(p.fees)} / ${fmt(p.trades)}`,JSON.stringify({sizing:p.sizingRejections,forced:p.forcedExits})];
    }))));
    const detail=el('details');detail.append(el('summary','Schedules, ties, unavailable reasons and exclusions'),el('pre',JSON.stringify(c.months,null,2)));months.append(detail);root.append(months);
    document.getElementById('diagnostics').textContent=JSON.stringify({scores:d.scores.filter(s=>s.selectorId===sel.value),coverage:d.coverage},null,2);
  }
  document.getElementById('attribution').textContent=JSON.stringify({request:d.request,allocation:d.allocation,identities:d.identities,implementation:d.implementation,capacity:d.capacity,verification:d.verification},null,2);
  sel.addEventListener('change',draw);ks.addEventListener('change',draw);draw();
})();
