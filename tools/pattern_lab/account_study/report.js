/* Pure interaction state is also exercised by the standalone Node harness. */
"use strict";
function orderTickers(rows, key, descending) {
  return rows.slice().sort((a, b) => {
    const x = a[key], y = b[key];
    if (x == null || y == null) {
      if (x == null && y != null) return 1;
      if (y == null && x != null) return -1;
    } else if (x !== y) return (x < y ? -1 : 1) * (descending ? -1 : 1);
    return a.instrumentId < b.instrumentId ? -1 : a.instrumentId > b.instrumentId ? 1 : 0;
  });
}
function sortState(state, key) {
  return {...state, key, descending: state.key === key ? !state.descending : true};
}
function moveTicker(order, selected, delta) {
  if (!order.length) return null;
  return order[Math.max(0, Math.min(order.length - 1, Math.max(0, order.indexOf(selected)) + delta))];
}
function profitableCount(rows, key) {
  const available = rows.filter(r => r[key] != null);
  return `${available.filter(r => r[key] > 0).length}/${available.length}`;
}
function acceptsArrow(target, tickerSelect, tickerRegion) {
  return target === tickerSelect || target === tickerRegion;
}
function closeNumber(a,b) {
  return Number.isFinite(a) && Number.isFinite(b) && Math.abs(a-b) <= Math.max(1e-9*Math.max(Math.abs(a),Math.abs(b)),1e-8);
}
function direction(value) {return !Number.isFinite(value) ? null : closeNumber(value,0) ? "flat" : value > 0 ? "up" : "down";}
function localView(summary, window, ids) {
  const match=a=>a.windowId===window && ids.includes(a.allocationId);
  return {accounts:summary.accounts.filter(match),aggregates:summary.aggregates.filter(match)};
}
function globalView(data, allocation) {
  const windows=data.summary.robustness?.windows || [];
  return windows.map(w=>({...w,aggregate:data.summary.aggregates.find(a=>a.allocationId===allocation && a.windowId===w.windowId)}));
}
function monthlyMatrix(data, allocation) {
  const windows=globalView(data,allocation);
  const labels=[...new Set(windows.flatMap(w=>w.aggregate?.robustness.passiveMonths.map(m=>m.month)||[]))].sort();
  return labels.map(month=>({month,cells:windows.map(w=>({windowId:w.windowId,
    strategy:w.aggregate?.months.find(m=>m.month===month)||null,
    passive:w.aggregate?.robustness.passiveMonths.find(m=>m.month===month)||null,
    status:w.aggregate?.robustness.passiveMonths.some(m=>m.month===month) ? w.aggregate.status : 'outsideWindow'}))}));
}
function pairView(data, window, baseline, candidate) {
  const unavailable=reason=>({status:'incompatible',reason,tickers:[],months:[]});
  if (!baseline || !candidate) return unavailable('Select baseline and candidate explicitly');
  if (baseline===candidate) return unavailable('Choose two different allocations');
  const find=id=>data.summary.aggregates.find(a=>a.windowId===window && a.allocationId===id);
  const b=find(baseline),c=find(candidate);
  if (!b || !c || b.status!=='available' || c.status!=='available') return unavailable('Both complete-roster aggregates must be available');
  if (b.initialCapital/b.plannedAccounts!==c.initialCapital/c.plannedAccounts || b.plannedAccounts!==c.plannedAccounts) return unavailable('Unequal initial capital or roster');
  const classify=(x,y)=>!Number.isFinite(x)||!Number.isFinite(y) ? 'unavailable' : closeNumber(x,y) ? 'tied' : x>y ? 'improved' : 'worse';
  const tickers=data.request.instruments.slice().sort().map(instrumentId=>{
    const findAccount=id=>data.summary.accounts.find(a=>a.windowId===window && a.allocationId===id && a.instrumentId===instrumentId);
    const x=findAccount(baseline),y=findAccount(candidate);
    const bv=x?.coverage.observedBars ? x.returnPct : null,cv=y?.coverage.observedBars ? y.returnPct : null;
    const status=classify(cv,bv);
    return {instrumentId,baselineReturnPct:bv,candidateReturnPct:cv,delta:status==='unavailable'?null:cv-bv,status};
  });
  const labels=[...new Set([...b.months,...c.months].map(m=>m.month))].sort();
  const months=labels.map(month=>{
    const x=b.months.find(m=>m.month===month),y=c.months.find(m=>m.month===month);
    const same=x && y && x.startMs===y.startMs && x.endMs===y.endMs;
    const eligible=!!(same && x.pairEligible && y.pairEligible);
    return {month,startMs:same?x.startMs:null,endMs:same?x.endMs:null,
      baselineReturnPct:x?.returnPct??null,candidateReturnPct:y?.returnPct??null,
      delta:same && Number.isFinite(x.returnPct) && Number.isFinite(y.returnPct)?y.returnPct-x.returnPct:null,
      eligible,status:eligible?classify(y.returnPct,x.returnPct):'excluded',
      reason:eligible?null:!same?'Different or absent interval':'Unavailable return, incomplete accounts or non-exact boundary',
      partialEdgeMonth:!!(x?.partialEdgeMonth||y?.partialEdgeMonth),
      interiorGap:!!(x && x.observedBars<x.expectedBars || y && y.observedBars<y.expectedBars)};
  });
  const counts=rows=>({improved:rows.filter(r=>r.status==='improved').length,tied:rows.filter(r=>r.status==='tied').length,
    worse:rows.filter(r=>r.status==='worse').length,valid:rows.filter(r=>['improved','tied','worse'].includes(r.status)).length,planned:rows.length});
  return {status:'available',deltas:Object.fromEntries(['returnPct','maxDrawdownMtmPct','netPnl','trades','timeInMarketPct'].map(k=>[k,c[k]-b[k]])),
    tickers,months,tickerCounts:counts(tickers),monthCounts:counts(months),
    eligiblePartialEdgeMonths:months.filter(m=>m.eligible&&m.partialEdgeMonth).length};
}
if (typeof module !== "undefined") module.exports = {orderTickers, sortState, moveTicker, profitableCount, acceptsArrow,closeNumber,direction,localView,globalView,monthlyMatrix,pairView};
if (typeof document !== "undefined") {
  const data = JSON.parse(document.getElementById("account-data").textContent);
  const byId = id => document.getElementById(id);
  const selectWindow = byId("window"), selectAllocations = byId("allocations"), selectTicker = byId("ticker");
  let state = {key: "", descending: true}, selected = null, displayed = [];
  const fmt = v => typeof v === "number" ? (Number.isInteger(v) ? String(v) : v.toFixed(3)) : v == null ? "Unavailable" : String(v);
  function element(tag, text, parent) {
    const e = document.createElement(tag); if (text != null) e.textContent = text; if (parent) parent.appendChild(e); return e;
  }
  function option(select, value, text) { const e = element("option", text, select); e.value = value; return e; }
  for (const w of data.request.windows) option(selectWindow, w.windowId, `${w.windowId}: ${w.startUtc} — ${w.endUtc}`);
  for (const a of data.request.allocations) option(selectAllocations, a.allocationId, data.allocationLabels[a.allocationId]).selected = true;
  const overviewAllocation=byId('overviewAllocation'), baseline=byId('baseline'),candidate=byId('candidate');
  option(baseline,'','Select baseline');option(candidate,'','Select candidate');
  for (const a of data.request.allocations) {
    for (const control of [overviewAllocation,baseline,candidate]) option(control,a.allocationId,data.allocationLabels[a.allocationId]);
  }
  let pairSort={key:'',descending:true};
  function active() { return Array.from(selectAllocations.selectedOptions).map(x => x.value); }
  function table(id, headers, rows) {
    const table = byId(id); table.replaceChildren();
    const head = element("tr", null, element("thead", null, table)); headers.forEach(h => element("th", h, head));
    const body = element("tbody", null, table);
    rows.forEach(row => {const tr = element("tr", null, body); row.forEach(v => element("td", fmt(v), tr));});
  }
  function plot(id, series) {
    const svg = byId(id), ns = "http://www.w3.org/2000/svg"; svg.replaceChildren(); svg.setAttribute("viewBox", "0 0 900 230");
    const points = series.flatMap(x => x.curve); if (!points.length) return;
    const xmin = Math.min(...points.map(p => p.timeMs)), xmax = Math.max(...points.map(p => p.timeMs));
    const ymin = Math.min(0, ...points.map(p => p.returnPct)), ymax = Math.max(0, ...points.map(p => p.returnPct));
    const x = v => 55 + 815 * (v-xmin)/(xmax-xmin || 1), y = v => 195 - 165*(v-ymin)/(ymax-ymin || 1);
    const colors = ["#1266ad", "#b34c24", "#248065", "#7553a2", "#946d13"];
    function text(t, a, b) {const e = document.createElementNS(ns, "text"); e.textContent = t; e.setAttribute("x", a); e.setAttribute("y", b); e.setAttribute("font-size", "11"); svg.appendChild(e);}
    text(`${ymax.toFixed(2)}%`, 1, 25); text(`${ymin.toFixed(2)}%`, 1, 195);
    text(new Date(xmin).toISOString().slice(0,10),55,222); text(new Date(xmax).toISOString().slice(0,10),785,222);
    series.forEach((s,i) => {const e = document.createElementNS(ns, "polyline"); e.setAttribute("fill","none"); e.setAttribute("stroke", colors[i%colors.length]); e.setAttribute("stroke-width","2"); e.setAttribute("points",s.curve.map(p => `${x(p.timeMs)},${y(p.returnPct)}`).join(" ")); svg.appendChild(e);});
  }
  function update() {
    const ids = active(), window = selectWindow.value;
    const {accounts,aggregates} = localView(data.summary,window,ids);
    table("comparison", ["Allocation", "Status", "Return %", "MTM DD %", "Profitable tickers", "Positive / all-account-observed months", "Observed / incomplete-account months", "Trades", "Exposure %", "Fees USDT", "Top 5 / net %", "Top month / net %"], aggregates.map(a =>
      [data.allocationLabels[a.allocationId], a.status, a.returnPct, a.maxDrawdownMtmPct, a.availableTickers == null ? null : `${a.profitableTickers}/${a.availableTickers}`, a.monthBreadth ? `${a.monthBreadth.positiveMonths}/${a.monthBreadth.breadthDenominator}` : null, a.monthBreadth ? `${a.monthBreadth.observedMonths}/${a.monthBreadth.partialMonths}` : null, a.trades,a.timeInMarketPct,a.fees,a.topFiveTickerContributionPct,a.largestPositiveMonthContributionPct]));
    plot("aggregateCurve", aggregates); byId("aggregateLegend").textContent = ids.map(id => data.allocationLabels[id]).join(" · ");
    const map = new Map();
    accounts.forEach(a => {if (!map.has(a.instrumentId)) map.set(a.instrumentId,{instrumentId:a.instrumentId});
      const r = map.get(a.instrumentId); ["returnPct","maxDrawdownMtmPct","trades"].forEach(k => r[a.allocationId+"/"+k] = a.coverage.observedBars ? a[k] : null);});
    const columns = ids.flatMap(id => ["returnPct","maxDrawdownMtmPct","trades"].map(metric => ({key:id+"/"+metric,label:data.allocationLabels[id]+" · "+metric,metric})));
    if (!columns.some(c => c.key === state.key)) state = {key: "", descending:true};
    displayed = orderTickers(Array.from(map.values()),state.key,state.descending);
    if (!displayed.some(r => r.instrumentId === selected)) selected = displayed[0]?.instrumentId || null;
    selectTicker.replaceChildren(); displayed.forEach(r => option(selectTicker,r.instrumentId,r.instrumentId)); selectTicker.value = selected || "";
    const t = byId("tickers"); t.replaceChildren(); const h = element("tr",null,element("thead",null,t)); element("th","Ticker",h);
    columns.forEach(c => {const button = element("button",c.label+(c.metric === "returnPct" ? " ("+profitableCount(displayed,c.key)+")" : ""),element("th",null,h)); button.onclick = () => {state = sortState(state,c.key); update();};});
    const body = element("tbody",null,t); displayed.forEach(r => {const tr = element("tr",null,body); tr.classList.toggle("selected",r.instrumentId === selected); element("td",r.instrumentId,tr); columns.forEach(c => element("td",fmt(r[c.key]),tr)); tr.onclick = () => {selected=r.instrumentId;update();};});
    const selectedAccounts = accounts.filter(a => a.instrumentId === selected);
    byId("tickerTitle").textContent = (selected || "No ticker")+" cumulative return (%)";
    plot("tickerCurve", selectedAccounts);
    const rows = [...aggregates.map(a => ({...a,instrumentId:"Aggregate"})), ...selectedAccounts];
    table("months",["Ticker", "Allocation", "Month", "PnL USDT", "Contribution %", "Return %", "Status", "Observed / expected", "Partial edge", "Exact boundaries"], rows.flatMap(a => a.months.map(m =>
      [a.instrumentId,a.allocationId,m.month,m.pnl,m.contributionPct,m.returnPct,m.returnStatus,`${m.observedBars}/${m.expectedBars}`,m.partialEdgeMonth,`${m.openingExact}/${m.closingExact}`])));
    byId("disclosures").textContent = JSON.stringify({request:data.request,identities:data.identities,limitations:data.summary.limitations,selectedAccounts:selectedAccounts.map(({curve,months,...rest})=>rest),aggregates:aggregates.map(({curve,months,...rest})=>rest)},null,2);
    updateRobustness(aggregates,selectedAccounts,window);
  }
  const date=t=>t==null?'Unavailable':new Date(t).toISOString();
  function updateRobustness(aggregates,selectedAccounts,window) {
    const overview=globalView(data,overviewAllocation.value);
    byId('protocol').textContent=JSON.stringify(data.request.protocol,null,2);
    table('windowOverview',['Window','Start UTC','End UTC','Days','Status','Return %','Bar-close DD %','Net USDT','Trades','Time in market %','Profitable / available tickers','Positive / all-account-observed months','Observed months','Calendar edge months','Incomplete-account months','Non-exact boundary months'],overview.map(w=>{
      const a=w.aggregate,r=a?.robustness;
      return [w.windowId,date(w.startMs),date(w.endMs),w.durationMs/86400000,a?.status,a?.returnPct,a?.maxDrawdownMtmPct,a?.netPnl,a?.trades,a?.timeInMarketPct,a?.availableTickers==null?null:`${a.profitableTickers}/${a.availableTickers}`,a?.monthBreadth?`${a.monthBreadth.positiveMonths}/${a.monthBreadth.breadthDenominator}`:null,r?.observedMonths,r?.partialEdgeMonths,r?.incompleteAccountMonths,r?.nonExactBoundaryMonths];
    }));
    table('overlaps',['Window A','Window B','Shared hours','% of A','% of B'],(data.summary.robustness?.overlaps||[]).map(x=>[x.windowA,x.windowB,x.sharedMs/3600000,x.sharedPctOfA,x.sharedPctOfB]));
    const matrix=monthlyMatrix(data,overviewAllocation.value);
    table('monthMatrix',['Month',...overview.map(w=>w.windowId)],matrix.map(row=>[row.month,...row.cells.map(c=>{
      if(c.status==='outsideWindow')return 'Outside window';
      const m=c.strategy,p=c.passive;
      return `${date(p.startMs)} to ${date(p.endMs)} | Strategy: ${fmt(m?.returnPct)}% (${m?.returnStatus||c.status}); PnL ${fmt(m?.pnl)} USDT; contribution ${fmt(m?.contributionPct)}%; observed ${fmt(m?.observedBars)}/${fmt(m?.expectedBars)}; all accounts ${fmt(m?.allAccountsObserved)}; calendar edge ${p.partialEdgeMonth}; exact boundaries ${fmt(m?.openingExact)}/${fmt(m?.closingExact)} | Passive long price: ${fmt(p.returnPct)}% ${p.direction||p.reason}; endpoints ${p.usableAccounts}/${p.plannedAccounts}; bars ${p.observedBars}/${p.expectedBars}; interior gap ${p.interiorGap}`;
    })]));
    table('tickerRemoval',['Allocation','Requested / removed','Removed tickers','Original / remaining accounts','Original / remaining capital USDT','Original / remaining PnL USDT','Remaining return %','Status'],aggregates.flatMap(a=>{
      const r=a.robustness?.removals;if(!r||r.status!=='available')return [[a.allocationId,null,null,null,null,null,null,r?.reason||'unavailable']];
      return r.tickerExTop.map(x=>[a.allocationId,`${x.requestedCount}/${x.removedCount}`,x.removedIds.join(', '),`${x.originalCount}/${x.remainingCount}`,`${fmt(x.originalCapital)}/${fmt(x.remainingCapital)}`,`${fmt(x.originalPnl)}/${fmt(x.remainingPnl)}`,x.remainingReturnPct,x.status]);
    }));
    table('erasure',['Allocation','Unit','Status','Removed / original count','Removed fraction','Selected IDs','Remaining PnL USDT','All units removed'],aggregates.flatMap(a=>['ticker','month'].map(unit=>{
      const x=a.robustness?.removals?.[unit+'ProfitErasure'];return [a.allocationId,unit,x?.status||'unavailable',x?`${fmt(x.removedCount)}/${x.originalCount}`:null,x?.removedFraction,x?.removedIds.join(', '),x?.remainingPnl,x?.allRemoved];
    })));
    table('monthRemoval',['Allocation','Selected month / count','Removed PnL USDT','Original capital USDT','Remaining accounting PnL USDT','Remaining contribution %','Selected coverage flags','Total / observed months','Calendar edges / incomplete-account months'],aggregates.map(a=>{
      const r=a.robustness,x=r?.removals?.monthExBest;return [a.allocationId,x?`${x.selectedMonth||'None'} / ${x.removedCount}`:r?.removals.reason,x?.removedPnl,x?.originalCapital,x?.remainingPnl,x?.remainingContributionPct,JSON.stringify(x?.selectedFlags),`${fmt(r?.totalMonths)}/${fmt(r?.observedMonths)}`,`${fmt(r?.partialEdgeMonths)}/${fmt(r?.incompleteAccountMonths)}`];
    }));
    table('sideAttribution',['Account / allocation','Side','Status','Trades','Gross USDT','Fees USDT','Net USDT','Holding hours','Capital contribution %','Time in market %'],[...aggregates,...selectedAccounts].flatMap(a=>(a.robustness?.sides||[]).map(s=>[`${a.instrumentId||'Aggregate'} / ${a.allocationId}`,s.side,s.status,s.trades,s.grossPnl,s.fees,s.netPnl,s.holdingMs==null?null:s.holdingMs/3600000,s.contributionPct,s.timeInMarketPct])));
    const pair=pairView(data,window,baseline.value,candidate.value);
    byId('pairStatus').textContent=pair.status==='available'?`Candidate minus baseline; ticker breadth ${JSON.stringify(pair.tickerCounts)}; monthly breadth ${JSON.stringify(pair.monthCounts)}; eligible calendar edges ${pair.eligiblePartialEdgeMonths}. Lower drawdown is better.`:pair.reason;
    table('pairTotals',['Return pp','Bar-close DD pp','Net USDT','Trades','Time in market pp'],pair.deltas?[Object.values(pair.deltas)]:[]);
    table('pairMonths',['Month','Interval UTC','Baseline return %','Candidate return %','Delta pp','Breadth status','Exclusion reason','Calendar edge','Gaps disclosed'],pair.months.map(m=>[m.month,`${date(m.startMs)} to ${date(m.endMs)}`,m.baselineReturnPct,m.candidateReturnPct,m.delta,m.status,m.reason,m.partialEdgeMonth,m.interiorGap]));
    const tickerRows=orderTickers(pair.tickers,pairSort.key,pairSort.descending);
    table('pairTickers',['Ticker','Baseline return %','Candidate return %','Delta pp','Status'],tickerRows.map(r=>[r.instrumentId,r.baselineReturnPct,r.candidateReturnPct,r.delta,r.status]));
    const headers=byId('pairTickers').querySelectorAll('th');
    ['baselineReturnPct','candidateReturnPct','delta'].forEach((key,i)=>{headers[i+1].replaceChildren();const b=element('button',['Baseline return %','Candidate return %','Delta pp'][i],headers[i+1]);b.onclick=()=>{pairSort=sortState(pairSort,key);update();};});
    byId('pairSettings').textContent=JSON.stringify(data.request.allocations.filter(a=>[baseline.value,candidate.value].includes(a.allocationId)),null,2);
  }
  overviewAllocation.onchange=update;baseline.onchange=update;candidate.onchange=update;
  selectWindow.onchange = update; selectAllocations.onchange = update;
  selectTicker.onchange = () => {selected=selectTicker.value;update();};
  function move(delta) {selected=moveTicker(displayed.map(r=>r.instrumentId),selected,delta);update();}
  byId("up").onclick = () => move(-1); byId("down").onclick = () => move(1);
  document.addEventListener("keydown", event => {if ((event.key === "ArrowUp" || event.key === "ArrowDown") && acceptsArrow(event.target,selectTicker,byId("tickerRegion"))) {event.preventDefault();move(event.key === "ArrowUp" ? -1 : 1);}});
  update();
}
