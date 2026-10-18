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
if (typeof module !== "undefined") module.exports = {orderTickers, sortState, moveTicker, profitableCount, acceptsArrow};
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
    const accounts = data.summary.accounts.filter(a => a.windowId === window && ids.includes(a.allocationId));
    const aggregates = data.summary.aggregates.filter(a => a.windowId === window && ids.includes(a.allocationId));
    table("comparison", ["Allocation", "Status", "Return %", "MTM DD %", "Profitable tickers", "Positive months / full", "Observed / partial months", "Trades", "Exposure %", "Fees USDT", "Top 5 / net %", "Top month / net %"], aggregates.map(a =>
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
  }
  selectWindow.onchange = update; selectAllocations.onchange = update;
  selectTicker.onchange = () => {selected=selectTicker.value;update();};
  function move(delta) {selected=moveTicker(displayed.map(r=>r.instrumentId),selected,delta);update();}
  byId("up").onclick = () => move(-1); byId("down").onclick = () => move(1);
  document.addEventListener("keydown", event => {if ((event.key === "ArrowUp" || event.key === "ArrowDown") && acceptsArrow(event.target,selectTicker,byId("tickerRegion"))) {event.preventDefault();move(event.key === "ArrowUp" ? -1 : 1);}});
  update();
}
