/* Presentation only. All financial metrics come from saved compact records. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const manifest = JSON.parse($('bootstrap').textContent), Core = globalThis.ConfigurationReportCore;
  const metadata = manifest.metadata, request = metadata.request;
  const allocationMap = new Map(request.allocations.map(a => [a.allocationId, a]));
  const flattened = request.allocations.map(a => Core.scalarPaths(a));
  const varying = [...new Set(flattened.flatMap(a => Object.keys(a)))].filter(p => !['/allocationId','/timeframeMinutes'].includes(p) && new Set(flattened.map(a => Core.key(a[p]))).size > 1).sort();
  const labels = manifest.presentation.parameterLabels || [];
  const parameter = p => ({key:p, label:labels.find(l=>l.path===p)?.label || p.split('/').at(-1).replace(/([a-z])([A-Z])/g,'$1 $2'), shortLabel:labels.find(l=>l.path===p)?.shortLabel, suffix:labels.find(l=>l.path===p)?.unit || ''});
  const instruments = [...new Set(metadata.configurations.map(c => c.instrumentId))];
  const accounts = metadata.configurations.map((c, ordinal) => {
    const [venue, contract] = c.instrumentId.split('_'), tf=c.timeframeMinutes;
    const roles=manifest.annotations.annotations.filter(a=>a.instrumentId===c.instrumentId && a.allocationId===c.allocationId).map(a=>a.role);
    return {...c, id:c.configurationId, ordinal, configId:c.allocationId, tf, tfLabel:tf%1440===0?`${tf/1440}D`:tf%60===0?`${tf/60}h`:`${tf}m`,
      symbol:instruments.filter(i=>i.split('_')[1]===contract).length>1?`${contract} \u00b7 ${venue}`:contract,
      params:Core.scalarPaths(allocationMap.get(c.allocationId)), roles:roles.filter(r=>r!=='reference'), isDefault:roles.includes('reference'), stages:{}};
  });
  const byId=Object.fromEntries(accounts.map(a=>[a.id,a]));
  const data={accounts, parameters:(labels.length?labels.map(l=>l.path):varying).map(parameter), filters:manifest.presentation.filterParameters.map(parameter),
    stages:Object.fromEntries(manifest.stages.map(s=>[s.stageId,s])), meta:{id:request.studyId,title:manifest.presentation.title||request.studyId, subtitle:manifest.presentation.subtitle||`${instruments.length} instruments / ${new Set(accounts.map(a=>a.tf)).size} timeframes / Independent IS / HO1 / HO2 accounts`,
    context:'Saved resolved configuration. Compact integrity verification cannot replay full execution.', initialSelection:manifest.presentation.initialSelection}};
  const loader=new Core.Loader(manifest,i=>$('block-'+i).textContent);
  const locations=new Map(), priceLocations=new Map();
  const rowIds=new Map(manifest.rows.filter(r=>r.status==='complete').map(r=>[r.resultId,r.configurationId]));
  manifest.index.members.forEach((entry,i)=>{
    if(entry.kind==='price')priceLocations.set(entry.instrumentId+'|'+entry.stageId,i);
    else entry.resultIds.forEach((rid,j)=>locations.set(rowIds.get(rid)+'|'+entry.stageId+'|'+entry.kind,[i,j]));
  });
  let generation=0, activeSeries=[], activeFacts=null, busy=false, requested=false, ready=false, errorState=null;
  const metricFields=['returnPct','annualizedProfitPct','annualizedProfitStatus','annualizedProfitShortSpan','performanceStatus','mtmMaxDdPct','balanceMaxDdPct','dailySharpe','dailySharpeStatus','dailySharpeObservations','dailySharpeActiveDays','trades','tradesPerMonth','winRatePct','profitFactor','profitFactorStatus','timeInMarketPct','maxEntryLeverage','exitReasons','attemptDispositions'];
  const loadedStages=new Set();
  const palette = ['#247e78','#c16c37','#6574b9','#ae567f','#83903c','#337ca5','#946535','#7e61a4','#bd4558','#384b94','#536c43','#b08713'];
  const pricePalette = ['#576773','#967d59','#777891','#718c7d'];
  const metrics = [
    {key:'returnPct',label:'Profit%',digits:2,signed:true,percent:true,title:'Net return for the whole stage; not annualized.'},
    {key:'mtmMaxDdPct',label:'MTM DD%',digits:2,percent:true,title:'Maximum drawdown from the full execution-bar-close equity path.'},
    {key:'balanceMaxDdPct',label:'Balance DD%',digits:2,percent:true,title:'Maximum drawdown from realized balance, including fees.'},
    {key:'dailySharpe',label:'Daily Sharpe',digits:2,title:'Observed UTC daily returns; 2% annual risk-free rate.'},
    {key:'trades',label:'Trades',digits:0,title:'Closed trades, including forced stage-end exits.'},
    {key:'tradesPerMonth',label:'Trades/mo',digits:2,title:'Closed trades divided by calendar-month equivalents.'}
  ];
  const annualMetric={key:'annualizedProfitPct',label:'Ann.P%',digits:2,signed:true,percent:true,title:'Geometric annual return: ((1 + Profit% / 100)^(365 / stage days) - 1) × 100. Merlin annualization rules.'};
  const tableMetrics=[metrics[0],annualMetric,metrics[1],metrics[5],metrics[3]];
  const cardMetrics=[annualMetric,metrics[1],metrics[5]];
  const detailRows = [metrics[0], annualMetric,
    metrics[1],metrics[2],metrics[3],{key:'dailySharpeObservations',label:'Sharpe days (active)',source:'metrics',format:'days'},
    metrics[4],metrics[5],{key:'winRatePct',label:'Win rate %',source:'metrics',digits:2,percent:true},
    {key:'profitFactor',label:'Profit factor',source:'metrics',digits:2},
    {key:'timeInMarketPct',label:'Time in market %',source:'metrics',digits:2,percent:true},
    {key:'maxEntryLeverage',label:'Max entry leverage (×)',source:'metrics',digits:2},
    {key:'exitReasons',label:'Exit reasons',source:'metrics',format:'counts'},
    {key:'attemptDispositions',label:'Entry attempts',source:'metrics',format:'counts'}];
  const humanize = text => String(text).replace(/([a-z])([A-Z])/g,'$1 $2').replace(/^./,c=>c.toUpperCase());
  const state = {selected:[],focus:null,solo:null,anchor:null,styles:{},nextStyle:0,ho2Visible:false,mode:'eq',price:false,priceFocus:null,
    stage:'all',view:null,sort:null,filters:{universe:'all',ticker:'',timeframe:'',search:'',params:{}}};
  let noticeTimer, plot=null, drag=null, hover=null;
  const stageIds=()=>state.ho2Visible?['IS','HO1','HO2']:['IS','HO1'];
  const stageInfo=s=>data.stages[s];
  function result(a,s){return s==='HO2'&&!state.ho2Visible?null:a.stages[s];}
  const shortDate = ms => new Date(ms).toISOString().slice(0,10);
  const fullDate = ms => new Date(ms).toISOString().slice(0,16).replace('T',' ')+' UTC';
  const stageLabel = s => { const d=stageInfo(s); return `${number(d.months,2)} months`; };
  const periodLabel = s => { const d=stageInfo(s); return `${shortDate(d.startMs)} → ${shortDate(d.endMs)} · ${d.months} months · UTC [start, end)`; };
  const paramsLabel = a => data.parameters.map(p => `${p.label} ${a.params[p.key]===undefined?'missing':Core.key(a.params[p.key])}`).join(' · ');
  const compactParams = a => data.parameters.map(p => `${p.shortLabel||p.label}${a.params[p.key]===undefined?'missing':Core.key(a.params[p.key])}${p.suffix||''}`).join(' / ');
  const accountLabel = a => `${a.symbol} ${a.tfLabel} · ${paramsLabel(a)}`;
  function number(v,d=2,signed=false) {
    if (v===null || v===undefined || !Number.isFinite(v)) return 'n/a';
    return (signed && v>0?'+':'')+v.toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
  }
  function metricValue(a,s,m) {
    const r=result(a,s);if(!r)return null;
    if(r.metrics.performanceStatus==='noObservedBars' && !['dailySharpeObservations','trades','exitReasons','attemptDispositions'].includes(m.key))return null;
    return r.metrics[m.key]??null;
  }
  function metricCell(a,s,m) {
    const r=result(a,s);
    if (!r) return `<span class="unavailable">${s==='HO2' && !state.ho2Visible ? 'Hidden' : errorState?'Error':'Loading'}</span>`;
    const source=r[m.source||'metrics'],v=metricValue(a,s,m), tone=m.signed && v!=null ? (v>0?'positive':v<0?'negative':'') : '';
    if(r.metrics.performanceStatus==='noObservedBars'&&!['dailySharpeObservations','trades','exitReasons','attemptDispositions'].includes(m.key))return '<span class="unavailable">No observed bars</span>';
    if(m.format==='counts')return v==null?'<span class="unavailable">n/a</span>':Object.entries(v).map(([k,n])=>`<span class="count-item">${esc(humanize(k))} <b>${number(n,0)}</b></span>`).join('');
    if(m.format==='days')return `<span title="${esc(r.metrics.dailySharpeStatus)}">${number(v,0)} (${number(r.metrics.dailySharpeActiveDays,0)})</span>`;
    const d=stageInfo(s),days=(d.endMs-d.startMs)/86400000,short=m.key==='annualizedProfitPct'&&r.metrics.annualizedProfitShortSpan&&v!=null;
    const title=m.key==='annualizedProfitPct' ? `${m.title} Saved status: ${source.annualizedProfitStatus}. Duration: ${number(days,2)} days.${v==null?' Unavailable for periods of 30 days or less, missing return or non-positive final capital.':short?' Short period: annualized value may be misleading.':''}` : v==null ? (source.performanceStatus==='noObservedBars'?'No observed bars':source[m.key+'Status'] || 'Unavailable in saved result') : `${m.label}: ${v}${m.percent?'%':''}`;
    return `<span class="${tone}" data-value="${v==null?'':esc(v)}" data-status="${esc(m.key==='annualizedProfitPct'?source.annualizedProfitStatus:source[m.key+'Status']||source.performanceStatus)}" title="${esc(title)}">${number(v,m.digits,m.signed)}${m.percent&&v!=null?'%':''}${short?'*':''}</span>`;
  }
  function style(a) {
    if (!(a.id in state.styles)) state.styles[a.id]=state.nextStyle++;
    const n=state.styles[a.id]; return {color:palette[n] || `hsl(${Math.round(n*137.508)%360} 46% 39%)`,dash:[]};
  }
  function swatch(a) { const s=style(a); return `<i class="sample ${s.dash.length?'dashed':''}" style="--series:${s.color}" aria-hidden="true"></i>`; }
  function badges(a) { return a.roles.map(r=>`<span class="role">${esc(r[0].toUpperCase()+r.slice(1))}</span>`).join('')+(a.isDefault?'<span class="role default">Default</span>':''); }
  function notify(message) { clearTimeout(noticeTimer); $('notice').textContent=message; $('notice').hidden=false; noticeTimer=setTimeout(()=>$('notice').hidden=true,4500); }
  function select(id) {
    if (state.selected.includes(id)) return true;
    state.selected.push(id); style(byId[id]); if (!state.focus) state.focus=id; return true;
  }
  function remove(id) {
    const index=state.selected.indexOf(id);
    state.selected=state.selected.filter(x=>x!==id);
    if (state.focus===id && state.solo!=='table') state.focus=state.selected[Math.min(Math.max(index,0),state.selected.length-1)]||null;
    if (state.solo==='selected'&&!state.selected.length) state.solo=null;
    if(state.priceFocus&&!state.selected.some(id=>byId[id].instrumentId===state.priceFocus))state.priceFocus=null;
  }
  const chartIds = () => state.solo&&state.focus ? [state.focus] : state.selected.length<=12 ? state.selected : [];
  const navigationIds = () => state.solo==='table' ? filterRows().map(a=>a.id) : state.selected;
  function exitSingle() {
    state.solo=null;state.priceFocus=null;
    if(!state.selected.includes(state.focus))state.focus=state.selected[0]||null;
    render();
  }
  function single(id,route) {
    if(state.solo&&state.focus===id){exitSingle();return;}
    state.solo=route;state.focus=id;state.priceFocus=null;style(byId[id]);
    render();scrollFocusedRow();
  }
  function moveSingle(direction) {
    const ids=navigationIds(),index=ids.indexOf(state.focus);
    const next=index<0 ? (direction>0?0:ids.length-1) : index+direction;
    if(next<0||next>=ids.length)return;
    state.focus=ids[next];state.priceFocus=null;style(byId[state.focus]);
    render();scrollFocusedRow();
  }
  function highlight(id) {
    state.solo=null;state.focus=id;state.priceFocus=null;
    render();scrollFocusedRow();
  }
  function moveHighlight(direction) {
    const ids=chartIds(),index=ids.indexOf(state.focus);
    const next=index<0 ? (direction>0?0:ids.length-1) : index+direction;
    if(next>=0&&next<ids.length)highlight(ids[next]);
  }
  function clearSelection() {
    state.selected=[];state.focus=null;state.solo=null;state.anchor=null;state.priceFocus=null;
    render();
  }
  function setChecked(ids,checked) {
    ids.forEach(id=>checked?select(id):remove(id));
  }
  function toggleRow(id,event) {
    const checked=!state.selected.includes(id),ids=filterRows().map(a=>a.id);
    const anchor=state.anchor,start=anchor?ids.indexOf(anchor.id):-1,end=ids.indexOf(id);
    if(!event.ctrlKey&&!event.shiftKey&&!event.target.closest('[data-select]')){
      select(id);state.anchor={id,checked:true};highlight(id);
      document.querySelector(`[data-inspect="${CSS.escape(id)}"]`)?.focus({preventScroll:true});
      return;
    }
    if(event.ctrlKey){setChecked(ids,checked);state.anchor={id,checked};}
    else if(event.shiftKey&&start>=0&&end>=0){setChecked(ids.slice(Math.min(start,end),Math.max(start,end)+1),anchor.checked);}
    else {setChecked([id],checked);state.anchor={id,checked};}
    if(!state.solo&&state.selected.includes(id)){state.focus=id;state.priceFocus=null;}
    render();
    document.querySelector(`[data-inspect="${CSS.escape(id)}"]`)?.focus({preventScroll:true});
  }
  function filterRows() {
    const f=state.filters;
    const rows=accounts.filter(a =>
      (f.universe==='all'||f.universe==='finalists'&&a.roles.length||f.universe==='default'&&a.isDefault||f.universe==='selected'&&state.selected.includes(a.id)) &&
      (!f.ticker||a.symbol===f.ticker)&&(!f.timeframe||String(a.tf)===f.timeframe)&&
      Object.entries(f.params).every(([k,v])=>!v||Core.key(a.params[k])===v)&&
      (!f.search||`${a.id} ${accountLabel(a)} ${JSON.stringify(a.params)}`.toLowerCase().includes(f.search.toLowerCase())));
    if (state.sort) rows.sort((a,b)=>{
      const m=tableMetrics.find(m=>m.key===state.sort.key);
      const av=metricValue(a,state.sort.stage,m),bv=metricValue(b,state.sort.stage,m);
      if (av==null&&bv==null) return a.ordinal-b.ordinal;
      if (av==null) return 1; if (bv==null) return -1;
      return (av-bv)*state.sort.dir || a.ordinal-b.ordinal;
    });
    return rows;
  }
  function renderStages() {
    $('stages').innerHTML=`<button data-stage="all" aria-pressed="${state.stage==='all'}">All visible stages</button>`+stageIds().map(s=>`<button data-stage="${s}" aria-pressed="${state.stage===s}">${s}<small>${stageLabel(s)}</small></button>`).join('');
    $('reveal').textContent=state.ho2Visible?'Hide HO2':'Reveal HO2';
    $('reveal').setAttribute('aria-pressed',String(state.ho2Visible));
    if(metadata.visibility==='public'){$('reveal').disabled=true;$('reveal').textContent='HO2 not included in this public report';}
  }
  function renderSelection() {
    $('selection-count').textContent=`${state.selected.length} selected · ${chartIds().length} shown`;
    $('selection').innerHTML=state.selected.length ? state.selected.map(id=>{
      const a=byId[id];
      return `<div class="series-chip ${state.focus===id&&!state.priceFocus?'active':''}"><button data-focus="${esc(id)}" aria-pressed="${state.focus===id&&!state.priceFocus}" title="Click to highlight; Alt+click to browse selected configurations. ${esc(accountLabel(a))}">${swatch(a)}${esc(a.symbol)} ${esc(a.tfLabel)} <span class="muted">${esc(a.configId)}</span></button><button class="remove" data-remove="${esc(id)}" aria-label="Remove ${esc(id)}">×</button></div>`;
    }).join('') : '<span class="empty">Choose configurations from the library below to start comparing.</span>';
  }
  function renderSingle() {
    const ids=navigationIds(),index=ids.indexOf(state.focus);
    $('single-view').classList.toggle('inactive',!state.solo);
    $('single-view').setAttribute('aria-hidden',String(!state.solo));
    $('single-view').inert=!state.solo;
    $('single-status').textContent=`Single view · ${state.solo==='table'?'Table':'Selected'} · ${index<0?'Outside list':index+1+' / '+ids.length}`;
    $('chart-limit').hidden=!!state.solo||state.selected.length<=12;
  }
  function renderInspector() {
    const a=byId[state.focus];
    if (!a) {$('inspector').innerHTML='<p class="empty">Select a configuration or click its name in the library to see its settings and saved stage metrics.</p>';return;}
    const ss=stageIds();
    const label=k=>data.meta.settingsLabels?.[k] || data.parameters.find(p=>p.key===k)?.label || humanize(k);
    const parameterKeys=[...new Set([...data.parameters.map(p=>p.key),...Object.keys(a.params)])];
    const settings=parameterKeys.map(k=>`<dt>${esc(label(k))}</dt><dd>${esc(a.params[k]===undefined?'missing':Core.key(a.params[k]))}${esc(data.meta.settingsUnits?.[k]||'')}</dd>`).join('');
    const rows=detailRows.map(m=>`<tr><th scope="row" title="${esc(m.title||m.label)}">${m.label}</th>${ss.map(s=>`<td data-metric="${m.key}" data-stage="${s}">${metricCell(a,s,m)}</td>`).join('')}</tr>`).join('');
    $('inspector').innerHTML=`
      <div class="focus-summary"><div><h2 class="focus-name">${state.selected.includes(a.id)?swatch(a):''}${esc(a.symbol)} <span class="muted">${esc(a.tfLabel)}</span> ${badges(a)}</h2><div class="config-code">${esc(a.id)}</div></div><button id="focus-only">Chart only this configuration</button></div>
      <div class="focus-content"><section class="settings-panel"><h3>Parameters</h3><dl class="settings-list"><dt>Instrument</dt><dd>${esc(a.instrumentId)}</dd>${settings}</dl><details><summary>Complete resolved allocation and bindings</summary><pre>${esc(JSON.stringify({allocation:allocationMap.get(a.allocationId),sources:request.sources,auxiliaryInputs:request.auxiliaryInputs||[]},null,2))}</pre></details><details><summary>Active visible-stage execution facts</summary><pre>${esc(activeFacts?JSON.stringify(activeFacts,null,2):errorState?'Unavailable: decode error':'Loading / no native facts available')}</pre></details><p class="inspector-note">${esc(data.meta.context)}</p></section>
      <div class="focus-table-scroll" tabindex="0" aria-label="Saved statistics by evaluation stage"><table class="metric-grid"><thead><tr><th scope="col">Saved statistics</th>${ss.map(s=>{const d=stageInfo(s);return `<th scope="col" title="${esc(periodLabel(s))}"><strong>${s}</strong><span>${shortDate(d.startMs)} → ${shortDate(d.endMs)}</span><span>${stageLabel(s)} · UTC [start, end)</span></th>`;}).join('')}</tr></thead><tbody>${rows}</tbody></table></div></div>
      <div class="focus-footer"><p class="inspector-note focus-note">${esc(manifest.annotations.annotations.filter(n=>n.instrumentId===a.instrumentId&&n.allocationId===a.allocationId).map(n=>n.role+': '+n.methodId+' - '+n.methodDescription).join('; ')||'No IS annotations supplied.')} ${!state.ho2Visible?'HO2 outcomes remain hidden.':''} Profit% is the whole-stage return. Ann.P% is the exact saved annualizedProfitPct, rounded only for display.</p><a href="#report-top" class="text-button" id="back-to-chart">Back to chart ↑</a></div>`;
    $('focus-only').onclick=()=>{single(a.id,'table');$('chart').scrollIntoView({block:'center'});$('chart').focus({preventScroll:true});};
  }
  function renderCards() {
    const a=byId[state.focus];
    $('focus-title').textContent=a?`${a.symbol} · ${a.tfLabel}`:'Choose a configuration';
    $('focus-settings').textContent=a?compactParams(a):'Select a configuration from the library';
    const ss=stageIds();
    $('focus-cards').innerHTML=cardMetrics.map(m=>`<div class="metric-card" data-card="${m.key}"><div class="card-label" title="${esc(m.title)}">${m.label}</div><div class="card-stages">${ss.map(s=>`<div data-card-stage="${s}"><span class="card-stage-label">${s}</span><strong>${a?metricCell(a,s,m):'—'}</strong></div>`).join('')}</div></div>`).join('');
  }
  function renderTable() {
    const active=document.activeElement;
    const restore=active?.dataset.select ? `[data-select="${CSS.escape(active.dataset.select)}"]` : active?.dataset.sort ? `[data-sort="${CSS.escape(active.dataset.sort)}"][data-sort-stage="${active.dataset.sortStage}"]` : null;
    const rows=filterRows(),ss=stageIds(),focused=byId[state.focus],pinned=focused&&!rows.includes(focused);
    const allDisplayed=pinned?[focused,...rows]:rows;
    const box=document.querySelector('.table-scroll');
    const first=Math.min(Math.max(0,allDisplayed.length-1),Math.max(0,Math.floor(box.scrollTop/34)-10));
    const displayed=allDisplayed.slice(first,first+60);
    const spacer=n=>n?`<tr aria-hidden="true" class="virtual-spacer" style="height:${n*34}px"><td colspan="${2+ss.length*tableMetrics.length}" style="height:${n*34}px;padding:0;border:0"></td></tr>`:'';
    $('row-count').textContent=`${rows.length} / ${accounts.length}${pinned?' · focused row outside filters':''}`;
    $('table-period').textContent=state.sort?`Sorted by ${state.sort.stage} ${tableMetrics.find(m=>m.key===state.sort.key).label} ${state.sort.dir===1?'↑':'↓'}`:'Separate stage metrics · frozen finalist roles';
    $('results').style.minWidth=(380+ss.length*tableMetrics.length*90)+'px';
    $('table-columns').innerHTML='<col style="width:40px"><col style="width:340px">'+ss.map(()=>tableMetrics.map(()=>'<col>').join('')).join('');
    $('table-head').innerHTML=`<tr class="period-groups"><th rowspan="2"><input id="select-visible" type="checkbox" aria-label="Select all configurations matching current filters" title="Select or clear all filtered rows, including rows below the scroll viewport"></th><th rowspan="2" scope="col">Configuration <span class="muted">· R / S / Ref roles</span></th>${ss.map(s=>`<th class="period-${s}" colspan="${tableMetrics.length}" scope="colgroup" title="${esc(periodLabel(s))}">${s} <span>${stageLabel(s)}</span></th>`).join('')}</tr><tr class="period-metrics">${ss.map(s=>tableMetrics.map((m,i)=>{const sorted=state.sort?.key===m.key&&state.sort.stage===s;return `<th class="period-${s} ${i===0?'period-start':''}" scope="col" aria-sort="${sorted?(state.sort.dir===1?'ascending':'descending'):'none'}"><button data-sort="${m.key}" data-sort-stage="${s}" title="${s}: ${esc(m.title)}">${m.label}${sorted?`<span class="sort-arrow">${state.sort.dir===1?'↑':'↓'}</span>`:''}</button></th>`;}).join('')).join('')}</tr>`;
    const checkedCount=rows.filter(a=>state.selected.includes(a.id)).length;
    $('select-visible').checked=!!rows.length&&checkedCount===rows.length;
    $('select-visible').indeterminate=checkedCount>0&&checkedCount<rows.length;
    $('select-visible').disabled=!rows.length;
    $('table-body').innerHTML=displayed.length?spacer(first)+displayed.map(a=>{
      const outside=pinned&&a===focused;
      const roles=a.roles.map(r=>`<span class="role" title="${esc(humanize(r))} finalist">${esc(r[0].toUpperCase())}</span>`).join('')+(a.isDefault?'<span class="role default" title="IS reference">Ref</span>':'');
      return `<tr data-account="${esc(a.id)}" class="${state.selected.includes(a.id)?'selected ':''}${a===focused?'focused ':''}${outside?'pinned':''}" aria-current="${a===focused?'true':'false'}"><td><input type="checkbox" data-select="${esc(a.id)}" aria-label="Compare ${esc(a.id)}" ${state.selected.includes(a.id)?'checked':''}></td><td><button class="row-name" data-inspect="${esc(a.id)}" title="${esc(a.id+' · '+paramsLabel(a))}">${state.selected.includes(a.id)?swatch(a):''}<span class="row-label">${esc(a.symbol)} · ${esc(a.tfLabel)} · ${esc(compactParams(a))}</span>${roles}${outside?'<span class="outside-label" title="Focused configuration is outside the current filters">Outside filters</span>':''}</button></td>${ss.map(s=>tableMetrics.map((m,i)=>`<td class="${i===0?'period-start':''}" data-metric="${m.key}" data-stage="${s}">${metricCell(a,s,m)}</td>`).join('')).join('')}</tr>`;
    }).join('')+spacer(Math.max(0,allDisplayed.length-first-displayed.length)):`<tr><td colspan="${2+ss.length*tableMetrics.length}" class="empty">No matching configurations. Reset filters to see the full grid.</td></tr>`;
    if(restore)document.querySelector(restore)?.focus({preventScroll:true});
    renderSingle();
  }
  function scrollFocusedRow() {
    let row=$('table-body').querySelector(`[data-account="${CSS.escape(state.focus||'')}"]`);
    const box=document.querySelector('.table-scroll');
    if(!row){const index=filterRows().findIndex(a=>a.id===state.focus);if(index<0)return;box.scrollTop=index*34;renderTable();row=$('table-body').querySelector(`[data-account="${CSS.escape(state.focus||'')}"]`);if(!row)return;}
    if(row.classList.contains('pinned')){box.scrollTop=0;return;}
    const r=row.getBoundingClientRect(),b=box.getBoundingClientRect(),head=$('table-head').getBoundingClientRect().height;
    if(r.top<b.top+head||r.bottom>b.bottom)box.scrollTop+=r.top-b.top-head-(box.clientHeight-head-r.height)/2;
  }
  function bounds() {
    const ss=state.stage==='all'?stageIds():[state.stage];
    return [Math.min(...ss.map(s=>stageInfo(s).startMs)),Math.max(...ss.map(s=>stageInfo(s).endMs))];
  }
  function visibleSeries(){return activeSeries;}
  function extent(series,lo,hi) {
    let min=0,max=0;
    for (const s of series) for (let i=0;i<s.t.length;i++) {
      if (s.t[i]<lo && (i===s.t.length-1||s.t[i+1]<lo)) continue;
      if (s.t[i]>hi && (i===0||s.t[i-1]>hi)) continue;
      const v=s.y[i]; if (v==null || !Number.isFinite(v)) continue; min=Math.min(min,v);max=Math.max(max,v);
    }
    const pad=Math.max((max-min)*.07,1),raw=(max-min+pad*2)/6,power=10**Math.floor(Math.log10(raw));
    const step=([1,2,2.5,5,10].find(n=>n*power>=raw)||10)*power;
    return [min-pad,max+pad,step];
  }
  // Claude's report calendar: width-aware month steps, then days on close zoom.
  function calendarTicks(lo,hi,width) {
    const day=86400000,spanMonths=(hi-lo)/(30.44*day),ticks=[];
    const months=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    if(spanMonths<2.5){
      const step=[1,2,7,14].find(n=>width/((hi-lo)/(n*day))>=88)||14;
      for(let t=Math.ceil(lo/day)*day;t<=hi;t+=step*day){
        const d=new Date(t);ticks.push({t,label:`${d.getUTCDate()} ${months[d.getUTCMonth()]} ${d.getUTCFullYear()}`,major:false});
      }
      return ticks;
    }
    const step=[1,2,3,6,12,24].find(n=>width/(spanMonths/n)>=64)||24;
    const first=new Date(lo),firstMonth=first.getUTCFullYear()*12+first.getUTCMonth();
    for(let index=firstMonth;;index++){
      const year=Math.floor(index/12),month=index%12,t=Date.UTC(year,month,1);
      if(t>hi)break;
      if(t>=lo&&index%step===0)ticks.push({t,label:month===0?String(year):months[month],major:month===0});
    }
    return ticks;
  }
  function draw() {
    const canvas=$('chart'),r=canvas.getBoundingClientRect(),dpr=window.devicePixelRatio||1;
    if (!r.width || !r.height) return;
    canvas.width=Math.round(r.width*dpr);canvas.height=Math.round(r.height*dpr);
    const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);
    const W=r.width,H=r.height,left=70,right=25,top=43,bottom=40,w=W-left-right,h=H-top-bottom;
    const limits=bounds(),range=state.view||limits,[lo,hi]=range;
    const series=visibleSeries(),accountSeries=series.filter(s=>!s.price),priceSeries=series.filter(s=>s.price);
    const yr=extent(series,lo,hi);
    const x=t=>left+(t-lo)/(hi-lo)*w, y=v=>top+h-(v-yr[0])/(yr[1]-yr[0])*h;
    const ticks=calendarTicks(lo,hi,w);
    plot={left,right,top,bottom,w,h,W,H,lo,hi,x,y,series,yr,ticks};
    ctx.font='12px Segoe UI';ctx.fillStyle='#65747b';ctx.textAlign='left';
    ctx.fillText(state.price?'Account & real price change % — shared scale':`${state.mode==='eq'?'Equity':'Balance'} change %`,left,15);
    ctx.save();ctx.beginPath();ctx.rect(left,top,w,h);ctx.clip();
    stageIds().forEach((s,i)=>{const info=stageInfo(s);ctx.fillStyle=i%2?'#f3f7f3':'#fcfdfc';ctx.fillRect(x(info.startMs),top,x(info.endMs)-x(info.startMs),h);});
    ctx.restore();
    for (let v=Math.ceil(yr[0]/yr[2])*yr[2];v<=yr[1];v+=yr[2]) {
      const yy=y(v);
      ctx.strokeStyle='#e5ebe6';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(W-right,yy);ctx.stroke();
      ctx.fillStyle='#718078';ctx.textAlign='right';ctx.fillText(number(v===0?0:v,Math.abs(v)<10?1:0)+'%',left-10,yy+4);
    }
    ctx.strokeStyle='#a5b6ac';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(left,y(0));ctx.lineTo(W-right,y(0));ctx.stroke();
    // Month grid lines remain visible; January emphasizes the year transition.
    ticks.forEach(tick=>{
      const xx=x(tick.t);ctx.strokeStyle=tick.major?'#c4d0c8':'#e6ece7';ctx.lineWidth=1;
      ctx.beginPath();ctx.moveTo(xx,top);ctx.lineTo(xx,top+h);ctx.stroke();
      ctx.font=(tick.major?'600 ':'')+'12px Segoe UI';ctx.fillStyle=tick.major?'#263e32':'#718078';
      const half=ctx.measureText(tick.label).width/2;
      ctx.textAlign='center';ctx.fillText(tick.label,Math.max(half+3,Math.min(W-half-3,xx)),H-14);
    });
    const ss=state.stage==='all'?stageIds():[state.stage];
    for (const s of ss) {
      const d=stageInfo(s),a=Math.max(left,x(d.startMs)),b=Math.min(W-right,x(d.endMs));if(b<=a)continue;
      ctx.fillStyle='#5b7265';ctx.textAlign='left';ctx.font='11px Segoe UI';if(b-a>35)ctx.fillText(s,a+8,top-12);
      if(x(d.startMs)>=left&&x(d.startMs)<=W-right){ctx.setLineDash([3,4]);ctx.strokeStyle='#9caf9e';ctx.beginPath();ctx.moveTo(x(d.startMs),top);ctx.lineTo(x(d.startMs),H-bottom);ctx.stroke();ctx.setLineDash([]);}
    }
    ctx.save();ctx.beginPath();ctx.rect(left,top,w,h);ctx.clip();
    // Each stage has its own path, including its independent zero anchor.
    const highlighted=s=>state.priceFocus ? s.price&&s.a.instrumentId===state.priceFocus : !s.price&&s.id===state.focus;
    const ordered=[...priceSeries,...accountSeries].sort((a,b)=>Number(highlighted(a))-Number(highlighted(b)));
    for (const s of ordered) {
      ctx.beginPath();ctx.strokeStyle=s.color;ctx.setLineDash(s.dash);
      ctx.lineWidth=highlighted(s)?2.8:s.price?1.5:1.7;
      ctx.globalAlpha=state.priceFocus ? (highlighted(s)?1:.18) : s.price ? .65 : state.selected.length>6&&!highlighted(s) ? .4 : 1;
      let started=false;
      for(let i=0;i<s.t.length;i++){
        if(s.t[i]<lo && (i===s.t.length-1||s.t[i+1]<lo))continue;
        if(s.t[i]>hi && (i===0||s.t[i-1]>hi))continue;
        if(s.y[i]==null||!Number.isFinite(s.y[i])){started=false;continue;}
        if(s.price && i>0 && s.gaps.some(g=>s.t[i-1]<g.endMs && s.t[i]>g.startMs))started=false;
        const xx=x(s.t[i]),yy=y(s.y[i]);if(!started){ctx.moveTo(xx,yy);started=true;}else ctx.lineTo(xx,yy);
      }
      ctx.stroke();ctx.globalAlpha=1;ctx.setLineDash([]);
      if(!s.price)for(const gap of s.gaps){ctx.strokeStyle=s.color;ctx.setLineDash([2,3]);ctx.beginPath();ctx.moveTo(x(gap.startMs),y(gap.cashQ/100));ctx.lineTo(x(gap.endMs),y(gap.cashQ/100));ctx.stroke();ctx.setLineDash([]);ctx.fillText('\u25c7',x(gap.startMs),y(gap.cashQ/100)-4);}
    }
    if(hover){ctx.strokeStyle='#8e9f94';ctx.setLineDash([2,3]);ctx.beginPath();ctx.moveTo(hover.x,top);ctx.lineTo(hover.x,H-bottom);ctx.stroke();}
    ctx.restore();
    if (!accountSeries.length) {ctx.fillStyle='#6b7c71';ctx.font='14px Segoe UI';ctx.textAlign='center';ctx.fillText(!state.solo&&state.selected.length>12?'':chartIds().length?errorState?'Unable to decode selected data.':!ready?'Loading...':'No observed bars':'Select accounts from the configuration library.',left+w/2,top+h/2);}
    $('view-range').textContent=`${shortDate(lo)} — ${shortDate(hi)} UTC`;
    renderPriceLegend(priceSeries);
  }
  function renderPriceLegend(series) {
    const legend=$('price-legend');legend.hidden=!state.price;
    const html=[...new Map(series.map(s=>[s.a.instrumentId,s])).values()].map(s=>`<button data-price-focus="${esc(s.a.instrumentId)}" aria-pressed="${state.priceFocus===s.a.instrumentId}" title="Highlight ${esc(s.a.symbol)} real price; click again to clear"><i class="sample dashed" style="--series:${s.color}"></i>${esc(s.a.symbol)} real price${s.unavailable?' - No observed bars':''}</button>`).join('');
    const contents=html?`<span class="price-hint">PRICE CONTEXT</span>${html}<span class="price-hint">Click a ticker to highlight · click again to clear</span>`:'No price series for the current selection.';
    // Do not replace focused legend controls during pointer hover or zoom redraws.
    if(legend.innerHTML!==contents){
      const focused=document.activeElement?.dataset.priceFocus;
      legend.innerHTML=contents;
      if(focused)legend.querySelector(`[data-price-focus="${CSS.escape(focused)}"]`)?.focus({preventScroll:true});
    }
  }
  function setView(lo,hi) {
    const [min,max]=bounds(),width=Math.min(max-min,Math.max(3*86400000,hi-lo));
    lo=Math.min(Math.max(lo,min),max-width);state.view=[lo,lo+width];hover=null;$('tooltip').hidden=true;draw();
  }
  function zoom(factor,anchor=.5) {const [lo,hi]=state.view||bounds(),width=(hi-lo)*factor,at=lo+(hi-lo)*anchor;setView(at-width*anchor,at+width*(1-anchor));}
  function resetView() {state.view=null;hover=null;$('tooltip').hidden=true;draw();}
  function nearest(ts,t) {let lo=0,hi=ts.length-1;while(lo<hi){const m=Math.floor((lo+hi)/2);if(ts[m]<t)lo=m+1;else hi=m;}return lo>0&&t-ts[lo-1]<ts[lo]-t?lo-1:lo;}
  function showHover(e) {
    if(!plot)return;const r=$('chart').getBoundingClientRect(),xx=e.clientX-r.left,yy=e.clientY-r.top;
    if(xx<plot.left||xx>plot.left+plot.w||yy<plot.top||yy>plot.top+plot.h){hover=null;$('tooltip').hidden=true;draw();return;}
    const t=plot.lo+(xx-plot.left)/plot.w*(plot.hi-plot.lo);
    const s=stageIds().find(s=>{const d=stageInfo(s);return t>=d.startMs&&t<d.endMs;});
    if(!s)return;
    const relevant=plot.series.filter(line=>line.stage===s&&line.t.length&&(!line.price||!line.gaps.some(g=>t>=g.startMs&&t<g.endMs))); 
    const rows=relevant.map(line=>{const i=nearest(line.t,t);return {line,i};}).filter(({line,i})=>line.y[i]!=null);
    const stamp=rows.length?rows[0].line.t[rows[0].i]:t;
    $('tooltip').innerHTML=`<b>${s} · ${fullDate(stamp)}</b><div class="muted">Nearest saved observation · ${state.mode==='eq'?'equity':'balance'} %${state.price?' and real price %':''}</div>`+rows.map(({line,i})=>`<div class="tip-row ${!line.price&&line.id===state.focus?'active':''}" data-tip-account="${line.price?'':esc(line.id)}"><i class="sample ${line.dash.length?'dashed':''}" style="--series:${line.color}"></i><span>${esc(line.a.symbol)} ${line.price?'real price':`${esc(line.a.tfLabel)} ${esc(line.a.configId)}`}</span><strong>${number(line.y[i],2,true)}%</strong></div>`).join('')+(rows.length?'':'<p>No tested result here.</p>');
    $('tooltip').hidden=false;
    const tip=$('tooltip');tip.style.left=Math.max(0,Math.min(xx+15,plot.W-tip.offsetWidth-5))+'px';tip.style.top=Math.max(0,Math.min(yy+12,plot.H-tip.offsetHeight-4))+'px';hover={x:xx};draw();
  }
  let tableSignature=null;
  function patchTable(){
    const signature=JSON.stringify([state.filters,state.sort,state.ho2Visible,[...loadedStages]]);
    const focused=$('table-body').querySelector(`[data-account="${CSS.escape(state.focus||'')}"]`);
    if(signature!==tableSignature || state.focus&&!focused || $('table-body').querySelector('.pinned')){tableSignature=signature;renderTable();return;}
    const selected=new Set(state.selected);
    for(const row of $('table-body').querySelectorAll('[data-account]')){
      const id=row.dataset.account;row.classList.toggle('focused',id===state.focus);row.classList.toggle('selected',selected.has(id));row.setAttribute('aria-current',String(id===state.focus));row.querySelector('input').checked=selected.has(id);
    }
    const rows=filterRows(),n=rows.filter(a=>selected.has(a.id)).length,h=$('select-visible');if(h){h.checked=rows.length>0&&n===rows.length;h.indeterminate=n>0&&n<rows.length;h.disabled=!rows.length;}
  }
  function display(){hover=null;$('tooltip').hidden=true;renderStages();renderSelection();renderCards();renderInspector();patchTable();renderSingle();draw();}
  function render(){
    generation++;requested=true;ready=false;errorState=null;activeSeries=[];activeFacts=null;
    $('load-status').hidden=false;$('load-status').textContent='Loading selected saved data.';display();pump();
  }
  async function loadStage(stage,token){
    if(loadedStages.has(stage))return;
    const staged=new Map();
    for(let i=0;i<manifest.index.members.length;i++){
      const e=manifest.index.members[i];if(e.kind!=='metrics'||e.stageId!==stage)continue;
      if(token!==generation)return;
      const chunk=await loader.get(i,false);
      if(token!==generation)return;
      for(const r of chunk.records)staged.set(r.configurationId,{metrics:Object.fromEntries(metricFields.map(k=>[k,r.metrics[k]]))});
    }
    if(token!==generation)return;
    for(const [id,r] of staged)byId[id].stages[stage]=r;
    loadedStages.add(stage);tableSignature=null;
  }
  async function pump(){
    if(busy)return;busy=true;
    try{while(requested){
      requested=false;const token=generation;
      try{
        Core.capabilities();
        for(const s of stageIds()){await loadStage(s,token);if(token!==generation)break;}
        if(token!==generation)continue;
        const ss=state.stage==='all'?stageIds():[state.stage],ids=[...chartIds()],needed=[];
        for(const id of ids)for(const stage of ss){needed.push(locations.get(id+'|'+stage+'|curves')[0]);if(state.price)needed.push(priceLocations.get(byId[id].instrumentId+'|'+stage));}
        const factStage=state.stage==='all'?'IS':state.stage;
        if(state.focus)needed.push(locations.get(state.focus+'|'+factStage+'|metrics')[0]);
        loader.retain(needed);const decoded=new Map();
        for(const i of new Set(needed)){decoded.set(i,await loader.get(i));if(token!==generation)break;}
        if(token!==generation)continue;
        const series=[],prices=new Set();
        for(const id of ids)for(const stage of ss){
          const a=byId[id],[i,j]=locations.get(id+'|'+stage+'|curves'),chunk=decoded.get(i),c=chunk.records[j].curve;
          if(c.status!=='noObservedBars')series.push({id,a,stage,t:chunk.timeGrids[c.gridId],y:(state.mode==='eq'?c.equityQ:c.balanceQ).map(v=>v/c.valueScale),gaps:c.cashGaps,...style(a)});
          if(state.price){const pk=a.instrumentId+'|'+stage;
            if(!prices.has(pk)){prices.add(pk);const p=decoded.get(priceLocations.get(pk)).records[0].price;
              series.push({id:pk,a,stage,t:p.timeMs,y:p.priceQ.map(v=>v===null?null:v/p.valueScale),gaps:p.coverageGaps,unavailable:p.status==='noObservedBars',price:true,color:pricePalette[instruments.indexOf(a.instrumentId)%pricePalette.length],dash:[3,5]});}
          }
        }
        if(state.focus){const [i,j]=locations.get(state.focus+'|'+factStage+'|metrics');activeFacts={stage:factStage,...decoded.get(i).records[j].execution};}
        activeSeries=series;ready=true;$('load-status').textContent='';$('load-status').hidden=true;display();
      }catch(error){if(token===generation){activeSeries=[];activeFacts=null;errorState=typeof DecompressionStream!=='function'||!globalThis.crypto?.subtle?'Open this standalone report in an installed Chrome or Edge with Web Crypto and DecompressionStream enabled.':'Saved report data failed integrity or resource validation. Reopen a verified report or ask its author to rebuild it.';ready=false;$('load-status').hidden=false;$('load-status').textContent=errorState;display();}}
    }}finally{busy=false;}
  }
  function populate(){
    $('title').textContent=data.meta.title;$('subtitle').textContent=data.meta.subtitle;document.title=data.meta.title+' / Pattern Lab comparison';
    $('synthetic-banner').hidden=!metadata.synthetic;
    $('study-counts').innerHTML=`<div class="stat"><b>${accounts.length}</b><span>configurations</span></div><div class="stat"><b>${new Set(accounts.map(a=>a.instrumentId+'|'+a.tf)).size}</b><span>ticker / TF cells</span></div><div class="stat"><b>${accounts.filter(a=>a.roles.length).length}</b><span>IS finalists</span></div>`;
    $('ticker').innerHTML='<option value="">All tickers</option>'+[...new Set(accounts.map(a=>a.symbol))].map(s=>`<option>${esc(s)}</option>`).join('');
    $('timeframe').innerHTML='<option value="">All timeframes</option>'+[...new Map(accounts.map(a=>[a.tf,a.tfLabel]))].sort((a,b)=>a[0]-b[0]).map(([tf,l])=>`<option value="${tf}">${esc(l)}</option>`).join('');
    $('parameter-filters').innerHTML=data.filters.map(p=>`<label>${esc(p.label)}<select data-param="${esc(p.key)}"><option value="">All</option>${[...new Set(accounts.map(a=>Core.key(a.params[p.key])))].map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('')}</select></label>`).join('');
    $('execution-note').textContent=data.meta.context+' Initial capital and all policy settings are retained in each allocation.';
    $('limitations').textContent=metadata.limitation;
    $('method').textContent=JSON.stringify(manifest.annotations,null,2);
    $('reference-note').textContent=manifest.annotations.annotations.length?'Roles are separate supplied IS annotations.':'No IS finalist or reference annotations were supplied.';
    $('attribution').textContent=JSON.stringify(manifest.attribution,null,2);
    $('provenance').textContent='Saved exact metrics / three-day account curves / daily price context';
    data.meta.initialSelection.forEach(id=>select(id));
  }
  let scrollFrame=null;
  document.querySelector('.table-scroll').addEventListener('scroll',()=>{if(scrollFrame===null)scrollFrame=requestAnimationFrame(()=>{scrollFrame=null;renderTable();});});
  populate();
  $('stages').onclick=e=>{const b=e.target.closest('[data-stage]');if(!b)return;state.stage=b.dataset.stage;state.view=null;render();document.querySelector(`[data-stage="${state.stage}"]`).focus({preventScroll:true});};
  $('reveal').onclick=()=>{
    if(metadata.visibility==='public')return;
    state.ho2Visible=!state.ho2Visible;
    if(!state.ho2Visible){if(state.stage==='HO2')state.stage='all';if(state.sort?.stage==='HO2')state.sort=null;loadedStages.delete('HO2');for(const a of accounts)delete a.stages.HO2;loader.hide();}
    state.view=null;tableSignature=null;render();
  };
  $('equity').onclick=()=>{state.mode='eq';$('equity').setAttribute('aria-pressed','true');$('balance').setAttribute('aria-pressed','false');render();};
  $('balance').onclick=()=>{state.mode='bal';$('equity').setAttribute('aria-pressed','false');$('balance').setAttribute('aria-pressed','true');render();};
  $('price').onchange=e=>{state.price=e.target.checked;if(!state.price)state.priceFocus=null;render();};
  $('zoom-in').onclick=()=>zoom(.7);$('zoom-out').onclick=()=>zoom(1/.7);$('reset').onclick=resetView;
  $('clear').onclick=clearSelection;
  $('exit-single').onclick=exitSingle;
  $('selection').onclick=e=>{
    const removeButton=e.target.closest('[data-remove]'),focusButton=e.target.closest('[data-focus]');
    if(removeButton){remove(removeButton.dataset.remove);render();}
    else if(focusButton){
      const id=focusButton.dataset.focus;
      if(e.altKey)single(id,'selected');
      else highlight(id);
      document.querySelector(`[data-focus="${CSS.escape(id)}"]`)?.focus({preventScroll:true});
    }
  };
  $('table-body').onmousedown=e=>{if(e.shiftKey)e.preventDefault();};
  $('table-body').onclick=e=>{
    const row=e.target.closest('[data-account]');if(!row)return;
    e.preventDefault();const id=row.dataset.account;
    if(e.altKey){single(id,'table');document.querySelector(`[data-inspect="${CSS.escape(id)}"]`)?.focus({preventScroll:true});}
    else toggleRow(id,e);
  };
  $('table-head').onchange=e=>{if(e.target.id!=='select-visible')return;setChecked(filterRows().map(a=>a.id),e.target.checked);state.anchor=null;render();$('select-visible').focus({preventScroll:true});};
  $('table-head').onclick=e=>{const b=e.target.closest('[data-sort]');if(!b)return;const key=b.dataset.sort,stage=b.dataset.sortStage;state.sort={key,stage,dir:state.sort?.key===key&&state.sort.stage===stage?-state.sort.dir:-1};renderTable();};
  $('canonical').onclick=()=>{state.sort=null;renderTable();};
  for(const id of ['universe','ticker','timeframe'])$(id).onchange=e=>{state.filters[id]=e.target.value;renderTable();};
  $('search').oninput=e=>{state.filters.search=e.target.value;renderTable();};
  $('parameter-filters').onchange=e=>{state.filters.params[e.target.dataset.param]=e.target.value;renderTable();};
  $('reset-filters').onclick=()=>{state.filters={universe:'all',ticker:'',timeframe:'',search:'',params:{}};for(const id of ['universe','ticker','timeframe','search'])$(id).value=state.filters[id];document.querySelectorAll('[data-param]').forEach(e=>e.value='');renderTable();};
  $('add-finalists').onclick=()=>{filterRows().filter(a=>a.roles.length).forEach(a=>select(a.id));render();};
  $('price-legend').onclick=e=>{const b=e.target.closest('[data-price-focus]');if(!b)return;state.priceFocus=state.priceFocus===b.dataset.priceFocus?null:b.dataset.priceFocus;hover=null;$('tooltip').hidden=true;renderSelection();draw();};
  const canvas=$('chart');
  canvas.addEventListener('wheel',e=>{e.preventDefault();if(!plot)return;const r=canvas.getBoundingClientRect();zoom(Math.exp(Math.max(-1,Math.min(1,e.deltaY*.002))),Math.max(0,Math.min(1,(e.clientX-r.left-plot.left)/plot.w)));},{passive:false});
  canvas.onpointerdown=e=>{if(e.button!==0||!plot)return;canvas.focus();drag={x:e.clientX,range:[...(state.view||bounds())]};canvas.setPointerCapture(e.pointerId);};
  canvas.onpointermove=e=>{if(drag){const dt=(e.clientX-drag.x)/plot.w*(drag.range[1]-drag.range[0]);setView(drag.range[0]-dt,drag.range[1]-dt);}else showHover(e);};
  canvas.onpointerup=e=>{drag=null;if(canvas.hasPointerCapture(e.pointerId))canvas.releasePointerCapture(e.pointerId);};
  canvas.onpointercancel=()=>{drag=null;};canvas.onpointerleave=()=>{if(!drag){hover=null;$('tooltip').hidden=true;draw();}};
  canvas.ondblclick=resetView;
  canvas.onkeydown=e=>{if(['+','=','-','ArrowLeft','ArrowRight','Home'].includes(e.key)){e.preventDefault();if(e.key==='+'||e.key==='=')zoom(.7);else if(e.key==='-')zoom(1/.7);else if(e.key==='Home')resetView();else{const [lo,hi]=state.view||bounds(),d=(hi-lo)*.15*(e.key==='ArrowLeft'?-1:1);setView(lo+d,hi+d);}}};
  document.addEventListener('keydown',e=>{
    const active=document.activeElement;
    if(e.defaultPrevented||active?.isContentEditable||active?.matches('input:not([type="checkbox"]),textarea,select'))return;
    if(e.ctrlKey&&e.altKey&&!e.shiftKey&&!e.metaKey&&e.code==='KeyD'){
      e.preventDefault();clearSelection();return;
    }
    if(e.key==='Escape'){
      if(state.solo){e.preventDefault();exitSingle();}
      else if(hover){hover=null;$('tooltip').hidden=true;draw();}
      return;
    }
    if(e.altKey||e.ctrlKey||e.shiftKey||e.metaKey||!['ArrowUp','ArrowDown'].includes(e.key))return;
    e.preventDefault();const direction=e.key==='ArrowDown'?1:-1;
    if(state.solo)moveSingle(direction);else moveHighlight(direction);
  });
  new ResizeObserver(()=>draw()).observe(canvas.parentElement);
  render();
  // A small read-only inspection surface for browser receipts; never used to generate metrics.
  window.reportVisibleMetrics=()=>accounts.map(a=>({configurationId:a.id,stages:Object.fromEntries(stageIds().filter(s=>a.stages[s]).map(s=>[s,{...a.stages[s].metrics}]))}));
  window.reportDebug=()=>({ready,error:errorState,generation,cache:{bytes:loader.bytes,peak:loader.peak,running:loader.running,entries:loader.cache.size},decodes:{...loader.decoded},selected:[...state.selected],focus:state.focus,solo:state.solo,navigation:state.solo?[...navigationIds()]:[],ho2Visible:state.ho2Visible,revealed:state.ho2Visible?accounts.filter(a=>a.stages.HO2).map(a=>a.id):[],ho2Loaded:loadedStages.has('HO2'),
    stage:state.stage,sort:state.sort,mode:state.mode,price:state.price,priceFocus:state.priceFocus,view:state.view,range:bounds(),
    styles:JSON.parse(JSON.stringify(state.styles)),rows:filterRows().map(a=>a.id),
    axis:plot?{min:plot.yr[0],max:plot.yr[1],ticks:plot.ticks,zeroY:plot.y(0),value150Y:plot.y(150),value1000Y:plot.y(1000)}:null,
    series:visibleSeries().map(s=>({id:s.id,stage:s.stage,price:!!s.price,first:s.y[0],start:s.t[0],end:s.t.at(-1),color:s.color,dash:s.dash,instrument:s.a.instrumentId}))});
})();
