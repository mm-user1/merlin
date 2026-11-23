/* SSR02 presentation only: verified summaries and saved-fact distributions. */
(() => {
  'use strict';
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const finite=v=>typeof v==='number'&&Number.isFinite(v);
  const num=(v,n=1,signed=false)=>finite(v)?`${signed&&v>0?'+':''}${v.toLocaleString('en-US',{minimumFractionDigits:n,maximumFractionDigits:n})}`:'N/A';
  const pct=(v,signed=false)=>finite(v)?num(v,1,signed)+'%':'N/A';
  const pp=v=>finite(v)?num(v,1,true)+' pp':'N/A';
  const change=(a,b)=>finite(a)&&finite(b)?b-a:null;
  const tone=v=>finite(v)?v<0?'negative':v>0?'positive':'':'';
  const tf=n=>n%1440===0?`${n/1440}D`:n%60===0?`${n/60}h`:`${n}m`;
  const ratio=f=>f&&f.denominator?`${f.success}/${f.denominator}`:'0/0 · unavailable';
  const fraction=(success,denominator)=>({success,denominator,pct:denominator?100*success/denominator:null});
  const share=f=>f?`${pct(f.pct)} <small>${ratio(f)}</small>`:'N/A';
  const shortMonth=m=>new Date(m+'-01T00:00:00Z').toLocaleDateString('en-US',{month:'short',timeZone:'UTC'});
  const labels={interesting:'Interesting settings',profitable:'Profitable settings',annualizedProfitPct:'Median annualized return',mtmMaxDdPct:'Median MTM drawdown',returnPct:'Actual return'};
  const missing='<span class="missing">N/A</span>';
  const value=(b,m)=>m==='interesting'||m==='profitable'?b?.[m]?.pct:b?.medians?.[m]?.value;
  function heat(v,kind){
    if(!finite(v))return '#dddddd';
    const diverging=kind==='annualizedProfitPct'||kind==='monthly';
    const max=kind==='monthly'?20:kind==='annualizedProfitPct'?50:100;
    const t=Math.min(1,Math.abs(v)/max),to=kind==='mtmMaxDdPct'||diverging&&v<0?[216,164,158]:[132,186,172],from=[250,251,249];
    return `rgb(${from.map((c,i)=>Math.round(c+(to[i]-c)*t)).join(',')})`;
  }
  // Overview uses raw saved values; rounding is display-only.
  function overviewValue(block,metric,period){
    if(metric==='tradesPerMonth')return block?.medians?.tradesPerMonth?.value??null;
    if(metric==='positiveWithoutBest2')return block?.concentration?.own?.['2']?.positive?.pct??null;
    return value(block,metric);
  }
  function cardColor(data,metric,v){
    if(!finite(v))return '#dddddd';
    const limits=data.meta.overviewColors[metric];let t=0;
    if(metric==='mtmMaxDdPct')t=-Math.max(0,(v-limits[0])/(limits[1]-limits[0]));
    else if(v<limits[1])t=-(limits[1]-v)/(limits[1]-limits[0]);
    else if(v>limits[2])t=(v-limits[2])/(limits[3]-limits[2]);
    const base=[250,251,249],target=t<0?[239,207,203]:[201,225,216],strength=Math.min(1,Math.abs(t));
    return `rgb(${base.map((c,i)=>Math.round(c+(target[i]-c)*strength)).join(',')})`;
  }
  function overviewDelta(a,b,metric){
    const raw=change(a,b),digits=metric==='tradesPerMonth'?0:1;
    if(!finite(raw))return {text:'N/A',tone:''};
    const rounded=Number(raw.toFixed(digits))||0;
    return {text:num(rounded,digits,true)+(metric==='tradesPerMonth'?' trades/mo':' pp'),tone:tone(metric==='mtmMaxDdPct'?-rounded:rounded)};
  }
  function cardAttrs(metric,interactive){
    return interactive?`data-overview-metric="${metric}" role="button" tabindex="0" aria-pressed="false" aria-controls="matrix-grid"`:`data-scope-metric="${metric}"`;
  }
  function overview(data,scope='all',interactive=true){
    const s=data.scopes[scope],periods=data.meta.periods;
    return '<div class="overview-grid">'+retentionCard(data,scope,interactive)+[['interesting','Interesting settings'],['profitable','Profitable settings'],['positiveWithoutBest2','Profitable without best 2 months'],['annualizedProfitPct','Median annualized return'],['mtmMaxDdPct','Median MTM drawdown'],['tradesPerMonth','Median trades / month']].map(([m,label])=>{
      const values=periods.map(p=>overviewValue(s.periods[p.id],m,p)),delta=overviewDelta(0,s.changes[m],m);
      return `<div class="metric-card" ${cardAttrs(m,interactive)}><div class="metric-label">${label}</div><div class="metric-values">${periods.map((p,i)=>{
        const block=s.periods[p.id],v=values[i],fraction=['interesting','profitable'].includes(m)?block[m]:m==='positiveWithoutBest2'?block.concentration.own['2'].positive:null;
        const detail=fraction?ratio(fraction)+` settings · ${fraction.available} available / ${fraction.missing} missing`:m==='tradesPerMonth'?`${block.lowActivity}/${block.declared} below ${esc(data.meta.activityLabel.toLowerCase().replace(/^below /,""))}`:'n='+block.medians[m].n;
        return `<div class="metric-period" style="background:${cardColor(data,m,v)}"><small>${esc(p.label)}</small><b>${m==='tradesPerMonth'?num(v,0):pct(v,m==='annualizedProfitPct')}</b><em>${detail}</em></div>`;
      }).join('')}</div><div class="delta ${delta.tone}">${delta.text} change</div></div>`;
    }).join('')+(interactive?'<span class="overview-selection" aria-hidden="true"></span>':'')+'</div>';
  }
  function retentionCard(data,scope='all',interactive=true){
    const c=data.scopes[scope].persistence,periods=data.meta.periods;
    const metrics=['profitable','interesting'];
    return `<div class="metric-card retention-card" ${cardAttrs('retention',interactive)}><div class="metric-label">Success retained &middot; ${esc(periods[0].short)} &rarr; ${esc(periods[1].short)}</div><div class="metric-values">${metrics.map(k=>`<div class="metric-period" style="background:${cardColor(data,'retention',c[k+'EarlierSuccess']?.pct)}"><small>${k==='profitable'?'Profitable':'Interesting'}</small><b>${finite(c[k+'EarlierSuccess']?.pct)?pct(c[k+'EarlierSuccess'].pct):'N/A'}</b></div>`).join('')}</div><div class="retention-deltas">${metrics.map(k=>{const d=overviewDelta(0,c[k].retentionDifferencePp,'retention');return `<div class="delta ${d.tone}">${d.text==='N/A'?'N/A':d.text}</div>`;}).join('')}</div></div>`;
  }
  const matrixMetrics=[['retention','Success retained','Success retained'],['interesting','Interesting','Interesting settings'],['profitable','Profitable','Profitable settings'],['positiveWithoutBest2','Without best 2 months','Profitable without best 2 months'],['annualizedProfitPct','Ann. return','Median annualized return'],['mtmMaxDdPct','MTM DD','Median MTM drawdown'],['tradesPerMonth','Trades / month','Median trades / month']];
  const compactGroups=[['retention','interesting','profitable','positiveWithoutBest2'],['annualizedProfitPct','mtmMaxDdPct','tradesPerMonth']];
  function matrixButtons(state){
    if(state.compact)return compactGroups.map((group,i)=>`<button data-matrix-group="${i}" aria-pressed="${state.compactGroup===i}">${group.map(id=>matrixMetrics.find(m=>m[0]===id)[1]).join(' | ')}</button>`).join('');
    return matrixMetrics.map(([id,label])=>`<button data-matrix-metric="${id}" aria-pressed="${state.metric===id}">${label}</button>`).join('');
  }
  function matrixValues(data,scope,m){
    if(m==='retention'){
      const c=scope.persistence,metrics=['profitable','interesting'];
      return `<span class="metric-values">${metrics.map(k=>`<span class="metric-period" style="background:${cardColor(data,'retention',c[k+'EarlierSuccess']?.pct)}"><small>${k==='profitable'?'Profitable':'Interesting'}</small><b>${finite(c[k+'EarlierSuccess']?.pct)?pct(c[k+'EarlierSuccess'].pct):'N/A'}</b><em>${c[k+'EarlierSuccess']?.denominator?ratio(c[k+'EarlierSuccess']):'0/0'}</em></span>`).join('')}</span><span class="retention-deltas">${metrics.map(k=>{const d=overviewDelta(0,c[k].retentionDifferencePp,'retention');return `<span class="delta ${d.tone}">${d.text==='N/A'?'N/A':d.text}</span>`;}).join('')}</span>`;
    }
    const periods=data.meta.periods,vals=periods.map(p=>overviewValue(scope.periods[p.id],m,p));
    const d=overviewDelta(0,scope.changes[m],m);
    return `<span class="metric-values">${periods.map((p,i)=>{
      const block=scope.periods[p.id],v=vals[i];
      const f=['interesting','profitable'].includes(m)?block?.[m]:m==='positiveWithoutBest2'?block?.concentration?.own?.['2']?.positive:null;
      const detail=f?ratio(f):'n='+(block?.medians?.[m]?.n??0);
      return `<span class="metric-period" style="background:${cardColor(data,m,v)}"><small>${esc(p.label)}</small><b>${m==='tradesPerMonth'?num(v,0):pct(v,m==='annualizedProfitPct')}</b><em>${detail}</em></span>`;
    }).join('')}</span><span class="delta ${d.tone}">${d.text}</span>`;
  }
  function matrix(data,state){
    const m=state.metric,current=data.scopes[state.scope],title=matrixMetrics.find(x=>x[0]===m)[2];
    function cell(id,isTotal=false){
      const scope=data.scopes[id];if(!scope)return '<td class="missing">Not declared</td>';
      const isCell=!!data.cellmeta[id],member=isCell&&current.cells.length>1&&current.cells.length<Object.keys(data.cellmeta).length&&current.cells.includes(id);
      return `<td class="${isTotal?'matrix-total':''}"><button class="matrix-card${member?' member':''}${m==='retention'?' retention-card':''}" data-scope="${esc(id)}" data-matrix-scope="${esc(id)}" ${isCell?`data-cell="${esc(id)}"`:''} aria-pressed="${state.scope===id}" aria-label="${esc(scope.label)}; ${esc(title)}">${matrixValues(data,scope,m)}</button></td>`;
    }
    return `<table class="matrix-table"><thead><tr><th scope="col">Ticker / TF</th>${data.meta.timeframes.map(t=>`<th scope="col">${tf(t)}</th>`).join('')}<th scope="col" class="matrix-total">All timeframes</th></tr></thead><tbody>${data.meta.tickers.map(ticker=>`<tr><th scope="row">${esc(ticker)}</th>${data.meta.timeframes.map(t=>cell(Object.keys(data.cellmeta).find(c=>data.cellmeta[c].ticker===ticker&&data.cellmeta[c].tf===t))).join('')}${cell(data.meta.scopeIds.instrument[ticker],true)}</tr>`).join('')}<tr class="matrix-total-row"><th scope="row">All tickers</th>${data.meta.timeframes.map(t=>cell(data.meta.scopeIds.timeframe[String(t)])).join('')}${cell('all',true)}</tr></tbody></table>`;
  }
  function matrixDisplay(data,state){
    if(!state.compact)return `<div data-matrix-view="${state.metric}">${matrix(data,state)}</div>`;
    return `<div class="compact-grid">${compactGroups[state.compactGroup].map(metric=>{
      const title=matrixMetrics.find(m=>m[0]===metric)[2];
      return `<section class="compact-slot"><div class="compact-canvas" data-matrix-view="${metric}"><h3 class="compact-title">${title}${metric==='retention'?` &middot; ${esc(data.meta.periods[0].short)} &rarr; ${esc(data.meta.periods[1].short)}`:''}</h3>${matrix(data,{...state,metric})}${matrixLegend(data,metric)}</div></section>`;
    }).join('')}</div>`;
  }
  function matrixLegend(data,m){
    const title=matrixMetrics.find(x=>x[0]===m)[2];
    const lim=data.meta.overviewColors[m],lo=m==='annualizedProfitPct'?lim[0]:0,hi=lim.at(-1),unit=m==='tradesPerMonth'?' trades/mo':'%';
    const stops=[lo,...lim.filter(x=>x>lo&&x<hi),hi].filter((x,i,a)=>!i||x!==a[i-1]);
    const ramp=stops.map(v=>`${cardColor(data,m,v)} ${100*(v-lo)/(hi-lo)}%`).join(',');
    const explanation=m==='mtmMaxDdPct'?`Neutral through ${lim[0]}%; full red at ${hi}%.`:m==='annualizedProfitPct'?`Neutral at 0%; saturation at ${lo}% and +${hi}%.`:`Neutral ${lim[1]}–${lim[2]}${unit}; saturation at ${lim[0]} and ${hi}${unit}.`;
    return `<div class="matrix-legend"><strong>${title}</strong><span>${lo}${unit}</span><span class="matrix-ramp" style="background:linear-gradient(90deg,${ramp})"></span><span>${hi}${unit}</span><span>${explanation} Same scale as the overview; values are not clipped.</span>${['retention','interesting','profitable','positiveWithoutBest2'].includes(m)?'<span class="matrix-unavailable">N/A</span>':''}${m==='retention'?'<span>Left: Profitable &middot; Right: Interesting. Below: advantage over previously unsuccessful settings.</span>':''}</div>`;
  }
  const intervalText=v=>Number.isInteger(v)?BigInt(v).toLocaleString('en-US')+'.0':String(v);
  function histogram(d,metric,maxCount,period){
    if(!d.n)return '<p class="subtle">N/A · n=0</p>';
    const x0=28,w=290,h=108,top=16,unit=metric==='monthlyContribution'?' pp':'%';
    const x=v=>x0+(v-d.lo)/(d.hi-d.lo)*w;
    const bars=d.counts.map((n,j)=>{
      const i=d.indices[j],a=d.lo+i*d.step,b=d.lo+(i+1)*d.step,width=w/d.bins;
      return `<rect tabindex="0" role="img" x="${x(a)}" y="${top+h-n/Math.max(1,maxCount)*h}" width="${Math.max(.01,width*.92)}" height="${n/Math.max(1,maxCount)*h}" fill="${b<=0?'#bc8a83':'#639d8f'}" data-tip="${esc(period)}&#10;${intervalText(a)}${unit} to ${intervalText(b)}${unit}${i===d.bins-1?' (inclusive)':' (upper exclusive)'}&#10;${n} settings" aria-label="${n} settings in ${intervalText(a)} to ${intervalText(b)}${unit}"/>`;
    }).join('');
    return `<svg viewBox="0 0 340 160" aria-label="${esc(period)} distribution"><line x1="${x0}" x2="${x0+w}" y1="${top+h}" y2="${top+h}" stroke="#cdd7d1"/><text x="2" y="${top+4}">${maxCount}</text><text x="9" y="${top+h}">0</text>${bars}<line x1="${x(0)}" x2="${x(0)}" y1="${top}" y2="${top+h}" stroke="#6b7f75" stroke-dasharray="2 3"/><text x="${x0}" y="148">${intervalText(d.lo)}${unit}</text><text x="${x(0)}" y="148" text-anchor="middle">0${unit}</text><text x="${x0+w}" y="148" text-anchor="end">${intervalText(d.hi)}${unit}</text></svg>`;
  }
  function distributions(data,state){
    const s=data.scopes[state.scope],m=state.distribution,all=data.meta.periods.map(p=>s.periods[p.id].distributions[m]),max=Math.max(...all.flatMap(d=>d.counts));
    return '<div class="plots">'+data.meta.periods.map((p,i)=>{const d=all[i];return `<div><div class="plot-title"><strong>${esc(p.label)}</strong><small>n=${d.n}</small></div>${histogram(d,m,max,p.label)}<div class="quantiles"><span>Median <b>${pct(d.median,true)}</b></span><span>Middle 50%<br><b>${pct(d.q1,true)} to ${pct(d.q3,true)}</b></span></div></div>`;}).join('')+'</div><p class="note">Y = number of settings. Red bars are below zero; teal bars are at or above zero. Fixed bin edges across all scopes and both periods; no tails are clipped. Hover or focus a bar for its count.</p>';
  }
  function concentration(data,state){
    const s=data.scopes[state.scope];
    return `<table class="compare concentration-table"><thead><tr><th>Subtract contributions</th>${data.meta.periods.map(p=>`<th>${esc(p.label)}<small>Median residual · positive share</small></th>`).join('')}</tr></thead><tbody>${[['own','1','Each setting’s best month'],['own','2','Each setting’s best two'],['common','1','Same best month'],['common','2','Same best two']].map(([kind,n,label])=>`<tr><td>${label}</td>${data.meta.periods.map(p=>{const r=s.periods[p.id].concentration[kind][n];return `<td><b class="${tone(r.medianResidualPp.value)}">${pp(r.medianResidualPp.value)}</b>${ratio(r.positive)} · ${pct(r.positive.pct)}<small>n=${r.medianResidualPp.n}; missing ${r.missing}${r.selectedMonths?'<br>'+r.selectedMonths.map(esc).join(' + '):''}</small></td>`;}).join('')}</tr>`).join('')}</tbody></table><p class="note">Own: different months may be removed from each setting. Same: months with the largest equal-cell mean contribution within this exact scope, including losses; ties use chronological order. Negative settings remain in the denominator.</p>`;
  }
  function recent(data,state){
    const s=data.scopes[state.scope];
    const cells=w=>!w?'Not declared':`<b>${pct(w.medianReturnPct.value,true)}</b><small>Contribution ${pp(w.medianContributionPp.value)} · n=${w.medianContributionPp.n}<br>Positive ${ratio(w.profitable)} · ${pct(w.profitable.pct)}<br>Return available ${w.available}/${w.declared} · exact windows ${w.exactWindowAvailable}<br>Median trades ${num(w.medianTrades.value,1)} · n=${w.medianTrades.n}<br>${esc(w.reason||w.returnPct.reason||'')}</small>`;
    return `<table class="compare"><thead><tr><th>Continuous window</th>${data.meta.periods.map(p=>`<th>${esc(p.label)}</th>`).join('')}</tr></thead><tbody><tr><td>Latest three declared months</td>${data.meta.periods.map(p=>`<td><small>${esc(p.tail)}</small>${cells(s.periods[p.id].tail)}</td>`).join('')}</tr>${data.meta.segments.map(seg=>`<tr><td>${esc(seg.label)}</td>${data.meta.periods.map(p=>`<td>${cells(s.periods[p.id].segments[seg.key])}</td>`).join('')}</tr>`).join('')}<tr><td>New-only evidence<br>${esc(data.meta.previousReviewEndUtc||'No previous review cutoff')}</td>${data.meta.periods.map(p=>`<td>${cells(s.periods[p.id].newOnly)}</td>`).join('')}</tr></tbody></table><p class="note">Returns use window-opening equity; contributions use original period capital. Segments belong only to their declared period. No reset, annualization or new trade hurdle.</p>`;
  }
  function evidenceLabel(data){
    const m=data.meta;
    if(m.nativeImplementation?.legacyFixture===true)return 'Legacy analytical evidence; no native completion seal';
    if(m.nativeSchema==='pattern-lab.strategy-screen.v1'&&m.nativeImplementation)return data.synthetic?'Synthetic compact fixture; no native execution':'Native account evidence';
    return 'Evidence identity unavailable';
  }
  function definitions(data){
    const m=data.meta;
    return `<div class="definitions-grid"><section><h3>Experiment & execution</h3><dl><dt>Periods</dt><dd>${m.periods.map(p=>`${esc(p.label)}: ${esc(p.start)} to ${esc(p.end)} (UTC, end exclusive) · ${p.months} months`).join('<br>')}</dd><dt>Markets</dt><dd>${m.tickers.map(esc).join(' · ')}; ${m.timeframes.map(tf).join(' / ')}</dd><dt>Criteria</dt><dd>Annualized return ≥ ${num(m.criteria.annualizedProfitPct)}%; MTM DD ≤ ${num(m.criteria.mtmMaxDdPct)}%. Trade minima by timeframe: ${Object.entries(m.tradeMinima).map(([t,v])=>`${tf(Number(t))}: ${v}`).join(' · ')}</dd><dt>Execution and costs</dt><dd>Independent accounts in each period. Each normalized recipe below records its actual capital, policy, costs and sources; settings may differ. Funding features are not cashflows; slippage is omitted.</dd><dt>Evidence</dt><dd>${esc(m.nativeSchema)} · ${evidenceLabel(data)}<br>${esc(m.limitation)}</dd></dl>${Object.entries(m.recipes).map(([id,r])=>`<details><summary>${esc(id)}</summary><pre>${esc(JSON.stringify(r.recipe,null,2))}</pre></details>`).join('')}</section><section><h3>Metrics & interpretation</h3><p>Breadth uses all declared settings and gives cells equal weight. Supporting counts are pooled; available and missing observations remain distinct. Medians pool eligible settings; n is the available count.</p><p>Success retained uses matched classifiable pairs and separate earlier-success and nonsuccess denominators. Its footer copies the saved retention difference. These are descriptive fractions, not probabilities of future success.</p><p>Monthly contributions use original period capital; cumulative observations are exact from-start month closes. Latest means the last three declared months without skipping missing evidence. Own/common best-month subtraction is saved arithmetic attribution, not a rerun.</p><p>Whole return distributions include only completed full-coverage settings eligible for the selected metric. Individual return ranking uses per-field availability. Low activity is known independently of classifiability. Failure reasons overlap; unknown is not failure.</p><p>Card colors use shared scales: shares red 0%, neutral 40–60%, green 100%; annualized return red −20%, neutral 0%, green +20%; DD neutral through 10%, red 60%; trades/month red 0, neutral 10–20, green 40. N/A is gray. Colors do not change classification.</p><p>Settings are correlated. Group summaries are not portfolios. Linked independent periods are illustrations, not continuous executions or validated trading edges.</p></section></div>`;
  }
  const detail=globalThis.SSRDetails({esc,num,pct,pp,finite,tf,tone,cardColor,overviewDelta,distributions,concentration,recent,histogram});
  function views(data,state,keys=null){
    const s=data.scopes[state.scope];
    const builders={'overview':()=>overview(data),'scale':()=>state.compact?'':matrixLegend(data,state.metric),'matrix-grid':()=>matrixDisplay(data,state),'matrix-metrics':()=>matrixButtons(state),
      'matrix-selection':()=>`Selected: <strong>${esc(state.scope==='all'?'All configurations':s.label)}</strong>`,
      'scope-title':()=>esc(s.label),'scope-caption':()=>`${s.cells.length} constituent cells · ${s.periods.earlier.declared} settings per period · pooled-setting medians`,
      'scope-members':()=>s.cells.map(id=>`<button data-scope="${esc(id)}" aria-pressed="${state.scope===id}">${esc(data.scopes[id].label)}</button>`).join(''),
      'scope-overview':()=>overview(data,state.scope,false),'field-content':()=>detail.field(data,state),'persistence':()=>detail.persistence(data,state),'months':()=>detail.monthly(data,state),
      'month-detail':()=>detail.monthDetail(data,state),'concentration':()=>detail.concentrationView(data,state),'recent-tail':()=>detail.tail(data,state),'failures':()=>detail.failures(data,state),'definitions':()=>definitions(data)};
    return Object.fromEntries((keys||Object.keys(builders)).map(k=>[k,builders[k]() ]));
  }
  function transition(data,state,action){
    if(action.type==='overviewMetric'&&matrixMetrics.some(m=>m[0]===action.value)){
      if(state.compact){
        const group=compactGroups.findIndex(g=>g.includes(action.value));
        return state.compactGroup===group?state:{...state,compactGroup:group};
      }
      return state.metric===action.value?state:{...state,metric:action.value};
    }
    if(action.type==='compact')return {...state,compact:!!action.value,compactGroup:action.value?compactGroups.findIndex(g=>g.includes(state.metric)):state.compactGroup};
    if(action.type==='compactGroup'&&[0,1].includes(action.value))return {...state,compactGroup:action.value};
    if(action.type==='scope'&&data.scopes[action.value])return {...state,scope:action.value,month:null};
    if(action.type==='metric'&&matrixMetrics.some(m=>m[0]===action.value))return {...state,metric:action.value};
    if(action.type==='fieldView'&&['scatter','distribution'].includes(action.value))return {...state,fieldView:action.value};
    if(action.type==='fieldGroup'&&['all','interesting','profitable','loss','lowActivity'].includes(action.value)){
      const group=state.fieldGroup===action.value?'all':action.value;
      return state.fieldGroup===group?state:{...state,fieldGroup:group};
    }
    if(action.type==='sameFieldColors')return {...state,sameFieldColors:!!action.value};
    if(action.type==='showExtremes')return {...state,showExtremes:!!action.value};
    if(action.type==='monthMode'&&['cumulative','monthly'].includes(action.value))return {...state,monthMode:action.value};
    if(action.type==='distribution'&&['returnPct','annualizedProfitPct'].includes(action.value))return {...state,distribution:action.value};
    if(action.type==='month')return {...state,month:action.value};
    return state;
  }
  const initial=()=>({scope:'all',metric:'interesting',distribution:'annualizedProfitPct',fieldView:'scatter',fieldGroup:'all',sameFieldColors:true,showExtremes:false,monthMode:'cumulative',month:null,compact:true,compactGroup:0});
  const api={esc,num,pct,ratio,heat,value,views,transition,initial,overviewValue,cardColor,overviewDelta,matrixValues,matrixMetrics,fieldCohort:detail.fieldCohort,monthlyScale:detail.monthlyScale};
  if(typeof module!=='undefined')module.exports=api;
  globalThis.SSRScreen=api;
  if(typeof document==='undefined')return;
  const $=id=>document.getElementById(id),data=JSON.parse($('screen-data').textContent);let state=initial();
  const linkedChart=globalThis.SSRLinkedChart?.mount(data);
  document.title=data.meta.title;$('title').textContent=data.meta.title;$('subtitle').textContent=data.meta.subtitle;
  const cellCount=Object.keys(data.cellmeta).length,declared=data.scopes.all.periods[data.meta.periods[0].id].declared;
  const sizes=Object.keys(data.cellmeta).map(k=>data.scopes[k].periods.earlier.declared),min=Math.min(...sizes),max=Math.max(...sizes),settingsRange=min===max?String(min):`${min}–${max}`;
  $('study-counts').innerHTML=[[declared,'Configurations'],[settingsRange,'Settings per cell'],[cellCount,'Ticker / TF cells']].map(([v,label])=>`<div class="stat"><b>${typeof v==='string'?esc(v):num(v,0)}</b><span>${label}</span></div>`).join('');
  function sizeCompactMatrices(){
    for(const canvas of document.querySelectorAll('.compact-canvas')){
      const slot=canvas.parentElement,scale=slot.clientWidth/1448;
      canvas.style.transform=`scale(${scale})`;
      slot.style.height=Math.ceil(canvas.offsetHeight*scale)+'px';
    }
  }
  function render(keys=null){
    for(const [id,html] of Object.entries(views(data,state,keys)))if(!keys||keys.includes(id)){const target=$(id);if(target)target.innerHTML=html;}
    $('matrix').classList.toggle('is-compact',state.compact);$('compact-mode').checked=state.compact;
    if(!keys||keys.includes('matrix-grid'))sizeCompactMatrices();
    updateOverviewSelection();
    document.querySelectorAll('[data-field-view]').forEach(e=>e.setAttribute('aria-pressed',String(e.dataset.fieldView===state.fieldView)));
    document.querySelectorAll('[data-month-mode]').forEach(e=>e.setAttribute('aria-pressed',String(e.dataset.monthMode===state.monthMode)));
    $('show-extremes').checked=state.showExtremes;
    $('extremes-control').hidden=state.monthMode!=='cumulative';
  }
  function updateOverviewSelection(){
    const active=state.compact?compactGroups[state.compactGroup]:[state.metric];
    const cards=[...$('overview').querySelectorAll('[data-overview-metric]')];
    const indices=[];
    cards.forEach((card,i)=>{
      const selected=active.includes(card.dataset.overviewMetric);
      card.setAttribute('aria-pressed',String(selected));
      if(selected)indices.push(i+1);
    });
    const frame=$('overview').querySelector('.overview-selection');
    frame.style.gridColumn=`${indices[0]} / ${indices.at(-1)+1}`;
  }
  function updateMatrixSelection(){
    // Keep the focused nodes and measured compact-slot heights stable on selection.
    // Replacing the matrix DOM can collapse its slots and trigger scroll anchoring.
    const current=data.scopes[state.scope],showMembers=current.cells.length>1&&current.cells.length<cellCount;
    for(const button of $('matrix-grid').querySelectorAll('[data-matrix-scope]')){
      const id=button.dataset.matrixScope;
      button.setAttribute('aria-pressed',String(id===state.scope));
      button.classList.toggle('member',showMembers&&current.cells.includes(id));
    }
  }
  function dispatch(action){
    const next=transition(data,state,action);
    if(next===state)return;
    state=next;
    const parts={overviewMetric:['scale','matrix-grid','matrix-metrics'],compact:['scale','matrix-grid','matrix-metrics'],compactGroup:['scale','matrix-grid','matrix-metrics'],fieldView:['field-content'],fieldGroup:['field-content'],showExtremes:['months'],monthMode:['months','month-detail'],month:['months','month-detail'],distribution:['field-content'],metric:['scale','matrix-grid','matrix-metrics'],
      sameFieldColors:['field-content'],
      scope:['matrix-selection','scope-title','scope-caption','scope-members','scope-overview','field-content','persistence','months','month-detail','concentration','recent-tail','failures']};
    render(parts[action.type]);
    highlightedSetting=null;
    if(action.type==='scope'){updateMatrixSelection();linkedChart?.setScope(state.scope);}
    $('tooltip').hidden=true;
  }
  document.addEventListener('click',e=>{
    const fieldControl=e.target.closest('[data-field-view]');if(fieldControl){dispatch({type:'fieldView',value:fieldControl.dataset.fieldView});return;}
    const fieldGroup=e.target.closest('[data-field-group]');if(fieldGroup){const value=fieldGroup.dataset.fieldGroup;dispatch({type:'fieldGroup',value});document.querySelector(`[data-field-group="${value}"]`)?.focus({preventScroll:true});if(value==='all')document.querySelector('[data-field-view="scatter"]')?.focus({preventScroll:true});return;}
    const monthControl=e.target.closest('[data-month-mode]');if(monthControl){dispatch({type:'monthMode',value:monthControl.dataset.monthMode});return;}
    if(e.target.closest('[data-close-month]')){dispatch({type:'month',value:null});return;}
    const overviewCard=e.target.closest('[data-overview-metric]');
    if(overviewCard){dispatch({type:'overviewMetric',value:overviewCard.dataset.overviewMetric});return;}
    const group=e.target.closest('[data-matrix-group]');
    if(group){const v=Number(group.dataset.matrixGroup);dispatch({type:'compactGroup',value:v});document.querySelector(`[data-matrix-group="${v}"]`)?.focus({preventScroll:true});return;}
    const metric=e.target.closest('[data-matrix-metric]');
    if(metric){const v=metric.dataset.matrixMetric;dispatch({type:'metric',value:v});document.querySelector(`[data-matrix-metric="${v}"]`)?.focus({preventScroll:true});return;}
    const scope=e.target.closest('[data-scope]');
    if(scope){const attr=scope.hasAttribute('data-matrix-scope')?'data-matrix-scope':'data-scope',v=scope.dataset.scope,view=scope.closest('[data-matrix-view]')?.dataset.matrixView;dispatch({type:'scope',value:v});document.querySelector(`${view?`[data-matrix-view="${view}"] `:''}[${attr}="${CSS.escape(v)}"]`)?.focus({preventScroll:true});}
    const month=e.target.closest('[data-month]');
    if(month){dispatch({type:'month',value:month.dataset.month});document.querySelector(`[data-month="${month.dataset.month}"]`)?.focus({preventScroll:true});}
  });
  $('compact-mode').onchange=e=>dispatch({type:'compact',value:e.target.checked});
  new ResizeObserver(sizeCompactMatrices).observe($('matrix'));
  document.addEventListener('change',e=>{if(e.target.id==='same-field-colors'){dispatch({type:'sameFieldColors',value:e.target.checked});$('same-field-colors')?.focus({preventScroll:true});}if(e.target.id==='distribution-metric'){dispatch({type:'distribution',value:e.target.value});$('distribution-metric')?.focus({preventScroll:true});}if(e.target.id==='show-extremes')dispatch({type:'showExtremes',value:e.target.checked});});
  document.addEventListener('keydown',e=>{
    if(e.key==='Escape'){$('tooltip').hidden=true;return;}
    const overviewCard=e.target.closest('[data-overview-metric]');
    if(overviewCard&&(e.key==='Enter'||e.key===' ')&&!e.ctrlKey&&!e.altKey&&!e.metaKey){
      e.preventDefault();
      if(!e.repeat)overviewCard.click();
      return;
    }
    const month=e.target.closest('[data-month]');if(month&&(e.key==='Enter'||e.key===' ')){e.preventDefault();month.dispatchEvent(new MouseEvent('click',{bubbles:true}));return;}
    const cell=e.target.closest('[data-matrix-scope]');if(!cell||e.ctrlKey||e.altKey||e.metaKey)return;
    const direction={ArrowLeft:[0,-1],ArrowRight:[0,1],ArrowUp:[-1,0],ArrowDown:[1,0]}[e.key];
    if(direction){
      e.preventDefault();
      const table=cell.closest('table'),td=cell.closest('td'),[dr,dc]=direction;
      let row=td.parentElement.rowIndex+dr,column=td.cellIndex+dc;
      while(row>=0&&row<table.rows.length&&column>=0&&column<table.rows[row].cells.length){
        const next=table.rows[row].cells[column].querySelector('[data-matrix-scope]');
        if(next){next.focus({preventScroll:true});break;}
        row+=dr;column+=dc;
      }
    }
  });
  function tooltip(target,x,y){const tip=$('tooltip');tip.textContent=target.dataset.tip;tip.hidden=false;const w=tip.offsetWidth,h=tip.offsetHeight;tip.style.left=Math.max(5,Math.min(x+14+w>innerWidth?x-w-14:x+14,innerWidth-w-5))+'px';tip.style.top=Math.max(5,Math.min(y+12,innerHeight-h-5))+'px';}
  let highlightedSetting=null;
  function highlightSetting(target){
    const id=target?.closest?.('[data-setting]')?.dataset.setting;
    if((id||null)===highlightedSetting)return;
    highlightedSetting=id||null;
    document.querySelectorAll('[data-setting]').forEach(e=>e.classList.toggle('setting-highlight',!!id&&e.dataset.setting===id));
  }
  document.addEventListener('pointerover',e=>highlightSetting(e.target));
  document.addEventListener('pointerout',e=>{if(e.target.closest('[data-setting]'))highlightSetting(e.relatedTarget);});
  document.addEventListener('focusin',e=>highlightSetting(e.target));
  document.addEventListener('focusout',e=>{if(e.target.closest('[data-setting]'))highlightSetting(e.relatedTarget);});
  document.addEventListener('pointerover',e=>{const target=e.target.closest('[data-tip]');if(target)tooltip(target,e.clientX,e.clientY);else $('tooltip').hidden=true;});
  document.addEventListener('focusin',e=>{const target=e.target.closest('[data-tip]');if(target){const r=target.getBoundingClientRect();tooltip(target,r.x+r.width/2,r.y+r.height/2);}else $('tooltip').hidden=true;});
  document.addEventListener('scroll',()=>{$('tooltip').hidden=true;},{passive:true});
  $('export').onclick=()=>{const blob=new Blob([$('screen-data').textContent+'\n'],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='strategy-screen-displayed-data.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),0);};
  render();globalThis.screenDebug=()=>({state:{...state},scopeCount:Object.keys(data.scopes).length,ready:true});
})();
