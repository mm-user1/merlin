/* Selected-scope views: saved facts only; no execution or new classifications. */
globalThis.SSRDetails = function(h){
  const {esc,num,pct,pp,finite,tf,tone,cardColor,overviewDelta,distributions,concentration,recent,histogram}=h;
  const periodNames=data=>data.meta.periods.map(p=>`<span>${esc(p.label)}</span>`).join('');
  const fraction=f=>`<strong>${finite(f?.pct)?pct(f.pct):'N/A'}</strong><small>${f?.success??0} / ${f?.denominator??0}</small>`;
  const categories=[['all','All'],['interesting','Interesting'],['profitable','Profitable, not interesting'],['loss','Not profitable']];
  const colors={interesting:'#176a62',profitable:'#75a99b',loss:'#b4493f'};
  const category=p=>!p?.classification?.valid?null:p.classification.interesting?'interesting':p.classification.profitable?'profitable':'loss';
  const plotted=p=>!!p?.classification?.valid&&finite(p.annualizedProfitPct)&&finite(p.mtmMaxDdPct);
  function fieldLimits(data){
    const facts=data.detail.settings.flatMap(s=>Object.values(s.periods)).filter(plotted),c=data.meta.criteria;
    const low=Math.min(0,c.annualizedProfitPct,...facts.map(p=>p.annualizedProfitPct)),
      high=Math.max(0,c.annualizedProfitPct,...facts.map(p=>p.annualizedProfitPct)),
      right=Math.max(10,c.mtmMaxDdPct,...facts.map(p=>p.mtmMaxDdPct));
    const rounded=(v,up)=>{const r=(up?Math.ceil(v/10):Math.floor(v/10))*10;return finite(r)?(up?Math.max(r,v):Math.min(r,v)):v;};
    return {xmax:rounded(right,true),lo:rounded(low,false),hi:high===low?10:rounded(high,true)};
  }
  function fieldCohort(data,state){
    const earlier=data.meta.periods[0],scope=data.scopes[state.scope],group=state.fieldGroup||'all';
    return data.detail.settings.filter(s=>{
      if(!scope.cells.includes(s.cell))return false;
      const p=s.periods[earlier.id];
      return group==='all'||(group==='lowActivity'?p?.classification?.lowActivity===true:category(p)===group);
    });
  }
  function bar(data,f,metric='profitable'){
    return `<div class="scope-bar-value">${fraction(f)}<span class="scope-bar"><i style="width:${finite(f?.pct)?f.pct:0}%;background:#639d8f"></i></span></div>`;
  }
  function persistence(data,state){
    const c=data.scopes[state.scope].persistence;
    return `<table class="compare persistence-table"><thead><tr><th>Success criterion</th><th>Successful again<small>Among earlier successful settings</small></th><th>Successful among the rest<small>Among earlier non-successful settings</small></th><th>Advantage<small>First share minus second</small></th></tr></thead><tbody>${['profitable','interesting'].map(k=>{
      const a=c[k+'EarlierSuccess'],b=c[k+'EarlierNotSuccess'],d=overviewDelta(0,c[k].retentionDifferencePp,'retention');
      return `<tr><td>${k==='profitable'?'Profitable':'Interesting'}</td><td style="background:${cardColor(data,'retention',a.pct)}">${bar(data,a,'retention')}</td><td style="background:${cardColor(data,'retention',b.pct)}">${bar(data,b,'retention')}</td><td class="${d.tone}">${d.text==='Unavailable'?'N/A':d.text}</td></tr>`;
    }).join('')}</tbody></table><p class="note">${c.classifiablePairs} classifiable matched settings · ${c.unavailablePairs} unavailable · ${c.unmatched} unmatched. Each bar uses its own earlier group as the denominator. Earlier non-success means not profitable / not interesting, respectively.</p>`;
  }
  function field(data,state){
    if(state.fieldView==='distribution')return `<div class="distribution-control"><label>Distribution metric <select id="distribution-metric"><option value="annualizedProfitPct" ${state.distribution==='annualizedProfitPct'?'selected':''}>Annualized return</option><option value="returnPct" ${state.distribution==='returnPct'?'selected':''}>Actual return</option></select></label></div>${distributions(data,state)}`;
    const scope=data.scopes[state.scope],earlier=data.meta.periods[0],group=state.fieldGroup||'all';
    const selected=fieldCohort(data,state);
    const c=data.meta.criteria,{xmax,lo,hi}=fieldLimits(data);
    const magnitude=Math.max(Math.abs(lo),Math.abs(hi),1),span=hi/magnitude-lo/magnitude;
    const W=760,H=365,L=60,R=20,T=24,B=48,x=v=>L+v/xmax*(W-L-R),y=v=>T+(hi/magnitude-v/magnitude)/span*(H-T-B);
    const colors={interesting:'#176a62',profitable:'#75a99b',loss:'#b4493f'};
    const filters=[...categories.slice(1),['lowActivity',data.meta.activityLabel]];
    const controls=`<div class="field-key field-filters" role="group" aria-label="Settings group in the earlier period"><span title="${esc(`${earlier.label}: ${earlier.start} to ${earlier.end} (UTC, end exclusive)`)}">Group in ${esc(earlier.short)}</span>${filters.map(([key,label])=>`<button data-field-group="${key}" aria-pressed="${group===key}"><i ${key==='lowActivity'?'class="hollow"':`style="background:${colors[key]}"`}></i>${label}</button>`).join('')}${group!=='all'?'<button data-field-group="all">All</button>':''}<label class="field-color-toggle"><input type="checkbox" id="same-field-colors" ${state.sameFieldColors?'checked':''}> Use ${esc(earlier.short)} colors</label></div>`;
    return `${controls}<div class="scope-plots">${data.meta.periods.map(per=>{
      const rows=selected.filter(s=>plotted(s.periods[per.id]));
      const axes=Array.from({length:6},(_,i)=>{
        const f=i/5,xv=xmax*f,yv=lo*(1-f)+hi*f;
        return `<line class="plot-grid" x1="${x(xv)}" x2="${x(xv)}" y1="${T}" y2="${H-B}"/><text x="${x(xv)}" y="${H-B+20}" text-anchor="middle">${num(xv,0)}%</text><line class="plot-grid" x1="${L}" x2="${W-R}" y1="${y(yv)}" y2="${y(yv)}"/><text x="${L-9}" y="${y(yv)+4}" text-anchor="end">${num(yv,0)}%</text>`;
      }).join('');
      const points=rows.map(s=>{
        const p=s.periods[per.id],colorPeriod=state.sameFieldColors?s.periods[earlier.id]:p,color=colors[category(colorPeriod)]||'#808b88';
        const label=`${data.scopes[s.cell].label} · ${s.label}`,other=data.meta.periods.find(q=>q.id!==per.id),op=s.periods[other.id];
        const metric=(q,k,format)=>q?.availability?.[k]?.available?format(q[k]):`N/A (${q?.availability?.[k]?.reason||'noAvailableValue'})`;
        const account=q=>`Ann. return ${metric(q,'annualizedProfitPct',v=>pct(v,true))} · MTM DD ${metric(q,'mtmMaxDdPct',pct)} · ${metric(q,'trades',v=>num(v,0))} trades · Coverage ${q?.coverageComplete?'complete':'incomplete'} · Trade minimum ${num(q?.effectiveCriteria?.minClosedTrades,0)}`;
        const tip=`${label}\n${per.label}: ${account(p)}\n${other.label}: ${account(op)}`;
        return `<circle class="setting-point" tabindex="0" role="img" data-setting="${esc(s.id)}" data-tip="${esc(tip)}" aria-label="${esc(tip)}" cx="${x(p.mtmMaxDdPct)}" cy="${y(p.annualizedProfitPct)}" r="4.5" fill="${p.classification.lowActivity===true?'white':color}" stroke="${color}" stroke-width="1.5"/>`;
      }).join('');
      return `<div><div class="plot-title"><strong>${esc(per.label)}</strong><small tabindex="0" data-tip="${esc(`${rows.length} plotted; ${selected.length-rows.length} unavailable within earlier cohort; ${scope.periods[per.id].declared-selected.length} outside earlier cohort`)}" aria-label="${esc(`${rows.length} / ${scope.periods[per.id].declared} shown. ${rows.length} plotted; ${selected.length-rows.length} unavailable within earlier cohort; ${scope.periods[per.id].declared-selected.length} outside earlier cohort`)}">${rows.length} / ${scope.periods[per.id].declared} shown</small></div><svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(per.label)}: annualized return versus MTM drawdown"><rect x="${L}" y="${T}" width="${x(c.mtmMaxDdPct)-L}" height="${y(c.annualizedProfitPct)-T}" fill="#edf5f1"/>${axes}<line class="plot-zero" x1="${L}" x2="${W-R}" y1="${y(0)}" y2="${y(0)}"/><path d="M${L},${y(c.annualizedProfitPct)}H${x(c.mtmMaxDdPct)}V${T}" fill="none" stroke="#7da69b" stroke-dasharray="4 4"/><text x="${L+8}" y="${T+14}">Return ≥ ${c.annualizedProfitPct}% · DD ≤ ${c.mtmMaxDdPct}%</text>${points}${!rows.length?`<text x="${W/2}" y="${H/2}" text-anchor="middle">${selected.length?'No available results in this period':'No settings in this earlier-period group'}</text>`:''}<text x="${W/2}" y="${H-5}" text-anchor="middle">MTM drawdown %</text><text x="${L}" y="14">Annualized return %</text></svg></div>`;
    }).join('')}</div><p class="note">${group!=='all'?`Group membership is fixed by ${esc(earlier.label)} results; the same settings remain selected in both periods. `:''}Each point is one setting. Hollow markers indicate low activity in the corresponding period. Hover or focus a point to highlight the same setting in both periods. Identical axes across periods and scopes; no values are clipped. The shaded region covers return and DD criteria; Interesting also requires the saved per-timeframe trade minimum.</p>`;
  }
  function runs(points,valid){
    const result=[];let run=[];
    for(const p of points){if(valid(p))run.push(p);else if(run.length){result.push(run);run=[];}}
    if(run.length)result.push(run);return result;
  }
  function monthlyScale(values){
    const available=values.filter(finite),min=Math.min(0,...available),max=Math.max(0,...available);
    const raw=(max-min||2)/5,power=10**Math.floor(Math.log10(raw)),unit=raw/power;
    const step=(unit<=1?1:unit<=2?2:unit<=5?5:10)*power;
    const lo=Math.floor(min/step)*step,hi=max===min?lo+step*4:Math.ceil(max/step)*step;
    const ticks=Array.from({length:Math.round((hi-lo)/step)+1},(_,i)=>lo+i*step);
    return {lo,hi,step,ticks,digits:Math.max(0,-Math.floor(Math.log10(step)))};
  }
  function monthly(data,state){
    const scope=data.scopes[state.scope],cum=state.monthMode!=='monthly',periods=data.meta.periods;
    const values=periods.flatMap(p=>data.detail.monthly[state.scope][p.id].flatMap((m,i)=>cum?[m.cumulative.q1,m.cumulative.q3,...(state.showExtremes?[m.cumulative.min,m.cumulative.max]:[])]:[scope.periods[p.id].monthly[i].median])).filter(finite);
    const {lo,hi,ticks,digits}=monthlyScale(values);
    const W=760,H=305,L=60,R=20,T=22,B=42,y=v=>T+(hi-v)/(hi-lo)*(H-T-B);
    return `<div class="scope-plots">${periods.map(per=>{
      const block=scope.periods[per.id],extra=data.detail.monthly[state.scope][per.id],N=extra.length;
      if(!N)return `<p>${esc(per.label)} · N/A</p>`;
      const step=(W-L-R)/N,x=i=>L+i*step,px=i=>x(i);
      const grid=ticks.map(v=>`<line class="plot-grid" x1="${L}" x2="${W-R}" y1="${y(v)}" y2="${y(v)}"/><text x="${L-8}" y="${y(v)+4}" text-anchor="end">${num(v,digits)}${cum?'%':' pp'}</text>`).join('');
      const points=[{i:0,median:0,q1:0,q3:0,min:0,max:0},...extra.map((m,i)=>({i:i+1,...m.cumulative}))];
      const segments=cum?runs(points,p=>finite(p.median)&&finite(p.q1)&&finite(p.q3)):[];
      const bands=segments.map(run=>`<path class="cumulative-band" d="M${run.map(p=>`${px(p.i)},${y(p.q1)}`).join('L')}L${[...run].reverse().map(p=>`${px(p.i)},${y(p.q3)}`).join('L')}Z" fill="#dcebe5"/>`).join('');
      const extremes=cum&&state.showExtremes?['min','max'].map(key=>runs(points,p=>finite(p[key])).map(run=>`<path class="cumulative-extreme" data-boundary="${key}" d="M${run.map(p=>`${px(p.i)},${y(p[key])}`).join('L')}"/>`).join('')).join(''):'';
      const curves=cum?segments.map(run=>`<path class="cumulative-line" d="M${run.map(p=>`${px(p.i)},${y(p.median)}`).join('L')}" fill="none" stroke="#176a62" stroke-width="2"/>${run.filter(p=>p.i>0).map(p=>`<circle cx="${px(p.i)}" cy="${y(p.median)}" r="3" fill="#176a62"/>`).join('')}`).join(''):block.monthly.map((m,i)=>finite(m.median)?`<rect x="${x(i)+step*.22}" width="${step*.56}" y="${y(Math.max(0,m.median))}" height="${Math.max(1,Math.abs(y(m.median)-y(0)))}" fill="${m.median>=0?'#639d8f':'#bc8a83'}"/>`:'').join('');
      const months=extra.map((m,i)=>{
        const v=block.monthly[i],tip=`${per.label} · ${m.month}\nCumulative return since period start\nMedian: ${pct(m.cumulative.median,true)}\nMiddle 50%: ${pct(m.cumulative.q1,true)} to ${pct(m.cumulative.q3,true)}\nMinimum / maximum: ${pct(m.cumulative.min,true)} / ${pct(m.cumulative.max,true)}\nAvailable: ${m.cumulative.n}/${block.declared}\n\nThis month\nMedian contribution: ${pp(v.median)}\nProfitable: ${pct(v.positive.pct)} (${v.positive.success}/${v.positive.denominator})\nMiddle 50% of contributions: ${pp(v.q1)} to ${pp(v.q3)}\nAvailable: ${v.n}/${block.declared}`;
        return `<text x="${x(i+.5)}" y="${H-B+20}" text-anchor="middle">${new Date(m.month+'-01T00:00:00Z').toLocaleDateString('en-US',{month:'short',year:'2-digit',timeZone:'UTC'})}</text><rect class="month-hit${state.month===m.month?' selected':''}" data-month="${m.month}" data-tip="${esc(tip)}" tabindex="0" role="button" aria-label="${esc(tip)}. Show distribution" aria-pressed="${state.month===m.month}" x="${x(i)}" y="${T}" width="${step}" height="${H-T-B}"/>`;
      }).join('');
      const tailStart=block.monthly.findIndex(m=>block.tailMonths.includes(m.month));
      return `<div><div class="plot-title"><strong>${esc(per.label)}</strong><small>${cum?`Median cumulative return · middle 50% band${state.showExtremes?' · min / max':''}`:'Median monthly contribution'}</small></div><svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(per.label)} monthly performance">${tailStart>=0?`<rect x="${x(tailStart)}" y="${T}" width="${step*(N-tailStart)}" height="${H-T-B}" fill="#f6f4ed"/><text x="${x(tailStart)+5}" y="${T+13}">Latest 3 declared months</text>`:''}${bands}${grid}<line class="plot-zero" x1="${L}" x2="${W-R}" y1="${y(0)}" y2="${y(0)}"/>${extremes}${curves}${months}</svg></div>`;
    }).join('')}</div><p class="note">${cum?'Month-end observations; each period starts at 0%. The line is the median of settings’ cumulative returns, not the sum of monthly medians or the equity of one setting. It does not show intramonth drawdown.':'Monthly contribution uses original period capital, in percentage points. Positive and negative bars show the median across settings; months are not independent reruns.'} Both periods share one scale. Hover for monthly facts; click a month for its distribution.</p>`;
  }
  function monthDetail(data,state){
    if(!state.month)return '<p class="subtle">Click a month above to inspect the distribution across its settings.</p>';
    const per=data.meta.periods.find(p=>data.detail.monthly[state.scope][p.id].some(m=>m.month===state.month));
    const m=per&&data.detail.monthly[state.scope][per.id].find(m=>m.month===state.month);
    if(!m)return '<p class="subtle">No monthly observations available.</p>';
    const d=m.distribution;
    return `<div class="month-distribution"><div><strong>${esc(state.month)} · ${esc(per.label)}</strong><p>Monthly contribution distribution</p><p class="subtle">${d.n} observations · median ${pp(d.median)}<br>Middle 50%: ${pp(d.q1)} to ${pp(d.q3)}</p><button class="text-button" data-close-month>Close distribution</button></div><div>${histogram(d,'monthlyContribution',Math.max(0,...d.counts),state.month)}<p class="note">X: monthly contribution (pp) · Y: settings. Same bins for every scope and month.</p></div></div>`;
  }
  function tail(data,state){
    const scope=data.scopes[state.scope];
    return `<div class="tail-cards">${data.meta.periods.map(per=>{
      const t=scope.periods[per.id].tail;
      return `<div class="tail-card"><strong>${esc(per.label)} · ${esc(per.tail)}</strong><div><span>Median return<b class="${tone(t.medianReturnPct.value)}">${pct(t.medianReturnPct.value,true)}</b></span><span>Profitable settings<b>${pct(t.profitable.pct)}</b><small>${t.profitable.success} / ${t.profitable.denominator}</small></span></div><small>${t.available}/${t.declared} available · return from each setting's window-opening equity</small></div>`;
    }).join('')}</div><details class="scope-extra"><summary>Continuous segments & calculation details</summary>${recent(data,state)}</details>`;
  }
  function concentrationView(data,state){
    const scope=data.scopes[state.scope];
    return `<div class="paired-bar-head"><span></span>${periodNames(data)}</div>${[['all','Full period'],['1','Without own best month'],['2','Without own best 2 months']].map(([key,label])=>`<div class="paired-bar-row"><strong>${label}</strong>${data.meta.periods.map(per=>{const b=scope.periods[per.id];return bar(data,key==='all'?b.profitable:b.concentration.own[key].positive);}).join('')}</div>`).join('')}<p class="note">Share still profitable. Best months are removed separately for each setting; these arithmetic residuals are not reruns or annualized returns.</p><details class="scope-extra"><summary>Residual values & common best months</summary>${concentration(data,state)}</details>`;
  }
  function failures(data,state){
    const scope=data.scopes[state.scope],c=data.meta.criteria;
    const rows=[['return',`Ann. return below ${c.annualizedProfitPct}%`],['drawdown',`MTM DD above ${c.mtmMaxDdPct}%`],['activity',data.meta.activityLabel]];
    return `<div class="paired-bar-head"><span></span>${periodNames(data)}</div>${rows.map(([k,label])=>`<div class="paired-bar-row"><strong>${label}</strong>${data.meta.periods.map(per=>{const f=scope.periods[per.id].failures,v=f.declared?100*f[k]/f.declared:null;return `<div class="scope-bar-value">${fraction({pct:v,success:f[k],denominator:f.declared})}<span class="scope-bar"><i class="failure-fill" style="width:${v??0}%"></i></span></div>`;}).join('')}</div>`).join('')}<p class="note">Reasons overlap: one setting can fail several criteria. Low activity is limited evidence, not necessarily a loss. Unavailable results are excluded from failure counts: ${data.meta.periods.map(p=>`${esc(p.label)} ${scope.periods[p.id].failures.unavailable}/${scope.periods[p.id].declared}`).join(' · ')}.</p>`;
  }
  return {field,persistence,monthly,monthDetail,tail,concentrationView,failures,fieldCohort,monthlyScale};
};
