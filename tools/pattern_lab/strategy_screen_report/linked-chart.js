/* Monthly fixed-setting paths. Separate tests are linked for display only. */
(()=>{
  'use strict';
  const finite=Number.isFinite, day=86400000;
  // Earlier-period colors use ticker/TF bounds, before category and extreme filters.
  function returnColor(value,domain={min:-100,max:100}){
    const neutral=[128,139,136],target=value<0?[213,57,55]:[13,157,84];
    const extent=value<0?Math.abs(domain.min):domain.max;
    const weight=finite(value)&&extent>0?Math.min(1,Math.abs(value)/extent):0;
    return `rgb(${neutral.map((v,i)=>Math.round(v+(target[i]-v)*weight)).join(',')})`;
  }
  const pct=v=>finite(v)?`${v>0?'+':''}${v.toFixed(1)}%`:'N/A';
  const date=t=>new Date(t).toISOString().slice(0,10);
  function series(data,scope,resetRecent=false){
    const periods=data.meta.periods,cells=new Set(data.scopes[scope].cells);
    return data.detail.settings.filter(s=>cells.has(s.cell)).map(s=>{
      const points=[];let base=1;
      periods.forEach((p,pi)=>{
        const values=s.periods[p.id]?.cumulativeReturnPct||[];
        const months=data.scopes.all.periods[p.id].monthly;
        if(true){
          if(pi&&points.length)points.at(-1).beforeReset=true;
          points.push({t:Date.parse(p.start),v:pi===0||resetRecent?0:finite(base)?100*(base-1):null,pid:p.id,month:null,breakBefore:pi>0});
        }
        months.forEach((m,i)=>{
          const [y,mo]=m.month.split('-').map(Number),v=values[i];
          points.push({t:Date.UTC(y,mo,1),v:resetRecent?(finite(v)?v:null):(finite(base)&&finite(v)?100*(base*(1+v/100)-1):null),pid:p.id,month:m.month});
        });
        const last=values[months.length-1];
        base=finite(base)&&months.length&&values.length===months.length&&finite(last)?base*(1+last/100):null;
      });
      const cell=data.cellmeta[s.cell],tf=cell.tf%1440===0?`${cell.tf/1440}D`:cell.tf%60===0?`${cell.tf/60}h`:`${cell.tf}m`;
      return {id:s.id,ticker:cell.ticker,tf:cell.tf,label:`${cell.ticker} · ${tf} · ${s.label}`,points,periods:s.periods,color:returnColor(s.periods[data.meta.periods[0].id]?.returnPct)};
    });
  }
  const initialFilters=()=>({tickers:[],timeframes:[],category:'all',min:false,max:false,basis:'earlier'});
  function colorDomain(lines,filters){
    let min=0,max=0,n=0;
    for(const line of lines){
      if(filters.tickers.length&&!filters.tickers.includes(line.ticker)||filters.timeframes.length&&!filters.timeframes.includes(line.tf))continue;
      const v=line.periods.earlier?.returnPct;if(!finite(v))continue;
      min=Math.min(min,v);max=Math.max(max,v);n++;
    }
    return {min,max,n};
  }
  function filtersFromScope(data,id,basis='earlier'){
    const cells=data.scopes[id].cells.map(id=>data.cellmeta[id]);
    const tickers=data.meta.tickers.filter(t=>cells.some(c=>c.ticker===t));
    const timeframes=data.meta.timeframes.filter(tf=>cells.some(c=>c.tf===tf));
    return {...initialFilters(),basis,tickers:tickers.length===data.meta.tickers.length?[]:tickers,timeframes:timeframes.length===data.meta.timeframes.length?[]:timeframes};
  }
  function matches(line,f,omit=null){
    const c=line.periods.earlier?.classification;
    const category=f.category==='all'||(f.category==='lowActivity'?c?.lowActivity===true:!!c?.valid&&(f.category==='interesting'?c.interesting:f.category==='profitable'?c.profitable&&!c.interesting:f.category==='loss'?!c.profitable:false));
    return category&&(omit==='tickers'||!f.tickers.length||f.tickers.includes(line.ticker))&&(omit==='timeframes'||!f.timeframes.length||f.timeframes.includes(line.tf));
  }
  function rankValue(line,basis){
    if(basis!=='linked')return line.periods[basis]?.returnPct;
    const a=line.periods.earlier?.cumulativeReturnPct,b=line.periods.recent?.cumulativeReturnPct;
    return a?.length&&b?.length&&finite(a.at(-1))&&finite(b.at(-1))?100*((1+a.at(-1)/100)*(1+b.at(-1)/100)-1):null;
  }
  function filterLines(lines,f){
    const eligible=lines.filter(l=>matches(l,f));if(!f.min&&!f.max)return eligible;
    let min=null,max=null,minValue=Infinity,maxValue=-Infinity;
    for(const line of eligible){const value=rankValue(line,f.basis);if(!finite(value))continue;
      if(value<minValue){min=line;minValue=value;}if(value>maxValue){max=line;maxValue=value;}}
    return eligible.filter(l=>f.min&&l===min||f.max&&l===max);
  }
  function clampView(view,bounds){
    const span=Math.min(bounds[1]-bounds[0],Math.max(60*day,view[1]-view[0]));
    const lo=Math.max(bounds[0],Math.min(bounds[1]-span,view[0]));return [lo,lo+span];
  }
  function scale(lines,view){
    let lo=0,hi=0;
    const add=v=>{if(finite(v)){lo=Math.min(lo,v);hi=Math.max(hi,v);}};
    for(const line of lines){
      line.points.forEach(p=>{if(p.t>=view[0]&&p.t<=view[1]&&!(p.beforeReset&&p.t===view[0]))add(p.v);});
      for(let i=1;i<line.points.length;i++){
        const a=line.points[i-1],b=line.points[i];if(b.breakBefore||!finite(a.v)||!finite(b.v))continue;
        for(const edge of view)if(a.t<edge&&b.t>edge)add(a.v+(b.v-a.v)*(edge-a.t)/(b.t-a.t));
      }
    }
    const pad=Math.max(1,(hi-lo)*.06);lo-=pad;hi+=pad;
    const raw=(hi-lo)/6,power=10**Math.floor(Math.log10(raw)),step=[1,2,5,10].find(n=>n*power>=raw)*power;
    lo=Math.floor(lo/step)*step;hi=Math.ceil(hi/step)*step;
    return {lo,hi,step};
  }
  function mount(data){
    const $=id=>document.getElementById(id),canvas=$('linked-overlay'),baseCanvas=$('linked-base');
    if(!canvas)return null;
    const box=canvas.parentElement,tip=$('linked-tip'),select=$('linked-select');
    const bounds=[Date.parse(data.meta.periods[0].start),Date.parse(data.meta.periods.at(-1).end)];
    let scope='all',allLines=[],lines=[],view=bounds.slice(),period='all',pinned=null,hover=null,pointer=null,drag=null;
    let resetRecent=true,filters=initialFilters(),colorBounds={min:0,max:0,n:0};
    let width=0,height=0,plot=null,buckets=[],pending=0,dirty=true,drawMs=0,hoverMs=0,overlayMs=0,refreshMs=0;
    const ctx=baseCanvas.getContext('2d'),overlay=canvas.getContext('2d');
    const periodBox=$('linked-periods');
    const toolbar=document.createElement('div');toolbar.className='linked-toolbar';
    const left=document.createElement('div');left.className='linked-mode-controls';
    const basisGroup=document.createElement('div');basisGroup.className='linked-segment';basisGroup.setAttribute('role','group');basisGroup.setAttribute('aria-label','Min / max ranked by return');
    const basisLabel=document.createElement('span');basisLabel.className='linked-control-label';basisLabel.textContent='Min / max by';left.append(basisLabel,basisGroup);
    const resetLabel=document.createElement('label');resetLabel.className='linked-reset-label';
    const resetInput=document.createElement('input');resetInput.type='checkbox';resetInput.id='linked-reset-year';resetInput.checked=true;
    resetLabel.append(resetInput,document.createTextNode(`Start ${data.meta.periods[1].short} at zero`));left.append(resetLabel);
    const right=document.createElement('div');right.className='linked-filter-controls';toolbar.append(left,right);periodBox.after(toolbar);
    const legend=document.createElement('div');legend.className='linked-color-legend';
    const legendTitle=document.createElement('span');legendTitle.textContent=`Curve color: ${data.meta.periods[0].short} return`;
    const ramp=document.createElement('span');ramp.className='linked-color-ramp';ramp.style.background=`linear-gradient(90deg,${returnColor(-100)},${returnColor(0)},${returnColor(100)})`;
    const low=document.createElement('span');low.textContent='−100%';const zero=document.createElement('span');zero.textContent='0%';const high=document.createElement('span');high.textContent='+100%';
    const key=document.createElement('div');key.className='linked-color-key';key.append(ramp,low,zero,high);
    const legendNote=document.createElement('span');legendNote.textContent='Selected tickers / timeframes · same color in both periods';legend.append(legendTitle,key,legendNote);$('linked-range').parentElement.after(legend);
    const controls=[];
    function button(parent,text,key,value){
      const b=document.createElement('button');b.textContent=text;b.dataset.linkedControl=key;b.dataset.value=String(value);
      b.onclick=()=>{
        if(key==='basis')filters.basis=value;
        else if(key==='category')filters.category=filters.category===value?'all':value;
        else if(key==='tickers'||key==='timeframes'){const a=filters[key],i=a.indexOf(value);if(i<0)a.push(value);else a.splice(i,1);}
        else if(key==='reset')filters={...initialFilters(),basis:filters.basis};
        else filters[key]=!filters[key];
        refresh();
      };
      parent.append(b);controls.push({b,key,value});return b;
    }
    for(const [key,label] of [['earlier',`${data.meta.periods[0].short} return`],['recent',`${data.meta.periods[1].short} return`],['linked','Linked return']])button(basisGroup,label,'basis',key);
    function group(label){const g=document.createElement('div');g.className='linked-filter-group';g.setAttribute('role','group');g.setAttribute('aria-label',label);right.append(g);return g;}
    const tickers=group('Ticker filters'),tfs=group('Timeframe filters');
    data.meta.tickers.forEach(t=>button(tickers,t,'tickers',t));
    data.meta.timeframes.forEach(tf=>button(tfs,tf%1440===0?`${tf/1440}D`:tf%60===0?`${tf/60}h`:`${tf}m`,'timeframes',tf));
    const categories=group(`Classification in ${data.meta.periods[0].short}`);
    for(const [key,label] of [['all','All'],['interesting','Interesting'],['profitable','Profitable, not interesting'],['loss','Not profitable'],['lowActivity',data.meta.activityLabel]])button(categories,label,'category',key);
    const extremes=group('Extreme configurations');button(extremes,'Min','min',true);button(extremes,'Max','max',true);button(extremes,'Reset filters','reset',true);
    resetInput.onchange=()=>{resetRecent=resetInput.checked;allLines=series(data,'all',resetRecent);refresh();};
    for(const [id,label,months] of [['all','All periods',null],...data.meta.periods.map(p=>[p.id,p.label,p.months])]){
      const b=document.createElement('button');b.dataset.linkedPeriod=id;b.textContent=label;
      b.setAttribute('aria-pressed',String(id===period));
      if(months){const small=document.createElement('small');small.textContent=`${months} months`;b.append(small);}
      b.onclick=()=>{period=id;view=id==='all'?bounds.slice():(()=>{const p=data.meta.periods.find(p=>p.id===id);return [Date.parse(p.start),Date.parse(p.end)];})();pointer=null;hover=null;schedule(true);};
      periodBox.append(b);
    }
    function schedule(full=false){dirty=dirty||full;if(!pending)pending=requestAnimationFrame(()=>{pending=0;if(dirty){drawBase();dirty=false;}drawOverlay();});}
    function resize(){
      width=box.clientWidth;height=box.clientHeight;const dpr=Math.min(devicePixelRatio||1,2);
      for(const c of [baseCanvas,canvas]){c.width=Math.round(width*dpr);c.height=Math.round(height*dpr);c.getContext('2d').setTransform(dpr,0,0,dpr,0,0);}
      schedule(true);
    }
    function path(context,points){
      context.beginPath();let connected=false;
      for(const p of points){if(!finite(p.v)){connected=false;continue;}const x=plot.x(p.t),y=plot.y(p.v);if(connected&&!p.breakBefore)context.lineTo(x,y);else context.moveTo(x,y);connected=true;}
      context.stroke();
    }
    function clip(context){context.beginPath();context.rect(plot.l,plot.t,plot.r-plot.l,plot.b-plot.t);context.clip();}
    function drawBase(){
      const started=performance.now();if(!width||!height)return;
      const s=scale(lines,view),l=24,r=width-80,t=50,b=height-44;
      plot={l,r,t,b,...s,x:v=>l+(v-view[0])/(view[1]-view[0])*(r-l),y:v=>b-(v-s.lo)/(s.hi-s.lo)*(b-t)};
      ctx.clearRect(0,0,width,height);ctx.font='12px "Segoe UI", Arial';ctx.textAlign='left';ctx.fillStyle='#596f6b';ctx.fillText(resetRecent?'Cumulative return % · each period starts at zero':'Linked cumulative return %',l,18);
      for(const [i,p] of data.meta.periods.entries()){
        const a=Math.max(l,plot.x(Date.parse(p.start))),z=Math.min(r,plot.x(Date.parse(p.end)));
        if(z<=a)continue;ctx.fillStyle=i%2?'#f3f7f4':'#fafcfb';ctx.fillRect(a,t,z-a,b-t);
        ctx.fillStyle='#59746a';ctx.fillText(p.short,a+8,t-12);
      }
      for(let tick=0;tick<20;tick++){
        const v=s.lo+tick*s.step;if(v>s.hi+s.step/100)break;
        const y=plot.y(v);ctx.strokeStyle=Math.abs(v)<s.step/100?'#a8bcb3':'#e3e9e5';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(l,y);ctx.lineTo(r,y);ctx.stroke();
        ctx.fillStyle='#62776e';ctx.fillText(`${Math.abs(v)<1e-8?0:Number(v.toFixed(2))}%`,r+10,y+4);
      }
      const begin=new Date(view[0]),months=(view[1]-view[0])/(30.44*day),stride=(r-l)/months>=52?1:3;
      for(let stamp=Date.UTC(begin.getUTCFullYear(),begin.getUTCMonth(),1);stamp<=view[1];){
        const d=new Date(stamp),mo=d.getUTCMonth();
        if(stamp>=view[0]&&(mo%stride===0)){
          const x=plot.x(stamp),year=mo===0;ctx.strokeStyle=year?'#c5d1ca':'#e7ece9';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(x,t);ctx.lineTo(x,b);ctx.stroke();
          ctx.fillStyle=year?'#234b40':'#62776e';ctx.textAlign='center';ctx.fillText(year?String(d.getUTCFullYear()):d.toLocaleDateString('en',{month:'short',timeZone:'UTC'}),x,b+26);
        }
        stamp=Date.UTC(d.getUTCFullYear(),mo+1,1);
      }
      ctx.textAlign='left';ctx.save();clip(ctx);ctx.lineWidth=1;
      ctx.globalAlpha=lines.length>1000?.30:lines.length>100?.40:.60;
      for(const line of lines){ctx.strokeStyle=line.color;path(ctx,line.points);}
      ctx.restore();
      // Horizontal bins restrict hit testing to nearby segments, even for large grids.
      buckets=Array.from({length:Math.ceil(width/40)},()=>[]);
      for(const line of lines)for(let i=1;i<line.points.length;i++){
        const p=line.points[i],a=line.points[i-1];
        if(p.breakBefore||!finite(a.v)||!finite(p.v)||p.t<view[0]||a.t>view[1])continue;
        const ax=plot.x(a.t),ay=plot.y(a.v),bx=plot.x(p.t),by=plot.y(p.v),segment={id:line.id,ax,ay,bx,by};
        for(let j=Math.max(0,Math.floor((ax-9)/40));j<=Math.min(buckets.length-1,Math.floor((bx+9)/40));j++)buckets[j].push(segment);
      }
      if(!lines.length){ctx.fillStyle='#596f6b';ctx.textAlign='center';ctx.fillText('No configurations match these filters',(l+r)/2,(t+b)/2);ctx.textAlign='left';}
      periodBox.querySelectorAll('button').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.linkedPeriod===period)));
      $('linked-range').textContent=`${date(view[0])} — ${date(view[1])} UTC`;
      drawMs=performance.now()-started;
    }
    function nearest(x,y){
      const start=performance.now();let best=null,dist=9*9;
      for(const {id,ax,ay,bx,by} of buckets[Math.floor(x/40)]||[]){
        if(x<Math.min(ax,bx)-9||x>Math.max(ax,bx)+9)continue;
        const dx=bx-ax,dy=by-ay,t=Math.max(0,Math.min(1,((x-ax)*dx+(y-ay)*dy)/(dx*dx+dy*dy||1)));
        const d=(x-ax-t*dx)**2+(y-ay-t*dy)**2;
        if(d<dist){best=id;dist=d;}
      }
      hoverMs=performance.now()-start;
      return best;
    }
    function drawOverlay(){
      const started=performance.now();
      overlay.clearRect(0,0,width,height);if(!plot)return;
      if(pointer&&!drag)hover=nearest(pointer.x,pointer.y);
      const active=lines.find(l=>l.id===(hover||pinned));
      $('linked-active').textContent=active?`${hover?'Preview':pinned?'Pinned':'Highlighted'} · ${active.label}`:'Hover near a curve to identify it · click to pin · ↑ / ↓ to browse';
      $('linked-active').style.color=active?active.color:'';
      $('linked-clear').disabled=!pinned&&!hover;
      select.value=pinned||'';
      overlay.save();clip(overlay);
      if(active){overlay.strokeStyle=active.color;overlay.lineWidth=2;path(overlay,active.points);}
      tip.hidden=true;
      if(pointer&&!drag){
        const time=view[0]+(pointer.x-plot.l)/(plot.r-plot.l)*(view[1]-view[0]);
        const times=(active||lines[0])?.points.filter(p=>p.t>=view[0]&&p.t<=view[1]&&(!resetRecent||(time<Date.parse(data.meta.periods[1].start)?p.pid===data.meta.periods[0].id:p.pid===data.meta.periods[1].id)))||[];
        const sample=times.reduce((a,p)=>!a||Math.abs(p.t-time)<Math.abs(a.t-time)?p:a,null);
        if(sample){
          const x=plot.x(sample.t);overlay.strokeStyle='#8fa99e';overlay.lineWidth=1;overlay.setLineDash([3,4]);overlay.beginPath();overlay.moveTo(x,plot.t);overlay.lineTo(x,plot.b);overlay.stroke();overlay.setLineDash([]);
          if(active){
            const p=active.points.find(p=>p.t===sample.t&&p.pid===sample.pid);
            if(p&&finite(p.v)){
              overlay.beginPath();overlay.arc(x,plot.y(p.v),4,0,Math.PI*2);overlay.fillStyle=active.color;overlay.fill();overlay.strokeStyle='#fff';overlay.stroke();
              tip.textContent=`${active.label}\n${p.month?`${p.month} · month close`:`${data.meta.periods.find(period=>period.id===p.pid).short} · period start`}\n${resetRecent?'Period cumulative return':'Linked cumulative return'}: ${pct(p.v)}\n\n${data.meta.periods.map(period=>`${period.label}: ${pct(active.periods[period.id]?.returnPct)}`).join('\n')}\nClick the curve to pin`;
              tip.hidden=false;const w=tip.offsetWidth,h=tip.offsetHeight;
              tip.style.left=Math.max(4,Math.min(pointer.x+18+w>width?pointer.x-w-18:pointer.x+18,width-w-4))+'px';tip.style.top=Math.max(4,Math.min(pointer.y+16,height-h-4))+'px';
            }
          }
        }
      }
      overlay.restore();
      overlayMs=performance.now()-started;
    }
    function local(e){const r=canvas.getBoundingClientRect();return {x:e.clientX-r.left,y:e.clientY-r.top};}
    function inside(p){return plot&&p.x>=plot.l&&p.x<=plot.r&&p.y>=plot.t&&p.y<=plot.b;}
    canvas.addEventListener('pointermove',e=>{
      const p=local(e);
      if(drag){
        if(Math.abs(p.x-drag.x)>4||Math.abs(p.y-drag.y)>4)drag.moved=true;
        if(drag.moved){const offset=(p.x-drag.x)/(plot.r-plot.l)*(drag.view[1]-drag.view[0]);view=clampView(drag.view.map(t=>t-offset),bounds);period='custom';hover=null;pointer=null;schedule(true);}return;
      }
      pointer=inside(p)?p:null;if(!pointer)hover=null;schedule();
    });
    canvas.addEventListener('pointerleave',()=>{if(!drag){pointer=null;hover=null;schedule();}});
    canvas.addEventListener('pointerdown',e=>{const p=local(e);if(e.button!==0||!inside(p))return;canvas.focus({preventScroll:true});drag={...p,view:view.slice(),moved:false};canvas.setPointerCapture(e.pointerId);});
    canvas.addEventListener('pointerup',e=>{
      if(!drag)return;const p=local(e),moved=drag.moved;drag=null;
      if(canvas.hasPointerCapture(e.pointerId))canvas.releasePointerCapture(e.pointerId);
      if(!moved&&inside(p)){const hit=nearest(p.x,p.y);pinned=hit===pinned?null:hit;}
      pointer=null;hover=null;schedule();
    });
    canvas.addEventListener('pointercancel',()=>{drag=null;pointer=null;hover=null;schedule();});
    canvas.addEventListener('wheel',e=>{
      const p=local(e);if(!inside(p))return;e.preventDefault();
      const ratio=(p.x-plot.l)/(plot.r-plot.l),anchor=view[0]+ratio*(view[1]-view[0]),span=(view[1]-view[0])*Math.exp(Math.max(-.5,Math.min(.5,e.deltaY*.0015)));
      view=clampView([anchor-ratio*span,anchor+(1-ratio)*span],bounds);period='custom';pointer=null;hover=null;schedule(true);
    },{passive:false});
    function reset(){view=bounds.slice();period='all';pointer=null;hover=null;schedule(true);}
    canvas.addEventListener('dblclick',reset);
    canvas.addEventListener('keydown',e=>{
      if(e.ctrlKey||e.altKey||e.metaKey)return;
      if(['ArrowUp','ArrowDown','ArrowLeft','ArrowRight','Home','Escape','+','-','='].includes(e.key))e.preventDefault();else return;
      pointer=null;hover=null;
      if(e.key==='Home')reset();
      else if(e.key==='Escape')pinned=null;
      else if(e.key==='ArrowUp'||e.key==='ArrowDown'){
        const i=lines.findIndex(l=>l.id===pinned),direction=e.key==='ArrowDown'?1:-1;
        if(lines.length)pinned=lines[(i<0?(direction>0?0:lines.length-1):(i+direction+lines.length)%lines.length)].id;
      }else{
        const span=view[1]-view[0],center=(view[0]+view[1])/2;
        if(e.key==='ArrowLeft'||e.key==='ArrowRight'){const d=span*.12*(e.key==='ArrowLeft'?-1:1);view=clampView(view.map(t=>t+d),bounds);}
        else {const half=span*(e.key==='-'?1.3:1/1.3)/2;view=clampView([center-half,center+half],bounds);}
        period='custom';schedule(true);
      }
      schedule();
    });
    select.onchange=()=>{pinned=select.value||null;pointer=null;hover=null;schedule();};
    $('linked-clear').onclick=()=>{pinned=null;pointer=null;hover=null;schedule();};
    function refresh(){
      const start=performance.now();colorBounds=colorDomain(allLines,filters);
      for(const line of allLines)line.color=returnColor(line.periods[data.meta.periods[0].id]?.returnPct,colorBounds);
      low.textContent=colorBounds.n?pct(colorBounds.min):'N/A';high.textContent=colorBounds.n?pct(colorBounds.max):'N/A';
      ramp.style.background=`linear-gradient(90deg,${returnColor(colorBounds.min,colorBounds)},${returnColor(0,colorBounds)},${returnColor(colorBounds.max,colorBounds)})`;
      lines=filterLines(allLines,filters);if(!lines.some(l=>l.id===pinned))pinned=null;hover=null;pointer=null;
      const tickerLabel=filters.tickers.length?filters.tickers.join(' + '):'All tickers';
      const tfLabel=filters.timeframes.length?filters.timeframes.map(tf=>tf%1440===0?`${tf/1440}D`:tf%60===0?`${tf/60}h`:`${tf}m`).join(' + '):'All timeframes';
      const categoryLabel={all:'All categories',interesting:'Interesting',profitable:'Profitable, not interesting',loss:'Not profitable',lowActivity:data.meta.activityLabel}[filters.category];
      const basisLabel=filters.basis==='linked'?'linked return':`${data.meta.periods.find(p=>p.id===filters.basis).short} return`;
      const extremeLabel=filters.min||filters.max?` · ${[filters.min?'Min':'',filters.max?'Max':''].filter(Boolean).join(' + ')} by ${basisLabel}`:'';
      $('linked-scope').textContent=`${tickerLabel} · ${tfLabel} · ${categoryLabel} in ${data.meta.periods[0].short}${extremeLabel}`;
      const available=lines.filter(l=>l.points.some(p=>p.month&&finite(p.v))).length;
      $('linked-count').textContent=`${available} shown / ${lines.length} selected / ${allLines.length} configurations · ${lines.length-available} unavailable`;
      select.replaceChildren(new Option('Choose a configuration…',''),...lines.map(l=>new Option(l.label,l.id)));
      for(const {b,key,value} of controls){
        const active=key==='basis'?filters.basis===value:key==='category'?filters.category===value:key==='tickers'||key==='timeframes'?filters[key].includes(value):key==='reset'?false:filters[key];
        b.setAttribute('aria-pressed',String(active));b.disabled=false;
        if(key==='tickers'||key==='timeframes')b.disabled=!active&&!allLines.some(l=>matches(l,filters,key)&&(key==='tickers'?l.ticker:l.tf)===value);
        if(key==='category'&&value==='all')b.hidden=filters.category==='all';
        if(key==='reset')b.disabled=!filters.tickers.length&&!filters.timeframes.length&&filters.category==='all'&&!filters.min&&!filters.max;
      }
      $('linked-note').textContent=resetRecent?'Each line follows one fixed configuration. Monthly closing equity; no intramonth path. Both tests start independently at zero return, with no line connecting the period boundary. Categories use the first period; Min / Max use the chosen return basis, regardless of the visible date range.':'Each line follows one fixed configuration. Monthly closing equity; no intramonth path. Period returns are linked multiplicatively. The underlying tests started separately, with no positions carried between them: this is a linked illustration, not a continuous backtest or a portfolio. Categories use the first period; Min / Max use the chosen return basis.';
      refreshMs=performance.now()-start;
      schedule(true);
    }
    function setScope(id){scope=id;filters=filtersFromScope(data,id,filters.basis);if(!allLines.length)allLines=series(data,'all',resetRecent);refresh();}
    new ResizeObserver(resize).observe(box);setScope(scope);
    globalThis.linkedChartDebug=()=>({scope,count:lines.length,total:allLines.length,resetRecent,colorBounds:{...colorBounds},filters:structuredClone(filters),ids:lines.map(l=>l.id),period,view:view.slice(),pinned,hover,drawMs,hoverMs,overlayMs,refreshMs,plot:plot?{l:plot.l,r:plot.r,t:plot.t,b:plot.b,lo:plot.lo,hi:plot.hi}:null,
      first:lines[0]?{id:lines[0].id,label:lines[0].label,color:lines[0].color,points:lines[0].points}:null});
    return {setScope};
  }
  const api={series,scale,clampView,mount,initialFilters,matches,filterLines,rankValue,returnColor,colorDomain,filtersFromScope};globalThis.SSRLinkedChart=api;
  if(typeof module!=='undefined')module.exports=api;
})();
