/* Actual embedded UI/core under a small id-driven DOM shim. No geometry/paint claims. */
'use strict';
const fs=require('node:fs'),vm=require('node:vm'),zlib=require('node:zlib'),assert=require('node:assert/strict');
if(!globalThis.crypto?.subtle)Object.defineProperty(globalThis,'crypto',{value:require('node:crypto').webcrypto,configurable:true});
const html=fs.readFileSync(process.argv[2],'utf8');
const bootstrap=html.match(/<script type="application\/json" id="bootstrap">(.*?)<\/script>/s)[1];
const manifest=JSON.parse(bootstrap);
assert.equal(manifest.metadata.synthetic,true,'Only synthetic fixtures belong in this UI harness');
const blocks=[...html.matchAll(/<script type="application\/octet-stream" id="block-\d+">(.*?)<\/script>/g)].map(m=>m[1]);
const unesc=s=>s.replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&');
const els=new Map(),listeners={},rows=new Map();let cards=null;
function element(id){
  const attrs={},classes=new Set();let contents='';
  const e={id,textContent:'',hidden:false,value:'',checked:false,disabled:false,dataset:{},style:{},
    scrollTop:0,clientWidth:1000,clientHeight:600,width:1000,height:600,offsetWidth:100,offsetHeight:20,
    classList:{toggle(k,v){if(v===undefined)v=!classes.has(k);v?classes.add(k):classes.delete(k);},add(...ks){ks.forEach(k=>classes.add(k));},remove(...ks){ks.forEach(k=>classes.delete(k));},contains:k=>classes.has(k)},
    setAttribute(k,v){attrs[k]=String(v);},getAttribute:k=>attrs[k]??null,removeAttribute(k){delete attrs[k];},
    toggleAttribute(){},querySelector(s){return ['strong','p'].includes(s)?$(id+' '+s):null;},querySelectorAll(){return [];},
    closest(){return null;},matches(){return false;},contains(){return false;},
    focus(){document.activeElement=this;},blur(){document.activeElement=null;},scrollIntoView(){},
    addEventListener(type,f){(this.events??={})[type]=f;},showModal(){this.open=true;},close(){this.open=false;this.events?.close?.();},
    setPointerCapture(){},hasPointerCapture(){return false;},releasePointerCapture(){},
    getBoundingClientRect(){return {x:0,y:0,left:0,top:0,right:1000,bottom:600,width:1000,height:600};},
    getContext(){return new Proxy({}, {get:(t,k)=>k==='measureText'?()=>({width:10}):k in t?t[k]:()=>{},set:(t,k,v)=>{t[k]=v;return true;}});},
    click(){if(this.disabled)return;if(this.id==='price'){this.checked=!this.checked;this.onchange?.({target:this});}else this.onclick?.({target:this});}
  };
  Object.defineProperty(e,'innerHTML',{get:()=>contents,set(value){contents=value;if(id==='table-body')parseRows(value);}});
  e.parentElement=e;return e;
}
const $=id=>{if(id==='bootstrap')return {textContent:bootstrap};const m=/^block-(\d+)$/.exec(id);if(m)return {textContent:blocks[+m[1]]};if(!els.has(id))els.set(id,element(id));return els.get(id);};
function parseRows(text){
  rows.clear();
  for(const m of text.matchAll(/<tr data-account="([^"]+)" class="([^"]*)"[^>]*>(.*?)<\/tr>/gs)){
    const id=unesc(m[1]),r=element('row');r.dataset.account=id;m[2].split(' ').filter(Boolean).forEach(k=>r.classList.add(k));
    const input=element('input');input.dataset.select=id;input.checked=/<input[^>]*\schecked/.test(m[3]);
    const label=element('label');label.dataset.inspect=id;let sample=null;
    function insert(markup){const color=markup.match(/--series:([^";]+)/)?.[1];sample=element('sample');sample.style.color=color;sample.remove=()=>{sample=null;};}
    const original=m[3].match(/<i class="sample[^>]*><\/i>/);if(original)insert(original[0]);
    label.querySelector=s=>s==='.sample'?sample:null;label.insertAdjacentHTML=(where,markup)=>insert(markup);
    r.querySelector=s=>s==='input'?input:s==='[data-inspect]'?label:null;
    input.closest=s=>s==='[data-select]'?input:s==='[data-account]'?r:null;
    label.closest=s=>s==='[data-account]'?r:null;
    rows.set(id,r);
  }
}
function query(s){
  if(s.startsWith('[data-stage='))return $(s);
  const m=s.match(/\[data-(account|inspect|select)="([^"]*)"\]/);if(m){const r=rows.get(m[2]);return m[1]==='account'?r:m[1]==='inspect'?r?.querySelector('[data-inspect]'):r?.querySelector('input');}
  return null;
}
$('table-body').querySelector=s=>s==='.pinned'?[...rows.values()].find(r=>r.classList.contains('pinned')):query(s);
$('table-body').querySelectorAll=s=>s==='[data-account]'?[...rows.values()]:[];
function parseCards(){cards=[];for(const group of ['chart-tickers','chart-timeframes'])for(const m of $(group).innerHTML.matchAll(/<button data-chart-group="([^"]+)" data-chart-value="([^"]+)"[^>]*>([^<]*)<\/button>/g)){
  const b=element('card');b.dataset={chartGroup:m[1],chartValue:unesc(m[2])};b.textContent=unesc(m[3]);cards.push(b);
}}
$('chart-filters').querySelectorAll=()=>{if(!cards)parseCards();return cards;};
const tableScroll=element('table-scroll');tableScroll.closest=s=>s.includes('.table-scroll')?tableScroll:null;
globalThis.document={getElementById:$,querySelector:s=>s==='.table-scroll'?tableScroll:query(s),querySelectorAll:()=>[],activeElement:null,
  addEventListener(type,f){(listeners[type]??=[]).push(f);},title:''};
globalThis.window=globalThis;globalThis.CSS={escape:s=>s};globalThis.devicePixelRatio=1;globalThis.innerHeight=900;
globalThis.ResizeObserver=class{observe(){}};globalThis.Path2D=class{moveTo(){}lineTo(){}};
globalThis.requestAnimationFrame=f=>setTimeout(f,0);globalThis.cancelAnimationFrame=clearTimeout;
const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
vm.runInThisContext(script,{filename:'generated-report.js'});
// Independent metric oracle: native gzip decoded by zlib, never by copied UI logic.
const metrics={IS:{},HO1:{},HO2:{}};
manifest.index.members.forEach((e,i)=>{if(e.kind!=='metrics')return;for(const r of JSON.parse(zlib.gunzipSync(Buffer.from(blocks[i],'base64'))).records)metrics[e.stageId][r.configurationId]=r.metrics;});
const configs=manifest.metadata.configurations,cfg=new Map(configs.map(c=>[c.configurationId,c]));
const ids=configs.map(c=>c.configurationId),owner=manifest.metadata.visibility==='owner';
const profitable=(id,stages)=>stages.every(s=>metrics[s][id]?.performanceStatus==='available'&&Number.isFinite(metrics[s][id]?.returnPct)&&metrics[s][id].returnPct>0);
const D=()=>reportDebug(),out={checks:[],synthetic:true,visibility:manifest.metadata.visibility};
const ok=(name,value)=>{assert.ok(value,name);out.checks.push(name);};
const equal=(a,b,name)=>{assert.deepEqual(a,b,name);out.checks.push(name);};
async function settle(){for(let i=0;i<1000;i++){await new Promise(r=>setTimeout(r,5));if(D().error)throw Error(D().error);if(D().ready)return;}throw Error('UI did not settle');}
async function click(id){$(id).click();await settle();}
function key(key,options={}){const e={key,code:key,ctrlKey:false,altKey:false,shiftKey:false,metaKey:false,repeat:false,isComposing:false,defaultPrevented:false,getModifierState:()=>false,
  preventDefault(){this.defaultPrevented=true;},...options};for(const f of listeners.keydown??[])f(e);return e;}
async function row(id,options={}){
  let r=rows.get(id);if(!r){tableScroll.scrollTop=ids.indexOf(id)*34;tableScroll.events.scroll();await new Promise(r=>setTimeout(r,10));r=rows.get(id);}
  assert.ok(r,'Row available in DOM window');const target=options.checkbox?r.querySelector('input'):r.querySelector('[data-inspect]');
  let prevented=false;$('table-body').onclick({target,ctrlKey:false,shiftKey:false,altKey:false,preventDefault(){prevented=true;},...options});await settle();return prevented;
}
function chip(id,remove=false,altKey=false){$('selection').onclick({altKey,target:{closest:s=>s===(remove?'[data-remove]':'[data-focus]')?{dataset:{[remove?'remove':'focus']:id}}:null}});}
async function card(group,value){const b=cards.find(b=>b.dataset.chartGroup===group&&b.dataset.chartValue===String(value));assert.ok(b);$('chart-filters').onclick({target:{closest:()=>b}});await settle();}
async function all(){tableScroll.scrollTop=0;$('table-head').onchange({target:{id:'select-visible',checked:true}});await settle();}
function matches(stages){const f=D().chartFilters;return D().selected.filter(id=>{const c=cfg.get(id);return (!f.tickers.length||f.tickers.includes(c.instrumentId))&&(!f.timeframes.length||f.timeframes.includes(c.timeframeMinutes))&&(!f.profitable||profitable(id,stages));});}
function availability(stages){const f=D().chartFilters;return cards.every(b=>{
  const g=b.dataset.chartGroup,v=g==='timeframes'?Number(b.dataset.chartValue):b.dataset.chartValue;
  const eligible=D().selected.some(id=>{const c=cfg.get(id);return (g==='tickers'?c.instrumentId===v:c.timeframeMinutes===v)&&
    (g==='tickers'||!f.tickers.length||f.tickers.includes(c.instrumentId))&&(g==='timeframes'||!f.timeframes.length||f.timeframes.includes(c.timeframeMinutes))&&(!f.profitable||profitable(id,stages));});
  return b.disabled===(!f[g].includes(v)&&!eligible);
});}
function swatches(){for(const [id,r] of rows){assert.equal(r.querySelector('input').checked,D().selected.includes(id));assert.equal(!!r.querySelector('[data-inspect]').querySelector('.sample'),D().selected.includes(id));assert.equal(r.classList.contains('selected'),D().matching.includes(id));}}
(async()=>{
  await settle();ok('ready with no hidden HO2 decode',D().ready&&!D().ho2Visible&&D().decodes.HO2===0);
  const ih=ids.filter(id=>profitable(id,['IS','HO1']));
  ok('fixture: more than twelve profitable and unprofitable choices',ih.length>12&&ih.length<ids.length);
  ok('fixture: HO1 zero differs from IS',ids.some(id=>metrics.IS[id].returnPct>0&&metrics.HO1[id].returnPct===0));
  ok('fixture: positive stored HO1 unavailable',ids.some(id=>metrics.HO1[id].returnPct>0&&metrics.HO1[id].performanceStatus==='noObservedBars'));
  ok('fixture: fails only IS',ids.some(id=>metrics.IS[id].returnPct<=0&&profitable(id,['HO1'])));
  const distinguishing=owner?ih.filter(id=>metrics.HO2[id].returnPct<=0):[];
  if(owner)ok('fixture: genuine nonpositive HO2 after positive IS/HO1',distinguishing.length>0);
  const tickers=cards.filter(b=>b.dataset.chartGroup==='tickers');
  ok('short SWAP labels and collision disambiguation',tickers.some(b=>b.textContent==='BBB')&&tickers.filter(b=>b.textContent.includes('AAA-USDT-SWAP')).length===2&&tickers.some(b=>b.textContent==='CCCUSDT'));
  const finals=configs.filter(c=>manifest.annotations.annotations.some(a=>a.instrumentId===c.instrumentId&&a.allocationId===c.allocationId&&a.role!=='reference')).map(c=>c.configurationId);
  await click('clear');$('search').value=ids[0];$('search').oninput({target:$('search')});await click('add-finalists');
  equal(D().selected,finals.includes(ids[0])?[ids[0]]:[],'listed finalists respect table filter');
  await click('chart-finalists');equal([...D().selected].sort(),[...finals].sort(),'global finalists ignore table filter/reference-only');
  equal(D().rows,[ids[0]],'chart action preserves table filters');
  equal($('chart-finalists').disabled,finals.length===0,'absent annotations invent no finalists');
  await click('reset-filters');await all();ok('all selected above limit, no arbitrary lines',D().selected.length===ids.length&&D().chartIds.length===0&&D().series.length===0);swatches();
  await click('chart-profitable');equal(D().matching,ih,'Profitable uses IS and HO1 status and exact values');
  $('stages').onclick({target:{closest:()=>({dataset:{stage:'IS'}})}});await settle();equal(D().matching,ih,'chart tab does not choose profitability stages');
  const ticker=cfg.get(ih[0]).instrumentId;await card('tickers',ticker);
  equal(D().matching,matches(['IS','HO1']),'ticker AND profitable');ok('card availability ignores own group',availability(['IS','HO1']));
  const other=tickers.find(b=>b.dataset.chartValue!==ticker&&!b.disabled);assert.ok(other);await card('tickers',other.dataset.chartValue);
  ok('OR expansion includes both tickers',new Set(D().matching.map(id=>cfg.get(id).instrumentId)).size===2);
  await click('chart-reset-filters');await click('chart-profitable');
  const saved=[...D().matching],styles={...D().styles};ok('saved set includes discriminating row',!owner||distinguishing.some(id=>saved.includes(id)));
  await click('chart-save-filtered');equal(D().selected,saved,'Save stores complete ordered matches above limit');
  ok('Save clears chart filters and preserves colors',!D().chartFilters.profitable&&!D().chartFilters.tickers.length&&saved.every(id=>D().styles[id]===styles[id]));
  if(owner){
    $('reveal').click();ok('Save disabled while loading',!D().ready&&$('chart-save-filtered').disabled);await settle();
    equal(D().selected,saved,'Save then Reveal retains nonpositive HO2 members');equal(D().matching,saved,'Save cleared Profitable before Reveal');
    await click('chart-profitable');equal(D().matching,saved.filter(id=>profitable(id,['IS','HO1','HO2'])),'revealed HO2 participates despite IS tab');
    ok('revealed filter excludes distinguishing row',distinguishing.every(id=>!D().matching.includes(id)));
    await click('reveal');const before=D().decodes.HO2;
    $('reveal').click();ok('loading chips do not claim settled no matches',!$('selection').innerHTML.includes('No selected configurations match'));
    $('reveal').click();await settle();ok('reveal-hide race clears hidden financial state',!D().ho2Visible&&!D().ho2Loaded&&!D().revealed.length);
    equal(D().matching,saved,'hidden race returns to IS/HO1 matches');
    $('reveal').click();$('reveal').click();$('reveal').click();await settle();
    equal(D().matching,saved.filter(id=>profitable(id,['IS','HO1','HO2'])),'rapid reveal-hide-reveal converges');
    ok('real decoder counts advanced for HO2',D().decodes.HO2>before);
  }else{await click('reveal');ok('public cannot reveal HO2',!D().ho2Visible&&D().decodes.HO2===0);}
  await click('chart-reset-filters');await all();
  for(const b of tickers)await card('tickers',b.dataset.chartValue);
  const route=[...D().matching];chip(route[0],false,true);await settle();equal(D().solo,'selected','Alt chip enters selected route above limit');
  equal(D().navigation,D().matching,'selected route uses matching order');
  document.activeElement=null;key('ArrowUp');await settle();equal(D().focus,route[0],'selected route stops at beginning');
  chip(route.at(-1),false,true);await settle();key('ArrowDown');await settle();equal(D().focus,route.at(-1),'selected route stops at end');
  await click('chart-save-filtered');ok('Save in Single View keeps all matches and exits',D().selected.length===ids.length&&D().solo===null&&!D().chartFilters.tickers.length);
  await row(ids[0],{altKey:true});document.activeElement=null;key('ArrowUp');await settle();equal(D().focus,ids[0],'table route stops at beginning');
  await row(ids.at(-1),{altKey:true});document.activeElement=null;key('ArrowDown');await settle();equal(D().focus,ids.at(-1),'table route stops at end');
  if(finals.length){await click('chart-finalists');equal(D().solo,null,'Finalists exits Single View');}
  else{await click('chart-finalists');equal(D().solo,'table','disabled absent-finalist action preserves route');}
  await click('clear');tableScroll.scrollTop=0;tableScroll.events.scroll();await new Promise(r=>setTimeout(r,10));
  ok('checkbox activation is not canceled',!await row(ids[0],{checkbox:true}));swatches();
  const sampleColor=rows.get(ids[0]).querySelector('[data-inspect]').querySelector('.sample').style.color;
  await row(ids[0],{checkbox:true});swatches();await row(ids[0],{checkbox:true});swatches();
  equal(rows.get(ids[0]).querySelector('[data-inspect]').querySelector('.sample').style.color,sampleColor,'incremental swatch keeps stable color');
  await row(ids[3],{shiftKey:true});equal(D().selected,ids.slice(0,4),'Shift range membership');swatches();
  await row(ids[0],{checkbox:true});await row(ids[3],{shiftKey:true});equal(D().selected,[],'Shift range removal');swatches();
  await all();ok('header check state', $('select-visible').checked&&!$('select-visible').indeterminate);await row(ids[0],{checkbox:true});ok('header becomes mixed',!$('select-visible').checked&&$('select-visible').indeterminate);
  await click('chart-profitable');for(const id of [...D().selected])chip(id,true);await settle();
  ok('empty active Profitable remains removable',D().chartFilters.profitable&&!$('chart-profitable').disabled);await click('chart-profitable');ok('empty inactive Profitable disabled',!D().chartFilters.profitable&&$('chart-profitable').disabled);
  await click('clear');ok('clear resets filters and Single View',D().selected.length===0&&!D().chartFilters.profitable&&D().solo===null);
  for(const queryText of ['OKX_BBB-USDT-SWAP','-USDT-SWAP','BYBIT']){$('search').value=queryText;$('search').oninput({target:$('search')});equal(D().rows,configs.filter(c=>c.instrumentId.toLowerCase().includes(queryText.toLowerCase())).map(c=>c.configurationId),'search full instrument identity '+queryText);}
  document.activeElement=tableScroll;ok('horizontal table key not canceled',!key('ArrowRight').defaultPrevented);
  out.ok=true;out.decodes=D().decodes;out.checkCount=out.checks.length;console.log(JSON.stringify(out));
})().catch(error=>{out.ok=false;out.error=error.stack;console.error(JSON.stringify(out));process.exitCode=1;});
