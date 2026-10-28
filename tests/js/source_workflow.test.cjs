const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '../..');
const clone = value => JSON.parse(JSON.stringify(value));
class Element {
  constructor(tag='div') { this.tagName=tag; this.children=[]; this.handlers={}; this.dataset={}; this.style={}; this.value=''; this.disabled=false; this.checked=false; this.hidden=false;
    this.classList={add(){},remove(){},toggle(){}}; }
  append(...nodes) { this.children.push(...nodes); }
  appendChild(node) { this.append(node); return node; }
  replaceChildren() { this.children=[]; }
  addEventListener(name, fn) { this.handlers[name]=fn; }
  setAttribute(name, value) { this[name]=value; }
  removeAttribute(name) { delete this[name]; }
  querySelectorAll(selector) { return selector==='tr[tabindex]' ? this.children.filter(n=>n.tabIndex===0) : []; }
  get firstChild() { return this.children[0]; }
  setCustomValidity(value) { this.validationMessage=value; }
}
function environment() {
  const elements=new Map();
  const get=id=>{if(!elements.has(id)) elements.set(id,new Element());return elements.get(id);};
  const timers=new Map(); let timer=0;
  const c={console, document:{getElementById:get,createElement:tag=>new Element(tag),querySelector:()=>null,querySelectorAll:()=>[],addEventListener(){}},
    sessionStorage:{getItem:()=>null,setItem(){}},setTimeout:fn=>{timers.set(++timer,fn);return timer;},clearTimeout:id=>timers.delete(id),
    localStorage:{getItem:()=>null,setItem(){}}, clonePreset:clone};
  c.window=c; vm.createContext(c);
  const load=name=>vm.runInContext(fs.readFileSync(path.join(root,'src/ui/static/js',name),'utf8'),c);
  load('ui-handlers.js');load('queue.js');load('parquet-input.js');
  c.getOptimizerMode=()=>c.mode || 'grid';c.isCurrentStrategyConfigReady=()=>true;
  c.currentStrategyId='s07_ha_v01_b2';c.currentStrategyConfig={engine:'v2',execution:{entryAdmission:'risk_bracket_v1'},entryFilterSupport:{version:1,methods:['candleDirection']}};
  c.config={optimization_mode:'grid',grid_v2_planning_policy:'full',grid_budget:748000,enabled_params:{stopX:true},fixed_params:{}};
  c.gatherFormState=()=>({start:'2024-01-01',end:'2024-02-01'});c.buildCurrentOptimizerConfig=()=>clone(c.config);
  c.renderSelectedFiles=()=>{if(c.ParquetInput.state.mode)c.ParquetInput.renderSelected();};
  c.syncFixedControls=()=>{};c.syncGridV2ManualAllocation=()=>{};c.renderCsvBrowserEntries=()=>{};c.showCsvBrowserError=message=>{c.pickerError=message;};
  c.closeCsvBrowserModal=()=>c.ParquetInput.close();
  c.getDatabaseTargetValidationError=()=>'';c.collectQueueUiSnapshot=()=>({});c.loadQueue=()=>({items:[],nextIndex:1});
  c.validateOptimizerForm=()=>[];
  get('enableWF').checked=true;get('warmupBars').value='100';
  const previews=[];
  c.fetchGridPreviewRequest=(...args)=>new Promise((resolve,reject)=>previews.push({args,resolve,reject}));
  const requests=[];c.fetch=url=>new Promise(resolve=>requests.push({url,resolve}));
  const tick=()=>{const pending=[...timers.values()];timers.clear();return Promise.all(pending.map(fn=>fn()));};
  return {c,get,load,tick,previews,requests,timers};
}
const instrument=(id,roles=[])=>({instrument_id:id,roles,first_open_utc:'2024-01-01',coverage_end_utc:'2024-02-01',verification:{closed_before_utc:'2024-02-01'},missing_bar_count:0});
const source=(id='TEST_AAA',tf=5)=>({type:'parquet',version:1,pack_root:'/pack',instrument_id:id,timeframe_minutes:tf});
const preview={requested_planning_policy:'full',effective_planning_policy:'full',planned_candidate_count:840,modes:[]};
const reply=(request,value,ok=true)=>request.resolve({ok,json:async()=>value});
async function selectionAndPreview() {
  const {c,get,tick,previews,timers}=environment(), p=c.ParquetInput;
  // Empty source -> failure -> add via real picker -> debounced valid request.
  p.setMode(true);let pending=tick();previews.at(-1).reject(Error('Select a Parquet source'));await pending;
  assert.equal(c.lastGridPreview,null);
  p.state.pack={instruments:[instrument('TEST_AAA',['trading'])]};p.model.reset(p.state.pack.instruments);
  vm.runInContext("csvBrowserState.currentPath='/pack'",c);p.state.paths.parquet='/pack';
  p.model.selected.add(0);p.model.checked[0][0]=p.model.checked[0][1]=true;
  await p.add();assert.equal(get('queueAutoCreateSet').disabled,false);assert.equal(get('queueAutoCreateSet').checked,true);
  assert.equal(timers.size,1);pending=tick();assert.deepEqual(clone(previews.at(-1).args[3]),source());
  previews.at(-1).resolve(preview);await pending;
  let item=c.collectQueueItem();assert.equal(item.planned_candidate_count,840);assert.match(item.label,/2 PQs/);
  // Explicit Off survives ineligible states, switching and Fixed WFA.
  c.writeQueueAutoCreateSetStoredPreference(get('queueAutoCreateSet'),false);
  c.mode='fixed';c.config.parameterMode='fixed';c.notifyDataSourcesChanged();assert.equal(get('queueAutoCreateSet').checked,false);
  assert.equal(c.getQueueGridPlannedFacts(c.collectQueueItem()).count,1);
  delete c.config.parameterMode;c.mode='grid';
  // The first request is obsolete as soon as selection changes, before debounce.
  pending=tick();const old=previews.at(-1);
  get('selectedFilesList').children[0].children[0].handlers.click();
  assert.equal(c.lastGridPreview,null);assert.equal(get('queueAutoCreateSet').disabled,true);
  old.resolve(preview);await pending;assert.equal(c.lastGridPreview,null);
  item=c.collectQueueItem();assert.equal(item.planned_candidate_count,undefined);assert.match(item.label,/count unavailable/);assert(!item.label.includes('748000'));
  pending=tick();assert.equal(previews.at(-1).args[3].timeframe_minutes,15);
  const stale=previews.at(-1);c.setSelectedCsvPaths(['/a.csv','/b.csv']);p.setMode(false);
  let latest=tick();previews.at(-1).resolve(preview);await latest;
  stale.resolve({...preview,planned_candidate_count:99});await pending;assert.equal(c.lastGridPreview.planned_candidate_count,840);
  assert.equal(get('queueAutoCreateSet').disabled,false);assert.equal(get('queueAutoCreateSet').checked,false);
  p.restore([source(),source('TEST_AAA',15)]);assert.equal(c.lastGridPreview,null);assert.equal(get('queueAutoCreateSet').checked,false);
  pending=tick();previews.at(-1).resolve(preview);await pending;
  get('warmupBars').value='200';assert.equal(c.collectQueueItem().planned_candidate_count,undefined);
  p.clear();assert.equal(c.lastGridPreview,null);assert.equal(get('queueAutoCreateSet').disabled,true);
  pending=tick();previews.at(-1).reject(Error('No target'));await pending;assert.equal(c.lastGridPreviewConfigKey,null);
  assert.equal(c.getQueueGridPlannedFacts({config:{grid_v2_planning_policy:'sampled',grid_budget:123}}).count,123);
  assert.equal(c.getQueueGridPlannedFacts({config:{grid_budget:456}}).count,456);
  assert.equal(c.getQueueGridPlannedFacts({strategyConfig:{engine:'v1'},config:{grid_v2_planning_policy:'full',grid_budget:456}}).count,456);
  assert.equal(c.getCurrentPlannedGridCandidates(c.config),null);
  c.currentStrategyConfig={engine:'v1'};assert.equal(c.getCurrentPlannedGridCandidates(c.config),748000);
  const mixed={sources:[source(),source('TEST_AAA',15),{type:'path',path:'/a.csv'},{type:'future',key:'preserve'}],config:{}};
  assert.match(c.generateQueueLabel(mixed),/2 PQs \+ 1 CSV \+ 1 unsupported source/);
  // Pack-only browsing does not turn CSV target selection into Parquet selection.
  p.setMode(false);p.state.packChoice=()=>{};assert.equal(c.getSelectedDataSources()[0].type,'path');
}
async function sets() {
  const {c,get,requests}=environment(),p=c.ParquetInput;
  p.setMode(true);
  const instruments=[instrument('TEST_A',['trading']),instrument('TEST_B',['research_only']),instrument('TEST_C',['factor']),instrument('TEST_D')];
  let pending=p.load('/pack');reply(requests.at(-1),{current_path:'/pack',entries:[],pack:{instruments}});await pending;
  const dropdown=get('parquetTickerSets');assert.equal(dropdown.hidden,false);
  assert.deepEqual(dropdown.children.slice(1).map(o=>o.textContent),['Main (1)','Additional (1)','All (4)']);
  p.model.checked[0][0]=true;p.model.checked[1][1]=true;p.model.commit(0,'10m');
  dropdown.value='Main';dropdown.onchange();assert.deepEqual([...p.model.selected],[0]);assert.equal(dropdown.value,'');
  dropdown.value='Additional';dropdown.onchange();assert.deepEqual([...p.model.selected],[1]);assert(p.model.checked[0][0]);assert.equal(p.model.durations[0],10);
  // Real Ctrl row event extends the set; Add merges pairs and leaves prior pairs.
  get('parquetGrid').firstChild.children[1].handlers.click({ctrlKey:true});
  await p.add();assert.equal(p.sources().length,2);
  await p.add();assert.equal(p.sources().length,2);
  pending=p.load('/other');assert.equal(dropdown.hidden,true);
  reply(requests.at(-1),{current_path:'/other',entries:[],pack:{instruments:[instrument('TEST_UNKNOWN',null)]}});await pending;
  assert.equal(p.model.selected.size,0);assert(dropdown.children[1].disabled);assert(dropdown.children[2].disabled);
  dropdown.value='All';dropdown.onchange();assert.equal(p.model.selected.size,1);
  await p.add();assert.match(c.pickerError,/select at least one timeframe/);
  p.state.packChoice=()=>{};pending=p.load('/filter');reply(requests.at(-1),{current_path:'/filter',entries:[],pack:{instruments}});await pending;assert(dropdown.hidden);
  p.state.packChoice=null;p.state.relocation={study:'old'};pending=p.load('/relocate');reply(requests.at(-1),{current_path:'/relocate',entries:[],pack:{instruments}});await pending;assert(dropdown.hidden);
  p.model.reset([instruments[0],instruments[0],instruments[1]]);assert.equal(p.model.members('All').length,2);
}
async function filters() {
  const {c,get,load,requests}=environment();get('entryFiltersEditor').dataset.defaultPack='/server-default';
  load('entry-filters.js');const api=c.EntryFilters;
  const restore=()=>api.restore({version:1,enabled:true,items:[]});
  const add=()=>get('entryFiltersEditor').children[0].children[2].onclick();
  const field=label=>{const walk=n=>n['aria-label']===label?n:n.children.map(walk).find(Boolean);return walk(get('entryFiltersEditor'));};
  const text=()=>{const walk=n=>[n.textContent||'',...n.children.map(walk)].join(' ');return walk(get('entryFiltersEditor'));};
  const flush=()=>new Promise(resolve=>setImmediate(resolve));
  c.ParquetInput.setMode(true);get('csvDirectory').value='/chosen';restore();add();
  assert.equal(field('Source pack').value,'/chosen');assert(requests.at(-1).url.includes('%2Fchosen'));
  reply(requests.at(-1),{pack:{instruments:[instrument('OKX_BTC-USDT-SWAP'),instrument('TEST_A')]}});await flush();
  assert.equal(field('Filter instrument').children.length,2);
  const saved=api.collect();get('csvDirectory').value='/another';assert.equal(api.collect().items[0].source.pack_root,'/chosen');
  const path=field('Source pack');path.value='/missing';path.onchange();const missing=requests.at(-1);
  path.value='/valid';path.onchange();reply(requests.at(-1),{pack:{instruments:[instrument('TEST_A')]}});await flush();
  assert.match(text(),/OKX_BTC-USDT-SWAP is absent/);
  reply(missing,{error:'old error'},false);await flush();assert(!text().includes('old error'));
  field('Source pack').value='/late';field('Source pack').onchange();const late=requests.at(-1);
  api.restore(saved);reply(late,{error:'late error'},false);await flush();assert(!text().includes('late error'));assert.equal(field('Source pack').value,'/chosen');
  c.ParquetInput.setMode(false);get('csvDirectory').value='/csv';c.ParquetInput.state.paths.parquet='';restore();add();
  assert.equal(field('Source pack').value,'/server-default');reply(requests.at(-1),{error:'Directory not found'},false);await flush();assert.match(text(),/Use Browse/);
  assert.equal(field('Source pack').value,'/server-default');
  c.ParquetInput.state.paths.parquet='/remembered';restore();add();assert.equal(field('Source pack').value,'/remembered');
  const removed=requests.at(-1);get('entryFiltersEditor').children.find(n=>n.tagName==='details').children[0].children[2].onclick({preventDefault(){}});
  reply(removed,{pack:{instruments:[instrument('OKX_BTC-USDT-SWAP')]}});await flush();assert.equal(api.collect().items.length,0);
}
async function queueCompletion() {
  for (const fixed of [false,true]) {
    const {c,get}=environment();
    const item={id:'work',mode:'wfa',strategyId:'s07',strategyConfig:{engine:'v2'},sources:[source(),source('TEST_B'),source('TEST_C'),source('TEST_D')],
      config:{...c.config,...(fixed?{parameterMode:'fixed'}:{})},wfa:{isPeriodDays:90,oosPeriodDays:30},studySet:{configured:true,autoCreate:true}};
    let finished=false, calls=0, sets=[];
    const queue={items:[item]};
    Object.assign(c,{AbortController,FormData,ensureQueueStateLoaded:async()=>{},getQueueForUi:()=>queue,loadQueue:()=>queue,
      countQueuePendingItems:()=>1,findNextPendingQueueItem:()=>finished?null:item,
      saveQueueRuntimeState:async()=>{},setQueueControlsDisabled(){},updateRunButtonState(){},saveOptimizationState(){},updateOptimizationState(){},
      buildStateForItem:()=>({}),setActiveOptimizationRunId(){},openResultsPage(){},setQueueItemState(){},renderQueue(){},
      persistItemProgress:async(id,patch)=>Object.assign(item,patch),finalizeQueueItem:async(id,status,patch)=>{Object.assign(item,patch);finished=true;},
      generateOptimizationRunId:()=> 'synthetic',
      runWalkForwardRequest:async form=>{assert.equal(JSON.parse(form.get('source')).type,'parquet');calls++;if(calls===4)throw Error('Coverage ends before requested End; extend the pack.');return {study_id:calls===1?'study-a':'study-b'};},
      createAnalyticsSetRequest:async(name,ids)=>{sets.push(clone(ids));return {id:7,name};},
      showQueueError:message=>{throw Error(message);}});
    await c.runQueue();
    assert.equal(calls,4);assert.equal(item.sourceCursor,4);assert.equal(item.successCount,3);assert.equal(item.failureCount,1);
    assert.deepEqual(sets,[['study-a','study-b']]);assert.match(item.lastError,/Coverage ends/);
    assert.match(get('optimizerResults').textContent,/Partial: 1/);assert.match(get('optimizerResults').textContent,/extend the pack/);
    await c.finalizeQueueStudySetIfNeeded(item,item.studySet);assert.equal(sets.length,1);
  }
}
(async()=>{await selectionAndPreview();await sets();await filters();await queueCompletion();console.log('Source events, Preview races/counts, typed Queue completion, Ticker Sets and filter defaults/races passed');})()
  .catch(error=>{console.error(error);process.exitCode=1;});
