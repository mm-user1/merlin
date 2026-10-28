const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const F = require('../../src/ui/static/js/entry-filters.js');
const clone = value => JSON.parse(JSON.stringify(value));
const item = (id='one', pack='C:\\Data\\Pack') => ({id,type:'candleDirection',version:1,enabled:true,
  parameters:{confirmationBars:2},source:{type:'parquet',version:1,pack_root:pack,instrument_id:'OKX_BTC-USDT-SWAP',timeframe_minutes:240}});
const editor = (...items) => ({version:1,enabled:true,items});
function record(value) {
  const effective = F.effective(value);
  return {entryFilters:value,entry_filter_inputs:effective.length ? {version:1,state:'filtered',effective,
    sources:effective.map(descriptor=>({descriptor,policy:'closed_candle_direction_v1',fingerprint_version:1,input_fingerprint:'a'.repeat(64),
      bounds:{start:'2024-01-01T00:00:00Z',end:'2024-02-01T00:00:00Z',origin:'2024-01-01T00:00:00Z',priorBars:28},instrument:{}}))}
    : {version:1,state:'off'}};
}
assert.equal(F.duration('4h'),240);
assert.equal(F.duration('3D'),4320);
assert.equal(F.locator('C:/'),'c:/');
assert.equal(F.locator('C:/x/../Pack'),'c:/pack');
assert.equal(F.locator('\\\\HOST\\Share\\..\\Pack'),'//host/share/pack');
assert.throws(()=>F.duration('7m'));
const a=record(editor(item())), duplicate=record(editor(item('two','c:/data/pack/'),item()));
assert.equal(F.saved(a).key,F.saved(duplicate).key);
const changed=record(editor(item('two','C:/Other')));
assert.notEqual(F.saved(a).key,F.saved(changed).key);
const second=item('two');second.parameters.confirmationBars=3;
assert.equal(F.saved(record(editor(item(),second))).key,F.saved(record(editor(second,item()))).key);
for(const value of [null,{}, {version:1,enabled:true,items:[item(),item()]}]) assert.throws(()=>F.effective(value));
assert.equal(F.saved({entryFilters:F.off()}).text,'Unavailable');
// V2 pins carry target-relative bounds and an explicit versioned origin rule.
function recordV2(n=3,m=2) {
  const value=editor({...item(),version:2,parameters:{confirmationBars:n,breakBars:m}});
  const result=record(value), pin=result.entry_filter_inputs.sources[0];
  result.data_input={source:{timeframe_minutes:30},bounds:{start:'2024-01-10T00:00:00Z',end:'2024-02-01T00:00:00Z'}};
  pin.policy='closed_candle_direction_v2';pin.originRule=m>1?'declared_source_origin':'bounded_prefix';
  if(m===1) pin.bounds.start=pin.bounds.origin='2024-01-05T04:00:00Z';
  return result;
}
const v2=recordV2();
assert.match(F.saved(v2).text,/F06 v2.*N 3 \/ break 2/);
assert.notEqual(F.saved(recordV2(2,1)).key,F.saved(a).key);
assert.notEqual(F.saved(recordV2(3,1)).key,null);
for(const mutate of [c=>delete c.data_input,c=>c.entry_filter_inputs.sources[0].originRule='bounded_prefix',
  c=>c.entry_filter_inputs.sources[0].policy='closed_candle_direction_v1',
  c=>c.data_input.bounds.end='2024-02-02T00:00:00Z',
  c=>Object.assign(c.entry_filter_inputs.sources[0].bounds,{start:'2024-01-10T00:00:00Z',origin:'2024-01-10T00:00:00Z'})]) {
  const broken=clone(v2);mutate(broken);assert.equal(F.saved(broken).text,'Unavailable');
}
for(const [n,m] of [[true,1],[3,true],[2,2],[7,1],[6,5],[3,0],[3,1.5]])
  assert.throws(()=>recordV2(n,m));
const studies=[{study_id:'a',config_json:a},{study_id:'b',config_json:duplicate},{study_id:'c',config_json:changed},
  {study_id:'old',config_json:{}},{study_id:'bad',config_json:{entryFilters:null}}];
assert.equal(F.consensus([studies[0],studies[1]]),F.saved(a).text);
assert.equal(F.consensus([studies[0],studies[2]]),'Mixed');
assert.equal(F.consensus([studies[0],studies[2],studies[4]]),'Unavailable');
assert.equal(F.consensus([]),'?');assert.equal(F.consensus([studies[3]]),'Filter Off');
const sets=[{id:1,study_ids:['a','b','orphan']},{id:2,study_ids:['b','c']}];
const options={studies,context:[studies[3]],sets,focusedSetId:1,checkedSetIds:new Set([1,2])};
assert.deepEqual(F.population({...options,mode:'setFocus'}).map(s=>s.study_id),['a','b']);
assert.deepEqual(F.population({...options,mode:'setCheckboxes'}).map(s=>s.study_id),['a','b','c']);
// Actual All Studies mode ignores checks retained behind forceAllStudies.
assert.deepEqual(F.population({...options,mode:'allStudies'}),[studies[3]]);
assert.deepEqual(F.population({...options,mode:'setCheckboxes',focusedId:'bad'}),[studies[4]]);

// Real editor event handlers, with explicit DOM state and no browser-layout claim.
const elements=new Map();
class Element {
  constructor(tag){this.tagName=tag;this.children=[];this.style={};this.value='';this.checked=false;this.disabled=false;}
  set id(value){this._id=value;elements.set(value,this);} get id(){return this._id;}
  append(...nodes){this.children.push(...nodes);} appendChild(n){this.append(n);}
  replaceChildren(){this.children=[];} setAttribute(k,v){this[k]=v;} setCustomValidity(v){this.validationMessage=v;}
}
const host=new Element('section');host.id='entryFiltersEditor';
const sandbox={console,document:{getElementById:id=>elements.get(id),createElement:tag=>new Element(tag),addEventListener(){}},
  currentStrategyConfig:{entryFilterSupport:{version:1,methods:['candleDirection']}},resetGridPreviewState(){},scheduleGridPreviewUpdate(){}};
sandbox.window=sandbox;vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(path.resolve(__dirname,'../../src/ui/static/js/entry-filters.js'),'utf8'),sandbox);
const api=sandbox.EntryFilters;
api.render();let master=elements.get('entryFiltersEnabled');master.checked=true;master.onchange();
host.children[0].children[2].onclick();
let details=host.children.find(n=>n.tagName==='details');assert(details.open);
const sourceInput=details.children[1].children[1].children[0];
sourceInput.value='C:/Data/Pack';
// Browse completion supplies manifest choices and does not alter target selection.
sandbox.ParquetInput={choosePack:(path,callback)=>callback('C:/Data/Pack',{instruments:[{instrument_id:'OKX_BTC-USDT-SWAP',first_open_utc:'2024-01-01',coverage_end_utc:'2024-02-01'}]})};
details.children[1].children[1].children[1].onclick();
let collected=api.collect();assert.equal(collected.items.length,1);const id=collected.items[0].id;
assert.equal(collected.items[0].version,2);
assert.deepEqual(clone(collected.items[0].parameters),{confirmationBars:2,breakBars:1});
details=host.children.find(n=>n.tagName==='details');details.open=false;details.ontoggle();api.render();
assert(!host.children.find(n=>n.tagName==='details').open);
master=elements.get('entryFiltersEnabled');master.checked=false;master.onchange();assert(!host.children.some(n=>n.tagName==='details'));
assert.equal(api.collect().items[0].id,id);
api.restore(collected);assert.equal(api.collect().items[0].id,id);
sandbox.currentStrategyConfig={};api.render();assert.throws(()=>api.collect(),/unsupported/);
master=elements.get('entryFiltersEnabled');assert.equal(master.disabled,false);master.checked=false;master.onchange();assert.equal(api.collect().enabled,false);
const unknown=editor({...item(),type:'future',enabled:false});api.restore(unknown);assert.equal(api.collect().items[0].type,'future');
api.restore(editor(null));assert(host.children.some(n=>String(n.textContent).includes('Unsupported saved filter')));
api.restore(collected);sandbox.currentStrategyConfig={entryFilterSupport:{version:1,methods:['candleDirection']}};api.render();
assert.equal(host.children[0].children[2].className,'btn-secondary');
function input(label) {
  function find(node) {return node['aria-label']===label ? node : node.children.map(find).find(Boolean);}
  return find(host);
}
function edit(label,value) {const node=input(label);node.value=String(value);node.onchange();}
const legacy=editor(item());legacy.items[0].parameters.confirmationBars=100;
api.restore(legacy);
edit('Trend Confirm Bars',99);edit('Filter timeframe','1h');
assert.equal(api.collect().items[0].version,1);
edit('Trend Break Bars',2); // Explicit conversion must first have valid N.
assert.equal(api.collect().items[0].version,1);
assert.match(elements.get('entryFiltersError').textContent,/before converting/);
edit('Trend Confirm Bars',4);
assert.equal(api.collect().items[0].version,1);
edit('Trend Break Bars',2);
assert.equal(api.collect().items[0].version,2);
assert.deepEqual(clone(api.collect().items[0].parameters),{confirmationBars:4,breakBars:2});
edit('Trend Break Bars',4);assert.throws(()=>api.collect(),/strictly below/);
api.restore(collected);

// Actual Sets All Studies handler and Analytics Research Info builder. Expose
// existing closure functions only in this VM; production globals stay unchanged.
function expose(filename, injection) {
  const source=fs.readFileSync(path.resolve(__dirname,'../../src/ui/static/js/'+filename),'utf8');
  return source.replace(/\}\)\(\);\s*$/,injection+'\n})();');
}
const analytics={console,document:{getElementById(){return null;},addEventListener(){}},EntryFilters:F};
analytics.window=analytics;vm.createContext(analytics);
vm.runInContext(expose('analytics-sets.js','window.setTest={state,resolveViewMode,handleAllStudiesClick};'),analytics);
vm.runInContext(expose('analytics.js','window.infoTest={AnalyticsState,buildResearchInfoRows,syncSetStateFromModule};'),analytics);
Object.assign(analytics.setTest.state,{studies,studyMap:new Map(studies.map(s=>[s.study_id,s])),sets,checkedSetIds:new Set([1]),focusedSetId:null});
Object.assign(analytics.infoTest.AnalyticsState,{studies,orderedStudyIds:['old']});
const row=()=>{analytics.infoTest.syncSetStateFromModule();return analytics.infoTest.buildResearchInfoRows({}).find(r=>r.key==='Entry Filters').val;};
analytics.setTest.resolveViewMode();assert.equal(row(),F.saved(a).text);
analytics.setTest.handleAllStudiesClick();assert.equal(analytics.AnalyticsSets.getViewMode(),'allStudies');
assert.equal(analytics.AnalyticsSets.getCheckedSetIds().size,1);assert.equal(row(),'Filter Off');
analytics.infoTest.AnalyticsState.focusedStudyId='bad';assert.equal(row(),'Unavailable');
analytics.infoTest.AnalyticsState.focusedStudyId=null;analytics.infoTest.AnalyticsState.orderedStudyIds=[];assert.equal(row(),'?');

// Real Queue restoration ordering: a stale generic snapshot runs before the
// authoritative Entry Filters restore. Only unrelated application services stubbed.
const queueSource=fs.readFileSync(path.resolve(__dirname,'../../src/ui/static/js/queue.js'),'utf8');
Object.assign(sandbox,{clonePreset:clone,setInputValue(){},renderSelectedFiles(){},
  getPathParentDirectory(){return '';},setSelectedCsvPaths(){}});
sandbox.ParquetInput={restore(){},validSource(){return true;}};
vm.runInContext(queueSource+`\nensureQueueStateLoaded=async()=>{};loadQueue=()=>({items:[]});
ensureQueueItemStrategyLoaded=async()=>{};applyQueueConfigFallback=()=>{};
applyQueueUiSnapshot=()=>EntryFilters.restore({version:1,enabled:false,items:[]});
applyQueueDatabaseTarget=async()=>{};buildQueueStudySetState=()=>({configured:false});
syncQueueAutoCreateSetUi=()=>{};refreshQueueFormUiAfterApply=()=>{};
window.restoreQueueForTest=loadQueueItemIntoForm;`,sandbox);
(async()=>{for(const value of [legacy,collected]) {
  await sandbox.restoreQueueForTest('filter-queue',{itemSnapshot:{id:'filter-queue',sources:[{type:'path',path:'C:/sample.csv'}],config:{entryFilters:value}}});
  assert.deepEqual(clone(api.collect()),clone(value));
}console.log('Real Queue restore passed for both method versions');})()
  .catch(e=>{console.error(e);process.exitCode=1;});
console.log('Entry filter config, editor and Analytics population contracts passed');
