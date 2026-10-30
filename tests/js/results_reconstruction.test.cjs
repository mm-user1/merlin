const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {Element} = require('./analytics_dom.cjs');
Object.defineProperty(Element.prototype, 'firstElementChild', {get(){return this.children[0] || null;}});
Element.prototype.insertAdjacentHTML = function(position, html) {
  const holder = new Element(); holder.innerHTML = html;
  for (const child of holder.children) child.parentElement = this;
  this.children = position === 'afterbegin' ? [...holder.children, ...this.children] : [...this.children, ...holder.children];
};
const document = new Element('document');
document.innerHTML = '<svg class="chart-svg"></svg><div id="equityAxis"></div><div id="comparisonLine"></div>' +
  '<div id="paramDetailsSection"><div id="paramDetailsTitle"></div><div id="paramDetailsContent"></div></div>' +
  '<div class="table-card"><table class="data-table"><thead><tr></tr></thead><tbody></tbody></table></div>';
document.getElementById = id => document.querySelector('#' + id);
document.createElement = tag => new Element(tag);
document.createElementNS = (_, tag) => new Element(tag);
const state = {studyId:'csv', strategyId:'s03', hasDataInput:false, activeTab:'optuna', selectedRowId:null,
  mode:'grid', strategyConfig:{parameters:{}}, optuna:{objectives:[],constraints:[]}, fixedParams:{}, results:[]};
const diagnostics = [];
const context = {document, ResultsState:state, console, Date, FormData:class {append(){}},
  EntryDiagnostics:{show(value){diagnostics.push(value);}}, createParamId:()=> 'params',
  formatParamValue:String, formatParamName:String};
context.window=context; vm.createContext(context);
function source(file) {return fs.readFileSync(path.join(__dirname, '../../src/ui/static/js', file), 'utf8');}
vm.runInContext(source('optuna-results-ui.js'), context);
vm.runInContext(source('results-tables.js'), context);
const controller = source('results-controller.js');
vm.runInContext(controller.slice(controller.indexOf('async function fetchEquityCurve('), controller.indexOf('function refreshResultsView(')), context);
vm.runInContext(source('wfa-results-ui.js'), context);
const svg=document.querySelector('.chart-svg'), axis=document.getElementById('equityAxis');
const points=()=>svg.querySelector('polyline')?.getAttribute('points');
const base=()=>Number(svg.children.find(e=>e.getAttribute('stroke-dasharray')==='3 4').getAttribute('y1'));
const dates=['2024-01-01T00:00:00Z','2024-01-30T00:00:00Z','2024-01-31T00:00:00Z'];
const ok = payload => ({ok:true,json:async()=>payload});
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const flush=async()=>{for(let n=0;n<8;n++)await Promise.resolve();};
const click=row=>row.handlers.click[0]();

async function main() {
  for (const capital of [250,1000]) {
    context.renderEquityChart([capital*.9,capital*1.1], null, [dates[0],dates[2]], {referenceCapital:capital});
    assert.equal(base(),130); assert.equal(points(),'0,240 800,20');
  }
  context.renderEquityChart([1000,1000,1100],null,dates,{referenceCapital:1000});
  assert.equal(points(),'0,240 400,240 800,20'); // Long delay keeps index spacing.
  context.renderEquityChart([100,100,110],null,dates,{useTimeScale:true,referenceCapital:100});
  assert.equal(base(),240); assert(points().includes(`${29/30*800},240`));
  context.renderEquityChart([999],null,[dates[0]],{referenceCapital:1000});
  assert.equal(base(),20); assert.equal(points(),'0,240');

  // Actual click handler -> actual parameter loader -> server-selected CSV replay.
  const rows=[{trial_number:7,param_id:'A',params:{}},{trial_number:8,param_id:'B',params:{}}];
  state.results=rows;state.strategyConfig=null;
  const configs=[deferred(),deferred()];let configIndex=0;const requests=[];
  context.fetchStrategyConfig=()=>configs[configIndex++].promise;
  context.fetch=async(url,options)=>{requests.push({url,options});return ok({equity:[900,1100],timestamps:[dates[0],dates[2]],reference_capital:1000});};
  context.renderOptunaTable(rows);
  const tableRows=document.querySelector('.data-table tbody').children;
  const first=click(tableRows[0]), second=click(tableRows[1]);
  assert.equal(requests.length,0);
  configs[1].resolve({parameters:{}});await second;
  const currentPoints=points();
  configs[0].resolve({parameters:{}});await first;
  assert.equal(requests.length,1);assert(requests[0].url.includes('/trials/8/equity'));
  assert.equal(JSON.parse(requests[0].options.body).module,'optuna');
  assert.equal(document.getElementById('paramDetailsTitle').textContent,'Parameters: B');
  assert.equal(base(),130);assert.equal(points(),currentPoints);

  // Selection can change while response.json itself is pending.
  const json=deferred();context.fetch=async()=>({ok:false,json:()=>json.promise});
  const late=context.fetchEquityCurve(rows[1]);await flush();
  state.studyId='new-study';state.activeManualTest={id:2};
  json.resolve({error:'obsolete refusal'});assert.equal(await late,null);assert.equal(points(),currentPoints);
  const slow=deferred();context.fetch=()=>slow.promise;
  const old=context.fetchEquityCurve(rows[0]);state.selectedRowId=8;
  context.captureEquitySelection({trialNumber:8});slow.resolve(ok({equity:[1,2],timestamps:dates.slice(0,2)}));
  assert.equal(await old,null);assert.equal(points(),currentPoints);
  context.fetch=async()=>({ok:false,json:async()=>({error:'Exact saved interval is unavailable.'})});
  assert.equal(await context.fetchEquityCurve(rows[1]),null);
  assert.equal(svg.children.length,0);assert.equal(axis.children.length,0);assert.equal(diagnostics.at(-1),null);
  assert.equal(document.getElementById('comparisonLine').textContent,'Exact saved interval is unavailable.');

  // Real WFA window row -> useful refusal -> valid monetary curve.
  state.mode='wfa';state.activeTab='wfa';state.activeManualTest=null;state.strategyConfig={parameters:{}};
  state.results=[{window_number:1,best_params:{initialCapital:1000},param_id:'window',available_modules:[],
    is_start_date:'2024-01-01',is_end_date:'2024-01-29',oos_start_date:'2024-01-30',oos_end_date:'2024-01-31',oos_execution_capital:750}];
  state.stitched_oos={equity_curve:[100,110],timestamps:[dates[0],dates[2]]};
  // Invoke both real stitched callers, observing their explicit options rather
  // than relying on renderer defaults (which would conceal a missing argument).
  const render=context.renderEquityChart, stitchedCalls=[];
  context.renderEquityChart=(...args)=>{stitchedCalls.push(args);return render(...args);};
  const checkStitched=()=>{
    assert.equal(stitchedCalls.length,1);
    const [equity,,timestamps,options]=stitchedCalls.pop();
    assert.deepEqual(equity,state.stitched_oos.equity_curve);
    assert.deepEqual(timestamps,state.stitched_oos.timestamps);
    assert.equal(options?.useTimeScale,true);
    assert.equal(options?.referenceCapital,100);
  };
  vm.runInContext(controller.slice(controller.indexOf('function refreshResultsView('),controller.indexOf('function bindCollapsibles(')),context);
  for(const name of ['updateStatusBadge','updateSidebarSettings','updateResultsHeader','updateTabsVisibility',
    'setTableExpandVisibility','updateTableHeader','displaySummaryCards','renderWindowIndicators'])context[name]=()=>{};
  context.getWfaStitchedPeriodLabel=()=> '2024-01-01 to 2024-01-31';
  context.refreshResultsView();checkStitched();
  await click(document.querySelector('.wfa-stitched-row'));checkStitched();
  context.renderEquityChart=render;
  context.WFAResultsUI.renderWFAResultsTable(state.results,{});
  const wfaRows=document.querySelectorAll('.wfa-window-row');assert.equal(wfaRows.length,2);
  await click(wfaRows[1]);assert.equal(svg.children.length,0);assert.equal(axis.children.length,0);
  assert.equal(document.getElementById('comparisonLine').textContent,'Exact saved interval is unavailable.');
  context.fetch=async()=>ok({equity_curve:[740,760],timestamps:[dates[1],dates[2]],reference_capital:750});
  await click(wfaRows[1]);assert.equal(base(),130);assert.equal(points(),'0,240 800,20');
  assert.equal(document.getElementById('comparisonLine').textContent,'');
  const previous=points(), waiting=deferred();context.fetch=()=>waiting.promise;
  const pending=click(wfaRows[0]);await flush();
  state.wfaSelection={type:'stitched'};waiting.resolve({ok:false,json:async()=>({error:'old window'})});
  await pending;assert.equal(points(),previous);
  // WFA parameter await also cannot adopt a later window/period identity.
  state.strategyConfig=null;const details=deferred();context.fetchStrategyConfig=()=>details.promise;
  let fetches=0;context.fetch=async()=>{fetches++;return ok({equity_curve:[],timestamps:[]});};
  const parameters=click(wfaRows[1]);state.wfaSelection={windowNumber:2,period:'is'};
  details.resolve({parameters:{}});await parameters;assert.equal(fetches,0);assert.equal(points(),previous);
  // Opening a study/manual test invalidates in-flight chart actions immediately,
  // before its own asynchronous data load has updated ResultsState identities.
  vm.runInContext(controller.slice(controller.indexOf('let studyLoadVersion ='), controller.indexOf('const MissingCsvState =')), context);
  vm.runInContext(controller.slice(controller.indexOf('async function loadManualTestResultsById('), controller.indexOf('async function ensureManualTestSelection(')), context);
  let prior=context.captureEquitySelection(), loading=deferred();
  context.fetchStudyDetails=()=>loading.promise;
  context.applyStudyPayload=async()=>{};context.setQueryStudyId=()=>{};
  context.loadStudiesList=async()=>{};context.refreshResultsView=()=>{};
  const opening=context.openStudy('next-study');assert.equal(prior.current(),false);
  loading.resolve({study:{study_name:'Next'},csv_exists:true});await opening;
  prior=context.captureEquitySelection();loading=deferred();
  context.fetchManualTestResults=()=>loading.promise;
  const manual=context.loadManualTestResultsById(9);assert.equal(prior.current(),false);
  loading.resolve({id:9,results_json:{results:[]}});await manual;
  console.log('Results: capital bases, index/time spacing, CSV stage routing, pre-await identity, late JSON/errors, WFA refusal/recovery passed');
}
main().catch(error=>{console.error(error);process.exitCode=1;});
