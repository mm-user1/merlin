const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const source = name => fs.readFileSync(path.join(root, 'src/ui/static/js', name), 'utf8');
require('../../src/ui/static/js/entry-diagnostics.js');
const D = globalThis.EntryDiagnostics;
const value = (maximum, skipped, scope='is') => ({version:1, scope, maxEntryLeverageUsed:maximum, leverageCapSkippedEntries:skipped});
assert.equal(D.format(value(7,26)), '7.00× / 26');
assert.equal(D.format(value(null,0)), '— / 0');
assert.equal(D.format(value(null,26)), '— / 26');
for (const invalid of [null, {}, value(Infinity,0), value(0,0), value(2,-1), value(2,1.5)]) assert.equal(D.format(invalid),'N/A');
assert(!D.card(value(7,26)).includes('title='));
const a = {study_id:'a',entry_diagnostics:value(7,2,'oos')};
const b = {study_id:'b',entry_diagnostics:value(3,4,'oos')};
assert.deepEqual(D.aggregate([a,b,a]), value(7,6,'oos'));
assert.equal(D.aggregate([a,{study_id:'old'}]),null);

const elements = new Map();
class Element {
  constructor() { this.children=[]; this.style={}; this.dataset={}; this.value=''; this.type=''; this.innerHTML=''; this.classList={add(){},remove(){},toggle(){}}; }
  set id(value) { this._id=value; elements.set(value,this); }
  get id() { return this._id; }
  appendChild(node) { this.children.push(node); return node; }
  addEventListener() {}
  setAttribute(name,value) { this[name]=value; }
  querySelector() { return null; }
  querySelectorAll() { return []; }
}
const box = new Element();
let shown;
const state = {mode:'grid', activeTab:'optuna', results:[{trial_number:1,params:{maxEntryLeverage:8},entry_diagnostics:{is:value(2,3),ft:value(4,5,'ft'),oos_test:value(6,7,'oos_test')}}],manualTestResults:[{trial_number:1,entry_diagnostics:value(3,8,'manual')}],fixedParams:{}};
const sandbox = {console, document:{getElementById:id=>elements.get(id),querySelector:()=>box,querySelectorAll:()=>[],createElement:()=>new Element(),addEventListener(){}},
  window:{EntryDiagnostics:{...D,show:(v,only)=>{shown={v,only};}}}, ResultsState:state,
  setComparisonLine(){},renderEquityChart(){},formatParamValue:String,formatParamName:String,createParamId:()=>'',fetch:()=>{}};
sandbox.window.ResultsState=state;
vm.createContext(sandbox);
vm.runInContext(source('strategy-config.js'),sandbox);
for (const id of ['backtestParamsContent','optimizerParamsContainer','paramDetailsContent','paramDetailsSection','paramDetailsTitle']) { const e=new Element(); e.id=id; }
const cap = {type:'float',role:'execution',default:8,label:'Max Entry Leverage',group:'Risk',optimize:{enabled:false}};
const config={id:'test',execution:{entryAdmission:'risk_bracket_v1'},parameters:{maxEntryLeverage:cap}};
sandbox.window.currentStrategyConfig=config;
sandbox.generateBacktestForm(config);
assert.equal(elements.get('backtest_maxEntryLeverage').value,8);
assert.equal(elements.get('backtestParamsContent').children[0].children[0].textContent,'Risk');
sandbox.generateOptimizerForm(config);
assert.equal(elements.has('opt-maxEntryLeverage'),false);
elements.delete('backtest_maxEntryLeverage');
sandbox.generateBacktestForm({...config,execution:{}});
assert.equal(elements.has('backtest_maxEntryLeverage'),false);
sandbox.generateBacktestForm(config);
const queue=source('queue.js');
vm.runInContext(queue.slice(queue.indexOf('function collectQueueUiSnapshot()'),queue.indexOf('function triggerControlEvent(')),sandbox);
sandbox.document.querySelectorAll=()=>[elements.get('backtest_maxEntryLeverage')];
elements.get('backtest_maxEntryLeverage').value='2.5';
const snapshot=sandbox.collectQueueUiSnapshot();
elements.get('backtest_maxEntryLeverage').value='8';
sandbox.applyQueueUiSnapshot(snapshot);
assert.equal(elements.get('backtest_maxEntryLeverage').value,'2.5');
sandbox.document.querySelectorAll=()=>[];
vm.runInContext(source('results-tables.js'),sandbox);
for (const [tab,key] of [['optuna','is'],['stress_test','is'],['forward_test','ft'],['oos_test','oos_test']]) {
  state.activeTab=tab; sandbox.selectTableRow(0,1);
  assert.deepEqual(shown.v,state.results[0].entry_diagnostics[key]); assert.equal(shown.only,true);
}
state.activeTab='manual_tests'; sandbox.selectTableRow(0,1); assert.equal(shown.v.scope,'manual');
sandbox.selectTableRow(0,999); assert.equal(shown.v,undefined);
state.strategyConfig=config;
const wfa=source('wfa-results-ui.js');
sandbox.WFAState={windowTrials:{1:{modules:{optuna_is:[{trial_number:1,module_metrics:{entry_diagnostics:value(2,3)}}]}}}};
sandbox.getWindowData=()=>({entry_diagnostics:{is:value(1,2),oos:value(3,4,'oos')}});
vm.runInContext(wfa.slice(wfa.indexOf('  function setWfaSelection('),wfa.indexOf('  function selectRow(')),sandbox);
state.stitched_oos={entry_diagnostics:value(5,9,'oos')};
for (const [selection,expected] of [[{type:'stitched'},5],[{windowNumber:1,period:'is'},1],[{windowNumber:1,period:'both'},3],[{windowNumber:1,period:'oos'},3],[{windowNumber:1,moduleType:'optuna_is',trialNumber:1},2]]) {
  sandbox.setWfaSelection(selection); assert.equal(shown.v.maxEntryLeverageUsed,expected);
}

async function asyncChecks() {
  // Direct backtest download reuses the returned cap/rules, including default resolution.
  const handlers = source('ui-handlers.js');
  vm.runInContext(handlers.slice(handlers.indexOf('async function executeBacktestRun('), handlers.indexOf('async function runBacktest(event)')),sandbox);
  for (const id of ['results','error']) { const e=new Element(); e.id=id; }
  sandbox.getSelectedDataSources=()=>[{type:'parquet',pack_root:'/synthetic'}];
  sandbox.isAbsoluteFilesystemPath=()=>true;
  sandbox.gatherFormState=()=>({payload:{}});
  sandbox.window.currentStrategyId='test';
  sandbox.isCurrentStrategyConfigReady=()=>true;
  sandbox.renderSelectedFiles=()=>{};
  sandbox.appendDataSource=()=>{};
  sandbox.buildBacktestRequestFormData=(path,params)=>new Map([['payload',params]]);
  const originalBuild=sandbox.buildBacktestRequestFormData;
  sandbox.buildBacktestRequestFormData=(path,params)=>{const form=originalBuild(path,params);form.append=form.set;return form;};
  const returned={data_input:{version:1},entry_admission:{version:1,maxEntryLeverage:8},parameters:{maxEntryLeverage:8}};
  sandbox.runBacktestRequest=async()=>returned;
  sandbox.formatResultBlock=()=>'';
  let downloaded;
  sandbox.downloadBacktestTradesRequest=async form=>{downloaded=form;return {};};
  sandbox.triggerDownloadFromResponse=async()=>{};
  await sandbox.executeBacktestRun({downloadTrades:true});
  assert.deepEqual(downloaded.get('payload'),returned.parameters);
  assert.deepEqual(JSON.parse(downloaded.get('entryAdmission')),returned.entry_admission);
  assert.deepEqual(JSON.parse(downloaded.get('dataInput')),returned.data_input);
  await sandbox.showParameterDetails(state.results[0]);
  const item=elements.get('paramDetailsContent').children[0].children[1];
  assert.equal(item.children[0].textContent,'Max Entry Leverage');
  assert.equal(item.children[1].textContent,'8');
  assert.equal(item.className,'param-item param-fixed');
  const controller=source('results-controller.js');
  vm.runInContext(controller.slice(controller.indexOf('async function fetchEquityCurve('),controller.indexOf('function refreshResultsView(')),sandbox);
  let release;
  sandbox.fetch=()=>new Promise(resolve=>{release=resolve;});
  state.studyId='a';state.activeTab='optuna';state.hasDataInput=true;state.selectedRowId=1;
  const pending=sandbox.fetchEquityCurve({trial_number:1});
  state.studyId='b'; shown={v:value(9,0)};
  release({ok:false,json:async()=>({error:'old failure'})});
  assert.equal(await pending,null);
  assert.equal(shown.v.maxEntryLeverageUsed,9);
  sandbox.displaySummaryCards({entry_diagnostics:value(5,2,'oos')});
  assert(box.innerHTML.includes('OOS WIN RATE (MED)'));
  assert.equal(shown.v.scope,'oos');
  console.log('Admission form, Queue, scope, formatting, deduplication and stale-response checks passed');
}
asyncChecks().catch(error=>{console.error(error);process.exitCode=1;});
