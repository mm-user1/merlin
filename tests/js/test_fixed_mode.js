'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '../..');
const read = name => fs.readFileSync(path.join(root, 'src/ui/static/js', name), 'utf8');
const handlers = read('ui-handlers.js');
function slice(source, start, end) {
  const a = source.indexOf(start), b = source.indexOf(end, a + start.length);
  assert(a >= 0 && b > a, start);
  return source.slice(a, b);
}
const elements = new Map();
function element(id, value = '', type = 'number') {
  const control = {id, value, type, checked: false, disabled: false, dataset: {}, style: {},
    classList: {add(){}, remove(){}, toggle(){}}, addEventListener(){},
    closest(){return {querySelectorAll: () => [control]};}};
  elements.set(id, control); return control;
}
for (const mode of ['Grid', 'Fixed', 'Optuna']) element('optimizerMode'+mode, mode.toLowerCase(), 'radio');
for (const id of ['optimizerModeOptunaLabel','optimizerModeFixedLabel','gridSettings','optunaSettings','gridModeHelp']) element(id);
const scalar = element('backtest_length', '7');
const selector = element('backtest_trailMode', 'Chandelier Exit', 'select');
const axis = element('opt-length', '', 'checkbox'); axis.checked = true;
const range = element('range', 'invalid');
for (const id of ['gridBudget', 'gridTopCandidates','gridSeed']) element(id, 'bad');
for (const id of ['enablePostProcess','enableDSR','enableStressTest','enableAdaptiveWF','gridSlowRefinementEnabled','gridStrictValidation','minProfitFilter','scoreFilter']) {
  element(id, '', 'checkbox').checked = true;
}
element('warmupBars', '100');
const config = {id:'synthetic', engine:'v2', parameters:{length:{type:'int',default:5}, trailMode:{type:'select', default:'Off (Bracket)'}}};
const c = {console, AbortController, FormData, Map, Set, JSON, Date,
  window:{currentStrategyConfig:config, currentStrategyId:'synthetic'},
  document:{getElementById:id=>elements.get(id)||null, addEventListener(){},
    querySelector: q => q === 'input[name="optimizerMode"]:checked'
      ? ['Fixed','Grid','Optuna'].map(m=>elements.get('optimizerMode'+m)).find(e=>e.checked) : null,
    querySelectorAll:q=>q.includes('#gridSettings input') ? [range, elements.get('gridBudget'), elements.get('enableAdaptiveWF')] : []},
  getOptimizerParamElements:()=>[{name:'length', checkbox:axis, fromInput:range,toInput:range,stepInput:range,def:config.parameters.length}],
  getMinProfitElements:()=>({checkbox:elements.get('minProfitFilter')}),
  collectDynamicBacktestParams:()=>({}), isGlobalBacktestControlParam:()=>false,
  collectScoreConfig:()=>({filter_enabled:true}), getWorkerProcessesValue:()=>1,
  getEnabledGridMetadata:()=>({available:true}), getEnabledParameterTieGroups:()=>[],
  gatherFormState:()=>({payload:{dateFilter:false},start:'2024-01-01',end:'2024-02-01'}),
  clonePreset:value=>JSON.parse(JSON.stringify(value)),
  setInputValue(id,value){if(elements.has(id)) elements.get(id).value=value;},
  localStorage:{getItem:()=>null,removeItem(){}}, setTimeout, clearTimeout,
};
c.window.OosTestUI = {collectConfig:()=>({enabled:true,periodDays:3})};
vm.createContext(c);
for (const [start,end] of [
  ['function getBacktestParamValue(', 'function getWorkerProcessesValue('],
  ['function getOptimizerMode()', 'function parseCompactCount('],
  ['function validateOptimizerForm(', 'function collectOptimizerParams('],
  ['function buildOptimizationConfig(', 'function buildOptunaConfig('],
  ['function getCurrentPlannedGridCandidates(', 'function clearWFResults('],
  ['function syncOptimizerModeUI()', 'async function submitOptimization('],
  ['async function updateGridPreview()', 'function syncGridObjectiveAndConstraintUi('],
]) vm.runInContext(slice(handlers,start,end),c);
vm.runInContext(read('queue.js'),c);
for (const name of ['syncGridBudgetHelp','syncGridProfileUi','syncGridAllocationUi','syncGridParameterOptions',
  'syncGridObjectiveUi','syncGridObjectiveAndConstraintUi','scheduleGridPreviewUpdate','syncParameterTieControls']) c[name]=()=>{};
function choose(mode) {
  ['Grid','Fixed','Optuna'].forEach(name=>elements.get('optimizerMode'+name).checked=name.toLowerCase()===mode);
  c.syncOptimizerModeUI();
}
async function main() {
  choose('grid'); choose('fixed');
  assert.equal(c.getOptimizerMode(),'fixed');
  assert.equal(axis.disabled,true); assert.equal(range.disabled,true);
  assert.equal(scalar.disabled,false); assert.equal(selector.disabled,false);
  assert.equal(elements.get('enablePostProcess').disabled,true);
  assert.equal(elements.get('enableAdaptiveWF').disabled,true);
  const p=c.buildCurrentOptimizerConfig(c.gatherFormState());
  assert.equal(p.parameterMode,'fixed'); assert.equal(p.optimization_mode,'grid');
  assert.equal(p.fixed_params.length,7); assert.equal(p.fixed_params.trailMode,'Chandelier Exit');
  assert.equal(Object.keys(p.enabled_params).length,0); assert.equal(Object.keys(p.param_ranges).length,0);
  assert.equal(p.postProcess.enabled,false); assert.equal(p.score_config.filter_enabled,false);
  assert.equal(p.grid_slow_refinement_enabled,false); assert.equal(p.oosTest.enabled,true);
  assert.equal(p.grid_enabled_modes.length,0); assert.equal(c.validateOptimizerForm(config).length,0);
  choose('grid'); assert.equal(axis.disabled,false); assert.equal(axis.checked,true);
  assert.equal(elements.get('enablePostProcess').checked,true); assert.equal(range.value,'invalid');
  choose('fixed'); c.window.currentStrategyConfig={...config,id:'other'}; c.syncOptimizerModeUI();
  assert.equal(c.getOptimizerMode(),'fixed');
  c.window.currentStrategyConfig={...config,engine:'v1'}; c.syncOptimizerModeUI();
  assert.notEqual(c.getOptimizerMode(),'fixed');
  c.window.currentStrategyConfig=config; choose('fixed');
  // Live Preview uses the same Fixed builder and does not consult checked Grid axes.
  c.isCurrentStrategyConfigReady=()=>true;
  c.fetchGridPreviewRequest=async actual=>{assert.equal(actual.parameterMode,'fixed');return {planned_candidate_count:1};};
  c.updateGridPreviewDom=preview=>{c.window.lastGridPreview=preview;};
  c.setGridPreviewError=message=>{throw Error(message);};
  vm.runInContext('var gridPreviewSeq=0;',c);
  await c.updateGridPreview(); assert.equal(c.window.lastGridPreview.planned_candidate_count,1);
  c.STRATEGY_CONFIG_NOT_READY_MESSAGE = 'Strategy not ready';
  c.loadStrategyConfig = async () => false;
  await assert.rejects(c.ensureQueueItemStrategyLoaded({strategyId:'synthetic',config:p}), /Strategy not ready/);
  c.loadStrategyConfig = async () => true;
  c.window.currentStrategyConfig = {...config, engine:'v1'};
  await assert.rejects(c.ensureQueueItemStrategyLoaded({strategyId:'synthetic',config:p}), /requires a V2 strategy/);
  c.window.currentStrategyConfig = config;
  await c.ensureQueueItemStrategyLoaded({strategyId:'synthetic',config:p});
  c.collectQueueSources = () => [{type:'path',path:'C:/one.csv'}];
  c.isCurrentStrategyConfigReady = () => false;
  c.showQueueError = message => assert.equal(message, 'Strategy not ready');
  assert.equal(c.collectQueueItem(), null);
  c.isCurrentStrategyConfigReady = () => true;

  // Exercise Queue collection and restoration with a contradictory UI snapshot.
  let item;
  Object.assign(c, {collectQueueSources:()=>[{type:'path',path:'C:/one.csv'},{type:'path',path:'C:/two.csv'}],
    getDatabaseTargetValidationError:()=>null, loadQueue:()=>({items:item?[item]:[],nextIndex:1}),
    getQueueAutoCreateSetPreference:()=>false, collectQueueUiSnapshot:()=>({controls:{optimizerModeGrid:{checked:true},optimizerModeFixed:{checked:false}}}),
    showQueueError:message=>{throw Error(message);}, ensureQueueStateLoaded:async()=>{},
    ensureQueueItemStrategyLoaded:async()=>{}, setSelectedCsvPaths(){}, renderSelectedFiles(){},
    applyQueueConfigFallback(){}, applyQueueDatabaseTarget:async()=>{}, syncQueueAutoCreateSetUi(){},
    refreshQueueFormUiAfterApply:()=>c.syncOptimizerModeUI()});
  item=c.collectQueueItem(); assert(item); assert.equal(item.sources.length,2);
  assert.match(c.generateQueueLabel(item),/FIXED 1 cfg/);
  await c.loadQueueItemIntoForm(item.id); assert.equal(c.getOptimizerMode(),'fixed');
  item=JSON.parse(JSON.stringify(item)); // Refresh transports executable config unchanged.
  item.mode='wfa'; item.wfa={isPeriodDays:90,oosPeriodDays:30,adaptiveMode:false};
  assert.match(c.generateQueueLabel(item),/WFA-F 90\/30 \u00B7 FIXED 1 cfg/); item.mode='grid';
  const sent=[];
  Object.assign(c,{getQueueForUi:()=>({items:[item]}), countQueuePendingItems:()=>1,
    findNextPendingQueueItem:()=>item.done?null:item, saveQueueRuntimeState:async()=>{},
    setQueueControlsDisabled(){},updateRunButtonState(){},saveOptimizationState(){},updateOptimizationState(){},
    setActiveOptimizationRunId(){},openResultsPage(){},setQueueItemState(){},
    persistItemProgress:async(_id,patch)=>Object.assign(item,patch),
    finalizeQueueItem:async()=>{item.done=true;}, finalizeQueueStudySetIfNeeded:async(_item,studySet)=>({studySet}),
    generateOptimizationRunId:()=> 'fixed-test', isAbsoluteFilesystemPath:()=>true,
    runOptimizationRequest:async form=>{sent.push(JSON.parse(form.get('config')));return {};},
    renderQueue(){},setQueueUiVisible(){},clearOptimizationState(){},
  });
  await c.runQueue(); assert.equal(sent.length,2);
  sent.forEach(actual=>assert.deepEqual(actual,JSON.parse(JSON.stringify(p))));
  console.log('Fixed switch, scalar, disabled controls, Preview, Queue restore and two-source execution passed.');
}
main().catch(error=>{console.error(error);process.exitCode=1;});
