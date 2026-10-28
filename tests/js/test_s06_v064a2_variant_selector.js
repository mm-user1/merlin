'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const repoRoot = path.resolve(__dirname, '..', '..');
const strategySource = fs.readFileSync(
  path.join(repoRoot, 'src', 'ui', 'static', 'js', 'strategy-config.js'), 'utf8',
);
const handlersSource = fs.readFileSync(
  path.join(repoRoot, 'src', 'ui', 'static', 'js', 'ui-handlers.js'), 'utf8',
);
const config = JSON.parse(fs.readFileSync(0, 'utf8'));

function sourceSlice(source, startSignature, endSignature) {
  const start = source.indexOf(startSignature);
  const end = source.indexOf(endSignature, start);
  assert.notEqual(start, -1, `Missing ${startSignature}`);
  assert.notEqual(end, -1, `Missing ${endSignature}`);
  return source.slice(start, end);
}

const allElements = [];
const byId = new Map();

function matches(element, selector) {
  if (selector === '.opt-param-toggle') return element.classList.contains('opt-param-toggle');
  if (selector === 'input[name="gridEnabledMode"]') {
    return element.tagName === 'INPUT' && element.name === 'gridEnabledMode';
  }
  if (selector === 'input[name="gridAllocationMethod"]') {
    return element.tagName === 'INPUT' && element.name === 'gridAllocationMethod';
  }
  if (selector === '#gridV2ManualAllocation input[data-grid-block-name]') return false;
  const optionMatch = selector.match(/^input\.select-option-checkbox\[data-param-name="(.+)"\]/);
  if (optionMatch) {
    return element.tagName === 'INPUT'
      && element.classList.contains('select-option-checkbox')
      && element.dataset.paramName === optionMatch[1]
      && element.dataset.optionValue !== '__ALL__';
  }
  return false;
}

class Element {
  constructor(tagName = 'div') {
    this.tagName = tagName.toUpperCase();
    this.children = [];
    this.dataset = {};
    this.style = {};
    this.checked = false;
    this.disabled = false;
    this.value = '';
    this.name = '';
    this.type = '';
    this._id = '';
    this._className = '';
    this.classList = {
      add: (...names) => {
        const current = new Set(this._className.split(/\s+/).filter(Boolean));
        names.forEach((name) => current.add(name));
        this._className = [...current].join(' ');
      },
      contains: (name) => this._className.split(/\s+/).includes(name),
    };
    allElements.push(this);
  }
  set id(value) { this._id = value; if (value) byId.set(value, this); }
  get id() { return this._id; }
  set className(value) { this._className = value; }
  get className() { return this._className; }
  set innerHTML(value) { this.children = []; this._innerHTML = value; }
  get innerHTML() { return this._innerHTML || ''; }
  appendChild(child) { this.children.push(child); return child; }
  addEventListener() {}
  querySelectorAll(selector) {
    const descendants = [];
    const visit = (node) => {
      (node.children || []).forEach((child) => { descendants.push(child); visit(child); });
    };
    visit(this);
    return descendants.filter((element) => matches(element, selector));
  }
}

const document = {
  createElement: (tagName) => new Element(tagName),
  createTextNode: (text) => ({textContent: text, children: []}),
  getElementById: (id) => byId.get(id) || null,
  querySelectorAll: (selector) => allElements.filter((element) => matches(element, selector)),
};

function addElement(id, value = '') {
  const element = new Element('div');
  element.id = id;
  element.value = value;
  return element;
}

addElement('optimizerParamsContainer');
addElement('gridEnabledModes');
addElement('gridProfileModesSection');
addElement('gridV2PlanningPolicy', 'full');
addElement('gridSeed', '42');
addElement('gridTopCandidates', '10');
addElement('gridMinQuota', '0.1');
addElement('gridDiversityMaxPerGroup', '2');
const allocation = new Element('input');
allocation.name = 'gridAllocationMethod';
allocation.value = 'auto_sqrt_space';
allocation.checked = true;

const context = {
  window: {
    currentStrategyId: config.id,
    currentStrategyConfig: config,
    bindOptimizerInputs() {},
  },
  document,
  console,
  collectDynamicBacktestParams: () => ({}),
  getMinProfitElements: () => ({}),
  getWorkerProcessesValue: () => 1,
  collectScoreConfig: () => ({}),
  buildOptunaConfig: () => ({}),
  getGridBudgetValue: () => 200000,
  collectGridObjectiveSelection: () => ({
    objectives: ['net_profit_pct'], primary_objective: 'net_profit_pct',
  }),
  scheduleGridPreviewUpdate() {},
  setGridPreviewError() {},
};

vm.createContext(context);
vm.runInContext(
  "const GLOBAL_BACKTEST_CONTROL_PARAM_NAMES = new Set(['dateFilter', 'start', 'end', 'warmupBars']);\n"
    + 'function isGlobalBacktestControlParam(name) { return GLOBAL_BACKTEST_CONTROL_PARAM_NAMES.has(String(name || \'\')); }\n'
    + sourceSlice(strategySource, 'let parameterTieState', 'const STRATEGY_CONFIG_NOT_READY_MESSAGE')
    + sourceSlice(strategySource, 'function orderedParameterEntries(', 'function generateBacktestForm(')
    + sourceSlice(strategySource, 'function generateOptimizerForm(', 'function createFormField(')
    + sourceSlice(handlersSource, 'function getBacktestParamValue(', 'function getWorkerProcessesValue(')
    + sourceSlice(handlersSource, 'function getEnabledGridMetadata()', 'function readSelectedOptimizerOptionValues(')
    + sourceSlice(handlersSource, 'function readSelectedOptimizerOptionValues(', 'function validateOptimizerForm(')
    + sourceSlice(handlersSource, 'function buildOptimizationConfig(', 'function buildOptunaConfig(')
    + sourceSlice(handlersSource, 'function buildGridConfig(', 'function getCurrentPlannedGridCandidates(')
    + '\nthis.__test = {generateOptimizerForm, syncGridProfileUi, buildGridConfig};',
  context,
);

context.__test.generateOptimizerForm(config);
assert.equal(document.getElementById('opt-trailMode'), null);
assert.equal(
  document.querySelectorAll('.opt-param-toggle').some((element) => element.dataset.paramName === 'trailMode'),
  false,
);

context.__test.syncGridProfileUi();
const modeInputs = document.querySelectorAll('input[name="gridEnabledMode"]');
assert.deepEqual(
  modeInputs.map((element) => element.value),
  ['bracket', 'r_trail', 'chandelier', 'fixed_af_sar'],
);
modeInputs.forEach((element) => { element.checked = element.value === 'r_trail'; });

const generated = context.__test.buildGridConfig({
  payload: {dateFilter: false}, start: null, end: null,
});
assert.equal(Object.hasOwn(generated.enabled_params, 'trailMode'), false);
assert.equal(generated.fixed_params.trailMode, 'R Trail');
assert.deepEqual(Array.from(generated.grid_enabled_modes), ['r_trail']);

modeInputs.find((element) => element.value === 'chandelier').checked = true;
const combined = context.__test.buildGridConfig({
  payload: {dateFilter: false}, start: null, end: null,
});
assert.deepEqual(Array.from(combined.grid_enabled_modes), ['r_trail', 'chandelier']);

console.log('S06 v06-4-A2 variant selector UI behavior: OK');
