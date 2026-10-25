const assert = require('node:assert/strict');
const { parseDuration, label, validSource, identity, Selection } = require('../../src/ui/static/js/parquet-input.js');

for (const [text, value, canonical] of [['60m', 60, '1h'], [' 3d ', 4320, '3D'], ['90m', 90, '90m'], ['48h', 2880, '2D']]) {
  assert.equal(parseDuration(text), value); assert.equal(label(value), canonical);
}
for (const invalid of ['0m', '-5m', '+5m', '1.5h', '1M', '1H', '7m', '9007199254740992h', '9999999999999D', '5']) {
  assert.throws(() => parseDuration(invalid));
}
const model = new Selection();
model.reset([{ instrument_id: 'OKX_BTC-USDT-SWAP' }, { instrument_id: 'BYBIT_BTCUSDT' }]);
assert.throws(() => model.sources('/pack'));
model.selected.add(0); model.selected.add(1);
assert.throws(() => model.sources('/pack'));
model.bulk(3); model.checked[0][1] = true; model.checked[0][2] = true;
assert.deepEqual(model.sources('/pack').map(s => s.timeframe_minutes), [15, 30, 60, 60]);
const snapshot = model.sources('/pack');
assert.throws(() => model.commit(0, '1h'));
assert.equal(model.durations[0], 5);
assert.throws(() => model.sources('/pack'));
model.cancel(0);
model.commit(0, '3D'); assert.equal(model.durations[0], 4320);
assert.equal(snapshot[2].timeframe_minutes, 60);
model.checked[0][3] = false;
model.bulk(3); assert(model.checked.every(row => row[3]));
model.bulk(3); assert(model.checked.every(row => !row[3]));
model.reset(model.instruments); assert.equal(model.selected.size, 0); assert(model.checked.every(row => row.every(n => !n)));
assert.equal(model.durations[0], 4320);
assert(snapshot.every(validSource));
assert.equal(new Map([...snapshot, ...snapshot].map(s => [identity(s), s])).size, 4);
assert.notEqual(identity(snapshot[2]), identity(snapshot[3]));
assert(!validSource({ ...snapshot[0], version: 9 }));
assert(!validSource({ ...snapshot[0], timeframe_minutes: 1.5 }));
const added = Array.from({ length: 9 }, (_, i) => ({ ...snapshot[0], timeframe_minutes: (i + 1) * 5 }));
assert.equal(new Map(added.map(s => [identity(s), s])).size, 9);
console.log('Parquet timeframe and selection contracts passed');

// Exercise the browser controller with a small DOM stand-in; real focus/layout
// acceptance is separate. Deferred fetches make response ordering deterministic.
const fs = require('node:fs');
const vm = require('node:vm');
class Element {
  constructor() { this.value = ''; this.children = []; this.handlers = {}; this.style = {}; this.classList = { toggle() {}, remove() {} }; }
  append(...nodes) { this.children.push(...nodes); }
  appendChild(node) { this.append(node); }
  replaceChildren() { this.children = []; }
  addEventListener(name, callback) { this.handlers[name] = callback; }
  setAttribute() {}
  removeAttribute() {}
  select() { this.textSelected = true; }
  querySelectorAll() { return []; }
  get firstChild() { return this.children[0]; }
}
async function controllerChecks() {
  const elements = new Map();
  const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
  const requests = [];
  const context = {
    document: { getElementById: get, createElement: () => new Element(), querySelector: () => null, addEventListener() {} },
    sessionStorage: { getItem: () => null, setItem() {} },
    csvBrowserState: {}, showCsvBrowserError() {}, renderCsvBrowserEntries() {},
    renderSelectedFiles() {}, closeCsvBrowserModal() {},
    fetch: url => new Promise(resolve => requests.push({ url, resolve })),
  };
  context.window = context;
  const queueSource = fs.readFileSync(require.resolve('../../src/ui/static/js/queue.js'), 'utf8');
  for (const [signature, next] of [
    ['function getPathParentDirectory(', 'async function applyQueueDatabaseTarget('],
    ['function applyQueueUiSnapshot(', 'function triggerControlEvent('],
    ['async function loadQueueItemIntoForm(', 'function extractQueueStrategyShortLabel('],
  ]) {
    const start = queueSource.indexOf(signature), end = queueSource.indexOf(next, start);
    assert(start >= 0 && end > start);
    vm.runInNewContext(queueSource.slice(start, end), context);
  }
  vm.runInNewContext(fs.readFileSync(require.resolve('../../src/ui/static/js/parquet-input.js'), 'utf8'), context);
  const picker = context.ParquetInput;
  get('csvDirectory').value = '/csv';
  picker.setMode(true); get('csvDirectory').value = '/pack';
  picker.setMode(false); assert.equal(get('csvDirectory').value, '/csv');
  picker.setMode(true); assert.equal(get('csvDirectory').value, '/pack');
  const old = picker.load('/old');
  const newer = picker.load('/new');
  const payload = path => ({ current_path: path, entries: [], pack: { instruments: [
    { instrument_id: 'TEST_AAA', first_open_utc: '2024-01-01', coverage_end_utc: '2024-01-03',
      verification: { closed_before_utc: '2024-01-03' }, missing_bar_count: 0 }
  ] } });
  requests[1].resolve({ ok: true, json: async () => payload('/new') }); await newer;
  assert.equal(context.csvBrowserState.currentPath, '/new');
  assert.equal(get('csvBrowserList').hidden, true);
  assert.equal(get('parquetGrid').hidden, false);
  requests[0].resolve({ ok: true, json: async () => payload('/old') }); await old;
  assert.equal(context.csvBrowserState.currentPath, '/new');
  const table = get('parquetGrid').firstChild;
  const edit = table.firstChild.children[1].children[1];
  edit.handlers.focus(); assert.equal(edit.textSelected, true);
  edit.value = '90m'; edit.handlers.input(); edit.handlers.blur();
  assert.equal(picker.model.durations[0], 90);
  edit.value = '1h'; edit.handlers.input(); edit.handlers.blur();
  assert.equal(picker.model.invalid.size, 1);
  edit.handlers.keydown({ key: 'Escape', preventDefault() {}, stopPropagation() {} });
  assert.equal(edit.value, '90m'); assert.equal(picker.model.invalid.size, 0);
  const duringSwitch = picker.load('/ignored'); picker.setMode(false);
  requests[2].resolve({ ok: true, json: async () => payload('/ignored') }); await duringSwitch;
  assert.equal(context.csvBrowserState.currentPath, '/new');
  const savedSource = { type: 'parquet', version: 1, pack_root: '/new', instrument_id: 'TEST_AAA', timeframe_minutes: 4320 };
  picker.restore([savedSource]); assert.equal(picker.active(), true);
  assert.equal(picker.sources()[0].timeframe_minutes, 4320);
  assert.throws(() => picker.restore([savedSource, { type: 'path', path: '/csv' }]));
  assert.equal(picker.sources().length, 1);
  // Run the actual Queue entrypoint and source controller together; source fields
  // in generic snapshots deliberately contradict the typed Queue selection.
  let item;
  Object.assign(context, {
    ensureQueueStateLoaded: async () => {}, ensureQueueItemStrategyLoaded: async () => {},
    loadQueue: () => ({ items: [item] }), getQueueSources: value => value.sources,
    setSelectedCsvPaths: paths => { context.selectedCsvPaths = paths; },
    applyQueueConfigFallback() {}, applyQueueDatabaseTarget: async () => {},
    buildQueueStudySetState: () => ({}), syncQueueAutoCreateSetUi() {}, refreshQueueFormUiAfterApply() {},
    queueRunning: false,
  });
  get('useParquet').type = 'checkbox';
  get('csvDirectory').value = '/remembered-pack';
  const csvSources = [{ type: 'path', path: 'C:\\restored-csv\\first.csv' }, { type: 'path', path: 'C:\\restored-csv\\second.csv' }];
  async function restoreItem(sources, snapshotMode) {
    item = { id: 'restore', sources, uiSnapshot: { controls: {
      csvDirectory: { value: '/wrong-snapshot' }, useParquet: { checked: snapshotMode },
    } } };
    await context.loadQueueItemIntoForm('restore');
  }
  await restoreItem(csvSources, true);
  assert.equal(picker.active(), false); assert.equal(get('useParquet').checked, false);
  assert.equal(get('csvDirectory').value, 'C:\\restored-csv');
  assert.equal(picker.state.paths.csv, 'C:\\restored-csv');
  assert.equal(picker.state.paths.parquet, '/remembered-pack');
  assert.deepEqual(Array.from(context.selectedCsvPaths), csvSources.map(s => s.path));
  await restoreItem(csvSources, true); // Same mode preserves both paths too.
  assert.equal(picker.state.paths.parquet, '/remembered-pack');
  await restoreItem([savedSource, { ...savedSource, timeframe_minutes: 5 }], false);
  assert.equal(picker.active(), true); assert.equal(get('useParquet').checked, true);
  assert.equal(get('csvDirectory').value, '/new');
  assert.equal(picker.state.paths.csv, 'C:\\restored-csv');
  assert.equal(picker.state.paths.parquet, '/new');
  assert.deepEqual(Array.from(picker.sources(), s => s.timeframe_minutes), [4320, 5]);
  await restoreItem([savedSource], false); // Same-mode Parquet restore.
  assert.equal(get('csvDirectory').value, '/new');
  const beforeInvalid = JSON.stringify({ state: picker.state, directory: get('csvDirectory').value });
  await assert.rejects(restoreItem([savedSource, csvSources[0]], false), /Mixed or invalid/);
  assert.equal(JSON.stringify({ state: picker.state, directory: get('csvDirectory').value }), beforeInvalid);
  picker.setMode(false); assert.equal(get('csvDirectory').value, 'C:\\restored-csv');
  console.log('Parquet controller mode, stale response, picker layout state and edit events passed');
  console.log('Queue entrypoint restores both directories, mode and selected pairs despite contradictory snapshots');
}
controllerChecks().catch(error => { console.error(error); process.exitCode = 1; });

// A rejected pinned replay must never leave the preceding result's curve/axis.
async function replayChartChecks() {
  class Node {
    constructor(tag) { this.tag = tag; this.children = []; this.style = {}; this.attributes = {}; }
    set innerHTML(value) { assert.equal(value, ''); this.children = []; }
    appendChild(child) { this.children.push(child); }
    setAttribute(name, value) { this.attributes[name] = value; }
  }
  const svg = new Node('svg'), axis = new Node('axis'), comparison = new Node('comparison');
  let response;
  const context = {
    ResultsState: { studyId: 'old-metadata', hasDataInput: true, activeTab: 'oos_test' },
    document: {
      querySelector: selector => selector === '.chart-svg' ? svg : null,
      getElementById: id => ({ equityAxis: axis, comparisonLine: comparison })[id],
      createElement: tag => new Node(tag), createElementNS: (_, tag) => new Node(tag),
    },
    fetch: async () => response,
  };
  for (const [file, signature, next] of [
    ['results-tables.js', 'function setComparisonLine(', 'function renderManualTestControls('],
    ['results-tables.js', 'function renderEquityChart(', 'function calculateWindowBoundaries('],
    ['results-controller.js', 'async function fetchEquityCurve(', 'function refreshResultsView('],
  ]) {
    const source = fs.readFileSync(require.resolve('../../src/ui/static/js/' + file), 'utf8');
    const start = source.indexOf(signature), end = source.indexOf(next, start);
    assert(start >= 0 && end > start);
    vm.runInNewContext(source.slice(start, end), context);
  }
  const payload = { equity: [100, 112], timestamps: ['2024-01-17T00:00:00Z', '2024-01-20T00:00:00Z'] };
  context.renderEquityChart(payload.equity, null, payload.timestamps);
  assert(svg.children.some(node => node.tag === 'polyline'));
  assert.equal(axis.children.length, 2);
  const error = 'This study predates exact OOS replay metadata; a new study is required for OOS reconstruction.';
  response = { ok: false, json: async () => ({ error }) };
  assert.equal(await context.fetchEquityCurve({ trial_number: 1 }), null);
  assert.equal(svg.children.length, 0);
  assert.equal(axis.children.length, 0);
  assert.equal(comparison.textContent, error);
  assert.equal(comparison.style.display, 'flex');
  // Returning to a supported result can render a fresh curve and date labels.
  context.setComparisonLine('');
  response = { ok: true, json: async () => payload };
  const replay = await context.fetchEquityCurve({ trial_number: 1 });
  context.renderEquityChart(replay.equity, null, replay.timestamps);
  assert(svg.children.some(node => node.tag === 'polyline'));
  assert.equal(axis.children.length, 2);
  assert.equal(comparison.style.display, 'none');
  console.log('Pinned replay refusal clears the previous curve and dates; successful rendering recovers');
}
replayChartChecks().catch(error => { console.error(error); process.exitCode = 1; });
