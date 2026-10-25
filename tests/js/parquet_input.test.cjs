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
  constructor() { this.value = ''; this.children = []; this.handlers = {}; this.style = {}; this.classList = { toggle() {} }; }
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
  console.log('Parquet controller mode, stale response, picker layout state and edit events passed');
}
controllerChecks().catch(error => { console.error(error); process.exitCode = 1; });
