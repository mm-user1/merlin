"use strict";
// Arbitrary retained native reports; strict fixture expectations remain in the other harness.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const ui = require('../../tools/pattern_lab/account_study/report.js');
assert.ok(process.argv[2], 'HTML path required');
const html = fs.readFileSync(process.argv[2], 'utf8');
const models = [...html.matchAll(/<script id="account-data" type="application\/json">([\s\S]*?)<\/script>/g)];
assert.equal(models.length, 1, 'exactly one account-data model required');
const data = JSON.parse(models[0][1]);
assert.ok(data && data.request && data.summary, 'missing report model');
const ids = (rows, key) => {
  assert.ok(Array.isArray(rows) && rows.length > 0, 'nonempty model roster required');
  const values = rows.map(row => key ? row[key] : row);
  assert.ok(values.every(v => typeof v === 'string' && v.length > 0));
  assert.equal(new Set(values).size, values.length, 'duplicate model IDs');
  return values;
};
const allocations = ids(data.request.allocations, 'allocationId');
const windows = ids(data.request.windows, 'windowId');
const tickers = ids(data.request.instruments);
const summary = data.summary;
assert.ok(['pattern-lab.account-metrics.v1', 'pattern-lab.account-metrics.v2', 'pattern-lab.account-metrics.v3'].includes(summary.schema));
const hasRobustness = summary.schema !== 'pattern-lab.account-metrics.v1';
if (hasRobustness) assert.deepEqual(ids(summary.robustness.windows, 'windowId').slice().sort(), windows.slice().sort());
assert.ok(Array.isArray(summary.accounts) && Array.isArray(summary.aggregates));
assert.equal(summary.aggregates.length, allocations.length * windows.length);
assert.equal(summary.accounts.length, allocations.length * windows.length * tickers.length);
const before = JSON.stringify(data);
let availablePairs = 0;
for (const allocation of allocations) {
  if (hasRobustness) {
  const global = ui.globalView(data, allocation);
  assert.deepEqual(global.map(w => w.windowId), summary.robustness.windows.map(w => w.windowId));
  assert.ok(global.every(w => w.aggregate && w.aggregate.allocationId === allocation));
  const matrix = ui.monthlyMatrix(data, allocation);
  const months = [...new Set(global.flatMap(w => w.aggregate.robustness.passiveMonths.map(m => m.month)))].sort();
  assert.deepEqual(matrix.map(m => m.month), months);
  for (const row of matrix) assert.deepEqual(row.cells.map(c => c.windowId), global.map(w => w.windowId));
  }
  for (const window of windows) {
    const local = ui.localView(summary, window, [allocation]);
    assert.equal(local.aggregates.length, 1);
    assert.equal(local.accounts.length, tickers.length);
    assert.deepEqual(local.accounts.map(a => a.instrumentId).sort(), tickers.slice().sort());
    assert.ok(local.accounts.every(a => a.allocationId === allocation && a.windowId === window));
    assert.deepEqual(ui.localView(summary, window, []), {accounts: [], aggregates: []});
    const order = ui.orderTickers(local.accounts, 'returnPct', true).map(a => a.instrumentId);
    assert.equal(ui.moveTicker(order, order[0], -1), order[0]);
    assert.equal(ui.moveTicker(order, order[order.length - 1], 1), order[order.length - 1]);
    if (order.length > 1) assert.equal(ui.moveTicker(order, order[0], 1), order[1]);
    if (!hasRobustness) continue;
    assert.equal(ui.pairView(data, window, allocation, allocation).status, 'incompatible');
    assert.equal(ui.pairView(data, window, '', allocation).status, 'incompatible');
    for (const other of allocations.filter(a => a !== allocation)) {
      const b = local.aggregates[0];
      const c = summary.aggregates.find(a => a.windowId === window && a.allocationId === other);
      assert.ok(c, 'missing aggregate');
      const compatible = b.status === 'available' && c.status === 'available' &&
        b.plannedAccounts === c.plannedAccounts && b.initialCapital / b.plannedAccounts === c.initialCapital / c.plannedAccounts;
      const pair = ui.pairView(data, window, allocation, other);
      assert.equal(pair.status, compatible ? 'available' : 'incompatible');
      if (!compatible) continue;
      availablePairs++;
      assert.deepEqual(pair.tickers.map(r => r.instrumentId), tickers.slice().sort());
      for (const [rows, counts] of [[pair.tickers, pair.tickerCounts], [pair.months, pair.monthCounts]]) {
        assert.equal(counts.planned, rows.length);
        for (const state of ['improved', 'tied', 'worse']) assert.equal(counts[state], rows.filter(r => r.status === state).length);
        assert.equal(counts.valid, counts.improved + counts.tied + counts.worse);
      }
    }
  }
}
assert.equal(JSON.stringify(data), before, 'UI helpers mutated the embedded model');
assert.ok(!/<script\b[^>]*\bsrc\s*=/i.test(html), 'report must be self-contained');
console.log(JSON.stringify({status: 'passed', allocations: allocations.length, windows: windows.length,
  instruments: tickers.length, availablePairs, browserOrFocusClaim: false}));
