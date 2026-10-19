"use strict";
const assert = require('node:assert/strict');
const ui = require('../../tools/pattern_lab/account_study/report.js');
const rows = [{instrumentId:'BBB',x:2},{instrumentId:'CCC',x:null},{instrumentId:'AAA',x:2},{instrumentId:'DDD',x:-1}];
let state = ui.sortState({key:'',descending:true},'x');
assert.equal(state.descending,true);
let sorted=ui.orderTickers(rows,state.key,state.descending);
assert.deepEqual(sorted.map(r=>r.instrumentId),['AAA','BBB','DDD','CCC']);
state=ui.sortState(state,'x');
assert.equal(state.descending,false);
sorted=ui.orderTickers(rows,state.key,state.descending);
assert.deepEqual(sorted.map(r=>r.instrumentId),['DDD','AAA','BBB','CCC']);
assert.equal(ui.moveTicker(sorted.map(r=>r.instrumentId),'AAA',1),'BBB');
assert.equal(ui.moveTicker(sorted.map(r=>r.instrumentId),'DDD',-1),'DDD');
assert.equal(ui.moveTicker(sorted.map(r=>r.instrumentId),'CCC',1),'CCC');
assert.equal(ui.profitableCount(rows,'x'),'2/3');
assert.equal(ui.moveTicker([],null,1),null);
const ticker={},region={},other={};
assert.equal(ui.acceptsArrow(ticker,ticker,region),true);
assert.equal(ui.acceptsArrow(region,ticker,region),true);
assert.equal(ui.acceptsArrow(other,ticker,region),false);
assert.equal(rows[0].instrumentId,'BBB'); // sorting never mutates the canonical input
const fs=require('node:fs'),path=require('node:path');
const vectors=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/robustness_numbers.json'),'utf8'));
for(const v of vectors) {
  assert.equal(ui.closeNumber(v.a,v.b),v.close);
  assert.equal(ui.closeNumber(v.b,v.a),v.close);
}
assert.equal(ui.direction(1e-8),'flat');assert.equal(ui.direction(-1.01e-8),'down');
assert.equal(ui.direction(null),null);
assert.equal(ui.closeNumber(1e9,1e9+1),true);
assert.equal(ui.closeNumber(1,0),false); // tie original values, not their difference against zero
const month={month:'2020-01',startMs:0,endMs:10,returnPct:1,pairEligible:true,partialEdgeMonth:true,observedBars:1,expectedBars:1};
const aggregate=(allocationId,windowId)=>({allocationId,windowId,status:'available',initialCapital:200,plannedAccounts:2,
  returnPct:1,maxDrawdownMtmPct:2,netPnl:2,trades:3,timeInMarketPct:4,months:[{...month}],robustness:{passiveMonths:[{...month}]}});
const model={request:{instruments:['A','B']},summary:{robustness:{windows:[{windowId:'early'},{windowId:'late'}]},
  aggregates:[aggregate('b','late'),aggregate('c','late'),aggregate('b','early')],accounts:['b','c'].flatMap(allocationId=>['A','B'].map(instrumentId=>({allocationId,instrumentId,windowId:'late',coverage:{observedBars:1},returnPct:allocationId==='c'&&instrumentId==='A'?2:1})))}};
assert.deepEqual(ui.globalView(model,'b').map(w=>w.windowId),['early','late']);
assert.equal(ui.localView(model.summary,'late',['b']).aggregates.length,1);
assert.deepEqual(ui.globalView(model,'b').map(w=>w.windowId),['early','late']);
assert.equal(ui.monthlyMatrix(model,'b')[0].cells.length,2);
assert.equal(ui.pairView(model,'late','','c').status,'incompatible');
assert.equal(ui.pairView(model,'late','b','b').status,'incompatible');
let pair=ui.pairView(model,'late','b','c');
assert.deepEqual(pair.tickerCounts,{improved:1,tied:1,worse:0,valid:2,planned:2});
assert.equal(pair.tickers[0].delta,1);assert.equal(pair.monthCounts.tied,1);
assert.equal(pair.eligiblePartialEdgeMonths,1);
model.summary.aggregates[1].months[0].pairEligible=false;
pair=ui.pairView(model,'late','b','c');
assert.equal(pair.monthCounts.valid,0);assert.equal(pair.months[0].status,'excluded');
model.summary.aggregates[1].months[0].pairEligible=true;
model.summary.aggregates[1].months[0].startMs=1;
assert.equal(ui.pairView(model,'late','b','c').monthCounts.valid,0);
model.summary.aggregates[1].initialCapital=400;
assert.equal(ui.pairView(model,'late','b','c').status,'incompatible');
model.summary.aggregates[1].initialCapital=200;
model.summary.aggregates[1].status='unavailable';
assert.equal(ui.pairView(model,'late','b','c').status,'incompatible');
if (process.argv[2]) {
  const html=fs.readFileSync(process.argv[2],'utf8');
  const payload=html.match(/<script id="account-data" type="application\/json">([\s\S]*?)<\/script>/);
  assert.ok(payload);
  const data=JSON.parse(payload[1]);
  if (data.summary.schema==='pattern-lab.account-metrics.v3') {
    assert.equal(data.summary.auxiliary.schema,'pattern-lab.account-auxiliary-metadata.v1');
    assert.ok(data.summary.aggregates.every(a=>a.trades>0));
    assert.equal(ui.globalView(data,'baseline').length,data.request.windows.length);
    for(const w of data.request.windows) {
      const pair=ui.pairView(data,w.windowId,'baseline','funding');
      assert.equal(pair.status,'available');assert.equal(pair.tickerCounts.valid,data.request.instruments.length);
      const order=ui.orderTickers(pair.tickers,'delta',true).map(r=>r.instrumentId);
      assert.equal(ui.moveTicker(order,order[0],-1),order[0]);
    }
    assert.ok(html.includes('Auxiliary threshold-crossing entry signal'));
    assert.ok(html.includes('Combined all-of:'));
    assert.ok(html.includes('not exchange observations'));
    assert.ok(!html.includes('<script src='));
  } else {
  assert.equal(data.summary.schema,'pattern-lab.account-metrics.v2');
  assert.ok(data.summary.accounts.some(a=>a.trades>0));
  assert.equal(ui.globalView(data,'rr2').length,data.request.windows.length);
  const windows=ui.globalView(data,'rr2').map(w=>w.windowId);
  let tied=0,differing=0,excluded=0;
  for(const w of windows) {
    const p=ui.pairView(data,w,'rr2','rr3');assert.equal(p.status,'available');
    tied+=p.tickerCounts.tied;differing+=p.tickerCounts.improved+p.tickerCounts.worse;
    excluded+=p.monthCounts.planned-p.monthCounts.valid;
    assert.equal(p.tickerCounts.planned,data.request.instruments.length);
    const order=ui.orderTickers(p.tickers,'delta',true).map(r=>r.instrumentId);
    assert.equal(ui.moveTicker(order,order[0],-1),order[0]);
    assert.deepEqual(ui.globalView(data,'rr2').map(w=>w.windowId),windows);
  }
  assert.ok(tied>0 && differing>0 && excluded>0);
  assert.ok(ui.monthlyMatrix(data,'rr2').length>=3);
  assert.ok(html.includes('Accounting removal; no account re-execution'));
  assert.ok(!html.includes('<script src='));
  }
}
console.log('Account report interaction state checks passed (no browser/focus claim).');
