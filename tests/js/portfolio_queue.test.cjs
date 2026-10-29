'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.resolve(__dirname, '../../src/ui/static/js/queue.js'), 'utf8');

async function check(abort) {
  const item = {id:'portfolio', tickerSelector:{enabled:true}, mode:'wfa', sources:[{},{}]};
  const queue = {items:[item]}, events = [], updates = [];
  let executed = 0, active = '';
  const context = {console, AbortController, window:{}, document:{getElementById(){return null;}}};
  vm.createContext(context);
  vm.runInContext(source, context);
  Object.assign(context, {
    ensureQueueStateLoaded:async()=>{}, getQueueForUi:()=>queue, loadQueue:()=>queue,
    countQueuePendingItems:()=>1, findNextPendingQueueItem:()=>item.finalState ? null : item,
    saveQueueRuntimeState:async()=>{}, setQueueControlsDisabled(){}, updateRunButtonState(){},
    buildStateForItem:()=>({}), saveOptimizationState(){}, openResultsPage(){}, renderQueue(){},
    updateOptimizationState:value=>updates.push(value), setQueueItemState:(_,state)=>events.push(state),
    persistItemProgress:async(_,patch)=>Object.assign(item,patch),
    finalizeQueueItem:async(_,state,patch)=>Object.assign(item,patch,{finalState:state}),
    generateOptimizationRunId:()=> 'portfolio-run', setActiveOptimizationRunId:id=>{active=id;},
    getQueueSources:()=>item.sources, buildQueueStudySetState:()=>({}),
    requestServerCancelBestEffort:async id=>{assert.equal(active,id);events.push('cancel:'+id);},
    showQueueError:message=>{throw new Error(message);},
  });
  context.window.TickerSelector = {async execute(received, signal, runId) {
    executed++;
    assert.equal(received,item); assert.equal(runId,'portfolio-run');
    if (abort) { context.window.optimizationAbortController.abort(); const error = new Error('cancelled'); error.name='AbortError'; throw error; }
    return {portfolio_id:'pf',set_id:7,name:'Saved PF',status:'complete'};
  }};
  await context.runQueue();
  assert.equal(executed,1,'The complete pool is one operation');
  assert.equal(active,'');
  if (abort) {
    assert.deepEqual(events,['running','skipped','cancel:portfolio-run']);
    assert.equal(updates.at(-1).status,'cancelled');
    assert.equal(item.finalState,undefined,'An interrupted item remains queued');
  } else {
    assert.equal(item.finalState,'completed'); assert.equal(item.studySet.createdSetId,7);
    assert.equal(item.sourceCursor,2); assert.equal(updates.at(-1).status,'completed');
  }
}
(async()=>{await check(false);await check(true);console.log('Production portfolio Queue success/cancellation: passed');})().catch(error=>{console.error(error);process.exitCode=1;});
