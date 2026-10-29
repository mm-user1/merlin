const assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {Element,fire}=require('./analytics_dom.cjs');
const root=path.resolve(__dirname,'../..');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const equity={curve:[100,112.5],timestamps:['2024-01-01','2024-04-01'],has_equity_curve:true};

async function fixture(cached=false) {
  const document=new Element('document'),requests=[],charts=[],alerts=[];
  document.getElementById=id=>document.querySelector('#'+id);
  document.createElement=tag=>new Element(tag);
  document.createTextNode=text=>{const n=new Element('text');n.textContent=text;return n;};
  document.innerHTML=fs.readFileSync(path.join(root,'src/ui/templates/analytics.html'),'utf8');
  const defer=url=>new Promise((resolve,reject)=>requests.push({url,resolve,reject}));
  const c={console,document,Element,HTMLElement:Element,HTMLInputElement:Element,HTMLTableRowElement:Element,Node:Element,
    Set,Map,URLSearchParams,AbortController,structuredClone,setTimeout,clearTimeout,
    alert:message=>alerts.push(message),getSelection:()=>({removeAllRanges(){}}),
    localStorage:{getItem:()=>null,setItem(){}},location:{search:''},
    fetch:url=>defer(url),fetchAnalyticsStudyEquityRequest:id=>defer('study/'+id),
    fetchAnalyticsStudyWindowBoundariesRequest:async()=>({windows:[]}),
    fetchAnalyticsSetEquityRequest:id=>defer('set/'+id),switchDatabaseRequest:name=>defer('switch/'+name),
    fetchDatabasesList:async()=>({databases:[{name:'b.db',active:true}]})};
  c.window=c;vm.createContext(c);
  for(const name of ['analytics-sets.js','analytics-table.js','entry-diagnostics.js','analytics-portfolio.js'])
    vm.runInContext(fs.readFileSync(path.join(root,'src/ui/static/js',name),'utf8'),c);
  const source=fs.readFileSync(path.join(root,'src/ui/static/js/analytics.js'),'utf8');
  const entry="document.addEventListener('DOMContentLoaded', initAnalyticsPage);";
  assert.ok(source.includes(entry));
  vm.runInContext(source.replace(entry,'window.coordinator={state:AnalyticsState,initSetsModule,renderTableWithCurrentState,renderDatabasesList,bindFocusHotkeys,onTableSelectionChange,setFocus};'),c);
  const api=c.coordinator,state=api.state,el=id=>document.getElementById(id);
  c.AnalyticsEquity={renderChart:curve=>charts.push([...curve]),renderEmpty:message=>charts.push([message])};
  const single={study_id:'s1',study_name:'Single ordinary',is_wfa:true,has_equity_curve:true,
    profit_pct:12.5,max_dd_pct:3.25,total_trades:9,winning_trades:6,
    entry_diagnostics:{version:1,scope:'oos',maxEntryLeverageUsed:2,leverageCapSkippedEntries:0}};
  const selector={enabled:true,topN:2,rebalanceMonths:1,evaluateUnselected:false,params:{lookbackMonths:1,timeframeMinutes:30}};
  const pf={id:7,kind:'portfolio',portfolio_id:'pf',name:'PF',study_ids:[],selector,metrics:{},
    periods:[{id:1,start:'2024-01-01',end:'2024-02-01',funded_count:2,metrics:{}}]};
  const ordinary={id:8,name:'Ordinary set',study_ids:['s1'],metrics:{profit_pct:12.5}};
  state.dbName='a.db';state.studies=[single];state.researchInfo={wfa_studies:1};
  c.fetchAnalyticsSetsRequest=async()=>({sets:[pf,ordinary]});
  api.initSetsModule();api.bindFocusHotkeys();await c.AnalyticsSets.loadSets();
  const take=prefix=>{const i=requests.findIndex(r=>r.url.startsWith(prefix));assert.ok(i>=0,`Missing ${prefix}: ${requests.map(r=>r.url)}`);return requests.splice(i,1)[0];};
  const reply=async(r,value)=>{r.resolve({ok:true,json:async()=>value});await tick();};
  // Realistic route: focused ordinary set has fetched set equity, not the individual curve.
  c.AnalyticsSets.setFocusedSetId(8);take('set/8').resolve({...equity,profit_pct:12.5});await tick();
  assert.deepEqual([...state.checkedStudyIds],['s1']);
  if(cached)state.studyEquityByStudyId.set('s1',equity);
  const detail={selector,rows:[1,2].map(i=>({study_id:'rank'+i,study_name:'Rank '+i,portfolio_id:'pf',portfolioRank:i,portfolioSelection:{ranks:[i]}})),periodRows:[]};
  async function completePF() {
    await reply(take('/api/portfolios/pf'),detail);
    await reply(take('/api/portfolios/equity'),{curve:[100,105],timestamps:equity.timestamps,profit_pct:5,total_trades:80});
  }
  c.AnalyticsSets.setFocusedSetId(7);await completePF();
  const cards=()=>el('analyticsSummaryRow').innerHTML;
  const snapshot=()=>({cards:cards(),chart:charts.at(-1),title:el('analyticsChartTitle').textContent,
    rows:[...c.AnalyticsTable.getOrderedStudyIds()],checked:[...state.checkedStudyIds],scope:c.AnalyticsSets.getPortfolioScope()?.id,
    active:c.AnalyticsPortfolio.active(),db:state.dbName});
  function ready(){assert.equal(el('analyticsLoading').hidden,true);assert.equal(el('analyticsSummaryRow').getAttribute('aria-busy'),'false');assert.ok(!c.AnalyticsPortfolio.leaving());assert.ok(!el('analyticsSummaryTable').closest('.table-card').style.minHeight);}
  function ordinaryCards(){
    assert.ok(cards().includes('Portfolio Profit'),'F-1: current ordinary labels must replace PF labels');
    for(const value of ['+12.5%','-3.3%','>9</div>'])assert.ok(cards().includes(value),`Missing ordinary metric ${value}`);
    assert.ok(cards().includes('2.00× / 0'),'ordinary diagnostics are current too');
    assert.ok(!cards().includes('NET PROFIT'),'PF labels must not remain ready');
    assert.deepEqual([...state.checkedStudyIds],['s1']);ready();
  }
  function escape(){fire(document,'keydown',{key:'Escape'});}
  function switchDB(){api.renderDatabasesList([{name:'a.db',active:true},{name:'b.db',active:false}]);fire(el('analyticsDbList').children[1],'click');}
  return {c,api,state,requests,charts,alerts,el,single,detail,take,reply,cards,snapshot,ready,ordinaryCards,escape,switchDB,completePF};
}

async function exits(){
  for(const mode of ['uncached','cached','error','empty','focused','late-success','late-error','ordinary-success','ordinary-error','db-superseded']) {
    const f=await fixture(mode==='cached'),before=f.cards();
    f.escape();
    if(mode==='cached') { f.ordinaryCards();assert.deepEqual(f.charts.at(-1),equity.curve);assert.equal(f.requests.length,0);continue; }
    assert.equal(f.cards(),before);assert.equal(f.el('analyticsLoading').hidden,false);
    assert.equal(f.requests.length,1,'one individual curve request, no card-only fetch');
    const request=f.take('study/s1');
    if(mode.startsWith('ordinary-')) {
      f.state.studies.push({...f.single,study_id:'s2',study_name:'New ordinary',profit_pct:-4,total_trades:4});
      f.api.onTableSelectionChange(new Set(['s2']));
      f.take('study/s2').resolve({...equity,curve:[100,96]});await tick();
      assert.ok(f.cards().includes('-4.0%'));assert.deepEqual(f.charts.at(-1),[100,96]);f.ready();
      const newer=f.snapshot();
      if(mode==='ordinary-success')request.resolve(equity);else request.reject(new Error('superseded ordinary failure'));
      await tick();assert.deepEqual(f.snapshot(),newer);continue;
    }
    if(mode.startsWith('late-')) {
      f.c.AnalyticsSets.setFocusedSetId(7);
      if(mode==='late-success')request.resolve(equity);else request.reject(new Error('superseded failure'));
      await tick();assert.equal(f.cards(),before);assert.equal(f.el('analyticsLoading').hidden,false);
      await f.completePF();f.ready();assert.deepEqual(f.charts.at(-1),[100,105]);continue;
    }
    if(mode==='db-superseded') {
      f.switchDB();f.take('switch/b.db').resolve({success:true});await tick();
      await f.reply(f.take('/api/analytics/summary'),{db_name:'b.db',studies:[],research_info:{}});
      const next=f.snapshot();request.reject(new Error('old DB failure'));await tick();assert.deepEqual(f.snapshot(),next);f.ready();continue;
    }
    if(mode==='focused') {f.api.setFocus('s1');await tick();assert.equal(f.requests.length,0);}
    if(mode==='error')request.reject(new Error('Current ordinary curve unavailable'));
    else request.resolve(mode==='empty'?{curve:[],timestamps:[],has_equity_curve:false,warning:'No saved curve'}:equity);
    await tick();
    if(mode==='focused') {
      for(const value of ['+12.50%','-3.25%','6/9','2.00× / 0'])assert.ok(f.cards().includes(value));
      f.ready();
    }
    else f.ordinaryCards();
    assert.deepEqual(f.charts.at(-1),mode==='error'?['Current ordinary curve unavailable']:mode==='empty'?['No saved curve']:equity.curve);
    assert.equal(f.requests.length,0);
  }
}

async function databases(){
  const f=await fixture(true),before=f.snapshot();
  f.switchDB();f.take('switch/b.db').reject(new Error('DB switch rejected'));await tick();
  assert.deepEqual(f.alerts,['DB switch rejected']);assert.equal(f.state.dbSwitchInProgress,false);
  assert.deepEqual(f.snapshot(),before,'F-2: rejected switch must retain coherent PF scope/content/checks');
  f.api.renderTableWithCurrentState();assert.deepEqual([...f.c.AnalyticsTable.getOrderedStudyIds()],before.rows);
  await f.reply(f.take('/api/portfolios/equity'),{curve:[100,105],timestamps:equity.timestamps,profit_pct:5,total_trades:80});
  f.escape();f.ordinaryCards();
  // Ordinary failed switching keeps the same behavior, too.
  const ordinary=f.snapshot();f.switchDB();f.take('switch/b.db').reject(new Error('Ordinary switch rejected'));await tick();
  assert.deepEqual(f.snapshot(),ordinary);assert.equal(f.state.dbSwitchInProgress,false);
  // Retry succeeds with PF detail outstanding; invalidate before summary completes.
  f.c.AnalyticsSets.setFocusedSetId(7);const oldDetail=f.take('/api/portfolios/pf');
  f.switchDB();f.take('switch/b.db').resolve({success:true});await tick();
  const cleared=f.snapshot();await f.reply(oldDetail,f.detail);assert.deepEqual(f.snapshot(),cleared);
  await f.reply(f.take('/api/analytics/summary'),{db_name:'b.db',studies:[],research_info:{}});
  assert.equal(f.state.dbName,'b.db');assert.equal(f.state.dbSwitchInProgress,false);assert.equal(f.c.AnalyticsPortfolio.active(),false);
  assert.equal(f.c.AnalyticsSets.getPortfolioScope(),null);assert.equal(f.c.AnalyticsTable.getOrderedStudyIds().length,0);f.ready();
  // Same successful boundary rejects an outstanding projection, including a late failure.
  f.c.AnalyticsSets.setFocusedSetId(7);await f.reply(f.take('/api/portfolios/pf'),f.detail);
  const oldCurve=f.take('/api/portfolios/equity');f.switchDB();f.take('switch/b.db').resolve({success:true});await tick();
  const reset=f.snapshot();oldCurve.reject(new Error('old PF projection'));await tick();assert.deepEqual(f.snapshot(),reset);
  await f.reply(f.take('/api/analytics/summary'),{db_name:'b.db',studies:[],research_info:{}});f.ready();assert.equal(f.requests.length,0);
}
assert.ok(['exit','database'].includes(process.argv[2]));
(process.argv[2]==='exit'?exits:databases)().then(()=>console.log(`Portfolio ${process.argv[2]} completion: passed`)).catch(e=>{console.error(e);process.exitCode=1;});
