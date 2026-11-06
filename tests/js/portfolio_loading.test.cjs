const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {Element,fire} = require('./analytics_dom.cjs');
const root = path.resolve(__dirname,'../..');
const document = new Element('document');
document.getElementById = id => document.querySelector('#'+id);
document.createElement = tag => new Element(tag);
document.createTextNode = text => {const node=new Element('text');node.textContent=text;return node;};
document.innerHTML = fs.readFileSync(path.join(root,'src/ui/templates/analytics.html'),'utf8');
const requests=[],charts=[];
const c={console,document,Element,HTMLElement:Element,HTMLInputElement:Element,HTMLTableRowElement:Element,Node:Element,
  Set,Map,URLSearchParams,AbortController,structuredClone,setTimeout,clearTimeout,
  getSelection:()=>({removeAllRanges(){}}),localStorage:{getItem:()=>null,setItem(){}},location:{search:''},
  fetch:(url,options)=>new Promise(resolve=>requests.push({url,options,resolve})),
  fetchAnalyticsSetEquityRequest:id=>new Promise(resolve=>requests.push({url:'ordinary/'+id,resolvePayload:resolve}))};
c.window=c;vm.createContext(c);
const load=name=>vm.runInContext(fs.readFileSync(path.join(root,'src/ui/static/js',name),'utf8'),c);
load('analytics-sets.js');load('analytics-table.js');load('entry-diagnostics.js');load('entry-filters.js');load('analytics-portfolio.js');
// Expose private entry points only in this VM. Their implementations and the
// coordinator completion callback/header renderer are unmodified production code.
vm.runInContext(fs.readFileSync(path.join(root,'src/ui/static/js/analytics.js'),'utf8').replace(
  "document.addEventListener('DOMContentLoaded', initAnalyticsPage);",
  'window.coordinator={state:AnalyticsState,initSetsModule,renderTableWithCurrentState,loadSummary,bindFocusHotkeys,onTableSelectionChange,setFocus};'),c);
c.AnalyticsEquity={renderChart:curve=>charts.push([...curve]),renderEmpty:message=>charts.push([message])};
const api=c.coordinator, state=api.state, el=id=>document.getElementById(id);
const selector={enabled:true,topN:8,rebalanceMonths:1,evaluateUnselected:true,params:{lookbackMonths:4,timeframeMinutes:30}};
const pf={id:7,kind:'portfolio',portfolio_id:'pf',name:'PF long',study_ids:[],selector,metrics:{portfolio:true},
  periods:[1,2,3,4].map(id=>({id,start:'2024-01-01',end:'2024-02-01',funded_count:8,metrics:{}}))};
const other={...pf,id:9,portfolio_id:'other',name:'PF other'};
const ordinary={id:8,name:'Ordinary',study_ids:['ordinary'],metrics:{profit_pct:3}};
// Same canonical Python descriptor on ordinary and portfolio server rows.
const on={key:'[{"parameters":{"breakBars":1,"confirmationBars":2},"source":{"instrument_id":"OKX_BTC-USDT-SWAP","pack_root":"/pack","timeframe_minutes":240,"type":"parquet","version":1},"type":"candleDirection","version":2}]',text:'F06 v2 · OKX_BTC-USDT-SWAP · 4h · N 2 / break 1'};
const off={key:'off',text:'Filter Off'}, missing={key:null,text:'Unavailable'};
state.dbName='synthetic.db';state.studies=[{study_id:'ordinary',study_name:'Ordinary',is_wfa:true,has_equity_curve:false,entry_filters:off}];
state.researchInfo={wfa_studies:1};state.checkedStudyIds=new Set(['ordinary']);
c.fetchAnalyticsSetsRequest=async()=>({sets:[pf,ordinary,other]});
api.initSetsModule(); api.bindFocusHotkeys();
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const take=prefix=>{const i=requests.findIndex(r=>r.url.startsWith(prefix));assert.ok(i>=0,`Missing ${prefix}: ${requests.map(r=>r.url)}`);return requests.splice(i,1)[0];};
async function reply(request,value,ok=true){request.resolve({ok,json:async()=>value});await tick();}
const payload=value=>({curve:[100,value],timestamps:['2024-01-01','2024-02-01'],profit_pct:value-100,ann_profit_pct:null});
function detail(period,description=on){return {selector,rows:Array.from({length:period?44:8},(_,i)=>({study_id:`${period||0}-${i}`,study_name:`Ticker ${i}`,entry_filters:description,
  portfolio_id:'pf',portfolioRank:i<8?i+1:null,portfolioSelection:period?{periods:[period],accounts:[i]}:{ranks:[i+1]},
  profit_pct:i,resultStudyId:'saved-'+i})),periodRows:[]};}
function counts(expected){const values=[...el('analyticsTableHeaderMeta').innerHTML.matchAll(/analytics-table-header-value">(\d+)</g)].map(m=>+m[1]);assert.deepEqual(values.slice(-3),expected);}
function pending(value){assert.equal(el('analyticsLoading').hidden,!value);assert.equal(el('analyticsChartTitle').closest('.chart-card').getAttribute('aria-busy'),String(value));}
const go=period=>c.AnalyticsSets.handlePeriodGesture(7,period,{altKey:true});
const filterText=()=>el('analyticsResearchInfo').children.find(n=>n.children[0]?.textContent==='Entry Filters').children[1].textContent;
const filters=expected=>assert.equal(filterText(),expected);
async function ready(period,value=105){await reply(take('/api/portfolios/pf'),detail(period));await reply(take('/api/portfolios/equity'),payload(value));}

async function main(){
  await c.AnalyticsSets.loadSets();
  state.checkedStudyIds=new Set(['ordinary']);
  api.renderTableWithCurrentState();
  filters('Filter Off');
  // Completed ordinary view is retained throughout detail and projection loading.
  const tableBefore=el('analyticsSummaryTableBody').innerHTML, chartBefore=charts.at(-1),cardsBefore=el('analyticsSummaryRow').innerHTML;
  c.AnalyticsSets.setFocusedSetId(7);
  filters('Filter Off');
  pending(true);assert.equal(el('analyticsSummaryTableBody').innerHTML,tableBefore);assert.deepEqual(charts.at(-1),chartBefore);
  assert.equal(el('analyticsSummaryRow').innerHTML,cardsBefore);assert.equal(el('analyticsSummaryTable').closest('.table-card').inert,true);
  api.onTableSelectionChange(new Set(['stale']));assert.ok(!state.checkedStudyIds.has('stale'));
  await reply(take('/api/portfolios/pf'),detail(null));
  filters(on.text); // Actual completion must repaint before the equity reply.
  counts([8,8,8]);pending(true);assert.equal(el('analyticsSummaryTable').closest('.table-card').inert,false);
  assert.deepEqual(charts.at(-1),chartBefore);
  assert.equal(requests.filter(r=>r.url==='/api/portfolios/equity').length,1,'header completion never refetches');
  await reply(take('/api/portfolios/equity'),payload(105));pending(false);
  assert.ok(el('analyticsSummaryRow').innerHTML.includes('N/A'),'valid annualization N/A remains');
  fire(el('analyticsSetsTableWrap').querySelector('[data-set-id="7"] .portfolio-expand'),'click');
  go(1);const oldDetail=take('/api/portfolios/pf');
  filters(on.text);
  fire(document,'keydown',{key:'ArrowDown'});
  assert.equal(c.AnalyticsSets.getPortfolioScope().period,2,'keyboard set navigation remains available during loading');
  await ready(2,110);
  const completed=el('analyticsSummaryTableBody').innerHTML;
  await reply(oldDetail,{error:'late detail failure'},false);pending(false);assert.equal(el('analyticsSummaryTableBody').innerHTML,completed);
  filters(on.text);
  counts([8,8,44]);assert.deepEqual(charts.at(-1),[100,110]);
  c.AnalyticsPortfolio.setShowUnselected(true);counts([8,44,44]);await reply(take('/api/portfolios/equity'),payload(111));
  api.onTableSelectionChange(new Set(['2-0','2-8']));counts([2,44,44]);const staleProjection=take('/api/portfolios/equity');
  c.AnalyticsPortfolio.setShowUnselected(false);counts([1,8,44]);await reply(take('/api/portfolios/equity'),payload(112));
  await reply(staleProjection,{error:'late projection failure'},false);pending(false);assert.deepEqual(charts.at(-1),[100,112]);
  api.setFocus('2-1');const focusOld=take('/api/portfolios/equity');api.setFocus('2-2');
  filters(on.text);
  await reply(take('/api/portfolios/equity'),payload(114));await reply(focusOld,payload(999));
  assert.deepEqual(charts.at(-1),[100,114]);assert.equal(el('analyticsChartTitle').textContent,'Ticker 2');counts([1,8,44]);
  const membership=[...state.checkedStudyIds];
  fire(el('analyticsSummaryTable').querySelector('[data-sort-key="profit_pct"]'),'click');
  assert.deepEqual([...state.checkedStudyIds],membership);await reply(take('/api/portfolios/equity'),payload(114));
  go(3);const earlier=take('/api/portfolios/pf');go(4);await reply(take('/api/portfolios/pf'),detail(4));
  const beforeExit=take('/api/portfolios/equity');
  c.AnalyticsSets.setFocusedSetId(8);pending(true);assert.deepEqual(charts.at(-1),[100,114]);
  filters('Filter Off');
  assert.deepEqual([...state.checkedStudyIds],['ordinary'],'outer checks restored');
  const interruptedOrdinary=take('ordinary/8');
  c.AnalyticsSets.setFocusedSetId(7);await ready(null,115);
  interruptedOrdinary.resolvePayload(payload(666));await tick();assert.deepEqual(charts.at(-1),[100,115]);
  c.AnalyticsSets.setFocusedSetId(8);take('ordinary/8').resolvePayload(payload(103));await tick();pending(false);
  const ordinaryChart=charts.at(-1);await reply(beforeExit,payload(888));await reply(earlier,detail(3));
  assert.deepEqual(charts.at(-1),ordinaryChart);assert.equal(c.AnalyticsPortfolio.active(),false);
  filters('Filter Off');
  c.AnalyticsSets.setFocusedSetId(9);await reply(take('/api/portfolios/other'),{error:'detail failed'},false);
  pending(false);counts([0,0,0]);assert.equal(el('analyticsChartTitle').textContent,'detail failed');
  filters('?');
  api.renderTableWithCurrentState();assert.equal(requests.filter(r=>r.url==='/api/portfolios/other').length,1,'failed scope retries');
  await reply(take('/api/portfolios/other'),detail(null));await reply(take('/api/portfolios/equity'),{error:'projection failed'},false);
  pending(false);counts([8,8,8]);assert.equal(el('analyticsChartTitle').textContent,'projection failed');
  api.setFocus('0-0');await reply(take('/api/portfolios/equity'),payload(120));pending(false);
  go(1);let data=detail(1,missing);data.rows=[{...data.rows[0],unavailable:true,reason:'Saved account unavailable'}];
  await reply(take('/api/portfolios/pf'),data);pending(false);counts([0,1,1]);
  api.setFocus('1-0');assert.equal(el('analyticsChartTitle').textContent,'Saved account unavailable');assert.equal(requests.length,0);
  filters('Unavailable');
  go(2);await reply(take('/api/portfolios/pf'),{...detail(2),rows:[]});pending(false);counts([0,0,0]);
  filters('?');
  go(3);await ready(3);api.onTableSelectionChange(new Set());pending(false);counts([0,8,44]);
  filters('?');
  // DB reset invalidates both request generations before awaiting its summary.
  go(1);const abandonedDetail=take('/api/portfolios/pf');
  const loadingSummary=api.loadSummary();
  await reply(abandonedDetail,detail(1));assert.equal(requests.filter(r=>r.url==='/api/portfolios/equity').length,0);
  await reply(take('/api/analytics/summary'),{db_name:'new.db',studies:[],research_info:{}});await loadingSummary;
  filters('?');
  go(2);await reply(take('/api/portfolios/pf'),detail(2));const abandonedCurve=take('/api/portfolios/equity');
  c.AnalyticsPortfolio.reset();const resetChart=charts.at(-1);await reply(abandonedCurve,payload(777));assert.deepEqual(charts.at(-1),resetChart);pending(false);
  assert.equal(c.AnalyticsTable.getOrderedStudyIds().length,0,'reset removes old actionable account rows');
  assert.ok(!el('analyticsSummaryTable').closest('.table-card').style.minHeight,'temporary geometry released');
  // Outer mixed aggregation retains ordinary members and selected PF periods.
  const reloaded=api.loadSummary();
  await reply(take('/api/analytics/summary'),{db_name:'mixed.db',studies:[{study_id:'ordinary',study_name:'Ordinary',is_wfa:true,entry_filters:on}],research_info:{wfa_studies:1}});
  await reloaded;
  fire(el('analyticsSetsTableWrap').querySelector('[data-set-id="8"]'),'click');
  fire(el('analyticsSetsTableWrap').querySelector('[data-set-id="7"]'),'click');
  const mixed=detail(null);mixed.periodRows=[1,2,3,4].map(i=>({...mixed.rows[i],study_id:'period-'+i,portfolioSelection:{periods:[i]}}));
  await reply(take('/api/portfolios/pf'),mixed);counts([5,5,5]);
  filters(on.text);
  const mixedRequest=take('/api/portfolios/equity'),body=JSON.parse(mixedRequest.options.body);
  assert.deepEqual(body.studyIds,['ordinary']);assert.deepEqual(body.portfolios.map(p=>p.selection.periods),[[1],[2],[3],[4]]);
  await reply(mixedRequest,payload(130));pending(false);
  // Different keys with identical display text must still be Mixed. Idle rows
  // carry saved context independently of trades; focus wins over empty checks.
  mixed.periodRows[1].entry_filters={...on,key:on.key.replace('"confirmationBars":2','"confirmationBars":3')};
  mixed.periodRows[1].total_trades=0;
  mixed.periodRows[2].entry_filters=off;mixed.periodRows[3].entry_filters=missing;
  const select=async(ids,expected)=>{api.onTableSelectionChange(new Set(ids));filters(expected);
    if(requests.some(r=>r.url==='/api/portfolios/equity')) await reply(take('/api/portfolios/equity'),payload(130));};
  await select(['ordinary','period-1'],on.text);
  await select(['period-1','period-2'],'Mixed');
  await select(['ordinary','period-3'],'Mixed');
  await select(['period-3'],'Filter Off');
  await select(['period-1','period-4'],'Unavailable');
  api.setFocus('period-2');filters(on.text);await reply(take('/api/portfolios/equity'),payload(130));
  await select([],on.text);
  api.setFocus(null);filters('?');
  // Explicitly Off whole-run and period/account scopes, plus a stale success
  // after a newer Off detail. Existing selection/loading epochs do all the work.
  c.AnalyticsSets.setFocusedSetId(7);const staleOn=take('/api/portfolios/pf');
  go(1);await reply(take('/api/portfolios/pf'),detail(1,off));filters('Filter Off');
  await reply(take('/api/portfolios/equity'),payload(110));
  await reply(staleOn,detail(null,on));filters('Filter Off');
  api.setFocus('1-0');filters('Filter Off');await reply(take('/api/portfolios/equity'),payload(110));
  c.AnalyticsSets.setFocusedSetId(7);await reply(take('/api/portfolios/pf'),detail(null,off));filters('Filter Off');
  await reply(take('/api/portfolios/equity'),payload(110));
  assert.equal(requests.length,0);
  console.log('Portfolio/coordinator delayed loading, counters, selection, errors and reset: passed');
}
async function restoration() {
  c.fetchAnalyticsSetEquityRequest=async()=>payload(103);
  const second={id:10,name:'Second ordinary',study_ids:['second'],metrics:{}};
  c.fetchAnalyticsSetsRequest=async()=>({sets:[pf,ordinary,second]});
  const click=id=>fire(el('analyticsSetsTableWrap').querySelector(`[data-set-id="${id}"]`),'click');
  const focus=id=>fire(el('analyticsSetsTableWrap').querySelector(`[data-set-id="${id}"]`),'click',{altKey:true});
  const mixed=()=>{const value=detail(null);value.periodRows=[1,2,3,4].map(i=>({...value.rows[i],study_id:'period-'+i,portfolioSelection:{periods:[i]}}));return value;};
  async function reset() {
    assert.equal(requests.length,0);
    const load=api.loadSummary();
    await reply(take('/api/analytics/summary'),{db_name:'restore.db',studies:['ordinary','second'].map(id=>({study_id:id,study_name:id,is_wfa:true,entry_filters:on})),research_info:{wfa_studies:2}});
    await load;
  }
  async function projection(ids,periods) {
    assert.deepEqual([...state.checkedStudyIds].sort(),[...ids,...periods.map(i=>'period-'+i)].sort());
    assert.deepEqual([...c.AnalyticsTable.getCheckedStudyIds()].sort(),[...state.checkedStudyIds].sort());
    if(!ids.length&&!periods.length){assert(!requests.some(r=>r.url==='/api/portfolios/equity'));return;}
    const request=take('/api/portfolios/equity'),body=JSON.parse(request.options.body);
    assert.deepEqual(body.studyIds.sort(),[...ids].sort());assert.deepEqual(body.portfolios.map(p=>p.selection.periods[0]).sort(),[...periods].sort());
    await reply(request,payload(120));filters(on.text);
  }
  async function enterMixed(){click(8);click(7);await reply(take('/api/portfolios/pf'),mixed());counts([5,5,5]);await projection(['ordinary'],[1,2,3,4]);}
  async function checkRows(ids) {
    const changes=el('analyticsSummaryTable').querySelectorAll('.analytics-row-check').filter(node=>node.checked!==ids.includes(node.dataset.studyId));
    for (const [index,node] of changes.entries()) {
      node.checked=ids.includes(node.dataset.studyId);fire(node,'change');
      if(index<changes.length-1 && requests.some(r=>r.url==='/api/portfolios/equity')) await reply(take('/api/portfolios/equity'),payload(120));
    }
  }
  async function temporaryFocus(){focus(7);await ready(null);focus(7);assert(c.AnalyticsPortfolio.detailPending());}
  // Reported ordinary-exit leak, partial periods, and a repeated fresh entry.
  for(const partial of [false,true]) {
    await reset();focus(7);await ready(null);focus(8);await tick();focus(8);click(8);
    if(partial)c.AnalyticsSets.handlePeriodGesture(7,2,{});else click(7);
    await reply(take('/api/portfolios/pf'),mixed());counts(partial?[2,2,2]:[5,5,5]);
    await projection(['ordinary'],partial?[2]:[1,2,3,4]);
    click(7);click(7);await reply(take('/api/portfolios/pf'),mixed());counts([5,5,5]);await projection(['ordinary'],[1,2,3,4]);
  }
  // Mixed subsets and empty checks must survive temporary focus, including a
  // same-key render while the restoration request is pending.
  for(const empty of [false,true]) {
    await reset();await enterMixed();await checkRows(empty?[]:['ordinary','period-2']);
    await projection(empty?[]:['ordinary'],empty?[]:[2]);
    await temporaryFocus();api.renderTableWithCurrentState();
    assert.equal(requests.filter(r=>r.url==='/api/portfolios/pf').length,1);
    await reply(take('/api/portfolios/pf'),mixed());counts([empty?0:2,5,5]);
    await projection(empty?[]:['ordinary'],empty?[]:[2]);if(empty)filters('?');
  }
  // A request owns its snapshot at start, even if it never completes successfully.
  for(const failure of [false,true]) {
    await reset();await enterMixed();await checkRows(['ordinary','period-2']);await projection(['ordinary'],[2]);
    await temporaryFocus();const old=take('/api/portfolios/pf');
    if(failure){await reply(old,{error:'restoration failed'},false);counts([0,0,0]);filters('?');api.renderTableWithCurrentState();}
    else click(10);
    await reply(take('/api/portfolios/pf'),mixed());counts(failure?[5,5,5]:[6,6,6]);
    await projection(failure?['ordinary']:['ordinary','second'],[1,2,3,4]);
    if(!failure){await reply(old,mixed());counts([6,6,6]);assert.equal(requests.length,0);}
  }
  // Rendering uses only Analytics' existing scripts; month cadence stays months.
  const html=fs.readFileSync(path.join(root,'src/ui/templates/analytics.html'),'utf8');
  assert(!html.includes('/static/js/parquet-input.js'));assert(!html.includes('/static/js/utils.js'));
  for(const [minutes,label] of [[5,'5m'],[15,'15m'],[30,'30m'],[90,'90m'],[60,'1h'],[240,'4h'],[1440,'1D'],[4320,'3D'],[0,'0m'],[-60,'-60m'],['60','60m'],[undefined,'undefinedm'],[1.5,'1.5m']]) {
    selector.params.timeframeMinutes=minutes;await reset();focus(7);
    await reply(take('/api/portfolios/pf'),detail(null));
    const info=el('analyticsResearchInfo').children.find(n=>n.children[0]?.textContent==='Ticker Selector').children[1].textContent;
    assert.equal(info,`X · Top 8 · Every 1m · 4m / ${label}`);
    assert(el('portfolioSelectorSettings').querySelector('.settings-list').innerHTML.includes(`<span class="key">Ranking Timeframe</span><span class="val">${label}</span>`));
    await reply(take('/api/portfolios/equity'),payload(120));
  }
  assert.equal(requests.length,0);console.log('Mixed restoration lifetime, superseded/failure recovery, canonical sidebar rendering: passed');
}
main().then(restoration).catch(error=>{console.error(error);process.exitCode=1;});
