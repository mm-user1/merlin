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
load('analytics-sets.js');load('analytics-table.js');load('entry-diagnostics.js');load('analytics-portfolio.js');
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
state.dbName='synthetic.db';state.studies=[{study_id:'ordinary',study_name:'Ordinary',is_wfa:true,has_equity_curve:false}];
state.researchInfo={wfa_studies:1};state.checkedStudyIds=new Set(['ordinary']);
c.fetchAnalyticsSetsRequest=async()=>({sets:[pf,ordinary,other]});
api.initSetsModule(); api.bindFocusHotkeys();
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const take=prefix=>{const i=requests.findIndex(r=>r.url.startsWith(prefix));assert.ok(i>=0,`Missing ${prefix}: ${requests.map(r=>r.url)}`);return requests.splice(i,1)[0];};
async function reply(request,value,ok=true){request.resolve({ok,json:async()=>value});await tick();}
const payload=value=>({curve:[100,value],timestamps:['2024-01-01','2024-02-01'],profit_pct:value-100,ann_profit_pct:null});
function detail(period){return {selector,rows:Array.from({length:period?44:8},(_,i)=>({study_id:`${period||0}-${i}`,study_name:`Ticker ${i}`,
  portfolio_id:'pf',portfolioRank:i<8?i+1:null,portfolioSelection:period?{periods:[period],accounts:[i]}:{ranks:[i+1]},
  profit_pct:i,resultStudyId:'saved-'+i})),periodRows:[]};}
function counts(expected){const values=[...el('analyticsTableHeaderMeta').innerHTML.matchAll(/analytics-table-header-value">(\d+)</g)].map(m=>+m[1]);assert.deepEqual(values.slice(-3),expected);}
function pending(value){assert.equal(el('analyticsLoading').hidden,!value);assert.equal(el('analyticsChartTitle').closest('.chart-card').getAttribute('aria-busy'),String(value));}
const go=period=>c.AnalyticsSets.handlePeriodGesture(7,period,{altKey:true});
async function ready(period,value=105){await reply(take('/api/portfolios/pf'),detail(period));await reply(take('/api/portfolios/equity'),payload(value));}

async function main(){
  await c.AnalyticsSets.loadSets();
  state.checkedStudyIds=new Set(['ordinary']);
  api.renderTableWithCurrentState();
  // Completed ordinary view is retained throughout detail and projection loading.
  const tableBefore=el('analyticsSummaryTableBody').innerHTML, chartBefore=charts.at(-1),cardsBefore=el('analyticsSummaryRow').innerHTML;
  c.AnalyticsSets.setFocusedSetId(7);
  pending(true);assert.equal(el('analyticsSummaryTableBody').innerHTML,tableBefore);assert.deepEqual(charts.at(-1),chartBefore);
  assert.equal(el('analyticsSummaryRow').innerHTML,cardsBefore);assert.equal(el('analyticsSummaryTable').closest('.table-card').inert,true);
  api.onTableSelectionChange(new Set(['stale']));assert.ok(!state.checkedStudyIds.has('stale'));
  await reply(take('/api/portfolios/pf'),detail(null));
  counts([8,8,8]);pending(true);assert.equal(el('analyticsSummaryTable').closest('.table-card').inert,false);
  assert.deepEqual(charts.at(-1),chartBefore);
  assert.equal(requests.filter(r=>r.url==='/api/portfolios/equity').length,1,'header completion never refetches');
  await reply(take('/api/portfolios/equity'),payload(105));pending(false);
  assert.ok(el('analyticsSummaryRow').innerHTML.includes('N/A'),'valid annualization N/A remains');
  fire(el('analyticsSetsTableWrap').querySelector('[data-set-id="7"] .portfolio-expand'),'click');
  go(1);const oldDetail=take('/api/portfolios/pf');
  fire(document,'keydown',{key:'ArrowDown'});
  assert.equal(c.AnalyticsSets.getPortfolioScope().period,2,'keyboard set navigation remains available during loading');
  await ready(2,110);
  const completed=el('analyticsSummaryTableBody').innerHTML;
  await reply(oldDetail,{error:'late detail failure'},false);pending(false);assert.equal(el('analyticsSummaryTableBody').innerHTML,completed);
  counts([8,8,44]);assert.deepEqual(charts.at(-1),[100,110]);
  c.AnalyticsPortfolio.setShowUnselected(true);counts([8,44,44]);await reply(take('/api/portfolios/equity'),payload(111));
  api.onTableSelectionChange(new Set(['2-0','2-8']));counts([2,44,44]);const staleProjection=take('/api/portfolios/equity');
  c.AnalyticsPortfolio.setShowUnselected(false);counts([1,8,44]);await reply(take('/api/portfolios/equity'),payload(112));
  await reply(staleProjection,{error:'late projection failure'},false);pending(false);assert.deepEqual(charts.at(-1),[100,112]);
  api.setFocus('2-1');const focusOld=take('/api/portfolios/equity');api.setFocus('2-2');
  await reply(take('/api/portfolios/equity'),payload(114));await reply(focusOld,payload(999));
  assert.deepEqual(charts.at(-1),[100,114]);assert.equal(el('analyticsChartTitle').textContent,'Ticker 2');counts([1,8,44]);
  const membership=[...state.checkedStudyIds];
  fire(el('analyticsSummaryTable').querySelector('[data-sort-key="profit_pct"]'),'click');
  assert.deepEqual([...state.checkedStudyIds],membership);await reply(take('/api/portfolios/equity'),payload(114));
  go(3);const earlier=take('/api/portfolios/pf');go(4);await reply(take('/api/portfolios/pf'),detail(4));
  const beforeExit=take('/api/portfolios/equity');
  c.AnalyticsSets.setFocusedSetId(8);pending(true);assert.deepEqual(charts.at(-1),[100,114]);
  assert.deepEqual([...state.checkedStudyIds],['ordinary'],'outer checks restored');
  const interruptedOrdinary=take('ordinary/8');
  c.AnalyticsSets.setFocusedSetId(7);await ready(null,115);
  interruptedOrdinary.resolvePayload(payload(666));await tick();assert.deepEqual(charts.at(-1),[100,115]);
  c.AnalyticsSets.setFocusedSetId(8);take('ordinary/8').resolvePayload(payload(103));await tick();pending(false);
  const ordinaryChart=charts.at(-1);await reply(beforeExit,payload(888));await reply(earlier,detail(3));
  assert.deepEqual(charts.at(-1),ordinaryChart);assert.equal(c.AnalyticsPortfolio.active(),false);
  c.AnalyticsSets.setFocusedSetId(9);await reply(take('/api/portfolios/other'),{error:'detail failed'},false);
  pending(false);counts([0,0,0]);assert.equal(el('analyticsChartTitle').textContent,'detail failed');
  api.renderTableWithCurrentState();assert.equal(requests.filter(r=>r.url==='/api/portfolios/other').length,1,'failed scope retries');
  await reply(take('/api/portfolios/other'),detail(null));await reply(take('/api/portfolios/equity'),{error:'projection failed'},false);
  pending(false);counts([8,8,8]);assert.equal(el('analyticsChartTitle').textContent,'projection failed');
  api.setFocus('0-0');await reply(take('/api/portfolios/equity'),payload(120));pending(false);
  go(1);let data=detail(1);data.rows=[{...data.rows[0],unavailable:true,reason:'Saved account unavailable'}];
  await reply(take('/api/portfolios/pf'),data);pending(false);counts([0,1,1]);
  api.setFocus('1-0');assert.equal(el('analyticsChartTitle').textContent,'Saved account unavailable');assert.equal(requests.length,0);
  go(2);await reply(take('/api/portfolios/pf'),{...detail(2),rows:[]});pending(false);counts([0,0,0]);
  go(3);await ready(3);api.onTableSelectionChange(new Set());pending(false);counts([0,8,44]);
  // DB reset invalidates both request generations before awaiting its summary.
  go(1);const abandonedDetail=take('/api/portfolios/pf');
  const loadingSummary=api.loadSummary();
  await reply(abandonedDetail,detail(1));assert.equal(requests.filter(r=>r.url==='/api/portfolios/equity').length,0);
  await reply(take('/api/analytics/summary'),{db_name:'new.db',studies:[],research_info:{}});await loadingSummary;
  go(2);await reply(take('/api/portfolios/pf'),detail(2));const abandonedCurve=take('/api/portfolios/equity');
  c.AnalyticsPortfolio.reset();const resetChart=charts.at(-1);await reply(abandonedCurve,payload(777));assert.deepEqual(charts.at(-1),resetChart);pending(false);
  assert.equal(c.AnalyticsTable.getOrderedStudyIds().length,0,'reset removes old actionable account rows');
  assert.ok(!el('analyticsSummaryTable').closest('.table-card').style.minHeight,'temporary geometry released');
  // Outer mixed aggregation retains ordinary members and selected PF periods.
  const reloaded=api.loadSummary();
  await reply(take('/api/analytics/summary'),{db_name:'mixed.db',studies:[{study_id:'ordinary',study_name:'Ordinary',is_wfa:true}],research_info:{wfa_studies:1}});
  await reloaded;
  fire(el('analyticsSetsTableWrap').querySelector('[data-set-id="8"]'),'click');
  fire(el('analyticsSetsTableWrap').querySelector('[data-set-id="7"]'),'click');
  const mixed=detail(null);mixed.periodRows=[1,2,3,4].map(i=>({...mixed.rows[i],study_id:'period-'+i,portfolioSelection:{periods:[i]}}));
  await reply(take('/api/portfolios/pf'),mixed);counts([5,5,5]);
  const mixedRequest=take('/api/portfolios/equity'),body=JSON.parse(mixedRequest.options.body);
  assert.deepEqual(body.studyIds,['ordinary']);assert.deepEqual(body.portfolios.map(p=>p.selection.periods),[[1],[2],[3],[4]]);
  await reply(mixedRequest,payload(130));pending(false);
  assert.equal(requests.length,0);
  console.log('Portfolio/coordinator delayed loading, counters, selection, errors and reset: passed');
}
main().catch(error=>{console.error(error);process.exitCode=1;});
