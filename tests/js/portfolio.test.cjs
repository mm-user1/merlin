const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname,'../..');
const clone = value=>JSON.parse(JSON.stringify(value));

// Only browser mechanics are shimmed. Selection, sorting, focus and projection
// requests below run the production modules and their registered event handlers.
class Element {
  constructor(tag='div') {
    this.tagName=tag.toUpperCase(); this.children=[];this.parentElement=null;this.attrs={};this.dataset={};this.style={setProperty(k,v){this[k]=v;},removeProperty(k){delete this[k];}};this.handlers={};
    this.checked=false;this.disabled=false;this.hidden=false;this.scrollTop=0;
    this.classList={contains:n=>this.className.split(/\s+/).includes(n),add:(...ns)=>{this.className=[...new Set([...this.className.split(/\s+/),...ns])].join(' ');},
      remove:(...ns)=>{this.className=this.className.split(/\s+/).filter(n=>!ns.includes(n)).join(' ');},
      toggle:(n,on)=>{on ??= !this.classList.contains(n);on?this.classList.add(n):this.classList.remove(n);return on;}};
  }
  get className(){return this.attrs.class||'';} set className(v){this.attrs.class=v;}
  get id(){return this.attrs.id||'';} set id(v){this.attrs.id=v;}
  setAttribute(k,v){this.attrs[k]=String(v);if(k.startsWith('data-'))this.dataset[k.slice(5).replace(/-([a-z])/g,(_,x)=>x.toUpperCase())]=String(v);if(['type','value'].includes(k))this[k]=v;if(['checked','disabled','hidden'].includes(k))this[k]=true;}
  getAttribute(k){return this.attrs[k]??null;}
  removeAttribute(k){delete this.attrs[k];}
  appendChild(n){n.parentElement=this;this.children.push(n);return n;}
  append(...ns){ns.forEach(n=>this.appendChild(n));}
  addEventListener(k,fn){(this.handlers[k]??=[]).push(fn);}
  matches(selector){
    return selector.split(',').some(s=>{
      s=s.trim();const attrs=[...s.matchAll(/\[([^=\]]+)(?:="([^"]*)")?\]/g)];s=s.replace(/\[[^\]]*\]/g,'');
      if(attrs.some(m=>this.getAttribute(m[1])===null||(m[2]!=null&&this.getAttribute(m[1])!==m[2])))return false;
      const tag=/^[\w-]+/.exec(s)?.[0];if(tag&&tag.toUpperCase()!==this.tagName)return false;
      if([...s.matchAll(/\.([\w-]+)/g)].some(m=>!this.classList.contains(m[1])))return false;
      const id=/#([\w-]+)/.exec(s)?.[1];return !id||id===this.id;
    });
  }
  closest(s){for(let n=this;n;n=n.parentElement)if(n.matches(s))return n;return null;}
  querySelectorAll(selector){
    const terms=selector.trim().split(/\s+(?![^\[]*\])/);const last=terms.pop(),results=[];
    const walk=n=>{for(const c of n.children){if(c.matches(last)){let parent=c.parentElement,ok=true;for(const term of [...terms].reverse()){while(parent&&!parent.matches(term))parent=parent.parentElement;if(!parent){ok=false;break;}parent=parent.parentElement;}if(ok)results.push(c);}walk(c);}};
    walk(this);return results;
  }
  querySelector(s){return this.querySelectorAll(s)[0]||null;}
  set innerHTML(html){
    this.children=[];this._html=html;const stack=[this];
    for(const token of String(html).matchAll(/<\/?[a-zA-Z][^>]*>/g)){
      const s=token[0];if(s.startsWith('</')){if(stack.length>1)stack.pop();continue;}
      const tag=/^<([\w-]+)/.exec(s)[1],node=new Element(tag);
      const attributes=s.slice(tag.length+1,-1);
      for(const a of attributes.matchAll(/([\w-]+)(?:="([^"]*)"|='([^']*)')?/g))node.setAttribute(a[1],a[2]??a[3]??'');
      stack.at(-1).appendChild(node);if(!['input','br','hr','img','meta','link'].includes(tag))stack.push(node);
    }
  }
  get innerHTML(){return this._html||'';}
  getBoundingClientRect(){return {top:0,bottom:100};}
  focus(){} contains(n){return n===this||this.children.some(c=>c.contains(n));}
  remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(c=>c!==this);}
}
function fire(node,type,options={}){const e={target:node,preventDefault(){this.defaultPrevented=true;},stopPropagation(){this.stopped=true;},...options};for(let n=node;n;n=n.parentElement){for(const fn of n.handlers[type]||[])fn(e);if(e.stopped)break;}return e;}
const document=new Element('document');document.getElementById=id=>document.querySelector('#'+id);document.createElement=tag=>new Element(tag);
document.innerHTML='<div id="analyticsSetsTableWrap"></div><div id="analyticsSetsSummary"></div><table id="analyticsSummaryTable"><thead><tr><th class="analytics-sortable" data-sort-key="study_name"><span class="sort-arrow"></span></th></tr></thead><tbody id="analyticsSummaryTableBody"></tbody></table><input id="analyticsHeaderCheck" type="checkbox"/><div id="analyticsSummaryRow"></div><h3 id="analyticsChartTitle"></h3><details id="portfolioSelectorSettings"><div class="settings-list"></div></details><label><input id="portfolioShowUnselected" type="checkbox"/></label>';
const c={console,document,Element,HTMLElement:Element,HTMLInputElement:Element,HTMLTableRowElement:Element,Node:Element,Set,Map,URLSearchParams,structuredClone,
  getSelection:()=>({removeAllRanges(){}}),localStorage:{getItem:()=>null,setItem(){}},location:{search:''},setTimeout,clearTimeout};c.window=c;vm.createContext(c);
const load=name=>vm.runInContext(fs.readFileSync(path.join(root,'src/ui/static/js',name),'utf8'),c);
load('analytics-sets.js');load('analytics-table.js');load('entry-diagnostics.js');load('analytics-portfolio.js');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const set={id:7,kind:'portfolio',portfolio_id:'pf',name:'#203 · PF HA',study_ids:[],period_count:3,selector:{enabled:true,topN:2,rebalanceMonths:1,params:{lookbackMonths:1,timeframeMinutes:30}},
  periods:[1,2,3].map(id=>({id,start:'2024-01-01',end:'2024-02-01',funded_count:2,metrics:{}})),metrics:{portfolio:true}};
const ordinary={id:8,name:'Ordinary',study_ids:['ordinary']};
const state={dbName:'test',sets:[set,ordinary],checkedSetIds:new Set(),checkedStudyIds:new Set(['ordinary']),studies:[{study_id:'ordinary'}],focusedStudyId:null,sortState:{}};
let events=[];c.fetchAnalyticsSetsRequest=async()=>({sets:[set,ordinary]});
c.AnalyticsSets.init({studies:state.studies,checkedStudyIds:state.checkedStudyIds,onStateChange:e=>{events.push(e);state.checkedSetIds=c.AnalyticsSets.getCheckedSetIds();state.focusedSetId=c.AnalyticsSets.getFocusedSetId();c.AnalyticsPortfolio.setsChanged(state,e);}});
const wrap=()=>document.getElementById('analyticsSetsTableWrap');
const parent=()=>wrap().querySelector('tr[data-set-id="7"]');
async function main(){
  await c.AnalyticsSets.loadSets();
  assert.ok(parent(),'production set rendering');
  fire(parent().querySelector('.portfolio-expand'),'click');
  assert.equal(wrap().querySelectorAll('.portfolio-period-row').length,3);
  assert.equal(c.AnalyticsSets.getCheckedSetIds().size,0,'expand is not a check');
  fire(parent(),'click');assert.deepEqual(clone(c.AnalyticsSets.getCheckedPortfolioPeriods()[0].periods),[1,2,3]);
  fire(wrap().querySelector('[data-period="2"]'),'click');
  assert.equal(parent().querySelector('input').indeterminate,true);
  fire(wrap().querySelector('[data-period="1"]'),'click',{altKey:true});
  assert.equal(c.AnalyticsSets.getPortfolioScope().period,1);
  assert.equal(parent().classList.contains('analytics-set-focused'),false);
  assert.ok(wrap().querySelector('[data-period="1"]').classList.contains('analytics-set-focused'));
  assert.ok(c.AnalyticsSets.moveInlineFocus(1));assert.equal(c.AnalyticsSets.getPortfolioScope().period,2);
  assert.equal(parent().classList.contains('analytics-set-focused'),false);
  assert.equal(wrap().querySelector('[data-period="1"]').classList.contains('analytics-set-focused'),false);
  assert.ok(wrap().querySelector('[data-period="2"]').classList.contains('analytics-set-focused'));
  fire(parent().querySelector('.portfolio-expand'),'click');assert.equal(c.AnalyticsSets.getPortfolioScope().period,2,'collapsed period focus survives');
  c.AnalyticsSets.handleEscapeFromSetFocus();assert.equal(c.AnalyticsSets.getPortfolioScope(),null);
  assert.deepEqual([...state.checkedStudyIds],['ordinary'],'outer checks restored');
  c.AnalyticsSets.handlePeriodGesture(7,1,{ctrlKey:true});
  c.AnalyticsSets.handlePeriodGesture(7,1,{});c.AnalyticsSets.handlePeriodGesture(7,3,{shiftKey:true});
  assert.ok(c.AnalyticsSets.getCheckedPortfolioPeriods()[0].periods.length===3);
  c.AnalyticsSets.handlePeriodGesture(7,2,{ctrlKey:true});assert.equal(c.AnalyticsSets.getCheckedSetIds().size,0);

  const rows=[{study_id:'r10',portfolio_id:'pf',portfolioRank:10,study_name:'#10 · 30m',profit_pct:2},
    {study_id:'r2',portfolio_id:'pf',portfolioRank:2,study_name:'#2 · 30m',profit_pct:1},
    {study_id:'a',portfolio_id:'pf',portfolioRank:null,study_name:'AAA, 30m',unavailable:true,profit_pct:null},
    {study_id:'z',portfolio_id:'pf',portfolioRank:null,study_name:'ZZZ, 30m',profit_pct:3}];
  let checked,focus,sort;
  c.AnalyticsTable.renderTable(rows,new Set(['r2']),ids=>{checked=ids;},{groupDatesEnabled:false,onFocusToggle:id=>{focus=id;},onSortChange:s=>{sort=s;}});
  assert.deepEqual(clone(c.AnalyticsTable.getOrderedStudyIds()),['r2','r10','a','z']);
  const table=document.getElementById('analyticsSummaryTable'), header=table.querySelector('th');
  fire(header,'click');fire(header,'click');assert.deepEqual(clone(c.AnalyticsTable.getOrderedStudyIds()),['r10','r2','z','a']);
  fire(header,'click');assert.equal(sort.sortColumn,null);
  const row=id=>table.querySelector(`tr[data-study-id="${id}"]`);
  fire(row('a'),'click');assert.ok(!c.AnalyticsTable.getCheckedStudyIds().includes('a'));
  fire(row('a'),'click',{altKey:true});assert.equal(focus,'a');
  fire(row('r10'),'click');fire(row('z'),'click',{shiftKey:true});assert.ok(!c.AnalyticsTable.getCheckedStudyIds().includes('a'));
  fire(row('r2'),'click',{ctrlKey:true});assert.ok(c.AnalyticsTable.getCheckedStudyIds().length<=3);
  c.AnalyticsTable.setAllChecked(true);assert.equal(c.AnalyticsTable.getCheckedStudyIds().length,3);
  assert.ok(table.querySelector('tbody').innerHTML.includes('N/A'));

  c.AnalyticsSets.setFocusedSetId(7);
  let responseRows=rows, responseSelector={...set.selector,evaluateUnselected:false};
  const requests=[];c.fetch=async(url,options)=>{requests.push({url,options});return {ok:true,json:async()=>url.includes('/equity')?{curve:[100,105],timestamps:['2024-01-01','2024-02-01'],profit_pct:5}:{rows:responseRows,selector:responseSelector,periodRows:[]}};};
  c.AnalyticsEquity={renderChart(){}};
  const callbacks={selection:ids=>{state.checkedStudyIds=new Set(ids);},sort:s=>{state.sortState=s;},focus:id=>{state.focusedStudyId=id;},view(){}};
  c.AnalyticsPortfolio.renderTable(state,callbacks);await tick();await tick();
  assert.ok(requests.some(r=>r.url==='/api/portfolios/equity'));
  assert.equal(document.getElementById('portfolioSelectorSettings').querySelectorAll('.setting-item').length,6);
  assert.deepEqual(JSON.parse(requests.filter(r=>r.options).at(-1).options.body).studyIds,[]);
  const unselectedLabel=document.getElementById('portfolioShowUnselected').closest('label');
  assert.equal(unselectedLabel.hidden,true,'Parent ranks never offer comparisons');
  c.AnalyticsSets.handlePeriodGesture(7,1,{altKey:true});
  c.AnalyticsPortfolio.renderTable(state,callbacks);await tick();await tick();
  assert.equal(unselectedLabel.hidden,true,'Evaluation Off does not offer unexecuted rows');
  responseSelector={...set.selector,evaluateUnselected:true};
  c.AnalyticsSets.handlePeriodGesture(7,2,{altKey:true});
  c.AnalyticsPortfolio.renderTable(state,callbacks);await tick();await tick();
  assert.equal(unselectedLabel.hidden,false,'Saved comparison entries can be shown');
  c.AnalyticsPortfolio.setShowUnselected(true);
  assert.ok(c.AnalyticsTable.getOrderedStudyIds().includes('z'));
  responseSelector={...set.selector,evaluateUnselected:false};
  c.AnalyticsSets.handlePeriodGesture(7,1,{altKey:true});
  c.AnalyticsPortfolio.renderTable(state,callbacks);await tick();await tick();
  assert.equal(unselectedLabel.hidden,true);
  assert.ok(!c.AnalyticsTable.getOrderedStudyIds().includes('z'),'Retained On cannot reveal unexecuted rows in an Off run');
  responseSelector={...set.selector,evaluateUnselected:true};responseRows=rows.filter(r=>r.portfolioRank!=null);
  c.AnalyticsSets.handlePeriodGesture(7,3,{altKey:true});
  c.AnalyticsPortfolio.renderTable(state,callbacks);await tick();await tick();
  assert.equal(unselectedLabel.hidden,true,'No comparison entries means no toggle');
  c.AnalyticsPortfolio.reset();assert.equal(c.AnalyticsPortfolio.active(),false);
  console.log('Production portfolio set/table handlers: passed');
}
main().catch(e=>{console.error(e);process.exitCode=1;});
