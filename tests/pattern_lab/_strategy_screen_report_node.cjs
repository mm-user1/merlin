const fs=require('fs'),path=require('path'),assert=require('assert/strict'),vm=require('vm');
const html=fs.readFileSync(process.argv[2],'utf8');
const data=JSON.parse(html.match(/<script id="screen-data" type="application\/json">([\s\S]*?)<\/script>/)[1]);
const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
vm.runInThisContext(script,{filename:'generated-strategy-screen.js'});
const screen=globalThis.SSRScreen,chart=globalThis.SSRLinkedChart;
let state=screen.initial();assert.equal(state.compact,true);assert.equal(state.sameFieldColors,true);
const views=screen.views(data,state);assert.ok(views.overview.includes('Success retained'));assert.ok(views.definitions.includes('Trade minima'));
for(const scope of Object.keys(data.scopes)){
 const next=screen.transition(data,state,{type:'scope',value:scope});
 const html=screen.views(data,next);
 assert.ok(html['scope-title']);assert.ok(!Object.values(html).join('').includes('NaN'));
 for(const key of ['interesting','profitable','positiveWithoutBest2','annualizedProfitPct','mtmMaxDdPct','tradesPerMonth']){
  assert.ok(screen.matrixValues(data,data.scopes[scope],key));
 }
}
state=screen.transition(data,state,{type:'metric',value:'mtmMaxDdPct'});
state=screen.transition(data,state,{type:'compact',value:true});assert.equal(state.compactGroup,1);
state=screen.transition(data,state,{type:'overviewMetric',value:'interesting'});assert.equal(state.compactGroup,0);
state=screen.transition(data,state,{type:'compact',value:false});assert.equal(state.metric,'mtmMaxDdPct');
state=screen.transition(data,state,{type:'fieldGroup',value:'interesting'});assert.equal(state.fieldGroup,'interesting');
state=screen.transition(data,state,{type:'fieldGroup',value:'interesting'});assert.equal(state.fieldGroup,'all');
assert.equal(screen.cardColor(data,'profitable',null),'#dddddd');
assert.equal(screen.overviewDelta(0,1,'mtmMaxDdPct').tone,'negative');
const witness=structuredClone(data),first=witness.detail.settings[0];
first.periods.earlier.classification={valid:false,lowActivity:true,profitable:null,interesting:null};
first.periods.recent.classification={valid:true,lowActivity:false,profitable:true,interesting:true};
assert.ok(screen.fieldCohort(witness,{...screen.initial(),fieldGroup:'lowActivity'}).some(s=>s.id===first.id));
const lines=chart.series(witness,'all',true);
assert.ok(chart.matches(lines[0],{...chart.initialFilters(),category:'lowActivity'}));
assert.ok(!chart.matches(lines[0],{...chart.initialFilters(),category:'loss'}));
const p0=witness.meta.periods[0],p1=witness.meta.periods[1];
p0.start='2020-01-01T00:00:00Z';p0.end='2020-02-01T00:00:00Z';
p1.start='2021-04-01T00:00:00Z';p1.end='2021-05-01T00:00:00Z';
witness.scopes.all.periods.earlier.monthly=[{month:'2020-01'}];
witness.scopes.all.periods.recent.monthly=[{month:'2021-04'}];
first.periods.earlier.cumulativeReturnPct=[50];first.periods.recent.cumulativeReturnPct=[-20];
let linked=chart.series(witness,'all',false)[0];
assert.ok(Math.abs(linked.points.at(-1).v-20)<1e-10);
assert.equal(linked.points[2].breakBefore,true);assert.equal(linked.points[2].v,50);
assert.equal(linked.points[2].t,Date.parse(p1.start));
assert.ok(Math.abs(chart.rankValue(linked,'linked')-20)<1e-10);
first.periods.earlier.cumulativeReturnPct=[null];first.periods.earlier.returnPct=55;
linked=chart.series(witness,'all',false)[0];
assert.equal(linked.points.at(-1).v,null);assert.equal(chart.rankValue(linked,'linked'),null);
assert.equal(chart.rankValue(linked,'earlier'),55);
const colors=[{ticker:'a',tf:60,periods:{earlier:{returnPct:-60}}},{ticker:'a',tf:60,periods:{earlier:{returnPct:45}}}];
const domain=chart.colorDomain(colors,chart.initialFilters());assert.deepEqual(domain,{min:-60,max:45,n:2});
assert.equal(chart.returnColor(-60,domain),'rgb(213,57,55)');assert.equal(chart.returnColor(45,domain),'rgb(13,157,84)');
assert.equal(chart.returnColor(0,domain),'rgb(128,139,136)');
const tie=lines.slice(0,2);for(const l of tie)l.periods.earlier.returnPct=1;
assert.deepEqual(chart.filterLines(tie,{...chart.initialFilters(),min:true,max:true}).map(x=>x.id),[tie[0].id]);
const monthly=screen.views(data,{...screen.initial(),monthMode:'monthly',month:data.scopes.all.periods.earlier.monthly[0].month},['months','month-detail']);
assert.ok(monthly['month-detail'].includes('distribution'));
console.log(JSON.stringify({ok:true,node:process.version,scopes:Object.keys(data.scopes).length,settings:data.detail.settings.length,
 checks:['actual shipped scripts','all scope views','native metric deltas','matrix transitions','low activity independent of classification','gap segments','exact linked endpoints','hues','stable ties','monthly inspector']}));

