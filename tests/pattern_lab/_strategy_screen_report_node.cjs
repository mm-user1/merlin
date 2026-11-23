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


// F3: shipped interval labels must share the indexed binary64 boundary.
{
 const i=2**51+1,lo=0,step=10,hi=(i+5)*step;
 const h={lo,hi,step,bins:i+5,indices:[i,i+1],counts:[1,1],n:2};
 const d={meta:{periods:data.meta.periods},scopes:{all:{periods:Object.fromEntries(data.meta.periods.map(p=>[p.id,{distributions:{returnPct:h}}]))}}};
 const markup=screen.views(d,{...screen.initial(),fieldView:'distribution',distribution:'returnPct'},['field-content'])['field-content'];
 const intervals=[...markup.matchAll(/aria-label="1 settings in ([^"]+)"/g)].slice(0,2).map(m=>m[1].replaceAll(',','').replace('%','').split(' to ').map(Number));
 assert.equal(intervals[0][1],intervals[1][0]);
 const value=22517998136852496;
 assert.ok(intervals[0][0]<=value&&value<intervals[0][1]);
 assert.deepEqual(intervals,[[lo+i*step,lo+(i+1)*step],[lo+(i+1)*step,lo+(i+2)*step]]);
}
// F4: identity, never presentation text or the compact marker alone, owns wording.
for(const [schema,implementation,synthetic,label] of [
 ['pattern-lab.strategy-screen.v1',{native:true},false,'Native account evidence'],
 ['pattern-lab.strategy-screen.v1',{fixture:true},true,'Synthetic compact fixture; no native execution'],
 ['gex021.exact-facts.v1',{legacyFixture:true},false,'Legacy analytical evidence; no native completion seal']]){
 const d={...data,synthetic,meta:{...data.meta,nativeSchema:schema,nativeImplementation:implementation,subtitle:'Native account evidence'}};
 const markup=screen.views(d,screen.initial(),['definitions']).definitions;
 assert.ok(markup.includes(label));if(schema.startsWith('gex'))assert.ok(!markup.includes('Native account evidence'));
}
// F5: independently inspect coordinates, thresholds, counts and paired accessible fields.
for(const threshold of [-10,-30,0,1e308])for(const count of [0,1]){
 const d={...data,meta:{...data.meta,criteria:{...data.meta.criteria,annualizedProfitPct:threshold,mtmMaxDdPct:0}},detail:{...data.detail,settings:[]}};
 if(count){
  const s=structuredClone(data.detail.settings[0]);
  for(const p of Object.values(s.periods))Object.assign(p,{annualizedProfitPct:threshold,mtmMaxDdPct:0,classification:{valid:true,lowActivity:false}});
  d.detail.settings=[s];
 }
 const markup=screen.views(d,screen.initial(),['field-content'])['field-content'];
 assert.ok(!/NaN|Infinity/.test(markup));
 assert.equal([...markup.matchAll(/<circle /g)].length,count*2);
 assert.equal([...markup.matchAll(new RegExp(`${count} shown / ${count} selected.*?0 unavailable`,'g'))].length,2);
 for(const m of markup.matchAll(/<path d="M60,([^H]+)H([^V]+)V24"/g)){
  assert.ok(+m[1]>=24&&+m[1]<=317);assert.ok(+m[2]>=60&&+m[2]<=740);
 }
 for(const m of markup.matchAll(/<line class="plot-grid" x1="([^"]+)" x2="([^"]+)" y1="([^"]+)" y2="([^"]+)"/g)){
  assert.ok(+m[1]>=60&&+m[2]<=740);assert.ok(+m[3]>=24-1e-10&&+m[4]<=317+1e-10);
 }
 for(const m of markup.matchAll(/cx="([^"]+)" cy="([^"]+)"/g)){assert.ok(+m[1]>=60&&+m[1]<=740);assert.ok(+m[2]>=24&&+m[2]<=317);}
 if(!count)assert.ok(markup.includes('No settings'));
}
for(const unavailable of ['earlier','recent']){
 const s=structuredClone(data.detail.settings[0]),other=unavailable==='earlier'?'recent':'earlier';
 for(const p of Object.values(s.periods))Object.assign(p,{annualizedProfitPct:0,mtmMaxDdPct:0,trades:0,classification:{valid:true,lowActivity:true},coverageComplete:true,effectiveCriteria:{minClosedTrades:37},availability:{annualizedProfitPct:{available:true},mtmMaxDdPct:{available:true},trades:{available:true}}});
 s.periods[unavailable].annualizedProfitPct=null;s.periods[unavailable].classification.valid=false;
 s.periods[unavailable].availability.annualizedProfitPct={available:false,reason:'shortSpan'};
 const d={...data,detail:{...data.detail,settings:[s]}};
 const markup=screen.views(d,screen.initial(),['field-content'])['field-content'];
 const points=[...markup.matchAll(/<circle[^>]+aria-label="([^"]+)"/g)];assert.equal(points.length,1);
 assert.ok(points[0][0].includes('tabindex="0"'));
 for(const text of ['N/A (shortSpan)','MTM DD 0.0%','0 trades','Coverage complete','Trade minimum 37'])assert.ok(points[0][1].includes(text),text);
}
console.log(JSON.stringify({ok:true,corrections:['F3 indexed sparse boundaries','F4 three evidence identities','F5 empty/extreme coordinates and paired accessible availability']}));
// T03B-2: compare literal digits to exact integers, never to Number(label).
{
 const big=2**51+1;
 for(const h of [
  {lo:0,hi:(big+3)*10,step:10,bins:big+3,indices:[big,big+1,big+2],counts:[2,1,1],n:4},
  {lo:-(big+3)*10,hi:0,step:10,bins:big+3,indices:[0,1,2],counts:[2,1,1],n:4},
  {lo:-12,hi:12,step:6,bins:4,indices:[0,1,2,3],counts:[1,1,1,2],n:5},
  {lo:-20,hi:20,step:10,bins:4,indices:[0,1,2,3],counts:[1,1,1,2],n:5}]){
  const d={meta:{periods:data.meta.periods},scopes:{all:{periods:Object.fromEntries(data.meta.periods.map(p=>[p.id,{distributions:{returnPct:h}}]))}}};
  const markup=screen.views(d,{...screen.initial(),fieldView:'distribution',distribution:'returnPct'},['field-content'])['field-content'];
  const bars=[...markup.matchAll(/<rect [^>]+>/g)].slice(0,h.indices.length);
  const digits=x=>x.replaceAll(',','').replace(/\.0$/,'');
  assert.equal(bars.length,h.indices.length);
  for(let j=0;j<bars.length;j++){
   const i=h.indices[j],a=BigInt(h.lo+i*h.step),b=BigInt(Math.min(h.hi,h.lo+(i+1)*h.step));
   const aria=bars[j][0].match(/aria-label="(\d+) settings in (.*?) to (.*?)%"/);
   const tip=bars[j][0].match(/data-tip="[^&]*&#10;(.*?)% to (.*?)% (\(.*?\))&#10;(\d+) settings"/);
   assert.ok(aria&&tip,bars[j][0]);
   assert.deepEqual([digits(aria[2]),digits(aria[3])],[a.toString(),b.toString()]);
   assert.deepEqual([digits(tip[1]),digits(tip[2])],[a.toString(),b.toString()]);
   assert.equal(+aria[1],h.counts[j]);assert.equal(+tip[4],h.counts[j]);
   assert.equal(tip[3],i===h.bins-1?'(inclusive)':'(upper exclusive)');
   if(j)assert.equal(a,BigInt(h.lo+(h.indices[j-1]+1)*h.step));
   if(h.lo===0&&j===0){
    assert.equal(digits(aria[2]),'22517998136852488');assert.equal(digits(aria[3]),'22517998136852500');
    for(const v of [22517998136852488n,22517998136852496n])assert.ok(a<=v&&v<b);
   }
  }
  const domain=[...markup.matchAll(/<text [^>]*y="148"[^>]*>(.*?)%<\/text>/g)].slice(0,3).map(m=>digits(m[1]));
  assert.deepEqual(domain,[BigInt(h.lo).toString(),'0',BigInt(h.hi).toString()]);
 }
 console.log(JSON.stringify({ok:true,exactLiteralIntervals:true,oracle:'BigInt integral digits; no Number round trip'}));
}
