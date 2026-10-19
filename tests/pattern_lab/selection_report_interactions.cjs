"use strict";
const fs=require('node:fs'),assert=require('node:assert/strict'),path=require('node:path');
const report=require('../../tools/pattern_lab/selection/report.js');
const html=fs.readFileSync(process.argv[2],'utf8');
const match=html.match(/<script id="selection-data" type="application\/json">([\s\S]*?)<\/script>/);
assert(match,'generated HTML payload');const data=JSON.parse(match[1]);
assert.equal(data.schema,'pattern-lab.selection-summary.v1');
assert(data.coverage.some(r=>r.trades>0),'genuine generated native trades');
class Element {
  constructor(){this.value='';this.content='';this.listeners={};}
  set innerHTML(value){this.content=value;const m=value.match(/<option value="([^"]+)"/);if(m&&!this.value)this.value=m[1];}
  get innerHTML(){return this.content;}
  addEventListener(name,fn){this.listeners[name]=fn;}
}
const elements=Object.fromEntries(['selector','k','results','overview','attribution'].map(k=>[k,new Element()]));
report.mount(data,{getElementById:id=>elements[id]});
let checks=0;
for(const selector of data.request.selectors)for(const k of data.request.kValues){
  elements.selector.value=selector.selectorId;elements.k.value=String(k);elements.selector.listeners.change();elements.k.listeners.change();
  assert(elements.results.innerHTML.includes('Monthly capital'));
  assert(elements.results.innerHTML.includes('Ranking, exclusions'));
  const selected=report.choose(data,selector.selectorId,k);assert.equal(selected.k,k);
  assert.equal(selected.selectorId,selector.selectorId);checks+=4;
}
assert.equal(report.percentage(null),'Unavailable');assert.equal(report.number(null),'Unavailable');
assert.equal(report.equivalent(1,1+1e-10),true);assert.equal(report.equivalent(null,0),false);
assert.equal(report.escape('<script>'),'&lt;script&gt;');
const c=structuredClone(data.comparisons[0]);
c.fullPeriodAvailable=false;c.fullPeriod.selected.cumulativeReturnPp=null;c.fullPeriod.selected.maxDrawdownPp=null;
c.matchedAvailableMonths.months=[];c.matchedAvailableMonths.usedMonths=0;
c.matchedAvailableMonths.selectedReturnPp=null;c.matchedAvailableMonths.allEligibleReturnPp=null;
c.matchedAvailableMonths.distribution={p10:null,median:null,p90:null,below:null,equal:null,above:null};
c.months[0].available=false;c.months[0].unavailableReasons=[{instrumentId:'<label>',reason:'insufficientCoverage'}];
c.months[0].selected.returnPct=null;c.months[0].boundaryTie={totalMembers:3,slots:1,chosen:['<chosen>']};
c.months[0].tieNeutralExpectedReturnPct=null;
let rendered=report.render(data,c);
assert(rendered.includes('Matched available-month subtotal'));
assert(rendered.includes('Full-period total and drawdown remain unavailable'));
assert(rendered.includes('&lt;label&gt;'));assert(rendered.includes('&lt;chosen&gt;'));
assert(!rendered.includes('Unavailable%'));assert(!rendered.includes('NaN'));
assert(rendered.includes('0 of '+c.matchedAvailableMonths.requestedMonths));
assert(html.includes('Display sampling preserves monthly endpoints'));
for(const comparison of data.comparisons)for(const segments of Object.values(comparison.displayPaths)){
  assert(segments.reduce((n,s)=>n+s.points.length,0)<=Math.max(2000,4*comparison.months.length+1));checks++;
}
console.log(JSON.stringify({status:'passed',assertions:checks+13,selectors:data.request.selectors.length,kValues:data.request.kValues}));
