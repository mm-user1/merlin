'use strict';
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const html=fs.readFileSync(process.argv[2],'utf8');
const payload=html.match(/<script id="selection-data" type="application\/json">([\s\S]*?)<\/script>/)[1];
const data=JSON.parse(payload),script=html.match(/<\/script><script>([\s\S]*?)<\/script>/)[1];
assert.equal(data.schema,'pattern-lab.selection-summary.v2');assert(data.coverage.some(r=>r.trades>0));
// Inspect rendered HTML, excluding all script source and embedded data.
const visible=html.replace(/<script[\s\S]*?<\/script>/g,'').replace(/<[^>]*>/g,' ');
assert(visible.includes('Even one unselected member of the eligible pool'));
assert(visible.includes('across every K and all controls'));
assert(visible.includes('need only the sealed selection bundle'));
assert(visible.includes('not included by selection export'));
for(const pin of Object.values(data.request.parent))assert(visible.includes(pin));
for(const binding of data.request.auxiliaryInputs)assert(visible.includes(binding.expectedSnapshotContentId));
for(const sources of Object.values(data.implementation.experimental))for(const hash of Object.values(sources))assert(visible.includes(hash));
class Element {
  constructor(tag='div'){this.tag=tag;this.children=[];this.textContent='';this.value='';this.listeners={};this.attributes={};}
  append(...nodes){this.children.push(...nodes);if(this.tag==='select'&&!this.value&&nodes.length)this.value=String(nodes[0].value);}
  replaceChildren(...nodes){this.children=nodes;}
  setAttribute(k,v){this.attributes[k]=v;}
  addEventListener(k,v){this.listeners[k]=v;}
  get text(){return this.textContent+this.children.map(c=>c.text).join('\n');}
}
const ids=['selection-data','overview','selector','k','results','diagnostics','attribution'];
const nodes=Object.fromEntries(ids.map(k=>[k,new Element(['selector','k'].includes(k)?'select':'div')]));nodes['selection-data'].textContent=payload;
const document={getElementById:id=>nodes[id],createElement:tag=>new Element(tag),createElementNS:(ns,tag)=>new Element(tag)};
vm.runInNewContext(script,{document,console});let checks=0;
for(const selector of data.request.selectors)for(const k of data.request.kValues){
  nodes.selector.value=selector.selectorId;nodes.k.value=String(k);nodes.selector.listeners.change();nodes.k.listeners.change();
  const text=nodes.results.text;assert(text.includes('Monthly portfolio facts'));assert(text.includes('Max drawdown'));assert(!text.includes('NaN'));
  assert(text.includes('Funded capital / idle cash (USDT)'));assert(text.includes('Opening / closing (USDT)'));
  const c=data.comparisons.find(c=>c.selectorId===selector.selectorId&&c.k===k);
  for(const m of c.months){assert(text.includes(m.month));for(const kind of ['selected','allEligible']){assert(text.includes(m[kind].members.join(', ')));checks++;}}
  for(const segments of Object.values(c.displayPaths)){assert(segments.reduce((a,s)=>a+s.points.length,0)<=Math.max(2000,4*c.months.length+1));checks++;}
  assert(nodes.diagnostics.text.includes(selector.selectorId));checks++;
}
assert(nodes.overview.text.includes(data.request.capitalMode==='carried'?'Carried monthly capital':'Fixed reference capital'));
console.log(JSON.stringify({status:'passed',mode:data.request.capitalMode,checks}));
