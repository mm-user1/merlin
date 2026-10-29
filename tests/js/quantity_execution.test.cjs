'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const root=path.resolve(__dirname,'../..'),elements=new Map();
class Element {
  constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.style={};this.dataset={};this.value='';this.type='';this.handlers={};this.classList={add(){},remove(){},toggle(){}};}
  set id(id){this._id=id;elements.set(id,this);} get id(){return this._id;}
  set innerHTML(value){for(const n of this.children){n.innerHTML='';if(n.id)elements.delete(n.id);}this.children=[];}
  appendChild(node){this.children.push(node);node.parentElement=this;if(this.tagName==='SELECT'&&(node.selected||this.children.length===1))this.value=node.value;return node;}
  append(...nodes){nodes.forEach(n=>this.appendChild(n));}
  addEventListener(name,fn){this.handlers[name]=fn;}
  setAttribute(name,value){this[name]=value;}
  querySelector(selector){return selector==='label'?this.children.find(n=>n.tagName==='LABEL'):null;} querySelectorAll(){return [];}
}
const c={console,structuredClone,document:{getElementById:id=>elements.get(id),createElement:tag=>new Element(tag),
  querySelector:()=>null,querySelectorAll:()=>[...elements.values()].filter(e=>['INPUT','SELECT'].includes(e.tagName)),addEventListener(){}},
  formatParamValue:String,formatParamName:String,createParamId:()=>'',getParamDisplayOrder:params=>Object.keys(params)};
c.window=c;vm.createContext(c);
const load=name=>vm.runInContext(fs.readFileSync(path.join(root,'src/ui/static/js',name),'utf8'),c);
['backtestParamsContent','optimizerParamsContainer','tickerSelectorEditor','tickerSelectorSettings','tickerSelectorError',
  'enableTickerSelector','selectorTimeframe','selectorTopN','selectorRebalance','selectorUnselected','selectorLookback',
  'paramDetailsContent','paramDetailsSection','paramDetailsTitle'].forEach(id=>{const e=new Element();e.id=id;});
load('strategy-config.js');load('ticker-selector.js');load('queue.js');load('results-tables.js');
const selector={version:1,enabled:true,method:'meanBarRangePct',methodVersion:1,topN:2,rebalanceMonths:1,evaluateUnselected:false,params:{lookbackMonths:1,timeframeMinutes:30}};
const ids=['s03_reversal_v11_regime_er_b2','s03_reversal_v16_4_a_adaptive_ma_b2','s06_r_trend_v02_b2','s06_r_trend_v02_regime_trendlines_b2','s06_r_trend_v06_4_a2_b2'];
(async()=>{
  for(const id of ids){
    const config=JSON.parse(fs.readFileSync(path.join(root,'src/strategies',id,'config.json'),'utf8'));
    config.tickerSelectorCapability={policy:'instrument_rules_v1'};c.currentStrategyConfig=config;
    c.generateBacktestForm(config);const contract=elements.get('backtest_contractSize');
    contract.value='0.1';c.TickerSelector.restore(selector);
    assert.equal(contract.disabled,true);assert.equal(contract.value,'0.1');
    assert.equal(elements.get('quantityRulesHint').textContent,'From PQ instrument rules');
    assert.equal(elements.get('quantityRulesHint').title,'From Parquet instrument rules');
    assert.equal(contract.style.flexShrink,'0');assert.equal(contract.parentElement.querySelector('label').style.flexShrink,'0');
    assert.equal(elements.get('quantityRulesHint').hidden,false);
    const snapshot=c.collectQueueUiSnapshot();contract.value='99';c.applyQueueUiSnapshot(snapshot);c.TickerSelector.restore(selector);
    assert.equal(contract.value,'0.1');assert.equal(contract.disabled,true);
    c.TickerSelector.restore({...selector,enabled:false});assert.equal(contract.disabled,false);assert.equal(contract.value,'0.1');
    assert.equal(elements.get('quantityRulesHint').hidden,true);
    c.TickerSelector.restore(selector);
    c.currentStrategyConfig={engine:'v1',tickerSelectorCapability:null};c.TickerSelector.sync();
    assert.equal(contract.disabled,false);assert(elements.get('tickerSelectorError').textContent.includes('supported V2'));
    c.currentStrategyConfig=config;c.generateBacktestForm(config);c.TickerSelector.sync();
    assert.equal(String(elements.get('backtest_contractSize').value),String(config.parameters.contractSize.default),'strategy changes use config defaults');
    c.ResultsState={strategyConfig:config,activeTab:'optuna',savedConfig:{quantity_execution:{version:1,mode:'instrument_rules_v1',rules:{instrument_id:'BYBIT_AAAUSDT',base_step:'0.025',base_minimum:'0.075',minimum_lots:3,minimum_notional:null}}}};
    await c.showParameterDetails({param_id:'test',params:{contractSize:99}});
    const cards=elements.get('paramDetailsContent').children;
    assert.equal(cards.length,1);assert.equal(cards[0].children[0].textContent,'Quantity rules');
    assert(cards[0].children[1].textContent.includes('Base-unit step: 0.025 · Minimum quantity: 0.075'));
    c.ResultsState.savedConfig={};await c.showParameterDetails({param_id:'ordinary',params:{contractSize:99}});
    assert.equal(elements.get('paramDetailsContent').children[0].children[1].children[1].textContent,'99');
  }
  c.currentStrategyConfig={tickerSelectorCapability:{policy:'risk_bracket_v1'}};c.TickerSelector.restore(selector);
  assert.equal(elements.get('backtest_contractSize').disabled,false);assert.equal(elements.get('quantityRulesHint').hidden,true);
  console.log('Real quantity selector, form, Queue snapshot and Results handlers: passed');
})().catch(error=>{console.error(error);process.exitCode=1;});
