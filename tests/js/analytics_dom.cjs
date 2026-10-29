// Minimal DOM mechanics for production Analytics handler tests; no layout claims.
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
  get hidden(){return 'hidden' in this.attrs;} set hidden(v){if(v)this.attrs.hidden='';else delete this.attrs.hidden;}
  setAttribute(k,v){this.attrs[k]=String(v);if(k.startsWith('data-'))this.dataset[k.slice(5).replace(/-([a-z])/g,(_,x)=>x.toUpperCase())]=String(v);if(['type','value'].includes(k))this[k]=v;if(['checked','disabled','hidden'].includes(k))this[k]=true;}
  getAttribute(k){return this.attrs[k]??null;}
  removeAttribute(k){delete this.attrs[k];}
  appendChild(n){n.parentElement=this;this.children.push(n);return n;}
  append(...ns){ns.forEach(n=>this.appendChild(n));}
  addEventListener(k,fn){(this.handlers[k]??=[]).push(fn);}
  matches(selector){
    return selector.split(',').some(s=>{
      s=s.trim();
      if([...s.matchAll(/:not\(([^)]+)\)/g)].some(m=>this.matches(m[1])))return false;
      s=s.replace(/:not\([^)]+\)/g,'');
      const attrs=[...s.matchAll(/\[([^=\]]+)(?:="([^"]*)")?\]/g)];s=s.replace(/\[[^\]]*\]/g,'');
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
  getBoundingClientRect(){return {top:0,bottom:100,height:100};}
  focus(){} contains(n){return n===this||this.children.some(c=>c.contains(n));}
  remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(c=>c!==this);}
}
function fire(node,type,options={}){const e={target:node,preventDefault(){this.defaultPrevented=true;},stopPropagation(){this.stopped=true;},...options};for(let n=node;n;n=n.parentElement){for(const fn of n.handlers[type]||[])fn(e);if(e.stopped)break;}return e;}

module.exports = {Element, fire};
