/* Run the shipped decoder without a browser. This does not certify trusted gestures. */
'use strict';
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict'),zlib=require('node:zlib');
globalThis.crypto=require('node:crypto').webcrypto;
vm.runInThisContext(fs.readFileSync(path.resolve(__dirname,'../../tools/pattern_lab/configuration_report/core.js'),'utf8'));
const Core=globalThis.ConfigurationReportCore;
const sha=raw=>require('node:crypto').createHash('sha256').update(raw).digest('hex');
async function main(){
  Core.capabilities();
  assert.notEqual(Core.key(1),Core.key('1'));assert.notEqual(Core.key(false),Core.key(null));assert.notEqual(Core.key(undefined),Core.key(null));
  assert.deepEqual(Core.scalarPaths({'a/b':{'c~d':[false,null,1]}}),{'/a~1b/c~0d/0':false,'/a~1b/c~0d/1':null,'/a~1b/c~0d/2':1});
  let blocksChecked=0,refusals=0;
  for(const filename of process.argv.slice(2)){
    const html=fs.readFileSync(filename,'utf8'),manifest=JSON.parse(html.match(/<script type="application\/json" id="bootstrap">(.*?)<\/script>/s)[1]);
    const blocks=[...html.matchAll(/<script type="application\/octet-stream" id="block-\d+">(.*?)<\/script>/g)].map(m=>m[1]);
    const configurations=new Map(manifest.metadata.configurations.map(c=>[c.configurationId,c]));
    configurations.byResult=new Map(manifest.rows.filter(r=>r.status==='complete').map(r=>[r.resultId,r.configurationId]));
    assert.equal(blocks.length,manifest.index.members.length);
    for(let i=0;i<blocks.length;i++){
      const e=manifest.index.members[i];
      const decoded=await Core.decode(blocks[i],e,configurations);
      assert.deepEqual(decoded,JSON.parse(zlib.gunzipSync(Buffer.from(blocks[i],'base64'))));
      if(manifest.metadata.visibility==='public')assert.notEqual(e.stageId,'HO2');
      blocksChecked++;
    }
    const original=Buffer.from(blocks[0],'base64'),entry=manifest.index.members[0];
    for(const kind of ['hash','length','decodedLength','overflow','truncated','damaged','concatenated','schema','identity','shape']){
      let bytes=Buffer.from(original),e={...entry};
      if(kind==='hash')e.sha256='0'.repeat(64);
      if(kind==='length')e.bytes++;
      if(kind==='decodedLength')e.decodedBytes++;
      if(kind==='overflow')e.decodedBytes--;
      if(kind==='truncated')bytes=bytes.subarray(0,-4);
      if(kind==='damaged')bytes[bytes.length-6]^=1;
      if(kind==='concatenated')bytes=Buffer.concat([bytes,bytes]);
      if(['schema','identity','shape'].includes(kind)){
        const v=JSON.parse(zlib.gunzipSync(original));
        if(kind==='schema')v.schema='bad';
        if(kind==='identity')v.records[0].resultId='bad';
        if(kind==='shape')v.records[0].metrics.returnPct='bad';
        const raw=Buffer.from(JSON.stringify(v));bytes=zlib.gzipSync(raw);e.decodedBytes=raw.length;e.decodedSha256=sha(raw);
      }
      if(!['hash','length','decodedLength','overflow'].includes(kind)){e.bytes=bytes.length;e.sha256=sha(bytes);}
      await assert.rejects(()=>Core.decode(bytes.toString('base64'),e,configurations),undefined,kind);refusals++;
    }
    const loader=new Core.Loader(manifest,i=>blocks[i]);loader.retain([0]);
    const [a,b]=await Promise.all([loader.get(0),loader.get(0)]);assert.equal(a,b);assert.equal(loader.decoded.IS,1);
    assert(loader.peak<=32*1024*1024);loader.retain([]);assert.equal(loader.bytes,0);
    const old=globalThis.DecompressionStream;globalThis.DecompressionStream=undefined;assert.throws(()=>Core.capabilities());globalThis.DecompressionStream=old;
  }
  assert(blocksChecked>0,'Pass generated report paths');
  console.log(JSON.stringify({ok:true,blocksChecked,refusals,node:process.version,shippedCore:true,browserProof:false}));
}
main().catch(error=>{console.error(error);process.exitCode=1;});
