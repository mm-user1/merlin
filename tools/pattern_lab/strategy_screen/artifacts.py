"""Bounded six-file artifacts and stored-only, deterministic numerical export."""
from copy import deepcopy
from pathlib import Path
import stat
import os
import zipfile

from ..accounts.contracts import require
from .. import PatternLabDataError
from ..account_study.common import closed, json_text, parse_json, file_hash, digest_id
from ..account_study.containers import zip_info
from ..configuration_study import codec
from ..study.contracts import semantic_digest
from .request import normalize_request, configurations, result_id, child_request, MAX_JSON, effective_criteria
from . import facts, analytics

SCHEMA='pattern-lab.strategy-screen.v1'
INDEX='pattern-lab.strategy-screen-index.v1'
CHUNK='pattern-lab.strategy-screen-chunk.v1'
COMPLETION='pattern-lab.strategy-screen-completion.v1'
FILES=('study.json','index.json','results.zip','provenance.json','status.json')
LIMITATION='Compact consistency only; full execution, per-fill fee arithmetic, daily Sharpe chronology and drawdown-path chronology cannot be replayed from monthly facts.'


def safe_path(value):
    path=Path(value).expanduser().absolute()
    # Inspect from the root down, before traversing a possibly linked component.
    for p in reversed((path,*path.parents)):
        try: info=p.lstat()
        except FileNotFoundError: continue
        require(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&0x400,
                'symlink/reparse path refused')
    return path.resolve()


def output_path(value, *inputs):
    path=safe_path(value)
    repo=Path(__file__).resolve().parents[3]
    for source in (*inputs,repo/'src',repo/'data',repo/'tests'):
        source=safe_path(source)
        require(not path.is_relative_to(source) and not source.is_relative_to(path),'overlapping/protected output')
    require(not path.exists(),'new output directory required')
    for parent in path.parents:
        names=set()
        for name in ('completion.json','study.json','selection.json','status.json','analysis.json','receipt.json'):
            try:(parent/name).lstat()
            except FileNotFoundError:continue
            names.add(name)
        # Names alone reserve ownership; never open/follow a marker.
        require(not names.intersection(('completion.json','study.json','selection.json','status.json')) and
                not {'analysis.json','receipt.json'}<=names,'output nested in an owned artifact')
    return path


def reserve(path):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.mkdir(exist_ok=False)
    return path


def json_bytes(value):
    raw=(json_text(value)+('\r\n' if os.name=='nt' else '\n')).encode('utf-8')
    require(len(raw)<=MAX_JSON,'root JSON byte bound')
    return raw


def save_bytes(path,raw):
    path=Path(path);temporary=path.with_name(path.name+'.tmp')
    temporary.write_bytes(raw)
    os.replace(temporary,path)


def save_json(path,value):
    save_bytes(path,json_bytes(value))


def read_json(path):
    with Path(path).open('rb') as f:raw=f.read(MAX_JSON+1)
    require(len(raw)<=MAX_JSON,'root JSON byte bound')
    return parse_json(raw.decode('utf-8'))


def artifact_identity(study,index):
    return semantic_digest(dict(identities=study['identities'],synthetic=study['synthetic'],index=index))


def seal(root):
    save_json(root/'completion.json',dict(schema=COMPLETION,
        members={n:dict(bytes=(root/n).stat().st_size,sha256=file_hash(root/n)) for n in FILES}))


def completion_valid(root):
    """The atomic successful marker is the commit point, including late exceptions."""
    try:
        c=read_json(root/'completion.json')
        return c['schema']==COMPLETION and set(c['members'])==set(FILES) and all(
            (root/n).stat().st_size==v['bytes'] and file_hash(root/n)==v['sha256'] for n,v in c['members'].items())
    except (OSError,ValueError,KeyError,TypeError,PatternLabDataError):return False


class Writer:
    def __init__(self,root,request,implementation,*,synthetic=False):
        self.root=Path(root);self.request=normalize_request(request);self.doc=self.request.document
        self.rows=configurations(self.doc);self.row_map={r['configurationId']:r for r in self.rows}
        self.periods={p['periodId']:p for p in self.doc['periods']}
        self.children={(p['periodId'],semantic_digest([c['instrumentId'],c['timeframeMinutes']])):child_request(self.doc,c,p)
            for p in self.doc['periods'] for c in self.doc['cells']}
        self.implementation=implementation;require(type(synthetic) is bool,'synthetic flag');self.synthetic=synthetic
        self.expected=[(r['configurationId'],p['periodId']) for p in reversed(self.doc['periods']) for r in self.rows]
        self.entries=[];self.pending=[];self.written=[];self.input_ids=[];self.total=0;self.projections=[]
        self.archive=zipfile.ZipFile(self.root/'results.zip','x',compression=zipfile.ZIP_STORED,allowZip64=False)

    def close(self):
        self.archive.close()

    def member(self,value,kind,ids):
        packed,info=codec.encode(value)
        require(len(self.entries)<codec.MAX_MEMBERS and self.total+info['decodedBytes']<=codec.MAX_TOTAL,'member/aggregate decoded bound')
        name='chunks/'+semantic_digest([kind,ids])+'.json.gz'
        directory=sum(46+len(e['name']) for e in self.entries)+46+len(name)+22
        require(self.archive.fp.tell()+30+len(name)+len(packed)+directory<=codec.MAX_ARCHIVE,'final archive capacity')
        self.archive.writestr(zip_info(name),packed)
        self.entries.append(dict(name=name,kind=kind,ids=ids,**info));self.total+=info['decodedBytes']

    def flush(self):
        if self.pending:
            self.member(dict(schema=CHUNK,records=self.pending),'results',[r['facts']['resultId'] for r in self.pending])
            self.pending=[]

    def append(self,unit):
        coordinate=(unit['configurationId'],unit['periodId'])
        require(len(self.written)<len(self.expected) and coordinate==self.expected[len(self.written)],'canonical result roster')
        c=self.row_map[coordinate[0]];p=self.periods[coordinate[1]]
        facts.validate(unit,c,p,self.children[(p['periodId'],c['cellId'])],synthetic=self.synthetic)
        projection=analytics.project(unit,c,self.doc)
        row=dict(facts=unit,classification=projection['classification'],analytics=analytics.result_analytics(projection,self.doc))
        trial=dict(schema=CHUNK,records=self.pending+[row])
        if self.pending and (len(self.pending)>=16 or len(json_text(trial).encode())>codec.MAX_MEMBER):self.flush()
        require(len(json_text(dict(schema=CHUNK,records=[row])).encode())<=codec.MAX_MEMBER,'single record too large')
        self.pending.append(row);self.written.append(coordinate);self.projections.append(projection)
        self.input_ids.append((unit['resultId'],semantic_digest(unit['execution'])))

    def finish(self,provenance,status,*,before_completion=None):
        require(self.written==self.expected,'all declared results required');self.flush()
        for summary in analytics.derive(self.doc,self.projections):
            self.member(dict(schema=CHUNK,records=[summary]),'summary',[summary['scopeId']])
        self.projections.clear();self.close()
        require((self.root/'results.zip').stat().st_size<=codec.MAX_ARCHIVE,'final archive size')
        index=dict(schema=INDEX,totalDecodedBytes=self.total,members=self.entries)
        study=dict(schema=SCHEMA,formatVersion=1,request=self.doc,configurations=self.rows,implementation=self.implementation,
            identities=dict(specification=self.request.identity,implementation=semantic_digest(self.implementation),dataInput=semantic_digest(self.input_ids)),
            synthetic=self.synthetic,counts=dict(configurations=len(self.rows),plannedResults=len(self.expected),completedResults=len(self.expected),
                months=sum(len(facts.calendar(p))*len(self.rows) for p in self.doc['periods'])),
            verificationLevel='compact',fullExecutionRetained=False,limitation=LIMITATION,artifactIdentity=None)
        study['artifactIdentity']=artifact_identity(study,index)
        save_json(self.root/'study.json',study);save_json(self.root/'index.json',index)
        save_json(self.root/'provenance.json',dict(provenance,synthetic=self.synthetic))
        save_json(self.root/'status.json',dict(status,state='complete',phase='complete',completedResults=len(self.expected)))
        _load(self.root,sealed=False,verify=True)
        if before_completion is not None:before_completion()
        seal(self.root)


class StrategyScreen:
    def __init__(self,root,study,index,archive,locations,summaries):
        self.root=root;self._study=study;self._index=index;self._archive=archive
        self._locations=locations;self._summaries=summaries
        self._entries={e['name']:e for e in index['members']}
    @property
    def metadata(self):return deepcopy(self._study)
    @property
    def configurations(self):return deepcopy(self._study['configurations'])
    @property
    def chunk_index(self):return deepcopy(self._index)
    @property
    def result_rows(self):
        return [dict(configurationId=c,periodId=p,resultId=result_id(c,next(x for x in self._study['request']['periods'] if x['periodId']==p)),status='complete')
                for c,p in self._locations]
    def raw_member(self,name):
        require(name in self._entries,'unknown member')
        raw=self._archive.read(name);require(codec.sha(raw)==self._entries[name]['sha256'],'raw member integrity');return raw
    def _records(self,entry):return codec.decode(self.raw_member(entry['name']),entry)['records']
    def result(self,configurationId,periodId):
        require((configurationId,periodId) in self._locations,'unknown result')
        entry,index=self._locations[(configurationId,periodId)];r=self._records(entry)[index]
        return dict(r['facts'],classification=r['classification'],analytics=r['analytics'])
    def months(self,configurationId,periodId):return self.result(configurationId,periodId)['months']
    def summary(self,scopeId='all'):
        require(scopeId in self._summaries,'unknown scope');return self._records(self._summaries[scopeId])[0]
    @property
    def scopes(self):return analytics.scopes(self._study['request'])


def _load(run_root,*,sealed=True,verify=False):
    root=safe_path(run_root);require(root.is_dir(),'screen directory required')
    expected=set(FILES)|({'completion.json'} if sealed else set())
    require({p.name for p in root.iterdir()}==expected,'exact screen root files')
    for name in expected:
        p=root/name;require(p.is_file() and not p.is_symlink() and not p.is_junction(),'regular root files')
        require(p.stat().st_size<=(codec.MAX_ARCHIVE if name=='results.zip' else MAX_JSON),'root resource bound')
    if sealed:
        completion=read_json(root/'completion.json');closed(completion,('schema','members'))
        require(completion['schema']==COMPLETION and set(completion['members'])==set(FILES),'completion schema/roster')
        for name,entry in completion['members'].items():
            closed(entry,('bytes','sha256'))
            require((root/name).stat().st_size==entry['bytes'] and file_hash(root/name)==entry['sha256'],'root seal integrity')
    study=read_json(root/'study.json')
    closed(study,('schema','formatVersion','request','configurations','implementation','identities','synthetic','counts',
        'verificationLevel','fullExecutionRetained','limitation','artifactIdentity'))
    require(study['schema']==SCHEMA and type(study['formatVersion']) is int and study['formatVersion']==1,'screen schema/version')
    raw=study['request']
    require(type(raw) is dict and type(raw.get('periods')) is list and len(raw['periods'])==2 and
        all(type(p) is dict and type(p.get('protocol')) is dict for p in raw['periods']), 'stored request and protocols must be objects')
    request=normalize_request(raw);doc=request.document;rows=configurations(doc)
    require(raw==doc and study['configurations']==rows,'normalized request/roster')
    require(type(study['synthetic']) is bool and study['verificationLevel']=='compact' and study['fullExecutionRetained'] is False and
        study['limitation']==LIMITATION,'verification level')
    identities=study['identities'];closed(identities,('specification','implementation','dataInput'))
    for value in identities.values():digest_id(value)
    implementation=study['implementation']
    require(type(implementation) is dict,'recorded implementation object')
    if not study['synthetic']:
        closed(implementation,('version','sources','python','libraries'))
        require(implementation['version']=='strategy-screen.1' and type(implementation['python']) is str and
            type(implementation['libraries']) is dict and type(implementation['sources']) is dict and implementation['sources'],'recorded implementation shape')
        for name,value in implementation['sources'].items():
            require(type(name) is str and not Path(name).is_absolute() and '\\' not in name and '..' not in name.split('/'),'recorded source name')
            digest_id(value)
    require(identities['specification']==request.identity and identities['implementation']==semantic_digest(study['implementation']),'recorded specification/implementation identity')
    require(study['counts']==dict(configurations=len(rows),plannedResults=2*len(rows),completedResults=2*len(rows),
        months=sum(len(facts.calendar(p))*len(rows) for p in doc['periods'])),'metadata counts')
    status=read_json(root/'status.json');provenance=read_json(root/'provenance.json')
    require(status['state']==status['phase']=='complete' and status['completedResults']==2*len(rows) and
        status['plannedResults']==2*len(rows) and provenance['synthetic']==study['synthetic'],'completed operational status')
    index=read_json(root/'index.json');closed(index,('schema','totalDecodedBytes','members'))
    require(index['schema']==INDEX and type(index['members']) is list and 0<len(index['members'])<=codec.MAX_MEMBERS,'index schema/members')
    total=0;names=[]
    for entry in index['members']:
        closed(entry,('name','kind','ids','bytes','sha256','decodedBytes','decodedSha256'))
        require(entry['kind'] in ('results','summary') and type(entry['ids']) is list and entry['ids'],'member role/IDs')
        for k in ('bytes','decodedBytes'):
            require(type(entry[k]) is int and 0<entry[k]<=(codec.MAX_MEMBER if k=='decodedBytes' else codec.MAX_ARCHIVE),'member size bound')
        require(entry['name']=='chunks/'+semantic_digest([entry['kind'],entry['ids']])+'.json.gz','canonical chunk name')
        total+=entry['decodedBytes'];require(total<=codec.MAX_TOTAL,'aggregate decoded-size bound');names.append(entry['name'])
    require(len(names)==len(set(names)) and index['totalDecodedBytes']==total,'index totals/duplicates')
    require(study['artifactIdentity']==artifact_identity(study,index),'artifact identity')
    archive=codec.Archive(root/'results.zip',{e['name']:e for e in index['members']})
    row_map={r['configurationId']:r for r in rows};periods={p['periodId']:p for p in doc['periods']}
    children={(p['periodId'],semantic_digest([c['instrumentId'],c['timeframeMinutes']])):child_request(doc,c,p)
              for p in doc['periods'] for c in doc['cells']}
    coordinates=[(r['configurationId'],p['periodId']) for p in reversed(doc['periods']) for r in rows]
    locations={};summaries={};inputs=[];projections=[];summary_order=[]
    for entry in index['members']:
        value=codec.decode(archive.read(entry['name']),entry);closed(value,('schema','records'))
        require(value['schema']==CHUNK and type(value['records']) is list,'chunk schema')
        if entry['kind']=='results':
            require(not summaries and 0<len(value['records'])<=16,'results before summaries; bounded group')
            ids=[]
            for position,row in enumerate(value['records']):
                closed(row,('facts','classification','analytics'));unit=row['facts'];coordinate=(unit['configurationId'],unit['periodId'])
                require(len(locations)<len(coordinates) and coordinate==coordinates[len(locations)],'exact canonical result coordinates')
                c=row_map[coordinate[0]];p=periods[coordinate[1]]
                facts.validate(unit,c,p,children[(p['periodId'],c['cellId'])],synthetic=study['synthetic'])
                projection=analytics.project(unit,c,doc)
                require(row['classification']==projection['classification'],'classification rederivation')
                require(row['analytics']==analytics.result_analytics(projection,doc),'per-result analytics rederivation')
                if verify:projections.append(projection)
                locations[coordinate]=(entry,position);ids.append(unit['resultId'])
                inputs.append((unit['resultId'],semantic_digest(unit['execution'])))
            require(entry['ids']==ids,'indexed result IDs')
        else:
            require(len(locations)==len(coordinates) and len(value['records'])==1,'summary shape/order')
            s=value['records'][0];closed(s,('scopeId','kind','key','cells','periods','changes','persistence'))
            scope=next((v for v in analytics.scopes(doc) if v['scopeId']==s['scopeId']),None)
            require(scope is not None and all(s[k]==v for k,v in scope.items()) and set(s['periods'])==set(periods),'summary scope/period references')
            sid=s['scopeId'];require(sid not in summaries and entry['ids']==[sid],'summary duplicate/IDs')
            summaries[sid]=entry;summary_order.append(sid)
    require(len(locations)==len(coordinates) and summary_order==[s['scopeId'] for s in analytics.scopes(doc)],'complete result/scope roster')
    require(identities['dataInput']==semantic_digest(inputs),'recorded consumed-input identity')
    reader=StrategyScreen(root,study,index,archive,locations,summaries)
    if verify:
        for expected in analytics.derive(doc,projections):require(reader.summary(expected['scopeId'])==expected,'stored numerical summary differs')
    return reader


def load_strategy_screen(run_root):return _load(run_root)


def verify_strategy_screen(run_root):
    reader=_load(run_root,verify=True)
    return dict(ok=True,artifactIdentity=reader.metadata['artifactIdentity'],counts=reader.metadata['counts'],
        verificationLevel='compact',fullExecutionRetained=False,limitation=LIMITATION)


def analyze_strategy_screen(run_root,*,output_root):
    reader=_load(run_root,verify=True);root=output_path(output_root,reader.root)
    metadata=reader.metadata
    analysis=dict(schema='pattern-lab.strategy-screen-analysis.v1',version=analytics.VERSION,
        artifactIdentity=metadata['artifactIdentity'],metadata=metadata,scopes=[reader.summary(s['scopeId']) for s in reader.scopes])
    raw=(json_text(analysis)+'\n').encode();require(len(raw)<=MAX_JSON,'analysis JSON bound')
    source_root=Path(__file__).parent
    sources={p.name:codec.sha(p.read_bytes().replace(b'\r\n',b'\n')) for p in sorted(source_root.glob('*.py'))}
    receipt=dict(schema='pattern-lab.strategy-screen-analysis-receipt.v1',inputArtifactIdentity=metadata['artifactIdentity'],
        inputCompletionSha256=file_hash(reader.root/'completion.json'),analysisVersion=analytics.VERSION,sourcesLf=sources,
        analysis=dict(bytes=len(raw),sha256=codec.sha(raw)),verificationLevel='compact',fullExecutionRetained=False)
    require(len(json_text(receipt).encode())<=MAX_JSON,'analysis receipt bound')
    reserve(root)
    try:
        (root/'analysis.json').write_bytes(raw);save_json(root/'receipt.json',receipt)
    except BaseException:
        # This call exclusively owns this new directory; preserve unrelated files.
        for name in ('analysis.json','receipt.json','receipt.json.tmp'):
            (root/name).unlink(missing_ok=True)
        raise
    return root
