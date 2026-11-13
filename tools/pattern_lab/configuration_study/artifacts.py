"""Streamed compact publication, stored-only verification and byte-preserving projection."""
from copy import deepcopy
from itertools import groupby
from pathlib import Path
import zipfile

from .. import PatternLabDataError
from ..accounts.contracts import require
from ..account_study.common import closed, file_hash, fresh_directory, json_text, parse_json, write_json
from ..account_study.containers import zip_info
from ..study.contracts import semantic_digest
from . import codec, validation
from .request import MAX_JSON, STAGES, normalize_request, configurations, result_id

SCHEMA='pattern-lab.configuration-study.v1'
INDEX_SCHEMA='pattern-lab.configuration-index.v1'
ROOT_FILES=('study.json','index.json','results.zip','provenance.json','status.json')
LIMITATION='Compact integrity and consistency only; full execution is not retained and cannot be replayed from display curves.'


def bounded_json(path):
    with Path(path).open('rb') as stream:raw=stream.read(MAX_JSON+1)
    require(len(raw)<=MAX_JSON,'root JSON bound')
    return parse_json(raw.decode('utf-8'))


def save_json(path,value):
    require(len(json_text(value).encode())+1<=MAX_JSON,'root JSON bound')
    write_json(path,value)


def groups(rows):
    for scope,values in groupby(rows,key=lambda r:(r['instrumentId'],r['timeframeMinutes'])):
        yield scope,list(values)


def chunk_name(stage,instrument,tf,number,kind):
    return f'chunks/{stage}/{semantic_digest([instrument,tf])}/{number:05d}.{kind}.json.gz'


def price_name(instrument,stage):return f'price/{semantic_digest(instrument)}/{stage}.json.gz'


def artifact_identity(study,index):
    return semantic_digest(dict(identities=study['identities'],visibility=study['visibility'],
        synthetic=study['synthetic'],index=index,lineage=study['lineage']))


def seal(root):
    members={n:dict(bytes=(root/n).stat().st_size,sha256=file_hash(root/n)) for n in ROOT_FILES}
    save_json(root/'completion.json',dict(schema='pattern-lab.configuration-completion.v1',members=members))


class Writer:
    """Only native coordinator and explicitly marked synthetic fixtures create units."""
    def __init__(self,root,request,implementation,*,synthetic=False):
        self.root=Path(root);self.request=normalize_request(request)
        self.rows=configurations(self.request.document)
        self.synthetic=synthetic;require(type(synthetic) is bool,'synthetic flag')
        self.implementation=implementation
        self.archive=zipfile.ZipFile(self.root/'results.zip','x',compression=zipfile.ZIP_STORED,allowZip64=False)
        self.entries=[];self.total=0;self.input_ids=[];self.written=[]

    def close(self):self.archive.close()

    def member(self,name,value,scope):
        packed,info=codec.encode(value)
        require(codec.decode(packed,info)==value,'compact chunk round trip')
        require(len(self.entries)<codec.MAX_MEMBERS and self.total+info['decodedBytes']<=codec.MAX_TOTAL,'compact total/member bound')
        require(self.archive.fp.tell()+len(packed)+len(name)+30<=codec.MAX_ARCHIVE,'archive byte bound')
        self.archive.writestr(zip_info(name),packed)
        self.entries.append(dict(name=name,**scope,**info));self.total+=info['decodedBytes']

    def group(self,stage,instrument,tf,units):
        pending=[];number=0
        def payloads(batch):
            return (dict(schema=codec.CHUNK_SCHEMA,records=[u['metric'] for u in batch]),
                    codec.pack_curves([u['curve'] for u in batch]))
        def emit(batch):
            nonlocal number
            ids=[u['metric']['resultId'] for u in batch]
            for kind,value in zip(('metrics','curves'),payloads(batch)):
                self.member(chunk_name(stage,instrument,tf,number,kind),value,
                    dict(kind=kind,stageId=stage,instrumentId=instrument,timeframeMinutes=tf,resultIds=ids))
            self.written.extend(ids)
            self.input_ids.extend((u['metric']['resultId'],semantic_digest(u['metric']['execution'])) for u in batch)
            number+=1
        for unit in units:
            trial=pending+[unit]
            if pending and (len(trial)>16 or any(len(json_text(v).encode())>codec.MAX_MEMBER for v in payloads(trial))):
                emit(pending);pending=[]
            pending.append(unit)
        if pending:emit(pending)

    def price(self,row):
        self.member(price_name(row['instrumentId'],row['stageId']),dict(schema=codec.CHUNK_SCHEMA,records=[row]),
            dict(kind='price',stageId=row['stageId'],instrumentId=row['instrumentId'],timeframeMinutes=5,resultIds=[]))

    def finish(self,provenance,status,*,before_completion=None):
        self.close()
        require((self.root/'results.zip').stat().st_size<=codec.MAX_ARCHIVE,'final archive byte bound')
        doc=self.request.document
        expected=[result_id(r['configurationId'],s) for s in doc['stages'] for r in self.rows]
        require(self.written==expected,'complete canonical production roster required')
        index=dict(schema=INDEX_SCHEMA,members=self.entries)
        study=dict(schema=SCHEMA,formatVersion=1,request=doc,configurations=self.rows,
            identities=dict(specification=self.request.identity,dataInput=semantic_digest(self.input_ids),
                implementation=semantic_digest(self.implementation)),implementation=self.implementation,
            counts=dict(configurations=len(self.rows),plannedResults=len(expected),completedResults=len(expected),retainedResults=len(expected)),
            visibility='owner',synthetic=self.synthetic,verificationLevel='compact',fullExecutionRetained=False,
            limitation=LIMITATION,lineage=None,artifactIdentity=None)
        study['artifactIdentity']=artifact_identity(study,index)
        save_json(self.root/'study.json',study);save_json(self.root/'index.json',index)
        save_json(self.root/'provenance.json',{**provenance,'synthetic':self.synthetic})
        save_json(self.root/'status.json',{**status,'state':'complete','phase':'complete'})
        _load(self.root,sealed=False)
        if before_completion is not None:before_completion()
        seal(self.root)


class ConfigurationStudy:
    def __init__(self,root,meta,index,archive,locations,prices,input_ids):
        self.root=root;self._meta=meta;self._index=index;self._archive=archive
        self._locations=locations;self._prices=prices;self._input_ids=input_ids

    @property
    def metadata(self):return deepcopy(self._meta)
    @property
    def configurations(self):return deepcopy(self._meta['configurations'])
    @property
    def chunk_index(self):return deepcopy(self._index)
    @property
    def stage_rows(self):
        return [dict(configurationId=c['configurationId'],stageId=s['stageId'],resultId=result_id(c['configurationId'],s),
            status='withheld' if self._meta['visibility']=='public' and s['stageId']=='HO2' else 'complete')
            for s in self._meta['request']['stages'] for c in self._meta['configurations']]

    def raw_member(self,name):
        require(name in self._archive.members,'unknown raw member')
        entry=next(e for e in self._index['members'] if e['name']==name)
        raw=self._archive.read(name)
        require(codec.sha(raw)==entry['sha256'],'raw member hash changed')
        return raw

    def _rows(self,entry):
        value=codec.decode(self.raw_member(entry['name']),entry)
        return codec.unpack_curves(value) if entry['kind']=='curves' else value['records']

    def _slot(self,configuration,stage,kind):
        require(configuration in {r['configurationId'] for r in self._meta['configurations']} and stage in STAGES,'unknown configuration/stage')
        if self._meta['visibility']=='public' and stage=='HO2':return dict(status='withheld',configurationId=configuration,stageId=stage)
        entry,position=self._locations[(configuration,stage,kind)]
        return self._rows(entry)[position]

    def result(self,configurationId,stageId):return self._slot(configurationId,stageId,'metrics')
    def curve(self,configurationId,stageId):return self._slot(configurationId,stageId,'curves')
    def price(self,instrumentId,stageId):
        require(instrumentId in self._meta['request']['instruments'] and stageId in STAGES,'unknown price scope')
        if self._meta['visibility']=='public' and stageId=='HO2':return dict(status='withheld',instrumentId=instrumentId,stageId=stageId)
        return self._rows(self._prices[(instrumentId,stageId)])[0]


def _load(root,*,sealed=True):
    root=Path(root).expanduser().resolve()
    require(root.is_dir(),'compact directory missing')
    names=set(ROOT_FILES)|({'completion.json'} if sealed else set())
    require({p.name for p in root.iterdir()}==names and all(not p.is_symlink() and p.is_file() for p in root.iterdir()),'exact compact root file set')
    for name in names:
        require((root/name).stat().st_size <= (codec.MAX_ARCHIVE if name=='results.zip' else MAX_JSON),'root file resource bound')
    if sealed:
        completion=bounded_json(root/'completion.json');closed(completion,('schema','members'))
        require(completion['schema']=='pattern-lab.configuration-completion.v1' and set(completion['members'])==set(ROOT_FILES),'completion schema/members')
        for name,row in completion['members'].items():
            closed(row,('bytes','sha256'))
            require(type(row['bytes']) is int and (root/name).stat().st_size==row['bytes'] and file_hash(root/name)==row['sha256'],'root integrity')
    study=bounded_json(root/'study.json');index=bounded_json(root/'index.json')
    closed(study,('schema','formatVersion','request','configurations','identities','implementation','counts','visibility','synthetic',
        'verificationLevel','fullExecutionRetained','limitation','lineage','artifactIdentity'))
    require(study['schema']==SCHEMA and type(study['formatVersion']) is int and study['formatVersion']==1,'compact schema/version')
    require(study['visibility'] in ('owner','public') and type(study['synthetic']) is bool and
        study['verificationLevel']=='compact' and study['fullExecutionRetained'] is False and study['limitation']==LIMITATION,'compact verification contract')
    stored=study['request']
    require(type(stored) is dict,'stored request must be an object')
    stages=stored.get('stages')
    require(type(stages) is list and len(stages)==len(STAGES),'stored stages must contain IS/HO1/HO2 objects')
    require(all(type(s) is dict and type(s.get('protocol')) is dict for s in stages),
            'stored stage protocols must be objects')
    normalized=normalize_request(stored);doc=normalized.document;rows=configurations(doc)
    require(doc==study['request'] and rows==study['configurations'],'canonical configuration metadata')
    closed(study['identities'],('specification','dataInput','implementation'))
    require(study['identities']['specification']==normalized.identity and
        study['identities']['implementation']==semantic_digest(study['implementation']),'metadata identities')
    if study['visibility']=='owner':require(study['lineage'] is None,'owner lineage')
    else:
        closed(study['lineage'],('ownerCompletionSha256',))
        validation.digest_id(study['lineage']['ownerCompletionSha256'])
    visible=[s for s in doc['stages'] if study['visibility']=='owner' or s['stageId']!='HO2']
    require(study['counts']==dict(configurations=len(rows),plannedResults=3*len(rows),completedResults=3*len(rows),
                                retainedResults=len(visible)*len(rows)),'complete/projection counts')
    require(bounded_json(root/'status.json')['state']=='complete','incomplete compact run')
    require(bounded_json(root/'provenance.json')['synthetic']==study['synthetic'],'synthetic provenance flag')
    closed(index,('schema','members'));require(index['schema']==INDEX_SCHEMA and type(index['members']) is list,'chunk index schema')
    entries=index['members'];require(len(entries)<=codec.MAX_MEMBERS,'member count bound')
    expected={};total=0
    for entry in entries:
        closed(entry,('name','kind','stageId','instrumentId','timeframeMinutes','resultIds','bytes','sha256','decodedBytes','decodedSha256'))
        require(entry['name'] not in expected,'duplicate indexed member')
        require(type(entry['bytes']) is int and 0<entry['bytes']<=codec.MAX_ARCHIVE and
            type(entry['decodedBytes']) is int and 0<entry['decodedBytes']<=codec.MAX_MEMBER,'indexed member bound')
        total+=entry['decodedBytes'];expected[entry['name']]=entry
    require(total<=codec.MAX_TOTAL,'total decompressed bound')
    archive=codec.Archive(root/'results.zip',expected)
    locations={};prices={};input_ids=[];cursor=0
    def decoded(entry):
        value=codec.decode(archive.read(entry['name']),entry)
        if entry['kind']=='curves':return codec.unpack_curves(value)
        closed(value,('schema','records'));require(value['schema']==codec.CHUNK_SCHEMA and type(value['records']) is list,'chunk schema')
        return value['records']
    for stage in visible:
        for (instrument,tf),group in groups(rows):
            position=0;chunk=0
            while position<len(group):
                require(cursor+2<=len(entries),'missing chunk pair')
                pair=entries[cursor:cursor+2];cursor+=2
                batch=[]
                for kind,entry in zip(('metrics','curves'),pair):
                    require(entry['name']==chunk_name(stage['stageId'],instrument,tf,chunk,kind) and entry['kind']==kind and
                        (entry['stageId'],entry['instrumentId'],entry['timeframeMinutes'])==(stage['stageId'],instrument,tf),
                        'canonical group/chunk sequence')
                    records=decoded(entry);require(1<=len(records)<=16,'chunk record count')
                    subset=group[position:position+len(records)]
                    require(len(subset)==len(records) and entry['resultIds']==[result_id(c['configurationId'],stage) for c in subset],
                        'chunk result roster')
                    if kind=='metrics':batch=records
                    else:require(len(records)==len(batch),'metric/curve pair size')
                    for i,(config,row) in enumerate(zip(subset,records)):
                        if kind=='metrics':
                            validation.metric_row(row,config,stage,study['synthetic'])
                            allocation=next(a for a in doc['allocations'] if a['allocationId']==config['allocationId'])
                            require(row['metrics']['initialCapital']==allocation['policy']['initialCapital'],'declared execution capital')
                            input_ids.append((row['resultId'],semantic_digest(row['execution'])))
                        else:validation.curve_row(row,batch[i],stage)
                        locations[(config['configurationId'],stage['stageId'],kind)]=(entry,i)
                position+=len(batch);chunk+=1
    for instrument in doc['instruments']:
        for stage in sorted(visible,key=lambda s:s['stageId']):
            require(cursor<len(entries),'missing price member');entry=entries[cursor];cursor+=1
            require(entry['name']==price_name(instrument,stage['stageId']) and entry['kind']=='price' and
                entry['stageId']==stage['stageId'] and entry['instrumentId']==instrument and entry['timeframeMinutes']==5 and
                entry['resultIds']==[],'canonical price sequence')
            records=decoded(entry);require(len(records)==1,'price record count');validation.price_row(records[0],instrument,stage)
            prices[(instrument,stage['stageId'])]=entry
    require(cursor==len(entries),'extra chunk/member')
    require(study['identities']['dataInput']==semantic_digest(input_ids) and study['artifactIdentity']==artifact_identity(study,index),
            'compact data/artifact identity')
    return ConfigurationStudy(root,study,index,archive,locations,prices,input_ids)


def load_configuration_study(run_root):
    try:return _load(run_root)
    except PatternLabDataError:raise
    except (OSError,ValueError,KeyError,TypeError,OverflowError,zipfile.BadZipFile,codec.zlib.error) as exc:
        raise PatternLabDataError('Invalid compact artifact ('+type(exc).__name__+').') from None


def verify_configuration_study(run_root):
    reader=load_configuration_study(run_root)
    return dict(status='verified',verificationLevel='compact',fullExecutionRetained=False,limitation=LIMITATION,
        synthetic=reader._meta['synthetic'],visibility=reader._meta['visibility'],counts=reader._meta['counts'],
        artifactIdentity=reader._meta['artifactIdentity'])


def export_public_results(run_root,*,output_root):
    owner=load_configuration_study(run_root)
    require(owner._meta['visibility']=='owner','public export requires owner artifact')
    root=fresh_directory(output_root,owner.root)
    try:
        entries=[e for e in owner._index['members'] if e['stageId']!='HO2']
        with zipfile.ZipFile(root/'results.zip','x',compression=zipfile.ZIP_STORED,allowZip64=False) as z:
            for entry in entries:z.writestr(zip_info(entry['name']),owner.raw_member(entry['name']))
        index=dict(schema=INDEX_SCHEMA,members=entries);study=owner.metadata
        retained={r for e in entries if e['kind']=='metrics' for r in e['resultIds']}
        study['visibility']='public';study['counts']['retainedResults']=len(retained)
        study['lineage']=dict(ownerCompletionSha256=file_hash(owner.root/'completion.json'))
        study['identities']['dataInput']=semantic_digest([p for p in owner._input_ids if p[0] in retained])
        study['artifactIdentity']=artifact_identity(study,index)
        save_json(root/'study.json',study);save_json(root/'index.json',index)
        save_json(root/'provenance.json',dict(synthetic=study['synthetic'],lineage=study['lineage']))
        save_json(root/'status.json',dict(state='complete',phase='complete',completedResults=len(retained)))
        _load(root,sealed=False);seal(root)
    except BaseException:
        save_json(root/'status.json',dict(state='failed',phase='publicProjection',reasonCode='projectionFailed'))
        raise PatternLabDataError('Configuration public projection failed.') from None
    return load_configuration_study(root)
