"""Closed staged requests, native normalization and outcome-independent identities."""
from dataclasses import dataclass
from pathlib import Path

from ..accounts.contracts import require, identifier
from ..account_study.common import closed, json_text, parse_json, read_json
from ..account_study.request import normalize_request as native_request
from ..study.contracts import semantic_digest
from data_provider.manifest import to_epoch_ms

SCHEMA = 'pattern-lab.configuration-study-request.v1'
STAGES = ('IS', 'HO1', 'HO2')
MAX_JSON = 16*1024*1024
MAX_MEMBERS = 65534


def owned(value):
    base = Path.cwd()
    if isinstance(value, (str, Path)):
        path = Path(value).expanduser().resolve()
        require(path.stat().st_size <= MAX_JSON, 'request JSON bound')
        value, base = read_json(path), path.parent
    raw = json_text(value)
    require(len(raw.encode()) <= MAX_JSON, 'request JSON bound')
    return parse_json(raw), base


@dataclass(frozen=True)
class ConfigurationStudyRequest:
    documentJson: str

    @property
    def document(self):
        return parse_json(self.documentJson)

    @property
    def identity(self):
        doc = self.document
        doc.pop('notes', None)
        for stage in doc['stages']: stage['protocol'].pop('notes', None)
        return semantic_digest(doc)


def child_request(doc, stage):
    result = {k:doc[k] for k in ('studyId','instruments','sources','allocations')}
    result.update(schema='pattern-lab.account-study-request.v2' if 'auxiliaryInputs' in doc else
                  'pattern-lab.account-study-request.v1', protocol=stage['protocol'],
                  preparationStartUtc=stage['preparationStartUtc'], windows=[dict(windowId=stage['stageId'],
                  startUtc=stage['startUtc'], endUtc=stage['endUtc'])])
    if 'auxiliaryInputs' in doc: result['auxiliaryInputs'] = doc['auxiliaryInputs']
    return result


def numeric_key(value):
    """Exact recursive equality: int/float equal, bool distinct, no lossy cast."""
    if type(value) in (int, float): return ('number',value)
    if type(value) is dict: return ('object',tuple((k,numeric_key(v)) for k,v in sorted(value.items())))
    if type(value) is list: return ('array',tuple(map(numeric_key,value)))
    return (type(value).__name__,value)


def configuration_payload(doc, instrument, allocation):
    used = {f['inputId'] for f in allocation['features']}
    aux_used = {f['bindingId'] for f in allocation.get('auxiliaryFeatures',[])}
    return dict(schema='configuration-identity.v1', instrumentId=instrument,
        allocation={k:v for k,v in allocation.items() if k!='allocationId'},
        sources=[s for s in doc['sources'] if s['inputId'] in used],
        auxiliaryInputs=[dict(inputId=b['inputId'],coveragePolicy=b['coveragePolicy'],
            source=b['sources'].get('*',b['sources'].get(instrument))) for b in doc.get('auxiliaryInputs',[])
            if b['inputId'] in aux_used])


def configurations(doc):
    count = len(doc['instruments'])*len(doc['allocations'])
    require(count*3*2 <= MAX_MEMBERS*16, 'planned roster exceeds member capacity')
    rows=[]; size=0
    for instrument in doc['instruments']:
        seen=set()
        for allocation in doc['allocations']:
            payload=configuration_payload(doc,instrument,allocation)
            key=numeric_key(payload)
            require(key not in seen, 'numerically equivalent configuration declarations')
            seen.add(key)
            row=dict(configurationId=semantic_digest(payload),instrumentId=instrument,
                     allocationId=allocation['allocationId'],timeframeMinutes=allocation['timeframeMinutes'])
            size+=len(json_text(row).encode())+1
            require(size <= MAX_JSON, 'planned configuration metadata bound')
            rows.append(row)
    return sorted(rows,key=lambda r:(r['instrumentId'],r['timeframeMinutes'],r['configurationId']))


def result_id(configuration, stage):
    return semantic_digest(dict(schema='configuration-result.v1',configurationId=configuration,
        stageId=stage['stageId'],startMs=to_epoch_ms(stage['startUtc'],'startUtc'),
        endMs=to_epoch_ms(stage['endUtc'],'endUtc')))


def normalize_request(value):
    if isinstance(value,ConfigurationStudyRequest): value=value.document
    doc,base=owned(value)
    closed(doc,('schema','studyId','instruments','sources','allocations','stages'),('notes','auxiliaryInputs'))
    require(doc['schema']==SCHEMA,'configuration request schema')
    identifier(doc['studyId'])
    require(doc.get('notes') is None or type(doc['notes']) is str,'notes')
    require(type(doc['stages']) is list and len(doc['stages'])==3,'exactly IS/HO1/HO2 stages required')
    protocols=set(); previous=None
    for i,stage in enumerate(doc['stages']):
        closed(stage,('stageId','startUtc','endUtc','preparationStartUtc','protocol'))
        require(stage['stageId']==STAGES[i],'stage order must be IS/HO1/HO2')
        if isinstance(stage['protocol'],str):stage['protocol']=read_json(base/stage['protocol'])
        native=native_request(child_request(doc,stage)).document
        stage.update(protocol=native['protocol'],preparationStartUtc=native['preparationStartUtc'],
                     startUtc=native['windows'][0]['startUtc'],endUtc=native['windows'][0]['endUtc'])
        for name in ('instruments','allocations','sources','auxiliaryInputs'):
            if name in native:doc[name]=native[name]
        start,end=(to_epoch_ms(stage[k],k) for k in ('startUtc','endUtc'))
        require(previous is None or previous<=start,'overlapping/out-of-order stages')
        previous=end
        pid=stage['protocol']['protocol_id']
        require(pid not in protocols,'distinct stage protocol IDs required');protocols.add(pid)
    for i,stage in enumerate(doc['stages']):
        reserve=stage['protocol']['reserved']
        left,right=to_epoch_ms(reserve['start_utc'],'reserve'),to_epoch_ms(reserve['end_utc'],'reserve')
        require(all(left<=to_epoch_ms(s['startUtc'],'stage') and to_epoch_ms(s['endUtc'],'stage')<=right
                    for s in doc['stages'][i+1:]),'stage reserve must contain every later stage')
    require(len({a['policy']['initialCapital'] for a in doc['allocations']})==1,'shared positive initialCapital required')
    rows=configurations(doc)
    require(len(json_text(dict(request=doc,configurations=rows)).encode())<=MAX_JSON,'planned root JSON bound')
    return ConfigurationStudyRequest(json_text(doc))


def normalize_annotations(value, study):
    doc,_=owned(value)
    closed(doc,('schema','specificationIdentity','annotations'))
    require(doc['schema']=='pattern-lab.configuration-annotations.v1','annotation schema')
    require(doc['specificationIdentity']==study.metadata['identities']['specification'],'annotation specification identity')
    known={(r['instrumentId'],r['allocationId']) for r in study.configurations}
    require(type(doc['annotations']) is list,'annotation list')
    seen=set()
    for a in doc['annotations']:
        closed(a,('instrumentId','allocationId','role','methodId','methodDescription','selectedFromStage'))
        require((a['instrumentId'],a['allocationId']) in known,'unknown annotation configuration')
        require(a['role'] in ('return','stability','reference') and a['selectedFromStage']=='IS','annotation role/stage')
        require(all(type(a[k]) is str and a[k].strip() for k in ('methodId','methodDescription')),'annotation method metadata')
        key=tuple(a[k] for k in ('instrumentId','allocationId','role'))
        require(key not in seen,'duplicate annotation');seen.add(key)
    doc['annotations'].sort(key=lambda a:tuple(a[k] for k in ('instrumentId','allocationId','role')))
    return doc
