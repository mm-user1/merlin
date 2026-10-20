"""Explicit v2 projection through unchanged v1 economic normalization."""
from ..accounts.contracts import require, number, identifier, integer
from ..account_study.common import closed,json_text,parse_json
from ..auxiliary.request import normalize_bindings
from ..auxiliary.features import validate_feature
from ..auxiliary.model import TYPES,INTERVALS
from .request import normalize_selection_request,SelectionRequest,strict_json
from .scorers import contract

SCHEMA='pattern-lab.selection-request.v2'


def normalize_v2(value, *, scorers=None, stored=False):
    strict_json(value);doc=parse_json(json_text(value))
    require('initialCapital' in doc and 'auxiliaryInputs' in doc,'v2 initialCapital/auxiliaryInputs required')
    mode=doc['capitalMode'];initial=doc.pop('initialCapital');bindings=doc.pop('auxiliaryInputs')
    require(mode in ('fixedReference','carried'),'capital mode')
    if mode=='carried':initial=number(initial,'initialCapital')
    else:require(initial is None,'fixedReference initialCapital must be null')
    doc.update(schema='pattern-lab.selection-request.v1',capitalMode='fixedReference')
    doc=normalize_selection_request(doc,scorers=scorers,stored=stored).document
    require(type(bindings) is list,'auxiliaryInputs list')
    targets=set()
    for b in bindings:
        require(type(b) is dict and type(b.get('sources')) is dict,'auxiliary binding sources')
        targets.update(k for k in b['sources'] if k!='*')
    adapter=dict(auxiliaryInputs=bindings,instruments=sorted(targets),sources=[])
    normalize_bindings(adapter)
    doc.update(schema=SCHEMA,capitalMode=mode,initialCapital=initial,auxiliaryInputs=adapter['auxiliaryInputs'])
    return SelectionRequest(json_text(doc))


def normalize_uses(uses,doc):
    strict_json(uses);require(type(uses) is list,'auxiliary uses list')
    uses=parse_json(json_text(uses));bindings={b['inputId']:b for b in doc['auxiliaryInputs']};ids=set()
    for u in uses:
        closed(u,('useId','bindingId','access','publicationLagMs','antecedentMs'),('feature',))
        identifier(u['useId']);require(u['useId'] not in ids,'duplicate useId');ids.add(u['useId'])
        require(u['bindingId'] in bindings and u['access'] in ('raw','feature'),'auxiliary use binding/access')
        integer(u['publicationLagMs'],'publicationLagMs');integer(u['antecedentMs'],'antecedentMs')
        if u['access']=='raw':require('feature' not in u,'raw use cannot declare feature')
        else:
            f=u.get('feature');validate_feature(f);identifier(f['featureId']);identifier(f['bindingId'])
            require(f['bindingId']==u['bindingId'] and f['publicationLagMs']==u['publicationLagMs'],'feature use binding/lag')
            for s in bindings[u['bindingId']]['sources'].values():
                require(f['field']==s['field'] and f['transform'] in TYPES[s['typeId']].transformations,'feature source field/transform')
                if f['transform']=='auxiliaryChange':
                    require(TYPES[s['typeId']].kind=='sampled' and f['lookbackMinutes']*60000%INTERVALS[s['interval']]==0,'feature exact lookback grid')
    return sorted(uses,key=lambda u:u['useId'])


def scorer_contract(s,parameters,doc):
    value=contract(s,parameters)
    require(s.auxiliaryUses is None or callable(s.auxiliaryUses),'auxiliaryUses callable')
    value['auxiliaryUses']=normalize_uses(s.auxiliaryUses(parse_json(json_text(parameters))) if s.auxiliaryUses else [],doc)
    return value
