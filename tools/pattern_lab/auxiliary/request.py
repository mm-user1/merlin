"""Closed v2 auxiliary bindings, explicit scalar rules and warmup admission."""
from dataclasses import asdict
import numpy as np

from ..accounts.contracts import require, identifier, integer, number
from ..account_study.common import closed, digest_id
from .features import validate_feature, condition
from .model import TYPES, INTERVALS

V2 = 'pattern-lab.account-study-request.v2'


def normalize_bindings(doc):
    require(type(doc['auxiliaryInputs']) is list, 'auxiliaryInputs list')
    ids = set()
    for b in doc['auxiliaryInputs']:
        closed(b, ('inputId', 'snapshotId', 'expectedSnapshotContentId', 'coveragePolicy', 'sources'))
        identifier(b['inputId']); identifier(b['snapshotId']); digest_id(b['expectedSnapshotContentId'])
        require(b['inputId'] not in ids, 'duplicate auxiliary input'); ids.add(b['inputId'])
        require(b['coveragePolicy'] in ('requireComplete','allowUnknown'), 'auxiliary coverage policy')
        require(type(b['sources']) is dict and (set(b['sources']) == {'*'} or set(b['sources']) == set(doc['instruments'])), 'complete explicit auxiliary target mapping')
        for s in b['sources'].values():
            closed(s, ('seriesId','provider','venue','contract','typeId','field','interval','unit','baseUnit','baseMultiplier'))
            identifier(s['seriesId']); identifier(s['provider']); identifier(s['venue'])
            require(s['typeId'] in TYPES and s['field'] in TYPES[s['typeId']].fields, 'auxiliary binding type/field')
            if s['typeId']=='openInterest': require(s['field']=='singleOpenInterest', 'explicit singleOpenInterest required')
            require(s['interval'] in INTERVALS if TYPES[s['typeId']].kind=='sampled' else s['interval'] is None, 'binding interval')
            require(all(isinstance(s[k],str) and s[k] for k in ('contract','unit','baseUnit')), 'binding source/unit')
            number(s['baseMultiplier'],'baseMultiplier')
    doc['auxiliaryInputs'].sort(key=lambda b:b['inputId'])
    require(not ids & ({s['inputId'] for s in doc['sources']} | {'target'}), 'native/auxiliary alias collision')


def components(raw):
    if raw is None:return []
    if set(raw)=={'allOf'}:
        require(type(raw['allOf']) is list and raw['allOf'], 'nonempty allOf required')
        return raw['allOf']
    return [raw]


def scalar_rule(p, features, *, entry=False):
    required=('providerId','version','featureId','long','short')
    closed(p, required, ('stop',) if entry else ('componentId',))
    require(p['providerId']=='scalarCondition' and p['version']=='1' and p['featureId'] in features, 'scalar provider/version/feature')
    for side in ('long','short'):condition(np.array([],dtype=float),p[side])
    require(p['long'] is not None or p['short'] is not None, 'scalar provider supplies no direction')
    if entry:
        closed(p.get('stop'), ('providerId','atrRequestId','swingBars','atrMultiplier'))
        require(p['stop']['providerId']=='realSwingAtr', 'scalar stop provider')
        integer(p['stop']['swingBars'],'swingBars',1);number(p['stop']['atrMultiplier'],'atrMultiplier',inclusive=True)


def normalize_allocation(doc, a, native_features):
    require(type(a['auxiliaryFeatures']) is list, 'auxiliaryFeatures list')
    bindings={b['inputId']:b for b in doc['auxiliaryInputs']}
    features={}
    for f in a['auxiliaryFeatures']:
        validate_feature(f);identifier(f['featureId']);identifier(f['bindingId'])
        require(f['featureId'] not in features and f['bindingId'] in bindings, 'auxiliary feature duplicate/binding')
        b=bindings[f['bindingId']]
        for s in b['sources'].values():
            require(f['field']==s['field'] and f['transform'] in TYPES[s['typeId']].transformations, 'feature binding field/transform')
            if f['transform']=='auxiliaryChange':require(f['lookbackMinutes']*60000 % INTERVALS[s['interval']]==0, 'lookback grid')
        features[f['featureId']]=f
    used=set();component_ids=set()
    for c in components(a['entryFilter']):
        require(type(c) is dict, 'filter component object');identifier(c.get('componentId'))
        require(c['componentId'] not in component_ids,'duplicate componentId');component_ids.add(c['componentId'])
        if c.get('providerId')=='candleDirection':
            closed(c, ('componentId','providerId','version','requestId'))
            require(c['version']=='1' and c['requestId'] in native_features and native_features[c['requestId']].featureId=='candleDirection', 'candle filter dependency')
        else:
            scalar_rule(c,features);used.add(c['featureId'])
    if a['entryFilter'] is not None and 'allOf' in a['entryFilter']:
        a['entryFilter']['allOf'].sort(key=lambda c:c['componentId'])
    p=a['provider']
    if p['providerId']=='scalarCondition':
        scalar_rule(p,features,entry=True);used.add(p['featureId'])
        require(a['policy']['mode']=='fixedBracket' and not a['policy']['stopUpdates'], 'scalar entries support fixedBracket only')
        atr=native_features.get(p['stop']['atrRequestId'])
        require(atr is not None and atr.featureId=='atr' and atr.inputId=='target', 'scalar ATR dependency')
    require(used==set(features), 'unused auxiliary feature')
    a['auxiliaryFeatures']=[features[k] for k in sorted(features)]
    return {f['bindingId'] for f in features.values()}


def auxiliary_warmup(doc,a,origin,start):
    bindings={b['inputId']:b for b in doc['auxiliaryInputs']}
    result={}
    for f in a['auxiliaryFeatures']:
        b=bindings[f['bindingId']]
        step=max(INTERVALS.get(s['interval'],1) for s in b['sources'].values())
        required=(f['lookbackMinutes'] or 0)*60000+step+f['publicationLagMs']
        if a['provider']['providerId']=='scalarCondition' and a['provider']['featureId']==f['featureId']:
            required+=a['timeframeMinutes']*60000
        require(start-origin>=required,'insufficient declared auxiliary warmup '+f['featureId'])
        result['auxiliary:'+f['featureId']]=dict(requiredPriorMs=required,availablePriorMs=start-origin,
            irregularAntecedentNotGuaranteed=b['sources'][next(iter(b['sources']))]['typeId']=='settledFunding')
    return result
