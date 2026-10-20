"""Selection-owned causal adapters over the accepted T03 snapshot/feature APIs."""
import numpy as np
from types import MappingProxyType
from ..accounts.contracts import require
from ..account_study.common import json_text,parse_json,read_json,closed,digest_id
from ..account_study.request import bounds
from ..auxiliary.request import normalize_bindings
from ..auxiliary.study import source_for,check_binding
from ..auxiliary.storage import load_snapshot,safe_path
from ..auxiliary.features import build_feature
from ..auxiliary.model import fingerprint,validate_series,INTERVALS,inspect_content
from ..study.contracts import semantic_digest
from .ledger import freeze
from .request import month_bounds


def admit_snapshots(doc,roots,output,parent):
    roots={} if roots is None else roots
    require(type(roots) is dict and set(roots)=={b['snapshotId'] for b in doc['auxiliaryInputs']},'exact auxiliary-root mapping required')
    adapter=dict(auxiliaryInputs=parse_json(json_text(doc['auxiliaryInputs'])),instruments=parent.study['request']['instruments'],sources=parent.study['request']['sources'])
    normalize_bindings(adapter)
    snapshots={}
    for alias,path in sorted(roots.items()):
        p=safe_path(path)
        require(not output.is_relative_to(p) and not p.is_relative_to(output),'auxiliary output overlap')
        snapshots[alias]=load_snapshot(p)
    for b in doc['auxiliaryInputs']:
        reader=snapshots[b['snapshotId']]
        require(reader.contentId==b['expectedSnapshotContentId'],'snapshot semantic content pin differs')
        for ticker in parent.study['request']['instruments']:
            s=source_for(b,ticker)
            require(s['seriesId'] in {v['seriesId'] for v in reader.manifest['series']},'auxiliary series absent')
            check_binding(b,ticker,read_json(reader.root/'series'/s['seriesId']/'series.json'))
    return snapshots


def envelope(use,source,history,cutoff,parent_bounds):
    lag=use['publicationLagMs'];end=cutoff-lag+1
    if use['access']=='raw':start=month_bounds(history[0])[0]-use['antecedentMs']
    else:
        f=use['feature'];step=INTERVALS[source['interval']] if source['interval'] is not None else 1
        start=cutoff-(f['maxAgeMs']+(f['lookbackMinutes'] or 0)*60000+step+lag+use['antecedentMs'])
    require(parent_bounds[0]<=start<end<=parent_bounds[2],'auxiliary envelope outside parent preparation/study bounds')
    return start,end


ALGORITHMS=('monthly-selection.2','monthly-selection.3')


def availability(complete,state,policy,algorithm):
    require(algorithm in ALGORITHMS,'unknown selection algorithm')
    feature_only=algorithm=='monthly-selection.3' and policy=='allowUnknown' and state is not None
    available=state['known'] if feature_only else complete and (state is None or state['known'])
    reason=None if available else state['reason'] if feature_only else 'incompleteCoverage' if not complete else state['reason']
    return available,reason


def cell(snapshot,binding,ticker,use,history,cutoff,parent_bounds,*,algorithm='monthly-selection.3'):
    source=source_for(binding,ticker);start,end=envelope(use,source,history,cutoff,parent_bounds)
    value=snapshot.slice(source['seriesId'],start,end);check_binding(binding,ticker,value)
    descriptor=validate_series(value['series']);inspection=inspect_content(value)
    complete=value['coverage']==[[start,end]] and all(r[source['field']] is not None for r in value['rows'])
    if descriptor.kind=='sampled':complete=complete and inspection['missingSampleSlots']==0
    state=None
    if use['access']=='feature':
        aligned,meta,identity=build_feature(value,use['feature'],np.array([cutoff],np.int64),start=start,end=end)
        known=bool(aligned.known[0]);ix=int(aligned.sourceIndex[0])
        state=dict(value=float(aligned.value[0]) if known else None,known=known,
            reason=None if known else 'expired' if int(aligned.unknownReason[0])==3 else (meta['reasons'][ix] if 0<=ix<len(meta['reasons']) else 'noAvailableObservation'),
            sourceIndex=ix,observedAtMs=int(aligned.observedAtMs[0]),availableAtMs=int(aligned.availableAtMs[0]),
            validUntilMs=int(aligned.validUntilMs[0]),identity=identity)
    available,reason=availability(complete,state,binding['coveragePolicy'],algorithm)
    require(binding['coveragePolicy']!='requireComplete' or available,
        'required auxiliary coverage/state unavailable: '+ticker+'/'+use['useId'])
    diagnostic=dict(instrumentId=ticker,useId=use['useId'],bindingId=use['bindingId'],source=source,
        startMs=start,endMs=end,cutoffMs=cutoff,publicationLagMs=use['publicationLagMs'],
        coveragePolicy=binding['coveragePolicy'],coverage=value['coverage'],complete=complete,
        available=available,reason=reason,
        consumedSliceFingerprint=fingerprint(value),featureState=state)
    exposed=dict(series=value['series'],rows=value['rows'],coverage=value['coverage'],complete=complete,
        available=available,reason=diagnostic['reason']) if use['access']=='raw' else state
    return diagnostic,exposed


class AuxiliaryView:
    """Only owned past slices/states; no snapshot roots, reader or future buffers."""
    def __init__(self,price,values):
        self._price=price
        self._cells=MappingProxyType({k:json_text(v) for k,v in values.items()})
        self.instruments=price.instruments;self.monthLabels=price.monthLabels;self.cutoffMs=price.cutoffMs
    def months(self,ticker):return self._price.months(ticker)
    def bars(self,ticker):return self._price.bars(ticker)
    def _get(self,ticker,useId,access):
        require(ticker in self.instruments,'auxiliary ticker outside view')
        key=ticker+'/'+useId
        require(key in self._cells,'undeclared auxiliary access')
        value=parse_json(self._cells[key])
        require(value['access']==access,'undeclared auxiliary access')
        return freeze(value['value'])
    def auxiliaryRaw(self,ticker,useId):return self._get(ticker,useId,'raw')
    def auxiliaryFeature(self,ticker,useId):return self._get(ticker,useId,'feature')
    @property
    def fingerprint(self):
        # Hash immutable owned text directly, without parsing/canonicalizing observations.
        import hashlib
        return semantic_digest(dict(price=self._price.fingerprint,auxiliary={k:hashlib.sha256(v.encode('utf-8')).hexdigest() for k,v in self._cells.items()},
            instruments=self.instruments,months=self.monthLabels,cutoff=self.cutoffMs))


def prepare_batch(doc,contract,pool,history,cutoff,snapshots,parent_bounds,*,algorithm):
    """Own only one selector-month; shared-source reuse ends with this call."""
    bindings={b['inputId']:b for b in doc['auxiliaryInputs']};cache={};diagnostics=[];values={}
    for ticker in pool:
        for use in contract['auxiliaryUses']:
            b=bindings[use['bindingId']];key=json_text([b,source_for(b,ticker),use,history,cutoff])
            if key not in cache:cache[key]=cell(snapshots[b['snapshotId']],b,ticker,use,history,cutoff,parent_bounds,algorithm=algorithm)
            diag,value=cache[key];diagnostics.append(dict(diag,instrumentId=ticker))
            values[ticker+'/'+use['useId']]=dict(access=use['access'],value=value)
    return diagnostics,values
