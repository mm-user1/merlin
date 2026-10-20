"""Selection-owned causal adapters over the accepted T03 snapshot/feature APIs."""
import numpy as np
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


def cell(snapshot,binding,ticker,use,history,cutoff,parent_bounds):
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
    available=complete and (state is None or state['known'])
    require(binding['coveragePolicy']!='requireComplete' or available,
        'required auxiliary coverage/state unavailable: '+ticker+'/'+use['useId'])
    diagnostic=dict(instrumentId=ticker,useId=use['useId'],bindingId=use['bindingId'],source=source,
        startMs=start,endMs=end,cutoffMs=cutoff,publicationLagMs=use['publicationLagMs'],
        coveragePolicy=binding['coveragePolicy'],coverage=value['coverage'],complete=complete,
        available=available,reason=None if available else 'incompleteCoverage' if not complete else state['reason'],
        consumedSliceFingerprint=fingerprint(value),featureState=state)
    exposed=dict(series=value['series'],rows=value['rows'],coverage=value['coverage'],complete=complete,
        available=available,reason=diagnostic['reason']) if use['access']=='raw' else state
    return diagnostic,exposed


class AuxiliaryView:
    """Only owned past slices/states; no snapshot roots, reader or future buffers."""
    def __init__(self,price,values):
        self._price=price;self._valuesJson=json_text(values)
        self.instruments=price.instruments;self.monthLabels=price.monthLabels;self.cutoffMs=price.cutoffMs
    def months(self,ticker):return self._price.months(ticker)
    def bars(self,ticker):return self._price.bars(ticker)
    def _get(self,ticker,useId,access):
        require(ticker in self.instruments,'auxiliary ticker outside view')
        values=parse_json(self._valuesJson);key=ticker+'/'+useId
        require(key in values and values[key]['access']==access,'undeclared auxiliary access')
        return freeze(values[key]['value'])
    def auxiliaryRaw(self,ticker,useId):return self._get(ticker,useId,'raw')
    def auxiliaryFeature(self,ticker,useId):return self._get(ticker,useId,'feature')
    @property
    def fingerprint(self):
        return semantic_digest(dict(price=self._price.fingerprint,auxiliary=parse_json(self._valuesJson),
            instruments=self.instruments,months=self.monthLabels,cutoff=self.cutoffMs))


def prepare_cells(doc,contracts,ledger,roster,snapshots,parent_bounds):
    from .ledger import history_pool
    bindings={b['inputId']:b for b in doc['auxiliaryInputs']};months=[w['month'] for w in doc['monthWindows']]
    used={u['bindingId'] for c in contracts.values() for u in c['auxiliaryUses']}
    require(used==set(bindings),'unused auxiliary bindings')
    prepared={};cache={}
    for selector in doc['selectors']:
        sid=selector['selectorId'];c=contracts[sid]
        for month in doc['evaluationMonths']:
            index=months.index(month);cutoff=month_bounds(month)[0];history=months[index-c['lookbackMonths']:index]
            pool,_=history_pool(ledger,roster,months[index-doc['pool']['historyMonths']:index],doc['pool']['minObservedBarsPct'])
            diagnostics=[];values={}
            for ticker in pool:
                for use in c['auxiliaryUses']:
                    b=bindings[use['bindingId']];key=json_text([b,source_for(b,ticker),use,history,cutoff])
                    # Each retained cell is target-labelled, even when slices are shared.
                    if key not in cache:cache[key]=cell(snapshots[b['snapshotId']],b,ticker,use,history,cutoff,parent_bounds)
                    diag,value=cache[key];diag=dict(diag,instrumentId=ticker);diagnostics.append(diag)
                    values[ticker+'/'+use['useId']]=dict(access=use['access'],value=value)
            prepared[sid,month]=(diagnostics,values)
    return prepared
