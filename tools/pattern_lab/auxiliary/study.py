"""V2 shared dependencies and explicit offline provider-command verification."""
from dataclasses import fields
from pathlib import Path
import json
import numpy as np

from .. import data
from ..accounts import PreparedInput, AccountFeatureRequest, prepare_inputs
from ..accounts.contracts import require, command_array_digest, EntryFilter
from ..account_study.common import read_json, write_json, closed, digest_id
from ..account_study.codec import save_input, load_input, save_arrays, load_arrays, preparation_facts
from .storage import load_snapshot, safe_path
from .model import content, fingerprint, inspect_content, ConsumedSlice
from .commands import build_auxiliary_commands

SCHEMA = 'pattern-lab.account-study.v2'


def source_for(binding,target):
    return binding['sources'].get('*',binding['sources'].get(target))


def check_binding(binding,target,value):
    expected=source_for(binding,target);s=value['series']
    actual={k:s[k] for k in ('seriesId','provider','venue','contract','interval','baseUnit','baseMultiplier')}
    actual.update(typeId=s['type']['typeId'],field=expected['field'],unit=s['units'].get(expected['field']))
    require(actual==expected,'auxiliary source/type/unit mapping differs')


def admit(doc,roots,output,pack):
    roots={} if roots is None else roots
    require(type(roots) is dict and set(roots)=={b['snapshotId'] for b in doc['auxiliaryInputs']},'exact auxiliary-root mapping required')
    snapshots={}
    for alias,path in sorted(roots.items()):
        p=safe_path(path)
        require(not output.is_relative_to(p) and not p.is_relative_to(output),'auxiliary output overlap')
        require(not p.is_relative_to(pack) and not pack.is_relative_to(p),'auxiliary/native roots overlap')
        snapshots[alias]=load_snapshot(p)
    for b in doc['auxiliaryInputs']:
        reader=snapshots[b['snapshotId']]
        require(reader.contentId==b['expectedSnapshotContentId'],'snapshot semantic content pin differs')
        for target in doc['instruments']:
            s=source_for(b,target)
            require(s['seriesId'] in {e['seriesId'] for e in reader.manifest['series']},'required auxiliary series absent')
            meta=read_json(reader.root/'series'/s['seriesId']/'series.json')
            check_binding(b,target,meta)
    return snapshots


def dependencies_for(doc,target,snapshots,start,end,shared):
    result={};cache={};bindings={b['inputId']:b for b in doc['auxiliaryInputs']}
    for a in doc['allocations']:
        deps={}
        for bid in sorted({f['bindingId'] for f in a['auxiliaryFeatures']}):
            b=bindings[bid];s=source_for(b,target);key=(b['snapshotId'],s['seriesId'])
            selected_cache=shared if '*' in b['sources'] else cache
            if key not in selected_cache:
                selected_cache[key]=ConsumedSlice.from_content(snapshots[key[0]].slice(key[1],start,end))
            deps[bid]=dict(content=selected_cache[key],coveragePolicy=b['coveragePolicy'])
        result[a['allocationId']]=deps
    return result


def input_fact(doc,label,bid,value):
    b=next(b for b in doc['auxiliaryInputs'] if b['inputId']==bid)
    a=next(a for a in doc['allocations'] if a['allocationId']==label['allocationId'])
    check_binding(b,label['instrumentId'],value)
    source=source_for(b,label['instrumentId'])
    return dict(instrumentId=label['instrumentId'],allocationId=label['allocationId'],bindingId=bid,
        series=value['series'],field=source['field'],unit=source['unit'],coveragePolicy=b['coveragePolicy'],
        features=[f for f in a['auxiliaryFeatures'] if f['bindingId']==bid],
        consumedSliceFingerprint=fingerprint(value),parentSnapshotContentId=b['expectedSnapshotContentId'])


def save_evidence(root,record):
    root=Path(root);e=record.auxiliaryEvidence
    grids=[save_input(root,v) for v in record.prepared.inputs]
    dependencies={}
    for bid,dep in sorted(e['dependencies'].items()):
        value=dep['content'];identity=fingerprint(value)
        path=root/'dependencies'/identity/'dependency.json'
        if not path.exists():write_json(path,value)
        dependencies[bid]=identity
    decisions=e['decisions'];folder=root/'components'/decisions['identity']
    if not folder.exists():
        save_arrays(folder/'arrays.npz',decisions['arrays']);write_json(folder/'components.json',decisions['metadata'])
    return dict(nativeGrids=grids,dependencies=dependencies,componentEvidenceId=decisions['identity'])


def load_links(root,keys):
    links=read_json(root/'dependency-links.json')
    closed(links,('schema','accounts'))
    require(links['schema']=='pattern-lab.account-dependency-links.v1' and set(links['accounts'])==set(keys),'dependency links roster/schema')
    return links['accounts']


def read_evidence(root,link):
    closed(link,('nativeGrids','dependencies','componentEvidenceId'))
    require(type(link['nativeGrids']) is list and len(set(link['nativeGrids']))==len(link['nativeGrids']),'native dependency grids')
    require(type(link['dependencies']) is dict,'auxiliary dependency mapping')
    inputs=[load_input(root,digest_id(k)) for k in link['nativeGrids']]
    dependencies={}
    for bid,identity in link['dependencies'].items():
        digest_id(identity);v=read_json(root/'dependencies'/identity/'dependency.json')
        closed(v,('series','rows','coverage'))
        require(content(**v)==v and fingerprint(v)==identity,'consumed dependency identity')
        dependencies[bid]=v
    identity=digest_id(link['componentEvidenceId']);folder=root/'components'/identity
    meta=read_json(folder/'components.json');arrays=load_arrays(folder/'arrays.npz')
    closed(meta,('schema','features','components','provider','projection'))
    require(meta['schema']=='pattern-lab.auxiliary-decisions.v1' and meta['projection']=='derived-target-decision.1', 'component evidence schema')
    require(command_array_digest(arrays,meta)==identity,'component evidence identity')
    return inputs,dependencies,meta,arrays


def check_links(root,doc,record,link):
    inputs,deps,meta,arrays=read_evidence(root,link)
    facts=[dict(inputId=v.inputId,header=v.header,inputFingerprint=v.inputFingerprint) for v in inputs]
    require(facts==record.preparationFacts['inputs'],'retained native preparation links')
    a=next(a for a in doc['allocations'] if a['allocationId']==record.labels['allocationId'])
    require(set(deps)=={f['bindingId'] for f in a['auxiliaryFeatures']},'auxiliary dependency subset')
    require(meta['provider']==a['provider'],'component provider declaration')
    from .request import components
    expected=sorted(components(a['entryFilter']),key=lambda c:c['componentId'])
    require([c['metadata']['component'] for c in meta['components']]==expected,'planned filter components')
    require(set(meta['features'])=={f['featureId'] for f in a['auxiliaryFeatures']},'planned auxiliary features')
    for f in a['auxiliaryFeatures']:
        require(meta['features'][f['featureId']]['metadata']['feature']==f,'feature declaration differs')
        require(meta['features'][f['featureId']]['metadata']['consumedSliceFingerprint']==fingerprint(deps[f['bindingId']]),'feature dependency link')
    provenance=json.loads(record.result.commands.provenance)
    require(provenance['auxiliaryPreparation']['componentEvidenceId']==link['componentEvidenceId'],'command component link')
    filt=record.result.commands.entryFilter
    if expected:
        known=np.logical_and.reduce([arrays['component.'+c['componentId']+'.known'] for c in expected])
        long=known & np.logical_and.reduce([arrays['component.'+c['componentId']+'.allowLong'] for c in expected])
        short=known & np.logical_and.reduce([arrays['component.'+c['componentId']+'.allowShort'] for c in expected])
        d=record.result.commands.decisionMs
        projection=EntryFilter(record.prepared.identity,d,known,long,short,np.arange(len(d),dtype=np.int64),d,d,
            d+record.prepared.stepMs,np.where(known,0,2).astype(np.int64),dict(provider='derivedTargetDecision',version='1',
            componentEvidenceId=link['componentEvidenceId'],components=[dict(componentId=c['componentId'],identity=c['identity']) for c in meta['components']]))
        require(filt is not None and projection.identity==filt.identity,'derived filter projection differs')
    else:require(filt is None,'unexpected final filter')
    members={'dependency-links.json'}
    for grid in link['nativeGrids']:members.update('grids/'+grid+'/'+n for n in ('arrays.npz','grid.json'))
    for identity in link['dependencies'].values():members.add('dependencies/'+identity+'/dependency.json')
    members.update('components/'+link['componentEvidenceId']+'/'+n for n in ('arrays.npz','components.json'))
    return members,[input_fact(doc,record.labels,bid,v) for bid,v in sorted(deps.items())]


def prepared_input(v):
    cov=json.loads(v.coverageJson)
    s=data.PreparedSeries(v.header['timeframe_minutes'],v.timestamps,v.values,v.researchMask,int(np.argmax(v.researchMask)),
        v.segmentStart,**cov,input_fingerprint=v.inputFingerprint)
    return PreparedInput(v.inputId,s,v.header)


def assert_arrays(actual,expected,prefix):
    require(set(actual)==set(expected),prefix+' array names differ')
    for name in sorted(actual):
        require(np.array_equal(actual[name],expected[name],equal_nan=True),prefix+' array differs: '+name)


def verify_commands(root,doc,record,link):
    inputs,deps,meta,arrays=read_evidence(root,link)
    bindings={v.inputId:prepared_input(v) for v in inputs}
    a=next(a for a in doc['allocations'] if a['allocationId']==record.labels['allocationId'])
    p=prepare_inputs(bindings['target'],tuple(bindings[k] for k in sorted(bindings) if k!='target'),
                     tuple(AccountFeatureRequest(**f) for f in a['features']))
    require(p.identity==record.prepared.identity and preparation_facts(p)==record.preparationFacts,'regenerated native preparation differs')
    policies={b['inputId']:b['coveragePolicy'] for b in doc['auxiliaryInputs']}
    bundle,evidence=build_auxiliary_commands(p,a,{bid:dict(content=v,coveragePolicy=policies[bid]) for bid,v in deps.items()})
    assert_arrays(evidence['arrays'],arrays,'component')
    require(evidence['metadata']==meta,'regenerated component metadata differs')
    saved=record.result.commands
    for side in ('long','short'):
        assert_arrays({f.name:getattr(getattr(bundle,side),f.name) for f in fields(bundle.long)},
                      {f.name:getattr(getattr(saved,side),f.name) for f in fields(saved.long)},'commands.'+side)
    require(bundle.identity==saved.identity,'regenerated command metadata/identity differs')
    require(evidence['identity']==link['componentEvidenceId'],'regenerated component identity differs')
    return p


def summary_metadata(reader):
    links=load_links(reader.root,reader.accountKeys);seen=set();rows=[]
    for key in reader.accountKeys:
        link=links[key]
        if link['componentEvidenceId'] in seen:continue
        seen.add(link['componentEvidenceId'])
        _,deps,meta,_=read_evidence(reader.root,link)
        features={name:dict(declaration=f['metadata']['feature'],identity=f['identity'],
            consumedSliceFingerprint=f['metadata']['consumedSliceFingerprint'],
            knownResearchDecisions=f['knownResearchDecisions'],unknownResearchDecisions=f['unknownResearchDecisions'],
            staleResearchDecisions=f['staleResearchDecisions']) for name,f in meta['features'].items()}
        rows.append(dict(componentEvidenceId=link['componentEvidenceId'],provider=meta['provider']['providerId'],
            components=[c['metadata']['component'] for c in meta['components']],features=features,
            sources=[dict(bindingId=bid,series=v['series'],inspection=inspect_content(v)) for bid,v in sorted(deps.items())]))
    return dict(schema='pattern-lab.account-auxiliary-metadata.v1',dependencies=rows,
        interpretation='Funding is rate per settlement, never cashflow; lags and retrieval completeness are assumptions. Derived filter clocks are target decisions, not exchange publication times.')
