"""Scalar entries and flat all-of filters projected to the unchanged executor."""
from dataclasses import fields
import json
import numpy as np

from ..accounts import build_commands, candle_filter, HARecipe
from ..accounts.contracts import EntryFilter, CommandBundle, SideCommands, require, command_array_digest, validate_bundle
from ..accounts.providers import real_swing_atr
from .features import build_feature, condition, transition_pulse
from .request import components


def build_auxiliary_commands(prepared, allocation, dependencies):
    decisions = prepared.target.timestamps+prepared.stepMs
    aligned, arrays, feature_facts = {}, {}, {}
    for f in allocation['auxiliaryFeatures']:
        dep = dependencies[f['bindingId']]
        state, facts, identity = build_feature(dep['content'],f,decisions,
            start=prepared.preparationStartMs,end=prepared.preparationEndMs)
        research=prepared.target.researchMask
        require(state.known[research].any(),'zero usable auxiliary research overlap '+f['featureId'])
        if dep['coveragePolicy']=='requireComplete':
            require(state.known[research].all(),'incomplete required auxiliary decisions '+f['featureId'])
        aligned[f['featureId']]=state
        for field in fields(state):arrays['feature.'+f['featureId']+'.'+field.name]=getattr(state,field.name)
        feature_facts[f['featureId']]=dict(identity=identity,metadata=facts,
            knownResearchDecisions=int(state.known[research].sum()),unknownResearchDecisions=int((~state.known[research]).sum()),
            staleResearchDecisions=int((state.unknownReason[research]==3).sum()))
    component_facts=[];permissions=[]
    for component in sorted(components(allocation['entryFilter']),key=lambda c:c['componentId']):
        if component['providerId']=='candleDirection':
            native=candle_filter(prepared,component['requestId'])
            source=prepared.feature(component['requestId']).aligned
            known,long,short=source.known,native.allowLong,native.allowShort
            metadata=json.loads(native.provenance)
        else:
            source=aligned[component['featureId']]
            known=source.known
            long=known & condition(source.value,component['long'])
            short=known & condition(source.value,component['short'])
            metadata=dict(featureIdentity=feature_facts[component['featureId']]['identity'])
        facts={f.name:getattr(source,f.name) for f in fields(source)}
        facts.update(allowLong=long,allowShort=short)
        meta=dict(component=component,source=metadata)
        identity=command_array_digest(facts,meta)
        arrays.update({'component.'+component['componentId']+'.'+k:v for k,v in facts.items()})
        component_facts.append(dict(componentId=component['componentId'],identity=identity,metadata=meta))
        permissions.append((known,long,short))
    meta=dict(schema='pattern-lab.auxiliary-decisions.v1',features=feature_facts,components=component_facts,
              provider=allocation['provider'],projection='derived-target-decision.1')
    evidence_id=command_array_digest(arrays,meta)
    filt=None
    if permissions:
        known=np.logical_and.reduce([p[0] for p in permissions])
        long=known & np.logical_and.reduce([p[1] for p in permissions])
        short=known & np.logical_and.reduce([p[2] for p in permissions])
        filt=EntryFilter(prepared.identity,decisions,known,long,short,np.arange(len(decisions),dtype=np.int64),
            decisions,decisions,decisions+prepared.stepMs,np.where(known,0,2).astype(np.int64),
            dict(provider='derivedTargetDecision',version='1',componentEvidenceId=evidence_id,
                 components=[dict(componentId=c['componentId'],identity=c['identity']) for c in component_facts]))
    provider,policy=allocation['provider'],allocation['policy']
    if provider['providerId']=='haRun':
        base=build_commands(prepared,HARecipe(**provider['recipe']),mode=policy['mode'],stopUpdates=policy['stopUpdates'],entryFilter=filt)
        long,short,provenance=base.long,base.short,json.loads(base.provenance)
    else:
        state=aligned[provider['featureId']];sides=[]
        for label,sign in (('long',1),('short',-1)):
            truth=condition(state.value,provider[label]);rule=provider['stop']
            stop,known=real_swing_atr(prepared,rule['atrRequestId'],rule['swingBars'],rule['atrMultiplier'],sign)
            n=len(decisions)
            sides.append(SideCommands(transition_pulse(truth,state.known,prepared.target.segmentStart),state.known,
                stop,known,np.zeros(n,bool),np.zeros(n,bool),np.full(n,np.nan),np.zeros(n,bool)))
        long,short=sides;provenance=dict(provider='scalarCondition',version='1',declaration=provider)
    provenance['auxiliaryPreparation']=dict(version='1',componentEvidenceId=evidence_id,
        features={k:v['identity'] for k,v in sorted(feature_facts.items())})
    bundle=CommandBundle(prepared.identity,decisions,long,short,policy['mode'],policy['stopUpdates'],provenance,filt)
    validate_bundle(prepared,bundle)
    return bundle,dict(identity=evidence_id,metadata=meta,arrays=arrays)
