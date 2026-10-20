"""Coordinator scoring and offline checks of closed causal dependency cells."""
from ..accounts.contracts import require,integer
from ..account_study.common import closed,json_text,parse_json,digest_id
from ..auxiliary.study import source_for
from .request import month_bounds
from .request_v2 import scorer_contract,normalize_uses
from .ledger import history_pool,score_view
from .scorers import evaluate
from .engine import validate_scores_saved
from .auxiliary import AuxiliaryView,prepare_batch,envelope,availability
from .capacity_v2 import auxiliary_plan


def build(parent,doc,ledger,roster,registered,snapshots,parent_bounds,stored=None,*,algorithm):
    saved={(b['selectorId'],b['month']):b for b in stored or []}
    contracts={s['selectorId']:scorer_contract(registered[s['scorerId']],s['parameters'],doc)
        if s['scorerId'] in registered else saved[s['selectorId'],doc['evaluationMonths'][0]]['contract'] for s in doc['selectors']}
    active={s['selectorId'] for s in doc['selectors'] if s['scorerId'] in registered}
    if stored is not None:
        for sid in active:
            require(contracts[sid]==saved[sid,doc['evaluationMonths'][0]]['contract'],'supplied scorer contract differs')
    batches=[];months=[w['month'] for w in doc['monthWindows']]
    bindings={b['inputId']:b for b in doc['auxiliaryInputs']}
    require({u['bindingId'] for c in contracts.values() for u in c['auxiliaryUses']}==set(bindings),'unused auxiliary bindings')
    recomputed={sid for sid in active if not contracts[sid]['auxiliaryUses'] or snapshots is not None}
    auxiliary_plan(doc,contracts,ledger,roster,snapshots,parent_bounds,recomputed)
    # Refuse ANY required bad cell before the first scorer call. Discard each
    # validation batch; never retain all raw months to save a second slice read.
    for sid in sorted(recomputed):
        c=contracts[sid];required=dict(c,auxiliaryUses=[u for u in c['auxiliaryUses'] if bindings[u['bindingId']]['coveragePolicy']=='requireComplete'])
        if not required['auxiliaryUses']:continue
        for month in doc['evaluationMonths']:
            index=months.index(month);history=months[index-c['lookbackMonths']:index]
            pool,_=history_pool(ledger,roster,months[index-doc['pool']['historyMonths']:index],doc['pool']['minObservedBarsPct'])
            prepare_batch(doc,required,pool,history,month_bounds(month)[0],snapshots,parent_bounds,algorithm=algorithm)
    for s in doc['selectors']:
        sid=s['selectorId'];c=contracts[sid]
        require(c['lookbackMonths']<=doc['pool']['historyMonths'],'scorer history contract')
        for month in doc['evaluationMonths']:
            if sid not in recomputed:
                batches.append(saved[sid,month]);continue
            require(scorer_contract(registered[s['scorerId']],s['parameters'],doc)==c,'scorer contract changed across months')
            index=months.index(month);seen=months[index-c['lookbackMonths']:index];cutoff=month_bounds(month)[0]
            eligible,excluded=history_pool(ledger,roster,months[index-doc['pool']['historyMonths']:index],doc['pool']['minObservedBarsPct'])
            price,deps=score_view(parent,doc,ledger,eligible,seen,cutoff,c['needsBars'])
            diagnostics,values=prepare_batch(doc,c,eligible,seen,cutoff,snapshots,parent_bounds,algorithm=algorithm)
            rows=evaluate(registered[s['scorerId']],AuxiliaryView(price,values),s['parameters'])
            del values,price
            unknown={d['instrumentId'] for d in diagnostics if not d['available']}
            for r in rows:
                if r['instrumentId'] in unknown:r.update(eligible=False,key=None,reason='auxiliaryUnavailable')
            batches.append(dict(selectorId=sid,month=month,cutoffMs=cutoff,historyMonths=seen,contract=c,
                commonPool=eligible,commonExclusions=excluded,barDependencies=deps,auxiliaryDependencies=diagnostics,rows=rows))
    validate(doc,ledger,roster,batches,parent_bounds,algorithm=algorithm)
    return batches


def validate(doc,ledger,roster,batches,parent_bounds,*,algorithm):
    projected=parse_json(json_text(batches));contracts={};bindings={b['inputId']:b for b in doc['auxiliaryInputs']};used=set()
    for b,p in zip(batches,projected):
        require(type(b) is dict and 'auxiliaryDependencies' in b,'auxiliary dependencies required')
        c=b['contract'];closed(c,('lookbackMonths','needsBars','keyNames','auxiliaryUses'))
        require(c['auxiliaryUses']==normalize_uses(c['auxiliaryUses'],doc),'saved auxiliary uses')
        signature=json_text(c);require(contracts.setdefault(b['selectorId'],signature)==signature,'saved auxiliary contract changes')
        uses={u['useId']:u for u in c['auxiliaryUses']};used.update(u['bindingId'] for u in uses.values())
        dependencies=b['auxiliaryDependencies'];require(type(dependencies) is list,'auxiliary dependency table')
        require([(d['instrumentId'],d['useId']) for d in dependencies]==[(i,u) for i in b['commonPool'] for u in uses],'auxiliary dependency roster/order')
        for d in dependencies:
            closed(d,('instrumentId','useId','bindingId','source','startMs','endMs','cutoffMs','publicationLagMs','coveragePolicy','coverage','complete','available','reason','consumedSliceFingerprint','featureState'))
            use=uses[d['useId']];binding=bindings[use['bindingId']];source=source_for(binding,d['instrumentId'])
            start,end=envelope(use,source,b['historyMonths'],b['cutoffMs'],parent_bounds)
            require((d['bindingId'],d['source'],d['startMs'],d['endMs'],d['cutoffMs'],d['publicationLagMs'],d['coveragePolicy'])==
                (use['bindingId'],source,start,end,b['cutoffMs'],use['publicationLagMs'],binding['coveragePolicy']),'auxiliary dependency binding/bounds')
            digest_id(d['consumedSliceFingerprint']);require(type(d['complete']) is bool and type(d['available']) is bool,'auxiliary availability types')
            require(type(d['coverage']) is list,'auxiliary coverage');previous=start-1
            for span in d['coverage']:
                require(type(span) is list and len(span)==2 and all(type(v) is int for v in span) and start<=span[0]<span[1]<=end and span[0]>previous,'auxiliary coverage spans')
                previous=span[1]
            require(not d['complete'] or d['coverage']==[[start,end]],'complete auxiliary envelope')
            state=d['featureState']
            if use['access']=='raw':require(state is None,'raw feature state')
            else:
                closed(state,('value','known','reason','sourceIndex','observedAtMs','availableAtMs','validUntilMs','identity'))
                digest_id(state['identity']);require(type(state['known']) is bool,'feature known flag')
                for key in ('sourceIndex','observedAtMs','availableAtMs','validUntilMs'):require(type(state[key]) is int,'feature timestamp/index')
                if state['known']:
                    import math
                    require(type(state['value']) in (float,int) and math.isfinite(state['value']) and state['reason'] is None and
                        start<=state['observedAtMs']<end and state['availableAtMs']==state['observedAtMs']+use['publicationLagMs'] and
                        state['availableAtMs']<=b['cutoffMs']<state['validUntilMs'],'known feature clock/value')
                else:require(state['value'] is None and type(state['reason']) is str and state['reason'],'unknown feature reason')
            available,reason=availability(d['complete'],state,binding['coveragePolicy'],algorithm)
            require(d['available']==available and d['reason']==reason,'auxiliary availability')
            require(binding['coveragePolicy']!='requireComplete' or available,'required saved auxiliary unavailable')
            if not available:
                row=next(r for r in b['rows'] if r['instrumentId']==d['instrumentId'])
                require(not row['eligible'] and row['reason']=='auxiliaryUnavailable','coordinator auxiliary exclusion')
        p.pop('auxiliaryDependencies');p['contract'].pop('auxiliaryUses')
    require(used==set(bindings),'unused auxiliary bindings')
    validate_scores_saved(doc,ledger,roster,projected)
