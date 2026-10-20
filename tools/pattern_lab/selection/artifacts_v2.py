"""Explicit v2 storage/replay dispatch. Ordinary loads never execute or import scorers."""
from dataclasses import fields,asdict
from pathlib import Path
import numpy as np
from ..accounts.contracts import require,integer,ExecutionPolicy
from ..account_study.common import closed,json_text,digest_id,member_names
from ..account_study.request import bounds
from ..account_study import verify_account_study
from ..study.contracts import semantic_digest
from ..auxiliary.storage import safe_path
from . import artifacts as base
from .request import normalize_selection_request
from .ledger import admit_parent,extract_ledger
from .engine import validate_ledger,path_identity,data_identity,source_hashes,implementation
from .scorers import registry
from .auxiliary import admit_snapshots,ALGORITHMS
from . import scores_v2
from .facts_v2 import account_links,validate_links,validate_records
from .capacity_v2 import admit,assessment
from .carried import carried_calculation
from .fixed_v2 import fixed_calculation
from .funded import FundedExecutor
from .storage_v2 import FUNDED,PORTFOLIOS,FIXED,write_parts,read_parts,write_paths,read_paths


class SelectionV2(base.Selection):
    def __init__(self,*args,funded,portfolios,funded_paths,links):
        super().__init__(*args)
        self._funded=funded;self._portfolios=portfolios;self._funded_paths=funded_paths;self._links=links


def data_id(doc,ledger,paths,scores,allocation,links,parent_bounds):
    return semantic_digest(dict(shadow=data_identity(doc,ledger,paths,scores,allocation),accountLinks=links,
        parentBounds=parent_bounds,auxiliaryInputs=doc['auxiliaryInputs'],auxiliaryDependencies=[
            dict(selectorId=b['selectorId'],month=b['month'],contract=b['contract'],dependencies=b['auxiliaryDependencies']) for b in scores]))


def run(normalized,*,parent_root,output_root,scorers,auxiliary_roots,workers):
    doc=normalized.document;registered=registry(scorers);used={s['scorerId'] for s in doc['selectors'] if s['scorerId'].startswith('x.')}
    before={k:v for k,v in source_hashes(registered).items() if k in used}
    parent_root=safe_path(parent_root);target=base.new_destination(output_root,parent_root)
    parent=admit_parent(doc,parent_root);parent_bounds=list(bounds(parent.study['request']))
    snapshots=admit_snapshots(doc,auxiliary_roots,target,parent)
    ledger,paths,roster=extract_ledger(parent,doc);validate_ledger(doc,roster,ledger,paths)
    algorithm='monthly-selection.3'
    scores=scores_v2.build(parent,doc,ledger,roster,registered,snapshots,parent_bounds,algorithm=algorithm)
    budget=admit(doc,ledger,roster,scores)
    allocation=next(a for a in parent.study['request']['allocations'] if a['allocationId']==doc['allocationId'])
    links=account_links(parent,ledger);validate_links(links,ledger,allocation)
    target.mkdir(parents=True)
    try:
        if doc['capitalMode']=='carried':
            with FundedExecutor(workers) as executor:
                def provide(month,needs):
                    rows,arrays=executor.run(parent_root,needs)
                    validate_records(rows,month,ledger,links,allocation,arrays)
                    return rows,arrays
                results,funded,portfolios,funded_paths,curves=carried_calculation(doc,ledger,roster,scores,provide)
        else:
            results,portfolios,curves=fixed_calculation(doc,ledger,paths,roster,scores)
            funded={m:[] for m in doc['evaluationMonths']};funded_paths={m:{} for m in funded}
        require(before=={k:v for k,v in source_hashes(registered).items() if k in used},'experimental source changed during run')
        attribution=implementation(before);attribution['algorithm']=algorithm
        meta=dict(schema=base.SCHEMA,formatVersion=2,request=doc,roster=roster,allocation=allocation,parentBounds=parent_bounds,
            capacity=budget,implementation=attribution,identities=dict(specification=normalized.identity,
                dataInput=data_id(doc,ledger,paths,scores,allocation,links,parent_bounds),implementation=semantic_digest(attribution)))
        parts={}
        for m in doc['evaluationMonths']:
            parts[m]=dict(funded=write_parts(target,'funded',m,funded[m],FUNDED),
                portfolios=write_parts(target,'portfolios',m,portfolios[m],PORTFOLIOS if doc['capitalMode']=='carried' else FIXED),
                paths=write_paths(target,m,funded_paths[m]))
        for name,value in (('selection',meta),('ledger',ledger),('scores',scores),('results',results),('account-links',links),('parts',parts),
            ('provenance',dict(parentRoot=str(parent_root),auxiliaryRoots={k:str(safe_path(v)) for k,v in (auxiliary_roots or {}).items()},workers=workers))):
            base.write_json(target/(name+'.json'),value)
        for month,path in paths.items():base.save_arrays(target/'paths'/(month+'.npz'),path)
        reader=SelectionV2(target,meta,ledger,scores,results,paths,curves,funded=funded,portfolios=portfolios,funded_paths=funded_paths,links=links)
        base.write_derived(reader,target)
        base.write_json(target/'status.json',status(reader));base.seal_directory(target,2)
        return base.load_selection(target)
    except BaseException as exc:
        if (target/'completion.json').exists():(target/'completion.json').unlink()
        base.write_json(target/'status.json',dict(state='failed',errorType=type(exc).__name__,error=str(exc)))
        raise


def status(reader):
    return dict(state='complete',ledgerRows=len(reader.ledger),scoreBatches=len(reader.scores),comparisons=len(reader.results['comparisons']),
        fundedRows=sum(map(len,reader._funded.values())),portfolioRows=sum(map(len,reader._portfolios.values())),retainedPaths=sum(map(len,reader._funded_paths.values())))


def load(root,seal):
    meta=base.bounded_json(root/'selection.json')
    closed(meta,('schema','formatVersion','request','roster','allocation','parentBounds','capacity','implementation','identities'))
    require(meta['schema']==base.SCHEMA and type(meta['formatVersion']) is int and meta['formatVersion']==2,'v2 selection schema')
    normalized=normalize_selection_request(meta['request'],stored=True);doc=normalized.document
    require(doc==meta['request'] and doc['schema']=='pattern-lab.selection-request.v2','normalized v2 request')
    allocation=meta['allocation'];closed(allocation,('allocationId','timeframeMinutes','features','provider','entryFilter','policy'),('auxiliaryFeatures',))
    integer(allocation['timeframeMinutes'],'timeframe',1);closed(allocation['policy'],[f.name for f in fields(ExecutionPolicy)])
    require(asdict(ExecutionPolicy(**allocation['policy']))==allocation['policy'] and allocation['allocationId']==doc['allocationId'] and
        allocation['policy']['initialCapital']==doc['referenceCapital'],'v2 saved allocation')
    pb=meta['parentBounds'];require(type(pb) is list and len(pb)==3 and all(type(v) is int for v in pb) and pb[0]<=pb[1]<pb[2],'parent bounds')
    ledger=base.bounded_json(root/'ledger.json');scores=base.bounded_json(root/'scores.json');results=base.bounded_json(root/'results.json')
    links=base.bounded_json(root/'account-links.json');parts=base.bounded_json(root/'parts.json')
    paths={w['month']:base.load_arrays(root/'paths'/(w['month']+'.npz')) for w in doc['monthWindows']}
    require(all(set(p)=={'times','pnl'} for p in paths.values()),'shadow path members')
    validate_ledger(doc,meta['roster'],ledger,paths);validate_links(links,ledger,allocation)
    require(all(r['stepMs']==allocation['timeframeMinutes']*60000 and pb[1]<=r['startMs']<r['endMs']<=pb[2] for r in ledger),'allocation timeframe/parent bounds')
    require(meta['implementation']['algorithm'] in ALGORITHMS,'unknown selection algorithm')
    scores_v2.validate(doc,ledger,meta['roster'],scores,pb,algorithm=meta['implementation']['algorithm'])
    closed(meta['identities'],('specification','dataInput','implementation'))
    require(meta['identities']==dict(specification=normalized.identity,dataInput=data_id(doc,ledger,paths,scores,allocation,links,pb),
        implementation=semantic_digest(meta['implementation'])),'v2 identities')
    impl=meta['implementation'];closed(impl,('algorithm','sources','experimental','python','libraries'))
    require(impl['algorithm'] in ALGORITHMS and type(impl['python']) is str and type(impl['experimental']) is dict,'implementation metadata')
    closed(impl['libraries'],('numpy','pandas','pyarrow','numba'))
    require(all(type(v) is str and v for v in impl['libraries'].values()),'library metadata')
    for hashes in [impl['sources'],*impl['experimental'].values()]:
        require(type(hashes) is dict and hashes,'implementation sources');member_names(hashes)
        for h in hashes.values():digest_id(h)
    require(set(impl['experimental'])=={s['scorerId'] for s in doc['selectors'] if s['scorerId'].startswith('x.')},'experimental attribution')
    require(meta['capacity']==assessment(doc,ledger,meta['roster'],scores),'saved capacity assessment')
    require(type(parts) is dict and set(parts)==set(doc['evaluationMonths']),'monthly part roster')
    expected={'selection.json','ledger.json','scores.json','results.json','account-links.json','parts.json','status.json','provenance.json',*base.DERIVED,
        *('paths/'+m+'.npz' for m in paths)}
    funded={};portfolios={};funded_paths={};carried=doc['capitalMode']=='carried'
    for m in doc['evaluationMonths']:
        index=parts[m];closed(index,('funded','portfolios','paths'))
        for kind,folder,ext in (('funded','funded','parquet'),('portfolios','portfolios','parquet'),('paths','funded-paths','npz')):
            names=index[kind];require(type(names) is list and names==[f'{folder}/{m}/part-{n:05d}.{ext}' for n in range(len(names))],'canonical part filenames')
            expected.update(names)
        funded[m]=read_parts(root,index['funded'],FUNDED)
        portfolios[m]=read_parts(root,index['portfolios'],PORTFOLIOS if carried else FIXED)
        funded_paths[m]=read_paths(root,index['paths'])
        validate_records(funded[m],m,ledger,links,allocation,funded_paths[m])
    require(set(seal['members'])==expected,'unexpected v2 member')
    if carried:
        actual,_,rows,_,curves=carried_calculation(doc,ledger,meta['roster'],scores,lambda m,needs:(funded[m],funded_paths[m]))
    else:
        require(not any(funded.values()) and not any(funded_paths.values()),'fixed mode funded records')
        actual,rows,curves=fixed_calculation(doc,ledger,paths,meta['roster'],scores)
    base.assert_equal(portfolios,rows,'monthly portfolio facts/capital carry');base.assert_equal(results,actual,'v2 portfolio results')
    reader=SelectionV2(root,meta,ledger,scores,results,paths,curves,funded=funded,portfolios=portfolios,funded_paths=funded_paths,links=links)
    require(base.bounded_json(root/'status.json')==status(reader),'v2 status')
    provenance=base.bounded_json(root/'provenance.json');closed(provenance,('parentRoot','auxiliaryRoots','workers'))
    require(type(provenance['parentRoot']) is str and type(provenance['auxiliaryRoots']) is dict and
        set(provenance['auxiliaryRoots'])=={b['snapshotId'] for b in doc['auxiliaryInputs']} and all(type(v) is str for v in provenance['auxiliaryRoots'].values()),'v2 provenance paths')
    integer(provenance['workers'],'workers',1);require(provenance['workers']<=6,'worker provenance')
    from .report_v2 import summary
    base.assert_equal(reader.summary,summary(reader),'v2 saved summary')
    return reader


def first_difference(actual,expected,prefix=''):
    if type(actual) is dict and type(expected) is dict:
        if set(actual)!=set(expected):return prefix+'.keys'
        for key in sorted(expected):
            found=first_difference(actual[key],expected[key],prefix+'.'+key)
            if found:return found
    elif actual!=expected:return prefix
    return None


def verify(reader,*,parent_root,scorers,auxiliary_roots,workers,require_scorers):
    integer(workers,'workers',1);require(workers<=6,'workers must be 1..6')
    meta=reader.selection;doc=meta['request'];parent=admit_parent(doc,safe_path(parent_root))
    parent_result=verify_account_study(parent.root)
    require(meta['allocation']==next(a for a in parent.study['request']['allocations'] if a['allocationId']==doc['allocationId']),'deep allocation differs')
    require(meta['parentBounds']==list(bounds(parent.study['request'])),'deep parent bounds differ')
    ledger,paths,roster=extract_ledger(parent,doc)
    base.assert_equal(ledger,reader.ledger,'parent shadow ledger');require(roster==meta['roster'] and path_identity(paths)==path_identity(reader._paths),'parent shadow paths')
    base.assert_equal(account_links(parent,ledger),reader._links,'parent account links')
    replayed=0
    with FundedExecutor(workers) as executor:
        for month in doc['evaluationMonths']:
            needs={}
            for r in reader._funded[month]:needs.setdefault(r['accountKey'],{})[r['capital']]=r['pathRef'] is not None
            rows,arrays=executor.run(parent.root,needs)
            require(len(rows)==len(reader._funded[month]),'deep funded row count')
            for actual,saved in zip(rows,reader._funded[month]):
                difference=first_difference(actual,saved)
                require(difference is None,'funded replay mismatch '+saved['executionKey']+' first field '+str(difference))
                if saved['pathRef'] is not None:
                    for name in ('times','equity'):
                        require(np.array_equal(arrays[saved['executionKey']][name],reader._funded_paths[month][saved['executionKey']][name]),
                            'funded replay mismatch '+saved['executionKey']+' first field path.'+name)
            replayed+=len(rows)
    registered=registry(scorers);provided=source_hashes(registered)
    for name,hashes in provided.items():require(name in meta['implementation']['experimental'] and hashes==meta['implementation']['experimental'][name],'experimental source mismatch')
    for s in doc['selectors']:
        if s['scorerId'] in registered:
            scorer=registered[s['scorerId']]
            require(scorer.version==s['version'] and scorer.normalizeParameters(s['parameters'])==s['parameters'],'deep scorer version/parameters')
    snapshots=admit_snapshots(doc,auxiliary_roots,reader.root,parent) if auxiliary_roots is not None else {} if not doc['auxiliaryInputs'] else None
    saved_contracts={b['selectorId']:b['contract'] for b in reader.scores}
    levels={s['selectorId']:'recomputed' if s['scorerId'] in registered and (not saved_contracts[s['selectorId']]['auxiliaryUses'] or snapshots is not None)
        else 'storedOutputsOnly' for s in doc['selectors']}
    require(not require_scorers or all(v=='recomputed' for v in levels.values()),'required scorer recomputation dependencies missing')
    scores=scores_v2.build(parent,doc,ledger,roster,registered,snapshots,meta['parentBounds'],stored=reader.scores,algorithm=meta['implementation']['algorithm'])
    base.assert_equal(scores,reader.scores,'deep score outputs/contracts/dependencies')
    require(provided==source_hashes(registered),'experimental source changed during verification')
    return dict(status='verified',level='exactParent',parent=parent_result,fundedExecutions=replayed,scorers=levels,
        auxiliaryRecomputed=any(levels[b['selectorId']]=='recomputed' and b['auxiliaryDependencies'] for b in scores))
