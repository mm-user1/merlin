"""Coordinator-only scoped preparation and canonical bounded native dispatch."""
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timezone
import time

import numpy as np

from .. import data, manifest, PatternLabDataError
from ..accounts import PreparedInput
from ..accounts.contracts import require, integer
from ..account_study.job import AccountJob, run_account_job, check_generation
from ..account_study.runner import implementation as account_implementation
from ..account_study.common import file_hash, fresh_directory
from ..account_study.codec import preparation_facts
from ..account_study.request import bounds
from ..study.runner import metadata_admission_failures
from ..study.bracket_rules import normalize_rules
from ..study import workers as workers_module
from .request import normalize_request, child_request, configurations, result_id
from .metrics import extract_metrics
from .curves import account_curve, price_curve
from .artifacts import Writer, groups, save_json, load_configuration_study
from . import codec, validation


def implementation(auxiliary):
    result=account_implementation(auxiliary=auxiliary)
    repo=Path(__file__).resolve().parents[3]
    names={p.relative_to(repo).as_posix() for p in Path(__file__).parent.glob('*.py')}
    names.add('src/core/metrics.py')
    result['sources'].update({name:file_hash(repo/name) for name in sorted(names)})
    result['sources']=dict(sorted(result['sources'].items()))
    result['version']='configuration-study.1'
    return result


class Progress:
    def __init__(self,root,count,workers):
        self.root=root;self.last=-float('inf')
        self.state=dict(state='running',phase='admission',currentJob=None,plannedResults=count,
            completedResults=0,requestedWorkers=workers,maximumInFlight=0,maximumCompactBuffer=0,
            startedUtc=datetime.now(timezone.utc).isoformat())
        self.write(force=True)

    def write(self,*,force=False):
        now=time.monotonic()
        if force or now-self.last>=1:
            save_json(self.root/'status.json',self.state);self.last=now


def prepare_scope(session,doc,instrument,allocations,entries,rules,snapshots,source,prices,physical):
    """One instrument/target-TF/stage lifetime; only native binding primitives."""
    origin,start,end=bounds(doc);raw={};prepared={};bindings={}
    def base(identifier):
        if identifier not in raw:
            loaded=session.load_slice(identifier,start=manifest.format_epoch_ms(start),end=manifest.format_epoch_ms(end),
                warmup_start=manifest.format_epoch_ms(origin),timeframe_minutes=5)
            raw[identifier]=(loaded.bars.index.view('int64')//1000000,loaded.bars.to_numpy(dtype=np.float64))
            physical[identifier]=dict(loaded.physical)
            if identifier==instrument and instrument not in prices:
                prices[instrument]=price_curve(*raw[identifier],start,end,loaded.input_fingerprint)
        return raw[identifier]
    for allocation in allocations:
        used={f['inputId'] for f in allocation['features']}
        declarations=[dict(inputId='target',instrumentId='target',timeframeMinutes=allocation['timeframeMinutes'])]
        declarations += [s for s in doc['sources'] if s['inputId'] in used]
        values=[]
        for declaration in declarations:
            resolved=instrument if declaration['instrumentId']=='target' else declaration['instrumentId']
            key=(resolved,declaration['timeframeMinutes'])
            if key not in prepared:
                times,ohlcv=base(resolved);entry=entries[resolved]
                header=data.fingerprint_header(instrument_id=resolved,venue=entry['venue'],contract=entry['contract'],
                    quote_currency=entry['quote_currency'],timeframe_minutes=key[1],start_ms=start,end_ms=end,warmup_start_ms=origin)
                series=data.prepare_series(timestamps=times,ohlcv=ohlcv,**{k:header[k] for k in
                    ('instrument_id','venue','contract','quote_currency','timeframe_minutes','start_ms','end_ms','warmup_start_ms')})
                for value in vars(series).values():
                    if isinstance(value,np.ndarray):value.setflags(write=False)
                prepared[key]=(header,series)
            header,series=prepared[key];values.append(PreparedInput(declaration['inputId'],series,header))
        bindings[allocation['allocationId']]=tuple(values)
    dependencies=None
    if 'auxiliaryInputs' in doc:
        from ..auxiliary.study import dependencies_for
        dependencies=dependencies_for({**doc,'allocations':allocations},instrument,snapshots,origin,end,{})
    return bindings,dependencies


def extract_unit(record,configuration,stage,synthetic=False):
    r=record.result
    require(record.labels==dict(instrumentId=configuration['instrumentId'],allocationId=configuration['allocationId'],
        windowId=stage['stageId']) and r.window.startMs==manifest.to_epoch_ms(stage['startUtc'],'stage') and
        r.window.endMs==manifest.to_epoch_ms(stage['endUtc'],'stage'),'native result coordinates')
    common=dict(configurationId=configuration['configurationId'],resultId=result_id(configuration['configurationId'],stage),stageId=stage['stageId'])
    aux=[]
    if record.auxiliaryEvidence:
        from ..auxiliary.model import fingerprint
        aux=[dict(inputId=k,coveragePolicy=v['coveragePolicy'],consumedSliceFingerprint=fingerprint(v['content']))
             for k,v in sorted(record.auxiliaryEvidence['dependencies'].items())]
    metric=dict(**common,status='complete',metrics=extract_metrics(record),execution=dict(kind='native',
        accountId=r.identity,preparationId=r.preparationId,commandId=r.commandId,validation='nativeValidated',
        inputs=preparation_facts(record.prepared)['inputs'],rules=asdict(r.rules),auxiliary=aux))
    curve=dict(**common,curve=account_curve(record))
    validation.metric_row(metric,configuration,stage,False);validation.curve_row(curve,metric,stage)
    unit=dict(metric=metric,curve=curve)
    packed,info=codec.encode(unit);require(codec.decode(packed,info)==unit,'compact unit round trip')
    return unit


def execute_group(group,make_job,stage,pool,workers,progress,timings):
    """Dispatch credit is released only when the canonical prefix is consumed."""
    submitted=consumed=0;ready={};pending={}
    while consumed<len(group):
        while submitted<len(group) and submitted-consumed<workers:
            configuration=group[submitted];key=configuration['configurationId']
            progress.state.update(phase='execute',currentJob=dict(stageId=stage['stageId'],**configuration))
            job=make_job(configuration)
            if pool is None:
                started=time.perf_counter();records=run_account_job(job);timings['directExecutionSeconds']+=time.perf_counter()-started
                started=time.perf_counter()
                require(len(records)==1,'one native result per job')
                ready[submitted]=extract_unit(records[0],configuration,stage)
                timings['extractionSeconds']+=time.perf_counter()-started
                del records
            else:pool.submit(key,job);pending[key]=submitted
            del job;submitted+=1
            progress.state['maximumInFlight']=max(progress.state['maximumInFlight'],submitted-consumed)
        if consumed not in ready:
            kind,key,records=pool.take()
            require(kind=='result' and key in pending,'native worker failed')
            index=pending.pop(key);configuration=group[index]
            progress.state.update(phase='extract',currentJob=dict(stageId=stage['stageId'],**configuration))
            require(len(records)==1,'one native result per job')
            started=time.perf_counter();ready[index]=extract_unit(records[0],configuration,stage)
            timings['extractionSeconds']+=time.perf_counter()-started
            del records
        progress.state['maximumCompactBuffer']=max(progress.state['maximumCompactBuffer'],len(ready)+16)
        while consumed in ready:
            unit=ready.pop(consumed);consumed+=1
            progress.state['completedResults']+=1;progress.write()
            yield unit
            del unit


def run_configuration_study(request,*,data_root,output_root,workers=1,auxiliary_roots=None):
    normalized=normalize_request(request);doc=normalized.document
    integer(workers,'workers',1);require(workers<=6,'workers must be 1..6')
    rows=configurations(doc);pack=Path(data_root).expanduser().resolve();output=Path(output_root).expanduser().resolve()
    require(not output.exists() and not output.is_relative_to(pack) and not pack.is_relative_to(output),'new disjoint output required')
    children=[child_request(doc,s) for s in doc['stages']]
    snapshots=None
    if 'auxiliaryInputs' in doc:
        from ..auxiliary.study import admit
        snapshots=admit(children[0],auxiliary_roots,output,pack)
    else:require(auxiliary_roots is None,'v1 has no auxiliary roots')
    source=implementation('auxiliaryInputs' in doc)
    with data.read_session(pack) as session:
        report=session.inspect(verify=False);require(report.get('state')=='ready','pack not ready')
        entries={e['instrument_id']:e for e in report['instruments']}
        wanted=set(doc['instruments'])|{s['instrumentId'] for s in doc['sources'] if s['instrumentId']!='target'}
        require(wanted<=entries.keys(),'declared target/source absent from pack')
        require(all(set(entries[i]['roles'])&{'trading','research_only'} for i in doc['instruments']),'factor-only target')
        for child in children:
            origin,start,end=bounds(child)
            failures=metadata_admission_failures([entries[i] for i in sorted(wanted)],
                SimpleNamespace(warmup_start_ms=origin,study_start_ms=start,study_end_ms=end,timeframes=(5,)))
            require(not failures,'configuration stage metadata admission failed')
        raw_entries={e['instrument_id']:e for e in manifest.read_manifest(pack)['instruments']}
        rules={i:normalize_rules(raw_entries[i]) for i in doc['instruments']}
        require(session.inspect(verify=True)['verification_check']['ok'],'pack integrity admission failed')
        root=fresh_directory(output,pack);progress=Progress(root,3*len(rows),workers)
        writer=None;pool=None;timings=dict(preparationSeconds=0.,extractionSeconds=0.,directExecutionSeconds=0.)
        physical={};prices={};started=time.perf_counter()
        try:
            writer=Writer(root,normalized,source)
            with workers_module.single_threaded_children():
                effective=min(workers,max(len(g) for _,g in groups(rows)))
                if workers>1:
                    pool=workers_module.SpawnJobPool(effective,workers_module.WorkerSettings((),(),()))
                    pool.start();progress.state.update(workerPids=pool.worker_pids,childThreads=pool.thread_evidence)
                progress.state['effectiveWorkers']=effective
                for stage,child in zip(doc['stages'],children):
                    price_stage={};physical_stage={}
                    for (instrument,tf),group in groups(rows):
                        progress.state.update(phase='prepare',currentJob=dict(stageId=stage['stageId'],instrumentId=instrument,timeframeMinutes=tf))
                        selected={r['allocationId'] for r in group}
                        allocations=[a for a in child['allocations'] if a['allocationId'] in selected]
                        tick=time.perf_counter()
                        bindings,deps=prepare_scope(session,child,instrument,allocations,entries,rules,snapshots,source,price_stage,physical_stage)
                        timings['preparationSeconds']+=time.perf_counter()-tick
                        allocation_map={a['allocationId']:a for a in allocations}
                        def make_job(config):
                            aid=config['allocationId']
                            return AccountJob(instrument,(allocation_map[aid],),tuple(child['windows']),{aid:bindings[aid]},
                                rules[instrument],source['sources'],None if deps is None else {aid:deps[aid]})
                        check_generation(source['sources'])
                        writer.group(stage['stageId'],instrument,tf,execute_group(group,make_job,stage,pool,effective,progress,timings))
                        check_generation(source['sources']);del bindings,deps
                    prices[stage['stageId']]=price_stage;physical[stage['stageId']]=physical_stage
                if pool is not None:pool.shutdown(abort=False);pool=None
            progress.state.update(phase='publish',currentJob=None)
            for instrument in doc['instruments']:
                for stage in sorted(doc['stages'],key=lambda s:s['stageId']):
                    writer.price(dict(instrumentId=instrument,stageId=stage['stageId'],price=prices[stage['stageId']][instrument]))
            timings['elapsedSeconds']=time.perf_counter()-started
            check_generation(source['sources'])
            writer.finish(dict(physicalInputs=physical,timings=timings),progress.state,
                          before_completion=lambda:check_generation(source['sources']))
        except BaseException as exc:
            if pool is not None:pool.shutdown(abort=True)
            if writer is not None:writer.close()
            progress.state.update(state='failed',errorType=type(exc).__name__,reasonCode='configurationOperationFailed')
            progress.write(force=True)
            if isinstance(exc,KeyboardInterrupt):raise KeyboardInterrupt from None
            raise PatternLabDataError('Configuration study failed; see sanitized status for phase and coordinates.') from None
    return load_configuration_study(root)
