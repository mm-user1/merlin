"""Scoped preparation and bounded canonical consumption of existing AccountJobs."""
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
from ..account_study.request import bounds
from ..account_study.common import file_hash, read_json
from ..study.runner import metadata_admission_failures
from ..study.bracket_rules import normalize_rules
from ..study import workers as workers_module
from .request import normalize_request, configurations, child_request, cell_id
from .facts import extract
from .artifacts import Writer, save_json, safe_path, output_path, reserve, completion_valid, load_strategy_screen


def implementation(auxiliary):
    source=account_implementation(auxiliary=auxiliary);repo=Path(__file__).resolve().parents[3]
    names={p.relative_to(repo).as_posix() for p in Path(__file__).parent.glob('*.py')}
    # Direct reusable metric/validation/identity/codec modules and their constants.
    names.update('tools/pattern_lab/configuration_study/'+n+'.py' for n in ('metrics','validation','request','codec','curves','__init__'))
    names.add('src/core/metrics.py')
    source['sources'].update({name:file_hash(repo/name) for name in sorted(names)})
    source['sources']=dict(sorted(source['sources'].items()));source['version']='strategy-screen.1'
    return source


class Progress:
    def __init__(self,root,state):
        self.root=root;self.state=state;self.last=-float('inf');self.write(force=True)
    def write(self,*,force=False):
        now=time.monotonic()
        if force or now-self.last>=1:
            save_json(self.root/'status.json',self.state);self.last=now


def prepare_scope(session,doc,instrument,entries,snapshots,physical):
    """Only native preparation primitives; no unused sampled price curves."""
    origin,start,end=bounds(doc);raw={};prepared={};bindings={}
    def base(identifier):
        if identifier not in raw:
            loaded=session.load_slice(identifier,start=manifest.format_epoch_ms(start),end=manifest.format_epoch_ms(end),
                warmup_start=manifest.format_epoch_ms(origin),timeframe_minutes=5)
            raw[identifier]=(loaded.bars.index.view('int64')//1000000,loaded.bars.to_numpy(dtype=np.float64))
            physical[identifier]=dict(loaded.physical)
        return raw[identifier]
    for a in doc['allocations']:
        used={f['inputId'] for f in a['features']}
        declarations=[dict(inputId='target',instrumentId=instrument,timeframeMinutes=a['timeframeMinutes'])]
        declarations += [s for s in doc['sources'] if s['inputId'] in used]
        values=[]
        for s in declarations:
            resolved=instrument if s['instrumentId']=='target' else s['instrumentId'];key=(resolved,s['timeframeMinutes'])
            if key not in prepared:
                times,ohlcv=base(resolved);entry=entries[resolved]
                header=data.fingerprint_header(instrument_id=resolved,venue=entry['venue'],contract=entry['contract'],
                    quote_currency=entry['quote_currency'],timeframe_minutes=key[1],start_ms=start,end_ms=end,warmup_start_ms=origin)
                series=data.prepare_series(timestamps=times,ohlcv=ohlcv,**{k:header[k] for k in
                    ('instrument_id','venue','contract','quote_currency','timeframe_minutes','start_ms','end_ms','warmup_start_ms')})
                for array in vars(series).values():
                    if isinstance(array,np.ndarray):array.setflags(write=False)
                prepared[key]=(header,series)
            header,series=prepared[key];values.append(PreparedInput(s['inputId'],series,header))
        bindings[a['allocationId']]=tuple(values)
    dependencies=None
    if 'auxiliaryInputs' in doc:
        from ..auxiliary.study import dependencies_for
        dependencies=dependencies_for(doc,instrument,snapshots,origin,end,{})
    return bindings,dependencies


def execute_group(group,make_job,period,child,pool,workers,progress,timings):
    submitted=consumed=0;ready={};pending={}
    def coordinate(c):return dict(periodId=period['periodId'],**c)
    while consumed<len(group):
        while submitted<len(group) and submitted-consumed<workers:
            c=group[submitted];progress.state.update(phase='execute',currentJob=coordinate(c));job=make_job(c)
            if pool is None:
                tick=time.perf_counter();records=run_account_job(job);timings['directExecutionSeconds']+=time.perf_counter()-tick
                require(len(records)==1,'one native account per job')
                tick=time.perf_counter();ready[submitted]=extract(records[0],c,period,child)
                timings['extractionSeconds']+=time.perf_counter()-tick;del records
            else:
                pool.submit(c['configurationId'],job);pending[c['configurationId']]=submitted
            del job;submitted+=1
            progress.state['maximumInFlight']=max(progress.state['maximumInFlight'],submitted-consumed)
        if consumed not in ready:
            kind,key,records=pool.take()
            progress.state.update(phase='extract',currentJob=coordinate(group[pending[key]]) if key in pending else None)
            require(kind=='result' and key in pending,'native worker failed')
            index=pending.pop(key);require(len(records)==1,'one native account per job')
            tick=time.perf_counter();ready[index]=extract(records[0],group[index],period,child)
            timings['extractionSeconds']+=time.perf_counter()-tick;del records
        progress.state['maximumCompactBuffer']=max(progress.state['maximumCompactBuffer'],len(ready)+16)
        while consumed in ready:
            unit=ready.pop(consumed);consumed+=1
            yield unit
            del unit;progress.state['completedResults']+=1;progress.write()


def admit_auxiliary(doc,children,roots,output,pack):
    if 'auxiliaryInputs' not in doc:
        require(roots is None,'v1 has no auxiliary roots');return None
    from ..auxiliary.storage import load_snapshot
    from ..auxiliary.study import check_binding,source_for
    roots={} if roots is None else roots
    require(type(roots) is dict and set(roots)=={b['snapshotId'] for b in doc['auxiliaryInputs']},'exact parent auxiliary-root mapping')
    snapshots={}
    for alias,value in sorted(roots.items()):
        p=safe_path(value)
        require(not p.is_relative_to(pack) and not pack.is_relative_to(p) and not p.is_relative_to(output) and not output.is_relative_to(p),'auxiliary root overlap')
        snapshots[alias]=load_snapshot(p)
    for b in doc['auxiliaryInputs']:
        require(snapshots[b['snapshotId']].contentId==b['expectedSnapshotContentId'],'snapshot content pin')
    # Actual source bindings are checked only for consuming cell targets. All
    # parent declarations, including unused-in-this-cell maps, were normalized.
    checked=set()
    for child in children:
        target=child['instruments'][0]
        for b in child['auxiliaryInputs']:
            key=(b['inputId'],target)
            if key in checked:continue
            checked.add(key);reader=snapshots[b['snapshotId']];source=source_for(b,target)
            require(source['seriesId'] in {e['seriesId'] for e in reader.manifest['series']},'auxiliary source absent')
            check_binding(b,target,read_json(reader.root/'series'/source['seriesId']/'series.json'))
    return snapshots


def run_strategy_screen(request,*,data_root,output_root,workers=1,auxiliary_roots=None):
    normalized=normalize_request(request);doc=normalized.document
    integer(workers,'workers',1);require(workers<=6,'workers must be 1..6')
    rows=configurations(doc);pack=safe_path(data_root);output=output_path(output_root,pack)
    groups={cell_id(c['instrumentId'],c['timeframeMinutes']):[r for r in rows if r['cellId']==cell_id(c['instrumentId'],c['timeframeMinutes'])] for c in doc['cells']}
    children={(p['periodId'],cell_id(c['instrumentId'],c['timeframeMinutes'])):child_request(doc,c,p) for p in doc['periods'] for c in doc['cells']}
    snapshots=admit_auxiliary(doc,children.values(),auxiliary_roots,output,pack)
    source=implementation('auxiliaryInputs' in doc)
    with data.read_session(pack) as session:
        report=session.inspect(verify=False);require(report.get('state')=='ready','pack not ready')
        entries={e['instrument_id']:e for e in report['instruments']}
        for child in children.values():
            wanted=set(child['instruments'])|{s['instrumentId'] for s in child['sources'] if s['instrumentId']!='target'}
            require(wanted<=entries.keys(),'target/source absent')
            require(all(set(entries[i]['roles'])&{'trading','research_only'} for i in child['instruments']),'factor-only target')
            origin,start,end=bounds(child)
            require(not metadata_admission_failures([entries[i] for i in sorted(wanted)],SimpleNamespace(
                warmup_start_ms=origin,study_start_ms=start,study_end_ms=end,timeframes=(5,))),'screen period metadata admission')
        raw={e['instrument_id']:e for e in manifest.read_manifest(pack)['instruments']}
        rules={c['instrumentId']:normalize_rules(raw[c['instrumentId']]) for c in doc['cells']}
        require(session.inspect(verify=True)['verification_check']['ok'],'pack integrity admission')
        root=reserve(output);writer=pool=None
        state=dict(state='running',phase='initialize',currentJob=None,plannedResults=2*len(rows),completedResults=0,
            requestedWorkers=workers,maximumInFlight=0,maximumCompactBuffer=0,startedUtc=datetime.now(timezone.utc).isoformat())
        timings=dict(preparationSeconds=0.,extractionSeconds=0.,directExecutionSeconds=0.);physical={};started=time.perf_counter()
        try:
            save_json(root/'study.json',dict(schema='pattern-lab.strategy-screen-plan.v1',request=doc,configurations=rows))
            progress=Progress(root,state);writer=Writer(root,normalized,source)
            effective=min(workers,max(len(g) for g in groups.values()));state['effectiveWorkers']=effective
            with workers_module.single_threaded_children():
                if workers>1:
                    pool=workers_module.SpawnJobPool(effective,workers_module.WorkerSettings((),(),()))
                    pool.start();state.update(workerPids=pool.worker_pids,childThreads=pool.thread_evidence)
                for period in reversed(doc['periods']):
                    for cid,group in groups.items():
                        child=children[(period['periodId'],cid)];instrument=child['instruments'][0];local_physical={}
                        state.update(phase='prepare',currentJob=dict(periodId=period['periodId'],cellId=cid))
                        tick=time.perf_counter();bindings,deps=prepare_scope(session,child,instrument,entries,snapshots,local_physical)
                        timings['preparationSeconds']+=time.perf_counter()-tick
                        amap={a['allocationId']:a for a in child['allocations']}
                        def make_job(c):
                            aid=c['allocationId']
                            return AccountJob(instrument,(amap[aid],),tuple(child['windows']),{aid:bindings[aid]},
                                rules[instrument],source['sources'],None if deps is None else {aid:deps[aid]})
                        check_generation(source['sources'])
                        for unit in execute_group(group,make_job,period,child,pool,effective,progress,timings):writer.append(unit)
                        check_generation(source['sources']);del bindings,deps
                        physical[period['periodId']+':'+cid]=local_physical
                if pool is not None:pool.shutdown(abort=False);pool=None
            state.update(phase='publish',currentJob=None);timings['elapsedSeconds']=time.perf_counter()-started
            writer.finish(dict(physicalInputs=physical,timings=timings),state,before_completion=lambda:check_generation(source['sources']))
        except BaseException as exc:
            if pool is not None:
                try:pool.shutdown(abort=True)
                except BaseException as cleanup:state['workerCleanupErrorType']=type(cleanup).__name__
            if writer is not None:
                try:writer.close()
                except BaseException as cleanup:state['writerCleanupErrorType']=type(cleanup).__name__
            if not completion_valid(root):
                state.update(state='failed',errorType=type(exc).__name__,reasonCode='strategyScreenOperationFailed')
                if not (root/'study.json').is_file():state['plan']=dict(request=doc,configurations=rows)
                try:save_json(root/'status.json',state)
                except OSError:pass
            if isinstance(exc,KeyboardInterrupt):raise KeyboardInterrupt from None
            raise PatternLabDataError('Strategy Screen failed; see sanitized owned status.') from None
    return load_strategy_screen(root)
