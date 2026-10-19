"""Closed sealed selections, offline reconstruction and exact-parent verification."""
from pathlib import Path
from dataclasses import fields,asdict
import os
import shutil
import stat
import tempfile
import zipfile

import numpy as np

from ..accounts.contracts import require, array, ExecutionPolicy, integer
from ..account_study.common import (closed, read_json, json_text, parse_json,
    file_hash, digest_id, member_names)
from ..account_study import verify_account_study
from ..auxiliary.storage import safe_path
from ..study.contracts import semantic_digest
from .request import normalize_selection_request
from .ledger import admit_parent, extract_ledger
from .scorers import registry
from .engine import (source_hashes, implementation, build_scores, validate_scores_saved,
    validate_ledger, data_identity, path_identity)
from .arithmetic import calculate
from .capacity import MAX_JSON, admit_results_capacity

SCHEMA='pattern-lab.universe-selection.v1'
MAX_FILE=256*1024*1024
MAX_TOTAL=1024*1024*1024
DERIVED=('derived/summary.json','derived/report.html')


def write_json(path, value):
    # Match the existing serializer and native text-mode newline, but check the
    # exact emitted UTF-8 bytes before creating either destination or temp file.
    payload=(json_text(value)+os.linesep).encode('utf-8')
    require(len(payload)<=MAX_JSON,
        f'selection JSON write bound: {Path(path).name}, {len(payload)} bytes exceeds {MAX_JSON}')
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_bytes(payload);os.replace(temporary,path)


def new_destination(value, source=None):
    p=safe_path(value)
    require(not p.exists(),'selection destination must be new')
    if source is not None:
        source=safe_path(source)
        require(not p.is_relative_to(source) and not source.is_relative_to(p),'overlapping selection input/output')
    return p


def bounded_json(path):
    require(path.is_file() and path.stat().st_size<=MAX_JSON,'JSON size bound')
    return read_json(path)


def envelope(doc):
    closed(doc,('schema','formatVersion','members'))
    require(doc['schema']==SCHEMA and type(doc['formatVersion']) is int and doc['formatVersion']==1,'selection seal schema')
    members=doc['members'];require(type(members) is dict and 0<len(members)<=10000 and 'completion.json' not in members,'seal members')
    member_names([*members,'completion.json'])
    for name,row in members.items():
        closed(row,('bytes','sha256'));digest_id(row['sha256'])
        require(type(row['bytes']) is int and 0<=row['bytes']<=MAX_FILE,'member size bound')
    require(sum(r['bytes'] for r in members.values())<=MAX_TOTAL,'selection total bound')
    return doc


def verify_directory(root):
    root=safe_path(root);require(root.is_dir(),'selection directory missing')
    seal=envelope(bounded_json(root/'completion.json'));actual=set()
    for base,dirs,files in os.walk(root,followlinks=False):
        for name in dirs+files:require(not (Path(base)/name).is_symlink(),'symlink member')
        actual.update((Path(base)/name).relative_to(root).as_posix() for name in files)
    require(actual==set(seal['members'])|{'completion.json'},'selection member set')
    for name,row in seal['members'].items():
        require((root/name).stat().st_size==row['bytes'] and file_hash(root/name)==row['sha256'],'selection member integrity: '+name)
    return seal


def seal_directory(root):
    members={p.relative_to(root).as_posix():dict(bytes=p.stat().st_size,sha256=file_hash(p))
        for p in sorted(root.rglob('*')) if p.is_file() and p.name!='completion.json'}
    write_json(root/'completion.json',envelope(dict(schema=SCHEMA,formatVersion=1,members=members)))
    verify_directory(root)


def save_arrays(path, arrays):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as stream:np.savez_compressed(stream,**arrays)


def load_arrays(path):
    require(path.stat().st_size<=MAX_FILE,'array file bound')
    with zipfile.ZipFile(path) as z:
        require(sum(i.file_size for i in z.infolist())<=MAX_FILE,'expanded array bound')
    with np.load(path,allow_pickle=False) as archive:
        require(len(archive.files)==len(set(archive.files)),'duplicate array name')
        return {k:array(archive[k],archive[k].dtype) for k in archive.files}


class Selection:
    def __init__(self,root,meta,ledger,scores,results,paths,curves):
        self.root=Path(root)
        self._documents={k:json_text(v) for k,v in dict(selection=meta,ledger=ledger,scores=scores,results=results).items()}
        self._paths=paths;self._curves=curves

    @property
    def selection(self):return parse_json(self._documents['selection'])
    @property
    def ledger(self):return parse_json(self._documents['ledger'])
    @property
    def scores(self):return parse_json(self._documents['scores'])
    @property
    def results(self):return parse_json(self._documents['results'])
    @property
    def summary(self):return bounded_json(self.root/DERIVED[0])


def assert_equal(actual,expected,label):
    # Exact same-host semantic reconstruction: no rounded scores or memberships.
    require(actual==expected,label+' differs')


def load_selection(run_root):
    root=safe_path(run_root);seal=verify_directory(root)
    meta=bounded_json(root/'selection.json')
    closed(meta,('schema','formatVersion','request','roster','allocation','identities','implementation'))
    require(meta['schema']==SCHEMA and type(meta['formatVersion']) is int and meta['formatVersion']==1,'selection schema')
    request=normalize_selection_request(meta['request'],stored=True);doc=request.document
    require(doc==meta['request'],'normalized selection request')
    ledger=bounded_json(root/'ledger.json');scores=bounded_json(root/'scores.json');results=bounded_json(root/'results.json')
    paths={w['month']:load_arrays(root/'paths'/(w['month']+'.npz')) for w in doc['monthWindows']}
    for path in paths.values():require(set(path)=={'times','pnl'},'monthly path members')
    validate_ledger(doc,meta['roster'],ledger,paths);validate_scores_saved(doc,ledger,meta['roster'],scores)
    closed(meta['identities'],('specification','dataInput','implementation'))
    closed(meta['allocation'],('allocationId','timeframeMinutes','features','provider','entryFilter','policy'),('auxiliaryFeatures',))
    integer(meta['allocation']['timeframeMinutes'],'allocation timeframe',1)
    closed(meta['allocation']['policy'],[f.name for f in fields(ExecutionPolicy)])
    require(asdict(ExecutionPolicy(**meta['allocation']['policy']))==meta['allocation']['policy'],'normalized allocation policy')
    require(all(r['stepMs']==meta['allocation']['timeframeMinutes']*60000 for r in ledger),'allocation ledger timeframe')
    require(meta['allocation']['allocationId']==doc['allocationId'] and meta['allocation']['policy']['initialCapital']==doc['referenceCapital'],'saved allocation binding')
    require(meta['identities']==dict(specification=request.identity,dataInput=data_identity(doc,ledger,paths,scores,meta['allocation']),
        implementation=semantic_digest(meta['implementation'])),'selection identities')
    closed(meta['implementation'],('algorithm','sources','experimental','python','libraries'))
    require(meta['implementation']['algorithm']=='fixed-reference.1','implementation algorithm')
    require(type(meta['implementation']['experimental']) is dict and type(meta['implementation']['python']) is str,'implementation metadata')
    closed(meta['implementation']['libraries'],('numpy','pandas','pyarrow','numba'))
    require(all(type(v) is str and v for v in meta['implementation']['libraries'].values()),'library versions')
    for sources in [meta['implementation']['sources'],*meta['implementation']['experimental'].values()]:
        require(type(sources) is dict and sources,'source attribution');member_names(sources)
        for h in sources.values():digest_id(h)
    require(set(meta['implementation']['experimental'])=={s['scorerId'] for s in doc['selectors'] if s['scorerId'].startswith('x.')},'experimental attribution roster')
    actual,curves=calculate(doc,ledger,paths,meta['roster'],scores)
    assert_equal(results,actual,'portfolio facts')
    saved=load_arrays(root/'curves.npz')
    require(set(saved)=={identity+'.'+field for identity in curves for field in ('times','values')},'curve array members')
    for identity,value in curves.items():
        for field,a in value.items():require(np.array_equal(saved[identity+'.'+field],a),'derived path differs: '+identity+'.'+field)
    status=bounded_json(root/'status.json')
    require(status==dict(state='complete',ledgerRows=len(ledger),scoreBatches=len(scores),comparisons=len(results['comparisons'])),'selection status counts')
    provenance=bounded_json(root/'provenance.json');closed(provenance,('parentRoot',))
    require(type(provenance['parentRoot']) is str,'parent provenance')
    expected={'selection.json','ledger.json','scores.json','results.json','curves.npz','status.json','provenance.json',*DERIVED,
        *('paths/'+m+'.npz' for m in paths)}
    require(set(seal['members'])==expected,'unexpected selection member')
    reader=Selection(root,meta,ledger,scores,results,paths,curves)
    from .report import summary
    assert_equal(reader.summary,summary(reader),'saved summary')
    return reader


def write_derived(reader,destination):
    from .report import summary,render
    value=summary(reader);write_json(destination/DERIVED[0],value)
    (destination/DERIVED[1]).write_text(render(value),encoding='utf-8')


def run_selection(request, *, parent_root, output_root, scorers=None):
    registered=registry(scorers);before=source_hashes(registered)
    normalized=normalize_selection_request(request,scorers=scorers);doc=normalized.document
    used={s['scorerId'] for s in doc['selectors'] if s['scorerId'].startswith('x.')}
    before={k:v for k,v in before.items() if k in used}
    parent_root=safe_path(parent_root);target=new_destination(output_root,parent_root)
    parent=admit_parent(doc,parent_root)
    admit_results_capacity(doc,parent.study['request']['instruments'])
    ledger,paths,roster=extract_ledger(parent,doc);validate_ledger(doc,roster,ledger,paths)
    target.mkdir(parents=True)
    try:
        scores=build_scores(parent,doc,ledger,roster,registered)
        validate_scores_saved(doc,ledger,roster,scores)
        require(before=={k:v for k,v in source_hashes(registered).items() if k in used},'experimental source changed during run')
        attribution=implementation(before)
        allocation=next(a for a in parent.study['request']['allocations'] if a['allocationId']==doc['allocationId'])
        meta=dict(schema=SCHEMA,formatVersion=1,request=doc,roster=roster,allocation=allocation,implementation=attribution,
            identities=dict(specification=normalized.identity,dataInput=data_identity(doc,ledger,paths,scores,allocation),implementation=semantic_digest(attribution)))
        results,curves=calculate(doc,ledger,paths,roster,scores)
        for name,value in (('selection',meta),('ledger',ledger),('scores',scores),('results',results),('provenance',dict(parentRoot=str(parent_root)))):
            write_json(target/(name+'.json'),value)
        for month,path in paths.items():save_arrays(target/'paths'/(month+'.npz'),path)
        save_arrays(target/'curves.npz',{identity+'.'+field:a for identity,p in curves.items() for field,a in p.items()})
        reader=Selection(target,meta,ledger,scores,results,paths,curves);write_derived(reader,target)
        write_json(target/'status.json',dict(state='complete',ledgerRows=len(ledger),scoreBatches=len(scores),comparisons=len(results['comparisons'])))
        seal_directory(target)
        return load_selection(target)
    except BaseException as exc:
        if (target/'completion.json').exists():(target/'completion.json').unlink()
        write_json(target/'status.json',dict(state='failed',errorType=type(exc).__name__,error=str(exc)))
        raise


def verify_selection(run_root, *, parent_root, scorers=None):
    reader=load_selection(run_root);meta=reader.selection;doc=meta['request']
    parent=admit_parent(doc,safe_path(parent_root));parent_result=verify_account_study(parent.root)
    require(meta['allocation']==next(a for a in parent.study['request']['allocations'] if a['allocationId']==doc['allocationId']),'parent allocation metadata differs')
    registered=registry(scorers);provided=source_hashes(registered)
    for name,hashes in provided.items():
        require(name in meta['implementation']['experimental'] and hashes==meta['implementation']['experimental'][name],'experimental source mismatch')
    # Missing experimental implementations are deliberately not normalized/imported.
    for s in doc['selectors']:
        if s['scorerId'] in registered:
            require(registered[s['scorerId']].version==s['version'],'deep scorer version mismatch')
            actual=registered[s['scorerId']].normalizeParameters(parse_json(json_text(s['parameters'])))
            require(actual==s['parameters'],'deep scorer normalized parameters differ')
    ledger,paths,roster=extract_ledger(parent,doc)
    assert_equal(ledger,reader.ledger,'parent ledger');require(roster==meta['roster'] and path_identity(paths)==path_identity(reader._paths),'parent paths')
    scores=build_scores(parent,doc,ledger,roster,registered,stored=reader.scores)
    assert_equal(scores,reader.scores,'deep score outputs/contracts/dependencies')
    require(provided==source_hashes(registered),'experimental sources changed during verification')
    results,curves=calculate(doc,ledger,paths,roster,scores);assert_equal(results,reader.results,'deep portfolio facts')
    return dict(status='verified',level='exactParent',parent=parent_result,ledgerRows=len(ledger),
        scorers={s['selectorId']:'recomputed' if s['scorerId'] in registered else 'storedOutputsOnly' for s in doc['selectors']})


def rebuild_selection_report(run_root, *, output_root):
    reader=load_selection(run_root);target=new_destination(output_root,run_root);target.mkdir(parents=True)
    write_derived(reader,target)
    return dict(status='complete',outputRoot=str(target),files=list(DERIVED),experimentalVerification='storedOutputsOnly')


def export_selection_bundle(run_root, *, zip_path):
    reader=load_selection(run_root);seal=verify_directory(reader.root);target=new_destination(zip_path,reader.root)
    target.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(target,'x',zipfile.ZIP_DEFLATED) as archive:
        for name in sorted([*seal['members'],'completion.json']):archive.write(reader.root/name,name)
    return dict(path=str(target),bytes=target.stat().st_size,sha256=file_hash(target))


def import_selection_bundle(zip_path, *, output_root):
    archive_path=safe_path(zip_path);target=new_destination(output_root,archive_path)
    target.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        infos=archive.infolist();names=[i.filename for i in infos]
        require(len(names)==len(set(names)) and names.count('completion.json')==1,'duplicate/missing ZIP member')
        member_names(names)
        for info in infos:
            require(not info.is_dir() and stat.S_IFMT(info.external_attr>>16) in (0,stat.S_IFREG),'non-file ZIP member')
        require(archive.getinfo('completion.json').file_size<=MAX_JSON,'ZIP completion bound')
        raw=archive.read('completion.json');seal=envelope(parse_json(raw.decode('utf-8')))
        require(set(names)==set(seal['members'])|{'completion.json'},'ZIP member set')
        for name,row in seal['members'].items():require(archive.getinfo(name).file_size==row['bytes'],'ZIP declared size')
        require(shutil.disk_usage(target.parent).free>=sum(r['bytes'] for r in seal['members'].values())+len(raw),'import free space')
        stage=Path(tempfile.mkdtemp(prefix='.selection-import-',dir=target.parent))
        try:
            (stage/'completion.json').write_bytes(raw)
            for name,row in seal['members'].items():
                path=stage/name;require(path.resolve().is_relative_to(stage.resolve()),'import containment')
                path.parent.mkdir(parents=True,exist_ok=True);remaining=row['bytes']
                with archive.open(name) as source,path.open('xb') as dest:
                    while remaining:
                        data=source.read(min(1024*1024,remaining));require(data,'truncated ZIP')
                        dest.write(data);remaining-=len(data)
                    require(not source.read(1),'oversized ZIP')
                require(file_hash(path)==row['sha256'],'ZIP payload hash')
            load_selection(stage);require(not target.exists(),'destination appeared')
            stage.rename(target)
        finally:
            if stage.exists():
                require(stage.resolve().parent==target.parent.resolve() and stage.name.startswith('.selection-import-'),'staging cleanup containment')
                shutil.rmtree(stage)
    return load_selection(target)
