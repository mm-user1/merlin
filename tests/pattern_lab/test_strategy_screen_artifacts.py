from pathlib import Path
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.strategy_screen import load_strategy_screen,verify_strategy_screen,analyze_strategy_screen
from ._strategy_screen_helpers import run,results,compact
from tools.pattern_lab.strategy_screen import artifacts
from tools.pattern_lab.account_study.common import read_json,write_json


def rewrite(root,mutate):
    import zipfile
    from tools.pattern_lab.configuration_study import codec
    from tools.pattern_lab.account_study.containers import zip_info
    index=read_json(root/'index.json');contents=[]
    with zipfile.ZipFile(root/'results.zip') as archive:
        for e in index['members']:contents.append(codec.decode(archive.read(e['name']),e))
    mutate(contents)
    with zipfile.ZipFile(root/'results.zip','w',compression=zipfile.ZIP_STORED,allowZip64=False) as archive:
        for e,value in zip(index['members'],contents):
            raw,info=codec.encode(value);e.update(info);archive.writestr(zip_info(e['name']),raw)
    index['totalDecodedBytes']=sum(e['decodedBytes'] for e in index['members']);write_json(root/'index.json',index)
    study=read_json(root/'study.json')
    study['identities']['dataInput']=artifacts.semantic_digest([
        (row['facts']['resultId'],artifacts.semantic_digest(row['facts']['execution']))
        for entry,block in zip(index['members'],contents) if entry['kind']=='results' for row in block['records']])
    study['artifactIdentity']=artifacts.artifact_identity(study,index);write_json(root/'study.json',study)
    artifacts.seal(root)


def tree_inventory(root):
    return {p.relative_to(root).as_posix():p.read_bytes() if p.is_file() else None for p in root.rglob('*')}


def test_both_public_outputs_preserve_other_completed_native_screen(tmp_path):
    from tools.pattern_lab.strategy_screen import runner
    from ._strategy_screen_helpers import request
    a=run(tmp_path/'a');b=run(tmp_path/'b');before=tree_inventory(b.root)
    with pytest.raises(PatternLabDataError):analyze_strategy_screen(a.root,output_root=b.root/'analysis')
    with pytest.raises(PatternLabDataError):
        runner.run_strategy_screen(request(),data_root=tmp_path/'a/pack',output_root=b.root/'missing/nested/run')
    assert tree_inventory(b.root)==before and verify_strategy_screen(b.root)['ok']
    analyze_strategy_screen(a.root,output_root=tmp_path/'sibling-analysis')
    assert runner.run_strategy_screen(request(),data_root=tmp_path/'a/pack',output_root=tmp_path/'sibling-run')


@pytest.mark.parametrize('marker',['completion.json','study.json','selection.json','status.json','analysisPair','malformedDirectory'])
def test_both_public_outputs_refuse_owned_markers_before_mkdir(tmp_path,marker):
    from tools.pattern_lab.strategy_screen import runner
    from ._strategy_screen_helpers import request
    a=compact(tmp_path/'a');b=tmp_path/'b';b.mkdir()
    if marker=='malformedDirectory':(b/'study.json').mkdir()
    elif marker=='analysisPair':
        (b/'analysis.json').write_text('bad');(b/'receipt.json').write_text('bad')
    elif marker=='status.json':write_json(b/marker,dict(state='failed',plan=dict(request=request(),configurations=[])))
    else:(b/marker).write_text('malformed or planned')
    before=tree_inventory(b);target=b/'missing/deeper/new'
    with pytest.raises(PatternLabDataError):analyze_strategy_screen(a.root,output_root=target)
    with pytest.raises(PatternLabDataError):runner.run_strategy_screen(request(),data_root=tmp_path/'pack',output_root=target)
    assert tree_inventory(b)==before


def test_output_guard_entry_errors_and_reparse_order(tmp_path,monkeypatch):
    from types import SimpleNamespace
    original=Path.lstat;bad=tmp_path/'reparse';target=bad/'missing/run';visited=[]
    def inspect(path,*a,**kw):
        visited.append(path)
        if path==bad:return SimpleNamespace(st_mode=0,st_file_attributes=0x400)
        assert bad not in path.parents,'traversed a reparse component'
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'lstat',inspect)
    with pytest.raises(PatternLabDataError,match='reparse'):artifacts.output_path(target)
    assert target not in visited
    def denied(path,*a,**kw):
        if path==tmp_path/'status.json':raise PermissionError('private')
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'lstat',denied)
    with pytest.raises(PermissionError):artifacts.output_path(tmp_path/'new')
    assert not (tmp_path/'new').exists()


def test_unmarked_and_half_analysis_parent_are_not_owned(tmp_path):
    (tmp_path/'analysis.json').write_text('half')
    assert artifacts.output_path(tmp_path/'new')==tmp_path/'new'


def test_dangling_marker_entry_is_owned_without_following(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import stat
    original=Path.lstat
    def inspect(path,*a,**kw):
        if path==tmp_path/'status.json':return SimpleNamespace(st_mode=stat.S_IFLNK,st_file_attributes=0x400)
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'lstat',inspect)
    with pytest.raises(PatternLabDataError,match='owned artifact'):artifacts.output_path(tmp_path/'missing/new')
    assert not (tmp_path/'missing').exists()


@pytest.fixture(scope='module')
def valid_native_screen(tmp_path_factory):
    return run(tmp_path_factory.mktemp('screen-sealed')/'native')


@pytest.mark.parametrize('mutation,reason',[('attribution','native account identity'),('fingerprint','native preparation identity'),
    ('terminal','terminal month ownership'),('dd','carried DD below observed endpoint')])
def test_independently_resealed_native_inner_checks(tmp_path,valid_native_screen,mutation,reason):
    import shutil
    from copy import deepcopy
    reader=valid_native_screen;root=tmp_path/'copy';shutil.copytree(reader.root,root)
    cid=next(c['configurationId'] for c in reader.configurations if c['allocationId']=='rr4')
    other=next(c['configurationId'] for c in reader.configurations if c['allocationId']=='rr3')
    def change(blocks):
        u=next(row['facts'] for block in blocks for row in block['records']
            if 'facts' in row and row['facts']['configurationId']==cid and row['facts']['periodId']=='recent')
        if mutation=='attribution':u['execution']=deepcopy(reader.result(other,'recent')['execution'])
        elif mutation=='fingerprint':u['execution']['inputs'][0]['inputFingerprint']='0'*64
        elif mutation=='dd':u['months'][1]['carriedPeakMaxDdPct']=0.
        else:
            for key in ('terminalTradeCount','terminalExitFees'):u['months'][0][key],u['months'][-1][key]=u['months'][-1][key],0
    rewrite(root,change)
    assert artifacts.completion_valid(root),'negative case must have a valid outer seal'
    for check in (load_strategy_screen,verify_strategy_screen):
        with pytest.raises(PatternLabDataError,match=reason):check(root)


def test_writer_rejects_wrong_account_before_publication(tmp_path,valid_native_screen):
    reader=valid_native_screen;root=tmp_path/'writer';root.mkdir()
    other=next(c['configurationId'] for c in reader.configurations if c['allocationId']=='rr3')
    cid=next(c['configurationId'] for c in reader.configurations if c['allocationId']=='rr4')
    writer=artifacts.Writer(root,reader.metadata['request'],reader.metadata['implementation'])
    try:
        for row in reader.result_rows:
            u=reader.result(row['configurationId'],row['periodId']);u.pop('classification');u.pop('analytics')
            if row['configurationId']==cid:
                u['execution']=reader.result(other,row['periodId'])['execution']
                with pytest.raises(PatternLabDataError,match='native account identity'):writer.append(u)
                break
            writer.append(u)
        else:pytest.fail('wrong attribution was not exercised')
    finally:writer.close()
    assert not (root/'completion.json').exists()


def test_native_roundtrip_and_deterministic_analysis(tmp_path):
    reader=run(tmp_path/'native')
    assert verify_strategy_screen(reader.root)['ok']
    assert results(reader)==results(load_strategy_screen(reader.root))
    assert set(p.name for p in reader.root.iterdir())=={'study.json','index.json','results.zip','provenance.json','status.json','completion.json'}
    analyze_strategy_screen(reader.root,output_root=tmp_path/'analysis1')
    analyze_strategy_screen(reader.root,output_root=tmp_path/'analysis2')
    for name in ('analysis.json','receipt.json'):
        assert (tmp_path/'analysis1'/name).read_bytes()==(tmp_path/'analysis2'/name).read_bytes()
    metadata=reader.metadata;metadata['counts'].clear()
    assert reader.metadata['counts']
    with (reader.root/'results.zip').open('ab') as stream:stream.write(b'bad')
    with pytest.raises(PatternLabDataError):load_strategy_screen(reader.root)


@pytest.mark.parametrize('mutation',[
    lambda u:u['months'][0].update(contributionPp=999),
    lambda u:u['months'][0].update(closedTrades=999),
    lambda u:u['months'][0].update(month='2099-01'),
    lambda u:u['months'][0].update(terminalExitFees=5),
    lambda u:u['months'][0].update(carriedPeakMaxDdPct=999),
    lambda u:u.update(resultId='0'*64),
    lambda u:u['audit'].update(tradesChecked=True),
])
def test_resealed_fact_arithmetic_not_just_hashes(tmp_path,mutation):
    reader=compact(tmp_path/'compact')
    rewrite(reader.root,lambda blocks:mutation(blocks[0]['records'][0]['facts']))
    with pytest.raises(PatternLabDataError):verify_strategy_screen(reader.root)


def test_verify_rederives_resealed_summary(tmp_path):
    reader=compact(tmp_path/'compact')
    rewrite(reader.root,lambda blocks:blocks[-1]['records'][0]['periods']['recent']['metrics']['returnPct'].update(median=123456))
    load_strategy_screen(reader.root)  # Structural load is distinct from numerical rederivation.
    with pytest.raises(PatternLabDataError,match='numerical summary'):verify_strategy_screen(reader.root)


@pytest.mark.parametrize('sealed',[True,False])
@pytest.mark.parametrize('protocol',[True,False])
def test_stored_path_rejected_before_normalization(tmp_path,monkeypatch,sealed,protocol):
    reader=compact(tmp_path/'compact');study=read_json(reader.root/'study.json')
    if protocol:study['request']['periods'][0]['protocol']='C:/private.json'
    else:study['request']='C:/private.json'
    write_json(reader.root/'study.json',study);artifacts.seal(reader.root)
    if not sealed:(reader.root/'completion.json').unlink()
    monkeypatch.setattr(artifacts,'normalize_request',lambda *a:pytest.fail('stored path reached normalizer'))
    with pytest.raises(PatternLabDataError,match='must be objects'):artifacts._load(reader.root,sealed=sealed)


def test_stored_only_relocation_no_external_reads_and_index_reuse(tmp_path,monkeypatch):
    import shutil
    import socket
    from tools.pattern_lab.strategy_screen import runner
    from tools.pattern_lab import data
    from tools.pattern_lab.auxiliary import storage
    from tools.pattern_lab.configuration_study import codec
    reader=compact(tmp_path/'compact');relocated=tmp_path/'relocated';shutil.copytree(reader.root,relocated)
    def refuse(*a,**kw):pytest.fail('external read/execution during stored-only operation')
    monkeypatch.setattr(data,'read_session',refuse);monkeypatch.setattr(storage,'load_snapshot',refuse)
    monkeypatch.setattr(runner,'run_account_job',refuse);monkeypatch.setattr(runner,'prepare_scope',refuse)
    monkeypatch.setattr(runner,'check_generation',refuse);monkeypatch.setattr(socket,'create_connection',refuse)
    loaded=load_strategy_screen(relocated)
    monkeypatch.setattr(codec.zipfile,'ZipFile',refuse)
    for row in loaded.result_rows:loaded.months(row['configurationId'],row['periodId'])
    for s in loaded.scopes:loaded.summary(s['scopeId'])
    monkeypatch.undo()
    assert verify_strategy_screen(relocated)['artifactIdentity']==verify_strategy_screen(reader.root)['artifactIdentity']
    with pytest.raises(PatternLabDataError):analyze_strategy_screen(relocated,output_root=relocated/'overlap')


def test_byte_bounds_extra_files_and_missing_completion(tmp_path,monkeypatch):
    reader=compact(tmp_path/'compact');(reader.root/'extra').write_text('extra')
    with pytest.raises(PatternLabDataError,match='exact screen root'):load_strategy_screen(reader.root)
    (reader.root/'extra').unlink();(reader.root/'completion.json').unlink()
    with pytest.raises(PatternLabDataError):load_strategy_screen(reader.root)
    artifacts.seal(reader.root);monkeypatch.setattr(artifacts.codec,'MAX_TOTAL',10)
    with pytest.raises(PatternLabDataError,match='aggregate'):load_strategy_screen(reader.root)
