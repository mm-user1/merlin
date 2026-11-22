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
    study=read_json(root/'study.json');study['artifactIdentity']=artifacts.artifact_identity(study,index);write_json(root/'study.json',study)
    artifacts.seal(root)


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
