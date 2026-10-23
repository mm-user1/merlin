"""Physical/logical container admission, streaming failures and bounded metadata reuse."""
from pathlib import Path
import json
import struct
import zipfile
from copy import deepcopy

import pytest

from tools.pattern_lab import PatternLabDataError as Error
from tools.pattern_lab.account_study import (run_account_study, load_account_study,
    verify_account_study, export_account_bundle, import_account_bundle)
from tools.pattern_lab.account_study import codec, containers as storage
from tools.pattern_lab.account_study.common import read_json, write_json, file_hash
from ._account_study_helpers import build_pack, request, reseal


@pytest.fixture
def retained(tmp_path):
    ids = build_pack(tmp_path/'pack', count=2, gaps=True)
    return run_account_study(request(ids), data_root=tmp_path/'pack', output_root=tmp_path/'run')


def rewrite(path, mutate):
    with zipfile.ZipFile(path) as archive:
        payload = [(i.filename, archive.read(i)) for i in archive.infolist()]
    with zipfile.ZipFile(path, 'w') as archive:
        for name, value in mutate(payload):
            archive.writestr(storage.zip_info(name), value)


def test_streamed_layout_equivalence_and_owned_reads(retained, tmp_path, monkeypatch):
    old = run_account_study(retained.study['request'], data_root=tmp_path/'pack',
                            output_root=tmp_path/'old', _legacy_directory=True)
    assert retained.study['formatVersion'] == 3 and old.study['formatVersion'] == 1
    assert retained.study['identities'] == old.study['identities']
    assert retained.summary == old.summary
    for key in retained.accountKeys:
        assert codec.equal_records(retained.account(key), old.account(key))
    assert all(p.suffix == '.zip' for p in (retained.root/'accounts').iterdir())
    for name in retained._locator.groups:
        with zipfile.ZipFile(retained.root/name) as z:
            assert z.namelist() == sorted(z.namelist())
            for info in z.infolist():
                assert info.date_time == (1980,1,1,0,0,0) and info.compress_type == zipfile.ZIP_STORED
                assert not info.flag_bits & 9
                key, filename = info.filename.split('/')[1:]
                assert z.read(info) == (old.root/'accounts'/key/filename).read_bytes()
    parses = []; original = storage.Container.inspect.__func__
    def inspect(cls, path, keys):
        parses.append(str(path)); return original(cls, path, keys)
    monkeypatch.setattr(storage.Container, 'inspect', classmethod(inspect))
    reader = load_account_study(retained.root)
    assert len(parses) == 2
    def denied(*a, **kw): raise AssertionError('account read attempted extraction or writing')
    monkeypatch.setattr(zipfile.ZipFile, 'extract', denied)
    monkeypatch.setattr(zipfile.ZipFile, 'extractall', denied)
    original_open = Path.open
    def readonly(self, mode='r', *a, **kw):
        assert not any(c in mode for c in 'wax+'), 'account read attempted scratch write'
        return original_open(self, mode, *a, **kw)
    monkeypatch.setattr(Path, 'open', readonly)
    for key in list(reader.accountKeys) + list(reversed(reader.accountKeys)):
        record = reader.account(key)
        assert len(record.result.path) > 0
    assert len(parses) == 2
    # Root+key path parses just its relevant container once, not the whole parent.
    codec.decode_account(reader.root, reader.accountKeys[0])
    assert len(parses) == 3


def test_export_stores_inner_bytes_and_import_keeps_layout(retained, tmp_path):
    export_account_bundle(retained.root, zip_path=tmp_path/'bundle.zip')
    with zipfile.ZipFile(tmp_path/'bundle.zip') as archive:
        for name in retained._locator.groups:
            assert archive.getinfo(name).compress_type == zipfile.ZIP_STORED
            assert archive.read(name) == (retained.root/name).read_bytes()
    restored = import_account_bundle(tmp_path/'bundle.zip', output_root=tmp_path/'restored')
    for name in retained._locator.groups:
        assert file_hash(retained.root/name) == file_hash(restored.root/name)
    assert verify_account_study(restored.root)['status'] == 'verified'


def test_container_full_validation_distinct_from_structural_load(retained):
    assert verify_account_study(retained.root)['status'] == 'verified'
    key = retained.accountKeys[0]
    path = retained.root / retained._locator.paths[key]
    member = 'accounts/' + key + '/account.json'
    original_balance = retained.account(key).result.finalBalance
    def mutate(items):
        for name, value in items:
            if name == member:
                meta = json.loads(value)
                meta['result']['finalBalance'] += 123
                value = json.dumps(meta).encode('utf-8')
            yield name, value
    rewrite(path, mutate)
    reseal(retained.root)
    loaded = load_account_study(retained.root)
    assert loaded.study['formatVersion'] == 3 and loaded.accountKeys == retained.accountKeys
    assert loaded.account(key).result.finalBalance == original_balance + 123
    with pytest.raises(Error, match='accumulated money mismatch'):
        verify_account_study(retained.root)


@pytest.mark.parametrize('case, message', [('missingIndex','account-index'), ('wrongTicker','index differs'),
    ('wrongKeys','index differs'), ('missingContainer','member set differs'), ('swapped','logical member set'),
    ('extra','member count'), ('missing','member count'), ('unsafe','unsafe member path'),
    ('duplicate','collision'), ('nonfile','non-file'), ('dosDirectory','non-file'),
    ('dosVolume','non-file'), ('compressed','compression'),
    ('badRecord','account record identity')])
def test_physical_and_logical_refusals(retained, case, message):
    root = retained.root; path = root/next(iter(retained._locator.groups))
    if case == 'missingIndex':
        (root/'account-index.json').unlink()
        # Root+key cannot fall back, even if a directory account is present.
        with pytest.raises(FileNotFoundError, match=message): codec.decode_account(root,retained.accountKeys[0])
        return
    if case in ('wrongTicker','wrongKeys'):
        index = read_json(root/'account-index.json'); row = index['containers'][0]
        if case == 'wrongTicker': row['instrumentId'] = 'OTHER'
        else: row['accountKeys'][0] = '0'*64
        write_json(root/'account-index.json',index)
        # Test root-only dispatch independently of physical hashing.
        with pytest.raises(Error,match=message): codec.decode_account(root,retained.accountKeys[0])
        return
    if case == 'missingContainer':
        path.unlink()
        with pytest.raises(Error,match=message): load_account_study(root)
        return
    if case == 'swapped':
        other = root/list(retained._locator.groups)[1]; path.write_bytes(other.read_bytes())
    else:
        def mutate(items):
            if case == 'extra': return items + [('extra',b'')]
            if case == 'missing': return items[1:]
            if case == 'unsafe': return [('../escape', items[0][1]), *items[1:]]
            if case == 'duplicate': return [items[1], *items[1:]]
            if case == 'badRecord':
                return [(n, b.replace(b'pattern-lab.account-record.v1',b'pattern-lab.account-record.v9')) for n,b in items]
            return items
        if case == 'duplicate':
            with pytest.warns(UserWarning): rewrite(path,mutate)
        else: rewrite(path,mutate)
        if case in ('nonfile','dosDirectory','dosVolume','compressed'):
            raw = bytearray(path.read_bytes()); central = raw.index(b'PK\x01\x02')
            if case == 'nonfile': struct.pack_into('<L',raw,central+38,0o120777<<16)
            elif case in ('dosDirectory','dosVolume'): struct.pack_into('<L',raw,central+38,16 if case=='dosDirectory' else 8)
            else: struct.pack_into('<H',raw,central+10,8)
            path.write_bytes(raw)
    reseal(root)
    with pytest.raises(Error,match=message): load_account_study(root)


@pytest.mark.parametrize('case,message', [('descriptor','flags/method'), ('localName','local header name'),
    ('localSize','CRC/size'), ('localCrc','CRC/size'), ('payload','member CRC'), ('overlap','extent')])
def test_local_headers_and_crc(case,message,tmp_path):
    key='0'*64; path=tmp_path/'test.zip'
    with zipfile.ZipFile(path,'w') as z:
        for name in storage.names_for([key]):
            with z.open(storage.zip_info(name),'w',force_zip64=True) as out: out.write(b'payload')
    raw=bytearray(path.read_bytes()); central=raw.index(b'PK\x01\x02')
    if case=='descriptor':struct.pack_into('<H',raw,6,8)
    elif case=='localName':raw[30]=ord('b')
    elif case=='localSize':struct.pack_into('<Q',raw,30+len(storage.names_for([key])[0])+4,8)
    elif case=='localCrc':struct.pack_into('<L',raw,14,0)
    elif case=='payload':raw[30+len(storage.names_for([key])[0])+20]^=1
    elif case=='overlap':
        second=raw.index(b'PK\x01\x02',central+4);struct.pack_into('<L',raw,second+42,1)
    path.write_bytes(raw)
    with pytest.raises(Error,match=message):
        c=storage.Container.inspect(path,[key])
        with c.read(storage.names_for([key])[0]):pass


def local_size_fixture(path, form, values=None, ordinary_delta=0, duplicate=False):
    """Explicit header forms, independent of the host's force_zip64 behavior."""
    key = '0' * 64
    size = 7
    defaults = dict(ordinary=b'', redundant=struct.pack('<QQ', size, size),
                    sentinels=struct.pack('<QQ', size, size),
                    sizeOnly=struct.pack('<Q', size), packedOnly=struct.pack('<Q', size))
    extra = defaults[form] if values is None else values
    with zipfile.ZipFile(path, 'w') as archive:
        for index, name in enumerate(storage.names_for([key])):
            info = storage.zip_info(name)
            if form != 'ordinary' or values is not None:
                info.extra = struct.pack('<HH', 1, len(extra)) + extra
                if duplicate and index == 0: info.extra *= 2
            archive.writestr(info, b'payload')
    raw = bytearray(path.read_bytes())
    with zipfile.ZipFile(path) as archive:
        offsets = [info.header_offset for info in archive.infolist()]
    for offset in offsets:
        struct.pack_into('<L', raw, offset + 18, 0xffffffff if form in ('sentinels', 'packedOnly') else size + ordinary_delta)
        struct.pack_into('<L', raw, offset + 22, 0xffffffff if form in ('sentinels', 'sizeOnly') else size)
    path.write_bytes(raw)
    return key


@pytest.mark.parametrize('form', ['ordinary', 'sentinels', 'redundant', 'sizeOnly', 'packedOnly'])
def test_explicit_local_size_forms(tmp_path, form):
    path = tmp_path / 'sizes.zip'
    key = local_size_fixture(path, form)
    container = storage.Container.inspect(path, [key])
    with container.read(storage.names_for([key])[0]) as stream:
        assert stream.read() == b'payload'


@pytest.mark.parametrize('form', ['sentinels', 'redundant'])
@pytest.mark.parametrize('field', ['uncompressed', 'compressed'])
def test_explicit_zip64_mismatch(tmp_path, form, field):
    path = tmp_path / 'sizes.zip'
    values = (8, 7) if field == 'uncompressed' else (7, 8)
    key = local_size_fixture(path, form, struct.pack('<QQ', *values))
    with pytest.raises(Error, match='local header CRC/size'):
        storage.Container.inspect(path, [key])


@pytest.mark.parametrize('form,values', [('sentinels', b''), ('sentinels', b'\0' * 8),
    ('sentinels', b'\0' * 15), ('sizeOnly', b''), ('packedOnly', b'\0' * 7),
    ('redundant', b'\0' * 8), ('redundant', b'\0' * 15)])
def test_missing_or_truncated_zip64_sizes(tmp_path, form, values):
    path = tmp_path / 'sizes.zip'
    key = local_size_fixture(path, form, values)
    with pytest.raises(Error, match='ZIP64 local size'):
        storage.Container.inspect(path, [key])


@pytest.mark.parametrize('case', ['missingTag', 'duplicateTag', 'ordinarySize', 'redundantOrdinarySize'])
def test_zip64_tag_and_ordinary_size_consistency(tmp_path, case):
    path = tmp_path / 'sizes.zip'
    key = local_size_fixture(path, 'redundant' if case in ('duplicateTag', 'redundantOrdinarySize') else 'ordinary',
                             ordinary_delta=int(case in ('ordinarySize', 'redundantOrdinarySize')),
                             duplicate=case == 'duplicateTag')
    if case == 'missingTag':
        raw = bytearray(path.read_bytes()); struct.pack_into('<L', raw, 22, 0xffffffff); path.write_bytes(raw)
    message = 'local extra' if case == 'duplicateTag' else 'ZIP64 local size' if case == 'missingTag' else 'CRC/size'
    with pytest.raises(Error, match=message):
        storage.Container.inspect(path, [key])


@pytest.mark.parametrize('phase', ['write','roundtrip','seal'])
def test_failed_publication_closes_and_never_seals(tmp_path,monkeypatch,phase):
    ids=build_pack(tmp_path/'pack',count=1)
    def fail(*a,**kw):raise OSError('injected '+phase)
    if phase=='write':monkeypatch.setattr(codec.pq,'write_table',fail)
    elif phase=='roundtrip':monkeypatch.setattr(codec,'equal_records',fail)
    else:monkeypatch.setattr('tools.pattern_lab.account_study.runner.seal_directory',fail)
    with pytest.raises(OSError,match='injected'):
        run_account_study(request(ids),data_root=tmp_path/'pack',output_root=tmp_path/'run')
    partial=load_account_study(tmp_path/'run',allow_partial=True)
    assert partial.status['state']=='failed' and not partial.accountKeys
    assert not (partial.root/'completion.json').exists()
    assert not list((partial.root/'accounts').glob('*.tmp'))
    if phase!='seal':assert not list((partial.root/'accounts').glob('*.zip'))
    # Windows cannot rename/delete an open container. No library handle survives failure.
    partial.root.rename(tmp_path/'moved')


def test_explicit_dispatch_bounds_and_no_overwrite(retained,tmp_path,monkeypatch):
    root=retained.root;key=retained.accountKeys[0];record=retained.account(key)
    before={p.name:file_hash(p) for p in (root/'accounts').iterdir()}
    with pytest.raises(Error,match='destination exists'):codec.encode_container(root,[record])
    assert before=={p.name:file_hash(p) for p in (root/'accounts').iterdir()}
    study=read_json(root/'study.json');study['formatVersion']=2;write_json(root/'study.json',study)
    with pytest.raises(Error,match='schema/version'):codec.decode_account(root,key)
    with pytest.raises(Error,match='bool'):
        run_account_study(retained.study['request'],data_root=tmp_path/'pack',output_root=tmp_path/'bad',_legacy_directory=1)
    old=run_account_study(retained.study['request'],data_root=tmp_path/'pack',output_root=tmp_path/'old',_legacy_directory=True)
    seal=read_json(old.root/'completion.json');seal['formatVersion']=True;write_json(old.root/'completion.json',seal)
    with pytest.raises(Error,match='schema/version'):codec.decode_account(old.root,old.accountKeys[0])
    monkeypatch.setattr(storage,'MAX_MEMBERS',1)
    with pytest.raises(Error,match='planned logical member limit'):
        run_account_study(retained.study['request'],data_root=tmp_path/'pack',output_root=tmp_path/'limit')
    assert not (tmp_path/'limit').exists()
    monkeypatch.setattr(storage,'MAX_JSON',1)
    with pytest.raises(Error,match='JSON exceeds bound'):storage.bounded_json(root/'study.json')


@pytest.mark.parametrize('v2',[False,True])
def test_directory_and_container_selection_pins_and_funded_replay(tmp_path,v2):
    from tools.pattern_lab.examples.selection_synthetic import build_inputs, selection_request
    from tools.pattern_lab.selection import run_selection, verify_selection
    raw=build_inputs(tmp_path/'pack');raw['instruments']=raw['instruments'][:2]
    if v2:
        raw.update(schema='pattern-lab.account-study-request.v2',auxiliaryInputs=[])
        for a in raw['allocations']:a['auxiliaryFeatures']=[]
    outputs={}
    for legacy in (True,False):
        parent=run_account_study(raw,data_root=tmp_path/'pack',output_root=tmp_path/str(legacy),
                                 auxiliary_roots={} if v2 else None,_legacy_directory=legacy)
        template=selection_request(parent);template['evaluationMonths']=template['evaluationMonths'][:1]
        template['selectors']=template['selectors'][:1];template['randomControl']['replicates']=1;template['kValues']=[1]
        for mode in ('fixedReference','carried'):
            doc=deepcopy(template)
            doc.update(schema='pattern-lab.selection-request.v2',capitalMode=mode,
                       initialCapital=10000. if mode=='carried' else None,auxiliaryInputs=[])
            selected=run_selection(doc,parent_root=parent.root,output_root=tmp_path/(str(legacy)+mode),workers=2)
            assert verify_selection(selected.root,parent_root=parent.root,workers=2)['status']=='verified'
            outputs[legacy,mode]=(parent,selected)
    for mode in ('fixedReference','carried'):
        old,a=outputs[True,mode];new,b=outputs[False,mode]
        assert old.study['identities']==new.study['identities']
        assert a.results==b.results and a.scores==b.scores
        assert a.selection['identities']['specification']!=b.selection['identities']['specification']
        assert a.selection['identities']['dataInput']!=b.selection['identities']['dataInput']
        with pytest.raises(Error,match='parent identity/completion pin'):
            verify_selection(a.root,parent_root=new.root)
