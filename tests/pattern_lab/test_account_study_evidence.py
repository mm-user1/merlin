from dataclasses import replace
import json
from pathlib import Path
import shutil
import zipfile

import numpy as np
import pyarrow.parquet as pq
import pytest

from tools.pattern_lab import PatternLabDataError as Error
from tools.pattern_lab.accounts import execute_account, validate_result
from tools.pattern_lab.account_study import (run_account_study, load_account_study,
    verify_account_study, export_account_bundle, import_account_bundle)
from tools.pattern_lab.account_study.codec import (AccountRecord, encode_accounts, decode_accounts,
    equal_records, DISK_ATTEMPTS)
from tools.pattern_lab.account_study.common import read_json, write_json, member_names
from ._account_helpers import prepared, side, bundle, filter_for, policy, rules, window
from ._account_study_helpers import build_pack, request, reseal


@pytest.mark.parametrize('filtered',[False,True])
def test_generic_codec_no_provider_import_and_reversible_filter_projection(tmp_path,filtered):
    p=prepared([(100,102,98,100)]*8,slots=[0,1,2,3,5,6,7,8],end=10*1800000)
    f=filter_for(p,known=[False,True,True,True,True,True,True,True]) if filtered else None
    c=bundle(p,long=side(8,entries=[0,1,4],exits=[2]),short=side(8,entries=[0],stop=110),filt=f)
    r=execute_account(p,c,policy(),window(p),rules())
    original=AccountRecord(p,r,dict(instrumentId='custom',allocationId='custom',windowId='w'))
    keys=encode_accounts(tmp_path,[original])
    restored=decode_accounts(tmp_path)[0]
    assert equal_records(original,restored)
    assert restored.prepared.features==() and len(restored.prepared.inputs)==1
    validate_result(restored.result,restored.prepared,restored.result.commands,r.policy,r.window,r.rules)
    disk=pq.read_table(tmp_path/'accounts'/keys[0]/'attempts.parquet')
    assert disk.schema==DISK_ATTEMPTS
    assert disk['filterPresent'].to_pylist()==[filtered]*len(disk)
    if not filtered:
        assert disk['sourceIndex'].null_count==len(disk)
        assert restored.result.attempts['sourceIndex'].to_pylist()==[-1]*len(disk)
    else:
        assert restored.result.attempts['reason'].to_pylist()[:2]==['filterUnknown','filterUnknown']
        assert restored.result.attempts['filterAllowed'].to_pylist()[:2]==[True,True]
    for a in (restored.prepared.target.values,restored.result.commands.long.initialStop):
        with pytest.raises(ValueError): a.setflags(write=True)
    assert np.array_equal(restored.prepared.target.values,p.target.values)


@pytest.fixture
def retained(tmp_path):
    pack=tmp_path/'pack';ids=build_pack(pack,count=1)
    return run_account_study(request(ids,filtered=False,multiple=False),data_root=pack,output_root=tmp_path/'run')


@pytest.mark.parametrize('member',['derived/report.html','derived/summary.json','status.json'])
def test_original_derived_and_status_sealed(retained,member):
    p=retained.root/member; p.write_bytes(p.read_bytes()+b' ')
    with pytest.raises(Error,match='integrity'): load_account_study(retained.root)


def test_full_validation_distinct_from_structural_load(retained):
    path=retained.root/'accounts'/retained.accountKeys[0]/'account.json'
    meta=read_json(path);meta['result']['finalBalance']+=123
    write_json(path,meta);reseal(retained.root)
    assert load_account_study(retained.root).accountKeys==retained.accountKeys
    with pytest.raises(Error): verify_account_study(retained.root)


@pytest.mark.parametrize('mutation',['missing','extra','duplicateJson','pathEscape','badIdentity','wrongRoster'])
def test_malformed_saved_structure(retained,mutation):
    root=retained.root
    if mutation=='missing': (root/'completion.json').unlink()
    elif mutation=='extra': (root/'extra.json').write_text('{}')
    elif mutation=='duplicateJson':
        p=root/'completion.json';p.write_text('{"schema":"x","schema":"y"}')
    else:
        p=root/'accounts'/retained.accountKeys[0]/'account.json';meta=read_json(p)
        if mutation=='pathEscape': meta['gridId']='../../outside'
        elif mutation=='badIdentity': meta['result']['identity']='0'*64
        elif mutation=='wrongRoster': meta['labels']['instrumentId']='other'
        write_json(p,meta);reseal(root)
    with pytest.raises(Error): load_account_study(root)


@pytest.mark.parametrize('names',[
    ['../escape'], ['/absolute'], ['C:/absolute'], ['a\\b'], ['a//b'], ['a/./b'], ['NUL.json'],
    ['COM1/x'], ['a.'], ['e\u0301'], ['A/x','a/X'], ['a','a/b'], ['x','x'], ['<script>']])
def test_portable_member_path_rejection(names):
    with pytest.raises(Error): member_names(names)


@pytest.mark.parametrize('mutation',['duplicate','extra','missing','unsafe','symlink','hash','size','case'])
def test_archive_refuses_before_publish(retained,tmp_path,mutation):
    export_account_bundle(retained.root,zip_path=tmp_path/'good.zip')
    with zipfile.ZipFile(tmp_path/'good.zip') as z: payload={n:z.read(n) for n in z.namelist()}
    target=tmp_path/'bad.zip'
    with zipfile.ZipFile(target,'w') as z:
        for n,b in payload.items():
            if mutation=='missing' and n=='status.json':continue
            if mutation=='hash' and n=='status.json':b=b.replace(b'complete',b'completx')
            if mutation=='size' and n=='status.json':b+=b' '
            if mutation=='symlink' and n=='status.json':
                info=zipfile.ZipInfo(n);info.create_system=3;info.external_attr=0o120777<<16;z.writestr(info,b)
            else:z.writestr(n,b)
        if mutation=='duplicate':
            with pytest.warns(UserWarning):z.writestr('completion.json',payload['completion.json'])
        elif mutation=='extra':z.writestr('extra.json',b'{}')
        elif mutation=='unsafe':z.writestr('../outside',b'x')
        elif mutation=='case':z.writestr('STATUS.JSON',b'{}')
    with pytest.raises(Error): import_account_bundle(target,output_root=tmp_path/'imported')
    assert not (tmp_path/'imported').exists() and target.exists()
    assert not list(tmp_path.glob('.account-import-*'))


def test_failed_typed_payload_real_spawn_pool_cleanup():
    from tools.pattern_lab.study.workers import SpawnJobPool,WorkerSettings,single_threaded_children
    from tools.pattern_lab.account_study.job import AccountJob
    job=AccountJob('deliberately-incomplete',({},),(),{},None,{})
    with single_threaded_children():
        pool=SpawnJobPool(1,WorkerSettings((),(),()))
        try:
            pool.start();pool.submit('invalid',job)
            kind,key,error=pool.take(timeout=30)
            assert kind=='error' and key=='invalid' and error['type']=='KeyError'
        finally:pool.shutdown(abort=True)
        assert pool.live_workers()==[]
