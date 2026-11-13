"""Stored-only schema, physical framing and resealed consistency refusals."""
from copy import deepcopy
from pathlib import Path
import json
import shutil
import struct
import zipfile
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.configuration_study import run_configuration_study,load_configuration_study,export_public_results
from tools.pattern_lab.configuration_study import codec,artifacts
from tools.pattern_lab.account_study.containers import zip_info
from ._configuration_helpers import inputs


@pytest.fixture(scope='module')
def compact(tmp_path_factory):
    root=tmp_path_factory.mktemp('compact')
    request=inputs(root/'pack',count=1,multi_tf=False)
    return run_configuration_study(request,data_root=root/'pack',output_root=root/'owner')


@pytest.mark.parametrize('fault',['trailing','concatenated','truncated','extraFlag','mtime','digest','decodedSize','oversized'])
def test_gzip_closed_framing(monkeypatch,fault):
    packed,info=codec.encode(dict(x='payload'))
    if fault=='trailing':packed+=b'!'
    if fault=='concatenated':packed+=packed
    if fault=='truncated':packed=packed[:-1]
    if fault=='extraFlag':packed=packed[:3]+b'\x04'+packed[4:]
    if fault=='mtime':packed=packed[:4]+b'\x01'+packed[5:]
    if fault=='digest':info['decodedSha256']='0'*64
    if fault=='decodedSize':info['decodedBytes']+=1
    if fault=='oversized':monkeypatch.setattr(codec,'MAX_MEMBER',2)
    info.update(bytes=len(packed),sha256=codec.sha(packed))
    with pytest.raises((PatternLabDataError,codec.zlib.error)):codec.decode(packed,info)


@pytest.mark.parametrize('fault',['zip64','localSize','crc','flags','prefix','comment','extraRoot','missing','unknownSchema','duplicateIdentity','incomplete','wrongScale','wrongEndpoint','unknownMember','decodedLimit'])
def test_resealed_artifact_refusals(compact,tmp_path,fault):
    root=tmp_path/'bad';shutil.copytree(compact.root,root)
    meta=artifacts.bounded_json(root/'study.json');index=artifacts.bounded_json(root/'index.json')
    raw=bytearray((root/'results.zip').read_bytes())
    if fault=='zip64':raw[-12:-10]=b'\xff\xff'
    if fault=='localSize':raw[18:22]=struct.pack('<L',1)
    if fault=='crc':raw[30+len(index['members'][0]['name'])+15]^=1
    if fault=='flags':raw[6]=8
    if fault=='prefix':raw=b'X'+raw
    if fault=='comment':raw+=b'X'
    (root/'results.zip').write_bytes(raw)
    if fault=='extraRoot':(root/'extra.json').write_text('{}')
    if fault=='missing':meta['configurations'].pop()
    if fault=='unknownSchema':meta['schema']='unknown'
    if fault=='duplicateIdentity':meta['configurations'][1]=deepcopy(meta['configurations'][0])
    if fault=='incomplete':meta['counts']['completedResults']-=1
    if fault=='decodedLimit':index['members'][0]['decodedBytes']=codec.MAX_MEMBER+1
    if fault in ('wrongScale','wrongEndpoint','unknownMember'):
        members={e['name']:compact.raw_member(e['name']) for e in index['members']}
        entry=index['members'][1];value=codec.decode(members[entry['name']],entry)
        if fault=='wrongScale':value['records'][0]['curve']['valueScale']=10
        if fault=='wrongEndpoint':value['records'][0]['curve']['equityQ'][-1]+=20
        if fault=='unknownMember':members['unexpected.json.gz']=b'unknown'
        else:
            packed,info=codec.encode(value);members[entry['name']]=packed;entry.update(info)
        with zipfile.ZipFile(root/'results.zip','w',compression=zipfile.ZIP_STORED) as z:
            for name,payload in members.items():z.writestr(zip_info(name),payload)
    meta['artifactIdentity']=artifacts.artifact_identity(meta,index)
    artifacts.save_json(root/'study.json',meta);artifacts.save_json(root/'index.json',index);artifacts.seal(root)
    with pytest.raises(PatternLabDataError):load_configuration_study(root)


def test_public_contains_no_ho2_members_or_execution_receipts(compact,tmp_path):
    public=export_public_results(compact.root,output_root=tmp_path/'public')
    hidden=[]
    for c in compact.configurations:
        row=compact.result(c['configurationId'],'HO2')
        hidden.extend([row['execution']['accountId'],row['execution']['preparationId'],row['resultId']])
    content=''.join(p.read_text() for p in public.root.glob('*.json'))
    for e in public.chunk_index['members']:
        assert e['stageId']!='HO2'
        content+=json.dumps(codec.decode(public.raw_member(e['name']),e))
    assert all(value not in content for value in hidden)
    assert public.metadata['synthetic'] is False


def test_fresh_views_and_no_curve_decoding_during_enumeration(compact,monkeypatch):
    meta=compact.metadata;meta['counts']['completedResults']=0
    assert compact.metadata['counts']['completedResults']>0
    monkeypatch.setattr(codec,'decode',lambda *a:(_ for _ in ()).throw(AssertionError('unexpected curve decode')))
    assert len(compact.stage_rows)==9 and len(compact.configurations)==3


@pytest.mark.parametrize('route',['load','verify','public','unsealed'])
@pytest.mark.parametrize('location',['outer','IS','HO2'])
def test_stored_external_paths_never_resolved(compact,tmp_path,monkeypatch,route,location):
    root=tmp_path/'bad';shutil.copytree(compact.root,root)
    outside=tmp_path/'external.json';outside.write_text('{"syntheticSentinel":true}')
    meta=artifacts.bounded_json(root/'study.json')
    if location=='outer':meta['request']=str(outside)
    else:next(s for s in meta['request']['stages'] if s['stageId']==location)['protocol']=str(outside)
    artifacts.save_json(root/'study.json',meta);artifacts.seal(root)
    if route=='unsealed':(root/'completion.json').unlink()
    accesses=[]
    def trap(original,name):
        def guarded(path,*a,**kw):
            if path==outside:
                accesses.append(name);raise AssertionError('external path reached '+name)
            return original(path,*a,**kw)
        return guarded
    for name in ('resolve','stat','open'):
        monkeypatch.setattr(Path,name,trap(getattr(Path,name),name))
    with pytest.raises(PatternLabDataError,match='stored'):
        if route=='load':load_configuration_study(root)
        elif route=='verify':artifacts.verify_configuration_study(root)
        elif route=='public':export_public_results(root,output_root=tmp_path/'public')
        else:artifacts._load(root,sealed=False)
    assert accesses==[] and not (tmp_path/'public').exists()


@pytest.mark.parametrize('fault',['requestList','requestNull','stagesMissing','stagesObject','stageCount','entryList','entryNull','protocolMissing','protocolList'])
def test_stored_request_shapes_refused_before_resolver(compact,tmp_path,monkeypatch,fault):
    from tools.pattern_lab.configuration_study import request as contract
    root=tmp_path/'bad';shutil.copytree(compact.root,root);meta=artifacts.bounded_json(root/'study.json');doc=meta['request']
    if fault=='requestList':meta['request']=[]
    if fault=='requestNull':meta['request']=None
    if fault=='stagesMissing':del doc['stages']
    if fault=='stagesObject':doc['stages']={}
    if fault=='stageCount':doc['stages'].pop()
    if fault=='entryList':doc['stages'][1]=[]
    if fault=='entryNull':doc['stages'][1]=None
    if fault=='protocolMissing':del doc['stages'][2]['protocol']
    if fault=='protocolList':doc['stages'][2]['protocol']=[]
    artifacts.save_json(root/'study.json',meta);artifacts.seal(root)
    def denied(*a,**kw):raise AssertionError('malformed stored shape reached resolver')
    monkeypatch.setattr(contract,'read_json',denied)
    with pytest.raises(PatternLabDataError):load_configuration_study(root)


def test_zip64_end_records_in_directory_refused(compact,tmp_path):
    root=tmp_path/'bad';shutil.copytree(compact.root,root);path=root/'results.zip';raw=path.read_bytes()
    sig,disk,cd_disk,here,total,size,offset,comment=struct.unpack('<4s4H2LH',raw[-22:])
    end=offset+size;assert end==len(raw)-22
    record=struct.pack('<4sQ2H2L4Q',b'PK\x06\x06',44,45,45,0,0,here,total,size,offset)
    locator=struct.pack('<4sLQL',b'PK\x06\x07',0,end,1)
    assert len(record+locator)==76
    eocd=struct.pack('<4s4H2LH',sig,disk,cd_disk,here,total,size+76,offset,comment)
    path.write_bytes(raw[:end]+record+locator+eocd);artifacts.seal(root)
    with pytest.raises(PatternLabDataError,match='central directory extent'):load_configuration_study(root)
