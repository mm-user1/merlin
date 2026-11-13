"""One framed gzip JSON member and a narrow canonical ordinary-STORED ZIP."""
import gzip
import hashlib
import io
from pathlib import Path
import stat
import struct
import zipfile
import zlib

from ..accounts.contracts import require
from ..account_study.common import json_text, parse_json, member_names
from ..account_study.containers import zip_info, _directory_start

MAX_MEMBER=8*1024*1024
MAX_TOTAL=512*1024*1024
MAX_ARCHIVE=128*1024*1024
MAX_MEMBERS=65534
GZIP_HEADER=bytes.fromhex('1f8b08000000000002ff')
CHUNK_SCHEMA='pattern-lab.configuration-chunk.v1'


def sha(raw):return hashlib.sha256(raw).hexdigest()


def encode(value):
    raw=json_text(value).encode('utf-8')
    require(len(raw)<=MAX_MEMBER,'decompressed member bound')
    stream=io.BytesIO()
    with gzip.GzipFile(fileobj=stream,mode='wb',compresslevel=9,mtime=0,filename='') as z:z.write(raw)
    packed=stream.getvalue()
    require(packed[:10]==GZIP_HEADER,'gzip writer header')
    return packed,dict(bytes=len(packed),sha256=sha(packed),decodedBytes=len(raw),decodedSha256=sha(raw))


def decode(packed, expected):
    require(len(packed)==expected['bytes'] and sha(packed)==expected['sha256'],'compressed member integrity')
    require(packed[:10]==GZIP_HEADER,'gzip framing/header')
    decoder=zlib.decompressobj(31);parts=[];count=0
    for offset in range(0,len(packed),65536):
        pending=packed[offset:offset+65536]
        while pending:
            raw=decoder.decompress(pending,MAX_MEMBER-count+1)
            count+=len(raw);require(count<=MAX_MEMBER,'decompressed member bound')
            parts.append(raw)
            require(not decoder.unused_data,'gzip trailing/concatenated member')
            pending=decoder.unconsumed_tail
    require(decoder.eof and not decoder.unconsumed_tail and not decoder.unused_data,'truncated gzip member')
    raw=b''.join(parts)
    require(count==expected['decodedBytes'] and sha(raw)==expected['decodedSha256'],'decoded member integrity')
    return parse_json(raw.decode('utf-8'))


def pack_curves(rows):
    grids=[];lookup={};records=[]
    for row in rows:
        curve=dict(row['curve']);times=tuple(curve.pop('timeMs'))
        if times not in lookup:lookup[times]=len(grids);grids.append(list(times))
        curve['gridId']=lookup[times]
        records.append({**row,'curve':curve})
    return dict(schema=CHUNK_SCHEMA,timeGrids=grids,records=records)


def unpack_curves(value):
    require(type(value) is dict and set(value)=={'schema','timeGrids','records'} and
            value['schema']==CHUNK_SCHEMA,'curve chunk schema')
    require(type(value['timeGrids']) is list and type(value['records']) is list,'curve chunk arrays')
    grids=value['timeGrids']
    require(all(type(g) is list for g in grids) and len({tuple(g) for g in grids})==len(grids),'duplicate/invalid time grids')
    used=set();rows=[]
    for row in value['records']:
        curve=dict(row['curve']);i=curve.pop('gridId')
        require(type(i) is int and 0<=i<len(grids),'curve grid reference')
        used.add(i);curve['timeMs']=list(grids[i]);rows.append({**row,'curve':curve})
    require(used==set(range(len(grids))),'unused time grid')
    return rows


class Archive:
    """Verified immutable offsets; each access opens only the selected byte range."""
    def __init__(self,path,expected):
        self.path=Path(path);self.members={}
        require(not self.path.is_symlink() and self.path.is_file(),'archive must be a regular file')
        size=self.path.stat().st_size
        require(22<=size<=MAX_ARCHIVE and len(expected)<=MAX_MEMBERS,'archive resource bound')
        member_names(expected)
        with self.path.open('rb') as f:
            # The canonical EOCD is the final 22 bytes, with no ZIP64 structures.
            f.seek(size-22);end=struct.unpack('<4s4H2LH',f.read(22))
            require(end[0]==b'PK\x05\x06' and end[1]==end[2]==end[7]==0 and
                    end[3]==end[4]==len(expected) and end[4]<65535 and
                    end[5]<0xffffffff and end[6]<0xffffffff and end[5]+end[6]==size-22,
                    'ordinary ZIP end record; ZIP64/comments refused')
            require(end[5]==sum(46+len(name.encode('ascii')) for name in expected),
                    'canonical ZIP central directory extent; trailing records refused')
            boundary=_directory_start(f,size,expected)
            require(boundary==end[6],'ZIP central directory start disagreement')
            with zipfile.ZipFile(f) as archive:
                infos=archive.infolist()
                require([i.filename for i in infos]==list(expected),'exact canonical ZIP member order')
                offset=0
                for info in infos:
                    canonical=zip_info(info.filename)
                    require(info.date_time==canonical.date_time and info.create_system==canonical.create_system and
                            info.external_attr==canonical.external_attr and not info.extra and not info.comment and
                            not info.is_dir() and stat.S_ISREG(info.external_attr>>16) and not info.external_attr&0x18,
                            'ZIP canonical file metadata')
                    require(info.flag_bits==0 and info.compress_type==zipfile.ZIP_STORED and
                            info.file_size==info.compress_size==expected[info.filename]['bytes'] and
                            info.file_size<0xffffffff and info.extract_version<=20,'ZIP size/flags/ZIP64 refused')
                    require(info.header_offset==offset and offset+30<=boundary,'ZIP overlapping/noncanonical extent')
                    f.seek(offset);h=struct.unpack('<4s5H3L2H',f.read(30))
                    require(h[:4]==(b'PK\x03\x04',info.extract_version,0,0) and h[4:6]==(0,33) and
                            h[6:9]==(info.CRC,info.file_size,info.file_size) and h[10]==0,
                            'ZIP local header/size/extra disagreement')
                    name=f.read(h[9]);require(name==info.filename.encode('ascii'),'ZIP local name')
                    offset+=30+h[9]
                    require(offset+info.file_size<=boundary,'ZIP payload extent')
                    self.members[info.filename]=(offset,info.file_size,info.CRC)
                    offset+=info.file_size
                require(offset==boundary,'ZIP unreferenced local bytes')

    def read(self,name):
        require(name in self.members,'unknown compressed member')
        offset,size,crc=self.members[name]
        with self.path.open('rb') as f:
            f.seek(offset);raw=f.read(size)
        require(len(raw)==size and zlib.crc32(raw)&0xffffffff==crc,'ZIP member CRC/size')
        return raw
