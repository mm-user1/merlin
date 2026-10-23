"""Account-owned STORED containers: bounded metadata, no extracted members or live cache handles."""
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
import io
import stat
import struct
import zipfile
import zlib

from ..accounts.contracts import require
from ..study.contracts import semantic_digest
from .common import digest_id, member_names, parse_json

MAX_JSON = 16 * 1024 * 1024
MAX_MEMBERS = 100000
PAIRS = frozenset((('pattern-lab.account-study.v1', 1), ('pattern-lab.account-study.v2', 2),
                   ('pattern-lab.account-study.v1', 3), ('pattern-lab.account-study.v2', 3)))
FILES = ('account.json', 'attempts.parquet', 'exitCommands.parquet', 'path.parquet',
         'proposals.parquet', 'trades.parquet')
INDEX_SCHEMA = 'pattern-lab.account-container-index.v1'


def bounded_json(path):
    with Path(path).open('rb') as stream:
        value = stream.read(MAX_JSON + 1)
    require(len(value) <= MAX_JSON, 'account JSON exceeds bound')
    return parse_json(value.decode('utf-8'))


def version(study):
    require(type(study.get('formatVersion')) is int and
            (study.get('schema'), study['formatVersion']) in PAIRS, 'study schema/version')
    return study['formatVersion']


def container_path(instrument):
    return 'accounts/' + semantic_digest(['account-container.1', instrument]) + '.zip'


def index_for(family):
    require(6 * len(family) <= MAX_MEMBERS, 'planned logical member limit')
    groups = {}
    for label in family:
        groups.setdefault(label['instrumentId'], []).append(semantic_digest(label))
    rows = [dict(instrumentId=i, path=container_path(i), accountKeys=sorted(groups[i])) for i in sorted(groups)]
    require(all(len(r['accountKeys']) == len(set(r['accountKeys'])) for r in rows), 'duplicate planned account')
    return dict(schema=INDEX_SCHEMA, containers=rows)


def names_for(keys):
    return tuple('accounts/' + key + '/' + name for key in sorted(keys) for name in FILES)


def zip_info(name):
    info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | 0o600) << 16
    info.extra = b''
    return info


def _directory_start(stream, size, expected):
    """Bound the standard-library directory read before constructing ZipFile (ZIP64 included)."""
    require(size >= 22, 'container end record')
    stream.seek(size - min(size, 65557))
    tail = stream.read(min(size, 65557))
    pos = tail.rfind(b'PK\x05\x06')
    require(pos >= 0 and pos + 22 <= len(tail), 'container end record')
    _, disk, directory_disk, local_count, count, length, start, comment = struct.unpack_from('<4s4H2LH', tail, pos)
    require(pos + 22 + comment == len(tail) and not comment and disk == directory_disk == 0,
            'container end record layout')
    end = size - len(tail) + pos
    if count == 65535 or length == 0xffffffff or start == 0xffffffff:
        require(end >= 76, 'container ZIP64 end record')
        stream.seek(end - 20)
        signature, disk, offset, disks = struct.unpack('<4sLQL', stream.read(20))
        require(signature == b'PK\x06\x07' and disk == 0 and disks == 1 and offset + 56 == end - 20,
                'container ZIP64 locator')
        stream.seek(offset)
        values = struct.unpack('<4sQ2H2L4Q', stream.read(56))
        require(values[0] == b'PK\x06\x06' and values[1] == 44 and values[4] == values[5] == 0,
                'container ZIP64 end record')
        local_count, count, length, start = values[6:]
        end = offset
    require(count == local_count == len(expected) and count <= MAX_MEMBERS, 'container member count')
    # Our canonical layout has no comments, and at most the ZIP64 extra field per entry.
    require(length <= sum(46 + len(n.encode('utf-8')) + 32 for n in expected), 'container directory size bound')
    require(start >= 0 and start + length == end, 'container directory extent')
    return start


@dataclass(frozen=True)
class Member:
    offset: int
    size: int
    crc: int


@dataclass(frozen=True)
class Container:
    path: Path
    members: object

    @classmethod
    def inspect(cls, path, keys):
        path = Path(path)
        expected = names_for(keys)
        require(not path.is_symlink() and path.is_file(), 'missing/non-file account container')
        locations = {}
        with path.open('rb') as stream:
            boundary = _directory_start(stream, path.stat().st_size, expected)
            with zipfile.ZipFile(stream) as archive:
                infos = archive.infolist()
                names = [i.filename for i in infos]
                member_names(names)
                require(set(names) == set(expected), 'container logical member set')
                previous = 0
                for info in sorted(infos, key=lambda i: i.header_offset):
                    require(not info.is_dir() and not info.external_attr & 0x18 and
                            stat.S_IFMT(info.external_attr >> 16) in (0, stat.S_IFREG),
                            'non-file account container member')
                    require(info.compress_type == zipfile.ZIP_STORED and info.flag_bits & ~0x800 == 0,
                            'container compression/encryption/data descriptor')
                    require(info.file_size == info.compress_size, 'container STORED sizes')
                    require(previous <= info.header_offset and info.header_offset + 30 <= boundary,
                            'container overlapping/out-of-bounds extent')
                    stream.seek(info.header_offset)
                    header = stream.read(30)
                    sig, needed, flags, method, tm, dt, crc, packed, size, nl, el = struct.unpack('<4s5H3L2H', header)
                    require(sig == b'PK\x03\x04' and flags == info.flag_bits and method == info.compress_type,
                            'container local header flags/method')
                    require(info.header_offset + 30 + nl + el <= boundary, 'container local header extent')
                    name, extra = stream.read(nl), stream.read(el)
                    require(name == info.filename.encode('utf-8' if flags & 0x800 else 'cp437'),
                            'container local header name')
                    extras = {}; cursor = 0
                    while cursor < len(extra):
                        require(cursor + 4 <= len(extra), 'container local extra')
                        tag, length = struct.unpack_from('<HH', extra, cursor); cursor += 4
                        require(cursor + length <= len(extra) and tag not in extras, 'container local extra')
                        extras[tag] = extra[cursor:cursor+length]; cursor += length
                    if size == 0xffffffff or packed == 0xffffffff:
                        values = extras.get(1, b''); cursor = 0
                        if size == 0xffffffff:
                            require(len(values) >= cursor + 8, 'container ZIP64 local size')
                            size = struct.unpack_from('<Q', values, cursor)[0]; cursor += 8
                        if packed == 0xffffffff:
                            require(len(values) >= cursor + 8, 'container ZIP64 local size')
                            packed = struct.unpack_from('<Q', values, cursor)[0]
                    require(crc == info.CRC and size == info.file_size and packed == info.compress_size,
                            'container local header CRC/size')
                    offset = info.header_offset + 30 + nl + el
                    require(offset + size <= boundary, 'container overlapping/out-of-bounds extent')
                    if info.filename.endswith('/account.json'):
                        require(size <= MAX_JSON, 'account JSON exceeds bound')
                    locations[info.filename] = Member(offset, size, crc)
                    previous = offset + size
        return cls(path, MappingProxyType(locations))

    def read(self, name):
        """One bounded member buffer, CRC checked before JSON/Arrow sees any payload."""
        member = self.members[name]
        result = io.BytesIO()
        try:
            with self.path.open('rb') as stream:
                stream.seek(member.offset)
                remaining, crc = member.size, 0
                while remaining:
                    block = stream.read(min(1024 * 1024, remaining))
                    require(block, 'truncated container member')
                    remaining -= len(block); crc = zlib.crc32(block, crc); result.write(block)
            require(crc == member.crc, 'container member CRC')
            result.seek(0)
            return result
        except BaseException:
            result.close()
            raise


class Locator:
    """Explicit root/layout ownership; immutable directory metadata reused across interleaved reads."""
    def __init__(self, root, keys, rows=()):
        self.root = Path(root).resolve()
        self.keys = frozenset(keys)
        self.paths = MappingProxyType({key: row['path'] for row in rows for key in row['accountKeys']})
        self.groups = MappingProxyType({row['path']: tuple(row['accountKeys']) for row in rows})
        self._directories = {}

    @classmethod
    def admitted(cls, root, study):
        layout = version(study)
        keys = [semantic_digest(label) for label in study['family']]
        if layout != 3:
            return cls(root, keys)
        index = bounded_json(Path(root)/'account-index.json')
        require(index == index_for(study['family']), 'account container index differs from planned family')
        return cls(root, keys, index['containers'])

    @classmethod
    def from_root(cls, root):
        study = bounded_json(Path(root)/'study.json')
        version(study)
        if (Path(root)/'completion.json').exists():
            seal = bounded_json(Path(root)/'completion.json')
            version(seal)
            require((seal.get('schema'), seal.get('formatVersion')) == (study['schema'], study['formatVersion']),
                    'study/seal version differs')
        return cls.admitted(root, study)

    def check(self, root, key):
        digest_id(key)
        require(Path(root).resolve() == self.root and key in self.keys, 'account locator root/key')

    def container(self, key):
        path = self.paths[key]
        if path not in self._directories:
            self._directories[path] = Container.inspect(self.root/path, self.groups[path])
        return self._directories[path]

    def metadata(self, key):
        if not self.paths:
            return bounded_json(self.root/'accounts'/key/'account.json')
        with self.container(key).read('accounts/'+key+'/account.json') as stream:
            return parse_json(stream.read().decode('utf-8'))

    def table(self, key, name):
        import pyarrow.parquet as pq
        if not self.paths:
            return pq.read_table(self.root/'accounts'/key/(name+'.parquet'))
        with self.container(key).read('accounts/'+key+'/'+name+'.parquet') as stream:
            return pq.read_table(stream)

    def physical(self):
        return {'account-index.json', *self.groups} if self.paths else set(names_for(self.keys))

    def logical_count(self, physical_count):
        return physical_count - len(self.groups) - 1 + 6 * len(self.keys) if self.paths else physical_count
