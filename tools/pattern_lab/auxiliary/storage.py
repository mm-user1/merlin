"""Immutable typed snapshots with separate semantic and physical identities."""
from pathlib import Path
import os

import pyarrow as pa
import pyarrow.parquet as pq

from ..accounts.contracts import require
from ..account_study.common import read_json, write_json, file_hash, closed, member_names, digest_id
from .model import content, fingerprint, inspect_content, validate_series

SCHEMA = 'pattern-lab.auxiliary-snapshot.v1'
MAX_JSON_BYTES = 16*1024*1024
MAX_TABLE_BYTES = 512*1024*1024
MAX_ROWS = 2000000


def inspection(value, request):
    facts=inspect_content(value)
    facts['collectionStatus']='retrieved' if request is not None else 'explicitOfflinePublication'
    facts['requestedBounds']=None
    facts['uncoveredIntervals']=[]
    if request is not None:
        from data_provider.manifest import to_epoch_ms
        start,end=(to_epoch_ms(request[k],k) for k in ('startUtc','endUtc'))
        facts['requestedBounds']=[start,end];cursor=start
        for a,b in value['coverage']:
            if cursor<a:facts['uncoveredIntervals'].append([cursor,a])
            cursor=max(cursor,b)
        if cursor<end:facts['uncoveredIntervals'].append([cursor,end])
    facts['emptyRetrievedSpans']=[[a,b] for a,b in value['coverage'] if not any(a<=r['observedAtMs']<b for r in value['rows'])]
    return facts


def safe_path(value):
    p = Path(value).expanduser().absolute()
    require(not any(q.is_symlink() for q in (p, *p.parents)), 'auxiliary symlink path')
    return p.resolve()


def new_root(value, previous=None):
    p = safe_path(value)
    require(not p.exists(), 'auxiliary output must be new')
    if previous is not None:
        old = safe_path(previous)
        require(not p.is_relative_to(old) and not old.is_relative_to(p), 'overlapping auxiliary paths')
    return p


def json_read(path):
    require(path.is_file() and path.stat().st_size <= MAX_JSON_BYTES, 'auxiliary JSON bound')
    return read_json(path)


def write_series(root, value, request, receipt):
    value = content(**value)
    s = value['series']; t = validate_series(s)
    require(len(value['rows']) <= MAX_ROWS, 'auxiliary row bound')
    sid = s['seriesId']
    folder = Path(root)/'series'/sid
    folder.mkdir(parents=True, exist_ok=False)
    pq.write_table(pa.Table.from_pylist(value['rows'], schema=t.arrow_schema()), folder/'rows.parquet',
                   compression='zstd', version='2.6')
    meta = dict(series=s, coverage=value['coverage'], contentId=fingerprint(value),
                inspection=inspection(value,request), request=request)
    write_json(folder/'series.json', meta)
    write_json(folder/'receipt.json', receipt)
    return dict(seriesId=sid, contentId=meta['contentId'])


def finish_snapshot(root, entries, provenance):
    root = Path(root)
    entries = sorted(entries, key=lambda e:e['seriesId'])
    require(entries and len({e['seriesId'] for e in entries}) == len(entries), 'snapshot series roster')
    write_json(root/'manifest.json', dict(schema=SCHEMA, series=entries, contentId=fingerprint(entries)))
    write_json(root/'provenance.json', provenance)
    files = sorted(p for p in root.rglob('*') if p.is_file())
    members = {p.relative_to(root).as_posix():dict(bytes=p.stat().st_size, sha256=file_hash(p)) for p in files}
    write_json(root/'completion.json', dict(schema=SCHEMA, members=members, provenanceId=fingerprint(members)))
    return load_snapshot(root)


def publish_snapshot(series, *, output_root, provenance=None):
    """Publish normalized offline observations (also useful for provider fixtures)."""
    values = [content(**v) for v in series]
    require(len({v['series']['seriesId'] for v in values}) == len(values), 'duplicate series')
    root = new_root(output_root); root.mkdir(parents=True)
    entries = [write_series(root, v, None, dict(origin='explicit normalized observations')) for v in values]
    return finish_snapshot(root, entries, provenance or {})


class Snapshot:
    def __init__(self, root, manifest, seal):
        self.root, self.manifest, self.seal = root, manifest, seal

    @property
    def contentId(self): return self.manifest['contentId']

    def series(self, sid):
        require(sid in {r['seriesId'] for r in self.manifest['series']}, 'snapshot series absent')
        folder = self.root/'series'/sid
        meta = json_read(folder/'series.json')
        closed(meta, ('series', 'coverage', 'contentId', 'inspection', 'request'))
        t = validate_series(meta['series'])
        path = folder/'rows.parquet'
        require(path.stat().st_size <= MAX_TABLE_BYTES, 'auxiliary table bound')
        parquet = pq.ParquetFile(path)
        require(parquet.metadata.num_rows <= MAX_ROWS and parquet.schema_arrow == t.arrow_schema(), 'auxiliary table schema/rows')
        rows = parquet.read().to_pylist()
        value = content(meta['series'], rows, meta['coverage'])
        require(rows == value['rows'] and meta['coverage'] == value['coverage'], 'noncanonical auxiliary content')
        expected=next(r['contentId'] for r in self.manifest['series'] if r['seriesId']==sid)
        require(meta['series']['seriesId'] == sid and fingerprint(value) == meta['contentId'] == expected, 'series content identity')
        require(inspection(value,meta['request']) == meta['inspection'], 'series inspection differs')
        return value

    def slice(self,sid,start,end):
        """Read only protocol-admitted observation rows after full parent verification."""
        require(sid in {r['seriesId'] for r in self.manifest['series']}, 'snapshot series absent')
        folder=self.root/'series'/sid
        def unchanged():
            for name in ('rows.parquet','series.json'):
                path=folder/name;row=self.seal['members']['series/'+sid+'/'+name]
                require(not path.is_symlink() and path.stat().st_size==row['bytes'] and file_hash(path)==row['sha256'], 'snapshot changed during consumption')
        unchanged();meta=json_read(folder/'series.json')
        rows=pq.read_table(folder/'rows.parquet',filters=[('observedAtMs','>=',start),('observedAtMs','<',end)]).to_pylist()
        unchanged()
        coverage=[[max(a,start),min(b,end)] for a,b in meta['coverage'] if max(a,start)<min(b,end)]
        return content(meta['series'],rows,coverage)


def load_snapshot(root, *, verify=True):
    root = safe_path(root)
    manifest, seal = json_read(root/'manifest.json'), json_read(root/'completion.json')
    closed(manifest, ('schema', 'series', 'contentId')); closed(seal, ('schema', 'members', 'provenanceId'))
    require(manifest['schema'] == seal['schema'] == SCHEMA, 'auxiliary snapshot schema')
    require(type(manifest['series']) is list and manifest['series'], 'snapshot roster')
    expected = {'manifest.json', 'provenance.json'}
    ids = []
    from ..accounts.contracts import identifier
    for row in manifest['series']:
        closed(row, ('seriesId', 'contentId')); identifier(row['seriesId']); digest_id(row['contentId'])
        ids.append(row['seriesId'])
        expected.update('series/'+row['seriesId']+'/'+n for n in ('rows.parquet', 'series.json', 'receipt.json'))
    require(ids == sorted(set(ids)), 'snapshot series order/duplicates')
    require(manifest['contentId'] == fingerprint(manifest['series']), 'snapshot content identity')
    require(type(seal['members']) is dict and set(seal['members']) == expected, 'snapshot exact members')
    member_names([*expected, 'completion.json'])
    actual = set()
    for base, dirs, files in os.walk(root, followlinks=False):
        require(not any((Path(base)/n).is_symlink() for n in dirs+files), 'snapshot symlink member')
        actual.update((Path(base)/n).relative_to(root).as_posix() for n in files)
    require(actual == expected | {'completion.json'}, 'snapshot member set differs')
    require(fingerprint(seal['members']) == seal['provenanceId'], 'snapshot provenance identity')
    for name, row in seal['members'].items():
        closed(row, ('bytes', 'sha256')); digest_id(row['sha256'])
        require(type(row['bytes']) is int and 0 <= row['bytes'] <= MAX_TABLE_BYTES, 'snapshot member bound')
        p = root/name
        require(p.stat().st_size == row['bytes'] and (not verify or file_hash(p) == row['sha256']), 'snapshot integrity '+name)
    reader = Snapshot(root, manifest, seal)
    if verify:
        for row in manifest['series']: require(fingerprint(reader.series(row['seriesId'])) == row['contentId'], 'snapshot semantic identity')
    return reader


def inspect_auxiliary(root, *, verify=False):
    reader = load_snapshot(root, verify=verify)
    return dict(schema=SCHEMA, contentId=reader.contentId, provenanceId=reader.seal['provenanceId'],
        integrityVerified=verify, series=[json_read(reader.root/'series'/e['seriesId']/'series.json') for e in reader.manifest['series']])
