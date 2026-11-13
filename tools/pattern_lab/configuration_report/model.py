"""Bounded presentation metadata; no financial projection or member decoding."""
from copy import deepcopy
import json
from pathlib import Path

from ..accounts.contracts import require
from ..account_study.common import closed, parse_json

SCHEMA = 'pattern-lab.configuration-report-presentation.v1'
MAX_OPTIONS = 1024 * 1024


def canonical(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(',', ':'))


def scalars(value, prefix=''):
    if isinstance(value, (dict, list)):
        items = value.items() if isinstance(value, dict) else enumerate(value)
        for key, child in items:
            token = str(key).replace('~', '~0').replace('/', '~1')
            yield from scalars(child, prefix + '/' + token)
    else:
        yield prefix, value


def presentation_options(value, metadata):
    if value is None:
        value = dict(schema=SCHEMA, specificationIdentity=metadata['identities']['specification'])
    elif isinstance(value, (str, Path)):
        with Path(value).open('rb') as stream:
            raw = stream.read(MAX_OPTIONS + 1)
        require(len(raw) <= MAX_OPTIONS, 'presentation byte bound')
        value = parse_json(raw.decode('utf-8'))
    else:
        value = deepcopy(value)
    require(len(canonical(value).encode()) <= MAX_OPTIONS, 'presentation byte bound')
    closed(value, ('schema', 'specificationIdentity'), ('title', 'subtitle', 'parameterLabels', 'initialSelection'))
    require(value['schema'] == SCHEMA and value['specificationIdentity'] == metadata['identities']['specification'], 'presentation identity/schema')
    for name, limit in [('title', 512), ('subtitle', 2048)]:
        if name in value:
            require(type(value[name]) is str and bool(value[name].strip()) and len(value[name]) <= limit, 'presentation text')
    allocations = metadata['request']['allocations']
    paths = {path for a in allocations for path, _ in scalars(a)}
    labels = value.get('parameterLabels', [])
    require(type(labels) is list, 'parameter labels')
    seen = set()
    for label in labels:
        closed(label, ('path', 'label'), ('shortLabel', 'unit'))
        path = label['path']
        require(type(path) is str and path in paths and path not in seen, 'unique scalar pointer')
        seen.add(path)
        for key in set(label) - {'path'}:
            require(type(label[key]) is str and bool(label[key].strip()) and len(label[key]) <= 128, 'parameter label text')
    ids = value.get('initialSelection', [metadata['configurations'][0]['configurationId']])
    known = {r['configurationId'] for r in metadata['configurations']}
    require(type(ids) is list and len(ids) <= 12 and all(type(i) is str and i in known for i in ids), 'initial selection')
    require(len(ids) == len(set(ids)), 'duplicate selection')
    return {**value, 'initialSelection': ids}
