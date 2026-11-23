"""Closed presentation metadata and histogram geometry, not financial analytics."""
from collections import Counter
from copy import deepcopy
import json
import math
from pathlib import Path

from ..accounts.contracts import require
from ..account_study.common import closed, parse_json

PRESENTATION = 'pattern-lab.strategy-screen-report-presentation.v1'
VIEW = 'pattern-lab.strategy-screen-report-view.v1'
MAX_OPTIONS = 1024 * 1024
MAX_VIEW = 32 * 1024 * 1024


def canonical(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(',', ':'))


def view_bytes(value):
    raw = canonical(value).replace('&', '\\u0026').replace('<', '\\u003c').replace('>', '\\u003e').encode('utf-8')
    require(len(raw) + 1 <= MAX_VIEW, 'view byte bound')
    return raw


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def scalars(value, prefix=''):
    if isinstance(value, (dict, list)):
        for key, child in (value.items() if isinstance(value, dict) else enumerate(value)):
            token = str(key).replace('~', '~0').replace('/', '~1')
            yield from scalars(child, prefix + '/' + token)
    else:
        yield prefix, value


def presentation_options(value, metadata):
    if value is None:
        value = dict(schema=PRESENTATION, artifactIdentity=metadata['artifactIdentity'])
    elif isinstance(value, (str, Path)):
        with Path(value).open('rb') as stream:
            raw = stream.read(MAX_OPTIONS + 1)
        require(len(raw) <= MAX_OPTIONS, 'presentation byte bound')
        value = parse_json(raw.decode('utf-8'))
    else:
        value = deepcopy(value)
    require(len(canonical(value).encode()) <= MAX_OPTIONS, 'presentation byte bound')
    closed(value, ('schema', 'artifactIdentity'), ('title', 'subtitle', 'periodLabels', 'parameterLabels'))
    require(value['schema'] == PRESENTATION and value['artifactIdentity'] == metadata['artifactIdentity'], 'presentation identity/schema')
    def text(s, limit):
        require(type(s) is str and bool(s.strip()) and len(s) <= limit, 'presentation text')
    for key, limit in [('title', 512), ('subtitle', 2048)]:
        if key in value:
            text(value[key], limit)
    labels = value.get('periodLabels', {})
    closed(labels, (), ('earlier', 'recent'))
    for label in labels.values():
        text(label, 128)
    paths = {p for a in metadata['request']['allocations'] for p, _ in scalars(a)}
    params = value.get('parameterLabels', [])
    require(type(params) is list, 'parameter labels')
    seen = set()
    for label in params:
        closed(label, ('path', 'label'), ('shortLabel', 'unit'))
        p = label['path']
        require(type(p) is str and p in paths and p not in seen, 'unique scalar pointer')
        seen.add(p)
        for key in set(label) - {'path'}:
            text(label[key], 128)
    return {**value, 'periodLabels': labels, 'parameterLabels': params}


def quantiles(saved):
    return {**saved, 'q1': saved['q25'], 'q3': saved['q75'], 'value': saved['median']}


def histogram_scale(values, step=10):
    """Zero-aligned domain; keep huge sparse ranges bounded and representable."""
    require(finite(step) and step > 0, 'histogram step')
    step = float(step)
    values = [v for v in values if finite(v)]
    if not values:
        return dict(lo=0, hi=step, step=step, bins=1)
    lo = min(0, math.floor(min(values) / step) * step)
    hi = max(step, math.ceil(max(values) / step) * step)
    if lo > min(values):
        lo -= step
    if hi < max(values):
        hi += step
    require(all(finite(v) for v in (lo, hi, hi-lo)) and lo < hi, 'histogram range')
    count = round((hi-lo) / step)
    hi = lo + count * step
    if hi < max(values):
        count += 1
        hi = lo + count * step
    # JS must distinguish neighboring true edges; refusing overflow never coarsens bins.
    require(0 < count <= 2**52 and lo + step > lo and hi - step < hi, 'histogram precision')
    return dict(lo=lo, hi=hi, step=step, bins=count)


def histogram(values, saved, scale):
    values = [v for v in values if finite(v)]
    require(len(values) == saved['n'], 'histogram population differs from native statistics')
    lo, hi, step, count = (scale[k] for k in ('lo', 'hi', 'step', 'bins'))
    lo, hi, step = float(lo), float(hi), float(step)
    def index(v):
        require(lo <= v <= hi, 'histogram out of range')
        # Binary search the actual indexed binary64 boundaries. At most 52
        # comparisons even for a huge sparse grid; no dense edges or epsilon.
        left, right = 0, count
        while left + 1 < right:
            middle = (left + right) // 2
            if v < lo + middle * step:
                right = middle
            else:
                left = middle
        return left
    occupied = Counter(index(v) for v in values)
    require(all(0 <= i < count for i in occupied), 'histogram out of range')
    indices = list(range(count)) if count <= 256 else sorted(occupied)
    return {**quantiles(saved), **scale, 'indices': indices,
            'counts': [occupied[i] for i in indices],
            'edges': [lo+i*step for i in range(count+1)] if count <= 256 else [lo, hi]}


def instrument_labels(instruments):
    """One roster-wide display lookup; raw IDs remain authoritative."""
    candidates = {}
    for ident in instruments:
        prefix, separator, contract = ident.partition('_')
        candidate = (contract if separator else ident).removesuffix('-USDT-SWAP') if not separator or prefix and contract else ident
        candidates[ident] = candidate or ident
    counts = Counter(candidates.values())
    labels = {ident: ident if counts[label] > 1 or label in candidates and label != ident else label
              for ident, label in candidates.items()}
    require(all(labels.values()) and len(set(labels.values())) == len(labels), 'unique instrument labels')
    return labels


def timeframe_label(minutes):
    return f'{minutes // 1440}D' if minutes % 1440 == 0 else f'{minutes // 60}h' if minutes % 60 == 0 else f'{minutes}m'


def period_shorts(calendars):
    years = {pid: {m['month'][:4] for m in months} for pid, months in calendars.items()}
    if all(len(y) == 1 for y in years.values()) and years['earlier'] != years['recent']:
        return {pid: next(iter(y)) for pid, y in years.items()}
    return dict(earlier='Earlier', recent='Recent')
