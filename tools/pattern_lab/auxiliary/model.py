"""Explicit typed auxiliary observations. No exchange-specific trading semantics."""
from dataclasses import dataclass, asdict
import math
import re
from bisect import bisect_left

import pyarrow as pa
import numpy as np

from ..accounts.contracts import require, identifier, integer, FrozenArrays, array
from ..account_study.common import closed, json_text, parse_json
from ..study.contracts import semantic_digest

INTERVALS = {'5min': 300000, '15min': 900000, '30min': 1800000,
             '1h': 3600000, '4h': 14400000, '1d': 86400000}


@dataclass(frozen=True)
class ConsumedSlice(FrozenArrays):
    """Owned read-only columns, retaining immutability through spawn pickling."""
    seriesJson: str
    coverageJson: str
    observedAtMs: np.ndarray
    values: np.ndarray
    eventIds: tuple

    def __post_init__(self):
        object.__setattr__(self,'observedAtMs',array(self.observedAtMs,np.int64))
        object.__setattr__(self,'values',array(self.values,np.float64))

    @classmethod
    def from_content(cls,value):
        names=value['series']['type']['fields'];rows=value['rows']
        values=np.array([[np.nan if r[f] is None else r[f] for f in names] for r in rows],dtype=np.float64).reshape(len(rows),len(names))
        return cls(json_text(value['series']),json_text(value['coverage']),
                   np.array([r['observedAtMs'] for r in rows],dtype=np.int64),values,
                   tuple(r.get('eventId') for r in rows))

    @property
    def document(self):
        series=parse_json(self.seriesJson);names=series['type']['fields'];rows=[]
        for i,stamp in enumerate(self.observedAtMs):
            row=dict(observedAtMs=int(stamp))
            row.update({f:None if np.isnan(self.values[i,j]) else float(self.values[i,j]) for j,f in enumerate(names)})
            if self.eventIds[i] is not None:row['eventId']=self.eventIds[i]
            rows.append(row)
        return dict(series=series,rows=rows,coverage=parse_json(self.coverageJson))


@dataclass(frozen=True)
class TypeDescriptor:
    typeId: str
    version: str
    kind: str
    fields: tuple
    definition: str
    timeSemantics: str
    coverageSemantics: str
    transformations: tuple
    nonnegative: bool = False
    population: str | None = None

    def document(self):
        d = asdict(self)
        d['fields'], d['transformations'] = list(self.fields), list(self.transformations)
        d['schema'] = 'pattern-lab.auxiliary-type.v1'
        d['recordKey'] = ['eventId'] if self.kind == 'events' else ['observedAtMs']
        return d

    def arrow_schema(self):
        return pa.schema([pa.field('observedAtMs', pa.int64(), nullable=False)] +
            ([pa.field('eventId', pa.string(), nullable=False)] if self.kind == 'events' else []) +
            [pa.field(f, pa.float64()) for f in self.fields])


TYPES = {
    'settledFunding': TypeDescriptor('settledFunding', '1', 'irregular', ('fundingRate',),
        'Signed settled rate per settlement; not annualized or normalized by interval',
        'Settlement timestamp; historical publication time is unmeasured',
        'Successful retrieval spans; not proof of publication completeness', ('auxiliaryLevel',)),
    'openInterest': TypeDescriptor('openInterest', '1', 'sampled', ('singleOpenInterest', 'openInterest'),
        'Native base units: singleOpenInterest one-sided; openInterest two-sided',
        'Epoch-aligned observation; publication lag is an assumption',
        'Successful retrieval spans with explicit missing expected sample slots',
        ('auxiliaryLevel', 'auxiliaryChange'), True),
}


def descriptor(value):
    closed(value, ('typeId', 'version', 'kind', 'fields', 'definition', 'timeSemantics',
                   'coverageSemantics', 'transformations', 'nonnegative', 'population', 'schema', 'recordKey'))
    expected = TYPES.get(value['typeId'])
    require(expected is not None and expected.document() == value, 'unregistered or changed auxiliary type')
    return expected


def validate_series(s):
    closed(s, ('seriesId', 'provider', 'providerVersion', 'venue', 'contract', 'market',
               'baseUnit', 'baseMultiplier', 'interval', 'type', 'units'))
    for k in ('seriesId', 'provider', 'providerVersion', 'venue'):
        identifier(s[k])
    from ..account_study.common import relative_name
    relative_name(s['seriesId'])
    for k in ('contract', 'baseUnit'):
        require(isinstance(s[k],str) and re.fullmatch(r'[A-Za-z0-9_.-]+',s[k]), 'source '+k)
    t = descriptor(s['type'])
    require(s['market'] == 'linearUSDTPerpetual', 'unsupported auxiliary market')
    require(type(s['baseMultiplier']) in (int, float) and math.isfinite(s['baseMultiplier'])
            and s['baseMultiplier'] > 0, 'explicit positive baseMultiplier')
    require(type(s['units']) is dict and set(s['units']) == set(t.fields) and
            all(isinstance(v, str) and v for v in s['units'].values()), 'explicit field units')
    if t.typeId in ('settledFunding','openInterest'):
        unit = 'ratePerSettlement' if t.typeId == 'settledFunding' else s['baseUnit']
        require(all(v == unit for v in s['units'].values()), 'native auxiliary units')
    require(s['interval'] in INTERVALS if t.kind == 'sampled' else s['interval'] is None, 'series sampling interval')
    return t


def spans(value):
    require(type(value) is list, 'coverage spans list')
    result = []
    for span in sorted(value):
        require(type(span) in (list, tuple) and len(span) == 2, 'coverage span')
        a, b = span
        integer(a, 'coverage start', 0); integer(b, 'coverage end', 1)
        require(a < b, 'empty coverage span')
        if result and a <= result[-1][1]: result[-1][1] = max(b, result[-1][1])
        else: result.append([a, b])
    return result


def covered(t, coverage):
    return any(a <= t < b for a, b in coverage)


def normalize_rows(series, rows):
    t = validate_series(series)
    out = {}
    for row in rows:
        closed(row, ('observedAtMs', *t.fields, *(['eventId'] if t.kind == 'events' else [])))
        stamp = integer(row['observedAtMs'], 'observedAtMs', 0)
        if t.kind == 'sampled': require(stamp % INTERVALS[series['interval']] == 0, 'off-grid auxiliary sample')
        if t.kind == 'events': require(isinstance(row['eventId'],str) and 0 < len(row['eventId']) <= 256, 'event ID')
        normalized = dict(row)
        for f in t.fields:
            v = row[f]
            require(v is None or (type(v) in (float, int) and math.isfinite(v) and
                    (not t.nonnegative or v >= 0)), 'invalid auxiliary field '+f)
            normalized[f] = None if v is None else float(v)
        key = (row['eventId'],) if t.kind == 'events' else (stamp,)
        require(key not in out or out[key] == normalized, 'conflicting auxiliary observation')
        out[key] = normalized
    return sorted(out.values(), key=lambda r:(r['observedAtMs'],r.get('eventId','')))


def content(series, rows, coverage):
    rows, coverage = normalize_rows(series, rows), spans(coverage)
    require(all(covered(r['observedAtMs'], coverage) for r in rows), 'observation outside retrieval coverage')
    return dict(series=series, rows=rows, coverage=coverage)


def fingerprint(value):
    return semantic_digest(value)


def inspect_content(value):
    s, rows, coverage = value['series'], value['rows'], value['coverage']
    t = validate_series(s)
    stamps = [r['observedAtMs'] for r in rows]
    spacing = {}
    for a, b in zip(stamps, stamps[1:]): spacing[str(b-a)] = spacing.get(str(b-a), 0)+1
    missing = None
    missing_ranges = []
    if t.kind == 'sampled':
        step = INTERVALS[s['interval']]
        expected = sum((b-1)//step - (a+step-1)//step + 1 for a, b in coverage)
        missing = expected-len(rows)
        for a,b in coverage:
            cursor=((a+step-1)//step)*step
            for stamp in stamps[bisect_left(stamps,a):bisect_left(stamps,b)]:
                if cursor<stamp:missing_ranges.append([cursor,stamp])
                cursor=stamp+step
            if cursor<b:missing_ranges.append([cursor,b])
    return dict(records=len(rows), firstObservedAtMs=stamps[0] if stamps else None,
        lastObservedAtMs=stamps[-1] if stamps else None, coverage=coverage,
        missingFields={f:sum(r[f] is None for r in rows) for f in t.fields}, missingSampleSlots=missing,
        missingSampleRanges=missing_ranges,
        observedSpacingMs=spacing, spacingMeaning='Observed adjacent spacing, not a known historical schedule',
        historyCompleteness='Unproven; successful retrieval is not measured historical publication')
