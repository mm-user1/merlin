"""Closed, portable selection requests; no recorded code is imported."""
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import math
import re

from ..accounts.contracts import require, identifier, integer, number
from ..account_study.common import closed, read_json, json_text, parse_json, digest_id
from ..study.contracts import semantic_digest

SCHEMA = 'pattern-lab.selection-request.v1'


def strict_json(value):
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float:
        require(math.isfinite(value), 'nonfinite JSON value'); return
    if type(value) is list:
        for item in value: strict_json(item)
        return
    require(type(value) is dict and all(type(k) is str for k in value), 'strict JSON value')
    for item in value.values(): strict_json(item)


def month_bounds(label):
    require(type(label) is str and re.fullmatch(r'[0-9]{4}-(0[1-9]|1[0-2])', label), 'UTC YYYY-MM required')
    year, month = map(int, label.split('-'))
    require(1 <= year <= 9998, 'month year')
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year+(month == 12), month % 12+1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()*1000), int(end.timestamp()*1000)


def consecutive(labels):
    require(labels and len(labels) == len(set(labels)), 'empty/duplicate months')
    spans = [month_bounds(m) for m in labels]
    require(all(a[1] == b[0] for a,b in zip(spans,spans[1:])), 'months must be contiguous')


@dataclass(frozen=True)
class SelectionRequest:
    documentJson: str

    @property
    def document(self): return parse_json(self.documentJson)

    @property
    def identity(self):
        doc = self.document; doc.pop('notes', None)
        return semantic_digest(doc)


def normalize_selection_request(value, *, scorers=None, stored=False):
    if isinstance(value, SelectionRequest): value = value.document
    if isinstance(value, (str, Path)): value = read_json(value)
    strict_json(value)
    doc = parse_json(json_text(value))
    closed(doc, ('schema','selectionId','parent','allocationId','monthWindows','evaluationMonths',
        'pool','selectors','kValues','capitalMode','referenceCapital','randomControl'), ('notes',))
    require(doc['schema'] == SCHEMA, 'selection request schema')
    identifier(doc['selectionId']); identifier(doc['allocationId'])
    require('notes' not in doc or type(doc['notes']) is str, 'notes must be text')
    closed(doc['parent'], ('specification','dataInput','implementation','completionSha256'))
    for v in doc['parent'].values(): digest_id(v)
    require(doc['capitalMode'] == 'fixedReference', 'only fixedReference is supported')
    doc['referenceCapital'] = number(doc['referenceCapital'], 'referenceCapital')
    closed(doc['pool'], ('historyMonths','minObservedBarsPct'))
    history = integer(doc['pool']['historyMonths'], 'historyMonths', 1)
    doc['pool']['minObservedBarsPct'] = number(doc['pool']['minObservedBarsPct'], 'minObservedBarsPct')
    require(doc['pool']['minObservedBarsPct'] <= 100, 'coverage exceeds 100%')
    closed(doc['randomControl'], ('seed','replicates'))
    integer(doc['randomControl']['seed'], 'seed'); integer(doc['randomControl']['replicates'], 'replicates', 1)
    for name in ('monthWindows','evaluationMonths','selectors','kValues'):
        require(type(doc[name]) is list and doc[name], name+' must be nonempty list')
    for w in doc['monthWindows']:
        closed(w, ('windowId','month')); identifier(w['windowId']); month_bounds(w['month'])
    require(len({w['windowId'] for w in doc['monthWindows']}) == len(doc['monthWindows']), 'duplicate window ID')
    doc['monthWindows'].sort(key=lambda w:w['month'])
    months = [w['month'] for w in doc['monthWindows']]; consecutive(months)
    for m in doc['evaluationMonths']: month_bounds(m)
    doc['evaluationMonths'].sort(); consecutive(doc['evaluationMonths'])
    require(set(doc['evaluationMonths']) <= set(months), 'evaluation outside declared months')
    require(months.index(doc['evaluationMonths'][0]) >= history, 'insufficient declared history months')
    for k in doc['kValues']: integer(k, 'K', 1)
    require(len(set(doc['kValues'])) == len(doc['kValues']), 'duplicate K')
    doc['kValues'].sort()
    from .scorers import registry, contract
    registered = registry(scorers)
    ids, semantics = set(), set()
    for s in doc['selectors']:
        closed(s, ('selectorId','scorerId','version','parameters'))
        identifier(s['selectorId']); identifier(s['scorerId'])
        require(type(s['version']) is str and s['version'], 'scorer version')
        require(s['selectorId'] not in ids, 'duplicate selector ID'); ids.add(s['selectorId'])
        require(type(s['parameters']) is dict, 'scorer parameters object')
        if stored and s['scorerId'].startswith('x.'):
            strict_json(s['parameters'])
        else:
            scorer = registered.get(s['scorerId'])
            require(scorer is not None and scorer.version == s['version'], 'unregistered scorer/version')
            s['parameters'] = scorer.normalizeParameters(s['parameters'])
            require(type(s['parameters']) is dict, 'normalized parameters object'); strict_json(s['parameters'])
            require(contract(scorer,s['parameters'])['lookbackMonths'] <= history, 'history shorter than scorer lookback')
        key = semantic_digest({k:v for k,v in s.items() if k != 'selectorId'})
        require(key not in semantics, 'duplicate semantic selector'); semantics.add(key)
    doc['selectors'].sort(key=lambda s:s['selectorId'])
    return SelectionRequest(json_text(doc))
