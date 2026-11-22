"""Closed two-period requests and sparse native child projection."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import math

from data_provider.manifest import to_epoch_ms, format_epoch_ms, normalize_timeframe_minutes
from ..accounts.contracts import require, identifier, integer
from ..account_study.common import closed, json_text, parse_json, read_json
from ..account_study.request import normalize_request as native_request
from ..configuration_study.request import owned, configuration_payload, numeric_key, MAX_JSON
from ..configuration_study.codec import MAX_MEMBER, MAX_TOTAL, MAX_MEMBERS
from ..study.contracts import semantic_digest

SCHEMA = 'pattern-lab.strategy-screen-request.v1'
PERIODS = ('earlier', 'recent')
MAX_MONTHS = MAX_MEMBER // 1024


def month_boundary(value):
    stamp = to_epoch_ms(value, 'month boundary')
    date = datetime.fromtimestamp(stamp / 1000, timezone.utc)
    require((date.day, date.hour, date.minute, date.second, date.microsecond) == (1, 0, 0, 0, 0),
            'whole calendar month boundary required')
    return stamp


def calendar(period):
    start, end = (month_boundary(period[k]) for k in ('startUtc', 'endUtc'))
    left = datetime.fromtimestamp(start / 1000, timezone.utc)
    right = datetime.fromtimestamp(end / 1000, timezone.utc)
    count = (right.year-left.year)*12+right.month-left.month
    require(0 < count <= MAX_MONTHS, 'calendar capacity/duration')
    rows = []
    for _ in range(count):
        nxt = datetime(left.year+(left.month == 12), left.month % 12+1, 1, tzinfo=timezone.utc)
        rows.append(dict(month=left.strftime('%Y-%m'), startMs=int(left.timestamp()*1000),
                         endMs=int(nxt.timestamp()*1000)))
        left = nxt
    return rows


def child_request(doc, cell, period):
    selected = set(cell['allocationIds'])
    allocations = [a for a in doc['allocations'] if a['allocationId'] in selected]
    used = {f['inputId'] for a in allocations for f in a['features']}
    child = dict(schema='pattern-lab.account-study-request.v2' if 'auxiliaryInputs' in doc else
                 'pattern-lab.account-study-request.v1', studyId=doc['studyId'],
                 instruments=[cell['instrumentId']], allocations=allocations,
                 sources=[s for s in doc['sources'] if s['inputId'] in used],
                 protocol=period['protocol'], preparationStartUtc=period['preparationStartUtc'],
                 windows=[dict(windowId=period['periodId'], startUtc=period['startUtc'], endUtc=period['endUtc'])])
    if 'auxiliaryInputs' in doc:
        used_aux = {f['bindingId'] for a in allocations for f in a['auxiliaryFeatures']}
        child['auxiliaryInputs'] = [dict(b, sources=b['sources'] if '*' in b['sources'] else
             {cell['instrumentId']: b['sources'][cell['instrumentId']]})
             for b in doc['auxiliaryInputs'] if b['inputId'] in used_aux]
    return deepcopy(child)


def cell_id(instrument, timeframe):
    return semantic_digest([instrument, timeframe])


def configurations(doc):
    allocations = {a['allocationId']: a for a in doc['allocations']}
    rows = []
    for cell in doc['cells']:
        seen = set()
        child = child_request(doc, cell, doc['periods'][0])
        for aid in cell['allocationIds']:
            payload = configuration_payload(child, cell['instrumentId'], allocations[aid])
            key = numeric_key(payload)
            require(key not in seen, 'equivalent settings in a cell'); seen.add(key)
            rows.append(dict(configurationId=semantic_digest(payload), allocationId=aid,
                instrumentId=cell['instrumentId'], timeframeMinutes=cell['timeframeMinutes'],
                cellId=cell_id(cell['instrumentId'], cell['timeframeMinutes'])))
    return sorted(rows, key=lambda r: (r['instrumentId'], r['timeframeMinutes'], r['configurationId']))


def result_id(configuration, period):
    return semantic_digest(dict(schema='strategy-screen-result.v1', configurationId=configuration,
        periodId=period['periodId'], startMs=month_boundary(period['startUtc']), endMs=month_boundary(period['endUtc'])))


def effective_criteria(doc, timeframe):
    c = doc['criteria']
    return {k: c[k] for k in ('annualizedProfitPct', 'mtmMaxDdPct')} | dict(
        minClosedTrades=c.get('minClosedTradesByTimeframe', {}).get(str(timeframe), c['minClosedTrades']))


@dataclass(frozen=True)
class StrategyScreenRequest:
    documentJson: str

    @property
    def document(self):
        return parse_json(self.documentJson)

    @property
    def identity(self):
        doc = self.document; doc.pop('notes', None)
        for p in doc['periods']: p['protocol'].pop('notes', None)
        return semantic_digest(doc)


def normalize_request(value):
    if isinstance(value, StrategyScreenRequest): value = value.document
    doc, base = owned(value)
    closed(doc, ('schema', 'studyId', 'allocations', 'sources', 'cells', 'periods', 'criteria'),
           ('segments', 'previousReviewEndUtc', 'auxiliaryInputs', 'notes'))
    require(doc['schema'] == SCHEMA, 'screen request schema'); identifier(doc['studyId'])
    require(doc.get('notes') is None or type(doc['notes']) is str, 'notes')
    for key in ('allocations', 'cells', 'sources'):
        require(type(doc[key]) is list and (doc[key] or key == 'sources'), key+' list')
    allocations = {}
    for a in doc['allocations']:
        require(type(a) is dict, 'allocation object'); identifier(a.get('allocationId'))
        require(a['allocationId'] not in allocations, 'duplicate allocation')
        a['timeframeMinutes'] = normalize_timeframe_minutes(a.get('timeframeMinutes'))
        require(type(a.get('features')) is list, 'features list')
        require(all(type(f) is dict for f in a['features']), 'feature objects')
        if 'auxiliaryInputs' in doc: require(type(a.get('auxiliaryFeatures')) is list, 'v2 auxiliaryFeatures list')
        require(all(type(f) is dict for f in a.get('auxiliaryFeatures',[])), 'auxiliary feature objects')
        allocations[a['allocationId']] = a
    aliases, equivalent = set(), set()
    for s in doc['sources']:
        closed(s, ('inputId', 'instrumentId', 'timeframeMinutes')); identifier(s['inputId']); identifier(s['instrumentId'])
        require(s['inputId'] != 'target' and s['inputId'] not in aliases, 'duplicate/reserved source alias')
        s['timeframeMinutes'] = normalize_timeframe_minutes(s['timeframeMinutes'])
        pair = (s['instrumentId'], s['timeframeMinutes'])
        require(pair not in equivalent, 'equivalent candle declarations')
        aliases.add(s['inputId']); equivalent.add(pair)
    used, cells, targets = set(), set(), set()
    for cell in doc['cells']:
        closed(cell, ('instrumentId', 'timeframeMinutes', 'allocationIds')); identifier(cell['instrumentId'])
        require(cell['instrumentId'] != 'target', 'reserved target identity')
        cell['timeframeMinutes'] = normalize_timeframe_minutes(cell['timeframeMinutes'])
        pair = (cell['instrumentId'], cell['timeframeMinutes'])
        require(pair not in cells, 'duplicate cell'); cells.add(pair); targets.add(cell['instrumentId'])
        ids = cell['allocationIds']; require(type(ids) is list and ids, 'cell allocations')
        for aid in ids: identifier(aid)
        require(len(set(ids)) == len(ids) and set(ids) <= allocations.keys(), 'cell allocation references')
        require(all(allocations[aid]['timeframeMinutes'] == cell['timeframeMinutes'] for aid in ids), 'cell timeframe mismatch')
        used.update(ids); ids.sort()
    require(used == allocations.keys(), 'unused allocation')
    require({f.get('inputId') for a in allocations.values() for f in a['features']} - {'target'} == aliases,
            'unused/unknown candle binding')
    if 'auxiliaryInputs' in doc:
        from ..auxiliary.request import normalize_bindings
        normalize_bindings(dict(doc, instruments=sorted(targets)))
        require({f.get('bindingId') for a in allocations.values() for f in a['auxiliaryFeatures']} ==
                {b['inputId'] for b in doc['auxiliaryInputs']}, 'unused/unknown auxiliary binding')
        pins = {}
        for b in doc['auxiliaryInputs']:
            require(pins.setdefault(b['snapshotId'], b['expectedSnapshotContentId']) == b['expectedSnapshotContentId'],
                    'inconsistent shared snapshot pin')
    require(type(doc['periods']) is list and len(doc['periods']) == 2, 'exactly two periods')
    previous = None; normalized = {}
    for role, p in zip(PERIODS, doc['periods']):
        closed(p, ('periodId', 'startUtc', 'endUtc', 'preparationStartUtc', 'protocol'))
        require(p['periodId'] == role, 'logical period order')
        months = calendar(p)
        require(previous is None or previous <= months[0]['startMs'], 'period overlap/order')
        previous = months[-1]['endMs']
        if isinstance(p['protocol'], str): p['protocol'] = read_json(base/p['protocol'])
        for cell in doc['cells']:
            child = native_request(child_request(doc, cell, p)).document
            p.update(protocol=child['protocol'], preparationStartUtc=child['preparationStartUtc'],
                     startUtc=child['windows'][0]['startUtc'], endUtc=child['windows'][0]['endUtc'])
            for a in child['allocations']:
                aid = a['allocationId']
                require(normalized.setdefault(aid, a) == a, 'allocation normalization differs across children')
    doc['allocations'] = [normalized[k] for k in sorted(normalized)]
    doc['cells'].sort(key=lambda c: (c['instrumentId'], c['timeframeMinutes']))
    doc['sources'].sort(key=lambda s: s['inputId'])
    require(len({a['policy']['initialCapital'] for a in doc['allocations']}) == 1, 'shared initial capital')
    c = doc['criteria']; closed(c, ('annualizedProfitPct', 'mtmMaxDdPct', 'minClosedTrades'), ('minClosedTradesByTimeframe',))
    for k in ('annualizedProfitPct', 'mtmMaxDdPct'):
        require(type(c[k]) in (int, float) and math.isfinite(c[k]), 'finite criterion')
    require(c['mtmMaxDdPct'] >= 0, 'nonnegative DD criterion'); integer(c['minClosedTrades'], 'trade threshold')
    overrides = c.get('minClosedTradesByTimeframe', {}); require(type(overrides) is dict, 'timeframe overrides')
    known = {str(tf) for _, tf in cells}
    for key, threshold in overrides.items():
        require(key in known, 'canonical declared timeframe key'); integer(threshold, 'trade override')
    c['minClosedTradesByTimeframe'] = dict(sorted(overrides.items()))
    doc.setdefault('segments', []); require(type(doc['segments']) is list, 'segments list'); ids = set()
    periods = {p['periodId']: p for p in doc['periods']}
    for s in doc['segments']:
        closed(s, ('segmentId', 'periodId', 'startUtc', 'endUtc')); identifier(s['segmentId'])
        require(s['segmentId'] not in ids and s['periodId'] in PERIODS, 'segment identity'); ids.add(s['segmentId'])
        lo, hi = month_boundary(s['startUtc']), month_boundary(s['endUtc']); p = periods[s['periodId']]
        require(month_boundary(p['startUtc']) <= lo < hi <= month_boundary(p['endUtc']), 'segment interval')
        s.update(startUtc=format_epoch_ms(lo), endUtc=format_epoch_ms(hi))
    doc.setdefault('previousReviewEndUtc', None)
    if doc['previousReviewEndUtc'] is not None:
        cutoff = month_boundary(doc['previousReviewEndUtc']); p = periods['recent']
        require(month_boundary(p['startUtc']) < cutoff < month_boundary(p['endUtc']), 'cutoff inside recent')
        doc['previousReviewEndUtc'] = format_epoch_ms(cutoff)
    count = sum(len(c['allocationIds']) for c in doc['cells'])
    require(2*((count+15)//16)+len(cells)+len(targets)+len(known)+1 <= MAX_MEMBERS, 'planned member bound')
    month_count = sum(len(calendar(p)) for p in doc['periods'])
    require(count*month_count*128 <= MAX_TOTAL, 'planned monthly resource bound')
    rows = configurations(doc)
    require(len(json_text(dict(request=doc,configurations=rows)).encode()) <= MAX_JSON, 'planned metadata bound')
    return StrategyScreenRequest(json_text(doc))
