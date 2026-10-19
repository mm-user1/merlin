"""Observation-clock coverage, assumed publication lag, and causal scalar states."""
from bisect import bisect_right
from dataclasses import fields
import numpy as np

from ..accounts.contracts import require, integer, number, command_array_digest
from ..accounts.preparation import StateRows, align_states
from ..account_study.common import closed
from .model import content, fingerprint, covered, INTERVALS, validate_series


def validate_feature(f, series=None):
    closed(f, ('featureId', 'bindingId', 'transform', 'version', 'field', 'publicationLagMs', 'maxAgeMs', 'lookbackMinutes'))
    require(f['version'] == '1' and f['transform'] in ('auxiliaryLevel', 'auxiliaryChange'), 'auxiliary transform/version')
    integer(f['publicationLagMs'], 'publicationLagMs'); integer(f['maxAgeMs'], 'maxAgeMs', 1)
    require(f['maxAgeMs'] > f['publicationLagMs'], 'maxAge must exceed publication lag')
    if f['transform'] == 'auxiliaryChange': integer(f['lookbackMinutes'], 'lookbackMinutes', 1)
    else: require(f['lookbackMinutes'] is None, 'unused auxiliary lookback')
    if series is not None:
        t = validate_series(series)
        require(f['field'] in t.fields and f['transform'] in t.transformations, 'feature type/field/transform mismatch')
        if t.typeId == 'openInterest': require(f['field'] == 'singleOpenInterest', 'OI initially selects singleOpenInterest explicitly')
        if f['transform'] == 'auxiliaryChange':
            require(t.kind == 'sampled' and f['lookbackMinutes']*60000 % INTERVALS[series['interval']] == 0, 'lookback must use exact sample grid')


def slice_content(value, start, end):
    """Consume only the protocol-approved envelope, including missing-field masks."""
    coverage = [[max(a,start),min(b,end)] for a,b in value['coverage'] if max(a,start)<min(b,end)]
    return content(value['series'], [r for r in value['rows'] if start <= r['observedAtMs'] < end], coverage)


def feature_states(value, f, *, start, end, decision_end):
    validate_feature(f, value['series'])
    lag, age = f['publicationLagMs'], f['maxAgeMs']
    rows, coverage, series = value['rows'], value['coverage'], value['series']
    index = {r['observedAtMs']:r for r in rows}
    field = f['field']
    # Unknown wins at equal availability, including the final coverage edge.
    unknown = {}
    cursor = start
    for a,b in coverage:
        if cursor < a: unknown[cursor] = 'uncovered'
        unknown[b] = 'coverageEnd'
        cursor = b
    if not coverage: unknown[start] = 'uncovered'
    t = validate_series(series)
    if t.kind == 'sampled':
        step = INTERVALS[series['interval']]
        for stamp in range(((start+step-1)//step)*step, end, step):
            if stamp not in index: unknown[stamp] = 'missingSample'
    candidates = {}
    for row in rows:
        stamp, v = row['observedAtMs'], row[field]
        reason = None
        if v is None: reason = 'missingField'
        elif f['transform'] == 'auxiliaryChange':
            step = INTERVALS[series['interval']]; left = stamp-f['lookbackMinutes']*60000
            samples = [index.get(at) for at in range(left, stamp+1, step)]
            if any(r is None or r[field] is None or not covered(r['observedAtMs'], coverage) for r in samples): reason = 'incompleteLookback'
            elif samples[0][field] <= 0: reason = 'nonpositiveDenominator'
            else: v = v/samples[0][field]-1
        if reason is not None: unknown[stamp] = reason
        else: candidates[stamp] = (v, fingerprint(row))
    boundaries = sorted(unknown)
    states = []
    unusable = 0
    envelope_end = max(end+lag+age+1, decision_end+1)
    for stamp in sorted(set(candidates)|set(unknown)):
        available = stamp+lag
        if stamp in unknown:
            states.append((stamp, available, envelope_end, np.nan, False, unknown[stamp],
                           fingerprint(index[stamp]) if stamp in index else None))
            continue
        pos = bisect_right(boundaries, stamp)
        until = min(stamp+age, boundaries[pos]+lag if pos<len(boundaries) else envelope_end)
        if available >= until:
            unusable += 1
            states.append((stamp, available, envelope_end, np.nan, False, 'unusableValidity', None))
        else:
            v, rid = candidates[stamp]
            states.append((stamp, available, until, v, True, None, rid))
    arrays = [np.array([r[i] for r in states], dtype=bool if i==4 else float if i==3 else np.int64) for i in range(5)]
    sr = StateRows(*arrays, min([start]+[r[0] for r in states]), envelope_end)
    return sr, dict(reasons=[r[5] for r in states], sourceRecordIds=[r[6] for r in states],
        unusableRows=unusable, rawCoverage=coverage, consumedSliceFingerprint=fingerprint(value))


def build_feature(value, f, decisions, *, start, end):
    rows, facts = feature_states(value, f, start=start, end=end, decision_end=int(decisions[-1]) if len(decisions) else end)
    aligned = align_states(rows, decisions)
    arrays = {v.name:getattr(aligned,v.name) for v in fields(aligned)}
    meta = dict(version='auxiliary-feature.1', feature=f, **facts)
    return aligned, meta, command_array_digest(arrays, meta)


def event_buckets(value, *, field, widthMs, start, end, publicationLagMs, maxAgeMs):
    """Covered-empty is zero; unknown buckets replace rather than forward-fill."""
    t = validate_series(value['series'])
    require(t.kind == 'events' and (field == 'count' or field in t.fields), 'event bucket field')
    integer(widthMs, 'widthMs', 1); integer(publicationLagMs, 'publicationLagMs')
    integer(maxAgeMs, 'maxAgeMs', publicationLagMs+1)
    require(start < end and start % widthMs == end % widthMs == 0, 'bucket bounds')
    records = value['rows']; columns = [[] for _ in range(5)]
    for left in range(start, end, widthMs):
        right = left+widthMs
        known = any(a<=left and b>=right for a,b in value['coverage'])
        selected = [r for r in records if left<=r['observedAtMs']<right]
        known &= field == 'count' or all(r[field] is not None for r in selected)
        v = (len(selected) if field == 'count' else sum(r[field] for r in selected)) if known else np.nan
        row = (right, right+publicationLagMs, min(right+maxAgeMs,right+widthMs+publicationLagMs),v,known)
        for col,x in zip(columns,row): col.append(x)
    return StateRows(*(np.array(c,dtype=bool if i==4 else float if i==3 else np.int64) for i,c in enumerate(columns)),
                     start,end+widthMs+publicationLagMs)


def condition(value, rule):
    if rule is None: return np.zeros(len(value),bool)
    closed(rule, ('operator', 'threshold'))
    require(rule['operator'] in ('gt','lt'), 'strict scalar condition')
    number(rule['threshold'], 'threshold', minimum=-float('inf'))
    return value > rule['threshold'] if rule['operator']=='gt' else value < rule['threshold']


def transition_pulse(truth, known, segment_start):
    pulse = np.zeros(len(truth),bool)
    pulse[1:] = truth[1:] & ~truth[:-1] & known[1:] & known[:-1] & ~segment_start[1:]
    return pulse
