"""Native monthly facts and physically limited, immutable scorer observations."""
from dataclasses import asdict, dataclass
from types import MappingProxyType
import math

import numpy as np

from ..accounts.contracts import require, array
from ..account_study import load_account_study
from ..account_study.metrics import synchronized
from ..account_study.common import file_hash, json_text, parse_json, closed
from data_provider.manifest import to_epoch_ms
from ..study.contracts import semantic_digest
from .request import month_bounds

REJECTIONS = ('zeroQuantity','belowMinQuantity','belowMinNotional','leverageCapExceeded',
    'leverageUndefined','nonpositiveCapital','invalidRiskDistance')


def money(final, initial):
    """One operation order for ledger, scorers, rebuild and parent verification."""
    net = final-initial
    return net, (100.0*net)/initial


def admit_parent(doc, parent_root):
    parent = load_account_study(parent_root)
    study = parent.study
    require({**study['identities'],'completionSha256':file_hash(parent.root/'completion.json')} == doc['parent'], 'parent identity/completion pin')
    allocations = {a['allocationId']:a for a in study['request']['allocations']}
    require(doc['allocationId'] in allocations, 'unknown allocation')
    allocation = allocations[doc['allocationId']]
    require(allocation['policy']['initialCapital'] == doc['referenceCapital'], 'reference capital differs from parent')
    windows = {w['windowId']:w for w in study['request']['windows']}
    for w in doc['monthWindows']:
        require(w['windowId'] in windows, 'missing parent window')
        actual = windows[w['windowId']]
        require((to_epoch_ms(actual['startUtc'],'start'),to_epoch_ms(actual['endUtc'],'end')) == month_bounds(w['month']), 'parent window is not the declared whole calendar month')
    for ticker in study['request']['instruments']:
        for w in doc['monthWindows']: parent.key_for(ticker,doc['allocationId'],w['windowId'])
    return parent


def extract_ledger(parent, doc):
    rows, paths = [], {}
    roster = sorted(parent.study['request']['instruments'])
    for w in doc['monthWindows']:
        month_rows, contributions, times = [], [], None
        for ticker in roster:
            key = parent.key_for(ticker,doc['allocationId'],w['windowId'])
            record = parent.account(key); r = record.result
            c = asdict(r.coverage); observed = r.terminalStatus == 'closed' and c['observedBars'] > 0
            require(r.policy.initialCapital == doc['referenceCapital'], 'monthly capital mismatch')
            trades = r.trades.to_pydict(); attempts = r.attempts['reason'].to_pylist()
            net, ret = money(r.finalBalance,r.policy.initialCapital)
            forced = {reason:dict(count=sum(v == reason for v in trades['exitReason']),
                netPnl=math.fsum(p for p,v in zip(trades['netPnl'],trades['exitReason']) if v == reason)) for reason in ('gapBoundary','terminal')}
            rejected = {reason:attempts.count(reason) for reason in REJECTIONS}
            interior = c['missingBars']-c['missingHeadSlots']-c['missingTailSlots']
            row = dict(allocationId=doc['allocationId'],instrumentId=ticker,month=w['month'],windowId=w['windowId'],
                accountKey=key,accountId=r.identity,startMs=r.window.startMs,endMs=r.window.endMs,
                stepMs=record.prepared.stepMs,initialCapital=r.policy.initialCapital,finalBalance=r.finalBalance,
                netPnl=net,returnPct=ret if observed else None,trades=len(r.trades),
                fees=math.fsum([*trades['entryFee'],*trades['exitFee']]),forcedExits=forced,
                sizingRejections=rejected,sizingRejectionCount=sum(rejected.values()),
                terminalStatus=r.terminalStatus,available=observed,reason=None if observed else 'noObservedBars',
                **c,missingInteriorSlots=interior if interior >= 0 else None)
            month_rows.append(row)
            t, equity, _ = synchronized(record)
            require(times is None or np.array_equal(times,t), 'inconsistent monthly target timeframes')
            times = t; contributions.append(equity-r.policy.initialCapital)
        rows.extend(month_rows)
        paths[w['month']] = dict(times=array(times,np.int64),pnl=array(np.asarray(contributions),np.float64))
    return rows, paths, roster


def covered(row, minimum):
    return row['available'] and row['terminalStatus'] == 'closed' and row['observedBars'] > 0 and row['expectedBars'] > 0 and 100*row['observedBars'] >= minimum*row['expectedBars']


def history_pool(ledger, roster, months, minimum):
    lookup = {(r['instrumentId'],r['month']):r for r in ledger}
    eligible, exclusions = [], []
    for ticker in roster:
        failures = []
        for month in months:
            row = lookup.get((ticker,month))
            if row is None or not covered(row,minimum):
                failures.append(dict(month=month,reason='missingMonth' if row is None else
                    'noObservedBars' if not row['available'] else 'insufficientCoverage'))
        if failures: exclusions.append(dict(instrumentId=ticker,reasons=failures))
        else: eligible.append(ticker)
    return eligible, exclusions


def freeze(value):
    if isinstance(value,dict): return MappingProxyType({k:freeze(v) for k,v in value.items()})
    if isinstance(value,list): return tuple(freeze(v) for v in value)
    return value


@dataclass(frozen=True)
class NativeBars:
    timestamps: np.ndarray
    values: np.ndarray
    stepMs: int
    coverage: tuple


class ScoreView:
    """Owns only the admitted past. No reader, path or future backing buffer."""
    def __init__(self, instruments, months, cutoff, rows, bars=None):
        self.instruments = tuple(instruments)
        self.monthLabels = tuple(months)
        self.cutoffMs = cutoff
        self._rowsJson = json_text(rows)
        self._bars = bars or {}
        self._cache = {}

    def months(self, instrumentId):
        require(instrumentId in self.instruments, 'ticker outside score view')
        return tuple(freeze(r) for r in parse_json(self._rowsJson) if r['instrumentId'] == instrumentId)

    def bars(self, instrumentId):
        require(instrumentId in self.instruments and instrumentId in self._bars, 'native bars were not requested')
        if instrumentId not in self._cache:
            times, values, step, coverage = self._bars[instrumentId]
            self._cache[instrumentId] = NativeBars(np.frombuffer(times,np.int64),np.frombuffer(values,np.float64).reshape(-1,5),step,freeze(coverage))
        return self._cache[instrumentId]

    @property
    def fingerprint(self):
        import hashlib
        for name, value in self._cache.items():
            original = self._bars[name]
            require((value.timestamps.tobytes(),value.values.tobytes(),value.stepMs) == original[:3], 'mutated native bar view')
        return semantic_digest(dict(instruments=self.instruments,months=self.monthLabels,cutoff=self.cutoffMs,
            rows=parse_json(self._rowsJson),bars={k:[hashlib.sha256(v[0]).hexdigest(),hashlib.sha256(v[1]).hexdigest(),v[2],v[3]] for k,v in sorted(self._bars.items())}))


def score_view(parent, doc, ledger, instruments, months, cutoff, needs_bars):
    rows = [{k:v for k,v in r.items() if k not in ('accountId','accountKey')} for r in ledger if r['instrumentId'] in instruments and r['month'] in months]
    rows.sort(key=lambda r:(r['instrumentId'],r['month']))
    payload, dependencies = {}, []
    if needs_bars:
        first = next(w['windowId'] for w in doc['monthWindows'] if w['month'] == months[0])
        start = month_bounds(months[0])[0]
        for ticker in instruments:
            record = parent.account(parent.key_for(ticker,doc['allocationId'],first))
            target = record.prepared.target
            mask = (target.timestamps >= start) & (target.timestamps+target.stepMs <= cutoff)
            times = target.timestamps[mask].tobytes(); values = target.values[mask].astype(np.float64).tobytes()
            coverage = [{k:r[k] for k in ('month','observedBars','expectedBars','missingBars','missingHeadSlots','missingTailSlots')} for r in rows if r['instrumentId'] == ticker]
            payload[ticker] = times,values,target.stepMs,coverage
            import hashlib
            dependencies.append(dict(instrumentId=ticker,startMs=start,endMs=cutoff,stepMs=target.stepMs,
                rows=int(mask.sum()),timestampsSha256=hashlib.sha256(times).hexdigest(),valuesSha256=hashlib.sha256(values).hexdigest()))
    return ScoreView(instruments,months,cutoff,rows,payload), dependencies
