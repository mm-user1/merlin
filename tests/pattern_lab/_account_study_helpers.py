"""Native synthetic packs and requests for the account-study integration."""
from dataclasses import replace
from copy import deepcopy
import numpy as np

from data_provider.manifest import format_epoch_ms
from tools.pattern_lab.accounts import execute_account, AccountWindow
from tools.pattern_lab.account_study.common import write_json, file_hash
from ._helpers import ANCHOR_MS, instrument_source, publish
from ._bracket_helpers import rule_entry

STEP = 1800000


def build_pack(root, *, count=2, gaps=False, slots=96):
    symbols = ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF'][:count]
    sources = []
    for j, symbol in enumerate(symbols+['BTC']):
        indices = np.arange(slots*6, dtype=np.int64)
        if gaps and symbol != 'BTC': indices = indices[indices//6 != 40]
        t = ANCHOR_MS+indices*300000
        wave = 3*np.sin(indices/18+j/3)
        op = 100+j*5+wave
        cl = op+np.cos(indices/18+j/3)*.4
        values = np.column_stack((op, np.maximum(op,cl)+.6, np.minimum(op,cl)-.6, cl, 1000+indices/7))
        raw = deepcopy(rule_entry('BYBIT')['instrument_rules'])
        raw.update(base_currency=symbol, quantity_unit=symbol)
        raw['raw_contract_fields']['baseCoin'] = symbol
        source = instrument_source(t, values, symbol=symbol, venue='BYBIT', contract=symbol+'USDT',
                                   roles=['factor'] if symbol == 'BTC' else ['trading'])
        sources.append(replace(source, instrument_rules=raw))
    publish(root, sources)
    return ['BYBIT_'+symbol+'USDT' for symbol in symbols]


def request(instruments, *, filtered=True, slots=96, multiple=True, own_source=False):
    utc = lambda n: format_epoch_ms(ANCHOR_MS+n*STEP)
    ha = dict(requestId='ha', featureId='ha', version='1', inputId='target', parameters={}, priorBars=2)
    atr = dict(requestId='atr', featureId='atr', version='1', inputId='target', parameters=dict(length=3), priorBars=0)
    source_alias = 'own4h' if own_source else 'btc4h'
    state = dict(requestId='state', featureId='candleDirection', version='1', inputId=source_alias,
                 parameters=dict(candleType='real', confirmationBars=1), priorBars=0)
    allocation = dict(allocationId='rr2', timeframeMinutes=30, features=[ha],
        provider=dict(providerId='haRun', version='1', recipe=dict(haRequestId='ha', entryRunLength=2)),
        entryFilter=None, policy=dict(mode='fixedBracket', direction='both', rewardRisk=2., stopUpdates=False, maxHoldingDays=None))
    allocations = [allocation]
    sources = []
    if filtered:
        allocation['features'].append(state)
        allocation['entryFilter'] = dict(providerId='candleDirection', version='1', requestId='state')
        sources.append(dict(inputId=source_alias, instrumentId='target' if own_source else 'BYBIT_BTCUSDT', timeframeMinutes=240))
    if multiple:
        rr = deepcopy(allocation); rr['allocationId']='rr3'; rr['policy']['rewardRisk']=3.
        managed = deepcopy(allocation); managed['allocationId']='trailing'
        managed['policy'].update(mode='signalManaged', rewardRisk=None, stopUpdates=True)
        managed['features'].append(atr)
        managed['provider']['recipe'].update(stopProvider='realSwingAtr', atrRequestId='atr', swingBars=2, atrMultiplier=1.)
        allocations.extend((rr, managed))
    return dict(schema='pattern-lab.account-study-request.v1', studyId='synthetic',
        protocol=dict(schema_version=1, protocol_id='synthetic',
            development=dict(start_utc=utc(16), end_utc=utc(slots)),
            reserved=dict(start_utc=utc(slots), end_utc=utc(slots+48)), earliest_warmup_start_utc=utc(0)),
        preparationStartUtc=utc(0), instruments=instruments, sources=sources,
        windows=[dict(windowId='full', startUtc=utc(16), endUtc=utc(slots)),
                 dict(windowId='overlap', startUtc=utc(32), endUtc=utc(slots))], allocations=allocations)


def assert_filtered_trades(reader):
    """Every allocation/window must exercise fills, not just equal empty tables."""
    counts = {}
    reasons = set()
    for key in reader.accountKeys:
        record = reader.account(key); result = record.result
        assert result.commands.entryFilter is not None
        group = (record.labels['allocationId'], record.labels['windowId'])
        row = counts.setdefault(group, dict(trades=0, filledFilteredAttempts=0, proposals=0))
        row['trades'] += len(result.trades)
        row['proposals'] += len(result.proposals)
        fills = []
        for attempt in result.attempts.to_pylist():
            reasons.add(attempt['reason'])
            if attempt['reason'] != 'filled': continue
            assert attempt['filterKnown'] and attempt['filterAllowed']
            assert attempt['sourceIndex'] >= 0 and attempt['sourceReason'] == 0
            assert attempt['observedAtMs'] <= attempt['availableAtMs'] <= attempt['decisionMs'] < attempt['validUntilMs']
            fills.append(attempt['tradeId'])
        assert set(fills) == set(result.trades['tradeId'].to_pylist())
        row['filledFilteredAttempts'] += len(fills)
    doc = reader.study['request']
    assert len(reader.accountKeys) == len(doc['instruments'])*len(doc['allocations'])*len(doc['windows'])
    for allocation in doc['allocations']:
        for window in doc['windows']:
            row = counts[allocation['allocationId'], window['windowId']]
            assert row['trades'] > 0 and row['filledFilteredAttempts'] == row['trades'], (allocation, window, row)
            if allocation['policy']['stopUpdates']: assert row['proposals'] > 0
    assert 'filterBlocked' in reasons
    return counts


def reseal(root):
    """Test-only coherent seal rewrite: distinguishes structural from full checks."""
    import json
    path=root/'completion.json'
    seal=json.loads(path.read_text())
    for name in seal['members']:
        p=root/name; seal['members'][name]=dict(bytes=p.stat().st_size, sha256=file_hash(p))
    write_json(path, seal)
