"""Portable canonical tables and offline semantic checks; no execution imports."""
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..analysis.research_inputs import _read, _keys, digest
from ..dataset import atomic_write, artifact_record, validate_artifact_record
from ..fixed.storage import safe_path
from ..selection.history_plan import checked_input
from ..selection.ranking import require
from .plan import open_plan
from .accounting import finite_sum, reconcile_trades, validate_path

SCHEMA = 'strategy_lab_portfolio_result_v1'
ALLOCATION_COLUMNS = 'variant period window instrument rank allocated opening closing status reason allocation_rounding_delta candidate_id source_account_id trade_count fees net_pnl actual_fingerprint actual_semantic_key reference_fingerprint reference_semantic_key reference_capital diagnostics'.split()
TRADE_COLUMNS = 'variant period window rank instrument trade direction entry_time exit_time entry_price exit_price size net_pnl profit_pct side exit_reason entry_fee exit_fee'.split()
LIMITATIONS = [
    'Research-only actual_carried_execution; independent_reference_returns use the same frozen membership and denominator.',
    'Reference returns are not executed money; a rerank-every-OOS headline is not equivalent to held membership.',
    'Off/F06 differences with different memberships are whole-system comparisons.',
    'Lot-floor, minimum-quantity and minimum-notional skips are not fully counted: zero diagnostic counters do not imply no skipped entries. Unsupported reason counts are unavailable.',
    'Compact curves are not lossless. Exact drawdowns are execution facts; full verification requires detailed replay.',
    'Close-labelled aggregate path DD differs from trade-level realized DD, intrabar excursion DD and production depleted-index availability.',
    'Trade-to-balance tolerance is 1e-10 + 1e-12*max(abs(opening),abs(closing),fsum(abs(trade.net_pnl))); growth-product tolerance is rel/abs 1e-12. Funding and detail reproduction are exact.',
]


def write_table(root, name, rows, columns):
    rows = [{k: json.dumps(r[k], sort_keys=True, allow_nan=False) if k == 'diagnostics' else r[k] for k in columns} for r in rows]
    table = pa.Table.from_pylist(rows) if rows else pa.table({k: pa.array([], type=pa.string()) for k in columns})
    atomic_write(safe_path(root, name), lambda path: pq.write_table(table, path, compression='zstd'))


def write_curve(root, name, ts, balance, equity):
    table = pa.table(dict(timestamp_ns=pa.array(ts, type=pa.int64()), balance=pa.array(balance, type=pa.float64()), equity=pa.array(equity, type=pa.float64())))
    atomic_write(safe_path(root, name), lambda path: pq.write_table(table, path, compression='zstd'))


def read_table(root, name, columns):
    table = pq.read_table(safe_path(root, name))
    require(table.column_names == columns, f'{name}: table columns mismatch.')
    rows = table.to_pylist()
    for r in rows:
        if 'diagnostics' in r:
            r['diagnostics'] = json.loads(r['diagnostics'])
    return rows


def diagnostics(rows):
    values = [r['diagnostics'] for r in rows if r['status'] == 'executed']
    result = {}
    for key in ('zero_size_entry_count', 'rejected_fill_count', 'margin_reject_count', 'liquidation_count', 'leverageCapSkippedEntries'):
        result[key] = sum(v[key] for v in values) if all(key in v for v in values) else None
    result['no_capital_halt'] = any(v.get('no_capital_halt', False) for v in values)
    numbers = [v['maxEntryLeverageUsed'] for v in values if v.get('maxEntryLeverageUsed') is not None]
    result['maxEntryLeverageUsed'] = max(numbers) if numbers else None
    return result


def validate_result(root, m, p, allocations, trades):
    _keys(m, 'schema_version status plan_identity execution_identity provenance variants artifacts limitations units'.split(), 'portfolio result')
    require(m['schema_version'] == SCHEMA and m['plan_identity'] == p['plan_identity'], 'result/plan version or identity mismatch.')
    require(m['execution_identity'] == digest(dict(version='strategy_lab_portfolio_execution_v1', plan_identity=p['plan_identity'])), 'execution identity mismatch.')
    require(m['limitations'] == LIMITATIONS, 'result limitations mismatch.')
    require([v['variant'] for v in m['variants']] == p['request']['variants'], 'result variants mismatch.')
    require(m['status'] == ('depleted' if any(v['status'] == 'depleted' for v in m['variants']) else 'complete'), 'result status mismatch.')
    keys = [(r['variant'], r['period'], r['window'], r['rank']) for r in allocations]
    require(len(set(keys)) == len(keys), 'duplicate allocation joins.')
    index = dict(zip(keys, allocations)); trade_index = {}
    for t in trades:
        key = tuple(t[k] for k in ('variant', 'period', 'window', 'rank'))
        require(key in index and t['instrument'] == index[key]['instrument'] and index[key]['status'] == 'executed', 'orphan trade.')
        trade_index.setdefault(key, []).append(t)
    cal = p['calendar']; expected_keys = set()
    for v in m['variants']:
        variant = v['variant']; vid = variant['id']; total = float(p['request']['initialCapital'])
        slots = len(cal['roster']) if variant['method'] == 'all_declared' else cal['topN']
        require(len(v['periods']) == len(cal['groups']), 'missing periods.')
        for group, decision, period in zip(cal['groups'], p['decisions'][vid], v['periods']):
            require(period['id'] == group['id'] and period['opening'] == total, 'period carry contradiction.')
            if total <= 0:
                require(period == dict(id=group['id'], opening=total, closing=total, allocation=None, allocation_rounding_delta=None,
                    status='not_funded', return_pct=None, reason='nonpositive_opening_capital'), 'invalid unfunded period.')
                continue
            allocation = total/slots; delta = finite_sum([allocation]*slots)-total
            require(period['allocation'] == allocation and period['allocation_rounding_delta'] == delta, 'allocation rounding contradiction.')
            members = [*decision['members'], *([None]*(slots-len(decision['members'])))]
            closes = []
            for rank, ticker in enumerate(members, 1):
                capital = allocation
                for wid in group['windows']:
                    key = (vid, group['id'], wid, rank); expected_keys.add(key)
                    require(key in index, 'missing allocation cell.')
                    row = index[key]; ts = trade_index.get(key, [])
                    cell = next((r for r in p['ledger'][variant['input']] if r['window'] == wid and r['ticker'] == ticker), None)
                    expected_status = 'idle' if ticker is None else 'depleted' if capital <= 0 else 'abstained' if cell['status'] == 'abstained' else 'executed'
                    require(row['status'] == expected_status and row['candidate_id'] == (cell['candidate_id'] if cell else None)
                        and row['source_account_id'] == (cell['account_id'] if cell else None), 'frozen choice/status contradiction.')
                    require(row['instrument'] == ticker and row['allocated'] == allocation and row['opening'] == capital
                        and row['allocation_rounding_delta'] == delta, 'allocation/carry contradiction.')
                    require(math.isfinite(row['closing']), 'nonfinite saved capital.')
                    require([t['trade'] for t in ts] == list(range(1, len(ts)+1)), 'duplicate/unordered trade IDs.')
                    require(row['trade_count'] == len(ts) and row['fees'] == finite_sum(t['entry_fee']+t['exit_fee'] for t in ts)
                        and row['net_pnl'] == reconcile_trades(capital, row['closing'], ts), 'trade totals contradiction.')
                    if row['status'] != 'executed':
                        require(row['status'] in ('idle', 'abstained', 'depleted') and not ts and row['closing'] == capital
                            and row['actual_fingerprint'] is None, 'invalid idle/depleted account.')
                    else:
                        require(capital > 0 and ticker is not None and isinstance(row['actual_fingerprint'], str)
                            and len(row['actual_fingerprint']) == 64, 'invalid execution binding.')
                        source = p['sources'][variant['input']]
                        params = next(a['params'] for a in source['manifest']['contract']['accounts'] if a['account_id'] == cell['account_id']) if cal['family'] == 'fixed' else next(
                            c['params'] for c in source['candidates']['candidates'] if c['candidate_id'] == cell['candidate_id'])
                        require(row['reference_capital'] == params['initialCapital'], 'reference capital contradiction.')
                        w = next(w for w in cal['windows'] if w['id'] == wid)
                        for trade in ts:
                            require(all(type(trade[k]) in (float, int) and math.isfinite(trade[k]) for k in
                                ('entry_price', 'exit_price', 'size', 'net_pnl', 'entry_fee', 'exit_fee')), 'nonfinite trade.')
                            require(trade['entry_fee'] == trade['entry_price']*trade['size']*(params['commissionPct']/100.)
                                and trade['exit_fee'] == trade['exit_price']*trade['size']*(params['commissionPct']/100.), 'trade fee contradiction.')
                            require(pd.Timestamp(w['start']) <= pd.Timestamp(trade['entry_time']) <= pd.Timestamp(trade['exit_time']) < pd.Timestamp(w['end']), 'trade outside frozen window.')
                    capital = row['closing']
                closes.append(capital)
            total = finite_sum(closes)
            require(period['closing'] == total and period['return_pct'] == 100*(total/period['opening']-1)
                and period['status'] == ('depleted' if total <= 0 else 'complete'), 'pool/period return contradiction.')
        metric = v['metrics']; initial = float(p['request']['initialCapital'])
        require(metric['initial'] == initial and metric['final'] == total and metric['profit'] == total-initial
            and metric['return_pct'] == 100*(total/initial-1), 'headline capital contradiction.')
        require(v['status'] == ('depleted' if total <= 0 else 'complete'), 'variant status contradiction.')
        growth = math.prod(period['closing']/period['opening'] for period in v['periods'] if period['opening'] > 0)
        require(math.isclose(growth, total/initial, rel_tol=1e-12, abs_tol=1e-12), 'saved growth contradiction.')
        rows = [r for r in allocations if r['variant'] == vid]
        require(v['diagnostics'] == diagnostics(rows) and v['trade_count'] == sum(r['trade_count'] for r in rows)
            and v['fees'] == finite_sum(r['fees'] for r in rows), 'headline trade/diagnostic contradiction.')
        path = pq.read_table(safe_path(root, v['curve_file']))
        require(path.column_names == ['timestamp_ns', 'balance', 'equity'], 'invalid curve columns.')
        ts, balance, equity = [path[k].to_numpy() for k in path.column_names]; validate_path(ts, balance, equity)
        require(balance[0] == equity[0] == initial and balance[-1] == equity[-1] == total
            and list(map(int, (ts[0], ts[-1]))) == v['bounds'], 'curve anchor/close contradiction.')
        require(len(ts) == v['curve_policy']['saved_count'] and v['native_count'] == v['curve_policy']['native_count'], 'curve counts mismatch.')
        require(v['curve_policy']['ordinary_count'] <= 6*p['request']['curveBuckets']
            and v['curve_policy']['buckets'] == p['request']['curveBuckets'], 'curve policy mismatch.')
        require(v['storage_identity'] == digest(dict(dense_digest=v['dense_digest'], policy=v['curve_policy'])), 'storage identity mismatch.')
        step = cal['timeframe']*60*10**9
        require(v['native_count'] == (pd.Timestamp(cal['windows'][-1]['end']).value-pd.Timestamp(cal['windows'][0]['start']).value)//step+1
            and len(v['dense_digest']) == 64, 'native curve count/digest mismatch.')
        points = {int(t): (float(b), float(e)) for t, b, e in zip(ts, balance, equity)}
        for group, period in zip(cal['groups'], v['periods']):
            require(points.get(pd.Timestamp(group['start']).value) == (period['opening'], period['opening']) and
                points.get(pd.Timestamp(group['end']).value) == (period['closing'], period['closing']), 'saved boundary contradicts period totals.')
        require(len(v['windows']) == len(cal['windows']), 'missing window facts.')
        for window, fact in zip(cal['windows'], v['windows']):
            require(fact['window'] == window['id'] and points.get(pd.Timestamp(window['start']).value) == (fact['opening'], fact['opening'])
                and points.get(pd.Timestamp(window['end']).value) == (fact['closing'], fact['closing']), 'window boundary contradiction.')
            require(fact['return_pct'] == (100*(fact['closing']/fact['opening']-1) if fact['opening'] > 0 else None), 'window return contradiction.')
            window_rows = [r for r in rows if r['window'] == window['id']]
            if window_rows:
                require(fact['closing'] == finite_sum(r['closing'] for r in window_rows), 'window pooled close contradiction.')
        from ..selection.evaluate import arm_return
        reference = []
        cells = {(r['window'], r['ticker']): r for r in p['ledger'][variant['input']]}
        for group, d in zip(cal['groups'], p['decisions'][vid]):
            for wid in group['windows']:
                reference.append(dict(window=wid, value=arm_return(d['members'], slots, {t: cells[wid, t] for t in cal['roster']})['value']))
        require(v['reference_windows'] == reference, 'reference return contradiction.')
        for key, col in (('balance_witness', 0), ('mtm_witness', 1)):
            w = metric[key]; require(w['peak'] in points and w['trough'] in points, 'missing DD witness.')
            peak, trough = points[w['peak']][col], points[w['trough']][col]
            require(w['percent'] == (peak-trough)/peak*100 and w['percent'] == metric['balance_dd_pct' if col == 0 else 'mtm_dd_pct'], 'DD witness contradiction.')
    require(set(index) == expected_keys, 'unexpected allocation cells.')


def open_result(root):
    root = checked_input(root); p = open_plan(root)
    _, _, m = _read(safe_path(root, 'result.json'))
    expected = {'plan.json', 'allocations.parquet', 'trades.parquet', *(v['curve_file'] for v in m['variants'])}
    require(set(m['artifacts']) == expected, 'result artifact inventory mismatch.')
    for name, claim in m['artifacts'].items():
        safe_path(root, name)
        require(claim['path'] == name and validate_artifact_record(root, claim), 'result artifact hash mismatch.')
    allocations = read_table(root, 'allocations.parquet', ALLOCATION_COLUMNS)
    trades = read_table(root, 'trades.parquet', TRADE_COLUMNS)
    validate_result(root, m, p, allocations, trades)
    return m, p, allocations, trades
