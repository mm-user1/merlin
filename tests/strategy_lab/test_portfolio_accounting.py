"""Independent scheduling and signed-account arithmetic witnesses."""
from copy import deepcopy
import math

import numpy as np
import pandas as pd
import pytest

from tools.strategy_lab.portfolio.execution import execute_variant
from tools.strategy_lab.portfolio.accounting import finite_sum


def scalar_plan(k=8, roster_size=8, total=100., selected=None):
    roster = [f'T{i}' for i in range(roster_size)]
    windows = [dict(id=i+1, start=f'2024-0{i+1}-01', end=f'2024-0{i+2}-01', isStart=None) for i in range(4)]
    groups = [dict(id=1, start=windows[0]['start'], end=windows[1]['end'], windows=[1, 2]),
        dict(id=2, start=windows[2]['start'], end=windows[3]['end'], windows=[3, 4])]
    selected = roster[:2] if selected is None else selected
    v = dict(id='chosen', input='off', method='prior_oos_mean')
    p = dict(request=dict(initialCapital=total), calendar=dict(roster=roster, topN=k, timeframe=1440, windows=windows, groups=groups),
        ledger={'off': [dict(window=w['id'], ticker=t, candidate_id=1, semantic_key='ref', account_id=None, status='completed', value=1.)
            for w in windows for t in roster]}, decisions={'chosen': [dict(members=selected) for g in groups]})
    return p, v


def install_execution(monkeypatch, changes):
    import tools.strategy_lab.portfolio.execution as ex
    calls = []
    monkeypatch.setattr(ex, 'source_context', lambda *a: {})
    monkeypatch.setattr(ex, 'reference', lambda ctx, ticker, window, minutes: (ticker, window))
    def execute(ctx, ref, capital):
        ticker, window = ref
        calls.append((ticker, window['id'], capital))
        gain = changes.get((ticker, window['id']), 0.)
        native = pd.date_range(window['start'], window['end'], inclusive='left', freq='D').as_unit('ns').asi8
        b = np.full(len(native), capital); b[-1] = capital+gain
        e = b.copy()
        trades = [dict(net_pnl=gain, entry_fee=0., exit_fee=0.)] if gain else []
        return native, b, e, trades, dict(actual_fingerprint='a'*64, actual_semantic_key='actual', reference_fingerprint='b'*64,
            reference_semantic_key='ref', reference_capital=1000., diagnostics={})
    monkeypatch.setattr(ex, 'execute', execute)
    return calls


def test_underfill_own_carry_abstention_reentry_and_pooling(monkeypatch):
    p, v = scalar_plan()
    for r in p['ledger']['off']:
        if r['ticker'] == 'T1' and r['window'] == 2:
            r.update(candidate_id=None, semantic_key=None, status='abstained', value=0.)
    calls = install_execution(monkeypatch, {('T0', 1): 10., ('T1', 1): 20., ('T0', 2): -5.})
    result, rows, _, _ = execute_variant(p, v, None)
    assert ('T0', 2, 22.5) in calls and not any(t == 'T1' and w == 2 for t,w,c in calls)
    assert result['periods'][0]['closing'] == 125.
    assert ('T0', 3, 125./8) in calls and ('T1', 3, 125./8) in calls
    assert len([r for r in rows if r['status'] == 'idle']) == 24
    assert all(r['allocated'] == 12.5 for r in rows if r['period'] == 1)
    assert result['metrics']['final'] == 125.


def test_signed_account_and_total_depletion(monkeypatch):
    p, v = scalar_plan()
    calls = install_execution(monkeypatch, {('T0', 1): -20.})
    result, rows, _, _ = execute_variant(p, v, None)
    debt = next(r for r in rows if r['instrument'] == 'T0' and r['window'] == 2)
    assert debt['opening'] == debt['closing'] == -7.5 and debt['status'] == 'depleted'
    assert ('T0', 3, 10.) in calls
    calls = install_execution(monkeypatch, {('T0', 1): -200.})
    result, rows, _, (_, b, e) = execute_variant(p, v, None)
    assert result['status'] == 'depleted' and result['metrics']['final'] == -100.
    assert result['periods'][1]['return_pct'] is None and result['windows'][2]['return_pct'] is None
    assert not any(w > 2 for t,w,c in calls) and b[-1] == e[-1] == -100.
    assert result['metrics']['balance_dd_pct'] == 200.
    install_execution(monkeypatch, {('T0', 1): float('nan')})
    with pytest.raises(ValueError, match='nonfinite'):
        execute_variant(p, v, None)


@pytest.mark.parametrize('k', [3, 7, 44])
def test_cash_rounding_and_full_roster_denominator(monkeypatch, k):
    install_execution(monkeypatch, {})
    p, v = scalar_plan(k=k, roster_size=k, total=7172.330335514883, selected=[])
    result, rows, _, _ = execute_variant(p, v, None)
    total = p['request']['initialCapital']
    for period in result['periods']:
        assert period['opening'] == total
        allocation = total/k
        assert period['allocation_rounding_delta'] == math.fsum([allocation]*k)-total
        assert all(r['opening'] == r['closing'] == allocation for r in rows if r['period'] == period['id'])
        total = math.fsum([allocation]*k)
        assert period['closing'] == total
    v['method'] = 'all_declared'; p['calendar']['topN'] = 1
    for d in p['decisions']['chosen']:
        d['members'] = p['calendar']['roster']
    control, control_rows, _, _ = execute_variant(p, v, None)
    assert control['metrics'] == result['metrics'] and control_rows[0]['allocated'] == p['request']['initialCapital']/k


def test_first_bar_close_label_and_mtm_only_trough(monkeypatch):
    import tools.strategy_lab.portfolio.execution as ex
    p, v = scalar_plan(k=1, roster_size=1, selected=['T0'])
    install_execution(monkeypatch, {})
    original = ex.execute
    def path(ctx, ref, capital):
        ts, b, e, trades, binding = original(ctx, ref, capital)
        b[0] = capital-1.  # First observed entry fee, not a replacement opening anchor.
        e[0] = capital-5.
        e[1] = capital-20.
        return ts, b, e, trades, binding
    monkeypatch.setattr(ex, 'execute', path)
    facts, _, _, (ts, b, e) = execute_variant(p, v, None)
    assert b[0] == e[0] == 100. and b[1] == 99. and e[1] == 95.
    assert pd.Timestamp(int(ts[1])) == pd.Timestamp('2024-01-02')
    assert facts['metrics']['balance_dd_pct'] == 1. and facts['metrics']['mtm_dd_pct'] == 20.
    assert facts['metrics']['mtm_witness']['peak'] == int(ts[0])
    assert facts['metrics']['mtm_witness']['trough'] == int(ts[2])


def test_compact_overflow_exact_indices_and_earliest_extrema():
    from tools.strategy_lab.portfolio.accounting import compact, witness
    ts = np.arange(513, dtype='int64')*10**15
    balance = 100.+(np.arange(len(ts)) % 7).astype(float)
    equity = 90.+(np.arange(len(ts)) % 11).astype(float)
    buckets = 32
    assert int(ts[-1])*buckets > 2**63-1
    w = witness(ts, equity)
    mandatory = [int(ts[9]), int(ts[300]), w['peak'], w['trough']]
    groups = {}
    for i, t in enumerate(ts.tolist()):
        bucket = min(buckets-1, (t-int(ts[0]))*buckets//(int(ts[-1])-int(ts[0])))
        groups.setdefault(bucket, []).append(i)
    ordinary = set()
    for points in groups.values():
        ordinary.update((points[0], points[-1]))
        for values in (balance, equity):
            ordinary.update((min(points, key=lambda i: values[i]), max(points, key=lambda i: values[i])))
    required = {0, len(ts)-1, *(ts.tolist().index(t) for t in mandatory)}
    chosen, policy = compact(ts, balance, equity, buckets, mandatory)
    assert chosen == sorted(ordinary | required)
    assert policy['ordinary_count'] == len(ordinary)
    assert policy['saved_count'] == len(ordinary | required)
    assert policy['mandatory_count'] == len(required)
    assert policy['additional_mandatory_count'] == len(required-ordinary)


def test_compact_exact_selection_at_float_ratio_boundary():
    from tools.strategy_lab.portfolio.accounting import compact
    ts = 1704067200 * 10**9 + np.arange(45, dtype='int64') * (5 * 60 * 10**9)
    values = 100. + np.arange(45, dtype=float)
    chosen, _ = compact(ts, values, values, 22, [])
    # Two points per bucket, then clamped 42/43/44: monotone extrema omit only 43.
    # The int(ratio * B) replacement also drops 29 at the exact bucket-15 boundary.
    assert chosen == [i for i in range(45) if i != 43]


def test_earliest_equal_peak_and_drawdown_witness():
    from tools.strategy_lab.portfolio.accounting import witness, metrics
    ts = np.arange(6, dtype='int64')*10**15
    values = np.array([100., 110., 110., 80., 110., 80.])
    expected = dict(percent=30./110.*100., peak=int(ts[1]), trough=int(ts[3]))
    assert witness(ts, values) == expected
    facts = metrics(ts, values, values, 100.)
    assert facts['balance_witness'] == facts['mtm_witness'] == expected
    assert facts['balance_dd_amount'] == 30.


def test_report_null_depletion_abstention_and_idle_cash(monkeypatch, tmp_path):
    import importlib
    from _portfolio_helpers import check_report
    from tools.strategy_lab.portfolio.run import finish_variant
    from tools.strategy_lab.portfolio.storage import LIMITATIONS
    p, v = scalar_plan()
    p['decisions']['<chosen&>'] = p['decisions'].pop(v['id']); v['id'] = '<chosen&>'
    for cell in p['ledger']['off']:
        if cell['ticker']=='T1' and cell['window']==2:
            cell.update(status='abstained', candidate_id=None, value=0.)
    p['request']['sourcePlan'] = dict(path=str(tmp_path/'source'))
    p['request']['candidateSelection'] = None
    p['sources'] = {}
    for g, d in zip(p['calendar']['groups'], p['decisions'][v['id']]):
        d['cutoff'] = g['start']
    install_execution(monkeypatch, {('T0', 1): -200.})
    facts, rows, _, _ = execute_variant(p, v, None)
    facts['reference_windows'][0]['value'] = None  # Unavailable saved reference at the reader seam.
    for row in rows:
        if row['status']=='executed':
            row['diagnostics'] = dict(zero_size_entry_count=0, rejected_fill_count=0, margin_reject_count=0,
                liquidation_count=0, leverageCapSkippedEntries=0, no_capital_halt=False, maxEntryLeverageUsed=None)
    executed = next(r for r in rows if r['status']=='executed')
    executed['diagnostics'] = dict(zero_size_entry_count=7, rejected_fill_count=4, margin_reject_count=2,
        liquidation_count=1, leverageCapSkippedEntries=3, no_capital_halt=True, maxEntryLeverageUsed=2.5)
    finish_variant(facts, rows)
    facts['curve_policy'] = dict(saved_count=10)
    m = dict(variants=[facts], limitations=LIMITATIONS, execution_identity='fixture', artifacts={})
    # Reader seam for rare execution-backed states; genuine saved-reader coverage is separate.
    module = importlib.import_module('tools.strategy_lab.portfolio.report')
    monkeypatch.setattr(module, 'open_result', lambda _: (m, p, rows, []))
    module.report(tmp_path/'parent', tmp_path/'report')
    check_report(tmp_path/'report', m, p, rows)
    html = (tmp_path/'report/index.html').read_text(encoding='utf-8')
    assert '&lt;chosen&amp;&gt;' in html
    assert 'maxEntryLeverageUsed is the maximum observed entry leverage over non-null values; null if none are available.' in html
    assert facts['periods'][1]['status']=='not_funded'
    assert facts['reference_windows'][0]['value'] is None
    assert {'abstained','depleted','idle'} <= {r['status'] for r in rows}
