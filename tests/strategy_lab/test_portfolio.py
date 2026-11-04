"""Frozen actual-capital execution and offline publication contracts."""
from copy import deepcopy
import json
import math

import numpy as np
import pandas as pd
import pytest

from _selection_helpers import selection_family, document
from _portfolio_helpers import request, production_family
from tools.strategy_lab.portfolio import prepare, run, open_plan, open_result, detail, report
from tools.strategy_lab.portfolio.accounting import compact, dense_digest, metrics, finite_sum, preflight, reconcile_trades


@pytest.mark.parametrize('family', ['fixed', 'is_oos'])
def test_real_saved_family(selection_family, tmp_path, family):
    from _fixed_helpers import snapshot
    f = selection_family
    datasets = f['fixed'] if family == 'fixed' else f['candidates']
    before = {n: snapshot(path) for n, path in datasets.items()}
    raw = request(f['rankings'][family], datasets, family=family)
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    run(tmp_path/'plan', f['pack'], tmp_path/'result')
    m, p, accounts, trades = open_result(tmp_path/'result')
    assert trades and all(v['fees'] > 0 for v in m['variants'])
    assert len(m['variants']) == 4
    assert m['variants'][0]['metrics'] != m['variants'][2]['metrics']
    assert any(a['opening'] != a['reference_capital'] for a in accounts if a['status'] == 'executed')
    if family == 'is_oos':
        assert any(a['opening'] != a['allocated'] for a in accounts)
    detail(tmp_path/'result', raw['variants'][0]['id'], f['pack'], tmp_path/'detail')
    report(tmp_path/'result', tmp_path/'report')
    assert (tmp_path/'report/index.html').exists()
    assert {n: snapshot(path) for n, path in datasets.items()} == before


def test_exact_atoms_rounding_tolerance_and_sampling():
    sequential = 0.
    for value in (1e16, 1., -1e16):
        sequential += value
    assert finite_sum([1e16, 1., -1e16]) == 1. != sequential
    for k in (3, 7, 44):
        b = 7172.330335514883
        atoms = [b/k]*k
        assert all(a == b/k for a in atoms)
        if k == 44:
            assert math.fsum(atoms)-b != 0
    reconcile_trades(1000., 1000.+1e-11, [])
    with pytest.raises(ValueError, match='trade P&L'):
        reconcile_trades(1000., 1001., [])
    ts = np.arange(12, dtype='int64')*10**17
    balance = np.array([100., 99, 100, 101, 101, 102, 101, 100, 103, 104, 105, 106])
    equity = balance.copy(); equity[6] = 80.
    m = metrics(ts, balance, equity, 100.)
    assert m['mtm_dd_pct'] > m['balance_dd_pct']
    assert m['mtm_witness']['peak'] == int(ts[5]) and m['mtm_witness']['trough'] == int(ts[6])
    digest = dense_digest(ts, balance, equity)
    for buckets in (1, 4096):
        indices, policy = compact(ts, balance, equity, buckets, [int(ts[6])])
        assert 0 in indices and 11 in indices and 6 in indices
        assert policy['ordinary_count'] <= 6*buckets
        assert dense_digest(ts, balance, equity) == digest
    negative = np.array([100., -10., -10.])
    assert metrics(ts[:3], negative, negative, 100.)['return_pct'] == pytest.approx(-110.)
    with pytest.raises(ValueError, match='256 MiB'):
        preflight([dict(id=1, start='2024-01-01', end='2034-01-01')], [dict(windows=[1])], 44, 5)


@pytest.mark.parametrize('family', ['fixed', 'is_oos'])
def test_production_singleton_parity(production_family, tmp_path, family, monkeypatch):
    import importlib
    from types import SimpleNamespace
    from core.portfolio_projection import balance_path
    from core.portfolio import run_portfolio
    from core.walkforward_engine import WFConfig
    from ui.server_services import _build_optimization_config
    from tests._portfolio_helpers import selector, STRATEGY
    f = production_family
    raw = request(f['rankings'][family], {'off': f['fixed' if family == 'fixed' else 'candidates']}, family=family)
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    execution = importlib.import_module('tools.strategy_lab.portfolio.execution')
    original = execution.execute
    observed = {}
    def capture(ctx, ref, capital):
        result = original(ctx, ref, capital)
        ts, b, e, trades, binding = result
        start = pd.Timestamp(int(ts[0]), tz='UTC'); end = pd.Timestamp(int(ts[-1]), tz='UTC')+pd.Timedelta(hours=1)
        observed[ctx['binding']['instrument_id'], start.isoformat(), capital] = balance_path(
            SimpleNamespace(timestamps=pd.to_datetime(ts, utc=True), balance_curve=b), start, end, capital, 60)
        return result
    monkeypatch.setattr(execution, 'execute', capture)
    run(tmp_path/'plan', f['pack'], tmp_path/'result')
    m, p, allocations, trades = open_result(tmp_path/'result')
    supplied = dict(parameterMode='fixed', optimization_mode='grid', enabled_params={}, param_ranges={}, worker_processes=1,
        fixed_params=dict(f['params'], initialCapital=237., dateFilter=True,
            start='2024-02-01' if family == 'fixed' else '2024-01-01', end='2024-04-30'),
        entryFilters=dict(version=1, enabled=False, items=[]))
    config = _build_optimization_config('', supplied, strategy_id=STRATEGY, warmup_bars=100)
    wfa = WFConfig(is_period_days=None, oos_period_days=None, period_unit='months', is_period_months=1,
        oos_period_months=1, strategy_id=STRATEGY, warmup_bars=100) if family == 'is_oos' else None
    core = run_portfolio(config, selector(), f['pool'], wfa=wfa, publish=False)
    actual = m['variants'][0]
    assert actual['trade_count'] > 0 and actual['fees'] > 0
    for group, saved in zip(core['periods'], actual['periods']):
        assert group['opening'] == saved['opening'] and group['closing'] == saved['closing']
        for account in group['accounts']:
            ours = [r for r in allocations if r['variant'] == 'off-merlin_x' and r['period'] == group['id'] and r['instrument'] == account['instrument']]
            assert ours[0]['opening'] == account['opening']
            assert ours[-1]['closing'] == account['closing']
            assert sum(r['trade_count'] for r in ours) == account['trades']
            combined = []
            for row in ours:
                w = next(w for w in p['calendar']['windows'] if w['id'] == row['window'])
                path = observed[account['instrument'], w['start'], row['opening']]
                combined.extend(path if not combined else path[1:])
            assert combined == account['path']
            if family == 'is_oos':
                assert [(r['opening'], r['closing']) for r in ours] == [(r['opening'], r['closing']) for r in account['windows']]
    assert actual['metrics']['final'] == core['periods'][-1]['closing']
