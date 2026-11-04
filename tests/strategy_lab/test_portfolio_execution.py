"""Independent saved-segment singleton oracle and real capital sensitivity."""
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
import json
import math
import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from _selection_helpers import selection_family, document
from _analysis_helpers import mutate_group
from _portfolio_helpers import request
from tools.strategy_lab.portfolio import prepare, run, open_result, detail
from tools.strategy_lab.fixed.run import build_execution
from tools.strategy_lab.parquet import replay, segment
from tools.strategy_lab.config import _validate_strategy
from tools.strategy_lab.config_v2 import validate_saved_request
from tools.strategy_lab.fixed.config import RUNTIME


def test_changing_parameters_independent_segment_oracle(selection_family, tmp_path):
    from core.engine_v2.runner import run_v2_strategy
    f = selection_family; source = tmp_path/'source'; shutil.copytree(f['candidates']['off'], source)
    for ticker in f['tickers']:
        for wid in range(1, 5):
            def choose(a, metrics, wid=wid):
                a[:, 0, metrics['net_profit_pct']] = [100., 1.] if wid % 2 else [1., 100.]
                a[:, 0, metrics['total_trades']] = 10.
            mutate_group(source, ticker, wid, choose)
    raw = request(f['rankings']['is_oos'], {'off': source}, family='is_oos')
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    run(tmp_path/'plan', f['pack'], tmp_path/'result')
    m, p, allocations, trades = open_result(tmp_path/'result')
    chosen = [r for r in allocations if r['variant'] == 'off-merlin_x']
    assert {r['candidate_id'] for r in chosen} == {1, 2}
    s = p['sources']['off']; contract = s['contract']; windows = validate_saved_request(contract)
    config, _ = _validate_strategy(contract['generation']['strategy'], parquet=True)
    curves = {}
    for row in chosen:
        binding = next(b for b in s['bindings']['instruments'] if b['instrument_id'] == row['instrument'])
        prepared = replay(binding, f['pack'])
        window = next(w for w in windows if pd.Timestamp(w.oos_start) == pd.Timestamp(next(w['start'] for w in p['calendar']['windows'] if w['id'] == row['window'])))
        part = segment(prepared, dict(instrument_id=row['instrument']), window, 'oos', SimpleNamespace(generation=contract['generation']))
        candidate = next(c for c in s['candidates']['candidates'] if c['candidate_id'] == row['candidate_id'])
        view = SimpleNamespace(facts=dict(bounds=dict(evaluation_start=part.dataframe.index[part.trade_start_idx].isoformat(),
            evaluation_end=(part.dataframe.index[-1]+pd.Timedelta(hours=1)-pd.Timedelta(microseconds=1)).isoformat())),
            trade_start_idx=part.trade_start_idx, entry_admission=prepared.entry_admission,
            admission_rules=prepared.admission_rules, entry_filter_context=prepared.entry_filter_context)
        ps = dict(params={k: v for k,v in candidate['params'].items() if k not in RUNTIME}, variants=[candidate['variant_name']])
        _, _, _, reference = build_execution(config, ps, view)
        assert reference['semantic_key'] == candidate['semantic_key']
        ps['params']['initialCapital'] = row['opening']
        plan, hooks, params, actual = build_execution(config, ps, view)
        execution = run_v2_strategy(data=hooks.build_execution_data(part.dataframe, params), profile=plan.profile,
            params=params, trade_start_idx=part.trade_start_idx, admission_rules=prepared.admission_rules,
            entry_filter_context=prepared.entry_filter_context, compute_sharpe_daily=True, compute_max_drawdown_mtm=True)
        sr = execution.strategy_result; start = part.trade_start_idx
        assert row['closing'] == sr.balance_curve[-1] and row['trade_count'] == len(sr.trades)
        assert row['actual_fingerprint'] == plan.plan_fingerprint and row['actual_semantic_key'] == actual['semantic_key']
        saved_trades = [t for t in trades if (t['variant'],t['period'],t['window'],t['rank']) == ('off-merlin_x',row['period'],row['window'],row['rank'])]
        assert [(t['size'], t['net_pnl']) for t in saved_trades] == [(t.size,t.net_pnl) for t in sr.trades]
        assert row['fees'] == math.fsum(t.entry_price*t.size*(params['commissionPct']/100.) + t.exit_price*t.size*(params['commissionPct']/100.) for t in sr.trades)
        curves[row['window'], row['rank']] = (np.asarray(sr.balance_curve[start:]), np.asarray(sr.equity_curve[start:]))
    detail(tmp_path/'result', 'off-merlin_x', f['pack'], tmp_path/'detail')
    dense = pq.read_table(tmp_path/'detail/dense.parquet').to_pydict()
    expected_b, expected_e = [237.], [237.]
    for window in p['calendar']['windows']:
        parts = [curves[window['id'], rank] for rank in (1,2)]
        expected_b.extend(math.fsum(values) for values in zip(*(b for b,e in parts)))
        expected_e.extend(math.fsum(values) for values in zip(*(e for b,e in parts)))
    assert dense['balance'] == expected_b and dense['equity'] == expected_e
    assert any(e < b for b,e in zip(expected_b, expected_e))
    assert m['variants'][0]['metrics']['mtm_dd_pct'] > 0


def test_subeditor_capital_and_minimum_lot_loss(selection_family, tmp_path):
    f = selection_family
    profits, counts = [], []
    for i, capital in enumerate((1000., .01, 2e12)):
        work = tmp_path/str(i)
        raw = request(f['rankings']['fixed'], {'off': f['fixed']['off']}, capital=capital)
        prepare(document(work, 'request.json', raw), work/'plan')
        run(work/'plan', f['pack'], work/'result')
        m, _, rows, trades = open_result(work/'result')
        v = m['variants'][0]; profits.append(v['metrics']['return_pct']); counts.append(v['trade_count'])
        if capital == .01:
            assert not trades
            assert all(v['diagnostics'][key] == 0 for key in ('zero_size_entry_count', 'rejected_fill_count', 'margin_reject_count'))
            assert 'zero diagnostic counters' in ' '.join(m['limitations'])
    assert counts[0] > 0 and counts[1] == 0 and len(set(profits)) > 1
