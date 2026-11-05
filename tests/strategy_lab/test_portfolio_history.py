"""Held historical membership, source scopes, and causal frozen decisions."""
from copy import deepcopy
import shutil

import pytest

from _history_helpers import history_family, history_request, plan as history_plan
from _selection_helpers import document
from _analysis_helpers import mutate_group
from _portfolio_helpers import request, check_report
from tools.strategy_lab.portfolio import prepare, open_plan, run, open_result, detail, report
from tools.strategy_lab.portfolio.plan import calendar
from tools.strategy_lab.selection import open_history_plan


def test_fixed_history_actual_singletons_and_report(history_family, tmp_path):
    from core.engine_v2.runner import run_v2_strategy
    from tools.strategy_lab.config import _validate_strategy
    from tools.strategy_lab.filters import effective
    from tools.strategy_lab.fixed.run import build_execution
    from tools.strategy_lab.fixed.config import RUNTIME
    from tools.strategy_lab.parquet import replay
    from _fixed_helpers import snapshot
    f = history_family
    raw = request(f['plans']['fixed'], f['fixed'], kind='history', cadence=1, method='prior_oos_mean')
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    run(tmp_path/'plan', f['pack'], tmp_path/'result')
    m, p, rows, trades = open_result(tmp_path/'result'); before = snapshot(tmp_path/'result')
    assert [g['windows'] for g in p['calendar']['groups']] == [[5], [6], [7], [8]]
    assert p['calendar']['excluded_history_ids'] == [1,2,3,4]
    assert m['variants'][0]['metrics'] != m['variants'][2]['metrics']
    assert any(r['opening'] != r['reference_capital'] for r in rows if r['status']=='executed')
    variants = {v['id']:v for v in raw['variants']}
    checked = set()
    for row in rows:
        if row['status'] != 'executed': continue
        variant = variants[row['variant']]
        period = next(t for v in m['variants'] if v['variant']==variant for t in v['periods'] if t['id']==row['period'])
        denominator = len(p['calendar']['roster']) if variant['method']=='all_declared' else p['calendar']['topN']
        assert row['opening'] == row['allocated'] == period['opening']/denominator
        contract = p['sources'][variant['input']]['manifest']['contract']
        account = next(a for a in contract['accounts'] if a['account_id']==row['source_account_id'])
        config, _ = _validate_strategy(contract['request']['generation']['strategy'], parquet=True)
        filters = effective(contract['request'], config)
        prepared = replay(account, f['pack'], filters)
        ps = dict(params={k:v for k,v in account['params'].items() if k not in RUNTIME}, variants=[account['candidate']['variant']])
        ps['params']['initialCapital'] = row['opening']
        plan, hooks, params, _ = build_execution(config, ps, prepared, filters)
        engine = run_v2_strategy(data=hooks.build_execution_data(prepared.frame,params), profile=plan.profile,params=params,
            trade_start_idx=prepared.trade_start_idx,admission_rules=prepared.admission_rules,
            entry_filter_context=prepared.entry_filter_context,compute_sharpe_daily=True,compute_max_drawdown_mtm=True)
        sr = engine.strategy_result
        assert row['closing']==sr.balance_curve[-1] and row['actual_fingerprint']==plan.plan_fingerprint
        saved = [t for t in trades if all(t[k]==row[k] for k in ('variant','period','window','rank'))]
        assert [(t['size'],t['net_pnl']) for t in saved]==[(t.size,t.net_pnl) for t in sr.trades]
        checked.add(variant['input'])
    assert checked == {'off','on'} and trades
    detail(tmp_path/'result','on-prior_oos_mean',f['pack'],tmp_path/'detail')
    report(tmp_path/'result',tmp_path/'report')
    check_report(tmp_path/'report',m,p,rows)
    assert snapshot(tmp_path/'result')==before


def test_history_holding_prior_four_single_target_and_future(history_family, tmp_path):
    f = history_family
    raw = request(f['plans']['is_oos'], f['candidates'], family='is_oos', kind='history', cadence=2)
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    p = open_plan(tmp_path/'plan')
    assert [g['windows'] for g in p['calendar']['groups']] == [[5, 6], [7, 8]]
    assert p['calendar']['excluded_history_ids'] == [1, 2, 3, 4]
    for decisions in p['decisions'].values():
        for d in decisions:
            if 'rows' in d['decision']:
                refs = d['decision']['rows'][0]['history_refs']
                assert len(refs) == 4
                assert refs[-1][1] == d['cutoff']
    run(tmp_path/'plan', f['pack'], tmp_path/'result')
    m, _, rows, _ = open_result(tmp_path/'result')
    for variant in raw['variants']:
        for group in p['calendar']['groups']:
            by_window = [{(r['rank'], r['instrument']) for r in rows if r['variant'] == variant['id'] and r['window'] == wid}
                for wid in group['windows']]
            assert by_window[0] == by_window[1]
    events = p['access_trace']
    for wid in range(1, 9):
        positions = [i for i,e in enumerate(events) if e['block_id'] == wid]
        assert [events[i]['event'] for i in positions] == ['is_frozen', 'is_frozen', 'oos_reveal', 'oos_reveal']
    changed = deepcopy(raw); changed['initialCapital'] *= .001
    prepare(document(tmp_path, 'capital.json', changed), tmp_path/'capital')
    assert open_plan(tmp_path/'capital')['decisions'] == p['decisions']
    clone = tmp_path/'future-source'; shutil.copytree(f['candidates']['off'], clone)
    def future(a, metrics): a[:, 1, metrics['net_profit_pct']] = 99999.
    mutate_group(clone, f['tickers'][0], 8, future)
    changed = deepcopy(raw); changed['inputs'][0]['dataset'] = str(clone)
    prepare(document(tmp_path, 'future.json', changed), tmp_path/'future')
    assert open_plan(tmp_path/'future')['decisions'] == p['decisions']
    single = history_plan(tmp_path, f['tickers'], history_request(mode='is_oos', end='2024-09-30', evaluation='2024-09-01'), 'single-history')
    changed = deepcopy(raw); changed['sourcePlan']['path'] = str(single)
    prepare(document(tmp_path, 'single.json', changed), tmp_path/'single')
    assert [g['windows'] for g in open_plan(tmp_path/'single')['calendar']['groups']] == [[5]]


@pytest.mark.parametrize('role', ['evaluation', 'history'])
def test_independent_scope_unlocks(history_family, tmp_path, role):
    f = history_family
    raw = request(f['plans']['is_oos'], {'off': f['candidates']['off']}, family='is_oos', kind='history')
    raw['inputs'][0][role+'Scope'] = 'temporal_dev' if role == 'evaluation' else 'locked_history'
    with pytest.raises(ValueError):
        prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    assert not (tmp_path/'plan/plan.json').exists()


def test_two_month_oos_cadence_and_short_group(tmp_path):
    request_source = history_request(mode='is_oos', end='2025-06-30', evaluation='2025-01-01')
    # The ordinary helper supplies defaults, so override the closed calendar explicitly.
    request_source['calendar']['oosMonths'] = 2
    path = history_plan(tmp_path, ['BYBIT_AAAUSDT', 'BYBIT_BBBUSDT'], request_source)
    source = dict(metadata=open_history_plan(path).metadata, records=[])
    raw = dict(sourcePlan=dict(kind='history'), rebalanceMonths=4, inputs=[{}], variants=[dict(method='prior_oos_mean')])
    c = calendar(raw, source)
    assert [len(g['windows']) for g in c['groups']] == [2, 1]
    for cadence in (1, 3):
        raw['rebalanceMonths'] = cadence
        with pytest.raises(ValueError, match='divisible'):
            calendar(raw, source)
