from copy import deepcopy
import math

import pytest

from core import storage
from core.portfolio import run_portfolio
from core.portfolio_projection import project
from core.portfolio_storage import load_portfolio
from core.ticker_selector import build_calendar, normalize_selector, rank_pool
from core.walkforward_engine import WFConfig
from ui.server_services import _build_optimization_config
from tests._portfolio_helpers import STRATEGY, publish, payload, selector


def assert_empty_database():
    with storage.get_db_connection() as conn:
        for table in ('studies','study_sets','portfolios','portfolio_accounts','trials','wfa_windows'):
            assert conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0


def test_calendar_complete_tail_and_end_interpretation():
    periods = build_calendar('2025-12-01', '2026-04-30', 30, 2, is_months=2, oos_months=1)
    assert [len(p['windows']) for p in periods] == [2, 1]
    assert periods[0]['start'].startswith('2026-02-01')
    assert periods[1]['start'].startswith('2026-04-01')
    assert build_calendar('2026-09-01', '2026-10-01T00:00:00Z', 30, 1) == build_calendar('2026-09-01', '2026-09-30', 30, 1)
    with pytest.raises(ValueError, match='complete'):
        build_calendar('2026-09-01', '2026-09-30T00:00:00Z', 30, 1)


@pytest.mark.parametrize('value', [True, 1.5, 0, float('nan'), float('inf')])
def test_invalid_counts(value):
    cfg = selector()
    cfg['topN'] = value
    with pytest.raises(ValueError):
        normalize_selector(cfg)


def test_independent_projection_hand_case():
    periods = []
    for pid, start, end, opening, closes in [(1, '2024-01-01', '2024-02-01', 100, [110, 100]),
                                            (2, '2024-02-01', '2024-03-01', 105, [94.5, 126])]:
        periods.append(dict(id=pid, start=start, end=end, windows=[], accounts=[dict(id=f'{pid}:{r}', period=pid,
            role='funded', rank=r, opening=opening, closing=close, path=[[start,opening],[end,close]]) for r,close in enumerate(closes,1)]))
    record=dict(selector={'topN':2}, periods=periods, wfa=None)
    assert project(record)['profit_pct'] == pytest.approx(10.25)
    assert project(record, {'ranks':[1]})['profit_pct'] == pytest.approx(-1)
    assert project(record, {'ranks':[2]})['profit_pct'] == pytest.approx(20)
    assert project(record, {'ranks':[1,2], 'periods':[1,2]}) == project(record)


@pytest.mark.parametrize('with_wfa', [False, True])
def test_real_carried_execution_and_atomic_owned_children(tmp_path, with_wfa):
    storage.create_new_db('T08 execution')
    pool = publish(tmp_path/'pack')
    config = _build_optimization_config('', payload(), strategy_id=STRATEGY, warmup_bars=100)
    wfa = WFConfig(is_period_days=None, oos_period_days=None, period_unit='months', is_period_months=1,
                   oos_period_months=1, strategy_id=STRATEGY, warmup_bars=100) if with_wfa else None
    result = run_portfolio(config, selector(), pool, wfa=wfa)
    saved = load_portfolio(result['id'])
    assert saved == result
    assert saved['metrics']['total_trades'] > 0
    assert saved['periods'][-1]['closing'] != 237
    assert storage.list_studies() == []
    assert storage.list_study_sets_with_analytics_cache()['sets'][0]['kind'] == 'portfolio'
    previous = 237
    for period in saved['periods']:
        assert period['opening'] == previous
        for account in period['accounts']:
            assert account['opening'] == pytest.approx(previous/2)
            child = storage.load_study_from_db(account['studyId'])
            assert child['study']['config_json']['portfolio']['id'] == result['id']
            if with_wfa:
                capital = account['opening']
                for window, facts in zip(child['windows'], account['windows']):
                    assert window['oos_execution_capital'] == capital
                    assert window['best_params']['initialCapital'] == 237/2
                    capital = facts['closing']
            with pytest.raises(ValueError, match='Owned'):
                storage.delete_study(account['studyId'])
        previous = period['closing']
    assert saved['metrics']['profit_pct'] == pytest.approx(100*(previous/237-1))
    storage.DB_INITIALIZED = False
    storage.init_database()
    assert load_portfolio(result['id']) == saved
    assert storage.list_study_sets_with_analytics_cache()['sets'][0]['kind'] == 'portfolio'
    storage.delete_study_set(result['setId'])
    with storage.get_db_connection() as conn:
        assert conn.execute('SELECT COUNT(*) FROM studies').fetchone()[0] == 0
        assert conn.execute('SELECT COUNT(*) FROM portfolios').fetchone()[0] == 0


@pytest.mark.parametrize('stage',['cancel','funded','publish'])
def test_no_partial_publication(tmp_path,monkeypatch,stage):
    import core.portfolio as coordinator
    storage.create_new_db('T08 rollback')
    pool=publish(tmp_path/'pack')
    config=_build_optimization_config('',payload(),strategy_id=STRATEGY,warmup_bars=100)
    def failure(*args,**kwargs): raise ValueError('injected failure')
    if stage=='funded': monkeypatch.setattr(coordinator,'run_grid_optimization',failure)
    if stage=='publish':
        real=storage.save_grid_study_to_db
        def after_save(*args,**kwargs):
            real(*args,**kwargs)
            failure()
        monkeypatch.setattr(storage,'save_grid_study_to_db',after_save)
    with pytest.raises(ValueError):
        run_portfolio(config,selector(),pool,cancelled=lambda:stage=='cancel')
    assert_empty_database()


def test_comparison_failure_does_not_change_funding(tmp_path,monkeypatch):
    import core.portfolio as coordinator
    storage.create_new_db('T08 comparisons')
    pool=publish(tmp_path/'pack')
    config=_build_optimization_config('',payload(),strategy_id=STRATEGY,warmup_bars=100)
    baseline=run_portfolio(config,selector(topN=1),pool,publish=False)
    real=coordinator.run_grid_optimization
    calls=[]
    def execute(config,*args,**kwargs):
        calls.append((config.portfolio['period'],config.portfolio['account']))
        if config.portfolio['role']=='comparison':raise ValueError('comparison only')
        return real(config,*args,**kwargs)
    monkeypatch.setattr(coordinator,'run_grid_optimization',execute)
    result=run_portfolio(config,selector(topN=1,evaluateUnselected=True),pool)
    assert len(calls)==len(set(calls))==4
    assert result['status']=='complete_with_comparison_errors'
    assert result['metrics']==baseline['metrics']
    assert all(next(a for a in p['accounts'] if a['role']=='comparison')['status']=='failed' for p in result['periods'])


def test_idle_places_and_subeditor_internal_capital(tmp_path):
    storage.create_new_db('T08 idle')
    def listing(i,stamps,values,entry):
        if i:
            entry['instrument_rules']['listed_at_utc']='2024-02-01T00:00:00Z'
            entry['instrument_rules']['raw_contract_fields']['listTime']='1706745600000'
        return stamps,values,entry
    pool=publish(tmp_path/'pack',transform=listing)
    source=payload(); source['fixed_params']['initialCapital']=10
    config=_build_optimization_config('',source,strategy_id=STRATEGY,warmup_bars=100)
    result=run_portfolio(config,selector(),pool)
    first=result['periods'][0]
    idle=next(a for a in first['accounts'] if a['role']=='idle')
    assert idle['opening']==idle['closing']==5
    funded=next(a for a in first['accounts'] if a['role']=='funded')
    child=storage.load_study_from_db(funded['studyId'])
    assert child['study']['config_json']['fixed_params']['initialCapital']==5
    assert project(result,{'accounts':[idle['id']]})['profit_pct']==0


def test_lossless_fee_and_boundary_anchors():
    from types import SimpleNamespace
    from core.portfolio_projection import balance_path
    result=SimpleNamespace(timestamps=['2024-01-01T00:00Z','2024-01-01T01:00Z','2024-01-01T02:00Z','2024-01-01T03:00Z'],
                           balance_curve=[99,99,105,105])
    path=balance_path(result,'2024-01-01T00:00Z','2024-01-01T04:00Z',100,60)
    assert [p[1] for p in path]==[100,99,105,105]
    assert path[1][0].startswith('2024-01-01T01:00')


def test_four_rank_subset_gaps_depletion_and_no_reinjection():
    periods=[]
    for number in range(1,5):
        start=f'2024-0{number}-01';end=f'2024-0{number+1}-01'
        accounts=[dict(id=f'{number}:{rank}',period=number,role='funded',rank=rank,opening=100,closing=100+rank,
            path=[[start,100],[end,100+rank]]) for rank in range(1,5)]
        periods.append(dict(id=number,start=start,end=end,accounts=accounts,windows=[]))
    record=dict(selector={'topN':4},periods=periods,wfa=None)
    assert project(record)['profit_pct']==pytest.approx(100*(1.025**4-1))
    assert project(record,{'periods':[1,4]})['profit_pct']==pytest.approx(100*(1.025**2-1))
    curve=project(record,{'periods':[1,4]})
    assert curve['curve'][1:4]==[102.5]*3
    assert project(record,{'ranks':[1,2,3,4],'periods':[1,2,3,4]})==project(record)
    record['periods'][0]['accounts'][0].update(closing=-10,path=[['2024-01-01',100],['2024-02-01',-10]])
    ruined=project(record,{'ranks':[1]})
    assert ruined['depleted'] and ruined['curve'][1:]==[-10]*4
    assert ruined['profit_pct'] is None and ruined['consistency_score'] is None


def test_grid_calendar_reference_once_and_single_window_child(tmp_path,monkeypatch):
    from core.walkforward_engine import WalkForwardEngine
    from core.post_process import PostProcessConfig
    storage.create_new_db('T08 Grid WFA')
    pool=publish(tmp_path/'pack')
    settings=payload();settings.update(parameterMode='search',enabled_params={'stopX':True},param_ranges={'stopX':[2,3,1]},param_types={'stopX':'float'})
    settings['fixed_params'].update(start='2024-01-01',end='2024-05-31')
    config=_build_optimization_config('',settings,strategy_id=STRATEGY,warmup_bars=100)
    wfa=WFConfig(is_period_days=None,oos_period_days=None,period_unit='months',is_period_months=2,oos_period_months=1,strategy_id=STRATEGY,warmup_bars=100,
        post_process=PostProcessConfig(enabled=True,ft_period_days=3,top_k=1,ft_threshold_pct=-100000,warmup_bars=100))
    calls=[];real=WalkForwardEngine.run_window
    def once(self,df,window,**kwargs):
        calls.append((self.base_config_template['portfolio']['account'],window.window_id))
        return real(self,df,window,**kwargs)
    monkeypatch.setattr(WalkForwardEngine,'run_window',once)
    result=run_portfolio(config,selector(),pool,wfa=wfa)
    assert len(calls)==len(set(calls))==6
    assert [len(p['windows']) for p in result['periods']]==[2,1]
    for p in result['periods']:
        for account in p['accounts']:
            child=storage.load_study_from_db(account['studyId'])
            assert len(child['windows'])==len(p['windows'])
            assert all(w['best_params']['initialCapital']==237/2 for w in child['windows'])


@pytest.mark.parametrize('action', ['no_trade', 'cooldown_reoptimize'])
def test_rejected_forward_windows_preserve_actual_cash(tmp_path, action):
    from core.post_process import PostProcessConfig
    storage.create_new_db(f'T08 FT rejection {action}')
    pool = publish(tmp_path/'pack')
    settings = payload()
    settings.update(parameterMode='search', enabled_params={'stopX':True},
                    param_ranges={'stopX':[2,3,1]}, param_types={'stopX':'float'})
    config = _build_optimization_config('',settings,strategy_id=STRATEGY,warmup_bars=100)
    wfa = WFConfig(is_period_days=None,oos_period_days=None,period_unit='months',
        is_period_months=1,oos_period_months=1,strategy_id=STRATEGY,warmup_bars=100,
        post_process=PostProcessConfig(enabled=True,ft_period_days=3,top_k=1,
            ft_threshold_pct=1e12,ft_reject_action=action,ft_reject_max_attempts=1,warmup_bars=100))
    result = run_portfolio(config,selector(),pool,wfa=wfa)
    assert result['metrics']['total_trades'] == 0
    assert result['periods'][-1]['closing'] == 237
    for period in result['periods']:
        for account in period['accounts']:
            assert all(point[1] == 237/2 for point in account['path'])
            child = storage.load_study_from_db(account['studyId'])
            assert all(w['window_status'] == 'no_trade' for w in child['windows'])
            assert all(w['oos_execution_capital'] == 237/2 for w in child['windows'])


@pytest.mark.parametrize('funded_gap', [False, True])
def test_execution_gap_is_checked_only_for_consumed_accounts(tmp_path, funded_gap):
    import pandas as pd
    from core.ticker_selector import rank_pool
    storage.create_new_db(f'T08 consumed gaps {funded_gap}')
    clean = publish(tmp_path/'clean')
    selection = selector(topN=1)
    ranked = rank_pool(clean, '2024-02-01', selection)
    chosen = ranked[0]['instrument']
    def gap(i, stamps, values, entry):
        instrument = clean[i]['instrument_id']
        if (instrument == chosen) == funded_gap:
            keep = stamps != pd.Timestamp('2024-02-15',tz='UTC').value//10**6
            return stamps[keep],values[keep],entry
        return stamps,values,entry
    pool = publish(tmp_path/'gap',transform=gap)
    settings = payload(); settings['fixed_params']['end'] = '2024-02-29'
    config = _build_optimization_config('',settings,strategy_id=STRATEGY,warmup_bars=100)
    if funded_gap:
        with pytest.raises(ValueError, match='preparation'):
            run_portfolio(config,selection,pool)
        assert_empty_database()
    else:
        result = run_portfolio(config,selection,pool)
        assert result['metrics']['total_trades'] > 0


def test_account_pins_append_and_consumed_change(tmp_path):
    from core.data_input import replay_input
    import pandas as pd
    storage.create_new_db('T08 pins')
    pool=publish(tmp_path/'original')
    config=_build_optimization_config('',payload(),strategy_id=STRATEGY,warmup_bars=100)
    result=run_portfolio(config,selector(),pool)
    account=next(a for a in result['periods'][0]['accounts'] if a['role']=='funded')
    pin=storage.load_study_from_db(account['studyId'])['study']['config_json']['data_input']
    expected=replay_input(pin).frame
    longer=publish(tmp_path/'longer',end='2024-07-01')
    pd.testing.assert_frame_equal(replay_input(pin,root_override=longer[0]['pack_root']).frame,expected)
    def change(i,stamps,values,entry):
        values[stamps==pd.Timestamp('2024-02-15',tz='UTC').value//10**6,1]+=1
        return stamps,values,entry
    changed=publish(tmp_path/'changed',transform=change)
    with pytest.raises(ValueError,match='Pinned input changed'):
        replay_input(pin,root_override=changed[0]['pack_root'])


def test_successful_repeated_runs_have_distinct_owned_children(tmp_path):
    storage.create_new_db('T08 repeated')
    pool=publish(tmp_path/'pack')
    config=_build_optimization_config('',payload(),strategy_id=STRATEGY,warmup_bars=100)
    first=run_portfolio(config,selector(),pool)
    second=run_portfolio(config,selector(),pool)
    assert first['setId']!=second['setId'] and first['name']!=second['name']
    assert first['metrics']==second['metrics']
    assert storage.list_studies()==[]


def test_actual_capital_reference_replay_is_not_return_scaling(tmp_path):
    from core.data_input import replay_input
    from core.backtest_engine import prepare_dataset_with_warmup
    from core.engine_v2.admission import run_strategy
    from strategies import get_strategy
    import pandas as pd
    storage.create_new_db('T08 rounding')
    pool=publish(tmp_path/'pack')
    config=_build_optimization_config('',payload(),strategy_id=STRATEGY,warmup_bars=100)
    wfa=WFConfig(is_period_days=None,oos_period_days=None,period_unit='months',is_period_months=1,oos_period_months=1,
        strategy_id=STRATEGY,warmup_bars=100)
    saved=run_portfolio(config,selector(),pool,wfa=wfa)
    account=next(a for a in saved['periods'][0]['accounts'] if a['role']=='funded')
    child=storage.load_study_from_db(account['studyId']);window=child['windows'][1]
    from ui.server_data_input import execution_input
    _,prepared=execution_input(child['study'])
    start=pd.Timestamp(account['windows'][1]['start']);end=pd.Timestamp(account['windows'][1]['end'])-pd.Timedelta(hours=1)
    frame,idx=prepare_dataset_with_warmup(prepared.frame,start,end,100)
    reference=window['best_params']['initialCapital'];actual=window['oos_execution_capital']
    assert reference!=actual
    finals=[]
    for capital in (reference,actual):
        params={**window['best_params'],'initialCapital':capital,'dateFilter':True,'start':start,'end':end}
        execution=run_strategy(get_strategy(STRATEGY),frame,params,idx,admission_rules=prepared.admission_rules,entry_filter_context=prepared.entry_filter_context)
        finals.append(execution.balance_curve[-1])
        assert all(pd.Timestamp(t.entry_time)>=start for t in execution.trades)
    assert finals[1]==pytest.approx(account['windows'][1]['closing'])
    assert abs(finals[0]*actual/reference-finals[1])>1e-7


def test_event_point_consistency_independent_correlation():
    import numpy as np
    import pandas as pd
    times=pd.to_datetime(['2024-01-01','2024-01-03','2024-01-07','2024-01-25','2024-01-28','2024-01-30','2024-02-01'],utc=True)
    values=[100,99,104,103,108,107,112]
    path=[[t.isoformat(),v] for t,v in zip(times,values)]
    account=dict(id='a',period=1,role='funded',rank=1,opening=100,closing=112,path=path)
    result=project(dict(selector={'topN':1},wfa=None,periods=[dict(id=1,start=path[0][0],end=path[-1][0],accounts=[account])]))
    def expected(values):
        x=np.arange(len(values));y=np.log(values);x=x-x.mean();y=y-y.mean()
        covariance=float(sum(x*y));r2=covariance**2/(float(sum(x*x))*float(sum(y*y)))
        return math.copysign(r2,covariance)
    assert result['consistency_score_full']==pytest.approx(expected(values),abs=1e-6)
    assert result['consistency_score']==pytest.approx(expected(values[3:]),abs=1e-6)


def test_coordinator_depletion_stops_new_funding(tmp_path,monkeypatch):
    import core.portfolio as coordinator
    storage.create_new_db('T08 depletion')
    pool=publish(tmp_path/'pack');config=_build_optimization_config('',payload(),strategy_id=STRATEGY,warmup_bars=100)
    real=coordinator.run_grid_optimization;calls=[]
    def loss(config,*args,**kwargs):
        calls.append(config.portfolio['period'])
        results,elapsed=real(config,*args,**kwargs)
        results[0].portfolio_execution.balance_curve[-1]=-10
        return results,elapsed
    monkeypatch.setattr(coordinator,'run_grid_optimization',loss)
    result=run_portfolio(config,selector(rebalanceMonths=1),pool,publish=False)
    assert calls==[1,1]
    assert result['status']=='depleted' and result['periods'][0]['closing']==-20
    assert all(p['status']=='not_funded' for p in result['periods'][1:])
