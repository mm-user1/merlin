"""Quantity-only portfolios through real coordinator, SQLite and replay routes."""
from copy import deepcopy
import csv
import io
import json
import pandas as pd
import pytest

from core import storage
from core.portfolio_storage import load_portfolio
from core.engine_v2.quantity import read_quantity_envelope
from strategies import get_strategy_config
from tests._portfolio_helpers import publish, selector, portfolio_database
from tests._quantity_helpers import LEGACY,trading_pack
from ui.server_services import _resolve_stored_execution_context

pytestmark = pytest.mark.usefixtures('portfolio_database')


def request_config(sid, *, mode='fixed', end='2024-04-30'):
    spec = get_strategy_config(sid)
    manual = spec['parameters']['contractSize'].get('options', [.1])[-1]
    return dict(parameterMode=mode, optimization_mode='grid', enabled_params={}, param_ranges={},
        fixed_params=dict(dateFilter=True, start='2024-02-01', end=end, initialCapital=237,
                          enableLong=True, enableShort=True, contractSize=manual,
                          **({'useRegime':False} if 'useRegime' in spec['parameters'] else {})), worker_processes=1)


def launch(client, pool, sid, *, mode='fixed', wfa=False, **changes):
    cfg=request_config(sid,mode=mode)
    if mode=='search':
        name='maLength3' if sid.startswith('s03') else 'stopX'
        cfg.update(enabled_params={name:True},param_ranges={name:[25,30,5] if name=='maLength3' else [2,3,1]},
                   param_types={name:'int' if name=='maLength3' else 'float'})
    cfg.update(changes)
    form=dict(strategy=sid, config=json.dumps(cfg), sources=json.dumps(pool),
        tickerSelector=json.dumps(selector()), warmupBars='100')
    if wfa:
        form.update(wf_enabled='true', wf_period_unit='months', wf_is_period_months='1', wf_oos_period_months='1')
    response=client.post('/api/portfolio',data=form)
    assert response.status_code==200,response.get_data(as_text=True)
    return load_portfolio(response.get_json()['portfolio_id'])


def assert_child(saved, sid):
    cfg=saved['study']['config_json']
    rules=read_quantity_envelope(cfg['quantity_execution'])
    assert all(k not in cfg for k in ('admission_rules','quantity_rules','entry_admission','entryFilters','entry_filter_inputs'))
    assert 'maxEntryLeverage' not in cfg['fixed_params']
    assert cfg['fixed_params']['contractSize']==get_strategy_config(sid)['parameters']['contractSize']['default']
    assert 'quantity_rules' not in cfg.get('grid_settings',{})
    _resolve_stored_execution_context(saved['study'])
    return rules


@pytest.mark.parametrize('sid',LEGACY)
def test_each_config_fixed_quantity_replay_and_manual(client,tmp_path,sid):
    pool=trading_pack(tmp_path/'pack')
    record=launch(client,pool,sid)
    assert 'quantity_rules' not in record['request'] and 'quantity_execution' not in record['request']
    assert record['metrics']['total_trades']>0
    for period in record['periods']:
        for account in period['accounts']:
            if account['role']!='funded':continue
            saved=storage.load_study_from_db(account['studyId']);assert_child(saved,sid)
            assert account['diagnostics'] is None
            trial=saved['trials'][0];number=trial['trial_number'];base=f"/api/studies/{account['studyId']}"
            response=client.post(f'{base}/trials/{number}/equity',json={})
            assert response.status_code==200,response.get_data(as_text=True)
            assert response.get_json()['equity'][-1]==pytest.approx(account['closing'])
            assert client.post(f'{base}/trials/{number}/trades',json={}).status_code==200
    account=record['periods'][0]['accounts'][0];base=f"/api/studies/{account['studyId']}"
    saved=storage.load_study_from_db(account['studyId']);number=saved['trials'][0]['trial_number']
    period=record['periods'][0]
    response=client.post(base+'/test',json=dict(dataSource='original_parquet',sourceTab='optuna',
        startDate=period['start'],endDate=(pd.Timestamp(period['end'])-pd.Timedelta(hours=1)).isoformat(),trialNumbers=[number]))
    assert response.status_code==200,response.get_data(as_text=True)
    test_id=response.get_json()['test_id'];manual=storage.load_manual_test_results(account['studyId'],test_id)
    assert 'quantity_execution' in manual['results_json']['config']
    assert 'entry_admission' not in manual['results_json']['config']
    replay=client.post(f'{base}/trials/{number}/equity',json=dict(module='manual_tests',testId=test_id))
    assert replay.status_code==200,replay.get_data(as_text=True)
    assert replay.get_json()['equity'][-1]==pytest.approx(account['closing'])


@pytest.mark.parametrize('sid',[LEGACY[0],LEGACY[2]])
@pytest.mark.parametrize('mode',['fixed','search'])
def test_owned_wfa_quantity_capital_and_missing_envelope(client,tmp_path,sid,mode):
    pool=trading_pack(tmp_path/'pack')
    record=launch(client,pool,sid,mode=mode,wfa=True)
    account=next(a for a in record['periods'][0]['accounts'] if a['role']=='funded')
    saved=storage.load_study_from_db(account['studyId']);assert_child(saved,sid)
    assert len(saved['windows'])==2
    first,last=saved['windows']
    assert last['oos_execution_capital']==pytest.approx(account['windows'][0]['closing'])
    assert last['oos_execution_capital']!=237/2
    assert last['best_params']['initialCapital']==237/2
    for window in saved['windows']:
        base=f"/api/studies/{account['studyId']}/wfa/windows/{window['window_number']}"
        response=client.post(base+'/equity',json={'period':'oos'})
        assert response.status_code==200,response.get_data(as_text=True)
        facts=next(w for w in account['windows'] if w['id']==window['window_number'])
        assert response.get_json()['equity_curve'][-1]==pytest.approx(facts['closing'])
        assert client.post(base+'/trades',json={'period':'oos'}).status_code==200
        is_response=client.post(base+'/equity',json={'period':'is'})
        assert is_response.status_code==200,is_response.get_data(as_text=True)
        assert is_response.get_json()['equity_curve'][0]==237/2
    number=last['window_number'];base=f"/api/studies/{account['studyId']}";facts=account['windows'][-1]
    response=client.post(base+'/test',json=dict(dataSource='original_parquet',sourceTab='portfolio_window',
        windowNumber=number,period='oos',startDate=last['oos_start_date'],endDate=last['oos_end_date'],trialNumbers=[number]))
    assert response.status_code==200,response.get_data(as_text=True)
    test_id=response.get_json()['test_id']
    manual=storage.load_manual_test_results(account['studyId'],test_id)
    assert manual['results_json']['config']['portfolio_source']['initialCapital']==last['oos_execution_capital']
    assert 'quantity_execution' in manual['results_json']['config']
    response=client.post(f'{base}/trials/{number}/equity',json={'module':'manual_tests','testId':test_id})
    assert response.status_code==200,response.get_data(as_text=True)
    assert response.get_json()['equity'][-1]==pytest.approx(facts['closing'])
    assert client.post(f'{base}/tests/{test_id}/trials/{number}/mt-trades').status_code==200
    missing=deepcopy(saved['study']);missing['config_json'].pop('quantity_execution')
    with pytest.raises(ValueError,match='quantity_execution'):
        _resolve_stored_execution_context(missing)
    forged=deepcopy(saved['study']);forged['study_id']='unowned'
    with pytest.raises(ValueError,match='ownership'):
        _resolve_stored_execution_context(forged)


@pytest.mark.parametrize('sid',[LEGACY[0],LEGACY[2]])
@pytest.mark.parametrize('reject',[False,True])
def test_grid_calendar_with_ft_and_stress_spawned_together(tmp_path,sid,reject):
    from core.portfolio import run_portfolio
    from core.walkforward_engine import WFConfig
    from core.post_process import PostProcessConfig,StressTestConfig
    from ui.server_services import _build_optimization_config
    settings=request_config(sid,mode='search',end='2024-04-30')
    name='maLength3' if sid.startswith('s03') else 'stopX'
    settings.update(enabled_params={name:True},param_ranges={name:[25,30,5] if name=='maLength3' else [2,3,1]},
        param_types={name:'int' if name=='maLength3' else 'float'},worker_processes=2)
    config=_build_optimization_config('',settings,strategy_id=sid,warmup_bars=100)
    wfa=WFConfig(is_period_days=None,oos_period_days=None,period_unit='months',is_period_months=1,oos_period_months=1,
        strategy_id=sid,warmup_bars=100,
        post_process=PostProcessConfig(enabled=True,ft_period_days=3,top_k=2,ft_threshold_pct=1e12 if reject else -1e12,warmup_bars=100),
        stress_test_config=StressTestConfig(enabled=True,top_k=1,failure_threshold=-1e12,warmup_bars=100))
    result=run_portfolio(config,selector(topN=1),trading_pack(tmp_path/'pack'),wfa=wfa)
    account=result['periods'][0]['accounts'][0]
    saved=storage.load_study_from_db(account['studyId']);assert_child(saved,sid)
    window=saved['windows'][0]
    assert window['module_status']['forward_test']['ran']
    if reject:
        assert account['closing']==account['opening']==237
        assert window['window_status']=='no_trade' and window['oos_execution_capital']==237
    else:
        assert window['module_status']['stress_test']['ran']
        assert window['oos_execution_capital']==237
        assert account['trades']>0


@pytest.mark.parametrize('sid',[LEGACY[0],LEGACY[2]])
def test_allocation_changes_actual_lots_and_oos_refusal_changes_carry(client,tmp_path,sid):
    from core.portfolio import run_portfolio
    from core.walkforward_engine import WFConfig
    from ui.server_services import _build_optimization_config
    pool=trading_pack(tmp_path/'pack')
    sizes=[]
    for top in (1,2):
        config=_build_optimization_config('',request_config(sid),strategy_id=sid,warmup_bars=100)
        record=run_portfolio(config,selector(topN=top,rebalanceMonths=1),pool)
        account=record['periods'][0]['accounts'][0]
        saved=storage.load_study_from_db(account['studyId']);number=saved['trials'][0]['trial_number']
        response=client.post(f"/api/studies/{account['studyId']}/trials/{number}/trades",json={})
        assert response.status_code==200,response.get_data(as_text=True)
        rows=list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
        assert rows
        sizes.append(float(rows[0]['Qty']))
    assert sizes[0]>sizes[1]>0
    refused=launch(client,trading_pack(tmp_path/'minimum',minimum='1000000'),sid,wfa=True)
    ordinary=launch(client,pool,sid,wfa=True)
    for account in refused['periods'][0]['accounts']:
        windows=storage.load_study_from_db(account['studyId'])['windows']
        assert account['trades']==0 and account['closing']==account['opening']==237/2
        assert all(w['oos_execution_capital']==237/2 and w['entry_diagnostics'] is None for w in windows)
        assert account['windows'][1]['closing']==237/2
    assert ordinary['periods'][0]['closing']!=237
    assert any(w['oos_execution_capital']!=237/2 for a in ordinary['periods'][0]['accounts']
        for w in storage.load_study_from_db(a['studyId'])['windows'])


@pytest.mark.parametrize('mode',['fixed','search'])
def test_relocation_append_only_offline_and_fail_closed_metadata(client,tmp_path,mode):
    sid=LEGACY[0];record=launch(client,trading_pack(tmp_path/'pack'),sid,mode=mode,wfa=mode=='search')
    account=record['periods'][0]['accounts'][0];identifier=account['studyId'];base=f'/api/studies/{identifier}'
    saved=storage.load_study_from_db(identifier);cfg=deepcopy(saved['study']['config_json'])
    number=saved['trials'][0]['trial_number'] if mode=='fixed' else saved['windows'][0]['window_number']
    route=f'{base}/trials/{number}/equity' if mode=='fixed' else f'{base}/wfa/windows/{number}/equity'
    body={} if mode=='fixed' else {'period':'oos'}
    def save(config):
        with storage.get_db_connection() as conn:
            conn.execute('UPDATE studies SET config_json=? WHERE study_id=?',(json.dumps(config),identifier));conn.commit()
    altered=[]
    for value in [None,{},dict(cfg['quantity_execution'],version=2)]:
        item=deepcopy(cfg);item['quantity_execution']=value;altered.append(item)
    for key in ('quantity_execution','portfolio'):
        item=deepcopy(cfg);item.pop(key);altered.append(item)
    for key in ('entry_admission','entryFilters','entry_filter_inputs'):
        item=deepcopy(cfg);item[key]=None;altered.append(item)
    item=deepcopy(cfg);item['portfolio']['account']='forged';altered.append(item)
    for invalid in altered:
        save(invalid)
        response=client.post(route,json=body);assert response.status_code==400,response.get_data(as_text=True)
        assert 'quantity' in response.get_data(as_text=True).lower()
    save(cfg)
    changed=trading_pack(tmp_path/'changed',minimum='1')
    response=client.post(base+'/locate-parquet-pack',json={'packRoot':changed[0]['pack_root']})
    assert response.status_code==400,response.get_data(as_text=True)
    assert storage.load_study_from_db(identifier)['study']['config_json']==cfg
    appended=trading_pack(tmp_path/'appended',end='2024-07-01')
    response=client.post(base+'/locate-parquet-pack',json={'packRoot':appended[0]['pack_root']})
    assert response.status_code==200,response.get_data(as_text=True)
    updated=storage.load_study_from_db(identifier)['study']['config_json']
    assert updated['data_input']==cfg['data_input'] and updated['quantity_execution']==cfg['quantity_execution']
    assert client.post(route,json=body).status_code==200
    (tmp_path/'appended').rename(tmp_path/'offline')
    assert client.get(base).status_code==200
    assert client.post(route,json=body).status_code==400


@pytest.mark.parametrize('sid',LEGACY)
def test_server_capability_and_preview_do_not_read_packs(client,monkeypatch,sid):
    from core import data_input
    from ui import server_routes_run
    def forbidden(*args,**kwargs):raise AssertionError('quantity Preview must not read or resolve packs')
    monkeypatch.setattr(server_routes_run,'preview_request_rules',forbidden)
    monkeypatch.setattr(data_input,'pack_session',forbidden)
    response=client.get(f'/api/strategy/{sid}/config')
    assert response.status_code==200,response.get_data(as_text=True)
    assert response.get_json()['tickerSelectorCapability']=={'policy':'instrument_rules_v1'}
    response=client.post('/api/grid/preview',json=dict(strategyId=sid,config=request_config(sid),warmupBars=100,
        tickerSelector=selector(),source=dict(type='parquet',version=1,pack_root='/absent',instrument_id='ABSENT',timeframe_minutes=60)))
    assert response.status_code==200,response.get_data(as_text=True)
    assert response.get_json()['preview']['planned_candidate_count']==1
