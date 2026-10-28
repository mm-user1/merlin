"""Registered S07 through real HTTP, Queue, storage and exact reconstruction."""
from copy import deepcopy
import json

import pytest

from core import storage
from core.entry_filter_input import describe_saved
from tests._s07_helpers import STRATEGY,publish,payload


def post(client,path,**fields):
    response=client.post(path,data={k:json.dumps(v) if isinstance(v,dict) else v for k,v in fields.items()})
    assert response.status_code==200,response.get_data(as_text=True)
    return response.get_json()


@pytest.mark.parametrize('fixed,enabled',[(True,False),(True,True),(False,False),(False,True)])
def test_optimize_queue_replay_manual_export(client,tmp_path,fixed,enabled):
    source=publish(tmp_path/'pack');config=payload(source,fixed=fixed,enabled=enabled)
    config['oosTest']=dict(enabled=True,periodDays=3,topK=2)
    if not fixed:
        config['postProcess']=dict(enabled=True,ftPeriodDays=3,topK=2,ftThresholdPct=-100000,
            stressTest=dict(enabled=True,topK=2))
    queue=dict(items=[dict(id='s07',index=1,strategyId=STRATEGY,sources=[source],sourceCursor=0,config=config)],nextIndex=2,runtime={'active':False})
    assert client.put('/api/queue',json=queue).status_code==200
    restored=client.get('/api/queue').get_json()['items'][0]
    assert restored['config']==config
    preview=client.post('/api/grid/preview',json=dict(strategyId=STRATEGY,source=source,config=config))
    assert preview.status_code==200,preview.get_data(as_text=True)
    assert preview.get_json()['preview']['planned_candidate_count']==(1 if fixed else 2)
    response=post(client,'/api/optimize',strategy=STRATEGY,source=source,config=config,warmupBars='100')
    identity=response['study_id'];saved=storage.load_study_from_db(identity)
    study=saved['study'];trial=saved['trials'][0]
    assert study['strategy_id']==STRATEGY
    assert trial['params']['maxEntryLeverage']==8
    assert trial['total_trades']>0
    if fixed: assert trial['net_profit_pct']<0  # Losing reference results survive.
    assert describe_saved(study['config_json'])['key'] not in (None,)
    base=f"/api/studies/{identity}/trials/{trial['trial_number']}"
    modules=[('optuna','trades'),('oos_test','oos-trades')]
    if not fixed: modules += [('forward_test','ft-trades'),('stress_test','trades')]
    for module,download in modules:
        a=client.post(base+'/equity',json={'module':module})
        assert a.status_code==200,a.get_data(as_text=True)
        assert a.get_json()==client.post(base+'/equity',json={'module':module}).get_json()
        assert client.post(base+'/'+download,json={}).status_code==200
    manual=client.post(f'/api/studies/{identity}/test',json=dict(dataSource='original_parquet',
        startDate='2024-01-10',endDate='2024-01-15',trialNumbers=[trial['trial_number']],sourceTab='optuna'))
    assert manual.status_code==200,manual.get_data(as_text=True)
    test_id=manual.get_json()['test_id']
    own=storage.load_manual_test_results(identity,test_id)['results_json']['config']
    assert own['entryFilters']==config['entryFilters']
    assert client.post(base+'/equity',json=dict(module='manual_tests',testId=test_id)).status_code==200
    assert set(trial['entry_diagnostics']) >= {'is','oos_test'}
    if enabled:
        pin=study['config_json']['entry_filter_inputs']['sources'][0]
        assert pin['originRule']=='declared_source_origin'
        assert own['entry_filter_inputs']['sources'][0]['bounds']['start']==pin['bounds']['start']


@pytest.mark.parametrize('mode',['fixed','grid','adaptive'])
def test_wfa_and_analytics(client,tmp_path,mode):
    source=publish(tmp_path/'pack');config=payload(source,fixed=mode=='fixed')
    response=post(client,'/api/walkforward',strategy=STRATEGY,source=source,config=config,warmupBars='100',
        wf_store_top_n_trials='2',wf_adaptive_mode=str(mode=='adaptive').lower(),wf_is_period_days='5',wf_oos_period_days='4',
        wf_max_oos_period_days='4',wf_min_oos_trades='1',wf_check_interval_trades='1')
    identity=response['study_id'];saved=storage.load_study_from_db(identity)
    assert len(saved['windows'])>=2
    for window in saved['windows'][:2]:
        for period in ('is','oos'):
            response=client.post(f"/api/studies/{identity}/wfa/windows/{window['window_number']}/equity",json={'period':period})
            assert response.status_code==200,response.get_data(as_text=True)
    summary=client.get('/api/analytics/summary').get_json()
    row=next(s for s in summary['studies'] if s['study_id']==identity)
    assert row['entry_filters']==describe_saved(saved['study']['config_json'])
    assert 'N 3 / break 2' in row['entry_filters']['text']


def test_fixed_no_entries_and_invalid_parameters(client,tmp_path):
    source=publish(tmp_path/'pack');config=payload(source,fixed=True,enabled=False)
    config['fixed_params'].update(enableLong=False,enableShort=False)
    response=post(client,'/api/optimize',strategy=STRATEGY,source=source,config=config,warmupBars='100')
    saved=storage.load_study_from_db(response['study_id'])
    assert len(saved['trials'])==1 and saved['trials'][0]['total_trades']==0
    for invalid in ({'maxEntryLeverage':12},{'stopLP':5},{'entryRunLength':True}):
        bad=deepcopy(config);bad['fixed_params'].update(invalid)
        response=client.post('/api/optimize',data=dict(strategy=STRATEGY,source=json.dumps(source),config=json.dumps(bad),warmupBars='100'))
        assert response.status_code==400,response.get_data(as_text=True)
