"""Real HTTP launch, owned Results and offline projections with synthetic packs."""
import json
from copy import deepcopy

import pytest

from core import storage
from tests._portfolio_helpers import STRATEGY, payload, publish, selector, portfolio_database

pytestmark = pytest.mark.usefixtures('portfolio_database')


@pytest.mark.parametrize('wfa',[False,True])
def test_http_launch_replay_manual_and_offline(client,tmp_path,wfa):
    pool = publish(tmp_path/'pack')
    form = dict(strategy=STRATEGY, config=json.dumps(payload()), sources=json.dumps(pool),
                tickerSelector=json.dumps(selector()),warmupBars='100')
    if wfa:
        form.update(wf_enabled='true',wf_period_unit='months',wf_is_period_months='1',wf_oos_period_months='1')
    response = client.post('/api/portfolio',data=form)
    assert response.status_code == 200, response.get_data(as_text=True)
    identifier = response.get_json()['portfolio_id']
    view = client.get(f'/api/portfolios/{identifier}?period=1').get_json()
    child = view['rows'][0]['resultStudyId']
    detail = client.get(f'/api/studies/{child}').get_json()
    assert detail['portfolio']['portfolioId'] == identifier
    assert client.delete(f'/api/studies/{child}').status_code == 400
    assert client.get('/api/studies').get_json()['studies'] == []
    assert client.get('/api/analytics/summary').get_json()['studies'] == []
    selection = dict(portfolios=[dict(id=identifier,selection=dict(ranks=[1,2]))])
    projected = client.post('/api/portfolios/equity',json=selection)
    assert projected.status_code == 200, projected.get_data(as_text=True)
    if wfa:
        window = detail['windows'][0]
        number = window['window_number']
        response = client.post(f'/api/studies/{child}/wfa/windows/{number}/equity',json={'period':'oos'})
        assert response.status_code == 200, response.get_data(as_text=True)
        assert response.get_json()['equity_curve'][-1] == pytest.approx(detail['portfolio']['account']['windows'][0]['closing'])
        exported = client.post(f'/api/studies/{child}/wfa/windows/{number}/trades',json={'period':'oos'})
        assert exported.status_code == 200,exported.get_data(as_text=True)
        manual = client.post(f'/api/studies/{child}/test',json=dict(dataSource='original_parquet',startDate='2024-03-01',endDate='2024-03-20',
            trialNumbers=[number],sourceTab='portfolio_window',windowNumber=number,period='oos'))
        assert manual.status_code == 200, manual.get_data(as_text=True)
        test = storage.load_manual_test_results(child,manual.get_json()['test_id'])
        assert test['results_json']['config']['portfolio_source']['initialCapital'] == window['oos_execution_capital']
        manual_id = manual.get_json()['test_id']
        replay = client.post(f'/api/studies/{child}/trials/{number}/equity',json={'module':'manual_tests','testId':manual_id})
        assert replay.status_code == 200,replay.get_data(as_text=True)
        exported = client.post(f'/api/studies/{child}/tests/{manual_id}/trials/{number}/mt-trades')
        assert exported.status_code == 200,exported.get_data(as_text=True)
    (tmp_path/'pack').rename(tmp_path/'offline-pack')
    offline = client.get(f'/api/studies/{child}')
    assert offline.status_code == 200
    assert client.post('/api/portfolios/equity',json=selection).get_json() == projected.get_json()
    bad = client.post('/api/portfolios/equity',json={'portfolios':[{'id':identifier,'selection':{'accounts':['foreign']}}]})
    assert bad.status_code == 400


def test_request_rejected_before_publication(client,tmp_path):
    pool = publish(tmp_path/'pack')
    for edit in ({'topN':3},{'method':'future'},{'rebalanceMonths':0}):
        response = client.post('/api/portfolio',data=dict(strategy=STRATEGY,config=json.dumps(payload()),sources=json.dumps(pool),
            tickerSelector=json.dumps(selector(**edit)),warmupBars='100'))
        assert response.status_code == 400,response.get_data(as_text=True)
    with storage.get_db_connection() as conn:
        assert conn.execute('SELECT COUNT(*) FROM studies').fetchone()[0] == 0
        assert conn.execute('SELECT COUNT(*) FROM study_sets').fetchone()[0] == 0


def test_mixed_members_equal_weights_and_no_overlap(client):
    start,end='2024-01-01T00:00:00+00:00','2024-03-01T00:00:00+00:00'
    record=dict(id='pf',selector={'topN':1},wfa=None,periods=[dict(id=1,start=start,end=end,windows=[],accounts=[
        dict(id='account',period=1,rank=1,role='funded',opening=100,closing=110,path=[[start,100],[end,110]])])])
    from core.portfolio_projection import project
    record.update(status='complete', metrics=project(record))
    with storage.get_db_connection() as conn:
        set_id=conn.execute("INSERT INTO study_sets(name,sort_order) VALUES ('PF',1)").lastrowid
        conn.execute('INSERT INTO portfolios VALUES (?,?,?)',('pf',set_id,json.dumps(record)))
        for number in range(3):
            conn.execute('INSERT INTO studies(study_id,study_name,strategy_id,optimization_mode,config_json,stitched_oos_equity_curve,stitched_oos_timestamps_json,stitched_oos_point_count,stitched_oos_start_ts,stitched_oos_end_ts) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (str(number),'Ordinary '+str(number),STRATEGY,'wfa','{}',json.dumps([100,120]),json.dumps([start,end]),2,start,end))
        conn.commit()
    assert storage.list_study_sets()[0]['kind'] == 'portfolio'
    payload={'portfolios':[{'id':'pf','selection':{'ranks':[1],'periods':[1]}}], 'studyIds':['0','1','2','0']}
    response=client.post('/api/portfolios/equity',json=payload)
    assert response.status_code==200,response.get_data(as_text=True)
    assert response.get_json()['profit_pct']==17.5
    assert response.get_json()['studies_used']==4
    assert response.get_json()['entry_diagnostics'] is None
    with storage.get_db_connection() as conn:
        conn.execute('UPDATE studies SET stitched_oos_timestamps_json=? WHERE study_id=?',(json.dumps(['2025-01-01','2025-03-01']),'2'));conn.commit()
    assert not client.post('/api/portfolios/equity',json=payload).get_json()['curve']
