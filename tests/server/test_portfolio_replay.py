"""Stored internal capital is distinct from user-entered Fixed scalar bounds."""
import csv
import hashlib
import io
import json
from copy import deepcopy
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

from core import storage
from core.portfolio_storage import load_portfolio
from tests._portfolio_helpers import STRATEGY, payload, publish, selector, portfolio_database
from ui.server_services import _resolve_stored_execution_context

pytestmark = pytest.mark.usefixtures('portfolio_database')


def launch(client, tmp_path, mode='fixed', total=.01):
    pool = publish(tmp_path/'pack')
    settings = payload()
    settings['fixed_params'].update(initialCapital=total)
    if total == 1_000_000:
        settings['fixed_params'].update(start='2024-03-01',end='2024-04-30')
    if mode == 'grid_wfa':
        settings.update(parameterMode='search',enabled_params={'stopX':True},
                        param_ranges={'stopX':[2,3,1]},param_types={'stopX':'float'})
    form = dict(strategy=STRATEGY,config=json.dumps(settings),sources=json.dumps(pool),
        tickerSelector=json.dumps(selector(topN=1 if total == 1_000_000 else 2,rebalanceMonths=1)),warmupBars='100')
    if mode != 'fixed':
        form.update(wf_enabled='true',wf_period_unit='months',wf_is_period_months='1',wf_oos_period_months='1')
    response = client.post('/api/portfolio',data=form)
    assert response.status_code == 200,response.get_data(as_text=True)
    record = load_portfolio(response.get_json()['portfolio_id'])
    period = record['periods'][-1 if total == 1_000_000 else 0]
    account = next(a for a in period['accounts'] if a['role'] == 'funded')
    return record,period,account,storage.load_study_from_db(account['studyId'])


def original_rows():
    with storage.get_db_connection() as conn:
        return {table:[tuple(row) for row in conn.execute(f'SELECT * FROM {table} ORDER BY rowid')]
                for table in ['studies','trials','wfa_windows','portfolios','portfolio_accounts']}


def pack_hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file() and p.suffix in {'.parquet','.json'}}


def assert_csv(response, count):
    assert response.status_code == 200,response.get_data(as_text=True)
    rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) == count*2
    return rows


@pytest.mark.parametrize('mode,total', [('fixed',1_000_000),('fixed',.01),('fixed_wfa',.01),('grid_wfa',.01),('fixed',237)])
def test_internal_capital_replay_manual_and_fresh_process(client,tmp_path,mode,total):
    record,period,account,saved = launch(client,tmp_path,mode,total)
    before = original_rows(); data_before = pack_hashes(tmp_path/'pack')
    study_id = account['studyId']; base = f'/api/studies/{study_id}'
    capital = account['opening']; closing = account['closing']
    if total == 1_000_000:
        assert record['periods'][0]['closing'] > total
        assert capital > 1_000_000 and account['trades'] > 0
    elif total == .01:
        assert capital == .005
    else:
        assert capital == 118.5
    context = _resolve_stored_execution_context(saved['study'])
    assert context.base_params['initialCapital'] == (capital if mode == 'fixed' else total/2)
    if mode == 'fixed':
        number = saved['trials'][0]['trial_number']
        route = f'{base}/trials/{number}/equity'; field='equity'; body={}
        exported = assert_csv(client.post(f'{base}/trials/{number}/trades',json={}),account['trades'])
        manual_source = dict(sourceTab='optuna')
        # Independent generic execution also checks export quantities and prices.
        from ui.server_data_input import execution_input
        from core.backtest_engine import prepare_dataset_with_warmup
        from core.engine_v2.admission import run_strategy
        from strategies import get_strategy
        from core.export import export_trades_csv
        _, prepared = execution_input(saved['study'])
        start,end=pd.Timestamp(period['start']),pd.Timestamp(period['end'])-pd.Timedelta(hours=1)
        frame,idx=prepare_dataset_with_warmup(prepared.frame,start,end,100)
        reference=run_strategy(get_strategy(STRATEGY),frame,context.params_for(saved['trials'][0]['params'],operation_start=start,operation_end=end),idx,
            admission_rules=prepared.admission_rules,entry_filter_context=prepared.entry_filter_context)
        assert reference.balance_curve[-1] == pytest.approx(closing)
        expected=list(csv.DictReader(io.StringIO(export_trades_csv(reference.trades))))
        assert [{k:v for k,v in r.items() if k!='Symbol'} for r in exported] == [{k:v for k,v in r.items() if k!='Symbol'} for r in expected]
    else:
        window=saved['windows'][0];number=window['window_number']
        assert window['best_params']['initialCapital'] == total/2
        assert window['oos_execution_capital'] == capital
        route=f'{base}/wfa/windows/{number}/equity';field='equity_curve';body={'period':'oos'}
        assert_csv(client.post(f'{base}/wfa/windows/{number}/trades',json=body),account['trades'])
        assert_csv(client.post(f'{base}/wfa/trades'),account['trades'])
        is_replay=client.post(route,json={'period':'is'})
        assert is_replay.status_code==200,is_replay.get_data(as_text=True)
        assert is_replay.get_json()['equity_curve'][0] == total/2
        manual_source=dict(sourceTab='portfolio_window',windowNumber=number,period='oos')
    replay=client.post(route,json=body)
    assert replay.status_code==200,replay.get_data(as_text=True)
    assert replay.get_json()[field][-1] == pytest.approx(closing)
    manual=client.post(base+'/test',json=dict(dataSource='original_parquet',startDate=period['start'],
        endDate=(pd.Timestamp(period['end'])-pd.Timedelta(hours=1)).isoformat(),trialNumbers=[number],**manual_source))
    assert manual.status_code==200,manual.get_data(as_text=True)
    test_id=manual.get_json()['test_id']
    manual_equity=client.post(f'{base}/trials/{number}/equity',json={'module':'manual_tests','testId':test_id})
    assert manual_equity.status_code==200,manual_equity.get_data(as_text=True)
    assert manual_equity.get_json()['equity'][-1] == pytest.approx(closing)
    assert_csv(client.post(f'{base}/tests/{test_id}/trials/{number}/mt-trades'),account['trades'])
    stored_test=storage.load_manual_test_results(study_id,test_id)
    assert stored_test['results_json']['config']['fixed_params']['initialCapital'] == capital

    # No coordinator state crosses this process boundary. The oracle is the
    # persisted original execution, not a freshly launched portfolio.
    code = '''import json,os,sys
from pathlib import Path
root,db,pack,route,body,field,expected=sys.argv[1:]
sys.path[:0]=[str(Path(root)/'src'),root]
os.environ['MERLIN_CSV_ALLOWED_ROOTS']=str(Path(pack).parent)
from core import storage
storage.STORAGE_DIR=Path(db).parent
storage.JOURNAL_DIR=Path(db).parent/'fresh-journals'
storage._active_db_path=Path(db);storage.DB_INITIALIZED=False
storage.init_database()
from ui.server import app
with app.test_client() as client:
    response=client.post(route,json=json.loads(body))
    assert response.status_code==200,response.get_data(as_text=True)
    actual=response.get_json()[field][-1]
    assert abs(actual-float(expected)) <= max(1e-10,abs(float(expected))*1e-10),(actual,expected)
    print(json.dumps(dict(actual=actual,expected=float(expected),db=db)))
'''
    root=Path(__file__).resolve().parents[2]
    db=storage.STORAGE_DIR/storage.get_active_db_name()
    process=subprocess.run([sys.executable,'-I','-B','-c',code,str(root),str(db),str(tmp_path/'pack'),route,json.dumps(body),field,str(closing)],
        cwd=tmp_path,capture_output=True,text=True,timeout=90)
    assert process.returncode==0,process.stdout+process.stderr
    (tmp_path/'fresh-process.log').write_text(process.stdout+process.stderr,encoding='utf-8')
    assert original_rows()==before
    assert pack_hashes(tmp_path/'pack')==data_before


def test_stored_capital_validation_and_untrusted_launch_metadata(client,tmp_path):
    _,_,account,saved=launch(client,tmp_path)
    study=saved['study'];before=original_rows()
    for invalid in [True,False,None,'0.005','bad',0,-1,float('nan'),float('inf'),float('-inf')]:
        candidate=deepcopy(study);candidate['config_json']['fixed_params']['initialCapital']=invalid
        with pytest.raises(ValueError):_resolve_stored_execution_context(candidate)
    candidate=deepcopy(study);del candidate['config_json']['fixed_params']['initialCapital']
    with pytest.raises(ValueError):_resolve_stored_execution_context(candidate)
    for edit in ['ordinary','foreign_owner','bad_scalar','bad_runtime']:
        candidate=deepcopy(study)
        if edit=='ordinary':del candidate['config_json']['portfolio']
        elif edit=='foreign_owner':candidate['study_id']='not-an-owned-study'
        elif edit=='bad_scalar':candidate['config_json']['fixed_params']['stopX']=-1
        else:candidate['config_json']['v2_runtime']={'contract_version':'unsupported'}
        with pytest.raises(ValueError):_resolve_stored_execution_context(candidate)
    for total in [.005,1_000_001]:
        settings=payload();settings['fixed_params']['initialCapital']=total
        settings['portfolio']=deepcopy(study['config_json']['portfolio'])
        response=client.post('/api/optimize',data=dict(strategy=STRATEGY,config=json.dumps(settings),
            source=json.dumps({'type':'parquet','version':1,'pack_root':str(tmp_path/'pack'),
                               'instrument_id':'OKX_AAA-USDT-SWAP','timeframe_minutes':60}),warmupBars='100'))
        assert response.status_code==400,response.get_data(as_text=True)
    def changed(i,stamps,values,entry):
        values[stamps==pd.Timestamp('2024-02-15',tz='UTC').value//10**6,1]+=1
        return stamps,values,entry
    replacement=publish(tmp_path/'changed',transform=changed)
    denied=client.post(f"/api/studies/{account['studyId']}/locate-parquet-pack",json={'packRoot':replacement[0]['pack_root']})
    assert denied.status_code==400,denied.get_data(as_text=True)
    assert original_rows()==before
