"""Real spawned workers receive a quantity policy without reopening a locked pack."""
import multiprocessing as mp
from types import SimpleNamespace
import pytest

from core.data_input import prepare_input,DataInputError
from core.post_process import run_forward_test,run_perturbations_parallel,PostProcessConfig,_run_is_backtest
from strategies import get_strategy_config
from tests._portfolio_helpers import publish
from tests._data_input_helpers import hold_pack
from tests._quantity_helpers import LEGACY,trading_pack
from tests.fixtures.admission_strategy.strategy import rule_entry


@pytest.mark.parametrize('sid',[LEGACY[0],LEGACY[2]])
def test_quantity_workers_apply_explicit_ram_context(tmp_path,sid):
    source=trading_pack(tmp_path/'pack',minimum='1000000')[0]
    prepared=prepare_input(source,date_filter=False,quantity_only=True)
    params={k:v['default'] for k,v in get_strategy_config(sid)['parameters'].items() if k!='warmupBars'}
    params.update(useRegime=False,maLength3=10)
    interval=('2024-02-01','2024-02-29')
    expected=_run_is_backtest('',sid,params,*interval,100,prepared_frame=prepared.frame)
    assert expected['total_trades']>0
    ctx=mp.get_context('spawn');ready,release=ctx.Event(),ctx.Event()
    holder=ctx.Process(target=hold_pack,args=(source['pack_root'],ready,release));holder.start()
    try:
        assert ready.wait(20)
        with pytest.raises(DataInputError,match='busy in another process'):prepare_input(source,date_filter=False)
        candidates=[SimpleNamespace(params=params,optuna_trial_number=i,net_profit_pct=1.,max_drawdown_pct=1.,total_trades=1) for i in (1,2)]
        result=run_forward_test(csv_path='',strategy_id=sid,optuna_results=candidates,
            config=PostProcessConfig(enabled=True,top_k=2,warmup_bars=100),is_period_days=1,ft_period_days=29,
            ft_start_date=interval[0],ft_end_date=interval[1],n_workers=2,prepared_frame=prepared.frame,quantity_rules=prepared.quantity_rules)
        assert len(result)==2 and all(r.ft_total_trades==0 for r in result)
        perturbations=[dict(params=params,perturbed_param='unused',direction=i,base_value=1,perturbed_value=1+i) for i in (-1,1)]
        result=run_perturbations_parallel('',sid,perturbations,*interval,{},100,2,
            prepared_frame=prepared.frame,quantity_rules=prepared.quantity_rules)
        assert len(result)==2 and all(r['total_trades']==0 for r in result)
    finally:
        release.set();holder.join(20)
        if holder.is_alive():holder.terminate();holder.join()
    assert holder.exitcode==0
