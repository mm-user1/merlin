"""Real spawned production S07 workers consume prepared RAM, even with a busy pack."""
import multiprocessing as mp
from types import SimpleNamespace

from core.data_input import prepare_input
from core.entry_filters import effective_config
from core.post_process import run_forward_test, run_perturbations_parallel, PostProcessConfig
from strategies.s07_ha_v01_b2.strategy import normalized_params
from tests._data_input_helpers import hold_pack
from tests._s07_helpers import STRATEGY,publish,payload


def test_production_spawn_no_candidate_pack_reads(tmp_path):
    source=publish(tmp_path/'pack');config=payload(source)
    prepared=prepare_input(source,date_filter=True,start='2024-01-06',end='2024-01-26',warmup_bars=100,
        admission_params={},entry_filters=effective_config(config['entryFilters']))
    params=normalized_params();context=mp.get_context('spawn');ready,release=context.Event(),context.Event()
    holder=context.Process(target=hold_pack,args=(source['pack_root'],ready,release))
    holder.start()
    try:
        assert ready.wait(20)
        candidates=[SimpleNamespace(params={**params,'entryRunLength':k},optuna_trial_number=k,
            net_profit_pct=1,max_drawdown_pct=1,total_trades=1) for k in (5,6)]
        options=dict(csv_path='',strategy_id=STRATEGY,optuna_results=candidates,
            config=PostProcessConfig(enabled=True,top_k=2,warmup_bars=100),is_period_days=1,ft_period_days=3,
            ft_start_date='2024-01-20',ft_end_date='2024-01-23',n_workers=2,prepared_frame=prepared.frame,
            admission_rules=prepared.admission_rules,entry_filter_context=prepared.entry_filter_context)
        result=run_forward_test(**options)
        assert len(result)==2 and all(r.ft_total_trades>0 for r in result)
        perturbations=[dict(params={**params,'entryRunLength':k},perturbed_param='entryRunLength',direction=d,
            base_value=6,perturbed_value=k) for k,d in ((5,-1),(7,1))]
        result=run_perturbations_parallel('',STRATEGY,perturbations,'2024-01-20','2024-01-23',{},100,2,
            prepared_frame=prepared.frame,admission_rules=prepared.admission_rules,entry_filter_context=prepared.entry_filter_context)
        assert len(result)==2
        # No inferred Off, including in a freshly initialized worker pool.
        options.pop('entry_filter_context')
        assert run_forward_test(**options)==[]
    finally:
        release.set();holder.join(20)
        if holder.is_alive():holder.terminate();holder.join()
    assert holder.exitcode==0
