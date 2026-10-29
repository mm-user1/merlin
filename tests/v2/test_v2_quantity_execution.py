"""Same-backend exact neutral-rule oracle over every approved production variant."""
from dataclasses import asdict, replace
import numpy as np
import pytest

from core.engine_v2.admission import run_strategy
from core.engine_v2.runner import run_v2_strategy
from core.engine_v2.compiled_kernel import evaluate_compiled_batch, _pack_config_arrays, pack_compiled_config_arrays_from_rows
from core.engine_v2.compiled_kernel_signal import build_signal_stacked_execution_data, evaluate_compiled_signal_stacked_batch
from core.engine_v2.profile import active_mode_values
from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan, execute_grid_v2_candidates
from tests._quantity_helpers import LEGACY, cases, setup, frame, rules


def same_result(a, b):
    assert [asdict(t) for t in a.trades] == [asdict(t) for t in b.trades]
    np.testing.assert_equal(a.equity_curve, b.equity_curve)
    np.testing.assert_equal(a.balance_curve, b.balance_curve)
    assert list(a.timestamps) == list(b.timestamps)


@pytest.mark.parametrize('sid,variant,value', list(cases()))
@pytest.mark.parametrize('notional', [None, '0'])
def test_neutral_rules_all_variants_exact(sid, variant, value, notional):
    cls, module, config, profile, params = setup(sid, value)
    bars = frame()
    quantity = rules(step=str(params['contractSize']), notional=notional)
    hooks = GridV2StrategyHooks.from_strategy(module)
    for date_filter in (False, True):
        params.update(dateFilter=date_filter, start=str(bars.index[300]), end=str(bars.index[2500]))
        ordinary = run_strategy(cls, bars, params, 200)
        actual = run_strategy(cls, bars, params, 200, quantity_rules=quantity)
        same_result(ordinary, actual)
        assert not hasattr(actual, 'entry_diagnostics')
        data = hooks.build_execution_data(bars, params)
        plain = run_v2_strategy(data=data, profile=profile, params=params, trade_start_idx=200)
        opted = run_v2_strategy(data=data, profile=profile, params=params, trade_start_idx=200, quantity_rules=quantity)
        same_result(plain.strategy_result, opted.strategy_result)
        np.testing.assert_equal(asdict(plain.guardrail_summary), asdict(opted.guardrail_summary))
        if profile.modes.get('topology') == 'signal_reversal':
            stacked = build_signal_stacked_execution_data([data], [0])
            evaluate = lambda q: evaluate_compiled_signal_stacked_batch(stacked_data=stacked, profile=profile,
                params_batch=[params], trade_start_idx=200, quantity_rules=q).outputs
        else:
            evaluate = lambda q: evaluate_compiled_batch(data=data, profile=profile,
                params_batch=[params], trade_start_idx=200, quantity_rules=q).outputs
            mapped = _pack_config_arrays(profile, [params], 200, quantity_rules=quantity)
            typed = pack_compiled_config_arrays_from_rows(row_count=1, get_value=lambda i,n,d:params.get(n,d),
                get_modes=lambda i:active_mode_values(profile, params), trade_start_idx=200, profile=profile, quantity_rules=quantity)
            for key in mapped:
                np.testing.assert_equal(mapped[key], typed[key])
        np.testing.assert_equal(evaluate(None), evaluate(quantity))
        assert ordinary.trades, 'neutral oracle must actually execute this variant'


@pytest.mark.parametrize('sid,variant,value', list(cases()))
def test_quantity_grid_packers_and_identity(sid, variant, value):
    _, module, config, profile, params = setup(sid, value)
    quantity = rules(step=str(params['contractSize']), notional='0')
    settings = GridV2Settings(enabled_axes=(), enabled_variants=(variant,) if profile.variant_selector and profile.variant_selector.user_facing else None, compiled_workers=1)
    plain = build_grid_v2_plan(config, settings, params)
    settings = replace(settings, quantity_rules=quantity)
    plan = build_grid_v2_plan(config, settings, params)
    other = build_grid_v2_plan(config, settings, dict(params, contractSize=99))
    assert plan.plan_fingerprint == other.plan_fingerprint != plain.plan_fingerprint
    changed = build_grid_v2_plan(config, replace(settings, quantity_rules=rules(step='.5', notional='0')), params)
    assert changed.plan_fingerprint != plan.plan_fingerprint
    assert plan.candidate_table.semantic_key_for_index(0) != changed.candidate_table.semantic_key_for_index(0)
    hooks = GridV2StrategyHooks.from_strategy(module)
    bars = frame()
    a = execute_grid_v2_candidates(plain, bars, 200, hooks)
    b = execute_grid_v2_candidates(plan, bars, 200, hooks)
    c = execute_grid_v2_candidates(replace(plan, settings=replace(settings, compiled_config_packing='mapping')), bars, 200, hooks)
    assert a.rows[0].status == b.rows[0].status == c.rows[0].status == 'ok'
    for field in ('net_profit_pct', 'total_trades', 'guardrail_summary'):
        np.testing.assert_equal(getattr(a.rows[0], field), getattr(b.rows[0], field))
        np.testing.assert_equal(getattr(b.rows[0], field), getattr(c.rows[0], field))
    np.testing.assert_equal(a.selected[0].metrics, b.selected[0].metrics)
    assert b.selected[0].entry_diagnostics is None
    with pytest.raises(ValueError, match='Contract Size'):
        build_grid_v2_plan(config, replace(settings, enabled_axes=('contractSize',)), params)


@pytest.mark.parametrize('sid',[LEGACY[0],LEGACY[2]])
@pytest.mark.parametrize('policy',['full','sampled'])
def test_quantity_chunked_two_threads_and_real_reference_fallback(sid,policy):
    from core.grid_v2 import estimate_grid_v2_cache
    _,module,config,profile,params=setup(sid,'false')
    axis='maLength3' if sid.startswith('s03') else 'stopX'
    quantity=rules(step='.2',minimum='.400001',notional='1')
    settings=GridV2Settings(enabled_axes=(axis,),quantity_rules=quantity,top_n=2,
        planning_policy=policy,requested_budget=4,compiled_workers=1)
    bars=frame();hooks=GridV2StrategyHooks.from_strategy(module)
    plan=build_grid_v2_plan(config,settings,params)
    whole=estimate_grid_v2_cache(plan,bars,100,hooks)
    one=estimate_grid_v2_cache(plan,bars,100,hooks,candidate_indices=(0,))
    limit=(whole.estimated_total_mb+one.estimated_total_mb)/2
    base=execute_grid_v2_candidates(plan,bars,100,hooks)
    signal=profile.modes.get('topology')=='signal_reversal'
    if not signal:
        with pytest.raises(MemoryError):
            execute_grid_v2_candidates(build_grid_v2_plan(config,replace(settings,max_signal_cache_mb=limit),params),bars,100,hooks)
        limit=settings.max_signal_cache_mb  # Position retains its existing whole-cache contract.
    chunked=execute_grid_v2_candidates(build_grid_v2_plan(config,replace(settings,compiled_workers=2,max_signal_cache_mb=limit),params),bars,100,hooks)
    fallback=execute_grid_v2_candidates(build_grid_v2_plan(config,replace(settings,prefer_compiled=False),params),bars,100,hooks)
    assert base.metadata['backend_kind']==chunked.metadata['backend_kind']=='compiled_numba'
    assert chunked.metadata['compiled_workers']==2
    assert chunked.metadata['chunk_count']>1 if signal else chunked.metadata['chunk_count']==1
    assert fallback.metadata['backend_kind']=='reference'
    for a,b,c in zip(base.rows,chunked.rows,fallback.rows):
        assert a.status==b.status==c.status=='ok'
        assert a.total_trades==b.total_trades==c.total_trades
        assert a.guardrail_summary==b.guardrail_summary
        assert all(c.guardrail_summary[k]==v for k,v in a.guardrail_summary.items())
        assert a.net_profit_pct==b.net_profit_pct
        assert a.net_profit_pct==pytest.approx(c.net_profit_pct,abs=1e-10,rel=1e-10)
    assert [x.row.candidate_id for x in base.selected]==[x.row.candidate_id for x in chunked.selected]
