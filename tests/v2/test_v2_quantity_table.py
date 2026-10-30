"""Vectorized quantity constants, exact packing oracles and real dispatch."""
from collections.abc import Mapping
from dataclasses import asdict, fields, is_dataclass, replace

import numpy as np
import pytest

import core.grid_v2 as grid
import core.engine_v2.compiled_kernel as compiled
from core.engine_v2.execution_modes import TRAIL_MODE_R_DISTANCE, TRAIL_MODE_CHANDELIER, TRAIL_MODE_FIXED_AF_SAR
from tests._quantity_helpers import LEGACY, cases, setup, frame, rules


POSITION_CASES = [case for case in cases() if case[0] in LEGACY[2:]]


def values(value):
    """Compare lazy and eager representations by their complete exact content."""
    if isinstance(value, Mapping): return {k: values(v) for k,v in value.items()}
    if is_dataclass(value):
        return {f.name: str(getattr(value,f.name)) if f.name == 'canonical_identity' else values(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value, (list, tuple)): return [values(v) for v in value]
    return value


def prepared(sid=LEGACY[2], variant='bracket', value='false', notional='1'):
    _, module, config, profile, params = setup(sid, value)
    settings = grid.GridV2Settings(enabled_variants=(variant,), enabled_axes=('stopX',),
        top_n=1, compiled_workers=1, quantity_rules=rules(step='.02', minimum='.060001', notional=notional))
    return config, params, grid.build_grid_v2_plan(config, settings, params), grid.GridV2StrategyHooks.from_strategy(module)


def typed(plan, indices):
    table = plan.candidate_table
    return compiled.pack_compiled_config_arrays_from_rows(row_count=len(indices),
        get_value=lambda row,n,d: table.param_value_for_index(indices[row],n) if table.has_param_for_index(indices[row],n) else d,
        get_modes=lambda row: table.modes_for_index(indices[row]), trade_start_idx=100,
        profile=plan.profile, quantity_rules=plan.settings.quantity_rules)


def assert_packed_equal(actual, oracle):
    # Only these inactive fields are not consumed by _compiled_loop_one. A2's
    # vectorized seeds remain 1/3/.01; row packers use zero outside their modes.
    active = {'trail_distance_r': TRAIL_MODE_R_DISTANCE,
              'chandelier_atr_mult': TRAIL_MODE_CHANDELIER, 'sar_speed': TRAIL_MODE_FIXED_AF_SAR}
    assert actual.keys() == oracle.keys()
    for name, array in actual.items():
        assert array.dtype == oracle[name].dtype and array.ndim == 1 and array.flags.c_contiguous
        mask = actual['trail_mode_code'] == active[name] if name in active else np.ones(len(array), bool)
        np.testing.assert_equal(array[mask], oracle[name][mask], err_msg=name)


@pytest.mark.parametrize('sid,variant,value', POSITION_CASES)
def test_quantity_table_all_position_variants_exact(sid, variant, value, monkeypatch):
    _, _, plan, hooks = prepared(sid, variant, value)
    indices = (plan.deduped_candidate_count-1, 0, 2)
    arrays = grid._pack_table_config_arrays(plan, indices, 100)
    mapped = compiled._pack_config_arrays(plan.profile, [plan.candidate_table.params_for_index(i) for i in indices],
        100, quantity_rules=plan.settings.quantity_rules)
    assert_packed_equal(arrays, mapped)
    assert_packed_equal(arrays, typed(plan, indices))
    bars = frame()
    fast, slow = [], []
    original_batch, original_reference = grid.evaluate_compiled_stacked_batch, grid.run_v2_strategy
    def batch(*args, **kwargs):
        result = original_batch(*args, **kwargs); fast.append(result.outputs.copy()); return result
    def reference(*args, **kwargs):
        result = original_reference(*args, **kwargs); slow.append(result); return result
    monkeypatch.setattr(grid, 'evaluate_compiled_stacked_batch', batch)
    monkeypatch.setattr(grid, 'run_v2_strategy', reference)
    table_result = grid.execute_grid_v2_candidates(plan, bars, 100, hooks)
    mapping_plan = replace(plan, settings=replace(plan.settings, compiled_config_packing='mapping'))
    mapping_result = grid.execute_grid_v2_candidates(mapping_plan, bars, 100, hooks)
    monkeypatch.setattr(grid, '_pack_table_config_arrays', lambda p, indices, start: typed(p, indices))
    typed_result = grid.execute_grid_v2_candidates(plan, bars, 100, hooks)
    assert plan.plan_fingerprint == mapping_plan.plan_fingerprint
    np.testing.assert_equal(values(table_result.rows), values(mapping_result.rows))
    np.testing.assert_equal(values(table_result.selected), values(mapping_result.selected))
    np.testing.assert_equal(values(table_result.rows), values(typed_result.rows))
    np.testing.assert_equal(values(table_result.selected), values(typed_result.selected))
    assert len(fast) == 3 and fast[0].shape[1] == 26
    np.testing.assert_equal(fast[0], fast[1])
    np.testing.assert_equal(fast[0], fast[2])
    assert len(slow) == 3 and slow[0].strategy_result.trades
    for other in slow[1:]:
        for name in ('trades', 'equity_curve', 'balance_curve', 'timestamps'):
            a,b = getattr(slow[0].strategy_result,name),getattr(other.strategy_result,name)
            if name == 'trades': a,b = [asdict(t) for t in a],[asdict(t) for t in b]
            np.testing.assert_equal(a,b)


@pytest.mark.parametrize('notional', [None, '0', '1000000000'])
def test_quantity_table_constants_empty_singleton_and_binding_refusals(notional):
    _, _, plan, hooks = prepared(notional=notional)
    for indices in ((), (0,), (4, 1, 3)):
        arrays = grid._pack_table_config_arrays(plan, indices, 100)
        assert_packed_equal(arrays, typed(plan, indices))
        assert np.all(arrays['contract_size'] == .02)
        assert np.all(arrays['admission_cap'] == 0)
        assert np.all(arrays['admission_min_lots'] == 4)
        np.testing.assert_equal(arrays['admission_min_notional'], np.full(len(indices), np.nan if notional is None else float(notional)))
    bars = frame()
    actual = grid.execute_grid_v2_candidates(plan, bars, 100, hooks)
    mapped = grid.execute_grid_v2_candidates(replace(plan,settings=replace(plan.settings,compiled_config_packing='mapping')),bars,100,hooks)
    np.testing.assert_equal(values(actual.rows), values(mapped.rows))
    assert any(r.total_trades for r in actual.rows) if notional != '1000000000' else all(r.total_trades == 0 for r in actual.rows)


def test_quantity_dispatch_forbids_both_row_packers(monkeypatch):
    _, _, plan, hooks = prepared()
    def forbidden(*args, **kwargs): raise AssertionError('eligible table dispatch used a per-candidate packer')
    for name in ('pack_compiled_config_arrays_from_rows', '_pack_config_arrays'):
        monkeypatch.setattr(compiled, name, forbidden)
        if hasattr(grid, name): monkeypatch.setattr(grid, name, forbidden)
    original = grid.evaluate_compiled_stacked_batch
    seen = []
    def observe(*args, **kwargs):
        assert kwargs.get('packed_config_arrays') is not None
        seen.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(grid, 'evaluate_compiled_stacked_batch', observe)
    actual = grid.execute_grid_v2_candidates(plan, frame(), 100, hooks)
    assert seen and actual.metadata['backend_kind'] == 'compiled_numba'


@pytest.mark.parametrize('fallback', ['mapping', 'sampled', 'normalizer'])
def test_quantity_mapping_fallback_stays_available(fallback, monkeypatch):
    config, params, plan, hooks = prepared()
    if fallback == 'mapping': plan = replace(plan,settings=replace(plan.settings,compiled_config_packing='mapping'))
    elif fallback == 'sampled': plan = grid.build_grid_v2_plan(config,replace(plan.settings,planning_policy='sampled',requested_budget=2),params)
    else:
        normalizer = hooks.normalize_params
        hooks = replace(hooks,normalize_params=lambda p: dict(normalizer(p), commissionPct=.02))
    def forbidden(*args, **kwargs): raise AssertionError('fallback used table packing')
    monkeypatch.setattr(grid, '_pack_table_config_arrays', forbidden)
    result = grid.execute_grid_v2_candidates(plan,frame(),100,hooks)
    assert result.metadata['compiled_config_packing'] == 'mapping'
    assert all(r.status == 'ok' for r in result.rows)


def test_s07_keeps_typed_rows_and_ordinary_keeps_zero_context(monkeypatch):
    _, _, ordinary, _ = prepared()
    ordinary = replace(ordinary,settings=replace(ordinary.settings,quantity_rules=None))
    arrays = grid._pack_table_config_arrays(ordinary,(0,2),100)
    for name in ('admission_cap','admission_min_lots','admission_min_notional'): assert np.all(arrays[name] == 0)
    assert_packed_equal(arrays,typed(ordinary,(0,2)))
    _, _, config, _, params = setup('s07_ha_v01_b2', None)
    plan = grid.build_grid_v2_plan(config,grid.GridV2Settings(enabled_axes=(),admission_rules=rules()),params)
    original = compiled.pack_compiled_config_arrays_from_rows
    seen=[]
    def observe(**kwargs): seen.append(True); return original(**kwargs)
    monkeypatch.setattr(compiled,'pack_compiled_config_arrays_from_rows',observe)
    actual = grid._pack_table_config_arrays(plan,(0,),100)
    assert seen and actual['admission_cap'][0] == params['maxEntryLeverage']


@pytest.mark.parametrize('bad', ['unvalidated', 'conflict'])
def test_quantity_empty_still_validates_context(bad):
    _, _, plan, _ = prepared()
    settings = replace(plan.settings,quantity_rules=object()) if bad == 'unvalidated' else replace(plan.settings,admission_rules=rules())
    with pytest.raises(ValueError): grid._pack_table_config_arrays(replace(plan,settings=settings),(),100)


@pytest.mark.parametrize('name,value', [('strict_boundary',False),('boundary_none',True),('rounding_code',1)])
def test_quantity_table_rejects_corrupted_row_modes(name,value,monkeypatch):
    _, _, plan, _ = prepared()
    original = grid._fill_mode_config_arrays
    def corrupted(arrays,*args): original(arrays,*args); arrays[name][0] = value
    monkeypatch.setattr(grid,'_fill_mode_config_arrays',corrupted)
    with pytest.raises(ValueError,match='Packed quantity rows require strict-close without price rounding'):
        grid._pack_table_config_arrays(plan,(0,),100)


@pytest.mark.parametrize('field', ['contract_size','admission_cap','admission_min_lots','admission_min_notional','strict_boundary','shape'])
def test_quantity_public_array_validation_is_not_bypassed(field):
    _, _, plan, hooks = prepared()
    arrays = grid._pack_table_config_arrays(plan,(0,),100)
    data = hooks.build_execution_data(frame(),plan.candidate_table.params_for_index(0))
    stacked = compiled.build_stacked_execution_data([data],[0])
    if field == 'shape': arrays['initial_capital'] = arrays['initial_capital'].reshape(1,1)
    elif field == 'strict_boundary': arrays[field][0] = False
    else: arrays[field][0] += 1
    with pytest.raises(ValueError):
        compiled.evaluate_compiled_stacked_batch(stacked_data=stacked,profile=plan.profile,packed_config_arrays=arrays,
            trade_start_idx=100,quantity_rules=plan.settings.quantity_rules)


@pytest.mark.parametrize('value,field,bad', [('r_trail','trail_distance_r',0.),('chandelier','chandelier_atr_mult',np.nan),
    ('fixed_af_sar','sar_speed',1.01),('r_trail','trail_activation_rr',np.inf)])
def test_quantity_active_trail_validation(value,field,bad,monkeypatch):
    selector = next(v for sid, variant, v in POSITION_CASES if sid == LEGACY[4] and variant == value)
    _, _, plan, hooks = prepared(LEGACY[4],value,selector)
    arrays = grid._pack_table_config_arrays(plan,(0,),100)
    data = hooks.build_execution_data(frame(),plan.candidate_table.params_for_index(0))
    arrays[field][0] = bad
    with pytest.raises(ValueError):
        compiled.evaluate_compiled_stacked_batch(stacked_data=compiled.build_stacked_execution_data([data],[0]),
            profile=plan.profile,packed_config_arrays=arrays,trade_start_idx=100,quantity_rules=plan.settings.quantity_rules)
    parameter = {'trail_distance_r':'trailDistanceR','chandelier_atr_mult':'chandelierATRMult',
                 'sar_speed':'sarSpeed','trail_activation_rr':'trailRR'}[field]
    original = grid._fill_float_config_array
    def corrupt(output,table,indices,name,default):
        original(output,table,indices,name,default)
        if name == parameter: output[0] = bad
    monkeypatch.setattr(grid,'_fill_float_config_array',corrupt)
    with pytest.raises(ValueError): grid._pack_table_config_arrays(plan,(0,),100)


def test_quantity_rebase_and_rule_changes_allocate_fresh_arrays():
    config, params, plan, _ = prepared()
    cache = grid.GridV2PlanReuseCache()
    first = cache.get_or_build(config,settings=plan.settings,base_params=dict(params,dateFilter=True,start='2023-01-02',end='2023-01-20'))
    original = grid._pack_table_config_arrays(first.plan,(0,2),100)
    saved = {k:v.copy() for k,v in original.items()}
    window = dict(params,dateFilter=True,start='2023-02-02',end='2023-02-20')
    second = cache.get_or_build(config,settings=plan.settings,base_params=window)
    assert second.hit and first.plan.plan_fingerprint == second.plan.plan_fingerprint
    rebased = grid._pack_table_config_arrays(second.plan,(0,2),100)
    assert np.all(rebased['start_ns'] != original['start_ns'])
    settings = replace(plan.settings,quantity_rules=rules(step='.03',minimum='.120001',notional='2'))
    changed = cache.get_or_build(config,settings=settings,base_params=dict(window,initialCapital=250.))
    assert not changed.hit and changed.plan.plan_fingerprint != first.plan.plan_fingerprint
    actual = grid._pack_table_config_arrays(changed.plan,(2,0),100)
    assert np.all(actual['initial_capital'] == 250.) and np.all(actual['contract_size'] == .03)
    assert np.all(actual['admission_min_lots'] == 5) and np.all(actual['admission_min_notional'] == 2)
    for name in original:
        np.testing.assert_equal(original[name],saved[name])
        assert not np.shares_memory(original[name],rebased[name])
        assert not np.shares_memory(original[name],actual[name])
