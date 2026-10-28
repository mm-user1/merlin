"""S07 preparation, planning and generic reference/compiled integration."""
from dataclasses import replace

import numba
import numpy as np
import pandas as pd
import pytest

from core.entry_filters import OFF
from core.engine_v2.admission import AdmissionRules
from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan, execute_grid_v2_candidates
from core.parameter_mode import resolve_fixed_scalars
from strategies import get_strategy, get_strategy_config
from strategies.base import BaseStrategy
from strategies.s07_ha_v01_b2 import strategy
from strategies.s07_ha_v01_b2.signals import indicators
from tests.fixtures.admission_strategy.strategy import rule_entry


def frame(rows=600, minutes=30):
    x = np.arange(rows)
    opening = 100 + 8 * np.sin(x / 7)
    close = 100 + 8 * np.sin((x + .8) / 7)
    return pd.DataFrame(dict(Open=opening, High=np.maximum(opening, close)+.4,
        Low=np.minimum(opening, close)-.4, Close=close, Volume=np.ones(rows)),
        index=pd.date_range('2024-01-01', periods=rows, freq=f'{minutes}min', tz='UTC'))


def settings(**kwargs):
    return GridV2Settings(admission_rules=AdmissionRules.from_entry(rule_entry(step='.001', minimum='.001', notional='0')), **kwargs)


def test_discovery_profile_and_full_fixed_sampled_planning():
    config = get_strategy_config('s07_ha_v01_b2')
    assert issubclass(get_strategy('s07_ha_v01_b2'), BaseStrategy)
    assert config['name'] == 'S_07 HA' and config['version'] == 'v01'
    profile = strategy.load_profile()
    assert len(profile.variants) == 1 and profile.entry_filter_methods == ('candleDirection',)
    assert set(strategy.SIGNAL_CACHE_PARAM_NAMES) == {n for n,s in config['parameters'].items() if s['role']=='signal'}
    assert set(strategy.DATAPREP_CACHE_PARAM_NAMES) == {*strategy.SIGNAL_CACHE_PARAM_NAMES, 'stopLP'}
    assert {n for n,s in config['parameters'].items() if s['optimize']['enabled']} == {'entryRunLength','stopLP','stopX','stopRR','stopMaxDays'}
    plan = build_grid_v2_plan(config, settings(), strategy.normalized_params())
    assert len(plan.candidates) == 840
    assert len({c.semantic_key for c in plan.candidates}) == 840
    assert [c.semantic_key for c in plan.candidates] == [c.semantic_key for c in build_grid_v2_plan(config, settings(), strategy.normalized_params()).candidates]
    fixed, modes = resolve_fixed_scalars(config, {'stopLP':3, 'maxEntryLeverage':7.5}, [])
    assert fixed['stopLP'] == 3 and fixed['maxEntryLeverage'] == 7.5
    assert len(build_grid_v2_plan(config, settings(enabled_axes=()), fixed).candidates) == 1
    for values in ({'maxEntryLeverage':12},{'entryRunLength':9},{'stopX':.5},{'stopLP':True}):
        with pytest.raises(ValueError): resolve_fixed_scalars(config, values, [])
    subset = settings(enabled_axes=('entryRunLength','stopX'))
    assert len(build_grid_v2_plan(config, subset, {}).candidates) == 35
    sampled = replace(subset, planning_policy='sampled', requested_budget=7)
    a, b = (build_grid_v2_plan(config, sampled, {}) for _ in range(2))
    assert len(a.candidates) == 7
    assert [c.semantic_key for c in a.candidates] == [c.semantic_key for c in b.candidates]
    with pytest.raises(ValueError, match='fixed'):
        build_grid_v2_plan(config, settings(enabled_axes=('maxEntryLeverage',)), {})


def test_independent_integer_ohlc_ha_atr_seed_and_neutrality():
    f = pd.DataFrame({'Open':[10,12,14,16]*20,'High':[13,15,17,19]*20,
        'Low':[9,11,13,15]*20,'Close':[12,14,16,18]*20}, index=pd.date_range('2024',periods=80,freq='5min',tz='UTC'))
    actual = indicators(f)
    np.testing.assert_array_equal(actual.ha_close[:4], [11,13,15,17])
    np.testing.assert_array_equal(actual.ha_open[:4], [11,11,12,13.5])
    np.testing.assert_array_equal(actual.colour[:4], [0,1,1,1])
    np.testing.assert_array_equal(actual.run[:4], [0,1,2,3])
    assert actual.atr[13] == 71/14  # Eleven TR=4 and three downward gaps TR=9.
    assert actual.atr[14] == (actual.atr[13]*13+4)/14
    data = strategy.build_v2_execution_data(f, {})
    assert all(getattr(data,n).dtype == np.float64 for n in ('open','high','low','close','atr','rolling_low','rolling_high'))
    assert data.rolling_low[1] == 9 and data.rolling_high[1] == 15
    assert not data.signals.long_entries[:64].any()


@pytest.mark.parametrize('minutes',[5,30,45,240,4320])
def test_causal_prefix_kth_event_sides_and_cache_behavior(minutes):
    f = frame(minutes=minutes)
    p = strategy.normalized_params()
    values = indicators(f)
    a = strategy.build_v2_execution_data(f,p)
    b = strategy.build_v2_execution_data(f.iloc[:300],p)
    for name in ('atr','rolling_low','rolling_high'):
        np.testing.assert_array_equal(getattr(a,name)[:300],getattr(b,name))
    np.testing.assert_array_equal(a.signals.long_entries[:300],b.signals.long_entries)
    expected = (np.arange(len(f))>=64) & (values.colour==1) & (values.run==6)
    np.testing.assert_array_equal(a.signals.long_entries,expected)
    assert a.signals.long_entries.any() and a.signals.short_entries.any()
    for key,value in [('enableLong',False),('enableShort',False),('entryRunLength',3)]:
        changed = strategy.build_v2_execution_data(f,{**p,key:value})
        assert not (np.array_equal(a.signals.long_entries,changed.signals.long_entries) and np.array_equal(a.signals.short_entries,changed.signals.short_entries))
    batch = strategy.build_v2_execution_data_batch(f,[p,{**p,'stopLP':3},{**p,'stopX':3}])
    assert batch[0].atr is batch[1].atr is batch[2].atr
    assert batch[0].rolling_low is batch[2].rolling_low
    assert not np.array_equal(batch[0].rolling_low,batch[1].rolling_low,equal_nan=True)
    off = strategy.build_v2_execution_data(f,{**p,'enableLong':False,'enableShort':False})
    assert not off.signals.long_entries.any() and not off.signals.short_entries.any()
    with pytest.raises(ValueError,match='explicit Off'):
        GridV2StrategyHooks.from_strategy(strategy)


@pytest.mark.parametrize('workers',[1,2])
def test_s07_reference_compiled_packers_threads(workers):
    assert numba.config.NUMBA_NUM_THREADS >= workers
    config = strategy.load_config()
    cfg = settings(enabled_axes=('entryRunLength',), compiled_workers=workers, top_n=7)
    params = strategy.normalized_params({'dateFilter':False})
    plan = build_grid_v2_plan(config,cfg,params)
    hooks = GridV2StrategyHooks.from_strategy(strategy,entry_filter_context=OFF)
    outputs = []
    for changes in ({},{'compiled_config_packing':'mapping'},{'prefer_compiled':False}):
        outputs.append(execute_grid_v2_candidates(replace(plan,settings=replace(cfg,**changes)),frame(),100,hooks))
    table,mapping,reference = outputs
    assert table.metadata['compiled_workers'] == workers
    assert table.metadata['compiled_config_packing'] == 'table'
    assert mapping.metadata['compiled_config_packing'] == 'mapping'
    for a,b,c in zip(table.rows,mapping.rows,reference.rows):
        assert a.status == b.status == c.status == 'ok'
        assert a.total_trades == b.total_trades == c.total_trades > 0
        assert a.net_profit_pct == b.net_profit_pct == pytest.approx(c.net_profit_pct,rel=1e-12,abs=1e-10)
    assert table.selected and all(s.entry_diagnostics is not None for s in table.selected)
    capped = build_grid_v2_plan(config,replace(cfg,enabled_axes=()),{**params,'riskPerTrade':100.,'maxEntryLeverage':1.})
    refused = execute_grid_v2_candidates(capped,frame(),100,hooks)
    assert refused.rows[0].total_trades == 0
    assert refused.rows[0].guardrail_summary['margin_reject_count'] > 0
