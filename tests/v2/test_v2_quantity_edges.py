"""Independent small quantity/fee/close examples and fail-closed compiled errors."""
from dataclasses import replace
import math
import numpy as np
import pytest

from core.engine_v2.admission import run_strategy
from core.engine_v2.compiled_kernel import evaluate_compiled_batch, OUTPUT_FLAGS, OUTPUT_TOTAL_TRADES, OUTPUT_FINAL_BALANCE, OUTPUT_ZERO_SIZE_ENTRY_COUNT
from core.engine_v2.compiled_kernel_signal import evaluate_compiled_signal_stacked_batch, build_signal_stacked_execution_data
from core.engine_v2.contracts import Signals, GUARDRAIL_FLAG_EXECUTION_ERROR
from core.engine_v2.quantity import read_quantity_envelope, quantity_envelope
from core.engine_v2.runner import run_v2_strategy
from tests._quantity_helpers import LEGACY, rules, setup
from tests.v2.test_v2_entry_admission import data


def pair(family, bars, quantity, *, kernel_only=False, **changes):
    _,_,_,profile,params=setup(LEGACY[0 if family=='signal' else 2], 'false' if family=='signal' else 'bracket')
    params.update(dateFilter=False, contractSize=.1, initialCapital=100., commissionPct=.1,
        stopX=1., stopRR=3., stopMaxPct=100., riskPerTrade=3., positionPct=30., enableLong=True,enableShort=True)
    params.update(changes)
    def reference(q):
        if kernel_only:
            from core.engine_v2.runner import build_kernel_config, build_signal_kernel_config
            from core.engine_v2.kernel import run_reference_kernel
            from core.engine_v2.kernel_signal import run_signal_reversal_kernel
            build,run=(build_signal_kernel_config,run_signal_reversal_kernel) if family=='signal' else (build_kernel_config,run_reference_kernel)
            return run(bars,build(profile=profile,params=params,quantity_rules=q))
        return run_v2_strategy(data=bars,profile=profile,params=params,quantity_rules=q)
    def compiled(q):
        if family=='signal':
            return evaluate_compiled_signal_stacked_batch(stacked_data=build_signal_stacked_execution_data([bars],[0]),
                profile=profile,params_batch=[params],trade_start_idx=0,quantity_rules=q).outputs[0]
        return evaluate_compiled_batch(data=bars,profile=profile,params_batch=[params],trade_start_idx=0,quantity_rules=q).outputs[0]
    return reference,compiled


@pytest.mark.parametrize('family',['position','signal'])
@pytest.mark.parametrize('short',[False,True])
@pytest.mark.parametrize('minimum,notional,opened,count',[
    ('.2','20',100.,1),('.20001','0',100.,0),('.1',None,100.,1),('.1','20.0001',100.,0),('.1','20',90.,0)])
def test_minimum_edges_and_gap_fees(family,short,minimum,notional,opened,count):
    # IEEE floor at the original sizing point produces .2 (not decimal .3).
    bars=data(short=short,opened=opened)
    q=rules(step='.1',minimum=minimum,notional=notional)
    slow,fast=pair(family,bars,q)
    result=slow(q);row=fast(q)
    assert len(result.strategy_result.trades)==row[OUTPUT_TOTAL_TRADES]==count
    assert result.strategy_result.balance_curve[-1]==pytest.approx(99.96 if count else 100.)
    assert row[OUTPUT_FINAL_BALANCE]==pytest.approx(result.strategy_result.balance_curve[-1])
    assert row[OUTPUT_FLAGS]==0
    assert result.kernel_result.entry_events is None
    if count:
        assert result.strategy_result.trades[0].size==.2
        assert result.strategy_result.trades[0].net_pnl==pytest.approx(-.04)


@pytest.mark.parametrize('family',['position','signal'])
def test_entry_minimum_does_not_block_existing_close(family):
    bars=data()
    for name in ('open','close','high','low'):
        values=getattr(bars,name).copy();values[2:]=50.;bars=replace(bars,**{name:values})
    q=rules(step='.1',notional='20')
    slow,fast=pair(family,bars,q)
    actual=slow(q).strategy_result
    assert len(actual.trades)==1 and actual.trades[0].exit_price==50.
    assert actual.balance_curve[-1]==pytest.approx(89.97)
    assert fast(q)[OUTPUT_FINAL_BALANCE]==pytest.approx(89.97)


@pytest.mark.parametrize('family',['position','signal'])
@pytest.mark.parametrize('capital',[0.,-5.,math.nan,math.inf,-math.inf])
def test_exhausted_capital_is_distinct_from_corruption(family,capital):
    q=rules(step='.1',notional='0')
    slow,fast=pair(family,data(),q,kernel_only=True,initialCapital=capital)
    if math.isfinite(capital):
        result=slow(q)
        assert not result.trades
        assert result.guardrail_summary.zero_size_entry_count==1
        row=fast(q);assert row[OUTPUT_ZERO_SIZE_ENTRY_COUNT]==1
        np.testing.assert_equal(row,fast(None))
    else:
        with pytest.raises(ValueError,match='Nonfinite'):slow(q)
        assert int(fast(q)[OUTPUT_FLAGS])&GUARDRAIL_FLAG_EXECUTION_ERROR


@pytest.mark.parametrize('family',['position','signal'])
def test_oversized_positive_lots_fail_explicitly(family):
    q=rules(step='1e-20',notional='0')
    slow,fast=pair(family,data(),q)
    with pytest.raises(ValueError,match='Unrepresentable'):slow(q)
    assert int(fast(q)[OUTPUT_FLAGS])&GUARDRAIL_FLAG_EXECUTION_ERROR


@pytest.mark.parametrize('short', [False, True])
def test_signal_finite_start_overflows_while_position_is_open(short):
    ordinary = data(length=5, short=short)
    quantity = rules(step='1', notional='0')
    changes = dict(kernel_only=True, initialCapital=1000., positionPct=100., commissionPct=0.)
    # Establish an actual finite fill at bar 1 before changing a nonfinal close.
    control, _ = pair('signal', ordinary, quantity, **changes)
    trade = control(quantity).trades[0]
    assert trade.entry_time == ordinary.timestamps[1] and trade.size == 10.
    closes, highs = ordinary.close.copy(), ordinary.high.copy()
    closes[2] = highs[2] = 1e308
    bars = replace(ordinary, close=closes, high=highs)
    slow, fast = pair('signal', bars, quantity, **changes)
    with pytest.raises(ValueError, match='Nonfinite quantity-execution account capital or equity'):
        slow(quantity)
    assert int(fast(quantity)[OUTPUT_FLAGS]) & GUARDRAIL_FLAG_EXECUTION_ERROR


@pytest.mark.parametrize('family',['position','signal'])
def test_midrun_negative_balance_refuses_new_entries_with_legacy_counts(family):
    bars=data(length=8,signals=(0,4) if family=='position' else (0,))
    if family=='position':
        opens=bars.open.copy();opens[2]=50
        bars=replace(bars,open=opens,low=np.minimum(opens,bars.close)-.1)
    else:
        opposite=np.zeros(8,bool);opposite[[2,5]]=True
        bars=replace(bars,signals=Signals(bars.signals.long_entries,opposite))
    q=rules(step='.1',notional='0')
    slow,fast=pair(family,bars,q,kernel_only=True,commissionPct=100.,riskPerTrade=10.,positionPct=100.)
    result=slow(q)
    assert len(result.trades)==1 and result.balance_curve[-1]==-100.
    assert result.guardrail_summary.zero_size_entry_count==1
    assert slow(None).guardrail_summary==result.guardrail_summary
    np.testing.assert_equal(fast(q),fast(None))


@pytest.mark.parametrize('short',[False,True])
def test_signal_close_requires_fresh_signal_and_survives_new_entry_refusal(short):
    bars=data(length=8,signals=())
    first=np.zeros(8,bool);first[0]=True
    opposite=np.zeros(8,bool);opposite[[2,5]]=True
    bars=replace(bars,signals=Signals(opposite if short else first,first if short else opposite))
    # Fill original at 100, close at 50. A new signal at 5 plans a new order, whose
    # next open gaps to 1: below-minimum entry is consumed and not revived at 7.
    opens=bars.open.copy();opens[3]=50;opens[6]=1
    bars=replace(bars,open=opens,high=np.maximum(opens,bars.close)+.1,low=np.minimum(opens,bars.close)-.1)
    q=rules(step='.1',notional='20')
    slow,fast=pair('signal',bars,q)
    result=slow(q).strategy_result
    assert len(result.trades)==1
    assert result.trades[0].entry_time==bars.timestamps[1]
    assert result.trades[0].exit_time==bars.timestamps[3]
    assert result.trades[0].exit_price==50
    assert fast(q)[OUTPUT_TOTAL_TRADES]==1


def test_envelope_and_unsupported_consumer_refuse():
    q=rules(step='.1',minimum='.100001',notional='0')
    assert q.minimum_lots==2
    assert read_quantity_envelope(quantity_envelope(q))==q
    for bad in [None,{},dict(quantity_envelope(q),version=2),dict(quantity_envelope(q),maxEntryLeverage=8)]:
        with pytest.raises(ValueError):read_quantity_envelope(bad)
    class Unregistered:
        @staticmethod
        def run(*args):raise AssertionError('must reject before dispatch')
    with pytest.raises(ValueError,match='registered'):run_strategy(Unregistered,None,{},quantity_rules=q)


def test_capability_is_profile_derived_and_incompatible_builders_reject():
    from copy import deepcopy
    from core.engine_v2.quantity import selector_capability
    from core.engine_v2.profile import parse_execution_profile
    from core.engine_v2.runner import build_kernel_config,build_signal_kernel_config
    from strategies import get_strategy_config
    for sid in LEGACY:
        cfg=get_strategy_config(sid)
        assert selector_capability(cfg)=={'policy':'instrument_rules_v1'}
        bad=deepcopy(cfg);bad['parameters']['contractSize']['optimize']['enabled']=True
        assert selector_capability(bad) is None
        bad=deepcopy(cfg);bad['execution']['boundary']='none'
        assert selector_capability(bad) is None
    risk=get_strategy_config('s07_ha_v01_b2');profile=parse_execution_profile(risk)
    assert selector_capability(risk)=={'policy':'risk_bracket_v1'}
    assert selector_capability({'engine':'v1'}) is None
    for builder in (build_kernel_config,build_signal_kernel_config):
        with pytest.raises(ValueError):builder(profile=profile,params=profile.parameter_defaults,quantity_rules=rules())


def test_packed_position_arrays_cannot_activate_or_change_policy():
    from core.engine_v2.compiled_kernel import _pack_config_arrays,build_stacked_execution_data,evaluate_compiled_stacked_batch
    _,_,_,profile,params=setup(LEGACY[2],'false')
    q=rules(step='.1',notional='0')
    arrays=_pack_config_arrays(profile,[params],0,quantity_rules=q)
    stacked=build_stacked_execution_data([data()],[0])
    with pytest.raises(ValueError):
        evaluate_compiled_stacked_batch(stacked_data=stacked,profile=profile,packed_config_arrays=arrays,trade_start_idx=0)
    for key in ('admission_cap','admission_min_lots','admission_min_notional','contract_size'):
        changed={k:v.copy() for k,v in arrays.items()};changed[key][0]+=1
        with pytest.raises(ValueError):
            evaluate_compiled_stacked_batch(stacked_data=stacked,profile=profile,packed_config_arrays=changed,trade_start_idx=0,quantity_rules=q)
