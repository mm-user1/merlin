import numpy as np
import pandas as pd
import pytest

from core.engine_v2.contracts import Signals
from core.engine_v2.kernel import ExecutionData, KernelConfig, intrabar_path, run_reference_kernel
from core.engine_v2.kernel import EntryPolicy, KernelTrace, quantity_lots


@pytest.mark.parametrize("cap,filled", [(2.0, True), (1.999, False), (2.001, True)])
def test_reference_policy_exact_cap_and_rejection_state(cap, filled):
    data = _data(open_=[100, 100, 100], high=[101, 101, 101], low=[90, 99, 99],
                 close=[100, 100, 100], long=[True, True, False])
    trace = KernelTrace()
    result = run_reference_kernel(data, KernelConfig(initial_capital=100, risk_per_trade_pct=20,
                                  contract_size=1, stop_x=0), policy=EntryPolicy(cap), trace=trace)
    assert trace.attempts[0].quantity == 2
    assert trace.attempts[0].required_leverage == 2
    assert trace.attempts[0].reason == ("filled" if filled else "leverage_cap_exceeded")
    assert trace.attempts[1].reason == ("occupied" if filled else "leverage_cap_exceeded")
    assert result.balance_curve == [100, 100, 100]
    assert result.guardrail_summary.margin_reject_count == (0 if filled else 2)
    assert not result.guardrail_summary.no_capital_halt


@pytest.mark.parametrize("fee,minimum,reason", [(1, None, "leverage_cap_exceeded"),
                                               (0, 201, "below_min_notional"),
                                               (60, None, "leverage_undefined")])
def test_reference_policy_fee_denominator_and_minimum(fee, minimum, reason):
    trace = KernelTrace()
    data = _data(open_=[100, 100], high=[100, 100], low=[90, 99], close=[100, 100], long=[True, False])
    result = run_reference_kernel(data, KernelConfig(initial_capital=100, risk_per_trade_pct=20,
                                  contract_size=1, stop_x=0, commission_pct=fee),
                                  policy=EntryPolicy(2, minimum_notional=minimum), trace=trace)
    assert trace.attempts[0].reason == reason
    assert result.trades == [] and result.balance_curve == [100, 100]
    if fee == 1:
        assert trace.attempts[0].required_leverage == 200/98
    assert result.guardrail_summary.margin_reject_count == int(reason == "leverage_cap_exceeded")


def test_reference_policy_gap_entry_two_fees_and_trace_only_preservation():
    data = _data(open_=[100, 80], high=[100, 82], low=[90, 79], close=[100, 81], long=[True, False])
    config = KernelConfig(initial_capital=100, risk_per_trade_pct=20, contract_size=1,
                          stop_x=0, commission_pct=.5)
    plain = run_reference_kernel(data, config)
    trace_only = KernelTrace()
    observed = run_reference_kernel(data, config, trace=trace_only)
    trace = KernelTrace()
    checked = run_reference_kernel(data, config, policy=EntryPolicy(8), trace=trace)
    from dataclasses import asdict
    for result in (observed, checked):
        assert result.trades == plain.trades
        assert result.balance_curve == plain.balance_curve
        assert result.equity_curve == plain.equity_curve
        assert result.timestamps == plain.timestamps
        assert result.guardrail_summary == plain.guardrail_summary
        for key, value in asdict(plain.standing_state).items():
            actual = getattr(result.standing_state, key)
            assert (np.isnan(actual) if isinstance(value, float) and np.isnan(value) else actual == value)
    assert checked.trades[0].net_pnl == -1.6
    assert checked.trades[0].exit_reason is None
    assert trace.exits[0].reason == "stop" and trace.exits[0].phase == "open"
    assert trace.exits[0].entry_fee == trace.exits[0].exit_fee == .8
    assert not trace.exits[0].ambiguous


def test_reference_policy_precedence_and_lot_boundary():
    from core.engine_v2.sizing import risk_position_size
    q = risk_position_size(balance=23, risk_distance=10, risk_per_trade_pct=100, contract_size=.1)
    assert q == 2.2 and quantity_lots(q, .1) == 22
    with pytest.raises(ValueError, match="Unrepresentable"):
        quantity_lots(1e30, .1)
    data = _data(open_=[100]*3, high=[100]*3, low=[90]*3, close=[100]*3,
                 long=[True]*3, atr=[np.nan, 0, 0])
    trace = KernelTrace()
    run_reference_kernel(data, KernelConfig(initial_capital=0, stop_x=0),
                         policy=EntryPolicy(8), trace=trace)
    assert [a.reason for a in trace.attempts] == ["indicator_unavailable", "nonpositive_capital", "no_next_bar"]


@pytest.mark.parametrize("minimum,reason",[(2,"filled"),(3,"below_min_quantity")])
def test_reference_policy_integer_minimum_lots(minimum,reason):
    data=_data(open_=[100,100],high=[100,100],low=[90,99],close=[100,100],long=[True,False])
    trace=KernelTrace()
    result=run_reference_kernel(data,KernelConfig(initial_capital=100,risk_per_trade_pct=20,contract_size=1,stop_x=0),
                                policy=EntryPolicy(8,minimum_lots=minimum),trace=trace)
    assert trace.attempts[0].reason==reason and trace.attempts[0].lots==2
    assert len(result.trades)==int(reason=="filled")


def test_reference_policy_nonfinite_fill_is_not_a_minimum_pass():
    data=_data(open_=[100,float('inf')],high=[100,float('inf')],low=[90,100],close=[100,100],long=[True,False])
    trace=KernelTrace()
    result=run_reference_kernel(data,KernelConfig(initial_capital=100,risk_per_trade_pct=20,contract_size=1,stop_x=0),
                                policy=EntryPolicy(8,minimum_notional=1000),trace=trace)
    assert trace.attempts[0].reason=="leverage_undefined" and not result.trades
    assert result.guardrail_summary.margin_reject_count==0


def test_reference_policy_overflowing_ratio_precedes_minimum_comparison():
    data = _data(open_=[2e-300, 1e100], high=[2e-300, 1e100], low=[1e-300, 1e100],
                 close=[2e-300, 1e100], long=[True, False])
    trace = KernelTrace()
    result = run_reference_kernel(data, KernelConfig(initial_capital=1e-300, risk_per_trade_pct=100,
                                  contract_size=1, stop_x=0),
                                  policy=EntryPolicy(8, minimum_notional=1e101), trace=trace)
    assert trace.attempts[0].notional == 1e100
    assert trace.attempts[0].reason == "leverage_undefined"
    assert trace.attempts[0].required_leverage is None and not result.trades


def _data(
    *,
    open_,
    high,
    low,
    close,
    long=None,
    short=None,
    atr=None,
    rolling_low=None,
    rolling_high=None,
    trail_long=None,
    trail_short=None,
    chandelier_atr=None,
):
    length = len(open_)
    return ExecutionData(
        timestamps=tuple(pd.date_range("2025-01-01", periods=length, freq="30min", tz="UTC")),
        open=np.array(open_, dtype=float),
        high=np.array(high, dtype=float),
        low=np.array(low, dtype=float),
        close=np.array(close, dtype=float),
        signals=Signals(
            long_entries=np.array(long if long is not None else [False] * length, dtype=bool),
            short_entries=np.array(short if short is not None else [False] * length, dtype=bool),
        ),
        atr=np.array(atr if atr is not None else [0.0] * length, dtype=float),
        rolling_low=np.array(rolling_low if rolling_low is not None else low, dtype=float),
        rolling_high=np.array(rolling_high if rolling_high is not None else high, dtype=float),
        trail_long=np.array(trail_long if trail_long is not None else [np.nan] * length, dtype=float),
        trail_short=np.array(trail_short if trail_short is not None else [np.nan] * length, dtype=float),
        chandelier_atr=(
            None if chandelier_atr is None else np.array(chandelier_atr, dtype=float)
        ),
    )


def test_market_entry_uses_signal_close_for_size_and_next_open_for_fill():
    data = _data(
        open_=[100.0, 105.0, 106.0],
        high=[100.0, 106.0, 106.0],
        low=[97.0, 104.0, 106.0],
        close=[100.0, 105.5, 106.0],
        long=[True, False, False],
        rolling_low=[97.0, 104.0, 106.0],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=99.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            reward_risk=2.0,
            max_stop_pct=10.0,
        ),
    )

    trade = result.trades[0]
    assert trade.entry_price == 105.0
    assert trade.exit_price == 106.0
    assert trade.size == 33.0


@pytest.mark.parametrize(
    ("direction", "open_", "high", "low", "close", "target_mode", "expected_exit"),
    [
        (1, [100.0, 107.0], [100.0, 108.0], [97.0, 106.0], [100.0, 107.0], "rr", 107.0),
        (1, [100.0, 96.0], [100.0, 97.0], [97.0, 95.0], [100.0, 96.0], "rr", 96.0),
        (-1, [100.0, 93.0], [103.0, 94.0], [100.0, 92.0], [100.0, 93.0], "rr", 93.0),
        (-1, [100.0, 104.0], [103.0, 105.0], [100.0, 103.0], [100.0, 104.0], "rr", 104.0),
    ],
)
def test_gap_exits_fill_at_open(direction, open_, high, low, close, target_mode, expected_exit):
    data = _data(
        open_=open_,
        high=high,
        low=low,
        close=close,
        long=[direction > 0, False],
        short=[direction < 0, False],
        rolling_low=[97.0, low[1]],
        rolling_high=[103.0, high[1]],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            reward_risk=2.0,
            max_stop_pct=10.0,
            target_mode=target_mode,
        ),
    )

    assert result.trades[0].exit_price == expected_exit


def test_intrabar_path_high_first_and_tie_low_first():
    assert intrabar_path(100.0, 103.0, 90.0, 101.0) == (100.0, 103.0, 90.0, 101.0)
    assert intrabar_path(100.0, 105.0, 95.0, 101.0) == (100.0, 95.0, 105.0, 101.0)


def test_stop_target_collision_uses_path_order_and_flat_segment_is_rising():
    low_first = _data(
        open_=[100.0, 100.0],
        high=[100.0, 106.0],
        low=[95.0, 94.0],
        close=[100.0, 100.0],
        long=[True, False],
        rolling_low=[95.0, 94.0],
    )
    low_first_result = run_reference_kernel(
        low_first,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            reward_risk=1.0,
            max_stop_pct=10.0,
        ),
    )

    high_first = _data(
        open_=[100.0, 100.0],
        high=[100.0, 104.0],
        low=[95.0, 90.0],
        close=[100.0, 100.0],
        long=[True, False],
        rolling_low=[95.0, 90.0],
    )
    high_first_result = run_reference_kernel(
        high_first,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            reward_risk=0.6,
            max_stop_pct=10.0,
        ),
    )

    assert low_first_result.trades[0].exit_price == 95.0
    assert high_first_result.trades[0].exit_price == 103.0
    assert intrabar_path(100.0, 100.0, 99.0, 100.0) == (100.0, 100.0, 99.0, 100.0)


def test_flat_segment_execution_remains_stable_when_open_equals_high():
    data = _data(
        open_=[100.0, 100.0],
        high=[100.0, 100.0],
        low=[97.0, 94.0],
        close=[100.0, 96.0],
        long=[True, False],
        rolling_low=[97.0, 94.0],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            reward_risk=1.0,
            max_stop_pct=10.0,
        ),
    )

    assert intrabar_path(100.0, 100.0, 94.0, 96.0)[:2] == (100.0, 100.0)
    assert result.trades[0].exit_price == 97.0


def test_trail_activation_on_entry_fill_bar_can_exit_same_bar():
    data = _data(
        open_=[100.0, 103.0],
        high=[100.0, 104.0],
        low=[97.0, 99.0],
        close=[100.0, 102.0],
        long=[True, False],
        rolling_low=[97.0, 99.0],
        trail_long=[np.nan, 101.0],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            target_mode="none",
            trail_mode="ma",
            trail_activation_mode="rr",
            trail_activation_rr=1.0,
        ),
    )

    assert result.trades[0].exit_price == 101.0


def test_post_path_trail_ratchet_applies_to_future_bar_only():
    data = _data(
        open_=[100.0, 100.0, 102.0, 101.0],
        high=[100.0, 102.0, 104.0, 101.0],
        low=[97.0, 99.0, 99.0, 100.0],
        close=[100.0, 101.0, 103.0, 100.5],
        long=[True, False, False, False],
        rolling_low=[97.0, 99.0, 99.0, 100.0],
        trail_long=[np.nan, np.nan, 101.5, 101.5],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            target_mode="none",
            trail_mode="ma",
            trail_activation_mode="rr",
            trail_activation_rr=1.0,
        ),
    )

    assert len(result.trades) == 1
    assert result.trades[0].exit_time == data.timestamps[3]
    assert result.trades[0].exit_price == 101.0


def _stateful_config(mode, **overrides):
    values = dict(
        initial_capital=100.0,
        risk_per_trade_pct=100.0,
        contract_size=1.0,
        stop_x=0.0,
        max_stop_pct=20.0,
        target_mode="none",
        trail_mode=mode,
        trail_activation_mode="rr",
        trail_activation_rr=1.0,
        max_days_enabled=False,
        boundary_mode="none",
    )
    values.update(overrides)
    return KernelConfig(**values)


def test_r_distance_activation_is_exact_and_future_effective_with_fill_price_breakeven():
    data = _data(
        open_=[100.0, 103.0, 102.0],
        high=[100.0, 105.0, 103.0],
        low=[95.0, 96.0, 101.0],
        close=[100.0, 101.0, 102.0],
        long=[True, False, False],
        rolling_low=[95.0, 96.0, 101.0],
    )

    result = run_reference_kernel(
        data,
        _stateful_config("r_distance", trail_distance_r=2.0),
    )

    assert result.trades[0].entry_price == 103.0
    assert result.trades[0].exit_time == data.timestamps[2]
    assert result.trades[0].exit_price == 102.0


def test_r_distance_first_candidate_requires_strict_side_but_later_candidates_only_ratchet():
    equality = _data(
        open_=[100.0, 100.0],
        high=[100.0, 105.0],
        low=[95.0, 99.0],
        close=[100.0, 101.0],
        long=[True, False],
        rolling_low=[95.0, 99.0],
    )
    equality_result = run_reference_kernel(
        equality,
        _stateful_config("r_distance", trail_distance_r=0.8),
    )
    assert equality_result.standing_state.trail_active is True
    assert equality_result.standing_state.trail_stop == 100.0

    later = _data(
        open_=[100.0, 100.0, 104.0],
        high=[100.0, 106.0, 110.0],
        low=[95.0, 99.0, 102.0],
        close=[100.0, 104.0, 104.0],
        long=[True, False, False],
        rolling_low=[95.0, 99.0, 102.0],
    )
    later_result = run_reference_kernel(
        later,
        _stateful_config("r_distance", trail_distance_r=1.0),
    )
    assert later_result.standing_state.trail_stop == 105.0


def test_chandelier_waits_for_first_finite_atr_then_becomes_method_active():
    data = _data(
        open_=[100.0, 100.0, 104.0],
        high=[100.0, 106.0, 110.0],
        low=[95.0, 99.0, 102.0],
        close=[100.0, 104.0, 108.0],
        long=[True, False, False],
        rolling_low=[95.0, 99.0, 102.0],
        chandelier_atr=[np.nan, np.nan, 2.0],
    )

    result = run_reference_kernel(
        data,
        _stateful_config("chandelier", chandelier_atr_mult=2.0),
    )

    assert result.standing_state.trail_stop == 106.0


def test_fixed_af_sar_uses_trade_local_recurrence_and_two_bar_range_cap():
    data = _data(
        open_=[100.0, 100.0, 104.0, 108.0],
        high=[100.0, 105.0, 110.0, 112.0],
        low=[95.0, 99.0, 103.0, 106.0],
        close=[100.0, 104.0, 108.0, 110.0],
        long=[True, False, False, False],
        rolling_low=[95.0, 99.0, 103.0, 106.0],
    )

    result = run_reference_kernel(
        data,
        _stateful_config("fixed_af_sar", sar_speed=0.2),
    )

    assert result.standing_state.trail_stop == pytest.approx(102.4)


def test_r_distance_short_uses_symmetric_activation_and_ratchet():
    data = _data(
        open_=[100.0, 100.0, 96.0],
        high=[105.0, 101.0, 98.0],
        low=[100.0, 94.0, 90.0],
        close=[100.0, 96.0, 92.0],
        short=[True, False, False],
        rolling_high=[105.0, 101.0, 98.0],
    )

    result = run_reference_kernel(
        data,
        _stateful_config("r_distance", trail_distance_r=1.0),
    )

    assert result.standing_state.trail_stop == 95.0


@pytest.mark.parametrize(
    ("mode", "extra", "chandelier", "expected"),
    [
        ("chandelier", {"chandelier_atr_mult": 2.0}, [np.nan, np.nan, 2.0], 94.0),
        ("fixed_af_sar", {"sar_speed": 0.2}, None, 97.6),
    ],
)
def test_chandelier_and_fixed_af_sar_short_formulas(mode, extra, chandelier, expected):
    if mode == "chandelier":
        open_ = [100.0, 100.0, 96.0]
        high = [105.0, 101.0, 98.0]
        low = [100.0, 94.0, 90.0]
        close = [100.0, 96.0, 92.0]
    else:
        open_ = [100.0, 100.0, 96.0, 92.0]
        high = [105.0, 101.0, 97.0, 94.0]
        low = [100.0, 95.0, 90.0, 88.0]
        close = [100.0, 96.0, 92.0, 90.0]
    data = _data(
        open_=open_,
        high=high,
        low=low,
        close=close,
        short=[True] + [False] * (len(open_) - 1),
        rolling_high=high,
        chandelier_atr=chandelier,
    )

    result = run_reference_kernel(data, _stateful_config(mode, **extra))

    assert result.standing_state.trail_stop == pytest.approx(expected)


@pytest.mark.parametrize(
    ("mode", "overrides", "message"),
    [
        ("r_distance", {"trail_activation_rr": 0.0, "trail_distance_r": 1.0}, "trailRR"),
        ("r_distance", {"trail_distance_r": np.nan}, "trailDistanceR"),
        ("chandelier", {"chandelier_atr_mult": 0.0}, "chandelierATRMult"),
        ("fixed_af_sar", {"sar_speed": 1.01}, "sarSpeed"),
    ],
)
def test_active_stateful_scalar_validation(mode, overrides, message):
    data = _data(
        open_=[100.0],
        high=[100.0],
        low=[95.0],
        close=[100.0],
        chandelier_atr=[1.0] if mode == "chandelier" else None,
    )

    with pytest.raises(ValueError, match=message):
        run_reference_kernel(data, _stateful_config(mode, **overrides))


def test_active_chandelier_rejects_missing_data():
    data = _data(open_=[100.0], high=[100.0], low=[95.0], close=[100.0])

    with pytest.raises(ValueError, match="chandelier_atr"):
        run_reference_kernel(data, _stateful_config("chandelier", chandelier_atr_mult=2.0))


def test_first_stateful_candidate_side_check_precedes_tick_rounding():
    data = _data(
        open_=[100.0, 99.0],
        high=[100.0, 105.04],
        low=[95.0, 98.0],
        close=[100.0, 100.04],
        long=[True, False],
        rolling_low=[95.0, 98.0],
    )

    result = run_reference_kernel(
        data,
        _stateful_config(
            "r_distance",
            trail_distance_r=1.0,
            price_rounding_mode="tick_outward",
            tick_size=0.1,
        ),
    )

    assert result.standing_state.trail_stop == 99.0


def test_stateful_trail_state_resets_after_close_before_next_trade():
    data = _data(
        open_=[100.0, 100.0, 99.0, 100.0, 100.0, 100.0],
        high=[100.0, 106.0, 100.0, 100.0, 102.0, 102.0],
        low=[95.0, 99.0, 98.0, 95.0, 99.0, 99.0],
        close=[100.0, 104.0, 99.0, 100.0, 101.0, 101.0],
        long=[True, False, False, True, False, False],
        rolling_low=[95.0, 99.0, 98.0, 95.0, 99.0, 99.0],
    )

    result = run_reference_kernel(
        data,
        _stateful_config("r_distance", trail_distance_r=1.0),
    )

    assert len(result.trades) == 1
    assert result.standing_state.position_direction == 1
    assert result.standing_state.trail_active is False
    assert result.standing_state.trail_stop == 95.0


def test_max_days_and_strict_boundary_behaviors():
    scheduled = _data(
        open_=[100.0, 100.0, 101.0, 103.0, 104.0],
        high=[100.0, 101.0, 102.0, 104.0, 104.0],
        low=[97.0, 99.0, 100.0, 102.0, 104.0],
        close=[100.0, 100.5, 101.0, 102.0, 104.0],
        long=[True, False, False, False, False],
        rolling_low=[97.0, 99.0, 100.0, 102.0, 104.0],
    )
    scheduled_result = run_reference_kernel(
        scheduled,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            reward_risk=10.0,
            max_days=1.0 / 48.0,
        ),
    )

    final = _data(
        open_=[100.0, 100.0, 101.0],
        high=[100.0, 101.0, 101.0],
        low=[97.0, 99.0, 99.0],
        close=[100.0, 100.0, 102.0],
        long=[True, False, False],
        rolling_low=[97.0, 99.0, 99.0],
    )
    final_result = run_reference_kernel(
        final,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            reward_risk=10.0,
            max_days=1.0 / 48.0,
        ),
    )

    assert scheduled_result.trades[0].exit_time == scheduled.timestamps[3]
    assert scheduled_result.trades[0].exit_price == 103.0
    assert final_result.trades[0].exit_time == final.timestamps[2]
    assert final_result.trades[0].exit_price == 102.0


def test_strict_boundary_cancels_final_bar_entry_signal():
    data = _data(
        open_=[100.0, 100.0],
        high=[100.0, 100.0],
        low=[97.0, 97.0],
        close=[100.0, 100.0],
        long=[False, True],
        rolling_low=[97.0, 97.0],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
        ),
    )

    assert result.trades == []
    assert result.standing_state.pending_entry_direction == 0


def test_boundary_none_preserves_pending_entry_and_pending_close_state():
    pending_entry_data = _data(
        open_=[100.0],
        high=[100.0],
        low=[97.0],
        close=[100.0],
        long=[True],
        rolling_low=[97.0],
    )
    pending_entry_result = run_reference_kernel(
        pending_entry_data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            boundary_mode="none",
        ),
    )

    pending_close_data = _data(
        open_=[100.0, 100.0, 100.0],
        high=[100.0, 101.0, 101.0],
        low=[97.0, 99.0, 99.0],
        close=[100.0, 100.0, 100.0],
        long=[True, False, False],
        rolling_low=[97.0, 99.0, 99.0],
    )
    pending_close_result = run_reference_kernel(
        pending_close_data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            reward_risk=10.0,
            max_days=1.0 / 48.0,
            boundary_mode="none",
        ),
    )

    assert pending_entry_result.standing_state.pending_entry_direction == 1
    assert pending_entry_result.standing_state.pending_entry_order_type == "market_next_open"
    assert pending_close_result.standing_state.position_direction == 1
    assert pending_close_result.standing_state.pending_market_close is True


def test_warmup_zone_signal_does_not_create_entry():
    data = _data(
        open_=[100.0, 100.0, 100.0, 100.0],
        high=[100.0, 100.0, 103.0, 106.0],
        low=[97.0, 97.0, 97.0, 99.0],
        close=[100.0, 100.0, 100.0, 105.0],
        long=[True, False, True, False],
        rolling_low=[97.0, 97.0, 97.0, 99.0],
    )

    result = run_reference_kernel(
        data,
        KernelConfig(
            initial_capital=100.0,
            risk_per_trade_pct=100.0,
            contract_size=1.0,
            stop_x=0.0,
            max_stop_pct=10.0,
            trade_start_idx=2,
        ),
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_time == data.timestamps[3]
