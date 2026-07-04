import numpy as np
import pandas as pd
import pytest

from core.engine_v2.contracts import Signals
from core.engine_v2.kernel import ExecutionData, KernelConfig, intrabar_path, run_reference_kernel


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


def test_max_days_and_strict_boundary_behaviors():
    scheduled = _data(
        open_=[100.0, 100.0, 100.0, 102.0],
        high=[100.0, 101.0, 101.0, 102.0],
        low=[97.0, 99.0, 99.0, 102.0],
        close=[100.0, 100.0, 100.0, 102.0],
        long=[True, False, False, False],
        rolling_low=[97.0, 99.0, 99.0, 102.0],
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
            max_days=1.0 / 24.0,
        ),
    )

    final = _data(
        open_=[100.0, 100.0, 100.0],
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
    assert scheduled_result.trades[0].exit_price == 102.0
    assert final_result.trades[0].exit_time == final.timestamps[2]
    assert final_result.trades[0].exit_price == 102.0


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
