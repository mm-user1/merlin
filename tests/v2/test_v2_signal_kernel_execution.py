import numpy as np
import pandas as pd
import pytest

from core.engine_v2.contracts import GUARDRAIL_FLAG_ZERO_SIZE_ENTRY, Signals
from core.engine_v2.dataprep import build_signal_execution_data
from core.engine_v2.kernel_signal import (
    EMERGENCY_SL_EXIT_REASON,
    SignalKernelConfig,
    run_signal_reversal_kernel,
)


def _data(
    *,
    open_,
    high,
    low,
    close,
    long=None,
    short=None,
    long_exit=None,
    short_exit=None,
):
    length = len(open_)
    frame = pd.DataFrame(
        {
            "Open": np.array(open_, dtype=float),
            "High": np.array(high, dtype=float),
            "Low": np.array(low, dtype=float),
            "Close": np.array(close, dtype=float),
            "Volume": np.full(length, 1000.0),
        },
        index=pd.date_range("2025-01-01", periods=length, freq="30min", tz="UTC"),
    )
    return build_signal_execution_data(
        frame,
        signals=Signals(
            long_entries=np.array(long if long is not None else [False] * length, dtype=bool),
            short_entries=np.array(short if short is not None else [False] * length, dtype=bool),
            long_exits=None if long_exit is None else np.array(long_exit, dtype=bool),
            short_exits=None if short_exit is None else np.array(short_exit, dtype=bool),
        ),
    )


def _run(data, **overrides):
    config = SignalKernelConfig(**overrides)
    return run_signal_reversal_kernel(data, config)


def test_fixed_pct_sizing_uses_plan_close_and_preserves_size_until_next_open_fill():
    data = _data(
        open_=[10.0, 12.0, 14.0],
        high=[10.0, 12.0, 14.0],
        low=[10.0, 12.0, 14.0],
        close=[10.0, 13.0, 14.0],
        long=[True, False, False],
    )

    result = _run(data, initial_capital=100.0, position_pct=50.0, contract_size=1.0)

    trade = result.trades[0]
    assert trade.size == 5.0
    assert trade.entry_time == data.timestamps[1]
    assert trade.entry_price == 12.0
    assert trade.exit_time == data.timestamps[2]
    assert trade.exit_price == 14.0


def test_opposite_signal_exits_next_open_and_persistent_reversal_fills_one_bar_later():
    data = _data(
        open_=[100.0, 101.0, 102.0, 103.0],
        high=[100.0, 101.0, 102.0, 103.0],
        low=[100.0, 101.0, 102.0, 103.0],
        close=[100.0, 101.0, 102.0, 103.0],
        long=[True, False, False, False],
        short=[False, True, True, False],
    )

    result = _run(data, contract_size=0.1, boundary_mode="none")

    assert len(result.trades) == 1
    assert result.trades[0].direction == "long"
    assert result.trades[0].exit_time == data.timestamps[2]
    assert result.trades[0].exit_reason is None
    assert result.standing_state.position_direction == -1
    assert result.standing_state.entry_time_ns == pd.Timestamp(data.timestamps[3]).value


def test_flat_exit_arrays_close_next_open_and_enable_flags_do_not_block_exits():
    data = _data(
        open_=[100.0, 101.0, 102.0, 103.0],
        high=[100.0, 101.0, 102.0, 103.0],
        low=[100.0, 101.0, 102.0, 103.0],
        close=[100.0, 101.0, 102.0, 103.0],
        long=[True, False, False, False],
        short=[False, True, True, False],
        long_exit=[False, True, False, False],
    )
    no_flat_data = _data(
        open_=[100.0, 101.0, 102.0, 103.0],
        high=[100.0, 101.0, 102.0, 103.0],
        low=[100.0, 101.0, 102.0, 103.0],
        close=[100.0, 101.0, 102.0, 103.0],
        long=[True, False, False, False],
        short=[False, False, False, False],
    )

    result = _run(data, contract_size=1.0, enable_short=False, boundary_mode="none")
    no_flat_result = _run(no_flat_data, contract_size=1.0, enable_short=False, boundary_mode="none")

    assert result.trades[0].exit_time == data.timestamps[2]
    assert result.trades[0].exit_reason is None
    assert result.standing_state.position_direction == 0
    assert no_flat_result.trades == []
    assert no_flat_result.standing_state.position_direction == 1


def test_out_of_range_close_fills_next_open_and_no_new_entries_out_of_range():
    data = _data(
        open_=[100.0, 101.0, 102.0, 103.0],
        high=[100.0, 101.0, 102.0, 103.0],
        low=[100.0, 101.0, 102.0, 103.0],
        close=[100.0, 101.0, 102.0, 103.0],
        long=[True, False, True, True],
    )

    result = _run(
        data,
        contract_size=1.0,
        start=data.timestamps[0],
        end=data.timestamps[1],
        boundary_mode="none",
    )

    assert len(result.trades) == 1
    assert result.trades[0].exit_time == data.timestamps[3]
    assert result.trades[0].exit_price == 103.0
    assert result.standing_state.position_direction == 0
    assert result.standing_state.pending_entry_direction == 0


def test_emergency_long_ignores_fill_bar_gap_fills_open_and_reenters_from_same_signal_bar():
    data = _data(
        open_=[100.0, 110.0, 80.0, 90.0],
        high=[100.0, 111.0, 90.0, 90.0],
        low=[100.0, 90.0, 79.0, 90.0],
        close=[100.0, 110.0, 90.0, 90.0],
        long=[True, False, True, False],
    )

    result = _run(
        data,
        initial_capital=1000.0,
        contract_size=1.0,
        emergency_stop_enabled=True,
        emergency_sl_pct=10.0,
        emergency_sl_update_bars=16,
        boundary_mode="none",
    )

    emergency_trade = result.trades[0]
    assert emergency_trade.exit_reason == EMERGENCY_SL_EXIT_REASON
    assert emergency_trade.entry_time == data.timestamps[1]
    assert emergency_trade.exit_time == data.timestamps[2]
    assert emergency_trade.exit_price == 80.0
    assert result.standing_state.position_direction == 1
    assert result.standing_state.entry_time_ns == pd.Timestamp(data.timestamps[3]).value


def test_emergency_short_ignores_fill_bar_and_gap_fills_open():
    data = _data(
        open_=[100.0, 90.0, 120.0, 120.0],
        high=[100.0, 110.0, 121.0, 120.0],
        low=[100.0, 89.0, 119.0, 120.0],
        close=[100.0, 90.0, 120.0, 120.0],
        short=[True, False, False, False],
    )

    result = _run(
        data,
        contract_size=1.0,
        emergency_stop_enabled=True,
        emergency_sl_pct=10.0,
        emergency_sl_update_bars=16,
        boundary_mode="none",
    )

    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == EMERGENCY_SL_EXIT_REASON
    assert result.trades[0].entry_time == data.timestamps[1]
    assert result.trades[0].exit_time == data.timestamps[2]
    assert result.trades[0].exit_price == 120.0


def test_emergency_ratchet_attempts_on_fill_plus_update_bars_and_resets_rejected_counter():
    closes = [100.0, 110.0, 101.0, 101.0, 120.0, 120.0]
    data = _data(
        open_=closes,
        high=[value + 1.0 for value in closes],
        low=[100.0, 109.0, 100.0, 100.0, 100.0, 100.0],
        close=closes,
        long=[True, False, False, False, False, False],
    )

    before_rejected = _run(
        _data(
            open_=closes[:3],
            high=[value + 1.0 for value in closes[:3]],
            low=[100.0, 109.0, 100.0],
            close=closes[:3],
            long=[True, False, False],
        ),
        contract_size=1.0,
        emergency_stop_enabled=True,
        emergency_sl_pct=10.0,
        emergency_sl_update_bars=2,
        boundary_mode="none",
    )
    after_rejected = _run(
        _data(
            open_=closes[:5],
            high=[value + 1.0 for value in closes[:5]],
            low=[100.0, 109.0, 100.0, 100.0, 100.0],
            close=closes[:5],
            long=[True, False, False, False, False],
        ),
        contract_size=1.0,
        emergency_stop_enabled=True,
        emergency_sl_pct=10.0,
        emergency_sl_update_bars=2,
        boundary_mode="none",
    )
    after_accepted = _run(
        data,
        contract_size=1.0,
        emergency_stop_enabled=True,
        emergency_sl_pct=10.0,
        emergency_sl_update_bars=2,
        boundary_mode="none",
    )

    assert before_rejected.standing_state.active_stop == pytest.approx(99.0)
    assert after_rejected.standing_state.active_stop == pytest.approx(99.0)
    assert after_accepted.standing_state.active_stop == pytest.approx(108.0)


def test_strict_boundary_closes_open_position_and_cancels_pending_order():
    open_position = _data(
        open_=[100.0, 101.0],
        high=[100.0, 101.0],
        low=[100.0, 101.0],
        close=[100.0, 102.0],
        long=[True, False],
    )
    final_pending = _data(
        open_=[100.0],
        high=[100.0],
        low=[100.0],
        close=[100.0],
        long=[True],
    )
    final_pending_none = _data(
        open_=[100.0],
        high=[100.0],
        low=[100.0],
        close=[100.0],
        long=[True],
    )

    closed = _run(open_position, contract_size=1.0)
    canceled = _run(final_pending, contract_size=1.0)
    standing = _run(final_pending_none, contract_size=1.0, boundary_mode="none")

    assert closed.trades[0].exit_time == open_position.timestamps[1]
    assert closed.trades[0].exit_price == 102.0
    assert closed.standing_state.position_direction == 0
    assert canceled.standing_state.pending_entry_direction == 0
    assert standing.standing_state.pending_entry_direction == 1


def test_zero_size_entry_sets_guardrail_without_creating_order():
    data = _data(
        open_=[100.0, 100.0],
        high=[100.0, 100.0],
        low=[100.0, 100.0],
        close=[100.0, 100.0],
        long=[True, False],
    )

    result = _run(data, initial_capital=1.0, contract_size=10.0, boundary_mode="none")

    assert result.trades == []
    assert result.guardrail_summary.zero_size_entry_count == 1
    assert result.guardrail_summary.first_guardrail_code == GUARDRAIL_FLAG_ZERO_SIZE_ENTRY
    assert result.guardrail_summary.flags & GUARDRAIL_FLAG_ZERO_SIZE_ENTRY
