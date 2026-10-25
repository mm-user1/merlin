"""Synthetic Merlin input admission and pinned replay, without market data."""
from copy import deepcopy
import io
import json

import numpy as np
import pandas as pd
import pytest

from data_provider import data, manifest
from core.backtest_engine import load_data
from core.data_input import (
    DataInputError, prepare_input, replay_input, prepare_manual_input,
    normalize_source, pack_session, timeframe_label, validate_pin,
)


from tests._data_input_helpers import publish


def test_exact_csv_frame_and_metadata_start(tmp_path):
    source = publish(tmp_path / "pack")
    prepared = prepare_input(source, date_filter=False, start="2030-01-01", end="2000-01-01")
    frame = prepared.frame.copy()
    frame.insert(0, "time", frame.index.asi8 // 10**9)
    csv = load_data(io.StringIO(frame.to_csv(index=False)))
    pd.testing.assert_frame_equal(prepared.frame, csv, check_names=False, check_exact=True)
    assert prepared.trade_start_idx == 0
    assert prepared.facts["bounds"]["start_origin"] == "metadata"


def test_submillisecond_start_and_date_end(tmp_path):
    source = publish(tmp_path / "pack")
    prepared = prepare_input(source, date_filter=True, start="2024-01-02T00:00:00.000001Z",
                             end="2024-01-02", warmup_bars=3)
    assert prepared.frame.index[prepared.trade_start_idx] == pd.Timestamp("2024-01-02T00:05Z")
    assert prepared.frame.index[-1] == pd.Timestamp("2024-01-02T23:55Z")
    assert prepared.trade_start_idx == 3


def test_epoch_three_day_grid(tmp_path):
    source = publish(tmp_path / "pack", rows=5000)
    source["timeframe_minutes"] = 4320
    prepared = prepare_input(source, date_filter=False)
    assert np.all(prepared.frame.index.asi8 % (4320 * 60 * 10**9) == 0)
    assert prepared.trade_start_idx == 0


def test_used_gap_refused_unused_gap_allowed(tmp_path):
    source = publish(tmp_path / "pack", drop=(1,))
    with pytest.raises(DataInputError, match="continuous"):
        prepare_input(source, date_filter=False)
    prepare_input(source, date_filter=True, start="2024-01-02", end="2024-01-02", warmup_bars=2)


def test_user_warmup_never_clamped(tmp_path):
    source = publish(tmp_path / "pack")
    with pytest.raises(DataInputError):
        prepare_input(source, date_filter=True, start="2024-01-01", end="2024-01-02", warmup_bars=1)


def test_relocation_unused_append_and_consumed_edit(tmp_path):
    source = publish(tmp_path / "original")
    prepared = prepare_input(source, date_filter=True, start="2024-01-02", end="2024-01-02", warmup_bars=2)
    pin = deepcopy(prepared.facts)
    longer = publish(tmp_path / "longer", rows=1500)
    actual = replay_input(pin, root_override=longer["pack_root"])
    pd.testing.assert_frame_equal(actual.frame, prepared.frame, check_exact=True)
    changed = publish(tmp_path / "changed", change=287)
    with pytest.raises(DataInputError, match="Pinned input changed"):
        replay_input(pin, root_override=changed["pack_root"])
    assert prepared.facts == pin


def test_manual_verifies_parent_before_new_interval(tmp_path):
    source = publish(tmp_path / "original")
    prepared = prepare_input(source, date_filter=True, start="2024-01-02", end="2024-01-02", warmup_bars=2)
    changed = publish(tmp_path / "changed", change=300)
    with pytest.raises(DataInputError, match="Pinned input changed"):
        prepare_manual_input(prepared.facts, root_override=changed["pack_root"],
                             date_filter=True, start="2024-01-03", end="2024-01-03", warmup_bars=2)


@pytest.mark.parametrize("value", [None, {}, [], {"version": 9}])
def test_corrupt_pin_is_authoritative(value):
    with pytest.raises(DataInputError, match="saved data_input"):
        validate_pin(value)


@pytest.mark.parametrize("minutes,label", [(2880, "2D"), (180, "3h"), (90, "90m")])
def test_timeframe_identity(minutes, label):
    assert timeframe_label(minutes) == label


def test_nonpack_preflight_does_not_create_lock(tmp_path):
    with pytest.raises(DataInputError, match="no pack manifest"):
        with pack_session(tmp_path):
            pytest.fail("ordinary directory admitted")
    assert not (tmp_path / ".pack-lock").exists()


def test_descriptor_does_not_infer_or_coerce(tmp_path):
    for source in ({"type": "path", "path": str(tmp_path / "x.parquet")},
                   {"type": "parquet", "version": True}, {"type": "future", "path": str(tmp_path)}):
        with pytest.raises(DataInputError):
            normalize_source(source)


@pytest.mark.parametrize("strategy_id", ["s03_reversal_v11_regime_er_b2", "s03_reversal_v16_4_a_adaptive_ma_b2",
    "s06_r_trend_v02_b2", "s06_r_trend_v02_regime_trendlines_b2", "s06_r_trend_v06_4_a2_b2"])
def test_all_v2_adapters_exact_csv_parity(tmp_path, strategy_id):
    from strategies import get_strategy, get_strategy_config
    source = publish(tmp_path / "pack", rows=3600)
    source["timeframe_minutes"] = 15
    prepared = prepare_input(source, date_filter=True, start="2024-01-03", end="2024-01-10", warmup_bars=100)
    frame = prepared.frame.copy()
    frame.insert(0, "time", frame.index.asi8 // 10**9)
    csv = load_data(io.StringIO(frame.to_csv(index=False)))
    pd.testing.assert_frame_equal(prepared.frame, csv, check_names=False, check_exact=True)
    params = {k: v.get("default") for k, v in get_strategy_config(strategy_id)["parameters"].items() if k != "warmupBars"}
    params.update(dateFilter=True, start="2024-01-03", end="2024-01-10")
    # Volume-weighted MA exercises quote-volume transport on the signal adapter.
    if strategy_id == "s03_reversal_v11_regime_er_b2":
        params.update(maType3="VWMA", maLength3=10, useRegime=False)
    if strategy_id == "s06_r_trend_v02_b2":
        params.update(entryMode="Trend @ Square", fastLength=8, slowLength=16,
                      fastSmooth=1, slowSmooth=1, stopMaxPct=50., stopX=.5,
                      stopRR=1., useTrailMA=False, thresholdOS=40, thresholdOB=40)
    adapter = get_strategy(strategy_id)
    parquet_result = adapter.run(prepared.frame, params, prepared.trade_start_idx)
    csv_result = adapter.run(csv, params, prepared.trade_start_idx)
    assert parquet_result.trades == csv_result.trades
    if strategy_id in {"s03_reversal_v11_regime_er_b2", "s06_r_trend_v02_b2"}:
        assert parquet_result.trades, "Parity must include executed position and reversal trades."
    assert json.dumps(parquet_result.to_dict(), sort_keys=True, default=str) == json.dumps(csv_result.to_dict(), sort_keys=True, default=str)


def test_metadata_inspection_reads_no_candles(tmp_path, monkeypatch):
    from core.data_input import inspect_pack
    source = publish(tmp_path / "pack")
    monkeypatch.setattr(data, "read_ohlcv_rows", lambda *a, **k: pytest.fail("metadata read candles"))
    monkeypatch.setattr(data, "verify_instrument_files", lambda *a, **k: pytest.fail("metadata verified whole files"))
    assert inspect_pack(source["pack_root"])["instruments"]


@pytest.mark.parametrize("start,end,start_origin,end_origin,warmup", [
    (None, "2024-01-02", "metadata", "user", 0),
    ("2024-01-02", None, "user", "metadata", 3),
    (None, None, "metadata", "metadata", 0),
])
def test_one_sided_bounds(tmp_path, start, end, start_origin, end_origin, warmup):
    source = publish(tmp_path / "pack")
    prepared = prepare_input(source, date_filter=True, start=start, end=end, warmup_bars=3)
    assert prepared.trade_start_idx == warmup
    assert prepared.facts["bounds"]["start_origin"] == start_origin
    assert prepared.facts["bounds"]["end_origin"] == end_origin


def test_nonaligned_opens_and_required_tail(tmp_path):
    source = publish(tmp_path / "pack", rows=288 + 139)  # Last available close: Jan 2 11:35.
    source["timeframe_minutes"] = 30
    prepared = prepare_input(source, date_filter=True, start="2024-01-02T10:10Z",
                             end="2024-01-02T11:00Z", warmup_bars=2)
    assert list(prepared.frame.index[2:]) == [pd.Timestamp("2024-01-02T10:30Z"), pd.Timestamp("2024-01-02T11:00Z")]
    with pytest.raises(DataInputError):
        prepare_input(source, date_filter=True, start="2024-01-02T11:30Z",
                      end="2024-01-02T11:30Z", warmup_bars=0)
    with pytest.raises(DataInputError, match="no target bar opens"):
        prepare_input(source, date_filter=True, start="2024-01-02T10:10Z",
                      end="2024-01-02T10:20Z", warmup_bars=0)


def test_pending_and_permission_failures_release_local_lock(tmp_path, monkeypatch):
    from pathlib import Path
    from core.data_input import inspect_pack
    source = publish(tmp_path / "pack")
    root = Path(source["pack_root"])
    marker = root / manifest.UPDATE_MARKER_NAME
    marker.write_text("{}", encoding="utf-8")
    with pytest.raises(DataInputError):
        prepare_input(source, date_filter=False)
    marker.unlink()
    with monkeypatch.context() as patch:
        def denied(*args, **kwargs):
            raise PermissionError("synthetic denied lock-file access")
        patch.setattr(data, "read_session", denied)
        with pytest.raises(DataInputError, match="denied lock-file"):
            inspect_pack(root)
    assert inspect_pack(root)["state"] == "ready"
