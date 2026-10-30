"""Saved execution reconstruction: original executions are the numerical oracle."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from core import storage
from core.export import export_trades_csv
from tests._data_input_helpers import STRATEGY, grid_payload


@pytest.fixture
def csv_source(tmp_path):
    slots = np.arange(6000)
    price = 100 + np.minimum(slots % 192, 192 - slots % 192) / 8
    frame = pd.DataFrame(dict(Open=price, High=price + 2, Low=price - 2,
                              Close=price + .5, Volume=1.),
                         index=pd.date_range("2024-01-01", periods=len(slots), freq="5min", tz="UTC"))
    path = tmp_path / "TEST_AAA, 5 synthetic.csv"
    exported = frame.copy()
    exported.insert(0, "time", frame.index.asi8 // 10**9)
    exported.to_csv(path, index=False)
    return path, frame


@pytest.mark.parametrize("date_filter", [False, True])
@pytest.mark.parametrize("ft", [False, True])
def test_csv_is_matches_original_post_split_execution(client, csv_source, monkeypatch, date_filter, ft):
    from strategies import get_strategy
    path, _ = csv_source
    config = grid_payload()
    config["worker_processes"] = 1
    config["fixed_params"].update(initialCapital=1000, dateFilter=date_filter,
                                 start="2024-01-03T00:00:00Z", end="2024-01-20T12:07:03.123456Z")
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    if ft:
        config["postProcess"] = dict(enabled=True, ftPeriodDays=3, topK=2, ftThresholdPct=-100000)
    from core import grid_engine
    original_slow = grid_engine._grid_v2_slow_result
    captured = []

    def original(**kwargs):
        result = original_slow(**{**kwargs, "retain_execution": True})
        captured.append((kwargs["df"].copy(), deepcopy(result.params),
                         kwargs["trade_start_idx"], result.portfolio_execution))
        return result

    with monkeypatch.context() as patch:
        patch.setattr(grid_engine, "_grid_v2_slow_result", original)
        response = client.post("/api/optimize", data=dict(strategy=STRATEGY, csvPath=str(path),
            config=json.dumps(config), warmupBars="100"))
    assert response.status_code == 200, response.get_data(as_text=True)
    sid = response.get_json()["study_id"]
    saved = storage.load_study_from_db(sid)
    fixed = saved["study"]["config_json"]["fixed_params"]
    assert fixed["dateFilter"]
    for trial in saved["trials"]:
        matches = [item for item in captured if item[1]["maLength3"] == trial["params"]["maLength3"]
                   and pd.Timestamp(item[1]["end"]) == pd.Timestamp(fixed["end"])]
        assert len(matches) == 1
        frame, params, idx, expected = matches[0]
        assert idx == 100 and expected.trades
        base = f"/api/studies/{sid}/trials/{trial['trial_number']}"
        trades = client.post(base + "/trades", json={})
        assert trades.status_code == 200, trades.get_data(as_text=True)
        assert trades.get_data(as_text=True) == export_trades_csv(expected.trades, symbol="TEST:AAA")
        curve = client.post(base + "/equity", json={})
        assert curve.status_code == 200, curve.get_data(as_text=True)
        body = curve.get_json()
        assert body["equity"] == list(expected.equity_curve[expected.metric_start_idx:])
        assert body["timestamps"] == [t.isoformat() for t in expected.timestamps[expected.metric_start_idx:]]
        assert body["reference_capital"] == 1000
    assert storage.load_study_from_db(sid) == saved


@pytest.mark.parametrize("delay_bars", [0, 6])
@pytest.mark.parametrize("surface", ["equity", "trades"])
@pytest.mark.parametrize("parquet", [False, True])
def test_wfa_projection_excludes_warmup(client, csv_source, tmp_path, monkeypatch, delay_bars, surface, parquet):
    from ui import server_routes_data
    from core.backtest_engine import prepare_dataset_with_warmup
    from strategies import get_strategy
    path, frame = csv_source
    pin = None
    if parquet:
        from core.data_input import prepare_input
        from tests._data_input_helpers import publish
        prepared_input = prepare_input(publish(tmp_path / "pack", rows=len(frame), triangle=True), date_filter=False)
        frame, pin = prepared_input.frame, prepared_input.facts
    start, end = frame.index[1000], frame.index[1600]
    live = frame.index[1000 + delay_bars]
    params = grid_payload()["fixed_params"]
    params.update(initialCapital=250, dateFilter=True, start=live, end=end)
    prepared, idx = prepare_dataset_with_warmup(frame, live, end, 100)
    expected = get_strategy(STRATEGY).run(prepared, params, idx)
    saved = dict(study=dict(study_id="projection", study_name="projection", optimization_mode="wfa",
        strategy_id=STRATEGY, csv_file_path=str(path), csv_file_name=path.name, warmup_bars=100,
        config_json=dict(fixed_params=params)), windows=[dict(window_number=1,
        best_params=params, oos_start_ts=start.isoformat(), oos_end_ts=end.isoformat(),
        trade_start_ts=live.isoformat(), trade_end_ts=end.isoformat(), window_status="traded",
        entry_delay_days=delay_bars / 288)])
    if pin is not None:
        saved["study"]["config_json"]["data_input"] = pin
    monkeypatch.setattr(server_routes_data, "load_study_from_db", lambda _: saved)
    if surface == "trades":
        individual = client.post("/api/studies/projection/wfa/windows/1/trades", json=dict(period="oos"))
        combined = client.post("/api/studies/projection/wfa/trades")
        assert individual.status_code == combined.status_code == 200
        assert individual.get_data(as_text=True) == export_trades_csv(expected.trades, symbol="TEST:AAA-USDT-SWAP" if parquet else "TEST:AAA")
        assert combined.get_data(as_text=True) == individual.get_data(as_text=True)
        return
    response = client.post("/api/studies/projection/wfa/windows/1/equity", json=dict(period="oos"))
    assert response.status_code == 200, response.get_data(as_text=True)
    body = response.get_json()
    timestamps = pd.to_datetime(body["timestamps"], utc=True)
    assert timestamps.is_monotonic_increasing and timestamps.is_unique
    assert timestamps[0] >= start and timestamps[-1] <= end
    real = [(ts, value) for ts, value in zip(timestamps, body["equity_curve"]) if ts >= live]
    assert real == list(zip(expected.timestamps[expected.metric_start_idx:], expected.equity_curve[expected.metric_start_idx:]))
    assert body["reference_capital"] == 250


@pytest.mark.parametrize("minutes,ft,end", [(5, False, "2024-01-20T12:07:03.123456Z"),
    (30, False, "2024-01-20"), (5, True, "2024-01-20"), (30, True, "2024-01-20T12:07:03.123456Z")])
def test_csv_oos_matches_original_execution(client, csv_source, monkeypatch, minutes, ft, end):
    from ui import server_routes_run
    from strategies import get_strategy
    path, source = csv_source
    if minutes == 30:
        source = source.resample("30min").agg(dict(Open="first", High="max", Low="min", Close="last", Volume="sum"))
        written = source.copy()
        written.insert(0, "time", source.index.asi8 // 10**9)
        written.to_csv(path, index=False)
    config = grid_payload()
    config["worker_processes"] = 1
    config["fixed_params"].update(dateFilter=True, start="2024-01-04T00:00:00Z", end=end)
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    if ft:
        config["postProcess"] = dict(enabled=True, ftPeriodDays=3, topK=2, ftThresholdPct=-100000)
    original_period = server_routes_run.run_period_test_for_trials
    raw_run = get_strategy(STRATEGY).run
    captured = []
    def period(**kwargs):
        def original(frame, params, idx):
            result = raw_run(frame, params, idx)
            captured.append((deepcopy(params), result, frame.copy(), idx))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(get_strategy(STRATEGY), "run", staticmethod(original))
            return original_period(**kwargs)
    monkeypatch.setattr(server_routes_run, "run_period_test_for_trials", period)
    response = client.post("/api/optimize", data=dict(strategy=STRATEGY, csvPath=str(path),
        config=json.dumps(config), warmupBars="100"))
    assert response.status_code == 200, response.get_data(as_text=True)
    sid = response.get_json()["study_id"]
    saved = storage.load_study_from_db(sid)
    assert len(captured) == 2
    facts = saved["study"]["config_json"]["csv_oos_replay"]
    for legacy in (False, True):
        if legacy:
            config = deepcopy(saved["study"]["config_json"])
            del config["csv_oos_replay"]
            storage.update_study_config_json(sid, config)
        before = storage.load_study_from_db(sid)
        for trial in saved["trials"]:
            params, expected, frame, idx = next(item for item in captured if item[0]["maLength3"] == trial["params"]["maLength3"])
            assert facts == dict(version=1, start=params["start"].isoformat(), end=params["end"].isoformat(),
                preparation_start=frame.index[0].isoformat(), preparation_end=frame.index[-1].isoformat(),
                warmup_bars=100, trade_start_idx=idx)
            if end == "2024-01-20":
                assert params["start"].isoformat().endswith("23:59:59.999999+00:00")
            replayed = []
            def replay(frame2, params2, idx2):
                pd.testing.assert_frame_equal(frame2, frame, check_exact=True)
                assert idx2 == idx
                assert pd.Timestamp(params2["start"]) == params["start"]
                assert pd.Timestamp(params2["end"]) == params["end"]
                actual = raw_run(frame2, params2, idx2)
                assert actual.trades == expected.trades
                replayed.append(actual)
                return actual
            base = f"/api/studies/{sid}/trials/{trial['trial_number']}"
            with monkeypatch.context() as patch:
                patch.setattr(get_strategy(STRATEGY), "run", staticmethod(replay))
                exported = client.post(base + "/oos-trades", json={})
                curve = client.post(base + "/equity", json=dict(module="oos_test"))
            assert exported.status_code == curve.status_code == 200, curve.get_data(as_text=True)
            assert len(replayed) == 2
            assert exported.get_data(as_text=True) == export_trades_csv(expected.trades, symbol="TEST:AAA")
            assert curve.get_json()["equity"] == list(expected.equity_curve[expected.metric_start_idx:])
            assert curve.get_json()["timestamps"] == [t.isoformat() for t in expected.timestamps[expected.metric_start_idx:]]
        assert storage.load_study_from_db(sid) == before


@pytest.fixture
def saved_csv(client, csv_source):
    path, _ = csv_source
    config = grid_payload()
    config["worker_processes"] = 1
    config["fixed_params"].update(dateFilter=True, start="2024-01-03T00:00:00Z", end="2024-01-20")
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    response = client.post("/api/optimize", data=dict(strategy=STRATEGY, csvPath=str(path),
        config=json.dumps(config), warmupBars="100"))
    assert response.status_code == 200, response.get_data(as_text=True)
    return response.get_json()["study_id"]


def test_actual_ft_cooldown_delayed_parquet_execution(client, tmp_path, monkeypatch):
    from tests.server.test_entry_admission import publish, grid_payload, installed, STRATEGY_ID
    from core.walkforward_engine import WalkForwardEngine
    source = publish(tmp_path / "admission-pack", rows=5000, turn_day=5)
    config = grid_payload()
    config["postProcess"] = dict(enabled=True, ftPeriodDays=2, topK=2, ftThresholdPct=0,
        ftRejectAction="cooldown_reoptimize", ftRejectCooldownDays=1,
        ftRejectMaxAttempts=3, ftRejectMinRemainingOosDays=1)
    original = WalkForwardEngine._run_period_backtest
    captured = []
    def execute(self, df, start, end, params):
        result = original(self, df, start, end, params)
        captured.append((pd.Timestamp(start), pd.Timestamp(end), result))
        return result
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", execute)
    with installed():
        response = client.post("/api/walkforward", data=dict(strategy=STRATEGY_ID, source=json.dumps(source),
            config=json.dumps(config), warmupBars="100", wf_store_top_n_trials="2", wf_adaptive_mode="true",
            wf_is_period_days="5", wf_oos_period_days="5", wf_max_oos_period_days="5",
            wf_min_oos_trades="1", wf_check_interval_trades="1"))
        assert response.status_code == 200, response.get_data(as_text=True)
        sid = response.get_json()["study_id"]
        saved = storage.load_study_from_db(sid)
        delayed = [w for w in saved["windows"] if w["window_status"] == "traded" and w["entry_delay_days"] > 0]
        assert delayed  # Delay is produced by the real FT rejection/retry policy.
        for window in delayed:
            start, end = pd.Timestamp(window["trade_start_ts"]), pd.Timestamp(window["trade_end_ts"])
            raw = next(result for a, b, result in captured if a == start and b >= end)
            response = client.post(f"/api/studies/{sid}/wfa/windows/{window['window_number']}/equity", json=dict(period="oos"))
            assert response.status_code == 200, response.get_data(as_text=True)
            body = response.get_json(); times = pd.to_datetime(body["timestamps"], utc=True)
            assert times.is_unique and times.is_monotonic_increasing
            assert times[0] == pd.Timestamp(window["oos_start_ts"])
            expected = [(t, v) for t, v in zip(raw.timestamps[raw.metric_start_idx:], raw.equity_curve[raw.metric_start_idx:]) if t < end]
            assert [(t, v) for t, v in zip(times, body["equity_curve"]) if start <= t < end] == expected
        assert storage.load_study_from_db(sid) == saved


def test_truncated_csv_refuses_stage_replay(client, saved_csv, csv_source):
    path, _ = csv_source
    saved = storage.load_study_from_db(saved_csv)
    frame = pd.read_csv(path)
    frame.iloc[:3500].to_csv(path, index=False)
    base = f"/api/studies/{saved_csv}/trials/{saved['trials'][0]['trial_number']}"
    for route, payload in [("trades", {}), ("equity", {}), ("oos-trades", {}), ("equity", {"module": "oos_test"})]:
        response = client.post(base + "/" + route, json=payload)
        assert response.status_code == 400, response.get_data(as_text=True)
        assert "cover the saved execution interval" in response.get_json()["error"]
    assert storage.load_study_from_db(saved_csv) == saved


def test_csv_facts_invalid_history_membership_and_atomic_save(client, saved_csv):
    sid = saved_csv
    saved = storage.load_study_from_db(sid)
    config = saved["study"]["config_json"]
    facts = config["csv_oos_replay"]
    trial = saved["trials"][0]["trial_number"]
    base = f"/api/studies/{sid}/trials/{trial}"
    corrupt = [None, {}, {**facts, "version": 2}, {**facts, "version": True},
               *[{**facts, key: value} for key, value in [("warmup_bars", True), ("trade_start_idx", -1),
                 ("warmup_bars", 99), ("start", "2024-01-17"), ("end", facts["preparation_start"]),
                 ("preparation_start", facts["preparation_end"]), ("trade_start_idx", 101)]]]
    for value in corrupt:
        storage.update_study_config_json(sid, {**config, "csv_oos_replay": value})
        before = storage.load_study_from_db(sid)
        for suffix, body in [("oos-trades", {}), ("equity", dict(module="oos_test"))]:
            response = client.post(base + "/" + suffix, json=body)
            assert response.status_code == 400 and "csv_oos_replay" in response.get_json()["error"]
        assert storage.load_study_from_db(sid) == before
    legacy = {key: value for key, value in config.items() if key != "csv_oos_replay"}
    for bad_end in (None, "2024-01-16", "2024-01-10T23:59:59.999999Z"):
        storage.update_study_config_json(sid, {**legacy, "fixed_params": {**legacy["fixed_params"], "end": bad_end}})
        before = storage.load_study_from_db(sid)
        assert client.post(base + "/oos-trades", json={}).status_code == 400
        assert storage.load_study_from_db(sid) == before
    storage.update_study_config_json(sid, legacy)
    with storage.get_db_connection() as conn:
        conn.execute("UPDATE studies SET oos_test_period_days=0 WHERE study_id=?", (sid,))
        conn.commit()
    assert client.post(base + "/oos-trades", json={}).status_code == 400
    storage.update_study_config_json(sid, config)
    before = storage.load_study_from_db(sid)
    kwargs = dict(oos_enabled=True, oos_period_days=3, oos_top_k=2, oos_start_date=facts["start"],
        oos_end_date=facts["end"], oos_source_module="optuna", csv_oos_replay=facts)
    good = [dict(trial_number=t["trial_number"], test_metrics=dict(total_trades=0, net_profit_pct=0)) for t in saved["trials"]]
    for bad in ([], good + [dict(trial_number=999, test_metrics={})], [good[0], good[0]]):
        with pytest.raises(RuntimeError, match="Failed to save OOS"):
            storage.save_oos_test_results(sid, bad, **kwargs)
        assert storage.load_study_from_db(sid) == before
    storage.save_oos_test_results(sid, good, **kwargs)
    assert storage.load_study_from_db(sid)["study"]["config_json"] == config
    # A recorded zero-trade OOS is valid; missing metrics and missing FT rank are not.
    assert client.post(base + "/oos-trades", json={}).status_code == 200
    with storage.get_db_connection() as conn:
        conn.execute("UPDATE studies SET ft_enabled=1 WHERE study_id=?", (sid,))
        conn.execute("UPDATE trials SET oos_test_net_profit_pct=NULL, ft_rank=NULL WHERE study_id=?", (sid,))
        conn.commit()
    before = storage.load_study_from_db(sid)
    for suffix, body in [("oos-trades", {}), ("ft-trades", {}), ("equity", dict(module="forward_test")), ("equity", dict(module="oos_test"))]:
        response = client.post(base + "/" + suffix, json=body)
        assert response.status_code == 400 and "unavailable" in response.get_json()["error"]
    assert storage.load_study_from_db(sid) == before


@pytest.mark.parametrize("bad", ["duplicate", "descending", "nan", "unequal"])
def test_invalid_real_projection_is_not_sorted_or_deduplicated(bad):
    from core.backtest_engine import StrategyResult
    from ui.server_services import _project_execution_equity
    ts = list(pd.date_range("2024-01-01", periods=3, tz="UTC"))
    values = [1000., 990., 1010.]
    if bad == "duplicate": ts[2] = ts[1]
    if bad == "descending": ts[1], ts[2] = ts[2], ts[1]
    if bad == "nan": values[1] = float("nan")
    if bad == "unequal": values.pop()
    raw = StrategyResult(trades=[], timestamps=ts, equity_curve=values, balance_curve=[], metric_initial_equity=1000)
    with pytest.raises(ValueError):
        _project_execution_equity(raw, dict(dateFilter=False), 1000, scheduled_start=ts[0] - pd.Timedelta(days=1))


@pytest.mark.parametrize("values", [[990.], [1010.], [990., 1010.]])
def test_projection_keeps_first_live_pnl_and_single_point(values):
    from core.backtest_engine import StrategyResult
    from ui.server_services import _project_execution_equity
    ts = list(pd.date_range("2024-01-02", periods=len(values), freq="h", tz="UTC"))
    raw = StrategyResult(trades=[], timestamps=ts, equity_curve=values, balance_curve=[], metric_initial_equity=980)
    curve, stamps = _project_execution_equity(raw, dict(dateFilter=True, start=ts[0], end=ts[-1]), 1000,
                                            scheduled_start=ts[0] - pd.Timedelta(days=1))
    assert curve == [1000.] + values
    assert stamps == [(ts[0] - pd.Timedelta(days=1)).isoformat()] + [t.isoformat() for t in ts]


def test_wfa_precise_bounds_legacy_and_offline_no_trade(client, monkeypatch):
    from ui import server_routes_data
    window = dict(window_number=1, best_params={}, oos_start_ts="2024-01-01T12:00:00Z",
                  oos_end_ts="2024-01-04T12:00:00Z", window_status="no_trade")
    saved = dict(study=dict(study_id="bounds", strategy_id="s01_trailing_ma", optimization_mode="wfa",
        csv_file_path="missing.csv", csv_file_name="TEST_AAA.csv", config_json={}), windows=[window])
    monkeypatch.setattr(server_routes_data, "load_study_from_db", lambda _: saved)
    def call():
        return [client.post("/api/studies/bounds/wfa/" + path, json=dict(period="oos"))
                for path in ("windows/1/equity", "windows/1/trades", "trades")]
    responses = call()
    assert [r.status_code for r in responses] == [200, 200, 200]
    assert responses[0].get_json()["equity_curve"] == [100, 100]
    assert responses[2].get_data(as_text=True) == "Symbol,Side,Qty,Fill Price,Closing Time\n"
    from ui.server_services import _resolve_wfa_oos_bounds
    base = {k: v for k, v in window.items() if k != "window_status"}
    # Added NULL SQL columns are absent facts; a populated zero is a modern fact.
    assert _resolve_wfa_oos_bounds({**base, "entry_delay_days": None})[0:2] == _resolve_wfa_oos_bounds(base)[2:4]
    invalid = [dict(entry_delay_days=0), dict(window_status="traded"), dict(trade_start_date="2024-01-01"),
        dict(trade_start_ts="2024-01-02T00:00:00Z"), dict(trade_end_ts="2024-01-03T00:00:00Z"),
        dict(trade_start_ts="2024-01-01", trade_end_ts="2024-01-03T00:00:00Z"),
        dict(trade_start_ts="2023-12-31T00:00:00Z", trade_end_ts="2024-01-03T00:00:00Z"),
        dict(trade_start_ts="2024-01-02T00:00:00Z", trade_end_ts="2024-01-05T00:00:00Z"),
        dict(oos_start_ts="2024-01-01"), dict(oos_start_ts=None, oos_start_date="2024-01-01")]
    for fields in invalid:
        saved["windows"] = [{**base, **fields}]
        before = deepcopy(saved)
        responses = call()
        assert [r.status_code for r in responses] == [400, 400, 400]
        assert "window 1:" in responses[2].get_json()["error"]
        assert saved == before


@pytest.mark.parametrize("date_filter", [False, None, True])
@pytest.mark.parametrize("strategy", ["s03_reversal_v10", STRATEGY])
def test_historical_optuna_stage_and_full_source(client, csv_source, monkeypatch, date_filter, strategy):
    from core.backtest_engine import prepare_dataset_with_warmup
    from strategies import get_strategy, get_strategy_config
    from ui import server_data_input, server_routes_data
    path, frame = csv_source
    params = {k: v["default"] for k, v in get_strategy_config(strategy)["parameters"].items() if "default" in v}
    params.update(maLength3=8, initialCapital=1000, dateFilter=bool(date_filter))
    if date_filter:
        params.update(start=frame.index[500].isoformat(), end=frame.index[1700].isoformat())
        original_frame, idx = prepare_dataset_with_warmup(frame, frame.index[500], frame.index[1700], 100)
    else:
        original_frame, idx = frame, 0
        if date_filter is None: params.pop("dateFilter")
    original = get_strategy(strategy).run(original_frame, params, idx)
    trial = dict(trial_number=1, params={"maLength3": 8})
    study = dict(study_id="v1", strategy_id=strategy, optimization_mode="optuna", warmup_bars=100,
                 csv_file_path=str(path), csv_file_name=path.name, config_json=dict(fixed_params=params))
    for target in (storage, server_routes_data):
        monkeypatch.setattr(target, "load_study_from_db", lambda _: dict(study=study))
        monkeypatch.setattr(target, "get_study_trial", lambda *args: trial)
    response = client.post("/api/studies/v1/trials/1/trades", json={})
    assert response.status_code == 200, response.get_data(as_text=True)
    assert response.get_data(as_text=True) == export_trades_csv(original.trades, symbol="TEST:AAA")
    curve = client.post("/api/studies/v1/trials/1/equity", json={})
    assert curve.status_code == 200, curve.get_data(as_text=True)
    assert curve.get_json()["equity"] == list(original.equity_curve[original.metric_start_idx:])


def test_csv_ft_uses_original_date_only_worker_invocation(client, csv_source, monkeypatch):
    from ui import server_routes_run
    from core import post_process
    from strategies import get_strategy
    from types import SimpleNamespace
    path, _ = csv_source
    config = grid_payload(); config["worker_processes"] = 1
    config["fixed_params"].update(initialCapital=1000, dateFilter=True,
        start="2024-01-03T00:00:00Z", end="2024-01-20T12:07:03.123456Z")
    config["postProcess"] = dict(enabled=True, ftPeriodDays=3, topK=2, ftThresholdPct=-100000)
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    real_ft = server_routes_run.run_forward_test
    raw_run = get_strategy(STRATEGY).run
    captured = []
    class InlinePool:
        # Preserve the actual worker/preparation; only process transport is local
        # so this test can observe its original strategy result directly.
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def starmap(self, worker, arguments): return [worker(*args) for args in arguments]
    def ft(**kwargs):
        assert kwargs["ft_start_date"] == "2024-01-14"
        assert kwargs["ft_end_date"] == "2024-01-17"
        def original(frame, params, idx):
            result = raw_run(frame, params, idx)
            captured.append((frame.copy(), deepcopy(params), idx, result))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(post_process, "mp", SimpleNamespace(get_context=lambda _: SimpleNamespace(Pool=InlinePool)))
            patch.setattr(get_strategy(STRATEGY), "run", staticmethod(original))
            return real_ft(**kwargs)
    monkeypatch.setattr(server_routes_run, "run_forward_test", ft)
    response = client.post("/api/optimize", data=dict(strategy=STRATEGY, csvPath=str(path),
        config=json.dumps(config), warmupBars="100"))
    assert response.status_code == 200, response.get_data(as_text=True)
    sid = response.get_json()["study_id"]
    saved = storage.load_study_from_db(sid)
    assert len(captured) == 2
    for trial in saved["trials"]:
        frame, params, idx, raw = next(item for item in captured if item[1]["maLength3"] == trial["params"]["maLength3"])
        assert params["start"].hour == 0 and params["end"].hour == 23
        base = f"/api/studies/{sid}/trials/{trial['trial_number']}"
        trades = client.post(base + "/ft-trades", json={})
        curve = client.post(base + "/equity", json=dict(module="forward_test"))
        assert trades.status_code == curve.status_code == 200, curve.get_data(as_text=True)
        assert trades.get_data(as_text=True) == export_trades_csv(raw.trades, symbol="TEST:AAA")
        assert curve.get_json()["equity"] == list(raw.equity_curve[raw.metric_start_idx:])
        assert curve.get_json()["timestamps"] == [t.isoformat() for t in raw.timestamps[raw.metric_start_idx:]]
    assert storage.load_study_from_db(sid) == saved


@pytest.mark.parametrize("calendar,adaptive", [(False, False), (True, False), (False, True)])
def test_real_wfa_delayed_composition_and_no_trade(client, tmp_path, monkeypatch, calendar, adaptive):
    from core.walkforward_engine import WalkForwardEngine, TriggerResult
    from dataclasses import replace
    slots = np.arange(3100 if calendar else 1500)
    price = 100 + np.minimum(slots % 80, 80 - slots % 80) / 8
    frame = pd.DataFrame(dict(Open=price, High=price + 2, Low=price - 2, Close=price + .5, Volume=1.),
                         index=pd.date_range("2024-01-01", periods=len(slots), freq="h" if calendar else "30min", tz="UTC"))
    path = tmp_path / "TEST_AAA, 60 synthetic.csv"
    written = frame.copy(); written.insert(0, "time", frame.index.asi8 // 10**9); written.to_csv(path, index=False)
    config = grid_payload(); config["worker_processes"] = 1
    config["fixed_params"].update(initialCapital=250, dateFilter=True,
        start="2024-01-06T00:00:00Z", end="2024-05-06T00:00:00Z" if calendar else "2024-01-30T00:00:00Z")
    original_plan = WalkForwardEngine._resolve_window_execution_plan
    original_period = WalkForwardEngine._run_period_backtest
    captured = []
    def plan(self, **kwargs):
        actual = original_plan(self, **kwargs)
        if kwargs["window_id"] == 2:
            return replace(actual, window_status="no_trade", trade_start=None, no_trade_reason="synthetic controlled rejection")
        return replace(actual, trade_start=actual.trade_start + pd.Timedelta(hours=6), entry_delay_days=.25)
    def execute(self, df, start, end, params):
        result = original_period(self, df, start, end, params)
        captured.append((start, end, deepcopy(params), result))
        return result
    def trigger(self, **kwargs):
        # A deterministic trigger during an open trade exercises the established
        # truncate-original versus terminal-forced-close replay distinction.
        minimum = kwargs["oos_start"] + pd.Timedelta(days=2)
        trades = sorted(kwargs["trades"], key=lambda t: t.exit_time)
        crossing = next((t for t in trades if t.exit_time > minimum and t.exit_time > t.entry_time + pd.Timedelta(minutes=30)), None)
        cutoff = crossing.exit_time - pd.Timedelta(minutes=30) if crossing else kwargs["oos_max_end"]
        count = sum(t.exit_time <= cutoff for t in trades)
        return TriggerResult(True, "cusum", count-1, cutoff, 6., 5., 0., 0., count,
                             (cutoff - kwargs["oos_start"]).total_seconds()/86400)
    with monkeypatch.context() as patch:
        patch.setattr(WalkForwardEngine, "_resolve_window_execution_plan", plan)
        patch.setattr(WalkForwardEngine, "_run_period_backtest", execute)
        if adaptive:
            patch.setattr(WalkForwardEngine, "_scan_triggers", trigger)
        fields = dict(strategy=STRATEGY, csvPath=str(path), config=json.dumps(config), warmupBars="100", wf_store_top_n_trials="2")
        fields.update(dict(wf_period_unit="months", wf_is_period_months="1", wf_oos_period_months="1") if calendar else
                      dict(wf_is_period_days="6", wf_oos_period_days="4"))
        if adaptive:
            fields.update(wf_adaptive_mode="true", wf_max_oos_period_days="4", wf_min_oos_trades="1",
                          wf_check_interval_trades="1", wf_cooldown_days="0")
        response = client.post("/api/walkforward", data=fields)
    assert response.status_code == 200, response.get_data(as_text=True)
    sid = response.get_json()["study_id"]
    saved = storage.load_study_from_db(sid)
    assert len(saved["windows"]) >= 2
    expected_trades = []
    traded = 0
    for window in saved["windows"]:
        base = f"/api/studies/{sid}/wfa/windows/{window['window_number']}"
        curve = client.post(base + "/equity", json=dict(period="oos"))
        trades = client.post(base + "/trades", json=dict(period="oos"))
        assert curve.status_code == trades.status_code == 200, curve.get_data(as_text=True)
        if window["window_status"] == "no_trade":
            assert curve.get_json()["equity_curve"] == [250, 250]
            continue
        traded += 1
        start, end = pd.Timestamp(window["trade_start_ts"]), pd.Timestamp(window["trade_end_ts"])
        _, _, original_params, raw = next(item for item in captured if item[0] == start and item[1] >= end)
        assert raw.trades
        expected = raw
        if adaptive:
            from core.backtest_engine import prepare_dataset_with_warmup
            from strategies import get_strategy
            prepared, idx = prepare_dataset_with_warmup(frame, start, end, 100)
            expected = get_strategy(STRATEGY).run(prepared, {**original_params, "dateFilter": True, "start": start, "end": end}, idx)
            individual = sorted(expected.trades, key=lambda t: (t.exit_time, t.entry_time))[:window["oos_total_trades"]]
        else:
            individual = expected.trades
        expected_trades.extend(individual)
        assert trades.get_data(as_text=True) == export_trades_csv(individual, symbol="TEST:AAA")
        body = curve.get_json(); times = pd.to_datetime(body["timestamps"], utc=True)
        assert times.is_unique and times.is_monotonic_increasing
        assert times[0] == pd.Timestamp(window["oos_start_ts"])
        assert body["reference_capital"] == 250
        real = [(t, value) for t, value in zip(times, body["equity_curve"]) if t >= start and (not adaptive or t < end)]
        original = [(t, value) for t, value in zip(raw.timestamps[raw.metric_start_idx:], raw.equity_curve[raw.metric_start_idx:]) if not adaptive or t < end]
        assert real == original
        assert body["equity_curve"][-1] == expected.equity_curve[-1]
    assert traded
    if not adaptive:
        expected_trades.sort(key=lambda trade: trade.entry_time)
    combined = client.post(f"/api/studies/{sid}/wfa/trades")
    assert combined.status_code == 200
    assert combined.get_data(as_text=True) == export_trades_csv(expected_trades, symbol="TEST:AAA")
    assert storage.load_study_from_db(sid) == saved
    # One invalid required window fails the entire download, even after earlier windows ran.
    with storage.get_db_connection() as conn:
        conn.execute("UPDATE wfa_windows SET window_status='traded',trade_start_ts=NULL,trade_end_ts=NULL WHERE study_id=? AND window_number=2", (sid,))
        conn.commit()
    failed = client.post(f"/api/studies/{sid}/wfa/trades")
    assert failed.status_code == 400 and "window 2:" in failed.get_json()["error"]
