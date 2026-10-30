"""A2 corrections: exact original OOS replay and preserved route boundaries."""
from copy import deepcopy
import csv
import io
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from core import storage
from core.data_input import prepare_input
from core.export import export_trades_csv
from tests._data_input_helpers import STRATEGY, grid_payload, publish


def optimize(client, source, config, warmup=100):
    response = client.post("/api/optimize", data={"strategy": STRATEGY, "source": json.dumps(source),
        "config": json.dumps(config), "warmupBars": str(warmup)})
    assert response.status_code == 200, response.get_data(as_text=True)
    return response.get_json()["study_id"]


@pytest.mark.parametrize("minutes", [5, 15])
@pytest.mark.parametrize("end", ["2024-01-20T00:00:00Z", "2024-01-20T12:07:03.123456Z", None])
@pytest.mark.parametrize("capital", [100, 1000])
def test_oos_replays_actual_original_execution(client, tmp_path, monkeypatch, minutes, end, capital):
    from ui import server_routes_run
    from strategies import get_strategy
    from data_provider import data

    source = publish(tmp_path / "pack", rows=6000, triangle=True)
    source["timeframe_minutes"] = minutes
    config = grid_payload()
    config["fixed_params"]["initialCapital"] = capital
    if end:
        config["fixed_params"].update(dateFilter=True, start="2024-01-04T00:07:00.123456Z", end=end)
    config["oosTest"] = {"enabled": True, "periodDays": 3, "topK": 2}
    config["postProcess"] = {"enabled": True, "ftPeriodDays": 3, "topK": 2, "ftThresholdPct": -100000}
    original_period = server_routes_run.run_period_test_for_trials
    original_run = get_strategy(STRATEGY).run
    originals = []
    reads = []
    original_read = data.read_ohlcv_rows

    def read(*args, **kwargs):
        reads.append(kwargs)
        return original_read(*args, **kwargs)

    def period(**kwargs):
        def run(frame, params, idx):
            result = original_run(frame, params, idx)
            originals.append((frame.copy(), deepcopy(params), idx, result))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(get_strategy(STRATEGY), "run", staticmethod(run))
            return original_period(**kwargs)

    monkeypatch.setattr(data, "read_ohlcv_rows", read)
    monkeypatch.setattr(server_routes_run, "run_period_test_for_trials", period)
    study_id = optimize(client, source, config)
    assert len(reads) == 1  # Capturing stage facts must not reread the pack.
    saved = storage.load_study_from_db(study_id)
    pin = deepcopy(saved["study"]["config_json"]["data_input"])
    record = saved["study"]["config_json"]["parquet_oos_replay"]
    assert len(originals) == 2
    if end is None:
        assert pin["bounds"]["start_origin"] == "metadata"
        assert pin["bounds"]["trade_start_idx"] == 0
    for trial in saved["trials"]:
        frame, params, idx, result = next(item for item in originals
                                        if item[1]["maLength3"] == trial["params"]["maLength3"])
        assert record == dict(version=1, start=pd.Timestamp(params["start"]).isoformat(),
            end=pd.Timestamp(params["end"]).isoformat(), preparation_start=frame.index[0].isoformat(),
            preparation_end=frame.index[-1].isoformat(), trade_start_idx=idx, warmup_bars=100)
        base = f"/api/studies/{study_id}/trials/{trial['trial_number']}"
        replayed = []

        def replay(frame2, params2, idx2):
            pd.testing.assert_frame_equal(frame, frame2, check_exact=True)
            assert idx2 == idx
            assert pd.Timestamp(params2["start"]) == pd.Timestamp(params["start"])
            assert pd.Timestamp(params2["end"]) == pd.Timestamp(params["end"])
            actual = original_run(frame2, params2, idx2)
            assert actual.trades == result.trades
            replayed.append(actual)
            return actual

        with monkeypatch.context() as patch:
            patch.setattr(get_strategy(STRATEGY), "run", staticmethod(replay))
            curve = client.post(base + "/equity", json={"module": "oos_test"})
            trades = client.post(base + "/oos-trades", json={})
        assert curve.status_code == trades.status_code == 200, curve.get_data(as_text=True)
        assert len(replayed) == 2
        equity = list((result.equity_curve or result.balance_curve)[result.metric_start_idx:])
        assert curve.get_json() == dict(equity=equity,
            timestamps=[t.isoformat() for t in result.timestamps[result.metric_start_idx:]], reference_capital=capital)
        assert trades.get_data(as_text=True) == export_trades_csv(result.trades, symbol="TEST:AAA-USDT-SWAP")
        assert len(result.trades) == trial["oos_test_total_trades"]
        capital = float(params["initialCapital"])
        assert (result.balance_curve[-1] / capital - 1) * 100 == pytest.approx(trial["oos_test_net_profit_pct"], abs=1e-9)
        for module, prefix in [("optuna", ""), ("forward_test", "ft_")]:
            preserved = client.post(base + "/equity", json={"module": module})
            assert preserved.status_code == 200, preserved.get_data(as_text=True)
            ending = preserved.get_json()["equity"][-1]
            assert (ending / capital - 1) * 100 == pytest.approx(trial[prefix + "net_profit_pct"], abs=1e-9)
    assert storage.load_study_from_db(study_id) == saved


@pytest.fixture
def oos_study(client, tmp_path):
    source = publish(tmp_path / "pack", rows=6000, triangle=True)
    config = grid_payload()
    config["oosTest"] = {"enabled": True, "periodDays": 3, "topK": 2}
    return optimize(client, source, config)


def test_invalid_oos_facts_and_old_study_are_read_only(client, oos_study, monkeypatch):
    from ui import server_data_input
    saved = storage.load_study_from_db(oos_study)
    config = saved["study"]["config_json"]
    record = config["parquet_oos_replay"]
    bad = [None, {}, {**record, "version": True}, {**record, "version": 2},
           *[{**record, key: value} for key, value in [
               ("start", "2024-01-18"), ("end", "2024-01-20T00:00:00+01:00"),
               ("start", 123), ("start", record["end"] + "x"), ("end", record["preparation_start"]),
               ("trade_start_idx", True), ("warmup_bars", -1), ("warmup_bars", 1.5),
               ("start", "2023-01-01T00:00:00Z"), ("preparation_end", "2030-01-01T00:00:00Z")]]]
    trial = saved["trials"][0]["trial_number"]
    base = f"/api/studies/{oos_study}/trials/{trial}"
    with monkeypatch.context() as patch:
        patch.setattr(server_data_input, "replay_config", lambda *a, **k: pytest.fail("invalid facts must precede IO"))
        for value in bad:
            storage.update_study_config_json(oos_study, {**config, "parquet_oos_replay": value})
            before = storage.load_study_from_db(oos_study)
            for path, body in [("equity", {"module": "oos_test"}), ("oos-trades", {})]:
                response = client.post(base + "/" + path, json=body)
                assert response.status_code == 400
                assert "parquet_oos_replay" in response.get_json()["error"]
            assert storage.load_study_from_db(oos_study) == before
    for key, value in [("trade_start_idx", record["trade_start_idx"] + 1),
                       ("preparation_start", record["preparation_end"])]:
        storage.update_study_config_json(oos_study, {**config, "parquet_oos_replay": {**record, key: value}})
        assert "conflicts" in client.post(base + "/oos-trades", json={}).get_json()["error"]
    old = {k: v for k, v in config.items() if k != "parquet_oos_replay"}
    storage.update_study_config_json(oos_study, old)
    assert client.get(f"/api/studies/{oos_study}").status_code == 200
    assert client.post(base + "/equity", json={"module": "optuna"}).status_code == 200
    for path, body in [("equity", {"module": "oos_test"}), ("oos-trades", {})]:
        response = client.post(base + "/" + path, json=body)
        assert response.status_code == 400
        assert "predates exact OOS replay metadata" in response.get_json()["error"]
    assert storage.load_study_from_db(oos_study)["trials"] == saved["trials"]
    storage.update_study_config_json(oos_study, config)
    with storage.get_db_connection() as conn:
        conn.execute("UPDATE trials SET oos_test_total_trades=NULL WHERE study_id=?", (oos_study,))
        conn.commit()
    with monkeypatch.context() as patch:
        patch.setattr(server_data_input, "replay_config", lambda *a, **k: pytest.fail("absent result must precede IO"))
        for path, body in [("equity", {"module": "oos_test"}), ("oos-trades", {})]:
            response = client.post(base + "/" + path, json=body)
            assert response.status_code == 400 and "unavailable" in response.get_json()["error"]


def test_oos_save_is_atomic_and_merges_config(oos_study):
    saved = storage.load_study_from_db(oos_study)
    config = saved["study"]["config_json"]
    config.update(unrelated={"preserve": True}, data_input_root_override=config["data_input"]["source"]["pack_root"])
    storage.update_study_config_json(oos_study, config)
    before = storage.load_study_from_db(oos_study)
    results = [{"trial_number": t["trial_number"], "test_metrics": {"total_trades": 0, "net_profit_pct": 0}}
               for t in saved["trials"]]
    kwargs = dict(oos_enabled=True, oos_period_days=3, oos_top_k=2, oos_start_date="2024-01-18",
                  oos_end_date="2024-01-21", oos_source_module="optuna", parquet_oos_replay=config["parquet_oos_replay"])
    for invalid in [[], results + [{"trial_number": 999, "test_metrics": {"total_trades": 0}}],
                    [results[0], results[0]], [{"trial_number": results[0]["trial_number"]}]]:
        with pytest.raises(RuntimeError, match="Failed to save OOS"):
            storage.save_oos_test_results(oos_study, invalid, **kwargs)
        assert storage.load_study_from_db(oos_study) == before
    storage.save_oos_test_results(oos_study, results, **kwargs)
    after = storage.load_study_from_db(oos_study)
    assert after["study"]["config_json"] == config
    assert all(t["oos_test_total_trades"] == 0 for t in after["trials"])


@pytest.mark.parametrize("zero_trades", [False, True])
def test_non100_csv_parquet_oos_manual_and_historical_metrics(client, tmp_path, monkeypatch, zero_trades):
    from strategies import get_strategy
    from ui import server_routes_run, server_routes_data
    from core.metrics import calculate_basic, calculate_advanced
    from core.post_process import calculate_comparison_metrics
    source = publish(tmp_path / "pack", rows=4000, triangle=True)
    frame = prepare_input(source, date_filter=False).frame
    csv_frame = frame.copy()
    csv_frame.insert(0, "time", frame.index.asi8 // 10**9)
    path = tmp_path / "TEST_AAA, 5 synthetic.csv"
    csv_frame.to_csv(path, index=False)
    config = grid_payload()
    config["fixed_params"].update(initialCapital=1000, dateFilter=True,
        start="2024-01-03T00:00:00Z", end="2024-01-14T00:00:00Z")
    if zero_trades:
        config["fixed_params"].update(closeCountLong=100000, closeCountShort=100000)
    config["oosTest"] = dict(enabled=True, periodDays=3, topK=2)
    raw_run = get_strategy(STRATEGY).run
    period_original = server_routes_run.run_period_test_for_trials
    originals = []
    def capture_period(**kwargs):
        def run(frame, params, idx):
            result = raw_run(frame, params, idx)
            originals.append((frame.copy(), deepcopy(params), idx, result))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(get_strategy(STRATEGY), "run", staticmethod(run))
            return period_original(**kwargs)
    monkeypatch.setattr(server_routes_run, "run_period_test_for_trials", capture_period)
    economic = []
    for kind in ("csv", "parquet"):
        originals.clear()
        fields = dict(strategy=STRATEGY, warmupBars="100", config=json.dumps(config))
        fields.update({"source": json.dumps(source)} if kind == "parquet" else {"csvPath": str(path)})
        response = client.post("/api/optimize", data=fields)
        assert response.status_code == 200, response.get_data(as_text=True)
        study_id = response.get_json()["study_id"]
        saved = storage.load_study_from_db(study_id)
        assert len(originals) == 2
        for trial in saved["trials"]:
            _, _, _, raw = next(item for item in originals if item[1]["maLength3"] == trial["params"]["maLength3"])
            assert bool(raw.trades) is not zero_trades
            assert trial["oos_test_net_profit_pct"] == pytest.approx((raw.balance_curve[-1] / 1000 - 1) * 100)
            assert trial["oos_test_romad"] == calculate_advanced(raw, 1000).romad
        economic.append(sorted((t["params"]["maLength3"], t["oos_test_net_profit_pct"], t["oos_test_romad"],
            t["oos_test_total_trades"]) for t in saved["trials"]))
        # A task-owned historical row deliberately retains the old inflated FT baseline.
        with storage.get_db_connection() as conn:
            conn.execute("UPDATE studies SET ft_enabled=1,ft_period_days=3 WHERE study_id=?", (study_id,))
            conn.execute("UPDATE trials SET ft_net_profit_pct=900,ft_romad=450,ft_max_drawdown_pct=2,ft_rank=trial_number WHERE study_id=?", (study_id,))
            conn.commit()
        historical = storage.load_study_from_db(study_id)
        base = f"/api/studies/{study_id}"
        assert client.get(base).status_code == 200
        number = historical["trials"][0]["trial_number"]
        assert client.post(base + f"/trials/{number}/trades", json={}).status_code == 200
        assert storage.load_study_from_db(study_id) == historical
        for tab in ("optuna", "forward_test"):
            manual_raw = []
            def manual_run(frame, params, idx):
                raw = raw_run(frame, params, idx)
                manual_raw.append((deepcopy(params), raw))
                return raw
            with monkeypatch.context() as patch:
                patch.setattr(get_strategy(STRATEGY), "run", staticmethod(manual_run))
                response = client.post(base + "/test", json=dict(dataSource="original_" + kind,
                    startDate="2024-01-05", endDate="2024-01-08", trialNumbers=[t["trial_number"] for t in historical["trials"]], sourceTab=tab))
            assert response.status_code == 200, response.get_data(as_text=True)
            payload = response.get_json()
            manual = storage.load_manual_test_results(study_id, payload["test_id"])
            results = manual["results_json"]["results"]
            assert len(results) == len(manual_raw) == 2
            for item, (params, raw) in zip(results, manual_raw):
                assert params["initialCapital"] == 1000
                assert item["test_metrics"]["net_profit_pct"] == calculate_basic(raw, 1000).net_profit_pct
                assert item["test_metrics"]["romad"] == calculate_advanced(raw, 1000).romad
                trial = next(t for t in historical["trials"] if t["trial_number"] == item["trial_number"])
                assert item["original_metrics"]["net_profit_pct"] == (900 if tab == "forward_test" else trial["net_profit_pct"])
                from core.post_process import calculate_is_period_days
                days = 3 if tab == "forward_test" else (historical["study"]["is_period_days"] or calculate_is_period_days(historical["study"]["config_json"]) or 0)
                expected = calculate_comparison_metrics(item["original_metrics"], item["test_metrics"], days, 3)
                assert item["comparison"]["profit_degradation"] == expected["profit_degradation"]
            values = [item["comparison"]["profit_degradation"] for item in results]
            assert manual["best_profit_degradation"] == payload["summary"]["best_profit_degradation"] == max(values)
            assert manual["worst_profit_degradation"] == payload["summary"]["worst_profit_degradation"] == min(values)
            assert storage.load_study_from_db(study_id)["trials"] == historical["trials"]
        # Stored runtime resolution supplies an invalid value to the real manual caller.
        original_context = server_routes_data._resolve_stored_execution_context
        context = original_context(historical["study"])
        monkeypatch.setattr(server_routes_data, "_resolve_stored_execution_context", lambda _: SimpleNamespace(
            base_params=context.base_params, warmup_bars=context.warmup_bars,
            params_for=lambda *a, **k: {**context.params_for(*a, **k), "initialCapital": 0}))
        response = client.post(base + "/test", json=dict(dataSource="original_" + kind,
            startDate="2024-01-05", endDate="2024-01-08", trialNumbers=[number], sourceTab="optuna"))
        assert response.status_code == 400
        monkeypatch.setattr(server_routes_data, "_resolve_stored_execution_context", original_context)
    assert economic[0] == economic[1]


def test_relocation_request_validation_without_io_or_writes(client, oos_study, monkeypatch):
    from ui import server_data_input
    before = storage.load_study_from_db(oos_study)
    monkeypatch.setattr(server_data_input, "resolve_root", lambda *a, **k: pytest.fail("invalid request opened pack"))
    url = f"/api/studies/{oos_study}/locate-parquet-pack"
    for body in [[], [1], "text", True, 1, None, *[{"testId": v} for v in
                 [True, False, 0, -1, 1.5, "", " ", "1.5", "-1", "1x", {}, []]]]:
        response = client.post(url, data=json.dumps(body), content_type="application/json")
        assert response.status_code == 400, (body, response.get_data(as_text=True))
    for test_id in [999999, "999999", "999999999999999999999999999999999"]:
        assert client.post(url, json={"testId": test_id}).status_code == 404
    for body in [{}, *[{"packRoot": value} for value in [None, "", "  ", 1, True, False, [], {}]]]:
        response = client.post(url, json=body)
        assert response.status_code == 400
        assert "packRoot must be a nonblank path string" in response.get_json()["error"]
        assert client.post("/api/studies/missing/locate-parquet-pack", json=body).status_code == 404
    # An ID belonging to another study must never be relocated through this one.
    with storage.get_db_connection() as conn:
        conn.execute("INSERT INTO studies(study_id,study_name,strategy_id,optimization_mode) VALUES (?,?,?,?)",
                     ("another-study", "other-study-for-relocation", STRATEGY, "grid"))
        conn.commit()
    other_id = storage.save_manual_test_to_db(study_id="another-study", test_name=None,
        data_source="original_parquet", csv_path=None, start_date="2024-01-01", end_date="2024-01-02",
        source_tab="optuna", trials_count=1, trials_tested_csv="1", best_profit_degradation=None,
        worst_profit_degradation=None, results_json={"config": before["study"]["config_json"]})
    assert client.post(url, json={"testId": str(other_id)}).status_code == 404
    assert storage.load_study_from_db(oos_study) == before
    config = before["study"]["config_json"]
    storage.update_study_config_json(oos_study, {k: v for k, v in config.items() if k != "data_input"})
    no_pin = storage.load_study_from_db(oos_study)
    response = client.post(url, json={"packRoot": []})
    assert response.status_code == 400
    assert response.get_json()["error"] == "This result has no Parquet input pin."
    assert storage.load_study_from_db(oos_study) == no_pin


def test_valid_relocation_and_missing_or_changed_inputs(client, oos_study, tmp_path, monkeypatch):
    from strategies import get_strategy
    saved = storage.load_study_from_db(oos_study)
    config = saved["study"]["config_json"]
    trial = saved["trials"][0]["trial_number"]
    base = f"/api/studies/{oos_study}"
    original = client.post(base + f"/trials/{trial}/equity", json={"module": "oos_test"})
    assert original.status_code == 200
    original_run = get_strategy(STRATEGY).run
    originals = []

    def capture(frame, params, idx):
        result = original_run(frame, params, idx)
        originals.append(result)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(get_strategy(STRATEGY), "run", staticmethod(capture))
        manual = client.post(base + "/test", json={"dataSource": "original_parquet", "startDate": "2024-01-03",
            "endDate": "2024-01-06", "trialNumbers": [trial], "sourceTab": "optuna"})
    assert manual.status_code == 200, manual.get_data(as_text=True)
    test_id = manual.get_json()["test_id"]
    assert len(originals) == 1
    manual_curve = client.post(base + f"/trials/{trial}/equity", json={"module": "manual_tests", "testId": test_id})
    assert manual_curve.status_code == 200
    assert manual_curve.get_json()["equity"] == list((originals[0].equity_curve or originals[0].balance_curve)[originals[0].metric_start_idx:])
    manual_trades = client.post(base + f"/tests/{test_id}/trials/{trial}/mt-trades", json={})
    assert manual_trades.get_data(as_text=True) == export_trades_csv(originals[0].trades, symbol="TEST:AAA-USDT-SWAP")
    manual_before = storage.load_manual_test_results(oos_study, test_id)
    replacement = publish(tmp_path / "replacement", rows=6300, triangle=True)
    for test in [None, str(test_id)]:
        response = client.post(base + "/locate-parquet-pack", json={"packRoot": replacement["pack_root"], "testId": test})
        assert response.status_code == 200, response.get_data(as_text=True)
    updated = storage.load_study_from_db(oos_study)
    assert updated["study"]["config_json"] == {**config, "data_input_root_override": replacement["pack_root"]}
    assert updated["trials"] == saved["trials"]
    assert storage.load_manual_test_results(oos_study, test_id)["results_json"]["config"] == {
        **manual_before["results_json"]["config"], "data_input_root_override": replacement["pack_root"]}
    assert client.post(base + f"/trials/{trial}/equity", json={"module": "oos_test"}).get_json() == original.get_json()
    changed = publish(tmp_path / "changed", rows=6300, triangle=True, change=300)
    for root in [str(tmp_path / "missing"), changed["pack_root"]]:
        storage.update_study_config_json(oos_study, {**config, "data_input_root_override": root})
        for endpoint, body in [("equity", {"module": "oos_test"}), ("oos-trades", {})]:
            assert client.post(base + f"/trials/{trial}/" + endpoint, json=body).status_code == 400
        assert storage.load_study_from_db(oos_study)["trials"] == saved["trials"]


@pytest.mark.parametrize("kind", ["csv", "parquet"])
@pytest.mark.parametrize("adaptive", [False, True])
def test_wfa_symbols_and_offline_no_trade_order(client, tmp_path, monkeypatch, kind, adaptive):
    from ui import server_routes_data, server_data_input
    from core.walkforward_engine import WalkForwardEngine
    from core import metrics
    from strategies import get_strategy
    captured = []
    original = WalkForwardEngine._run_period_backtest
    def capture(self, **kwargs):
        result = original(self, **kwargs)
        captured.append((kwargs, result))
        return result
    monkeypatch.setattr(WalkForwardEngine, "_run_period_backtest", capture)
    original_truncate = WalkForwardEngine._truncate_oos_result
    def truncate(self, **kwargs):
        result, end = original_truncate(self, **kwargs)
        captured[-1] = (captured[-1][0], result)
        return result, end
    monkeypatch.setattr(WalkForwardEngine, "_truncate_oos_result", truncate)
    source = publish(tmp_path / "pack", rows=5000, triangle=True)
    config = grid_payload()
    config["fixed_params"]["initialCapital"] = 1000
    fields = {"strategy": STRATEGY, "warmupBars": "100", "config": json.dumps(config),
              "wf_is_period_days": "5", "wf_oos_period_days": "3"}
    if adaptive:
        fields.update(wf_adaptive_mode="true", wf_max_oos_period_days="3", wf_min_oos_trades="10000")
    if kind == "parquet":
        fields["source"] = json.dumps(source)
        symbol = "TEST:AAA-USDT-SWAP"
    else:
        frame = prepare_input(source, date_filter=False).frame
        frame.insert(0, "time", frame.index.asi8 // 10**9)
        path = tmp_path / "OKX_TESTUSDT.P, 5 synthetic.csv"
        frame.to_csv(path, index=False)
        fields["csvPath"] = str(path)
        symbol = "OKX:TESTUSDT.P"
    response = client.post("/api/walkforward", data=fields)
    assert response.status_code == 200, response.get_data(as_text=True)
    study_id = response.get_json()["study_id"]
    saved = storage.load_study_from_db(study_id)
    assert captured and len(captured) == 2 * len(saved["windows"])
    for window, ((is_args, is_raw), (oos_args, oos_raw)) in zip(saved["windows"], zip(captured[::2], captured[1::2])):
        assert is_args["params"]["initialCapital"] == oos_args["params"]["initialCapital"] == 1000
        for prefix, raw in [("is_", is_raw), ("oos_", oos_raw)]:
            if prefix == "is_":
                assert raw.trades
            assert window[prefix + "net_profit_pct"] == pytest.approx((raw.balance_curve[-1] / 1000 - 1) * 100)
            assert window[prefix + "romad"] == metrics.calculate_advanced(raw, 1000).romad
    scalar = 100.
    for _, raw in captured[1::2]:
        scalar *= raw.balance_curve[-1] / raw.balance_curve[0]
    stored_curve = json.loads(saved["study"]["stitched_oos_equity_curve"])
    assert stored_curve[0] == 100
    assert saved["study"]["stitched_oos_net_profit_pct"] == pytest.approx(scalar - 100)
    number = saved["windows"][0]["window_number"]
    base = f"/api/studies/{study_id}/wfa"
    for endpoint in [f"windows/{number}/trades", "trades"]:
        response = client.post(base + "/" + endpoint, json={"period": "oos"})
        assert response.status_code == 200
        rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
        assert len(rows) > 1 and {r[0] for r in rows[1:]} == {symbol}
    if not adaptive:
        # Existing route warmup/order differs from engine evaluation arrays. Compare
        # synthetic amounts and common live observations, not the entire arrays.
        scheduled = pd.Timestamp(saved["windows"][0]["oos_start_date"], tz="UTC")
        live = scheduled + pd.Timedelta(days=1)
        with storage.get_db_connection() as conn:
            conn.execute("UPDATE wfa_windows SET trade_start_ts=? WHERE study_id=? AND window_number=?",
                         (live.isoformat(), study_id, number))
            conn.commit()
        raw_runs = []
        original_run = get_strategy(STRATEGY).run
        def replay(frame, params, idx):
            result = original_run(frame, params, idx)
            raw_runs.append(result)
            return result
        with monkeypatch.context() as patch:
            patch.setattr(get_strategy(STRATEGY), "run", staticmethod(replay))
            response = client.post(base + f"/windows/{number}/equity", json={"period": "oos"})
        assert response.status_code == 200, response.get_data(as_text=True)
        assert len(raw_runs) == 1
        route = response.get_json()
        assert route["equity_curve"][0] == 1000
        engine_view = WalkForwardEngine._prepend_flat_prefix(raw_runs[0], scheduled_start=scheduled,
            live_start=live, initial_balance=1000)
        assert engine_view.equity_curve[0] == 1000
        common = {pd.Timestamp(t): v for t, v in zip(route["timestamps"], route["equity_curve"]) if pd.Timestamp(t) >= live}
        assert common
        for stamp, value in zip(engine_view.timestamps, engine_view.equity_curve):
            if stamp >= live:
                assert common[stamp] == value
    with storage.get_db_connection() as conn:
        conn.execute("UPDATE wfa_windows SET window_status='no_trade' WHERE study_id=?", (study_id,))
        conn.commit()
    monkeypatch.setattr(server_routes_data, "_resolve_csv_path", lambda *a, **k: pytest.fail("shortcut resolved CSV"))
    monkeypatch.setattr(server_data_input, "replay_config", lambda *a, **k: pytest.fail("shortcut replayed pack"))
    monkeypatch.setattr(get_strategy(STRATEGY), "run", lambda *a, **k: pytest.fail("flat shortcut ran strategy"))
    for endpoint in ("equity", "trades"):
        assert client.post(base + f"/windows/999/{endpoint}", json={"period": "oos"}).status_code == 404
        assert client.post(base + f"/windows/{number}/{endpoint}", json={"moduleType": "optuna", "trialNumber": 999}).status_code == 404
        assert client.post(base + f"/windows/{number}/{endpoint}", json={"period": "invalid"}).status_code == 400
        response = client.post(base + f"/windows/{number}/{endpoint}", json={"period": "oos"})
        assert response.status_code == 200
        if endpoint == "equity":
            assert response.get_json()["equity_curve"] == [1000.0, 1000.0]
        else:
            assert response.get_data(as_text=True) == export_trades_csv([], symbol=symbol)
    assert storage.load_study_from_db(study_id)["study"]["config_json"] == saved["study"]["config_json"]


@pytest.mark.parametrize("failure,status,message", [
    (FileNotFoundError("raw-path"), 400, "CSV file is missing for this study."),
    (PermissionError("outside allowed roots"), 403, "outside allowed roots"),
    (IsADirectoryError("raw-path"), 400, "CSV path must point to a file."),
    (OSError("raw-path"), 400, "Failed to access CSV file."),
])
def test_csv_export_error_contract(client, monkeypatch, failure, status, message):
    from ui import server_routes_data as routes
    study = dict(study_id="stub", strategy_id=STRATEGY, optimization_mode="grid", config_json={},
                 csv_file_path="original.csv", ft_enabled=True, oos_test_enabled=True)
    monkeypatch.setattr(routes, "load_study_from_db", lambda _: {"study": study, "windows": []})
    monkeypatch.setattr(routes, "_resolve_stored_execution_context", lambda _: SimpleNamespace(warmup_bars=0, params_for=lambda p: p))
    paths = []

    def resolve(path):
        paths.append(path)
        raise failure

    monkeypatch.setattr(routes, "_resolve_csv_path", resolve)
    for endpoint in ("trades", "ft-trades", "oos-trades"):
        response = client.post(f"/api/studies/stub/trials/1/{endpoint}", json={})
        assert response.status_code == status and response.get_json()["error"] == message
    for source, path, expected in [("original_csv", None, "original.csv"), ("new_csv", "new.csv", "new.csv")]:
        monkeypatch.setattr(routes, "load_manual_test_results", lambda *a: {"data_source": source, "csv_path": path})
        response = client.post("/api/studies/stub/tests/1/trials/1/mt-trades", json={})
        assert response.status_code == status
        assert response.get_json()["error"] == message.replace("this study", "this manual test")
        assert paths[-1] == expected
    study["optimization_mode"] = "wfa"
    window = {"window_id": "w1", "window_number": 1, "is_start_date": "2024-01-01", "is_end_date": "2024-01-03",
              "oos_start_ts": "2024-01-04T00:00:00Z", "oos_end_ts": "2024-01-06T00:00:00Z", "best_params": {}}
    monkeypatch.setattr(routes, "load_study_from_db", lambda _: {"study": study, "windows": [window]})
    for endpoint in ("trades", "windows/1/trades", "windows/1/equity"):
        response = client.post(f"/api/studies/stub/wfa/{endpoint}", json={"period": "oos"})
        expected = f"WFA window 1: {message}" if endpoint == "trades" else message
        assert response.status_code == status and response.get_json()["error"] == expected
