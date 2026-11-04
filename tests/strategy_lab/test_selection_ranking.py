"""Live core parity, frozen causal plans and offline evidence invariants."""
from copy import deepcopy
import hashlib
import json
import shutil
import numpy as np
import pytest

from _selection_helpers import publish, request, document, universe
from tools.strategy_lab.selection import prepare_ranking, open_ranking, write_fixed_request
from tools.strategy_lab.selection.ranking import controls_identity, ranking_identity, decision_identity
from tools.strategy_lab.dataset import artifact_record
from tools.strategy_lab.analysis.json_utils import canonical_json_bytes
from _fixed_helpers import snapshot
from _filter_helpers import fixed_request


def prepare(tmp_path, *, count=3, late=None, tie=False, future=None, drop=False, raw=None):
    pack = tmp_path/"pack"
    tickers = publish(pack, count=count, late=late, tie=tie, future=future, drop=drop)
    u = universe(tmp_path, tickers)
    spec = document(tmp_path, "request.json", raw or request())
    root = tmp_path/"ranking"
    prepare_ranking(spec, root, ticker_list=u, data_root=pack)
    return open_ranking(root)


def test_core_parity_listing_tie_70min_and_future_causality(tmp_path):
    from core.ticker_selector import rank_pool, build_calendar
    raw = request(cadence=1, end="2024-05-15")
    raw["tickerSelector"]["params"]["timeframeMinutes"] = 70
    a = prepare(tmp_path/"a", count=4, tie=True, late="2024-02-10", raw=raw)
    b = prepare(tmp_path/"b", count=4, tie=True, late="2024-02-10", future="2024-03-01", raw=raw)
    assert a.metadata["ranking_identity"] != b.metadata["ranking_identity"]
    assert a.records[0]["ranking_decision_identity"] == b.records[0]["ranking_decision_identity"]
    np.testing.assert_array_equal(a.plan(0), b.plan(0))
    assert a.metadata["calendar"]["periods"] == build_calendar("2024-03-01", "2024-05-15", 60, 1)
    assert a.metadata["calendar"]["trailing"][0] == "2024-05-01T00:00:00+00:00"
    for record in a.records:
        direct = rank_pool(a.metadata["pool"], record["period_key"][0], raw["tickerSelector"])
        assert [{k:v for k,v in r.items() if k != "reason_code"} for r in record["rows"]] == direct
    rows = a.records[0]["rows"]
    assert len({r["score"] for r in rows if r["eligible"]}) > 1
    tied = [r for r in rows if r["instrument"] in ("BYBIT_AAAUSDT", "BYBIT_BBBUSDT")]
    assert tied[0]["score"] == tied[1]["score"] and tied[0]["rank"] < tied[1]["rank"]
    assert rows[-1]["reason_code"] == "listing_after_lookback_start" and rows[-1]["months"] == []


def test_identity_exclusions_and_offline_replay(tmp_path, monkeypatch):
    from core import ticker_selector
    a = prepare(tmp_path/"a")
    raw = request(); raw["name"] = "display only"; raw["tickerSelector"]["evaluateUnselected"] = True
    raw["controls"] = dict(seed=42, draws=7)
    u = json.loads((tmp_path/"a/universe.json").read_text())
    u.update(tickers=list(reversed(u["tickers"])), selection_note="changed note", name="new display")
    prepare_ranking(document(tmp_path, "different.json", raw), tmp_path/"different", ticker_list=document(tmp_path, "u.json", u), data_root=tmp_path/"a/pack")
    b = open_ranking(tmp_path/"different")
    assert a.metadata["ranking_identity"] == b.metadata["ranking_identity"]
    assert a.metadata["controls_identity"] != b.metadata["controls_identity"]
    for name in ("rank_pool", "build_calendar", "normalize_selector"):
        monkeypatch.setattr(ticker_selector, name, lambda *a, **k: pytest.fail("live X called offline"))
    monkeypatch.setattr(np.random, "PCG64", lambda *a, **k: pytest.fail("RNG replay"))
    shutil.copytree(a.root, tmp_path/"relocated")
    moved = open_ranking(tmp_path/"relocated")
    assert moved.metadata["ranking_identity"] == a.metadata["ranking_identity"]
    np.testing.assert_array_equal(moved.plan(0), a.plan(0))
    moved.records[0]["rows"][0]["score"] = 999
    with pytest.raises(ValueError, match="mutated"):
        moved.plan(0)


def test_coverage_refusal_and_transform_listing_guard(tmp_path):
    with pytest.raises(ValueError, match="coverage|gap|99%"):
        prepare(tmp_path/"bad", drop=True)
    assert not (tmp_path/"bad/ranking/ranking.json").exists()
    a = prepare(tmp_path/"late", late="2024-03-01")
    raw = fixed_request()
    raw["generation"]["market_data"]["instruments"] = [dict(instrument_id=t, cell="research") for t in a.metadata["universe"]["document"]["tickers"]]
    raw["generation"]["expected_run_count"] = 3
    template = document(tmp_path, "template.json", raw)
    before = snapshot(a.root)
    with pytest.raises(ValueError, match="impossible execution/warmup"):
        write_fixed_request(a, template, tmp_path/"refused", warmup_bars=100)
    assert not (tmp_path/"refused").exists() and snapshot(a.root) == before


@pytest.mark.parametrize("change", ["version", "rank", "listing", "pin", "calendar", "csv", "dtype", "shape", "duplicate", "out_of_range"])
def test_resealed_malformed_frozen_artifacts_refused(tmp_path, change):
    a = prepare(tmp_path)
    m = deepcopy(a.metadata)
    rows = dict(schema_version="strategy_lab_x_rows_v1", periods=deepcopy(a.records))
    if change == "version": m["schema_version"] = "future"
    elif change == "rank": rows["periods"][0]["rows"][0]["rank"] = 2
    elif change == "listing": rows["periods"][0]["rows"][0]["listing"] = "2024-03-10"
    elif change == "pin": rows["periods"][0]["rows"][0]["months"][0]["observed"] = 0
    elif change == "calendar": m["calendar"]["periods"][0]["windows"][0]["isStart"] = "2024-01-01"
    elif change == "csv": (a.root/"ranking.csv").write_text("bad\n", encoding="utf-8")
    else:
        plan = m["controls"]["periods"][0]; array = a.plan(0).copy()
        if change == "dtype": array = array.astype(np.int64)
        elif change == "shape": array = array[:-1]
        elif change == "duplicate": array[0, 1] = array[0, 0]
        else: array[0, 1] = 255
        np.save(a.root/plan["file"], array, allow_pickle=False)
        plan["content_sha256"] = hashlib.sha256(array.tobytes()).hexdigest()
        m["controls_identity"] = controls_identity(m)
    (a.root/"ranking_rows.json").write_bytes(canonical_json_bytes(rows))
    for name in m["artifacts"]:
        m["artifacts"][name] = artifact_record(a.root/name, a.root)
    (a.root/"ranking.json").write_bytes(canonical_json_bytes(m))
    with pytest.raises(ValueError):
        open_ranking(a.root)


@pytest.mark.parametrize("draws", [True, 0, 100001])
def test_controls_bounds_before_market_reads(tmp_path, draws):
    raw = request(draws=draws)
    with pytest.raises(ValueError, match="draws"):
        prepare_ranking(document(tmp_path, "request.json", raw), tmp_path/"out", ticker_list="absent", data_root="absent")
    assert not (tmp_path/"out").exists()


def test_executable_listing_exclusion_and_transform_offline(tmp_path, monkeypatch):
    from tools.strategy_lab.fixed.run import run
    from tools.strategy_lab.selection import evaluate_selection
    from core import ticker_selector
    a = prepare(tmp_path/"source", late="2024-02-10")
    raw = fixed_request()
    raw["generation"]["market_data"]["instruments"] = [dict(instrument_id=t, cell="research") for t in a.metadata["universe"]["document"]["tickers"]]
    raw["generation"]["expected_run_count"] = 3
    template = document(tmp_path, "template.json", raw)
    monkeypatch.setattr(ticker_selector, "rank_pool", lambda *a,**k:pytest.fail("transform/evaluation reranked"))
    write_fixed_request(a, template, tmp_path/"transform", warmup_bars=100)
    run(tmp_path/"transform/fixed-request.json", tmp_path/"source/pack", tmp_path/"fixed")
    result = evaluate_selection(a, fixed_inputs={"off":tmp_path/"fixed"})
    row = next(r for r in result.tables["decisions"] if r["block_id"] == 1 and r["ticker"] == "BYBIT_CCCUSDT")
    assert row["eligible"] is False and row["status"] == "completed" and row["value"] is not None
    block = next(r for r in result.tables["blocks"] if r["block_id"] == 1 and r["arm"] == "all_declared")
    assert block["selected_count"] == 3 and block["expected_denominator"] == 3
    eligible = next(r for r in result.tables["blocks"] if r["block_id"] == 1 and r["arm"] == "all_x_eligible")
    assert eligible["selected_count"] == 2 and eligible["expected_denominator"] == 2


def test_empty_eligible_plan_and_multi_month_calendar(tmp_path):
    empty = prepare(tmp_path/"empty", count=1, late="2024-03-02", raw=request(end="2024-03-31", k=1))
    assert empty.plan(0).shape == (32,0)
    assert empty.metadata["controls"]["total_array_bytes"] == 0
    raw = request(mode="is_oos", start="2024-01-01", end="2024-08-15", cadence=4)
    raw["calendar"].update(isMonths=1, oosMonths=2)
    a = prepare(tmp_path/"multi", raw=raw)
    from core.ticker_selector import build_calendar
    assert a.metadata["calendar"]["periods"] == build_calendar("2024-01-01", "2024-08-15", 60, 4, is_months=1, oos_months=2)
    assert [len(p["windows"]) for p in a.metadata["calendar"]["periods"]] == [2,1]


def test_prepare_and_transform_descriptor_last_interruption(tmp_path, monkeypatch):
    from tools.strategy_lab import dataset as publication
    pack = tmp_path/"pack"; tickers = publish(pack)
    path = document(tmp_path,"request.json",request()); u = universe(tmp_path,tickers)
    original = publication.atomic_write
    def interrupt(path, writer):
        if path.name == "ranking.csv": raise OSError("synthetic interruption")
        return original(path,writer)
    monkeypatch.setattr(publication,"atomic_write",interrupt)
    with pytest.raises(OSError,match="interruption"):
        prepare_ranking(path,tmp_path/"interrupted",ticker_list=u,data_root=pack)
    assert not (tmp_path/"interrupted/ranking.json").exists()
    with pytest.raises(ValueError,match="cannot load"):
        open_ranking(tmp_path/"interrupted")
    with pytest.raises(ValueError,match="empty output"):
        prepare_ranking(path,tmp_path/"interrupted",ticker_list=u,data_root=pack)
    with pytest.raises(ValueError,match="overlaps"):
        prepare_ranking(path,pack/"report",ticker_list=u,data_root=pack)
    monkeypatch.setattr(publication,"atomic_write",original)
    prepare_ranking(path,tmp_path/"ranking",ticker_list=u,data_root=pack)
    raw = fixed_request()
    raw["generation"]["market_data"]["instruments"] = [dict(instrument_id=t,cell="research") for t in tickers]
    raw["generation"]["expected_run_count"] = len(tickers)
    template = document(tmp_path,"template.json",raw)
    def interrupt_transform(path,writer):
        if path.name == "fixed-request.json": raise OSError("synthetic transform interruption")
        return original(path,writer)
    monkeypatch.setattr(publication,"atomic_write",interrupt_transform)
    with pytest.raises(OSError,match="interruption"):
        write_fixed_request(tmp_path/"ranking",template,tmp_path/"transform",warmup_bars=100)
    assert not (tmp_path/"transform/transform.json").exists()


def test_explicit_whole_counts_normalize_like_core():
    from tools.strategy_lab.selection.ranking import request as validate
    from core.ticker_selector import normalize_selector
    raw = request(mode="is_oos")
    raw["tickerSelector"].update(topN=2.0, rebalanceMonths=2.0)
    raw["tickerSelector"]["params"]["lookbackMonths"] = 1.0
    raw["calendar"].update(isMonths=1.0, oosMonths=1.0)
    assert validate(raw)["tickerSelector"] == normalize_selector(raw["tickerSelector"])
    assert validate(raw) == validate(raw,live=True)
    raw["tickerSelector"]["topN"] = True
    with pytest.raises(ValueError,match="positive whole"):
        validate(raw)
