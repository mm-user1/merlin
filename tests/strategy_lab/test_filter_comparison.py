"""Strict matched Fixed-only deltas, nulls, read-only sources and output ownership."""
from copy import deepcopy
import csv
import importlib
import json
import shutil

import pytest

from tools.strategy_lab.compare_filters import compare
from tools.strategy_lab.fixed.run import run
from tools.strategy_lab.fixed.storage import open_dataset, read_account, RESULT_V2
from tools.strategy_lab.dataset import atomic_write_json, artifact_record
from tools.strategy_lab.generate import generate_dataset
from tools.strategy_lab.synthetic_parquet import publish
from _filter_helpers import packs, fixed_request, is_request, save_request, snapshot


@pytest.fixture
def pair(packs, tmp_path):
    target, filters = packs
    roots = [tmp_path / "off", tmp_path / "on"]
    for name, root, raw in zip(("off", "on"), roots, (fixed_request(), fixed_request(filters))):
        raw["run_name"] = name
        run(save_request(tmp_path, raw, name+".json"), target, root)
    return roots


def test_paired_deltas_offline_and_unavailable(pair, packs, tmp_path, monkeypatch):
    off, on = pair
    before = [snapshot(p) for p in pair]
    from core import data_input
    from tools.strategy_lab import config
    for module, name in ((data_input, "prepare_input"), (data_input, "replay_input"), (config, "get_strategy_config")):
        monkeypatch.setattr(module, name, lambda *a, **k: pytest.fail("offline comparison used sources/live config"))
    output = tmp_path / "comparison"
    meta = compare(off, on, output)
    with (output / "pairs.csv").open(newline="", encoding="utf-8") as f: row, = csv.DictReader(f)
    assert meta["row_count"] == 1 and meta["units"]["net_profit_pct"]["delta"] == "percentage_points"
    assert row["off_account_id"] != row["on_account_id"]
    for metric in ("net_profit_pct", "max_drawdown_mtm_pct", "total_trades"):
        assert float(row["delta_"+metric]) == float(row["on_"+metric]) - float(row["off_"+metric])
    assert [snapshot(p) for p in pair] == before
    # Model an unavailable saved MTM value, preserving its recorded reason.
    manifest = open_dataset(on); account = manifest["contract"]["accounts"][0]
    claim = manifest["completed"][account["account_id"]]
    path = on / claim["result.json"]["path"]
    result = json.loads(path.read_text()); result["metrics"]["max_drawdown_mtm_pct"] = None
    result["metric_unavailable_reasons"]["max_drawdown_mtm_pct"] = "unavailable_core_metric"
    atomic_write_json(path, result); claim["result.json"] = artifact_record(path, on)
    atomic_write_json(on / "manifest.json", manifest)
    compare(off, on, tmp_path / "unavailable")
    with (tmp_path / "unavailable/pairs.csv").open(newline="", encoding="utf-8") as f: row, = csv.DictReader(f)
    assert row["on_max_drawdown_mtm_pct"] == row["delta_max_drawdown_mtm_pct"] == ""
    assert row["on_max_drawdown_mtm_pct_unavailable_reason"] == "unavailable_core_metric"


def test_ordered_series_with_equivalent_target_relocation(packs, tmp_path):
    target, filters = packs
    relocated = tmp_path / "relocated"
    shutil.copytree(target, relocated)
    roots = [tmp_path / "off", tmp_path / "on"]
    for name, root, pack, raw in zip(("off", "on"), roots, (target, relocated), (fixed_request(), fixed_request(filters))):
        second = deepcopy(raw["generation"]["parameter_sets"][0])
        second["name"] = "second"
        second["params"]["stopX"] = 2.17
        raw["generation"]["parameter_sets"].append(second)
        raw["generation"]["expected_run_count"] = 2
        run(save_request(tmp_path, raw, name+".json"), pack, root)
    before = [snapshot(p) for p in roots]
    metadata = compare(*roots, tmp_path / "comparison")
    accounts = open_dataset(roots[0])["contract"]["accounts"]
    with (tmp_path / "comparison/pairs.csv").open(newline="", encoding="utf-8") as f: rows = list(csv.DictReader(f))
    assert metadata["row_count"] == len(rows) == 2
    assert [r["off_account_id"] for r in rows] == [a["account_id"] for a in accounts]
    assert [r["parameter_set"] for r in rows] == [a["parameter_set"] for a in accounts]
    assert [snapshot(p) for p in roots] == before


@pytest.mark.parametrize("change", ["economics", "parameters", "bounds", "warmup", "cell", "roster", "target"])
def test_nonfilter_mismatch_refused(pair, packs, tmp_path, change):
    target, filters = packs; raw = fixed_request(filters)
    if change == "economics": raw["generation"]["economics"]["base_params"]["initialCapital"] = 2000
    elif change == "parameters": raw["generation"]["parameter_sets"][0]["params"]["stopX"] = 2.17
    elif change == "bounds": raw["generation"]["periods"][0]["start"] = "2024-03-02"
    elif change == "warmup": raw["generation"]["periods"][0]["warmup_bars"] = 101
    elif change == "cell": raw["generation"]["market_data"]["instruments"][0]["cell"] = "other"
    elif change == "roster": raw["generation"]["market_data"]["instruments"][0]["instrument_id"] = "BYBIT_BBBUSDT"
    else:
        target = tmp_path / "changed-pack"
        publish(target, symbols=("AAA", "BBB"), start="2024-01-01", end="2024-05-02", change=65*288)
    variant = tmp_path / "changed"; run(save_request(tmp_path, raw), target, variant)
    before = [snapshot(p) for p in (pair[0], variant)]
    with pytest.raises(ValueError, match="differ|mismatch"):
        compare(pair[0], variant, tmp_path / "comparison")
    assert not (tmp_path / "comparison").exists() and before == [snapshot(p) for p in (pair[0], variant)]


@pytest.mark.parametrize("family", ["partial", "old", "is_oos", "on_on", "off_off"])
def test_unsupported_comparisons(pair, packs, tmp_path, family):
    target, filters = packs; baseline, variant = pair
    if family in ("partial", "old"):
        raw = fixed_request(filters if family == "partial" else None)
        if family == "old":
            raw["schema_version"] = "strategy_lab_fixed_runspec_v1"
            raw["generation"]["execution"].pop("entryFilters"); raw["generation"]["execution"]["entry_filters"] = []
        other = tmp_path / "other"; run(save_request(tmp_path, raw), target, other, preflight_only=family == "partial")
        if family == "old": baseline = other
        else: variant = other
    elif family == "is_oos":
        variant = tmp_path / "is_oos"
        generate_dataset(save_request(tmp_path, is_request(filters)), data_root=target, output_dir=variant)
    elif family == "on_on": baseline = variant
    else: variant = baseline
    with pytest.raises(ValueError): compare(baseline, variant, tmp_path / "comparison")
    assert not (tmp_path / "comparison").exists()


def test_output_ownership_and_completion_order(pair, packs, tmp_path, monkeypatch):
    for output in (pair[0] / "report", pair[0].parent, packs[1] / "comparison"):
        with pytest.raises(ValueError, match="overlap"): compare(*pair, output)
    foreign = tmp_path / "foreign"; foreign.mkdir(); (foreign / "owner.txt").write_text("owner")
    before = snapshot(foreign)
    with pytest.raises(ValueError, match="empty output"): compare(*pair, foreign)
    assert snapshot(foreign) == before
    module = importlib.import_module("tools.strategy_lab.compare_filters")
    def fail(path, value):
        assert (path.parent / "pairs.csv").is_file()
        raise OSError("completion interrupted")
    monkeypatch.setattr(module, "atomic_write_json", fail)
    with pytest.raises(OSError): compare(*pair, tmp_path / "failed")
    assert not (tmp_path / "failed/comparison.json").exists()
