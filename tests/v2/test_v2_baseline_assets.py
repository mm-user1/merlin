import csv
import hashlib
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_ROOT = REPO_ROOT / "data" / "baseline_v2" / "s06_r_trend_v02"


def _load_json(path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _csv_rows(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_dataset_json_parses_and_matches_expected_market_data_hash():
    dataset = _load_json(BASELINE_ROOT / "dataset.json")
    market_data = dataset["market_data"]
    data_path = REPO_ROOT / market_data["path"]

    assert dataset["strategy_id"] == "s06_r_trend_v02"
    assert market_data["path"] == "data/raw/OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
    assert market_data["sha256"] == "d664bbae2903828f84b19e7af548fdc744b970a17f56846ad77882a9ca786aae"
    assert data_path.exists()
    assert _sha256(data_path) == market_data["sha256"]


def test_dataset_json_pins_pine_source_hash():
    dataset = _load_json(BASELINE_ROOT / "dataset.json")
    pine_source = dataset["pine_source"]
    pine_path = REPO_ROOT / pine_source["path"]

    assert pine_path.exists()
    assert _sha256(pine_path) == pine_source["sha256"]


def test_dataset_json_pins_reference_trade_csv_hashes():
    dataset = _load_json(BASELINE_ROOT / "dataset.json")
    asset_hashes = dataset["asset_hashes"]
    expected_paths = {
        "data/baseline_v2/s06_r_trend_v02/reference_a_reversal_trail/tradingview_trades.csv",
        "data/baseline_v2/s06_r_trend_v02/reference_a_reversal_trail/trades_normalized_utc.csv",
        "data/baseline_v2/s06_r_trend_v02/reference_b_trend_bracket/tradingview_trades.csv",
        "data/baseline_v2/s06_r_trend_v02/reference_b_trend_bracket/trades_normalized_utc.csv",
    }

    assert {item["path"] for item in asset_hashes} == expected_paths
    for item in asset_hashes:
        asset_path = REPO_ROOT / item["path"]
        assert asset_path.exists()
        assert _sha256(asset_path) == item["sha256"]


def test_reference_json_files_parse():
    for reference in ("reference_a_reversal_trail", "reference_b_trend_bracket"):
        params = _load_json(BASELINE_ROOT / reference / "params.json")
        summary = _load_json(BASELINE_ROOT / reference / "tradingview_summary.json")

        assert params["reference_id"] == reference
        assert summary["reference_id"] == reference
        assert "strategy_inputs" in params
        assert "metrics" in summary


def test_normalized_trade_counts_and_boundary_timestamps_match_summaries():
    reference_a_rows = _csv_rows(BASELINE_ROOT / "reference_a_reversal_trail" / "trades_normalized_utc.csv")
    reference_b_rows = _csv_rows(BASELINE_ROOT / "reference_b_trend_bracket" / "trades_normalized_utc.csv")
    summary_a = _load_json(BASELINE_ROOT / "reference_a_reversal_trail" / "tradingview_summary.json")
    summary_b = _load_json(BASELINE_ROOT / "reference_b_trend_bracket" / "tradingview_summary.json")

    assert len(reference_a_rows) == 61
    assert len(reference_b_rows) == 48
    assert len(reference_a_rows) == summary_a["metrics"]["total_trades"]
    assert len(reference_b_rows) == summary_b["metrics"]["total_trades"]
    assert reference_a_rows[-1]["exit_time_utc"] == "2025-12-01T00:00:00Z"
    assert reference_b_rows[-1]["exit_time_utc"] < "2025-12-01T00:00:00Z"


def test_raw_tradingview_counts_match_summary_row_counts():
    for reference in ("reference_a_reversal_trail", "reference_b_trend_bracket"):
        reference_dir = BASELINE_ROOT / reference
        summary = _load_json(reference_dir / "tradingview_summary.json")
        raw_rows = _csv_rows(reference_dir / "tradingview_trades.csv")
        normalized_rows = _csv_rows(reference_dir / "trades_normalized_utc.csv")

        assert len(raw_rows) == summary["raw_export"]["rows"]
        assert len(raw_rows) == summary["raw_export"]["closed_trades"] * 2
        assert len(normalized_rows) == summary["raw_export"]["closed_trades"]
        assert len(normalized_rows) == summary["metrics"]["total_trades"]
