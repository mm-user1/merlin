"""Historical NPZ import: provenance, source preservation and sidecar strictness."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import import_npz as legacy
from tools.pattern_lab import manifest as pack_manifest

from ._helpers import ANCHOR_MS, STEP_MS, synthetic_series, utc

OKX_ID = "OKX_AAA-USDT-SWAP"
BYBIT_ID = "BYBIT_ENAUSDT"
OKX_ENDPOINT = "GET https://www.okx.com/api/v5/market/history-candles (bar=5m)"


def legacy_series(symbol="AAA", venue="OKX", contract="AAA-USDT-SWAP", role="trading", slot_count=48, **extra):
    stamps, values = synthetic_series(slot_count)
    spec = {
        "symbol": symbol,
        "venue": venue,
        "contract": contract,
        "role": role,
        "timestamps": stamps,
        "ohlcv": values.astype(np.float32),
        "scalars": True,
        "arrays": None,
    }
    spec.update(extra)
    return spec


def write_legacy_pack(root: Path, specs, *, manifest_overrides=None, entry_overrides=None) -> Path:
    """Write a prototype-layout NPZ pack at ``root`` and return it."""
    root = Path(root)
    (root / "5m").mkdir(parents=True)
    series = []
    for spec in specs:
        path = root / "5m" / f"{spec['symbol']}.npz"
        arrays = spec["arrays"]
        if arrays is None:
            arrays = {"ts": spec["timestamps"], "ohlcv": spec["ohlcv"]}
            if spec["scalars"]:
                arrays["ex"] = np.array(spec.get("scalar_venue", spec["venue"]))
                arrays["sym"] = np.array(spec.get("scalar_symbol", spec["symbol"]))
        np.savez_compressed(path, **arrays)
        entry = {
            "symbol": spec["symbol"],
            "venue": spec["venue"],
            "contract": spec["contract"],
            "role": spec["role"],
            "source_endpoint": OKX_ENDPOINT,
            "base_timeframe": "5m",
            "file": f"5m/{spec['symbol']}.npz",
            "bars": int(np.size(spec["timestamps"])),
            "first_ts_ms": int(np.min(spec["timestamps"])),
            "last_ts_ms": int(np.max(spec["timestamps"])),
            "sha256": pack_manifest.file_sha256(path),
        }
        entry.update(spec.get("entry_overrides") or {})
        entry.update(entry_overrides or {})
        series.append(entry)
    manifest = {
        "manifest_version": 1,
        "generated_utc": "2026-09-13T02:26:59Z",
        "volume_convention": "quote_volume_usd (USD notional), NOT base-asset volume.",
        "gap_policy": "Gaps < 3 bars are forward-filled with a stale flag.",
        "splits": {"discovery": [0.0, 0.65], "validation": [0.65, 0.85], "holdout": [0.85, 1.0],
                   "status": "discovery used; validation and holdout UNTOUCHED as of 2026-09-13"},
        "series": series,
    }
    manifest.update(manifest_overrides or {})
    (root / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=1), encoding="utf-8", newline="\n"
    )
    return root


def sidecar(instruments=None, **overrides):
    payload = {
        "schema_version": 1,
        "instruments": instruments
        if instruments is not None
        else {
            OKX_ID: {
                "quote_currency": "USDT",
                "volume_unit_evidence": "OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
                "evidence_source": "Source review plus the OKX candle field definition",
            }
        },
    }
    payload.update(overrides)
    return payload


def source_hashes(root: Path) -> dict[str, str]:
    paths = sorted(Path(root).rglob("*"))
    return {str(path.relative_to(root)): pack_manifest.file_sha256(path) for path in paths if path.is_file()}


class TestCanonicalImport:
    def test_round_trip_preserves_values_roles_and_provenance(self, tmp_path):
        source = write_legacy_pack(
            tmp_path / "legacy",
            [
                legacy_series(),
                legacy_series(symbol="ENA", venue="Bybit", contract="ENAUSDT", role="research_only"),
            ],
        )
        metadata = sidecar(
            {
                OKX_ID: {
                    "quote_currency": "USDT",
                    "volume_unit_evidence": "OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
                    "evidence_source": "Source review plus the OKX candle field definition",
                },
                BYBIT_ID: {
                    "quote_currency": "USDT",
                    "volume_unit_evidence": "Linear ENAUSDT turnover; fetch_base.py reads kline field 6.",
                    "evidence_source": "Source review plus the Bybit V5 kline field definition",
                },
            }
        )
        summary = legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=metadata)

        assert summary["instrument_count"] == 2
        assert summary["state"] == "ready"
        manifest = pack_manifest.read_manifest(tmp_path / "pack")
        okx, bybit = manifest["instruments"]
        assert okx["instrument_id"] == OKX_ID
        assert bybit["instrument_id"] == BYBIT_ID
        assert okx["roles"] == ["trading"]
        assert bybit["roles"] == ["research_only"]
        assert okx["quote_currency"] == "USDT"
        assert okx["source"]["input_format"] == "npz"
        assert okx["source"]["input_dtype"] == "float32"
        assert okx["source"]["float32_promoted"] is True
        assert okx["source"]["legacy_role"] == "trading"
        assert okx["source"]["timestamps_resorted"] is False
        assert okx["source"]["source_hash"] == pack_manifest.file_sha256(source / "5m" / "AAA.npz")
        assert "field 7" in okx["source"]["volume_unit_evidence"]
        assert okx["verification"]["volume_quote_verified"] is True
        assert okx["verification"]["closed_before_utc"] is None
        assert manifest["universe"]["selection_date"] is None
        assert manifest["universe"]["historical_membership"] == "unknown"

        stamps, values = synthetic_series(48)
        read_stamps, read_values = pack_data.read_ohlcv_rows(
            tmp_path / "pack" / "ohlcv" / f"{OKX_ID}_5m.parquet"
        )
        assert np.array_equal(read_stamps, stamps)
        assert np.array_equal(read_values, values.astype(np.float32).astype(np.float64))

    def test_old_split_and_gap_fill_claims_are_not_adopted(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())
        published = pack_manifest.manifest_path(tmp_path / "pack").read_text(encoding="utf-8")
        readme = (tmp_path / "pack" / "README.md").read_text(encoding="utf-8")
        for claim in ("discovery", "validation", "holdout", "UNTOUCHED", "forward-filled", "USD notional"):
            assert claim not in published
            assert claim not in readme

    def test_import_with_closure_evidence_supports_research_reads(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series(slot_count=144)])
        cutoff = utc(ANCHOR_MS + 144 * STEP_MS)
        metadata = sidecar(
            {
                OKX_ID: {
                    "quote_currency": "USDT",
                    "volume_unit_evidence": "OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
                    "evidence_source": "Source review plus the OKX candle field definition",
                    "closed_before_utc": cutoff,
                    "closure_evidence": "Every retained bar precedes the recorded fetch cutoff.",
                    "closure_source": "Prototype fetch log reviewed at import time",
                }
            }
        )
        legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=metadata)
        loaded = pack_data.load_slice(
            tmp_path / "pack", OKX_ID, start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 144 * STEP_MS), timeframe_minutes=60
        )
        assert len(loaded.bars) == 12

    def test_unsorted_timestamps_are_stably_sorted_and_recorded(self, tmp_path):
        stamps, values = synthetic_series(12)
        order = np.array([3, 0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11])
        spec = legacy_series()
        spec["timestamps"] = stamps[order]
        spec["ohlcv"] = values[order].astype(np.float32)
        source = write_legacy_pack(tmp_path / "legacy", [spec])
        legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())
        entry = pack_manifest.read_manifest(tmp_path / "pack")["instruments"][0]
        assert entry["source"]["timestamps_resorted"] is True
        read_stamps, read_values = pack_data.read_ohlcv_rows(
            tmp_path / "pack" / "ohlcv" / f"{OKX_ID}_5m.parquet"
        )
        assert np.array_equal(read_stamps, stamps)
        assert np.array_equal(read_values, values.astype(np.float32).astype(np.float64))


class TestSourcePreservation:
    def test_sources_are_unchanged_after_success_and_failure(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        before = source_hashes(source)

        legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())
        assert source_hashes(source) == before

        with pytest.raises(PatternLabDataError):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())
        with pytest.raises(PatternLabDataError):
            legacy.import_npz_pack(source, tmp_path / "other", source_metadata=sidecar({"OKX_WRONG": {
                "quote_currency": "USDT", "volume_unit_evidence": "x", "evidence_source": "y"}}))
        assert source_hashes(source) == before

    def test_object_arrays_are_never_unpickled(self, tmp_path):
        spec = legacy_series()
        spec["arrays"] = {
            "ts": np.array([{"evil": True}], dtype=object),
            "ohlcv": spec["ohlcv"],
        }
        source = write_legacy_pack(tmp_path / "legacy", [spec])
        with pytest.raises(PatternLabDataError, match="without pickle"):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"sha256": "b" * 64}, "does not match the source manifest digest"),
            ({"bars": 47}, "do not match the manifest count"),
            ({"first_ts_ms": 0}, "does not match the manifest range"),
            ({"last_ts_ms": 0}, "does not match the manifest range"),
        ],
    )
    def test_mismatched_manifest_facts_are_refused(self, tmp_path, overrides, message):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()], entry_overrides=overrides)
        with pytest.raises(PatternLabDataError, match=message):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

    @pytest.mark.parametrize(
        "overrides",
        [
            {"role": "holdout"},
            {"file": "../outside.npz"},
            {"file": "/abs/AAA.npz"},
            {"file": "5m/AAA.txt"},
            {"bars": True},
        ],
    )
    def test_invalid_legacy_entries_are_rejected(self, tmp_path, overrides):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()], entry_overrides=overrides)
        with pytest.raises(PatternLabDataError):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

    def test_unknown_legacy_manifest_version_is_rejected(self, tmp_path):
        source = write_legacy_pack(
            tmp_path / "legacy", [legacy_series()], manifest_overrides={"manifest_version": 2}
        )
        with pytest.raises(PatternLabDataError, match="version 1 prototype layout"):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

    @pytest.mark.parametrize("field, value", [("scalar_venue", "Bybit"), ("scalar_symbol", "BBB")])
    def test_optional_scalars_are_checked_against_the_manifest(self, tmp_path, field, value):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series(**{field: value})])
        with pytest.raises(PatternLabDataError, match="does not match the manifest"):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

    def test_missing_optional_scalars_are_accepted(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series(scalars=False)])
        legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())
        assert pack_manifest.read_manifest(tmp_path / "pack")["instruments"][0]["row_count"] == 48

    def test_duplicate_timestamps_are_refused(self, tmp_path):
        stamps, values = synthetic_series(12)
        stamps[5] = stamps[4]
        spec = legacy_series()
        spec["timestamps"] = stamps
        spec["ohlcv"] = values.astype(np.float32)
        source = write_legacy_pack(tmp_path / "legacy", [spec])
        with pytest.raises(PatternLabDataError, match="duplicate"):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

    def test_overlapping_roots_are_refused(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        with pytest.raises(PatternLabDataError, match="must not overlap"):
            legacy.import_npz_pack(source, source / "pack", source_metadata=sidecar())


class TestSourceMetadataSidecar:
    def test_valid_sidecar_is_normalized(self, tmp_path):
        path = tmp_path / "metadata.json"
        path.write_text(json.dumps(sidecar()), encoding="utf-8", newline="\n")
        loaded = legacy.load_source_metadata(path)
        assert loaded["instruments"][OKX_ID]["closed_before_utc"] is None
        assert loaded["instruments"][OKX_ID]["quote_currency"] == "USDT"

    def test_duplicate_json_keys_are_rejected(self, tmp_path):
        path = tmp_path / "metadata.json"
        path.write_text(
            '{"schema_version": 1, "instruments": {"OKX_A": {"quote_currency": "USDT", '
            '"quote_currency": "USDC", "volume_unit_evidence": "e", "evidence_source": "s"}}}',
            encoding="utf-8",
            newline="\n",
        )
        with pytest.raises(PatternLabDataError, match="duplicate JSON key"):
            legacy.load_source_metadata(path)

    @pytest.mark.parametrize(
        "payload",
        [
            sidecar(schema_version=2),
            sidecar(schema_version=True),
            {"instruments": {}},
            {"schema_version": 1},
            dict(sidecar(), extra="unexpected"),
            sidecar({}),
            sidecar({"okx_aaa-usdt-swap": {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s"}}),
            sidecar({"OKXAAA": {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "e"}}),
            sidecar({OKX_ID: {"quote_currency": "usdt", "volume_unit_evidence": "e", "evidence_source": "s"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "  ", "evidence_source": "s"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": 7, "evidence_source": "s"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s",
                              "note": "unknown key"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s",
                              "closure_evidence": "partial"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s",
                              "closed_before_utc": "2026-01-01T00:00:00Z", "closure_evidence": "e"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s",
                              "closed_before_utc": "2026-01-01T00:01:00Z", "closure_evidence": "e",
                              "closure_source": "s"}}),
            sidecar({OKX_ID: {"quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s",
                              "closed_before_utc": "2026-01-01T00:00:00", "closure_evidence": "e",
                              "closure_source": "s"}}),
        ],
    )
    def test_invalid_sidecars_are_rejected(self, payload):
        with pytest.raises(PatternLabDataError):
            legacy.validate_source_metadata(payload)

    def test_instrument_ids_must_match_the_source_exactly(self, tmp_path):
        source = write_legacy_pack(
            tmp_path / "legacy",
            [legacy_series(), legacy_series(symbol="ENA", venue="Bybit", contract="ENAUSDT")],
        )
        with pytest.raises(PatternLabDataError, match=r"missing \['BYBIT_ENAUSDT'\]"):
            legacy.import_npz_pack(source, tmp_path / "pack", source_metadata=sidecar())

        extra = sidecar()
        extra["instruments"]["OKX_ZZZ"] = {
            "quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s"
        }
        extra["instruments"][BYBIT_ID] = {
            "quote_currency": "USDT", "volume_unit_evidence": "e", "evidence_source": "s"
        }
        with pytest.raises(PatternLabDataError, match=r"unexpected \['OKX_ZZZ'\]"):
            legacy.import_npz_pack(source, tmp_path / "other", source_metadata=extra)

    def test_missing_sidecar_file_is_reported(self, tmp_path):
        with pytest.raises(PatternLabDataError, match="file not found"):
            legacy.load_source_metadata(tmp_path / "absent.json")
