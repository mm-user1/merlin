"""Publication, low-level file writing and pack inspection contracts."""

from __future__ import annotations

from datetime import timedelta, timezone
import json

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import manifest as pack_manifest

from ._helpers import (
    ANCHOR_MS,
    GENERATED_UTC,
    STEP_MS,
    instrument_source,
    mutate_manifest,
    publish,
    read_raw_manifest,
    single_pack,
    synthetic_series,
    utc,
    write_raw_manifest,
)


class TestCanonicalRoundTrip:
    def test_multiple_instruments_and_roles_round_trip_exactly(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(120)
        float32 = values.astype(np.float32)
        sources = [
            instrument_source(stamps, float32, symbol="AAA", venue="OKX", contract="AAA-USDT-SWAP"),
            instrument_source(
                stamps, values, symbol="BTC", venue="OKX", contract="BTC-USDT-SWAP", roles=["factor"]
            ),
            instrument_source(
                stamps, values, symbol="ENA", venue="Bybit", contract="ENAUSDT", roles=["trading", "factor"]
            ),
            instrument_source(
                stamps, values, symbol="ZK", venue="OKX", contract="ZK-USDT-SWAP", roles=["research_only"]
            ),
        ]
        summary = publish(root, sources)

        assert summary["instrument_count"] == 4
        assert [item["instrument_id"] for item in summary["instruments"]] == [
            "OKX_AAA-USDT-SWAP",
            "OKX_BTC-USDT-SWAP",
            "BYBIT_ENAUSDT",
            "OKX_ZK-USDT-SWAP",
        ]
        manifest = pack_manifest.read_manifest(root)
        assert manifest["state"] == "ready"
        assert manifest["revision"] == 1
        assert [entry["roles"] for entry in manifest["instruments"]] == [
            ["trading"],
            ["factor"],
            ["factor", "trading"],
            ["research_only"],
        ]

        read_stamps, read_values = pack_data.read_ohlcv_rows(root / "ohlcv" / "OKX_AAA-USDT-SWAP_5m.parquet")
        assert read_values.dtype == np.float64
        assert np.array_equal(read_stamps, stamps)
        # float32 input is promoted exactly; no precision is claimed to be recovered.
        assert np.array_equal(read_values, float32.astype(np.float64))

    def test_written_file_has_exactly_the_schema_v1_columns(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        import pyarrow.parquet as pq

        root = tmp_path / "pack"
        single_pack(root, slot_count=24)
        schema = pq.read_schema(root / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet")
        assert schema.names == ["timestamp", "open", "high", "low", "close", "volume_quote"]
        assert schema.field("timestamp").type == pa.timestamp("ms", tz="UTC")
        assert all(schema.field(name).type == pa.float64() for name in pack_data.OHLCV_COLUMNS)

    def test_generated_readme_and_history_agree_with_the_manifest(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(30, drop_slots=[7])
        publish(root, [instrument_source(stamps, values)])
        manifest = pack_manifest.read_manifest(root)
        entry = manifest["instruments"][0]

        readme = (root / "README.md").read_text(encoding="utf-8")
        assert readme == pack_manifest.render_readme(manifest)
        assert entry["sha256"] in readme
        assert entry["coverage_end_utc"] in readme
        assert "1 missing 5m bars" in readme

        records = [json.loads(line) for line in (root / "updates.jsonl").read_text(encoding="utf-8").splitlines()]
        assert len(records) == 1
        assert records[0]["event"] == "publish"
        assert records[0]["revision"] == manifest["revision"]
        assert records[0]["state"] == manifest["state"]
        assert records[0]["instruments"] == [entry["instrument_id"]]
        assert entry["missing_bar_count"] == 1
        assert entry["row_count"] == 29

    def test_caller_created_frames_are_normalized_to_float64(self, tmp_path):
        pandas = pytest.importorskip("pandas")
        stamps, values = synthetic_series(12)
        frame = pandas.DataFrame(
            values.astype(np.float32), columns=list(pack_data.OHLCV_COLUMNS)
        )
        frame.insert(0, "timestamp", pandas.to_datetime(stamps, unit="ms", utc=True))
        canonical_stamps, canonical_values = pack_data.series_from_frame(frame)
        assert canonical_values.dtype == np.float64
        assert np.array_equal(canonical_stamps, stamps)
        assert np.array_equal(canonical_values, values.astype(np.float32).astype(np.float64))


class TestInvalidInput:
    @pytest.mark.parametrize(
        "mutate, message",
        [
            (lambda t, v: (t, _with(v, 0, 1, np.nan)), "finite"),
            (lambda t, v: (t, _with(v, 0, 1, np.inf)), "finite"),
            (lambda t, v: (t, _with(v, 0, 0, -1.0)), "positive"),
            (lambda t, v: (t, _with(v, 0, 0, 0.0)), "positive"),
            (lambda t, v: (t, _with(v, 0, 1, 1.0)), "low <= min"),
            (lambda t, v: (t, _with(v, 0, 2, 1e9)), "low <= min"),
            (lambda t, v: (t, _with(v, 0, 4, -1.0)), "nonnegative"),
            (lambda t, v: (t, _with(v, 0, 4, np.nan)), "finite"),
            (lambda t, v: (np.array([], dtype=np.int64), v[:0]), "empty instruments"),
            (lambda t, v: (_with_stamp(t, 1, t[0]), v), "duplicate"),
            (lambda t, v: (_with_stamp(t, 1, t[0] + 60_000), v), "grid"),
            (lambda t, v: (t[::-1].copy(), v), "strictly increasing"),
            (lambda t, v: (t[:-1], v), "do not match"),
            (lambda t, v: (t.astype(np.float64), v), "integer epoch"),
            (lambda t, v: (t, v[:, :4]), r"expected a \(N, 5\)"),
        ],
    )
    def test_invalid_series_are_rejected(self, mutate, message):
        stamps, values = synthetic_series(12)
        bad_stamps, bad_values = mutate(stamps, values)
        with pytest.raises(PatternLabDataError, match=message):
            pack_data.validate_series(bad_stamps, bad_values)

    def test_signed_zero_volume_is_normalized(self, tmp_path):
        stamps, values = synthetic_series(6)
        values[:, 4] = -0.0
        _, canonical = pack_data.validate_series(stamps, values)
        assert np.array_equal(np.copysign(1.0, canonical[:, 4]), np.ones(6))

        root = tmp_path / "pack"
        publish(root, [instrument_source(stamps, values)])
        _, stored = pack_data.read_ohlcv_rows(root / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet")
        assert np.array_equal(np.copysign(1.0, stored[:, 4]), np.ones(6))

    def test_validation_does_not_mutate_the_caller_array(self):
        stamps, values = synthetic_series(6)
        values[:, 4] = -0.0
        original = values.copy()
        pack_data.validate_series(stamps, values)
        assert np.array_equal(np.copysign(1.0, values[:, 4]), np.copysign(1.0, original[:, 4]))

    def test_zero_volume_bars_are_preserved(self):
        stamps, values = synthetic_series(6)
        values[:, 4] = 0.0
        _, canonical = pack_data.validate_series(stamps, values)
        assert np.array_equal(canonical[:, 4], np.zeros(6))


def _with(values, row, column, replacement):
    updated = values.copy()
    updated[row, column] = replacement
    return updated


def _with_stamp(stamps, index, replacement):
    updated = stamps.copy()
    updated[index] = replacement
    return updated


class TestPublicationBoundaries:
    def test_existing_destination_is_refused(self, tmp_path):
        root = tmp_path / "pack"
        root.mkdir()
        stamps, values = synthetic_series(6)
        with pytest.raises(PatternLabDataError, match="already exists"):
            publish(root, [instrument_source(stamps, values)])
        assert not (root / pack_manifest.MANIFEST_NAME).exists()

    def test_second_publication_cannot_overwrite_the_result(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=12)
        digest = pack_manifest.read_manifest(root)["instruments"][0]["sha256"]
        stamps, values = synthetic_series(24)
        with pytest.raises(PatternLabDataError, match="already exists"):
            publish(root, [instrument_source(stamps, values)])
        assert pack_manifest.read_manifest(root)["instruments"][0]["sha256"] == digest

    @pytest.mark.parametrize("nested", ["inside", "outside"])
    def test_overlapping_source_and_output_roots_are_refused(self, tmp_path, nested):
        source = tmp_path / "source"
        source.mkdir()
        output = source / "pack" if nested == "inside" else source
        stamps, values = synthetic_series(6)
        with pytest.raises(PatternLabDataError, match="must not overlap"):
            pack_data.publish_pack(
                output,
                [instrument_source(stamps, values)],
                universe=pack_manifest.build_universe(),
                generated_utc=GENERATED_UTC,
                source_root=source,
            )

    def test_failure_before_the_manifest_leaves_unreadable_output(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(12)
        broken = values.copy()
        broken[3, 2] = 1e9  # low above the candle body
        with pytest.raises(PatternLabDataError):
            publish(
                root,
                [
                    instrument_source(stamps, values, symbol="AAA", contract="AAA-USDT-SWAP"),
                    instrument_source(stamps, broken, symbol="BBB", contract="BBB-USDT-SWAP"),
                ],
            )
        # The first file survives as error evidence; no ready manifest was published.
        assert (root / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet").is_file()
        assert not (root / pack_manifest.MANIFEST_NAME).exists()
        assert not list((root / "ohlcv").glob(".*.tmp-*"))
        with pytest.raises(PatternLabDataError, match="publication did not complete"):
            pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + STEP_MS))

    def test_publication_requires_at_least_one_instrument(self, tmp_path):
        with pytest.raises(PatternLabDataError, match="at least one instrument"):
            pack_data.publish_pack(
                tmp_path / "pack", [], universe=pack_manifest.build_universe(), generated_utc=GENERATED_UTC
            )

    def test_duplicate_instruments_are_refused(self, tmp_path):
        stamps, values = synthetic_series(6)
        with pytest.raises(PatternLabDataError, match="duplicate instrument"):
            publish(tmp_path / "pack", [instrument_source(stamps, values), instrument_source(stamps, values)])


class TestLowLevelFileWriter:
    def test_default_refuses_to_replace_an_existing_file(self, tmp_path):
        stamps, values = synthetic_series(6)
        target = tmp_path / "OWNED_A_5m.parquet"
        written = pack_data.write_ohlcv_file(target, stamps, values)
        assert written.row_count == 6
        with pytest.raises(PatternLabDataError, match="refusing to replace"):
            pack_data.write_ohlcv_file(target, stamps, values)

    def test_explicit_replacement_updates_a_caller_owned_file(self, tmp_path):
        target = tmp_path / "OWNED_A_5m.parquet"
        first_stamps, first_values = synthetic_series(6)
        original = pack_data.write_ohlcv_file(target, first_stamps, first_values)
        second_stamps, second_values = synthetic_series(9)
        replaced = pack_data.write_ohlcv_file(target, second_stamps, second_values, replace_existing=True)
        assert replaced.sha256 != original.sha256
        assert replaced.row_count == 9
        stamps, _ = pack_data.read_ohlcv_rows(target)
        assert np.array_equal(stamps, second_stamps)
        assert not list(tmp_path.glob(".*.tmp-*"))

    def test_readback_failure_preserves_the_original_and_removes_the_temporary(self, tmp_path, monkeypatch):
        target = tmp_path / "OWNED_A_5m.parquet"
        stamps, values = synthetic_series(6)
        original = pack_data.write_ohlcv_file(target, stamps, values)

        def explode(*args, **kwargs):
            raise PatternLabDataError("injected readback failure")

        monkeypatch.setattr(pack_data, "read_ohlcv_rows", explode)
        other_stamps, other_values = synthetic_series(12)
        with pytest.raises(PatternLabDataError, match="injected readback failure"):
            pack_data.write_ohlcv_file(target, other_stamps, other_values, replace_existing=True)
        monkeypatch.undo()

        assert pack_manifest.file_sha256(target) == original.sha256
        assert not list(tmp_path.glob(".*.tmp-*"))

    def test_missing_destination_directory_is_reported(self, tmp_path):
        stamps, values = synthetic_series(6)
        with pytest.raises(PatternLabDataError, match="does not exist"):
            pack_data.write_ohlcv_file(tmp_path / "absent" / "A_5m.parquet", stamps, values)


class TestInspection:
    def test_inspect_reports_metadata_and_verification_limitations(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(30, drop_slots=[4])
        publish(root, [instrument_source(stamps, values, closed_before_ms=None)])
        report = pack_data.inspect_pack(root)

        assert report["state"] == "ready"
        assert report["update_in_progress"] is False
        assert report["verification_check"] == {"checked": False, "ok": None, "problems": []}
        entry = report["instruments"][0]
        assert entry["research_readable"] is False
        assert entry["research_blockers"] == ["final-candle closure is unknown; no interval can be read"]
        assert "1 missing 5m bars inside the declared coverage" in entry["research_limitations"]

    def test_verify_accepts_an_intact_pack(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        report = pack_data.inspect_pack(root, verify=True)
        assert report["verification_check"] == {"checked": True, "ok": True, "problems": []}

    def test_verify_rejects_a_corrupted_file(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        target = root / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet"
        payload = bytearray(target.read_bytes())
        payload[len(payload) // 2] ^= 0xFF
        target.write_bytes(bytes(payload))
        report = pack_data.inspect_pack(root, verify=True)
        assert report["verification_check"]["ok"] is False
        assert any("SHA-256" in problem for problem in report["verification_check"]["problems"])

    def test_verify_rejects_mismatched_coverage(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)

        def shrink(manifest):
            entry = manifest["instruments"][0]
            entry["row_count"] = 47
            entry["missing_bar_count"] = 1

        mutate_manifest(root, shrink)
        report = pack_data.inspect_pack(root, verify=True)
        assert report["verification_check"]["ok"] is False
        assert any("do not match the declared row_count" in item for item in report["verification_check"]["problems"])

    def test_verify_reports_a_missing_file(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=12)
        (root / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet").unlink()
        report = pack_data.inspect_pack(root, verify=True)
        assert report["verification_check"]["ok"] is False
        assert any("is missing" in item for item in report["verification_check"]["problems"])

    def test_missing_manifest_is_reported(self, tmp_path):
        root = tmp_path / "pack"
        root.mkdir()
        with pytest.raises(PatternLabDataError, match="no readable pack manifest"):
            pack_data.inspect_pack(root)


class TestPublicationVerificationPolicy:
    """T01 publishers never emit unverified quote volume, whatever the schema allows."""

    @staticmethod
    def _verification(**overrides):
        payload = pack_manifest.build_verification(
            volume_quote_verified=True, volume_quote_evidence="synthetic quote turnover"
        )
        payload.update(overrides)
        return payload

    @pytest.mark.parametrize(
        "verification",
        [
            {"volume_quote_verified": False, "volume_quote_evidence": "units were never established"},
            {"volume_quote_verified": "true"},
            {"volume_quote_verified": 1},
        ],
    )
    def test_unverified_quote_volume_leaves_no_ready_manifest(self, tmp_path, verification):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(12)
        payload = self._verification(**verification)
        if "volume_quote_verified" in verification and verification["volume_quote_verified"] is False:
            payload["volume_quote_evidence"] = verification["volume_quote_evidence"]
        with pytest.raises(PatternLabDataError, match="volume_quote_verified"):
            publish(root, [instrument_source(stamps, values, verification=payload)])
        assert not (root / pack_manifest.MANIFEST_NAME).exists()
        assert not any((root / "ohlcv").glob("*.parquet"))

    def test_missing_flag_names_the_instrument(self, tmp_path):
        stamps, values = synthetic_series(12)
        payload = self._verification()
        del payload["volume_quote_verified"]
        with pytest.raises(PatternLabDataError, match="TEST_AAA-USDT-SWAP.volume_quote_verified"):
            publish(tmp_path / "pack", [instrument_source(stamps, values, verification=payload)])

    def test_verified_volume_with_unknown_closure_stays_archivable(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(48)
        publish(root, [instrument_source(stamps, values, closed_before_ms=None)])
        entry = pack_manifest.read_manifest(root)["instruments"][0]
        assert entry["verification"]["volume_quote_verified"] is True
        assert entry["verification"]["closed_before_utc"] is None
        assert pack_data.inspect_pack(root, verify=True)["verification_check"]["ok"] is True
        with pytest.raises(PatternLabDataError, match="closure is unknown"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )


class TestOffsetManifestRoundTrip:
    """An accepted explicit-offset manifest must verify and read like a canonical one."""

    @staticmethod
    def _as_offset(value, hours):
        moment = pack_manifest.parse_utc(value, "timestamp").astimezone(timezone(timedelta(hours=hours)))
        return moment.isoformat()

    def test_equivalent_offsets_verify_and_load(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        canonical = pack_manifest.read_manifest(root)["instruments"][0]

        def shift(manifest):
            entry = manifest["instruments"][0]
            entry["first_open_utc"] = self._as_offset(entry["first_open_utc"], 2)
            entry["last_open_utc"] = self._as_offset(entry["last_open_utc"], -5)
            entry["coverage_end_utc"] = self._as_offset(entry["coverage_end_utc"], 9)
            entry["verification"]["closed_before_utc"] = self._as_offset(
                entry["verification"]["closed_before_utc"], 3
            )

        mutate_manifest(root, shift)
        reread = pack_manifest.read_manifest(root)["instruments"][0]
        assert reread["first_open_utc"] == canonical["first_open_utc"]
        assert reread["coverage_end_utc"] == canonical["coverage_end_utc"]
        assert reread["verification"]["closed_before_utc"] == canonical["verification"]["closed_before_utc"]

        report = pack_data.inspect_pack(root, verify=True)
        assert report["verification_check"] == {"checked": True, "ok": True, "problems": []}
        loaded = pack_data.load_slice(
            root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 288 * STEP_MS)
        )
        assert len(loaded.bars) == 288


class TestGeneratedPackEvidence:
    """The rendered pack README must carry the evidence behind its verification flags."""

    def test_evidence_and_references_are_rendered(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(30, drop_slots=[7])
        source = pack_manifest.build_source_metadata(
            input_format="npz",
            input_dtype="float32",
            source_reference="5m/AAA.npz in the prototype NPZ pack",
            volume_unit_evidence="OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
            source_hash="b" * 64,
            extra={"evidence_source": "Source review plus the OKX candle field definition"},
        )
        verified = pack_manifest.build_verification(
            volume_quote_verified=True,
            volume_quote_evidence="OKX swap volCcyQuote; fetch_base.py reads candle field 7.",
            closed_before_utc=utc(ANCHOR_MS + 30 * STEP_MS),
            closure_evidence="Every retained bar precedes the recorded fetch cutoff.",
            closure_source="Prototype fetch log reviewed at import time",
        )
        unknown = pack_manifest.build_verification(
            volume_quote_verified=True, volume_quote_evidence="Bybit linear turnover, kline field 6."
        )
        publish(
            root,
            [
                pack_data.InstrumentSource(
                    symbol="AAA", venue="OKX", contract="AAA-USDT-SWAP", quote_currency="USDT",
                    roles=["trading"], timestamps=stamps, ohlcv=values,
                    source=source, verification=verified,
                ),
                instrument_source(
                    stamps, values, symbol="ENA", venue="Bybit", contract="ENAUSDT",
                    closed_before_ms=None, verification=unknown,
                    ),
            ],
        )
        readme = (root / "README.md").read_text(encoding="utf-8")

        assert "### OKX_AAA-USDT-SWAP" in readme
        assert "OKX swap volCcyQuote; fetch_base.py reads candle field 7." in readme
        assert "Source review plus the OKX candle field definition" in readme
        assert "5m/AAA.npz in the prototype NPZ pack" in readme
        assert "b" * 64 in readme
        assert "Every retained bar precedes the recorded fetch cutoff." in readme
        assert "Prototype fetch log reviewed at import time" in readme
        assert "Quote volume verified: yes" in readme
        assert "1 missing 5m bars inside the declared coverage" in readme

        assert "### BYBIT_ENAUSDT" in readme
        assert "Bybit linear turnover, kline field 6." in readme
        assert "Closed before UTC: unknown" in readme
        assert "every research read of this instrument is refused" in readme
        assert "performs no exchange verification of" in readme

    def test_absent_provenance_is_not_fabricated(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=12)
        readme = (root / "README.md").read_text(encoding="utf-8")
        assert "Source SHA-256" not in readme
        assert "Evidence source" not in readme
        assert "Source reference: tests/pattern_lab/_helpers.py synthetic generator" in readme
