"""Research input identity: the frozen encoding, invariances and invalidations."""

from __future__ import annotations

import json

import numpy as np
import pytest

from tools.pattern_lab import data as pack_data
from tools.pattern_lab import manifest as pack_manifest

from ._helpers import (
    ANCHOR_MS,
    STEP_MS,
    instrument_source,
    mutate_manifest,
    publish,
    synthetic_series,
    utc,
)

# Independently computed reference digest for the section 9 golden vector.
GOLDEN_DIGEST = "260319787a122516ce732bf93bf8b5c68aec4881e2d47e8e23911d3c161548b7"
GOLDEN_HEADER_BYTES = 325
GOLDEN_PAYLOAD_BYTES = 437

GOLDEN_TIMESTAMPS = np.array([0, 300_000], dtype=np.int64)
GOLDEN_OHLCV = np.array([[10.0, 12.0, 9.0, 11.0, 100.0], [11.0, 13.0, 10.0, 12.0, 0.0]], dtype=np.float64)


def golden_header():
    return pack_data.fingerprint_header(
        instrument_id="TEST_AAA-USDT",
        venue="TEST",
        contract="AAA-USDT",
        quote_currency="USDT",
        timeframe_minutes=5,
        start_ms=0,
        end_ms=600_000,
        warmup_start_ms=0,
    )


class TestFrozenEncoding:
    def test_golden_vector(self):
        digest = pack_data.input_fingerprint(
            header=golden_header(), timestamps=GOLDEN_TIMESTAMPS, ohlcv=GOLDEN_OHLCV
        )
        assert digest == GOLDEN_DIGEST
        assert len(digest) == 64
        assert digest == digest.lower()

    def test_canonical_header_is_closed_and_sized(self):
        header = golden_header()
        assert set(header) == {
            "fingerprint_version",
            "instrument_id",
            "venue",
            "contract",
            "quote_currency",
            "volume_unit",
            "base_timeframe_minutes",
            "timeframe_minutes",
            "start_ms",
            "end_ms",
            "warmup_start_ms",
            "resampling_policy",
            "missing_bar_policy",
        }
        assert header["volume_unit"] == "quote_turnover"
        assert header["resampling_policy"] == "utc_epoch_complete_v1"
        assert header["missing_bar_policy"] == "no_fill_v1"
        assert header["fingerprint_version"] == 1
        assert header["base_timeframe_minutes"] == 5
        encoded = json.dumps(
            header, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        assert len(encoded) == GOLDEN_HEADER_BYTES
        assert 8 + len(encoded) + 8 + 2 * 8 + 2 * 5 * 8 == GOLDEN_PAYLOAD_BYTES

    @pytest.mark.parametrize("field", ["start_ms", "end_ms", "warmup_start_ms", "timeframe_minutes"])
    @pytest.mark.parametrize("value", [True, 5.0, "5"])
    def test_integer_header_fields_reject_booleans_and_floats(self, field, value):
        from tools.pattern_lab import PatternLabDataError

        arguments = {
            "instrument_id": "TEST_AAA-USDT",
            "venue": "TEST",
            "contract": "AAA-USDT",
            "quote_currency": "USDT",
            "timeframe_minutes": 5,
            "start_ms": 0,
            "end_ms": 600_000,
            "warmup_start_ms": 0,
        }
        arguments[field] = value
        with pytest.raises(PatternLabDataError):
            pack_data.fingerprint_header(**arguments)

    def test_equivalent_representations_produce_the_same_digest(self):
        header = golden_header()
        expected = pack_data.input_fingerprint(
            header=header, timestamps=GOLDEN_TIMESTAMPS, ohlcv=GOLDEN_OHLCV
        )

        strided_stamps = np.repeat(GOLDEN_TIMESTAMPS, 2)[::2]
        strided_values = np.repeat(GOLDEN_OHLCV, 2, axis=0)[::2]
        assert not strided_values.flags["C_CONTIGUOUS"]
        assert pack_data.input_fingerprint(
            header=header, timestamps=strided_stamps, ohlcv=strided_values
        ) == expected

        assert pack_data.input_fingerprint(
            header=header,
            timestamps=GOLDEN_TIMESTAMPS.astype(">i8"),
            ohlcv=GOLDEN_OHLCV.astype(">f8"),
        ) == expected

        promoted = GOLDEN_OHLCV.astype(np.float32).astype(np.float64)
        assert np.array_equal(promoted, GOLDEN_OHLCV)
        assert pack_data.input_fingerprint(header=header, timestamps=GOLDEN_TIMESTAMPS, ohlcv=promoted) == expected

        signed_zero = GOLDEN_OHLCV.copy()
        signed_zero[1, 4] = -0.0
        assert np.signbit(signed_zero[1, 4])
        assert pack_data.input_fingerprint(header=header, timestamps=GOLDEN_TIMESTAMPS, ohlcv=signed_zero) == expected

    def test_changed_values_and_header_fields_change_the_digest(self):
        header = golden_header()
        expected = pack_data.input_fingerprint(
            header=header, timestamps=GOLDEN_TIMESTAMPS, ohlcv=GOLDEN_OHLCV
        )
        edited = GOLDEN_OHLCV.copy()
        edited[1, 4] = 1.0
        assert pack_data.input_fingerprint(header=header, timestamps=GOLDEN_TIMESTAMPS, ohlcv=edited) != expected

        dropped = pack_data.input_fingerprint(
            header=header, timestamps=GOLDEN_TIMESTAMPS[:1], ohlcv=GOLDEN_OHLCV[:1]
        )
        assert dropped != expected

        moved = pack_data.input_fingerprint(
            header=header, timestamps=GOLDEN_TIMESTAMPS + STEP_MS, ohlcv=GOLDEN_OHLCV
        )
        assert moved != expected


class TestSliceIdentity:
    @staticmethod
    def _slice(root, **overrides):
        request = {
            "start": utc(ANCHOR_MS + 24 * STEP_MS),
            "end": utc(ANCHOR_MS + 120 * STEP_MS),
            "warmup_start": utc(ANCHOR_MS),
            "timeframe_minutes": 30,
        }
        request.update(overrides)
        return pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", **request)

    def test_unused_append_and_backfill_do_not_change_identity(self, tmp_path):
        base = tmp_path / "base"
        publish(base, [instrument_source(*synthetic_series(144))])
        grown = tmp_path / "grown"
        publish(grown, [instrument_source(*synthetic_series(240, first_slot=-48))], revision=4)

        original = self._slice(base)
        extended = self._slice(grown)
        assert extended.input_fingerprint == original.input_fingerprint
        assert extended.bars.equals(original.bars)
        # Physical provenance does change with the republished pack.
        assert extended.physical["file_sha256"] != original.physical["file_sha256"]
        assert extended.physical["manifest_revision"] == 4

    def test_relocating_the_pack_does_not_change_identity(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "nested" / "second"
        for root in (first, second):
            publish(root, [instrument_source(*synthetic_series(144))])
        assert self._slice(second).input_fingerprint == self._slice(first).input_fingerprint

    def test_added_roles_do_not_change_the_data_fingerprint(self, tmp_path):
        trading = tmp_path / "trading"
        both = tmp_path / "both"
        stamps, values = synthetic_series(144)
        publish(trading, [instrument_source(stamps, values, roles=["trading"])])
        publish(both, [instrument_source(stamps, values, roles=["trading", "factor"])])
        assert pack_manifest.read_manifest(both)["instruments"][0]["roles"] == ["factor", "trading"]
        assert self._slice(both).input_fingerprint == self._slice(trading).input_fingerprint

    def test_an_extra_unused_column_does_not_change_identity(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        import pyarrow.parquet as pq

        base = tmp_path / "base"
        widened = tmp_path / "widened"
        stamps, values = synthetic_series(144)
        for root in (base, widened):
            publish(root, [instrument_source(stamps, values)])
        original = self._slice(base)

        path = widened / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet"
        table = pq.read_table(path)
        table = table.append_column(
            pa.field("trade_count", pa.int64()), pa.array(np.arange(table.num_rows, dtype=np.int64))
        )
        pq.write_table(table, path, compression="zstd", row_group_size=8192)
        digest = pack_manifest.file_sha256(path)
        mutate_manifest(widened, lambda m: m["instruments"][0].__setitem__("sha256", digest))

        assert pack_data.inspect_pack(widened, verify=True)["verification_check"]["ok"] is True
        assert self._slice(widened).input_fingerprint == original.input_fingerprint

    def test_an_equivalent_parquet_encoding_does_not_change_identity(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        base = tmp_path / "base"
        recoded = tmp_path / "recoded"
        stamps, values = synthetic_series(144)
        for root in (base, recoded):
            publish(root, [instrument_source(stamps, values)])
        original = self._slice(base)

        path = recoded / "ohlcv" / "TEST_AAA-USDT-SWAP_5m.parquet"
        pq.write_table(pq.read_table(path), path, compression="none", row_group_size=16)
        digest = pack_manifest.file_sha256(path)
        mutate_manifest(recoded, lambda m: m["instruments"][0].__setitem__("sha256", digest))

        reread = self._slice(recoded)
        assert reread.input_fingerprint == original.input_fingerprint
        assert reread.bars.equals(original.bars)

    def test_consumed_edits_units_gaps_and_warmup_invalidate_identity(self, tmp_path):
        base = tmp_path / "base"
        stamps, values = synthetic_series(144)
        publish(base, [instrument_source(stamps, values)])
        original = self._slice(base)

        edited_root = tmp_path / "edited"
        edited = values.copy()
        edited[40, 4] += 1.0
        publish(edited_root, [instrument_source(stamps, edited)])
        assert self._slice(edited_root).input_fingerprint != original.input_fingerprint

        units_root = tmp_path / "units"
        publish(units_root, [instrument_source(stamps, values, quote_currency="USDC")])
        assert self._slice(units_root).input_fingerprint != original.input_fingerprint

        gap_root = tmp_path / "gapped"
        gapped_stamps, gapped_values = synthetic_series(144, drop_slots=[40])
        publish(gap_root, [instrument_source(gapped_stamps, gapped_values)])
        gapped = self._slice(gap_root)
        assert gapped.input_fingerprint != original.input_fingerprint
        assert gapped.base_gap_count == 1

        assert self._slice(base, warmup_start=utc(ANCHOR_MS + 12 * STEP_MS)).input_fingerprint != (
            original.input_fingerprint
        )
        assert self._slice(base, timeframe_minutes=60).input_fingerprint != original.input_fingerprint

    def test_identity_covers_omitted_incomplete_groups(self, tmp_path):
        """A repaired slot must change identity even when its group was omitted."""
        gapped = tmp_path / "gapped"
        repaired = tmp_path / "repaired"
        publish(gapped, [instrument_source(*synthetic_series(144, drop_slots=[40]))])
        publish(repaired, [instrument_source(*synthetic_series(144))])
        gapped_slice = self._slice(gapped)
        repaired_slice = self._slice(repaired)
        assert gapped_slice.omitted_group_count == 1
        assert repaired_slice.omitted_group_count == 0
        assert gapped_slice.input_fingerprint != repaired_slice.input_fingerprint

    def test_growing_closure_evidence_is_not_hashed(self, tmp_path):
        early = tmp_path / "early"
        later = tmp_path / "later"
        stamps, values = synthetic_series(144)
        publish(early, [instrument_source(stamps, values, closed_before_ms=ANCHOR_MS + 130 * STEP_MS)])
        publish(later, [instrument_source(stamps, values, closed_before_ms=ANCHOR_MS + 144 * STEP_MS)])
        assert self._slice(later).input_fingerprint == self._slice(early).input_fingerprint
