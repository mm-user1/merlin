"""Fixed-range reader, closure gating, resampling and concurrency detection."""

from __future__ import annotations

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import manifest as pack_manifest

from ._helpers import (
    ANCHOR_MS,
    STEP_MS,
    instrument_source,
    mutate_manifest,
    publish,
    single_pack,
    synthetic_series,
    utc,
)

HOUR_MS = 3_600_000


def expected_group(first_slot: int, slots: int) -> list[float]:
    """Hand-computed aggregate of the synthetic slot-keyed bars."""
    last = first_slot + slots - 1
    return [
        100.0 + first_slot,
        102.0 + last,
        99.0 + first_slot,
        101.0 + last,
        float(sum(1000 + index for index in range(first_slot, last + 1))),
    ]


class TestRanges:
    def test_half_open_interval_returns_exact_boundaries(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        start = ANCHOR_MS + 12 * STEP_MS
        end = ANCHOR_MS + 24 * STEP_MS
        loaded = pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end))

        stamps = loaded.bars.index.asi8 // 1_000_000
        assert stamps[0] == start
        assert stamps[-1] == end - STEP_MS
        assert len(loaded.bars) == 12
        assert loaded.research_start_index == 0
        assert bool(loaded.research_mask.all())
        assert loaded.base_row_count == 12
        assert loaded.base_gap_count == 0
        assert loaded.omitted_group_count == 0

    def test_row_group_neighbours_never_reach_the_result(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = single_pack(root, slot_count=2000)
        start = ANCHOR_MS + 500 * STEP_MS
        end = ANCHOR_MS + 520 * STEP_MS
        loaded = pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end))
        assert len(loaded.bars) == 20
        assert np.array_equal(loaded.bars["open"].to_numpy(), values[500:520, 0])

    def test_warmup_is_consumed_and_distinguishable(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        warmup = ANCHOR_MS
        start = ANCHOR_MS + 24 * STEP_MS
        end = ANCHOR_MS + 48 * STEP_MS
        loaded = pack_data.load_slice(
            root, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end), warmup_start=utc(warmup)
        )
        assert loaded.base_row_count == 48
        assert loaded.research_start_index == 24
        assert int(np.count_nonzero(loaded.research_mask)) == 24
        assert pack_data.research_bars(loaded).index[0].value // 1_000_000 == start

    def test_timezone_offsets_normalize_to_the_same_slice(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        start = ANCHOR_MS + 12 * STEP_MS
        end = ANCHOR_MS + 24 * STEP_MS
        utc_slice = pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end))
        shifted_start = pack_manifest.from_epoch_ms(start).astimezone(
            pack_manifest.from_epoch_ms(start).tzinfo
        )
        offset_slice = pack_data.load_slice(
            root,
            "TEST_AAA-USDT-SWAP",
            start=shifted_start.isoformat(),
            end=pack_manifest.from_epoch_ms(end),
        )
        assert offset_slice.input_fingerprint == utc_slice.input_fingerprint
        assert offset_slice.bars.equals(utc_slice.bars)

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00Z"}, "naive"),
            ({"start": None, "end": None}, "expected an ISO-8601"),
            ({"timeframe_minutes": 7}, "multiple of 5"),
            ({"timeframe_minutes": 30.0}, "expected an integer"),
            ({"timeframe_minutes": True}, "expected an integer"),
        ],
    )
    def test_invalid_requests_are_rejected(self, tmp_path, kwargs, message):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        request = {"start": utc(ANCHOR_MS), "end": utc(ANCHOR_MS + 12 * STEP_MS)}
        request.update(kwargs)
        with pytest.raises(PatternLabDataError, match=message):
            pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", **request)

    def test_reversed_and_empty_intervals_are_rejected(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        for start, end in ((ANCHOR_MS + 12 * STEP_MS, ANCHOR_MS), (ANCHOR_MS, ANCHOR_MS)):
            with pytest.raises(PatternLabDataError, match="start < end"):
                pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end))

    def test_warmup_after_start_is_rejected(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        with pytest.raises(PatternLabDataError, match="warmup_start <= start"):
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=utc(ANCHOR_MS + 12 * STEP_MS),
                end=utc(ANCHOR_MS + 24 * STEP_MS),
                warmup_start=utc(ANCHOR_MS + 18 * STEP_MS),
            )

    @pytest.mark.parametrize("boundary", ["start", "end", "warmup_start"])
    def test_every_boundary_must_align_to_the_requested_timeframe(self, tmp_path, boundary):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        request = {
            "warmup_start": utc(ANCHOR_MS),
            "start": utc(ANCHOR_MS + 12 * STEP_MS),
            "end": utc(ANCHOR_MS + 24 * STEP_MS),
            "timeframe_minutes": 30,
        }
        request[boundary] = utc(pack_manifest.to_epoch_ms(request[boundary]) + 2 * STEP_MS)
        with pytest.raises(PatternLabDataError, match="not aligned to the 30m UTC epoch grid"):
            pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", **request)

    def test_missing_history_or_end_coverage_fails(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        with pytest.raises(PatternLabDataError, match="coverage starts at"):
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=utc(ANCHOR_MS),
                end=utc(ANCHOR_MS + 12 * STEP_MS),
                warmup_start=utc(ANCHOR_MS - 6 * STEP_MS),
            )
        with pytest.raises(PatternLabDataError, match="coverage ends at"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 96 * STEP_MS)
            )

    def test_an_unrelated_instrument_cannot_truncate_the_selected_slice(self, tmp_path):
        root = tmp_path / "pack"
        long_stamps, long_values = synthetic_series(288)
        short_stamps, short_values = synthetic_series(24)
        publish(
            root,
            [
                instrument_source(long_stamps, long_values, symbol="AAA", contract="AAA-USDT-SWAP"),
                instrument_source(short_stamps, short_values, symbol="BBB", contract="BBB-USDT-SWAP"),
            ],
        )
        loaded = pack_data.load_slice(
            root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 288 * STEP_MS)
        )
        assert len(loaded.bars) == 288

    def test_unknown_instruments_are_named_in_the_error(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=12)
        with pytest.raises(PatternLabDataError, match="TEST_MISSING"):
            pack_data.load_slice(
                root, "TEST_MISSING", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + STEP_MS)
            )


class TestClosureAndVerification:
    def test_cutoff_equal_to_end_is_accepted(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(48)
        end = ANCHOR_MS + 24 * STEP_MS
        publish(root, [instrument_source(stamps, values, closed_before_ms=end)])
        loaded = pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(end))
        assert len(loaded.bars) == 24

    @pytest.mark.parametrize("offset_slots", [1, 12])
    def test_earlier_cutoff_is_rejected(self, tmp_path, offset_slots):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(48)
        end = ANCHOR_MS + 24 * STEP_MS
        publish(root, [instrument_source(stamps, values, closed_before_ms=end - offset_slots * STEP_MS)])
        with pytest.raises(PatternLabDataError, match="not certified closed"):
            pack_data.load_slice(root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(end))

    def test_warmup_rows_are_covered_by_the_same_certification(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(48)
        start = ANCHOR_MS + 24 * STEP_MS
        end = ANCHOR_MS + 36 * STEP_MS
        publish(root, [instrument_source(stamps, values, closed_before_ms=start)])
        with pytest.raises(PatternLabDataError, match="not certified closed"):
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=utc(start),
                end=utc(end),
                warmup_start=utc(ANCHOR_MS),
            )

    def test_unknown_closure_is_preserved_and_refuses_research_reads(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = synthetic_series(48)
        publish(root, [instrument_source(stamps, values, closed_before_ms=None)])
        entry = pack_manifest.read_manifest(root)["instruments"][0]
        assert entry["verification"]["closed_before_utc"] is None
        assert entry["row_count"] == 48
        # A later wall clock cannot promote rows whose closure was never evidenced.
        with pytest.raises(PatternLabDataError, match="closure is unknown"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )

    def test_unverified_quote_volume_state_is_rejected(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)

        def unverify(manifest):
            manifest["instruments"][0]["verification"]["volume_quote_verified"] = False

        mutate_manifest(root, unverify)
        with pytest.raises(PatternLabDataError, match="quote-volume units are not verified"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )


class TestResampling:
    @pytest.mark.parametrize("minutes, slots", [(5, 1), (10, 2), (30, 6), (60, 12)])
    def test_hand_computed_aggregates(self, tmp_path, minutes, slots):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        start = ANCHOR_MS
        end = ANCHOR_MS + 144 * STEP_MS
        loaded = pack_data.load_slice(
            root, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end), timeframe_minutes=minutes
        )
        assert len(loaded.bars) == 144 // slots
        assert loaded.bars.iloc[0].tolist() == expected_group(0, slots)
        assert loaded.bars.iloc[1].tolist() == expected_group(slots, slots)
        assert loaded.bars.iloc[-1].tolist() == expected_group(144 - slots, slots)
        assert loaded.omitted_group_count == 0
        assert int(np.count_nonzero(loaded.segment_start)) == 1

    def test_five_minute_reads_return_canonical_values_unchanged(self, tmp_path):
        root = tmp_path / "pack"
        stamps, values = single_pack(root, slot_count=64)
        loaded = pack_data.load_slice(
            root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 64 * STEP_MS)
        )
        assert np.array_equal(loaded.bars.to_numpy(), values)

    def test_a_missing_slot_invalidates_only_its_group(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=144, drop_slots=[7])
        loaded = pack_data.load_slice(
            root,
            "TEST_AAA-USDT-SWAP",
            start=utc(ANCHOR_MS),
            end=utc(ANCHOR_MS + 144 * STEP_MS),
            timeframe_minutes=30,
        )
        stamps = loaded.bars.index.asi8 // 1_000_000
        assert loaded.omitted_group_count == 1
        assert len(loaded.bars) == 23
        assert ANCHOR_MS + 6 * STEP_MS not in set(stamps.tolist())
        assert loaded.bars.iloc[0].tolist() == expected_group(0, 6)
        assert loaded.bars.iloc[1].tolist() == expected_group(12, 6)
        assert loaded.base_gap_count == 1

    def test_gaps_start_new_segments(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48, drop_slots=[10, 11])
        loaded = pack_data.load_slice(
            root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 48 * STEP_MS)
        )
        stamps = loaded.bars.index.asi8 // 1_000_000
        assert len(loaded.bars) == 46
        assert loaded.base_gap_count == 2
        assert bool(loaded.segment_start[0])
        starts = stamps[loaded.segment_start]
        assert starts.tolist() == [ANCHOR_MS, ANCHOR_MS + 12 * STEP_MS]

    def test_warmup_gap_is_visible_as_a_segment_before_research(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48, drop_slots=[4])
        loaded = pack_data.load_slice(
            root,
            "TEST_AAA-USDT-SWAP",
            start=utc(ANCHOR_MS + 12 * STEP_MS),
            end=utc(ANCHOR_MS + 24 * STEP_MS),
            warmup_start=utc(ANCHOR_MS),
        )
        assert loaded.base_row_count == 23
        assert loaded.base_gap_count == 1
        assert int(np.count_nonzero(loaded.segment_start)) == 2
        assert loaded.research_start_index == 11

    def test_no_complete_research_bar_is_a_coverage_error(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48, drop_slots=[13, 19])
        with pytest.raises(PatternLabDataError, match="no complete 30m research bar"):
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=utc(ANCHOR_MS + 12 * STEP_MS),
                end=utc(ANCHOR_MS + 24 * STEP_MS),
                timeframe_minutes=30,
            )

    def test_no_rows_at_all_is_a_coverage_error(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48, drop_slots=range(12, 24))
        with pytest.raises(PatternLabDataError, match="no complete 5m research bar"):
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=utc(ANCHOR_MS + 12 * STEP_MS),
                end=utc(ANCHOR_MS + 24 * STEP_MS),
            )


class TestPackStateDetection:
    def test_update_marker_blocks_reads(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        pack_manifest.update_marker_path(root).write_text("{}", encoding="utf-8", newline="\n")
        with pytest.raises(PatternLabDataError, match="is present"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )

    def test_incomplete_manifest_blocks_reads(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        mutate_manifest(root, lambda manifest: manifest.__setitem__("state", "incomplete"))
        with pytest.raises(PatternLabDataError, match="only a ready pack"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )

    def test_missing_manifest_blocks_reads(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        pack_manifest.manifest_path(root).unlink()
        with pytest.raises(PatternLabDataError, match="publication did not complete"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )

    @pytest.mark.parametrize(
        "change, message",
        [
            (lambda manifest: manifest.__setitem__("revision", 2), "changed during the read"),
            (lambda manifest: manifest.__setitem__("state", "incomplete"), "changed during the read"),
            (None, "appeared during the read"),
        ],
    )
    def test_concurrent_changes_fail_instead_of_mixing_versions(self, tmp_path, monkeypatch, change, message):
        root = tmp_path / "pack"
        single_pack(root, slot_count=48)
        original = pack_data.read_ohlcv_rows

        def interfering(*args, **kwargs):
            result = original(*args, **kwargs)
            if change is None:
                pack_manifest.update_marker_path(root).write_text("{}", encoding="utf-8", newline="\n")
            else:
                mutate_manifest(root, change)
            return result

        monkeypatch.setattr(pack_data, "read_ohlcv_rows", interfering)
        with pytest.raises(PatternLabDataError, match=message):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 12 * STEP_MS)
            )


class TestPublicResampler:
    """Direct calls to the documented resampler must enforce its own assumptions."""

    VALID_ROWS = np.array([[10.0, 12.0, 9.0, 11.0, 100.0], [11.0, 13.0, 10.0, 12.0, 5.0]])

    def test_duplicate_timestamps_cannot_become_a_complete_bar(self):
        with pytest.raises(PatternLabDataError, match="duplicate timestamps"):
            pack_data.resample_complete_groups(np.array([0, 0]), self.VALID_ROWS, 10)

    @pytest.mark.parametrize(
        "timestamps, message",
        [
            (np.array([0, 60_000]), "grid"),
            (np.array([300_000, 0]), "strictly increasing"),
            (np.array([0.0, 300_000.0]), "integer epoch"),
            (np.array([[0], [300_000]]), "one-dimensional"),
            (np.array([0, 300_000, 600_000]), "do not match"),
        ],
    )
    def test_invalid_direct_input_is_rejected(self, timestamps, message):
        with pytest.raises(PatternLabDataError, match=message):
            pack_data.resample_complete_groups(timestamps, self.VALID_ROWS, 10)

    def test_fractional_timestamps_are_not_truncated(self):
        stamps = np.array(["1970-01-01T00:00:00.000100", "1970-01-01T00:05:00.000000"], dtype="datetime64[us]")
        with pytest.raises(PatternLabDataError, match="sub-millisecond"):
            pack_data.resample_complete_groups(stamps, self.VALID_ROWS, 10)

    def test_invalid_values_are_rejected(self):
        broken = self.VALID_ROWS.copy()
        broken[0, 2] = 1e9
        with pytest.raises(PatternLabDataError, match="low <= min"):
            pack_data.resample_complete_groups(np.array([0, 300_000]), broken, 10)

    def test_well_formed_empty_input_is_preserved(self):
        stamps, values, omitted = pack_data.resample_complete_groups(
            np.empty(0, dtype=np.int64), np.empty((0, 5), dtype=np.float64), 30
        )
        assert stamps.size == 0
        assert values.shape == (0, 5)
        assert omitted == 0

    @pytest.mark.parametrize("ohlcv", [np.empty(0), np.empty((0, 4))])
    def test_malformed_empty_shapes_fail(self, ohlcv):
        with pytest.raises(PatternLabDataError):
            pack_data.resample_complete_groups(np.empty(0, dtype=np.int64), ohlcv, 30)

    def test_valid_direct_call_keeps_group_and_signed_zero_semantics(self):
        stamps = ANCHOR_MS + np.array([0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11], dtype=np.int64) * STEP_MS
        values = np.column_stack(
            [
                100.0 + np.arange(11),
                102.0 + np.arange(11),
                99.0 + np.arange(11),
                101.0 + np.arange(11),
                np.full(11, -0.0),
            ]
        )
        original = values.copy()
        groups, aggregated, omitted = pack_data.resample_complete_groups(stamps, values, 30)
        assert groups.tolist() == [ANCHOR_MS]
        assert aggregated[0].tolist() == [100.0, 107.0, 99.0, 106.0, 0.0]
        assert not np.signbit(aggregated[0, 4])
        assert omitted == 1
        assert np.array_equal(np.copysign(1.0, values[:, 4]), np.copysign(1.0, original[:, 4]))
