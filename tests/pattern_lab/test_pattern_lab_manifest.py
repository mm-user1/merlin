"""Identifier, path, manifest and README contracts for Pattern Lab packs."""

from __future__ import annotations

from datetime import datetime

import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import manifest as pack_manifest

from ._helpers import ANCHOR_MS, STEP_MS, utc


def _entry(**overrides):
    entry = pack_manifest.build_instrument_entry(
        symbol="AAA",
        venue="Test",
        contract="aaa-usdt-swap",
        quote_currency="USDT",
        roles=["trading"],
        row_count=3,
        first_open_ms=ANCHOR_MS,
        last_open_ms=ANCHOR_MS + 2 * STEP_MS,
        missing_bar_count=0,
        sha256="a" * 64,
        source=pack_manifest.build_source_metadata(
            input_format="synthetic",
            input_dtype="float64",
            source_reference="unit test",
            volume_unit_evidence="synthetic quote turnover",
        ),
        verification=pack_manifest.build_verification(
            volume_quote_verified=True, volume_quote_evidence="synthetic"
        ),
    )
    entry.update(overrides)
    return entry


def _manifest(entries=None, **overrides):
    document = {
        "schema_version": 1,
        "revision": 1,
        "state": "ready",
        "generated_utc": "2026-09-14T00:00:00Z",
        "base_timeframe_minutes": 5,
        "volume_unit": "quote_turnover",
        "universe": pack_manifest.build_universe(notes="unit test"),
        "instruments": entries if entries is not None else [_entry()],
    }
    document.update(overrides)
    return document


class TestIdentifiers:
    def test_id_parts_are_uppercased_and_joined(self):
        assert pack_manifest.build_instrument_id("Bybit", "ENAUSDT") == "BYBIT_ENAUSDT"
        assert pack_manifest.build_instrument_id("okx", "link-usdt-swap") == "OKX_LINK-USDT-SWAP"

    @pytest.mark.parametrize(
        "venue, contract",
        [
            ("OK X", "AAA"),
            ("OKX", "AAA USDT"),
            ("OK_X", "AAA"),
            ("OKX", "AAA_USDT"),
            ("OKX", "-AAA"),
            ("ÖKX", "AAA"),
            ("", "AAA"),
            ("OKX", ""),
            (5, "AAA"),
            (True, "AAA"),
        ],
    )
    def test_unsafe_id_parts_are_rejected(self, venue, contract):
        with pytest.raises(PatternLabDataError):
            pack_manifest.build_instrument_id(venue, contract)

    def test_full_identifier_requires_exactly_one_delimiter(self):
        assert pack_manifest.normalize_instrument_id("okx_aaa") == "OKX_AAA"
        for value in ("OKX", "OKX_AAA_BBB", ""):
            with pytest.raises(PatternLabDataError):
                pack_manifest.normalize_instrument_id(value)

    def test_file_name_has_no_date_component(self):
        assert pack_manifest.instrument_relative_file("OKX_LINK-USDT-SWAP") == (
            "ohlcv/OKX_LINK-USDT-SWAP_5m.parquet"
        )


class TestPackPaths:
    @pytest.mark.parametrize(
        "value",
        [
            "/abs/ohlcv/A_B_5m.parquet",
            "ohlcv\\A_B_5m.parquet",
            "ohlcv/../A_B_5m.parquet",
            "../ohlcv/A_B_5m.parquet",
            "other/A_B_5m.parquet",
            "ohlcv/nested/A_B_5m.parquet",
            "ohlcv/A_B_5m.txt",
            "C:/ohlcv/A_B_5m.parquet",
            "",
        ],
    )
    def test_invalid_relative_paths_are_rejected(self, value):
        with pytest.raises(PatternLabDataError):
            pack_manifest.validate_relative_file(value)

    def test_resolved_symlink_escape_is_rejected(self, tmp_path):
        root = tmp_path / "pack"
        outside = tmp_path / "outside"
        outside.mkdir()
        root.mkdir()
        (root / "ohlcv").symlink_to(outside, target_is_directory=True)
        with pytest.raises(PatternLabDataError, match="resolves outside"):
            pack_manifest.resolve_pack_path(root, "ohlcv/A_B_5m.parquet")

    def test_resolved_path_inside_the_root_is_accepted(self, tmp_path):
        root = tmp_path / "pack"
        (root / "ohlcv").mkdir(parents=True)
        resolved = pack_manifest.resolve_pack_path(root, "ohlcv/A_B_5m.parquet")
        assert resolved == (root.resolve() / "ohlcv" / "A_B_5m.parquet")


class TestRoles:
    @pytest.mark.parametrize(
        "roles, expected",
        [
            (["trading"], ["trading"]),
            (["research_only"], ["research_only"]),
            (["factor"], ["factor"]),
            (["factor", "trading"], ["factor", "trading"]),
            (["trading", "factor"], ["factor", "trading"]),
        ],
    )
    def test_legal_role_sets_are_sorted(self, roles, expected):
        assert pack_manifest.normalize_roles(roles) == expected

    @pytest.mark.parametrize(
        "roles",
        [
            [],
            ["trading", "trading"],
            ["trading", "research_only"],
            ["research_only", "factor"],
            ["trading", "research_only", "factor"],
            ["hedge"],
            "trading",
            {"trading"},
        ],
    )
    def test_illegal_role_sets_are_rejected(self, roles):
        with pytest.raises(PatternLabDataError):
            pack_manifest.normalize_roles(roles)


class TestTimeContracts:
    def test_offsets_are_normalized_to_utc(self):
        assert pack_manifest.to_epoch_ms("1970-01-01T02:00:00+02:00") == 0
        assert pack_manifest.format_utc("2026-01-02T03:04:05+00:00") == "2026-01-02T03:04:05Z"

    def test_epoch_milliseconds_round_trip_exactly(self):
        for value in (0, 1_760_000_000_000, 1_760_000_000_123, -300_000):
            assert pack_manifest.to_epoch_ms(pack_manifest.format_epoch_ms(value)) == value

    @pytest.mark.parametrize(
        "value",
        ["2026-01-02T03:04:05", datetime(2026, 1, 2), 1_760_000_000_000, True, None, "", "not-a-time"],
    )
    def test_naive_and_non_timestamp_values_are_rejected(self, value):
        with pytest.raises(PatternLabDataError):
            pack_manifest.parse_utc(value, "start")

    def test_not_a_time_is_rejected(self):
        pandas = pytest.importorskip("pandas")
        with pytest.raises(PatternLabDataError):
            pack_manifest.parse_utc(pandas.NaT, "start")

    @pytest.mark.parametrize("value", [5, 10, 30, 60, 240])
    def test_supported_timeframes(self, value):
        assert pack_manifest.normalize_timeframe_minutes(value) == value

    @pytest.mark.parametrize("value", [True, 5.0, 7, 0, -5, "30", None])
    def test_unsupported_timeframes_are_rejected(self, value):
        with pytest.raises(PatternLabDataError):
            pack_manifest.normalize_timeframe_minutes(value)


class TestStrictJson:
    def test_duplicate_keys_are_rejected(self):
        with pytest.raises(PatternLabDataError, match="duplicate JSON key"):
            pack_manifest.loads_strict('{"a": 1, "a": 2}', source="fixture")

    @pytest.mark.parametrize("text", ['{"a": NaN}', '{"a": Infinity}', '{"a": -Infinity}'])
    def test_non_finite_constants_are_rejected(self, text):
        with pytest.raises(PatternLabDataError):
            pack_manifest.loads_strict(text, source="fixture")


class TestManifestValidation:
    def test_valid_manifest_normalizes_identifiers_and_roles(self):
        validated = pack_manifest.validate_manifest(_manifest())
        entry = validated["instruments"][0]
        assert entry["instrument_id"] == "TEST_AAA-USDT-SWAP"
        assert entry["contract"] == "AAA-USDT-SWAP"
        assert entry["roles"] == ["trading"]
        assert entry["coverage_end_utc"] == utc(ANCHOR_MS + 3 * STEP_MS)

    @pytest.mark.parametrize("version", [2, 0, "1", True, None])
    def test_unknown_schema_versions_fail_clearly(self, version):
        with pytest.raises(PatternLabDataError, match="schema_version"):
            pack_manifest.validate_manifest(_manifest(schema_version=version))

    def test_future_states_are_recognized_by_the_validator(self):
        validated = pack_manifest.validate_manifest(_manifest(state="incomplete"))
        assert validated["state"] == "incomplete"
        unverified = _entry(
            verification={
                "volume_quote_verified": False,
                "volume_quote_evidence": "units were never established",
                "closed_before_utc": None,
                "closure_evidence": None,
                "closure_source": None,
            }
        )
        assert pack_manifest.validate_manifest(_manifest([unverified]))["instruments"][0][
            "verification"
        ]["volume_quote_verified"] is False

    @pytest.mark.parametrize(
        "overrides",
        [
            {"state": "published"},
            {"revision": 0},
            {"revision": True},
            {"base_timeframe_minutes": 1},
            {"volume_unit": "base_volume"},
            {"generated_utc": "2026-09-14T00:00:00"},
            {"instruments": []},
        ],
    )
    def test_invalid_manifest_headers_are_rejected(self, overrides):
        with pytest.raises(PatternLabDataError):
            pack_manifest.validate_manifest(_manifest(**overrides))

    @pytest.mark.parametrize(
        "overrides",
        [
            {"row_count": 2},
            {"row_count": True},
            {"missing_bar_count": 5},
            {"coverage_end_utc": utc(ANCHOR_MS + 2 * STEP_MS)},
            {"last_open_utc": utc(ANCHOR_MS - STEP_MS)},
            {"first_open_utc": utc(ANCHOR_MS + 60_000)},
            {"sha256": "A" * 64},
            {"sha256": "abc"},
            {"quote_currency": "usdt"},
            {"file": "ohlcv/other.parquet.bak"},
            {"instrument_id": "TEST_OTHER"},
            {"symbol": "A A"},
            {"roles": ["trading", "research_only"]},
        ],
    )
    def test_invalid_instrument_entries_are_rejected(self, overrides):
        with pytest.raises(PatternLabDataError):
            pack_manifest.validate_manifest(_manifest([_entry(**overrides)]))

    def test_duplicate_instruments_and_files_are_rejected(self):
        with pytest.raises(PatternLabDataError, match="duplicate instrument_id"):
            pack_manifest.validate_manifest(_manifest([_entry(), _entry()]))
        second = _entry(instrument_id="TEST_BBB-USDT-SWAP", contract="BBB-USDT-SWAP")
        with pytest.raises(PatternLabDataError, match="duplicate file"):
            pack_manifest.validate_manifest(_manifest([_entry(), second]))

    def test_partial_closure_evidence_is_rejected(self):
        for verification in (
            {"closed_before_utc": None, "closure_evidence": "guess", "closure_source": None},
            {"closed_before_utc": utc(ANCHOR_MS), "closure_evidence": None, "closure_source": "x"},
            {"closed_before_utc": utc(ANCHOR_MS + 60_000), "closure_evidence": "e", "closure_source": "s"},
        ):
            payload = {"volume_quote_verified": True, "volume_quote_evidence": "synthetic"}
            payload.update(verification)
            with pytest.raises(PatternLabDataError):
                pack_manifest.validate_manifest(_manifest([_entry(verification=payload)]))

    def test_unknown_extra_metadata_is_retained(self):
        entry = _entry(instrument_rules={"quantity_step": "unknown"}, future_field=7)
        validated = pack_manifest.validate_manifest(_manifest([entry], future_header="kept"))
        assert validated["future_header"] == "kept"
        assert validated["instruments"][0]["future_field"] == 7
        assert validated["instruments"][0]["instrument_rules"] == {"quantity_step": "unknown"}

    def test_required_fields_are_never_inferred_from_absence(self):
        for field in ("row_count", "sha256", "verification", "source", "roles", "file"):
            broken = _entry()
            broken.pop(field)
            with pytest.raises(PatternLabDataError):
                pack_manifest.validate_manifest(_manifest([broken]))


class TestUniverseAndReadme:
    def test_fabricated_selection_date_is_rejected(self):
        with pytest.raises(PatternLabDataError, match="selection date"):
            pack_manifest.build_universe(selection_date="2026-09-14")

    def test_unknown_membership_state_is_rejected(self):
        with pytest.raises(PatternLabDataError):
            pack_manifest.build_universe(historical_membership="probably")

    def test_readme_is_rendered_from_the_manifest(self):
        entry = _entry(
            missing_bar_count=1,
            row_count=2,
            verification={
                "volume_quote_verified": True,
                "volume_quote_evidence": "synthetic",
                "closed_before_utc": None,
                "closure_evidence": None,
                "closure_source": None,
            },
        )
        readme = pack_manifest.render_readme(pack_manifest.validate_manifest(_manifest([entry])))
        assert "TEST_AAA-USDT-SWAP" in readme
        assert "final-candle closure is unknown" in readme
        assert "1 missing 5m bars" in readme
        assert "quote_turnover" in readme
        assert "a" * 64 in readme
