"""Identifier, path, manifest and README contracts for Pattern Lab packs."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import manifest as pack_manifest

from ._helpers import ANCHOR_MS, STEP_MS, utc


def _entry(*, venue="Test", **overrides):
    entry = pack_manifest.build_instrument_entry(
        symbol="AAA",
        venue=venue,
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


def _as_offset(value: str, hours: int = 2) -> str:
    """Render a canonical UTC string as an equivalent explicit-offset string."""
    moment = pack_manifest.parse_utc(value, "timestamp").astimezone(timezone(timedelta(hours=hours)))
    return moment.isoformat()


class TestNormalizedManifestIsUsable:
    """Regression cases for accepted metadata that consumers could not use."""

    def test_omitted_optional_notes_normalizes_to_null_and_renders(self):
        document = _manifest()
        document["universe"] = dict(document["universe"])
        del document["universe"]["notes"]
        validated = pack_manifest.validate_manifest(document)
        assert validated["universe"]["notes"] is None
        assert "Notes: none" in pack_manifest.render_readme(validated)

    def test_blank_or_mistyped_notes_still_fail(self):
        for notes in ("   ", 7, True):
            document = _manifest()
            document["universe"] = dict(document["universe"], notes=notes)
            with pytest.raises(PatternLabDataError, match="universe.notes"):
                pack_manifest.validate_manifest(document)

    def test_equivalent_offsets_normalize_to_canonical_utc(self):
        entry = _entry()
        canonical = pack_manifest.validate_manifest(_manifest([entry]))["instruments"][0]
        shifted = _entry(
            first_open_utc=_as_offset(entry["first_open_utc"]),
            last_open_utc=_as_offset(entry["last_open_utc"], hours=-5),
            coverage_end_utc=_as_offset(entry["coverage_end_utc"], hours=9),
            verification=pack_manifest.build_verification(
                volume_quote_verified=True,
                volume_quote_evidence="synthetic",
                closed_before_utc=_as_offset(entry["coverage_end_utc"], hours=3),
                closure_evidence="fixture",
                closure_source="fixture",
            ),
        )
        document = _manifest([shifted])
        before = copy.deepcopy(document)
        validated = pack_manifest.validate_manifest(document)["instruments"][0]

        assert validated["first_open_utc"] == canonical["first_open_utc"]
        assert validated["last_open_utc"] == canonical["last_open_utc"]
        assert validated["coverage_end_utc"] == canonical["coverage_end_utc"]
        assert validated["verification"]["closed_before_utc"] == canonical["coverage_end_utc"]
        assert document == before  # caller metadata is never mutated

    @pytest.mark.parametrize("value", [5.0, True, "5", None])
    def test_base_timeframe_must_be_the_exact_integer(self, value):
        with pytest.raises(PatternLabDataError, match="base_timeframe_minutes"):
            pack_manifest.validate_manifest(_manifest(base_timeframe_minutes=value))


class TestPublicationVerificationPolicy:
    def test_unverified_quote_volume_is_refused_at_the_publication_boundary(self):
        verification = pack_manifest.build_verification(
            volume_quote_verified=True, volume_quote_evidence="synthetic"
        )
        assert pack_manifest.require_published_verification(verification, "TEST_A") == verification

        for payload in (
            dict(verification, volume_quote_verified=False),
            dict(verification, volume_quote_verified="true"),
            {key: value for key, value in verification.items() if key != "volume_quote_verified"},
        ):
            with pytest.raises(PatternLabDataError, match="volume_quote_verified"):
                pack_manifest.require_published_verification(payload, "TEST_A")

    def test_unknown_closure_remains_publishable(self):
        verification = pack_manifest.build_verification(
            volume_quote_verified=True, volume_quote_evidence="synthetic"
        )
        assert pack_manifest.require_published_verification(verification, "TEST_A")["closed_before_utc"] is None


def _raw(**overrides):
    """Build the OKX source-specific raw contract fields of a managed entry."""
    raw = {
        "instType": "SWAP",
        "ctType": "linear",
        "settleCcy": "USDT",
        "ctVal": "0.1",
        "ctValCcy": "AAA",
        "ctMult": None,
        "lotSz": "1",
        "minSz": "1",
        "tickSz": "0.001",
        "listTime": "1700000000000",
        "state": "live",
    }
    raw.update(overrides)
    return raw


def _rules(**overrides):
    """Build a valid collector-managed instrument-rule object."""
    rules = {
        "schema_version": 1,
        "source_reference": "GET /api/v5/public/instruments (instType=SWAP) instId=AAA-USDT-SWAP",
        "as_of_utc": "2026-09-14T00:00:00Z",
        "contract_type": "linear_perpetual",
        "base_currency": "AAA",
        "quote_currency": "USDT",
        "settlement_currency": "USDT",
        "quantity_unit": "contracts",
        "quantity_step": "1",
        "minimum_quantity": "1",
        "price_tick": "0.001",
        "minimum_notional": None,
        "listed_at_utc": "2023-11-14T22:13:20Z",
        "trading_status": "live",
        "raw_contract_fields": _raw(),
    }
    rules.update(overrides)
    return rules


def _managed_entry(**overrides):
    """Build one OKX collector-managed entry whose closure covers its coverage end."""
    entry = _entry(
        venue="OKX",
        instrument_rules=_rules(),  # noqa: E501 - keyword order mirrors the manifest entry
        verification=pack_manifest.build_verification(
            volume_quote_verified=True,
            volume_quote_evidence="synthetic",
            closed_before_utc=utc(ANCHOR_MS + 3 * STEP_MS),
            closure_evidence="synthetic fixture declares closed bars",
            closure_source="tests/pattern_lab/test_pattern_lab_manifest.py",
        ),
    )
    entry.update(overrides)
    return entry


def _roster(**overrides):
    entry = {
        "contract": "AAA-USDT-SWAP",
        "instrument_id": "OKX_AAA-USDT-SWAP",
        "quote_currency": "USDT",
        "roles": ["trading"],
        "symbol": "AAA",
        "venue": "OKX",
    }
    entry.update(overrides)
    return [entry]


def _collector(roster=None, **overrides):
    entries = _roster() if roster is None else roster
    payload = {
        "schema_version": 1,
        "roster": entries,
        "roster_sha256": pack_manifest.roster_sha256(entries),
        "managed_start_utc": utc(ANCHOR_MS),
        "last_request": {"start_utc": utc(ANCHOR_MS), "end_utc": utc(ANCHOR_MS + 3 * STEP_MS)},
        "operation_id": "20260914T000000Z-0123456789ab",
    }
    payload.update(overrides)
    return payload


class TestCollectorProvenance:
    def test_a_managed_manifest_validates_and_renders(self):
        managed = _manifest([_managed_entry()], collector=_collector())
        validated = pack_manifest.validate_manifest(managed)
        assert validated["collector"]["roster_sha256"] == pack_manifest.roster_sha256(_roster())
        readme = pack_manifest.render_readme(validated)
        assert "## Collector-managed coverage" in readme
        assert "Tail shortfall (5m bars)" in readme
        assert "| 0 |" in readme
        assert "Quantity unit: contracts" in readme

    def test_an_archival_manifest_without_a_collector_stays_unmanaged(self):
        validated = pack_manifest.validate_manifest(_manifest())
        assert "collector" not in validated
        assert "## Collector-managed coverage" not in pack_manifest.render_readme(validated)

    def test_the_roster_digest_excludes_universe_dates_and_paths(self):
        first = pack_manifest.roster_sha256(_roster())
        managed = _manifest(
            [_managed_entry()],
            collector=_collector(),
            universe=pack_manifest.build_universe(notes="entirely different provenance"),
        )
        assert pack_manifest.validate_manifest(managed)["collector"]["roster_sha256"] == first
        assert pack_manifest.canonical_roster_bytes(_roster()).endswith(b"}]")

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"schema_version": 2}, "unsupported collector version"),
            ({"roster_sha256": "b" * 64}, "does not match the canonical roster digest"),
            ({"operation_id": "  "}, "nonblank string"),
            ({"managed_start_utc": utc(ANCHOR_MS + 60_000)}, "not aligned"),
            ({"extra": 1}, "closed"),
            (
                {"last_request": {"start_utc": utc(ANCHOR_MS), "end_utc": utc(ANCHOR_MS)}},
                "start_utc < end_utc",
            ),
            (
                {
                    "last_request": {
                        "start_utc": utc(ANCHOR_MS + STEP_MS),
                        "end_utc": utc(ANCHOR_MS + 3 * STEP_MS),
                    }
                },
                "must equal managed_start_utc",
            ),
            (
                {
                    "last_request": {
                        "start_utc": utc(ANCHOR_MS),
                        "end_utc": utc(ANCHOR_MS + STEP_MS),
                        "token": "latest-closed",
                    }
                },
                "closed",
            ),
        ],
    )
    def test_invalid_collector_objects_are_rejected(self, overrides, message):
        managed = _manifest([_managed_entry()], collector=_collector(**overrides))
        with pytest.raises(PatternLabDataError, match=message):
            pack_manifest.validate_manifest(managed)

    @pytest.mark.parametrize(
        "field, value, message",
        [
            ("instrument_id", "OKX_ZZZ-USDT-SWAP", "must match the published instrument"),
            ("roles", ["research_only"], r"disagrees with its published entry on \['roles'\]"),
            ("symbol", "ZZZ", r"disagrees with its published entry on \['symbol'\]"),
            ("quote_currency", "USDC", r"disagrees with its published entry on \['quote_currency'\]"),
        ],
    )
    def test_all_six_roster_fields_must_match_the_published_entry(self, field, value, message):
        relabelled = _roster(**{field: value})
        if field == "instrument_id":
            relabelled[0]["contract"] = "ZZZ-USDT-SWAP"
        managed = _manifest([_managed_entry()], collector=_collector(roster=relabelled))
        with pytest.raises(PatternLabDataError, match=message):
            pack_manifest.validate_manifest(managed)

    def test_the_managed_start_must_equal_every_first_stored_row(self):
        managed = _manifest(
            [_managed_entry()],
            collector=_collector(
                managed_start_utc=utc(ANCHOR_MS - 288 * STEP_MS),
                last_request={
                    "start_utc": utc(ANCHOR_MS - 288 * STEP_MS),
                    "end_utc": utc(ANCHOR_MS + 3 * STEP_MS),
                },
            ),
        )
        with pytest.raises(PatternLabDataError, match="must equal every"):
            pack_manifest.validate_manifest(managed)

    def test_a_managed_entry_requires_the_versioned_rule_object(self):
        managed = _manifest([_managed_entry(instrument_rules=None)], collector=_collector())
        with pytest.raises(PatternLabDataError, match="instrument_rules"):
            pack_manifest.validate_manifest(managed)

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"schema_version": 2}, "unsupported version"),
            ({"contract_type": "future"}, "contract_type"),
            ({"trading_status": "halted"}, "trading_status"),
            ({"quote_currency": "USDC"}, "USDT-quoted"),
            ({"quantity_unit": "lots"}, "quantity_unit"),
            ({"quantity_step": "0"}, "positive decimal"),
            ({"price_tick": "abc"}, "decimal number"),
            ({"minimum_notional": "-1"}, "nonnegative decimal"),
            ({"raw_contract_fields": {"lotSz": 1}}, "retained as strings"),
            ({"extra": 1}, "closed"),
        ],
    )
    def test_invalid_instrument_rules_are_rejected(self, overrides, message):
        with pytest.raises(PatternLabDataError, match=message):
            pack_manifest.validate_instrument_rules(_rules(**overrides), "rules", venue="OKX")

    def test_a_listing_instant_need_not_sit_on_the_grid(self):
        rules = pack_manifest.validate_instrument_rules(
            _rules(listed_at_utc="2023-11-14T22:13:21Z", raw_contract_fields=_raw(listTime="1700000001000")),
            "rules",
            venue="OKX",
        )
        assert rules["listed_at_utc"] == "2023-11-14T22:13:21Z"
        assert pack_manifest.validate_instrument_rules(
            _rules(listed_at_utc=None, raw_contract_fields=_raw(listTime=None)), "rules", venue="OKX"
        )["listed_at_utc"] is None

    def test_tail_shortfall_is_derived_from_the_requested_end(self):
        entry = _entry()
        assert pack_manifest.tail_shortfall_bars(entry, entry["coverage_end_utc"]) == 0
        later = utc(ANCHOR_MS + 10 * STEP_MS)
        assert pack_manifest.tail_shortfall_bars(entry, later) == 7


def _bybit_raw(**overrides):
    """Build the Bybit source-specific raw contract fields of a managed entry."""
    raw = {
        "contractType": "LinearPerpetual",
        "baseCoin": "BBB",
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "launchTime": "1700000000000",
        "status": "Trading",
        "qtyStep": "0.1",
        "minOrderQty": "0.1",
        "minNotionalValue": "5",
        "tickSize": "0.0001",
    }
    raw.update(overrides)
    return raw


def _bybit_rules(**overrides):
    """Build a valid Bybit collector-managed instrument-rule object."""
    rules = {
        "schema_version": 1,
        "source_reference": "GET /v5/market/instruments-info (category=linear) symbol=BBBUSDT",
        "as_of_utc": "2026-09-14T00:00:00Z",
        "contract_type": "linear_perpetual",
        "base_currency": "BBB",
        "quote_currency": "USDT",
        "settlement_currency": "USDT",
        "quantity_unit": "BBB",
        "quantity_step": "0.1",
        "minimum_quantity": "0.1",
        "price_tick": "0.0001",
        "minimum_notional": "5",
        "listed_at_utc": "2023-11-14T22:13:20Z",
        "trading_status": "Trading",
        "raw_contract_fields": _bybit_raw(),
    }
    rules.update(overrides)
    return rules


class TestManagedRuleAgreement:
    """Raw source fields and their normalized values must agree exactly."""

    def test_both_venue_shapes_validate_and_normalize_unchanged(self):
        assert pack_manifest.validate_instrument_rules(_rules(), "rules", venue="OKX") == _rules()
        assert (
            pack_manifest.validate_instrument_rules(_bybit_rules(), "rules", venue="BYBIT")
            == _bybit_rules()
        )

    @pytest.mark.parametrize(
        "venue, rules, message",
        [
            ("OKX", _rules(raw_contract_fields={}), "object is closed"),
            ("BYBIT", _bybit_rules(raw_contract_fields={}), "object is closed"),
            (
                "OKX",
                _rules(raw_contract_fields=_raw(unexpected="x")),
                "unexpected keys \\['unexpected'\\]",
            ),
            # The reproduced case: a raw step that contradicts the normalized one.
            (
                "BYBIT",
                _bybit_rules(raw_contract_fields=_bybit_raw(qtyStep="999")),
                "does not agree numerically with the normalized quantity step",
            ),
            (
                "OKX",
                _rules(raw_contract_fields=_raw(tickSz="0.5")),
                "does not agree numerically with the normalized price tick",
            ),
            # The base currency comes from the source, never from a display label.
            (
                "OKX",
                _rules(base_currency="ZZZ"),
                "does not agree with the normalized base currency",
            ),
            (
                "BYBIT",
                _bybit_rules(base_currency="ZZZ", quantity_unit="ZZZ"),
                "does not agree with the normalized base currency",
            ),
            (
                "OKX",
                _rules(listed_at_utc="2020-01-01T00:00:00Z"),
                "does not agree with the source listTime",
            ),
            (
                "BYBIT",
                _bybit_rules(minimum_notional=None),
                "does not agree numerically with the normalized minimum notional",
            ),
            (
                "BYBIT",
                _bybit_rules(raw_contract_fields=_bybit_raw(minNotionalValue=None)),
                "the source published no minimum notional",
            ),
            ("OKX", _rules(minimum_notional="5"), "publish no minimum notional"),
            ("OKX", _rules(quantity_unit="AAA"), "sized in 'contracts'"),
            ("BYBIT", _bybit_rules(quantity_unit="contracts"), "sized in its base currency"),
            ("OKX", _rules(trading_status="Trading"), "expected the original OKX source status"),
            ("BYBIT", _bybit_rules(trading_status="live"), "expected the original BYBIT source status"),
            (
                "OKX",
                _rules(raw_contract_fields=_raw(ctType="inverse")),
                "does not agree with the normalized contract type",
            ),
        ],
    )
    def test_source_to_normalized_disagreements_are_rejected(self, venue, rules, message):
        with pytest.raises(PatternLabDataError, match=message):
            pack_manifest.validate_instrument_rules(rules, "rules", venue=venue)

    def test_a_differently_spelled_but_equal_decimal_is_accepted(self):
        rules = _bybit_rules(raw_contract_fields=_bybit_raw(qtyStep="0.100"))
        validated = pack_manifest.validate_instrument_rules(rules, "rules", venue="BYBIT")
        # Both spellings are preserved exactly as the source published them.
        assert validated["raw_contract_fields"]["qtyStep"] == "0.100"
        assert validated["quantity_step"] == "0.1"

    def test_an_unsupported_venue_is_not_collector_managed(self):
        with pytest.raises(PatternLabDataError, match="must come from one of"):
            pack_manifest.validate_instrument_rules(_rules(), "rules", venue="TEST")


class TestClosureCertificationInvariant:
    """A managed publication certifies every row it retains."""

    def _with_closure(self, closed_before_ms):
        return _managed_entry(
            verification=pack_manifest.build_verification(
                volume_quote_verified=True,
                volume_quote_evidence="synthetic",
                closed_before_utc=utc(closed_before_ms),
                closure_evidence="synthetic fixture declares closed bars",
                closure_source="tests/pattern_lab/test_pattern_lab_manifest.py",
            )
        )

    def test_a_managed_entry_whose_cutoff_misses_stored_rows_is_refused(self):
        managed = _manifest([self._with_closure(ANCHOR_MS + STEP_MS)], collector=_collector())
        with pytest.raises(PatternLabDataError, match="does not certify the stored coverage end"):
            pack_manifest.validate_manifest(managed)

    def test_a_managed_entry_without_a_cutoff_is_refused(self):
        entry = _managed_entry(
            verification=pack_manifest.build_verification(
                volume_quote_verified=True, volume_quote_evidence="synthetic"
            )
        )
        with pytest.raises(PatternLabDataError, match="must record the closure cutoff"):
            pack_manifest.validate_manifest(_manifest([entry], collector=_collector()))

    def test_a_cutoff_beyond_the_stored_coverage_is_allowed(self):
        managed = _manifest([self._with_closure(ANCHOR_MS + 99 * STEP_MS)], collector=_collector())
        assert pack_manifest.validate_manifest(managed)["revision"] == 1

    def test_an_archival_shortfall_is_a_limitation_not_a_blocker(self):
        entry = _entry(
            verification=pack_manifest.build_verification(
                volume_quote_verified=True,
                volume_quote_evidence="synthetic",
                closed_before_utc=utc(ANCHOR_MS + STEP_MS),
                closure_evidence="archival evidence",
                closure_source="archival source",
            )
        )
        validated = pack_manifest.validate_manifest(_manifest([entry]))
        stored = validated["instruments"][0]
        assert pack_manifest.certification_shortfall_bars(stored) == 2
        assert pack_manifest.research_blockers(stored) == []  # earlier slices stay admissible
        assert any("uncertified" in item for item in pack_manifest.research_limitations(stored))
        assert "uncertified" in pack_manifest.render_readme(validated)


class TestOpaqueArchivalRules:
    """Unmanaged rule mappings stay opaque, in validation and in the README."""

    def test_an_opaque_mapping_validates_and_renders_without_collector_fields(self):
        opaque = {"schema_version": 1, "custom": "archival opaque rules"}
        validated = pack_manifest.validate_manifest(_manifest([_entry(instrument_rules=opaque)]))
        assert validated["instruments"][0]["instrument_rules"] == opaque
        readme = pack_manifest.render_readme(validated)  # must not raise KeyError
        assert "opaque archival metadata, preserved verbatim" in readme
        assert "Quantity unit:" not in readme
        assert "Contract: linear_perpetual" not in readme

    def test_a_managed_pack_still_renders_its_validated_rule_fields(self):
        validated = pack_manifest.validate_manifest(
            _manifest([_managed_entry()], collector=_collector())
        )
        readme = pack_manifest.render_readme(validated)
        assert "Quantity unit: contracts" in readme
        assert "opaque archival metadata" not in readme
