"""Roster configuration, source adapters, preflight, closure and merge contracts.

Every exchange response is generated at runtime through the injectable transport,
so the adapters' real request construction, cursor arithmetic and decoding are
exercised without any network access.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabPendingError
from tools.pattern_lab import collect as pack_collect
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import exchange_data
from tools.pattern_lab import manifest as pack_manifest
from tools.pattern_lab import update_transaction

from ._helpers import (
    ANCHOR_MS,
    BYBIT_CONTRACT,
    BYBIT_ID,
    OKX_CONTRACT,
    OKX_ID,
    STEP_MS,
    FakeClock,
    FakeExchange,
    build_exchange,
    bybit_instrument,
    collector_options,
    fake_client,
    mutate_manifest,
    okx_instrument,
    roster_document,
    run_collect,
    run_update,
    synthetic_series,
    utc,
    write_roster,
)

@pytest.fixture
def roster(tmp_path):
    return write_roster(tmp_path / "universe.json")


class TestRosterConfiguration:
    def test_the_tracked_roster_preserves_the_supplied_counts_and_roles(self):
        loaded = pack_collect.load_roster()
        assert len(loaded["roster"]) == 54
        roles = [tuple(entry["roles"]) for entry in loaded["roster"]]
        assert roles.count(("trading",)) == 44
        assert roles.count(("research_only",)) == 9
        assert roles.count(("factor",)) == 1
        assert loaded["roster_sha256"] == pack_manifest.roster_sha256(loaded["roster"])
        assert loaded["universe"]["historical_membership"] == "unknown"
        assert loaded["universe"]["selection_date"] is None

    def test_the_tracked_roster_file_is_the_supplied_configuration(self):
        raw = json.loads(pack_collect.DEFAULT_ROSTER_PATH.read_text(encoding="utf-8"))
        assert sorted(raw) == ["instruments", "schema_version", "universe"]
        assert len(raw["instruments"]) == 54
        assert {entry["venue"] for entry in raw["instruments"]} == {"OKX", "BYBIT"}

    def test_a_custom_roster_works(self, tmp_path):
        path = write_roster(
            tmp_path / "custom.json",
            roster_document(
                entries=[
                    {
                        "instrument_id": OKX_ID,
                        "symbol": "AAA",
                        "venue": "OKX",
                        "contract": OKX_CONTRACT,
                        "quote_currency": "USDT",
                        "roles": ["factor"],
                    }
                ]
            ),
        )
        loaded = pack_collect.load_roster(path)
        assert [entry["instrument_id"] for entry in loaded["roster"]] == [OKX_ID]
        assert loaded["roster"][0]["roles"] == ["factor"]

    @pytest.mark.parametrize(
        "mutate, message",
        [
            (lambda doc: doc["instruments"].append(dict(doc["instruments"][0])), "duplicate instrument"),
            (
                lambda doc: doc["instruments"][0].update(
                    contract="BBBUSD", instrument_id="BYBIT_BBBUSD"
                ),
                "not supported",
            ),
            (
                lambda doc: doc["instruments"][1].update(
                    contract="AAA-USDC-SWAP", instrument_id="OKX_AAA-USDC-SWAP"
                ),
                "not supported",
            ),
            (
                lambda doc: doc["instruments"][0].update(
                    venue="KRAKEN", instrument_id="KRAKEN_BBBUSDT"
                ),
                "not supported",
            ),
            (lambda doc: doc["instruments"][0].update(quote_currency="USDC"), "not supported"),
            (lambda doc: doc["instruments"][0].update(extra="x"), "closed"),
            (lambda doc: doc.update(schema_version=2), "unsupported roster version"),
            (lambda doc: doc.update(extra={}), "closed"),
            (lambda doc: doc["universe"].update(extra=1), "unexpected keys"),
        ],
    )
    def test_invalid_rosters_are_rejected(self, mutate, message):
        document = roster_document()
        mutate(document)
        with pytest.raises(PatternLabDataError, match=message):
            pack_collect.validate_roster_config(document, source="roster")

    @pytest.mark.parametrize(
        "instrument_id, contract",
        [("OKX_../ESCAPE", "../ESCAPE"), ("OKX_A B", "A B"), ("OKX_A_B", "A_B")],
    )
    def test_unsafe_identifiers_are_rejected(self, instrument_id, contract):
        document = roster_document(
            entries=[
                {
                    "instrument_id": instrument_id,
                    "symbol": "AAA",
                    "venue": "OKX",
                    "contract": contract,
                    "quote_currency": "USDT",
                    "roles": ["trading"],
                }
            ]
        )
        with pytest.raises(PatternLabDataError):
            pack_collect.validate_roster_config(document, source="roster")

    def test_entries_must_be_sorted_by_instrument_id(self):
        document = roster_document()
        document["instruments"].reverse()
        with pytest.raises(PatternLabDataError, match="sorted by instrument_id"):
            pack_collect.validate_roster_config(document, source="roster")


class TestAdapters:
    def test_okx_pages_backwards_and_keeps_the_quote_turnover_field(self):
        exchange, clock, stamps, values = build_exchange(slots=7)
        client = fake_client(exchange, clock)
        adapter = exchange_data.OkxAdapter()
        end_ms = int(stamps[-1]) + STEP_MS
        got_stamps, got_values = adapter.fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=end_ms, limit=3
        )
        assert np.array_equal(got_stamps, stamps)
        assert np.array_equal(got_values, values)
        cursors = [
            int(params["after"])
            for path, params in exchange.requests
            if path == "/api/v5/market/history-candles"
        ]
        assert cursors[0] == end_ms
        assert cursors == sorted(cursors, reverse=True)
        assert len(set(cursors)) == len(cursors)

    def test_bybit_pages_backwards_with_an_exclusive_end_cursor(self):
        exchange, clock, stamps, values = build_exchange(slots=7)
        client = fake_client(exchange, clock)
        adapter = exchange_data.BybitAdapter()
        end_ms = int(stamps[-1]) + STEP_MS
        got_stamps, got_values = adapter.fetch_candles(
            client, BYBIT_CONTRACT, start_ms=int(stamps[0]), end_ms=end_ms, limit=3
        )
        assert np.array_equal(got_stamps, stamps)
        assert np.array_equal(got_values, values)
        ends = [
            int(params["end"]) for path, params in exchange.requests if path == "/v5/market/kline"
        ]
        assert ends[0] == end_ms - 1
        assert ends == sorted(ends, reverse=True)

    def test_the_exclusive_end_excludes_the_bar_opening_at_it(self):
        exchange, clock, stamps, _ = build_exchange(slots=10)
        client = fake_client(exchange, clock)
        end_ms = int(stamps[5])
        for adapter, contract in (
            (exchange_data.OkxAdapter(), OKX_CONTRACT),
            (exchange_data.BybitAdapter(), BYBIT_CONTRACT),
        ):
            got, _ = adapter.fetch_candles(
                client, contract, start_ms=int(stamps[0]), end_ms=end_ms
            )
            assert np.array_equal(got, stamps[:5])

    def test_a_short_nonempty_page_is_not_exhaustion(self):
        exchange, clock, stamps, _ = build_exchange(slots=25)
        exchange.page_limit_override = 2
        client = fake_client(exchange, clock)
        got, _ = exchange_data.OkxAdapter().fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS, limit=50
        )
        assert np.array_equal(got, stamps)

    def test_an_empty_page_ends_exhausted_history(self):
        exchange, clock, stamps, _ = build_exchange(slots=6)
        client = fake_client(exchange, clock)
        got, _ = exchange_data.OkxAdapter().fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]) - 50 * STEP_MS,
            end_ms=int(stamps[-1]) + STEP_MS,
        )
        assert np.array_equal(got, stamps)

    def test_stalled_pagination_is_a_protocol_error(self):
        exchange, clock, stamps, _ = build_exchange(slots=40)
        exchange.page_limit_override = 5
        client = fake_client(exchange, clock)
        exchange.scripted.append(
            exchange_data.HttpResponse(
                status=200,
                body=json.dumps(
                    {
                        "code": "0",
                        "msg": "",
                        "data": [
                            [str(int(stamps[-1])), "1", "1", "1", "1", "0", "0", "1", "1"]
                            for _ in range(2)
                        ],
                    }
                ),
            )
        )
        exchange.scripted.append(exchange.scripted[0])
        with pytest.raises(PatternLabDataError, match="pagination did not progress"):
            exchange_data.OkxAdapter().fetch_candles(
                client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS
            )

    def test_okx_refuses_an_incomplete_candle(self):
        stamps, values = synthetic_series(6)
        # The newest bar has not closed at the venue clock, so confirm is "0".
        exchange = FakeExchange(now_ms=int(stamps[-1]) + 60_000)
        exchange.add_okx(OKX_CONTRACT, timestamps=stamps, ohlcv=values)
        client = fake_client(exchange, FakeClock(exchange.now_ms))
        got, _ = exchange_data.OkxAdapter().fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS
        )
        assert np.array_equal(got, stamps[:-1])

    def test_conflicting_duplicate_rows_from_one_download_fail(self):
        exchange, clock, stamps, _ = build_exchange(slots=4)
        client = fake_client(exchange, clock)
        row = [str(int(stamps[0])), "1", "1", "1", "1", "0", "0", "5", "1"]
        other = [str(int(stamps[0])), "2", "2", "2", "2", "0", "0", "5", "1"]
        exchange.scripted.append(
            exchange_data.HttpResponse(
                status=200, body=json.dumps({"code": "0", "msg": "", "data": [row, other]})
            )
        )
        with pytest.raises(PatternLabDataError, match="conflicting values"):
            exchange_data.OkxAdapter().fetch_candles(
                client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[0]) + STEP_MS
            )

    def test_identical_duplicate_rows_are_harmless(self):
        exchange, clock, stamps, values = build_exchange(slots=4)
        client = fake_client(exchange, clock)
        row = [
            str(int(stamps[0])),
            *(f"{float(item):.10f}" for item in values[0][:4]),
            "0",
            "0",
            f"{float(values[0][4]):.10f}",
            "1",
        ]
        exchange.scripted.append(
            exchange_data.HttpResponse(
                status=200, body=json.dumps({"code": "0", "msg": "", "data": [row, list(row)]})
            )
        )
        got, _ = exchange_data.OkxAdapter().fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[0]) + STEP_MS
        )
        assert got.tolist() == [int(stamps[0])]

    @pytest.mark.parametrize(
        "body, message",
        [
            ("not json", "not valid JSON"),
            (json.dumps({"code": "51001", "msg": "no such instrument"}), "code '51001'"),
            (json.dumps({"code": "0", "msg": "", "data": {}}), "expected a JSON array"),
            (json.dumps({"code": "0", "msg": "", "data": [["1", "2"]]}), "at least 9 fields"),
        ],
    )
    def test_malformed_okx_envelopes_fail_clearly(self, body, message):
        exchange, clock, stamps, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(exchange_data.HttpResponse(status=200, body=body))
        with pytest.raises(PatternLabDataError, match=message):
            exchange_data.OkxAdapter().fetch_candles(
                client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS
            )

    def test_an_invalid_symbol_is_not_retried_as_empty_history(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        with pytest.raises(PatternLabDataError, match="51001"):
            exchange_data.OkxAdapter().fetch_candles(
                client, "ZZZ-USDT-SWAP", start_ms=ANCHOR_MS, end_ms=ANCHOR_MS + STEP_MS
            )
        assert len([path for path, _ in exchange.requests]) == 1

    def test_transient_conditions_are_retried_with_bounded_backoff(self):
        exchange, clock, stamps, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(exchange_data.HttpResponse(status=0, body="", error="timeout"))
        exchange.scripted.append(
            exchange_data.HttpResponse(status=429, body="", headers={"Retry-After": "2"})
        )
        got, _ = exchange_data.OkxAdapter().fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS
        )
        assert got.size == stamps.size
        assert clock.sleeps and max(clock.sleeps) >= 2.0

    def test_retries_are_bounded_by_max_attempts(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        for _ in range(5):
            exchange.scripted.append(exchange_data.HttpResponse(status=503, body=""))
        with pytest.raises(PatternLabDataError, match="HTTP 503"):
            exchange_data.OkxAdapter().server_time_ms(client)
        assert len(exchange.requests) == 3  # the fixture client allows three attempts

    def test_instrument_metadata_is_normalized_per_venue(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        okx = exchange_data.OkxAdapter().instrument_metadata(client, OKX_CONTRACT)
        bybit = exchange_data.BybitAdapter().instrument_metadata(client, BYBIT_CONTRACT)
        assert okx["quantity_unit"] == "contracts"
        assert okx["base_currency"] == "AAA"
        assert okx["raw_contract_fields"]["ctValCcy"] == "AAA"
        assert bybit["quantity_unit"] == bybit["base_currency"] == "BBB"
        assert bybit["minimum_notional"] == "5"
        for venue, rules in (("OKX", okx), ("BYBIT", bybit)):
            assert rules["contract_type"] == "linear_perpetual"
            assert rules["quote_currency"] == rules["settlement_currency"] == "USDT"
            # The adapter's own output satisfies the closed per-venue rule schema,
            # including raw-to-normalized agreement on every quantity and listing.
            assert pack_manifest.validate_instrument_rules(rules, "rules", venue=venue) == rules

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"ctType": "inverse"}, "USDT linear SWAP"),
            ({"settleCcy": "USDC"}, "USDT linear SWAP"),
            ({"state": "suspend"}, "state is 'suspend'"),
            ({"lotSz": ""}, "nonblank string"),
            ({"tickSz": "-1"}, "positive"),
        ],
    )
    def test_unsupported_or_incomplete_okx_metadata_is_refused(self, overrides, message):
        exchange, clock, _, _ = build_exchange(slots=3)
        exchange.instruments[("OKX", OKX_CONTRACT)] = okx_instrument(OKX_CONTRACT, **overrides)
        client = fake_client(exchange, clock)
        with pytest.raises(PatternLabDataError, match=message):
            exchange_data.OkxAdapter().instrument_metadata(client, OKX_CONTRACT)

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"contractType": "InversePerpetual"}, "USDT linear perpetual"),
            ({"status": "PreLaunch"}, "status is 'PreLaunch'"),
            ({"lotSizeFilter": {"minNotionalValue": ""}}, None),
        ],
    )
    def test_bybit_metadata_identity_and_optional_fields(self, overrides, message):
        exchange, clock, _, _ = build_exchange(slots=3)
        exchange.instruments[("BYBIT", BYBIT_CONTRACT)] = bybit_instrument(
            BYBIT_CONTRACT, **overrides
        )
        client = fake_client(exchange, clock)
        if message is None:
            rules = exchange_data.BybitAdapter().instrument_metadata(client, BYBIT_CONTRACT)
            assert rules["minimum_notional"] is None
            assert rules["raw_contract_fields"]["minNotionalValue"] is None
            return
        with pytest.raises(PatternLabDataError, match=message):
            exchange_data.BybitAdapter().instrument_metadata(client, BYBIT_CONTRACT)

    def test_an_absent_listing_time_stays_null(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        exchange.instruments[("OKX", OKX_CONTRACT)] = okx_instrument(OKX_CONTRACT, listTime="")
        client = fake_client(exchange, clock)
        rules = exchange_data.OkxAdapter().instrument_metadata(client, OKX_CONTRACT)
        assert rules["listed_at_utc"] is None


class TestClosure:
    def test_the_cutoff_uses_the_slowest_server_clock_minus_the_lag(self):
        assert exchange_data.safe_closed_cutoff_ms([2_000_000_000_000, 1_999_999_999_000]) == (
            ((1_999_999_999_000 - 60_000) // STEP_MS) * STEP_MS
        )

    def test_latest_closed_resolves_once_and_never_reaches_the_manifest(self, tmp_path, roster):
        exchange, clock, stamps, _ = build_exchange(slots=30)
        result = run_collect(tmp_path / "pack", roster, exchange, clock)
        assert result["resolved_request"]["requested_end_token"] == "latest-closed"
        manifest = pack_manifest.read_manifest(tmp_path / "pack")
        text = json.dumps(manifest)
        assert "latest-closed" not in text
        assert manifest["collector"]["last_request"]["end_utc"] == utc(int(stamps[-1]) + STEP_MS)

    def test_an_explicit_future_end_is_refused_rather_than_shortened(self, tmp_path, roster):
        exchange, clock, stamps, _ = build_exchange(slots=30)
        with pytest.raises(PatternLabDataError, match="beyond the safe closed cutoff"):
            run_collect(
                tmp_path / "pack", roster, exchange, clock, end=utc(int(stamps[-1]) + 20 * STEP_MS)
            )

    def test_a_failed_server_time_request_blocks_the_operation(self, tmp_path, roster):
        exchange, clock, _, _ = build_exchange(slots=30)
        exchange.fail_time_for.add("BYBIT")
        with pytest.raises(PatternLabDataError, match="Bybit server time"):
            run_collect(tmp_path / "pack", roster, exchange, clock)

    def test_the_frozen_end_survives_a_clock_that_advances_between_pages(self, tmp_path, roster):
        exchange, clock, stamps, values = build_exchange(slots=30)
        frozen_end = int(stamps[-1]) + STEP_MS

        longer_stamps, longer_values = synthetic_series(40)
        original = exchange._okx_candles

        def advancing(params):
            # New bars appear at the venue mid-operation; the frozen end excludes them.
            exchange.now_ms = int(longer_stamps[-1]) + STEP_MS
            exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
            exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)
            return original(params)

        exchange._okx_candles = advancing
        result = run_collect(tmp_path / "pack", roster, exchange, clock)
        assert result["resolved_request"]["end_utc"] == utc(frozen_end)
        for item in result["instruments"]:
            assert item["coverage_end_utc"] == utc(frozen_end)


class TestPreflight:
    def test_all_metadata_is_checked_before_any_bulk_page(self, tmp_path, roster):
        exchange, clock, _, _ = build_exchange(slots=30)
        run_collect(tmp_path / "pack", roster, exchange, clock)
        paths = [path for path, _ in exchange.requests]
        first_bulk = min(
            paths.index("/api/v5/market/history-candles"), paths.index("/v5/market/kline")
        )
        metadata_positions = [
            index
            for index, path in enumerate(paths)
            if path in ("/api/v5/public/instruments", "/v5/market/instruments-info")
        ]
        assert len(metadata_positions) == 2
        assert max(metadata_positions) < first_bulk

    def test_a_listing_after_the_requested_start_is_rejected(self, tmp_path, roster):
        exchange, clock, stamps, _ = build_exchange(slots=30)
        exchange.instruments[("OKX", OKX_CONTRACT)] = okx_instrument(
            OKX_CONTRACT, listTime=str(int(stamps[5]))
        )
        with pytest.raises(PatternLabDataError, match="lists this contract at"):
            run_collect(tmp_path / "pack", roster, exchange, clock)

    def test_an_unknown_listing_time_still_requires_the_start_probe(self, tmp_path, roster):
        exchange, clock, stamps, values = build_exchange(slots=30)
        exchange.instruments[("OKX", OKX_CONTRACT)] = okx_instrument(OKX_CONTRACT, listTime="")
        # Retention starts later than the request even though the listing is unknown.
        exchange.set_rows("OKX", OKX_CONTRACT, stamps[3:], values[3:])
        with pytest.raises(PatternLabDataError, match="is not retrievable"):
            run_collect(tmp_path / "pack", roster, exchange, clock)

    def test_the_probe_is_one_bounded_page_per_instrument(self, tmp_path, roster):
        exchange, clock, stamps, _ = build_exchange(slots=30)
        run_collect(tmp_path / "pack", roster, exchange, clock)
        probes = [
            params
            for path, params in exchange.requests
            if path == "/api/v5/market/history-candles" and params["limit"] == "1"
        ]
        assert len(probes) == 1
        assert int(probes[0]["after"]) == ANCHOR_MS + STEP_MS

    def test_every_failure_is_reported_together(self, tmp_path, roster):
        exchange, clock, _, _ = build_exchange(slots=30)
        exchange.instruments.pop(("OKX", OKX_CONTRACT))
        exchange.instruments.pop(("BYBIT", BYBIT_CONTRACT))
        with pytest.raises(PatternLabDataError) as failure:
            run_collect(tmp_path / "pack", roster, exchange, clock)
        assert OKX_ID in str(failure.value) and BYBIT_ID in str(failure.value)
        assert failure.value.error_code == "preflight_failed"

    def test_an_ordinary_append_needs_no_historical_probe(self, tmp_path, roster):
        exchange, clock, stamps, values = build_exchange(slots=30)
        run_collect(tmp_path / "pack", roster, exchange, clock)
        longer_stamps, longer_values = synthetic_series(40)
        exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
        exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)
        exchange.requests.clear()
        pack_collect.update_pack(
            tmp_path / "pack",
            end="latest-closed",
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        probes = [
            params
            for path, params in exchange.requests
            if path in ("/api/v5/market/history-candles", "/v5/market/kline")
            and params["limit"] == "1"
        ]
        assert probes == []


class TestCoverageAndMerge:
    def test_fetch_ranges_extend_both_ends_with_one_hour_of_overlap(self):
        overlap = pack_collect.OVERLAP_BARS * STEP_MS
        first, last = ANCHOR_MS, ANCHOR_MS + 100 * STEP_MS
        ranges = pack_collect.fetch_ranges(
            effective_start_ms=first - 50 * STEP_MS,
            end_ms=last + 50 * STEP_MS,
            first_ms=first,
            last_ms=last,
        )
        assert ranges == [
            (first - 50 * STEP_MS, first + overlap),
            (last + STEP_MS - overlap, last + 50 * STEP_MS),
        ]

    def test_overlapping_prefix_and_tail_requests_are_coalesced(self):
        first, last = ANCHOR_MS, ANCHOR_MS + 4 * STEP_MS
        ranges = pack_collect.fetch_ranges(
            effective_start_ms=first - STEP_MS,
            end_ms=last + 2 * STEP_MS,
            first_ms=first,
            last_ms=last,
        )
        assert ranges == [(first - STEP_MS, last + 2 * STEP_MS)]

    def test_an_older_end_never_manufactures_an_inverted_range(self):
        first, last = ANCHOR_MS, ANCHOR_MS + 100 * STEP_MS
        assert (
            pack_collect.fetch_ranges(
                effective_start_ms=first, end_ms=first + 10 * STEP_MS, first_ms=first, last_ms=last
            )
            == []
        )

    def test_identical_overlaps_preserve_the_existing_row(self):
        stamps, values = synthetic_series(10)
        merged = pack_collect.merge_series(stamps, values, stamps, values.copy())
        assert merged["conflict_count"] == 0
        assert (merged["prefix_rows"], merged["inserted_rows"], merged["appended_rows"]) == (0, 0, 0)
        assert np.array_equal(merged["values"], values)
        assert np.array_equal(merged["stamps"], stamps)

    def test_a_differing_overlap_row_is_a_conflict(self):
        stamps, values = synthetic_series(10)
        changed = values.copy()
        changed[3, 1] += 1.0
        merged = pack_collect.merge_series(stamps, values, stamps, changed)
        assert merged["conflict_count"] == 1
        assert merged["conflicts"][0]["timestamp_utc"] == utc(int(stamps[3]))

    def test_new_rows_are_classified_by_position(self):
        stamps, values = synthetic_series(20, first_slot=0, drop_slots=(5,))
        full_stamps, full_values = synthetic_series(26, first_slot=-3)
        merged = pack_collect.merge_series(stamps, values, full_stamps, full_values)
        assert merged["prefix_rows"] == 3
        assert merged["inserted_rows"] == 1
        assert merged["appended_rows"] == 3
        assert merged["conflict_count"] == 0

    def test_a_conflict_leaves_every_live_file_intact(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, values = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}

        longer_stamps, longer_values = synthetic_series(40)
        # Slot 25 lies inside the one-hour tail overlap that an update re-fetches.
        longer_values[25, 4] += 5.0  # a stored historical quote turnover now disagrees
        exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
        exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)
        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.update_pack(
                root, end="latest-closed", options=collector_options(),
                transport=exchange, clock=clock,
            )
        assert failure.value.error_code == "historical_conflict"
        assert "abort-update" in str(failure.value)
        assert "never" in str(failure.value)
        after = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        assert {k: v for k, v in after.items() if k in before} == before

        # The staging journal is preserved, so an ordinary update now reports it.
        with pytest.raises(PatternLabPendingError):
            pack_collect.update_pack(
                root, end="latest-closed", options=collector_options(),
                transport=exchange, clock=clock,
            )

    def test_a_missing_requested_start_blocks_the_whole_publication(self, tmp_path, roster):
        exchange, clock, stamps, values = build_exchange(slots=30)
        # The probe passes, then the bulk page omits the first slot.
        original = exchange._okx_candles
        state = {"probed": False}

        def sparse(params):
            if params["limit"] == "1":
                state["probed"] = True
                return original(params)
            exchange.set_rows("OKX", OKX_CONTRACT, stamps[2:], values[2:])
            return original(params)

        exchange._okx_candles = sparse
        with pytest.raises(PatternLabDataError) as failure:
            run_collect(tmp_path / "pack", roster, exchange, clock)
        assert state["probed"]
        assert failure.value.error_code == "missing_start_coverage"
        assert not (tmp_path / "pack" / pack_manifest.MANIFEST_NAME).exists()

    def test_internal_gaps_are_reported_and_never_filled(self, tmp_path, roster):
        exchange, clock, stamps, _ = build_exchange(slots=30, drop_slots=(7, 8, 20))
        result = run_collect(tmp_path / "pack", roster, exchange, clock)
        item = next(entry for entry in result["instruments"] if entry["instrument_id"] == OKX_ID)
        assert item["gap_bar_count"] == 3
        assert item["gap_range_count"] == 2
        assert item["gap_ranges"][0]["missing_bars"] == 2
        assert item["row_count"] == 27
        manifest = pack_manifest.read_manifest(tmp_path / "pack")
        assert manifest["instruments"][0]["missing_bar_count"] == 3

    def test_a_short_tail_publishes_and_is_reported(self, tmp_path, roster):
        stamps, values = synthetic_series(30)
        lagging_end = int(stamps[-1]) + STEP_MS
        exchange = FakeExchange(now_ms=lagging_end + 20 * STEP_MS + 90_000)
        exchange.add_okx(OKX_CONTRACT, timestamps=stamps, ohlcv=values)
        exchange.add_bybit(BYBIT_CONTRACT, timestamps=stamps, ohlcv=values)
        clock = FakeClock(exchange.now_ms)
        result = run_collect(tmp_path / "pack", roster, exchange, clock)
        assert result["status"] == "completed"
        assert result["tail_shortfall_bars"] == 40  # 20 bars short on each of two instruments
        assert sorted(result["instruments_with_tail_shortfall"]) == [BYBIT_ID, OKX_ID]
        readme = (tmp_path / "pack" / pack_manifest.README_NAME).read_text(encoding="utf-8")
        assert "Tail shortfall (5m bars)" in readme
        assert "| 20 |" in readme

    def test_a_no_op_preserves_every_byte_and_the_revision(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        clock.now_ms += 3 * STEP_MS
        result = pack_collect.update_pack(
            root, end="latest-closed", options=collector_options(), transport=exchange, clock=clock
        )
        assert result["status"] == "no_op"
        assert result["revision_before"] == result["revision_after"] == 1
        assert result["checked_utc"]
        after = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        assert after == before

    def test_an_unused_append_preserves_a_fixed_slice_identity(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=60)
        run_collect(root, roster, exchange, clock)
        window = {"start": utc(ANCHOR_MS), "end": utc(ANCHOR_MS + 30 * STEP_MS)}
        before = pack_data.load_slice(root, OKX_ID, **window)

        longer_stamps, longer_values = synthetic_series(90)
        exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
        exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)
        pack_collect.update_pack(
            root, end="latest-closed", options=collector_options(), transport=exchange, clock=clock
        )
        after = pack_data.load_slice(root, OKX_ID, **window)
        assert after.input_fingerprint == before.input_fingerprint
        assert len(after.bars) == len(before.bars)

    def test_a_consumed_inserted_gap_slot_changes_identity(self, tmp_path, roster):
        root = tmp_path / "pack"
        # The hole sits inside the prefix range that an earlier-start update refetches.
        exchange, clock, stamps, values = build_exchange(slots=60, drop_slots=(3,))
        run_collect(root, roster, exchange, clock)
        window = {"start": utc(ANCHOR_MS), "end": utc(ANCHOR_MS + 30 * STEP_MS)}
        before = pack_data.load_slice(root, OKX_ID, **window)
        assert before.base_gap_count == 1

        repaired_stamps, repaired_values = synthetic_series(80, first_slot=-20)
        exchange.set_rows("OKX", OKX_CONTRACT, repaired_stamps, repaired_values)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, repaired_stamps, repaired_values)
        result = pack_collect.update_pack(
            root,
            end="latest-closed",
            start=utc(ANCHOR_MS - 20 * STEP_MS),
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        inserted = next(item for item in result["instruments"] if item["instrument_id"] == OKX_ID)
        assert inserted["prefix_rows"] == 20
        assert inserted["inserted_rows"] == 1
        after = pack_data.load_slice(root, OKX_ID, **window)
        assert after.base_gap_count == 0
        assert after.input_fingerprint != before.input_fingerprint


class TestProvenance:
    def test_published_provenance_records_units_evidence_and_rules(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        manifest = pack_manifest.read_manifest(root)
        okx = pack_manifest.find_instrument(manifest, OKX_ID)
        bybit = pack_manifest.find_instrument(manifest, BYBIT_ID)
        assert "volCcyQuote" in okx["source"]["volume_unit_evidence"]
        assert "turnover" in bybit["source"]["volume_unit_evidence"]
        assert okx["source"]["input_dtype"] == "float64"
        assert okx["verification"]["volume_quote_verified"] is True
        assert okx["verification"]["closed_before_utc"] == utc(int(stamps[-1]) + STEP_MS)
        assert "confirm flag" in okx["verification"]["closure_evidence"]
        assert "confirm flag" not in bybit["verification"]["closure_evidence"]
        assert okx["instrument_rules"]["quantity_unit"] == "contracts"
        assert bybit["instrument_rules"]["quantity_unit"] == "BBB"

    def test_the_collector_object_pins_the_roster_and_request(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        collector = pack_manifest.read_manifest(root)["collector"]
        assert collector["schema_version"] == 1
        assert collector["managed_start_utc"] == utc(ANCHOR_MS)
        assert collector["last_request"]["start_utc"] == collector["managed_start_utc"]
        assert collector["roster_sha256"] == pack_manifest.roster_sha256(collector["roster"])
        assert [entry["instrument_id"] for entry in collector["roster"]] == [BYBIT_ID, OKX_ID]

    def test_readme_and_history_agree_with_the_manifest(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        manifest = pack_manifest.read_manifest(root)
        assert (root / pack_manifest.README_NAME).read_text(encoding="utf-8") == (
            pack_manifest.render_readme(manifest)
        )
        history = [
            json.loads(line)
            for line in (root / pack_manifest.UPDATES_NAME).read_text(encoding="utf-8").splitlines()
        ]
        assert len(history) == 1
        assert history[0]["event"] == "collect"
        assert history[0]["revision"] == 1
        assert history[0]["recorded_utc"] == manifest["generated_utc"]
        assert history[0]["source"]["operation_id"] == manifest["collector"]["operation_id"]

    def test_a_refreshed_observation_time_alone_is_not_a_rule_change(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        before = pack_manifest.find_instrument(pack_manifest.read_manifest(root), OKX_ID)

        longer_stamps, longer_values = synthetic_series(40)
        exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
        exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)
        pack_collect.update_pack(
            root, end="latest-closed", options=collector_options(), transport=exchange, clock=clock
        )
        after = pack_manifest.find_instrument(pack_manifest.read_manifest(root), OKX_ID)
        assert after["instrument_rules"] == before["instrument_rules"]
        assert after["row_count"] > before["row_count"]

    def test_a_changed_tick_size_publishes_new_rules(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        before_digest = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["sha256"]
        exchange.instruments[("OKX", OKX_CONTRACT)] = okx_instrument(OKX_CONTRACT, tickSz="0.01")
        clock.now_ms += STEP_MS
        result = pack_collect.update_pack(
            root, end="latest-closed", options=collector_options(), transport=exchange, clock=clock
        )
        assert result["status"] == "completed"
        assert result["revision_after"] == 2
        assert result["changed_files"] == []  # metadata only: no OHLCV file was replaced
        after = pack_manifest.find_instrument(pack_manifest.read_manifest(root), OKX_ID)
        assert after["instrument_rules"]["price_tick"] == "0.01"
        assert after["sha256"] == before_digest


class TestUpdateBoundaries:
    def test_an_archival_pack_is_never_adopted(self, tmp_path):
        from ._helpers import single_pack

        root = tmp_path / "archival"
        single_pack(root, slot_count=40)
        with pytest.raises(PatternLabDataError, match="no collector provenance"):
            pack_collect.update_pack(
                root, end=utc(ANCHOR_MS + 40 * STEP_MS), options=collector_options()
            )

    def test_unknown_extra_columns_block_an_update_but_not_a_read(
        self, tmp_path, roster, monkeypatch
    ):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        _rewrite_with_extra_column(root, OKX_ID)

        # Reading a tolerated future column keeps working.
        loaded = pack_data.load_slice(
            root, OKX_ID, start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 10 * STEP_MS)
        )
        assert len(loaded.bars) == 10

        exchange.requests.clear()
        reads = []
        original = pack_data.read_ohlcv_rows

        def counted(path, **kwargs):
            reads.append(path)
            return original(path, **kwargs)

        monkeypatch.setattr(pack_data, "read_ohlcv_rows", counted)
        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.update_pack(
                root, end="latest-closed", options=collector_options(),
                transport=exchange, clock=clock,
            )
        assert failure.value.error_code == "unsupported_extra_columns"
        assert "silently discard" in str(failure.value)
        # The guard is a schema-only column check made before any network request,
        # and the shared integrity verification read each file exactly once.
        assert exchange.requests == []
        assert len(reads) == len(pack_manifest.read_manifest(root)["instruments"])

    def test_a_later_start_or_older_end_is_already_covered(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=40)
        run_collect(root, roster, exchange, clock)
        result = pack_collect.update_pack(
            root,
            end=utc(int(stamps[20])),
            start=utc(int(stamps[10])),
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        assert result["status"] == "no_op"
        manifest = pack_manifest.read_manifest(root)
        assert manifest["revision"] == 1
        assert manifest["instruments"][0]["row_count"] == 40


def _rewrite_with_extra_column(root, instrument_id):
    """Rewrite one stored file with a tolerated future column, keeping the pack valid."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = root / pack_manifest.instrument_relative_file(instrument_id)
    table = pq.read_table(path)
    extended = table.append_column(
        "volume_base", pa.array(np.zeros(table.num_rows), type=pa.float64())
    )
    pq.write_table(extended, path, compression="zstd")
    # Keep the manifest digest truthful so the update fails on the column policy,
    # not on base integrity.
    manifest = pack_manifest.read_manifest(root)
    for entry in manifest["instruments"]:
        if entry["instrument_id"] == instrument_id:
            entry["sha256"] = pack_manifest.file_sha256(path)
    pack_manifest.write_manifest(root, manifest)


def _okx_error(code, message="synthetic"):
    return exchange_data.HttpResponse(
        status=200, body=json.dumps({"code": code, "msg": message, "data": []})
    )


def _bybit_error(code, message="synthetic"):
    return exchange_data.HttpResponse(
        status=200, body=json.dumps({"retCode": code, "retMsg": message, "result": {}})
    )


def _okx_status_error(status, payload, message="API endpoint request timeout"):
    """One non-200 OKX response carrying an error envelope, as documented."""
    body = payload if isinstance(payload, str) else json.dumps({**payload, "msg": message})
    return exchange_data.HttpResponse(status=status, body=body)


# OKX documents 50004 with HTTP 400 and inside an HTTP-200 envelope; Bybit
# documents 10000 as a server timeout on HTTP 200.
OKX_TIMEOUT_400 = _okx_status_error(400, {"code": "50004", "data": []})
OKX_TIMEOUT_200 = _okx_error("50004", "API endpoint request timeout")
BYBIT_TIMEOUT_200 = _bybit_error(10000, "Server Timeout")


class TestRequestAttemptBudget:
    """One bounded attempt budget shared by transport, HTTP and venue errors."""

    @pytest.mark.parametrize(
        "adapter, response",
        [
            (exchange_data.OkxAdapter(), _okx_error("50011", "rate limit")),
            (exchange_data.BybitAdapter(), _bybit_error(10006, "too many visits")),
        ],
    )
    def test_a_transient_business_code_is_actually_retried(self, adapter, response):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(response)
        assert adapter.server_time_ms(client) == exchange.now_ms
        assert len(exchange.requests) == 2  # the retry is a second real request
        assert clock.sleeps == [exchange_data.BACKOFF_BASE_SECONDS]

    @pytest.mark.parametrize(
        "adapter, response, fragment",
        [
            (exchange_data.OkxAdapter(), _okx_error("50013", "systems are busy"), "50013"),
            (exchange_data.BybitAdapter(), _bybit_error(10016, "server error"), "10016"),
        ],
    )
    def test_a_persistent_transient_code_exhausts_exactly_the_budget(
        self, adapter, response, fragment
    ):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)  # three attempts
        for _ in range(5):
            exchange.scripted.append(response)
        with pytest.raises(PatternLabDataError) as failure:
            adapter.server_time_ms(client)
        assert len(exchange.requests) == 3
        assert fragment in str(failure.value)
        assert "failed after 3 attempt(s) of at most 3" in str(failure.value)

    @pytest.mark.parametrize(
        "adapter, response",
        [
            (exchange_data.OkxAdapter(), OKX_TIMEOUT_400),
            (exchange_data.OkxAdapter(), OKX_TIMEOUT_200),
            (exchange_data.BybitAdapter(), BYBIT_TIMEOUT_200),
        ],
    )
    def test_a_documented_server_timeout_is_retried(self, adapter, response):
        """OKX 50004 (HTTP 400 and HTTP 200) and Bybit 10000 are transient."""
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(response)
        assert adapter.server_time_ms(client) == exchange.now_ms
        assert len(exchange.requests) == 2  # the valid second response is consumed
        assert clock.sleeps == [exchange_data.BACKOFF_BASE_SECONDS]

    @pytest.mark.parametrize(
        "adapter, response, fragment",
        [
            (exchange_data.OkxAdapter(), OKX_TIMEOUT_400, "HTTP 400 with code '50004'"),
            (exchange_data.OkxAdapter(), OKX_TIMEOUT_200, "50004"),
            (exchange_data.BybitAdapter(), BYBIT_TIMEOUT_200, "10000"),
        ],
    )
    def test_a_persistent_server_timeout_exhausts_exactly_the_budget(
        self, adapter, response, fragment
    ):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)  # three attempts
        for _ in range(5):
            exchange.scripted.append(response)
        with pytest.raises(PatternLabDataError) as failure:
            adapter.server_time_ms(client)
        assert len(exchange.requests) == 3
        assert fragment in str(failure.value)
        assert "failed after 3 attempt(s) of at most 3" in str(failure.value)

    def test_a_documented_timeout_shares_the_budget_with_http_failures(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(OKX_TIMEOUT_400)
        exchange.scripted.append(exchange_data.HttpResponse(status=503, body=""))
        exchange.scripted.append(OKX_TIMEOUT_200)
        with pytest.raises(PatternLabDataError) as failure:
            exchange_data.OkxAdapter().server_time_ms(client)
        assert len(exchange.requests) == 3  # one budget, not one per class
        assert "50004" in str(failure.value)

    def test_a_documented_timeout_is_retried_for_candles_too(self):
        """A temporary server timeout must not interrupt a long download."""
        exchange, clock, stamps, values = build_exchange(slots=6)
        client = fake_client(exchange, clock)
        exchange.scripted.append(OKX_TIMEOUT_400)
        fetched, _ = exchange_data.OkxAdapter().fetch_candles(
            client, OKX_CONTRACT, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS
        )
        assert fetched.size == stamps.size

    @pytest.mark.parametrize(
        "adapter, response, fragment",
        [
            # A generic or malformed 400 body is never parsed into a retry.
            (
                exchange_data.OkxAdapter(),
                _okx_status_error(400, {"code": "51000", "data": []}, "parameter error"),
                "HTTP 400",
            ),
            (exchange_data.OkxAdapter(), _okx_status_error(400, "{ not json"), "HTTP 400"),
            (exchange_data.OkxAdapter(), _okx_status_error(400, '{"code": "50004"}'), "HTTP 400"),
            (exchange_data.OkxAdapter(), _okx_status_error(401, {"code": "50004", "data": []}), "HTTP 401"),
            (exchange_data.OkxAdapter(), _okx_status_error(403, {"code": "50004", "data": []}), "HTTP 403"),
            # Another venue's body quoting the same digits is classified by the
            # actual adapter, not by a string match.
            (
                exchange_data.BybitAdapter(),
                _okx_status_error(400, {"code": "50004", "data": []}),
                "HTTP 400",
            ),
            (exchange_data.BybitAdapter(), _bybit_error(10001, "params error"), "10001"),
        ],
    )
    def test_a_permanent_400_is_not_widened_into_a_transient_error(self, adapter, response, fragment):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        for _ in range(5):
            exchange.scripted.append(response)
        with pytest.raises(PatternLabDataError) as failure:
            adapter.server_time_ms(client)
        assert len(exchange.requests) == 1
        assert fragment in str(failure.value)
        assert clock.sleeps == []

    def test_the_legacy_bybit_rate_limit_code_is_labelled_as_such(self):
        """10018 is struck through upstream; its handling is legacy, not required."""
        assert 10018 in exchange_data.BYBIT_LEGACY_RETRYABLE_CODES
        assert 10018 not in exchange_data.BYBIT_RETRYABLE_CODES
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        for _ in range(5):
            exchange.scripted.append(_bybit_error(10018, "ip rate limit"))
        with pytest.raises(PatternLabDataError, match="legacy transient condition"):
            exchange_data.BybitAdapter().server_time_ms(client)
        assert len(exchange.requests) == 3

    def test_http_and_business_failures_share_one_budget(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(exchange_data.HttpResponse(status=503, body=""))
        exchange.scripted.append(_okx_error("50026", "system error"))
        exchange.scripted.append(exchange_data.HttpResponse(status=429, body=""))
        with pytest.raises(PatternLabDataError) as failure:
            exchange_data.OkxAdapter().server_time_ms(client)
        # Three attempts in total, not five HTTP retries nested inside five more.
        assert len(exchange.requests) == 3
        assert "HTTP 429 rate limit" in str(failure.value)  # the final underlying error

    @pytest.mark.parametrize(
        "adapter, response, fragment",
        [
            (exchange_data.OkxAdapter(), _okx_error("51001", "no such instrument"), "51001"),
            (exchange_data.OkxAdapter(), _okx_error("50113", "invalid signature"), "50113"),
            (exchange_data.BybitAdapter(), _bybit_error(10001, "params error"), "10001"),
            (exchange_data.BybitAdapter(), _bybit_error(10002, "request time window"), "10002"),
            (exchange_data.BybitAdapter(), _bybit_error(10429, "websocket protection"), "10429"),
            (exchange_data.OkxAdapter(), exchange_data.HttpResponse(status=403, body="denied"), "HTTP 403"),
            (
                exchange_data.OkxAdapter(),
                exchange_data.HttpResponse(
                    status=0, body="", error="SSLCertVerificationError: bad chain", error_kind="tls"
                ),
                "permanent TLS",
            ),
            (
                exchange_data.OkxAdapter(),
                exchange_data.HttpResponse(
                    status=0, body="", error="ValueError: unknown url type", error_kind="configuration"
                ),
                "permanent transport configuration",
            ),
        ],
    )
    def test_a_permanent_failure_fails_on_its_first_response(self, adapter, response, fragment):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        for _ in range(5):
            exchange.scripted.append(response)
        with pytest.raises(PatternLabDataError) as failure:
            adapter.server_time_ms(client)
        assert len(exchange.requests) == 1
        assert fragment in str(failure.value)
        assert clock.sleeps == []

    def test_a_timeout_is_still_retried_while_tls_is_not(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(
            exchange_data.HttpResponse(
                status=0, body="", error="TimeoutError: read timed out", error_kind="timeout"
            )
        )
        assert exchange_data.OkxAdapter().server_time_ms(client) == exchange.now_ms
        assert len(exchange.requests) == 2

    @pytest.mark.parametrize(
        "exc, kind",
        [
            (TimeoutError("read timed out"), "timeout"),
            (ConnectionResetError("peer reset"), "connection"),
            (__import__("socket").gaierror("name resolution failed"), "connection"),
            (__import__("ssl").SSLCertVerificationError("bad chain"), "tls"),
            (ValueError("unknown url type"), "configuration"),
        ],
    )
    def test_transport_exceptions_are_classified_rather_than_lumped_together(self, exc, kind):
        assert exchange_data.classify_transport_exception(exc) == kind

    @pytest.mark.parametrize(
        "body, fragment",
        [
            ("null", "not a JSON object"),
            ("{ not json", "not valid JSON"),
            ("[]", "not a JSON object"),
        ],
    )
    def test_a_malformed_payload_names_the_protocol_problem(self, body, fragment):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(exchange_data.HttpResponse(status=200, body=body))
        with pytest.raises(PatternLabDataError) as failure:
            exchange_data.OkxAdapter().server_time_ms(client)
        assert fragment in str(failure.value)
        assert "unreachable" not in str(failure.value)
        assert len(exchange.requests) == 1

    def test_retry_after_is_honored_within_a_bounded_budget(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock)
        exchange.scripted.append(
            exchange_data.HttpResponse(status=429, body="", headers={"Retry-After": "99999"})
        )
        assert exchange_data.OkxAdapter().server_time_ms(client) == exchange.now_ms
        assert clock.sleeps == [exchange_data.RETRY_WAIT_BUDGET_SECONDS]

    @pytest.mark.parametrize("value", [True, 0, 6, 3.0, "3", None])
    def test_an_invalid_attempt_count_is_rejected_at_every_boundary(self, value):
        with pytest.raises(PatternLabDataError, match="max_attempts"):
            exchange_data.normalize_max_attempts(value)
        with pytest.raises(PatternLabDataError, match="max_attempts"):
            exchange_data.HttpClient(max_attempts=value)
        with pytest.raises(PatternLabDataError, match="max_attempts"):
            pack_collect.build_options(max_attempts=value)
        with pytest.raises(PatternLabDataError, match="max_attempts"):
            pack_collect._client_from_options(
                collector_options(max_attempts=value), transport=None, clock=None
            )

    def test_pacing_counts_a_retry_as_a_request(self):
        exchange, clock, _, _ = build_exchange(slots=3)
        client = fake_client(exchange, clock, rates={"OKX": 1.0, "BYBIT": 1.0})
        exchange.scripted.append(exchange_data.HttpResponse(status=503, body=""))
        exchange_data.OkxAdapter().server_time_ms(client)
        # The 0.5s backoff already elapsed, so pacing waits only the remaining
        # 0.5s of the one-second interval: a retry is paced like any request.
        assert clock.sleeps == [exchange_data.BACKOFF_BASE_SECONDS, 0.5]
        assert len(exchange.requests) == 2


class TestQuoteFieldEvidence:
    """The later smoke recipe's raw-response comparison, run against fixtures.

    This verifies the recipe's logic only. It is synthetic evidence about the
    snippet, never certification of a live endpoint, and no live request is made.
    """

    class _Capture:
        """The recipe's thin transport wrapper: one small raw candle sample."""

        def __init__(self, transport):
            self.transport = transport
            self.sample = None
            self.pages = 0

        def __call__(self, url, timeout):
            response = self.transport(url, timeout)
            if "candles" in url or "kline" in url:
                self.pages += 1
                if self.sample is None and response.status == 200:
                    self.sample = json.loads(response.body)  # one bounded page
            return response

    @pytest.mark.parametrize(
        "adapter, contract, quote_index, rows_of",
        [
            (exchange_data.OkxAdapter(), OKX_CONTRACT, 7, lambda body: body["data"]),
            (exchange_data.BybitAdapter(), BYBIT_CONTRACT, 6, lambda body: body["result"]["list"]),
        ],
    )
    def test_a_raw_row_confirms_the_decoded_quote_turnover(
        self, adapter, contract, quote_index, rows_of
    ):
        exchange, clock, stamps, _ = build_exchange(slots=8)
        capture = self._Capture(exchange)
        client = fake_client(exchange, clock)
        client.transport = capture
        fetched, values = adapter.fetch_candles(
            client, contract, start_ms=int(stamps[0]), end_ms=int(stamps[-1]) + STEP_MS
        )
        assert capture.pages >= 1
        assert np.isfinite(values).all()

        raw = {int(row[0]): row for row in rows_of(capture.sample)}
        matched = [int(stamp) for stamp in fetched if int(stamp) in raw]
        assert matched, "no retained closed timestamp appears in the captured page"
        stamp = matched[0]
        decoded = float(values[list(fetched).index(stamp), 4])
        assert float(raw[stamp][quote_index]) == decoded
        # The same fixture's base volume differs, so reading the wrong index is
        # detected rather than passing on a coincidence.
        assert float(raw[stamp][5]) != decoded


class TestNetworkIsolation:
    """The suite can never reach a real exchange, however a fixture is wired."""

    def test_the_default_transport_is_denied(self):
        with pytest.raises(AssertionError, match="real exchange request"):
            exchange_data.HttpClient().get_json(
                "https://example.invalid/api", {}, venue="OKX", where="probe"
            )

    def test_a_raw_urlopen_is_denied(self):
        import urllib.request

        with pytest.raises(AssertionError, match="real exchange request"):
            urllib.request.urlopen("https://example.invalid/api")

    def test_monkeypatch_undo_restores_the_guard_not_the_network(self, monkeypatch):
        monkeypatch.setattr(
            exchange_data,
            "urllib_transport",
            lambda url, timeout: exchange_data.HttpResponse(status=200, body="{}"),
        )
        assert exchange_data.urllib_transport("https://example.invalid/api", 1.0).status == 200
        monkeypatch.undo()
        with pytest.raises(AssertionError, match="real exchange request"):
            exchange_data.urllib_transport("https://example.invalid/api", 1.0)


class TestClosureCertification:
    """A publication never withdraws certification from rows it keeps."""

    def _collected_with_earlier_history(self, tmp_path, roster):
        """Collect slots 0..59 from a source that also holds slots -10..-1."""
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=70, first_slot=-10)
        run_collect(root, roster, exchange, clock, start=utc(ANCHOR_MS))
        return root, exchange, clock, stamps

    def test_a_prefix_extension_with_an_older_end_keeps_the_certified_tail(self, tmp_path, roster):
        root, exchange, clock, _ = self._collected_with_earlier_history(tmp_path, roster)
        coverage_end = utc(ANCHOR_MS + 60 * STEP_MS)
        tail = {"start": utc(ANCHOR_MS + 50 * STEP_MS), "end": coverage_end}
        fixed = {"start": utc(ANCHOR_MS + 20 * STEP_MS), "end": utc(ANCHOR_MS + 30 * STEP_MS)}
        before_tail = pack_data.load_slice(root, OKX_ID, **tail)
        before_fixed = pack_data.load_slice(root, OKX_ID, **fixed)

        result = pack_collect.update_pack(
            root,
            start=utc(ANCHOR_MS - 10 * STEP_MS),
            end=utc(ANCHOR_MS + 40 * STEP_MS),  # older than the stored coverage end
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        assert result["status"] == "completed"

        entry = pack_manifest.find_instrument(pack_manifest.read_manifest(root), OKX_ID)
        assert entry["first_open_utc"] == utc(ANCHOR_MS - 10 * STEP_MS)
        assert entry["coverage_end_utc"] == coverage_end
        # The maximum justified cutoff is preserved, not replaced by this request.
        assert entry["verification"]["closed_before_utc"] == coverage_end
        assert "retained from the previous generation" in entry["verification"]["closure_evidence"]
        assert "did not re-verify them" in entry["verification"]["closure_evidence"]
        assert "Retained tail certification" in entry["verification"]["closure_source"]

        after_tail = pack_data.load_slice(root, OKX_ID, **tail)
        assert after_tail.input_fingerprint == before_tail.input_fingerprint
        after_fixed = pack_data.load_slice(root, OKX_ID, **fixed)
        assert after_fixed.input_fingerprint == before_fixed.input_fingerprint

    def test_a_later_no_op_preserves_the_extended_pack_byte_for_byte(self, tmp_path, roster):
        root, exchange, clock, _ = self._collected_with_earlier_history(tmp_path, roster)
        window = {
            "start": utc(ANCHOR_MS - 10 * STEP_MS),
            "end": utc(ANCHOR_MS + 40 * STEP_MS),
        }
        pack_collect.update_pack(
            root, options=collector_options(), transport=exchange, clock=clock, **window
        )
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        result = pack_collect.update_pack(
            root, options=collector_options(), transport=exchange, clock=clock, **window
        )
        assert result["status"] == "no_op"
        assert pack_manifest.read_manifest(root)["revision"] == 2
        assert {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()} == before

    def test_a_fresh_collection_certifies_exactly_its_own_request(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=40)
        run_collect(root, roster, exchange, clock)
        entry = pack_manifest.find_instrument(pack_manifest.read_manifest(root), OKX_ID)
        assert entry["verification"]["closed_before_utc"] == entry["coverage_end_utc"]
        assert "retained from the previous generation" not in entry["verification"]["closure_evidence"]
        assert entry["verification"]["retained_closure"] is None


class TestRetainedClosureEvidence:
    """The retained certification lives in one flat, bounded, real record."""

    def _extended(self, tmp_path, roster):
        """A pack whose stored tail is certified later than the last request."""
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=70, first_slot=-10)
        run_collect(root, roster, exchange, clock, start=utc(ANCHOR_MS))
        original = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]
        pack_collect.update_pack(
            root,
            start=utc(ANCHOR_MS - 10 * STEP_MS),
            end=utc(ANCHOR_MS + 40 * STEP_MS),  # older than the stored coverage end
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        return root, exchange, clock, original

    def test_the_retained_record_carries_the_previous_evidence_verbatim(self, tmp_path, roster):
        root, _, _, original = self._extended(tmp_path, roster)
        verification = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]
        retained = verification["retained_closure"]
        assert sorted(retained) == list(pack_manifest.RETAINED_CLOSURE_KEYS)
        assert retained["closed_before_utc"] == verification["closed_before_utc"]
        assert retained["closure_evidence"] == original["closure_evidence"]
        assert retained["closure_source"] == original["closure_source"]
        # The generated explanation points at the real location, not history.
        assert "verification.retained_closure" in verification["closure_source"]
        assert "updates.jsonl" not in verification["closure_source"]
        history = (root / pack_manifest.UPDATES_NAME).read_text(encoding="utf-8")
        assert original["closure_evidence"] not in history  # history never stored it
        assert retained["closure_evidence"] in pack_manifest.render_readme(
            pack_manifest.read_manifest(root)
        )

    def test_a_second_older_end_extension_carries_it_forward_without_nesting(
        self, tmp_path, roster
    ):
        root, exchange, clock, original = self._extended(tmp_path, roster)
        coverage_end = utc(ANCHOR_MS + 60 * STEP_MS)
        tail = {"start": utc(ANCHOR_MS + 50 * STEP_MS), "end": coverage_end}
        before_tail = pack_data.load_slice(root, OKX_ID, **tail)
        first = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]

        exchange.set_rows("OKX", OKX_CONTRACT, *synthetic_series(90, first_slot=-30))
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, *synthetic_series(90, first_slot=-30))
        result = pack_collect.update_pack(
            root,
            start=utc(ANCHOR_MS - 30 * STEP_MS),
            end=utc(ANCHOR_MS + 45 * STEP_MS),  # still older than the stored tail
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        assert result["status"] == "completed"
        verification = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]
        retained = verification["retained_closure"]
        # The ORIGINAL evidence survives, flat and bounded: no record inside a
        # record and no second generation's generated explanation appended.
        assert retained == {
            "closed_before_utc": coverage_end,
            "closure_evidence": original["closure_evidence"],
            "closure_source": original["closure_source"],
        }
        assert first["closure_evidence"] not in retained["closure_evidence"]
        assert len(verification["closure_source"]) < 2 * len(first["closure_source"])
        assert verification["closed_before_utc"] == coverage_end
        assert pack_data.load_slice(root, OKX_ID, **tail).input_fingerprint == (
            before_tail.input_fingerprint
        )

    def test_a_genuine_new_tail_drops_the_obsolete_record(self, tmp_path, roster):
        root, exchange, clock, _ = self._extended(tmp_path, roster)
        assert pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]["retained_closure"] is not None

        stamps, values = synthetic_series(100, first_slot=-10)
        exchange.now_ms = clock.now_ms = int(stamps[-1]) + STEP_MS + 90_000
        exchange.set_rows("OKX", OKX_CONTRACT, stamps, values)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, stamps, values)
        result = run_update(root, exchange, clock)
        assert result["status"] == "completed"

        verification = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]
        assert verification["retained_closure"] is None  # this fetch certifies it all
        assert verification["closed_before_utc"] == utc(int(stamps[-1]) + STEP_MS)
        assert "retained from the previous generation" not in verification["closure_evidence"]

    def test_a_no_op_preserves_the_retained_record_byte_for_byte(self, tmp_path, roster):
        root, exchange, clock, _ = self._extended(tmp_path, roster)
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        result = pack_collect.update_pack(
            root,
            start=utc(ANCHOR_MS - 10 * STEP_MS),
            end=utc(ANCHOR_MS + 40 * STEP_MS),
            options=collector_options(),
            transport=exchange,
            clock=clock,
        )
        assert result["status"] == "no_op"
        assert {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()} == before

    def test_an_interrupted_publication_recovers_the_frozen_record(
        self, tmp_path, roster, monkeypatch
    ):
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=70, first_slot=-10)
        run_collect(root, roster, exchange, clock, start=utc(ANCHOR_MS))
        original = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]

        original_apply = update_transaction.apply_operation
        monkeypatch.setattr(
            update_transaction,
            "apply_operation",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")),
        )
        with pytest.raises(RuntimeError):
            pack_collect.update_pack(
                root,
                start=utc(ANCHOR_MS - 10 * STEP_MS),
                end=utc(ANCHOR_MS + 40 * STEP_MS),
                options=collector_options(),
                transport=exchange,
                clock=clock,
            )
        monkeypatch.setattr(update_transaction, "apply_operation", original_apply)
        journal = update_transaction.pending_state(root)["journal"]
        frozen = journal["staged"][OKX_ID]["entry"]["verification"]["retained_closure"]
        assert frozen["closure_evidence"] == original["closure_evidence"]

        pack_collect.recover_pack(root, transport=exchange, clock=clock)
        published = pack_manifest.find_instrument(
            pack_manifest.read_manifest(root), OKX_ID
        )["verification"]["retained_closure"]
        assert published == frozen  # republished, never regenerated

    @pytest.mark.parametrize(
        "record, message",
        [
            ({}, "retained closure record is closed"),
            ({"closed_before_utc": None, "closure_evidence": "e", "closure_source": "s"}, "closed_before_utc"),
            (
                {"closed_before_utc": "2026-01-01T00:00:00Z", "closure_evidence": "e", "closure_source": "s"},
                "is not the published cutoff",
            ),
        ],
    )
    def test_a_malformed_supplied_record_is_rejected(self, tmp_path, roster, record, message):
        root, _, _, _ = self._extended(tmp_path, roster)

        def mutate(manifest):
            for entry in manifest["instruments"]:
                entry["verification"]["retained_closure"] = dict(record)

        mutate_manifest(root, mutate)
        with pytest.raises(PatternLabDataError, match=message):
            pack_manifest.read_manifest(root)

    def test_an_absent_field_remains_valid(self, tmp_path, roster):
        root, _, _, _ = self._extended(tmp_path, roster)

        def mutate(manifest):
            for entry in manifest["instruments"]:
                entry["verification"].pop("retained_closure", None)

        mutate_manifest(root, mutate)
        manifest = pack_manifest.read_manifest(root)  # legacy managed entries still read
        assert "retained_closure" not in manifest["instruments"][0]["verification"]


class TestMetadataPreservation:
    """An update replaces the values it owns and preserves everything else."""

    def _decorate(self, root):
        """Add unknown top-level, per-entry and nested metadata to a live pack."""

        def mutate(manifest):
            manifest["future_top_level"] = {"kept": True}
            for entry in manifest["instruments"]:
                entry["future_entry"] = f"kept for {entry['instrument_id']}"
                entry["source"]["future_source"] = "kept source extra"
                entry["verification"]["future_verification"] = "kept verification extra"

        mutate_manifest(root, mutate)

    def test_changed_unchanged_and_rules_only_entries_all_keep_their_extras(
        self, tmp_path, roster
    ):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=40)
        run_collect(root, roster, exchange, clock)
        self._decorate(root)
        before = pack_manifest.read_manifest(root)

        # OKX gains rows; Bybit keeps its rows but publishes a new tick size.
        longer_stamps, longer_values = synthetic_series(60)
        exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
        exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
        exchange.instruments[("BYBIT", BYBIT_CONTRACT)] = bybit_instrument(
            BYBIT_CONTRACT, priceFilter={"tickSize": "0.0002"}
        )
        result = pack_collect.update_pack(
            root, end="latest-closed", options=collector_options(), transport=exchange, clock=clock
        )
        assert result["status"] == "completed"

        manifest = pack_manifest.read_manifest(root)
        assert manifest["future_top_level"] == {"kept": True}
        assert manifest["universe"] == before["universe"]
        assert manifest["collector"]["roster"] == before["collector"]["roster"]
        for entry in manifest["instruments"]:
            assert entry["future_entry"] == f"kept for {entry['instrument_id']}"
            assert entry["source"]["future_source"] == "kept source extra"
            assert entry["verification"]["future_verification"] == "kept verification extra"

        changed = pack_manifest.find_instrument(manifest, OKX_ID)
        rules_only = pack_manifest.find_instrument(manifest, BYBIT_ID)
        assert changed["row_count"] == 60
        # A canonical owned value of this operation wins over the stale one.
        assert changed["source"]["operation_id"] == result["operation_id"]
        assert rules_only["row_count"] == 40
        assert rules_only["instrument_rules"]["price_tick"] == "0.0002"
        assert rules_only["sha256"] == pack_manifest.find_instrument(before, BYBIT_ID)["sha256"]

    def test_a_no_op_preserves_extras_without_publishing_a_revision(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=40)
        run_collect(root, roster, exchange, clock)
        self._decorate(root)
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        result = pack_collect.update_pack(
            root, end="latest-closed", options=collector_options(), transport=exchange, clock=clock
        )
        assert result["status"] == "no_op"
        assert {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()} == before


class TestFailureDiagnostics:
    """A failure explains what is pending without inventing or deleting one."""

    def test_a_server_clock_failure_leaves_and_reports_no_journal(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, _, _ = build_exchange(slots=40)
        exchange.fail_time_for.add("OKX")
        with pytest.raises(PatternLabDataError) as failure:
            run_collect(root, roster, exchange, clock)
        assert "remains journalled" not in str(failure.value)
        assert update_transaction.pending_state(root) is None

    def test_a_failure_after_journalling_explains_the_phase_and_the_next_action(
        self, tmp_path, roster
    ):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        longer_stamps, longer_values = synthetic_series(40)
        exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
        conflicting = longer_values.copy()
        conflicting[25, 4] += 5.0  # slot 25 lies inside the one-hour tail overlap
        exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, conflicting)
        exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)

        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.update_pack(
                root, end="latest-closed", options=collector_options(),
                transport=exchange, clock=clock,
            )
        message = str(failure.value)
        assert failure.value.error_code == "historical_conflict"  # the original failure is kept
        assert "remains journalled in the staging phase" in message
        assert "research reads" in message
        assert "abort-update" in message
        state = update_transaction.pending_state(root)
        assert state is not None and state["valid"]  # evidence is retained, not removed
