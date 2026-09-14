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

from ._helpers import (
    ANCHOR_MS,
    BYBIT_CONTRACT,
    OKX_CONTRACT,
    STEP_MS,
    FakeClock,
    FakeExchange,
    bybit_instrument,
    collector_options,
    fake_client,
    okx_instrument,
    roster_document,
    synthetic_series,
    utc,
    write_roster,
)

OKX_ID = f"OKX_{OKX_CONTRACT}"
BYBIT_ID = f"BYBIT_{BYBIT_CONTRACT}"


def build_exchange(*, slots=120, first_slot=0, lag_ms=90_000, drop_slots=(), now_ms=None):
    """Return ``(exchange, clock, stamps, values)`` for a two-venue fixture."""
    stamps, values = synthetic_series(slots, first_slot=first_slot, drop_slots=drop_slots)
    moment = int(now_ms if now_ms is not None else int(stamps[-1]) + STEP_MS + lag_ms)
    exchange = FakeExchange(now_ms=moment)
    exchange.add_okx(OKX_CONTRACT, timestamps=stamps, ohlcv=values)
    exchange.add_bybit(BYBIT_CONTRACT, timestamps=stamps, ohlcv=values)
    return exchange, FakeClock(moment), stamps, values


def run_collect(root, roster_path, exchange, clock, *, start=None, end="latest-closed", **kwargs):
    return pack_collect.collect_pack(
        root,
        start=start if start is not None else utc(ANCHOR_MS),
        end=end,
        roster_path=roster_path,
        options=collector_options(),
        transport=exchange,
        clock=clock,
        **kwargs,
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
        for rules in (okx, bybit):
            assert rules["contract_type"] == "linear_perpetual"
            assert rules["quote_currency"] == rules["settlement_currency"] == "USDT"
            pack_manifest.validate_instrument_rules(rules, "rules")

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

    def test_unknown_extra_columns_block_an_update_but_not_a_read(self, tmp_path, roster):
        root = tmp_path / "pack"
        exchange, clock, stamps, _ = build_exchange(slots=30)
        run_collect(root, roster, exchange, clock)
        _rewrite_with_extra_column(root, OKX_ID)

        # Reading a tolerated future column keeps working.
        loaded = pack_data.load_slice(
            root, OKX_ID, start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 10 * STEP_MS)
        )
        assert len(loaded.bars) == 10

        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.update_pack(
                root, end="latest-closed", options=collector_options(),
                transport=exchange, clock=clock,
            )
        assert failure.value.error_code == "unsupported_extra_columns"
        assert "silently discard" in str(failure.value)

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
