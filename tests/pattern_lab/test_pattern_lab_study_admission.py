"""Admission, identity, sequential execution and the error boundary.

These cases use runtime-generated synthetic packs under the launcher's external
temporary root and the real coordinator, so admission ordering, recorded status
and retained evidence are observed rather than asserted structurally.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from tools.pattern_lab import (
    PatternLabBusyError,
    PatternLabDataError,
    PatternLabStudyError,
)
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import update_transaction
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.__main__ import main as cli_main
from tools.pattern_lab.study import evidence as study_evidence
from tools.pattern_lab.study import runner as study_runner

from ._helpers import (
    TWO_GREEN_EVERY_BAR,
    pending_journal,
    fixed_horizon_model,
    instrument_source,
    publish,
    study_group_ms,
    study_protocol,
    study_request,
    timeframe_bars,
    utc,
)

TIMEFRAME = 30
GROUPS = 16
WARMUP_GROUP = 0
START_GROUP = 2
END_GROUP = 12


def rising(count: int = GROUPS):
    return tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(count)
    )


def falling(count: int = GROUPS):
    return tuple(
        (100.0 - index, 100.5 - index, 97.0 - index, 99.0 - index, 10.0 + index)
        for index in range(count)
    )


def build_pack(root: Path, instruments, *, groups: int = GROUPS):
    """Publish one synthetic pack; ``instruments`` maps a contract to its specs."""
    sources = []
    for contract, options in instruments.items():
        specs = options.get("specs", rising(groups))
        stamps, values = timeframe_bars(
            TIMEFRAME,
            specs,
            drop_groups=options.get("drop_groups", ()),
            drop_slots=options.get("drop_slots", ()),
        )
        sources.append(
            instrument_source(
                stamps,
                values,
                symbol=contract.split("-")[0],
                venue="TEST",
                contract=contract,
                roles=options.get("roles", ("trading",)),
                closed_before_ms=options.get("closed_before_ms", -1),
            )
        )
    publish(root, sources)
    return root


def request_for(instrument_ids=None, *, end_group: int = END_GROUP, warmup_group: int = WARMUP_GROUP,
                timeframes=(TIMEFRAME,), models=None, start_group: int = START_GROUP):
    return study_request(
        protocol=study_protocol(
            first_ms=study_group_ms(warmup_group, TIMEFRAME),
            coverage_end_ms=study_group_ms(GROUPS + 8, TIMEFRAME),
        ),
        start_ms=study_group_ms(start_group, TIMEFRAME),
        end_ms=study_group_ms(end_group, TIMEFRAME),
        warmup_ms=study_group_ms(warmup_group, TIMEFRAME),
        timeframes=list(timeframes),
        hypotheses=[TWO_GREEN_EVERY_BAR],
        models=list(models if models is not None else [fixed_horizon_model(TIMEFRAME, [30, 60])]),
        instruments=None if instrument_ids is None else {"ids": list(instrument_ids)},
    )


# --------------------------------------------------------------------------
# preflight admission
# --------------------------------------------------------------------------

def test_a_metadata_failure_on_the_last_instrument_rejects_before_any_output(tmp_path):
    pack = build_pack(
        tmp_path / "pack",
        {
            "AAA-USDT-SWAP": {},
            # The last selected instrument has no closure evidence at all.
            "ZZZ-USDT-SWAP": {"closed_before_ms": None},
        },
    )
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.run_study(
            request=request_for(["TEST_AAA-USDT-SWAP", "TEST_ZZZ-USDT-SWAP"]),
            data_root=pack,
            output_root=run_root,
        )
    assert failure.value.error_code == "admission_failed"
    assert "TEST_ZZZ-USDT-SWAP" in str(failure.value)
    assert not run_root.exists()


def test_all_metadata_admission_failures_are_collected_together(tmp_path):
    pack = build_pack(
        tmp_path / "pack",
        {
            "AAA-USDT-SWAP": {"closed_before_ms": None},
            "BBB-USDT-SWAP": {},
            "CCC-USDT-SWAP": {"specs": rising(6)},
        },
    )
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.run_study(
            request=request_for(
                ["TEST_AAA-USDT-SWAP", "TEST_BBB-USDT-SWAP", "TEST_CCC-USDT-SWAP"]
            ),
            data_root=pack,
            output_root=tmp_path / "run",
        )
    message = str(failure.value)
    assert "TEST_AAA-USDT-SWAP" in message and "TEST_CCC-USDT-SWAP" in message


def test_an_empty_selection_fails_rather_than_producing_an_empty_run(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {"roles": ("research_only",)}})
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="resolved no instrument"):
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    assert not run_root.exists()


def test_a_factor_only_series_is_not_a_standalone_target(tmp_path):
    pack = build_pack(
        tmp_path / "pack",
        {"AAA-USDT-SWAP": {}, "BTC-USDT-SWAP": {"roles": ("factor",)}},
    )
    with pytest.raises(PatternLabDataError, match="factor-only series is context"):
        pack_study.run_study(
            request=request_for(["TEST_AAA-USDT-SWAP", "TEST_BTC-USDT-SWAP"]),
            data_root=pack,
            output_root=tmp_path / "run",
        )


def test_a_corrupt_unselected_file_blocks_the_whole_run(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    (pack / "ohlcv" / "TEST_BBB-USDT-SWAP_5m.parquet").write_bytes(b"not a parquet file")
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.run_study(
            request=request_for(["TEST_AAA-USDT-SWAP"]), data_root=pack, output_root=run_root
        )
    assert "full-pack integrity" in str(failure.value)
    assert "TEST_BBB-USDT-SWAP" in str(failure.value)
    assert not run_root.exists()
    # The integrity scope is the whole pack, and it is recorded as such.
    assert study_runner.INTEGRITY_SCOPE == "full_pack"


def test_an_existing_output_root_or_an_overlapping_one_is_refused(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    existing = tmp_path / "run"
    existing.mkdir()
    with pytest.raises(PatternLabDataError, match="must be a new directory"):
        pack_study.run_study(request=request_for(), data_root=pack, output_root=existing)
    with pytest.raises(PatternLabDataError, match="overlaps the market-data root"):
        pack_study.run_study(request=request_for(), data_root=pack, output_root=pack / "inside")


# --------------------------------------------------------------------------
# data admission after preflight
# --------------------------------------------------------------------------

def test_a_timeframe_with_no_complete_research_group_fails_the_recorded_job(tmp_path):
    # Every 30m group of the second instrument is missing one 5m slot.
    incomplete = tuple(group * 6 + 3 for group in range(GROUPS))
    pack = build_pack(
        tmp_path / "pack",
        {"AAA-USDT-SWAP": {}, "ZZZ-USDT-SWAP": {"drop_slots": incomplete}},
    )
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=request_for(["TEST_AAA-USDT-SWAP", "TEST_ZZZ-USDT-SWAP"]),
            data_root=pack,
            output_root=run_root,
        )
    assert "no complete 30m research bar" in str(failure.value.__cause__)

    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    states = {item["instrument_id"]: item["state"] for item in status["instruments"]}
    assert states == {"TEST_AAA-USDT-SWAP": "completed", "TEST_ZZZ-USDT-SWAP": "failed"}
    # The completed job survives and no completion marker is published.
    assert (run_root / "jobs" / "TEST_AAA-USDT-SWAP" / "bundle.json").is_file()
    assert not study_evidence.completion_path(run_root).is_file()


def test_a_valid_zero_event_group_completes_with_explicit_empty_evidence(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {"specs": falling()}})
    run_root = tmp_path / "run"
    result = pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    assert result["counts"]["completed"] == 1

    results = pack_study.load_results(run_root)
    emissions = results.table("TEST_AAA-USDT-SWAP", "emissions")
    assert len(emissions) == 0
    assert list(emissions.columns)  # an empty table keeps its schema
    summary = pack_study.summarize_results(results)
    assert all(group["events"] == 0 for group in summary["groups"])
    assert all(
        outcome["event_weighted"]["mean"] is None
        for group in summary["groups"]
        for outcome in group["outcomes"]
    )


def test_a_gap_re_warms_the_masks_without_invalidating_the_run(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {"drop_groups": (5,)}})
    run_root = tmp_path / "run"
    result = pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    assert result["counts"]["completed"] == 1

    results = pack_study.load_results(run_root)
    conditions = results.table("TEST_AAA-USDT-SWAP", "conditions")
    unknown = conditions.loc[conditions["anchor_open_ms"] == study_group_ms(6)]
    assert bool(unknown.iloc[0]["valid"]) is False
    assert bool(conditions.loc[conditions["anchor_open_ms"] == study_group_ms(7)].iloc[0]["valid"])
    assert len(results.table("TEST_AAA-USDT-SWAP", "episodes")) == 2


# --------------------------------------------------------------------------
# identity
# --------------------------------------------------------------------------

@pytest.mark.parametrize("timeframe", [5, 30, 120])
def test_in_memory_preparation_matches_the_reader_exactly(tmp_path, timeframe):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}}, groups=GROUPS)
    start = study_group_ms(4, TIMEFRAME)
    end = study_group_ms(12, TIMEFRAME)
    warmup = study_group_ms(0, TIMEFRAME)
    reference = pack_data.load_slice(
        pack, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end),
        warmup_start=utc(warmup), timeframe_minutes=timeframe,
    )
    base = pack_data.load_slice(
        pack, "TEST_AAA-USDT-SWAP", start=utc(start), end=utc(end),
        warmup_start=utc(warmup), timeframe_minutes=5,
    )
    prepared = pack_data.prepare_series(
        instrument_id="TEST_AAA-USDT-SWAP",
        venue="TEST",
        contract="AAA-USDT-SWAP",
        quote_currency="USDT",
        timestamps=base.bars.index.view("int64") // 1_000_000,
        ohlcv=base.bars.to_numpy(dtype=np.float64),
        start_ms=start,
        end_ms=end,
        warmup_start_ms=warmup,
        timeframe_minutes=timeframe,
    )
    assert prepared.input_fingerprint == reference.input_fingerprint
    assert np.array_equal(prepared.timestamps, reference.bars.index.view("int64") // 1_000_000)
    assert np.allclose(prepared.values, reference.bars.to_numpy(dtype=np.float64))
    assert prepared.omitted_group_count == reference.omitted_group_count
    assert prepared.research_start_index == reference.research_start_index


def run_identity(pack: Path, run_root: Path, **kwargs) -> str:
    result = pack_study.run_study(
        request=request_for(**kwargs), data_root=pack, output_root=run_root
    )
    return result["identities"]["data_input_sha256"]


def test_appended_out_of_range_data_and_a_moved_root_keep_the_same_identity(tmp_path):
    short = build_pack(tmp_path / "short", {"AAA-USDT-SWAP": {"specs": rising(GROUPS)}})
    longer = build_pack(tmp_path / "longer", {"AAA-USDT-SWAP": {"specs": rising(GROUPS + 20)}})
    first = run_identity(short, tmp_path / "run-short")
    second = run_identity(longer, tmp_path / "run-longer")
    assert first == second

    moved = tmp_path / "moved"
    short.rename(moved)
    assert run_identity(moved, tmp_path / "run-moved") == first


def test_an_omitted_incomplete_group_and_a_repaired_warmup_bar_change_the_identity(tmp_path):
    complete = build_pack(tmp_path / "complete", {"AAA-USDT-SWAP": {}})
    # One 5m slot inside the research window is missing: its group is omitted,
    # but the raw consumed rows still enter research input identity.
    omitted = build_pack(
        tmp_path / "omitted", {"AAA-USDT-SWAP": {"drop_slots": (6 * 6 + 2,)}}
    )
    # One consumed warmup slot is missing: repairing it changes identity too.
    warmup_gap = build_pack(tmp_path / "warmup", {"AAA-USDT-SWAP": {"drop_slots": (3,)}})

    baseline = run_identity(complete, tmp_path / "run-complete")
    assert run_identity(omitted, tmp_path / "run-omitted") != baseline
    assert run_identity(warmup_gap, tmp_path / "run-warmup") != baseline


def test_the_worker_count_and_the_roots_are_provenance_not_identity(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=tmp_path / "run"
    )
    provenance = json.loads((Path(result["run_root"]) / "provenance.json").read_text())
    assert provenance["workers"] == 1
    assert provenance["execution"]["requested_workers"] == 1
    assert provenance["execution"]["effective_workers"] == 1
    assert provenance["execution"]["mode"] == "direct"
    assert provenance["integrity_scope"] == "full_pack"
    assert provenance["data_root"] == str(pack.resolve())
    identity_document = json.loads((Path(result["run_root"]) / "spec" / "request.json").read_text())
    assert "data_root" not in identity_document and "workers" not in identity_document


# --------------------------------------------------------------------------
# sequential execution and the error boundary
# --------------------------------------------------------------------------

@pytest.mark.parametrize("workers", [0, -1, True, 1.0, "2", None])
def test_only_a_positive_integer_worker_count_is_accepted(tmp_path, workers):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="workers"):
        pack_study.run_study(
            request=request_for(), data_root=pack, output_root=run_root, workers=workers
        )
    assert not run_root.exists()


def test_a_competing_reader_fails_while_the_coordinator_session_is_held(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    seen: list[str] = []
    original = study_runner.run_instrument_job

    def probing_job(payload):
        try:
            pack_data.inspect_pack(pack)
        except PatternLabBusyError as exc:
            seen.append(str(exc))
        else:  # pragma: no cover - the guard must be held for the whole run
            raise AssertionError("the coordinator's read session did not hold the pack lock")
        return original(payload)

    study_runner.run_instrument_job = probing_job
    try:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=tmp_path / "run")
    finally:
        study_runner.run_instrument_job = original
    assert len(seen) == 2


def test_an_ordinary_job_exception_stops_dispatch_and_records_honest_state(tmp_path):
    pack = build_pack(
        tmp_path / "pack",
        {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}, "CCC-USDT-SWAP": {}},
    )
    run_root = tmp_path / "run"
    original = study_runner.run_instrument_job

    def failing_job(payload):
        if payload.instrument_id == "TEST_BBB-USDT-SWAP":
            raise ValueError("synthetic numerical failure")
        return original(payload)

    study_runner.run_instrument_job = failing_job
    try:
        with pytest.raises(PatternLabStudyError) as failure:
            pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    finally:
        study_runner.run_instrument_job = original

    assert isinstance(failure.value.__cause__, ValueError)
    assert failure.value.context["instrument_id"] == "TEST_BBB-USDT-SWAP"
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    states = {item["instrument_id"]: item["state"] for item in status["instruments"]}
    assert states == {
        "TEST_AAA-USDT-SWAP": "completed",
        "TEST_BBB-USDT-SWAP": "failed",
        "TEST_CCC-USDT-SWAP": "not_started",
    }
    assert (run_root / "jobs" / "TEST_AAA-USDT-SWAP").is_dir()
    assert not (run_root / "jobs" / "TEST_CCC-USDT-SWAP").exists()
    assert not study_evidence.completion_path(run_root).is_file()


def test_a_keyboard_interrupt_records_the_interruption_and_keeps_finished_work(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    run_root = tmp_path / "run"
    original = study_runner.run_instrument_job

    def interrupting_job(payload):
        if payload.instrument_id == "TEST_BBB-USDT-SWAP":
            raise KeyboardInterrupt
        return original(payload)

    study_runner.run_instrument_job = interrupting_job
    try:
        with pytest.raises(KeyboardInterrupt):
            pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    finally:
        study_runner.run_instrument_job = original

    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "interrupted"
    assert (run_root / "jobs" / "TEST_AAA-USDT-SWAP" / "bundle.json").is_file()
    assert not study_evidence.completion_path(run_root).is_file()


def test_the_numerical_job_needs_no_pack_and_writes_no_output(tmp_path):
    from ._helpers import study_job

    _request, payload = study_job(rising(9), start_group=2, end_group=9)
    before = sorted(item.name for item in tmp_path.iterdir())
    result = pack_study.run_instrument_job(payload)
    assert sorted(item.name for item in tmp_path.iterdir()) == before
    assert set(result.tables) >= {"conditions", "emissions", "episodes", "primitives"}
    assert not hasattr(payload, "data_root")


# --------------------------------------------------------------------------
# CLI exit contract
# --------------------------------------------------------------------------

def write_spec(path: Path, document) -> Path:
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    return path


def test_the_cli_maps_success_failures_busy_and_pending_states(tmp_path, capsys):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    spec = write_spec(tmp_path / "study.json", request_for())

    assert cli_main([
        "study", "--spec", str(spec), "--data-root", str(pack),
        "--output-root", str(tmp_path / "run"), "--workers", "1",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "completed"

    assert cli_main(["report", "--run-root", str(tmp_path / "run")]) == 0
    capsys.readouterr()

    # An existing output root is exit 2 with a structured JSON status.
    assert cli_main([
        "study", "--spec", str(spec), "--data-root", str(pack),
        "--output-root", str(tmp_path / "run"), "--workers", "1",
    ]) == 2
    failed = json.loads(capsys.readouterr().out)
    assert failed["status"] == "failed" and failed["command"] == "study"

    # A nonpositive worker count is rejected before anything is created.
    assert cli_main([
        "study", "--spec", str(spec), "--data-root", str(pack),
        "--output-root", str(tmp_path / "run-zero"), "--workers", "0",
    ]) == 2
    assert "workers" in json.loads(capsys.readouterr().out)["error"]
    assert not (tmp_path / "run-zero").exists()

    # A busy pack stays exit 3 and a pending operation stays exit 4.
    with pack_data.read_session(pack):
        assert cli_main([
            "study", "--spec", str(spec), "--data-root", str(pack),
            "--output-root", str(tmp_path / "run-busy"), "--workers", "1",
        ]) == 3
    assert json.loads(capsys.readouterr().out)["error_code"] == "pack_busy"

    update_transaction.write_journal(pack, pending_journal(pack))
    assert cli_main([
        "study", "--spec", str(spec), "--data-root", str(pack),
        "--output-root", str(tmp_path / "run-pending"), "--workers", "1",
    ]) == 4
    assert json.loads(capsys.readouterr().out)["error_code"] == "pending_operation"
    assert not (tmp_path / "run-pending").exists()


def test_a_keyboard_interrupt_in_the_cli_is_exit_130(tmp_path, capsys, monkeypatch):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    spec = write_spec(tmp_path / "study.json", request_for())

    def interrupt(**kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(pack_study, "run_study", interrupt)
    assert cli_main([
        "study", "--spec", str(spec), "--data-root", str(pack),
        "--output-root", str(tmp_path / "run"), "--workers", "1",
    ]) == 130
    assert json.loads(capsys.readouterr().out)["status"] == "interrupted"


def test_the_cli_refuses_a_report_of_a_failed_or_partial_run(tmp_path, capsys):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    run_root = tmp_path / "run"
    original = study_runner.run_instrument_job

    def failing_job(payload):
        if payload.instrument_id == "TEST_BBB-USDT-SWAP":
            raise ValueError("synthetic numerical failure")
        return original(payload)

    study_runner.run_instrument_job = failing_job
    try:
        with pytest.raises(PatternLabStudyError):
            pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    finally:
        study_runner.run_instrument_job = original

    assert cli_main(["report", "--run-root", str(run_root)]) == 2
    assert json.loads(capsys.readouterr().out)["error_code"] == "incomplete_run"
