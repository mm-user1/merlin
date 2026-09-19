"""Descriptive aggregation, immutable evidence and regenerable reports."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.__main__ import main as cli_main
from tools.pattern_lab.study import evidence as study_evidence
from tools.pattern_lab.study import runner as study_runner

from ._helpers import (
    TWO_GREEN_EVERY_BAR,
    fixed_horizon_model,
    instrument_source,
    publish,
    study_group_ms,
    study_protocol,
    study_request,
    timeframe_bars,
)

TIMEFRAME = 30
GROUPS = 16
BUSY = "AAA-USDT-SWAP"
QUIET = "BBB-USDT-SWAP"


def rising(count: int = GROUPS):
    return tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(count)
    )


def mostly_red(count: int = GROUPS):
    """Only groups 6, 7 and 8 form a green run with rising quote volume."""
    specs = []
    for index in range(count):
        if index in (6, 7, 8):
            specs.append((100.0 + index, 103.0 + index, 98.0 + index, 102.0 + index, 50.0 + index))
        else:
            specs.append((100.0 - index, 100.5 - index, 96.0 - index, 98.0 - index, 5.0))
    return tuple(specs)


def build_pack(root: Path):
    sources = []
    for contract, specs in ((BUSY, rising()), (QUIET, mostly_red())):
        stamps, values = timeframe_bars(TIMEFRAME, specs)
        sources.append(
            instrument_source(
                stamps, values, symbol=contract.split("-")[0], venue="TEST", contract=contract
            )
        )
    publish(root, sources)
    return root


def request_for(*, study_name: str = "aggregation study", horizons=(30, 60), primary=60):
    return study_request(
        protocol=study_protocol(
            first_ms=study_group_ms(0, TIMEFRAME),
            coverage_end_ms=study_group_ms(GROUPS + 8, TIMEFRAME),
        ),
        start_ms=study_group_ms(2, TIMEFRAME),
        end_ms=study_group_ms(12, TIMEFRAME),
        warmup_ms=study_group_ms(0, TIMEFRAME),
        timeframes=[TIMEFRAME],
        hypotheses=[TWO_GREEN_EVERY_BAR],
        models=[fixed_horizon_model(TIMEFRAME, list(horizons), primary=primary)],
        study_name=study_name,
    )


@pytest.fixture
def completed_run(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    return pack, run_root


# --------------------------------------------------------------------------
# aggregation
# --------------------------------------------------------------------------

def test_event_weighted_and_equal_ticker_summaries_differ_on_unequal_samples(completed_run):
    _pack, run_root = completed_run
    results = pack_study.load_results(run_root)
    summary = pack_study.summarize_results(results)
    group = next(item for item in summary["groups"] if item["case_id"] == "tf30m.h60m.long")

    samples = {}
    for instrument_id in (f"TEST_{BUSY}", f"TEST_{QUIET}"):
        frame = results.observations(
            instrument_id,
            model_instance_id="fh",
            timeframe_minutes=TIMEFRAME,
            case_id="tf30m.h60m.long",
            variant_id="two_green_every",
        )
        values = frame.loc[frame["return_valid"], "gross_return"].to_numpy(dtype=np.float64)
        samples[instrument_id] = values[np.isfinite(values)]
    assert samples[f"TEST_{BUSY}"].size != samples[f"TEST_{QUIET}"].size
    assert samples[f"TEST_{QUIET}"].size > 0

    pooled = np.concatenate(list(samples.values()))
    outcome = next(item for item in group["outcomes"] if item["name"] == "gross_return")
    assert outcome["event_weighted"]["n"] == int(pooled.size)
    assert outcome["event_weighted"]["mean"] == pytest.approx(float(np.mean(pooled)))
    assert outcome["event_weighted"]["median"] == pytest.approx(float(np.median(pooled)))
    for label, quantile in (("p10", 0.10), ("p25", 0.25), ("p75", 0.75), ("p90", 0.90)):
        assert outcome["event_weighted"][label] == pytest.approx(
            float(np.quantile(pooled, quantile, method="linear"))
        )

    ticker_means = [float(np.mean(values)) for values in samples.values()]
    assert outcome["equal_ticker"]["tickers"] == 2
    assert outcome["equal_ticker"]["mean_of_ticker_means"] == pytest.approx(
        float(np.mean(ticker_means))
    )
    assert outcome["equal_ticker"]["mean_of_ticker_means"] != pytest.approx(
        outcome["event_weighted"]["mean"]
    )
    assert group["events"] == sum(
        len(
            results.observations(
                instrument_id,
                model_instance_id="fh",
                timeframe_minutes=TIMEFRAME,
                case_id="tf30m.h60m.long",
                variant_id="two_green_every",
            )
        )
        for instrument_id in (f"TEST_{BUSY}", f"TEST_{QUIET}")
    )


def test_invalid_outcomes_are_counted_by_reason_and_never_read_as_zero(completed_run):
    _pack, run_root = completed_run
    summary = pack_study.summarize_results(pack_study.load_results(run_root))
    group = next(item for item in summary["groups"] if item["case_id"] == "tf30m.h60m.long")
    outcome = next(item for item in group["outcomes"] if item["name"] == "net_return")
    assert outcome["invalid_count"] == group["events"] - outcome["valid_count"]
    assert set(outcome["invalid_by_reason"]) <= {
        "terminal_study_end", "missing_entry_bar", "incomplete_path"
    }
    assert sum(outcome["invalid_by_reason"].values()) == outcome["invalid_count"]


def test_a_group_without_support_stays_visible_with_null_metrics(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    # A horizon longer than the whole study leaves every outcome censored.
    pack_study.run_study(
        request=request_for(horizons=(30, 3000), primary=30),
        data_root=pack,
        output_root=run_root,
    )
    summary = pack_study.summarize_results(pack_study.load_results(run_root))
    censored = next(item for item in summary["groups"] if item["case_id"] == "tf30m.h3000m.long")
    outcome = next(item for item in censored["outcomes"] if item["name"] == "net_return")
    assert censored["events"] > 0
    assert outcome["valid_count"] == 0
    assert outcome["event_weighted"]["mean"] is None
    assert outcome["event_weighted"]["fraction_positive"] is None
    assert outcome["invalid_by_reason"] == {"terminal_study_end": censored["events"]}
    assert sorted(censored["zero_support_tickers"]) == [f"TEST_{BUSY}", f"TEST_{QUIET}"]


def test_the_summary_follows_the_declared_family_and_its_declared_primary(completed_run):
    _pack, run_root = completed_run
    results = pack_study.load_results(run_root)
    summary = pack_study.summarize_results(results)
    assert [group["case_id"] for group in summary["groups"]] == [
        "tf30m.h30m.long", "tf30m.h30m.short", "tf30m.h60m.long", "tf30m.h60m.short"
    ]
    primaries = {group["case_id"] for group in summary["groups"] if group["primary"]}
    assert primaries == {"tf30m.h60m.long", "tf30m.h60m.short"}
    assert summary["quantile_convention"] == "NumPy linear empirical quantiles"
    assert summary["minimum_support"] == "One valid event per group and ticker."


# --------------------------------------------------------------------------
# the report
# --------------------------------------------------------------------------

def test_the_report_is_offline_escaped_and_states_its_limits(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    pack_study.run_study(
        request=request_for(study_name='<script>alert("x")</script> & co'),
        data_root=pack,
        output_root=run_root,
    )
    html = (run_root / "derived" / "report.html").read_text(encoding="utf-8")

    assert "Descriptive event study — statistical validation is not implemented in M2." in html
    assert "<script>alert" not in html
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt; &amp; co" in html
    assert "http://" not in html and "https://" not in html
    assert "slippage and funding are excluded" in html.lower()
    assert "overlap" in html.lower()
    # No inferential or portfolio language belongs in this model.
    for forbidden in ("confidence interval", "Sharpe", "CAGR", "equity curve", "p-value"):
        assert forbidden.lower() not in html.lower().replace(
            "not an executable equity curve", ""
        )
    assert "tf30m.h60m.long" in html and "declared primary case" in html


def test_report_regeneration_needs_no_pack_and_changes_no_immutable_evidence(completed_run):
    pack, run_root = completed_run
    record_before = json.loads((run_root / "completion.json").read_text())
    status_before = (run_root / "status.json").read_bytes()

    # Remove the derived outputs, then make the pack unreadable to this caller.
    (run_root / "derived" / "report.html").unlink()
    (run_root / "derived" / "summary.json").write_text("{}", encoding="utf-8")
    for path in (pack / "ohlcv").iterdir():
        path.unlink()

    result = pack_study.regenerate_report(run_root)
    assert result["status"] == "regenerated"
    assert (run_root / "derived" / "report.html").is_file()
    assert json.loads((run_root / "completion.json").read_text()) == record_before
    assert (run_root / "status.json").read_bytes() == status_before
    study_evidence.verify_completion(run_root)


def test_the_completion_record_covers_every_immutable_file_and_never_itself(completed_run):
    _pack, run_root = completed_run
    record = json.loads((run_root / "completion.json").read_text())
    recorded = set(record["evidence_sha256"])
    assert "completion.json" not in recorded
    assert "derived/summary.json" not in recorded and "derived/report.html" not in recorded
    assert {"spec/request.json", "spec/protocol.json", "spec/family.json", "status.json"} <= recorded
    assert f"jobs/TEST_{BUSY}/bundle.json" in recorded
    assert f"admitted/TEST_{BUSY}.json" in recorded
    assert record["terminal_status"] == "completed"


def test_corrupt_raw_evidence_is_rejected_and_blocks_regeneration(completed_run):
    _pack, run_root = completed_run
    target = run_root / "jobs" / f"TEST_{BUSY}" / "primitives.parquet"
    target.write_bytes(b"corrupted")

    with pytest.raises(PatternLabDataError) as failure:
        pack_study.load_results(run_root)
    assert failure.value.error_code == "corrupt_evidence"
    with pytest.raises(PatternLabDataError) as regeneration:
        pack_study.regenerate_report(run_root)
    assert regeneration.value.error_code == "corrupt_evidence"


def test_the_partial_api_exposes_missing_jobs_without_hiding_corruption(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    original = study_runner.run_instrument_job

    def failing_job(payload):
        if payload.instrument_id == f"TEST_{QUIET}":
            raise ValueError("synthetic numerical failure")
        return original(payload)

    study_runner.run_instrument_job = failing_job
    try:
        with pytest.raises(Exception):
            pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    finally:
        study_runner.run_instrument_job = original

    with pytest.raises(PatternLabDataError) as strict:
        pack_study.load_results(run_root)
    assert strict.value.error_code == "incomplete_run"

    partial = pack_study.load_results(run_root, allow_partial=True)
    assert partial.complete is False
    assert partial.counts["completed"] == 1 and partial.counts["failed"] == 1
    assert partial.completed_instruments == [f"TEST_{BUSY}"]
    summary = pack_study.summarize_results(partial)
    assert summary["complete"] is False

    # Partial mode is not a corruption bypass.
    (run_root / "jobs" / f"TEST_{BUSY}" / "conditions.parquet").write_bytes(b"corrupt")
    with pytest.raises(PatternLabDataError) as corrupt:
        pack_study.load_results(run_root, allow_partial=True)
    assert corrupt.value.error_code == "corrupt_evidence"


def test_empty_tables_keep_their_schema_and_use_utc_float64_evidence(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    results = pack_study.load_results(run_root)
    primitives = results.table(f"TEST_{BUSY}", "primitives")
    assert primitives["entry_price"].dtype == np.float64
    assert primitives["anchor_open_ms"].dtype == np.int64
    assert primitives["return_valid"].dtype == np.bool_

    conditions = results.table(f"TEST_{QUIET}", "conditions")
    assert list(conditions.columns) == [
        "instrument_id", "timeframe_minutes", "condition_id", "anchor_open_ms",
        "signal_time_ms", "value", "valid", "episode_id",
    ]
    summary_text = (run_root / "derived" / "summary.json").read_text(encoding="utf-8")
    assert "NaN" not in summary_text and "Infinity" not in summary_text


def test_the_report_command_regenerates_only_the_derived_files(completed_run, capsys):
    _pack, run_root = completed_run
    before = {
        path.name: path.read_bytes()
        for path in (run_root / "jobs" / f"TEST_{BUSY}").iterdir()
    }
    (run_root / "derived" / "report.html").write_text("stale", encoding="utf-8")
    assert cli_main(["report", "--run-root", str(run_root)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "regenerated"
    assert "stale" not in (run_root / "derived" / "report.html").read_text(encoding="utf-8")
    after = {
        path.name: path.read_bytes()
        for path in (run_root / "jobs" / f"TEST_{BUSY}").iterdir()
    }
    assert after == before
