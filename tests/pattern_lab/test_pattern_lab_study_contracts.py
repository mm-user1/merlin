"""Enforcement of the study's advertised boundary contracts (R1-R7).

These cases drive the real coordinator over runtime-generated synthetic packs
and task-owned trusted extension modules.  They cover the boundaries a passing
happy path cannot reach: a custom model's returned rows, saved evidence decoded
again later, every accepted public request form, used-source attribution,
resolved dependency warmup, truthful failure and partial progress, and bounded
evidence reuse.

Each case writes its own extension module under a unique name, because a Python
interpreter imports a module once and that is exactly the condition the study's
fresh-interpreter source error describes.
"""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import re
import shutil
import textwrap

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabStudyError
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.study import contracts as study_contracts
from tools.pattern_lab.study import evidence as study_evidence
from tools.pattern_lab.study import report as study_report
from tools.pattern_lab.study import results as study_results
from tools.pattern_lab.study import runner as study_runner
from tools.pattern_lab.study import spec as study_spec
from tools.pattern_lab.study import validation as study_validation

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

HEADER = """
import numpy as np

from tools.pattern_lab.study import contracts
from tools.pattern_lab.study.contracts import (
    ConditionValue,
    FeatureDescriptor,
    FeatureRequest,
    FeatureValue,
    HypothesisDescriptor,
    MetricDescriptor,
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
)
"""


def write_module(root: Path, name: str, body: str, *, helpers=None) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{name}.py").write_text(HEADER + textwrap.dedent(body), encoding="utf-8")
    for helper_name, helper_body in (helpers or {}).items():
        (root / helper_name).write_text(textwrap.dedent(helper_body), encoding="utf-8")
    return root / f"{name}.py"


def declaration(root: Path, module: str, helpers=()):
    return {"module": module, "source_root": str(root), "helpers": list(helpers)}


def rising(count: int = GROUPS):
    return tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(count)
    )


def build_pack(root: Path, contracts_map=None, *, drop_groups=()):
    sources = []
    for contract, specs in (contracts_map or {"AAA-USDT-SWAP": rising()}).items():
        stamps, values = timeframe_bars(TIMEFRAME, specs, drop_groups=drop_groups)
        sources.append(
            instrument_source(
                stamps, values, symbol=contract.split("-")[0], venue="TEST", contract=contract
            )
        )
    publish(root, sources)
    return root


def request_for(*, hypotheses=None, models=None, metrics=None, extensions=None,
                start_group: int = 2, end_group: int = 12, warmup_group: int = 0):
    return study_request(
        protocol=study_protocol(
            first_ms=study_group_ms(warmup_group, TIMEFRAME),
            coverage_end_ms=study_group_ms(GROUPS + 8, TIMEFRAME),
        ),
        start_ms=study_group_ms(start_group, TIMEFRAME),
        end_ms=study_group_ms(end_group, TIMEFRAME),
        warmup_ms=study_group_ms(warmup_group, TIMEFRAME),
        timeframes=[TIMEFRAME],
        hypotheses=list(hypotheses if hypotheses is not None else [TWO_GREEN_EVERY_BAR]),
        models=list(models if models is not None else [fixed_horizon_model(TIMEFRAME, [30, 60])]),
        metrics=metrics,
        extensions=extensions,
    )


# --------------------------------------------------------------------------
# R1: a custom model's returned evidence
# --------------------------------------------------------------------------

# One declared case, one outcome; ``mode`` injects exactly one defect.
BROKEN_MODEL = '''
def _validate(settings, timeframes):
    contracts.closed_keys(settings, ("mode",), "settings")
    return {"mode": str(settings["mode"])}


def _cases(settings, timeframe):
    return (
        ModelCase(
            case_id="only",
            timeframe_minutes=int(timeframe),
            parameters={},
            outcomes=(OutcomeSpec("score", "fraction", "synthetic score"),),
            primary=True,
        ),
    )


def _evaluate(series, settings, anchors):
    mode = settings["mode"]
    stamps = anchors.open_ms.copy()
    if mode == "duplicate":
        stamps = np.concatenate([stamps, stamps[:1]])
    if mode == "missing":
        stamps = stamps[1:]
    values = np.ones(stamps.size, dtype=np.float64)
    reasons = np.full(stamps.size, "available", dtype=object)
    case_ids = np.full(stamps.size, "only", dtype=object)
    if mode == "unknown_case" and case_ids.size:
        case_ids[0] = "not_a_case"
    if mode == "infinity" and values.size:
        values[0] = np.inf
    if mode == "finite_with_reason" and values.size:
        reasons[0] = "missing_next_bar"
    if mode == "null_with_available" and values.size:
        values[0] = np.nan
    if mode == "float_timestamps":
        stamps = stamps.astype(np.float64)
    if mode == "boolean_timestamps":
        stamps = np.ones(stamps.size, dtype=bool)
    if mode == "blank_reason" and reasons.size:
        values[0] = np.nan
        reasons[0] = "   "
    rows = {
        "case_id": case_ids,
        "anchor_open_ms": stamps,
        "score": values,
        "score__reason": reasons,
    }
    if mode == "extra_column":
        rows["surprise"] = np.zeros(stamps.size, dtype=np.float64)
    if mode == "wrong_kind":
        return ModelEvidence(kind=contracts.FIXED_HORIZON_EVIDENCE_KIND, rows=rows)
    return ModelEvidence(kind=contracts.CUSTOM_CASE_EVIDENCE_KIND, rows=rows)


def register(context):
    context.register_model(
        ModelDescriptor(
            model_id="{model_id}",
            version="1",
            validate_settings=_validate,
            resolve_cases=_cases,
            evaluate=_evaluate,
        )
    )
'''

# Two cases with different outcome sets, mixed availability and model-owned reasons.
MIXED_SUPPORT_MODEL = '''
_BOTH = (
    OutcomeSpec("gap", "fraction", "next open over anchor close"),
    OutcomeSpec("span", "fraction", "bar range over close"),
)
_ONE = (OutcomeSpec("gap", "fraction", "next open over anchor close"),)


def _validate(settings, timeframes):
    contracts.closed_keys(settings, (), "settings")
    return {}


def _cases(settings, timeframe):
    return (
        ModelCase("wide", int(timeframe), {}, _BOTH, primary=True),
        ModelCase("narrow", int(timeframe), {}, _ONE),
    )


def _evaluate(series, settings, anchors):
    rows = anchors.rows
    count = int(rows.size)
    total = series.row_count
    following = rows + 1
    safe = np.minimum(following, max(total - 1, 0))
    present = (following < total) & (series.slots[safe] - series.slots[rows] == 1)
    gap = np.full(count, np.nan, dtype=np.float64)
    if present.any():
        gap[present] = series.open[following[present]] / series.close[rows[present]] - 1.0
    gap_reason = np.where(present, "available", "no_following_bar").astype(object)
    span = (series.high[rows] - series.low[rows]) / series.close[rows]
    span_reason = np.full(count, "available", dtype=object)

    stamps = np.concatenate([anchors.open_ms, anchors.open_ms]).astype(np.int64)
    case_ids = np.concatenate([
        np.full(count, "wide", dtype=object), np.full(count, "narrow", dtype=object)
    ])
    # 'narrow' does not declare 'span': that union column stays null with an
    # explicit model-owned nonavailable reason.
    span_values = np.concatenate([span, np.full(count, np.nan)])
    span_reasons = np.concatenate([
        span_reason, np.full(count, "not_measured_for_narrow", dtype=object)
    ])
    return ModelEvidence(
        kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
        rows={
            "case_id": case_ids,
            "anchor_open_ms": stamps,
            "gap": np.concatenate([gap, gap]),
            "gap__reason": np.concatenate([gap_reason, gap_reason]),
            "span": span_values,
            "span__reason": span_reasons,
        },
    )


def _share(frame):
    values = frame["gap"].to_numpy(dtype=np.float64)
    values = values[np.isfinite(values)]
    return None if values.size == 0 else float(np.count_nonzero(values > 0.0) / values.size)


def _count(frame):
    return float(len(frame))


def register(context):
    context.register_model(
        ModelDescriptor(
            model_id="{model_id}",
            version="1",
            validate_settings=_validate,
            resolve_cases=_cases,
            evaluate=_evaluate,
        )
    )
    context.register_metric(
        MetricDescriptor("{model_id}_share", "1", ("gap",), "fraction", _share)
    )
    context.register_metric(
        MetricDescriptor("{model_id}_rows", "1", ("gap",), "count", _count)
    )
    context.register_metric(
        MetricDescriptor("{model_id}_signal", "1", ("signal_time_ms",), "count", _count)
    )
'''


def run_broken_model(tmp_path, name: str, mode: str, **kwargs):
    pack = build_pack(tmp_path / "pack")
    root = tmp_path / "ext"
    write_module(root, name, BROKEN_MODEL.replace("{model_id}", f"{name}_model"))
    document = request_for(
        models=[{"id": "bad", "model": f"{name}_model", "settings": {"mode": mode}}],
        extensions=[declaration(root, name)],
        **kwargs,
    )
    return pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("duplicate", "duplicate (case, anchor) row"),
        ("missing", "row(s) are missing"),
        ("unknown_case", "undeclared case IDs"),
        ("infinity", "non-finite value"),
        ("finite_with_reason", "finite value(s) with a non-available reason"),
        ("null_with_available", "null value(s); an available outcome requires a finite value"),
        ("float_timestamps", "expected exact integer UTC milliseconds"),
        ("boolean_timestamps", "expected exact integer UTC milliseconds"),
        ("blank_reason", "nonblank string"),
        ("extra_column", "unexpected columns"),
        ("wrong_kind", "does not match the registered"),
    ],
)
def test_r1_a_custom_model_output_defect_fails_the_job_without_completion(tmp_path, mode, expected):
    with pytest.raises(PatternLabStudyError) as failure:
        run_broken_model(tmp_path, f"ext_r1_{mode}", mode)
    assert expected in str(failure.value.__cause__)

    assert not study_evidence.completion_path(tmp_path / "run").is_file()
    status = study_evidence.read_status(tmp_path / "run")
    assert status["terminal_status"] == "failed"


def test_r1_empty_eligible_anchors_produce_the_declared_empty_schema(tmp_path):
    # A one-bar window has a research bar but no complete anchor.
    result = run_broken_model(
        tmp_path, "ext_r1_empty", "valid", start_group=2, end_group=3
    )
    results = pack_study.load_results(result["run_root"])
    instrument = results.completed_instruments[0]
    saved = results.table(instrument, "custom__bad")
    assert list(saved.columns) == [
        "instrument_id", "timeframe_minutes", "model_instance_id", "case_id",
        "anchor_open_ms", "score", "score__reason",
    ]
    assert len(saved) == 0
    view = results.observations(
        instrument, model_instance_id="bad", timeframe_minutes=TIMEFRAME, case_id="only"
    )
    assert len(view) == 0 and "signal_time_ms" in view.columns
    assert pack_study.summarize_results(results)["groups"][0]["events"] == 0


def mixed_support_run(tmp_path, name: str, *, metrics=None):
    pack = build_pack(tmp_path / "pack")
    root = tmp_path / "ext"
    write_module(root, name, MIXED_SUPPORT_MODEL.replace("{model_id}", f"{name}_model"))
    document = request_for(
        models=[{"id": "mixed", "model": f"{name}_model", "settings": {}}],
        metrics=metrics,
        extensions=[declaration(root, name)],
    )
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )
    normalized = pack_study.normalize_request(document, source="test", base=None)
    return pack, root, result, normalized


def test_r1_mixed_support_is_retained_and_event_counts_reconcile(tmp_path):
    _pack, _root, result, _normalized = mixed_support_run(tmp_path, "ext_r1_mixed")
    results = pack_study.load_results(result["run_root"])
    instrument = results.completed_instruments[0]
    summary = pack_study.summarize_results(results)
    groups = {group["case_id"]: group for group in summary["groups"]}
    assert set(groups) == {"wide", "narrow"}

    emissions = results.table(instrument, "emissions")
    events = int(len(emissions))
    for case_id, group in groups.items():
        assert group["events"] == events
        for outcome in group["outcomes"]:
            # Valid plus invalid always reconciles with the group's event count,
            # and every invalid observation carries a counted reason.
            assert outcome["valid_count"] + outcome["invalid_count"] == group["events"]
            assert sum(outcome["invalid_by_reason"].values()) == outcome["invalid_count"]
    # 'narrow' declares only 'gap'; the union column it does not declare stays
    # null with the model's own explicit reason and never reports availability.
    assert [item["name"] for item in groups["narrow"]["outcomes"]] == ["gap"]
    saved = results.table(instrument, "custom__mixed")
    narrow = saved.loc[saved["case_id"] == "narrow"]
    assert narrow["span"].isna().all()
    assert set(narrow["span__reason"]) == {"not_measured_for_narrow"}


def test_r1_the_axis_free_view_carries_the_exact_derived_signal_time(tmp_path):
    _pack, _root, result, _normalized = mixed_support_run(tmp_path, "ext_r1_signal")
    results = pack_study.load_results(result["run_root"])
    instrument = results.completed_instruments[0]
    view = results.observations(
        instrument, model_instance_id="mixed", timeframe_minutes=TIMEFRAME, case_id="wide"
    )
    anchors = view["anchor_open_ms"].to_numpy(dtype=np.int64)
    signals = view["signal_time_ms"].to_numpy(dtype=np.int64)
    assert np.array_equal(signals, anchors + TIMEFRAME * 60_000)
    # The derived column is not stored: valid older evidence gains it on read.
    assert "signal_time_ms" not in results.table(instrument, "custom__mixed").columns


def reseal(run_root: Path) -> None:
    """Recompute every recorded digest so only the row values are wrong."""
    for job in sorted((run_root / study_evidence.JOBS_DIR).iterdir()):
        bundle_path = job / "bundle.json"
        bundle = dict(study_evidence.read_json(bundle_path))
        for record in bundle["files"]:
            record["sha256"] = study_evidence.file_digest(job / record["name"])
        study_evidence.write_json(bundle_path, bundle)
    completion_path = study_evidence.completion_path(run_root)
    record = dict(study_evidence.read_json(completion_path))
    digests = {
        name: study_evidence.file_digest(run_root / name)
        for name in study_evidence.immutable_files(run_root)
    }
    record["evidence_sha256"] = digests
    record["evidence_set_sha256"] = study_contracts.semantic_digest(digests)
    study_evidence.write_json(completion_path, record)


def corrupt_saved_custom(run_root: Path, transform) -> None:
    results = pack_study.load_results(run_root)
    instrument = results.completed_instruments[0]
    path = study_evidence.job_path(run_root, instrument) / "custom__mixed.parquet"
    study_evidence.write_table(path, transform(study_evidence.read_table(path)),
                               name="custom__mixed")
    reseal(run_root)


@pytest.mark.parametrize(
    "name, transform, expected",
    [
        ("dropped_row", lambda frame: frame.iloc[1:].reset_index(drop=True), "are missing"),
        (
            "duplicated_row",
            lambda frame: frame.iloc[[0, *range(len(frame))]].reset_index(drop=True),
            "duplicate (case, anchor) row",
        ),
        (
            "infinite_value",
            lambda frame: frame.assign(gap=frame["gap"].mask(frame.index == 0, np.inf)),
            "non-finite value",
        ),
        (
            "incoherent_reason",
            lambda frame: frame.assign(
                gap__reason=frame["gap__reason"].mask(frame.index == 0, "invented_reason")
            ),
            "finite value(s) with a non-available reason",
        ),
    ],
)
def test_r1_malformed_saved_custom_evidence_fails_even_with_matching_hashes(
    tmp_path, name, transform, expected
):
    _pack, _root, result, normalized = mixed_support_run(
        tmp_path, f"ext_r1_saved_{name}",
        metrics=[{"id": "share", "metric": f"ext_r1_saved_{name}_model_share"}],
    )
    run_root = Path(result["run_root"])
    corrupt_saved_custom(run_root, transform)

    # The file hashes still match: only the rows are wrong.
    results = pack_study.load_results(run_root)
    assert results.complete
    instrument = results.completed_instruments[0]
    pattern = re.escape(expected)
    with pytest.raises(PatternLabDataError, match=pattern):
        results.observations(
            instrument, model_instance_id="mixed", timeframe_minutes=TIMEFRAME, case_id="wide"
        )
    with pytest.raises(PatternLabDataError, match=pattern):
        pack_study.summarize_results(results)
    with pytest.raises(PatternLabDataError, match=pattern):
        pack_study.regenerate_report(run_root)
    with pytest.raises(PatternLabDataError, match=pattern):
        study_results.compute_metric_values(results, normalized.metrics)


def test_r1_valid_saved_custom_evidence_reads_without_the_pack_or_the_module(tmp_path):
    pack, root, result, _normalized = mixed_support_run(
        tmp_path, "ext_r1_valid_old",
        metrics=[{"id": "share", "metric": "ext_r1_valid_old_model_share"}],
    )
    run_root = Path(result["run_root"])
    baseline = json.loads((run_root / "derived" / "summary.json").read_text(encoding="utf-8"))

    # Neither the market pack nor the extension source is needed to read it.
    for path in sorted(root.iterdir()):
        path.unlink()
    for path in sorted((pack / "ohlcv").iterdir()):
        path.unlink()
    (pack / "manifest.json").unlink()

    results = pack_study.load_results(run_root)
    regenerated = pack_study.summarize_results(results)
    assert regenerated["groups"] == baseline["groups"]
    assert regenerated["metrics"]["values"][0]["metric_id"] == "ext_r1_valid_old_model_share"


# --------------------------------------------------------------------------
# R2: integrity, completion and the initial report
# --------------------------------------------------------------------------

def completed_run(tmp_path, *, contracts_map=None, output="run"):
    pack = build_pack(tmp_path / "pack", contracts_map)
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=tmp_path / output
    )
    return pack, Path(result["run_root"])


@pytest.mark.parametrize("allow_partial", [False, True])
def test_r2_both_reader_modes_reject_an_edited_family_fee(tmp_path, allow_partial):
    _pack, run_root = completed_run(tmp_path)
    family = json.loads((run_root / study_evidence.FAMILY_FILE).read_text(encoding="utf-8"))
    for case in family["models"][0]["cases"][str(TIMEFRAME)]:
        case["parameters"]["commission_pct_per_side"] = 10.0
    study_evidence.write_json(run_root / study_evidence.FAMILY_FILE, family)
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.load_results(run_root, allow_partial=allow_partial)
    assert failure.value.error_code == "corrupt_evidence"


@pytest.mark.parametrize("allow_partial", [False, True])
def test_r2_both_reader_modes_reject_an_edited_raw_evidence_file(tmp_path, allow_partial):
    _pack, run_root = completed_run(tmp_path)
    instrument = pack_study.load_results(run_root).completed_instruments[0]
    path = study_evidence.job_path(run_root, instrument) / "emissions.parquet"
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.load_results(run_root, allow_partial=allow_partial)
    assert failure.value.error_code == "corrupt_evidence"


@pytest.mark.parametrize(
    "record",
    [
        {"schema_version": 1, "terminal_status": "completed"},
        {"schema_version": 99, "terminal_status": "completed", "evidence_sha256": {}},
        {"schema_version": 1, "terminal_status": "failed", "evidence_sha256": {}},
        {"schema_version": 1, "terminal_status": "completed", "evidence_sha256": "not a mapping"},
    ],
)
def test_r2_malformed_completion_metadata_is_an_actionable_validation_error(tmp_path, record):
    _pack, run_root = completed_run(tmp_path)
    study_evidence.write_json(study_evidence.completion_path(run_root), record)
    for allow_partial in (False, True):
        with pytest.raises(PatternLabDataError) as failure:
            pack_study.load_results(run_root, allow_partial=allow_partial)
        assert failure.value.error_code == "corrupt_evidence"


def test_r2_a_missing_completion_record_stays_incomplete(tmp_path):
    _pack, run_root = completed_run(tmp_path)
    study_evidence.completion_path(run_root).unlink()
    # The terminal status still says completed; only the record is authoritative.
    assert study_evidence.read_status(run_root)["terminal_status"] == "completed"
    partial = pack_study.load_results(run_root, allow_partial=True)
    assert partial.complete is False and partial.completion is None
    assert pack_study.summarize_results(partial)["complete"] is False
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.load_results(run_root)
    assert failure.value.error_code == "incomplete_run"
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.regenerate_report(run_root)
    assert failure.value.error_code == "incomplete_run"


def test_r2_the_initial_and_regenerated_summaries_agree(tmp_path):
    _pack, run_root = completed_run(tmp_path, contracts_map={
        "AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising()
    })
    initial = json.loads((run_root / "derived" / "summary.json").read_text(encoding="utf-8"))
    assert initial["complete"] is True
    pack_study.regenerate_report(run_root)
    regenerated = json.loads((run_root / "derived" / "summary.json").read_text(encoding="utf-8"))
    assert regenerated == initial


def test_r2_a_completion_write_failure_removes_only_this_runs_derived_files(tmp_path, monkeypatch):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"

    def refuse(*args, **kwargs):
        raise OSError("synthetic completion write failure")

    monkeypatch.setattr(study_evidence, "write_completion", refuse)
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    monkeypatch.undo()

    assert failure.value.context["phase"] == "publish"
    assert failure.value.context["derived_cleanup"]["removed"] == list(
        study_evidence.DERIVED_FILES
    )
    assert not (run_root / "derived" / "summary.json").exists()
    assert not (run_root / "derived" / "report.html").exists()
    # Raw evidence is retained and the run is honestly incomplete.
    assert not study_evidence.completion_path(run_root).is_file()
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    assert status["failure"]["derived_cleanup"]["removed"]
    assert (run_root / "jobs" / "TEST_AAA-USDT-SWAP" / "bundle.json").is_file()
    assert pack_study.load_results(run_root, allow_partial=True).complete is False


def test_r2_a_derived_file_that_cannot_be_removed_is_reported_separately(tmp_path):
    root = tmp_path / "run"
    (root / "derived").mkdir(parents=True)
    (root / study_evidence.SUMMARY_FILE).write_text("{}", encoding="utf-8")
    # A directory in place of the report file cannot be unlinked on either host.
    (root / study_evidence.REPORT_FILE).mkdir()
    (root / study_evidence.REPORT_FILE / "keep.txt").write_text("x", encoding="utf-8")
    cleanup = study_evidence.remove_new_derived(root)
    assert cleanup["removed"] == [study_evidence.SUMMARY_FILE]
    assert len(cleanup["retained"]) == 1
    assert cleanup["retained"][0].startswith(study_evidence.REPORT_FILE)
    assert (root / study_evidence.REPORT_FILE).is_dir()


def test_r2_a_published_completion_is_never_rolled_back(tmp_path, monkeypatch):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    real = study_evidence.write_completion

    def write_then_fail(run_root_arg, **kwargs):
        real(run_root_arg, **kwargs)
        raise OSError("synthetic failure after the atomic replacement")

    monkeypatch.setattr(study_evidence, "write_completion", write_then_fail)
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=run_root
    )
    monkeypatch.undo()

    # The record was already published, so the run is sealed and complete.
    assert result["status"] == "completed"
    assert study_evidence.read_status(run_root)["terminal_status"] == "completed"
    assert (run_root / "derived" / "report.html").is_file()
    assert pack_study.load_results(run_root).complete is True

    # A later regeneration failure never reclassifies that sealed success.
    before = (run_root / "derived" / "report.html").read_bytes()
    monkeypatch.setattr(study_report, "render_report", lambda summary: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        pack_study.regenerate_report(run_root)
    monkeypatch.undo()
    assert study_evidence.read_status(run_root)["terminal_status"] == "completed"
    assert (run_root / "derived" / "report.html").read_bytes() == before
    assert pack_study.load_results(run_root).complete is True


# --------------------------------------------------------------------------
# R3: one semantic validation path for public inputs
# --------------------------------------------------------------------------

def test_r3_every_valid_request_form_executes(tmp_path):
    pack = build_pack(tmp_path / "pack")
    document = request_for()
    spec_path = tmp_path / "study.json"
    spec_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    normalized = pack_study.normalize_request(document, source="test", base=None)

    identities = []
    for index, form in enumerate((spec_path, document, normalized)):
        result = pack_study.run_study(
            request=form, data_root=pack, output_root=tmp_path / f"run-{index}"
        )
        identities.append(result["identities"]["specification_sha256"])
    assert len(set(identities)) == 1


def test_r3_an_out_of_protocol_replacement_cannot_execute(tmp_path):
    pack = build_pack(tmp_path / "pack")
    normalized = pack_study.normalize_request(request_for(), source="test", base=None)
    changed = replace(normalized, study_end_ms=study_group_ms(GROUPS + 40, TIMEFRAME))
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="development interval"):
        pack_study.run_study(request=changed, data_root=pack, output_root=run_root)
    assert not run_root.exists()


def test_r3_a_forged_private_protocol_bounds_cache_is_rebuilt(tmp_path):
    pack = build_pack(tmp_path / "pack")
    normalized = pack_study.normalize_request(request_for(), source="test", base=None)
    protocol = dict(normalized.protocol)
    protocol["_bounds"] = dict(protocol["_bounds"])
    protocol["_bounds"]["development_end_ms"] += 30 * 86_400_000
    forged = replace(
        normalized, protocol=protocol, study_end_ms=study_group_ms(GROUPS + 40, TIMEFRAME)
    )
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="development interval"):
        pack_study.run_study(request=forged, data_root=pack, output_root=run_root)
    assert not run_root.exists()

    # A bounds cache that merely disagrees with its own document is rejected too.
    only_forged = replace(normalized, protocol=protocol)
    with pytest.raises(PatternLabDataError, match="private bounds cache"):
        pack_study.run_study(request=only_forged, data_root=pack, output_root=run_root)
    assert not run_root.exists()


def test_r3_a_mutated_nested_setting_leaves_stale_resolved_cases(tmp_path):
    pack = build_pack(tmp_path / "pack")
    normalized = pack_study.normalize_request(request_for(), source="test", base=None)
    # Frozen dataclasses are shallow: the nested settings mapping is mutable.
    normalized.models[0].settings["commission_pct_per_side"] = 10.0
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="contradict a fresh resolution"):
        pack_study.run_study(request=normalized, data_root=pack, output_root=run_root)
    assert not run_root.exists()


def test_r3_a_stale_derived_condition_identity_is_recomputed_and_rejected(tmp_path):
    pack = build_pack(tmp_path / "pack")
    normalized = pack_study.normalize_request(request_for(), source="test", base=None)
    stale = replace(
        normalized,
        variants=(replace(normalized.variants[0], condition_id="0" * 32),),
    )
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="contradict a fresh resolution"):
        pack_study.run_study(request=stale, data_root=pack, output_root=run_root)
    assert not run_root.exists()


def test_r3_insufficient_warmup_on_a_replaced_request_is_rejected(tmp_path):
    pack = build_pack(tmp_path / "pack")
    normalized = pack_study.normalize_request(request_for(), source="test", base=None)
    starved = replace(normalized, warmup_start_ms=normalized.study_start_ms)
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError, match="declared warmup supplies"):
        pack_study.run_study(request=starved, data_root=pack, output_root=run_root)
    assert not run_root.exists()


def test_r3_validation_never_mutates_the_caller_s_request(tmp_path):
    document = request_for()
    before = json.dumps(document, sort_keys=True)
    normalized = pack_study.normalize_request(document, source="test", base=None)
    fresh = study_validation.validated_request(normalized)
    assert json.dumps(document, sort_keys=True) == before
    assert fresh is not normalized
    assert fresh.semantic_document() == normalized.semantic_document()


# --------------------------------------------------------------------------
# R4 and R5: used source closure and dependency warmup
# --------------------------------------------------------------------------

NESTED_FEATURES = '''
def _period(parameters):
    contracts.closed_keys(parameters, ("period",), "parameters")
    return {"period": int(parameters["period"])}


def _mean(series, parameters, features):
    period = int(parameters["period"])
    rows = series.row_count
    values = np.full(rows, np.nan, dtype=np.float64)
    valid = np.zeros(rows, dtype=bool)
    if rows >= period:
        windows = np.lib.stride_tricks.sliding_window_view(series.close, period)
        prefix = np.concatenate(([0], np.cumsum(series.contiguous_with_previous().astype(np.int64))))
        starts = np.arange(0, rows - period + 1)
        whole = prefix[starts + period] - prefix[starts + 1] == (period - 1)
        values[starts + period - 1] = windows.mean(axis=1)
        valid[starts + period - 1] = whole
    return FeatureValue(values=values, valid=valid)


def _slope(series, parameters, features):
    period = int(parameters["period"])
    inner = features[FeatureRequest("{prefix}_mean", {"period": period}).key]
    values = np.full(series.row_count, np.nan, dtype=np.float64)
    valid = np.zeros(series.row_count, dtype=bool)
    if series.row_count > 1:
        contiguous = series.contiguous_with_previous()[1:]
        values[1:] = inner.values[1:] - inner.values[:-1]
        valid[1:] = contiguous & inner.valid[1:] & inner.valid[:-1]
    values[~valid] = np.nan
    return FeatureValue(values=values, valid=valid)


def _rising(series, parameters, features):
    period = int(parameters["period"])
    slope = features[FeatureRequest("{prefix}_slope", {"period": period}).key]
    value = np.zeros(series.row_count, dtype=bool)
    np.greater(slope.values, 0.0, out=value, where=slope.valid)
    return ConditionValue(value=value & slope.valid, valid=slope.valid)


def register(context):
    context.register_feature(
        FeatureDescriptor(
            feature_id="{prefix}_mean", version="1", evaluate=_mean,
            validate_parameters=_period,
            prior_bars=lambda parameters: int(parameters["period"]) - 1,
        )
    )
    # The outer feature declares the TOTAL history its own calculation needs.
    context.register_feature(
        FeatureDescriptor(
            feature_id="{prefix}_slope", version="1", evaluate=_slope,
            validate_parameters=_period,
            dependencies=lambda parameters: (
                FeatureRequest("{prefix}_mean", {"period": int(parameters["period"])}),
            ),
            prior_bars=lambda parameters: int(parameters["period"]),
        )
    )
    # The hypothesis understates its own lookback: the closure supplies it.
    context.register_hypothesis(
        HypothesisDescriptor(
            hypothesis_id="{prefix}_rising", version="1", evaluate=_rising,
            validate_parameters=_period,
            dependencies=lambda parameters: (
                FeatureRequest("{prefix}_slope", {"period": int(parameters["period"])}),
            ),
            prior_bars=lambda parameters: 0,
        )
    )
'''

CYCLIC_FEATURES = '''
def _identity(series, parameters, features):
    return FeatureValue(values=series.close.copy(), valid=np.ones(series.row_count, dtype=bool))


def _condition(series, parameters, features):
    ones = np.ones(series.row_count, dtype=bool)
    return ConditionValue(value=ones, valid=ones)


def register(context):
    context.register_feature(
        FeatureDescriptor(
            feature_id="{prefix}_a", version="1", evaluate=_identity,
            dependencies=lambda parameters: (FeatureRequest("{prefix}_b"),),
        )
    )
    context.register_feature(
        FeatureDescriptor(
            feature_id="{prefix}_b", version="1", evaluate=_identity,
            dependencies=lambda parameters: (FeatureRequest("{prefix}_a"),),
        )
    )
    context.register_hypothesis(
        HypothesisDescriptor(
            hypothesis_id="{prefix}_cycle", version="1", evaluate=_condition,
            dependencies=lambda parameters: (FeatureRequest("{prefix}_a"),),
        )
    )
'''

PANEL_FEATURE = '''
def _identity(series, parameters, features):
    return FeatureValue(values=series.close.copy(), valid=np.ones(series.row_count, dtype=bool))


def _condition(series, parameters, features):
    ones = np.ones(series.row_count, dtype=bool)
    return ConditionValue(value=ones, valid=ones)


def register(context):
    context.register_feature(
        FeatureDescriptor(feature_id="{prefix}_panel", version="1", evaluate=_identity,
                          scope="panel")
    )
    context.register_hypothesis(
        HypothesisDescriptor(
            hypothesis_id="{prefix}_needs_panel", version="1", evaluate=_condition,
            dependencies=lambda parameters: (FeatureRequest("{prefix}_panel"),),
        )
    )
'''


def nested_variant(prefix: str, period: int = 4, warmup_group: int = 0):
    return {
        "id": "nested",
        "hypothesis": f"{prefix}_rising",
        "parameters": {"period": period},
        "occurrence": "every_qualifying_bar",
    }


def test_r5_a_transitive_dependency_supplies_the_resolved_warmup(tmp_path):
    prefix = "ext_r5_nested"
    root = tmp_path / "ext"
    write_module(root, prefix, NESTED_FEATURES.replace("{prefix}", prefix))
    document = request_for(
        hypotheses=[nested_variant(prefix, period=4)],
        extensions=[declaration(root, prefix)],
        start_group=6,
    )
    normalized = pack_study.normalize_request(document, source="test", base=None)
    facts = study_spec.warmup_requirements(normalized)[str(TIMEFRAME)]
    # max(hypothesis 0, slope 4, mean 3) -- totals are never summed.
    assert facts["required_prior_bars"] == 4
    assert facts["by_variant"] == {"nested": 4}

    starved = request_for(
        hypotheses=[nested_variant(prefix, period=4)],
        extensions=[declaration(root, prefix)],
        start_group=3,
    )
    with pytest.raises(PatternLabDataError, match="but the requested hypotheses need 4"):
        pack_study.normalize_request(starved, source="test", base=None)


def test_r5_a_parameterized_dependency_scales_the_resolved_warmup(tmp_path):
    prefix = "ext_r5_param"
    root = tmp_path / "ext"
    write_module(root, prefix, NESTED_FEATURES.replace("{prefix}", prefix))
    for period, expected in ((2, 2), (7, 7)):
        document = request_for(
            hypotheses=[nested_variant(prefix, period=period)],
            extensions=[declaration(root, prefix)],
            start_group=8,
        )
        normalized = pack_study.normalize_request(document, source="test", base=None)
        facts = study_spec.warmup_requirements(normalized)[str(TIMEFRAME)]
        assert facts["required_prior_bars"] == expected


def test_r5_a_dependency_cycle_and_a_missing_registration_fail_before_output(tmp_path):
    prefix = "ext_r5_cycle"
    root = tmp_path / "ext"
    write_module(root, prefix, CYCLIC_FEATURES.replace("{prefix}", prefix))
    document = request_for(
        hypotheses=[{
            "id": "cyclic", "hypothesis": f"{prefix}_cycle", "parameters": {},
            "occurrence": "every_qualifying_bar",
        }],
        extensions=[declaration(root, prefix)],
    )
    with pytest.raises(PatternLabDataError, match="depends on itself"):
        pack_study.normalize_request(document, source="test", base=None)

    unknown = request_for(hypotheses=[{
        "id": "absent", "hypothesis": "never_registered_hypothesis", "parameters": {},
        "occurrence": "every_qualifying_bar",
    }])
    with pytest.raises(PatternLabDataError, match="unknown hypothesis"):
        pack_study.normalize_request(unknown, source="test", base=None)


def test_r5_post_gap_bars_re_warm_instead_of_reusing_a_stale_lookback(tmp_path):
    prefix = "ext_r5_gap"
    root = tmp_path / "ext"
    write_module(root, prefix, NESTED_FEATURES.replace("{prefix}", prefix))
    # Group 8 is missing, so the contiguous window must rebuild after it.
    pack = build_pack(tmp_path / "pack", drop_groups=(8,))
    document = request_for(
        hypotheses=[nested_variant(prefix, period=4)],
        extensions=[declaration(root, prefix)],
        start_group=6,
        end_group=14,
    )
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )
    results = pack_study.load_results(result["run_root"])
    instrument = results.completed_instruments[0]
    conditions = results.table(instrument, "conditions")
    anchors = conditions["anchor_open_ms"].to_numpy(dtype=np.int64)
    valid = conditions["valid"].to_numpy(dtype=bool)
    invalid_groups = {
        int((stamp - study_group_ms(0, TIMEFRAME)) // (TIMEFRAME * 60_000))
        for stamp, ok in zip(anchors, valid) if not ok
    }
    # Calendar warmup is sufficient, yet the bars right after the gap are
    # unknown rather than false until a whole contiguous window exists again.
    assert {9, 10, 11, 12} <= invalid_groups
    assert 7 not in invalid_groups


def test_r5_an_unsupported_dependency_scope_fails_before_output(tmp_path):
    prefix = "ext_r5_scope"
    root = tmp_path / "ext"
    write_module(root, prefix, PANEL_FEATURE.replace("{prefix}", prefix))
    document = request_for(
        hypotheses=[{
            "id": "panel", "hypothesis": f"{prefix}_needs_panel", "parameters": {},
            "occurrence": "every_qualifying_bar",
        }],
        extensions=[declaration(root, prefix)],
    )
    with pytest.raises(PatternLabDataError, match="scope 'panel' is not supported"):
        pack_study.normalize_request(document, source="test", base=None)
    assert not (tmp_path / "run").exists()


def test_r4_an_undeclared_registration_blocks_the_study_before_output(tmp_path):
    pack = build_pack(tmp_path / "pack")
    study_contracts.register_hypothesis(
        study_contracts.HypothesisDescriptor(
            hypothesis_id="r4_ghost_hypothesis",
            version="1",
            evaluate=lambda series, parameters, features: study_contracts.ConditionValue(
                np.ones(series.row_count, dtype=bool), np.ones(series.row_count, dtype=bool)
            ),
        ),
        source_digest="deadbeef",
        source_path=str(tmp_path / "nonexistent.py"),
    )
    document = request_for(hypotheses=[{
        "id": "ghost", "hypothesis": "r4_ghost_hypothesis", "parameters": {},
        "occurrence": "every_qualifying_bar",
    }])
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.run_study(request=document, data_root=pack, output_root=run_root)
    assert failure.value.error_code == "unverified_source"
    assert not run_root.exists()

    # The same unused entry must not block an unrelated built-in study.
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=tmp_path / "builtin"
    )
    source = json.loads(
        (Path(result["run_root"]) / study_evidence.SOURCE_FILE).read_text(encoding="utf-8")
    )
    assert source["extensions"] == []


def test_r4_an_undeclared_transitive_dependency_blocks_the_study(tmp_path):
    prefix = "ext_r4_dep"
    root = tmp_path / "ext"
    write_module(root, prefix, NESTED_FEATURES.replace("{prefix}", prefix))
    pack = build_pack(tmp_path / "pack")
    document = request_for(
        hypotheses=[nested_variant(prefix, period=4)],
        extensions=[declaration(root, prefix)],
        start_group=6,
    )
    normalized = pack_study.normalize_request(document, source="test", base=None)
    used = {registration.identifier
            for registration in study_validation.used_registrations(normalized)}
    # The transitive inner feature is part of the used set, not just the direct one.
    assert {f"{prefix}_rising", f"{prefix}_slope", f"{prefix}_mean"} <= used

    # Declaring nothing leaves every used descriptor unattributable.
    with pytest.raises(PatternLabDataError) as failure:
        study_validation.require_declared_sources(
            study_validation.used_registrations(normalized), {}, where="probe"
        )
    assert failure.value.error_code == "unverified_source"

    # The declared generation makes the whole closure attributable.
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )
    assert result["counts"]["completed"] == 1
    # And a second run reuses the same verified generation.
    again = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run-2"
    )
    assert again["identities"]["implementation_sha256"] == result["identities"][
        "implementation_sha256"
    ]


# --------------------------------------------------------------------------
# R6: failure attribution and durable partial progress
# --------------------------------------------------------------------------

def test_r6_a_base_slice_with_no_research_rows_names_the_attempted_instrument(tmp_path):
    # Metadata admission passes: the bars exist before and after the window.
    pack = build_pack(tmp_path / "pack", drop_groups=range(2, 12))
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    assert failure.value.context["instrument_id"] == "TEST_AAA-USDT-SWAP"
    assert failure.value.context["phase"] == "read"
    status = study_evidence.read_status(run_root)
    assert status["counts"] == {
        "admitted": 0, "completed": 0, "failed": 1, "not_started": 0, "planned": 1
    }
    record = status["instruments"][0]
    assert record["state"] == "failed"
    assert "no complete 5m research bar" in record["error"]
    admitted = json.loads(
        (run_root / "admitted" / "TEST_AAA-USDT-SWAP.json").read_text(encoding="utf-8")
    )
    # Known metadata context is kept; no fingerprint is invented for unread rows.
    assert admitted["declared"]["file_sha256"]
    assert admitted["timeframes"] == []
    assert "read_error" in admitted and "consumed" not in admitted


def test_r6_a_published_bundle_is_visible_before_the_final_status(tmp_path, monkeypatch):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising()})
    run_root = tmp_path / "run"
    observed: dict = {}
    real = study_evidence.record_job_state

    def capture(run_root_arg, instrument_id, state, **kwargs):
        real(run_root_arg, instrument_id, state, **kwargs)
        if state == study_evidence.STATE_COMPLETED and not observed:
            partial = pack_study.load_results(run_root_arg, allow_partial=True)
            observed["counts"] = dict(partial.counts)
            observed["completed"] = list(partial.completed_instruments)
            observed["complete"] = partial.complete
            raise RuntimeError("synthetic stop after the durable publication")

    monkeypatch.setattr(study_evidence, "record_job_state", capture)
    with pytest.raises(PatternLabStudyError):
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    monkeypatch.undo()

    assert observed["completed"] == ["TEST_AAA-USDT-SWAP"]
    assert observed["counts"]["completed"] == 1
    assert observed["counts"]["not_started"] == 1
    assert observed["complete"] is False


def test_r6_unfinished_staging_directories_are_not_completed_evidence(tmp_path):
    _pack, run_root = completed_run(tmp_path)
    staging = run_root / study_evidence.JOBS_DIR / ".TEST_ZZZ-USDT-SWAP.tmp-abcdef"
    staging.mkdir()
    (staging / "emissions.parquet").write_bytes(b"partial")
    partial = pack_study.load_results(run_root, allow_partial=True)
    assert partial.completed_instruments == ["TEST_AAA-USDT-SWAP"]
    assert partial.counts["completed"] == 1


def test_r6_a_corrupt_claimed_completed_bundle_fails_partial_inspection(tmp_path):
    _pack, run_root = completed_run(tmp_path)
    path = study_evidence.job_path(run_root, "TEST_AAA-USDT-SWAP") / "episodes.parquet"
    path.write_bytes(b"not parquet")
    study_evidence.completion_path(run_root).unlink()
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.load_results(run_root, allow_partial=True)
    assert failure.value.error_code == "corrupt_evidence"

    # A missing bundle record fails rather than making the job disappear.
    path.unlink()
    (study_evidence.job_path(run_root, "TEST_AAA-USDT-SWAP") / "bundle.json").unlink()
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.load_results(run_root, allow_partial=True)
    assert failure.value.error_code == "corrupt_evidence"


# Every exceptional exit from read or preparation, not only a PatternLabDataError,
# names the attempted instrument (T04-1 R1).

def three_instrument_pack(tmp_path):
    return build_pack(
        tmp_path / "pack",
        {"AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising(), "CCC-USDT-SWAP": rising()},
    )


def fail_preparing(monkeypatch, symbol: str, error: BaseException):
    """Raise ``error`` while preparing one instrument's timeframes."""
    real = study_runner._prepare_timeframes

    def prepare(entry, *args, **kwargs):
        if entry["symbol"] == symbol:
            raise error
        return real(entry, *args, **kwargs)

    monkeypatch.setattr(study_runner, "_prepare_timeframes", prepare)


def recorded_states(run_root) -> dict:
    status = study_evidence.read_status(run_root)
    return {item["instrument_id"]: item for item in status["instruments"]}


def test_r6_an_ordinary_read_failure_names_the_attempted_instrument(tmp_path, monkeypatch):
    pack = three_instrument_pack(tmp_path)
    run_root = tmp_path / "run"
    real = pack_data._ReadSession.load_slice

    def load_slice(self, instrument_id, *args, **kwargs):
        if instrument_id == "TEST_BBB-USDT-SWAP":
            raise RuntimeError("synthetic base-slice failure")
        return real(self, instrument_id, *args, **kwargs)

    monkeypatch.setattr(pack_data._ReadSession, "load_slice", load_slice)
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    monkeypatch.undo()

    assert failure.value.context["instrument_id"] == "TEST_BBB-USDT-SWAP"
    assert failure.value.context["phase"] == "read"
    status = study_evidence.read_status(run_root)
    assert status["counts"] == {
        "admitted": 0, "completed": 1, "failed": 1, "not_started": 1, "planned": 3
    }
    states = recorded_states(run_root)
    assert states["TEST_AAA-USDT-SWAP"]["state"] == "completed"
    assert states["TEST_CCC-USDT-SWAP"]["state"] == "not_started"
    assert "RuntimeError: synthetic base-slice failure" in states["TEST_BBB-USDT-SWAP"]["error"]
    admitted = json.loads(
        (run_root / "admitted" / "TEST_BBB-USDT-SWAP.json").read_text(encoding="utf-8")
    )
    # Nothing was read, so no fingerprint and no consumed context is invented.
    assert admitted["phase"] == "read" and admitted["state"] == "failed"
    assert admitted["timeframes"] == [] and "consumed" not in admitted
    assert not study_evidence.completion_path(run_root).is_file()


def test_r6_an_ordinary_preparation_failure_keeps_the_consumed_context(tmp_path, monkeypatch):
    pack = three_instrument_pack(tmp_path)
    run_root = tmp_path / "run"
    fail_preparing(monkeypatch, "BBB", RuntimeError("synthetic preparation failure"))
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    monkeypatch.undo()

    assert failure.value.context["instrument_id"] == "TEST_BBB-USDT-SWAP"
    assert failure.value.context["phase"] == "prepare"
    counts = study_evidence.read_status(run_root)["counts"]
    assert counts == {
        "admitted": 0, "completed": 1, "failed": 1, "not_started": 1, "planned": 3
    }
    admitted = json.loads(
        (run_root / "admitted" / "TEST_BBB-USDT-SWAP.json").read_text(encoding="utf-8")
    )
    # The read succeeded, so its context survives; the timeframes never did.
    assert admitted["consumed"]["base_row_count"] > 0
    assert admitted["timeframes"] == []
    assert "RuntimeError: synthetic preparation failure" in admitted["error"]

    # Terminal counts, per-job records and partial inspection all agree, and
    # the earlier completed bundle is preserved.
    partial = pack_study.load_results(run_root, allow_partial=True)
    assert dict(partial.counts) == counts
    assert partial.completed_instruments == ["TEST_AAA-USDT-SWAP"]
    assert partial.complete is False


def test_r6_an_admission_interrupt_is_recorded_and_stays_an_interrupt(tmp_path, monkeypatch):
    pack = three_instrument_pack(tmp_path)
    run_root = tmp_path / "run"
    # An injected interrupt, not physical console Ctrl+C.
    fail_preparing(monkeypatch, "BBB", KeyboardInterrupt("injected admission interrupt"))
    with pytest.raises(KeyboardInterrupt):
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    monkeypatch.undo()

    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "interrupted"
    assert status["failure"]["reason"] == "keyboard_interrupt"
    assert status["failure"]["instrument_id"] == "TEST_BBB-USDT-SWAP"
    assert status["counts"] == {
        "admitted": 0, "completed": 1, "failed": 1, "not_started": 1, "planned": 3
    }
    states = recorded_states(run_root)
    assert "KeyboardInterrupt: injected admission interrupt" in states["TEST_BBB-USDT-SWAP"]["error"]
    assert not study_evidence.completion_path(run_root).is_file()
    assert pack_study.load_results(run_root, allow_partial=True).completed_instruments == [
        "TEST_AAA-USDT-SWAP"
    ]


def test_r6_a_secondary_admission_write_failure_never_replaces_the_cause(tmp_path, monkeypatch):
    pack = three_instrument_pack(tmp_path)
    run_root = tmp_path / "run"
    real_record = study_evidence.record_admission

    def refuse(run_root_arg, instrument_id, identity):
        if instrument_id == "TEST_BBB-USDT-SWAP":
            raise OSError("synthetic admission write failure")
        return real_record(run_root_arg, instrument_id, identity)

    monkeypatch.setattr(study_evidence, "record_admission", refuse)
    fail_preparing(monkeypatch, "BBB", RuntimeError("synthetic preparation failure"))
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    monkeypatch.undo()

    # The original cause and control flow survive the secondary write failure.
    assert "synthetic preparation failure" in str(failure.value)
    assert isinstance(failure.value.__cause__, RuntimeError)
    assert failure.value.context["instrument_id"] == "TEST_BBB-USDT-SWAP"
    assert failure.value.context["phase"] == "prepare"
    # The in-memory state is still honest even though nothing could be written.
    assert not (run_root / "admitted" / "TEST_BBB-USDT-SWAP.json").exists()
    counts = study_evidence.read_status(run_root)["counts"]
    assert counts == {
        "admitted": 0, "completed": 1, "failed": 1, "not_started": 1, "planned": 3
    }
    assert dict(pack_study.load_results(run_root, allow_partial=True).counts) == counts



@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_admission_record_failure_keeps_the_attempted_job_failed(tmp_path, monkeypatch, error_type):
    pack = three_instrument_pack(tmp_path)
    run_root = tmp_path / "run"
    original = study_evidence.record_admission
    injected = error_type("synthetic admission publication failure")
    attempted = False

    def record(root, identifier, identity):
        nonlocal attempted
        if identifier == "TEST_BBB-USDT-SWAP" and not attempted:
            attempted = True
            raise injected
        return original(root, identifier, identity)

    monkeypatch.setattr(study_evidence, "record_admission", record)
    expected = KeyboardInterrupt if error_type is KeyboardInterrupt else PatternLabStudyError
    with pytest.raises(expected) as failure:
        pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    if error_type is KeyboardInterrupt:
        assert failure.value is injected
    else:
        assert failure.value.__cause__ is injected
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == ("interrupted" if error_type is KeyboardInterrupt else "failed")
    assert status["failure"]["instrument_id"] == "TEST_BBB-USDT-SWAP"
    assert status["failure"]["phase"] == "admit"
    assert status["counts"] == {
        "admitted": 0, "completed": 1, "failed": 1, "not_started": 1, "planned": 3
    }
    admitted = study_evidence.read_json(run_root / "admitted" / "TEST_BBB-USDT-SWAP.json")
    assert admitted["phase"] == "admit" and admitted["state"] == "failed"
    assert admitted["consumed"]["base_row_count"] > 0 and admitted["timeframes"]
    assert "synthetic admission publication failure" in admitted["error"]
    partial = pack_study.load_results(run_root, allow_partial=True)
    assert dict(partial.counts) == status["counts"]
    assert partial.completed_instruments == ["TEST_AAA-USDT-SWAP"]
    assert not study_evidence.completion_path(run_root).exists()


# The whole published completion record is validated, not only the fields the
# file-digest pass touches (T04-1 R2).

def completion_of(run_root) -> dict:
    return dict(study_evidence.read_json(study_evidence.completion_path(run_root)))


def _replace_counts(record, **changes):
    record["counts"] = {**record["counts"], **changes}


COMPLETION_MUTATIONS = {
    "missing_aggregate": lambda record: record.pop("evidence_set_sha256"),
    "zeroed_aggregate": lambda record: record.update(evidence_set_sha256="0" * 64),
    # A valid-length, valid-alphabet, wrong value.
    "wrong_aggregate": lambda record: record.update(evidence_set_sha256="a1" * 32),
    "short_aggregate": lambda record: record.update(evidence_set_sha256="abc"),
    "boolean_version": lambda record: record.update(schema_version=True),
    "boolean_count": lambda record: _replace_counts(record, completed=True),
    "float_count": lambda record: _replace_counts(record, planned=3.0),
    "negative_count": lambda record: _replace_counts(record, failed=-1),
    # The tech lead's reproduction: a 999 planned count in a three-job run.
    "planned_999": lambda record: _replace_counts(record, planned=999),
    "unstarted_job_in_a_completed_run": lambda record: _replace_counts(record, not_started=1),
    # Internally coherent, but not this run's jobs.
    "coherent_but_wrong_counts": lambda record: _replace_counts(record, planned=2, completed=2),
    "missing_counts": lambda record: record.pop("counts"),
    "missing_count_key": lambda record: record["counts"].pop("admitted"),
    "missing_identities": lambda record: record.pop("identities"),
    "missing_identity_key": lambda record: record["identities"].pop("specification_sha256"),
    "forged_identity": lambda record: record["identities"].update(data_input_sha256="b1" * 32),
    "missing_derived_files": lambda record: record.pop("derived_files"),
    "duplicated_derived_file": lambda record: record.update(
        derived_files=[study_evidence.SUMMARY_FILE, study_evidence.SUMMARY_FILE]
    ),
    "arbitrary_derived_file": lambda record: record.update(derived_files=["derived/anything.json"]),
    "missing_run_root": lambda record: record.pop("run_root"),
    "blank_run_root": lambda record: record.update(run_root="   "),
    "non_digest_file_hash": lambda record: record["evidence_sha256"].update(
        {study_evidence.STATUS_FILE: "not a digest"}
    ),
}


@pytest.mark.parametrize("mutation", sorted(COMPLETION_MUTATIONS))
def test_r2_contradictory_completion_metadata_fails_every_reader(tmp_path, mutation):
    _pack, run_root = completed_run(tmp_path, contracts_map={
        "AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising(), "CCC-USDT-SWAP": rising()
    })
    summary_before = (run_root / study_evidence.SUMMARY_FILE).read_bytes()
    report_before = (run_root / study_evidence.REPORT_FILE).read_bytes()
    bundle_before = (
        study_evidence.job_path(run_root, "TEST_AAA-USDT-SWAP") / "bundle.json"
    ).read_bytes()
    record = completion_of(run_root)
    COMPLETION_MUTATIONS[mutation](record)
    study_evidence.write_json(study_evidence.completion_path(run_root), record)

    for allow_partial in (False, True):
        with pytest.raises(PatternLabDataError) as failure:
            pack_study.load_results(run_root, allow_partial=allow_partial)
        assert failure.value.error_code == "corrupt_evidence"
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.regenerate_report(run_root)
    assert failure.value.error_code == "corrupt_evidence"

    # Rejection happens before anything derived is replaced, and raw evidence,
    # status and the record itself are left exactly as they were.
    assert (run_root / study_evidence.SUMMARY_FILE).read_bytes() == summary_before
    assert (run_root / study_evidence.REPORT_FILE).read_bytes() == report_before
    assert (
        study_evidence.job_path(run_root, "TEST_AAA-USDT-SWAP") / "bundle.json"
    ).read_bytes() == bundle_before
    assert study_evidence.read_status(run_root)["terminal_status"] == "completed"
    assert completion_of(run_root) == record


@pytest.mark.parametrize("changed", ["family", "status"])
def test_completed_job_counts_must_agree_with_the_saved_plan_and_status(tmp_path, changed):
    _pack, run_root = completed_run(tmp_path)
    path = run_root / (study_evidence.FAMILY_FILE if changed == "family" else study_evidence.STATUS_FILE)
    document = study_evidence.read_json(path)
    if changed == "family":
        document["planned_job_count"] = 999
    else:
        document["counts"]["planned"] = 999
    study_evidence.write_json(path, document)
    # Keep hashes valid to exercise semantic agreement, not file corruption.
    completion = completion_of(run_root)
    study_evidence.write_completion(
        run_root, summary={key: completion[key] for key in ("run_root", "counts", "identities")}
    )
    before = {name: (run_root / name).read_bytes() for name in study_evidence.DERIVED_FILES}
    for partial in (False, True):
        with pytest.raises(PatternLabDataError) as failure:
            pack_study.load_results(run_root, allow_partial=partial)
        assert failure.value.error_code == "corrupt_evidence"
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.regenerate_report(run_root)
    assert failure.value.error_code == "corrupt_evidence"
    assert {name: (run_root / name).read_bytes() for name in before} == before


def test_r2_a_relocated_run_keeps_loading_and_regenerating(tmp_path):
    _pack, run_root = completed_run(tmp_path)
    moved = tmp_path / "moved" / "another-name"
    moved.parent.mkdir(parents=True)
    shutil.copytree(run_root, moved)
    results = pack_study.load_results(moved)
    assert results.complete is True
    # run_root is recorded provenance, not a claim about the current host.
    assert results.completion["run_root"] == str(run_root)
    assert pack_study.regenerate_report(moved)["status"] == "regenerated"


def test_r2_missing_or_edited_derived_outputs_are_regenerable_not_evidence(tmp_path):
    _pack, run_root = completed_run(tmp_path)
    (run_root / study_evidence.SUMMARY_FILE).unlink()
    (run_root / study_evidence.REPORT_FILE).write_text("edited by hand", encoding="utf-8")
    assert pack_study.load_results(run_root).complete is True
    assert pack_study.regenerate_report(run_root)["status"] == "regenerated"
    assert json.loads(
        (run_root / study_evidence.SUMMARY_FILE).read_text(encoding="utf-8")
    )["complete"] is True
    assert "edited by hand" not in (run_root / study_evidence.REPORT_FILE).read_text(
        encoding="utf-8"
    )


def test_r2_a_valid_completed_run_still_reads_after_the_stricter_checks(tmp_path):
    _pack, run_root = completed_run(tmp_path, contracts_map={
        "AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising()
    })
    record = completion_of(run_root)
    assert record["counts"] == {
        "admitted": 0, "completed": 2, "failed": 0, "not_started": 0, "planned": 2
    }
    strict = pack_study.load_results(run_root)
    partial = pack_study.load_results(run_root, allow_partial=True)
    assert strict.complete is True and partial.complete is True
    assert record["identities"] == dict(strict.provenance["identities"])



# Normalized-request protocol provenance (T04-1 R4).

def test_r3_a_normalized_request_keeps_its_protocol_file_provenance(tmp_path):
    pack = build_pack(tmp_path / "pack")
    document = request_for()
    protocol_path = tmp_path / "protocol.json"
    study_evidence.write_json(protocol_path, document["protocol"])
    document["protocol"] = str(protocol_path)
    normalized = pack_study.normalize_request(document, source="test", base=None)
    assert normalized.protocol_source == str(protocol_path)

    # Revalidation rebuilds the protocol from the normalized document itself:
    # removing the file proves it is never reopened, while the provenance the
    # caller supplied is still true and is what the saved request records.
    protocol_path.unlink()
    checked = study_validation.validated_request(normalized)
    assert checked.protocol_source == str(protocol_path)
    assert checked.protocol["_bounds"] == normalized.protocol["_bounds"]
    result = pack_study.run_study(
        request=normalized, data_root=pack, output_root=tmp_path / "run"
    )
    saved = json.loads(
        (Path(result["run_root"]) / study_evidence.REQUEST_FILE).read_text(encoding="utf-8")
    )
    assert saved["protocol_source"] == str(protocol_path)

    # Inline provenance stays inline, and equivalent settings keep one identity.
    inline = pack_study.normalize_request(request_for(), source="test", base=None)
    assert study_validation.validated_request(inline).protocol_source == "inline"
    assert inline.semantic_document() == normalized.semantic_document()
    inline_result = pack_study.run_study(
        request=inline, data_root=pack, output_root=tmp_path / "run-inline"
    )
    assert (
        inline_result["identities"]["specification_sha256"]
        == result["identities"]["specification_sha256"]
    )



# --------------------------------------------------------------------------
# R7: bounded evidence reuse and reporting
# --------------------------------------------------------------------------

class _CountingReads:
    """Count every decoded evidence table, keyed by its path."""

    def __init__(self, monkeypatch):
        self.calls: list[str] = []
        real = study_evidence.read_table

        def counted(path):
            self.calls.append(Path(path).name)
            return real(path)

        monkeypatch.setattr(study_evidence, "read_table", counted)

    def reset(self) -> None:
        self.calls.clear()


def test_r7_the_summary_read_count_is_independent_of_the_group_count(tmp_path, monkeypatch):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising()})
    small = pack_study.run_study(
        request=request_for(models=[fixed_horizon_model(TIMEFRAME, [30])]),
        data_root=pack, output_root=tmp_path / "run-small",
    )
    large = pack_study.run_study(
        request=request_for(models=[fixed_horizon_model(TIMEFRAME, [30, 60, 90, 120])]),
        data_root=pack, output_root=tmp_path / "run-large",
    )
    counter = _CountingReads(monkeypatch)

    counter.reset()
    small_results = pack_study.load_results(small["run_root"])
    small_summary = pack_study.summarize_results(small_results)
    small_reads = list(counter.calls)

    counter.reset()
    large_results = pack_study.load_results(large["run_root"])
    large_summary = pack_study.summarize_results(large_results)
    large_reads = list(counter.calls)

    assert len(small_summary["groups"]) == 2
    assert len(large_summary["groups"]) == 8
    # Two instruments, three tables each, whatever the group count is.
    assert sorted(small_reads) == sorted(large_reads)
    assert len(large_reads) == 6
    assert len(set(large_reads)) == 3


def test_r7_several_metrics_share_one_groups_observation_construction(tmp_path, monkeypatch):
    prefix = "ext_r7_metrics"
    root = tmp_path / "ext"
    write_module(root, prefix, MIXED_SUPPORT_MODEL.replace("{model_id}", f"{prefix}_model"))
    pack = build_pack(tmp_path / "pack")
    one = [{"id": "share", "metric": f"{prefix}_model_share"}]
    three = one + [
        {"id": "rows", "metric": f"{prefix}_model_rows"},
        {"id": "signal", "metric": f"{prefix}_model_signal"},
    ]
    document = request_for(
        models=[{"id": "mixed", "model": f"{prefix}_model", "settings": {}}],
        metrics=three,
        extensions=[declaration(root, prefix)],
    )
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )
    normalized = pack_study.normalize_request(document, source="test", base=None)
    results = pack_study.load_results(result["run_root"])

    counter = _CountingReads(monkeypatch)
    counter.reset()
    study_results.compute_metric_values(results, normalized.metrics[:1])
    single = len(counter.calls)
    counter.reset()
    values = study_results.compute_metric_values(results, normalized.metrics)
    triple = len(counter.calls)
    assert single == triple
    # The derived signal timestamp is a real observation column a metric can use.
    signal = [item for item in values["values"] if item["declaration_id"] == "signal"]
    assert signal and all(item["availability"] == "available" for item in signal)


def test_r7_pooled_statistics_match_the_observation_rows(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": rising(), "BBB-USDT-SWAP": rising()})
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=tmp_path / "run"
    )
    results = pack_study.load_results(result["run_root"])
    summary = pack_study.summarize_results(results)
    group = next(item for item in summary["groups"] if item["primary"])
    pooled = []
    for instrument_id in results.completed_instruments:
        frame = results.observations(
            instrument_id,
            model_instance_id=group["model_instance_id"],
            timeframe_minutes=group["timeframe_minutes"],
            case_id=group["case_id"],
            variant_id=group["variant_id"],
        )
        values = frame["net_return"].to_numpy(dtype=np.float64)
        pooled.append(values[frame["return_valid"].to_numpy(dtype=bool)])
    sample = np.concatenate(pooled)
    net = next(item for item in group["outcomes"] if item["name"] == "net_return")
    assert net["event_weighted"]["n"] == int(sample.size)
    assert net["event_weighted"]["mean"] == pytest.approx(float(np.mean(sample)))
    assert net["event_weighted"]["p90"] == pytest.approx(
        float(np.quantile(sample, 0.90, method="linear"))
    )


def test_r7_regeneration_verifies_the_completion_record_exactly_once(tmp_path, monkeypatch):
    _pack, run_root = completed_run(tmp_path)
    calls: list[str] = []
    real = study_evidence.verify_completion

    def counted(root):
        calls.append(str(root))
        return real(root)

    monkeypatch.setattr(study_evidence, "verify_completion", counted)
    pack_study.regenerate_report(run_root)
    assert len(calls) == 1


def test_r7_the_report_links_to_real_evidence_with_working_relative_paths(tmp_path):
    _pack, run_root = completed_run(tmp_path)
    html = (run_root / study_evidence.REPORT_FILE).read_text(encoding="utf-8")
    hrefs = [
        part.split('"', 1)[0]
        for part in html.split('<a href="')[1:]
    ]
    assert hrefs, "the report must link to its machine-readable evidence"
    derived = run_root / study_evidence.DERIVED_DIR
    for href in hrefs:
        assert "://" not in href
        assert (derived / href).resolve().is_file(), href
    assert any(href.endswith("bundle.json") for href in hrefs)
    assert "Authoritative completion is a matching verified completion.json" in html
