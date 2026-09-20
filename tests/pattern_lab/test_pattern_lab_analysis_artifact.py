"""The sealed M3a artifact: publication, failure, verification and rendering.

These cases drive the real coordinator over real completed studies, so the
recorded status, the hashed artifact set and the exit codes are observed rather
than asserted structurally.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabStudyError
from tools.pattern_lab import analysis as pack_analysis
from tools.pattern_lab.__main__ import main as cli_main
from tools.pattern_lab.analysis import artifacts as analysis_artifacts
from tools.pattern_lab.analysis import runner as analysis_runner
from tools.pattern_lab.analysis import source as analysis_source
from tools.pattern_lab.study import evidence as study_evidence

from ._helpers import analysis_request_document, analysis_source_study


@pytest.fixture(scope="module")
def source_study(tmp_path_factory):
    root = tmp_path_factory.mktemp("analysis-artifact-source")
    return analysis_source_study(root)


@pytest.fixture(scope="module")
def sealed(tmp_path_factory, source_study):
    _pack, run_root, _request = source_study
    output = tmp_path_factory.mktemp("analysis-artifact") / "analysis"
    result = pack_analysis.run_analysis(
        request=analysis_request_document(), run_root=run_root, output_root=output
    )
    return output, result


# --------------------------------------------------------------------------
# publication
# --------------------------------------------------------------------------

def test_the_published_layout_and_seal(sealed):
    root, result = sealed
    for name in analysis_artifacts.IMMUTABLE_FILES + (analysis_artifacts.COMPLETION_FILE,):
        assert (root / name).is_file(), name
    assert (root / analysis_artifacts.REPORT_FILE).is_file()
    record = json.loads((root / analysis_artifacts.COMPLETION_FILE).read_text(encoding="utf-8"))
    assert record["artifact"] == "pattern_lab_analysis"
    assert record["analysis_schema_version"] == 1
    # The seal never hashes itself and never hashes the regenerable report.
    assert set(record["evidence_sha256"]) == set(analysis_artifacts.IMMUTABLE_FILES)
    assert record["derived_files"] == [analysis_artifacts.REPORT_FILE]
    assert result["status"] == "completed"
    assert result["counts"]["processed_members"] == result["counts"]["planned_members"]


def test_a_completed_artifact_publishes_even_when_no_member_has_inference(sealed):
    _root, result = sealed
    assert result["members_with_inference"] == 0
    assert result["inference_scope"] == "approximate_development_screen"


def test_the_request_family_and_source_are_durable_before_aggregation(
    tmp_path, source_study, monkeypatch
):
    _pack, run_root, _request = source_study
    output = tmp_path / "interrupted"
    original = analysis_runner.evaluate_observations

    def explode(*args, **kwargs):
        raise RuntimeError("aggregation exploded")

    monkeypatch.setattr(analysis_runner, "evaluate_observations", explode)
    with pytest.raises(PatternLabStudyError, match="aggregation exploded"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    assert (output / analysis_artifacts.REQUEST_FILE).is_file()
    assert (output / analysis_artifacts.SOURCE_FILE).is_file()
    assert (output / analysis_artifacts.FAMILY_FILE).is_file()
    status = json.loads((output / analysis_artifacts.STATUS_FILE).read_text(encoding="utf-8"))
    assert status["terminal_status"] == "failed"
    assert status["failure"]["phase"] == "aggregate"
    assert not (output / analysis_artifacts.COMPLETION_FILE).exists()
    with pytest.raises(PatternLabDataError) as error:
        pack_analysis.load_analysis(output)
    assert error.value.error_code == "incomplete_analysis"


def test_an_interrupt_leaves_an_honest_status_and_no_seal(tmp_path, source_study, monkeypatch):
    _pack, run_root, _request = source_study
    output = tmp_path / "keyboard"

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(analysis_runner, "evaluate_observations", interrupt)
    with pytest.raises(KeyboardInterrupt):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    status = json.loads((output / analysis_artifacts.STATUS_FILE).read_text(encoding="utf-8"))
    assert status["terminal_status"] == "interrupted"
    assert status["failure"]["reason"] == "keyboard_interrupt"
    assert not (output / analysis_artifacts.COMPLETION_FILE).exists()


def _break_freeze_write(monkeypatch, target: str, error: BaseException) -> None:
    """Fail exactly one of the freeze writes, by the file it is writing."""
    original = study_evidence.write_json

    def write_json(path, payload):
        if Path(path).name == target:
            raise error
        return original(path, payload)

    monkeypatch.setattr(study_evidence, "write_json", write_json)


@pytest.mark.parametrize(
    "target", [analysis_artifacts.REQUEST_FILE, analysis_artifacts.FAMILY_FILE]
)
def test_a_freeze_write_failure_is_recorded_with_its_phase_and_root(
    tmp_path, source_study, monkeypatch, target
):
    """The first and a later freeze write are inside the protected region."""
    _pack, run_root, _request = source_study
    output = tmp_path / f"freeze-{target}"
    _break_freeze_write(monkeypatch, target, OSError("the freeze write failed"))
    with pytest.raises(PatternLabStudyError, match="the freeze write failed") as failure:
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    assert failure.value.context == {
        "operation": "analyze", "phase": "freeze", "analysis_root": str(output),
    }
    monkeypatch.undo()
    status = json.loads((output / analysis_artifacts.STATUS_FILE).read_text(encoding="utf-8"))
    assert status["terminal_status"] == "failed"
    assert status["failure"]["phase"] == "freeze"
    assert status["failure"]["reason"] == "OSError"
    assert not (output / analysis_artifacts.COMPLETION_FILE).exists()
    # The failing write left no file; the ones before it are already durable.
    assert not (output / target).exists()
    assert (output / analysis_artifacts.SOURCE_FILE).exists() is (
        target == analysis_artifacts.FAMILY_FILE
    )


def test_a_failing_initial_status_write_still_records_the_freeze_failure(
    tmp_path, source_study, monkeypatch
):
    _pack, run_root, _request = source_study
    output = tmp_path / "initial-status"
    calls = {"count": 0}
    original = analysis_artifacts.write_status

    def failing(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("the initial status is unwritable")
        return original(*args, **kwargs)

    monkeypatch.setattr(analysis_artifacts, "write_status", failing)
    with pytest.raises(PatternLabStudyError, match="the initial status is unwritable"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    monkeypatch.undo()
    # The request, source and family are already durable, and the diagnostic
    # status write is the second call, which succeeds.
    assert calls["count"] == 2
    status = json.loads((output / analysis_artifacts.STATUS_FILE).read_text(encoding="utf-8"))
    assert status["terminal_status"] == "failed"
    assert status["failure"]["phase"] == "freeze"


def test_an_interrupted_freeze_leaves_an_interrupted_status_and_no_seal(
    tmp_path, source_study, monkeypatch
):
    _pack, run_root, _request = source_study
    output = tmp_path / "freeze-interrupt"
    _break_freeze_write(monkeypatch, analysis_artifacts.SOURCE_FILE, KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    monkeypatch.undo()
    status = json.loads((output / analysis_artifacts.STATUS_FILE).read_text(encoding="utf-8"))
    assert status["terminal_status"] == "interrupted"
    assert status["failure"]["reason"] == "keyboard_interrupt"
    assert status["failure"]["phase"] == "freeze"
    assert not (output / analysis_artifacts.COMPLETION_FILE).exists()


def test_a_freeze_diagnostic_that_also_fails_preserves_the_original_cause(
    tmp_path, source_study, monkeypatch
):
    _pack, run_root, _request = source_study
    output = tmp_path / "freeze-diagnostic"
    _break_freeze_write(monkeypatch, analysis_artifacts.REQUEST_FILE, OSError("original cause"))
    monkeypatch.setattr(
        analysis_artifacts, "write_status",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("status is unwritable")),
    )
    with pytest.raises(PatternLabStudyError, match="original cause") as failure:
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    assert failure.value.context["phase"] == "freeze"
    monkeypatch.undo()
    assert not (output / analysis_artifacts.STATUS_FILE).exists()
    assert not (output / analysis_artifacts.COMPLETION_FILE).exists()


def test_the_cli_reports_a_freeze_failure_with_its_phase_and_root(
    tmp_path, source_study, monkeypatch, capsys
):
    _pack, run_root, _request = source_study
    spec = tmp_path / "analysis.json"
    spec.write_text(json.dumps(analysis_request_document()), encoding="utf-8")
    output = tmp_path / "cli-freeze"
    _break_freeze_write(monkeypatch, analysis_artifacts.REQUEST_FILE, OSError("no space left"))
    exit_code = cli_main(
        [
            "analyze", "--run-root", str(run_root), "--spec", str(spec),
            "--output-root", str(output),
        ]
    )
    monkeypatch.undo()
    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed" and payload["error_code"] == "analysis_failed"
    assert payload["context"] == {
        "operation": "analyze", "phase": "freeze", "analysis_root": str(output),
    }
    assert payload["cause"] == {"type": "OSError", "message": "no space left"}


def test_a_secondary_status_write_failure_preserves_the_original_cause(
    tmp_path, source_study, monkeypatch
):
    _pack, run_root, _request = source_study
    output = tmp_path / "status-failure"
    monkeypatch.setattr(
        analysis_runner, "evaluate_observations",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("original cause")),
    )
    calls = {"count": 0}
    original = analysis_artifacts.write_status

    def failing(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] > 1:
            raise OSError("status is unwritable")
        return original(*args, **kwargs)

    monkeypatch.setattr(analysis_artifacts, "write_status", failing)
    with pytest.raises(PatternLabStudyError, match="original cause"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )


def test_a_failed_output_root_is_never_reused_automatically(tmp_path, source_study, monkeypatch):
    _pack, run_root, _request = source_study
    output = tmp_path / "reuse"
    monkeypatch.setattr(
        analysis_runner, "evaluate_observations",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(PatternLabStudyError):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    monkeypatch.undo()
    with pytest.raises(PatternLabDataError, match="must be a new directory"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )


def test_the_output_root_may_not_overlap_the_source_run_including_symlinks(
    tmp_path, source_study
):
    _pack, run_root, _request = source_study
    with pytest.raises(PatternLabDataError, match="overlaps the source study run"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root,
            output_root=run_root / "inside",
        )
    link = tmp_path / "link"
    link.symlink_to(run_root, target_is_directory=True)
    with pytest.raises(PatternLabDataError, match="overlaps the source study run"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root,
            output_root=link / "inside",
        )


def test_the_source_study_is_never_mutated(tmp_path, source_study):
    _pack, run_root, _request = source_study
    before = {
        path.relative_to(run_root).as_posix(): study_evidence.file_digest(path)
        for path in sorted(run_root.rglob("*"))
        if path.is_file()
    }
    output = tmp_path / "nonmutating"
    pack_analysis.run_analysis(
        request=analysis_request_document(), run_root=run_root, output_root=output
    )
    after = {
        path.relative_to(run_root).as_posix(): study_evidence.file_digest(path)
        for path in sorted(run_root.rglob("*"))
        if path.is_file()
    }
    assert before == after


def test_a_source_that_changes_during_the_analysis_fails_without_a_seal(
    tmp_path, source_study, monkeypatch
):
    _pack, run_root, _request = source_study
    mutable = tmp_path / "mutable"
    shutil.copytree(run_root, mutable)
    output = tmp_path / "changed"
    original = analysis_source.reverify_source

    def mutate_then_verify(source, *, where):
        (mutable / "spec" / "request.json").write_text("{}", encoding="utf-8")
        return original(source, where=where)

    monkeypatch.setattr(analysis_source, "reverify_source", mutate_then_verify)
    with pytest.raises(PatternLabStudyError):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=mutable, output_root=output
        )
    assert not (output / analysis_artifacts.COMPLETION_FILE).exists()


# --------------------------------------------------------------------------
# verification and rendering
# --------------------------------------------------------------------------

def test_a_relocated_artifact_stays_readable_and_regenerable(tmp_path, sealed):
    root, _result = sealed
    moved = tmp_path / "moved" / "analysis"
    moved.parent.mkdir(parents=True)
    shutil.copytree(root, moved)
    loaded = pack_analysis.load_analysis(moved)
    assert loaded.completion["analysis_root"] != str(moved)
    assert len(loaded.members) == loaded.completion["counts"]["planned_members"]
    result = pack_analysis.regenerate_report(moved)
    assert result["status"] == "regenerated"


def test_regeneration_needs_neither_the_source_study_nor_the_pack(tmp_path, source_study):
    pack, run_root, _request = source_study
    work = tmp_path / "work"
    work.mkdir()
    shutil.copytree(run_root, work / "study")
    shutil.copytree(pack, work / "pack")
    output = work / "analysis"
    pack_analysis.run_analysis(
        request=analysis_request_document(), run_root=work / "study", output_root=output
    )
    shutil.rmtree(work / "study")
    shutil.rmtree(work / "pack")
    result = pack_analysis.regenerate_report(output)
    assert result["status"] == "regenerated"
    assert (output / analysis_artifacts.REPORT_FILE).read_text(encoding="utf-8").startswith(
        "<!DOCTYPE html>"
    )


def test_an_unsealed_or_mutated_artifact_is_rejected_before_any_rewrite(tmp_path, sealed):
    root, _result = sealed
    copy = tmp_path / "mutated"
    shutil.copytree(root, copy)
    before = (copy / analysis_artifacts.REPORT_FILE).read_text(encoding="utf-8")
    summary = json.loads((copy / analysis_artifacts.SUMMARY_FILE).read_text(encoding="utf-8"))
    summary["members"][0]["lift"] = 1.0
    (copy / analysis_artifacts.SUMMARY_FILE).write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(PatternLabDataError) as error:
        pack_analysis.regenerate_report(copy)
    assert error.value.error_code == "corrupt_evidence"
    assert (copy / analysis_artifacts.REPORT_FILE).read_text(encoding="utf-8") == before

    unsealed = tmp_path / "unsealed"
    shutil.copytree(root, unsealed)
    (unsealed / analysis_artifacts.COMPLETION_FILE).unlink()
    with pytest.raises(PatternLabDataError) as error:
        pack_analysis.regenerate_report(unsealed)
    assert error.value.error_code == "incomplete_analysis"


def test_a_rehashed_but_inconsistent_seal_is_rejected(tmp_path, sealed):
    """Matching bytes prove the files, not that the counts and family agree."""
    root, _result = sealed
    copy = tmp_path / "inconsistent"
    shutil.copytree(root, copy)
    record = json.loads((copy / analysis_artifacts.COMPLETION_FILE).read_text(encoding="utf-8"))
    record["counts"]["planned_members"] = record["counts"]["planned_members"] + 1
    record["counts"]["processed_members"] = record["counts"]["processed_members"] + 1
    (copy / analysis_artifacts.COMPLETION_FILE).write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(PatternLabDataError, match="contradicts the frozen family"):
        pack_analysis.load_analysis(copy)


def test_a_seal_whose_identities_contradict_its_artifacts_is_rejected(tmp_path, sealed):
    root, _result = sealed
    copy = tmp_path / "identity"
    shutil.copytree(root, copy)
    record = json.loads((copy / analysis_artifacts.COMPLETION_FILE).read_text(encoding="utf-8"))
    record["identities"]["family_sha256"] = "0" * 64
    (copy / analysis_artifacts.COMPLETION_FILE).write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(PatternLabDataError, match="contradict the artifacts"):
        pack_analysis.load_analysis(copy)


def test_a_study_completion_record_is_not_an_analysis_seal(tmp_path, source_study, sealed):
    root, _result = sealed
    _pack, run_root, _request = source_study
    copy = tmp_path / "wrong-kind"
    shutil.copytree(root, copy)
    shutil.copyfile(
        run_root / study_evidence.COMPLETION_FILE, copy / analysis_artifacts.COMPLETION_FILE
    )
    with pytest.raises(PatternLabDataError, match="analysis_schema_version|is not"):
        pack_analysis.load_analysis(copy)


def test_a_sealed_analysis_is_not_retroactively_failed_by_a_render_exception(
    tmp_path, sealed, monkeypatch
):
    root, _result = sealed
    copy = tmp_path / "render-failure"
    shutil.copytree(root, copy)
    monkeypatch.setattr(
        analysis_runner.analysis_report, "render_report",
        lambda summary: (_ for _ in ()).throw(RuntimeError("render exploded")),
    )
    with pytest.raises(RuntimeError, match="render exploded"):
        pack_analysis.regenerate_report(copy)
    monkeypatch.undo()
    reloaded = pack_analysis.load_analysis(copy)
    assert reloaded.completion["terminal_status"] == "completed"


# --------------------------------------------------------------------------
# saved tables and the report
# --------------------------------------------------------------------------

def test_the_saved_tables_are_documented_and_readable(sealed):
    root, _result = sealed
    loaded = pack_analysis.load_analysis(root)
    comparisons = loaded.comparisons()
    assert set(loaded.family["members"][0]) >= {"member_id", "comparison_id", "case_id"}
    assert list(comparisons["member_id"]) == [item["member_id"] for item in loaded.members]
    strata = loaded.strata()
    assert {"instrument_id", "utc_month", "retained", "exclusion_reasons"} <= set(strata.columns)
    daily = loaded.daily()
    assert {"member_id", "day_utc", "target_count", "control_net_sum"} <= set(daily.columns)
    # Every stratum stays visible, including zero-event and excluded ones.
    assert len(strata) >= int(strata["retained"].sum())


def test_the_report_escapes_labels_and_uses_no_remote_asset(tmp_path, source_study):
    _pack, run_root, _request = source_study
    output = tmp_path / "escaped"
    pack_analysis.run_analysis(
        request=analysis_request_document(analysis_name='<script>alert("x")</script>'),
        run_root=run_root,
        output_root=output,
    )
    html = (output / analysis_artifacts.REPORT_FILE).read_text(encoding="utf-8")
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html
    assert "http://" not in html and "cdn" not in html.lower()
    assert "unvalidated inference" in html.lower()
    assert "seven-day block does not control error" in html


def test_the_report_states_the_nominal_levels_beside_the_inference_table(sealed):
    root, _result = sealed
    html = (root / analysis_artifacts.REPORT_FILE).read_text(encoding="utf-8")
    assert "nominally two-sided at alpha 0.05" in html
    assert "Commission is included; funding and slippage are excluded" in html
    assert "not a validated edge" in html.lower()
    # The page states the measured calibration outcome, not a met envelope.
    assert "DID NOT MEET the declared empirical error envelope" in html


# --------------------------------------------------------------------------
# the command line
# --------------------------------------------------------------------------

def test_the_cli_and_the_api_produce_equal_results(tmp_path, source_study, capsys):
    _pack, run_root, _request = source_study
    spec = tmp_path / "analysis.json"
    spec.write_text(json.dumps(analysis_request_document()), encoding="utf-8")
    cli_root = tmp_path / "cli"
    exit_code = cli_main(
        [
            "analyze", "--run-root", str(run_root), "--spec", str(spec),
            "--output-root", str(cli_root),
        ]
    )
    assert exit_code == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["status"] == "completed"
    api_root = tmp_path / "api"
    api = pack_analysis.run_analysis(
        request=spec, run_root=run_root, output_root=api_root
    )
    assert printed["identities"]["analysis_semantic_sha256"] == api["identities"][
        "analysis_semantic_sha256"
    ]
    assert _numerical(cli_root) == _numerical(api_root)

    exit_code = cli_main(["analysis-report", "--analysis-root", str(cli_root)])
    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "regenerated"


def test_an_invalid_analysis_request_exits_two_with_structured_json(tmp_path, source_study, capsys):
    _pack, run_root, _request = source_study
    spec = tmp_path / "bad.json"
    spec.write_text(json.dumps(analysis_request_document(resamples=5)), encoding="utf-8")
    exit_code = cli_main(
        [
            "analyze", "--run-root", str(run_root), "--spec", str(spec),
            "--output-root", str(tmp_path / "never"),
        ]
    )
    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed" and payload["command"] == "analyze"
    assert not (tmp_path / "never").exists()


def test_cli_help_and_import_start_no_work(tmp_path):
    completed = subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-m", "tools.pattern_lab", "--help"],
        cwd=str(Path(__file__).resolve().parents[2]), capture_output=True, text=True, timeout=120,
    )
    assert completed.returncode == 0
    assert "analyze" in completed.stdout and "analysis-report" in completed.stdout
    imported = subprocess.run(
        [
            sys.executable, "-B", "-X", "utf8", "-c",
            "from tools.pattern_lab import analysis; print(analysis.METHOD_ID)",
        ],
        cwd=str(Path(__file__).resolve().parents[2]), capture_output=True, text=True, timeout=120,
    )
    assert imported.returncode == 0
    assert imported.stdout.strip() == "calendar_score_cbb_v1"


# --------------------------------------------------------------------------
# worker-invariant sources
# --------------------------------------------------------------------------

@pytest.mark.slow
def test_sources_from_workers_one_and_two_give_identical_numerical_results(tmp_path):
    from ._helpers import analysis_source_study as build

    direct = build(tmp_path / "w1", groups=1200, workers=1)
    pooled = build(tmp_path / "w2", groups=1200, workers=2)
    first = pack_analysis.run_analysis(
        request=analysis_request_document(), run_root=direct[1], output_root=tmp_path / "a1"
    )
    second = pack_analysis.run_analysis(
        request=analysis_request_document(), run_root=pooled[1], output_root=tmp_path / "a2"
    )
    # Physical evidence hashes may differ; the semantic identity and every
    # numerical field must not.
    assert first["identities"]["analysis_semantic_sha256"] == second["identities"][
        "analysis_semantic_sha256"
    ]
    assert _numerical(tmp_path / "a1") == _numerical(tmp_path / "a2")
    physical = [
        json.loads((root / analysis_artifacts.SOURCE_FILE).read_text(encoding="utf-8"))["physical"]
        for root in (tmp_path / "a1", tmp_path / "a2")
    ]
    assert physical[0]["evidence_set_sha256"] and physical[1]["evidence_set_sha256"]


def _numerical(root: Path) -> dict:
    """The numerical and semantic result fields, without physical provenance."""
    summary = json.loads((Path(root) / analysis_artifacts.SUMMARY_FILE).read_text(encoding="utf-8"))
    return {
        "members": summary["members"],
        "method": summary["method"],
        "calendar": summary["calendar"],
        "family_size": summary["family_size"],
        "resamples": summary["resamples"],
        "seed": summary["seed"],
        "instruments": summary["instruments"],
        "comparisons": summary["comparisons"],
    }


# --------------------------------------------------------------------------
# per-horizon support and month boundaries
# --------------------------------------------------------------------------

def test_horizons_that_retain_different_anchors_are_named_as_different_populations(sealed):
    """A longer horizon loses terminal anchors, so its population differs."""
    root, _result = sealed
    loaded = pack_analysis.load_analysis(root)
    by_group: dict[tuple, list] = {}
    for member in loaded.members:
        key = (member["comparison_id"], member["direction"])
        by_group.setdefault(key, []).append(member)
    for members in by_group.values():
        horizons = sorted(item["horizon_minutes"] for item in members)
        assert horizons == [60, 120]
        longer = next(item for item in members if item["horizon_minutes"] == 120)
        shorter = next(item for item in members if item["horizon_minutes"] == 60)
        # A short horizon is never trimmed to the longest one's support.
        assert (
            shorter["counts"]["valid_target_available"]
            >= longer["counts"]["valid_target_available"]
        )
        agreement = shorter["horizon_population"]["agrees_across_horizons"]
        assert agreement == longer["horizon_population"]["agrees_across_horizons"]
        if shorter["population_fingerprint"] != longer["population_fingerprint"]:
            assert agreement is False
            assert "different retained populations" in shorter["horizon_population"]["note"]
        else:
            assert agreement is True


def test_terminal_and_month_boundary_outcomes_are_counted_not_dropped(sealed):
    """Outcomes lost at the study end are lost support, never silent removals."""
    root, _result = sealed
    loaded = pack_analysis.load_analysis(root)
    horizon = max(item["horizon_minutes"] for item in loaded.members)
    longest = [item for item in loaded.members if item["horizon_minutes"] == horizon]
    lost = 0
    for member in longest:
        counts = member["counts"]
        # The loss accounting is exact for every member, never a silent removal.
        assert counts["valid_target_available"] == (
            counts["target_rows"] - counts["target_unavailable"] - counts["target_invalid_return"]
        )
        lost += counts["target_invalid_return"] + counts["control_invalid_return"]
    # The study end truncates the longest horizon's last anchors.
    assert lost > 0
    # Strata are keyed by the signal month; an outcome may cross a month
    # boundary, so several months are represented.
    strata = loaded.strata()
    member = strata.loc[strata["member_id"] == longest[0]["member_id"]]
    assert member["utc_month"].nunique() >= 2
    assert int(member["target_observations"].sum()) == longest[0]["counts"][
        "valid_target_available"
    ]


def test_a_failure_after_the_seal_never_rewrites_the_sealed_status(
    tmp_path, source_study, monkeypatch
):
    """The completion record is the point of no return for an analysis too."""
    _pack, run_root, _request = source_study
    output = tmp_path / "sealed-then-failed"
    original = analysis_artifacts.write_completion

    def seal_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("something after the seal")

    monkeypatch.setattr(analysis_artifacts, "write_completion", seal_then_fail)
    with pytest.raises(PatternLabStudyError, match="something after the seal"):
        pack_analysis.run_analysis(
            request=analysis_request_document(), run_root=run_root, output_root=output
        )
    monkeypatch.undo()
    status = json.loads((output / analysis_artifacts.STATUS_FILE).read_text(encoding="utf-8"))
    assert status["terminal_status"] == "completed"
    loaded = pack_analysis.load_analysis(output)
    assert loaded.completion["terminal_status"] == "completed"
