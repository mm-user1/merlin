"""Frozen generation, split admission, honest child seals and offline reading."""
import copy
import json
import shutil
from pathlib import Path

import pytest

from tools.pattern_lab import PatternLabDataError, analysis, candidate, data, study
from tools.pattern_lab.study import evidence, extensions, validation
from tools.pattern_lab.analysis import artifacts, request as analysis_request
from . import _helpers as h


def generation(root, *, version=2, context=True, days=92):
    tf = 60
    start = h.study_group_ms(24*32, tf)
    end = start + days*86400000
    groups = (32+days)*24
    pack = root/"pack"
    sources = []
    for index, name in enumerate(("AAA", "BBB")):
        stamps, values = h.timeframe_bars(tf, h.analysis_bar_specs(groups, 11+index))
        sources.append(h.instrument_source(stamps, values, symbol=name, contract=name+"-USDT-SWAP"))
    h.publish(pack, sources)
    protocol = h.study_protocol(first_ms=h.study_group_ms(0,tf), coverage_end_ms=end)
    protocol["development"] = {"start_utc":h.utc(h.study_group_ms(0,tf)), "end_utc":h.utc(start)}
    protocol["reserved"] = {"start_utc":h.utc(start), "end_utc":h.utc(end)}
    variants = list(copy.deepcopy(h.ANALYSIS_PAIR_VARIANTS))
    if context:
        variants.append({"id":"filtered", "hypothesis":"two_green_volume_panel",
                         "parameters":{"alias":"market", "threshold":.5}, "occurrence":"every_qualifying_bar"})
    doc = h.study_request(protocol=protocol, start_ms=h.study_group_ms(4,tf), end_ms=start,
        warmup_ms=h.study_group_ms(0,tf), timeframes=[tf], hypotheses=variants,
        models=[h.fixed_horizon_model(tf,[60,120,240,480],primary=240)])
    if version == 2:
        doc.update(schema_version=2, context={"market":{"ids":["TEST_AAA-USDT-SWAP","TEST_BBB-USDT-SWAP"]}} if context else {},
                   execution={"kind":"development"})
    study.run_study(request=doc, data_root=pack, output_root=root/"study")
    ar = h.analysis_request_document(schema_version=2)
    ar.pop("resamples"); ar.pop("seed")
    analysis.run_analysis(request=ar, run_root=root/"study", output_root=root/"analysis")
    frozen = candidate.freeze_candidate(study_root=root/"study", analysis_root=root/"analysis",
        start=h.utc(start), end=h.utc(end), warmup_start=h.utc(start-2*tf*60000), output=root/"candidate.json")
    return frozen, pack


@pytest.fixture(scope="module")
def frozen_generation(tmp_path_factory):
    root = tmp_path_factory.mktemp("candidate-generation")
    frozen, pack = generation(root)
    return root, frozen, pack


def test_roundtrip_code_policy_and_no_decode(frozen_generation, monkeypatch):
    root, frozen, pack = frozen_generation
    monkeypatch.setattr(data, "require_pyarrow", lambda: pytest.fail("freeze decoded numerical evidence"))
    again = candidate.freeze_candidate(study_root=root/"study", analysis_root=root/"analysis",
        start=frozen["split"]["evaluation"]["start_utc"], end=frozen["split"]["evaluation"]["end_utc"],
        warmup_start=frozen["split"]["evaluation"]["warmup_start_utc"], output=root/"second.json")
    assert again["candidate_id"] == frozen["candidate_id"]
    assert candidate.load_candidate(root/"candidate.json") == frozen
    assert set(frozen["required_code"]["core"]) == set(candidate.REQUIRED_CORE_MODULES+candidate.CONTEXT_CORE_MODULES)
    assert "tools.pattern_lab.candidate" not in frozen["required_code"]["core"]
    assert "tools.pattern_lab.study.report" not in frozen["required_code"]["core"]


@pytest.mark.parametrize("change", ["parameter", "period", "member", "family", "code", "type", "unknown"])
def test_candidate_drift_refused(frozen_generation, change):
    _, frozen, _ = frozen_generation
    changed = copy.deepcopy(frozen)
    if change == "parameter": changed["recipe"]["study"]["hypotheses"][-1]["parameters"]["threshold"] = .75
    if change == "period": changed["split"]["evaluation"]["end_utc"] = "2030-01-01T00:00:00Z"
    if change == "member": changed["recipe"]["study_semantic"]["context"]["market"]["ids"].pop()
    if change == "family": changed["recipe"]["analysis_family"]["members"].pop()
    if change == "code": changed["required_code"]["core"][candidate.REQUIRED_CORE_MODULES[0]] = "0"*64
    if change == "type": changed["schema_version"] = True
    if change == "unknown": changed["surprise"] = 1
    with pytest.raises(PatternLabDataError): candidate.load_candidate(changed)


def test_validation_short_descriptive_and_offline(frozen_generation, tmp_path, monkeypatch):
    root, frozen, pack = frozen_generation
    receipt = candidate.run_validation(candidate=frozen, data_root=pack, output_root=tmp_path/"validation", workers=2)
    assert receipt["status"] == "completed"
    run = tmp_path/"validation"
    for name, seal in receipt["children"].items():
        assert seal["completion_sha256"] == candidate.manifest.file_sha256(run/name/"completion.json")
        assert seal["evidence_set_sha256"] == json.loads((run/name/"completion.json").read_text())["evidence_set_sha256"]
    loaded = analysis.load_analysis(run/"analysis")
    assert loaded.summary["validation"]["candidate_id"] == frozen["candidate_id"]
    members = loaded.summary["members"]
    assert any(m["signal"] is not None for m in members)
    assert all(m["p_raw"] is None and m["unavailable_reasons"] for m in members)
    assert len(members) == frozen["recipe"]["analysis_family"]["family_size"]
    # Independent weighting oracle over the checked saved accumulations.
    strata = loaded.strata()
    for member in members:
        rows = strata.loc[(strata.member_id == member["member_id"]) & strata.retained]
        count = int(rows.target_observations.sum())
        assert count == member["supported_population"]["retained_target_observations"]
        if count:
            signal = rows.target_net_sum.sum()/count
            control = sum(row.target_observations * row.control_net_sum/row.control_observations
                          for row in rows.itertuples())/count
            assert member["signal"] == pytest.approx(signal)
            assert member["control"] == pytest.approx(control)
            assert member["lift"] == pytest.approx(signal-control)
    assert "Frozen candidate validation" in (run/"analysis"/artifacts.REPORT_FILE).read_text()
    relocated = tmp_path/"relocated"
    shutil.copytree(run/"analysis", relocated)
    def forbidden(*args, **kwargs): pytest.fail("offline report accessed execution inputs")
    monkeypatch.setattr(data, "read_session", forbidden)
    monkeypatch.setattr(study, "load_results", forbidden)
    from tools.pattern_lab.analysis import runner
    monkeypatch.setattr(runner, "evaluate_monthly_observations", forbidden)
    analysis.regenerate_report(relocated)
    assert "candidate_id" in analysis.load_analysis(relocated).comparisons().columns


def test_generation_drift_before_output(frozen_generation, tmp_path, monkeypatch):
    _, frozen, pack = frozen_generation
    original = extensions.file_digest
    def altered(path):
        return "0"*64 if str(path).endswith("monthly.py") else original(path)
    monkeypatch.setattr(extensions, "file_digest", altered)
    with pytest.raises(PatternLabDataError, match="required generation"):
        candidate.run_validation(candidate=frozen, data_root=pack, output_root=tmp_path/"bad")
    assert not (tmp_path/"bad").exists()


def test_development_and_edited_normalized_cannot_bypass(frozen_generation, tmp_path):
    _, frozen, pack = frozen_generation
    doc = copy.deepcopy(frozen["recipe"]["study"])
    doc["study"] = frozen["split"]["evaluation"]
    with pytest.raises(PatternLabDataError, match="development"):
        validation.validated_request(doc)
    doc["execution"] = {"kind":"validation"}
    with pytest.raises(PatternLabDataError, match="candidate"):
        validation.validated_request(doc)
    doc["execution"]["candidate"] = frozen
    normalized = validation.validated_request(doc)
    normalized.variants[-1].parameters["threshold"] = .75
    with pytest.raises(PatternLabDataError, match="candidate"):
        validation.validated_request(normalized)


@pytest.mark.parametrize("interrupted", [False, True])
def test_child_failure_retains_completed_study(frozen_generation, tmp_path, monkeypatch, interrupted):
    _, frozen, pack = frozen_generation
    from tools.pattern_lab.analysis import runner
    error_type = KeyboardInterrupt if interrupted else RuntimeError
    def fail(**kwargs): raise error_type("analysis deliberately failed")
    monkeypatch.setattr(runner, "run_analysis", fail)
    with pytest.raises(error_type, match="deliberately"):
        candidate.run_validation(candidate=frozen, data_root=pack, output_root=tmp_path/"failed")
    assert study.load_results(tmp_path/"failed"/"study").complete
    assert not (tmp_path/"failed"/"receipt.json").exists()
    assert json.loads((tmp_path/"failed"/"status.json").read_text())["terminal_status"] == ("interrupted" if interrupted else "failed")


def test_v1_source_lift(tmp_path):
    frozen, pack = generation(tmp_path/"legacy", version=1, context=False, days=3)
    assert frozen["discovery"]["source_study_version"] == 1
    assert frozen["recipe"]["study"]["context"] == {}
    candidate.run_validation(candidate=frozen, data_root=pack, output_root=tmp_path/"result")
    assert study.load_results(tmp_path/"result"/"study").request["schema_version"] == 2
    period = frozen["split"]["evaluation"]
    with pytest.raises(PatternLabDataError, match="validation-origin"):
        candidate.freeze_candidate(study_root=tmp_path/"result"/"study", analysis_root=tmp_path/"result"/"analysis",
            start=period["start_utc"], end=period["end_utc"], warmup_start=period["warmup_start_utc"], output=tmp_path/"bad.json")


def test_report_only_drift_and_split_rejections(frozen_generation, monkeypatch):
    _, frozen, _ = frozen_generation
    original = extensions.file_digest
    monkeypatch.setattr(extensions, "file_digest", lambda path: "0"*64 if str(path).endswith("report.py") else original(path))
    candidate.verify_current_generation(frozen)
    recipe = frozen["recipe"]
    period = frozen["split"]["evaluation"]
    with pytest.raises(PatternLabDataError, match="discovery end"):
        candidate._split(recipe, recipe["study_semantic"]["study"]["start_utc"], period["end_utc"], period["warmup_start_utc"])
    start = candidate.manifest.to_epoch_ms(period["start_utc"], "start")
    with pytest.raises(PatternLabDataError, match="development"):
        candidate._split(recipe, period["start_utc"], h.utc(start+86400000), period["warmup_start_utc"])
    with pytest.raises(PatternLabDataError, match="warmup"):
        candidate._split(recipe, period["start_utc"], period["end_utc"], period["start_utc"])
    internal = copy.deepcopy(recipe)
    internal["study_semantic"]["study"]["end_utc"] = h.utc(start-2*86400000)
    split = candidate._split(internal, h.utc(start-2*86400000), h.utc(start-86400000), h.utc(start-3*86400000))
    assert split["mode"] == "internal_reuse"


def test_cli_structured_status(frozen_generation, tmp_path, monkeypatch, capsys):
    from tools.pattern_lab import __main__ as cli, PatternLabBusyError, PatternLabPendingError
    root, frozen, pack = frozen_generation
    period = frozen["split"]["evaluation"]
    args = ["freeze-candidate", "--study-root",str(root/"study"), "--analysis-root",str(root/"analysis"),
            "--start",period["start_utc"], "--end",period["end_utc"], "--warmup-start",period["warmup_start_utc"],
            "--output",str(tmp_path/"cli.json")]
    monkeypatch.setattr(data, "require_pyarrow", lambda: pytest.fail("freeze dependency probe"))
    assert cli.main(args) == 0
    assert json.loads(capsys.readouterr().out)["candidate_id"] == frozen["candidate_id"]
    assert cli.main(args) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    validation_args = ["validate-candidate", "--candidate",str(root/"candidate.json"), "--data-root",str(pack), "--output-root",str(tmp_path/"validation")]
    for exception, code in ((PatternLabBusyError("busy"),3), (PatternLabPendingError("pending"),4), (KeyboardInterrupt(),130)):
        def fail(**kwargs): raise exception
        monkeypatch.setattr(candidate, "run_validation", fail)
        assert cli.main(validation_args) == code
        assert json.loads(capsys.readouterr().out)["status"] in ("failed","interrupted")
    monkeypatch.setattr(candidate, "freeze_candidate", lambda **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))
    assert cli.main(args) == 130
    assert json.loads(capsys.readouterr().out)["status"] == "interrupted"


def test_missing_attribution_and_shared_mismatch(frozen_generation):
    from tools.pattern_lab.analysis.source import admit_source
    root, frozen, _ = frozen_generation
    source = admit_source(root/"study", model_instances=["fh"])
    analyzed = artifacts.load_analysis(root/"analysis")
    source.results.source["core_source"].pop("tools.pattern_lab.data")
    with pytest.raises(PatternLabDataError, match="fresh development"):
        candidate._generation(source, analyzed, frozen["recipe"])
    source = admit_source(root/"study", model_instances=["fh"])
    source.results.source["core_source"]["tools.pattern_lab.study.contracts"] = "0"*64
    with pytest.raises(PatternLabDataError, match="shared"):
        candidate._generation(source, analyzed, frozen["recipe"])


def test_moved_discovery_partial_and_different_generation(frozen_generation, tmp_path):
    root, frozen, pack = frozen_generation
    period = frozen["split"]["evaluation"]
    def freeze(study_root, output):
        return candidate.freeze_candidate(study_root=study_root, analysis_root=root/"analysis",
            start=period["start_utc"], end=period["end_utc"], warmup_start=period["warmup_start_utc"], output=output)
    moved = shutil.copytree(root/"study", tmp_path/"moved")
    assert freeze(moved, tmp_path/"moved.json")["candidate_id"] == frozen["candidate_id"]
    (moved/"completion.json").unlink()
    with pytest.raises(PatternLabDataError, match="completion"):
        freeze(moved, tmp_path/"partial.json")
    study.run_study(request=frozen["recipe"]["study"], data_root=pack, output_root=tmp_path/"another")
    with pytest.raises(PatternLabDataError, match="different study generation"):
        freeze(tmp_path/"another", tmp_path/"different.json")


def test_candidate_paths_labels_and_clock_are_provenance(frozen_generation):
    _, frozen, _ = frozen_generation
    changed = copy.deepcopy(frozen)
    changed["provenance"]["frozen_utc"] = "2030-01-01T00:00:00Z"
    changed["discovery"]["binding"]["run_root"] = "a relocated source"
    changed["recipe"]["study"]["study_name"] = "A new display label"
    assert candidate.load_candidate(changed)["candidate_id"] == frozen["candidate_id"]


def test_output_overlap_and_inadequate_coverage_fail_before_parent(frozen_generation, tmp_path):
    root, frozen, pack = frozen_generation
    with pytest.raises(PatternLabDataError, match="overlap"):
        candidate.run_validation(candidate=frozen, data_root=pack, output_root=pack/"result")
    assert not (pack/"result").exists()
    short = tmp_path/"short"
    stamps, values = h.synthetic_series(240)
    h.publish(short, [h.instrument_source(stamps, values), h.instrument_source(stamps, values,symbol="BBB",contract="BBB-USDT-SWAP")])
    with pytest.raises(PatternLabDataError, match="metadata"):
        candidate.run_validation(candidate=frozen, data_root=short, output_root=tmp_path/"bad")
    assert not (tmp_path/"bad").exists()
