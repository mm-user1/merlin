"""Actual frozen generation, offline receipt verification and publication faults."""
import copy
from dataclasses import replace
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools.pattern_lab import PatternLabDataError, analysis, candidate, data, manifest, study
from tools.pattern_lab.analysis import artifacts, source
from tools.pattern_lab.study import contracts, evidence, extensions, results, validation
from . import _helpers as h


@pytest.fixture(scope="module")
def generation(tmp_path_factory):
    root = tmp_path_factory.mktemp("frozen-integrity")
    frozen = h.frozen_extension_generation(root)
    candidate.run_validation(candidate=frozen, data_root=root/"pack", output_root=root/"parent")
    return root, frozen


PROBE = '''
import copy, json, sys
from dataclasses import replace
from pathlib import Path
from tools.pattern_lab import candidate, data, study, PatternLabDataError
from tools.pattern_lab.study import validation
root, request_file, output, form, refuse = sys.argv[1:]
doc = json.loads(Path(request_file).read_text())
request = doc
if form == 'file': request = Path(request_file)
if refuse == 'yes':
    def forbidden(*args, **kwargs): raise AssertionError('pack/numerical work reached')
    data.read_session = forbidden
try:
    if form == 'normalized': request = replace(validation.validated_request(doc), notes='edited normalized object')
    study.run_study(request=request, data_root=Path(root)/'pack', output_root=output)
except PatternLabDataError as error:
    print(json.dumps({'error':str(error), 'output_exists':Path(output).exists()}))
else:
    result = study.load_results(output)
    print(json.dumps({'complete':result.complete, 'identities':result.completion['identities']}))
'''


def probe(root, doc, work, form, refuse):
    request_file = work/"request.json"
    request_file.write_text(json.dumps(doc), encoding="utf-8")
    result = subprocess.run([sys.executable, "-B", "-c", PROBE, str(root), str(request_file),
        str(work/"output"), form, "yes" if refuse else "no"], text=True, capture_output=True, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def validation_document(frozen):
    doc = copy.deepcopy(frozen["recipe"]["study"])
    doc.update(study=frozen["split"]["evaluation"], execution={"kind":"validation", "candidate":frozen})
    return doc


@pytest.mark.parametrize("form", ["mapping", "file", "normalized"])
@pytest.mark.parametrize("changed", ["module", "helper"])
def test_actual_alternate_generation_fails_before_pack_or_output(generation, tmp_path, form, changed):
    root, frozen = generation
    alternate = tmp_path/"alternate"
    shutil.copytree(root/"sources", alternate)
    doc = validation_document(frozen)
    declared = doc["extensions"][0]
    path = alternate/(declared["module"] + ".py" if changed == "module" else declared["helpers"][0])
    marker = tmp_path/"IMPORTED"
    path.write_bytes((f"from pathlib import Path as _P\n_P({str(marker)!r}).write_text('imported')\n").encode() + path.read_bytes())
    declared["source_root"] = str(alternate)
    result = probe(root, doc, tmp_path, form, True)
    required = {f["path"]:f["sha256"] for f in frozen["required_code"]["extensions"][0]["files"]}
    assert path.name in result["error"]
    assert required[path.name] in result["error"]
    assert extensions.file_digest(path) in result["error"]
    assert not result["output_exists"]
    assert not marker.exists()


def test_identical_move_without_old_source_root_and_spawn_parity(tmp_path):
    root = tmp_path/"generation"
    frozen = h.frozen_extension_generation(root, module="moved_frozen_extension")
    alternate = root/"moved-sources"
    # Both resolved paths are task-owned siblings; retain every original byte.
    assert (root/"sources").resolve().is_relative_to(tmp_path.resolve())
    assert alternate.resolve().is_relative_to(tmp_path.resolve())
    (root/"sources").rename(alternate)
    doc = validation_document(frozen)
    doc["extensions"][0]["source_root"] = str(alternate)
    direct = tmp_path/"direct"
    direct.mkdir()
    first = probe(root, doc, direct, "mapping", False)
    # A real spawn run uses the same chosen root and source records in workers.
    second_script = PROBE.replace("output_root=output)", "output_root=output, workers=2)")
    request_file = direct/"request.json"
    spawned = subprocess.run([sys.executable, "-B", "-c", second_script, str(root), str(request_file),
        str(tmp_path/"spawn"), "file", "no"], text=True, capture_output=True, timeout=90)
    assert spawned.returncode == 0, spawned.stdout + spawned.stderr
    second = json.loads(spawned.stdout)
    assert first["complete"] and second["complete"]
    assert first["identities"] == second["identities"]
    a, b = study.load_results(direct/"output"), study.load_results(tmp_path/"spawn")
    for table in ("conditions", "episodes", "emissions", "primitives"):
        assert a.table("TEST_AAA-USDT-SWAP", table).equals(b.table("TEST_AAA-USDT-SWAP", table))


@pytest.mark.parametrize("change", ["digest", "duplicate_module", "duplicate_helper", "missing"])
def test_saved_actual_attribution_is_checked_offline(generation, tmp_path, monkeypatch, change):
    root, frozen = generation
    target = tmp_path/"changed-study"
    shutil.copytree(root/"parent"/"study", target)
    path = target/evidence.SOURCE_FILE
    saved = evidence.read_json(path)
    records = saved["extensions"]
    if change == "digest": records[0]["files"][0]["sha256"] = "0"*64
    if change == "duplicate_module": records.append(copy.deepcopy(records[0]))
    if change == "duplicate_helper": records[0]["files"].append(copy.deepcopy(records[0]["files"][0]))
    if change == "missing": records[0]["files"].pop()
    evidence.write_json(path, saved)
    seal = evidence.read_json(target/evidence.COMPLETION_FILE)
    seal["evidence_sha256"][evidence.SOURCE_FILE] = manifest.file_sha256(path)
    seal["evidence_set_sha256"] = contracts.semantic_digest(seal["evidence_sha256"])
    evidence.write_json(target/evidence.COMPLETION_FILE, seal)
    evidence.verify_completion(target)  # A structurally valid, coherently rehashed seal.
    def forbidden(*args, **kwargs): pytest.fail("offline attribution accessed current execution inputs")
    monkeypatch.setattr(candidate, "verify_current_generation", forbidden)
    monkeypatch.setattr(extensions, "load_extensions", forbidden)
    monkeypatch.setattr(extensions, "file_digest", forbidden)
    monkeypatch.setattr(data, "read_session", forbidden)
    for reader in (lambda: study.load_results(target),
                   lambda: source.admit_source(target, model_instances=["fh"]),
                   lambda: analysis.run_analysis(request={k:v for k,v in frozen["recipe"]["analysis_request"].items() if k != "method"},
                       run_root=target, output_root=tmp_path/"new-analysis")):
        with pytest.raises(PatternLabDataError, match="candidate validation"):
            reader()
    assert not (tmp_path/"new-analysis").exists()


def test_relocated_parent_offline_without_decode_or_execution_inputs(generation, tmp_path, monkeypatch):
    root, _ = generation
    relocated = tmp_path/"relocated"
    shutil.copytree(root/"parent", relocated)
    expected = evidence.read_json(relocated/"receipt.json")
    def forbidden(*args, **kwargs): pytest.fail("offline validation accessed execution inputs/tables")
    for owner, name in ((candidate,"verify_current_generation"), (extensions,"load_extensions"),
                        (extensions,"file_digest"), (data,"read_session"), (evidence,"read_table")):
        monkeypatch.setattr(owner, name, forbidden)
    original = evidence.read_json
    def local_only(path):
        assert Path(path).resolve().is_relative_to(relocated.resolve())
        return original(path)
    monkeypatch.setattr(evidence, "read_json", local_only)
    assert candidate.load_validation(relocated) == expected


@pytest.mark.parametrize("change", ["absent", "version", "digest_shape", "metadata", "child_seal", "child_evidence", "foreign"])
def test_receipt_is_authoritative_and_exact(generation, tmp_path, change):
    root, frozen = generation
    target = tmp_path/"parent"
    shutil.copytree(root/"parent", target)
    receipt = evidence.read_json(target/"receipt.json")
    if change == "absent":
        (target/"receipt.json").unlink()
        assert evidence.read_json(target/"status.json")["terminal_status"] == "completed"
    elif change == "child_seal":
        path = target/"study"/"completion.json"
        path.write_bytes(path.read_bytes() + b"\n")
    elif change == "child_evidence":
        path = target/"analysis"/artifacts.SUMMARY_FILE
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        if change == "version": receipt["schema_version"] = 99
        if change == "digest_shape": receipt["children"]["study"]["completion_sha256"] = "bad"
        if change == "metadata": receipt["fixed_family_size"] += 1
        if change == "foreign":
            candidate.run_validation(candidate=frozen, data_root=root/"pack", output_root=tmp_path/"other")
            receipt = evidence.read_json(tmp_path/"other"/"receipt.json")
        evidence.write_json(target/"receipt.json", receipt)
    with pytest.raises(PatternLabDataError):
        candidate.load_validation(target)


@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("interrupt", [False, True])
def test_publication_fault_before_or_after_atomic_receipt(generation, tmp_path, monkeypatch, committed, interrupt):
    root, frozen = generation
    target = tmp_path/"parent"
    original = manifest.os.replace
    cause = KeyboardInterrupt("publication interrupted") if interrupt else OSError("publication failed")
    def fail(temporary, destination):
        if Path(destination) == target/"receipt.json":
            assert Path(temporary).is_file()
            if committed: original(temporary, destination)
            raise cause
        original(temporary, destination)
    monkeypatch.setattr(manifest.os, "replace", fail)
    if committed and not interrupt:
        assert candidate.run_validation(candidate=frozen, data_root=root/"pack", output_root=target)["status"] == "completed"
    else:
        with pytest.raises(type(cause)) as caught:
            candidate.run_validation(candidate=frozen, data_root=root/"pack", output_root=target)
        assert caught.value is cause
    assert study.load_results(target/"study").complete
    assert analysis.load_analysis(target/"analysis").completion
    status = evidence.read_json(target/"status.json")
    if committed:
        assert candidate.load_validation(target)["status"] == "completed"
        assert status["terminal_status"] == "completed"
    else:
        with pytest.raises(PatternLabDataError, match="missing final receipt"):
            candidate.load_validation(target)
        assert status["terminal_status"] == ("interrupted" if interrupt else "failed")
        assert status["error"] == str(cause)


def test_secondary_status_failure_preserves_original_publication_cause(generation, tmp_path, monkeypatch):
    root, frozen = generation
    target = tmp_path/"parent"
    original = evidence.write_json
    cause = OSError("original receipt failure")
    def fail(path, document):
        if Path(path) == target/"receipt.json":
            raise cause
        if Path(path) == target/"status.json" and document["terminal_status"] == "failed":
            raise OSError("secondary status failure")
        original(path, document)
    monkeypatch.setattr(evidence, "write_json", fail)
    with pytest.raises(OSError) as caught:
        candidate.run_validation(candidate=frozen, data_root=root/"pack", output_root=target)
    assert caught.value is cause
    with pytest.raises(PatternLabDataError, match="missing final receipt"):
        candidate.load_validation(target)


def test_summary_mapping_and_execution_diagnostics():
    assert results.SUMMARY_SCHEMA_VERSION == 1
    assert [results.summary_schema_version(v) for v in (1, 2)] == [1, 2]
    with pytest.raises(PatternLabDataError, match="unsupported study version 3"):
        results.summarize_results(type("Future", (), {"request":{"schema_version":3}})())
    for kind in ("surprise", None):
        with pytest.raises(PatternLabDataError, match="supported: development, validation"):
            validation.validate_saved_execution({"schema_version":2, "context":{}, "execution":{"kind":kind}})
    saved = validation.external_document(h.normalized_study(start_group=2, end_group=12))
    saved["study"]["warmup_start_utc"] = "bad"
    with pytest.raises(PatternLabDataError, match=r"fixture source.study.warmup_start_utc"):
        validation.validate_saved_execution(saved, source="fixture source")
