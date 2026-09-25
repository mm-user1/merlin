"""Policy-1 history, portable identities, exact generations and scoped LF checkout."""
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools.pattern_lab import PatternLabDataError, analysis, candidate, data, study
from tools.pattern_lab import __main__ as cli
from tools.pattern_lab.study import contracts, evidence, extensions, validation
from . import _helpers as h, _portable as p
from .test_pattern_lab_context import context_fixture

VECTORS = json.loads(Path(__file__).with_name("legacy_identity_vectors.json").read_text())


def identities(vector, **kwargs):
    return dict(specification=evidence.specification_identity(vector["request"], vector["family"], run_version=vector["run_version"], **kwargs),
                implementation=evidence.implementation_identity(vector["source"], run_version=vector["run_version"], **kwargs),
                data_input=evidence.data_input_identity(**{**vector["data"], "semantic_specification": vector["request"]}, run_version=vector["run_version"], **kwargs))


@pytest.mark.parametrize("vector", VECTORS, ids=["request-v1", "request-v2"])
def test_baseline_literal_digest_vectors(vector):
    assert identities(vector) == vector["expected"]
    assert identities(vector, identity_policy_version=1) == vector["expected"]
    assert all(identities(vector, identity_policy_version=2)[k] != v for k, v in vector["expected"].items())


@pytest.mark.parametrize("policy", [None, True, False, 2.0, "2", 0, 3])
def test_builder_policy_is_strict(policy):
    v = VECTORS[0]
    calls = [lambda: evidence.specification_identity(v["request"], v["family"], identity_policy_version=policy),
             lambda: evidence.implementation_identity(v["source"], identity_policy_version=policy),
             lambda: evidence.data_input_identity(**v["data"], identity_policy_version=policy)]
    for call in calls:
        with pytest.raises(PatternLabDataError, match="identity_policy_version"):
            call()


def test_projection_is_owned_shallow_and_preserves_snapshots():
    old = copy.deepcopy(VECTORS[1])
    moved = copy.deepcopy(old)
    moved["request"]["extensions"][0]["source_root"] = "/linux/elsewhere"
    moved["source"]["extensions"][0].update(source_root="/linux/elsewhere", module_path="/linux/elsewhere/probe.py")
    assert identities(old, identity_policy_version=2) == identities(moved, identity_policy_version=2)
    assert all(identities(old)[k] != identities(moved)[k] for k in identities(old))
    for key in ("path", "source_root", "label"):
        changed = copy.deepcopy(old)
        changed["request"]["parameters"][key] += "-changed"
        for name in ("specification", "data_input"):
            assert identities(changed, identity_policy_version=2)[name] != identities(old, identity_policy_version=2)[name]
    for container in ("request", "family"):
        changed = copy.deepcopy(old)
        changed[container]["execution"]["candidate"]["provenance"]["frozen_utc"] = "2026-09-02T00:00:00Z"
        assert identities(changed, identity_policy_version=2)["specification"] != identities(old, identity_policy_version=2)["specification"]
    assert old == VECTORS[1]  # Builders never mutate their inputs.


@pytest.mark.parametrize("change,affected", [
    ("module", {"specification", "data_input", "implementation"}),
    ("helpers", {"specification", "data_input", "implementation"}),
    ("core", {"implementation"}), ("bytes", {"implementation"}),
    ("fingerprint", {"data_input"}), ("context", {"data_input"}),
    ("bracket_rules", {"data_input"}), ("environment", {"implementation"}),
])
def test_projection_retains_computational_inputs(change, affected):
    v = copy.deepcopy(VECTORS[1])
    before = identities(v, identity_policy_version=2)
    if change == "module":
        v["request"]["extensions"][0]["module"] += "new"
        v["source"]["extensions"][0]["module"] += "new"
    elif change == "helpers":
        v["request"]["extensions"][0]["helpers"].append("new.py")
        v["source"]["extensions"][0]["files"].append({"path": "new.py", "sha256": "0" * 64})
    elif change == "core": v["source"]["core_source"]["module"] = "0" * 64
    elif change == "bytes": v["source"]["extensions"][0]["files"][0]["sha256"] = "0" * 64
    elif change == "environment": v["source"]["library_versions"]["python"] = "3.11.0rc1"
    elif change == "fingerprint": v["data"]["fingerprints"][0]["input_fingerprint"] = "0" * 64
    elif change == "context": v["data"]["context"]["features"]["reference"] = "0" * 64
    elif change == "bracket_rules": v["data"]["bracket_rules"]["AAA"]["tick_size"] = "0.02"
    after = identities(v, identity_policy_version=2)
    assert {k for k in before if before[k] != after[k]} == affected


@pytest.fixture(scope="module")
def generation(tmp_path_factory):
    root = tmp_path_factory.mktemp("portable")
    frozen = p.frozen_generation(root)
    return root, frozen


def test_saved_context_identities_recompute_independently(tmp_path):
    request = context_fixture(tmp_path / "pack")
    result = study.run_study(request=request, data_root=tmp_path / "pack", output_root=tmp_path / "run")
    assert p.recompute(tmp_path / "run") == result["identities"]
    assert evidence.read_json(tmp_path / "run/context.json")  # Includes computed outputs.


def reseal(root):
    seal = evidence.read_json(root / evidence.COMPLETION_FILE)
    seal["evidence_sha256"][evidence.SOURCE_FILE] = evidence.file_digest(root / evidence.SOURCE_FILE)
    seal["evidence_set_sha256"] = contracts.semantic_digest(seal["evidence_sha256"])
    evidence.write_json(root / evidence.COMPLETION_FILE, seal)


@pytest.mark.parametrize("value", [1, None, True, False, 2.0, "2", 3])
@pytest.mark.parametrize("partial", [False, True])
def test_marker_rejects_coherent_reseal(generation, tmp_path, value, partial):
    root, _ = generation
    shutil.copytree(root / "study", tmp_path / "run")
    target = tmp_path / "run"
    source = evidence.read_json(target / evidence.SOURCE_FILE)
    source["identity_policy_version"] = value
    evidence.write_json(target / evidence.SOURCE_FILE, source)
    reseal(target)
    evidence.verify_completion(target)
    with pytest.raises(PatternLabDataError, match="identity_policy_version"):
        study.load_results(target, allow_partial=partial)


def test_marker_single_source_location_and_historical_absence(generation, tmp_path):
    root, frozen = generation
    assert p.recompute(root / "study") == study.load_results(root / "study").completion["identities"]
    for name in ("spec/request.json", "spec/family.json", "provenance.json", "completion.json"):
        assert "identity_policy_version" not in (root / "study" / name).read_text()
    assert "identity_policy_version" not in json.dumps(frozen["recipe"])
    assert "identity_policy_version" not in json.dumps(analysis.load_analysis(root / "analysis").source)
    shutil.copytree(root / "study", tmp_path / "old")
    saved = evidence.read_json(tmp_path / "old" / evidence.SOURCE_FILE)
    saved.pop("identity_policy_version")
    evidence.write_json(tmp_path / "old" / evidence.SOURCE_FILE, saved)
    reseal(tmp_path / "old")
    assert study.load_results(tmp_path / "old").complete
    (tmp_path / "old/completion.json").unlink()
    assert study.load_results(tmp_path / "old", allow_partial=True)


class BytesPath:
    def __fspath__(self): return b"/bytes"


@pytest.mark.parametrize("overrides", [[], True, "x", {"unknown": "x"}, {1: "x"},
    {"portable_signal": b"x"}, {"portable_signal": True}, {"portable_signal": BytesPath()},
    {"portable_signal": ""}, {"portable_signal": " "}, {"portable_signal": "relative"},
    {"portable_signal": "C:relative"}, {"portable_signal": "\\relative"}])
def test_api_rejects_bad_overrides_before_inputs(generation, tmp_path, monkeypatch, overrides):
    _, frozen = generation
    def forbidden(*args, **kwargs): pytest.fail("pack/import reached")
    monkeypatch.setattr(data, "read_session", forbidden)
    monkeypatch.setattr(extensions, "load_extensions", forbidden)
    original = copy.deepcopy(frozen)
    with pytest.raises(PatternLabDataError, match="extension_roots"):
        candidate.run_validation(candidate=frozen, data_root="absent", output_root=tmp_path / "out", extension_roots=overrides)
    assert frozen == original and not (tmp_path / "out").exists()


@pytest.mark.parametrize("change", ["missing_root", "missing_main", "missing_helper", "main", "helper", "unused_helper"])
def test_api_preflight_all_bytes_before_import(generation, tmp_path, monkeypatch, change):
    root, frozen = generation
    alternate = tmp_path / "alternate"
    shutil.copytree(root / "sources", alternate)
    module = "portable_unused" if change == "unused_helper" else "portable_signal"
    name = module + ("_helper.py" if "helper" in change else ".py")
    path = alternate / name
    marker = tmp_path / "IMPORTED"
    if change == "missing_root": alternate = tmp_path / "missing"
    elif change.startswith("missing_"): path.unlink()
    else: path.write_bytes(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n".encode() + path.read_bytes())
    def forbidden(*args, **kwargs): pytest.fail("pack/import reached")
    monkeypatch.setattr(data, "read_session", forbidden)
    monkeypatch.setattr(extensions, "load_extensions", forbidden)
    overrides = {item["module"]: alternate for item in frozen["recipe"]["study"]["extensions"]}
    with pytest.raises(PatternLabDataError):
        candidate.run_validation(candidate=frozen, data_root=root / "pack", output_root=tmp_path / "out", extension_roots=overrides)
    assert not marker.exists() and not (tmp_path / "out").exists()


def test_normalized_revalidation_preflights_before_cached_import(generation, tmp_path, monkeypatch):
    root, frozen = generation
    doc = copy.deepcopy(frozen["recipe"]["study"])
    doc.update(study=frozen["split"]["evaluation"], execution={"kind": "validation", "candidate": frozen})
    normalized = validation.validated_request(doc)
    alternate = tmp_path / "alternate"
    shutil.copytree(root / "sources", alternate)
    helper = alternate / "portable_signal_helper.py"
    helper.write_bytes(helper.read_bytes() + b"# changed\n")
    normalized = replace(normalized, extensions=tuple(replace(d, source_root=str(alternate)) for d in normalized.extensions))
    def forbidden(*args, **kwargs): pytest.fail("pack/import reached")
    monkeypatch.setattr(data, "read_session", forbidden)
    monkeypatch.setattr(extensions, "load_extensions", forbidden)
    with pytest.raises(PatternLabDataError, match="candidate validation"):
        study.run_study(request=normalized, data_root=root / "pack", output_root=tmp_path / "out")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("values", [["bad"], ["=dir"], ["portable_signal="], ["portable_signal=x", "portable_signal=x"], ["unknown=x"]])
def test_cli_override_errors_are_structured(generation, tmp_path, capsys, values):
    root, _ = generation
    args = ["validate-candidate", "--candidate", str(root / "candidate.json"), "--data-root", "absent", "--output-root", str(tmp_path / "out")]
    for value in values: args += ["--extension-root", value]
    assert cli.main(args) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"]
    assert "extension" in str(payload["error"])
    assert not (tmp_path / "out").exists()


def test_cli_directory_spaces_equals_and_relative(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert cli._extension_roots(["module=folder with = sign"])["module"] == str((tmp_path / "folder with = sign").resolve())
    drive = str(tmp_path / "drive path")
    assert cli._extension_roots(["module=" + drive]) == {"module": drive}


def child(code, *args):
    result = subprocess.run([sys.executable, "-B", "-c", code, *map(str, args)], capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


VALIDATE = '''
import json, sys
from pathlib import Path
from tools.pattern_lab import candidate
frozen, pack, output, sources, workers = sys.argv[1:]
result = candidate.run_validation(candidate=frozen, data_root=pack, output_root=output, workers=int(workers),
    extension_roots={name:Path(sources) for name in ('portable_signal', 'portable_unused')})
print(json.dumps(result))
'''


def test_actual_cross_root_spawn_receipts_analysis_and_refreeze(generation, tmp_path):
    root, frozen = generation
    sources = tmp_path / "other sources=LF"
    pack = tmp_path / "other-pack"
    shutil.copytree(root / "sources", sources)
    shutil.copytree(root / "pack", pack)
    child(VALIDATE, root / "candidate.json", root / "pack", tmp_path / "one", root / "sources", 1)
    child(VALIDATE, root / "candidate.json", pack, tmp_path / "two", sources, 2)
    a, b = [study.load_results(tmp_path / name / "study") for name in ("one", "two")]
    assert a.completion["identities"] == b.completion["identities"]
    assert a.family["execution"]["candidate"] == b.family["execution"]["candidate"] == frozen
    assert a.request["execution"]["candidate"] == b.request["execution"]["candidate"] == frozen
    assert b.request["extensions"][0]["source_root"] == str(sources)
    for instrument in a.completed_instruments:
        for table in evidence.TABLE_NAMES:
            assert a.table(instrument, table).equals(b.table(instrument, table))
    assert len(a.table(a.completed_instruments[0], "emissions")) > 0
    assert evidence.read_json(tmp_path / "one/study/derived/summary.json")["groups"] == evidence.read_json(tmp_path / "two/study/derived/summary.json")["groups"]
    aa, ab = [analysis.load_analysis(tmp_path / name / "analysis") for name in ("one", "two")]
    assert aa.completion["identities"] == ab.completion["identities"]
    assert aa.comparisons().equals(ab.comparisons())
    assert candidate.load_validation(tmp_path / "two")["candidate_id"] == frozen["candidate_id"]
    assert p.recompute(tmp_path / "two/study") == b.completion["identities"]
    split = frozen["split"]["evaluation"]
    refrozen = candidate.freeze_candidate(study_root=root / "study", analysis_root=root / "analysis",
        start=split["start_utc"], end=split["end_utc"], warmup_start=split["warmup_start_utc"], output=tmp_path / "refrozen.json")
    assert refrozen["candidate_id"] == frozen["candidate_id"]
    assert refrozen["provenance"] != frozen["provenance"]
    child(VALIDATE, tmp_path / "refrozen.json", pack, tmp_path / "refrozen", sources, 1)
    changed = study.load_results(tmp_path / "refrozen/study")
    assert changed.completion["identities"]["specification_sha256"] != a.completion["identities"]["specification_sha256"]
    assert changed.completion["identities"]["data_input_sha256"] != a.completion["identities"]["data_input_sha256"]
    assert analysis.load_analysis(tmp_path / "refrozen/analysis").completion["identities"]["analysis_semantic_sha256"] != aa.completion["identities"]["analysis_semantic_sha256"]


def test_root_cache_requires_fresh_interpreter(generation, tmp_path):
    root, frozen = generation
    shutil.copytree(root / "sources", tmp_path / "moved")
    from tools.pattern_lab.study.spec import ExtensionDeclaration
    declarations = [ExtensionDeclaration(**{**d, "source_root": str(tmp_path / "moved")}) for d in frozen["recipe"]["study"]["extensions"]]
    with pytest.raises(PatternLabDataError, match="root changed.*fresh interpreter"):
        extensions.load_extensions(declarations)
    assert extensions._LOADED["portable_signal"].source_root == str(root / "sources")


def test_final_check_rehashes_effective_helper(generation, tmp_path):
    root, _ = generation
    shutil.copytree(root / "sources", tmp_path / "moved")
    code = VALIDATE.replace("result = candidate.run_validation", '''
from tools.pattern_lab import PatternLabDataError
original = candidate._verified_validation_children
def changed(*args):
    result = original(*args)
    helper = Path(sources)/'portable_unused_helper.py'
    helper.write_bytes(helper.read_bytes() + b'# changed after children\\n')
    return result
candidate._verified_validation_children = changed
try:
    result = candidate.run_validation''').replace("print(json.dumps(result))", '''
except PatternLabDataError as error:
    print(json.dumps({'error':str(error), 'receipt':(Path(output)/'receipt.json').exists()}))
else: raise AssertionError('changed helper accepted')
''')
    result = child(code, root / "candidate.json", root / "pack", tmp_path / "out", tmp_path / "moved", 1)
    assert "portable_unused_helper.py" in result["error"] and not result["receipt"]


def scoped_sources():
    paths = subprocess.check_output(["git", "ls-files", "tools/pattern_lab/*.py"], cwd=h.REPO_ROOT, text=True).splitlines()
    return paths + [name for name in extensions.bracket_source_digests() if name.startswith("src/")]


def test_scoped_tracked_sources_have_no_cr():
    assert not [name for name in scoped_sources() if b"\r" in (h.REPO_ROOT / name).read_bytes()]


def test_lf_checkout_scope_with_autocrlf(tmp_path):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=tmp_path, stderr=subprocess.STDOUT)
    git("init", "-q")
    git("config", "core.autocrlf", "true")
    shutil.copyfile(h.REPO_ROOT / ".gitattributes", tmp_path / ".gitattributes")
    scope = ["tools/pattern_lab/root.py", "tools/pattern_lab/nested/deep/module.py"] + [s for s in scoped_sources() if s.startswith("src/")]
    paths = scope + ["outside.py", "data/raw/fixture.csv", "data/baseline_v2/fixture.csv"]
    for name in paths:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"line one\r\nline two\r\n")
    git("add", ".")  # Only this task-owned external scratch repository.
    git("-c", "user.name=T09 test", "-c", "user.email=t09@example.invalid", "commit", "-qm", "fixture")
    for name in paths: (tmp_path / name).unlink()
    git("checkout", "--", ".")
    for name in scope: assert (tmp_path / name).read_bytes() == b"line one\nline two\n"
    for name in paths[len(scope):]: assert (tmp_path / name).read_bytes() == b"line one\r\nline two\r\n"
    # Git creates read-only objects on Windows; leave this external fixture
    # removable by the ordinary isolated test launcher.
    for path in (tmp_path / ".git").rglob("*"):
        if path.is_file(): path.chmod(0o600)


def test_v1_writer_also_marks_policy_two(tmp_path):
    h.publish(tmp_path / "pack", [h.instrument_source(*h.synthetic_series(240))])
    request = validation.external_document(h.normalized_study(start_group=2, end_group=35))
    result = study.run_study(request=request, data_root=tmp_path / "pack", output_root=tmp_path / "study")
    assert evidence.read_json(tmp_path / "study/spec/source.json")["identity_policy_version"] == 2
    assert p.recompute(tmp_path / "study") == result["identities"]


@pytest.mark.parametrize("name", ["path", "source_root", "label"])
def test_real_hypothesis_physical_named_parameter_changes_identity(generation, tmp_path, name):
    root, frozen = generation
    request = copy.deepcopy(frozen["recipe"]["study"])
    request["instruments"] = evidence.read_json(root / "study/spec/request.json")["instruments"]
    request["hypotheses"][0]["parameters"][name] = 1
    result = study.run_study(request=request, data_root=root / "pack", output_root=tmp_path / "changed")
    old = study.load_results(root / "study")
    new = study.load_results(tmp_path / "changed")
    for key in ("specification_sha256", "data_input_sha256"):
        assert result["identities"][key] != old.completion["identities"][key]
    assert result["identities"]["implementation_sha256"] == old.completion["identities"]["implementation_sha256"]
    assert not new.table(new.completed_instruments[0], "emissions").equals(old.table(old.completed_instruments[0], "emissions"))


@pytest.mark.parametrize("overrides", [None, {}])
def test_default_roots_and_empty_map(generation, tmp_path, overrides):
    root, frozen = generation
    result = candidate.run_validation(candidate=frozen, data_root=root / "pack", output_root=tmp_path / "out", extension_roots=overrides)
    assert result["candidate_id"] == frozen["candidate_id"]


def test_partial_map_and_shared_text_pathlike_roots(generation, tmp_path):
    root, frozen = generation
    class TextPath:
        def __fspath__(self): return str(root / "sources")
    for names in [("portable_signal",), ("portable_signal", "portable_unused")]:
        document = copy.deepcopy(frozen["recipe"]["study"])
        candidate._relocate_extensions(document, {name: TextPath() for name in names})
        assert document == frozen["recipe"]["study"]
    result = candidate.run_validation(candidate=frozen, data_root=root / "pack", output_root=tmp_path / "out",
                                      extension_roots={"portable_signal": TextPath()})
    assert result["status"] == "completed"


def test_output_cannot_overlap_effective_source(generation):
    root, frozen = generation
    target = root / "sources/new-output"
    with pytest.raises(PatternLabDataError, match="overlap"):
        candidate.run_validation(candidate=frozen, data_root=root / "pack", output_root=target,
                                 extension_roots={"portable_signal": root / "sources"})
    assert not target.exists()


def test_cli_absent_old_roots_and_offline_relocation(generation, tmp_path, monkeypatch):
    root = tmp_path / "separate"
    child('''
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd()/'tests'))
from pattern_lab import _portable
print(json.dumps(_portable.frozen_generation(Path(sys.argv[1]))))
''', root)
    frozen = candidate.load_candidate(root / "candidate.json")
    sources = tmp_path / "sources"
    assert (root / "sources").resolve().is_relative_to(tmp_path.resolve())
    assert sources.resolve().is_relative_to(tmp_path.resolve())
    (root / "sources").rename(sources)
    original = copy.deepcopy(frozen)
    args = [sys.executable, "-B", "-m", "tools.pattern_lab", "validate-candidate", "--candidate", str(root / "candidate.json"),
            "--data-root", str(root / "pack"), "--output-root", str(tmp_path / "parent")]
    for name in ("portable_signal", "portable_unused"):
        args += ["--extension-root", name + "=" + str(sources)]
    run = subprocess.run(args, capture_output=True, text=True, timeout=180)
    assert run.returncode == 0, run.stdout + run.stderr
    assert evidence.read_json(root / "candidate.json") == original
    shutil.copytree(tmp_path / "parent", tmp_path / "offline")
    def forbidden(*args, **kwargs): pytest.fail("offline reader accessed current execution inputs")
    for owner, name in [(data, "read_session"), (extensions, "file_digest"), (extensions, "load_extensions"), (candidate, "verify_current_generation")]:
        monkeypatch.setattr(owner, name, forbidden)
    assert candidate.load_validation(tmp_path / "offline")["candidate_id"] == frozen["candidate_id"]
    study.regenerate_report(tmp_path / "offline/study")
    analysis.regenerate_report(tmp_path / "offline/analysis")


def test_extension_lf_crlf_changes_only_implementation_and_fails_frozen(generation, tmp_path):
    root, frozen = generation
    outputs = []
    for ending in ("lf", "crlf"):
        sources = tmp_path / ending
        sources.mkdir()
        for path in (root / "sources").glob("*.py"):
            raw = path.read_bytes()
            assert b"\r" not in raw
            (sources / path.name).write_bytes(raw if ending == "lf" else raw.replace(b"\n", b"\r\n"))
        doc = copy.deepcopy(frozen["recipe"]["study"])
        for item in doc["extensions"]: item["source_root"] = str(sources)
        evidence.write_json(tmp_path / (ending + ".json"), doc)
        code = '''
import json, sys
from tools.pattern_lab import study
print(json.dumps(study.run_study(request=sys.argv[1], data_root=sys.argv[2], output_root=sys.argv[3])))
'''
        outputs.append(child(code, tmp_path / (ending + ".json"), root / "pack", tmp_path / (ending + "-study"))["identities"])
    assert outputs[0]["specification_sha256"] == outputs[1]["specification_sha256"]
    assert outputs[0]["data_input_sha256"] == outputs[1]["data_input_sha256"]
    assert outputs[0]["implementation_sha256"] != outputs[1]["implementation_sha256"]
    with pytest.raises(PatternLabDataError, match="candidate validation"):
        candidate.run_validation(candidate=frozen, data_root=root / "pack", output_root=tmp_path / "refused",
            extension_roots={name: tmp_path / "crlf" for name in ("portable_signal", "portable_unused")})
    assert not (tmp_path / "refused").exists()
