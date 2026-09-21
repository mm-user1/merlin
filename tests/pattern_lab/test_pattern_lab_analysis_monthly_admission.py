"""Compact constructed evidence tests the gate without Monte Carlo generation."""
from copy import deepcopy
from dataclasses import replace
import gzip
import hashlib
import json
import shutil

import pytest

from tools.pattern_lab.analysis import calibration_monthly as monthly
from tools.pattern_lab.analysis import calibration_evidence as admission
from tools.pattern_lab.analysis import calibration as cal
from tools.pattern_lab.study.contracts import semantic_digest


def _row(fixture, *, available=True, reject=False):
    ids, primary = monthly._fixture_family(fixture)
    size = len(ids)
    p = .001 if reject else .5
    lo, hi = (1., 2.) if reject else (-1., 1.)
    return dict(id=20000, primary_member_id=primary, available=available,
                reasons=[] if available else ["insufficient_span"],
                inherited_reasons=[] if available else ["insufficient_span"],
                lift=1.5 if reject else 0., standard_error=.5 if available else None,
                interval_lower=lo if available else None, interval_upper=hi if available else None,
                interval_width=hi-lo if available else None,
                p_raw=p if available else None, p_holm=min(1.,p*size) if available else None,
                reject_raw=reject if available else None, reject_holm=reject if available else None,
                noncoverage=reject if available else None,
                informative_months=12 if available else None, degrees_of_freedom=11 if available else None,
                family_size=size, family_available=size if available else 0,
                family_any_rejection=available and reject, member_ids=ids,
                member_available=[available]*size, member_p_raw=[p if available else None]*size,
                member_p_holm=[min(1.,p*size) if available else None]*size,
                member_reject_raw=[reject if available else None]*size,
                member_reject_holm=[reject if available else None]*size)


def _write(path, value):
    path.write_text(json.dumps(value, allow_nan=False), encoding="utf-8")


def _rehash(root, manifest):
    manifest["manifest_digest"] = semantic_digest({k:v for k,v in manifest.items() if k != "manifest_digest"})
    _write(root / "manifest.json", manifest)
    run = json.loads((root / "run.json").read_text())
    run["manifest_digest"] = manifest["manifest_digest"]
    _write(root / "run.json", run)


def _save_rows(root, fixture, rows):
    (root / "records" / f"{fixture.label}.json.gz").write_bytes(gzip.compress(json.dumps({"rows":rows}).encode()))


def _rows(root, fixture):
    return monthly.read_records(root, fixture.label)["rows"]


@pytest.fixture(scope="module")
def complete_archive(tmp_path_factory):
    root = tmp_path_factory.mktemp("monthly-constructed")
    (root / "records").mkdir()
    manifest = monthly.candidate_manifest()
    _write(root / "manifest.json", manifest)
    for fixture in monthly.MAIN_MATRIX + monthly.SUPPLEMENTARY_MATRIX:
        row = _row(fixture, available=fixture.kind != "refusal")
        _save_rows(root, fixture, [{**row, "id":20000+i} for i in range(fixture.attempts)])
    replays = {}
    for name in ("candle_disk_replay", "candidate_evidence_replay"):
        ids, _ = monthly._fixture_family(monthly.MAIN_MATRIX[0])
        structure = {f"{side}_member_ids":ids for side in ("expected","left","right")}
        structure.update({f"{side}_member_count":len(ids) for side in ("expected","left","right")})
        replays[name] = dict(repetitions=[20000,20001], agrees=True, mismatched=[], max_absolute_difference=0.,
                            comparisons=[dict(repetition=i, mismatched=[], max_absolute_difference=0., **structure)
                                         for i in (20000,20001)])
    replays["candidate_evidence_replay"]["scenario"] = "null_dependent_t5"
    _write(root / "run.json", dict(schema_version=2, method=monthly.CANDIDATE_METHOD_ID,
           plan_version=1, decision_policy_version=2, manifest_digest=manifest["manifest_digest"],
           status="completed", incomplete_reason=None, stops=[], attempts_override=None,
           planned_fixtures=[f.label for f in monthly.MAIN_MATRIX + monthly.SUPPLEMENTARY_MATRIX], replays=replays))
    return root


@pytest.fixture
def archive(complete_archive, tmp_path):
    root = tmp_path / "archive"
    shutil.copytree(complete_archive, root)
    return root


def test_complete_pass_needs_no_generation_estimation_or_memory(archive, monkeypatch):
    def forbidden(*a, **k):
        pytest.fail("offline scorer reached a generation/inference/platform API")
    monkeypatch.setattr(monthly, "run_candidate_repetition", forbidden)
    monkeypatch.setattr(monthly, "evaluate_candidate", forbidden)
    monkeypatch.setattr(monthly, "accumulate_observations", forbidden)
    monkeypatch.setattr(cal, "generate_records", forbidden)
    monkeypatch.setattr(monthly, "host_memory", forbidden)
    monkeypatch.setattr(monthly, "process_memory", forbidden)
    result = monthly.summarize(archive)
    assert result["decision"] == "PASS", result["decision_reasons"]
    assert sum(r["attempts"] for r in result["results"]) == 19400
    assert "not supplied" in result["verification"]["provenance_limitations"][0]


@pytest.mark.parametrize("change", ["missing", "truncated", "publishes", "stress_rejects"])
def test_mandatory_disclosures_and_actual_refusals(archive, change):
    fixture = monthly.SUPPLEMENTARY_MATRIX[0 if change != "stress_rejects" else 4]
    if change == "missing":
        (archive / "records" / f"{fixture.label}.json.gz").unlink()
    else:
        rows = _rows(archive, fixture)
        if change == "truncated": rows.pop()
        elif change == "publishes": rows[0] = _row(fixture)
        else: rows = [{**_row(fixture, reject=True), "id":20000+i} for i in range(fixture.attempts)]
        _save_rows(archive, fixture, rows)
    result = monthly.summarize(archive)
    assert result["decision"] == {"publishes":"FAIL", "stress_rejects":"PASS"}.get(change,"INCOMPLETE")
    assert result["integrity_problems"] == []


@pytest.mark.parametrize("change", ["empty", "partial", "fabricated", "ids", "duplicate", "truncated", "nan",
                                    "aggregate", "structural", "agrees", "scenario"])
def test_replay_evidence_cannot_be_replaced_by_a_truthy_flag(archive, change):
    run = json.loads((archive / "run.json").read_text())
    replay = run["replays"]["candidate_evidence_replay"]
    if change == "empty": run["replays"] = {}
    elif change == "partial": del run["replays"]["candle_disk_replay"]
    elif change == "fabricated": run["replays"] = {"anything":{"agrees":True}}
    elif change == "ids": replay["repetitions"] = [20000,20002]
    elif change == "duplicate": replay["comparisons"][1]["repetition"] = 20000
    elif change == "truncated": replay["comparisons"].pop()
    elif change == "nan": replay["max_absolute_difference"] = "nan"
    elif change == "aggregate": replay["max_absolute_difference"] = 1e-13
    elif change == "structural": del replay["comparisons"][0]["left_member_ids"]
    elif change == "agrees": replay["agrees"] = 1
    else: replay["scenario"] = "null_independent"
    _write(archive / "run.json",run)
    assert monthly.summarize(archive)["decision"] == "INCOMPLETE"


@pytest.mark.parametrize("change", ["running", "reference", "override", "duplicate", "method", "plan"])
def test_run_identity_and_terminal_state(archive, change):
    run = json.loads((archive / "run.json").read_text())
    if change == "running": run["status"] = "running"
    elif change == "reference": run["manifest_digest"] = "0"*64
    elif change == "override": run["attempts_override"] = 2000
    elif change == "duplicate": run["planned_fixtures"].append(run["planned_fixtures"][0])
    elif change == "method": run["method"] = "other"
    else: run["plan_version"] = 99
    _write(archive / "run.json", run)
    result = monthly.summarize(archive)
    assert result["decision"] == "INCOMPLETE"
    if change in ("running", "override"): assert result["integrity_problems"] == []


@pytest.mark.parametrize("change", ["empty", "reduced", "duplicate", "attempts", "required", "fixture", "seed", "alpha", "plan", "source"])
def test_rehashed_manifest_cannot_redefine_acceptance(archive, change):
    manifest = json.loads((archive / "manifest.json").read_text())
    main = manifest["matrix"]["main"]
    if change == "empty": main.clear()
    elif change == "reduced": main.pop()
    elif change == "duplicate": main.append(deepcopy(main[0]))
    elif change == "attempts": main[0]["attempts"] = 1
    elif change == "required": main[0]["required"] = False
    elif change == "fixture": main[0]["scenario_name"] = "null_independent"
    elif change == "seed": manifest["seeds"]["master_seed"] += 1
    elif change == "alpha": manifest["family"]["alpha"] = .2
    elif change == "plan": manifest["plan_version"] = 2
    else: manifest.pop("research_implementation")
    _rehash(archive, manifest)
    result = monthly.summarize(archive)
    assert result["decision"] == "INCOMPLETE" and result["integrity_problems"]


@pytest.mark.parametrize("change", ["id_bool", "duplicate_id", "member", "primary", "ragged", "p_flag", "p_range",
                                    "p_nan", "holm", "ci", "width", "family", "unavailable", "null_flag", "p_ci"])
def test_record_admission_rejects_contradictions(change):
    fixture = monthly.MAIN_MATRIX[0]
    row = _row(fixture)
    rows = [row]
    if change == "id_bool": row["id"] = True
    elif change == "duplicate_id": rows.append(deepcopy(row))
    elif change == "member": row["member_ids"][0] = "substituted"
    elif change == "primary": row["primary_member_id"] = row["member_ids"][0]
    elif change == "ragged": row["member_reject_raw"].pop()
    elif change == "p_flag": row["member_p_raw"][0] = 0.
    elif change == "p_range": row["member_p_raw"][0] = -1.
    elif change == "p_nan": row["member_p_raw"][0] = float("nan")
    elif change == "holm": row["member_p_holm"][0] = .9
    elif change == "ci": row["noncoverage"] = True
    elif change == "width": row["interval_width"] = .5
    elif change == "family": row["family_any_rejection"] = True
    elif change == "unavailable": row["member_available"][0] = False
    elif change == "p_ci":
        row.update(lift=1.5, interval_lower=1., interval_upper=2., interval_width=1., noncoverage=True)
    else: row["reject_raw"] = None
    ids, primary = monthly._fixture_family(fixture)
    with pytest.raises(ValueError):
        admission.validate_rows(rows, fixture=fixture, member_ids=ids, primary_id=primary, new_format=True)


@pytest.mark.parametrize("early", [False, True, "fabricated"])
def test_statistical_failure_and_proven_impossibility_can_precede_later_fixtures(archive, early):
    fixture = monthly.MAIN_MATRIX[0]
    count = 140 if early else fixture.attempts
    rows = [{**_row(fixture, reject=early != "fabricated"), "id":20000+i} for i in range(count)]
    _save_rows(archive,fixture,rows)
    (archive / "records" / f"{monthly.MAIN_MATRIX[1].label}.json.gz").unlink()
    if early:
        run = json.loads((archive / "run.json").read_text())
        proof = monthly.impossibility(fixture,[{**r,"reject_raw":True} for r in rows])
        run["stops"] = [{"fixture":fixture.label, **proof}]
        _write(archive / "run.json",run)
    result = monthly.summarize(archive)
    assert result["decision"] == ("INCOMPLETE" if early == "fabricated" else "FAIL")


def test_legacy_format_and_unrelated_fixture_extension(archive,monkeypatch):
    manifest = json.loads((archive / "manifest.json").read_text())
    manifest["schema_version"] = 1
    for key in ("plan_version","decision_policy_version","research_implementation"): manifest.pop(key)
    _rehash(archive,manifest)
    run = json.loads((archive / "run.json").read_text())
    run["schema_version"] = 1
    run.pop("plan_version")
    run.pop("decision_policy_version")
    for replay in run["replays"].values():
        for comparison in replay["comparisons"]:
            for key in list(comparison):
                if "member" in key: comparison.pop(key)
    _write(archive / "run.json",run)
    extra = replace(cal.SCENARIOS[0],scenario_id=999,name="future_fixture")
    monkeypatch.setattr(cal,"SCENARIOS",(*cal.SCENARIOS,extra))
    result = monthly.summarize(archive)
    assert result["decision"] == "PASS", result["decision_reasons"]
    assert len(result["verification"]["provenance_limitations"]) == 3


@pytest.mark.parametrize("change", ["valid", "malformed", "mismatch", "duplicate", "unknown", "missing", "ambiguous"])
def test_optional_checksums_and_plain_gzip_ambiguity(archive,change):
    entries = []
    for fixture in monthly.MAIN_MATRIX + monthly.SUPPLEMENTARY_MATRIX:
        raw = admission.record_bytes(archive,fixture.label)
        entries.append(f"{hashlib.sha256(raw).hexdigest()}  {fixture.label}.json")
    # Refusal variants legitimately have equal bytes/digests, but distinct identities.
    assert entries[8].split()[0] == entries[9].split()[0]
    if change == "malformed": entries[0] = "broken"
    elif change == "mismatch": entries[0] = "0"*64 + entries[0][64:]
    elif change == "duplicate": entries.append(entries[0])
    elif change == "unknown": entries.append("0"*64 + "  ../unsafe.json")
    elif change == "missing": entries.pop(0)
    elif change == "ambiguous":
        fixture = monthly.MAIN_MATRIX[0]
        (archive / "records" / f"{fixture.label}.json").write_bytes(admission.record_bytes(archive,fixture.label))
    (archive / "records.sha256").write_text("\n".join(entries)+"\n",encoding="utf-8")
    result = monthly.summarize(archive)
    assert result["decision"] == ("PASS" if change == "valid" else "INCOMPLETE")


def test_missing_interior_attempt_is_incomplete_not_corruption(archive):
    fixture = monthly.MAIN_MATRIX[0]
    rows = _rows(archive,fixture)
    rows.pop(30)
    _save_rows(archive,fixture,rows)
    result = monthly.summarize(archive)
    assert result["decision"] == "INCOMPLETE"
    assert result["integrity_problems"] == []


def test_proven_availability_impossibility(archive):
    fixture = monthly.MAIN_MATRIX[0]
    rows = [{**_row(fixture,available=False),"id":20000+i} for i in range(101)]
    _save_rows(archive,fixture,rows)
    run = json.loads((archive/'run.json').read_text())
    run['stops'] = [{"fixture":fixture.label, **monthly.impossibility(fixture,rows)}]
    _write(archive/'run.json',run)
    assert monthly.summarize(archive)['decision'] == 'FAIL'


@pytest.mark.parametrize('filename,payload', [('manifest.json','[]'),('manifest.json','{}'),
                                            ('run.json','{"status":'),('run.json','[]')])
def test_malformed_evidence_has_an_explicit_decision(archive,filename,payload):
    (archive/filename).write_text(payload,encoding='utf-8')
    result = monthly.summarize(archive)
    assert result['decision'] == 'INCOMPLETE'
    assert result['integrity_problems']


def test_cli_invalid_override_reports_incomplete_without_traceback(tmp_path,capsys):
    assert monthly.main(['--output-root',str(tmp_path/'run'),'--attempts','0']) == 2
    assert 'INCOMPLETE' in capsys.readouterr().err


def test_checksum_listed_missing_record_is_incomplete(archive):
    fixture = monthly.MAIN_MATRIX[0]
    entries = []
    for item in monthly.MAIN_MATRIX + monthly.SUPPLEMENTARY_MATRIX:
        raw = admission.record_bytes(archive,item.label)
        entries.append(f'{hashlib.sha256(raw).hexdigest()}  {item.label}.json')
    (archive/'records.sha256').write_text('\n'.join(entries)+'\n',encoding='utf-8')
    (archive/'records'/f'{fixture.label}.json.gz').unlink()
    result = monthly.summarize(archive)
    assert result['decision']=='INCOMPLETE'
    assert result['integrity_problems']==[]
