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


def test_plan_entry_summary_names_and_full_failure_labels(archive):
    main, refusal = monthly.MAIN_MATRIX[0], monthly.SUPPLEMENTARY_MATRIX[0]
    missing, partial = monthly.SUPPLEMENTARY_MATRIX[-2:]
    _save_rows(archive, main, [{**_row(main, reject=True), "id":20000+i} for i in range(main.attempts)])
    rows = _rows(archive, refusal)
    rows[0] = _row(refusal)
    _save_rows(archive, refusal, rows)
    (archive / "records" / f"{missing.label}.json.gz").unlink()
    _save_rows(archive, partial, _rows(archive, partial)[:-1])
    result = monthly.summarize(archive)
    assert result["decision"] == "FAIL"
    assert result["required_plan_entries"] == [f.label for f in monthly.MAIN_MATRIX + monthly.SUPPLEMENTARY_MATRIX]
    assert result["required_main_fixtures"] == [f.label for f in monthly.MAIN_MATRIX]
    assert result["missing_plan_entries"] == [missing.label]
    assert result["incomplete_plan_entries"] == [partial.label]
    assert result["failing_plan_entries"] == [main.label, refusal.label]
    assert not any(k in result for k in ("required_fixtures", "missing_required_fixtures",
                                       "incomplete_required_fixtures", "failing_required_fixtures"))
    assert sum(r["attempts"] for r in result["results"]) == 19400 - missing.attempts - 1
    rendered = monthly.render_summary(result)
    assert f"failing_plan_entries: {main.label}, {refusal.label}" in rendered


@pytest.mark.parametrize("case", ["proved_failure", "resource_only", "invalid", "override", "impossibility"])
def test_coordinator_budget_stop_preserves_only_admitted_failure(case, tmp_path, monkeypatch):
    root = tmp_path / "run"
    fixture = monthly.MAIN_MATRIX[0]
    def stop(phase):
        return monthly.BudgetStop({"kind":"wall_clock", "phase":phase, "message":"injected later budget stop"})
    def sample(self, phase, **kwargs):
        if phase == "before_replays":
            raise stop(phase)
        return {"peak_rss_bytes":100}
    def produce(selected, *, attempts, **kwargs):
        if selected != fixture:
            raise stop("later_fixture")
        count = 140 if case == "impossibility" else attempts
        rows = [{**_row(fixture, reject=case != "resource_only"), "id":20000+i} for i in range(count)]
        if case == "invalid":
            rows[0]["member_p_raw"][0] = .5  # contradicts the rejection vector
        proof = monthly.impossibility(fixture, rows) if case == "impossibility" else None
        return rows, proof
    monkeypatch.setattr(monthly.Budget, "sample", sample)
    monkeypatch.setattr(monthly, "_run_pilot", lambda *a, **k: ({"projected_total_seconds":0}, {}))
    monkeypatch.setattr(monthly, "run_fixture", produce)
    monkeypatch.setattr(monthly, "run_candidate_repetition", lambda *a: pytest.fail("unexpected generation"))
    run = monthly.run_experiment(output_root=root, attempts=2 if case == "override" else None)
    assert run["status"] == "incomplete"
    result = monthly.summarize(root)
    assert result["decision"] == ("FAIL" if case in ("proved_failure", "impossibility") else "INCOMPLETE")
    assert bool(result["integrity_problems"]) == (case == "invalid")
    assert result["missing_plan_entries"]
    if case == "impossibility":
        assert result["failing_plan_entries"] == []  # proof remains in stops/reasons
        assert "cannot pass" in " ".join(result["decision_reasons"])


def test_refusal_violation_survives_later_budget_stop(archive):
    fixture = monthly.SUPPLEMENTARY_MATRIX[0]
    _save_rows(archive, fixture, [_row(fixture)])
    run = json.loads((archive / "run.json").read_text())
    run.update(status="incomplete", incomplete_reason="later budget stop", replays=None,
               stops=[{"kind":"wall_clock", "message":"later budget stop"}])
    _write(archive / "run.json", run)
    result = monthly.summarize(archive)
    assert result["decision"] == "FAIL"
    assert result["failing_plan_entries"] == [fixture.label]


def _change_live_scenario(monkeypatch, **changes):
    original = cal.SCENARIOS_BY_NAME["null_dependent_t5"]
    changed = replace(original, **changes)
    monkeypatch.setattr(cal, "SCENARIOS", tuple(changed if s.name == original.name else s for s in cal.SCENARIOS))
    monkeypatch.setitem(cal.SCENARIOS_BY_NAME, original.name, changed)


@pytest.mark.parametrize("mutation", ["parameter", "lookup_only", "global", "family", "candle"])
def test_changed_live_semantics_cannot_generate_plan1(mutation, monkeypatch, tmp_path):
    if mutation == "parameter":
        _change_live_scenario(monkeypatch, daily_ar=.123)
    elif mutation == "lookup_only":
        s = cal.SCENARIOS_BY_NAME["null_dependent_t5"]
        monkeypatch.setitem(cal.SCENARIOS_BY_NAME, s.name, replace(s, daily_ar=.123))
    elif mutation == "global":
        monkeypatch.setattr(cal, "COMMON_LOADING", .123)
    elif mutation == "family":
        original = cal.scenario_family
        monkeypatch.setattr(cal, "scenario_family", lambda s: original(s)[:-1])
    else:
        original = monthly.candle_generator_contract
        monkeypatch.setattr(monthly, "candle_generator_contract", lambda: {**original(), "return_scale":.123})
    monkeypatch.setattr(monthly, "run_candidate_repetition", lambda *a: pytest.fail("unexpected generation"))
    monkeypatch.setattr(monthly, "_run_pilot", lambda *a, **k: pytest.fail("unexpected pilot"))
    root = tmp_path / "run"
    with pytest.raises(monthly.PatternLabDataError, match="plan 1"):
        monthly.run_experiment(output_root=root, attempts=1)
    assert not root.exists()


def test_rehashed_required_semantic_mutation_is_rejected(archive):
    manifest = json.loads((archive / "manifest.json").read_text())
    contract = manifest["generators"]["record_contract"]
    next(s for s in contract["scenarios"] if s["name"] == "null_dependent_t5")["daily_ar"] = .123
    contract["generator_digest"] = semantic_digest({k:v for k,v in contract.items() if k != "generator_digest"})
    _rehash(archive, manifest)
    result = monthly.summarize(archive)
    assert result["decision"] == "INCOMPLETE"
    assert any("null_dependent_t5" in problem for problem in result["integrity_problems"])


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("change", ["parameter_and_family", "global", "prose_and_extra"])
def test_saved_plan_is_independent_of_live_semantics(archive, monkeypatch, change, version):
    if version == 1:
        manifest = json.loads((archive / "manifest.json").read_text())
        manifest["schema_version"] = 1
        for key in ("plan_version", "decision_policy_version", "research_implementation"):
            manifest.pop(key)
        _rehash(archive, manifest)
        run = json.loads((archive / "run.json").read_text())
        run["schema_version"] = 1
        _write(archive / "run.json", run)
    if change == "parameter_and_family":
        _change_live_scenario(monkeypatch, daily_ar=.123, comparison="parent_child", planted_effect=99.)
        monkeypatch.setattr(cal, "scenario_family", lambda *a: pytest.fail("offline live family lookup"))
        monkeypatch.setattr(monthly, "candle_family", lambda *a: pytest.fail("offline live candle family"))
    elif change == "global":
        monkeypatch.setattr(cal, "COMMON_LOADING", .123)
        monkeypatch.setattr(cal, "HORIZON_MINUTES", (60,))
    else:
        _change_live_scenario(monkeypatch, note="new explanatory prose")
        extra = replace(cal.SCENARIOS[0], scenario_id=999, name="future_fixture")
        monkeypatch.setattr(cal, "SCENARIOS", (*cal.SCENARIOS, extra))
    result = monthly.summarize(archive)
    assert result["decision"] == "PASS", result["decision_reasons"]
    assert len(result["required_plan_entries"]) == 17
    assert sum(r["attempts"] for r in result["results"]) == 19400
    assert result["results"][0]["true_lift"] == 0.


def test_prose_and_unrelated_registry_growth_keep_fresh_plan_eligible(monkeypatch):
    _change_live_scenario(monkeypatch, note="clarified prose", caveat="clarified caveat")
    extra = replace(cal.SCENARIOS[0], scenario_id=999, name="future_fixture")
    monkeypatch.setattr(cal, "SCENARIOS", (*cal.SCENARIOS, extra))
    manifest = monthly.candidate_manifest()
    monthly._require_plan1_generation(manifest, list(monthly.MAIN_MATRIX))
    assert admission.manifest_problems(manifest, monthly.RESEARCH_MODULES) == []


@pytest.mark.parametrize("payload", [b'\x1f\x8b\x08\x00' + b'\x00'*6 + b'\x07' + b'\x00'*8,
                                  gzip.compress(b'{"rows":[]}')[:-4], b'not gzip'])
def test_corrupt_compressed_record_through_offline_cli(archive, payload, capsys):
    fixture = monthly.MAIN_MATRIX[0]
    path = archive / "records" / f"{fixture.label}.json.gz"
    path.write_bytes(payload)
    assert monthly.main(["--output-root", str(archive), "--summarize-only"]) == 2
    result = json.loads((archive / "summary.json").read_text())
    assert result["decision"] == "INCOMPLETE"
    assert any("invalid compressed record" in p and str(path) in p for p in result["integrity_problems"])
    assert '"decision": "INCOMPLETE"' in capsys.readouterr().out


@pytest.mark.parametrize("change", ["envelope", "floor", "rate_set", "demote", "attempts", "impossibility", "unknown", "max_main"])
def test_consumed_verifier_drift_is_integrity_before_scoring(archive, monkeypatch, capsys, change):
    if change == "envelope":
        monkeypatch.setattr(cal, "ERROR_ENVELOPE", .055)
    elif change == "floor":
        monkeypatch.setattr(cal, "MIN_PRIMARY_AVAILABILITY", .99)
    elif change == "rate_set":
        monkeypatch.setattr(cal, "ACCEPTANCE_RATES", cal.ACCEPTANCE_RATES[:-1])
    elif change == "impossibility":
        monkeypatch.setattr(monthly, "IMPOSSIBLE_ERRORS", 139)
    elif change == "max_main":
        monkeypatch.setattr(monthly, "MAX_MAIN_ATTEMPTS", 1)
    else:
        fixture = monthly.MAIN_MATRIX[0]
        fields = {"demote":{"required":False}, "attempts":{"attempts":1999},
                  "unknown":{"scenario_name":"unsupported", "name":"unsupported"}}[change]
        monkeypatch.setattr(monthly, "MAIN_MATRIX", (replace(fixture, **fields), *monthly.MAIN_MATRIX[1:]))
    def forbidden(*a, **k):
        pytest.fail("altered verifier reached scoring or stop proof")
    monkeypatch.setattr(monthly, "score_fixture", forbidden)
    monkeypatch.setattr(monthly, "impossibility", forbidden)
    monkeypatch.setattr(monthly, "candidate_manifest", forbidden)
    assert monthly.main(["--output-root", str(archive), "--summarize-only"]) == 2
    result = json.loads((archive / "summary.json").read_text())
    assert result["decision"] == "INCOMPLETE" and result["results"] == []
    assert any("verifier" in p for p in result["integrity_problems"])
    assert '"decision": "INCOMPLETE"' in capsys.readouterr().out


@pytest.mark.parametrize("function", [admission.plan_truth, admission.plan_family])
def test_unknown_frozen_scenario_is_explicit(function):
    with pytest.raises(ValueError, match="unsupported.*not_registered"):
        function("not_registered")
    assert function(None) is not None


def test_unknown_scenario_at_record_boundary_is_structured(archive, monkeypatch):
    monkeypatch.setattr(monthly, "_fixture_family", lambda f: admission.plan_family("not_registered"))
    assert monthly.main(["--output-root", str(archive), "--summarize-only"]) == 2
    result=json.loads((archive/"summary.json").read_text())
    assert result["decision"] == "INCOMPLETE"
    assert all("not_registered" in p for p in result["integrity_problems"])


@pytest.mark.parametrize("revision", [1, 2])
def test_research_source_revision_preserves_old_scope_without_weakening_new(archive, revision):
    manifest=json.loads((archive/"manifest.json").read_text())
    if revision==1:
        manifest.pop("research_attribution_revision")
    manifest["research_implementation"].pop(admission.SHARED_MONTHLY_MODULE)
    _rehash(archive,manifest)
    result=monthly.summarize(archive)
    assert result["decision"] == ("PASS" if revision==1 else "INCOMPLETE")
    if revision==1:
        assert any("five-source scope" in s for s in result["verification"]["provenance_limitations"])
    else:
        assert any("research source hashes" in s for s in result["integrity_problems"])
