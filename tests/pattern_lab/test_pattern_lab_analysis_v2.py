"""Versioned ordinary analysis, shared arithmetic and sealed offline reporting."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest

from tools.pattern_lab import analysis, PatternLabDataError, PatternLabStudyError
from tools.pattern_lab.analysis import artifacts, estimator, runner, source
from tools.pattern_lab.analysis import calibration as cal
from tools.pattern_lab.analysis import calibration_monthly as research
from tools.pattern_lab.study.contracts import semantic_digest
from ._helpers import analysis_request_document, analysis_source_study


def request(version=2):
    value = analysis_request_document(schema_version=version)
    if version == 2:
        value.pop("seed")
        value.pop("resamples")
    return value


@pytest.mark.parametrize("version", [1, 2])
def test_all_public_request_forms_and_documents(version, tmp_path):
    document = request(version)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    normalized = analysis.load_analysis_request(document)
    for form in (document, path, normalized):
        item = analysis.load_analysis_request(form)
        assert item == normalized
        for rendered in (item.semantic_document(), item.request_document(), item.external_document()):
            assert rendered["schema_version"] == version
            assert ("seed" in rendered) == (version == 1)
            assert ("resamples" in rendered) == (version == 1)
    assert analysis.method_settings()["method"] == analysis.METHOD_ID
    assert analysis.METHOD_ID != analysis.V2_METHOD_ID


@pytest.mark.parametrize("version", [None, True, 3, "2"])
def test_version_precedes_unknown_keys(version):
    with pytest.raises(PatternLabDataError, match="schema_version"):
        analysis.load_analysis_request({**request(), "schema_version":version, "unknown":1})


@pytest.mark.parametrize("key", ["seed", "resamples", "block_length_days"])
def test_v2_rejects_legacy_options_before_output(key, tmp_path):
    root = tmp_path / "output"
    with pytest.raises(PatternLabDataError, match=key):
        analysis.run_analysis(request={**request(), key:1}, run_root=tmp_path / "absent", output_root=root)
    assert not root.exists()


@pytest.mark.parametrize("key", ["seed", "resamples"])
def test_contradictory_normalized_v2_cannot_render_or_load(key):
    value = replace(analysis.load_analysis_request(request()), **{key:1})
    for call in (value.external_document, value.request_document, value.semantic_document,
                 lambda: analysis.load_analysis_request(value)):
        with pytest.raises(PatternLabDataError, match="seed or resamples"):
            call()


def accumulated(days=365):
    scenario = replace(cal.SCENARIOS_BY_NAME["null_dependent_t5"], days=days)
    family = cal.scenario_family(scenario)
    records = cal.generate_records(scenario, 20000)
    return estimator.accumulate_observations(cal.record_frames(records, family), family=family,
        instruments=cal.INSTRUMENTS, study_start_ms=scenario.study_start_ms, study_end_ms=scenario.study_end_ms)


@pytest.mark.parametrize("days,groups", [(365,12), (396,13)])
def test_integrated_month_counts_and_candidate_parity(days, groups):
    acc = accumulated(days)
    result = estimator._evaluate_monthly_accumulated(acc)
    candidate = research.evaluate_candidate(acc)
    assert result["schema_version"] == 2
    assert not set(("seed", "resamples", "p_resolution", "p_resolution_blocks_first_rejection")) & result.keys()
    assert not {"block_length_days", "k_draw", "k_draw_note"} & result["calendar"].keys()
    assert any(r["inference_available"] for r in result["members"])
    for row, old in zip(result["members"], candidate["members"]):
        assert row["member_id"] == old["member_id"]
        assert row["p_raw"] == old["p_raw"] and row["p_holm"] == old["p_holm"]
        assert row["intervals"] == old["candidate"]["intervals"]
        assert row["inference_available"] == old["inference_available"]
        assert row["monthly_inference"]["informative_months"] == groups
        assert row["monthly_inference"]["degrees_of_freedom"] == groups-1
        assert row["monthly_inference"]["month_count_in_calibration"] == (groups == 12)
        assert row["monthly_inference"]["standard_error"] == old["candidate"]["standard_error"]
        assert not {"bootstrap", "p_upper", "p_lower", "degeneracy"} & row.keys()
        assert "k_draw" not in row["geometry"]
        assert not {"theta", "intervals", "p_lift"} & row["monthly_inference"].keys()


def test_no_retained_support_has_known_zero_months():
    result = estimator._evaluate_monthly_accumulated(accumulated(days=1))
    for member in result["members"]:
        info = member["monthly_inference"]
        assert not member["inference_available"]
        assert info["informative_months"] == 0
        assert info["degrees_of_freedom"] == 0
        assert info["month_count_in_calibration"] is False
        assert all(value is None for value in info["standard_error"].values())


def test_evaluation_order_preserves_compact_results_and_arrays():
    acc = accumulated()
    original = deepcopy(acc.results)
    arrays = {name: value.copy() for name,value in vars(acc).items() if isinstance(value,np.ndarray)}
    # B=1 is a permitted explicit low-level diagnostic: bootstrap SD is zero,
    # whereas the fixed monthly data remain nondegenerate. Public v1 bounds stay unchanged.
    bootstrap = lambda a: estimator._evaluate_accumulated(a,resamples=1,seed=123,batch_size=1)
    monthly = estimator._evaluate_monthly_accumulated
    b1,m1 = bootstrap(acc),monthly(acc)
    m2,b2 = monthly(acc),bootstrap(acc)
    fresh = accumulated()
    assert b1["members"] == b2["members"] == bootstrap(fresh)["members"]
    assert m1["members"] == m2["members"] == monthly(fresh)["members"]
    assert all("degenerate_contrast" in r["unavailable_reasons"] for r in b1["members"])
    assert all(r["inference_available"] for r in m1["members"])
    assert acc.results == original
    for name, value in arrays.items():
        np.testing.assert_array_equal(getattr(acc,name),value)
    m1["members"][0]["counts"]["records_supplied"] = -1
    assert acc.results == original


@pytest.fixture(scope="module")
def studies(tmp_path_factory):
    return {kind:analysis_source_study(tmp_path_factory.mktemp(kind),groups=groups)
            for kind,groups in (("short-v2",1600),("available-v2",19000))}


@pytest.fixture(scope="module")
def sealed_v2(tmp_path_factory, studies):
    output=tmp_path_factory.mktemp("sealed-v2")/"analysis"
    analysis.run_analysis(request=request(),run_root=studies["available-v2"][1],output_root=output)
    return output


def inventory(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


def test_real_v2_artifact_api_report_and_relocation(sealed_v2, studies, tmp_path, monkeypatch):
    import shutil
    result=analysis.load_analysis(sealed_v2)
    assert result.summary["schema_version"] == result.provenance["schema_version"] == result.status["schema_version"] == 2
    assert result.completion["analysis_schema_version"] == 2 and result.family["schema_version"] == 1
    assert any(r["inference_available"] for r in result.members)
    flat=result.comparisons()
    assert {"method","informative_months","degrees_of_freedom","standard_error_lift",
            "month_count_in_calibration","effect_sign","nominal_reject_raw"} <= set(flat.columns)
    assert not result.strata().empty and not result.daily().empty
    moved=tmp_path/"moved"
    shutil.copytree(sealed_v2,moved)
    before=inventory(moved)
    def forbidden(*a,**k):
        pytest.fail("offline regeneration reached source or inference")
    monkeypatch.setattr(source,"admit_source",forbidden)
    monkeypatch.setattr(source.RecordSource,"frames",forbidden)
    monkeypatch.setattr(runner,"evaluate_observations",forbidden)
    monkeypatch.setattr(runner,"evaluate_monthly_observations",forbidden)
    analysis.regenerate_report(moved)
    after=inventory(moved)
    assert {k:v for k,v in before.items() if k != artifacts.REPORT_FILE} == {k:v for k,v in after.items() if k != artifacts.REPORT_FILE}
    html=(moved/artifacts.REPORT_FILE).read_text(encoding="utf-8")
    assert "Monthly jackknife" in html and "Lift SE" in html
    assert "Bootstrap resamples" not in html and "2/(B+1)" not in html
    assert "Month count not covered by calibration" in html


def test_v1_v2_identities_differ_and_short_v2_honestly_refuses(studies,tmp_path):
    run=studies["short-v2"][1]
    before=inventory(run)
    results=[]
    for version in (1,2):
        output=tmp_path/str(version)
        analysis.run_analysis(request=request(version),run_root=run,output_root=output)
        saved=analysis.load_analysis(output)
        results.append(saved)
        assert all(not row["inference_available"] for row in saved.members)
        assert all(row["effect_sign"] is None for row in saved.members)
    assert inventory(run)==before
    assert results[0].completion["identities"]["analysis_semantic_sha256"] != results[1].completion["identities"]["analysis_semantic_sha256"]
    assert all("nominal_reject_raw" not in r and "monthly_inference" not in r for r in results[0].members)
    assert results[0].comparisons()["informative_months"].isna().all()


@pytest.mark.parametrize("filename,field,value", [
    ("status.json","schema_version",1),("provenance.json","schema_version",1),
    ("summary.json","schema_version",True),("request.json","schema_version",1),
    ("family.json","method",{"method":"calendar_score_cbb_v1"}),
    ("summary.json","method",{"method":"unknown"}),
])
def test_rehashed_version_or_method_contradiction_precedes_render(sealed_v2,tmp_path,filename,field,value):
    import shutil
    root=tmp_path/"corrupt"
    shutil.copytree(sealed_v2,root)
    path=root/filename
    document=json.loads(path.read_text())
    document[field]=value
    path.write_text(json.dumps(document),encoding="utf-8")
    completion=json.loads((root/artifacts.COMPLETION_FILE).read_text())
    completion["evidence_sha256"][filename]=hashlib.sha256(path.read_bytes()).hexdigest()
    completion["evidence_set_sha256"]=semantic_digest(completion["evidence_sha256"])
    (root/artifacts.COMPLETION_FILE).write_text(json.dumps(completion),encoding="utf-8")
    html=(root/artifacts.REPORT_FILE).read_bytes()
    with pytest.raises(PatternLabDataError):
        analysis.regenerate_report(root)
    assert (root/artifacts.REPORT_FILE).read_bytes()==html


@pytest.mark.parametrize("phase",["freeze","aggregate","publish"])
def test_v2_failure_keeps_version_and_honest_status(studies,tmp_path,monkeypatch,phase):
    def fail(*a,**k):
        raise OSError("injected v2 failure")
    if phase=="freeze":
        monkeypatch.setattr(artifacts,"write_status",fail)
    elif phase=="aggregate":
        monkeypatch.setattr(runner,"evaluate_monthly_observations",fail)
    else:
        monkeypatch.setattr(artifacts,"replace_report",fail)
    root=tmp_path/phase
    with pytest.raises(PatternLabStudyError,match="injected v2 failure"):
        analysis.run_analysis(request=request(),run_root=studies["short-v2"][1],output_root=root)
    assert not (root/artifacts.COMPLETION_FILE).exists()
    if phase!="freeze":
        status=json.loads((root/artifacts.STATUS_FILE).read_text())
        assert status["schema_version"]==2 and status["terminal_status"]=="failed"


def test_fresh_process_ordinary_execution_never_imports_research(studies,tmp_path):
    script=tmp_path/"isolated.py"
    script.write_text('''import importlib.abc, json, os, sys
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if 'calibration' in fullname:
            raise AssertionError('ordinary analysis imported research: '+fullname)
sys.meta_path.insert(0,Guard())
names=('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS')
before={n:os.environ.get(n) for n in names}
from tools.pattern_lab import analysis
analysis.run_analysis(request=json.loads(sys.argv[3]),run_root=sys.argv[1],output_root=sys.argv[2])
assert before=={n:os.environ.get(n) for n in names}
assert not any('calibration' in n for n in sys.modules)
''',encoding="utf-8")
    env=dict(os.environ,PYTHONPATH=str(Path.cwd()),OPENBLAS_NUM_THREADS="1",OMP_NUM_THREADS="2",
             MKL_NUM_THREADS="3",NUMEXPR_NUM_THREADS="4")
    done=subprocess.run([sys.executable,str(script),str(studies["short-v2"][1]),str(tmp_path/"isolated"),json.dumps(request())],
                        env=env,capture_output=True,text=True,timeout=60)
    assert done.returncode==0,done.stdout+done.stderr
