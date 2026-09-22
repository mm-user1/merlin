"""Historical seals use their recorded contracts, independently of launch policy."""
from copy import deepcopy
import hashlib
import json
import shutil

import pytest

from tools.pattern_lab import analysis, data, PatternLabDataError
from tools.pattern_lab.analysis import artifacts, estimator, monthly, request, runner, source
from tools.pattern_lab.study.contracts import semantic_digest
from ._helpers import analysis_request_document, analysis_source_study


@pytest.fixture(scope="module")
def sealed_versions(tmp_path_factory):
    base = tmp_path_factory.mktemp("historical-analysis")
    _, study, _ = analysis_source_study(base / "source", groups=1600)
    roots = {}
    for version in (1, 2):
        document = analysis_request_document(schema_version=version)
        if version == 2:
            document.pop("seed")
            document.pop("resamples")
        roots[version] = base / f"v{version}"
        analysis.run_analysis(request=document, run_root=study, output_root=roots[version])
    return roots


def _hashes(root):
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in (*artifacts.IMMUTABLE_FILES, artifacts.COMPLETION_FILE)}


def _copy(sealed_versions, version, tmp_path):
    return shutil.copytree(sealed_versions[version], tmp_path / "copy")


def _guard_execution(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("historical rendering reached source access or inference")
    monkeypatch.setattr(source, "admit_source", forbidden)
    monkeypatch.setattr(source.RecordSource, "frames", forbidden)
    monkeypatch.setattr(data, "read_session", forbidden)
    monkeypatch.setattr(data, "load_slice", forbidden)
    for module in (runner, estimator):
        for name in ("evaluate_observations", "evaluate_monthly_observations"):
            monkeypatch.setattr(module, name, forbidden)
    monkeypatch.setattr(estimator, "accumulate_observations", forbidden)
    monkeypatch.setattr(monthly, "monthly_jackknife", forbidden)


@pytest.mark.parametrize("version", [1, 2])
def test_historical_methods_survive_current_disclosure_change(
    sealed_versions, version, tmp_path, monkeypatch
):
    root = _copy(sealed_versions, version, tmp_path)
    before = _hashes(root)
    saved_method = analysis.load_analysis(root).request["method"]
    original = request.method_settings

    def changed(version=1):
        return {**original(version), "descriptive_note": "A later generation's disclosure."}

    monkeypatch.setattr(request, "method_settings", changed)
    _guard_execution(monkeypatch)
    assert analysis.load_analysis(root).request["method"] == saved_method
    analysis.regenerate_report(root)
    assert _hashes(root) == before
    assert "descriptive_note" not in analysis.load_analysis(root).summary["method"]


def test_historical_v1_bounds_are_separate_from_current_launch_policy(
    sealed_versions, tmp_path, monkeypatch
):
    root = _copy(sealed_versions, 1, tmp_path)
    before = _hashes(root)
    normalized = analysis.load_analysis_request(analysis_request_document())
    monkeypatch.setattr(request, "MIN_RESAMPLES", 5000)
    for form in (analysis_request_document(), normalized):
        with pytest.raises(PatternLabDataError, match="resamples"):
            analysis.load_analysis_request(form)
    _guard_execution(monkeypatch)
    assert analysis.load_analysis(root).request["resamples"] == 1999
    analysis.regenerate_report(root)
    assert _hashes(root) == before


def _write(root, filename, document):
    (root / filename).write_text(json.dumps(document), encoding="utf-8")


def _read(root, filename):
    return json.loads((root / filename).read_text(encoding="utf-8"))


def _reseal(root):
    """Update dependent saved identities too, so schema checks are decisive."""
    completion = _read(root, artifacts.COMPLETION_FILE)
    saved_request = _read(root, artifacts.REQUEST_FILE)
    family = _read(root, artifacts.FAMILY_FILE)
    binding = _read(root, artifacts.SOURCE_FILE)
    semantic_request = {k: v for k, v in saved_request.items()
                        if k not in ("analysis_name", "notes")}
    identities = completion["identities"]
    identities["request_sha256"] = semantic_digest(semantic_request)
    identities["family_sha256"] = semantic_digest(family)
    identities["analysis_semantic_sha256"] = artifacts.semantic_identity(
        artifact_version=completion["analysis_schema_version"],
        request_document=semantic_request, family=family, source=binding["semantic_inputs"],
    )
    provenance = _read(root, artifacts.PROVENANCE_FILE)
    provenance["identities"] = identities
    _write(root, artifacts.PROVENANCE_FILE, provenance)
    completion["evidence_sha256"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in artifacts.IMMUTABLE_FILES
    }
    completion["evidence_set_sha256"] = semantic_digest(completion["evidence_sha256"])
    _write(root, artifacts.COMPLETION_FILE, completion)
    artifacts.verify_completion(root)


@pytest.mark.parametrize("version", [1, 2])
def test_consistent_saved_disclosure_need_not_equal_current_metadata(
    sealed_versions, tmp_path, version, monkeypatch
):
    root = _copy(sealed_versions, version, tmp_path)
    for filename in (artifacts.REQUEST_FILE, artifacts.FAMILY_FILE, artifacts.SUMMARY_FILE):
        document = _read(root, filename)
        document["method"]["generation_note"] = "Saved by another compatible generation."
        _write(root, filename, document)
    _reseal(root)
    before = _hashes(root)
    _guard_execution(monkeypatch)
    assert "generation_note" in analysis.load_analysis(root).summary["method"]
    analysis.regenerate_report(root)
    assert _hashes(root) == before


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("wrong_id", ["other_supported", "unknown_method"])
def test_coherently_rehashed_wrong_method_id_still_fails(
    sealed_versions, tmp_path, version, wrong_id
):
    root = _copy(sealed_versions, version, tmp_path)
    replacement = (request.V2_METHOD_ID if version == 1 else request.METHOD_ID)
    if wrong_id == "unknown_method":
        replacement = wrong_id
    for filename in (artifacts.REQUEST_FILE, artifacts.FAMILY_FILE, artifacts.SUMMARY_FILE):
        document = _read(root, filename)
        document["method"]["method"] = replacement
        _write(root, filename, document)
    _reseal(root)
    prior_html = (root / artifacts.REPORT_FILE).read_bytes()
    for action in (analysis.load_analysis, analysis.regenerate_report):
        with pytest.raises(PatternLabDataError, match="method.*version"):
            action(root)
    assert (root / artifacts.REPORT_FILE).read_bytes() == prior_html


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("field,value", [
    ("analysis_name", " "), ("model_instances", []), ("model_instances", ["fh", "fh"]),
    ("model_instances", [""]), ("model_instances", "fh"), ("notes", 3),
    ("pairwise", None), ("pairwise", {}),
    ("pairwise", [{"id": "self", "target_variant": "a", "control_variant": "a"}]),
    ("pairwise", [{"id": "duplicate", "target_variant": "a", "control_variant": "b"}] * 2),
    ("method", None), ("method", []), ("method", {}), ("block_length_days", 7),
    ("schema_version", True), ("schema_version", 3),
])
def test_resealed_malformed_saved_request_fails_before_html(
    sealed_versions, tmp_path, version, field, value
):
    root = _copy(sealed_versions, version, tmp_path)
    document = _read(root, artifacts.REQUEST_FILE)
    document[field] = value
    _write(root, artifacts.REQUEST_FILE, document)
    _reseal(root)
    prior_html = (root / artifacts.REPORT_FILE).read_bytes()
    with pytest.raises(PatternLabDataError, match="saved request"):
        analysis.regenerate_report(root)
    assert (root / artifacts.REPORT_FILE).read_bytes() == prior_html


@pytest.mark.parametrize("version,field,value", [
    (1, "resamples", 1), (1, "resamples", 100000), (1, "resamples", True),
    (1, "seed", -1), (1, "seed", 2**32), (1, "seed", False),
    (2, "resamples", None), (2, "seed", None),
])
def test_original_saved_integer_domains_remain_enforced(
    sealed_versions, tmp_path, version, field, value
):
    root = _copy(sealed_versions, version, tmp_path)
    document = _read(root, artifacts.REQUEST_FILE)
    document[field] = value
    _write(root, artifacts.REQUEST_FILE, document)
    _reseal(root)
    prior_html = (root / artifacts.REPORT_FILE).read_bytes()
    with pytest.raises(PatternLabDataError, match=field):
        analysis.regenerate_report(root)
    assert (root / artifacts.REPORT_FILE).read_bytes() == prior_html


@pytest.mark.parametrize("field", ["analysis_name", "model_instances", "pairwise", "notes", "method"])
def test_saved_required_fields_have_no_reconstructed_defaults(sealed_versions, tmp_path, field):
    root = _copy(sealed_versions, 2, tmp_path)
    document = _read(root, artifacts.REQUEST_FILE)
    del document[field]
    _write(root, artifacts.REQUEST_FILE, document)
    _reseal(root)
    prior_html = (root / artifacts.REPORT_FILE).read_bytes()
    with pytest.raises(PatternLabDataError, match=field):
        analysis.regenerate_report(root)
    assert (root / artifacts.REPORT_FILE).read_bytes() == prior_html


@pytest.mark.parametrize("groups", [12, 13, 0, None])
def test_width_reference_is_explicit_with_actual_and_refused_geometry(sealed_versions, groups):
    # A presentation fixture only; no inference is recomputed or claimed for it.
    summary = deepcopy(analysis.load_analysis(sealed_versions[2]).summary)
    for member in summary["members"]:
        info = member["monthly_inference"]
        info["informative_months"] = groups
        info["degrees_of_freedom"] = None if groups is None else max(groups - 1, 0)
        info["month_count_in_calibration"] = None if groups is None else groups == 12
        assert not member["inference_available"]
        assert member["intervals"]["lift"] is None
    html = analysis.render_report(summary)
    assert "Calibration reference example only: at G=12 (df=11)" in html
    assert "Actual G and df are listed for each member below" in html
    assert "not a power-based minimum detectable effect" in html
    first = summary["members"][0]
    g = "—" if groups is None else str(groups)
    df = "—" if groups is None else str(max(groups - 1, 0))
    assert f'{first["member_id"]}</td><td>{g}</td><td>{df}</td><td>—</td>' in html
    if groups != 12:
        assert ("Unknown G" if groups is None else "Month count not covered by calibration") in html


def test_declared_monthly_validity_matches_enforced_values():
    from tools.pattern_lab.analysis import calibration_monthly as research
    declared = request.method_settings(2)["mathematical_validity"]
    assert declared["min_informative_months"] == monthly.MIN_INFORMATIVE_MONTHS == 2
    assert declared["degeneracy_multiplier"] == monthly.DEGENERACY_MULTIPLIER == 128
    assert research.MIN_INFORMATIVE_MONTHS == monthly.MIN_INFORMATIVE_MONTHS
    assert research.DEGENERACY_MULTIPLIER == monthly.DEGENERACY_MULTIPLIER
    for groups in (1, 2):
        result = monthly.monthly_jackknife(
            month_index=list(range(groups)), target_count=[40.] * groups,
            control_count=[80.] * groups, target_net_sum=[1., 3.][:groups],
            control_net_sum=[0.] * groups,
        )
        assert result["available"] == (groups >= declared["min_informative_months"])
        if result["available"]:
            assert result["degeneracy"]["multiplier"] == declared["degeneracy_multiplier"]
