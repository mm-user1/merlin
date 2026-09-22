"""The M3a request, family and source-admission contracts.

Every accepted public request form reaches one strict normalization path, the
resolved family never depends on list order, and a completed source study is
admitted only through its whole contract: the model triple, the built-in
settings and case resolution, the evidence view version and the saved study
bounds against the study's own development protocol.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import analysis as pack_analysis
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.analysis import family as analysis_family
from tools.pattern_lab.analysis import request as analysis_request
from tools.pattern_lab.analysis import source as analysis_source
from tools.pattern_lab.study import evidence as study_evidence

from ._helpers import (
    analysis_request_document,
    analysis_source_study,
    fixed_horizon_model,
)


# --------------------------------------------------------------------------
# the versioned request
# --------------------------------------------------------------------------

def test_a_path_and_a_mapping_normalize_identically(tmp_path):
    document = analysis_request_document()
    path = tmp_path / "analysis.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    from_mapping = analysis_request.load_analysis_request(document)
    from_path = analysis_request.load_analysis_request(path)
    assert from_mapping.semantic_document() == from_path.semantic_document()
    # A normalized object is rendered back into the external schema and
    # revalidated; there is no trusted normalized-object bypass.
    again = analysis_request.load_analysis_request(from_path)
    assert again.semantic_document() == from_path.semantic_document()


def test_the_frozen_method_settings_are_part_of_the_semantic_request():
    normalized = analysis_request.load_analysis_request(analysis_request_document())
    method = normalized.semantic_document()["method"]
    assert method["method"] == "calendar_score_cbb_v1"
    assert method["matching"] == "instrument_utc_month_v1"
    assert method["block_length_days"] == 7
    assert method["alpha"] == 0.05
    assert method["confidence_level"] == 0.95
    assert method["support"]["min_supported_blocks"] == 48


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"schema_version": 3}, "unsupported analysis request version"),
        ({"schema_version": True}, "expected an integer"),
        ({"analysis_name": "   "}, "nonblank string"),
        ({"model_instances": []}, "nonempty list"),
        ({"model_instances": ["fh", "fh"]}, "duplicate model instance"),
        ({"resamples": 1998}, "must be >= 1999"),
        ({"resamples": 100000}, "must be <= 99999"),
        ({"resamples": True}, "expected an integer"),
        ({"seed": -1}, "must be >= 0"),
        ({"seed": 2**32}, "must be <= 4294967295"),
        ({"seed": True}, "expected an integer"),
        ({"notes": 3}, "expected a string or null"),
    ],
)
def test_a_malformed_request_field_names_itself(overrides, message):
    with pytest.raises(PatternLabDataError, match=message):
        analysis_request.load_analysis_request(analysis_request_document(**overrides))


def test_unknown_keys_are_rejected_at_every_level():
    with pytest.raises(PatternLabDataError, match="unknown keys"):
        analysis_request.load_analysis_request(analysis_request_document(extra=1))
    document = analysis_request_document()
    document["pairwise"][0]["method"] = "other"
    with pytest.raises(PatternLabDataError, match="unknown keys"):
        analysis_request.load_analysis_request(document)


def test_duplicate_json_keys_are_rejected_by_the_strict_reader(tmp_path):
    path = tmp_path / "analysis.json"
    path.write_text(
        '{"schema_version": 1, "schema_version": 1, "analysis_name": "x", '
        '"model_instances": ["fh"], "resamples": 1999, "seed": 1}',
        encoding="utf-8",
    )
    with pytest.raises(PatternLabDataError, match="duplicate"):
        analysis_request.load_analysis_request(path)


def test_a_self_comparison_and_a_duplicate_semantic_pair_are_rejected():
    document = analysis_request_document()
    document["pairwise"][0]["control_variant"] = "two_green_volume"
    with pytest.raises(PatternLabDataError, match="with itself"):
        analysis_request.load_analysis_request(document)
    document = analysis_request_document()
    document["pairwise"].append(dict(document["pairwise"][0], id="again"))
    with pytest.raises(PatternLabDataError, match="same semantic pair"):
        analysis_request.load_analysis_request(document)
    document = analysis_request_document()
    document["pairwise"].append(dict(document["pairwise"][0]))
    with pytest.raises(PatternLabDataError, match="duplicate comparison ID"):
        analysis_request.load_analysis_request(document)


def test_the_generated_baseline_prefix_is_reserved_against_user_ids():
    document = analysis_request_document()
    document["pairwise"][0]["id"] = "baseline__mine"
    with pytest.raises(PatternLabDataError, match="reserved"):
        analysis_request.load_analysis_request(document)


# --------------------------------------------------------------------------
# the resolved family
# --------------------------------------------------------------------------

def _variants():
    return [
        {"variant_id": "two_green_plain", "hypothesis_id": "two_green",
         "occurrence": "every_qualifying_bar", "condition_id": "c2"},
        {"variant_id": "two_green_volume", "hypothesis_id": "two_green_rising_quote_volume",
         "occurrence": "every_qualifying_bar", "condition_id": "c1"},
    ]


def _instances():
    settings = fixed_horizon_model(30, [60, 120], primary=120)["settings"]
    from tools.pattern_lab.study import builtins as study_builtins

    resolved = study_builtins.validate_fixed_horizon_settings(settings, [30])
    cases = [case.as_json() for case in study_builtins.resolve_fixed_horizon_cases(resolved, 30)]
    return {
        "fh": {
            "model_instance_id": "fh",
            "model_id": "fixed_horizon_path",
            "model_version": "1",
            "evidence_kind": "fixed_horizon_path_v1",
            "settings": resolved,
            "cases": {"30": cases},
        }
    }


def test_every_saved_variant_gets_a_generated_baseline_then_the_declared_pairs():
    normalized = analysis_request.load_analysis_request(analysis_request_document())
    comparisons, members = analysis_family.resolve_family(
        normalized, variants=_variants(), instances=_instances(), timeframes=[30]
    )
    assert [item.comparison_id for item in comparisons] == [
        "baseline__two_green_plain",
        "baseline__two_green_volume",
        "volume_filter",
    ]
    assert len(members) == 3 * 4
    assert members[0].member_id == "baseline__two_green_plain|fh|tf30m|tf30m.h60m.long"


def test_the_canonical_order_does_not_depend_on_the_request_list_order():
    document = analysis_request_document()
    document["pairwise"] = [
        {"id": "zzz", "target_variant": "two_green_plain", "control_variant": "two_green_volume"},
        {"id": "aaa", "target_variant": "two_green_volume", "control_variant": "two_green_plain"},
    ]
    reversed_document = analysis_request_document()
    reversed_document["pairwise"] = list(reversed(document["pairwise"]))
    first = analysis_family.resolve_family(
        analysis_request.load_analysis_request(document),
        variants=list(reversed(_variants())), instances=_instances(), timeframes=[30],
    )[1]
    second = analysis_family.resolve_family(
        analysis_request.load_analysis_request(reversed_document),
        variants=_variants(), instances=_instances(), timeframes=[30],
    )[1]
    assert [item.member_id for item in first] == [item.member_id for item in second]


def test_a_pairwise_comparison_that_names_an_unsaved_variant_fails():
    document = analysis_request_document()
    document["pairwise"][0]["control_variant"] = "missing"
    normalized = analysis_request.load_analysis_request(document)
    with pytest.raises(PatternLabDataError, match="never saved"):
        analysis_request.require_known_variants(
            normalized, ["two_green_volume"], where="admission"
        )


# --------------------------------------------------------------------------
# source admission
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def source_study(tmp_path_factory):
    root = tmp_path_factory.mktemp("analysis-source")
    return analysis_source_study(root)


def test_a_completed_study_is_admitted_with_its_whole_model_contract(source_study):
    _pack, run_root, _request = source_study
    admitted = analysis_source.admit_source(run_root, model_instances=["fh"])
    assert admitted.instruments == ("TEST_AAA-USDT-SWAP", "TEST_BBB-USDT-SWAP")
    assert admitted.timeframes == (30,)
    binding = admitted.binding_document()
    assert binding["semantic"]["evidence_view_version"] == 1
    assert binding["physical"]["evidence_set_sha256"]
    assert "not integrity proofs" in binding["note"]


def test_a_results_object_does_not_bypass_strict_admission(source_study):
    _pack, run_root, _request = source_study
    results = pack_study.load_results(run_root)
    with pytest.raises(PatternLabDataError, match="does not bypass strict admission"):
        analysis_source.admit_source(results, model_instances=["fh"])


def test_a_partial_or_missing_completion_record_is_refused(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "copied"
    _copy_tree(run_root, copy)
    (copy / study_evidence.COMPLETION_FILE).unlink()
    with pytest.raises(PatternLabDataError) as error:
        analysis_source.admit_source(copy, model_instances=["fh"])
    assert error.value.error_code == "incomplete_run"


def test_corrupt_source_evidence_is_refused_before_any_output(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "corrupt"
    _copy_tree(run_root, copy)
    target = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "conditions.parquet"
    target.write_bytes(target.read_bytes() + b"\x00")
    with pytest.raises(PatternLabDataError) as error:
        analysis_source.admit_source(copy, model_instances=["fh"])
    assert error.value.error_code == "corrupt_evidence"


def test_an_unknown_model_instance_names_the_saved_instances(source_study):
    _pack, run_root, _request = source_study
    with pytest.raises(PatternLabDataError, match="not saved by this study"):
        analysis_source.admit_source(run_root, model_instances=["other"])


def test_an_extension_model_claiming_the_builtin_evidence_kind_is_rejected(tmp_path, source_study):
    """Evidence kind alone is not enough: extensions may declare the same kind."""
    _pack, run_root, _request = source_study
    copy = tmp_path / "lookalike"
    _copy_tree(run_root, copy)
    family = json.loads((copy / study_evidence.FAMILY_FILE).read_text(encoding="utf-8"))
    family["models"][0]["model_id"] = "custom_lookalike"
    (copy / study_evidence.FAMILY_FILE).write_text(json.dumps(family), encoding="utf-8")
    _reseal(copy)
    with pytest.raises(PatternLabDataError, match="supports only the built-in"):
        analysis_source.admit_source(copy, model_instances=["fh"])


def test_an_inconsistent_saved_case_list_is_rejected(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "cases"
    _copy_tree(run_root, copy)
    family = json.loads((copy / study_evidence.FAMILY_FILE).read_text(encoding="utf-8"))
    family["models"][0]["cases"]["30"][0]["parameters"]["horizon_minutes"] = 90
    (copy / study_evidence.FAMILY_FILE).write_text(json.dumps(family), encoding="utf-8")
    _reseal(copy)
    with pytest.raises(PatternLabDataError, match="do not match the built-in resolver"):
        analysis_source.admit_source(copy, model_instances=["fh"])


def test_inconsistent_saved_settings_are_rejected(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "settings"
    _copy_tree(run_root, copy)
    family = json.loads((copy / study_evidence.FAMILY_FILE).read_text(encoding="utf-8"))
    family["models"][0]["settings"]["directions"] = ["short", "long"]
    (copy / study_evidence.FAMILY_FILE).write_text(json.dumps(family), encoding="utf-8")
    _reseal(copy)
    with pytest.raises(PatternLabDataError, match="do not match the built-in resolver|validator"):
        analysis_source.admit_source(copy, model_instances=["fh"])


def test_an_unsupported_evidence_view_version_is_rejected(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "view"
    _copy_tree(run_root, copy)
    source = json.loads((copy / study_evidence.SOURCE_FILE).read_text(encoding="utf-8"))
    source["evidence_view_version"] = 2
    (copy / study_evidence.SOURCE_FILE).write_text(json.dumps(source), encoding="utf-8")
    _reseal(copy)
    with pytest.raises(PatternLabDataError, match="evidence view version"):
        analysis_source.admit_source(copy, model_instances=["fh"])


def test_the_saved_study_bounds_are_checked_against_the_saved_protocol(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "protocol"
    _copy_tree(run_root, copy)
    protocol = json.loads((copy / study_evidence.PROTOCOL_FILE).read_text(encoding="utf-8"))
    protocol["reserved"] = dict(protocol["development"])
    (copy / study_evidence.PROTOCOL_FILE).write_text(json.dumps(protocol), encoding="utf-8")
    _reseal(copy)
    with pytest.raises(PatternLabDataError, match="reserved interval"):
        analysis_source.admit_source(copy, model_instances=["fh"])


# --------------------------------------------------------------------------
# the checked alignment
# --------------------------------------------------------------------------

def _members(admitted, document=None):
    normalized = analysis_request.load_analysis_request(document or analysis_request_document())
    return analysis_family.resolve_family(
        normalized,
        variants=admitted.variants,
        instances=admitted.instances,
        timeframes=admitted.timeframes,
    )[1]


def test_the_alignment_uses_the_all_anchor_view_and_counts_eligible_anchors(source_study):
    _pack, run_root, _request = source_study
    admitted = analysis_source.admit_source(run_root, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    frames = list(records.frames())
    assert frames
    anchors = records.eligible_anchors["30"]
    assert anchors > 0
    baseline = [
        frame for frame in frames
        if frame["member_id"].iloc[0].startswith("baseline__two_green_volume")
    ]
    # A baseline's records are the condition-valid anchors of both instruments.
    assert sum(len(frame) for frame in baseline) <= anchors * 4
    assert set(frames[0].columns) == set(pack_analysis.RECORD_COLUMNS)


def test_a_duplicated_saved_condition_row_fails_rather_than_being_merged(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "duplicate"
    _copy_tree(run_root, copy)
    path = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "conditions.parquet"
    frame = study_evidence.read_table(path)
    doubled = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    study_evidence.write_table(path, doubled, name="conditions")
    _reseal(copy)
    admitted = analysis_source.admit_source(copy, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    with pytest.raises(PatternLabDataError, match="duplicate anchors"):
        list(records.frames())


def test_a_missing_saved_condition_row_fails_rather_than_being_inner_joined(
    tmp_path, source_study
):
    _pack, run_root, _request = source_study
    copy = tmp_path / "missing"
    _copy_tree(run_root, copy)
    path = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "conditions.parquet"
    frame = study_evidence.read_table(path)
    condition = frame["condition_id"].iloc[0]
    dropped = frame.drop(
        index=frame.index[frame["condition_id"] == condition][0]
    ).reset_index(drop=True)
    study_evidence.write_table(path, dropped, name="conditions")
    _reseal(copy)
    admitted = analysis_source.admit_source(copy, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    with pytest.raises(PatternLabDataError, match="different anchor set|checked one-to-one"):
        list(records.frames())


def test_an_emission_without_a_known_true_condition_fails(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "emission"
    _copy_tree(run_root, copy)
    path = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "conditions.parquet"
    frame = study_evidence.read_table(path)
    first = frame.index[frame["value"].to_numpy() & frame["valid"].to_numpy()][0]
    frame.loc[first, "valid"] = False
    study_evidence.write_table(path, frame, name="conditions")
    _reseal(copy)
    admitted = analysis_source.admit_source(copy, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    with pytest.raises(PatternLabDataError, match="not a known-valid true condition"):
        list(records.frames())


def test_an_emission_anchor_outside_the_saved_anchors_fails(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "anchor"
    _copy_tree(run_root, copy)
    path = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "emissions.parquet"
    frame = study_evidence.read_table(path)
    frame.loc[frame.index[0], "anchor_open_ms"] = int(frame["anchor_open_ms"].max()) + 7
    study_evidence.write_table(path, frame, name="emissions")
    _reseal(copy)
    admitted = analysis_source.admit_source(copy, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    with pytest.raises(PatternLabDataError, match="not a saved eligible anchor"):
        list(records.frames())


def test_emission_signal_times_are_checked_against_their_own_anchor(tmp_path, source_study):
    """Permuting signal times across two anchors is a real defect, not a reordering."""
    _pack, run_root, _request = source_study
    copy = tmp_path / "permuted"
    _copy_tree(run_root, copy)
    path = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "emissions.parquet"
    frame = study_evidence.read_table(path)
    variant = frame["variant_id"].iloc[0]
    first, second = frame.index[frame["variant_id"] == variant][:2]
    assert frame.loc[first, "anchor_open_ms"] != frame.loc[second, "anchor_open_ms"]
    frame.loc[[first, second], "signal_time_ms"] = frame.loc[
        [second, first], "signal_time_ms"
    ].to_numpy()
    study_evidence.write_table(path, frame, name="emissions")
    _reseal(copy)
    admitted = analysis_source.admit_source(copy, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    with pytest.raises(PatternLabDataError, match="emission signal time that is not"):
        list(records.frames())


def test_a_reordered_but_intact_emission_table_is_still_admitted(tmp_path, source_study):
    _pack, run_root, _request = source_study
    copy = tmp_path / "reordered"
    _copy_tree(run_root, copy)
    path = copy / "jobs" / "TEST_AAA-USDT-SWAP" / "emissions.parquet"
    frame = study_evidence.read_table(path)
    study_evidence.write_table(
        path, frame.iloc[::-1].reset_index(drop=True), name="emissions"
    )
    _reseal(copy)
    admitted = analysis_source.admit_source(copy, model_instances=["fh"])
    records = analysis_source.RecordSource(admitted, _members(admitted))
    assert list(records.frames())


def test_each_raw_table_is_decoded_once_per_instrument(source_study):
    _pack, run_root, _request = source_study
    admitted = analysis_source.admit_source(run_root, model_instances=["fh"])
    members = _members(admitted)
    decoded: list[str] = []
    original = pack_study.StudyResults.table

    def counting(self, instrument_id, name):
        decoded.append(f"{instrument_id}/{name}")
        return original(self, instrument_id, name)

    pack_study.StudyResults.table = counting
    try:
        records = analysis_source.RecordSource(admitted, members)
        for _frame in records.frames():
            pass
    finally:
        pack_study.StudyResults.table = original
    assert len(decoded) == len(set(decoded))
    assert set(decoded) == {
        f"{instrument}/{name}"
        for instrument in admitted.instruments
        for name in ("conditions", "emissions", "primitives")
    }


def test_state_entry_true_nonemitting_and_unknown_anchors_are_not_controls(tmp_path):
    """A known-true nonemitting anchor belongs to neither group; unknown is not false."""
    from tools.pattern_lab.analysis.family import FamilyMember
    from tools.pattern_lab.analysis.source import _member_frame, _TimeframeMasks

    anchors = np.array([0, 1, 2, 3, 4], dtype=np.int64)
    masks = _TimeframeMasks(
        anchors=anchors,
        signal_time_ms=anchors + 1,
        condition_value={"c1": np.array([True, True, False, True, False])},
        condition_valid={"c1": np.array([True, True, True, False, True])},
        # state_entry emits only the false-to-true transition at index 0.
        emitted={"entry": np.array([True, False, False, False, False])},
    )
    member = FamilyMember(
        member_id="m", comparison_id="baseline__entry", kind="nonsignal_baseline",
        target_variant="entry", control_variant=None, model_instance_id="fh",
        timeframe_minutes=30, case_id="c", direction="long", horizon_minutes=60,
        commission_pct_per_side=0.05, primary=False, label="m",
    )
    outcomes = (
        np.zeros(5, dtype=np.float64), np.zeros(5, dtype=np.float64), np.ones(5, dtype=bool)
    )
    frame = _member_frame(
        member, masks, outcomes, "AAA", {"entry": {"condition_id": "c1"}}
    )
    rows = dict(zip(frame["signal_time_ms"].tolist(), zip(frame["is_target"], frame["is_control"])))
    assert rows[1] == (True, False)      # the emitted transition
    assert 2 not in rows                 # true, valid, nonemitting: neither group
    assert rows[3] == (False, True)      # known-valid false condition: a control
    assert 4 not in rows                 # unknown history is not a false control
    assert rows[5] == (False, True)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _copy_tree(source: Path, target: Path) -> None:
    import shutil

    shutil.copytree(source, target)


def _reseal(run_root: Path) -> None:
    """Rewrite the completion record so a deliberate edit is not a hash failure."""
    bundle_path = None
    for job in sorted((run_root / "jobs").iterdir()):
        bundle_path = job / "bundle.json"
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        for record in bundle["files"]:
            record["sha256"] = study_evidence.file_digest(job / record["name"])
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    record = json.loads(
        (run_root / study_evidence.COMPLETION_FILE).read_text(encoding="utf-8")
    )
    digests = {
        name: study_evidence.file_digest(run_root / name)
        for name in study_evidence.immutable_files(run_root)
    }
    record["evidence_sha256"] = digests
    from tools.pattern_lab.study import contracts as study_contracts

    record["evidence_set_sha256"] = study_contracts.semantic_digest(digests)
    (run_root / study_evidence.COMPLETION_FILE).write_text(
        json.dumps(record), encoding="utf-8"
    )


# --------------------------------------------------------------------------
# the tracked examples
# --------------------------------------------------------------------------

CONFIGS = Path(__file__).resolve().parents[2] / "tools" / "pattern_lab" / "configs"


def test_the_tracked_analysis_request_normalizes():
    normalized = analysis_request.load_analysis_request(
        CONFIGS / "example_analysis_two_green_pair.json"
    )
    assert normalized.model_instances == ("fixed_horizon",)
    assert [item.comparison_id for item in normalized.pairwise] == ["volume_filter"]
    assert normalized.pairwise[0].target_variant == "two_green_volume"
    assert normalized.pairwise[0].control_variant == "two_green_plain"
    assert normalized.schema_version == 2
    assert normalized.resamples is None and normalized.seed is None


def test_the_tracked_pair_study_declares_both_variants_and_keeps_the_original():
    document = json.loads(
        (CONFIGS / "example_study_two_green_pair_30m.json").read_text(encoding="utf-8")
    )
    variants = {item["id"]: item["hypothesis"] for item in document["hypotheses"]}
    assert variants == {
        "two_green_volume": "two_green_rising_quote_volume",
        "two_green_plain": "two_green",
    }
    entry = document["models"][0]["settings"]["by_timeframe"]["30"]
    assert entry["horizons_minutes"] == [60, 120, 240, 480]
    assert entry["primary_horizon_minutes"] == 240
    assert document["models"][0]["settings"]["directions"] == ["long", "short"]
    original = json.loads(
        (CONFIGS / "example_study_two_green_30m.json").read_text(encoding="utf-8")
    )
    assert [item["id"] for item in original["hypotheses"]] == ["two_green_every_bar"]


def test_the_plain_two_green_hypothesis_is_an_inclusive_parent_of_the_filtered_one():
    from tools.pattern_lab.study import builtins as study_builtins
    from tools.pattern_lab.study.contracts import BarSeries

    stamps = np.arange(8, dtype=np.int64) * 1_800_000
    values = np.array(
        [
            # open, high, low, close, quote volume
            [100.0, 101.0, 99.0, 100.5, 10.0],   # green
            [100.5, 101.5, 99.5, 101.0, 9.0],    # green, volume falls
            [101.0, 102.0, 100.0, 101.5, 12.0],  # green, volume rises
            [101.5, 102.0, 100.0, 101.0, 13.0],  # red
            [101.0, 102.0, 100.0, 101.5, 14.0],  # green after a red
            [101.5, 102.5, 100.5, 102.0, 15.0],  # green, volume rises
            [102.0, 103.0, 101.0, 102.5, 14.0],  # green, volume falls
            [102.5, 103.5, 101.5, 103.0, 16.0],  # green, volume rises
        ],
        dtype=np.float64,
    )
    series = BarSeries(
        instrument_id="AAA", timeframe_minutes=30, step_ms=1_800_000,
        timestamps_ms=stamps, slots=stamps // 1_800_000, values=values, research_start_index=0,
    )
    plain = study_builtins.TWO_GREEN_PLAIN_DESCRIPTOR.evaluate(series, {}, {})
    filtered = study_builtins.TWO_GREEN_DESCRIPTOR.evaluate(series, {}, {})
    assert plain.valid.tolist() == filtered.valid.tolist() == [False] + [True] * 7
    assert plain.value.tolist() == [False, True, True, False, False, True, True, True]
    assert filtered.value.tolist() == [False, False, True, False, False, True, False, True]
    # Every filtered anchor is a plain anchor on common validity.
    assert bool(np.all(plain.value[filtered.value]))
    assert study_builtins.TWO_GREEN_PLAIN_DESCRIPTOR.prior_bars({}) == 1
