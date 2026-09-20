"""Small machinery checks for the M3a calibration driver.

Ordinary pytest runs these cheap checks; the large declared experiments run once
for delivery through the module's own command and save their JSON evidence under
an external task-owned root.  Nothing here certifies an error rate.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.analysis import calibration as analysis_calibration
from tools.pattern_lab.analysis.calibration import (
    SCENARIOS,
    SCENARIOS_BY_NAME,
    Scenario,
    exact_upper_bound,
    generate_records,
    primary_member_id,
    record_frames,
    scenario_family,
    wilson_interval,
)


# --------------------------------------------------------------------------
# the frozen contract
# --------------------------------------------------------------------------

def test_every_generator_setting_is_frozen_and_digested():
    contract = analysis_calibration._contract_with_digest()
    assert contract["master_seed"] == 20260920
    assert contract["daily_ar_coefficient"] == 0.4
    assert contract["daily_level_scale"] == 0.001
    assert contract["bar_innovation_scale"] == 0.005
    assert contract["common_loading"] == 0.8 and contract["individual_loading"] == 0.6
    assert contract["signal_p11"] == 0.70 and contract["signal_p10"] == 0.0857
    assert contract["bar_target_probability_state_1"] == 0.45
    assert contract["bar_target_probability_state_0"] == 0.05
    assert contract["student_t_df"] == 5
    assert contract["months"].startswith("actual UTC calendar months")
    assert contract["generator_digest"]
    assert len(contract["scenarios"]) == len(SCENARIOS)


def test_the_declared_scenarios_cover_the_required_experiments():
    kinds = {}
    for scenario in SCENARIOS:
        kinds.setdefault(scenario.kind, []).append(scenario.name)
    assert len(kinds["admitted_null"]) == 5
    assert len(kinds["refusal"]) == 2
    assert len(kinds["stress"]) == 2
    assert len(kinds["planted"]) == 2
    assert kinds["planted_descriptive"] == ["planted_modest"]
    assert kinds["smoke"] == ["default_b_smoke"]
    boundary = SCENARIOS_BY_NAME["null_admission_boundary_336"]
    assert boundary.days == 336 and boundary.start_day == "2025-07-15"
    assert SCENARIOS_BY_NAME["null_dependent_t5"].innovation == "student_t5"
    assert SCENARIOS_BY_NAME["null_dependent_gaussian_companion"].innovation == "gaussian"
    for name in ("stress_long_dependence_ar09_p070", "stress_long_dependence_ar09_p097"):
        assert SCENARIOS_BY_NAME[name].daily_ar == 0.9
        assert SCENARIOS_BY_NAME[name].admitted is False
    assert SCENARIOS_BY_NAME["planted_positive_strong"].planted_effect == 0.005
    assert SCENARIOS_BY_NAME["planted_negative_strong"].planted_effect == -0.005
    assert SCENARIOS_BY_NAME["planted_modest"].planted_effect == 0.00005
    assert SCENARIOS_BY_NAME["default_b_smoke"].resamples == 9999


def test_the_seed_derivation_is_reproducible_and_stream_separated():
    first = analysis_calibration.bootstrap_seed(2, 17)
    assert first == analysis_calibration.bootstrap_seed(2, 17)
    assert first != analysis_calibration.bootstrap_seed(2, 18)
    assert 0 <= first <= 2**32 - 1
    data = analysis_calibration.data_generator(2, 17).standard_normal(4)
    assert np.allclose(data, analysis_calibration.data_generator(2, 17).standard_normal(4))
    assert not np.allclose(data, analysis_calibration.data_generator(3, 17).standard_normal(4))


def test_each_family_carries_both_directions_and_every_horizon():
    for scenario in SCENARIOS:
        members = scenario_family(scenario)
        horizons = sorted({item.horizon_minutes for item in members})
        assert horizons == [60, 120, 240, 480]
        assert sorted({item.direction for item in members}) == ["long", "short"]
        expected = 8 if scenario.comparison == "baseline" else 24
        assert len(members) == expected
        assert primary_member_id(scenario) in {item.member_id for item in members}


def test_the_parent_child_family_keeps_both_generated_baselines():
    scenario = SCENARIOS_BY_NAME["null_inclusive_parent"]
    comparisons = sorted({item.comparison_id for item in scenario_family(scenario)})
    assert comparisons == ["baseline__child", "baseline__parent", "child_versus_parent"]


# --------------------------------------------------------------------------
# the generators
# --------------------------------------------------------------------------

def _small(**overrides) -> Scenario:
    defaults = dict(
        scenario_id=901, name="unit", kind="unit", start_day="2025-07-01", days=40,
        dependent=True, innovation="gaussian", signal="markov", comparison="baseline",
        repetitions=1, resamples=1999, admitted=False,
    )
    defaults.update(overrides)
    return Scenario(**defaults)


def test_the_markov_signal_chain_is_stationary_and_independent_of_the_returns():
    rng = np.random.default_rng(5)
    states = analysis_calibration._markov_states(rng, 200000, 0.70)
    expected = 0.0857 / (0.0857 + 0.30)
    assert float(states.mean()) == pytest.approx(expected, abs=0.01)


def test_the_daily_factor_is_initialized_from_its_stationary_law():
    rng = np.random.default_rng(7)
    draws = np.array(
        [analysis_calibration._ar1_daily(rng, 400, 0.4)[0] for _index in range(2000)]
    )
    assert float(np.std(draws)) == pytest.approx(0.001, rel=0.1)


def test_standardized_student_t_innovations_have_unit_variance():
    rng = np.random.default_rng(11)
    values = analysis_calibration._innovations(rng, (400000,), "student_t5")
    assert float(np.std(values)) == pytest.approx(1.0, abs=0.05)
    with pytest.raises(PatternLabDataError, match="unknown innovation law"):
        analysis_calibration._innovations(rng, (4,), "cauchy")


def test_the_records_use_actual_utc_months_and_the_declared_horizons():
    scenario = _small()
    records = generate_records(scenario, 0)
    members = scenario_family(scenario)
    frames = list(record_frames(records, members))
    assert frames
    combined = frames[0]
    assert set(combined.columns) == set(analysis_calibration.RECORD_COLUMNS)
    signal = combined["signal_time_ms"].to_numpy()
    assert signal.min() >= scenario.study_start_ms
    assert signal.max() < scenario.study_end_ms
    months = np.unique(
        np.datetime64(scenario.study_start_ms, "ms").astype("datetime64[M]")
        + np.zeros(1, dtype="int64")
    )
    assert months.size == 1


def test_an_inclusive_parent_scenario_masks_identical_availability():
    scenario = SCENARIOS_BY_NAME["null_inclusive_parent"]
    records = generate_records(scenario, 0)
    for instrument in analysis_calibration.INSTRUMENTS:
        child = records.target_mask[instrument]
        parent = records.parent_mask[instrument]
        # Nonpredictive thinning: every child anchor is a parent anchor.
        assert bool(np.all(parent[child]))
        assert float(child.sum()) < float(parent.sum())
    shorter = records.available[analysis_calibration.SHORT_HISTORY_INSTRUMENT]
    assert not bool(shorter[: analysis_calibration.SHORT_HISTORY_START_DAY].any())


def test_a_planted_effect_moves_only_the_target_observations():
    scenario = _small(scenario_id=902, planted_effect=0.01)
    plain = _small(scenario_id=902)
    members = scenario_family(scenario)
    planted = list(record_frames(generate_records(scenario, 0), members))[0]
    baseline = list(record_frames(generate_records(plain, 0), members))[0]
    target = planted["is_target"].to_numpy()
    assert planted.loc[target, "net_return"].to_numpy() == pytest.approx(
        baseline.loc[target, "net_return"].to_numpy() + 0.01, nan_ok=True
    )
    assert planted.loc[~target, "net_return"].to_numpy() == pytest.approx(
        baseline.loc[~target, "net_return"].to_numpy(), nan_ok=True
    )


# --------------------------------------------------------------------------
# rate arithmetic and acceptance
# --------------------------------------------------------------------------

def test_the_one_sided_exact_binomial_bound_matches_scipy():
    from scipy.stats import beta

    assert exact_upper_bound(100, 2000) == pytest.approx(float(beta.ppf(0.95, 101, 1900)))
    assert exact_upper_bound(5, 5) == 1.0
    assert exact_upper_bound(0, 0) is None


def test_the_declared_gate_admits_at_most_the_documented_event_count():
    """The reviewed arithmetic: 2000 trials admit at most 139 events at 8%."""
    assert exact_upper_bound(139, 2000) <= analysis_calibration.ERROR_ENVELOPE
    assert exact_upper_bound(140, 2000) > analysis_calibration.ERROR_ENVELOPE


def test_the_wilson_interval_is_reported_with_its_denominator():
    interval = wilson_interval(100, 2000)
    assert 0.0 < interval["lower"] < 0.05 < interval["upper"] < 0.1
    assert wilson_interval(0, 0) == {"lower": None, "upper": None, "level": 0.95}


def test_acceptance_is_an_intersection_over_the_admitted_scenarios_only():
    def record(name, admitted, events):
        return {
            "name": name,
            "admitted": admitted,
            "primary_availability": 0.99,
            "primary_availability_meets_floor": True,
            "rates": {
                key: analysis_calibration.rate_record(key, events, 2000)
                for key in analysis_calibration.ACCEPTANCE_RATES
            },
        }

    passing = [record(f"s{index}", True, 100) for index in range(5)]
    scored = analysis_calibration.score_acceptance(passing + [record("stress", False, 900)])
    assert scored["checks_total"] == 15
    assert scored["checks_passed"] == 15 and scored["accepted"] is True
    assert "not a joint 95% statement" in scored["meaning"]

    failing = passing[:-1] + [record("s4", True, 300)]
    scored = analysis_calibration.score_acceptance(failing)
    assert scored["accepted"] is False and scored["checks_passed"] == 12


def test_availability_below_the_floor_fails_acceptance():
    record = {
        "name": "s1",
        "admitted": True,
        "primary_availability": 0.90,
        "primary_availability_meets_floor": False,
        "rates": {
            key: analysis_calibration.rate_record(key, 100, 2000)
            for key in analysis_calibration.ACCEPTANCE_RATES
        },
    }
    scored = analysis_calibration.score_acceptance([record])
    assert scored["accepted"] is False


# --------------------------------------------------------------------------
# the driver
# --------------------------------------------------------------------------

def test_a_short_population_publishes_no_p_value_or_interval():
    scenario = SCENARIOS_BY_NAME["short_population_84_days"]
    row = analysis_calibration.run_repetition(scenario, 0)
    assert row["primary_available"] is False
    assert row["primary_p_raw"] is None and row["primary_interval_lower"] is None
    assert "insufficient_span" in row["primary_reasons"]
    # Descriptive estimates survive the refusal.
    assert row["primary_lift"] is not None


def test_zero_contribution_padding_does_not_restore_availability():
    scenario = SCENARIOS_BY_NAME["short_population_84_days"]
    padded = analysis_calibration.run_repetition(scenario, 0, padded_days=365)
    assert padded["primary_geometry"]["day_grid_days"] == 365
    assert padded["primary_geometry"]["supported_span_days"] == 84
    assert padded["primary_available"] is False


def test_the_admission_boundary_scenario_exercises_exactly_the_declared_geometry():
    scenario = SCENARIOS_BY_NAME["null_admission_boundary_336"]
    row = analysis_calibration.run_repetition(scenario, 0)
    geometry = row["primary_geometry"]
    assert geometry["day_grid_days"] == 336
    assert geometry["supported_span_days"] == 336
    assert geometry["supported_blocks"] == 48
    assert row["primary_available"] is True


@pytest.mark.slow
def test_the_driver_saves_a_compact_evidence_artifact(tmp_path):
    document = analysis_calibration.run_calibration(
        output_root=tmp_path, scenarios=["null_independent"], repetitions=3
    )
    saved = json.loads((tmp_path / "calibration.json").read_text(encoding="utf-8"))
    assert saved == document
    assert saved["results"][0]["repetitions"] == 3
    assert set(saved["results"][0]["rates"]) == set(analysis_calibration.ACCEPTANCE_RATES)
    assert saved["generator_contract"]["generator_digest"]
    assert "certifies neither unsimulated generators" in saved["scope"]


def test_an_unknown_scenario_name_is_an_actionable_error(tmp_path):
    with pytest.raises(PatternLabDataError, match="unknown calibration scenario"):
        analysis_calibration.run_calibration(output_root=tmp_path, scenarios=["nope"])


# --------------------------------------------------------------------------
# the evidence replay
# --------------------------------------------------------------------------

@pytest.mark.slow
def test_the_production_joins_and_the_numerical_boundary_agree(tmp_path):
    """One fixed repetition through both paths on the same synthetic records."""
    outcome = analysis_calibration.run_evidence_replay(repetitions=1, root=tmp_path)
    assert outcome["mismatched"] == []
    assert max(outcome["max_absolute_difference"].values()) <= 1e-12
    assert outcome["agrees"] is True


@pytest.mark.slow
def test_the_state_entry_replay_excludes_nonemitting_and_unknown_anchors(tmp_path):
    outcome = analysis_calibration._state_entry_replay(tmp_path)
    assert outcome["occurrence"] == "state_entry"
    # A known-true nonemitting anchor and an unknown-history anchor are in
    # neither group, so fewer records are supplied than there are anchors.
    anchors = len(analysis_calibration.INSTRUMENTS) * (
        analysis_calibration.SCENARIOS_BY_NAME["null_dependent_t5"].days
        * analysis_calibration.BARS_PER_DAY
        - 1
    )
    assert 0 < outcome["records_supplied"] < anchors
    assert outcome["target_rows"] < outcome["control_rows"]
