"""Small machinery checks for the M3a calibration driver.

Ordinary pytest runs these cheap checks; the large declared experiments run once
for delivery through the module's own command and save their JSON evidence under
an external task-owned root.  Nothing here certifies an error rate.
"""

from __future__ import annotations

import json
from dataclasses import asdict, replace

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.analysis import calibration as analysis_calibration
from tools.pattern_lab.study.contracts import semantic_digest
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
    assert len(kinds["admitted_null"]) == 6
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
    corrected = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    assert corrected.scenario_id == 101 and corrected.signal == "confounded_signal_month"
    assert corrected.admitted is True and corrected.repetitions == 2000


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
# fixture 101: the signal-month-aligned confounded null
# --------------------------------------------------------------------------

def _stratum_months(scenario):
    """The production signal-month ordinal of every anchor, from the real grid."""
    from tools.pattern_lab.analysis.estimator import CalendarGrid

    bars = scenario.days * analysis_calibration.BARS_PER_DAY
    anchors = bars - 1
    step = analysis_calibration.TIMEFRAME_MINUTES * 60_000
    signal = scenario.grid_start_ms + (np.arange(anchors, dtype=np.int64) + 1) * step
    grid = CalendarGrid(scenario.study_start_ms, scenario.study_end_ms)
    return grid, signal, grid.month_of_day[grid.day_offsets(signal)]


def _assigned_probabilities(scenario, instrument_index, anchors):
    """The event probability the generator assigns to every anchor."""
    bars = scenario.days * analysis_calibration.BARS_PER_DAY
    table = np.asarray(analysis_calibration.CONFOUNDED_PROBABILITIES)
    if scenario.signal == "confounded_signal_month":
        owner = analysis_calibration._signal_month_index(scenario.grid_start_ms, bars)[:anchors]
    else:
        owner = np.repeat(
            analysis_calibration._month_index(scenario.grid_start_ms, scenario.days),
            analysis_calibration.BARS_PER_DAY,
        )[:anchors]
    return table[(instrument_index + owner) % table.size]


def test_a_month_end_anchor_belongs_to_the_next_signal_month():
    scenario = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    bars = scenario.days * analysis_calibration.BARS_PER_DAY
    driver = analysis_calibration._signal_month_index(scenario.grid_start_ms, bars)
    bar_month = np.repeat(
        analysis_calibration._month_index(scenario.grid_start_ms, scenario.days),
        analysis_calibration.BARS_PER_DAY,
    )
    boundary = np.flatnonzero(driver[: bars - 1] != bar_month[: bars - 1])
    # One 23:30 anchor per month boundary inside the 365-day grid.
    assert boundary.size == 11
    assert np.all(driver[boundary] == bar_month[boundary] + 1)
    assert np.all(boundary % analysis_calibration.BARS_PER_DAY == analysis_calibration.BARS_PER_DAY - 1)
    # The driver's ordinal is exactly the production grid's stratum month.
    _grid, _signal, production = _stratum_months(scenario)
    assert np.array_equal(driver[: production.size], production)


def test_fixture_101_probabilities_are_constant_inside_every_actual_stratum():
    corrected = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    legacy = SCENARIOS_BY_NAME["null_conditional_confounded"]
    for scenario, constant in ((corrected, True), (legacy, False)):
        _grid, _signal, months = _stratum_months(scenario)
        mismatched = 0
        for index in range(len(analysis_calibration.INSTRUMENTS)):
            probabilities = _assigned_probabilities(scenario, index, months.size)
            for month in np.unique(months):
                distinct = np.unique(probabilities[months == month])
                if distinct.size != 1:
                    mismatched += 1
        assert (mismatched == 0) is constant
    # Exactly the 44 boundary anchors of the legacy fixture carry the wrong month.
    _grid, _signal, months = _stratum_months(legacy)
    wrong = sum(
        int(
            np.count_nonzero(
                _assigned_probabilities(legacy, index, months.size)
                != _assigned_probabilities(corrected, index, months.size)
            )
        )
        for index in range(len(analysis_calibration.INSTRUMENTS))
    )
    assert wrong == 44


def test_the_two_confounded_fixtures_keep_their_opposing_instrument_parity():
    corrected = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    _grid, _signal, months = _stratum_months(corrected)
    probabilities = [
        _assigned_probabilities(corrected, index, months.size)
        for index in range(len(analysis_calibration.INSTRUMENTS))
    ]
    # ``(index + month) % 2`` pairs the even and the odd instruments.
    assert np.array_equal(probabilities[0], probabilities[2])
    assert np.array_equal(probabilities[1], probabilities[3])
    assert not np.array_equal(probabilities[0], probabilities[1])
    assert np.all(probabilities[0] + probabilities[1] == pytest.approx(0.50))
    # The deterministic return-mean schedule keeps its original bar-time
    # ownership, so the return law is the legacy one.
    legacy = SCENARIOS_BY_NAME["null_conditional_confounded"]
    assert corrected.innovation == legacy.innovation
    assert corrected.dependent == legacy.dependent
    assert corrected.daily_ar == legacy.daily_ar
    assert corrected.days == legacy.days and corrected.start_day == legacy.start_day
    assert corrected.comparison == legacy.comparison
    assert corrected.resamples == legacy.resamples


def _population_lift(scenario, horizon_minutes):
    """Exact stratum row-mixture target and control means, at one horizon.

    Direct short window sums: a long cumulative prefix difference would cancel
    against a running total of order 0.6 and hide a 1e-18 residual.
    """
    bars = scenario.days * analysis_calibration.BARS_PER_DAY
    anchors = bars - 1
    steps = horizon_minutes // analysis_calibration.TIMEFRAME_MINUTES
    _grid, _signal, months = _stratum_months(scenario)
    bar_month = np.repeat(
        analysis_calibration._month_index(scenario.grid_start_ms, scenario.days),
        analysis_calibration.BARS_PER_DAY,
    )
    index_of = np.arange(anchors)
    valid = index_of + 1 + steps <= bars
    worst = 0.0
    numerator = 0.0
    denominator = 0.0
    for index in range(len(analysis_calibration.INSTRUMENTS)):
        sign = 1.0 if index % 2 == 0 else -1.0
        shift = (
            sign
            * analysis_calibration.CONFOUNDED_MONTH_SHIFT
            * np.where(bar_month % 2 == 0, 1.0, -1.0)
        )
        windows = np.lib.stride_tricks.sliding_window_view(shift, steps).sum(axis=1)
        mu = np.zeros(anchors, dtype=np.float64)
        mu[: windows.size - 1] = windows[1:]
        probabilities = _assigned_probabilities(scenario, index, anchors)
        for month in np.unique(months):
            take = valid & (months == month)
            if not take.any():
                continue
            weight_e = probabilities[take].sum()
            weight_c = (1.0 - probabilities[take]).sum()
            lift = float(
                (probabilities[take] * mu[take]).sum() / weight_e
                - ((1.0 - probabilities[take]) * mu[take]).sum() / weight_c
            )
            worst = max(worst, abs(lift))
            numerator += weight_e * lift
            denominator += weight_e
    return worst, numerator / denominator


@pytest.mark.slow
def test_the_corrected_fixture_has_a_zero_population_contrast_at_every_horizon():
    """The deterministic row-mixture identity, not a claim about realized masks."""
    corrected = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    legacy = SCENARIOS_BY_NAME["null_conditional_confounded"]
    for horizon in analysis_calibration.HORIZON_MINUTES:
        worst, weighted = _population_lift(corrected, horizon)
        # The contributions are sums of at most 16 terms of 4e-4, so the
        # floating-point floor of this identity is around 1e-17.
        assert worst < 1e-17
        assert abs(weighted) < 1e-17
    # The legacy fixture keeps its documented, tiny, nonzero mismatch.
    legacy_worst, legacy_weighted = _population_lift(legacy, 240)
    assert legacy_worst == pytest.approx(4.1254148207e-08, rel=1e-9)
    assert legacy_weighted == pytest.approx(1.4622196520e-08, rel=1e-9)
    assert "not a waiver for a failed rate" in legacy.caveat
    assert corrected.caveat == ""


def test_the_corrected_fixture_censors_its_final_horizon_windows():
    scenario = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    records = generate_records(scenario, 0)
    members = [
        item
        for item in scenario_family(scenario)
        if item.direction == "long" and item.horizon_minutes == 480
    ]
    frame = next(iter(record_frames(records, members)))
    valid = frame["return_valid"].to_numpy()
    signal = frame["signal_time_ms"].to_numpy()
    steps = 480 // analysis_calibration.TIMEFRAME_MINUTES
    tail = scenario.grid_end_ms - steps * analysis_calibration.TIMEFRAME_MINUTES * 60_000
    # Every anchor whose 8-hour window would run past the generated grid is
    # invalid, and nothing before that boundary is censored by the horizon.
    assert not valid[signal > tail].any()
    assert valid[signal <= tail].all()
    assert np.isnan(frame["net_return"].to_numpy()[~valid]).all()


def test_the_corrected_fixture_reaches_full_support_through_the_production_path():
    scenario = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    row = analysis_calibration.run_repetition(scenario, 20000)
    assert row["primary_available"] is True and row["primary_reasons"] == []
    geometry = row["primary_geometry"]
    assert geometry["day_grid_days"] == 365 and geometry["retained_months"] == 12
    assert geometry["retained_strata"] == 48
    assert row["family_available"] == row["family_size"] == 8


def test_the_corrected_fixture_draws_an_independent_realization_of_its_own_id():
    """Fixture 101 keys its seed on 101, so it is not a paired correction of 3."""
    corrected = SCENARIOS_BY_NAME["null_confounded_signal_month_v2"]
    legacy = SCENARIOS_BY_NAME["null_conditional_confounded"]
    first = generate_records(corrected, 0)
    second = generate_records(legacy, 0)
    instrument = analysis_calibration.INSTRUMENTS[0]
    assert not np.array_equal(first.returns[instrument], second.returns[instrument])
    assert not np.array_equal(first.target_mask[instrument], second.target_mask[instrument])
    assert np.array_equal(
        first.returns[instrument], generate_records(corrected, 0).returns[instrument]
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


def test_the_rate_scorer_is_an_intersection_over_the_admitted_records_it_is_given():
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
    assert scored["checks_passed"] == 15
    assert scored["all_requested_checks_passed"] is True
    assert scored["scope"] == "requested_subset"
    assert "not a joint 95% statement" in scored["meaning"]
    assert "never release acceptance" in scored["meaning"]
    # The release Boolean is not this helper's to publish.
    assert "accepted" not in scored

    failing = passing[:-1] + [record("s4", True, 300)]
    scored = analysis_calibration.score_acceptance(failing)
    assert scored["all_requested_checks_passed"] is False and scored["checks_passed"] == 12


def test_availability_below_the_floor_fails_the_rate_scorer():
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
    assert scored["all_requested_checks_passed"] is False


# --------------------------------------------------------------------------
# the versioned legacy-protocol gate
# --------------------------------------------------------------------------

def _entry_record(entry, *, events=100, repetitions=None, availability=1.0, refuses=None):
    """One synthetic executed result for a planned legacy entry."""
    scenario = SCENARIOS_BY_NAME[entry["name"].removesuffix("_padded_365")]
    padded = entry["variant"] == analysis_calibration.PADDED_VARIANT
    total = int(entry["repetitions"] if repetitions is None else repetitions)
    available = round(availability * total)
    refusing = scenario.kind == "refusal" if refuses is None else refuses
    ids = list(range(total))
    # A synthetic count can never exceed its own denominator.
    primary_events = min(events, available)
    family_events = min(events, total)
    return {
        "scenario_id": scenario.scenario_id,
        "name": entry["name"],
        "kind": scenario.kind,
        "admitted": bool(scenario.admitted and not padded),
        "caveat": scenario.caveat or None,
        "config": asdict(scenario),
        "ledger": analysis_calibration.attempt_ledger(
            scenario,
            padded_days=365 if padded else None,
            requested=total,
            attempted=ids,
            completed=ids,
        ),
        "repetitions": total,
        "resamples": scenario.resamples,
        "primary_availability": availability,
        "primary_availability_meets_floor": availability >= 0.95,
        "rates": {
            "primary_raw_rejection": analysis_calibration.rate_record(
                "primary_raw_rejection", primary_events, available
            ),
            "primary_interval_noncoverage": analysis_calibration.rate_record(
                "primary_interval_noncoverage", primary_events, available
            ),
            "family_wise_holm_rejection": analysis_calibration.rate_record(
                "family_wise_holm_rejection", family_events, total
            ),
        },
        "published_inference": {
            "repetitions_with_p_value": 0 if refusing else available,
            "repetitions_with_interval": 0 if refusing else available,
        },
    }


def _complete_document(**overrides):
    """A synthetic, complete and passing legacy-protocol run."""
    plan = analysis_calibration.build_run_plan(analysis_calibration.LEGACY_PROTOCOL_SCENARIOS)
    contract = analysis_calibration._contract_with_digest()
    document = {
        "schema_version": analysis_calibration.CALIBRATION_SCHEMA_VERSION,
        "generator_contract": contract,
        "run_plan": plan,
        "results": [
            _entry_record(entry) for entry in analysis_calibration.legacy_protocol_plan()
        ],
        "evidence_replay": {
            "repetitions": analysis_calibration.LEGACY_REPLAY_REPETITIONS,
            "agrees": True,
            "mismatched": [],
        },
        "state_entry_replay": {"records_supplied": 1234},
    }
    document.update(overrides)
    return document


def test_a_complete_passing_synthetic_run_is_accepted_and_a_failing_one_is_not():
    state = analysis_calibration.legacy_protocol_state(_complete_document())
    assert state["protocol"] == analysis_calibration.LEGACY_PROTOCOL
    assert state["complete_run"] is True
    assert state["eligibility_reasons"] == []
    assert state["diagnostic_checks_passed"] is True
    assert state["accepted"] is True and state["accepted_reasons"] == []
    assert state["diagnostic"]["checks_total"] == 15

    document = _complete_document()
    document["results"] = [
        _entry_record(entry, events=300 if entry["name"] == "null_dependent_t5" else 100)
        for entry in analysis_calibration.legacy_protocol_plan()
    ]
    failing = analysis_calibration.legacy_protocol_state(document)
    assert failing["complete_run"] is True
    assert failing["diagnostic_checks_passed"] is False
    assert failing["accepted"] is False
    assert any("null_dependent_t5" in reason for reason in failing["accepted_reasons"])


def test_a_passing_subset_is_diagnostic_only_and_never_release_acceptance():
    """The audit's counterexample in synthetic form: one passing fixture."""
    plan = analysis_calibration.legacy_protocol_plan()
    one = [_entry_record(plan[0])]
    assert analysis_calibration.score_acceptance(one)["all_requested_checks_passed"] is True
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=one))
    assert state["diagnostic_checks_passed"] is True
    assert state["complete_run"] is False
    assert state["accepted"] is False
    assert any("is missing" in reason for reason in state["eligibility_reasons"])
    assert any("is missing" in reason for reason in state["accepted_reasons"])


def test_an_empty_run_is_neither_complete_nor_diagnostically_passing():
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=[]))
    assert state["diagnostic_checks_passed"] is False
    assert state["complete_run"] is False and state["accepted"] is False
    assert "no admitted rate check was scored" in state["accepted_reasons"]


def test_a_duplicated_or_substituted_fixture_cannot_be_a_complete_run():
    plan = analysis_calibration.legacy_protocol_plan()
    rows = [_entry_record(entry) for entry in plan]
    duplicated = analysis_calibration.legacy_protocol_state(
        _complete_document(results=rows + [_entry_record(plan[0])])
    )
    assert duplicated["complete_run"] is False
    assert any("appears 2 times" in reason for reason in duplicated["eligibility_reasons"])

    substituted = list(rows)
    substituted[1] = _entry_record(plan[0])
    substituted[1]["name"] = plan[1]["name"]
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=substituted))
    assert state["complete_run"] is False
    assert any("does not match the planned" in r for r in state["eligibility_reasons"])

    extra = list(rows)
    extra.append({**_entry_record(plan[0]), "name": "null_confounded_signal_month_v2"})
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=extra))
    assert any("not part of this protocol" in r for r in state["eligibility_reasons"])


def test_reduced_attempts_and_a_smoke_override_stay_diagnostic():
    plan = analysis_calibration.legacy_protocol_plan()
    rows = [_entry_record(entry, repetitions=50) for entry in plan]
    document = _complete_document(
        results=rows,
        run_plan=analysis_calibration.build_run_plan(
            analysis_calibration.LEGACY_PROTOCOL_SCENARIOS, repetitions=50
        ),
    )
    state = analysis_calibration.legacy_protocol_state(document)
    assert state["complete_run"] is False and state["accepted"] is False
    assert any("smoke run" in reason for reason in state["eligibility_reasons"])
    assert any("requested repetitions instead of" in r for r in state["eligibility_reasons"])


def test_an_omitted_or_disagreeing_replay_blocks_acceptance():
    for override, fragment in (
        ({"evidence_replay": None}, "evidence replay is absent"),
        (
            {
                "evidence_replay": {
                    "repetitions": analysis_calibration.LEGACY_REPLAY_REPETITIONS,
                    "agrees": False,
                    "mismatched": ["x"],
                }
            },
            "do not agree",
        ),
        (
            {"evidence_replay": {"repetitions": 2, "agrees": True, "mismatched": []}},
            "not 25",
        ),
        ({"state_entry_replay": None}, "state-entry replay is absent"),
        (
            {
                "run_plan": analysis_calibration.build_run_plan(
                    analysis_calibration.LEGACY_PROTOCOL_SCENARIOS, replay_repetitions=0
                )
            },
            "replay ran 0 repetitions",
        ),
    ):
        state = analysis_calibration.legacy_protocol_state(_complete_document(**override))
        assert state["accepted"] is False
        assert any(fragment in reason for reason in state["eligibility_reasons"]), fragment


def test_a_required_refusal_that_published_inference_blocks_acceptance():
    rows = [
        _entry_record(
            entry, refuses=entry["name"] != "short_population_84_days_padded_365"
        )
        for entry in analysis_calibration.legacy_protocol_plan()
    ]
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=rows))
    assert state["complete_run"] is False
    assert any("deterministic refusal published" in r for r in state["eligibility_reasons"])


def test_inconsistent_counters_and_configurations_block_acceptance():
    plan = analysis_calibration.legacy_protocol_plan()

    def mutate(index, change):
        rows = [_entry_record(entry) for entry in plan]
        rows[index] = {**rows[index], **change}
        return analysis_calibration.legacy_protocol_state(_complete_document(results=rows))

    missing_ledger = mutate(0, {"ledger": None})
    assert any("no attempt ledger" in r for r in missing_ledger["eligibility_reasons"])

    rows = [_entry_record(entry) for entry in plan]
    rows[0]["ledger"] = analysis_calibration.attempt_ledger(
        SCENARIOS_BY_NAME["null_independent"],
        padded_days=None,
        requested=2000,
        attempted=list(range(1999)) + [5000],
        completed=list(range(1999)) + [5000],
    )
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=rows))
    assert any("attempted repetition IDs" in r for r in state["eligibility_reasons"])

    rows = [_entry_record(entry) for entry in plan]
    rows[0]["ledger"] = analysis_calibration.attempt_ledger(
        SCENARIOS_BY_NAME["null_independent"],
        padded_days=None,
        requested=2000,
        attempted=list(range(2000)),
        completed=list(range(1500)),
    )
    state = analysis_calibration.legacy_protocol_state(_complete_document(results=rows))
    assert any("incomplete_attempts" in r for r in state["eligibility_reasons"])

    bad_denominator = mutate(
        0,
        {
            "rates": {
                "primary_raw_rejection": analysis_calibration.rate_record("r", 10, 2000),
                "primary_interval_noncoverage": analysis_calibration.rate_record("n", 10, 1900),
                "family_wise_holm_rejection": analysis_calibration.rate_record("f", 10, 2000),
            }
        },
    )
    assert any("denominators disagree" in r for r in bad_denominator["eligibility_reasons"])

    wrong_availability = mutate(0, {"primary_availability": 0.5})
    assert any("contradicts the reported" in r for r in wrong_availability["eligibility_reasons"])

    bad_family = mutate(
        0,
        {
            "rates": {
                "primary_raw_rejection": analysis_calibration.rate_record("r", 10, 2000),
                "primary_interval_noncoverage": analysis_calibration.rate_record("n", 10, 2000),
                "family_wise_holm_rejection": analysis_calibration.rate_record("f", 10, 1000),
            }
        },
    )
    assert any("is not every attempted" in r for r in bad_family["eligibility_reasons"])


def test_a_schema_v1_document_or_a_tampered_contract_is_ineligible():
    state = analysis_calibration.legacy_protocol_state(_complete_document(schema_version=1))
    assert state["complete_run"] is False
    assert any("complete-run contract" in r for r in state["eligibility_reasons"])

    document = _complete_document()
    document["generator_contract"] = {
        **document["generator_contract"], "master_seed": 1234,
    }
    state = analysis_calibration.legacy_protocol_state(document)
    assert any("does not match its own contract" in r for r in state["eligibility_reasons"])

    document = _complete_document()
    contract = dict(document["generator_contract"])
    contract["master_seed"] = 1234
    contract["generator_digest"] = semantic_digest(
        {k: v for k, v in contract.items() if k != "generator_digest"}
    )
    document["generator_contract"] = contract
    state = analysis_calibration.legacy_protocol_state(document)
    assert any("differs from this driver's frozen settings" in r for r in state["eligibility_reasons"])


def test_the_attempt_ledger_records_identities_not_only_counts():
    ledger = analysis_calibration.attempt_ledger(
        SCENARIOS_BY_NAME["null_independent"],
        padded_days=None,
        requested=4,
        attempted=[0, 1, 2, 3],
        completed=[0, 1, 2, 3],
    )
    assert ledger["status"] == "completed" and ledger["reasons"] == []
    assert ledger["attempted"]["ranges"] == [[0, 3]]
    assert ledger["variant"] == analysis_calibration.UNPADDED_VARIANT
    assert ledger["config_digest"] == analysis_calibration.scenario_config_digest(
        SCENARIOS_BY_NAME["null_independent"]
    )
    duplicated = analysis_calibration.attempt_ledger(
        SCENARIOS_BY_NAME["null_independent"],
        padded_days=365,
        requested=4,
        attempted=[0, 1, 2, 2],
        completed=[0, 1, 2, 2],
    )
    assert duplicated["status"] == "incomplete"
    assert "duplicate_attempt_id" in duplicated["reasons"]
    assert duplicated["variant"] == analysis_calibration.PADDED_VARIANT
    assert analysis_calibration.id_ranges([5, 0, 1, 2, 9, 10]) == [[0, 2], [5, 5], [9, 10]]


def test_legacy_eligibility_survives_unrelated_registry_growth(monkeypatch):
    from dataclasses import replace
    document = _complete_document()
    extra = replace(SCENARIOS[0], scenario_id=999, name="unrelated_future_fixture")
    monkeypatch.setattr(analysis_calibration, "SCENARIOS", (*SCENARIOS, extra))
    assert analysis_calibration.legacy_protocol_state(document)["complete_run"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "changed"])
def test_legacy_required_contract_entries_still_matter(change):
    from copy import deepcopy
    document = deepcopy(_complete_document())
    contract = document["generator_contract"]
    scenarios = contract["scenarios"]
    if change == "missing":
        scenarios.pop(0)
    elif change == "duplicate":
        scenarios.append(deepcopy(scenarios[0]))
    else:
        scenarios[0]["days"] += 1
    contract["generator_digest"] = semantic_digest({k: v for k, v in contract.items() if k != "generator_digest"})
    assert not analysis_calibration.legacy_protocol_state(document)["complete_run"]


def test_the_run_plan_lists_driver_generated_padded_refusal_variants():
    plan = analysis_calibration.build_run_plan(["short_population_84_days", "null_independent"])
    assert [item["name"] for item in plan["entries"]] == [
        "short_population_84_days",
        "short_population_84_days_padded_365",
        "null_independent",
    ]
    assert plan["entries"][1]["driver_generated"] is True
    assert plan["protocol"] == analysis_calibration.LEGACY_PROTOCOL
    with pytest.raises(PatternLabDataError, match="unknown calibration scenario"):
        analysis_calibration.build_run_plan(["nope"])


def test_the_legacy_protocol_set_is_pinned_and_excludes_the_new_fixtures():
    assert analysis_calibration.DEFAULT_SCENARIOS is analysis_calibration.LEGACY_PROTOCOL_SCENARIOS
    assert "null_confounded_signal_month_v2" not in analysis_calibration.LEGACY_PROTOCOL_SCENARIOS
    assert set(analysis_calibration.LEGACY_PROTOCOL_SCENARIOS) < set(SCENARIOS_BY_NAME)
    assert len(analysis_calibration.LEGACY_PROTOCOL_SCENARIOS) == 14


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


def test_a_repetition_retains_the_primary_bootstrap_scale_and_error_quantiles():
    """The diagnostics are read back from the production bootstrap, not recomputed."""
    scenario = SCENARIOS_BY_NAME["null_admission_boundary_336"]
    row = analysis_calibration.run_repetition(scenario, 0)
    assert row["primary_available"] is True
    assert row["primary_bootstrap_sd"] > 0.0
    lower = row["primary_error_quantile_0025"]
    upper = row["primary_error_quantile_0975"]
    assert lower < upper
    # The published basic interval is exactly this error distribution inverted.
    assert row["primary_interval_lower"] == pytest.approx(row["primary_lift"] - upper)
    assert row["primary_interval_upper"] == pytest.approx(row["primary_lift"] - lower)


def test_a_refused_repetition_reports_null_bootstrap_diagnostics():
    scenario = SCENARIOS_BY_NAME["short_population_84_days"]
    row = analysis_calibration.run_repetition(scenario, 0)
    assert row["primary_available"] is False
    assert row["primary_bootstrap_sd"] is None
    assert row["primary_error_quantile_0025"] is None
    assert row["primary_error_quantile_0975"] is None


def test_a_degenerate_repetition_reports_null_bootstrap_diagnostics(monkeypatch):
    scenario = SCENARIOS_BY_NAME["null_inclusive_parent"]
    records = generate_records(scenario, 0)
    # An identical target and parent gives a zero contrast after bootstrapping,
    # rather than failing the support gate before a bootstrap record exists.
    records = replace(records, control_mask=records.target_mask)
    monkeypatch.setattr(analysis_calibration, "generate_records", lambda *_: records)

    row = analysis_calibration.run_repetition(scenario, 0)
    assert row["primary_available"] is False
    assert row["primary_reasons"] == ["degenerate_contrast"]
    assert row["primary_lift"] == pytest.approx(0.0, abs=1e-15)
    assert row["primary_bootstrap_sd"] is None
    assert row["primary_error_quantile_0025"] is None
    assert row["primary_error_quantile_0975"] is None


def test_the_scale_diagnostic_uses_one_shared_available_primary_population():
    scenario = SCENARIOS_BY_NAME["null_admission_boundary_336"]
    record = analysis_calibration.run_scenario(scenario, repetitions=2)
    scale = record["bootstrap_scale"]
    assert scale["n"] == 2
    assert scale["mean_bootstrap_sd"] > 0.0
    assert scale["rms_bootstrap_sd"] >= scale["mean_bootstrap_sd"]
    assert scale["empirical_lift_sd"] > 0.0
    assert scale["mean_bootstrap_sd_over_empirical_sd"] == pytest.approx(
        scale["mean_bootstrap_sd"] / scale["empirical_lift_sd"]
    )
    # The existing fields keep their own population and their exact values.
    assert record["effect"]["mean_lift"] is not None
    assert set(record["rates"]) == set(analysis_calibration.ACCEPTANCE_RATES)


def test_a_single_repetition_leaves_the_scale_ratio_undefined_with_its_count():
    record = analysis_calibration.run_scenario(
        SCENARIOS_BY_NAME["null_admission_boundary_336"], repetitions=1
    )
    scale = record["bootstrap_scale"]
    assert scale["n"] == 1
    assert scale["mean_bootstrap_sd"] > 0.0
    assert scale["empirical_lift_sd"] is None
    assert scale["mean_bootstrap_sd_over_empirical_sd"] is None


def test_an_unavailable_population_publishes_an_empty_scale_diagnostic():
    record = analysis_calibration.run_scenario(
        SCENARIOS_BY_NAME["short_population_84_days"], repetitions=2
    )
    scale = record["bootstrap_scale"]
    assert scale["n"] == 0
    assert scale["mean_bootstrap_sd"] is None
    assert scale["rms_bootstrap_sd"] is None
    assert scale["empirical_lift_sd"] is None
    assert scale["mean_bootstrap_sd_over_empirical_sd"] is None
    # Descriptive estimates survive the refusal, on their own denominator.
    assert record["effect"]["mean_lift"] is not None


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
    # The additive scale diagnostics survive serialization at schema version 1.
    assert saved["schema_version"] == analysis_calibration.CALIBRATION_SCHEMA_VERSION == 2
    scale = saved["results"][0]["bootstrap_scale"]
    assert scale["n"] == 3 and scale["mean_bootstrap_sd"] > 0.0
    assert scale["mean_bootstrap_sd_over_empirical_sd"] > 0.0
    assert "not a variance factor" in scale["population"]
    assert saved["generator_contract"]["generator_digest"]
    assert "certifies neither unsimulated generators" in saved["scope"]


def _inject_driver(monkeypatch, *, events=100):
    """Drive the real ``run_calibration`` with injected per-entry results."""
    planned = {item["name"]: item for item in analysis_calibration.legacy_protocol_plan()}

    def fake_run_scenario(scenario, *, repetitions=None, padded_days=None, progress=None):
        name = f"{scenario.name}_padded_365" if padded_days else scenario.name
        return _entry_record(planned[name], events=events, repetitions=repetitions)

    monkeypatch.setattr(analysis_calibration, "run_scenario", fake_run_scenario)
    monkeypatch.setattr(
        analysis_calibration,
        "run_evidence_replay",
        lambda **kwargs: {
            "repetitions": kwargs["repetitions"], "agrees": True, "mismatched": []
        },
    )
    monkeypatch.setattr(
        analysis_calibration, "_state_entry_replay", lambda root: {"records_supplied": 7}
    )


def test_the_driver_and_cli_publish_the_versioned_gate(tmp_path, monkeypatch, capsys):
    _inject_driver(monkeypatch)
    exit_code = analysis_calibration.main(["--output-root", str(tmp_path / "pass")])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["accepted"] is True and payload["complete_run"] is True
    assert payload["diagnostic_checks_passed"] is True
    assert payload["checks_total"] == 15 and payload["checks_passed"] == 15
    assert payload["protocol"] == analysis_calibration.LEGACY_PROTOCOL
    assert payload["eligibility_reasons"] == [] and payload["accepted_reasons"] == []
    assert "deliberately does not distinguish" in payload["exit_code_note"]
    saved = json.loads(
        (tmp_path / "pass" / "calibration.json").read_text(encoding="utf-8")
    )
    assert saved["acceptance"]["accepted"] is True
    assert saved["run_plan"]["protocol"] == analysis_calibration.LEGACY_PROTOCOL
    assert saved["schema_version"] == 2


def test_the_cli_returns_two_for_a_completed_failing_run(tmp_path, monkeypatch, capsys):
    _inject_driver(monkeypatch, events=300)
    exit_code = analysis_calibration.main(["--output-root", str(tmp_path / "fail")])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["complete_run"] is True
    assert payload["diagnostic_checks_passed"] is False
    assert payload["accepted"] is False and payload["accepted_reasons"]


def test_the_cli_returns_two_for_a_completed_diagnostic_subset(tmp_path, monkeypatch, capsys):
    _inject_driver(monkeypatch)
    exit_code = analysis_calibration.main(
        ["--output-root", str(tmp_path / "subset"), "--scenarios", "null_independent"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["diagnostic_checks_passed"] is True
    assert payload["complete_run"] is False and payload["accepted"] is False
    assert any("is missing" in reason for reason in payload["eligibility_reasons"])


def test_a_smoke_repetition_override_cannot_return_success(tmp_path, monkeypatch, capsys):
    # Zero errors in 40 attempts is inside the envelope, so this smoke run is
    # rejected for being a smoke run, not for its rates.
    _inject_driver(monkeypatch, events=0)
    exit_code = analysis_calibration.main(
        ["--output-root", str(tmp_path / "smoke"), "--repetitions", "40"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["diagnostic_checks_passed"] is True
    assert payload["complete_run"] is False
    assert any("smoke run" in reason for reason in payload["eligibility_reasons"])


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
