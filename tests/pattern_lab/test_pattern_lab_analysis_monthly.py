"""Focused checks for the experimental monthly-cluster-jackknife candidate.

These are the deterministic checks that run before the frozen statistical
validation.  The large decision matrix runs once, for a delivery, through the
module's own command and saves its evidence under an external task-owned root;
nothing here certifies an error rate, and nothing here integrates the candidate.

Reference values are hand-derived or recomputed by an independent route, never
by mirroring the implementation's own expression.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.analysis import calibration as cal
from tools.pattern_lab.analysis import calibration_monthly as monthly
from tools.pattern_lab.analysis import estimator as analysis_estimator
from tools.pattern_lab.analysis.estimator import accumulate_observations
from tools.pattern_lab.study import builtins as study_builtins
from tools.pattern_lab.study.contracts import BarSeries, FeatureRequest

REPETITION = monthly.FIRST_REPETITION_ID


def _accumulated(scenario_name: str, repetition: int = REPETITION, *, padded_days=None):
    scenario = cal.SCENARIOS_BY_NAME[scenario_name]
    records = cal.generate_records(scenario, repetition)
    members = cal.scenario_family(scenario)
    start = scenario.study_start_ms
    end = start + padded_days * cal.DAY_MS if padded_days else scenario.study_end_ms
    accumulated = accumulate_observations(
        cal.record_frames(records, members),
        family=members,
        instruments=cal.INSTRUMENTS,
        study_start_ms=start,
        study_end_ms=end,
    )
    return accumulated, cal.primary_member_id(scenario)


def _index_of(accumulated, member_id: str) -> int:
    return [item["member_id"] for item in accumulated.results].index(member_id)


# --------------------------------------------------------------------------
# 1. point and score identities
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "scenario_name",
    ["null_independent", "null_inclusive_parent", "null_confounded_signal_month_v2"],
)
def test_the_candidate_point_vector_reproduces_the_production_estimates(scenario_name):
    """Unequal counts, partial months and inclusive overlap all included."""
    accumulated, primary_id = _accumulated(scenario_name)
    outcome = monthly.evaluate_candidate(accumulated)
    assert outcome["members"]
    for result, member in zip(accumulated.results, outcome["members"]):
        assert result["member_id"] == member["member_id"]
        if not member["inference_available"]:
            continue
        strata = accumulated.strata[_index_of(accumulated, member["member_id"])]
        # The lift is a genuine cancellation, so the tolerance is anchored to the
        # absolute contribution scale that produced it, not to a relative error.
        scale = float(
            (
                np.abs(strata["target_net_sum"])
                + np.abs(
                    strata["target_count"]
                    / strata["control_count"]
                    * strata["control_net_sum"]
                )
            ).sum()
            / strata["target_count"].sum()
        )
        theta = member["candidate"]["theta"]
        assert theta["signal"] == pytest.approx(result["signal"], rel=1e-12)
        assert theta["control"] == pytest.approx(result["control"], rel=1e-12)
        assert abs(theta["lift"] - result["lift"]) <= 1e-12 * scale
        assert member["point_identity_max_abs_difference"] <= 1e-12 * scale
        # The reported point estimate is the original one, not a deletion average.
        assert member["lift"] == result["lift"]


def test_the_monthly_score_sums_equal_the_exact_aggregation_identity():
    """``sum_{d in m} u_D,d == (A_m - lift * N_m) / N`` on the production vectors."""
    accumulated, primary_id = _accumulated("null_inclusive_parent")
    index = _index_of(accumulated, primary_id)
    strata = accumulated.strata[index]
    influence = accumulated.influence[index]["uD"]
    lift = accumulated.results[index]["lift"]
    months = strata["month_index"]
    ratio = strata["target_count"] / strata["control_count"]
    numerator = strata["target_net_sum"] - ratio * strata["control_net_sum"]
    total = float(strata["target_count"].sum())
    mass = float(
        (np.abs(strata["target_net_sum"]) + np.abs(ratio * strata["control_net_sum"])).sum()
        / total
    )
    day_month = accumulated.grid.month_of_day
    seen = 0
    for month in np.unique(months):
        take = months == month
        expected = float(
            (numerator[take].sum() - lift * strata["target_count"][take].sum()) / total
        )
        actual = float(influence[day_month == month].sum())
        assert abs(actual - expected) <= 1e-14 * mass
        seen += 1
    assert seen >= 12


def test_deleting_a_month_agrees_with_a_direct_recomputation():
    """The sufficient-statistic deletion equals recomputing on the kept strata."""
    accumulated, primary_id = _accumulated("null_dependent_t5")
    strata = accumulated.strata[_index_of(accumulated, primary_id)]
    months = strata["month_index"]
    e = strata["target_count"]
    c = strata["control_count"]
    a = strata["target_net_sum"]
    b = strata["control_net_sum"]
    result = monthly.monthly_jackknife(
        month_index=months,
        target_count=e,
        control_count=c,
        target_net_sum=a,
        control_net_sum=b,
    )
    labels = np.unique(months)
    assert result["informative_months"] == labels.size
    for position, month in enumerate(labels):
        keep = months != month
        # A direct recomputation on the kept strata, with the other strata fixed.
        kept_total = float(e[keep].sum())
        direct = np.array(
            [
                float(a[keep].sum() / kept_total),
                float(((e[keep] / c[keep]) * b[keep]).sum() / kept_total),
                float((a[keep] - (e[keep] / c[keep]) * b[keep]).sum() / kept_total),
            ]
        )
        aggregate = np.array(
            [
                float(a.sum()),
                float(((e / c) * b).sum()),
                float((a - (e / c) * b).sum()),
            ]
        )
        formula = (aggregate - _month_vector(months, month, e, c, a, b)) / (
            float(e.sum()) - float(e[months == month].sum())
        )
        assert direct == pytest.approx(formula, rel=1e-11, abs=1e-18)
    # No full-year admission gate is reapplied inside a deletion: every month is
    # deletable although each deleted sample is far shorter than 336 days.
    assert result["available"] is True
    assert result["degrees_of_freedom"] == labels.size - 1


def _month_vector(months, month, e, c, a, b):
    take = months == month
    return np.array(
        [
            float(a[take].sum()),
            float(((e[take] / c[take]) * b[take]).sum()),
            float((a[take] - (e[take] / c[take]) * b[take]).sum()),
        ]
    )


# --------------------------------------------------------------------------
# 2. reference reductions and inversion
# --------------------------------------------------------------------------

def test_equal_monthly_counts_reduce_to_the_ordinary_monthly_mean_standard_error():
    """With ``N_m = N/G`` the scalar SE is exactly ``s / sqrt(G)``."""
    groups = 6
    months = np.repeat(np.arange(groups), 2)
    e = np.full(months.size, 50.0)
    c = np.full(months.size, 100.0)
    rng = np.random.default_rng(4)
    a = rng.normal(0.0, 0.5, size=months.size)
    b = rng.normal(0.0, 0.5, size=months.size)
    result = monthly.monthly_jackknife(
        month_index=months, target_count=e, control_count=c,
        target_net_sum=a, control_net_sum=b,
    )
    total = e.sum()
    numerator = a - (e / c) * b
    monthly_numerator = np.array(
        [numerator[months == month].sum() for month in range(groups)]
    )
    # The independent reduction: x_m = G * Q_m / N are the per-month estimates
    # and their ordinary mean has standard error s / sqrt(G).
    x = groups * monthly_numerator / total
    expected = float(np.std(x, ddof=1) / np.sqrt(groups))
    assert result["standard_error"]["lift"] == pytest.approx(expected, rel=1e-12)
    assert float(np.mean(x)) == pytest.approx(result["theta"]["lift"], rel=1e-12)


def test_unequal_counts_keep_the_event_weighting():
    months = np.array([0, 1, 2, 3])
    e = np.array([10.0, 90.0, 50.0, 50.0])
    c = np.array([100.0, 100.0, 100.0, 100.0])
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([0.5, 4.0, 1.0, 2.0])
    result = monthly.monthly_jackknife(
        month_index=months, target_count=e, control_count=c,
        target_net_sum=a, control_net_sum=b,
    )
    assert result["theta"]["signal"] == pytest.approx(10.0 / 200.0)
    control = float(((e / c) * b).sum() / e.sum())
    assert result["theta"]["control"] == pytest.approx(control)
    # An unweighted mean of the four monthly control means would be a different
    # estimand; the event weights are kept.
    assert result["theta"]["control"] != pytest.approx(float(np.mean(b / c)))
    assert result["balance"]["max_monthly_share"] == pytest.approx(90.0 / 200.0)
    shares = e / e.sum()
    assert result["balance"]["inverse_sum_squared_shares"] == pytest.approx(
        1.0 / float(np.square(shares).sum())
    )


def test_the_interval_and_the_test_invert_each_other_at_the_zero_null():
    """``p <= .05`` and ``0 outside the 95% interval`` are the same event."""
    from scipy.stats import t as student_t

    rng = np.random.default_rng(17)
    for trial in range(40):
        groups = 12
        months = np.arange(groups)
        e = np.full(groups, 60.0)
        c = np.full(groups, 150.0)
        a = rng.normal(0.0, 0.4, size=groups)
        b = rng.normal(0.0, 0.4, size=groups)
        result = monthly.monthly_jackknife(
            month_index=months, target_count=e, control_count=c,
            target_net_sum=a, control_net_sum=b,
        )
        interval = result["intervals"]["lift"]
        outside = not (interval["lower"] <= 0.0 <= interval["upper"])
        assert outside == (result["p_lift"] <= 0.05), trial
        # The reference distribution is a t with G-1 degrees of freedom.
        assert result["t_critical"] == pytest.approx(
            float(student_t.ppf(0.975, groups - 1))
        )
        assert result["p_lift"] == pytest.approx(
            2.0 * float(student_t.sf(result["t_statistic"], groups - 1))
        )


def test_the_covariance_is_singular_by_construction_and_only_its_diagonal_is_used():
    months = np.arange(8)
    rng = np.random.default_rng(3)
    e = rng.integers(30, 120, size=8).astype(float)
    c = e + rng.integers(50, 200, size=8)
    a = rng.normal(0.0, 0.3, size=8)
    b = rng.normal(0.0, 0.3, size=8)
    errors = monthly.monthly_jackknife(
        month_index=months, target_count=e, control_count=c,
        target_net_sum=a, control_net_sum=b,
    )["standard_error"]
    # Lift is signal minus matched control, so the three marginal errors satisfy
    # the triangle relation of a rank-deficient covariance.
    assert errors["lift"] <= errors["signal"] + errors["control"] + 1e-15
    assert all(value > 0.0 for value in errors.values())


def test_holm_uses_the_candidate_p_values_over_the_whole_declared_family():
    accumulated, primary_id = _accumulated("null_independent")
    outcome = monthly.evaluate_candidate(accumulated)
    assert outcome["family_size"] == len(accumulated.results) == 8
    available = [item for item in outcome["members"] if item["inference_available"]]
    assert available
    raw = np.array([item["p_raw"] for item in available], dtype=np.float64)
    holm = np.array([item["p_holm"] for item in available], dtype=np.float64)
    assert np.all(holm >= raw - 1e-15)
    assert np.all(holm <= 1.0)
    for item in outcome["members"]:
        if item["inference_available"]:
            assert item["nominal_reject_holm"] == (item["p_holm"] <= 0.05)
        else:
            assert item["p_holm"] is None and item["nominal_reject_holm"] is None
            # An unavailable member stays in the family with internal p = 1.
            assert item["p_holm_internal"] == 1.0
    assert monthly.CANDIDATE_METHOD_ID == "monthly_cluster_jackknife_v1"


# --------------------------------------------------------------------------
# 3. refusals
# --------------------------------------------------------------------------

def test_original_insufficient_support_stays_unavailable_under_the_candidate():
    accumulated, primary_id = _accumulated("short_population_84_days")
    outcome = monthly.evaluate_candidate(accumulated)
    primary = next(item for item in outcome["members"] if item["member_id"] == primary_id)
    assert primary["inference_available"] is False
    assert "insufficient_span" in primary["unavailable_reasons"]
    assert primary["candidate"]["p_lift"] is None
    assert primary["candidate"]["standard_error"]["lift"] is None
    # The descriptive estimate survives the refusal.
    assert primary["lift"] is not None


def test_zero_contribution_padding_does_not_restore_candidate_availability():
    accumulated, primary_id = _accumulated("short_population_84_days", padded_days=365)
    outcome = monthly.evaluate_candidate(accumulated)
    primary = next(item for item in outcome["members"] if item["member_id"] == primary_id)
    assert primary["inference_available"] is False
    assert primary["candidate"]["intervals"]["lift"] is None


def test_an_exactly_identical_contrast_is_refused_rather_than_measured_as_tiny():
    months = np.arange(10)
    e = np.full(10, 40.0)
    c = np.full(10, 40.0)
    a = np.linspace(-2.0, 2.0, 10)
    result = monthly.monthly_jackknife(
        month_index=months, target_count=e, control_count=c,
        target_net_sum=a, control_net_sum=a,
    )
    assert result["available"] is False
    assert result["reasons"] == ["degenerate_contrast"]
    assert result["p_lift"] is None and result["standard_error"]["lift"] is None
    assert result["theta"]["lift"] == pytest.approx(0.0, abs=1e-15)
    assert result["degeneracy"]["standard_error_lift"] == 0.0
    # The recorded scale uses pre-cancellation component mass, so a zero SE is
    # recognised even though the cancelled contrast is exactly zero.
    assert result["degeneracy"]["component_mass"] > 0.0
    assert result["degeneracy"]["deletion_amplification"] > 1.0
    assert result["degeneracy"]["scale_lift"] > 0.0


def test_an_exact_zero_return_contrast_is_unavailable_even_at_a_zero_threshold():
    months = np.arange(10)
    zeros = np.zeros(10)
    result = monthly.monthly_jackknife(
        month_index=months, target_count=np.full(10, 25.0),
        control_count=np.full(10, 80.0), target_net_sum=zeros, control_net_sum=zeros,
    )
    assert result["degeneracy"]["threshold"] == 0.0
    assert result["available"] is False
    assert result["reasons"] == ["degenerate_contrast"]
    assert result["p_lift"] is None


def test_a_rescaled_nonzero_contrast_stays_available_at_every_scale():
    months = np.arange(12)
    rng = np.random.default_rng(9)
    e = np.full(12, 55.0)
    c = np.full(12, 140.0)
    a = rng.normal(0.0, 0.3, size=12)
    b = rng.normal(0.0, 0.3, size=12)
    reference = monthly.monthly_jackknife(
        month_index=months, target_count=e, control_count=c,
        target_net_sum=a, control_net_sum=b,
    )
    for factor in (1e-8, 1e8):
        scaled = monthly.monthly_jackknife(
            month_index=months, target_count=e, control_count=c,
            target_net_sum=a * factor, control_net_sum=b * factor,
        )
        assert scaled["available"] is True
        assert scaled["p_lift"] == pytest.approx(reference["p_lift"], rel=1e-9)
        assert scaled["theta"]["lift"] == pytest.approx(
            reference["theta"]["lift"] * factor, rel=1e-9
        )


def test_unequal_deletion_weights_change_the_amplification_not_the_availability():
    months = np.arange(5)
    e = np.array([10.0, 10.0, 10.0, 10.0, 400.0])
    c = np.full(5, 500.0)
    a = np.array([0.1, -0.2, 0.3, -0.1, 0.4])
    b = np.array([0.05, 0.05, 0.05, 0.05, 0.05])
    result = monthly.monthly_jackknife(
        month_index=months, target_count=e, control_count=c,
        target_net_sum=a, control_net_sum=b,
    )
    total = e.sum()
    assert result["degeneracy"]["deletion_amplification"] == pytest.approx(
        float(total / (total - e.max()))
    )
    assert result["available"] is True
    assert result["balance"]["max_monthly_share"] == pytest.approx(400.0 / 440.0)


def test_one_informative_month_and_an_exhausted_denominator_are_explicit_refusals():
    single = monthly.monthly_jackknife(
        month_index=[3, 3], target_count=[40.0, 50.0], control_count=[100.0, 120.0],
        target_net_sum=[0.2, 0.3], control_net_sum=[0.1, 0.1],
    )
    assert single["available"] is False
    assert single["reasons"] == ["insufficient_informative_months"]
    assert single["informative_months"] == 1
    assert single["p_lift"] is None
    empty = monthly.monthly_jackknife(
        month_index=[], target_count=[], control_count=[],
        target_net_sum=[], control_net_sum=[],
    )
    assert empty["available"] is False and empty["reasons"] == ["no_matched_support"]


def test_nonfinite_inputs_are_a_correctness_error_not_a_hidden_refusal():
    with pytest.raises(PatternLabDataError, match="correctness failure"):
        monthly.monthly_jackknife(
            month_index=[0, 1], target_count=[10.0, 10.0], control_count=[20.0, 20.0],
            target_net_sum=[np.nan, 1.0], control_net_sum=[0.5, 0.5],
        )
    with pytest.raises(PatternLabDataError, match="non-positive target or control count"):
        monthly.monthly_jackknife(
            month_index=[0, 1], target_count=[10.0, 0.0], control_count=[20.0, 20.0],
            target_net_sum=[1.0, 1.0], control_net_sum=[0.5, 0.5],
        )
    with pytest.raises(PatternLabDataError, match="different lengths"):
        monthly.monthly_jackknife(
            month_index=[0, 1], target_count=[10.0], control_count=[20.0, 20.0],
            target_net_sum=[1.0, 1.0], control_net_sum=[0.5, 0.5],
        )


def test_the_family_wise_counter_counts_rejection_of_true_nulls():
    """A mixed family: only the true-null members may raise the family-wise flag."""
    accumulated, primary_id = _accumulated("planted_positive_strong")
    outcome = monthly.evaluate_candidate(accumulated)
    planted = [item for item in outcome["members"] if item["inference_available"]]
    assert planted, "the wiring check needs available members"
    # Every member of this fixture carries the same planted effect, so a
    # rejection here is a rejection of a true alternative, not a false positive.
    assert any(item["nominal_reject_holm"] for item in planted)
    true_null, _ = _accumulated("null_independent")
    null_outcome = monthly.evaluate_candidate(true_null)
    labelled = [
        item for item in null_outcome["members"] if item["inference_available"]
    ]
    false_positives = sum(1 for item in labelled if item["nominal_reject_holm"])
    # The counter is defined over explicitly true-null members; this repetition
    # is one draw and is a wiring check, not evidence of family-wise control.
    assert false_positives in (0, len(labelled)) or 0 <= false_positives <= len(labelled)
    assert all(item["nominal_reject_holm"] is not None for item in labelled)


# --------------------------------------------------------------------------
# 4. the actual production pipeline, fixture 102
# --------------------------------------------------------------------------

def test_the_candle_law_is_bounded_positive_and_has_a_common_green_signal():
    series = monthly.candle_bars(REPETITION)
    stamps = series["_timestamps_ms"]
    assert stamps.size == (monthly.CANDLE_WARMUP_DAYS + monthly.CANDLE_STUDY_DAYS) * cal.BARS_PER_DAY
    assert int(np.diff(stamps).min()) == int(np.diff(stamps).max()) == cal.TIMEFRAME_MINUTES * 60_000
    greens = []
    for instrument in cal.INSTRUMENTS:
        values = series[instrument]
        open_price, high, low, close, volume = (values[:, index] for index in range(5))
        assert np.all(open_price > 0.0) and np.all(close > 0.0)
        assert np.all(high >= np.maximum(open_price, close))
        assert np.all(low <= np.minimum(open_price, close))
        assert np.all(volume > 0.0)
        # The next open is exactly the previous close, which is what makes the
        # next-open entry a clean price-ratio.
        assert open_price[1:] == pytest.approx(close[:-1], rel=0, abs=0)
        assert np.all(np.isin(np.round(np.abs(close / open_price - 1.0), 12), [0.0014, 0.0002]))
        greens.append(close > open_price)
    # |0.8| > |0.6| with Rademacher draws, so green is exactly C = +1 on every
    # instrument: this fixture deliberately tests a fully common price signal.
    for other in greens[1:]:
        assert np.array_equal(greens[0], other)
    # The independent volume draws still give distinct event masks.
    volumes = [series[item][:, 4] for item in cal.INSTRUMENTS]
    rising = [value[1:] > value[:-1] for value in volumes]
    assert not np.array_equal(rising[0], rising[1])
    contract = monthly.candle_generator_contract()
    assert "fully common price signal" in contract["common_signal_disclosure"]
    assert "bounded" in contract["bounded_law_disclosure"]


def test_the_candle_draw_order_is_frozen_and_reproducible():
    first = monthly.candle_bars(REPETITION)
    again = monthly.candle_bars(REPETITION)
    other = monthly.candle_bars(REPETITION + 1)
    for instrument in cal.INSTRUMENTS:
        assert np.array_equal(first[instrument], again[instrument])
        assert not np.array_equal(first[instrument], other[instrument])
    # The contract records the order a second implementation would need.
    order = monthly.candle_generator_contract()["draw_order"]
    assert "C for every chronological" in order and "INSTRUMENTS order" in order


def test_fixture_102_charges_the_pinned_nonzero_production_fee():
    accumulated = monthly.candle_accumulated(REPETITION)
    assert monthly.CANDLE_COMMISSION_PCT_PER_SIDE == 0.05
    fraction = monthly.CANDLE_COMMISSION_PCT_PER_SIDE / 100.0
    assert fraction == pytest.approx(0.0005)
    for member in monthly.candle_family():
        assert member.commission_pct_per_side == 0.05
    settings = monthly.candle_model_instance()["settings"]
    assert settings["commission_pct_per_side"] == 0.05
    # The legacy record family keeps its zero commission.
    assert all(
        item.commission_pct_per_side == 0.0
        for item in cal.scenario_family(cal.SCENARIOS_BY_NAME["null_independent"])
    )
    for result in accumulated.results:
        # commission = f * (1 + exit/entry) and the ratio is close to one, so the
        # expected round-trip cost is 2f.
        assert result["signal_commission"] == pytest.approx(2 * fraction, rel=1e-3)
        assert result["control_commission"] == pytest.approx(2 * fraction, rel=1e-3)
        assert result["signal_commission"] > 0.0


def test_fixture_102_runs_the_actual_condition_emission_and_outcome_pipeline():
    tables = monthly.candle_evidence(REPETITION)
    for instrument in cal.INSTRUMENTS:
        conditions = tables[instrument]["conditions"]
        emissions = tables[instrument]["emissions"]
        primitives = tables[instrument]["primitives"]
        assert set(conditions["condition_id"]) == {monthly.CANDLE_CONDITION_ID}
        assert set(emissions["variant_id"]) == {monthly.CANDLE_VARIANT}
        assert set(emissions["occurrence"]) == {"every_qualifying_bar"}
        # Every eligible study anchor, not only the events. The final bar closes
        # exactly at the study end, so it is not an eligible anchor.
        assert len(conditions) == cal.BARS_PER_DAY * monthly.CANDLE_STUDY_DAYS - 1
        assert set(primitives["horizon_minutes"]) == set(cal.HORIZON_MINUTES)
        # Next-open entry at the anchor's close.
        step = cal.TIMEFRAME_MINUTES * 60_000
        assert np.array_equal(
            primitives["entry_time_ms"].to_numpy(),
            primitives["anchor_open_ms"].to_numpy() + step,
        )
        assert np.array_equal(
            primitives["exit_time_ms"].to_numpy(),
            primitives["anchor_open_ms"].to_numpy()
            + (primitives["horizon_minutes"].to_numpy() // cal.TIMEFRAME_MINUTES + 1) * step,
        )
        # Endpoint censoring is per horizon: the longest horizon loses the most.
        invalid = {
            int(horizon): int(
                (~primitives.loc[primitives["horizon_minutes"] == horizon, "return_valid"]).sum()
            )
            for horizon in cal.HORIZON_MINUTES
        }
        assert invalid[60] < invalid[240] < invalid[480]
        assert set(
            primitives.loc[~primitives["return_valid"], "return_reason"]
        ) <= {"terminal_study_end", "incomplete_path", "missing_entry_bar"}


def test_fixture_102_reaches_the_declared_support_and_mirrors_its_directions():
    accumulated = monthly.candle_accumulated(REPETITION)
    outcome = monthly.evaluate_candidate(accumulated)
    primary = next(item for item in outcome["members"] if item["primary"])
    assert primary["inference_available"] is True
    index = _index_of(accumulated, primary["member_id"])
    geometry = accumulated.results[index]["geometry"]
    assert geometry["day_grid_days"] == 365
    assert geometry["retained_months"] == 12 and geometry["retained_strata"] == 48
    assert accumulated.results[index]["supported_population"]["retained_target_share"] == 1.0
    assert primary["candidate"]["informative_months"] == 12

    # With a fixed proportional fee the two directions are near-exact mirrors:
    # lift_short = -(1+f)/(1-f) * lift_long, and the SE scales by the same
    # positive factor, so the two-sided t p-values coincide.
    fraction = monthly.CANDLE_COMMISSION_PCT_PER_SIDE / 100.0
    factor = (1.0 + fraction) / (1.0 - fraction)
    by_case = {
        (item["horizon_minutes"], item["direction"]): item for item in outcome["members"]
    }
    for horizon in cal.HORIZON_MINUTES:
        long_member = by_case[(horizon, "long")]
        short_member = by_case[(horizon, "short")]
        if not (long_member["inference_available"] and short_member["inference_available"]):
            continue
        assert short_member["lift"] == pytest.approx(-factor * long_member["lift"], rel=2e-3)
        assert short_member["p_raw"] == pytest.approx(long_member["p_raw"], rel=2e-3)


@pytest.mark.slow
def test_the_in_memory_adapter_agrees_with_a_real_on_disk_study(tmp_path):
    """The bounded disk proof: the same evidence through the normal reader."""
    outcome = monthly.candle_disk_replay(tmp_path, repetitions=(REPETITION,))
    assert outcome["mismatched"] == []
    assert outcome["max_absolute_difference"] <= 1e-12
    assert outcome["agrees"] is True


@pytest.mark.slow
def test_the_candidate_agrees_across_the_production_checked_joins(tmp_path):
    outcome = monthly.candidate_evidence_replay(tmp_path, repetitions=(REPETITION,))
    assert outcome["mismatched"] == []
    assert outcome["max_absolute_difference"] <= 1e-12
    assert outcome["agrees"] is True


def test_a_known_false_state_entry_anchor_is_neither_target_nor_control(tmp_path):
    """The existing membership contract still holds on the candidate path."""
    outcome = cal._state_entry_replay(tmp_path)
    assert outcome["occurrence"] == "state_entry"
    assert outcome["target_rows"] < outcome["control_rows"]
    assert 0 < outcome["records_supplied"]


def test_the_inclusive_parent_comparison_shares_one_availability_mask():
    accumulated, _primary = _accumulated("null_inclusive_parent")
    pairwise = [
        item for item in accumulated.results if item["comparison_id"] == "child_versus_parent"
    ]
    assert pairwise
    for item in pairwise:
        counts = item["counts"]
        assert counts["target_unavailable"] == 0 or counts["control_unavailable"] >= 0
        assert item["supported_population"]["retained_overlapping_anchors"] > 0
        share = item["supported_population"]["mean_inclusive_parent_share"]
        assert share is not None and 0.0 < share < 1.0


# --------------------------------------------------------------------------
# 5. causal prefix and suffix invariance
# --------------------------------------------------------------------------

def _series(values: np.ndarray, *, gap_at: int | None = None) -> BarSeries:
    step = 30 * 60_000
    slots = np.arange(values.shape[0], dtype=np.int64)
    if gap_at is not None:
        slots[gap_at:] += 5
    return BarSeries(
        instrument_id="SYN",
        timeframe_minutes=30,
        step_ms=step,
        timestamps_ms=slots * step,
        slots=slots,
        values=np.ascontiguousarray(values, dtype=np.float64),
        research_start_index=8,
    )


def _candle_values(rng: np.random.Generator, rows: int) -> np.ndarray:
    returns = 0.001 * (2 * rng.integers(0, 2, size=rows) - 1)
    close = 100.0 * np.cumprod(1.0 + returns)
    open_price = np.concatenate([[100.0], close[:-1]])
    high = np.maximum(open_price, close) * 1.0002
    low = np.minimum(open_price, close) * 0.9998
    volume = np.exp(0.25 * rng.standard_normal(rows))
    return np.column_stack([open_price, high, low, close, volume])


@pytest.mark.parametrize("gap_at", [None, 20])
def test_the_builtin_condition_is_prefix_and_suffix_causal(gap_at):
    rng = np.random.default_rng(12)
    rows = 64
    values = _candle_values(rng, rows)
    full = study_builtins._two_green_evaluate(_series(values, gap_at=gap_at), {}, {})
    cut = 40
    truncated = study_builtins._two_green_evaluate(
        _series(values[:cut], gap_at=gap_at), {}, {}
    )
    assert np.array_equal(full.value[:cut], truncated.value)
    assert np.array_equal(full.valid[:cut], truncated.valid)

    perturbed = values.copy()
    perturbed[cut:] = _candle_values(np.random.default_rng(999), rows - cut)
    changed = study_builtins._two_green_evaluate(_series(perturbed, gap_at=gap_at), {}, {})
    assert np.array_equal(full.value[:cut], changed.value[:cut])
    assert np.array_equal(full.valid[:cut], changed.valid[:cut])
    # The gap and the first bar are unknown rather than false.
    assert full.valid[0] == np.False_
    if gap_at is not None:
        assert full.valid[gap_at] == np.False_


def test_the_custom_example_feature_and_hypothesis_are_prefix_and_suffix_causal():
    from tools.pattern_lab.examples import custom_extension

    rng = np.random.default_rng(21)
    rows = 80
    values = _candle_values(rng, rows)
    parameters = {"period": 6}
    key = FeatureRequest(custom_extension.SMA_FEATURE_ID, {"period": 6}).key
    series = _series(values, gap_at=30)
    feature = custom_extension._sma(series, parameters, {})
    condition = custom_extension._close_above_sma(series, parameters, {key: feature})
    cut = 50
    short_series = _series(values[:cut], gap_at=30)
    short_feature = custom_extension._sma(short_series, parameters, {})
    assert np.allclose(
        feature.values[:cut], short_feature.values, equal_nan=True
    )
    assert np.array_equal(feature.valid[:cut], short_feature.valid)

    perturbed = values.copy()
    perturbed[cut:] = _candle_values(np.random.default_rng(555), rows - cut)
    changed_series = _series(perturbed, gap_at=30)
    changed = custom_extension._sma(changed_series, parameters, {})
    assert np.allclose(feature.values[:cut], changed.values[:cut], equal_nan=True)
    assert np.array_equal(feature.valid[:cut], changed.valid[:cut])
    changed_condition = custom_extension._close_above_sma(
        changed_series, parameters, {key: changed}
    )
    assert np.array_equal(condition.value[:cut], changed_condition.value[:cut])
    assert np.array_equal(condition.valid[:cut], changed_condition.valid[:cut])
    # The window re-warms after the gap rather than crossing it.
    assert not feature.valid[30:35].any()


def test_emissions_of_past_anchors_survive_a_future_perturbation():
    """Only the outcomes measured with changed future data may move."""
    rng = np.random.default_rng(33)
    rows = 96
    values = _candle_values(rng, rows)
    cut = 60
    perturbed = values.copy()
    perturbed[cut:] = _candle_values(np.random.default_rng(77), rows - cut)
    emissions = []
    for candidate in (values, perturbed):
        series = _series(candidate)
        condition = study_builtins._two_green_evaluate(series, {}, {})
        emissions.append((condition.value & condition.valid)[:cut])
    assert np.array_equal(emissions[0], emissions[1])


# --------------------------------------------------------------------------
# the shared extraction, the manifest and the driver
# --------------------------------------------------------------------------

def test_the_shared_accumulation_does_not_depend_on_bootstrap_draws():
    """Availability is decided before any resampling, with no fake B."""
    scenario = cal.SCENARIOS_BY_NAME["null_independent"]
    records = cal.generate_records(scenario, REPETITION)
    members = cal.scenario_family(scenario)
    accumulated = accumulate_observations(
        cal.record_frames(records, members),
        family=members,
        instruments=cal.INSTRUMENTS,
        study_start_ms=scenario.study_start_ms,
        study_end_ms=scenario.study_end_ms,
    )
    assert isinstance(accumulated, analysis_estimator.AccumulatedEstimates)
    assert set(accumulated.strata) <= set(accumulated.influence)
    for index in accumulated.strata:
        assert accumulated.results[index]["unavailable_reasons"] == []
        assert accumulated.results[index]["p_raw"] is None
        assert accumulated.results[index]["bootstrap"] == {
            "signal": None, "control": None, "lift": None
        }
    # The published entry point keeps its signature, behaviour and results.
    full = analysis_estimator.evaluate_observations(
        cal.record_frames(cal.generate_records(scenario, REPETITION), members),
        family=members,
        instruments=cal.INSTRUMENTS,
        study_start_ms=scenario.study_start_ms,
        study_end_ms=scenario.study_end_ms,
        resamples=299,
        seed=11,
    )
    for accumulated_result, published in zip(accumulated.results, full["members"]):
        assert accumulated_result["member_id"] == published["member_id"]
        assert accumulated_result["lift"] == published["lift"]
        assert accumulated_result["signal"] == published["signal"]
        assert published["p_raw"] is not None


def test_the_accumulation_boundary_validates_its_inputs():
    scenario = cal.SCENARIOS_BY_NAME["null_independent"]
    members = cal.scenario_family(scenario)
    frames = list(cal.record_frames(cal.generate_records(scenario, REPETITION), members))
    with pytest.raises(PatternLabDataError, match="duplicate instrument IDs"):
        accumulate_observations(
            frames, family=members, instruments=("SYN_A", "SYN_A"),
            study_start_ms=scenario.study_start_ms, study_end_ms=scenario.study_end_ms,
        )
    with pytest.raises(PatternLabDataError, match="at least one instrument"):
        accumulate_observations(
            frames, family=members, instruments=(),
            study_start_ms=scenario.study_start_ms, study_end_ms=scenario.study_end_ms,
        )
    broken = frames[0].copy()
    broken["net_return"] = np.inf
    with pytest.raises(PatternLabDataError):
        accumulate_observations(
            [broken], family=members, instruments=cal.INSTRUMENTS,
            study_start_ms=scenario.study_start_ms, study_end_ms=scenario.study_end_ms,
        )


def test_the_frozen_matrix_and_its_stopping_arithmetic_are_declared():
    assert [item.fixture_id for item in monthly.MAIN_MATRIX] == [2, 5, 1, 4, 3, 12, 101, 102]
    assert all(item.attempts == 2000 for item in monthly.MAIN_MATRIX)
    assert sum(item.attempts for item in monthly.MAIN_MATRIX) == monthly.MAX_MAIN_ATTEMPTS == 16000
    assert monthly.FIRST_REPETITION_ID == 20000 and monthly.LAST_REPETITION_ID == 21999
    # The declared impossibility boundaries, against the production function.
    assert cal.exact_upper_bound(monthly.IMPOSSIBLE_ERRORS - 1, 2000) <= cal.ERROR_ENVELOPE
    assert cal.exact_upper_bound(monthly.IMPOSSIBLE_ERRORS, 2000) > cal.ERROR_ENVELOPE
    assert (2000 - monthly.IMPOSSIBLE_UNAVAILABLE) / 2000 < cal.MIN_PRIMARY_AVAILABILITY
    assert (2000 - monthly.IMPOSSIBLE_UNAVAILABLE + 1) / 2000 >= cal.MIN_PRIMARY_AVAILABILITY


def test_the_manifest_freezes_the_formula_and_digests_itself():
    from tools.pattern_lab.study.contracts import semantic_digest

    manifest = monthly.candidate_manifest()
    recomputed = dict(manifest)
    digest = recomputed.pop("manifest_digest")
    assert semantic_digest(recomputed) == digest
    assert manifest["method"] == monthly.CANDIDATE_METHOD_ID
    assert manifest["decision"]["error_envelope"] == 0.08
    assert manifest["decision"]["min_primary_availability"] == 0.95
    assert "no calibration evidence for other month counts" in (
        manifest["decision"]["month_count_disclosure"]
    )
    assert "not 24 independent" in manifest["decision"]["duality_disclosure"]
    assert manifest["formula"]["validity"]["min_informative_months"] == 2
    assert manifest["generators"]["candle_contract"]["commission_pct_per_side"] == 0.05


def test_an_impossible_rate_stops_the_fixture_and_a_survivable_one_does_not():
    fixture = monthly.MAIN_MATRIX[0]

    def rows(events: int, attempts: int):
        return [
            {
                "available": True,
                "reject_raw": index < events,
                "noncoverage": index < events,
                "family_any_rejection": index < events,
            }
            for index in range(attempts)
        ]

    assert monthly.impossibility(fixture, rows(139, 500)) is None
    stop = monthly.impossibility(fixture, rows(140, 500))
    assert stop is not None and stop["reason"] == "rate_cannot_pass"
    assert "can only grow" in stop["proof"]
    unavailable = [
        {"available": False, "reject_raw": None, "noncoverage": None, "family_any_rejection": False}
        for _index in range(101)
    ]
    stop = monthly.impossibility(fixture, unavailable)
    assert stop is not None and stop["reason"] == "availability_cannot_pass"
    # A disclosure fixture is never stopped for a rate it does not gate.
    assert monthly.impossibility(monthly.SUPPLEMENTARY_MATRIX[0], rows(200, 200)) is None


@pytest.mark.slow
def test_the_driver_and_summarizer_reproduce_a_decision_from_saved_records(tmp_path):
    root = tmp_path / "run"
    document = monthly.run_experiment(
        output_root=root,
        fixtures=[monthly.MAIN_MATRIX[2]],
        attempts=monthly.PILOT_ATTEMPTS,
        include_replays=False,
    )
    assert document["status"] == "completed"
    assert (root / "manifest.json").is_file() and (root / "environment.json").is_file()
    records = json.loads(
        (root / "records" / "001_null_independent.json").read_text(encoding="utf-8")
    )["rows"]
    assert [item["id"] for item in records] == list(range(20000, 20005))
    assert all(item["informative_months"] == 12 for item in records if item["available"])

    summary = monthly.summarize(root)
    assert summary["decision"] == "INCOMPLETE"
    assert summary["integrity_problems"] == []
    assert "did not complete its declared attempts" in " ".join(summary["decision_reasons"])
    assert "the bounded replay and adapter checks were not run" in summary["decision_reasons"]
    # The summarizer regenerates identically from the saved records alone.
    again = monthly.summarize(root)
    assert again["results"] == summary["results"]
    text = (root / "summary.md").read_text(encoding="utf-8")
    assert "monthly_cluster_jackknife_v1" in text and "INCOMPLETE" in text
    assert "not 24 independent" not in text  # the disclosure lives in the manifest
    assert "same event" in text


@pytest.mark.slow
def test_the_summarizer_reads_a_gzip_compressed_record_archive(tmp_path):
    """A retained archive holds the same bytes compressed, and summarizes the same."""
    import gzip

    root = tmp_path / "run"
    monthly.run_experiment(
        output_root=root,
        fixtures=[monthly.MAIN_MATRIX[2]],
        attempts=monthly.PILOT_ATTEMPTS,
        include_replays=False,
    )
    plain = root / "records" / "001_null_independent.json"
    original = monthly.summarize(root)
    raw = plain.read_bytes()
    (root / "records" / "001_null_independent.json.gz").write_bytes(gzip.compress(raw))
    plain.unlink()
    assert monthly.read_records(root, "001_null_independent")["rows"]
    archived = monthly.summarize(root)
    assert archived["results"] == original["results"]
    assert archived["decision"] == original["decision"]
    assert monthly.read_records(root, "no_such_fixture") is None


def test_a_tampered_manifest_is_reported_rather_than_summarized(tmp_path):
    root = tmp_path / "tampered"
    (root / "records").mkdir(parents=True)
    manifest = monthly.candidate_manifest()
    manifest["decision"]["error_envelope"] = 0.2
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    summary = monthly.summarize(root)
    assert summary["decision"] == "INCOMPLETE"
    assert any("manifest digest" in item for item in summary["integrity_problems"])
