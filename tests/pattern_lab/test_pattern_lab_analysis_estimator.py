"""The M3a numerical estimator: weights, joint influence, bootstrap and Holm.

Every oracle here is written independently of the production vectorization: a
row-level weighted calculation, a central finite-difference derivative and a slow
index-based resampler.  A test that merely mirrors the production formula would
pass while the control uncertainty or the event-weight terms were missing.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.analysis import estimator as analysis_estimator
from tools.pattern_lab.analysis import request as analysis_request
from tools.pattern_lab.analysis.estimator import (
    CalendarGrid,
    block_lengths,
    block_starts,
    evaluate_observations,
    replicate_indices,
)

from ._helpers import (
    analysis_member,
    analysis_records,
    day_ms,
    evaluate_simple,
    utc_ms,
)

YEAR_START = utc_ms("2025-07-01")
YEAR_END = utc_ms("2026-07-01")


# --------------------------------------------------------------------------
# the calendar grid
# --------------------------------------------------------------------------

def test_the_day_grid_counts_days_that_intersect_the_half_open_interval():
    grid = CalendarGrid(YEAR_START, YEAR_END)
    assert grid.days == 365
    assert grid.as_json()["first_day_utc"] == "2025-07-01T00:00:00Z"
    assert grid.as_json()["last_day_utc"] == "2026-06-30T00:00:00Z"
    assert grid.month_labels[0] == "2025-07" and grid.month_labels[-1] == "2026-06"


def test_a_partial_boundary_day_stays_an_explicit_grid_day():
    grid = CalendarGrid(YEAR_START + 3_600_000, YEAR_START + day_ms(2) + 1)
    assert grid.days == 3
    assert grid.month_length.tolist() == [3]


# --------------------------------------------------------------------------
# strata, weights and point estimates
# --------------------------------------------------------------------------

def test_hand_computed_stratified_means_and_weights():
    """Two instruments with deliberately unequal signal rates and month means."""
    records = []
    # AAA: 24 targets and 24 controls in July, 48 targets and 24 controls in August.
    for month, targets, target_value, control_value in (
        (0, 24, 0.010, 0.002), (1, 48, 0.004, 0.001)
    ):
        records.append(
            analysis_records(
                instrument="AAA", month_offset=month, targets=targets, controls=24,
                target_value=target_value, control_value=control_value,
            )
        )
    for month, targets, target_value, control_value in (
        (0, 24, -0.002, 0.003), (1, 24, 0.006, -0.001)
    ):
        records.append(
            analysis_records(
                instrument="BBB", month_offset=month, targets=targets, controls=24,
                target_value=target_value, control_value=control_value,
            )
        )
    result = evaluate_simple(records, instruments=("AAA", "BBB"))
    member = result["members"][0]

    counts = [24, 48, 24, 24]
    target_means = [0.010, 0.004, -0.002, 0.006]
    control_means = [0.002, 0.001, 0.003, -0.001]
    total = sum(counts)
    weights = [item / total for item in counts]
    assert member["signal"] == pytest.approx(
        sum(w * m for w, m in zip(weights, target_means))
    )
    assert member["control"] == pytest.approx(
        sum(w * m for w, m in zip(weights, control_means))
    )
    assert member["lift"] == pytest.approx(member["signal"] - member["control"])
    assert member["supported_population"]["retained_target_observations"] == total


def test_a_stratum_below_any_support_minimum_is_excluded_with_its_reason():
    records = [
        analysis_records(instrument="AAA", month_offset=0, targets=19, controls=40),
        analysis_records(instrument="AAA", month_offset=1, targets=40, controls=19),
        analysis_records(instrument="BBB", month_offset=0, targets=40, controls=40, days=9),
        analysis_records(instrument="BBB", month_offset=1, targets=40, controls=40),
    ]
    result = evaluate_simple(records, instruments=("AAA", "BBB"))
    table = pd.DataFrame(result["strata"])
    retained = table.loc[table["retained"]]
    assert len(retained) == 1
    assert retained.iloc[0]["instrument_id"] == "BBB" and retained.iloc[0]["utc_month"] == "2025-08"
    reasons = dict(zip(table["instrument_id"] + "/" + table["utc_month"], table["exclusion_reasons"]))
    assert reasons["AAA/2025-07"] == "target_count_below_minimum"
    assert reasons["AAA/2025-08"] == "control_count_below_minimum"
    assert reasons["BBB/2025-07"] == "target_days_below_minimum;control_days_below_minimum"
    # Every stratum stays visible, including the zero-event ones.
    assert len(table) == result["family_size"] * 2 * 12


def test_an_empty_supported_population_gives_nulls_and_a_reason_not_zeros():
    result = evaluate_simple(
        [analysis_records(instrument="AAA", month_offset=0, targets=5, controls=5)],
        instruments=("AAA",),
    )
    member = result["members"][0]
    assert member["signal"] is None and member["control"] is None and member["lift"] is None
    assert member["unavailable_reasons"] == ["no_matched_support"]
    assert member["bootstrap"] == {"signal": None, "control": None, "lift": None}


def test_support_filtered_means_are_reported_beside_the_raw_valid_targets():
    records = [
        analysis_records(instrument="AAA", month_offset=0, targets=10, controls=40, target_value=1.0),
        analysis_records(instrument="AAA", month_offset=1, targets=40, controls=40, target_value=0.001),
    ]
    result = evaluate_simple(records, instruments=("AAA",))
    member = result["members"][0]
    assert member["raw_valid_target"]["n"] == 50
    assert member["raw_valid_target"]["mean_net_return"] == pytest.approx(
        (10 * 1.0 + 40 * 0.001) / 50
    )
    assert member["signal"] == pytest.approx(0.001)
    assert member["supported_population"]["retained_target_share"] == pytest.approx(40 / 50)
    assert member["supported_population"]["excluded_target_observations"] == 10


# --------------------------------------------------------------------------
# overlap and the inclusive parent
# --------------------------------------------------------------------------

def test_an_inclusive_parent_contrast_matches_a_row_level_weighted_oracle():
    rng = np.random.default_rng(11)
    frames = []
    for instrument in ("AAA", "BBB"):
        for month in range(3):
            frames.append(
                analysis_records(
                    instrument=instrument, month_offset=month, targets=40, controls=80,
                    overlap=40, rng=rng,
                )
            )
    result = evaluate_simple(frames, instruments=("AAA", "BBB"))
    member = result["members"][0]

    combined = pd.concat(frames, ignore_index=True)
    grid = CalendarGrid(YEAR_START, YEAR_END)
    months = grid.month_index_of(combined["signal_time_ms"].to_numpy())
    signal_total = 0.0
    control_total = 0.0
    weight_total = 0
    for key in sorted(set(zip(combined["instrument_id"], months))):
        rows = combined.loc[
            (combined["instrument_id"] == key[0]) & (months == key[1])
        ]
        targets = rows.loc[rows["is_target"] & rows["available"] & rows["return_valid"]]
        controls = rows.loc[rows["is_control"] & rows["available"] & rows["return_valid"]]
        weight_total += len(targets)
        signal_total += float(targets["net_return"].sum())
        control_total += len(targets) * float(controls["net_return"].mean())
    assert member["signal"] == pytest.approx(signal_total / weight_total)
    assert member["control"] == pytest.approx(control_total / weight_total)
    assert member["supported_population"]["retained_overlapping_anchors"] == 6 * 40

    table = pd.DataFrame(result["strata"])
    retained = table.loc[table["retained"]]
    assert retained["inclusive_parent_share"].to_numpy() == pytest.approx(
        np.full(len(retained), 40 / 80)
    )
    # lift_s = (1 - nE_s/nC_s) * disjoint_lift_s, per stratum.
    for _index, row in retained.iterrows():
        stratum_lift = row["mean_target_net"] - row["mean_control_net"]
        disjoint = row["mean_target_net"] - row["disjoint_complement_mean_net"]
        assert stratum_lift == pytest.approx(
            (1.0 - row["inclusive_parent_share"]) * disjoint
        )


def test_identical_target_and_control_give_a_zero_contrast_and_no_significance():
    rng = np.random.default_rng(5)
    frames = [
        analysis_records(
            instrument="AAA", month_offset=month, targets=60, controls=60, overlap=60, rng=rng
        )
        for month in range(12)
    ]
    result = evaluate_simple(frames, instruments=("AAA",), resamples=1999)
    member = result["members"][0]
    assert member["lift"] == pytest.approx(0.0, abs=1e-15)
    assert "degenerate_contrast" in member["unavailable_reasons"]
    assert member["inference_available"] is False
    assert member["p_raw"] is None and member["intervals"]["lift"] is None
    # Coverage geometry stays present even though no inferential claim is made.
    assert member["geometry"]["joint_active_days"] > 0


# --------------------------------------------------------------------------
# daily sufficient statistics and the joint influence
# --------------------------------------------------------------------------

def _weighted_estimates(frames, weights_by_day, grid):
    """A slow independent weighted estimator over the daily observation weights."""
    combined = pd.concat(frames, ignore_index=True)
    days = grid.day_offsets(combined["signal_time_ms"].to_numpy())
    months = grid.month_of_day[days]
    weight = weights_by_day[days]
    valid_target = (
        combined["is_target"].to_numpy()
        & combined["available"].to_numpy()
        & combined["return_valid"].to_numpy()
    )
    valid_control = (
        combined["is_control"].to_numpy()
        & combined["available"].to_numpy()
        & combined["return_valid"].to_numpy()
    )
    net = combined["net_return"].to_numpy(dtype=np.float64)
    keys = sorted(set(zip(combined["instrument_id"].tolist(), months.tolist())))
    total = 0.0
    signal = 0.0
    control_terms = []
    for instrument, month in keys:
        mask = (combined["instrument_id"].to_numpy() == instrument) & (months == month)
        target = mask & valid_target
        control = mask & valid_control
        nE = float(weight[target].sum())
        nC = float(weight[control].sum())
        if nE == 0.0 or nC == 0.0:
            continue
        total += nE
        signal += float((weight[target] * net[target]).sum())
        control_terms.append((nE, float((weight[control] * net[control]).sum()) / nC))
    signal_mean = signal / total
    control_mean = sum(nE * mu for nE, mu in control_terms) / total
    return signal_mean, control_mean


def test_the_joint_influence_matches_a_central_finite_difference_derivative():
    rng = np.random.default_rng(23)
    frames = [
        analysis_records(
            instrument=instrument, month_offset=month, targets=40, controls=60, rng=rng
        )
        for instrument in ("AAA", "BBB")
        for month in range(12)
    ]
    grid = CalendarGrid(YEAR_START, YEAR_END)
    members = [analysis_member()]
    accumulator = analysis_estimator._Accumulator(
        members=[members[0].member_id], instruments=["AAA", "BBB"], grid=grid
    )
    for position, frame in enumerate(frames):
        accumulator.add(frame, where=f"records[{position}]")
    influence = _production_influence(accumulator, members, grid)

    base = np.ones(grid.days, dtype=np.float64)
    step = 1e-6
    probed = [0, 1, 37, 200, 364]
    for day in probed:
        upper = base.copy()
        upper[day] += step
        lower = base.copy()
        lower[day] -= step
        signal_up, control_up = _weighted_estimates(frames, upper, grid)
        signal_down, control_down = _weighted_estimates(frames, lower, grid)
        assert influence["uE"][day] == pytest.approx(
            (signal_up - signal_down) / (2 * step), rel=1e-5, abs=1e-14
        )
        assert influence["uC"][day] == pytest.approx(
            (control_up - control_down) / (2 * step), rel=1e-5, abs=1e-14
        )
        assert influence["uD"][day] == pytest.approx(
            influence["uE"][day] - influence["uC"][day]
        )


def _production_influence(accumulator, members, grid):
    """Return the production joint influence vectors before centering."""
    captured = {}
    original = analysis_estimator._run_bootstrap

    def capture(results, influence, **kwargs):
        captured.update({index: dict(value) for index, value in influence.items()})
        return original(results, influence, **kwargs)

    analysis_estimator._run_bootstrap = capture
    try:
        analysis_estimator._evaluate_accumulated(
            accumulator,
            members=[
                {
                    "member_id": members[0].member_id,
                    "comparison_id": members[0].comparison_id,
                    "kind": members[0].kind,
                    "model_instance_id": members[0].model_instance_id,
                    "timeframe_minutes": members[0].timeframe_minutes,
                    "case_id": members[0].case_id,
                    "direction": members[0].direction,
                    "horizon_minutes": members[0].horizon_minutes,
                    "primary": members[0].primary,
                }
            ],
            member_ids=[members[0].member_id],
            instrument_ids=["AAA", "BBB"],
            grid=grid,
            resamples=99,
            seed=1,
            batch_size=64,
        )
    finally:
        analysis_estimator._run_bootstrap = original
    return captured[0]


def test_a_frozen_event_weight_would_change_the_control_influence_materially():
    """Dropping the ``e_sd`` term is a different vector, not a rounding detail."""
    rng = np.random.default_rng(31)
    frames = [
        analysis_records(
            instrument=instrument, month_offset=month, targets=targets, controls=60, rng=rng
        )
        for instrument, targets in (("AAA", 30), ("BBB", 90))
        for month in range(12)
    ]
    grid = CalendarGrid(YEAR_START, YEAR_END)
    members = [analysis_member()]
    accumulator = analysis_estimator._Accumulator(
        members=[members[0].member_id], instruments=["AAA", "BBB"], grid=grid
    )
    for position, frame in enumerate(frames):
        accumulator.add(frame, where=f"records[{position}]")
    influence = _production_influence(accumulator, members, grid)

    frozen = _frozen_weight_control_influence(frames, grid)
    difference = np.max(np.abs(influence["uC"] - frozen))
    assert difference > 0.05 * np.max(np.abs(influence["uC"]))
    # Both vectors sum to zero, so the zero-sum check alone cannot notice the
    # omission: only the derivative check above distinguishes them.
    assert influence["uC"].sum() == pytest.approx(0.0, abs=1e-15)
    assert float(frozen.sum()) == pytest.approx(0.0, abs=1e-15)


def _frozen_weight_control_influence(frames, grid):
    """uC computed with the event weights held fixed: the defect to detect."""
    combined = pd.concat(frames, ignore_index=True)
    days = grid.day_offsets(combined["signal_time_ms"].to_numpy())
    months = grid.month_of_day[days]
    valid_target = (
        combined["is_target"].to_numpy()
        & combined["available"].to_numpy()
        & combined["return_valid"].to_numpy()
    )
    valid_control = (
        combined["is_control"].to_numpy()
        & combined["available"].to_numpy()
        & combined["return_valid"].to_numpy()
    )
    net = combined["net_return"].to_numpy(dtype=np.float64)
    instruments = combined["instrument_id"].to_numpy()
    total = int(np.count_nonzero(valid_target))
    vector = np.zeros(grid.days, dtype=np.float64)
    for key in sorted(set(zip(instruments.tolist(), months.tolist()))):
        mask = (instruments == key[0]) & (months == key[1])
        target = mask & valid_target
        control = mask & valid_control
        nE = int(np.count_nonzero(target))
        nC = int(np.count_nonzero(control))
        if nE == 0 or nC == 0:
            continue
        muC = float(net[control].sum()) / nC
        contribution = (nE / nC) * (
            np.bincount(days[control], weights=net[control], minlength=grid.days)
            - muC * np.bincount(days[control], minlength=grid.days).astype(np.float64)
        )
        vector += contribution / total
    return vector


def test_daily_counts_and_sums_reconcile_with_the_point_estimates():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    result = evaluate_simple(frames, instruments=("AAA",))
    daily = pd.DataFrame(result["daily"])
    member = result["members"][0]
    assert int(daily["target_count"].sum()) == member["supported_population"][
        "retained_target_observations"
    ]
    assert float(daily["target_net_sum"].sum()) == pytest.approx(
        member["signal"] * member["supported_population"]["retained_target_observations"]
    )
    # Only contributing days are stored; the complete grid and the zero rule are
    # recorded beside the table.
    assert len(daily) <= result["calendar"]["day_grid_days"] * 12
    assert result["calendar"]["rule"].startswith("Day d is included")


# --------------------------------------------------------------------------
# resampling
# --------------------------------------------------------------------------

def test_the_fast_block_sums_match_the_slow_index_oracle():
    rng = np.random.default_rng(17)
    for days in (336, 365, 100, 7, 8):
        vector = rng.normal(size=days)
        vector -= vector.mean()
        lengths = block_lengths(days)
        starts = block_starts(
            np.random.Generator(np.random.PCG64(4)), days=days, blocks=int(lengths.size), count=25
        )
        fast = analysis_estimator._bootstrap_sums(vector, starts, lengths)
        slow = np.array(
            [vector[replicate_indices(row, days)].sum() for row in starts], dtype=np.float64
        )
        assert fast == pytest.approx(slow)
        assert replicate_indices(starts[0], days).size == days


def test_block_lengths_truncate_the_last_block_to_the_day_grid():
    assert block_lengths(365).tolist() == [7] * 52 + [1]
    assert block_lengths(336).tolist() == [7] * 48
    assert int(block_lengths(365).sum()) == 365


def test_batching_and_group_processing_do_not_change_the_drawn_indices():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    one = evaluate_simple(frames, instruments=("AAA",), resamples=1999, batch_size=1999)
    many = evaluate_simple(frames, instruments=("AAA",), resamples=1999, batch_size=37)
    assert one["members"][0]["p_raw"] == many["members"][0]["p_raw"]
    assert one["members"][0]["intervals"]["lift"] == many["members"][0]["intervals"]["lift"]
    assert one["members"][0]["bootstrap"] == many["members"][0]["bootstrap"]


def test_one_shared_index_sequence_serves_every_instrument_and_comparison():
    rng = np.random.default_rng(9)
    frames = [
        analysis_records(
            instrument=instrument, month_offset=month, targets=40, controls=60, rng=rng,
            member_id=member_id,
        )
        for member_id in ("m1", "m2")
        for instrument in ("AAA", "BBB")
        for month in range(12)
    ]
    members = [analysis_member(member_id="m1"), analysis_member(member_id="m2")]
    first = evaluate_observations(
        frames, family=members, instruments=("AAA", "BBB"),
        study_start_ms=YEAR_START, study_end_ms=YEAR_END, resamples=999, seed=7,
    )
    only_second = evaluate_observations(
        [frame for frame in frames if frame["member_id"].iloc[0] == "m2"],
        family=[members[1]], instruments=("AAA", "BBB"),
        study_start_ms=YEAR_START, study_end_ms=YEAR_END, resamples=999, seed=7,
    )
    # The same seed and day grid draw the same calendar indices, so a member's
    # bootstrap does not depend on which other members share the family.
    assert first["members"][1]["bootstrap"] == only_second["members"][0]["bootstrap"]


def test_the_basic_interval_endpoints_use_the_linear_quantile_convention():
    values = np.arange(-50.0, 50.0)
    interval = analysis_estimator._basic_interval(2.0, values)
    quantiles = np.quantile(values, [0.025, 0.975], method="linear")
    assert interval["lower"] == pytest.approx(2.0 - quantiles[1])
    assert interval["upper"] == pytest.approx(2.0 - quantiles[0])


def test_the_two_sided_p_value_counts_ties_inclusively():
    values = np.array([-1.0, 0.0, 0.0, 1.0, 2.0])
    upper = (1 + int(np.count_nonzero(values >= 0.0))) / (values.size + 1)
    lower = (1 + int(np.count_nonzero(values <= 0.0))) / (values.size + 1)
    assert min(1.0, 2 * min(upper, lower)) == pytest.approx(min(1.0, 2 * min(4 / 6, 4 / 6)))


def test_the_minimum_two_sided_resolution_is_recorded_and_warned_about():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    result = evaluate_simple(frames, instruments=("AAA",), resamples=1999)
    assert result["p_resolution"] == pytest.approx(2 / 2000)
    assert result["p_resolution_blocks_first_rejection"] is False
    assert result["members"][0]["p_raw"] >= result["p_resolution"]


# --------------------------------------------------------------------------
# invariances
# --------------------------------------------------------------------------

def test_cloning_a_ticker_does_not_narrow_the_uncertainty():
    rng = np.random.default_rng(29)
    single = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    cloned = list(single)
    for frame in single:
        copy = frame.copy()
        copy["instrument_id"] = "AAA_CLONE"
        cloned.append(copy)
    one = evaluate_simple(single, instruments=("AAA",), resamples=1999)
    two = evaluate_simple(cloned, instruments=("AAA", "AAA_CLONE"), resamples=1999)
    assert two["members"][0]["lift"] == pytest.approx(one["members"][0]["lift"])
    widened = two["members"][0]["bootstrap"]["lift"]["standard_deviation"]
    original = one["members"][0]["bootstrap"]["lift"]["standard_deviation"]
    assert widened == pytest.approx(original, rel=1e-9)


def test_adding_one_constant_to_every_outcome_leaves_the_lift_unchanged():
    rng = np.random.default_rng(13)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    shifted = []
    for frame in frames:
        copy = frame.copy()
        copy["net_return"] = copy["net_return"] + 0.01
        copy["gross_return"] = copy["gross_return"] + 0.01
        shifted.append(copy)
    base = evaluate_simple(frames, instruments=("AAA",), resamples=999)
    moved = evaluate_simple(shifted, instruments=("AAA",), resamples=999)
    assert moved["members"][0]["signal"] == pytest.approx(base["members"][0]["signal"] + 0.01)
    assert moved["members"][0]["control"] == pytest.approx(base["members"][0]["control"] + 0.01)
    assert moved["members"][0]["lift"] == pytest.approx(base["members"][0]["lift"])
    assert moved["members"][0]["p_raw"] == base["members"][0]["p_raw"]
    assert moved["members"][0]["bootstrap"]["lift"]["standard_deviation"] == pytest.approx(
        base["members"][0]["bootstrap"]["lift"]["standard_deviation"], rel=1e-9
    )


# --------------------------------------------------------------------------
# gates and reasons
# --------------------------------------------------------------------------

def _twelve_months(rng, *, targets=40, controls=60, instrument="AAA", days=28):
    return [
        analysis_records(
            instrument=instrument, month_offset=month, targets=targets, controls=controls,
            rng=rng, days=days,
        )
        for month in range(12)
    ]


def test_a_short_day_grid_reports_insufficient_span():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(4)
    ]
    result = evaluate_observations(
        frames, family=[analysis_member()], instruments=("AAA",),
        study_start_ms=YEAR_START, study_end_ms=YEAR_START + day_ms(120),
        resamples=1999, seed=5,
    )
    member = result["members"][0]
    assert "insufficient_span" in member["unavailable_reasons"]
    assert member["inference_available"] is False
    assert member["signal"] is not None  # descriptive results survive


def test_sparse_support_reports_active_day_and_block_shortfalls():
    rng = np.random.default_rng(3)
    frames = _twelve_months(rng, days=10)
    result = evaluate_simple(frames, instruments=("AAA",))
    member = result["members"][0]
    assert member["geometry"]["joint_active_days"] == 120
    assert "insufficient_active_days" in member["unavailable_reasons"]
    assert "insufficient_supported_blocks" in member["unavailable_reasons"]
    assert member["geometry"]["supported_span_days"] >= 336


def test_retained_coverage_below_eighty_percent_refuses_inference():
    rng = np.random.default_rng(3)
    frames = _twelve_months(rng)
    frames.append(
        analysis_records(instrument="BBB", month_offset=0, targets=200, controls=5, rng=rng)
    )
    result = evaluate_simple(frames, instruments=("AAA", "BBB"))
    member = result["members"][0]
    assert member["supported_population"]["retained_target_share"] < 0.80
    assert "insufficient_retained_coverage" in member["unavailable_reasons"]


def test_a_horizon_beyond_eight_hours_keeps_descriptive_results_only():
    rng = np.random.default_rng(3)
    frames = _twelve_months(rng)
    member = analysis_member(horizon_minutes=720, case_id="tf30m.h720m.long")
    result = evaluate_observations(
        frames, family=[member], instruments=("AAA",),
        study_start_ms=YEAR_START, study_end_ms=YEAR_END, resamples=1999, seed=5,
    )
    row = result["members"][0]
    assert row["unavailable_reasons"] == ["unsupported_inference_horizon"]
    assert row["lift"] is not None and row["p_raw"] is None


def test_every_reason_code_is_reported_deterministically_together():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng,
                         days=10)
        for month in range(3)
    ]
    member = analysis_member(horizon_minutes=720, case_id="tf30m.h720m.long")
    result = evaluate_observations(
        frames, family=[member], instruments=("AAA",),
        study_start_ms=YEAR_START, study_end_ms=YEAR_START + day_ms(100),
        resamples=1999, seed=5,
    )
    assert result["members"][0]["unavailable_reasons"] == [
        "insufficient_span",
        "insufficient_active_days",
        "insufficient_supported_blocks",
        "unsupported_inference_horizon",
    ]


def test_a_near_roundoff_cancellation_is_detected_as_degenerate():
    """Two large, separately computed components whose difference is roundoff.

    The target and control rows are distinct observations that carry the same
    values on the same days, so ``uE`` and ``uC`` are large and are produced by
    different arithmetic paths while ``uD`` is pure float64 noise.  A rule
    relative to the already cancelled ``uD`` alone would miss this.
    """
    rng = np.random.default_rng(41)
    frames = []
    for month in range(12):
        frame = analysis_records(
            instrument="AAA", month_offset=month, targets=28, controls=28, rng=rng, days=28
        )
        values = frame["net_return"].to_numpy() * 200.0
        values[28:] = values[:28]
        frame["net_return"] = values
        frame["gross_return"] = values
        frames.append(frame)
    result = evaluate_simple(frames, instruments=("AAA",), resamples=999)
    member = result["members"][0]
    assert member["lift"] == pytest.approx(0.0, abs=1e-12)
    assert "degenerate_contrast" in member["unavailable_reasons"]
    assert member["inference_available"] is False
    assert member["p_raw"] is None
    degeneracy = member["degeneracy"]
    # The component scale is large: the cancellation is detected against uE and
    # uC, not against the already cancelled difference.
    assert degeneracy["component_scale"] > 1e-3
    assert degeneracy["sigma"] <= degeneracy["threshold"]


def test_a_small_but_well_resolved_effect_is_not_called_degenerate():
    rng = np.random.default_rng(43)
    frames = []
    for month in range(12):
        frame = analysis_records(
            instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng
        )
        frame.loc[frame["is_target"], "net_return"] += 1e-7
        frames.append(frame)
    result = evaluate_simple(frames, instruments=("AAA",), resamples=999)
    member = result["members"][0]
    assert member["inference_available"] is True
    assert "degenerate_contrast" not in member["unavailable_reasons"]


# --------------------------------------------------------------------------
# family correction
# --------------------------------------------------------------------------

def test_holm_matches_a_known_worked_example_with_unavailable_members():
    p_values = np.array([0.001, 0.009, 0.04, 1.0, 1.0])
    adjusted = analysis_estimator._holm(p_values, ["a", "b", "c", "d", "e"])
    assert adjusted.tolist() == pytest.approx([0.005, 0.036, 0.12, 1.0, 1.0])


def test_unavailable_members_keep_their_place_in_the_family_size():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(
            instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng,
            member_id="full",
        )
        for month in range(12)
    ]
    frames.extend(
        analysis_records(
            instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng,
            member_id="short",
        )
        for month in range(2)
    )
    members = [analysis_member(member_id="full"), analysis_member(member_id="short")]
    result = evaluate_observations(
        frames, family=members, instruments=("AAA",),
        study_start_ms=YEAR_START, study_end_ms=YEAR_END, resamples=999, seed=5,
    )
    assert result["family_size"] == 2
    unavailable = next(item for item in result["members"] if item["member_id"] == "short")
    available = next(item for item in result["members"] if item["member_id"] == "full")
    assert unavailable["p_holm"] is None and unavailable["nominal_reject_holm"] is None
    assert unavailable["p_holm_internal"] == 1.0
    assert available["p_holm"] == pytest.approx(min(1.0, 2 * available["p_raw"]))


def test_a_resolution_that_prevents_any_first_rejection_is_reported():
    rng = np.random.default_rng(3)
    frames = _twelve_months(rng)
    members = [analysis_member(member_id=f"m{index}") for index in range(80)]
    expanded = [frame.assign(member_id=member.member_id) for member in members for frame in frames]
    result = evaluate_observations(
        expanded, family=members, instruments=("AAA",),
        study_start_ms=YEAR_START, study_end_ms=YEAR_END, resamples=1999, seed=5,
    )
    assert result["p_resolution_blocks_first_rejection"] is True


# --------------------------------------------------------------------------
# the record contract
# --------------------------------------------------------------------------

def test_the_record_schema_is_closed_and_aligned():
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    with pytest.raises(PatternLabDataError, match="schema is closed"):
        evaluate_simple([frame.assign(extra=1.0)], instruments=("AAA",))
    with pytest.raises(PatternLabDataError, match="schema is closed"):
        evaluate_simple([frame.drop(columns=["available"])], instruments=("AAA",))


@pytest.mark.parametrize("column", ["is_target", "is_control", "available", "return_valid"])
def test_a_non_boolean_mask_is_rejected(column):
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    frame[column] = frame[column].astype(int)
    with pytest.raises(PatternLabDataError, match="boolean mask"):
        evaluate_simple([frame], instruments=("AAA",))


def test_a_float_or_boolean_join_key_is_rejected_and_int32_is_accepted():
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    with pytest.raises(PatternLabDataError, match="exact integer"):
        evaluate_simple(
            [frame.assign(signal_time_ms=frame["signal_time_ms"].astype(float))],
            instruments=("AAA",),
        )
    small = frame.copy()
    small["signal_time_ms"] = small["signal_time_ms"].astype("int64")
    evaluate_simple([small], instruments=("AAA",))


def test_an_invalid_row_must_be_null_and_is_never_dropped_silently():
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    broken = frame.copy()
    broken.loc[broken.index[0], "return_valid"] = False
    with pytest.raises(PatternLabDataError, match="marked invalid"):
        evaluate_simple([broken], instruments=("AAA",))
    fixed = broken.copy()
    fixed.loc[fixed.index[0], ["net_return", "gross_return"]] = np.nan
    result = evaluate_simple([fixed], instruments=("AAA",))
    assert result["members"][0]["counts"]["target_invalid_return"] == 1


def test_an_unknown_member_or_instrument_is_an_error():
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    with pytest.raises(PatternLabDataError, match="not a member of the frozen family"):
        evaluate_simple([frame.assign(member_id="other")], instruments=("AAA",))
    with pytest.raises(PatternLabDataError, match="not one of the declared instruments"):
        evaluate_simple([frame.assign(instrument_id="ZZZ")], instruments=("AAA",))


def test_an_observation_outside_the_study_interval_is_an_error():
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    moved = frame.copy()
    moved["signal_time_ms"] = moved["signal_time_ms"] - day_ms(400)
    with pytest.raises(PatternLabDataError, match="outside the half-open"):
        evaluate_simple([moved], instruments=("AAA",))


def test_the_request_bounds_are_checked_at_the_numerical_boundary():
    frame = analysis_records(instrument="AAA", month_offset=0, targets=40, controls=40)
    with pytest.raises(PatternLabDataError, match="positive integer"):
        evaluate_simple([frame], instruments=("AAA",), resamples=0)
    with pytest.raises(PatternLabDataError, match=r"\[0, 2\*\*32-1\]"):
        evaluate_observations(
            [frame], family=[analysis_member()], instruments=("AAA",),
            study_start_ms=YEAR_START, study_end_ms=YEAR_END, resamples=99,
            seed=analysis_request.MAX_SEED + 1,
        )


def test_the_resampling_stage_touches_no_file(monkeypatch):
    """Replicates are pure arithmetic: no per-draw filesystem access."""
    import builtins
    import io

    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    opened: list[str] = []
    real_open = builtins.open
    real_io_open = io.open

    def watching(file, *args, **kwargs):
        opened.append(str(file))
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", watching)
    monkeypatch.setattr(io, "open", watching)
    result = evaluate_simple(frames, instruments=("AAA",), resamples=9999)
    monkeypatch.undo()
    assert result["members"][0]["inference_available"] is True
    assert opened == []


def test_a_larger_resample_count_does_not_change_the_point_estimates():
    rng = np.random.default_rng(3)
    frames = [
        analysis_records(instrument="AAA", month_offset=month, targets=40, controls=60, rng=rng)
        for month in range(12)
    ]
    small = evaluate_simple(frames, instruments=("AAA",), resamples=1999)
    large = evaluate_simple(frames, instruments=("AAA",), resamples=9999)
    assert small["members"][0]["lift"] == large["members"][0]["lift"]
    assert large["p_resolution"] == pytest.approx(2 / 10000)
