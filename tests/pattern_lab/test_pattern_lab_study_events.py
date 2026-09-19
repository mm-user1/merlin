"""Causality, occurrence policies and episode accounting.

Every fixture is a small hand-calculated series, and every assertion is about
observable evidence rather than an implementation detail.  These cases drive the
top-level RAM-only job directly, without a pack, a lock or an output directory.
"""

from __future__ import annotations

import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.study.job import build_series, run_instrument_job

from ._helpers import (
    ANCHOR_MS,
    STUDY_INSTRUMENT as INSTRUMENT,
    STUDY_TIMEFRAME as TIMEFRAME,
    TWO_GREEN_EVERY_BAR,
    TWO_GREEN_STATE_ENTRY,
    fixed_horizon_model,
    normalized_study as normalized,
    study_group_ms as group_ms,
    study_job as make_job,
)

# open, high, low, close, quote volume; one tuple per 30m bar.
STRICT_SPECS = (
    (100.0, 101.0, 99.0, 100.0, 10.0),   # g0 equal open/close: not green
    (100.0, 102.0, 99.0, 101.0, 20.0),   # g1 green, volume rises
    (101.0, 103.0, 100.0, 102.0, 20.0),  # g2 green, volume equal: not rising
    (102.0, 104.0, 101.0, 103.0, 30.0),  # g3 green after green, volume rises
    (103.0, 105.0, 102.0, 104.0, 40.0),  # g4 the run continues and qualifies again
    (104.0, 106.0, 103.0, 103.0, 50.0),  # g5 red close
    (103.0, 105.0, 102.0, 104.0, 60.0),  # g6 closes exactly at the study end
)


def anchor_groups(frame, column: str = "anchor_open_ms") -> list[int]:
    return [int((int(value) - ANCHOR_MS) // (TIMEFRAME * 60_000)) for value in frame[column]]


def test_strict_green_and_volume_comparisons_and_repeated_qualifying_closes():
    _request, payload = make_job(STRICT_SPECS, start_group=1, end_group=7)
    result = run_instrument_job(payload)
    conditions = result.tables["conditions"]

    # The final bar closes exactly at the study end, so it is not an anchor.
    assert anchor_groups(conditions) == [1, 2, 3, 4, 5]
    assert conditions["valid"].tolist() == [True] * 5
    # g1 follows a bar that is not green, g2's volume did not rise, g5 closes red.
    assert conditions["value"].tolist() == [False, False, True, True, False]

    emissions = result.tables["emissions"]
    assert anchor_groups(emissions) == [3, 4]


def test_state_entry_emits_only_the_known_false_to_true_transition():
    _request, payload = make_job(
        STRICT_SPECS, start_group=1, end_group=7,
        hypotheses=(TWO_GREEN_EVERY_BAR, TWO_GREEN_STATE_ENTRY),
    )
    result = run_instrument_job(payload)
    emissions = result.tables["emissions"]
    every = emissions.loc[emissions["variant_id"] == "two_green_every"]
    entry = emissions.loc[emissions["variant_id"] == "two_green_entry"]

    assert anchor_groups(every) == [3, 4]
    # g3 has a known false predecessor; g4's predecessor is already true.
    assert anchor_groups(entry) == [3]


def test_condition_identity_is_shared_while_emitted_event_identity_differs_by_policy():
    _request, payload = make_job(
        STRICT_SPECS, start_group=1, end_group=7,
        hypotheses=(TWO_GREEN_EVERY_BAR, TWO_GREEN_STATE_ENTRY),
    )
    result = run_instrument_job(payload)
    conditions = result.tables["conditions"]
    emissions = result.tables["emissions"]

    assert conditions["condition_id"].nunique() == 1
    assert emissions["condition_id"].nunique() == 1
    shared_anchor = emissions.loc[emissions["anchor_open_ms"] == group_ms(3)]
    assert len(shared_anchor) == 2
    assert shared_anchor["event_id"].nunique() == 2


def test_an_unknown_predecessor_is_not_a_false_to_true_transition():
    # g2 is missing, so g3's immediately preceding observation is unknown.
    _request, payload = make_job(
        STRICT_SPECS, start_group=1, end_group=7, drop_groups=(2,),
        hypotheses=(TWO_GREEN_EVERY_BAR, TWO_GREEN_STATE_ENTRY),
    )
    result = run_instrument_job(payload)
    conditions = result.tables["conditions"]
    emissions = result.tables["emissions"]

    values = dict(zip(anchor_groups(conditions), conditions["valid"].tolist()))
    assert values[3] is False or values[3] == False  # noqa: E712 - unknown, not false
    assert anchor_groups(emissions.loc[emissions["variant_id"] == "two_green_every"]) == [4]
    # A true condition after unknown history never emits a state entry.
    assert anchor_groups(emissions.loc[emissions["variant_id"] == "two_green_entry"]) == []


def test_warmup_history_supplies_the_first_research_anchor():
    # The study starts at g3: its condition still uses the warmup bar g2.
    _request, payload = make_job(STRICT_SPECS, start_group=3, end_group=7)
    result = run_instrument_job(payload)
    conditions = result.tables["conditions"]
    assert anchor_groups(conditions) == [3, 4, 5]
    assert conditions["valid"].tolist() == [True, True, True]
    assert conditions["value"].tolist() == [True, True, False]
    assert anchor_groups(result.tables["emissions"]) == [3, 4]


def test_the_first_observation_is_unknown_rather_than_false():
    _request, payload = make_job(STRICT_SPECS, start_group=1, end_group=7)
    series = build_series(INSTRUMENT, payload.timeframes[0])
    descriptor = pack_study.registrations("hypothesis")[pack_study.TWO_GREEN_HYPOTHESIS_ID].descriptor
    condition = descriptor.evaluate(series, {}, {})
    assert bool(condition.valid[0]) is False
    assert bool(condition.value[0]) is False
    assert bool(condition.valid[1]) is True


def test_a_declared_warmup_shorter_than_the_required_lookback_is_rejected():
    with pytest.raises(PatternLabDataError, match="prior 30m observation bar"):
        normalized(start_group=0, end_group=7, warmup_group=0)


def test_episodes_record_bounds_censoring_and_eligible_anchor_counts():
    _request, payload = make_job(STRICT_SPECS, start_group=1, end_group=7)
    episodes = run_instrument_job(payload).tables["episodes"]

    assert len(episodes) == 1
    row = episodes.iloc[0]
    assert int(row["first_bar_open_ms"]) == group_ms(3)
    assert int(row["last_bar_open_ms"]) == group_ms(4)
    assert int(row["bar_count"]) == 2
    assert int(row["eligible_anchor_count"]) == 2
    assert bool(row["left_censored"]) is False
    assert bool(row["right_censored"]) is False


def test_an_episode_running_into_the_study_boundary_is_censored_on_both_sides():
    # Every bar is green with rising volume, so one episode spans the window.
    specs = tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(6)
    )
    _request, payload = make_job(specs, start_group=2, end_group=6)
    episodes = run_instrument_job(payload).tables["episodes"]
    row = episodes.iloc[0]
    assert int(row["first_bar_open_ms"]) == group_ms(1)  # it began before the study
    assert bool(row["left_censored"]) is True
    assert bool(row["right_censored"]) is True
    assert int(row["eligible_anchor_count"]) == 3


def test_a_gap_breaks_one_episode_into_two_and_re_warms_the_condition():
    specs = tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(8)
    )
    _request, payload = make_job(specs, start_group=1, end_group=8, drop_groups=(4,))
    result = run_instrument_job(payload)
    episodes = result.tables["episodes"].sort_values("first_bar_open_ms").reset_index(drop=True)

    assert len(episodes) == 2
    assert int(episodes.loc[0, "last_bar_open_ms"]) == group_ms(3)
    assert bool(episodes.loc[0, "right_censored"]) is True
    assert int(episodes.loc[1, "first_bar_open_ms"]) == group_ms(6)
    assert bool(episodes.loc[1, "left_censored"]) is True
    # g5 re-warms after the gap, so its condition is unknown rather than false.
    conditions = result.tables["conditions"]
    unknown = conditions.loc[conditions["anchor_open_ms"] == group_ms(5)].iloc[0]
    assert bool(unknown["valid"]) is False


def test_an_incomplete_aggregate_group_is_omitted_and_breaks_contiguity():
    specs = tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(8)
    )
    # One 5m slot of group 4 is missing, so that 30m group is never promoted.
    _request, payload = make_job(specs, start_group=1, end_group=8, drop_slots=(4 * 6 + 3,))
    assert payload.timeframes[0].omitted_group_count == 1
    result = run_instrument_job(payload)
    assert group_ms(4) not in [int(value) for value in result.tables["conditions"]["anchor_open_ms"]]
    assert len(result.tables["episodes"]) == 2


def test_adding_a_horizon_or_a_model_leaves_existing_events_unchanged():
    base_request, base_payload = make_job(
        STRICT_SPECS, start_group=1, end_group=7, models=[fixed_horizon_model(TIMEFRAME, [30])]
    )
    wide_request, wide_payload = make_job(
        STRICT_SPECS,
        start_group=1,
        end_group=7,
        models=[
            fixed_horizon_model(TIMEFRAME, [30, 60, 120]),
            fixed_horizon_model(TIMEFRAME, [60], instance_id="second", directions=("long",)),
        ],
    )
    base = run_instrument_job(base_payload).tables["emissions"]
    wide = run_instrument_job(wide_payload).tables["emissions"]

    assert base["event_id"].tolist() == wide["event_id"].tolist()
    assert base_request.variants[0].condition_id == wide_request.variants[0].condition_id


def test_an_occurrence_policy_outside_the_supported_set_is_rejected():
    with pytest.raises(PatternLabDataError, match="unknown policy"):
        normalized(
            start_group=1,
            end_group=7,
            hypotheses=[dict(TWO_GREEN_EVERY_BAR, occurrence="first_trigger")],
        )


def test_two_semantically_identical_variants_are_rejected_rather_than_counted_twice():
    with pytest.raises(PatternLabDataError, match="semantically identical"):
        normalized(
            start_group=1,
            end_group=7,
            hypotheses=[TWO_GREEN_EVERY_BAR, dict(TWO_GREEN_EVERY_BAR, id="duplicate")],
        )
