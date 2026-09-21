"""The reproducible calibration driver for the M3a inference screen.

Every repetition enters at :func:`~tools.pattern_lab.analysis.estimator.evaluate_observations`,
so the production calendar stratification, support exclusions, weighting, daily
aggregation, joint influence, inference gates, bootstrap and Holm correction all
run on every draw.  Nothing here is a second estimator, a hand-prepared influence
vector or a precomputed mean.

The generators produce **synthetic outcome and mask records**, not coherent OHLC
backtests: a horizon outcome is an additive path sum and the per-side cost is a
constant, which leaves the tested lift unchanged because a constant cancels in a
difference of means.  No distributional claim about real markets follows from
any setting here.

Acceptance is an explicit empirical screen: for each admitted scenario the raw
primary rejection rate, the primary nominal-95% interval noncoverage and the
nominal Holm family-wise rejection rate each need a one-sided 95% exact binomial
upper bound at or below 0.08.  That 8% ceiling is a declared maximum empirical
error envelope on these fixtures — **not** a new test alpha, a 92% interval or a
certification of exact 5% control.  Alpha stays 0.05 and the intervals stay
nominally 95%.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, asdict
import json
from pathlib import Path
import sys
import time
from typing import Any, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from ..study import contracts
from . import artifacts
from .estimator import RECORD_COLUMNS, evaluate_observations
from .family import FamilyMember
from .request import ALPHA

# Version 2 changes the *meaning* of the release-facing result: a completed run
# now carries a driver-owned attempt ledger and an explicit protocol gate, and
# the rate scorer's Boolean is scoped to the requested subset rather than to
# release acceptance.  Saved schema-v1 documents remain readable historical
# records; they carry no ledger, so they are ineligible under the complete-run
# contract below and are never rewritten or granted retrospective status.
CALIBRATION_SCHEMA_VERSION = 2

# The named validation contract of the existing bootstrap command.  A pass of
# this protocol is a pass of *this* protocol only: it is not acceptance of the
# inference method on market data, and it is not a contract for another method.
LEGACY_PROTOCOL = "legacy_bootstrap_v1"
LEGACY_GATE_VERSION = 1
LEGACY_REPLAY_REPETITIONS = 25

MASTER_SEED = 20260920
DATA_STREAM = 0
BOOTSTRAP_STREAM = 1

DAY_MS = 86_400_000
TIMEFRAME_MINUTES = 30
BARS_PER_DAY = (24 * 60) // TIMEFRAME_MINUTES
HORIZON_BARS = (2, 4, 8, 16)
HORIZON_MINUTES = tuple(bars * TIMEFRAME_MINUTES for bars in HORIZON_BARS)
DIRECTIONS = ("long", "short")
PRIMARY_HORIZON_MINUTES = 240
PRIMARY_DIRECTION = "long"

INSTRUMENTS = ("SYN_A", "SYN_B", "SYN_C", "SYN_D")

# Every generator setting is frozen here, before any scoring.
COMMON_LOADING = 0.8
INDIVIDUAL_LOADING = 0.6
DAILY_AR_COEFFICIENT = 0.4
DAILY_LEVEL_SCALE = 0.001
BAR_INNOVATION_SCALE = 0.005
STUDENT_T_DF = 5
SIGNAL_P11 = 0.70
SIGNAL_P10 = 0.0857
SIGNAL_TARGET_IN_STATE_1 = 0.45
SIGNAL_TARGET_IN_STATE_0 = 0.05
INDEPENDENT_SIGNAL_PROBABILITY = 0.20
CONSTANT_COST_PER_OBSERVATION = 0.001

# Scenario 3's declared confounding: opposing month means across tickers and an
# event frequency that is constant inside a stratum but differs between strata.
CONFOUNDED_MONTH_SHIFT = 0.0004
CONFOUNDED_PROBABILITIES = (0.10, 0.40)

# Scenario 4's declared thinning, missing intervals and short history.
THINNING_PROBABILITY = 0.5
MISSING_INTERVAL_DAYS = ((40, 47), (150, 157), (260, 266))
SHORT_HISTORY_INSTRUMENT = "SYN_D"
SHORT_HISTORY_START_DAY = 35

# The empirical acceptance envelope, and the availability floor beneath it.
ERROR_ENVELOPE = 0.08
MIN_PRIMARY_AVAILABILITY = 0.95

PARENT_VARIANT = "parent"
CHILD_VARIANT = "child"
SINGLE_VARIANT = "signal"

_UTC_DAY_ZERO = np.datetime64("1970-01-01", "D")


def _epoch_ms(day: str) -> int:
    return int((np.datetime64(day, "D") - _UTC_DAY_ZERO).astype("int64")) * DAY_MS


@dataclass(frozen=True)
class Scenario:
    """One frozen calibration scenario."""

    scenario_id: int
    name: str
    kind: str
    start_day: str
    days: int
    dependent: bool
    innovation: str
    signal: str
    comparison: str
    repetitions: int
    resamples: int
    admitted: bool
    planted_effect: float = 0.0
    daily_ar: float = DAILY_AR_COEFFICIENT
    signal_p11: float = SIGNAL_P11
    evaluation_days: int | None = None
    evaluation_start_day: str | None = None
    note: str = ""
    # A known limitation of this fixture's own null, carried with every record
    # that reports it.  It qualifies the interpretation of a rate; it is never a
    # waiver for a failed rate.
    caveat: str = ""

    @property
    def grid_start_ms(self) -> int:
        return _epoch_ms(self.start_day)

    @property
    def grid_end_ms(self) -> int:
        return self.grid_start_ms + self.days * DAY_MS

    @property
    def study_start_ms(self) -> int:
        return _epoch_ms(self.evaluation_start_day or self.start_day)

    @property
    def study_end_ms(self) -> int:
        return self.study_start_ms + (self.evaluation_days or self.days) * DAY_MS


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        scenario_id=1,
        name="null_independent",
        kind="admitted_null",
        start_day="2025-07-01",
        days=365,
        dependent=False,
        innovation="gaussian",
        signal="iid",
        comparison="baseline",
        repetitions=2000,
        resamples=1999,
        admitted=True,
        note="Independent outcome innovations and iid signal masks at probability 0.20.",
    ),
    Scenario(
        scenario_id=2,
        name="null_dependent_t5",
        kind="admitted_null",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=2000,
        resamples=1999,
        admitted=True,
        note=(
            "Shared and individual AR(1) daily factors, clustered Markov signals and overlapping "
            "1/2/4/8-hour outcomes, with standardized Student-t(5) bar innovations."
        ),
    ),
    Scenario(
        scenario_id=3,
        name="null_conditional_confounded",
        kind="admitted_null",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="confounded",
        comparison="baseline",
        repetitions=2000,
        resamples=1999,
        admitted=True,
        note=(
            "Known instrument/month mean shifts with opposing signs across tickers and event "
            "probabilities 0.10/0.40 that are constant inside a stratum. The conditional null holds "
            "inside every true UTC-month stratum, including boundary-crossing outcomes, while naive "
            "unmatched pooling is confounded."
        ),
        caveat=(
            "Event probabilities are assigned by the anchor's own bar month, while matching owns "
            "the signal-close month, so the 44 anchors that open at 23:30 on a month's last day "
            "carry the previous month's probability. The resulting deterministic population "
            "contrast is tiny — 4.1254148207e-08 at most per stratum and 1.4622196520e-08 "
            "event-weighted at the 240m primary horizon — but it is not exactly zero. Fixture 101 "
            "is the signal-month-aligned correction; this fixture is retained unchanged as a "
            "compatibility check and its caveat is not a waiver for a failed rate."
        ),
    ),
    Scenario(
        scenario_id=4,
        name="null_inclusive_parent",
        kind="admitted_null",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="markov",
        comparison="parent_child",
        repetitions=2000,
        resamples=1999,
        admitted=True,
        note=(
            "Inclusive parent/child comparison with nonpredictive thinning at 0.5, exogenous "
            "missing intervals and one shorter instrument history. The two conditions share one "
            "identical availability mask, as the production contract requires."
        ),
    ),
    Scenario(
        scenario_id=5,
        name="null_admission_boundary_336",
        kind="admitted_null",
        start_day="2025-07-15",
        days=336,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=2000,
        resamples=1999,
        admitted=True,
        note=(
            "The exact admission boundary: 336 days from 2025-07-15, otherwise the scenario-2 "
            "generator. Both partial boundary months sit above the stratum minimum and exactly 48 "
            "full seven-day bins are exercised, with no 365-day cushion."
        ),
    ),
    Scenario(
        scenario_id=12,
        name="null_dependent_gaussian_companion",
        kind="reported_null",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="markov",
        comparison="baseline",
        repetitions=2000,
        resamples=1999,
        admitted=False,
        note=(
            "The otherwise comparable Gaussian case for scenario 2. Reported beside the admitted "
            "scenarios; it is not one of the fifteen acceptance checks."
        ),
    ),
    Scenario(
        scenario_id=10,
        name="short_population_84_days",
        kind="refusal",
        start_day="2025-07-01",
        days=84,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=200,
        resamples=1999,
        admitted=False,
        note="A short population must publish no p-value and no confidence interval.",
    ),
    Scenario(
        scenario_id=11,
        name="short_population_180_days",
        kind="refusal",
        start_day="2025-07-01",
        days=180,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=200,
        resamples=1999,
        admitted=False,
        note="A short population must publish no p-value and no confidence interval.",
    ),
    Scenario(
        scenario_id=20,
        name="stress_long_dependence_ar09_p070",
        kind="stress",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=1000,
        resamples=1999,
        admitted=False,
        daily_ar=0.9,
        signal_p11=0.70,
        note=(
            "Required limitation experiment, outside the admitted-null envelope: daily AR 0.9 with "
            "the declared signal persistence unchanged."
        ),
    ),
    Scenario(
        scenario_id=21,
        name="stress_long_dependence_ar09_p097",
        kind="stress",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=1000,
        resamples=1999,
        admitted=False,
        daily_ar=0.9,
        signal_p11=0.97,
        note=(
            "Required limitation experiment, outside the admitted-null envelope: daily AR 0.9 with "
            "signal persistence 0.97."
        ),
    ),
    Scenario(
        scenario_id=30,
        name="planted_positive_strong",
        kind="planted",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="markov",
        comparison="baseline",
        repetitions=200,
        resamples=1999,
        admitted=False,
        planted_effect=0.005,
        note="Strong-effect wiring check against disjoint nonsignal controls, not a power promise.",
    ),
    Scenario(
        scenario_id=31,
        name="planted_negative_strong",
        kind="planted",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="markov",
        comparison="baseline",
        repetitions=200,
        resamples=1999,
        admitted=False,
        planted_effect=-0.005,
        note="Strong-effect wiring check against disjoint nonsignal controls, not a power promise.",
    ),
    Scenario(
        scenario_id=32,
        name="planted_modest",
        kind="planted_descriptive",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="markov",
        comparison="baseline",
        repetitions=200,
        resamples=1999,
        admitted=False,
        planted_effect=0.00005,
        note=(
            "One fixed modest effect, characterized descriptively with no minimum power gate and "
            "no tuning of its magnitude."
        ),
    ),
    Scenario(
        scenario_id=40,
        name="default_b_smoke",
        kind="smoke",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="student_t5",
        signal="markov",
        comparison="baseline",
        repetitions=100,
        resamples=9999,
        admitted=False,
        note=(
            "A default-B smoke on the scenario-2 generator. The primary error checks stay at "
            "B=1999."
        ),
    ),
    Scenario(
        scenario_id=101,
        name="null_confounded_signal_month_v2",
        kind="admitted_null",
        start_day="2025-07-01",
        days=365,
        dependent=True,
        innovation="gaussian",
        signal="confounded_signal_month",
        comparison="baseline",
        repetitions=2000,
        resamples=1999,
        admitted=True,
        note=(
            "Scenario 3's return law, comparison, calendar, support, family and generator "
            "parameters, with the target-event probability assigned by the anchor's signal-close "
            "UTC month instead of its own bar month. The deterministic return-mean schedule keeps "
            "its original bar-time ownership, so probabilities are constant inside every actual "
            "matching stratum and the row-mixture target and control population means agree at "
            "roundoff. Its seed uses fixture ID 101, so it is an independent realization of "
            "scenario 3's law rather than a paired correction of the same draws."
        ),
    ),
)

SCENARIOS_BY_NAME = {item.name: item for item in SCENARIOS}
SCENARIOS_BY_ID = {item.scenario_id: item for item in SCENARIOS}

# The fixtures the legacy bootstrap protocol runs, pinned explicitly in their
# original order rather than derived from :data:`SCENARIOS`.  A later fixture
# added to the registry — 101 is the first — must not silently change what this
# command runs or what its release gate requires.
LEGACY_PROTOCOL_SCENARIOS: tuple[str, ...] = (
    "null_independent",
    "null_dependent_t5",
    "null_conditional_confounded",
    "null_inclusive_parent",
    "null_admission_boundary_336",
    "null_dependent_gaussian_companion",
    "short_population_84_days",
    "short_population_180_days",
    "stress_long_dependence_ar09_p070",
    "stress_long_dependence_ar09_p097",
    "planted_positive_strong",
    "planted_negative_strong",
    "planted_modest",
    "default_b_smoke",
)


# --------------------------------------------------------------------------
# the frozen family of one scenario
# --------------------------------------------------------------------------

def _case_id(horizon: int, direction: str) -> str:
    return f"tf{TIMEFRAME_MINUTES}m.h{horizon}m.{direction}"


def scenario_family(scenario: Scenario) -> tuple[FamilyMember, ...]:
    """Every automatically generated comparison of a scenario, both directions."""
    if scenario.comparison == "baseline":
        comparisons = [("baseline__signal", "nonsignal_baseline", SINGLE_VARIANT, None)]
    else:
        comparisons = [
            ("baseline__child", "nonsignal_baseline", CHILD_VARIANT, None),
            ("baseline__parent", "nonsignal_baseline", PARENT_VARIANT, None),
            ("child_versus_parent", "pairwise", CHILD_VARIANT, PARENT_VARIANT),
        ]
    members: list[FamilyMember] = []
    for comparison_id, kind, target, control in comparisons:
        for horizon in HORIZON_MINUTES:
            for direction in DIRECTIONS:
                case_id = _case_id(horizon, direction)
                members.append(
                    FamilyMember(
                        member_id=f"{comparison_id}|synthetic|tf{TIMEFRAME_MINUTES}m|{case_id}",
                        comparison_id=comparison_id,
                        kind=kind,
                        target_variant=target,
                        control_variant=control,
                        model_instance_id="synthetic",
                        timeframe_minutes=TIMEFRAME_MINUTES,
                        case_id=case_id,
                        direction=direction,
                        horizon_minutes=horizon,
                        commission_pct_per_side=0.0,
                        primary=horizon == PRIMARY_HORIZON_MINUTES,
                        label=f"{comparison_id} {direction} {horizon}m",
                    )
                )
    return tuple(members)


def primary_member_id(scenario: Scenario) -> str:
    """The monitored member: the 4h long contrast of the scenario's comparison."""
    comparison = "child_versus_parent" if scenario.comparison == "parent_child" else "baseline__signal"
    return (
        f"{comparison}|synthetic|tf{TIMEFRAME_MINUTES}m|"
        f"{_case_id(PRIMARY_HORIZON_MINUTES, PRIMARY_DIRECTION)}"
    )


# --------------------------------------------------------------------------
# the frozen generators
# --------------------------------------------------------------------------

def seed_sequence(scenario_id: int, repetition: int, stream: int) -> np.random.SeedSequence:
    return np.random.SeedSequence([MASTER_SEED, scenario_id, repetition, stream])


def bootstrap_seed(scenario_id: int, repetition: int) -> int:
    sequence = seed_sequence(scenario_id, repetition, BOOTSTRAP_STREAM)
    return int(sequence.generate_state(1, dtype=np.uint32)[0])


def data_generator(scenario_id: int, repetition: int) -> np.random.Generator:
    return np.random.Generator(np.random.PCG64(seed_sequence(scenario_id, repetition, DATA_STREAM)))


def _innovations(rng: np.random.Generator, shape: tuple[int, ...], law: str) -> np.ndarray:
    if law == "gaussian":
        return rng.standard_normal(shape)
    if law == "student_t5":
        # Standardized so the innovation scale means the same thing under both laws.
        return rng.standard_t(STUDENT_T_DF, size=shape) / np.sqrt(
            STUDENT_T_DF / (STUDENT_T_DF - 2)
        )
    raise PatternLabDataError(f"unknown innovation law {law!r}.")


def _ar1_daily(rng: np.random.Generator, days: int, phi: float) -> np.ndarray:
    """One AR(1) daily factor initialized from its stationary law."""
    stationary = DAILY_LEVEL_SCALE
    innovation = stationary * np.sqrt(max(1.0 - phi * phi, 0.0))
    values = np.empty(days, dtype=np.float64)
    values[0] = rng.standard_normal() * stationary
    shocks = rng.standard_normal(days - 1) * innovation if days > 1 else np.zeros(0)
    for index in range(1, days):
        values[index] = phi * values[index - 1] + shocks[index - 1]
    return values


def _markov_states(rng: np.random.Generator, days: int, p11: float) -> np.ndarray:
    """A daily two-state chain initialized from its stationary distribution."""
    p10 = SIGNAL_P10
    p01 = 1.0 - p11
    stationary = p10 / (p10 + p01)
    states = np.empty(days, dtype=bool)
    states[0] = rng.random() < stationary
    draws = rng.random(days - 1) if days > 1 else np.zeros(0)
    for index in range(1, days):
        threshold = p11 if states[index - 1] else p10
        states[index] = draws[index - 1] < threshold
    return states


def _month_index(start_ms: int, days: int) -> np.ndarray:
    """The UTC calendar month ordinal of every day in the grid."""
    dates = (np.datetime64(start_ms, "ms").astype("datetime64[D]") + np.arange(days)).astype(
        "datetime64[M]"
    )
    return (dates - dates[0]).astype("int64")


def _signal_month_index(start_ms: int, bars: int) -> np.ndarray:
    """The UTC calendar month ordinal of every anchor's **signal close**.

    Anchor ``j`` opens at ``start_ms + j * step`` and signals at its close, one
    step later, which is the instant matching owns.  The 23:30 anchor of a
    month's last day therefore belongs to the next month's stratum.  Ordinals
    are relative to the first bar's own month, so a grid that starts mid-month
    keeps ordinal zero for that partial month.
    """
    step = TIMEFRAME_MINUTES * 60_000
    signal_ms = start_ms + (np.arange(bars, dtype=np.int64) + 1) * step
    months = signal_ms.astype("datetime64[ms]").astype("datetime64[M]")
    first = np.datetime64(start_ms, "ms").astype("datetime64[M]")
    return (months - first).astype("int64")


@dataclass(frozen=True)
class SyntheticRecords:
    """One repetition's frozen synthetic outcomes and masks."""

    scenario: Scenario
    returns: Mapping[str, np.ndarray]
    target_mask: Mapping[str, np.ndarray]
    control_mask: Mapping[str, np.ndarray]
    available: Mapping[str, np.ndarray]
    parent_mask: Mapping[str, np.ndarray] | None


def generate_records(scenario: Scenario, repetition: int) -> SyntheticRecords:
    """Generate one repetition's per-bar returns and per-anchor masks.

    The return innovations are drawn first and the signal process afterwards, so
    the signal states are statistically independent of the entire
    return-innovation stream.

    ``confounded`` assigns its event probability from the anchor's own bar month
    and ``confounded_signal_month`` from the anchor's signal-close month, which
    is the month the production matching strata by.  Both share one deterministic
    return-mean schedule that stays owned by bar time.
    """
    rng = data_generator(scenario.scenario_id, repetition)
    days = scenario.days
    bars = days * BARS_PER_DAY
    month_of_day = _month_index(scenario.grid_start_ms, days)
    signal_month_of_bar = (
        _signal_month_index(scenario.grid_start_ms, bars)
        if scenario.signal == "confounded_signal_month"
        else None
    )

    common = _ar1_daily(rng, days, scenario.daily_ar) if scenario.dependent else np.zeros(days)
    returns: dict[str, np.ndarray] = {}
    for instrument in INSTRUMENTS:
        individual = (
            _ar1_daily(rng, days, scenario.daily_ar) if scenario.dependent else np.zeros(days)
        )
        level = COMMON_LOADING * common + INDIVIDUAL_LOADING * individual
        noise = _innovations(rng, (days, BARS_PER_DAY), scenario.innovation) * BAR_INNOVATION_SCALE
        series = level[:, np.newaxis] + noise
        if scenario.signal in ("confounded", "confounded_signal_month"):
            sign = 1.0 if INSTRUMENTS.index(instrument) % 2 == 0 else -1.0
            shift = sign * CONFOUNDED_MONTH_SHIFT * np.where(month_of_day % 2 == 0, 1.0, -1.0)
            series = series + shift[:, np.newaxis]
        returns[instrument] = series.reshape(bars)

    target: dict[str, np.ndarray] = {}
    control: dict[str, np.ndarray] = {}
    available: dict[str, np.ndarray] = {}
    parent: dict[str, np.ndarray] = {}
    for instrument in INSTRUMENTS:
        if scenario.signal == "iid":
            mask = rng.random(bars) < INDEPENDENT_SIGNAL_PROBABILITY
        elif scenario.signal == "confounded":
            index = INSTRUMENTS.index(instrument)
            probabilities = np.array(
                [
                    CONFOUNDED_PROBABILITIES[(index + int(month)) % len(CONFOUNDED_PROBABILITIES)]
                    for month in month_of_day
                ],
                dtype=np.float64,
            )
            mask = (rng.random((days, BARS_PER_DAY)) < probabilities[:, np.newaxis]).reshape(bars)
        elif scenario.signal == "confounded_signal_month":
            # Fixture 101: the same law as ``confounded``, with the probability
            # assigned by the anchor's signal-close month, which is the month the
            # production matching actually strata by.  The deterministic return
            # mean above keeps its original bar-time ownership, so probabilities
            # are constant inside every real stratum without changing the return
            # process this fixture inherits from scenario 3.
            index = INSTRUMENTS.index(instrument)
            probabilities = np.asarray(CONFOUNDED_PROBABILITIES, dtype=np.float64)[
                (index + signal_month_of_bar) % len(CONFOUNDED_PROBABILITIES)
            ]
            mask = rng.random(bars) < probabilities
        else:
            states = _markov_states(rng, days, scenario.signal_p11)
            rates = np.where(states, SIGNAL_TARGET_IN_STATE_1, SIGNAL_TARGET_IN_STATE_0)
            mask = (rng.random((days, BARS_PER_DAY)) < rates[:, np.newaxis]).reshape(bars)
        known = np.ones(bars, dtype=bool)
        if scenario.comparison == "parent_child":
            for first, last in MISSING_INTERVAL_DAYS:
                known[first * BARS_PER_DAY : (last + 1) * BARS_PER_DAY] = False
            if instrument == SHORT_HISTORY_INSTRUMENT:
                known[: SHORT_HISTORY_START_DAY * BARS_PER_DAY] = False
            thinned = mask & (rng.random(bars) < THINNING_PROBABILITY)
            parent[instrument] = mask
            target[instrument] = thinned
            control[instrument] = mask
        else:
            target[instrument] = mask
            control[instrument] = ~mask
        available[instrument] = known
    return SyntheticRecords(
        scenario=scenario,
        returns=returns,
        target_mask=target,
        control_mask=control,
        available=available,
        parent_mask=parent or None,
    )


def _horizon_outcome(series: np.ndarray, bars: int) -> tuple[np.ndarray, np.ndarray]:
    """The additive path sum over the next ``bars`` bars, and its validity."""
    total = series.size
    prefix = np.concatenate([[0.0], np.cumsum(series)])
    anchors = np.arange(total - 1, dtype=np.int64)
    end = anchors + 1 + bars
    valid = end <= total
    safe = np.minimum(end, total)
    outcome = np.where(valid, prefix[safe] - prefix[anchors + 1], np.nan)
    return outcome, valid


def record_frames(
    records: SyntheticRecords,
    members: Sequence[FamilyMember],
    *,
    cost: float = CONSTANT_COST_PER_OBSERVATION,
) -> Iterator[pd.DataFrame]:
    """Yield the aligned observation frames of one repetition."""
    scenario = records.scenario
    anchors = scenario.days * BARS_PER_DAY - 1
    step = TIMEFRAME_MINUTES * 60_000
    signal_time = scenario.grid_start_ms + (np.arange(anchors, dtype=np.int64) + 1) * step
    inside = (signal_time >= scenario.study_start_ms) & (signal_time < scenario.study_end_ms)
    by_horizon: dict[int, list[FamilyMember]] = {}
    for member in members:
        by_horizon.setdefault(member.horizon_minutes, []).append(member)

    for instrument in INSTRUMENTS:
        series = records.returns[instrument]
        known = records.available[instrument][:anchors]
        for horizon, horizon_members in by_horizon.items():
            outcome, valid = _horizon_outcome(series, horizon // TIMEFRAME_MINUTES)
            return_valid = valid & known & inside
            for member in horizon_members:
                sign = 1.0 if member.direction == "long" else -1.0
                gross = sign * outcome
                net = gross - cost
                if member.kind == "nonsignal_baseline":
                    is_target = records.target_mask[instrument][:anchors]
                    if member.target_variant == PARENT_VARIANT:
                        is_target = records.parent_mask[instrument][:anchors]
                    is_control = ~is_target
                    available = known
                else:
                    is_target = records.target_mask[instrument][:anchors]
                    is_control = records.control_mask[instrument][:anchors]
                    available = known
                effect = scenario.planted_effect
                member_net = net + effect * is_target if effect else net
                member_gross = gross + effect * is_target if effect else gross
                selected = (is_target | is_control) & inside
                if not selected.any():
                    continue
                values_net = np.where(return_valid, member_net, np.nan)
                values_gross = np.where(return_valid, member_gross, np.nan)
                yield pd.DataFrame(
                    {
                        "member_id": member.member_id,
                        "instrument_id": instrument,
                        "signal_time_ms": signal_time[selected],
                        "is_target": is_target[selected],
                        "is_control": is_control[selected],
                        "available": available[selected],
                        "net_return": values_net[selected],
                        "gross_return": values_gross[selected],
                        "return_valid": return_valid[selected],
                    },
                    columns=list(RECORD_COLUMNS),
                )


# --------------------------------------------------------------------------
# one repetition
# --------------------------------------------------------------------------

def run_repetition(
    scenario: Scenario, repetition: int, *, padded_days: int | None = None
) -> dict[str, Any]:
    """Run one repetition through the production numerical boundary."""
    records = generate_records(scenario, repetition)
    members = scenario_family(scenario)
    study_start = scenario.study_start_ms
    study_end = (
        study_start + padded_days * DAY_MS if padded_days else scenario.study_end_ms
    )
    estimates = evaluate_observations(
        record_frames(records, members),
        family=members,
        instruments=INSTRUMENTS,
        study_start_ms=study_start,
        study_end_ms=study_end,
        resamples=scenario.resamples,
        seed=bootstrap_seed(scenario.scenario_id, repetition),
    )
    primary_id = primary_member_id(scenario)
    primary = next(item for item in estimates["members"] if item["member_id"] == primary_id)
    interval = primary["intervals"]["lift"]
    # Already computed by the production bootstrap; retained here rather than
    # recomputed, so no second resampling and no extra random draw is consumed.
    bootstrap = (primary["bootstrap"]["lift"] or {}) if primary["inference_available"] else {}
    family_rejected = any(
        item["nominal_reject_holm"] for item in estimates["members"] if item["inference_available"]
    )
    return {
        "repetition": repetition,
        "primary_available": bool(primary["inference_available"]),
        "primary_reasons": list(primary["unavailable_reasons"]),
        "primary_lift": primary["lift"],
        "primary_p_raw": primary["p_raw"],
        "primary_p_holm": primary["p_holm"],
        "primary_reject_raw": (
            None if primary["p_raw"] is None else bool(primary["p_raw"] <= ALPHA)
        ),
        "primary_reject_holm": primary["nominal_reject_holm"],
        "primary_interval_lower": None if interval is None else interval["lower"],
        "primary_interval_upper": None if interval is None else interval["upper"],
        "primary_interval_width": (
            None if interval is None else interval["upper"] - interval["lower"]
        ),
        "primary_bootstrap_sd": bootstrap.get("standard_deviation"),
        "primary_error_quantile_0025": bootstrap.get("quantile_0025"),
        "primary_error_quantile_0975": bootstrap.get("quantile_0975"),
        "primary_geometry": dict(primary["geometry"]),
        "primary_retained_targets": primary["supported_population"][
            "retained_target_observations"
        ],
        "family_any_rejection": bool(family_rejected),
        "family_available": sum(
            1 for item in estimates["members"] if item["inference_available"]
        ),
        "family_size": estimates["family_size"],
    }


# --------------------------------------------------------------------------
# the evidence replay
# --------------------------------------------------------------------------

REPLAY_MODEL_INSTANCE = "synthetic"
REPLAY_CONDITION_ID = "c_signal"
REPLAY_INSTRUMENT_PREFIX = "SYN_"


def _replay_family(scenario: Scenario, *, occurrence: str) -> tuple[FamilyMember, ...]:
    """The replay's family, keyed to the built-in model instance it writes."""
    return tuple(
        member for member in scenario_family(scenario)
        if member.comparison_id == "baseline__signal"
    )


def _replay_model_instance() -> dict[str, Any]:
    from ..study import builtins as study_builtins

    settings = study_builtins.validate_fixed_horizon_settings(
        {
            "directions": list(DIRECTIONS),
            "commission_pct_per_side": 0.0,
            "by_timeframe": {
                str(TIMEFRAME_MINUTES): {
                    "horizons_minutes": list(HORIZON_MINUTES),
                    "primary_horizon_minutes": PRIMARY_HORIZON_MINUTES,
                }
            },
        },
        [TIMEFRAME_MINUTES],
    )
    cases = [
        case.as_json()
        for case in study_builtins.resolve_fixed_horizon_cases(settings, TIMEFRAME_MINUTES)
    ]
    return {
        "model_instance_id": REPLAY_MODEL_INSTANCE,
        "model_id": study_builtins.FIXED_HORIZON_MODEL_ID,
        "model_version": "1",
        "evidence_kind": "fixed_horizon_path_v1",
        "settings": settings,
        "cases": {str(TIMEFRAME_MINUTES): cases},
    }


def _replay_tables(
    records: SyntheticRecords,
    instrument: str,
    *,
    occurrence: str,
    unknown_days: Sequence[int],
) -> dict[str, pd.DataFrame]:
    """Evidence-shaped conditions, emissions and primitives for one instrument.

    Entry is fixed at 1.0 and the exit is ``1 + long gross``, so the built-in
    expansion derives exactly the synthetic outcome from the saved primitives.
    """
    scenario = records.scenario
    bars = scenario.days * BARS_PER_DAY
    anchors = bars - 1
    step = TIMEFRAME_MINUTES * 60_000
    anchor_open = scenario.grid_start_ms + np.arange(anchors, dtype=np.int64) * step
    signal_time = anchor_open + step

    value = records.target_mask[instrument][:anchors].copy()
    valid = np.ones(anchors, dtype=bool)
    for day in unknown_days:
        valid[day * BARS_PER_DAY : (day + 1) * BARS_PER_DAY] = False
    value = value & valid

    if occurrence == "state_entry":
        emitted = np.zeros(anchors, dtype=bool)
        emitted[1:] = value[1:] & valid[:-1] & ~value[:-1]
    else:
        emitted = value & valid

    conditions = pd.DataFrame(
        {
            "instrument_id": instrument,
            "timeframe_minutes": np.full(anchors, TIMEFRAME_MINUTES, dtype=np.int64),
            "condition_id": REPLAY_CONDITION_ID,
            "anchor_open_ms": anchor_open,
            "signal_time_ms": signal_time,
            "value": value,
            "valid": valid,
            "episode_id": None,
        }
    )
    selected = np.flatnonzero(emitted)
    emissions = pd.DataFrame(
        {
            "instrument_id": instrument,
            "timeframe_minutes": np.full(selected.size, TIMEFRAME_MINUTES, dtype=np.int64),
            "variant_id": SINGLE_VARIANT,
            "condition_id": REPLAY_CONDITION_ID,
            "occurrence": occurrence,
            "anchor_open_ms": anchor_open[selected],
            "signal_time_ms": signal_time[selected],
            "event_id": [f"e{int(item)}" for item in selected],
            "episode_id": None,
        }
    )

    series = records.returns[instrument]
    known = records.available[instrument][:anchors]
    frames = []
    for horizon in HORIZON_MINUTES:
        outcome, valid_return = _horizon_outcome(series, horizon // TIMEFRAME_MINUTES)
        available = valid_return & known
        exit_price = np.where(available, 1.0 + outcome, np.nan)
        reason = np.where(available, "available", "incomplete_path").astype(object)
        frames.append(
            pd.DataFrame(
                {
                    "instrument_id": instrument,
                    "timeframe_minutes": np.full(anchors, TIMEFRAME_MINUTES, dtype=np.int64),
                    "model_instance_id": REPLAY_MODEL_INSTANCE,
                    "anchor_open_ms": anchor_open,
                    "signal_time_ms": signal_time,
                    "horizon_minutes": np.full(anchors, horizon, dtype=np.int64),
                    "entry_time_ms": signal_time,
                    "exit_time_ms": signal_time + horizon * 60_000,
                    "entry_price": np.where(available, 1.0, np.nan),
                    "exit_price": exit_price,
                    "path_high": exit_price,
                    "path_low": exit_price,
                    "return_valid": available,
                    "return_reason": reason,
                    "path_valid": available,
                    "path_reason": reason,
                }
            )
        )
    return {
        "conditions": conditions,
        "emissions": emissions,
        "primitives": pd.concat(frames, ignore_index=True),
    }


def build_replay_source(
    records: SyntheticRecords,
    root: Any,
    *,
    occurrence: str = "every_qualifying_bar",
    unknown_days: Sequence[int] = (),
):
    """Write evidence-shaped tables and return an admitted source over them.

    The tables are decoded again through the production reader, so the checked
    condition/emission alignment, the shared case expansion and the membership
    mapping are the real ones.  This is a bounded in-memory replay, not a sealed
    study publication.
    """
    from ..study import evidence as study_evidence
    from ..study import results as study_results
    from .source import AdmittedSource

    scenario = records.scenario
    base = Path(root)
    instance = _replay_model_instance()
    for instrument in INSTRUMENTS:
        tables = _replay_tables(
            records, instrument, occurrence=occurrence, unknown_days=unknown_days
        )
        directory = study_evidence.job_path(base, instrument)
        directory.mkdir(parents=True, exist_ok=True)
        for name, frame in tables.items():
            study_evidence.write_table(directory / f"{name}.parquet", frame, name=name)
    family = {
        "instruments": [{"instrument_id": item} for item in INSTRUMENTS],
        "timeframes_minutes": [TIMEFRAME_MINUTES],
        "variants": [
            {
                "variant_id": SINGLE_VARIANT,
                "hypothesis_id": "synthetic_signal",
                "occurrence": occurrence,
                "condition_id": REPLAY_CONDITION_ID,
            }
        ],
        "models": [instance],
    }
    results = study_results.StudyResults(
        run_root=base,
        request={"study_name": "replay", "study": {}},
        protocol={},
        family=family,
        source={"evidence_view_version": 1},
        provenance={"identities": {}},
        status={},
        completion=None,
        complete=True,
        jobs={item: {} for item in INSTRUMENTS},
        job_states={item: "completed" for item in INSTRUMENTS},
        counts={},
    )
    return AdmittedSource(
        run_root=base,
        results=results,
        variants=tuple(family["variants"]),
        instances={REPLAY_MODEL_INSTANCE: instance},
        timeframes=(TIMEFRAME_MINUTES,),
        instruments=tuple(INSTRUMENTS),
        study_start_ms=scenario.study_start_ms,
        study_end_ms=scenario.study_end_ms,
    )


def run_replay_repetition(
    scenario: Scenario, repetition: int, root: Any, **replay
) -> dict[str, Any]:
    """Compare the production join path with the numerical boundary path.

    Both paths consume the same synthetic returns and masks.  The replay's model
    instance charges no commission, so the records the production expansion
    derives are the same values the direct path supplies.
    """
    from .source import RecordSource

    records = generate_records(scenario, repetition)
    members = _replay_family(scenario, occurrence=replay.get("occurrence", "every_qualifying_bar"))
    source = build_replay_source(records, root, **replay)
    seed = bootstrap_seed(scenario.scenario_id, repetition)
    joined = evaluate_observations(
        RecordSource(source, members).frames(),
        family=members,
        instruments=INSTRUMENTS,
        study_start_ms=scenario.study_start_ms,
        study_end_ms=scenario.study_end_ms,
        resamples=scenario.resamples,
        seed=seed,
    )
    direct = evaluate_observations(
        record_frames(records, members, cost=0.0),
        family=members,
        instruments=INSTRUMENTS,
        study_start_ms=scenario.study_start_ms,
        study_end_ms=scenario.study_end_ms,
        resamples=scenario.resamples,
        seed=seed,
    )
    return {"repetition": repetition, "joined": joined, "direct": direct}


def replay_difference(joined: Mapping[str, Any], direct: Mapping[str, Any]) -> dict[str, Any]:
    """The largest disagreement between the two paths, per compared field."""
    fields = ("signal", "control", "lift", "p_raw", "p_holm")
    worst: dict[str, float] = {name: 0.0 for name in fields}
    mismatched: list[str] = []
    for left, right in zip(joined["members"], direct["members"]):
        if left["member_id"] != right["member_id"]:
            mismatched.append(f"{left['member_id']} != {right['member_id']}")
            continue
        if left["inference_available"] != right["inference_available"]:
            mismatched.append(f"{left['member_id']}: availability differs")
        if left["unavailable_reasons"] != right["unavailable_reasons"]:
            mismatched.append(f"{left['member_id']}: reasons differ")
        for name in fields:
            if left[name] is None or right[name] is None:
                if left[name] is not right[name]:
                    mismatched.append(f"{left['member_id']}.{name}: one side is null")
                continue
            worst[name] = max(worst[name], abs(float(left[name]) - float(right[name])))
    return {"max_absolute_difference": worst, "mismatched": mismatched}


def _state_entry_replay(root: Any) -> dict[str, Any]:
    """One deterministic replay with state_entry emissions and unknown history.

    This mapping changes the null population, so it is reported separately from
    the unmodified null-rate series: it checks the membership contract, not an
    error rate.
    """
    import shutil

    scenario = SCENARIOS_BY_NAME["null_dependent_t5"]
    unknown = (10, 11, 12, 130, 131, 250)
    base = Path(root)
    try:
        outcome = run_replay_repetition(
            scenario, 0, base, occurrence="state_entry", unknown_days=unknown
        )
    finally:
        shutil.rmtree(base, ignore_errors=True)
    primary = next(
        item for item in outcome["joined"]["members"]
        if item["member_id"] == primary_member_id(scenario)
    )
    counts = primary["counts"]
    return {
        "occurrence": "state_entry",
        "unknown_days": list(unknown),
        "primary_member_id": primary["member_id"],
        "records_supplied": counts["records_supplied"],
        "target_rows": counts["target_rows"],
        "target_unavailable": counts["target_unavailable"],
        "control_rows": counts["control_rows"],
        "control_unavailable": counts["control_unavailable"],
        "valid_target_available": counts["valid_target_available"],
        "inference_available": primary["inference_available"],
        "unavailable_reasons": list(primary["unavailable_reasons"]),
        "note": (
            "A known-true nonemitting state_entry anchor belongs to neither group and unknown "
            "history is never a false control, so the record set is smaller than the anchor set. "
            "This mapping changes the null population and is kept out of the null-rate series."
        ),
    }


def run_evidence_replay(
    *, repetitions: int = 25, root: Any, scenario_name: str = "null_dependent_t5", progress=None
) -> dict[str, Any]:
    """Route fixed repetitions through the production checked joins."""
    import shutil

    scenario = SCENARIOS_BY_NAME[scenario_name]
    base = Path(root)
    base.mkdir(parents=True, exist_ok=True)
    worst = {name: 0.0 for name in ("signal", "control", "lift", "p_raw", "p_holm")}
    mismatched: list[str] = []
    started = time.monotonic()
    for repetition in range(repetitions):
        directory = base / f"replay-{repetition:03d}"
        try:
            outcome = run_replay_repetition(scenario, repetition, directory)
            difference = replay_difference(outcome["joined"], outcome["direct"])
            for name, value in difference["max_absolute_difference"].items():
                worst[name] = max(worst[name], value)
            mismatched.extend(difference["mismatched"])
        finally:
            shutil.rmtree(directory, ignore_errors=True)
        if progress is not None:
            progress(f"evidence replay {repetition + 1}/{repetitions}")
    return {
        "scenario": scenario.name,
        "repetitions": repetitions,
        "max_absolute_difference": worst,
        "mismatched": mismatched,
        "agrees": not mismatched and max(worst.values()) <= 1e-12,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "note": (
            "The production checked joins and the numerical boundary consume the same synthetic "
            "returns and masks. This is a bounded in-memory replay, not a sealed study and "
            "analysis publication; section 9.1 owns the separate end-to-end artifact tests."
        ),
    }


# --------------------------------------------------------------------------
# rate arithmetic
# --------------------------------------------------------------------------

def wilson_interval(successes: int, trials: int, *, level: float = 0.95) -> dict[str, Any]:
    """A two-sided Wilson score interval; an empty denominator is explicit."""
    if trials <= 0:
        return {"lower": None, "upper": None, "level": level}
    from scipy.stats import norm

    z = float(norm.ppf(0.5 + level / 2))
    proportion = successes / trials
    denominator = 1.0 + z * z / trials
    centre = (proportion + z * z / (2 * trials)) / denominator
    margin = (
        z
        * np.sqrt(proportion * (1 - proportion) / trials + z * z / (4 * trials * trials))
        / denominator
    )
    return {
        "lower": float(max(0.0, centre - margin)),
        "upper": float(min(1.0, centre + margin)),
        "level": level,
    }


def exact_upper_bound(successes: int, trials: int, *, level: float = 0.95) -> float | None:
    """The one-sided exact binomial upper confidence bound on the true rate."""
    if trials <= 0:
        return None
    if successes >= trials:
        return 1.0
    from scipy.stats import beta

    return float(beta.ppf(level, successes + 1, trials - successes))


def rate_record(name: str, successes: int, trials: int) -> dict[str, Any]:
    bound = exact_upper_bound(successes, trials)
    return {
        "name": name,
        "events": int(successes),
        "denominator": int(trials),
        "rate": (successes / trials) if trials else None,
        "wilson_95": wilson_interval(successes, trials),
        "one_sided_95_upper_bound": bound,
        "within_envelope": None if bound is None else bool(bound <= ERROR_ENVELOPE),
    }


# --------------------------------------------------------------------------
# the driver-owned attempt ledger
# --------------------------------------------------------------------------

UNPADDED_VARIANT = "unpadded"
PADDED_VARIANT = "padded_365"


def id_ranges(values: Sequence[int]) -> list[list[int]]:
    """Compress sorted-unique repetition IDs into inclusive ``[first, last]`` runs.

    The ledger records identities, not only counts, so a substituted or repeated
    attempt is visible; contiguous runs keep that record small.
    """
    ordered = sorted({int(item) for item in values})
    runs: list[list[int]] = []
    for item in ordered:
        if runs and item == runs[-1][1] + 1:
            runs[-1][1] = item
        else:
            runs.append([item, item])
    return runs


def _id_record(values: Sequence[int]) -> dict[str, Any]:
    ordered = [int(item) for item in values]
    unique = sorted(set(ordered))
    return {
        "count": len(ordered),
        "unique_count": len(unique),
        "first": unique[0] if unique else None,
        "last": unique[-1] if unique else None,
        "ranges": id_ranges(unique),
    }


def scenario_config_digest(scenario: Scenario) -> str:
    """The digest of one fixture's whole frozen configuration."""
    return contracts.semantic_digest(asdict(scenario))


def attempt_ledger(
    scenario: Scenario,
    *,
    padded_days: int | None,
    requested: int,
    attempted: Sequence[int],
    completed: Sequence[int],
) -> dict[str, Any]:
    """One executed entry's identity, attempts and status.

    ``run_scenario``'s aggregates cannot prove completeness: matching counts or a
    matching name do not establish that the declared repetition IDs were the ones
    actually run, at the declared configuration.  This ledger is owned by the
    driver and is what the protocol gate binds to.
    """
    reasons: list[str] = []
    attempted_record = _id_record(attempted)
    completed_record = _id_record(completed)
    if attempted_record["count"] != attempted_record["unique_count"]:
        reasons.append("duplicate_attempt_id")
    if completed_record["count"] != completed_record["unique_count"]:
        reasons.append("duplicate_completed_id")
    if set(completed) - set(attempted):
        reasons.append("completed_id_was_never_attempted")
    if attempted_record["count"] != int(requested):
        reasons.append("attempted_count_differs_from_requested")
    if completed_record["count"] != attempted_record["count"]:
        reasons.append("incomplete_attempts")
    return {
        "fixture_id": int(scenario.scenario_id),
        "fixture_name": scenario.name,
        "variant": PADDED_VARIANT if padded_days else UNPADDED_VARIANT,
        "padded_days": int(padded_days) if padded_days else None,
        "requested_repetitions": int(requested),
        "declared_repetitions": int(scenario.repetitions),
        "resamples": int(scenario.resamples),
        "config_digest": scenario_config_digest(scenario),
        "attempted": attempted_record,
        "completed": completed_record,
        "status": "completed" if not reasons else "incomplete",
        "reasons": reasons,
    }


# --------------------------------------------------------------------------
# one scenario
# --------------------------------------------------------------------------

def run_scenario(
    scenario: Scenario,
    *,
    repetitions: int | None = None,
    padded_days: int | None = None,
    progress=None,
) -> dict[str, Any]:
    """Run one scenario's repetitions, score its rates and record its ledger."""
    total = int(repetitions if repetitions is not None else scenario.repetitions)
    started = time.monotonic()
    rows: list[dict[str, Any]] = []
    attempted: list[int] = []
    for repetition in range(total):
        attempted.append(repetition)
        rows.append(run_repetition(scenario, repetition, padded_days=padded_days))
        if progress is not None and (repetition + 1) % 50 == 0:
            progress(
                f"{scenario.name}: {repetition + 1}/{total} "
                f"({time.monotonic() - started:.0f}s)"
            )

    available = [item for item in rows if item["primary_available"]]
    reject_raw = sum(1 for item in available if item["primary_reject_raw"])
    truth = float(scenario.planted_effect)
    noncoverage = sum(
        1
        for item in available
        if not (item["primary_interval_lower"] <= truth <= item["primary_interval_upper"])
    )
    family_rejections = sum(1 for item in rows if item["family_any_rejection"])
    holm_rejections = sum(1 for item in available if item["primary_reject_holm"])
    lifts = np.array(
        [item["primary_lift"] for item in rows if item["primary_lift"] is not None],
        dtype=np.float64,
    )
    widths = np.array(
        [item["primary_interval_width"] for item in available], dtype=np.float64
    )
    # A direct scale diagnostic over one shared population: the available
    # primaries that published a finite bootstrap SD and lift. Numerator and
    # denominator use exactly these samples, which is not the `effect.*`
    # population of every repetition with a non-null lift.
    diagnostic = [
        item
        for item in available
        if item["primary_bootstrap_sd"] is not None
        and item["primary_lift"] is not None
        and np.isfinite(item["primary_bootstrap_sd"])
        and np.isfinite(item["primary_lift"])
    ]
    diagnostic_sd = np.array(
        [item["primary_bootstrap_sd"] for item in diagnostic], dtype=np.float64
    )
    diagnostic_lifts = np.array(
        [item["primary_lift"] for item in diagnostic], dtype=np.float64
    )
    empirical_sd = float(np.std(diagnostic_lifts, ddof=1)) if diagnostic_lifts.size > 1 else None
    mean_bootstrap_sd = float(np.mean(diagnostic_sd)) if diagnostic_sd.size else None
    rms_bootstrap_sd = (
        float(np.sqrt(np.mean(np.square(diagnostic_sd)))) if diagnostic_sd.size else None
    )
    reasons: dict[str, int] = {}
    for item in rows:
        for reason in item["primary_reasons"]:
            reasons[reason] = reasons.get(reason, 0) + 1

    availability = len(available) / total if total else None
    bias = float(np.mean(lifts) - truth) if lifts.size else None
    monte_carlo = None
    if lifts.size > 1:
        from scipy.stats import norm

        half = float(norm.ppf(0.995)) * float(np.std(lifts, ddof=1)) / np.sqrt(lifts.size)
        centre = float(np.mean(lifts))
        monte_carlo = {
            "level": 0.99,
            "mean_lift": centre,
            "lower": centre - half,
            "upper": centre + half,
            "contains_true_effect": bool(centre - half <= truth <= centre + half),
        }

    record = {
        "scenario_id": scenario.scenario_id,
        "name": scenario.name + ("_padded_365" if padded_days else ""),
        "kind": scenario.kind,
        "admitted": bool(scenario.admitted and padded_days is None),
        "caveat": scenario.caveat or None,
        "config": asdict(scenario),
        "ledger": attempt_ledger(
            scenario,
            padded_days=padded_days,
            requested=total,
            attempted=attempted,
            completed=[int(item["repetition"]) for item in rows],
        ),
        "evaluation_days": padded_days or (scenario.evaluation_days or scenario.days),
        "repetitions": total,
        "resamples": scenario.resamples,
        "primary_member_id": primary_member_id(scenario),
        "primary_availability": availability,
        "primary_availability_meets_floor": (
            None if availability is None else bool(availability >= MIN_PRIMARY_AVAILABILITY)
        ),
        "refusal_reasons": dict(sorted(reasons.items())),
        "rates": {
            "primary_raw_rejection": rate_record(
                "primary_raw_rejection", reject_raw, len(available)
            ),
            "primary_interval_noncoverage": rate_record(
                "primary_interval_noncoverage", noncoverage, len(available)
            ),
            "family_wise_holm_rejection": rate_record(
                "family_wise_holm_rejection", family_rejections, total
            ),
        },
        "primary_holm_rejections": {
            "events": holm_rejections,
            "denominator": len(available),
            "rate": (holm_rejections / len(available)) if available else None,
        },
        "effect": {
            "true_lift": truth,
            "mean_lift": float(np.mean(lifts)) if lifts.size else None,
            "bias": bias,
            "monte_carlo_99": monte_carlo,
        },
        "interval_width": {
            "n": int(widths.size),
            "mean": float(np.mean(widths)) if widths.size else None,
            "median": float(np.median(widths)) if widths.size else None,
            "p10": float(np.quantile(widths, 0.10)) if widths.size else None,
            "p90": float(np.quantile(widths, 0.90)) if widths.size else None,
        },
        "bootstrap_scale": {
            "n": len(diagnostic),
            "population": (
                "Available primaries with a finite published bootstrap SD and lift. The ratio "
                "uses these same samples in its numerator and denominator; it is a standard-"
                "deviation diagnostic on this scenario's own repetitions, not a variance factor "
                "and not the effect.* population."
            ),
            "empirical_lift_sd": empirical_sd,
            "mean_bootstrap_sd": mean_bootstrap_sd,
            "rms_bootstrap_sd": rms_bootstrap_sd,
            "mean_bootstrap_sd_over_empirical_sd": (
                mean_bootstrap_sd / empirical_sd
                if mean_bootstrap_sd is not None and empirical_sd
                else None
            ),
        },
        "supported_geometry": _geometry_summary(rows),
        "published_inference": {
            "repetitions_with_p_value": sum(1 for item in rows if item["primary_p_raw"] is not None),
            "repetitions_with_interval": sum(
                1 for item in rows if item["primary_interval_lower"] is not None
            ),
        },
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    return record


def _geometry_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    keys = ("day_grid_days", "supported_span_days", "joint_active_days", "supported_blocks")
    summary: dict[str, Any] = {}
    for key in keys:
        values = np.array([item["primary_geometry"][key] for item in rows], dtype=np.float64)
        summary[key] = {
            "min": float(values.min()) if values.size else None,
            "median": float(np.median(values)) if values.size else None,
            "max": float(values.max()) if values.size else None,
        }
    targets = np.array([item["primary_retained_targets"] for item in rows], dtype=np.float64)
    summary["retained_target_observations"] = {
        "min": float(targets.min()) if targets.size else None,
        "median": float(np.median(targets)) if targets.size else None,
        "max": float(targets.max()) if targets.size else None,
    }
    return summary


# --------------------------------------------------------------------------
# acceptance
# --------------------------------------------------------------------------

ACCEPTANCE_RATES = (
    "primary_raw_rejection",
    "primary_interval_noncoverage",
    "family_wise_holm_rejection",
)


def score_acceptance(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Score the rate checks of whatever admitted records it is given.

    This is a reusable **subset** calculation, not a release gate: it reports
    whether every check of the records supplied passed, and it cannot know which
    fixtures a named validation contract requires.  Supplying one passing
    fixture, or the same fixture twice, legitimately returns
    ``all_requested_checks_passed`` — which is why the release decision belongs
    to :func:`legacy_protocol_state` and never to this Boolean.
    """
    checks: list[dict[str, Any]] = []
    for record in results:
        if not record["admitted"]:
            continue
        for name in ACCEPTANCE_RATES:
            rate = record["rates"][name]
            checks.append(
                {
                    "scenario": record["name"],
                    "rate": name,
                    "events": rate["events"],
                    "denominator": rate["denominator"],
                    "observed_rate": rate["rate"],
                    "one_sided_95_upper_bound": rate["one_sided_95_upper_bound"],
                    "ceiling": ERROR_ENVELOPE,
                    "passed": rate["within_envelope"],
                }
            )
    availability = [
        {
            "scenario": record["name"],
            "availability": record["primary_availability"],
            "floor": MIN_PRIMARY_AVAILABILITY,
            "passed": record["primary_availability_meets_floor"],
        }
        for record in results
        if record["admitted"]
    ]
    return {
        "scope": "requested_subset",
        "ceiling": ERROR_ENVELOPE,
        "availability_floor": MIN_PRIMARY_AVAILABILITY,
        "checks": checks,
        "checks_total": len(checks),
        "checks_passed": sum(1 for item in checks if item["passed"]),
        "availability_checks": availability,
        "all_requested_checks_passed": bool(
            checks
            and all(item["passed"] for item in checks)
            and all(item["passed"] for item in availability)
        ),
        "meaning": (
            "Each bound is a separate one-sided 95% exact binomial upper bound and the requirement "
            "is their intersection. The collection is not a joint 95% statement, the 8% ceiling is "
            "not a test alpha or a 92% interval, and passing does not imply the true error rate "
            "equals 5%. These checks cover exactly the admitted records supplied: a subset that "
            "passes is a diagnostic observation, never release acceptance."
        ),
    }


# --------------------------------------------------------------------------
# the versioned release gate of the legacy bootstrap protocol
# --------------------------------------------------------------------------

def legacy_protocol_plan() -> tuple[dict[str, Any], ...]:
    """The exact entries a complete legacy-protocol run must contain.

    Refusal fixtures are run twice by the driver: once as declared and once with
    the same records embedded in a padded 365-day grid.  The padded entry is
    generated here, not requested by a caller, so the gate expects it.
    """
    planned: list[dict[str, Any]] = []
    for name in LEGACY_PROTOCOL_SCENARIOS:
        scenario = SCENARIOS_BY_NAME[name]
        planned.append(
            {
                "name": scenario.name,
                "fixture_id": scenario.scenario_id,
                "variant": UNPADDED_VARIANT,
                "repetitions": scenario.repetitions,
                "resamples": scenario.resamples,
                "config_digest": scenario_config_digest(scenario),
                "kind": scenario.kind,
                "driver_generated": False,
            }
        )
        if scenario.kind == "refusal":
            planned.append(
                {
                    "name": f"{scenario.name}_padded_365",
                    "fixture_id": scenario.scenario_id,
                    "variant": PADDED_VARIANT,
                    "repetitions": scenario.repetitions,
                    "resamples": scenario.resamples,
                    "config_digest": scenario_config_digest(scenario),
                    "kind": scenario.kind,
                    "driver_generated": True,
                }
            )
    return tuple(planned)


def _contract_without_implementation(contract: Mapping[str, Any]) -> dict[str, Any]:
    """The semantic part of a generator contract, without physical digests."""
    return {
        key: value
        for key, value in contract.items()
        if key not in ("implementation", "generator_digest")
    }


def _eligibility_reasons(document: Mapping[str, Any]) -> list[str]:
    """Why this document is, or is not, a complete legacy-protocol run."""
    reasons: list[str] = []
    if document.get("schema_version") != CALIBRATION_SCHEMA_VERSION:
        reasons.append(
            f"schema_version {document.get('schema_version')!r} is not the versioned "
            f"complete-run contract {CALIBRATION_SCHEMA_VERSION}"
        )
    plan = document.get("run_plan")
    if not isinstance(plan, Mapping):
        reasons.append("the document carries no driver-owned run plan")
    else:
        if plan.get("protocol") != LEGACY_PROTOCOL:
            reasons.append(
                f"the run plan declares protocol {plan.get('protocol')!r}, not {LEGACY_PROTOCOL!r}"
            )
        if plan.get("repetition_override") is not None:
            reasons.append(
                f"a repetition override of {plan['repetition_override']!r} makes this a smoke run"
            )
        if plan.get("replay_repetitions") != LEGACY_REPLAY_REPETITIONS:
            reasons.append(
                f"the replay ran {plan.get('replay_repetitions')!r} repetitions, not the declared "
                f"{LEGACY_REPLAY_REPETITIONS}"
            )

    contract = document.get("generator_contract")
    if not isinstance(contract, Mapping):
        reasons.append("the document carries no generator contract")
    else:
        recorded = contract.get("generator_digest")
        recomputed = contracts.semantic_digest(
            {key: value for key, value in contract.items() if key != "generator_digest"}
        )
        if recorded != recomputed:
            reasons.append("the recorded generator digest does not match its own contract")
        if _contract_without_implementation(contract) != _contract_without_implementation(
            generator_contract()
        ):
            reasons.append(
                "the recorded generator contract differs from this driver's frozen settings"
            )

    expected = legacy_protocol_plan()
    by_name: dict[str, list[Mapping[str, Any]]] = {}
    for record in document.get("results", ()):
        if not isinstance(record, Mapping):
            reasons.append("a result row is not an object")
            continue
        by_name.setdefault(str(record.get("name")), []).append(record)
    for entry in expected:
        found = by_name.pop(entry["name"], [])
        if not found:
            reasons.append(f"required entry {entry['name']!r} is missing")
            continue
        if len(found) > 1:
            reasons.append(f"required entry {entry['name']!r} appears {len(found)} times")
            continue
        reasons.extend(_entry_reasons(found[0], entry))
    for name in sorted(by_name):
        reasons.append(f"unexpected entry {name!r} is not part of this protocol")

    replay = document.get("evidence_replay")
    if not isinstance(replay, Mapping):
        reasons.append("the required evidence replay is absent")
    else:
        if replay.get("repetitions") != LEGACY_REPLAY_REPETITIONS:
            reasons.append(
                f"the evidence replay ran {replay.get('repetitions')!r} repetitions, not "
                f"{LEGACY_REPLAY_REPETITIONS}"
            )
        if not replay.get("agrees"):
            reasons.append("the production joins and the numerical boundary do not agree")
    state_entry = document.get("state_entry_replay")
    if not isinstance(state_entry, Mapping):
        reasons.append("the required state-entry replay is absent")
    elif not state_entry.get("records_supplied"):
        reasons.append("the state-entry replay supplied no records")
    return reasons


def _entry_reasons(record: Mapping[str, Any], entry: Mapping[str, Any]) -> list[str]:
    """Bind one executed result to the driver-owned plan entry it claims to be."""
    name = entry["name"]
    reasons: list[str] = []
    ledger = record.get("ledger")
    if not isinstance(ledger, Mapping):
        reasons.append(f"{name}: no attempt ledger; counts alone do not prove completeness")
        return reasons
    if ledger.get("status") != "completed":
        detail = ", ".join(ledger.get("reasons") or ["unspecified"])
        reasons.append(f"{name}: the ledger status is {ledger.get('status')!r} ({detail})")
    for key in ("fixture_id", "variant", "config_digest", "resamples"):
        if ledger.get(key) != entry.get(key, ledger.get(key)) and key in entry:
            reasons.append(
                f"{name}: ledger {key} {ledger.get(key)!r} does not match the planned "
                f"{entry[key]!r}"
            )
    if ledger.get("requested_repetitions") != entry["repetitions"]:
        reasons.append(
            f"{name}: {ledger.get('requested_repetitions')!r} requested repetitions instead of "
            f"the declared {entry['repetitions']}"
        )
    attempted = ledger.get("attempted") or {}
    if attempted.get("ranges") != [[0, entry["repetitions"] - 1]]:
        reasons.append(
            f"{name}: the attempted repetition IDs are {attempted.get('ranges')!r}, not the "
            f"declared contiguous block [[0, {entry['repetitions'] - 1}]]"
        )
    if (ledger.get("completed") or {}).get("ranges") != attempted.get("ranges"):
        reasons.append(f"{name}: the completed repetition IDs differ from the attempted ones")

    repetitions = record.get("repetitions")
    if repetitions != entry["repetitions"]:
        reasons.append(f"{name}: the record reports {repetitions!r} repetitions")
    availability = record.get("primary_availability")
    rates = record.get("rates")
    if not isinstance(rates, Mapping) or set(rates) != set(ACCEPTANCE_RATES):
        reasons.append(f"{name}: the record does not publish the three declared rates")
        return reasons
    family = rates["family_wise_holm_rejection"]
    if family["denominator"] != repetitions:
        reasons.append(
            f"{name}: the family-wise denominator {family['denominator']!r} is not every attempted "
            f"repetition"
        )
    available = rates["primary_raw_rejection"]["denominator"]
    if rates["primary_interval_noncoverage"]["denominator"] != available:
        reasons.append(f"{name}: the two available-primary denominators disagree")
    if (
        isinstance(repetitions, int)
        and repetitions
        and availability is not None
        and available != round(float(availability) * repetitions)
    ):
        reasons.append(
            f"{name}: the available-primary denominator {available!r} contradicts the reported "
            f"availability {availability!r}"
        )
    for key, rate in rates.items():
        if rate["events"] > rate["denominator"]:
            reasons.append(f"{name}: {key} counts more events than its denominator")

    if entry["kind"] == "refusal":
        published = record.get("published_inference") or {}
        if published.get("repetitions_with_p_value") or published.get("repetitions_with_interval"):
            reasons.append(f"{name}: a required deterministic refusal published inference")
    return reasons


def legacy_protocol_state(document: Mapping[str, Any]) -> dict[str, Any]:
    """The release decision of the legacy bootstrap calibration command.

    Three questions are answered separately and never collapsed: whether the
    requested rate checks passed, whether the run is a *complete* run of this
    named protocol, and whether that complete run is accepted.  A subset, a smoke
    repetition override, an omitted required replay or an unfinished run is
    diagnostic-only and can never return ``accepted``.
    """
    diagnostic = score_acceptance(document.get("results", ()))
    reasons = _eligibility_reasons(document)
    complete = not reasons
    accepted_reasons: list[str] = list(reasons)
    if not diagnostic["all_requested_checks_passed"]:
        failed = [item for item in diagnostic["checks"] if not item["passed"]]
        accepted_reasons.extend(
            f"{item['scenario']}: {item['rate']} upper bound "
            f"{item['one_sided_95_upper_bound']} exceeds {item['ceiling']}"
            for item in failed
        )
        accepted_reasons.extend(
            f"{item['scenario']}: primary availability {item['availability']} is below "
            f"{item['floor']}"
            for item in diagnostic["availability_checks"]
            if not item["passed"]
        )
        if not failed and not diagnostic["checks"]:
            accepted_reasons.append("no admitted rate check was scored")
    return {
        "gate_version": LEGACY_GATE_VERSION,
        "protocol": LEGACY_PROTOCOL,
        "required_entries": [item["name"] for item in legacy_protocol_plan()],
        "diagnostic": diagnostic,
        "diagnostic_checks_passed": bool(diagnostic["all_requested_checks_passed"]),
        "complete_run": complete,
        "eligibility_reasons": reasons,
        "accepted": bool(complete and diagnostic["all_requested_checks_passed"]),
        "accepted_reasons": accepted_reasons,
        "meaning": (
            "A pass of this gate is a pass of the named legacy_bootstrap_v1 protocol on its "
            "declared synthetic fixtures, and nothing else. Completeness is bound to the "
            "driver-owned fixture, generator and attempt ledger, not to result-supplied names, "
            "counts or admitted flags. Stress and planted fixtures are required disclosures whose "
            "rates are deliberately outside the admitted-null ceiling, and legacy scenario 3 "
            "carries a documented tiny population-null mismatch that qualifies its number without "
            "waiving its check."
        ),
    }


# --------------------------------------------------------------------------
# the driver
# --------------------------------------------------------------------------

def generator_contract() -> dict[str, Any]:
    """Every frozen generator setting, recorded before any scoring."""
    return {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "master_seed": MASTER_SEED,
        "seed_rule": (
            "SeedSequence([20260920, scenario_id, repetition_id, stream_id]); stream 0 builds the "
            "data generator directly and stream 1 derives the request bootstrap seed as "
            "int(ss.generate_state(1, dtype=np.uint32)[0])."
        ),
        "timeframe_minutes": TIMEFRAME_MINUTES,
        "bars_per_day": BARS_PER_DAY,
        "instruments": list(INSTRUMENTS),
        "horizons_minutes": list(HORIZON_MINUTES),
        "directions": list(DIRECTIONS),
        "primary_member": f"{PRIMARY_DIRECTION} {PRIMARY_HORIZON_MINUTES}m",
        "common_loading": COMMON_LOADING,
        "individual_loading": INDIVIDUAL_LOADING,
        "daily_ar_coefficient": DAILY_AR_COEFFICIENT,
        "daily_level_scale": DAILY_LEVEL_SCALE,
        "bar_innovation_scale": BAR_INNOVATION_SCALE,
        "student_t_df": STUDENT_T_DF,
        "ar_initialization": "stationary law, with no discarded burn-in",
        "signal_p11": SIGNAL_P11,
        "signal_p10": SIGNAL_P10,
        "signal_stationary_probability": SIGNAL_P10 / (SIGNAL_P10 + (1.0 - SIGNAL_P11)),
        "signal_initialization": "stationary distribution",
        "bar_target_probability_state_1": SIGNAL_TARGET_IN_STATE_1,
        "bar_target_probability_state_0": SIGNAL_TARGET_IN_STATE_0,
        "independent_signal_probability": INDEPENDENT_SIGNAL_PROBABILITY,
        "constant_cost_per_observation": CONSTANT_COST_PER_OBSERVATION,
        "confounded_month_shift": CONFOUNDED_MONTH_SHIFT,
        "confounded_probabilities": list(CONFOUNDED_PROBABILITIES),
        "confounded_probability_month_ownership": {
            "null_conditional_confounded": (
                "the anchor's own bar month, which is the original fixture and differs from the "
                "matching stratum at the 44 month-boundary anchors"
            ),
            "null_confounded_signal_month_v2": (
                "the anchor's signal-close month, which is the month production matching strata "
                "by; the deterministic return-mean schedule keeps bar-time ownership in both"
            ),
        },
        "thinning_probability": THINNING_PROBABILITY,
        "missing_interval_days": [list(item) for item in MISSING_INTERVAL_DAYS],
        "short_history_instrument": SHORT_HISTORY_INSTRUMENT,
        "short_history_start_day": SHORT_HISTORY_START_DAY,
        "months": "actual UTC calendar months, never 30-day bins",
        "outcome_model": (
            "A horizon outcome is the additive sum of the next H/30m synthetic bar returns and the "
            "per-observation cost is a constant, so these are synthetic outcome and mask records, "
            "not coherent OHLC backtests. A constant cost cancels in the tested difference of "
            "means."
        ),
        "scenarios": [asdict(item) for item in SCENARIOS],
        "implementation": artifacts.module_digests(),
        "generator_digest": None,
    }


def _contract_with_digest() -> dict[str, Any]:
    contract = generator_contract()
    contract["generator_digest"] = contracts.semantic_digest(
        {key: value for key, value in contract.items() if key != "generator_digest"}
    )
    return contract


# The CLI default and the legacy gate both use the pinned protocol tuple, never
# a listing of the growing registry.
DEFAULT_SCENARIOS = LEGACY_PROTOCOL_SCENARIOS


def build_run_plan(
    scenarios: Sequence[str],
    *,
    repetitions: int | None = None,
    replay_repetitions: int = LEGACY_REPLAY_REPETITIONS,
) -> dict[str, Any]:
    """Resolve the requested names into the exact entries this run will execute.

    Driver-generated padded refusal variants are listed here explicitly: they are
    not duplicate caller requests, and a gate that reads the executed results
    must expect them.
    """
    entries: list[dict[str, Any]] = []
    for name in scenarios:
        scenario = SCENARIOS_BY_NAME.get(name)
        if scenario is None:
            raise PatternLabDataError(
                f"unknown calibration scenario {name!r}; declared scenarios are "
                f"{sorted(SCENARIOS_BY_NAME)}."
            )
        entries.append({"name": scenario.name, "padded_days": None, "driver_generated": False})
        if scenario.kind == "refusal":
            entries.append(
                {
                    "name": f"{scenario.name}_padded_365",
                    "padded_days": 365,
                    "driver_generated": True,
                }
            )
    return {
        "protocol": LEGACY_PROTOCOL,
        "requested_scenarios": [str(item) for item in scenarios],
        "entries": entries,
        "repetition_override": None if repetitions is None else int(repetitions),
        "replay_repetitions": int(replay_repetitions),
    }


def run_calibration(
    *,
    output_root: Any,
    scenarios: Sequence[str] = DEFAULT_SCENARIOS,
    repetitions: int | None = None,
    replay_repetitions: int = LEGACY_REPLAY_REPETITIONS,
    progress=None,
) -> dict[str, Any]:
    """Run the planned entries and save the compact evidence artifact."""
    root = Path(output_root).expanduser()
    plan = build_run_plan(
        scenarios, repetitions=repetitions, replay_repetitions=replay_repetitions
    )
    root.mkdir(parents=True, exist_ok=True)
    contract = _contract_with_digest()
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    for entry in plan["entries"]:
        scenario = SCENARIOS_BY_NAME[
            entry["name"][: -len("_padded_365")] if entry["padded_days"] else entry["name"]
        ]
        if progress is not None:
            progress(f"scenario {entry['name']} starting")
        results.append(
            run_scenario(
                scenario,
                repetitions=repetitions,
                padded_days=entry["padded_days"],
                progress=progress,
            )
        )
    replay: dict[str, Any] | None = None
    state_entry_replay: dict[str, Any] | None = None
    if replay_repetitions:
        if progress is not None:
            progress("evidence replay starting")
        replay = run_evidence_replay(
            repetitions=replay_repetitions, root=root / "replay", progress=progress
        )
        state_entry_replay = _state_entry_replay(root / "replay-state-entry")
    document = {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "generated_utc": artifacts.now_utc(),
        "generator_contract": contract,
        "run_plan": plan,
        "results": results,
        "evidence_replay": replay,
        "state_entry_replay": state_entry_replay,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "scope": (
            "This measures and bounds the finite-sample approximation on the declared synthetic "
            "fixtures. It certifies neither unsimulated generators nor actual market error rates."
        ),
    }
    document["acceptance"] = legacy_protocol_state(document)
    path = root / "calibration.json"
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return document


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.pattern_lab.analysis.calibration",
        description=(
            "Run the Pattern Lab M3a calibration scenarios through the production numerical "
            "boundary and save the compact JSON evidence. Acceptance is an empirical screen with a "
            "declared 8% upper-bound envelope, not a certification of exact 5% error."
        ),
    )
    parser.add_argument("--output-root", type=Path, required=True, metavar="DIR")
    parser.add_argument(
        "--scenarios", nargs="*", default=list(DEFAULT_SCENARIOS), metavar="NAME",
        help=(
            "Scenario names to run; the default is the pinned legacy_bootstrap_v1 protocol set. "
            "Any other selection is diagnostic-only."
        ),
    )
    parser.add_argument(
        "--repetitions", type=int, default=None, metavar="N",
        help="Override every scenario's frozen repetition count (for a smoke run only).",
    )
    parser.add_argument(
        "--replay-repetitions", type=int, default=LEGACY_REPLAY_REPETITIONS, metavar="N",
        help=(
            "Repetitions routed through the production checked joins as evidence-shaped frames "
            "(default 25); 0 skips the replay."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    def progress(message: str) -> None:
        print(f"pattern-lab-calibration: {message}", file=sys.stderr, flush=True)

    document = run_calibration(
        output_root=args.output_root,
        scenarios=args.scenarios,
        repetitions=args.repetitions,
        replay_repetitions=args.replay_repetitions,
        progress=progress,
    )
    acceptance = document["acceptance"]
    diagnostic = acceptance["diagnostic"]
    replay = document.get("evidence_replay")
    print(
        json.dumps(
            {
                "status": "completed",
                "output_root": str(Path(args.output_root).expanduser()),
                "protocol": acceptance["protocol"],
                "scenarios": [item["name"] for item in document["results"]],
                "checks_passed": diagnostic["checks_passed"],
                "checks_total": diagnostic["checks_total"],
                "diagnostic_checks_passed": acceptance["diagnostic_checks_passed"],
                "complete_run": acceptance["complete_run"],
                "eligibility_reasons": acceptance["eligibility_reasons"],
                "accepted": acceptance["accepted"],
                "accepted_reasons": acceptance["accepted_reasons"],
                "evidence_replay_agrees": None if replay is None else replay["agrees"],
                "exit_code_note": (
                    "Exit 0 means complete acceptance of this named protocol. Exit 2 is every "
                    "other completed run and deliberately does not distinguish a diagnostic or "
                    "subset run from a statistical failure: read complete_run, "
                    "diagnostic_checks_passed and the reason lists above."
                ),
            },
            indent=2,
        )
    )
    return 0 if acceptance["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
