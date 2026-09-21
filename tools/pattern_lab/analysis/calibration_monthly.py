"""The experimental ``monthly_cluster_jackknife_v1`` candidate and its decision driver.

**Research only.**  Nothing here is reachable from an analysis request, a sealed
artifact or the HTML report, and nothing here is an accepted inference method.
The module exists to decide one bounded question: does a leave-one-signal-month
jackknife of the existing matched estimator meet the declared synthetic
calibration contract that the existing seven-day block bootstrap failed?

The candidate reuses the production estimator's matching, strata, support gates,
weighting and declared family without change.  It consumes
:func:`~tools.pattern_lab.analysis.estimator.accumulate_observations`, the shared
pre-inference stage, so its availability is decided before any resampling and can
never depend on a sampled bootstrap draw.  Only the uncertainty calculation
differs: the estimand, the point estimate and the family are the originals.

Two research fixtures live here beside the driver.  Fixture 101 is an ordinary
outcome-record scenario and belongs to the calibration registry; fixture **102**,
``null_causal_ohlc_v1``, is a coherent synthetic candle null that runs the real
closed-candle condition, event emission, next-open fixed-horizon model, actual
price-ratio fee expansion and checked source alignment, so it is built here
rather than in ``generate_records``.

A PASS of the contract in :mod:`tools.pattern_lab.analysis.calibration` scored
with this method is a pass of that synthetic screen only.  It is not adoption, it
is not market error control, and it supplies no calibration evidence for month
counts other than the ones actually observed.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, asdict
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import time
from typing import Any, Mapping, Sequence

# The experiment is a single RAM-first process. Numerical-library threads are
# pinned to one **before** the first array import, because a BLAS pool sizes
# itself at load time. Pinning here only binds when this module is imported
# before NumPy; a caller that imported NumPy first must export these itself, and
# the saved environment document records what was actually in effect.
THREAD_VARIABLES = (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)
_INHERITED_THREAD_SETTINGS = {name: os.environ.get(name) for name in THREAD_VARIABLES}
for _name in THREAD_VARIABLES:
    os.environ.setdefault(_name, "1")

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from ..study import contracts
from . import artifacts
from . import calibration as cal
from .calibration_memory import host_memory, process_memory
from . import calibration_evidence as admission
from .estimator import (
    ALPHA,
    REASON_DEGENERATE,
    REASON_ACTIVE_DAYS,
    REASON_BLOCKS,
    REASON_COVERAGE,
    REASON_HORIZON,
    REASON_NO_SUPPORT,
    REASON_SPAN,
    AccumulatedEstimates,
    accumulate_observations,
    _holm,
)
from .family import FamilyMember

CANDIDATE_METHOD_ID = "monthly_cluster_jackknife_v1"
CANDIDATE_SCHEMA_VERSION = 1
RESEARCH_MODULES = (
    "tools.pattern_lab.analysis.calibration_monthly",
    "tools.pattern_lab.analysis.calibration",
    "tools.pattern_lab.analysis.calibration_memory",
    "tools.pattern_lab.analysis.calibration_evidence",
    "tools.pattern_lab.study.job",
)


def research_digests() -> dict[str, str]:
    """Explicit research dependencies, separate from production artifact attribution."""
    return {name: hashlib.sha256(Path(importlib.util.find_spec(name).origin).read_bytes()).hexdigest()
            for name in RESEARCH_MODULES}

# Mathematical validity requirements of the candidate, beyond the unchanged
# production support gates.  They are not a new support threshold.
MIN_INFORMATIVE_MONTHS = 2
REASON_MONTHS = "insufficient_informative_months"
REASON_DELETION = "exhausted_deletion_denominator"

CANDIDATE_REASON_ORDER = (
    REASON_NO_SUPPORT,
    REASON_SPAN,
    REASON_ACTIVE_DAYS,
    REASON_BLOCKS,
    REASON_COVERAGE,
    REASON_HORIZON,
    REASON_MONTHS,
    REASON_DELETION,
    REASON_DEGENERATE,
)

DEGENERACY_MULTIPLIER = 128
CONFIDENCE_LEVEL = 0.95


# --------------------------------------------------------------------------
# the pure numerical candidate
# --------------------------------------------------------------------------

def _require_finite(values: np.ndarray, where: str) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    if not np.all(np.isfinite(array)):
        raise PatternLabDataError(
            f"{where}: the retained sufficient statistics contain a non-finite value. "
            "Non-finite arithmetic is a correctness failure, not a refusal to hide in "
            "availability."
        )
    return array


def monthly_jackknife(
    *,
    month_index: Sequence[int],
    target_count: Sequence[float],
    control_count: Sequence[float],
    target_net_sum: Sequence[float],
    control_net_sum: Sequence[float],
) -> dict[str, Any]:
    """The leave-one-signal-month jackknife of one member's matched contrast.

    The inputs are one member's **retained** strata, exactly as the production
    support gates left them: a signal-month ordinal, target and control counts and
    their net-return sums.  Strata are aggregated to whole signal months, each
    month is deleted in turn, and the deletion spread gives the covariance of the
    three-vector ``(signal, matched control, lift)``.

    The point estimate is the ordinary full-sample one, never the deletion
    average.  ``V_J`` is positive semidefinite and **singular by construction**,
    because lift is signal minus matched control; only its diagonal is used, it is
    never inverted, factored or ridged, and its off-diagonal entries do not add an
    independent dimension.  The ``t`` reference with ``G-1`` degrees of freedom is
    an explicit approximation: months need not be independent in real data, and
    the monthly cancellation removes the need to estimate a fitted within-stratum
    imbalance term at a sub-stratum block scale — it does not remove control-mean
    uncertainty, which stays in the variation of the monthly contrast numerators.
    """
    from scipy.stats import t as student_t

    months = np.asarray(month_index, dtype=np.int64)
    e = _require_finite(target_count, "target_count")
    c = _require_finite(control_count, "control_count")
    a = _require_finite(target_net_sum, "target_net_sum")
    b = _require_finite(control_net_sum, "control_net_sum")
    if not (months.size == e.size == c.size == a.size == b.size):
        raise PatternLabDataError(
            "monthly_jackknife: the retained stratum arrays have different lengths."
        )
    if months.size and (np.any(e <= 0.0) or np.any(c <= 0.0)):
        raise PatternLabDataError(
            "monthly_jackknife: a retained stratum has a non-positive target or control count; "
            "the production support gate admits neither."
        )

    labels, inverse = np.unique(months, return_inverse=True)
    groups = int(labels.size)
    if groups == 0:
        return _unavailable_candidate([REASON_NO_SUPPORT])

    ratio = e / c
    columns = np.empty((groups, 3), dtype=np.float64)
    columns[:, 0] = np.bincount(inverse, weights=a, minlength=groups)
    columns[:, 1] = np.bincount(inverse, weights=ratio * b, minlength=groups)
    # The lift column is exactly the first minus the second, which is what makes
    # the resulting covariance singular.
    columns[:, 2] = columns[:, 0] - columns[:, 1]
    monthly_targets = np.bincount(inverse, weights=e, minlength=groups)
    total = float(monthly_targets.sum())

    balance = _balance(labels, monthly_targets, total)
    if groups < MIN_INFORMATIVE_MONTHS:
        return _unavailable_candidate([REASON_MONTHS], groups=groups, balance=balance)
    remaining = total - monthly_targets
    if float(total) <= 0.0 or np.any(remaining <= 0.0):
        return _unavailable_candidate([REASON_DELETION], groups=groups, balance=balance)

    aggregate = columns.sum(axis=0)
    theta = aggregate / total
    deletion = (aggregate[np.newaxis, :] - columns) / remaining[:, np.newaxis]
    centred = deletion - deletion.mean(axis=0)
    covariance = (groups - 1) / groups * (centred.T @ centred)
    variance = np.diag(covariance)
    if not np.all(np.isfinite(theta)) or not np.all(np.isfinite(variance)):
        raise PatternLabDataError(
            "monthly_jackknife: finite retained statistics produced a non-finite estimate or "
            "variance; this is an implementation failure, not a refusal."
        )
    standard_error = np.sqrt(np.maximum(variance, 0.0))

    # Pre-cancellation component mass, amplified by the largest deletion
    # denominator, so a contrast that cancels against genuinely large components
    # is recognised as degenerate rather than measured as tiny.
    mass = float((np.abs(a) + np.abs(ratio * b)).sum() / total)
    amplification = float(np.max(total / remaining))
    scale = max(abs(float(theta[2])), mass * amplification)
    threshold = DEGENERACY_MULTIPLIER * float(np.finfo(np.float64).eps) * scale
    degeneracy = {
        "component_mass": mass,
        "deletion_amplification": amplification,
        "scale_lift": scale,
        "threshold": threshold,
        "standard_error_lift": float(standard_error[2]),
        "multiplier": DEGENERACY_MULTIPLIER,
    }
    if float(standard_error[2]) <= threshold:
        return _unavailable_candidate(
            [REASON_DEGENERATE], groups=groups, balance=balance, degeneracy=degeneracy,
            theta=theta, retained_targets=total,
        )

    degrees = groups - 1
    critical = float(student_t.ppf(0.5 + CONFIDENCE_LEVEL / 2, degrees))
    statistic = abs(float(theta[2])) / float(standard_error[2])
    p_lift = float(min(1.0, 2.0 * float(student_t.sf(statistic, degrees))))
    names = ("signal", "control", "lift")
    return {
        "method": CANDIDATE_METHOD_ID,
        "available": True,
        "reasons": [],
        "theta": {name: float(theta[index]) for index, name in enumerate(names)},
        "standard_error": {name: float(standard_error[index]) for index, name in enumerate(names)},
        "intervals": {
            name: {
                "lower": float(theta[index] - critical * standard_error[index]),
                "upper": float(theta[index] + critical * standard_error[index]),
            }
            for index, name in enumerate(names)
        },
        "p_lift": p_lift,
        "t_statistic": statistic,
        "t_critical": critical,
        "informative_months": groups,
        "degrees_of_freedom": degrees,
        "retained_target_observations": total,
        "balance": balance,
        "degeneracy": degeneracy,
        "covariance_note": (
            "V_J is singular by construction: its lift column is the signal column minus the "
            "matched-control column. Only its diagonal is used; it is never inverted, factored "
            "or ridged, and its off-diagonal entries are meaningful without adding an "
            "independent dimension."
        ),
    }


def _balance(
    labels: np.ndarray, monthly_targets: np.ndarray, total: float
) -> dict[str, Any]:
    """Monthly balance diagnostics; the inverse share sum is not a substitute df."""
    if total <= 0.0:
        return {
            "monthly_target_counts": [int(item) for item in monthly_targets],
            "month_ordinals": [int(item) for item in labels],
            "max_monthly_share": None,
            "inverse_sum_squared_shares": None,
            "note": "No retained target observation: the shares are undefined.",
        }
    shares = monthly_targets / total
    return {
        "monthly_target_counts": [int(item) for item in monthly_targets],
        "month_ordinals": [int(item) for item in labels],
        "max_monthly_share": float(shares.max()),
        "inverse_sum_squared_shares": float(1.0 / float(np.square(shares).sum())),
        "note": (
            "The inverse sum of squared monthly shares is a concentration diagnostic, not an "
            "effective degrees-of-freedom estimator and not a substitute for G-1."
        ),
    }


def _unavailable_candidate(
    reasons: Sequence[str],
    *,
    groups: int | None = None,
    balance: Mapping[str, Any] | None = None,
    degeneracy: Mapping[str, Any] | None = None,
    theta: np.ndarray | None = None,
    retained_targets: float | None = None,
) -> dict[str, Any]:
    """An explicit refusal: nulls and reasons, never an epsilon SE or ``p=0``."""
    names = ("signal", "control", "lift")
    return {
        "method": CANDIDATE_METHOD_ID,
        "available": False,
        "reasons": [item for item in CANDIDATE_REASON_ORDER if item in set(reasons)],
        "theta": (
            {name: float(theta[index]) for index, name in enumerate(names)}
            if theta is not None
            else {name: None for name in names}
        ),
        "standard_error": {name: None for name in names},
        "intervals": {name: None for name in names},
        "p_lift": None,
        "t_statistic": None,
        "t_critical": None,
        "informative_months": groups,
        "degrees_of_freedom": None if groups is None else max(groups - 1, 0),
        "retained_target_observations": retained_targets,
        "balance": dict(balance) if balance is not None else None,
        "degeneracy": dict(degeneracy) if degeneracy is not None else None,
        "covariance_note": None,
    }


# --------------------------------------------------------------------------
# the candidate over one accumulated family
# --------------------------------------------------------------------------

def evaluate_candidate(accumulated: AccumulatedEstimates) -> dict[str, Any]:
    """Apply the candidate to every member of one accumulated family.

    The unchanged full-sample support, span, active-day, block, coverage and
    horizon gates have already been applied once, by the production accumulation.
    A member they refused stays refused; the candidate adds only its own
    mathematical validity rules and never reapplies a year-length admission gate
    to a deleted-month sample.
    """
    members: list[dict[str, Any]] = []
    for index, result in enumerate(accumulated.results):
        inherited = list(result["unavailable_reasons"])
        strata = accumulated.strata.get(index)
        if strata is None:
            candidate = _unavailable_candidate(inherited or [REASON_NO_SUPPORT])
        else:
            candidate = monthly_jackknife(
                month_index=strata["month_index"],
                target_count=strata["target_count"],
                control_count=strata["control_count"],
                target_net_sum=strata["target_net_sum"],
                control_net_sum=strata["control_net_sum"],
            )
        point = {name: result[name] for name in ("signal", "control", "lift")}
        identity = None
        if candidate["theta"]["lift"] is not None and point["lift"] is not None:
            identity = max(
                abs(float(candidate["theta"][name]) - float(point[name]))
                for name in ("signal", "control", "lift")
            )
        members.append(
            {
                "member_id": result["member_id"],
                "primary": bool(result["primary"]),
                "horizon_minutes": int(result["horizon_minutes"]),
                "direction": result["direction"],
                # The reported point estimate is the original full-sample one.
                "signal": point["signal"],
                "control": point["control"],
                "lift": point["lift"],
                "point_identity_max_abs_difference": identity,
                "inherited_reasons": inherited,
                "candidate": candidate,
                "inference_available": bool(candidate["available"]),
                "unavailable_reasons": candidate["reasons"],
                "p_raw": candidate["p_lift"],
            }
        )

    internal = np.array(
        [1.0 if item["p_raw"] is None else float(item["p_raw"]) for item in members],
        dtype=np.float64,
    )
    adjusted = _holm(internal, [item["member_id"] for item in members])
    for index, item in enumerate(members):
        item["p_holm_internal"] = float(adjusted[index])
        if item["inference_available"]:
            item["p_holm"] = float(adjusted[index])
            item["nominal_reject_holm"] = bool(adjusted[index] <= ALPHA)
            item["nominal_reject_raw"] = bool(item["p_raw"] <= ALPHA)
        else:
            item["p_holm"] = None
            item["nominal_reject_holm"] = None
            item["nominal_reject_raw"] = None
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "method": CANDIDATE_METHOD_ID,
        "scope": "experimental_research_candidate",
        "alpha": ALPHA,
        "confidence_level": CONFIDENCE_LEVEL,
        "family_size": len(members),
        "members": members,
        "note": (
            "Holm uses the candidate's own p-values over the entire unchanged declared family, "
            "with p=1 kept internally for unavailable members. No original bootstrap p-value "
            "enters this family-wise result, and no seed or resample count governs this method."
        ),
    }


# --------------------------------------------------------------------------
# fixture 102: a coherent causal candle null through the production pipeline
# --------------------------------------------------------------------------

CANDLE_FIXTURE_ID = 102
CANDLE_FIXTURE_NAME = "null_causal_ohlc_v1"
CANDLE_STUDY_START_DAY = "2025-07-01"
CANDLE_STUDY_DAYS = 365
CANDLE_WARMUP_DAYS = 7
CANDLE_COMMON_LOADING = 0.8
CANDLE_INDIVIDUAL_LOADING = 0.6
CANDLE_RETURN_SCALE = 0.001
CANDLE_INITIAL_OPEN = 100.0
CANDLE_HIGH_FACTOR = 1.0002
CANDLE_LOW_FACTOR = 0.9998
CANDLE_VOLUME_LOG_SCALE = 0.25
# 0.05 **percent** per side, which the production expansion turns into the
# fraction f = 0.0005 through ``commission_pct_per_side / 100``.
CANDLE_COMMISSION_PCT_PER_SIDE = 0.05
CANDLE_MODEL_INSTANCE = "synthetic"
CANDLE_VARIANT = "signal"
CANDLE_CONDITION_ID = "c_two_green_volume"
CANDLE_COMPARISON_ID = "baseline__signal"
CANDLE_DATA_STREAM = 0


def candle_generator_contract() -> dict[str, Any]:
    """Every frozen setting and the exact draw order of fixture 102."""
    from ..study import builtins as study_builtins

    return {
        "fixture_id": CANDLE_FIXTURE_ID,
        "fixture_name": CANDLE_FIXTURE_NAME,
        "timeframe_minutes": cal.TIMEFRAME_MINUTES,
        "instruments": list(cal.INSTRUMENTS),
        "study_start_utc_day": CANDLE_STUDY_START_DAY,
        "study_days": CANDLE_STUDY_DAYS,
        "warmup_days": CANDLE_WARMUP_DAYS,
        "return_law": (
            "r_i = 0.001 * (0.8 * C + 0.6 * E_i) with a shared Rademacher sign C and independent "
            "instrument Rademacher signs E_i, independent over time."
        ),
        "common_loading": CANDLE_COMMON_LOADING,
        "individual_loading": CANDLE_INDIVIDUAL_LOADING,
        "return_scale": CANDLE_RETURN_SCALE,
        "price_law": (
            "open[0]=100; close=open*(1+r); open[k+1]=close[k]; high=max(open,close)*1.0002; "
            "low=min(open,close)*0.9998."
        ),
        "initial_open": CANDLE_INITIAL_OPEN,
        "high_factor": CANDLE_HIGH_FACTOR,
        "low_factor": CANDLE_LOW_FACTOR,
        "volume_law": "quote volume = exp(0.25 * Z_i), Z_i standard normal.",
        "volume_log_scale": CANDLE_VOLUME_LOG_SCALE,
        "seed_rule": (
            "numpy.random.PCG64(SeedSequence([20260920, 102, repetition_id, 0]))."
        ),
        "draw_order": (
            "C for every chronological warmup+study bar first, as "
            "2*rng.integers(0,2,size=bars,dtype=int64)-1; then, in the frozen INSTRUMENTS order, "
            "each instrument's E_i signs by the same rule followed by its Z_i volume normals from "
            "rng.standard_normal(bars). Draws are never interleaved by bar and never redrawn for "
            "another horizon; warmup and study are one coherent path."
        ),
        "condition": study_builtins.TWO_GREEN_HYPOTHESIS_ID,
        "occurrence": "every_qualifying_bar",
        "comparison": "nonsignal baseline against known-valid false-condition anchors",
        "commission_pct_per_side": CANDLE_COMMISSION_PCT_PER_SIDE,
        "commission_fraction_per_side": CANDLE_COMMISSION_PCT_PER_SIDE / 100.0,
        "horizons_minutes": list(cal.HORIZON_MINUTES),
        "directions": list(cal.DIRECTIONS),
        "primary": f"{cal.PRIMARY_DIRECTION} {cal.PRIMARY_HORIZON_MINUTES}m",
        "common_signal_disclosure": (
            "Because |0.8| > |0.6| and both draws are +/-1, sign(0.8C + 0.6E_i) = sign(C): a bar "
            "is green exactly when C = +1, so the two-green component of the condition is "
            "identical on all four instruments by construction. This fixture therefore "
            "deliberately tests a fully common price signal with strong cross-sectional event "
            "clustering; the independent volume filter still gives distinct event masks and the "
            "return magnitudes still differ. Common signs do not invalidate the null."
        ),
        "null_argument": (
            "Entry is open[i+1] = close[i] and exit is close[i+k], so the gross price ratio has "
            "conditional mean one given the past and the expected fee is 2f. The condition is "
            "measurable with respect to the past, so the target and control populations have the "
            "same zero expected net difference. This is a coherent population-null construction, "
            "not a claim that a finite random-denominator estimate, or an expectation conditional "
            "on the whole endogenous mask, is exactly zero. The exogenous-mask oracle of the "
            "record fixtures does not apply here."
        ),
        "bounded_law_disclosure": (
            "Rademacher innovations are bounded, which guarantees strictly positive price paths. "
            "Gaussian return innovations would be unbounded and would lose that guarantee."
        ),
    }


def candle_family() -> tuple[FamilyMember, ...]:
    """Fixture 102's declared family: the baseline comparison at the pinned fee."""
    members: list[FamilyMember] = []
    for horizon in cal.HORIZON_MINUTES:
        for direction in cal.DIRECTIONS:
            case_id = f"tf{cal.TIMEFRAME_MINUTES}m.h{horizon}m.{direction}"
            members.append(
                FamilyMember(
                    member_id=(
                        f"{CANDLE_COMPARISON_ID}|{CANDLE_MODEL_INSTANCE}"
                        f"|tf{cal.TIMEFRAME_MINUTES}m|{case_id}"
                    ),
                    comparison_id=CANDLE_COMPARISON_ID,
                    kind="nonsignal_baseline",
                    target_variant=CANDLE_VARIANT,
                    control_variant=None,
                    model_instance_id=CANDLE_MODEL_INSTANCE,
                    timeframe_minutes=cal.TIMEFRAME_MINUTES,
                    case_id=case_id,
                    direction=direction,
                    horizon_minutes=horizon,
                    commission_pct_per_side=CANDLE_COMMISSION_PCT_PER_SIDE,
                    primary=(
                        horizon == cal.PRIMARY_HORIZON_MINUTES
                        and direction == cal.PRIMARY_DIRECTION
                    ),
                    label=f"{CANDLE_COMPARISON_ID} {direction} {horizon}m",
                )
            )
    return tuple(members)


def candle_primary_member_id() -> str:
    return next(item.member_id for item in candle_family() if item.primary)


def _candle_bounds() -> tuple[int, int, int, int]:
    """Grid start, study start, study end and bar count of fixture 102."""
    study_start = cal._epoch_ms(CANDLE_STUDY_START_DAY)
    grid_start = study_start - CANDLE_WARMUP_DAYS * cal.DAY_MS
    study_end = study_start + CANDLE_STUDY_DAYS * cal.DAY_MS
    bars = (CANDLE_WARMUP_DAYS + CANDLE_STUDY_DAYS) * cal.BARS_PER_DAY
    return grid_start, study_start, study_end, bars


def candle_bars(repetition: int) -> dict[str, np.ndarray]:
    """One repetition's coherent OHLCV path per instrument, warmup included.

    The draw order is frozen: the shared sign for every chronological bar first,
    then each instrument's own signs followed by its own volume normals.
    """
    grid_start, _study_start, _study_end, bars = _candle_bounds()
    sequence = cal.seed_sequence(CANDLE_FIXTURE_ID, repetition, CANDLE_DATA_STREAM)
    rng = np.random.Generator(np.random.PCG64(sequence))
    common = 2 * rng.integers(0, 2, size=bars, dtype=np.int64) - 1
    step = cal.TIMEFRAME_MINUTES * 60_000
    timestamps = grid_start + np.arange(bars, dtype=np.int64) * step
    series: dict[str, np.ndarray] = {"_timestamps_ms": timestamps}
    for instrument in cal.INSTRUMENTS:
        individual = 2 * rng.integers(0, 2, size=bars, dtype=np.int64) - 1
        volume_normal = rng.standard_normal(bars)
        returns = CANDLE_RETURN_SCALE * (
            CANDLE_COMMON_LOADING * common + CANDLE_INDIVIDUAL_LOADING * individual
        )
        close = CANDLE_INITIAL_OPEN * np.cumprod(1.0 + returns)
        open_price = np.empty(bars, dtype=np.float64)
        open_price[0] = CANDLE_INITIAL_OPEN
        open_price[1:] = close[:-1]
        high = np.maximum(open_price, close) * CANDLE_HIGH_FACTOR
        low = np.minimum(open_price, close) * CANDLE_LOW_FACTOR
        volume = np.exp(CANDLE_VOLUME_LOG_SCALE * volume_normal)
        series[instrument] = np.column_stack([open_price, high, low, close, volume])
    return series


def candle_model_instance() -> dict[str, Any]:
    """The built-in fixed-horizon instance fixture 102 declares, at the pinned fee."""
    from ..study import builtins as study_builtins

    settings = study_builtins.validate_fixed_horizon_settings(
        {
            "directions": list(cal.DIRECTIONS),
            "commission_pct_per_side": CANDLE_COMMISSION_PCT_PER_SIDE,
            "by_timeframe": {
                str(cal.TIMEFRAME_MINUTES): {
                    "horizons_minutes": list(cal.HORIZON_MINUTES),
                    "primary_horizon_minutes": cal.PRIMARY_HORIZON_MINUTES,
                }
            },
        },
        [cal.TIMEFRAME_MINUTES],
    )
    cases = [
        case.as_json()
        for case in study_builtins.resolve_fixed_horizon_cases(settings, cal.TIMEFRAME_MINUTES)
    ]
    return {
        "model_instance_id": CANDLE_MODEL_INSTANCE,
        "model_id": study_builtins.FIXED_HORIZON_MODEL_ID,
        "model_version": "1",
        "evidence_kind": "fixed_horizon_path_v1",
        "settings": settings,
        "cases": {str(cal.TIMEFRAME_MINUTES): cases},
    }


def candle_variants() -> tuple[dict[str, Any], ...]:
    from ..study import builtins as study_builtins

    return (
        {
            "variant_id": CANDLE_VARIANT,
            "hypothesis_id": study_builtins.TWO_GREEN_HYPOTHESIS_ID,
            "parameters": {},
            "occurrence": "every_qualifying_bar",
            "condition_id": CANDLE_CONDITION_ID,
        },
    )


def candle_evidence(repetition: int) -> dict[str, dict[str, pd.DataFrame]]:
    """Run the real instrument job over one repetition's candles.

    The built-in two-green/rising-volume condition, its event emission and the
    next-open fixed-horizon model all execute here exactly as a published study
    would execute them; only the pack read and the disk publication are replaced
    by in-memory arrays.
    """
    from ..study import contracts as study_contracts
    from ..study import job as study_job

    study_contracts.registered("hypothesis")  # ensure the registry module is live
    from ..study import builtins as study_builtins

    study_builtins.register_builtins()

    grid_start, study_start, study_end, bars = _candle_bounds()
    series = candle_bars(repetition)
    instance = candle_model_instance()
    variants = candle_variants()
    tables: dict[str, dict[str, pd.DataFrame]] = {}
    for instrument in cal.INSTRUMENTS:
        prepared = study_job.TimeframeInput(
            timeframe_minutes=cal.TIMEFRAME_MINUTES,
            timestamps_ms=series["_timestamps_ms"],
            values=series[instrument],
            research_start_index=CANDLE_WARMUP_DAYS * cal.BARS_PER_DAY,
            input_fingerprint=f"{CANDLE_FIXTURE_NAME}:{repetition}:{instrument}",
            base_row_count=bars,
            base_gap_count=0,
            omitted_group_count=0,
            segment_count=1,
        )
        payload = study_job.InstrumentJobInput(
            instrument_id=instrument,
            symbol=instrument,
            venue="SYNTHETIC",
            contract="linear_perpetual",
            quote_currency="USDT",
            roles=("research",),
            study_start_ms=study_start,
            study_end_ms=study_end,
            warmup_start_ms=grid_start,
            timeframes=(prepared,),
            variants=variants,
            models=(instance,),
        )
        tables[instrument] = dict(study_job.run_instrument_job(payload).tables)
    return tables


@dataclass(frozen=True)
class _InMemoryStudyResults:
    """A ``StudyResults``-shaped provider whose verified tables are already in RAM.

    Only the table read is replaced.  ``InstrumentReader``, the shared case
    expansion, the checked condition/emission alignment, the all-anchor case view
    and the membership mapping are the production ones, unchanged.
    """

    run_root: Path
    request: Mapping[str, Any]
    protocol: Mapping[str, Any]
    family: Mapping[str, Any]
    source: Mapping[str, Any]
    provenance: Mapping[str, Any]
    status: Mapping[str, Any]
    completion: Mapping[str, Any] | None
    complete: bool
    jobs: Mapping[str, Mapping[str, Any]]
    job_states: Mapping[str, str]
    counts: Mapping[str, int]
    tables_by_instrument: Mapping[str, Mapping[str, pd.DataFrame]]

    @property
    def completed_instruments(self) -> list[str]:
        return sorted(self.jobs)

    def table(self, instrument_id: str, name: str) -> pd.DataFrame:
        if instrument_id not in self.jobs:
            raise PatternLabDataError(f"{instrument_id}: no completed job in this replay.")
        frame = self.tables_by_instrument[instrument_id].get(name)
        if frame is None:
            raise PatternLabDataError(f"{instrument_id}: this replay saved no {name!r} table.")
        return frame

    def model_instance(self, model_instance_id: str) -> dict[str, Any]:
        from ..study import results as study_results

        return study_results.StudyResults.model_instance(self, model_instance_id)

    def cases(self, model_instance_id: str, timeframe_minutes: int):
        from ..study import results as study_results

        return study_results.StudyResults.cases(self, model_instance_id, timeframe_minutes)

    def case(self, model_instance_id: str, timeframe_minutes: int, case_id: str):
        from ..study import results as study_results

        return study_results.StudyResults.case(
            self, model_instance_id, timeframe_minutes, case_id
        )

    def instrument_reader(self, instrument_id: str):
        from ..study import results as study_results

        return study_results.InstrumentReader(self, instrument_id)


def candle_source(tables: Mapping[str, Mapping[str, pd.DataFrame]]):
    """Wrap in-memory evidence as an admitted source over the production reader."""
    from .source import AdmittedSource

    _grid_start, study_start, study_end, _bars = _candle_bounds()
    instance = candle_model_instance()
    family = {
        "instruments": [{"instrument_id": item} for item in cal.INSTRUMENTS],
        "timeframes_minutes": [cal.TIMEFRAME_MINUTES],
        "variants": [dict(item) for item in candle_variants()],
        "models": [instance],
    }
    results = _InMemoryStudyResults(
        run_root=Path("<in-memory>"),
        request={"study_name": CANDLE_FIXTURE_NAME, "study": {}},
        protocol={},
        family=family,
        source={"evidence_view_version": 1},
        provenance={"identities": {}},
        status={},
        completion=None,
        complete=True,
        jobs={item: {} for item in cal.INSTRUMENTS},
        job_states={item: "completed" for item in cal.INSTRUMENTS},
        counts={},
        tables_by_instrument=tables,
    )
    return AdmittedSource(
        run_root=Path("<in-memory>"),
        results=results,
        variants=tuple(family["variants"]),
        instances={CANDLE_MODEL_INSTANCE: instance},
        timeframes=(cal.TIMEFRAME_MINUTES,),
        instruments=tuple(cal.INSTRUMENTS),
        study_start_ms=study_start,
        study_end_ms=study_end,
    )


def candle_accumulated(repetition: int) -> AccumulatedEstimates:
    """One fixture-102 repetition, from candles to accumulated estimates."""
    from .source import RecordSource

    _grid_start, study_start, study_end, _bars = _candle_bounds()
    members = candle_family()
    source = candle_source(candle_evidence(repetition))
    return accumulate_observations(
        RecordSource(source, members).frames(),
        family=members,
        instruments=cal.INSTRUMENTS,
        study_start_ms=study_start,
        study_end_ms=study_end,
    )


# --------------------------------------------------------------------------
# the frozen decision matrix
# --------------------------------------------------------------------------

FIRST_REPETITION_ID = 20000
LAST_REPETITION_ID = 21999
PILOT_ATTEMPTS = 5
MAIN_ATTEMPTS = 2000
MAX_MAIN_ATTEMPTS = 16000

ERROR_ENVELOPE = cal.ERROR_ENVELOPE
MIN_PRIMARY_AVAILABILITY = cal.MIN_PRIMARY_AVAILABILITY
# The largest error count and the largest unavailable count that can still pass
# at the maximum denominator; both are confirmed against the production exact
# binomial function before the matrix runs.
IMPOSSIBLE_ERRORS = 140
IMPOSSIBLE_UNAVAILABLE = 101

WALL_CLOCK_BUDGET_SECONDS = 3 * 60 * 60
RSS_CEILING_BYTES = 1024 * 1024 * 1024
# The host must have at least this much RAM available before the matrix starts,
# and the effective ceiling is tightened to leave this reserve free. Neither
# loosens the declared 1 GiB ceiling; on a small host they stop the run honestly
# instead of waiting for an OOM kill.
HEADROOM_REQUIRED_BYTES = 512 * 1024 * 1024
HEADROOM_RESERVE_BYTES = 192 * 1024 * 1024
PROGRESS_BATCH = 100


@dataclass(frozen=True)
class CandidateFixture:
    """One fixture of the frozen candidate matrix."""

    order: int
    fixture_id: int
    name: str
    kind: str
    attempts: int
    source: str
    interpretation: str
    scenario_name: str | None = None
    padded_days: int | None = None
    required: bool = False

    @property
    def label(self) -> str:
        return f"{self.fixture_id:03d}_{self.name}"

    @property
    def truth(self) -> float:
        if self.scenario_name is None:
            return 0.0
        return float(cal.SCENARIOS_BY_NAME[self.scenario_name].planted_effect)


def _main_matrix() -> tuple[CandidateFixture, ...]:
    """The eight required fixtures, hardest first so a failure surfaces early."""
    declared = (
        (2, "null_dependent_t5", "Dependent t5 original admitted null"),
        (5, "null_admission_boundary_336", "Original 336-day admission boundary"),
        (1, "null_independent", "Original independent null"),
        (4, "null_inclusive_parent", "Original inclusive-parent null"),
        (
            3,
            "null_conditional_confounded",
            "Legacy confounded zero-reference compatibility; its documented tiny population-null "
            "mismatch qualifies the number and does not waive the check",
        ),
        (
            12,
            "null_dependent_gaussian_companion",
            "Gaussian dependent companion, required for this candidate",
        ),
        (
            101,
            "null_confounded_signal_month_v2",
            "Corrected signal-month confounded null",
        ),
    )
    fixtures = [
        CandidateFixture(
            order=index + 1,
            fixture_id=fixture_id,
            name=name,
            kind="required_null",
            attempts=MAIN_ATTEMPTS,
            source="records",
            interpretation=interpretation,
            scenario_name=name,
            required=True,
        )
        for index, (fixture_id, name, interpretation) in enumerate(declared)
    ]
    fixtures.append(
        CandidateFixture(
            order=len(declared) + 1,
            fixture_id=CANDLE_FIXTURE_ID,
            name=CANDLE_FIXTURE_NAME,
            kind="required_null",
            attempts=MAIN_ATTEMPTS,
            source="candles",
            interpretation="Coherent causal-candle pipeline null",
            required=True,
        )
    )
    return tuple(fixtures)


def _supplementary_matrix() -> tuple[CandidateFixture, ...]:
    """Refusal, limitation and planted disclosures, at their declared counts."""
    entries: list[CandidateFixture] = []
    order = len(_main_matrix())
    # Plan 1 membership/counts are independent of the growing scenario registry.
    for fixture_id, name, kind, attempts in (
        (10, "short_population_84_days", "refusal", 200),
        (11, "short_population_180_days", "refusal", 200),
        (20, "stress_long_dependence_ar09_p070", "stress", 1000),
        (21, "stress_long_dependence_ar09_p097", "stress", 1000),
        (30, "planted_positive_strong", "planted", 200),
        (31, "planted_negative_strong", "planted", 200),
        (32, "planted_modest", "planted_descriptive", 200),
    ):
        scenario = cal.SCENARIOS_BY_NAME[name]
        order += 1
        entries.append(
            CandidateFixture(
                order=order,
                fixture_id=fixture_id,
                name=scenario.name,
                kind=kind,
                attempts=attempts,
                source="records",
                interpretation=scenario.note,
                scenario_name=scenario.name,
            )
        )
        if kind == "refusal":
            order += 1
            entries.append(
                CandidateFixture(
                    order=order,
                    fixture_id=fixture_id,
                    name=f"{scenario.name}_padded_365",
                    kind=kind,
                    attempts=attempts,
                    source="records",
                    interpretation=(
                        "The same records embedded in a padded 365-day grid must stay unavailable."
                    ),
                    scenario_name=scenario.name,
                    padded_days=365,
                )
            )
    return tuple(entries)


MAIN_MATRIX = _main_matrix()
SUPPLEMENTARY_MATRIX = _supplementary_matrix()


# --------------------------------------------------------------------------
# one repetition
# --------------------------------------------------------------------------

def _record_accumulated(fixture: CandidateFixture, repetition: int) -> tuple[AccumulatedEstimates, str]:
    scenario = cal.SCENARIOS_BY_NAME[fixture.scenario_name]
    records = cal.generate_records(scenario, repetition)
    members = cal.scenario_family(scenario)
    study_start = scenario.study_start_ms
    study_end = (
        study_start + fixture.padded_days * cal.DAY_MS
        if fixture.padded_days
        else scenario.study_end_ms
    )
    accumulated = accumulate_observations(
        cal.record_frames(records, members),
        family=members,
        instruments=cal.INSTRUMENTS,
        study_start_ms=study_start,
        study_end_ms=study_end,
    )
    return accumulated, cal.primary_member_id(scenario)


def run_candidate_repetition(fixture: CandidateFixture, repetition: int) -> dict[str, Any]:
    """Run one repetition of one fixture and record its compact candidate result."""
    if fixture.source == "candles":
        accumulated = candle_accumulated(repetition)
        primary_id = candle_primary_member_id()
    else:
        accumulated, primary_id = _record_accumulated(fixture, repetition)
    outcome = evaluate_candidate(accumulated)
    members = outcome["members"]
    primary = next(item for item in members if item["member_id"] == primary_id)
    candidate = primary["candidate"]
    interval = candidate["intervals"]["lift"]
    truth = fixture.truth
    balance = candidate["balance"] or {}
    return {
        "id": int(repetition),
        "primary_member_id": primary_id,
        "available": bool(primary["inference_available"]),
        "reasons": list(primary["unavailable_reasons"]),
        "inherited_reasons": list(primary["inherited_reasons"]),
        "lift": primary["lift"],
        "standard_error": candidate["standard_error"]["lift"],
        "interval_lower": None if interval is None else interval["lower"],
        "interval_upper": None if interval is None else interval["upper"],
        "interval_width": None if interval is None else interval["upper"] - interval["lower"],
        "p_raw": primary["p_raw"],
        "p_holm": primary["p_holm"],
        "reject_raw": primary["nominal_reject_raw"],
        "reject_holm": primary["nominal_reject_holm"],
        "noncoverage": (
            None if interval is None else not (interval["lower"] <= truth <= interval["upper"])
        ),
        "informative_months": candidate["informative_months"],
        "degrees_of_freedom": candidate["degrees_of_freedom"],
        "monthly_target_counts": balance.get("monthly_target_counts"),
        "max_monthly_share": balance.get("max_monthly_share"),
        "inverse_sum_squared_shares": balance.get("inverse_sum_squared_shares"),
        "point_identity_max_abs_difference": primary["point_identity_max_abs_difference"],
        "retained_targets": accumulated.results[
            [item["member_id"] for item in accumulated.results].index(primary_id)
        ]["supported_population"]["retained_target_observations"],
        "family_size": outcome["family_size"],
        "family_available": sum(1 for item in members if item["inference_available"]),
        "family_any_rejection": bool(
            any(item["nominal_reject_holm"] for item in members if item["inference_available"])
        ),
        "member_ids": [item["member_id"] for item in members],
        "member_available": [bool(item["inference_available"]) for item in members],
        "member_p_raw": [item["p_raw"] for item in members],
        "member_p_holm": [item["p_holm"] for item in members],
        "member_reject_raw": [item["nominal_reject_raw"] for item in members],
        "member_reject_holm": [item["nominal_reject_holm"] for item in members],
    }


# --------------------------------------------------------------------------
# scoring one fixture
# --------------------------------------------------------------------------

def _finite(value: Any) -> bool:
    return value is not None and bool(np.isfinite(float(value)))


def score_fixture(fixture: CandidateFixture, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Score one fixture's attempts against the unchanged acceptance envelope."""
    total = len(rows)
    available = [item for item in rows if item["available"]]
    reject_raw = sum(1 for item in available if item["reject_raw"])
    noncoverage = sum(1 for item in available if item["noncoverage"])
    family = sum(1 for item in rows if item["family_any_rejection"])
    availability = (len(available) / total) if total else None

    # One explicitly counted cohort: available primaries with a finite lift and a
    # finite jackknife SE. The numerator and the denominator use exactly these
    # samples. This is a standard-deviation diagnostic, never a variance factor,
    # and it does not identify the cause of a pass or prove robustness to longer
    # market dependence.
    cohort = [
        item
        for item in available
        if _finite(item["lift"]) and _finite(item["standard_error"])
    ]
    lifts = np.array([item["lift"] for item in cohort], dtype=np.float64)
    errors = np.array([item["standard_error"] for item in cohort], dtype=np.float64)
    empirical_sd = float(np.std(lifts, ddof=1)) if lifts.size > 1 else None
    mean_se = float(np.mean(errors)) if errors.size else None
    rms_se = float(np.sqrt(np.mean(np.square(errors)))) if errors.size else None
    widths = np.array(
        [item["interval_width"] for item in available if _finite(item["interval_width"])],
        dtype=np.float64,
    )
    months = np.array(
        [item["informative_months"] for item in available if item["informative_months"]],
        dtype=np.float64,
    )
    reasons: dict[str, int] = {}
    for item in rows:
        for reason in item["reasons"]:
            reasons[reason] = reasons.get(reason, 0) + 1
    all_lifts = np.array(
        [item["lift"] for item in rows if _finite(item["lift"])], dtype=np.float64
    )

    rates = {
        "primary_raw_rejection": cal.rate_record(
            "primary_raw_rejection", reject_raw, len(available)
        ),
        "primary_interval_noncoverage": cal.rate_record(
            "primary_interval_noncoverage", noncoverage, len(available)
        ),
        "family_wise_holm_rejection": cal.rate_record(
            "family_wise_holm_rejection", family, total
        ),
    }
    marginal = _member_marginals(rows)
    return {
        "order": fixture.order,
        "fixture_id": fixture.fixture_id,
        "name": fixture.name,
        "kind": fixture.kind,
        "required": fixture.required,
        "interpretation": fixture.interpretation,
        "source": fixture.source,
        "padded_days": fixture.padded_days,
        "true_lift": fixture.truth,
        "attempts": total,
        "declared_attempts": fixture.attempts,
        "complete": total == fixture.attempts,
        "primary_availability": availability,
        "primary_availability_meets_floor": (
            None if availability is None else bool(availability >= MIN_PRIMARY_AVAILABILITY)
        ),
        "refusal_reasons": dict(sorted(reasons.items())),
        "rates": rates,
        "rate_checks_passed": (
            None
            if availability is None
            else bool(
                all(item["within_envelope"] for item in rates.values())
                and availability >= MIN_PRIMARY_AVAILABILITY
            )
        ),
        "effect": {
            "true_lift": fixture.truth,
            "n": int(all_lifts.size),
            "mean_lift": float(np.mean(all_lifts)) if all_lifts.size else None,
            "bias": float(np.mean(all_lifts) - fixture.truth) if all_lifts.size else None,
        },
        "scale_diagnostic": {
            "n": len(cohort),
            "population": (
                "Available primaries with a finite lift and a finite jackknife SE; the numerator "
                "and the denominator use exactly these samples. A standard-deviation diagnostic "
                "on this fixture's own repetitions, not a variance factor and not a cause."
            ),
            "empirical_lift_sd": empirical_sd,
            "mean_jackknife_se": mean_se,
            "rms_jackknife_se": rms_se,
            "mean_se_over_empirical_sd": (
                mean_se / empirical_sd if mean_se is not None and empirical_sd else None
            ),
        },
        "interval_width": {
            "n": int(widths.size),
            "mean": float(np.mean(widths)) if widths.size else None,
            "median": float(np.median(widths)) if widths.size else None,
            "note": (
                "Interval width is kept separate from the standard error: the t critical value "
                "affects the width, not the variance estimate."
            ),
        },
        "informative_months": {
            "min": int(months.min()) if months.size else None,
            "max": int(months.max()) if months.size else None,
            "distribution": {
                str(int(value)): int(count)
                for value, count in zip(*np.unique(months, return_counts=True))
            }
            if months.size
            else {},
        },
        "member_marginal_rates": marginal,
        "published_inference": {
            "repetitions_with_p_value": sum(1 for item in rows if item["p_raw"] is not None),
            "repetitions_with_interval": sum(
                1 for item in rows if item["interval_lower"] is not None
            ),
        },
    }


def _member_marginals(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Every family member's descriptive marginal rates, including the longest horizon."""
    if not rows:
        return []
    ids = list(rows[0]["member_ids"])
    marginal: list[dict[str, Any]] = []
    for index, member_id in enumerate(ids):
        available = 0
        reject_raw = 0
        reject_holm = 0
        for item in rows:
            if item["member_available"][index]:
                available += 1
                if item["member_reject_raw"][index]:
                    reject_raw += 1
                if item["member_reject_holm"][index]:
                    reject_holm += 1
        marginal.append(
            {
                "member_id": member_id,
                "available": available,
                "attempts": len(rows),
                "raw_rejection": cal.rate_record("raw_rejection", reject_raw, available),
                "holm_rejection": cal.rate_record("holm_rejection", reject_holm, available),
            }
        )
    return marginal


def impossibility(fixture: CandidateFixture, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """Whether this fixture can no longer pass, however its remaining attempts fall.

    Only a mathematically impossible outcome stops a fixture early; there is no
    early acceptance and no stop on an unfavourable but survivable rate.
    """
    if not fixture.required:
        return None
    available = [item for item in rows if item["available"]]
    unavailable = len(rows) - len(available)
    counts = {
        "primary_raw_rejection": sum(1 for item in available if item["reject_raw"]),
        "primary_interval_noncoverage": sum(1 for item in available if item["noncoverage"]),
        "family_wise_holm_rejection": sum(1 for item in rows if item["family_any_rejection"]),
    }
    for name, value in counts.items():
        if value >= IMPOSSIBLE_ERRORS:
            return {
                "reason": "rate_cannot_pass",
                "rate": name,
                "events": value,
                "attempts_so_far": len(rows),
                "largest_possible_denominator": fixture.attempts,
                "proof": (
                    f"exact_upper_bound({IMPOSSIBLE_ERRORS - 1}, {fixture.attempts}) = "
                    f"{cal.exact_upper_bound(IMPOSSIBLE_ERRORS - 1, fixture.attempts)} <= "
                    f"{ERROR_ENVELOPE} < exact_upper_bound({IMPOSSIBLE_ERRORS}, "
                    f"{fixture.attempts}) = "
                    f"{cal.exact_upper_bound(IMPOSSIBLE_ERRORS, fixture.attempts)}; the count "
                    "can only grow, so no remaining attempt can bring the bound back."
                ),
            }
    if unavailable >= IMPOSSIBLE_UNAVAILABLE:
        return {
            "reason": "availability_cannot_pass",
            "rate": "primary_availability",
            "events": unavailable,
            "attempts_so_far": len(rows),
            "largest_possible_denominator": fixture.attempts,
            "proof": (
                f"{fixture.attempts - IMPOSSIBLE_UNAVAILABLE}/{fixture.attempts} = "
                f"{(fixture.attempts - IMPOSSIBLE_UNAVAILABLE) / fixture.attempts} < "
                f"{MIN_PRIMARY_AVAILABILITY}; the unavailable count can only grow."
            ),
        }
    return None


# --------------------------------------------------------------------------
# resources
# --------------------------------------------------------------------------

class BudgetStop(Exception):
    """Raised when the declared wall-clock or RSS budget can no longer be met."""

    def __init__(self, detail: Mapping[str, Any]) -> None:
        super().__init__(detail.get("message", "budget stop"))
        self.detail = dict(detail)


@dataclass
class Budget:
    """The declared wall-clock and RSS budget, measured rather than assumed.

    A sampled RSS reading is not an OS-enforced allocation limit: it is a guard
    that stops the experiment honestly instead of waiting for an OOM kill.
    """

    started: float
    wall_clock_seconds: float = WALL_CLOCK_BUDGET_SECONDS
    rss_ceiling_bytes: int = RSS_CEILING_BYTES
    samples: list[dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.samples is None:
            self.samples = []

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started

    @property
    def remaining(self) -> float:
        return self.wall_clock_seconds - self.elapsed

    def sample(self, label: str, *, projected_seconds: float | None = None,
               process: Mapping[str, Any] | None = None,
               memory: Mapping[str, Any] | None = None) -> dict[str, Any]:
        process = process_memory() if process is None else process
        memory = host_memory() if memory is None else memory
        peak = process["peak_rss_bytes"]
        record = {
            "label": label,
            "elapsed_seconds": round(self.elapsed, 3),
            "remaining_seconds": round(self.remaining, 3),
            **process,
            "host_memory": memory,
            "projected_remaining_seconds": (
                None if projected_seconds is None else round(projected_seconds, 3)
            ),
        }
        self.samples.append(record)
        available = memory["available_bytes"]
        if peak is None or available is None:
            raise BudgetStop({
                "message": f"required peak resident / available physical measurement unavailable at {label}: "
                           f"{process['unavailable']}; {memory['unavailable']}",
                "kind": "measurement_unavailable", **record,
            })
        if available < HEADROOM_RESERVE_BYTES:
            raise BudgetStop({"message": f"available physical memory below system reserve at {label}",
                              "kind": "headroom", **record})
        if peak > self.rss_ceiling_bytes:
            raise BudgetStop(
                {
                    "message": (
                        f"peak RSS {peak} bytes exceeded the declared ceiling "
                        f"{self.rss_ceiling_bytes} at {label}"
                    ),
                    "kind": "rss_ceiling",
                    **record,
                }
            )
        if self.remaining <= 0.0:
            raise BudgetStop(
                {
                    "message": f"the three-hour wall-clock budget was exhausted at {label}",
                    "kind": "wall_clock",
                    **record,
                }
            )
        if projected_seconds is not None and projected_seconds > self.remaining:
            raise BudgetStop(
                {
                    "message": (
                        f"the projected remaining cost {projected_seconds:.0f}s exceeds the "
                        f"{self.remaining:.0f}s left in the budget at {label}"
                    ),
                    "kind": "projection",
                    **record,
                }
            )
        return record


# --------------------------------------------------------------------------
# bounded wiring and adapter replays
# --------------------------------------------------------------------------

def _remove_owned_replay(path: Path, owner: Path) -> None:
    """Remove only a replay tree created below this call's output root."""
    resolved, base = path.resolve(), owner.resolve()
    if not resolved.is_relative_to(base) or resolved == base:
        raise PatternLabDataError(f"unsafe replay cleanup target {resolved} (owner {base})")
    if path.exists():
        try:
            shutil.rmtree(path)
        except OSError as error:
            raise PatternLabDataError(f"owned replay cleanup failed; retained {resolved}: {error}") from error


def candle_disk_replay(root: Any, *, repetitions: Sequence[int] = (20000, 20001)) -> dict[str, Any]:
    """Prove the in-memory adapter against a real temporary on-disk study.

    The same evidence is written as Parquet, read back through the normal reader
    and joined by the production alignment.  This is deliberately bounded to two
    cases: it is a pipeline proof, not a collector or pack benchmark.
    """
    from ..study import evidence as study_evidence
    from ..study import results as study_results
    from .source import RecordSource

    _grid_start, study_start, study_end, _bars = _candle_bounds()
    members = candle_family()
    base = Path(root)
    comparisons: list[dict[str, Any]] = []
    worst = 0.0
    mismatched: list[str] = []
    for repetition in repetitions:
        tables = candle_evidence(repetition)
        memory = evaluate_candidate(
            accumulate_observations(
                RecordSource(candle_source(tables), members).frames(),
                family=members,
                instruments=cal.INSTRUMENTS,
                study_start_ms=study_start,
                study_end_ms=study_end,
            )
        )
        directory = base / f"candle-{repetition}"
        if directory.exists():
            raise PatternLabDataError(f"replay directory must be new: {directory}")
        try:
            for instrument, frames in tables.items():
                job = study_evidence.job_path(directory, instrument)
                job.mkdir(parents=True, exist_ok=True)
                for name, frame in frames.items():
                    if len(frame):
                        study_evidence.write_table(job / f"{name}.parquet", frame, name=name)
            instance = candle_model_instance()
            family = {
                "instruments": [{"instrument_id": item} for item in cal.INSTRUMENTS],
                "timeframes_minutes": [cal.TIMEFRAME_MINUTES],
                "variants": [dict(item) for item in candle_variants()],
                "models": [instance],
            }
            results = study_results.StudyResults(
                run_root=directory,
                request={"study_name": CANDLE_FIXTURE_NAME, "study": {}},
                protocol={},
                family=family,
                source={"evidence_view_version": 1},
                provenance={"identities": {}},
                status={},
                completion=None,
                complete=True,
                jobs={item: {} for item in cal.INSTRUMENTS},
                job_states={item: "completed" for item in cal.INSTRUMENTS},
                counts={},
            )
            from .source import AdmittedSource

            source = AdmittedSource(
                run_root=directory,
                results=results,
                variants=tuple(family["variants"]),
                instances={CANDLE_MODEL_INSTANCE: instance},
                timeframes=(cal.TIMEFRAME_MINUTES,),
                instruments=tuple(cal.INSTRUMENTS),
                study_start_ms=study_start,
                study_end_ms=study_end,
            )
            disk = evaluate_candidate(
                accumulate_observations(
                    RecordSource(source, members).frames(),
                    family=members,
                    instruments=cal.INSTRUMENTS,
                    study_start_ms=study_start,
                    study_end_ms=study_end,
                )
            )
        finally:
            _remove_owned_replay(directory, base)
        difference = _candidate_difference(memory, disk, expected_member_ids=[item.member_id for item in members])
        worst = max(worst, difference["max_absolute_difference"])
        mismatched.extend(difference["mismatched"])
        comparisons.append({"repetition": int(repetition), **difference})
    return {
        "repetitions": [int(item) for item in repetitions],
        "comparisons": comparisons,
        "max_absolute_difference": worst,
        "mismatched": mismatched,
        "agrees": not mismatched and worst <= 1e-12,
        "note": (
            "The in-memory table provider replaces only the verified table read. The reader, the "
            "shared case expansion, the checked condition and emission alignment, the all-anchor "
            "case view and the membership mapping are the production ones."
        ),
    }


def candidate_evidence_replay(
    root: Any, *, scenario_name: str = "null_dependent_t5", repetitions: Sequence[int] = (20000, 20001)
) -> dict[str, Any]:
    """Route fixed record repetitions through the production checked joins.

    This is a wiring check for the candidate on the joined path; it replaces no
    claim about the original bootstrap's resample count, which this method does
    not use at all.
    """
    from .source import RecordSource

    scenario = cal.SCENARIOS_BY_NAME[scenario_name]
    members = tuple(
        item for item in cal.scenario_family(scenario) if item.comparison_id == "baseline__signal"
    )
    base = Path(root)
    base.mkdir(parents=True, exist_ok=True)
    comparisons: list[dict[str, Any]] = []
    worst = 0.0
    mismatched: list[str] = []
    for repetition in repetitions:
        records = cal.generate_records(scenario, repetition)
        directory = base / f"replay-{repetition}"
        if directory.exists():
            raise PatternLabDataError(f"replay directory must be new: {directory}")
        try:
            source = cal.build_replay_source(records, directory)
            joined = evaluate_candidate(
                accumulate_observations(
                    RecordSource(source, members).frames(),
                    family=members,
                    instruments=cal.INSTRUMENTS,
                    study_start_ms=scenario.study_start_ms,
                    study_end_ms=scenario.study_end_ms,
                )
            )
        finally:
            _remove_owned_replay(directory, base)
        direct = evaluate_candidate(
            accumulate_observations(
                cal.record_frames(records, members, cost=0.0),
                family=members,
                instruments=cal.INSTRUMENTS,
                study_start_ms=scenario.study_start_ms,
                study_end_ms=scenario.study_end_ms,
            )
        )
        difference = _candidate_difference(joined, direct, expected_member_ids=[item.member_id for item in members])
        worst = max(worst, difference["max_absolute_difference"])
        mismatched.extend(difference["mismatched"])
        comparisons.append({"repetition": int(repetition), **difference})
    return {
        "scenario": scenario_name,
        "repetitions": [int(item) for item in repetitions],
        "comparisons": comparisons,
        "max_absolute_difference": worst,
        "mismatched": mismatched,
        "agrees": not mismatched and worst <= 1e-12,
    }


def _candidate_difference(left: Mapping[str, Any], right: Mapping[str, Any], *,
                          expected_member_ids: Sequence[str]) -> dict[str, Any]:
    """The largest disagreement between two candidate evaluations."""
    fields = ("signal", "control", "lift", "p_raw", "p_holm")
    worst = 0.0
    mismatched: list[str] = []
    expected = list(expected_member_ids)
    identities = {side: [item["member_id"] for item in outcome["members"]]
                  for side, outcome in (("left", left), ("right", right))}
    structure = {"expected_member_ids": expected, "expected_member_count": len(expected),
                 **{f"{side}_member_ids": ids for side, ids in identities.items()},
                 **{f"{side}_member_count": len(ids) for side, ids in identities.items()}}
    if not expected or len(set(expected)) != len(expected):
        mismatched.append("expected family is empty or contains duplicate identities")
    for side, outcome in (("left", left), ("right", right)):
        ids = identities[side]
        if ids != expected or len(ids) != len(set(ids)) or outcome.get("family_size") != len(expected):
            mismatched.append(f"{side}: member count / unique ordered identities differ from expected family")
    if mismatched:
        return {"max_absolute_difference": worst, "mismatched": mismatched, **structure}

    def compare(one, other, name):
        nonlocal worst
        if one is None or other is None:
            if one is not other:
                mismatched.append(f"{name}: one side is null")
        elif not _finite(one) or not _finite(other):
            mismatched.append(f"{name}: non-finite comparison")
        else:
            worst = max(worst, abs(float(one) - float(other)))

    for first, second in zip(left["members"], right["members"]):
        if first["member_id"] != second["member_id"]:
            mismatched.append(f"{first['member_id']} != {second['member_id']}")
            continue
        if first["inference_available"] != second["inference_available"]:
            mismatched.append(f"{first['member_id']}: availability differs")
        if first["unavailable_reasons"] != second["unavailable_reasons"]:
            mismatched.append(f"{first['member_id']}: reasons differ")
        for name in fields:
            compare(first[name], second[name], f"{first['member_id']}.{name}")
        for name in ("signal", "control", "lift"):
            one = first["candidate"]["standard_error"][name]
            other = second["candidate"]["standard_error"][name]
            compare(one, other, f"{first['member_id']}.se_{name}")
            one_ci = first["candidate"]["intervals"][name]
            other_ci = second["candidate"]["intervals"][name]
            for bound in ("lower", "upper"):
                compare(None if one_ci is None else one_ci[bound],
                        None if other_ci is None else other_ci[bound],
                        f"{first['member_id']}.{name}.{bound}")
    return {"max_absolute_difference": worst, "mismatched": mismatched, **structure}


# --------------------------------------------------------------------------
# the frozen manifest
# --------------------------------------------------------------------------

def candidate_manifest() -> dict[str, Any]:
    """Every frozen setting of this experiment, saved before any outcome is seen.

    The formula, the generator settings and digests, the family and primary
    definitions, the numerical rules, the support contract, the ordering, the
    seeds and the decision criteria are all recorded here.  Nothing below may be
    tuned after this document is written.
    """
    manifest = {
        "schema_version": admission.EVIDENCE_SCHEMA_VERSION,
        "plan_version": admission.PLAN_VERSION,
        "decision_policy_version": admission.DECISION_POLICY_VERSION,
        "method": CANDIDATE_METHOD_ID,
        "scope": "experimental_research_candidate",
        "formula": {
            "aggregation": (
                "For each retained stratum s with r_s = e_s/c_s, the signal-month vector is "
                "Q_m = [sum a_s, sum r_s b_s, sum (a_s - r_s b_s)] and N_m = sum e_s. With "
                "N = sum_m N_m, theta = sum_m Q_m / N is (signal, matched control, lift)."
            ),
            "jackknife": (
                "theta_(-m) = (sum_l Q_l - Q_m) / (N - N_m); theta_bar = mean_m theta_(-m); "
                "V_J = (G-1)/G * sum_m outer(theta_(-m) - theta_bar, theta_(-m) - theta_bar); "
                "SE_j = sqrt(V_J[j,j])."
            ),
            "reference": (
                "CI_j = theta_j +/- t_quantile(.975, G-1) * SE_j and "
                "p_lift = 2 * t_survival(|theta_lift| / SE_lift, G-1). The reported point "
                "estimate is the original full-sample theta, never the deletion average."
            ),
            "degeneracy": (
                "mass = sum_s (|a_s| + |r_s b_s|) / N; amplification = max_m N/(N - N_m); "
                "scale_lift = max(|theta_lift|, mass * amplification); "
                f"threshold = {DEGENERACY_MULTIPLIER} * finfo(float64).eps * scale_lift; "
                "degenerate iff SE_lift <= threshold. Exact zero stays unavailable even when the "
                "threshold is zero."
            ),
            "covariance": (
                "V_J is positive semidefinite and singular by construction. Only its diagonal is "
                "used: it is never inverted, Cholesky-factored or ridged."
            ),
            "validity": {
                "min_informative_months": MIN_INFORMATIVE_MONTHS,
                "positive_deletion_denominator": True,
                "finite_inputs_and_results": True,
                "nondegenerate_contrast": True,
                "note": (
                    "These are mathematical validity rules, not new support thresholds. The "
                    "production full-sample strata, support, coverage and horizon gates are "
                    "applied once, by the shared accumulation, and are never reapplied to a "
                    "deleted-month sample. Partial endpoint months, unequal counts and outcomes "
                    "crossing month boundaries are preserved; each outcome belongs to its signal "
                    "month. There is no equal-month reweighting, trimming, post-hoc concentration "
                    "gate or selection of successful months."
                ),
            },
        },
        "family": {
            "declaration": (
                "The entire unchanged declared family of each fixture, both directions and all "
                "four horizons. Holm uses the candidate's own p-values with the existing "
                "deterministic (p, stable_id) tie handling; unavailable members keep internal "
                "p=1 and display unavailable inference. No original bootstrap p-value enters the "
                "candidate's family-wise result."
            ),
            "record_primary": f"{cal.PRIMARY_DIRECTION} {cal.PRIMARY_HORIZON_MINUTES}m",
            "candle_primary_member_id": candle_primary_member_id(),
            "alpha": ALPHA,
            "confidence_level": CONFIDENCE_LEVEL,
        },
        "generators": {
            "record_contract": cal._contract_with_digest(),
            "candle_contract": candle_generator_contract(),
        },
        "seeds": {
            "master_seed": cal.MASTER_SEED,
            "construction": "SeedSequence([master, scenario_id, repetition_id, stream_id])",
            "statistical_range": [FIRST_REPETITION_ID, LAST_REPETITION_ID],
            "pilot_ids": list(range(FIRST_REPETITION_ID, FIRST_REPETITION_ID + PILOT_ATTEMPTS)),
            "excluded": (
                "Historical IDs 0-399 and audit IDs 10000-10031 are not fresh validation and are "
                "not used here. No seed or resample count governs the candidate formula."
            ),
        },
        "matrix": {
            "main": [asdict(item) for item in MAIN_MATRIX],
            "supplementary": [asdict(item) for item in SUPPLEMENTARY_MATRIX],
            "max_main_attempts": MAX_MAIN_ATTEMPTS,
            "order_note": (
                "The order is fixed so the difficult fixtures run first. The candidate scores "
                "this explicit fixture set even where a legacy scenario is admitted=False in the "
                "legacy protocol."
            ),
        },
        "decision": {
            "error_envelope": ERROR_ENVELOPE,
            "min_primary_availability": MIN_PRIMARY_AVAILABILITY,
            "rates": list(cal.ACCEPTANCE_RATES),
            "denominators": (
                "Primary raw rejection and nominal-95% noncoverage are counted among available "
                "primary attempts; Holm family-wise rejection is counted among all attempted "
                "repetitions; availability is scored separately against every attempt."
            ),
            "impossible_errors": IMPOSSIBLE_ERRORS,
            "impossible_unavailable": IMPOSSIBLE_UNAVAILABLE,
            "impossibility_proof": {
                "upper_bound_139_of_2000": cal.exact_upper_bound(139, MAIN_ATTEMPTS),
                "upper_bound_140_of_2000": cal.exact_upper_bound(140, MAIN_ATTEMPTS),
                "availability_at_101_unavailable": (MAIN_ATTEMPTS - 101) / MAIN_ATTEMPTS,
            },
            "duality_disclosure": (
                "The candidate's interval and test are an exact inversion of one another, so for "
                "a zero-truth fixture raw rejection and nominal-95% noncoverage are the same "
                "event apart from numerical or exact-boundary conventions. Both columns are kept "
                "for compatibility; the 24 displayed rate checks are not 24 independent "
                "confirmations."
            ),
            "mirror_disclosure": (
                "Constant-cost long and short lifts are exact sign mirrors with equal candidate "
                "p-values. For fixture 102 with identical availability and a fixed proportional "
                "fee f, lift_short = -(1+f)/(1-f) * lift_long and the SE scales by the same "
                "positive factor, so its two-sided t p-values also coincide apart from roundoff. "
                "The full frozen family is kept; Holm over tied pairs is conservative, so the "
                "family-wise rate is limited global-null evidence, not proof of strong "
                "family-wise control."
            ),
            "month_count_disclosure": (
                "The 252 active-day support gate implies at least nine occupied calendar months, "
                "so a pure-helper G=2 case is mathematically valid but is not admitted inference. "
                "That lower bound does not prove that every G>=9 population passes the other "
                "support gates. The frozen matrix is expected to cover G=12 only: a PASS supplies "
                "no calibration evidence for other month counts or for all admitted support "
                "geometries, and broader production adoption remains a later decision."
            ),
            "stress_disclosure": (
                "Refusal, long-dependence and planted fixtures are required disclosures. Their "
                "rates are reported separately and are never added to the admitted-null 8% gate; "
                "the planted results are wiring checks, not a power gate."
            ),
        },
        "budget": {
            "wall_clock_seconds": WALL_CLOCK_BUDGET_SECONDS,
            "rss_ceiling_bytes": RSS_CEILING_BYTES,
            "headroom_required_bytes": HEADROOM_REQUIRED_BYTES,
            "headroom_reserve_bytes": HEADROOM_RESERVE_BYTES,
            "pilot_attempts": PILOT_ATTEMPTS,
            "note": (
                "The pilot uses the already frozen formula and its completed records are reused "
                "in the final matrix; there is no reroll and no parameter change after observing "
                "pilot results. A budget stop is an INCOMPLETE delivery, not a statistical "
                "rejection of the candidate."
            ),
        },
        "integration": (
            "No production analysis request, method default, sealed artifact schema or HTML "
            "rendering exposes this candidate. The existing bootstrap remains implemented and "
            "explicitly unvalidated. Even a PASS leads to a later integration decision, not "
            "automatic adoption."
        ),
        "implementation": artifacts.module_digests(),
        "research_implementation": research_digests(),
    }
    manifest["manifest_digest"] = contracts.semantic_digest(manifest)
    return manifest


def environment_document(*, include_memory: bool = True) -> dict[str, Any]:
    """The interpreter, packages and host conditions this run actually used."""
    import scipy

    try:
        import pyarrow

        arrow_version = pyarrow.__version__
    except Exception:  # pragma: no cover - the replay checks need it, the matrix does not
        arrow_version = None
    return {
        "python": sys.version,
        "python_version": platform.python_version(),
        "executable": sys.executable,
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "scipy": scipy.__version__,
        "pyarrow": arrow_version,
        "thread_settings_in_effect": {name: os.environ.get(name) for name in THREAD_VARIABLES},
        "thread_settings_inherited": dict(_INHERITED_THREAD_SETTINGS),
        **({"host_memory": host_memory(), "process_memory": process_memory()} if include_memory else {}),
        "note": (
            "Thread pinning binds only when this module is imported before NumPy. A Linux run "
            "does not certify Windows behaviour."
        ),
    }


# --------------------------------------------------------------------------
# the experiment driver
# --------------------------------------------------------------------------

def _write_json(path: Path, payload: Any) -> None:
    """Strict JSON: no NaN, no Infinity, no silently dropped record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=1, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )


def run_fixture(
    fixture: CandidateFixture,
    *,
    attempts: int,
    budget: Budget,
    reuse: Mapping[int, Mapping[str, Any]] | None = None,
    progress=None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Run one fixture's attempts, reusing any already completed pilot records."""
    rows: list[dict[str, Any]] = []
    stop: dict[str, Any] | None = None
    reuse = dict(reuse or {})
    started = time.monotonic()
    for index in range(attempts):
        repetition = FIRST_REPETITION_ID + index
        if repetition > LAST_REPETITION_ID:
            raise PatternLabDataError(
                f"{fixture.label}: repetition ID {repetition} leaves the frozen validation range "
                f"[{FIRST_REPETITION_ID}, {LAST_REPETITION_ID}]."
            )
        completed = reuse.get(repetition)
        rows.append(dict(completed) if completed is not None else run_candidate_repetition(fixture, repetition))
        done = index + 1
        if done % PROGRESS_BATCH == 0 or done == attempts:
            elapsed = time.monotonic() - started
            per = elapsed / done
            measurement = budget.sample(
                f"{fixture.label}:{done}/{attempts}",
                projected_seconds=per * (attempts - done),
            )
            if progress is not None:
                progress(
                    f"{fixture.label}: {done}/{attempts} "
                    f"({elapsed:.0f}s, {per:.3f}s/rep, peak "
                    f"{measurement['peak_rss_bytes'] / 2**20:.0f}MB)"
                )
        stop = impossibility(fixture, rows)
        if stop is not None:
            if progress is not None:
                progress(f"{fixture.label}: stopping early, {stop['reason']}")
            break
    return rows, stop


def run_experiment(
    *,
    output_root: Any,
    fixtures: Sequence[CandidateFixture] | None = None,
    attempts: int | None = None,
    include_supplementary: bool = True,
    include_replays: bool = True,
    progress=None,
) -> dict[str, Any]:
    """Run the frozen decision matrix and save its compact retained evidence.

    The manifest is written **before** any outcome is inspected.  Per-fixture
    records are written as each fixture completes, so an interrupted or
    budget-stopped run still retains its partial evidence honestly.
    """
    root = Path(output_root).expanduser()
    _validate_attempts(attempts)
    if root.exists():
        raise PatternLabDataError(f"{root}: output root must be a new directory")
    (root / "records").mkdir(parents=True)
    manifest = candidate_manifest()
    environment = environment_document()
    _write_json(root / "manifest.json", manifest)
    _write_json(root / "environment.json", environment)

    memory = environment["host_memory"]
    available = memory.get("available_bytes")
    ceiling = RSS_CEILING_BYTES
    headroom_note = None
    if available is not None:
        ceiling = min(RSS_CEILING_BYTES, max(available - HEADROOM_RESERVE_BYTES, 0))
        headroom_note = (
            f"Available physical memory was {available} bytes, so the effective ceiling is "
            f"{ceiling} bytes: the declared 1 GiB ceiling tightened to leave "
            f"{HEADROOM_RESERVE_BYTES} bytes of system headroom."
        )
    budget = Budget(started=time.monotonic(), rss_ceiling_bytes=ceiling)

    planned = list(fixtures if fixtures is not None else MAIN_MATRIX)
    if fixtures is None and include_supplementary:
        planned = planned + list(SUPPLEMENTARY_MATRIX)
    results: list[dict[str, Any]] = []
    stops: list[dict[str, Any]] = []
    status = "completed"
    incomplete_reason: str | None = None

    try:
        budget.sample("before_generation", process=environment["process_memory"], memory=memory)
    except BudgetStop as stop:
        status = "incomplete"
        incomplete_reason = stop.detail["message"]
        stops.append(stop.detail)
        planned = []

    if available is not None and available < HEADROOM_REQUIRED_BYTES:
        status = "incomplete"
        incomplete_reason = (
            f"the host reported only {available} bytes of available RAM against the declared "
            f"{HEADROOM_REQUIRED_BYTES}-byte headroom requirement, with "
            f"{memory.get('swap_total_bytes')} bytes of swap; the matrix was not started."
        )
        planned = []

    pilot: dict[str, Any] | None = None
    reuse: dict[str, dict[int, Mapping[str, Any]]] = {}
    required = [item for item in planned if item.required]
    if required:
        try:
            pilot, reuse = _run_pilot(
                required, planned=planned, budget=budget, attempts=attempts,
                include_replays=include_replays, progress=progress
            )
            _write_json(root / "pilot.json", pilot)
            projection = pilot["projected_total_seconds"]
            budget.sample("pilot", projected_seconds=projection)
        except BudgetStop as stop:
            status = "incomplete"
            incomplete_reason = stop.detail["message"]
            stops.append(stop.detail)
            planned = []

    for fixture in planned:
        count = int(attempts if attempts is not None else fixture.attempts)
        if progress is not None:
            progress(f"{fixture.label}: starting {count} attempts")
        try:
            rows, stop = run_fixture(
                fixture,
                attempts=count,
                budget=budget,
                reuse=reuse.get(fixture.label),
                progress=progress,
            )
        except BudgetStop as budget_stop:
            status = "incomplete"
            incomplete_reason = budget_stop.detail["message"]
            stops.append(budget_stop.detail)
            break
        _write_json(root / "records" / f"{fixture.label}.json", {"rows": rows})
        scored = score_fixture(fixture, rows)
        if stop is not None:
            scored["early_stop"] = stop
            stops.append({"fixture": fixture.label, **stop})
        results.append(scored)
        if fixture.required and (
            stop is not None
            or (scored["complete"] and scored["rate_checks_passed"] is False)
        ):
            if progress is not None:
                progress(f"{fixture.label}: a required gate failed; the matrix ends here")
            break

    replays: dict[str, Any] | None = None
    if include_replays and status != "incomplete":
        if progress is not None:
            progress("bounded replay and adapter checks")
        try:
            budget.sample("before_replays")
            replays = {
                "candle_disk_replay": candle_disk_replay(root / "_replay-candle"),
                "candidate_evidence_replay": candidate_evidence_replay(root / "_replay-records"),
            }
            budget.sample("after_replays")
            _remove_owned_replay(root / "_replay-candle", root)
            _remove_owned_replay(root / "_replay-records", root)
        except BudgetStop as stop:
            status = "incomplete"
            incomplete_reason = stop.detail["message"]
            stops.append(stop.detail)
        except (OSError, PatternLabDataError) as error:
            status = "incomplete"
            incomplete_reason = f"bounded replay failed: {error}"

    document = {
        "schema_version": admission.EVIDENCE_SCHEMA_VERSION,
        "plan_version": admission.PLAN_VERSION,
        "decision_policy_version": admission.DECISION_POLICY_VERSION,
        "method": CANDIDATE_METHOD_ID,
        "generated_utc": artifacts.now_utc(),
        "manifest_digest": manifest["manifest_digest"],
        "status": status,
        "incomplete_reason": incomplete_reason,
        "planned_fixtures": [item.label for item in (fixtures if fixtures is not None else MAIN_MATRIX)]
        + ([item.label for item in SUPPLEMENTARY_MATRIX] if fixtures is None and include_supplementary else []),
        "attempts_override": attempts,
        "diagnostic_selection": fixtures is not None,
        "results": results,
        "pilot": pilot,
        "stops": stops,
        "replays": replays,
        "budget": {
            "wall_clock_seconds": WALL_CLOCK_BUDGET_SECONDS,
            "declared_rss_ceiling_bytes": RSS_CEILING_BYTES,
            "effective_rss_ceiling_bytes": budget.rss_ceiling_bytes,
            "headroom_note": headroom_note,
            "elapsed_seconds": round(budget.elapsed, 3),
            "peak_rss_bytes": max((item["peak_rss_bytes"] for item in budget.samples
                                   if item["peak_rss_bytes"] is not None), default=None),
            "samples": budget.samples,
            "note": (
                "A sampled RSS reading is a guard, not an OS-enforced allocation limit. Ordinary "
                "implementation and pytest time are outside this experiment budget."
            ),
        },
    }
    _write_json(root / "run.json", document)
    return document


REPLAY_ALLOWANCE_SECONDS = 300.0


def _validate_attempts(attempts: int | None) -> None:
    if attempts is not None and (type(attempts) is not int or attempts < 1):
        raise PatternLabDataError("attempts: expected a positive integer")


def _run_pilot(
    fixtures: Sequence[CandidateFixture],
    *,
    planned: Sequence[CandidateFixture],
    budget: Budget,
    attempts: int | None = None,
    include_replays: bool = True,
    progress=None,
) -> tuple[dict[str, Any], dict[str, dict[int, Mapping[str, Any]]]]:
    """A fixed five-attempt pilot per required fixture, reused by the final matrix.

    The projection covers the whole planned run — the required matrix, the
    disclosure fixtures priced at the pilot's own record-fixture cost, and a
    fixed allowance for the bounded replays — so a stop is decided against the
    real remaining work rather than against the required matrix alone.
    """
    entries: list[dict[str, Any]] = []
    _validate_attempts(attempts)
    pilot_count = min(PILOT_ATTEMPTS, attempts) if attempts is not None else PILOT_ATTEMPTS
    reuse: dict[str, dict[int, Mapping[str, Any]]] = {}
    projected = 0.0
    record_costs: list[float] = []
    for fixture in fixtures:
        started = time.monotonic()
        rows = [
            run_candidate_repetition(fixture, FIRST_REPETITION_ID + index)
            for index in range(pilot_count)
        ]
        elapsed = time.monotonic() - started
        per = elapsed / pilot_count
        reuse[fixture.label] = {int(item["id"]): item for item in rows}
        if fixture.source == "records":
            record_costs.append(per)
        remaining = per * ((attempts if attempts is not None else fixture.attempts) - pilot_count)
        projected += remaining
        entries.append(
            {
                "fixture": fixture.label,
                "attempts": pilot_count,
                "elapsed_seconds": round(elapsed, 3),
                "seconds_per_repetition": round(per, 4),
                "projected_remaining_seconds": round(remaining, 1),
                "available": sum(1 for item in rows if item["available"]),
                "informative_months": sorted(
                    {item["informative_months"] for item in rows if item["informative_months"]}
                ),
            }
        )
        budget.sample(f"pilot:{fixture.label}")
        if progress is not None:
            progress(f"pilot {fixture.label}: {per:.3f}s/rep, projecting {remaining:.0f}s")
    record_cost = float(np.mean(record_costs)) if record_costs else 0.0
    supplementary = [item for item in planned if not item.required]
    supplementary_seconds = record_cost * sum(attempts if attempts is not None else item.attempts
                                               for item in supplementary)
    replay_allowance = REPLAY_ALLOWANCE_SECONDS if include_replays else 0.0
    projected += supplementary_seconds + replay_allowance
    return (
        {
            "attempts_per_fixture": pilot_count,
            "mean_record_seconds_per_repetition": round(record_cost, 4),
            "projected_supplementary_seconds": round(supplementary_seconds, 1),
            "replay_allowance_seconds": replay_allowance,
            "repetition_ids": list(range(FIRST_REPETITION_ID, FIRST_REPETITION_ID + pilot_count)),
            "entries": entries,
            "projected_total_seconds": round(projected, 1),
            "note": (
                "The pilot uses the already frozen formula and its completed records are reused "
                "in the final matrix. There is no reroll and no parameter change after observing "
                "pilot results."
            ),
        },
        reuse,
    )


# --------------------------------------------------------------------------
# the offline summarizer
# --------------------------------------------------------------------------

def read_records(root: Path, label: str) -> dict[str, Any] | None:
    """Read the unambiguous plain/gzip representation using strict JSON."""
    raw = admission.record_bytes(root, label)
    return None if raw is None else admission.load_json(raw)


def _fixture_family(fixture):
    if fixture.source == "candles":
        return [item.member_id for item in candle_family()], candle_primary_member_id()
    scenario = cal.SCENARIOS_BY_NAME[fixture.scenario_name]
    return [item.member_id for item in cal.scenario_family(scenario)], cal.primary_member_id(scenario)


def summarize(output_root: Any) -> dict[str, Any]:
    """One admission/decision path for fresh runs and offline historical re-scoring.

    No generator, estimator or memory API is executed. Counts come only from
    admitted records. Missing evidence is distinct from contradictory evidence.
    """
    root = Path(output_root).expanduser()
    fixtures = MAIN_MATRIX + SUPPLEMENTARY_MATRIX
    labels = [item.label for item in fixtures]
    problems, incomplete = [], []
    manifest, run = {}, None
    input_hashes = {}
    for filename in ("manifest.json", "run.json"):
        try:
            raw = (root / filename).read_bytes()
            input_hashes[filename] = hashlib.sha256(raw).hexdigest()
            value = admission.load_json(raw)
            if not isinstance(value, dict):
                raise ValueError("expected a JSON object")
            if filename == "manifest.json":
                manifest = value
            else:
                run = value
        except FileNotFoundError:
            incomplete.append(f"{filename} is missing")
        except (OSError, ValueError, TypeError) as error:
            problems.append(f"{filename}: {error}")
    try:
        names = list(dict.fromkeys(f.scenario_name for f in fixtures if f.scenario_name))
        problems.extend(admission.manifest_problems(manifest, candidate_manifest(), names, RESEARCH_MODULES))
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        problems.append(f"malformed manifest: {error}")
    run_missing, run_errors = admission.run_problems(run, manifest, labels)
    incomplete.extend(run_missing)
    problems.extend(run_errors)
    supplied_checksums = None
    try:
        supplied_checksums = admission.checksums(root, labels)
    except (OSError, ValueError) as error:
        problems.append(str(error))
    if (root / "records.sha256").is_file():
        input_hashes["records.sha256"] = hashlib.sha256((root / "records.sha256").read_bytes()).hexdigest()
    for path in (root / "records").glob("*.json*"):
        label = path.name.removesuffix(".gz").removesuffix(".json")
        if label not in labels or path.name not in (f"{label}.json", f"{label}.json.gz"):
            problems.append(f"unknown record file {path.name}")
    results, rows_by_label, missing = [], {}, []
    new_format = manifest.get("schema_version") == admission.EVIDENCE_SCHEMA_VERSION
    for fixture in fixtures:
        try:
            raw = admission.record_bytes(root, fixture.label)
            if raw is None:
                missing.append(fixture.label)
                incomplete.append(f"required fixture {fixture.label} produced no records")
                continue
            digest = hashlib.sha256(raw).hexdigest()
            input_hashes[f"records/{fixture.label}.json (uncompressed)"] = digest
            if supplied_checksums is not None and supplied_checksums.get(fixture.label) != digest:
                raise ValueError("supplied checksum missing or mismatching consumed bytes")
            saved = admission.load_json(raw)
            rows = saved["rows"]
            member_ids, primary_id = _fixture_family(fixture)
            admission.validate_rows(rows, fixture=fixture, member_ids=member_ids,
                                    primary_id=primary_id, new_format=new_format)
            if run:
                planned = run.get("planned_fixtures")
                if isinstance(planned, list) and fixture.label not in planned:
                    raise ValueError("records exist for an unplanned fixture")
                override = run.get("attempts_override")
                if type(override) is int and (len(rows) > override or
                        any(row["id"] >= FIRST_REPETITION_ID + override for row in rows)):
                    raise ValueError("record count exceeds requested attempts override")
            rows_by_label[fixture.label] = rows
            scored = score_fixture(fixture, rows)
            results.append(scored)
            if not scored["complete"]:
                incomplete.append(f"required fixture {fixture.label} did not complete its declared attempts")
        except (OSError, EOFError, KeyError, TypeError, ValueError, IndexError, AttributeError) as error:
            problems.append(f"{fixture.label}: {error}")
    replay_families = {
        "candle_disk_replay": [item.member_id for item in candle_family()],
        "candidate_evidence_replay": [item.member_id for item in cal.scenario_family(
            cal.SCENARIOS_BY_NAME["null_dependent_t5"]) if item.comparison_id == "baseline__signal"],
    }
    replay_missing, replay_errors = admission.replay_problems(
        (run or {}).get("replays"), replay_families, new_format=new_format)
    incomplete.extend(replay_missing)
    problems.extend(replay_errors)
    proven_stops = []
    stops = (run or {}).get("stops", [])
    if not isinstance(stops, list) or any(not isinstance(item, dict) for item in stops):
        problems.append("run stops must be a list of objects")
        stops = []
    for stop in stops:
        if stop.get("reason") in ("rate_cannot_pass", "availability_cannot_pass"):
            fixture = next((f for f in fixtures if f.label == stop.get("fixture")), None)
            rows = rows_by_label.get(stop.get("fixture"))
            proof = impossibility(fixture, rows) if fixture is not None and rows is not None else None
            keys = ("reason", "rate", "events", "attempts_so_far", "largest_possible_denominator")
            if proof is None or not admission.same({k: stop.get(k) for k in keys}, {k: proof[k] for k in keys}):
                problems.append(f"claimed statistical stop is not proved by saved records: {stop.get('fixture')}")
            else:
                proven_stops.append(stop)
        elif stop.get("kind") not in ("rss_ceiling", "wall_clock", "projection", "headroom", "measurement_unavailable"):
            problems.append("unknown stop record")
        elif (run or {}).get("status") == "completed":
            problems.append("completed run contradicts a budget stop")
    failing = [item["name"] for item in results if item["required"] and item["complete"]
               and item["rate_checks_passed"] is not True]
    refusal_failures = [f.label for f in fixtures if f.kind == "refusal" and
                        any(any(r["member_available"]) for r in rows_by_label.get(f.label, []))]
    decision, reasons = _decide(problems=problems, incomplete=incomplete, failing=failing,
                                refusal_failures=refusal_failures, proven_stops=proven_stops,
                                run_document=run)
    limitations = []
    if supplied_checksums is None:
        limitations.append("records.sha256: not supplied; semantic admission still applies")
    if manifest.get("schema_version") == 1:
        limitations.extend([
            "Historical format 1 omits pre-run candidate/generator research source hashes; no hashes are retroactively inserted.",
            "Historical replay comparisons contain no ordered member/count evidence; new structural checks were not historically recorded.",
        ])
    summary = {
        "schema_version": admission.EVIDENCE_SCHEMA_VERSION,
        "plan_version": admission.PLAN_VERSION,
        "decision_policy_version": admission.DECISION_POLICY_VERSION,
        "method": CANDIDATE_METHOD_ID,
        "manifest_digest": manifest.get("manifest_digest"),
        "decision": decision,
        "decision_reasons": reasons,
        "integrity_problems": problems,
        "incomplete_evidence": list(dict.fromkeys(incomplete)),
        "required_fixtures": [item.label for item in fixtures if item.required],
        "missing_required_fixtures": missing,
        "incomplete_required_fixtures": [item["name"] for item in results if not item["complete"]],
        "failing_required_fixtures": failing + refusal_failures,
        "results": results,
        "replays": (run or {}).get("replays"),
        "stops": stops,
        "budget": (run or {}).get("budget"),
        "verification": {
            "input_files_sha256": input_hashes,
            "input_identity": contracts.semantic_digest(input_hashes),
            "original_manifest_digest": manifest.get("manifest_digest"),
            "producer_implementation": manifest.get("implementation"),
            "producer_research_implementation": manifest.get("research_implementation"),
            "producer_commit": None,
            "producer_commit_note": "No original commit is asserted by these manifest fields.",
            "verifier_implementation": artifacts.module_digests(),
            "verifier_research_implementation": research_digests(),
            "environment": environment_document(include_memory=False),
            "provenance_limitations": limitations,
        },
        "scope": (
            "A PASS means the candidate passed this synthetic contract only. It is not adoption, "
            "not market error control, and it supplies no calibration evidence for month counts "
            "other than those observed. Product integration remains a separate task and M3a "
            "production inference is still unaccepted."
        ),
    }
    _write_json(root / "summary.json", summary)
    (root / "summary.md").write_text(render_summary(summary), encoding="utf-8")
    return summary


def _decide(*, problems, incomplete, failing, refusal_failures, proven_stops, run_document):
    """Only coherent saved evidence can establish statistical failure."""
    if problems:
        return "INCOMPLETE", [f"evidence integrity: {item}" for item in problems] + list(dict.fromkeys(incomplete))
    if run_document and run_document.get("status") == "completed" and run_document.get("attempts_override") is None:
        reasons = [f"required fixture {name} completed its declared attempts and failed its rate or availability gate"
                   for name in failing]
        reasons += [f"required refusal fixture {name} published inference" for name in refusal_failures]
        reasons += [f"required fixture {s['fixture']} cannot pass: {s['rate']} reached {s['events']} events "
                    f"after {s['attempts_so_far']} attempts" for s in proven_stops]
        if reasons:
            return "FAIL", reasons
    if incomplete:
        return "INCOMPLETE", list(dict.fromkeys(incomplete))
    return "PASS", ["All 17 plan-1 entries completed, all eight main gates passed, every refusal withheld "
                    "inference, stress/planted disclosures completed, and both required replays agreed."]


def _percent(value: Any) -> str:
    return "n/a" if value is None else f"{float(value) * 100:.2f}%"


def _number(value: Any, digits: int = 4) -> str:
    return "n/a" if value is None else f"{float(value):.{digits}g}"


def render_summary(summary: Mapping[str, Any]) -> str:
    """The readable Markdown conclusion, rendered from the saved summary alone."""
    lines = [
        f"# {summary['method']} — candidate decision: **{summary['decision']}**",
        "",
        f"Manifest digest `{summary['manifest_digest']}`.",
        f"Plan version {admission.PLAN_VERSION}; decision policy {admission.DECISION_POLICY_VERSION}.",
        "",
        "This is an experimental research candidate. It is not integrated into any analysis",
        "request, sealed artifact or report, and no outcome here accepts M3a or starts M3b.",
        "",
        "## Decision reasons",
        "",
    ]
    lines.extend(f"- {reason}" for reason in summary["decision_reasons"])
    lines.extend(
        [
            "",
            "## Required matrix",
            "",
            "| # | Fixture | Attempts | Availability | Raw rejection (UB) | Noncoverage (UB) "
            "| Holm FWER (UB) | Passed |",
            "| ---: | --- | ---: | ---: | --- | --- | --- | :---: |",
        ]
    )
    for item in summary["results"]:
        if not item["required"]:
            continue
        rates = item["rates"]
        lines.append(
            f"| {item['order']} | {item['fixture_id']:03d} {item['name']} | "
            f"{item['attempts']} | {_percent(item['primary_availability'])} | "
            f"{_percent(rates['primary_raw_rejection']['rate'])} "
            f"({_percent(rates['primary_raw_rejection']['one_sided_95_upper_bound'])}) | "
            f"{_percent(rates['primary_interval_noncoverage']['rate'])} "
            f"({_percent(rates['primary_interval_noncoverage']['one_sided_95_upper_bound'])}) | "
            f"{_percent(rates['family_wise_holm_rejection']['rate'])} "
            f"({_percent(rates['family_wise_holm_rejection']['one_sided_95_upper_bound'])}) | "
            f"{'yes' if item['rate_checks_passed'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "Rates are counted on their own denominators: raw rejection and nominal-95%",
            "noncoverage among available primary attempts, Holm family-wise rejection among every",
            "attempted repetition. Each bound is a separate one-sided 95% exact binomial upper",
            "bound against the declared 8% envelope; the collection is not a joint 95% statement,",
            "the ceiling is not a test alpha, and passing does not imply a true 5% error rate.",
            "The interval and the test are an exact inversion, so on a zero-truth fixture the raw",
            "rejection and noncoverage columns are the same event: they are not independent",
            "confirmations.",
            "",
            "## Scale and balance diagnostics",
            "",
            "| Fixture | Cohort | Empirical lift SD | Mean jackknife SE | RMS SE | Mean SE / SD "
            "| G |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for item in summary["results"]:
        scale = item["scale_diagnostic"]
        months = item["informative_months"]
        span = (
            "n/a"
            if months["min"] is None
            else (str(months["min"]) if months["min"] == months["max"] else f"{months['min']}-{months['max']}")
        )
        lines.append(
            f"| {item['fixture_id']:03d} {item['name']} | {scale['n']} | "
            f"{_number(scale['empirical_lift_sd'])} | {_number(scale['mean_jackknife_se'])} | "
            f"{_number(scale['rms_jackknife_se'])} | "
            f"{_number(scale['mean_se_over_empirical_sd'], 4)} | {span} |"
        )
    lines.extend(
        [
            "",
            "These are cheap record-derived standard-deviation diagnostics. They do not identify",
            "the cause of a pass and do not establish robustness to longer market dependence.",
            "Interval width is reported separately, because the t critical value affects the",
            "width and not the variance estimate.",
            "",
            "## Mandatory disclosures (rates outside the admitted-null ceiling)",
            "",
            "| Fixture | Kind | Attempts | Availability | Raw rejection | Holm FWER |",
            "| --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for item in summary["results"]:
        if item["required"]:
            continue
        rates = item["rates"]
        lines.append(
            f"| {item['fixture_id']:03d} {item['name']} | {item['kind']} | {item['attempts']} | "
            f"{_percent(item['primary_availability'])} | "
            f"{_percent(rates['primary_raw_rejection']['rate'])} | "
            f"{_percent(rates['family_wise_holm_rejection']['rate'])} |"
        )
    lines.extend(
        [
            "",
            "Refusal, long-dependence and planted fixtures are required disclosures. Their rates",
            "are never added to the admitted-null envelope, and the planted results are wiring",
            "checks rather than a power gate. Completion and actual refusal are required for PASS.",
            "",
            "## Scope",
            "",
            summary["scope"],
            "",
        ]
    )
    limitations = summary.get("verification", {}).get("provenance_limitations", [])
    if limitations:
        lines.extend(["## Provenance limitations", ""])
        lines.extend(f"- {item}" for item in limitations)
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# the command
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.pattern_lab.analysis.calibration_monthly",
        description=(
            "Run the experimental monthly_cluster_jackknife_v1 candidate over its frozen "
            "synthetic decision matrix and save the compact retained evidence. This is a "
            "research-only go/no-go experiment: it integrates no method, accepts no milestone "
            "and certifies no market error rate."
        ),
    )
    parser.add_argument("--output-root", type=Path, required=True, metavar="DIR")
    parser.add_argument(
        "--fixtures", nargs="*", default=None, metavar="LABEL",
        help=(
            "Fixture labels to run instead of the frozen matrix, for a smoke run only. Any "
            "selection other than the default makes the decision INCOMPLETE."
        ),
    )
    parser.add_argument(
        "--attempts", type=int, default=None, metavar="N",
        help="Override every fixture's declared attempt count (smoke runs only).",
    )
    parser.add_argument(
        "--skip-supplementary", action="store_true",
        help="Omit mandatory disclosures (leaves the decision INCOMPLETE).",
    )
    parser.add_argument(
        "--skip-replays", action="store_true",
        help="Skip the bounded replay and adapter checks (leaves the decision INCOMPLETE).",
    )
    parser.add_argument(
        "--summarize-only", action="store_true",
        help="Rebuild summary.json and summary.md from an existing output root and stop.",
    )
    return parser


def _selected(labels: Sequence[str] | None) -> Sequence[CandidateFixture] | None:
    if labels is None:
        return None
    if len(set(labels)) != len(labels):
        raise PatternLabDataError("duplicate candidate fixture labels")
    known = {item.label: item for item in MAIN_MATRIX + SUPPLEMENTARY_MATRIX}
    unknown = sorted(set(labels) - set(known))
    if unknown:
        raise PatternLabDataError(
            f"unknown candidate fixture label(s) {unknown}; declared labels are {sorted(known)}."
        )
    return [known[item] for item in labels]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.output_root).expanduser()

    def progress(message: str) -> None:
        print(f"pattern-lab-monthly: {message}", file=sys.stderr, flush=True)

    try:
        if not args.summarize_only:
            run_experiment(
                output_root=root,
                fixtures=_selected(args.fixtures),
                attempts=args.attempts,
                include_supplementary=not args.skip_supplementary,
                include_replays=not args.skip_replays,
                progress=progress,
            )
        summary = summarize(root)
    except (PatternLabDataError, OSError, ValueError) as error:
        print(json.dumps({"decision": "INCOMPLETE", "decision_reasons": [str(error)]}), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "method": CANDIDATE_METHOD_ID,
                "output_root": str(root),
                "decision": summary["decision"],
                "decision_reasons": summary["decision_reasons"],
                "required_fixtures_scored": [
                    item["name"] for item in summary["results"] if item["required"]
                ],
                "integration": (
                    "Research only: no production request, artifact or report exposes this "
                    "method, and no outcome here accepts M3a or starts M3b."
                ),
            },
            indent=2,
        )
    )
    return 0 if summary["decision"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
