"""Shared leave-one-signal-month jackknife kernel; no estimator or research imports."""
from __future__ import annotations
from typing import Any, Mapping, Sequence
import numpy as np
from .. import PatternLabDataError
from .request import (
    V2_METHOD_ID as CANDIDATE_METHOD_ID, ALPHA,
    REASON_NO_SUPPORT, REASON_SPAN, REASON_ACTIVE_DAYS, REASON_BLOCKS,
    REASON_COVERAGE, REASON_HORIZON, REASON_DEGENERATE,
)

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
