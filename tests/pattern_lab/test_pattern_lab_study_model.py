"""Fixed-horizon time math, directional values and the stored primitives.

The worked example follows the specification exactly: a 30m bar opening at
10:00 closes at 10:30, entry is the 10:30 open, the 1h outcome exits at 11:30
using the close of the 11:00 bar, and the path extremes come from the 10:30 and
11:00 bars only.
"""

from __future__ import annotations

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.study import builtins as study_builtins
from tools.pattern_lab.study.job import run_instrument_job

from ._helpers import (
    TWO_GREEN_EVERY_BAR,
    fixed_horizon_model,
    normalized_study,
    study_group_ms,
    study_job,
)

TIMEFRAME = 30
TEN_OCLOCK = 20  # ANCHOR_MS is UTC-day aligned, so group 20 opens at 10:00.

# group: open, high, low, close, quote volume
WORKED_SPECS = {
    18: (98.0, 99.0, 97.0, 98.5, 10.0),
    19: (98.5, 100.0, 98.0, 99.0, 11.0),
    20: (100.0, 130.0, 70.0, 101.0, 12.0),   # the signal bar; its extremes are excluded
    21: (102.0, 110.0, 95.0, 105.0, 13.0),   # entry bar, opens at 10:30
    22: (105.0, 120.0, 90.0, 108.0, 14.0),   # exit bar of the 1h horizon, opens at 11:00
    23: (108.0, 112.0, 104.0, 110.0, 15.0),
    24: (110.0, 115.0, 106.0, 112.0, 16.0),
}


def specs(groups) -> tuple:
    return tuple(WORKED_SPECS[group] for group in groups)


def primitives_for(end_group=25, *, drop_groups=(), horizons=(30, 60), start_group=19):
    _request, payload = study_job(
        specs(range(18, end_group)),
        start_group=start_group,
        end_group=end_group,
        warmup_group=18,
        drop_groups=drop_groups,
        models=[fixed_horizon_model(TIMEFRAME, list(horizons))],
    )
    return run_instrument_job(payload).tables["primitives"]


def row_for(primitives, group: int, horizon: int):
    selection = primitives.loc[
        (primitives["anchor_open_ms"] == study_group_ms(group))
        & (primitives["horizon_minutes"] == horizon)
    ]
    assert len(selection) == 1, (group, horizon, len(selection))
    return selection.iloc[0]


def test_the_worked_thirty_minute_one_hour_example():
    row = row_for(primitives_for(), TEN_OCLOCK, 60)
    assert int(row["signal_time_ms"]) == study_group_ms(21)      # the 10:00 bar closes at 10:30
    assert int(row["entry_time_ms"]) == study_group_ms(21)       # entry at 10:30
    assert int(row["exit_time_ms"]) == study_group_ms(23)        # exit at 11:30
    assert bool(row["return_valid"]) is True
    assert row["entry_price"] == pytest.approx(102.0)            # open of the 10:30 bar
    assert row["exit_price"] == pytest.approx(108.0)             # close of the 11:00 bar
    # The path is the 10:30 and 11:00 bars; the signal bar's 130/70 are excluded.
    assert row["path_high"] == pytest.approx(120.0)
    assert row["path_low"] == pytest.approx(90.0)


def test_a_one_bar_horizon_exits_at_the_entry_bars_own_close():
    row = row_for(primitives_for(), TEN_OCLOCK, 30)
    assert row["entry_price"] == pytest.approx(102.0)
    assert row["exit_price"] == pytest.approx(105.0)
    assert row["path_high"] == pytest.approx(110.0)
    assert row["path_low"] == pytest.approx(95.0)
    assert int(row["exit_time_ms"]) == study_group_ms(22)


def test_an_outcome_may_end_exactly_at_the_study_end_while_a_longer_one_is_censored():
    primitives = primitives_for(end_group=23)
    anchors = sorted({int(value) for value in primitives["anchor_open_ms"]})
    assert anchors[-1] == study_group_ms(21)  # the bar closing at the end is not an anchor

    short_horizon = row_for(primitives, 21, 30)
    assert int(short_horizon["exit_time_ms"]) == study_group_ms(23)
    assert bool(short_horizon["return_valid"]) is True
    assert short_horizon["exit_price"] == pytest.approx(108.0)

    long_horizon = row_for(primitives, 21, 60)
    assert bool(long_horizon["return_valid"]) is False
    assert long_horizon["return_reason"] == study_builtins.REASON_TERMINAL
    assert np.isnan(long_horizon["exit_price"])
    # A shorter horizon is never trimmed to the longest horizon's support.
    assert bool(short_horizon["path_valid"]) is True


def test_a_missing_entry_bar_and_an_internal_gap_have_distinct_reasons():
    without_entry = primitives_for(drop_groups=(21,))
    entry_row = row_for(without_entry, TEN_OCLOCK, 60)
    assert entry_row["return_reason"] == study_builtins.REASON_MISSING_ENTRY
    assert np.isnan(entry_row["entry_price"])

    with_gap = primitives_for(drop_groups=(22,))
    gap_row = row_for(with_gap, TEN_OCLOCK, 60)
    assert gap_row["return_reason"] == study_builtins.REASON_INCOMPLETE_PATH
    # The known entry price is retained; the unavailable exit and extremes are null.
    assert gap_row["entry_price"] == pytest.approx(102.0)
    assert np.isnan(gap_row["exit_price"])
    assert np.isnan(gap_row["path_high"])
    assert np.isnan(gap_row["path_low"])
    # The 30m horizon of the same anchor still has full support.
    assert bool(row_for(with_gap, TEN_OCLOCK, 30)["return_valid"]) is True


def test_no_row_beyond_the_study_end_is_ever_read_or_stored():
    primitives = primitives_for(end_group=23)
    end_ms = study_group_ms(23)
    valid = primitives.loc[primitives["return_valid"]]
    assert (valid["exit_time_ms"] <= end_ms).all()
    assert (primitives["signal_time_ms"] < end_ms).all()


def _mixed_request(timeframes, by_timeframe):
    from ._helpers import study_protocol, study_request

    return study_request(
        protocol=study_protocol(first_ms=study_group_ms(0, 120), coverage_end_ms=study_group_ms(200, 120)),
        start_ms=study_group_ms(2, 120),
        end_ms=study_group_ms(20, 120),
        warmup_ms=study_group_ms(0, 120),
        timeframes=list(timeframes),
        hypotheses=[TWO_GREEN_EVERY_BAR],
        models=[
            {
                "id": "mixed",
                "model": "fixed_horizon_path",
                "settings": {
                    "directions": ["long"],
                    "commission_pct_per_side": 0.05,
                    "by_timeframe": by_timeframe,
                },
            }
        ],
    )


def test_a_thirty_minute_one_hour_and_two_hour_two_hour_family_is_accepted():
    document = _mixed_request(
        [30, 120],
        {
            "30": {"horizons_minutes": [60], "primary_horizon_minutes": 60},
            "120": {"horizons_minutes": [120], "primary_horizon_minutes": 120},
        },
    )
    request = pack_study.normalize_request(document, source="test", base=None)
    assert [case.case_id for case in request.models[0].cases[30]] == ["tf30m.h60m.long"]
    assert [case.case_id for case in request.models[0].cases[120]] == ["tf120m.h120m.long"]


def test_a_horizon_that_is_not_a_multiple_of_its_own_timeframe_is_rejected():
    document = _mixed_request(
        [30, 120],
        {
            "30": {"horizons_minutes": [60], "primary_horizon_minutes": 60},
            # 60 is a multiple of the other selected timeframe, not of this one.
            "120": {"horizons_minutes": [60], "primary_horizon_minutes": 60},
        },
    )
    with pytest.raises(PatternLabDataError, match="own 120m observation timeframe"):
        pack_study.normalize_request(document, source="test", base=None)


def test_a_missing_or_extra_timeframe_entry_is_rejected():
    document = _mixed_request(
        [30, 120], {"30": {"horizons_minutes": [60], "primary_horizon_minutes": 60}}
    )
    with pytest.raises(PatternLabDataError, match="missing \\['120'\\]"):
        pack_study.normalize_request(document, source="test", base=None)


def test_a_primary_horizon_outside_its_own_list_is_rejected():
    document = _mixed_request(
        [120], {"120": {"horizons_minutes": [120, 240], "primary_horizon_minutes": 360}}
    )
    with pytest.raises(PatternLabDataError, match="primary_horizon_minutes"):
        pack_study.normalize_request(document, source="test", base=None)


# --------------------------------------------------------------------------
# directional values derived from the stored primitives
# --------------------------------------------------------------------------

def expanded(case_id: str, *, commission: float = 0.05, horizons=(30, 60)):
    request, payload = study_job(
        specs(range(18, 25)),
        start_group=19,
        end_group=25,
        warmup_group=18,
        models=[fixed_horizon_model(TIMEFRAME, list(horizons), commission_pct_per_side=commission)],
    )
    primitives = run_instrument_job(payload).tables["primitives"]
    case = next(item for item in request.models[0].cases[TIMEFRAME] if item.case_id == case_id)
    frame = study_builtins.expand_fixed_horizon_case(primitives, case)
    return frame.set_index("anchor_open_ms")


def test_long_and_short_values_use_their_own_formulas_and_share_the_commission():
    longs = expanded("tf30m.h60m.long").loc[study_group_ms(TEN_OCLOCK)]
    shorts = expanded("tf30m.h60m.short").loc[study_group_ms(TEN_OCLOCK)]
    entry, exit_price = 102.0, 108.0
    ratio = exit_price / entry
    commission = 0.0005 * (1.0 + ratio)

    assert longs["gross_return"] == pytest.approx(ratio - 1.0)
    assert shorts["gross_return"] == pytest.approx(-(ratio - 1.0))
    assert longs["commission_return"] == pytest.approx(commission)
    assert shorts["commission_return"] == pytest.approx(commission)
    assert longs["net_return"] == pytest.approx(ratio - 1.0 - commission)
    assert shorts["net_return"] == pytest.approx(-(ratio - 1.0) - commission)
    # Net returns are not sign mirrors, and the short side is not minus the long.
    assert shorts["net_return"] != pytest.approx(-longs["net_return"])


def test_excursions_are_nonnegative_fractions_of_entry_and_mirror_by_direction():
    longs = expanded("tf30m.h60m.long").loc[study_group_ms(TEN_OCLOCK)]
    shorts = expanded("tf30m.h60m.short").loc[study_group_ms(TEN_OCLOCK)]
    entry, high, low = 102.0, 120.0, 90.0

    assert longs["mfe"] == pytest.approx(high / entry - 1.0)
    assert longs["mae"] == pytest.approx(1.0 - low / entry)
    assert shorts["mfe"] == pytest.approx(1.0 - low / entry)
    assert shorts["mae"] == pytest.approx(high / entry - 1.0)


def test_an_unchanged_exit_price_still_pays_both_commissions():
    _request, payload = study_job(
        ((100.0, 101.0, 99.0, 100.0, 5.0),) * 6,
        start_group=1,
        end_group=6,
        models=[fixed_horizon_model(TIMEFRAME, [30])],
    )
    request, _ = study_job(
        ((100.0, 101.0, 99.0, 100.0, 5.0),) * 6,
        start_group=1,
        end_group=6,
        models=[fixed_horizon_model(TIMEFRAME, [30])],
    )
    primitives = run_instrument_job(payload).tables["primitives"]
    case = next(item for item in request.models[0].cases[TIMEFRAME] if item.case_id == "tf30m.h30m.long")
    frame = study_builtins.expand_fixed_horizon_case(primitives, case)
    valid = frame.loc[frame["return_valid"]]
    assert len(valid) > 0
    assert valid["gross_return"].to_numpy() == pytest.approx(0.0)
    assert valid["commission_return"].to_numpy() == pytest.approx(0.001)
    assert valid["net_return"].to_numpy() == pytest.approx(-0.001)


def test_a_non_flat_path_can_still_have_a_zero_excursion():
    # Entry 100, the path never rises above entry and never falls below it in turn.
    falling = (
        (100.0, 100.0, 100.0, 100.0, 5.0),   # g0 warmup
        (100.0, 100.0, 100.0, 100.0, 5.0),   # g1 anchor: entry is the next open
        (100.0, 100.0, 95.0, 97.0, 5.0),     # g2 entry bar: high == entry, low below
        (97.0, 99.0, 96.0, 98.0, 5.0),
    )
    request, payload = study_job(
        falling, start_group=1, end_group=4, models=[fixed_horizon_model(TIMEFRAME, [30])]
    )
    primitives = run_instrument_job(payload).tables["primitives"]
    case = next(item for item in request.models[0].cases[TIMEFRAME] if item.case_id == "tf30m.h30m.long")
    frame = study_builtins.expand_fixed_horizon_case(primitives, case).set_index("anchor_open_ms")
    row = frame.loc[study_group_ms(1)]
    assert row["path_high"] == pytest.approx(100.0)
    assert row["path_low"] == pytest.approx(95.0)
    assert row["mfe"] == pytest.approx(0.0)      # a non-flat path with zero MFE
    assert row["mae"] == pytest.approx(0.05)


def test_invalid_outcomes_are_null_rather_than_zero():
    request, payload = study_job(
        specs(range(18, 24)),
        start_group=19,
        end_group=24,
        warmup_group=18,
        drop_groups=(21,),
        models=[fixed_horizon_model(TIMEFRAME, [60])],
    )
    primitives = run_instrument_job(payload).tables["primitives"]
    case = next(item for item in request.models[0].cases[TIMEFRAME] if item.case_id == "tf30m.h60m.long")
    frame = study_builtins.expand_fixed_horizon_case(primitives, case).set_index("anchor_open_ms")
    row = frame.loc[study_group_ms(TEN_OCLOCK)]
    for name in ("gross_return", "commission_return", "net_return", "mfe", "mae"):
        assert np.isnan(row[name]), name


def test_primitives_are_direction_independent_and_keep_every_eligible_anchor():
    # Group 20 closes red, so its own and the next bar's conditions are false.
    mixed = dict(WORKED_SPECS)
    mixed[20] = (100.0, 130.0, 70.0, 99.0, 12.0)
    request, payload = study_job(
        tuple(mixed[group] for group in range(18, 25)),
        start_group=19,
        end_group=25,
        warmup_group=18,
        models=[fixed_horizon_model(TIMEFRAME, [30, 60])],
    )
    result = run_instrument_job(payload)
    primitives = result.tables["primitives"]
    conditions = result.tables["conditions"]
    emissions = result.tables["emissions"]

    anchors = sorted({int(value) for value in conditions["anchor_open_ms"]})
    # Two horizons, one physical row each, no duplicated long and short rows.
    assert len(primitives) == 2 * len(anchors)
    assert sorted({int(value) for value in primitives["anchor_open_ms"]}) == anchors
    assert len(emissions) < len(anchors)  # non-event anchors are retained too
    assert "direction" not in primitives.columns

    # Both directional cases reconstruct from the very same physical rows.
    cases = {case.case_id: case for case in request.models[0].cases[TIMEFRAME]}
    longs = study_builtins.expand_fixed_horizon_case(primitives, cases["tf30m.h60m.long"])
    shorts = study_builtins.expand_fixed_horizon_case(primitives, cases["tf30m.h60m.short"])
    assert len(longs) == len(shorts) == len(anchors)
    common = longs["return_valid"].to_numpy() & shorts["return_valid"].to_numpy()
    assert (
        longs["gross_return"].to_numpy()[common]
        == pytest.approx(-shorts["gross_return"].to_numpy()[common])
    )


def test_outcome_validity_is_separate_from_condition_validity():
    request, payload = study_job(
        specs(range(18, 24)),
        start_group=19,
        end_group=24,
        warmup_group=18,
        drop_groups=(22,),
        models=[fixed_horizon_model(TIMEFRAME, [60])],
    )
    result = run_instrument_job(payload)
    conditions = result.tables["conditions"].set_index("anchor_open_ms")
    primitives = result.tables["primitives"].set_index("anchor_open_ms")
    anchor = study_group_ms(TEN_OCLOCK)
    # The condition is known at this anchor while the 1h outcome is not available.
    assert bool(conditions.loc[anchor, "valid"]) is True
    assert bool(primitives.loc[anchor, "return_valid"]) is False
    assert bool(primitives.loc[anchor, "path_valid"]) is False
