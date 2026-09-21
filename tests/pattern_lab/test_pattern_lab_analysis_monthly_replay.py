from copy import deepcopy

import pytest

from tools.pattern_lab.analysis import calibration_monthly as monthly


def _outcome():
    return {"family_size": 2, "members": [
        {"member_id": name, "inference_available": True, "unavailable_reasons": [],
         "signal": 1., "control": 2., "lift": -1., "p_raw": .5, "p_holm": 1.,
         "candidate": {"standard_error": dict.fromkeys(("signal", "control", "lift"), 1.),
                       "intervals": {key: {"lower": -3., "upper": 3.}
                                     for key in ("signal", "control", "lift")}}}
        for name in ("a", "b")]}


@pytest.mark.parametrize("side", ["left", "right"])
@pytest.mark.parametrize("change", ["empty", "missing", "extra", "duplicate", "reordered", "size",
                                    "availability", "null", "nan", "inf", "interval"])
def test_replay_rejects_structural_and_numeric_mismatches(side, change):
    left = _outcome()
    right = deepcopy(left)
    altered = left if side == "left" else right
    members = altered["members"]
    if change == "empty": members.clear()
    elif change == "missing": members.pop()
    elif change == "extra": members.append(deepcopy(members[0]))
    elif change == "duplicate": members[1]["member_id"] = "a"
    elif change == "reordered": members.reverse()
    elif change == "size": altered["family_size"] = 1
    elif change == "availability": members[0]["inference_available"] = False
    elif change == "null": members[0]["p_raw"] = None
    elif change == "interval": members[0]["candidate"]["intervals"]["lift"] = None
    else: members[0]["lift"] = float(change)
    result = monthly._candidate_difference(left, right, expected_member_ids=["a", "b"])
    assert result["mismatched"]


def test_replay_retains_ordered_family_evidence_and_tolerance():
    left = _outcome()
    right = deepcopy(left)
    right["members"][0]["p_raw"] += 1e-14
    result = monthly._candidate_difference(left, right, expected_member_ids=["a", "b"])
    assert not result["mismatched"]
    assert 0 < result["max_absolute_difference"] < 1e-12
    assert result["left_member_ids"] == result["right_member_ids"] == ["a", "b"]
    assert result["expected_member_count"] == result["left_member_count"] == result["right_member_count"] == 2
