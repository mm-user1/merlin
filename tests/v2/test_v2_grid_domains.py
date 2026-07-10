from __future__ import annotations

import copy
import json

import pytest

from core.grid_v2 import GridV2Settings, build_grid_v2_plan, preview_grid_v2_counts
from core.engine_v2.profile import ProfileValidationError
from strategies.s06_r_trend_v02_b2.strategy import load_config


def _base_config():
    return {
        "id": "grid_fixture",
        "version": "test",
        "engine": "v2",
        "execution": {
            "entryOrder": "market_next_open",
            "stop": "atr_swing",
            "sizing": "risk_per_trade",
            "maxDays": True,
            "margin": "off",
            "boundary": "strict_close",
            "target": "rr",
            "trail": "none",
            "priceRounding": "none",
        },
        "parameters": {
            "selectSignal": {
                "type": "select",
                "default": "A",
                "options": ["A", "B"],
                "role": "signal",
                "optimize": {"enabled": True},
            },
            "gridSignal": {
                "type": "float",
                "default": 0.1,
                "gridValues": [0.1, 0.2],
                "role": "signal",
                "optimize": {"enabled": True, "min": 0.1, "max": 0.3, "step": 0.1},
            },
            "optionalInt": {
                "type": "int",
                "default": 2,
                "role": "signal",
                "optimize": {"enabled": True, "default_enabled": False, "min": 1, "max": 3, "step": 1},
            },
            "boolSignal": {
                "type": "bool",
                "default": True,
                "role": "signal",
                "optimize": {"enabled": True},
            },
            "runtimeStart": {
                "type": "int",
                "default": 0,
                "role": "runtime",
                "optimize": {"enabled": True, "min": 0, "max": 10, "step": 1},
            },
            "stopX": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "stopLP": {"type": "int", "default": 2, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxPct": {"type": "float", "default": 10.0, "role": "execution", "optimize": {"enabled": False}},
            "stopRR": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "riskPerTrade": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "contractSize": {"type": "float", "default": 0.01, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxDays": {"type": "int", "default": 4, "role": "execution", "optimize": {"enabled": False}},
        },
    }


def test_domain_sources_optional_axes_and_runtime_exclusion():
    config = _base_config()
    plan = build_grid_v2_plan(config)

    assert plan.parameter_domains["selectSignal"].values == ("A", "B")
    assert plan.parameter_domains["gridSignal"].values == (0.1, 0.2)
    assert plan.parameter_domains["boolSignal"].values == (False, True)
    assert plan.parameter_domains["optionalInt"].values == (2,)
    assert plan.parameter_domains["optionalInt"].is_axis is False
    assert plan.parameter_domains["runtimeStart"].values == (0,)
    assert plan.parameter_domains["runtimeStart"].is_axis is False

    overridden = build_grid_v2_plan(config, GridV2Settings(enabled_axes=("optionalInt",)))
    assert overridden.parameter_domains["optionalInt"].values == (1, 2, 3)
    assert overridden.parameter_domains["selectSignal"].values == ("A",)
    assert overridden.deduped_candidate_count == 3

    with pytest.raises(ValueError, match="not an optimized non-runtime"):
        build_grid_v2_plan(config, GridV2Settings(enabled_axes=("runtimeStart",)))


def test_invalid_numeric_ranges_fail_clearly():
    config = _base_config()
    bad = copy.deepcopy(config)
    bad["parameters"]["broken"] = {
        "type": "float",
        "default": 0.0,
        "role": "signal",
        "optimize": {"enabled": True, "min": 0.0, "max": 1.0},
    }
    with pytest.raises(ValueError, match="missing"):
        build_grid_v2_plan(bad)

    bad = copy.deepcopy(config)
    bad["parameters"]["broken"] = {
        "type": "int",
        "default": 1,
        "role": "signal",
        "optimize": {"enabled": True, "min": 1, "max": 3, "step": 0},
    }
    with pytest.raises(ValueError, match="positive step"):
        build_grid_v2_plan(bad)


def test_cross_role_depends_on_still_rejected():
    config = _base_config()
    config["parameters"]["selectSignal"]["depends_on"] = "stopX"
    with pytest.raises(ProfileValidationError, match="cross-role"):
        build_grid_v2_plan(config)


def test_semantic_keys_are_stable_json_with_canonical_float_values():
    first = build_grid_v2_plan(_base_config())
    second = build_grid_v2_plan(_base_config())

    assert [candidate.semantic_key for candidate in first.candidates] == [
        candidate.semantic_key for candidate in second.candidates
    ]
    payload = json.loads(first.candidates[0].semantic_key)
    assert payload["params"]["gridSignal"] == 0.1


def test_s06_default_and_threshold_enabled_breadth_counts():
    config = load_config()
    plan = build_grid_v2_plan(config)

    assert plan.deduped_candidate_count == 48_480
    assert plan.per_variant_counts == {"bracket": 480, "trail": 48_000}
    assert plan.parameter_domains["thresholdOS"].is_axis is False
    assert plan.parameter_domains["thresholdOB"].is_axis is False
    assert plan.parameter_domains["useTrailMA"].is_axis is False
    assert plan.candidates[0].params["useTrailMA"] is False
    assert plan.candidates[480].params["useTrailMA"] is True

    expanded = preview_grid_v2_counts(
        config,
        GridV2Settings(
            enabled_axes=(
                "thresholdOS",
                "thresholdOB",
                "stopX",
                "stopRR",
                "stopLP",
                "stopMaxPct",
                "stopMaxDays",
                "trailMAType",
                "trailMALength",
                "trailMAOffsetEx",
                "trailRR",
            )
        ),
    )
    assert expanded.deduped_candidate_count == 436_320
    assert expanded.per_variant_counts == {"bracket": 4_320, "trail": 432_000}


def test_s06_b2_select_axis_runtime_options_subset_counts():
    config = load_config()

    all_types = build_grid_v2_plan(
        config,
        base_params={"trailMAType_options": ["SMA", "HMA", "KAMA", "T3"]},
    )
    assert all_types.deduped_candidate_count == 48_480
    assert all_types.per_variant_counts == {"bracket": 480, "trail": 48_000}

    one_type = build_grid_v2_plan(config, base_params={"trailMAType_options": ["SMA"]})
    assert one_type.deduped_candidate_count == 12_480
    assert one_type.per_variant_counts == {"bracket": 480, "trail": 12_000}
    assert one_type.parameter_domains["trailMAType"].values == ("SMA",)

    two_types = preview_grid_v2_counts(
        config,
        base_params={"trailMAType_options": ["SMA", "HMA"]},
    )
    assert two_types.deduped_candidate_count == 24_480
    assert two_types.per_variant_counts == {"bracket": 480, "trail": 24_000}

    with pytest.raises(ValueError, match="unknown option"):
        build_grid_v2_plan(config, base_params={"trailMAType_options": ["SMA", "BAD"]})


def test_s06_b2_trailing_parameter_order_matches_v1_axis_order():
    parameter_names = list(load_config()["parameters"])
    assert parameter_names.index("trailMAType") < parameter_names.index("trailMALength")
    assert parameter_names.index("trailMALength") < parameter_names.index("trailMAOffsetEx")
    assert parameter_names.index("trailMAOffsetEx") < parameter_names.index("trailRR")
