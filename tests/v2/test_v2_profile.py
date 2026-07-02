import json
from pathlib import Path

import pytest

from core.engine_v2.profile import (
    ProfileValidationError,
    active_mode_values,
    active_parameter_names,
    canonical_selector_key,
    inactive_parameter_names,
    is_v2_config,
    mode_binding_for,
    parse_execution_profile,
    resolve_variant,
    validate_parameter_roles,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _param(role, default, *, optimize=True, depends_on=None):
    payload = {
        "type": "float",
        "default": default,
        "role": role,
        "optimize": {"enabled": optimize},
    }
    if depends_on is not None:
        payload["depends_on"] = depends_on
    return payload


def _variant_config():
    return {
        "id": "generic_variant_fixture",
        "engine": "v2",
        "execution": {
            "entryOrder": "market_next_open",
            "stop": "atr_swing",
            "sizing": "risk_per_trade",
            "maxDays": True,
            "variantSelector": {
                "param": "selector",
                "mapping": {False: "mode_a", True: "mode_b"},
            },
            "variants": {
                "mode_a": {"target": "rr", "trail": "none"},
                "mode_b": {"target": "none", "trail": "ma"},
            },
        },
        "parameters": {
            "signalLen": _param("signal", 14),
            "selector": {"type": "bool", "default": False, "role": "execution", "optimize": {"enabled": True}},
            "stopX": _param("execution", 2.0),
            "stopLP": _param("execution", 2),
            "stopMaxPct": _param("execution", 6.0),
            "stopRR": _param("execution", 2.0),
            "trailRR": _param("execution", 1.0),
            "trailMAType": {"type": "select", "default": "SMA", "role": "execution", "optimize": {"enabled": True}},
            "trailMALength": _param("execution", 150),
            "trailMAOffsetEx": _param("execution", 0.0),
            "riskPerTrade": _param("execution", 2.0),
            "contractSize": _param("execution", 0.01),
            "stopMaxDays": _param("execution", 6),
            "unboundExec": _param("execution", 1.0),
            "runtimeOnly": _param("runtime", "2025-01-01", optimize=False),
        },
    }


def test_no_variants_creates_implicit_default_variant():
    config = {
        "id": "implicit_variant_fixture",
        "engine": "v2",
        "execution": {"entryOrder": "market_next_open", "target": "rr"},
        "parameters": {
            "signalLen": _param("signal", 5),
            "stopRR": _param("execution", 2.0),
        },
    }

    profile = parse_execution_profile(config)

    assert list(profile.variants) == ["default"]
    assert active_mode_values(profile, {})["target"] == "rr"
    assert active_parameter_names(profile, {}) == {"signalLen", "stopRR"}


def test_variant_selector_resolves_bool_mapping_keys_and_arbitrary_names():
    profile = parse_execution_profile(_variant_config())

    assert profile.variant_selector.mapping == {"false": "mode_a", "true": "mode_b"}
    assert resolve_variant(profile, {"selector": False}).name == "mode_a"
    assert resolve_variant(profile, {"selector": True}).name == "mode_b"


def test_canonical_selector_key_collapses_integral_numbers():
    assert canonical_selector_key(True) == "true"
    assert canonical_selector_key(False) == "false"
    assert canonical_selector_key(1.0) == "1"
    assert canonical_selector_key(1.25) == "1.25"


def test_numeric_mapping_keys_are_canonicalized():
    config = _variant_config()
    config["execution"]["variantSelector"] = {
        "param": "selector",
        "mapping": {1.0: "mode_a", 2: "mode_b"},
    }
    config["parameters"]["selector"] = _param("execution", 1.0)
    profile = parse_execution_profile(config)

    assert profile.variant_selector.mapping == {"1": "mode_a", "2": "mode_b"}
    assert resolve_variant(profile, {}).name == "mode_a"


def test_active_and_inactive_params_come_from_mode_bindings():
    profile = parse_execution_profile(_variant_config())

    mode_a_active = active_parameter_names(profile, {"selector": False})
    mode_a_inactive = inactive_parameter_names(profile, {"selector": False})
    mode_b_active = active_parameter_names(profile, {"selector": True})
    mode_b_inactive = inactive_parameter_names(profile, {"selector": True})

    assert "stopRR" in mode_a_active
    assert {"trailRR", "trailMAType", "trailMALength", "trailMAOffsetEx"} <= mode_a_inactive
    assert {"trailRR", "trailMAType", "trailMALength", "trailMAOffsetEx"} <= mode_b_active
    assert "stopRR" in mode_b_inactive
    assert "signalLen" in mode_a_active
    assert "runtimeOnly" not in mode_a_active


def test_binding_table_exposes_expected_phase_1_modes():
    assert mode_binding_for("target", "rr").consumes_params == ("stopRR",)
    assert mode_binding_for("target", "none").consumes_params == ()
    assert mode_binding_for("trail", "ma").consumes_params == (
        "trailRR",
        "trailMAType",
        "trailMALength",
        "trailMAOffsetEx",
    )
    assert mode_binding_for("trail", "none").consumes_params == ()


def test_unbound_execution_params_are_active_with_warning():
    profile = parse_execution_profile(_variant_config())

    assert "unboundExec" in active_parameter_names(profile, {"selector": False})
    assert any("unboundExec" in warning for warning in profile.validation_warnings)


def test_selector_missing_from_params_uses_config_default():
    profile = parse_execution_profile(_variant_config())

    assert resolve_variant(profile, {}).name == "mode_a"


def test_selector_missing_without_default_raises():
    config = _variant_config()
    del config["parameters"]["selector"]["default"]
    profile = parse_execution_profile(config)

    with pytest.raises(ProfileValidationError, match="selector parameter 'selector' missing"):
        resolve_variant(profile, {})


def test_optimized_parameter_without_role_fails_for_v2():
    config = _variant_config()
    del config["parameters"]["signalLen"]["role"]

    with pytest.raises(ProfileValidationError, match="signalLen"):
        parse_execution_profile(config)


def test_real_v1_config_does_not_trigger_v2_validation():
    with (REPO_ROOT / "src" / "strategies" / "s06_r_trend_v02" / "config.json").open(
        encoding="utf-8"
    ) as handle:
        config = json.load(handle)

    assert is_v2_config(config) is False
    validate_parameter_roles(config)


def test_cross_role_depends_on_fails_validation():
    config = _variant_config()
    config["parameters"]["stopX"]["depends_on"] = "signalLen"

    with pytest.raises(ProfileValidationError, match="cross-role depends_on"):
        parse_execution_profile(config)


def test_within_role_depends_on_is_accepted():
    config = _variant_config()
    config["parameters"]["trailRR"]["depends_on"] = "selector"

    parse_execution_profile(config)


def test_core_profile_modules_do_not_contain_strategy_specific_branches():
    forbidden = ("s06", "s06_r_trend_v02", "useTrailMA")
    for path in (REPO_ROOT / "src" / "core" / "engine_v2").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text
