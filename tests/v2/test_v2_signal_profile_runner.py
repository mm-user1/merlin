import copy

import numpy as np
import pytest

from core.engine_v2.contracts import Signals
from core.engine_v2.dataprep import build_signal_execution_data
from core.engine_v2.profile import active_mode_values, active_parameter_names, parse_execution_profile
from core.engine_v2.runner import build_signal_kernel_config, run_v2_strategy

from s03_like_test_helpers import fixture_config, normalized_params, synthetic_ohlc


def _minimal_data():
    df = synthetic_ohlc([100.0, 101.0, 102.0])
    long_entries = np.zeros(len(df), dtype=bool)
    short_entries = np.zeros(len(df), dtype=bool)
    long_entries[0] = True
    signals = Signals(
        long_entries=long_entries,
        short_entries=short_entries,
    )
    return build_signal_execution_data(df, signals=signals)


def _profile_from_execution_mutation(**updates):
    config = fixture_config()
    config["execution"].update(updates)
    return parse_execution_profile(config)


def test_signal_reversal_profile_variants_control_active_emergency_params():
    profile = parse_execution_profile(fixture_config())
    plain_params = normalized_params({"useEmergencySL": False, "emergencySlPct": 0.0, "emergencySlUpdateBars": 0})
    emergency_params = normalized_params({"useEmergencySL": True})

    assert active_mode_values(profile, plain_params)["topology"] == "signal_reversal"
    assert active_mode_values(profile, plain_params)["stop"] == "none"
    assert active_mode_values(profile, emergency_params)["stop"] == "emergency_pct"

    plain_active = active_parameter_names(profile, plain_params)
    emergency_active = active_parameter_names(profile, emergency_params)

    assert {"positionPct", "contractSize"} <= plain_active
    assert "emergencySlPct" not in plain_active
    assert "emergencySlUpdateBars" not in plain_active
    assert {"emergencySlPct", "emergencySlUpdateBars"} <= emergency_active


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("sizing", "risk_per_trade", "sizing"),
        ("target", "rr", "target"),
        ("trail", "ma", "trail"),
        ("maxDays", True, "maxDays"),
        ("margin", "report_only", "margin"),
        ("priceRounding", "tick_outward", "priceRounding"),
        ("exitOnSignal", False, "exitOnSignal"),
    ],
)
def test_signal_reversal_rejects_unsupported_mode_combinations(field, value, message):
    profile = _profile_from_execution_mutation(**{field: value})

    with pytest.raises(ValueError, match=message):
        build_signal_kernel_config(profile=profile, params=profile.parameter_defaults)


def test_unknown_topology_fails_in_runner_without_reaching_s06_path():
    profile = _profile_from_execution_mutation(topology="unknown")

    with pytest.raises(ValueError, match="Unsupported V2 execution topology"):
        run_v2_strategy(
            data=_minimal_data(),
            profile=profile,
            params=profile.parameter_defaults,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"emergencySlPct": 0.0}, "emergencySlPct"),
        ({"emergencySlUpdateBars": 0}, "emergencySlUpdateBars"),
    ],
)
def test_enabled_emergency_params_validate_at_runner_boundary(overrides, message):
    params = normalized_params({"useEmergencySL": True, **overrides})

    with pytest.raises(ValueError, match=message):
        run_v2_strategy(
            data=_minimal_data(),
            profile=parse_execution_profile(fixture_config()),
            params=params,
        )


def test_disabled_emergency_params_are_inert_for_runner_validation():
    params = normalized_params({"useEmergencySL": False, "emergencySlPct": 0.0, "emergencySlUpdateBars": 0})
    run = run_v2_strategy(
        data=_minimal_data(),
        profile=parse_execution_profile(fixture_config()),
        params=params,
    )

    assert run.strategy_result.total_trades == 1


def test_s06_profile_modes_stay_without_topology():
    config = fixture_config()
    del config["execution"]["topology"]
    config["execution"]["stop"] = "atr_swing"
    config["execution"]["sizing"] = "risk_per_trade"
    config["execution"]["target"] = "rr"
    config["execution"]["margin"] = "off"
    config["execution"]["maxDays"] = False
    config["execution"].pop("exitOnSignal")
    config["execution"].pop("variantSelector")
    config["execution"].pop("variants")
    parameters = copy.deepcopy(config["parameters"])
    parameters.update(
        {
            "stopX": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "stopLP": {"type": "int", "default": 2, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxPct": {"type": "float", "default": 6.0, "role": "execution", "optimize": {"enabled": False}},
            "stopRR": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "riskPerTrade": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
        }
    )
    config["parameters"] = parameters
    profile = parse_execution_profile(config)

    assert "topology" not in active_mode_values(profile, profile.parameter_defaults)
