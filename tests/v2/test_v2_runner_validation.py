import pytest

from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import build_kernel_config


TOPOLOGY_ERROR = (
    "Phase 1 supports exactly one exit topology: target=rr with no trailing mode "
    "or target=none with moving-average trailing mode and trailActivation=rr."
)


def _profile(*, target, trail, trail_activation=None, max_days=True):
    execution = {
        "entryOrder": "market_next_open",
        "stop": "atr_swing",
        "sizing": "risk_per_trade",
        "margin": "report_only",
        "boundary": "strict_close",
        "maxDays": max_days,
        "target": target,
        "trail": trail,
    }
    if trail_activation is not None:
        execution["trailActivation"] = trail_activation

    return parse_execution_profile(
        {
            "id": "generic_runner_fixture",
            "engine": "v2",
            "execution": execution,
            "parameters": {
                "stopX": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
                "stopLP": {"type": "int", "default": 2, "role": "execution", "optimize": {"enabled": False}},
                "stopMaxPct": {"type": "float", "default": 6.0, "role": "execution", "optimize": {"enabled": False}},
                "stopRR": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
                "trailRR": {"type": "float", "default": 1.0, "role": "execution", "optimize": {"enabled": False}},
                "riskPerTrade": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
                "contractSize": {"type": "float", "default": 0.01, "role": "execution", "optimize": {"enabled": False}},
                "stopMaxDays": {"type": "int", "default": 6, "role": "execution", "optimize": {"enabled": False}},
            },
        }
    )


def _build(profile):
    return build_kernel_config(profile=profile, params=profile.parameter_defaults)


def test_valid_bracket_topology_builds_kernel_config():
    config = _build(_profile(target="rr", trail="none"))

    assert config.target_mode == "rr"
    assert config.trail_mode == "none"
    assert config.trail_activation_mode == "none"
    assert config.max_days_enabled is True


def test_valid_trail_topology_builds_kernel_config():
    config = _build(_profile(target="none", trail="ma", trail_activation="rr"))

    assert config.target_mode == "none"
    assert config.trail_mode == "ma"
    assert config.trail_activation_mode == "rr"


@pytest.mark.parametrize(
    ("target", "trail", "trail_activation"),
    [
        ("rr", "ma", "rr"),
        ("none", "none", None),
        ("none", "ma", None),
        ("rr", "none", "rr"),
    ],
)
def test_unsupported_exit_topologies_raise(target, trail, trail_activation):
    with pytest.raises(ValueError, match=TOPOLOGY_ERROR):
        _build(_profile(target=target, trail=trail, trail_activation=trail_activation))


def test_unknown_trail_activation_raises():
    with pytest.raises(ValueError, match="Unsupported Phase-1 trailActivation mode"):
        _build(_profile(target="none", trail="ma", trail_activation="open"))


def test_unknown_max_days_mode_raises():
    with pytest.raises(ValueError, match="Unsupported Phase-1 execution mode maxDays"):
        _build(_profile(target="rr", trail="none", max_days="sometimes"))


def test_false_max_days_mode_is_accepted():
    config = _build(_profile(target="rr", trail="none", max_days=False))

    assert config.max_days_enabled is False
