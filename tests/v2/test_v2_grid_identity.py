from __future__ import annotations

import json
import os
from pathlib import Path

from core.grid_v2 import GRID_V2_ENGINE_VERSION, GridV2Settings, build_grid_v2_plan
from core.optuna_engine import OptimizationConfig

from strategies.s06_r_trend_v02_b2.strategy import load_config


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
TRADING_START = "2025-08-01T00:00:00+00:00"
TRADING_END = "2025-12-01T00:00:00+00:00"
GRID_PARAMS = (
    "stopX",
    "stopRR",
    "stopLP",
    "stopMaxPct",
    "stopMaxDays",
    "trailRR",
    "trailMAType",
    "trailMALength",
    "trailMAOffsetEx",
)


def _fast_grid():
    # The V1 fast-grid oracle is imported lazily with JIT disabled. Identity
    # mapping does not need compiled V1 execution, and V1 has separate
    # compiled-vs-interpreted tests. This setting is process-global, so this
    # helper is called only by V1-oracle tests.
    os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
    try:
        import numba

        numba.config.DISABLE_JIT = True
    except Exception:
        pass
    from strategies.s06_r_trend_v02 import fast_grid

    return fast_grid


def _v1_config() -> OptimizationConfig:
    return OptimizationConfig(
        csv_file=str(DATA_PATH),
        strategy_id="s06_r_trend_v02",
        enabled_params={name: True for name in GRID_PARAMS} | {"thresholdOS": False, "thresholdOB": False},
        param_ranges={},
        param_types={
            "thresholdOS": "int",
            "thresholdOB": "int",
            "stopX": "float",
            "stopRR": "float",
            "stopLP": "int",
            "stopMaxPct": "float",
            "stopMaxDays": "int",
            "trailRR": "float",
            "trailMAType": "select",
            "trailMALength": "int",
            "trailMAOffsetEx": "float",
        },
        fixed_params={
            "dateFilter": True,
            "start": TRADING_START,
            "end": TRADING_END,
            "entryMode": "Reversal @ Triangle",
            "enableLong": True,
            "enableShort": True,
            "fastLength": 21,
            "fastSmoothing": 7,
            "slowLength": 112,
            "slowSmoothing": 3,
            "thresholdOS": 20,
            "thresholdOB": 20,
            "stopX": 2.0,
            "stopRR": 3.0,
            "stopLP": 2,
            "stopMaxPct": 6.0,
            "stopMaxDays": 6,
            "riskPerTrade": 2.0,
            "contractSize": 0.01,
            "useTrailMA": True,
            "trailRR": 1.0,
            "trailMAType": "SMA",
            "trailMALength": 150,
            "trailMAOffsetEx": 0.0,
            "initialCapital": 100.0,
            "commissionPct": 0.05,
        },
        warmup_bars=1000,
        optimization_mode="grid",
        objectives=["net_profit_pct"],
        grid_enabled_modes=["bracket", "trail"],
        grid_budget=1,
    )


def _v2_base_params() -> dict:
    fixed = dict(_v1_config().fixed_params)
    fixed["fastSmooth"] = fixed.pop("fastSmoothing")
    fixed["slowSmooth"] = fixed.pop("slowSmoothing")
    return fixed


def _v1_candidates():
    fast_grid = _fast_grid()
    config = _v1_config()
    space = fast_grid.build_parameter_space(config)
    allocation = fast_grid.build_allocation(config, space, None)
    return fast_grid.generate_candidates(config, space, allocation, seed=123).candidates


def _canonical_from_v1(candidate) -> tuple:
    fast_grid = _fast_grid()
    return (
        candidate.mode,
        tuple((name, candidate.params[name]) for name in fast_grid.MODE_AXES[candidate.mode]),
    )


def _canonical_from_v2(candidate) -> tuple:
    fast_grid = _fast_grid()
    return (
        candidate.variant_name,
        tuple((name, candidate.params[name]) for name in fast_grid.MODE_AXES[candidate.variant_name]),
    )


def test_full_s06_default_identity_space_maps_one_to_one_with_v1_fast_grid():
    v1 = _v1_candidates()
    v2 = build_grid_v2_plan(load_config(), base_params=_v2_base_params())

    assert len(v1) == 48_480
    assert v2.deduped_candidate_count == 48_480
    assert v2.per_variant_counts == {"bracket": 480, "trail": 48_000}

    v1_keys = [_canonical_from_v1(candidate) for candidate in v1]
    v2_keys = [_canonical_from_v2(candidate) for candidate in v2.candidates]
    assert v2_keys == v1_keys
    assert len(set(v2_keys)) == 48_480

    sample_indices = [0, 1, 479, 480, 20_000, 48_479]
    assert [v2.candidates[index].candidate_id for index in sample_indices] == [
        index + 1 for index in sample_indices
    ]


def test_v2_semantic_keys_exclude_runtime_and_inactive_variant_params():
    plan = build_grid_v2_plan(load_config(), base_params=_v2_base_params())

    bracket_payload = json.loads(plan.candidates[0].semantic_key)
    assert "dateFilter" not in bracket_payload["params"]
    assert "start" not in bracket_payload["params"]
    assert "end" not in bracket_payload["params"]
    assert "trailMAType" not in bracket_payload["params"]
    assert "trailRR" not in bracket_payload["params"]
    assert "stopRR" in bracket_payload["params"]

    trail_payload = json.loads(plan.candidates[480].semantic_key)
    assert "stopRR" not in trail_payload["params"]
    assert "trailMAType" in trail_payload["params"]
    assert "trailRR" in trail_payload["params"]


def test_candidate_table_lazily_decodes_identity_subset_without_full_legacy_tuple():
    plan = build_grid_v2_plan(load_config(), base_params=_v2_base_params())
    table = plan.candidate_table

    assert plan._candidates_cache is None
    assert table.legacy_candidates_materialized_count == 0
    assert table.params_by_row is None
    assert table.params_materialized_count == 0
    assert table.semantic_keys_materialized_count == plan.deduped_candidate_count
    assert table.canonical_identities_materialized_count == 0

    subset = (0, 479, 480, 18_435, 48_479)
    for index in subset:
        candidate = plan.candidate_for_index(index)
        assert candidate.candidate_id == index + 1
        assert dict(table.params_for_index(index)) == dict(candidate.params)
        assert table.active_names_for_index(index) == candidate.active_param_names
        assert table.inactive_names_for_index(index) == candidate.inactive_param_names
        assert table.axis_names_for_index(index) == candidate.axis_param_names
        assert table.semantic_payload_for_index(index) == candidate.semantic_payload
        assert table.semantic_key_for_index(index) == candidate.semantic_key
        assert table.canonical_identity_for_index(index) == candidate.canonical_identity

    assert table.legacy_candidates_materialized_count == len(subset)
    assert table.params_materialized_count == len(subset)
    assert table.semantic_keys_materialized_count == plan.deduped_candidate_count
    assert table.canonical_identities_materialized_count == len(subset)
    assert plan._candidates_cache is None


def test_candidate_table_lazy_params_match_legacy_values_for_deterministic_rows():
    plan = build_grid_v2_plan(load_config(), base_params=_v2_base_params())
    table = plan.candidate_table
    assert table.params_by_row is None

    for candidate_id in (1, 480, 481, 18_436, 48_480):
        index = candidate_id - 1
        decoded = dict(table.params_for_index(index))
        candidate = plan.candidate_for_index(index)
        assert decoded == dict(candidate.params)
        assert candidate.candidate_id == candidate_id

    assert table.params_materialized_count == 5


def test_candidate_table_compatibility_candidates_materialize_on_explicit_access():
    plan = build_grid_v2_plan(load_config(), base_params=_v2_base_params())

    candidates = plan.candidates

    assert len(candidates) == 48_480
    assert plan._candidates_cache is candidates
    assert plan.candidate_table.legacy_candidates_materialized_count == 48_480
    assert candidates[0].candidate_id == 1
    assert candidates[-1].candidate_id == 48_480
    assert json.loads(candidates[0].semantic_key)["engine"] == GRID_V2_ENGINE_VERSION


def test_select_subset_helper_does_not_affect_identity_or_candidate_order():
    base_params = _v2_base_params()
    first = build_grid_v2_plan(
        load_config(),
        base_params={**base_params, "trailMAType_options": ["SMA", "HMA"]},
    )
    reordered = build_grid_v2_plan(
        load_config(),
        base_params={**base_params, "trailMAType_options": ["HMA", "SMA"]},
    )

    assert first.deduped_candidate_count == 24_480
    assert first.per_variant_counts == {"bracket": 480, "trail": 24_000}
    assert first.parameter_domains["trailMAType"].values == ("SMA", "HMA")
    assert reordered.parameter_domains["trailMAType"].values == ("SMA", "HMA")
    assert [candidate.canonical_identity for candidate in first.candidates] == [
        candidate.canonical_identity for candidate in reordered.candidates
    ]
    assert [candidate.semantic_key for candidate in first.candidates] == [
        candidate.semantic_key for candidate in reordered.candidates
    ]
    assert len({candidate.semantic_key for candidate in first.candidates}) == first.deduped_candidate_count
    assert all(
        not str(key).endswith("_options")
        for candidate in first.candidates
        for key in candidate.params
    )
    assert all(
        "trailMAType_options" not in json.loads(candidate.semantic_key)["params"]
        for candidate in first.candidates
    )


def _collapse_config():
    config = {
        "id": "collapse_fixture",
        "version": "test",
        "engine": "v2",
        "execution": {
            "entryOrder": "market_next_open",
            "stop": "atr_swing",
            "sizing": "risk_per_trade",
            "maxDays": True,
            "margin": "off",
            "boundary": "strict_close",
            "priceRounding": "none",
            "variantSelector": {
                "param": "selector",
                "mapping": {"false": "with_target", "true": "without_target"},
            },
            "variants": {
                "with_target": {"target": "rr", "trail": "none"},
                "without_target": {"target": "none", "trail": "none"},
            },
        },
        "parameters": {
            "selector": {"type": "bool", "default": False, "role": "execution", "optimize": {"enabled": False}},
            "signal": {"type": "int", "default": 1, "role": "signal", "optimize": {"enabled": False}},
            "stopRR": {
                "type": "float",
                "default": 1.0,
                "role": "execution",
                "optimize": {"enabled": True, "gridValues": [1.0, 2.0]},
            },
            "stopX": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "stopLP": {"type": "int", "default": 2, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxPct": {"type": "float", "default": 10.0, "role": "execution", "optimize": {"enabled": False}},
            "riskPerTrade": {"type": "float", "default": 2.0, "role": "execution", "optimize": {"enabled": False}},
            "contractSize": {"type": "float", "default": 0.01, "role": "execution", "optimize": {"enabled": False}},
            "stopMaxDays": {"type": "int", "default": 4, "role": "execution", "optimize": {"enabled": False}},
        },
    }
    return config


def test_inactive_axis_dedup_collapses_to_first_deterministic_candidate():
    plan = build_grid_v2_plan(
        _collapse_config(),
        GridV2Settings(include_inactive_axes_for_dedup=True),
    )

    assert plan.raw_candidate_count == 4
    assert plan.enumerated_candidate_count == 4
    assert plan.deduped_candidate_count == 3
    assert plan.per_variant_counts == {"with_target": 2, "without_target": 1}
    assert [candidate.candidate_id for candidate in plan.candidates] == [1, 2, 3]
    assert plan.candidates[-1].params["stopRR"] == 1.0
