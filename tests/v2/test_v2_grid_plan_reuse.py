from __future__ import annotations

import pytest

from core.grid_v2 import (
    GridV2PlanReuseCache,
    GridV2Settings,
    _pack_table_config_arrays,
    build_grid_v2_plan,
)


def _tiny_v2_config() -> dict:
    return {
        "id": "grid_reuse_fixture",
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
            "signalMode": {
                "type": "select",
                "default": "A",
                "options": ["A", "B"],
                "role": "signal",
                "optimize": {"enabled": True},
            },
            "threshold": {
                "type": "float",
                "default": 1.0,
                "gridValues": [1.0, 2.0],
                "role": "signal",
                "optimize": {"enabled": True},
            },
            "dateFilter": {
                "type": "bool",
                "default": False,
                "role": "runtime",
                "optimize": {"enabled": False},
            },
            "start": {
                "type": "select",
                "default": "",
                "options": [""],
                "role": "runtime",
                "optimize": {"enabled": False},
            },
            "end": {
                "type": "select",
                "default": "",
                "options": [""],
                "role": "runtime",
                "optimize": {"enabled": False},
            },
            "stopX": {
                "type": "float",
                "default": 2.0,
                "role": "execution",
                "optimize": {"enabled": False},
            },
            "riskPerTrade": {
                "type": "float",
                "default": 2.0,
                "role": "execution",
                "optimize": {"enabled": False},
            },
            "contractSize": {
                "type": "float",
                "default": 0.01,
                "role": "execution",
                "optimize": {"enabled": False},
            },
            "stopMaxDays": {
                "type": "int",
                "default": 4,
                "role": "execution",
                "optimize": {"enabled": False},
            },
        },
    }


def _window_params(start: str, end: str, **extra) -> dict:
    params = {
        "dateFilter": True,
        "start": start,
        "end": end,
        "stopX": 2.0,
    }
    params.update(extra)
    return params


def test_plan_reuse_rebases_runtime_dates_without_mutating_cached_table():
    cache = GridV2PlanReuseCache()
    settings = GridV2Settings(enabled_axes=("threshold",))

    first = cache.get_or_build(
        _tiny_v2_config(),
        settings=settings,
        base_params=_window_params("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z"),
    )
    first_table = first.plan.candidate_table
    first_params = first_table.params_for_index(0)

    second = cache.get_or_build(
        _tiny_v2_config(),
        settings=settings,
        base_params=_window_params("2025-02-01T00:00:00Z", "2025-03-01T00:00:00Z"),
    )
    second_table = second.plan.candidate_table
    second_params = second_table.params_for_index(0)

    assert first.hit is False
    assert second.hit is True
    assert second.stats.build_count == 1
    assert second.stats.hit_count == 1
    assert second.stats.miss_count == 1
    assert second.plan is not first.plan
    assert second_table is not first_table
    assert second_table.variant_codes is first_table.variant_codes
    assert second_table.axis_value_codes is first_table.axis_value_codes
    assert first_params["start"] == "2025-01-01T00:00:00Z"
    assert first_table.params_for_index(0)["start"] == "2025-01-01T00:00:00Z"
    assert second_params["start"] == "2025-02-01T00:00:00Z"
    assert second_params["end"] == "2025-03-01T00:00:00Z"
    assert second_params["dateFilter"] is True
    assert second.runtime_rebase_seconds >= 0.0
    assert second.plan_build_seconds == pytest.approx(0.0)


def test_plan_reuse_key_excludes_only_window_dates():
    cache = GridV2PlanReuseCache()
    settings = GridV2Settings(enabled_axes=("threshold",))

    cache.get_or_build(
        _tiny_v2_config(),
        settings=settings,
        base_params=_window_params("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z"),
    )
    date_only = cache.get_or_build(
        _tiny_v2_config(),
        settings=settings,
        base_params=_window_params("2025-02-01T00:00:00Z", "2025-03-01T00:00:00Z"),
    )
    non_date_change = cache.get_or_build(
        _tiny_v2_config(),
        settings=settings,
        base_params=_window_params(
            "2025-03-01T00:00:00Z",
            "2025-04-01T00:00:00Z",
            stopX=3.0,
        ),
    )

    assert date_only.hit is True
    assert non_date_change.hit is False
    assert non_date_change.stats.build_count == 2
    assert non_date_change.stats.hit_count == 1
    assert non_date_change.stats.miss_count == 2


def test_rebased_plan_matches_fresh_window_plan_for_params_and_packed_dates():
    cache = GridV2PlanReuseCache()
    settings = GridV2Settings(enabled_axes=("threshold",))
    config = _tiny_v2_config()
    cache.get_or_build(
        config,
        settings=settings,
        base_params=_window_params("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z"),
    )
    second_params = _window_params(
        "2025-02-01T00:00:00Z",
        "2025-03-01T00:00:00Z",
    )
    rebased = cache.get_or_build(config, settings=settings, base_params=second_params).plan
    fresh = build_grid_v2_plan(config, settings=settings, base_params=second_params)
    indices = (0, rebased.deduped_candidate_count - 1)

    assert rebased.deduped_candidate_count == fresh.deduped_candidate_count
    assert rebased.per_variant_counts == fresh.per_variant_counts
    for index in indices:
        assert rebased.candidate_table.params_for_index(index) == fresh.candidate_table.params_for_index(index)
        assert rebased.candidate_table.semantic_key_for_index(index) == fresh.candidate_table.semantic_key_for_index(index)
        assert rebased.candidate_table.canonical_identity_for_index(index) == fresh.candidate_table.canonical_identity_for_index(index)

    rebased_arrays = _pack_table_config_arrays(rebased, indices, trade_start_idx=0)
    fresh_arrays = _pack_table_config_arrays(fresh, indices, trade_start_idx=0)
    assert rebased_arrays["use_date_filter"].tolist() == fresh_arrays["use_date_filter"].tolist()
    assert rebased_arrays["start_ns"].tolist() == fresh_arrays["start_ns"].tolist()
    assert rebased_arrays["end_ns"].tolist() == fresh_arrays["end_ns"].tolist()
