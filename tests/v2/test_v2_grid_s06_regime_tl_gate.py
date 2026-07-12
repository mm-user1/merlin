"""Generic Grid V2 gate for the S06 Regime-TL pilot strategy.

Proves the pilot works through the unmodified generic Grid V2 path: plan
building, deterministic candidate identity, fixed-per-study `useRegime`,
opt-in regime numeric axes with cache identity, compiled-vs-reference subset
parity for both `useRegime` studies, and public-runner consistency for
selected candidates. No strategy-specific Fast Grid backend exists and no
core file knows this strategy's vocabulary.
"""

from __future__ import annotations

import math
import os
import re
from pathlib import Path

import pytest

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.engine_v2.runner import run_v2_strategy
from core.grid_v2 import (
    GridV2Settings,
    GridV2StrategyHooks,
    build_grid_v2_plan,
    deterministic_candidate_subset_indices,
    estimate_grid_v2_cache,
    execute_grid_v2_candidates,
)

from strategies.s06_r_trend_v02_regime_trendlines_b2 import strategy as regime_tl_strategy
from strategies.s06_r_trend_v02_regime_trendlines_b2.strategy import load_config

from s06_regime_tl_test_helpers import (
    MARKET_DATA_PATH,
    BASELINE_END,
    BASELINE_START,
    REFERENCE_A,
    REFERENCE_B,
    merged_reference_params,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPILED_SUBSET_LIMIT = 24
JIT_DISABLED = os.environ.get("NUMBA_DISABLE_JIT", "").strip().lower() in {"1", "true", "yes"}


@pytest.fixture(scope="module")
def prepared_data():
    if not MARKET_DATA_PATH.exists():
        pytest.skip(f"Reference data not found: {MARKET_DATA_PATH}")
    return prepare_dataset_with_warmup(
        load_data(str(MARKET_DATA_PATH)),
        BASELINE_START,
        BASELINE_END,
        1000,
    )


@pytest.fixture(scope="module")
def hooks():
    return GridV2StrategyHooks.from_strategy(regime_tl_strategy)


def test_default_plan_matches_s06_b2_axis_space_with_regime_params_fixed():
    plan = build_grid_v2_plan(load_config(), base_params=merged_reference_params(REFERENCE_B))

    assert plan.deduped_candidate_count == 48_480
    assert plan.per_variant_counts == {"bracket": 480, "trail": 48_000}
    default_axes = set(plan.metadata["default_enabled_axes"])
    assert not default_axes & {"useRegime", "regimePivotLen", "regimeSlopeFactor", "regimeBreakBufferX"}
    for name in ("useRegime", "regimePivotLen", "regimeSlopeFactor", "regimeBreakBufferX"):
        domain = plan.parameter_domains[name]
        assert domain.is_axis is False
        assert len(domain.values) == 1
    # useRegime is fixed per study through base params.
    assert plan.parameter_domains["useRegime"].values == (True,)


def test_use_regime_cannot_be_enabled_as_a_grid_axis():
    with pytest.raises(ValueError, match="useRegime"):
        build_grid_v2_plan(
            load_config(),
            GridV2Settings(enabled_axes=("stopX", "useRegime")),
            base_params=merged_reference_params(REFERENCE_B),
        )


def test_candidate_identity_is_deterministic_across_plan_builds():
    base_params = merged_reference_params(REFERENCE_B)
    first = build_grid_v2_plan(load_config(), base_params=base_params)
    second = build_grid_v2_plan(load_config(), base_params=base_params)

    assert first.deduped_candidate_count == second.deduped_candidate_count
    subset = (0, 1, 479, 480, 18_435, 48_479)
    for index in subset:
        first_candidate = first.candidate_for_index(index)
        second_candidate = second.candidate_for_index(index)
        assert first_candidate.candidate_id == index + 1
        assert first_candidate.candidate_id == second_candidate.candidate_id
        assert first_candidate.semantic_key == second_candidate.semantic_key
        assert dict(first_candidate.params) == dict(second_candidate.params)
    # Semantic payloads carry fixed regime params for regime studies.
    payload = first.candidate_table.semantic_payload_for_index(0)
    assert payload["params"]["useRegime"] is True
    assert payload["params"]["regimePivotLen"] == 15


def test_regime_numeric_axis_is_opt_in_and_extends_signal_cache_identity(prepared_data, hooks):
    df, trade_start_idx = prepared_data
    base_params = merged_reference_params(REFERENCE_B)

    fixed_plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_axes=()),
        base_params=base_params,
    )
    assert fixed_plan.deduped_candidate_count == 2  # one per variant
    fixed_estimate = estimate_grid_v2_cache(fixed_plan, df, trade_start_idx, hooks)
    assert fixed_estimate.signal_combo_count == 1

    axis_plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_axes=("regimePivotLen",)),
        base_params=base_params,
    )
    pivot_domain = axis_plan.parameter_domains["regimePivotLen"]
    assert pivot_domain.is_axis is True
    assert pivot_domain.values == (10, 15, 20)
    assert axis_plan.deduped_candidate_count == 6  # 3 pivot lengths x 2 variants
    axis_estimate = estimate_grid_v2_cache(axis_plan, df, trade_start_idx, hooks)
    # Every regime value owns a distinct signal cache identity: a stale cache
    # declaration would collapse this to 1 and silently reuse wrong signals.
    assert axis_estimate.signal_combo_count == 3
    # Dataprep identity is variant-aware (trail params are active only in the
    # trail variant), so 3 pivot lengths x 2 variants = 6 dataprep combos.
    assert axis_estimate.dataprep_combo_count == 6


def _assert_float_equal(actual, expected):
    actual = float(actual)
    expected = float(expected)
    if math.isnan(expected):
        assert math.isnan(actual)
    elif math.isinf(expected):
        assert math.isinf(actual) and (actual > 0.0) == (expected > 0.0)
    else:
        assert actual == pytest.approx(expected, rel=1e-9, abs=1e-12)


def _assert_rows_equal(compiled_row, reference_row):
    assert compiled_row.candidate_id == reference_row.candidate_id
    assert compiled_row.total_trades == reference_row.total_trades
    assert compiled_row.winning_trades == reference_row.winning_trades
    assert compiled_row.losing_trades == reference_row.losing_trades
    assert compiled_row.max_consecutive_losses == reference_row.max_consecutive_losses
    _assert_float_equal(compiled_row.net_profit_pct, reference_row.net_profit_pct)
    _assert_float_equal(compiled_row.max_drawdown_pct, reference_row.max_drawdown_pct)
    _assert_float_equal(compiled_row.romad, reference_row.romad)
    _assert_float_equal(compiled_row.profit_factor, reference_row.profit_factor)
    _assert_float_equal(compiled_row.win_rate_pct, reference_row.win_rate_pct)
    _assert_float_equal(compiled_row.gross_profit, reference_row.gross_profit)
    _assert_float_equal(compiled_row.gross_loss, reference_row.gross_loss)
    _assert_float_equal(compiled_row.final_balance, reference_row.final_balance)


@pytest.mark.skipif(JIT_DISABLED, reason="compiled Grid V2 parity requires Numba JIT")
@pytest.mark.parametrize("reference_id", [REFERENCE_A, REFERENCE_B])
def test_compiled_grid_v2_subset_matches_reference_backend(prepared_data, hooks, reference_id):
    if os.environ.get("NUMBA_DISABLE_JIT", "").strip().lower() in {"1", "true", "yes"}:
        # Known JIT process-isolation rule: an earlier V1-oracle test module
        # sets NUMBA_DISABLE_JIT process-globally. This certification test is
        # meaningless without JIT; run it in a fresh JIT-on process.
        pytest.skip("NUMBA_DISABLE_JIT poisoned process-globally; rerun in a fresh JIT-on process")
    df, trade_start_idx = prepared_data
    base_params = merged_reference_params(reference_id)
    config = load_config()
    compiled_plan = build_grid_v2_plan(
        config, GridV2Settings(prefer_compiled=True, top_n=6), base_params=base_params
    )
    reference_plan = build_grid_v2_plan(
        config, GridV2Settings(prefer_compiled=False, top_n=6), base_params=base_params
    )
    indices = deterministic_candidate_subset_indices(
        compiled_plan.deduped_candidate_count,
        COMPILED_SUBSET_LIMIT,
        required_indices=(0, 1, 479, 480, compiled_plan.deduped_candidate_count - 1),
    )

    compiled = execute_grid_v2_candidates(compiled_plan, df, trade_start_idx, hooks, indices)
    reference = execute_grid_v2_candidates(reference_plan, df, trade_start_idx, hooks, indices)

    assert compiled.metadata["backend_kind"] == "compiled_numba"
    assert compiled.metadata["compiled_batch_used"] is True
    assert reference.metadata["backend_kind"] == "reference"
    assert {row.variant_name for row in compiled.rows} == {"bracket", "trail"}
    assert len(compiled.rows) == len(reference.rows) == len(indices)
    for compiled_row, reference_row in zip(compiled.rows, reference.rows):
        _assert_rows_equal(compiled_row, reference_row)


def test_selected_candidates_match_public_v2_runner(prepared_data, hooks):
    """Grid rows must agree with the public run_v2_strategy path used for slow
    enrichment and WFA OOS — same data builder, same profile, same params."""

    df, trade_start_idx = prepared_data
    plan = build_grid_v2_plan(
        load_config(),
        GridV2Settings(enabled_axes=("stopX",), prefer_compiled=False),
        base_params=merged_reference_params(REFERENCE_B),
    )
    assert plan.deduped_candidate_count == 10  # 5 stopX values x 2 variants
    indices = tuple(range(plan.deduped_candidate_count))
    result = execute_grid_v2_candidates(plan, df, trade_start_idx, hooks, indices)

    for row in result.rows[:4]:
        candidate = plan.candidate_for_index(row.candidate_id - 1)
        params = hooks.normalize_params(dict(candidate.params))
        data = hooks.build_execution_data(df, params)
        run = run_v2_strategy(
            data=data, profile=plan.profile, params=params, trade_start_idx=trade_start_idx
        )
        direct = run.strategy_result
        assert row.total_trades == direct.total_trades
        assert row.winning_trades == direct.winning_trades
        _assert_float_equal(row.net_profit_pct, direct.net_profit_pct)
        _assert_float_equal(row.profit_factor, direct.profit_factor)
        _assert_float_equal(row.max_drawdown_pct, direct.max_drawdown_pct)


def test_generic_core_has_no_regime_tl_vocabulary():
    core_files = [
        REPO_ROOT / "src" / "core" / "grid_v2.py",
        REPO_ROOT / "src" / "core" / "grid_engine.py",
        *sorted((REPO_ROOT / "src" / "core" / "engine_v2").glob("*.py")),
    ]
    pattern = re.compile(
        r"s06_r_trend_v02_regime_trendlines|Regime-TL|useRegime|regimePivotLen|regimeSlopeFactor|regimeBreakBufferX"
    )
    for path in core_files:
        matches = [
            (number, line.strip())
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
            if pattern.search(line)
        ]
        assert not matches, f"{path.name}: {matches}"


def test_no_v1_fast_grid_backend_registered_for_regime_tl():
    from core.grid_engine import FAST_GRID_BACKENDS, supports_fast_grid, supports_grid_v2

    assert "s06_r_trend_v02_regime_trendlines_b2" not in FAST_GRID_BACKENDS
    assert supports_fast_grid("s06_r_trend_v02_regime_trendlines_b2") is False
    assert supports_grid_v2("s06_r_trend_v02_regime_trendlines_b2") is True
