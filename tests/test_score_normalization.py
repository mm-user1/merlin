"""
Unit tests for composite score normalization methods.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.optuna_engine import (  # noqa: E402
    DEFAULT_SCORE_CONFIG,
    OptimizationResult,
    SCORE_METRIC_ATTRS,
    calculate_score,
)


class TestMinMaxNormalization:
    """Test min-max normalization behavior."""

    def test_minmax_deterministic(self):
        """Min-max should be deterministic for same input."""
        result = OptimizationResult(
            params={},
            net_profit_pct=100.0,
            max_drawdown_pct=5.0,
            total_trades=100,
            romad=5.0,
            sharpe_ratio=2.0,
            profit_factor=3.0,
            ulcer_index=10.0,
            sqn=4.0,
            consistency_score=0.8,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {k: True for k in SCORE_METRIC_ATTRS}
        config["weights"] = {k: 1.0 for k in SCORE_METRIC_ATTRS}

        scores = []
        for _ in range(5):
            scored = calculate_score([result], config)
            scores.append(scored[0].score)

        assert all(s == scores[0] for s in scores)

    def test_minmax_clamping_above_max(self):
        """Values above max should be clamped to 100%."""
        result = OptimizationResult(
            params={},
            net_profit_pct=100.0,
            max_drawdown_pct=5.0,
            total_trades=100,
            romad=20.0,
            sharpe_ratio=None,
            profit_factor=None,
            ulcer_index=None,
            sqn=None,
            consistency_score=None,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {"romad": True}
        config["weights"] = {"romad": 1.0}

        scored = calculate_score([result], config)

        assert scored[0].score == pytest.approx(100.0, abs=0.01)

    def test_minmax_clamping_below_min(self):
        """Values below min should be clamped to 0%."""
        result = OptimizationResult(
            params={},
            net_profit_pct=-50.0,
            max_drawdown_pct=50.0,
            total_trades=100,
            romad=-5.0,
            sharpe_ratio=None,
            profit_factor=None,
            ulcer_index=None,
            sqn=None,
            consistency_score=None,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {"romad": True}
        config["weights"] = {"romad": 1.0}

        scored = calculate_score([result], config)

        assert scored[0].score == pytest.approx(0.0, abs=0.01)

    def test_minmax_inversion(self):
        """Inverted metrics should have inverted normalization."""
        result = OptimizationResult(
            params={},
            net_profit_pct=100.0,
            max_drawdown_pct=5.0,
            total_trades=100,
            romad=None,
            sharpe_ratio=None,
            profit_factor=None,
            ulcer_index=0.0,
            sqn=None,
            consistency_score=None,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {"ulcer": True}
        config["weights"] = {"ulcer": 1.0}
        config["invert_metrics"] = {"ulcer": True}

        scored = calculate_score([result], config)

        assert scored[0].score == pytest.approx(100.0, abs=0.01)

    def test_minmax_missing_values(self):
        """Missing metric values should default to 50%."""
        result = OptimizationResult(
            params={},
            net_profit_pct=100.0,
            max_drawdown_pct=5.0,
            total_trades=100,
            romad=None,
            sharpe_ratio=None,
            profit_factor=None,
            ulcer_index=None,
            sqn=None,
            consistency_score=None,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {k: True for k in SCORE_METRIC_ATTRS}
        config["weights"] = {k: 1.0 for k in SCORE_METRIC_ATTRS}

        scored = calculate_score([result], config)

        assert scored[0].score == pytest.approx(50.0, abs=0.01)


class TestPercentileNormalization:
    """Test percentile-based normalization (backward compatibility)."""

    def test_percentile_requires_multiple_results(self):
        """Percentile ranking needs multiple results for meaningful comparison."""
        results = [
            OptimizationResult(
                params={"id": i},
                net_profit_pct=float(i * 10),
                max_drawdown_pct=5.0,
                total_trades=100,
                romad=float(i),
            )
            for i in range(1, 11)
        ]

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "percentile"
        config["enabled_metrics"] = {"romad": True}
        config["weights"] = {"romad": 1.0}

        scored = calculate_score(results, config)

        assert scored[0].score < scored[-1].score


class TestScoreConsistency:
    """Test score consistency for minmax normalization."""

    def test_minmax_independent_of_other_results(self):
        """Minmax score should not change when other results are added."""
        result = OptimizationResult(
            params={},
            net_profit_pct=100.0,
            max_drawdown_pct=5.0,
            total_trades=100,
            romad=5.0,
            sharpe_ratio=1.5,
            profit_factor=2.0,
            ulcer_index=8.0,
            sqn=3.0,
            consistency_score=0.6,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {k: True for k in SCORE_METRIC_ATTRS}
        config["weights"] = {k: 1.0 for k in SCORE_METRIC_ATTRS}
        config["invert_metrics"] = {"ulcer": True}

        scored_alone = calculate_score([result], config)
        score_alone = scored_alone[0].score

        other_results = [
            OptimizationResult(
                params={"id": i},
                net_profit_pct=float(i * 10),
                max_drawdown_pct=float(i),
                total_trades=50 + i,
                romad=float(i),
                sharpe_ratio=float(i * 0.5),
                profit_factor=float(i * 0.3),
                ulcer_index=float(i * 2),
                sqn=float(i * 0.5),
                consistency_score=float(i / 100),
            )
            for i in range(1, 100)
        ]
        all_results = other_results + [result]
        scored_all = calculate_score(all_results, config)
        score_in_group = scored_all[-1].score

        assert score_alone == pytest.approx(score_in_group, abs=0.01)

    def test_legacy_consistency_bounds_are_migrated(self):
        """Old 0..100 consistency bounds should map to the new signed R² range."""
        result = OptimizationResult(
            params={},
            net_profit_pct=10.0,
            max_drawdown_pct=5.0,
            total_trades=30,
            consistency_score=0.5,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {"consistency": True}
        config["weights"] = {"consistency": 1.0}
        config["metric_bounds"] = {"consistency": {"min": 0.0, "max": 100.0}}

        scored = calculate_score([result], config)

        assert scored[0].score == pytest.approx(75.0, abs=0.01)


class TestCustomBounds:
    """Test custom metric bounds."""

    def test_custom_bounds_applied(self):
        """Custom bounds should affect normalization."""
        result = OptimizationResult(
            params={},
            net_profit_pct=100.0,
            max_drawdown_pct=5.0,
            total_trades=100,
            romad=5.0,
            sharpe_ratio=None,
            profit_factor=None,
            ulcer_index=None,
            sqn=None,
            consistency_score=None,
        )

        config = DEFAULT_SCORE_CONFIG.copy()
        config["normalization_method"] = "minmax"
        config["enabled_metrics"] = {"romad": True}
        config["weights"] = {"romad": 1.0}
        config["metric_bounds"] = {
            "romad": {"min": 0.0, "max": 5.0},
        }

        scored = calculate_score([result], config)

        assert scored[0].score == pytest.approx(100.0, abs=0.01)
