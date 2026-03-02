import sys
import time
from collections import Counter
from pathlib import Path

import optuna

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.optuna_engine import (  # noqa: E402
    NSGAIIISampler,
    NSGAIISampler,
    OptimizationConfig,
    OptunaConfig,
    OptunaOptimizer,
    SamplerConfig,
    _analyze_coverage_requirements,
    _generate_coverage_trials,
)


def _base_config() -> OptimizationConfig:
    return OptimizationConfig(
        csv_file="dummy.csv",
        strategy_id="s01_trailing_ma",
        enabled_params={},
        param_ranges={},
        param_types={},
        fixed_params={},
    )


def _sample_space() -> dict:
    return {
        "maType": {"type": "categorical", "choices": ["EMA", "SMA", "HMA", "WMA"]},
        "trailType": {"type": "categorical", "choices": ["EMA", "SMA"]},
        "maLength": {"type": "int", "low": 10, "high": 50, "step": 10},
        "closeCountLong": {"type": "int", "low": 2, "high": 10, "step": 1},
        "stopX": {"type": "float", "low": 1.0, "high": 3.0, "step": 0.5},
    }


def test_generate_coverage_trials_is_deterministic():
    first = _generate_coverage_trials(_sample_space(), 16)
    second = _generate_coverage_trials(_sample_space(), 16)
    assert first == second


def test_generate_coverage_trials_full_block_coverage_and_primary_numeric_only():
    # Categorical product size is 4 * 2 = 8, so 16 gives exactly 2 full blocks.
    trials = _generate_coverage_trials(_sample_space(), 16)
    assert len(trials) == 16

    combos = [(trial["maType"], trial["trailType"]) for trial in trials]
    counts = Counter(combos)
    assert len(counts) == 8
    assert all(count == 2 for count in counts.values())

    # For A=2 anchors we expect 1/3 and 2/3 quantiles -> 20 and 40 for maLength.
    ma_length_values = {int(trial["maLength"]) for trial in trials}
    assert ma_length_values == {20, 40}

    # Other numeric parameters stay at midpoint anchors.
    assert {int(trial["closeCountLong"]) for trial in trials} == {6}
    assert {float(trial["stopX"]) for trial in trials} == {2.0}


def test_generate_coverage_trials_three_blocks_include_min_mid_max_for_primary_numeric():
    # 24 trials -> A=3 full blocks for C=8.
    trials = _generate_coverage_trials(_sample_space(), 24)
    assert len(trials) == 24
    assert {int(trial["maLength"]) for trial in trials} == {10, 30, 50}


def test_generate_coverage_trials_partial_block_is_still_deterministic():
    # 10 trials -> 1 full block (8) + 2 partial.
    trials = _generate_coverage_trials(_sample_space(), 10)
    assert len(trials) == 10
    assert len({(trial["maType"], trial["trailType"]) for trial in trials[:8]}) == 8
    assert trials == _generate_coverage_trials(_sample_space(), 10)


def test_analyze_coverage_requirements_uses_full_categorical_product():
    search_space = {
        "maType": {
            "type": "categorical",
            "choices": ["EMA", "SMA", "HMA", "WMA", "ALMA", "KAMA", "TMA", "T3", "DEMA", "VWMA", "VWAP"],
        },
        "trailMaType": {
            "type": "categorical",
            "choices": ["EMA", "SMA", "HMA", "WMA", "ALMA", "KAMA", "TMA", "T3", "DEMA", "VWMA", "VWAP"],
        },
        "maLength": {"type": "int", "low": 25, "high": 500, "step": 25},
    }

    report = _analyze_coverage_requirements(search_space)
    assert report["n_min"] == 121
    assert report["n_rec"] == 242
    assert report["coverage_block_size"] == 121
    assert report["main_axis_name"] == "maType"
    assert report["main_axis_options"] == 11
    assert report["primary_numeric_name"] == "maLength"


def test_optuna_optimizer_sets_tpe_startup_to_zero_in_coverage_mode():
    optuna_cfg = OptunaConfig(
        objectives=["net_profit_pct"],
        sampler_config=SamplerConfig(sampler_type="tpe", n_startup_trials=20),
        warmup_trials=20,
        coverage_mode=True,
    )
    optimizer = OptunaOptimizer(_base_config(), optuna_cfg)
    assert optimizer.sampler_config.n_startup_trials == 0


def test_optuna_summary_contains_coverage_warning_message():
    base_config = _base_config()
    optuna_cfg = OptunaConfig(
        objectives=["net_profit_pct"],
        sampler_config=SamplerConfig(sampler_type="tpe", n_startup_trials=20),
        warmup_trials=5,
        coverage_mode=True,
    )
    optimizer = OptunaOptimizer(base_config, optuna_cfg)
    optimizer.start_time = time.time() - 1
    optimizer.trial_results = []
    optimizer._coverage_report = {
        "n_min": 11,
        "n_rec": 22,
        "coverage_block_size": 11,
        "main_axis_name": "maType",
        "main_axis_options": 11,
        "primary_numeric_name": "maLength",
    }

    optimizer._finalize_results()
    summary = getattr(base_config, "optuna_summary", {})
    assert summary.get("initial_search_mode") == "coverage"
    assert summary.get("initial_search_trials") == 5
    assert summary.get("coverage_warning") == "Need more initial trials (min: 11, recommended: 22)"


def test_optuna_summary_no_coverage_warning_when_minimum_is_met():
    base_config = _base_config()
    optuna_cfg = OptunaConfig(
        objectives=["net_profit_pct"],
        sampler_config=SamplerConfig(sampler_type="tpe", n_startup_trials=11),
        warmup_trials=11,
        coverage_mode=True,
    )
    optimizer = OptunaOptimizer(base_config, optuna_cfg)
    optimizer.start_time = time.time() - 1
    optimizer.trial_results = []
    optimizer._coverage_report = {
        "n_min": 11,
        "n_rec": 22,
        "coverage_block_size": 11,
        "main_axis_name": "maType",
        "main_axis_options": 11,
        "primary_numeric_name": "maLength",
    }

    optimizer._finalize_results()
    summary = getattr(base_config, "optuna_summary", {})
    assert summary.get("coverage_warning") is None


def test_nsga2_coverage_marks_enqueued_trial_as_generation_zero():
    optuna_cfg = OptunaConfig(
        objectives=["net_profit_pct", "max_drawdown_pct"],
        sampler_config=SamplerConfig(sampler_type="nsga2", population_size=4),
        warmup_trials=1,
        coverage_mode=True,
    )
    optimizer = OptunaOptimizer(_base_config(), optuna_cfg)
    study = optuna.create_study(
        directions=["maximize", "minimize"],
        sampler=optimizer._create_sampler(),
    )
    study.enqueue_trial({"x": 3})

    def objective(trial):
        optimizer._mark_coverage_generation_for_nsga(trial)
        x = trial.suggest_int("x", 0, 10)
        return float(x), float(-x)

    study.optimize(objective, n_trials=1)
    attrs = study.trials[0].system_attrs
    assert attrs.get(NSGAIISampler._GENERATION_KEY) == 0


def test_nsga3_coverage_marks_enqueued_trial_as_generation_zero():
    optuna_cfg = OptunaConfig(
        objectives=["net_profit_pct", "max_drawdown_pct"],
        sampler_config=SamplerConfig(sampler_type="nsga3", population_size=4),
        warmup_trials=1,
        coverage_mode=True,
    )
    optimizer = OptunaOptimizer(_base_config(), optuna_cfg)
    study = optuna.create_study(
        directions=["maximize", "minimize"],
        sampler=optimizer._create_sampler(),
    )
    study.enqueue_trial({"x": 5})

    def objective(trial):
        optimizer._mark_coverage_generation_for_nsga(trial)
        x = trial.suggest_int("x", 0, 10)
        return float(x), float(-x)

    study.optimize(objective, n_trials=1)
    attrs = study.trials[0].system_attrs
    assert attrs.get(NSGAIIISampler._GENERATION_KEY) == 0


def test_nsga_coverage_marker_is_noop_when_coverage_mode_disabled():
    optuna_cfg = OptunaConfig(
        objectives=["net_profit_pct", "max_drawdown_pct"],
        sampler_config=SamplerConfig(sampler_type="nsga2", population_size=4),
        warmup_trials=1,
        coverage_mode=False,
    )
    optimizer = OptunaOptimizer(_base_config(), optuna_cfg)
    study = optuna.create_study(
        directions=["maximize", "minimize"],
        sampler=optimizer._create_sampler(),
    )
    study.enqueue_trial({"x": 1})

    def objective(trial):
        optimizer._mark_coverage_generation_for_nsga(trial)
        x = trial.suggest_int("x", 0, 10)
        return float(x), float(-x)

    study.optimize(objective, n_trials=1)
    attrs = study.trials[0].system_attrs
    assert attrs.get(NSGAIISampler._GENERATION_KEY) is None
