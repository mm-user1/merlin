"""Actual spawned FT/Stress consume only explicit RAM permissions and frames."""
import multiprocessing as mp
from types import SimpleNamespace

import pytest

from core.data_input import prepare_input
from core.entry_filters import OFF, effective_config
from core.post_process import (run_forward_test, run_perturbations_parallel, PostProcessConfig,
                               _run_is_backtest, _initialize_prepared_worker)
from tests._data_input_helpers import hold_pack
from tests.fixtures.admission_strategy import filtered as strategy
from tests.fixtures.admission_strategy.bootstrap import installed, publish


@pytest.mark.parametrize("enabled", [False, True])
def test_spawn_with_locked_sources_and_negative_omission(tmp_path, enabled):
    source = publish(tmp_path / "pack", rows=3000, turn_day=6)
    filters = dict(version=1, enabled=enabled, items=[dict(id="one", type="candleDirection", version=1,
        enabled=True, parameters=dict(confirmationBars=2), source={**source, "timeframe_minutes": 60})])
    prepared = prepare_input(source, date_filter=True, start="2024-01-03", end="2024-01-10",
        warmup_bars=100, admission_params={}, entry_filters=effective_config(filters))
    params = strategy.normalized_params()
    ctx = mp.get_context("spawn")
    ready, release = ctx.Event(), ctx.Event()
    holder = ctx.Process(target=hold_pack, args=(source["pack_root"], ready, release))
    with installed(filtered=True):
        holder.start()
        try:
            assert ready.wait(20)
            candidates = [SimpleNamespace(params=params, optuna_trial_number=i, net_profit_pct=1,
                max_drawdown_pct=1, total_trades=1) for i in (1, 2)]
            options = dict(csv_path="", strategy_id=strategy.STRATEGY_ID, optuna_results=candidates,
                config=PostProcessConfig(enabled=True, top_k=2, warmup_bars=100), is_period_days=1,
                ft_period_days=3, ft_start_date="2024-01-07", ft_end_date="2024-01-10",
                n_workers=2, prepared_frame=prepared.frame, admission_rules=prepared.admission_rules)
            good = run_forward_test(**options, entry_filter_context=prepared.entry_filter_context)
            assert len(good) == 2 and all(r.ft_total_trades > 0 for r in good)
            # A new initializer clears any prior context; intended Off still needs transport.
            bad = run_forward_test(**options)
            assert bad == []
            direct = _run_is_backtest("", strategy.STRATEGY_ID, params, "2024-01-07", "2024-01-10", 100,
                prepared_frame=prepared.frame, admission_rules=prepared.admission_rules,
                entry_filter_context=prepared.entry_filter_context)
            assert direct is not None
            assert _run_is_backtest("", strategy.STRATEGY_ID, params, "2024-01-07", "2024-01-10", 100,
                prepared_frame=prepared.frame, admission_rules=prepared.admission_rules) is None
            perturbations = [dict(params={**params, "signalEvery": n}, perturbed_param="signalEvery",
                direction=d, base_value=12, perturbed_value=n) for n, d in ((10, -1), (14, 1))]
            result = run_perturbations_parallel("", strategy.STRATEGY_ID, perturbations, "2024-01-07", "2024-01-10", {},
                100, 2, prepared_frame=prepared.frame, admission_rules=prepared.admission_rules,
                entry_filter_context=prepared.entry_filter_context)
            assert len(result) == 2
        finally:
            release.set(); holder.join(20)
            if holder.is_alive():
                holder.terminate(); holder.join()
    assert holder.exitcode == 0
