"""Real process exclusion and RAM-only FT/stress worker transport."""
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import threading
from types import SimpleNamespace

import pytest

from core.data_input import DataInputError, prepare_input, pack_session, inspect_pack
from core.post_process import PostProcessConfig, run_forward_test, run_perturbations_parallel
from strategies import get_strategy_config
from tests._data_input_helpers import publish, hold_pack


def test_actual_workers_use_prepared_generation_while_pack_locked(tmp_path):
    source = publish(tmp_path / "pack", rows=1800)
    prepared = prepare_input(source, date_filter=False)
    strategy = "s03_reversal_v11_regime_er_b2"
    params = {k: v.get("default") for k, v in get_strategy_config(strategy)["parameters"].items() if k != "warmupBars"}
    params.update(dateFilter=True, start="2024-01-02", end="2024-01-05", useRegime=False, maLength3=10)
    ctx = mp.get_context("spawn")
    ready, release = ctx.Event(), ctx.Event()
    process = ctx.Process(target=hold_pack, args=(source["pack_root"], ready, release))
    process.start()
    try:
        assert ready.wait(20)
        with pytest.raises(DataInputError, match="busy in another process"):
            prepare_input(source, date_filter=False)
        candidates = [SimpleNamespace(params=params, optuna_trial_number=n, net_profit_pct=1.,
                                      max_drawdown_pct=1., total_trades=2) for n in (1, 2)]
        results = run_forward_test(csv_path="", strategy_id=strategy, optuna_results=candidates,
            config=PostProcessConfig(enabled=True, top_k=2, warmup_bars=100),
            is_period_days=1, ft_period_days=3, ft_start_date="2024-01-02", ft_end_date="2024-01-05",
            n_workers=2, prepared_frame=prepared.frame)
        assert len(results) == 2
        perturbations = [{"params": {**params, "maLength3": length}, "perturbed_param": "maLength3",
                          "direction": direction, "base_value": 10, "perturbed_value": length}
                         for length, direction in ((8, -1), (12, 1))]
        results = run_perturbations_parallel("", strategy, perturbations, "2024-01-02", "2024-01-05",
                                            {}, 100, 2, prepared_frame=prepared.frame)
        assert len(results) == 2
    finally:
        release.set()
        process.join(20)
        if process.is_alive():
            process.terminate()
            process.join()
    assert process.exitcode == 0
    assert inspect_pack(source["pack_root"])["manifest_present"]


def test_same_root_local_wait_cancellation_and_exception_release(tmp_path):
    source = publish(tmp_path / "pack")
    acquired = threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pack_session(source["pack_root"]):
            def read():
                with pack_session(source["pack_root"]):
                    acquired.set()
            future = pool.submit(read)
            assert not acquired.wait(.15)
            with pytest.raises(DataInputError, match="cancelled"):
                with pack_session(source["pack_root"], cancelled=lambda: True):
                    pytest.fail("cancelled session acquired")
        future.result(timeout=5)
    assert acquired.is_set()
    with pytest.raises(RuntimeError):
        with pack_session(source["pack_root"]):
            raise RuntimeError("intentional failure")
    assert inspect_pack(source["pack_root"])["manifest_present"]
