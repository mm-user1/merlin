"""Synthetic rule preparation, replay, actual spawn and selected-run persistence."""
from copy import deepcopy
import multiprocessing as mp
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.data_input import prepare_input, replay_input, prepare_manual_input, DataInputError
from core.engine_v2.admission import read_admission_envelope, run_strategy
from tests.fixtures.admission_strategy.bootstrap import installed, publish, grid_payload
from tests.fixtures.admission_strategy.strategy import STRATEGY_ID, AdmissionTestStrategy
from tests._data_input_helpers import hold_pack


def test_rules_replay_semantics_and_current_manual(tmp_path):
    from data_provider import manifest
    source = publish(tmp_path / "pack")
    prepared = prepare_input(source, date_filter=False, admission_params={})
    assert prepared.entry_admission["maxEntryLeverage"] == 8
    pin = deepcopy(prepared.entry_admission)
    root = Path(source["pack_root"])
    document = manifest.read_manifest(root)
    rules = document["instruments"][0]["instrument_rules"]
    rules["source_reference"] = "new provenance"
    rules["as_of_utc"] = "2026-10-27T00:00:00Z"
    rules["price_tick"] = rules["raw_contract_fields"]["tickSize"] = "0.002"
    rules["quantity_step"] = rules["raw_contract_fields"]["qtyStep"] = "0.1000"
    manifest.write_manifest(root, document)
    replay = replay_input(prepared.facts, entry_admission=pin, admission_cap=8)
    assert replay.admission_rules == prepared.admission_rules
    import shutil
    relocated = tmp_path / "relocated"
    shutil.copytree(root, relocated)
    assert replay_input(prepared.facts, root_override=str(relocated), entry_admission=pin, admission_cap=8).admission_rules == prepared.admission_rules
    for invalid in ({**pin, "version": 2}, {**pin, "maxEntryLeverage": 7}, {**pin, "rules": {}}, None):
        with pytest.raises(ValueError):
            read_admission_envelope(invalid, 8)
    rules["quantity_step"] = rules["raw_contract_fields"]["qtyStep"] = "0.2"
    manifest.write_manifest(root, document)
    with pytest.raises(DataInputError, match="rules changed"):
        replay_input(prepared.facts, entry_admission=pin, admission_cap=8)
    manual = prepare_manual_input(prepared.facts, date_filter=False, admission_params={"maxEntryLeverage": 8})
    assert manual.admission_rules.base_step == "0.2"
    assert prepared.entry_admission == pin
    with pytest.raises(ValueError):
        read_admission_envelope(pin, None)


def test_actual_new_mode_spawn_while_pack_locked(tmp_path):
    from core.post_process import run_forward_test, run_perturbations_parallel, PostProcessConfig
    prepared = prepare_input(publish(tmp_path / "pack"), date_filter=False, admission_params={})
    params = grid_payload()["fixed_params"]
    context = mp.get_context("spawn")
    ready, release = context.Event(), context.Event()
    process = context.Process(target=hold_pack, args=(prepared.facts["source"]["pack_root"], ready, release))
    with installed():
        process.start()
        try:
            assert ready.wait(20)
            with pytest.raises(DataInputError, match="busy"):
                prepare_input(prepared.facts["source"], date_filter=False, admission_params={})
            candidates = [SimpleNamespace(params=params, optuna_trial_number=i, net_profit_pct=1,
                max_drawdown_pct=1, total_trades=1) for i in (1, 2)]
            results = run_forward_test(csv_path="", strategy_id=STRATEGY_ID, optuna_results=candidates,
                config=PostProcessConfig(enabled=True, top_k=2, warmup_bars=100),
                is_period_days=1, ft_period_days=3, ft_start_date="2024-01-02", ft_end_date="2024-01-05",
                n_workers=2, prepared_frame=prepared.frame, admission_rules=prepared.admission_rules)
            assert len(results) == 2
            assert all(r.entry_diagnostics["maxEntryLeverageUsed"] > 0 for r in results)
            perturbations = [dict(params={**params, "signalEvery": n}, perturbed_param="signalEvery",
                direction=d, base_value=12, perturbed_value=n) for n,d in ((10,-1),(14,1))]
            stress = run_perturbations_parallel("", STRATEGY_ID, perturbations, "2024-01-02", "2024-01-05", {},
                100, 2, prepared_frame=prepared.frame, admission_rules=prepared.admission_rules)
            assert len(stress) == 2
        finally:
            release.set()
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join()
    assert process.exitcode == 0


def test_grid_selected_diagnostics_and_saved_context(tmp_path):
    from core.optuna_engine import OptimizationConfig
    from core.grid_engine import run_grid_optimization
    from core.storage import load_study_from_db
    prepared = prepare_input(publish(tmp_path / "pack"), date_filter=False, admission_params={})
    with installed():
        config = OptimizationConfig(csv_file="", strategy_id=STRATEGY_ID, **grid_payload(),
            data_input=prepared.facts, admission_rules=prepared.admission_rules, entry_admission=prepared.entry_admission)
        results, study_id = run_grid_optimization(config, prepared_frame=prepared.frame)
        assert len(results) == 2
        saved = load_study_from_db(study_id)
        assert saved["study"]["config_json"]["entry_admission"] == prepared.entry_admission
        assert "admission_rules" not in saved["study"]["config_json"]
        for result, trial in zip(results, saved["trials"]):
            assert trial["entry_diagnostics"]["is"] == result.entry_diagnostics
            direct = run_strategy(AdmissionTestStrategy, prepared.frame, result.params, admission_rules=prepared.admission_rules)
            assert direct.entry_diagnostics["maxEntryLeverageUsed"] == result.entry_diagnostics["maxEntryLeverageUsed"]


def test_csv_tools_registry_and_all_error_failure(tmp_path, monkeypatch):
    import json
    import sys
    import strategies
    import run_backtest
    from core.optuna_engine import OptimizationConfig
    from core.grid_engine import run_grid_optimization
    from tools.strategy_lab.config import _validate_strategy, StrategyLabConfigError
    from tests.fixtures.admission_strategy.strategy import load_config
    assert STRATEGY_ID not in strategies._REGISTRY
    strategies._discover_strategies()
    assert STRATEGY_ID not in strategies._REGISTRY
    with installed():
        monkeypatch.setattr(run_backtest, "get_strategy", lambda _: AdmissionTestStrategy)
        monkeypatch.setattr(run_backtest, "load_data", lambda _: pytest.fail("CSV must not be opened"))
        monkeypatch.setattr(sys, "argv", ["run_backtest"])
        with pytest.raises(ValueError, match="CSV CLI"):
            run_backtest.main()
        spec = json.loads((Path(__file__).parents[1]/"tools/strategy_lab/runspecs/s06_bracket_mvp.json").read_text(encoding="utf-8"))
        identity = spec["generation"]["strategy"]
        identity.update(strategy_id=STRATEGY_ID, strategy_version=load_config()["version"])
        with pytest.raises(StrategyLabConfigError, match="CSV execution"):
            _validate_strategy(identity)
        config = OptimizationConfig(csv_file="never-open.csv", strategy_id=STRATEGY_ID, **grid_payload())
        with pytest.raises(ValueError, match="Parquet"):
            run_grid_optimization(config)
        prepared = prepare_input(publish(tmp_path/"pack", rows=100), date_filter=False, admission_params={})
        config.csv_file = ""
        config.fixed_params["riskPerTrade"] = 1e308
        config.data_input = prepared.facts
        config.admission_rules = prepared.admission_rules
        config.entry_admission = prepared.entry_admission
        from ui import server_services
        monkeypatch.setattr(server_services, "load_data", lambda _: pytest.fail("CSV export must not open a file"))
        for export in (server_services._run_trade_export, server_services._run_equity_export):
            outcome = export(strategy_id=STRATEGY_ID, csv_path="never-open.csv", params=config.fixed_params,
                warmup_bars=100, admission_rules=prepared.admission_rules)
            assert "Parquet" in outcome[-1]
        with pytest.raises(ValueError, match="(?i)(no.*(valid|usable)|all.*fail)"):
            run_grid_optimization(config, prepared_frame=prepared.frame)
    assert STRATEGY_ID not in strategies._REGISTRY
