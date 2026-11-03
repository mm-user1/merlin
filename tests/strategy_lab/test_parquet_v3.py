"""Real provider -> Lab -> compiled/reference -> offline analysis certification."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import shutil

import numpy as np
import pytest

from tools.strategy_lab.config import REPO_ROOT, load_run_spec
from tools.strategy_lab import config_v2, parquet
from tools.strategy_lab.dataset import METRIC_AXIS_V3, load_json
from tools.strategy_lab.generate import generate_dataset
from tools.strategy_lab.synthetic_parquet import publish, synthetic_rules
from tools.strategy_lab.analysis.dataset import open_dataset
from tools.strategy_lab.analysis.evaluate import evaluate_scope
from tools.strategy_lab.analysis.allocation import DatasetInput, evaluate_allocation
from tools.strategy_lab.analysis.output import write_analysis, write_allocation


def request(tmp_path, *, full=False, two=False, months=1, **base):
    raw = json.loads((REPO_ROOT / "tools/strategy_lab/runspecs/s07_off_example.json").read_text())
    if not full:
        raw["generation"]["planning"].update(enabled_axes=["entryRunLength"],
            axis_values={"entryRunLength": list(range(2, 9))}, expected_candidate_count=7)
    if two:
        raw["generation"]["market_data"]["instruments"].append({"instrument_id": "BYBIT_BBBUSDT", "cell": "research"})
    if months != 1:
        raw["generation"]["windows"].update(oos_period_months=months, requested_end="2024-09-01")
    raw["generation"]["economics"]["base_params"].update(base)
    path = tmp_path / "request.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def run(path, root, output, **kwargs):
    return generate_dataset(path, data_root=root, output_dir=output, **kwargs)


def snapshot(path):
    return {p.relative_to(path).as_posix(): p.read_bytes() for p in path.rglob("*") if p.is_file()}


def test_offline_preflight_and_short_calendar(tmp_path, monkeypatch):
    path = request(tmp_path)
    with monkeypatch.context() as m:
        m.setattr(parquet.data_input, "prepare_input", lambda *a, **k: pytest.fail("offline pack read"))
        m.setattr(config_v2, "build_grid_v2_plan", lambda *a, **k: pytest.fail("offline rule-free plan"))
        spec = load_run_spec(path)
        assert spec.plan is None
        assert [w.window_id for w in config_v2.calendar(spec.generation)] == [1, 2, 3]
    publish(tmp_path / "pack")
    with monkeypatch.context() as m:
        m.setattr(parquet, "execute_grid_v2_candidates", lambda *a, **k: pytest.fail("preflight execution"))
        result = run(path, tmp_path / "pack", tmp_path / "out", window_selectors=[2], preflight_only=True)
    assert result.manifest_path.name == "manifest.partial.json"
    assert not (tmp_path / "out/manifest.json").exists()
    binding = load_json(tmp_path / "out/input_bindings.json", "bindings")["instruments"][0]
    assert {s["window_id"] for s in binding["segments"]} == {2}


@pytest.mark.parametrize("mutation", [
    lambda r: r["generation"]["market_data"].update(timeframe_minutes=True),
    lambda r: r["generation"]["market_data"].update(timeframe_minutes=35),
    lambda r: r["generation"]["market_data"].update(timeframe_minutes=420),
    lambda r: r["generation"]["market_data"]["instruments"].append(r["generation"]["market_data"]["instruments"][0]),
    lambda r: r["generation"]["market_data"]["instruments"][0].update(instrument_id="AAAUSDT"),
    lambda r: r["generation"]["market_data"]["instruments"][0].update(instrument_id="BYBIT_AAA."),
    lambda r: r["generation"]["execution"].update(entry_filters=[{}]),
    lambda r: r["generation"]["execution"].update(policy="legacy_manual"),
    lambda r: r["generation"]["economics"]["base_params"].update(contractSize=.01),
    lambda r: r["generation"]["economics"]["base_params"].update(initialCapital=True),
    lambda r: r["generation"]["planning"].update(enabled_axes=["maxEntryLeverage"], axis_values={"maxEntryLeverage":[8]}),
    lambda r: r["preregistration"]["analysis_scopes"][0].update(window_numbers=[2, 1]),
    lambda r: r["preregistration"]["analysis_scopes"][0].update(window_numbers=[1, 1]),
    lambda r: r["preregistration"]["analysis_scopes"][0].update(window_numbers=[True]),
    lambda r: r["preregistration"]["analysis_scopes"][0].update(window_numbers=[4]),
    lambda r: r["preregistration"]["analysis_scopes"][0].update(ticker_cell="missing"),
    lambda r: r["preregistration"]["analysis_scopes"].append(r["preregistration"]["analysis_scopes"][0]),
    lambda r: r["preregistration"].update(seed=True),
])
def test_invalid_contract(tmp_path, mutation):
    path = request(tmp_path)
    raw = json.loads(path.read_text())
    mutation(raw)
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_run_spec(path)


@pytest.mark.slow
def test_full_enumeration_reference_and_offline(tmp_path):
    path = request(tmp_path, full=True)
    root, out = tmp_path / "pack", tmp_path / "out"
    publish(root)
    result = run(path, root, out, window_selectors=[1])
    assert result.completed_groups == 1
    saved = snapshot(out)
    assert run(path, root, out, window_selectors=[1]).no_op
    assert snapshot(out) == saved
    spec = load_run_spec(path)
    binding = load_json(out / "input_bindings.json", "bindings")["instruments"][0]
    prepared = parquet.replay(binding, root)
    plan = config_v2.build_plan(spec, prepared.admission_rules)
    assert plan.deduped_candidate_count == 840
    # Predeclared mixed-radix corners cover all five axes; both sides remain enabled.
    indices = [0, 1, 5, 23, 119, 419, 839]
    from core.grid_v2 import GridV2StrategyHooks, execute_grid_v2_candidates
    from strategies.s07_ha_v01_b2 import strategy
    hooks = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=prepared.entry_filter_context)
    reference = replace(plan, settings=replace(plan.settings, prefer_compiled=False))
    window = config_v2.calendar(spec.generation)[0]
    matrix = np.load(out / "groups/BYBIT_AAAUSDT/window_01.npy")
    assert matrix.shape == (840, 2, 22)
    assert np.isfinite(matrix[:, :, -2]).all()
    from tools.strategy_lab.dataset import result_row_values
    for si, name in enumerate(("is", "oos")):
        part = parquet.segment(prepared, binding, window, name, spec)
        oracle = execute_grid_v2_candidates(reference, part.dataframe, part.trade_start_idx, hooks,
            candidate_indices=indices, compute_sharpe=False, compute_sharpe_daily=True, compute_sqn=True, compute_max_drawdown_mtm=True)
        for index, row in zip(indices, oracle.rows):
            assert row.backend_kind != "compiled_numba" and row.total_trades > 0
            expected = np.append(result_row_values(row), row.guardrail_summary["margin_reject_count"])
            np.testing.assert_allclose(matrix[index, si], expected, rtol=1e-9, atol=1e-8, equal_nan=True)
            for metric in ("total_trades", "winning_trades", "losing_trades", "flags", "margin_reject_count"):
                mi = METRIC_AXIS_V3.index(metric)
                assert matrix[index, si, mi] == expected[mi]
    # A Windows original locator is inert even on Linux; current root reconstructs the descriptor.
    binding["original_root"] = "C:\\old\\pack"
    relocated = tmp_path / "relocated"
    shutil.copytree(root, relocated)
    np.testing.assert_array_equal(parquet.replay(binding, relocated).frame, prepared.frame)
    hidden = tmp_path / "hidden"
    root.rename(hidden)
    dataset = open_dataset(out)
    with pytest.raises(ValueError, match="missing actual window"):
        dataset.resolve_scope("research")
    scope = dataset.resolve_scope("research", allow_partial=True)
    analysis = evaluate_scope(dataset, scope)
    assert analysis.summary["schema_version"] == "strategy_lab_analysis_v2"
    assert analysis.summary["capabilities"]["evidence"]["status"] == "not_requested"
    write_analysis(analysis, tmp_path / "analysis", dataset_root=out)
    allocation = evaluate_allocation([DatasetInput("s07", dataset, scope)], candidate_rule="primary_profit", primary_k=6, sensitivity_k=8)
    assert set(allocation.summary["datasets"]["s07"]["variants"]) == {"primary", "sensitivity"}
    write_allocation(allocation, tmp_path / "allocation", dataset_roots=[out])
    assert len(list((tmp_path / "analysis").iterdir())) == 5
    assert len(list((tmp_path / "allocation").iterdir())) == 6


@pytest.mark.slow
def test_two_instruments_resume_and_consumption_policy(tmp_path, monkeypatch):
    path = request(tmp_path, two=True)
    root, fresh, partial = (tmp_path / name for name in ("pack", "fresh", "partial"))
    publish(root, symbols=("AAA", "BBB"), rules={"BBB": synthetic_rules("BBB", step=".1", minimum=".2")})
    run(path, root, fresh)
    def interrupt(count, _path):
        if count == 3:
            raise RuntimeError("intentional interruption")
    with pytest.raises(RuntimeError, match="intentional"):
        run(path, root, partial, _after_group=interrupt)
    bindings = load_json(partial / "input_bindings.json", "bindings")["instruments"]
    assert bindings[0]["plan_fingerprint"] != bindings[1]["plan_fingerprint"]
    assert bindings[0]["semantic_key_digest"] == bindings[1]["semantic_key_digest"]
    actual_replay = parquet.replay
    calls = []
    def observed(binding, root):
        calls.append(binding["instrument_id"])
        assert binding["instrument_id"] == "BYBIT_BBBUSDT", "completed instrument must not reread"
        return actual_replay(binding, root)
    with monkeypatch.context() as m:
        m.setattr(parquet, "replay", observed)
        result = run(path, root, partial, resume=True)
    assert calls == ["BYBIT_BBBUSDT"]  # Also proves there is no final all-source pass.
    assert result.reused_groups == 3 and result.regenerated_groups == 3
    for name in ("normalized_runspec.json", "candidates.json", "input_bindings.json", "data_quality.csv"):
        assert (fresh / name).read_bytes() == (partial / name).read_bytes()
    fm, pm = (load_json(p / "manifest.json", "manifest") for p in (fresh, partial))
    assert fm["identity"] == pm["identity"] and fm["groups"] == pm["groups"]
    # All groups are reusable historical evidence, even if the source subsequently changes.
    finished = tmp_path / "all-published"
    def after_last(count, _path):
        if count == 6:
            raise RuntimeError("all published")
    with pytest.raises(RuntimeError):
        run(path, root, finished, _after_group=after_last)
    with monkeypatch.context() as m:
        m.setattr(parquet, "replay", lambda *a, **k: pytest.fail("finalization must be offline"))
        assert run(path, root, finished, resume=True).reused_groups == 6


@pytest.mark.parametrize("scenario", ["cap", "minimum_quantity", "minimum_notional", "no_signal"])
@pytest.mark.slow
def test_refusal_and_no_fill_reference(tmp_path, scenario):
    base = {"riskPerTrade": 100., "maxEntryLeverage": 1.} if scenario == "cap" else {}
    if scenario == "no_signal":
        base.update(enableLong=False, enableShort=False)
    path = request(tmp_path, **base)
    rules = synthetic_rules(minimum="100000000" if scenario == "minimum_quantity" else ".001",
                            notional="100000000" if scenario == "minimum_notional" else "0")
    root, out = tmp_path / "pack", tmp_path / "out"
    publish(root, rules={"AAA": rules})
    run(path, root, out, window_selectors=[1])
    spec = load_run_spec(path)
    binding = load_json(out / "input_bindings.json", "bindings")["instruments"][0]
    prepared = parquet.replay(binding, root)
    plan = config_v2.build_plan(spec, prepared.admission_rules)
    from core.grid_v2 import GridV2StrategyHooks, execute_grid_v2_candidates
    from strategies.s07_ha_v01_b2 import strategy
    from tools.strategy_lab.dataset import result_row_values
    hooks = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=prepared.entry_filter_context)
    reference = replace(plan, settings=replace(plan.settings, prefer_compiled=False))
    matrix = np.load(out / "groups/BYBIT_AAAUSDT/window_01.npy")
    assert np.all(matrix[:, :, METRIC_AXIS_V3.index("total_trades")] == 0)
    if scenario == "cap":
        assert np.all(matrix[:, :, -1] > 0)
    else:
        assert np.all(matrix[:, :, -1] == 0)
    for si, name in enumerate(("is", "oos")):
        part = parquet.segment(prepared, binding, config_v2.calendar(spec.generation)[0], name, spec)
        oracle = execute_grid_v2_candidates(reference, part.dataframe, part.trade_start_idx, hooks,
            candidate_indices=[0, 6], compute_sharpe=False, compute_sharpe_daily=True, compute_sqn=True, compute_max_drawdown_mtm=True)
        for index, row in zip([0, 6], oracle.rows):
            np.testing.assert_allclose(matrix[index, si], np.append(result_row_values(row), row.guardrail_summary["margin_reject_count"]),
                                       rtol=1e-9, atol=1e-8, equal_nan=True)
    dataset = open_dataset(out)
    scope = dataset.resolve_scope("research", allow_partial=True)
    # Genuine zero returns are available to primary_profit; the trade-gated pool is empty.
    result = evaluate_scope(dataset, scope)
    assert result.summary["population"]["finite_count"] == 7
    assert result.summary["population"]["mean"] == 0
    allocation = evaluate_allocation([DatasetInput("empty", dataset, scope)], candidate_rule="trade_gate15_profit")
    block = allocation.summary["datasets"]["empty"]["blocks"][0]
    assert block["available_tickers"] == 0
    assert block["variants"]["primary"]["random_k"]["base_seed"] == 20261101
    assert block["variants"]["primary"]["random_k"]["draw_count"] == 100


@pytest.mark.slow
def test_replay_mutations_relocation_and_immutable_completion(tmp_path):
    path = request(tmp_path)
    root, out = tmp_path / "pack", tmp_path / "out"
    publish(root)
    run(path, root, out, window_selectors=[1], preflight_only=True)
    changed = tmp_path / "changed"
    publish(changed, change=1500)
    before = snapshot(out)
    with pytest.raises(ValueError, match="Pinned input changed"):
        run(path, changed, out, window_selectors=[1], resume=True)
    assert snapshot(out) == before
    rule_changed = tmp_path / "rules"
    publish(rule_changed, rules={"AAA": synthetic_rules(step=".01")})
    with pytest.raises(ValueError, match="rules changed"):
        run(path, rule_changed, out, window_selectors=[1], resume=True)
    refreshed = synthetic_rules(step="0.0010", minimum="0.0010", notional="0.00")
    refreshed["as_of_utc"] = "2026-11-02T00:00:00Z"
    refreshed["source_reference"] = "refreshed provenance"
    compatible = tmp_path / "compatible"
    publish(compatible, end="2024-07-02", rules={"AAA": refreshed})
    run(path, compatible, out, window_selectors=[1], resume=True)
    completed = snapshot(out)
    assert run(path, compatible, out, window_selectors=[1]).no_op
    assert snapshot(out) == completed
    for bad in (changed, rule_changed):
        with pytest.raises(ValueError):
            run(path, bad, out, window_selectors=[1])
        assert snapshot(out) == completed
    missing_venue = tmp_path / "venue"
    publish(missing_venue, symbols=("BBB",))
    with pytest.raises(ValueError):
        run(path, missing_venue, out, window_selectors=[1])
    assert snapshot(out) == completed


@pytest.mark.slow
def test_partial_corruption_backend_failure_and_completed_refusal(tmp_path, monkeypatch):
    path = request(tmp_path)
    root, out = tmp_path / "pack", tmp_path / "out"
    publish(root)
    def interrupted(*args):
        raise RuntimeError("stop")
    with pytest.raises(RuntimeError):
        run(path, root, out, window_selectors=[1], _after_group=interrupted)
    group = out / "groups/BYBIT_AAAUSDT/window_01.npy"
    expected = group.read_bytes()
    group.write_bytes(b"corrupt")
    result = run(path, root, out, window_selectors=[1], resume=True)
    assert result.regenerated_groups == 1 and group.read_bytes() == expected
    group.write_bytes(b"corrupt complete")
    before = snapshot(out)
    with pytest.raises(ValueError, match="checksum"):
        run(path, root, out, window_selectors=[1])
    with pytest.raises(ValueError, match="checksum"):
        open_dataset(out)
    assert snapshot(out) == before
    failed = tmp_path / "failed"
    original = parquet.execute_grid_v2_candidates
    def fallback(*args, **kwargs):
        result = original(*args, **kwargs)
        result.metadata["backend_kind"] = "reference"
        return result
    monkeypatch.setattr(parquet, "execute_grid_v2_candidates", fallback)
    with pytest.raises(ValueError, match="compiled_numba"):
        run(path, root, failed, window_selectors=[1])
    assert not (failed / "manifest.json").exists()


def test_prepare_failures_and_source_output_alias(tmp_path):
    path = request(tmp_path)
    for name, options in (("warmup", {"start": "2024-02-01"}),
                          ("tail", {"end": "2024-03-31"}),
                          ("gap", {"drop": 1500})):
        root = tmp_path / name
        publish(root, **options)
        with pytest.raises(ValueError):
            run(path, root, tmp_path / (name + "-out"), window_selectors=[1], preflight_only=True)
        assert not (tmp_path / (name + "-out/manifest.json")).exists()
    root = tmp_path / "normal"
    publish(root)
    with pytest.raises(ValueError, match="market-data root"):
        run(path, root, root / ".." / "normal" / "out", preflight_only=True)
    from data_provider import manifest
    (root / manifest.UPDATE_MARKER_NAME).write_text("{}")
    with pytest.raises(ValueError, match="pending"):
        run(path, root, tmp_path / "pending", preflight_only=True)


@pytest.mark.slow
def test_multimonth_units_scope_unlock_and_determinism(tmp_path):
    path = request(tmp_path, months=2)
    raw = json.loads(path.read_text())
    raw["preregistration"]["analysis_scopes"].append({"name": "locked", "ticker_cell": "research", "window_numbers": [1, 2], "requires_unlock": True})
    path.write_text(json.dumps(raw))
    root, out = tmp_path / "pack", tmp_path / "out"
    publish(root, end="2024-09-02")
    run(path, root, out)
    dataset = open_dataset(out)
    with pytest.raises(ValueError, match="unlock"):
        dataset.resolve_scope("locked")
    policy = tmp_path / "policy.txt"
    policy.write_text("Frozen synthetic policy")
    assert dataset.resolve_scope("locked", unlock=True, policy_path=policy).requires_unlock
    for attempt in (1, 2):
        dataset = open_dataset(out)
        scope = dataset.resolve_scope("research")
        analysis = evaluate_scope(dataset, scope)
        assert analysis.run_metadata["period_unit"] == "oos_block"
        assert analysis.summary["oos_period_months"] == 2
        write_analysis(analysis, tmp_path / f"analysis{attempt}", dataset_root=out)
        allocation = evaluate_allocation([DatasetInput("s07", dataset, scope)], candidate_rule="primary_profit")
        assert allocation.summary["oos_period_months"] == {"s07": 2}
        write_allocation(allocation, tmp_path / f"allocation{attempt}", dataset_roots=[out])
    assert snapshot(tmp_path / "analysis1") == snapshot(tmp_path / "analysis2")
    assert snapshot(tmp_path / "allocation1") == snapshot(tmp_path / "allocation2")


@pytest.mark.slow
def test_is_decisions_precede_oos_and_unavailable_blocks(tmp_path, monkeypatch):
    path = request(tmp_path, two=True)
    root, out = tmp_path / "pack", tmp_path / "out"
    publish(root, symbols=("AAA", "BBB"))
    run(path, root, out)
    from tools.strategy_lab.analysis import evaluate, allocation
    dataset = open_dataset(out)
    scope = dataset.resolve_scope("research")
    events = []
    original_selection = evaluate.select_candidates
    original_oos = dataset.load_oos_window
    def select(*args, **kwargs):
        result = original_selection(*args, **kwargs)
        events.append("selection")
        return result
    def oos(scope, window):
        assert events.count("selection") >= 2
        events.clear()
        return original_oos(scope, window)
    with monkeypatch.context() as m:
        m.setattr(evaluate, "select_candidates", select)
        m.setattr(dataset, "load_oos_window", oos)
        evaluate_scope(dataset, scope)
    original_rank = allocation._rank
    def rank(*args, **kwargs):
        result = original_rank(*args, **kwargs)
        events.append("rank")
        return result
    def missing_oos(scope, window):
        assert events.count("selection") == 2 and events.count("rank") >= 2
        events.clear()
        result = original_oos(scope, window)
        if window.window_id == 2:
            for matrix in result.values():
                matrix[:, dataset.metric_index["net_profit_pct"]] = np.nan
        return result
    with monkeypatch.context() as m:
        m.setattr(allocation, "select_candidates", select)
        m.setattr(allocation, "_rank", rank)
        m.setattr(dataset, "load_oos_window", missing_oos)
        result = evaluate_allocation([DatasetInput("s07", dataset, scope)], candidate_rule="primary_profit")
    primary = result.summary["datasets"]["s07"]["variants"]["primary"]
    assert primary["monthly_capacity_returns_pct"][1] is None
    assert primary["headline_mean_capacity_return_pct"] is None
    assert primary["portfolio"]["compounded_return_pct"] is None
    legacy = deepcopy(dataset)
    legacy.schema_version = "strategy_lab_dataset_v2"
    with pytest.raises(ValueError, match="mixed v2/v3"):
        evaluate_allocation([DatasetInput("new", dataset, scope), DatasetInput("old", legacy, scope)], candidate_rule="primary_profit")


def test_source_alias_and_lock_refusal(tmp_path):
    import os
    import subprocess
    path = request(tmp_path)
    root, alias = tmp_path / "pack", tmp_path / "alias"
    publish(root)
    if os.name == "nt":
        # Junctions exercise resolved directory aliases without changing Windows privileges.
        subprocess.run(["cmd", "/c", "mklink", "/J", str(alias), str(root)], check=True, capture_output=True)
    else:
        alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError, match="market-data root"):
        run(path, root, alias / "output", preflight_only=True)
    from data_provider.pack_lock import pack_guard
    with pack_guard(root):
        with pytest.raises(ValueError, match="busy"):
            run(path, root, tmp_path / "locked", preflight_only=True)


def test_domain_geometry_and_foreign_group_refusal(tmp_path, monkeypatch):
    path = request(tmp_path, two=True)
    root = tmp_path / "pack"
    publish(root, symbols=("AAA", "BBB"))
    original = parquet.geometry
    calls = []
    def mismatch(*args):
        result = original(*args)
        calls.append(1)
        if len(calls) == 2:
            result["candidates"][0]["params"]["stopX"] += 1
        return result
    with monkeypatch.context() as m:
        m.setattr(parquet, "geometry", mismatch)
        with pytest.raises(ValueError, match="geometry differs"):
            run(path, root, tmp_path / "different", preflight_only=True)
    assert not (tmp_path / "different").exists()
    raw = json.loads(path.read_text())
    raw["generation"]["planning"]["axis_values"]["entryRunLength"] = [2, 3]
    path.write_text(json.dumps(raw))
    load_run_spec(path)  # Offline validates types, not a rule-free plan.
    with pytest.raises(ValueError, match="registered Grid domain"):
        run(path, root, tmp_path / "domains", preflight_only=True)
    path = request(tmp_path, two=True)
    out = tmp_path / "out"
    run(path, root, out, preflight_only=True)
    manifest_path = out / "manifest.partial.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["groups"] = [{"path": "groups/FOREIGN_USDT/window_01.npy"}]
    manifest_path.write_text(json.dumps(manifest))
    before = snapshot(out)
    with pytest.raises(ValueError, match="foreign"):
        run(path, root, out, resume=True)
    assert snapshot(out) == before


@pytest.fixture(scope="module")
def historical_v3(tmp_path_factory):
    """Real completed two-candidate/two-block input, created before registry drift."""
    work = tmp_path_factory.mktemp("historical-v3")
    path = request(work, two=True)
    raw = json.loads(path.read_text())
    raw["generation"]["planning"].update(enabled_axes=["stopLP"],
        axis_values={"stopLP": [2, 4]}, expected_candidate_count=2)
    raw["generation"]["windows"].update(requested_end="2024-05-01", expected_window_count=2)
    raw["preregistration"]["analysis_scopes"][0]["window_numbers"] = [1, 2]
    path.write_text(json.dumps(raw), encoding="utf-8")
    root, out, partial = (work / name for name in ("pack", "out", "partial"))
    publish(root, symbols=("AAA", "BBB"))
    run(path, root, partial, preflight_only=True)
    run(path, root, out)
    hidden = work / "hidden-pack"
    root.rename(hidden)
    assert not root.exists()
    return path, hidden, out, partial


def offline_results(out):
    from tools.strategy_lab.analysis.output import render_files, render_allocation_files
    dataset = open_dataset(out)
    scope = dataset.resolve_scope("research")
    analysis = evaluate_scope(dataset, scope)
    allocation = evaluate_allocation([DatasetInput("s07", dataset, scope)], candidate_rule="primary_profit")
    return dataset.manifest["identity"], render_files(analysis), render_allocation_files(allocation)


def test_declarative_policy_and_universe_on_offline_v3(historical_v3, tmp_path, monkeypatch):
    from tools.strategy_lab.analysis import load_candidate_policy, load_universe
    from tools.strategy_lab.analysis.evaluate import CustomRule
    from tools.strategy_lab.analysis.dataset import AnalysisError
    from core import data_input
    from tools.strategy_lab import config
    _, _, out, _ = historical_v3
    before = snapshot(out)
    for module, name in ((data_input, "prepare_input"), (data_input, "replay_input"), (config, "get_strategy_config")):
        monkeypatch.setattr(module, name, lambda *a, **k: pytest.fail("offline input accessed live stack"))
    policy = load_candidate_policy(REPO_ROOT / "tools/strategy_lab/runspecs/candidate_policy_example.json")
    universe = load_universe(REPO_ROOT / "tools/strategy_lab/runspecs/analysis_universe_example.json")
    ds = open_dataset(out); scope = ds.resolve_scope("research")
    with pytest.raises(AnalysisError, match="built-in"):
        evaluate_scope(ds, scope, custom_rules=[CustomRule("custom", "1", lambda *a: np.ones(2))])
    analysis = evaluate_scope(ds, scope, candidate_policy=policy, universe=universe)
    allocation = evaluate_allocation([DatasetInput("v3", ds, scope)], candidate_policy=policy, universe=universe)
    assert analysis.run_metadata["analysis_scope"]["tickers"] == ["BYBIT_AAAUSDT"]
    assert analysis.run_metadata["analysis_scope"]["ticker_cell_count"] == 2
    assert analysis.summary["rules"][policy.key]["bootstraps"]["top5_lift"]["reason"] == "selection_count=1"
    assert allocation.summary["schema_version"] == "strategy_lab_allocation_v2"
    write_analysis(analysis, tmp_path / "analysis", dataset_root=out)
    write_allocation(allocation, tmp_path / "allocation", dataset_roots=(out,))
    assert snapshot(out) == before


@pytest.mark.parametrize("drift", ["engine", "bounds", "options", "unregistered", "profile", "registry", "nomination", "ties"])
def test_historical_reader_ignores_current_generation_state(historical_v3, tmp_path, monkeypatch, drift):
    from tools.strategy_lab import config
    from tools.strategy_lab.analysis import dataset_v3
    path, root, out, partial = historical_v3
    before = snapshot(out)
    expected = offline_results(out)
    original_config = config.get_strategy_config

    def unavailable(_):
        raise ValueError("producing strategy is no longer registered")

    def changed_config(strategy_id):
        value = deepcopy(original_config(strategy_id))
        if drift == "bounds":
            value["parameters"]["initialCapital"]["max"] = 500
        elif drift == "options":
            value["parameters"]["entryRunLength"]["options"] = [999]
        else:
            value["execution"]["entryAdmission"] = "unsupported_future_admission"
        return value

    def forbidden(*args, **kwargs):
        pytest.fail("offline reader or stale generation invoked live lookup/preparation/planning/execution")

    with monkeypatch.context() as m:
        if drift == "engine":
            m.setattr(config, "GRID_V2_ENGINE_VERSION", "future_engine")
        elif drift == "ties":
            # A nested edit also catches an import-time shallow copy of the registry.
            m.setitem(config.RULE_REGISTRY["tie_break"][0], "direction", "ascending")
        elif drift in {"registry", "nomination"}:
            # Mutate the existing nested list, not just the module's dict binding.
            key = "selectable_rules" if drift == "registry" else "nomination_eligible_rules"
            original = config.RULE_REGISTRY[key]
            m.setitem(config.RULE_REGISTRY, key, original.copy())
            config.RULE_REGISTRY[key].pop()
        else:
            m.setattr(config, "get_strategy_config", unavailable if drift == "unregistered" else changed_config)
        m.setattr(parquet, "generate_parquet", forbidden)
        with pytest.raises(config.StrategyLabConfigError):
            load_run_spec(path)
        with pytest.raises(config.StrategyLabConfigError):
            run(path, root, tmp_path / "new", preflight_only=True)
        assert not (tmp_path / "new").exists()
        if drift == "engine":
            for output, kwargs in ((partial, {"resume": True}), (out, {})):
                saved = snapshot(output)
                with pytest.raises(config.StrategyLabConfigError):
                    run(path, root, output, **kwargs)
                assert snapshot(output) == saved
        for module, name in ((config, "get_strategy_config"), (config, "_validate_strategy"),
                (config_v2, "build_grid_v2_plan"), (parquet, "get_strategy"),
                (parquet.data_input, "prepare_input"), (parquet.data_input, "replay_input"),
                (parquet, "execute_grid_v2_candidates")):
            m.setattr(module, name, forbidden)
        calls = []
        validate = dataset_v3.validate_saved_request
        def counted(raw):
            calls.append(True)
            return validate(raw)
        m.setattr(dataset_v3, "validate_saved_request", counted)
        assert offline_results(out) == expected
        assert len(calls) == 1
    assert snapshot(out) == before
    assert run(path, root, out).no_op
    assert snapshot(out) == before


@pytest.mark.parametrize("field,value", [
    ("version", "unknown_rules"),
    ("minimum_completed_trades", 14),
    ("minimum_completed_trades", True),
    ("baseline_rule", "trade_gate15_profit"),
    ("selectable_rules", ["primary_profit"]),
    ("selectable_rules", ["unknown_rule"]),
    ("nomination_eligible_rules", []),
    ("non_nominatable_diagnostics", []),
    ("tie_break", [{"field": "candidate_id", "direction": "descending"}]),
])
def test_frozen_rules_reject_changed_semantics(historical_v3, field, value):
    raw = json.loads(historical_v3[0].read_text())
    raw["preregistration"]["rule_registry"][field] = value
    with pytest.raises(ValueError, match="rule registry"):
        config_v2.validate_saved_request(raw)


@pytest.mark.parametrize("mutation", [
    lambda r: r.update(schema_version="unknown_request"),
    lambda r: r["preregistration"].update(analysis_contract="unknown_analysis"),
    lambda r: r["generation"]["strategy"].update(strategy_version=1),
    lambda r: r["generation"]["strategy"].pop("grid_v2_engine_version"),
    lambda r: r["generation"]["economics"]["base_params"].update(stopLP=[2]),
    lambda r: r["generation"]["economics"]["base_params"].update(stopX=float("inf")),
])
def test_frozen_request_rejects_unsupported_structure(historical_v3, mutation):
    raw = json.loads(historical_v3[0].read_text())
    mutation(raw)
    with pytest.raises(ValueError):
        config_v2.validate_saved_request(raw)


def test_reader_rejects_resealed_rule_semantics(historical_v3, tmp_path):
    from tools.strategy_lab.config import canonical_sha256
    from tools.strategy_lab.dataset import artifact_record, atomic_write_json
    from tools.strategy_lab.analysis.dataset import AnalysisError
    out = tmp_path / "resealed"
    shutil.copytree(historical_v3[2], out)
    raw = load_json(out / "normalized_runspec.json", "request")
    # Keep the version, membership and all other facts; change only declared order.
    raw["preregistration"]["rule_registry"]["nomination_eligible_rules"].reverse()
    atomic_write_json(out / "normalized_runspec.json", raw)
    manifest = load_json(out / "manifest.json", "manifest")
    manifest["artifacts"]["normalized_runspec.json"] = artifact_record(out / "normalized_runspec.json", out)
    manifest["identity"]["pre_registration_sha256"] = canonical_sha256(raw)
    atomic_write_json(out / "manifest.json", manifest)
    parquet.verify_artifacts(out, manifest)  # All artifact sizes/hashes are valid.
    before = snapshot(out)
    with pytest.raises(AnalysisError, match="rule registry"):
        open_dataset(out)
    assert snapshot(out) == before


@pytest.mark.parametrize("case", ["both_labels", "gate14", "unknown_analysis", "reverse", "missing", "unknown_request"])
def test_reader_rejects_resealed_schema_pair(historical_v3, tmp_path, case):
    from tools.strategy_lab.config import canonical_sha256
    from tools.strategy_lab.dataset import artifact_record, atomic_write_json
    from tools.strategy_lab.analysis.dataset import AnalysisError, _candidate_geometry
    source = historical_v3[2]
    original = snapshot(source)
    out = tmp_path / "resealed"
    shutil.copytree(source, out)
    raw = load_json(out / "normalized_runspec.json", "request")
    manifest = load_json(out / "manifest.json", "manifest")
    candidates = load_json(out / "candidates.json", "candidates")
    if case in {"both_labels", "gate14", "unknown_analysis"}:
        # Both manifest declarations AND the candidate label change; the request
        # remains v2. A manifest-only mutation would fail at the wrong boundary.
        manifest["schema_version"] = manifest["identity"]["dataset_schema"] = "strategy_lab_dataset_v2"
        candidates["schema_version"] = "strategy_lab_candidates_v1"
        if case == "gate14":
            raw["preregistration"]["rule_registry"]["minimum_completed_trades"] = 14
        elif case == "unknown_analysis":
            raw["preregistration"]["analysis_contract"] = "unsupported_analysis_contract"
    elif case == "missing":
        raw.pop("schema_version")
    else:
        raw["schema_version"] = "strategy_lab_runspec_v1" if case == "reverse" else "unknown_request"
    for name, payload in (("normalized_runspec.json", raw), ("candidates.json", candidates)):
        atomic_write_json(out / name, payload)
        manifest["artifacts"][name] = artifact_record(out / name, out)
    manifest["identity"]["pre_registration_sha256"] = canonical_sha256(raw)
    atomic_write_json(out / "manifest.json", manifest)
    parquet.verify_artifacts(out, manifest)
    assert _candidate_geometry(candidates, manifest["identity"]).count == 2
    assert manifest["identity"]["pre_registration_sha256"] == canonical_sha256(raw)
    before = snapshot(out)
    with pytest.raises(AnalysisError, match="dataset/RunSpec schema mismatch"):
        open_dataset(out)
    assert snapshot(out) == before
    assert snapshot(source) == original


def test_descriptive_population_weights_with_asymmetric_missingness(historical_v3, monkeypatch):
    from tools.strategy_lab.analysis.output import render_files
    dataset = open_dataset(historical_v3[2])
    scope = dataset.resolve_scope("research")
    assert dataset.geometry.count == 2 and len(scope.windows) == len(scope.tickers) == 2
    actual = dataset.load_oos_window
    def observations(scope, window):
        matrices = actual(scope, window)
        for ti, matrix in enumerate(matrices.values()):
            matrix[:, dataset.metric_index["net_profit_pct"]] = (
                [100, np.nan] if ti == 1 and window.window_id == 1 else [0, 0])
            # Net return has no generic metric-summary key. Feed the same small
            # observation set through an existing diagnostic channel to certify
            # that pooling path without adding a new output statistic.
            matrix[:, dataset.metric_index["profit_factor"]] = matrix[:, dataset.metric_index["net_profit_pct"]]
        return matrices
    monkeypatch.setattr(dataset, "load_oos_window", observations)
    result = evaluate_scope(dataset, scope)
    population = result.summary["population"]
    diagnostic = result.summary["diagnostics"]["metric_summaries"]["oos"]["profit_factor"]
    assert population["monthly_means"] == [50, 0]
    assert population["mean"] == 25
    assert population["median"] == population["quartile_25"] == population["quartile_75"] == 0
    assert population["finite_count"] == diagnostic["finite_count"] == 7
    assert population["unavailable_count"] == 1 and population["profitable_share"] == 1 / 8
    assert diagnostic["mean"] == pytest.approx(100 / 7)
    report = render_files(result)["report.md"].decode()
    assert "headline mean (equal instrument/block weights): 25.000000" in report
    assert "median (pooled finite observations): 0.000000" in report
    assert "including unavailable entries in the denominator" in report
    assert "Generic diagnostic summaries of other metrics pool finite observations" in report
    assert "Net profit has no generic metric-summary output." in report
    assert "net_profit_pct" not in result.summary["diagnostics"]["metric_summaries"]["oos"]
    assert "finite candidate returns within each instrument/block" in report
    assert "weak with five or six blocks" not in report
