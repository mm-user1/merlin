"""Filtered union/segment execution, explicit v4 analysis and legacy dispatch."""
from copy import deepcopy
from dataclasses import replace
import json
import shutil

import numpy as np
import pandas as pd
import pytest

from tools.strategy_lab.config import load_run_spec, canonical_sha256
from tools.strategy_lab.config_v2 import build_plan, calendar
from tools.strategy_lab import parquet
from tools.strategy_lab.generate import generate_dataset
from tools.strategy_lab.dataset import METRIC_AXIS_V3, result_row_values, atomic_write_json, artifact_record
from tools.strategy_lab.filters import effective
from tools.strategy_lab.analysis.dataset import open_dataset, ScopeLockedError, AnalysisError
from tools.strategy_lab.analysis.evaluate import evaluate_scope
from tools.strategy_lab.analysis.allocation import DatasetInput, evaluate_allocation
from tools.strategy_lab.analysis.output import write_analysis, write_allocation
from _filter_helpers import packs, is_request, publish_filter, save_request, snapshot


def generate(path, target, out, **kwargs):
    return generate_dataset(path, data_root=target, output_dir=out, **kwargs)


@pytest.mark.parametrize("version,m", [(1, 1), (2, 1), (2, 2)])
def test_compiled_union_reference_and_boundary_state(packs, tmp_path, version, m):
    from core import data_input
    from core.entry_filters import OFF
    from core.grid_v2 import GridV2StrategyHooks, execute_grid_v2_candidates
    from strategies.s07_ha_v01_b2 import strategy
    target, filters = packs
    raw = is_request(filters, version=version, m=m)
    path = save_request(tmp_path, raw); out = tmp_path / "out"
    assert generate(path, target, out).completed_groups == 2
    bindings = json.loads((out / "input_bindings.json").read_text())
    assert bindings["schema_version"] == 2 and bindings["effective_entry_filters"] == effective(raw)
    binding = bindings["instruments"][0]
    prepared = parquet.replay(binding, target, effective(raw))
    spec = load_run_spec(path); plan = build_plan(spec, prepared.admission_rules)
    hooks = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=prepared.entry_filter_context)
    reference = replace(plan, settings=replace(plan.settings, prefer_compiled=False))
    window = calendar(spec.generation)[0]
    matrix = np.load(out / "groups/BYBIT_AAAUSDT/window_01.npy")
    for si, name in enumerate(("is", "oos")):
        part = parquet.segment(prepared, binding, window, name, spec)
        oracle = execute_grid_v2_candidates(reference, part.dataframe, part.trade_start_idx, hooks,
            compute_sharpe=False, compute_sharpe_daily=True, compute_sqn=True, compute_max_drawdown_mtm=True)
        for index, row in enumerate(oracle.rows):
            expected = np.append(result_row_values(row), row.guardrail_summary["margin_reject_count"])
            np.testing.assert_allclose(matrix[index, si], expected, rtol=1e-9, atol=1e-8, equal_nan=True)
            assert matrix[index, si, METRIC_AXIS_V3.index("total_trades")] == expected[METRIC_AXIS_V3.index("total_trades")]
    if m == 1: return
    # OOS is a slice of the actual union, never a fresh filter origin at March 1.
    part = parquet.segment(prepared, binding, window, "oos", spec)
    late = publish_filter(tmp_path / "late", start="2024-02-20")
    bars = []
    for root in (filters, late):
        with data_input.pack_session(root) as session:
            bars.append(session.load_slice("BYBIT_BBBUSDT", start="2024-02-20T00:00Z", warmup_start="2024-02-20T00:00Z",
                end="2024-05-02T00:00Z", timeframe_minutes=5).bars)
    pd.testing.assert_frame_equal(*bars, check_exact=True)
    source = dict(type="parquet", version=1, pack_root=str(target), instrument_id="BYBIT_AAAUSDT", timeframe_minutes=60)
    controls = []
    for control in (is_request(late), is_request(filters, m=1)):
        controls.append(data_input.prepare_input(source, date_filter=True, start="2024-03-01", end="2024-03-31",
            warmup_bars=100, admission_params=raw["generation"]["economics"]["base_params"], entry_filters=effective(control)))
    raw_signals = GridV2StrategyHooks.from_strategy(strategy, entry_filter_context=OFF).build_execution_data(
        part.dataframe, plan.candidate_table.params_for_index(0)).signals
    context = prepared.entry_filter_context
    indices = context.indices(part.dataframe.index)[part.trade_start_idx:]
    raw_entries = raw_signals.long_entries[part.trade_start_idx:]
    for control in controls:
        np.testing.assert_array_equal(control.frame.index, part.dataframe.index)
        different = context.permit_long[indices] != control.entry_filter_context.permit_long[control.trade_start_idx:]
        assert np.any(different & raw_entries)
    blocked = np.count_nonzero(raw_entries & ~context.permit_long[indices] |
        raw_signals.short_entries[part.trade_start_idx:] & ~context.permit_short[indices])
    trades = matrix[0, 1, METRIC_AXIS_V3.index("total_trades")]
    assert trades > 0 and blocked > 0
    print(json.dumps({"published_overlap_rows": len(bars[0]), "overlap_identical": True,
        "actual_oos_trades": float(trades), "blocked_raw_entries": int(blocked), "controls_differ_on_raw_entries": True}))


def test_v4_offline_scope_locks_and_labeled_allocation(packs, tmp_path, monkeypatch):
    target, filters = packs; raw = is_request(filters)
    raw["preregistration"]["analysis_scopes"].append(dict(name="locked", ticker_cell="research", window_numbers=[1, 2], requires_unlock=True))
    out = tmp_path / "on"; generate(save_request(tmp_path, raw), target, out)
    off = tmp_path / "off"; generate(save_request(tmp_path, is_request(), "off.json"), target, off)
    before = [snapshot(p) for p in (out, off)]
    from core import data_input
    from tools.strategy_lab import config
    for module, name in ((data_input, "prepare_input"), (data_input, "replay_input"), (config, "get_strategy_config"), (parquet, "build_plan")):
        monkeypatch.setattr(module, name, lambda *a, **k: pytest.fail("offline access to live source/config/plan"))
    on_ds, off_ds = open_dataset(out), open_dataset(off)
    with pytest.raises(ScopeLockedError): on_ds.resolve_scope("locked")
    with pytest.raises(ScopeLockedError): on_ds.resolve_scope("locked", unlock=True)
    policy = tmp_path / "policy.txt"; policy.write_text("frozen synthetic policy", encoding="utf-8")
    scope = on_ds.resolve_scope("locked", unlock=True, policy_path=policy)
    analysis = evaluate_scope(on_ds, scope)
    assert analysis.summary["schema_version"] == "strategy_lab_analysis_v2"
    assert analysis.summary["capabilities"]["evidence"]["status"] == "not_requested"
    write_analysis(analysis, tmp_path / "analysis", dataset_root=out)
    allocation = evaluate_allocation([DatasetInput("on", on_ds, scope),
        DatasetInput("off", off_ds, off_ds.resolve_scope("research"))], candidate_rule="primary_profit")
    write_allocation(allocation, tmp_path / "allocation", dataset_roots=(out, off))
    text = (tmp_path / "allocation/report.md").read_text(encoding="utf-8")
    assert "Filter Off" in text and "F06 v2" in text and "entry_filter_inputs" in text
    assert analysis.run_metadata["analysis_scope"]["unlock_evidence"]["policy_sha256"]
    from tools.strategy_lab.analysis import load_candidate_policy, load_universe
    from test_research_inputs import document, policy_doc, universe_doc
    selection = load_candidate_policy(document(tmp_path, policy_doc()))
    universe = load_universe(document(tmp_path, universe_doc(["BYBIT_AAAUSDT"]), "universe.json"))
    exploratory = evaluate_scope(on_ds, scope, candidate_policy=selection, universe=universe)
    paired = evaluate_allocation([DatasetInput("on", on_ds, scope),
        DatasetInput("off", off_ds, off_ds.resolve_scope("research"))], candidate_policy=selection, universe=universe)
    assert exploratory.summary["rules"][selection.key]["bootstraps"]["top5_lift"]["reason"] == "selection_count=1"
    assert paired.run_metadata["research_inputs"]["universe"]["datasets"]["on"]["unlock_evidence"] == scope.unlock_evidence
    assert paired.run_metadata["datasets"]["on"]["entry_filters"]["effective"]
    assert not paired.run_metadata["datasets"]["off"]["entry_filters"]["effective"]
    write_analysis(exploratory, tmp_path / "policy-analysis", dataset_root=out)
    write_allocation(paired, tmp_path / "policy-allocation", dataset_roots=(out, off))
    assert [snapshot(p) for p in (out, off)] == before


def test_v4_pending_source_checks_and_all_valid_reuse(packs, tmp_path, monkeypatch):
    target, original = packs; filters = tmp_path / "filters"; shutil.copytree(original, filters)
    path = save_request(tmp_path, is_request(filters)); out = tmp_path / "out"
    def stop(count, path):
        if count == 1: raise KeyboardInterrupt("after first group")
    with pytest.raises(KeyboardInterrupt): generate(path, target, out, _after_group=stop)
    saved = snapshot(out / "groups")
    changed = publish_filter(tmp_path / "changed", change="2024-01-15T00:00Z")
    shutil.copytree(changed, filters, dirs_exist_ok=True)
    with pytest.raises(ValueError, match="Pinned filter input changed"): generate(path, target, out, resume=True)
    assert snapshot(out / "groups") == saved and not (out / "manifest.json").exists()
    shutil.copytree(original, filters, dirs_exist_ok=True)
    def last(count, path):
        if count == 2: raise KeyboardInterrupt("all groups claimed")
    with pytest.raises(KeyboardInterrupt): generate(path, target, out, resume=True, _after_group=last)
    # All-valid partial finalization keeps the historical IS/OOS reuse policy.
    with monkeypatch.context() as patch:
        patch.setattr(parquet, "replay", lambda *a, **k: pytest.fail("all-valid partial opened sources"))
        assert generate(path, target, out, resume=True).reused_groups == 2
    before = snapshot(out)
    shutil.copytree(changed, filters, dirs_exist_ok=True)
    with pytest.raises(ValueError, match="Pinned filter input changed"): generate(path, target, out)
    assert snapshot(out) == before


def test_v4_new_old_off_and_mixed_allocation_refusal(packs, tmp_path):
    new = is_request(); old = deepcopy(new); old["schema_version"] = "strategy_lab_runspec_v2"
    old["generation"]["execution"] = {"policy": "risk_bracket_v1", "entry_filters": []}
    outputs = []
    for name, raw in (("old", old), ("new", new)):
        out = tmp_path / name; generate(save_request(tmp_path, raw, name+".json"), packs[0], out); outputs.append(out)
    for file in (outputs[0] / "groups").rglob("*.npy"):
        assert file.read_bytes() == (outputs[1] / file.relative_to(outputs[0])).read_bytes()
    datasets = [open_dataset(p) for p in outputs]
    with pytest.raises(AnalysisError, match="mixed"):
        evaluate_allocation([DatasetInput(str(i), d, d.resolve_scope("research")) for i, d in enumerate(datasets)], candidate_rule="primary_profit")


@pytest.mark.parametrize("case", ["pair", "binding_version", "effective", "origin", "instrument"])
def test_resealed_v4_filter_history_refused(packs, tmp_path, monkeypatch, case):
    out = tmp_path / "out"
    generate(save_request(tmp_path, is_request(packs[1])), packs[0], out)
    manifest = json.loads((out / "manifest.json").read_text())
    bindings = json.loads((out / "input_bindings.json").read_text())
    if case == "pair": manifest["schema_version"] = "strategy_lab_dataset_v3"
    elif case == "binding_version": bindings["schema_version"] = 1
    elif case == "effective": bindings["effective_entry_filters"] = []
    else:
        source = bindings["instruments"][0]["entry_filter_inputs"]["sources"][0]
        if case == "origin": source["bounds"]["origin"] = "2024-02-01T00:00:00+00:00"
        else: source["instrument"]["instrument_id"] = "BYBIT_OTHERUSDT"
    atomic_write_json(out / "input_bindings.json", bindings)
    manifest["artifacts"]["input_bindings.json"] = artifact_record(out / "input_bindings.json", out)
    manifest["identity"]["input_bindings_sha256"] = canonical_sha256([parquet.semantic_binding(b) for b in bindings["instruments"]])
    atomic_write_json(out / "manifest.json", manifest)
    before = snapshot(out)
    from core import data_input
    monkeypatch.setattr(data_input, "replay_input", lambda *a, **k: pytest.fail("offline source replay"))
    with pytest.raises(AnalysisError): open_dataset(out)
    assert snapshot(out) == before


@pytest.mark.parametrize("version", [None, "unknown_request"])
def test_legacy_missing_unknown_request_version(tmp_path, version):
    from test_strategy_lab_analysis import _synthetic_dataset
    out = _synthetic_dataset(tmp_path / "legacy")
    # The helper returns the dataset path; valid legacy control must still open.
    control = snapshot(out)
    open_dataset(out)
    assert snapshot(out) == control
    manifest = json.loads((out / "manifest.json").read_text())
    raw = json.loads((out / "normalized_runspec.json").read_text())
    if version is None: raw.pop("schema_version")
    else: raw["schema_version"] = version
    atomic_write_json(out / "normalized_runspec.json", raw)
    manifest["artifacts"]["normalized_runspec.json"] = artifact_record(out / "normalized_runspec.json", out)
    manifest["identity"]["pre_registration_sha256"] = canonical_sha256(raw)
    atomic_write_json(out / "manifest.json", manifest)
    before = snapshot(out)
    with pytest.raises(AnalysisError, match="dataset/RunSpec schema mismatch"): open_dataset(out)
    assert snapshot(out) == before
