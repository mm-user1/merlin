"""Frozen origins, strict relocation, publication and source failure propagation."""
from copy import deepcopy
from dataclasses import replace
import importlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pytest

from tools.strategy_lab.filters import effective, runtime_roots, read_overrides
from tools.strategy_lab.fixed.config import resolve
from tools.strategy_lab.fixed.storage import open_dataset, account_identity, contract_identity
from tools.strategy_lab.config import canonical_sha256
from tools.strategy_lab.dataset import atomic_write_json
from _filter_helpers import packs, fixed_request, publish_filter, save_request, snapshot

runner = importlib.import_module("tools.strategy_lab.fixed.run")


def test_saved_origin_extension_and_consumed_mutation(packs, tmp_path, monkeypatch):
    target, original = packs
    filters = tmp_path / "source"; shutil.copytree(original, filters)
    raw = fixed_request(filters); path = save_request(tmp_path, raw); out = tmp_path / "out"
    runner.run(path, target, out, preflight_only=True)
    frozen = open_dataset(out, allow_partial=True)
    account = frozen["contract"]["accounts"][0]
    items = frozen["contract"]["resolved"]["entry_filters"]
    prior = runner.replay(account, target, items)
    extended = publish_filter(tmp_path / "extended", start="2023-12-01", end="2024-06-01")
    shutil.copytree(extended, filters, dirs_exist_ok=True)
    replayed = runner.replay(account, target, items)
    assert replayed.entry_filter_inputs == prior.entry_filter_inputs
    np.testing.assert_array_equal(replayed.entry_filter_context.permit_long, prior.entry_filter_context.permit_long)
    monkeypatch.setattr(runner, "prepare_account", lambda *a: pytest.fail("saved origin freshly prepared"))
    assert runner.run(path, target, out, resume=True)["complete"]
    saved = snapshot(out)
    assert runner.run(path, target, out)["no_op"] and snapshot(out) == saved
    assert open_dataset(out)["contract"] == frozen["contract"]
    # An early volume edit preserves the colours and eventual permissions, but is consumed.
    edited = publish_filter(tmp_path / "edited", start="2023-12-01", change="2024-01-15T00:00Z")
    cfg, resolved = resolve(raw)
    from core import data_input
    source = dict(type="parquet", version=1, pack_root=str(target), instrument_id="BYBIT_AAAUSDT", timeframe_minutes=60)
    changed_raw = fixed_request(edited)
    fresh = data_input.prepare_input(source, date_filter=True, start="2024-03-01", end="2024-03-31",
        warmup_bars=100, admission_params=resolved["parameter_sets"][0]["params"], entry_filters=effective(changed_raw))
    np.testing.assert_array_equal(fresh.entry_filter_context.permit_long, prior.entry_filter_context.permit_long)
    shutil.copytree(edited, filters, dirs_exist_ok=True)
    with pytest.raises(ValueError, match="Pinned filter input changed"):
        runner.run(path, target, out)
    assert snapshot(out) == saved


def test_shared_locator_relocation_and_final_claim_recovery(packs, tmp_path, monkeypatch):
    target, filters = packs
    raw = fixed_request(filters)
    extra = deepcopy(raw["generation"]["execution"]["entryFilters"]["items"][0])
    extra["id"] = "second"; extra["parameters"]["confirmationBars"] = 4
    raw["generation"]["execution"]["entryFilters"]["items"].append(extra)
    path = save_request(tmp_path, raw); out = tmp_path / "out"
    writer = runner.write_index
    def fail(root, manifest):
        if manifest["completed"]: raise KeyboardInterrupt("final claim saved")
        writer(root, manifest)
    monkeypatch.setattr(runner, "write_index", fail)
    with pytest.raises(KeyboardInterrupt): runner.run(path, target, out)
    partial = open_dataset(out, allow_partial=True)
    saved = snapshot(out / "accounts")
    relocated = tmp_path / "relocated"; shutil.copytree(filters, relocated)
    moved_target = tmp_path / "target"; shutil.copytree(target, moved_target)
    monkeypatch.setattr(runner, "write_index", writer)
    monkeypatch.setattr(runner, "execute_account", lambda *a: pytest.fail("claimed account executed"))
    overrides = {str(filters): str(relocated)}
    assert runner.run(path, moved_target, out, resume=True, filter_root_overrides=overrides)["complete"]
    assert snapshot(out / "accounts") == saved
    assert open_dataset(out)["contract"] == partial["contract"]
    before = snapshot(out)
    assert runner.run(path, moved_target, out, filter_root_overrides=overrides)["no_op"]
    assert snapshot(out) == before
    for a in partial["contract"]["accounts"]:
        assert len(a["entry_filter_inputs"]["sources"]) == 2


@pytest.mark.parametrize("case", ["unknown", "relative", "blank", "off", "new", "conflict"])
def test_override_refusals_before_io(packs, tmp_path, monkeypatch, case):
    target, filters = packs; raw = fixed_request(filters); items = effective(raw)
    existing = case != "new"
    overrides = {str(filters): str(tmp_path / "moved")}
    if case == "unknown": overrides = {str(tmp_path / "unknown"): str(tmp_path / "moved")}
    elif case == "relative": overrides[str(filters)] = "relative"
    elif case == "blank": overrides[str(filters)] = ""
    elif case == "off": items = []
    elif case == "conflict":
        items[0]["source"]["pack_root"] = "c:/research/filter-pack"
        overrides = {"C:\\Research\\Filter-Pack": str(tmp_path / "a"), "c:/research/filter-pack": str(tmp_path / "b")}
    with pytest.raises(ValueError):
        runtime_roots(items, overrides, existing=existing, output=tmp_path / "out", target=target)
    assert not (tmp_path / "out").exists()


def test_foreign_saved_locator_and_raw_windows_override(packs, tmp_path, monkeypatch):
    from core.entry_filters import canonical
    target, filters = packs; raw = fixed_request(filters); path = save_request(tmp_path, raw); out = tmp_path / "out"
    runner.run(path, target, out, preflight_only=True)
    manifest = open_dataset(out, allow_partial=True); account = manifest["contract"]["accounts"][0]
    prepared = runner.replay(account, target, effective(raw))
    # Model a preflight authored on another host, retaining identical consumed facts.
    foreign = "C:\\Research\\Filter-Pack" if not Path("C:/").is_absolute() else "/foreign/filter-pack"
    raw["generation"]["execution"]["entryFilters"]["items"][0]["source"]["pack_root"] = foreign
    cfg, resolved = resolve(raw); items = resolved["entry_filters"]
    envelope = deepcopy(account["entry_filter_inputs"])
    envelope["effective"] = items
    for pin, item in zip(envelope["sources"], items): pin["descriptor"] = item
    prepared = replace(prepared, entry_filter_inputs=envelope,
        entry_filter_context=replace(prepared.entry_filter_context, effective_json=canonical(items)))
    plan, _, params, candidate = runner.build_execution(cfg, resolved["parameter_sets"][0], prepared, items)
    account.update(entry_filter_inputs=envelope, plan_fingerprint=plan.plan_fingerprint, candidate=candidate)
    account["account_id"] = canonical_sha256(account_identity(raw, account))
    manifest["contract"].update(request=raw, resolved=resolved)
    manifest["request_sha256"] = canonical_sha256(raw); manifest["contract_sha256"] = contract_identity(manifest["contract"])
    atomic_write_json(out / "manifest.partial.json", manifest); save_request(tmp_path, raw)
    before = snapshot(out)
    from core import data_input
    with monkeypatch.context() as patch:
        patch.setattr(data_input, "replay_input", lambda *a, **k: pytest.fail("foreign locator reached core I/O"))
        with pytest.raises(ValueError, match="foreign filter locator"):
            runner.run(path, target, out, resume=True)
    assert snapshot(out) == before
    assert runner.run(path, target, out, resume=True, filter_root_overrides={foreign: str(filters)})["complete"]
    # Windows keys normalize even on a host where this case used a POSIX foreign source.
    win_items = deepcopy(items); win_items[0]["source"]["pack_root"] = "c:/research/filter-pack"
    normalized = runtime_roots(win_items, {"C:\\Research\\Filter-Pack": str(filters)}, existing=True, output=tmp_path / "other", target=target)
    assert normalized == {"c:/research/filter-pack": str(filters.resolve())}


@pytest.mark.parametrize("override", [False, True])
def test_filter_output_overlap_refused(packs, tmp_path, override):
    target, filters = packs; raw = fixed_request(filters); path = save_request(tmp_path, raw)
    if not override:
        with pytest.raises(ValueError, match="overlap"):
            runner.run(path, target, filters / "output")
    else:
        out = tmp_path / "out"; runner.run(path, target, out, preflight_only=True)
        before = snapshot(out)
        with pytest.raises(ValueError, match="overlap"):
            runner.run(path, target, out, resume=True, filter_root_overrides={str(filters): str(out / "source")})
        assert snapshot(out) == before


@pytest.mark.parametrize("failure", ["prefix", "empty", "closure"])
def test_invalid_filter_sources_refuse_preflight(packs, tmp_path, failure):
    root = tmp_path / "filters"
    if failure == "prefix": publish_filter(root, start="2024-03-01")
    elif failure == "empty": publish_filter(root, drop=("2024-01-10T00:00Z", "2024-04-01T00:00Z"))
    else: publish_filter(root, closed="2024-03-02T00:00Z")
    out = tmp_path / "out"
    with pytest.raises(ValueError): runner.run(save_request(tmp_path, fixed_request(root)), packs[0], out)
    assert not (out / "manifest.json").exists()


def test_gapped_filter_valid_but_consumed_gap_edit_refused(packs, tmp_path):
    filters = publish_filter(tmp_path / "gap", drop=("2024-03-10T00:00Z", "2024-03-11T00:00Z"))
    raw = fixed_request(filters); path = save_request(tmp_path, raw); out = tmp_path / "out"
    assert runner.run(path, packs[0], out)["complete"]
    before = snapshot(out)
    repaired = publish_filter(tmp_path / "repaired"); shutil.copytree(repaired, filters, dirs_exist_ok=True)
    with pytest.raises(ValueError, match="Pinned filter input changed"): runner.run(path, packs[0], out)
    assert snapshot(out) == before


@pytest.mark.parametrize("timeframe", [30, 240])
def test_same_root_filter_transport(packs, tmp_path, timeframe):
    raw = fixed_request(packs[0], version=1, n=2, timeframe=timeframe)
    out = tmp_path / "out"
    assert runner.run(save_request(tmp_path, raw), packs[0], out)["complete"]
    account = open_dataset(out)["contract"]["accounts"][0]
    assert account["timeframe_minutes"] == 60
    assert account["entry_filter_inputs"]["effective"][0]["source"]["timeframe_minutes"] == timeframe


def test_filter_changes_between_freeze_and_consumption_stop_series(packs, tmp_path, monkeypatch):
    source = tmp_path / "source"; shutil.copytree(packs[1], source)
    changed = publish_filter(tmp_path / "changed", change="2024-01-15T00:00Z")
    raw = fixed_request(source)
    raw["generation"]["market_data"]["instruments"].append({"instrument_id": "BYBIT_BBBUSDT", "cell": "other"})
    raw["generation"]["expected_run_count"] = 2
    replay = runner.replay
    def consume(*args):
        shutil.copytree(changed, source, dirs_exist_ok=True)
        return replay(*args)
    monkeypatch.setattr(runner, "replay", consume)
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="Pinned filter input changed"):
        runner.run(save_request(tmp_path, raw), packs[0], out)
    manifest = open_dataset(out, allow_partial=True)
    assert not manifest["completed"] and manifest["failure"]["account_id"] == manifest["contract"]["accounts"][0]["account_id"]
    assert not (out / "manifest.json").exists()
