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


def test_native_locator_lexical_classification(tmp_path):
    import ntpath
    import os
    from core.entry_filters import source_locator
    from tools.strategy_lab.filters import native_locator, protect_output
    for value in (r'\\server\share\filters','//server/share/filters',r'C:\Research\Filters','//tmp/Data',str(tmp_path)):
        # Classify using the running host's ntpath semantics, as core does.
        windows=ntpath.isabs(value) and bool(ntpath.splitdrive(value)[0] or value.startswith('\\\\'))
        expected=(os.name=='nt') if windows else Path(value).is_absolute()
        assert native_locator(value)==expected
        assert native_locator(source_locator(value))==expected
    for root in (tmp_path/'native', tmp_path/'native/child'):
        with pytest.raises(ValueError,match='overlaps'):protect_output(tmp_path/'native', [root])


@pytest.mark.skipif(Path('C:/').is_absolute(),reason='POSIX foreign-destination and metadata behavior requires POSIX')
def test_posix_foreign_roots_and_override_destinations(tmp_path):
    from core.entry_filters import source_locator
    from tools.strategy_lab.filters import protect_output
    for foreign in (r'\\server\share\filters','//server/share/filters',r'C:\Research\Filters'):
        original=source_locator(foreign);items=[dict(source=dict(pack_root=original))]
        with pytest.raises(ValueError,match='foreign filter locator'):
            runtime_roots(items,None,existing=True,output=tmp_path/'out',target=tmp_path/'data')
        with pytest.raises(ValueError,match='native absolute'):
            runtime_roots(items,{foreign:foreign},existing=True,output=tmp_path/'out',target=tmp_path/'data')
        assert runtime_roots(items,{foreign:str(tmp_path/'native')},existing=True,output=tmp_path/'out',target=tmp_path/'data')=={original:str(tmp_path/'native')}
        # A foreign UNC must not become local /server/share during offline overlap checks.
        protect_output('/server/share/filters/report',[foreign])


@pytest.mark.parametrize('complete',[False,True])
def test_real_fixed_v2_live_plan_drift_refuses_before_writes(packs,tmp_path,monkeypatch,complete):
    import core.grid_v2 as grid
    target,filters=packs;raw=fixed_request(filters);path=save_request(tmp_path,raw);out=tmp_path/'out'
    runner.run(path,target,out,preflight_only=not complete)
    frozen=open_dataset(out,allow_partial=True)['contract']['accounts'][0]
    cfg,resolved=resolve(raw);items=effective(raw)
    before=snapshot(out)
    monkeypatch.setattr(grid,'GRID_V2_PLAN_IDENTITY_SCHEMA_VERSION','grid_v2_plan_identity_test_drift')
    prepared=runner.replay(frozen,target,items)
    plan,_,params,candidate=runner.build_execution(cfg,resolved['parameter_sets'][0],prepared,items)
    assert params==frozen['params'] and candidate==frozen['candidate']
    assert plan.plan_fingerprint!=frozen['plan_fingerprint']
    def forbidden(*a,**k):pytest.fail('live plan drift reached execution or publication')
    monkeypatch.setattr(runner,'execute_account',forbidden)
    monkeypatch.setattr(runner,'atomic_write_json',forbidden)
    monkeypatch.setattr(runner,'write_index',forbidden)
    with pytest.raises(ValueError,match='replayed live singleton plan differs from frozen account'):
        runner.run(path,target,out,resume=not complete)
    assert snapshot(out)==before


@pytest.mark.parametrize('n',[4,8])
def test_fixed_index_work_is_linear_through_run_resume(packs,tmp_path,monkeypatch,n):
    import tools.strategy_lab.filters as filters_module
    raw=fixed_request(packs[1]);g=raw['generation'];ps=g['parameter_sets'][0]
    g['parameter_sets']=[dict(ps,name='set'+str(i),params=dict(ps['params'],riskPerTrade=1.+i)) for i in range(n)]
    g['expected_run_count']=n;path=save_request(tmp_path,raw);out=tmp_path/'out'
    reads=[];summaries=[];read=Path.read_text;summary=filters_module.summary;execute=runner.execute_account
    def reading(p,*a,**k):
        if p.name=='result.json' and p.is_relative_to(out):reads.append(p)
        return read(p,*a,**k)
    def summarizing(*a,**k):summaries.append(True);return summary(*a,**k)
    calls=[]
    def interrupted(*a,**k):
        calls.append(True)
        if len(calls)==n//2+1:raise KeyboardInterrupt('half complete')
        return execute(*a,**k)
    monkeypatch.setattr(Path,'read_text',reading);monkeypatch.setattr(filters_module,'summary',summarizing)
    monkeypatch.setattr(runner,'execute_account',interrupted)
    with pytest.raises(KeyboardInterrupt,match='half complete'):runner.run(path,packs[0],out)
    # Two required validation/HTML reads plus one index projection per newly committed account.
    assert len(reads)==3*(n//2)
    assert len(summaries)==n+n//2  # One static index summary per declared account, plus account HTML.
    reads.clear();summaries.clear();monkeypatch.setattr(runner,'execute_account',execute)
    assert runner.run(path,packs[0],out,resume=True)['complete']
    assert len(reads)==2*(n//2)+3*(n-n//2)  # Resumed claim validation + one scalar projection.
    assert len(summaries)==n+(n-n//2)
    from tools.strategy_lab.fixed.report import index_rows
    m=open_dataset(out);rows=index_rows(out,m)
    assert all(r['status']=='complete' and r['reason']=='' for r in rows)
    assert any(r['total_trades']>0 for r in rows)


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
    def fail(root, manifest, facts=None):
        if manifest["completed"]: raise KeyboardInterrupt("final claim saved")
        writer(root, manifest, facts)
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


@pytest.mark.parametrize('spelling', ['drive', 'raw_unc', 'normalized_unc'])
def test_foreign_saved_locator_and_raw_windows_override(packs, tmp_path, monkeypatch, spelling):
    from core.entry_filters import canonical
    target, filters = packs; raw = fixed_request(filters); path = save_request(tmp_path, raw); out = tmp_path / "out"
    runner.run(path, target, out, preflight_only=True)
    manifest = open_dataset(out, allow_partial=True); account = manifest["contract"]["accounts"][0]
    prepared = runner.replay(account, target, effective(raw))
    # Model a preflight authored on another host, retaining identical consumed facts.
    if Path('C:/').is_absolute() and spelling!='drive':
        pytest.skip('foreign UNC replay requires POSIX; Windows lexical acceptance is tested separately')
    foreign = {'drive':"C:\\Research\\Filter-Pack", 'raw_unc':r'\\server\share\filters',
        'normalized_unc':'//server/share/filters'}[spelling] if not Path("C:/").is_absolute() else "/foreign/filter-pack"
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
