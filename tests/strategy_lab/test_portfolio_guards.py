"""Saved-reader contradictions, exact dense replay and protected boundaries."""
from copy import deepcopy
import importlib
import json
from pathlib import Path
import shutil

import numpy as np
import pyarrow.parquet as pq
import pytest

from _selection_helpers import selection_family, document, publish
from _fixed_helpers import snapshot
from _portfolio_helpers import request, check_report
from tools.strategy_lab.portfolio import prepare, run, report, detail, open_plan, open_result
from tools.strategy_lab.portfolio.storage import write_table, ALLOCATION_COLUMNS, TRADE_COLUMNS
from tools.strategy_lab.dataset import artifact_record


@pytest.fixture(scope='module')
def saved(selection_family, tmp_path_factory):
    f = selection_family; work = tmp_path_factory.mktemp('carried-result')
    raw = request(f['rankings']['is_oos'], f['candidates'], family='is_oos', buckets=1)
    prepare(document(work, 'request.json', raw), work/'plan')
    run(work/'plan', f['pack'], work/'result')
    return dict(f=f, work=work, raw=raw)


def test_report_saved_values(saved, tmp_path):
    parent = saved['work']/'result'; before = snapshot(parent)
    m, p, a, _ = open_result(parent)
    assert len(p['calendar']['groups'][0]['windows']) > 1
    assert {v['variant']['input'] for v in m['variants']} == {'off', 'on'}
    report(parent, tmp_path/'report')
    check_report(tmp_path/'report', m, p, a)
    assert snapshot(parent) == before


def test_off_detail_without_other_live_pack(saved, tmp_path):
    import os
    s = saved; parent = s['work']/'result'; before = snapshot(parent)
    original = s['f']['filters'].resolve(); hidden = original.with_name(original.name+'-hidden')
    scratch = Path(os.environ['MERLIN_TEST_ROOT']).resolve()
    assert original.is_relative_to(scratch) and hidden.is_relative_to(scratch)
    assert not original.is_symlink() and not hidden.exists()
    original.rename(hidden)
    try:
        got = detail(parent, 'off-merlin_x', s['f']['pack'], tmp_path/'detail')
        m, _, _, _ = open_result(parent)
        assert got['dense_digest'] == m['variants'][0]['dense_digest']
    finally:
        assert hidden.resolve().is_relative_to(scratch) and not original.exists()
        hidden.rename(original)
    assert snapshot(parent) == before


def test_detail_override_scope_and_protection(saved, tmp_path, monkeypatch):
    s = saved; parent = s['work']/'result'; before = snapshot(parent)
    original = str(s['f']['filters']); unused = str(tmp_path/'unused-missing-pack')
    detail(parent, 'off-merlin_x', s['f']['pack'], tmp_path/'unused-ok', filter_root_overrides={original: unused})
    ex = importlib.import_module('tools.strategy_lab.portfolio.execution')
    def forbidden(*a, **k): pytest.fail('source refusal must precede execution')
    monkeypatch.setattr(ex, 'execute', forbidden)
    with pytest.raises(ValueError, match='unknown filter root override'):
        detail(parent, 'off-merlin_x', s['f']['pack'], tmp_path/'unknown', filter_root_overrides={str(tmp_path/'unknown'): unused})
    with pytest.raises(ValueError, match='native absolute'):
        detail(parent, 'off-merlin_x', s['f']['pack'], tmp_path/'relative', filter_root_overrides={original:'relative'})
    with pytest.raises(ValueError, match='conflicting normalized'):
        detail(parent, 'off-merlin_x', s['f']['pack'], tmp_path/'conflict',
            filter_root_overrides={original:unused, original+'/./':str(tmp_path/'different')})
    for i, root in enumerate((s['f']['filters'], Path(unused))):
        with pytest.raises(ValueError, match='overlaps'):
            detail(parent, 'off-merlin_x', s['f']['pack'], root/('output-'+str(i)), filter_root_overrides={original:unused})
    with pytest.raises(ValueError, match='no pack manifest'):
        detail(parent, 'on-merlin_x', s['f']['pack'], tmp_path/'missing-selected', filter_root_overrides={original:unused})
    # Corruption in the unselected variant must still refuse at the complete parent reader.
    clone = tmp_path/'corrupt'; shutil.copytree(parent, clone)
    (clone/'curves/on-merlin_x.parquet').write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='artifact hash'):
        detail(clone, 'off-merlin_x', s['f']['pack'], tmp_path/'corrupt-detail')
    assert snapshot(parent) == before


def test_detail_foreign_unselected_locator(saved, tmp_path, monkeypatch):
    from core.entry_filters import source_locator
    s = saved; parent = s['work']/'result'; before = snapshot(parent)
    module = importlib.import_module('tools.strategy_lab.portfolio.run'); real = module.open_result
    original = source_locator(str(s['f']['filters']))
    foreign = 'c:/foreign/filter-pack' if not Path('C:/').is_absolute() else '/foreign/filter-pack'
    def foreign_locator(path):
        m, p, a, t = real(path)  # Validate the entire genuine saved parent before the locator-only seam.
        p = json.loads(json.dumps(p).replace(original, foreign))
        return m, p, a, t
    monkeypatch.setattr(module, 'open_result', foreign_locator)
    detail(parent, 'off-merlin_x', s['f']['pack'], tmp_path/'off')
    with pytest.raises(ValueError, match='foreign filter locator'):
        detail(parent, 'on-merlin_x', s['f']['pack'], tmp_path/'missing-override')
    # Real selected F06 relocation/reproduction is covered below with original pins intact.
    assert snapshot(parent) == before


@pytest.mark.parametrize('family', ['fixed', 'is_oos'])
def test_preflight_replay_counts_and_all_windows(selection_family, tmp_path, monkeypatch, family):
    f = selection_family
    raw = request(f['rankings'][family], f['fixed' if family=='fixed' else 'candidates'], family=family)
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    p = open_plan(tmp_path/'plan'); cal = p['calendar']
    assert len(cal['groups']) > 1
    if family == 'is_oos':
        cell = p['ledger']['off'][-1]
        cell.update(candidate_id=None, status='abstained', value=0., semantic_key=None)
    ex = importlib.import_module('tools.strategy_lab.portfolio.execution')
    source, reference = ex.source_context, ex.reference
    contexts, windows = [], []
    def context(p, label, ticker, ws, *args):
        contexts.append((label,ticker)); ctx = source(p,label,ticker,ws,*args)
        ctx['test_label'] = label; return ctx
    def ref(ctx,ticker,w,*args):
        windows.append((ctx['test_label'],ticker,w['id']))
        return reference(ctx,ticker,w,*args)
    monkeypatch.setattr(ex,'source_context',context); monkeypatch.setattr(ex,'reference',ref)
    for label in (None, 'off'):
        contexts.clear(); windows.clear()
        count = ex.verify_live(p, f['pack'], {}, label)
        labels = list(p['sources']) if label is None else [label]
        expected = len(labels)*len(cal['roster'])*(len(cal['groups']) if family=='fixed' else 1)
        assert count == len(contexts) == expected
        assert sorted(windows) == sorted((l,t,w['id']) for l in labels for t in cal['roster'] for w in cal['windows'])


@pytest.mark.parametrize('mode', ['run', 'detail'])
@pytest.mark.parametrize('failure', ['later_label', 'late_window'])
def test_preflight_refuses_before_first_engine(saved, tmp_path, monkeypatch, mode, failure):
    s = saved; ex = importlib.import_module('tools.strategy_lab.portfolio.execution')
    p = open_plan(s['work']/'plan'); last = p['calendar']['windows'][-1]['id']; ticker = p['calendar']['roster'][-1]
    real_source, real_ref = ex.source_context, ex.reference
    def source(p,label,*args):
        if failure=='later_label' and label=='on': raise ValueError('required later input unavailable')
        return real_source(p,label,*args)
    def reference(ctx,t,w,*args):
        if failure=='late_window' and t==ticker and w['id']==last: raise ValueError('required late window invalid')
        return real_ref(ctx,t,w,*args)
    def forbidden(*args,**kwargs): pytest.fail('engine ran before full required preflight')
    monkeypatch.setattr(ex,'source_context',source); monkeypatch.setattr(ex,'reference',reference)
    monkeypatch.setattr(ex,'execute',forbidden)
    with pytest.raises(ValueError,match='required (later input|late window)'):
        if mode=='run': run(s['work']/'plan',s['f']['pack'],tmp_path/'refused')
        else: detail(s['work']/'result','on-merlin_x',s['f']['pack'],tmp_path/'refused')
    assert not (tmp_path/'refused/result.json').exists() and not (tmp_path/'refused/detail.json').exists()


def test_at_use_replay_still_checks_after_preflight(saved, tmp_path, monkeypatch):
    ex = importlib.import_module('tools.strategy_lab.portfolio.execution'); real = ex.verify_live
    def drift(*args, **kwargs):
        count = real(*args, **kwargs)
        import tools.strategy_lab.parquet as parquet
        def changed(*a, **k): raise ValueError('at-use source drift')
        monkeypatch.setattr(parquet, 'replay', changed)
        return count
    monkeypatch.setattr(ex, 'verify_live', drift)
    with pytest.raises(ValueError, match='at-use source drift'):
        detail(saved['work']/'result', 'off-merlin_x', saved['f']['pack'], tmp_path/'detail')
    assert not (tmp_path/'detail/detail.json').exists()


def test_sampling_identity_offline_and_runtime_spies(saved, tmp_path, monkeypatch):
    s = saved; parent = s['work']/'result'; before = snapshot(parent)
    m, p, a, t = open_result(parent)
    import core.grid_engine as grid
    import core.ticker_selector as selector
    import core.walkforward_engine as wf
    import core.storage as storage
    def forbidden(*args, **kwargs):
        raise AssertionError('optimization/ranking/persistence is forbidden')
    monkeypatch.setattr(grid, 'run_grid_optimization', forbidden)
    monkeypatch.setattr(selector, 'rank_pool', forbidden)
    monkeypatch.setattr(wf.WalkForwardEngine, 'run_window', forbidden)
    monkeypatch.setattr(storage, 'save_grid_study_to_db', forbidden)
    raw = deepcopy(s['raw']); raw['curveBuckets'] = 64
    prepare(document(tmp_path, 'request.json', raw), tmp_path/'plan')
    assert open_plan(tmp_path/'plan')['plan_identity'] == p['plan_identity']
    run(tmp_path/'plan', s['f']['pack'], tmp_path/'result')
    other, _, a2, t2 = open_result(tmp_path/'result')
    assert a == a2 and t == t2 and m['execution_identity'] == other['execution_identity']
    for v, v2 in zip(m['variants'], other['variants']):
        assert v['metrics'] == v2['metrics'] and v['dense_digest'] == v2['dense_digest']
        assert v['curve_policy']['saved_count'] < v2['curve_policy']['saved_count']
    import tools.strategy_lab.config as config
    import core.data_input as data
    monkeypatch.setattr(config, '_validate_strategy', forbidden)
    monkeypatch.setattr(data, 'replay_input', forbidden)
    # Only saved-result paths can be read; original input datasets/packs are unavailable.
    original = Path.read_bytes
    def read(path):
        if path.is_relative_to(s['f']['work']):
            raise AssertionError('offline reader accessed hidden inputs')
        return original(path)
    monkeypatch.setattr(Path, 'read_bytes', read)
    report(parent, tmp_path/'report')
    assert snapshot(parent) == before


@pytest.mark.parametrize('kind', ['hash', 'carry', 'trade', 'duplicate', 'status', 'schema', 'path'])
def test_reader_rejects_corruption_and_resealed_contradictions(saved, tmp_path, kind):
    root = tmp_path/'copy'; shutil.copytree(saved['work']/'result', root)
    m, _, a, t = open_result(root)
    if kind == 'hash':
        (root/'trades.parquet').write_bytes(b'bad')
    elif kind in ('carry', 'duplicate', 'status'):
        if kind == 'carry': a[1]['opening'] += 1.
        elif kind == 'status': a[0]['status'] = 'abstained'
        else: a.append(deepcopy(a[0]))
        write_table(root, 'allocations.parquet', a, ALLOCATION_COLUMNS)
        m['artifacts']['allocations.parquet'] = artifact_record(root/'allocations.parquet', root)
    elif kind == 'trade':
        t[0]['net_pnl'] += 10.
        write_table(root, 'trades.parquet', t, TRADE_COLUMNS)
        m['artifacts']['trades.parquet'] = artifact_record(root/'trades.parquet', root)
    elif kind == 'schema': m['schema_version'] = 'future'
    else: m['variants'][0]['curve_file'] = '../escape.parquet'
    document(root, 'result.json', m)
    with pytest.raises(ValueError):
        open_result(root)


def test_detail_detects_omitted_native_sample(saved, tmp_path, monkeypatch):
    s = saved; parent = s['work']/'result'; m, _, _, _ = open_result(parent)
    variant = m['variants'][0]
    included = set(pq.read_table(parent/variant['curve_file'])['timestamp_ns'].to_pylist())
    engine = importlib.import_module('tools.strategy_lab.portfolio.execution')
    real = engine.execute_variant
    def changed(*args, **kwargs):
        facts, a, t, (ts, b, e) = real(*args, **kwargs)
        index = next(i for i in range(1, len(ts)-1) if int(ts[i]) not in included and e[i] < max(e) and e[i] > min(e))
        e[index] = np.nextafter(e[index], np.inf)
        from tools.strategy_lab.portfolio.accounting import dense_digest, metrics
        assert metrics(ts, b, e, facts['metrics']['initial']) == facts['metrics']
        facts['dense_digest'] = dense_digest(ts, b, e)
        return facts, a, t, (ts, b, e)
    monkeypatch.setattr(engine, 'execute_variant', changed)
    before = snapshot(parent)
    with pytest.raises(ValueError, match='dense_digest'):
        detail(parent, variant['variant']['id'], s['f']['pack'], tmp_path/'detail')
    assert not (tmp_path/'detail/detail.json').exists() and snapshot(parent) == before


def test_relocation_drift_cancellation_and_new_output(saved, tmp_path, monkeypatch):
    from _filter_helpers import publish_filter
    s = saved; before = snapshot(s['f']['pack']); parent = s['work']/'result'
    shutil.copytree(s['f']['pack'], tmp_path/'pack')
    shutil.copytree(s['f']['filters'], tmp_path/'filters')
    detail(parent, 'on-merlin_x', tmp_path/'pack', tmp_path/'detail',
        filter_root_overrides={str(s['f']['filters']): str(tmp_path/'filters')})
    ex = importlib.import_module('tools.strategy_lab.portfolio.execution')
    def forbidden(*a, **k): pytest.fail('drift must refuse before execution')
    monkeypatch.setattr(ex, 'execute', forbidden)
    publish(tmp_path/'changed', future='2024-04-01')
    with pytest.raises(ValueError):
        detail(parent, 'off-merlin_x', tmp_path/'changed', tmp_path/'drift')
    assert not (tmp_path/'drift/detail.json').exists()
    publish_filter(tmp_path/'changed-filter', start='2023-12-01', end='2024-09-01', change='2024-03-05T00:00:00Z')
    with pytest.raises(ValueError):
        detail(parent, 'on-merlin_x', s['f']['pack'], tmp_path/'filter-drift',
            filter_root_overrides={str(s['f']['filters']): str(tmp_path/'changed-filter')})
    with pytest.raises(ValueError, match='cancelled'):
        run(s['work']/'plan', s['f']['pack'], tmp_path/'cancelled', cancelled=lambda: True)
    with pytest.raises(ValueError):
        run(s['work']/'plan', s['f']['pack'], s['f']['pack']/'overlap')
    with pytest.raises(ValueError):
        run(s['work']/'plan', s['f']['pack'], parent)
    assert snapshot(s['f']['pack']) == before


def test_request_duplicate_and_interrupted_publication(saved, tmp_path, monkeypatch):
    raw = deepcopy(saved['raw']); raw['variants'].append(raw['variants'][0])
    with pytest.raises(ValueError, match='duplicate'):
        prepare(document(tmp_path, 'bad.json', raw), tmp_path/'bad-plan')
    module = importlib.import_module('tools.strategy_lab.portfolio.run')
    original = module.write_table
    def fail(root, name, *args):
        if name == 'trades.parquet': raise ValueError('interrupted')
        return original(root, name, *args)
    monkeypatch.setattr(module, 'write_table', fail)
    with pytest.raises(ValueError, match='interrupted'):
        run(saved['work']/'plan', saved['f']['pack'], tmp_path/'partial')
    assert not (tmp_path/'partial/result.json').exists()
    with pytest.raises(ValueError): open_result(tmp_path/'partial')
