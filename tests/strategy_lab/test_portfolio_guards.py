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
from _portfolio_helpers import request
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


def test_relocation_drift_cancellation_and_new_output(saved, tmp_path):
    from _filter_helpers import publish_filter
    s = saved; before = snapshot(s['f']['pack']); parent = s['work']/'result'
    shutil.copytree(s['f']['pack'], tmp_path/'pack')
    shutil.copytree(s['f']['filters'], tmp_path/'filters')
    detail(parent, 'on-merlin_x', tmp_path/'pack', tmp_path/'detail',
        filter_root_overrides={str(s['f']['filters']): str(tmp_path/'filters')})
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
