"""Shared-provider ownership and generation boundaries introduced by A1."""
import copy
import importlib
import json
import os
import subprocess
import sys

import pytest

from tools import pattern_lab
from tools.pattern_lab import candidate, PatternLabDataError
from tools.pattern_lab.study import extensions
from tools.pattern_lab.analysis import artifacts
from data_provider import READER_MODULES
from data_provider import errors
from . import _helpers as h, _portable


def child(code):
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    return subprocess.run([sys.executable, '-B', '-c', code], cwd=h.REPO_ROOT,
                          env=env, capture_output=True, text=True)


def test_standalone_provider_isolation_and_canonical_identity():
    result = child('''
import sys
sys.path.insert(0, 'src')
def guard(event, args):
    if event.startswith(('socket.', 'urllib.')):
        raise AssertionError(event)
sys.addaudithook(guard)
import data_provider
from data_provider import data, manifest, collect, pack_lock, update_transaction, exchange_data
assert not any(n.startswith(('tools.pattern_lab', 'flask', 'core', 'ui', 'strategies', 'src.data_provider')) for n in sys.modules)
assert not any(n == 'pyarrow.parquet' for n in sys.modules)
import tools.pattern_lab as lab
assert lab.data is data
assert lab.collect is collect
try:
    import src.data_provider
except ImportError:
    pass
else:
    raise AssertionError('duplicate package accepted')
assert 'src.data_provider' not in sys.modules
''')
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('name', ['data_provider', 'data_provider.data'])
def test_foreign_provider_refused(name):
    result = child(f'''
import sys, types
module = types.ModuleType({name!r})
module.__file__ = '/foreign/provider.py'
sys.modules[{name!r}] = module
try:
    import tools.pattern_lab
except ImportError as error:
    assert 'Foreign data_provider' in str(error)
else:
    raise AssertionError('foreign module accepted')
''')
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('name', ['data', 'manifest', 'pack_lock', 'update_transaction', 'exchange_data', 'collect'])
def test_attributes_are_real_modules(name, monkeypatch):
    canonical = importlib.import_module('data_provider.' + name)
    assert getattr(pattern_lab, name) is canonical
    assert 'tools.pattern_lab.' + name not in sys.modules
    if name == 'data':
        def denied(*args, **kwargs):
            raise AssertionError('guard reached canonical read')
        monkeypatch.setattr(canonical, 'load_slice', denied)
        with pytest.raises(AssertionError, match='guard reached canonical read'):
            pattern_lab.data.load_slice('unused')


def test_errors_and_explicit_roster(tmp_path):
    for old, new in [('PatternLabError', 'DataProviderError'), ('PatternLabDataError', 'DataError'),
                     ('PatternLabDependencyError', 'DependencyError'), ('PatternLabBusyError', 'BusyError'),
                     ('PatternLabPendingError', 'PendingError')]:
        assert getattr(pattern_lab, old) is getattr(errors, new)
        assert getattr(errors, new)('message', error_code='custom').error_code == 'custom'
    assert errors.DataProviderError.error_code == 'pattern_lab_error'
    assert issubclass(pattern_lab.PatternLabStudyError, errors.DataProviderError)
    assert type(pattern_lab.PatternLabDataError()).__name__ == 'DataError'
    assert pattern_lab.collect.load_roster(pattern_lab.DEFAULT_ROSTER_PATH)['roster']
    with pytest.raises(PatternLabDataError) as caught:
        pattern_lab.collect.collect_pack(tmp_path / 'absent', start='2020-01-01T00:00:00Z', end='2020-01-02T00:00:00Z')
    assert caught.value.error_code == 'invalid_roster'
    assert not (tmp_path / 'absent').exists()


@pytest.mark.parametrize('module, names, function', [
    (extensions, 'CORE_MODULES', 'core_source_digests'),
    (artifacts, 'ATTRIBUTED_MODULES', 'module_digests'),
])
def test_missing_required_sources_fail(module, names, function, monkeypatch):
    monkeypatch.setattr(module, names, (*getattr(module, names), 'missing_a1_source'))
    with pytest.raises(PatternLabDataError, match='Missing required'):
        getattr(module, function)()


def test_reader_attribution_uses_exact_sources():
    from tools.pattern_lab.account_study.runner import implementation as account_implementation
    from tools.pattern_lab.selection.engine import implementation as selection_implementation
    assert set(n for n in extensions.CORE_MODULES if n.startswith('data_provider')) == set(READER_MODULES)
    assert not set(extensions.CORE_MODULES) & {'tools.pattern_lab.' + n for n in ('data', 'manifest', 'pack_lock', 'update_transaction')}
    expected = {'src/data_provider/__init__.py' if n == 'data_provider' else 'src/' + n.replace('.', '/') + '.py' for n in READER_MODULES}
    for record in (account_implementation(), selection_implementation({})):
        actual = {n for n in record['sources'] if n.startswith('src/data_provider/')}
        assert actual == expected
        assert all(record['sources'][n] == extensions.file_digest(h.REPO_ROOT / n) for n in expected)


@pytest.fixture(scope='module')
def generation(tmp_path_factory):
    root = tmp_path_factory.mktemp('provider-candidate')
    frozen = _portable.frozen_generation(root, module_prefix="provider")
    return root, frozen


def test_policy_two_freeze_execute_and_mutation(generation, tmp_path, monkeypatch):
    root, frozen = generation
    assert frozen['required_code_policy_version'] == 2
    assert set(frozen['required_code']['core']) == set(candidate.REQUIRED_CORE_MODULES)
    candidate.run_validation(candidate=frozen, data_root=root / 'pack', output_root=tmp_path / 'validation')
    candidate.load_validation(tmp_path / 'validation')
    real = extensions.file_digest
    monkeypatch.setattr(extensions, 'file_digest', lambda p: '0' * 64 if p.name == 'errors.py' else real(p))
    with pytest.raises(PatternLabDataError, match='required generation changed'):
        candidate.verify_current_generation(frozen)


@pytest.mark.parametrize('change', ['missing', 'mixed', 'policy', 'unknown'])
def test_candidate_policy_tampering(generation, change):
    frozen = copy.deepcopy(generation[1])
    if change == 'missing':
        frozen['required_code']['core'].pop('data_provider.errors')
    elif change == 'mixed':
        frozen['required_code']['core']['tools.pattern_lab.data'] = '0' * 64
    else:
        frozen['required_code_policy_version'] = 1 if change == 'policy' else 3
    frozen['candidate_id'] = candidate.candidate_identity(frozen)
    with pytest.raises(PatternLabDataError, match='membership|unsupported version'):
        candidate.load_candidate(frozen)


@pytest.mark.parametrize('change', ['missing', 'mixed', 'analysis_only'])
def test_incomplete_saved_generation_never_falls_back(generation, change):
    from tools.pattern_lab.analysis.source import admit_source
    root, frozen = generation
    source = admit_source(root / 'study', model_instances=['fh'])
    analyzed = artifacts.load_analysis(root / 'analysis')
    hashes = source.results.source['core_source']
    if change == 'missing':
        hashes.pop('data_provider.errors')
    elif change == 'mixed':
        hashes['tools.pattern_lab.data'] = '0' * 64
    else:
        analyzed.provenance['implementation']['modules']['data_provider.errors'] = hashes.pop('data_provider.errors')
    with pytest.raises(PatternLabDataError, match='incomplete or mixed'):
        candidate._generation(source, analyzed, frozen['recipe'])
