"""Explicit generic attribution, real hash inputs, and existing generation guards."""
import hashlib
import ast
from pathlib import Path
import subprocess
import sys

import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.account_study import runner, job
from tools.pattern_lab.selection import engine
from tools.pattern_lab.study import extensions, execution_sources, bracket
from tools.pattern_lab.study.contracts import semantic_digest

REPO = Path(__file__).resolve().parents[2]
GENERIC = {"src/core/backtest_engine.py"} | {
    f"src/core/engine_v2/{name}.py" for name in (
        "kernel", "sizing", "contracts", "price_rounding", "execution_modes",
        "diagnostics", "admission", "quantity")}
HELPER = "tools/pattern_lab/study/execution_sources.py"
ROUTES = ("bracket", "account", "selection")


def sources(route):
    if route == "bracket": return extensions.bracket_source_digests()
    if route == "account": return runner.implementation()['sources']
    return engine.implementation(False)['sources']


@pytest.mark.parametrize('route', ROUTES)
def test_membership_hash_convention_and_order(route):
    result = sources(route)
    helper = 'study/execution_sources.py' if route == 'selection' else HELPER
    assert GENERIC == set(execution_sources.GENERIC_EXECUTION_PATHS)
    assert GENERIC | {helper} <= result.keys()
    assert list(result) == sorted(result)
    if route == 'bracket':
        assert set(result) == GENERIC | {HELPER} | {
            f'tools/pattern_lab/study/{n}.py' for n in ('bracket','bracket_rules','sequential','sequential_checks')}
    for key in GENERIC | {helper}:
        raw = (REPO / (HELPER if key == helper else key)).read_bytes()
        assert b'\r' not in raw  # Includes the helper while it is still untracked.
        assert result[key] == hashlib.sha256(raw.replace(b'\r\n',b'\n') if route == 'selection' else raw).hexdigest()


@pytest.mark.parametrize('route', ROUTES)
@pytest.mark.parametrize('name', sorted(GENERIC | {HELPER}))
def test_missing_required_path_names_the_source(route, name, monkeypatch):
    original = Path.is_file
    monkeypatch.setattr(Path, 'is_file', lambda p: False if p == REPO/name else original(p))
    with pytest.raises(PatternLabDataError, match=name.replace('.', r'\.')):
        sources(route)


@pytest.mark.parametrize('route', ROUTES)
@pytest.mark.parametrize('name', ['src/core/engine_v2/quantity.py','src/core/engine_v2/admission.py',HELPER])
def test_actual_source_bytes_change_fingerprint(route, name, tmp_path, monkeypatch):
    before = sources(route)
    changed = tmp_path/'changed.py'
    changed.write_bytes((REPO/name).read_bytes()+b'\n# attribution mutation\n')
    if route == 'selection':
        original = Path.read_bytes
        monkeypatch.setattr(Path,'read_bytes',lambda p: original(changed if p == REPO/name else p))
    else:
        module, attr = (extensions,'file_digest') if route == 'bracket' else (runner,'file_hash')
        original = getattr(module,attr)
        monkeypatch.setattr(module,attr,lambda p: original(changed if p == REPO/name else p))
    after = sources(route)
    key = 'study/execution_sources.py' if name == HELPER and route == 'selection' else name
    assert {k for k in before if before[k] != after[k]} == {key}
    assert semantic_digest(before) != semantic_digest(after)


@pytest.mark.parametrize('route', ROUTES)
def test_existing_raw_vs_lf_hash_conventions(route, tmp_path, monkeypatch):
    name = 'src/core/engine_v2/admission.py'
    changed = tmp_path/'crlf.py'
    raw = (REPO/name).read_bytes().replace(b'\n',b'\r\n')
    changed.write_bytes(raw)
    if route == 'selection':
        original = Path.read_bytes
        monkeypatch.setattr(Path,'read_bytes',lambda p: original(changed if p == REPO/name else p))
    else:
        module, attr = (extensions,'file_digest') if route == 'bracket' else (runner,'file_hash')
        original = getattr(module,attr)
        monkeypatch.setattr(module,attr,lambda p: original(changed if p == REPO/name else p))
    expected = raw.replace(b'\r\n',b'\n') if route == 'selection' else raw
    assert sources(route)[name] == hashlib.sha256(expected).hexdigest()


@pytest.mark.parametrize('name', ['src/core/engine_v2/diagnostics.py','src/core/engine_v2/admission.py',
                                'src/core/engine_v2/quantity.py',HELPER])
def test_new_account_sources_use_existing_job_guard(name, tmp_path, monkeypatch):
    expected = runner.implementation()['sources']
    job.check_generation(expected)
    changed = tmp_path/'changed.py';changed.write_bytes((REPO/name).read_bytes()+b'\n# changed\n')
    original = job.file_hash
    monkeypatch.setattr(job,'file_hash',lambda p: original(changed if p == REPO/name else p))
    with pytest.raises(PatternLabDataError,match='account source generation changed'):
        job.check_generation(expected)


def test_account_guarded_reference_loader_still_used(monkeypatch):
    def refused(): raise PatternLabDataError('guarded reference origin refused')
    monkeypatch.setattr(bracket,'reference_core',refused)
    with pytest.raises(PatternLabDataError,match='guarded reference origin refused'):
        runner.implementation()


def test_inventory_import_is_pure():
    # The existing study package initializer imports extensions. Check this
    # helper's dependency boundary separately from that pre-existing behavior.
    tree = ast.parse((REPO/HELPER).read_text(encoding='utf-8'))
    assert not any(isinstance(n,ast.Import) for n in ast.walk(tree))
    assert {(n.level,n.module,tuple(a.name for a in n.names)) for n in ast.walk(tree)
            if isinstance(n,ast.ImportFrom)} == {(0,'pathlib',('Path',)),(2,None,('PatternLabDataError',))}
    script = """
import sys
from pathlib import Path
from tools.pattern_lab.study.execution_sources import execution_source_paths
assert len(execution_source_paths(Path.cwd())) == 10
assert not any(n == 'core' or n.startswith('core.') for n in sys.modules)
assert 'tools.pattern_lab.account_study.runner' not in sys.modules
assert 'tools.pattern_lab.selection.engine' not in sys.modules
"""
    subprocess.run([sys.executable,'-B','-c',script],cwd=REPO,check=True)
