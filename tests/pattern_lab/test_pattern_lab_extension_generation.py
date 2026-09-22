"""Ordinary local imports, executed source bytes and failed-attempt ownership."""
import builtins
import importlib
import os
from pathlib import Path
import py_compile
import sys

import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.study import contracts, extensions, spec


@pytest.fixture(autouse=True)
def isolated_imports(tmp_path, monkeypatch):
    before = set(sys.modules)
    for name in ("_LOADED", "_VERIFIED_MODULES", "_VERIFIED_IMPORTS"):
        monkeypatch.setattr(extensions, name, dict(getattr(extensions, name)))
    monkeypatch.setattr(contracts, "_REGISTRY", {k: dict(v) for k, v in contracts._REGISTRY.items()})
    yield
    for name in set(sys.modules) - before:
        origin = getattr(sys.modules[name], "__file__", None)
        if origin and tmp_path in Path(origin).resolve().parents:
            sys.modules.pop(name, None)


def declaration(root, module="checked_extension", helpers=()):
    return spec.ExtensionDeclaration(module=module, source_root=str(root), helpers=tuple(helpers))


@pytest.mark.parametrize("style", ["namespace", "scalar"])
@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("declared", [False, True])
def test_ordinary_imports_and_unknown_cached_generation(tmp_path, monkeypatch, style, cached, declared):
    helper = tmp_path/"checked_helper.py"
    helper.write_text("VALUE = 1\n")
    statement = "import checked_helper\nVALUE = checked_helper.VALUE" if style == "namespace" else "from checked_helper import VALUE"
    (tmp_path/"checked_extension.py").write_text(statement + "\ndef register(context): pass\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    prior = importlib.import_module("checked_helper") if cached else None
    request = declaration(tmp_path, helpers=[helper.name] if declared else [])
    hooks = (builtins.__import__, list(sys.meta_path), list(sys.path))
    if declared and not cached:
        records = extensions.load_extensions([request])
        assert sys.modules["checked_extension"].VALUE == 1
        assert extensions.load_extensions([request]) == records
        assert dict(records[0].files)[helper.name] == extensions.file_digest(helper)
    else:
        match = "runtime source generation.*fresh interpreter" if declared else "undeclared local helper"
        with pytest.raises(PatternLabDataError, match=match):
            extensions.load_extensions([request])
        assert "checked_extension" not in sys.modules
        assert "checked_extension" not in extensions._LOADED
        if cached:
            assert sys.modules["checked_helper"] is prior
        else:
            assert "checked_helper" not in sys.modules
            # Correcting the declaration is a usable same-interpreter retry.
            extensions.load_extensions([declaration(tmp_path, helpers=[helper.name])])
            assert sys.modules["checked_extension"].VALUE == 1
    assert (builtins.__import__, sys.meta_path, sys.path) == hooks


def test_cached_declared_helper_cannot_claim_new_disk_bytes(tmp_path, monkeypatch):
    helper = tmp_path/"checked_helper.py"
    helper.write_text("VALUE = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    old = importlib.import_module("checked_helper")
    helper.write_text("VALUE = 2\n")
    (tmp_path/"checked_extension.py").write_text("from checked_helper import VALUE\ndef register(context): pass\n")
    with pytest.raises(PatternLabDataError, match="checked_helper.*runtime source generation.*fresh interpreter"):
        extensions.load_extensions([declaration(tmp_path, helpers=[helper.name])])
    assert old.VALUE == 1
    assert "checked_extension" not in sys.modules


@pytest.mark.parametrize("package", [False, True])
def test_cached_helper_from_another_root_is_not_current_attribution(tmp_path, monkeypatch, package):
    old, chosen = tmp_path/"old", tmp_path/"chosen"
    old.mkdir()
    chosen.mkdir()
    relative = "checked_helper/__init__.py" if package else "checked_helper.py"
    for root, value in ((old, 1), (chosen, 2)):
        path = root/relative
        path.parent.mkdir(exist_ok=True)
        path.write_text(f"VALUE = {value}\n")
    monkeypatch.syspath_prepend(str(old))
    cached = importlib.import_module("checked_helper")
    (chosen/"checked_extension.py").write_text("from checked_helper import VALUE\ndef register(context): pass\n")
    with pytest.raises(PatternLabDataError, match="cached from.*not chosen source.*fresh interpreter"):
        extensions.load_extensions([declaration(chosen, helpers=[relative])])
    assert sys.modules["checked_helper"] is cached and cached.VALUE == 1
    assert "checked_extension" not in sys.modules


@pytest.mark.parametrize("cached", [False, True])
def test_transitive_helper_coverage_including_verified_cached_import(tmp_path, cached):
    (tmp_path/"checked_leaf.py").write_text("VALUE = 3\n")
    (tmp_path/"checked_helper.py").write_text("from checked_leaf import VALUE\n")
    body = "from checked_helper import VALUE\ndef register(context): pass\n"
    (tmp_path/"checked_extension.py").write_text(body)
    if cached:
        (tmp_path/"accepted_extension.py").write_text(body)
        accepted = declaration(tmp_path, "accepted_extension", ["checked_helper.py", "checked_leaf.py"])
        extensions.load_extensions([accepted])
    with pytest.raises(PatternLabDataError, match="undeclared local helper checked_leaf.py"):
        extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py"])])
    assert "checked_extension" not in sys.modules
    if cached:
        assert sys.modules["accepted_extension"].VALUE == 3
        assert extensions.load_extensions([accepted])
    extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py", "checked_leaf.py"])])
    assert sys.modules["checked_extension"].VALUE == 3


@pytest.mark.parametrize("stage", ["import", "registration", "helper_check"])
@pytest.mark.parametrize("failure", ["RuntimeError", "KeyboardInterrupt"])
def test_failure_restores_owned_modules_registry_and_hooks(tmp_path, monkeypatch, stage, failure):
    (tmp_path/"unrelated.py").write_text("VALUE = 17\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    unrelated = importlib.import_module("unrelated")
    (tmp_path/"checked_helper.py").write_text("VALUE = 1\n")
    body = "from checked_helper import VALUE\nfrom tools.pattern_lab.study import contracts\n"
    if stage == "import":
        body += f"raise {failure}('original cause')\n"
    body += "def register(context):\n    context.register_hypothesis(contracts.HypothesisDescriptor('owned_failure', '1', lambda *args: None))\n"
    if stage == "registration":
        body += f"    raise {failure}('original cause')\n"
    (tmp_path/"checked_extension.py").write_text(body)
    hooks = (builtins.__import__, list(sys.meta_path), list(sys.path))
    registry = {k: dict(v) for k, v in contracts._REGISTRY.items()}
    if stage == "helper_check":
        def fail(*args, **kwargs):
            raise getattr(builtins, failure)("original cause")
        monkeypatch.setattr(extensions, "verify_extensions", fail)
    with pytest.raises(getattr(builtins, failure), match="original cause"):
        extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py"])])
    assert contracts._REGISTRY == registry
    assert sys.modules["unrelated"] is unrelated
    assert "checked_extension" not in sys.modules and "checked_helper" not in sys.modules
    assert "checked_extension" not in extensions._LOADED
    assert "checked_helper" not in extensions._VERIFIED_MODULES
    assert (builtins.__import__, sys.meta_path, sys.path) == hooks


@pytest.mark.parametrize("target", ["main", "helper"])
def test_timestamp_valid_stale_bytecode_is_never_executed(tmp_path, target):
    helper = tmp_path/"checked_helper.py"
    main = tmp_path/"checked_extension.py"
    helper.write_text("VALUE = 1\n")
    main.write_text("from checked_helper import VALUE\nOWN = 1\ndef register(context): pass\n")
    changed = main if target == "main" else helper
    old_time = changed.stat()
    pyc = Path(py_compile.compile(str(changed), doraise=True))
    cached_bytes = pyc.read_bytes()
    changed.write_text(changed.read_text().replace("= 1", "= 2"))
    os.utime(changed, ns=(old_time.st_atime_ns, old_time.st_mtime_ns))
    extensions.load_extensions([declaration(tmp_path, helpers=[helper.name])])
    loaded = sys.modules["checked_extension"]
    assert (loaded.OWN if target == "main" else loaded.VALUE) == 2
    assert pyc.read_bytes() == cached_bytes  # No deletion or repair of user caches.
