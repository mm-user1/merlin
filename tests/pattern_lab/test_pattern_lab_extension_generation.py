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


def hook_state():
    return builtins.__import__, importlib.import_module, list(sys.meta_path), list(sys.path)


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
    hooks = hook_state()
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
    assert hook_state() == hooks


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
@pytest.mark.parametrize("route", ["statement", "importlib"])
def test_failure_restores_owned_modules_registry_and_hooks(tmp_path, monkeypatch, stage, failure, route):
    (tmp_path/"unrelated.py").write_text("VALUE = 17\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    unrelated = importlib.import_module("unrelated")
    (tmp_path/"checked_helper.py").write_text("VALUE = 1\n")
    body = ("from checked_helper import VALUE\n" if route == "statement" else
            "import importlib\nVALUE = importlib.import_module('checked_helper').VALUE\n")
    body += "import sys\nsys.path.append('temporary-failed-extension-path')\n"
    body += "from tools.pattern_lab.study import contracts\n"
    if stage == "import":
        body += f"raise {failure}('original cause')\n"
    body += "def register(context):\n    context.register_hypothesis(contracts.HypothesisDescriptor('owned_failure', '1', lambda *args: None))\n"
    if stage == "registration":
        body += f"    raise {failure}('original cause')\n"
    (tmp_path/"checked_extension.py").write_text(body)
    hooks = hook_state()
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
    assert hook_state() == hooks


def test_new_helper_is_found_with_unchanged_directory_mtime(tmp_path):
    assert importlib.machinery.PathFinder.find_spec("checked_helper", [str(tmp_path)]) is None
    stamp = tmp_path.stat()
    (tmp_path/"checked_helper.py").write_text("VALUE = 5\n")
    (tmp_path/"checked_extension.py").write_text("from checked_helper import VALUE\ndef register(context): pass\n")
    os.utime(tmp_path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    assert tmp_path.stat().st_mtime_ns == stamp.st_mtime_ns
    hooks = hook_state()
    records = extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py"])])
    assert sys.modules["checked_extension"].VALUE == 5
    assert dict(records[0].files)["checked_helper.py"] == extensions.file_digest(tmp_path/"checked_helper.py")
    assert hook_state() == hooks


def test_genuinely_missing_declared_helper_is_named(tmp_path):
    (tmp_path/"checked_extension.py").write_text("def register(context): pass\n")
    with pytest.raises(PatternLabDataError, match="declared helper 'missing.py' does not exist"):
        extensions.load_extensions([declaration(tmp_path, helpers=["missing.py"])])
    assert "checked_extension" not in sys.modules
    assert "checked_extension" not in extensions._LOADED


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_cache_invalidation_failure_preserves_cause_and_uninstalled_hooks(tmp_path, monkeypatch, failure):
    (tmp_path/"checked_extension.py").write_text("def register(context): pass\n")
    cause = failure("cache invalidator stopped")
    def fail():
        raise cause
    monkeypatch.setattr(importlib, "invalidate_caches", fail)
    hooks = hook_state()
    with pytest.raises(failure) as caught:
        extensions.load_extensions([declaration(tmp_path)])
    assert caught.value is cause
    assert hook_state() == hooks
    assert "checked_extension" not in sys.modules
    assert "checked_extension" not in extensions._LOADED
    assert "checked_extension" not in extensions._VERIFIED_MODULES


@pytest.mark.parametrize("case,stage,form", [
    ("declared", "module", "scalar"), ("undeclared", "module", "module"),
    ("foreign", "module", "scalar"), ("declared", "registration", "module"),
    ("undeclared", "registration", "scalar"), ("foreign", "registration", "module"),
    ("foreign_package", "module", "module"),
])
def test_cached_importlib_refuses_all_three_generations(tmp_path, monkeypatch, case, stage, form):
    chosen = tmp_path/"chosen"
    chosen.mkdir()
    old = tmp_path/"old" if case.startswith("foreign") else chosen
    old.mkdir(exist_ok=True)
    helper_path = "checked_helper/__init__.py" if case == "foreign_package" else "checked_helper.py"
    path = old/helper_path
    path.parent.mkdir(exist_ok=True)
    path.write_text("VALUE = 1\n")
    monkeypatch.syspath_prepend(str(old))
    cached = importlib.import_module("checked_helper")
    (chosen/helper_path).parent.mkdir(exist_ok=True)
    (chosen/helper_path).write_text("VALUE = 2\n")
    expression = "importlib.import_module('checked_helper')" + (".VALUE" if form == "scalar" else "")
    body = "import importlib\nfrom tools.pattern_lab.study import contracts\n"
    if case == "declared" and stage == "registration":
        body += "from importlib import import_module as load\n"
        expression = expression.replace("importlib.import_module", "load")
    if stage == "module":
        body += f"VALUE = {expression}\ndef register(context): pass\n"
    else:
        body += ("def register(context):\n"
                 "    context.register_hypothesis(contracts.HypothesisDescriptor('failed_importlib', '1', lambda *a: None))\n"
                 f"    value = {expression}\n")
    (chosen/"checked_extension.py").write_text(body)
    before_registry = {k:dict(v) for k,v in contracts._REGISTRY.items()}
    before_records = {name:dict(getattr(extensions, name)) for name in ("_LOADED", "_VERIFIED_MODULES", "_VERIFIED_IMPORTS")}
    hooks = hook_state()
    match = ("undeclared local helper" if case == "undeclared" else
             "cached from.*not chosen source.*fresh interpreter" if case.startswith("foreign") else
             "runtime source generation.*fresh interpreter")
    with pytest.raises(PatternLabDataError, match=match):
        extensions.load_extensions([declaration(chosen, helpers=[] if case == "undeclared" else [helper_path])])
    assert cached.VALUE == 1 and sys.modules["checked_helper"] is cached
    assert "checked_extension" not in sys.modules
    assert contracts._REGISTRY == before_registry
    for name, before in before_records.items():
        assert getattr(extensions, name) == before
    assert hook_state() == hooks


@pytest.mark.parametrize("route", ["module", "alias", "registration", "relative"])
def test_importlib_fresh_verified_reuse_alias_and_relative(tmp_path, route):
    helpers = ["checked_helper.py"]
    (tmp_path/"checked_helper.py").write_text("VALUE = 7\n")
    expression = "importlib.import_module('checked_helper')"
    prefix = "import importlib\nimport sys\nsys.path.append('temporary-extension-path')\n"
    if route == "alias":
        prefix += "from importlib import import_module as load\n"
        expression = "load('checked_helper')"
    if route == "relative":
        package = tmp_path/"checked_package"
        package.mkdir()
        (package/"__init__.py").write_text("# declared package\n")
        (package/"helper.py").write_text("VALUE = 7\n")
        helpers = ["checked_package/__init__.py", "checked_package/helper.py"]
        expression = "importlib.import_module('.helper', 'checked_package')"
    prefix += "assert importlib.import_module('json').loads('7') == 7\n"
    if route == "registration":
        body = prefix + f"def register(context):\n    global VALUE\n    VALUE = {expression}.VALUE\n"
    else:
        body = prefix + f"VALUE = {expression}.VALUE\ndef register(context): pass\n"
    (tmp_path/"checked_extension.py").write_text(body)
    hooks = hook_state()
    request = declaration(tmp_path, helpers=helpers)
    records = extensions.load_extensions([request])
    assert sys.modules["checked_extension"].VALUE == 7
    assert extensions.load_extensions([request]) == records
    for path, digest in records[0].files:
        assert extensions.file_digest(tmp_path/path) == digest
    # A second extension also reuses the established helper generation.
    (tmp_path/"second_extension.py").write_text(body)
    extensions.load_extensions([declaration(tmp_path, "second_extension", helpers)])
    assert sys.modules["second_extension"].VALUE == 7
    assert hook_state() == hooks
    assert "temporary-extension-path" not in sys.path


def test_importlib_transitive_cached_reuse_keeps_dependency_coverage(tmp_path):
    (tmp_path/"checked_leaf.py").write_text("VALUE = 9\n")
    (tmp_path/"checked_helper.py").write_text("import importlib\nVALUE = importlib.import_module('checked_leaf').VALUE\n")
    body = "import importlib\nVALUE = importlib.import_module('checked_helper').VALUE\ndef register(context): pass\n"
    for name in ("accepted_extension", "checked_extension"):
        (tmp_path/(name+".py")).write_text(body)
    accepted = declaration(tmp_path, "accepted_extension", ["checked_helper.py", "checked_leaf.py"])
    extensions.load_extensions([accepted])
    hooks = hook_state()
    with pytest.raises(PatternLabDataError, match="undeclared local helper checked_leaf.py"):
        extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py"])])
    assert "checked_extension" not in sys.modules and "checked_extension" not in extensions._LOADED
    assert extensions.load_extensions([accepted])
    extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py", "checked_leaf.py"])])
    assert sys.modules["checked_extension"].VALUE == sys.modules["accepted_extension"].VALUE == 9
    assert hook_state() == hooks


def test_fresh_undeclared_importlib_helper_can_retry_without_losing_unrelated_cache(tmp_path, monkeypatch):
    (tmp_path/"unrelated.py").write_text("VALUE = 42\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    unrelated = importlib.import_module("unrelated")
    (tmp_path/"checked_helper.py").write_text("VALUE = 7\n")
    (tmp_path/"checked_extension.py").write_text("import importlib\nVALUE = importlib.import_module('checked_helper').VALUE\ndef register(context): pass\n")
    hooks = hook_state()
    with pytest.raises(PatternLabDataError, match="undeclared local helper checked_helper.py"):
        extensions.load_extensions([declaration(tmp_path)])
    assert "checked_extension" not in sys.modules and "checked_helper" not in sys.modules
    assert "checked_extension" not in extensions._LOADED and "checked_helper" not in extensions._VERIFIED_MODULES
    assert hook_state() == hooks
    extensions.load_extensions([declaration(tmp_path, helpers=["checked_helper.py"])])
    assert sys.modules["checked_extension"].VALUE == 7
    assert sys.modules["unrelated"] is unrelated and unrelated.VALUE == 42
    assert hook_state() == hooks


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
