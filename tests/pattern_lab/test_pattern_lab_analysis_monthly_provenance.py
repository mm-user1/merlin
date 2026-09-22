import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.pattern_lab.analysis import calibration_monthly as monthly
from tools.pattern_lab.analysis import artifacts


def _direct_research_dependencies():
    """Bounded static check of these research modules, not a repository import graph."""
    dependencies = set()
    for filename in ("calibration_monthly.py", "calibration.py", "calibration_evidence.py", "calibration_memory.py"):
        tree = ast.parse(Path(monthly.__file__).with_name(filename).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or not node.level:
                continue
            module = importlib.util.resolve_name("." * node.level + (node.module or ""), monthly.__package__)
            if node.module in (None, "study"):
                for alias in node.names:
                    candidate = module + "." + alias.name
                    if alias.name != "PatternLabDataError": dependencies.add(candidate)
            else:
                dependencies.add(module)
    return dependencies


def test_research_attribution_covers_actual_direct_local_imports():
    known = set(monthly.RESEARCH_MODULES) | set(artifacts.ATTRIBUTED_MODULES)
    dependencies = _direct_research_dependencies()
    assert dependencies <= known, dependencies - known
    assert "tools.pattern_lab.analysis.calibration_evidence" in dependencies - (known - {
        "tools.pattern_lab.analysis.calibration_evidence"})
    assert not set(monthly.RESEARCH_MODULES) & set(artifacts.ATTRIBUTED_MODULES)


def test_helper_change_changes_research_identity(tmp_path, monkeypatch):
    before = monthly.research_digests()
    helper = tmp_path / "calibration_memory.py"
    original = Path(monthly.__file__).with_name(helper.name)
    helper.write_bytes(original.read_bytes() + b"\n# bounded attribution mutation\n")
    find = importlib.util.find_spec
    monkeypatch.setattr(importlib.util,"find_spec",lambda name: SimpleNamespace(origin=str(helper))
                        if name == "tools.pattern_lab.analysis.calibration_memory" else find(name))
    after = monthly.research_digests()
    assert before["tools.pattern_lab.analysis.calibration_memory"] != after["tools.pattern_lab.analysis.calibration_memory"]
    assert {name for name in before if before[name] != after[name]} == {"tools.pattern_lab.analysis.calibration_memory"}


def test_cleanup_failure_names_owned_path_and_does_not_hide_it(tmp_path,monkeypatch):
    owned = tmp_path / "replay"
    owned.mkdir()
    def fail(path):
        raise PermissionError("injected open file")
    monkeypatch.setattr(monthly.shutil,"rmtree",fail)
    with pytest.raises(monthly.PatternLabDataError,match="retained") as error:
        monthly._remove_owned_replay(owned,tmp_path)
    assert str(owned.resolve()) in str(error.value)
    assert owned.exists()
    with pytest.raises(monthly.PatternLabDataError,match="unsafe"):
        monthly._remove_owned_replay(tmp_path,tmp_path)


@pytest.mark.parametrize("failure", ["absent", "origin", "directory", "missing_file", "unreadable", "resolution"])
def test_missing_research_source_has_module_diagnostic(failure, monkeypatch, tmp_path, capsys):
    name = monthly.RESEARCH_MODULES[0]
    source = tmp_path / "source.py"
    source.write_text("# source", encoding="utf-8")
    original_resolve = importlib.util.find_spec
    def resolve(module):
        if module != name:
            return original_resolve(module)
        if failure == "resolution":
            raise ModuleNotFoundError("injected resolution failure")
        return None if failure == "absent" else SimpleNamespace(origin={
            "origin": None, "directory": str(tmp_path), "missing_file": str(tmp_path / "absent.py"),
        }.get(failure, str(source)))
    monkeypatch.setattr(importlib.util, "find_spec", resolve)
    if failure == "unreadable":
        original_read = Path.read_bytes
        def denied(path):
            if path == source:
                raise PermissionError("injected unreadable source")
            return original_read(path)
        monkeypatch.setattr(Path, "read_bytes", denied)
    with pytest.raises(monthly.PatternLabDataError, match=name):
        monthly.research_digests()
    root = tmp_path / "run"
    assert monthly.main(["--output-root", str(root), "--attempts", "1"]) == 2
    assert name in capsys.readouterr().err
    assert not root.exists()
