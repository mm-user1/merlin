"""Behavioral UI checks; actual browser layout remains an owner gate."""
from pathlib import Path
import os
import shutil
import subprocess
import pytest


def test_entry_admission_javascript(tmp_path):
    node = os.environ.get("MERLIN_NODE_EXECUTABLE") or shutil.which("node")
    if not node:
        pytest.fail("Node is required for the entry-admission UI gate.")
    result = subprocess.run([node, str(Path(__file__).parent / "js" / "entry_admission.test.cjs")],
        cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
