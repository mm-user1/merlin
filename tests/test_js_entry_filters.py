"""Run the actual editor/selection code with installed Node; retain external cwd."""
from pathlib import Path
import os
import shutil
import subprocess
import pytest


def test_entry_filters_javascript(tmp_path):
    node = os.environ.get("MERLIN_NODE_EXECUTABLE") or shutil.which("node")
    if not node:
        pytest.fail("An installed Node runtime is required for the entry-filter gate.")
    script = Path(__file__).parent / "js" / "entry_filters.test.cjs"
    result = subprocess.run([node, str(script)], cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
