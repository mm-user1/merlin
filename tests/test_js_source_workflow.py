"""Actual Start/Queue event flow; browser layout is an independent owner gate."""
import os
from pathlib import Path
import shutil
import subprocess


def test_source_workflow_javascript(tmp_path):
    node = os.environ.get('MERLIN_NODE_EXECUTABLE') or shutil.which('node')
    assert node, 'An installed Node runtime is required.'
    script = Path(__file__).parent / 'js' / 'source_workflow.test.cjs'
    result = subprocess.run([node, str(script)], cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
