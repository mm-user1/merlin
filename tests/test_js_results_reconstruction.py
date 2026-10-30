"""Actual Results rendering and asynchronous selection contracts; layout is separate."""
from pathlib import Path
import os
import shutil
import subprocess


def test_results_reconstruction_javascript(tmp_path):
    node = os.environ.get("MERLIN_NODE_EXECUTABLE") or shutil.which("node")
    assert node, "An installed Node runtime is required for this gate."
    result = subprocess.run([node, str(Path(__file__).parent / "js/results_reconstruction.test.cjs")],
                            cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
