import shutil
import os
import subprocess
from pathlib import Path


def test_fixed_mode_real_handlers(tmp_path):
    node = os.environ.get("MERLIN_NODE_EXECUTABLE") or shutil.which("node")
    assert node, "Node is required for Fixed mode UI verification."
    script = Path(__file__).parent / "js/test_fixed_mode.js"
    result = subprocess.run([node, str(script.resolve())], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
