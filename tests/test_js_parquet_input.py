"""Node contract checks; browser layout/focus remains a separate owner gate."""
from pathlib import Path
import shutil
import subprocess

import pytest


def test_parquet_input_javascript():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node unavailable; required JavaScript gate remains pending on a capable host.")
    result = subprocess.run([node, str(Path(__file__).parent / "js" / "parquet_input.test.cjs")],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
