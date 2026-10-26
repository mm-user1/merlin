from __future__ import annotations

import shutil
import os
import subprocess
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
JAVASCRIPT_TEST = REPOSITORY_ROOT / "tests" / "js" / "test_queue_behavior.js"


def test_queue_javascript_behavior(tmp_path):
    node = os.environ.get("MERLIN_NODE_EXECUTABLE") or shutil.which("node")
    if node is None:
        pytest.skip("Node.js is unavailable; skipping Queue JavaScript behavior test.")

    completed = subprocess.run(
        [node, str(JAVASCRIPT_TEST)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, (
        "Queue JavaScript behavior test failed.\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
