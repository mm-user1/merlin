import shutil
import subprocess
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
JAVASCRIPT_TEST = REPOSITORY_ROOT / "tests" / "js" / "test_wfa_calendar_ui.js"


def test_wfa_calendar_ui_node_behavior(tmp_path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is unavailable; WFA calendar UI behavior test skipped.")

    completed = subprocess.run(
        [node, str(JAVASCRIPT_TEST)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, (
        "WFA calendar UI Node test failed.\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
