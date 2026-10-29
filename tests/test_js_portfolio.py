import os
from pathlib import Path
import shutil
import subprocess
import pytest


@pytest.mark.parametrize('filename', ['portfolio.test.cjs', 'portfolio_queue.test.cjs', 'portfolio_loading.test.cjs'])
def test_portfolio_production_handlers(tmp_path, filename):
    node = os.environ.get('MERLIN_NODE_EXECUTABLE') or shutil.which('node')
    assert node, 'An installed Node runtime is required.'
    script = Path(__file__).parent/'js'/filename
    result = subprocess.run([node,str(script)],cwd=tmp_path,capture_output=True,text=True,timeout=30)
    assert result.returncode == 0, result.stdout+result.stderr
