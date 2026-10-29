import os
from pathlib import Path
import shutil
import subprocess
import pytest


@pytest.mark.parametrize('filename', ['portfolio.test.cjs', 'portfolio_queue.test.cjs', 'portfolio_loading.test.cjs', 'quantity_execution.test.cjs'])
def test_portfolio_production_handlers(tmp_path, filename):
    run_handler(tmp_path, filename)


@pytest.mark.parametrize('scenario', ['exit', 'database'])
def test_portfolio_navigation_completion(tmp_path, scenario):
    run_handler(tmp_path, 'portfolio_navigation.test.cjs', scenario)


def run_handler(tmp_path, filename, *args):
    node = os.environ.get('MERLIN_NODE_EXECUTABLE') or shutil.which('node')
    assert node, 'An installed Node runtime is required.'
    script = Path(__file__).parent/'js'/filename
    result = subprocess.run([node,str(script),*args],cwd=tmp_path,capture_output=True,text=True,timeout=30)
    assert result.returncode == 0, result.stdout+result.stderr
