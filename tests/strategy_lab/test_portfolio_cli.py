"""Copyable synthetic workflow, CLI protocol and saved path boundaries."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

from tools.strategy_lab.config import REPO_ROOT


def test_documented_example_commands(tmp_path):
    work = tmp_path/'example'
    def call(module, *args):
        result = subprocess.run([sys.executable, '-B', '-m', module, *map(str,args)], cwd=REPO_ROOT, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)
    root = 'tools.strategy_lab.portfolio'
    call(root+'.example', '--output-dir', work)
    call(root, 'prepare', work/'request.json', '--output-dir', work/'plan')
    call(root, 'run', '--plan', work/'plan', '--data-root', work/'pack', '--output-dir', work/'result')
    call(root, 'report', '--result', work/'result', '--output-dir', work/'report')
    assert call(root, 'detail', '--result', work/'result', '--variant', 'selected', '--data-root', work/'pack', '--output-dir', work/'detail')['status'] == 'verified'
    result = subprocess.run([sys.executable, '-B', '-m', root, 'report', '--result', str(work/'result'), '--output-dir', str(work/'report')],
        cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 2 and not result.stdout and 'Strategy Lab portfolio:' in result.stderr


def test_portfolio_symlink_ancestors(tmp_path):
    from tools.strategy_lab.portfolio import open_plan
    real = tmp_path/'real'; real.mkdir()
    link = tmp_path/'link'
    try: link.symlink_to(real, target_is_directory=True)
    except OSError as exc: pytest.skip(f'host symlink privilege unavailable: {exc}')
    with pytest.raises(ValueError, match='symlink'):
        open_plan(link/'nested')
