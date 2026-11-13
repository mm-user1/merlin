"""Node exercises the exact shipped decoder; absent Windows Node is explicit."""
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools.pattern_lab.configuration_report import render_configuration_report
from tools.pattern_lab.configuration_study import export_public_results
from ._configuration_report_helpers import synthetic


def test_shipped_javascript_decoder(tmp_path):
    node=shutil.which('node')
    if node is None:
        if sys.platform != 'win32':
            pytest.fail('Node is required for the shipped decoder gate on Linux')
        pytest.skip('Node is not installed on this Windows host; Linux review requires this harness with Node')
    source=synthetic(tmp_path/'owner',per_tf=1)
    public=export_public_results(source,output_root=tmp_path/'public')
    owner_html=render_configuration_report(source,output_root=tmp_path/'owner-report')
    public_html=render_configuration_report(public.root,output_root=tmp_path/'public-report')
    result=subprocess.run([node,str(Path(__file__).with_name('configuration_report_node.cjs')),str(owner_html),str(public_html)],capture_output=True,text=True,timeout=60)
    assert result.returncode==0,result.stdout+result.stderr
