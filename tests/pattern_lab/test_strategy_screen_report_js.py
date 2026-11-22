import os
from pathlib import Path
import shutil
import subprocess

from tools.pattern_lab.strategy_screen_report import render_strategy_screen_report
from ._strategy_screen_report_helpers import compact_report_input


def test_actual_shipped_node_scripts(tmp_path):
    node=os.environ.get('MERLIN_NODE_EXECUTABLE') or shutil.which('node')
    assert node, 'Node is a required gate; absence is not a passing skip'
    reader=compact_report_input(tmp_path/'input',sparse=True)
    html=render_strategy_screen_report(reader.root,output_root=tmp_path/'report',export_view_data=True)
    result=subprocess.run([node,str(Path(__file__).with_name('_strategy_screen_report_node.cjs')),str(html)],
                          capture_output=True,text=True,encoding='utf-8',timeout=60)
    assert result.returncode==0,result.stdout+result.stderr
    assert '"ok":true' in result.stdout

