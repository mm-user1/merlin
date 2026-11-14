"""Node exercises the exact shipped decoder; absent Windows Node is explicit."""
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools.pattern_lab.configuration_report import render_configuration_report
from tools.pattern_lab.configuration_study import export_public_results
from ._configuration_report_helpers import synthetic, interactions, interaction_annotations
from tools.pattern_lab.configuration_study import load_configuration_study


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


@pytest.fixture(scope='module')
def ui_source(tmp_path_factory):
    return interactions(tmp_path_factory.mktemp('report-ui')/'owner')


def test_ui_fixture_distinguishes_evaluation_stages(ui_source):
    reader=load_configuration_study(ui_source)
    configs=reader.configurations
    stages={s:{c['configurationId']:reader.result(c['configurationId'],s)['metrics']
               for c in configs} for s in ('IS','HO1','HO2')}
    def positive(cid, periods):
        return all(stages[s][cid]['performanceStatus']=='available' and stages[s][cid]['returnPct']>0
                   for s in periods)
    ih=[c['configurationId'] for c in configs if positive(c['configurationId'],('IS','HO1'))]
    assert len(ih)>12
    assert any(stages['HO2'][cid]['returnPct']<=0 for cid in ih)
    assert any(stages['HO2'][cid]['performanceStatus']=='noObservedBars' for cid in ih)
    assert any(positive(c['configurationId'],('IS',)) and stages['HO1'][c['configurationId']]['returnPct']==0 for c in configs)
    assert any(m['performanceStatus']=='noObservedBars' and m['returnPct']>0 for m in stages['HO1'].values())
    assert any(not positive(c['configurationId'],('IS',)) and positive(c['configurationId'],('HO1','HO2')) for c in configs)
    assert len({c['instrumentId'] for c in configs})>=20
    assert len({c['timeframeMinutes'] for c in configs})>1
    assert {'BYBIT_AAA-USDT-SWAP','OKX_AAA-USDT-SWAP','BYBIT_CCCUSDT'} <= {c['instrumentId'] for c in configs}


def test_generated_javascript_interactions(ui_source,tmp_path):
    node=shutil.which('node')
    if node is None:
        if sys.platform!='win32':
            pytest.fail('Node is required for the generated report UI gate on Linux')
        pytest.skip('Node is not installed on this Windows host; Linux review requires the UI harness')
    annotations=interaction_annotations(load_configuration_study(ui_source).metadata)
    public=export_public_results(ui_source,output_root=tmp_path/'public')
    for name,source,notes in [('owner',ui_source,annotations),('public',public.root,annotations),
                              ('unannotated',ui_source,None)]:
        html=render_configuration_report(source,output_root=tmp_path/(name+'-report'),annotations=notes)
        result=subprocess.run([node,str(Path(__file__).with_name('configuration_report_interactions.cjs')),str(html)],
                              capture_output=True,text=True,timeout=120)
        assert result.returncode==0,result.stdout+result.stderr
