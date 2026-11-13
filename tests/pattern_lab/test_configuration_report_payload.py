"""Exact source bytes, stored-only rebuilds, public omission and safe metadata."""
import base64
import hashlib
import json
import re
import shutil

import pytest

from tools.pattern_lab.configuration_report import render_configuration_report
from tools.pattern_lab.configuration_report import render
from tools.pattern_lab.configuration_study import artifacts, codec, export_public_results
from ._configuration_report_helpers import native, synthetic, extract, adversarial


@pytest.fixture(scope='module')
def pair(tmp_path_factory):
    return native(tmp_path_factory.mktemp('native-report'))


def test_one_verified_reader_no_second_decode(pair, tmp_path, monkeypatch):
    original=render.load_configuration_study
    calls=[]
    def opened(path):
        reader=original(path);calls.append(path)
        def prohibited(*args,**kwargs):raise AssertionError('second decode or execution')
        monkeypatch.setattr(codec,'decode',prohibited)
        monkeypatch.setattr(artifacts.ConfigurationStudy,'result',prohibited)
        monkeypatch.setattr(artifacts.ConfigurationStudy,'curve',prohibited)
        return reader
    monkeypatch.setattr(render,'load_configuration_study',opened)
    html=render_configuration_report(pair[0].root, output_root=tmp_path/'report')
    manifest,blocks,_=extract(html)
    assert len(calls)==1
    assert blocks==[pair[0].raw_member(e['name']) for e in manifest['index']['members']]


def test_exact_native_members_metrics_statuses_and_public(pair,tmp_path):
    for source in pair:
        html=render_configuration_report(source.root,output_root=tmp_path/source.metadata['visibility'])
        m,blocks,_=extract(html)
        assert m['metadata']==source.metadata
        for e,raw in zip(m['index']['members'],blocks):
            assert raw==source.raw_member(e['name'])
            if source.metadata['visibility']=='public':assert e['stageId']!='HO2'
            value=codec.decode(raw,e)
            if e['kind']=='metrics':
                for r in value['records']:assert r==source.result(r['configurationId'],r['stageId'])
            if e['kind']=='curves':
                for r in codec.unpack_curves(value):assert r==source.curve(r['configurationId'],r['stageId'])


def test_metadata_escaping_annotations_and_script_csp(pair,tmp_path,monkeypatch):
    source=pair[0];meta=source.metadata;c=source.configurations[0]
    sentinel='</script><img src=x onerror="alert(1)">&\u2028'
    annotations=dict(schema='pattern-lab.configuration-annotations.v1',specificationIdentity=meta['identities']['specification'],annotations=[dict(instrumentId=c['instrumentId'],allocationId=c['allocationId'],role=r,methodId='synthetic',methodDescription=sentinel,selectedFromStage='IS') for r in ('return','stability','reference')])
    presentation=dict(schema='pattern-lab.configuration-report-presentation.v1',specificationIdentity=meta['identities']['specification'],title=sentinel,parameterLabels=[dict(path='/policy/initialCapital',label=sentinel)])
    html=render_configuration_report(source.root,output_root=tmp_path/'out',annotations=annotations,presentation=presentation)
    m,_,text=extract(html)
    assert 'K: HA run' not in text and 'H: hold days' not in text and 'CEX017' not in text
    assert m['presentation']['title']==sentinel
    assert sentinel not in text
    script=re.search(r'<script>(.*?)</script>',text,re.S).group(1)
    digest=base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
    assert "script-src 'sha256-"+digest+"'" in text
    assert "connect-src 'none'" in text and "style-src 'unsafe-inline'" in text
    assert not re.search(r'<(?:script|link|img)[^>]+(?:src|href)=',text)


def test_annotations_are_metadata_only(pair,tmp_path,monkeypatch):
    source=pair[0];meta=source.metadata
    def no_decode(*args):raise AssertionError('annotation decoded a member')
    monkeypatch.setattr(source,'raw_member',no_decode)
    from tools.pattern_lab.configuration_study import normalize_annotations
    assert normalize_annotations(dict(schema='pattern-lab.configuration-annotations.v1',specificationIdentity=meta['identities']['specification'],annotations=[]),source)['annotations']==[]


def test_size_guard_cleans_output(pair,tmp_path,monkeypatch):
    monkeypatch.setattr(render,'MAX_HTML',1)
    with pytest.raises(Exception):render_configuration_report(pair[0].root,output_root=tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_generic_valid_status_gap_and_hidden_only_fixtures(tmp_path):
    from tools.pattern_lab.configuration_study import load_configuration_study
    first=load_configuration_study(adversarial(tmp_path/'first'))
    second=load_configuration_study(adversarial(tmp_path/'second',hidden_variant=1))
    assert len(first.configurations)==16
    assert {c['timeframeMinutes'] for c in first.configurations}=={45,90}
    statuses=set();gaps=0;annual=set()
    for c in first.configurations:
        for stage in ('IS','HO1'):
            a=first.result(c['configurationId'],stage);b=second.result(c['configurationId'],stage)
            assert a==b
            statuses.add(a['metrics']['performanceStatus']);annual.add(a['metrics']['annualizedProfitStatus'])
            gaps+=len(first.curve(c['configurationId'],stage)['curve']['cashGaps'])
    assert statuses=={'available','noObservedBars'} and gaps>0
    assert {'available','shortSpan','noObservedBars'}<=annual
    public=export_public_results(first.root,output_root=tmp_path/'public')
    assert any(first.result(c['configurationId'],'HO2')['metrics']['dailySharpe']==87654321.125 for c in first.configurations)
    for e in public.chunk_index['members']:
        assert '87654321.125' not in json.dumps(codec.decode(public.raw_member(e['name']),e))
