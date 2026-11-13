"""Standalone report lifecycle, presentation boundary, determinism and CLI privacy."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.configuration_report import render_configuration_report
from tools.pattern_lab.configuration_report import render, model
from tools.pattern_lab.configuration_study import load_configuration_study
from ._configuration_report_helpers import synthetic, extract


@pytest.fixture(scope='module')
def source(tmp_path_factory):
    return synthetic(tmp_path_factory.mktemp('report-source')/'owner', per_tf=1)


def test_deterministic_relocated_crlf_and_exact_receipt(source, tmp_path, monkeypatch):
    before = {p.name:p.read_bytes() for p in source.iterdir()}
    first = render_configuration_report(source, output_root=tmp_path/'one')
    copied = tmp_path/'relocated'
    shutil.copytree(source, copied)
    assets = tmp_path/'crlf'
    assets.mkdir()
    for name in render.SOURCES:
        (assets/name).write_bytes(render.normalized(name).replace(b'\n', b'\r\n'))
    monkeypatch.setattr(render, 'ASSETS', assets)
    second = render_configuration_report(copied, output_root=tmp_path/'two')
    assert first.read_bytes() == second.read_bytes()
    assert first.with_suffix('.json').read_bytes() == second.with_suffix('.json').read_bytes()
    receipt = json.loads(first.with_suffix('.json').read_text())
    assert receipt['html'] == {'bytes':first.stat().st_size, 'sha256':hashlib.sha256(first.read_bytes()).hexdigest()}
    assert set(p.name for p in first.parent.iterdir()) == {'report.html','report.json'}
    assert before == {p.name:p.read_bytes() for p in source.iterdir()}


@pytest.mark.parametrize('boundary', ['validate', 'beforeHtml', 'afterHtml', 'beforeReceipt', 'afterReceipt'])
@pytest.mark.parametrize('interrupt', [False, True])
@pytest.mark.parametrize('preexisting', [False, True])
def test_owned_cleanup(source, tmp_path, monkeypatch, boundary, interrupt, preexisting):
    output = tmp_path/'out'
    if preexisting: output.mkdir()
    error = KeyboardInterrupt if interrupt else RuntimeError
    def fail():
        (output/'concurrent.txt').write_text('unrelated')
        raise error('HO2_SENTINEL_PRIVATE')
    original = render.publish_file
    def publish(temp, final, owned):
        tag = 'Html' if final.suffix == '.html' else 'Receipt'
        if boundary == 'before'+tag: fail()
        original(temp, final, owned)
        if boundary == 'after'+tag: fail()
    monkeypatch.setattr(render, 'publish_file', publish)
    if boundary == 'validate': monkeypatch.setattr(render, 'validate_html', lambda *a:fail())
    with pytest.raises(error):
        render_configuration_report(source, output_root=output)
    assert {p.name for p in output.iterdir()} == {'concurrent.txt'}


def test_cleanup_empty_directory_ownership(source, tmp_path, monkeypatch):
    def fail(*args): raise KeyboardInterrupt
    monkeypatch.setattr(render, 'validate_html', fail)
    for existing in (False, True):
        out = tmp_path/str(existing)
        if existing:out.mkdir()
        with pytest.raises(KeyboardInterrupt):render_configuration_report(source, output_root=out)
        assert out.exists() == existing


@pytest.mark.parametrize('fault', ['schema', 'identity', 'unknown', 'titleType', 'longTitle', 'labelEmpty', 'duplicate', 'path', 'initialType', 'initialUnknown', 'initialDuplicate', 'nonScalar', 'oversized'])
def test_presentation_refusals(source, tmp_path, fault):
    meta = load_configuration_study(source).metadata
    value = dict(schema=model.SCHEMA, specificationIdentity=meta['identities']['specification'])
    if fault == 'schema':value['schema']='wrong'
    if fault == 'identity':value['specificationIdentity']='wrong'
    if fault == 'unknown':value['metrics']={}
    if fault == 'titleType':value['title']=42
    if fault == 'longTitle':value['title']='x'*513
    if fault == 'oversized':value['title']='x'*(model.MAX_OPTIONS+1)
    if fault in ('labelEmpty','duplicate','path','nonScalar'):
        value['parameterLabels']=[dict(path='/policy/rewardRisk',label='RR')]
        if fault == 'labelEmpty':value['parameterLabels'][0]['label']=' '
        if fault == 'duplicate':value['parameterLabels']*=2
        if fault == 'path':value['parameterLabels'][0]['path']='/unknown~2'
        if fault == 'nonScalar':value['parameterLabels'][0]['path']='/policy'
    if fault.startswith('initial'):
        value['initialSelection']={'initialType':{}, 'initialUnknown':['unknown'], 'initialDuplicate':[meta['configurations'][0]['configurationId']]*2}[fault]
    with pytest.raises((PatternLabDataError, ValueError)):
        render_configuration_report(source, output_root=tmp_path/'out', presentation=value)
    assert not (tmp_path/'out').exists()


def test_output_and_source_refusals(source, tmp_path):
    for out in (source, source/'child', source.parent):
        with pytest.raises(PatternLabDataError):render_configuration_report(source, output_root=out)
    occupied=tmp_path/'out';occupied.mkdir();(occupied/'mine').write_text('keep')
    with pytest.raises(PatternLabDataError):render_configuration_report(source, output_root=occupied)
    assert (occupied/'mine').read_text()=='keep'
    broken=tmp_path/'broken';shutil.copytree(source,broken);(broken/'completion.json').unlink()
    with pytest.raises(PatternLabDataError):render_configuration_report(broken,output_root=tmp_path/'new')
    assert not (tmp_path/'new').exists()


@pytest.mark.parametrize('kind,exitcode', [('data',2),('dependency',2),('busy',3),('pending',4),('unexpected',2),('interrupt',130)])
def test_cli_sanitized(source, tmp_path, monkeypatch, capsys, kind, exitcode):
    from tools.pattern_lab import __main__ as cli
    from tools.pattern_lab import PatternLabDependencyError, PatternLabBusyError, PatternLabPendingError
    errors={'data':PatternLabDataError,'dependency':PatternLabDependencyError,'busy':PatternLabBusyError,'pending':PatternLabPendingError,'unexpected':RuntimeError,'interrupt':KeyboardInterrupt}
    def fail(*args): raise errors[kind]('HO2_SENTINEL_PRIVATE')
    monkeypatch.setattr(render, 'validate_html', fail)
    assert cli.main(['configuration-report','--run-root',str(source),'--output-root',str(tmp_path/'out')])==exitcode
    captured=capsys.readouterr()
    assert 'HO2_SENTINEL' not in captured.out+captured.err
    assert 'Traceback' not in captured.out+captured.err
    assert 'bundles' not in captured.out+captured.err
    payload=json.loads(captured.out)
    assert payload['status']==('interrupted' if kind=='interrupt' else 'failed')
    assert not (tmp_path/'out').exists()


def test_cli_success(source,tmp_path,capsys):
    from tools.pattern_lab.__main__ import main
    assert main(['configuration-report','--run-root',str(source),'--output-root',str(tmp_path/'report')])==0
    assert json.loads(capsys.readouterr().out)['status']=='complete'
