import json
from pathlib import Path
import shutil
import sys
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.strategy_screen import load_strategy_screen, verify_strategy_screen
from tools.pattern_lab.strategy_screen_report import render_strategy_screen_report
from tools.pattern_lab.strategy_screen_report import render
from tools.pattern_lab.strategy_screen_report.model import PRESENTATION
from ._strategy_screen_report_helpers import compact_report_input, inventory, reseal_false_summary


@pytest.fixture
def reader(tmp_path):
    return compact_report_input(tmp_path/'input')


def test_public_determinism_relocation_export_and_sources(reader,tmp_path):
    before=inventory(reader.root)
    options=dict(schema=PRESENTATION,artifactIdentity=reader.metadata['artifactIdentity'],title='</script> __JS__ __CSS__ &')
    a=render_strategy_screen_report(reader.root,output_root=tmp_path/'a',presentation=options,export_view_data=True)
    relocated=tmp_path/'relocated';shutil.copytree(reader.root,relocated)
    b=render_strategy_screen_report(relocated,output_root=tmp_path/'b',presentation=options,export_view_data=True)
    assert inventory(a.parent)==inventory(b.parent) and inventory(reader.root)==before
    receipt=json.loads((a.parent/'report.json').read_text())
    raw=(a.parent/'view-data.json').read_bytes()
    assert raw.endswith(b'\n') and not raw.endswith(b'\n\n')
    assert receipt['view']['sha256']==render.sha(raw[:-1])
    assert receipt['export']==dict(bytes=len(raw),sha256=render.sha(raw))
    assert raw[:-1] in a.read_bytes()
    assert b"form-action 'none'" in a.read_bytes() and b"script-src 'sha256-" in a.read_bytes()
    assert 'tools/pattern_lab/strategy_screen/facts.py' in receipt['generator']['sources']


def test_fully_verified_not_merely_loaded(reader,tmp_path):
    reseal_false_summary(reader.root)
    load_strategy_screen(reader.root)
    with pytest.raises(PatternLabDataError,match='numerical summary'):verify_strategy_screen(reader.root)
    output=tmp_path/'parent'/'output'
    with pytest.raises(PatternLabDataError) as caught:
        render_strategy_screen_report(reader.root,output_root=output)
    assert caught.value.args==(render.FAILURE,)
    assert caught.value.__cause__ is caught.value.__context__ is None
    assert not output.parent.exists()


@pytest.mark.parametrize('marker',['completion.json','study.json','selection.json','status.json','report.json','analysis-pair'])
def test_owned_ancestor_marker_names(reader,tmp_path,marker):
    parent=tmp_path/'owned';parent.mkdir()
    names=['analysis.json','receipt.json'] if marker=='analysis-pair' else [marker]
    for name in names:(parent/name).write_text('not JSON',encoding='utf-8')
    before=inventory(parent)
    with pytest.raises(PatternLabDataError):render_strategy_screen_report(reader.root,output_root=parent/'new')
    assert inventory(parent)==before and not (parent/'new').exists()


@pytest.mark.parametrize('name',['report.html','view-data.json','report.json'])
def test_exclusive_collision_preserves_competitor_and_empty_root(reader,tmp_path,monkeypatch,name):
    output=tmp_path/'output';output.mkdir();original=render.publish_file
    def collision(temp,dest,owned):
        if dest.name==name:dest.write_bytes(b'competitor')
        original(temp,dest,owned)
    monkeypatch.setattr(render,'publish_file',collision)
    with pytest.raises(PatternLabDataError):render_strategy_screen_report(reader.root,output_root=output,export_view_data=True)
    assert inventory(output)=={name:b'competitor'}
    assert verify_strategy_screen(reader.root)['ok']


@pytest.mark.parametrize('after',['report.html','view-data.json'])
def test_interrupt_owned_cleanup_and_unrelated_file(reader,tmp_path,monkeypatch,after):
    output=tmp_path/'new-parent'/'output';original=render.publish_file
    def interrupt(temp,dest,owned):
        original(temp,dest,owned)
        if dest.name==after:
            (dest.parent/'other').write_bytes(b'preserve')
            raise KeyboardInterrupt
    monkeypatch.setattr(render,'publish_file',interrupt)
    with pytest.raises(KeyboardInterrupt):render_strategy_screen_report(reader.root,output_root=output,export_view_data=True)
    assert inventory(output)=={'other':b'preserve'}


def test_invalid_options_before_parent_creation(reader,tmp_path):
    with pytest.raises(PatternLabDataError):
        render_strategy_screen_report(reader.root,output_root=tmp_path/'new'/'out',presentation={'bad':1})
    assert not (tmp_path/'new').exists()


def test_empty_output_and_sibling_allowed(reader,tmp_path):
    out=tmp_path/'empty';out.mkdir()
    assert render_strategy_screen_report(reader.root,output_root=out).exists()


def test_completion_captured_before_verify_and_checked_after_mapping(reader,tmp_path,monkeypatch):
    original=render.verify_strategy_screen
    def change(root):
        receipt=original(root)
        path=root/'completion.json';path.write_bytes(path.read_bytes()+b' ')
        return receipt
    monkeypatch.setattr(render,'verify_strategy_screen',change)
    with pytest.raises(PatternLabDataError):
        render_strategy_screen_report(reader.root,output_root=tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_verify_load_identity_reconciled(reader,tmp_path,monkeypatch):
    original=render.verify_strategy_screen
    def wrong(root):
        v=original(root);v['counts']={'configurations':0};return v
    monkeypatch.setattr(render,'verify_strategy_screen',wrong)
    with pytest.raises(PatternLabDataError):render_strategy_screen_report(reader.root,output_root=tmp_path/'out')


def test_cli_offline_branch_and_fixed_errors(reader,tmp_path,capsys,monkeypatch):
    from tools.pattern_lab import __main__ as cli
    out=tmp_path/'cli'
    assert cli.main(['strategy-screen-report','--run-root',str(reader.root),'--output-root',str(out),'--export-view-data'])==0
    assert json.loads(capsys.readouterr().out)['command']=='strategy-screen-report'
    assert cli.main(['strategy-screen-report','--run-root',str(reader.root),'--output-root',str(out)])==2
    captured=capsys.readouterr();assert json.loads(captured.out)['error']==render.FAILURE
    assert render.FAILURE in captured.err
    def interrupt(*a,**kw):raise KeyboardInterrupt
    import tools.pattern_lab.strategy_screen_report as package
    monkeypatch.setattr(package,'render_strategy_screen_report',interrupt)
    assert cli.main(['strategy-screen-report','--run-root',str(reader.root),'--output-root',str(tmp_path/'new')])==130
    captured=capsys.readouterr();assert json.loads(captured.out)['error']==render.INTERRUPTED


def test_path_inspection_is_root_first_and_refuses_reparse(tmp_path,monkeypatch):
    original=Path.lstat;seen=[];ancestor=tmp_path/'linked'
    ancestor.mkdir()
    class Reparse:
        st_mode=0o40755
        st_file_attributes=0x400
    def inspect(p,*a,**kw):
        seen.append(p)
        if p==ancestor:return Reparse()
        assert p!=ancestor/'child'
        return original(p,*a,**kw)
    monkeypatch.setattr(Path,'lstat',inspect)
    with pytest.raises(PatternLabDataError):render.safe_path(ancestor/'child')
    assert seen[-1]==ancestor


def test_both_direction_overlap_and_presentation_path(reader,tmp_path):
    for out in (reader.root/'child',tmp_path,reader.root):
        with pytest.raises(PatternLabDataError):render_strategy_screen_report(reader.root,output_root=out)
    out=tmp_path/'options';out.mkdir();p=out/'presentation.json'
    p.write_text(json.dumps(dict(schema=PRESENTATION,artifactIdentity=reader.metadata['artifactIdentity'])))
    with pytest.raises(PatternLabDataError):render_strategy_screen_report(reader.root,output_root=out,presentation=p)

def test_genuine_full_february_zero_trades_retains_low_activity(tmp_path):
    from tools.pattern_lab.examples.strategy_screen_synthetic import request,build_inputs
    from tools.pattern_lab.strategy_screen import run_strategy_screen
    from tools.pattern_lab.strategy_screen_report.payload import build_view
    from tools.pattern_lab.strategy_screen_report.model import presentation_options
    doc=request();doc['allocations']=doc['allocations'][:1]
    doc['allocations'][0]['provider']['recipe']['entryRunLength']=40
    doc['cells'][0]['allocationIds']=['rr2']
    doc['periods'][0].update(startUtc='2020-02-01T00:00:00Z',endUtc='2020-03-01T00:00:00Z')
    build_inputs(tmp_path/'pack',document=doc)
    r=run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=tmp_path/'screen')
    v=build_view(r,presentation_options(None,r.metadata))
    p=v['detail']['settings'][0]['periods']['earlier'];b=v['scopes']['all']['periods']['earlier']
    assert p['returnPct']==p['trades']==p['mtmMaxDdPct']==0 and p['coverageComplete']
    assert p['annualizedProfitPct'] is None and p['classification']['lowActivity'] is True
    assert p['classification']['valid'] is False
    assert b['profitable']['pct']==0 and b['profitable']['missing']==1 and b['profitable']['available']==0
    assert b['failures']['activity']==0 and b['lowActivity']==1
    assert b['distributions']['returnPct']['counts']==[1]
    assert b['distributions']['annualizedProfitPct']['n']==0
    html=render_strategy_screen_report(r.root,output_root=tmp_path/'february-report',export_view_data=True,
        presentation=dict(schema=PRESENTATION,artifactIdentity=r.metadata['artifactIdentity'],
                          subtitle='Artificial-price native execution fixture; not market research'))
    assert json.loads((html.parent/'view-data.json').read_bytes())['detail']['settings'][0]['periods']['earlier']==p


@pytest.mark.parametrize('exception',[RuntimeError,KeyboardInterrupt])
def test_receipt_replace_is_commit_point(reader,tmp_path,monkeypatch,exception):
    original=render.publish_file
    def late(temp,dest,owned):
        original(temp,dest,owned)
        if dest.name=='report.json':raise exception('after commit')
    monkeypatch.setattr(render,'publish_file',late)
    html=render_strategy_screen_report(reader.root,output_root=tmp_path/'out')
    assert html.exists() and (html.parent/'report.json').exists()


def test_public_api_refuses_legacy_or_unsealed_input(tmp_path):
    root=tmp_path/'legacy';root.mkdir();(root/'exact-facts.json').write_text('[]')
    with pytest.raises(PatternLabDataError):render_strategy_screen_report(root,output_root=tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_precommit_cleanup_removes_new_empty_parents(reader,tmp_path,monkeypatch):
    def fail(*a):raise OSError('private path')
    monkeypatch.setattr(render,'publish_file',fail)
    with pytest.raises(PatternLabDataError) as error:
        render_strategy_screen_report(reader.root,output_root=tmp_path/'new'/'child')
    assert error.value.__context__ is error.value.__cause__ is None
    assert not (tmp_path/'new').exists()


def test_dangling_ancestor_marker_is_not_followed(reader,tmp_path,monkeypatch):
    parent=tmp_path/'parent';parent.mkdir()
    original=Path.lstat
    class Dangling:
        st_mode=0o120777
        st_file_attributes=0
    def lstat(p,*a,**kw):
        return Dangling() if p==parent/'report.json' else original(p,*a,**kw)
    monkeypatch.setattr(Path,'lstat',lstat)
    with pytest.raises(PatternLabDataError):render_strategy_screen_report(reader.root,output_root=parent/'out')
    assert not (parent/'out').exists()



@pytest.mark.parametrize('installed,observation,replace_kind,read_kind', [
    (installed, observation, replace_kind, read_kind)
    for installed in (False, True) for observation in ('readable','denied','interrupted')
    for replace_kind in (OSError, KeyboardInterrupt)
    for read_kind in ([KeyboardInterrupt] if observation=='interrupted' else [PermissionError])
])
def test_publication_outcome_controls(reader,tmp_path,monkeypatch,installed,observation,replace_kind,read_kind):
    out=tmp_path/'out';replace=render.os.replace;open_file=Path.open;attempted=False
    def fault(source,dest):
        nonlocal attempted
        if dest.name!='report.json':return replace(source,dest)
        attempted=True
        if installed:replace(source,dest)
        raise replace_kind('PRIVATE_REPLACE')
    def observe(path,*args,**kwargs):
        if attempted and path==out/'report.json' and args==('rb',) and observation!='readable':
            raise read_kind('PRIVATE_RECONCILE')
        return open_file(path,*args,**kwargs)
    with monkeypatch.context() as m:
        m.setattr(render.os,'replace',fault);m.setattr(Path,'open',observe)
        if installed and observation=='readable':
            assert render_strategy_screen_report(reader.root,output_root=out)==out/'report.html'
        else:
            interrupted=replace_kind is KeyboardInterrupt or observation=='interrupted'
            unknown=observation!='readable'
            expected=KeyboardInterrupt if interrupted else PatternLabDataError
            with pytest.raises(expected) as caught:render_strategy_screen_report(reader.root,output_root=out)
            assert caught.value.args==(render.UNCERTAIN if unknown else render.INTERRUPTED if interrupted else render.FAILURE,)
            assert caught.value.__cause__ is caught.value.__context__ is None
            assert isinstance(caught.value,(render.PublicationUnknown,render.PublicationInterrupted))==unknown
    if observation!='readable' or installed:
        assert (out/'report.html').exists()
        raw=(out/'report.json').read_bytes()
        if installed:assert json.loads(raw)['html']['sha256']==render.sha((out/'report.html').read_bytes())
        else:assert raw==b''
    else:assert not out.exists()


@pytest.mark.parametrize('name',['report.html','view-data.json','report.json'])
@pytest.mark.parametrize('kind',[OSError,KeyboardInterrupt])
def test_before_replace_clean_and_sanitized(reader,tmp_path,monkeypatch,name,kind):
    out=tmp_path/'out';out.mkdir();replace=render.os.replace
    def fault(source,dest):
        if dest.name==name:raise kind('PRIVATE_BEFORE_REPLACE')
        return replace(source,dest)
    monkeypatch.setattr(render.os,'replace',fault)
    with pytest.raises(KeyboardInterrupt if kind is KeyboardInterrupt else PatternLabDataError) as caught:
        render_strategy_screen_report(reader.root,output_root=out,export_view_data=True)
    assert caught.value.args==(render.INTERRUPTED if kind is KeyboardInterrupt else render.FAILURE,)
    assert caught.value.__cause__ is caught.value.__context__ is None
    assert out.is_dir() and not list(out.iterdir())


def test_matching_unowned_receipt_is_not_completion(reader,tmp_path,monkeypatch):
    original=render.publish_file;out=tmp_path/'out'
    def collide(source,dest,owned):
        if dest.name=='report.json':dest.write_bytes(source.read_bytes())
        original(source,dest,owned)
    monkeypatch.setattr(render,'publish_file',collide)
    with pytest.raises(PatternLabDataError,match='generation failed'):
        render_strategy_screen_report(reader.root,output_root=out)
    assert list(out.iterdir())==[out/'report.json']
    assert json.loads((out/'report.json').read_bytes())['schema']=='pattern-lab.strategy-screen-report.v1'


@pytest.mark.parametrize('phase',['write','partial','flush','emit'])
@pytest.mark.parametrize('kind',[OSError,KeyboardInterrupt])
@pytest.mark.parametrize('stderr_fault',[None,'write','flush'])
def test_cli_published_notification_boundary(reader,tmp_path,monkeypatch,phase,kind,stderr_fault):
    import io
    from tools.pattern_lab import __main__ as cli
    class Stream(io.StringIO):
        def __init__(self,phase):super().__init__();self.phase=phase;self.calls=0
        def write(self,s):
            self.calls+=1
            if self.phase=='partial':super().write(s[:5])
            if self.phase in ('write','partial'):raise kind('PRIVATE_STREAM')
            return super().write(s)
        def flush(self):
            if self.phase=='flush':raise kind('PRIVATE_FLUSH')
    stdout=Stream(phase);stderr=Stream(stderr_fault);out=tmp_path/'out'
    with monkeypatch.context() as m:
        m.setattr(sys,'stdout',stdout);m.setattr(sys,'stderr',stderr)
        if phase=='emit':
            def fail(payload):raise kind('PRIVATE_EMIT')
            m.setattr(cli,'_emit',fail)
        assert cli.main(['strategy-screen-report','--run-root',str(reader.root),'--output-root',str(out)])==(130 if kind is KeyboardInterrupt else 2)
    receipt=json.loads((out/'report.json').read_bytes())
    assert receipt['html']['sha256']==render.sha((out/'report.html').read_bytes())
    assert stderr.calls==1 and stdout.calls<=1
    if stderr_fault!='write':
        assert 'was published; completion notification' in stderr.getvalue()
        assert 'PRIVATE' not in stderr.getvalue()


@pytest.mark.parametrize('unknown,interrupted',[(False,False),(False,True),(True,False),(True,True)])
def test_cli_failed_notification_preserves_generation_disposition(reader,tmp_path,monkeypatch,unknown,interrupted):
    import io
    from tools.pattern_lab import __main__ as cli
    import tools.pattern_lab.strategy_screen_report as package
    kind=(render.PublicationInterrupted if interrupted else render.PublicationUnknown) if unknown else (KeyboardInterrupt if interrupted else PatternLabDataError)
    def fail(*a,**k):raise kind('PRIVATE_GENERATION')
    output=io.StringIO();error=io.StringIO()
    with monkeypatch.context() as m:
        m.setattr(package,'render_strategy_screen_report',fail);m.setattr(sys,'stdout',output);m.setattr(sys,'stderr',error)
        assert cli.main(['strategy-screen-report','--run-root',str(reader.root),'--output-root',str(tmp_path/'out')])==(130 if interrupted else 2)
    payload=json.loads(output.getvalue())
    assert payload['status']==('interrupted' if interrupted else 'failed')
    assert payload.get('publicationState')==('unknown' if unknown else None)
    assert payload['error']==(render.UNCERTAIN if unknown else render.INTERRUPTED if interrupted else render.FAILURE)
    class Broken(io.StringIO):
        def write(self,s):raise KeyboardInterrupt('PRIVATE_SECONDARY')
        def flush(self):raise OSError('PRIVATE_FLUSH')
    with monkeypatch.context() as m:
        m.setattr(package,'render_strategy_screen_report',fail);m.setattr(sys,'stdout',Broken());m.setattr(sys,'stderr',Broken())
        assert cli.main(['strategy-screen-report','--run-root',str(reader.root),'--output-root',str(tmp_path/'out')])==(130 if interrupted else 2)


def test_actual_buffered_closed_pipe_preserves_published_report(reader,tmp_path):
    import os,subprocess
    out=tmp_path/'out';read_fd,write_fd=os.pipe();os.close(read_fd)
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1');env.pop('PYTHONUNBUFFERED',None)
    try:
        child=subprocess.run([sys.executable,'-B','-X','utf8','-m','tools.pattern_lab','strategy-screen-report',
            '--run-root',str(reader.root),'--output-root',str(out)],stdout=write_fd,stderr=subprocess.PIPE,env=env,timeout=60)
    finally:os.close(write_fd)
    assert child.returncode==2,child.stderr
    assert child.stderr.decode().strip()=='pattern-lab: Strategy Screen report was published; completion notification failed.'
    receipt=json.loads((out/'report.json').read_bytes())
    assert receipt['html']['sha256']==render.sha((out/'report.html').read_bytes())


def test_current_validation_sources_and_change_sensitivity(reader,tmp_path,monkeypatch):
    names=['accounts/preparation.py','auxiliary/request.py','auxiliary/features.py','auxiliary/model.py','study/spec.py']
    original=render.sources()
    for name in names:
        key='tools/pattern_lab/'+name
        assert original[key]==render.sha((render.REPO/key).read_bytes().replace(b'\r\n',b'\n'))
    # Substitute a small source root, never mutate the protected real files.
    copy=tmp_path/'source'
    for name in original:
        p=copy/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((render.REPO/name).read_bytes())
    monkeypatch.setattr(render,'REPO',copy);monkeypatch.setattr(render,'PACKAGE',copy/'tools/pattern_lab/strategy_screen_report')
    for name in names:
        key='tools/pattern_lab/'+name;p=copy/key;p.write_bytes(p.read_bytes()+b'\n# source attribution witness\n')
        assert render.sources()[key]!=original[key]
