import pytest
from tools.pattern_lab.strategy_screen import runner, verify_strategy_screen
from ._strategy_screen_helpers import run,results
from ._strategy_screen_helpers import build_inputs
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.account_study.common import read_json
from tools.pattern_lab.strategy_screen.artifacts import completion_valid


@pytest.mark.parametrize('workers',[2,6])
def test_native_worker_parity(tmp_path,workers):
    direct=run(tmp_path/'direct',auxiliary=True)
    parallel=run(tmp_path/'parallel',workers=workers,auxiliary=True)
    assert results(direct)==results(parallel)
    for name in ('study.json','index.json','results.zip'):
        assert (direct.root/name).read_bytes()==(parallel.root/name).read_bytes()
    assert all(r['metrics']['trades']>0 for r in results(direct))
    assert all(r['metrics']['fees']>0 for r in results(direct))
    assert verify_strategy_screen(parallel.root)['ok']


def test_delayed_first_job_keeps_credit(monkeypatch,tmp_path):
    state=dict(maximumInFlight=0,maximumCompactBuffer=0,completedResults=0)
    progress=runner.Progress(tmp_path,state);submitted=[];consumed=[];events=iter([2,1,0,5,4,3])
    class Pool:
        def submit(self,key,job):
            submitted.append(int(key));assert len(submitted)-len(consumed)<=3
        def take(self):
            i=next(events)
            if i in (2,1,0):assert submitted==[0,1,2] and not consumed
            return 'result',str(i),[i]
    monkeypatch.setattr(runner,'extract',lambda record,*a:record)
    group=[dict(configurationId=str(i)) for i in range(6)]
    for unit in runner.execute_group(group,lambda c:c,dict(periodId='recent'),{},Pool(),3,progress,dict(extractionSeconds=0)):
        consumed.append(unit)
    assert consumed==submitted==list(range(6))


@pytest.mark.parametrize('point',['plan','progress','writer','pool','prepare','execute','generation'])
def test_failed_owned_lifecycle(monkeypatch,tmp_path,point):
    doc=build_inputs(tmp_path/'pack');out=tmp_path/'screen';original=runner.save_json
    def fail(*a,**kw):raise RuntimeError('private financial information')
    workers=1
    if point=='plan':
        def save(path,value):
            if path.name=='study.json':fail()
            return original(path,value)
        monkeypatch.setattr(runner,'save_json',save)
    elif point=='progress':monkeypatch.setattr(runner,'Progress',fail)
    elif point=='writer':monkeypatch.setattr(runner,'Writer',fail)
    elif point=='pool':
        workers=2;monkeypatch.setattr(runner.workers_module,'SpawnJobPool',fail)
    elif point=='prepare':monkeypatch.setattr(runner,'prepare_scope',fail)
    elif point=='execute':monkeypatch.setattr(runner,'run_account_job',fail)
    else:monkeypatch.setattr(runner,'check_generation',fail)
    with pytest.raises(PatternLabDataError,match='sanitized'):
        runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=out,workers=workers)
    state=read_json(out/'status.json')
    assert state['state']=='failed' and not (out/'completion.json').exists()
    assert 'private financial' not in (out/'status.json').read_text()
    if point!='plan':assert read_json(out/'study.json')['configurations']


def test_late_return_error_preserves_completion(monkeypatch,tmp_path):
    def fail(*a):raise RuntimeError('late load')
    monkeypatch.setattr(runner,'load_strategy_screen',fail)
    with pytest.raises(PatternLabDataError,match='sanitized'):run(tmp_path/'late')
    root=tmp_path/'late'/'screen'
    assert completion_valid(root) and read_json(root/'status.json')['state']=='complete'


def test_earlier_admission_before_recent_or_reservation(monkeypatch,tmp_path):
    doc=build_inputs(tmp_path/'pack');doc['periods'][0]['preparationStartUtc']='2019-11-01T00:00:00Z'
    doc['periods'][0]['protocol']['earliest_warmup_start_utc']='2019-11-01T00:00:00Z'
    monkeypatch.setattr(runner,'run_account_job',lambda *a:pytest.fail('executed before admission'))
    with pytest.raises(PatternLabDataError):runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=tmp_path/'absent')
    assert not (tmp_path/'absent').exists()


def test_failed_out_of_order_job_coordinate(monkeypatch,tmp_path):
    from types import SimpleNamespace
    state=dict(maximumInFlight=0,maximumCompactBuffer=0,completedResults=0)
    progress=runner.Progress(tmp_path,state)
    pool=SimpleNamespace(submit=lambda *a:None,take=lambda:('error','first','secret'))
    group=[dict(configurationId='first'),dict(configurationId='second')]
    with pytest.raises(PatternLabDataError):
        list(runner.execute_group(group,lambda c:c,dict(periodId='recent'),{},pool,2,progress,{}))
    assert state['currentJob']['configurationId']=='first'


@pytest.mark.parametrize('after',[False,True])
def test_interrupt_completion_commit_point(monkeypatch,tmp_path,after):
    from tools.pattern_lab.strategy_screen import artifacts
    original=artifacts.seal
    def interrupt(root):
        if after:original(root)
        raise KeyboardInterrupt
    monkeypatch.setattr(artifacts,'seal',interrupt)
    with pytest.raises(KeyboardInterrupt):run(tmp_path/'interrupt')
    root=tmp_path/'interrupt'/'screen'
    assert completion_valid(root)==after
    assert read_json(root/'status.json')['state']==('complete' if after else 'failed')


def test_generation_mutation_before_seal(monkeypatch,tmp_path):
    checks=[];original=runner.check_generation
    def check(sources):
        checks.append(1)
        if len(checks)==5:raise PatternLabDataError('source changed')
        return original(sources)
    monkeypatch.setattr(runner,'check_generation',check)
    with pytest.raises(PatternLabDataError):run(tmp_path/'generation')
    assert len(checks)==5 and not (tmp_path/'generation'/'screen'/'completion.json').exists()


@pytest.mark.parametrize('point',['initialClock','firstPublicationClock','reservationRace','reservationDenied','postSealReadError'])
def test_reservation_and_uncertain_completion(monkeypatch,tmp_path,point):
    from pathlib import Path
    from types import SimpleNamespace
    from tools.pattern_lab.strategy_screen import artifacts
    doc=build_inputs(tmp_path/'pack');out=tmp_path/'screen'
    def fail(*a,**kw):raise OSError('private clock or storage failure')
    if point=='initialClock':monkeypatch.setattr(runner,'datetime',SimpleNamespace(now=fail))
    elif point=='firstPublicationClock':monkeypatch.setattr(runner.time,'monotonic',fail)
    elif point.startswith('reservation'):
        mkdir=Path.mkdir
        def reserve(path,*a,**kw):
            if path==out:
                if point=='reservationRace':
                    mkdir(path);(path/'owner.txt').write_text('other owner')
                    raise FileExistsError('private race')
                raise PermissionError('private refusal')
            return mkdir(path,*a,**kw)
        monkeypatch.setattr(Path,'mkdir',reserve)
    else:
        seal=artifacts.seal
        def after(root):
            seal(root)
            monkeypatch.setattr(artifacts,'file_hash',fail)
            raise RuntimeError('private postseal failure')
        monkeypatch.setattr(artifacts,'seal',after)
    with pytest.raises(PatternLabDataError):runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=out)
    if point in ('initialClock','reservationDenied'):assert not out.exists()
    elif point=='reservationRace':assert [p.name for p in out.iterdir()]==['owner.txt']
    elif point=='firstPublicationClock':
        state=read_json(out/'status.json');assert state['state']=='failed' and state['plan']['configurations']
    else:
        monkeypatch.undo()
        assert artifacts.completion_valid(out) and verify_strategy_screen(out)['ok']


@pytest.mark.parametrize('case',['one','several','none','foreign','mixed','timeout','transport','keyed','bufferedAppend'])
def test_persisted_receive_and_append_attribution(tmp_path,monkeypatch,case):
    from tools.pattern_lab.study.workers import WorkerLostError,WorkerTransportError
    from tools.pattern_lab.strategy_screen.request import normalize_request,configurations
    doc=build_inputs(tmp_path/'pack');out=tmp_path/'screen';jobs=[];stopped=[];taken=[]
    class Pool:
        worker_pids=[];thread_evidence={}
        def __init__(self,*a):pass
        def start(self):pass
        def submit(self,key,job):jobs.append((key,job))
        def shutdown(self,*,abort):stopped.append(abort)
        def take(self):
            if case=='bufferedAppend':
                index=1 if not taken else 0;taken.append(index)
                key,job=jobs[index];return 'result',key,runner.run_account_job(job)
            if case=='timeout':raise TimeoutError('PRIVATE_UNKNOWN_ID')
            if case=='transport':raise WorkerTransportError('PRIVATE_UNKNOWN_ID')
            if case=='keyed':return 'error',jobs[0][0],'PRIVATE_UNKNOWN_ID'
            orphaned={'one':[jobs[0][0]],'several':[jobs[1][0],jobs[0][0]],'none':[],
                'foreign':['PRIVATE_UNKNOWN_ID'],'mixed':[jobs[0][0],'PRIVATE_UNKNOWN_ID']}[case]
            raise WorkerLostError('PRIVATE_UNKNOWN_ID',orphaned=orphaned)
    monkeypatch.setattr(runner.workers_module,'SpawnJobPool',Pool)
    if case=='bufferedAppend':
        append=runner.Writer.append;calls=[]
        def write(writer,unit):
            calls.append(unit['configurationId'])
            if len(calls)==2:raise OSError('PRIVATE_UNKNOWN_ID')
            return append(writer,unit)
        monkeypatch.setattr(runner.Writer,'append',write)
    with pytest.raises(PatternLabDataError):runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=out,workers=2)
    state=read_json(out/'status.json');ordered=configurations(normalize_request(doc).document)
    assert stopped==[True] and state['currentGroup']==dict(periodId='recent',cellId=ordered[0]['cellId'])
    if case in ('one','keyed','bufferedAppend'):
        assert state['currentJob']==dict(periodId='recent',**ordered[1 if case=='bufferedAppend' else 0])
    else:assert state['currentJob'] is None
    assert state.get('affectedJobs',[])==([dict(periodId='recent',**c) for c in ordered[:2]] if case=='several' else [])
    assert state['completedResults']==(1 if case=='bufferedAppend' else 0)
    assert 'PRIVATE_UNKNOWN_ID' not in (out/'status.json').read_text()
    assert state['reasonCode']==('nativeWorkerReceiveFailed' if case=='timeout' else 'nativeWorkerTransportFailed' if case=='transport'
        else 'nativeWorkerJobFailed' if case=='keyed' else 'strategyScreenOperationFailed' if case=='bufferedAppend' else 'nativeWorkerLost')


@pytest.mark.parametrize('point',['request','protocol','normalize','root','session','reserve'])
def test_preflight_has_no_private_exception_context(tmp_path,monkeypatch,point):
    from pathlib import Path
    from ._strategy_screen_helpers import request
    secret='PRIVATE_SECRET_WINDOWS_PATH';doc=request();out=tmp_path/'screen'
    if point=='request':doc=tmp_path/secret/'missing.json'
    elif point=='protocol':doc['periods'][0]['protocol']=str(tmp_path/secret/'protocol.json')
    elif point=='normalize':doc['cells']=secret
    elif point=='root':out=tmp_path/secret;out.mkdir();(out/'keep').write_text('owned')
    else:
        def fail(*a,**kw):
            e=OSError(13,secret,str(tmp_path/secret));e.privateDetail=secret;raise e
        if point=='session':monkeypatch.setattr(runner.data,'read_session',fail)
        else:
            doc=build_inputs(tmp_path/'pack');original=Path.mkdir
            def mkdir(path,*a,**kw):
                if path==out:fail()
                return original(path,*a,**kw)
            monkeypatch.setattr(Path,'mkdir',mkdir)
    with pytest.raises(PatternLabDataError) as caught:
        runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=out)
    error=caught.value
    assert error.__context__ is error.__cause__ is None
    assert getattr(error,'filename',None) is getattr(error,'filename2',None) is None
    assert secret not in repr(error.args) and secret not in repr(vars(error))
    if point=='root':assert [p.name for p in out.iterdir()]==['keep']
    else:assert not out.exists()


def test_preflight_interrupt_and_cli_private_paths(tmp_path,monkeypatch,capsys):
    import json
    from tools.pattern_lab.__main__ import main
    from ._strategy_screen_helpers import request
    private=tmp_path/'PRIVATE_CLI_PATH'/'missing.json';out=tmp_path/'out'
    assert main(['strategy-screen','--request',str(private),'--data-root',str(tmp_path/'pack'),'--output-root',str(out)])!=0
    output=capsys.readouterr();payload=json.loads(output.out)
    assert 'PRIVATE_CLI_PATH' not in json.dumps(payload)+output.err and not out.exists()
    def interrupt(*a,**kw):raise KeyboardInterrupt
    monkeypatch.setattr(runner,'normalize_request',interrupt)
    with pytest.raises(KeyboardInterrupt):runner.run_strategy_screen(request(),data_root=tmp_path/'pack',output_root=out)


@pytest.mark.parametrize('failure',[OSError,KeyboardInterrupt,RuntimeError])
def test_prepare_phase_does_not_mislabel_non_data_failures(tmp_path,monkeypatch,failure):
    doc=build_inputs(tmp_path/'pack');out=tmp_path/'out'
    def fail(*a,**kw):raise failure('private')
    monkeypatch.setattr(runner,'prepare_scope',fail)
    with pytest.raises(KeyboardInterrupt if failure is KeyboardInterrupt else PatternLabDataError):
        runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=out)
    state=read_json(out/'status.json')
    assert state['phase']=='prepare' and state['reasonCode']=='strategyScreenOperationFailed'


def test_real_empty_earlier_span_fails_after_six_recent_accounts(tmp_path,monkeypatch):
    from dataclasses import replace
    from tools.pattern_lab import data
    from tools.pattern_lab.strategy_screen import load_strategy_screen,analyze_strategy_screen
    from tools.pattern_lab.strategy_screen.request import month_boundary
    publish=data.publish_pack;start=month_boundary('2020-01-01T00:00:00Z');end=month_boundary('2020-04-01T00:00:00Z')
    def sparse(root,sources,**kw):
        values=[]
        for source in sources:
            if source.symbol=='AAA':
                keep=(source.timestamps<start)|(source.timestamps>=end)
                source=replace(source,timestamps=source.timestamps[keep],ohlcv=source.ohlcv[keep])
            values.append(source)
        return publish(root,values,**kw)
    monkeypatch.setattr(data,'publish_pack',sparse)
    doc=build_inputs(tmp_path/'pack');out=tmp_path/'screen';pools=[]
    original=runner.workers_module.SpawnJobPool
    def pool(*a,**kw):
        value=original(*a,**kw);pools.append(value);return value
    monkeypatch.setattr(runner.workers_module,'SpawnJobPool',pool)
    with pytest.raises(PatternLabDataError):runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=out,workers=2)
    state=read_json(out/'status.json')
    assert state['state']=='failed' and state['completedResults']==6 and state['plannedResults']==12
    assert state['reasonCode']=='nativePreparationRejected' and state['currentJob']['periodId']=='earlier'
    assert state['currentJob']['cellId'] and not (out/'completion.json').exists()
    for check in (load_strategy_screen,verify_strategy_screen):
        with pytest.raises(PatternLabDataError):check(out)
    with pytest.raises(PatternLabDataError):analyze_strategy_screen(out,output_root=tmp_path/'analysis')
    assert not (tmp_path/'analysis').exists()
    assert pools and all(not pool.live_workers() for pool in pools)
