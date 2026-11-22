import pytest
from tools.pattern_lab.strategy_screen import runner, verify_strategy_screen
from ._strategy_screen_helpers import run,results
from ._strategy_screen_helpers import build_inputs
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.account_study.common import read_json


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
    with pytest.raises(RuntimeError,match='late load'):run(tmp_path/'late')
    root=tmp_path/'late'/'screen'
    assert runner.completion_valid(root) and read_json(root/'status.json')['state']=='complete'


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
    assert runner.completion_valid(root)==after
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
