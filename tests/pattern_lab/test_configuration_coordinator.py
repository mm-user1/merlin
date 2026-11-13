"""Scheduling credits and progress writes tested without timing races."""
from types import SimpleNamespace
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.configuration_study import runner


def test_delayed_first_job_does_not_refill(monkeypatch,tmp_path):
    progress=runner.Progress(tmp_path,8,3);submitted=[];consumed=[];outstanding=[]
    events=iter([2,1,0,5,4,3,7,6])
    def observe():
        outstanding.append(len(submitted)-len(consumed))
        assert 0<=outstanding[-1]<=3
    class Pool:
        def submit(self,key,job):submitted.append(int(key));observe()
        def take(self):
            observe();i=next(events)
            assert i in submitted
            if i in (2,1,0):assert submitted==[0,1,2] and consumed==[]
            return 'result',str(i),[i]
    monkeypatch.setattr(runner,'extract_unit',lambda record,*a:record)
    group=[dict(configurationId=str(i)) for i in range(8)]
    for unit in runner.execute_group(group,lambda c:c,dict(stageId='IS'),Pool(),3,progress,dict(extractionSeconds=0)):
        consumed.append(unit);observe()
    assert submitted==consumed==list(range(8)) and max(outstanding)==3
    assert progress.state['maximumCompactBuffer']==19


def test_worker_failure_refuses_raw_error_payload(monkeypatch,tmp_path):
    progress=runner.Progress(tmp_path,1,2)
    pool=SimpleNamespace(submit=lambda *a:None,take=lambda:('error','0','secret HO2 PnL 999999'))
    with pytest.raises(PatternLabDataError,match='native worker failed'):
        list(runner.execute_group([dict(configurationId='0')],lambda c:c,dict(stageId='HO2'),pool,2,progress,{}))


def test_progress_throttles_intermediate_writes(monkeypatch,tmp_path):
    clock=[0.];writes=[]
    monkeypatch.setattr(runner.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(runner,'save_json',lambda *a:writes.append(clock[0]))
    p=runner.Progress(tmp_path,100,1)
    for i in range(20):clock[0]=i/20;p.write()
    assert writes==[0.]
    clock[0]=1;p.write();p.write(force=True)
    assert writes==[0.,1,1]
