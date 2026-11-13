"""Native all-stage parity, bounded execution and offline/public artifacts."""
from copy import deepcopy
from pathlib import Path
import json

import pytest

from tools.pattern_lab.configuration_study import (normalize_request,run_configuration_study,
    load_configuration_study,verify_configuration_study,export_public_results,normalize_annotations)
from tools.pattern_lab.configuration_study.request import child_request
from tools.pattern_lab.configuration_study.metrics import NATIVE_FIELDS
from tools.pattern_lab.account_study import run_account_study
from tools.pattern_lab.account_study.metrics import account_metrics
from ._configuration_helpers import inputs
from tools.pattern_lab import PatternLabDataError


def test_native_parity_and_public(tmp_path):
    raw=inputs(tmp_path/'pack')
    reader=run_configuration_study(raw,data_root=tmp_path/'pack',output_root=tmp_path/'compact')
    assert reader.metadata['counts']['completedResults']==24
    assert verify_configuration_study(reader.root)['verificationLevel']=='compact'
    for stage in reader.metadata['request']['stages']:
        native=run_account_study(child_request(reader.metadata['request'],stage),data_root=tmp_path/'pack',
            output_root=tmp_path/('native-'+stage['stageId']))
        for configuration in reader.configurations:
            actual=reader.result(configuration['configurationId'],stage['stageId'])
            record=native.account(native.key_for(configuration['instrumentId'],configuration['allocationId'],stage['stageId']))
            expected=account_metrics(record)
            assert actual['execution']['accountId']==record.result.identity
            assert actual['execution']['preparationId']==record.prepared.identity
            assert actual['execution']['commandId']==record.result.commandId
            for key in NATIVE_FIELDS:
                value='noTrades' if key=='profitFactorStatus' and expected['trades']==0 else expected[key]
                assert actual['metrics'][key]==value,key
    public=export_public_results(reader.root,output_root=tmp_path/'public')
    assert public.metadata['identities']['specification']==reader.metadata['identities']['specification']
    for entry in public.chunk_index['members']:
        assert entry['stageId']!='HO2' and public.raw_member(entry['name'])==reader.raw_member(entry['name'])
    for configuration in public.configurations:
        assert public.result(configuration['configurationId'],'HO2')['status']=='withheld'
    annotations=dict(schema='pattern-lab.configuration-annotations.v1',
        specificationIdentity=reader.metadata['identities']['specification'],annotations=[dict(
        instrumentId=reader.configurations[0]['instrumentId'],allocationId=reader.configurations[0]['allocationId'],
        role='reference',methodId='synthetic',methodDescription='Declared synthetic IS choice',selectedFromStage='IS')])
    assert normalize_annotations(annotations,reader)==normalize_annotations(annotations,public)


@pytest.mark.parametrize('workers',[2,6])
def test_auxiliary_native_and_worker_bytes(tmp_path,workers):
    from tools.pattern_lab.examples.configuration_synthetic import build_inputs
    from tools.pattern_lab.configuration_study.request import configurations
    doc=build_inputs(tmp_path/'pack',tmp_path/'aux',targets=1)
    normalized=normalize_request(doc).document
    alias=deepcopy(doc)
    for binding in alias['auxiliaryInputs']:binding['snapshotId']='anotherRoot'
    assert configurations(normalize_request(alias).document)==configurations(normalized)
    direct=run_configuration_study(doc,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/'direct')
    spawned=run_configuration_study(doc,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/'spawned',workers=workers)
    assert direct.metadata['identities']==spawned.metadata['identities']
    assert (direct.root/'results.zip').read_bytes()==(spawned.root/'results.zip').read_bytes()
    status=json.loads((spawned.root/'status.json').read_text())
    assert status['effectiveWorkers']==workers and status['maximumInFlight']<=workers
    assert status['maximumCompactBuffer']<=workers+16
    for stage in normalized['stages']:
        native=run_account_study(child_request(normalized,stage),data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/('native-'+stage['stageId']))
        for c in direct.configurations:
            record=native.account(native.key_for(c['instrumentId'],c['allocationId'],stage['stageId']))
            row=direct.result(c['configurationId'],stage['stageId'])
            assert row['execution']['accountId']==record.result.identity
            assert row['metrics']['trades']>0
            metrics=account_metrics(record)
            assert all(row['metrics'][k]==metrics[k] for k in NATIVE_FIELDS)


def test_offline_called_names_and_annotation_isolation(tmp_path,monkeypatch):
    import tools.pattern_lab.configuration_study.runner as runner
    import tools.pattern_lab.account_study.job as job
    doc=inputs(tmp_path/'pack',count=1,multi_tf=False)
    owner=run_configuration_study(doc,data_root=tmp_path/'pack',output_root=tmp_path/'owner')
    def denied(*a,**kw):raise AssertionError('offline execution/input access')
    monkeypatch.setattr(runner.data,'read_session',denied)
    monkeypatch.setattr(runner,'run_account_job',denied)
    monkeypatch.setattr(job,'execute_account',denied)
    monkeypatch.setattr('tools.pattern_lab.auxiliary.storage.load_snapshot',denied)
    monkeypatch.setattr(runner,'implementation',denied)
    moved=tmp_path/'relocated';owner.root.rename(moved)
    owner=load_configuration_study(moved)
    public=export_public_results(moved,output_root=tmp_path/'public')
    assert verify_configuration_study(public.root)['status']=='verified'
    monkeypatch.setattr(type(owner),'_rows',denied)
    a=dict(schema='pattern-lab.configuration-annotations.v1',specificationIdentity=owner.metadata['identities']['specification'],annotations=[])
    assert normalize_annotations(a,owner)==normalize_annotations(a,public)
    assert len(owner.stage_rows)==9
    with pytest.raises(PatternLabDataError):export_public_results(public.root,output_root=tmp_path/'again')


def test_late_stage_admission_before_any_output(tmp_path,monkeypatch):
    import tools.pattern_lab.configuration_study.runner as runner
    doc=inputs(tmp_path/'pack',count=1)
    later='2030-01-01T00:00:00Z';reserve='2030-02-01T00:00:00Z'
    doc['stages'][-1]['endUtc']=later
    doc['stages'][-1]['protocol']['development']['end_utc']=later
    doc['stages'][-1]['protocol']['reserved']['start_utc']=later
    for stage in doc['stages']:stage['protocol']['reserved']['end_utc']=reserve
    normalize_request(doc)
    def denied(*a,**kw):raise AssertionError('late admission reached output or execution')
    for name in ('fresh_directory','run_account_job'):monkeypatch.setattr(runner,name,denied)
    monkeypatch.setattr(runner.workers_module,'SpawnJobPool',denied)
    admitted=[];original=runner.metadata_admission_failures
    def observed(entries,span):
        failures=original(entries,span);admitted.append(bool(failures));return failures
    monkeypatch.setattr(runner,'metadata_admission_failures',observed)
    with pytest.raises(PatternLabDataError,match='configuration stage metadata admission failed'):
        run_configuration_study(doc,data_root=tmp_path/'pack',output_root=tmp_path/'out',workers=2)
    assert admitted==[False,False,True]
    assert not (tmp_path/'out').exists()


def test_ho2_failure_and_interrupted_publication_are_unsealed(tmp_path,monkeypatch):
    import tools.pattern_lab.configuration_study.runner as runner
    doc=inputs(tmp_path/'pack',count=1,multi_tf=False)
    original=runner.run_account_job
    sentinel='HO2 profit 987654.321 hidden financial sentinel'
    def broken(job):
        if job.windows[0]['windowId']=='HO2':raise ValueError(sentinel)
        return original(job)
    monkeypatch.setattr(runner,'run_account_job',broken)
    with pytest.raises(PatternLabDataError) as caught:run_configuration_study(doc,data_root=tmp_path/'pack',output_root=tmp_path/'failed')
    assert sentinel not in str(caught.value)
    assert sentinel not in (tmp_path/'failed'/'status.json').read_text()
    assert not (tmp_path/'failed'/'completion.json').exists()
    monkeypatch.setattr(runner,'run_account_job',original)
    monkeypatch.setattr('tools.pattern_lab.configuration_study.artifacts.seal',lambda root:(_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):run_configuration_study(doc,data_root=tmp_path/'pack',output_root=tmp_path/'interrupted')
    assert not (tmp_path/'interrupted'/'completion.json').exists()


def test_pool_owned_shutdown_on_worker_failure(tmp_path,monkeypatch):
    import tools.pattern_lab.configuration_study.runner as runner
    calls=[]
    class FailedPool:
        worker_pids=[];thread_evidence={}
        def __init__(self,*a):pass
        def start(self):calls.append('start')
        def submit(self,*a):pass
        def take(self):return 'error','unused','HO2 secret 987654'
        def shutdown(self,*,abort):calls.append(('shutdown',abort))
    monkeypatch.setattr(runner.workers_module,'SpawnJobPool',FailedPool)
    doc=inputs(tmp_path/'pack',count=1,multi_tf=False)
    with pytest.raises(PatternLabDataError):run_configuration_study(doc,data_root=tmp_path/'pack',output_root=tmp_path/'failed',workers=2)
    assert calls==['start',('shutdown',True)] and not (tmp_path/'failed'/'completion.json').exists()


def test_source_generation_recheck_before_seal(tmp_path,monkeypatch):
    import tools.pattern_lab.configuration_study.runner as runner
    from tools.pattern_lab import PatternLabDataError
    doc=inputs(tmp_path/'pack',count=1,multi_tf=False);checks=[]
    def changed(sources):
        checks.append(1)
        if len(checks)==8:raise PatternLabDataError('source changed before seal')
    monkeypatch.setattr(runner,'check_generation',changed)
    with pytest.raises(PatternLabDataError):run_configuration_study(doc,data_root=tmp_path/'pack',output_root=tmp_path/'failed')
    assert len(checks)==8 and not (tmp_path/'failed'/'completion.json').exists()
