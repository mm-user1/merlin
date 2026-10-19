"""Integrated v2 studies: real executor, shared evidence, offline regeneration."""
from copy import deepcopy
import json
import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.account_study import (normalize_request,run_account_study,verify_account_study,
    export_account_bundle,import_account_bundle,rebuild_account_report)
from tools.pattern_lab.account_study.codec import equal_records
from tools.pattern_lab.examples.account_auxiliary_synthetic import build_inputs
from tools.pattern_lab.auxiliary.study import load_links,read_evidence


def assert_contents(reader):
    groups={};sides=set();reasons=set();component_counts={};unknown_blocked=0
    isolated_blocks={'funding':0,'btc':0};availability_edges=0;recovered=0;zero_funding=False;spacing=set();missing_oi=False
    links=load_links(reader.root,reader.accountKeys)
    for key in reader.accountKeys:
        record=reader.account(key);r=record.result
        group=record.labels['allocationId']+'/'+record.labels['windowId']
        groups[group]=groups.get(group,0)+len(r.trades)
        sides.update(r.trades['side'].to_pylist());reasons.update(r.attempts['reason'].to_pylist())
        _,deps,meta,arrays=read_evidence(reader.root,links[key])
        for dep in deps.values():
            assert dep['series']['baseMultiplier']==1000
            if dep['series']['type']['typeId']=='settledFunding':
                zero_funding |= any(row['fundingRate']==0 for row in dep['rows'])
                stamps=[row['observedAtMs'] for row in dep['rows']]
                spacing.update(b-a for a,b in zip(stamps,stamps[1:]))
            else:
                from tools.pattern_lab.auxiliary.model import inspect_content
                missing_oi |= inspect_content(dep)['missingSampleSlots']>0
        for c in meta['components']:
            prefix='component.'+c['componentId']+'.';known=arrays[prefix+'known']
            counts=component_counts.setdefault(c['componentId'],dict(known=0,unknown=0,blockedLong=0,blockedShort=0,stale=0))
            counts['known']+=int(known.sum());counts['unknown']+=int((~known).sum())
            counts['stale']+=int((arrays[prefix+'unknownReason']==3).sum())
            availability_edges+=int((known & (arrays[prefix+'decisionMs']==arrays[prefix+'availableAtMs'])).sum())
            if c['componentId']=='oi':recovered+=int((~known[:-1] & known[1:]).sum())
            for side in ('Long','Short'):counts['blocked'+side]+=int((known & ~arrays[prefix+'allow'+side]).sum())
        if record.labels['allocationId']=='combined':
            fknown=arrays['component.funding.known'];bknown=arrays['component.btc.known']
            unknown_blocked+=sum(int((~fknown & bknown & ~arrays['component.btc.allow'+side]).sum()) for side in ('Long','Short'))
            for side in ('Long','Short'):
                fp,bp=arrays['component.funding.allow'+side],arrays['component.btc.allow'+side]
                isolated_blocks['funding']+=int((fknown & bknown & ~fp & bp).sum())
                isolated_blocks['btc']+=int((fknown & bknown & fp & ~bp).sum())
    assert len(groups)==10 and all(n>0 for n in groups.values()),groups
    assert sides=={-1,1} and 'filterBlocked' in reasons
    assert component_counts['funding']['stale']>0 and component_counts['oi']['unknown']>0
    assert all(component_counts[c]['blockedLong']>0 and component_counts[c]['blockedShort']>0 for c in ('funding','btc'))
    assert unknown_blocked>0
    assert all(n>0 for n in isolated_blocks.values()) and availability_edges>0 and recovered>0
    assert zero_funding and len(spacing)>1 and missing_oi
    assert any(groups['baseline/'+w['windowId']]!=groups['funding/'+w['windowId']] for w in reader.study['request']['windows'])
    assert reader.summary['schema']=='pattern-lab.account-metrics.v3'
    return dict(tradesByGroup=groups,componentCounts=component_counts,unknownAndBlocked=unknown_blocked,
        isolatedComponentBlocks=isolated_blocks,sameMillisecondAvailabilityEdges=availability_edges,
        recoveredOiLookbacks=recovered,knownZeroFunding=zero_funding,observedFundingIntervalsMs=sorted(spacing),missingOiSamples=missing_oi)


def test_v2_roundtrip_and_full_offline_validation(tmp_path,monkeypatch):
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux')
    assert normalize_request(raw)==normalize_request(normalize_request(raw))
    reader=run_account_study(raw,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/'study')
    counts=assert_contents(reader)
    assert len(reader.accountKeys)==20
    before=(reader.root/'completion.json').read_bytes()
    def denied(*a,**kw):raise AssertionError('offline pack/snapshot/executor access')
    monkeypatch.setattr('tools.pattern_lab.data.read_session',denied)
    monkeypatch.setattr('tools.pattern_lab.auxiliary.storage.load_snapshot',denied)
    monkeypatch.setattr('tools.pattern_lab.accounts.execute_account',denied)
    assert verify_account_study(reader.root)['commandRegeneration']=='exact'
    export_account_bundle(reader.root,zip_path=tmp_path/'bundle.zip')
    imported=import_account_bundle(tmp_path/'bundle.zip',output_root=tmp_path/'imported')
    assert assert_contents(imported)==counts
    assert verify_account_study(imported.root)['commandRegeneration']=='exact'
    for destination in ('report1','report2'):
        rebuild_account_report(imported.root,output_root=tmp_path/destination)
        for name in ('summary.json','report.html'):
            assert (tmp_path/destination/'derived'/name).read_bytes()==(reader.root/'derived'/name).read_bytes()
    assert (reader.root/'completion.json').read_bytes()==before


def test_direct_six_workers_debug_and_composition_order(tmp_path):
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux',targets=6)
    direct=run_account_study(raw,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/'direct')
    reversed_request=deepcopy(raw)
    next(a for a in reversed_request['allocations'] if a['allocationId']=='combined')['entryFilter']['allOf'].reverse()
    assert normalize_request(raw)==normalize_request(reversed_request)
    spawned=run_account_study(reversed_request,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},
        output_root=tmp_path/'spawned',workers=6,retention_profile='debug')
    assert spawned.status['effectiveWorkers']==6 and spawned.status['maximumInFlight']<=6
    assert direct.study['identities']==spawned.study['identities'] and direct.summary==spawned.summary
    assert assert_contents(direct)==assert_contents(spawned)
    for key in direct.accountKeys:assert equal_records(direct.account(key),spawned.account(key))
    assert verify_account_study(spawned.root)['commandRegeneration']=='exact'


@pytest.mark.parametrize('case',['pin','unit','mapping','unused','nested','duplicate','warmup','lookback','overlap'])
def test_static_admission_before_output(tmp_path,case):
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux')
    output=tmp_path/'out'
    if case=='pin':raw['auxiliaryInputs'][0]['expectedSnapshotContentId']='0'*64
    if case=='unit':raw['auxiliaryInputs'][0]['sources']['*']['unit']='wrong'
    if case=='mapping':raw['auxiliaryInputs'][0]['sources']={'BYBIT_AAAUSDT':raw['auxiliaryInputs'][0]['sources']['*']}
    if case=='unused':raw['auxiliaryInputs'].append(dict(deepcopy(raw['auxiliaryInputs'][0]),inputId='unused'))
    if case in ('nested','duplicate'):
        group=next(a for a in raw['allocations'] if a['allocationId']=='combined')['entryFilter']['allOf']
        group.append({'allOf':deepcopy(group)} if case=='nested' else deepcopy(group[0]))
    if case=='warmup':
        for a in raw['allocations']:
            for f in a['auxiliaryFeatures']:f['publicationLagMs']=20*3600000;f['maxAgeMs']=30*3600000
    if case=='lookback':next(a for a in raw['allocations'] if a['allocationId']=='oi')['auxiliaryFeatures'][0]['lookbackMinutes']=7
    if case=='overlap':output=tmp_path/'aux'/'inside'
    with pytest.raises(PatternLabDataError):run_account_study(raw,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=output)
    assert not output.exists()


@pytest.mark.parametrize('case',['requireComplete','zeroOverlap'])
def test_data_dependent_admission_is_failed_unsealed(tmp_path,case):
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux')
    if case=='requireComplete':raw['auxiliaryInputs'][0]['coveragePolicy']='requireComplete'
    else:
        from tools.pattern_lab.auxiliary import load_snapshot,publish_snapshot
        snapshot=load_snapshot(tmp_path/'aux');values=[snapshot.series(e['seriesId']) for e in snapshot.manifest['series']]
        for v in values:v['rows']=[]
        newer=publish_snapshot(values,output_root=tmp_path/'empty')
        for b in raw['auxiliaryInputs']:b['expectedSnapshotContentId']=newer.contentId
    with pytest.raises(PatternLabDataError,match='incomplete required|zero usable'):
        run_account_study(raw,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/('empty' if case=='zeroOverlap' else 'aux')},output_root=tmp_path/'out')
    assert json.loads((tmp_path/'out/status.json').read_text())['state']=='failed'
    assert not (tmp_path/'out/completion.json').exists()


def test_consumed_identity_parent_extension_and_recollection(tmp_path):
    from tools.pattern_lab.auxiliary import load_snapshot,publish_snapshot
    from tools.pattern_lab.examples.account_auxiliary_synthetic import ORIGIN,STEP,SLOTS
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux',targets=1)
    raw['allocations']=[a for a in raw['allocations'] if a['allocationId']=='funding'];raw['sources']=[]
    raw['auxiliaryInputs']=[b for b in raw['auxiliaryInputs'] if b['inputId']=='funding']
    original=load_snapshot(tmp_path/'aux')
    def execute(doc,root,name):return run_account_study(doc,data_root=tmp_path/'pack',auxiliary_roots={'aux':root},output_root=tmp_path/name)
    a=execute(raw,original.root,'a')
    values=[original.series(e['seriesId']) for e in original.manifest['series']]
    same=publish_snapshot(values,output_root=tmp_path/'recollected',provenance={'another':'retrieval'})
    assert same.contentId==original.contentId
    b=execute(raw,same.root,'b')
    assert a.study['identities']==b.study['identities']
    extended=deepcopy(values)
    for v in extended:
        if v['series']['seriesId']=='funding':
            v['rows'].append(dict(observedAtMs=ORIGIN+SLOTS*STEP,fundingRate=.125));v['coverage'][0][1]+=STEP
    future=publish_snapshot(extended,output_root=tmp_path/'future')
    future_request=deepcopy(raw);future_request['auxiliaryInputs'][0]['expectedSnapshotContentId']=future.contentId
    c=execute(future_request,future.root,'c')
    assert a.study['identities']['dataInput']!=c.study['identities']['dataInput']
    for k in a.accountKeys:assert a.account(k).result.identity==c.account(k).result.identity
    changed=deepcopy(values)
    next(v for v in changed if v['series']['seriesId']=='funding')['rows'][0]['fundingRate']=-.375
    altered=publish_snapshot(changed,output_root=tmp_path/'altered')
    altered_request=deepcopy(raw);altered_request['auxiliaryInputs'][0]['expectedSnapshotContentId']=altered.contentId
    d=execute(altered_request,altered.root,'d')
    for k in a.accountKeys:
        x,y=a.account(k).result,d.account(k).result
        assert x.commandId!=y.commandId
        np.testing.assert_array_equal(x.commands.entryFilter.allowLong,y.commands.entryFilter.allowLong)
        np.testing.assert_array_equal(x.commands.entryFilter.allowShort,y.commands.entryFilter.allowShort)


def test_full_check_detects_resealed_provider_command_tamper(tmp_path):
    from dataclasses import replace
    import shutil
    from tools.pattern_lab.account_study import load_account_study
    from tools.pattern_lab.account_study.codec import save_commands
    from tools.pattern_lab.account_study.common import read_json,write_json
    from tools.pattern_lab.accounts.execution import account_identity
    from tools.pattern_lab.account_study.artifacts import seal_directory
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux',targets=1)
    reader=run_account_study(raw,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/'run')
    first=reader.account(reader.accountKeys[0]);old=first.result.commands
    entry=old.long.entry.copy();entry[0]=not entry[0]
    forged=replace(old,long=replace(old.long,entry=entry));save_commands(reader.root,forged)
    for key in reader.accountKeys:
        record=reader.account(key);r=record.result
        if r.commandId!=old.identity:continue
        path=reader.root/'accounts'/key/'account.json';meta=read_json(path)
        meta['result']['commandId']=forged.identity
        meta['result']['identity']=account_identity(record.prepared,forged,r.policy,r.window,r.rules)
        write_json(path,meta)
    shutil.rmtree(reader.root/'commands'/old.identity)
    (reader.root/'completion.json').unlink();seal_directory(reader.root)
    assert load_account_study(reader.root).accountKeys==reader.accountKeys
    with pytest.raises(PatternLabDataError,match='commands.long array differs: entry'):verify_account_study(reader.root)


def test_v2_cli_and_report_metadata(tmp_path,capsys):
    from tools.pattern_lab.__main__ import main
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux',targets=1)
    path=tmp_path/'request.json';path.write_text(json.dumps(raw))
    assert main(['account-study','--request',str(path),'--data-root',str(tmp_path/'pack'),
        '--auxiliary-root','aux='+str(tmp_path/'aux'),'--output-root',str(tmp_path/'run')])==0
    assert json.loads(capsys.readouterr().out)['accounts']==10
    html=(tmp_path/'run/derived/report.html').read_text(encoding='utf-8')
    for text in ('Auxiliary threshold-crossing entry signal','Combined all-of:','rate per settlement','not exchange observations'):
        assert text in html
    assert '<script src=' not in html
    assert main(['aux-inspect','--snapshot-root',str(tmp_path/'aux'),'--verify'])==0
    assert json.loads(capsys.readouterr().out)['integrityVerified']


def test_node_auxiliary_report(tmp_path):
    import shutil,subprocess
    from pathlib import Path
    node=shutil.which('node')
    if node is None:pytest.skip('Node unavailable on PATH; v2 auxiliary report interaction gate pending Linux Node review')
    raw=build_inputs(tmp_path/'pack',tmp_path/'aux')
    r=run_account_study(raw,data_root=tmp_path/'pack',auxiliary_roots={'aux':tmp_path/'aux'},output_root=tmp_path/'run')
    harness=Path(__file__).with_name('account_report_interactions.cjs')
    subprocess.run([node,str(harness),str(r.root/'derived/report.html')],check=True,capture_output=True,text=True)


def test_public_example_outside_repository_without_pythonpath(tmp_path):
    import os,subprocess,sys
    from pathlib import Path
    from tools.pattern_lab.account_study import load_account_study
    script=Path(__file__).resolve().parents[2]/'tools/pattern_lab/examples/account_auxiliary_synthetic.py'
    env=dict(os.environ);env.pop('PYTHONPATH',None)
    result=subprocess.run([sys.executable,'-B','-X','utf8',str(script),'--output-root',str(tmp_path/'example')],
        cwd=tmp_path,env=env,capture_output=True,text=True,check=True)
    receipt=json.loads(result.stdout)
    assert receipt['verification']['commandRegeneration']=='exact'
    assert_contents(load_account_study(receipt['runRoot']))
