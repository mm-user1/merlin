from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json
import math
import shutil
import subprocess

import numpy as np
import pytest

from tools.pattern_lab.account_study import run_account_study, rebuild_account_report, verify_account_study
from tools.pattern_lab.account_study.metrics import account_metrics, aggregate_metrics
from tools.pattern_lab.account_study import robustness as rb
from tools.pattern_lab.account_study.common import read_json, file_hash
from ._account_robustness_helpers import legacy_records, build_multi_month, assert_multi_month, assert_numbers


def test_portable_pre_t02_metrics():
    expected=read_json(Path(__file__).with_name('fixtures')/'account_metrics_v1.json')
    records=legacy_records(); accounts=[account_metrics(r) for r in records]
    assert_numbers(expected['accounts'],accounts,subset=True)
    assert_numbers(expected['aggregate'],aggregate_metrics(iter(records),accounts),subset=True)
    assert all(a['schema']=='pattern-lab.account-metrics.v2' for a in accounts)


def test_shared_numeric_vectors():
    for v in read_json(Path(__file__).with_name('fixtures')/'robustness_numbers.json'):
        assert rb.close(v['a'],v['b']) is v['close']
        assert rb.close(v['b'],v['a']) is v['close']
    assert rb.direction(1e-8)=='flat' and rb.direction(1.01e-8)=='up'
    assert rb.direction(-1.01e-8)=='down' and rb.direction(None) is None
    assert not rb.close(float('inf'),float('inf'))


def test_exact_overlap_and_new_order_only():
    def w(name,start,end):return dict(windowId=name,startUtc=start,endUtc=end)
    doc=dict(windows=[w('z','2020-01-15T00:00:00Z','2020-02-15T00:00:00Z'),
        w('a','2020-02-01T00:00:00Z','2020-02-15T00:00:00Z'),
        w('same','2020-01-15T00:00:00Z','2020-02-15T00:00:00Z'),
        w('touch','2020-02-15T00:00:00Z','2020-02-16T00:00:00Z'),
        w('apart','2020-03-01T00:00:00Z','2020-03-02T00:00:00Z')])
    original=deepcopy(doc);facts=rb.window_facts(doc)
    assert doc==original
    assert [w['windowId'] for w in facts['windows']]==['same','z','a','touch','apart']
    rows={(r['windowA'],r['windowB']):r for r in facts['overlaps']}
    assert rows['same','z']['sharedPctOfA']==rows['same','z']['sharedPctOfB']==100
    assert rows['z','a']['sharedMs']==14*86400000
    assert rows['z','a']['sharedPctOfA']==pytest.approx(100*14/31)
    assert rows['z','a']['sharedPctOfB']==100
    assert rows['z','touch']['sharedMs']==rows['z','apart']['sharedMs']==0


def test_passive_strict_endpoints_midnight_and_gap():
    m=dict(month='1970-01',startMs=0,endMs=40,partialEdgeMonth=True)
    target=SimpleNamespace(timestamps=np.array([0,10,30]),values=np.array([[100,120,90,105],[105,120,90,110],[110,120,90,125.]]))
    cell=rb.passive_month(target,10,m)
    assert cell['returnPct']==25 and cell['expectedBars']==4 and cell['observedBars']==3 and cell['interiorGap']
    # Close at R belongs to this interval; a later bar is never borrowed.
    missing=rb.passive_month(SimpleNamespace(timestamps=np.array([10,30]),values=target.values[1:]),10,m)
    assert missing['returnPct'] is None and missing['reason']=='missingOpeningBar'
    missing=rb.passive_month(SimpleNamespace(timestamps=np.array([0,10]),values=target.values[:2]),10,m)
    assert missing['reason']=='missingClosingBar'
    assert rb.passive_month(target,10,dict(m,startMs=1))['reason']=='unalignedInterval'


def removal_input(nets, pnls):
    months=[dict(month=str(i),pnl=v,partialEdgeMonth=i==0,allAccountsObserved=True,
        openingExact=True,closingExact=True,observedBars=1,expectedBars=1) for i,v in enumerate(pnls)]
    return dict(netPnl=math.fsum(nets),initialCapital=100*len(nets),months=months),[
        dict(instrumentId=chr(65+i),netPnl=v) for i,v in enumerate(nets)]


@pytest.mark.parametrize('nets,pnls', [([60,60,-80,0],[50,-10]),([10],[10]),([10,20],[5,25]),
    ([-10,0],[-10]),([0,0],[0]),([10,-20],[-10]),([10,-10+1e-9],[10,-10+1e-9])])
def test_removal_denominators_and_reconciliation(nets,pnls):
    aggregate,members=removal_input(nets,pnls);r=rb.removals(aggregate,members)
    for row in r['tickerExTop']:
        rest=[s for s in members if s['instrumentId'] not in row['removedIds']]
        assert row['remainingCount']==len(rest)
        if rest:
            assert row['remainingReturnPct']==pytest.approx(sum(s['netPnl'] for s in rest)/len(rest),abs=1e-8)
        else:assert row['remainingReturnPct'] is None and row['status']=='emptyRemainingRoster'
    assert r['monthExBest']['remainingContributionPct']==pytest.approx(100*(sum(nets)-max([0]+pnls))/(100*len(nets)))


def test_rank_ties_erasure_and_month_interpretation():
    a,s=removal_input([60,60,-80,0],[50,-10]);r=rb.removals(a,s)
    assert r['tickerExTop'][0]['removedIds']==['A']
    assert r['tickerExTop'][1]['removedCount']==2
    assert r['tickerProfitErasure']['removedCount']==1 and r['tickerProfitErasure']['removedFraction']==.25
    assert r['monthProfitErasure']['removedIds']==['0']
    a,s=removal_input([10,20],[10,20]);r=rb.removals(a,s)
    assert r['tickerProfitErasure']['status']=='noNonemptySubsetErasesProfit'
    assert r['monthProfitErasure']['allRemoved'] and r['monthInterpretation']=='Accounting removal; no account re-execution'
    a,s=removal_input([-1,0],[-1,0]);r=rb.removals(a,s)
    assert r['tickerProfitErasure']['removedCount'] is None and r['monthExBest']['removedCount']==0


def test_sides_use_closed_trades_including_zero_holding_and_zero_side():
    records=legacy_records()
    for record in records:
        a=account_metrics(record);s=a['robustness']['sides']
        for key in ('trades','grossPnl','fees','netPnl','holdingMs','timeInMarketPct'):
            assert sum(v[key] for v in s)==pytest.approx(a[key],rel=1e-9,abs=1e-8)
        assert a['forcedExits']['gapBoundary']['trades']>0
    split=rb.sides([dict(side=1,grossPnl=10,entryFee=1,exitFee=1,netPnl=8,holdingMs=0)],100,1000)
    assert split[0]['contributionPct']==8 and split[0]['timeInMarketPct']==0
    assert split[1]['trades']==0 and split[1]['netPnl']==0


def test_unavailable_aggregate_retains_calendar_edges_without_false_coverage():
    template=[dict(month='2020-01',startMs=0,endMs=10,partialEdgeMonth=True)]
    summary=dict(months=template,robustness=dict(passiveMonths=[dict(template[0],status='unavailable',
        reason='missingOpeningBar',returnPct=None,expectedBars=1,observedBars=0,interiorGap=False)]),instrumentId='A')
    a=dict(status='unavailable',reason='noObservedBars',months=[])
    facts=rb.aggregate_facts(a,[summary])
    assert facts['partialEdgeMonths']==1 and facts['incompleteAccountMonths'] is None
    assert facts['observedMonths'] is None and facts['nonExactBoundaryMonths'] is None
    assert facts['removals']['status']=='unavailable'
    assert facts['sides'][0]['netPnl'] is None
    assert facts['passiveMonths'][0]['usableAccounts']==0


def test_pair_eligibility_keeps_partial_edges_but_excludes_nonexact_and_missing_returns():
    # Exercise the actual reduction using an available record, then vary only monthly facts.
    records=legacy_records(); summaries=[account_metrics(r) for r in records]
    a=aggregate_metrics(iter(records),summaries)
    a['months'][0]['partialEdgeMonth']=True
    rb.aggregate_facts(a,summaries)
    assert a['months'][0]['pairEligible']
    for key,value in [('openingExact',False),('closingExact',False),('allAccountsObserved',False),('returnPct',None)]:
        changed=deepcopy(a);changed['months'][0][key]=value
        rb.aggregate_facts(changed,summaries)
        assert not changed['months'][0]['pairEligible']


@pytest.fixture(scope='module')
def multi_month(tmp_path_factory):
    root=tmp_path_factory.mktemp('robustness-multimonth')
    request=build_multi_month(root/'pack')
    return run_account_study(request,data_root=root/'pack',output_root=root/'study')


def test_nonvacuous_months_coverage_removal_and_attribution(multi_month):
    summary=multi_month.summary
    assert_multi_month(summary)
    assert [w['windowId'] for w in summary['robustness']['windows']]==['early','late','dec']
    assert [a['windowId'] for a in summary['aggregates'][:3]]==['dec','early','late']
    a=next(a for a in summary['aggregates'] if a['allocationId']=='rr2' and a['windowId']=='early')
    assert a['monthBreadth']['partialMonths']==0 and a['robustness']['partialEdgeMonths']==2
    assert not a['months'][0]['closingExact'] and not a['months'][0]['pairEligible']
    passive=a['robustness']['passiveMonths'][0]
    assert passive['usableAccounts']==2 and passive['plannedAccounts']==3 and passive['returnPct'] is None
    assert summary['derivation']['inputStudy']==dict(schema=multi_month.study['schema'],identities=multi_month.study['identities'])
    sources=summary['derivation']['sources']
    assert 'tools/pattern_lab/account_study/robustness.py' in sources
    assert list(sources)==sorted(sources)
    from tools.pattern_lab.account_study.runner import implementation
    assert 'tools/pattern_lab/account_study/robustness.py' in implementation()['sources']


def test_multi_month_offline_rebuild(multi_month,tmp_path,monkeypatch):
    before={p.relative_to(multi_month.root).as_posix():file_hash(p) for p in multi_month.root.rglob('*') if p.is_file()}
    def denied(*a,**kw):raise AssertionError('offline pack access')
    monkeypatch.setattr('tools.pattern_lab.data.read_session',denied)
    assert verify_account_study(multi_month.root)['accountsChecked']==len(multi_month.accountKeys)
    for name in ('one','two'):rebuild_account_report(multi_month.root,output_root=tmp_path/name)
    for name in ('summary.json','report.html'):
        assert (tmp_path/'one/derived'/name).read_bytes()==(tmp_path/'two/derived'/name).read_bytes()==(multi_month.root/'derived'/name).read_bytes()
    assert before=={p.relative_to(multi_month.root).as_posix():file_hash(p) for p in multi_month.root.rglob('*') if p.is_file()}


def test_node_generated_report(multi_month):
    node=shutil.which('node')
    if node is None:pytest.skip('Node unavailable on PATH; generated-report interaction gate pending Linux Node review')
    harness=Path(__file__).with_name('account_report_interactions.cjs')
    subprocess.run([node,str(harness),str(multi_month.root/'derived/report.html')],check=True,timeout=60)
