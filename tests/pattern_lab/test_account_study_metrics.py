from dataclasses import replace
from datetime import datetime,timezone
import numpy as np
import pytest
import pyarrow as pa

from tools.pattern_lab.accounts import execute_account
from tools.pattern_lab.account_study.codec import AccountRecord
from tools.pattern_lab.account_study.metrics import drawdown,monthly,account_metrics,aggregate_metrics,synchronized
from tools.pattern_lab.account_study.report import allocation_labels,render
from ._account_helpers import prepared,bundle,side,policy,window,rules
from ._account_study_helpers import request


def test_full_path_drawdown_and_aggregate_are_not_monthly_or_mean_dd():
    assert drawdown([110,70,120],100)==pytest.approx(100*40/110)
    assert drawdown([110,120],100)==0
    assert drawdown([-20],100)==120
    # Asynchronous account losses: portfolio DD differs from mean individual DD.
    assert drawdown(np.array([100,80,100])+np.array([100,120,100]),200)==0
    assert (drawdown([100,80,100],100)+drawdown([100,120,100],100))/2>0


def test_month_boundaries_missing_month_and_partial_edges():
    ms=lambda s:int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()*1000)
    start,end=ms('2020-01-15'),ms('2020-04-15')
    closes=np.array([ms('2020-02-01'),ms('2020-03-01'),ms('2020-04-01'),end],np.int64)
    rows=monthly(closes,np.array([110.,110.,90.,105.]),np.array([1,0,1,1]),start,end,100.,105.)
    assert [r['month'] for r in rows]==['2020-01','2020-02','2020-03','2020-04']
    assert [r['pnl'] for r in rows]==[10,0,-20,15]
    assert rows[1]['returnPct'] is None and rows[1]['returnStatus']=='noObservedBars'
    assert rows[0]['closingExact'] and not rows[1]['closingExact']
    assert rows[0]['partialEdgeMonth'] and rows[-1]['partialEdgeMonth']
    assert sum(r['contributionPct'] for r in rows)==5
    negative=monthly(np.array([end]),np.array([-10.]),np.array([1]),start,end,100.,-10.)
    assert sum(r['pnl'] for r in negative)==-110


@pytest.mark.parametrize('same_open',[False,True])
def test_exposure_clock_occupied_bar_union_and_passive(same_open):
    p=prepared([(100,102,98,100),(89 if same_open else 100,102,88,100)])
    c=bundle(p,long=side(2,entries=[0],stop=90))
    r=execute_account(p,c,policy(),window(p),rules())
    m=account_metrics(AccountRecord(p,r,dict(instrumentId='x',allocationId='a',windowId='w')))
    assert m['holdingMs']==0 and m['timeInMarketPct']==0
    assert m['occupiedBarSharePct']==50 and m['returnPctPerExposureDay'] is None
    assert m['passive']['longReturnPct']==0 and m['passive']['shortReturnPct']==0
    assert m['fees']>0 and m['trades']==1


def test_zero_trades_gaps_missing_head_tail_and_unobserved_account():
    p=prepared([(100,102,98,100)]*4,slots=[1,2,5,6],end=8*1800000)
    c=bundle(p)
    r=execute_account(p,c,policy(),window(p),rules())
    record=AccountRecord(p,r,dict(instrumentId='x',allocationId='a',windowId='w'))
    t,eq,seen=synchronized(record)
    assert eq.tolist()==[10000]*8 and seen.tolist()==[False,True,True,False,False,True,True,False]
    m=account_metrics(record)
    assert m['profitFactor'] is None and m['winRatePct'] is None and m['trades']==0
    assert m['coverage']['missingHeadSlots']==m['coverage']['missingTailSlots']==1
    empty=execute_account(p,c,policy(),window(p,3*1800000,5*1800000),rules())
    empty_record=replace(record,result=empty)
    metrics=account_metrics(empty_record)
    assert metrics['terminalStatus']=='noObservedBars' and metrics['curve']==[]
    assert aggregate_metrics(iter([empty_record]),[metrics])['status']=='unavailable'


def test_report_safe_embedding_and_rr_groups():
    raw=request(['BYBIT_AAAUSDT'])
    labels=allocation_labels(raw)
    assert labels['rr2'].startswith('RR ') and labels['rr3'].startswith('RR ')
    assert labels['trailing']=='trailing'
    raw['allocations'][1]['provider']['recipe']['stopProvider']='haSwing2'
    assert allocation_labels(raw)['rr3']=='rr3'
    raw['notes']='</script><script>alert(1)</script>'
    html=render(raw,dict(accounts=[],aggregates=[],limitations=[]),{})
    assert raw['notes'] not in html and '\\u003c/script' in html
    assert 'https://' not in html and '<link' not in html and '<script src=' not in html
    assert 'light' not in html or 'background:white' in html
