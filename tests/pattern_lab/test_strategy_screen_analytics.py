from tools.pattern_lab.strategy_screen.analytics import stats,residual,classification
from tools.pattern_lab.strategy_screen import analytics as a
from tools.pattern_lab.strategy_screen.request import normalize_request,configurations,calendar
from ._strategy_screen_helpers import request,compact_unit
from copy import deepcopy


def test_quantiles_and_empty():
    assert stats([0,10])['q25']==2.5
    assert stats([])['median'] is None and stats([])['n']==0


def test_remove_all_exact_zero_and_tiny_nonempty_residual():
    row=dict(metrics={'returnPct':1e-12},months=[dict(month='a',exact=True,contributionPp=0)])
    assert residual(row,1)['value']==0
    row['months'].append(dict(month='b',exact=True,contributionPp=0))
    assert residual(row,1)['value']==1e-12
    row['months'][0]['exact']=False
    assert residual(row,2)['value'] is None


def test_zero_return_and_unavailable_annualization():
    unit=dict(status='complete',metrics=dict(coverage=dict(expectedBars=10,observedBars=10),
        returnPct=0,annualizedProfitPct=0,annualizedProfitStatus='available',mtmMaxDdPct=0,trades=0,performanceStatus='available'))
    criteria=dict(annualizedProfitPct=0,mtmMaxDdPct=200,minClosedTrades=0)
    value=classification(unit,criteria)
    assert value['classifiable'] and value['interesting'] and value['profitable'] is False
    unit['metrics']['annualizedProfitPct']=None;unit['metrics']['annualizedProfitStatus']='tooShort'
    value=classification(unit,criteria)
    assert value['profitable'] is value['interesting'] is None and value['lowActivity'] is False


def projection(ordinal=0):
    doc=normalize_request(request()).document;c=configurations(doc)[0];p=doc['periods'][0]
    return a.project(compact_unit(c,p,ordinal),c,doc),doc,p


def test_unequal_grid_weighting_pooled_medians_and_overlapping_failures():
    row,doc,period=projection();rows=[deepcopy(row) for _ in range(4)]
    for i,r in enumerate(rows):
        r['cellId']='one' if i==0 else 'three';r['metrics'].update(returnPct=[100,-3,-2,-1][i],annualizedProfitPct=[100,-3,-2,-1][i],mtmMaxDdPct=200,trades=0)
        r['classification'].update(classifiable=True,profitable=i==0,interesting=False,lowActivity=True)
        for m in r['months']:m['contributionPp']=1 if i==0 else -1
    value=a.period_summary(rows,period,doc)
    assert value['breadth']['profitable']['pct']==25 and value['breadth']['profitable']['equalCellPct']==50
    assert value['metrics']['returnPct']['median']==-1.5
    assert value['failures']==dict(declared=4,classifiable=4,unavailable=0,annualizedReturn=3,drawdown=4,activity=4)
    assert value['monthly'][0]['positiveContribution']['pct']==25
    assert value['monthly'][0]['equalCellMeanContributionPp']==0


def test_per_metric_population_and_missing_common_month():
    row,doc,period=projection();row['metrics']['annualizedProfitPct']=None;row['classification'].update(classifiable=False,profitable=None,interesting=None,lowActivity=True)
    row['months'][1].update(exact=False,contributionPp=None,cumulativeReturnPct=None)
    value=a.period_summary([row],period,doc)
    assert value['metrics']['returnPct']['n']==1 and value['metrics']['annualizedProfitPct']['n']==0
    assert value['activity']==dict(known=1,unknown=0,low=1)
    assert value['monthly'][1]['positiveContribution']['pct'] is None
    assert value['latestThreeMonths']['returnPct']['n']==0
    assert value['concentration']['common']['1']['pooledPositivePct'] is None
    assert value['concentration']['common']['1']['reason']=='incompleteScopeEvidence'


def test_nonpositive_window_opening_retains_contribution_and_trades():
    row,doc,period=projection();row['months'][0]['openingEquity']=-10;row['months'][-1]['closingEquity']=10
    slots=calendar(period);v=a.window_summary([row],slots,slots[0]['startMs'],slots[-1]['endMs'])
    assert v['returnPct']['n']==0 and v['contributionPp']['n']==v['trades']['n']==1
    assert v['contributionPp']['median']==.2 and v['profitable']['pct'] is None
    assert v['profitable']['reason']=='nonpositiveOpeningEquity'


def test_pooled_persistence_and_empty_groups():
    row,doc,period=projection();earlier=[];recent=[]
    for i in range(4):
        e=deepcopy(row);e.update(configurationId=str(i),cellId='small' if i==0 else 'large');r=deepcopy(e)
        e['classification'].update(classifiable=True,profitable=i<3,interesting=i==0)
        r['classification'].update(classifiable=True,profitable=i in (0,3),interesting=False)
        earlier.append(e);recent.append(r)
    value=a.persistence(earlier,recent)
    assert value['profitable']['earlierSuccess']['pct']==100/3
    assert value['profitable']['earlierNonsuccess']['pct']==100
    empty=a.persistence([],[]);assert empty['interesting']['earlierSuccess']['pct'] is None
    doc['cells']=[]
    assert list(a.derive(doc,[]))[0]['changes']['profitableBreadthPp'] is None


def test_chronological_ties_too_few_months_and_link_formula():
    row,doc,period=projection()
    for m in row['months']:m['contributionPp']=1
    assert a.residual(row,1)['selectedMonths']==[row['months'][0]['month']]
    row['months']=row['months'][:1]
    assert a.residual(row,2)['reason']=='tooFewMonths'
    assert 100*((1+50/100)*(1-20/100)-1)==pytest.approx(20)


import pytest
