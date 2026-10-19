"""Independent hand accounting, scorer boundaries, and fixed hash vectors."""
from copy import deepcopy
import math

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.selection import normalize_selection_request, Score, Scorer, check_deterministic, check_prefix_invariance
from tools.pattern_lab.selection.arithmetic import calculate, rank, boundary_tie, random_order, distribution, quantile
from tools.pattern_lab.selection.ledger import ScoreView, history_pool, money, covered
from tools.pattern_lab.selection.scorers import BUILTINS, evaluate
from tools.pattern_lab.selection.report import display_paths
from tools.pattern_lab.selection.request import month_bounds
from tools.pattern_lab.study.contracts import semantic_digest
from ._selection_helpers import request_template, hand_case


@pytest.mark.parametrize('case',['unknown','bool','nan','capital','month','duplicate','gap','history','lookback','mode','k','semantic','version'])
def test_closed_request_refusals(case):
    doc=request_template()
    if case=='unknown':doc['extra']=1
    if case=='bool':doc['randomControl']['seed']=True
    if case=='nan':doc['referenceCapital']=float('nan')
    if case=='capital':doc['referenceCapital']=0
    if case=='month':doc['monthWindows'][0]['month']='2020-1'
    if case=='duplicate':doc['monthWindows'][1]['windowId']='m1'
    if case=='gap':doc['monthWindows'].pop(1)
    if case=='history':doc['evaluationMonths']=['2020-04']
    if case=='lookback':doc['selectors'][0]['parameters']['months']=5
    if case=='mode':doc['capitalMode']='carried'
    if case=='k':doc['kValues']=[0]
    if case=='semantic':doc['selectors'].append(dict(deepcopy(doc['selectors'][0]),selectorId='other'))
    if case=='version':doc['selectors'][0]['version']='2'
    with pytest.raises(PatternLabDataError):normalize_selection_request(doc)


def test_normalization_order_notes_and_json_structure():
    doc=request_template();a=normalize_selection_request(doc)
    doc['monthWindows'].reverse();doc['evaluationMonths'].reverse();doc['kValues'].reverse();doc['notes']='not economic'
    b=normalize_selection_request(doc)
    assert a.identity==b.identity
    b.document['kValues'].append(99)
    assert b.document['kValues']==[1,3]
    doc['selectors'][0].update(scorerId='x.missing',parameters={'arbitrary':['strict',1,None,True]})
    assert normalize_selection_request(doc,stored=True).document['selectors'][0]['parameters']['arbitrary'][0]=='strict'
    with pytest.raises(PatternLabDataError):normalize_selection_request(doc)
    doc['selectors'][0]['parameters']={1:'not a string key'}
    with pytest.raises(PatternLabDataError):normalize_selection_request(doc,stored=True)


def test_hand_fixed_reference_denominators_drawdown_and_tie():
    doc,ledger,paths,roster,scores=hand_case()
    output,curves=calculate(doc,ledger,paths,roster,scores)
    one,three=output['comparisons']
    assert one['fullPeriod']['selected']==dict(cumulativeReturnPp=5.,maxDrawdownPp=20.,netPnl=5.)
    assert one['fullPeriod']['allEligible']==dict(cumulativeReturnPp=10.,maxDrawdownPp=10.,netPnl=20.)
    assert three['fullPeriod']['selected']['cumulativeReturnPp']==pytest.approx(20/3)
    assert three['months'][0]['selected']['idleSlots']==1
    assert three['months'][0]['allEligible']['plannedCapital']==200.
    assert three['months'][0]['selected']['plannedCapital']==300.
    assert one['months'][0]['boundaryTie']==dict(key=[2.],members=['A','B'],totalMembers=2,slots=1,chosen=['A'])
    assert one['months'][0]['tieNeutralExpectedReturnPct']==10.
    # Same endpoint, radically different drawdown; endpoint-only DD is wrong.
    altered=deepcopy(paths);altered['2020-05']['pnl'][0]=[0,5,10]
    changed,_=calculate(doc,ledger,altered,roster,scores)
    assert changed['comparisons'][0]['fullPeriod']['selected']['cumulativeReturnPp']==5.
    assert changed['comparisons'][0]['fullPeriod']['selected']['maxDrawdownPp']==5.


def test_common_mask_preserves_membership_and_partial_distribution():
    doc,ledger,paths,roster,scores=hand_case()
    baseline,_=calculate(doc,ledger,paths,roster,scores)
    # B is not Top-1; its missing future data still invalidates the common mask.
    row=next(r for r in ledger if r['month']=='2020-06' and r['instrumentId']=='B')
    row.update(available=False,terminalStatus='noObservedBars',observedBars=0)
    result,curves=calculate(doc,ledger,paths,roster,scores);c=result['comparisons'][0]
    assert c['months'][1]['selected']['members']==baseline['comparisons'][0]['months'][1]['selected']['members']
    assert not c['months'][1]['available'] and c['months'][1]['selected']['returnPct'] is None
    assert all(r['returnPct'] is None for r in c['months'][1]['random'])
    assert c['fullPeriod']['selected']['cumulativeReturnPp'] is None
    assert c['fullPeriod']['selected']['maxDrawdownPp'] is None
    assert c['matchedAvailableMonths']['months']==['2020-05']
    assert c['matchedAvailableMonths']['randomReturnPp']==[10.]*4
    assert c['matchedAvailableMonths']['distribution']['equal']==4
    assert len(display_paths(c,curves,'selected'))==1
    for r in ledger:r.update(available=False,terminalStatus='noObservedBars',observedBars=0)
    empty,_=calculate(doc,ledger,paths,roster,scores)
    partial=empty['comparisons'][0]['matchedAvailableMonths']
    assert partial['months']==[] and partial['selectedReturnPp'] is None and partial['distribution']['equal'] is None


def test_empty_pool_cash_and_method_pool_denominators():
    doc,ledger,paths,roster,scores=hand_case()
    for b in scores:b['rows']=[]
    result,_=calculate(doc,ledger,paths,roster,scores)
    for c in result['comparisons']:
        assert c['emptyPoolMonths']==2 and c['fullPeriod']['selected']['cumulativeReturnPp']==0.
        for m in c['months']:
            assert m['selected']['idleSlots']==c['k'] and m['allEligible']['plannedCapital']==0.
    doc,ledger,paths,roster,scores=hand_case()
    for b in scores:b['rows'][1].update(eligible=False,key=None,reason='method exclusion')
    result,_=calculate(doc,ledger,paths,roster,scores)
    for c in result['comparisons']:
        for m in c['months']:
            assert m['eligiblePool']==['A'] and m['allEligible']['fundedSlots']==1
            assert all(r['members']==['A'] for r in m['random'])


def test_exact_keys_sorting_all_tuple_levels_and_uniform_tie_expectation():
    rows=[dict(instrumentId=i,eligible=True,key=k) for i,k in [('D',[2,1,9]),('C',[2,2,0]),('B',[2,2,1]),('A',[2,2,1])]]
    assert [r['instrumentId'] for r in rank(rows)]==['A','B','C','D']
    assert boundary_tie(rank(rows),1)['chosen']==['A']
    rows[0]['key']=[2,2,1+1e-12]
    assert rank(rows)[0]['instrumentId']=='D'  # exact keys, no comparison tolerance
    doc,ledger,paths,roster,scores=hand_case()
    ledger[1]['netPnl']=30.;paths['2020-05']['pnl'][1,-1]=30.
    result,_=calculate(doc,ledger,paths,roster,scores)
    assert result['comparisons'][0]['months'][0]['tieNeutralExpectedReturnPct']==20.


def test_random_golden_vectors_nested_k_and_quantiles():
    assert semantic_digest(['random-k.1',7,0,'2020-05','D'])=='7e3aebeb797ed88942239c5a57d4812a50097b44e664c5eade53b0e809a47422'
    assert random_order(['A','B','C','D'],7,0,'2020-05')==['D','B','A','C']
    assert random_order(['D','B','A','C'],7,0,'2020-05')[:2]==['D','B']
    assert quantile([0.,10.,20.,30.],.1)==pytest.approx(3.)
    d=distribution([1.,1.+1e-10,2.,0.],1.)
    assert [d[k] for k in ('p10','median','p90')]==pytest.approx([.3,1.00000000005,1.70000000003])
    assert (d['below'],d['equal'],d['above'],d['reason'])==(1,2,1,None)


def test_coverage_history_no_backfill_and_valid_zero():
    rows=[dict(instrumentId='A',month=f'2020-{m:02d}',available=True,terminalStatus='closed',observedBars=99,expectedBars=100,returnPct=0.) for m in range(1,6)]
    assert history_pool(rows,['A'],['2020-01','2020-02'],99.)[0]==['A']
    rows[1].update(available=False,observedBars=0,terminalStatus='noObservedBars')
    pool,excluded=history_pool(rows,['A'],['2020-02','2020-03'],90.)
    assert pool==[] and excluded[0]['reasons']==[dict(month='2020-02',reason='noObservedBars')]
    assert covered(rows[0],99.) and not covered(rows[0],100.)
    assert money(100.1,100.)==(100.1-100.,(100.*(100.1-100.))/100.)


def test_builtins_view_immutability_and_prefix_helpers():
    rows=[dict(instrumentId='A',month=f'2020-{i+1:02d}',returnPct=v) for i,v in enumerate([1.,0.,-2.,3.])]
    view=ScoreView(['A'],[r['month'] for r in rows],100,rows)
    assert evaluate(BUILTINS['historicalConsistency4m'],view,{})[0]['key']==[2,-2.,.5]
    assert evaluate(BUILTINS['positiveMonthCount'],view,{'months':4})[0]['key']==[2]
    with pytest.raises(TypeError):view.months('A')[0]['returnPct']=999
    rows[-1]['returnPct']=999  # original caller buffer is not retained
    assert check_prefix_invariance(BUILTINS['historicalConsistency4m'],view,view,{})[0]['key']==[2,-2.,.5]
    assert check_deterministic(BUILTINS['meanMonthlyReturn'],view,{'months':4})[0]['key']==[.5]


@pytest.mark.parametrize('bad',['omitted','duplicate','extra','nan','key','mutation'])
def test_experimental_output_and_mutation_refusal(bad):
    view=ScoreView(['A'],['2020-01'],1,[dict(instrumentId='A',returnPct=1.)])
    def score(v,p):
        if bad=='mutation':v.cutoffMs=2
        row=Score('EXTRA' if bad=='extra' else 'A',True,(float('nan'),) if bad=='nan' else (1.,2.) if bad=='key' else (1.,),None,{})
        return [] if bad=='omitted' else [row,row] if bad=='duplicate' else [row]
    scorer=Scorer('x.test','1',lambda p:p,lambda p:1,('value',),score)
    with pytest.raises(PatternLabDataError):evaluate(scorer,view,{})


def test_display_sampling_preserves_month_extrema_and_cap():
    doc,ledger,paths,roster,scores=hand_case();result,curves=calculate(doc,ledger,paths,roster,scores)
    comparison=result['comparisons'][0]
    for month in comparison['months']:
        curve=curves[month['curves']['selected']]
        start=curve['times'][0];curve['times']=np.arange(start,start+10000,dtype=np.int64)
        curve['values']=np.zeros(10000);curve['values'][123]=-99.;curve['values'][789]=55.;curve['values'][-1]=month['selected']['returnPct']
    sampled=display_paths(comparison,curves,'selected')
    assert sum(len(s['points']) for s in sampled)<=2000
    for j,segment in enumerate(sampled):
        values=[p[1] for p in segment['points']];offset=0 if j==0 else 10
        assert -99+offset in values and 55+offset in values
        assert segment['points'][0][0]==month_bounds(segment['month'])[0]
