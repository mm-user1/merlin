from copy import deepcopy
import json
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.strategy_screen_report.model import (
    VIEW, PRESENTATION, presentation_options, histogram, histogram_scale, quantiles, view_bytes)
from tools.pattern_lab.strategy_screen_report.payload import build_view, eligible, setting_period
from tools.pattern_lab.strategy_screen.analytics import stats
from ._strategy_screen_report_helpers import compact_report_input


@pytest.fixture
def reader(tmp_path):
    return compact_report_input(tmp_path/'input')


def test_saved_statistics_populations_and_independent_month_availability(reader):
    view = build_view(reader, presentation_options(None, reader.metadata))
    assert view['schema'] == VIEW and view['synthetic'] is True
    for scope in reader.scopes:
        sid = scope['scopeId']; native = reader.summary(sid); mapped = view['scopes'][sid]
        for pid, p in native['periods'].items():
            b = mapped['periods'][pid]
            for k, q in p['metrics'].items():
                assert b['medians'][k] == quantiles(q)
            for k in ('returnPct', 'annualizedProfitPct'):
                assert sum(b['distributions'][k]['counts']) == p['metrics'][k]['n']
            for i, month in enumerate(p['monthly']):
                d = view['detail']['monthly'][sid][pid][i]
                assert sum(d['distribution']['counts']) == month['contribution']['n']
                assert d['cumulative'] == quantiles(month['cumulative'])
            assert b['profitable']['pct'] == p['breadth']['profitable']['equalCellPct']
    missing = view['detail']['settings'][0]['periods']['earlier']
    assert missing['returnPct'] is not None and not missing['coverageComplete']
    assert missing['contributionPp'][1] is None and missing['contributionPp'][2] is not None
    assert missing['cumulativeReturnPct'][2] is None


@pytest.mark.parametrize('values,step,edges,counts', [
    ([], 10, [0,10], [0]), ([0],10,[0,10],[1]), ([10],10,[0,10],[1]),
    ([-10,0,10,20],10,[-10,0,10,20],[1,1,2]),
    ([0,0],1,[0,1],[2]), ([-1,0,1],1,[-1,0,1],[1,2])])
def test_histogram_boundary_oracles(values,step,edges,counts):
    h=histogram(values,stats(values),histogram_scale(values,step))
    assert h['edges']==edges and h['counts']==counts


def test_sparse_extreme_bins_remain_true_and_bounded():
    values=[-1e9,0,1e9]; h=histogram(values,stats(values),histogram_scale(values))
    assert h['bins']==200000000 and len(h['counts'])==3 and sum(h['counts'])==3
    assert h['indices']==[0,100000000,199999999]
    with pytest.raises((PatternLabDataError,OverflowError)):histogram_scale([-1e308,1e308])


def test_histogram_refuses_population_mismatch():
    with pytest.raises(PatternLabDataError):histogram([1],stats([1,2]),histogram_scale([1,2]))


def test_per_field_statuses_short_month_and_bool(reader):
    unit=reader.result(reader.configurations[0]['configurationId'],'earlier')
    unit['metrics'].update(returnPct=0.,annualizedProfitPct=None,annualizedProfitStatus='shortSpan',trades=0)
    unit['classification'].update(classifiable=False,profitable=None,interesting=None,lowActivity=True)
    p=setting_period(unit,reader.summary()['periods']['earlier']['monthly'])
    assert p['returnPct']==0 and p['trades']==0 and p['annualizedProfitPct'] is None
    assert p['classification']['lowActivity'] is True and not p['classification']['valid']
    unit['metrics']['performanceStatus']='noObservedBars'
    assert not eligible(unit,'returnPct') and eligible(unit,'trades')
    unit['metrics']['trades']=False
    assert not eligible(unit,'trades')


@pytest.mark.parametrize('bad', [dict(extra=True),dict(schema='bad'),dict(artifactIdentity='bad'),
    dict(title=''),dict(subtitle='x'*2049),dict(periodLabels={'other':'x'}),dict(parameterLabels=[{'path':'/bad','label':'x'}]),
    dict(parameterLabels=[{'path':'/policy','label':'x'}]),dict(parameterLabels=[{'path':'/allocationId','label':'x'}]*2)])
def test_presentation_closed_and_bound(reader,bad):
    options=dict(schema=PRESENTATION,artifactIdentity=reader.metadata['artifactIdentity']);options.update(bad)
    with pytest.raises((PatternLabDataError,ValueError,TypeError)):
        presentation_options(options,reader.metadata)


def test_hostile_labels_and_literal_template_tokens(reader):
    options=dict(schema=PRESENTATION,artifactIdentity=reader.metadata['artifactIdentity'],title='</script><img src=x> __JS__ & __DATA__',
        parameterLabels=[dict(path='/policy/rewardRisk',label='RR')])
    p=presentation_options(options,reader.metadata); view=build_view(reader,p);raw=view_bytes(view)
    assert b'<' not in raw and b'&' not in raw and json.loads(raw)['meta']['title']==options['title']
    assert '__DATA__' in view['meta']['title']


def test_unequal_sparse_grid_and_per_tf_minimum(tmp_path):
    r=compact_report_input(tmp_path/'sparse',sparse=True,missing=False)
    v=build_view(r,presentation_options(None,r.metadata))
    assert len(v['cellmeta'])==3 and len(v['meta']['timeframes'])==2
    assert v['meta']['activityLabel']=='Below trade minimum'
    assert v['meta']['tradeMinima']=={'1440':100,'720':1}
    for sid,s in v['scopes'].items():
        a,b=(s['periods'][p]['concentration']['own']['2']['equalCellPositivePct'] for p in ('earlier','recent'))
        assert s['changes']['positiveWithoutBest2']==(b-a if a is not None and b is not None else None)
        assert s['changes']['profitable']==r.summary(sid)['changes']['profitableBreadthPp']

def test_reader_calls_once_and_presentation_scalar_types(reader):
    class Counting:
        metadata=reader.metadata
        configurations=reader.configurations
        scopes=reader.scopes
        result_rows=reader.result_rows
        calls=[]
        summaries=[]
        def result(self,c,p):
            self.calls.append((c,p));return reader.result(c,p)
        def summary(self,s):
            self.summaries.append(s);return reader.summary(s)
    c=Counting()
    build_view(c,presentation_options(None,c.metadata))
    assert len(c.calls)==len(set(c.calls))==len(c.result_rows)
    assert len(c.summaries)==len(set(c.summaries))==len(c.scopes)
    from tools.pattern_lab.strategy_screen_report.model import scalars
    assert dict(scalars({'a/b':{'~x':[None,True,'1',1]}}))=={
        '/a~1b/~0x/0':None,'/a~1b/~0x/1':True,'/a~1b/~0x/2':'1','/a~1b/~0x/3':1}


def test_inconsistent_calendar_and_nonfinite_view_refused(reader):
    unit=reader.result(reader.configurations[0]['configurationId'],'earlier')
    unit['months'][0]['endMs']+=1
    with pytest.raises(PatternLabDataError):
        setting_period(unit,reader.summary()['periods']['earlier']['monthly'])
    with pytest.raises(ValueError):view_bytes({'value':float('nan')})


