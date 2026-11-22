from copy import deepcopy
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.strategy_screen import normalize_request
from tools.pattern_lab.strategy_screen.request import configurations,child_request
from ._strategy_screen_helpers import request


@pytest.mark.parametrize('overrides',[{1440:1},{True:1},{1440.0:1},{'01440':1},{'60':1},{1440:1,'1440':2}])
def test_original_override_key_types_and_no_mutation(overrides):
    doc=request();doc['criteria']['minClosedTradesByTimeframe']=overrides;before=deepcopy(doc)
    with pytest.raises(PatternLabDataError):normalize_request(doc)
    assert doc==before and list(doc['criteria']['minClosedTradesByTimeframe'])==list(overrides)


def test_native_defaults_and_v2_presence():
    for auxiliary in (False,True):
        doc=normalize_request(request(auxiliary=auxiliary)).document
        child=child_request(doc,doc['cells'][0],doc['periods'][0])
        assert child['schema'].endswith('v2' if auxiliary else 'v1')
        assert ('auxiliaryInputs' in child)==auxiliary
        assert normalize_request(doc).document==doc
        assert len(configurations(doc))==6


@pytest.mark.parametrize('mutation',[
    lambda d:d['periods'].reverse(),
    lambda d:d['periods'][0].update(startUtc='2020-01-02T00:00:00Z'),
    lambda d:d['criteria'].update(minClosedTrades=True),
    lambda d:d['criteria'].update(mtmMaxDdPct=-1),
    lambda d:d['criteria'].update(minClosedTradesByTimeframe={'01440':1}),
    lambda d:d['cells'].append(deepcopy(d['cells'][0])),
    lambda d:d['cells'][0]['allocationIds'].pop(),
    lambda d:d.update(unknown=1),
])
def test_closed_contract_refusals(mutation):
    doc=request();mutation(doc)
    with pytest.raises(PatternLabDataError):normalize_request(doc)


def test_sparse_cells_do_not_expand_cartesian():
    doc=request();doc['cells'][0]['allocationIds']= ['rr2']
    doc['cells'].append(dict(instrumentId='BYBIT_BBBUSDT',timeframeMinutes=1440,allocationIds=['rr3','rr4','rr5','rr6','rr7']))
    normalized=normalize_request(doc).document
    assert len(configurations(normalized))==6
    assert len(child_request(normalized,normalized['cells'][0],normalized['periods'][0])['allocations'])==1


def test_renamed_allocations_preserve_logical_ids_and_hostile_identity_rejected():
    doc=request();before=configurations(normalize_request(doc).document)
    doc['allocations'][0]['allocationId']='renamed';doc['cells'][0]['allocationIds'][0]='renamed'
    assert [r['configurationId'] for r in before]==[r['configurationId'] for r in configurations(normalize_request(doc).document)]
    doc['cells'][0]['instrumentId']='<script>bad</script>'
    with pytest.raises(PatternLabDataError):normalize_request(doc)


@pytest.mark.parametrize('wildcard',[False,True])
def test_native_sparse_alias_projection_and_job_roster(tmp_path,monkeypatch,wildcard):
    from tools.pattern_lab.strategy_screen import runner
    from ._strategy_screen_helpers import build_inputs
    doc=build_inputs(tmp_path/'pack',tmp_path/'aux',targets=('AAA','BBB'))
    doc['cells']=[dict(instrumentId='BYBIT_AAAUSDT',timeframeMinutes=1440,allocationIds=['rr2','rr6','rr7']),
                  dict(instrumentId='BYBIT_BBBUSDT',timeframeMinutes=1440,allocationIds=['rr3','rr4','rr5'])]
    if not wildcard:
        source=doc['auxiliaryInputs'][0]['sources']['*']
        doc['auxiliaryInputs'][0]['sources']={t:deepcopy(source) for t in ('BYBIT_AAAUSDT','BYBIT_BBBUSDT')}
    normalized=normalize_request(doc).document;child=child_request(normalized,normalized['cells'][1],normalized['periods'][0])
    assert child['schema'].endswith('v2') and child['auxiliaryInputs']==child['sources']==[]
    observed=[];execute=runner.run_account_job
    def track(job):
        records=execute(job);observed.extend((r.labels['instrumentId'],r.labels['allocationId'],r.labels['windowId']) for r in records)
        return records
    monkeypatch.setattr(runner,'run_account_job',track)
    reader=runner.run_strategy_screen(doc,data_root=tmp_path/'pack',output_root=tmp_path/'screen',auxiliary_roots={'aux':tmp_path/'aux'})
    expected={(c['instrumentId'],aid,p['periodId']) for c in doc['cells'] for aid in c['allocationIds'] for p in doc['periods']}
    assert set(observed)==expected and len(observed)==12
    assert [v[2] for v in observed]==['recent']*6+['earlier']*6
    assert len(reader.configurations)==6
