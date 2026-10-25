"""Exactly representable synthetic inputs for robustness and legacy preservation."""
from tools.pattern_lab.accounts import execute_account
from tools.pattern_lab.account_study.codec import AccountRecord
from ._account_helpers import prepared, bundle, side, policy, window, rules


def assert_numbers(expected, actual, *, subset=False):
    import math
    if isinstance(expected,dict):
        if not subset:assert set(expected)==set(actual)
        for key,value in expected.items():assert_numbers(value,actual[key],subset=subset)
    elif isinstance(expected,list):
        assert len(expected)==len(actual)
        for x,y in zip(expected,actual):assert_numbers(x,y,subset=subset)
    elif type(expected) is float:
        assert math.isclose(expected,actual,rel_tol=1e-9,abs_tol=1e-8),(expected,actual)
    else:assert type(expected) is type(actual) and expected==actual,(expected,actual)


def legacy_records():
    records = []
    for name, last in [('A', 104), ('B', 96)]:
        p = prepared([(100,102,98,100),(100,106,99,104),(104,105,95,96),
                      (96,105,94,last)], slots=[0,1,2,4], end=5*1800000)
        c = bundle(p, long=side(4,entries=[0],stop=90), short=side(4,entries=[2],stop=110))
        result = execute_account(p,c,policy(),window(p),rules())
        records.append(AccountRecord(p,result,dict(instrumentId=name,allocationId='base',windowId='w')))
    return records


def build_multi_month(root):
    """Native pack: trending AAA, choppy BBB, flat CCC; all prices are dyadic."""
    from copy import deepcopy
    from dataclasses import replace
    import numpy as np
    from data_provider.manifest import to_epoch_ms
    from ._helpers import instrument_source, publish
    from ._bracket_helpers import rule_entry
    from ._account_study_helpers import request
    at=lambda s:to_epoch_ms(s,'fixture')
    start=at('2025-10-09T00:00:00Z'); end=at('2026-01-06T00:00:00Z')
    sources=[]
    for j,symbol in enumerate(('AAA','BBB','CCC')):
        ix=np.arange((end-start)//300000,dtype=np.int64); stamps=start+ix*300000
        missing=at('2025-10-31T23:30:00Z') if symbol=='AAA' else at('2025-12-01T00:00:00Z')
        keep=~((stamps>=missing)&(stamps<missing+1800000)) if symbol!='CCC' else np.ones(len(ix),bool)
        ix,stamps=ix[keep],stamps[keep]
        period=96 if symbol=='AAA' else 24
        phase=ix % period
        op=100+j*16+np.minimum(phase,period-phase)*(0.5 if symbol=='AAA' else 0.125)
        cl=op+np.where(phase<period//2,0.0625,-0.0625)
        if symbol=='CCC':op=np.full(len(ix),128.);cl=op.copy()
        values=np.column_stack((op,np.maximum(op,cl)+.25,np.minimum(op,cl)-.25,cl,np.full(len(ix),1000.)))
        rules_=deepcopy(rule_entry('BYBIT')['instrument_rules'])
        rules_.update(base_currency=symbol,quantity_unit=symbol);rules_['raw_contract_fields']['baseCoin']=symbol
        source=instrument_source(stamps,values,symbol=symbol,venue='BYBIT',contract=symbol+'USDT',roles=['trading'])
        sources.append(replace(source,instrument_rules=rules_))
    publish(root,sources)
    raw=request(['BYBIT_'+s+'USDT' for s in ('AAA','BBB','CCC')],filtered=False)
    raw['allocations']=raw['allocations'][:2]
    for allocation in raw['allocations']:allocation['policy']['riskPct']=.25
    raw['preparationStartUtc']='2025-10-09T00:00:00Z'
    raw['protocol'].update(development=dict(start_utc='2025-10-09T08:00:00Z',end_utc='2026-01-05T00:00:00Z'),
        reserved=dict(start_utc='2026-01-05T00:00:00Z',end_utc='2026-02-01T00:00:00Z'),
        earliest_warmup_start_utc='2025-10-09T00:00:00Z')
    raw['windows']=[dict(windowId='early',startUtc='2025-10-09T08:00:00Z',endUtc='2025-12-15T12:00:00Z'),
        dict(windowId='late',startUtc='2025-11-15T12:00:00Z',endUtc='2026-01-05T00:00:00Z'),
        dict(windowId='dec',startUtc='2025-12-01T00:00:00Z',endUtc='2026-01-01T00:00:00Z')]
    return raw


def assert_multi_month(summary):
    from tools.pattern_lab.account_study.robustness import close
    aggregates=summary['aggregates']; accounts=summary['accounts']
    assert len({m['month'] for a in aggregates for m in a['months']})>=3
    overlaps=summary['robustness']['overlaps']
    assert any(o['sharedPctOfA']==100 or o['sharedPctOfB']==100 for o in overlaps)
    assert any(0<o['sharedPctOfA']<100 and 0<o['sharedPctOfB']<100 for o in overlaps)
    assert all(all(s['trades']>0 for s in a['robustness']['sides']) for a in aggregates)
    assert any(a['robustness']['partialEdgeMonths']>0 for a in aggregates)
    assert any(a['robustness']['nonExactBoundaryMonths']>0 for a in aggregates)
    unavailable=[(a['allocationId'],a['windowId'],m['month']) for a in aggregates
        for m in a['robustness']['passiveMonths'] if m['status']=='unavailable']
    assert unavailable
    erasures=[(a['allocationId'],a['windowId']) for a in aggregates
        if a['netPnl']>0 and a['robustness']['removals']['tickerProfitErasure']['status']=='available']
    assert erasures
    pairs=[]
    for b in accounts:
        if b['allocationId']!='rr2':continue
        c=next(a for a in accounts if (a['instrumentId'],a['windowId'],a['allocationId'])==(b['instrumentId'],b['windowId'],'rr3'))
        pairs.append(dict(instrumentId=b['instrumentId'],windowId=b['windowId'],tied=close(c['returnPct'],b['returnPct'])))
    assert any(p['tied'] for p in pairs) and any(not p['tied'] for p in pairs)
    return dict(accounts=len(accounts),trades=sum(a['trades'] for a in accounts),
        sidesByGroup={a['allocationId']+'/'+a['windowId']:{s['side']:s['trades'] for s in a['robustness']['sides']} for a in aggregates},
        unavailablePassiveCells=unavailable,profitErasureGroups=erasures,pairedTickers=pairs)
