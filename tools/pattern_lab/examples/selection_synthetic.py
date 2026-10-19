"""Seven-month, six-target native selection example; synthetic inputs only."""
from pathlib import Path
import argparse
import json
import sys
import numpy as np

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[3]))

from tools.pattern_lab import data, manifest
from tools.pattern_lab.account_study import run_account_study, export_account_bundle
from tools.pattern_lab.account_study.common import fresh_directory, file_hash, write_json
from tools.pattern_lab.selection import run_selection, verify_selection, export_selection_bundle
from tools.pattern_lab.selection.request import month_bounds

MONTHS=[f'2020-{m:02d}' for m in range(1,8)]


def build_inputs(pack_root, *, gaps=False):
    origin=manifest.to_epoch_ms('2019-12-30T00:00:00Z','origin');end=month_bounds(MONTHS[-1])[1]
    ix=np.arange((end-origin)//300000,dtype=np.int64);stamps=origin+ix*300000
    month_index=np.searchsorted([month_bounds(m)[0] for m in MONTHS],stamps,side='right')-1
    regimes=np.array([[0,0,0,0,1,0,0],[0,0,0,0,1,0,1],[1,0,1,0,0,1,0],
        [0,1,0,1,0,0,1],[1,1,1,0,0,0,0],[2,2,2,2,2,2,2]])
    sources=[];targets=[]
    for j,symbol in enumerate(('AAA','BBB','CCC','DDD','EEE','FFF')):
        mode=regimes[j,np.maximum(0,month_index)];period=np.where(mode==0,768,192)
        phase=ix%period
        op=128+np.minimum(phase,period-phase)*.0625
        cl=op+np.where(phase<period//2,.0625,-.0625)
        op=np.where(mode==2,128.,op);cl=np.where(mode==2,128.,cl)
        values=np.column_stack((op,np.maximum(op,cl)+.25,np.minimum(op,cl)-.25,cl,np.full(len(ix),1000.)))
        keep=np.ones(len(ix),bool)
        if gaps:
            # Equal one-native-bar head/tail/interior holes in May; separate no-data June.
            may_start,may_end=month_bounds('2020-05')
            missing=[may_start,may_end-14400000,may_start+10*86400000]
            if j<3:keep &= ~((stamps>=missing[j])&(stamps<missing[j]+14400000))
            if j==4:
                a,b=month_bounds('2020-06');keep &= ~((stamps>=a)&(stamps<b))
        contract=symbol+'USDT';targets.append('BYBIT_'+contract)
        utc=manifest.format_epoch_ms
        rules=dict(schema_version=1,source_reference='synthetic monthly selection',as_of_utc=utc(end),
            contract_type='linear_perpetual',base_currency=symbol,quote_currency='USDT',settlement_currency='USDT',
            quantity_unit=symbol,quantity_step='0.125',minimum_quantity='0.125',price_tick='0.125',minimum_notional=None,
            listed_at_utc=utc(origin),trading_status='Trading',raw_contract_fields=dict(contractType='LinearPerpetual',
                baseCoin=symbol,quoteCoin='USDT',settleCoin='USDT',launchTime=str(origin),status='Trading',
                qtyStep='0.125',minOrderQty='0.125',minNotionalValue=None,tickSize='0.125'))
        sources.append(data.InstrumentSource(symbol,'BYBIT',contract,'USDT',['trading'],stamps[keep],values[keep],
            manifest.build_source_metadata(input_format='synthetic',input_dtype='float64',
                source_reference='selection_synthetic.py',volume_unit_evidence='synthetic quote turnover'),
            manifest.build_verification(volume_quote_verified=True,volume_quote_evidence='synthetic quote turnover',
                closed_before_utc=utc(end),closure_evidence='synthetic complete grid',closure_source='selection_synthetic.py'),instrument_rules=rules))
    data.publish_pack(Path(pack_root),sources,universe=manifest.build_universe(notes='synthetic selection only'),generated_utc=utc(end))
    allocation=dict(allocationId='monthly',timeframeMinutes=240,
        features=[dict(requestId='ha',featureId='ha',version='1',inputId='target',parameters={},priorBars=2)],
        provider=dict(providerId='haRun',version='1',recipe=dict(haRequestId='ha',entryRunLength=2)),entryFilter=None,
        policy=dict(mode='fixedBracket',direction='both',rewardRisk=2.,stopUpdates=False,maxHoldingDays=None,riskPct=.25,initialCapital=10000.))
    return dict(schema='pattern-lab.account-study-request.v1',studyId='monthly-selection-parent',
        protocol=dict(schema_version=1,protocol_id='monthly-synthetic',development=dict(start_utc=utc(month_bounds(MONTHS[0])[0]),end_utc=utc(end)),
            reserved=dict(start_utc=utc(end),end_utc='2020-09-01T00:00:00Z'),earliest_warmup_start_utc=utc(origin)),
        preparationStartUtc=utc(origin),instruments=targets,sources=[],
        windows=[dict(windowId='m'+m.replace('-',''),startUtc=utc(month_bounds(m)[0]),endUtc=utc(month_bounds(m)[1])) for m in MONTHS],allocations=[allocation])


def selection_request(parent):
    return dict(schema='pattern-lab.selection-request.v1',selectionId='monthly-synthetic',
        parent={**parent.study['identities'],'completionSha256':file_hash(parent.root/'completion.json')},allocationId='monthly',
        monthWindows=[dict(windowId='m'+m.replace('-',''),month=m) for m in MONTHS],evaluationMonths=MONTHS[4:],
        pool=dict(historyMonths=4,minObservedBarsPct=99.),
        selectors=[dict(selectorId=name,scorerId=scorer,version='1',parameters=params) for name,scorer,params in (
            ('consistency','historicalConsistency4m',{}),('mean','meanMonthlyReturn',dict(months=2)),
            ('positive','positiveMonthCount',dict(months=4)))],kValues=[2,8],capitalMode='fixedReference',referenceCapital=10000.,
        randomControl=dict(seed=2026,replicates=12))


def run(output_root, *, workers=1):
    root=fresh_directory(output_root)
    request=build_inputs(root/'pack');write_json(root/'parent-request.json',request)
    parent=run_account_study(request,data_root=root/'pack',output_root=root/'parent',workers=workers)
    request=selection_request(parent);write_json(root/'selection-request.json',request)
    selected=run_selection(request,parent_root=parent.root,output_root=root/'selection')
    verified=verify_selection(selected.root,parent_root=parent.root)
    return dict(parentBundle=export_account_bundle(parent.root,zip_path=root/'monthly-parent.zip'),
        selectionBundle=export_selection_bundle(selected.root,zip_path=root/'selection.zip'),verification=verified)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root',required=True,type=Path)
    parser.add_argument('--workers',type=int,choices=range(1,7),default=1)
    args=parser.parse_args();print(json.dumps(run(args.output_root,workers=args.workers),indent=2))
