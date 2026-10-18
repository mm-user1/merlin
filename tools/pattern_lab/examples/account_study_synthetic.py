"""Bounded synthetic account study. Requires a new, explicitly chosen output root."""
from pathlib import Path
from copy import deepcopy
import argparse
import json
import numpy as np

from tools.pattern_lab import data, manifest
from tools.pattern_lab.account_study import run_account_study, verify_account_study, export_account_bundle
from tools.pattern_lab.account_study.common import fresh_directory, write_json


def build_inputs(packRoot):
    origin, step, slots = 1577836800000, 1800000, 96
    utc = lambda n: manifest.format_epoch_ms(origin+n*step)
    sources, instruments = [], []
    for j, symbol in enumerate(('AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'BTC')):
        ix = np.arange(slots*6, dtype=np.int64)
        if symbol != 'BTC': ix = ix[ix//6 != 40]  # explicit internal missing target group
        stamps = origin+ix*300000
        op = 100+j*5+3*np.sin(ix/18+j/3)
        cl = op+.4*np.cos(ix/18+j/3)
        values = np.column_stack((op,np.maximum(op,cl)+.6,np.minimum(op,cl)-.6,cl,1000+ix/7))
        contract = symbol+'USDT'
        if symbol != 'BTC': instruments.append('BYBIT_'+contract)
        rules = dict(schema_version=1, source_reference='synthetic account example', as_of_utc=utc(slots),
            contract_type='linear_perpetual', base_currency=symbol, quote_currency='USDT', settlement_currency='USDT',
            quantity_unit=symbol, quantity_step='0.1', minimum_quantity='0.1', price_tick='0.001', minimum_notional='5',
            listed_at_utc=utc(0), trading_status='Trading', raw_contract_fields=dict(contractType='LinearPerpetual',
                baseCoin=symbol,quoteCoin='USDT',settleCoin='USDT',launchTime=str(origin),status='Trading',
                qtyStep='0.1',minOrderQty='0.1',minNotionalValue='5',tickSize='0.001'))
        sources.append(data.InstrumentSource(symbol,'BYBIT',contract,'USDT',['factor'] if symbol=='BTC' else ['trading'],stamps,values,
            manifest.build_source_metadata(input_format='synthetic',input_dtype='float64',
                source_reference='account_study_synthetic.py',volume_unit_evidence='synthetic quote turnover'),
            manifest.build_verification(volume_quote_verified=True,volume_quote_evidence='synthetic quote turnover',
                closed_before_utc=utc(slots),closure_evidence='synthetic closed grid',closure_source='account_study_synthetic.py'),
            instrument_rules=rules))
    data.publish_pack(Path(packRoot),sources,universe=manifest.build_universe(notes='synthetic only'),generated_utc=utc(slots))
    ha = dict(requestId='ha',featureId='ha',version='1',inputId='target',parameters={},priorBars=2)
    state = dict(requestId='state',featureId='candleDirection',version='1',inputId='btc4h',
                 parameters=dict(candleType='real',confirmationBars=1),priorBars=0)
    base = dict(allocationId='rr2',timeframeMinutes=30,features=[ha,state],
        provider=dict(providerId='haRun',version='1',recipe=dict(haRequestId='ha',entryRunLength=2)),
        entryFilter=dict(providerId='candleDirection',version='1',requestId='state'),
        policy=dict(mode='fixedBracket',direction='both',rewardRisk=2.,stopUpdates=False,maxHoldingDays=None))
    rr = deepcopy(base); rr['allocationId']='rr3';rr['policy']['rewardRisk']=3.
    trailing = deepcopy(base);trailing['allocationId']='trailing'
    trailing['policy'].update(mode='signalManaged',rewardRisk=None,stopUpdates=True)
    trailing['features'].append(dict(requestId='atr',featureId='atr',version='1',inputId='target',
        parameters=dict(length=3),priorBars=0))
    trailing['provider']['recipe'].update(stopProvider='realSwingAtr',atrRequestId='atr',swingBars=2,atrMultiplier=1.)
    return dict(schema='pattern-lab.account-study-request.v1',studyId='six-synthetic-accounts',
        protocol=dict(schema_version=1,protocol_id='synthetic',development=dict(start_utc=utc(16),end_utc=utc(slots)),
            reserved=dict(start_utc=utc(slots),end_utc=utc(slots+48)),earliest_warmup_start_utc=utc(0)),
        preparationStartUtc=utc(0),instruments=instruments,
        sources=[dict(inputId='btc4h',instrumentId='BYBIT_BTCUSDT',timeframeMinutes=240)],
        windows=[dict(windowId='full',startUtc=utc(16),endUtc=utc(slots)),
                 dict(windowId='overlap',startUtc=utc(32),endUtc=utc(slots))],allocations=[base,rr,trailing])


def run(outputRoot, *, workers=1):
    root = fresh_directory(outputRoot)
    request = build_inputs(root/'pack')
    write_json(root/'request.json',request)
    reader = run_account_study(request,data_root=root/'pack',output_root=root/'study',workers=workers)
    verification = verify_account_study(reader.root)
    bundle = export_account_bundle(reader.root,zip_path=root/'account-study.zip')
    return dict(runRoot=str(reader.root),verification=verification,bundle=bundle)


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root',type=Path,required=True)
    parser.add_argument('--workers',type=int,choices=range(1,7),default=1)
    args=parser.parse_args()
    print(json.dumps(run(args.output_root,workers=args.workers),indent=2))
