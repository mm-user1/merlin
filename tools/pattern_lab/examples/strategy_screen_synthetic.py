"""Small deterministic multi-month inputs for the native Strategy Screen."""
from copy import deepcopy
from pathlib import Path
import argparse
import json
import numpy as np

from tools.pattern_lab import data, manifest
from tools.pattern_lab.auxiliary import publish_snapshot
from tools.pattern_lab.auxiliary.model import TYPES
from tools.pattern_lab.strategy_screen import run_strategy_screen, verify_strategy_screen


def request(*, auxiliary=False):
    ha=dict(requestId='ha',featureId='ha',version='1',inputId='target',parameters={},priorBars=2)
    base=dict(allocationId='rr2',timeframeMinutes=1440,features=[ha],
        provider=dict(providerId='haRun',version='1',recipe=dict(haRequestId='ha',entryRunLength=2)),
        entryFilter=None,policy=dict(mode='fixedBracket',direction='both',rewardRisk=2.,stopUpdates=False,maxHoldingDays=None))
    allocations=[]
    for rr in range(2,8):
        a=deepcopy(base);a['allocationId']='rr'+str(rr);a['policy']['rewardRisk']=float(rr)
        if auxiliary:a['auxiliaryFeatures']=[]
        allocations.append(a)
    protocol=dict(schema_version=1,protocol_id='screen-synthetic',
        development=dict(start_utc='2020-01-01T00:00:00Z',end_utc='2020-07-01T00:00:00Z'),
        reserved=dict(start_utc='2020-07-01T00:00:00Z',end_utc='2020-08-01T00:00:00Z'),
        earliest_warmup_start_utc='2019-12-01T00:00:00Z')
    doc=dict(schema='pattern-lab.strategy-screen-request.v1',studyId='screen-synthetic',allocations=allocations,sources=[],
        cells=[dict(instrumentId='BYBIT_AAAUSDT',timeframeMinutes=1440,allocationIds=[a['allocationId'] for a in allocations])],
        periods=[dict(periodId=role,startUtc=start,endUtc=end,preparationStartUtc='2019-12-01T00:00:00Z',protocol=deepcopy(protocol))
            for role,start,end in [('earlier','2020-01-01T00:00:00Z','2020-04-01T00:00:00Z'),
                                   ('recent','2020-04-01T00:00:00Z','2020-07-01T00:00:00Z')]],
        criteria=dict(annualizedProfitPct=0.,mtmMaxDdPct=150.,minClosedTrades=1),
        segments=[dict(segmentId='last-two',periodId='recent',startUtc='2020-05-01T00:00:00Z',endUtc='2020-07-01T00:00:00Z')],
        previousReviewEndUtc='2020-05-01T00:00:00Z')
    if auxiliary:doc['auxiliaryInputs']=[]
    return doc


def build_inputs(pack_root, auxiliary_root=None, *, document=None, targets=('AAA',)):
    doc=deepcopy(document) if document is not None else request(auxiliary=auxiliary_root is not None)
    start=min(manifest.to_epoch_ms(p['preparationStartUtc'],'start') for p in doc['periods'])
    end=max(manifest.to_epoch_ms(p['endUtc'],'end') for p in doc['periods'])
    ix=np.arange((end-start)//300000,dtype=np.int64);times=start+ix*300000;sources=[]
    for j,symbol in enumerate((*targets,'BTC')):
        period=288*(18+j*6)
        triangle=lambda x:np.minimum(x%period,period-x%period)/288
        op=128+j*8+triangle(ix)*4;cl=128+j*8+triangle(ix+1)*4
        values=np.column_stack((op,np.maximum(op,cl)+.5,np.minimum(op,cl)-.5,cl,1000+ix))
        rules=dict(schema_version=1,source_reference='synthetic strategy screen',as_of_utc=manifest.format_epoch_ms(end),
            contract_type='linear_perpetual',base_currency=symbol,quote_currency='USDT',settlement_currency='USDT',
            quantity_unit=symbol,quantity_step='0.125',minimum_quantity='0.125',price_tick='0.125',minimum_notional=None,
            listed_at_utc=manifest.format_epoch_ms(start),trading_status='Trading',raw_contract_fields=dict(
                contractType='LinearPerpetual',baseCoin=symbol,quoteCoin='USDT',settleCoin='USDT',launchTime=str(start),
                status='Trading',qtyStep='0.125',minOrderQty='0.125',minNotionalValue=None,tickSize='0.125'))
        sources.append(data.InstrumentSource(symbol,'BYBIT',symbol+'USDT','USDT',['factor'] if symbol=='BTC' else ['trading'],times,values,
            manifest.build_source_metadata(input_format='synthetic',input_dtype='float64',source_reference='strategy_screen_synthetic.py',volume_unit_evidence='synthetic quote turnover'),
            manifest.build_verification(volume_quote_verified=True,volume_quote_evidence='synthetic quote turnover',
                closed_before_utc=manifest.format_epoch_ms(end),closure_evidence='synthetic closed grid',closure_source='strategy_screen_synthetic.py'),instrument_rules=rules))
    data.publish_pack(Path(pack_root),sources,universe=manifest.build_universe(notes='synthetic only'),generated_utc=manifest.format_epoch_ms(end))
    if auxiliary_root is not None:
        series=dict(seriesId='funding',provider='bybit',providerVersion='v5',venue='BYBIT',contract='AAAUSDT',
            market='linearUSDTPerpetual',baseUnit='AAA',baseMultiplier=1,interval=None,type=TYPES['settledFunding'].document(),units={'fundingRate':'ratePerSettlement'})
        snapshot=publish_snapshot([dict(series=series,rows=[dict(observedAtMs=int(t),fundingRate=.5 if i//24%2 else -.5) for i,t in enumerate(times[::96])],coverage=[[start,end]])],
            output_root=auxiliary_root,provenance=dict(origin='synthetic'))
        source={k:series[k] for k in ('seriesId','provider','venue','contract','interval','baseUnit','baseMultiplier')}
        source.update(typeId=series['type']['typeId'],field='fundingRate',unit='ratePerSettlement')
        doc['auxiliaryInputs']=[dict(inputId='funding',snapshotId='aux',expectedSnapshotContentId=snapshot.contentId,coveragePolicy='allowUnknown',sources={'*':source})]
        a=doc['allocations'][-1]
        a['auxiliaryFeatures']=[dict(featureId='fundingLevel',bindingId='funding',transform='auxiliaryLevel',version='1',field='fundingRate',publicationLagMs=300000,maxAgeMs=86400000,lookbackMinutes=None)]
        a['features']=[dict(requestId='atr',featureId='atr',version='1',inputId='target',parameters=dict(length=3),priorBars=0)]
        a['provider']=dict(providerId='scalarCondition',version='1',featureId='fundingLevel',long=dict(operator='gt',threshold=0),short=dict(operator='lt',threshold=0),
            stop=dict(providerId='realSwingAtr',atrRequestId='atr',swingBars=2,atrMultiplier=1.))
        a=doc['allocations'][-2]
        a['features'].append(dict(requestId='btc',featureId='candleDirection',version='1',inputId='btc',parameters=dict(candleType='real',confirmationBars=1),priorBars=0))
        a['entryFilter']=dict(componentId='btc',providerId='candleDirection',version='1',requestId='btc')
        doc['sources']=[dict(inputId='btc',instrumentId='BYBIT_BTCUSDT',timeframeMinutes=1440)]
    return doc


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output-root',type=Path,required=True)
    parser.add_argument('--workers',type=int,choices=range(1,7),default=1);parser.add_argument('--auxiliary',action='store_true');args=parser.parse_args()
    args.output_root.mkdir(parents=True,exist_ok=False)
    aux=args.output_root/'auxiliary' if args.auxiliary else None
    doc=build_inputs(args.output_root/'pack',aux)
    reader=run_strategy_screen(doc,data_root=args.output_root/'pack',output_root=args.output_root/'screen',workers=args.workers,
        auxiliary_roots={'aux':aux} if aux else None)
    print(json.dumps(verify_strategy_screen(reader.root),indent=2))


if __name__=='__main__':main()
