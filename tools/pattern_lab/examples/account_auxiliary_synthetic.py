"""Offline integer/dyadic auxiliary study; no market inputs or network calls."""
from pathlib import Path
from copy import deepcopy
import argparse
import json
import sys
import numpy as np

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[3]))

from tools.pattern_lab import data, manifest
from tools.pattern_lab.auxiliary import publish_snapshot
from tools.pattern_lab.auxiliary.model import TYPES
from tools.pattern_lab.account_study import run_account_study, verify_account_study, export_account_bundle
from tools.pattern_lab.account_study.common import fresh_directory, write_json

ORIGIN=1577836800000
STEP=300000
SLOTS=2304


def build_inputs(pack_root, auxiliary_root, *, targets=2):
    utc=lambda i:manifest.format_epoch_ms(ORIGIN+i*STEP)
    sources=[];instruments=[]
    for j,symbol in enumerate(['AAA','BBB','CCC','DDD','EEE','FFF'][:targets]+['BTC']):
        ix=np.arange(SLOTS,dtype=np.int64);period=192 if symbol=='BTC' else 96
        triangle=lambda x:np.minimum(x%period,period-x%period)
        op=128+j*8+triangle(ix)*.5;cl=128+j*8+triangle(ix+1)*.5
        values=np.column_stack((op,np.maximum(op,cl)+1,np.minimum(op,cl)-1,cl,1000+ix))
        contract=symbol+'USDT'
        if symbol!='BTC':instruments.append('BYBIT_'+contract)
        rules=dict(schema_version=1,source_reference='synthetic auxiliary example',as_of_utc=utc(SLOTS),
            contract_type='linear_perpetual',base_currency=symbol,quote_currency='USDT',settlement_currency='USDT',
            quantity_unit=symbol,quantity_step='0.125',minimum_quantity='0.125',price_tick='0.125',minimum_notional=None,
            listed_at_utc=utc(0),trading_status='Trading',raw_contract_fields=dict(contractType='LinearPerpetual',
                baseCoin=symbol,quoteCoin='USDT',settleCoin='USDT',launchTime=str(ORIGIN),status='Trading',
                qtyStep='0.125',minOrderQty='0.125',minNotionalValue=None,tickSize='0.125'))
        sources.append(data.InstrumentSource(symbol,'BYBIT',contract,'USDT',['factor'] if symbol=='BTC' else ['trading'],
            ORIGIN+ix*STEP,values,manifest.build_source_metadata(input_format='synthetic',input_dtype='float64',
                source_reference='account_auxiliary_synthetic.py',volume_unit_evidence='synthetic quote turnover'),
            manifest.build_verification(volume_quote_verified=True,volume_quote_evidence='synthetic quote turnover',
                closed_before_utc=utc(SLOTS),closure_evidence='synthetic closed grid',closure_source='account_auxiliary_synthetic.py'),
            instrument_rules=rules))
    data.publish_pack(Path(pack_root),sources,universe=manifest.build_universe(notes='synthetic only'),generated_utc=utc(SLOTS))
    values=[]
    for kind,sid in (('settledFunding','funding'),('openInterest','oi')):
        t=TYPES[kind]
        series=dict(seriesId=sid,provider='bybit',providerVersion='v5',venue='BYBIT',contract='1000PEPEUSDT',
            market='linearUSDTPerpetual',baseUnit='1000PEPE',baseMultiplier=1000,interval='5min' if sid=='oi' else None,
            type=t.document(),units={f:'ratePerSettlement' if sid=='funding' else '1000PEPE' for f in t.fields})
        if sid=='funding':
            times=[];i=0;k=0
            while i<SLOTS:
                times.append((i,[-.5,0.,.5,0.][k%4]));i+=48 if k%3 else 96;k+=1
            rows=[dict(observedAtMs=ORIGIN+i*STEP,fundingRate=v) for i,v in times]
        else:
            rows=[dict(observedAtMs=ORIGIN+i*STEP,singleOpenInterest=float(128+i%96),openInterest=float(256+2*(i%96)))
                  for i in range(SLOTS) if i not in (245,246,965)]
        values.append(dict(series=series,rows=rows,coverage=[[ORIGIN,ORIGIN+SLOTS*STEP]]))
    snapshot=publish_snapshot(values,output_root=auxiliary_root,provenance=dict(origin='synthetic deterministic observations'))
    bindings=[]
    for value in values:
        s=value['series'];field='fundingRate' if s['seriesId']=='funding' else 'singleOpenInterest'
        source={k:s[k] for k in ('seriesId','provider','venue','contract','interval','baseUnit','baseMultiplier')}
        source.update(typeId=s['type']['typeId'],field=field,unit=s['units'][field])
        bindings.append(dict(inputId=s['seriesId'],snapshotId='aux',expectedSnapshotContentId=snapshot.contentId,
            coveragePolicy='allowUnknown',sources={'*':source}))
    ha=dict(requestId='ha',featureId='ha',version='1',inputId='target',parameters={},priorBars=2)
    atr=dict(requestId='atr',featureId='atr',version='1',inputId='target',parameters=dict(length=4),priorBars=0)
    candle=dict(requestId='btc',featureId='candleDirection',version='1',inputId='btc4h',
                parameters=dict(candleType='real',confirmationBars=1),priorBars=0)
    funding=dict(featureId='fundingLevel',bindingId='funding',transform='auxiliaryLevel',version='1',field='fundingRate',
                 publicationLagMs=STEP,maxAgeMs=6*3600000,lookbackMinutes=None)
    oi=dict(featureId='oiChange',bindingId='oi',transform='auxiliaryChange',version='1',field='singleOpenInterest',
            publicationLagMs=STEP,maxAgeMs=3*STEP,lookbackMinutes=15)
    funding_filter=dict(componentId='funding',providerId='scalarCondition',version='1',featureId='fundingLevel',
                        long=dict(operator='gt',threshold=-.25),short=dict(operator='lt',threshold=.25))
    oi_filter=dict(componentId='oi',providerId='scalarCondition',version='1',featureId='oiChange',
                   long=dict(operator='gt',threshold=0),short=dict(operator='lt',threshold=0))
    base=dict(allocationId='baseline',timeframeMinutes=30,features=[ha],auxiliaryFeatures=[],
        provider=dict(providerId='haRun',version='1',recipe=dict(haRequestId='ha',entryRunLength=2)),entryFilter=None,
        policy=dict(mode='fixedBracket',direction='both',rewardRisk=2.,stopUpdates=False,maxHoldingDays=None))
    allocations=[base]
    for aid,features,filt in [('funding',[funding],funding_filter),('oi',[oi],oi_filter),('combined',[funding],
            dict(allOf=[funding_filter,dict(componentId='btc',providerId='candleDirection',version='1',requestId='btc')]))]:
        a=deepcopy(base);a.update(allocationId=aid,auxiliaryFeatures=deepcopy(features),entryFilter=deepcopy(filt))
        if aid=='combined':a['features'].append(candle)
        allocations.append(a)
    scalar=deepcopy(base);scalar.update(allocationId='scalar',features=[atr],auxiliaryFeatures=[deepcopy(funding)],
        provider=dict(providerId='scalarCondition',version='1',featureId='fundingLevel',long=dict(operator='gt',threshold=0),
            short=dict(operator='lt',threshold=0),stop=dict(providerId='realSwingAtr',atrRequestId='atr',swingBars=2,atrMultiplier=1.)))
    allocations.append(scalar)
    return dict(schema='pattern-lab.account-study-request.v2',studyId='auxiliary-synthetic',
        protocol=dict(schema_version=1,protocol_id='synthetic',development=dict(start_utc=utc(192),end_utc=utc(SLOTS)),
            reserved=dict(start_utc=utc(SLOTS),end_utc=utc(SLOTS+576)),earliest_warmup_start_utc=utc(0)),
        preparationStartUtc=utc(0),instruments=instruments,
        sources=[dict(inputId='btc4h',instrumentId='BYBIT_BTCUSDT',timeframeMinutes=240)],auxiliaryInputs=bindings,
        windows=[dict(windowId='full',startUtc=utc(192),endUtc=utc(SLOTS)),
                 dict(windowId='overlap',startUtc=utc(576),endUtc=utc(SLOTS-192))],allocations=allocations)


def run(output_root, *, workers=1):
    root=fresh_directory(output_root)
    request=build_inputs(root/'pack',root/'auxiliary')
    write_json(root/'request.json',request)
    reader=run_account_study(request,data_root=root/'pack',auxiliary_roots={'aux':root/'auxiliary'},output_root=root/'study',workers=workers)
    return dict(runRoot=str(reader.root),verification=verify_account_study(reader.root),
                bundle=export_account_bundle(reader.root,zip_path=root/'auxiliary-study.zip'))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root',type=Path,required=True)
    parser.add_argument('--workers',type=int,choices=range(1,7),default=1)
    args=parser.parse_args();print(json.dumps(run(args.output_root,workers=args.workers),indent=2))
