"""Opt-in sealed external S07 gates; synthetic CI never reads owner market data.

Set MERLIN_S07_PACK and MERLIN_S07_REFERENCE to the verified read-only inputs.
All generated comparisons go to pytest's retained external scratch.
"""
from dataclasses import asdict
import csv
import hashlib
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data_input import pack_session, prepare_input
from core.engine_v2.admission import AdmissionRules, run_strategy
from core.engine_v2.kernel import KernelTrace, run_reference_kernel, quantity_lots
from core.engine_v2.runner import build_kernel_config
from core.entry_filters import OFF, PreparedPermissions, apply_permissions, canonical, candle_direction, effective_config
from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan, execute_grid_v2_candidates
from strategies import get_strategy
from strategies.s07_ha_v01_b2 import strategy
from strategies.s07_ha_v01_b2.signals import indicators

BASE = Path(__file__).resolve().parents[1] / 'data/baseline_v2/s07_ha_v01'
EXPECTED = json.loads((BASE/'expected.json').read_text(encoding='utf-8'))
pytestmark = pytest.mark.skipif(not os.environ.get('MERLIN_S07_PACK'), reason='Explicit external S07 pack opt-in required')


def rows(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def verify(path, expected):
    with path.open('rb') as stream:
        actual = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert actual == expected, str(path)
    return actual


@pytest.fixture(scope='module')
def inputs():
    pack = Path(os.environ['MERLIN_S07_PACK']).resolve()
    ref = Path(os.environ['MERLIN_S07_REFERENCE']).resolve()
    hashes = {}
    with pack_session(pack):
        for name, digest in EXPECTED['pack'].items():
            hashes[str(pack/name)] = verify(pack/name, digest)
        manifest = json.loads((pack/'manifest.json').read_text(encoding='utf-8'))
    for name, digest in EXPECTED['september'].items():
        hashes[str(ref/name)] = verify(ref/name, digest)
    verify(BASE/'pine/S_07 HA v01.pine', EXPECTED['pine_sha256'])
    entries = manifest['instruments']
    if isinstance(entries, dict):
        entries = list(entries.values())
    entry = next(e for e in entries if e['instrument_id']=='OKX_ETH-USDT-SWAP')
    rules = AdmissionRules.from_entry(entry)
    assert rules.base_step == '0.001'
    assert rules.base_currency == 'ETH' and rules.quantity_unit == 'contracts'
    return pack, ref, rules, hashes, entry


def source(pack, ticker='ETH', minutes=30):
    return dict(type='parquet', version=1, pack_root=str(pack), instrument_id=f'OKX_{ticker}-USDT-SWAP', timeframe_minutes=minutes)


def filters(pack):
    return effective_config(dict(version=1,enabled=True,items=[dict(id='btc',type='candleDirection',version=2,
        enabled=True,parameters=dict(confirmationBars=2,breakBars=1),source=source(pack,'BTC',240))]))


def execute(frame, start, params, rules, context):
    """Real registered application adapter, traced reference and compiled singleton."""
    data = strategy.build_v2_execution_data(frame, params)
    trace = KernelTrace()
    kernel = run_reference_kernel(apply_permissions(data,context),
        build_kernel_config(profile=strategy.load_profile(),params=params,trade_start_idx=start,admission_rules=rules),trace=trace)
    result = run_strategy(get_strategy('s07_ha_v01_b2'),frame,params,start,admission_rules=rules,entry_filter_context=context)
    assert result.trades == kernel.trades
    settings = GridV2Settings(enabled_axes=(),compiled_workers=1,top_n=1,admission_rules=rules,
        entry_filter_config=tuple(json.loads(context.effective_json)) if isinstance(context,PreparedPermissions) else ())
    plan = build_grid_v2_plan(strategy.load_config(),settings,params)
    compiled = execute_grid_v2_candidates(plan,frame,start,
        GridV2StrategyHooks.from_strategy(strategy,entry_filter_context=context))
    assert compiled.metadata['compiled_workers'] == 1
    assert compiled.rows[0].status == 'ok'
    assert compiled.rows[0].total_trades == len(result.trades)
    net = sum(t.net_pnl for t in result.trades)
    assert compiled.rows[0].net_profit_pct == pytest.approx(net/params['initialCapital']*100, abs=1e-10)
    return result, trace, compiled


@pytest.mark.parametrize('warmup',[11664,500])
def test_exact_september(inputs, tmp_path, warmup):
    pack,ref,rules,hashes,entry = inputs
    params = strategy.normalized_params(dict(initialCapital=10000.,start='2025-09-01T00:00:00Z',end='2025-10-01T00:00:00Z'))
    with np.load(ref/'eth-btc-prepared-inputs.npz', allow_pickle=False) as stored:
        frame = pd.DataFrame(stored['targetOHLCV'],columns=['Open','High','Low','Close','Volume'],
            index=pd.to_datetime(stored['targetTimes'],unit='ms',utc=True))
        btc = pd.DataFrame(stored['btcOHLCV'],columns=['open','high','low','close','volume'],
            index=pd.to_datetime(stored['btcTimes'],unit='ms',utc=True))
    expected_start = frame.index.searchsorted(pd.Timestamp(params['start']))
    assert expected_start == 11664
    if warmup == 500:
        prepared = prepare_input(source(pack),date_filter=True,start='2025-09-01',end='2025-09-30',
            warmup_bars=500,admission_params=params,entry_filters=filters(pack))
        np.testing.assert_array_equal(prepared.frame[['Open','High','Low','Close','Volume']],frame.iloc[expected_start-500:])
        frame,context = prepared.frame,prepared.entry_filter_context
    else:
        opens = frame.index.as_unit('ns').asi8
        decisions = opens+1800_000_000_000
        context = PreparedPermissions(canonical(filters(pack)),opens,decisions,
            *candle_direction(btc,240,2,decisions,break_bars=1))
    values = indicators(frame)
    data = strategy.build_v2_execution_data(frame,params)
    expectations = rows(ref/'eth-september-decisions.csv')
    assert len(frame)-warmup == len(expectations) == 1440
    actual = dict(haOpen=values.ha_open,haClose=values.ha_close,atr14=values.atr,
        haColor=values.colour,haRun=values.run,longRawEvent=data.signals.long_entries,shortRawEvent=data.signals.short_entries,
        longStop=data.rolling_low-2*values.atr,shortStop=data.rolling_high+2*values.atr,
        btcKnown=context.known,btcState=context.permit_long.astype(int)-context.permit_short.astype(int),
        longAllowed=context.permit_long,shortAllowed=context.permit_short)
    for name,array in actual.items():
        wanted = [r[name]=='True' for r in expectations] if array.dtype==bool else [float(r[name]) for r in expectations]
        np.testing.assert_array_equal(array[warmup:],wanted,err_msg=name)
    assert data.signals.long_entries[warmup:].sum()==44 and data.signals.short_entries[warmup:].sum()==40
    trades,attempts = rows(ref/'eth-september-trades.csv'),rows(ref/'eth-september-attempts.csv')
    report = dict(inputHashes=hashes,ruleEntry=entry,warmup=warmup,allocations={})
    for allocation in json.loads((ref/'eth-account-expectations.json').read_text()):
        aid=allocation['allocationId'];sides=aid.removeprefix('HA_F06_')
        p={**params,'enableLong':'L' in sides,'enableShort':'S' in sides}
        result,trace,compiled=execute(frame,warmup,p,rules,context)
        wanted=[r for r in trades if r['allocationId']==aid]
        assert len(result.trades)==allocation['trades']==len(wanted)
        for trade,event,want in zip(result.trades,trace.exits,wanted):
            assert (1 if trade.direction=='long' else -1)==int(want['side'])
            assert trade.entry_time.value//10**6==int(want['entryMs'])
            # Merlin TradeRecord labels bars by open, including terminal closes.
            semantic_exit = trade.exit_time.value//10**6 + (1800_000 if event.phase=='close' else 0)
            assert semantic_exit==int(want['exitMs'])
            assert frame.index[event.exit_index].value//10**6==int(want['exitBarOpenMs'])
            assert quantity_lots(trade.size,float(rules.base_step))==int(want['lots'])
            assert trade.entry_price==float(want['entryPrice']) and trade.exit_price==float(want['exitPrice'])
        balance=result.balance_curve[-1]
        assert balance==pytest.approx(allocation['finalBalance'],abs=1e-9,rel=1e-12)
        # Kernel attempts eliminated by side/filter gates are joined to raw events.
        by_signal={a.signal_index:a for a in trace.attempts}
        checked=0
        for want in (r for r in attempts if r['allocationId']==aid):
            index=int(want['signalIndex'])-11664+warmup
            side=int(want['side']);enabled=p['enableLong'] if side==1 else p['enableShort']
            permission=context.permit_long[index] if side==1 else context.permit_short[index]
            attempt=by_signal.get(index)
            admitted=enabled and permission and attempt is not None and attempt.trade_index is not None
            assert admitted==(want['tradeId']!=''), (aid,index,want['reason'],attempt)
            checked+=1
        assert checked==84
        report['allocations'][aid]=dict(trades=len(wanted),attempts=checked,balance=balance,
            residual=balance-allocation['finalBalance'],diagnostics=result.entry_diagnostics,compiled=compiled.metadata)
    (tmp_path/'september.json').write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')


@pytest.mark.parametrize('name',['reference_a_filter_on','reference_b_filter_off'])
def test_tradingview_long(inputs,tmp_path,name):
    pack,ref,rules,hashes,entry=inputs
    expected=EXPECTED['tradingview'][name]
    verify(BASE/name/'tradingview_trades.csv',expected['sha256'])
    params=strategy.normalized_params(dict(initialCapital=10000.,start='2025-03-01T00:00:00Z',end='2026-10-01T00:00:00Z'))
    prepared=prepare_input(source(pack),date_filter=True,start='2025-03-01',end='2026-09-30',warmup_bars=2832,
        admission_params=params,entry_filters=filters(pack) if name=='reference_a_filter_on' else [])
    frame=prepared.frame;context=prepared.entry_filter_context
    assert len(frame)==30624 and prepared.trade_start_idx==2832
    assert frame.index[0]==pd.Timestamp('2025-01-01T00:00:00Z')
    result,trace,compiled=execute(frame,prepared.trade_start_idx,params,rules,context)
    raw=rows(BASE/name/'tradingview_trades.csv')
    entries={int(r['Trade number']):r for r in raw if r['Type'].startswith('Entry')}
    exits={int(r['Trade number']):r for r in raw if r['Type'].startswith('Exit')}
    timestamp=lambda r: pd.Timestamp(r['Date and time']).tz_localize('Asia/Shanghai').tz_convert('UTC')
    merlin={(t.entry_time,t.direction):(t,e) for t,e in zip(result.trades,trace.exits)}
    attempts={a.signal_index:a for a in trace.attempts}
    data=strategy.build_v2_execution_data(frame,params)
    values=indicators(frame)
    report=dict(name=name,inputHashes=hashes,rules=rules.semantic(),facts=prepared.facts,filterInputs=prepared.entry_filter_inputs,
        compiled=compiled.metadata,diagnostics=result.entry_diagnostics,
        merlinTrades=len(result.trades),tvTrades=len(entries),merlinNet=sum(t.net_pnl for t in result.trades),
        comparisons=[],missing=[],extra=[],attempts=[asdict(a) for a in trace.attempts])
    full_balance=10000.;previous_cumulative=0.;tv_keys=set()
    for tid,e in entries.items():
        x=exits[tid];direction=e['Type'].split()[1];sign=1 if direction=='long' else -1
        key=(timestamp(e),direction);tv_keys.add(key)
        index=frame.index.get_loc(key[0])-1
        entry_price=float(e['Price USDT']);exit_price=float(x['Price USDT']);quantity=float(e['Size (qty)'])
        tv_lots=round(quantity/.001)
        stop=data.rolling_low[index]-2*values.atr[index] if sign==1 else data.rolling_high[index]+2*values.atr[index]
        risk=(frame.Close.iloc[index]-stop)*sign
        target=frame.Close.iloc[index]+sign*3*risk
        cumulative_balance=10000.+previous_cumulative
        sized=lambda balance: math.floor(balance*.02/risk/.001)
        lo,hi=sized(cumulative_balance-.005),sized(cumulative_balance+.005)
        # Exact fees reconstructed from exported prices/lots; rounded cumulative
        # PnL is retained separately with its half-cent uncertainty interval.
        fee=(entry_price+exit_price)*quantity*.0005
        tv_net=(exit_price-entry_price)*quantity*sign-fee
        observed=dict(trade=tid,side=direction,signal=str(frame.index[index]),entry=str(key[0]),tvExit=str(timestamp(x)),
            rawEvent=bool(data.signals.long_entries[index] if sign==1 else data.signals.short_entries[index]),
            permission=True if context is OFF else bool(context.permit_long[index] if sign==1 else context.permit_short[index]),
            atr=values.atr[index],low=data.rolling_low[index],high=data.rolling_high[index],stop=stop,target=target,risk=risk,
            signalClose=frame.Close.iloc[index],tvEntry=entry_price,tvExitPrice=exit_price,tvLots=tv_lots,
            tvBalanceReconstructed=full_balance,tvBalanceFromCumulative=cumulative_balance,
            recoveredLots=sized(full_balance),centUncertaintyLots=[lo,hi],tvFees=fee,tvNetReconstructed=tv_net,
            tvNetExported=float(x['Net PnL USDT']),tvCumulative=float(x['Cumulative PnL USDT']),
            pinePlannedLeverage=quantity*frame.Close.iloc[index]/full_balance,
            actualPostFeeLeverage=quantity*entry_price/(full_balance-quantity*entry_price*.0005))
        if key not in merlin:
            observed['attempt']=asdict(attempts[index]) if index in attempts else None
            report['missing'].append(observed)
        else:
            trade,event=merlin[key]
            observed.update(merlinExit=str(trade.exit_time),merlinExitPrice=trade.exit_price,
                merlinLots=quantity_lots(trade.size,.001),merlinNet=trade.net_pnl,exitReason=event.reason,
                exitPhase=event.phase,merlinFees=event.entry_fee+event.exit_fee,merlinAttempt=asdict(attempts[index]),
                exitBar=frame.iloc[event.exit_index][['Open','High','Low','Close']].to_dict())
            if event.reason in ('stop','target') and event.phase=='intrabar':
                # Outward is lower for long stops / short targets, higher otherwise.
                outward=sign if event.reason=='target' else -sign
                observed['outwardPriceDelta']=outward*(exit_price-trade.exit_price)
            report['comparisons'].append(observed)
        full_balance+=tv_net
        previous_cumulative=float(x['Cumulative PnL USDT'])
    for key,(trade,event) in merlin.items():
        if key not in tv_keys:
            report['extra'].append(dict(trade=asdict(trade),exit=asdict(event),attempt=asdict(attempts[event.signal_index])))
    report['tvReconstructedBalance']=full_balance
    (tmp_path/(name+'.json')).write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')
    assert len(entries)==expected['trades']
    assert not report['missing'] and not report['extra'], f"See retained comparison: {tmp_path}"
    for row in report['comparisons']:
        assert row['rawEvent'] and row['permission']
        assert row['tvExit']==row['merlinExit'], row
        assert row['tvEntry']==frame.Open.loc[pd.Timestamp(row['entry'])]
        if 'outwardPriceDelta' in row:
            assert -1e-10 <= row['outwardPriceDelta'] < .01+1e-10, row
        else:
            assert row['tvExitPrice']==row['merlinExitPrice'], row
        assert row['tvLots']==row['recoveredLots'] or row['centUncertaintyLots'][0]<=row['tvLots']<=row['centUncertaintyLots'][1],row
