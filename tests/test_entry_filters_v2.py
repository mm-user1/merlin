"""Versioned F06 state, declared-origin preparation and immutable replay."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from core.data_input import DataInputError, prepare_input, replay_input, prepare_manual_input
from core.entry_filters import candle_direction, canonical, descriptor, effective_config
from core.entry_filter_input import describe_saved, source_bounds, validate_envelope, verify_relocation
from tests._data_input_helpers import publish


def editor(source, n=3, m=2):
    return dict(version=1,enabled=True,items=[dict(id='f',type='candleDirection',version=2,enabled=True,
        parameters=dict(confirmationBars=n,breakBars=m),source=source)])


def bars(colours, minutes=5):
    return pd.DataFrame(dict(open=np.full(len(colours),100.),close=100+np.array(colours)),
        index=pd.date_range('2024-01-01',periods=len(colours),freq=f'{minutes}min',tz='UTC'))


@pytest.mark.parametrize('n',range(2,7))
def test_m1_exact_v1_permissions(n):
    rng=np.random.default_rng(47)
    f=bars(rng.choice([-1,0,1],900)).drop(pd.date_range('2024-01-02',periods=3,freq='5min',tz='UTC'))
    decisions=pd.date_range('2024-01-01',periods=920,freq='5min',tz='UTC').asi8
    for old,new in zip(candle_direction(f,5,n,decisions),candle_direction(f,5,n,decisions,break_bars=1)):
        np.testing.assert_array_equal(old,new)


@pytest.mark.parametrize('sign',[1,-1])
def test_retained_state_activation_neutral_gap_and_expiry(sign):
    f=bars(np.array([0]*29+[1,1,1,-1,1,-1,-1,1,1,1,0,-1,-1,-1])*sign)
    step=300_000_000_000;closes=f.index.asi8+step
    lo,sh,known,why=candle_direction(f,5,3,closes,break_bars=2)
    direction=lo.astype(int)-sh.astype(int)
    np.testing.assert_array_equal(direction[29:],np.array([0,0,1,1,1,1,0,0,0,1,0,0,0,-1])*sign)
    assert not known[:28].any() and known[28:].all()
    latest=candle_direction(f,5,3,np.array([closes[-1]-1,closes[-1],closes[-1]+step-1,closes[-1]+step]),break_bars=2)
    assert latest[2].tolist()==[True,True,True,False]
    retained=bars([1]*31+[-1,1]*400)
    long=candle_direction(retained,5,3,retained.index.asi8+step,break_bars=2)[0]
    assert long[28:].all()
    assert not candle_direction(retained.iloc[-29:],5,3,np.array([retained.index[-1].value+step]),break_bars=2)[0][0]
    gap=retained.drop(retained.index[400])
    observed=candle_direction(gap,5,3,retained.index.asi8+step,break_bars=2)
    assert observed[3][400]==2 and not observed[2][401:429].any() and observed[2][429]
    assert not observed[0][401:].any()  # Alternation cannot restore the lost direction.


@pytest.mark.parametrize('n,m',[(True,1),(3,True),(3.,1),(2,2),(7,1),(6,5),(3,0),(3,1.5)])
def test_v2_domains_and_inactive_preservation(n,m):
    source=dict(type='parquet',version=1,pack_root='/pack',instrument_id='OKX_BTC-USDT-SWAP',timeframe_minutes=240)
    value=editor(source,n,m)
    with pytest.raises(DataInputError):effective_config(value)
    value['enabled']=False
    assert effective_config(value)==[]


def test_v1_large_n_and_v2_identity_are_distinct():
    source=dict(type='parquet',version=1,pack_root='/pack',instrument_id='OKX_BTC-USDT-SWAP',timeframe_minutes=240)
    v2=editor(source,2,1)['items'][0];v1=deepcopy(v2);v1.update(version=1,parameters={'confirmationBars':2})
    assert canonical(descriptor(v1)) != canonical(descriptor(v2))
    v1['parameters']['confirmationBars']=100
    assert descriptor(v1)['version']==1


def test_declared_origin_replay_relocation_parent_and_overlap(tmp_path,monkeypatch):
    from data_provider import data
    source=publish(tmp_path/'pack',rows=1500)
    value=editor(source);effective=effective_config(value)
    calls=[];original=data._ReadSession.load_slice
    def counted(self,*a,**kw):calls.append(kw);return original(self,*a,**kw)
    monkeypatch.setattr(data._ReadSession,'load_slice',counted)
    kwargs=dict(date_filter=True,start='2024-01-03',end='2024-01-03',warmup_bars=2,entry_filters=effective)
    prepared=prepare_input(source,**kwargs)
    assert len(calls)==2  # Same session, one target read and one full-origin source read.
    pin=prepared.entry_filter_inputs['sources'][0]
    assert pin['policy']=='closed_candle_direction_v2' and pin['originRule']=='declared_source_origin'
    assert pd.Timestamp(pin['bounds']['start'])==pd.Timestamp('2024-01-01',tz='UTC')
    config=dict(entryFilters=value,entry_filter_inputs=prepared.entry_filter_inputs,data_input=prepared.facts)
    assert 'N 3 / break 2' in describe_saved(config)['text']
    second=prepare_input(source,**{**kwargs,'start':'2024-01-02'})
    a,b=prepared.entry_filter_context,second.entry_filter_context
    idx=b.indices(prepared.frame.index)
    np.testing.assert_array_equal(a.permit_long,b.permit_long[idx])
    np.testing.assert_array_equal(a.permit_short,b.permit_short[idx])
    relocated=publish(tmp_path/'relocated',rows=1800)
    root=relocated['pack_root'];original_root=effective[0]['source']['pack_root']
    verify_relocation(config,original_root,root)
    replay=replay_input(prepared.facts,root_override=root,entry_filters=effective,
        entry_filter_inputs=prepared.entry_filter_inputs,filter_root_overrides={original_root:root})
    assert replay.entry_filter_inputs==prepared.entry_filter_inputs
    manual=prepare_manual_input(prepared.facts,date_filter=True,start='2024-01-04',end='2024-01-04',warmup_bars=2,
        root_override=root,entry_filters=effective,parent_filter_inputs=prepared.entry_filter_inputs,
        filter_root_overrides={original_root:root})
    assert manual.entry_filter_inputs['sources'][0]['bounds']['start']==pin['bounds']['start']
    repaired=publish(tmp_path/'repaired',rows=1800,change=1)
    with pytest.raises(DataInputError,match='filter.*(changed|differ)'):
        replay_input(prepared.facts,root_override=repaired['pack_root'],entry_filters=effective,
            entry_filter_inputs=prepared.entry_filter_inputs,filter_root_overrides={original_root:repaired['pack_root']})
    for field,bad in [('policy','closed_candle_direction_v1'),('originRule','bounded_prefix')]:
        broken=deepcopy(config);broken['entry_filter_inputs']['sources'][0][field]=bad
        assert describe_saved(broken)['text']=='Unavailable'
    broken=deepcopy(config);broken['data_input']['bounds']['end']='2024-01-05T00:00:00Z'
    assert describe_saved(broken)['text']=='Unavailable'
    broken=deepcopy(config);broken['entry_filter_inputs']['sources'][0]['bounds']['start']='2024-01-03T00:00:00Z'
    broken['entry_filter_inputs']['sources'][0]['bounds']['origin']='2024-01-03T00:00:00Z'
    assert describe_saved(broken)['text']=='Unavailable'


def test_m1_bounds_policy_and_method_corruption(tmp_path):
    source=publish(tmp_path/'pack')
    effective=effective_config(editor(source,3,1))
    prepared=prepare_input(source,date_filter=True,start='2024-01-03',end='2024-01-03',warmup_bars=2,entry_filters=effective)
    pin=prepared.entry_filter_inputs['sources'][0]
    assert pin['originRule']=='bounded_prefix' and pin['bounds']==source_bounds(prepared.facts,effective[0])
    validate_envelope(prepared.entry_filter_inputs,effective,prepared.facts)
    broken=deepcopy(prepared.entry_filter_inputs)
    broken['sources'][0]['bounds'].update(start='2024-01-01T00:00:00+00:00',origin='2024-01-01T00:00:00+00:00')
    with pytest.raises(DataInputError,match='bounds'):
        validate_envelope(broken,effective,prepared.facts)


def test_backward_extension_retains_parent_origin_and_mixed_versions(tmp_path):
    from tests._s07_helpers import publish as publish_s07
    source=publish_s07(tmp_path/'original',first_slot=288)
    filtered=editor({**source,'timeframe_minutes':60})
    effective=effective_config(filtered)
    prepared=prepare_input(source,date_filter=True,start='2024-01-10',end='2024-01-15',warmup_bars=100,entry_filters=effective)
    parent=prepared.entry_filter_inputs
    extended=publish_s07(tmp_path/'extended',first_slot=0)
    root=extended['pack_root'];original=effective[0]['source']['pack_root']
    overrides={original:root}
    replay=replay_input(prepared.facts,root_override=root,entry_filters=effective,entry_filter_inputs=parent,filter_root_overrides=overrides)
    assert replay.entry_filter_inputs==parent
    child=prepare_manual_input(prepared.facts,date_filter=True,start='2024-01-12',end='2024-01-16',warmup_bars=100,
        root_override=root,entry_filters=effective,parent_filter_inputs=parent,filter_root_overrides=overrides)
    assert child.entry_filter_inputs['sources'][0]['bounds']['start']==parent['sources'][0]['bounds']['start']
    fresh=prepare_input(extended,date_filter=True,start='2024-01-12',end='2024-01-16',warmup_bars=100,
        entry_filters=effective_config(editor({**extended,'timeframe_minutes':60})))
    assert fresh.entry_filter_inputs['sources'][0]['bounds']['start']!=child.entry_filter_inputs['sources'][0]['bounds']['start']
    v1=deepcopy(filtered['items'][0]);v1.update(id='old',version=1,parameters={'confirmationBars':3})
    filtered['items'].append(v1)
    mixed=prepare_input(source,date_filter=True,start='2024-01-10',end='2024-01-15',warmup_bars=100,entry_filters=effective_config(filtered))
    assert {pin['descriptor']['version'] for pin in mixed.entry_filter_inputs['sources']}=={1,2}
    assert np.all(~mixed.entry_filter_context.permit_long | prepared.entry_filter_context.permit_long)
    assert np.all(~mixed.entry_filter_context.permit_short | prepared.entry_filter_context.permit_short)


@pytest.mark.parametrize('minutes',[5,240,4320])
def test_v2_native_causal_prefix_and_declared_origin_rounding(minutes):
    f=bars([1]*32+[-1,1]*80+[-1]*10,minutes)
    step=minutes*60*10**9
    decisions=f.index.as_unit('ns').asi8+step
    whole=candle_direction(f,minutes,3,decisions,break_bars=2)
    prefix=candle_direction(f.iloc[:90],minutes,3,decisions[:90],break_bars=2)
    for a,b in zip(whole,prefix): np.testing.assert_array_equal(a[:90],b)
    item=effective_config(editor(dict(type='parquet',version=1,pack_root='/pack',instrument_id='TEST_X',timeframe_minutes=minutes)))[0]
    target={'source':{'timeframe_minutes':minutes},'bounds':{'start':str(f.index[100]),'end':str(f.index[-1])}}
    aligned=source_bounds(target,item,declared_first_open=str(f.index[0]+pd.Timedelta(minutes=1)))
    first=pd.Timestamp(f.index[0]+pd.Timedelta(minutes=1)).value
    assert pd.Timestamp(aligned['start']).value==-(-first//step)*step
    with pytest.raises(DataInputError,match='prefix'):
        source_bounds(target,item,declared_first_open=target['bounds']['start'])
