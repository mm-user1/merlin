"""Small synthetic S07 pack and normal application requests for tests/preview."""
import numpy as np
import pandas as pd

from data_provider import data, manifest
from strategies.s07_ha_v01_b2.strategy import normalized_params
from tests.fixtures.admission_strategy.strategy import rule_entry

STRATEGY = 's07_ha_v01_b2'


def publish(root, rows=8000, first_slot=0):
    entry=rule_entry(step='.001',minimum='.001',notional='0')
    slots=np.arange(first_slot,rows,dtype=np.int64)
    timestamps=pd.Timestamp('2024-01-01',tz='UTC').value//10**6+slots*300000
    opening=100+8*np.sin(slots/30)
    close=100+8*np.sin((slots+1)/30)
    values=np.column_stack((opening,np.maximum(opening,close)+.4,np.minimum(opening,close)-.4,close,np.ones(len(slots))))
    instrument=data.InstrumentSource(symbol='AAA',venue=entry['venue'],contract=entry['contract'],quote_currency='USDT',
        roles=['trading'],timestamps=timestamps,ohlcv=values,instrument_rules=entry['instrument_rules'],
        source=manifest.build_source_metadata(input_format='synthetic',input_dtype='float64',
            source_reference='S07 deterministic sine fixture',volume_unit_evidence='generated quote turnover'),
        verification=manifest.build_verification(volume_quote_verified=True,volume_quote_evidence='generated quote turnover',
            closed_before_utc=manifest.format_epoch_ms(int(timestamps[-1])+300000),closure_evidence='synthetic closed bars',closure_source='test'))
    data.publish_pack(root,[instrument],universe=manifest.build_universe())
    return dict(type='parquet',version=1,pack_root=str(root),instrument_id=entry['instrument_id'],timeframe_minutes=30)


def payload(source, *, fixed=False, enabled=True, version=2):
    params=normalized_params(dict(dateFilter=True,start='2024-01-06',end='2024-01-26'))
    params.pop('warmupBars')
    item=dict(id='filter',type='candleDirection',version=version,enabled=True,
        parameters=dict(confirmationBars=3,**({'breakBars':2} if version==2 else {})),
        source={**source,'timeframe_minutes':60})
    return dict(optimization_mode='grid',parameterMode='fixed' if fixed else 'search',
        enabled_params={} if fixed else {'entryRunLength':True},
        param_ranges={} if fixed else {'entryRunLength':[5,6,1]},param_types={'entryRunLength':'int'},
        fixed_params=params,objectives=['net_profit_pct'],grid_fast_objectives=['net_profit_pct'],
        grid_top_candidates=2,grid_slow_refinement_enabled=not fixed,grid_slow_objectives=['net_profit_pct'],
        worker_processes=2,entryFilters=dict(version=1,enabled=enabled,items=[item]))
