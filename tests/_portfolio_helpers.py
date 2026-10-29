"""Small deterministic import-built packs, never production market data."""
import numpy as np
import pandas as pd
import pytest
from uuid import uuid4

from data_provider import data, manifest
from tests.fixtures.admission_strategy.strategy import rule_entry

STRATEGY = 's07_ha_v01_b2'


@pytest.fixture
def portfolio_database():
    """Opt-in module fixture: each test owns a DB and always restores its caller."""
    from core import storage
    previous = storage.get_active_db_name()
    try:
        storage.create_new_db('portfolio_' + uuid4().hex)
        yield
    finally:
        storage.set_active_db(previous)


def publish(root, *, start='2023-12-01', end='2024-06-01', venues=('BYBIT', 'OKX'), transform=None):
    index = pd.date_range(start, end, freq='5min', inclusive='left', tz='UTC')
    slots = np.arange(len(index))
    instruments, sources = [], []
    for i, venue in enumerate(venues):
        entry = rule_entry(venue, step='.01', minimum='.01', notional='0')
        opening = 100 + 6 * np.sin(slots / (17 + i))
        close = 100 + 6 * np.sin((slots + 1) / (17 + i))
        values = np.column_stack([opening, np.maximum(opening, close) + .2 + i * .3,
                                 np.minimum(opening, close) - .2 - i * .3, close, np.ones(len(index))])
        stamps = index.as_unit('ms').asi8
        if transform:
            stamps, values, entry = transform(i, stamps.copy(), values, entry)
        instruments.append(data.InstrumentSource(symbol='AAA', venue=venue, contract=entry['contract'], quote_currency='USDT',
            roles=['trading'], timestamps=stamps, ohlcv=values, instrument_rules=entry['instrument_rules'],
            source=manifest.build_source_metadata(input_format='synthetic', input_dtype='float64',
                source_reference='T08 deterministic fixture', volume_unit_evidence='generated quote turnover'),
            verification=manifest.build_verification(volume_quote_verified=True, volume_quote_evidence='synthetic',
                closed_before_utc=end+'T00:00:00Z', closure_evidence='synthetic', closure_source='test')))
        sources.append(dict(type='parquet', version=1, pack_root=str(root), instrument_id=entry['instrument_id'], timeframe_minutes=60))
    data.publish_pack(root, instruments, universe=manifest.build_universe())
    return sources


def payload():
    return dict(parameterMode='fixed', optimization_mode='grid', enabled_params={}, param_ranges={},
                fixed_params=dict(dateFilter=True, start='2024-02-01', end='2024-04-30', initialCapital=237,
                                  enableLong=True, enableShort=True, entryRunLength=2), worker_processes=1,
                entryFilters=dict(version=1, enabled=False, items=[]))


def selector(**changes):
    result = dict(version=1, enabled=True, method='meanBarRangePct', methodVersion=1, topN=2,
                rebalanceMonths=2, evaluateUnselected=False,
                params=dict(lookbackMonths=1, timeframeMinutes=30))
    result.update(changes)
    return result
