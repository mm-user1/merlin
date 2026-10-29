import math

import numpy as np
import pandas as pd
import pytest

from core.ticker_selector import build_calendar, normalize_pool, normalize_selector, rank_pool
from tests._portfolio_helpers import publish, selector


def constant(i, stamps, values, entry):
    values[:, :4] = [100, 102, 98, 100]
    return stamps, values, entry


def test_x_independent_formula_ascii_tie_and_future_invariance(tmp_path):
    pool = publish(tmp_path/'a', transform=constant)
    result = rank_pool(pool, '2024-02-01', selector())
    assert [r['instrument'] for r in result] == sorted(p['instrument_id'] for p in pool)
    assert all(r['score'] == 4 and r['months'][0]['observed'] == 31*48 for r in result)
    def future(i, stamps, values, entry):
        stamps, values, entry = constant(i, stamps, values, entry)
        values[stamps >= pd.Timestamp('2024-02-01',tz='UTC').value//10**6, 1] = 1000
        return stamps, values, entry
    later = rank_pool(publish(tmp_path/'b', transform=future), '2024-02-01', selector())
    assert [(r['instrument'],r['score'],r['rank']) for r in result] == [(r['instrument'],r['score'],r['rank']) for r in later]


@pytest.mark.parametrize('missing,accepted', [(14,True),(15,False)])
def test_integer_monthly_99_percent(tmp_path, missing, accepted):
    # 1488 expected half-hour bars: 14 missing pass, 15 fail.
    def gaps(i, stamps, values, entry):
        left = pd.Timestamp('2024-01-15',tz='UTC').value//10**6
        mask = (stamps < left) | (stamps >= left+missing*30*60*1000)
        return stamps[mask], values[mask], entry
    pool = publish(tmp_path/'pack', transform=gaps)
    if accepted:
        assert rank_pool(pool,'2024-02-01',selector())[0]['months'][0]['observed'] == 1488-missing
    else:
        with pytest.raises(ValueError,match='2024-01.*99%'):
            rank_pool(pool,'2024-02-01',selector())


def test_month_straddlers_excluded_and_tf_changes(tmp_path):
    pool = publish(tmp_path/'pack')
    for tf in (30, 70, 2*1440):
        settings = selector(params=dict(lookbackMonths=2,timeframeMinutes=tf))
        rows = rank_pool(pool,'2024-02-01',settings)
        from data_provider.data import read_session
        for row in rows:
            terms = []
            with read_session(pool[0]['pack_root']) as session:
                for month in row['months']:
                    frame = session.load_slice(row['instrument'],start=month['start'],end=month['end'],timeframe_minutes=tf).bars
                    assert len(frame) == month['expected']
                    assert all(t.month == (t+pd.Timedelta(minutes=tf)-pd.Timedelta(nanoseconds=1)).month for t in frame.index)
                    terms += [100*(h-l)/c for h,l,c in zip(frame.high,frame.low,frame.close)]
            assert row['score'] == math.fsum(terms)/len(terms)


def test_listing_vs_late_acquisition_and_pool_admission(tmp_path):
    def new_listing(i, stamps, values, entry):
        entry['instrument_rules']['listed_at_utc'] = '2024-01-15T00:00:00Z'
        entry['instrument_rules']['raw_contract_fields']['launchTime' if entry['venue']=='BYBIT' else 'listTime'] = str(pd.Timestamp('2024-01-15',tz='UTC').value//10**6)
        return stamps, values, entry
    pool = publish(tmp_path/'listed', start='2024-01-15', transform=new_listing)
    rows = rank_pool(pool,'2024-02-01',selector())
    assert all(not r['eligible'] and r['score'] is None for r in rows)
    pool = publish(tmp_path/'late', start='2024-01-15')
    with pytest.raises(ValueError):
        rank_pool(pool,'2024-02-01',selector())
    assert len(normalize_pool(pool+pool)) == 2
    with pytest.raises(ValueError):
        normalize_pool([pool[0],dict(pool[1],timeframe_minutes=30)])


def test_selector_disabled_inert_and_active_unknown_rejected():
    assert normalize_selector({'enabled':False,'future':42})['future'] == 42
    for bad in ({'future':42},{'method':'pnl'},{'methodVersion':2}):
        with pytest.raises(ValueError): normalize_selector(selector(**bad))


def test_calendar_multiday_alignment_is_per_plan():
    # Epoch-aligned 2-day boundaries: February 2024 has an odd number of days.
    assert build_calendar('2024-04-01','2024-04-30',2880,1)
    with pytest.raises(ValueError,match='straddles'):
        build_calendar('2024-02-01','2024-02-29',2880,1)
