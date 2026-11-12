"""Exact native metrics plus canonical observed-day and calendar calculations."""
from calendar import monthrange
from datetime import datetime, timezone
import importlib
import math

import numpy as np
import pandas as pd

from ..account_study.metrics import account_metrics, drawdown
from ..study.bracket import reference_core

DAY = 86400000
VERSION = 'configuration-metrics.1'
NATIVE_FIELDS = ('initialCapital','finalCapital','netPnl','grossPnl','fees','returnPct',
    'trades','wins','losses','breakevens','winRatePct','profitFactor','profitFactorStatus',
    'timeInMarketPct','maxEntryLeverage','exitReasons','attemptDispositions','terminalStatus','coverage')


def month_equivalents(start, end):
    current=datetime.fromtimestamp(start/1000,timezone.utc)
    total=0.
    while int(current.timestamp()*1000)<end:
        month=datetime(current.year,current.month,1,tzinfo=timezone.utc)
        following=datetime(current.year+(current.month==12),current.month%12+1,1,tzinfo=timezone.utc)
        right=min(end,int(following.timestamp()*1000))
        total+=(right-max(start,int(month.timestamp()*1000)))/(monthrange(current.year,current.month)[1]*DAY)
        current=following
    return total


def annualized(return_pct, days):
    short=31<=days<90
    if days<=30:return None,'shortSpan',short
    multiple=1+return_pct/100
    if multiple<=0:return None,'nonpositiveMultiple',short
    try:value=(multiple**(365/days)-1)*100
    except (OverflowError,ValueError):return None,'invalidOrOverflow',short
    return (float(value),'available',short) if math.isfinite(value) else (None,'invalidOrOverflow',short)


def daily_sharpe(equity, opens, initial, trades):
    reference_core()
    core=importlib.import_module('core.metrics')
    times=pd.DatetimeIndex(pd.to_datetime(opens,unit='ms',utc=True))
    returns=core._calculate_daily_returns(equity,times,initial_equity=initial)
    if returns is None:return None,'invalidDailySeries',None,None
    count=int(returns.size)
    active=int(np.count_nonzero(np.abs(returns)>core._DAILY_ACTIVE_RETURN_TOLERANCE))
    if not trades:return None,'noTrades',count,active
    value=core._calculate_sharpe_daily_value(returns)
    status='available' if value is not None else 'tooFewObservations' if count<2 else 'zeroOrInvalidVariance'
    return value,status,count,active


def extract_metrics(record):
    native=account_metrics(record)
    result={k:native[k] for k in NATIVE_FIELDS}
    r=record.result;path=r.path.to_pydict()
    observed=len(path['barOpenMs'])
    result.update(metricsVersion=VERSION,performanceStatus='available' if observed else 'noObservedBars',
        mtmMaxDdPct=native['maxDrawdownMtmPct'],
        balanceMaxDdPct=drawdown(path['balance'],r.policy.initialCapital) if observed else None,
        readiness=dict(commands=native['commandReadiness'],unknownExitDecisions=native['unknownExitDecisions'],
            filterUnknownDecisionBars=native['filterUnknownDecisionBars'],filterUnknownAttempts=native['filterUnknownAttempts'],
            filterBlockedAttempts=native['filterBlockedAttempts']))
    if result['trades']==0:result['profitFactorStatus']='noTrades'
    ratio,status,count,active=daily_sharpe(path['equity'],path['barOpenMs'],r.policy.initialCapital,result['trades'])
    result.update(dailySharpe=ratio,dailySharpeStatus=status,dailySharpeObservations=count,dailySharpeActiveDays=active)
    months=month_equivalents(r.window.startMs,r.window.endMs)
    ann,status,short=annualized(result['returnPct'],(r.window.endMs-r.window.startMs)/DAY)
    result.update(monthEquivalents=months,tradesPerMonth=result['trades']/months,
        annualizedProfitPct=ann if observed else None,
        annualizedProfitStatus=status if observed else 'noObservedBars',annualizedProfitShortSpan=short)
    return result
