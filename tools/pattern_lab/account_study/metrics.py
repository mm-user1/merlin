"""Descriptive metrics from retained evidence; never executes an account."""
from collections import Counter
from datetime import datetime, timezone
from dataclasses import asdict
import math

import numpy as np

from ..accounts.contracts import require
from . import robustness

VERSION = 'pattern-lab.account-metrics.v2'
DAY = 86400000


def drawdown(equity, initial):
    values = np.r_[initial, equity].astype(np.float64)
    peaks = np.maximum.accumulate(values)
    return float(np.max(100*(peaks-values)/peaks))


def reconciles(a, b):
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-8)


def synchronized(record):
    """Fill missing grid slots only with provably flat cash, never market marks."""
    r, p = record.result, record.prepared
    opens = np.arange(r.window.startMs, r.window.endMs, p.stepMs, dtype=np.int64)
    eq = np.full(len(opens), r.policy.initialCapital)
    observed = np.zeros(len(opens), bool)
    path = r.path.to_pydict()
    positions = np.searchsorted(opens, path['barOpenMs'])
    for j, index in enumerate(positions):
        eq[index] = path['equity'][j]; observed[index] = True
        until = int(positions[j+1]) if j+1 < len(positions) else len(opens)
        if until > index+1:
            require(path['side'][j] == 0 and path['quantity'][j] == 0 and
                    path['equity'][j] == path['balance'][j], 'cannot fill missing open-position equity')
            eq[index+1:until] = path['balance'][j]
    return opens+p.stepMs, eq, observed


def month_intervals(start, end):
    t = datetime.fromtimestamp(start/1000, timezone.utc)
    month = datetime(t.year, t.month, 1, tzinfo=timezone.utc)
    while int(month.timestamp()*1000) < end:
        nxt = datetime(month.year+(month.month == 12), month.month % 12+1, 1, tzinfo=timezone.utc)
        left, right = int(month.timestamp()*1000), int(nxt.timestamp()*1000)
        yield month.strftime('%Y-%m'), max(left, start), min(right, end), left < start or right > end
        month = nxt


def monthly(closes, equity, observed, start, end, initial, final, *, account_count=1):
    """Observed is an integer count of accounts with an actual close at a slot."""
    rows = []
    observed = np.asarray(observed, dtype=np.int64)
    def boundary(t):
        indices = np.flatnonzero((closes <= t) & (observed > 0))
        if t == start: return initial, start, True
        stamp = int(closes[indices[-1]]) if len(indices) else None
        value = float(equity[np.searchsorted(closes, t, side='right')-1]) if len(closes) and t >= closes[0] else initial
        return (final if t == end else value), stamp, stamp == t
    for label, left, right, partial in month_intervals(start, end):
        opening, opening_stamp, opening_exact = boundary(left)
        closing, closing_stamp, closing_exact = boundary(right)
        mask = (closes > left) & (closes <= right)
        count = int(observed[mask].sum())
        expected = int(mask.sum())*account_count
        pnl = closing-opening
        rows.append(dict(month=label, viewKind='continuousContribution', startMs=left, endMs=right,
            openingEquity=opening, closingEquity=closing, pnl=pnl, contributionPct=100*pnl/initial,
            returnPct=100*pnl/opening if count and opening > 0 else None,
            returnStatus='noObservedBars' if not count else 'nonpositiveOpeningEquity' if opening <= 0 else 'available',
            observedBars=count, expectedBars=expected, coveragePct=100*count/expected if expected else None,
            partialEdgeMonth=partial, openingSampleMs=opening_stamp, closingSampleMs=closing_stamp,
            openingExact=opening_exact, closingExact=closing_exact))
    require(reconciles(sum(row['pnl'] for row in rows), final-initial), 'monthly PnL reconciliation')
    return rows


def breadth(rows):
    observed = [r for r in rows if r['observedBars'] > 0]
    full = [r for r in observed if r.get('allAccountsObserved', True)]
    return dict(positiveMonths=sum(r['pnl'] > 0 for r in full), breadthDenominator=len(full),
                observedMonths=len(observed), fullMonths=len(full), partialMonths=len(observed)-len(full),
                unobservedMonths=len(rows)-len(observed))


def curve(rows, start, end, initial):
    points = [dict(timeMs=start, returnPct=0.)]
    points.extend(dict(timeMs=r['endMs'], returnPct=100*(r['closingEquity']/initial-1)) for r in rows)
    return points


def account_metrics(record):
    r, p = record.result, record.prepared
    initial, final = r.policy.initialCapital, r.finalBalance
    path, trades, attempts = r.path.to_pydict(), r.trades.to_pydict(), r.attempts.to_pydict()
    net = final-initial
    pnl = np.asarray(trades['netPnl'], dtype=float)
    holding = np.asarray(trades['holdingMs'], dtype=np.int64)
    gross = float(sum(trades['grossPnl']))
    fees = float(sum(trades['entryFee'])+sum(trades['exitFee']))
    require(reconciles(float(pnl.sum()), net) and reconciles(gross-fees, net), 'metric cashflow reconciliation')
    positive, negative = float(pnl[pnl > 0].sum()), float(-pnl[pnl < 0].sum())
    total_holding = int(holding.sum())
    occupied = set()
    forced = {}
    for j, (entry, exit_) in enumerate(zip(trades['entryIndex'], trades['exitIndex'])):
        last = exit_ if trades['exitPhase'][j] != 'open' or exit_ == entry else exit_-1
        occupied.update(range(entry, last+1))
    for reason in ('terminal', 'gapBoundary'):
        selected = [j for j, v in enumerate(trades['exitReason']) if v == reason]
        forced[reason] = dict(trades=len(selected), netPnl=sum(trades['netPnl'][j] for j in selected))
    indices = np.asarray(path['barIndex'], dtype=np.int64)
    readiness = {}
    for side in ('long', 'short'):
        if r.policy.direction not in ('both', side): continue
        commands = getattr(r.commands, side)
        entry, stop = ~commands.entryKnown[indices], ~commands.initialStopKnown[indices]
        readiness[side] = dict(observedDecisionBars=len(indices), unknownEntryDecisionBars=int(entry.sum()),
            unknownInitialStopDecisionBars=int(stop.sum()), unknownEitherDecisionBars=int((entry | stop).sum()))
    filt = r.commands.entryFilter
    times, eq, observed = synchronized(record)
    months = monthly(times, eq, observed, r.window.startMs, r.window.endMs, initial, final)
    passive = dict(status='noObservedBars', longReturnPct=None, shortReturnPct=None)
    if len(indices):
        first, last = indices[0], indices[-1]
        op, cl = float(p.target.values[first, 0]), float(p.target.values[last, 3])
        passive = dict(status='available', firstOpen=op, lastClose=cl,
            firstOpenMs=int(p.target.timestamps[first]), lastCloseMs=int(p.target.timestamps[last])+p.stepMs,
            observedSpanMs=int(p.target.timestamps[last]-p.target.timestamps[first])+p.stepMs,
            longReturnPct=100*(cl/op-1), shortReturnPct=-100*(cl/op-1), coverage=asdict(r.coverage))
    result = dict(**record.labels, accountId=r.identity, schema=VERSION, coverage=asdict(r.coverage),
        terminalStatus=r.terminalStatus, initialCapital=initial, finalCapital=final,
        netPnl=net, grossPnl=gross, fees=fees, returnPct=100*(final/initial-1),
        maxDrawdownMtmPct=drawdown(path['equity'], initial) if len(indices) else None,
        trades=len(pnl), wins=int((pnl > 0).sum()), losses=int((pnl < 0).sum()), breakevens=int((pnl == 0).sum()),
        winRatePct=100*int((pnl > 0).sum())/len(pnl) if len(pnl) else None,
        profitFactor=positive/negative if negative else None, profitFactorStatus='available' if negative else 'noLosses',
        attemptDispositions=dict(sorted(Counter(attempts['reason']).items())), exitReasons=dict(sorted(Counter(trades['exitReason']).items())),
        forcedExits=forced, ambiguousTrades=sum(trades['ambiguous']), openingStops=sum(trades['openingStop']),
        openingTargets=sum(trades['openingTarget']), explicitExitPending=sum(trades['explicitExitPending']), expiryPending=sum(trades['expiryPending']),
        unknownExitDecisions=r.unknownExitDecisions, commandReadiness=readiness,
        filterUnknownDecisionBars=int((~filt.known[indices]).sum()) if filt is not None else 0,
        filterUnknownAttempts=attempts['reason'].count('filterUnknown'), filterBlockedAttempts=attempts['reason'].count('filterBlocked'),
        conflictAttempts=attempts['reason'].count('entryConflict'),
        holdingMs=total_holding, holdingDistributionMs={k: float(np.quantile(holding, q)) if len(holding) else None
            for k, q in (('min', 0), ('median', .5), ('p90', .9), ('max', 1))},
        meanEntryLeverage=float(np.mean(trades['entryLeverage'])) if len(pnl) else None,
        maxEntryLeverage=max(trades['entryLeverage'], default=None),
        timeInMarketPct=100*total_holding/(r.window.endMs-r.window.startMs),
        occupiedBarSharePct=100*len(occupied)/len(indices) if len(indices) else None,
        returnPctPerExposureDay=100*(final/initial-1)/(total_holding/DAY) if total_holding else None,
        passive=passive, months=months, monthBreadth=breadth(months),
        curve=curve(months, r.window.startMs, r.window.endMs, initial) if len(indices) else [])
    result['robustness'] = robustness.account_facts(record,result)
    return result


def aggregate_metrics(records, summaries):
    """Streaming canonical roster reduction; one group path retained at a time."""
    total, times, observed = None, None, None
    initial = final = holding = 0.
    count, no_data = 0, []
    starts = ends = step = capital = None
    for record in records:
        r = record.result
        t, eq, seen = synchronized(record)
        if total is None:
            total, observed, times = np.zeros(len(t)), np.zeros(len(t), np.int64), t
            starts, ends, step, capital = r.window.startMs, r.window.endMs, record.prepared.stepMs, r.policy.initialCapital
        require(np.array_equal(times, t) and (starts, ends, step, capital) ==
                (r.window.startMs, r.window.endMs, record.prepared.stepMs, r.policy.initialCapital), 'incompatible aggregate')
        total += eq; observed += seen
        initial += capital; final += r.finalBalance; holding += sum(r.trades['holdingMs'].to_pylist())
        count += 1
        if not seen.any(): no_data.append(record.labels['instrumentId'])
    require(count == len(summaries) and count > 0, 'incomplete aggregate roster')
    labels = {k: summaries[0][k] for k in ('allocationId', 'windowId')}
    if no_data:
        return dict(**labels, status='unavailable', reason='noObservedBars', unavailableInstruments=no_data,
                    plannedAccounts=count, curve=[], months=[])
    net = final-initial
    months = monthly(times, total, observed, starts, ends, initial, final, account_count=count)
    for j, row in enumerate(months):
        row['observedAccounts'] = sum(s['months'][j]['observedBars'] > 0 for s in summaries)
        row['allAccountsObserved'] = row['observedAccounts'] == count
        for boundary in ('opening', 'closing'):
            row[boundary+'Exact'] = all(s['months'][j][boundary+'Exact'] for s in summaries)
            row[boundary+'Samples'] = {s['instrumentId']: s['months'][j][boundary+'SampleMs'] for s in summaries}
    mean_days = holding/count/DAY
    passive = [s['passive'] for s in summaries]
    return dict(**labels, status='available', plannedAccounts=count, initialCapital=initial, finalCapital=final,
        netPnl=net, returnPct=100*(final/initial-1), medianTickerReturnPct=float(np.median([s['returnPct'] for s in summaries])),
        maxDrawdownMtmPct=drawdown(total, initial), profitableTickers=sum(s['netPnl'] > 0 for s in summaries),
        availableTickers=count, trades=sum(s['trades'] for s in summaries), fees=sum(s['fees'] for s in summaries),
        meanExposureDays=mean_days, timeInMarketPct=100*holding/count/(ends-starts),
        returnPctPerExposureDay=100*(final/initial-1)/mean_days if mean_days else None,
        marketUnobservedAccountSlots=int((count-observed).sum()),
        topFiveTickerContributionPct=100*sum(sorted((max(0, s['netPnl']) for s in summaries), reverse=True)[:5])/net if net > 0 else None,
        largestPositiveMonthContributionPct=100*max([0]+[row['pnl'] for row in months])/net if net > 0 else None,
        concentrationStatus='available' if net > 0 else 'nonpositiveNetPnl',
        passiveLongReturnPct=sum(p['longReturnPct'] for p in passive)/count,
        passiveShortReturnPct=sum(p['shortReturnPct'] for p in passive)/count,
        passiveBoundariesDiffer=len({(p['firstOpenMs'], p['lastCloseMs']) for p in passive}) > 1,
        months=months, monthBreadth=breadth(months), curve=curve(months, starts, ends, initial))


def derive(reader):
    accounts = [account_metrics(reader.account(key)) for key in reader.accountKeys]
    accounts.sort(key=lambda s: (s['instrumentId'], s['allocationId'], s['windowId']))
    aggregate = []
    for allocation, window in sorted({(s['allocationId'], s['windowId']) for s in accounts}):
        summaries = [s for s in accounts if (s['allocationId'], s['windowId']) == (allocation, window)]
        keys = [reader.key_for(s['instrumentId'], allocation, window) for s in summaries]
        row = aggregate_metrics((reader.account(k) for k in keys), summaries)
        row['robustness'] = robustness.aggregate_facts(row,summaries)
        aggregate.append(row)
    version = 'pattern-lab.account-metrics.v3' if reader.study['schema'] == 'pattern-lab.account-study.v2' else VERSION
    result = dict(schema=version, units=dict(money='quote currency USDT', returns='percent', duration='milliseconds'),
        accounts=accounts, aggregates=aggregate, robustness=robustness.window_facts(reader.study['request']),
        derivation=robustness.attribution(reader,version),
        limitations=['Descriptive independent accounts, not shared margin or a validated edge.',
            'Intrabar exits use the bar open clock; exact intrabar exposure time is unknown.',
            'Passive price references exclude fees, funding, liquidation and account sizing.',
            'Continuous monthly contributions are not monthly restarts; overlapping windows are not independent.'])
    if version.endswith('.v3'):
        from ..auxiliary.study import summary_metadata
        result['auxiliary'] = summary_metadata(reader)
    return result
