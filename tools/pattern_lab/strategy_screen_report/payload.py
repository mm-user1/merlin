"""Public-reader projection. Saved financial facts stay authoritative."""
from datetime import datetime
import math

from ..accounts.contracts import require
from .model import VIEW, canonical, finite, histogram, histogram_scale, quantiles, scalars, instrument_labels, timeframe_label, period_shorts

METRICS = ('returnPct', 'annualizedProfitPct', 'mtmMaxDdPct', 'trades', 'tradesPerMonth')
COLORS = {**{k: [0, 40, 60, 100] for k in ('retention', 'interesting', 'profitable', 'positiveWithoutBest2')},
          'annualizedProfitPct': [-20, 0, 0, 20], 'mtmMaxDdPct': [10, 60], 'tradesPerMonth': [0, 10, 20, 40]}


def eligible(result, metric):
    """Mirror the documented native per-field predicate, independently of classification."""
    m = result['metrics']
    return (result['status'] == 'complete' and finite(m.get(metric))
            and (metric == 'trades' or m['performanceStatus'] == 'available')
            and (metric != 'annualizedProfitPct' or m['annualizedProfitStatus'] == 'available'))


def fraction(saved, *, equal=False):
    return {**saved, 'denominator': saved['declared'], 'pct': saved['equalCellPct'] if equal else saved['pct']}


def residual(saved):
    return {**saved, 'medianResidualPp': quantiles(saved['residualPp']),
            'positive': dict(success=saved['positive'], denominator=saved['declared'],
                             pct=saved['equalCellPositivePct'], pooledPct=saved['pooledPositivePct'],
                             available=saved['available'], missing=saved['missing'],
                             status=saved['status'], reason=saved['reason'])}


def window(saved):
    return {**saved, 'available': saved['returnPct']['n'], 'medianReturnPct': quantiles(saved['returnPct']),
            'medianContributionPp': quantiles(saved['contributionPp']), 'medianTrades': quantiles(saved['trades']),
            'profitable': fraction(saved['profitable'])}


def setting_period(result, calendar):
    require([m['month'] for m in result['months']] == [m['month'] for m in calendar], 'result calendar')
    require(all((m['startMs'], m['endMs']) == (s['startMs'], s['endMs'])
                for m, s in zip(result['months'], calendar)), 'result calendar boundaries')
    m = result['metrics']; coverage = m['coverage']
    availability = {k: dict(available=eligible(result, k), reason=None if eligible(result, k) else
                    'incompleteExecution' if result['status'] != 'complete' else
                    m['annualizedProfitStatus'] if k == 'annualizedProfitPct' and m['annualizedProfitStatus'] != 'available' else
                    m['performanceStatus'] if k != 'trades' and m['performanceStatus'] != 'available' else 'noAvailableValue') for k in METRICS}
    monthly = result['months']
    contribution = [x['contributionPp'] if x['exact'] and finite(x['contributionPp']) else None for x in monthly]
    cumulative = [x['cumulativeReturnPct'] if x['cumulativeStatus'] == 'available' and finite(x['cumulativeReturnPct']) else None for x in monthly]
    return {**{k: m[k] if availability[k]['available'] else None for k in METRICS},
            'resultId': result['resultId'], 'status': result['status'], 'availability': availability,
            'coverageComplete': coverage['expectedBars'] > 0 and coverage['observedBars'] == coverage['expectedBars'],
            'classification': {**result['classification'], 'valid': result['classification']['classifiable']},
            'effectiveCriteria': result['analytics']['effectiveCriteria'],
            'contributionPp': contribution, 'cumulativeReturnPct': cumulative,
            'monthStatus': [dict(exact=x['exact'], reason=x['reason'], cumulativeStatus=x['cumulativeStatus']) for x in monthly]}


def build_view(reader, options):
    """Read every result and summary once; retain only display values after each read."""
    metadata = reader.metadata; doc = metadata['request']; configurations = reader.configurations
    require(configurations == metadata['configurations'], 'reader configuration mismatch')
    require(len({c['configurationId'] for c in configurations}) == len(configurations), 'duplicate configuration')
    native_scopes = reader.scopes
    summaries = {s['scopeId']: reader.summary(s['scopeId']) for s in native_scopes}
    require('all' in summaries and len(summaries) == len(native_scopes), 'scope roster')
    calendars = {p: summaries['all']['periods'][p]['monthly'] for p in ('earlier', 'recent')}
    cells = {c['cellId']: dict(ticker=c['instrumentId'], instrumentId=c['instrumentId'], tf=c['timeframeMinutes']) for c in configurations}
    labels = instrument_labels({c['ticker'] for c in cells.values()})
    short_names = period_shorts(calendars)
    cell_scope = {s['key']: s['scopeId'] for s in native_scopes if s['kind'] == 'cell'}
    # UI keys are native scope IDs, including cell scopes; the canonical cell ID is retained too.
    cellmeta = {cell_scope[k]: {**v, 'cellId': k} for k, v in cells.items()}
    allocations = {a['allocationId']: a for a in doc['allocations']}
    recipes = {}
    for aid, recipe in allocations.items():
        leaves = dict(scalars(recipe)); parts = []
        for label in options['parameterLabels']:
            p = label['path']; value = canonical(leaves[p]) if p in leaves else 'Not applicable'
            parts.append(f"{label.get('shortLabel', label['label'])}: {value}" + (f" {label['unit']}" if 'unit' in label else ''))
        recipes[aid] = dict(recipe=recipe, label=' · '.join(parts) if parts else aid)
    rows = {(r['configurationId'], r['periodId']): r for r in reader.result_rows}
    require(len(rows) == 2*len(configurations), 'result roster count')
    settings = []
    for config in configurations:
        periods = {}
        for pid in ('earlier', 'recent'):
            result = reader.result(config['configurationId'], pid)
            row = rows[(config['configurationId'], pid)]
            require(all(result[k] == row[k] for k in ('configurationId', 'periodId', 'resultId', 'status')), 'result identity')
            periods[pid] = setting_period(result, calendars[pid])
        settings.append(dict(id=config['configurationId'], configurationId=config['configurationId'],
            allocationId=config['allocationId'], cell=cell_scope[config['cellId']], cellId=config['cellId'],
            label=recipes[config['allocationId']]['label'], periods=periods))
    global_values = {k: [s['periods'][p][k] for s in settings for p in ('earlier', 'recent')
                         if s['periods'][p]['coverageComplete'] and finite(s['periods'][p][k])]
                     for k in ('returnPct', 'annualizedProfitPct')}
    scales = {k: histogram_scale(v) for k, v in global_values.items()}
    contributions = [v for s in settings for p in s['periods'].values() for v in p['contributionPp'] if finite(v)]
    span = max(contributions)-min(contributions) if contributions else 0
    require(finite(span), 'monthly histogram range')
    monthly_scale = histogram_scale(contributions, max(1, math.ceil(span/16)))
    scopes = {}; detail_monthly = {}
    for native in native_scopes:
        sid = native['scopeId']; summary = summaries[sid]
        require(summary['scopeId'] == sid and summary['cells'] == native['cells'], 'summary identity')
        members = set(native['cells']); chosen = [s for s in settings if s['cellId'] in members]
        periods = {}; detail_monthly[sid] = {}
        for pid in ('earlier', 'recent'):
            saved = summary['periods'][pid]; values = [s['periods'][pid] for s in chosen]
            require(saved['declared'] == len(chosen), 'scope declared population')
            distributions = {k: histogram([v[k] for v in values if v['coverageComplete'] and finite(v[k])], saved['metrics'][k], scales[k])
                             for k in scales}
            require(len(saved['monthly']) == len(calendars[pid]), 'summary calendar length')
            monthly = []; details = []
            for i, month in enumerate(saved['monthly']):
                require(month['month'] == calendars[pid][i]['month'], 'summary calendar')
                monthly.append({**{k: month[k] for k in ('month', 'startMs', 'endMs')}, **quantiles(month['contribution']),
                                'positive': fraction(month['positiveContribution']), 'mean': month['equalCellMeanContributionPp'],
                                'meanReason': month['equalCellMeanReason']})
                details.append(dict(month=month['month'], startMs=month['startMs'], endMs=month['endMs'],
                    cumulative=quantiles(month['cumulative']), distribution=histogram(
                        [v['contributionPp'][i] for v in values], month['contribution'], monthly_scale)))
            detail_monthly[sid][pid] = details
            periods[pid] = dict(declared=saved['declared'], valid=saved['classifiable'], incomplete=saved['incompleteCoverage'],
                lowActivity=saved['activity']['low'], activity=saved['activity'],
                profitable=fraction(saved['breadth']['profitable'], equal=True), interesting=fraction(saved['breadth']['interesting'], equal=True),
                medians={k: quantiles(v) for k, v in saved['metrics'].items()}, distributions=distributions,
                concentration={kind: {k: residual(v) for k, v in saved['concentration'][kind].items()} for kind in ('own', 'common')},
                failures={**saved['failures'], 'return': saved['failures']['annualizedReturn']}, monthly=monthly,
                tailMonths=saved['latestThreeMonths']['months'], tail=window(saved['latestThreeMonths']),
                segments={k: window(v) for k, v in saved['segments'].items()}, newOnly=window(saved['newOnly']))
        persistence = summary['persistence'].copy()
        for k in ('profitable', 'interesting'):
            persistence[k+'EarlierSuccess'] = fraction(persistence[k]['earlierSuccess'])
            persistence[k+'EarlierNotSuccess'] = fraction(persistence[k]['earlierNonsuccess'])
        changes = {k: v['value'] if isinstance(v, dict) else v for k, v in summary['changes'].items()}
        for k in ('profitable', 'interesting'):
            changes[k] = changes[k+'BreadthPp']
        a, b = (periods[p]['concentration']['own']['2']['equalCellPositivePct'] for p in ('earlier', 'recent'))
        changes['positiveWithoutBest2'] = b-a if finite(a) and finite(b) else None
        label = ('All configurations' if native['kind'] == 'panel' else
                 f"{labels[cells[native['key']]['ticker']]} · {timeframe_label(cells[native['key']]['tf'])}" if native['kind'] == 'cell' else
                 timeframe_label(native['key']) if native['kind'] == 'timeframe' else labels[native['key']])
        scopes[sid] = dict(id=sid, scopeId=sid, kind=native['kind'], label=label,
                          cells=[cell_scope[c] for c in native['cells']], periods=periods, changes=changes, persistence=persistence)
    periods = []
    for p in doc['periods']:
        pid = p['periodId']; start, end = (p[k][:10] for k in ('startUtc', 'endUtc'))
        label = options['periodLabels'].get(pid, f'{start} – {end}')
        months = calendars[pid]
        periods.append(dict(id=pid, label=label, short=short_names[pid], start=p['startUtc'], end=p['endUtc'], months=len(months),
            days=(datetime.fromisoformat(end)-datetime.fromisoformat(start)).days,
            tail=' – '.join(m['month'] for m in months[-3:]) if len(months) >= 3 else 'N/A: fewer than three months'))
    criteria = doc['criteria']; minima = {str(c['tf']): criteria['minClosedTradesByTimeframe'].get(str(c['tf']), criteria['minClosedTrades']) for c in cells.values()}
    subtitle = options.get('subtitle', 'Independent earlier and recent accounts · descriptive research')
    if metadata['synthetic']:
        subtitle += ' · Synthetic compact fixture; no native execution'
    view = dict(schema=VIEW, artifactIdentity=metadata['artifactIdentity'], synthetic=metadata['synthetic'],
        meta=dict(title=options.get('title', doc['studyId']), subtitle=subtitle, periods=periods,
            tickers=sorted({c['ticker'] for c in cells.values()}), timeframes=sorted({c['tf'] for c in cells.values()}),
            scopeIds=dict(instrument={s['key']: s['scopeId'] for s in native_scopes if s['kind']=='instrument'},
                          timeframe={str(s['key']): s['scopeId'] for s in native_scopes if s['kind']=='timeframe'}),
            criteria={**criteria, 'trades': next(iter(minima.values())) if len(set(minima.values()))==1 else None},
            tradeMinima=minima, activityLabel=f'Below {next(iter(minima.values()))} trades' if len(set(minima.values()))==1 else 'Below trade minimum',
            segments=[dict(**s, key=s['segmentId'], label=s['segmentId']) for s in doc['segments']],
            previousReviewEndUtc=doc['previousReviewEndUtc'], overviewColors=COLORS, recipes=recipes,
            nativeSchema=metadata['schema'], nativeImplementation=metadata['implementation'], limitation=metadata['limitation']),
        cellmeta=cellmeta, scopes=scopes, detail=dict(settings=settings, monthly=detail_monthly))
    # Also rejects nonfinite metadata/scalars. Serialization is performed again by the publisher with its byte bound.
    canonical(view)
    return view
