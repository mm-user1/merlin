"""Exact monthly extraction; compact checks never claim full execution replay."""
from dataclasses import asdict
from decimal import Decimal, ROUND_CEILING
import json
import math
import numpy as np

from ..accounts.contracts import require, integer
from ..account_study.common import closed, digest_id
from ..account_study.metrics import synchronized, monthly, reconciles
from ..account_study.codec import preparation_facts
from ..configuration_study.metrics import extract_metrics
from ..configuration_study.validation import metric_row, finite_json
from ..configuration_study.request import result_id as metric_result_id
from ..study.contracts import semantic_digest
from ..study.bracket_rules import ExecutionRules
from data_provider.data import validate_fingerprint_header
from .request import calendar, result_id, month_boundary, child_request

VERSION = 'strategy-screen-facts.1'
MONTH_KEYS = ('month','startMs','endMs','openingEquity','closingEquity','openingBalance','closingBalance',
    'openingSampleMs','closingSampleMs','openingExact','closingExact','observedBars','expectedBars',
    'coverageComplete','diagnosticPnl','diagnosticContributionPp','closedTrades','terminalTradeCount',
    'terminalExitFees','minEquity','maxEquity','localDrawdownPct','localDrawdownStatus',
    'carriedPeakMaxDdPct','carriedPeakStatus','exact','reason','pnl','contributionPp','windowReturnPct',
    'returnStatus','cumulativeReturnPct','cumulativeStatus')


def near(a, b, message):
    require(reconciles(a, b), message)


def analytical_month(row, initial, prefix):
    full = row['expectedBars'] > 0 and row['observedBars'] == row['expectedBars']
    exact = full and row['openingExact'] and row['closingExact']
    reason = None if exact else 'noExpectedBars' if not row['expectedBars'] else (
        'incompleteCoverage' if not full else 'inexactBoundary')
    opening, closing = row['openingEquity'], row['closingEquity']
    prefix = prefix and exact
    return dict(coverageComplete=full, exact=exact, reason=reason,
        pnl=closing-opening if exact else None,
        contributionPp=100*(closing-opening)/initial if exact else None,
        windowReturnPct=100*(closing/opening-1) if exact and opening > 0 else None,
        returnStatus='available' if exact and opening > 0 else 'nonpositiveOpeningEquity' if exact else reason,
        cumulativeReturnPct=100*(closing/initial-1) if prefix else None,
        cumulativeStatus='available' if prefix else 'inexactPrefix')


def local_dd(values, opening):
    peak, worst, available = opening, 0., opening > 0
    for value in values:
        peak = max(peak, float(value))
        if peak > 0:
            available = True; worst = max(worst, 100*(peak-value)/peak)
    return float(worst) if available else None


def trade_month(exit_ms, months, end):
    if exit_ms == end: return len(months)-1
    for i, m in enumerate(months):
        if m['startMs'] <= exit_ms < m['endMs']: return i
    require(False, 'trade outside declared calendar')


def native_identities(execution, allocation, period):
    """Native semantic equations, without constructing proxy accounts or arrays."""
    preparation = dict(schema='pattern-lab.account.v1', preparationVersion='1', alignmentVersion='1',
        seed='segment-first-real-open-close;pine-rma;neutral-break;priorBars-mask',
        validity='next-scheduled-close-exclusive', targetId='target',
        inputs=sorted(execution['inputs'],key=lambda v:v['inputId']),
        requests=sorted(allocation['features'],key=lambda f:f['requestId']))
    require(semantic_digest(preparation)==execution['preparationId'],'native preparation identity')
    account = dict(schema='pattern-lab.account.v1', preparationId=execution['preparationId'],
        commandId=execution['commandId'], policy=allocation['policy'],
        window=dict(startMs=month_boundary(period['startUtc']),endMs=month_boundary(period['endUtc'])),
        rules=execution['rules'])
    require(semantic_digest(account)==execution['accountId'],'native account identity')
    # Same recorded-rules semantics as accounts.execution.validate_inputs. That
    # API also requires full prepared arrays/commands, unavailable in compact data.
    r=ExecutionRules(**execution['rules']);h=next(v['header'] for v in execution['inputs'] if v['inputId']=='target')
    require(r.venue==h['venue'] and r.contract==h['contract'] and
        r.quote_currency==r.settlement_currency==h['quote_currency']=='USDT','rules instrument')
    integer(r.minimum_lots,'minimum_lots',1)
    require(r.minimum_lots<=2**53 and r.enforce_minimum_quantity is True and
        type(r.enforce_minimum_notional) is bool and r.enforce_minimum_notional==(r.minimum_notional is not None),'rules limits/flags')
    for name in ('base_step','base_minimum','quantity_step','minimum_quantity'):
        value=getattr(r,name)
        require(isinstance(value,str) and math.isfinite(float(value)) and float(value)>0,'rules quantity')
    step,minimum=Decimal(r.base_step),Decimal(r.base_minimum)
    factor=Decimal(r.ct_val) if r.venue=='OKX' else Decimal(1)
    require((r.venue=='OKX' and r.ct_mult=='1' and factor>0 and r.quantity_unit=='contracts') or
        (r.venue=='BYBIT' and r.ct_val is None and r.ct_mult is None and r.quantity_unit==r.base_currency),'rules conversion')
    require(Decimal(r.quantity_step)*factor==step and Decimal(r.minimum_quantity)*factor==minimum and
        int((minimum/step).to_integral_value(rounding=ROUND_CEILING))==r.minimum_lots,'rules normalization')
    if r.minimum_notional is not None:
        require(isinstance(r.minimum_notional,str) and math.isfinite(float(r.minimum_notional)) and
            float(r.minimum_notional)>=0,'minimum notional')


def live_declarations(record, configuration, child):
    r=record.result;p=record.prepared;bundle=r.commands
    a=next(a for a in child['allocations'] if a['allocationId']==configuration['allocationId'])
    require(asdict(r.policy)==a['policy'] and p.targetId=='target' and
        p.target.header['instrument_id']==configuration['instrumentId'],'native policy/target declaration')
    require(preparation_facts(p)['requests']==sorted(a['features'],key=lambda f:f['requestId']),
        'native candle declarations')
    require(p.identity==r.preparationId==bundle.preparationId and bundle.identity==r.commandId,'native identity links')
    require((bundle.mode,bundle.stopUpdates)==(r.policy.mode,r.policy.stopUpdates),'native command policy')
    provenance=json.loads(bundle.provenance)
    if 'auxiliaryInputs' in child:
        from ..auxiliary.request import components
        from ..auxiliary.study import check_binding
        evidence=record.auxiliaryEvidence
        require(type(evidence) is dict,'native v2 evidence')
        meta=evidence['decisions']['metadata']
        require(meta['provider']==a['provider'],'native auxiliary provider declaration')
        require({k:v['metadata']['feature'] for k,v in meta['features'].items()}==
            {f['featureId']:f for f in a['auxiliaryFeatures']},'native auxiliary feature declarations')
        expected=sorted(components(a['entryFilter']),key=lambda c:c['componentId'])
        require([c['metadata']['component'] for c in meta['components']]==expected,'native auxiliary component declarations')
        require((bundle.entryFilter is not None)==bool(expected),'native auxiliary filter presence')
        bindings={b['inputId']:b for b in child['auxiliaryInputs']}
        deps=evidence['dependencies']
        require(set(deps)=={f['bindingId'] for f in a['auxiliaryFeatures']},'native dependency roster')
        for bid,dep in deps.items():
            require(dep['coveragePolicy']==bindings[bid]['coveragePolicy'],'native dependency coverage')
            check_binding(bindings[bid],configuration['instrumentId'],dep['content'])
    else:
        require(record.auxiliaryEvidence is None,'native v1 auxiliary evidence')
        require(provenance==dict(provider='haCommands',version='1',recipe=a['provider']['recipe']),
            'native HA recipe declaration')
        filt=a['entryFilter'];actual=bundle.entryFilter
        require((actual is not None)==(filt is not None),'native filter presence')
        if filt is not None:
            declared=next(f for f in a['features'] if f['requestId']==filt['requestId'])
            source=next(v for v in p.inputs if v.inputId==declared['inputId'])
            require(json.loads(actual.provenance)==dict(provider='candleDirection',version='1',request=declared,
                sourceHeader=source.header,inputFingerprint=source.inputFingerprint),'native filter declaration')


def extract(record, configuration, period, child):
    r = record.result; initial = r.policy.initialCapital
    require(record.labels == dict(instrumentId=configuration['instrumentId'],
        allocationId=configuration['allocationId'], windowId=period['periodId']), 'native labels')
    require((r.window.startMs, r.window.endMs) == tuple(month_boundary(period[k]) for k in ('startUtc','endUtc')),
            'native period bounds')
    live_declarations(record,configuration,child)
    metrics = extract_metrics(record)
    path, trades = r.path.to_pydict(), r.trades.to_pylist()
    gross = math.fsum(t['grossPnl'] for t in trades)
    fees = math.fsum(v for t in trades for v in (t['entryFee'], t['exitFee']))
    net = math.fsum(t['netPnl'] for t in trades)
    for actual, expected in ((gross,metrics['grossPnl']), (fees,metrics['fees']),
            (net,metrics['netPnl']), (gross-fees,net), (net,r.finalBalance-initial)):
        near(actual, expected, 'native trade cashflow reconciliation')
    times, equity, observed = synchronized(record)
    balance = np.full(len(times), initial, dtype=float)
    actual_closes = np.asarray(path['barOpenMs'], dtype=np.int64)+record.prepared.stepMs
    positions = np.searchsorted(times, actual_closes)
    for j, ix in enumerate(positions):
        balance[ix:positions[j+1] if j+1 < len(positions) else len(times)] = path['balance'][j]
    diagnostics = monthly(times,equity,observed,r.window.startMs,r.window.endMs,initial,r.finalBalance)
    months = []; prefix = True; peak = initial; carried = []
    for value in path['equity']:
        peak = max(peak, value); carried.append(100*(peak-value)/peak)
    def boundary(values, stamp):
        ix = np.searchsorted(times, stamp, side='right')-1
        return initial if stamp == r.window.startMs or ix < 0 else float(values[ix])
    for d in diagnostics:
        mask = (times > d['startMs']) & (times <= d['endMs'])
        real = (actual_closes > d['startMs']) & (actual_closes <= d['endMs'])
        values = np.r_[d['openingEquity'],equity[mask]]
        dd = local_dd(equity[mask], d['openingEquity'])
        carried_dd = max((v for v, keep in zip(carried,real) if keep), default=None)
        m = {k:d[k] for k in ('month','startMs','endMs','openingEquity','closingEquity',
            'openingSampleMs','closingSampleMs','openingExact','closingExact','observedBars','expectedBars')}
        m.update(openingBalance=boundary(balance,d['startMs']), closingBalance=boundary(balance,d['endMs']),
            diagnosticPnl=d['pnl'], diagnosticContributionPp=d['contributionPct'],closedTrades=0,
            terminalTradeCount=0,terminalExitFees=0., minEquity=float(values.min()), maxEquity=float(values.max()),
            localDrawdownPct=dd,localDrawdownStatus='available' if dd is not None else 'noPositivePeak',
            carriedPeakMaxDdPct=carried_dd,carriedPeakStatus='available' if carried_dd is not None else 'noObservedBars')
        m.update(analytical_month(m,initial,prefix)); prefix = prefix and m['exact']; months.append(m)
    terminal_fees = [[] for _ in months]
    for t in trades:
        i = trade_month(t['exitMs'], months, r.window.endMs); m = months[i]; m['closedTrades'] += 1
        if t['exitReason'] == 'terminal':
            m['terminalTradeCount'] += 1; terminal_fees[i].append(t['exitFee'])
    for m, values in zip(months,terminal_fees): m['terminalExitFees'] = math.fsum(values)
    dd = max([0.]+carried) if carried else None
    if dd is not None: near(dd,metrics['mtmMaxDdPct'],'carried-peak native DD')
    aux = []
    if record.auxiliaryEvidence:
        from ..auxiliary.model import fingerprint
        bindings = {b['inputId']:b for b in child['auxiliaryInputs']}
        for bid, dep in sorted(record.auxiliaryEvidence['dependencies'].items()):
            b = bindings[bid]
            aux.append(dict(inputId=bid,coveragePolicy=dep['coveragePolicy'],consumedSliceFingerprint=fingerprint(dep['content']),
                snapshotContentId=b['expectedSnapshotContentId'],source=b['sources'].get('*',b['sources'].get(configuration['instrumentId']))))
    unit = dict(factsVersion=VERSION,configurationId=configuration['configurationId'],
        resultId=result_id(configuration['configurationId'],period),periodId=period['periodId'],status='complete',
        metrics=metrics,months=months,execution=dict(kind='native',accountId=r.identity,preparationId=r.preparationId,
            commandId=r.commandId,validation='nativeValidated',inputs=preparation_facts(record.prepared)['inputs'],rules=asdict(r.rules),auxiliary=aux),
        terminal=dict(status=r.terminalStatus,finalFlat=not path['side'] or path['side'][-1]==0,finalBalance=r.finalBalance),
        audit=dict(version=VERSION,tradesChecked=len(trades),pathPoints=len(path['equity']),grossPnl=gross,netPnl=net,fees=fees,
            monthlyReconstructedDd=dd,terminalTrades=sum(m['terminalTradeCount'] for m in months),
            terminalExitFees=math.fsum(m['terminalExitFees'] for m in months)))
    validate(unit,configuration,period,child,synthetic=False)
    return unit


def validate(unit, config, period, child, *, synthetic):
    closed(unit,('factsVersion','configurationId','resultId','periodId','status','metrics','months','execution','terminal','audit'))
    finite_json(unit)
    require(unit['factsVersion']==VERSION and unit['status']=='complete' and unit['periodId']==period['periodId'] and
        unit['configurationId']==config['configurationId'] and unit['resultId']==result_id(config['configurationId'],period), 'screen unit identity/status')
    m = unit['metrics']; execution = unit['execution']
    # Reuse the canonical metric validator on a transient envelope, never store a
    # Configuration Study identity or schema in this distinct screen artifact.
    stage = dict(stageId=period['periodId'],startUtc=period['startUtc'],endUtc=period['endUtc'])
    e = dict(execution,auxiliary=[{k:a[k] for k in ('inputId','coveragePolicy','consumedSliceFingerprint')}
                                 for a in execution['auxiliary']])
    metric_row(dict(configurationId=config['configurationId'],resultId=metric_result_id(config['configurationId'],stage),
        stageId=stage['stageId'],status='complete',metrics=m,execution=e),config,stage,synthetic)
    allocation = next(a for a in child['allocations'] if a['allocationId']==config['allocationId'])
    require(m['initialCapital']==allocation['policy']['initialCapital'], 'declared capital')
    if not synthetic:
        used = {f['inputId'] for f in allocation['features']}
        sources = {'target':dict(instrumentId=config['instrumentId'],timeframeMinutes=config['timeframeMinutes'])}
        sources.update({s['inputId']:s for s in child['sources'] if s['inputId'] in used})
        require({v['inputId'] for v in e['inputs']}==sources.keys(), 'exact prepared alias set')
        origin = to_ms(child['preparationStartUtc'])
        for v in e['inputs']:
            s=sources[v['inputId']]; h=v['header']
            validate_fingerprint_header(h)
            target = config['instrumentId'] if s['instrumentId']=='target' else s['instrumentId']
            require(h['instrument_id']==target and h['timeframe_minutes']==s['timeframeMinutes'] and h['warmup_start_ms']==origin and
                h['start_ms']==month_boundary(period['startUtc']) and h['end_ms']==month_boundary(period['endUtc']),
                    'prepared source/warmup binding')
        native_identities(execution,allocation,period)
        bindings={b['inputId']:b for b in child.get('auxiliaryInputs',[])}
        used_aux={f['bindingId'] for f in allocation.get('auxiliaryFeatures',[])}
        require([a['inputId'] for a in execution['auxiliary']]==sorted(used_aux), 'auxiliary evidence roster')
        for a in execution['auxiliary']:
            closed(a,('inputId','coveragePolicy','consumedSliceFingerprint','snapshotContentId','source'))
            b=bindings[a['inputId']];digest_id(a['snapshotContentId'])
            require(a['snapshotContentId']==b['expectedSnapshotContentId'] and a['coveragePolicy']==b['coveragePolicy'] and
                a['source']==b['sources'].get('*',b['sources'].get(config['instrumentId'])), 'auxiliary pin/resolved binding')
    terminal=unit['terminal'];closed(terminal,('status','finalFlat','finalBalance'))
    require(terminal['status']==m['terminalStatus'] and terminal['finalFlat'] is True and terminal['finalBalance']==m['finalCapital'], 'terminal reconciliation')
    months=unit['months']; slots=calendar(period)
    require(type(months) is list and len(months)==len(slots),'complete monthly roster')
    initial=m['initialCapital']; prefix=True; previous=initial; previous_balance=initial;peak_bound=initial
    start=slots[0]['startMs'];step=config['timeframeMinutes']*60000
    end=slots[-1]['endMs'];coverage=m['coverage'];last=coverage['lastObservedCloseMs']
    if coverage['observedBars']:
        require(type(last) is int and start<last<=end and (last-start)%step==0 and
            coverage['missingTailSlots']==(end-last)//step,'last observation/grid/tail')
    else: require(last is None and m['exitReasons'].get('terminal',0)==0,'unobserved terminal')
    require(months[-1]['closingSampleMs']==last,'final observed sample')
    owner=trade_month(last,slots,end) if m['exitReasons'].get('terminal',0) else None
    latest=None
    for index,(row, slot) in enumerate(zip(months,slots)):
        closed(row,MONTH_KEYS)
        require(all(row[k]==v for k,v in slot.items()),'month coordinates')
        for k in ('observedBars','expectedBars','closedTrades','terminalTradeCount'):integer(row[k],k)
        require(row['expectedBars']==(slot['endMs']-start)//step-(slot['startMs']-start)//step and
            0<=row['observedBars']<=row['expectedBars'] and row['terminalTradeCount']<=row['closedTrades'],'month counts')
        for k in ('openingEquity','closingEquity','openingBalance','closingBalance','minEquity','maxEquity','terminalExitFees'):
            require(type(row[k]) in (int,float), 'monthly financial type')
        require(row['terminalExitFees']>=0 and (row['terminalTradeCount'] or row['terminalExitFees']==0), 'terminal fee count')
        if index!=owner:require(row['terminalTradeCount']==0 and row['terminalExitFees']==0,'terminal month ownership')
        require(row['openingSampleMs']==(start if index==0 else latest),'opening sample continuity')
        if row['observedBars']:
            require(last is not None and type(row['closingSampleMs']) is int and slot['startMs']<row['closingSampleMs']<=min(slot['endMs'],last) and
                (row['closingSampleMs']-start)%step==0,'observed month sample')
        else: require(row['closingSampleMs']==latest,'empty month sample carry')
        if last is not None and slot['startMs']<last<=slot['endMs']:
            require(row['observedBars']>0 and row['closingSampleMs']==last,'last observation equity month')
        latest=row['closingSampleMs']
        require(row['openingEquity']==previous and row['openingBalance']==previous_balance,'monthly diagnostic continuity')
        require(row['minEquity']<=min(row['openingEquity'],row['closingEquity'])<=max(row['openingEquity'],row['closingEquity'])<=row['maxEquity'], 'monthly extrema')
        peak_bound=max(peak_bound,row['maxEquity'])
        for boundary in ('opening','closing'):
            stamp=row[boundary+'SampleMs'];bound=slot['startMs' if boundary=='opening' else 'endMs']
            require(stamp is None or type(stamp) is int and start<=stamp<=bound,'sample bounds')
            require(type(row[boundary+'Exact']) is bool and row[boundary+'Exact']==(stamp==bound),'sample exactness')
        require(row['diagnosticPnl']==row['closingEquity']-row['openingEquity'] and
            row['diagnosticContributionPp']==100*row['diagnosticPnl']/initial,'diagnostic arithmetic')
        derived=analytical_month(row,initial,prefix)
        require(all(row[k]==v for k,v in derived.items()),'monthly analytical availability/arithmetic')
        prefix=prefix and row['exact']; previous=row['closingEquity']; previous_balance=row['closingBalance']
        dd=row['carriedPeakMaxDdPct']
        require((dd is not None)==(row['observedBars']>0) and row['carriedPeakStatus']==('available' if dd is not None else 'noObservedBars'), 'carried DD availability')
        require(dd is None or type(dd) in (int,float) and dd>=0, 'carried DD bounds')
        if dd is not None:
            if row['closingExact']:
                floor=max(0,100*(peak_bound-row['closingEquity'])/peak_bound)
                require(dd>=floor or reconciles(dd,floor),'carried DD below observed endpoint')
            bound=100*(1-row['minEquity']/(peak_bound if row['minEquity']>=0 else initial))
            require(dd<=max(0,bound) or reconciles(dd,max(0,bound)), 'carried DD exceeds retained extrema bound')
        local=row['localDrawdownPct']
        require((local is not None)==(row['maxEquity']>0) and row['localDrawdownStatus']==('available' if local is not None else 'noPositivePeak'), 'local DD availability')
        require(local is None or type(local) in (int,float) and local>=0,'local DD bounds')
    require(previous==previous_balance==m['finalCapital'],'monthly final capital')
    require(sum(x['observedBars'] for x in months)==m['coverage']['observedBars'] and
        sum(x['expectedBars'] for x in months)==m['coverage']['expectedBars'],'monthly coverage totals')
    require(sum(x['closedTrades'] for x in months)==m['trades'],'monthly trade totals')
    near(math.fsum(x['diagnosticPnl'] for x in months),m['netPnl'],'monthly diagnostic sum')
    if prefix:near(math.fsum(x['contributionPp'] for x in months),m['returnPct'],'exact monthly contribution sum')
    a=unit['audit'];closed(a,('version','tradesChecked','pathPoints','grossPnl','netPnl','fees','monthlyReconstructedDd','terminalTrades','terminalExitFees'))
    for key in ('tradesChecked','pathPoints','terminalTrades'):integer(a[key],key)
    require(a['version']==VERSION and a['tradesChecked']==m['trades'] and a['pathPoints']==m['coverage']['observedBars'],'extraction counts/version')
    for k in ('grossPnl','netPnl','fees'):near(a[k],m[k],'extraction cashflow')
    near(a['grossPnl']-a['fees'],a['netPnl'],'gross less fees')
    require(a['terminalTrades']==sum(x['terminalTradeCount'] for x in months)==m['exitReasons'].get('terminal',0), 'terminal count ownership')
    require(a['terminalExitFees']==math.fsum(x['terminalExitFees'] for x in months),'terminal fee ownership')
    dd=max([0.]+[x['carriedPeakMaxDdPct'] for x in months if x['carriedPeakMaxDdPct'] is not None]) if a['pathPoints'] else None
    require(dd==a['monthlyReconstructedDd'],'monthly DD scalar maximum')
    if dd is None:require(m['mtmMaxDdPct'] is None,'unobserved DD')
    else:near(dd,m['mtmMaxDdPct'],'native carried DD reconciliation')


def to_ms(value):
    from data_provider.manifest import to_epoch_ms
    return to_epoch_ms(value,'preparationStartUtc')
