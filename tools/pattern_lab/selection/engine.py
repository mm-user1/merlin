"""Selection admission, scoring contracts and semantic consistency checks."""
from pathlib import Path
import hashlib
import importlib.metadata
import math
import sys

import numpy as np

from ..accounts.contracts import require, integer, identifier
from ..account_study.common import closed, json_text, parse_json, digest_id, member_names
from ..study.contracts import semantic_digest
from .request import month_bounds, normalize_selection_request
from .ledger import history_pool, score_view, ScoreView, money, REJECTIONS
from .scorers import registry, contract, evaluate, Score, validate_scores


def source_hashes(scorers):
    result={}
    for name,s in sorted(scorers.items()):
        if not name.startswith('x.'): continue
        member_names(s.sourceFiles)
        result[name]={logical:hashlib.sha256(Path(path).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
            for logical,path in sorted(s.sourceFiles.items())}
    return result


def implementation(experimental):
    root=Path(__file__).resolve().parents[1]
    files=[]
    for folder in ('selection','account_study','accounts','auxiliary','study'):
        files.extend(p for p in (root/folder).iterdir() if p.suffix in ('.py','.js'))
    files.extend(root/name for name in ('data.py','manifest.py'))
    return dict(algorithm='fixed-reference.1',sources={p.relative_to(root).as_posix():
        hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest() for p in sorted(files)},
        experimental=experimental,python=sys.version.split()[0],
        libraries={n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow','numba')})


def build_scores(parent,doc,ledger,roster,registered, *, stored=None):
    batches=[];months=[w['month'] for w in doc['monthWindows']]
    saved={(b['selectorId'],b['month']):b for b in stored or []}
    for selector in doc['selectors']:
        sid=selector['selectorId'];s=registered.get(selector['scorerId'])
        for month in doc['evaluationMonths']:
            index=months.index(month);history=months[index-doc['pool']['historyMonths']:index]
            eligible,exclusions=history_pool(ledger,roster,history,doc['pool']['minObservedBarsPct'])
            declared=contract(s,selector['parameters']) if s else saved[sid,month]['contract']
            require(declared['lookbackMonths'] <= doc['pool']['historyMonths'], 'scorer history contract')
            seen=months[index-declared['lookbackMonths']:index];cutoff=month_bounds(month)[0]
            view,dependencies=score_view(parent,doc,ledger,eligible,seen,cutoff,declared['needsBars'])
            rows=evaluate(s,view,selector['parameters']) if s else saved[sid,month]['rows']
            batches.append(dict(selectorId=sid,month=month,cutoffMs=cutoff,historyMonths=seen,
                contract=declared,commonPool=eligible,commonExclusions=exclusions,barDependencies=dependencies,rows=rows))
    return batches


def validate_scores_saved(doc,ledger,roster,batches):
    require(type(batches) is list, 'score table')
    expected=[(s['selectorId'],m) for s in doc['selectors'] for m in doc['evaluationMonths']]
    require([(b.get('selectorId'),b.get('month')) for b in batches] == expected, 'score batch keys/order')
    selectors={s['selectorId']:s for s in doc['selectors']};months=[w['month'] for w in doc['monthWindows']]
    registered=registry();contracts={}
    for b in batches:
        closed(b,('selectorId','month','cutoffMs','historyMonths','contract','commonPool','commonExclusions','barDependencies','rows'))
        c=b['contract'];closed(c,('lookbackMonths','keyNames','needsBars'))
        integer(c['lookbackMonths'],'lookback',1);require(c['lookbackMonths']<=doc['pool']['historyMonths'],'saved lookback')
        require(type(c['needsBars']) is bool and type(c['keyNames']) is list and c['keyNames'] and len(set(c['keyNames']))==len(c['keyNames']),'saved contract')
        for name in c['keyNames']:identifier(name)
        signature=json_text(c)
        require(contracts.setdefault(b['selectorId'],signature)==signature,
            'saved contract changes across months for selector '+b['selectorId'])
        index=months.index(b['month']);history=months[index-doc['pool']['historyMonths']:index]
        eligible,excluded=history_pool(ledger,roster,history,doc['pool']['minObservedBarsPct'])
        seen=months[index-c['lookbackMonths']:index];cutoff=month_bounds(b['month'])[0]
        require((b['commonPool'],b['commonExclusions'],b['historyMonths'],b['cutoffMs'])==(eligible,excluded,seen,cutoff),'score history/pool/clock')
        values=[]
        for row in b['rows']:
            closed(row,('instrumentId','eligible','key','reason','components'))
            require(row['key'] is None or type(row['key']) is list,'saved key')
            values.append(Score(row['instrumentId'],row['eligible'],tuple(row['key']) if row['key'] is not None else None,row['reason'],row['components']))
        require(validate_scores(values,eligible,c['keyNames'])==b['rows'],'score rows/order')
        require(type(b['barDependencies']) is list,'bar dependencies')
        require([d.get('instrumentId') for d in b['barDependencies']]==(eligible if c['needsBars'] else []),'bar dependency roster')
        for d in b['barDependencies']:
            closed(d,('instrumentId','startMs','endMs','stepMs','rows','timestampsSha256','valuesSha256'))
            require((d['startMs'],d['endMs'])==(month_bounds(seen[0])[0],cutoff),'bar slice bounds')
            integer(d['stepMs'],'step',1);integer(d['rows'],'bar rows')
            digest_id(d['timestampsSha256']);digest_id(d['valuesSha256'])
        selector=selectors[b['selectorId']]
        if not selector['scorerId'].startswith('x.'):
            scorer=registered[selector['scorerId']]
            require(c==contract(scorer,selector['parameters']),'built-in contract')
            view,_=score_view(None,doc,ledger,eligible,seen,cutoff,False)
            require(b['rows']==evaluate(scorer,view,selector['parameters']),'built-in score recomputation differs')


LEDGER_KEYS=('allocationId','instrumentId','month','windowId','accountKey','accountId','startMs','endMs','stepMs',
    'initialCapital','finalBalance','netPnl','returnPct','trades','fees','forcedExits','sizingRejections','sizingRejectionCount',
    'terminalStatus','available','reason','requestedStartMs','requestedEndMs','firstObservedOpenMs','lastObservedCloseMs',
    'expectedBars','observedBars','missingBars','missingHeadSlots','missingTailSlots','missingInteriorSlots')


def validate_ledger(doc,roster,ledger,paths):
    require(type(roster) is list and roster and roster==sorted(set(roster)),'selection roster')
    for ticker in roster:identifier(ticker)
    require(type(ledger) is list,'ledger table')
    expected=[(w['month'],i) for w in doc['monthWindows'] for i in roster]
    require([(r.get('month'),r.get('instrumentId')) for r in ledger]==expected,'ledger key/order')
    windows={w['month']:w['windowId'] for w in doc['monthWindows']}
    require(set(paths)==set(windows),'path month set')
    steps=set()
    for row in ledger:
        closed(row,LEDGER_KEYS);start,end=month_bounds(row['month'])
        require(row['allocationId']==doc['allocationId'] and row['windowId']==windows[row['month']],'ledger allocation/window')
        require((row['startMs'],row['endMs'],row['requestedStartMs'],row['requestedEndMs'])==(start,end,start,end),'ledger bounds')
        digest_id(row['accountId']);digest_id(row['accountKey'])
        require(row['accountKey']==semantic_digest(dict(instrumentId=row['instrumentId'],allocationId=row['allocationId'],windowId=row['windowId'])),'account key')
        step=integer(row['stepMs'],'step',1);steps.add(step)
        require((end-start)%step==0 and row['expectedBars']==(end-start)//step,'expected slots')
        for key in ('observedBars','expectedBars','missingBars','missingHeadSlots','missingTailSlots','trades','sizingRejectionCount'):
            integer(row[key],key)
        require(row['observedBars']+row['missingBars']==row['expectedBars'],'coverage counts')
        interior=row['missingBars']-row['missingHeadSlots']-row['missingTailSlots']
        require(row['missingInteriorSlots']==(interior if interior>=0 else None),'interior slots')
        observed=row['observedBars']>0
        require(type(row['available']) is bool and row['available']==observed and
            row['terminalStatus']==('closed' if observed else 'noObservedBars') and row['reason']==(None if observed else 'noObservedBars'),'ledger availability')
        if observed:
            require(row['firstObservedOpenMs']==start+row['missingHeadSlots']*step and
                row['lastObservedCloseMs']==end-row['missingTailSlots']*step and interior>=0,'coverage edge facts')
        else: require(row['firstObservedOpenMs'] is None and row['lastObservedCloseMs'] is None,'empty coverage timestamps')
        require(row['initialCapital']==doc['referenceCapital'],'ledger capital')
        require(all(type(row[k]) in (int,float) and math.isfinite(row[k]) for k in ('initialCapital','finalBalance','netPnl','fees')),'finite ledger amounts')
        net,ret=money(row['finalBalance'],row['initialCapital'])
        require(row['netPnl']==net and row['returnPct']==(ret if observed else None) and row['fees']>=0,'ledger money')
        closed(row['sizingRejections'],REJECTIONS)
        for v in row['sizingRejections'].values():integer(v,'rejection count')
        require(row['sizingRejectionCount']==sum(row['sizingRejections'].values()),'rejection total')
        closed(row['forcedExits'],('gapBoundary','terminal'))
        for v in row['forcedExits'].values():
            closed(v,('count','netPnl'));integer(v['count'],'forced exits')
            require(type(v['netPnl']) in (int,float) and math.isfinite(v['netPnl']),'forced PnL')
    require(len(steps)==1,'inconsistent monthly timeframe')
    step=next(iter(steps))
    for month,path in paths.items():
        start,end=month_bounds(month);times=path['times'];pnl=path['pnl']
        require(times.dtype==np.dtype('int64') and np.array_equal(times,np.arange(start+step,end+1,step,dtype=np.int64)),'path time grid')
        require(pnl.dtype==np.dtype('float64') and pnl.shape==(len(roster),len(times)) and np.isfinite(pnl).all(),'monthly path shape/values')
        for j,ticker in enumerate(roster):
            row=ledger[expected.index((month,ticker))]
            require(math.isclose(float(pnl[j,-1]),row['netPnl'],rel_tol=1e-9,abs_tol=1e-8),'native path final balance')
            if not row['available']:require(np.all(pnl[j]==0),'unobserved path must remain unknown flat reference')


def path_identity(paths):
    return semantic_digest({m:{k:dict(dtype=str(a.dtype),shape=list(a.shape),sha256=hashlib.sha256(a.tobytes()).hexdigest())
        for k,a in sorted(p.items())} for m,p in sorted(paths.items())})


def data_identity(doc,ledger,paths,scores,allocation):
    return semantic_digest(dict(parent=doc['parent'],allocation=allocation,ledger=ledger,paths=path_identity(paths),
        nativeBarDependencies=[dict(selectorId=b['selectorId'],month=b['month'],dependencies=b['barDependencies']) for b in scores]))
