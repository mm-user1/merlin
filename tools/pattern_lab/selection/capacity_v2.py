"""Conservative uncompressed planning before any funded execution or R-sized draw."""
from ..accounts.contracts import require
from ..account_study.common import json_text
from .carried import schedules
from .request import month_bounds


def auxiliary_plan(doc,contracts,ledger,roster,snapshots,parent_bounds,active):
    """Cheap envelope/dimension admission, before slicing; not a hard RAM limit."""
    from .auxiliary import envelope
    from .ledger import history_pool
    from ..auxiliary.study import source_for
    from ..auxiliary.model import INTERVALS
    from ..account_study.common import read_json
    months=[w['month'] for w in doc['monthWindows']];bindings={b['inputId']:b for b in doc['auxiliaryInputs']}
    cells=largest_rows=largest_cells=0
    for sid in sorted(active):
        contract=contracts[sid]
        require(contract['lookbackMonths']<=doc['pool']['historyMonths'],'scorer history contract')
        for month in doc['evaluationMonths']:
            index=months.index(month);history=months[index-contract['lookbackMonths']:index]
            pool,_=history_pool(ledger,roster,months[index-doc['pool']['historyMonths']:index],doc['pool']['minObservedBarsPct'])
            batch_rows=batch_cells=0
            for ticker in pool:
                for use in contract['auxiliaryUses']:
                    b=bindings[use['bindingId']];source=source_for(b,ticker)
                    start,end=envelope(use,source,history,month_bounds(month)[0],parent_bounds)
                    batch_cells+=1
                    if use['access']=='raw':
                        if source['interval'] is not None:
                            step=INTERVALS[source['interval']];rows=(end-start+step-1)//step
                        else:
                            # Sealed metadata only, never rows or a whole-snapshot series read.
                            rows=read_json(snapshots[b['snapshotId']].root/'series'/source['seriesId']/'series.json')['inspection']['records']
                        batch_rows+=rows
            cells+=batch_cells;largest_rows=max(largest_rows,batch_rows);largest_cells=max(largest_cells,batch_cells)
    # Necessary lower bounds, not the final eligible-pool capacity assessment.
    # Even empty diagnostics require these bytes. Shadow vectors are always retained.
    score_min=cells*128
    shadow=sum((len(roster)+1)*(month_bounds(w['month'])[1]-month_bounds(w['month'])[0])//ledger[0]['stepMs']*8 for w in doc['monthWindows'])
    require(score_min<=16*1024*1024 and shadow<=1024*1024*1024,'early selection dimensions exceed artifact bounds')
    return dict(requiredCells=cells,largestBatchCells=largest_cells,rawObservationsUpperEstimate=largest_rows,
        serializedBytesEstimate=largest_rows*192+largest_cells*4096,
        liveBatchBytesEstimate=largest_rows*(512+192+512)+largest_cells*8192,
        explanation='One owned raw batch, serialized cells and decoded/frozen copies; excludes snapshot admission, price views and scorer allocations. Estimate, not a hard RAM guarantee.')


def assessment(doc,ledger,roster,scores):
    plan=schedules(doc,ledger,scores);stopped=set();jobs=funded=retained=path_bytes=portfolio_rows=0
    r=doc['randomControl']['replicates'];step=ledger[0]['stepMs'];carried=doc['capitalMode']=='carried'
    for month in doc['evaluationMonths']:
        bars=(month_bounds(month)[1]-month_bounds(month)[0])//step
        for s in doc['selectors']:
            sid=s['selectorId'];p=plan[sid,month];n=len(p['pool'])
            portfolio_rows+=len(doc['kValues'])*(r+2)
            if p['failures']:stopped.add(sid)
            if carried and sid in stopped:continue
            if not carried:continue
            jobs+=n
            count=sum(min(k,n)*(r+1)+n for k in doc['kValues'])
            keep=sum(min(k,n)+n for k in doc['kValues'])
            funded+=count;retained+=keep;path_bytes+=(keep+1)*bars*8+keep*1024
    identifier_cost=sum(len(json_text(i).encode('utf-8'))+1 for i in roster)
    scalar_bytes=funded*4096+portfolio_rows*(2048+identifier_cost+96*len(roster))
    shadow_bytes=sum((len(roster)+1)*(month_bounds(w['month'])[1]-month_bounds(w['month'])[0])//step*8 for w in doc['monthWindows'])
    json_bytes=512*1024+len(scores)*(8192+len(roster)*2048)+len(doc['selectors'])*len(doc['kValues'])*(len(doc['evaluationMonths'])*8192+r*80)
    # Includes a conservative second copy for summary/HTML, not compressed ratios.
    total=shadow_bytes+path_bytes+scalar_bytes+4*json_bytes
    # Each monthly partition can add a partial tail. Every retained execution
    # can occupy its own NPZ in the worst case; never assume market compression.
    parts=(funded+9999)//10000+(portfolio_rows+9999)//10000+retained+2*len(doc['evaluationMonths'])+len(doc['monthWindows'])+16
    return dict(policy='selection-capacity.2',replicates=r,accountJobsUpperBound=jobs,fundedRecordsUpperBound=funded,
        retainedExecutionsUpperBound=retained,portfolioRows=portfolio_rows,expandedRetainedPathBytes=path_bytes,
        shadowPathBytes=shadow_bytes,scalarBudgetBytes=scalar_bytes,jsonBudgetBytes=json_bytes,totalBudgetBytes=total,
        partCountUpperBound=parts,supported=json_bytes<=16*1024*1024 and total<=1024*1024*1024 and parts<=10000)


def admit(doc,ledger,roster,scores):
    value=assessment(doc,ledger,roster,scores)
    require(value['supported'],'v2 selection capacity budget exceeded: '+json_text(value))
    require(len(json_text(scores).encode('utf-8'))+2<=16*1024*1024,'v2 scores JSON size bound before funded work')
    return value
