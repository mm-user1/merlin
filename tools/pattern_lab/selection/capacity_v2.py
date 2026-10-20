"""Conservative uncompressed planning before any funded execution or R-sized draw."""
from ..accounts.contracts import require
from ..account_study.common import json_text
from .carried import schedules
from .request import month_bounds


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
