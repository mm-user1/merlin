"""V2 fixed-reference storage adapter; the accepted v1 arithmetic is unchanged."""
from .arithmetic import calculate
from .storage_v2 import portfolio_order


def fixed_calculation(doc,ledger,paths,roster,scores):
    results,curves=calculate(doc,ledger,paths,roster,scores)
    rows={m:[] for m in doc['evaluationMonths']}
    for c in results['comparisons']:
        for m in c['months']:
            for name in ('selected','allEligible'):
                rows[m['month']].append(dict(selectorId=c['selectorId'],k=c['k'],portfolio=name,replicateIndex=-1,month=m['month'],**m[name]))
            for r in m.pop('random'):
                rows[m['month']].append(dict(selectorId=c['selectorId'],k=c['k'],portfolio='random',month=m['month'],**r))
    results.update(schema='pattern-lab.selection-results.v2',capitalMode='fixedReference')
    return results,{m:sorted(v,key=portfolio_order) for m,v in rows.items()},curves
