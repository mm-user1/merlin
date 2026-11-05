"""Offline HTML and small CSV summaries from verified saved evidence."""
from html import escape
import json
import os
from pathlib import Path

from ..analysis.json_utils import canonical_json_bytes
from ..report_support import new_output, write_bytes, write_rows
from .storage import open_result
from .plan import protected_roots
from .accounting import finite_sum


def table(rows):
    def cell(value):
        if value is None:
            return 'unavailable'
        return json.dumps(value, separators=(',', ':')) if isinstance(value, list) else str(value)
    columns = list(rows[0])
    body = '<table><tr>'+''.join('<th>'+escape(k)+'</th>' for k in columns)+'</tr>'
    for row in rows:
        body += '<tr>'+''.join('<td>'+escape(cell(row[k]))+'</td>' for k in columns)+'</tr>'
    return body+'</table>'


def report(result, output_dir):
    m, p, allocations, _ = open_result(result)
    out = new_output(output_dir, [result, *protected_roots(p)])
    rows = [dict(variant=v['variant']['id'], input=v['variant']['input'], method=v['variant']['method'], status=v['status'],
        **{k: v['metrics'][k] for k in ('initial', 'final', 'profit', 'return_pct', 'balance_dd_pct', 'mtm_dd_pct')},
        trades=v['trade_count'], fees=v['fees'], native_points=v['native_count'], saved_points=v['curve_policy']['saved_count'],
        **v['diagnostics']) for v in m['variants']]
    windows, periods = [], []
    cal = p['calendar']; bounds = {w['id']: w for w in cal['windows']}
    accounts = {}
    for row in allocations:
        accounts.setdefault((row['variant'], row['window']), []).append(row)
    for v in m['variants']:
        variant = v['variant']; vid = variant['id']
        slots = len(cal['roster']) if variant['method'] == 'all_declared' else cal['topN']
        actual = {w['window']: w for w in v['windows']}
        reference = {w['window']: w['value'] for w in v['reference_windows']}
        for group, decision, period in zip(cal['groups'], p['decisions'][vid], v['periods']):
            common = dict(variant=vid, input=variant['input'], method=variant['method'], period=period['id'],
                period_start=group['start'], period_end=group['end'], selection_cutoff=decision['cutoff'],
                roster_size=len(cal['roster']), denominator=slots, selected_count=len(decision['members']),
                held_tickers=decision['members'], unused_slots=slots-len(decision['members']))
            first = accounts.get((vid, group['windows'][0]), [])
            periods.append(dict(**common, **{k: period[k] for k in
                ('opening', 'closing', 'allocation', 'allocation_rounding_delta', 'status', 'return_pct', 'reason')},
                unassigned_slot_cash=finite_sum(r['opening'] for r in first if r['status'] == 'idle') if first else None))
            for wid in group['windows']:
                a = accounts.get((vid, wid), [])
                windows.append(dict(**common, window=wid, window_start=bounds[wid]['start'], window_end=bounds[wid]['end'],
                    period_status=period['status'], abstained_accounts=sum(r['status'] == 'abstained' for r in a) if a else None,
                    depleted_accounts=sum(r['status'] == 'depleted' for r in a) if a else None,
                    **{'actual_'+k: actual[wid][k] for k in ('opening', 'closing', 'return_pct', 'reason')},
                    reference_return_pct=reference[wid]))
    write_rows(out, 'summary.csv', rows)
    write_rows(out, 'windows.csv', windows)
    write_rows(out, 'periods.csv', periods)
    body = '<!doctype html><html lang="en"><meta charset="utf-8"><title>Strategy Lab carried portfolio</title><style>body{font:16px system-ui;margin:2rem;color:#17212b;background:#fff}td,th{padding:.5rem;text-align:left;border-bottom:1px solid #ddd}table{border-collapse:collapse}</style><h1>Actual carried execution</h1>'
    body += '<p>Frozen research policies. Each variant starts with the same total capital; account balances carry until the scheduled rebalance.</p>'
    body += '<p>Amounts are account currency; returns are percent; bounds and cutoffs are UTC. Empty CSV cells and unavailable HTML cells mean null, not zero. Held ticker lists are JSON arrays.</p>'
    body += table(rows)
    body += '<h2>Window comparison</h2><p>actual_carried_execution versus independent_reference_returns: reference values use the same held membership and denominator but independent reference-capital accounts. Reference percentages are not compounded into a funded portfolio.</p>'
    body += table(windows)
    body += '<h2>Period funding and status</h2><p>unassigned-slot cash sums idle slot openings in the first window only. It excludes funded abstained ticker accounts. Allocation rounding delta is a diagnostic, not a cash account.</p>'
    body += table(periods)
    body += '<h2>Interpretation and diagnostics</h2><p>Diagnostics retain saved counter names and aggregation: counters sum, no_capital_halt uses any, maxEntryLeverageUsed is the maximum available leverage.</p>'
    body += ''.join('<p>'+escape(line)+'</p>' for line in m['limitations'])
    body += '<h2>Saved artifacts</h2><ul>'
    for name in ('summary.csv', 'windows.csv', 'periods.csv'):
        body += '<li><a href="'+name+'">'+name+'</a></li>'
    for name in ['result.json', *m['artifacts']]:
        link = os.path.relpath(Path(result).absolute()/name, out).replace('\\', '/')
        body += '<li><a href="'+escape(link, quote=True)+'">'+escape(name)+'</a></li>'
    body += '</ul></html>'
    write_bytes(out, 'index.html', body.encode('utf-8'))
    write_bytes(out, 'report.json', canonical_json_bytes(dict(schema_version='strategy_lab_portfolio_report_v1', execution_identity=m['execution_identity'])))
    return dict(output=str(out), variants=len(rows))
