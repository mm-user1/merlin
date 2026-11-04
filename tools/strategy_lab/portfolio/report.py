"""Offline HTML and small CSV summaries from verified saved evidence."""
from html import escape
import os
from pathlib import Path

from ..analysis.json_utils import canonical_json_bytes
from ..report_support import new_output, write_bytes, write_rows
from .storage import open_result
from .plan import protected_roots


def report(result, output_dir):
    m, p, _, _ = open_result(result)
    out = new_output(output_dir, [result, *protected_roots(p)])
    rows = [dict(variant=v['variant']['id'], input=v['variant']['input'], method=v['variant']['method'], status=v['status'],
        **{k: v['metrics'][k] for k in ('initial', 'final', 'profit', 'return_pct', 'balance_dd_pct', 'mtm_dd_pct')},
        trades=v['trade_count'], fees=v['fees'], native_points=v['native_count'], saved_points=v['curve_policy']['saved_count']) for v in m['variants']]
    write_rows(out, 'summary.csv', rows)
    body = '<!doctype html><html lang="en"><meta charset="utf-8"><title>Strategy Lab carried portfolio</title><style>body{font:16px system-ui;margin:2rem;color:#17212b;background:#fff}td,th{padding:.5rem;text-align:left;border-bottom:1px solid #ddd}table{border-collapse:collapse}</style><h1>Actual carried execution</h1>'
    body += '<p>Frozen research policies. Each variant starts with the same total capital; account balances carry until the scheduled rebalance.</p>'
    body += '<table><tr>'+''.join('<th>'+escape(k)+'</th>' for k in rows[0])+'</tr>'
    for row in rows:
        body += '<tr>'+''.join('<td>'+escape(str(v))+'</td>' for v in row.values())+'</tr>'
    body += '</table><h2>Interpretation and diagnostics</h2>'
    body += ''.join('<p>'+escape(line)+'</p>' for line in m['limitations'])
    body += '<h2>Saved artifacts</h2><ul><li><a href="summary.csv">Summary CSV</a></li>'
    for name in ['result.json', *m['artifacts']]:
        link = os.path.relpath(Path(result).absolute()/name, out).replace('\\', '/')
        body += '<li><a href="'+escape(link, quote=True)+'">'+escape(name)+'</a></li>'
    body += '</ul></html>'
    write_bytes(out, 'index.html', body.encode('utf-8'))
    write_bytes(out, 'report.json', canonical_json_bytes(dict(schema_version='strategy_lab_portfolio_report_v1', execution_identity=m['execution_identity'])))
    return dict(output=str(out), variants=len(rows))
