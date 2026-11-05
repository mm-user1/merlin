"""Small frozen portfolio requests over ordinary saved-family fixtures."""
import json
import pytest


def check_report(root, m, p, allocations):
    """Assert rendered rows against saved facts, including HTML cell associations."""
    import csv
    import math
    from html.parser import HTMLParser
    class Tables(HTMLParser):
        def __init__(self):
            super().__init__(); self.tables = []; self.cell = None
        def handle_starttag(self, tag, attrs):
            if tag == 'table': self.tables.append([])
            if tag == 'tr': self.tables[-1].append([])
            if tag in ('th', 'td'): self.cell = ''
        def handle_data(self, data):
            if self.cell is not None: self.cell += data
        def handle_endtag(self, tag):
            if tag in ('th', 'td'):
                self.tables[-1][-1].append(self.cell); self.cell = None
    html = (root/'index.html').read_text(encoding='utf-8')
    parser = Tables(); parser.feed(html)
    tables = {}
    for name, rendered in zip(('summary', 'windows', 'periods'), parser.tables):
        with (root/(name+'.csv')).open(encoding='utf-8', newline='') as stream:
            rows = list(csv.DictReader(stream))
        assert rendered[0] == list(rows[0])
        assert rendered[1:] == [[v if v != '' else 'unavailable' for v in r.values()] for r in rows]
        tables[name] = rows
        assert 'href="'+name+'.csv"' in html
    assert set(tables) == {'summary', 'windows', 'periods'}
    assert 'actual_carried_execution' in html and 'independent_reference_returns' in html
    assert 'unassigned-slot cash' in html and 'independent reference-capital accounts' in html
    def value(row, key, expected):
        assert row[key] == ('' if expected is None else str(expected)), (key, row[key], expected)
    cal = p['calendar']
    for v, summary in zip(m['variants'], tables['summary']):
        vid = v['variant']['id']; label = v['variant']['input']; method = v['variant']['method']
        for k, n in v['diagnostics'].items(): value(summary, k, n)
        denominator = len(cal['roster']) if method == 'all_declared' else cal['topN']
        wr = [r for r in tables['windows'] if r['variant'] == vid]
        pr = [r for r in tables['periods'] if r['variant'] == vid]
        assert len(wr) == len(v['windows']) and len(pr) == len(v['periods'])
        for group, d, period, row in zip(cal['groups'], p['decisions'][vid], v['periods'], pr):
            common = dict(variant=vid, input=label, method=method, period=group['id'],
                period_start=group['start'], period_end=group['end'], selection_cutoff=d['cutoff'],
                roster_size=len(cal['roster']), denominator=denominator, selected_count=len(d['members']),
                unused_slots=denominator-len(d['members']))
            for k, n in common.items(): value(row, k, n)
            assert json.loads(row['held_tickers']) == d['members']
            for k in ('opening','closing','allocation','allocation_rounding_delta','status','return_pct','reason'):
                value(row, k, period[k])
            first = [a for a in allocations if a['variant']==vid and a['window']==group['windows'][0]]
            value(row, 'unassigned_slot_cash', math.fsum(a['opening'] for a in first if a['status']=='idle') if first else None)
            for wid in group['windows']:
                row = next(r for r in wr if r['window']==str(wid))
                for k,n in common.items(): value(row,k,n)
                assert json.loads(row['held_tickers']) == d['members']
                w = next(w for w in cal['windows'] if w['id']==wid)
                value(row,'window_start',w['start']); value(row,'window_end',w['end'])
                value(row,'period_status',period['status'])
                actual = next(w for w in v['windows'] if w['window']==wid)
                for k in ('opening','closing','return_pct','reason'): value(row,'actual_'+k,actual[k])
                value(row,'reference_return_pct',next(w['value'] for w in v['reference_windows'] if w['window']==wid))
                accounts = [a for a in allocations if a['variant']==vid and a['window']==wid]
                for status in ('abstained','depleted'):
                    value(row,status+'_accounts',sum(a['status']==status for a in accounts) if accounts else None)


def request(source, datasets, *, family='fixed', kind='x', capital=237., cadence=2, buckets=16, method=None):
    method = method or ('merlin_x' if kind == 'x' else 'prior_oos_positive_count')
    inputs = []
    for label, dataset in datasets.items():
        if family == 'fixed':
            manifest = json.loads((dataset/'manifest.json').read_text(encoding='utf-8'))
            inputs.append(dict(label=label, dataset=str(dataset), parameterSet=manifest['contract']['resolved']['parameter_sets'][0]['name']))
        else:
            inputs.append(dict(label=label, dataset=str(dataset), evaluationScope='research', evaluationUnlock=None,
                historyScope='research' if kind == 'history' else None, historyUnlock=None))
    return dict(schema_version='strategy_lab_portfolio_request_v1', name='synthetic portfolio', sourcePlan=dict(kind=kind, path=str(source)),
        inputs=inputs, candidateSelection=None if family == 'fixed' else dict(rule='primary_profit'), initialCapital=capital,
        rebalanceMonths=cadence, curveBuckets=buckets,
        variants=[dict(id=label+'-'+m, input=label, method=m) for label in datasets for m in (method, 'all_declared')])


@pytest.fixture(scope='module')
def production_family(tmp_path_factory):
    from tests._portfolio_helpers import publish
    from _selection_helpers import document, universe, request as x_request
    from _fixed_helpers import request as fixed_request
    from _filter_helpers import is_request
    from tools.strategy_lab.selection import prepare_ranking, write_fixed_request
    from tools.strategy_lab.fixed.config import resolve
    from tools.strategy_lab.fixed.run import run
    from tools.strategy_lab.generate import generate_dataset
    work = tmp_path_factory.mktemp('portfolio-parity'); pack = work/'pack'
    pool = publish(pack); tickers = [s['instrument_id'] for s in pool]
    u = universe(work, tickers)
    raw = fixed_request(); raw['generation']['economics']['base_params']['initialCapital'] = 237./2
    raw['generation']['economics']['base_params']['entryRunLength'] = 2
    raw['generation']['market_data']['instruments'] = [dict(instrument_id=t, cell='research') for t in tickers]
    raw['generation']['expected_run_count'] = 2
    _, resolved = resolve(raw)
    params = resolved['parameter_sets'][0]['params']
    rankings = {}
    for family in ('fixed', 'is_oos'):
        rankings[family] = work/(family+'-ranking')
        source = x_request(mode=family, start='2024-02-01' if family == 'fixed' else '2024-01-01', end='2024-04-30')
        prepare_ranking(document(work, family+'.json', source), rankings[family], ticker_list=u, data_root=pack)
    write_fixed_request(rankings['fixed'], document(work, 'template.json', raw), work/'transform', warmup_bars=100)
    run(work/'transform/fixed-request.json', pack, work/'fixed')
    candidate = is_request(None); g = candidate['generation']
    g['economics']['base_params'] = params
    g['market_data']['instruments'] = raw['generation']['market_data']['instruments']
    g['windows'].update(requested_start='2024-01-01', requested_end='2024-05-01', is_period_months=1, expected_window_count=3)
    g['planning'].update(enabled_axes=[], axis_values={}, expected_candidate_count=1)
    candidate['preregistration']['analysis_scopes'][0]['window_numbers'] = [1, 2, 3]
    generate_dataset(document(work, 'candidate.json', candidate), data_root=pack, output_dir=work/'candidates')
    return dict(work=work, pack=pack, pool=pool, params=params, rankings=rankings, fixed=work/'fixed', candidates=work/'candidates')
