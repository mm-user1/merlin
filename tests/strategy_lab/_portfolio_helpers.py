"""Small frozen portfolio requests over ordinary saved-family fixtures."""
import json
import pytest


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
