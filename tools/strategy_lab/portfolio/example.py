"""Create small synthetic saved inputs for the documented portfolio CLI example."""
import argparse
from contextlib import redirect_stdout
import json
from pathlib import Path
import sys

from ..config import REPO_ROOT
from ..report_support import new_output


def build(output_dir):
    from ..synthetic_parquet import publish
    from ..selection import prepare_ranking, write_fixed_request
    from ..fixed.run import run
    root = new_output(output_dir, [])
    root.mkdir(parents=True, exist_ok=True)
    def write(name, value):
        path = root/name
        path.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
        return path
    tickers = ['BYBIT_AAAUSDT', 'BYBIT_BBBUSDT']
    publish(root/'pack', symbols=('AAA', 'BBB'), start='2024-01-01', end='2024-06-01')
    universe = write('universe.json', dict(schema_version='strategy_lab_universe_v1', name='synthetic',
        evidence_classification='exploratory', selection_note='Example only', tickers=tickers))
    x = dict(schema_version='strategy_lab_x_request_v1', name='synthetic X',
        calendar=dict(mode='fixed', start='2024-03-01', end='2024-05-31', tradingTimeframeMinutes=60),
        tickerSelector=dict(version=1, enabled=True, method='meanBarRangePct', methodVersion=1, topN=2,
            rebalanceMonths=2, evaluateUnselected=False, params=dict(lookbackMonths=1, timeframeMinutes=30)), controls=dict(seed=17, draws=2))
    prepare_ranking(write('x.json', x), root/'ranking', ticker_list=universe, data_root=root/'pack')
    raw = json.loads((REPO_ROOT/'tools/strategy_lab/runspecs/s07_fixed_example.json').read_text(encoding='utf-8'))
    g = raw['generation']; g['parameter_sets'] = g['parameter_sets'][:1]
    g['market_data']['instruments'] = [dict(instrument_id=t, cell='research') for t in tickers]
    g['timeframes_minutes'] = [60]; g['periods'] = g['periods'][:1]; g['expected_run_count'] = 2
    g['economics']['base_params']['entryRunLength'] = 2
    write_fixed_request(root/'ranking', write('template.json', raw), root/'transform', warmup_bars=100)
    run(root/'transform/fixed-request.json', root/'pack', root/'reference')
    request = dict(schema_version='strategy_lab_portfolio_request_v1', name='synthetic carried portfolio',
        sourcePlan=dict(kind='x', path=str(root/'ranking')), inputs=[dict(label='off', dataset=str(root/'reference'), parameterSet='base')],
        candidateSelection=None, initialCapital=237., rebalanceMonths=2, curveBuckets=512,
        variants=[dict(id='selected', input='off', method='merlin_x'), dict(id='control', input='off', method='all_declared')])
    write('request.json', request)
    return dict(output=str(root), request=str(root/'request.json'), data_root=str(root/'pack'))


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('--output-dir', required=True)
    args = parser.parse_args()
    try:
        with redirect_stdout(sys.stderr): result = build(args.output_dir)
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr); return 2
    print(json.dumps(result)); return 0


if __name__ == '__main__':
    raise SystemExit(main())
