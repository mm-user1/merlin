"""Publish complete portfolios and verify optional dense regeneration."""
import platform
from pathlib import Path

import numpy as np
import pandas as pd

from ..analysis.research_inputs import digest
from ..analysis.json_utils import canonical_json_bytes
from ..analysis.dataset import git_facts
from ..dataset import artifact_record
from ..filters import runtime_roots
from ..report_support import new_output, write_bytes
from ..selection.ranking import require
from .plan import open_plan, protected_roots
from .accounting import compact, preflight, finite_sum
from .storage import (SCHEMA, LIMITATIONS, ALLOCATION_COLUMNS, TRADE_COLUMNS, write_table, write_curve,
    validate_result, open_result, diagnostics)


def output_root(p, output, inputs, data_root, overrides, label=None):
    from core.entry_filters import source_locator
    items = {name: s['manifest']['contract']['resolved'].get('entry_filters', []) if p['calendar']['family'] == 'fixed'
        else s['bindings'].get('effective_entry_filters', []) for name, s in p['sources'].items()}
    require(overrides is None or isinstance(overrides, dict), 'filter_root_overrides must map original locators to local roots.')
    keys = {source_locator(k) for k in (overrides or {})}
    # Validate supplied mappings against all known locators without resolving unused originals.
    supplied = [i for entries in items.values() for i in entries if i['source']['pack_root'] in keys]
    normalized = runtime_roots(supplied, overrides, existing=True, output=output, target=data_root)
    roots = [*inputs, data_root, *protected_roots(p), *normalized.values()]
    relevant = [i for name, entries in items.items() if label is None or name == label for i in entries]
    locators = {i['source']['pack_root'] for i in relevant}
    normalized = runtime_roots(relevant, {k: v for k, v in normalized.items() if k in locators},
        existing=True, output=output, target=data_root)
    return new_output(output, roots), normalized


def provenance():
    from ..fixed.run import peak_rss_bytes
    return dict(git=git_facts(), python=platform.python_version(), numpy=np.__version__, platform=platform.platform(),
        process_peak_rss_bytes=peak_rss_bytes())


def finish_variant(facts, allocations):
    facts.update(diagnostics=diagnostics(allocations), trade_count=sum(r['trade_count'] for r in allocations),
        fees=finite_sum(r['fees'] for r in allocations))
    return facts


def run(plan, data_root, output_dir, *, filter_root_overrides=None, cancelled=lambda: False):
    from .execution import verify_live, execute_variant
    p = open_plan(plan)
    out, overrides = output_root(p, output_dir, [plan], data_root, filter_root_overrides)
    for v in p['request']['variants']:
        preflight(p['calendar']['windows'], p['calendar']['groups'],
            len(p['calendar']['roster']) if v['method'] == 'all_declared' else p['calendar']['topN'], p['calendar']['timeframe'])
    require(not cancelled(), 'portfolio cancelled.')
    preflight_calls = verify_live(p, data_root, overrides)
    variants, allocations, trades, files = [], [], [], ['plan.json', 'allocations.parquet', 'trades.parquet']
    for variant in p['request']['variants']:
        facts, a, t, (ts, balance, equity) = execute_variant(p, variant, data_root, overrides, cancelled)
        finish_variant(facts, a)
        mandatory = [pd.Timestamp(w[k]).value for w in p['calendar']['windows'] for k in ('start', 'end')]
        mandatory += [facts['metrics'][k][part] for k in ('balance_witness', 'mtm_witness') for part in ('peak', 'trough')]
        chosen, policy = compact(ts, balance, equity, p['request']['curveBuckets'], mandatory)
        name = 'curves/'+variant['id']+'.parquet'
        write_curve(out, name, ts[chosen], balance[chosen], equity[chosen])
        facts.update(curve_file=name, curve_policy=policy, storage_identity=digest(dict(dense_digest=facts['dense_digest'], policy=policy)))
        variants.append(facts); allocations.extend(a); trades.extend(t); files.append(name)
        del ts, balance, equity, a, t
    write_bytes(out, 'plan.json', canonical_json_bytes(p))
    write_table(out, 'allocations.parquet', allocations, ALLOCATION_COLUMNS)
    write_table(out, 'trades.parquet', trades, TRADE_COLUMNS)
    m = dict(schema_version=SCHEMA, status='depleted' if any(v['status'] == 'depleted' for v in variants) else 'complete', plan_identity=p['plan_identity'],
        execution_identity=digest(dict(version='strategy_lab_portfolio_execution_v1', plan_identity=p['plan_identity'])),
        provenance=dict(**provenance(), preflight_replay_calls=preflight_calls), variants=variants,
        artifacts={n: artifact_record(out/n, out) for n in files}, limitations=LIMITATIONS,
        units=dict(amounts='account_currency', returns='percent', timestamps='UTC_nanoseconds_bar_close', reference='independent_reference_returns'))
    validate_result(out, m, p, allocations, trades)
    require(not cancelled(), 'portfolio cancelled before publication.')
    write_bytes(out, 'result.json', canonical_json_bytes(m))
    return dict(output=str(out), execution_identity=m['execution_identity'], variants=len(variants), status=m['status'])


def detail(result, variant, data_root, output_dir, *, filter_root_overrides=None, cancelled=lambda: False):
    from .execution import verify_live, execute_variant
    m, p, allocations, trades = open_result(result)
    selected = [v for v in m['variants'] if v['variant']['id'] == variant]
    require(len(selected) == 1, 'unknown detail variant.')
    saved = selected[0]
    label = saved['variant']['input']
    out, overrides = output_root(p, output_dir, [result], data_root, filter_root_overrides, label)
    verify_live(p, data_root, overrides, label)
    facts, a, t, curves = execute_variant(p, saved['variant'], data_root, overrides, cancelled)
    finish_variant(facts, a)
    for key, value in facts.items():
        require(value == saved[key], f'detail not reproduced: {key}; compare saved/current environment.')
    require(a == [r for r in allocations if r['variant'] == variant], 'detail not reproduced: funding/bindings/diagnostics.')
    require(t == [r for r in trades if r['variant'] == variant], 'detail not reproduced: trades.')
    require(not cancelled(), 'detail cancelled.')
    write_curve(out, 'dense.parquet', *curves)
    meta = dict(schema_version='strategy_lab_portfolio_detail_v1', parent_execution_identity=m['execution_identity'],
        variant=variant, status='verified', dense_digest=facts['dense_digest'], native_count=facts['native_count'],
        checked=['metrics', 'funding', 'trades', 'diagnostics', 'bindings', 'dense_digest'], provenance=provenance(),
        artifact=artifact_record(out/'dense.parquet', out))
    write_bytes(out, 'detail.json', canonical_json_bytes(meta))
    return dict(output=str(out), status='verified', variant=variant, dense_digest=facts['dense_digest'])
