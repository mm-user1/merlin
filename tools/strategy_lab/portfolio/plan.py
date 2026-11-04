"""Offline frozen choices from validated X/history and saved Lab families."""
from copy import deepcopy
import math
from pathlib import Path

import pandas as pd

from ..analysis.research_inputs import _keys, _text, _read, digest
from ..analysis.json_utils import canonical_json_bytes
from ..selection.ranking import require, integer, stamp, open_ranking, validate_calendar, validate_rows, ranking_identity
from ..selection.history_plan import checked_input, open_history_plan, history_request, block_map, plan_identity
from ..selection.history_score import project_decision, score_history
from ..selection.outcomes import collect_fixed, collect_candidates
from ..selection.history_evaluate import strategy_settings
from ..report_support import new_output, write_bytes, labels
from ..fixed.storage import safe_path

SCHEMA = 'strategy_lab_portfolio_plan_v1'


def request(raw):
    _keys(raw, 'schema_version name sourcePlan inputs candidateSelection initialCapital rebalanceMonths variants curveBuckets'.split(), 'portfolio request')
    require(raw['schema_version'] == 'strategy_lab_portfolio_request_v1', 'unsupported portfolio request.')
    _text(raw['name'], 'name')
    _keys(raw['sourcePlan'], ['kind', 'path'], 'sourcePlan')
    require(raw['sourcePlan']['kind'] in ('x', 'history'), 'invalid source plan kind.')
    _text(raw['sourcePlan']['path'], 'source path')
    require(type(raw['initialCapital']) in (int, float) and math.isfinite(raw['initialCapital']) and raw['initialCapital'] > 0,
        'initialCapital must be finite and positive.')
    integer(raw['rebalanceMonths'], 1, 120000, 'rebalanceMonths')
    integer(raw['curveBuckets'], 1, 4096, 'curveBuckets')
    require(isinstance(raw['inputs'], list) and raw['inputs'], 'inputs required.')
    labels([(i.get('label'), i.get('dataset')) for i in raw['inputs']])
    fixed = 'parameterSet' in raw['inputs'][0]
    for item in raw['inputs']:
        _keys(item, ['label', 'dataset', 'parameterSet'] if fixed else
            ['label', 'dataset', 'evaluationScope', 'historyScope', 'evaluationUnlock', 'historyUnlock'], 'input')
        _text(item['dataset'], 'dataset')
        if fixed:
            _text(item['parameterSet'], 'parameterSet')
        else:
            _text(item['evaluationScope'], 'evaluationScope')
            if raw['sourcePlan']['kind'] == 'history':
                _text(item['historyScope'], 'historyScope')
            else:
                require(item['historyScope'] is None and item['historyUnlock'] is None, 'X has no history scope.')
            for key in ('evaluationUnlock', 'historyUnlock'):
                if item[key] is not None:
                    _keys(item[key], ['unlock', 'policy'], key)
                    require(item[key]['unlock'] is True, 'unlock must be true.')
                    _text(item[key]['policy'], 'unlock policy')
    choice = raw['candidateSelection']
    if fixed:
        require(choice is None, 'Fixed has no candidateSelection.')
    else:
        require(isinstance(choice, dict) and set(choice) in ({'rule'}, {'policy'}), 'one rule or policy required.')
        _text(next(iter(choice.values())), 'candidateSelection')
    variants = raw['variants']
    require(isinstance(variants, list) and variants, 'variants required.')
    for v in variants:
        _keys(v, ['id', 'input', 'method'], 'variant')
        labels([(v['id'], '')])
        require(v['input'] in {i['label'] for i in raw['inputs']}, 'unknown variant input.')
        _text(v['method'], 'method')
    require(len({v['id'] for v in variants}) == len(variants) and
        len({(v['input'], v['method']) for v in variants}) == len(variants), 'duplicate variant.')
    for item in raw['inputs']:
        methods = {v['method'] for v in variants if v['input'] == item['label']}
        require('all_declared' in methods and len(methods) >= 2, 'each input needs selected-method and all_declared variants.')
    return deepcopy(raw)


def calendar(raw, source):
    r = source['metadata']['request']['effective']; c = r['calendar']
    fixed = c['mode'] == 'fixed'
    require(fixed == ('parameterSet' in raw['inputs'][0]), 'source/input family mismatch.')
    roster = sorted(source['metadata']['universe']['document']['tickers'])
    cadence = raw['rebalanceMonths']
    if raw['sourcePlan']['kind'] == 'x':
        require(cadence == r['tickerSelector']['rebalanceMonths'], 'X rebalance cadence must match source.')
        periods = source['metadata']['calendar']['periods']
        windows = [dict(id=p['id'], start=p['start'], end=p['end'], isStart=None) for p in periods] if fixed else [w for p in periods for w in p['windows']]
        groups = [dict(id=p['id'], windows=[p['id']] if fixed else [w['id'] for w in p['windows']]) for p in periods]
        excluded = []
        k = r['tickerSelector']['topN']; methods = ['merlin_x']
    else:
        mapping = source['metadata']['mapping']
        windows = mapping['blocks'][len(mapping['history_ids']):]
        excluded = mapping['history_ids']; k = r['topN']; methods = r['methods']
        if fixed:
            require(cadence == c['periodMonths'], 'Fixed history cadence must match periodMonths.')
            size = 1
        else:
            require(cadence % c['oosMonths'] == 0, 'rebalanceMonths must be divisible by oosMonths.')
            size = cadence // c['oosMonths']
        groups = [dict(id=i//size+1, windows=[w['id'] for w in windows[i:i+size]]) for i in range(0, len(windows), size)]
    require(all(v['method'] in [*methods, 'all_declared'] for v in raw['variants']), 'method not declared by source.')
    byid = {w['id']: w for w in windows}
    for g in groups:
        g.update(start=byid[g['windows'][0]]['start'], end=byid[g['windows'][-1]]['end'])
    return dict(family=c['mode'], timeframe=c['tradingTimeframeMinutes'], roster=roster, topN=k,
        windows=windows, groups=groups, excluded_history_ids=excluded, trailing=source['metadata']['calendar']['trailing'])


def decisions(raw, source, cal, ledgers, settings):
    result = {}
    for v in raw['variants']:
        selected = []
        for g in cal['groups']:
            if v['method'] == 'all_declared':
                decision = dict(ordered=cal['roster'], decision_identity=digest(dict(method='all_declared', roster=cal['roster'])))
                members = cal['roster']
            elif raw['sourcePlan']['kind'] == 'x':
                record = next(r for r in source['records'] if r['id'] == g['id'])
                decision = deepcopy(record)
                members = [r['instrument'] for r in sorted(record['rows'], key=lambda r: r['rank'] or 2**32) if r['selected']]
            else:
                blocks = source['metadata']['mapping']['blocks']
                index = next(i for i, b in enumerate(blocks) if b['id'] == g['windows'][0])
                projection = project_decision(source['metadata']['request']['effective'], cal['roster'], blocks, index,
                    ledgers[v['input']], v['method'], settings[v['input']])
                decision = score_history(projection)
                members = decision['ordered'][:cal['topN']]
            selected.append(dict(group=g['id'], cutoff=g['start'], members=members, decision=decision))
        result[v['id']] = selected
    return result


def identity(plan):
    # Display sampling is explicitly outside the execution/choice identity.
    return digest(dict(schema_version='strategy_lab_portfolio_plan_identity_v1',
        **{k: v for k, v in plan.items() if k not in ('schema_version', 'plan_identity', 'request')},
        request={k: v for k, v in plan['request'].items() if k != 'curveBuckets'}))


def prepare(request_path, output_dir):
    from ..analysis import open_dataset, DatasetInput, load_candidate_policy
    from ..dataset import load_json
    path, _, raw = _read(checked_input(request_path)); raw = request(raw)
    history = raw['sourcePlan']['kind'] == 'history'
    owner = (open_history_plan if history else open_ranking)(checked_input(raw['sourcePlan']['path']))
    source = dict(metadata=deepcopy(owner.metadata), records=[] if history else deepcopy(owner.records), sha256=owner.file_sha256)
    cal = calendar(raw, source); fixed = cal['family'] == 'fixed'
    windows = source['metadata']['mapping']['blocks'] if history else cal['windows']
    policy = None; rule = None
    if fixed:
        ledger, sources, roots, trace = collect_fixed(cal['roster'], cal['timeframe'], windows,
            [(i['label'], checked_input(i['dataset'])) for i in raw['inputs']], {i['label']: i['parameterSet'] for i in raw['inputs']}, False)
    else:
        choice = raw['candidateSelection']; rule = choice.get('rule')
        policy = load_candidate_policy(checked_input(choice['policy'])) if 'policy' in choice else None
        inputs, scopes = [], {}
        for item in raw['inputs']:
            d = open_dataset(checked_input(item['dataset']))
            def scope(role):
                u = item[role+'Unlock']
                return d.resolve_scope(item[role+'Scope'], unlock=u is not None, policy_path=checked_input(u['policy']) if u else None)
            inputs.append(DatasetInput(item['label'], d, scope('evaluation')))
            if history:
                scopes[item['label']] = scope('history')
        ledger, sources, roots, trace = collect_candidates(cal['roster'], cal['timeframe'], windows, inputs, rule, policy,
            history_scopes=scopes if history else None, history_ids=cal['excluded_history_ids'])
        for item in inputs:
            sources[item.label]['candidates'] = load_json(item.dataset.root/'candidates.json', 'candidates')
    settings = {n: strategy_settings(s, fixed, rule, policy) for n, s in sources.items()} if history else {}
    for s in sources.values():
        g = s['manifest']['contract']['request']['generation'] if fixed else s['contract']['generation']
        require(g['strategy']['strategy_id'] == 's07_ha_v01_b2' and g['execution']['policy'] == 'risk_bracket_v1', 'portfolio v1 requires S07 risk_bracket_v1.')
    p = dict(schema_version=SCHEMA, request=raw, source=source, calendar=cal, sources=sources, settings=settings,
        ledger={n: [dict(window=w, ticker=t, **row) for (w, t), row in rows.items()] for n, rows in ledger.items()},
        decisions=decisions(raw, source, cal, ledger, settings), access_trace=trace)
    p['plan_identity'] = identity(p)
    validate(p)
    out = new_output(output_dir, [path, owner.root, *roots])
    write_bytes(out, 'plan.json', canonical_json_bytes(p))
    return dict(output=str(out), plan_identity=p['plan_identity'], variants=len(raw['variants']))


def validate(p):
    _keys(p, 'schema_version request source calendar sources settings ledger decisions access_trace plan_identity'.split(), 'portfolio plan')
    require(p['schema_version'] == SCHEMA, 'unsupported portfolio plan schema.')
    raw = request(p['request']); s = p['source']; m = s['metadata']
    if raw['sourcePlan']['kind'] == 'history':
        from ..selection.ranking import validate_calendar_parts
        r = history_request(m['request']['raw'])
        require(r == m['request']['effective'], 'source history request mismatch.')
        c = r['calendar']; validate_calendar_parts(c, c.get('periodMonths', c.get('oosMonths')), m['calendar'])
        require(m['mapping'] == block_map(r, m['calendar']) and m['plan_identity'] == plan_identity(m), 'source history mapping mismatch.')
    else:
        validate_calendar(m); validate_rows(m, s['records'])
        require(m['ranking_identity'] == ranking_identity(m, s['records']), 'source X identity mismatch.')
    cal = calendar(raw, s)
    require(cal == p['calendar'], 'portfolio calendar mismatch.')
    names = {i['label'] for i in raw['inputs']}
    require(set(p['sources']) == set(p['ledger']) == names, 'source/ledger input mismatch.')
    fixed = cal['family'] == 'fixed'
    ledgers = {}
    windows = m['mapping']['blocks'] if raw['sourcePlan']['kind'] == 'history' else cal['windows']
    expected = {(w['id'], t) for w in windows for t in cal['roster']}
    for label, rows in p['ledger'].items():
        ledgers[label] = {(row['window'], row['ticker']): {k: v for k, v in row.items() if k not in ('window', 'ticker')} for row in rows}
        require(len(ledgers[label]) == len(rows) and set(ledgers[label]) == expected, 'missing/duplicate frozen cells.')
        source = p['sources'][label]
        if fixed:
            from ..fixed.storage import validate_contract, contract_identity
            contract = source['manifest']['contract']; validate_contract(contract)
            require(source['manifest']['contract_sha256'] == contract_identity(contract), 'reference Fixed contract identity mismatch.')
            accounts = {a['account_id']: a for a in contract['accounts']}
            for row in rows:
                require(row['account_id'] in accounts, 'unknown frozen account.')
                a = accounts[row['account_id']]; w = next(w for w in windows if w['id'] == row['window'])
                require(a['instrument_id'] == row['ticker'] and stamp(a['input_pin']['bounds']['start']) == stamp(w['start'])
                    and stamp(a['input_pin']['bounds']['end']) == stamp(w['end']), 'frozen account cell mismatch.')
        else:
            from ..config_v2 import validate_saved_request
            from ..analysis.dataset import _candidate_geometry
            from ..parquet import validate_bindings, identity as dataset_identity
            from ..config import canonical_sha256
            from types import SimpleNamespace
            all_windows = validate_saved_request(source['contract'])
            raw_source = source['contract']; manifest_identity = source['manifest']['identity']
            _candidate_geometry(source['candidates'], manifest_identity)
            included = manifest_identity['included_tickers']
            entries = [e for e in raw_source['generation']['market_data']['instruments']
                if e['instrument_id'] in {i['canonical_symbol'] for i in included}]
            saved_windows = [w for w in all_windows if w.window_id in {i['window_id'] for i in manifest_identity['windows']}]
            spec = SimpleNamespace(raw=raw_source, generation=raw_source['generation'],
                generation_sha256=canonical_sha256(raw_source['generation']), pre_registration_sha256=canonical_sha256(raw_source))
            validate_bindings(spec, entries, saved_windows, source['candidates'], source['bindings'])
            require(dataset_identity(spec, entries, saved_windows, source['candidates'], source['bindings']) == manifest_identity, 'reference dataset identity mismatch.')
            candidates = {c['candidate_id']: c for c in source['candidates']['candidates']}
            for row in rows:
                if row['candidate_id'] is None:
                    require(row['status'] == 'abstained' and row['value'] == 0 and row['semantic_key'] is None, 'invalid abstention.')
                else:
                    require(row['candidate_id'] in candidates and candidates[row['candidate_id']]['semantic_key'] == row['semantic_key'], 'frozen candidate identity mismatch.')
    require(p['decisions'] == decisions(raw, s, cal, ledgers, p['settings']), 'frozen decisions contradict source/causal projection.')
    require(p['plan_identity'] == identity(p), 'portfolio plan identity mismatch.')
    return p


def open_plan(root):
    _, _, p = _read(safe_path(checked_input(root), 'plan.json'))
    return validate(p)


def protected_roots(p):
    """Recorded native locators remain protected even after live relocation."""
    from ..filters import recorded_roots
    roots = [p['request']['sourcePlan']['path']]
    for s in p['sources'].values():
        roots.append(s['path'])
        if p['calendar']['family'] == 'fixed':
            contract = s['manifest']['contract']
            roots.extend(a['provenance']['pack_root'] for a in contract['accounts'])
            roots.extend(recorded_roots(contract['resolved'].get('entry_filters', [])))
        else:
            root = s['manifest'].get('provenance', {}).get('resolved_data_root')
            if root: roots.append(root)
            roots.extend(b['original_root'] for b in s['bindings']['instruments'])
            roots.extend(recorded_roots(s['bindings'].get('effective_entry_filters', [])))
        for role in ('scope',):
            evidence = s.get(role, {}).get('unlock_evidence')
            if evidence: roots.append(evidence['policy_ref'])
        for role in s.get('scope_roles', {}).values():
            evidence = role['scope'].get('unlock_evidence')
            if evidence: roots.append(evidence['policy_ref'])
    if p['request']['candidateSelection'] and 'policy' in p['request']['candidateSelection']:
        roots.append(p['request']['candidateSelection']['policy'])
    return roots
