"""Sequential frozen singleton replay; engine owns all trading semantics."""
from copy import deepcopy
from dataclasses import asdict
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd

from ..selection.ranking import require
from ..fixed.config import RUNTIME
from ..fixed.accounting import finite_json
from .accounting import finite_sum, metrics, preflight, dense_digest, reconcile_trades


def source_context(p, label, ticker, windows, data_root, overrides):
    from ..config import _validate_strategy
    from ..filters import effective
    from ..parquet import replay
    source = p['sources'][label]; fixed = p['calendar']['family'] == 'fixed'
    raw = source['manifest']['contract']['request'] if fixed else source['contract']
    config, profile = _validate_strategy(raw['generation']['strategy'], parquet=True)
    require(config['id'] == 's07_ha_v01_b2' and profile.modes.get('entryAdmission') == 'risk_bracket_v1', 'unsupported portfolio execution capability.')
    filters = effective(raw, config)
    cells = {(r['window'], r['ticker']): r for r in p['ledger'][label]}
    if fixed:
        account_id = cells[windows[0]['id'], ticker]['account_id']
        binding = next(a for a in source['manifest']['contract']['accounts'] if a['account_id'] == account_id)
    else:
        binding = next(b for b in source['bindings']['instruments'] if b['instrument_id'] == ticker)
    relevant = {i['source']['pack_root'] for i in filters}
    from core.entry_filters import source_locator
    local = {k: v for k, v in (overrides or {}).items() if source_locator(k) in relevant}
    prepared = replay(binding, data_root, filters, local)
    return dict(prepared=prepared, binding=binding, config=config, filters=filters, source=source, raw=raw, fixed=fixed, cells=cells)


def reference(ctx, ticker, window, minutes):
    from ..fixed.run import build_execution
    from ..parquet import segment, segment_facts
    prepared = ctx['prepared']; cell = ctx['cells'][window['id'], ticker]
    if ctx['fixed']:
        b = ctx['binding']
        ps = dict(params={k: v for k, v in b['params'].items() if k not in RUNTIME}, variants=[b['candidate']['variant']])
        frame, view = prepared.frame, prepared
        expected = b['candidate']
    else:
        from core.walkforward_engine import WindowSplit
        step = pd.Timedelta(minutes=minutes)
        segments = ctx['binding']['segments']
        match = [s for s in segments if s['segment'] == 'oos' and pd.Timestamp(s['first_timestamp']) == pd.Timestamp(window['start'])]
        require(len(match) == 1, 'saved OOS segment absent/ambiguous.')
        split = WindowSplit(match[0]['window_id'], pd.Timestamp(window['isStart']), pd.Timestamp(window['start'])-step,
            pd.Timestamp(window['start']), pd.Timestamp(window['end'])-step)
        part = segment(prepared, dict(instrument_id=ticker), split, 'oos', SimpleNamespace(generation=ctx['raw']['generation']))
        require(segment_facts(part, minutes) == match[0], 'saved segment geometry changed.')
        frame = part.dataframe
        view = SimpleNamespace(frame=frame, trade_start_idx=part.trade_start_idx, entry_admission=prepared.entry_admission,
            admission_rules=prepared.admission_rules, entry_filter_context=prepared.entry_filter_context,
            facts=dict(bounds=dict(evaluation_start=frame.index[part.trade_start_idx].isoformat(),
                evaluation_end=(frame.index[-1]+step-pd.Timedelta(microseconds=1)).isoformat())))
        if cell['candidate_id'] is None:
            return None
        expected = next(c for c in ctx['source']['candidates']['candidates'] if c['candidate_id'] == cell['candidate_id'])
        ps = dict(params={k: v for k, v in expected['params'].items() if k not in RUNTIME}, variants=[expected['variant_name']])
    plan, hooks, params, candidate = build_execution(ctx['config'], ps, view, ctx['filters'])
    require(candidate['semantic_key'] == expected['semantic_key'], 'reference singleton semantic key changed.')
    require({k: v for k, v in params.items() if k not in RUNTIME} == ps['params'], 'reference scalar binding changed.')
    if ctx['fixed']:
        require(candidate == expected and plan.plan_fingerprint == ctx['binding']['plan_fingerprint'], 'reference Fixed fingerprint changed.')
    return dict(view=view, frame=frame, ps=ps, params=params, candidate=candidate, fingerprint=plan.plan_fingerprint)


def execute(ctx, ref, capital):
    from core.portfolio import positive_capital
    from core.engine_v2.runner import run_v2_strategy
    from core.engine_v2.contracts import GUARDRAIL_FLAG_EXECUTION_ERROR
    from core.entry_filters import validate_context
    from ..fixed.run import build_execution
    actual = deepcopy(ref['ps']); actual['params']['initialCapital'] = positive_capital(capital)
    plan, hooks, params, candidate = build_execution(ctx['config'], actual, ref['view'], ctx['filters'])
    require({k: v for k, v in params.items() if k != 'initialCapital'} ==
        {k: v for k, v in ref['params'].items() if k != 'initialCapital'} and candidate['variant'] == ref['candidate']['variant'],
        'actual execution altered non-capital scalars/variant.')
    view = ref['view']
    validate_context(plan.profile.entry_filter_methods, view.entry_filter_context, list(plan.settings.entry_filter_config))
    result = run_v2_strategy(data=hooks.build_execution_data(ref['frame'], params), profile=plan.profile,
        params=params, trade_start_idx=view.trade_start_idx, admission_rules=view.admission_rules,
        entry_filter_context=view.entry_filter_context, compute_sharpe_daily=True, compute_max_drawdown_mtm=True)
    require(not result.guardrail_summary.flags & GUARDRAIL_FLAG_EXECUTION_ERROR, 'portfolio execution-error guardrail.')
    require(result.standing_state.position_direction == result.standing_state.position_size == 0, 'strict_close failed.')
    sr = result.strategy_result; start = view.trade_start_idx
    require(sr.metric_start_idx == start and sr.metric_initial_equity == capital, 'actual metric anchor mismatch.')
    ts = pd.DatetimeIndex(sr.timestamps[start:]).as_unit('ns').asi8
    balance = np.asarray(sr.balance_curve[start:], dtype='float64'); equity = np.asarray(sr.equity_curve[start:], dtype='float64')
    require(len(ts) == len(balance) == len(equity) and np.all(np.isfinite(balance)) and np.all(np.isfinite(equity)), 'nonfinite/malformed engine path.')
    require(len(balance) and balance[-1] == equity[-1], 'strict-close balance/equity mismatch.')
    trades = []
    for trade in sr.trades:
        row = asdict(trade)
        for key in ('entry_time', 'exit_time'):
            row[key] = pd.Timestamp(row[key]).isoformat()
        rate = params['commissionPct']/100.
        row.update(entry_fee=trade.entry_price*trade.size*rate, exit_fee=trade.exit_price*trade.size*rate)
        trades.append(row)
    reconcile_trades(capital, float(balance[-1]), trades)
    diagnostic = finite_json(asdict(result.guardrail_summary))
    diagnostic.update(finite_json(sr.entry_diagnostics or {}))
    return ts, balance, equity, trades, dict(actual_fingerprint=plan.plan_fingerprint, actual_semantic_key=candidate['semantic_key'],
        reference_fingerprint=ref['fingerprint'], reference_semantic_key=ref['candidate']['semantic_key'],
        reference_capital=ref['params']['initialCapital'], diagnostics=diagnostic)


def verify_live(p, data_root, overrides):
    """Validate every required full-roster contract before any account executes."""
    calls = 0; windows = {w['id']: w for w in p['calendar']['windows']}
    for label in p['sources']:
        for group in p['calendar']['groups']:
            selected = [windows[i] for i in group['windows']]
            for ticker in p['calendar']['roster']:
                ctx = source_context(p, label, ticker, selected, data_root, overrides); calls += 1
                for w in selected:
                    reference(ctx, ticker, w, p['calendar']['timeframe'])
                del ctx
    return calls


def execute_variant(p, variant, data_root, overrides=None, cancelled=lambda: False):
    from core.portfolio import positive_capital
    cal = p['calendar']; minutes = cal['timeframe']; step = minutes*60*10**9
    windows = {w['id']: w for w in cal['windows']}
    slots = len(cal['roster']) if variant['method'] == 'all_declared' else cal['topN']
    estimate = preflight(cal['windows'], cal['groups'], slots, minutes)
    first, end = (pd.Timestamp(cal['windows'][i][k]).value for i, k in ((0, 'start'), (-1, 'end')))
    ts = np.arange(first, end+step, step, dtype='int64')
    balance, equity = np.empty(len(ts)), np.empty(len(ts))
    initial = float(p['request']['initialCapital']); total = initial
    balance[0] = equity[0] = initial
    allocations, trades, periods, references = [], [], [], []
    choices = {(r['window'], r['ticker']): r for r in p['ledger'][variant['input']]}
    replay_calls = 0
    for group, selection in zip(cal['groups'], p['decisions'][variant['id']]):
        require(not cancelled(), 'portfolio cancelled.')
        opening = total; funded = total > 0
        members = selection['members']; tickers = [*members, *([None]*(slots-len(members)))]
        start_i = (pd.Timestamp(group['start']).value-first)//step
        end_i = (pd.Timestamp(group['end']).value-first)//step
        n = end_i-start_i
        if not funded:
            balance[start_i+1:end_i+1] = equity[start_i+1:end_i+1] = total
            periods.append(dict(id=group['id'], opening=opening, closing=total, allocation=None, allocation_rounding_delta=None,
                status='not_funded', return_pct=None, reason='nonpositive_opening_capital'))
            continue
        allocation = positive_capital(total/slots)
        delta = finite_sum([allocation]*slots)-total
        slot_balance = np.empty((slots, n)); slot_equity = np.empty((slots, n))
        closes = []
        for rank, ticker in enumerate(tickers, 1):
            capital = allocation
            ctx = source_context(p, variant['input'], ticker, [windows[i] for i in group['windows']], data_root, overrides) if ticker else None
            replay_calls += int(ticker is not None)
            offset = 0
            for wid in group['windows']:
                require(not cancelled(), 'portfolio cancelled.')
                w = windows[wid]; count = (pd.Timestamp(w['end']).value-pd.Timestamp(w['start']).value)//step
                cell = choices[wid, ticker] if ticker else None
                status = 'idle' if ticker is None else 'depleted' if capital <= 0 else 'abstained' if cell['status'] == 'abstained' else 'executed'
                row = dict(variant=variant['id'], period=group['id'], window=wid, instrument=ticker, rank=rank,
                    allocated=allocation, opening=capital, closing=capital, status=status,
                    reason='nonpositive_opening_capital' if capital <= 0 else 'no_eligible_is_candidate' if status == 'abstained' else None,
                    allocation_rounding_delta=delta, candidate_id=cell['candidate_id'] if cell else None,
                    source_account_id=cell['account_id'] if cell else None, trade_count=0, fees=0., net_pnl=0.,
                    actual_fingerprint=None, actual_semantic_key=None, reference_fingerprint=None, reference_semantic_key=cell['semantic_key'] if cell else None,
                    reference_capital=None, diagnostics={})
                if status == 'executed':
                    ref = reference(ctx, ticker, w, minutes)
                    native, b, e, executed, binding = execute(ctx, ref, capital)
                    expected = np.arange(pd.Timestamp(w['start']).value, pd.Timestamp(w['end']).value, step, dtype='int64')
                    require(np.array_equal(native, expected), 'missing/misaligned execution observations.')
                    slot_balance[rank-1, offset:offset+count] = b; slot_equity[rank-1, offset:offset+count] = e
                    row.update(binding, closing=float(b[-1]), trade_count=len(executed),
                        fees=finite_sum(t['entry_fee']+t['exit_fee'] for t in executed), net_pnl=finite_sum(t['net_pnl'] for t in executed))
                    for i, t in enumerate(executed):
                        trades.append(dict(variant=variant['id'], period=group['id'], window=wid, rank=rank, instrument=ticker, trade=i+1, **t))
                    del ref, native, b, e, executed
                else:
                    slot_balance[rank-1, offset:offset+count] = slot_equity[rank-1, offset:offset+count] = capital
                allocations.append(row); capital = row['closing']; offset += count
            closes.append(capital)
            del ctx
        for i in range(n):
            balance[start_i+i+1] = finite_sum(slot_balance[:, i]); equity[start_i+i+1] = finite_sum(slot_equity[:, i])
        total = finite_sum(closes)
        require(total == balance[end_i] == equity[end_i], 'pooled close mismatch.')
        periods.append(dict(id=group['id'], opening=opening, closing=total, allocation=allocation,
            allocation_rounding_delta=delta, status='depleted' if total <= 0 else 'complete', return_pct=100*(total/opening-1), reason=None))
        del slot_balance, slot_equity
    # Reference percentages use held membership and T04 canonical reduction, never funded money.
    from ..selection.evaluate import arm_return
    for group, selection in zip(cal['groups'], p['decisions'][variant['id']]):
        for wid in group['windows']:
            cells = {t: choices[wid, t] for t in cal['roster']}
            references.append(dict(window=wid, value=arm_return(selection['members'], slots, cells)['value']))
    facts = metrics(ts, balance, equity, initial)
    growth = math.prod(period['closing']/period['opening'] for period in periods if period['opening'] > 0)
    require(math.isclose(growth, total/initial, rel_tol=1e-12, abs_tol=1e-12), 'period growth reconciliation failed.')
    window_results = []
    for w in cal['windows']:
        opening = float(balance[(pd.Timestamp(w['start']).value-first)//step])
        closing = float(balance[(pd.Timestamp(w['end']).value-first)//step])
        window_results.append(dict(window=w['id'], opening=opening, closing=closing,
            return_pct=100*(closing/opening-1) if opening > 0 else None,
            reason=None if opening > 0 else 'nonpositive_opening_capital'))
    return dict(variant=variant, status='depleted' if total <= 0 else 'complete', metrics=facts, periods=periods, windows=window_results,
        reference_windows=references, dense_digest=dense_digest(ts, balance, equity), native_count=len(ts),
        bounds=[int(ts[0]), int(ts[-1])], accounting_array_estimate=estimate, replay_calls=replay_calls), allocations, trades, (ts, balance, equity)
