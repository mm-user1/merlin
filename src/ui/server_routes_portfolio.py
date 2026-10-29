"""Offline portfolio views. All arithmetic stays in the shared Python projection."""
from flask import jsonify, request

from core import storage
from core.portfolio_projection import project
from core.portfolio_storage import load_portfolio, account_context, display_end


def summary_row(record, identity, name, selection, *, account=None):
    result = project(record, selection) if account is None or account.get('path') else {}
    return {"study_id": identity, "study_name": name, "strategy": record['strategy'], "portfolio_id": record['id'],
            "portfolioSelection": selection, "portfolioRank": (account or {}).get('rank'),
            "resultStudyId": (account or {}).get('studyId'), "unavailable": account is not None and not account.get('path'),
            "reason": (account or {}).get('reason') or (account or {}).get('status'),
            "entry_filters": record.get('entryFiltersDescription', {'key':None,'text':'Unavailable'}),
            "profit_pct": result.get('profit_pct'), "ann_profit_pct": result.get('ann_profit_pct'),
            "max_dd_pct": result.get('max_drawdown_pct'), "total_trades": result.get('total_trades'),
            "winning_trades": result.get('winning_trades'), "wfe_pct": result.get('wfe'),
            "profitable_windows": result.get('oos_positive_windows'), "total_windows": result.get('oos_window_count'),
            "profitable_windows_pct": result.get('oos_win_rate'), "median_window_profit": result.get('oos_profit_median'),
            "median_window_wr": result.get('oos_wr_median'), "consistency_recent": result.get('consistency_score'),
            "consistency_full": result.get('consistency_score_full'), "entry_diagnostics": result.get('entry_diagnostics'),
            "dataset_start_date": record['periods'][0]['start'], "dataset_end_date": record['periods'][-1]['end'],
            "wfa_mode": 'Fixed' if not record['wfa'] else 'Calendar',
            "is_oos": f"{record['wfa']['is_period_months']}m/{record['wfa']['oos_period_months']}m" if record['wfa'] else 'N/A'}


def register_routes(app):
    @app.get('/api/portfolios/<identifier>')
    def portfolio_view(identifier):
        try:
            from core.data_input import timeframe_label
            record = load_portfolio(identifier)
            period_id = int(request.args['period']) if 'period' in request.args else None
            tf = timeframe_label(record['timeframeMinutes'])
            rows = []
            if period_id is None:
                for rank in range(1, record['selector']['topN'] + 1):
                    row = summary_row(record, f"pf:{identifier}:rank:{rank}", f"#{rank} · {tf}", {'ranks':[rank]})
                    row['portfolioRank'] = rank
                    rows.append(row)
            else:
                period = next((p for p in record['periods'] if p['id'] == period_id), None)
                if period is None:
                    raise ValueError('Portfolio period not found.')
                symbols = {}
                for account in period['accounts']:
                    if account['instrument']:
                        symbol = account['instrument'].split('_', 1)[1].replace('-USDT-SWAP', 'USDT')
                        symbols[symbol] = symbols.get(symbol, 0) + 1
                for account in period['accounts']:
                    rank = account['rank']
                    if account['instrument']:
                        venue, contract = account['instrument'].split('_', 1)
                        symbol = contract.replace('-USDT-SWAP', 'USDT')
                        symbol = f'{venue}:{symbol}' if symbols[symbol] > 1 else symbol
                        name = f"{'#'+str(rank)+' ' if rank else ''}{symbol}, {tf}"
                    else:
                        name = f'#{rank} —'
                    rows.append(summary_row(record, account['id'], name, {'accounts':[account['id']]}, account=account))
            period_rows = [summary_row(record, f"pf:{identifier}:period:{p['id']}",
                f"{record['name'].split(' · PF')[0] if record['name'].startswith('#') else 'PF'} · P{p['id']} · {p['start'][:10]}–{display_end(p['end'])}",
                {'periods':[p['id']]}) for p in record['periods']]
            return jsonify({'id':identifier, 'name':record['name'], 'selector':record['selector'], 'rows':rows,
                            'periodRows':period_rows, 'status':record['status']})
        except ValueError as exc:
            return jsonify(error=str(exc)), 400

    @app.get('/api/studies/<study_id>/portfolio')
    def portfolio_account(study_id):
        return jsonify(account_context(study_id))

    @app.post('/api/portfolios/equity')
    def portfolio_equity():
        try:
            from core.analytics import aggregate_equity_curves
            from core.engine_v2.admission import aggregate_entries
            payload = request.get_json(silent=True) or {}
            requested = payload.get('portfolios', [])
            ordinary = payload.get('studyIds', [])
            if not isinstance(requested, list) or not isinstance(ordinary, list):
                raise ValueError('Expected portfolio and study identity arrays.')
            grouped = {}
            for item in requested:
                if not isinstance(item, dict) or set(item) - {'id','selection'}:
                    raise ValueError('Invalid portfolio identity.')
                identifier = item['id']
                record = load_portfolio(identifier)
                from core.portfolio_projection import resolve_atoms
                ids = [a['id'] for a in resolve_atoms(record, item.get('selection'))]
                if identifier not in grouped:
                    grouped[identifier] = (record, set())
                grouped[identifier][1].update(ids)
            members = [project(record, {'accounts': sorted(ids)}) for record,ids in grouped.values()]
            if not ordinary and len(members) == 1:
                return jsonify(members[0])
            for study_id in dict.fromkeys(ordinary):
                with storage.get_db_connection() as conn:
                    row = conn.execute('SELECT * FROM ordinary_studies WHERE study_id=? AND optimization_mode=?', (study_id,'wfa')).fetchone()
                if row is None:
                    raise ValueError('Unknown ordinary WFA study identity.')
                row = dict(row)
                curve = storage.load_study_analytics_equity(study_id)
                from core.engine_v2.admission import aggregate_entries
                details = storage.load_study_from_db(study_id)
                diagnostic = aggregate_entries([(w['window_id'], (w.get('entry_diagnostics') or {}).get('oos')) for w in details['windows']])
                members.append({**curve, 'total_trades':row.get('stitched_oos_total_trades'),
                    'winning_trades':row.get('stitched_oos_winning_trades'), 'profit_pct':row.get('stitched_oos_net_profit_pct'),
                    'entry_diagnostics':diagnostic,
                    'wfe':row.get('best_value'), 'oos_win_rate':row.get('stitched_oos_win_rate'),
                    'oos_profit_median':row.get('median_window_profit'), 'oos_wr_median':row.get('median_window_wr')})
            result = aggregate_equity_curves([{'equity_curve':m['curve'], 'timestamps':m['timestamps']} for m in members])
            def average(field):
                values = [m[field] for m in members if m.get(field) is not None]
                return sum(values)/len(values) if values else None
            result.update(total_trades=sum(m.get('total_trades') or 0 for m in members),
                          winning_trades=sum(m.get('winning_trades') or 0 for m in members),
                          oos_wr_median=average('oos_wr_median'),
                          wfe=average('wfe'), oos_win_rate=average('oos_win_rate'), oos_profit_median=average('oos_profit_median'),
                          profitable_pct=100*sum(m.get('profit_pct',0) is not None and m.get('profit_pct',0)>0 for m in members)/len(members) if members else None,
                          entry_diagnostics=aggregate_entries([(str(i),m.get('entry_diagnostics')) for i,m in enumerate(members)]))
            return jsonify(result)
        except (ValueError, KeyError, TypeError) as exc:
            return jsonify(error=str(exc)), 400
