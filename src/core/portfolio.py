"""Selection, preparation and sequential carried-account execution.

Numerical work finishes before the single publication transaction. The coordinator
owns funding, while existing Grid/WFA and generic V2 runners own every execution.
"""
from copy import deepcopy
from dataclasses import asdict
import math
import time
from uuid import uuid4

import pandas as pd

from .data_input import prepare_input, timeframe_label
from .entry_filters import effective_config
from .engine_v2.admission import aggregate_entries
from .grid_engine import run_grid_optimization, _grid_v2_settings_from_config
from .grid_v2 import GridV2PlanReuseCache
from .portfolio_projection import balance_path, project
from .ticker_selector import normalize_pool, normalize_selector, require_capability, build_calendar, rank_pool
from .walkforward_engine import WalkForwardEngine, WindowSplit, WFResult


def positive_capital(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
        raise ValueError("Execution capital must be finite and positive.")
    return float(value)


def plan_request(config, selector, sources, wfa=None):
    from strategies import get_strategy_config
    require_capability(get_strategy_config(config.strategy_id))
    selector = normalize_selector(selector)
    if not selector.get("enabled"):
        raise ValueError("Portfolio operation requires an enabled Ticker Selector.")
    pool = normalize_pool(sources)
    if selector["topN"] > len(pool):
        raise ValueError("Top N exceeds the deduplicated pool size.")
    fixed = config.fixed_params
    if fixed.get("dateFilter") is not True or not fixed.get("start") or not fixed.get("end"):
        raise ValueError("Ticker Selector requires Date Filter with Start and End.")
    positive_capital(fixed.get("initialCapital"))
    if config.optimization_mode != "grid":
        raise ValueError("Portfolio execution requires the V2 Grid engine.")
    if wfa is None and config.parameterMode != "fixed":
        raise ValueError("Grid Ticker Selector requires Calendar Months WFA.")
    if wfa and (wfa.adaptive_mode or wfa.period_unit != "months"):
        raise ValueError("Ticker Selector supports Calendar Months WFA only.")
    if config.grid_needs_dsr or (wfa and wfa.dsr_config and wfa.dsr_config.enabled):
        raise ValueError("V2 Grid DSR is unavailable in Phase 2.5.")
    calendar = build_calendar(fixed["start"], fixed["end"], pool[0]["timeframe_minutes"],
                              selector["rebalanceMonths"],
                              is_months=wfa.is_period_months if wfa else None,
                              oos_months=wfa.oos_period_months if wfa else None)
    return selector, pool, calendar


def run_portfolio(config, selector, sources, *, wfa=None, cancelled=lambda: False,
                  progress=lambda message: None, queue_number=None, publish=True):
    from .storage import _prepare_study_config_payload
    selector, pool, periods = plan_request(config, selector, sources, wfa)
    started = time.time()
    identifier = str(uuid4())
    total = positive_capital(config.fixed_params["initialCapital"])
    reference = total / selector["topN"]
    minutes = pool[0]["timeframe_minutes"]
    step = pd.Timedelta(minutes=minutes)
    request = {key: deepcopy(value) for key, value in vars(config).items()
               if key not in {"admission_rules", "entry_filter_context"} and not key.startswith("_")}
    record = {"version": 1, "id": identifier, "selector": selector, "request": request,
              "pool": pool, "periods": periods, "initialCapital": total, "referenceCapital": reference,
              "wfa": asdict(wfa) if wfa else None, "status": "complete", "requestedStart": config.fixed_params["start"],
              "requestedEnd": config.fixed_params["end"], "timeframeMinutes": minutes, "strategy": config.strategy_id}
    cache, prepared, children = {}, {}, []

    def checkpoint(message):
        progress(message)
        if cancelled():
            raise ValueError(f"Portfolio cancelled: {message}")

    # Complete causal selection and mandatory input admission before numerical work.
    for period in periods:
        checkpoint(f"Ranking period {period['id']}/{len(periods)}")
        period["ranking"] = rank_pool(pool, period["start"], selector, cancelled=cancelled, cache=cache)
        period["accounts"] = []
        for row in period["ranking"]:
            role = "funded" if row["selected"] else "comparison"
            account = {"id": f"{identifier}:{period['id']}:{role}:{row['instrument']}", "period": period["id"],
                       "instrument": row["instrument"], "role": role, "rank": row["rank"] if row["selected"] else None,
                       "status": "not_run" if row["eligible"] else "ineligible", "reason": row["reason"]}
            period["accounts"].append(account)
            if not row["eligible"] or not row["selected"] and not selector["evaluateUnselected"]:
                continue
            checkpoint(f"Preparing period {period['id']} {role} {row['instrument']}")
            try:
                prepared[account["id"]] = prepare_input(row["source"], date_filter=True,
                    start=period["windows"][0]["isStart"] if wfa else period["start"],
                    end=(pd.Timestamp(period["end"]) - step).isoformat(), warmup_bars=config.warmup_bars,
                    cancelled=cancelled, admission_params=config.fixed_params,
                    entry_filters=effective_config(config.entryFilters))
            except (ValueError, OSError) as exc:
                if cancelled() or role == "funded":
                    raise ValueError(f"Period {period['id']} {row['instrument']} preparation: {exc}") from exc
                account.update(status="failed", reason=str(exc))
                record["status"] = "complete_with_comparison_errors"
    plan_cache = GridV2PlanReuseCache()
    for period in periods:
        if total <= 0:
            period["status"] = "not_funded"
            continue
        allocation = positive_capital(total / selector["topN"])
        period.update(opening=total, allocation=allocation, status="complete")
        for account in period["accounts"]:
            if account["id"] not in prepared:
                continue
            checkpoint(f"Executing period {period['id']} {account['role']} {account['instrument']}")
            try:
                data = prepared[account["id"]]
                child_config = deepcopy(config)
                child_config.data_input, child_config.entry_admission = data.facts, data.entry_admission
                child_config.fixed_params['maxEntryLeverage'] = data.entry_admission['maxEntryLeverage']
                child_config.admission_rules = data.admission_rules
                child_config.entry_filter_context, child_config.entry_filter_inputs = data.entry_filter_context, data.entry_filter_inputs
                child_config.fixed_params.update(start=data.facts["bounds"]["evaluation_start"], end=data.facts["bounds"]["evaluation_end"])
                child_config.csv_original_name = f"{account['instrument']}, {timeframe_label(minutes)}"
                child_config.fixed_params["initialCapital"] = reference if wfa else allocation
                identity = {"id": identifier, "period": period["id"], "account": account["id"], "role": account["role"],
                            "rank": account["rank"], "initialCapital": record["initialCapital"], "referenceCapital": reference,
                            "openingCapital": allocation, "selector": selector}
                child_config.portfolio = identity
                if 'entryFiltersDescription' not in record:
                    from .entry_filter_input import describe_saved
                    record['entryFiltersDescription'] = describe_saved(_prepare_study_config_payload(child_config))
                account.update(opening=allocation, windows=[], path=[], trades=0, wins=0)
                if wfa:
                    template = _prepare_study_config_payload(child_config)
                    template["portfolio"] = identity
                    template["portfolio_reference_capital"] = reference
                    template["wfa"] = asdict(wfa)
                    engine = WalkForwardEngine(wfa, template, {}, prepared_input=data)
                    engine._grid_v2_plan_cache = plan_cache
                    window_results, dense_windows, compact_windows, diagnostics = [], [], [], []
                    capital = allocation
                    for window in period["windows"]:
                        checkpoint(f"IS/OOS period {period['id']} window {window['id']} {account['instrument']}")
                        if capital <= 0:
                            account["path"].append([window["end"], capital])
                            continue
                        split = WindowSplit(window["id"], pd.Timestamp(window["isStart"]), pd.Timestamp(window["start"]) - step,
                                            pd.Timestamp(window["start"]), pd.Timestamp(window["end"]) - step)
                        result, execution, dense, compact = engine.run_window(data.frame, split, oos_capital=capital)
                        path = balance_path(execution, window["start"], window["end"], capital, minutes)
                        account["path"].extend(path if not account["path"] else path[1:])
                        capital = path[-1][1]
                        account["windows"].append({"id": window["id"], "start": window["start"], "end": window["end"],
                            "opening": result.oos_execution_capital, "closing": capital, "profit": result.oos_net_profit_pct,
                            "isProfit": result.is_net_profit_pct, "trades": result.oos_total_trades,
                            "wins": result.oos_winning_trades or 0})
                        diagnostics.append((str(window["id"]), (result.entry_diagnostics or {}).get("oos")))
                        window_results.append(result)
                        dense_windows.append(dense)
                        compact_windows.append(compact)
                    stitched = engine._build_stitched_oos_equity(window_results, dense_windows=dense_windows, compact_windows=compact_windows)
                    wf_result = WFResult(wfa, window_results, stitched, config.strategy_id, len(window_results),
                                         pd.Timestamp(period["windows"][0]["isStart"]), pd.Timestamp(period["end"]) - step, config.warmup_bars)
                    children.append((account, "wfa", (wf_result, template)))
                    account["diagnostics"] = aggregate_entries(diagnostics)
                    account["trades"] = sum(w["trades"] for w in account["windows"])
                    account["wins"] = sum(w["wins"] for w in account["windows"])
                else:
                    child_config._portfolio_execution_capital = allocation
                    child_config._portfolio_capture = True
                    results, _ = run_grid_optimization(child_config, save_study=False, grid_v2_plan_cache=plan_cache, prepared_frame=data.frame)
                    execution = results[0].portfolio_execution
                    account["path"] = balance_path(execution, period["start"], period["end"], allocation, minutes)
                    account.update(trades=results[0].total_trades, wins=results[0].winning_trades,
                                   diagnostics=getattr(results[0], "entry_diagnostics", None))
                    children.append((account, "fixed", (child_config, _grid_v2_settings_from_config(child_config), child_config.grid_summary, results)))
                account.update(closing=account["path"][-1][1], status="depleted" if account["path"][-1][1] <= 0 else "complete")
            except Exception as exc:
                if cancelled() or account["role"] == "funded":
                    raise ValueError(f"Period {period['id']} {account['instrument']} execution: {exc}") from exc
                children[:] = [child for child in children if child[0] is not account]
                for key in ("path", "windows", "trades", "wins", "opening", "closing", "diagnostics"):
                    account.pop(key, None)
                account.update(status="failed", reason=str(exc))
                record["status"] = "complete_with_comparison_errors"
        funded = [a for a in period["accounts"] if a["role"] == "funded"]
        for rank in range(len(funded) + 1, selector["topN"] + 1):
            period["accounts"].append({"id": f"{identifier}:{period['id']}:idle:{rank}", "period": period["id"],
                "instrument": None, "role": "idle", "rank": rank, "status": "idle", "opening": allocation,
                "closing": allocation, "path": [[period["start"], allocation], [period["end"], allocation]]})
        total = math.fsum(a["closing"] for a in period["accounts"] if a["role"] in ("funded", "idle"))
        if not math.isfinite(total):
            raise ValueError("Nonfinite portfolio closing balance.")
        period["closing"] = total
        if total <= 0:
            record["status"] = "depleted"
    record["metrics"] = project(record)
    checkpoint("Publishing portfolio")
    if publish:
        from .portfolio_storage import publish_portfolio
        publish_portfolio(record, children, started=started, queue_number=queue_number, cancelled=cancelled)
    return record
