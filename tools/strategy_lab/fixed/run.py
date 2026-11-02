"""One-account-at-a-time preparation, reference execution and durable claims."""
from copy import deepcopy
from dataclasses import asdict
import importlib
from pathlib import Path
import platform
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import canonical_sha256
from ..dataset import atomic_write, atomic_write_json, artifact_record
from ..parquet import portable_pin, replay
from .config import DATASET_SCHEMA, cells, load
from .accounting import CURVE_SCHEMA, MONTH_COLUMNS, csv_bytes, finite_json, monthly, validate_curve
from .storage import (BACKEND, RESULT_SCHEMA, account_identity, contract_identity, open_dataset,
    read_account, safe_path, validate_contract)
from .report import account_html, write_index


def peak_rss_bytes():
    """Native lifetime process peak, not an input budget or per-account maximum."""
    try:
        if platform.system() == "Windows":
            import ctypes
            class Counters(ctypes.Structure):
                _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
                    (n, ctypes.c_size_t) for n in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
            counters = Counters()
            counters.cb = ctypes.sizeof(counters)
            if not ctypes.windll.psapi.GetProcessMemoryInfo(ctypes.c_void_p(-1), ctypes.byref(counters), counters.cb):
                return None
            return counters.PeakWorkingSetSize
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value * (1 if platform.system() == "Darwin" else 1024))
    except (ImportError, OSError, AttributeError):
        return None


def build_execution(config, parameter_set, prepared):
    from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan
    from core.parameter_mode import require_singleton
    from strategies import get_strategy
    b = prepared.facts["bounds"]
    params = parameter_set["params"] | {"dateFilter":True, "start":b["evaluation_start"], "end":b["evaluation_end"], "warmupBars":prepared.trade_start_idx,
        "maxEntryLeverage":prepared.entry_admission["maxEntryLeverage"]}
    variants = parameter_set["variants"]
    plan = build_grid_v2_plan(config, GridV2Settings(enabled_axes=(), enabled_variants=tuple(variants) if variants else None,
        admission_rules=prepared.admission_rules, prefer_compiled=False, slow_enrich_selected=False), params)
    require_singleton(plan)
    if any(d.get("severity") in ("warning", "error") for d in plan.metadata.get("diagnostics", ())):
        raise ValueError("Fixed singleton plan has warning/error diagnostics.")
    module = importlib.import_module(get_strategy(config["id"]).__module__)
    hooks = GridV2StrategyHooks.from_strategy(module, config=config, entry_filter_context=prepared.entry_filter_context)
    effective = dict(plan.candidate_table.params_for_index(0))
    if hooks.normalize_params:
        effective = hooks.normalize_params(effective)
    if effective != params:
        raise ValueError("singleton execution changed resolved Fixed scalars/runtime binding.")
    candidate = {"count":1, "variant":plan.candidate_table.variant_name_for_index(0),
        "semantic_key":plan.candidate_table.semantic_key_for_index(0), "params":effective}
    return plan, hooks, effective, candidate


def prepare_account(raw, config, cell, root):
    from core import data_input
    ps, timeframe, period, instrument = cell
    started = time.perf_counter()
    prepared = data_input.prepare_input({"type":"parquet", "version":1, "pack_root":str(root),
        "instrument_id":instrument["instrument_id"], "timeframe_minutes":timeframe}, date_filter=True,
        start=period["start"], end=period["end"], warmup_bars=period["warmup_bars"], admission_params=ps["params"], entry_filters=[])
    plan, _, params, candidate = build_execution(config, ps, prepared)
    account = {"parameter_set":ps["name"], "period":period["name"], **instrument, "timeframe_minutes":timeframe,
        "params":params, "input_pin":portable_pin(prepared.facts), "entry_admission":prepared.entry_admission,
        "entry_filter_inputs":prepared.entry_filter_inputs, "candidate":candidate, "plan_fingerprint":plan.plan_fingerprint,
        "provenance":{"pack_root":str(root), "preparation_seconds":time.perf_counter()-started,
            "consumed_rows":len(prepared.frame), "process_peak_rss_bytes":peak_rss_bytes()}}
    account["account_id"] = canonical_sha256(account_identity(raw, account))
    return account  # No frame, plan, hooks or execution arrays escape this call.


def execute_account(config, parameter_set, account, root):
    from core.engine_v2.runner import run_v2_strategy
    from core.engine_v2.contracts import GUARDRAIL_FLAG_EXECUTION_ERROR
    prepared = replay(account, root)
    plan, hooks, params, candidate = build_execution(config, parameter_set, prepared)
    if plan.plan_fingerprint != account["plan_fingerprint"] or candidate != account["candidate"]:
        raise ValueError("consumed candidate geometry differs from frozen plan.")
    started = time.perf_counter()
    result = run_v2_strategy(data=hooks.build_execution_data(prepared.frame, params), profile=plan.profile,
        params=params, trade_start_idx=prepared.trade_start_idx, admission_rules=prepared.admission_rules,
        entry_filter_context=prepared.entry_filter_context, compute_sharpe_daily=True, compute_max_drawdown_mtm=True)
    if result.guardrail_summary.flags & GUARDRAIL_FLAG_EXECUTION_ERROR:
        raise ValueError("Fixed reference execution-error guardrail.")
    if result.standing_state.position_direction != 0 or result.standing_state.position_size != 0:
        raise ValueError("Fixed strict_close execution did not finish flat.")
    return result, {"execution_seconds":time.perf_counter()-started, "consumed_rows":len(prepared.frame),
        "process_peak_rss_bytes":peak_rss_bytes(), "platform":platform.platform(), "python":platform.python_version()}


def publish_account(output, account, execution, provenance):
    sr = execution.strategy_result
    start = account["input_pin"]["bounds"]["trade_start_idx"]
    capital = account["params"]["initialCapital"]
    if sr.metric_start_idx != start or sr.metric_initial_equity != capital:
        raise ValueError("reference metric anchor/boundary mismatch.")
    if not len(sr.timestamps) == len(sr.equity_curve) == len(sr.balance_curve):
        raise ValueError("reference whole-run accounting shape mismatch.")
    ts = pd.DatetimeIndex(sr.timestamps[start:]).as_unit("ns").asi8
    equity, balance = (np.asarray(v[start:], dtype="float64") for v in (sr.equity_curve, sr.balance_curve))
    b = account["input_pin"]["bounds"]
    validate_curve(ts, equity, balance, start=b["start"], end=b["end"], timeframe=account["timeframe_minutes"])
    rows = monthly(ts, equity, balance, capital, account["timeframe_minutes"])
    metrics = finite_json(asdict(execution.basic_metrics) | asdict(execution.advanced_metrics) |
                          {"max_drawdown_mtm_pct":execution.max_drawdown_mtm_pct})
    units = {n:("percent" if n.endswith("_pct") or n == "win_rate" else "count" if n in (
        "total_trades", "winning_trades", "losing_trades", "max_consecutive_losses", "sharpe_daily_observations", "sharpe_daily_active_days")
        else "account_currency" if n in ("net_profit", "gross_profit", "gross_loss", "max_drawdown", "avg_win", "avg_loss", "avg_trade") else "ratio") for n in metrics}
    prefix = "accounts/" + account["account_id"] + "/"
    paths = {n:safe_path(output, prefix+n) for n in ("curve.parquet", "monthly.csv", "result.json", "report.html")}
    curve = pa.Table.from_arrays([pa.array(ts), pa.array(equity), pa.array(balance)], schema=CURVE_SCHEMA)
    atomic_write(paths["curve.parquet"], lambda p:pq.write_table(curve, p, compression="zstd"))
    atomic_write(paths["monthly.csv"], lambda p:p.write_bytes(csv_bytes(rows, MONTH_COLUMNS)))
    artifacts = {n:artifact_record(paths[n], output) for n in ("curve.parquet", "monthly.csv")}
    payload = {"schema_version":RESULT_SCHEMA, "account":account, "backend":BACKEND, "initial_capital":capital,
        "metric_anchor":capital, "prepared_trade_start_idx":start, "curve_start_idx":0, "observation_count":len(ts),
        "metrics":metrics, "metric_units":units, "metric_unavailable_reasons":{n:("no_completed_trades" if not metrics["total_trades"]
            else "unavailable_core_metric") for n,v in metrics.items() if v is None},
        "monthly_return_observations":sum(r["equity_return_pct"] is not None for r in rows),
        "guardrails":finite_json(asdict(execution.guardrail_summary)), "final_state":finite_json(asdict(execution.standing_state)),
        "entry_diagnostics":finite_json(sr.entry_diagnostics), "artifacts":artifacts, "provenance":provenance}
    atomic_write_json(paths["result.json"], payload)
    claim = {**artifacts, "result.json":artifact_record(paths["result.json"], output)}
    # Validate before publishing the manifest claim. Unclaimed files are recoverable.
    read_account(output, account, claim)
    account_html(output, account, claim, paths["report.html"])
    return claim


def run(request, data_root, output, *, resume=False, preflight_only=False):
    raw, config, resolved = load(request)
    root, output = Path(data_root).resolve(), Path(output).absolute()
    if output.resolve().is_relative_to(root) or root.is_relative_to(output.resolve()):
        raise ValueError("Fixed output must not overlap the market root.")
    partial, complete = (safe_path(output, n) for n in ("manifest.partial.json", "manifest.json"))
    existing = None
    if partial.exists() or complete.exists():
        existing = open_dataset(output, allow_partial=True)
        if not existing["complete"] and not resume:
            raise ValueError("partial Fixed dataset requires explicit --resume.")
        if existing["contract"]["request"] != raw or existing["contract"]["resolved"] != resolved:
            raise ValueError("Fixed request/effective scalar contract changed; use a new output.")
    elif output.exists() and any(output.iterdir()):
        raise ValueError("refusing nonempty output without a Fixed manifest.")
    started = time.perf_counter()
    accounts, seen = [], set()
    for cell in cells(raw, resolved):
        a = prepare_account(raw, config, cell, root)
        if a["account_id"] in seen:
            raise ValueError("duplicate account identity after timeframe grid resolution.")
        seen.add(a["account_id"])
        accounts.append(a)
    contract = {"request":raw, "resolved":resolved, "accounts":accounts}
    validate_contract(contract)
    if existing is not None:
        if contract_identity(contract) != existing["contract_sha256"]:
            raise ValueError("frozen input/rules/geometry changed; resume refused.")
        m = existing  # Keep producing provenance, bytes and timestamps unchanged.
        if m["complete"]:
            return {"complete":True, "no_op":True, "completed":len(accounts), "accounts":len(accounts), "seconds":time.perf_counter()-started}
    else:
        m = {"schema_version":DATASET_SCHEMA, "complete":False, "contract":contract,
            "request_sha256":canonical_sha256(raw), "contract_sha256":contract_identity(contract), "completed":{}, "failure":None}
        atomic_write_json(partial, m)
        write_index(output, m)
    if preflight_only:
        return {"complete":False, "completed":len(m["completed"]), "accounts":len(accounts), "preflight_seconds":time.perf_counter()-started}
    sets = {p["name"]:p for p in resolved["parameter_sets"]}
    for a in m["contract"]["accounts"]:
        ident = a["account_id"]
        if ident in m["completed"]:
            continue
        try:
            execution, provenance = execute_account(config, sets[a["parameter_set"]], a, root)
            claim = publish_account(output, a, execution, provenance)
            del execution
            # Mutate a copy so a failed claim write cannot be mistaken for publication.
            claimed = deepcopy(m)
            claimed["completed"][ident] = claim
            claimed["failure"] = None
            atomic_write_json(partial, claimed)
            m = claimed
        except (Exception, KeyboardInterrupt) as exc:
            m["failure"] = {"account_id":ident, "reason":f"{type(exc).__name__}: {exc}"}
            atomic_write_json(partial, m)
            write_index(output, m)
            raise
        write_index(output, m)
    m["complete"] = True
    atomic_write_json(complete, m)
    return {"complete":True, "no_op":False, "completed":len(accounts), "accounts":len(accounts), "seconds":time.perf_counter()-started}
