"""Fixed saved contracts and account publication verification. No live lookups."""
from copy import deepcopy
import math
from pathlib import Path

import numpy as np
import pandas as pd

from .. import config as c
from ..dataset import load_json, validate_artifact_record
from .config import DATASET_SCHEMA, REQUEST_SCHEMA, REQUEST_V2, DATASET_V2, SCHEMA_PAIRS, ECONOMICS, RUNTIME, cells, scalar, validate_structure
from .accounting import MONTH_COLUMNS, csv_bytes, monthly, read_curve, validate_curve

RESULT_SCHEMA = "strategy_lab_fixed_result_v1"
RESULT_V2 = "strategy_lab_fixed_result_v2"
RESULT_PAIRS = {DATASET_SCHEMA: RESULT_SCHEMA, DATASET_V2: RESULT_V2}
BACKEND = "generic_v2_reference"
BASIC = ("net_profit", "net_profit_pct", "gross_profit", "gross_loss", "max_drawdown", "max_drawdown_pct",
    "total_trades", "winning_trades", "losing_trades", "win_rate", "avg_win", "avg_loss", "avg_trade", "max_consecutive_losses")
ADVANCED = ("sharpe_ratio", "sortino_ratio", "profit_factor", "romad", "sqn", "ulcer_index", "consistency_score",
    "sharpe_daily", "sharpe_daily_observations", "sharpe_daily_active_days")
METRICS = BASIC + ADVANCED + ("max_drawdown_mtm_pct",)


def safe_path(root, relative):
    """Closed relative paths, including every ancestor (not only final symlinks)."""
    root = Path(root).absolute()
    if not isinstance(relative, str) or any(x in relative for x in ("\\", ":")) or any(p in ("", ".", "..") for p in relative.split("/")):
        raise ValueError("unsafe Fixed artifact path.")
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Fixed path escapes output.")
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError("Fixed artifact symlinks are forbidden.")
        if parent == root:
            break
    return path


def account_identity(raw, account):
    bounds = account["input_pin"]["bounds"]
    execution = raw["generation"]["execution"]
    if raw["schema_version"] == REQUEST_V2:
        from ..filters import effective
        execution = {"parameter_mode": "fixed", "policy": execution["policy"], "entry_filters": effective(raw)}
    return {"strategy": raw["generation"]["strategy"], "execution": execution,
        "params": {k: v for k, v in account["params"].items() if k not in RUNTIME},
        "instrument_id": account["instrument_id"], "timeframe_minutes": account["timeframe_minutes"],
        "bounds": {k: bounds[k] for k in ("warmup_start", "start", "end", "trade_start_idx")}}


def semantic_binding(account):
    from core.engine_v2.admission import read_admission_envelope
    envelope = account["entry_admission"]
    rules = read_admission_envelope(envelope, account["params"]["maxEntryLeverage"])
    return {k: account[k] for k in ("account_id", "params", "input_pin", "entry_filter_inputs", "candidate", "plan_fingerprint")} | {
        "entry_admission": {"version": envelope["version"], "mode": envelope["mode"],
            "maxEntryLeverage": envelope["maxEntryLeverage"], "rules": rules.semantic()}}


def contract_identity(contract):
    return c.canonical_sha256({"request": contract["request"], "resolved": contract["resolved"],
        "accounts": [semantic_binding(a) for a in contract["accounts"]]})


def validate_contract(contract):
    from core import data_input
    from core.engine_v2.admission import read_admission_envelope
    c._keys(contract, "Fixed contract", {"request", "resolved", "accounts"})
    raw = contract["request"]
    validate_structure(raw)
    resolved = contract["resolved"]
    new = raw["schema_version"] == REQUEST_V2
    c._keys(resolved, "resolved", {"parameter_specs", "parameter_sets"} | ({"entry_filters"} if new else set()))
    if new:
        from ..filters import effective, validate_saved
        if resolved["entry_filters"] != effective(raw):
            raise ValueError("saved effective filter contract mismatch.")
    specs = c._object(resolved["parameter_specs"], "parameter_specs")
    if not set(ECONOMICS) <= set(specs) or set(specs) & (RUNTIME | {"contractSize"}):
        raise ValueError("invalid saved scalar contract.")
    for spec in specs.values():
        c._keys(spec, "scalar spec", {"type"}, optional=frozenset({"min", "max", "options"}))
        if spec["type"] not in {"bool", "int", "float", "select", "string"}:
            raise ValueError("unsupported saved scalar type.")
    sets = resolved["parameter_sets"]
    if not isinstance(sets, list) or len(sets) != len(raw["generation"]["parameter_sets"]):
        raise ValueError("saved parameter-set count mismatch.")
    for declared, saved in zip(raw["generation"]["parameter_sets"], sets):
        c._keys(saved, "resolved set", {"name", "params", "variants"})
        if declared["name"] != saved["name"] or set(saved["params"]) != set(specs):
            raise ValueError("saved scalar names/ownership mismatch.")
        c._string_list(saved["variants"], "variants", allow_empty=True)
        for n, v in saved["params"].items():
            scalar(v, n, specs[n])
        supplied = raw["generation"]["economics"]["base_params"] | declared["params"]
        for n, v in supplied.items():
            if n not in specs:
                raise ValueError("unknown saved request parameter.")
            scalar(v, n, specs[n])
        # Core may canonicalize inactive strategy children. Economics are never implicit.
        if any(saved["params"][n] != supplied[n] for n in ECONOMICS):
            raise ValueError("saved effective economics differ from explicit request.")
    accounts = contract["accounts"]
    if not isinstance(accounts, list) or len(accounts) != raw["generation"]["expected_run_count"]:
        raise ValueError("saved account count mismatch.")
    seen = set()
    for a, (ps, tf, period, instrument) in zip(accounts, cells(raw, resolved)):
        c._keys(a, "account", {"account_id", "parameter_set", "period", "instrument_id", "cell", "timeframe_minutes",
            "params", "input_pin", "entry_admission", "entry_filter_inputs", "candidate", "plan_fingerprint", "provenance"})
        labels = {"parameter_set": ps["name"], "period": period["name"], **instrument, "timeframe_minutes": tf}
        if any(a[k] != v for k, v in labels.items()):
            raise ValueError("saved account order/labels mismatch.")
        pin = a["input_pin"]
        if pin["source"] != {"type": "parquet", "version": 1, "instrument_id": instrument["instrument_id"], "timeframe_minutes": tf}:
            raise ValueError("saved source ownership mismatch.")
        local = deepcopy(pin)
        local["source"]["pack_root"] = str(Path.cwd().resolve())
        data_input.validate_pin(local)  # Structural only, never loads this locator.
        b = pin["bounds"]
        requested = {"dateFilter": True, "start": period["start"], "end": period["end"], "warmupBars": period["warmup_bars"]}
        if b["requested"] != requested or b["trade_start_idx"] != period["warmup_bars"] or b["start_origin"] != "user" or b["end_origin"] != "user":
            raise ValueError("saved bounds/warmup ownership mismatch.")
        if (data_input._timestamp(period["start"]) != pd.Timestamp(b["evaluation_start"]) or
                data_input._timestamp(period["end"], end=True) != pd.Timestamp(b["evaluation_end"])):
            raise ValueError("saved requested/effective bounds mismatch.")
        expected = ps["params"] | {"dateFilter": True, "start": b["evaluation_start"], "end": b["evaluation_end"], "warmupBars": b["trade_start_idx"]}
        if a["params"] != expected:
            raise ValueError("saved execution scalars/runtime binding mismatch.")
        rules = read_admission_envelope(a["entry_admission"], expected["maxEntryLeverage"])
        if rules.instrument_id != instrument["instrument_id"] or pin["instrument"]["instrument_id"] != instrument["instrument_id"]:
            raise ValueError("saved admission instrument mismatch.")
        if new:
            validate_saved(raw["generation"]["execution"]["entryFilters"], resolved["entry_filters"], a["entry_filter_inputs"], pin)
        elif a["entry_filter_inputs"] != {"version": 1, "state": "off"}:
            raise ValueError("saved Fixed context must be Off.")
        candidate = a["candidate"]
        c._keys(candidate, "candidate", {"count", "variant", "semantic_key", "params"})
        if type(candidate["count"]) is not int or candidate["count"] != 1 or candidate["params"] != expected:
            raise ValueError("saved singleton geometry mismatch.")
        c._text(candidate["variant"], "variant")
        c._text(candidate["semantic_key"], "semantic_key")
        if ps["variants"] and ps["variants"] != [candidate["variant"]]:
            raise ValueError("saved variant mismatch.")
        c._sha256(a["plan_fingerprint"], "plan_fingerprint")
        if a["account_id"] != c.canonical_sha256(account_identity(raw, a)) or a["account_id"] in seen:
            raise ValueError("account identity collision or mismatch.")
        seen.add(a["account_id"])
    c.canonical_json_bytes(contract)


def verify_record(root, record, expected):
    c._keys(record, "artifact", {"path", "size", "sha256"})
    path = safe_path(root, expected)
    if record["path"] != expected or not validate_artifact_record(root, record):
        raise ValueError(f"claimed Fixed artifact failed verification: {expected}.")
    return path


def read_account(root, account, claim, result_schema=RESULT_SCHEMA):
    """Verify one claimed result and its arithmetic, retaining only this curve."""
    prefix = "accounts/" + account["account_id"] + "/"
    c._keys(claim, "completed account", {"result.json", "curve.parquet", "monthly.csv"})
    paths = {name: verify_record(root, row, prefix+name) for name, row in claim.items()}
    result = load_json(paths["result.json"], "Fixed result")
    c._keys(result, "result", {"schema_version", "account", "backend", "initial_capital", "metric_anchor",
        "prepared_trade_start_idx", "curve_start_idx", "observation_count", "metrics", "metric_units",
        "metric_unavailable_reasons", "monthly_return_observations", "guardrails", "final_state", "entry_diagnostics",
        "artifacts", "provenance"})
    capital = account["params"]["initialCapital"]
    if (result_schema not in RESULT_PAIRS.values() or result["schema_version"] != result_schema or result["account"] != account or result["backend"] != BACKEND or
            result["initial_capital"] != capital or result["metric_anchor"] != capital or result["curve_start_idx"] != 0 or
            result["prepared_trade_start_idx"] != account["input_pin"]["bounds"]["trade_start_idx"]):
        raise ValueError("saved Fixed result identity/anchor/backend mismatch.")
    if result["artifacts"] != {n: claim[n] for n in ("curve.parquet", "monthly.csv")}:
        raise ValueError("result artifact ownership mismatch.")
    values = read_curve(paths["curve.parquet"])
    ts, equity, balance = values
    b = account["input_pin"]["bounds"]
    validate_curve(*values, start=b["start"], end=b["end"], timeframe=account["timeframe_minutes"])
    if type(result["observation_count"]) is not int or result["observation_count"] != len(ts):
        raise ValueError("saved observation count mismatch.")
    rows = monthly(*values, capital, account["timeframe_minutes"])
    if paths["monthly.csv"].read_bytes() != csv_bytes(rows, MONTH_COLUMNS):
        raise ValueError("saved monthly arithmetic differs from curve.")
    metrics = result["metrics"]
    if set(metrics) != set(METRICS) or set(result["metric_units"]) != set(METRICS):
        raise ValueError("saved metric schema mismatch.")
    for name, value in metrics.items():
        if value is not None:
            c._number(value, name)
    for name in ("total_trades", "winning_trades", "losing_trades", "max_consecutive_losses"):
        c._integer(metrics[name], name)
    if metrics["winning_trades"] + metrics["losing_trades"] > metrics["total_trades"]:
        raise ValueError("saved trade counts are inconsistent.")
    if set(result["metric_unavailable_reasons"]) != {n for n,v in metrics.items() if v is None}:
        raise ValueError("saved metric availability mismatch.")
    for reason in result["metric_unavailable_reasons"].values():
        c._text(reason, "metric unavailable reason")
    if result["monthly_return_observations"] != sum(r["equity_return_pct"] is not None for r in rows):
        raise ValueError("saved monthly observation count mismatch.")
    for name, expected in (("net_profit", balance[-1]-capital), ("net_profit_pct", 100*(balance[-1]-capital)/capital)):
        if metrics[name] is None or not math.isclose(metrics[name], expected, rel_tol=1e-10, abs_tol=1e-9):
            raise ValueError("saved realized P&L differs from curve.")
    state = result["final_state"]
    if state.get("position_direction") != 0 or state.get("position_size") != 0 or not np.isclose(equity[-1], balance[-1], rtol=1e-10, atol=1e-9):
        raise ValueError("Fixed strict-close account is not flat.")
    if type(result["guardrails"].get("flags")) is not int or result["guardrails"]["flags"] & 256:
        raise ValueError("saved account has execution-error guardrail.")
    c.canonical_json_bytes(result)
    return result, values, rows


def open_dataset(root, *, allow_partial=False, on_account=None):
    root = Path(root).absolute()
    path = safe_path(root, "manifest.json")
    if not path.exists():
        path = safe_path(root, "manifest.partial.json")
    m = deepcopy(load_json(path, "Fixed manifest"))
    if m.get("schema_version") not in RESULT_PAIRS or SCHEMA_PAIRS.get(m.get("contract", {}).get("request", {}).get("schema_version")) != m["schema_version"]:
        raise ValueError("unsupported Fixed dataset/request schema pair.")
    c._keys(m, "manifest", {"schema_version", "complete", "contract", "request_sha256", "contract_sha256", "completed", "failure"})
    if type(m["complete"]) is not bool or m["complete"] != (path.name == "manifest.json"):
        raise ValueError("Fixed completion marker mismatch.")
    validate_contract(m["contract"])
    if m["request_sha256"] != c.canonical_sha256(m["contract"]["request"]) or m["contract_sha256"] != contract_identity(m["contract"]):
        raise ValueError("Fixed frozen identity mismatch.")
    accounts = m["contract"]["accounts"]
    completed = c._object(m["completed"], "completed")
    ids = [a["account_id"] for a in accounts]
    if set(completed) != set(ids[:len(completed)]):
        raise ValueError("completed accounts must be a declared sequential prefix.")
    if m["failure"] is not None:
        c._keys(m["failure"], "failure", {"account_id", "reason"})
        if len(completed) == len(ids) or m["failure"]["account_id"] != ids[len(completed)]:
            raise ValueError("failure must belong to first unclaimed account.")
        c._text(m["failure"]["reason"], "failure reason")
    if m["complete"] and (len(completed) != len(ids) or m["failure"] is not None):
        raise ValueError("incomplete account coverage in complete Fixed manifest.")
    for a in accounts:
        if a["account_id"] in completed:
            result, curve, rows = read_account(root, a, completed[a["account_id"]], RESULT_PAIRS[m["schema_version"]])
            if on_account is not None:
                on_account(deepcopy(a), deepcopy(result), deepcopy(rows))
            del result, curve, rows
    if not m["complete"] and not allow_partial:
        raise ValueError("Fixed dataset is partial; explicitly use --allow-partial.")
    return m
