"""Version-one sequential tables: explicit physical types and shared checks."""
from decimal import Decimal
import math

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from .. import PatternLabDataError
from . import contracts

SCHEMA_VERSION = 1
KEY = ("instrument_id", "timeframe_minutes", "variant_id", "model_instance_id", "case_id")
REASONS = ("no_next_bar", "occupied", "reentry_suppressed", "indicator_unavailable",
           "invalid_risk_distance", "nonpositive_capital", "zero_quantity", "below_min_quantity",
           "below_min_notional", "leverage_undefined", "leverage_cap_exceeded", "filled")
_COMMON = [(k, pa.int64() if k == "timeframe_minutes" else pa.string(), False) for k in KEY]


def _schema(strings, integers, floats, booleans="", nullable=()):
    fields = _COMMON + [(k, t, k in nullable) for names,t in
                        ((strings,pa.string()), (integers,pa.int64()), (floats,pa.float64()), (booleans,pa.bool_())) for k in names.split()]
    return pa.schema([pa.field(*item) for item in fields], metadata={b"sequential_schema_version":b"1"})


SCHEMAS = {
    "attempts": _schema("event_id reason trade_id", "anchor_open_ms signal_index intended_fill_ms fill_index lots",
        "anchor_price stop target risk_distance quantity pre_entry_balance proposed_fee notional required_leverage",
        nullable="trade_id fill_index lots anchor_price stop target risk_distance quantity proposed_fee notional required_leverage".split()),
    "trades": _schema("trade_id event_id direction instrument_quantity exit_phase exit_reason",
        "signal_index entry_index exit_index entry_time_ms exit_bar_open_ms exit_bar_end_ms holding_ms lots",
        "entry_price exit_price quantity stop target planned_cash_risk entry_fee exit_fee gross_pnl net_pnl net_r entry_leverage",
        "ambiguous"),
    "path": _schema("", "bar_index bar_open_ms segment position_direction",
        "close_price balance equity position_quantity entry_price", "segment_end study_end", nullable=["entry_price"]),
}


def frame(name, rows):
    return pa.Table.from_pylist(rows, schema=SCHEMAS[name]).to_pandas(types_mapper=pd.ArrowDtype)


def check_physical(table, name):
    if not table.schema.equals(SCHEMAS[name], check_metadata=False) or (table.schema.metadata or {}).get(b"sequential_schema_version") != b"1":
        raise PatternLabDataError(f"sequential {name}: physical Arrow schema/version mismatch")
    for field, column in zip(SCHEMAS[name], table.columns):
        if not field.nullable and column.null_count:
            raise PatternLabDataError(f"sequential {name}.{field.name}: null in required field")
        if pa.types.is_floating(field.type):
            if pc.all(pc.fill_null(pc.is_finite(column), True)).as_py() is False:
                raise PatternLabDataError(f"sequential {name}.{field.name}: nonfinite value")


def checked_table(table, name):
    try:
        actual = pa.Table.from_pandas(table, schema=SCHEMAS[name], preserve_index=False)
        if list(table.columns) != SCHEMAS[name].names:
            raise ValueError("column order/coverage mismatch")
        check_physical(actual, name)
        return actual
    except (ValueError, TypeError, pa.ArrowException) as exc:
        raise PatternLabDataError(f"sequential {name}: invalid typed evidence: {exc}") from exc


def validate(tables, **kwargs):
    from .sequential_checks import validate as check
    return check(tables, **kwargs)


def expected_bars(conditions, stats, end_ms):
    """Recover evaluated observed bars from all-anchor coverage and job counts."""
    result = {}
    for text,facts in stats["timeframes"].items():
        tf=int(text)
        bars=sorted(set(map(int,conditions.loc[conditions.timeframe_minutes==tf,"anchor_open_ms"])))
        count=facts["research_bar_count"]
        final=end_ms-tf*60000
        if len(bars)+1==count and final not in bars:
            bars.append(final)
        if len(bars)!=count or bars!=sorted(bars):
            raise PatternLabDataError("Sequential all-anchor/evaluated-bar coverage contradiction")
        result[tf]=bars
    return result


def summaries(grouped, instances, *, requested_end_ms=None):
    cases={(m["model_instance_id"],int(tf),c["case_id"]):c["parameters"]
           for m in instances for tf,cs in m["cases"].items() for c in cs}
    result=[]
    def describe(values):
        return {"count":len(values),"mean":float(np.mean(values)) if values else None,
                "median":float(np.median(values)) if values else None,
                "min":min(values) if values else None,"max":max(values) if values else None}
    def drawdown(values, initial):
        peak=initial
        maximum=0.
        for value in values:
            peak=max(peak,value)
            maximum=max(maximum,(peak-value)/peak*100)
        return maximum
    for key,attempts in grouped["attempts"].items():
        trades,path=grouped["trades"][key],grouped["path"][key]
        settings=cases[(key[3],key[1],key[4])]
        counts={reason:sum(a["reason"]==reason for a in attempts) for reason in REASONS}
        finite_cap=counts["filled"]+counts["leverage_cap_exceeded"]
        required=[a["required_leverage"] for a in attempts if a["reason"] in ("filled","leverage_cap_exceeded")]
        used=[t["entry_leverage"] for t in trades]
        winning=[t["net_pnl"] for t in trades if t["net_pnl"]>0]
        losing=[t["net_pnl"] for t in trades if t["net_pnl"]<0]
        initial=settings["initial_capital"]
        final=float(path["balance"][-1]) if len(path["balance"]) else initial
        result.append({**dict(zip(KEY,key)),"settings":settings,**({"coverage":coverage(path,trades,key[1],requested_end_ms)} if requested_end_ms is not None else {}),"signals":len(attempts),"dispositions":counts,
            "completed_trades":len(trades),"wins":len(winning),"losses":len(losing),
            "breakeven":len(trades)-len(winning)-len(losing),"win_rate":len(winning)/len(trades) if trades else None,
            "initial_capital":initial,"final_capital":final,"gross_pnl":sum(t["gross_pnl"] for t in trades),
            "net_pnl":sum(t["net_pnl"] for t in trades),"total_fees":sum(t["entry_fee"]+t["exit_fee"] for t in trades),
            "return_pct":(final-initial)/initial*100,
            "profit_factor":sum(winning)/-sum(losing) if losing else None,
            "profit_factor_status":"available" if losing else "no_losses" if winning else "no_wins_or_losses",
            "net_planned_r":describe([t["net_r"] for t in trades]),
            "holding_ms":describe([t["holding_ms"] for t in trades]),
            "realized_balance_drawdown_pct":drawdown(path["balance"],initial),
            "bar_close_mtm_drawdown_pct":drawdown(path["equity"],initial),
            "finite_cap_attempt_count":finite_cap,"cap_rejection_share":counts["leverage_cap_exceeded"]/finite_cap if finite_cap else None,
            "max_attempt_required_leverage":max(required) if required else None,
            "max_executed_entry_leverage":max(used) if used else None,
            "stop_width_pct":describe([a["risk_distance"]/a["anchor_price"]*100 for a in attempts if a["risk_distance"] is not None and a["risk_distance"]>0 and a["anchor_price"]>0]),
            "exits":{reason:sum(t["exit_reason"]==reason for t in trades) for reason in ("stop","target","expiry","terminal","gap_boundary")},
            "gap_open_exits":sum(t["exit_phase"]=="open" and t["exit_reason"] in ("stop","target") for t in trades),
            "ambiguous_trades":sum(t["ambiguous"] for t in trades),
            "ambiguous_bars":len({t["exit_index"] for t in trades if t["ambiguous"]})})
    return result


def coverage(path, trades, timeframe_minutes, requested_end_ms):
    """Derived tail facts; raw v1 terminal flags still mean last observed row."""
    step = timeframe_minutes * 60000
    stamps = path["bar_open_ms"]
    last_close = int(stamps[-1])+step if len(stamps) else None
    missing = None
    if last_close is not None:
        gap = requested_end_ms-last_close
        if gap < 0 or gap % step:
            raise PatternLabDataError("Sequential coverage: impossible requested end/path bounds")
        missing = gap//step
    return dict(requested_end_ms=requested_end_ms, last_observed_close_ms=last_close,
                tail_complete=last_close == requested_end_ms, missing_tail_slots=missing,
                terminal_exits_before_requested_end=sum(t["exit_reason"] == "terminal" and
                    t["exit_bar_end_ms"] < requested_end_ms for t in trades))
