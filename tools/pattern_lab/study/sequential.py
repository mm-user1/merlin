"""Version-one sequential tables: explicit physical types and shared checks."""
from decimal import Decimal
import math

import numpy as np
import pandas as pd
import pyarrow as pa

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
            if any(x is not None and not math.isfinite(x) for x in column.to_pylist()):
                raise PatternLabDataError(f"sequential {name}.{field.name}: nonfinite value")


def _records(table, name):
    try:
        actual = pa.Table.from_pandas(table, schema=SCHEMAS[name], preserve_index=False)
        if list(table.columns) != SCHEMAS[name].names:
            raise ValueError("column order/coverage mismatch")
        check_physical(actual, name)
        return actual.to_pylist()
    except (ValueError, TypeError, pa.ArrowException) as exc:
        raise PatternLabDataError(f"sequential {name}: invalid typed evidence: {exc}") from exc


def validate(tables, *, instrument_id, instances, variants, emissions, expected_bars, rules):
    """Check saved consistency, not engine replay or adversarial authenticity.

    Money reconciliation uses abs_tol=1e-8, rel_tol=1e-9. IDs, integer counts,
    admission comparisons and membership are exact.
    """
    def require(ok, message):
        if not ok:
            raise PatternLabDataError(f"{instrument_id}: sequential evidence: {message}")
    def same(a,b):
        return a is not None and b is not None and math.isclose(a,b, rel_tol=1e-9, abs_tol=1e-8)
    accounts = {}
    for instance in instances:
        require(contracts.is_sequential(instance), "unsupported model contract")
        for timeframe,cases in instance["cases"].items():
            for variant in variants:
                for case in cases:
                    accounts[(instrument_id,int(timeframe),variant["variant_id"],instance["model_instance_id"],case["case_id"])] = case["parameters"]
    grouped = {name:{key:[] for key in accounts} for name in SCHEMAS}
    for name in SCHEMAS:
        require(name in tables, f"missing {name} table")
        for row in _records(tables[name], name):
            key = tuple(row[k] for k in KEY)
            require(key in accounts, f"unknown account in {name}")
            grouped[name][key].append(row)
    events = {(int(row.timeframe_minutes), row.variant_id, row.event_id):int(row.anchor_open_ms)
              for row in emissions.itertuples()}
    for key, settings in accounts.items():
        attempts, trades, path = [grouped[name][key] for name in SCHEMAS]
        expected = {e:stamp for (tf,v,e),stamp in events.items() if (tf,v)==(key[1],key[2])}
        require(len(attempts)==len(expected) and {a["event_id"] for a in attempts}==set(expected), "duplicate/missing attempts")
        require([p["bar_open_ms"] for p in path] == list(expected_bars[key[1]]), "missing/unordered account path bars")
        require(len({p["bar_index"] for p in path})==len(path), "duplicate path indices")
        by_bar = {p["bar_index"]:p for p in path}
        for index,p in enumerate(path):
            last=index==len(path)-1
            boundary=last or path[index+1]["bar_open_ms"]-p["bar_open_ms"]!=key[1]*60000
            require(p["segment_end"]==boundary and p["study_end"]==last, "path boundary flags")
            if not last:
                following=path[index+1]
                require(following["bar_index"]==p["bar_index"]+1 and following["segment"]==p["segment"]+int(boundary), "path segment/index chronology")
        by_trade = {t["trade_id"]:t for t in trades}
        require(len(by_trade)==len(trades), "duplicate trades")
        filled = [a for a in attempts if a["reason"]=="filled"]
        require(len(filled)==len(trades) and {a["trade_id"] for a in filled}==set(by_trade), "filled/trade links disagree")
        fee_rate = settings["commission_pct_per_side"]/100
        for a in attempts:
            require(a["reason"] in REASONS, "unknown terminal reason")
            require(a["anchor_open_ms"]==expected[a["event_id"]] and a["intended_fill_ms"]==a["anchor_open_ms"]+key[1]*60000, "attempt event timing")
            require(a["signal_index"] in by_bar and by_bar[a["signal_index"]]["bar_open_ms"]==a["anchor_open_ms"], "attempt signal index")
            require((a["trade_id"] is not None)==(a["reason"]=="filled"), "rejected attempt links trade")
            signal=by_bar[a["signal_index"]]
            previous=by_bar.get(a["signal_index"]-1)
            had_position=any(t["entry_index"]<=a["signal_index"]<=t["exit_index"] for t in trades)
            priority=("no_next_bar" if signal["segment_end"] else "occupied" if signal["position_direction"] else
                      "reentry_suppressed" if had_position or (previous and previous["segment"]==signal["segment"] and previous["position_direction"]) else None)
            require(priority==a["reason"] if priority else a["reason"] not in ("no_next_bar","occupied","reentry_suppressed"), "signal disposition priority")
            require(same(a["pre_entry_balance"],signal["balance"]), "attempt capital differs from signal close")
            if a["lots"] is not None:
                require(0<=a["lots"]<=2**53 and same(a["quantity"],a["lots"]*float(rules["base_step"])), "integer lot reconstruction")
                require((a["reason"]=="zero_quantity")==(a["lots"]==0), "zero quantity disposition")
                if a["lots"]>0:
                    require((a["reason"]=="below_min_quantity")==(a["lots"]<rules["minimum_lots"]), "minimum quantity disposition")
                require(a["risk_distance"] is not None and a["risk_distance"]>0, "planned distance")
                sign=1 if settings["direction"]=="long" else -1
                require(same(a["risk_distance"],sign*(a["anchor_price"]-a["stop"])) and
                        same(a["target"],a["anchor_price"]+sign*settings["reward_risk"]*a["risk_distance"]), "planned levels")
            if a["reason"] in ("filled","leverage_cap_exceeded"):
                require(a["fill_index"] in by_bar and a["fill_index"]==a["signal_index"]+1, "noncontiguous fill")
                require(a["notional"] is not None and same(a["proposed_fee"],a["notional"]*fee_rate), "proposed fee")
                denominator = a["pre_entry_balance"]-a["proposed_fee"]
                require(denominator>0 and same(a["required_leverage"],a["notional"]/denominator), "required leverage")
                require((a["required_leverage"]<=settings["max_leverage"])==(a["reason"]=="filled"), "cap admission")
                require(rules["minimum_notional"] is None or a["notional"]>=float(rules["minimum_notional"]), "minimum notional admission")
            if a["reason"]=="below_min_notional":
                require(rules["minimum_notional"] is not None and a["notional"] is not None and
                        a["notional"]<float(rules["minimum_notional"]), "minimum notional disposition")
            if a["reason"]=="leverage_undefined":
                require(a["required_leverage"] is None, "undefined leverage must be null")
            if a["reason"]=="nonpositive_capital":
                require(a["pre_entry_balance"]<=0, "nonpositive capital disposition")
            if a["reason"]=="filled":
                t = by_trade[a["trade_id"]]
                require(t["event_id"]==a["event_id"] and t["signal_index"]==a["signal_index"] and t["entry_index"]==a["fill_index"], "trade origin link")
                for field in ("quantity","lots","stop","target"):
                    require(t[field]==a[field], "trade plan changed")
                require(same(t["planned_cash_risk"],a["quantity"]*a["risk_distance"]), "planned risk")
                require(t["entry_leverage"]==a["required_leverage"], "trade leverage")
        for t in trades:
            require(t["direction"]==settings["direction"], "trade direction")
            require(t["exit_phase"] in ("open","intrabar","close") and t["exit_reason"] in ("stop","target","expiry","terminal","gap_boundary"), "exit attribution")
            require(t["entry_index"] in by_bar and t["exit_index"] in by_bar and t["entry_index"]<=t["exit_index"], "trade chronology")
            entry,exit_ = by_bar[t["entry_index"]],by_bar[t["exit_index"]]
            require(entry["segment"]==exit_["segment"], "trade crosses a gap")
            require(t["entry_time_ms"]==entry["bar_open_ms"] and t["exit_bar_open_ms"]==exit_["bar_open_ms"] and t["exit_bar_end_ms"]==exit_["bar_open_ms"]+key[1]*60000, "trade times")
            require(t["holding_ms"]==t["exit_bar_open_ms"]+(key[1]*60000 if t["exit_phase"]=="close" else 0)-t["entry_time_ms"], "duration clock")
            require(t["quantity"]>0 and t["planned_cash_risk"]>0, "nonpositive trade size/risk")
            require(Decimal(t["instrument_quantity"])==Decimal(t["lots"])*Decimal(rules["quantity_step"]), "instrument quantity units")
            gross=(t["exit_price"]-t["entry_price"])*t["quantity"]*(1 if t["direction"]=="long" else -1)
            require(same(t["gross_pnl"],gross) and same(t["entry_fee"],t["entry_price"]*t["quantity"]*fee_rate) and same(t["exit_fee"],t["exit_price"]*t["quantity"]*fee_rate), "PnL/fee contradiction")
            require(same(t["net_pnl"],gross-t["entry_fee"]-t["exit_fee"]) and same(t["net_r"],t["net_pnl"]/t["planned_cash_risk"]), "net PnL/R contradiction")
            if t["exit_reason"] in ("terminal","gap_boundary"):
                require(exit_["segment_end"] and t["exit_phase"]=="close" and (t["exit_reason"]=="terminal")==exit_["study_end"], "boundary attribution")
        balance = settings["initial_capital"]
        for p in path:
            i=p["bar_index"]
            balance -= sum(t["entry_fee"] for t in trades if t["entry_index"]==i)
            balance += sum(t["gross_pnl"]-t["exit_fee"] for t in trades if t["exit_index"]==i)
            active=[t for t in trades if t["entry_index"]<=i<t["exit_index"]]
            require(len(active)<=1, "overlapping positions")
            quantity=active[0]["quantity"] if active else 0
            direction=(1 if settings["direction"]=="long" else -1) if active else 0
            unrealized=(p["close_price"]-active[0]["entry_price"])*quantity*direction if active else 0
            require(same(p["balance"],balance) and same(p["equity"],balance+unrealized), "account balance/equity contradiction")
            require(p["position_direction"]==direction and same(p["position_quantity"],quantity), "account position contradiction")
            require(p["entry_price"]==(active[0]["entry_price"] if active else None), "account entry price")
            require(not p["segment_end"] or not active, "position carried across boundary")
    return grouped


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


def summaries(grouped, instances):
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
        final=path[-1]["balance"] if path else initial
        result.append({**dict(zip(KEY,key)),"settings":settings,"signals":len(attempts),"dispositions":counts,
            "completed_trades":len(trades),"wins":len(winning),"losses":len(losing),
            "breakeven":len(trades)-len(winning)-len(losing),"win_rate":len(winning)/len(trades) if trades else None,
            "initial_capital":initial,"final_capital":final,"gross_pnl":sum(t["gross_pnl"] for t in trades),
            "net_pnl":sum(t["net_pnl"] for t in trades),"total_fees":sum(t["entry_fee"]+t["exit_fee"] for t in trades),
            "return_pct":(final-initial)/initial*100,
            "profit_factor":sum(winning)/-sum(losing) if losing else None,
            "profit_factor_status":"available" if losing else "no_losses" if winning else "no_wins_or_losses",
            "net_planned_r":describe([t["net_r"] for t in trades]),
            "holding_ms":describe([t["holding_ms"] for t in trades]),
            "realized_balance_drawdown_pct":drawdown([p["balance"] for p in path],initial),
            "bar_close_mtm_drawdown_pct":drawdown([p["equity"] for p in path],initial),
            "finite_cap_attempt_count":finite_cap,"cap_rejection_share":counts["leverage_cap_exceeded"]/finite_cap if finite_cap else None,
            "max_attempt_required_leverage":max(required) if required else None,
            "max_executed_entry_leverage":max(used) if used else None,
            "stop_width_pct":describe([a["risk_distance"]/a["anchor_price"]*100 for a in attempts if a["risk_distance"] is not None and a["risk_distance"]>0 and a["anchor_price"]>0]),
            "exits":{reason:sum(t["exit_reason"]==reason for t in trades) for reason in ("stop","target","expiry","terminal","gap_boundary")},
            "gap_open_exits":sum(t["exit_phase"]=="open" and t["exit_reason"] in ("stop","target") for t in trades),
            "ambiguous_trades":sum(t["ambiguous"] for t in trades),
            "ambiguous_bars":len({t["exit_index"] for t in trades if t["ambiguous"]})})
    return result
