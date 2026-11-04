"""Pure historical ranking over an explicit, closed pre-outcome projection."""
from copy import deepcopy
import numpy as np

from .history_plan import METHODS
from .ranking import require, stamp
from ..analysis.research_inputs import _keys, digest
from ..report_support import finite

CURRENT = "candidate_id semantic_key is_net_profit_pct is_total_trades".split()
PRIOR = "status reason value account_id candidate_id semantic_key is_net_profit_pct is_total_trades".split()


def score_history(projection):
    """No dataset, callback, global ledger or current outcome is accepted here."""
    _keys(projection, "schema_version settings block prior current".split(), "decision projection")
    require(projection["schema_version"] == "strategy_lab_history_decision_v1", "unsupported decision version.")
    s, block, prior, current = (projection[k] for k in ("settings", "block", "prior", "current"))
    _keys(s, "method methodVersion topN roster tradingTimeframeMinutes family geometry historyWindows strategy_settings".split(), "decision settings")
    method = s["method"]
    require(method in METHODS and s["methodVersion"] == 1 and s["historyWindows"] == 4, "invalid method/version/history.")
    roster = s["roster"]
    require(roster == sorted(set(roster)) and roster and type(s["topN"]) is int and 0 < s["topN"] <= len(roster), "invalid decision roster/K.")
    _keys(block, ["start", "end", "isStart"], "current local block")
    require(stamp(block["start"]) < stamp(block["end"]), "invalid local block.")
    fixed = s["family"] == "fixed"
    require(s["family"] in ("fixed", "is_oos"), "invalid decision family.")
    require(not fixed or method in METHODS[:2], "Fixed method mismatch.")
    require(method != METHODS[3] or s["geometry"] == dict(isMonths=4, oosMonths=1), "tc4w requires IS4/OOS1.")
    require((current is None) if fixed else isinstance(current, dict) and set(current) == set(roster), "invalid current candidate facts.")
    if not fixed:
        for fact in current.values():
            _keys(fact, CURRENT, "current candidate facts")
    require(isinstance(prior, list) and len(prior) == (0 if method == METHODS[2] else 4), "exactly four adjacent history rows required.")
    previous_end = None
    for p in prior:
        _keys(p, ["block", "rows"], "prior block")
        _keys(p["block"], ["start", "end", "isStart"], "prior local block")
        require(stamp(p["block"]["start"]) < stamp(p["block"]["end"]) <= stamp(block["start"]), "history must complete before cutoff.")
        require(previous_end is None or previous_end == p["block"]["start"], "history must be adjacent; no older replacement.")
        previous_end = p["block"]["end"]
        require(set(p["rows"]) == set(roster), "history must retain the full roster.")
        for row in p["rows"].values():
            _keys(row, PRIOR, "prior scalar row")
    require(not prior or previous_end == block["start"], "history must immediately precede cutoff.")
    rows = []
    for ticker in roster:
        facts = None if fixed else current[ticker]
        reason = None
        if not fixed:
            reason = "no_current_candidate" if facts["candidate_id"] is None else "nonfinite_current_is" if not finite(facts["is_net_profit_pct"]) else None
        history = [p["rows"][ticker] for p in prior]
        if reason is None and any(r["status"] == "unknown" or not finite(r["value"]) for r in history):
            reason = "unavailable_adjacent_history"
        if reason is None and method == METHODS[3] and any(not finite(r["is_net_profit_pct"]) for r in history):
            reason = "unavailable_historical_is"
        features, score = {}, []
        if reason is None:
            if method == METHODS[2]:
                features = dict(selected_is_net_profit=facts["is_net_profit_pct"])
            else:
                returns = np.asarray([r["value"] for r in history], dtype=np.float64)
                if method in (METHODS[0], METHODS[3]):
                    features["positive_count"] = int(np.sum(returns > 0))
                if method == METHODS[3]:
                    features["minimum_return"] = float(np.min(returns))
                if method in (METHODS[1], METHODS[3]):
                    features["mean_return"] = float(np.mean(returns))
                if method == METHODS[3]:
                    features["mean_transfer_gap"] = float(np.mean(returns-np.asarray([r["is_net_profit_pct"] for r in history])/4))
                    features["selected_is_net_profit"] = facts["is_net_profit_pct"]
            score = list(features.values())
        rows.append(dict(ticker=ticker, candidate_facts=deepcopy(facts), history_refs=[[p["block"]["start"], p["block"]["end"], ticker] for p in prior],
            eligible=reason is None, reason=reason, features=features, score=score, rank=None, selected=False, bottom=False))
    eligible = sorted((r for r in rows if r["eligible"]), key=lambda r:(tuple(-v for v in r["score"]), r["ticker"]))
    bottom = eligible[-s["topN"]:]
    for i, r in enumerate(eligible, 1):
        r.update(rank=i, selected=i <= s["topN"], bottom=r in bottom)
    groups = {}
    for r in eligible:
        groups.setdefault(tuple(r["score"]), []).append(r["ticker"])
    return dict(decision_identity=digest(projection), rows=rows, ordered=[r["ticker"] for r in eligible],
        eligible=sorted(r["ticker"] for r in eligible), tie_groups=[dict(score=list(k), tickers=v) for k,v in groups.items() if len(v)>1])


def local_block(block):
    return {key: block[key] for key in ("start", "end", "isStart")}


def project_decision(request, roster, blocks, index, ledger, method, strategy_settings):
    """The sole ledger-to-scorer boundary; only explicitly enumerated facts cross."""
    c = request["calendar"]
    fixed = c["mode"] == "fixed"
    return deepcopy(dict(schema_version="strategy_lab_history_decision_v1", settings=dict(method=method, methodVersion=1,
        topN=request["topN"], roster=roster, tradingTimeframeMinutes=c["tradingTimeframeMinutes"], family=c["mode"],
        geometry={k:c[k] for k in (["periodMonths"] if fixed else ["isMonths", "oosMonths"])},
        historyWindows=4, strategy_settings=strategy_settings), block=local_block(blocks[index]),
        prior=[] if method == METHODS[2] else [dict(block=local_block(b), rows={t:{k:ledger[b["id"], t][k] for k in PRIOR} for t in roster})
            for b in blocks[index-4:index]],
        current=None if fixed else {t:{k:ledger[blocks[index]["id"], t][k] for k in CURRENT} for t in roster}))
