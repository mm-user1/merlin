"""Shared pre-outcome memberships, guarded scalar returns and paired draw paths."""
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
import numpy as np

from .ranking import Ranking, open_ranking, require, stamp
from .outcomes import fixed_outcomes, candidate_outcomes
from ..analysis.dataset import git_facts
from ..analysis.json_utils import canonical_json_bytes
from ..report_support import labels, finite, concentration, new_output, write_rows, write_bytes, finish_report


def memberships(record, roster, k):
    eligible = [r for r in record["rows"] if r["eligible"]]
    return dict(x_top_k=([r["instrument"] for r in eligible[:k]], k), all_declared=(roster, len(roster)),
        all_x_eligible=([r["instrument"] for r in eligible], len(eligible)),
        x_bottom_k=([r["instrument"] for r in sorted(eligible, key=lambda r:(r["score"], r["instrument"]))[:k]], k))


def distribution(values):
    a = np.asarray(values, dtype=float)
    valid = a[np.isfinite(a)]
    complete = len(valid) == len(a) and len(a) > 0
    # Match chronological per-draw accumulation when these values are blocks.
    return dict(expected=len(a), finite=len(valid), mean=float(np.cumsum(valid)[-1]/len(valid)) if complete else None,
        median=float(np.quantile(valid, .5)) if complete else None, p05=float(np.quantile(valid, .05)) if complete else None,
        p95=float(np.quantile(valid, .95)) if complete else None, reason=None if complete else "incomplete_required_observations")


def arm_return(members, denominator, cells):
    selected = [cells[t] for t in sorted(members)]
    missing = [t for t in members if cells[t]["status"] == "unknown"]
    abstained = sum(c["status"] == "abstained" for c in selected)
    active = len(selected)-len(missing)-abstained
    # Saved random rows use this same canonical-ID float64 reduction.
    return dict(value=None if missing else float(np.sum([c["value"] for c in selected], dtype=float)/denominator) if denominator else 0.0,
        reason="unknown_required_outcome" if missing else None, unknown_tickers=missing, expected_denominator=denominator,
        selected_count=len(selected), active_count=active, abstained_count=abstained, missing_count=len(missing),
        occupied_weight=len(selected)/denominator if denominator else 0.0,
        funded_weight=(len(selected)-abstained)/denominator if denominator else 0.0,
        cash_weight=(denominator-len(selected)+abstained)/denominator if denominator else 1.0)


@dataclass(frozen=True)
class SelectionResult:
    metadata: dict
    tables: dict
    summary: dict
    protected_roots: tuple


def evaluate_selection(ranking, *, fixed_inputs=None, inputs=None, parameter_sets=None, allow_partial=False,
                       candidate_rule=None, candidate_policy=None, baseline=None):
    ranking = ranking if isinstance(ranking, Ranking) else open_ranking(ranking)
    ranking.verify()
    require((fixed_inputs is None) != (inputs is None), "supply exactly one saved outcome family.")
    fixed = fixed_inputs is not None
    supplied = list(fixed_inputs.items()) if fixed and isinstance(fixed_inputs, dict) else list(fixed_inputs if fixed else inputs)
    pairs = supplied if fixed else [(i.label, i.dataset.root) for i in supplied]
    labels(pairs)
    names = [label for label, _ in pairs]
    require(baseline is None or baseline in names, "baseline label is not an input.")
    require(not fixed or (candidate_rule is None and candidate_policy is None), "Fixed evaluation has no candidate policy or IS rule.")
    require(fixed or (not parameter_sets and not allow_partial), "parameter-set and allow-partial are Fixed-only.")
    m = deepcopy(ranking.metadata)
    roster = sorted(m["universe"]["document"]["tickers"])
    k, draws = m["request"]["effective"]["tickerSelector"]["topN"], m["controls"]["draws"]
    if fixed:
        outcomes, sources, roots, trace = fixed_outcomes(ranking, supplied, parameter_sets or {}, allow_partial)
    else:
        outcomes, sources, roots, trace = candidate_outcomes(ranking, supplied, candidate_rule, candidate_policy)
    periods = m["calendar"]["periods"]
    calendar = [dict(id=p["id"], start=p["start"], end=p["end"], period_id=p["id"]) for p in periods] if fixed else [
        dict(**w, period_id=p["id"]) for p in periods for w in p["windows"]]
    block_count = len(calendar)
    durations = Counter((stamp(b["end"]).year-stamp(b["start"]).year)*12+stamp(b["end"]).month-stamp(b["start"]).month for b in calendar)
    decisions, blocks, comparisons, turnover = [], [], [], []
    headlines = defaultdict(list)
    path_sums, path_valid = {n:np.zeros(draws) for n in names}, {n:np.ones(draws, dtype=bool) for n in names}
    delta_sums, delta_valid = {n:np.zeros(draws) for n in names if n != baseline}, {n:np.ones(draws, dtype=bool) for n in names if n != baseline}
    contribution, participation, unknown_participation, incomplete = defaultdict(float), Counter(), Counter(), set()
    previous, previous_plan, previous_roster = None, None, None
    for pi, (period, record) in enumerate(zip(periods, ranking.records)):
        arms = memberships(record, roster, k)
        plan = ranking.plan(pi)
        eligible = m["controls"]["periods"][pi]["eligible"]
        frequencies = np.bincount(plan.ravel().astype(np.int64), minlength=len(eligible))/draws
        rows = {r["instrument"]:r for r in record["rows"]}
        if previous is not None:
            for arm, (members, _) in arms.items():
                before, after = set(previous[arm][0]), set(members)
                turnover.append(dict(period_id=period["id"], arm=arm, added=sorted(after-before), removed=sorted(before-after),
                    retained=sorted(before&after), added_count=len(after-before), removed_count=len(before-after), retained_count=len(before&after),
                    expected_draws=None, added_distribution=None, removed_distribution=None, retained_distribution=None))
            changes = []
            for old, new in zip(previous_plan, plan):
                before = {previous_roster[int(i)] for i in old}; after = {eligible[int(i)] for i in new}
                changes.append((len(after-before), len(before-after), len(before&after)))
            turnover.append(dict(period_id=period["id"], arm="random_k", added=None, removed=None, retained=None,
                added_count=float(np.mean([c[0] for c in changes])), removed_count=float(np.mean([c[1] for c in changes])),
                retained_count=float(np.mean([c[2] for c in changes])), expected_draws=draws,
                **{name+"_distribution":distribution([c[j] for c in changes]) for j,name in enumerate(("added", "removed", "retained"))}))
        current_blocks = [b for b in calendar if b["period_id"] == period["id"]]
        for b in current_blocks:
            block_key = [b["start"], b["end"]]
            duration = (stamp(b["end"]).year-stamp(b["start"]).year)*12+stamp(b["end"]).month-stamp(b["start"]).month
            returns, random_values = {}, {}
            for label in names:
                cells = {t:outcomes[label][b["id"], t] for t in roster}
                for ticker, c in cells.items():
                    rank = rows[ticker]
                    decisions.append(dict(dataset_label=label, block_id=b["id"], block_key=block_key, period_id=period["id"], ticker=ticker,
                        eligible=rank["eligible"], score=rank["score"], rank=rank["rank"], selected=rank["selected"],
                        ranking_reason=rank["reason_code"], **{key:c[key] for key in
                            ("status", "reason", "value", "account_id", "semantic_key", "candidate_id", "is_net_profit_pct", "is_total_trades")},
                        total_trades=c["metrics"].get("total_trades"), max_drawdown_pct=c["metrics"].get("max_drawdown_pct"),
                        max_drawdown_mtm_pct=c["metrics"].get("max_drawdown_mtm_pct")))
                returns[label] = {}
                for arm, (members, denominator) in arms.items():
                    result = arm_return(members, denominator, cells)
                    returns[label][arm] = result["value"]
                    headlines[label, arm].append(result["value"])
                    blocks.append(dict(level="arm_block", dataset_label=label, block_id=b["id"], block_key=block_key,
                        period_id=period["id"], duration_months=duration, arm=arm, unit="percent_reference_return", **result))
                    for t in members:
                        key = label, arm, t
                        participation[key] += 1
                        if cells[t]["status"] == "unknown":
                            incomplete.add((label, arm))
                            unknown_participation[key] += 1
                        else:
                            contribution[key] += cells[t]["value"]/denominator/block_count
                values = np.array([cells[t]["value"] if finite(cells[t]["value"]) else np.nan for t in eligible])
                random = np.sum(values[plan], axis=1)/k
                random_values[label] = random
                good = np.isfinite(random)
                path_sums[label] += np.where(good, random, 0)
                path_valid[label] &= good
                random_stats = distribution(random)
                blocks.append(dict(level="random_block_distribution", dataset_label=label, block_id=b["id"], block_key=block_key,
                    period_id=period["id"], duration_months=duration, arm="random_k", unit="percent_reference_return",
                    **random_stats, expected_denominator=k, selected_count=plan.shape[1],
                    active_count=sum(frequencies[j] for j,t in enumerate(eligible) if cells[t]["status"] == "completed"),
                    abstained_count=sum(frequencies[j] for j,t in enumerate(eligible) if cells[t]["status"] == "abstained"),
                    missing_count=sum(frequencies[j] for j,t in enumerate(eligible) if cells[t]["status"] == "unknown"),
                    occupied_weight=plan.shape[1]/k, funded_weight=sum(frequencies[j] for j,t in enumerate(eligible) if cells[t]["status"] != "abstained")/k,
                    cash_weight=1-sum(frequencies[j] for j,t in enumerate(eligible) if cells[t]["status"] != "abstained")/k,
                    unknown_tickers=[t for j,t in enumerate(eligible) if frequencies[j] and cells[t]["status"] == "unknown"]))
                for j,t in enumerate(eligible):
                    if not frequencies[j]:
                        continue
                    key = label, "random_k", t
                    participation[key] += float(frequencies[j])
                    if cells[t]["status"] == "unknown":
                        incomplete.add((label, "random_k"))
                        unknown_participation[key] += float(frequencies[j])
                    else:
                        contribution[key] += frequencies[j]*cells[t]["value"]/k/block_count
            if baseline is not None:
                for label in names:
                    if label == baseline:
                        continue
                    for arm in arms:
                        left, right = returns[label][arm], returns[baseline][arm]
                        delta = left-right if finite(left) and finite(right) else None
                        headlines[label, "delta:"+arm].append(delta)
                        comparisons.append(dict(level="paired_block", dataset_label=label, baseline=baseline, arm=arm,
                            block_key=block_key, value=delta, unit="percentage_points", reason=None if delta is not None else "incomplete_pair"))
                    delta = random_values[label]-random_values[baseline]
                    good = np.isfinite(delta)
                    delta_sums[label] += np.where(good, delta, 0)
                    delta_valid[label] &= good
                    comparisons.append(dict(level="paired_random_block_distribution", dataset_label=label, baseline=baseline,
                        arm="random_k", block_key=block_key, unit="percentage_points", **distribution(delta)))
        previous, previous_plan, previous_roster = arms, plan, eligible
    summary_labels = {}
    for label in names:
        arm_summaries = {}
        for arm in ("x_top_k", "all_declared", "all_x_eligible", "x_bottom_k"):
            values = headlines[label, arm]
            finite_values = [v for v in values if finite(v)]
            arm_summaries[arm] = dict(**distribution([v if v is not None else np.nan for v in values]),
                finite_only_descriptive=distribution(finite_values), unit="percent_reference_return")
        paths = np.where(path_valid[label], path_sums[label]/block_count, np.nan)
        random_summary = distribution(paths)
        x = arm_summaries["x_top_k"]["mean"]
        random_summary["x_midrank_fraction"] = float((np.sum(paths < x)+.5*np.sum(paths == x))/draws) if finite(x) and path_valid[label].all() else None
        summary_labels[label] = dict(arms=arm_summaries, random_path_mean_distribution=random_summary)
        if baseline is not None and label != baseline:
            for arm in arm_summaries:
                comparisons.append(dict(level="paired_headline", dataset_label=label, baseline=baseline, arm=arm, block_key=None,
                    unit="percentage_points", **distribution([v if v is not None else np.nan for v in headlines[label, "delta:"+arm]])))
            comparisons.append(dict(level="paired_random_path_distribution", dataset_label=label, baseline=baseline, arm="random_k", block_key=None,
                unit="percentage_points", **distribution(np.where(delta_valid[label], delta_sums[label]/block_count, np.nan))))
    concentrated = []
    for label in names:
        for arm in ("x_top_k", "all_declared", "all_x_eligible", "x_bottom_k", "random_k"):
            parts = [(t, None if participation[label, arm, t] > 0 and
                unknown_participation[label, arm, t] == participation[label, arm, t] else contribution[label, arm, t]) for t in roster]
            for row in concentration(parts, complete=(label, arm) not in incomplete, partial_support=(label, arm) in incomplete):
                concentrated.append(dict(dataset_label=label, arm=arm, unit="percentage_points_of_equal_block_return",
                    participation_blocks=participation[label, arm, row["group"]],
                    unknown_participation_blocks=unknown_participation[label, arm, row["group"]], expected_blocks=block_count, **row))
    roots.extend([ranking.root, m["request"]["path"], m["universe"]["path"], *(s["pack_root"] for s in m["pool"])])
    if candidate_policy is not None:
        roots.append(candidate_policy.path)
    metadata = dict(schema_version="strategy_lab_x_evaluation_v1", git=git_facts(), ranking_path=str(ranking.root),
        ranking_sha256=ranking.file_sha256, ranking=m, sources=sources, candidate_rule=candidate_rule,
        candidate_policy=candidate_policy.provenance() if candidate_policy is not None else None,
        family="fixed" if fixed else "is_oos", baseline=baseline, freeze_trace=trace,
        interpretation="Independent reference-capital returns; X membership parity only, not production carried portfolio execution parity.",
        confounders="Source roots/pins, saved versions, strategies, filters, economics and IS geometry may differ; paired policy comparisons are descriptive.",
        inference="not_requested")
    summary = dict(schema_version="strategy_lab_x_summary_v1", expected_blocks=block_count,
        duration_month_counts={str(k):v for k,v in sorted(durations.items())},
        headline="equal-block mean across mixed durations" if len(durations)>1 else "equal-block mean of independent reference-capital returns",
        datasets=summary_labels, random_interpretation="Block draw distributions and across-draw path-mean distributions are distinct; all required observations must be finite.")
    return SelectionResult(deepcopy(metadata), dict(decisions=decisions, blocks=blocks,
        comparisons=comparisons or [dict(level="not_requested", reason="No non-baseline comparison requested.")],
        turnover=turnover or [dict(arm="not_requested", reason="Only one rebalance period.")], concentration=concentrated), summary, tuple(roots))


def write_selection(result, output_dir):
    output = new_output(output_dir, result.protected_roots)
    files = []
    for name, rows in result.tables.items():
        write_rows(output, name+".csv", rows)
        files.append(name+".csv")
    write_bytes(output, "summary.json", canonical_json_bytes(result.summary))
    files.append("summary.json")
    return finish_report(output, "selection.json", result.metadata, files, "Merlin X ticker-selection research",
        dict(headline=result.summary["headline"], duration_month_counts=result.summary["duration_month_counts"],
            interpretation=result.metadata["interpretation"], inference="not_requested"),
        tables=[("Equal-block reference returns (%)", [dict(dataset=label, arm=arm, mean=row["mean"],
            finite_blocks=row["finite"], expected_blocks=row["expected"]) for label, facts in result.summary["datasets"].items()
            for arm, row in facts["arms"].items()])])
