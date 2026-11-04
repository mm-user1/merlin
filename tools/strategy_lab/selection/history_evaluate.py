"""Collect saved scalars once, freeze causal decisions, then publish target payoffs."""
from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import io
from pathlib import Path
import sys

import numpy as np

from .history_plan import HistoryPlan, open_history_plan, checked_input
from .history_score import project_decision, score_history
from .ranking import require, index_dtype, stamp, ALGORITHM
from .outcomes import collect_fixed, collect_candidates
from .evaluate import arm_return, distribution, random_returns, accumulate_paths, midrank
from ..analysis.dataset import git_facts
from ..analysis.research_inputs import digest
from ..analysis.json_utils import canonical_json_bytes
from ..dataset import artifact_record, validate_artifact_record
from ..fixed.storage import safe_path
from ..report_support import labels, finite, new_output, write_bytes, write_rows, finish_report, concentration


def strategy_settings(source, fixed, rule, policy):
    """Stable selection semantics only, excluding source locators and global hashes."""
    g = source["manifest"]["contract"]["request"]["generation"] if fixed else source["contract"]["generation"]
    filters = (source["manifest"]["contract"]["resolved"].get("entry_filters", []) if fixed
        else source["bindings"].get("effective_entry_filters", []))
    def without_paths(value):
        if isinstance(value, dict):
            return {k:without_paths(v) for k,v in value.items() if k not in ("pack_root", "path", "name", "id")}
        return [without_paths(v) for v in value] if isinstance(value, list) else value
    return deepcopy(dict(strategy=g["strategy"], execution_policy=g["execution"]["policy"],
        filters=without_paths(filters), parameters=source["resolved_params"] if fixed else g["economics"],
        candidate_geometry=None if fixed else source["candidate_semantics"],
        candidate_rule=rule, candidate_policy=None if policy is None else {k:v for k,v in policy.document.items() if k != "name"}))


def pool_spec(block, eligible, request):
    projection = dict(schema_version="strategy_lab_history_random_pool_v1", block_key=[block["start"], block["end"]],
        tradingTimeframeMinutes=request["calendar"]["tradingTimeframeMinutes"], topN=request["topN"],
        **request["controls"], eligible=eligible)
    key = digest(projection)
    shape = [request["controls"]["draws"], min(request["topN"], len(eligible))]
    dtype = index_dtype(len(eligible))
    return dict(pool_key=key, projection=projection, eligible=eligible, shape=shape, dtype=dtype,
        derived_seed=int.from_bytes(bytes.fromhex(key)[:8], "big"), file=f"plans/{key}.npy",
        array_bytes=shape[0]*shape[1]*np.dtype(dtype).itemsize)


def read_random_plan(root, spec):
    """Validate actual saved indices without replaying RNG; internal report helper."""
    path = safe_path(root, spec["file"])
    require(validate_artifact_record(root, spec["artifact"]), "history random plan file hash mismatch.")
    with path.open("rb") as stream:
        require(stream.read(6) == b"\x93NUMPY", "random plan must be uncompressed NPY.")
    a = np.load(path, mmap_mode="r", allow_pickle=False)
    require(a.dtype.str == spec["dtype"] and list(a.shape) == spec["shape"], "random dtype/shape mismatch.")
    require(hashlib.sha256(a.tobytes(order="C")).hexdigest() == spec["content_sha256"], "random content hash mismatch.")
    require(not a.size or (int(a.max()) < len(spec["eligible"]) and np.all(a[:,1:] > a[:,:-1])), "random indices must be sorted, unique and in range.")
    return a


def history_arms(decision, roster, k):
    return dict(top_k=([r["ticker"] for r in decision["rows"] if r["selected"]], k),
        bottom_k=([r["ticker"] for r in decision["rows"] if r["bottom"]], k),
        all_method_eligible=(decision["eligible"], len(decision["eligible"])))


def random_turnover(root, before, after):
    """One mapped plan at a time, retaining only draw-length flags/counts.

    Plans have already been hash/shape validated. Membership changes do not
    depend on label payoffs, so callers share this calculation by pool pair.
    """
    retained = np.zeros(after["shape"][0])
    for ticker in sorted(set(before["eligible"]) & set(after["eligible"])):
        array = np.load(safe_path(root, before["file"]), mmap_mode="r", allow_pickle=False)
        present = np.any(array == before["eligible"].index(ticker), axis=1)
        del array
        array = np.load(safe_path(root, after["file"]), mmap_mode="r", allow_pickle=False)
        retained += present & np.any(array == after["eligible"].index(ticker), axis=1)
        del array
    return dict(added_distribution=distribution(after["shape"][1]-retained),
        removed_distribution=distribution(before["shape"][1]-retained), retained_distribution=distribution(retained))


def coverage_summary(rows):
    return dict(expected_blocks=len(rows), unknown_selected_blocks=sum(int(r["missing_count"] > 0) for r in rows),
        selected_observations=sum(r["selected_count"] for r in rows),
        unknown_observations=sum(r["missing_count"] for r in rows),
        abstained_observations=sum(r["abstained_count"] for r in rows),
        mean_cash_weight=distribution([r["cash_weight"] for r in rows])["mean"],
        mean_planned_cash_weight=distribution([r["planned_cash_weight"] for r in rows])["mean"])


def evaluate_history(plan, output_dir, *, fixed_inputs=None, inputs=None, history_scopes=None,
                     parameter_sets=None, allow_partial=False, candidate_rule=None, candidate_policy=None, baseline=None):
    plan = plan if isinstance(plan, HistoryPlan) else open_history_plan(plan)
    plan.verify()
    require((fixed_inputs is None) != (inputs is None), "supply exactly one saved outcome family.")
    fixed = fixed_inputs is not None
    supplied = list(fixed_inputs.items()) if fixed and isinstance(fixed_inputs, dict) else list(fixed_inputs if fixed else inputs)
    pairs = supplied if fixed else [(i.label, i.dataset.root) for i in supplied]
    labels(pairs)
    for _, path in pairs:
        checked_input(path)
    names = [n for n,_ in pairs]
    require(baseline is None or baseline in names, "baseline label is not an input.")
    require(not fixed or (candidate_rule is None and candidate_policy is None and history_scopes is None), "candidate options are irrelevant to Fixed.")
    require(fixed or (not parameter_sets and not allow_partial), "parameter-set/allow-partial are Fixed-only.")
    m = deepcopy(plan.metadata); request = m["request"]["effective"]; c = request["calendar"]
    require(c["mode"] == ("fixed" if fixed else "is_oos"), "history plan/outcome family mismatch.")
    roster = sorted(m["universe"]["document"]["tickers"])
    blocks = m["mapping"]["blocks"]; history_ids = m["mapping"]["history_ids"]
    targets = blocks[len(history_ids):]; k = request["topN"]; draws = request["controls"]["draws"]
    if fixed:
        ledger, sources, roots, access_trace = collect_fixed(roster, c["tradingTimeframeMinutes"], blocks, supplied, parameter_sets or {}, allow_partial)
    else:
        require(isinstance(history_scopes, dict) and set(history_scopes) == set(names), "explicit history scope required for every label; unknown labels refuse.")
        ledger, sources, roots, access_trace = collect_candidates(roster, c["tradingTimeframeMinutes"], blocks, supplied,
            candidate_rule, candidate_policy, history_scopes=history_scopes, history_ids=history_ids)
    roots.extend([plan.root, m["request"]["path"], m["universe"]["path"]])
    if candidate_policy is not None:
        roots.append(candidate_policy.path)
    output = new_output(output_dir, roots)
    settings = {n:strategy_settings(sources[n], fixed, candidate_rule, candidate_policy) for n in names}
    decisions, pools, trace = {}, {}, []
    for i in range(len(history_ids), len(blocks)):
        b = blocks[i]
        for label in names:
            for method in request["methods"]:
                projection = project_decision(request, roster, blocks, i, ledger[label], method, settings[label])
                decision = score_history(projection)
                spec = pool_spec(b, decision["eligible"], request)
                pools.setdefault(spec["pool_key"], spec)
                decision.update(label=label, method=method, block_id=b["id"], block_key=[b["start"], b["end"]],
                    pool_key=spec["pool_key"], history_block_ids=[] if method == "selected_is_net_profit" else [p["id"] for p in blocks[i-4:i]])
                decisions[label, method, b["id"]] = decision
                trace.append(dict(event="historical_decision", label=label, method=method, block_key=decision["block_key"],
                    history_block_ids=decision["history_block_ids"], decision_identity=decision["decision_identity"], pool_key=spec["pool_key"]))
    total_bytes = sum(p["array_bytes"] for p in pools.values())
    print(f"History random plans: {len(pools)} unique, {total_bytes} payload bytes before allocation.", file=sys.stderr)
    files = []
    for spec in pools.values():
        generator = np.random.Generator(np.random.PCG64(spec["derived_seed"]))
        indices = np.empty(spec["shape"], dtype=spec["dtype"])
        for row in indices:
            row[:] = np.sort(generator.choice(len(spec["eligible"]), size=spec["shape"][1], replace=False))
        spec["content_sha256"] = hashlib.sha256(indices.tobytes(order="C")).hexdigest()
        buffer = io.BytesIO(); np.save(buffer, indices, allow_pickle=False)
        write_bytes(output, spec["file"], buffer.getvalue())
        spec["artifact"] = artifact_record(output/spec["file"], output)
        files.append(spec["file"])
        del indices, buffer
        checked = read_random_plan(output, spec)
        del checked
    # Methods with identical pool sequences share random outcomes within a label.
    contexts, method_context = {}, {}
    for label in names:
        for method in request["methods"]:
            sequence = [decisions[label, method, b["id"]]["pool_key"] for b in targets]
            key = digest(dict(label=label, pool_keys=sequence))
            contexts.setdefault(key, dict(label=label, pool_keys=sequence, methods=[]))["methods"].append(method)
            method_context[label, method] = key
    sums = {key:np.zeros(draws) for key in contexts}; valid = {key:np.ones(draws, dtype=bool) for key in contexts}
    pair_specs = []
    for label in names:
        for method in request["methods"]:
            if method != request["referenceMethod"]:
                pair_specs.append((label, method, label, request["referenceMethod"]))
            if baseline is not None and label != baseline:
                pair_specs.append((label, method, baseline, method))
    delta_sums, delta_valid = {}, {}
    for pair in pair_specs:
        left, right = method_context[pair[:2]], method_context[pair[2:]]
        if left != right and contexts[left]["pool_keys"] == contexts[right]["pool_keys"]:
            delta_sums[pair] = np.zeros(draws); delta_valid[pair] = np.ones(draws, dtype=bool)
    tables = dict(blocks=[], comparisons=[], turnover=[], concentration=[])
    headlines, contribution, participation, unknown = defaultdict(list), defaultdict(float), Counter(), Counter()
    previous, pool_turnover = {}, {}
    for ti, b in enumerate(targets):
        key = [b["start"], b["end"]]
        returns, random_values = {}, {}
        for label in names:
            cells = {t:ledger[label][b["id"], t] for t in roster}
            arm_contexts = [("all_declared", "all_declared", roster, len(roster))]
            for method in request["methods"]:
                decision = decisions[label, method, b["id"]]
                arm_contexts.extend((method, arm, members, denominator) for arm,(members,denominator) in history_arms(decision, roster, k).items())
            for context, arm, members, denominator in arm_contexts:
                result = arm_return(members, denominator, cells)
                identity = label, context, arm
                returns[identity] = result["value"]; headlines[identity].append(result["value"])
                tables["blocks"].append(dict(label=label, context=context, arm=arm, block_id=b["id"], block_key=key,
                    level="arm_block", unit="percent_reference_return", planned_cash_weight=(denominator-len(members))/denominator if denominator else 1., **result))
                for t in members:
                    ck = (*identity,t); participation[ck] += 1
                    if cells[t]["status"] == "unknown": unknown[ck] += 1
                    else: contribution[ck] += cells[t]["value"]/denominator/len(targets)
                if identity in previous:
                    old, new = previous[identity], set(members)
                    tables["turnover"].append(dict(label=label, context=context, arm=arm, block_id=b["id"],
                        added=sorted(new-old), removed=sorted(old-new), retained=sorted(old&new)))
                previous[identity] = set(members)
        for context, facts in contexts.items():
            spec = pools[facts["pool_keys"][ti]]; label = facts["label"]
            cells = {t:ledger[label][b["id"],t] for t in roster}
            array = read_random_plan(output, spec)
            values = random_returns(cells, spec["eligible"], array, k)
            random_values[context] = values
            accumulate_paths(sums[context], valid[context], values)
            freq = np.bincount(array.ravel().astype(np.int64), minlength=len(spec["eligible"]))/draws
            missing = sum(freq[j] for j,t in enumerate(spec["eligible"]) if cells[t]["status"] == "unknown")
            abstained = sum(freq[j] for j,t in enumerate(spec["eligible"]) if cells[t]["status"] == "abstained")
            tables["blocks"].append(dict(label=label, context=context, arm="random_k", block_id=b["id"], block_key=key,
                level="random_block_distribution", unit="percent_reference_return", **distribution(values),
                expected_denominator=k, selected_count=spec["shape"][1], missing_count=missing, abstained_count=abstained,
                cash_weight=(k-spec["shape"][1]+abstained)/k, occupied_weight=spec["shape"][1]/k,
                funded_weight=(spec["shape"][1]-abstained)/k,
                planned_cash_weight=(k-spec["shape"][1])/k,
                unknown_tickers=[t for j,t in enumerate(spec["eligible"]) if freq[j] and cells[t]["status"] == "unknown"]))
            for j,t in enumerate(spec["eligible"]):
                ck = label, context, "random_k", t; participation[ck] += float(freq[j])
                if cells[t]["status"] == "unknown": unknown[ck] += float(freq[j])
                else: contribution[ck] += freq[j]*cells[t]["value"]/k/len(targets)
            del array
            if ti:
                old_spec = pools[facts["pool_keys"][ti-1]]
                pair_key = old_spec["pool_key"], spec["pool_key"]
                if pair_key not in pool_turnover:
                    pool_turnover[pair_key] = random_turnover(output, old_spec, spec)
                tables["turnover"].append(dict(label=label, context=context, arm="random_k", block_id=b["id"], **pool_turnover[pair_key]))
        for pair in pair_specs:
            label, method, ref_label, ref_method = pair
            left, right = method_context[pair[:2]], method_context[pair[2:]]
            dl, dr = decisions[label,method,b["id"]], decisions[ref_label,ref_method,b["id"]]
            lm = {r["ticker"] for r in dl["rows"] if r["selected"]}; rm = {r["ticker"] for r in dr["rows"] if r["selected"]}
            common = dict(label=label, method=method, reference_label=ref_label, reference_method=ref_method,
                interpretation="within-label method comparison" if label == ref_label else "whole-system comparison: configuration plus its own history-based selection")
            for arm in ("top_k", "bottom_k", "all_method_eligible"):
                a, z = returns[label,method,arm], returns[ref_label,ref_method,arm]
                delta = a-z if finite(a) and finite(z) else None
                headlines[pair,arm].append(delta)
                tables["comparisons"].append(dict(**common, level="paired_block", arm=arm, block_key=key, value=delta,
                    reason=None if delta is not None else "incomplete_pair", top_overlap=sorted(lm&rm),
                    left_only=sorted(lm-rm), right_only=sorted(rm-lm), same_pool=dl["pool_key"] == dr["pool_key"]))
            if left != right:
                same = dl["pool_key"] == dr["pool_key"]
                delta = random_values[left]-random_values[right] if same else None
                if label != ref_label or not same:
                    tables["comparisons"].append(dict(**common, level="paired_random_block_distribution", arm="random_k", block_key=key,
                        **(distribution(delta) if same else dict(reason="different_random_pools", mean=None))))
                if pair in delta_sums:
                    accumulate_paths(delta_sums[pair], delta_valid[pair], delta)
        del random_values
    summary_labels = {}
    random_summaries = {}
    coverage = defaultdict(list)
    for row in tables["blocks"]:
        coverage[row["label"],row["context"],row["arm"]].append(row)
    for context in contexts:
        paths = np.where(valid[context], sums[context]/len(targets), np.nan)
        random_summaries[context] = dict(**contexts[context], path_mean_distribution=distribution(paths),
            coverage=coverage_summary(coverage[contexts[context]["label"],context,"random_k"]))
        for method in contexts[context]["methods"]:
            label = contexts[context]["label"]
            top = distribution(headlines[label,method,"top_k"])["mean"]
            random_summaries[context].setdefault("method_midrank", {})[method] = midrank(paths, top)
    for label in names:
        summary_labels[label] = dict(all_declared=dict(**distribution(headlines[label,"all_declared","all_declared"]),
            coverage=coverage_summary(coverage[label,"all_declared","all_declared"])), methods={})
        for method in request["methods"]:
            summary_labels[label]["methods"][method] = dict(random_context=method_context[label,method], arms={arm:dict(
                **distribution(headlines[label,method,arm]), finite_only_descriptive=distribution([v for v in headlines[label,method,arm] if finite(v)]),
                coverage=coverage_summary(coverage[label,method,arm]))
                for arm in ("top_k", "bottom_k", "all_method_eligible")})
    for pair in pair_specs:
        common = dict(label=pair[0], method=pair[1], reference_label=pair[2], reference_method=pair[3])
        for arm in ("top_k", "bottom_k", "all_method_eligible"):
            tables["comparisons"].append(dict(**common, level="paired_headline", arm=arm, block_key=None, **distribution(headlines[pair,arm])))
        left, right = method_context[pair[:2]], method_context[pair[2:]]
        if left != right:
            tables["comparisons"].append(dict(**common, level="paired_random_path_distribution", arm="random_k", block_key=None,
                **(distribution(np.where(delta_valid[pair], delta_sums[pair]/len(targets), np.nan)) if pair in delta_sums
                    else dict(reason="different_random_pools", mean=None))))
    for identity in sorted({key[:3] for key in participation}):
        parts = [(t, None if unknown[*identity,t] == participation[*identity,t] and participation[*identity,t] else contribution[*identity,t]) for t in roster]
        complete = not any(unknown[*identity,t] for t in roster)
        for row in concentration(parts, complete=complete, partial_support=not complete):
            tables["concentration"].append(dict(label=identity[0], context=identity[1], arm=identity[2], group_kind="ticker", expected_blocks=len(targets),
                participation_blocks=participation[*identity,row["group"]], unknown_participation_blocks=unknown[*identity,row["group"]], **row))
    block_parts = defaultdict(list)
    for row in tables["blocks"]:
        value = row["value"] if row["level"] == "arm_block" else row["mean"]
        block_parts[row["label"],row["context"],row["arm"]].append((row["block_id"], value/len(targets) if finite(value) else None))
    for identity, parts in block_parts.items():
        complete = all(finite(v) for _,v in parts)
        for row in concentration(parts, complete=complete, partial_support=not complete, order_key=int):
            tables["concentration"].append(dict(label=identity[0], context=identity[1], arm=identity[2], group_kind="block",
                expected_blocks=len(targets), **row))
    durations = Counter((stamp(b["end"]).year-stamp(b["start"]).year)*12+stamp(b["end"]).month-stamp(b["start"]).month for b in targets)
    summary = dict(schema_version="strategy_lab_history_summary_v1", expected_blocks=len(targets),
        excluded_history_blocks=len(history_ids), duration_month_counts={str(k):v for k,v in sorted(durations.items())},
        headline="equal-block mean of independent Fixed account returns" if fixed else "equal-block mean of selected-candidate OOS returns",
        datasets=summary_labels, random_contexts=random_summaries,
        coverage={n:dict(Counter(r["status"] for (bid,t),r in ledger[n].items() if bid not in history_ids)) for n in names})
    if len(durations) > 1:
        summary["headline"] += " across mixed durations (no duration normalization)"
    history_rows = [dict(label=n, block_id=b["id"], block_key=[b["start"],b["end"]], history_only=b["id"] in history_ids,
        ticker=t, **ledger[n][b["id"],t]) for n in names for b in blocks for t in roster]
    decision_rows = [dict(label=d["label"], method=d["method"], block_id=d["block_id"], block_key=d["block_key"],
        decision_identity=d["decision_identity"], pool_key=d["pool_key"], **r) for d in decisions.values() for r in d["rows"]]
    for name, value in (("history", dict(schema_version="strategy_lab_history_ledger_v1", rows=history_rows)),
                        ("decisions", dict(schema_version="strategy_lab_history_decisions_v1", decisions=list(decisions.values()))), ("summary", summary)):
        write_bytes(output, name+".json", canonical_json_bytes(value)); files.append(name+".json")
    for name, rows in dict(history=history_rows, decisions=decision_rows, **tables).items():
        write_rows(output, name+".csv", rows); files.append(name+".csv")
    metadata = dict(schema_version="strategy_lab_history_evaluation_v1", git=git_facts(), plan_path=str(plan.root), plan_sha256=plan.file_sha256,
        plan=m, sources=sources, strategy_settings=settings, method_versions={method:1 for method in request["methods"]},
        candidate_rule=candidate_rule, candidate_policy=candidate_policy.provenance() if candidate_policy else None,
        family=c["mode"], baseline=baseline, candidate_access_trace=access_trace, history_decision_trace=trace,
        controls=dict(algorithm=ALGORITHM, numpy_version=np.__version__, unique_plan_count=len(pools), total_array_bytes=total_bytes,
            saved_array_bytes=sum(p["artifact"]["size"] for p in pools.values()), plans=list(pools.values())),
        interpretation="Independent reference-capital research. Whole-system comparison: configuration plus its own history-based selection.",
        inference="not_requested")
    return finish_report(output, "history-evaluation.json", metadata, files, "Historical ticker-selection research", dict(
        headline=summary["headline"], duration_month_counts=summary["duration_month_counts"], expected_target_blocks=len(targets),
        excluded_history_blocks=len(history_ids), random_plan_bytes=total_bytes, inference="not_requested"),
        tables=[("Target equal-block reference returns (%)", [dict(label=n, method=method, mean=facts["arms"]["top_k"]["mean"],
            finite_blocks=facts["arms"]["top_k"]["finite"], expected_blocks=len(targets))
            for n, d in summary_labels.items() for method,facts in d["methods"].items()])])
