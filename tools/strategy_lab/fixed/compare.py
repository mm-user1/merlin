"""Saved continuous-account comparisons at explicit variant/stratum/period grain."""
from collections import defaultdict
from copy import deepcopy
import hashlib
from pathlib import Path

import pandas as pd

from .config import ECONOMICS, RUNTIME
from .storage import open_dataset
from ..analysis.dataset import AnalysisError, git_facts
from ..analysis.research_inputs import Universe, digest, load_universe
from ..filters import recorded_roots
from ..report_support import (labels, finite, statistics, concentration, neighbourhood,
    new_output, write_rows, finish_report)

METRICS = ("net_profit_pct", "max_drawdown_pct", "max_drawdown_mtm_pct", "total_trades")


def _period(account):
    bounds = account["input_pin"]["bounds"]
    return tuple(pd.Timestamp(bounds[k]).tz_convert("UTC").isoformat() for k in ("evaluation_start", "evaluation_end"))


def _admission(account):
    from core.engine_v2.admission import read_admission_envelope
    e = account["entry_admission"]
    return {k: e[k] for k in ("version", "mode", "maxEntryLeverage")} | {
        "rules": read_admission_envelope(e, e["maxEntryLeverage"]).semantic()}


def _membership(universe, manifests, mode):
    inventories = {label: list(dict.fromkeys(a["instrument_id"] for a in m["contract"]["accounts"])) for label, m in manifests.items()}
    if universe is None:
        if mode is not None:
            raise AnalysisError("ticker-list mode requires a universe.")
        return inventories, dict(mode="declared_union", datasets=inventories)
    if not isinstance(universe, Universe):
        raise AnalysisError("universe must be supplied by load_universe.")
    universe.verify()
    mode = mode or "strict"
    if mode not in ("strict", "intersection"):
        raise AnalysisError("ticker-list mode must be strict or intersection.")
    requested = set(universe.document["tickers"])
    if requested-set().union(*(set(x) for x in inventories.values())):
        raise AnalysisError("universe contains IDs absent from every Fixed input.")
    facts, selected = {}, {}
    for label, inventory in inventories.items():
        missing = sorted(requested-set(inventory))
        if missing and mode == "strict":
            raise AnalysisError(f"Fixed {label!r} missing requested IDs: {missing}.")
        selected[label] = [t for t in inventory if t in requested]
        if not selected[label]:
            raise AnalysisError(f"Fixed {label!r} has an empty requested intersection.")
        facts[label] = dict(declared_tickers=inventory, effective_tickers=selected[label],
            missing_requested_tickers=missing, excluded_tickers=sorted(set(inventory)-requested))
    return selected, {**universe.provenance(), "mode": mode, "datasets": facts}


def _summaries(accounts):
    groups = defaultdict(list)
    cohorts = defaultdict(dict)
    for row in accounts:
        key = row["variant_key"], row["stratum_key"], tuple(row["period_key"])
        groups[key].append(row)
    for (variant, stratum, period), rows in groups.items():
        tickers = [r["ticker"] for r in rows]
        if len(tickers) != len(set(tickers)):
            raise AnalysisError(f"ambiguous duplicate ticker within variant/stratum/period: {variant}, {period}.")
        cohorts[stratum, period][variant] = rows
    output = []
    for (stratum, period), variants in sorted(cohorts.items()):
        variant_keys = sorted(variants)
        expected = sorted({r["ticker"] for rows in variants.values() for r in rows})
        for metric in METRICS:
            finite_sets = {variant: {r["ticker"] for r in rows if r["status"] == "completed" and finite(r["metrics"].get(metric))}
                for variant, rows in variants.items()}
            common = set.intersection(*finite_sets.values())
            facts = {variant: {r["ticker"]: dict(input_fingerprint=r["input_pin"]["input_fingerprint"],
                bounds=r["input_pin"]["bounds"], timeframe=r["timeframe_minutes"], admission=r["admission"])
                for r in rows if r["ticker"] in common} for variant, rows in variants.items()}
            identical = all(facts[v] == facts[variant_keys[0]] for v in variant_keys)
            for variant, rows in sorted(variants.items()):
                for mode in ("available", "common"):
                    selected = rows if mode == "available" else [r for r in rows if r["ticker"] in common]
                    values = [r["metrics"].get(metric) if r["status"] == "completed" else None for r in selected]
                    stats = statistics(values)
                    stats.update(expected=len(expected), missing=len(expected)-stats["finite"],
                        positive_share_expected=stats["positive"]/len(expected))
                    if mode == "common" and not common:
                        stats["reason"] = "empty_common_sample"
                    output.append(dict(level="primary", variant_key=variant, stratum_key=stratum, period_key=period,
                        metric=metric, membership=mode, cohort_variants=variant_keys, cohort_sha256=digest(variant_keys),
                        expected_tickers=expected, expected_count=len(expected), declared_count=len(rows),
                        completed_count=sum(r["status"] == "completed" for r in rows),
                        no_trade_count=sum(r["status"] == "completed" and r["metrics"].get("total_trades") == 0 for r in rows),
                        finite_tickers=sorted(finite_sets[variant] if mode == "available" else common),
                        missing_nonfinite_tickers=sorted(set(expected)-finite_sets[variant]),
                        cohort_missing_nonfinite={v: sorted(set(expected)-finite_sets[v]) for v in variant_keys},
                        sample_facts=facts, sample_classification="unavailable_common_membership" if not common else
                            "identical_saved_sample_facts" if identical else "descriptive_common_membership",
                        **stats, positive_share_expected_cohort=stats["positive"]/len(expected),
                        observed_sum=sum(v for v in values if finite(v)) if metric == "total_trades" and stats["finite"] else None,
                        complete_total=sum(values) if metric == "total_trades" and len(values) == len(expected) and stats["finite"] == len(expected) else None))
    # Across-period/TF displays preserve each primary mean; they never pool accounts.
    cross = defaultdict(list)
    variant_facts = {r["variant_key"]: r["variant"] for r in accounts}
    for row in output:
        if row["membership"] != "available":
            continue
        variant = variant_facts[row["variant_key"]]
        cross["period", row["variant_key"], row["stratum_key"], row["metric"]].append(row)
        without_tf = {k: v for k, v in variant.items() if k != "timeframe_minutes"}
        cross["timeframe", digest(without_tf), row["stratum_key"], row["metric"], tuple(row["period_key"])].append(row)
    for key, rows in cross.items():
        values = [r["mean"] for r in rows]
        stats = statistics(values)
        periods = sorted(set(tuple(r["period_key"]) for r in rows))
        timeframes = sorted({variant_facts[r["variant_key"]]["timeframe_minutes"] for r in rows})
        output.append(dict(level="cross_"+key[0], variant_key=key[1], stratum_key=key[2], metric=key[3],
            primary_values=[dict(variant_key=r["variant_key"], period_key=r["period_key"], value=r["mean"]) for r in rows],
            finite=stats["finite"], minimum=stats["minimum"], median=stats["median"], maximum=stats["maximum"],
            overlapping_periods=any(a[1] >= b[0] for a, b in zip(periods, periods[1:])), timeframes_minutes=timeframes,
            evaluation_geometry_varies=len(periods) > 1 or len(timeframes) > 1,
            mean=None, reason="independent runs; periods may overlap and timeframe/warmup geometry may differ; no pooled mean"))
    return output, groups


def _neighbours(accounts, axes):
    if not axes:
        return [dict(status="not_requested", reason="Declare --parameter-axis NAME for numeric strategy axes.")]
    if len(set(axes)) != len(axes):
        raise AnalysisError("duplicate parameter axis.")
    for axis in axes:
        if axis in set(ECONOMICS) | RUNTIME or any(axis not in r["strategy_params"] or
                type(r["strategy_params"][axis]) not in (int, float) or not finite(r["strategy_params"][axis]) for r in accounts):
            raise AnalysisError(f"parameter axis {axis!r} must be a finite numeric strategy scalar in every account.")
    configurations = defaultdict(dict)
    for row in accounts:
        key = row["stratum_key"], row["filter_key"], row["timeframe_minutes"]
        configurations[key][digest(row["strategy_params"])] = row["strategy_params"]
    output = []
    for center in accounts:
        key = center["stratum_key"], center["filter_key"], center["timeframe_minutes"]
        declarations = configurations[key]
        adjacent = []
        for axis in axes:
            values = sorted({p[axis] for p in declarations.values()})
            index = values.index(center["strategy_params"][axis])
            for pos in (index-1, index+1):
                if 0 <= pos < len(values):
                    params = center["strategy_params"] | {axis: values[pos]}
                    if digest(params) in declarations:
                        adjacent.append((axis, params))
        neighbors = []
        for axis, params in adjacent:
            match = next((r for r in accounts if r["stratum_key"] == key[0] and r["filter_key"] == key[1] and
                r["timeframe_minutes"] == key[2] and r["strategy_params"] == params and
                r["ticker"] == center["ticker"] and r["period_key"] == center["period_key"]), None)
            comparable = match is not None and all(match[k] == center[k] for k in ("input_pin", "admission", "timeframe_minutes"))
            neighbors.append((match, comparable))
            for metric in METRICS:
                a = center["metrics"].get(metric)
                b = match["metrics"].get(metric) if match is not None and comparable else None
                output.append(dict(level="edge", center_account_id=center["account_id"],
                    neighbor_account_id=match["account_id"] if match else None, ticker=center["ticker"], period_key=center["period_key"],
                    axis=axis, distance=params[axis]-center["strategy_params"][axis], metric=metric,
                    center=a, neighbor=b, difference=b-a if finite(a) and finite(b) else None,
                    comparable=comparable, status="comparable" if comparable else "incomparable",
                    reason=None if comparable else "different_target_pin_admission_or_evaluation_geometry" if match else "undeclared_ticker_period_cell"))
        values = [center["metrics"].get("net_profit_pct"), *[
            match["metrics"].get("net_profit_pct") if match is not None and comparable else None for match, comparable in neighbors]]
        output.append(dict(level="neighborhood", center_account_id=center["account_id"], ticker=center["ticker"],
            period_key=center["period_key"], metric="net_profit_pct", **neighbourhood(values, list(range(len(values))), 0)))
    return output


def _concentration(groups, calendar):
    output = []
    monthly = defaultdict(list)
    for row in calendar:
        monthly[row["account_id"]].append(row)
    compatible = defaultdict(list)
    for (variant, stratum, period), rows in groups.items():
        for row in rows:
            compatible[variant, stratum, period, row["input_pin"]["instrument"]["quote_currency"]].append(row)
    for (variant, stratum, period, currency), rows in compatible.items():
        complete = all(r["status"] == "completed" and finite(r["metrics"].get("net_profit")) for r in rows)
        base = dict(variant_key=variant, stratum_key=stratum, period_key=period,
            unit="saved_account_currency", currency=currency,
            group_expected_accounts=len(rows), group_completed_accounts=sum(r["status"] == "completed" for r in rows),
            interpretation="independent-account additive currency; no shared portfolio")
        output.extend(dict(**base, account_id=None, dimension="ticker", channel="realized_pnl", **r)
            for r in concentration([(r["ticker"], r["metrics"].get("net_profit")) for r in rows], complete=complete))
        for channel in ("realized_pnl", "equity_change"):
            combined = []
            for account in rows:
                values = [(r["month"], r[channel]) for r in monthly[account["account_id"]]]
                combined.extend(values)
                output.extend(dict(**base, account_id=account["account_id"], dimension="time", channel=channel, **r)
                    for r in concentration(values, complete=account["status"] == "completed"))
            output.extend(dict(**base, account_id=None, dimension="time", channel=channel, **r)
                for r in concentration(combined, complete=complete))
    return output


def compare_fixed(inputs, output_dir, *, universe=None, ticker_list=None, ticker_list_mode=None,
                  allow_partial=False, parameter_axes=()):
    """Compare labeled saved paths; retain only validated scalar/monthly snapshots."""
    inputs = list(inputs.items()) if isinstance(inputs, dict) else list(inputs)
    labels(inputs)
    if ticker_list is not None:
        if universe is not None:
            raise AnalysisError("provide universe or ticker_list, not both.")
        universe = load_universe(ticker_list)
    if universe is not None:
        universe.verify()
    captured, manifests, roots, sources = {}, {}, [], {}
    identities, seen_accounts = {}, {}
    for label, path in inputs:
        path = Path(path).absolute()
        saved = {}
        def capture(account, result, monthly_rows):
            saved[account["account_id"]] = (deepcopy(result), deepcopy(monthly_rows))
        manifest = open_dataset(path, allow_partial=allow_partial, on_account=capture)
        identity = manifest["contract_sha256"]
        if identity in identities:
            raise AnalysisError(f"duplicate source identity for {label!r} and {identities[identity]!r}.")
        identities[identity] = label
        for account in manifest["contract"]["accounts"]:
            aid = account["account_id"]
            if aid in seen_accounts:
                raise AnalysisError(f"duplicate account_id {aid} in {seen_accounts[aid]!r} and {label!r}.")
            seen_accounts[aid] = label
        captured[label], manifests[label] = saved, manifest
        contract = manifest["contract"]
        filters = contract["resolved"].get("entry_filters", [])
        roots.extend([path, *recorded_roots(filters), *(a["provenance"]["pack_root"] for a in contract["accounts"])])
        manifest_path = path/("manifest.json" if manifest["complete"] else "manifest.partial.json")
        sources[label] = dict(path=str(path), schema_version=manifest["schema_version"],
            manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(), manifest=deepcopy(manifest))
    selected, membership = _membership(universe, manifests, ticker_list_mode)
    accounts, calendar = [], []
    for label, manifest in manifests.items():
        contract = manifest["contract"]
        generation = contract["request"]["generation"]
        filters = contract["resolved"].get("entry_filters", [])
        for account in contract["accounts"]:
            if account["instrument_id"] not in selected[label]:
                continue
            result, months = captured[label].get(account["account_id"], ({}, []))
            params = {k: v for k, v in account["params"].items() if k not in set(ECONOMICS) | RUNTIME}
            economics = {k: account["params"][k] for k in ECONOMICS}
            variant = dict(strategy=generation["strategy"], params=params, filters=filters, timeframe_minutes=account["timeframe_minutes"])
            stratum = dict(strategy=generation["strategy"], execution_policy=generation["execution"]["policy"], economics=economics,
                funding_modelled=generation["economics"]["funding_modelled"], slippage_modelled=generation["economics"]["slippage_modelled"])
            failure = manifest["failure"]
            status = "completed" if result else "failed" if failure and failure["account_id"] == account["account_id"] else "pending"
            row = dict(dataset_label=label, schema_version=manifest["schema_version"], manifest_sha256=sources[label]["manifest_sha256"],
                account_id=account["account_id"], parameter_set=account["parameter_set"], period_name=account["period"],
                ticker=account["instrument_id"], cell=account["cell"], timeframe_minutes=account["timeframe_minutes"],
                period_key=_period(account), period_kind="independent_run", variant_key=digest(variant), variant=variant,
                stratum_key=digest(stratum), stratum=stratum, filter_key=digest(filters), strategy_params=params,
                params=account["params"], economics=economics, status=status,
                reason=None if status == "completed" else failure["reason"] if status == "failed" else "not_completed",
                input_pin=account["input_pin"], admission=_admission(account), provenance=account["provenance"],
                observation_count=result.get("observation_count"), metrics=result.get("metrics", {}),
                metric_units=result.get("metric_units", {}), metric_unavailable_reasons=result.get("metric_unavailable_reasons", {}))
            accounts.append(row)
            calendar.extend(dict(dataset_label=label, account_id=row["account_id"], variant_key=row["variant_key"],
                stratum_key=row["stratum_key"], ticker=row["ticker"], period_key=row["period_key"],
                period_kind="continuous_calendar_slice", **month) for month in months)
    summaries, groups = _summaries(accounts)
    neighbors = _neighbours(accounts, tuple(parameter_axes))
    concentrated = _concentration(groups, calendar)
    output = new_output(output_dir, roots)
    tables = dict(accounts=accounts, calendar=calendar, summaries=summaries, neighbours=neighbors, concentration=concentrated)
    for name, rows in tables.items():
        write_rows(output, name+".csv", rows)
    metadata = dict(schema_version="strategy_lab_fixed_comparison_v1", git=git_facts(), sources=sources,
        research_inputs=membership, parameter_axes=list(parameter_axes),
        classification="descriptive_independent_accounts", grain="variant x economics stratum x exact evaluation period; aggregate tickers only",
        account_count=len(accounts), completed_count=sum(r["status"] == "completed" for r in accounts),
        variants={r["variant_key"]: r["variant"] for r in accounts}, strata={r["stratum_key"]: r["stratum"] for r in accounts},
        units="saved metrics; return/drawdown percent; counts; concentration currency, never summed percentages",
        limitations="No chaining independent periods; no shared-capital portfolio; common membership is metric-specific and may have different saved sample facts.")
    return finish_report(output, "comparison.json", metadata, [n+".csv" for n in tables], "Continuous Fixed comparison",
        {k: metadata[k] for k in ("account_count", "completed_count", "grain", "limitations")},
        [("Summary coverage", [dict(metric=metric,
            primary_grains=sum(r["level"] == "primary" and r["metric"] == metric and r["membership"] == "available" for r in summaries),
            empty_common_grains=sum(r["level"] == "primary" and r["metric"] == metric and r["membership"] == "common" and r["finite"] == 0 for r in summaries))
            for metric in METRICS]), ("Declared account status", [dict(status=status, count=sum(r["status"] == status for r in accounts))
            for status in ("completed", "failed", "pending")])])
