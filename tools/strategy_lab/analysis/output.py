"""Deterministic five-file publication for Strategy Lab analysis."""

from __future__ import annotations

import csv
import io
import json
import math
import os
import uuid
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .dataset import AnalysisError
from .evaluate import AnalysisResult
from .allocation import AllocationResult


OUTPUT_FILES = (
    "run_metadata.json",
    "summary.json",
    "pair_decisions.csv",
    "monthly_results.csv",
    "report.md",
)

ALLOCATION_OUTPUT_FILES = (
    "run_metadata.json",
    "summary.json",
    "pair_decisions.csv",
    "monthly_results.csv",
    "ticker_allocations.csv",
    "report.md",
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Mapping):
        return {str(key): _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(child) for child in value]
    return value


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            _jsonable(value),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _csv_bytes(rows: tuple[Mapping[str, Any], ...], columns: tuple[str, ...]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        encoded = {}
        for column in columns:
            value = _jsonable(row.get(column))
            if isinstance(value, (list, dict)):
                encoded[column] = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            elif value is None:
                encoded[column] = ""
            else:
                encoded[column] = value
        writer.writerow(encoded)
    return stream.getvalue().encode("utf-8")


def _report(result: AnalysisResult) -> bytes:
    metadata, summary = result.run_metadata, result.summary
    dataset = metadata["dataset"]
    scope = metadata["analysis_scope"]
    population = summary["population"]
    descriptive = metadata.get("period_unit") == "oos_block"

    def display(value: Any) -> str:
        if value is None:
            return "unavailable"
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)):
            return f"{float(value):.6f}"
        return str(value)

    actual_ids = scope["actual_window_ids"]
    missing_ids = scope["missing_window_ids"]
    smoke_note = (
        " **SMOKE / NON-FULL DATASET:** completeness does not make this a full dataset."
        if dataset["scope"] == "smoke"
        else ""
    )

    lines = [
        "# Strategy Lab analysis report",
        "",
        f"- Dataset schema/scope/status: `{dataset['schema_version']}` / `{dataset['scope']}` / `{dataset['status']}`.{smoke_note}",
        f"- Manifest SHA-256: `{dataset['manifest_sha256']}`",
        *(["- Entry filters and saved source facts: " + json.dumps(dataset["entry_filters"], sort_keys=True)] if "entry_filters" in dataset else []),
        f"- Requested analysis scope/cell: `{scope['name']}` / `{scope['ticker_cell']}`",
        f"- Actual tickers / full ticker-cell authority: {scope['ticker_count']} / {scope['ticker_cell_count']}",
        f"- Actual calendar blocks: {scope['actual_calendar_block_count']}",
        f"- Declared / actual / missing window IDs: `{scope['declared_window_ids']}` / `{actual_ids}` / `{missing_ids}`",
        f"- Partial scope: `{str(scope['is_partial']).lower()}`",
        f"- Scope requires unlock: `{str(scope['requires_unlock']).lower()}`",
        f"- Population observations: {population['observation_count']}",
        *([
            f"- OOS Net Profit headline mean (equal instrument/block weights): {display(population['mean'])}",
            f"- OOS Net Profit median (pooled finite observations): {display(population['median'])}",
            f"- OOS profitable share (pooled, all observations in denominator): {display(population['profitable_share'])}",
            "",
            "The population headline mean first averages finite candidate returns within each instrument/block, then gives equal weights to available instruments within a block and equal weights to available blocks.",
            "Population median and quantiles pool finite observations. Profitable share pools positive finite observations over all observations, including unavailable entries in the denominator. These statistics do not use the headline's hierarchical weights.",
            "Generic diagnostic summaries of other metrics pool finite observations. Net profit has no generic metric-summary output.",
        ] if descriptive else [
            f"- OOS Net Profit mean / median: {display(population['mean'])} / {display(population['median'])}",
            f"- OOS profitable share: {display(population['profitable_share'])}",
        ]),
        "",
        "### Actual UTC calendar blocks",
        "",
        "| Window | Block key (OOS start, OOS end) | IS boundaries |",
        "|---:|---|---|",
    ]
    for block in scope["utc_blocks"]:
        lines.append(
            f"| {block['window_id']} | `{block['oos_start']}` — `{block['oos_end']}` "
            f"| `{block['is_start']}` — `{block['is_end']}` |"
        )
    if scope.get("unlock_evidence") is not None:
        unlock = scope["unlock_evidence"]
        lines.extend(
            [
                "",
                "### Holdout unlock evidence",
                "",
                f"- Policy: `{unlock['policy_ref']}`",
                f"- Policy SHA-256: `{unlock['policy_sha256']}`",
                f"- Unlock code commit / dirty: `{unlock['code_commit']}` / `{str(unlock['dirty_worktree']).lower()}`",
            ]
        )
    lines.extend(
        [
        "",
        "## Rule results",
        "",
        "| Rule | Kind/state | Selected pairs | Unavailable reason | Top-1 headline | Lift | Robust lift |",
        "|---|---|---:|---|---:|---:|---:|",
        ]
    )
    for name, row in summary["rules"].items():
        lines.append(
            f"| `{name}` | {row['rule_kind']} / {row['rule_status']} | {row['selected_pairs']} / {row['total_pairs']} "
            f"| {row['unavailable_pair_reason'] or ''} "
            f"| {display(row['top1_headline_mean'])} | {display(row['top1_lift_headline'])} "
            f"| {display(row['robustness']['robust_lift_headline'])} |"
        )
    unsupported = [
        (name, state)
        for name, state in metadata["rule_states"].items()
        if state["status"] != "supported"
    ]
    if unsupported:
        lines.extend(
            [
                "",
                "### Unsupported registered rules",
                "",
                "| Rule | Kind | State/reason |",
                "|---|---|---|",
            ]
        )
        for name, state in unsupported:
            reason = "missing metrics: " + ", ".join(state["missing_metrics"])
            lines.append(f"| `{name}` | {state['kind']} | {state['status']}: {reason} |")

    if metadata.get("period_unit") == "oos_block":
        lines.extend(["", f"Returns cover {metadata['oos_period_months']}-month OOS blocks. `monthly_*` fields and `monthly_results.csv` are historical per-block aliases; no annualization is applied.",
                      "Evidence, nominations, bootstrap intervals and outlier removal: not_requested (strategy_lab_descriptive_v1).", ""])
    else:
        lines.extend(
            [
                "",
                "## Descriptive month-block bootstrap intervals",
                "",
                "These intervals are descriptive only. With five or six blocks they are weak and are not statistical proof.",
                "",
                "| Rule | Headline | Effect | Blocks | CI95 low | CI95 high | CI95 width | Status/reason |",
                "|---|---|---:|---:|---:|---:|---:|---|",
            ]
        )
        selectable = metadata["contracts"]["rule_registry"]["selectable_rules"]
        for name in selectable:
            row = summary["rules"].get(name)
            if row is None:
                continue
            for headline, interval in row["bootstraps"].items():
                reason = interval["status"] if interval["reason"] is None else f"{interval['status']}: {interval['reason']}"
                lines.append(
                    f"| `{name}` | `{headline}` | {display(interval['effect'])} | {interval['block_count']} "
                    f"| {display(interval['lower'])} | {display(interval['upper'])} "
                    f"| {display(interval['width'])} | {reason} |"
                )

        lines.extend(["", "## Mechanical evidence", ""])
        evidence_by_rule = summary["evidence_by_selectable_rule"]
        for name in selectable:
            rows = evidence_by_rule.get(name)
            if rows is None:
                state = metadata["rule_states"][name]
                lines.extend(
                    [
                        f"### `{name}`",
                        "",
                        f"Evidence unavailable: {state['status']}; missing metrics: {', '.join(state['missing_metrics'])}.",
                        "",
                    ]
                )
                continue
            lines.extend(
                [
                    f"### `{name}`",
                    "",
                    "| Criterion | Operator | Threshold | Of | Observed | Status | Reason |",
                    "|---|---:|---:|---:|---:|---|---|",
                ]
            )
            for evidence in rows:
                declared_of = "" if evidence["of"] is None else display(evidence["of"])
                lines.append(
                    f"| `{evidence['criterion']}` | `{evidence['operator']}` | {display(evidence['threshold'])} "
                    f"| {declared_of} | {display(evidence['observed'])} "
                    f"| {evidence['status']} | {evidence['reason'] or ''} |"
                )
            lines.append("")

    diagnostics = summary["diagnostics"]
    lines.extend(
        [
            "## Guardrails and availability diagnostics",
            "",
            "| Flag bit | Value | OOS observation count | Interpretation |",
            "|---|---:|---:|---|",
        ]
    )
    interpretations = {
        "rejected_fill": "strategy-filter diagnostic; not automatically an execution fault",
        "invalid_stop_distance": "execution-fault flag",
        "zero_size_entry": "execution-fault flag",
    }
    for name, bit in diagnostics["known_flag_bits"].items():
        lines.append(
            f"| `{name}` | {bit} | {diagnostics['known_flag_bit_counts'][name]} | {interpretations[name]} |"
        )
    lines.append(f"\n- Unknown flag bits (bitwise mask): `{diagnostics['unknown_flag_bits']}`")
    faults = diagnostics["guardrail_fault_observation_counts"]
    lines.append(
        f"- Fault observations: `zero_size_entry_count={faults['zero_size_entry_count']}`, "
        f"`invalid_stop_distance_count={faults['invalid_stop_distance_count']}`."
    )
    rejected = diagnostics["guardrails"].get("rejected_fill_count", {})
    lines.append(
        f"- Rejected-fill diagnostic observations: `{rejected.get('nonzero_observation_count', 'unavailable')}` "
        "(not classified wholesale as execution faults)."
    )
    lines.extend(
        [
            "",
            "### Metric availability",
            "",
            "| Segment | Metric | Finite | Unavailable |",
            "|---|---|---:|---:|",
        ]
    )
    for segment, metrics in diagnostics["metric_summaries"].items():
        for metric, facts in metrics.items():
            if facts["unavailable_count"]:
                lines.append(
                    f"| {segment} | `{metric}` | {facts['finite_count']} | {facts['unavailable_count']} |"
                )
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "This artifact applies frozen formulas and reports evidence mechanically. It does not nominate rules, allocate tickers, or conclude strategy quality. "
            + ("" if descriptive else "The descriptive month-block intervals are weak with five or six blocks."),
            "",
        ]
    )
    lines.extend(_research_report(metadata))
    return "\n".join(lines).encode("utf-8")


def render_files(result: AnalysisResult) -> Mapping[str, bytes]:
    pair_columns = (
        "rule",
        "rule_kind",
        "result_label",
        "ticker",
        "window_id",
        "is_start",
        "is_end",
        "oos_start",
        "oos_end",
        "status",
        "reason",
        "selected_candidate_ids",
        "selected_semantic_keys",
        "ordered_scores",
        "ordered_is_net_profit_pct",
        "required_is_metrics",
        "matching_oos_metrics",
        "tie_depth_at_rank1",
        "tie_break_level_used",
        "top1_oos_net_profit_pct",
        "top5_oos_net_profit_pct",
        "top10_oos_net_profit_pct",
        "top1_oos_percentile",
        "top5_mean_individual_oos_percentile",
    )
    monthly_columns = (
        "row_kind",
        "rule",
        "window_id",
        "oos_start",
        "oos_end",
        "top1_mean",
        "top1_median",
        "top1_profitable_share",
        "top1_lift_mean",
        "top5_lift_mean",
        "population_mean",
        "population_median",
        "population_profitable_share",
    )
    return {
        "run_metadata.json": _json_bytes(result.run_metadata),
        "summary.json": _json_bytes(result.summary),
        "pair_decisions.csv": _csv_bytes(result.pair_decisions, pair_columns),
        "monthly_results.csv": _csv_bytes(result.monthly_results, monthly_columns),
        "report.md": _report(result),
    }


def _research_report(metadata):
    if "research_inputs" not in metadata:
        return []
    inputs = metadata["research_inputs"]
    lines = ["", "## Exploratory research inputs", "",
             "These files declare analysis choices; they do not establish independent confirmation or unlock a scope."]
    universe = inputs["universe"]
    if universe:
        lines.extend(["", f"Universe: {universe['document']['name']} ({universe['mode']}).",
            f"Selection note: {universe['document']['selection_note']}",
            f"Source: `{universe['path']}`; file SHA-256 `{universe['file_sha256']}`; document SHA-256 `{universe['document_sha256']}`.",
            f"Requested IDs: `{universe['requested_tickers']}`.",
            f"Resolved IDs: `{universe['resolved_tickers']}`; membership SHA-256 `{universe['resolved_tickers_sha256']}`."])
        lines.append("")
        for label, facts in universe["datasets"].items():
            lines.append(f"- {label}: authorized {facts['authorized_ticker_count']}, cell authority {facts['ticker_cell_count']}; "
                f"excluded {facts['excluded_tickers']}; missing requested {facts['missing_requested_tickers']}; "
                f"partial {facts['is_partial']}; unlock evidence {json.dumps(facts['unlock_evidence'], sort_keys=True)}.")
    policy = inputs["candidate_policy"]
    if policy:
        lines.extend(["", f"Candidate policy: {policy['name']} / {policy['version']}; `{policy['key']}` (exploratory).",
            f"Source: `{policy['path']}`; file SHA-256 `{policy['file_sha256']}`; document SHA-256 `{policy['document_sha256']}`.",
            "Definition: `" + json.dumps(policy["document"], sort_keys=True) + "`.",
            f"Required IS metrics: {policy['required_metrics']}; score direction: {policy['score_direction']}.",
            f"Ties: {policy['ties']}. No eligible candidate: {policy['no_eligible']}.",
            "Top-5/10: not_requested, selection_count=1. Policy results are excluded from official evidence/nominations.",
            "Allocation still ranks tickers by selected IS net profit and uses fixed 1/K weights."])
    return lines


def _allocation_report(result: AllocationResult) -> bytes:
    metadata, summary = result.run_metadata, result.summary
    scorer = metadata["ticker_scorer"]
    alignment = metadata["alignment"]
    lines = [
        "# Strategy Lab fixed-capacity allocation report",
        "",
        f"- Candidate rule: `{metadata['candidate_rule']}`",
        f"- Ticker scorer: `{scorer['name']}` / `{scorer['version']}` / "
        f"`{scorer['identity_sha256']}`",
        f"- Primary / sensitivity K: `{metadata['primary_k']}` / `{metadata['sensitivity_k']}`",
        f"- Common tickers / exact UTC blocks: `{alignment['common_ticker_count']}` / "
        f"`{alignment['common_block_count']}`",
        "- Allocation return uses fixed `1/K` slot weights; unfilled slots remain cash.",
        "- `requested_capacity_fraction` is K/available; canonical `selectivity` is "
        "selected/available.",
        "",
        "## Dataset results",
        "",
    ]
    if metadata.get("period_unit") == "oos_block":
        lines.extend([f"OOS block lengths in months by dataset: `{metadata['oos_period_months']}`.",
                      "`monthly_*` fields and `monthly_results.csv` are historical OOS-block aliases; returns are not annualized.",
                      "Matched fraction, evidence, nominations, bootstrap intervals and outlier removal: not_requested (strategy_lab_descriptive_v1).", ""])
    for label in metadata["dataset_labels"]:
        if "entry_filters" in metadata["datasets"][label]:
            lines.append(f"- {label} entry filters and saved source facts: " + json.dumps(metadata["datasets"][label]["entry_filters"], sort_keys=True))
        facts = summary["datasets"][label]
        lines.extend(
            [
                f"### `{label}`",
                "",
                f"- Manifest SHA-256: `{facts['manifest_sha256']}`",
                f"- Declared/requested/common tickers: "
                f"`{metadata['datasets'][label]['declared_ticker_count']}` / "
                f"`{facts['requested_ticker_count']}` / "
                f"`{facts['common_ticker_count']}`",
                f"- Omitted tickers: `{facts['omitted_tickers']}`",
                f"- Omitted exact UTC blocks: `{facts['omitted_blocks']}`",
                "",
                "| Variant | K values | Mean capacity return | Compounded return | "
                "Monthly-series MaxDD | Mean turnover | Bootstrap CI95 / width |",
                "|---|---|---:|---:|---:|---:|---|",
            ]
        )
        for name, variant in facts["variants"].items():
            portfolio = variant["portfolio"]

            def show(value: Any) -> str:
                return "unavailable" if value is None else f"{float(value):.6f}"

            lines.append(
                f"| `{name}` ({variant['label']}) | `{variant['ks']}` | "
                f"{show(variant['headline_mean_capacity_return_pct'])} | "
                f"{show(portfolio['compounded_return_pct'])} | "
                f"{show(portfolio['monthly_series_max_drawdown_pct'])} | "
                f"{show(variant['turnover']['mean_turnover'])} | "
                f"{show(variant['bootstrap']['lower'])} to "
                f"{show(variant['bootstrap']['upper'])} / "
                f"{show(variant['bootstrap']['width'])} "
                f"({variant['bootstrap']['status']}) |"
            )
        lines.extend(["", "### K-specific block controls and spreads", ""])
        for block in facts["blocks"]:
            lines.append(
                f"- `{block['block_key']}`: available `{block['available_tickers']}`, "
                f"unavailable `{block['unavailable_tickers']}`."
            )
            for name, variant in block["variants"].items():
                lines.append(
                    f"  - `{name}` K={variant['k']}: capacity="
                    f"`{variant['capacity_return_pct']}`, all_available="
                    f"`{variant['all_available_mean_pct']}`, bottom="
                    f"`{variant['bottom_k']['capacity_return_pct']}`, random_mean="
                    f"`{variant['random_k']['random_mean_pct']}`, oracle="
                    f"`{variant['oracle_k']['capacity_return_pct']}`, anti_oracle="
                    f"`{variant['anti_oracle_k']['capacity_return_pct']}`, percentile="
                    f"`{variant['random_k']['top_k_random_percentile_fraction']}`; "
                    f"selected/cash=`{variant['selected_count']}`/`{variant['cash_slots']}`, "
                    f"requested/realized/selectivity="
                    f"`{variant['requested_capacity_fraction']}`/"
                    f"`{variant['realized_selected_fraction']}`/`{variant['selectivity']}`, "
                    f"spreads=`{variant['spreads']}`."
                )
        lines.append("")
    lines.extend(
        [
            "## Interpretation boundary",
            "",
            "This is development-only software evidence. Oracle controls are hindsight and "
            "non-deployable. Calendar-block compounding and drawdown are not bar-level "
            "portfolio equity. No policy is nominated and no strategy-quality conclusion is "
            "made.",
            "",
            "## Access proof and limitations",
            "",
        ]
    )
    for label, audit in metadata["load_audit"].items():
        window_ids = sorted({entry[1] for entry in audit["entries"]})
        segments = sorted({entry[2] for entry in audit["entries"]})
        lines.append(
            f"- `{label}` loaded window IDs `{window_ids}`, segments `{segments}`; "
            f"outside-scope loads: `{audit['outside_scope_loads']}`."
        )
    lines.extend(f"- {item}" for item in metadata["limitations"])
    lines.append("")
    lines.extend(_research_report(metadata))
    return "\n".join(lines).encode("utf-8")


def render_allocation_files(result: AllocationResult) -> Mapping[str, bytes]:
    pair_columns = (
        "dataset_label", "candidate_rule", "ticker_scorer", "ticker", "window_id",
        "oos_start", "oos_end", "status", "reason", "candidate_id", "semantic_key",
        "candidate_rule_score", "ticker_score", "is_net_profit_pct", "is_total_trades",
        "oos_return_pct",
    )
    monthly_columns = (
        "dataset_label", "candidate_rule", "ticker_scorer", "variant", "k", "window_id",
        "oos_start", "oos_end", "capacity_return_pct", "conditional_selected_mean_pct",
        "all_available_mean_pct", "bottom_k_return_pct", "random_mean_pct",
        "oracle_k_return_pct", "anti_oracle_k_return_pct",
        "top_k_random_percentile_fraction", "status", "reason",
    )
    allocation_columns = (
        "dataset_label", "candidate_rule", "ticker_scorer", "window_id", "oos_start",
        "oos_end", "row_kind", "variant", "allocation_kind", "k",
        "available_tickers", "selected_count", "cash_slots",
        "requested_capacity_fraction", "realized_selected_fraction", "selectivity",
        "ticker", "rank", "candidate_id", "semantic_key", "candidate_rule_score",
        "ticker_score", "is_net_profit_pct", "is_total_trades", "oos_return_pct",
        "slot_weight", "capacity_contribution_pct", "draw_count", "base_seed",
        "derived_seed", "seed_payload_sha256", "random_mean_pct", "random_median_pct",
        "top_k_random_percentile_fraction", "deployable", "diagnostic", "hindsight",
        "non_deployable", "status", "reason",
    )
    return {
        "run_metadata.json": _json_bytes(result.run_metadata),
        "summary.json": _json_bytes(result.summary),
        "pair_decisions.csv": _csv_bytes(result.pair_decisions, pair_columns),
        "monthly_results.csv": _csv_bytes(result.monthly_results, monthly_columns),
        "ticker_allocations.csv": _csv_bytes(result.ticker_allocations, allocation_columns),
        "report.md": _allocation_report(result),
    }


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _protect_filter_output(output, datasets, metadata, names):
    from ..filters import protect_output, recorded_roots
    from ..fixed.storage import safe_path
    roots = list(datasets)
    for facts in metadata:
        roots.extend(recorded_roots(facts["effective"]))
        roots.extend(i["target_root"] for i in facts["instruments"])
    protect_output(output, roots)
    for name in names:
        safe_path(output, name)


def write_analysis(
    result: AnalysisResult,
    output_dir: str | Path,
    *,
    dataset_root: str | Path,
) -> Mapping[str, Any]:
    output = Path(output_dir).resolve()
    dataset = Path(dataset_root).resolve()
    if "entry_filters" in result.run_metadata["dataset"]:
        _protect_filter_output(output_dir, [dataset], [result.run_metadata["dataset"]["entry_filters"]], OUTPUT_FILES)
    if output == dataset or _inside(output, dataset):
        raise AnalysisError("analysis output must be outside every input dataset root.")
    files = render_files(result)
    if output.exists() and not output.is_dir():
        raise AnalysisError(f"analysis output is not a directory: {output}.")
    if output.exists():
        existing = {path.name for path in output.iterdir()}
        unknown = sorted(existing - set(OUTPUT_FILES))
        if unknown:
            raise AnalysisError(f"analysis output contains unexpected files: {unknown}.")
        if existing:
            if existing == set(OUTPUT_FILES) and all(
                (output / name).read_bytes() == content for name, content in files.items()
            ):
                return {"status": "verified_noop", "output": str(output), "files": list(OUTPUT_FILES)}
            raise AnalysisError(
                "analysis output already contains nonmatching results; choose a new directory."
            )
    output.mkdir(parents=True, exist_ok=True)
    published: list[Path] = []
    try:
        for name in OUTPUT_FILES:
            target = output / name
            temporary = output / f".{name}.{uuid.uuid4().hex}.tmp"
            temporary.write_bytes(files[name])
            os.replace(temporary, target)
            published.append(target)
    except OSError as exc:
        for target in published:
            try:
                target.unlink()
            except OSError:
                pass
        raise AnalysisError(f"cannot publish analysis output: {exc}") from exc
    return {"status": "published", "output": str(output), "files": list(OUTPUT_FILES)}


def write_allocation(
    result: AllocationResult,
    output_dir: str | Path,
    *,
    dataset_roots: tuple[str | Path, ...],
) -> Mapping[str, Any]:
    output = Path(output_dir).resolve()
    datasets = tuple(Path(root).resolve() for root in dataset_roots)
    filters = [v["entry_filters"] for v in result.run_metadata["datasets"].values() if "entry_filters" in v]
    if filters:
        _protect_filter_output(output_dir, datasets, filters, ALLOCATION_OUTPUT_FILES)
    if not datasets:
        raise AnalysisError("allocation publication requires input dataset roots.")
    if any(output == dataset or _inside(output, dataset) for dataset in datasets):
        raise AnalysisError("allocation output must be outside every input dataset root.")
    files = render_allocation_files(result)
    if output.exists() and not output.is_dir():
        raise AnalysisError(f"allocation output is not a directory: {output}.")
    if output.exists():
        existing = {path.name for path in output.iterdir()}
        unknown = sorted(existing - set(ALLOCATION_OUTPUT_FILES))
        if unknown:
            raise AnalysisError(f"allocation output contains unexpected files: {unknown}.")
        if existing:
            if existing == set(ALLOCATION_OUTPUT_FILES) and all(
                (output / name).read_bytes() == content for name, content in files.items()
            ):
                return {
                    "status": "verified_noop",
                    "output": str(output),
                    "files": list(ALLOCATION_OUTPUT_FILES),
                }
            raise AnalysisError(
                "allocation output already contains nonmatching results; choose a new directory."
            )
    output.mkdir(parents=True, exist_ok=True)
    published: list[Path] = []
    try:
        for name in ALLOCATION_OUTPUT_FILES:
            target = output / name
            temporary = output / f".{name}.{uuid.uuid4().hex}.tmp"
            temporary.write_bytes(files[name])
            os.replace(temporary, target)
            published.append(target)
    except OSError as exc:
        for target in published:
            try:
                target.unlink()
            except OSError:
                pass
        raise AnalysisError(f"cannot publish allocation output: {exc}") from exc
    return {
        "status": "published",
        "output": str(output),
        "files": list(ALLOCATION_OUTPUT_FILES),
    }
