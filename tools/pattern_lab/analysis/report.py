"""The standalone offline analysis report.

The page is a light-theme, escaped HTML document with no CDN, no network request
and no required JavaScript package.  It renders the sealed summary exactly as
saved: no bootstrap is rerun, and the original study, its market pack and any
saved module are all unnecessary.  The approximation qualification, the nominal
levels and the long-dependence limitation sit next to the inference table rather
than in a footnote.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any, Mapping, Sequence

BANNER = (
    "Exploratory development analysis — UNVALIDATED inference: the declared calibration "
    "envelope was not met. Not a validated edge."
)

APPROXIMATION = (
    "Tests are nominally two-sided at alpha 0.05 and intervals are nominally 95% pointwise basic "
    "intervals. The calendar block bootstrap is an asymptotic approximation and its delivered "
    "calibration DID NOT MEET the declared empirical error envelope: on the tracked synthetic "
    "fixtures with persistent daily signal states the measured rejection and noncoverage rates "
    "were about 7.5-8.3% against a nominal 5%, because the bootstrap variance is roughly 10% too "
    "small there. This inference is therefore unvalidated and anti-conservative under clustered "
    "signals; it certifies no error rate, on these fixtures or on real market data."
)

LONG_DEPENDENCE = (
    "Known assumption limitation: a seven-day block does not control error under dependence "
    "substantially longer than a week, and the tracked measurements show it is already "
    "anti-conservative when the daily signal state is persistent. The software does not detect "
    "such dependence automatically in a real run. Treat a nominal rejection as a screening hint "
    "that needs independent confirmation, never as evidence of an edge."
)

COMPLETION_AUTHORITY = (
    "Authoritative completion is a matching verified completion.json: the analysis artifacts must "
    "still hash to the values that record names. This regenerable page is not a completion check, "
    "and artifact completion is not statistical availability."
)

STYLE = """
:root { color-scheme: light; }
body { margin: 0; padding: 24px; background: #f7f8fa; color: #1c2430;
       font-family: "Segoe UI", Roboto, Helvetica, Arial, sans-serif; font-size: 14px; }
h1 { font-size: 22px; margin: 0 0 4px; }
h2 { font-size: 17px; margin: 28px 0 8px; border-bottom: 1px solid #d7dbe2; padding-bottom: 4px; }
h3 { font-size: 15px; margin: 18px 0 6px; }
.banner { background: #fff4d6; border: 1px solid #e0b94a; border-radius: 6px;
          padding: 12px 14px; font-weight: 600; margin: 12px 0 18px; }
.qualify { background: #eef3fb; border: 1px solid #b9cbe6; border-radius: 6px;
           padding: 10px 12px; margin: 8px 0 12px; }
.card { background: #ffffff; border: 1px solid #dfe3e9; border-radius: 6px;
        padding: 14px 16px; margin-bottom: 16px; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 12px; background: #ffffff; }
th, td { border: 1px solid #dfe3e9; padding: 5px 8px; text-align: right; }
th { background: #eef1f5; font-weight: 600; text-align: right; }
th.key, td.key { text-align: left; }
caption { caption-side: top; text-align: left; font-weight: 600; padding: 4px 0; }
.small { color: #5a6474; font-size: 12px; }
ul.notes { margin: 6px 0 0 18px; padding: 0; }
ul.notes li { margin: 2px 0; }
code { background: #eef1f5; padding: 1px 4px; border-radius: 3px; }
details { margin: 4px 0 10px; }
summary { cursor: pointer; color: #29486b; }
.primary { background: #eaf3ff; }
"""

PERCENT = 100.0


def _pct(value: Any, digits: int = 4) -> str:
    """Render a fractional return as a labelled percentage value."""
    if value is None:
        return "—"
    return f"{float(value) * PERCENT:.{digits}f}"


def _number(value: Any, digits: int = 6) -> str:
    if value is None:
        return "—"
    return f"{float(value):.{digits}f}"


def _integer(value: Any) -> str:
    return "—" if value is None else f"{int(value)}"


def _flag(value: Any) -> str:
    if value is None:
        return "—"
    return "yes" if value else "no"


def _rows(rows: Sequence[Sequence[Any]], *, header: Sequence[str], caption: str = "") -> str:
    parts = ["<table>"]
    if caption:
        parts.append(f"<caption>{escape(caption)}</caption>")
    parts.append("<thead><tr>")
    for index, name in enumerate(header):
        css = ' class="key"' if index == 0 else ""
        parts.append(f"<th{css}>{escape(str(name))}</th>")
    parts.append("</tr></thead><tbody>")
    for row in rows:
        parts.append("<tr>")
        for index, cell in enumerate(row):
            css = ' class="key"' if index == 0 else ""
            parts.append(f"<td{css}>{escape(str(cell))}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def _definition(pairs: Sequence[tuple[str, Any]]) -> str:
    return _rows(
        [[key, "" if value is None else str(value)] for key, value in pairs],
        header=["Field", "Value"],
    )


def _estimate_rows(members: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    for member in members:
        rows.append(
            [
                member["member_id"],
                _pct(member["signal"]),
                _pct(member["control"]),
                _pct(member["lift"]),
                _pct(member["signal_gross"]),
                _pct(member["control_gross"]),
                _pct(member["lift_gross"]),
                _pct(member["signal_commission"]),
                _integer(member["supported_population"]["retained_target_observations"]),
                _integer(member["supported_population"]["retained_control_observations"]),
            ]
        )
    return _rows(
        rows,
        header=[
            "Member", "Signal net %", "Control net %", "Net lift pp", "Signal gross %",
            "Control gross %", "Gross lift pp", "Signal commission pp", "Retained targets",
            "Retained controls",
        ],
        caption=(
            "Event-weighted means over the supported population. Values are percentages; lift is "
            "in percentage points. Commission is included; funding and slippage are excluded."
        ),
    )


def _inference_rows(members: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    for member in members:
        interval = member["intervals"]["lift"]
        rows.append(
            [
                member["member_id"],
                _pct(member["lift"]),
                "—" if interval is None else _pct(interval["lower"]),
                "—" if interval is None else _pct(interval["upper"]),
                _number(member["p_raw"], 4),
                _number(member["p_holm"], 4),
                _flag(member["nominal_reject_holm"]),
                member["effect_sign"] or "—",
                ", ".join(member["unavailable_reasons"]) or "—",
            ]
        )
    return _rows(
        rows,
        header=[
            "Member", "Net lift pp", "Interval lower pp", "Interval upper pp", "Raw p",
            "Holm-adjusted p", "Nominal Holm rejection", "Sign", "Unavailable reasons",
        ],
        caption=(
            "Pointwise nominal 95% basic intervals on the lift, with the raw and Holm-adjusted "
            "two-sided lift p-values. These are not simultaneous Holm-adjusted intervals."
        ),
    )


def _support_rows(members: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    for member in members:
        population = member["supported_population"]
        geometry = member["geometry"]
        counts = member["counts"]
        rows.append(
            [
                member["member_id"],
                _integer(counts["valid_target_available"]),
                _integer(population["retained_target_observations"]),
                _number(population["retained_target_share"], 4),
                _integer(counts["target_unavailable"]),
                _integer(counts["target_invalid_return"]),
                _integer(population["excluded_target_observations"]),
                _integer(population["retained_overlapping_anchors"]),
                _number(population["mean_inclusive_parent_share"], 4),
                _integer(geometry["retained_strata"]),
                _integer(len(population["retained_instruments"])),
                _integer(len(population["excluded_instruments"])),
                _integer(len(population["retained_months"])),
                _integer(len(population["excluded_months"])),
            ]
        )
    return _rows(
        rows,
        header=[
            "Member", "Valid targets available", "Retained targets", "Retained share",
            "Lost: unavailable condition", "Lost: invalid outcome", "Lost: stratum support",
            "Overlapping anchors", "Mean inclusive-parent share", "Retained strata",
            "Instruments included", "Instruments excluded", "Months included",
            "Months excluded",
        ],
        caption=(
            "Support and loss accounting. A support-filtered mean is never presented as the mean "
            "of all original signals."
        ),
    )


def _geometry_rows(members: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    for member in members:
        geometry = member["geometry"]
        rows.append(
            [
                member["member_id"],
                _integer(geometry["day_grid_days"]),
                _integer(geometry["block_length_days"]),
                _integer(geometry["k_draw"]),
                _integer(geometry["supported_span_days"]),
                _integer(geometry["joint_active_days"]),
                _integer(geometry["supported_blocks"]),
                _flag(member["horizon_population"]["agrees_across_horizons"]),
            ]
        )
    return _rows(
        rows,
        header=[
            "Member", "Day grid T", "Block L", "K_draw", "Supported span (days)",
            "Joint active days", "Supported blocks", "Same population across horizons",
        ],
        caption=(
            "Calendar and block geometry. K_draw is the number of bootstrap block draws per "
            "replicate, not the amount of supported data, and these are coverage diagnostics, not "
            "independent-sample-size estimates."
        ),
    )


def _bootstrap_rows(members: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    for member in members:
        for name in ("signal", "control", "lift"):
            record = member["bootstrap"][name]
            rows.append(
                [
                    member["member_id"],
                    name,
                    "—" if record is None else _number(record["standard_deviation"]),
                    "—" if record is None else _number(record["quantile_0025"]),
                    "—" if record is None else _number(record["quantile_0975"]),
                ]
            )
    return _rows(
        rows,
        header=["Member", "Estimator", "Bootstrap SD", "2.5% quantile", "97.5% quantile"],
        caption=(
            "Bootstrap diagnostics in fractional-return units. An unsupported member keeps null "
            "diagnostics while its coverage geometry stays present."
        ),
    )


def _raw_rows(members: Sequence[Mapping[str, Any]]) -> str:
    rows = [
        [
            member["member_id"],
            _integer(member["raw_valid_target"]["n"]),
            _pct(member["raw_valid_target"]["mean_net_return"]),
            _pct(member["raw_valid_target"]["mean_gross_return"]),
        ]
        for member in members
    ]
    return _rows(
        rows,
        header=["Member", "Valid targets", "Raw mean net %", "Raw mean gross %"],
        caption=(
            "Raw valid target summaries before matching and before stratum support exclusions, "
            "kept clearly separate from the supported estimates above."
        ),
    )


def _ticker_details(members: Sequence[Mapping[str, Any]]) -> str:
    parts = ["<details><summary>Ticker and month descriptive diagnostics</summary>"]
    parts.append(
        '<p class="small">These descriptive companions carry no p-value and no significance badge '
        "in this delivery. The equal-ticker companion averages defined per-ticker estimates, where "
        "each ticker's own months use its target-count weights; a missing ticker is omitted with "
        "its denominator disclosed, never assigned zero.</p>"
    )
    for member in members:
        companion = member["equal_ticker"]
        rows = [
            [
                item["instrument_id"],
                _pct(item["signal"]),
                _pct(item["control"]),
                _pct(item["lift"]),
                _integer(item["retained_target_observations"]),
                _integer(item["retained_strata"]),
            ]
            for item in companion["per_ticker"]
        ]
        if not rows:
            continue
        parts.append(
            _rows(
                rows,
                header=[
                    "Instrument", "Signal net %", "Control net %", "Net lift pp",
                    "Retained targets", "Retained strata",
                ],
                caption=(
                    f"{member['member_id']} — equal-ticker mean lift "
                    f"{_pct(companion['lift'])} pp over {companion['tickers']} ticker(s); omitted: "
                    + (", ".join(companion["omitted_instruments"]) or "none")
                ),
            )
        )
    parts.append("</details>")
    return "".join(parts)


def _comparison_rows(summary: Mapping[str, Any]) -> str:
    rows = [
        [item["comparison_id"], item["kind"], item["target_variant"],
         item["control_variant"] or "known-false condition anchors", item["label"]]
        for item in summary["comparisons"]
    ]
    return _rows(
        rows,
        header=["Comparison", "Kind", "Target variant", "Control population", "Label"],
        caption=(
            "The declared comparison family. A comparison is called an inclusive parent only where "
            "that relationship actually holds in the retained strata; logical implication is never "
            "inferred from a variant name."
        ),
    )


def render_report(summary: Mapping[str, Any]) -> str:
    """Render the complete offline analysis report from a sealed summary."""
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    members = list(summary["members"])
    primary = [item for item in members if item["primary"]]
    source = summary["source"]
    calendar = summary["calendar"]
    method = summary["method"]
    diagnostics = summary["diagnostics"]
    disclosures = "".join(f"<li>{escape(item)}</li>" for item in summary["disclosures"])

    resolution_note = (
        "The minimum two-sided resolution 2/(B+1) exceeds alpha/m, so no first Holm rejection is "
        "possible at this family size; B is not increased silently."
        if summary["p_resolution_blocks_first_rejection"]
        else "The minimum two-sided resolution 2/(B+1) does not by itself prevent a first Holm "
        "rejection at this family size."
    )

    body = f"""<h1>Pattern Lab matched comparisons: {escape(str(summary['analysis_name']))}</h1>
<p class="small">Generated {escape(generated)} · analysis of study
<code>{escape(str(source['run_root']))}</code></p>
<div class="banner">{escape(BANNER)}</div>
<div class="card">
<h2>Source study, family and frozen method</h2>
{_definition([
    ("Source study name", source["study_name"]),
    ("Source study interval (half-open, UTC)", diagnostics["source_study_interval"]),
    ("Source protocol", source["protocol"]["protocol_id"]),
    ("Source specification identity", source["semantic"]["specification_sha256"]),
    ("Source data-input identity", source["semantic"]["data_input_sha256"]),
    ("Source implementation identity", source["semantic"]["implementation_sha256"]),
    ("Source physical evidence set", source["physical"]["evidence_set_sha256"]),
    ("Source evidence view version", source["semantic"]["evidence_view_version"]),
    ("Instruments", ", ".join(summary["instruments"])),
    ("Original source family counts", ", ".join(
        f"{key}={value}" for key, value in sorted(diagnostics["source_family_counts"].items()))),
    ("Eligible anchors by timeframe", ", ".join(
        f"{key}m={value}" for key, value in sorted(diagnostics["eligible_anchors_by_timeframe"].items()))),
    ("Inference method", method["method"]),
    ("Matching", method["matching"]),
    ("Block length (days)", method["block_length_days"]),
    ("Two-sided alpha", method["alpha"]),
    ("Pointwise confidence level", method["confidence_level"]),
    ("Inference scope", summary["inference_scope"]),
    ("Bootstrap resamples B", summary["resamples"]),
    ("Bootstrap seed", summary["seed"]),
    ("Family size m", summary["family_size"]),
    ("Minimum two-sided p resolution", summary["p_resolution"]),
    ("Calendar day grid", f"{calendar['day_grid_days']} days, "
        f"{calendar['first_day_utc']} to {calendar['last_day_utc']}"),
    ("Calendar rule", calendar["rule"]),
    ("Completion authority", COMPLETION_AUTHORITY),
])}
{_comparison_rows(summary)}
</div>
<div class="card">
<h2>Supported estimates</h2>
<p class="small">Signal, control, net and gross are distinct fields; overlapping observation
returns are never summed into profit and no equity or profitability claim follows.</p>
{_estimate_rows(members)}
{_raw_rows(members)}
</div>
<div class="card">
<h2>Inference</h2>
<div class="qualify">{escape(APPROXIMATION)}</div>
<div class="qualify">{escape(LONG_DEPENDENCE)}</div>
<p class="small">{escape(resolution_note)}</p>
{_inference_rows(members)}
{_bootstrap_rows(members)}
<p class="small">A negative difference is not a positive edge, and a non-rejection is insufficient
evidence rather than proof of absence. Net profitability is not inferred from the lift p-value, and
no gross-return, excursion or subgroup p-value is reported.</p>
</div>
<div class="card">
<h2>Support, exclusions and overlap</h2>
{_support_rows(members)}
{_geometry_rows(members)}
{_ticker_details(members)}
<p class="small">The full stratum table, including zero-event and excluded strata with their
reason codes, is saved in <code>strata.parquet</code>; the daily counts and sums that reproduce the
estimator are in <code>daily.parquet</code>.</p>
</div>
<div class="card">
<h2>Declared primary cases</h2>
<p class="small">Order follows the canonical family, not best return or smallest p. The declared
primary horizon is emphasized without hiding the other horizons or directions.</p>
{_inference_rows(primary) if primary else '<p class="small">No case is declared primary.</p>'}
</div>
<div class="card">
<h2>Disclosures and limitations</h2>
<ul class="notes">{disclosures}</ul>
</div>
"""
    return (
        '<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>Pattern Lab analysis: {escape(str(summary['analysis_name']))}</title>"
        f"<style>{STYLE}</style></head><body>{body}</body></html>\n"
    )
