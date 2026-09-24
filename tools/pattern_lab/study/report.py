"""The standalone descriptive HTML report.

The page is offline: no CDN, no network request and no templating dependency.
It shows the frozen study question, conventions and costs, then one section per
resolved model case.  Millions of observation rows stay in the linked
machine-readable evidence; the report links to them instead of embedding them.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any, Mapping, Sequence
from urllib.parse import quote

BANNER = "Descriptive event study — statistical validation is not implemented in M2."

# This page is regenerable and is never part of the completion record's hash
# set, so it can never certify a run by itself.
COMPLETION_AUTHORITY = (
    "Authoritative completion is a matching verified completion.json: the run's immutable evidence "
    "must still hash to the values that record names. Opening this regenerable page, or reading "
    "'Run complete' below, is not a completion check."
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
.controls { margin: 10px 0 4px; }
.primary { background: #eaf3ff; }
"""

SCRIPT = """
(function () {
  var box = document.getElementById('hide-empty');
  if (!box) { return; }
  box.addEventListener('change', function () {
    var sections = document.querySelectorAll('section.group');
    for (var index = 0; index < sections.length; index += 1) {
      var empty = sections[index].getAttribute('data-events') === '0';
      sections[index].style.display = (box.checked && empty) ? 'none' : '';
    }
  });
})();
"""


def _number(value: Any, digits: int = 6) -> str:
    if value is None:
        return "—"
    return f"{float(value):.{digits}f}"


def _rows(rows: Sequence[Sequence[str]], *, header: Sequence[str], caption: str = "") -> str:
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


def _evidence_link(relative: str, label: str) -> str:
    """An offline link from derived/report.html to a sibling evidence path."""
    # The page lives in derived/, so every evidence path is one level up.
    return f'<a href="../{quote(relative)}">{escape(label)}</a>'


def _evidence_links(summary: Mapping[str, Any]) -> str:
    """Working relative links to the machine-readable evidence of this run."""
    items = [
        _evidence_link("spec/request.json", "spec/request.json"),
        _evidence_link("spec/family.json", "spec/family.json"),
        _evidence_link("provenance.json", "provenance.json"),
        _evidence_link("status.json", "status.json"),
        _evidence_link("completion.json", "completion.json"),
        f'<a href="{quote("summary.json")}">derived/summary.json</a>',
    ]
    for instrument_id in summary["completed_instruments"]:
        items.append(
            _evidence_link(f"jobs/{instrument_id}/bundle.json", f"jobs/{instrument_id}/bundle.json")
        )
    return "".join(f"<li>{item}</li>" for item in items)


def _definition(pairs: Sequence[tuple[str, Any]]) -> str:
    return _rows([[key, "" if value is None else str(value)] for key, value in pairs],
                 header=["Field", "Value"])


def _group_title(group: Mapping[str, Any]) -> str:
    parameters = ", ".join(f"{key}={value}" for key, value in sorted(group["case_parameters"].items()))
    return (
        f"{group['variant_id']} / {group['model_instance_id']} / {group['case_id']}"
        + (f" ({parameters})" if parameters else "")
    )


def _outcome_table(group: Mapping[str, Any]) -> str:
    header = [
        "Outcome", "Unit", "Valid", "Invalid", "Mean", "Median", "p10", "p25", "p75", "p90",
        "Fraction > 0", "Equal-ticker mean", "Tickers",
    ]
    rows = []
    for outcome in group["outcomes"]:
        pooled = outcome["event_weighted"]
        rows.append(
            [
                outcome["name"],
                outcome["unit"],
                pooled["n"],
                outcome["invalid_count"],
                _number(pooled["mean"]),
                _number(pooled["median"]),
                _number(pooled["p10"]),
                _number(pooled["p25"]),
                _number(pooled["p75"]),
                _number(pooled["p90"]),
                _number(pooled["fraction_positive"], 4),
                _number(outcome["equal_ticker"]["mean_of_ticker_means"]),
                outcome["equal_ticker"]["tickers"],
            ]
        )
    return _rows(rows, header=header, caption="Event-weighted pooling, with the equal-ticker companion")


def _reason_table(group: Mapping[str, Any]) -> str:
    rows = []
    for outcome in group["outcomes"]:
        for reason, count in outcome["invalid_by_reason"].items():
            rows.append([outcome["name"], reason, count])
    if not rows:
        return '<p class="small">No invalid outcome was recorded for this group.</p>'
    return _rows(rows, header=["Outcome", "Reason", "Count"], caption="Invalid outcomes by reason")


def _metric_table(group: Mapping[str, Any]) -> str:
    records = group.get("metrics") or []
    if not records:
        return ""
    rows = [
        [
            record["declaration_id"],
            record["metric_id"],
            record["unit"],
            record["availability"],
            _number(record["value"]) if record["value"] is not None else "—",
            ", ".join(record.get("missing_columns", [])) or "—",
        ]
        for record in records
    ]
    return _rows(
        rows,
        header=["Declaration", "Metric", "Unit", "Availability", "Value", "Missing inputs"],
        caption="Declared descriptive metrics (not selection objectives)",
    )


def _per_ticker(group: Mapping[str, Any]) -> str:
    parts = ["<details><summary>Per-ticker means and coverage</summary>"]
    for outcome in group["outcomes"]:
        rows = [[item["instrument_id"], _number(item["mean"])] for item in outcome["per_ticker"]]
        if rows:
            parts.append(_rows(rows, header=["Instrument", f"Mean {outcome['name']}"],
                               caption=f"{outcome['name']} ({outcome['unit']})"))
    zero = group["zero_support_tickers"]
    parts.append(
        '<p class="small">Tickers with at least one valid event: '
        f"{group['tickers_with_events']} of {group['tickers_planned']} planned. "
        + (
            "Zero-support tickers: " + escape(", ".join(zero)) + "."
            if zero
            else "No zero-support ticker in this group."
        )
        + "</p>"
    )
    parts.append("</details>")
    return "".join(parts)


def render_report(summary: Mapping[str, Any]) -> str:
    """Render the complete offline report from a saved summary document."""
    study = summary["study"]
    protocol = summary["protocol"]
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    variant_rows = [
        [
            item["variant_id"],
            item["hypothesis_id"],
            item["occurrence"],
            item["condition_id"],
            item["required_prior_bars"],
        ]
        for item in summary["variants"]
    ]
    model_rows = [
        [item["model_instance_id"], item["model_id"], item["model_version"], item["evidence_kind"],
         ", ".join(f"{key}={value}" for key, value in sorted(item["settings"].items()))]
        for item in summary["models"]
    ]
    instrument_rows = [
        [item["instrument_id"], item["symbol"], item["venue"], ", ".join(item["roles"])]
        for item in summary["instruments"]
    ]

    sections = []
    for group in summary["groups"]:
        css = "group primary" if group["primary"] else "group"
        sections.append(
            f'<section class="{css}" data-events="{int(group["events"])}">'
            f"<h3>{escape(_group_title(group))}"
            + (" — declared primary case" if group["primary"] else "")
            + "</h3>"
            + f'<p class="small">Timeframe {escape(str(group["timeframe_minutes"]))}m · occurrence '
            f'{escape(group["occurrence"])} · events {group["events"]} · episodes {group["episodes"]} '
            f'· model {escape(group["model_id"])}</p>'
            + _outcome_table(group)
            + _reason_table(group)
            + _metric_table(group)
            + _per_ticker(group)
            + "</section>"
        )

    counts = summary["counts"]
    disclosures = "".join(f"<li>{escape(item)}</li>" for item in summary["disclosures"])
    context_html = ""
    if summary.get("context"):
        context_html = '<div class="card"><h2>Explicit market context</h2>' + _definition([
            ("Aliases and canonical members", str(summary["context"])),
            ("Roles and self-inclusion", str(summary["context_admission"])),
            ("Coverage and memory", str(summary["context_diagnostics"])),
            ("Thresholds (fractions)", str([(v["hypothesis_id"], v["parameters"]) for v in summary["variants"]])),
            ("Availability", "Same-timeframe bar-close inputs; every declared panel member must be valid. Unknown context is excluded from controls. Strict thresholds: equality is false. Doji is not green."),
            ("Interpretation", "Filtered versus inclusive parent is incremental association, not a causal contribution or buy-and-hold outperformance."),
        ]) + '</div>'
    body = f"""<h1>Pattern Lab event study: {escape(str(summary['study_name']))}</h1>
<p class="small">Generated {escape(generated)} · run <code>{escape(str(summary['run_root']))}</code></p>
<div class="banner">{escape(BANNER)}</div>
{context_html}
<div class="card">
<h2>Study question and frozen settings</h2>
{_definition([
    ("Study interval (half-open, UTC)", f"[{study['start_utc']}, {study['end_utc']})"),
    ("Declared warmup start", study["warmup_start_utc"]),
    ("Protocol", protocol["protocol_id"]),
    ("Protocol development interval", f"[{protocol['development']['start_utc']}, {protocol['development']['end_utc']})"),
    ("Protocol reserved interval", f"[{protocol['reserved']['start_utc']}, {protocol['reserved']['end_utc']})"),
    ("Observation timeframes (minutes)", ", ".join(str(item) for item in summary["timeframes_minutes"])),
    ("Anchor convention", summary["anchor_convention"]),
    ("Signal, entry and exit", "The condition is known at the anchor's close; notional entry is the next bar's open at that close; the exit is the close of the horizon's last bar; the path covers the bars from entry to exit and excludes the signal bar."),
    ("Costs", "Commission is charged on the entry notional and separately on the exit notional, from each model's own declared rate. Slippage and funding are excluded."),
    ("Pooling", summary["pooling"]),
    ("Quantiles", summary["quantile_convention"]),
    ("Minimum support", summary["minimum_support"]),
    ("Jobs", f"planned {counts.get('planned', 0)}, completed {counts.get('completed', 0)}, failed {counts.get('failed', 0)}, not started {counts.get('not_started', 0)}"),
    ("Run complete", "yes" if summary["complete"] else "no"),
    ("Completion authority", COMPLETION_AUTHORITY),
    ("Specification identity", summary["identities"]["specification_sha256"]),
    ("Data input identity", summary["identities"]["data_input_sha256"]),
    ("Implementation identity", summary["identities"]["implementation_sha256"]),
    ("Pack manifest revision", summary["manifest"]["revision"]),
    ("Historical universe membership", summary["manifest"]["universe"].get("historical_membership")),
])}
</div>
<div class="card">
<h2>Disclosures and limitations</h2>
<ul class="notes">{disclosures}</ul>
</div>
<div class="card">
<h2>Hypothesis variants</h2>
{_rows(variant_rows, header=["Variant", "Hypothesis", "Occurrence", "Condition identity", "Required prior bars"])}
<h2>Model instances and their own case axes</h2>
{_rows(model_rows, header=["Instance", "Model", "Version", "Evidence kind", "Settings"])}
<h2>Universe</h2>
{_rows(instrument_rows, header=["Instrument", "Symbol", "Venue", "Roles"])}
<p class="small">Completed jobs: {escape(", ".join(summary["completed_instruments"])) or "none"}.</p>
</div>
<div class="card">
<h2>Saved machine-readable evidence</h2>
<p class="small">These offline relative links resolve from this page inside <code>derived/</code>.
Raw evidence is immutable; only this page and <code>derived/summary.json</code> are regenerated.</p>
<ul class="notes">{_evidence_links(summary)}</ul>
</div>
<div class="card">
<h2>Declared outcome groups</h2>
<p class="small">Order follows the declared family, not performance. Values are fractional returns
and fractional excursions; multiply by 100 for percent. Saved machine-readable evidence lives in
<code>jobs/&lt;instrument&gt;/</code> and this page's own inputs in <code>derived/summary.json</code>.</p>
<div class="controls"><label><input type="checkbox" id="hide-empty"> Hide zero-event groups</label></div>
{''.join(sections)}
</div>
"""
    if "sequential_accounts" in summary:
        body += '<div class="card"><h2>Sequential ATR bracket accounts</h2><p>Descriptive independent accounts, not a portfolio or validated edge. All declared cases are shown; no RR is selected.</p>'
        body += '<p>Signal-close levels and legacy float risk sizing; next-contiguous-open fills. Entry-only leverage cap rejects without resizing. Commission applies on both executed notionals; slippage, funding, liquidation and maintenance margin are not modeled. Execution uses observation OHLC: O-H-L-C when open is nearer high, otherwise O-L-H-C (including ties).</p>'
        body += '<p>ATR uses an arithmetic TR seed and recursive Pine update, resetting after gaps. Segment survivors close at the last observed close and capital carries forward. Four days is an elapsed-time expiry trigger followed by next-open closure; terminal closure takes precedence. Intrabar fills have an unknown instant: duration uses the exit bar open, open exits use that open, and terminal exits use the bar close.</p>'
        body += '<p>Drawdowns include the initial-capital anchor, distinguish realized balance from bar-close MTM, and may exceed 100%. Wins/losses use net PnL after both fees. Planned R uses rounded quantity times signal-close distance. Profit factor is net winning PnL / absolute net losing PnL; no-loss cases remain null with an explicit status.</p>'
        body += _rows([[a["instrument_id"],a["variant_id"],a["model_instance_id"],str(a["timeframe_minutes"]),a["case_id"],str(a["signals"]),str(a["completed_trades"]),
            _number(a["net_pnl"]),_number(a["total_fees"]),_number(a["return_pct"]),_number(a["realized_balance_drawdown_pct"]),_number(a["bar_close_mtm_drawdown_pct"])] for a in summary["sequential_accounts"]],
            header=["Instrument","Variant","Model","Minutes","Case","Signals","Trades","Net PnL USDT","Fees USDT","Return %","Realized DD %","MTM DD %"])
        for account in summary["sequential_accounts"]:
            body += '<details><summary>'+escape(' / '.join(str(account[k]) for k in ("instrument_id","variant_id","model_instance_id","case_id")))+'</summary>'
            body += _definition([(k,str(v)) for k,v in account.items()])+ '</details>'
        body += '<h3>Frozen quantity rules</h3><p>Current exchange snapshots are not historical rule history. OKX contracts convert through base-denominated ctVal with ctMult=1; Bybit uses base quantity. Integer lots preserve original-unit quantities. Optional minimum notional is enforced only when published. Prices are not tick-rounded; price tick is provenance, not full exchange-order validation.</p>'
        body += _definition([(key,str(value)) for key,value in summary["sequential_rules"].items()])+'</div>'
    return (
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"<title>Pattern Lab event study: {escape(str(summary['study_name']))}</title>"
        f"<style>{STYLE}</style></head><body>{body}"
        f"<script>{SCRIPT}</script></body></html>\n"
    )
