"""Static, offline account views. No JavaScript, strategy imports or market reads."""
from html import escape
import os
from pathlib import Path
from urllib.parse import quote

import numpy as np

from ..dataset import atomic_write, atomic_write_json, load_json
from .accounting import MONTH_COLUMNS, csv_bytes
from .config import ECONOMICS
from .storage import METRICS, RESULT_SCHEMA, RESULT_PAIRS, open_dataset, read_account, safe_path
from .config import DATASET_V2

INDEX_COLUMNS = ("account_id", "parameter_set", "period", "instrument_id", "cell", "timeframe_minutes",
    "requested_start", "requested_end", "resolved_start", "resolved_end", "warmup_bars", "status", "reason",
    *ECONOMICS, *METRICS, "observation_count", "result", "curve", "monthly", "html")
FILTER_COLUMNS = ("entry_filter_state", "entry_filter_key", "entry_filter_description")


def index_columns(manifest):
    return INDEX_COLUMNS + (FILTER_COLUMNS if manifest["schema_version"] == DATASET_V2 else ())


def text_file(path, content):
    atomic_write(path, lambda p: p.write_bytes(content.encode("utf-8")))


def table(rows, columns):
    def display(v):
        return "unavailable" if v is None else str(v)
    return "<table><thead><tr>" + "".join(f"<th>{escape(k)}</th>" for k in columns) + "</tr></thead><tbody>" + "".join(
        "<tr>"+"".join(f"<td>{escape(display(row.get(k, '')))}</td>" for k in columns)+"</tr>" for row in rows)+"</tbody></table>"


def page(title, content):
    return '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">' + (
        f"<title>{escape(title)}</title><style>body{{font:15px system-ui;margin:2rem;color:#17212b;background:white}}"
        "table{border-collapse:collapse;display:block;overflow:auto;margin:1rem 0}td,th{border:1px solid #ccd2d8;padding:.4rem;text-align:left}"
        "svg{width:100%;max-width:1000px;border:1px solid #ccd2d8}a{color:#07549b}dt{font-weight:bold}dd{overflow-wrap:anywhere}</style>"
        f"<h1>{escape(title)}</h1>{content}</html>")


def account_html(root, account, claim, destination, result_schema=RESULT_SCHEMA, filter_editor=None):
    result, (ts, eq, bal), rows = read_account(root, account, claim, result_schema)
    sample = np.unique(np.linspace(0, len(ts)-1, min(1000, len(ts)), dtype=int))
    low, high = min(eq.min(), bal.min()), max(eq.max(), bal.max())
    span = max(high-low, 1.0)
    def poly(values, color):
        points = " ".join(f"{25+950*(int(ts[i])-int(ts[0]))/max(int(ts[-1])-int(ts[0]),1):.2f},{275-250*(values[i]-low)/span:.2f}" for i in sample)
        return f'<polyline fill="none" stroke="{color}" stroke-width="1.5" points="{points}"/>'
    bounds = account["input_pin"]["bounds"]
    content = ("<p>Independent continuous account; calendar slices do not restart capital or close positions. "
        "A separately rerun subperiod is a different experiment. Funding and slippage are not modelled.</p>"
        + table([{"account_id": account["account_id"], **{k:account[k] for k in ("parameter_set", "period", "instrument_id", "cell", "timeframe_minutes")}}],
                ("account_id", "parameter_set", "period", "instrument_id", "cell", "timeframe_minutes"))
        + table([bounds["requested"]], ("start", "end", "warmupBars"))
        + table([bounds], ("evaluation_start", "evaluation_end", "warmup_start", "start", "end", "trade_start_idx"))
        + "<h2>Effective economics and scalars</h2>" + table([account["params"]], tuple(account["params"]))
        + f"<p>Initial capital / pre-evaluation anchor: {result['metric_anchor']}. "
          f"Evaluation observations: {len(ts)}; no warmup or synthetic anchor rows. Backend: {escape(result['backend'])}.</p>"
        + "<h2>Equity and realized balance</h2><p>Blue: bar-close mark-to-market equity. Orange: realized balance. "
          "End-of-bar values use UTC bar-open timestamps. Plot-only sampling retains at most 1,000 points; saved data and calculations retain every observation.</p>"
        + f'<svg viewBox="0 0 1000 300" role="img" aria-label="Account equity and balance">{poly(eq, "#1765a8")}{poly(bal, "#b9570b")}</svg>'
        + f"<p>Currency range: {low:.8g} to {high:.8g}; UTC interval: {escape(bounds['start'])} through last bar before {escape(bounds['end'])}.</p>"
        + "<h2>Core whole-run metrics</h2><p>Net Profit and Max DD are realized balance metrics. "
          "max_drawdown_mtm_pct is separate bar-close MTM drawdown. Trade statistics are saved engine results, "
          "not reconstructed from equity. No raw trade export is included. Monthly and daily Sharpe retain core conventions.</p>"
        + table([{"metric": k, "value":result["metrics"][k], "unit":result["metric_units"][k],
            "unavailable_reason":result["metric_unavailable_reasons"].get(k, "")} for k in METRICS],
            ("metric", "value", "unit", "unavailable_reason"))
        + "<h2>Calendar accounting</h2><p>Returns require positive opening equity. Realized P&amp;L and MTM change are distinct; "
          "partial months are retained without annualizing their returns. Precise midnight End can add a one-bar month.</p>"
        + table(rows, MONTH_COLUMNS))
    if filter_editor is not None:
        from ..filters import summary
        from core.entry_filters import effective_config
        content += "<h2>Entry filters</h2>" + table([summary(filter_editor, effective_config(filter_editor),
            account["entry_filter_inputs"], account["input_pin"])], FILTER_COLUMNS)
    links = []
    for name in ("result.json", "curve.parquet", "monthly.csv"):
        target = safe_path(root, claim[name]["path"])
        href = quote(Path(os.path.relpath(target, destination.parent)).as_posix(), safe="/.")
        links.append(f'<a href="{escape(href, quote=True)}">{name}</a>')
    text_file(destination, page("Fixed account " + account["instrument_id"], content + "<p>" + " · ".join(links) + "</p>"))


def index_rows(root, manifest):
    rows = []
    for a in manifest["contract"]["accounts"]:
        ident = a["account_id"]
        b = a["input_pin"]["bounds"]
        row = {k:a[k] for k in INDEX_COLUMNS[:6]} | {"requested_start":b["requested"]["start"], "requested_end":b["requested"]["end"],
            "resolved_start":b["start"], "resolved_end":b["end"], "warmup_bars":b["trade_start_idx"],
            "status":"pending", "reason":"", **{k:a["params"][k] for k in ECONOMICS}}
        if ident in manifest["completed"]:
            claim = manifest["completed"][ident]
            result = load_json(safe_path(root, claim["result.json"]["path"]), "result")
            row.update(status="complete", **result["metrics"], observation_count=result["observation_count"],
                result=claim["result.json"]["path"], curve=claim["curve.parquet"]["path"], monthly=claim["monthly.csv"]["path"],
                html=f"accounts/{ident}/report.html")
        elif manifest["failure"] and manifest["failure"]["account_id"] == ident:
            row.update(status="failed", reason=manifest["failure"]["reason"])
        if manifest["schema_version"] == DATASET_V2:
            from ..filters import summary
            contract = manifest["contract"]
            row.update(summary(contract["request"]["generation"]["execution"]["entryFilters"],
                contract["resolved"]["entry_filters"], a["entry_filter_inputs"], a["input_pin"]))
        rows.append({k:row.get(k, "") for k in index_columns(manifest)})
    return rows


def write_index(root, manifest):
    data = csv_bytes(index_rows(root, manifest), index_columns(manifest))
    atomic_write(safe_path(root, "series-index.csv"), lambda p:p.write_bytes(data))


def report(dataset, output, *, allow_partial=False):
    root, output = Path(dataset).resolve(), Path(output).absolute()
    m = open_dataset(root, allow_partial=allow_partial)
    # A report must never replace its source, an ancestor, or a recorded market root.
    from ..filters import protect_output, recorded_roots
    contract = m["contract"]
    protect_output(output, [root, *(a["provenance"]["pack_root"] for a in contract["accounts"]),
        *recorded_roots(contract["resolved"].get("entry_filters", []))])
    editor = contract["request"]["generation"]["execution"].get("entryFilters")
    marker = safe_path(output, "report.json")
    identity = {"schema_version":"strategy_lab_fixed_report_v1", "contract_sha256":m["contract_sha256"]}
    if output.exists() and any(output.iterdir()) and (not marker.exists() or load_json(marker, "report") != identity):
        raise ValueError("refusing unrelated nonempty report output.")
    output.mkdir(parents=True, exist_ok=True)
    rows = index_rows(root, m)
    links = []
    for a, row in zip(m["contract"]["accounts"], rows):
        if a["account_id"] in m["completed"]:
            name = f"accounts/{a['account_id']}/report.html"
            account_html(root, a, m["completed"][a["account_id"]], safe_path(output, name), RESULT_PAIRS[m["schema_version"]], editor)
            links.append(f'<li><a href="{name}">{escape(a["parameter_set"])} / {escape(a["period"])} / {escape(a["instrument_id"])} / {a["timeframe_minutes"]}m</a></li>')
            for field in ("result", "curve", "monthly"):
                row[field] = Path(os.path.relpath(safe_path(root, row[field]), output)).as_posix()
    atomic_write(safe_path(output, "series-index.csv"), lambda p:p.write_bytes(csv_bytes(rows, index_columns(m))))
    title = "Fixed series — " + ("complete" if m["complete"] else "PARTIAL")
    text_file(safe_path(output, "index.html"), page(title,
        f"<p>{len(m['completed'])} / {len(rows)} completed independent accounts. No portfolio aggregation or ranking.</p>"
        + '<p><a href="series-index.csv">Machine-readable series index</a></p><ul>' + "".join(links) + "</ul>"
        + table(rows, ("parameter_set", "period", "instrument_id", "cell", "timeframe_minutes", "status", "reason", *ECONOMICS, "net_profit_pct")
            + (FILTER_COLUMNS if editor is not None else ()))))
    atomic_write_json(marker, identity)
    return {"complete":m["complete"], "accounts":len(rows), "completed":len(m["completed"]), "report":str(output/"index.html")}
