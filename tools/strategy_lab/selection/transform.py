"""Offline period substitution; the existing Fixed runner owns live admission."""
from copy import deepcopy
from pathlib import Path
import pandas as pd

from .ranking import open_ranking, Ranking, require, integer, stamp
from ..analysis.research_inputs import _read, digest
from ..analysis.json_utils import canonical_json_bytes
from ..fixed.config import validate_structure
from ..filters import recorded_roots, effective
from ..report_support import new_output, write_bytes, finish_report


def substitute_periods(raw, periods, roster, tf, name, warmup_bars, prefix):
    integer(warmup_bars, 100, 5000, "warmup_bars")
    g = validate_structure(raw)
    require(sorted(i["instrument_id"] for i in g["market_data"]["instruments"]) == roster and g["timeframes_minutes"] == [tf],
        "template must declare the full ranking roster and its single trading TF.")
    transformed = deepcopy(raw)
    transformed["run_name"] = name+"--"+raw["run_name"]
    transformed["generation"]["periods"] = [dict(name=f"{prefix}-period-{p['id']}", start=p["start"],
        end=(stamp(p["end"])-pd.Timedelta(minutes=tf)).isoformat(), warmup_bars=warmup_bars) for p in periods]
    transformed["generation"]["expected_run_count"] = len(roster)*len(periods)*len(g["parameter_sets"])
    validate_structure(transformed)
    return transformed, g


def write_fixed_request(ranking, template, output_dir, *, warmup_bars):
    ranking = ranking if isinstance(ranking, Ranking) else open_ranking(ranking)
    ranking.verify()
    integer(warmup_bars, 100, 5000, "warmup_bars")
    m = ranking.metadata
    require(m["request"]["effective"]["calendar"]["mode"] == "fixed", "Fixed transform requires a fixed ranking calendar.")
    path, template_hash, raw = _read(template)
    original_bytes = Path(path).read_bytes()
    # Freeze the same byte snapshot that was parsed, without mutable live resolution.
    import hashlib
    require(hashlib.sha256(original_bytes).hexdigest() == template_hash, "template changed during read.")
    g = validate_structure(raw)
    roster = sorted(m["universe"]["document"]["tickers"])
    tf = m["request"]["effective"]["calendar"]["tradingTimeframeMinutes"]
    require(sorted(i["instrument_id"] for i in g["market_data"]["instruments"]) == roster and g["timeframes_minutes"] == [tf],
        "template must declare the full ranking roster and its single trading TF.")
    step = pd.Timedelta(minutes=tf)
    impossible = []
    listings = {r["instrument"]: r["listing"] for r in ranking.records[0]["rows"]}
    periods = m["calendar"]["periods"]
    for p in periods:
        required = stamp(p["start"])-warmup_bars*step
        for ticker, listing in listings.items():
            if listing is not None and stamp(listing) > required:
                impossible.append(dict(ticker=ticker, period_id=p["id"], listing=listing, required_start=required.isoformat()))
    require(not impossible, f"listing proves impossible execution/warmup cells: {impossible}. Choose a later Start or different explicit universe in a new pre-outcome plan.")
    transformed, g = substitute_periods(raw, periods, roster, tf, m["request"]["effective"]["name"], warmup_bars, "x")
    roots = [ranking.root, path, m["request"]["path"], m["universe"]["path"],
        *(s["pack_root"] for s in m["pool"]), *recorded_roots(effective(raw))]
    output = new_output(output_dir, roots)
    write_bytes(output, "template.json", original_bytes)
    write_bytes(output, "fixed-request.json", canonical_json_bytes(transformed))
    metadata = dict(schema_version="strategy_lab_x_fixed_transform_v1", template_path=path, template_sha256=template_hash,
        ranking_identity=m["ranking_identity"], controls_identity=m["controls_identity"], ranking_sha256=ranking.file_sha256,
        warmup_bars=warmup_bars, original_periods=g["periods"], period_map=[dict(source_period_id=p["id"],
            source_period_key=[p["start"], p["end"]], target=t) for p, t in zip(periods, transformed["generation"]["periods"])],
        changed_fields=["run_name", "generation.periods", "generation.expected_run_count"], request_sha256=digest(transformed))
    return finish_report(output, "transform.json", metadata, ["template.json", "fixed-request.json"],
        "Fixed request from frozen X calendar", dict(warmup_bars=warmup_bars, periods=len(periods),
            interpretation="Independent reference-capital accounts; existing Fixed runner still owns all live preflight."))
