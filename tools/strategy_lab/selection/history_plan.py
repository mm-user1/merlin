"""Explicit offline calendar plans; live creation delegates to the core calendar."""
from copy import deepcopy
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re

from .ranking import require, integer, stamp, calendar_facts, validate_calendar_parts
from .transform import substitute_periods
from ..analysis.research_inputs import _read, _keys, _text, digest, load_universe, Universe
from ..analysis.json_utils import canonical_json_bytes
from ..analysis.dataset import git_facts
from ..dataset import validate_artifact_record
from ..fixed.storage import safe_path
from ..filters import effective, recorded_roots
from ..report_support import new_output, write_bytes, finish_report

METHODS = ("prior_oos_positive_count", "prior_oos_mean", "selected_is_net_profit", "transfer_consistency_4w_v1")


def checked_input(path):
    path = Path(path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), "history input symlinks are forbidden.")
    return path


def history_request(raw):
    _keys(raw, "schema_version name calendar evaluationStart historyWindows topN methods referenceMethod controls".split(), "history request")
    require(raw["schema_version"] == "strategy_lab_history_request_v1", "unsupported history request version.")
    _text(raw["name"], "name")
    c = raw["calendar"]
    require(isinstance(c, dict) and c.get("mode") in ("fixed", "is_oos"), "invalid calendar mode.")
    _keys(c, ["mode", "start", "end", "tradingTimeframeMinutes"]+(["periodMonths"] if c["mode"] == "fixed" else ["isMonths", "oosMonths"]), "calendar")
    tf = integer(c["tradingTimeframeMinutes"], 5, 1440, "trading TF")
    require(tf % 5 == 0 and 1440 % tf == 0, "trading TF must divide a day.")
    for key in (["periodMonths"] if c["mode"] == "fixed" else ["isMonths", "oosMonths"]):
        integer(c[key], 1, 120000, key)
    stamp(c["start"]); stamp(c["end"], end=True); stamp(raw["evaluationStart"])
    integer(raw["historyWindows"], 4, 4, "historyWindows")
    integer(raw["topN"], 1, 2**32-1, "topN")
    methods = raw["methods"]
    require(isinstance(methods, list) and methods and all(isinstance(m, str) and m in METHODS for m in methods)
        and len(set(methods)) == len(methods), "methods must be unique supported IDs.")
    require(raw["referenceMethod"] in methods, "referenceMethod must be declared.")
    require(c["mode"] != "fixed" or set(methods) <= set(METHODS[:2]), "Fixed supports only prior-return methods.")
    require(METHODS[3] not in methods or (c.get("isMonths"), c.get("oosMonths")) == (4, 1), "tc4w requires IS4/OOS1.")
    _keys(raw["controls"], ["seed", "draws"], "controls")
    integer(raw["controls"]["seed"], 0, 2**64-1, "seed")
    integer(raw["controls"]["draws"], 1, 100000, "draws")
    return deepcopy(raw)


def block_map(request, cal):
    fixed = request["calendar"]["mode"] == "fixed"
    blocks = [dict(id=p["id"], start=p["start"], end=p["end"], isStart=None) for p in cal["periods"]] if fixed else [w for p in cal["periods"] for w in p["windows"]]
    matches = [i for i, b in enumerate(blocks) if stamp(b["start"]) == stamp(request["evaluationStart"])]
    require(len(matches) == 1 and matches[0] >= 4, "evaluationStart needs four preceding complete adjacent blocks and a target.")
    first = matches[0]
    return dict(blocks=blocks, history_ids=[b["id"] for b in blocks[:first]], target_ids=[b["id"] for b in blocks[first:]])


def plan_identity(m):
    return digest(dict(schema_version="strategy_lab_history_plan_identity_v1", request=m["request"]["effective"],
        roster=sorted(m["universe"]["document"]["tickers"]), calendar=m["calendar"], mapping=m["mapping"]))


def prepare_history_plan(request_path, output_dir, *, ticker_list):
    from core.ticker_selector import build_calendar
    from data_provider.manifest import normalize_instrument_id
    path, raw_hash, raw = _read(checked_input(request_path))
    request = history_request(raw)
    universe = ticker_list if isinstance(ticker_list, Universe) else load_universe(checked_input(ticker_list))
    universe.verify()
    roster = sorted(universe.document["tickers"])
    require(all(normalize_instrument_id(t) == t for t in roster), "canonical full instrument IDs required.")
    require(request["topN"] <= len(roster), "topN exceeds declared roster.")
    c = request["calendar"]
    periods = build_calendar(c["start"], c["end"], c["tradingTimeframeMinutes"], c.get("periodMonths", c.get("oosMonths")),
        **(dict(is_months=c["isMonths"], oos_months=c["oosMonths"]) if c["mode"] == "is_oos" else {}))
    cal = calendar_facts(request, periods)
    m = dict(schema_version="strategy_lab_history_plan_v1", request=dict(path=path, file_sha256=raw_hash, raw=raw,
        effective=request, effective_sha256=digest(request)), universe=universe.provenance(), calendar=cal,
        mapping=block_map(request, cal), git=git_facts())
    m["plan_identity"] = plan_identity(m)
    output = new_output(output_dir, [path, universe.path])
    return finish_report(output, "plan.json", m, [], "Historical ticker-selection plan", dict(
        history_blocks=len(m["mapping"]["history_ids"]), target_blocks=len(m["mapping"]["target_ids"]),
        interpretation="Offline calendar only; full roster execution coverage including warmup is required."))


@dataclass(frozen=True)
class HistoryPlan:
    root: Path
    metadata: dict
    file_sha256: str
    snapshot_sha256: str

    def verify(self):
        require(digest(self.metadata) == self.snapshot_sha256, "loaded history plan was mutated; reload.")


def open_history_plan(root):
    root = checked_input(root)
    _, file_hash, m = _read(safe_path(root, "plan.json"))
    _keys(m, "schema_version request universe calendar mapping git plan_identity artifacts".split(), "history plan")
    require(m["schema_version"] == "strategy_lab_history_plan_v1", "unsupported history plan version.")
    r = m["request"]
    _keys(r, "path file_sha256 raw effective effective_sha256".split(), "request snapshot")
    request = history_request(r["raw"])
    require(request == r["effective"] and digest(request) == r["effective_sha256"], "history request snapshot mismatch.")
    u = m["universe"]
    _keys(u, "path file_sha256 document document_sha256".split(), "universe snapshot")
    d = u["document"]
    _keys(d, "schema_version name evidence_classification selection_note tickers".split(), "universe")
    require(d["schema_version"] == "strategy_lab_universe_v1" and d["evidence_classification"] == "exploratory", "invalid universe.")
    for key in ("name", "selection_note"):
        _text(d[key], key)
    roster = d["tickers"]
    require(isinstance(roster, list) and roster and all(isinstance(t, str) and re.fullmatch(r"[A-Z0-9][A-Z0-9.-]*_[A-Z0-9][A-Z0-9.-]*", t) for t in roster)
        and len(set(roster)) == len(roster) and request["topN"] <= len(roster), "invalid history roster/K.")
    require(digest(d) == u["document_sha256"], "universe snapshot mismatch.")
    for snapshot in (u, r):
        _text(snapshot["path"], "snapshot path")
        require(isinstance(snapshot["file_sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", snapshot["file_sha256"]), "invalid byte hash.")
    c = request["calendar"]
    validate_calendar_parts(c, c.get("periodMonths", c.get("oosMonths")), m["calendar"])
    require(m["mapping"] == block_map(request, m["calendar"]), "history mapping mismatch.")
    require(m["plan_identity"] == plan_identity(m), "history plan identity mismatch.")
    require(set(m["artifacts"]) == {"index.html"}, "history plan artifacts mismatch.")
    for name, claim in m["artifacts"].items():
        safe_path(root, name)
        require(claim["path"] == name and validate_artifact_record(root, claim), "history plan artifact mismatch.")
    return HistoryPlan(root, m, file_hash, digest(m))


def write_history_fixed_request(plan, template, output_dir, *, warmup_bars):
    plan = plan if isinstance(plan, HistoryPlan) else open_history_plan(plan)
    plan.verify()
    m = plan.metadata; r = m["request"]["effective"]
    require(r["calendar"]["mode"] == "fixed", "Fixed transform requires a fixed history plan.")
    path, template_hash, raw = _read(checked_input(template))
    original = Path(path).read_bytes()
    require(hashlib.sha256(original).hexdigest() == template_hash, "template changed during read.")
    periods = m["calendar"]["periods"]
    transformed, g = substitute_periods(raw, periods, sorted(m["universe"]["document"]["tickers"]),
        r["calendar"]["tradingTimeframeMinutes"], r["name"], warmup_bars, "history")
    roots = [plan.root, path, m["request"]["path"], m["universe"]["path"], *recorded_roots(effective(raw))]
    output = new_output(output_dir, roots)
    write_bytes(output, "template.json", original)
    write_bytes(output, "fixed-request.json", canonical_json_bytes(transformed))
    metadata = dict(schema_version="strategy_lab_history_fixed_transform_v1", template_path=path, template_sha256=template_hash,
        plan_identity=m["plan_identity"], plan_sha256=plan.file_sha256, warmup_bars=warmup_bars, original_periods=g["periods"],
        period_map=[dict(source_period_id=p["id"], source_period_key=[p["start"], p["end"]], target=t)
            for p, t in zip(periods, transformed["generation"]["periods"])],
        changed_fields=["run_name", "generation.periods", "generation.expected_run_count"], request_sha256=digest(transformed))
    return finish_report(output, "transform.json", metadata, ["template.json", "fixed-request.json"], "Fixed request from history plan",
        dict(warmup_bars=warmup_bars, periods=len(periods), interpretation="No listing evidence; run existing Fixed preflight before execution."))
