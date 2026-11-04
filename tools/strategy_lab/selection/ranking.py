"""Live core preparation and a separate, frozen-v1 offline evidence reader."""
from copy import deepcopy
from dataclasses import dataclass
import csv
import hashlib
import io
import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from ..analysis.dataset import AnalysisError, git_facts
from ..analysis.research_inputs import _read, _keys, _text, digest, load_universe, Universe
from ..analysis.json_utils import canonical_json_bytes
from ..dataset import validate_artifact_record
from ..fixed.storage import safe_path
from ..report_support import new_output, write_bytes, write_rows, finish_report

ALGORITHM = "numpy_generator_pcg64_choice_without_replacement_v1"
ROW_KEYS = "instrument source listing eligible reason score rank selected months reason_code".split()
PIN_KEYS = "month start end expected observed fingerprintVersion fingerprint".split()
CSV_COLUMNS = "period_id cutoff instrument eligible reason_code reason listing score rank selected months".split()


def require(condition, message):
    if not condition:
        raise AnalysisError(message)


def integer(value, low, high, name):
    require(type(value) is int and low <= value <= high, f"{name}: integer in {low}..{high} required.")
    return value


def stamp(value, *, end=False):
    require(isinstance(value, str), "timestamp must be a string.")
    t = pd.Timestamp(value)
    require(not pd.isna(t), "timestamp is unavailable.")
    t = t.tz_localize("UTC") if t.tz is None else t.tz_convert("UTC")
    if end and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value.strip()):
        t += pd.Timedelta(days=1)-pd.Timedelta(microseconds=1)
    return t


def request(raw, *, live=False):
    _keys(raw, "schema_version name calendar tickerSelector controls".split(), "X request")
    require(raw["schema_version"] == "strategy_lab_x_request_v1", "unsupported X request version.")
    _text(raw["name"], "name")
    c, s, controls = raw["calendar"], raw["tickerSelector"], raw["controls"]
    require(isinstance(c, dict) and c.get("mode") in ("fixed", "is_oos"), "calendar mode must be fixed or is_oos.")
    _keys(c, ["mode", "start", "end", "tradingTimeframeMinutes"]+(["isMonths", "oosMonths"] if c["mode"] == "is_oos" else []), "calendar")
    tf = integer(c["tradingTimeframeMinutes"], 5, 1440, "trading TF")
    require(tf % 5 == 0 and 1440 % tf == 0, "trading TF must divide a day.")
    stamp(c["start"]); stamp(c["end"], end=True)
    _keys(s, "version enabled method methodVersion topN rebalanceMonths evaluateUnselected params".split(), "tickerSelector")
    require(type(s["version"]) is int and s["version"] == 1 and type(s["methodVersion"]) is int and s["methodVersion"] == 1,
        "unsupported X method/version.")
    require(s["enabled"] is True and s["method"] == "meanBarRangePct" and type(s["evaluateUnselected"]) is bool, "X must be enabled with an explicit boolean evaluateUnselected.")
    _keys(s["params"], ["lookbackMonths", "timeframeMinutes"], "X params")
    effective = deepcopy(raw)
    # Frozen v1 accepts the same positive-whole-number count domain as core.
    # Live preparation still delegates normalization to core below.
    for obj, keys in ((effective["tickerSelector"], ("topN", "rebalanceMonths")),
                      (effective["tickerSelector"]["params"], ("lookbackMonths",)),
                      (effective["calendar"], ("isMonths", "oosMonths") if c["mode"] == "is_oos" else ())):
        for key in keys:
            value = obj[key]
            require(type(value) in (int, float) and math.isfinite(value) and value >= 1 and int(value) == value,
                f"{key}: positive whole number required.")
            obj[key] = int(value)
    rank_tf = integer(s["params"]["timeframeMinutes"], 5, pd.Timedelta.max.value//(60*10**9), "ranking TF")
    require(rank_tf % 5 == 0, "ranking TF must be a multiple of five minutes.")
    _keys(controls, ["seed", "draws"], "controls")
    integer(controls["seed"], 0, 2**64-1, "seed")
    integer(controls["draws"], 1, 100000, "draws")
    if live:
        from core.ticker_selector import normalize_selector
        effective["tickerSelector"] = normalize_selector(s)
    return effective


def common_projection(metadata):
    effective = metadata["request"]["effective"]
    return dict(schema_version="strategy_lab_x_semantics_v1",
        selector={k: v for k, v in effective["tickerSelector"].items() if k != "evaluateUnselected"},
        roster=sorted(metadata["universe"]["document"]["tickers"]),
        trading_timeframe_minutes=effective["calendar"]["tradingTimeframeMinutes"])


def row_evidence(rows):
    return [{k: r[k] for k in ("instrument", "listing", "reason_code", "eligible", "score", "rank", "selected", "months")} for r in rows]


def decision_identity(metadata, period, rows):
    return digest(dict(**common_projection(metadata), period_key=[period["start"], period["end"]],
        cutoff=period["start"], rows=row_evidence(rows)))


def ranking_identity(metadata, records):
    return digest(dict(**common_projection(metadata), calendar=metadata["calendar"],
        periods=[dict(period_key=p["period_key"], rows=row_evidence(p["rows"])) for p in records]))


def seed_for(metadata, record):
    controls = metadata["request"]["effective"]["controls"]
    raw = canonical_json_bytes(dict(schema_version="strategy_lab_x_random_seed_v1", **controls,
        ranking_decision_identity=record["ranking_decision_identity"], period_key=record["period_key"]))
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big")


def index_dtype(size):
    return "|u1" if size <= 256 else "<u2" if size <= 65536 else "<u4"


def controls_identity(metadata):
    c = metadata["controls"]
    return digest(dict(schema_version="strategy_lab_x_controls_v1", ranking_identity=metadata["ranking_identity"],
        seed=c["seed"], draws=c["draws"], algorithm=c["algorithm"], periods=[{k: p[k] for k in
            ("period_key", "eligible", "dtype", "shape", "content_sha256")} for p in c["periods"]]))


def csv_rows(records):
    return [dict(period_id=p["id"], cutoff=p["period_key"][0], **{k: r[k] for k in CSV_COLUMNS[2:]})
        for p in records for r in p["rows"]]


def calendar_facts(effective, periods):
    c = effective["calendar"]
    step = pd.Timedelta(minutes=c["tradingTimeframeMinutes"])
    end = stamp(c["end"], end=True)
    exclusive = pd.Timestamp((end.value//step.value+1)*step.value, tz="UTC")
    return dict(requested_start=stamp(c["start"]).isoformat(), requested_end=end.isoformat(),
        exclusive_end=exclusive.isoformat(), periods=periods,
        trailing=[periods[-1]["end"], exclusive.isoformat()] if stamp(periods[-1]["end"]) < exclusive else None)


def prepare_ranking(request_path, output_dir, *, ticker_list, data_root):
    from core.ticker_selector import normalize_pool, build_calendar, rank_pool
    from data_provider.manifest import normalize_instrument_id
    path, raw_hash, raw = _read(request_path)
    effective = request(raw, live=True)
    universe = ticker_list if isinstance(ticker_list, Universe) else load_universe(ticker_list)
    universe.verify()
    roster = sorted(universe.document["tickers"])
    require(all(normalize_instrument_id(t) == t for t in roster), "universe requires canonical full instrument IDs.")
    c, selector = effective["calendar"], effective["tickerSelector"]
    require(selector["topN"] <= len(roster), "topN exceeds declared roster.")
    root = Path(data_root).resolve()
    output = new_output(output_dir, [root, path, universe.path])
    pool = normalize_pool([dict(type="parquet", version=1, pack_root=str(root), instrument_id=t,
        timeframe_minutes=c["tradingTimeframeMinutes"]) for t in roster])
    periods = build_calendar(c["start"], c["end"], c["tradingTimeframeMinutes"], selector["rebalanceMonths"],
        **(dict(is_months=c["isMonths"], oos_months=c["oosMonths"]) if c["mode"] == "is_oos" else {}))
    metadata = dict(schema_version="strategy_lab_x_ranking_v1", request=dict(path=path, file_sha256=raw_hash,
        raw=raw, effective=effective, effective_sha256=digest(effective)), universe=universe.provenance(),
        pool=pool, calendar=calendar_facts(effective, periods), git=git_facts())
    records, listings = [], {}
    for period in periods:
        rows = rank_pool(pool, period["start"], selector, cache=None)
        for row in rows:
            row["reason_code"] = None if row["eligible"] else "listing_after_lookback_start"
            ticker = row["instrument"]
            require(ticker not in listings or listings[ticker] == row["listing"], "listing evidence changed between cutoffs.")
            listings[ticker] = row["listing"]
        records.append(dict(id=period["id"], period_key=[period["start"], period["end"]], rows=rows,
            ranking_decision_identity=decision_identity(metadata, period, rows)))
    metadata["ranking_identity"] = ranking_identity(metadata, records)
    controls = metadata["controls"] = dict(**effective["controls"], algorithm=ALGORITHM,
        numpy_version=np.__version__, total_array_bytes=0, periods=[])
    for record in records:
        eligible = sorted(r["instrument"] for r in record["rows"] if r["eligible"])
        shape = [controls["draws"], min(selector["topN"], len(eligible))]
        dtype = index_dtype(len(eligible))
        controls["total_array_bytes"] += shape[0]*shape[1]*np.dtype(dtype).itemsize
        controls["periods"].append(dict(period_id=record["id"], period_key=record["period_key"], eligible=eligible,
            shape=shape, dtype=dtype, derived_seed=seed_for(metadata, record), file=f"plans/{record['id']}.npy"))
    files = ["ranking_rows.json", "ranking.csv"]
    print(f"Random plan arrays: {controls['total_array_bytes']} bytes across {len(records)} periods.", file=sys.stderr)
    for plan in controls["periods"]:
        generator = np.random.Generator(np.random.PCG64(plan["derived_seed"]))
        indices = np.empty(plan["shape"], dtype=plan["dtype"])
        for row in indices:
            row[:] = np.sort(generator.choice(len(plan["eligible"]), size=plan["shape"][1], replace=False))
        plan["content_sha256"] = hashlib.sha256(indices.tobytes(order="C")).hexdigest()
        buffer = io.BytesIO(); np.save(buffer, indices, allow_pickle=False)
        write_bytes(output, plan["file"], buffer.getvalue())
        files.append(plan["file"])
        del indices, buffer
    metadata["controls_identity"] = controls_identity(metadata)
    write_bytes(output, "ranking_rows.json", canonical_json_bytes(dict(schema_version="strategy_lab_x_rows_v1", periods=records)))
    write_rows(output, "ranking.csv", csv_rows(records), CSV_COLUMNS)
    return finish_report(output, "ranking.json", metadata, files, "Frozen Merlin X ranking",
        dict(instruments=len(roster), periods=len(periods), plan_array_bytes=controls["total_array_bytes"],
            interpretation="Market ranking only; no strategy outcomes or portfolio execution."))


def validate_calendar(metadata):
    c = metadata["request"]["effective"]["calendar"]
    selector = metadata["request"]["effective"]["tickerSelector"]
    cal = metadata["calendar"]
    _keys(cal, "requested_start requested_end exclusive_end periods trailing".split(), "saved calendar")
    periods = cal["periods"]
    require(isinstance(periods, list) and periods, "empty saved calendar.")
    require(cal == calendar_facts(metadata["request"]["effective"], periods), "calendar endpoint/trailing mismatch.")
    start, end = stamp(cal["requested_start"]), stamp(cal["exclusive_end"])
    require(start.day == 1 and start == start.normalize(), "calendar Start must be month-1 UTC.")
    history, width = (c["isMonths"], c["oosMonths"]) if c["mode"] == "is_oos" else (0, 1)
    cadence = selector["rebalanceMonths"]
    require(cadence % width == 0, "cadence must divide OOS geometry.")
    next_start, wid = start+pd.DateOffset(months=history), 1
    step = pd.Timedelta(minutes=c["tradingTimeframeMinutes"]).value
    for i, period in enumerate(periods, 1):
        _keys(period, "id start end windows".split(), "period")
        require(type(period["id"]) is int and period["id"] == i and period["windows"], "invalid period ID/windows.")
        require(period["start"] == next_start.isoformat(), "period gap/start mismatch.")
        windows = period["windows"]
        require(len(windows) <= cadence//width and (i == len(periods) or len(windows) == cadence//width), "invalid rebalance group width.")
        for w in windows:
            _keys(w, "id start end isStart".split(), "window")
            finish = next_start+pd.DateOffset(months=width)
            beginning = next_start-pd.DateOffset(months=history)
            require(type(w["id"]) is int and w == dict(id=wid, start=next_start.isoformat(), end=finish.isoformat(),
                isStart=beginning.isoformat() if history else None), "saved window boundaries/ID disagree.")
            require(finish <= end and all(t.value % step == 0 for t in (beginning, next_start, finish)), "invalid complete-window/grid geometry.")
            next_start, wid = finish, wid+1
        require(period["end"] == next_start.isoformat(), "period end mismatch.")
    require(wid-1 >= (2 if history else 1) and next_start+pd.DateOffset(months=width) > end, "saved calendar omits complete windows.")


def validate_rows(metadata, records):
    roster = sorted(metadata["universe"]["document"]["tickers"])
    require(roster and len(set(roster)) == len(roster) and all(isinstance(t, str) and re.fullmatch(r"[A-Z0-9][A-Z0-9.-]*_[A-Z0-9][A-Z0-9.-]*", t) for t in roster), "invalid canonical roster.")
    selector = metadata["request"]["effective"]["tickerSelector"]
    require(selector["topN"] <= len(roster), "topN exceeds roster.")
    require(len(metadata["pool"]) == len(roster), "pool size mismatch.")
    pool = {}
    for source in metadata["pool"]:
        _keys(source, "type version pack_root instrument_id timeframe_minutes".split(), "source")
        require(source["type"] == "parquet" and type(source["version"]) is int and source["version"] == 1 and
            source["timeframe_minutes"] == metadata["request"]["effective"]["calendar"]["tradingTimeframeMinutes"], "unsupported source identity.")
        _text(source["pack_root"], "source root")
        pool[source["instrument_id"]] = source
    require(sorted(pool) == roster, "source roster mismatch.")
    periods = metadata["calendar"]["periods"]
    require(isinstance(records, list) and len(records) == len(periods), "rank period count mismatch.")
    listings = {}
    for p, record in zip(periods, records):
        _keys(record, "id period_key rows ranking_decision_identity".split(), "rank period")
        require(type(record["id"]) is int and record["id"] == p["id"] and record["period_key"] == [p["start"], p["end"]], "rank period mismatch.")
        rows = record["rows"]
        require(isinstance(rows, list) and len(rows) == len(roster), "rank roster size mismatch.")
        require(sorted(r.get("instrument", "") for r in rows) == roster, "rank roster mismatch.")
        first = stamp(p["start"])-pd.DateOffset(months=selector["params"]["lookbackMonths"])
        step = pd.Timedelta(minutes=selector["params"]["timeframeMinutes"]).value
        for r in rows:
            _keys(r, ROW_KEYS, "rank row")
            t = r["instrument"]
            require(r["source"] == pool[t], "row source differs from pool.")
            require(t not in listings or listings[t] == r["listing"], "listing drift.")
            listings[t] = r["listing"]
            eligible = r["listing"] is None or stamp(r["listing"]) <= first
            require(type(r["eligible"]) is bool and r["eligible"] == eligible and type(r["selected"]) is bool, "listing/eligibility mismatch.")
            if not eligible:
                require(r["score"] is None and r["rank"] is None and r["selected"] is False and r["months"] == [] and
                    r["reason_code"] == "listing_after_lookback_start", "invalid listing exclusion.")
                _text(r["reason"], "core listing reason")
                continue
            require(r["reason"] is None and r["reason_code"] is None and type(r["score"]) in (int, float) and
                np.isfinite(r["score"]) and r["score"] >= 0 and type(r["rank"]) is int, "invalid eligible score/reason/rank.")
            require(len(r["months"]) == selector["params"]["lookbackMonths"], "monthly pin count mismatch.")
            for j, pin in enumerate(r["months"]):
                _keys(pin, PIN_KEYS, "monthly pin")
                month = first+pd.DateOffset(months=j); following = month+pd.DateOffset(months=1)
                left, right = -(-month.value//step)*step, following.value//step*step
                expected = (right-left)//step
                require(pin["month"] == month.strftime("%Y-%m") and stamp(pin["start"]).value == left and stamp(pin["end"]).value == right,
                    "monthly pin bounds mismatch.")
                require(type(pin["expected"]) is int and pin["expected"] == expected and expected > 0, "invalid expected bar count.")
                integer(pin["observed"], 0, expected, "observed bars")
                require(100*pin["observed"] >= 99*expected, "ranking monthly coverage below 99%.")
                require(type(pin["fingerprintVersion"]) is int and pin["fingerprintVersion"] == 1 and
                    isinstance(pin["fingerprint"], str) and re.fullmatch(r"[0-9a-f]{64}", pin["fingerprint"]), "unsupported monthly fingerprint.")
        eligible = sorted((r for r in rows if r["eligible"]), key=lambda r: (-r["score"], r["instrument"]))
        require(rows == eligible+sorted((r for r in rows if not r["eligible"]), key=lambda r:r["instrument"]), "rank row ordering mismatch.")
        for rank, r in enumerate(eligible, 1):
            require(r["rank"] == rank and r["selected"] == (rank <= selector["topN"]), "rank/Top-K mismatch.")
        require(record["ranking_decision_identity"] == decision_identity(metadata, p, rows), "decision identity mismatch.")


@dataclass(frozen=True)
class Ranking:
    root: Path
    metadata: dict
    records: list
    file_sha256: str
    snapshot_sha256: str

    def verify(self):
        require(digest([self.metadata, self.records]) == self.snapshot_sha256, "loaded ranking was mutated; reload.")

    def plan(self, index):
        self.verify()
        p = self.metadata["controls"]["periods"][index]
        claim = self.metadata["artifacts"][p["file"]]
        path = safe_path(self.root, p["file"])
        require(validate_artifact_record(self.root, claim), "random plan file verification failed.")
        with path.open("rb") as stream:
            require(stream.read(6) == b"\x93NUMPY", "random plan must be an uncompressed NPY array.")
        a = np.load(path, mmap_mode="r", allow_pickle=False)
        require(a.dtype.str == p["dtype"] and list(a.shape) == p["shape"], "random plan dtype/shape mismatch.")
        require(hashlib.sha256(a.tobytes(order="C")).hexdigest() == p["content_sha256"], "random index content hash mismatch.")
        require(a.size == 0 or (int(a.max()) < len(p["eligible"]) and np.all(a[:, 1:] > a[:, :-1])), "random indices must be sorted, unique and in range.")
        return a


def open_ranking(root):
    root = Path(root).absolute()
    path, file_hash, m = _read(safe_path(root, "ranking.json"))
    _keys(m, "schema_version request universe pool calendar git ranking_identity controls controls_identity artifacts".split(), "ranking descriptor")
    require(m["schema_version"] == "strategy_lab_x_ranking_v1", "unsupported frozen ranking version.")
    _keys(m["request"], "path file_sha256 raw effective effective_sha256".split(), "saved request")
    _text(m["request"]["path"], "request path")
    require(isinstance(m["request"]["file_sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", m["request"]["file_sha256"]), "invalid request byte hash.")
    effective = request(m["request"]["raw"])
    require(effective == m["request"]["effective"] and digest(effective) == m["request"]["effective_sha256"], "effective request mismatch.")
    u = m["universe"]
    _keys(u, "path file_sha256 document document_sha256".split(), "universe snapshot")
    _text(u["path"], "universe path")
    require(isinstance(u["file_sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", u["file_sha256"]), "invalid universe byte hash.")
    require(digest(u["document"]) == u["document_sha256"], "universe snapshot mismatch.")
    _keys(u["document"], "schema_version name evidence_classification selection_note tickers".split(), "universe")
    require(u["document"]["schema_version"] == "strategy_lab_universe_v1" and u["document"]["evidence_classification"] == "exploratory", "invalid universe schema/classification.")
    for k in ("name", "selection_note"):
        _text(u["document"][k], k)
    validate_calendar(m)
    controls = m["controls"]
    _keys(controls, "seed draws algorithm numpy_version total_array_bytes periods".split(), "controls")
    require(digest({k: controls[k] for k in ("seed", "draws")}) == digest(effective["controls"]) and controls["algorithm"] == ALGORITHM, "controls settings mismatch.")
    _text(controls["numpy_version"], "generation NumPy version")
    integer(controls["total_array_bytes"], 0, 2**64-1, "total plan bytes")
    require(len(controls["periods"]) == len(m["calendar"]["periods"]), "plan period count mismatch.")
    expected_files = {"ranking_rows.json", "ranking.csv", "index.html", *(f"plans/{p['id']}.npy" for p in m["calendar"]["periods"])}
    require(set(m["artifacts"]) == expected_files, "ranking artifacts mismatch.")
    for name, claim in m["artifacts"].items():
        _keys(claim, ["path", "size", "sha256"], "artifact")
        require(claim["path"] == name, "artifact path mismatch.")
        safe_path(root, name)
        require(validate_artifact_record(root, claim), f"ranking artifact verification failed: {name}.")
    _, _, rows = _read(root/"ranking_rows.json")
    _keys(rows, ["schema_version", "periods"], "rank rows")
    require(rows["schema_version"] == "strategy_lab_x_rows_v1", "unsupported rows version.")
    records = rows["periods"]
    validate_rows(m, records)
    require(m["ranking_identity"] == ranking_identity(m, records), "ranking identity mismatch.")
    with (root/"ranking.csv").open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        expected = [{k: json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False) if isinstance(v, (dict, list)) else
            "" if v is None else str(v) for k, v in row.items()} for row in csv_rows(records)]
        require(reader.fieldnames == CSV_COLUMNS and list(reader) == expected, "ranking CSV disagrees with authoritative rows.")
    total = 0
    for record, plan in zip(records, controls["periods"]):
        _keys(plan, "period_id period_key eligible shape dtype derived_seed file content_sha256".split(), "plan")
        eligible = sorted(r["instrument"] for r in record["rows"] if r["eligible"])
        shape = [controls["draws"], min(effective["tickerSelector"]["topN"], len(eligible))]
        require(type(plan["period_id"]) is int and isinstance(plan["shape"], list) and
            all(type(v) is int for v in plan["shape"]), "invalid plan ID/shape types.")
        integer(plan["derived_seed"], 0, 2**64-1, "derived seed")
        require(plan["period_id"] == record["id"] and plan["period_key"] == record["period_key"] and plan["eligible"] == eligible and
            plan["shape"] == shape and plan["dtype"] == index_dtype(len(eligible)) and plan["file"] == f"plans/{record['id']}.npy" and
            plan["derived_seed"] == seed_for(m, record), "plan metadata mismatch.")
        total += shape[0]*shape[1]*np.dtype(plan["dtype"]).itemsize
    require(controls["total_array_bytes"] == total and m["controls_identity"] == controls_identity(m), "controls identity/size mismatch.")
    result = Ranking(root, m, records, file_hash, digest([m, records]))
    for index in range(len(records)):
        result.plan(index)
    return result
