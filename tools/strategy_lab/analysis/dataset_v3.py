"""Offline v3 integrity/ownership validation; no source reads or current plans."""
import csv
import hashlib
from types import SimpleNamespace

from ..config import canonical_sha256
from ..config_v2 import validate_request, calendar
from ..dataset import DatasetError, load_json
from ..parquet import (identity, validate_bindings, verify_artifacts, verified_groups)


def validate_dataset_v3(root, manifest, raw):
    validate_request(raw)
    verify_artifacts(root, manifest)
    saved_identity = manifest["identity"]
    roster = raw["generation"]["market_data"]["instruments"]
    included = saved_identity["included_tickers"]
    symbols = [e["canonical_symbol"] for e in included]
    entries = [e for e in roster if e["instrument_id"] in symbols]
    if included != [{"canonical_symbol": e["instrument_id"], "cell": e["cell"]} for e in entries] or not entries:
        raise DatasetError("selected roster ownership/order mismatch.")
    ids = [w["window_id"] for w in saved_identity["windows"]]
    all_windows = calendar(raw["generation"])
    windows = [w for w in all_windows if w.window_id in ids]
    if [w.window_id for w in windows] != ids or not windows:
        raise DatasetError("selected window ownership/order mismatch.")
    scope = "full" if len(entries) == len(roster) and len(windows) == len(all_windows) else "smoke"
    if manifest.get("scope") != scope:
        raise DatasetError("dataset full/smoke scope mismatch.")
    candidates = load_json(root / "candidates.json", "candidates")
    if candidates.get("schema_version") != "strategy_lab_candidates_v2" or "plan_fingerprint" in candidates:
        raise DatasetError("v3 requires shared candidate geometry schema v2.")
    rows = candidates["candidates"]
    if len(rows) != raw["generation"]["planning"]["expected_candidate_count"] or candidates["identity_versions"] != raw["generation"]["strategy"]:
        raise DatasetError("candidate count/version mismatch.")
    digest = hashlib.sha256()
    axes = candidates["global_axis_names"]
    domains = raw["generation"]["planning"]["axis_values"]
    if set(axes) != set(domains) or len(axes) != len(domains):
        raise DatasetError("candidate axes disagree with declaration.")
    for index, row in enumerate(rows):
        if row["row_index"] != index or row["candidate_id"] != index + 1:
            raise DatasetError("candidate row order mismatch.")
        digest.update((row["semantic_key"] + "\n").encode())
        if len(row["active_axis_mask"]) != len(axes) or len(row["axis_value_codes"]) != len(axes) or len(row["active_axis_values"]) != len(axes):
            raise DatasetError("candidate axis width mismatch.")
        for axis, code, active, value in zip(axes, row["axis_value_codes"], row["active_axis_mask"], row["active_axis_values"]):
            if type(active) is not bool or type(code) is not int or active != (axis in row["axis_names"]):
                raise DatasetError("candidate axis mask mismatch.")
            if active:
                if code < 0 or code >= len(domains[axis]) or value != domains[axis][code] or row["params"][axis] != value:
                    raise DatasetError("candidate value/code mismatch.")
            elif code != -1 or value is not None:
                raise DatasetError("inactive candidate axis mismatch.")
    if digest.hexdigest() != candidates["semantic_key_digest"]:
        raise DatasetError("candidate semantic-key digest mismatch.")
    bindings = load_json(root / "input_bindings.json", "bindings")
    spec = SimpleNamespace(raw=raw, generation=raw["generation"],
        generation_sha256=canonical_sha256(raw["generation"]), pre_registration_sha256=canonical_sha256(raw))
    validate_bindings(spec, entries, windows, candidates, bindings)
    if identity(spec, entries, windows, candidates, bindings) != saved_identity:
        raise DatasetError("dataset semantic identity mismatch.")
    verified_groups(root, manifest, entries, windows, bindings, complete=True)
    from ..data_quality import QUALITY_COLUMNS
    with (root / "data_quality.csv").open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != list(QUALITY_COLUMNS):
            raise DatasetError("quality column schema mismatch.")
        quality = list(reader)
    expected_quality = [(e["instrument_id"], e["cell"], "", "source") for e in entries]
    expected_quality += [(e["instrument_id"], e["cell"], str(w.window_id), name)
                         for e in entries for w in windows for name in ("is", "oos")]
    observed = [(q["canonical_symbol"], q["cell"], q["window_id"], q["segment"]) for q in quality]
    if sorted(observed) != sorted(expected_quality) or any(
        q["status"] != "ok" or q["relative_filename"] or q["source_sha256"] for q in quality
    ):
        raise DatasetError("quality status/ownership mismatch.")
