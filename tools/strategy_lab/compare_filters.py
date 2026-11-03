"""Offline, strictly paired continuous Fixed v2 Off/on account deltas."""
import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path

from .config import canonical_sha256
from .dataset import atomic_write, atomic_write_json, artifact_record
from .filters import protect_output, recorded_roots
from .fixed.accounting import csv_bytes
from .fixed.config import DATASET_V2
from .fixed.storage import open_dataset, safe_path

METRICS = ("net_profit_pct", "max_drawdown_mtm_pct", "total_trades")
LABELS = ("parameter_set", "period", "instrument_id", "cell", "timeframe_minutes")


def compare(baseline, variant, output_dir):
    roots = [Path(baseline).resolve(), Path(variant).resolve()]
    captured = [{}, {}]
    manifests = [open_dataset(root, on_account=lambda a, r, rows, saved=saved: saved.update({a["account_id"]: r}))
                 for root, saved in zip(roots, captured)]
    if any(m["schema_version"] != DATASET_V2 for m in manifests):
        raise ValueError("comparison requires two complete Fixed v2 datasets; IS/OOS and historical families are unsupported.")
    contracts = [m["contract"] for m in manifests]
    items = [c["resolved"]["entry_filters"] for c in contracts]
    if items[0] or not items[1]:
        raise ValueError("comparison requires effective Off baseline and active variant.")
    declarations = []
    for c in contracts:
        generation = deepcopy(c["request"]["generation"])
        del generation["execution"]["entryFilters"]
        declarations.append(generation)
    if declarations[0] != declarations[1]:
        raise ValueError("non-filter declared settings differ (roster/order, economics, parameters, bounds or resources).")
    scalars = [{k: v for k, v in c["resolved"].items() if k != "entry_filters"} for c in contracts]
    if scalars[0] != scalars[1]:
        raise ValueError("effective strategy scalar contracts differ.")
    output = Path(output_dir).absolute()
    protected = [*roots]
    for c, descriptors in zip(contracts, items):
        protected.extend(a["provenance"]["pack_root"] for a in c["accounts"])
        protected.extend(recorded_roots(descriptors))
    protect_output(output, protected)
    for name in ("pairs.csv", "comparison.json"):
        safe_path(output, name)
    if output.exists() and any(output.iterdir()):
        raise ValueError("comparison requires a new empty output directory.")
    if len(contracts[0]["accounts"]) != len(contracts[1]["accounts"]):
        raise ValueError("comparison coverage differs.")
    from core.engine_v2.admission import read_admission_envelope
    rows = []
    for off, on in zip(contracts[0]["accounts"], contracts[1]["accounts"]):
        for key in (*LABELS, "params", "input_pin", "candidate"):
            if off[key] != on[key]:
                raise ValueError(f"comparison target/scalar/geometry mismatch: {key}.")
        rules = [read_admission_envelope(a["entry_admission"], a["params"]["maxEntryLeverage"]).semantic() for a in (off, on)]
        if rules[0] != rules[1]:
            raise ValueError("comparison operative admission rules differ.")
        row = {k: off[k] for k in LABELS}
        row.update(off_account_id=off["account_id"], on_account_id=on["account_id"], candidate_semantic_key=off["candidate"]["semantic_key"])
        for label, saved, a in zip(("off", "on"), captured, (off, on)):
            result = saved[a["account_id"]]
            for metric in METRICS:
                row[label + "_" + metric] = result["metrics"][metric]
                row[label + "_" + metric + "_unavailable_reason"] = result["metric_unavailable_reasons"].get(metric, "")
        for metric in METRICS:
            values = [row[label + "_" + metric] for label in ("off", "on")]
            row["delta_" + metric] = values[1] - values[0] if all(v is not None and math.isfinite(v) for v in values) else None
        rows.append(row)
    columns = (*LABELS, "off_account_id", "on_account_id", "candidate_semantic_key", *(
        k for metric in METRICS for k in ("off_"+metric, "off_"+metric+"_unavailable_reason",
            "on_"+metric, "on_"+metric+"_unavailable_reason", "delta_"+metric)))
    metadata = {"schema_version": "strategy_lab_filter_comparison_v1", "mode": "continuous_fixed_v2",
        "matching_policy": "exact_nonfilter_declaration_scalars_target_pin_admission_geometry_in_declared_order",
        "row_count": len(rows), "coverage": [{k: r[k] for k in LABELS} for r in rows],
        "sources": {label: {"path": str(root), "schema_version": m["schema_version"],
            "manifest_sha256": hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest(),
            "contract_sha256": m["contract_sha256"], "request_sha256": m["request_sha256"],
            "entryFilters": c["request"]["generation"]["execution"]["entryFilters"],
            "entry_filter_key": canonical_sha256(descriptors)}
            for label, root, m, c, descriptors in zip(("off", "on"), roots, manifests, contracts, items)},
        "units": {metric: {"off": "count" if metric == "total_trades" else "percent",
            "on": "count" if metric == "total_trades" else "percent",
            "delta": "count" if metric == "total_trades" else "percentage_points"} for metric in METRICS},
        "delta_direction": "On minus Off; negative drawdown delta means less measured drawdown, not higher profit.",
        "interpretation": "Descriptive paired accounts; no ranking, portfolio aggregation or established filter improvement."}
    atomic_write(safe_path(output, "pairs.csv"), lambda p: p.write_bytes(csv_bytes(rows, columns)))
    metadata["pairs"] = artifact_record(output / "pairs.csv", output)
    atomic_write_json(safe_path(output, "comparison.json"), metadata)
    return metadata


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--variant", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = compare(args.baseline, args.variant, args.output_dir)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        parser.exit(2, f"Filter comparison refused: {exc}\n")
    print(json.dumps({"rows": result["row_count"], "output": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
