"""Finite reductions and safe publication shared by the three saved-result reports."""
from collections import defaultdict
from copy import deepcopy
import csv
import hashlib
from html import escape
import json
import math
from pathlib import Path

import numpy as np

from .analysis.dataset import AnalysisError
from .analysis.json_utils import canonical_json_bytes
from .paths import PROTECTED_CANONICAL_OUTPUTS, REPO_ROOT


def finite(value):
    return value is not None and math.isfinite(value)


def number(value):
    return float(value) if finite(value) else None


def statistics(values):
    values = list(values)
    valid = [float(v) for v in values if finite(v)]
    return dict(expected=len(values), finite=len(valid), missing=len(values)-len(valid),
        positive=sum(v > 0 for v in valid),
        positive_share_expected=sum(v > 0 for v in valid)/len(values) if values else None,
        positive_share_finite=sum(v > 0 for v in valid)/len(valid) if valid else None,
        mean=float(np.mean(valid)) if valid else None, median=float(np.median(valid)) if valid else None,
        q25=float(np.quantile(valid, .25)) if valid else None,
        q75=float(np.quantile(valid, .75)) if valid else None,
        minimum=min(valid) if valid else None, maximum=max(valid) if valid else None,
        reason=None if valid else "no_finite_values")


def labels(items):
    names = [name for name, _ in items]
    if not names or len(set(names)) != len(names) or any(
        not isinstance(n, str) or not n.strip() or any(c in n for c in ("/", "\\", ":", "=")) or n in (".", "..")
        for n in names
    ):
        raise AnalysisError("unique nonblank display labels are required; paths are not labels.")


def concentration(contributions, *, complete=True, partial_support=False, order_key=None):
    """Group additive contributions; removal is arithmetic, never reweighting."""
    groups = defaultdict(list)
    for key, value in contributions:
        groups[str(key)].append(value)
    observed = {key: sum(v for v in values if finite(v)) for key, values in groups.items()
                if any(finite(v) for v in values)}
    positive = sum(max(v, 0) for v in observed.values())
    negative = -sum(min(v, 0) for v in observed.values())
    total = sum(observed.values()) if observed else None
    largest = min((k for k, v in observed.items() if v > 0),
                  key=lambda k: (-observed[k], order_key(k) if order_key else k), default=None)
    return [dict(group=k, contribution=observed.get(k), expected=len(groups[k]),
        finite=sum(finite(v) for v in groups[k]), observed_subtotal=total,
        complete_total=total if complete else None, partial_support=partial_support,
        positive_gross=positive, absolute_negative_gross=negative,
        positive_gross_share=max(observed[k], 0)/positive if k in observed and positive else None,
        largest_positive_group=largest,
        largest_positive_share=observed[largest]/positive if largest is not None else None,
        total_excluding_largest=total-observed[largest] if largest is not None and complete else None,
        reason="incomplete_contributions" if not complete else (None if positive else "no_positive_contribution"))
        for k in sorted(groups, key=order_key)]


def neighbourhood(values, indices, center):
    members = [values[i] for i in indices]
    distinct = sum(i != center and finite(values[i]) for i in indices)
    valid = [float(v) for v in members if finite(v)]
    mean = sum(valid)/len(valid) if valid else None
    status = ("insufficient_neighbors" if len(indices) < 2 else
              "no_finite_distinct_neighbor" if not distinct else "available")
    return dict(expected_members=len(indices), finite_members=len(valid),
        finite_distinct_neighbor_count=distinct, status=status,
        partial_support=len(valid) < len(members), center=number(values[center]),
        mean=mean if status == "available" else None,
        worst=min(valid) if status == "available" else None,
        positive_share=sum(v > 0 for v in valid)/len(valid) if status == "available" else None,
        center_minus_mean=values[center]-mean if status == "available" and finite(values[center]) else None)


def saved_roots(dataset):
    """Locators are provenance; only native absolute roots are protected."""
    from .filters import recorded_roots
    roots = [dataset.root]
    locator = dataset.manifest.get("provenance", {}).get("resolved_data_root")
    if locator:
        roots.append(locator)
    path = dataset.root / "input_bindings.json"
    if path.exists():
        doc = json.loads(path.read_text(encoding="utf-8"))
        roots.extend(b["original_root"] for b in doc["instruments"])
        roots.extend(recorded_roots(doc.get("effective_entry_filters", [])))
    source = dataset.run_spec.get("generation", {}).get("market_data", {})
    for key in ("root", "data_root", "pack_root"):
        if key in source:
            roots.append(source[key])
    return roots


def source_facts(dataset, scope):
    from dataclasses import asdict
    return deepcopy(dict(path=str(dataset.root), manifest_sha256=dataset.manifest_sha256,
        schema_version=dataset.schema_version, manifest=dataset.manifest,
        contract=dataset.run_spec, scope=asdict(scope), entry_filters=dataset.filter_metadata))


def new_output(output, roots):
    from .filters import protect_output
    from .fixed.storage import safe_path
    output = Path(output).absolute()
    protect_output(output, [*roots, *PROTECTED_CANONICAL_OUTPUTS, REPO_ROOT / "src" / "storage"])
    # safe_path checks existing ancestors; include ancestors above the report root too.
    if any(p.is_symlink() for p in (output, *output.parents)):
        raise AnalysisError("report output symlinks are forbidden.")
    safe_path(output, "index.html")
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise AnalysisError("report requires a new empty output directory.")
    return output


def write_bytes(output, name, data):
    from .dataset import atomic_write
    from .fixed.storage import safe_path
    atomic_write(safe_path(output, name), lambda path: path.write_bytes(data))


def write_rows(output, name, rows, columns=None):
    """Write a row iterator atomically, without retaining full pair tables."""
    from .dataset import atomic_write
    from .fixed.storage import safe_path
    if columns is None and isinstance(rows, (list, tuple)) and rows:
        columns = list(dict.fromkeys(k for row in rows for k in row))
    def write(path):
        iterator = iter(rows)
        first = next(iterator, None)
        names = columns or (list(first) if first is not None else ["status"])
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, names, lineterminator="\n")
            writer.writeheader()
            def emit(row):
                writer.writerow({k: json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False)
                    if isinstance(v, (dict, list, tuple)) else v for k, v in row.items()})
            if first is not None:
                emit(first)
            for row in iterator:
                emit(row)
    atomic_write(safe_path(output, name), write)


def finish_report(output, descriptor, metadata, files, title, overview, tables=()):
    """Publish the completion descriptor last; interrupted directories cannot resume."""
    from .dataset import artifact_record
    body = "<h1>" + escape(title) + "</h1><p>Saved-result descriptive diagnostics. CSV tables and linked structured JSON facts are authoritative.</p>"
    body += "<table><tr><th>Coverage / interpretation</th><th>Value</th></tr>"
    for key, value in overview.items():
        body += "<tr><td>" + escape(str(key)) + "</td><td>" + escape(str(value)) + "</td></tr>"
    body += "</table>"
    for caption, rows in tables:
        if not rows:
            continue
        columns = list(rows[0])
        body += "<h2>"+escape(caption)+"</h2><table><tr>"+"".join("<th>"+escape(c)+"</th>" for c in columns)+"</tr>"
        for row in rows:
            body += "<tr>"+"".join("<td>"+escape(str(row.get(c)))+"</td>" for c in columns)+"</tr>"
        body += "</table>"
    body += "<ul>" + "".join('<li><a href="' + escape(n, quote=True) + '">' + escape(n) + '</a></li>' for n in files) + "</ul>"
    html = '<!doctype html><html lang="en"><meta charset="utf-8"><title>' + escape(title) + '</title><style>body{background:#fff;color:#222;font:16px system-ui;max-width:1100px;margin:2em auto;padding:1em}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:.5em;text-align:left}a{color:#135ca0}</style>' + body + '</html>'
    write_bytes(output, "index.html", html.encode())
    metadata = deepcopy(metadata)
    metadata["artifacts"] = {n: artifact_record(output/n, output) for n in [*files, "index.html"]}
    write_bytes(output, descriptor, canonical_json_bytes(metadata))
    return dict(status="published", output=str(output), files=[*files, "index.html", descriptor])
