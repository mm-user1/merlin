"""Small exact-bar fixture and independent saved-input identity review oracle."""
import copy
import hashlib
import json
from pathlib import Path

from tools.pattern_lab import analysis, candidate, study
from . import _helpers as h


def frozen_generation(root):
    sources = root / "sources"
    sources.mkdir(parents=True)
    for module in ("portable_signal", "portable_unused"):
        (sources / (module + "_helper.py")).write_bytes(b"PERIOD = 3\n")
        (sources / (module + ".py")).write_bytes((
            f"from {module}_helper import PERIOD\nimport numpy as np\n"
            "from tools.pattern_lab.study import contracts\n"
            "def evaluate(series, parameters, features):\n"
            "    values = (series.timestamps_ms // 3600000 + sum(parameters.values())) % PERIOD == 0\n"
            "    return contracts.ConditionValue(values, np.ones(series.row_count, dtype=bool))\n"
            "def register(context):\n"
            f"    context.register_hypothesis(contracts.HypothesisDescriptor('{module}', '1', evaluate, validate_parameters=dict))\n"
        ).encode())
    stamps, values = h.timeframe_bars(60, [(100., 104., 96., 102., 16.)] * (24 * 12))
    h.publish(root / "pack", [h.instrument_source(stamps, values, symbol=name, contract=name + "-USDT-SWAP")
                              for name in ("AAA", "BBB")])
    start, discovery_end, end, coverage = [h.study_group_ms(24 * days, 60) for days in (4, 8, 11, 12)]
    protocol = h.study_protocol(first_ms=h.study_group_ms(0, 60), coverage_end_ms=coverage)
    protocol["development"] = {"start_utc": h.utc(h.study_group_ms(0, 60)), "end_utc": h.utc(end)}
    protocol["reserved"] = {"start_utc": h.utc(end), "end_utc": h.utc(coverage)}
    request = h.study_request(protocol=protocol, start_ms=start, end_ms=discovery_end,
        warmup_ms=h.study_group_ms(0, 60), timeframes=[60],
        hypotheses=[{"id": "portable", "hypothesis": "portable_signal", "parameters": {}, "occurrence": "every_qualifying_bar"}],
        models=[h.fixed_horizon_model(60, [60, 120, 240, 480], primary=240)],
        extensions=[{"module": name, "source_root": str(sources), "helpers": [name + "_helper.py"]}
                    for name in ("portable_signal", "portable_unused")])
    request.update(schema_version=2, context={}, execution={"kind": "development"})
    study.run_study(request=request, data_root=root / "pack", output_root=root / "study")
    analysis_request = h.analysis_request_document(schema_version=2, pairwise=[])
    analysis_request.pop("resamples")
    analysis_request.pop("seed")
    analysis.run_analysis(request=analysis_request, run_root=root / "study", output_root=root / "analysis")
    return candidate.freeze_candidate(study_root=root / "study", analysis_root=root / "analysis",
        start=h.utc(discovery_end), end=h.utc(end), warmup_start=h.utc(discovery_end - 2 * 3600000),
        output=root / "candidate.json")


def saved_identity_payloads(root, *, versions=None):
    """Reconstruct hash payloads from saved JSON, independently of the builders."""
    read = lambda name: json.loads((Path(root) / name).read_text(encoding="utf-8"))
    request, family, source = [read("spec/" + name + ".json") for name in ("request", "family", "source")]
    version = request["schema_version"]
    semantic = {k: v for k, v in request.items() if k not in ("study_name", "notes", "protocol_source")}
    source = copy.deepcopy(source)
    policy = source.get("identity_policy_version", 1)
    marker = {"identity_policy_version": 2} if policy == 2 else {}
    if policy == 2:
        for item in semantic["extensions"]:
            item.pop("source_root")
        for item in source["extensions"]:
            item.pop("source_root")
            item.pop("module_path")
    fingerprints, rules = [], {}
    for item in family["instruments"]:
        admitted = read(f"admitted/{item['instrument_id']}.json")
        fingerprints.extend({k: row[k] for k in ("instrument_id", "timeframe_minutes", "input_fingerprint")}
                            for row in admitted["timeframes"])
        if "bracket_rules" in admitted:
            rules[item["instrument_id"]] = admitted["bracket_rules"]
    implementation = {k: source[k] for k in ("core_source", "extensions", "library_versions", "bracket_source") if k in source}
    if versions is not None:
        implementation["library_versions"] = versions
    data = dict(fingerprints=fingerprints, specification=semantic,
                universe=[{"instrument_id": i["instrument_id"], "roles": i["roles"]} for i in family["instruments"]],
                protocol=read("spec/protocol.json"), version=version, **marker)
    if version == 2:
        data["context"] = {"admission": read("spec/context.json"), "outputs": read("context.json")}
    if rules:
        data["bracket_rules"] = rules
    return {"specification_sha256": dict(specification=semantic, family=family, version=version, **marker),
            "implementation_sha256": dict(implementation, version=version, **marker), "data_input_sha256": data}


def recompute(root, *, versions=None):
    return {k: hashlib.sha256(json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
            for k, v in saved_identity_payloads(root, versions=versions).items()}
