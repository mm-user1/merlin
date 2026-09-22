"""Frozen whole-family candidates and later-interval validation.

The ID binds the acyclic numerical payload. Paths, names and the freeze clock
are provenance. Loading checks saved facts only; execution additionally checks
current required source bytes, resolved descriptors and admitted contracts.
"""
from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import re
from typing import Any, Mapping

from . import PatternLabDataError
from . import data, manifest, update_transaction
from .study import contracts, evidence, extensions, spec, validation
from .analysis import artifacts, family as analysis_family, request as analysis_request

CANDIDATE_SCHEMA_VERSION = 1
REQUIRED_CODE_POLICY_VERSION = 1
REQUIRED_CORE_MODULES = tuple("tools.pattern_lab." + name for name in (
    "data", "manifest", "study.contracts", "study.builtins", "study.spec",
    "study.validation", "study.extensions", "study.observations", "study.job",
    "study.runner", "study.results", "analysis.request", "analysis.family",
    "analysis.estimator", "analysis.monthly", "analysis.source", "analysis.runner",
    "analysis.artifacts",
))
CONTEXT_CORE_MODULES = ("tools.pattern_lab.study.context",)
PRIOR_USE = (
    "Freezing is not proof of historical preregistration or unseen data.",
    "The operational July-September 2026 reserve is prospective, not certified untouched: "
    "prototype validation reached approximately 2026-07-19 12:30 UTC and global volume "
    "ranks/correlations consumed reserved observations. Freezing does not erase prior use.",
    "Internal reuse is development evidence. Short validation retains the unchanged "
    "monthly support gates and can provide descriptions without inference or an edge claim.",
)
_KEYS = ("schema_version", "required_code_policy_version", "candidate_id", "recipe", "split",
         "discovery", "required_code", "prior_use", "provenance")


def _object(value, keys, where):
    result = contracts.require_mapping(value, where)
    contracts.closed_keys(result, keys, where)
    missing = set(keys) - set(result)
    if missing:
        raise PatternLabDataError(f"{where}: missing fields {sorted(missing)}.")
    return result


def _digest(value, where):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise PatternLabDataError(f"{where}: expected a SHA-256 digest.")
    return value


def _numerical(value):
    """Remove only named descriptive/physical fields from the candidate ID."""
    if isinstance(value, dict):
        return {key: (copy.deepcopy(item) if key in ("parameters", "settings") else _numerical(item)) for key, item in value.items()
                if key not in ("study_name", "analysis_name", "notes", "protocol_source",
                               "source_root", "module_path", "run_root", "label")}
    if isinstance(value, list):
        return [_numerical(item) for item in value]
    return value


def candidate_identity(document):
    return contracts.semantic_digest(_numerical({key: document[key] for key in _KEYS
                                                if key not in ("candidate_id", "provenance", "prior_use")}))


def _semantic(document):
    return {key: copy.deepcopy(value) for key, value in document.items()
            if key not in ("study_name", "notes", "protocol_source")}


def _external(saved):
    """Lossless saved semantic recipe -> public request, without a registry."""
    document = copy.deepcopy(dict(saved))
    document.pop("protocol_source", None)
    document["hypotheses"] = [
        {"id": item["variant_id"], "hypothesis": item["hypothesis_id"],
         "parameters": item["parameters"], "occurrence": item["occurrence"]}
        for item in saved["hypotheses"]]
    document["models"] = [{"id": item["model_instance_id"], "model": item["model_id"],
                           "settings": item["settings"]} for item in saved["models"]]
    document["metrics"] = [{"id": item["declaration_id"], "metric": item["metric_id"]}
                           for item in saved["metrics"]]
    document.setdefault("study_name", "Frozen candidate")
    document.setdefault("notes", None)
    return document


def _split(recipe, start, end, warmup_start):
    semantic = recipe["study_semantic"]
    protocol = spec.normalize_protocol(semantic["protocol"], source="candidate protocol")
    bounds = protocol["_bounds"]
    start_ms, end_ms, warmup_ms = [manifest.to_epoch_ms(value, name) for value, name in
                                  ((start, "start"), (end, "end"), (warmup_start, "warmup_start"))]
    discovery_end = manifest.to_epoch_ms(semantic["study"]["end_utc"], "discovery.end")
    if not bounds["earliest_warmup_ms"] <= warmup_ms <= start_ms < end_ms or start_ms < discovery_end:
        raise PatternLabDataError("candidate split: requires permitted warmup <= start < end and start >= discovery end.")
    if (start_ms, end_ms) == (bounds["reserved_start_ms"], bounds["reserved_end_ms"]):
        if start_ms < bounds["development_end_ms"]:
            raise PatternLabDataError("candidate split: full reserve must be later than and disjoint from development.")
        mode = "reserved"
    else:
        spec.validate_against_protocol(protocol, study_start_ms=start_ms, study_end_ms=end_ms,
                                       warmup_start_ms=warmup_ms)
        mode = "internal_reuse"
    for tf in semantic["timeframes_minutes"]:
        step = manifest.require_int(tf, "candidate timeframe", minimum=5) * 60000
        for name, value in (("start",start_ms), ("end",end_ms), ("warmup",warmup_ms)):
            manifest.require_aligned(value, step, "candidate " + name)
        required = max(item["required_prior_bars"] for item in semantic["hypotheses"])
        if (start_ms-warmup_ms)//step < required:
            raise PatternLabDataError("candidate warmup: insufficient prior observation bars.")
    return {"mode": mode, "evaluation": {"start_utc": manifest.format_epoch_ms(start_ms),
            "end_utc": manifest.format_epoch_ms(end_ms), "warmup_start_utc": manifest.format_epoch_ms(warmup_ms)}}


def _required_names(recipe):
    return (*REQUIRED_CORE_MODULES, *(CONTEXT_CORE_MODULES if recipe["study_semantic"]["context"] else ()))


def _generation(source, analyzed, recipe):
    study_hashes = source.results.source["core_source"]
    analysis_hashes = analyzed.provenance["implementation"]["modules"]
    shared = set(study_hashes) & set(analysis_hashes) & set(_required_names(recipe))
    if any(study_hashes[key] != analysis_hashes[key] for key in shared):
        raise PatternLabDataError("candidate generation: shared study/analysis module hashes disagree.")
    merged = {**study_hashes, **analysis_hashes}
    missing = sorted(set(_required_names(recipe)) - set(merged))
    if missing:
        raise PatternLabDataError(f"candidate generation: historical attribution lacks {missing}; produce a fresh development generation.")
    return {"core": {key: merged[key] for key in _required_names(recipe)},
            "extensions": [{"module": item["module"], "files": item["files"]}
                           for item in source.results.source["extensions"]]}


def _resolved_analysis(recipe):
    saved = recipe["analysis_request"]
    request = analysis_request.load_analysis_request({key: value for key, value in saved.items() if key != "method"})
    study_family = recipe["study_family"]
    instances = {item["model_instance_id"]: item for item in study_family["models"]}
    analysis_request.require_known_models(request, instances, where="candidate")
    analysis_request.require_known_variants(request, [v["variant_id"] for v in study_family["variants"]], where="candidate")
    comparisons, members = analysis_family.resolve_family(request, variants=study_family["variants"],
        instances=instances, timeframes=study_family["timeframes_minutes"])
    return analysis_family.family_document(request_version=2, comparisons=comparisons, members=members,
        model_instances=sorted(request.model_instances), timeframes=study_family["timeframes_minutes"],
        variants=study_family["variants"])


def load_candidate(candidate: Any) -> dict[str, Any]:
    """Verify a candidate path or mapping without pack/source/current-code access."""
    raw = manifest.read_json_file(Path(candidate)) if isinstance(candidate, (str, Path)) else candidate
    try:
        document = copy.deepcopy(dict(_object(raw, _KEYS, "candidate")))
        for key in ("schema_version", "required_code_policy_version"):
            if type(document[key]) is not int or document[key] != 1:
                raise PatternLabDataError(f"candidate.{key}: unsupported version.")
        recipe = _object(document["recipe"], ("study", "study_semantic", "study_family", "context_admission",
                         "analysis_request", "analysis_family"), "candidate.recipe")
        semantic = contracts.require_mapping(recipe["study_semantic"], "candidate.study_semantic")
        _object(semantic, ("schema_version", "study", "instruments", "timeframes_minutes", "hypotheses",
                          "models", "metrics", "protocol", "extensions", "context", "execution"), "candidate.study_semantic")
        _object(recipe["study"], (*spec.REQUEST_KEYS, "context", "execution"), "candidate.study")
        if semantic.get("schema_version") != 2 or semantic.get("execution") != {"kind":"development"}:
            raise PatternLabDataError("candidate recipe must be a lifted v2 development study.")
        validation.validate_saved_execution(semantic)
        if _numerical(_external(semantic)) != _numerical(recipe["study"]):
            raise PatternLabDataError("candidate recipe: external and resolved study disagree.")
        family = recipe["study_family"]
        for key in ("hypotheses", "models", "timeframes_minutes"):
            if not isinstance(semantic[key], list) or not semantic[key]:
                raise PatternLabDataError(f"candidate.study_semantic.{key}: expected a nonempty list.")
        for variant in semantic["hypotheses"]:
            _object(variant, ("variant_id", "hypothesis_id", "hypothesis_version", "parameters", "occurrence",
                              "condition_id", "required_prior_bars"), "candidate hypothesis")
            manifest.require_int(variant["required_prior_bars"], "candidate required_prior_bars", minimum=0)
            if variant["occurrence"] not in spec.OCCURRENCE_POLICIES:
                raise PatternLabDataError("candidate hypothesis: unknown occurrence policy.")
        if recipe["context_admission"] is not None:
            context_facts = recipe["context_admission"]
            if context_facts["aliases"] != semantic["context"]:
                raise PatternLabDataError("candidate context admission contradicts alias membership.")
            expected_sources = {identifier for alias in semantic["context"].values() for identifier in alias["ids"]}
            if {item["instrument_id"] for item in context_facts["sources"]} != expected_sources:
                raise PatternLabDataError("candidate context admission contradicts source membership.")
        elif semantic["context"]:
            raise PatternLabDataError("candidate context admission is required for context inputs.")
        if family["variants"] != semantic["hypotheses"] or family["models"] != semantic["models"]:
            raise PatternLabDataError("candidate recipe: resolved family contradicts study descriptors.")
        if semantic["instruments"] != {"ids": [item["instrument_id"] for item in family["instruments"]]}:
            raise PatternLabDataError("candidate recipe: explicit membership contradicts resolved family.")
        if family["timeframes_minutes"] != semantic["timeframes_minutes"]:
            raise PatternLabDataError("candidate recipe: timeframe drift.")
        if analysis_request.validate_saved_request(recipe["analysis_request"], source="candidate analysis") != 2:
            raise PatternLabDataError("candidate requires monthly analysis v2.")
        if recipe["analysis_family"]["method"] != recipe["analysis_request"]["method"]:
            raise PatternLabDataError("candidate analysis family/method drift.")
        if recipe["analysis_family"]["source_variants"] != semantic["hypotheses"]:
            raise PatternLabDataError("candidate analysis family/variants drift.")
        analysis_members = recipe["analysis_family"]["members"]
        if (type(recipe["analysis_family"]["family_size"]) is not int
                or recipe["analysis_family"]["family_size"] != len(analysis_members)
                or len({member["member_id"] for member in analysis_members}) != len(analysis_members)):
            raise PatternLabDataError("candidate analysis family: inconsistent member count or duplicate IDs.")
        split = _object(document["split"], ("mode", "evaluation"), "candidate.split")
        period = _object(split["evaluation"], ("start_utc", "end_utc", "warmup_start_utc"), "candidate.evaluation")
        if split != _split(recipe, period["start_utc"], period["end_utc"], period["warmup_start_utc"]):
            raise PatternLabDataError("candidate split: inconsistent classification.")
        code = _object(document["required_code"], ("core", "extensions"), "candidate.required_code")
        if set(code["core"]) != set(_required_names(recipe)):
            raise PatternLabDataError("candidate required-code policy: incorrect module membership.")
        for key, value in code["core"].items():
            _digest(value, key)
        if not isinstance(code["extensions"], list):
            raise PatternLabDataError("candidate extensions: expected a list.")
        extensions.source_records(code["extensions"], semantic["extensions"], where="candidate")
        for extension in code["extensions"]:
            _object(extension, ("module", "files"), "candidate extension")
            for item in extension["files"]:
                _object(item, ("path", "sha256"), "candidate helper")
        discovery = _object(document["discovery"], ("binding", "study_completion_sha256", "analysis_completion_sha256",
                            "analysis_identities", "source_study_version"), "candidate.discovery")
        _digest(discovery["study_completion_sha256"], "discovery study completion")
        _digest(discovery["analysis_completion_sha256"], "discovery analysis completion")
        binding = discovery["binding"]
        if type(discovery["source_study_version"]) is not int or discovery["source_study_version"] not in (1,2):
            raise PatternLabDataError("candidate discovery source study version is invalid.")
        if binding["schema_version"] != discovery["source_study_version"]:
            raise PatternLabDataError("candidate discovery source binding version is inconsistent.")
        for key in artifacts.COMPLETION_IDENTITY_KEYS:
            _digest(discovery["analysis_identities"][key], "discovery analysis " + key)
        for key in evidence.COMPLETION_IDENTITY_KEYS:
            _digest(binding["semantic"][key], "discovery " + key)
        physical = binding["physical"]
        if physical["evidence_set_sha256"] != contracts.semantic_digest(physical["evidence_sha256"]):
            raise PatternLabDataError("candidate discovery evidence set drift.")
        if binding["study"] != semantic["study"] or binding["protocol"] != semantic["protocol"]:
            raise PatternLabDataError("candidate discovery protocol/period drift.")
        if not isinstance(document["prior_use"], list) or not all(isinstance(v,str) for v in document["prior_use"]):
            raise PatternLabDataError("candidate prior_use: expected a list of strings.")
        contracts.require_mapping(document["provenance"], "candidate provenance")
        if _digest(document["candidate_id"], "candidate_id") != candidate_identity(document):
            raise PatternLabDataError("candidate_id: payload digest mismatch.")
        return document
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError) as error:
        raise PatternLabDataError(f"candidate: malformed saved contract: {error}") from error


def freeze_candidate(*, study_root, analysis_root, start, end, warmup_start, output):
    """Freeze a complete development generation; metadata/digests only."""
    from .analysis.source import admit_source
    analyzed = artifacts.load_analysis(analysis_root)
    if analyzed.request["schema_version"] != 2:
        raise PatternLabDataError("freeze requires a completed monthly-v2 analysis.")
    source = admit_source(study_root, model_instances=analyzed.request["model_instances"], where="candidate freeze")
    saved = source.results.request
    if saved.get("execution", {"kind":"development"}) != {"kind":"development"}:
        raise PatternLabDataError("cannot freeze a validation-origin study.")
    binding = source.binding_document()
    for key in ("semantic", "physical", "semantic_inputs"):
        if binding[key] != analyzed.source[key]:
            raise PatternLabDataError(f"candidate discovery: analysis {key} names a different study generation.")
    lifted = _semantic(saved)
    lifted.update(schema_version=2, context=copy.deepcopy(saved.get("context", {})), execution={"kind":"development"},
                  instruments={"ids": list(source.instruments)})
    recipe = {"study": _external(lifted), "study_semantic": lifted,
              "study_family": copy.deepcopy(source.results.family),
              "context_admission": (evidence.read_json(source.run_root/evidence.CONTEXT_ADMISSION_FILE)
                                    if saved["schema_version"] == 2 else None),
              "analysis_request": copy.deepcopy(analyzed.request), "analysis_family": copy.deepcopy(analyzed.family)}
    if _resolved_analysis(recipe) != analyzed.family:
        raise PatternLabDataError("candidate analysis family differs from its complete declared recipe.")
    document = {"schema_version": 1, "required_code_policy_version": 1, "candidate_id": "",
                "recipe": recipe, "split": _split(recipe, start, end, warmup_start),
                "discovery": {"binding": binding, "study_completion_sha256": manifest.file_sha256(source.run_root/evidence.COMPLETION_FILE),
                    "analysis_completion_sha256": manifest.file_sha256(analyzed.analysis_root/artifacts.COMPLETION_FILE),
                    "analysis_identities": dict(analyzed.completion["identities"]), "source_study_version": saved["schema_version"]},
                "required_code": _generation(source, analyzed, recipe), "prior_use": list(PRIOR_USE),
                "provenance": {"frozen_utc": artifacts.now_utc(), "study_root": str(source.run_root),
                    "analysis_root": str(analyzed.analysis_root), "study_source": source.results.source,
                    "analysis_implementation": analyzed.provenance["implementation"]}}
    document["candidate_id"] = candidate_identity(document)
    checked = load_candidate(document)
    path = Path(output).expanduser().resolve()
    if path.exists() or any(root == path or root in path.parents for root in (source.run_root, analyzed.analysis_root)):
        raise PatternLabDataError("candidate output must be new and outside discovery artifacts.")
    evidence.write_json(path, checked)
    return checked


def validate_execution(document):
    """One saved policy for execution and offline study/analysis admission."""
    execution = _object(document.get("execution"), ("kind", "candidate"), "execution")
    if execution["kind"] != "validation" or document.get("schema_version") != 2:
        raise PatternLabDataError("execution requires a v2 candidate-bound validation study.")
    candidate = load_candidate(execution["candidate"])
    expected = copy.deepcopy(candidate["recipe"]["study_semantic"])
    expected["study"] = candidate["split"]["evaluation"]
    expected["execution"] = execution
    if _numerical(_semantic(document)) != _numerical(expected):
        raise PatternLabDataError("validation execution: recipe, protocol or exact split differs from the candidate.")
    return candidate


def verify_current_generation(candidate, *, actual_extensions=None):
    """Required source bytes only: report/transport/provenance drift is permitted."""
    for name, digest in candidate["required_code"]["core"].items():
        module = importlib.util.find_spec(name)
        if module is None or not module.origin or extensions.file_digest(Path(module.origin)) != digest:
            raise PatternLabDataError(f"candidate required generation changed: {name}; produce a fresh development generation.")
    if actual_extensions is None:
        actual_extensions = []
        for item in candidate["recipe"]["study"]["extensions"]:
            declaration = spec.ExtensionDeclaration(**item)
            actual_extensions.append({"module": item["module"], "files": [
                {"path": name, "sha256": digest} for name, digest in extensions._resolve_digests(declaration)]})
    verify_extension_generation(candidate, actual_extensions)


def verify_extension_generation(candidate, actual):
    """Compare actual execution or saved attribution, using records only."""
    extensions.compare_source_records(actual, candidate["required_code"]["extensions"],
        candidate["recipe"]["study_semantic"]["extensions"], where="candidate validation")


def verify_admitted_contracts(candidate, request, entries, context_entries):
    from .study import context, runner
    current = runner.planned_family(request, entries)
    frozen = candidate["recipe"]["study_family"]
    for key in ("instruments", "timeframes_minutes", "variants", "models", "groups"):
        if current[key] != frozen[key]:
            raise PatternLabDataError(f"candidate admitted {key}: frozen contracts or descriptors changed.")
    old = candidate["recipe"]["context_admission"]
    if old is not None and context.admission(request, context_entries, [e["instrument_id"] for e in entries]) != old:
        raise PatternLabDataError("candidate admitted context: contracts/roles/membership changed.")


def validation_metadata(candidate):
    return {"kind": "validation", "candidate_id": candidate["candidate_id"],
            "discovery": candidate["recipe"]["study_semantic"]["study"],
            **candidate["split"], "prior_use": candidate["prior_use"],
            "fixed_family_size": candidate["recipe"]["analysis_family"]["family_size"]}


def _verified_validation_children(root, frozen):
    """One saved-record check shared by receipt publication and offline reads."""
    from .analysis.source import admit_source
    source = admit_source(root/"study", model_instances=frozen["recipe"]["analysis_request"]["model_instances"])
    analyzed = artifacts.load_analysis(root/"analysis")
    if source.results.request.get("execution") != {"kind": "validation", "candidate": frozen}:
        raise PatternLabDataError("validation child candidate contradicts the parent snapshot.")
    binding = source.binding_document()
    for key in ("semantic", "physical", "semantic_inputs"):
        if binding[key] != analyzed.source[key]:
            raise PatternLabDataError(f"validation child source binding mismatch: {key}.")
    if (analyzed.request != frozen["recipe"]["analysis_request"]
            or analyzed.family != frozen["recipe"]["analysis_family"]):
        raise PatternLabDataError("validation child analysis contradicts the frozen request/family.")
    return {name: {"completion_sha256": manifest.file_sha256(root/name/"completion.json"),
                   "evidence_set_sha256": seal["evidence_set_sha256"]}
            for name, seal in (("study", source.results.completion), ("analysis", analyzed.completion))}


def load_validation(output_root):
    """Return the verified final receipt; status alone never means complete.

    Reads saved metadata and file digests only. Relocation needs neither source
    code, original paths, a market pack, table decoding nor inference.
    """
    root = Path(output_root).expanduser().resolve()
    if not (root/"receipt.json").is_file():
        raise PatternLabDataError(f"{root}: validation is incomplete: missing final receipt.json.",
                                  error_code="incomplete_run")
    try:
        frozen = load_candidate(root/"candidate.json")
        metadata = validation_metadata(frozen)
        receipt = _object(evidence.read_json(root/"receipt.json"),
            ("schema_version", "status", *metadata, "children", "completed_utc"), "validation receipt")
        if type(receipt["schema_version"]) is not int or receipt["schema_version"] != 1:
            raise PatternLabDataError("validation receipt: unsupported schema_version; supported: 1.")
        if receipt["status"] != "completed" or any(receipt[key] != value for key, value in metadata.items()):
            raise PatternLabDataError("validation receipt: status/candidate/period/family metadata disagreement.")
        manifest.require_int(receipt["fixed_family_size"], "validation receipt.fixed_family_size", minimum=1)
        manifest.to_epoch_ms(receipt["completed_utc"], "validation receipt.completed_utc")
        children = _object(receipt["children"], ("study", "analysis"), "validation receipt.children")
        for name, child in children.items():
            child = _object(child, ("completion_sha256", "evidence_set_sha256"), "validation receipt." + name)
            for key, value in child.items():
                _digest(value, "validation receipt." + name + "." + key)
        if children != _verified_validation_children(root, frozen):
            raise PatternLabDataError("validation receipt: child completion/evidence-set digest mismatch.")
        return dict(receipt)
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise PatternLabDataError(f"{root}: malformed validation receipt/evidence: {error}") from error


def run_validation(*, candidate, data_root, output_root, workers=1):
    """Run the frozen recipe. Runtime arguments are locations and worker count only."""
    from . import PatternLabPendingError
    from .study import runner, context
    from .analysis import runner as analysis_runner
    frozen = load_candidate(candidate)
    verify_current_generation(frozen)
    runner.normalize_workers(workers)
    document = copy.deepcopy(frozen["recipe"]["study"])
    document.update(study=frozen["split"]["evaluation"], execution={"kind":"validation", "candidate":frozen})
    normalized = validation.validated_request(document)
    if _resolved_analysis(frozen["recipe"]) != frozen["recipe"]["analysis_family"]:
        raise PatternLabDataError("candidate analysis family changed under current generation.")
    pack = Path(data_root).expanduser().resolve()
    # Metadata before parent creation; the ordinary runner repeats under its
    # own pinned session, guarding changes between admission and execution.
    with data.read_session(pack) as session:
        report = session.inspect(verify=False)
        pending = update_transaction.pending_state(pack)
        if pending is not None and pending["valid"]:
            raise PatternLabPendingError(update_transaction.pending_problem(pending))
        if report.get("state") != "ready":
            raise PatternLabDataError("validation pack is not ready.")
        entries = runner.resolve_selection(report, normalized)
        context_entries = context.resolve_entries(report, normalized)
        union = {e["instrument_id"]:e for e in entries+context_entries}
        failures = runner.metadata_admission_failures(list(union.values()), normalized)
        if failures:
            raise PatternLabDataError("validation metadata: " + "; ".join(failures))
        verify_admitted_contracts(frozen, normalized, entries, context_entries)
        integrity = session.inspect(verify=True)["verification_check"]
        if not integrity["ok"]:
            raise PatternLabDataError("validation integrity: " + "; ".join(integrity["problems"]))
    root = Path(output_root).expanduser().resolve()
    protected = [pack]
    if isinstance(candidate, (str, Path)):
        protected.append(Path(candidate).expanduser().resolve())
    protected.extend(Path(frozen["provenance"][key]).resolve() for key in ("study_root", "analysis_root")
                     if key in frozen["provenance"])
    if root.exists() or any(root == p or root in p.parents or p in root.parents for p in protected):
        raise PatternLabDataError("validation output must be new and cannot overlap inputs.")
    root.mkdir(parents=True, exist_ok=False)
    phase = "freeze"
    def status(terminal, error=None):
        evidence.write_json(root/"status.json", {"schema_version":1, "candidate_id":frozen["candidate_id"],
            "terminal_status":terminal, "phase":phase, "error":None if error is None else str(error)})
    try:
        evidence.write_json(root/"candidate.json", frozen)
        status("running")
        phase = "study"
        runner.run_study(request=normalized, data_root=pack, output_root=root/"study", workers=workers)
        phase = "analysis"
        analyzed_request = {key:value for key,value in frozen["recipe"]["analysis_request"].items() if key != "method"}
        analysis_runner.run_analysis(request=analyzed_request, run_root=root/"study", output_root=root/"analysis")
        phase = "receipt"
        children = _verified_validation_children(root, frozen)
        verify_current_generation(frozen)
        if load_candidate(root/"candidate.json") != frozen:
            raise PatternLabDataError("validation candidate snapshot changed during execution.")
        receipt = {"schema_version":1, "status":"completed", **validation_metadata(frozen),
            "children": children,
            "completed_utc":artifacts.now_utc()}
        status("completed")
        evidence.write_json(root/"receipt.json", receipt)
        receipt = load_validation(root)
    except BaseException as error:
        try:
            committed = load_validation(root)
        except BaseException:
            committed = None
        if committed is not None:
            if not isinstance(error, Exception):
                raise  # Preserve completed evidence while propagating control flow.
            return {**committed, "output_root":str(root), "report":str(root/"analysis"/artifacts.REPORT_FILE)}
        try:
            status("interrupted" if isinstance(error, KeyboardInterrupt) else "failed", error)
        except BaseException:
            pass  # Status-write failure must not replace the computation cause.
        raise
    return {**receipt, "output_root":str(root), "report":str(root/"analysis"/artifacts.REPORT_FILE)}
