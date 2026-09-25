"""Versioned study request, protocol document and frozen planned family.

One Python entry point normalizes the request for both the CLI and agent
scripts.  Everything that changes what is computed is semantic; the data root,
the output root and the worker count are execution arguments recorded as
provenance only. Saved extension declarations retain physical roots; identity
policy 2 projects those top-level roots out without altering nested candidates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .. import PatternLabDataError
from ..manifest import (
    BASE_TIMEFRAME_MINUTES,
    format_epoch_ms,
    normalize_timeframe_minutes,
    read_json_file,
    require_aligned,
    require_int,
    require_text,
    to_epoch_ms,
)
from . import builtins as study_builtins
from . import contracts
from . import extensions as study_extensions
from .contracts import ModelCase

study_builtins.register_builtins()

REQUEST_SCHEMA_VERSION = 1
CURRENT_REQUEST_SCHEMA_VERSION = 2
SUPPORTED_REQUEST_VERSIONS = (1, 2)
PROTOCOL_SCHEMA_VERSION = 1

OCCURRENCE_POLICIES = ("every_qualifying_bar", "state_entry")
SELECTABLE_ROLES = ("trading", "research_only")

REQUEST_KEYS = (
    "schema_version",
    "study_name",
    "notes",
    "protocol",
    "study",
    "instruments",
    "timeframes_minutes",
    "hypotheses",
    "models",
    "metrics",
    "extensions",
)
PROTOCOL_KEYS = (
    "schema_version",
    "protocol_id",
    "development",
    "reserved",
    "earliest_warmup_start_utc",
    "notes",
)


# --------------------------------------------------------------------------
# protocol
# --------------------------------------------------------------------------

def _interval(raw: Any, where: str) -> dict[str, Any]:
    values = contracts.require_mapping(raw, where)
    contracts.closed_keys(values, ("start_utc", "end_utc"), where)
    for key in ("start_utc", "end_utc"):
        if key not in values:
            raise PatternLabDataError(f"{where}.{key}: an explicit UTC boundary is required.")
    start = to_epoch_ms(values["start_utc"], f"{where}.start_utc")
    end = to_epoch_ms(values["end_utc"], f"{where}.end_utc")
    if start >= end:
        raise PatternLabDataError(
            f"{where}: requires start < end, got {format_epoch_ms(start)} >= {format_epoch_ms(end)}."
        )
    return {"start_utc": format_epoch_ms(start), "end_utc": format_epoch_ms(end),
            "start_ms": start, "end_ms": end}


def normalize_protocol(document: Any, *, source: str) -> dict[str, Any]:
    """Validate a protocol document and return its canonical semantic copy."""
    values = contracts.require_mapping(document, source)
    contracts.closed_keys(values, PROTOCOL_KEYS, source)
    version = require_int(values.get("schema_version"), f"{source}.schema_version")
    if version != PROTOCOL_SCHEMA_VERSION:
        raise PatternLabDataError(
            f"{source}.schema_version: unsupported protocol version {version}; this build reads "
            f"{PROTOCOL_SCHEMA_VERSION}."
        )
    protocol_id = contracts.require_identifier(values.get("protocol_id"), f"{source}.protocol_id")
    development = _interval(values.get("development"), f"{source}.development")
    reserved = _interval(values.get("reserved"), f"{source}.reserved")
    warmup_floor = to_epoch_ms(
        values.get("earliest_warmup_start_utc"), f"{source}.earliest_warmup_start_utc"
    )
    if warmup_floor > development["start_ms"]:
        raise PatternLabDataError(
            f"{source}.earliest_warmup_start_utc: the earliest permitted warmup "
            f"{format_epoch_ms(warmup_floor)} is after the development start "
            f"{development['start_utc']}."
        )
    notes = values.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise PatternLabDataError(f"{source}.notes: expected a string or null.")
    return {
        "schema_version": version,
        "protocol_id": protocol_id,
        "development": {"start_utc": development["start_utc"], "end_utc": development["end_utc"]},
        "reserved": {"start_utc": reserved["start_utc"], "end_utc": reserved["end_utc"]},
        "earliest_warmup_start_utc": format_epoch_ms(warmup_floor),
        "notes": notes,
        "_bounds": {
            "development_start_ms": development["start_ms"],
            "development_end_ms": development["end_ms"],
            "reserved_start_ms": reserved["start_ms"],
            "reserved_end_ms": reserved["end_ms"],
            "earliest_warmup_ms": warmup_floor,
        },
    }


def protocol_document(protocol: Mapping[str, Any]) -> dict[str, Any]:
    """Return the protocol without its derived private bounds."""
    return {key: value for key, value in protocol.items() if not key.startswith("_")}


def validate_against_protocol(
    protocol: Mapping[str, Any], *, study_start_ms: int, study_end_ms: int, warmup_start_ms: int
) -> None:
    """Refuse a study or consumed warmup outside the declared development data."""
    bounds = protocol["_bounds"]
    if study_start_ms < bounds["development_start_ms"] or study_end_ms > bounds["development_end_ms"]:
        raise PatternLabDataError(
            f"study: [{format_epoch_ms(study_start_ms)}, {format_epoch_ms(study_end_ms)}) is not "
            f"inside the protocol's development interval [{protocol['development']['start_utc']}, "
            f"{protocol['development']['end_utc']}). This build runs development studies only; "
            "there is no holdout mode or bypass flag."
        )
    if warmup_start_ms < bounds["earliest_warmup_ms"]:
        raise PatternLabDataError(
            f"study.warmup_start_utc: {format_epoch_ms(warmup_start_ms)} is before the protocol's "
            f"earliest permitted warmup {protocol['earliest_warmup_start_utc']}."
        )
    reserved_start = bounds["reserved_start_ms"]
    reserved_end = bounds["reserved_end_ms"]
    if warmup_start_ms < reserved_end and study_end_ms > reserved_start:
        raise PatternLabDataError(
            f"study: the consumed interval [{format_epoch_ms(warmup_start_ms)}, "
            f"{format_epoch_ms(study_end_ms)}) intersects the protocol's reserved interval "
            f"[{protocol['reserved']['start_utc']}, {protocol['reserved']['end_utc']})."
        )


# --------------------------------------------------------------------------
# request
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class HypothesisVariant:
    """One explicit hypothesis variant: parameters plus its occurrence policy."""

    variant_id: str
    hypothesis_id: str
    version: str
    parameters: Mapping[str, Any]
    occurrence: str
    condition_id: str
    required_prior_bars: int

    def as_json(self) -> dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "hypothesis_id": self.hypothesis_id,
            "hypothesis_version": self.version,
            "parameters": dict(self.parameters),
            "occurrence": self.occurrence,
            "condition_id": self.condition_id,
            "required_prior_bars": self.required_prior_bars,
        }


@dataclass(frozen=True)
class ModelInstance:
    """One explicit model instance with its own settings and resolved cases."""

    model_instance_id: str
    model_id: str
    version: str
    evidence_kind: str
    settings: Mapping[str, Any]
    cases: Mapping[int, tuple[ModelCase, ...]]

    def as_json(self) -> dict[str, Any]:
        return {
            "model_instance_id": self.model_instance_id,
            "model_id": self.model_id,
            "model_version": self.version,
            "evidence_kind": self.evidence_kind,
            "settings": dict(self.settings),
            "cases": {
                str(timeframe): [case.as_json() for case in cases]
                for timeframe, cases in sorted(self.cases.items())
            },
        }


@dataclass(frozen=True)
class MetricDeclaration:
    """One declared summary metric over saved observations."""

    declaration_id: str
    metric_id: str
    version: str
    unit: str
    required_columns: tuple[str, ...]

    def as_json(self) -> dict[str, Any]:
        return {
            "declaration_id": self.declaration_id,
            "metric_id": self.metric_id,
            "metric_version": self.version,
            "unit": self.unit,
            "required_columns": list(self.required_columns),
        }


def _normalize_metrics(raw: Any) -> tuple[MetricDeclaration, ...]:
    if raw is None:
        return ()
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError("metrics: expected a list of explicit metric declarations.")
    declarations: list[MetricDeclaration] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        where = f"metrics[{index}]"
        values = contracts.require_mapping(item, where)
        contracts.closed_keys(values, ("id", "metric"), where)
        for key in ("id", "metric"):
            if key not in values:
                raise PatternLabDataError(f"{where}.{key}: an explicit value is required.")
        declaration_id = contracts.require_identifier(values["id"], f"{where}.id")
        if declaration_id in seen:
            raise PatternLabDataError(f"{where}.id: duplicate metric declaration {declaration_id!r}.")
        descriptor = contracts.metric(require_text(values["metric"], f"{where}.metric"))
        seen.add(declaration_id)
        declarations.append(
            MetricDeclaration(
                declaration_id=declaration_id,
                metric_id=descriptor.metric_id,
                version=descriptor.version,
                unit=descriptor.unit,
                required_columns=tuple(descriptor.required_columns),
            )
        )
    return tuple(declarations)


@dataclass(frozen=True)
class ExtensionDeclaration:
    """One trusted Python extension module and its declared local helpers."""

    module: str
    source_root: str
    helpers: tuple[str, ...]

    def as_json(self) -> dict[str, Any]:
        return {"module": self.module, "source_root": self.source_root, "helpers": list(self.helpers)}


@dataclass(frozen=True)
class StudyRequest:
    """The normalized semantic study request."""

    schema_version: int
    study_name: str
    notes: str | None
    protocol: Mapping[str, Any]
    protocol_source: str
    study_start_ms: int
    study_end_ms: int
    warmup_start_ms: int
    timeframes: tuple[int, ...]
    role_selection: tuple[str, ...] | None
    instrument_selection: tuple[str, ...] | None
    variants: tuple[HypothesisVariant, ...]
    models: tuple[ModelInstance, ...]
    metrics: tuple[MetricDeclaration, ...]
    extensions: tuple[ExtensionDeclaration, ...]
    context: Mapping[str, Any] = field(default_factory=dict)
    execution: Mapping[str, Any] = field(default_factory=lambda: {"kind": "development"})

    def semantic_document(self) -> dict[str, Any]:
        """The canonical semantic payload; free-form notes stay outside it."""
        return {
            "schema_version": self.schema_version,
            **({"context": dict(self.context), "execution": dict(self.execution)}
               if self.schema_version == 2 else {}),
            "study": {
                "start_utc": format_epoch_ms(self.study_start_ms),
                "end_utc": format_epoch_ms(self.study_end_ms),
                "warmup_start_utc": format_epoch_ms(self.warmup_start_ms),
            },
            "instruments": (
                {"roles": list(self.role_selection)}
                if self.role_selection is not None
                else {"ids": list(self.instrument_selection or ())}
            ),
            "timeframes_minutes": list(self.timeframes),
            "hypotheses": [variant.as_json() for variant in self.variants],
            "models": [instance.as_json() for instance in self.models],
            "metrics": [item.as_json() for item in self.metrics],
            "protocol": protocol_document(self.protocol),
            "extensions": [item.as_json() for item in self.extensions],
        }

    def request_document(self) -> dict[str, Any]:
        document = self.semantic_document()
        document["study_name"] = self.study_name
        document["notes"] = self.notes
        document["protocol_source"] = self.protocol_source
        return document


def _resolve_relative(value: str, *, base: Path | None, field_name: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    if base is None:
        raise PatternLabDataError(
            f"{field_name}: {value!r} is relative, but this request was supplied inline, so there "
            "is no declaring file to resolve it against. Use an absolute path."
        )
    return (base / candidate).resolve()


def _normalize_timeframes(raw: Any) -> tuple[int, ...]:
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError(
            f"timeframes_minutes: expected a list of minutes, got {type(raw).__name__}."
        )
    if not raw:
        raise PatternLabDataError("timeframes_minutes: at least one observation timeframe is required.")
    minutes: list[int] = []
    for index, item in enumerate(raw):
        value = normalize_timeframe_minutes(item, f"timeframes_minutes[{index}]")
        if value in minutes:
            raise PatternLabDataError(f"timeframes_minutes[{index}]: duplicate timeframe {value}.")
        minutes.append(value)
    return tuple(sorted(minutes))


def _normalize_selection(raw: Any) -> tuple[tuple[str, ...] | None, tuple[str, ...] | None]:
    values = contracts.require_mapping(raw, "instruments")
    contracts.closed_keys(values, ("roles", "ids"), "instruments")
    present = [key for key in ("roles", "ids") if key in values]
    if len(present) != 1:
        raise PatternLabDataError(
            "instruments: declare exactly one of 'roles' or 'ids'; the selection is never inferred."
        )
    if present[0] == "roles":
        raw_roles = values["roles"]
        if isinstance(raw_roles, (str, bytes)) or not isinstance(raw_roles, (list, tuple)) or not raw_roles:
            raise PatternLabDataError("instruments.roles: expected a nonempty list of roles.")
        roles: list[str] = []
        for index, item in enumerate(raw_roles):
            text = require_text(item, f"instruments.roles[{index}]")
            if text not in SELECTABLE_ROLES:
                raise PatternLabDataError(
                    f"instruments.roles[{index}]: {text!r} cannot be selected as a study target; "
                    f"selectable roles are {list(SELECTABLE_ROLES)}. A factor-only series is context, "
                    "not a standalone target."
                )
            if text in roles:
                raise PatternLabDataError(f"instruments.roles[{index}]: duplicate role {text!r}.")
            roles.append(text)
        return tuple(sorted(roles)), None
    raw_ids = values["ids"]
    if isinstance(raw_ids, (str, bytes)) or not isinstance(raw_ids, (list, tuple)) or not raw_ids:
        raise PatternLabDataError("instruments.ids: expected a nonempty list of instrument IDs.")
    identifiers: list[str] = []
    for index, item in enumerate(raw_ids):
        text = require_text(item, f"instruments.ids[{index}]")
        if text in identifiers:
            raise PatternLabDataError(f"instruments.ids[{index}]: duplicate instrument {text!r}.")
        identifiers.append(text)
    return None, tuple(identifiers)


def _normalize_variants(raw: Any) -> tuple[HypothesisVariant, ...]:
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)) or not raw:
        raise PatternLabDataError("hypotheses: expected a nonempty list of explicit variants.")
    variants: list[HypothesisVariant] = []
    seen_ids: set[str] = set()
    seen_semantics: dict[str, str] = {}
    for index, item in enumerate(raw):
        where = f"hypotheses[{index}]"
        values = contracts.require_mapping(item, where)
        contracts.closed_keys(values, ("id", "hypothesis", "parameters", "occurrence"), where)
        for key in ("id", "hypothesis", "occurrence"):
            if key not in values:
                raise PatternLabDataError(f"{where}.{key}: an explicit value is required.")
        variant_id = contracts.require_identifier(values["id"], f"{where}.id")
        if variant_id in seen_ids:
            raise PatternLabDataError(f"{where}.id: duplicate variant ID {variant_id!r}.")
        hypothesis_id = require_text(values["hypothesis"], f"{where}.hypothesis")
        occurrence = require_text(values["occurrence"], f"{where}.occurrence")
        if occurrence not in OCCURRENCE_POLICIES:
            raise PatternLabDataError(
                f"{where}.occurrence: unknown policy {occurrence!r}; supported policies are "
                f"{list(OCCURRENCE_POLICIES)}."
            )
        descriptor = contracts.hypothesis(hypothesis_id)
        contracts.require_scope(descriptor.scope, f"{where}.hypothesis scope")
        parameters = descriptor.validate_parameters(
            contracts.require_mapping(values.get("parameters", {}), f"{where}.parameters")
        )
        dependencies = tuple(descriptor.dependencies(parameters))
        # The transitive closure checks registration, scope, parameters and
        # cycles, and supplies the dependency warmup totals resolved below.
        closure = contracts.resolve_feature_closure(dependencies, where=where)
        condition_id = contracts.short_digest(
            {
                "hypothesis_id": hypothesis_id,
                "hypothesis_version": descriptor.version,
                "parameters": dict(parameters),
                "dependencies": [
                    {"feature_id": dependency.feature_id, "parameters": dict(dependency.parameters)}
                    for dependency in dependencies
                ],
            }
        )
        semantic = f"{condition_id}|{occurrence}"
        if semantic in seen_semantics:
            raise PatternLabDataError(
                f"{where}: this variant is semantically identical to {seen_semantics[semantic]!r}; "
                "duplicate semantic variants are rejected instead of counted twice."
            )
        prior_bars = contracts.resolved_prior_bars(
            descriptor.prior_bars(parameters), closure, where=where
        )
        seen_ids.add(variant_id)
        seen_semantics[semantic] = variant_id
        variants.append(
            HypothesisVariant(
                variant_id=variant_id,
                hypothesis_id=hypothesis_id,
                version=descriptor.version,
                parameters=parameters,
                occurrence=occurrence,
                condition_id=condition_id,
                required_prior_bars=prior_bars,
            )
        )
    return tuple(variants)


def _normalize_models(raw: Any, timeframes: Sequence[int]) -> tuple[ModelInstance, ...]:
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)) or not raw:
        raise PatternLabDataError("models: expected a nonempty list of explicit model instances.")
    instances: list[ModelInstance] = []
    seen_ids: set[str] = set()
    seen_semantics: dict[str, str] = {}
    for index, item in enumerate(raw):
        where = f"models[{index}]"
        values = contracts.require_mapping(item, where)
        contracts.closed_keys(values, ("id", "model", "settings"), where)
        for key in ("id", "model"):
            if key not in values:
                raise PatternLabDataError(f"{where}.{key}: an explicit value is required.")
        instance_id = contracts.require_identifier(values["id"], f"{where}.id")
        if instance_id in seen_ids:
            raise PatternLabDataError(f"{where}.id: duplicate model instance ID {instance_id!r}.")
        model_id = require_text(values["model"], f"{where}.model")
        descriptor = contracts.model(model_id)
        contracts.require_scope(descriptor.scope, f"{where}.model scope")
        settings = descriptor.validate_settings(
            contracts.require_mapping(values.get("settings", {}), f"{where}.settings"), list(timeframes)
        )
        cases: dict[int, tuple[ModelCase, ...]] = {}
        for timeframe in timeframes:
            resolved = tuple(descriptor.resolve_cases(settings, timeframe))
            if not resolved:
                raise PatternLabDataError(
                    f"{where}: model {model_id!r} resolved no case for the selected {timeframe}m "
                    "timeframe; an unsupported timeframe/model combination must fail before execution."
                )
            case_ids: list[str] = []
            for case in resolved:
                if not isinstance(case, ModelCase):
                    raise PatternLabDataError(f"{where}: resolve_cases must return ModelCase objects.")
                contracts.require_identifier(case.case_id, f"{where}.cases.case_id")
                if case.timeframe_minutes != timeframe:
                    raise PatternLabDataError(
                        f"{where}: case {case.case_id!r} declares timeframe "
                        f"{case.timeframe_minutes}, not the requested {timeframe}."
                    )
                if not case.outcomes and not isinstance(descriptor, contracts.SequentialModelDescriptor):
                    raise PatternLabDataError(f"{where}: case {case.case_id!r} declares no outcome.")
                outcome_names: list[str] = []
                for outcome in case.outcomes:
                    name = contracts.require_identifier(outcome.name, f"{where}.cases.outcomes.name")
                    require_text(outcome.unit, f"{where}.cases.outcomes.unit")
                    if name in contracts.RESERVED_EVIDENCE_FIELDS:
                        raise PatternLabDataError(
                            f"{where}: outcome {name!r} collides with the reserved evidence field of "
                            f"the same name; reserved fields are {list(contracts.RESERVED_EVIDENCE_FIELDS)}."
                        )
                    if name in outcome_names:
                        raise PatternLabDataError(f"{where}: duplicate outcome {name!r} in case {case.case_id!r}.")
                    outcome_names.append(name)
                if case.case_id in case_ids:
                    raise PatternLabDataError(f"{where}: duplicate case ID {case.case_id!r}.")
                case_ids.append(case.case_id)
            cases[timeframe] = resolved
        semantic = contracts.canonical_json({"model_id": model_id, "settings": settings})
        if semantic in seen_semantics:
            raise PatternLabDataError(
                f"{where}: identical semantic settings to model instance {seen_semantics[semantic]!r}; "
                "duplicate semantic variants are rejected instead of counted twice."
            )
        seen_ids.add(instance_id)
        seen_semantics[semantic] = instance_id
        instances.append(
            ModelInstance(
                model_instance_id=instance_id,
                model_id=model_id,
                version=descriptor.version,
                evidence_kind=descriptor.evidence_kind,
                settings=settings,
                cases=cases,
            )
        )
    return tuple(instances)


def _normalize_extensions(raw: Any, *, base: Path | None) -> tuple[ExtensionDeclaration, ...]:
    if raw is None:
        return ()
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError("extensions: expected a list of trusted module declarations.")
    declarations: list[ExtensionDeclaration] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        where = f"extensions[{index}]"
        values = contracts.require_mapping(item, where)
        contracts.closed_keys(values, ("module", "source_root", "helpers"), where)
        for key in ("module", "source_root"):
            if key not in values:
                raise PatternLabDataError(f"{where}.{key}: an explicit value is required.")
        module = require_text(values["module"], f"{where}.module")
        if not module.isidentifier():
            raise PatternLabDataError(
                f"{where}.module: {module!r} is not a plain module name. Declare one importable "
                "top-level module per entry; there is no directory-wide discovery."
            )
        if module in seen:
            raise PatternLabDataError(f"{where}.module: duplicate module {module!r}.")
        root = _resolve_relative(
            require_text(values["source_root"], f"{where}.source_root"), base=base,
            field_name=f"{where}.source_root",
        )
        helpers_raw = values.get("helpers", [])
        if isinstance(helpers_raw, (str, bytes)) or not isinstance(helpers_raw, (list, tuple)):
            raise PatternLabDataError(f"{where}.helpers: expected a list of relative file names.")
        helpers: list[str] = []
        for helper_index, helper in enumerate(helpers_raw):
            text = require_text(helper, f"{where}.helpers[{helper_index}]")
            candidate = Path(text)
            if candidate.is_absolute() or ".." in candidate.parts:
                raise PatternLabDataError(
                    f"{where}.helpers[{helper_index}]: {text!r} must be a relative path inside the "
                    "declared source root."
                )
            if text in helpers:
                raise PatternLabDataError(f"{where}.helpers[{helper_index}]: duplicate helper {text!r}.")
            helpers.append(text)
        seen.add(module)
        declarations.append(
            ExtensionDeclaration(module=module, source_root=str(root), helpers=tuple(sorted(helpers)))
        )
    return tuple(declarations)


def normalize_request(document: Any, *, source: str, base: Path | None) -> StudyRequest:
    """Validate a study request and return its normalized semantic form.

    ``base`` is the directory of the declaring file: relative protocol and
    extension paths resolve against it, never against the process's cwd.
    """
    values = contracts.require_mapping(document, source)
    version = require_int(values.get("schema_version"), f"{source}.schema_version")
    if version not in SUPPORTED_REQUEST_VERSIONS:
        raise PatternLabDataError(
            f"{source}.schema_version: unsupported request version {version}; this build reads "
            f"{SUPPORTED_REQUEST_VERSIONS}."
        )
    contracts.closed_keys(values, (*REQUEST_KEYS, *(("context", "execution") if version == 2 else ())), source)
    from . import context as study_context
    context = study_context.normalize_aliases(values.get("context")) if version == 2 else {}
    execution = contracts.require_mapping(values.get("execution"), "execution") if version == 2 else {"kind": "development"}
    if execution.get("kind") not in ("development", "validation"):
        raise PatternLabDataError(f"execution.kind: unsupported {execution.get('kind')!r}; supported: development, validation.")
    if execution != {"kind": "development"}:
        # Candidate-bound validation is checked by the shared split policy below.
        contracts.closed_keys(execution, ("kind", "candidate"), "execution")
        if execution.get("kind") != "validation" or not isinstance(execution.get("candidate"), dict):
            raise PatternLabDataError("execution: validation requires a verified embedded candidate snapshot.")
    study_name = require_text(values.get("study_name"), f"{source}.study_name")
    notes = values.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise PatternLabDataError(f"{source}.notes: expected a string or null.")

    raw_protocol = values.get("protocol")
    if isinstance(raw_protocol, str):
        protocol_path = _resolve_relative(raw_protocol, base=base, field_name=f"{source}.protocol")
        protocol = normalize_protocol(read_json_file(protocol_path), source=str(protocol_path))
        protocol_source = str(protocol_path)
    elif isinstance(raw_protocol, Mapping):
        protocol = normalize_protocol(raw_protocol, source=f"{source}.protocol")
        protocol_source = "inline"
    else:
        raise PatternLabDataError(
            f"{source}.protocol: expected a path to a protocol document or an inline protocol object."
        )

    timeframes = _normalize_timeframes(values.get("timeframes_minutes"))
    study = contracts.require_mapping(values.get("study"), f"{source}.study")
    contracts.closed_keys(study, ("start_utc", "end_utc", "warmup_start_utc"), f"{source}.study")
    for key in ("start_utc", "end_utc", "warmup_start_utc"):
        if key not in study:
            raise PatternLabDataError(f"{source}.study.{key}: an explicit UTC boundary is required.")
    start_ms = to_epoch_ms(study["start_utc"], f"{source}.study.start_utc")
    end_ms = to_epoch_ms(study["end_utc"], f"{source}.study.end_utc")
    warmup_ms = to_epoch_ms(study["warmup_start_utc"], f"{source}.study.warmup_start_utc")
    if not warmup_ms <= start_ms < end_ms:
        raise PatternLabDataError(
            f"{source}.study: requires warmup_start_utc <= start_utc < end_utc, got "
            f"{format_epoch_ms(warmup_ms)} / {format_epoch_ms(start_ms)} / {format_epoch_ms(end_ms)}."
        )
    for timeframe in timeframes:
        step_ms = timeframe * 60_000
        require_aligned(warmup_ms, step_ms, f"{source}.study.warmup_start_utc")
        require_aligned(start_ms, step_ms, f"{source}.study.start_utc")
        require_aligned(end_ms, step_ms, f"{source}.study.end_utc")

    if execution["kind"] == "development":
        validate_against_protocol(
            protocol, study_start_ms=start_ms, study_end_ms=end_ms, warmup_start_ms=warmup_ms
        )

    extensions = _normalize_extensions(values.get("extensions"), base=base)
    if execution["kind"] == "validation":
        from ..candidate import load_candidate, verify_extension_generation
        frozen = load_candidate(execution["candidate"])
        verify_extension_generation(frozen, study_extensions.declared_source_records(extensions))
    # Trusted modules are hashed, imported and registered before any descriptor
    # is looked up, so a declared custom hypothesis or model exists by name.
    study_extensions.load_extensions(extensions)
    role_selection, instrument_selection = _normalize_selection(values.get("instruments"))
    variants = _normalize_variants(values.get("hypotheses"))
    models = _normalize_models(values.get("models"), timeframes)
    metrics = _normalize_metrics(values.get("metrics"))
    sequential = [contracts.is_sequential(model.as_json()) for model in models]
    if any(sequential) and values.get("schema_version", 1) != 2:
        raise PatternLabDataError("atr_bracket requires study request version 2")
    if metrics and all(sequential):
        raise PatternLabDataError("Observation metrics require at least one per-anchor model")

    request = StudyRequest(
        schema_version=version,
        context=context,
        execution=execution,
        study_name=study_name,
        notes=notes,
        protocol=protocol,
        protocol_source=protocol_source,
        study_start_ms=start_ms,
        study_end_ms=end_ms,
        warmup_start_ms=warmup_ms,
        timeframes=timeframes,
        role_selection=role_selection,
        instrument_selection=instrument_selection,
        variants=variants,
        models=models,
        metrics=metrics,
        extensions=extensions,
    )
    study_context.dependencies(request)
    if execution["kind"] == "validation":
        from ..candidate import validate_execution
        validate_execution(request.semantic_document())
    check_warmup_sufficiency(request)
    return request


def warmup_requirements(request: StudyRequest) -> dict[str, Any]:
    """Return the declared and resolved warmup facts for every timeframe."""
    requirements: dict[str, Any] = {}
    for timeframe in request.timeframes:
        step_ms = timeframe * 60_000
        declared = (request.study_start_ms - request.warmup_start_ms) // step_ms
        needed = max((variant.required_prior_bars for variant in request.variants), default=0)
        by_model = {m.model_instance_id: contracts.model(m.model_id).prior_bars(m.settings)
                    for m in request.models if contracts.is_sequential(m.as_json())
                    and contracts.model(m.model_id).prior_bars(m.settings)}
        needed = max(needed, max(by_model.values(), default=0))
        requirements[str(timeframe)] = {
            "declared_warmup_bars": int(declared),
            "required_prior_bars": int(needed),
            **({"by_model": by_model} if by_model else {}),
            "by_variant": {
                variant.variant_id: variant.required_prior_bars for variant in request.variants
            },
        }
    return requirements


def check_warmup_sufficiency(request: StudyRequest) -> None:
    """Refuse an insufficient declared warmup instead of shortening a lookback."""
    for timeframe, facts in warmup_requirements(request).items():
        if facts["declared_warmup_bars"] < facts["required_prior_bars"]:
            raise PatternLabDataError(
                f"study.warmup_start_utc: the declared warmup supplies "
                f"{facts['declared_warmup_bars']} prior {timeframe}m observation bar(s), but the "
                f"requested {'hypotheses/models' if facts.get('by_model') else 'hypotheses'} need {facts['required_prior_bars']}. Declare more warmup; "
                "lookbacks are never shortened to fit."
            )


def load_request(path: Path) -> StudyRequest:
    """Read and normalize a study request file, resolving its relative paths."""
    path = Path(path).resolve()
    document = read_json_file(path)
    return normalize_request(document, source=str(path), base=path.parent)
