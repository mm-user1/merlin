"""Small explicit contracts and registries for Pattern Lab event studies.

Ordinary importable functions plus frozen descriptors are enough: no inheritance,
plugin manager or expression language is required.  A feature turns bars into
aligned numeric values and a validity mask, a hypothesis turns declared features
into a condition and its validity, a model resolves its own finite case list and
produces typed evidence, and a metric summarizes saved observations.

Every descriptor declares its identity, version, parameters, warmup requirement
and instrument scope, so unsupported panel scope and insufficient warmup fail
before any market data is touched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from .. import PatternLabDataError
from ..manifest import require_int, require_text

INSTRUMENT_SCOPE = "instrument"
SUPPORTED_SCOPES = (INSTRUMENT_SCOPE,)

# Reserved envelope fields of every model evidence table.  A custom model
# declares its own outcomes; it may not collide with these names.
RESERVED_EVIDENCE_FIELDS = (
    "instrument_id",
    "timeframe_minutes",
    "model_instance_id",
    "case_id",
    "anchor_open_ms",
    "signal_time_ms",
)

FIXED_HORIZON_EVIDENCE_KIND = "fixed_horizon_path_v1"
CUSTOM_CASE_EVIDENCE_KIND = "custom_case_outcomes_v1"
SEQUENTIAL_EVIDENCE_KIND = "sequential_bracket_v1"
EVIDENCE_KINDS = (FIXED_HORIZON_EVIDENCE_KIND, CUSTOM_CASE_EVIDENCE_KIND, SEQUENTIAL_EVIDENCE_KIND)


# --------------------------------------------------------------------------
# canonical identity helpers
# --------------------------------------------------------------------------

def canonical_json(payload: Any) -> str:
    """Return the compact canonical JSON used for every semantic digest."""
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def semantic_digest(payload: Any) -> str:
    """Return the SHA-256 of a payload's canonical JSON bytes."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def short_digest(payload: Any) -> str:
    """Return the first 32 hex characters of :func:`semantic_digest`."""
    return semantic_digest(payload)[:32]


def require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PatternLabDataError(f"{field_name}: expected an object, got {type(value).__name__}.")
    return dict(value)


def closed_keys(value: Mapping[str, Any], allowed: Sequence[str], field_name: str) -> None:
    """Reject unknown semantic keys, naming the field and the unexpected names."""
    extra = sorted(set(value) - set(allowed))
    if extra:
        raise PatternLabDataError(
            f"{field_name}: unknown keys {extra}; allowed keys are {sorted(allowed)}."
        )


def require_number(value: Any, field_name: str, *, minimum: float | None = None) -> float:
    """Return a finite float; booleans and not-a-number values are rejected."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PatternLabDataError(f"{field_name}: expected a finite number, got {type(value).__name__}.")
    number = float(value)
    if not np.isfinite(number):
        raise PatternLabDataError(f"{field_name}: expected a finite number, got {value!r}.")
    if minimum is not None and number < minimum:
        raise PatternLabDataError(f"{field_name}: must be >= {minimum}, got {number}.")
    return number


def require_identifier(value: Any, field_name: str) -> str:
    """Return a compact identifier usable in a file name, an ID and a digest."""
    text = require_text(value, field_name)
    if not all(character.isalnum() or character in "._-" for character in text):
        raise PatternLabDataError(
            f"{field_name}: {value!r} may contain only letters, digits, '.', '_' and '-'."
        )
    return text


def require_unique(values: Sequence[str], field_name: str) -> list[str]:
    seen: list[str] = []
    for index, item in enumerate(values):
        if item in seen:
            raise PatternLabDataError(f"{field_name}[{index}]: duplicate entry {item!r}.")
        seen.append(item)
    return seen


def require_scope(value: Any, field_name: str, *, context_feature: bool = False) -> str:
    text = require_text(value, field_name)
    if text not in (*SUPPORTED_SCOPES, *(("context",) if context_feature else ())):
        raise PatternLabDataError(
            f"{field_name}: scope {text!r} is not supported; this milestone executes only "
            f"{list(SUPPORTED_SCOPES)} jobs; only feature descriptors may declare context scope."
        )
    return text


# --------------------------------------------------------------------------
# aligned observation inputs and results
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class BarSeries:
    """One instrument's prepared observation bars at one timeframe.

    ``slots`` are UTC epoch grid indices of the bar opens, so a missing slot is
    an explicit gap rather than a silently compacted row.  ``research_start_index``
    is the first row at or after the study start; earlier rows are consumed
    warmup and never emit an event.
    """

    instrument_id: str
    timeframe_minutes: int
    step_ms: int
    timestamps_ms: np.ndarray
    slots: np.ndarray
    values: np.ndarray
    research_start_index: int

    @property
    def open(self) -> np.ndarray:
        return self.values[:, 0]

    @property
    def high(self) -> np.ndarray:
        return self.values[:, 1]

    @property
    def low(self) -> np.ndarray:
        return self.values[:, 2]

    @property
    def close(self) -> np.ndarray:
        return self.values[:, 3]

    @property
    def volume_quote(self) -> np.ndarray:
        return self.values[:, 4]

    @property
    def row_count(self) -> int:
        return int(self.timestamps_ms.size)

    def contiguous_with_previous(self) -> np.ndarray:
        """True where the previous row is the immediately preceding grid slot."""
        previous = np.zeros(self.row_count, dtype=bool)
        if self.row_count > 1:
            previous[1:] = np.diff(self.slots) == 1
        return previous


@dataclass(frozen=True)
class FeatureValue:
    """Aligned numeric feature values plus their validity mask."""

    values: np.ndarray
    valid: np.ndarray


@dataclass(frozen=True)
class ConditionValue:
    """Aligned boolean condition plus its validity mask; unknown is not false."""

    value: np.ndarray
    valid: np.ndarray


@dataclass(frozen=True)
class Anchors:
    """The eligible study anchors of one prepared series."""

    rows: np.ndarray
    open_ms: np.ndarray
    study_start_ms: int
    study_end_ms: int


def check_feature_result(result: Any, series: BarSeries, where: str) -> FeatureValue:
    """Validate a feature's aligned shapes, dtypes and finite valid values."""
    if not isinstance(result, FeatureValue):
        raise PatternLabDataError(f"{where}: expected a FeatureValue, got {type(result).__name__}.")
    values = np.asarray(result.values)
    valid = np.asarray(result.valid)
    if values.shape != (series.row_count,) or valid.shape != (series.row_count,):
        raise PatternLabDataError(
            f"{where}: expected aligned ({series.row_count},) arrays, got {values.shape} and {valid.shape}."
        )
    if values.dtype != np.float64:
        raise PatternLabDataError(f"{where}: values must be float64, got {values.dtype}.")
    if valid.dtype != np.bool_:
        raise PatternLabDataError(f"{where}: the validity mask must be boolean, got {valid.dtype}.")
    if valid.any() and not np.isfinite(values[valid]).all():
        raise PatternLabDataError(f"{where}: values marked valid must be finite.")
    return FeatureValue(values=values, valid=valid)


def check_condition_result(result: Any, series: BarSeries, where: str) -> ConditionValue:
    """Validate a hypothesis's aligned boolean condition and validity mask."""
    if not isinstance(result, ConditionValue):
        raise PatternLabDataError(f"{where}: expected a ConditionValue, got {type(result).__name__}.")
    value = np.asarray(result.value)
    valid = np.asarray(result.valid)
    if value.shape != (series.row_count,) or valid.shape != (series.row_count,):
        raise PatternLabDataError(
            f"{where}: expected aligned ({series.row_count},) arrays, got {value.shape} and {valid.shape}."
        )
    if value.dtype != np.bool_ or valid.dtype != np.bool_:
        raise PatternLabDataError(
            f"{where}: the condition and its validity mask must both be boolean, got "
            f"{value.dtype} and {valid.dtype}."
        )
    return ConditionValue(value=value, valid=valid)


# --------------------------------------------------------------------------
# descriptors
# --------------------------------------------------------------------------

def _no_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    closed_keys(parameters, (), "parameters")
    return {}


def _no_dependencies(parameters: Mapping[str, Any]) -> tuple["FeatureRequest", ...]:
    return ()


def _no_prior_bars(parameters: Mapping[str, Any]) -> int:
    return 0


@dataclass(frozen=True)
class FeatureRequest:
    """One resolved feature dependency: an ID plus its normalized parameters."""

    feature_id: str
    parameters: Mapping[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.feature_id}|{canonical_json(dict(self.parameters))}"


@dataclass(frozen=True)
class FeatureDescriptor:
    """A causal aligned feature over one instrument's observation bars."""

    feature_id: str
    version: str
    evaluate: Callable[[BarSeries, Mapping[str, Any], Mapping[str, FeatureValue]], FeatureValue]
    validate_parameters: Callable[[Mapping[str, Any]], dict[str, Any]] = _no_parameters
    dependencies: Callable[[Mapping[str, Any]], tuple[FeatureRequest, ...]] = _no_dependencies
    prior_bars: Callable[[Mapping[str, Any]], int] = _no_prior_bars
    scope: str = INSTRUMENT_SCOPE
    description: str = ""
    initialization: str = ""
    context_aliases: Callable[[Mapping[str, Any]], tuple[str, ...]] = lambda parameters: ()
    context_source_count: int | None = None


@dataclass(frozen=True)
class HypothesisDescriptor:
    """A causal condition over declared features and the observation bars."""

    hypothesis_id: str
    version: str
    evaluate: Callable[[BarSeries, Mapping[str, Any], Mapping[str, FeatureValue]], ConditionValue]
    validate_parameters: Callable[[Mapping[str, Any]], dict[str, Any]] = _no_parameters
    dependencies: Callable[[Mapping[str, Any]], tuple[FeatureRequest, ...]] = _no_dependencies
    prior_bars: Callable[[Mapping[str, Any]], int] = _no_prior_bars
    scope: str = INSTRUMENT_SCOPE
    description: str = ""


@dataclass(frozen=True)
class OutcomeSpec:
    """One declared model outcome: its stored column name, unit and meaning."""

    name: str
    unit: str
    description: str = ""


@dataclass(frozen=True)
class ModelCase:
    """One resolved case of a model instance at one observation timeframe.

    A model with no direction or horizon resolves exactly one axis-free case; it
    never invents dummy axes.  ``primary`` marks the emphasis declared before
    work, not a case selected from results.
    """

    case_id: str
    timeframe_minutes: int
    parameters: Mapping[str, Any]
    outcomes: tuple[OutcomeSpec, ...]
    primary: bool = False

    def as_json(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "timeframe_minutes": self.timeframe_minutes,
            "parameters": dict(self.parameters),
            "outcomes": [
                {"name": item.name, "unit": item.unit, "description": item.description}
                for item in self.outcomes
            ],
            "primary": self.primary,
        }


@dataclass(frozen=True)
class ModelEvidence:
    """One model instance's typed evidence rows for one instrument/timeframe."""

    kind: str
    rows: Mapping[str, np.ndarray]


@dataclass(frozen=True)
class ModelDescriptor:
    """A model owns its settings, its case axes and its evidence schema."""

    model_id: str
    version: str
    validate_settings: Callable[[Mapping[str, Any], Sequence[int]], dict[str, Any]]
    resolve_cases: Callable[[Mapping[str, Any], int], tuple[ModelCase, ...]]
    evaluate: Callable[[BarSeries, Mapping[str, Any], Anchors], ModelEvidence]
    evidence_kind: str = CUSTOM_CASE_EVIDENCE_KIND
    scope: str = INSTRUMENT_SCOPE
    description: str = ""


@dataclass(frozen=True)
class SequentialEvidence:
    tables: Mapping[str, Any]


@dataclass(frozen=True)
class SequentialModelDescriptor:
    """Event-dependent account evaluation; separate from per-anchor evidence."""
    model_id: str
    version: str
    validate_settings: Callable
    resolve_cases: Callable
    evaluate: Callable
    prior_bars: Callable
    evidence_kind: str = SEQUENTIAL_EVIDENCE_KIND
    scope: str = INSTRUMENT_SCOPE
    description: str = ""


def is_sequential(instance):
    """Check the complete reserved saved contract, without loading registrations."""
    triple = (instance.get("model_id"), instance.get("model_version"), instance.get("evidence_kind"))
    reserved = triple[2] == SEQUENTIAL_EVIDENCE_KIND
    if reserved and triple != ("atr_bracket", "1", SEQUENTIAL_EVIDENCE_KIND):
        raise PatternLabDataError("Unsupported sequential model/version/evidence-kind triple")
    return reserved


@dataclass(frozen=True)
class MetricDescriptor:
    """A summary metric over saved observations of one declared outcome group."""

    metric_id: str
    version: str
    required_columns: tuple[str, ...]
    unit: str
    compute: Callable[[Any], float | None]
    description: str = ""


# --------------------------------------------------------------------------
# the small explicit registry
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Registration:
    """One registered descriptor plus the source identity that produced it."""

    kind: str
    identifier: str
    descriptor: Any
    builtin: bool
    source_digest: str | None = None
    source_path: str | None = None


_REGISTRY: dict[str, dict[str, Registration]] = {
    "feature": {},
    "hypothesis": {},
    "model": {},
    "metric": {},
}


def _register(kind: str, identifier: str, descriptor: Any, *, builtin: bool,
              source_digest: str | None, source_path: str | None) -> Registration:
    name = require_identifier(identifier, f"{kind}_id")
    existing = _REGISTRY[kind].get(name)
    registration = Registration(
        kind=kind,
        identifier=name,
        descriptor=descriptor,
        builtin=builtin,
        source_digest=source_digest,
        source_path=source_path,
    )
    if existing is not None:
        if existing.builtin or existing.source_digest != source_digest:
            raise PatternLabDataError(
                f"{kind} {name!r} is already registered"
                + (" as a built-in" if existing.builtin else f" from {existing.source_path}")
                + "; a duplicate registration never silently replaces it."
            )
        return existing
    _REGISTRY[kind][name] = registration
    return registration


def register_feature(descriptor: FeatureDescriptor, *, builtin: bool = False,
                     source_digest: str | None = None, source_path: str | None = None) -> Registration:
    require_scope(descriptor.scope, f"feature {descriptor.feature_id}.scope", context_feature=True)
    if not callable(descriptor.context_aliases):
        raise PatternLabDataError(f"feature {descriptor.feature_id}: context_aliases must be callable.")
    if descriptor.context_source_count is not None:
        require_int(descriptor.context_source_count, f"feature {descriptor.feature_id}.context_source_count", minimum=1)
    return _register("feature", descriptor.feature_id, descriptor, builtin=builtin,
                     source_digest=source_digest, source_path=source_path)


def register_hypothesis(descriptor: HypothesisDescriptor, *, builtin: bool = False,
                        source_digest: str | None = None, source_path: str | None = None) -> Registration:
    require_scope(descriptor.scope, f"hypothesis {descriptor.hypothesis_id}.scope")
    return _register("hypothesis", descriptor.hypothesis_id, descriptor, builtin=builtin,
                     source_digest=source_digest, source_path=source_path)


def register_model(descriptor: ModelDescriptor, *, builtin: bool = False,
                   source_digest: str | None = None, source_path: str | None = None) -> Registration:
    if descriptor.model_id == "atr_bracket":
        from .bracket import DESCRIPTOR
        if descriptor is not DESCRIPTOR or not builtin:
            raise PatternLabDataError("atr_bracket requires the owned built-in descriptor; extensions cannot register it")
    sequential = is_sequential(dict(model_id=descriptor.model_id, model_version=descriptor.version,
                                    evidence_kind=descriptor.evidence_kind))
    if sequential != isinstance(descriptor, SequentialModelDescriptor) or (sequential and not builtin):
        raise PatternLabDataError("Sequential models require the built-in sequential descriptor; extensions cannot register them")
    if sequential:
        from .bracket import DESCRIPTOR
        if descriptor is not DESCRIPTOR:
            raise PatternLabDataError("Only the owned built-in atr_bracket descriptor may register sequential evidence")
    require_scope(descriptor.scope, f"model {descriptor.model_id}.scope")
    if descriptor.evidence_kind not in EVIDENCE_KINDS:
        raise PatternLabDataError(
            f"model {descriptor.model_id}: unknown evidence kind {descriptor.evidence_kind!r}; "
            f"supported kinds are {list(EVIDENCE_KINDS)}."
        )
    return _register("model", descriptor.model_id, descriptor, builtin=builtin,
                     source_digest=source_digest, source_path=source_path)


def register_metric(descriptor: MetricDescriptor, *, builtin: bool = False,
                    source_digest: str | None = None, source_path: str | None = None) -> Registration:
    return _register("metric", descriptor.metric_id, descriptor, builtin=builtin,
                     source_digest=source_digest, source_path=source_path)


def registration(kind: str, identifier: str) -> Registration:
    """Return one registration, naming the available identifiers when absent."""
    try:
        return _REGISTRY[kind][identifier]
    except KeyError:
        available = ", ".join(sorted(_REGISTRY[kind])) or "none"
        raise PatternLabDataError(
            f"unknown {kind} {identifier!r}; registered {kind}s: {available}. Declare its trusted "
            "extension module in the study request before using it."
        ) from None


def feature(identifier: str) -> FeatureDescriptor:
    return registration("feature", identifier).descriptor


def hypothesis(identifier: str) -> HypothesisDescriptor:
    return registration("hypothesis", identifier).descriptor


def model(identifier: str) -> ModelDescriptor:
    return registration("model", identifier).descriptor


def metric(identifier: str) -> MetricDescriptor:
    return registration("metric", identifier).descriptor


def registered(kind: str) -> list[str]:
    return sorted(_REGISTRY[kind])


def registrations(kind: str) -> dict[str, Registration]:
    return dict(_REGISTRY[kind])


# --------------------------------------------------------------------------
# the transitive feature closure
# --------------------------------------------------------------------------

def resolve_feature_closure(
    requests: Sequence[FeatureRequest], *, where: str
) -> tuple[FeatureRequest, ...]:
    """Return the transitive feature dependencies with normalized parameters.

    Registration, instrument scope, declared parameters and dependency cycles
    are all checked here, so warmup resolution and used-source attribution share
    one traversal instead of each walking the graph their own way.
    """
    resolved: dict[str, FeatureRequest] = {}

    def visit(request: FeatureRequest, chain: tuple[str, ...]) -> None:
        if request.feature_id in chain:
            raise PatternLabDataError(
                f"{where}: feature {request.feature_id!r} depends on itself through "
                f"{list(chain + (request.feature_id,))}; a dependency cycle is rejected before "
                "any market data is read."
            )
        descriptor = feature(request.feature_id)
        require_scope(descriptor.scope, f"{where} feature {request.feature_id}.scope", context_feature=True)
        if chain and feature(chain[-1]).scope == "context" and descriptor.scope != "context":
            raise PatternLabDataError(f"{where}: context feature cannot depend on instrument feature {request.feature_id!r}.")
        parameters = descriptor.validate_parameters(
            require_mapping(request.parameters, f"{where} feature {request.feature_id}.parameters")
        )
        normalized = FeatureRequest(feature_id=request.feature_id, parameters=parameters)
        if normalized.key in resolved:
            return
        resolved[normalized.key] = normalized
        for dependency in descriptor.dependencies(parameters):
            if not isinstance(dependency, FeatureRequest):
                raise PatternLabDataError(
                    f"{where}: feature {request.feature_id!r} declared a dependency of type "
                    f"{type(dependency).__name__}; dependencies() must return FeatureRequest values."
                )
            visit(dependency, chain + (request.feature_id,))

    for item in requests:
        if not isinstance(item, FeatureRequest):
            raise PatternLabDataError(
                f"{where}: declared dependencies must be FeatureRequest values, got "
                f"{type(item).__name__}."
            )
        visit(item, ())
    return tuple(resolved.values())


def resolved_prior_bars(
    declared: int, closure: Sequence[FeatureRequest], *, where: str
) -> int:
    """Return the effective total prior observation bars one condition needs.

    ``prior_bars`` declarations are *totals*, so the effective requirement is the
    maximum of the hypothesis's own declaration and every transitive dependency's
    declaration.  Totals are never added, which would count a shared lookback
    twice.
    """
    total = require_int(declared, f"{where} required_prior_bars", minimum=0)
    for request in closure:
        descriptor = feature(request.feature_id)
        total = max(
            total,
            require_int(
                descriptor.prior_bars(request.parameters),
                f"{where} feature {request.feature_id}.prior_bars",
                minimum=0,
            ),
        )
    return total


def require_verified_registrations(declared: Mapping[str, str]) -> None:
    """Fail when a non-built-in registration's recorded digest is not declared.

    Only registrations whose recorded source digest matches a currently declared
    and verified module are reused.  Custom code that happens to be preloaded in
    this interpreter is never silently accepted as verified.
    """
    for kind, entries in _REGISTRY.items():
        for name, entry in entries.items():
            if entry.builtin:
                continue
            expected = declared.get(entry.source_path or "")
            if entry.source_digest is None or expected is None:
                continue
            if expected != entry.source_digest:
                raise PatternLabDataError(
                    f"{kind} {name!r} was registered from {entry.source_path} with digest "
                    f"{entry.source_digest}, but that file now hashes to {expected}. Start a fresh "
                    "interpreter so the registration matches the source generation it came from.",
                    error_code="source_changed",
                )
