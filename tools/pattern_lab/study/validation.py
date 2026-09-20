"""One semantic validation path for every accepted public request form.

A request file, a request mapping and an already normalized
:class:`~tools.pattern_lab.study.spec.StudyRequest` must all reach execution
through the same validators.  A frozen dataclass proves nothing on its own:
``dataclasses.replace`` never revalidates, nested mappings stay mutable, and
derived facts such as condition identities, resolved cases and the protocol's
private bounds cache can end up contradicting the settings they came from.

The adapter here renders a normalized request back into the *external* request
schema and feeds it to ``spec.normalize_request``, so there is one set of
validators rather than a second competing schema.  The freshly resolved value
is then compared with the supplied one: a contradiction fails before any output
directory, market read or numerical work.

The same module resolves the used descriptor set — hypotheses, their transitive
feature dependencies, models and metrics — so every non-built-in descriptor a
study actually consumes must be attributable to a declared, verified source
generation.  An unrelated registry entry left behind by an earlier study is not
consulted and cannot block a built-in run.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from .. import PatternLabDataError
from ..manifest import format_epoch_ms
from . import contracts
from . import spec as study_spec
from .spec import StudyRequest


def external_document(request: StudyRequest) -> dict[str, Any]:
    """Render a normalized request in the external request schema.

    The protocol is rendered as its public document, without the private bounds
    cache, so normalization rebuilds those bounds from the dates themselves.
    Every nested mapping is copied: validation never mutates a caller's object.
    """
    if request.role_selection is not None:
        instruments: dict[str, Any] = {"roles": list(request.role_selection)}
    else:
        instruments = {"ids": list(request.instrument_selection or ())}
    return {
        "schema_version": request.schema_version,
        "study_name": request.study_name,
        "notes": request.notes,
        "protocol": copy.deepcopy(study_spec.protocol_document(request.protocol)),
        "study": {
            "start_utc": format_epoch_ms(request.study_start_ms),
            "end_utc": format_epoch_ms(request.study_end_ms),
            "warmup_start_utc": format_epoch_ms(request.warmup_start_ms),
        },
        "instruments": instruments,
        "timeframes_minutes": list(request.timeframes),
        "hypotheses": [
            {
                "id": variant.variant_id,
                "hypothesis": variant.hypothesis_id,
                "parameters": copy.deepcopy(dict(variant.parameters)),
                "occurrence": variant.occurrence,
            }
            for variant in request.variants
        ],
        "models": [
            {
                "id": instance.model_instance_id,
                "model": instance.model_id,
                "settings": copy.deepcopy(dict(instance.settings)),
            }
            for instance in request.models
        ],
        "metrics": [
            {"id": item.declaration_id, "metric": item.metric_id} for item in request.metrics
        ],
        "extensions": [
            {
                "module": item.module,
                "source_root": item.source_root,
                "helpers": list(item.helpers),
            }
            for item in request.extensions
        ],
    }


def _require_consistent(supplied: StudyRequest, fresh: StudyRequest) -> None:
    """Refuse a normalized object whose derived facts contradict its settings."""
    if supplied.semantic_document() != fresh.semantic_document():
        raise PatternLabDataError(
            "request: this normalized request carries derived facts that contradict a fresh "
            "resolution of its own settings. Condition identities, resolved model cases, declared "
            "metric contracts and the protocol document are recomputed at the execution boundary; "
            "build a new request instead of replacing fields on a validated one.",
            error_code="invalid_request",
        )
    supplied_bounds = supplied.protocol.get("_bounds")
    if supplied_bounds is not None and dict(supplied_bounds) != dict(fresh.protocol["_bounds"]):
        raise PatternLabDataError(
            "request.protocol: the private bounds cache does not match the protocol document it "
            "claims to summarize. Protocol bounds are rebuilt from the declared dates and are "
            "never trusted as supplied.",
            error_code="invalid_request",
        )


def validated_request(request: Any) -> StudyRequest:
    """Return a freshly validated request for any accepted public input form."""
    if isinstance(request, StudyRequest):
        fresh = study_spec.normalize_request(
            external_document(request), source="normalized request", base=None
        )
        _require_consistent(request, fresh)
        # The protocol was revalidated as an inline document so its bounds are
        # rebuilt from the dates themselves.  Where the supplied request came
        # from stays true afterwards: it is non-semantic provenance the
        # consistency check has already bound to this exact protocol document,
        # and carrying it never reopens or rereads that file.
        supplied_source = request.protocol_source
        if isinstance(supplied_source, str) and supplied_source.strip():
            fresh = replace(fresh, protocol_source=supplied_source)
        return fresh
    if isinstance(request, (str, Path)):
        return study_spec.load_request(Path(request))
    return study_spec.normalize_request(request, source="request", base=None)


# --------------------------------------------------------------------------
# the used descriptor set
# --------------------------------------------------------------------------

def used_registrations(request: StudyRequest) -> list[contracts.Registration]:
    """Resolve every descriptor this study actually consumes.

    Hypotheses, their transitive feature dependencies, models and declared
    metrics are all included; nothing else in the registry is inspected.
    """
    used: dict[tuple[str, str], contracts.Registration] = {}

    def add(kind: str, identifier: str) -> None:
        used[(kind, identifier)] = contracts.registration(kind, identifier)

    for variant in request.variants:
        add("hypothesis", variant.hypothesis_id)
        descriptor = contracts.hypothesis(variant.hypothesis_id)
        closure = contracts.resolve_feature_closure(
            descriptor.dependencies(dict(variant.parameters)),
            where=f"hypothesis variant {variant.variant_id!r}",
        )
        for dependency in closure:
            add("feature", dependency.feature_id)
    for instance in request.models:
        add("model", instance.model_id)
    for declaration in request.metrics:
        add("metric", declaration.metric_id)
    return [used[key] for key in sorted(used)]


def require_declared_sources(
    used: Sequence[contracts.Registration], declared: Mapping[str, str], *, where: str
) -> None:
    """Every used non-built-in descriptor must come from a declared generation.

    ``declared`` maps each verified extension module's path to the digest it was
    imported from.  A descriptor registered from an undeclared path, from no
    path at all, or from a different generation of a declared path is rejected;
    a registration is never accepted merely because it is present in this
    interpreter.
    """
    generations = set(declared.values())
    for registration in used:
        if registration.builtin:
            continue
        label = f"{registration.kind} {registration.identifier!r}"
        if not registration.source_path or not registration.source_digest:
            raise PatternLabDataError(
                f"{where}: {label} was registered without a declared trusted source, so this study "
                "cannot attribute it to a verified source generation. Declare its extension module "
                "in the request.",
                error_code="unverified_source",
            )
        expected = declared.get(registration.source_path)
        if expected is not None and expected != registration.source_digest:
            raise PatternLabDataError(
                f"{where}: {label} was registered from {registration.source_path} with digest "
                f"{registration.source_digest}, but that declared source now hashes to {expected}. "
                "Start a fresh interpreter so the registration matches its source generation.",
                error_code="source_changed",
            )
        # A generation is a digest, not a path: two declarations of byte-identical
        # source are the same generation, and the registry deduplicates them.
        if registration.source_digest not in generations:
            raise PatternLabDataError(
                f"{where}: {label} was registered from {registration.source_path} with digest "
                f"{registration.source_digest}, which is not a source generation this request "
                "declares and verifies. Every used descriptor needs a declared, verified source.",
                error_code="unverified_source",
            )


def declared_digests(loaded: Sequence[Any]) -> dict[str, str]:
    """Map each loaded extension's module path to its verified module digest."""
    return {record.module_path: dict(record.files)[f"{record.module}.py"] for record in loaded}


def require_used_sources(
    request: StudyRequest, loaded: Sequence[Any], *, where: str
) -> list[contracts.Registration]:
    """Resolve and check the used descriptor set in one step."""
    used = used_registrations(request)
    require_declared_sources(used, declared_digests(loaded), where=where)
    return used
