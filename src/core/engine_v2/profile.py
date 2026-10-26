"""Generic Backtester V2 execution profile parsing and validation."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Optional

from .contracts import ExecutionProfile, ModeBinding, VariantSelector, VariantSpec
from .diagnostics import V2Diagnostic, V2ValidationError, warning_messages
from .execution_modes import (
    ExecutionModeValidationError,
    execution_family_supports_mode_value,
    validate_execution_modes,
)
from .runtime_contract import (
    V2_RESERVED_RUNTIME_PARAM_NAMES,
    V2RuntimeValidationError,
    validate_v2_runtime_declarations,
)


class ProfileValidationError(V2ValidationError):
    """Raised when a V2 config cannot be parsed as an execution profile."""

    def __init__(
        self,
        diagnostic: V2Diagnostic | Iterable[V2Diagnostic] | str,
        *,
        strategy_id: str = "<unknown strategy>",
        path: str = "execution",
        variant: str | None = None,
        code: str = "V2_PROFILE_VALIDATION_ERROR",
    ) -> None:
        if isinstance(diagnostic, str):
            diagnostic = V2Diagnostic(
                severity="error",
                code=code,
                strategy_id=strategy_id,
                path=path,
                variant=variant,
                message=diagnostic,
            )
        super().__init__(diagnostic)


VALID_PARAMETER_ROLES = {"signal", "execution", "runtime"}

# Binding metadata remains the authority for active execution parameters. Mode
# and combination support is validated once by execution_modes.py.
MODE_PARAMETER_BINDINGS: tuple[ModeBinding, ...] = (
    ModeBinding("entryAdmission", "risk_bracket_v1", "admission1", ("maxEntryLeverage",)),
    ModeBinding("topology", "signal_reversal", "phase34"),
    ModeBinding("entryOrder", "market_next_open", "phase1"),
    ModeBinding("stop", "atr_swing", "phase1", ("stopX", "stopLP", "stopMaxPct"), ("atr", "rolling_low_high")),
    ModeBinding("stop", "none", "phase34"),
    ModeBinding("stop", "emergency_pct", "phase34", ("emergencySlPct", "emergencySlUpdateBars")),
    ModeBinding("target", "rr", "phase1", ("stopRR",)),
    ModeBinding("target", "none", "phase1"),
    ModeBinding("trail", "ma", "phase1", ("trailRR", "trailMAType", "trailMALength", "trailMAOffsetEx"), ("ma",)),
    ModeBinding("trail", "r_distance", "phase17", ("trailRR", "trailDistanceR")),
    ModeBinding(
        "trail",
        "chandelier",
        "phase17",
        ("trailRR", "chandelierATRLength", "chandelierATRMult"),
        ("chandelier_atr",),
        ("chandelierATRLength",),
    ),
    ModeBinding("trail", "fixed_af_sar", "phase17", ("trailRR", "sarSpeed")),
    ModeBinding("trail", "none", "phase1"),
    ModeBinding("trailActivation", "rr", "phase1"),
    ModeBinding("trailActivation", "none", "phase1"),
    ModeBinding("sizing", "risk_per_trade", "phase1", ("riskPerTrade", "contractSize")),
    ModeBinding("sizing", "fixed_pct_equity", "phase34", ("positionPct", "contractSize")),
    ModeBinding("maxDays", "true", "phase1", ("stopMaxDays",), ("timestamps",)),
    ModeBinding("maxDays", "false", "phase1"),
    ModeBinding("boundary", "strict_close", "phase1"),
    ModeBinding("boundary", "none", "phase1"),
    ModeBinding("priceRounding", "none", "phase1"),
    ModeBinding("priceRounding", "tick_outward", "phase1", ("tickSize",)),
    # report_only observes the leverage implied by fills; it has no configured
    # leverage or maintenance inputs.  Those belong to the deferred simulated
    # margin policy below.
    ModeBinding("margin", "report_only", "phase1", (), ()),
    ModeBinding("margin", "off", "phase1"),
    ModeBinding("exitOnSignal", "true", "phase34"),
    # Explicitly known but uncertified values remain unavailable.
    ModeBinding("margin", "simulate", "later", ("leverage", "maintenanceMarginPct", "markPriceSource"), ("mark_price",)),
    ModeBinding("stop", "atr", "later", ("stopX", "stopMaxPct"), ("atr",)),
    ModeBinding("stop", "swing", "later", ("stopLP", "stopMaxPct"), ("rolling_low_high",)),
    ModeBinding("stop", "pct", "later", ("stopPct", "stopMaxPct")),
    ModeBinding("trail", "atr", "later", ("trailRR", "trailAtrMult"), ("atr",)),
)
_BINDINGS_BY_KEY = {(binding.mode_field, binding.mode_value): binding for binding in MODE_PARAMETER_BINDINGS}
_TOPOLOGY_WIDE_EXECUTION_PARAMS = {
    "position": frozenset({"initialCapital", "commissionPct", "enableLong", "enableShort"}),
    "signal_reversal": frozenset({"initialCapital", "commissionPct", "enableLong", "enableShort"}),
}


def is_v2_config(config: Mapping[str, Any]) -> bool:
    return str(config.get("engine", "v1")).strip().lower() == "v2"


def canonical_selector_key(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return str(int(value))
        return repr(value)
    if value is None:
        return "null"
    return str(value)


def _canonical_mapping_key(value: Any) -> str:
    if not isinstance(value, str):
        return canonical_selector_key(value)
    raw = value.strip()
    lower = raw.lower()
    if lower in {"true", "false"}:
        return lower
    try:
        return str(int(raw, 10))
    except ValueError:
        pass
    try:
        parsed = float(raw)
    except ValueError:
        return value
    if not math.isfinite(parsed):
        return value
    return str(int(parsed)) if parsed.is_integer() else repr(parsed)


def _strategy_label(config: Mapping[str, Any]) -> str:
    return str(config.get("id") or "<unknown strategy>")


def _error(
    strategy_id: str,
    message: str,
    *,
    path: str,
    code: str,
    variant: str | None = None,
) -> ProfileValidationError:
    return ProfileValidationError(
        V2Diagnostic("error", code, strategy_id, path, variant, message)
    )


def _warning(
    strategy_id: str,
    message: str,
    *,
    path: str,
    code: str,
    variant: str | None = None,
) -> V2Diagnostic:
    return V2Diagnostic("warning", code, strategy_id, path, variant, message)


def _info(
    strategy_id: str,
    message: str,
    *,
    path: str,
    code: str,
    variant: str | None = None,
) -> V2Diagnostic:
    return V2Diagnostic("info", code, strategy_id, path, variant, message)


def _parameters(config: Mapping[str, Any]) -> Mapping[str, Any]:
    params = config.get("parameters", {})
    if not isinstance(params, Mapping):
        strategy_id = _strategy_label(config)
        raise _error(
            strategy_id,
            f"{strategy_id}: parameters must be a mapping.",
            path="parameters",
            code="V2_INVALID_PROFILE",
        )
    return params


def _is_optimized(spec: Any) -> bool:
    if not isinstance(spec, Mapping):
        return False
    optimize = spec.get("optimize", {})
    return isinstance(optimize, Mapping) and bool(optimize.get("enabled", False))


def _role_for(name: str, spec: Any, strategy_id: str, *, required: bool) -> Optional[str]:
    role = spec.get("role") if isinstance(spec, Mapping) else None
    if role is None:
        if required:
            raise _error(
                strategy_id,
                f"{strategy_id}: optimized parameter '{name}' must declare role ('signal', 'execution', or 'runtime').",
                path=f"parameters.{name}.role",
                code="V2_INVALID_PARAMETER_ROLE",
            )
        return None
    normalized = str(role).strip().lower()
    if normalized not in VALID_PARAMETER_ROLES:
        raise _error(
            strategy_id,
            f"{strategy_id}: parameter '{name}' has invalid role '{role}'. Expected one of {sorted(VALID_PARAMETER_ROLES)}.",
            path=f"parameters.{name}.role",
            code="V2_INVALID_PARAMETER_ROLE",
        )
    return normalized


def _dependency_names(
    depends_on: Any,
    *,
    strategy_id: str,
    parameter_name: str,
) -> tuple[str, ...]:
    if depends_on in (None, ""):
        return ()
    if isinstance(depends_on, str):
        return (depends_on,)
    if isinstance(depends_on, Sequence) and not isinstance(depends_on, (str, bytes, bytearray)):
        if all(isinstance(item, str) and item.strip() for item in depends_on):
            return tuple(depends_on)
        raise _error(
            strategy_id,
            f"{strategy_id}: parameter '{parameter_name}' depends_on list must contain only non-empty strings.",
            path=f"parameters.{parameter_name}.depends_on",
            code="V2_INVALID_DEPENDENCY",
        )
    raise _error(
        strategy_id,
        f"{strategy_id}: parameter '{parameter_name}' depends_on must be a string or list of strings.",
        path=f"parameters.{parameter_name}.depends_on",
        code="V2_INVALID_DEPENDENCY",
    )


def _validate_dependencies(
    *,
    strategy_id: str,
    params: Mapping[str, Any],
    roles: Mapping[str, Optional[str]],
) -> None:
    graph: dict[str, tuple[str, ...]] = {}
    for raw_name, raw_spec in params.items():
        name = str(raw_name)
        if not isinstance(raw_spec, Mapping) or "depends_on" not in raw_spec:
            continue
        parents = _dependency_names(
            raw_spec.get("depends_on"),
            strategy_id=strategy_id,
            parameter_name=name,
        )
        if len(set(parents)) != len(parents):
            raise _error(strategy_id, f"{strategy_id}: parameter '{name}' has duplicate depends_on parents.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
        if name in parents:
            raise _error(strategy_id, f"{strategy_id}: parameter '{name}' cannot depend on itself.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
        child_role = roles.get(name)
        if child_role is None:
            raise _error(strategy_id, f"{strategy_id}: parameter '{name}' uses depends_on and must declare a role.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
        if child_role == "runtime":
            raise _error(strategy_id, f"{strategy_id}: runtime parameter '{name}' cannot use depends_on.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
        for parent in parents:
            if parent not in params:
                raise _error(strategy_id, f"{strategy_id}: parameter '{name}' depends on unknown parameter '{parent}'.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
            parent_spec = params[parent]
            parent_role = roles.get(parent)
            if parent_role is None:
                raise _error(strategy_id, f"{strategy_id}: dependency parent '{parent}' must declare a role.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
            if parent_role == "runtime" or parent in V2_RESERVED_RUNTIME_PARAM_NAMES:
                raise _error(strategy_id, f"{strategy_id}: runtime parameter '{parent}' cannot be a depends_on parent.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
            if parent_role != child_role:
                raise _error(strategy_id, f"{strategy_id}: cross-role depends_on is not supported in V2 ('{name}' role={child_role}, '{parent}' role={parent_role}).", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
            parent_type = str(parent_spec.get("type") or "").strip().lower() if isinstance(parent_spec, Mapping) else ""
            if parent_type != "bool":
                raise _error(strategy_id, f"{strategy_id}: depends_on parent '{parent}' for '{name}' must be boolean.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
        graph[name] = parents

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise _error(strategy_id, f"{strategy_id}: depends_on contains a cycle at '{name}'.", path=f"parameters.{name}.depends_on", code="V2_INVALID_DEPENDENCY")
        visiting.add(name)
        for parent in graph.get(name, ()):
            visit(parent)
        visiting.remove(name)
        visited.add(name)

    for name in graph:
        visit(name)


def validate_parameter_roles(config: Mapping[str, Any]) -> None:
    """Validate V2 roles/dependencies; V1 remains an exact no-op."""

    if not is_v2_config(config):
        return
    try:
        validate_v2_runtime_declarations(config)
        from .parameter_ties import parameter_tie_groups
        parameter_tie_groups(config)
    except V2RuntimeValidationError as exc:
        raise ProfileValidationError(exc.diagnostics) from exc
    except ValueError as exc:
        raise ProfileValidationError(str(exc), strategy_id=_strategy_label(config), path="optimization_rules.parameter_tie_groups") from exc
    strategy_id = _strategy_label(config)
    params = _parameters(config)
    roles = {str(name): _role_for(str(name), spec, strategy_id, required=_is_optimized(spec)) for name, spec in params.items()}
    _validate_dependencies(strategy_id=strategy_id, params=params, roles=roles)


def _parameter_defaults(params: Mapping[str, Any]) -> dict[str, Any]:
    return {str(name): spec["default"] for name, spec in params.items() if isinstance(spec, Mapping) and "default" in spec}


def _parameter_roles(config: Mapping[str, Any]) -> dict[str, str]:
    if not is_v2_config(config):
        return {}
    strategy_id = _strategy_label(config)
    roles: dict[str, str] = {}
    for name, spec in _parameters(config).items():
        role = _role_for(str(name), spec, strategy_id, required=_is_optimized(spec))
        if role is not None:
            roles[str(name)] = role
    return roles


def _base_modes(execution: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): canonical_selector_key(value) for key, value in execution.items() if key not in {"variantSelector", "variants"}}


def _parse_variants(
    execution: Mapping[str, Any],
    base_modes: Mapping[str, str],
    strategy_id: str,
) -> dict[str, VariantSpec]:
    raw = execution.get("variants")
    if raw is None:
        return {"default": VariantSpec("default", dict(base_modes))}
    if not isinstance(raw, Mapping) or not raw:
        raise _error(
            strategy_id,
            f"{strategy_id}: execution.variants must be a non-empty mapping when present.",
            path="execution.variants",
            code="V2_INVALID_PROFILE",
        )
    variants: dict[str, VariantSpec] = {}
    for name, payload in raw.items():
        if not isinstance(payload, Mapping):
            raise _error(
                strategy_id,
                f"{strategy_id}: execution variant '{name}' must be a mapping.",
                path=f"execution.variants.{name}",
                variant=str(name),
                code="V2_INVALID_PROFILE",
            )
        if "entryAdmission" in payload:
            raise _error(strategy_id, "entryAdmission is profile-wide; variants must not declare it.",
                         path=f"execution.variants.{name}.entryAdmission", code="V2_INVALID_PROFILE")
        modes = dict(base_modes)
        modes.update({str(key): canonical_selector_key(value) for key, value in payload.items()})
        variants[str(name)] = VariantSpec(str(name), modes)
    return variants


def _parse_selector(
    execution: Mapping[str, Any],
    variants: Mapping[str, VariantSpec],
    params: Mapping[str, Any],
    roles: Mapping[str, str],
    strategy_id: str,
) -> Optional[VariantSelector]:
    raw = execution.get("variantSelector")
    if raw is None:
        if len(variants) > 1:
            raise _error(strategy_id, f"{strategy_id}: execution.variantSelector is required for multiple variants.", path="execution.variantSelector", code="V2_INVALID_SELECTOR")
        return None
    if not isinstance(raw, Mapping):
        raise _error(strategy_id, f"{strategy_id}: execution.variantSelector must be a mapping.", path="execution.variantSelector", code="V2_INVALID_SELECTOR")
    selector_param = str(raw.get("param") or "")
    if not selector_param or selector_param not in params:
        raise _error(strategy_id, f"{strategy_id}: variantSelector parameter '{selector_param}' is not declared in parameters.", path="execution.variantSelector.param", code="V2_INVALID_SELECTOR")
    selector_spec = params[selector_param]
    selector_type = str(selector_spec.get("type") or "").strip().lower() if isinstance(selector_spec, Mapping) else ""
    if selector_param in V2_RESERVED_RUNTIME_PARAM_NAMES or roles.get(selector_param) != "execution" or selector_type not in {"bool", "select", "options"}:
        raise _error(strategy_id, f"{strategy_id}: variantSelector parameter '{selector_param}' must be a non-runtime bool/select execution parameter.", path="execution.variantSelector.param", code="V2_INVALID_SELECTOR")
    if isinstance(selector_spec, Mapping) and "depends_on" in selector_spec:
        raise _error(strategy_id, f"{strategy_id}: variantSelector parameter '{selector_param}' cannot use depends_on.", path=f"parameters.{selector_param}.depends_on", code="V2_INVALID_SELECTOR")
    raw_mapping = raw.get("mapping")
    if not isinstance(raw_mapping, Mapping) or not raw_mapping:
        raise _error(strategy_id, f"{strategy_id}: execution.variantSelector.mapping must be a non-empty mapping.", path="execution.variantSelector.mapping", code="V2_INVALID_SELECTOR")
    mapping: dict[str, str] = {}
    for raw_key, raw_variant in raw_mapping.items():
        key = _canonical_mapping_key(raw_key)
        if key in mapping:
            raise _error(strategy_id, f"{strategy_id}: variantSelector has duplicate normalized key '{key}'.", path="execution.variantSelector.mapping", code="V2_INVALID_SELECTOR")
        variant = str(raw_variant)
        if variant not in variants:
            raise _error(strategy_id, f"{strategy_id}: execution.variantSelector maps '{key}' to unknown variant '{variant}'.", path="execution.variantSelector.mapping", code="V2_INVALID_SELECTOR")
        mapping[key] = variant
    unreachable = [name for name in variants if name not in set(mapping.values())]
    if unreachable:
        raise _error(strategy_id, f"{strategy_id}: variantSelector cannot reach variant(s) {unreachable}.", path="execution.variantSelector.mapping", code="V2_INVALID_SELECTOR")
    user_facing = raw.get("userFacing", True)
    if not isinstance(user_facing, bool):
        raise _error(strategy_id, f"{strategy_id}: variantSelector.userFacing must be boolean.", path="execution.variantSelector.userFacing", code="V2_INVALID_SELECTOR")
    return VariantSelector(selector_param, mapping, user_facing)


def _binding_for(mode_field: str, mode_value: Any) -> Optional[ModeBinding]:
    return _BINDINGS_BY_KEY.get((str(mode_field), canonical_selector_key(mode_value)))


def _consumed_params_for_modes(modes: Mapping[str, Any]) -> set[str]:
    consumed: set[str] = set()
    for field, value in modes.items():
        binding = _binding_for(field, value)
        if binding is not None:
            consumed.update(binding.consumes_params)
    if modes.get("entryAdmission") == "risk_bracket_v1":
        consumed.discard("contractSize")
    return consumed


def _validate_variants(
    *,
    strategy_id: str,
    variants: Mapping[str, VariantSpec],
    params: Mapping[str, Any],
) -> Mapping[str, str]:
    families: dict[str, str] = {}
    for name, variant in variants.items():
        try:
            family = validate_execution_modes(variant.modes)
        except ExecutionModeValidationError as exc:
            raise _error(strategy_id, f"{strategy_id}: variant '{name}': {exc}", path=f"execution.variants.{name}.{exc.field}", variant=name, code="V2_UNSUPPORTED_EXECUTION_MODE") from exc
        families[name] = family
        for field, value in variant.modes.items():
            binding = _binding_for(field, value)
            if binding is None:
                continue
            if binding.status == "later":
                raise _error(strategy_id, f"{strategy_id}: variant '{name}' uses uncertified {field}={value!r}.", path=f"execution.variants.{name}.{field}", variant=name, code="V2_UNSUPPORTED_EXECUTION_MODE")
            required = binding.consumes_params
            if variant.modes.get("entryAdmission") == "risk_bracket_v1":
                required = tuple(p for p in required if p != "contractSize" and (p != "stopMaxPct" or p in params))
            missing = [param for param in required if param not in params]
            if missing:
                raise _error(strategy_id, f"{strategy_id}: variant '{name}' mode {field}={value!r} consumes undeclared parameter(s) {missing}.", path=f"execution.variants.{name}.{field}", variant=name, code="V2_UNDECLARED_CONSUMED_PARAMETER")
    return families


def _positive_integer(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0 and number.is_integer()


def _validate_chandelier_length_declaration(
    *,
    strategy_id: str,
    variants: Mapping[str, VariantSpec],
    params: Mapping[str, Any],
) -> None:
    if not any(variant.modes.get("trail") == "chandelier" for variant in variants.values()):
        return
    name = "chandelierATRLength"
    spec = params.get(name)
    if not isinstance(spec, Mapping):
        raise _error(
            strategy_id,
            f"{strategy_id}: Chandelier trail requires a declared positive-integer '{name}' parameter.",
            path=f"parameters.{name}",
            code="V2_INVALID_PROFILE",
        )
    param_type = str(spec.get("type") or "").strip().lower()
    if param_type not in {"int", "integer"} or not _positive_integer(spec.get("default")):
        raise _error(
            strategy_id,
            f"{strategy_id}: '{name}' must be an integer parameter with a positive integer default.",
            path=f"parameters.{name}",
            code="V2_INVALID_PROFILE",
        )
    optimize = spec.get("optimize", {})
    if optimize is not None and not isinstance(optimize, Mapping):
        raise _error(
            strategy_id,
            f"{strategy_id}: '{name}.optimize' must be a mapping when present.",
            path=f"parameters.{name}.optimize",
            code="V2_INVALID_PROFILE",
        )
    optimize = optimize if isinstance(optimize, Mapping) else {}
    explicit = next(
        (
            values
            for values in (
                spec.get("gridValues"),
                optimize.get("gridValues"),
                optimize.get("values"),
            )
            if values is not None
        ),
        None,
    )
    if explicit is not None:
        valid = isinstance(explicit, Sequence) and not isinstance(explicit, (str, bytes)) and bool(explicit)
        valid = valid and all(_positive_integer(value) for value in explicit)
    elif optimize.get("enabled", False):
        bounds = tuple(optimize.get(key, spec.get(key)) for key in ("min", "max", "step"))
        valid = all(_positive_integer(value) for value in bounds)
        if valid:
            valid = float(bounds[1]) >= float(bounds[0])
    else:
        valid = True
    if not valid:
        raise _error(
            strategy_id,
            f"{strategy_id}: '{name}' must declare a non-empty positive-integer Grid domain.",
            path=f"parameters.{name}.optimize",
            code="V2_INVALID_PROFILE",
        )


def _variant_independent_params(
    *,
    strategy_id: str,
    params: Mapping[str, Any],
    roles: Mapping[str, str],
    variants: Mapping[str, VariantSpec],
    families: Mapping[str, str],
    selector: Optional[VariantSelector],
) -> tuple[tuple[str, ...], tuple[V2Diagnostic, ...]]:
    diagnostics: list[V2Diagnostic] = []
    independent = {name for name, role in roles.items() if role == "signal"}
    if selector is not None:
        independent.add(selector.param)
    variant_consumed = set().union(
        *(_consumed_params_for_modes(variant.modes) for variant in variants.values())
    )
    topology_consumed: set[str] = set()
    for family in set(families.values()):
        topology_consumed.update(
            name
            for name in _TOPOLOGY_WIDE_EXECUTION_PARAMS[family]
            if roles.get(name) == "execution"
        )
    bound = variant_consumed | topology_consumed
    independent.update(topology_consumed)
    declared_families = tuple(dict.fromkeys(families.values()))

    for name, role in roles.items():
        if role != "execution" or name in bound or (selector is not None and name == selector.param):
            continue
        consuming_bindings = tuple(
            binding
            for binding in MODE_PARAMETER_BINDINGS
            if name in binding.consumes_params
        )
        globally_certified_bindings = tuple(
            binding for binding in consuming_bindings if binding.status != "later"
        )
        family_certified_bindings = tuple(
            binding
            for binding in globally_certified_bindings
            if any(
                execution_family_supports_mode_value(
                    family,
                    binding.mode_field,
                    binding.mode_value,
                )
                for family in declared_families
            )
        )
        optimized = _is_optimized(params.get(name))
        if family_certified_bindings:
            modes = ", ".join(
                f"{binding.mode_field}={binding.mode_value}"
                for binding in family_certified_bindings
            )
            if optimized:
                raise _error(
                    strategy_id,
                    f"{strategy_id}: optimized execution parameter '{name}' is consumed by "
                    f"certified mode(s) [{modes}], but no declared variant selects them.",
                    path=f"parameters.{name}",
                    code="V2_UNSELECTED_MODE_OPTIMIZED_EXECUTION_PARAM",
                )
            diagnostics.append(
                _info(
                    strategy_id,
                    f"{strategy_id}: fixed execution parameter '{name}' is consumed by "
                    f"certified mode(s) [{modes}], but no declared variant selects them; "
                    "it is excluded from semantic identity.",
                    path=f"parameters.{name}",
                    code="V2_UNSELECTED_MODE_EXECUTION_PARAM",
                )
            )
            continue
        if globally_certified_bindings:
            modes = ", ".join(
                f"{binding.mode_field}={binding.mode_value}"
                for binding in globally_certified_bindings
            )
            family_names = ", ".join(declared_families)
            detail = (
                f" has known certified consumer mode(s) [{modes}], but they are "
                f"incompatible with declared execution family/families [{family_names}]"
            )
        elif consuming_bindings:
            modes = ", ".join(
                f"{binding.mode_field}={binding.mode_value}"
                for binding in consuming_bindings
            )
            detail = f" has only uncertified consumer mode(s) [{modes}]"
        else:
            detail = " has no known execution-mode consumer"
        if optimized:
            raise _error(
                strategy_id,
                f"{strategy_id}: optimized execution parameter '{name}'{detail} in the "
                "current certified contract.",
                path=f"parameters.{name}",
                code="V2_UNBOUND_OPTIMIZED_EXECUTION_PARAM",
            )
        diagnostics.append(
            _warning(
                strategy_id,
                f"{strategy_id}: fixed execution parameter '{name}'{detail} in the "
                "current certified contract; it is excluded from semantic identity.",
                path=f"parameters.{name}",
                code="V2_UNBOUND_FIXED_EXECUTION_PARAM",
            )
        )
    return tuple(name for name in params if name in independent), tuple(diagnostics)


def parse_execution_profile(config: Mapping[str, Any]) -> ExecutionProfile:
    """Parse and fully validate a V2 execution profile once, before hot paths."""

    if not isinstance(config, Mapping):
        raise ProfileValidationError("strategy config must be a mapping.")
    validate_parameter_roles(config)
    strategy_id = _strategy_label(config)
    engine = str(config.get("engine", "v1")).strip().lower()
    execution = config.get("execution", {}) or {}
    if not isinstance(execution, Mapping):
        raise _error(strategy_id, f"{strategy_id}: execution must be a mapping.", path="execution", code="V2_INVALID_PROFILE")
    params = _parameters(config)
    parameter_names = tuple(str(name) for name in params)
    defaults = _parameter_defaults(params)
    roles = _parameter_roles(config)
    base_modes = _base_modes(execution)
    if base_modes.get("entryAdmission") == "risk_bracket_v1":
        from .admission import positive_cap
        if "contractSize" in params:
            raise _error(strategy_id, "risk_bracket_v1 must not declare contractSize; instrument rules own sizing.", path="parameters.contractSize", code="V2_INVALID_PROFILE")
        cap_spec = params.get("maxEntryLeverage", {})
        if roles.get("maxEntryLeverage") != "execution" or _is_optimized(cap_spec):
            raise _error(strategy_id, "maxEntryLeverage must be a fixed execution parameter.", path="parameters.maxEntryLeverage", code="V2_INVALID_PROFILE")
        try:
            valid_default = positive_cap(cap_spec.get("default")) == 8
        except ValueError:
            valid_default = False
        if not valid_default:
            raise _error(strategy_id, "maxEntryLeverage must declare default 8.", path="parameters.maxEntryLeverage.default", code="V2_INVALID_PROFILE")
        if cap_spec.get("depends_on") or any(isinstance(spec, Mapping) and
                "maxEntryLeverage" in _dependency_names(spec.get("depends_on"), strategy_id=strategy_id, parameter_name=name)
                for name, spec in params.items() if spec.get("depends_on")):
            raise _error(strategy_id, "maxEntryLeverage cannot participate in dependencies.", path="parameters.maxEntryLeverage", code="V2_INVALID_PROFILE")
        if (execution.get("variantSelector") or {}).get("param") == "maxEntryLeverage":
            raise _error(strategy_id, "maxEntryLeverage cannot be a variant selector.", path="execution.variantSelector", code="V2_INVALID_PROFILE")
    variants = _parse_variants(execution, base_modes, strategy_id)
    selector = _parse_selector(execution, variants, params, roles, strategy_id)
    families = _validate_variants(strategy_id=strategy_id, variants=variants, params=params)
    _validate_chandelier_length_declaration(
        strategy_id=strategy_id,
        variants=variants,
        params=params,
    )
    independent, diagnostics = _variant_independent_params(
        strategy_id=strategy_id,
        params=params,
        roles=roles,
        variants=variants,
        families=families,
        selector=selector,
    )
    return ExecutionProfile(
        strategy_id=strategy_id,
        engine=engine,
        modes=dict(base_modes),
        variants=variants,
        variant_selector=selector,
        parameter_defaults=defaults,
        parameter_names=parameter_names,
        parameter_roles=roles,
        variant_independent_params=independent,
        validation_warnings=warning_messages(diagnostics),
        diagnostics=diagnostics,
    )


def resolve_variant(profile: ExecutionProfile, params: Mapping[str, Any]) -> VariantSpec:
    selector = profile.variant_selector
    if selector is None:
        if len(profile.variants) != 1:
            raise _error(
                profile.strategy_id,
                f"{profile.strategy_id}: profile has multiple variants but no selector.",
                path="execution.variantSelector",
                code="V2_INVALID_SELECTOR",
            )
        return next(iter(profile.variants.values()))
    if selector.param in params:
        raw_value = params[selector.param]
    elif selector.param in profile.parameter_defaults:
        raw_value = profile.parameter_defaults[selector.param]
    else:
        raise _error(
            profile.strategy_id,
            f"{profile.strategy_id}: selector parameter '{selector.param}' is missing "
            "from params and has no config default.",
            path=f"parameters.{selector.param}",
            code="V2_INVALID_SELECTOR",
        )
    key = canonical_selector_key(raw_value)
    variant_name = selector.mapping.get(key)
    if variant_name is None:
        raise _error(
            profile.strategy_id,
            f"{profile.strategy_id}: selector parameter '{selector.param}' value "
            f"{raw_value!r} maps to '{key}', which is not in variantSelector.mapping.",
            path="execution.variantSelector.mapping",
            code="V2_INVALID_SELECTOR",
        )
    return profile.variants[variant_name]


def active_mode_values(profile: ExecutionProfile, params: Mapping[str, Any]) -> dict[str, str]:
    return dict(resolve_variant(profile, params).modes)


def active_parameter_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> set[str]:
    active = set(profile.variant_independent_params)
    declared = set(profile.parameter_names)
    active.update(name for name in _consumed_params_for_modes(active_mode_values(profile, params)) if name in declared)
    return {name for name in active if profile.parameter_roles.get(name) != "runtime"}


def inactive_parameter_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> set[str]:
    candidates = {name for name in profile.parameter_names if profile.parameter_roles.get(name) != "runtime"}
    return candidates - active_parameter_names(profile, params)


def mode_binding_for(mode_field: str, mode_value: Any) -> Optional[ModeBinding]:
    return _binding_for(mode_field, mode_value)


__all__ = [
    "MODE_PARAMETER_BINDINGS",
    "ProfileValidationError",
    "VALID_PARAMETER_ROLES",
    "active_mode_values",
    "active_parameter_names",
    "canonical_selector_key",
    "inactive_parameter_names",
    "is_v2_config",
    "mode_binding_for",
    "parse_execution_profile",
    "resolve_variant",
    "validate_parameter_roles",
]
