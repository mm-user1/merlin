"""Generic Backtester V2 execution profile parsing and validation."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any, Optional

from .contracts import ExecutionProfile, ModeBinding, VariantSelector, VariantSpec


class ProfileValidationError(ValueError):
    """Raised when a V2 config cannot be parsed as an execution profile."""


VALID_PARAMETER_ROLES = {"signal", "execution", "runtime"}

MODE_PARAMETER_BINDINGS: tuple[ModeBinding, ...] = (
    ModeBinding("entryOrder", "market_next_open", "phase1"),
    ModeBinding("stop", "atr_swing", "phase1", ("stopX", "stopLP", "stopMaxPct"), ("atr", "rolling_low_high")),
    ModeBinding("target", "rr", "phase1", ("stopRR",)),
    ModeBinding("target", "none", "phase1"),
    ModeBinding(
        "trail",
        "ma",
        "phase1",
        ("trailRR", "trailMAType", "trailMALength", "trailMAOffsetEx"),
        ("ma",),
    ),
    ModeBinding("trail", "none", "phase1"),
    ModeBinding("sizing", "risk_per_trade", "phase1", ("riskPerTrade", "contractSize")),
    ModeBinding("maxDays", "true", "phase1", ("stopMaxDays",), ("timestamps",)),
    ModeBinding(
        "margin",
        "report_only",
        "phase1",
        ("leverage", "maintenanceMarginPct", "markPriceSource"),
        ("mark_price",),
    ),
    ModeBinding("margin", "off", "phase1"),
    ModeBinding(
        "margin",
        "simulate",
        "later",
        ("leverage", "maintenanceMarginPct", "markPriceSource"),
        ("mark_price",),
    ),
    ModeBinding("stop", "atr", "later", ("stopX", "stopMaxPct"), ("atr",)),
    ModeBinding("stop", "swing", "later", ("stopLP", "stopMaxPct"), ("rolling_low_high",)),
    ModeBinding("stop", "pct", "later", ("stopPct", "stopMaxPct")),
    ModeBinding("trail", "atr", "later", ("trailRR", "trailAtrMult"), ("atr",)),
    ModeBinding("sizing", "fixed_pct_equity", "later", ("positionPct",)),
    ModeBinding("exitOnSignal", "true", "later"),
)

_BINDINGS_BY_KEY = {
    (binding.mode_field, binding.mode_value): binding
    for binding in MODE_PARAMETER_BINDINGS
}


def is_v2_config(config: Mapping[str, Any]) -> bool:
    """Return whether a strategy config opts into Backtester V2."""

    return str(config.get("engine", "v1")).strip().lower() == "v2"


def canonical_selector_key(value: Any) -> str:
    """Canonical string form used by variant selector mappings."""

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


def _strategy_label(config: Mapping[str, Any]) -> str:
    return str(config.get("id") or "<unknown strategy>")


def _parameters(config: Mapping[str, Any]) -> Mapping[str, Any]:
    params = config.get("parameters", {})
    if not isinstance(params, Mapping):
        raise ProfileValidationError(f"{_strategy_label(config)}: parameters must be a mapping.")
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
            raise ProfileValidationError(
                f"{strategy_id}: optimized parameter '{name}' must declare role "
                "('signal', 'execution', or 'runtime')."
            )
        return None
    normalized = str(role).strip().lower()
    if normalized not in VALID_PARAMETER_ROLES:
        raise ProfileValidationError(
            f"{strategy_id}: parameter '{name}' has invalid role '{role}'. "
            f"Expected one of {sorted(VALID_PARAMETER_ROLES)}."
        )
    return normalized


def _dependency_names(depends_on: Any) -> tuple[str, ...]:
    if depends_on in (None, ""):
        return ()
    if isinstance(depends_on, str):
        return (depends_on,)
    if isinstance(depends_on, Iterable):
        return tuple(str(item) for item in depends_on)
    raise ProfileValidationError("depends_on must be a string or list of strings.")


def validate_parameter_roles(config: Mapping[str, Any]) -> None:
    """Validate explicit V2 roles and reject cross-role dependencies.

    This is intentionally a no-op for V1 configs so legacy strategy discovery
    and runtime paths do not inherit V2 validation.
    """

    if not is_v2_config(config):
        return

    strategy_id = _strategy_label(config)
    params = _parameters(config)
    roles: dict[str, Optional[str]] = {}
    for name, spec in params.items():
        roles[str(name)] = _role_for(str(name), spec, strategy_id, required=_is_optimized(spec))

    for name, spec in params.items():
        if not isinstance(spec, Mapping) or "depends_on" not in spec:
            continue
        child = str(name)
        child_role = roles.get(child)
        if child_role is None:
            raise ProfileValidationError(
                f"{strategy_id}: parameter '{child}' uses depends_on and must declare a role."
            )
        for parent in _dependency_names(spec.get("depends_on")):
            if parent not in params:
                raise ProfileValidationError(
                    f"{strategy_id}: parameter '{child}' depends on unknown parameter '{parent}'."
                )
            parent_role = roles.get(parent)
            if parent_role is None:
                raise ProfileValidationError(
                    f"{strategy_id}: dependency parent '{parent}' must declare a role."
                )
            if parent_role != child_role:
                raise ProfileValidationError(
                    f"{strategy_id}: cross-role depends_on is not supported in V2 "
                    f"('{child}' role={child_role}, '{parent}' role={parent_role})."
                )


def _parameter_defaults(params: Mapping[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {}
    for name, spec in params.items():
        if isinstance(spec, Mapping) and "default" in spec:
            defaults[str(name)] = spec["default"]
    return defaults


def _parameter_roles(config: Mapping[str, Any]) -> dict[str, str]:
    if not is_v2_config(config):
        return {}
    roles: dict[str, str] = {}
    strategy_id = _strategy_label(config)
    for name, spec in _parameters(config).items():
        role = _role_for(str(name), spec, strategy_id, required=_is_optimized(spec))
        if role is not None:
            roles[str(name)] = role
    return roles


def _base_modes(execution: Mapping[str, Any]) -> dict[str, str]:
    excluded = {"variantSelector", "variants"}
    return {
        str(key): canonical_selector_key(value)
        for key, value in execution.items()
        if key not in excluded
    }


def _parse_variants(execution: Mapping[str, Any], base_modes: Mapping[str, str]) -> dict[str, VariantSpec]:
    raw_variants = execution.get("variants")
    if raw_variants is None:
        return {"default": VariantSpec(name="default", modes=dict(base_modes))}
    if not isinstance(raw_variants, Mapping) or not raw_variants:
        raise ProfileValidationError("execution.variants must be a non-empty mapping when present.")

    variants: dict[str, VariantSpec] = {}
    for name, payload in raw_variants.items():
        if not isinstance(payload, Mapping):
            raise ProfileValidationError(f"execution variant '{name}' must be a mapping.")
        modes = dict(base_modes)
        modes.update({str(key): canonical_selector_key(value) for key, value in payload.items()})
        variants[str(name)] = VariantSpec(name=str(name), modes=modes)
    return variants


def _parse_selector(
    execution: Mapping[str, Any],
    variants: Mapping[str, VariantSpec],
) -> Optional[VariantSelector]:
    raw_selector = execution.get("variantSelector")
    if raw_selector is None:
        if len(variants) > 1:
            raise ProfileValidationError("execution.variantSelector is required for multiple variants.")
        return None
    if not isinstance(raw_selector, Mapping):
        raise ProfileValidationError("execution.variantSelector must be a mapping.")
    selector_param = raw_selector.get("param")
    if not selector_param:
        raise ProfileValidationError("execution.variantSelector.param is required.")
    raw_mapping = raw_selector.get("mapping")
    if not isinstance(raw_mapping, Mapping) or not raw_mapping:
        raise ProfileValidationError("execution.variantSelector.mapping must be a non-empty mapping.")

    mapping: dict[str, str] = {}
    for raw_key, raw_variant_name in raw_mapping.items():
        key = canonical_selector_key(raw_key)
        variant_name = str(raw_variant_name)
        if variant_name not in variants:
            raise ProfileValidationError(
                f"execution.variantSelector maps '{key}' to unknown variant '{variant_name}'."
            )
        mapping[key] = variant_name
    return VariantSelector(param=str(selector_param), mapping=mapping)


def _binding_for(mode_field: str, mode_value: Any) -> Optional[ModeBinding]:
    return _BINDINGS_BY_KEY.get((str(mode_field), canonical_selector_key(mode_value)))


def _consumed_params_for_modes(modes: Mapping[str, Any]) -> set[str]:
    consumed: set[str] = set()
    for mode_field, mode_value in modes.items():
        binding = _binding_for(str(mode_field), mode_value)
        if binding is not None:
            consumed.update(binding.consumes_params)
    return consumed


def _all_bound_params(variants: Mapping[str, VariantSpec]) -> set[str]:
    bound: set[str] = set()
    for variant in variants.values():
        bound.update(_consumed_params_for_modes(variant.modes))
    return bound


def _variant_independent_params(
    *,
    roles: Mapping[str, str],
    variants: Mapping[str, VariantSpec],
    selector: Optional[VariantSelector],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    warnings: list[str] = []
    independent: set[str] = {
        name
        for name, role in roles.items()
        if role == "signal"
    }
    if selector is not None:
        independent.add(selector.param)

    bound_params = _all_bound_params(variants)
    for name, role in roles.items():
        if role != "execution" or name in bound_params or name in independent:
            continue
        independent.add(name)
        warnings.append(
            f"execution parameter '{name}' is not consumed by any mode binding; "
            "treating it as variant-independent active."
        )
    return tuple(sorted(independent)), tuple(warnings)


def parse_execution_profile(config: Mapping[str, Any]) -> ExecutionProfile:
    """Parse a V2 strategy config into an execution profile.

    The parser is data-driven and performs no runtime dispatch.
    """

    if not isinstance(config, Mapping):
        raise ProfileValidationError("strategy config must be a mapping.")
    validate_parameter_roles(config)

    strategy_id = _strategy_label(config)
    engine = str(config.get("engine", "v1")).strip().lower()
    execution = config.get("execution", {})
    if execution is None:
        execution = {}
    if not isinstance(execution, Mapping):
        raise ProfileValidationError(f"{strategy_id}: execution must be a mapping.")

    params = _parameters(config)
    defaults = _parameter_defaults(params)
    roles = _parameter_roles(config)
    base_modes = _base_modes(execution)
    variants = _parse_variants(execution, base_modes)
    selector = _parse_selector(execution, variants)
    independent, warnings = _variant_independent_params(
        roles=roles,
        variants=variants,
        selector=selector,
    )
    return ExecutionProfile(
        strategy_id=strategy_id,
        engine=engine,
        modes=dict(base_modes),
        variants=variants,
        variant_selector=selector,
        parameter_defaults=defaults,
        parameter_roles=roles,
        variant_independent_params=independent,
        validation_warnings=warnings,
    )


def resolve_variant(profile: ExecutionProfile, params: Mapping[str, Any]) -> VariantSpec:
    """Resolve the active variant for a parameter set."""

    selector = profile.variant_selector
    if selector is None:
        if len(profile.variants) != 1:
            raise ProfileValidationError(
                f"{profile.strategy_id}: profile has multiple variants but no selector."
            )
        return next(iter(profile.variants.values()))

    if selector.param in params:
        raw_value = params[selector.param]
    elif selector.param in profile.parameter_defaults:
        raw_value = profile.parameter_defaults[selector.param]
    else:
        raise ProfileValidationError(
            f"{profile.strategy_id}: selector parameter '{selector.param}' missing "
            "from params and has no config default."
        )

    key = canonical_selector_key(raw_value)
    variant_name = selector.mapping.get(key)
    if variant_name is None:
        raise ProfileValidationError(
            f"{profile.strategy_id}: selector parameter '{selector.param}' value "
            f"{raw_value!r} maps to '{key}', which is not in variantSelector.mapping."
        )
    return profile.variants[variant_name]


def active_mode_values(profile: ExecutionProfile, params: Mapping[str, Any]) -> dict[str, str]:
    """Return mode field/value pairs for the resolved variant."""

    return dict(resolve_variant(profile, params).modes)


def active_parameter_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> set[str]:
    """Return parameter names active for semantic identity and candidate packing."""

    active = set(profile.variant_independent_params)
    modes = active_mode_values(profile, params)
    configured = set(profile.parameter_roles)
    for consumed in _consumed_params_for_modes(modes):
        if not configured or consumed in configured:
            active.add(consumed)
    return {
        name
        for name in active
        if profile.parameter_roles.get(name) != "runtime"
    }


def inactive_parameter_names(profile: ExecutionProfile, params: Mapping[str, Any]) -> set[str]:
    """Return configured non-runtime params inactive for the resolved variant."""

    candidates = {
        name
        for name, role in profile.parameter_roles.items()
        if role != "runtime"
    }
    return candidates - active_parameter_names(profile, params)


def mode_binding_for(mode_field: str, mode_value: Any) -> Optional[ModeBinding]:
    """Return binding metadata for a profile mode field/value pair."""

    return _binding_for(mode_field, mode_value)


__all__ = [
    "MODE_PARAMETER_BINDINGS",
    "ProfileValidationError",
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
