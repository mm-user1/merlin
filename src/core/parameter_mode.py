"""Fixed research request validation, separate from strategy and plan identity."""

from typing import Any, Mapping


def parameter_mode(payload: Mapping[str, Any]) -> str:
    if "parameterMode" not in payload:
        return "search"
    value = payload["parameterMode"]
    if not isinstance(value, str) or value not in ("search", "fixed"):
        raise ValueError("V2_INVALID_PARAMETER_MODE: parameterMode must be 'search' or 'fixed'; omit it for legacy search.")
    return value


def normalize_parameter_mode(payload: Mapping[str, Any], strategy_config: Mapping[str, Any], *, execution_capital=None) -> dict:
    """Refuse active search intent and normalize inert values before parsing them."""
    mode = parameter_mode(payload)
    if mode != "fixed":
        return dict(payload)
    from .grid_v2 import _coerce_bool

    def refuse(message):
        raise ValueError(f"V2_INVALID_PARAMETER_MODE: Fixed mode {message}")

    if strategy_config.get("engine") != "v2" or str(payload.get("optimization_mode", "")).strip().lower() != "grid":
        refuse("requires a V2 strategy and optimization_mode='grid'.")
    axes = payload.get("enabled_params", {})
    if not isinstance(axes, dict) or any(_coerce_bool(value) for value in axes.values()):
        refuse("requires all optimization axes disabled.")
    if payload.get("param_ranges", {}):
        refuse("does not accept parameter ranges.")
    fixed = payload.get("fixed_params", {})
    if not isinstance(fixed, dict) or any(str(key).endswith("_options") for key in fixed):
        refuse("requires scalar fixed_params without option subsets.")
    post = payload.get("postProcess") or {}
    if not isinstance(post, dict):
        refuse("requires postProcess to be an object.")
    stress = post.get("stressTest") or {}
    score = payload.get("score_config") or {}
    grid = payload.get("grid_config") or {}
    if not all(isinstance(value, dict) for value in (stress, score, grid)):
        refuse("requires object-valued stress, score and Grid settings.")
    flags = {
        "Forward Test": [post.get("enabled", False)],
        "DSR": [post.get("dsrEnabled", False), payload.get("grid_needs_dsr", False)],
        "Stress Test": [stress.get("enabled", False)],
        "Adaptive WFA": [payload.get("wf_adaptive_mode", False), payload.get("adaptive_mode", False)],
        "Score filter": [score.get("filter_enabled", False)],
        "Net Profit filter": [payload.get("filter_min_profit", False)],
        "Slow refinement": [payload.get("grid_slow_refinement_enabled", False), payload.get("gridSlowRefinementEnabled", False), grid.get("slow_refinement_enabled", False)],
    }
    for name, values in flags.items():
        if any(_coerce_bool(value) for value in values):
            refuse(f"does not support {name}; disable it.")
    constraints = payload.get("constraints") or []
    if not isinstance(constraints, list) or not all(isinstance(item, dict) for item in constraints):
        refuse("requires a constraints array.")
    if any(_coerce_bool(item.get("enabled", False)) for item in constraints):
        refuse("does not support enabled constraints.")
    result = dict(payload)
    result.update(
        enabled_params={}, param_ranges={}, objectives=["net_profit_pct"],
        primary_objective="net_profit_pct", grid_fast_objectives=["net_profit_pct"],
        grid_fast_primary_objective="net_profit_pct", grid_top_candidates=1,
        grid_diversity_enabled=False, grid_diversity_max_per_group=2,
        grid_v2_planning_policy="full", grid_budget=200000, grid_seed=42,
        grid_allocation_method="auto_sqrt_space", grid_min_quota=0.10,
        grid_manual_percents={}, grid_slow_objectives=["net_profit_pct"],
        grid_slow_primary_objective="net_profit_pct", grid_slow_refinement_enabled=False,
        postProcess={"enabled": False, "dsrEnabled": False, "stressTest": {"enabled": False}},
        constraints=[], filter_min_profit=False,
    )
    params, modes = resolve_fixed_scalars(strategy_config, fixed,
        payload.get("grid_v2_enabled_tie_groups", []), execution_capital=execution_capital)
    for requested in (payload.get("grid_enabled_modes"), payload.get("gridEnabledModes"), grid.get("enabled_modes")):
        if requested and requested != modes:
            refuse(f"grid_enabled_modes conflicts with scalar selector; expected {modes}.")
    result.update(fixed_params=params, grid_enabled_modes=modes)
    return result


def resolve_fixed_scalars(config, supplied, ties, *, execution_capital=None):
    from .engine_v2.profile import parse_execution_profile, canonical_selector_key
    from .engine_v2.parameter_ties import enabled_parameter_ties
    from .grid_v2 import _coerce_value, _canonicalize_globally_inactive_params, _selector_values_by_variant, _inactive_dependency_children

    profile = parse_execution_profile(config)
    specs = config["parameters"]
    params = {name: spec.get("default") for name, spec in specs.items()
              if profile.parameter_roles.get(name) != "runtime"}
    params.update(supplied)
    if profile.modes.get("entryAdmission"):
        from .engine_v2.admission import positive_cap
        params["maxEntryLeverage"] = positive_cap(params["maxEntryLeverage"])
    # Dependency and selector resolution must see canonical booleans, not strings.
    for name, spec in specs.items():
        if spec.get("type") == "bool" and profile.parameter_roles.get(name) != "runtime":
            params[name] = _coerce_value(params[name], "bool")
    selector = profile.variant_selector
    if selector:
        value = _coerce_value(params[selector.param], specs[selector.param].get("type", "select"))
        variant = selector.mapping.get(canonical_selector_key(value))
        if variant is None:
            raise ValueError(f"Fixed selector '{selector.param}' has an undeclared value: {value!r}.")
        selected = (variant,)
        modes = [variant] if selector.user_facing else []
    else:
        selected = tuple(profile.variants)
        modes = list(selected)
    params = _canonicalize_globally_inactive_params(specs, profile, params,
        _selector_values_by_variant(config, profile), selected)
    for name in _inactive_dependency_children(config, params):
        params[name] = specs[name].get("default")
    for source, target in enabled_parameter_ties(config, ties):
        params[target] = params[source]
    for name, spec in specs.items():
        if profile.parameter_roles.get(name) == "runtime":
            continue
        if name == "initialCapital" and execution_capital is not None:
            from .portfolio import positive_capital
            params[name] = positive_capital(execution_capital)
            continue
        try:
            value = _coerce_value(params[name], spec.get("type", "float"))
            options = spec.get("options")
            if options is not None and value not in options:
                raise ValueError(f"must be one of {options}")
            if "min" in spec and value < spec["min"] or "max" in spec and value > spec["max"]:
                raise ValueError("outside declared min/max")
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise ValueError(f"Fixed parameter '{name}': {exc}") from exc
        params[name] = value
    return params, modes


def require_singleton(plan):
    if plan.deduped_candidate_count != 1:
        raise ValueError("Fixed mode requires exactly one configuration; conflicting modes/logical blocks produced "
                         f"{plan.deduped_candidate_count} candidates.")
