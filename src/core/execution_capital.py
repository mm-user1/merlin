"""Resolve the execution balance separately from advanced metric anchors."""
import math


def resolve_execution_capital(strategy_id, params):
    # Lazy import: strategy base classes themselves depend on core execution.
    from strategies import get_strategy_config

    parameters = get_strategy_config(strategy_id).get("parameters", {})
    if "initialCapital" not in parameters:
        if strategy_id == "s01_trailing_ma":
            return 100.0  # S01 v26 has fixed capital and ignores extraneous keys.
        raise ValueError(f"{strategy_id}: no declared execution capital contract.")
    spec = parameters["initialCapital"]
    value = params["initialCapital"] if "initialCapital" in params else (
        spec.get("default") if isinstance(spec, dict) else None
    )
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        capital = float(value)
        if not math.isfinite(capital) or capital <= 0:
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{strategy_id}: initialCapital must be finite and positive.") from exc
    return capital
