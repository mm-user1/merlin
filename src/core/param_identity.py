"""Display identity helpers for optimization parameter sets."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Mapping, Optional


DISPLAY_PARAM_ID_IGNORED_KEYS = {
    "dateFilter",
    "start",
    "end",
    "riskPerTrade",
    "commissionRate",
    "commissionPct",
    "initialCapital",
    "contractSize",
    "warmupBars",
    "date_filter",
    "risk_per_trade",
    "risk_per_trade_pct",
    "commission_rate",
    "commission_pct",
    "initial_capital",
    "contract_size",
    "warmup_bars",
    "use_backtester",
}


def canonical_strategy_params(
    params: Mapping[str, Any] | None,
    fixed_params: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return params suitable for compact display IDs.

    Runtime/execution fields are excluded so the same trading parameter
    combination keeps one display identity across WFA modules.
    """
    merged: Dict[str, Any] = {}
    if fixed_params:
        merged.update(dict(fixed_params))
    if params:
        merged.update(dict(params))

    canonical: Dict[str, Any] = {}
    for key, value in merged.items():
        if key in DISPLAY_PARAM_ID_IGNORED_KEYS:
            continue
        if key.endswith("_options"):
            continue
        canonical[key] = value
    return canonical


def create_display_param_id(
    params: Mapping[str, Any] | None,
    *,
    strategy_id: Optional[str] = None,
    strategy_config: Optional[Mapping[str, Any]] = None,
    fixed_params: Mapping[str, Any] | None = None,
    logger: Optional[logging.Logger] = None,
) -> str:
    """Create a compact, stable display ID for a trading parameter set."""
    canonical = canonical_strategy_params(params, fixed_params)
    param_str = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]

    config = strategy_config
    if config is None and strategy_id:
        try:
            from strategies import get_strategy_config

            config = get_strategy_config(strategy_id)
        except (ImportError, ValueError, KeyError, TypeError, AttributeError) as exc:
            if logger is not None:
                logger.warning(
                    "Falling back to hash-only param_id for strategy '%s': %s",
                    strategy_id,
                    exc,
                )
            return param_hash

    parameters = config.get("parameters", {}) if isinstance(config, Mapping) else {}
    preferred_pairs = [
        ("maType", "maLength"),
        ("maType3", "maLength3"),
        ("maType2", "maLength2"),
    ]
    for left, right in preferred_pairs:
        if left in canonical and right in canonical:
            return f"{canonical.get(left)} {canonical.get(right)}_{param_hash}"

    optimizable = []
    for param_name, param_spec in parameters.items():
        if not isinstance(param_spec, Mapping):
            continue
        optimize_cfg = param_spec.get("optimize", {})
        if isinstance(optimize_cfg, Mapping) and optimize_cfg.get("enabled", False):
            optimizable.append(param_name)
        if len(optimizable) == 2:
            break

    label_parts = [str(canonical.get(param_name, "?")) for param_name in optimizable]
    if label_parts:
        return f"{' '.join(label_parts)}_{param_hash}"
    return param_hash
