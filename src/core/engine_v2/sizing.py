"""Risk sizing helpers for Backtester V2."""

from __future__ import annotations

import math


def risk_position_size(
    *,
    balance: float,
    risk_distance: float,
    risk_per_trade_pct: float,
    contract_size: float,
) -> float:
    """Return contract-rounded size for percentage-risk sizing."""

    if (
        not math.isfinite(balance)
        or not math.isfinite(risk_distance)
        or not math.isfinite(risk_per_trade_pct)
        or not math.isfinite(contract_size)
        or risk_distance <= 0.0
        or risk_per_trade_pct <= 0.0
        or contract_size <= 0.0
    ):
        return 0.0

    risk_cash = balance * risk_per_trade_pct / 100.0
    raw_size = risk_cash / risk_distance
    return math.floor(raw_size / contract_size) * contract_size


__all__ = ["risk_position_size"]
