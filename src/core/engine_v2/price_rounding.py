"""Generic Backtester V2 order-price rounding helpers."""

from __future__ import annotations

import math


PRICE_ROUNDING_NONE = "none"
PRICE_ROUNDING_TICK_OUTWARD = "tick_outward"

_GRID_EPSILON = 1e-9


def validate_tick_size(tick_size: float) -> float:
    """Return a valid positive tick size or raise a clear error."""

    value = float(tick_size)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("tickSize must be a finite positive value when tick rounding is active.")
    return value


def round_to_tick_floor(price: float, tick_size: float) -> float:
    """Round a price down to the tick grid with an epsilon guard."""

    tick = validate_tick_size(tick_size)
    scaled = float(price) / tick
    return math.floor(scaled + _GRID_EPSILON) * tick


def round_to_tick_ceil(price: float, tick_size: float) -> float:
    """Round a price up to the tick grid with an epsilon guard."""

    tick = validate_tick_size(tick_size)
    scaled = float(price) / tick
    return math.ceil(scaled - _GRID_EPSILON) * tick


def round_level_outward(price: float, tick_size: float, *, below_market: bool) -> float:
    """Round an order level away from the current market side of the level."""

    if below_market:
        return round_to_tick_floor(price, tick_size)
    return round_to_tick_ceil(price, tick_size)


def _validate_direction(direction: int) -> int:
    value = int(direction)
    if value not in {-1, 1}:
        raise ValueError("direction must be -1 or 1 for outward price rounding.")
    return value


def round_stop_level(direction: int, price: float, tick_size: float) -> float:
    """Round an entry stop level outward for a long or short position."""

    side = _validate_direction(direction)
    return round_level_outward(price, tick_size, below_market=side > 0)


def round_target_level(direction: int, price: float, tick_size: float) -> float:
    """Round an entry target level outward for a long or short position."""

    side = _validate_direction(direction)
    return round_level_outward(price, tick_size, below_market=side < 0)


def round_trail_level(direction: int, price: float, tick_size: float) -> float:
    """Round a trailing stop or trail band level outward."""

    side = _validate_direction(direction)
    return round_level_outward(price, tick_size, below_market=side > 0)


__all__ = [
    "PRICE_ROUNDING_NONE",
    "PRICE_ROUNDING_TICK_OUTWARD",
    "round_level_outward",
    "round_stop_level",
    "round_target_level",
    "round_to_tick_ceil",
    "round_to_tick_floor",
    "round_trail_level",
    "validate_tick_size",
]
