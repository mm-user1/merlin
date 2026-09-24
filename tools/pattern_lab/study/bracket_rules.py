"""Frozen linear-USDT quantity conversion, separate from snapshot provenance."""
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_CEILING
import math

from .. import PatternLabDataError, manifest


def decimal_text(value):
    text = format(Decimal(value), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


@dataclass(frozen=True)
class ExecutionRules:
    venue: str
    contract: str
    base_currency: str
    quote_currency: str
    settlement_currency: str
    quantity_unit: str
    quantity_step: str
    minimum_quantity: str
    base_step: str
    base_minimum: str
    minimum_lots: int
    minimum_notional: str | None
    ct_val: str | None
    ct_mult: str | None
    enforce_minimum_quantity: bool = True
    enforce_minimum_notional: bool = False

    def semantic(self):
        return asdict(self)


def normalize_rules(entry):
    where = f"{entry['instrument_id']}: bracket instrument_rules"
    rules = manifest.validate_instrument_rules(entry.get("instrument_rules"), where, venue=entry["venue"])
    step, minimum = Decimal(rules["quantity_step"]), Decimal(rules["minimum_quantity"])
    value, multiplier = None, None
    if entry["venue"] == "OKX":
        raw = rules["raw_contract_fields"]
        try:
            multiplier = Decimal(raw["ctMult"])
        except (KeyError, TypeError, ValueError, ArithmeticError):
            multiplier = Decimal("NaN")
        if not multiplier.is_finite() or multiplier != 1:
            raise PatternLabDataError(where + ": only finite unity ctMult is supported")
        value = Decimal(raw["ctVal"])
    factor = value if value is not None else Decimal(1)
    base_step, base_minimum = step * factor, minimum * factor
    for name, number in (("base step", base_step), ("base minimum", base_minimum)):
        if not math.isfinite(float(number)) or float(number) <= 0:
            raise PatternLabDataError(where + f": {name} is not representable as a positive float")
    minimum_lots = int((base_minimum / base_step).to_integral_value(rounding=ROUND_CEILING))
    if minimum_lots > 2**53:
        raise PatternLabDataError(where + ": minimum lot count is unrepresentable")
    notional = rules["minimum_notional"]
    if notional is not None and not math.isfinite(float(notional)):
        raise PatternLabDataError(where + ": minimum notional is unrepresentable")
    return ExecutionRules(
        entry["venue"], entry["contract"], rules["base_currency"], rules["quote_currency"],
        rules["settlement_currency"], rules["quantity_unit"], decimal_text(step), decimal_text(minimum),
        decimal_text(base_step), decimal_text(base_minimum), minimum_lots,
        None if notional is None else decimal_text(notional),
        None if value is None else decimal_text(value), None if multiplier is None else decimal_text(multiplier),
        enforce_minimum_notional=notional is not None)
