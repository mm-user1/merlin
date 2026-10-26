"""Risk-bracket execution context and selected-run entry diagnostics."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal, localcontext
from fractions import Fraction
import json
import math
from typing import Any, Mapping
import numpy as np

from core.backtest_engine import StrategyResult

ADMISSION_MODE = "risk_bracket_v1"


def uses_admission(config):
    return (config.get("execution") or {}).get("entryAdmission") == ADMISSION_MODE


def run_strategy(strategy_class, frame, params, trade_start_idx=0, *, admission_rules=None):
    """Application adapter; legacy strategies retain their existing run entry point."""
    from strategies import get_strategy_config
    strategy_id = getattr(strategy_class, "STRATEGY_ID", None)
    if strategy_id is None:
        return strategy_class.run(frame, params, trade_start_idx)
    config = get_strategy_config(strategy_id)
    if not uses_admission(config):
        return strategy_class.run(frame, params, trade_start_idx)
    import importlib
    from .profile import parse_execution_profile
    from .runner import run_v2_strategy
    from core.grid_v2 import GridV2StrategyHooks
    hooks = GridV2StrategyHooks.from_strategy(importlib.import_module(strategy_class.__module__))
    normalized = hooks.normalize_params(dict(params)) if hooks.normalize_params else dict(params)
    return run_v2_strategy(data=hooks.build_execution_data(frame, normalized),
        profile=parse_execution_profile(config), params=normalized, trade_start_idx=trade_start_idx,
        admission_rules=admission_rules).strategy_result


def positive_cap(value: Any) -> float:
    try:
        if value is None or isinstance(value, (bool, np.bool_)):
            raise ValueError
        cap = float(value)
        if not math.isfinite(cap) or cap <= 0:
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("maxEntryLeverage must be a finite positive number.") from exc
    return cap


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


@dataclass(frozen=True)
class AdmissionRules:
    """Serializable, immutable effective rules with the validated source snapshot."""

    instrument_id: str
    venue: str
    contract: str
    base_currency: str
    quote_currency: str
    settlement_currency: str
    quantity_unit: str
    base_step: str
    base_minimum: str
    minimum_lots: int
    minimum_notional: str | None
    ct_val: str | None
    ct_mult: str | None
    source_rules_json: str

    @classmethod
    def from_entry(cls, entry: Mapping[str, Any]) -> AdmissionRules:
        from data_provider.errors import DataError
        from data_provider.manifest import validate_instrument_rules

        where = f"{entry.get('instrument_id')}: risk-bracket instrument_rules"
        venue = entry.get("venue")
        try:
            rules = validate_instrument_rules(entry.get("instrument_rules"), where, venue=venue)
        except DataError as exc:
            raise ValueError(str(exc)) from exc
        step, minimum = Decimal(rules["quantity_step"]), Decimal(rules["minimum_quantity"])
        value = multiplier = None
        if venue == "OKX":
            raw = rules["raw_contract_fields"]
            try:
                multiplier = Decimal(raw["ctMult"])
            except (KeyError, TypeError, ValueError, ArithmeticError):
                multiplier = Decimal("NaN")
            if not multiplier.is_finite() or multiplier != 1:
                raise ValueError(where + ": only finite unity ctMult is supported.")
            value = Decimal(raw["ctVal"])
        elif venue != "BYBIT":
            raise ValueError(where + ": only BYBIT and OKX linear USDT rules are supported.")
        factor = value if value is not None else Decimal(1)
        with localcontext() as context:
            context.prec = max(len(step.as_tuple().digits), len(minimum.as_tuple().digits)) + len(factor.as_tuple().digits)
            base_step, base_minimum = step * factor, minimum * factor
        for label, number in (("base step", base_step), ("base minimum", base_minimum)):
            if not math.isfinite(float(number)) or float(number) <= 0:
                raise ValueError(where + f": {label} is not representable as a positive float.")
        ratio = Fraction(base_minimum) / Fraction(base_step)
        lots = -(-ratio.numerator // ratio.denominator)
        if not 1 <= lots <= 2**53:
            raise ValueError(where + ": minimum lot count is unrepresentable.")
        notional = rules["minimum_notional"]
        if notional is not None and (not math.isfinite(float(notional)) or float(notional) < 0
                                     or (Decimal(notional) != 0 and float(notional) == 0)):
            raise ValueError(where + ": minimum notional is unrepresentable.")
        for key in ("instrument_id", "contract"):
            if not isinstance(entry.get(key), str) or not entry[key].strip():
                raise ValueError(where + f": missing instrument {key}.")
        if entry["instrument_id"] != f"{venue}_{entry['contract']}":
            raise ValueError(where + ": instrument identity disagrees with venue/contract.")
        return cls(entry["instrument_id"], venue, entry["contract"], rules["base_currency"],
            rules["quote_currency"], rules["settlement_currency"], rules["quantity_unit"],
            _decimal_text(base_step), _decimal_text(base_minimum), lots,
            None if notional is None else _decimal_text(Decimal(notional)),
            None if value is None else _decimal_text(value), None if multiplier is None else _decimal_text(multiplier),
            json.dumps(rules, sort_keys=True, separators=(",", ":")))

    def semantic(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "source_rules_json"}

    def validated(self) -> AdmissionRules:
        rebuilt = type(self).from_entry(dict(instrument_id=self.instrument_id, venue=self.venue,
            contract=self.contract, instrument_rules=json.loads(self.source_rules_json)))
        if asdict(rebuilt) != asdict(self):
            raise ValueError("Admission rules disagree with their validated source snapshot.")
        return self


def admission_cap(profile, params) -> float | None:
    """Validate candidate parameters without resolving or normalizing rules."""
    if profile.modes.get("entryAdmission") != ADMISSION_MODE:
        return None
    if "contractSize" in params:
        raise ValueError("risk_bracket_v1 does not accept contractSize; instrument rules own quantity sizing.")
    if "stopMaxPct" in params and "stopMaxPct" not in profile.parameter_names:
        raise ValueError("Undeclared stopMaxPct is not allowed in risk_bracket_v1.")
    if "stopMaxPct" in profile.parameter_names:
        value = params.get("stopMaxPct", profile.parameter_defaults.get("stopMaxPct"))
        if isinstance(value, bool) or value is None or not math.isfinite(float(value)):
            raise ValueError("Declared stopMaxPct must be finite.")
    return positive_cap(params.get("maxEntryLeverage", 8))


def execution_admission(profile, params, rules: AdmissionRules | None) -> tuple[float, AdmissionRules] | None:
    cap = admission_cap(profile, params)
    if cap is None:
        return None
    if not isinstance(rules, AdmissionRules):
        raise ValueError("risk_bracket_v1 requires validated Parquet instrument rules.")
    return cap, rules.validated()


def admission_envelope(rules: AdmissionRules, cap: Any) -> dict[str, Any]:
    return dict(version=1, mode=ADMISSION_MODE, maxEntryLeverage=positive_cap(cap), rules=asdict(rules.validated()))


def read_admission_envelope(value: Any, cap: Any) -> AdmissionRules:
    try:
        if not isinstance(value, dict) or set(value) != {"version", "mode", "maxEntryLeverage", "rules"}:
            raise ValueError
        if type(value["version"]) is not int or value["version"] != 1 or value["mode"] != ADMISSION_MODE:
            raise ValueError
        if positive_cap(value["maxEntryLeverage"]) != positive_cap(cap):
            raise ValueError("maxEntryLeverage disagrees with saved admission context.")
        return AdmissionRules(**value["rules"]).validated()
    except (ValueError, TypeError, KeyError) as exc:
        raise ValueError(f"Invalid saved entry_admission context: {exc}") from exc


@dataclass(frozen=True)
class AdmissionEvent:
    fill_time_ns: int
    trade_index: int | None  # None denotes a cap refusal, not an accepted fill.
    leverage: float | None


def summarize_entries(events, scope="execution") -> dict[str, Any]:
    fills = [e.leverage for e in events if e.trade_index is not None]
    return dict(version=1, scope=scope, maxEntryLeverageUsed=max(fills) if fills else None,
                leverageCapSkippedEntries=sum(e.trade_index is None for e in events))


@dataclass
class AdmissionStrategyResult(StrategyResult):
    entry_diagnostics: dict[str, Any] | None = None
    entry_events: tuple[AdmissionEvent, ...] = field(default_factory=tuple)


def result_diagnostics(result, scope):
    value = getattr(result, "entry_diagnostics", None)
    return {**value, "scope": scope} if value is not None else None


def retained_events(result, trades, cutoff):
    """Preserve retained-trade semantics and inclusive refusal cutoff in Adaptive WFA."""
    if not isinstance(result, AdmissionStrategyResult):
        return {}
    indices = {id(trade): index for index, trade in enumerate(trades)}
    original = result.trades
    events = []
    for event in result.entry_events:
        if event.trade_index is None:
            if event.fill_time_ns <= cutoff.value:
                events.append(event)
        elif event.trade_index < len(original) and id(original[event.trade_index]) in indices:
            events.append(AdmissionEvent(event.fill_time_ns, indices[id(original[event.trade_index])], event.leverage))
    return dict(entry_events=tuple(events), entry_diagnostics=summarize_entries(events, "oos"))


def aggregate_entries(executions, scope="oos"):
    """Unique execution IDs only; incomplete or invalid selections are unavailable."""
    unique = dict(executions)
    if not unique or any(value is None for value in unique.values()):
        return None
    maxima, skipped = [], 0
    for value in unique.values():
        if (not isinstance(value, dict) or set(value) != {"version", "scope", "maxEntryLeverageUsed", "leverageCapSkippedEntries"}
                or value.get("scope") not in {"execution", "is", "ft", "oos", "oos_test", "manual"}
                or type(value.get("version")) is not int or value["version"] != 1
                or type(value.get("leverageCapSkippedEntries")) is not int or value["leverageCapSkippedEntries"] < 0):
            return None
        maximum = value.get("maxEntryLeverageUsed")
        if maximum is not None:
            if isinstance(maximum, bool) or not isinstance(maximum, (int, float)) or not math.isfinite(maximum) or maximum <= 0:
                return None
            maxima.append(maximum)
        skipped += value["leverageCapSkippedEntries"]
    return dict(version=1, scope=scope, maxEntryLeverageUsed=max(maxima) if maxima else None,
                leverageCapSkippedEntries=skipped)
