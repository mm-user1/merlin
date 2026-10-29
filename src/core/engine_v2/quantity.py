"""Opt-in instrument quantity rules, independent of risk-bracket admission."""
from dataclasses import asdict
import math

from .admission import ADMISSION_MODE, AdmissionRules

QUANTITY_MODE = "instrument_rules_v1"


def profile_policy(profile):
    if profile.engine != "v2" or "initialCapital" not in profile.parameter_names:
        return None
    modes = [variant.modes for variant in profile.variants.values()]
    if not modes or any(m.get("boundary") != "strict_close" or m.get("priceRounding", "none") != "none" for m in modes):
        return None
    if all(m.get("entryAdmission") == ADMISSION_MODE for m in modes):
        return ADMISSION_MODE
    if (profile.parameter_roles.get("contractSize") == "execution"
            and all(m.get("entryAdmission") is None and m.get("sizing") in {"risk_per_trade", "fixed_pct_equity"} for m in modes)):
        return QUANTITY_MODE
    return None


def selector_capability(config):
    from .profile import parse_execution_profile
    if config.get("engine") != "v2":
        return None
    policy = profile_policy(parse_execution_profile(config))
    if policy == QUANTITY_MODE and config["parameters"]["contractSize"].get("optimize", {}).get("enabled", False):
        return None
    return {"policy": policy} if policy else None


def execution_quantity(profile, rules, admission_rules=None):
    if rules is None:
        return None
    if admission_rules is not None or profile is None or profile_policy(profile) != QUANTITY_MODE:
        raise ValueError("Quantity execution requires a compatible legacy V2 profile without admission context.")
    if not isinstance(rules, AdmissionRules):
        raise ValueError("Quantity execution requires validated Parquet instrument rules.")
    return rules.validated()


def quantity_envelope(rules):
    return dict(version=1, mode=QUANTITY_MODE, rules=asdict(rules.validated()))


def read_quantity_envelope(value):
    try:
        if (not isinstance(value, dict) or set(value) != {"version", "mode", "rules"}
                or type(value["version"]) is not int or value["version"] != 1 or value["mode"] != QUANTITY_MODE):
            raise ValueError("expected version-1 instrument_rules_v1 envelope")
        return AdmissionRules(**value["rules"]).validated()
    except (ValueError, TypeError, KeyError) as exc:
        raise ValueError(f"Invalid saved quantity_execution context: {exc}") from exc


def validate_kernel_quantity(config):
    rules = config.quantity_rules
    if rules is None:
        return None
    if (not isinstance(rules, AdmissionRules) or config.boundary_mode != "strict_close"
            or getattr(config, "price_rounding_mode", "none") != "none"
            or getattr(config, "entry_admission", None) is not None
            or getattr(config, "admission_rules", None) is not None):
        raise ValueError("Incompatible quantity execution context.")
    if not math.isfinite(config.initial_capital):
        raise ValueError("Nonfinite quantity-execution account capital.")
    return rules.validated()


def check_lot_ratio(ratio):
    if not math.isfinite(ratio) or ratio > 2**53:
        raise ValueError("Unrepresentable planned quantity/lot count.")


def entry_notional_allowed(rules, price, size, commission_rate, balance):
    notional = abs(price * size)
    fee = notional * commission_rate
    if not all(math.isfinite(x) for x in (notional, fee, balance - fee)):
        raise ValueError("Nonfinite quantity-execution fill arithmetic.")
    return rules.minimum_notional is None or notional >= float(rules.minimum_notional)
