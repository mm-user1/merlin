"""Explicit absence of legacy inference in the descriptive contract."""

REASON = "strategy_lab_descriptive_v1 requests descriptive OOS-block results only"


def not_requested():
    return {"status": "not_requested", "reason": REASON}


def interval_not_requested():
    return {**not_requested(), "lower": None, "upper": None, "width": None}


def random_settings(contract):
    if contract.descriptive:
        return {"seed": contract.seed, "resamples": contract.random_control_draws}
    return contract.evidence.get("uncertainty", {})
