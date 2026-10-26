"""Deterministic synthetic signals for generic admission integration."""
import numpy as np

from core.engine_v2.contracts import Signals
from core.engine_v2.kernel import ExecutionData
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import run_v2_strategy

STRATEGY_ID = "test_risk_bracket_v1"
SIGNAL_CACHE_PARAM_NAMES = ("signalEvery",)
DATAPREP_CACHE_PARAM_NAMES = ("signalEvery", "stopLP")


def load_config():
    params = {}
    for name, default, kind in [("initialCapital", 1000., "float"), ("commissionPct", .05, "float"),
        ("riskPerTrade", 2., "float"), ("stopX", 1., "float"), ("stopLP", 2, "int"),
        ("stopRR", 2., "float"), ("stopMaxDays", 4., "float"), ("enableLong", True, "bool"),
        ("enableShort", False, "bool"), ("maxEntryLeverage", 8., "float")]:
        params[name] = dict(type=kind, default=default, role="execution", group="Risk", label=name,
                            optimize={"enabled": False})
    params["maxEntryLeverage"]["label"] = "Max Entry Leverage"
    params["stopX"]["optimize"] = dict(enabled=True, min=.5, max=2., step=.5)
    params["stopRR"]["optimize"] = dict(enabled=True, min=1., max=3., step=.5)
    params["signalEvery"] = dict(type="int", label="Signal every", default=12, role="signal", group="Signals",
        min=2, max=24, step=2, optimize=dict(enabled=True, min=10, max=12, step=2))
    params["warmupBars"] = dict(type="int", default=1000, min=100, max=5000, role="runtime", optimize={"enabled": False})
    return dict(id=STRATEGY_ID, name="Synthetic Risk Bracket", version="test-1", engine="v2",
        parameters=params, execution=dict(entryAdmission="risk_bracket_v1", entryOrder="market_next_open",
            stop="atr_swing", sizing="risk_per_trade", target="rr", trail="none", trailActivation="none",
            boundary="strict_close", margin="off", priceRounding="none", maxDays=True))


def load_profile():
    return parse_execution_profile(load_config())


def normalized_params(params=None):
    return {**{k: v["default"] for k, v in load_config()["parameters"].items()}, **(params or {})}


def build_v2_execution_data(df, params):
    n = len(df)
    interval = int(params.get("signalEvery", 12))
    slots = df.index.asi8 // (5 * 60 * 10**9)
    long = slots % (2 * interval) == 0
    short = slots % (2 * interval) == interval
    length = int(params.get("stopLP", 2))
    return ExecutionData(timestamps=df.index, open=df.Open.to_numpy(dtype=float), high=df.High.to_numpy(dtype=float),
        low=df.Low.to_numpy(dtype=float), close=df.Close.to_numpy(dtype=float), signals=Signals(long, short),
        atr=np.full(n, .5), rolling_low=df.Low.rolling(length).min().to_numpy(dtype=float),
        rolling_high=df.High.rolling(length).max().to_numpy(dtype=float), trail_long=np.full(n, np.nan),
        trail_short=np.full(n, np.nan))


class AdmissionTestStrategy:
    STRATEGY_ID = STRATEGY_ID
    STRATEGY_NAME = "Synthetic Risk Bracket"
    STRATEGY_VERSION = "test-1"

    @staticmethod
    def run(df, params, trade_start_idx=0, *, admission_rules=None):
        params = normalized_params(params)
        return run_v2_strategy(data=build_v2_execution_data(df, params), profile=load_profile(), params=params,
            trade_start_idx=trade_start_idx, admission_rules=admission_rules).strategy_result


def rule_entry(venue="BYBIT", step="0.1", minimum="0.1", notional="5"):
    if venue == "BYBIT":
        raw = dict(contractType="LinearPerpetual", baseCoin="AAA", quoteCoin="USDT", settleCoin="USDT",
            launchTime="1700000000000", status="Trading", qtyStep=step, minOrderQty=minimum,
            minNotionalValue=notional, tickSize="0.001")
        contract, unit, status = "AAAUSDT", "AAA", "Trading"
    else:
        raw = dict(instType="SWAP", ctType="linear", settleCcy="USDT", ctVal="0.1", ctValCcy="AAA",
            ctMult="1", lotSz=step, minSz=minimum, tickSz="0.001", listTime="1700000000000", state="live")
        contract, unit, status, notional = "AAA-USDT-SWAP", "contracts", "live", None
    return dict(instrument_id=venue + "_" + contract, venue=venue, contract=contract,
        instrument_rules=dict(schema_version=1, source_reference="synthetic", as_of_utc="2026-10-26T00:00:00Z",
            contract_type="linear_perpetual", base_currency="AAA", quote_currency="USDT", settlement_currency="USDT",
            quantity_unit=unit, quantity_step=step, minimum_quantity=minimum, price_tick="0.001",
            minimum_notional=notional, listed_at_utc="2023-11-14T22:13:20Z", trading_status=status, raw_contract_fields=raw))
