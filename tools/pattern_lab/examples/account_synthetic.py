"""Run from repository root: python -m tools.pattern_lab.examples.account_synthetic.

Generated 5m candles only; no packs, files, networks or automatic workers.
"""
from dataclasses import asdict
import json
import numpy as np

from tools.pattern_lab import data
from tools.pattern_lab.accounts import (PreparedInput, AccountFeatureRequest, HARecipe,
    AccountWindow, ExecutionPolicy, prepare_inputs, build_commands, execute_account)
from tools.pattern_lab.study.bracket_rules import normalize_rules


def run():
    timestamps = np.arange(144, dtype=np.int64)*300000
    price = 100 + np.arange(144, dtype=np.float64)/10
    values = np.column_stack((price, price+3, price-1, price+2, np.ones(144)))
    header_args = dict(instrument_id="BYBIT_AAAUSDT", venue="BYBIT", contract="AAAUSDT",
        quote_currency="USDT", timeframe_minutes=30, start_ms=0, end_ms=43200000, warmup_start_ms=0)
    target = PreparedInput("target", data.prepare_series(timestamps=timestamps, ohlcv=values, **header_args),
                           data.fingerprint_header(**header_args))
    prepared = prepare_inputs(target, (), (AccountFeatureRequest("ha", "ha", "1", "target", {}, 0),))
    commands = build_commands(prepared, HARecipe("ha", entryRunLength=2), mode="signalManaged", stopUpdates=True)
    raw_rules = dict(schema_version=1, source_reference="synthetic-example", as_of_utc="2026-10-18T00:00:00Z",
        contract_type="linear_perpetual", base_currency="AAA", quote_currency="USDT", settlement_currency="USDT",
        quantity_unit="AAA", quantity_step="0.1", minimum_quantity="0.1", price_tick="0.001", minimum_notional="5",
        listed_at_utc="2023-11-14T22:13:20Z", trading_status="Trading",
        raw_contract_fields=dict(contractType="LinearPerpetual", baseCoin="AAA", quoteCoin="USDT", settleCoin="USDT",
            launchTime="1700000000000", status="Trading", qtyStep="0.1", minOrderQty="0.1",
            minNotionalValue="5", tickSize="0.001"))
    rules = normalize_rules(dict(instrument_id="BYBIT_AAAUSDT", venue="BYBIT", contract="AAAUSDT", instrument_rules=raw_rules))
    policy = ExecutionPolicy("signalManaged", "both", None, True, None)
    return execute_account(prepared, commands, policy, AccountWindow(0, 43200000), rules)


if __name__ == "__main__":
    result = run()
    print(json.dumps(dict(schema=result.schema, identity=result.identity, trades=len(result.trades),
                          finalBalance=result.finalBalance, coverage=asdict(result.coverage)), sort_keys=True))
