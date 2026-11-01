"""Create a small deterministic example pack; never collects market data."""
from __future__ import annotations

import argparse
from pathlib import Path

from .config import REPO_ROOT  # bootstrap the canonical provider import
import numpy as np
import pandas as pd
from data_provider import data, manifest


def synthetic_rules(symbol="AAA", *, step="0.001", minimum="0.001", notional="0"):
    return dict(schema_version=1, source_reference="Strategy Lab synthetic fixture",
        as_of_utc="2026-11-01T00:00:00Z", contract_type="linear_perpetual",
        base_currency=symbol, quote_currency="USDT", settlement_currency="USDT",
        quantity_unit=symbol, quantity_step=step, minimum_quantity=minimum,
        price_tick="0.001", minimum_notional=notional, listed_at_utc="2023-11-14T22:13:20Z",
        trading_status="Trading", raw_contract_fields=dict(contractType="LinearPerpetual",
            baseCoin=symbol, quoteCoin="USDT", settleCoin="USDT", launchTime="1700000000000",
            status="Trading", qtyStep=step, minOrderQty=minimum, minNotionalValue=notional, tickSize="0.001"))


def publish(root, *, symbols=("AAA",), end="2024-06-02", rules=None, start="2024-01-28", change=None, drop=None):
    """Public provider publication into a new, caller-owned synthetic directory."""
    times = pd.date_range(start, end, freq="5min", tz="UTC", inclusive="left")
    slots = (times.asi8 - pd.Timestamp("2024-01-01", tz="UTC").value) // (300 * 10**9)
    opening = 100 + 8 * np.sin(slots / 30)
    close = 100 + 8 * np.sin((slots + 1) / 30)
    values = np.column_stack((opening, np.maximum(opening, close)+.4,
        np.minimum(opening, close)-.4, close, np.ones(len(slots))))
    if change is not None:
        values[int(change), 4] += 1  # A consumed quote-turnover change, with valid OHLC.
    if drop is not None:
        keep = np.arange(len(times)) != int(drop)
        times, values = times[keep], values[keep]
    sources = [data.InstrumentSource(symbol=symbol, venue="BYBIT", contract=symbol+"USDT",
        quote_currency="USDT", roles=["research_only"], timestamps=times.asi8 // 10**6,
        ohlcv=values, instrument_rules=(rules or {}).get(symbol, synthetic_rules(symbol)),
        source=manifest.build_source_metadata(input_format="synthetic", input_dtype="float64",
            source_reference="Strategy Lab deterministic sine", volume_unit_evidence="synthetic quote turnover"),
        verification=manifest.build_verification(volume_quote_verified=True,
            volume_quote_evidence="synthetic quote turnover", closed_before_utc=times[-1]+pd.Timedelta(minutes=5),
            closure_evidence="synthetic closed bars", closure_source="example")) for symbol in symbols]
    data.publish_pack(root, sources, universe=manifest.build_universe())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.output_dir.resolve()
    if root.is_relative_to(REPO_ROOT):
        parser.error("use a fresh external directory for synthetic input")
    publish(root)
    print(root)


if __name__ == "__main__":
    main()
