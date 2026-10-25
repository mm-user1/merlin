"""Public-API synthetic market packs for Merlin tests."""
import numpy as np
import pandas as pd
from data_provider import data, manifest


def hold_pack(root, ready, release):
    from data_provider.pack_lock import pack_guard
    with pack_guard(root):
        ready.set()
        if not release.wait(60):
            raise RuntimeError("Lock probe release timed out.")


def publish(root, *, rows=1200, drop=(), change=None, triangle=False):
    slots = np.array([i for i in range(rows) if i not in drop], dtype=np.int64)
    timestamps = pd.Timestamp("2024-01-01", tz="UTC").value // 10**6 + slots * 300000
    price = 100 + (slots % 97) / 8
    if triangle:
        price = 100 + np.minimum(slots % 192, 192 - slots % 192) / 8
    values = np.column_stack((price, price + 2, price - 2, price + .5, (slots % 13) / 4))
    if change is not None:
        values[change, 4] += .25
    source = data.InstrumentSource(
        symbol="AAA", venue="TEST", contract="AAA-USDT-SWAP", quote_currency="USDT",
        roles=["trading"], timestamps=timestamps, ohlcv=values,
        source=manifest.build_source_metadata(input_format="synthetic", input_dtype="float64",
            source_reference="Merlin synthetic test", volume_unit_evidence="generated quote turnover"),
        verification=manifest.build_verification(volume_quote_verified=True,
            volume_quote_evidence="generated quote turnover",
            closed_before_utc=manifest.format_epoch_ms(int(timestamps[-1]) + 300000),
            closure_evidence="synthetic closed bars", closure_source="test"))
    data.publish_pack(root, [source], universe=manifest.build_universe())
    return {"type": "parquet", "version": 1, "pack_root": str(root),
            "instrument_id": "TEST_AAA-USDT-SWAP", "timeframe_minutes": 5}



STRATEGY = "s03_reversal_v11_regime_er_b2"

def grid_payload():
    from strategies import get_strategy_config
    defaults = {k: v.get("default") for k, v in get_strategy_config(STRATEGY)["parameters"].items()}
    defaults.pop("warmupBars", None)
    defaults.update(dateFilter=False, useRegime=False, useEmergencySL=False, maLength3=10)
    return {"optimization_mode": "grid", "enabled_params": {"maLength3": True},
            "param_ranges": {"maLength3": [8, 10, 2]}, "param_types": {"maLength3": "int"},
            "fixed_params": defaults, "objectives": ["net_profit_pct"],
            "grid_fast_objectives": ["net_profit_pct"], "grid_top_candidates": 2,
            "grid_slow_refinement_enabled": True, "grid_slow_objectives": ["net_profit_pct"],
            "worker_processes": 2}

