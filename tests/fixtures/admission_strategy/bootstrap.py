"""Test/preview registry bootstrap; no production discovery or execution replacement."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from .strategy import AdmissionTestStrategy, STRATEGY_ID, load_config


def register():
    import strategies
    previous = strategies._REGISTRY.get(STRATEGY_ID)
    if previous is not None and previous.get("class") is not AdmissionTestStrategy:
        raise ValueError("Admission fixture ID collides with an existing strategy.")
    strategies._REGISTRY[STRATEGY_ID] = dict(
        config=load_config(), **{"class": AdmissionTestStrategy}, path=Path(__file__).parent)
    return previous


def _worker_initialize(initializer, initargs):
    register()
    if initializer is not None:
        initializer(*initargs)


@contextmanager
def installed():
    """Only post_process's local context gets a registration-first Pool initializer."""
    import multiprocessing
    import strategies
    from core import post_process
    previous = register()
    original_mp = post_process.mp
    real_context = multiprocessing.get_context("spawn")

    def pool(*args, initializer=None, initargs=(), **kwargs):
        return real_context.Pool(*args, initializer=_worker_initialize,
                                 initargs=(initializer, initargs), **kwargs)

    def get_context(method):
        if method != "spawn":
            raise AssertionError("Admission workers must use real spawn.")
        return SimpleNamespace(Pool=pool)

    post_process.mp = SimpleNamespace(get_context=get_context)
    try:
        yield
    finally:
        post_process.mp = original_mp
        if previous is None:
            strategies._REGISTRY.pop(STRATEGY_ID, None)
        else:
            strategies._REGISTRY[STRATEGY_ID] = previous


def publish(root, *, rows=3000, venue="BYBIT", step="0.1", minimum="0.1", turn_day=None):
    import numpy as np
    import pandas as pd
    from data_provider import data, manifest
    from .strategy import rule_entry
    entry = rule_entry(venue, step, minimum)
    slots = np.arange(rows, dtype=np.int64)
    timestamps = pd.Timestamp("2024-01-01", tz="UTC").value // 10**6 + slots * 300000
    price = 100 + slots * .05
    if turn_day is not None:
        price = 100 + np.abs(slots - int(turn_day * 288)) * .05
    values = np.column_stack((price, price + .6, price - .6, price + .2, np.ones(rows)))
    source = data.InstrumentSource(symbol="AAA", venue=venue, contract=entry["contract"],
        quote_currency="USDT", roles=["trading"], timestamps=timestamps, ohlcv=values,
        instrument_rules=entry["instrument_rules"],
        source=manifest.build_source_metadata(input_format="synthetic", input_dtype="float64",
            source_reference="Merlin C synthetic", volume_unit_evidence="generated quote turnover"),
        verification=manifest.build_verification(volume_quote_verified=True,
            volume_quote_evidence="generated quote turnover",
            closed_before_utc=manifest.format_epoch_ms(int(timestamps[-1]) + 300000),
            closure_evidence="synthetic closed bars", closure_source="test"))
    data.publish_pack(root, [source], universe=manifest.build_universe())
    return dict(type="parquet", version=1, pack_root=str(root), instrument_id=entry["instrument_id"], timeframe_minutes=5)


def grid_payload():
    from .strategy import normalized_params
    params = normalized_params()
    params.pop("warmupBars")
    params["dateFilter"] = False
    return dict(optimization_mode="grid", enabled_params={"signalEvery": True},
        param_ranges={"signalEvery": [10, 12, 2]}, param_types={"signalEvery": "int"},
        fixed_params=params, objectives=["net_profit_pct"], grid_fast_objectives=["net_profit_pct"],
        grid_top_candidates=2, grid_slow_refinement_enabled=True, grid_slow_objectives=["net_profit_pct"],
        worker_processes=2)
