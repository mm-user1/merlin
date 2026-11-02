"""Provider-published filter candles with a single global origin for exact slices."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from tools.strategy_lab.config import REPO_ROOT
from tools.strategy_lab.synthetic_parquet import publish, synthetic_rules
from _fixed_helpers import request, save_request, snapshot


def editor(root=None, *, version=2, n=3, m=2, timeframe=240):
    items = [] if root is None else [{"id": "regime", "type": "candleDirection", "version": version,
        "enabled": True, "parameters": {"confirmationBars": n, **({"breakBars": m} if version == 2 else {})},
        "source": {"type": "parquet", "version": 1, "pack_root": str(root),
            "instrument_id": "BYBIT_BBBUSDT", "timeframe_minutes": timeframe}}]
    return {"version": 1, "enabled": root is not None, "items": items}


def fixed_request(root=None, **kwargs):
    raw = request()
    raw["schema_version"] = "strategy_lab_fixed_runspec_v2"
    raw["generation"]["execution"].pop("entry_filters")
    raw["generation"]["execution"]["entryFilters"] = editor(root, **kwargs)
    raw["generation"]["periods"][0].update(start="2024-03-01", end="2024-03-31")
    return raw


def is_request(root=None, **kwargs):
    raw = json.loads((REPO_ROOT / "tools/strategy_lab/runspecs/s07_off_example.json").read_text())
    raw["schema_version"] = "strategy_lab_runspec_v3"
    g = raw["generation"]
    g["execution"] = {"policy": "risk_bracket_v1", "entryFilters": editor(root, **kwargs)}
    g["market_data"]["timeframe_minutes"] = 60
    g["windows"].update(timeframe_minutes=60, requested_end="2024-05-01", expected_window_count=2)
    g["planning"].update(enabled_axes=["stopLP"], axis_values={"stopLP": [2, 4]}, expected_candidate_count=2)
    raw["preregistration"]["analysis_scopes"][0]["window_numbers"] = [1, 2]
    raw["preregistration"]["random_control_draws"] = 3
    return raw


def publish_filter(root, *, start="2024-01-10", end="2024-05-02", drop=None, change=None, closed=None):
    from data_provider import data, manifest
    # Every call builds the same global array; a later start is a true slice.
    times = pd.date_range("2023-12-01", end, freq="5min", tz="UTC", inclusive="left")
    cut = pd.Timestamp("2024-02-20", tz="UTC")
    blocks = (times.asi8 - cut.value) // pd.Timedelta(hours=4).value
    step = np.where(blocks < 0, 1, np.where(blocks % 2 == 0, -1, 1)) * .001
    close = 100 + np.cumsum(step)
    opening = close - step
    values = np.column_stack((opening, np.maximum(opening, close) + .01,
        np.minimum(opening, close) - .01, close, np.ones(len(times))))
    if change is not None:
        values[times == pd.Timestamp(change), 4] = 2.0
    mask = times >= pd.Timestamp(start, tz="UTC")
    if drop is not None:
        first, last = (pd.Timestamp(v) for v in drop)
        mask &= ~((times >= first) & (times < last))
    source = data.InstrumentSource(symbol="BBB", venue="BYBIT", contract="BBBUSDT", quote_currency="USDT",
        roles=["research_only"], timestamps=times.asi8[mask] // 10**6, ohlcv=values[mask].copy(),
        instrument_rules=synthetic_rules("BBB"), source=manifest.build_source_metadata(input_format="synthetic",
            input_dtype="float64", source_reference="same global candle sequence", volume_unit_evidence="synthetic"),
        verification=manifest.build_verification(volume_quote_verified=True, volume_quote_evidence="synthetic",
            closed_before_utc=closed or times[-1] + pd.Timedelta(minutes=5), closure_evidence="synthetic", closure_source="example"))
    data.publish_pack(root, [source], universe=manifest.build_universe())
    return root


@pytest.fixture(scope="module")
def packs(tmp_path_factory):
    work = tmp_path_factory.mktemp("filter-packs")
    target = work / "target"
    publish(target, symbols=("AAA", "BBB"), start="2024-01-01", end="2024-05-02")
    return target, publish_filter(work / "filters")
