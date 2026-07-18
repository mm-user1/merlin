import csv
import json
from pathlib import Path

import pandas as pd

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.engine_v2.runner import V2RunResult, run_v2_strategy
from strategies.s03_reversal_v11_regime_er_b2 import strategy as s03_regime_er_strategy
from strategies.s03_reversal_v11_regime_er_b2.signals import (
    S03RegimeERParams,
    build_s03_regime_er_execution_data,
)
from strategies.s03_reversal_v11_regime_er_b2.strategy import (
    S03ReversalV11RegimeERB2,
    load_profile,
    normalized_params,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_ROOT = REPO_ROOT / "data" / "baseline_v2" / "s03_reversal_v11_regime_er"
MARKET_DATA_PATH = REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
BASELINE_START = pd.Timestamp("2025-02-01T00:00:00Z")
BASELINE_END = pd.Timestamp("2026-02-01T00:00:00Z")
BASELINE_RUNTIME_PARAMS = {
    "dateFilter": True,
    "start": BASELINE_START.isoformat().replace("+00:00", "Z"),
    "end": BASELINE_END.isoformat().replace("+00:00", "Z"),
}
REFERENCE_A = "reference_a_no_emergency_sl"
REFERENCE_B = "reference_b_emergency_sl_10"


def csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_reference(reference_id: str) -> tuple[Path, dict, list[dict[str, str]]]:
    reference_dir = BASELINE_ROOT / reference_id
    with (reference_dir / "params.json").open(encoding="utf-8") as handle:
        params = json.load(handle)["strategy_inputs"]
    return reference_dir, params, csv_rows(reference_dir / "trades_normalized_utc.csv")


def load_summary(reference_id: str) -> dict:
    reference_dir = BASELINE_ROOT / reference_id
    with (reference_dir / "tradingview_summary.json").open(encoding="utf-8") as handle:
        return json.load(handle)


def baseline_market_data():
    return load_data(MARKET_DATA_PATH)


def merged_reference_params(reference_id: str, extra: dict | None = None) -> dict:
    _, params, _ = load_reference(reference_id)
    merged = normalized_params({**params, **BASELINE_RUNTIME_PARAMS})
    merged.update(extra or {})
    return merged


def prepared_reference_dataset(*, warmup_bars: int = 1000, end: pd.Timestamp = BASELINE_END):
    return prepare_dataset_with_warmup(
        baseline_market_data(),
        BASELINE_START,
        end,
        warmup_bars,
    )


def run_reference(
    reference_id: str,
    *,
    warmup_bars: int = 1000,
    end: pd.Timestamp = BASELINE_END,
) -> V2RunResult:
    merged = merged_reference_params(reference_id, {"end": end.isoformat().replace("+00:00", "Z")})
    parsed = S03RegimeERParams.from_dict(merged)
    prepared, trade_start_idx = prepared_reference_dataset(warmup_bars=warmup_bars, end=end)
    data = s03_regime_er_strategy.build_v2_execution_data(prepared, merged)
    return run_v2_strategy(
        data=data,
        profile=load_profile(),
        params=merged,
        trade_start_idx=trade_start_idx,
    )


def run_reference_without_adapter_truncation(reference_id: str, *, warmup_bars: int = 1000) -> V2RunResult:
    merged = merged_reference_params(reference_id)
    parsed = S03RegimeERParams.from_dict(merged)
    full_market = baseline_market_data()
    start_idx = int((full_market.index >= BASELINE_START).argmax())
    warmup_start = max(0, start_idx - warmup_bars)
    prepared = full_market.iloc[warmup_start:].copy()
    trade_start_idx = start_idx - warmup_start
    data = build_s03_regime_er_execution_data(prepared, parsed)
    return run_v2_strategy(
        data=data,
        profile=load_profile(),
        params=merged,
        trade_start_idx=trade_start_idx,
    )


def run_public_reference(reference_id: str, *, warmup_bars: int = 1000):
    params = merged_reference_params(reference_id)
    prepared, trade_start_idx = prepared_reference_dataset(warmup_bars=warmup_bars)
    return S03ReversalV11RegimeERB2.run(prepared, params, trade_start_idx=trade_start_idx)


def iso_timestamp(value) -> str:
    return pd.Timestamp(value).isoformat().replace("+00:00", "Z")


def trade_skeleton(result) -> tuple:
    return tuple(
        (
            trade.direction,
            iso_timestamp(trade.entry_time),
            iso_timestamp(trade.exit_time),
            trade.entry_price,
            trade.exit_price,
            trade.exit_reason,
        )
        for trade in result.trades
    )


def synthetic_ohlc(
    closes: list[float],
    *,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> pd.DataFrame:
    import numpy as np

    close_values = np.asarray(closes, dtype=float)
    open_values = np.asarray(opens if opens is not None else closes, dtype=float)
    high_values = np.asarray(highs if highs is not None else np.maximum(open_values, close_values), dtype=float)
    low_values = np.asarray(lows if lows is not None else np.minimum(open_values, close_values), dtype=float)
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Volume": np.full(len(close_values), 1000.0),
        },
        index=pd.date_range("2025-01-01", periods=len(close_values), freq="30min", tz="UTC"),
    )


__all__ = [
    "BASELINE_END",
    "BASELINE_ROOT",
    "BASELINE_START",
    "MARKET_DATA_PATH",
    "REFERENCE_A",
    "REFERENCE_B",
    "baseline_market_data",
    "csv_rows",
    "iso_timestamp",
    "load_reference",
    "load_summary",
    "merged_reference_params",
    "prepared_reference_dataset",
    "run_public_reference",
    "run_reference",
    "run_reference_without_adapter_truncation",
    "synthetic_ohlc",
    "trade_skeleton",
]

