import csv
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path

import pandas as pd

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from core.engine_v2.profile import parse_execution_profile
from core.engine_v2.runner import V2RunResult, run_v2_strategy
from strategies.s06_r_trend_v02_b2.signals import S06B2Params, build_s06_b2_execution_data
from strategies.s06_r_trend_v02_b2.strategy import (
    S06RTrendV02B2,
    load_config,
    load_profile,
    normalized_params,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_ROOT = REPO_ROOT / "data" / "baseline_v2" / "s06_r_trend_v02"
MARKET_DATA_PATH = REPO_ROOT / "data" / "raw" / "OKX_SUIUSDT.P, 30 2025.01.01-2026.02.01.csv"
BASELINE_START = pd.Timestamp("2025-08-01T00:00:00Z")
BASELINE_END = pd.Timestamp("2025-12-01T00:00:00Z")
BASELINE_RUNTIME_PARAMS = {
    "dateFilter": True,
    "start": BASELINE_START.isoformat().replace("+00:00", "Z"),
    "end": BASELINE_END.isoformat().replace("+00:00", "Z"),
}


def csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_reference(reference_id: str) -> tuple[Path, dict, list[dict[str, str]]]:
    reference_dir = BASELINE_ROOT / reference_id
    with (reference_dir / "params.json").open(encoding="utf-8") as handle:
        params = json.load(handle)["strategy_inputs"]
    return reference_dir, params, csv_rows(reference_dir / "trades_normalized_utc.csv")


def baseline_market_data():
    return load_data(MARKET_DATA_PATH)


def merged_reference_params(reference_id: str, extra: dict | None = None) -> dict:
    _, params, _ = load_reference(reference_id)
    merged = normalized_params({**params, **BASELINE_RUNTIME_PARAMS})
    merged.update(extra or {})
    return merged


def profile_with_rounding(price_rounding: str = "none"):
    if price_rounding == "none":
        return load_profile()
    config = load_config()
    config["execution"]["priceRounding"] = price_rounding
    return parse_execution_profile(config)


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
    price_rounding: str = "none",
    warmup_bars: int = 1000,
    end: pd.Timestamp = BASELINE_END,
) -> V2RunResult:
    merged = merged_reference_params(reference_id, {"end": end.isoformat().replace("+00:00", "Z")})
    parsed = S06B2Params.from_dict(merged)
    prepared, trade_start_idx = prepared_reference_dataset(warmup_bars=warmup_bars, end=end)
    data = build_s06_b2_execution_data(prepared, parsed)
    return run_v2_strategy(
        data=data,
        profile=profile_with_rounding(price_rounding),
        params=merged,
        trade_start_idx=trade_start_idx,
    )


def run_public_reference(reference_id: str, *, warmup_bars: int = 1000):
    params = merged_reference_params(reference_id)
    parsed = S06B2Params.from_dict(params)
    prepared, trade_start_idx = prepared_reference_dataset(warmup_bars=warmup_bars)
    if parsed.dateFilter and parsed.end is not None:
        prepared = prepared.loc[prepared.index <= parsed.end]
    return S06RTrendV02B2.run(prepared, params, trade_start_idx=trade_start_idx)


def iso_timestamp(value) -> str:
    return pd.Timestamp(value).isoformat().replace("+00:00", "Z")


def trade_signature(result) -> tuple:
    return tuple(
        (
            trade.direction,
            iso_timestamp(trade.entry_time),
            iso_timestamp(trade.exit_time),
            trade.entry_price,
            trade.exit_price,
            trade.size,
            trade.net_pnl,
        )
        for trade in result.trades
    )


def run_signature(run: V2RunResult) -> tuple:
    result = run.strategy_result
    guardrails = asdict(run.guardrail_summary) if is_dataclass(run.guardrail_summary) else run.guardrail_summary
    standing = asdict(run.standing_state) if is_dataclass(run.standing_state) else run.standing_state
    metrics = (
        result.balance_curve[-1] if result.balance_curve else None,
        result.net_profit,
        result.net_profit_pct,
        result.profit_factor,
        result.max_drawdown_pct,
        result.total_trades,
        result.winning_trades,
        result.losing_trades,
    )
    return trade_signature(result), metrics, tuple(sorted(guardrails.items())), tuple(sorted(standing.items()))
