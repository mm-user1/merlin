from dataclasses import dataclass
from pathlib import Path
import re
from typing import IO, Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from indicators.volatility import atr


CSVSource = Union[str, Path, IO[str], IO[bytes]]


@dataclass
class TradeRecord:
    direction: Optional[str] = None
    entry_time: Optional[pd.Timestamp] = None
    exit_time: Optional[pd.Timestamp] = None
    entry_price: float = 0.0
    exit_price: float = 0.0
    size: float = 0.0
    net_pnl: float = 0.0
    profit_pct: Optional[float] = None
    side: Optional[str] = None


@dataclass
class StrategyResult:
    """
    Complete result of a strategy backtest.

    Stores both raw curves and calculated metrics to keep orchestration and
    calculation concerns separate.
    """

    trades: List[TradeRecord]
    equity_curve: List[float]
    balance_curve: List[float]
    timestamps: List[pd.Timestamp]

    net_profit: float = 0.0
    net_profit_pct: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0

    sharpe_ratio: Optional[float] = None
    profit_factor: Optional[float] = None
    romad: Optional[float] = None  # Return Over Maximum Drawdown
    ulcer_index: Optional[float] = None
    sqn: Optional[float] = None
    consistency_score: Optional[float] = None  # Signed R² equity consistency [-1, +1]

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "net_profit": self.net_profit,
            "net_profit_pct": self.net_profit_pct,
            "gross_profit": self.gross_profit,
            "gross_loss": self.gross_loss,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_pct": self.max_drawdown_pct,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "equity_curve": self.equity_curve,
            "balance_curve": self.balance_curve,
            "timestamps": [ts.isoformat() if hasattr(ts, "isoformat") else ts for ts in self.timestamps],
        }

        optional_metrics = {
            "sharpe_ratio": self.sharpe_ratio,
            "profit_factor": self.profit_factor,
            "romad": self.romad,
            "ulcer_index": self.ulcer_index,
            "sqn": self.sqn,
            "consistency_score": self.consistency_score,
        }

        for key, value in optional_metrics.items():
            if value is not None:
                data[key] = value

        return data

def load_data(csv_source: CSVSource) -> pd.DataFrame:
    df = pd.read_csv(csv_source)
    if "time" not in df.columns:
        raise ValueError("CSV must include a 'time' column with timestamps in seconds")
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    if df["time"].isna().all():
        raise ValueError("Failed to parse timestamps from 'time' column")
    df = df.set_index("time").sort_index()
    expected_cols = {"open", "high", "low", "close", "Volume", "volume"}
    available_cols = set(df.columns)
    price_cols = {"open", "high", "low", "close"}
    if not price_cols.issubset({col.lower() for col in available_cols}):
        raise ValueError("CSV must include open, high, low, close columns")
    volume_col = None
    for col in ("Volume", "volume", "VOL", "vol"):
        if col in df.columns:
            volume_col = col
            break
    if volume_col is None:
        raise ValueError("CSV must include a volume column")
    renamed = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        volume_col: "Volume",
    }
    normalized_cols = {col: renamed.get(col.lower(), col) for col in df.columns}
    df = df.rename(columns=normalized_cols)
    return df[["Open", "High", "Low", "Close", "Volume"]]


def prepare_dataset_with_warmup(
    df: pd.DataFrame,
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
    warmup_bars: int,
) -> tuple[pd.DataFrame, int]:
    """
    Trim dataset with warmup period for MA calculations.

    Args:
        df: Full OHLCV DataFrame with datetime index
        start: Start date for trading (None = use all data)
        end: End date for trading (None = use all data)
        warmup_bars: Number of bars to include before the start date

    Returns:
        Tuple of (trimmed_df, trade_start_idx)
        - trimmed_df: DataFrame with warmup + trading period
        - trade_start_idx: Index where trading should begin (warmup ends)
    """
    try:
        normalized_warmup = int(warmup_bars)
    except (TypeError, ValueError):
        normalized_warmup = 0

    normalized_warmup = max(0, normalized_warmup)

    # If no date filtering, use entire dataset
    if start is None and end is None:
        return df.copy(), 0

    # Find indices for start and end dates
    times = df.index

    # Determine start index
    if start is not None:
        # Find first index >= start
        start_mask = times >= start
        if not start_mask.any():
            # Start date is after all data
            print(f"Warning: Start date {start} is after all available data")
            return df.iloc[0:0].copy(), 0  # Return empty df
        start_idx = int(start_mask.argmax())
    else:
        start_idx = 0

    # Determine end index
    if end is not None:
        # Find last index <= end
        end_mask = times <= end
        if not end_mask.any():
            # End date is before all data
            print(f"Warning: End date {end} is before all available data")
            return df.iloc[0:0].copy(), 0  # Return empty df
        # Get the last True value
        end_idx = len(end_mask) - 1 - int(end_mask[::-1].argmax())
        end_idx += 1  # Include the end bar
    else:
        end_idx = len(df)

    # Calculate warmup start (go back from start_idx)
    warmup_start_idx = max(0, start_idx - normalized_warmup)

    # Check if we have enough data
    actual_warmup = start_idx - warmup_start_idx
    if actual_warmup < normalized_warmup:
        print(f"Warning: Insufficient warmup data. Need {normalized_warmup} bars, "
              f"only have {actual_warmup} bars available")

    # Trim the dataframe
    trimmed_df = df.iloc[warmup_start_idx:end_idx].copy()

    # Trade start index is where actual trading begins (after warmup)
    trade_start_idx = start_idx - warmup_start_idx

    return trimmed_df, trade_start_idx


def parse_timestamp_utc(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def _is_date_only(value: Any) -> bool:
    return isinstance(value, str) and bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value.strip()))


def _align_date_only(ts: Optional[pd.Timestamp], index: pd.Index, *, side: str) -> Optional[pd.Timestamp]:
    if ts is None or index.empty:
        return ts
    if side == "start":
        idx = index.searchsorted(ts, side="left")
        if idx >= len(index):
            return ts
        return index[idx]
    if side == "end":
        day_end = ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        idx = index.searchsorted(day_end, side="right") - 1
        if idx < 0:
            return ts
        return index[idx]
    return ts


def align_date_bounds(
    index: pd.Index,
    start_raw: Any,
    end_raw: Any,
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    start = parse_timestamp_utc(start_raw)
    end = parse_timestamp_utc(end_raw)

    if _is_date_only(start_raw):
        start = _align_date_only(start, index, side="start")
    if _is_date_only(end_raw):
        end = _align_date_only(end, index, side="end")

    return start, end


def build_forced_close_trade(
    *,
    position: int,
    entry_time: Optional[pd.Timestamp],
    exit_time: pd.Timestamp,
    entry_price: float,
    exit_price: float,
    size: float,
    entry_commission: float,
    commission_rate: float,
    commission_is_pct: bool = False,
) -> tuple[Optional[TradeRecord], float, float, float]:
    """
    Build a TradeRecord for a forced close at the end of the dataset.

    Returns:
        (trade, gross_pnl, exit_commission, net_pnl)
    """
    if position == 0 or entry_time is None or size == 0:
        return None, 0.0, 0.0, 0.0

    gross_pnl = (exit_price - entry_price) * size if position > 0 else (entry_price - exit_price) * size
    rate = commission_rate / 100.0 if commission_is_pct else commission_rate
    exit_commission = exit_price * size * rate
    net_pnl = gross_pnl - exit_commission - entry_commission
    entry_value = entry_price * size
    profit_pct = (net_pnl / entry_value * 100.0) if entry_value else None
    direction = "long" if position > 0 else "short"
    side = "LONG" if position > 0 else "SHORT"
    trade = TradeRecord(
        direction=direction,
        side=side,
        entry_time=entry_time,
        exit_time=exit_time,
        entry_price=entry_price,
        exit_price=exit_price,
        size=size,
        net_pnl=net_pnl,
        profit_pct=profit_pct,
    )
    return trade, gross_pnl, exit_commission, net_pnl
