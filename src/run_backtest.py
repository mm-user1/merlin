import argparse
import pandas as pd

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from strategies import get_strategy, get_strategy_config
from strategies.s01_trailing_ma import S01Params

DEFAULT_CSV_PATH = "data/raw/OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
DEFAULT_WARMUP_BARS = 1000


def build_default_params() -> S01Params:
    return S01Params(
        use_date_filter=True,
        start=pd.Timestamp("2025-06-15", tz="UTC"),
        end=pd.Timestamp("2025-11-15", tz="UTC"),
        maType="SMA",
        maLength=300,
        closeCountLong=9,
        closeCountShort=5,
        stopLongX=2.0,
        stopLongRR=3.0,
        stopLongLP=2,
        stopShortX=2.0,
        stopShortRR=3.0,
        stopShortLP=2,
        stopLongMaxPct=7.0,
        stopShortMaxPct=10.0,
        stopLongMaxDays=5,
        stopShortMaxDays=2,
        trailRRLong=1.0,
        trailRRShort=1.0,
        trailMaType="EMA",
        trailLongLength=90,
        trailLongOffset=-0.5,
        trailShortLength=190,
        trailShortOffset=2.0,
        riskPerTrade=2.0,
        contractSize=0.01,
        commissionRate=0.0005,
        atrPeriod=14,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the S01 Trailing MA backtest")
    parser.add_argument(
        "--csv",
        type=str,
        default=DEFAULT_CSV_PATH,
        help="Path to the CSV file with OHLCV data",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=DEFAULT_WARMUP_BARS,
        help="Warmup bars to include before the start date",
    )
    args = parser.parse_args()

    strategy_cls = get_strategy("s01_trailing_ma")
    from core.engine_v2.admission import uses_admission
    if uses_admission(get_strategy_config(strategy_cls.STRATEGY_ID)):
        raise ValueError("The CSV CLI does not support risk_bracket_v1; use Merlin Parquet execution.")
    df = load_data(args.csv)
    params = build_default_params()

    df_prepared, trade_start_idx = prepare_dataset_with_warmup(
        df, params.start, params.end, args.warmup
    )

    result = strategy_cls.run(df_prepared, params.__dict__, trade_start_idx)

    print(f"Net Profit %: {result.net_profit_pct:.2f}")
    print(f"Max Portfolio Drawdown %: {result.max_drawdown_pct:.2f}")
    print(f"Total Trades: {result.total_trades}")


if __name__ == "__main__":
    main()
