"""Agent-defined descriptive ranking, entirely from public checked saved tables.

Run from repository root: python -m tools.pattern_lab.examples.rank_bracket_accounts RUN_ROOT
This explicit net-PnL ranking is not automatic RR selection or inference.
"""
import argparse
import json

from tools.pattern_lab.study import load_results


def rank_accounts(root):
    results = load_results(root)
    ranked = []
    for instrument in results.completed_instruments:
        reader = results.instrument_reader(instrument)
        try:
            tables = reader.sequential_tables()
            keys = ["instrument_id", "timeframe_minutes", "variant_id", "model_instance_id", "case_id"]
            for key, path in tables["path"].groupby(keys, sort=True):
                trades = tables["trades"]
                for column, value in zip(keys, key):
                    trades = trades.loc[trades[column] == value]
                ranked.append({**dict(zip(keys,key)), "completed_trades": len(trades),
                               "net_pnl": float(trades.net_pnl.sum()),
                               "final_capital": float(path.iloc[-1].balance)})
        finally:
            reader.release()
    return sorted(ranked, key=lambda row: (-row["net_pnl"], str(row)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_root")
    print(json.dumps(rank_accounts(parser.parse_args().run_root), indent=2, allow_nan=False))
