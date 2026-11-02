"""Calendar views of saved evaluation accounting; execution metrics stay in core."""
import csv
import io
import math

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CURVE_SCHEMA = pa.schema([("timestamp_ns", pa.int64()), ("equity", pa.float64()), ("balance", pa.float64())])
MONTH_COLUMNS = ("month", "opening_equity", "closing_equity", "equity_return_pct", "return_unavailable_reason",
    "opening_balance", "closing_balance", "realized_pnl", "equity_change", "first_timestamp_ns",
    "last_timestamp_ns", "observation_count", "partial_month")


def finite_json(value):
    if isinstance(value, dict):
        return {k: finite_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [finite_json(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    return None if isinstance(value, float) and not math.isfinite(value) else value


def validate_curve(ts, equity, balance, *, start=None, end=None, timeframe=None):
    if ts.dtype != np.dtype("int64") or equity.dtype != np.dtype("float64") or balance.dtype != np.dtype("float64"):
        raise ValueError("curve columns require int64/float64/float64.")
    if any(x.ndim != 1 for x in (ts, equity, balance)) or not len(ts) or not len(ts) == len(equity) == len(balance):
        raise ValueError("invalid curve shape/count.")
    if not np.isfinite(equity).all() or not np.isfinite(balance).all() or np.any(np.diff(ts) <= 0):
        raise ValueError("nonfinite or unordered curve.")
    if timeframe is not None:
        step = timeframe * 60 * 10**9
        if ts[0] != pd.Timestamp(start).value or ts[-1] + step != pd.Timestamp(end).value or np.any(np.diff(ts) != step):
            raise ValueError("curve does not cover every frozen evaluation bar.")


def read_curve(path):
    table = pq.read_table(path)
    if not table.schema.equals(CURVE_SCHEMA, check_metadata=False) or any(c.null_count for c in table.columns):
        raise ValueError("invalid Fixed curve schema/nulls.")
    values = tuple(table[n].to_numpy() for n in CURVE_SCHEMA.names)
    validate_curve(*values)
    return values


def monthly(ts, equity, balance, anchor, timeframe):
    validate_curve(ts, equity, balance)
    if type(anchor) not in (int, float) or not math.isfinite(anchor):
        raise ValueError("invalid metric anchor.")
    dates = pd.to_datetime(ts, utc=True)
    keys = dates.strftime("%Y-%m").to_numpy()
    boundaries = np.r_[0, np.flatnonzero(keys[1:] != keys[:-1]) + 1, len(ts)]
    rows = []
    opening_equity = opening_balance = float(anchor)
    step = timeframe * 60 * 10**9
    for first, stop in zip(boundaries[:-1], boundaries[1:]):
        last = stop - 1
        start = pd.Timestamp(keys[first] + "-01", tz="UTC")
        end = start + pd.offsets.MonthBegin(1)
        closing_equity, closing_balance = float(equity[last]), float(balance[last])
        rows.append(dict(month=keys[first], opening_equity=opening_equity, closing_equity=closing_equity,
            equity_return_pct=100 * (closing_equity / opening_equity - 1) if opening_equity > 0 else None,
            return_unavailable_reason="" if opening_equity > 0 else "nonpositive_opening_equity",
            opening_balance=opening_balance, closing_balance=closing_balance,
            realized_pnl=closing_balance - opening_balance, equity_change=closing_equity - opening_equity,
            first_timestamp_ns=int(ts[first]), last_timestamp_ns=int(ts[last]), observation_count=int(stop-first),
            partial_month=not (ts[first] == start.value and ts[last]+step == end.value and
                               stop-first == (end.value-start.value)//step)))
        opening_equity, opening_balance = closing_equity, closing_balance
    return rows


def csv_bytes(rows, columns):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")
