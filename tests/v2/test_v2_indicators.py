import numpy as np
import pandas as pd
import pytest

from indicators.williams import williams_r
from strategies.s06_r_trend_v02.strategy import _williams_r as s06_williams_r


def test_williams_r_on_deterministic_ohlc():
    high = pd.Series([10.0, 12.0, 14.0, 13.0])
    low = pd.Series([5.0, 6.0, 7.0, 8.0])
    close = pd.Series([7.0, 9.0, 10.0, 12.0])

    result = williams_r(high, low, close, 3)

    assert np.isnan(result.iloc[0])
    assert np.isnan(result.iloc[1])
    assert result.iloc[2] == pytest.approx(100.0 * (10.0 - 14.0) / 9.0)
    assert result.iloc[3] == pytest.approx(100.0 * (12.0 - 14.0) / 8.0)


def test_williams_r_flat_range_returns_nan():
    high = pd.Series([10.0, 10.0, 10.0])
    low = pd.Series([10.0, 10.0, 10.0])
    close = pd.Series([10.0, 10.0, 10.0])

    result = williams_r(high, low, close, 2)

    assert result.isna().all()


def test_williams_r_matches_existing_s06_helper():
    high = pd.Series([10.0, 12.0, 11.0, 15.0, 14.0, 16.0])
    low = pd.Series([7.0, 8.0, 8.5, 9.0, 10.0, 11.0])
    close = pd.Series([8.0, 10.0, 9.0, 14.0, 11.0, 15.0])

    shared = williams_r(high, low, close, 3).to_numpy()
    existing = s06_williams_r(high, low, close, 3)

    np.testing.assert_allclose(shared, existing, equal_nan=True)


def test_williams_r_rejects_non_positive_length():
    with pytest.raises(ValueError, match="length"):
        williams_r(pd.Series([1.0]), pd.Series([1.0]), pd.Series([1.0]), 0)


def test_williams_r_rejects_input_length_mismatch():
    with pytest.raises(ValueError, match="equal length"):
        williams_r(
            pd.Series([1.0, 2.0]),
            pd.Series([1.0]),
            pd.Series([1.5, 1.8]),
            2,
        )
