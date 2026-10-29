"""Synthetic quantity-policy fixtures; no external packs or frozen oracle changes."""
import importlib
import numpy as np
import pandas as pd

from core.engine_v2.admission import AdmissionRules
from core.engine_v2.profile import parse_execution_profile
from strategies import get_strategy, get_strategy_config
from tests.fixtures.admission_strategy.strategy import rule_entry

LEGACY = ("s03_reversal_v11_regime_er_b2", "s03_reversal_v16_4_a_adaptive_ma_b2",
          "s06_r_trend_v02_b2", "s06_r_trend_v02_regime_trendlines_b2", "s06_r_trend_v06_4_a2_b2")


def frame(n=3000):
    rng = np.random.default_rng(7)
    close = 100 * np.exp(np.cumsum(rng.normal(0, .006, n)))
    opening = np.r_[close[0], close[:-1]] * (1 + rng.normal(0, .001, n))
    return pd.DataFrame(dict(Open=opening, High=np.maximum(opening, close) * (1 + abs(rng.normal(0, .003, n))),
        Low=np.minimum(opening, close) * (1 - abs(rng.normal(0, .003, n))), Close=close, Volume=np.full(n, 1000.)),
        index=pd.date_range('2023-01-01', periods=n, freq='h', tz='UTC'))


def rules(step='.01', minimum=None, notional=None, venue='BYBIT'):
    return AdmissionRules.from_entry(rule_entry(venue, step=step, minimum=minimum or step, notional=notional))


def trading_pack(root, minimum='.01', end='2024-06-01'):
    from tests._portfolio_helpers import publish
    def transform(i, stamps, values, entry):
        rng = np.random.default_rng(7 + i)
        close = 100 * np.exp(np.cumsum(rng.normal(0, .003, len(stamps))))
        opening = np.r_[close[0], close[:-1]]
        values[:, :4] = np.column_stack([opening, np.maximum(opening, close)*1.001, np.minimum(opening, close)*.999, close])
        entry = rule_entry(entry['venue'], step='.01', minimum=minimum, notional='0')
        return stamps, values, entry
    return publish(root, transform=transform, end=end)


def cases():
    for sid in LEGACY:
        config = get_strategy_config(sid)
        profile = parse_execution_profile(config)
        selector = profile.variant_selector
        for variant in profile.variants:
            value = next((key for key, name in selector.mapping.items() if name == variant), None) if selector else None
            yield sid, variant, value


def setup(sid, value):
    cls = get_strategy(sid)
    module = importlib.import_module(cls.__module__)
    config = get_strategy_config(sid)
    profile = parse_execution_profile(config)
    params = dict(profile.parameter_defaults, dateFilter=False)
    if profile.variant_selector:
        spec = config['parameters'][profile.variant_selector.param]
        params[profile.variant_selector.param] = value == 'true' if spec['type'] == 'bool' else value
    params = module.normalized_params(params)
    return cls, module, config, profile, params
