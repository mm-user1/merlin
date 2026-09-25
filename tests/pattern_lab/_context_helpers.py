"""Shared synthetic context pack, without test-module imports."""
import numpy as np
from tools.pattern_lab.study import validation
from . import _helpers as h


def context_fixture(root, *, suffix=0, change=False, drop=(), target_drop=(), future_change=False):
    stamps, values = h.synthetic_series(240 + suffix)
    other = values.copy()
    other[:, 3] += (np.arange(len(other)) % 3 == 0) * 2
    if change:
        other[30, 3] += .5
    other[:, 1] = np.maximum(other[:, 1], other[:, 3]) + 1
    if future_change:
        other[150:, :4] *= 2
    keep = ~np.isin(np.arange(len(stamps)), drop)
    target_keep = ~np.isin(np.arange(len(stamps)), target_drop)
    h.publish(root, [h.instrument_source(stamps[target_keep], values[target_keep]),
                    h.instrument_source(stamps[keep], other[keep], symbol="BTC", contract="BTC-USDT-SWAP", roles=["factor"])])
    document = validation.external_document(h.normalized_study(start_group=2, end_group=35))
    document.update(schema_version=2, context={"btc": {"ids": ["TEST_BTC-USDT-SWAP"]}},
                    execution={"kind": "development"})
    document["hypotheses"] = [{"id": "btc", "hypothesis": "two_green_volume_btc",
                                "parameters": {"alias": "btc"}, "occurrence": "state_entry"}]
    return document

