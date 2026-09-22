"""Trusted example: strict single-source positive return through the public API.

Declare this module and its directory in extensions; use hypothesis
example_context_positive with parameters {"alias": "btc"}. No filesystem or
market access occurs here. The dense grid makes a missing prior slot unknown.
"""
import numpy as np
from tools.pattern_lab.study import contracts


def parameters(raw):
    contracts.closed_keys(raw, ("alias",), "example context parameters")
    return {"alias": contracts.require_identifier(raw.get("alias"), "alias")}


def aliases(params):
    return (params["alias"],)


def evaluate(grid, params, dependencies):
    source = next(iter(grid.sources[params["alias"]].values()))
    valid = np.zeros(grid.row_count, dtype=bool)
    valid[1:] = source.valid[1:] & source.valid[:-1] & (source.bars.close[:-1] > 0)
    values = np.zeros(grid.row_count, dtype=np.float64)
    np.divide(source.bars.close[1:], source.bars.close[:-1], out=values[1:], where=valid[1:])
    values[valid] -= 1
    return contracts.FeatureValue(values, valid)


def dependencies(params):
    return (contracts.FeatureRequest("example_context_return", params),)


def condition(series, params, features):
    value = features[dependencies(params)[0].key]
    return contracts.ConditionValue(value.values > 0, value.valid.copy())


def prior(params):
    return 1


def register(context):
    context.register_feature(contracts.FeatureDescriptor(
        "example_context_return", "1", evaluate, validate_parameters=parameters,
        prior_bars=prior, scope="context", context_aliases=aliases, context_source_count=1,
    ))
    context.register_hypothesis(contracts.HypothesisDescriptor(
        "example_context_positive", "1", condition, validate_parameters=parameters,
        dependencies=dependencies, prior_bars=prior,
    ))
