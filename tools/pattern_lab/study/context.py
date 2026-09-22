"""Explicit same-timeframe context, prepared once by the study coordinator."""
from dataclasses import dataclass, replace
import hashlib
from types import MappingProxyType
from typing import Any, Mapping

import numpy as np

from .. import PatternLabDataError
from ..manifest import format_epoch_ms, require_text, VOLUME_UNIT
from . import contracts
from .contracts import BarSeries, FeatureValue, FeatureRequest

POLICY = "dense_utc_close_same_timeframe_all_declared_members_no_fill_v1"


@dataclass(frozen=True)
class ContextSeries:
    """Dense epoch slots; availability is owned by ``valid``.

    ``bars.contiguous_with_previous()`` tests slot adjacency, not missing data.
    Rolling features must require validity over their full declared lookback
    and re-warm after every invalid slot.
    """

    bars: BarSeries
    valid: np.ndarray


@dataclass(frozen=True)
class ContextGrid:
    timeframe_minutes: int
    timestamps_ms: np.ndarray
    sources: Mapping[str, Mapping[str, ContextSeries]]

    @property
    def row_count(self):
        return self.timestamps_ms.size


def normalize_aliases(raw):
    values = contracts.require_mapping(raw, "context")
    result = {}
    for alias, selection in sorted(values.items()):
        contracts.require_identifier(alias, "context alias")
        item = contracts.require_mapping(selection, f"context.{alias}")
        contracts.closed_keys(item, ("ids",), f"context.{alias}")
        ids = item.get("ids")
        if not isinstance(ids, list) or not ids:
            raise PatternLabDataError(f"context.{alias}.ids: expected a nonempty explicit list.")
        normalized = [require_text(value, f"context.{alias}.ids") for value in ids]
        result[alias] = {"ids": sorted(contracts.require_unique(normalized, f"context.{alias}.ids"))}
    return result


def dependencies(request):
    roots = []
    for variant in request.variants:
        roots.extend(contracts.hypothesis(variant.hypothesis_id).dependencies(variant.parameters))
    closure = contracts.resolve_feature_closure(roots, where="study context")
    selected = [item for item in closure if contracts.feature(item.feature_id).scope == "context"]
    used = set()
    for item in selected:
        descriptor = contracts.feature(item.feature_id)
        aliases = descriptor.context_aliases(item.parameters)
        if not isinstance(aliases, tuple) or any(not isinstance(a, str) for a in aliases):
            raise PatternLabDataError(f"context feature {item.feature_id}: context_aliases must return a tuple of names.")
        used.update(aliases)
        for alias in aliases:
            if alias in request.context and descriptor.context_source_count is not None:
                if len(request.context[alias]["ids"]) != descriptor.context_source_count:
                    raise PatternLabDataError(f"context feature {item.feature_id}: alias {alias} requires exactly {descriptor.context_source_count} source(s).")
    if used != set(request.context):
        raise PatternLabDataError(
            f"context aliases: missing {sorted(used - set(request.context))}; unused {sorted(set(request.context) - used)}."
        )
    return tuple(selected)


def resolve_entries(report, request):
    identifiers = sorted({identifier for value in request.context.values() for identifier in value["ids"]})
    by_id = {item["instrument_id"]: item for item in report["instruments"]}
    missing = sorted(set(identifiers) - set(by_id))
    if missing:
        raise PatternLabDataError(f"context: missing explicit source IDs {missing}.")
    return [by_id[identifier] for identifier in identifiers]


def admission(request, entries, targets):
    fields = ("instrument_id", "symbol", "venue", "contract", "quote_currency", "roles")
    return {"schema_version": 1, "aliases": dict(request.context), "policy": POLICY,
            "volume_unit": VOLUME_UNIT,
            "sources": [{key: item[key] for key in fields} for item in entries],
            "self_inclusion": sorted(set(targets) & {item["instrument_id"] for item in entries})}


def prepare(session, entries, request):
    # Import locally to reuse exactly the target preparation/fingerprint path.
    from .runner import _prepare_timeframes
    by_timeframe = {tf: {} for tf in request.timeframes}
    fingerprints = []
    raw_bytes = 0
    for entry in entries:
        identifier = entry["instrument_id"]
        try:
            base = session.load_slice(
                identifier, start=format_epoch_ms(request.study_start_ms),
                end=format_epoch_ms(request.study_end_ms),
                warmup_start=format_epoch_ms(request.warmup_start_ms), timeframe_minutes=5,
            )
            prepared, facts = _prepare_timeframes(entry, base, request)
            del base
            fingerprints.extend(facts)
            for item in prepared:
                tf = item.timeframe_minutes
                stamps = np.arange(request.warmup_start_ms, request.study_end_ms, tf * 60000, dtype=np.int64)
                values = np.full((stamps.size, 5), np.nan, dtype=np.float64)
                valid = np.zeros(stamps.size, dtype=bool)
                positions = (item.timestamps_ms - request.warmup_start_ms) // (tf * 60000)
                values[positions] = item.values
                valid[positions] = True
                slots = stamps // (tf * 60000)
                for array in (stamps, slots, values, valid):
                    array.flags.writeable = False
                bars = BarSeries(identifier, tf, tf * 60000, stamps, slots, values,
                                 int((request.study_start_ms-request.warmup_start_ms)//(tf*60000)))
                by_timeframe[tf][identifier] = ContextSeries(bars, valid)
                raw_bytes += stamps.nbytes + slots.nbytes + values.nbytes + valid.nbytes
        except Exception as error:
            raise PatternLabDataError(f"context series {identifier}: {error}") from error
    outputs = {}
    diagnostics = []
    for tf, sources in by_timeframe.items():
        stamps = np.arange(request.warmup_start_ms, request.study_end_ms, tf*60000, dtype=np.int64)
        stamps.flags.writeable = False
        cache = {}
        def evaluate(item):
            descriptor = contracts.feature(item.feature_id)
            item = FeatureRequest(item.feature_id, descriptor.validate_parameters(dict(item.parameters)))
            if item.key in cache:
                return cache[item.key]
            parents = {dep.key: evaluate(dep) for dep in descriptor.dependencies(item.parameters)}
            aliases = descriptor.context_aliases(item.parameters)
            declared = MappingProxyType({alias: MappingProxyType({identifier: sources[identifier]
                       for identifier in request.context[alias]["ids"]}) for alias in aliases})
            grid = ContextGrid(tf, stamps, declared)
            try:
                value = contracts.check_feature_result(
                    descriptor.evaluate(grid, MappingProxyType(dict(item.parameters)), MappingProxyType(parents)),
                    grid, f"context feature {item.feature_id}",
                )
            except Exception as error:
                raise PatternLabDataError(f"context feature {item.feature_id}, aliases {aliases}: {error}") from error
            # Providers may return a column view into a whole raw panel. Own the
            # compact output so its base cannot keep that panel alive.
            value = FeatureValue(np.array(value.values, copy=True), np.array(value.valid, copy=True))
            value.values.flags.writeable = False
            value.valid.flags.writeable = False
            cache[item.key] = value
            key = contracts.semantic_digest({"feature": item.key, "version": descriptor.version,
                                            "sources": request.context, "timeframe": tf})
            diagnostics.append({"cache_key": key, "feature": item.key, "timeframe_minutes": tf,
                                "feature_id": item.feature_id, "feature_version": descriptor.version,
                                "parameters": dict(item.parameters),
                                "slots": int(stamps.size), "valid": int(value.valid.sum()),
                                "sha256": hashlib.sha256(value.values.tobytes()+value.valid.tobytes()).hexdigest()})
            return value
        for item in dependencies(request):
            evaluate(item)
        outputs[tf] = cache
        sources.clear()
        # The recursive closure otherwise keeps the final source mapping and
        # cache until cyclic GC. Release it at the preparation boundary.
        evaluate = None
    return outputs, {"schema_version": 1, "policy": POLICY, "fingerprints": fingerprints,
                     "features": diagnostics, "raw_context_bytes": raw_bytes,
                     "output_bytes": sum(v.values.nbytes+v.valid.nbytes for cache in outputs.values() for v in cache.values())}


def align(prepared, request, outputs):
    result = []
    for item in prepared:
        positions = (item.timestamps_ms-request.warmup_start_ms)//(item.timeframe_minutes*60000)
        features = {key: FeatureValue(value.values[positions], value.valid[positions])
                    for key, value in outputs.get(item.timeframe_minutes, {}).items()}
        result.append(replace(item, context_features=features))
    return result


def _parameters(raw, *, panel=False):
    contracts.closed_keys(raw, ("alias", "threshold"), "context filter parameters")
    alias = contracts.require_identifier(raw.get("alias"), "context filter alias")
    threshold = contracts.require_number(raw.get("threshold", .5 if panel else 0.), "threshold")
    if panel and not 0 <= threshold <= 1:
        raise PatternLabDataError("panel threshold: expected a fraction in [0, 1].")
    return {"alias": alias, "threshold": threshold}


def btc_return(grid, parameters, features):
    sources = grid.sources[parameters["alias"]]
    if len(sources) != 1:
        raise PatternLabDataError("BTC context alias requires exactly one source.")
    source = next(iter(sources.values()))
    values = np.zeros(grid.row_count, dtype=np.float64)
    valid = np.zeros(grid.row_count, dtype=bool)
    valid[1:] = source.valid[1:] & source.valid[:-1] & (source.bars.close[:-1] > 0)
    np.divide(source.bars.close[1:], source.bars.close[:-1], out=values[1:], where=valid[1:])
    values[valid] -= 1
    return FeatureValue(values, valid)


def panel_green(grid, parameters, features):
    sources = tuple(grid.sources[parameters["alias"]].values())
    valid = np.logical_and.reduce([source.valid for source in sources])
    values = np.mean([source.bars.close > source.bars.open for source in sources], axis=0).astype(np.float64)
    return FeatureValue(values, valid)


def _btc_parameters(raw):
    return _parameters(raw)


def _panel_parameters(raw):
    return _parameters(raw, panel=True)


def _aliases(parameters):
    return (parameters["alias"],)


def _btc_dependencies(parameters):
    return (FeatureRequest("btc_close_return", parameters),)


def _panel_dependencies(parameters):
    return (FeatureRequest("panel_green_fraction", parameters),)


def _filtered(series, parameters, features):
    from .builtins import _two_green_evaluate
    parent = _two_green_evaluate(series, {}, {})
    feature = next(iter(features.values()))
    valid = parent.valid & feature.valid
    return contracts.ConditionValue(parent.value & valid & (feature.values > parameters["threshold"]), valid)


def register_builtins():
    for feature_id, hypothesis_id, evaluate, parameters, dependencies, prior in (
        ("btc_close_return", "two_green_volume_btc", btc_return, _btc_parameters, _btc_dependencies, 1),
        ("panel_green_fraction", "two_green_volume_panel", panel_green, _panel_parameters, _panel_dependencies, 0),
    ):
        if feature_id not in contracts.registered("feature"):
            contracts.register_feature(contracts.FeatureDescriptor(
                feature_id, "1", evaluate, validate_parameters=parameters,
                prior_bars=(lambda p: 1) if prior else (lambda p: 0), scope="context", context_aliases=_aliases,
                context_source_count=1 if prior else None,
            ), builtin=True)
        if hypothesis_id not in contracts.registered("hypothesis"):
            contracts.register_hypothesis(contracts.HypothesisDescriptor(
                hypothesis_id, "1", _filtered, validate_parameters=parameters,
                dependencies=dependencies, prior_bars=lambda p: 1,
            ), builtin=True)
