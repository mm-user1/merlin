"""Small HA command and candle permission providers; no account state."""
from dataclasses import asdict
import numpy as np
import pandas as pd

from .contracts import (HARecipe, SideCommands, CommandBundle, EntryFilter, require,
                        integer, number, validate_mode, validate_bundle)


def candle_filter(prepared, requestId):
    feature = prepared.feature(requestId)
    require(feature.request["featureId"] == "candleDirection", "filter needs candleDirection")
    a = feature.aligned
    source = next(v for v in prepared.inputs if v.inputId == feature.request["inputId"])
    return EntryFilter(prepared.identity, a.decisionMs, a.known,
        a.known & (a.value == 1), a.known & (a.value == -1), a.sourceIndex, a.observedAtMs, a.availableAtMs,
        a.validUntilMs, a.unknownReason, dict(provider="candleDirection", version="1", request=feature.request,
            sourceHeader=source.header, inputFingerprint=source.inputFingerprint))


def build_commands(prepared, recipe, *, mode, stopUpdates, entryFilter=None):
    validate_mode(mode, stopUpdates)
    require(isinstance(recipe, HARecipe), "HARecipe required")
    integer(recipe.entryRunLength, "entryRunLength", 1)
    integer(recipe.exitRunLength, "exitRunLength", 1)
    integer(recipe.swingBars, "swingBars", 1)
    number(recipe.atrMultiplier, "atrMultiplier", inclusive=True)
    require(recipe.stopProvider in ("haLast", "haSwing2", "realSwingAtr"), "stopProvider")
    ha = prepared.feature(recipe.haRequestId)
    require(ha.request["featureId"] == "ha" and ha.request["inputId"] == prepared.targetId, "HA dependency")
    atr = None
    if recipe.stopProvider == "realSwingAtr":
        atr = prepared.feature(recipe.atrRequestId)
        require(atr.request["featureId"] == "atr" and atr.request["inputId"] == prepared.targetId, "ATR dependency")
    else:
        require(recipe.atrRequestId is None, "unused ATR dependency")
    n = len(prepared.target.timestamps)
    sides = []
    for sign in (1, -1):
        levels = ha.values[:, 2 if sign == 1 else 1]
        known = ha.known.copy()
        if recipe.stopProvider == "haLast":
            stop = levels.copy()
        else:
            width = 2 if recipe.stopProvider == "haSwing2" else recipe.swingBars
            if atr is not None:
                levels = prepared.target.values[:, 2 if sign == 1 else 1]
            rolling = pd.Series(levels).rolling(width, min_periods=width)
            stop = (rolling.min() if sign == 1 else rolling.max()).to_numpy()
            known &= ha.position >= width - 1
            stop[ha.position < width - 1] = np.nan
            if atr is not None:
                stop = stop - sign * recipe.atrMultiplier * atr.values
                known &= atr.known
        sides.append(SideCommands((ha.colour == sign) & (ha.run == recipe.entryRunLength), ha.known,
            stop, known, (ha.colour == -sign) & (ha.run >= recipe.exitRunLength) if mode == "signalManaged" else np.zeros(n, bool),
            ha.known if mode == "signalManaged" else np.zeros(n, bool),
            stop if stopUpdates else np.full(n, np.nan), known if stopUpdates else np.zeros(n, bool)))
    bundle = CommandBundle(prepared.identity, prepared.target.timestamps + prepared.stepMs, *sides,
        mode, stopUpdates, dict(provider="haCommands", version="1", recipe=asdict(recipe)), entryFilter)
    validate_bundle(prepared, bundle)
    return bundle
