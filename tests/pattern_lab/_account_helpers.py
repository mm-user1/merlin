"""Synthetic account inputs produced from 5m rows through native preparation."""
from dataclasses import replace
import numpy as np

from tools.pattern_lab import data
from tools.pattern_lab.accounts import (PreparedInput, AccountFeatureRequest, prepare_inputs,
    SideCommands, CommandBundle, EntryFilter, AccountWindow, ExecutionPolicy)
from tools.pattern_lab.study.bracket_rules import normalize_rules
from ._bracket_helpers import rule_entry

STEP = 1800000


def binding(bars, *, slots=None, tf=30, alias="target", start=0, end=None, warmup=0):
    v = np.asarray(bars, dtype=np.float64)
    if v.shape[1] == 4:
        v = np.column_stack((v, np.ones(len(v))))
    slots = np.arange(len(v)) if slots is None else np.asarray(slots)
    stamps = (slots[:, None]*STEP + np.arange(6)*300000).reshape(-1).astype(np.int64)
    raw = np.repeat(v, 6, axis=0)
    return raw_binding(stamps, raw, tf=tf, alias=alias, start=start,
                       end=int((slots[-1]+1)*STEP) if end is None else end, warmup=warmup)


def raw_binding(stamps, raw, *, tf=30, alias="target", start=0, end, warmup=0):
    args = dict(instrument_id="OKX_AAA-USDT-SWAP", venue="OKX", contract="AAA-USDT-SWAP",
                quote_currency="USDT", timeframe_minutes=tf, start_ms=start, end_ms=end, warmup_start_ms=warmup)
    return PreparedInput(alias, data.prepare_series(timestamps=stamps, ohlcv=raw, **args), data.fingerprint_header(**args))


def prepared(bars=None, **kwargs):
    if bars is None:
        bars = [(100, 102, 98, 100)]*6
    return prepare_inputs(binding(bars, **kwargs), (), ())


def rules():
    return normalize_rules(rule_entry())


def side(n, entries=(), stop=90., exits=(), proposals=None, entryKnown=None, stopKnown=None, exitKnown=None):
    event, exit_event = np.zeros(n, bool), np.zeros(n, bool)
    event[list(entries)], exit_event[list(exits)] = True, True
    level = np.full(n, stop, dtype=np.float64) if np.isscalar(stop) else np.array(stop, dtype=np.float64)
    proposed = np.full(n, np.nan) if proposals is None else np.array(proposals, dtype=np.float64)
    return SideCommands(event, np.ones(n, bool) if entryKnown is None else np.array(entryKnown, bool), level,
        np.ones(n, bool) if stopKnown is None else np.array(stopKnown, bool), exit_event,
        np.ones(n, bool) if exitKnown is None else np.array(exitKnown, bool), proposed,
        np.isfinite(proposed) if proposals is not None else np.zeros(n, bool))


def bundle(p, *, long=None, short=None, mode="signalManaged", updates=False, filt=None):
    n = len(p.target.timestamps)
    return CommandBundle(p.identity, p.target.timestamps+p.stepMs,
        long if long is not None else side(n), short if short is not None else side(n, stop=110.),
        mode, updates, {"provider": "synthetic", "version": "1"}, filt)


def policy(mode="signalManaged", updates=False, direction="both", **kwargs):
    args = dict(mode=mode, direction=direction, rewardRisk=1. if mode == "fixedBracket" else None,
                stopUpdates=updates, maxHoldingDays=None)
    return ExecutionPolicy(**{**args, **kwargs})


def window(p, start=None, end=None):
    return AccountWindow(p.target.header["start_ms"] if start is None else start,
                         p.preparationEndMs if end is None else end)


def filter_for(p, long=True, short=True, known=True, tag="test"):
    n = len(p.target.timestamps)
    vals = lambda x: np.full(n, x, dtype=bool) if np.isscalar(x) else np.array(x, dtype=bool)
    known = vals(known)
    t = p.target.timestamps+p.stepMs
    return EntryFilter(p.identity, t, known, vals(long), vals(short), np.arange(n, dtype=np.int64),
                       t, t, t+p.stepMs, np.where(known, 0, 2).astype(np.int64), {"tag": tag})


def feature(requestId="ha", featureId="ha", inputId="target", priorBars=0, **parameters):
    return AccountFeatureRequest(requestId, featureId, "1", inputId, parameters, priorBars)


def spawn_run(connection, p, c, pol, w, r):
    """Top-level child target: mutations must really fail after unpickling."""
    from tools.pattern_lab.accounts import execute_account
    try:
        protected = []
        for a in (p.target.values, p.target.timestamps, p.features[0].values, c.long.initialStop, c.decisionMs):
            try:
                a.flat[0] = 0
            except ValueError:
                protected.append(True)
            else:
                protected.append(False)
        result = execute_account(p, c, pol, w, r)
        connection.send((protected, result))
    finally:
        connection.close()
