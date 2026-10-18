"""Native complete-series binding, segment features and causal state alignment."""
from dataclasses import dataclass, asdict
import json
import re

import numpy as np

from .. import data
from ..study.bracket import pine_atr
from ..study.contracts import canonical_json, semantic_digest
from .contracts import (SCHEMA, PreparedInput, AccountFeatureRequest, FrozenArrays,
                        array, require, integer, identifier)


@dataclass(frozen=True)
class InputValues(FrozenArrays):
    inputId: str
    headerJson: str
    inputFingerprint: str
    timestamps: np.ndarray
    values: np.ndarray
    researchMask: np.ndarray
    segmentStart: np.ndarray
    coverageJson: str

    @property
    def header(self):
        return json.loads(self.headerJson)

    @property
    def stepMs(self):
        return self.header["timeframe_minutes"] * 60000


@dataclass(frozen=True)
class StateRows(FrozenArrays):
    observedAtMs: np.ndarray
    availableAtMs: np.ndarray
    validUntilMs: np.ndarray
    value: np.ndarray
    known: np.ndarray
    coverageStartMs: int
    coverageEndMs: int

    def __post_init__(self):
        integer(self.coverageStartMs, "coverageStartMs", -(2**63))
        integer(self.coverageEndMs, "coverageEndMs", -(2**63))
        require(self.coverageStartMs < self.coverageEndMs, "state coverage")
        require(isinstance(self.value, np.ndarray) and self.value.ndim == 1, "state value shape")
        for name in ("observedAtMs", "availableAtMs", "validUntilMs", "value", "known"):
            dtype = bool if name == "known" else np.float64 if name == "value" else np.int64
            object.__setattr__(self, name, array(getattr(self, name), dtype, self.value.shape))
        require(np.all(np.diff(self.availableAtMs) > 0) and
                np.all(self.observedAtMs <= self.availableAtMs) and
                np.all(self.availableAtMs < self.validUntilMs), "state chronology")
        require(np.all(self.observedAtMs >= self.coverageStartMs) and
                np.all(self.validUntilMs <= self.coverageEndMs), "state outside declared coverage")
        require(not np.isinf(self.value).any() and np.isfinite(self.value[self.known]).all(), "state price")


@dataclass(frozen=True)
class AlignedState(FrozenArrays):
    decisionMs: np.ndarray
    sourceIndex: np.ndarray
    observedAtMs: np.ndarray
    availableAtMs: np.ndarray
    validUntilMs: np.ndarray
    value: np.ndarray
    known: np.ndarray
    unknownReason: np.ndarray


def align_states(rows, decisionMs):
    require(isinstance(rows, StateRows), "StateRows required")
    decisions = array(decisionMs, np.int64)
    require(decisions.ndim == 1 and np.all(np.diff(decisions) > 0), "alignment decision order")
    ix = np.searchsorted(rows.availableAtMs, decisions, side="right") - 1
    present = ix >= 0
    observed = np.zeros(len(ix), dtype=np.int64)
    available, until = observed.copy(), observed.copy()
    value = np.full(len(ix), np.nan)
    known = np.zeros(len(ix), dtype=bool)
    reason = np.ones(len(ix), dtype=np.int64)
    if present.any():
        j = ix[present]
        observed[present], available[present], until[present] = (
            rows.observedAtMs[j], rows.availableAtMs[j], rows.validUntilMs[j])
        value[present] = rows.value[j]
        expired = decisions[present] >= rows.validUntilMs[j]
        known[present] = rows.known[j] & ~expired
        reason[present] = np.where(expired, 3, np.where(rows.known[j], 0, 2))
    return AlignedState(*(array(v, v.dtype) for v in
                          (decisions, ix.astype(np.int64), observed, available, until, value, known, reason)))


@dataclass(frozen=True)
class Feature(FrozenArrays):
    requestJson: str
    values: np.ndarray
    known: np.ndarray
    colour: np.ndarray
    run: np.ndarray
    position: np.ndarray
    aligned: AlignedState | None

    @property
    def request(self):
        return json.loads(self.requestJson)


@dataclass(frozen=True)
class PreparedAccount:
    schema: str
    identity: str
    targetId: str
    inputs: tuple[InputValues, ...]
    features: tuple[Feature, ...]

    @property
    def target(self):
        return next(v for v in self.inputs if v.inputId == self.targetId)

    @property
    def stepMs(self):
        return self.target.stepMs

    @property
    def preparationStartMs(self):
        return self.target.header["warmup_start_ms"]

    @property
    def preparationEndMs(self):
        return self.target.header["end_ms"]

    def feature(self, requestId):
        found = [f for f in self.features if f.request["requestId"] == requestId]
        require(len(found) == 1, "missing feature " + str(requestId))
        return found[0]


def bind(value):
    require(isinstance(value, PreparedInput), "PreparedInput required")
    identifier(value.inputId)
    h = data.validate_fingerprint_header(value.header)
    s = value.series
    require(isinstance(s, data.PreparedSeries), "native PreparedSeries required")
    require(type(s.timeframe_minutes) is int and s.timeframe_minutes == h["timeframe_minutes"], "header timeframe")
    t, v = s.timestamps, s.values
    require(isinstance(t, np.ndarray) and t.dtype == np.int64 and t.ndim == 1 and len(t) > 0, "native timestamps")
    require(isinstance(v, np.ndarray) and v.dtype == np.float64 and v.shape == (len(t), 5), "native OHLCV")
    data.validate_series(t, v)
    step = s.timeframe_minutes * 60000
    require(np.all(t % step == 0) and t[0] >= h["warmup_start_ms"] and
            int(t[-1]) + step <= h["end_ms"], "input bounds/grid")
    research = t >= h["start_ms"]
    segment = np.r_[True, np.diff(t) != step]
    require(research.any(), "no complete research bars")
    for actual, expected in ((s.research_mask, research), (s.segment_start, segment)):
        require(isinstance(actual, np.ndarray) and actual.dtype == bool and
                np.array_equal(actual, expected), "native masks/segments")
    require(type(s.research_start_index) is int and s.research_start_index == int(np.argmax(research)), "research start index")
    coverage = {k: integer(getattr(s, k), k) for k in ("base_row_count", "base_gap_count", "omitted_group_count")}
    expected = (h["end_ms"] - h["warmup_start_ms"]) // data.BASE_STEP_MS
    require(s.base_row_count + s.base_gap_count == expected and
            s.base_row_count >= len(t) * (step // data.BASE_STEP_MS) and
            s.omitted_group_count <= expected // (step // data.BASE_STEP_MS), "native coverage")
    require(isinstance(s.input_fingerprint, str) and re.fullmatch("[0-9a-f]{64}", s.input_fingerprint), "native fingerprint")
    return InputValues(value.inputId, canonical_json(h), s.input_fingerprint,
                       array(t, np.int64), array(v, np.float64), array(research, bool),
                       array(segment, bool), canonical_json(coverage))


def validate_request(request, inputs):
    require(isinstance(request, AccountFeatureRequest), "AccountFeatureRequest required")
    for name in ("requestId", "featureId", "inputId"):
        identifier(getattr(request, name))
    require(request.inputId in inputs and request.version == "1", "feature input/version")
    integer(request.priorBars, "priorBars")
    p = request.parameters
    require(type(p) is dict, "feature parameters")
    keys = {"ha": set(), "atr": {"length"}, "candleDirection": {"candleType", "confirmationBars"}}
    require(request.featureId in keys and set(p) == keys[request.featureId], "closed feature parameters")
    if request.featureId == "atr":
        integer(p["length"], "length", 1)
    if request.featureId == "candleDirection":
        require(p["candleType"] in ("real", "ha"), "candleType")
        integer(p["confirmationBars"], "confirmationBars", 1)


def colour_runs(op, cl, segments):
    colour = np.where(cl > op, 1, np.where(cl < op, -1, 0)).astype(np.int64)
    run, position = np.zeros(len(op), dtype=np.int64), np.zeros(len(op), dtype=np.int64)
    for i in range(len(op)):
        position[i] = 0 if segments[i] else position[i-1] + 1
        if colour[i]:
            run[i] = run[i-1] + 1 if position[i] and colour[i] == colour[i-1] else 1
    return colour, run, position


def ha_values(source):
    v = source.values
    close = (v[:, 0] + v[:, 1] + v[:, 2] + v[:, 3]) / 4.
    op = np.empty(len(v))
    for i in range(len(v)):
        op[i] = (v[i, 0] + v[i, 3]) / 2. if source.segmentStart[i] else (op[i-1] + close[i-1]) / 2.
    return np.column_stack((op, np.maximum(v[:, 1], np.maximum(op, close)),
                            np.minimum(v[:, 2], np.minimum(op, close)), close))


def prepare_inputs(target, sources, featureRequests):
    """Own native arrays once; features share recursion but retain request warmup."""
    require(isinstance(sources, tuple) and isinstance(featureRequests, tuple), "sources/featureRequests must be tuples")
    bindings = [bind(v) for v in (target,) + sources]
    require(len({v.inputId for v in bindings}) == len(bindings), "duplicate inputId")
    bindings.sort(key=lambda v: v.inputId)
    inputs = {v.inputId: v for v in bindings}
    bounds = lambda v: tuple(v.header[k] for k in ("warmup_start_ms", "start_ms", "end_ms"))
    require(all(bounds(v) == bounds(bindings[0]) for v in bindings), "shared preparation bounds")
    for r in featureRequests:
        validate_request(r, inputs)
    require(len({r.requestId for r in featureRequests}) == len(featureRequests), "duplicate requestId")
    requests = sorted(featureRequests, key=lambda r: r.requestId)
    recursion, features = {}, []  # local to this bounded preparation only
    decisions = inputs[target.inputId].timestamps + inputs[target.inputId].stepMs
    for r in requests:
        source = inputs[r.inputId]
        kind = "ha" if r.featureId == "ha" or (r.featureId == "candleDirection" and r.parameters["candleType"] == "ha") else "real"
        key = (r.inputId, kind)
        if key not in recursion:
            candles = ha_values(source) if kind == "ha" else source.values[:, :4]
            recursion[key] = (candles, *colour_runs(candles[:, 0], candles[:, 3], source.segmentStart))
        candles, colour, run, position = recursion[key]
        known = position >= r.priorBars
        values, aligned = candles, None
        if r.featureId == "atr":
            key = (r.inputId, "atr", r.parameters["length"])
            if key not in recursion:
                atr = np.full(len(candles), np.nan)
                starts = np.flatnonzero(source.segmentStart)
                for a, z in zip(starts, np.r_[starts[1:], len(candles)]):
                    v = source.values[a:z]
                    atr[a:z] = pine_atr(v[:, 1], v[:, 2], v[:, 3], r.parameters["length"])
                recursion[key] = atr
            values = recursion[key]
            known = known & np.isfinite(values)
        elif r.featureId == "candleDirection":
            values = np.where(run >= r.parameters["confirmationBars"], colour, 0).astype(np.float64)
            close = source.timestamps + source.stepMs
            rows = StateRows(close, close, close + source.stepMs, values, known,
                             source.header["warmup_start_ms"], source.header["end_ms"] + source.stepMs)
            aligned = align_states(rows, decisions)
        require(np.isfinite(values[known]).all(), "nonfinite prepared feature")
        features.append(Feature(canonical_json(asdict(r)), *(array(v, v.dtype) for v in
                                (values, known, colour, run, position)), aligned))
    identity = semantic_digest(dict(schema=SCHEMA, preparationVersion="1", alignmentVersion="1",
        seed="segment-first-real-open-close;pine-rma;neutral-break;priorBars-mask", validity="next-scheduled-close-exclusive",
        targetId=target.inputId, inputs=[dict(inputId=v.inputId, header=v.header,
        inputFingerprint=v.inputFingerprint) for v in bindings], requests=[asdict(r) for r in requests]))
    return PreparedAccount(SCHEMA, identity, target.inputId, tuple(bindings), tuple(features))
