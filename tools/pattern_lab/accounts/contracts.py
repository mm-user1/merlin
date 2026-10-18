"""Closed account contracts, owned immutable arrays and content identities."""
from __future__ import annotations

from dataclasses import dataclass, field, fields, asdict
from typing import TYPE_CHECKING
import hashlib
import json
import math
import re

import numpy as np

from .. import PatternLabDataError
from ..data import PreparedSeries
from ..study.contracts import canonical_json, semantic_digest

if TYPE_CHECKING:
    import pyarrow as pa
    from ..study.bracket_rules import ExecutionRules

SCHEMA = "pattern-lab.account.v1"


def require(condition, message):
    if not condition:
        raise PatternLabDataError("account: " + message)


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, name + ": invalid integer")
    return value


def number(value, name, minimum=0, inclusive=False):
    require(type(value) in (int, float) and math.isfinite(value) and
            (value >= minimum if inclusive else value > minimum), name + ": invalid number")
    return float(value)


def identifier(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", value),
            "invalid identifier")


def array(value, dtype, shape=None):
    require(isinstance(value, np.ndarray) and value.dtype == np.dtype(dtype) and
            (shape is None or value.shape == shape), "array dtype/shape")
    # A bytes-backed array cannot be made writable, including through a caller alias.
    return np.frombuffer(value.tobytes(order="C"), dtype=value.dtype).reshape(value.shape)


class FrozenArrays:
    """Restore actual read-only buffers after ordinary pickle / Windows spawn."""
    def __setstate__(self, state):
        for name, value in state.items():
            if isinstance(value, np.ndarray):
                value = array(value, value.dtype)
            object.__setattr__(self, name, value)


@dataclass(frozen=True)
class PreparedInput:
    inputId: str
    series: PreparedSeries
    header: dict


@dataclass(frozen=True)
class AccountFeatureRequest:
    requestId: str
    featureId: str
    version: str
    inputId: str
    parameters: dict
    priorBars: int


@dataclass(frozen=True)
class AccountWindow:
    startMs: int
    endMs: int


@dataclass(frozen=True)
class ExecutionPolicy:
    mode: str
    direction: str
    rewardRisk: float | None
    stopUpdates: bool
    maxHoldingDays: float | None  # deliberately required, including explicit null
    initialCapital: float = 10000.
    riskPct: float = 2.
    commissionPctPerSide: float = .05
    maxLeverage: float = 8.

    def __post_init__(self):
        validate_mode(self.mode, self.stopUpdates)
        require(self.direction in ("long", "short", "both"), "direction")
        for name in ("initialCapital", "riskPct", "maxLeverage"):
            object.__setattr__(self, name, number(getattr(self, name), name))
        object.__setattr__(self, "commissionPctPerSide", number(
            self.commissionPctPerSide, "commissionPctPerSide", inclusive=True))
        require(self.riskPct <= 100 and self.commissionPctPerSide < 100, "policy limits")
        if self.mode == "fixedBracket":
            object.__setattr__(self, "rewardRisk", number(self.rewardRisk, "rewardRisk"))
            if self.maxHoldingDays is not None:
                object.__setattr__(self, "maxHoldingDays", number(self.maxHoldingDays, "maxHoldingDays"))
        else:
            require(self.rewardRisk is None and self.maxHoldingDays is None, "signalManaged target/expiry")


def validate_mode(mode, updates):
    require(mode in ("fixedBracket", "signalManaged") and type(updates) is bool,
            "mode/stopUpdates")
    require(mode != "fixedBracket" or not updates, "fixedBracket stopUpdates")


@dataclass(frozen=True)
class HARecipe:
    haRequestId: str
    stopProvider: str = "haLast"
    entryRunLength: int = 3
    exitRunLength: int = 1
    atrRequestId: str | None = None
    swingBars: int = 2
    atrMultiplier: float = 2.


@dataclass(frozen=True)
class SideCommands(FrozenArrays):
    entry: np.ndarray
    entryKnown: np.ndarray
    initialStop: np.ndarray
    initialStopKnown: np.ndarray
    exit: np.ndarray
    exitKnown: np.ndarray
    proposedStop: np.ndarray
    proposedStopKnown: np.ndarray

    def __post_init__(self):
        require(isinstance(self.entry, np.ndarray) and self.entry.ndim == 1, "entry shape")
        shape = self.entry.shape
        for f in fields(self):
            dtype = np.float64 if f.name in ("initialStop", "proposedStop") else bool
            object.__setattr__(self, f.name, array(getattr(self, f.name), dtype, shape))
        for name in ("initialStop", "proposedStop"):
            values, known = getattr(self, name), getattr(self, name + "Known")
            require(not np.isinf(values).any() and np.isfinite(values[known]).all(),
                    name + ": infinity or known NaN")


def command_array_digest(arrays, metadata):
    """Version 1: named little-endian C buffers, canonical NaNs, signed zero intact."""
    records = []
    for name, value in sorted(arrays.items()):
        require(isinstance(value, np.ndarray) and value.dtype.kind in "bif" and
                value.dtype.itemsize in (1, 8), "unsupported digest dtype")
        dtype = "bool" if value.dtype.kind == "b" else "int64" if value.dtype.kind == "i" else "float64"
        value = np.array(value, dtype={"bool": "?", "int64": "<i8", "float64": "<f8"}[dtype], order="C")
        if dtype == "float64":
            require(not np.isinf(value).any(), "infinite digest array")
            value[np.isnan(value)] = np.nan
        records.append(dict(name=name, dtype=dtype, shape=list(value.shape),
                            hash=hashlib.sha256(value.tobytes()).hexdigest()))
    return semantic_digest(dict(version="account-command-arrays.1", arrays=records, metadata=metadata))


def metadata_json(value):
    if isinstance(value, str):
        value = json.loads(value)
    require(isinstance(value, dict), "provenance must be a JSON object")
    return canonical_json(value)


@dataclass(frozen=True)
class EntryFilter(FrozenArrays):
    preparationId: str
    decisionMs: np.ndarray
    known: np.ndarray
    allowLong: np.ndarray
    allowShort: np.ndarray
    sourceIndex: np.ndarray
    observedAtMs: np.ndarray
    availableAtMs: np.ndarray
    validUntilMs: np.ndarray
    unknownReason: np.ndarray  # 0 known, 1 before first, 2 unknown row, 3 expired
    provenance: str | dict
    identity: str = field(init=False)

    def __post_init__(self):
        require(isinstance(self.decisionMs, np.ndarray) and self.decisionMs.ndim == 1, "filter shape")
        shape = self.decisionMs.shape
        arrays = {}
        for f in fields(self):
            v = getattr(self, f.name, None)
            if isinstance(v, np.ndarray):
                v = array(v, bool if f.name in ("known", "allowLong", "allowShort") else np.int64, shape)
                arrays[f.name] = v
                object.__setattr__(self, f.name, v)
        require(len(arrays) == 9, "filter requires typed arrays")
        require(np.all(np.diff(self.decisionMs) > 0), "filter decisions")
        present = self.sourceIndex >= 0
        require(np.all(self.sourceIndex >= -1) and np.all(np.isin(self.unknownReason, [0, 1, 2, 3])), "filter source/reason")
        require(np.array_equal(self.known, self.unknownReason == 0), "filter known/reason")
        require(np.array_equal(~present, self.unknownReason == 1) and
                np.all(np.diff(self.sourceIndex) >= 0), "filter source selection")
        require(np.all(~self.known | present), "known filter without source")
        require(np.all(self.observedAtMs[present] <= self.availableAtMs[present]) and
                np.all(self.availableAtMs[present] <= self.decisionMs[present]) and
                np.all(self.availableAtMs[present] < self.validUntilMs[present]) and
                np.all(self.decisionMs[self.known] < self.validUntilMs[self.known]), "filter timing")
        require(np.array_equal(present & (self.decisionMs >= self.validUntilMs), self.unknownReason == 3),
                "filter exclusive expiry")
        object.__setattr__(self, "provenance", metadata_json(self.provenance))
        object.__setattr__(self, "identity", command_array_digest(arrays, dict(
            preparationId=self.preparationId, provenance=json.loads(self.provenance))))


@dataclass(frozen=True)
class CommandBundle(FrozenArrays):
    preparationId: str
    decisionMs: np.ndarray
    long: SideCommands
    short: SideCommands
    mode: str
    stopUpdates: bool
    provenance: str | dict
    entryFilter: EntryFilter | None = None
    identity: str = field(init=False)

    def __post_init__(self):
        validate_mode(self.mode, self.stopUpdates)
        require(isinstance(self.decisionMs, np.ndarray) and self.decisionMs.ndim == 1, "decisions shape")
        object.__setattr__(self, "decisionMs", array(self.decisionMs, np.int64))
        require(np.all(np.diff(self.decisionMs) > 0), "decisions order")
        arrays = {"decisionMs": self.decisionMs}
        for name in ("long", "short"):
            side = getattr(self, name)
            require(isinstance(side, SideCommands) and side.entry.shape == self.decisionMs.shape, "side commands")
            require(self.mode != "fixedBracket" or not np.any(side.exit & side.exitKnown), "active bracket exits")
            require(self.stopUpdates or not side.proposedStopKnown.any(), "active disabled proposals")
            arrays.update({name + "." + f.name: getattr(side, f.name) for f in fields(side)})
        filt = self.entryFilter
        require(filt is None or isinstance(filt, EntryFilter), "exactly one EntryFilter or None required")
        if filt is not None:
            require(filt.preparationId == self.preparationId and
                    np.array_equal(filt.decisionMs, self.decisionMs), "filter binding")
            arrays.update({"filter." + f.name: getattr(filt, f.name) for f in fields(filt)
                           if isinstance(getattr(filt, f.name), np.ndarray)})
        object.__setattr__(self, "provenance", metadata_json(self.provenance))
        object.__setattr__(self, "identity", command_array_digest(arrays, dict(
            schema=SCHEMA, preparationId=self.preparationId, mode=self.mode, stopUpdates=self.stopUpdates,
            provenance=json.loads(self.provenance), entryFilter=None if filt is None else filt.identity)))


def validate_bundle(prepared, commands):
    require(isinstance(commands, CommandBundle) and commands.preparationId == prepared.identity,
            "command preparation binding")
    require(np.array_equal(commands.decisionMs, prepared.target.timestamps + prepared.stepMs), "decisionMs must equal close")


@dataclass(frozen=True)
class Coverage:
    requestedStartMs: int
    requestedEndMs: int
    firstObservedOpenMs: int | None
    lastObservedCloseMs: int | None
    expectedBars: int
    observedBars: int
    missingBars: int
    missingHeadSlots: int
    missingTailSlots: int


@dataclass(frozen=True)
class AccountResult:
    schema: str
    identity: str
    preparationId: str
    commandId: str
    policy: ExecutionPolicy
    window: AccountWindow
    rules: ExecutionRules
    coverage: Coverage
    terminalStatus: str
    finalBalance: float
    unknownExitDecisions: int
    commands: CommandBundle  # shared immutable known masks, timing and provider provenance
    attempts: pa.Table  # explicit schemas in evidence.py
    trades: pa.Table
    proposals: pa.Table
    exitCommands: pa.Table
    path: pa.Table
