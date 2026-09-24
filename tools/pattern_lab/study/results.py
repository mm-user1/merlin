"""Loading saved evidence and summarizing it descriptively.

``load_results`` exposes the raw primitives and the documented directional
observation view.  ``summarize_results`` accumulates compact numeric samples per
declared outcome group, so exact empirical quantiles are available without
materializing every directional row of the universe at once.

Reading is bounded per instrument: each needed raw table is decoded once, reused
across the declared groups and released before the next instrument.  Exact
quantiles still retain compact per-group sample arrays across instruments, which
is a separate memory cost from the one live instrument's frames.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from .. import manifest as pack_manifest
from . import builtins as study_builtins
from . import contracts, evidence
from . import observations as study_observations
from .builtins import EVIDENCE_VIEW_VERSION
from .contracts import ModelCase, OutcomeSpec

SUMMARY_SCHEMA_VERSION = 1
CURRENT_SUMMARY_SCHEMA_VERSION = 2
SUMMARY_VERSION_BY_STUDY = {1: 1, 2: 2}


def summary_schema_version(study_version):
    if type(study_version) is not int or study_version not in SUMMARY_VERSION_BY_STUDY:
        raise PatternLabDataError(f"summary: unsupported study version {study_version!r}; supported: 1, 2.")
    return SUMMARY_VERSION_BY_STUDY[study_version]

QUANTILES = (0.10, 0.25, 0.75, 0.90)
QUANTILE_LABELS = ("p10", "p25", "p75", "p90")

# How terminal one recorded job state is.  A published bundle outranks a later
# run-level failure, so completed work is never lost by reconciliation.
_STATE_PRECEDENCE = {
    evidence.STATE_NOT_STARTED: 0,
    evidence.STATE_ADMITTED: 1,
    evidence.STATE_FAILED: 2,
    evidence.STATE_COMPLETED: 3,
}

DISCLOSURES = (
    "Descriptive event study — statistical validation is not implemented in M2.",
    "Events may overlap: these are conditional observations, not an executable equity curve.",
    "Historical universe membership is not point-in-time verified; see the pack manifest.",
    "The reserved final-evaluation interval was used by the earlier prototype, so it is not "
    "certified historically untouched.",
    "Commission is included; slippage and funding are excluded by explicit decision.",
    "Neither event counts nor ticker breadth imply independent observations.",
)


def _case_from_json(payload: Mapping[str, Any]) -> ModelCase:
    return ModelCase(
        case_id=payload["case_id"],
        timeframe_minutes=int(payload["timeframe_minutes"]),
        parameters=dict(payload["parameters"]),
        outcomes=tuple(
            OutcomeSpec(item["name"], item["unit"], item.get("description", ""))
            for item in payload["outcomes"]
        ),
        primary=bool(payload["primary"]),
    )


@dataclass(frozen=True)
class StudyResults:
    """One run's frozen specification, status and verified job bundles."""

    run_root: Path
    request: Mapping[str, Any]
    protocol: Mapping[str, Any]
    family: Mapping[str, Any]
    source: Mapping[str, Any]
    provenance: Mapping[str, Any]
    status: Mapping[str, Any]
    completion: Mapping[str, Any] | None
    complete: bool
    jobs: Mapping[str, Mapping[str, Any]]
    job_states: Mapping[str, str]
    counts: Mapping[str, int]

    @property
    def completed_instruments(self) -> list[str]:
        return sorted(self.jobs)

    def table(self, instrument_id: str, name: str) -> pd.DataFrame:
        if name.startswith("sequential_"):
            reader = self.instrument_reader(instrument_id)
            try:
                return reader._checked_sequential()[name.removeprefix("sequential_")]
            finally:
                reader.release()
        return self._raw_table(instrument_id, name)

    def sequential_account(self, instrument_id, *, timeframe_minutes, variant_id, model_instance_id, case_id):
        """Checked attempts/trades/path for exactly one isolated account."""
        reader = self.instrument_reader(instrument_id)
        try:
            return reader.sequential_account(timeframe_minutes=timeframe_minutes,
                variant_id=variant_id, model_instance_id=model_instance_id, case_id=case_id)
        finally:
            reader.release()

    def _raw_table(self, instrument_id: str, name: str) -> pd.DataFrame:
        """Read one verified raw table of one completed job."""
        if instrument_id not in self.jobs:
            raise PatternLabDataError(
                f"{instrument_id}: no completed, verified job bundle is available in this run."
            )
        path = evidence.job_path(self.run_root, instrument_id) / f"{name}.parquet"
        if not path.is_file():
            raise PatternLabDataError(
                f"{instrument_id}: this job saved no {name!r} table. A metric that needs unsaved "
                "fields requires a new explicit study; nothing is guessed."
            )
        return evidence.read_table(path)

    def model_instance(self, model_instance_id: str) -> dict[str, Any]:
        for instance in self.family["models"]:
            if instance["model_instance_id"] == model_instance_id:
                return dict(instance)
        raise PatternLabDataError(f"unknown model instance {model_instance_id!r} in this run.")

    def cases(self, model_instance_id: str, timeframe_minutes: int) -> list[ModelCase]:
        instance = self.model_instance(model_instance_id)
        return [_case_from_json(item) for item in instance["cases"][str(int(timeframe_minutes))]]

    def case(self, model_instance_id: str, timeframe_minutes: int, case_id: str) -> ModelCase:
        for case in self.cases(model_instance_id, timeframe_minutes):
            if case.case_id == case_id:
                return case
        raise PatternLabDataError(f"unknown case {case_id!r} for model instance {model_instance_id!r}.")

    def instrument_reader(self, instrument_id: str) -> "InstrumentReader":
        """Return one instrument's bounded reader; the caller releases it.

        The reader decodes each needed raw table once and reuses it across the
        cases and groups the caller asks for, so an offline analysis does not
        open a fresh reader per group.  ``release()`` drops the frames; the
        existing lifecycle and the public observation semantics are unchanged.
        """
        return InstrumentReader(self, instrument_id)

    def observations(
        self,
        instrument_id: str,
        *,
        model_instance_id: str,
        timeframe_minutes: int,
        case_id: str,
        variant_id: str | None = None,
    ) -> pd.DataFrame:
        """Return one case's observation view for one instrument.

        With ``variant_id`` the rows are that variant's emitted events only; with
        no variant the rows are every eligible anchor, including non-events.
        Saved custom evidence is structurally validated first: a matching file
        hash proves the bytes, not that the rows are a complete, coherent sample.
        """
        reader = InstrumentReader(self, instrument_id)
        try:
            return reader.observations(
                instance=self.model_instance(model_instance_id),
                case=self.case(model_instance_id, timeframe_minutes, case_id),
                timeframe_minutes=int(timeframe_minutes),
                variant_id=variant_id,
            )
        finally:
            reader.release()


class InstrumentReader:
    """One instrument's decoded raw tables, reused across groups then released.

    Custom evidence is validated once per instrument and model instance, before
    any group filtering, so a missing or duplicated row cannot disappear into a
    group's inner join.
    """

    def __init__(self, results: StudyResults, instrument_id: str) -> None:
        self._results = results
        self._instrument_id = instrument_id
        self._tables: dict[str, pd.DataFrame] = {}
        self._validated: set[str] = set()
        self._anchors: dict[int, np.ndarray] | None = None

    @property
    def instrument_id(self) -> str:
        return self._instrument_id

    def table(self, name: str) -> pd.DataFrame:
        if name.startswith("sequential_"):
            return self.sequential_tables()[name.removeprefix("sequential_")]
        frame = self._tables.get(name)
        if frame is None:
            frame = self._results.table(self._instrument_id, name)
            self._tables[name] = frame
        return frame

    def _checked_sequential(self):
        from . import sequential
        from ..manifest import to_epoch_ms
        tables = {}
        for name in sequential.SCHEMAS:
            raw = "sequential_"+name
            if raw not in self._tables:
                self._tables[raw] = self._results._raw_table(self._instrument_id, raw)
            tables[name] = self._tables[raw]
        if "sequential" not in self._validated:
            result = self._results
            admitted = evidence.read_json(evidence.admitted_path(result.run_root,self._instrument_id))
            self._sequential_grouped = sequential.validate(tables,instrument_id=self._instrument_id,
                instances=[m for m in result.family["models"] if contracts.is_sequential(m)],
                variants=result.family["variants"],emissions=self.table("emissions"),rules=admitted["bracket_rules"],
                expected_bars=sequential.expected_bars(self.table("conditions"), result.jobs[self._instrument_id]["stats"],
                    to_epoch_ms(result.request["study"]["end_utc"])))
            self._validated.add("sequential")
        return tables

    def sequential_tables(self):
        """Checked caller-owned copies; edits cannot poison this reader's cache."""
        return {name:frame.copy(deep=True) for name,frame in self._checked_sequential().items()}

    def sequential_keys(self):
        """Account keys in declaration order, for one bounded instrument loop."""
        self._checked_sequential()
        return list(self._sequential_grouped["path"])

    def _account_key(self, timeframe_minutes, variant_id, model_instance_id, case_id):
        self._checked_sequential()
        key = (self._instrument_id,timeframe_minutes,variant_id,model_instance_id,case_id)
        if key not in self._sequential_grouped["path"]:
            raise PatternLabDataError(f"Unknown sequential account {key}")
        return key

    def sequential_account(self, *, timeframe_minutes, variant_id, model_instance_id, case_id):
        """Reuse validation/indexes; return independent attempts/trades/path copies."""
        key = self._account_key(timeframe_minutes,variant_id,model_instance_id,case_id)
        return {name:frame.iloc[self._sequential_grouped["indices"][name].get(key, [])].reset_index(drop=True).copy(deep=True)
                for name,frame in self._checked_sequential().items()}

    def sequential_coverage(self, *, timeframe_minutes, variant_id, model_instance_id, case_id):
        """Derived requested-end coverage; raw terminal flags mean last observed."""
        from . import sequential
        from ..manifest import to_epoch_ms
        key = self._account_key(timeframe_minutes,variant_id,model_instance_id,case_id)
        return sequential.coverage(self._sequential_grouped["path"][key], self._sequential_grouped["trades"][key],
                                   timeframe_minutes,to_epoch_ms(self._results.request["study"]["end_utc"]))

    def eligible_anchors(self) -> dict[int, np.ndarray]:
        """Every eligible study anchor per timeframe, from the saved conditions.

        The all-anchor conditions table is the run's own record of what the
        model was asked about; emissions are a filtered subset and could never
        prove that a row is missing.
        """
        if self._anchors is None:
            conditions = self.table("conditions")
            anchors: dict[int, np.ndarray] = {}
            if len(conditions):
                stamps = conditions["anchor_open_ms"].to_numpy(dtype=np.int64)
                timeframes = conditions["timeframe_minutes"].to_numpy(dtype=np.int64)
                for timeframe in np.unique(timeframes).tolist():
                    anchors[int(timeframe)] = np.unique(stamps[timeframes == timeframe])
            self._anchors = anchors
        return self._anchors

    def evidence_table(self, instance: Mapping[str, Any]) -> pd.DataFrame:
        """Return the verified raw table this model instance's cases expand from."""
        if contracts.is_sequential(instance):
            raise PatternLabDataError("Sequential models have account evidence, not per-anchor observations")
        if instance["evidence_kind"] == contracts.FIXED_HORIZON_EVIDENCE_KIND:
            return self.table("primitives")
        name = f"custom__{instance['model_instance_id']}"
        frame = self.table(name)
        if name not in self._validated:
            study_observations.validate_custom_table(
                frame,
                instance=instance,
                instrument_id=self._instrument_id,
                expected_anchors=self.eligible_anchors(),
                where=f"{self._instrument_id}: saved {name}.parquet",
            )
            self._validated.add(name)
        return frame

    def observations(
        self,
        *,
        instance: Mapping[str, Any],
        case: ModelCase,
        timeframe_minutes: int,
        variant_id: str | None = None,
    ) -> pd.DataFrame:
        frame = study_observations.expand_case(
            evidence_kind=instance["evidence_kind"],
            table=self.evidence_table(instance),
            model_instance_id=instance["model_instance_id"],
            case=case,
            timeframe_minutes=timeframe_minutes,
        )
        if variant_id is None:
            return frame
        return study_observations.join_events(
            frame, self.table("emissions"), variant_id=variant_id,
            timeframe_minutes=timeframe_minutes,
        )

    def release(self) -> None:
        """Drop this instrument's frames before the next instrument is read."""
        self._tables.clear()
        self._validated.clear()
        self._anchors = None
        if hasattr(self, "_sequential_grouped"):
            del self._sequential_grouped


# --------------------------------------------------------------------------
# loading and reconciliation
# --------------------------------------------------------------------------

def _reconcile(run_root: Path, status: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, int]]:
    """Merge the run-level status with the committed per-job records.

    A published bundle or a committed record is newer than the run-level status,
    and a claimed completed job must verify rather than quietly disappear.
    """
    records = evidence.read_job_records(run_root)
    states: dict[str, str] = {}
    for item in status["instruments"]:
        identifier = item["instrument_id"]
        candidates = [item["state"]]
        record = records.get(identifier)
        if record is not None:
            candidates.append(str(record.get("state", evidence.STATE_NOT_STARTED)))
        if evidence.job_path(run_root, identifier).is_dir():
            candidates.append(evidence.STATE_COMPLETED)
        states[identifier] = max(candidates, key=lambda state: _STATE_PRECEDENCE.get(state, 0))
    counts = {
        evidence.STATE_ADMITTED: 0,
        evidence.STATE_COMPLETED: 0,
        evidence.STATE_FAILED: 0,
        evidence.STATE_NOT_STARTED: 0,
    }
    for state in states.values():
        counts[state] = counts.get(state, 0) + 1
    counts["planned"] = len(states)
    return states, counts


def _verify_completion_agreement(
    run_root: Path,
    completion: Mapping[str, Any],
    *,
    status: Mapping[str, Any],
    counts: Mapping[str, int],
    provenance: Mapping[str, Any],
    family: Mapping[str, Any],
) -> None:
    """Cross-check a shape-verified completion record against the run's own facts.

    :func:`evidence.verify_completion` owns the published record's shape and the
    immutable file digests; the reconciled job counts and the run's verified
    provenance only exist here, so duplicated metadata that is merely
    well-typed is refused at this one boundary rather than in a second
    whole-run verification pass.
    """
    path = evidence.completion_path(run_root)
    if status.get("terminal_status") != evidence.TERMINAL_COMPLETED:
        raise PatternLabDataError(
            f"{path}: the completion record claims a completed study, but the run's terminal "
            f"status is {status.get('terminal_status')!r}.",
            error_code="corrupt_evidence",
        )
    recorded = {key: completion["counts"][key] for key in evidence.COMPLETION_COUNT_KEYS}
    reconciled = {key: int(counts.get(key, 0)) for key in evidence.COMPLETION_COUNT_KEYS}
    if recorded != reconciled:
        raise PatternLabDataError(
            f"{path}: the completion record's counts {recorded} contradict the run's reconciled "
            f"job counts {reconciled}.",
            error_code="corrupt_evidence",
        )
    planned = family.get("planned_job_count")
    if type(planned) is not int or planned != reconciled["planned"]:
        raise PatternLabDataError(
            f"{path}: the family's planned_job_count {planned!r} contradicts the run's "
            f"reconciled planned count {reconciled['planned']}.",
            error_code="corrupt_evidence",
        )
    if status.get("counts") != reconciled:
        raise PatternLabDataError(
            f"{path}: the terminal status counts contradict the run's reconciled job counts.",
            error_code="corrupt_evidence",
        )
    declared = provenance.get("identities")
    declared = dict(declared) if isinstance(declared, Mapping) else {}
    disagree = sorted(
        key
        for key in evidence.COMPLETION_IDENTITY_KEYS
        if declared.get(key) != completion["identities"][key]
    )
    if disagree:
        raise PatternLabDataError(
            f"{path}: the completion record's identities {disagree} contradict the verified "
            "provenance of the same run.",
            error_code="corrupt_evidence",
        )


def _load(run_root: Any, *, mode: str) -> StudyResults:
    root = evidence.require_run_directory(run_root)
    completion: Mapping[str, Any] | None = None
    # An existing completion record is verified in every mode: explicit partial
    # inspection is never a corruption bypass.
    if mode == "strict" or evidence.completion_path(root).is_file():
        completion = evidence.verify_completion(root)
    status = evidence.read_status(root)
    states, counts = _reconcile(root, status)
    provenance = dict(evidence.read_json(root / evidence.PROVENANCE_FILE))
    family = dict(evidence.read_json(root / evidence.FAMILY_FILE))
    sequential_models = [m for m in family["models"] if contracts.is_sequential(m)]
    request = dict(evidence.read_json(root / evidence.REQUEST_FILE))
    source = dict(evidence.read_json(root / evidence.SOURCE_FILE))
    if type(request.get("schema_version")) is not int or request["schema_version"] not in evidence.SUPPORTED_RUN_SCHEMA_VERSIONS:
        raise PatternLabDataError("request: unsupported saved study version.")
    if request.get("schema_version") == 2:
        from .validation import validate_saved_execution
        validate_saved_execution(request, source=str(root / evidence.REQUEST_FILE))
        if request["execution"]["kind"] == "validation":
            from ..candidate import verify_extension_generation
            verify_extension_generation(request["execution"]["candidate"], source.get("extensions"))
        if request["protocol"] != evidence.read_json(root / evidence.PROTOCOL_FILE):
            raise PatternLabDataError("request protocol contradicts separately frozen protocol.")
        for name, document in (("status", status), ("family", family), ("provenance", provenance)):
            if document.get("schema_version") != 2:
                raise PatternLabDataError(f"{name}: run version contradicts request version 2.")
        if completion is not None and completion.get("schema_version") != 2:
            raise PatternLabDataError("completion: run version contradicts request version 2.")
        if any(family.get(key) != request.get(key) for key in ("context", "execution")):
            raise PatternLabDataError("family: context/execution contradicts saved request.")
    if completion is not None:
        _verify_completion_agreement(
            root, completion, status=status, counts=counts, provenance=provenance, family=family
        )
    jobs = {
        identifier: evidence.verify_job_bundle(root, identifier)
        for identifier, state in sorted(states.items())
        if state == evidence.STATE_COMPLETED
    }
    for identifier, bundle in jobs.items():
        evidence.check_table_coverage(family["models"], [item["table"] for item in bundle["files"]], where=identifier)
    consistent = (
        status["terminal_status"] == evidence.TERMINAL_COMPLETED
        and counts["planned"] == counts[evidence.STATE_COMPLETED]
        and counts[evidence.STATE_FAILED] == 0
        and counts[evidence.STATE_NOT_STARTED] == 0
        and counts[evidence.STATE_ADMITTED] == 0
    )
    complete = consistent if mode == "prepublication" else (completion is not None and consistent)
    if mode in ("strict", "prepublication") and not complete:
        raise PatternLabDataError(
            f"{root}: this run is not a verified completed study; terminal status is "
            f"{status['terminal_status']!r} with counts {counts}. Use "
            "load_results(run_root, allow_partial=True) to inspect it instead.",
            error_code="incomplete_run",
        )
    result = StudyResults(
        run_root=root,
        request=request,
        protocol=dict(evidence.read_json(root / evidence.PROTOCOL_FILE)),
        family=family,
        source=source,
        provenance=provenance,
        status=status,
        completion=completion,
        complete=complete,
        jobs=jobs,
        job_states=states,
        counts=counts,
    )
    if sequential_models:
        from . import bracket
        from ..manifest import to_epoch_ms
        if request["schema_version"] != 2:
            raise PatternLabDataError("Sequential evidence requires study version 2")
        if family["models"] != request["models"]:
            raise PatternLabDataError("Sequential request/family models disagree")
        by_model={m["model_instance_id"]:bracket.prior_bars(m["settings"]) for m in sequential_models
                  if bracket.prior_bars(m["settings"])}
        by_variant={v["variant_id"]:v["required_prior_bars"] for v in family["variants"]}
        needed=max([*by_model.values(),*by_variant.values()],default=0)
        warmup=to_epoch_ms(request["study"]["start_utc"])-to_epoch_ms(request["study"]["warmup_start_utc"])
        expected_warmup={str(tf):{"declared_warmup_bars":warmup//(tf*60000),"required_prior_bars":needed,
                                 "by_variant":by_variant,**({"by_model":by_model} if by_model else {})}
                         for tf in family["timeframes_minutes"]}
        if family["warmup_requirements"] != expected_warmup or any(f["declared_warmup_bars"]<needed for f in expected_warmup.values()):
            raise PatternLabDataError("Saved sequential warmup contract disagrees")
        for model in sequential_models:
            settings=bracket.validate_settings(model["settings"],family["timeframes_minutes"])
            if settings!=model["settings"] or model["cases"]!={str(tf):[c.as_json() for c in bracket.resolve_cases(settings,tf)] for tf in family["timeframes_minutes"]}:
                raise PatternLabDataError("Saved sequential settings/cases disagree")
        for identifier in result.completed_instruments:
            if evidence.read_json(evidence.admitted_path(root,identifier))["warmup_requirements"] != expected_warmup:
                raise PatternLabDataError("Admitted sequential warmup contract disagrees")
            reader=result.instrument_reader(identifier)
            try:
                reader._checked_sequential()
            finally:
                reader.release()
    return result


def load_results(run_root: Any, *, allow_partial: bool = False) -> StudyResults:
    """Load a run's saved evidence.

    Without ``allow_partial`` a verified completed run is required: the
    completion record, the immutable evidence it names and the terminal state
    must all agree.  ``allow_partial=True`` is the explicit partial-inspection
    API: it exposes the planned, admitted, completed, failed and not-started
    counts reconciled from the committed per-job records and loads only intact
    completed bundles.  An existing completion record is still verified, and a
    job claimed completed whose bundle is missing or corrupt fails; partial
    inspection is never a corruption bypass and never certifies completion.
    """
    return _load(run_root, mode="partial" if allow_partial else "strict")


def load_prepublication_results(run_root: Any) -> StudyResults:
    """Internal: read a run the coordinator has just finished, before sealing it.

    The coordinator has published and verified every planned bundle and written
    the terminal status, but the completion record is deliberately written last.
    This trusted internal call lets that first report describe the run exactly as
    a later regeneration does, without giving the public partial reader a way to
    claim premature success.  It is not exported as a public flag.
    """
    return _load(run_root, mode="prepublication")


# --------------------------------------------------------------------------
# descriptive aggregation
# --------------------------------------------------------------------------

def _outcome_columns(frame: pd.DataFrame, name: str, kind: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return one outcome's values, validity mask and reason labels."""
    if kind == contracts.FIXED_HORIZON_EVIDENCE_KIND:
        prefix = "path" if name in ("mfe", "mae") else "return"
        valid = frame[f"{prefix}_valid"].to_numpy(dtype=bool)
        reason = frame[f"{prefix}_reason"].to_numpy()
    else:
        reason = frame[f"{name}__reason"].to_numpy()
        valid = ~pd.isna(frame[name]).to_numpy()
    values = frame[name].to_numpy(dtype=np.float64)
    return values, valid, reason


def _describe(sample: np.ndarray) -> dict[str, Any]:
    """Arithmetic mean, median, empirical quantiles and positive fraction."""
    if sample.size == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            **{label: None for label in QUANTILE_LABELS},
            "fraction_positive": None,
        }
    quantiles = np.quantile(sample, QUANTILES, method="linear")
    return {
        "n": int(sample.size),
        "mean": float(np.mean(sample)),
        "median": float(np.median(sample)),
        **{label: float(value) for label, value in zip(QUANTILE_LABELS, quantiles)},
        "fraction_positive": float(np.count_nonzero(sample > 0.0) / sample.size),
    }


@dataclass
class _GroupAccumulator:
    group: Mapping[str, Any]
    outcomes: tuple[str, ...]
    units: Mapping[str, str]
    samples: dict[str, list[np.ndarray]] = field(default_factory=dict)
    ticker_means: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    invalid: dict[str, dict[str, int]] = field(default_factory=dict)
    valid_counts: dict[str, int] = field(default_factory=dict)
    events: int = 0
    episodes: int = 0
    tickers_with_events: list[str] = field(default_factory=list)
    zero_support_tickers: list[str] = field(default_factory=list)

    def prepare(self) -> None:
        for name in self.outcomes:
            self.samples.setdefault(name, [])
            self.ticker_means.setdefault(name, [])
            self.invalid.setdefault(name, {})
            self.valid_counts.setdefault(name, 0)


def _attach_metrics(results: StudyResults, groups: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Attach recorded custom metric values; regeneration imports no saved module."""
    recorded = recorded_metrics(results.run_root)
    if recorded is None:
        for group in groups:
            group["metrics"] = []
        return {"declarations": [], "values": []}
    index: dict[tuple[str, str, int, str], list[dict[str, Any]]] = {}
    for record in recorded["values"]:
        key = (
            record["variant_id"],
            record["model_instance_id"],
            int(record["timeframe_minutes"]),
            record["case_id"],
        )
        index.setdefault(key, []).append(record)
    for group in groups:
        key = (
            group["variant_id"],
            group["model_instance_id"],
            int(group["timeframe_minutes"]),
            group["case_id"],
        )
        group["metrics"] = index.get(key, [])
    return recorded


def summarize_results(results: StudyResults) -> dict[str, Any]:
    """Summarize saved observations by resolved model case, one instrument at a time."""
    summary_version = summary_schema_version(results.request.get("schema_version"))
    family = results.family
    accumulators: dict[tuple[str, str, int, str], _GroupAccumulator] = {}
    instances = {item["model_instance_id"]: item for item in family["models"]}
    variants = {item["variant_id"]: item for item in family["variants"]}
    cases: dict[tuple[str, str, int, str], ModelCase] = {}

    for group in family["groups"]:
        key = (
            group["variant_id"],
            group["model_instance_id"],
            int(group["timeframe_minutes"]),
            group["case_id"],
        )
        instance = instances[group["model_instance_id"]]
        if contracts.is_sequential(instance):
            continue
        case = next(
            item
            for item in instance["cases"][str(int(group["timeframe_minutes"]))]
            if item["case_id"] == group["case_id"]
        )
        cases[key] = _case_from_json(case)
        accumulator = _GroupAccumulator(
            group=group,
            outcomes=tuple(item["name"] for item in case["outcomes"]),
            units={item["name"]: item["unit"] for item in case["outcomes"]},
        )
        accumulator.prepare()
        accumulators[key] = accumulator

    planned_tickers = [item["instrument_id"] for item in family["instruments"]]
    for instrument_id in results.completed_instruments:
        reader = InstrumentReader(results, instrument_id)
        try:
            episodes = reader.table("episodes")
            for key, accumulator in accumulators.items():
                variant_id, instance_id, timeframe, case_id = key
                variant = variants[variant_id]
                instance = instances[instance_id]
                frame = reader.observations(
                    instance=instance,
                    case=cases[key],
                    timeframe_minutes=timeframe,
                    variant_id=variant_id,
                )
                accumulator.events += int(len(frame))
                accumulator.episodes += int(
                    len(
                        episodes.loc[
                            (episodes["condition_id"] == variant["condition_id"])
                            & (episodes["timeframe_minutes"] == timeframe)
                        ]
                    )
                )
                has_valid = False
                for name in accumulator.outcomes:
                    values, valid, reason = _outcome_columns(frame, name, instance["evidence_kind"])
                    sample = values[valid]
                    sample = sample[np.isfinite(sample)]
                    if sample.size:
                        accumulator.samples[name].append(sample)
                        accumulator.ticker_means[name].append((instrument_id, float(np.mean(sample))))
                        accumulator.valid_counts[name] += int(sample.size)
                        has_valid = True
                    for label in reason[~valid]:
                        text = str(label)
                        accumulator.invalid[name][text] = accumulator.invalid[name].get(text, 0) + 1
                if len(frame):
                    accumulator.tickers_with_events.append(instrument_id)
                if not has_valid:
                    accumulator.zero_support_tickers.append(instrument_id)
        finally:
            reader.release()

    groups: list[dict[str, Any]] = []
    for key, accumulator in accumulators.items():
        variant_id, instance_id, timeframe, case_id = key
        instance = instances[instance_id]
        variant = variants[variant_id]
        case = next(
            item for item in instance["cases"][str(timeframe)] if item["case_id"] == case_id
        )
        outcomes = []
        for name in accumulator.outcomes:
            pooled = (
                np.concatenate(accumulator.samples[name])
                if accumulator.samples[name]
                else np.zeros(0, dtype=np.float64)
            )
            ticker_means = accumulator.ticker_means[name]
            equal_ticker = (
                float(np.mean([value for _identifier, value in ticker_means]))
                if ticker_means
                else None
            )
            outcomes.append(
                {
                    "name": name,
                    "unit": accumulator.units[name],
                    "valid_count": accumulator.valid_counts[name],
                    "invalid_count": int(accumulator.events - accumulator.valid_counts[name]),
                    "invalid_by_reason": dict(sorted(accumulator.invalid[name].items())),
                    "event_weighted": _describe(pooled),
                    "equal_ticker": {
                        "tickers": len(ticker_means),
                        "mean_of_ticker_means": equal_ticker,
                    },
                    "per_ticker": [
                        {"instrument_id": identifier, "mean": value}
                        for identifier, value in sorted(ticker_means)
                    ],
                }
            )
        groups.append(
            {
                "variant_id": variant_id,
                "hypothesis_id": variant["hypothesis_id"],
                "occurrence": variant["occurrence"],
                "condition_id": variant["condition_id"],
                "model_instance_id": instance_id,
                "model_id": instance["model_id"],
                "evidence_kind": instance["evidence_kind"],
                "timeframe_minutes": timeframe,
                "case_id": case_id,
                "primary": bool(case["primary"]),
                "case_parameters": dict(case["parameters"]),
                "events": accumulator.events,
                "episodes": accumulator.episodes,
                "tickers_with_events": len(accumulator.tickers_with_events),
                "tickers_planned": len(planned_tickers),
                "zero_support_tickers": sorted(accumulator.zero_support_tickers),
                "outcomes": outcomes,
            }
        )

    sequential_accounts=[]
    sequential_rules={}
    sequential_models=[m for m in family["models"] if contracts.is_sequential(m)]
    if sequential_models:
        from . import sequential
        for identifier in results.completed_instruments:
            reader=results.instrument_reader(identifier)
            try:
                reader._checked_sequential()
                sequential_accounts.extend(sequential.summaries(reader._sequential_grouped,sequential_models,
                    requested_end_ms=pack_manifest.to_epoch_ms(results.request["study"]["end_utc"])))
                admitted=evidence.read_json(evidence.admitted_path(results.run_root,identifier))
                sequential_rules[identifier]={"semantic":admitted["bracket_rules"],"snapshot":admitted["rule_snapshot"]}
            finally:
                reader.release()
    return {
        "schema_version": summary_version,
        **({"sequential_accounts":sequential_accounts,"sequential_rules":sequential_rules} if sequential_models else {}),
        **({"context": dict(results.request["context"]),
            "context_admission": evidence.read_json(results.run_root / evidence.CONTEXT_ADMISSION_FILE),
            "context_diagnostics": evidence.read_json(results.run_root / evidence.CONTEXT_FILE),
            "execution": dict(results.request["execution"])}
           if results.request["schema_version"] == 2 and (results.run_root / evidence.CONTEXT_FILE).is_file() else {}),
        "evidence_view_version": EVIDENCE_VIEW_VERSION,
        "run_root": str(results.run_root),
        "study_name": results.request["study_name"],
        "notes": results.request.get("notes"),
        "complete": results.complete,
        "counts": dict(results.counts),
        "study": dict(results.request["study"]),
        "protocol": dict(results.protocol),
        "timeframes_minutes": list(family["timeframes_minutes"]),
        "instruments": list(family["instruments"]),
        "completed_instruments": results.completed_instruments,
        "variants": list(family["variants"]),
        "models": [
            {
                "model_instance_id": item["model_instance_id"],
                "model_id": item["model_id"],
                "model_version": item["model_version"],
                "evidence_kind": item["evidence_kind"],
                "settings": item["settings"],
            }
            for item in family["models"]
        ],
        "anchor_convention": family["anchor_convention"],
        "warmup_requirements": family["warmup_requirements"],
        "identities": dict(results.provenance["identities"]),
        "manifest": dict(results.provenance["manifest"]),
        "quantile_convention": "NumPy linear empirical quantiles",
        "pooling": (
            "Primary pooling is event-weighted; the equal-ticker companion is the mean of defined "
            "per-ticker means. Averaged per-ticker quantiles are not pooled quantiles."
        ),
        "minimum_support": "One valid event per group and ticker.",
        "disclosures": list(DISCLOSURES),
        "metrics": _attach_metrics(results, groups),
        "groups": groups,
    }


# --------------------------------------------------------------------------
# declared summary metrics
# --------------------------------------------------------------------------

METRICS_SCHEMA_VERSION = 1


def compute_metric_values(results: StudyResults, declarations: Sequence[Any]) -> dict[str, Any]:
    """Compute each declared metric once per group, or record why it is unavailable.

    A metric declares the saved columns it needs.  When a group's observation
    view does not expose them the metric is recorded as explicitly unavailable;
    nothing is guessed, and a descriptive metric never becomes a selection
    objective.

    One group's observation view is assembled once per instrument and shared by
    every declared metric of that group.  A metric may need whole-group rows, so
    the frames stay bounded to the group being computed; the deliberate tradeoff
    is that a later group reads those tables again rather than retaining every
    group's frames at once.
    """
    family = results.family
    instances = {item["model_instance_id"]: item for item in family["models"]}
    values: list[dict[str, Any]] = []
    for group in family["groups"]:
        timeframe = int(group["timeframe_minutes"])
        instance = instances[group["model_instance_id"]]
        if contracts.is_sequential(instance):
            continue
        case = results.case(group["model_instance_id"], timeframe, group["case_id"])
        collected: dict[str, list[pd.DataFrame]] = {
            declaration.declaration_id: [] for declaration in declarations
        }
        missing: dict[str, list[str]] = {
            declaration.declaration_id: [] for declaration in declarations
        }
        for instrument_id in results.completed_instruments:
            reader = InstrumentReader(results, instrument_id)
            try:
                frame = reader.observations(
                    instance=instance,
                    case=case,
                    timeframe_minutes=timeframe,
                    variant_id=group["variant_id"],
                )
                for declaration in declarations:
                    if missing[declaration.declaration_id]:
                        continue
                    descriptor = contracts.metric(declaration.metric_id)
                    absent = [
                        name for name in descriptor.required_columns if name not in frame.columns
                    ]
                    if absent:
                        missing[declaration.declaration_id] = absent
                        collected[declaration.declaration_id] = []
                        continue
                    collected[declaration.declaration_id].append(
                        frame.loc[:, list(descriptor.required_columns)]
                    )
            finally:
                reader.release()
        for declaration in declarations:
            descriptor = contracts.metric(declaration.metric_id)
            record = {
                "declaration_id": declaration.declaration_id,
                "metric_id": declaration.metric_id,
                "metric_version": declaration.version,
                "unit": declaration.unit,
                "variant_id": group["variant_id"],
                "model_instance_id": group["model_instance_id"],
                "timeframe_minutes": timeframe,
                "case_id": group["case_id"],
            }
            absent = missing[declaration.declaration_id]
            if absent:
                record.update(
                    {"availability": "missing_inputs", "missing_columns": absent, "value": None}
                )
            else:
                frames = collected[declaration.declaration_id]
                pooled = (
                    pd.concat(frames, ignore_index=True)
                    if frames
                    else pd.DataFrame(columns=list(descriptor.required_columns))
                )
                computed = descriptor.compute(pooled)
                if computed is not None and not np.isfinite(float(computed)):
                    raise PatternLabDataError(
                        f"metric {declaration.metric_id!r}: returned a non-finite value."
                    )
                record.update(
                    {
                        "availability": "available",
                        "missing_columns": [],
                        "value": None if computed is None else float(computed),
                    }
                )
            values.append(record)
    return {
        "schema_version": METRICS_SCHEMA_VERSION,
        "declarations": [declaration.as_json() for declaration in declarations],
        "values": values,
    }


def recorded_metrics(run_root: Path) -> dict[str, Any] | None:
    """Read the immutable recorded metric values, when a run declared any."""
    path = Path(run_root) / evidence.METRICS_FILE
    if not path.is_file():
        return None
    return dict(evidence.read_json(path))
