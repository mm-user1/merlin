"""Loading saved evidence and summarizing it descriptively.

``load_results`` exposes the raw primitives and the documented directional
observation view.  ``summarize_results`` accumulates compact numeric samples per
declared outcome group, so exact empirical quantiles are available without
materializing every directional row of the universe at once.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import PatternLabDataError
from . import builtins as study_builtins
from . import contracts, evidence
from .builtins import EVIDENCE_VIEW_VERSION
from .contracts import ModelCase, OutcomeSpec

SUMMARY_SCHEMA_VERSION = 1

QUANTILES = (0.10, 0.25, 0.75, 0.90)
QUANTILE_LABELS = ("p10", "p25", "p75", "p90")

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

    @property
    def counts(self) -> dict[str, int]:
        return dict(self.status["counts"])

    @property
    def completed_instruments(self) -> list[str]:
        return sorted(self.jobs)

    def table(self, instrument_id: str, name: str) -> pd.DataFrame:
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
        """
        instance = self.model_instance(model_instance_id)
        case = self.case(model_instance_id, timeframe_minutes, case_id)
        if instance["evidence_kind"] == contracts.FIXED_HORIZON_EVIDENCE_KIND:
            primitives = self.table(instrument_id, "primitives")
            selection = primitives.loc[
                (primitives["model_instance_id"] == model_instance_id)
                & (primitives["timeframe_minutes"] == int(timeframe_minutes))
            ]
            frame = study_builtins.expand_fixed_horizon_case(selection.reset_index(drop=True), case)
        else:
            custom = self.table(instrument_id, f"custom__{model_instance_id}")
            frame = custom.loc[
                (custom["case_id"] == case_id)
                & (custom["timeframe_minutes"] == int(timeframe_minutes))
            ].reset_index(drop=True)
        if variant_id is None:
            return frame
        emissions = self.table(instrument_id, "emissions")
        events = emissions.loc[
            (emissions["variant_id"] == variant_id)
            & (emissions["timeframe_minutes"] == int(timeframe_minutes)),
            ["anchor_open_ms", "event_id", "episode_id"],
        ]
        return frame.merge(events, on="anchor_open_ms", how="inner")


def _verify_jobs(run_root: Path, status: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    jobs: dict[str, dict[str, Any]] = {}
    for record in status["instruments"]:
        if record["state"] != evidence.STATE_COMPLETED:
            continue
        jobs[record["instrument_id"]] = evidence.verify_job_bundle(run_root, record["instrument_id"])
    return jobs


def load_results(run_root: Any, *, allow_partial: bool = False) -> StudyResults:
    """Load a run's saved evidence.

    Without ``allow_partial`` a completed run is required and its immutable
    evidence is verified against the completion record.  ``allow_partial=True``
    is the explicit partial-inspection API: it exposes the planned, admitted,
    completed, failed and not-started counts and loads only intact completed
    bundles.  It is never a corruption bypass.
    """
    root = evidence.require_run_directory(run_root)
    completion: Mapping[str, Any] | None = None
    if not allow_partial:
        completion = evidence.verify_completion(root)
    elif evidence.completion_path(root).is_file():
        completion = dict(evidence.read_json(evidence.completion_path(root)))
    status = evidence.read_status(root)
    jobs = _verify_jobs(root, status)
    return StudyResults(
        run_root=root,
        request=dict(evidence.read_json(root / evidence.REQUEST_FILE)),
        protocol=dict(evidence.read_json(root / evidence.PROTOCOL_FILE)),
        family=dict(evidence.read_json(root / evidence.FAMILY_FILE)),
        source=dict(evidence.read_json(root / evidence.SOURCE_FILE)),
        provenance=dict(evidence.read_json(root / evidence.PROVENANCE_FILE)),
        status=status,
        completion=completion,
        # The terminal status decides completeness, so a run's own first report
        # and every later regeneration describe the same run identically.
        complete=status["terminal_status"] == evidence.TERMINAL_COMPLETED,
        jobs=jobs,
    )


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
    """Summarize saved observations by resolved model case, one group at a time."""
    family = results.family
    accumulators: dict[tuple[str, str, int, str], _GroupAccumulator] = {}
    instances = {item["model_instance_id"]: item for item in family["models"]}
    variants = {item["variant_id"]: item for item in family["variants"]}

    for group in family["groups"]:
        key = (
            group["variant_id"],
            group["model_instance_id"],
            int(group["timeframe_minutes"]),
            group["case_id"],
        )
        instance = instances[group["model_instance_id"]]
        case = next(
            item
            for item in instance["cases"][str(int(group["timeframe_minutes"]))]
            if item["case_id"] == group["case_id"]
        )
        accumulator = _GroupAccumulator(
            group=group,
            outcomes=tuple(item["name"] for item in case["outcomes"]),
            units={item["name"]: item["unit"] for item in case["outcomes"]},
        )
        accumulator.prepare()
        accumulators[key] = accumulator

    planned_tickers = [item["instrument_id"] for item in family["instruments"]]
    for instrument_id in results.completed_instruments:
        episodes = results.table(instrument_id, "episodes")
        for key, accumulator in accumulators.items():
            variant_id, instance_id, timeframe, case_id = key
            variant = variants[variant_id]
            instance = instances[instance_id]
            frame = results.observations(
                instrument_id,
                model_instance_id=instance_id,
                timeframe_minutes=timeframe,
                case_id=case_id,
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

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "evidence_view_version": EVIDENCE_VIEW_VERSION,
        "run_root": str(results.run_root),
        "study_name": results.request["study_name"],
        "notes": results.request.get("notes"),
        "complete": results.complete,
        "counts": results.counts,
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
    """
    family = results.family
    values: list[dict[str, Any]] = []
    for group in family["groups"]:
        timeframe = int(group["timeframe_minutes"])
        for declaration in declarations:
            descriptor = contracts.metric(declaration.metric_id)
            frames: list[pd.DataFrame] = []
            missing: list[str] = []
            for instrument_id in results.completed_instruments:
                frame = results.observations(
                    instrument_id,
                    model_instance_id=group["model_instance_id"],
                    timeframe_minutes=timeframe,
                    case_id=group["case_id"],
                    variant_id=group["variant_id"],
                )
                absent = [name for name in descriptor.required_columns if name not in frame.columns]
                if absent:
                    missing = absent
                    break
                frames.append(frame.loc[:, list(descriptor.required_columns)])
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
            if missing:
                record.update(
                    {"availability": "missing_inputs", "missing_columns": missing, "value": None}
                )
            else:
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
