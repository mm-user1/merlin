"""Pre-T10 group-at-a-time metric oracle, retained only for parity tests."""
from typing import Any, Sequence
import numpy as np
import pandas as pd
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.study.results import StudyResults, InstrumentReader, METRICS_SCHEMA_VERSION
from tools.pattern_lab.study import contracts

def group_at_a_time(results: StudyResults, declarations: Sequence[Any]) -> dict[str, Any]:
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
