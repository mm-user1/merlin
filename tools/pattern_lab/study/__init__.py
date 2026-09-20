"""Pattern Lab event studies: features, hypotheses, models, evidence and reports.

The CLI is a thin wrapper around this package.  An agent script imports the same
functions, registers its own descriptors through a declared trusted module and
reads the same saved evidence.  Every accepted request form passes the same
execution-boundary validation, and every used non-built-in descriptor must come
from a declared, verified source generation.

    from tools.pattern_lab import study

    result = study.run_study(
        request="tools/pattern_lab/configs/example_study_two_green_30m.json",
        data_root="docs/_work/pattern-lab-data",
        output_root="runs/two-green-30m",
        workers=1,
    )
    results = study.load_results(result["run_root"])
    summary = study.summarize_results(results)
    html = study.render_report(summary)
"""

from .builtins import (
    EVIDENCE_VIEW_VERSION,
    FIXED_HORIZON_MODEL_ID,
    TWO_GREEN_HYPOTHESIS_ID,
    TWO_GREEN_PLAIN_HYPOTHESIS_ID,
    expand_fixed_horizon_case,
    register_builtins,
)
from .contracts import (
    Anchors,
    BarSeries,
    ConditionValue,
    FeatureDescriptor,
    FeatureRequest,
    FeatureValue,
    HypothesisDescriptor,
    MetricDescriptor,
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
    registered,
    registrations,
)
from .evidence import RUN_SCHEMA_VERSION
from .extensions import ExtensionContext
from .job import InstrumentJobInput, InstrumentJobResult, TimeframeInput, run_instrument_job
from .report import render_report
from .results import InstrumentReader, StudyResults, load_results, summarize_results
from .runner import regenerate_report, run_study
from .spec import (
    OCCURRENCE_POLICIES,
    PROTOCOL_SCHEMA_VERSION,
    REQUEST_SCHEMA_VERSION,
    StudyRequest,
    load_request,
    normalize_protocol,
    normalize_request,
)

register_builtins()

__all__ = [
    "Anchors",
    "BarSeries",
    "ConditionValue",
    "EVIDENCE_VIEW_VERSION",
    "ExtensionContext",
    "FIXED_HORIZON_MODEL_ID",
    "FeatureDescriptor",
    "FeatureRequest",
    "FeatureValue",
    "HypothesisDescriptor",
    "InstrumentJobInput",
    "InstrumentJobResult",
    "InstrumentReader",
    "MetricDescriptor",
    "ModelCase",
    "ModelDescriptor",
    "ModelEvidence",
    "OCCURRENCE_POLICIES",
    "OutcomeSpec",
    "PROTOCOL_SCHEMA_VERSION",
    "REQUEST_SCHEMA_VERSION",
    "RUN_SCHEMA_VERSION",
    "StudyRequest",
    "StudyResults",
    "TWO_GREEN_HYPOTHESIS_ID",
    "TWO_GREEN_PLAIN_HYPOTHESIS_ID",
    "TimeframeInput",
    "expand_fixed_horizon_case",
    "load_request",
    "load_results",
    "normalize_protocol",
    "normalize_request",
    "register_builtins",
    "registered",
    "registrations",
    "regenerate_report",
    "render_report",
    "run_instrument_job",
    "run_study",
    "summarize_results",
]
