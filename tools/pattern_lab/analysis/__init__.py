"""Pattern Lab M3a: matched comparisons and calibrated inference.

Offline analysis of one **completed** M2 event study.  It measures the
difference between signal outcomes and comparable control outcomes, quantifies
that difference's uncertainty and reports the declared multiple-testing family.
It runs no new backtest, generates no equity and certifies no profitable
strategy.

    from tools.pattern_lab import analysis

    result = analysis.run_analysis(
        request="analysis.json", run_root="completed-study", output_root="new-analysis"
    )
    saved = analysis.load_analysis("new-analysis")
    saved.comparisons()   # estimates, intervals, p-values and availability reasons
    saved.strata()        # every stratum, including zero-event and excluded ones
    saved.daily()         # the daily counts and sums that reproduce the estimator
    analysis.regenerate_report("new-analysis")

These helpers execute no hypothesis module, no saved Python snapshot and no
metric plugin.  Report regeneration needs only the sealed analysis artifact: not
the original study, its market pack or any saved source.

The inference is an **approximate development screen**.  Nothing here is a
statistically validated edge.
"""

from .artifacts import (
    ANALYSIS_SCHEMA_VERSION,
    SUPPORTED_ANALYSIS_SCHEMA_VERSIONS,
    CURRENT_ANALYSIS_SCHEMA_VERSION,
    AnalysisResults,
    load_analysis,
)
from .estimator import (
    ESTIMATOR_SCHEMA_VERSION,
    RECORD_COLUMNS,
    CalendarGrid,
    evaluate_observations,
    evaluate_monthly_observations,
)
from .family import Comparison, FamilyMember, resolve_family
from .report import render_report
from .request import (
    ANALYSIS_REQUEST_SCHEMA_VERSION,
    BLOCK_LENGTH_DAYS,
    INFERENCE_SCOPE,
    METHOD_ID,
    V2_METHOD_ID,
    SUPPORTED_REQUEST_VERSIONS,
    CURRENT_REQUEST_SCHEMA_VERSION,
    AnalysisRequest,
    load_analysis_request,
    method_settings,
    normalize_analysis_request,
)
from .runner import DISCLOSURES, regenerate_report, run_analysis
from .source import AdmittedSource, RecordSource, admit_source

__all__ = [
    "V2_METHOD_ID", "SUPPORTED_REQUEST_VERSIONS", "SUPPORTED_ANALYSIS_SCHEMA_VERSIONS",
    "CURRENT_REQUEST_SCHEMA_VERSION", "CURRENT_ANALYSIS_SCHEMA_VERSION", "evaluate_monthly_observations",
    "ANALYSIS_REQUEST_SCHEMA_VERSION",
    "ANALYSIS_SCHEMA_VERSION",
    "BLOCK_LENGTH_DAYS",
    "DISCLOSURES",
    "ESTIMATOR_SCHEMA_VERSION",
    "INFERENCE_SCOPE",
    "METHOD_ID",
    "RECORD_COLUMNS",
    "AdmittedSource",
    "AnalysisRequest",
    "AnalysisResults",
    "CalendarGrid",
    "Comparison",
    "FamilyMember",
    "RecordSource",
    "admit_source",
    "evaluate_observations",
    "load_analysis",
    "load_analysis_request",
    "method_settings",
    "normalize_analysis_request",
    "regenerate_report",
    "render_report",
    "resolve_family",
    "run_analysis",
]
