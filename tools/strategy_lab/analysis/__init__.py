"""Read-only, pre-registration-driven Strategy Lab analysis."""

from .dataset import AnalysisError, ScopeLockedError, open_dataset
from .allocation import (
    AllocationResult,
    DatasetInput,
    OFFICIAL_TICKER_SCORER,
    SelectedISTickerView,
    TickerScorer,
    evaluate_allocation,
)
from .evaluate import AnalysisResult, evaluate_scope
from .candidate_policy import CandidatePolicy, load_candidate_policy
from .research_inputs import Universe, load_universe

__all__ = [
    "AnalysisError",
    "AnalysisResult",
    "AllocationResult",
    "DatasetInput",
    "OFFICIAL_TICKER_SCORER",
    "SelectedISTickerView",
    "ScopeLockedError",
    "TickerScorer",
    "evaluate_allocation",
    "evaluate_scope",
    "open_dataset",
    "CandidatePolicy",
    "Universe",
    "load_candidate_policy",
    "load_universe",
]
