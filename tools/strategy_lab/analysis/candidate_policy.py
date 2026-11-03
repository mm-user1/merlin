"""Closed, analysis-owned IS candidate policy v1; no execution-stack imports."""
from dataclasses import dataclass
import math
import operator

import numpy as np

from .dataset import AnalysisError
from .research_inputs import InputDocument, _read, _keys, _text, digest
from .rules import RuleResult

METRICS = frozenset((
    "net_profit_pct", "max_drawdown_pct", "total_trades", "winning_trades", "losing_trades",
    "gross_profit", "gross_loss", "profit_factor", "win_rate_pct", "romad", "max_consecutive_losses",
    "sharpe_daily", "sharpe_daily_observations", "sharpe_daily_active_days", "sqn", "rejected_fill_count",
    "zero_size_entry_count", "invalid_stop_distance_count", "max_required_leverage",
    "max_drawdown_mtm_pct", "margin_reject_count",
))
OPERATORS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le}


@dataclass(frozen=True)
class CandidatePolicy(InputDocument):
    @property
    def key(self):
        return "declarative:" + self.document_sha256

    @property
    def required_metrics(self):
        return tuple(sorted({"net_profit_pct", self.document["objective"]["metric"],
                             *(c["metric"] for c in self.document["constraints"])}))

    def provenance(self):
        return {**super().provenance(), "key": self.key, "name": self.document["name"],
            "version": self.document["version"], "required_metrics": list(self.required_metrics),
            "score_direction": self.document["objective"]["direction"], "evidence_classification": "exploratory",
            "ties": "score descending; finite IS net profit first, descending; semantic key ascending; candidate ID ascending",
            "no_eligible": "unavailable; no fallback", "selection_count": 1}

    def evaluate(self, view, geometry):
        metrics = {}
        for name in self.required_metrics:
            if name not in view.metrics:
                raise AnalysisError(f"candidate policy requires metric {name!r}.")
            values = np.asarray(view.metrics[name], dtype=np.float64)
            if values.shape != (geometry.count,):
                raise AnalysisError(f"policy metric {name!r} is not candidate-aligned.")
            metrics[name] = values
        objective = self.document["objective"]
        values = metrics[objective["metric"]]
        eligible = np.isfinite(values)
        for constraint in self.document["constraints"]:
            value = metrics[constraint["metric"]]
            eligible &= np.isfinite(value) & OPERATORS[constraint["operator"]](value, constraint["value"])
        score = 0.0 + values if objective["direction"] == "maximize" else 0.0 - values
        score[score == 0] = 0.0
        return RuleResult(self.key, score, eligible, self.required_metrics, {}, exploratory=True)


def load_candidate_policy(path):
    """Validate and normalize a single file snapshot, without opening a dataset."""
    path, raw_hash, doc = _read(path)
    _keys(doc, ("schema_version", "name", "version", "objective", "constraints", "selection_count",
                "no_eligible", "evidence_classification"), "candidate policy")
    if (doc["schema_version"] != "strategy_lab_candidate_policy_v1" or
            doc["evidence_classification"] != "exploratory" or doc["no_eligible"] != "unavailable" or
            type(doc["selection_count"]) is not int or doc["selection_count"] != 1):
        raise AnalysisError("policy requires v1, exploratory, selection_count=1 and no_eligible=unavailable.")
    for name in ("name", "version"):
        _text(doc[name], name)
    _keys(doc["objective"], ("metric", "direction"), "objective")
    objective = doc["objective"]
    if not isinstance(objective["metric"], str) or objective["metric"] not in METRICS:
        raise AnalysisError("unknown policy objective metric.")
    if objective["direction"] not in ("maximize", "minimize"):
        raise AnalysisError("policy direction must be maximize or minimize.")
    if not isinstance(doc["constraints"], list):
        raise AnalysisError("policy constraints must be a list.")
    for constraint in doc["constraints"]:
        _keys(constraint, ("metric", "operator", "value"), "constraint")
        if not isinstance(constraint["metric"], str) or constraint["metric"] not in METRICS:
            raise AnalysisError("unknown policy constraint metric.")
        if not isinstance(constraint["operator"], str) or constraint["operator"] not in OPERATORS:
            raise AnalysisError("policy constraint operator must be >, >=, < or <=.")
        value = constraint["value"]
        try:
            if type(value) not in (int, float) or not math.isfinite(float(value)):
                raise ValueError()
            constraint["value"] = 0.0 if value == 0 else float(value)
        except (ValueError, OverflowError):
            raise AnalysisError("policy threshold must be a finite binary64 JSON number, not a boolean or string.") from None
    return CandidatePolicy(path, raw_hash, doc, digest(doc))


def validate_policy_metrics(policy, metric_index, label):
    if not isinstance(policy, CandidatePolicy):
        raise AnalysisError("candidate_policy must be supplied by load_candidate_policy.")
    policy.verify()
    missing = sorted(set(policy.required_metrics) - set(metric_index))
    if missing:
        raise AnalysisError(f"dataset {label!r} candidate policy missing metrics: {missing}.")
