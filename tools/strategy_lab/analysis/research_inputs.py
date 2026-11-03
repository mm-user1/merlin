"""Snapshot loading and scope restriction for explicit exploratory research inputs."""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from .dataset import AnalysisError
from .json_utils import canonical_json_bytes


def digest(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _keys(value, keys, label):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise AnalysisError(f"{label} requires exactly these keys: {sorted(keys)}.")


def _text(value, label):
    if not isinstance(value, str) or not value.strip():
        raise AnalysisError(f"{label} must be a nonblank string.")


def _read(path):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise AnalysisError(f"duplicate JSON key: {key!r}.")
            result[key] = value
        return result

    def constant(value):
        raise AnalysisError(f"nonfinite JSON number: {value}.")

    try:
        path = Path(path).resolve()
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    except (OSError, UnicodeError, ValueError) as exc:
        raise AnalysisError(f"cannot load research input {path}: {exc}") from exc
    return str(path), hashlib.sha256(raw).hexdigest(), value


@dataclass(frozen=True)
class InputDocument:
    path: str
    file_sha256: str
    document: dict
    document_sha256: str

    def provenance(self):
        return dict(path=self.path, file_sha256=self.file_sha256,
                    document=self.document, document_sha256=self.document_sha256)


@dataclass(frozen=True)
class Universe(InputDocument):
    """Validated explicit membership; independent of candidate geometry."""


def load_universe(path):
    """Read one UTF-8 file snapshot and validate the closed universe-v1 contract."""
    path, raw_hash, doc = _read(path)
    _keys(doc, ("schema_version", "name", "evidence_classification", "selection_note", "tickers"), "universe")
    if doc["schema_version"] != "strategy_lab_universe_v1" or doc["evidence_classification"] != "exploratory":
        raise AnalysisError("universe requires strategy_lab_universe_v1 and exploratory classification.")
    for key in ("name", "selection_note"):
        _text(doc[key], key)
    tickers = doc["tickers"]
    if (not isinstance(tickers, list) or not tickers or
            any(not isinstance(t, str) or not t or t != t.strip() for t in tickers)):
        raise AnalysisError("universe tickers must be a nonempty list of exact nonblank IDs without surrounding whitespace.")
    if len(set(tickers)) != len(tickers):
        raise AnalysisError("universe contains duplicate tickers.")
    return Universe(path, raw_hash, doc, digest(doc))


def restrict_universe(universe, inputs, mode=None):
    """Filter authorized scopes in saved order; leave common-pool alignment to its owner.

    inputs contains (label, dataset, resolved_scope) tuples.
    """
    if universe is None:
        if mode is not None:
            raise AnalysisError("ticker-list mode requires a universe/ticker list.")
        return [scope for _, _, scope in inputs], None
    if not isinstance(universe, Universe):
        raise AnalysisError("universe must be supplied by load_universe.")
    mode = "strict" if mode is None else mode
    if mode not in ("strict", "intersection"):
        raise AnalysisError("ticker-list mode must be strict or intersection.")
    requested = set(universe.document["tickers"])
    known = set().union(*(scope.tickers for _, _, scope in inputs))
    if mode == "intersection" and requested - known:
        raise AnalysisError(f"universe IDs absent from every resolved scope: {sorted(requested-known)}.")
    scopes, facts = [], {}
    for label, dataset, scope in inputs:
        missing = sorted(requested - set(scope.tickers))
        if missing and mode == "strict":
            raise AnalysisError(f"dataset {label!r} universe IDs outside resolved scope: {missing}.")
        subset = dataset.subset_scope(scope, tickers=tuple(t for t in scope.tickers if t in requested))
        scopes.append(subset)
        facts[label] = dict(scope=scope.name, ticker_cell=scope.ticker_cell,
            authorized_tickers=list(scope.tickers), authorized_ticker_count=len(scope.tickers),
            ticker_cell_count=scope.ticker_cell_count, excluded_tickers=sorted(set(scope.tickers)-requested),
            missing_requested_tickers=missing, effective_tickers=list(subset.tickers),
            original_is_partial=scope.is_partial, is_partial=subset.is_partial,
            membership_subset=subset.tickers != scope.tickers,
            declared_window_ids=list(scope.declared_window_ids), missing_window_ids=list(scope.missing_window_ids),
            requires_unlock=scope.requires_unlock, unlock_evidence=scope.unlock_evidence)
    return scopes, {**universe.provenance(), "requested_tickers": list(universe.document["tickers"]),
                    "mode": mode, "datasets": facts}


def attach_research_inputs(metadata, summary, universe, policy, resolved_tickers):
    if universe is None and policy is None:
        return
    if universe is not None:
        resolved = sorted(resolved_tickers)
        universe = {**universe, "resolved_tickers": resolved, "resolved_tickers_sha256": digest(resolved)}
    metadata["research_inputs"] = dict(schema_version="strategy_lab_research_inputs_v1",
        universe=universe, candidate_policy=policy.provenance() if policy is not None else None)
    summary["research_inputs"] = dict(schema_version="strategy_lab_research_inputs_v1",
        universe_document_sha256=universe["document_sha256"] if universe else None,
        candidate_policy_key=policy.key if policy is not None else None)
