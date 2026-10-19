"""Three explicit built-ins and a trusted, in-process experimental route."""
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping
import math

from ..accounts.contracts import require, identifier, integer
from ..account_study.common import closed, json_text, parse_json, relative_name
from ..study.contracts import semantic_digest
from .request import strict_json


@dataclass(frozen=True)
class Score:
    instrumentId: str
    eligible: bool
    key: tuple | None
    reason: str | None
    components: dict


@dataclass(frozen=True)
class Scorer:
    scorerId: str
    version: str
    normalizeParameters: Callable
    lookbackMonths: Callable
    keyNames: tuple[str, ...]
    score: Callable
    sourceFiles: Mapping[str, Path] | None = None
    needsBars: bool = False


def no_parameters(p): closed(p, ()); return {}
def months_parameters(p):
    closed(p, ('months',)); return dict(months=integer(p['months'],'months',1))


def consistency(view, parameters):
    for instrument in view.instruments:
        returns = [r['returnPct'] for r in view.months(instrument)]
        key = (sum(v > 0 for v in returns), min(returns), math.fsum(returns)/len(returns))
        yield Score(instrument,True,key,None,dict(returns=returns))


def mean_return(view, parameters):
    for instrument in view.instruments:
        values = [r['returnPct'] for r in view.months(instrument)]
        yield Score(instrument,True,(math.fsum(values)/len(values),),None,dict(returns=values))


def positive_count(view, parameters):
    for instrument in view.instruments:
        values = [r['returnPct'] for r in view.months(instrument)]
        yield Score(instrument,True,(sum(v > 0 for v in values),),None,dict(returns=values))


BUILTINS = {
    'historicalConsistency4m': Scorer('historicalConsistency4m','1',no_parameters,lambda p:4,
        ('positiveMonths','worstReturnPct','meanReturnPct'),consistency),
    'meanMonthlyReturn': Scorer('meanMonthlyReturn','1',months_parameters,lambda p:p['months'],('meanReturnPct',),mean_return),
    'positiveMonthCount': Scorer('positiveMonthCount','1',months_parameters,lambda p:p['months'],('positiveMonths',),positive_count),
}


def registry(experimental=None):
    result = dict(BUILTINS)
    for name, scorer in (experimental or {}).items():
        require(isinstance(scorer,Scorer) and name == scorer.scorerId and name.startswith('x.'), 'experimental scorer ID')
        identifier(name)
        require(type(scorer.version) is str and scorer.version and type(scorer.needsBars) is bool, 'scorer declaration')
        require(all(callable(f) for f in (scorer.normalizeParameters,scorer.lookbackMonths,scorer.score)), 'scorer functions')
        require(scorer.sourceFiles and isinstance(scorer.sourceFiles, Mapping), 'experimental source files required')
        for logical, path in scorer.sourceFiles.items():
            relative_name(logical)
            require(Path(path).is_file(), 'missing experimental source: '+logical)
        result[name] = scorer
    return result


def contract(scorer, parameters):
    n = integer(scorer.lookbackMonths(parse_json(json_text(parameters))), 'scorer lookback', 1)
    require(type(scorer.keyNames) is tuple and scorer.keyNames and len(set(scorer.keyNames)) == len(scorer.keyNames), 'key names')
    for name in scorer.keyNames: identifier(name)
    return dict(lookbackMonths=n,keyNames=list(scorer.keyNames),needsBars=scorer.needsBars)


def validate_scores(values, instruments, key_names):
    rows = []
    for value in values:
        require(isinstance(value,Score) and value.instrumentId in instruments and type(value.eligible) is bool, 'invalid score row')
        require(type(value.components) is dict, 'score explanations'); strict_json(value.components)
        if value.eligible:
            require(type(value.key) is tuple and len(value.key) == len(key_names) and
                all(type(v) in (int,float) and math.isfinite(v) for v in value.key), 'finite score tuple required')
            require(value.reason is None, 'eligible score reason')
        else:
            require(value.key is None and type(value.reason) is str and value.reason, 'ineligible score needs reason and null key')
        rows.append(dict(instrumentId=value.instrumentId,eligible=value.eligible,key=list(value.key) if value.key is not None else None,
            reason=value.reason,components=parse_json(json_text(value.components))))
    require(len(rows) == len(instruments) and {r['instrumentId'] for r in rows} == set(instruments), 'missing/duplicate score outputs')
    return sorted(rows,key=lambda r:r['instrumentId'])


def evaluate(scorer, view, parameters):
    before = view.fingerprint
    rows = validate_scores(scorer.score(view,parse_json(json_text(parameters))),view.instruments,scorer.keyNames)
    require(before == view.fingerprint, 'scorer mutated input view')
    return rows


def check_deterministic(scorer, view, parameters):
    """Reusable bounded experimental check; caller supplies an already sliced view."""
    first = evaluate(scorer,view,parameters)
    require(first == evaluate(scorer,view,parameters), 'nondeterministic scorer')
    return first


def check_prefix_invariance(scorer, original_view, extended_view, parameters):
    """Compare same-cutoff slices prepared from original and future-extended inputs."""
    require((original_view.cutoffMs,original_view.monthLabels,original_view.instruments)==
        (extended_view.cutoffMs,extended_view.monthLabels,extended_view.instruments),'prefix views must share cutoff/history/pool')
    first=check_deterministic(scorer,original_view,parameters)
    require(first==check_deterministic(scorer,extended_view,parameters),'scorer changed on future extension')
    return first
