"""Cheap, conservative run planning; never applied to retained artifacts."""
from ..accounts.contracts import require
from ..account_study.common import json_text

MAX_JSON = 16 * 1024 * 1024


def results_capacity(doc, roster):
    """Assess normalized dimensions without scoring, random draws or row lists.

    This is a supported workload budget, not a proof of serialized size. The
    fixed allowances cover ordinary numeric tokens/metadata; actual writes are
    independently checked. Identifier costs use their real JSON encodings.
    """
    selectors = len(doc['selectors']); months = len(doc['evaluationMonths'])
    ks = doc['kValues']; replicates = doc['randomControl']['replicates']
    lengths = sorted((len(json_text(i).encode('utf-8')) for i in roster), reverse=True)
    prefix = [0]
    for size in lengths: prefix.append(prefix[-1] + size + 1)
    # 768 bytes for row keys, scalars, counts and forced-exit objects; longest
    # possible membership array for each K, including quotes and separators.
    row_bytes = [768 + 2 + prefix[min(k, len(roster))] for k in ks]
    random_bytes = selectors * months * replicates * sum(row_bytes)
    # Two retained replicate-total arrays per comparison, 32-byte numbers + comma.
    totals_bytes = selectors * len(ks) * replicates * 2 * 33
    reserve = (256 * 1024 + selectors * len(ks) * 2048 + selectors * len(ks) * months *
        (4096 + 8 * sum(lengths) + 256 * len(roster)))
    estimated = reserve + (5 * (random_bytes + totals_bytes) + 3) // 4
    return dict(selectors=selectors, kValues=list(ks), evaluationMonths=months,
        rosterSize=len(roster), replicates=replicates, randomRows=selectors*len(ks)*months*replicates,
        rowBudgetBytes=row_bytes, randomBytes=random_bytes, replicateTotalsBytes=totals_bytes,
        reservedBytes=reserve, estimatedBytes=estimated, jsonLimitBytes=MAX_JSON,
        supported=estimated <= MAX_JSON)


def admit_results_capacity(doc, roster):
    assessment = results_capacity(doc, roster)
    require(assessment['supported'],
        'selection results planning budget exceeded: '+json_text(assessment)+
        '; use fewer replicates or fewer separately declared K/selector comparisons')
    return assessment
