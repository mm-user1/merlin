"""Synthetic report witnesses; no dependency on ignored analytical archives."""
from copy import deepcopy
from pathlib import Path
import zipfile

from ._strategy_screen_helpers import compact_request, compact_unit
from tools.pattern_lab.strategy_screen.request import normalize_request, configurations
from tools.pattern_lab.strategy_screen.artifacts import Writer, load_strategy_screen, seal, artifact_identity
from tools.pattern_lab.strategy_screen.facts import analytical_month
from tools.pattern_lab.configuration_study import codec
from tools.pattern_lab.account_study.containers import zip_info
from tools.pattern_lab.account_study.common import read_json, write_json


def missing_month(unit, index=1):
    """Remove one interior bar; later contributions survive the missing prefix."""
    m = unit['months'][index]; m['observedBars'] -= 1
    cov = unit['metrics']['coverage']; cov['observedBars'] -= 1; cov['missingBars'] += 1
    unit['audit']['pathPoints'] -= 1
    prefix = True
    for month in unit['months']:
        month.update(analytical_month(month, unit['metrics']['initialCapital'], prefix))
        prefix = prefix and month['exact']


def compact_report_input(root, *, capacity=False, per_cell=3, missing=True, sparse=False):
    doc = compact_request(per_cell, capacity=capacity)
    if sparse:
        base = deepcopy(doc['allocations'][0]); base.update(allocationId='other-tf', timeframeMinutes=720)
        doc['allocations'].append(base)
        doc['cells'].extend([dict(instrumentId='OKX_AAAUSDT', timeframeMinutes=1440, allocationIds=[doc['allocations'][0]['allocationId']]),
                            dict(instrumentId='OTHER_ID', timeframeMinutes=720, allocationIds=['other-tf'])])
        doc['criteria']['minClosedTradesByTimeframe'] = {'1440': 100, '720': 1}
    normalized = normalize_request(doc); doc = normalized.document
    root = Path(root); root.mkdir(parents=True, exist_ok=False)
    writer = Writer(root, normalized, {'fixture': 'report-numeric-compact-only'}, synthetic=True)
    for period in reversed(doc['periods']):
        for i, config in enumerate(configurations(doc)):
            unit = compact_unit(config, period, i+(13 if period['periodId']=='recent' else 0))
            if missing and i % 7 == 0:
                missing_month(unit)
            writer.append(unit)
    writer.finish({'fixture': 'synthetic compact-only with missing observations'}, dict(plannedResults=2*len(configurations(doc))))
    return load_strategy_screen(root)


def reseal_false_summary(root):
    """Structural-load-valid numerical contradiction for full verification."""
    index = read_json(root/'index.json'); blocks = []
    with zipfile.ZipFile(root/'results.zip') as archive:
        for entry in index['members']:
            blocks.append(codec.decode(archive.read(entry['name']), entry))
    blocks[-1]['records'][0]['periods']['recent']['metrics']['returnPct']['median'] = 123456.
    with zipfile.ZipFile(root/'results.zip', 'w', compression=zipfile.ZIP_STORED, allowZip64=False) as archive:
        for entry, block in zip(index['members'], blocks):
            raw, info = codec.encode(block); entry.update(info); archive.writestr(zip_info(entry['name']), raw)
    index['totalDecodedBytes'] = sum(e['decodedBytes'] for e in index['members'])
    write_json(root/'index.json', index)
    study = read_json(root/'study.json'); study['artifactIdentity'] = artifact_identity(study, index)
    write_json(root/'study.json', study); seal(root)


def inventory(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}

