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


# Fresh-process CLI fault witnesses; no ignored task inputs are required.
def cli_fault_child(kind, source, output, exception_name, target='package'):
    import builtins
    import json
    import os
    import sys
    from unittest.mock import patch
    from tools.pattern_lab import __main__ as cli
    exception = KeyboardInterrupt if exception_name == 'KeyboardInterrupt' else OSError
    argv=['strategy-screen-report','--run-root',source,'--output-root',output]
    if kind=='cold':
        assert 'tools.pattern_lab.strategy_screen_report' not in sys.modules
        original=builtins.__import__
        def load(name,globals=None,locals=None,fromlist=(),level=0):
            wanted='strategy_screen_report' if target=='package' else 'strategy_screen_report.render'
            if name==wanted and globals and globals.get('__name__')==cli.__name__:
                raise exception('PRIVATE_COLD_IMPORT')
            return original(name,globals,locals,fromlist,level)
        with patch.object(builtins,'__import__',load):
            return cli.main(argv)
    # This process owns real pipe descriptors. Do not run this probe in a parent.
    original_out,original_err=sys.stdout,sys.stderr
    before=[os.fstat(s.fileno()) for s in (original_out,original_err)]
    os.write(original_out.fileno(),b'BEFORE_STDOUT\n');os.write(original_err.fileno(),b'BEFORE_STDERR\n')
    class Once:
        def __init__(self,stream,phase):self.stream=stream;self.phase=phase;self.fired=False
        def fileno(self):return self.stream.fileno()
        def write(self,text):
            if self.phase=='write' and not self.fired:
                self.fired=True;raise exception('PRIVATE_WRITE')
            return self.stream.write(text)
        def flush(self):
            if self.phase=='flush' and not self.fired:
                self.fired=True;raise exception('PRIVATE_FLUSH')
            return self.stream.flush()
    stdout=Once(original_out,kind if kind in ('write','flush') else None)
    stderr=Once(original_err,kind.removeprefix('stderr-') if kind.startswith('stderr-') else None)
    def notify(payload):
        raise (OSError if kind.startswith('stderr-') else exception)('PRIVATE_NOTIFICATION')
    with patch.object(sys,'stdout',stdout),patch.object(sys,'stderr',stderr):
        if kind=='notification' or kind.startswith('stderr-'):
            # The explicit baseline probe selects its old command emitter without
            # changing the expected stream-lifetime assertion.
            emitter='_emit_report' if hasattr(cli,'_emit_report') else '_emit'
            with patch.object(cli,emitter,notify):code=cli.main(argv)
        else:code=cli.main(argv)
        assert sys.stdout is stdout and sys.stderr is stderr
    original_out.flush();original_err.flush()
    after=[os.fstat(s.fileno()) for s in (original_out,original_err)]
    os.write(original_out.fileno(),b'AFTER_STDOUT\n');os.write(original_err.fileno(),b'AFTER_STDERR\n')
    evidence=dict(mainReturn=code,descriptorIdentityPreserved=[(a.st_dev,a.st_ino,a.st_mode)==(b.st_dev,b.st_ino,b.st_mode) for a,b in zip(before,after)],
        objectsRestored=sys.stdout is original_out and sys.stderr is original_err)
    Path(output+'.child.json').write_text(json.dumps(evidence),encoding='utf-8')
    return 0


if __name__=='__main__':
    import sys
    raise SystemExit(cli_fault_child(*sys.argv[1:]))
