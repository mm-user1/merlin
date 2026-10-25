"""One sealed member set for directory readers and thin ZIP transport."""
from pathlib import Path
from dataclasses import asdict
import os
import re
import shutil
import stat
import tempfile
import zipfile

from ..accounts import validate_result
from ..accounts.contracts import require
from ..study.contracts import semantic_digest
from .common import (closed, read_json, write_json, parse_json, json_text, file_hash,
                     member_names, fresh_directory, digest_id)
from .request import normalize_request, planned_family, bounds, warmup_requirements
from .codec import decode_account
from .containers import Locator, PAIRS, MAX_MEMBERS, version

SCHEMA = 'pattern-lab.account-study.v1'
SCHEMA_V2 = 'pattern-lab.account-study.v2'
MAX_ENVELOPE_BYTES = 16*1024*1024
DERIVED = ('derived/summary.json', 'derived/report.html')


def envelope(value):
    closed(value, ('schema', 'formatVersion', 'members'))
    require(type(value['formatVersion']) is int and (value['schema'], value['formatVersion']) in PAIRS, 'completion schema/version')
    members = value['members']
    require(type(members) is dict and members and len(members) <= 100000, 'completion members')
    require('completion.json' not in members, 'manifest self-entry')
    member_names([*members, 'completion.json'])
    for name, row in members.items():
        closed(row, ('bytes', 'sha256'))
        require(type(row['bytes']) is int and 0 <= row['bytes'] <= 2**63-1 and
                isinstance(row['sha256'], str) and re.fullmatch('[a-f0-9]{64}', row['sha256']), 'member size/hash')
    return value


def verify_directory(root):
    root = Path(root)
    require(root.is_dir() and not root.is_symlink(), 'study directory')
    path = root/'completion.json'
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_ENVELOPE_BYTES, 'missing/invalid completion envelope')
    seal = envelope(read_json(path))
    actual = set()
    for base, dirs, files in os.walk(root, followlinks=False):
        for name in dirs+files:
            require(not (Path(base)/name).is_symlink(), 'symlink member')
        actual.update((Path(base)/name).relative_to(root).as_posix() for name in files)
    require(actual == set(seal['members']) | {'completion.json'}, 'completion member set differs')
    for name, row in seal['members'].items():
        p = root/name
        require(p.stat().st_size == row['bytes'] and file_hash(p) == row['sha256'], 'member integrity: '+name)
    return seal


def seal_directory(root):
    root = Path(root)
    require(not (root/'completion.json').exists(), 'already sealed')
    require(read_json(root/'status.json')['state'] == 'complete', 'final status must precede completion')
    files = sorted(p for p in root.rglob('*') if p.is_file())
    members = {p.relative_to(root).as_posix(): dict(bytes=p.stat().st_size, sha256=file_hash(p)) for p in files}
    study = read_json(root/'study.json')
    locator = Locator.admitted(root, study)
    require(locator.logical_count(len(members)) <= MAX_MEMBERS, 'logical member limit')
    value = envelope(dict(schema=study['schema'], formatVersion=study['formatVersion'], members=members))
    require(len(json_text(value).encode('utf-8')) <= MAX_ENVELOPE_BYTES, 'completion too large')
    write_json(root/'completion.json', value)
    verify_directory(root)


class AccountStudy:
    """Checked, lazy reader. Each account access decodes owned arrays/tables anew."""
    def __init__(self, root, study, keys, status, partial=False, *, locator=None):
        self.root = Path(root)
        self._study = json_text(study)
        self.accountKeys = tuple(keys)
        self._keys = frozenset(keys)
        self._locator = locator
        self._status = json_text(status)
        self.partial = partial

    @property
    def study(self): return parse_json(self._study)

    @property
    def status(self): return parse_json(self._status)

    def key_for(self, instrumentId, allocationId, windowId):
        key = semantic_digest(dict(instrumentId=instrumentId, allocationId=allocationId, windowId=windowId))
        require(key in self._keys, 'unknown account')
        return key

    def account(self, key):
        require(key in self._keys, 'unknown account key')
        return decode_account(self.root, key, _locator=self._locator)

    @property
    def summary(self):
        require(not self.partial, 'partial studies have no full-roster summary')
        return read_json(self.root/DERIVED[0])


def _load(root, *, sealed=True):
    root = Path(root)
    seal = verify_directory(root) if sealed else None
    study, status = read_json(root/'study.json'), read_json(root/'status.json')
    closed(study, ('schema', 'formatVersion', 'request', 'family', 'identities', 'inputs', 'implementation', 'retentionProfile', 'warmupRequirements', 'roster'))
    layout = version(study)
    v2 = study['schema'] == SCHEMA_V2
    if seal: require(seal['schema'] == study['schema'] and seal['formatVersion'] == study['formatVersion'], 'study/seal version differs')
    require(study['retentionProfile'] in ('standard', 'debug'), 'saved retention profile')
    request = normalize_request(study['request'])
    require((request.document['schema'] == 'pattern-lab.account-study-request.v2') == v2, 'study/request version differs')
    require(request.document == study['request'] and planned_family(request.document) == study['family'], 'normalized request/plan mismatch')
    require(study['identities'] == dict(specification=request.identity,
        dataInput=semantic_digest(study['inputs']), implementation=semantic_digest(study['implementation'])), 'study identities')
    keys = [semantic_digest(label) for label in study['family']]
    require(status['state'] == 'complete' if sealed else status['state'] in ('running', 'complete'), 'incomplete study status')
    locator = Locator.admitted(root, study)
    expected_accounts = {Path(p).name for p in locator.groups} if layout == 3 else set(keys)
    require(set(p.name for p in (root/'accounts').iterdir()) == expected_accounts, 'account roster differs')
    allocations = {a['allocationId']: a for a in request.document['allocations']}
    windows = {w['windowId']: w for w in request.document['windows']}
    from data_provider.manifest import to_epoch_ms
    reader = AccountStudy(root, study, keys, status, locator=locator)
    if v2:
        from ..auxiliary import study as auxiliary
        dependency_links = auxiliary.load_links(root,keys)
        closed(study['inputs'], ('native','auxiliary'))
        auxiliary_inputs = {}
        provenance = read_json(root/'provenance.json')
        require(set(provenance.get('auxiliarySnapshots',{})) == {b['snapshotId'] for b in request.document['auxiliaryInputs']}, 'snapshot provenance roster')
        for b in request.document['auxiliaryInputs']:
            parent = provenance['auxiliarySnapshots'][b['snapshotId']]
            closed(parent,('contentId','provenanceId')); digest_id(parent['provenanceId'])
            require(parent['contentId']==b['expectedSnapshotContentId'],'parent semantic pin provenance')
    expected_members = {'study.json', 'status.json', 'provenance.json', *DERIVED}
    expected_members.update(locator.physical())
    expected_inputs = {}
    origin, start, end = bounds(request.document)
    sources = {s['inputId']: s for s in request.document['sources']}
    require(study['warmupRequirements'] == {a['allocationId']: warmup_requirements(request.document, a)
        for a in request.document['allocations']}, 'warmup facts')
    for label, key in zip(study['family'], keys):
        record = reader.account(key)
        if v2:
            members, facts = auxiliary.check_links(root,request.document,record,dependency_links[key])
            expected_members.update(members)
            for fact in facts: auxiliary_inputs[(fact['instrumentId'],fact['allocationId'],fact['bindingId'])] = fact
        require(record.labels == label, 'record roster labels')
        allocation, window = allocations[label['allocationId']], windows[label['windowId']]
        require(asdict(record.result.policy) == allocation['policy'] and
            asdict(record.result.window) == dict(startMs=to_epoch_ms(window['startUtc'], 'start'),
                                                endMs=to_epoch_ms(window['endUtc'], 'end')), 'planned account parameters')
        require(record.prepared.target.header['instrument_id'] == label['instrumentId'] and
                record.prepared.target.stepMs == allocation['timeframeMinutes']*60000, 'planned target binding')
        require(record.preparationFacts['requests'] == allocation['features'], 'planned feature requests')
        require(record.preparationFacts['targetId'] == 'target', 'planned target alias')
        used = {f['inputId'] for f in allocation['features']} | {'target'}
        require({i['inputId'] for i in record.preparationFacts['inputs']} == used, 'allocation input subset')
        for item in record.preparationFacts['inputs']:
            alias = item['inputId']
            declaration = dict(inputId='target', instrumentId='target', timeframeMinutes=allocation['timeframeMinutes']) if alias == 'target' else sources[alias]
            resolved = label['instrumentId'] if declaration['instrumentId'] == 'target' else declaration['instrumentId']
            h = item['header']
            require((h['instrument_id'], h['timeframe_minutes'], h['warmup_start_ms'], h['start_ms'], h['end_ms']) ==
                    (resolved, declaration['timeframeMinutes'], origin, start, end), 'resolved input bounds/binding')
            expected_inputs[(label['instrumentId'], label['allocationId'], alias)] = dict(
                instrumentId=label['instrumentId'], allocationId=label['allocationId'], declared=declaration,
                resolvedInstrumentId=resolved, header=h, inputFingerprint=item['inputFingerprint'],
                rules=asdict(record.result.rules) if alias == 'target' else None)
        meta = locator.metadata(key)
        expected_members.update('commands/'+record.result.commandId+'/'+n for n in ('commands.json', 'arrays.npz'))
        grid_ids = [meta['gridId']]
        if study['retentionProfile'] == 'debug':
            folder = 'debug/'+record.result.preparationId+'/'
            debug = read_json(root/(folder+'debug.json'))
            closed(debug, ('inputs', 'requests', 'targetId'))
            require(debug['requests'] == record.preparationFacts['requests'] and debug['targetId'] == 'target', 'debug facts')
            grid_ids += debug['inputs']
            expected_members.update(folder+n for n in ('features.npz', 'debug.json'))
        for grid in grid_ids:
            digest_id(grid)
            expected_members.update('grids/'+grid+'/'+n for n in ('grid.json', 'arrays.npz'))
        # Also refuses carrying an open mark across missing target slots.
        from .metrics import synchronized
        synchronized(record)
    actual_inputs = [expected_inputs[k] for k in sorted(expected_inputs)]
    if v2: actual_inputs = dict(native=actual_inputs,auxiliary=[auxiliary_inputs[k] for k in sorted(auxiliary_inputs)])
    require(study['inputs'] == actual_inputs, 'data-input facts differ from accounts')
    require(locator.logical_count(len(expected_members)) <= MAX_MEMBERS, 'logical member limit')
    if sealed:
        require(status['completedAccounts'] == status['plannedAccounts'] == len(keys) and status['failedJobs'] == 0,
                'completion counts')
        require(expected_members == set(seal['members']), 'unexpected/missing evidence member')
    return reader


def load_account_study(run_root, *, allow_partial=False):
    root = Path(run_root)
    if not (root/'completion.json').exists() and allow_partial:
        status = read_json(root/'status.json')
        require(status['state'] in ('running', 'failed', 'interrupted', 'complete'), 'partial status')
        # Diagnostic metadata only; never advertise unsealed results as checked accounts.
        return AccountStudy(root, read_json(root/'study.json'), (), status, partial=True)
    return _load(root)


def verify_account_study(run_root):
    reader = load_account_study(run_root)
    v2 = reader.study['schema'] == SCHEMA_V2
    if v2:
        from ..auxiliary.study import load_links, verify_commands
        links = load_links(reader.root,reader.accountKeys)
    for key in reader.accountKeys:
        record = reader.account(key); r = record.result
        prepared = verify_commands(reader.root,reader.study['request'],record,links[key]) if v2 else record.prepared
        validate_result(r, prepared, r.commands, r.policy, r.window, r.rules)
    result = dict(status='verified', accountsChecked=len(reader.accountKeys),
        validator='tools.pattern_lab.accounts.validation.validate_result',
        validatorSha256=file_hash(Path(__file__).parents[1]/'accounts/validation.py'),
        limitations=('Retained native/auxiliary commands regenerated; parent membership cannot be proven without the parent snapshot.' if v2 else
            'Target-only original preparation facts; no pack authentication, provider signal regeneration or first-exit search.'))
    if v2: result['commandRegeneration'] = 'exact'
    return result


def write_derived(reader, destination):
    from .metrics import derive
    from .report import render
    destination = Path(destination)
    summary = derive(reader)
    write_json(destination/'derived/summary.json', summary)
    (destination/'derived/report.html').write_text(render(reader.study['request'], summary, reader.study['identities']), encoding='utf-8')
    return summary


def rebuild_account_report(run_root, *, output_root):
    reader = load_account_study(run_root)
    destination = fresh_directory(output_root, run_root)
    write_derived(reader, destination)
    return dict(status='complete', outputRoot=str(destination), files=list(DERIVED))


def export_account_bundle(run_root, *, zip_path):
    reader = load_account_study(run_root)
    seal = verify_directory(reader.root)
    target = Path(zip_path).expanduser().resolve()
    require(not target.exists() and not target.is_relative_to(reader.root.resolve()), 'ZIP output must be new and outside run')
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted([*seal['members'], 'completion.json']):
            stored = reader.study['formatVersion'] == 3 and name in reader._locator.groups
            archive.write(reader.root/name, name, compress_type=zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED)
    return dict(path=str(target), bytes=target.stat().st_size, sha256=file_hash(target))


def import_account_bundle(zip_path, *, output_root):
    target = Path(output_root).expanduser().resolve()
    require(not target.exists(), 'import destination exists')
    archive_path = Path(zip_path).expanduser().resolve()
    require(not archive_path.is_relative_to(target), 'ZIP overlaps destination')
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        infos = archive.infolist()
        names = [item.filename for item in infos]
        require(len(names) == len(set(names)) and names.count('completion.json') == 1, 'duplicate/missing ZIP member')
        member_names(names)
        for info in infos:
            require(not info.is_dir() and not stat.S_ISLNK(info.external_attr >> 16) and
                    stat.S_IFMT(info.external_attr >> 16) in (0, stat.S_IFREG), 'non-file ZIP member')
        info = archive.getinfo('completion.json')
        require(info.file_size <= MAX_ENVELOPE_BYTES, 'ZIP completion too large')
        with archive.open(info) as stream:
            raw = stream.read(MAX_ENVELOPE_BYTES+1)
        require(len(raw) <= MAX_ENVELOPE_BYTES, 'ZIP completion exceeds bound')
        seal = envelope(parse_json(raw.decode('utf-8')))
        require(set(names) == set(seal['members']) | {'completion.json'}, 'ZIP member set differs')
        total = sum(row['bytes'] for row in seal['members'].values()) + len(raw)
        require(shutil.disk_usage(target.parent).free >= total, 'insufficient disk for declared ZIP payload')
        for name, row in seal['members'].items():
            require(archive.getinfo(name).file_size == row['bytes'], 'ZIP declared size mismatch')
        staging = Path(tempfile.mkdtemp(prefix='.account-import-', dir=target.parent))
        try:
            (staging/'completion.json').write_bytes(raw)
            for name, row in seal['members'].items():
                p = staging/name; p.parent.mkdir(parents=True, exist_ok=True)
                remaining = row['bytes']
                with archive.open(name) as source, p.open('xb') as out:
                    while remaining:
                        block = source.read(min(1024*1024, remaining))
                        require(block, 'truncated ZIP payload')
                        out.write(block); remaining -= len(block)
                    require(not source.read(1), 'ZIP exceeds declared size')
                require(file_hash(p) == row['sha256'], 'ZIP hash mismatch')
            load_account_study(staging)
            require(not target.exists(), 'import destination appeared during verification')
            staging.rename(target)
        finally:
            if staging.exists(): shutil.rmtree(staging)
    return load_account_study(target)
