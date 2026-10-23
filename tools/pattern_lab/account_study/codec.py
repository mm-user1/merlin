"""Lossless account records, shared grids/commands and target-only validation views."""
from dataclasses import dataclass, fields, asdict
from pathlib import Path
import json
import os
import tempfile
import zipfile

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .. import data
from ..accounts import contracts as c, evidence as e
from ..accounts.preparation import PreparedAccount, bind, Feature, AlignedState, validate_request
from ..accounts.execution import validate_inputs, account_identity, window_indices, coverage
from ..study.bracket_rules import ExecutionRules
from ..study.contracts import semantic_digest, canonical_json
from .common import read_json, write_json, closed, digest_id, json_text
from .containers import Locator, FILES, MAX_JSON, container_path, zip_info

FORMAT = 'pattern-lab.account-record.v1'
TABLES = dict(attempts=e.ATTEMPTS, trades=e.TRADES, proposals=e.PROPOSALS, exitCommands=e.EXITS, path=e.PATH)
SOURCE_FACTS = ('sourceIndex', 'sourceReason', 'observedAtMs', 'availableAtMs', 'validUntilMs')
DISK_ATTEMPTS = pa.schema([pa.field(f.name, f.type, nullable=True if f.name in SOURCE_FACTS else f.nullable)
                          for f in e.ATTEMPTS] + [pa.field('filterPresent', pa.bool_(), nullable=False)])
DISK_PATH = pa.schema([f for f in e.PATH if f.name not in ('barOpenMs', 'closePrice')])


@dataclass(frozen=True)
class AccountRecord:
    prepared: PreparedAccount
    result: c.AccountResult
    labels: dict
    preparationFacts: dict | None = None
    auxiliaryEvidence: dict | None = None


def preparation_facts(p):
    return dict(schema=c.SCHEMA, preparationVersion='1', alignmentVersion='1',
        seed='segment-first-real-open-close;pine-rma;neutral-break;priorBars-mask',
        validity='next-scheduled-close-exclusive', targetId=p.targetId,
        inputs=[dict(inputId=v.inputId, header=v.header, inputFingerprint=v.inputFingerprint)
                for v in sorted(p.inputs, key=lambda v: v.inputId)],
        requests=[f.request for f in sorted(p.features, key=lambda f: f.request['requestId'])])


def save_arrays(path, arrays):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as stream:
        np.savez_compressed(stream, **arrays)


def load_arrays(path):
    with np.load(path, allow_pickle=False) as archive:
        c.require(len(archive.files) == len(set(archive.files)), 'duplicate array key')
        return {k: c.array(archive[k], archive[k].dtype) for k in archive.files}


def save_input(root, value):
    arrays = {k: getattr(value, k) for k in ('timestamps', 'values', 'researchMask', 'segmentStart')}
    meta = dict(inputId=value.inputId, header=value.header, inputFingerprint=value.inputFingerprint,
                coverage=json.loads(value.coverageJson))
    identity = c.command_array_digest(arrays, meta)
    directory = root/'grids'/identity
    if not directory.exists():
        directory.mkdir(parents=True)
        save_arrays(directory/'arrays.npz', arrays)
        write_json(directory/'grid.json', meta)
    return identity


def load_input(root, identity):
    digest_id(identity)
    directory = root/'grids'/identity
    meta, arrays = read_json(directory/'grid.json'), load_arrays(directory/'arrays.npz')
    closed(meta, ('inputId', 'header', 'inputFingerprint', 'coverage'))
    c.require(c.command_array_digest(arrays, meta) == identity, 'grid identity')
    c.require(set(arrays) == {'timestamps', 'values', 'researchMask', 'segmentStart'}, 'grid array keys')
    h, cov = meta['header'], meta['coverage']
    closed(cov, ('base_row_count', 'base_gap_count', 'omitted_group_count'))
    s = data.PreparedSeries(h['timeframe_minutes'], arrays['timestamps'], arrays['values'],
        arrays['researchMask'], int(np.argmax(arrays['researchMask'])), arrays['segmentStart'],
        **cov, input_fingerprint=meta['inputFingerprint'])
    return bind(c.PreparedInput(meta['inputId'], s, h))


def save_commands(root, commands):
    directory = root/'commands'/commands.identity
    if directory.exists(): return
    arrays = {'decisionMs': commands.decisionMs}
    for side in ('long', 'short'):
        arrays.update({side+'.'+f.name: getattr(getattr(commands, side), f.name) for f in fields(c.SideCommands)})
    filt = commands.entryFilter
    fm = None
    if filt is not None:
        arrays.update({'filter.'+f.name: getattr(filt, f.name) for f in fields(filt)
                       if isinstance(getattr(filt, f.name), np.ndarray)})
        fm = dict(preparationId=filt.preparationId, provenance=json.loads(filt.provenance), identity=filt.identity)
    save_arrays(directory/'arrays.npz', arrays)
    write_json(directory/'commands.json', dict(preparationId=commands.preparationId,
        mode=commands.mode, stopUpdates=commands.stopUpdates, provenance=json.loads(commands.provenance), entryFilter=fm))


def load_commands(root, identity):
    digest_id(identity)
    directory = root/'commands'/identity
    meta, arrays = read_json(directory/'commands.json'), load_arrays(directory/'arrays.npz')
    closed(meta, ('preparationId', 'mode', 'stopUpdates', 'provenance', 'entryFilter'))
    decisions = arrays.pop('decisionMs')
    sides = {side: c.SideCommands(**{f.name: arrays.pop(side+'.'+f.name) for f in fields(c.SideCommands)})
             for side in ('long', 'short')}
    fm = meta.pop('entryFilter')
    filt = None
    if fm is not None:
        closed(fm, ('preparationId', 'provenance', 'identity'))
        c.require(all(k.startswith('filter.') for k in arrays), 'unexpected command array')
        filt = c.EntryFilter(**{k[7:]: v for k, v in arrays.items()},
                            preparationId=fm['preparationId'], provenance=fm['provenance'])
        c.require(filt.identity == fm['identity'], 'filter identity')
    else:
        c.require(not arrays, 'unexpected filter arrays')
    result = c.CommandBundle(decisionMs=decisions, **sides, entryFilter=filt, **meta)
    c.require(result.identity == identity, 'command identity')
    return result


def disk_attempts(table, present):
    arrays = [pa.nulls(len(table), f.type) if not present and f.name in SOURCE_FACTS else table[f.name]
              for f in e.ATTEMPTS]
    return pa.Table.from_arrays(arrays+[pa.array(np.full(len(table), present))], schema=DISK_ATTEMPTS)


def native_attempts(table, present):
    check_table(table, DISK_ATTEMPTS)
    c.require(pc.all(pc.equal(table['filterPresent'], present)).as_py() is not False, 'filter presence')
    if not present:
        c.require(all(table[k].null_count == len(table) for k in SOURCE_FACTS), 'unfiltered source facts must be null')
        c.require(all(pc.all(table[k]).as_py() is not False for k in ('filterKnown', 'filterAllowed')), 'unfiltered permissions')
    arrays = [pa.array(np.full(len(table), -1 if f.name == 'sourceIndex' else 0), type=f.type)
              if not present and f.name in ('sourceIndex', 'sourceReason') else table[f.name] for f in e.ATTEMPTS]
    return pa.Table.from_arrays(arrays, schema=e.ATTEMPTS)


def check_table(table, schema):
    c.require(table.schema == schema, 'retained table schema')
    for f in schema:
        col = table[f.name]
        c.require(f.nullable or col.null_count == 0, 'required table null')
        if pa.types.is_floating(f.type):
            c.require(pc.all(pc.fill_null(pc.is_finite(col), True)).as_py() is not False, 'nonfinite table value')


def _metadata(root, record, retention_profile):
    p, r = record.prepared, record.result
    facts = record.preparationFacts or preparation_facts(p)
    c.require(semantic_digest(facts) == p.identity, 'original preparation facts')
    grid = save_input(root, p.target)
    save_commands(root, r.commands)
    if retention_profile == 'debug': save_debug(root, p)
    meta = {f.name: getattr(r, f.name) for f in fields(r) if f.name not in TABLES and f.name != 'commands'}
    for name in ('policy', 'window', 'rules', 'coverage'): meta[name] = asdict(meta[name])
    return dict(format=FORMAT, labels=record.labels, gridId=grid, preparationFacts=facts, result=meta)


def _disk_table(record, name):
    table = getattr(record.result, name)
    if name == 'attempts': return disk_attempts(table, record.result.commands.entryFilter is not None)
    if name == 'path': return table.select(DISK_PATH.names)
    return table


def encode_accounts(directory, records, *, retention_profile='standard'):
    root = Path(directory)
    c.require(retention_profile in ('standard', 'debug'), 'retention profile')
    published = []
    for record in records:
        meta = _metadata(root, record, retention_profile)
        key = semantic_digest(record.labels)
        folder = root/'accounts'/key
        c.require(not folder.exists(), 'duplicate account record')
        folder.mkdir(parents=True)
        write_json(folder/'account.json', meta)
        for name in TABLES:
            pq.write_table(_disk_table(record, name), folder/(name+'.parquet'), compression='zstd', version='2.6')
        decoded = decode_account(root, key, _locator=Locator(root, [key]))
        c.require(equal_records(record, decoded), 'codec publication round-trip mismatch')
        published.append(key)
    return published


def encode_container(directory, records, *, retention_profile='standard'):
    root = Path(directory)
    c.require(retention_profile in ('standard', 'debug'), 'retention profile')
    c.require(records, 'empty account container')
    instrument = records[0].labels['instrumentId']
    ordered = sorted((semantic_digest(r.labels), r) for r in records)
    keys = [k for k, r in ordered]
    c.require(len(set(keys)) == len(keys) and all(r.labels['instrumentId'] == instrument for k, r in ordered),
              'container account roster')
    destination = root/container_path(instrument)
    c.require(not destination.exists(), 'account container destination exists')
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Only metadata is retained in addition to the existing worker batch.
    metadata = {key: _metadata(root, record, retention_profile) for key, record in ordered}
    fd, name = tempfile.mkstemp(prefix='.account-', suffix='.tmp', dir=destination.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'w+b') as output:
            with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for key, record in ordered:
                    for filename in FILES:
                        info = zip_info('accounts/'+key+'/'+filename)
                        with archive.open(info, 'w', force_zip64=True) as stream:
                            if filename == 'account.json':
                                raw = (json_text(metadata[key]) + os.linesep).encode('utf-8')
                                c.require(len(raw) <= MAX_JSON, 'account JSON exceeds bound')
                                stream.write(raw)
                            else:
                                pq.write_table(_disk_table(record, filename[:-8]), stream, compression='zstd', version='2.6')
        path = temporary.relative_to(root).as_posix()
        locator = Locator(root, keys, [dict(path=path, accountKeys=keys)])
        for key, record in ordered:
            c.require(equal_records(record, decode_account(root, key, _locator=locator)),
                      'codec publication round-trip mismatch')
        c.require(not destination.exists(), 'account container destination appeared before publication')
        temporary.rename(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return keys


def decode_account(root, key, *, _locator=None):
    digest_id(key)
    root = Path(root)
    locator = _locator if _locator is not None else Locator.from_root(root)
    locator.check(root, key)
    meta = locator.metadata(key)
    closed(meta, ('format', 'labels', 'gridId', 'preparationFacts', 'result'))
    c.require(meta['format'] == FORMAT and semantic_digest(meta['labels']) == key, 'account record identity')
    facts, raw = meta['preparationFacts'], meta['result']
    closed(raw, [f.name for f in fields(c.AccountResult) if f.name not in TABLES and f.name != 'commands'])
    closed(facts, ('schema', 'preparationVersion', 'alignmentVersion', 'seed', 'validity', 'targetId', 'inputs', 'requests'))
    c.require(facts['schema'] == c.SCHEMA and facts['preparationVersion'] == facts['alignmentVersion'] == '1' and
        facts['seed'] == 'segment-first-real-open-close;pine-rma;neutral-break;priorBars-mask' and
        facts['validity'] == 'next-scheduled-close-exclusive', 'preparation version/conventions')
    c.require(semantic_digest(facts) == raw['preparationId'], 'preparation identity facts')
    target = load_input(root, meta['gridId'])
    ids = []
    for item in facts['inputs']:
        closed(item, ('inputId', 'header', 'inputFingerprint'))
        c.identifier(item['inputId']); ids.append(item['inputId'])
        data.validate_fingerprint_header(item['header'])
        digest_id(item['inputFingerprint'])
    c.require(ids == sorted(set(ids)), 'preparation input order/duplicates')
    request_ids = []
    for raw_request in facts['requests']:
        request = c.AccountFeatureRequest(**raw_request)
        validate_request(request, dict.fromkeys(ids))
        request_ids.append(request.requestId)
    c.require(request_ids == sorted(set(request_ids)), 'preparation request order/duplicates')
    c.require(dict(inputId=target.inputId, header=target.header, inputFingerprint=target.inputFingerprint)
              in facts['inputs'] and facts['targetId'] == target.inputId, 'target facts')
    p = PreparedAccount(c.SCHEMA, raw['preparationId'], target.inputId, (target,), ())
    commands = load_commands(root, raw['commandId'])
    tables = {}
    for name, schema in TABLES.items():
        t = locator.table(key, name)
        if name == 'attempts': t = native_attempts(t, commands.entryFilter is not None)
        if name == 'path':
            check_table(t, DISK_PATH)
            ix = t['barIndex'].to_numpy()
            c.require(np.all((ix >= 0) & (ix < len(target.timestamps))), 'path index bounds')
            t = pa.Table.from_arrays([pa.array(target.timestamps[ix]) if f.name == 'barOpenMs' else
                pa.array(target.values[ix, 3]) if f.name == 'closePrice' else t[f.name] for f in schema], schema=schema)
        check_table(t, schema); tables[name] = t
    scalar = dict(raw)
    for name, cls in (('policy', c.ExecutionPolicy), ('window', c.AccountWindow),
                       ('rules', ExecutionRules), ('coverage', c.Coverage)):
        scalar[name] = cls(**scalar[name])
    r = c.AccountResult(**scalar, commands=commands, **tables)
    validate_inputs(p, commands, r.policy, r.window, r.rules)
    c.require(r.schema == c.SCHEMA and r.identity == account_identity(p, commands, r.policy, r.window, r.rules), 'account identity')
    c.require(r.commandId == commands.identity and r.preparationId == p.identity, 'account bindings')
    ix = window_indices(p, r.window)
    c.require(r.coverage == coverage(p, r.window, ix) and r.terminalStatus == ('closed' if len(ix) else 'noObservedBars'), 'account coverage')
    c.require(np.array_equal(r.path['barIndex'].to_numpy(), ix), 'path indices/order')
    c.number(r.finalBalance, 'finalBalance', minimum=-float('inf'))
    c.integer(r.unknownExitDecisions, 'unknownExitDecisions')
    check_links(r, p)
    return AccountRecord(p, r, meta['labels'], facts)


def check_links(r, p):
    """Bounded structural links only. Financial consistency belongs to validate_result."""
    n = len(p.target.timestamps)
    for name, id_name in (('attempts', 'attemptId'), ('trades', 'tradeId')):
        t = getattr(r, name)
        c.require(np.array_equal(t[id_name].to_numpy(), np.arange(len(t))), 'table key order')
        ix = t['signalIndex'].to_numpy()
        c.require(np.all((ix >= 0) & (ix < n)) and np.all(np.diff(ix) >= 0), 'signal order/bounds')
        c.require(np.array_equal(t['decisionMs'].to_numpy(), p.target.timestamps[ix]+p.stepMs), 'decision links')
    trades, attempts = r.trades.to_pydict(), r.attempts.to_pydict()
    previous = -1
    for j, aid in enumerate(trades['attemptId']):
        a, en, ex = trades['signalIndex'][j], trades['entryIndex'][j], trades['exitIndex'][j]
        c.require(0 <= aid < len(r.attempts) and a > previous and a < en <= ex < n, 'trade chronology')
        previous = ex
        c.require(attempts['tradeId'][aid] == j and attempts['eventId'][aid] == trades['eventId'][j] and
                  attempts['reason'][aid] == 'filled', 'trade/attempt link')
    for aid, tid in enumerate(attempts['tradeId']):
        c.require((tid is None and attempts['reason'][aid] != 'filled') or
                  (type(tid) is int and 0 <= tid < len(r.trades) and trades['attemptId'][tid] == aid), 'attempt/trade link')
    for table in (r.proposals, r.exitCommands):
        tids, indices = table['tradeId'].to_numpy(), table['decisionIndex'].to_numpy()
        c.require(np.all((tids >= 0) & (tids < len(r.trades))) and np.all((indices >= 0) & (indices < n)), 'trace link bounds')


def decode_accounts(directory):
    root = Path(directory)
    keys = [p.name for p in sorted((root/'accounts').iterdir())]
    locator = Locator(root, keys)  # Explicit low-level directory codec; no format inference.
    return [decode_account(root, key, _locator=locator) for key in keys]


def equal_records(a, b):
    x, y = a.result, b.result
    return (a.labels == b.labels and x.commands.identity == y.commands.identity and
        all(getattr(x, f.name).equals(getattr(y, f.name)) if f.name in TABLES else
            getattr(x, f.name) == getattr(y, f.name) for f in fields(x) if f.name != 'commands'))


def save_debug(root, p):
    folder = root/'debug'/p.identity
    if folder.exists(): return
    inputs = [save_input(root, value) for value in p.inputs]
    arrays, requests = {}, []
    for i, f in enumerate(p.features):
        requests.append(f.request)
        for name in ('values', 'known', 'colour', 'run', 'position'):
            arrays[str(i)+'.'+name] = getattr(f, name)
        if f.aligned is not None:
            for field in fields(f.aligned): arrays[str(i)+'.aligned.'+field.name] = getattr(f.aligned, field.name)
    save_arrays(folder/'features.npz', arrays)
    write_json(folder/'debug.json', dict(inputs=inputs, requests=requests, targetId=p.targetId))


def load_debug_preparation(root, identity):
    digest_id(identity)
    root = Path(root); folder = root/'debug'/identity
    meta, arrays = read_json(folder/'debug.json'), load_arrays(folder/'features.npz')
    features = []
    for i, request in enumerate(meta['requests']):
        aligned = {k.split('.aligned.')[1]: v for k, v in arrays.items() if k.startswith(str(i)+'.aligned.')}
        features.append(Feature(canonical_json(request), *(arrays[str(i)+'.'+n] for n in
            ('values', 'known', 'colour', 'run', 'position')), AlignedState(**aligned) if aligned else None))
    p = PreparedAccount(c.SCHEMA, identity, meta['targetId'],
                        tuple(load_input(root, k) for k in meta['inputs']), tuple(features))
    c.require(semantic_digest(preparation_facts(p)) == identity, 'debug preparation identity')
    return p
