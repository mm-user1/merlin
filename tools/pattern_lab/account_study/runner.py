"""Coordinator-only pack reads/publication, bounded existing-pool dispatch."""
from dataclasses import asdict
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
import sys

import numpy as np

from .. import data, manifest, PatternLabDataError
from ..accounts import PreparedInput
from ..accounts.contracts import require, integer
from ..study import workers as workers_module
from ..study.runner import metadata_admission_failures
from ..study.bracket_rules import normalize_rules
from ..study.contracts import semantic_digest
from .request import normalize_request, bounds, planned_family, warmup_requirements
from .common import fresh_directory, file_hash, write_json
from .codec import encode_accounts, encode_container
from .containers import index_for, MAX_JSON
from .common import json_text
from .job import AccountJob, run_account_job, check_generation
from .artifacts import SCHEMA, _load, load_account_study, write_derived, seal_directory


def implementation(*, auxiliary=False):
    """Account-owned attribution; never changes legacy source-generation lists."""
    repo = Path(__file__).resolve().parents[3]
    from ..study.extensions import CORE_MODULES
    names = {('src/data_provider/__init__.py' if name == 'data_provider' else
              ('src/' if name.startswith('data_provider.') else '') + name.replace('.', '/') + '.py')
             for name in CORE_MODULES}
    names.update(('tools/pattern_lab/study/workers.py', 'tools/pattern_lab/study/bracket.py',
                  'tools/pattern_lab/study/bracket_rules.py', 'src/core/engine_v2/sizing.py',
                  'src/core/engine_v2/contracts.py', 'src/core/engine_v2/price_rounding.py',
                  'src/core/engine_v2/execution_modes.py', 'src/core/backtest_engine.py'))
    # The guarded reference loader owns the numerical source list too.
    from ..study.bracket import reference_core
    core = reference_core()
    names.add(Path(core.__file__).resolve().relative_to(repo).as_posix())
    for package in ('accounts', 'account_study') + (('auxiliary',) if auxiliary else ()):
        names.update(p.relative_to(repo).as_posix() for p in (repo/'tools/pattern_lab'/package).iterdir()
                     if p.suffix in ('.py', '.js'))
    # Only real consumed files belong in attribution (CORE_MODULES are import names).
    require(all((repo/name).is_file() for name in names), 'missing account implementation source')
    return dict(version='account-study.1', sources={name: file_hash(repo/name) for name in sorted(names)},
        python=sys.version.split()[0], libraries={name: version(name) for name in ('numpy', 'pandas', 'pyarrow', 'numba')})


def run_account_study(request, *, data_root, output_root, workers=1, retention_profile='standard', auxiliary_roots=None,
                      _legacy_directory=False):
    normalized = normalize_request(request)
    integer(workers, 'workers', 1)
    require(retention_profile in ('standard', 'debug'), 'retention profile')
    require(type(_legacy_directory) is bool, 'legacy directory hook must be bool')
    doc = normalized.document
    index = index_for(planned_family(doc))  # Lower bound before execution/output creation.
    require(len(json_text(index).encode('utf-8')) + 2 <= MAX_JSON, 'account index exceeds bound')
    v2 = doc['schema'] == 'pattern-lab.account-study-request.v2'
    origin, start, end = bounds(doc)
    pack_root, output = Path(data_root).expanduser().resolve(), Path(output_root).expanduser().resolve()
    require(not output.exists() and not output.is_relative_to(pack_root) and not pack_root.is_relative_to(output), 'new disjoint output required')
    snapshots = None
    if v2:
        from ..auxiliary import study as auxiliary
        snapshots = auxiliary.admit(doc, auxiliary_roots, output, pack_root)
    else:
        require(auxiliary_roots is None, 'v1 has no auxiliary roots')
    source = implementation(auxiliary=v2)
    with data.read_session(pack_root) as session:
        report = session.inspect(verify=False)
        require(report.get('state') == 'ready', 'pack not ready')
        entries = {e['instrument_id']: e for e in report['instruments']}
        fixed_ids = {s['instrumentId'] for s in doc['sources'] if s['instrumentId'] != 'target'}
        wanted = set(doc['instruments']) | fixed_ids
        require(wanted <= set(entries), 'declared target/source absent from pack')
        require(all(set(entries[i]['roles']) & {'trading', 'research_only'} for i in doc['instruments']), 'factor-only target')
        failures = metadata_admission_failures([entries[i] for i in sorted(wanted)],
            SimpleNamespace(warmup_start_ms=origin, study_start_ms=start, study_end_ms=end, timeframes=(5,)))
        require(not failures, 'metadata admission: '+'; '.join(failures))
        raw_entries = {e['instrument_id']: e for e in manifest.read_manifest(pack_root)['instruments']}
        rules = {i: normalize_rules(raw_entries[i]) for i in doc['instruments']}
        require(session.inspect(verify=True)['verification_check']['ok'], 'pack integrity admission failed')
        root = fresh_directory(output, pack_root)
        effective = min(workers, len(doc['instruments']))
        state = dict(state='running', phase='read', currentJob=None, plannedAccounts=len(planned_family(doc)),
            completedAccounts=0, failedJobs=0, plannedJobs=len(doc['instruments']), completedJobs=0,
            jobs={i: 'notStarted' for i in doc['instruments']}, requestedWorkers=workers, effectiveWorkers=effective,
            maximumInFlight=0, startedUtc=datetime.now(timezone.utc).isoformat())
        study = dict(schema=auxiliary.SCHEMA if v2 else SCHEMA, formatVersion=(2 if v2 else 1) if _legacy_directory else 3, request=doc, family=planned_family(doc),
            identities={}, inputs=[], implementation=source, retentionProfile=retention_profile,
            warmupRequirements={a['allocationId']: warmup_requirements(doc, a) for a in doc['allocations']},
            roster=[{k: entries[i][k] for k in ('instrument_id', 'venue', 'contract', 'quote_currency', 'roles')}
                    for i in doc['instruments']])
        write_json(root/'study.json', study)
        write_json(root/'status.json', state)
        common, input_facts, physical = {}, {}, {}
        auxiliary_facts, dependency_links, auxiliary_common = {}, {}, {}

        def read_base(identifier):
            base = session.load_slice(identifier, start=manifest.format_epoch_ms(start), end=manifest.format_epoch_ms(end),
                warmup_start=manifest.format_epoch_ms(origin), timeframe_minutes=5)
            physical[identifier] = dict(base.physical)
            return base.bars.index.view('int64')//1000000, base.bars.to_numpy(dtype=np.float64)

        def payload(identifier):
            state.update(currentJob=identifier, phase='read')
            state['jobs'][identifier] = 'admitting'
            write_json(root/'status.json', state)
            local = {}
            def raw(i):
                if i in fixed_ids:
                    if i not in common: common[i] = read_base(i)
                    return common[i]
                if i not in local: local[i] = read_base(i)
                return local[i]
            bindings, prepared = {}, {}
            for a in doc['allocations']:
                used = {f['inputId'] for f in a['features']}
                sources = [s for s in doc['sources'] if s['inputId'] in used]
                declarations = [dict(inputId='target', instrumentId='target', timeframeMinutes=a['timeframeMinutes'])]+sources
                values = []
                for s in declarations:
                    resolved = identifier if s['instrumentId'] == 'target' else s['instrumentId']
                    key = (resolved, s['timeframeMinutes'])
                    if key not in prepared:
                        t, v = raw(resolved)
                        entry = entries[resolved]
                        header = data.fingerprint_header(instrument_id=resolved, venue=entry['venue'], contract=entry['contract'],
                            quote_currency=entry['quote_currency'], timeframe_minutes=s['timeframeMinutes'],
                            start_ms=start, end_ms=end, warmup_start_ms=origin)
                        series = data.prepare_series(timestamps=t, ohlcv=v, **{k: header[k] for k in
                            ('instrument_id', 'venue', 'contract', 'quote_currency', 'timeframe_minutes', 'start_ms', 'end_ms', 'warmup_start_ms')})
                        prepared[key] = (header, series)
                    header, series = prepared[key]
                    values.append(PreparedInput(s['inputId'], series, header))
                    fact = dict(instrumentId=identifier, allocationId=a['allocationId'], declared=s, resolvedInstrumentId=resolved,
                        header=header, inputFingerprint=series.input_fingerprint,
                        rules=asdict(rules[identifier]) if s['inputId'] == 'target' else None)
                    input_facts[(identifier, a['allocationId'], s['inputId'])] = fact
                bindings[a['allocationId']] = tuple(values)
            state['jobs'][identifier] = 'running'
            deps = auxiliary.dependencies_for(doc,identifier,snapshots,origin,end,auxiliary_common) if v2 else None
            return AccountJob(identifier, tuple(doc['allocations']), tuple(doc['windows']), bindings,
                              rules[identifier], source['sources'], deps)

        def publish(identifier, records):
            state.update(currentJob=identifier, phase='publishJob')
            check_generation(source['sources'])
            require(len(records) == len(doc['allocations'])*len(doc['windows']) and
                    all(r.labels['instrumentId'] == identifier for r in records), 'worker result roster')
            writer = encode_accounts if _legacy_directory else encode_container
            writer(root, records, retention_profile=retention_profile)
            if v2:
                for record in records:
                    key = semantic_digest(record.labels)
                    dependency_links[key] = auxiliary.save_evidence(root,record)
                    for bid, dep in record.auxiliaryEvidence['dependencies'].items():
                        fact = auxiliary.input_fact(doc,record.labels,bid,dep['content'])
                        auxiliary_facts[(identifier,record.labels['allocationId'],bid)] = fact
            state['completedAccounts'] += len(records); state['completedJobs'] += 1
            state['jobs'][identifier] = 'complete'
            write_json(root/'status.json', state)

        try:
            if workers == 1:
                for identifier in doc['instruments']:
                    job = payload(identifier)
                    state['maximumInFlight'] = 1
                    publish(identifier, run_account_job(job))
                    del job
            else:
                pool_module = workers_module
                with pool_module.single_threaded_children():
                    pool = pool_module.SpawnJobPool(effective, pool_module.WorkerSettings((), (), ()))
                    abort = True
                    try:
                        pool.start()
                        state['workerPids'] = pool.worker_pids
                        state['childThreads'] = pool.thread_evidence
                        pending, cursor = set(), 0
                        while cursor < len(doc['instruments']) or pending:
                            while cursor < len(doc['instruments']) and len(pending) < effective:
                                identifier = doc['instruments'][cursor]
                                job = payload(identifier)
                                pool.submit(identifier, job); del job
                                pending.add(identifier); cursor += 1
                                state['maximumInFlight'] = max(state['maximumInFlight'], len(pending))
                            kind, identifier, result = pool.take()
                            state['currentJob'] = identifier
                            require(kind == 'result', 'account worker failed: '+str(result) if kind != 'result' else '')
                            publish(identifier, result); del result
                            pending.remove(identifier)
                        abort = False
                    finally:
                        pool.shutdown(abort=abort)
            state.update(phase='derive', currentJob=None)
            if not _legacy_directory: write_json(root/'account-index.json', index)
            study['inputs'] = [input_facts[k] for k in sorted(input_facts)]
            if v2:
                study['inputs'] = dict(native=study['inputs'], auxiliary=[auxiliary_facts[k] for k in sorted(auxiliary_facts)])
                write_json(root/'dependency-links.json',dict(schema='pattern-lab.account-dependency-links.v1',accounts=dependency_links))
            study['identities'] = dict(specification=normalized.identity, dataInput=semantic_digest(study['inputs']),
                                       implementation=semantic_digest(source))
            write_json(root/'study.json', study)
            provenance = dict(physicalInputs=physical, ruleSnapshots={i: raw_entries[i]['instrument_rules'] for i in doc['instruments']})
            if v2:
                provenance['auxiliarySnapshots'] = {alias:dict(contentId=s.contentId,provenanceId=s.seal['provenanceId']) for alias,s in snapshots.items()}
            write_json(root/'provenance.json', provenance)
            write_derived(_load(root, sealed=False), root)
            check_generation(source['sources'])
            state.update(state='complete', phase='complete', finishedUtc=datetime.now(timezone.utc).isoformat())
            write_json(root/'status.json', state)
            seal_directory(root)
        except BaseException as exc:
            # No completed subset is advertised, including publication and interrupt failures.
            if not (root/'completion.json').exists():
                current = state['currentJob']
                if current is not None: state['jobs'][current] = 'failed'
                state.update(state='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed', failedJobs=1,
                    errorType=type(exc).__name__, error=str(exc), finishedUtc=datetime.now(timezone.utc).isoformat())
                write_json(root/'status.json', state)
            raise
    return load_account_study(root)
