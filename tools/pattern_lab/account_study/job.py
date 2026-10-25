"""Typed, RAM-only account instrument job for direct and existing spawn dispatch."""
from dataclasses import dataclass
from pathlib import Path

from ..accounts import (prepare_inputs, build_commands, candle_filter, execute_account,
                        AccountFeatureRequest, HARecipe, ExecutionPolicy, AccountWindow)
from ..accounts.contracts import require
from data_provider.manifest import to_epoch_ms
from ..study.contracts import semantic_digest
from .codec import AccountRecord
from .common import file_hash


@dataclass(frozen=True)
class AccountJob:
    instrumentId: str
    allocations: tuple
    windows: tuple
    bindings: dict
    rules: object
    sourceGeneration: dict
    auxiliary: dict | None = None


def check_generation(expected):
    repo = Path(__file__).resolve().parents[3]
    require(all(file_hash(repo/name) == digest for name, digest in expected.items()), 'account source generation changed')


def run_account_job(job):
    require(isinstance(job, AccountJob), 'typed account job required')
    check_generation(job.sourceGeneration)
    preparations, commands, records = {}, {}, []
    decoded_slices = {}
    for allocation in job.allocations:
        bindings = job.bindings[allocation['allocationId']]
        features = tuple(AccountFeatureRequest(**f) for f in allocation['features'])
        key = semantic_digest(dict(inputs=[dict(inputId=b.inputId, header=b.header,
            fingerprint=b.series.input_fingerprint) for b in bindings], features=allocation['features']))
        if key not in preparations:
            preparations[key] = prepare_inputs(bindings[0], tuple(bindings[1:]), features)
        p = preparations[key]
        policy = ExecutionPolicy(**allocation['policy'])
        command_key = semantic_digest(dict(preparationId=p.identity, provider=allocation['provider'],
            entryFilter=allocation['entryFilter'], mode=policy.mode, stopUpdates=policy.stopUpdates))
        evidence = None
        if job.auxiliary is not None:
            from ..auxiliary.commands import build_auxiliary_commands
            dependencies = {}
            for bid,dep in job.auxiliary[allocation['allocationId']].items():
                frozen=dep['content'];key=id(frozen)
                if key not in decoded_slices:decoded_slices[key]=frozen.document
                dependencies[bid]=dict(content=decoded_slices[key],coveragePolicy=dep['coveragePolicy'])
            bundle, decisions = build_auxiliary_commands(p, allocation, dependencies)
            evidence = dict(decisions=decisions, dependencies=dependencies)
        elif command_key not in commands:
            recipe = HARecipe(**allocation['provider']['recipe'])
            filt = allocation['entryFilter']
            commands[command_key] = build_commands(p, recipe, mode=policy.mode, stopUpdates=policy.stopUpdates,
                entryFilter=candle_filter(p, filt['requestId']) if filt else None)
        if job.auxiliary is None: bundle = commands[command_key]
        for w in job.windows:
            window = AccountWindow(to_epoch_ms(w['startUtc'], 'startUtc'), to_epoch_ms(w['endUtc'], 'endUtc'))
            result = execute_account(p, bundle, policy, window, job.rules)
            records.append(AccountRecord(p, result, dict(instrumentId=job.instrumentId,
                allocationId=allocation['allocationId'], windowId=w['windowId']), auxiliaryEvidence=evidence))
    check_generation(job.sourceGeneration)
    return records
