"""One decoded native account per task; bounded selection-owned spawn execution."""
from dataclasses import asdict,replace
from concurrent.futures import ProcessPoolExecutor,wait,FIRST_COMPLETED
import multiprocessing
import math
from pathlib import Path

from ..accounts import execute_account
from ..accounts.contracts import require,integer
from ..account_study.codec import decode_account,AccountRecord
from ..account_study.metrics import synchronized
from ..account_study.common import json_text
from ..study.contracts import semantic_digest
from ..study.workers import single_threaded_children,configure_child_threads
from .ledger import REJECTIONS


def execution_key(account_key,capital):
    return semantic_digest(['selection-funded.1',account_key,capital])


def run_job(job):
    root,key,variants=job
    record=decode_account(Path(root),key);original=record.result
    facts=[];paths={}
    require(variants==sorted(variants) and len({c for c,_ in variants})==len(variants),'funded job capital order')
    for capital,retain in variants:
        policy=replace(original.policy,initialCapital=capital)
        result=execute_account(record.prepared,original.commands,policy,original.window,original.rules)
        require(asdict(result.coverage)==asdict(original.coverage) and result.terminalStatus==original.terminalStatus,
            'funded coverage/terminal status differs from parent ledger: '+key)
        trades=result.trades.to_pydict();attempts=result.attempts['reason'].to_pylist()
        identity=execution_key(key,capital)
        row=dict(executionKey=identity,accountKey=key,instrumentId=record.labels['instrumentId'],
            capital=capital,finalBalance=result.finalBalance,accountId=result.identity,
            preparationId=result.preparationId,commandId=result.commandId,
            policyJson=json_text(asdict(policy)),rulesJson=json_text(asdict(result.rules)),windowJson=json_text(asdict(result.window)),
            fees=math.fsum([*trades['entryFee'],*trades['exitFee']]),trades=len(result.trades),attempts=len(result.attempts),
            proposals=len(result.proposals),exitCommands=len(result.exitCommands),unknownExitDecisions=result.unknownExitDecisions,
            sizingRejections={r:attempts.count(r) for r in REJECTIONS},
            forcedExits={r:dict(count=sum(v==r for v in trades['exitReason']),
                netPnl=math.fsum(p for p,v in zip(trades['netPnl'],trades['exitReason']) if v==r)) for r in ('terminal','gapBoundary')},
            pathRef=identity if retain else None)
        facts.append(row)
        if retain:
            times,equity,_=synchronized(AccountRecord(record.prepared,result,record.labels))
            paths[identity]=dict(times=times,equity=equity)
    return facts,paths


class FundedExecutor:
    """Month barriers and at most 2*workers in flight, no retry or fallback."""
    def __init__(self,workers):
        self.workers=integer(workers,'workers',1);require(workers<=6,'workers must be 1..6')
        self.pool=None;self.environment=None
    def __enter__(self):
        if self.workers>1:
            self.environment=single_threaded_children();self.environment.__enter__()
            try:self.pool=ProcessPoolExecutor(max_workers=self.workers,mp_context=multiprocessing.get_context('spawn'),initializer=configure_child_threads)
            except BaseException:
                self.environment.__exit__(None,None,None);raise
        return self
    def run(self,parent_root,needs):
        jobs=iter((str(parent_root),key,sorted(capitals.items())) for key,capitals in sorted(needs.items()))
        facts=[];paths={}
        def collect(result):
            rows,arrays=result;facts.extend(rows);paths.update(arrays)
        if self.pool is None:
            for job in jobs:collect(run_job(job))
        else:
            pending=set()
            try:
                while True:
                    while len(pending)<2*self.workers:
                        job=next(jobs,None)
                        if job is None:break
                        pending.add(self.pool.submit(run_job,job))
                    if not pending:break
                    done,pending=wait(pending,return_when=FIRST_COMPLETED)
                    for future in done:collect(future.result())
            except BaseException:
                for future in pending:future.cancel()
                self.pool.shutdown(wait=True,cancel_futures=True);self.pool=None
                raise
        return sorted(facts,key=lambda r:(r['accountKey'],r['capital'])),paths
    def __exit__(self,*args):
        try:
            if self.pool is not None:self.pool.shutdown(wait=True,cancel_futures=True)
        finally:
            if self.environment is not None:self.environment.__exit__(*args)
