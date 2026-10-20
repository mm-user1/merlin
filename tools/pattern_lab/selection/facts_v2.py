"""Closed funded scalars and immutable links to the parent account evidence."""
from dataclasses import asdict,fields
import math
from ..accounts.contracts import require,integer,number,ExecutionPolicy,AccountWindow,SCHEMA
from ..account_study.common import closed,json_text,parse_json,digest_id
from ..study.bracket_rules import ExecutionRules
from ..study.contracts import semantic_digest
from .funded import execution_key
from .ledger import REJECTIONS
from .storage_v2 import FUNDED


def account_links(parent,ledger):
    links={}
    for row in ledger:
        r=parent.account(row['accountKey']).result
        links[row['accountKey']]=dict(preparationId=r.preparationId,commandId=r.commandId,
            rules=asdict(r.rules),window=asdict(r.window))
    return links


def identity(link,policy):
    return semantic_digest(dict(schema=SCHEMA,preparationId=link['preparationId'],commandId=link['commandId'],
        policy=policy,window=link['window'],rules=link['rules']))


def validate_links(links,ledger,allocation):
    require(type(links) is dict and set(links)=={r['accountKey'] for r in ledger},'parent account link roster')
    for r in ledger:
        link=links[r['accountKey']];closed(link,('preparationId','commandId','rules','window'))
        digest_id(link['preparationId']);digest_id(link['commandId'])
        closed(link['rules'],[f.name for f in fields(ExecutionRules)])
        closed(link['window'],[f.name for f in fields(AccountWindow)])
        require(link['window']==dict(startMs=r['startMs'],endMs=r['endMs']),'parent account window link')
        require(identity(link,allocation['policy'])==r['accountId'],'parent account identity link')


def validate_records(records,month,ledger,links,allocation,paths):
    lookup={r['accountKey']:r for r in ledger if r['month']==month}
    require(records==sorted(records,key=lambda r:(r['accountKey'],r['capital'])),'funded record order')
    seen=set()
    for r in records:
        closed(r,FUNDED.names);key=r['executionKey'];require(key not in seen,'duplicate funded execution');seen.add(key)
        require(r['accountKey'] in lookup and r['instrumentId']==lookup[r['accountKey']]['instrumentId'],'funded parent link')
        capital=number(r['capital'],'funded capital');require(key==execution_key(r['accountKey'],capital),'funded execution key')
        link=links[r['accountKey']];policy=dict(allocation['policy'],initialCapital=capital)
        require(r['policyJson']==json_text(policy) and asdict(ExecutionPolicy(**policy))==policy,'funded policy')
        require(r['rulesJson']==json_text(link['rules']) and r['windowJson']==json_text(link['window']) and
            r['preparationId']==link['preparationId'] and r['commandId']==link['commandId'],'funded immutable parent facts')
        require(r['accountId']==identity(link,policy),'funded account identity')
        require(type(r['finalBalance']) in (int,float) and math.isfinite(r['finalBalance']),'funded final balance')
        number(r['fees'],'funded fees',inclusive=True)
        for name in ('trades','attempts','proposals','exitCommands','unknownExitDecisions'):integer(r[name],name)
        closed(r['sizingRejections'],REJECTIONS)
        for n in r['sizingRejections'].values():integer(n,'funded rejection count')
        require(sum(r['sizingRejections'].values())<=r['attempts'],'funded rejection total')
        closed(r['forcedExits'],('terminal','gapBoundary'))
        for v in r['forcedExits'].values():
            closed(v,('count','netPnl'));integer(v['count'],'forced count')
            require(type(v['netPnl']) in (int,float) and math.isfinite(v['netPnl']),'forced pnl')
        require(sum(v['count'] for v in r['forcedExits'].values())<=r['trades'],'forced count total')
        require(r['pathRef'] in (None,key),'funded path reference')
        if r['pathRef'] is not None:
            require(key in paths,'funded path absent');path=paths[key]
            require(math.isclose(float(path['equity'][-1]),r['finalBalance'],rel_tol=1e-9,abs_tol=1e-8),'funded endpoint reconciliation')
