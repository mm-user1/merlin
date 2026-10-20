"""Typed deterministic monthly parts for v2; no account codec duplication."""
from pathlib import Path
import math
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from ..accounts.contracts import require
from ..account_study.codec import check_table
from .ledger import REJECTIONS

PART_ROWS=10000
MAX_EXPANDED=256*1024*1024
def field(name,kind,nullable=False):return pa.field(name,kind,nullable=nullable)
S=pa.string();F=pa.float64();I=pa.int64();B=pa.bool_()
COUNTS=pa.struct([field(n,I) for n in REJECTIONS])
EXIT=pa.struct([field('count',I),field('netPnl',F)])
EXITS=pa.struct([field(n,EXIT) for n in ('terminal','gapBoundary')])
FUNDED=pa.schema([field(n,S) for n in ('executionKey','accountKey','instrumentId')]+[field(n,F) for n in ('capital','finalBalance')]+
    [field(n,S) for n in ('accountId','preparationId','commandId','policyJson','rulesJson','windowJson')]+
    [field('fees',F)]+[field(n,I) for n in ('trades','attempts','proposals','exitCommands','unknownExitDecisions')]+
    [field('sizingRejections',COUNTS),field('forcedExits',EXITS),field('pathRef',S,True)])
SLOT=pa.struct([field('instrumentId',S),field('executionKey',S)])
PORTFOLIOS=pa.schema([field('portfolioId',S),field('selectorId',S),field('k',I),field('portfolio',S),field('replicateIndex',I),
    field('month',S),field('status',S),field('reason',S,True),field('plannedSlots',I),field('fundedSlots',I),field('idleSlots',I),
    field('slots',pa.list_(field('item',SLOT))),field('members',pa.list_(field('item',S)))]+
    [field(n,F,True) for n in ('openingCapital','closingCapital','slotCapital','fundedCapital','cash','utilizationPct','netPnl','returnPct','fees')]+
    [field('trades',I,True),field('sizingRejections',COUNTS,True),field('forcedExits',EXITS,True)])
FIXED=pa.schema([field('selectorId',S),field('k',I),field('portfolio',S),field('replicateIndex',I),field('month',S),
    field('members',pa.list_(field('item',S)))]+[field(n,I) for n in ('fundedSlots','plannedSlots','idleSlots')]+
    [field(n,F) for n in ('fundedCapital','plannedCapital','utilizationPct')]+[field('available',B)]+
    [field(n,F,True) for n in ('netPnl','returnPct','investedReturnPct','fees')]+
    [field(n,I,True) for n in ('trades','sizingRejections')]+[field('forcedExits',EXITS,True)])


def portfolio_order(r):return r['selectorId'],r['k'],r['portfolio'],r['replicateIndex']


def write_parts(root,folder,month,rows,schema):
    names=[]
    for offset in range(0,len(rows),PART_ROWS):
        relative=f'{folder}/{month}/part-{offset//PART_ROWS:05d}.parquet';p=root/relative;p.parent.mkdir(parents=True,exist_ok=True)
        table=pa.Table.from_pylist(rows[offset:offset+PART_ROWS],schema=schema);check_table(table,schema)
        require(table.nbytes<=MAX_EXPANDED,'expanded Parquet part bound')
        pq.write_table(table,p,compression='zstd',use_dictionary=False)
        require(p.stat().st_size<=MAX_EXPANDED,'Parquet part bound');names.append(relative)
    return names


def read_parts(root,names,schema):
    rows=[]
    for i,name in enumerate(names):
        p=root/name;require(p.stat().st_size<=MAX_EXPANDED,'Parquet file bound')
        pf=pq.ParquetFile(p)
        require(pf.schema_arrow==schema and 0<pf.metadata.num_rows<=PART_ROWS,'Parquet schema/row bound')
        require(sum(pf.metadata.row_group(j).total_byte_size for j in range(pf.num_row_groups))<=MAX_EXPANDED,'expanded Parquet bound')
        table=pf.read();check_table(table,schema)
        require(i==len(names)-1 or len(table)==PART_ROWS,'noncanonical Parquet partition')
        rows.extend(table.to_pylist())
    return rows


def path_groups(paths):
    groups=[];keys=[];size=0;times=None
    for key,p in sorted(paths.items()):
        require(p['times'].dtype==np.dtype('int64') and p['equity'].dtype==np.dtype('float64'),'path dtypes')
        require(p['times'].ndim==p['equity'].ndim==1 and len(p['times'])==len(p['equity']) and np.isfinite(p['equity']).all(),'path values')
        require(times is None or np.array_equal(times,p['times']),'shared monthly path grid')
        times=p['times'];cost=p['equity'].nbytes+1024
        require(times.nbytes+cost+4096<=MAX_EXPANDED,'single funded path bound')
        if keys and size+cost>MAX_EXPANDED:
            groups.append(keys);keys=[];size=0
        if not keys:size=times.nbytes+4096
        keys.append(key);size+=cost
    if keys:groups.append(keys)
    return groups


def write_paths(root,month,paths):
    from .artifacts import save_arrays
    names=[]
    for n,keys in enumerate(path_groups(paths)):
        name=f'funded-paths/{month}/part-{n:05d}.npz'
        save_arrays(root/name,dict(times=paths[keys[0]]['times'],**{key:paths[key]['equity'] for key in keys}));names.append(name)
    return names


def read_paths(root,names):
    from .artifacts import load_arrays
    paths={};groups=[]
    for name in names:
        arrays=load_arrays(root/name);require('times' in arrays and len(arrays)>1,'funded path part members')
        times=arrays.pop('times');keys=sorted(arrays);groups.append(keys)
        for key,values in arrays.items():
            require(key not in paths,'duplicate funded path link');paths[key]=dict(times=times,equity=values)
    require(groups==path_groups(paths),'noncanonical funded path partitions')
    return paths
