"""Part schema/order and bounded spawn failure contracts."""
from concurrent.futures import Future
from pathlib import Path
import os
import subprocess
import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.selection import storage_v2 as storage
from tools.pattern_lab.selection import funded


def test_typed_parts_boundaries_order_and_wrong_schema(tmp_path):
    schema=pa.schema([pa.field('value',pa.int64(),nullable=False)])
    rows=[dict(value=i) for i in range(10001)]
    names=storage.write_parts(tmp_path,'portfolios','2020-05',rows,schema)
    assert names==['portfolios/2020-05/part-00000.parquet','portfolios/2020-05/part-00001.parquet']
    assert storage.read_parts(tmp_path,names,schema)==rows
    with pytest.raises(PatternLabDataError,match='partition'):storage.read_parts(tmp_path,names[::-1],schema)
    pq.write_table(pa.table({'value':['wrong']}),tmp_path/names[0])
    with pytest.raises(PatternLabDataError,match='schema'):storage.read_parts(tmp_path,names,schema)


def test_path_partitions_no_object_arrays_and_duplicate_links(tmp_path,monkeypatch):
    monkeypatch.setattr(storage,'MAX_EXPANDED',7000)
    paths={str(i):dict(times=np.arange(100,dtype=np.int64),equity=np.full(100,float(i))) for i in range(3)}
    names=storage.write_paths(tmp_path,'2020-05',paths);assert len(names)==3
    got=storage.read_paths(tmp_path,names)
    for key in paths:assert np.array_equal(got[key]['equity'],paths[key]['equity'])
    with pytest.raises(PatternLabDataError,match='duplicate'):storage.read_paths(tmp_path,[names[0],names[0]])
    with pytest.raises(PatternLabDataError,match='dtypes'):storage.path_groups({'x':dict(times=np.arange(1),equity=np.array(['x'],object))})


def test_spawn_failure_bounded_submissions_no_retry_cleanup(monkeypatch):
    pools=[]
    class Pool:
        def __init__(self,**kwargs):self.jobs=[];self.closed=[];pools.append(self)
        def submit(self,fn,job):
            self.jobs.append(job);future=Future();future.set_exception(RuntimeError('worker failed'));return future
        def shutdown(self,**kwargs):self.closed.append(kwargs)
    monkeypatch.setattr(funded,'ProcessPoolExecutor',Pool)
    with pytest.raises(RuntimeError,match='worker failed'):
        with funded.FundedExecutor(6) as executor:executor.run(Path('unused'),{str(n):{100.:False} for n in range(50)})
    assert len(pools)==1 and len(pools[0].jobs)==12
    assert len({j[1] for j in pools[0].jobs})==12
    assert pools[0].closed==[dict(wait=True,cancel_futures=True)]


def test_public_carried_example_absolute_path_external_cwd(tmp_path):
    script=Path(__file__).resolve().parents[2]/'tools/pattern_lab/examples/selection_carried_synthetic.py'
    env=dict(os.environ);env.pop('PYTHONPATH',None)
    result=subprocess.run([sys.executable,'-B','-X','utf8',str(script),'--output-root',str(tmp_path/'example')],cwd=tmp_path,env=env,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr
    import json
    evidence=json.loads(result.stdout)
    assert evidence['verification']['carried']['fundedExecutions']>0
    assert evidence['verification']['carried']['scorers']=={'consistency':'recomputed','oi':'recomputed'}
