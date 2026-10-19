"""Bounded planning and exact write guards, without large selection runs."""
from copy import deepcopy
import os
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.account_study.common import json_text, write_json as ordinary_write
from tools.pattern_lab.selection import capacity, artifacts
from ._selection_helpers import request_template


def sizing_request():
    doc=request_template()
    doc['selectors']=[dict(selectorId=n) for n in ('consistency','mean','positive')]
    doc['evaluationMonths']=[f'2021-{m:02d}' for m in range(1,13)]
    doc['kValues']=[6,8,10];doc['randomControl']['replicates']=100
    return doc,[f'BYBIT_TICKER{n:02d}USDT' for n in range(44)]


def test_representative_capacity_and_dimension_growth():
    doc,roster=sizing_request();base=capacity.results_capacity(doc,roster)
    assert base['supported'] and base['randomRows']==10800
    for field in ('replicates','months','selectors','k','identifiers'):
        changed=deepcopy(doc);ids=list(roster)
        if field=='replicates':changed['randomControl']['replicates']*=2
        if field=='months':changed['evaluationMonths']*=2
        if field=='selectors':changed['selectors']*=2
        if field=='k':changed['kValues']+=[12]
        if field=='identifiers':ids=[i+'X'*64 for i in ids]
        assert capacity.results_capacity(changed,ids)['estimatedBytes']>base['estimatedBytes']
    doc['randomControl']['replicates']=10**12
    assert not capacity.results_capacity(doc,roster)['supported']  # no R-sized allocation


@pytest.mark.parametrize('margin',[-1,0,1])
def test_planning_exact_boundary(monkeypatch,margin):
    doc=request_template();roster=['A','LONGER_B']
    amount=capacity.results_capacity(doc,roster)['estimatedBytes']
    monkeypatch.setattr(capacity,'MAX_JSON',amount+margin)
    if margin<0:
        with pytest.raises(PatternLabDataError,match='planning budget.*estimatedBytes.*fewer replicates'):
            capacity.admit_results_capacity(doc,roster)
    else:assert capacity.admit_results_capacity(doc,roster)['supported']


@pytest.mark.parametrize('margin',[-1,0,1])
def test_json_exact_emitted_bytes_before_write(tmp_path,monkeypatch,margin):
    value={'label':'\u00e9\nquoted "value"'}
    payload=(json_text(value)+os.linesep).encode('utf-8')
    ordinary_write(tmp_path/'reference.json',value)
    assert (tmp_path/'reference.json').read_bytes()==payload
    monkeypatch.setattr(artifacts,'MAX_JSON',len(payload)+margin)
    target=tmp_path/'new'/'value.json'
    if margin<0:
        with pytest.raises(PatternLabDataError,match='JSON write bound'):artifacts.write_json(target,value)
        assert not target.parent.exists()
    else:
        artifacts.write_json(target,value)
        assert target.read_bytes()==payload and not target.with_suffix('.json.tmp').exists()
